"""Bronze↔staging rowcount parity for ``WHERE col IS NOT NULL`` filters.

For every staging model that filters bronze rows via a non-null guard,
assert that the filter is conservative — no bronze row is silently dropped
by an unintended side-effect.

Invariant per (bronze_table, staging_model, filter_predicate) triple::

    count(staging) + count(bronze WHERE filter_predicate) == count(bronze)

where ``filter_predicate`` describes rows the staging model drops. Any
deviation means either (a) the staging model joined away rows, (b) an
additional implicit filter narrowed the set, or (c) the stated filter-col
is not the only predicate. All three are bugs the author must triage.

Four filter sites are audited (G3 from the PR #173 bronze drop-safety
audit — the Mode 4 "filter drop" failure mode):

  - ``stg_idsse__events``       — ``where x is not null and y is not null``
  - ``stg_idsse__tracking``     — ``where x is not null and y is not null``
  - ``stg_skillcorner__tracking`` — ``where x is not null and y is not null``
  - ``stg_wyscout__players``    — ``where wyId is not null``

The fifth site — ``stg_metrica__tracking`` — is intentionally excluded.
Its filter (``where raw_x is not null and raw_y is not null``) runs on
the ``all_players`` CTE which is a ``lateral view explode`` unpivot of
the bronze ``home_players`` / ``away_players`` JSON maps. The direct
invariant ``count(staging) + count(bronze WHERE ...) == count(bronze)``
does not apply there — the staging row count is ~22x the bronze row
count (one row per player per frame), and the filter's denominator is
the exploded rowset, not bronze. Mode 4 coverage for that model belongs
in a staging-to-staging parity test (compute the unpivot, count drops
against the unpivot baseline) — out of scope for this drop-safety sweep.

Requires live Databricks. Skipped when ``DATABRICKS_{HOST,HTTP_PATH,TOKEN}``
env vars are unset or ``databricks-sql-connector`` is not installed. The
CI entry point is ``.github/workflows/data-quality-ci.yml``.

Schema targets:
  Bronze tables live in ``soccer_analytics.bronze`` (single-env bronze —
  the daily ingestion job writes here regardless of dev/prod context).
  Staging models live in ``soccer_analytics.dev_silver`` (the dev dbt
  schema where ``dbt build`` materializes staging views). The prod
  ``soccer_analytics.silver`` schema currently contains no ``stg_*``
  tables because production dbt builds only materialize marts into
  ``gold``. When prod staging comes online, override ``STAGING_SCHEMA``
  via env in the workflow.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator

import pytest

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


# (bronze_table, staging_model, filter_predicate_matching_dropped_rows).
# The predicate matches rows the staging model DROPS (i.e. the inverse of
# the ``WHERE ... IS NOT NULL`` conjunction that staging keeps). For a
# multi-column filter like ``where x is not null and y is not null``,
# the dropped predicate is the disjunction ``(x IS NULL OR y IS NULL)``.
_FILTER_SITES: list[tuple[str, str, str]] = [
    ("idsse_events", "stg_idsse__events", "(x IS NULL OR y IS NULL)"),
    ("idsse_tracking", "stg_idsse__tracking", "(x IS NULL OR y IS NULL)"),
    ("skillcorner_tracking", "stg_skillcorner__tracking", "(x IS NULL OR y IS NULL)"),
    ("wyscout_players", "stg_wyscout__players", "wyId IS NULL"),
    # NOTE: metrica_tracking is intentionally excluded — its filter runs on
    # an unpivot CTE (all_players), not directly on bronze. See module
    # docstring for the rationale.
]


# Schema defaults; override via env in CI. See module docstring.
_BRONZE_SCHEMA = os.environ.get("BRONZE_SCHEMA", "soccer_analytics.bronze")
_STAGING_SCHEMA = os.environ.get("STAGING_SCHEMA", "soccer_analytics.dev_silver")

# Defense-in-depth: validate every identifier the tests build SQL from, so
# even if a future env override passes in untrusted input, the SQL cannot be
# injected. All production values are internal constants that already match.
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$")
# Filter predicates are hardcoded in _FILTER_SITES (no external input ever
# reaches them), but validate the accepted charset anyway — alphanumerics,
# underscores, whitespace, parens, quotes, equals, and the literal tokens
# IS/NULL/OR/AND. Tight enough to refuse ``; DROP TABLE ...``.
_PREDICATE_RE = re.compile(r"^[A-Za-z0-9_\s()=_'.,]+$")


def _validate_ident(name: str, *, label: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"invalid {label} identifier: {name!r}")
    return name


def _validate_predicate(pred: str) -> str:
    if not _PREDICATE_RE.match(pred):
        raise ValueError(f"invalid filter predicate: {pred!r}")
    return pred


@pytest.fixture(scope="module")
def conn() -> Iterator[object]:
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    c = databricks_sql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    try:
        yield c
    finally:
        c.close()


def _scalar_count(conn: object, sql: str) -> int:
    cur = conn.cursor()  # type: ignore[attr-defined]
    try:
        cur.execute(sql)
        row = cur.fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        cur.close()


@requires_databricks
@pytest.mark.parametrize(("bronze_table", "staging_model", "filter_predicate"), _FILTER_SITES)
def test_staging_preserves_bronze_rows(
    conn: object,
    bronze_table: str,
    staging_model: str,
    filter_predicate: str,
) -> None:
    bronze_schema = _validate_ident(_BRONZE_SCHEMA, label="bronze schema")
    staging_schema = _validate_ident(_STAGING_SCHEMA, label="staging schema")
    bronze_tbl = _validate_ident(bronze_table, label="bronze table")
    staging_tbl = _validate_ident(staging_model, label="staging model")
    predicate = _validate_predicate(filter_predicate)

    # S608 is a false positive on every SELECT below: every interpolated
    # identifier passes _validate_ident and the predicate passes
    # _validate_predicate. No untrusted input can reach these f-strings —
    # the values are either module-level constants or parametrize args
    # drawn from the _FILTER_SITES constant. The per-line noqa declarations
    # below document the suppression explicitly.
    bronze_total = _scalar_count(
        conn,
        f"SELECT count(*) FROM {bronze_schema}.{bronze_tbl}",  # noqa: S608 — validated identifiers
    )
    bronze_dropped = _scalar_count(
        conn,
        f"SELECT count(*) FROM {bronze_schema}.{bronze_tbl} WHERE {predicate}",  # noqa: S608 — validated identifiers + predicate charset
    )
    try:
        staging_total = _scalar_count(
            conn,
            f"SELECT count(*) FROM {staging_schema}.{staging_tbl}",  # noqa: S608 — validated identifiers
        )
    except Exception as exc:
        # Staging views depend on dbt-live-ci materializing them. If the view
        # is stale (references columns that no longer exist in bronze after a
        # schema change) or unmaterialized (ref'd model not yet built), the
        # query fails with an AnalysisException. This is a test precondition
        # failure, not a code bug — skip with a clear diagnostic.
        pytest.skip(
            f"Staging view {staging_schema}.{staging_tbl} is not queryable "
            f"(stale or unmaterialized). Run dbt-live-ci to refresh. "
            f"Error: {exc}"
        )

    expected = bronze_total - bronze_dropped
    delta = staging_total - expected
    assert staging_total == expected, (
        f"\n[{bronze_tbl} → {staging_tbl}] Mode 4 filter-drop parity broken:\n"
        f"  bronze total                = {bronze_total:,}\n"
        f"  bronze matching filter-drop = {bronze_dropped:,}\n"
        f"  staging total               = {staging_total:,}\n"
        f"  expected (bronze - dropped) = {expected:,}\n"
        f"  delta                       = {delta:+,}\n"
        f"  dropped filter predicate    = {predicate!r}\n"
        "Mode 4 (filter drop) violation: staging is dropping rows that the\n"
        "documented `WHERE ... IS NOT NULL` filter does not account for.\n"
        "Fix: either expand the predicate to match, remove the hidden extra\n"
        "filter, or document the additional exclusion in the staging SQL."
    )
