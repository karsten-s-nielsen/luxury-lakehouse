"""Live boundary test: bronze.idsse_*.match_id must match the canonical
``shared.identifiers.idsse_native_match_id`` BARE format.

ADR-018 cross-table format-contract test (live-data flavour, complementing
the static ``test_format_contract.py::TestIdsseFormatContract`` which only
checks the generator function's own output format).

Catches the regression class that surfaced in session 69 (2026-04-30):

* PR #229 changed bronze IDSSE writers to emit BARE DFL MatchId
  (``J03WMX``) — replacing the legacy ``idsse_J03WMX`` prefixed form.
* The G7 cleanup in session 66 deleted prefixed rows from
  ``bronze.idsse_events`` but missed ``bronze.idsse_tracking``,
  leaving 7 prefixed orphan match_ids alongside the 7 bare ones.
* Downstream ``stg_idsse__matches`` then produced 14 rows where dbt
  expected 7 — failing both ``not_null`` and ``unique`` data tests.

This test queries the live bronze tables and rejects ANY ``match_id`` value
that does not match ``_IDSSE_MATCH_ID_PATTERN`` (``^[A-Z0-9]+$``). Re-running
after a future bronze-format change forces the operator to also clean up
the corresponding bronze rows — the test fails until both code AND data
are aligned.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

# Re-import the canonical pattern so this test fails-loudly if the source-
# of-truth regex in shared/identifiers.py is ever loosened or removed.
from shared.identifiers import _IDSSE_MATCH_ID_PATTERN

databricks_sql = pytest.importorskip("databricks.sql")

requires_databricks = pytest.mark.skipif(
    not all(os.environ.get(var) for var in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")),
    reason="Databricks SQL env vars not set",
)


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


def _distinct_match_ids(conn: object, table: str) -> list[str]:
    cur = conn.cursor()  # type: ignore[attr-defined]
    # `table` is parametrized from a hardcoded literal set in this file
    # (['idsse_tracking', 'idsse_events']) — never user input.
    cur.execute(f"SELECT DISTINCT match_id FROM soccer_analytics.bronze.{table}")  # noqa: S608
    return [str(row[0]) for row in cur.fetchall()]


@requires_databricks
@pytest.mark.parametrize("table", ["idsse_tracking", "idsse_events"])
def test_bronze_idsse_match_id_is_bare_format(conn: object, table: str) -> None:
    """Every distinct ``match_id`` value in bronze must match
    ``shared.identifiers._IDSSE_MATCH_ID_PATTERN`` (``^[A-Z0-9]+$``).

    Failures indicate either:
      (a) a bronze writer regressed and started emitting prefixed values, or
      (b) cleanup of legacy prefixed rows is incomplete (session 69 case).

    The fix is always to clean up the bronze table so all rows match the
    canonical generator's output — never to relax the regex.
    """
    distinct = _distinct_match_ids(conn, table)
    invalid = [mid for mid in distinct if not _IDSSE_MATCH_ID_PATTERN.match(mid)]
    assert not invalid, (
        f"bronze.{table} contains {len(invalid)} match_id value(s) that do not match "
        f"the canonical bare DFL MatchId pattern ('^[A-Z0-9]+$'): {sorted(invalid)}. "
        f"Fix: clean up bronze rows under those match_ids and re-ingest with the "
        f"canonical writer (which calls shared.identifiers.idsse_native_match_id)."
    )


@requires_databricks
def test_bronze_idsse_tracking_has_seven_matches(conn: object) -> None:
    """Coverage smoke test: bronze.idsse_tracking must have exactly the
    expected 7 IDSSE_MATCH_IDS — no orphans, no missing.

    Catches asymmetric regressions where one match's prefixed rows linger
    while the bare counterpart was already cleaned (or vice versa).
    """
    from ingestion.idsse import IDSSE_MATCH_IDS

    distinct = set(_distinct_match_ids(conn, "idsse_tracking"))
    expected = set(IDSSE_MATCH_IDS)
    extra = distinct - expected
    missing = expected - distinct
    assert not extra and not missing, (
        f"bronze.idsse_tracking match_id set drift: extra={sorted(extra)}, "
        f"missing={sorted(missing)}. Expected exactly {sorted(expected)}."
    )
