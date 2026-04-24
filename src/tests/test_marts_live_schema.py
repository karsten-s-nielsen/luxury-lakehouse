"""Live-Delta gold mart schema coverage.

Complements dbt's ``contract: enforced: true`` by asserting the live Delta
table exposes the expected column set + types. Catches drift between the
YAML contract, the compiled SQL, and what Databricks actually stores.

Scope at PR 4b (2026-04-23): ``fct_action_values`` only. Pattern lifted
from ``test_bronze_live_schema.py``. Expand as subsequent Kimball PRs
land.

Requires live Databricks SQL warehouse. Skipped when
``DATABRICKS_{HOST,HTTP_PATH,TOKEN}`` env vars are unset.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

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


def _describe_live(conn: object, table_fqn: str) -> dict[str, str]:
    """Return {column_name: column_type_lower} for a live Delta table."""
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(f"DESCRIBE {table_fqn}")
    rows = cur.fetchall()
    cols: dict[str, str] = {}
    for row in rows:
        name = row[0]
        if not name or name.startswith("#"):
            continue
        cols[name] = str(row[1]).lower().strip()
    return cols


# ---------------------------------------------------------------------------
# fct_action_values (PR 4b Kimball migration, 2026-04-23)
# ---------------------------------------------------------------------------

_FCT_ACTION_VALUES_EXPECTED_COLS: dict[str, str] = {
    "action_value_id": "string",
    # Kimball surrogates + legacy BIGINT match_id (PR 4b, 2026-04-23).
    "match_key": "bigint",
    "competition_key": "bigint",
    "match_id": "bigint",
    # Legacy competition_id / player/team/season IDs are int per contract.
    "competition_id": "int",
    "player_id": "int",
    "team_id": "int",
    "season_id": "int",
    "period": "int",
    "time_seconds": "double",
    "minute": "int",
    "second": "int",
    "start_x": "double",
    "start_y": "double",
    "end_x": "double",
    "end_y": "double",
    "action_type": "string",
    "action_result": "string",
    "bodypart": "string",
    "offensive_value": "double",
    "defensive_value": "double",
    "vaep_value": "double",
    "possession_id": "bigint",
    "possession_team_id": "int",
    "game_state": "string",
    "data_source": "string",
    "original_event_id": "string",
    "_loaded_at": "timestamp",
}


@requires_databricks
def test_fct_action_values_live_schema_matches_contract(conn: object) -> None:
    """Live DESCRIBE on dev_gold.fct_action_values matches the PR 4b contract."""
    actual = _describe_live(conn, "soccer_analytics.dev_gold.fct_action_values")
    missing = set(_FCT_ACTION_VALUES_EXPECTED_COLS) - set(actual)
    extras = set(actual) - set(_FCT_ACTION_VALUES_EXPECTED_COLS)
    assert not missing, f"Columns missing from live fct_action_values: {sorted(missing)}"
    assert not extras, f"Unexpected columns in live fct_action_values: {sorted(extras)}"
    type_mismatches = [
        (c, _FCT_ACTION_VALUES_EXPECTED_COLS[c], actual[c])
        for c in _FCT_ACTION_VALUES_EXPECTED_COLS
        if actual[c] != _FCT_ACTION_VALUES_EXPECTED_COLS[c]
    ]
    assert not type_mismatches, f"Type mismatches: {type_mismatches}"


@requires_databricks
def test_fct_action_values_match_key_not_null(conn: object) -> None:
    """Zero rows should have NULL match_key post-migration.

    Validates dim_matches resolution succeeded for every (match_id, data_source)
    combination in the source. A non-zero count here indicates orphan rows
    in stg_spadl__action_values that aren't in dim_matches.
    """
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute("SELECT count(*) FROM soccer_analytics.dev_gold.fct_action_values WHERE match_key IS NULL")
    row = cur.fetchone()
    null_count = int(row[0]) if row else 0
    assert null_count == 0, (
        f"fct_action_values has {null_count} rows with NULL match_key. "
        "Indicates dim_matches resolution failed for some (match_id, data_source) — "
        "check stg_spadl__action_values orphans vs dim_matches."
    )


@requires_databricks
def test_fct_action_values_legacy_match_id_matches_new_key(conn: object) -> None:
    """Dual-column invariant: (match_key, match_id) are 1:1 related.

    Every distinct match_key maps to exactly one match_id and vice versa.
    Catches accidental divergence between the Kimball surrogate and the
    legacy native identifier during the 90-day window.
    """
    cur = conn.cursor()  # type: ignore[attr-defined]
    cur.execute(
        "SELECT count(*) FROM ("
        "  SELECT match_key, count(DISTINCT match_id) AS n"
        "  FROM soccer_analytics.dev_gold.fct_action_values"
        "  WHERE match_key IS NOT NULL AND match_id IS NOT NULL"
        "  GROUP BY match_key"
        "  HAVING count(DISTINCT match_id) > 1"
        ") t"
    )
    row = cur.fetchone()
    divergent = int(row[0]) if row else 0
    assert divergent == 0, f"{divergent} match_keys map to multiple match_ids"
