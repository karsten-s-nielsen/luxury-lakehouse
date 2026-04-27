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


# ---------------------------------------------------------------------------
# PR 6 (2026-04-26): fct_defensive_values, fct_defcon_actions,
# fct_defcon_pressure, fct_goalkeeper_stats, fct_gk_actions_detail.
#
# The expected schemas below assert presence + types. If a column type
# mismatches, dbt's contract: enforced will catch it at build time too —
# this test is the post-deploy live check. Spark sum(case) returns BIGINT
# (not int) for boolean-aggregation columns; count(*) returns BIGINT.
# ---------------------------------------------------------------------------


_FCT_DEFENSIVE_VALUES_EXPECTED_COLS: dict[str, str] = {
    "defensive_value_id": "string",
    "player_id": "int",
    "match_id": "string",
    "competition_id": "int",
    "season_id": "int",
    "team_id": "int",
    "data_source": "string",
    # PR 6 Kimball surrogates
    "match_key": "bigint",
    "team_key": "bigint",
    "player_key": "bigint",
    "total_defcon_value": "double",
    "total_credits": "bigint",
    "intercept_value": "double",
    "concede_value": "double",
    "disturb_value": "double",
    "deter_value": "double",
    "intercept_count": "bigint",
    "concede_count": "bigint",
    "disturb_count": "bigint",
    "deter_count": "bigint",
    "high_confidence_count": "bigint",
    "approx_confidence_count": "bigint",
    "_loaded_at": "timestamp",
}


_FCT_DEFCON_ACTIONS_EXPECTED_COLS: dict[str, str] = {
    "defcon_action_id": "string",
    "event_id": "string",
    "match_id": "string",
    "competition_id": "int",
    "season_id": "int",
    "player_id": "int",
    "team_id": "int",
    "defender_x": "double",
    "defender_y": "double",
    "action_player_id": "int",
    "action_type": "string",
    "action_x": "double",
    "action_y": "double",
    "credit_type": "string",
    "confidence": "string",
    "defcon_value": "double",
    "dist_to_ball": "double",
    "pitch_control_at_action": "double",
    "data_source": "string",
    # PR 6 Kimball surrogates (defender + action_player)
    "match_key": "bigint",
    "team_key": "bigint",
    "player_key": "bigint",
    "action_player_key": "bigint",
    "_loaded_at": "timestamp",
}


_FCT_DEFCON_PRESSURE_EXPECTED_COLS: dict[str, str] = {
    "pressure_id": "string",
    "player_id": "int",
    "match_id": "string",
    "competition_id": "int",
    "season_id": "int",
    "data_source": "string",
    # PR 6 Kimball surrogates
    "match_key": "bigint",
    "player_key": "bigint",
    "total_pressure": "double",
    "total_defensive_actions": "bigint",
    "intercept_pressure": "double",
    "concede_pressure": "double",
    "disturb_pressure": "double",
    "deter_pressure": "double",
    "intercept_count": "bigint",
    "concede_count": "bigint",
    "disturb_count": "bigint",
    "deter_count": "bigint",
    "high_confidence_count": "bigint",
    "approx_confidence_count": "bigint",
    "_loaded_at": "timestamp",
}


_FCT_GOALKEEPER_STATS_EXPECTED_COLS: dict[str, str] = {
    "gk_stat_id": "string",
    "player_id": "int",
    "match_id": "bigint",
    "team_id": "int",
    "competition_id": "int",
    "season_id": "int",
    "data_source": "string",
    # PR 6 Kimball surrogates + permanent data_source column
    "match_key": "bigint",
    "team_key": "bigint",
    "player_key": "bigint",
    "minutes_played": "double",
    "saves": "bigint",
    "save_pct": "double",
    "claims": "bigint",
    "claim_success_rate": "double",
    "punches": "bigint",
    "distribution_passes": "bigint",
    "gk_xt_delta_total": "double",
    "gk_xt_per_pass": "double",
    "launch_rate": "double",
    "keeper_pick_ups": "bigint",
    "psxg_faced": "double",
    "goals_conceded": "int",
    "goals_prevented": "double",
    "avg_defensive_action_distance": "double",
    "actions_outside_box_per_90": "double",
}


_FCT_GK_ACTIONS_DETAIL_EXPECTED_COLS: dict[str, str] = {
    "gk_action_id": "string",
    "match_id": "bigint",
    "match_key": "bigint",
    "competition_id": "int",
    "season_id": "int",
    "team_id": "int",
    "player_id": "int",
    "team_key": "bigint",
    "player_key": "bigint",
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
    "data_source": "string",
    "_loaded_at": "timestamp",
}


_PR6_MARTS: tuple[tuple[str, dict[str, str]], ...] = (
    ("fct_defensive_values", _FCT_DEFENSIVE_VALUES_EXPECTED_COLS),
    ("fct_defcon_actions", _FCT_DEFCON_ACTIONS_EXPECTED_COLS),
    ("fct_defcon_pressure", _FCT_DEFCON_PRESSURE_EXPECTED_COLS),
    ("fct_goalkeeper_stats", _FCT_GOALKEEPER_STATS_EXPECTED_COLS),
    ("fct_gk_actions_detail", _FCT_GK_ACTIONS_DETAIL_EXPECTED_COLS),
)


@requires_databricks
@pytest.mark.parametrize(("mart", "expected"), _PR6_MARTS)
def test_pr6_mart_live_schema_matches_contract(conn: object, mart: str, expected: dict[str, str]) -> None:
    """Live DESCRIBE on each PR-6 mart matches the expected column set + types.

    Catches drift between the YAML contract, compiled SQL, and live storage.
    Type-tolerance: `bigint` and `int` distinctions matter for FK joins.
    """
    actual = _describe_live(conn, f"soccer_analytics.dev_gold.{mart}")
    missing = set(expected) - set(actual)
    extras = set(actual) - set(expected)
    assert not missing, f"Columns missing from live {mart}: {sorted(missing)}"
    assert not extras, f"Unexpected columns in live {mart}: {sorted(extras)}"
    type_mismatches = [
        (c, expected[c], actual[c])
        for c in expected
        if actual[c] != expected[c]
    ]
    assert not type_mismatches, f"Type mismatches in {mart}: {type_mismatches}"
