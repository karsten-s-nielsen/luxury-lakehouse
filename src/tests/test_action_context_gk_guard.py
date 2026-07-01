"""Merge-time guard: fct_action_context carries the mart-level GK-contamination exclusion.

dbt PR CI is parse-only (Thrift unreachable from GH runners), so the dbt singular test
``assert_xt_gk_contamination_bounded`` only runs in the daily-live build. This python test
asserts, at merge time, that the mart SQL still carries the guard — the exclusion CTE, the
value-family NULLing, and the flag column — so a refactor cannot silently drop it. See
reference_dbt_ci_parse_only_tests_daily + the 2026-07-01 mart-level-gk-scored-players-guard
handoff.
"""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MART_SQL = _REPO_ROOT / "dbt_project" / "models" / "marts" / "fct_action_context.sql"
_SINGULAR_TEST = _REPO_ROOT / "dbt_project" / "tests" / "assert_xt_gk_contamination_bounded.sql"

# The full xt_gk value family that MUST be excluded (NULLed) on a contaminated match — the
# scored value + presets + raw components + completion. Provenance/coord columns are retained.
_XT_GK_VALUE_COLUMNS = (
    "xt_gk",
    "xt_gk_possession",
    "xt_gk_counter",
    "xt_gk_direct",
    "xt_gk_high_press",
    "xt_gk_low_block",
    "xt_gk_base",
    "xt_gk_pev",
    "xt_gk_rav",
    "xt_gk_dzv",
    "xt_gk_pressure",
    "gk_completion",
)


def test_mart_defines_contamination_cte_keyed_on_the_threshold_var() -> None:
    sql = _MART_SQL.read_text(encoding="utf-8")
    assert "xt_gk_contaminated_matches as (" in sql, "contamination-exclusion CTE missing from mart"
    assert "count(distinct player_key)" in sql, "guard must count distinct scored players per (match, team)"
    assert "xt_gk_max_scored_players_per_team" in sql, "guard threshold must be the tunable dbt var"


def test_mart_nulls_every_xt_gk_value_column_when_contaminated() -> None:
    sql = _MART_SQL.read_text(encoding="utf-8")
    for col in _XT_GK_VALUE_COLUMNS:
        expected = f"case when cm._contam_match_key is not null then null else {col} end as {col}"
        assert expected in sql, f"xt_gk value column {col!r} not excluded on contaminated matches"


def test_mart_exposes_the_contamination_flag_column() -> None:
    sql = _MART_SQL.read_text(encoding="utf-8")
    assert "as xt_gk_match_contaminated" in sql, "mart must expose the xt_gk_match_contaminated flag"


def test_daily_singular_regression_test_exists() -> None:
    assert _SINGULAR_TEST.exists(), "assert_xt_gk_contamination_bounded.sql (daily-live guard) missing"
    body = _SINGULAR_TEST.read_text(encoding="utf-8")
    assert "xt_gk_max_scored_players_per_team" in body
    assert "fct_action_context" in body
