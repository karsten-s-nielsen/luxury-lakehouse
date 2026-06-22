"""App-expected GK Analytics mart columns must exist in the dbt contract (read-side parity, ADR-051/061).

Static CI gate (no live DB): the redesigned Goalkeeper Analytics page reads specific columns from four
gold marts; this asserts each required set is a subset of the `_marts__models.yml` contract, so a producer
column rename fails CI here instead of silently in the deployed Space. The live-DB counterpart
(hf_taipy_app/src/test_gk_analytics_read_contract.py) is operator/live-run only — it skips in CI — so this
static test is the actual CI guard. Path setup follows the repo's per-test sys.path-insert convention for
hf_taipy_app/src imports (see the pyright extraPaths note in pyproject.toml).
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))

_YML = Path(__file__).parents[2] / "dbt_project" / "models" / "marts" / "_marts__models.yml"


def _contract_columns(model_name: str) -> set[str]:
    doc = yaml.safe_load(_YML.read_text(encoding="utf-8"))
    model = next(m for m in doc["models"] if m["name"] == model_name)
    return {c["name"] for c in model["columns"]}


def test_distribution_profile_cols_subset_of_actions_contract():
    # queries.gk_analytics.build_distribution_profile_sql (ADR-061: action-grain off fct_gk_tracking_actions).
    needed = {"player_key", "match_key", "xt_gk", "gk_completion", "start_x", "end_x"}
    missing = needed - _contract_columns("fct_gk_tracking_actions")
    assert not missing, f"distribution query expects columns absent from the contract: {missing}"


def test_sweeper_and_lov_cols_subset_of_stats_contract():
    from queries.gk_analytics import _SWEEP_COLS

    needed = {
        "gk_player_key",
        "match_key",
        "data_source",
        "n_distributions",
        "n_defended_actions",
        "shots_faced",
        "ghost_deviation_mean_m",
    } | set(_SWEEP_COLS)
    missing = needed - _contract_columns("fct_gk_tracking_stats")
    assert not missing, f"sweeper/LOV query expects columns absent from the stats contract: {missing}"


def test_line_cols_subset_of_defensive_line_contract():
    # queries.gk_analytics.build_line_context_sql — own-goal-distance line height (ADR-061).
    needed = {"gk_player_key", "competition_key", "avg_line_height_m", "avg_width", "avg_compactness", "n_actions"}
    missing = needed - _contract_columns("fct_gk_defensive_line")
    assert not missing, f"line query expects columns absent from the defensive-line contract: {missing}"


def test_goals_prevented_cols_subset_of_pooled_contract():
    # queries.gk_analytics.build_goals_prevented_sql.
    needed = {
        "player_key",
        "competition_key",
        "season_id",
        "data_source",
        "goals_prevented",
        "goals_prevented_ci_low",
        "goals_prevented_ci_high",
        "shots_faced_total",
        "low_sample",
    }
    missing = needed - _contract_columns("fct_gk_shot_stopping_pooled")
    assert not missing, f"goals-prevented query expects columns absent from the pooled contract: {missing}"
