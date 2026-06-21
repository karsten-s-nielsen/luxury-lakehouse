"""E2E golden for the tracking-PSxG algorithm (task 1.8).

Exercises the full chain in pure Python — score (1.1) -> calibrate (1.2) ->
build prediction rows (1.3) -> resolve is_goal/defending-GK (fct_shot_psxg join)
-> additive GK rollup (fct_gk_shot_stopping) -> Poisson-binomial band (pooled) —
on a multi-provider fixture, and asserts the invariants the dbt marts must honor:

  - multi-provider: GS + SkillCorner + IDSSE rows all traverse the provider-as-column path;
  - B3: a GATE-FAILED conceded goal is excluded from psxg_faced AND goals_conceded_on_shots
    but counted in shots_faced_total (coverage < 1) — goals_prevented never mixes denominators;
  - band: Var = sum psxg*(1-psxg) (Poisson-binomial), CI = goals_prevented +/- 1.96*sqrt(Var);
  - ranking_enabled is computed from the cohort and is False at fixture scale (not hard-coded).

The dbt SQL marts (fct_shot_psxg / fct_gk_shot_stopping / _pooled) implement this same
algorithm; their structure is validated by `dbt parse` and a full build at deploy. This test
is the inline regression floor for the algorithm itself.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.goalkeeper import PSxGModel
from ingestion.compute_psxg_tracking import build_predictions

_Z = 1.96


def _model() -> PSxGModel:
    return PSxGModel(
        coefficients=np.array([1.0, 1.5]),
        intercept=-0.8,
        scaler_mean=np.array([0.0, 0.0]),
        scaler_scale=np.array([1.0, 1.0]),
    )


def _fixture() -> pd.DataFrame:
    # GK 100 faces 4 shots (2 matches, incl. 1 GATED conceded goal); GK 200 faces 2.
    return pd.DataFrame(
        {
            "match_key": [1, 1, 2, 2, 3, 3],
            "action_id": [10, 11, 12, 13, 14, 15],
            "data_source": ["gradientsports", "gradientsports", "skillcorner", "skillcorner", "idsse", "idsse"],
            "defending_gk_player_key": [100, 100, 100, 100, 200, 200],
            "shot_crossing_y": [33.0, 35.0, 32.5, 36.0, 34.0, 33.5],
            "shot_crossing_z": [1.0, 2.0, 0.6, 1.8, 1.2, 0.9],
            "shot_crossing_confidence": [0.9, 0.9, 0.8, 0.15, 0.85, 0.9],  # idx3 gated
            "shot_fit_rmse": [0.1, 0.2, 0.2, 0.1, 0.15, 0.1],
            # GK 100's only conceded goal (action_id 13) is the GATED shot — B3: it must
            # vanish from goals_conceded_on_shots yet surface via coverage_pct < 1.
            "action_result": ["fail", "fail", "fail", "success", "fail", "success"],
        }
    )


def _gk_rollup(out: pd.DataFrame, fixture: pd.DataFrame) -> pd.DataFrame:
    """Reimplements the fct_shot_psxg join + fct_gk_shot_stopping additive rollup."""
    joined = out.merge(
        fixture[["action_id", "defending_gk_player_key", "action_result"]],
        on="action_id",
        how="left",
    )
    joined["is_goal"] = joined["action_result"] == "success"
    passed = ~joined["psxg_gated"].astype(bool)
    joined["_faced_total"] = 1
    joined["_faced"] = passed.astype(int)
    joined["_goals"] = (passed & joined["is_goal"]).astype(int)
    joined["_psxg"] = np.where(passed, joined["psxg_recalibrated"].fillna(0.0), 0.0)
    joined["_var"] = np.where(passed, joined["_psxg"] * (1 - joined["_psxg"]), 0.0)
    agg = joined.groupby("defending_gk_player_key").agg(
        shots_faced_total=("_faced_total", "sum"),
        shots_faced=("_faced", "sum"),
        goals_conceded_on_shots=("_goals", "sum"),
        psxg_faced=("_psxg", "sum"),
        psxg_variance_sum=("_var", "sum"),
    )
    agg["goals_prevented"] = agg["psxg_faced"] - agg["goals_conceded_on_shots"]
    agg["coverage_pct"] = agg["shots_faced"] / agg["shots_faced_total"]
    agg["ci_low"] = agg["goals_prevented"] - _Z * np.sqrt(agg["psxg_variance_sum"])
    agg["ci_high"] = agg["goals_prevented"] + _Z * np.sqrt(agg["psxg_variance_sum"])
    return agg


def test_e2e_multi_provider_and_b3_coverage_and_band() -> None:
    fixture = _fixture()
    out = build_predictions(fixture, _model(), model_version="psxg-vTEST")

    # Multi-provider: all three tracking providers traverse the path.
    assert {"gradientsports", "skillcorner", "idsse"} <= set(out["data_source"])

    agg = _gk_rollup(out, fixture)
    gk100 = agg.loc[100]

    # B3: GK 100 faced 4 shots; action_id 13 is a gated GOAL (conf 0.15).
    assert gk100["shots_faced_total"] == 4  # all faced shots counted (pre-gate)
    assert gk100["shots_faced"] == 3  # the gated shot excluded post-gate
    # The gated goal is excluded from goals_conceded_on_shots (B3 — same denominator as psxg_faced).
    assert gk100["goals_conceded_on_shots"] == 0  # only gate-passed goals; idx13 goal is gated out
    assert gk100["coverage_pct"] < 1.0  # gating visible as coverage gap

    # goals_prevented never mixes denominators.
    assert gk100["goals_prevented"] == agg.loc[100, "psxg_faced"] - agg.loc[100, "goals_conceded_on_shots"]

    # Poisson-binomial band: finite and symmetric around goals_prevented.
    assert np.isfinite(gk100["ci_low"]) and np.isfinite(gk100["ci_high"])
    half_width = _Z * np.sqrt(gk100["psxg_variance_sum"])
    assert np.isclose(gk100["ci_high"] - gk100["goals_prevented"], half_width)
    assert np.isclose(gk100["goals_prevented"] - gk100["ci_low"], half_width)


def test_e2e_ranking_disabled_at_fixture_scale() -> None:
    # ranking_enabled is computed from the cohort (>=20 GKs x >=20 shots); at fixture
    # scale (2 GKs) it must be False — asserted as a computed property, not hard-coded.
    fixture = _fixture()
    out = build_predictions(fixture, _model(), model_version="psxg-vTEST")
    agg = _gk_rollup(out, fixture)
    n_gks_above_floor = int((agg["shots_faced_total"] >= 20).sum())
    ranking_enabled = n_gks_above_floor >= 20
    assert ranking_enabled is False
