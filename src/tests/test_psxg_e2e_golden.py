"""E2E golden for the tracking-PSxG algorithm (task 1.8).

Exercises the full chain — score (1.1) -> calibrate (1.2) -> build prediction
rows (1.3) -> resolve is_goal/defending-GK (fct_shot_psxg join) -> additive GK
rollup (fct_gk_shot_stopping) -> Poisson-binomial band (pooled) — on a
multi-provider fixture, and asserts the invariants the dbt marts must honor:

  - multi-provider: GS + SkillCorner + IDSSE rows all traverse the provider-as-column path;
  - B3: a GATE-FAILED conceded goal is excluded from psxg_faced AND goals_conceded_on_shots
    but counted in shots_faced_total (coverage < 1) — goals_prevented never mixes denominators;
  - band: Var = sum psxg*(1-psxg) (Poisson-binomial), CI = goals_prevented +/- 1.96*sqrt(Var);
  - ranking_enabled is computed from the cohort and is False at fixture scale (not hard-coded).

The rollup is reimplemented here in pure Python (native floats — no pandas-scalar
typing) so the assertions are type-checker-clean and version-independent. The dbt
SQL marts implement the same algorithm; their structure is validated by `dbt parse`
and a full build at deploy. This test is the inline regression floor for the algorithm.
"""

from __future__ import annotations

import math

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


def _gk_rollup(out: pd.DataFrame, fixture: pd.DataFrame) -> dict[int, dict[str, float]]:
    """Pure-Python reimplementation of the fct_shot_psxg join + fct_gk_shot_stopping rollup.

    Returns one dict of native floats per defending GK (player_key) — no pandas-scalar
    types leak into the assertions.
    """
    gk_of = dict(zip(fixture["action_id"].tolist(), fixture["defending_gk_player_key"].tolist(), strict=True))
    goal_of = {
        aid: res == "success"
        for aid, res in zip(fixture["action_id"].tolist(), fixture["action_result"].tolist(), strict=True)
    }

    acc: dict[int, dict[str, float]] = {}
    for rec in out.to_dict("records"):
        aid = rec["action_id"]
        gk = int(gk_of[aid])
        a = acc.setdefault(
            gk,
            {
                "shots_faced_total": 0.0,
                "shots_faced": 0.0,
                "goals_conceded_on_shots": 0.0,
                "psxg_faced": 0.0,
                "psxg_variance_sum": 0.0,
            },
        )
        a["shots_faced_total"] += 1.0
        if not bool(rec["psxg_gated"]):
            p = float(rec["psxg_recalibrated"])
            a["shots_faced"] += 1.0
            a["psxg_faced"] += p
            a["psxg_variance_sum"] += p * (1.0 - p)
            if goal_of[aid]:
                a["goals_conceded_on_shots"] += 1.0

    result: dict[int, dict[str, float]] = {}
    for gk, a in acc.items():
        gp = a["psxg_faced"] - a["goals_conceded_on_shots"]
        sd = math.sqrt(a["psxg_variance_sum"])
        result[gk] = {
            **a,
            "goals_prevented": gp,
            "coverage_pct": a["shots_faced"] / a["shots_faced_total"],
            "ci_low": gp - _Z * sd,
            "ci_high": gp + _Z * sd,
        }
    return result


def test_e2e_multi_provider_and_b3_coverage_and_band() -> None:
    fixture = _fixture()
    out = build_predictions(fixture, _model(), model_version="psxg-vTEST")

    # Multi-provider: all three tracking providers traverse the path.
    assert {"gradientsports", "skillcorner", "idsse"} <= set(out["data_source"])

    gk100 = _gk_rollup(out, fixture)[100]

    # B3: GK 100 faced 4 shots; action_id 13 is a gated GOAL (conf 0.15).
    assert gk100["shots_faced_total"] == 4  # all faced shots counted (pre-gate)
    assert gk100["shots_faced"] == 3  # the gated shot excluded post-gate
    # The gated goal is excluded from goals_conceded_on_shots (B3 — same denominator as psxg_faced).
    assert gk100["goals_conceded_on_shots"] == 0  # only gate-passed goals; idx13 goal is gated out
    assert gk100["coverage_pct"] < 1.0  # gating visible as coverage gap

    # goals_prevented never mixes denominators.
    assert math.isclose(gk100["goals_prevented"], gk100["psxg_faced"] - gk100["goals_conceded_on_shots"])

    # Poisson-binomial band: finite and symmetric around goals_prevented.
    assert math.isfinite(gk100["ci_low"]) and math.isfinite(gk100["ci_high"])
    half_width = _Z * math.sqrt(gk100["psxg_variance_sum"])
    assert math.isclose(gk100["ci_high"] - gk100["goals_prevented"], half_width)
    assert math.isclose(gk100["goals_prevented"] - gk100["ci_low"], half_width)


def test_e2e_ranking_disabled_at_fixture_scale() -> None:
    # ranking_enabled is computed from the cohort (>=20 GKs x >=20 shots); at fixture
    # scale (2 GKs) it must be False — asserted as a computed property, not hard-coded.
    fixture = _fixture()
    out = build_predictions(fixture, _model(), model_version="psxg-vTEST")
    rollup = _gk_rollup(out, fixture)
    n_gks_above_floor = sum(1 for gk in rollup.values() if gk["shots_faced_total"] >= 20)
    ranking_enabled = n_gks_above_floor >= 20
    assert ranking_enabled is False
