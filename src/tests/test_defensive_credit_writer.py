"""Unit tests for ``ingestion.defensive_credit_writer`` (Tasks 17d + 17f) on real fixture tracking.

Exercises the PURE cores ``compute_action_defensive_credit`` (per-action aggregate) +
``compute_defensive_credit_long`` (per-(action, player, rule)) and the ``attach_xg`` merge on the
SkillCorner + IDSSE AC fixtures. ``fct_shot_xg`` is unavailable in the fixtures, so a synthetic per-shot
xG is injected via ``attach_xg`` (the same LEFT-JOIN the live ``run_pipeline`` runs against
``bronze.xg_shot_predictions``). The Spark ``run_pipeline`` is validated live in Part B.

SkillCorner is the primary fixture (spec §6.7: the upstream non-chronological-``action_id`` bug can make
IDSSE ``add_defensive_credit`` raise); IDSSE ``J03WMX_p1`` is a guarded secondary.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.unit_inputs import UnitInputs, build_unit_inputs
from analytics.action_context.work_unit import WorkUnit
from ingestion.defensive_credit_writer import (
    AGG_OUTPUT_COLUMNS,
    LONG_OUTPUT_COLUMNS,
    attach_xg,
    compute_action_defensive_credit,
    compute_defensive_credit_long,
)

_ROOT = "src/tests/fixtures/action_context"

# silly-kicks 4.87.0 DEFENSIVE_CREDIT_RULES (the closed 10-vocab for the long-form `rule` column).
_RULE_VOCAB = {
    "pressure_on_missed_shot",
    "failed_pressure_shot_on_target",
    "shot_block",
    "pressure_pass_fail",
    "recovery_double_credit",
    "synchronized_final_third_pressure",
    "forced_bad_touch",
    "failed_cross_block",
    "failed_marking_through_ball",
    "beaten_1v1",
}


def _build_inputs(provider: str, match_id: str, period: int) -> UnitInputs:
    wu = WorkUnit(provider=provider, match_id=match_id, period=period)
    grid, xt_l, xt_w = ParquetXtSource(_ROOT).grid()
    return build_unit_inputs(
        wu,
        frame_bundle=ParquetFrameSource(_ROOT).frames(wu),
        actions_df=ParquetActionsSource(_ROOT).actions(wu),
        meta=ParquetMatchMetadataSource(_ROOT).metadata(wu),
        xt_grid_data=grid,
        xt_l=xt_l,
        xt_w=xt_w,
    )


def _actions_with_synthetic_xg(inp: UnitInputs) -> pd.DataFrame:
    """Inject a per-shot xG via the production ``attach_xg`` LEFT-JOIN (fct_shot_xg unavailable here)."""
    import silly_kicks.spadl.config as cfg

    shot_id = cfg.actiontype_id["shot"]
    shots = inp.actions[inp.actions["type_id"] == shot_id]
    xg_preds = pd.DataFrame(
        {
            "data_source": shots["data_source"].to_numpy(),
            "match_id_native": shots["match_id_native"].to_numpy(),
            "action_id": shots["action_id"].to_numpy(),
            "xg": np.full(len(shots), 0.12),
        }
    )
    return attach_xg(inp.actions, xg_preds)


def test_attach_xg_puts_xg_on_shots_only() -> None:
    actions = pd.DataFrame(
        {
            "data_source": ["skillcorner"] * 3,
            "match_id_native": ["1886347"] * 3,
            "action_id": [0, 1, 2],
            "type_id": [0, 1, 0],
        }
    )
    xg_preds = pd.DataFrame(
        {"data_source": ["skillcorner"], "match_id_native": ["1886347"], "action_id": [2], "xg": [0.3]}
    )
    merged = attach_xg(actions, xg_preds)
    assert merged["xg"].tolist()[2] == pytest.approx(0.3)
    assert pd.isna(merged["xg"].iloc[0]) and pd.isna(merged["xg"].iloc[1])
    assert len(merged) == len(actions)  # LEFT join must not fan out


def test_action_defensive_credit_zero_not_nan_skillcorner() -> None:
    inp = _build_inputs("skillcorner", "1886347", 2)
    actions = _actions_with_synthetic_xg(inp)
    agg = compute_action_defensive_credit(actions, inp.frames, inp.xt)

    assert list(agg.columns) == list(AGG_OUTPUT_COLUMNS)
    assert (agg["data_source"] == "skillcorner").all()
    assert (agg["match_id"] == "1886347").all()

    # Grain: one row per action.
    assert not agg["action_id"].duplicated().any()

    # THE domain contract (spec / review-4): no-credit actions are 0.0, NEVER NaN.
    for col in ("defensive_credit_net", "defensive_credit_plus", "defensive_credit_minus"):
        assert agg[col].dtype == np.float64
        assert agg[col].notna().all(), f"{col} carried NaN — must be 0.0 where no credit"
    assert (agg["n_defensive_credits"].fillna(0) >= 0).all()
    # net = plus + minus (aggregate identity).
    assert np.allclose(
        agg["defensive_credit_net"].to_numpy(),
        (agg["defensive_credit_plus"] + agg["defensive_credit_minus"]).to_numpy(),
        atol=1e-9,
    )


def test_defensive_credit_long_schema_and_vocab_skillcorner() -> None:
    inp = _build_inputs("skillcorner", "1886347", 2)
    actions = _actions_with_synthetic_xg(inp)
    long = compute_defensive_credit_long(actions, inp.frames, inp.xt)

    assert list(long.columns) == list(LONG_OUTPUT_COLUMNS)
    assert not long.empty, "no credit attributions on the SkillCorner fixture — mis-oriented inputs?"
    assert (long["data_source"] == "skillcorner").all()
    assert (long["match_id"] == "1886347").all()

    # Closed vocab (a value outside it means an upstream vocab change to fold in, not a test to loosen).
    assert set(long["rule"].dropna().unique()) <= _RULE_VOCAB, set(long["rule"].dropna().unique())

    assert str(long["player_id"].dtype) == "string"
    assert str(long["team_id"].dtype) == "string"
    # signed_value is a DOUBLE that is legitimately NULLABLE: a credit row can fire while its
    # xT/xG-sized magnitude is unresolvable (e.g. the shot's goal-mouth geometry could not be resolved
    # from the frames), so the contract is float64 + no +/-inf — NOT all-finite.
    assert long["signed_value"].dtype == np.float64
    sv = long["signed_value"].to_numpy(dtype=float)
    assert not np.isinf(sv).any(), "signed_value carried +/-inf"

    # Grain is a per-credit-EVENT log, NOT uniquely keyed by (action, player, rule): a single
    # (action, player, rule) can legitimately carry MANY rows (e.g. synchronized_final_third_pressure
    # credits a defender once per synchronized presser). Assert the vocab columns are populated where a
    # row exists (anchor_type / sizing / resolution are closed vocabs, never empty on a real credit).
    for col in ("anchor_type", "sizing", "resolution"):
        assert long[col].notna().any(), f"{col} entirely null — an upstream vocab column dropped?"


def test_defensive_credit_idsse_secondary() -> None:
    try:
        inp = _build_inputs("idsse", "J03WMX", 1)
        actions = _actions_with_synthetic_xg(inp)
        agg = compute_action_defensive_credit(actions, inp.frames, inp.xt)
        long = compute_defensive_credit_long(actions, inp.frames, inp.xt)
    except Exception as exc:  # spec §6.7: upstream non-chronological action_id may raise on IDSSE
        pytest.skip(f"IDSSE fixture hit the upstream action_id ordering issue (spec §6.7): {exc}")
    assert list(agg.columns) == list(AGG_OUTPUT_COLUMNS)
    for col in ("defensive_credit_net", "defensive_credit_plus", "defensive_credit_minus"):
        assert agg[col].notna().all()
    assert list(long.columns) == list(LONG_OUTPUT_COLUMNS)
    assert set(long["rule"].dropna().unique()) <= _RULE_VOCAB
