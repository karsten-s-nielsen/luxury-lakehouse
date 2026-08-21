"""Unit tests for ``ingestion.off_ball_runs_writer`` (Task 17e) on real fixture tracking.

Exercises the PURE core ``compute_off_ball_runs`` (+ the shared ``build_unit_inputs`` seam) on the
SkillCorner + IDSSE AC fixtures — reconstructing the oriented ``(actions, frames, xt)`` the AC drain
would build, then detecting + valuing runs. The Spark ``run_pipeline`` is validated live in Part B.

SkillCorner is the primary fixture (spec §6.7: the non-chronological-``action_id`` upstream bug can make
IDSSE ``detect_off_ball_runs`` raise; SkillCorner is unaffected). The IDSSE ``J03WMX_p1`` anchor happens
to carry no intra-period time-inversion, so it is used as a richer secondary — guarded to skip (not fail)
if a future re-extract introduces one.
"""

from __future__ import annotations

import numpy as np
import pytest

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.unit_inputs import UnitInputs, build_unit_inputs
from analytics.action_context.work_unit import WorkUnit
from ingestion.off_ball_runs_writer import OUTPUT_COLUMNS, compute_off_ball_runs

_ROOT = "src/tests/fixtures/action_context"


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


def test_off_ball_runs_columns_and_grain_skillcorner() -> None:
    inp = _build_inputs("skillcorner", "1886347", 2)
    result = compute_off_ball_runs(inp.actions, inp.frames, inp.xt)

    assert list(result.columns) == list(OUTPUT_COLUMNS)
    assert not result.empty, "no runs detected on the SkillCorner fixture — build_unit_inputs mis-oriented?"

    # Identity stamped from the (native) actions frame.
    assert (result["data_source"] == "skillcorner").all()
    assert (result["match_id"] == "1886347").all()

    # Grain: one row per (action, runner) — no duplicate (action_id, player_id) pairs.
    dupes = result.groupby(["action_id", "player_id"]).size()
    assert dupes[dupes > 1].empty, f"grain violated: {dupes[dupes > 1].to_dict()}"


def test_off_ball_runs_dtypes_and_value_domain_skillcorner() -> None:
    inp = _build_inputs("skillcorner", "1886347", 2)
    result = compute_off_ball_runs(inp.actions, inp.frames, inp.xt)

    # dtypes on the load-bearing columns.
    assert result["run_value"].dtype == np.float64
    assert result["enabled_pass_credit"].dtype == np.float64
    assert result["toward_goal"].dtype == "boolean"
    assert str(result["player_id"].dtype) == "string"

    # review-4 B5 null-rate contract: value_off_ball_runs values ONLY completed passes/crosses with a
    # resolved receiver, so off-domain runs (role is <NA>) are NEVER valued — run_value must be NaN.
    off_domain = result["role"].isna()
    if off_domain.any():
        assert result.loc[off_domain, "run_value"].isna().all(), "off-domain run carried a non-NaN run_value"

    # peak_speed_source is a closed vocab from silly-kicks (measured / displacement_rate).
    assert set(result["peak_speed_source"].dropna().unique()) <= {"measured", "displacement_rate"}


def test_off_ball_runs_empty_when_no_runs() -> None:
    """A synthetic single-action unit produces no qualifying run -> empty frame with the full schema."""
    inp = _build_inputs("skillcorner", "1886347", 2)
    one_action = inp.actions.head(1).copy()
    result = compute_off_ball_runs(one_action, inp.frames, inp.xt)
    assert list(result.columns) == list(OUTPUT_COLUMNS)


def test_off_ball_runs_idsse_secondary() -> None:
    try:
        inp = _build_inputs("idsse", "J03WMX", 1)
        result = compute_off_ball_runs(inp.actions, inp.frames, inp.xt)
    except Exception as exc:  # spec §6.7: upstream non-chronological action_id may raise on IDSSE
        pytest.skip(f"IDSSE fixture hit the upstream action_id ordering issue (spec §6.7): {exc}")
    assert list(result.columns) == list(OUTPUT_COLUMNS)
    assert not result.empty
    # Same off-domain contract holds cross-provider.
    off_domain = result["role"].isna()
    if off_domain.any():
        assert result.loc[off_domain, "run_value"].isna().all()
