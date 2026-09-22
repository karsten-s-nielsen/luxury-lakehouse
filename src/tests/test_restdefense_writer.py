"""Unit tests for ``ingestion.restdefense_writer`` (sk4118 P1 Phase E) on real fixture tracking.

Exercises the PURE core ``compute_rest_defense_samples`` (+ the shared ``build_unit_inputs`` seam) on the
SkillCorner AC fixture — reconstructing the oriented ``(actions, frames, xt)`` the AC drain builds, then
scoring restdefense L1 (always) + L2 (with a fitted xt). The Spark drain dispatch is validated live in
Part B.
"""

from __future__ import annotations

import numpy as np
import pytest

pytest.importorskip("silly_kicks")

from analytics.action_context.local.parquet_sources import (
    ParquetActionsSource,
    ParquetFrameSource,
    ParquetMatchMetadataSource,
    ParquetXtSource,
)
from analytics.action_context.unit_inputs import UnitInputs, build_unit_inputs
from analytics.action_context.work_unit import WorkUnit
from ingestion.restdefense_writer import (
    _SK_SAMPLE_COLUMNS,
    OUTPUT_COLUMNS,
    compute_rest_defense_samples,
)

_ROOT = "src/tests/fixtures/action_context"
_L1 = [
    "rd_num_superiority",
    "rd_num_superiority_gk",
    "rd_zone_occupancy",
    "rd_line_height",
    "rd_line_height_relative",
    "rd_compactness_x",
    "rd_width",
    "rd_depth",
    "rd_shape_2_3_vs_3_2",
    "rd_gk_line_height",
    "rd_gk_to_line_distance",
]
_L2 = [
    "rd_attacker_space_control",
    "rd_danger_behind_line",
    "rd_danger_behind_line_gk",
    "rd_gk_coverage_behind_line",
    "rd_gk_reachable_coverage_m2",
]


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


def test_sk_sample_columns_match_live_output() -> None:
    """Drift guard: our pinned sample-column list equals what compute_rest_defense actually emits.

    Non-vacuous — a silly-kicks re-order/rename of the sample table breaks this before it can silently
    mis-map into the bronze schema.
    """
    from silly_kicks.restdefense import compute_rest_defense

    inp = _build_inputs("skillcorner", "1886347", 2)
    samples, _ = compute_rest_defense(inp.actions, inp.frames, xt=inp.xt)
    assert set(samples.columns) == set(_SK_SAMPLE_COLUMNS), (
        f"sk restdefense sample columns drifted: only-sk={sorted(set(samples.columns) - set(_SK_SAMPLE_COLUMNS))} "
        f"only-pinned={sorted(set(_SK_SAMPLE_COLUMNS) - set(samples.columns))}"
    )


def test_columns_grain_and_identity_skillcorner() -> None:
    inp = _build_inputs("skillcorner", "1886347", 2)
    result = compute_rest_defense_samples(inp.actions, inp.frames, inp.xt, access_tier="restricted")

    assert list(result.columns) == list(OUTPUT_COLUMNS)
    assert not result.empty, "no restdefense samples on the SkillCorner fixture — build_unit_inputs mis-oriented?"

    # Identity stamped from the (native) actions frame.
    assert (result["data_source"] == "skillcorner").all()
    assert (result["match_id"] == "1886347").all()
    assert (result["access_tier"] == "restricted").all()

    # Grain: one row per (team_id, action_id) sample.
    dupes = result.groupby(["team_id", "action_id"]).size()
    assert dupes[dupes > 1].empty, f"grain violated: {dupes[dupes > 1].to_dict()}"


def test_team_id_native_resolves() -> None:
    """The DEFENDING team_id maps back to a native id (SkillCorner native == stringified int)."""
    inp = _build_inputs("skillcorner", "1886347", 2)
    result = compute_rest_defense_samples(inp.actions, inp.frames, inp.xt, access_tier="restricted")
    # Every sampled team is one of the match's two teams -> the per-match map resolves it (no NaN).
    assert result["team_id_native"].notna().all(), "a defending team_id failed to map to a native id"
    assert set(result["team_id_native"].unique()) <= set(inp.actions["team_id_native"].astype(str).unique())


def test_layer2_is_nan_without_xt_and_finite_with_xt() -> None:
    """L1 is byte-identical with/without xt; L2 is all-NaN without a fitted xt, populated with one."""
    from silly_kicks.restdefense import compute_rest_defense

    inp = _build_inputs("skillcorner", "1886347", 2)
    no_xt, _ = compute_rest_defense(inp.actions, inp.frames, xt=None)
    with_xt, _ = compute_rest_defense(inp.actions, inp.frames, xt=inp.xt)

    assert not with_xt.empty
    # L2 all-NaN without a fitted xt (numeric L2 cols only — rd_* L2 are all floats).
    for col in _L2:
        assert no_xt[col].isna().all(), f"{col} was populated without a fitted xt"
    # At least one L2 col carries a finite value once xt is injected (non-vacuity).
    assert any(np.isfinite(with_xt[col].to_numpy(dtype=float)).any() for col in _L2), (
        "L2 never populated with a fitted xt"
    )

    # L1 descriptive cols do not depend on xt — identical numeric columns either way.
    for col in _L1:
        if col == "rd_shape_2_3_vs_3_2":
            continue  # categorical (object) — compared as-is
        a = no_xt[col].to_numpy(dtype=float)
        b = with_xt[col].to_numpy(dtype=float)
        np.testing.assert_array_equal(np.isnan(a), np.isnan(b))
        mask = ~np.isnan(a)
        np.testing.assert_allclose(a[mask], b[mask], rtol=0, atol=1e-9)


def test_dtypes_and_metric_domains() -> None:
    inp = _build_inputs("skillcorner", "1886347", 2)
    result = compute_rest_defense_samples(inp.actions, inp.frames, inp.xt, access_tier="public")

    assert str(result["team_id_native"].dtype) == "object"  # native string ids
    # Count metrics are integer-typed (Int64 nullable); coverage areas are non-negative where present.
    for col in ("rd_gk_reachable_coverage_m2", "rd_gk_coverage_behind_line"):
        vals = result[col].to_numpy(dtype=float)
        finite = vals[np.isfinite(vals)]
        assert (finite >= 0).all(), f"{col} has a negative area"
