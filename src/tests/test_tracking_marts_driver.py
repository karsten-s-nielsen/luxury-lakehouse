"""Unit tests for ``ingestion.tracking_marts_driver.read_and_build_unit_inputs`` (Task 3, ADR-037).

The per-unit read + build was the LOOP BODY of ``iter_unit_inputs``; it is factored out so a per-unit
drain processor builds inputs one unit at a time. The live Spark reads (``_read_unit``) and the oriented
conversion (``build_unit_inputs`` -> silly-kicks) are validated in Part B; these tests fake both to cover
the NEW behaviour on the seam: the empty-unit guard now RETURNS ``None`` (was ``continue``), and the
tracking ``FrameBundle`` + xT-grid wiring is passed through unchanged.
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.unit_inputs import UnitInputs
from analytics.action_context.work_unit import WorkUnit
from ingestion import tracking_marts_driver as drv


def test_returns_none_for_empty_tracking(monkeypatch) -> None:
    """No tracking frames -> None, and build_unit_inputs is NEVER called (mirrors the old continue)."""
    monkeypatch.setattr(drv, "_read_unit", lambda *a: (pd.DataFrame(), pd.DataFrame({"period_id": [1]}), object()))

    def _no_build(*a, **k):
        raise AssertionError("build_unit_inputs must not run for an empty unit")

    monkeypatch.setattr(drv, "build_unit_inputs", _no_build)

    unit = WorkUnit(provider="idsse", match_id="M1", period=1)
    assert drv.read_and_build_unit_inputs(None, "cat", unit, xt_grid_data=[[0.0]], xt_l=1, xt_w=1) is None


def test_returns_none_for_empty_actions(monkeypatch) -> None:
    """Frames present but no actions -> also None (either empty leg short-circuits)."""
    monkeypatch.setattr(drv, "_read_unit", lambda *a: (pd.DataFrame({"x": [1]}), pd.DataFrame(), object()))

    def _no_build(*a, **k):
        raise AssertionError("build_unit_inputs must not run for an empty unit")

    monkeypatch.setattr(drv, "build_unit_inputs", _no_build)

    unit = WorkUnit(provider="idsse", match_id="M1", period=1)
    assert drv.read_and_build_unit_inputs(None, "cat", unit, xt_grid_data=[[0.0]], xt_l=1, xt_w=1) is None


def test_builds_inputs_with_tracking_bundle_and_grid(monkeypatch) -> None:
    """Non-empty unit -> build_unit_inputs receives the tracking FrameBundle + the passed xT grid."""
    trk = pd.DataFrame({"x": [1, 2]})
    acts = pd.DataFrame({"period_id": [1, 1]})
    meta = object()
    captured: dict = {}

    def _build(wu, *, frame_bundle, actions_df, meta, xt_grid_data, xt_l, xt_w):
        captured.update(
            wu=wu,
            tier=frame_bundle.tier,
            frames=frame_bundle.frames,
            actions=actions_df,
            meta=meta,
            grid=xt_grid_data,
            xt_l=xt_l,
            xt_w=xt_w,
        )
        return UnitInputs(actions=actions_df, frames=frame_bundle.frames, xt="XT")

    monkeypatch.setattr(drv, "_read_unit", lambda spark, catalog, provider, match_id, period: (trk, acts, meta))
    monkeypatch.setattr(drv, "build_unit_inputs", _build)

    unit = WorkUnit(provider="skillcorner", match_id="M9", period=2)
    result = drv.read_and_build_unit_inputs("spark", "cat", unit, xt_grid_data=[[0.5]], xt_l=3, xt_w=4)

    assert isinstance(result, UnitInputs)
    assert result.xt == "XT"
    assert captured["wu"] is unit
    assert captured["tier"] == "tracking"
    assert captured["frames"] is trk
    assert captured["actions"] is acts
    assert captured["meta"] is meta
    assert (captured["grid"], captured["xt_l"], captured["xt_w"]) == ([[0.5]], 3, 4)
