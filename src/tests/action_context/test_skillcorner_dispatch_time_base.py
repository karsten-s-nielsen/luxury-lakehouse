"""SkillCorner DISPATCH-level time-base re-base (ADR-040 amendment, 2026-06-11).

bronze.skillcorner_tracking.timestamp is the ABSOLUTE broadcast clock (P2 = 2700s+)
while SPADL action time_seconds is period-relative. The CONVERTER re-base
(_bronze_skillcorner_to_frames, covered by test_skillcorner_frame_time_base) was
fixed in the 4.20.1 cycle — but the DISPATCH layer (per-batch action window filter
+ M13 ownership in enrich_batch) reads the bronze column BEFORE conversion, and
that second consumer was missed: the scoped prod run 1020873732479562 emitted 65
of 536 (12.1%) and 50 of 573 (8.7%) P2 actions as "successful" units, plus 2
duplicate P1 action_ids from the de-aligned ownership map.

This module locks the dispatch re-base (same nominal offsets as the converter —
ONE imported constant) and the cross-driver lockstep, mirroring
test_metrica_period_relative_time's structure.
"""

from __future__ import annotations

import inspect

import pandas as pd
from silly_kicks.spadl.skillcorner import _PERIOD_START_SECONDS as _SKILLCORNER_PERIOD_START_SECONDS


def _rebase(ts: pd.Series, period: pd.Series) -> pd.Series:
    """The shared dispatch re-base both drivers apply (kept in sync by the sentinel below)."""
    return ts.astype("float64") - period.map(_SKILLCORNER_PERIOD_START_SECONDS).fillna(0.0)


def test_dispatch_rebase_is_period_relative() -> None:
    """P2 frames at the absolute broadcast clock re-base to ~0 — the per-batch action
    window then overlaps the period-relative actions instead of dropping ~90% of them."""
    from analytics.action_context.time_base_guard import assert_frames_time_base

    frames = pd.DataFrame(
        {
            "period": [1, 1, 2, 2],
            # Real shape from bronze 1886347: P1 0.0..2777.9 (abs == relative), P2 2700.0..5823.8.
            "timestamp": [0.0, 2777.9, 2700.0, 5823.8],
        }
    )
    ts = _rebase(frames["timestamp"], frames["period"])
    assert float(ts[frames["period"] == 1].min()) == 0.0
    assert float(ts[frames["period"] == 2].min()) == 0.0  # was 2700 → the silent-drop cause
    assert float(ts[frames["period"] == 2].max()) < 3200.0  # ≈ period length + stoppage
    per_period_min = {int(p): float(ts[frames["period"] == p].min()) for p in frames["period"].unique()}
    assert_frames_time_base(per_period_min)  # must not raise post-rebase


def test_dispatch_rebase_matches_converter_offsets() -> None:
    """Dispatch and converter MUST subtract identical offsets or the batch window and the
    converted frames de-align. Both import _SKILLCORNER_PERIOD_START_SECONDS — assert the
    constant covers regulation + ET + shootout with silly-kicks' nominal starts."""
    assert _SKILLCORNER_PERIOD_START_SECONDS == {1: 0.0, 2: 45 * 60.0, 3: 90 * 60.0, 4: 105 * 60.0, 5: 120 * 60.0}


def test_rebase_present_in_both_dispatchers() -> None:
    """Lockstep: BOTH dispatchers must apply the SkillCorner re-base via the shared
    constant, or the path that drifts silently drops period>=2 actions again."""
    from analytics.action_context import pipeline
    from ingestion import action_context

    local_src = inspect.getsource(pipeline.run_work_unit)
    assert 'wu.provider == "skillcorner"' in local_src
    assert "_SKILLCORNER_PERIOD_START_SECONDS" in local_src

    spark_src = inspect.getsource(action_context._process_tracking_match)
    assert 'provider == "skillcorner"' in spark_src
    assert "_SKILLCORNER_PERIOD_START_SECONDS" in spark_src


def test_frames_side_guard_present_in_both_dispatchers() -> None:
    """Lockstep for the two-sided guard: both dispatchers call assert_frames_time_base
    AFTER the provider re-bases, so the NEXT provider with an absolute frames clock
    fails loud at dispatch instead of silently filtering (this class's third member
    must be its last)."""
    from analytics.action_context import pipeline
    from ingestion import action_context

    assert "assert_frames_time_base" in inspect.getsource(pipeline.run_work_unit)
    assert "assert_frames_time_base" in inspect.getsource(action_context._process_tracking_match)


def test_completeness_invariant_present_in_both_dispatchers() -> None:
    """Lockstep for the per-unit completeness invariant (the deepest net: ANY future
    silent action drop becomes a loud unit failure)."""
    from analytics.action_context import pipeline
    from ingestion import action_context

    assert "assert_unit_action_completeness" in inspect.getsource(pipeline.run_work_unit)
    assert "assert_unit_action_completeness" in inspect.getsource(action_context._process_tracking_match)
