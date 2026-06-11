"""M13 GLOBAL ownership anchors (ADR-040 amendment 2 follow-up, 2026-06-11).

The per-batch frame↔time fit double-claimed boundary actions on gappy tracking:
SkillCorner broadcast has ~30% of frames missing (1899585 P1: 20,608 distinct
frames over a 29,656 span), so adjacent batches fit slightly different lines and
an action within ~1 frame of a batch boundary got ``est_frame`` 14749.x in one
batch and 14750.0 in the next — BOTH claimed it (duplicate action rows 346/365,
prod runs v2 + v3). The fix: the dispatcher computes ONE per-period anchor
``(t0, f0, slope)`` over the whole unit and every batch evaluates the identical
line — single ownership holds by construction.
"""

from __future__ import annotations

import inspect

import pandas as pd

from analytics.action_context.pipeline import _owned_action_ids, compute_ownership_anchors

_FPS = 10.0  # SkillCorner-like
_BS = 250


def _batch(
    frame_lo: int, frame_hi: int, *, drop: frozenset[int] = frozenset(), jitter_last: float = 0.0
) -> pd.DataFrame:
    """A batch of 10fps frames [frame_lo, frame_hi], optionally with gaps and edge jitter."""
    frames = [f for f in range(frame_lo, frame_hi + 1) if f not in drop]
    return pd.DataFrame(
        {
            "frame": frames,
            "timestamp": [f / _FPS + (jitter_last if f == frames[-1] else 0.0) for f in frames],
            "period": [1] * len(frames),
            "frame_batch_id": [frame_lo // _BS] * len(frames),
        }
    )


def _boundary_action() -> pd.DataFrame:
    # t = 1475.0s → true est_frame = 14750.0 — EXACTLY the batch 58/59 boundary
    # (the real dupes 346/365 sat at frames 14749/14750 and 15999/16000).
    return pd.DataFrame({"action_id": [346], "time_seconds": [1475.0]})


def test_per_batch_fits_double_claim_on_gappy_tracking() -> None:
    """The bug shape: jittered per-batch fits make BOTH adjacent batches claim the action."""
    # Batch 58 (frames 14500..14749): a timestamp wobble at its edge tilts its fit so the
    # action's est_frame lands just BELOW 14750 → batch 58 claims it...
    batch_a = _batch(14500, 14749, jitter_last=+0.01)
    owned_a = _owned_action_ids("skillcorner", batch_a, _boundary_action(), _BS)
    # ...while batch 59 (exact clock) computes est_frame = 14750.0 → batch 59 claims it too.
    batch_b = _batch(14750, 14999)
    owned_b = _owned_action_ids("skillcorner", batch_b, _boundary_action(), _BS)
    assert owned_a == {346} and owned_b == {346}  # the duplicate-ownership bug, reproduced


def test_global_anchor_restores_single_ownership() -> None:
    """With the dispatcher's ONE anchor, exactly one batch claims the boundary action."""
    unit = pd.concat([_batch(14500, 14749, jitter_last=+0.01), _batch(14750, 14999)], ignore_index=True)
    anchors = compute_ownership_anchors(unit, "frame")
    anchor = anchors[1]
    batch_a = _batch(14500, 14749, jitter_last=+0.01)
    owned_a = _owned_action_ids("skillcorner", batch_a, _boundary_action(), _BS, anchor=anchor)
    owned_b = _owned_action_ids("skillcorner", _batch(14750, 14999), _boundary_action(), _BS, anchor=anchor)
    assert (owned_a or set()) | (owned_b or set()) == {346}  # claimed
    assert (owned_a or set()) & (owned_b or set()) == set()  # by exactly one batch


def test_anchor_identical_regardless_of_gaps() -> None:
    """Gaps in the middle of the unit do not change the anchor (it keys on the period's
    earliest/latest timestamps), so ownership is gap-invariant by construction."""
    full = pd.concat([_batch(14500, 14749), _batch(14750, 14999)], ignore_index=True)
    gappy = pd.concat(
        [_batch(14500, 14749, drop=frozenset(range(14600, 14680))), _batch(14750, 14999)], ignore_index=True
    )
    assert compute_ownership_anchors(full, "frame") == compute_ownership_anchors(gappy, "frame")


def test_degenerate_period_omitted() -> None:
    """A period with a single timestamp yields no anchor (legacy no-dedup fallback)."""
    one_frame = pd.DataFrame({"frame": [100], "timestamp": [10.0], "period": [1]})
    assert compute_ownership_anchors(one_frame, "frame") == {}


def test_anchors_present_in_both_dispatchers() -> None:
    """Lockstep: BOTH dispatchers compute + pass the global anchors, or whichever drifts
    re-grows the per-batch-fit duplicate-ownership bug."""
    from analytics.action_context import pipeline
    from ingestion import action_context

    local_src = inspect.getsource(pipeline.run_work_unit)
    assert "compute_ownership_anchors" in local_src
    assert "ownership_anchors" in local_src

    spark_src = inspect.getsource(action_context._process_tracking_match)
    assert "_ownership_anchors" in spark_src
    assert "min_by" in spark_src  # the frame-at-earliest-timestamp agg
