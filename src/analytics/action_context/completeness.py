"""Per-unit action-completeness invariant (ADR-040 amendment).

The action-context contract is ONE output row per SPADL action of the work
unit. Nothing previously asserted it: a unit whose dispatch silently dropped
rows (a mis-based clock, an over-eager filter, a future variant of either)
terminated as *processed* with no alarm — the SkillCorner period-2 incident
(2026-06-11) shipped 12% of a half's actions as a "successful" unit. This
module converts silent data loss into a loud unit failure.

Shared by both drivers: the local hexagon (``pipeline.run_work_unit``) and the
Spark production driver (``ingestion.action_context._process_tracking_match``),
kept in lockstep by a source-level sentinel.

Pure module — stdlib only.
"""

from __future__ import annotations

# Minimum fraction of the unit's COVERED SPADL actions that must be emitted. The
# pipeline emits a row for EVERY owned action (NaN features still emit the row), so
# healthy units sit at ~100%; the margin absorbs only edge effects (e.g. an action
# in a mid-period tracking gap). Anything below this is data loss.
MIN_UNIT_ACTION_COVERAGE: float = 0.95

# Below this many covered actions the check is skipped: the invariant is a BULK
# data-loss detector, not a per-action guarantee. At slice edges, M13 ownership can
# legitimately assign a window-interior action to the adjacent batch OUTSIDE a small
# slice (the dead-ball fixtures emit 0 of 1 by design), so tiny samples are
# structurally ambiguous. Full prod halves carry hundreds of actions — far above this.
MIN_EXPECTED_ACTIONS_FOR_CHECK: int = 10


def expected_actions_within_coverage(
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
    buffer_s: float = 0.5,
) -> int:
    """Count the unit's actions strictly INTERIOR to the frames' per-period coverage.

    The completeness contract is "every action the frames COVER must be emitted" —
    relativizing to the frames' window keeps the invariant exact for full prod halves
    (window spans the half → all actions expected) while staying valid for slice
    fixtures and partial broadcast coverage (only the covered actions expected).
    The window is SHRUNK by ``buffer_s`` on each edge (mirror of the dispatch's
    ±_ACTION_TIME_BUFFER_SECONDS): an action within the edge zone can be owned by
    the neighboring batch outside the slice, so it is not safely expectable.

    Both inputs must be on the SAME (period-relative) clock — call AFTER the
    dispatcher's provider re-bases, which the frames-side guard already enforces.
    """
    expected = 0
    for period, times in action_times_by_period.items():
        window = frame_window_by_period.get(period)
        if window is None:
            continue
        lo, hi = window[0] + buffer_s, window[1] - buffer_s
        expected += sum(1 for t in times if lo <= t <= hi)
    return expected


def assert_unit_action_completeness(
    *,
    emitted: int,
    expected: int,
    unit_desc: str,
    min_coverage: float = MIN_UNIT_ACTION_COVERAGE,
) -> None:
    """Raise ``RuntimeError`` when a unit emitted fewer rows than its action count allows.

    Parameters
    ----------
    emitted : int
        Rows written for the unit (one per enriched action).
    expected : int
        SPADL actions belonging to the unit (the match, period-filtered when the
        unit is a half). ``0`` skips the check — nothing to lose.
    unit_desc : str
        ``provider:match:period`` for the error message (ADR-002 §5: the group key
        travels with the exception).
    min_coverage : float
        Override for callers with a documented reason; defaults to
        ``MIN_UNIT_ACTION_COVERAGE``.

    Raises
    ------
    RuntimeError
        If ``emitted < min_coverage * expected`` (and ``expected`` is at least
        ``MIN_EXPECTED_ACTIONS_FOR_CHECK`` — tiny samples are M13-boundary-ambiguous).
    """
    if expected < MIN_EXPECTED_ACTIONS_FOR_CHECK:
        return
    coverage = emitted / expected
    if coverage >= min_coverage:
        return
    raise RuntimeError(
        f"action-context completeness violated for {unit_desc}: emitted {emitted} of "
        f"{expected} SPADL actions ({coverage:.1%} < {min_coverage:.0%}). The unit silently "
        "dropped actions — check the dispatch time-base (ADR-040) and batch window filters "
        "before re-running; do NOT accept this unit's output."
    )
