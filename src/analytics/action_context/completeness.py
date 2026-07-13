"""Per-unit action-completeness invariant (ADR-040 amendment; re-anchored by ADR-067).

The action-context contract is ONE output row per SPADL action of the work unit. Nothing asserted
it until ADR-040: a unit whose dispatch silently dropped rows (a mis-based clock, an over-eager
filter) terminated as *processed* with no alarm -- the SkillCorner period-2 incident (2026-06-11)
shipped 12% of a half's actions as a "successful" unit.

**Why ADR-067 re-anchored it.** The original expectation was ``expected_actions_within_coverage``:
the actions falling inside the FRAMES' time window. But that window is derived from the very frame
clock the guard is meant to police, and the dispatch filter selects actions with the same quantity.
A corrupted clock therefore shrank ``emitted`` and ``expected`` *together*, holding the ratio at
~1.0 -- the invariant validated its output against an expectation derived from its corrupted input,
and was structurally incapable of detecting the class it was written for.

The expectation is now the unit's **bronze SPADL action count**, which no frame clock can move. The
frame window survives only as an *excuse* for a shortfall -- and that excuse is itself bounded (see
``MISMATCH_OVERLAP_FLOOR``), because a broken window would otherwise "explain" the very shortfall it
caused.

Shared by both drivers: the local hexagon (``pipeline.run_work_unit``) and the Spark production
driver (``ingestion.action_context._process_tracking_match``), kept in lockstep by a source-level
sentinel.

Pure module -- stdlib only.
"""

from __future__ import annotations

# Minimum fraction of the unit's SPADL actions that must be emitted. The pipeline emits a row for
# EVERY owned action (NaN features still emit the row), so healthy units sit at ~100%; the margin
# absorbs only edge effects (e.g. an action in a mid-period tracking gap).
MIN_UNIT_ACTION_COVERAGE: float = 0.95

# Minimum fraction of a unit's actions that the frame window must overlap before a window-based
# EXCUSE for a shortfall is believed. Adopted from silly-kicks' MISMATCH_OVERLAP_FLOOR
# (silly_kicks/tracking/utils.py:28).
#
# WHY THIS IS SAFE HERE, when time_base_guard.py:15-24 records that an OVERLAP metric was tried and
# REJECTED: that rejection was for a PRIMARY guard, which must distinguish an offset clock from
# legitimately sparse frame coverage -- and it cannot, because both yield low overlap. This use is
# different in kind. It never raises on its own. It only decides whether a window is credible enough
# to EXCUSE a shortfall that the (clock-independent) bronze count has already proven real. A unit
# that emitted everything its frames cover never reaches the excuse path at all, so sparse-but-
# correct broadcast coverage still passes. Pinned by test_genuine_sparse_coverage_is_excused.
MISMATCH_OVERLAP_FLOOR: float = 0.2


def coverage_overlap_fraction(
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
) -> float:
    """Fraction of the unit's actions lying inside the frames' UNBUFFERED per-period window.

    A credibility check on the WINDOW -- never the expectation. A window on a broken clock overlaps
    ~0% of the unit's actions, and must not be allowed to excuse the shortfall it caused.
    """
    total = sum(len(t) for t in action_times_by_period.values())
    if total == 0:
        return 1.0
    inside = 0
    for period, times in action_times_by_period.items():
        window = frame_window_by_period.get(period)
        if window is None:
            continue
        lo, hi = window
        inside += sum(1 for t in times if lo <= t <= hi)
    return inside / total


def expected_actions_within_coverage(
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
    buffer_s: float = 0.5,
) -> int:
    """Count the unit's actions strictly INTERIOR to the frames' per-period coverage.

    Used ONLY to explain a shortfall, never to define the expectation (that is ``bronze_expected``).
    The window is SHRUNK by ``buffer_s`` on each edge, mirroring the dispatch's ownership buffer: an
    action in the edge zone can be owned by the neighbouring batch outside the slice.

    Both inputs must be on the SAME (period-relative) clock -- call AFTER the dispatcher's provider
    re-bases, which the frames-side guard already enforces.
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
    bronze_expected: int,
    action_times_by_period: dict[int, list[float]],
    frame_window_by_period: dict[int, tuple[float, float]],
    unit_desc: str,
    buffer_s: float = 0.5,
    min_coverage: float = MIN_UNIT_ACTION_COVERAGE,
    is_slice: bool = False,
) -> None:
    """Raise ``RuntimeError`` when a unit emitted fewer rows than its BRONZE action count allows.

    Two levels (ADR-067):

    1. ``bronze_expected`` -- the unit's SPADL action count -- is the expectation. It is independent
       of the frame clock, so a corrupted clock cannot shrink it in lockstep with ``emitted``.
    2. A shortfall MAY be excused when the missing actions lie outside frame coverage -- but only if
       the window is CREDIBLE, i.e. it overlaps at least ``MISMATCH_OVERLAP_FLOOR`` of the unit's
       actions. Otherwise the shortfall is UNEXPLAINED and we raise.

    Parameters
    ----------
    emitted : int
        Rows written for the unit (one per enriched action).
    bronze_expected : int
        SPADL actions belonging to the unit (period-filtered when the unit is a half).
    unit_desc : str
        ``provider:match:period`` for the error message (ADR-002 §5: the group key travels with the
        exception).
    is_slice : bool
        Exempt a test/golden FIXTURE. Fixtures are extracted with a windowed frame slice but the
        WHOLE match's actions (``extract_action_context_fixture._pull_actions`` applies no time
        filter), so ``bronze_expected`` legitimately dwarfs ``emitted``.

        This is an EXPLICIT flag, never a size threshold: a threshold small enough to exempt a
        3-action fixture also exempts a 9-action clip of a REAL half-match, which is exactly the
        silent skip this guard exists to prevent. (The former ``MIN_EXPECTED_ACTIONS_FOR_CHECK = 10``
        did precisely that.) The proper fix is for the extractor to slice actions to the frame
        window too, at which point this flag can go -- see ADR-067.

    Raises
    ------
    RuntimeError
        If the unit emitted too few rows and the shortfall cannot be credibly explained by the
        frames' coverage.
    """
    if is_slice or bronze_expected == 0:
        return

    if emitted >= min_coverage * bronze_expected:
        return  # the whole unit was emitted -- nothing to explain

    overlap = coverage_overlap_fraction(action_times_by_period, frame_window_by_period)
    if overlap < MISMATCH_OVERLAP_FLOOR:
        raise RuntimeError(
            f"action-context completeness violated for {unit_desc}: emitted {emitted} of "
            f"{bronze_expected} bronze SPADL actions, and the shortfall is UNEXPLAINED -- the frame "
            f"window overlaps only {overlap:.1%} of the unit's actions (floor "
            f"{MISMATCH_OVERLAP_FLOOR:.0%}). The tracking clock is not trustworthy, so its coverage "
            "cannot be used to excuse the missing rows. Check the dispatch time-base (ADR-040) "
            "before re-running; do NOT accept this unit's output."
        )

    covered = expected_actions_within_coverage(action_times_by_period, frame_window_by_period, buffer_s=buffer_s)
    if covered and emitted >= min_coverage * covered:
        return  # excused: the missing actions genuinely lie outside a CREDIBLE frame window

    coverage = emitted / covered if covered else 0.0
    raise RuntimeError(
        f"action-context completeness violated for {unit_desc}: emitted {emitted} of {covered} "
        f"SPADL actions ({coverage:.1%} < {min_coverage:.0%}; bronze total {bronze_expected}). The "
        "unit silently dropped actions -- check the dispatch time-base (ADR-040) and batch window "
        "filters before re-running; do NOT accept this unit's output."
    )
