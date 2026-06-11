"""Work-unit time-base alignment guards (ADR-040 + amendment: two-sided).

Asserts, at the AC-1 **work-unit entry**, that BOTH time streams use silly-kicks'
canonical **period-relative** base (seconds since the start of *that* period,
resetting to 0 each period) rather than an absolute match clock:

- ``assert_work_unit_time_base`` — the SPADL **actions** side (the original
  GradientSports period-2 class: GS actions were injected on the absolute clock).
- ``assert_frames_time_base`` — the tracking **frames** side, added after the
  SkillCorner period-2 incident (2026-06-11): SC bronze ``timestamp`` is the
  absolute broadcast clock; the actions were fine, so the one-sided guard passed
  while the per-batch action window silently dropped ~90% of P2 actions. Run it
  AFTER the dispatcher's provider-specific re-bases.

**Min-based, not overlap-based, by design.** An earlier draft compared the action
time range to the frame time range via silly-kicks' ``validate_time_base``, but
that overlap metric cannot distinguish a genuine base mismatch (offset clocks)
from legitimate *sparse/partial* frame coverage — both yield low overlap — so it
false-raised on any period whose frames cover only a slice of the action span
(dead-ball batches, broadcast-tracking gaps). Both guards here key only on each
period's EARLIEST time: a period-relative period starts near its own kickoff
(t ≈ 0); an absolute-clock period ``p >= 2`` starts at its nominal offset
(>= 2700 s for period 2). Sparse coverage barely moves a minimum, so neither
guard fires on partial tracking.

Shared by both drivers: the local hexagon (``pipeline.run_work_unit``) and the
Spark production driver (``ingestion.action_context._process_tracking_match``),
kept in lockstep by a source-level sentinel (the Spark driver is not locally
runnable — ``feedback_test_production_driver_entry_point``).

Pure module — stdlib only, no pandas/Spark/silly-kicks import.
"""

from __future__ import annotations

# Earliest-action time above which a period's actions are deemed to be on an ABSOLUTE
# match clock rather than period-relative. A real period-relative half/ET-half has its
# first action within ~1 min of kickoff (min time_seconds ≈ 0); an absolute-clock period
# p>=2 starts at its nominal offset (period 2 = 2700 s, period 3 = 5400 s, …). 1800 s
# (30 min) sits comfortably between: no real period-relative period has its first action
# 30 min in, and every absolute-clock period p>=2 exceeds it. Period 1 is exempt by
# construction (absolute == period-relative at offset 0, and was never the bug). See ADR-040.
_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS: float = 1800.0


def assert_work_unit_time_base(action_period_min: dict[int, float]) -> None:
    """Raise ``ValueError`` if any period's actions look like an absolute match clock.

    Parameters
    ----------
    action_period_min : dict[int, float]
        ``period_id -> min(time_seconds)`` over that period's actions (NaN-dropped).

    Raises
    ------
    ValueError
        If any period's earliest action is at or beyond
        ``_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS`` — i.e. the actions are on the match
        clock, not silly-kicks' canonical period-relative `time_seconds`.
    """
    offenders = {p: m for p, m in action_period_min.items() if m >= _ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS}
    if not offenders:
        return
    detail = ", ".join(f"period {p}: earliest action t={m:.0f}s" for p, m in sorted(offenders.items()))
    # Hard-fail-first at the work-unit boundary (ADR-002 §5): a silently mis-based period is
    # worse than a work unit that fails loud and quarantines.
    raise ValueError(
        "assert_work_unit_time_base: actions appear to use an ABSOLUTE match clock, not silly-kicks' "
        f"canonical period-relative time_seconds ({detail}; floor {_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS:.0f}s). "
        "A period-relative period's first action starts near 0; an absolute clock starts at the period "
        "offset (e.g. 2700 s for period 2). See ADR-040."
    )


def assert_frames_time_base(frame_period_min: dict[int, float]) -> None:
    """Raise ``ValueError`` if any period's FRAMES look like an absolute match clock.

    Call AFTER the dispatcher's provider-specific timestamp re-bases (GS alias,
    Metrica frame re-base, SkillCorner offset subtraction) — a failure here means a
    provider's re-base is missing or broken, and the per-batch action window would
    silently drop most period>=2 actions (the SkillCorner 2026-06-11 class).

    Parameters
    ----------
    frame_period_min : dict[int, float]
        ``period -> min(timestamp)`` over that period's frames (NaN-dropped).

    Raises
    ------
    ValueError
        If any period's earliest frame time is at or beyond
        ``_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS``.
    """
    offenders = {p: m for p, m in frame_period_min.items() if m >= _ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS}
    if not offenders:
        return
    detail = ", ".join(f"period {p}: earliest frame t={m:.0f}s" for p, m in sorted(offenders.items()))
    # Hard-fail-first at the work-unit boundary (ADR-002 §5): a frames-side absolute
    # clock silently empties the per-batch action window — fail the unit loud instead.
    raise ValueError(
        "assert_frames_time_base: tracking frames appear to use an ABSOLUTE match clock after the "
        f"provider re-bases ({detail}; floor {_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS:.0f}s). The per-batch "
        "action window filter would silently drop most period>=2 actions (the SkillCorner P2 class). "
        "Add/fix the provider's dispatch-level timestamp re-base. See ADR-040 amendment."
    )
