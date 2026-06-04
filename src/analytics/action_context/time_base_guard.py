"""Work-unit time-base alignment guard (ADR-040).

Asserts, at the AC-1 **work-unit entry**, that the work unit's SPADL actions use
silly-kicks' canonical **period-relative** ``time_seconds`` (seconds since the
start of *that* period, resetting to 0 each period) rather than an absolute match
clock. This is the lakehouse-side guard for the GradientSports period-2 class
(GS actions were injected on the absolute clock; silly-kicks' convention-lock
tests deliberately omit GS because its time originates upstream here — see
silly-kicks ADR-017's scope note).

**Frame-independent by design.** An earlier draft compared the action time range
to the frame time range via silly-kicks' ``validate_time_base``, but that overlap
metric cannot distinguish a genuine base mismatch (offset clocks) from legitimate
*sparse/partial* frame coverage — both yield low overlap — so it false-raised on
any period whose frames cover only a slice of the action span (dead-ball batches,
broadcast-tracking gaps). This check looks only at the actions: a period-relative
period's earliest action starts near its own kickoff (t ≈ 0); an absolute-clock
period ``p >= 2`` starts at that period's nominal offset (>= 2700 s for period 2).
That separation is frame-independent and never fires on sparse tracking.

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
