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

import math

# Earliest-action time above which a period's actions are deemed to be on an ABSOLUTE
# match clock rather than period-relative. A real period-relative half/ET-half has its
# first action within ~1 min of kickoff (min time_seconds ≈ 0); an absolute-clock period
# p>=2 starts at its nominal offset (period 2 = 2700 s, period 3 = 5400 s, …). 1800 s
# (30 min) sits comfortably between: no real period-relative period has its first action
# 30 min in, and every absolute-clock period p>=2 exceeds it. Period 1 is exempt by
# construction (absolute == period-relative at offset 0, and was never the bug). See ADR-040.
_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS: float = 1800.0

# D5 (ADR-067) — the floor above is ONE-SIDED, and that left two mis-based clocks passing silently.
# Both empty the per-batch action window exactly like the absolute clock it was built to catch:
#
#   * an OVER-subtracted re-base sits far BELOW the floor, not above it. The documented -2700 s
#     SkillCorner double-subtraction (ADR-040) passes `>= 1800.0` trivially.
#   * NaN passes too, because `float("nan") >= 1800.0` is False. A lower bound alone does NOT catch
#     it -- NaN compares False against every bound -- so it must be rejected explicitly.
#
# The lower bound is generous: a period-relative clock may legitimately start a hair below zero
# (float noise, or an action a fraction of a second before the nominal kickoff frame). Only a LARGE
# negative min indicates an over-subtracted re-base.
_PERIOD_RELATIVE_MIN_FLOOR_SECONDS: float = -60.0


def _offending_periods(period_min: dict[int, float]) -> dict[int, float]:
    """Periods whose earliest time is NaN, over-subtracted, or on an absolute match clock."""
    return {
        p: m
        for p, m in period_min.items()
        if math.isnan(m) or m < _PERIOD_RELATIVE_MIN_FLOOR_SECONDS or m >= _ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS
    }


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
    offenders = _offending_periods(action_period_min)
    if not offenders:
        return
    detail = ", ".join(f"period {p}: earliest action t={m:.0f}s" for p, m in sorted(offenders.items()))
    # Hard-fail-first at the work-unit boundary (ADR-002 §5): a silently mis-based period is
    # worse than a work unit that fails loud and quarantines.
    raise ValueError(
        "assert_work_unit_time_base: actions are not on silly-kicks' canonical period-relative "
        f"time_seconds ({detail}; valid range [{_PERIOD_RELATIVE_MIN_FLOOR_SECONDS:.0f}s, "
        f"{_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS:.0f}s), NaN rejected). Too HIGH => an ABSOLUTE match clock "
        "(a period-relative period's first action starts near 0; an absolute clock starts at the period "
        "offset, e.g. 2700 s for period 2). Too LOW => an OVER-subtracted re-base (the -2700 s class). "
        "NaN => missing timestamps. See ADR-040 / ADR-067."
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
    offenders = _offending_periods(frame_period_min)
    if not offenders:
        return
    detail = ", ".join(f"period {p}: earliest frame t={m:.0f}s" for p, m in sorted(offenders.items()))
    # Hard-fail-first at the work-unit boundary (ADR-002 §5): a frames-side mis-based clock silently
    # empties the per-batch action window — fail the unit loud instead.
    raise ValueError(
        "assert_frames_time_base: tracking frames are not on silly-kicks' canonical period-relative "
        f"clock after the provider re-bases ({detail}; valid range "
        f"[{_PERIOD_RELATIVE_MIN_FLOOR_SECONDS:.0f}s, {_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS:.0f}s), NaN "
        "rejected). Too HIGH => an ABSOLUTE match clock. Too LOW => an OVER-subtracted re-base (the "
        "-2700 s class). NaN => missing timestamps. The per-batch action window filter would silently "
        "drop most period>=2 actions (the SkillCorner P2 class). Add/fix the provider's dispatch-level "
        "timestamp re-base. See ADR-040 amendment / ADR-067."
    )
