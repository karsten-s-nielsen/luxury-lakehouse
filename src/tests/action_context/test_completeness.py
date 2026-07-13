"""Unit tests for the per-unit action-completeness invariant (ADR-040 amendment; ADR-067 re-anchor).

REWRITTEN (not extended) by ADR-067: the old module imported ``MIN_EXPECTED_ACTIONS_FOR_CHECK`` and
called ``assert_unit_action_completeness(expected=...)``, both of which are gone. The expectation is
now the unit's BRONZE action count -- an anchor no frame clock can move -- and the frame window
survives only as a *bounded* excuse for a shortfall.
"""

from __future__ import annotations

import pytest

from analytics.action_context.completeness import (
    MIN_UNIT_ACTION_COVERAGE,
    MISMATCH_OVERLAP_FLOOR,
    assert_unit_action_completeness,
    coverage_overlap_fraction,
    expected_actions_within_coverage,
)

# A healthy full-coverage unit: frames span the period, every action is inside them.
_FULL_TIMES = {2: [float(t) for t in range(0, 536)]}
_FULL_WINDOW = {2: (-1.0, 600.0)}


def test_full_coverage_passes() -> None:
    assert_unit_action_completeness(
        emitted=536,
        bronze_expected=536,
        action_times_by_period=_FULL_TIMES,
        frame_window_by_period=_FULL_WINDOW,
        unit_desc="skillcorner:1886347:2",
    )


def test_silent_drop_raises_with_unit_key_and_counts() -> None:
    """The real SkillCorner P2 numbers: 65 of 536 emitted (12.1%) by a "successful" unit.

    Kept from the original module -- it IS the incident. The frames here are credible (they span the
    period, so the overlap floor is cleared), which means the shortfall cannot be excused and the
    invariant must name the unit and the counts.
    """
    with pytest.raises(RuntimeError, match=r"skillcorner:1886347:2.*65 of 536"):
        assert_unit_action_completeness(
            emitted=65,
            bronze_expected=536,
            action_times_by_period=_FULL_TIMES,
            frame_window_by_period=_FULL_WINDOW,
            unit_desc="skillcorner:1886347:2",
        )


def test_margin_absorbs_small_edge_losses() -> None:
    """96% > the 95% floor — a couple of frame-gap actions do not fail the unit."""
    assert_unit_action_completeness(
        emitted=96,
        bronze_expected=100,
        action_times_by_period={1: [float(t) for t in range(100)]},
        frame_window_by_period={1: (-1.0, 200.0)},
        unit_desc="idsse:J03WMX:1",
    )


def test_just_below_floor_raises() -> None:
    with pytest.raises(RuntimeError, match=r"94\.0%"):
        assert_unit_action_completeness(
            emitted=94,
            bronze_expected=100,
            action_times_by_period={1: [float(t) for t in range(100)]},
            frame_window_by_period={1: (-1.0, 200.0)},
            unit_desc="idsse:J03WMX:1",
        )


def test_zero_expected_skips() -> None:
    assert_unit_action_completeness(
        emitted=0,
        bronze_expected=0,
        action_times_by_period={},
        frame_window_by_period={},
        unit_desc="metrica:Sample_Game_1:2",
    )


# ── ADR-067: the class the old invariant could not see ─────────────────────────────────────────


def test_corrupted_clock_with_intact_bronze_count_raises() -> None:
    """The self-referentiality fix, in one test.

    OLD behaviour: ``expected`` was the count of actions inside the FRAME window. A frame clock
    shifted onto the absolute match clock produced a window covering ~no actions, so ``expected``
    collapsed to ~0 alongside ``emitted`` -- ratio ~1.0, silent PASS.

    NEW behaviour: ``expected`` is the bronze count (60), which the broken clock cannot touch. The
    shortfall is real, and the window is NOT credible enough to excuse it (it overlaps 0% of the
    actions), so the unit fails loudly as UNEXPLAINED.
    """
    with pytest.raises(RuntimeError, match="UNEXPLAINED"):  # case-sensitive: re.search
        assert_unit_action_completeness(
            emitted=0,
            bronze_expected=60,
            action_times_by_period={2: [float(t) for t in range(0, 600, 10)]},
            frame_window_by_period={2: (5000.0, 5600.0)},  # absolute clock: overlaps nothing
            unit_desc="skillcorner:1552423:2",
        )


def test_genuine_sparse_coverage_is_excused() -> None:
    """H4 regression guard — the reason the overlap floor is safe HERE.

    ``time_base_guard.py:15-24`` records that an overlap metric was tried and REJECTED as a PRIMARY
    guard, because it cannot distinguish an offset clock from legitimately sparse coverage. This use
    is different: it only bounds an EXCUSE. Broadcast frames covering half the period, with every
    covered action emitted, must still pass.
    """
    assert_unit_action_completeness(
        emitted=30,
        bronze_expected=60,
        action_times_by_period={1: [float(t) for t in range(0, 600, 10)]},
        frame_window_by_period={1: (0.0, 300.0)},  # frames cover ~half the period
        unit_desc="skillcorner:1:1",
    )


def test_nine_covered_actions_zero_emitted_raises() -> None:
    """D4: the old ``expected < MIN_EXPECTED_ACTIONS_FOR_CHECK`` (=10) skip band let this through.

    A broken window clipping a real half-match to 9 covered actions, emitting none, passed silently.
    The production band is now 0 -- every real unit is checked.
    """
    with pytest.raises(RuntimeError):
        assert_unit_action_completeness(
            emitted=0,
            bronze_expected=9,
            action_times_by_period={1: [float(t) for t in range(9)]},
            frame_window_by_period={1: (-1.0, 100.0)},
            unit_desc="p:m:1",
        )


def test_slice_fixture_is_exempt_by_flag_not_by_size() -> None:
    """Fixtures are exempted EXPLICITLY.

    They carry a windowed frame slice but the WHOLE match's actions (the extractor's ``_pull_actions``
    applies no time filter), so ``bronze_expected`` legitimately dwarfs ``emitted``. A SIZE threshold
    would also exempt a 9-action clip of a real half-match -- see the test above.
    """
    assert_unit_action_completeness(
        emitted=0,
        bronze_expected=3,
        action_times_by_period={1: [1.0, 2.0, 3.0]},
        frame_window_by_period={1: (0.0, 5.0)},
        unit_desc="idsse:mini:1",
        is_slice=True,
    )


def test_overlap_fraction_uses_the_unbuffered_window() -> None:
    assert coverage_overlap_fraction({1: [1.0, 2.0, 3.0, 4.0]}, {1: (0.0, 2.5)}) == 0.5
    assert coverage_overlap_fraction({1: [1.0, 2.0]}, {1: (900.0, 1000.0)}) == 0.0
    assert coverage_overlap_fraction({}, {}) == 1.0


def test_window_interior_counting_excludes_edge_zone() -> None:
    # Window [0, 100] with 0.5s buffer → interior [0.5, 99.5]: edge actions excluded.
    n = expected_actions_within_coverage(
        action_times_by_period={1: [0.1, 0.5, 50.0, 99.5, 99.9]},
        frame_window_by_period={1: (0.0, 100.0)},
        buffer_s=0.5,
    )
    assert n == 3  # 0.5, 50.0, 99.5 are interior (inclusive); 0.1 and 99.9 are edge-zone


def test_floor_constants_are_strict() -> None:
    """Loosening either floor is a deliberate, reviewed decision — not a convenience edit."""
    assert MIN_UNIT_ACTION_COVERAGE >= 0.95
    assert MISMATCH_OVERLAP_FLOOR == 0.2  # silly-kicks' MISMATCH_OVERLAP_FLOOR
