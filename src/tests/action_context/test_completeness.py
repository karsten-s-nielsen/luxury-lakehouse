"""Unit tests for the per-unit action-completeness invariant (ADR-040 amendment)."""

from __future__ import annotations

import pytest

from analytics.action_context.completeness import (
    MIN_EXPECTED_ACTIONS_FOR_CHECK,
    MIN_UNIT_ACTION_COVERAGE,
    assert_unit_action_completeness,
    expected_actions_within_coverage,
)


def test_full_coverage_passes() -> None:
    assert_unit_action_completeness(emitted=536, expected=536, unit_desc="skillcorner:1886347:2")


def test_silent_drop_raises_with_unit_key_and_counts() -> None:
    # The real SkillCorner P2 numbers: 65 of 536 emitted (12.1%) by a "successful" unit.
    with pytest.raises(RuntimeError, match=r"skillcorner:1886347:2.*65 of 536"):
        assert_unit_action_completeness(emitted=65, expected=536, unit_desc="skillcorner:1886347:2")


def test_margin_absorbs_small_edge_losses() -> None:
    # 96% > the 95% floor — a couple of frame-gap actions do not fail the unit.
    assert_unit_action_completeness(emitted=96, expected=100, unit_desc="idsse:J03WMX:1")


def test_just_below_floor_raises() -> None:
    with pytest.raises(RuntimeError, match=r"94\.0%"):
        assert_unit_action_completeness(emitted=94, expected=100, unit_desc="idsse:J03WMX:1")


def test_zero_expected_skips() -> None:
    assert_unit_action_completeness(emitted=0, expected=0, unit_desc="metrica:Sample_Game_1:2")


def test_tiny_samples_skip_the_check() -> None:
    # M13 boundary ambiguity: a small slice's window-interior action can be owned by
    # the adjacent batch outside the slice (the dead-ball fixtures emit 0 of 1 by
    # design) — below MIN_EXPECTED_ACTIONS_FOR_CHECK the invariant must not fire.
    assert_unit_action_completeness(emitted=0, expected=MIN_EXPECTED_ACTIONS_FOR_CHECK - 1, unit_desc="idsse:J03WN1:1")


def test_window_interior_counting_excludes_edge_zone() -> None:
    # Window [0, 100] with 0.5s buffer → interior [0.5, 99.5]: edge actions excluded.
    n = expected_actions_within_coverage(
        action_times_by_period={1: [0.1, 0.5, 50.0, 99.5, 99.9]},
        frame_window_by_period={1: (0.0, 100.0)},
        buffer_s=0.5,
    )
    assert n == 3  # 0.5, 50.0, 99.5 are interior (inclusive); 0.1 and 99.9 are edge-zone


def test_floor_constant_is_strict() -> None:
    # The floor exists to catch data loss; it must stay close to 1 — loosening it
    # is a deliberate, reviewed decision, not a convenience edit.
    assert MIN_UNIT_ACTION_COVERAGE >= 0.95
