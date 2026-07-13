"""Tests for the AC-1 work-unit time-base alignment guard (ADR-040).

Exercises the shared helper that both drivers (local ``run_work_unit`` + Spark
``_process_tracking_match``) call at work-unit entry. This is the lakehouse-side
guard for the GradientSports period-2 class — the silly-kicks convention-lock
tests deliberately do NOT cover GS (its time_seconds originates upstream here);
this guard + the adapter test (``test_adapt_time_seconds_is_period_relative``)
are that coverage. See silly-kicks ADR-017's scope note.

The guard is frame-INDEPENDENT (checks only that each period's earliest action is
period-relative), so it never false-fires on sparse/partial frame coverage.
"""

from __future__ import annotations

import math

import pytest

from analytics.action_context.time_base_guard import assert_frames_time_base, assert_work_unit_time_base


def test_period_relative_actions_pass() -> None:
    # Earliest action of each period starts near 0 (period-relative) -> no raise.
    assert_work_unit_time_base({1: 5.7, 2: 0.4})


def test_gs_period2_absolute_clock_raises() -> None:
    """GS 10503 p2 on the absolute clock: earliest action at 2700 s -> raise."""
    with pytest.raises(ValueError, match="ABSOLUTE match clock"):
        assert_work_unit_time_base({1: 5.7, 2: 2700.0})


def test_absolute_extra_time_period_raises() -> None:
    # An absolute-clock ET first half starts at 5400 s.
    with pytest.raises(ValueError, match="period 3"):
        assert_work_unit_time_base({3: 5400.0})


def test_period1_absolute_equals_relative_passes() -> None:
    # Period 1 absolute == period-relative (offset 0); never flagged.
    assert_work_unit_time_base({1: 0.0})


def test_sparse_coverage_does_not_false_fire() -> None:
    # The dead-ball regression shape: a full period of period-relative actions whose
    # frames happen to be a narrow window. The guard is frame-independent, so the late
    # MAX is irrelevant — only the (near-zero) MIN matters. Must NOT raise.
    assert_work_unit_time_base({1: 5.7})  # actions spanned [5.7, 2744]; min is what counts


def test_empty_is_noop() -> None:
    assert_work_unit_time_base({})


# ── D5 (ADR-067): the floor was ONE-SIDED and NaN-blind ────────────────────────────────────────
#
# `_ABSOLUTE_CLOCK_MIN_FLOOR_SECONDS` only rejected times at or ABOVE 1800 s. Two mis-based clocks
# therefore passed silently, and both empty the per-batch action window exactly like the absolute
# clock the guard was built for:
#
#   * an OVER-subtracted re-base -- the documented -2700 s SkillCorner double-subtraction (ADR-040)
#     -- sits far BELOW the floor;
#   * NaN, because `float("nan") >= 1800.0` is False.


def test_over_subtracted_clock_raises_actions() -> None:
    with pytest.raises(ValueError, match="period-relative"):
        assert_work_unit_time_base({2: -2700.0})


def test_over_subtracted_clock_raises_frames() -> None:
    with pytest.raises(ValueError, match="period-relative"):
        assert_frames_time_base({2: -2700.0})


def test_nan_clock_raises_actions() -> None:
    with pytest.raises(ValueError, match="period-relative"):
        assert_work_unit_time_base({2: math.nan})


def test_nan_clock_raises_frames() -> None:
    with pytest.raises(ValueError, match="period-relative"):
        assert_frames_time_base({2: math.nan})


def test_small_negative_float_noise_is_tolerated() -> None:
    """A period-relative clock may legitimately start a hair below zero (float noise, or an action a
    fraction of a second before the nominal kickoff frame). Only a LARGE negative min indicates an
    over-subtracted re-base -- otherwise this guard would false-fire on healthy units."""
    assert_work_unit_time_base({2: -0.3})
    assert_frames_time_base({2: -0.3})


def test_both_drivers_invoke_work_unit_time_base_guard() -> None:
    """Source-level sentinel (mirrors test_et_direction_sentinel): BOTH the local hexagon driver
    (pipeline.run_work_unit) and the Spark production driver
    (ingestion.action_context._process_tracking_match) MUST call assert_work_unit_time_base.

    The Spark driver is not locally runnable (feedback_test_production_driver_entry_point), so a
    source-text gate catches a future refactor that silently drops the guard from either path at
    PR time — before an unguarded absolute-clock work unit reaches production."""
    from pathlib import Path

    src_root = Path(__file__).resolve().parents[2]  # src/tests/action_context -> src
    pipeline_src = (src_root / "analytics" / "action_context" / "pipeline.py").read_text(encoding="utf-8")
    driver_src = (src_root / "ingestion" / "action_context.py").read_text(encoding="utf-8")

    assert "assert_work_unit_time_base(" in pipeline_src, "pipeline.run_work_unit dropped the guard"

    # Scope the driver assertion to the _process_tracking_match function body.
    start = driver_src.index("def _process_tracking_match")
    end = driver_src.index("\ndef ", start + 1)
    assert "assert_work_unit_time_base(" in driver_src[start:end], (
        "_process_tracking_match dropped the work-unit time-base guard"
    )
