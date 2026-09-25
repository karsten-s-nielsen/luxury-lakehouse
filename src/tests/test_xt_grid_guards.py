"""ADR-063 xT-grid guards reimplemented against a fitted sk ``ExpectedThreat`` (ExT-v2, ADR-085).

Pins the guard semantics AND the ADR-041 orientation empirically: a real LTR fit must pass
``assert_directional`` (via the ``physical_grid`` seam), and a reversed (RTL-attacking) fit must FAIL
it — so the guard is non-vacuous, not a tautology on the raw ``.xT`` storage.
"""

from __future__ import annotations

import logging

import numpy as np
import pytest

from analytics.xt_grid_guards import (
    assert_directional,
    assert_profile_directional,
    grid_drift,
    validate_differential,
    validate_structural,
)

_LOG = logging.getLogger("test_xt_grid_guards")


def _model_with_xt_columns(per_x: np.ndarray):
    """A fitted-enough ExpectedThreat(16x12) whose ``.xT`` is ``per_x`` broadcast across y.

    ``physical_grid`` reads ONLY ``.xT`` + geometry, so setting the raw grid directly deterministically
    pins the physical orientation WITHOUT needing a realistic value-iteration gradient (a synthetic
    random action stream fits to a near-flat grid — a fixture limitation, not a guard behaviour). The
    orientation mapping (raw x-column -> physical x, same direction) is verified by
    ``test_physical_profile_orientation`` below.
    """
    from silly_kicks.xthreat import ExpectedThreat

    xt = ExpectedThreat(l=16, w=12)
    xt.xT = np.tile(np.asarray(per_x, dtype=np.float64), (12, 1))  # (w=12, l=16)
    return xt


# --------------------------------------------------------------------------- #
# assert_profile_directional — the pure ADR-063 gate on a per-x profile
# --------------------------------------------------------------------------- #
def test_profile_directional_passes_ascending() -> None:
    assert_profile_directional(np.linspace(0.02, 0.40, 16), competition_id="asc", logger=_LOG)


def test_profile_directional_fails_flat() -> None:
    with pytest.raises(ValueError, match="directional"):
        assert_profile_directional(np.full(16, 0.20), competition_id="flat", logger=_LOG)


def test_profile_directional_fails_reversed() -> None:
    """NON-VACUITY: a defensive-high (RTL) profile fails the thirds-ratio gate."""
    with pytest.raises(ValueError, match="not attacking-directional"):
        assert_profile_directional(np.linspace(0.40, 0.02, 16), competition_id="rev", logger=_LOG)


def test_profile_directional_fails_mid_scramble() -> None:
    """Passes the thirds-ratio (low def, high att) but the scrambled middle fails the rank-corr gate."""
    prof = np.array([0.02, 0.02, 0.02, 0.02, 0.02, 0.6, 0.01, 0.6, 0.01, 0.6, 0.01, 0.30, 0.30, 0.30, 0.30, 0.30])
    with pytest.raises(ValueError, match="rank correlation"):
        assert_profile_directional(prof, competition_id="scramble", logger=_LOG)


# --------------------------------------------------------------------------- #
# physical_grid sampling orientation (ADR-041) + the wired assert_directional(model)
# --------------------------------------------------------------------------- #
def test_physical_profile_orientation() -> None:
    """Raw ``.xT`` ascending in the x-column index → physical per-x profile ascends (index -1 = attacking)."""
    from analytics.xt_grid_guards import _physical_profile

    xt = _model_with_xt_columns(np.linspace(0.0, 1.0, 16))
    prof = _physical_profile(xt)
    assert prof[-1] > prof[0]
    assert np.all(np.diff(prof) >= -1e-9)  # monotone ascending


def test_assert_directional_model_passes_ltr() -> None:
    assert_directional(_model_with_xt_columns(np.linspace(0.02, 0.40, 16)), competition_id="ltr", logger=_LOG)


def test_assert_directional_model_reversed_raises() -> None:
    """NON-VACUITY through the full wired path: a defensive-high model raises."""
    with pytest.raises(ValueError, match="directional"):
        assert_directional(_model_with_xt_columns(np.linspace(0.40, 0.02, 16)), competition_id="rtl", logger=_LOG)


# --------------------------------------------------------------------------- #
# validate_structural — raw .xT, orientation-agnostic
# --------------------------------------------------------------------------- #
def test_structural_passes_valid() -> None:
    xt = np.linspace(0.0, 0.30, 16 * 12).reshape(12, 16)
    validate_structural(xt)  # non-negative, range 0.30, no max cap


def test_structural_rejects_negative() -> None:
    xt = np.full((12, 16), -0.1)
    with pytest.raises(ValueError, match="negative"):
        validate_structural(xt)


def test_structural_rejects_exceeds_max() -> None:
    xt = np.linspace(0.0, 0.60, 16 * 12).reshape(12, 16)
    with pytest.raises(ValueError, match="exceeds max_value"):
        validate_structural(xt, max_value=0.50)


def test_structural_rejects_narrow_range() -> None:
    xt = np.full((12, 16), 0.10)  # range 0 < 0.05
    with pytest.raises(ValueError, match="range too narrow"):
        validate_structural(xt)


# --------------------------------------------------------------------------- #
# validate_differential
# --------------------------------------------------------------------------- #
def test_differential_none_skips() -> None:
    validate_differential(np.full((12, 16), 0.3), None)  # no raise


def test_differential_within_threshold() -> None:
    prev = np.full((12, 16), 0.30)
    new = np.full((12, 16), 0.33)  # +10%
    validate_differential(new, prev, max_relative_change=0.30)


def test_differential_rejects_large_shift() -> None:
    prev = np.full((12, 16), 0.30)
    new = np.full((12, 16), 0.50)  # +67%
    with pytest.raises(ValueError, match="changed by"):
        validate_differential(new, prev, max_relative_change=0.30)


def test_differential_zero_baseline_skips() -> None:
    validate_differential(np.full((12, 16), 0.3), np.zeros((12, 16)))  # no raise


# --------------------------------------------------------------------------- #
# grid_drift
# --------------------------------------------------------------------------- #
def test_drift_none_baseline() -> None:
    assert grid_drift(np.full((12, 16), 0.3), None) is None


def test_drift_shape_mismatch_none() -> None:
    assert grid_drift(np.full((12, 16), 0.3), np.full((8, 16), 0.3)) is None


def test_drift_measures_above_floor() -> None:
    prev = np.full((12, 16), 0.10)
    new = prev.copy()
    new[0, 0] = 0.12  # +20% on an above-floor cell
    drift = grid_drift(new, prev)
    assert drift == pytest.approx(0.20, rel=1e-6)
