"""ADR-063 directionality guard for the xT grid — jax-free (always runs).

`XTGrid.validate_structural(require_directional=True)` is the build-time gate that makes a stale /
symmetric / inverted grid (the negative-DZV root cause) a loud failure. These tests are kept OUT of
``test_expected_threat.py`` because that module is gated on ``importorskip("jax")`` (the value-iteration
tests need jax); the directionality check is pure NumPy and must be tested unconditionally.
"""

from __future__ import annotations

import numpy as np
import pytest

from analytics.expected_threat import XTGrid


def _grid(col: list[float] | np.ndarray, n_y: int = 8) -> XTGrid:
    """Build a (len(col), n_y) grid whose every y-column equals ``col`` (per-zone_x profile = col)."""
    values = np.repeat(np.asarray(col, dtype=float)[:, None], n_y, axis=1)
    return XTGrid(values=values, pitch_length=105.0, pitch_width=68.0, coord_system="spadl")


# Stale-grid signature: U-shaped, high at both ends, low middle (att/def ratio ~0.98).
_U_SHAPED = [0.18, 0.16, 0.10, 0.07, 0.06, 0.05, 0.05, 0.06, 0.07, 0.10, 0.16, 0.17]
# Inverted: defensive end higher than attacking end (ratio < 1).
_INVERTED = [0.17, 0.15, 0.13, 0.11, 0.09, 0.07, 0.06, 0.05, 0.04, 0.03, 0.02, 0.01]
# Correct: monotone rise toward the attacking goal (att/def ratio ~5.4).
_DIRECTIONAL = [0.007, 0.007, 0.007, 0.008, 0.009, 0.010, 0.011, 0.014, 0.018, 0.024, 0.048, 0.067]


def test_u_shaped_grid_rejected_when_directional() -> None:
    with pytest.raises(ValueError, match="directional"):
        _grid(_U_SHAPED).validate_structural(max_value=0.50, require_directional=True)


def test_inverted_grid_rejected_when_directional() -> None:
    with pytest.raises(ValueError, match="directional"):
        _grid(_INVERTED).validate_structural(max_value=0.50, require_directional=True)


def test_directional_grid_accepted_when_directional() -> None:
    # Must not raise.
    _grid(_DIRECTIONAL).validate_structural(max_value=0.50, require_directional=True)


def test_directionality_is_opt_in() -> None:
    # Without the flag, a non-directional grid passes structural checks (the caller decides when to gate).
    _grid(_U_SHAPED).validate_structural(max_value=0.50)  # should not raise


def test_below_min_actions_comp_not_false_failed() -> None:
    """A small/noisy per-competition grid is exempt: the caller passes require_directional=False
    below the min-action threshold, so it is NOT directionality-checked (ADR-063 / review M5)."""
    noisy = [0.02, 0.10, 0.01, 0.12, 0.03, 0.08, 0.05, 0.02, 0.11, 0.01, 0.09, 0.03]
    _grid(noisy).validate_structural(max_value=0.50, require_directional=False)  # should not raise


def test_thirds_ratio_threshold_is_configurable() -> None:
    # ratio of _DIRECTIONAL is ~5.4; a stricter bar rejects it.
    with pytest.raises(ValueError, match="ratio"):
        _grid(_DIRECTIONAL).validate_structural(max_value=0.50, require_directional=True, min_attack_ratio=8.0)
