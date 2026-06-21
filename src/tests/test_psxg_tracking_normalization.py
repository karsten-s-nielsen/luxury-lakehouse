"""Tracking PSxG normalization-port tests (0.3 / spec D-E).

Pins that SPADL-metre goalmouth crossings map into the SAME StatsBomb-trained
[0,1] space as the model's own `_normalise_goalmouth`, and that handedness is
not mirrored. The "2.44 m" model-card value was wrong — the model divides by 8
StatsBomb units (≈7.32 m); these tests guard the metric→normalized parity.
"""

from __future__ import annotations

import numpy as np
import pytest

from analytics.goalkeeper import _normalise_goalmouth, normalise_tracking_goalmouth


def test_crossbar_parity_tracking_matches_statsbomb() -> None:
    # StatsBomb crossbar 2.44 m ≈ 2.67 units → 2.67/8 = 0.334; tracking 2.44/7.32 = 0.333.
    trk = normalise_tracking_goalmouth(np.array([34.0]), np.array([2.44]))
    sb = _normalise_goalmouth(np.array([40.0]), np.array([2.67]))
    assert trk[0, 1] == pytest.approx(2.44 / 7.32, abs=1e-3)
    assert sb[0, 1] == pytest.approx(trk[0, 1], abs=2e-3)


def test_goal_centre_maps_to_half() -> None:
    trk = normalise_tracking_goalmouth(np.array([34.0]), np.array([0.0]))
    sb = _normalise_goalmouth(np.array([40.0]), np.array([0.0]))
    assert trk[0, 0] == pytest.approx(0.5, abs=1e-6)
    assert sb[0, 0] == pytest.approx(0.5, abs=1e-6)


def test_posts_map_to_0_and_1() -> None:
    out = normalise_tracking_goalmouth(np.array([30.34, 37.66]), np.array([0.0, 0.0]))
    assert out[0, 0] == pytest.approx(0.0, abs=1e-3)  # left post → 0
    assert out[1, 0] == pytest.approx(1.0, abs=1e-3)  # right post → 1


def test_handedness_not_mirrored() -> None:
    # A near-low-post shot must map below goal centre (0.5), not be mirrored above it.
    out = normalise_tracking_goalmouth(np.array([31.0]), np.array([1.0]))
    assert out[0, 0] < 0.5
