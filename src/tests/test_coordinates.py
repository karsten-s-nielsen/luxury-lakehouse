"""Tests for coordinate transformation module."""
# pyright: reportCallIssue=false, reportArgumentType=false

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.coordinates import (
    PITCH_LENGTH_M,
    PITCH_WIDTH_M,
    STATSBOMB_LENGTH,
    STATSBOMB_WIDTH,
    center_m_to_statsbomb,
    metrica_to_statsbomb,
    pct_to_statsbomb,
    pitch_m_to_statsbomb,
    statsbomb_to_meters,
)


class TestMetricaToStatsbomb:
    """Metrica [0,1] with y-flip to StatsBomb 120x80."""

    def test_origin(self) -> None:
        x_sb, y_sb = metrica_to_statsbomb(0.0, 0.0)
        assert x_sb == pytest.approx(0.0)
        assert y_sb == pytest.approx(80.0)

    def test_far_corner(self) -> None:
        x_sb, y_sb = metrica_to_statsbomb(1.0, 1.0)
        assert x_sb == pytest.approx(120.0)
        assert y_sb == pytest.approx(0.0)

    def test_center(self) -> None:
        x_sb, y_sb = metrica_to_statsbomb(0.5, 0.5)
        assert x_sb == pytest.approx(60.0)
        assert y_sb == pytest.approx(40.0)

    def test_vectorized(self) -> None:
        x = np.array([0.0, 0.5, 1.0])
        y = np.array([0.0, 0.5, 1.0])
        x_sb, y_sb = metrica_to_statsbomb(x, y)
        np.testing.assert_allclose(x_sb, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb, [80.0, 40.0, 0.0])

    def test_pandas_series(self) -> None:
        df = pd.DataFrame({"x": [0.0, 0.5, 1.0], "y": [0.0, 0.5, 1.0]})
        x_sb, y_sb = metrica_to_statsbomb(df["x"], df["y"])
        np.testing.assert_allclose(x_sb.values, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb.values, [80.0, 40.0, 0.0])


class TestCenterMToStatsbomb:
    """Center-origin meters (IDSSE/SkillCorner) to StatsBomb 120x80."""

    def test_center(self) -> None:
        x_sb, y_sb = center_m_to_statsbomb(0.0, 0.0)
        assert x_sb == pytest.approx(60.0)
        assert y_sb == pytest.approx(40.0)

    def test_negative_corner(self) -> None:
        x_sb, y_sb = center_m_to_statsbomb(-52.5, -34.0)
        assert x_sb == pytest.approx(0.0)
        assert y_sb == pytest.approx(0.0)

    def test_positive_corner(self) -> None:
        x_sb, y_sb = center_m_to_statsbomb(52.5, 34.0)
        assert x_sb == pytest.approx(120.0)
        assert y_sb == pytest.approx(80.0)

    def test_vectorized(self) -> None:
        x = np.array([-52.5, 0.0, 52.5])
        y = np.array([-34.0, 0.0, 34.0])
        x_sb, y_sb = center_m_to_statsbomb(x, y)
        np.testing.assert_allclose(x_sb, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb, [0.0, 40.0, 80.0])

    def test_pandas_series(self) -> None:
        df = pd.DataFrame({"x": [-52.5, 0.0, 52.5], "y": [-34.0, 0.0, 34.0]})
        x_sb, y_sb = center_m_to_statsbomb(df["x"], df["y"])
        np.testing.assert_allclose(x_sb.values, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb.values, [0.0, 40.0, 80.0])


class TestPitchMToStatsbomb:
    """Pitch-origin meters (0-105, 0-68) to StatsBomb 120x80."""

    def test_origin(self) -> None:
        x_sb, y_sb = pitch_m_to_statsbomb(0.0, 0.0)
        assert x_sb == pytest.approx(0.0)
        assert y_sb == pytest.approx(0.0)

    def test_far_corner(self) -> None:
        x_sb, y_sb = pitch_m_to_statsbomb(105.0, 68.0)
        assert x_sb == pytest.approx(120.0)
        assert y_sb == pytest.approx(80.0)

    def test_center(self) -> None:
        x_sb, y_sb = pitch_m_to_statsbomb(52.5, 34.0)
        assert x_sb == pytest.approx(60.0)
        assert y_sb == pytest.approx(40.0)

    def test_vectorized(self) -> None:
        x = np.array([0.0, 52.5, 105.0])
        y = np.array([0.0, 34.0, 68.0])
        x_sb, y_sb = pitch_m_to_statsbomb(x, y)
        np.testing.assert_allclose(x_sb, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb, [0.0, 40.0, 80.0])

    def test_pandas_series(self) -> None:
        df = pd.DataFrame({"x": [0.0, 52.5, 105.0], "y": [0.0, 34.0, 68.0]})
        x_sb, y_sb = pitch_m_to_statsbomb(df["x"], df["y"])
        np.testing.assert_allclose(x_sb.values, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb.values, [0.0, 40.0, 80.0])


class TestPctToStatsbomb:
    """Percentage [0,100] (Wyscout) to StatsBomb 120x80."""

    def test_origin(self) -> None:
        x_sb, y_sb = pct_to_statsbomb(0.0, 0.0)
        assert x_sb == pytest.approx(0.0)
        assert y_sb == pytest.approx(0.0)

    def test_far_corner(self) -> None:
        x_sb, y_sb = pct_to_statsbomb(100.0, 100.0)
        assert x_sb == pytest.approx(120.0)
        assert y_sb == pytest.approx(80.0)

    def test_center(self) -> None:
        x_sb, y_sb = pct_to_statsbomb(50.0, 50.0)
        assert x_sb == pytest.approx(60.0)
        assert y_sb == pytest.approx(40.0)

    def test_vectorized(self) -> None:
        x = np.array([0.0, 50.0, 100.0])
        y = np.array([0.0, 50.0, 100.0])
        x_sb, y_sb = pct_to_statsbomb(x, y)
        np.testing.assert_allclose(x_sb, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb, [0.0, 40.0, 80.0])

    def test_pandas_series(self) -> None:
        df = pd.DataFrame({"x": [0.0, 50.0, 100.0], "y": [0.0, 50.0, 100.0]})
        x_sb, y_sb = pct_to_statsbomb(df["x"], df["y"])
        np.testing.assert_allclose(x_sb.values, [0.0, 60.0, 120.0])
        np.testing.assert_allclose(y_sb.values, [0.0, 40.0, 80.0])


class TestStatsbombToMeters:
    """StatsBomb 120x80 to real-world meters (0-105, 0-68)."""

    def test_origin(self) -> None:
        x_m, y_m = statsbomb_to_meters(0.0, 0.0)
        assert x_m == pytest.approx(0.0)
        assert y_m == pytest.approx(0.0)

    def test_center(self) -> None:
        x_m, y_m = statsbomb_to_meters(60.0, 40.0)
        assert x_m == pytest.approx(52.5)
        assert y_m == pytest.approx(34.0)

    def test_far_corner(self) -> None:
        x_m, y_m = statsbomb_to_meters(120.0, 80.0)
        assert x_m == pytest.approx(105.0)
        assert y_m == pytest.approx(68.0)

    def test_vectorized(self) -> None:
        x = np.array([0.0, 60.0, 120.0])
        y = np.array([0.0, 40.0, 80.0])
        x_m, y_m = statsbomb_to_meters(x, y)
        np.testing.assert_allclose(x_m, [0.0, 52.5, 105.0])
        np.testing.assert_allclose(y_m, [0.0, 34.0, 68.0])

    def test_pandas_series(self) -> None:
        df = pd.DataFrame({"x": [0.0, 60.0, 120.0], "y": [0.0, 40.0, 80.0]})
        x_m, y_m = statsbomb_to_meters(df["x"], df["y"])
        np.testing.assert_allclose(x_m.values, [0.0, 52.5, 105.0])
        np.testing.assert_allclose(y_m.values, [0.0, 34.0, 68.0])


class TestInverseConsistency:
    """statsbomb_to_meters reverses pitch_m_to_statsbomb."""

    def test_roundtrip_scalar(self) -> None:
        x_m, y_m = 73.5, 42.0
        x_sb, y_sb = pitch_m_to_statsbomb(x_m, y_m)
        x_rt, y_rt = statsbomb_to_meters(x_sb, y_sb)
        assert x_rt == pytest.approx(x_m)
        assert y_rt == pytest.approx(y_m)

    def test_roundtrip_vectorized(self) -> None:
        x_m = np.array([0.0, 52.5, 105.0, 73.5, 21.0])
        y_m = np.array([0.0, 34.0, 68.0, 42.0, 10.0])
        x_sb, y_sb = pitch_m_to_statsbomb(x_m, y_m)
        x_rt, y_rt = statsbomb_to_meters(x_sb, y_sb)
        np.testing.assert_allclose(x_rt, x_m)
        np.testing.assert_allclose(y_rt, y_m)

    def test_roundtrip_reverse_direction(self) -> None:
        """Start from StatsBomb, go to meters, come back."""
        x_sb, y_sb = 90.0, 60.0
        x_m, y_m = statsbomb_to_meters(x_sb, y_sb)
        x_rt, y_rt = pitch_m_to_statsbomb(x_m, y_m)
        assert x_rt == pytest.approx(x_sb)
        assert y_rt == pytest.approx(y_sb)


class TestConstants:
    """Verify module-level constants match dbt vars."""

    def test_statsbomb_length(self) -> None:
        assert STATSBOMB_LENGTH == 120.0

    def test_statsbomb_width(self) -> None:
        assert STATSBOMB_WIDTH == 80.0

    def test_pitch_length_m(self) -> None:
        assert PITCH_LENGTH_M == 105.0

    def test_pitch_width_m(self) -> None:
        assert PITCH_WIDTH_M == 68.0
