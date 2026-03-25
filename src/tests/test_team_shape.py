"""Tests for team shape spatial metrics module (D19)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from analytics.team_shape import (  # noqa: F401  # TeamShapeResult used in Tasks 2+
    TeamShapeParams,
    TeamShapeResult,
    compute_team_shape,
    compute_team_shape_frame,
)


class TestTeamShapeParams:
    """Verify params dataclass defaults and overrides."""

    def test_defaults(self) -> None:
        p = TeamShapeParams()
        assert p.n_defensive_lines == 3
        assert p.min_players == 3
        assert p.pitch_length == 120.0
        assert p.pitch_width == 80.0

    def test_override(self) -> None:
        p = TeamShapeParams(n_defensive_lines=4, min_players=4)
        assert p.n_defensive_lines == 4
        assert p.min_players == 4

    def test_frozen(self) -> None:
        p = TeamShapeParams()
        with pytest.raises(AttributeError):
            p.n_defensive_lines = 5  # type: ignore[misc]


def _make_rectangle() -> tuple[np.ndarray, np.ndarray]:
    """4 players in a 20x10 rectangle — known geometry.

    Positions: (50,35), (70,35), (50,45), (70,45)
    Centroid: (60, 40). Hull area: 20*10=200.
    Length (x-spread): 20. Width (y-spread): 10.
    Stretch index: mean distance from (60,40) = sqrt(10²+5²) ≈ 11.18.
    """
    x = np.array([50.0, 70.0, 50.0, 70.0])
    y = np.array([35.0, 35.0, 45.0, 45.0])
    return x, y


def _make_442() -> tuple[np.ndarray, np.ndarray]:
    """10 outfield players in a 4-4-2 formation (no GK).

    Three clear lines along x-axis for Ward clustering:
    - Back 4: x ≈ 30
    - Midfield 4: x ≈ 60
    - Forward 2: x ≈ 90
    """
    x = np.array([28.0, 30.0, 32.0, 30.0, 58.0, 60.0, 62.0, 60.0, 88.0, 92.0])
    y = np.array([20.0, 35.0, 50.0, 65.0, 20.0, 35.0, 50.0, 65.0, 35.0, 50.0])
    return x, y


class TestComputeTeamShape:
    """Core compute_team_shape function tests."""

    def test_rectangle_centroid(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        assert abs(result.centroid_x - 60.0) < 1e-10
        assert abs(result.centroid_y - 40.0) < 1e-10

    def test_rectangle_hull_area(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        np.testing.assert_allclose(result.convex_hull_area, 200.0, atol=1e-10)

    def test_rectangle_length_width(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        np.testing.assert_allclose(result.team_length, 20.0, atol=1e-10)
        np.testing.assert_allclose(result.team_width, 10.0, atol=1e-10)

    def test_rectangle_stretch_index(self) -> None:
        x, y = _make_rectangle()
        result = compute_team_shape(x, y)
        expected = np.sqrt(10.0**2 + 5.0**2)
        np.testing.assert_allclose(result.stretch_index, expected, atol=1e-10)

    def test_442_three_lines(self) -> None:
        """Ward clustering should find 3 clusters along the x-axis."""
        x, y = _make_442()
        result = compute_team_shape(x, y)
        assert len(result.inter_line_gaps) == 2
        for gap in result.inter_line_gaps:
            assert 25.0 < gap < 35.0

    def test_442_defensive_line(self) -> None:
        """Defensive line height should be near x=30 (the back 4)."""
        x, y = _make_442()
        result = compute_team_shape(x, y)
        assert 25.0 < result.defensive_line_height < 35.0


class TestComputeTeamShapeFrame:
    """DataFrame wrapper tests."""

    def test_both_teams(self) -> None:
        df = pd.DataFrame(
            {
                "x": [50.0, 70.0, 50.0, 70.0, 30.0, 40.0, 30.0, 40.0],
                "y": [35.0, 35.0, 45.0, 45.0, 20.0, 20.0, 30.0, 30.0],
                "team": ["home"] * 4 + ["away"] * 4,
            }
        )
        result = compute_team_shape_frame(df)
        assert set(result.keys()) == {"home", "away"}
        np.testing.assert_allclose(result["home"].centroid_x, 60.0, atol=1e-10)
        np.testing.assert_allclose(result["away"].centroid_x, 35.0, atol=1e-10)

    def test_ignores_non_team_rows(self) -> None:
        """Rows with team values other than home/away are ignored."""
        df = pd.DataFrame(
            {
                "x": [50.0, 70.0, 50.0, 99.0],
                "y": [35.0, 35.0, 45.0, 99.0],
                "team": ["home", "home", "home", "ball"],
            }
        )
        result = compute_team_shape_frame(df)
        assert "ball" not in result
        assert "home" in result


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_arrays(self) -> None:
        result = compute_team_shape(np.array([]), np.array([]))
        assert math.isnan(result.centroid_x)
        assert result.inter_line_gaps == ()

    def test_single_player(self) -> None:
        result = compute_team_shape(np.array([50.0]), np.array([40.0]))
        assert math.isnan(result.centroid_x)

    def test_two_players(self) -> None:
        result = compute_team_shape(np.array([50.0, 70.0]), np.array([40.0, 40.0]))
        assert math.isnan(result.centroid_x)

    def test_three_collinear(self) -> None:
        """Three collinear players — ConvexHull can't form 2-D hull."""
        result = compute_team_shape(
            np.array([50.0, 60.0, 70.0]),
            np.array([40.0, 40.0, 40.0]),
        )
        assert result.convex_hull_area == 0.0
        assert result.team_width == 0.0
        assert result.team_length == 20.0

    def test_all_same_position(self) -> None:
        x = np.array([50.0, 50.0, 50.0])
        y = np.array([40.0, 40.0, 40.0])
        result = compute_team_shape(x, y)
        assert result.stretch_index == 0.0
        assert result.team_length == 0.0
        assert result.convex_hull_area == 0.0
