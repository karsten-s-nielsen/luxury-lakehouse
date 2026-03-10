"""Tests for the physics-based pitch control model (Spearman 2017)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.pitch_control import (
    PitchControlParams,
    _compute_team_influence,
    _compute_time_to_intercept,
    _meters_to_sb_x,
    _meters_to_sb_y,
    _sb_to_meters_x,
    _sb_to_meters_y,
    compute_pitch_control_at_point,
    compute_pitch_control_at_points,
    compute_pitch_control_frame,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_players(
    home_positions: list[tuple[float, float]],
    away_positions: list[tuple[float, float]],
    home_velocities: list[tuple[float, float]] | None = None,
    away_velocities: list[tuple[float, float]] | None = None,
) -> pd.DataFrame:
    """Build a players DataFrame in StatsBomb coordinates (120x80)."""
    n_home = len(home_positions)
    n_away = len(away_positions)

    if home_velocities is None:
        home_velocities = [(0.0, 0.0)] * n_home
    if away_velocities is None:
        away_velocities = [(0.0, 0.0)] * n_away

    rows: list[dict[str, object]] = []
    for i, ((x, y), (vx, vy)) in enumerate(zip(home_positions, home_velocities, strict=True)):
        rows.append({"x": x, "y": y, "velocity_x": vx, "velocity_y": vy, "team": "home", "player_id": f"h{i}"})
    for i, ((x, y), (vx, vy)) in enumerate(zip(away_positions, away_velocities, strict=True)):
        rows.append({"x": x, "y": y, "velocity_x": vx, "velocity_y": vy, "team": "away", "player_id": f"a{i}"})

    return pd.DataFrame(rows)


_DEFAULT_PARAMS = PitchControlParams()


class TestPitchControlParams:
    """Test parameter dataclass."""

    def test_default_values(self) -> None:
        p = PitchControlParams()
        assert p.reaction_time == 0.7
        assert p.max_acceleration == 7.0
        assert p.sigma == 0.45
        assert p.grid_cells_x == 50
        assert p.grid_cells_y == 32

    def test_custom_override(self) -> None:
        p = PitchControlParams(reaction_time=0.5, max_acceleration=8.0, grid_cells_x=100)
        assert p.reaction_time == 0.5
        assert p.max_acceleration == 8.0
        assert p.grid_cells_x == 100
        # Defaults preserved
        assert p.sigma == 0.45


class TestCoordinateConversion:
    """Test StatsBomb <-> meters coordinate conversion."""

    def test_sb_to_meters_x(self) -> None:
        x = np.array([0.0, 60.0, 120.0])
        result = _sb_to_meters_x(x, _DEFAULT_PARAMS)
        np.testing.assert_allclose(result, [0.0, 52.5, 105.0])

    def test_sb_to_meters_y(self) -> None:
        y = np.array([0.0, 40.0, 80.0])
        result = _sb_to_meters_y(y, _DEFAULT_PARAMS)
        np.testing.assert_allclose(result, [0.0, 34.0, 68.0])

    def test_meters_to_sb_x(self) -> None:
        x = np.array([0.0, 52.5, 105.0])
        result = _meters_to_sb_x(x, _DEFAULT_PARAMS)
        np.testing.assert_allclose(result, [0.0, 60.0, 120.0])

    def test_meters_to_sb_y(self) -> None:
        y = np.array([0.0, 34.0, 68.0])
        result = _meters_to_sb_y(y, _DEFAULT_PARAMS)
        np.testing.assert_allclose(result, [0.0, 40.0, 80.0])


class TestTimeToIntercept:
    """Test kinematic time-to-intercept calculation."""

    def test_player_at_target(self) -> None:
        """Player already at the target — TTI should equal reaction_time."""
        pos = np.array([[52.5, 34.0]])
        vel = np.array([[0.0, 0.0]])
        target = np.array([[52.5, 34.0]])
        tti = _compute_time_to_intercept(pos, vel, target, _DEFAULT_PARAMS)
        np.testing.assert_allclose(tti, [[_DEFAULT_PARAMS.reaction_time]], atol=1e-6)

    def test_player_far_away(self) -> None:
        """Player far from target — TTI should be significantly larger than reaction_time."""
        pos = np.array([[0.0, 0.0]])
        vel = np.array([[0.0, 0.0]])
        target = np.array([[105.0, 68.0]])
        tti = _compute_time_to_intercept(pos, vel, target, _DEFAULT_PARAMS)
        assert tti[0, 0] > _DEFAULT_PARAMS.reaction_time + 3.0  # Far enough for significant travel time

    def test_player_moving_toward_target(self) -> None:
        """Player moving toward target should have shorter TTI than stationary."""
        pos = np.array([[0.0, 0.0]])
        target = np.array([[10.0, 0.0]])

        # Stationary
        vel_zero = np.array([[0.0, 0.0]])
        tti_stationary = _compute_time_to_intercept(pos, vel_zero, target, _DEFAULT_PARAMS)

        # Moving toward
        vel_toward = np.array([[3.0, 0.0]])
        tti_moving = _compute_time_to_intercept(pos, vel_toward, target, _DEFAULT_PARAMS)

        assert tti_moving[0, 0] < tti_stationary[0, 0]

    def test_player_moving_away_from_target(self) -> None:
        """Player moving away from target should have longer TTI than stationary."""
        pos = np.array([[0.0, 0.0]])
        target = np.array([[10.0, 0.0]])

        vel_zero = np.array([[0.0, 0.0]])
        tti_stationary = _compute_time_to_intercept(pos, vel_zero, target, _DEFAULT_PARAMS)

        vel_away = np.array([[-3.0, 0.0]])
        tti_away = _compute_time_to_intercept(pos, vel_away, target, _DEFAULT_PARAMS)

        assert tti_away[0, 0] > tti_stationary[0, 0]


class TestPlayerInfluence:
    """Test logistic sigmoid influence calculation."""

    def test_arrives_well_before_opponent(self) -> None:
        """Player arriving well before opponent should have influence near 1."""
        # Team TTI: arrives at 1.0s, opponent min at 3.0s
        team_tti = np.array([[1.0]])
        opp_min_tti = np.array([3.0])
        influence = _compute_team_influence(team_tti, opp_min_tti, _DEFAULT_PARAMS)
        assert influence[0] > 0.95

    def test_arrives_well_after_opponent(self) -> None:
        """Player arriving well after opponent should have influence near 0."""
        team_tti = np.array([[3.0]])
        opp_min_tti = np.array([1.0])
        influence = _compute_team_influence(team_tti, opp_min_tti, _DEFAULT_PARAMS)
        assert influence[0] < 0.05

    def test_arrives_at_same_time(self) -> None:
        """Equal TTI should give influence approximately 0.5."""
        team_tti = np.array([[2.0]])
        opp_min_tti = np.array([2.0])
        influence = _compute_team_influence(team_tti, opp_min_tti, _DEFAULT_PARAMS)
        np.testing.assert_allclose(influence[0], 0.5, atol=0.01)


class TestPitchControlFrame:
    """Test full pitch control frame computation."""

    def test_single_home_player_dominates_own_half(self) -> None:
        """A lone home player at (30, 40) should dominate nearby cells."""
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        _grid_x, _grid_y, surface = compute_pitch_control_frame(players)
        assert surface.shape == (32, 50)

        # Home player near x=30 (grid index ~12): should have home control > 0.5
        home_col_idx = int(30 / 120 * 49)
        mid_row_idx = 16
        assert surface[mid_row_idx, home_col_idx] > 0.5

    def test_symmetric_players_give_balanced_control(self) -> None:
        """Symmetric home/away players should produce ~0.5 at center."""
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        _grid_x, _grid_y, surface = compute_pitch_control_frame(players)

        # Center of pitch
        center_col = 25  # ~x=60
        center_row = 16  # ~y=40
        assert 0.3 < surface[center_row, center_col] < 0.7

    def test_velocity_shifts_control(self) -> None:
        """A sprinting player should control more space ahead of them."""
        # Home player sprinting right, away stationary
        players_moving = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
            home_velocities=[(5.0, 0.0)],  # sprinting right
            away_velocities=[(0.0, 0.0)],
        )
        _gx, _gy, surface_moving = compute_pitch_control_frame(players_moving)

        # Home player stationary
        players_static = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        _gx2, _gy2, surface_static = compute_pitch_control_frame(players_static)

        # At a point ahead of home player (x~50), moving player should have more control
        ahead_col = int(50 / 120 * 49)
        mid_row = 16
        assert surface_moving[mid_row, ahead_col] > surface_static[mid_row, ahead_col]

    def test_empty_players(self) -> None:
        """Empty DataFrame should return 0.5 everywhere."""
        players = pd.DataFrame(columns=pd.Index(["x", "y", "velocity_x", "velocity_y", "team", "player_id"]))
        _grid_x, _grid_y, surface = compute_pitch_control_frame(players)
        assert surface.shape == (32, 50)
        np.testing.assert_allclose(surface, 0.5)

    def test_nan_velocities_default_to_contested(self) -> None:
        """NaN velocities produce 0.5 (contested) via safe division guard."""
        players = pd.DataFrame(
            {
                "x": [30.0, 90.0],
                "y": [40.0, 40.0],
                "velocity_x": [float("nan"), 0.0],
                "velocity_y": [0.0, 0.0],
                "team": ["home", "away"],
                "player_id": ["h0", "a0"],
            }
        )
        _gx, _gy, surface = compute_pitch_control_frame(players)
        # NaN propagates through TTI/influence but safe division defaults to 0.5
        assert not np.isnan(surface).any()
        np.testing.assert_allclose(surface, 0.5)

    def test_unknown_team_label_excluded(self) -> None:
        """Players with unrecognized team labels are excluded from both teams."""
        players_with_ball = pd.DataFrame(
            {
                "x": [30.0, 90.0, 60.0],
                "y": [40.0, 40.0, 40.0],
                "velocity_x": [0.0, 0.0, 0.0],
                "velocity_y": [0.0, 0.0, 0.0],
                "team": ["home", "away", "ball"],
                "player_id": ["h0", "a0", "b0"],
            }
        )
        players_without_ball = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        _gx1, _gy1, surface_with = compute_pitch_control_frame(players_with_ball)
        _gx2, _gy2, surface_without = compute_pitch_control_frame(players_without_ball)
        # "ball" team row should be silently ignored — surfaces should match
        np.testing.assert_allclose(surface_with, surface_without, atol=1e-10)

    def test_output_shape_and_range(self) -> None:
        """Output surface should be (ny, nx) with values in [0, 1]."""
        params = PitchControlParams(grid_cells_x=25, grid_cells_y=16)
        players = _make_players(
            home_positions=[(30.0, 40.0), (60.0, 20.0)],
            away_positions=[(90.0, 40.0), (60.0, 60.0)],
        )
        grid_x, grid_y, surface = compute_pitch_control_frame(players, params)
        assert grid_x.shape == (25,)
        assert grid_y.shape == (16,)
        assert surface.shape == (16, 25)
        assert surface.min() >= 0.0
        assert surface.max() <= 1.0


class TestPitchControlAtPoint:
    """Test single-point pitch control convenience function."""

    def test_at_home_player_position(self) -> None:
        """Control at the home player's position should be > 0.5."""
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        control = compute_pitch_control_at_point(players, 30.0, 40.0)
        assert control > 0.5

    def test_contested_midpoint(self) -> None:
        """Control at equidistant midpoint should be ~0.5."""
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        control = compute_pitch_control_at_point(players, 60.0, 40.0)
        assert 0.3 < control < 0.7


class TestBatchPitchControl:
    """Test batched pitch control computation at multiple points."""

    def test_single_point_matches_scalar(self) -> None:
        """Batch with 1 target must match compute_pitch_control_at_point."""
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        scalar = compute_pitch_control_at_point(players, 45.0, 30.0)
        batch = compute_pitch_control_at_points(players, np.array([[45.0, 30.0]]))
        assert batch.shape == (1,)
        np.testing.assert_allclose(batch[0], scalar, atol=1e-10)

    def test_multiple_points_shape(self) -> None:
        """Batch with N targets returns (N,) array."""
        players = _make_players(
            home_positions=[(30.0, 40.0), (60.0, 20.0)],
            away_positions=[(90.0, 40.0), (60.0, 60.0)],
        )
        targets = np.array([[10.0, 10.0], [60.0, 40.0], [100.0, 70.0], [30.0, 40.0], [90.0, 40.0]])
        result = compute_pitch_control_at_points(players, targets)
        assert result.shape == (5,)

    def test_values_bounded_zero_one(self) -> None:
        """All batch values must be in [0, 1]."""
        players = _make_players(
            home_positions=[(30.0, 40.0), (50.0, 20.0)],
            away_positions=[(90.0, 40.0), (70.0, 60.0)],
            home_velocities=[(3.0, 1.0), (-1.0, 2.0)],
            away_velocities=[(-2.0, 0.5), (1.0, -1.0)],
        )
        targets = np.array(
            [[0.0, 0.0], [60.0, 40.0], [120.0, 80.0], [30.0, 40.0], [90.0, 40.0], [10.0, 70.0], [110.0, 10.0]]
        )
        result = compute_pitch_control_at_points(players, targets)
        assert result.shape == (7,)
        assert np.all(result >= 0.0)
        assert np.all(result <= 1.0)

    def test_empty_targets_returns_empty(self) -> None:
        """Empty target array returns empty result."""
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(90.0, 40.0)],
        )
        result = compute_pitch_control_at_points(players, np.empty((0, 2)))
        assert result.shape == (0,)
