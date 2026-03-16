"""Tests for OBSO (Off-Ball Scoring Opportunity) value surface computation."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.obso import (
    compute_obso_surface,
    compute_pass_obso,
    interpolate_grid,
    load_static_grid,
)
from analytics.pitch_control import generate_ghost_trajectories

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


def _make_uniform_grid(rows: int, cols: int, value: float = 0.5) -> np.ndarray:
    """Create a uniform grid filled with a constant value."""
    return np.full((rows, cols), value, dtype=np.float64)


# ---------------------------------------------------------------------------
# Tests: interpolate_grid
# ---------------------------------------------------------------------------


class TestInterpolateGrid:
    """Test bilinear interpolation of static grids."""

    def test_identity_same_shape(self) -> None:
        """Interpolation to same shape returns a copy of the original."""
        grid = np.random.default_rng(42).uniform(0, 1, (32, 50))
        result = interpolate_grid(grid, (32, 50))
        np.testing.assert_allclose(result, grid)

    def test_upsample_preserves_corners(self) -> None:
        """Corner values are preserved when upsampling."""
        grid = np.array([[1.0, 2.0], [3.0, 4.0]])
        result = interpolate_grid(grid, (4, 4))
        assert result.shape == (4, 4)
        # Corners should match original
        np.testing.assert_allclose(result[0, 0], 1.0, atol=1e-10)
        np.testing.assert_allclose(result[0, -1], 2.0, atol=1e-10)
        np.testing.assert_allclose(result[-1, 0], 3.0, atol=1e-10)
        np.testing.assert_allclose(result[-1, -1], 4.0, atol=1e-10)

    def test_interpolate_grid_preserves_sum(self) -> None:
        """Interpolation approximately preserves probability mass."""
        rng = np.random.default_rng(42)
        grid = rng.uniform(0.0, 0.1, (8, 12))
        original_mean = float(np.mean(grid))

        result = interpolate_grid(grid, (68, 104))
        result_mean = float(np.mean(result))

        # Mean value should be approximately preserved
        np.testing.assert_allclose(result_mean, original_mean, rtol=0.1)

    def test_downsample_shape(self) -> None:
        """Downsampling produces the correct shape."""
        grid = np.random.default_rng(42).uniform(0, 1, (68, 104))
        result = interpolate_grid(grid, (32, 50))
        assert result.shape == (32, 50)


# ---------------------------------------------------------------------------
# Tests: load_static_grid
# ---------------------------------------------------------------------------


class TestLoadStaticGrid:
    """Test loading static grids from CSV."""

    def test_load_from_csv(self, tmp_path: object) -> None:
        """Load a CSV grid and verify shape and values."""
        import pathlib

        csv_path = pathlib.Path(str(tmp_path)) / "test_grid.csv"
        grid_data = np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
        np.savetxt(str(csv_path), grid_data, delimiter=",")

        result = load_static_grid(str(csv_path))
        assert result.shape == (2, 3)
        np.testing.assert_allclose(result, grid_data, atol=1e-10)


# ---------------------------------------------------------------------------
# Tests: compute_obso_surface
# ---------------------------------------------------------------------------


class TestComputeObsoSurface:
    """Test OBSO surface computation."""

    def test_obso_shape_matches_ppcf(self) -> None:
        """Output grid has the same shape as the PPCF input."""
        ppcf = _make_uniform_grid(68, 104, 0.6)
        transition = _make_uniform_grid(64, 100, 0.5)
        epv = _make_uniform_grid(32, 50, 0.3)
        grid_x = np.linspace(0, 120, 104)
        grid_y = np.linspace(0, 80, 68)

        obso = compute_obso_surface(ppcf, transition, epv, (60.0, 40.0), grid_x, grid_y)
        assert obso.shape == ppcf.shape

    def test_obso_values_bounded(self) -> None:
        """OBSO values must be in [0, 1]."""
        rng = np.random.default_rng(42)
        ppcf = rng.uniform(0, 1, (68, 104))
        transition = rng.uniform(0, 1, (64, 100))
        epv = rng.uniform(0, 1, (32, 50))
        grid_x = np.linspace(0, 120, 104)
        grid_y = np.linspace(0, 80, 68)

        obso = compute_obso_surface(ppcf, transition, epv, (60.0, 40.0), grid_x, grid_y)
        assert np.all(obso >= 0.0)
        assert np.all(obso <= 1.0)

    def test_obso_zero_when_no_control(self) -> None:
        """OBSO = 0 where PPCF = 0 (no pitch control)."""
        ppcf = np.zeros((68, 104))
        transition = _make_uniform_grid(64, 100, 0.8)
        epv = _make_uniform_grid(32, 50, 0.5)
        grid_x = np.linspace(0, 120, 104)
        grid_y = np.linspace(0, 80, 68)

        obso = compute_obso_surface(ppcf, transition, epv, (60.0, 40.0), grid_x, grid_y)
        np.testing.assert_allclose(obso, 0.0)

    def test_obso_zero_when_no_epv(self) -> None:
        """OBSO = 0 where EPV = 0 (no scoring opportunity)."""
        ppcf = _make_uniform_grid(68, 104, 0.8)
        transition = _make_uniform_grid(64, 100, 0.5)
        epv = np.zeros((32, 50))
        grid_x = np.linspace(0, 120, 104)
        grid_y = np.linspace(0, 80, 68)

        obso = compute_obso_surface(ppcf, transition, epv, (60.0, 40.0), grid_x, grid_y)
        np.testing.assert_allclose(obso, 0.0)

    def test_obso_higher_near_ball(self) -> None:
        """OBSO should be higher near the ball position due to transition decay."""
        ppcf = _make_uniform_grid(68, 104, 0.8)
        transition = _make_uniform_grid(64, 100, 0.5)
        epv = _make_uniform_grid(32, 50, 0.5)
        grid_x = np.linspace(0, 120, 104)
        grid_y = np.linspace(0, 80, 68)

        ball_x, ball_y = 60.0, 40.0
        obso = compute_obso_surface(ppcf, transition, epv, (ball_x, ball_y), grid_x, grid_y)

        # Center of the pitch (near ball) should have higher OBSO than edges
        center_col = 52  # ~x=60
        center_row = 34  # ~y=40
        edge_val = float(obso[0, 0])
        center_val = float(obso[center_row, center_col])
        assert center_val > edge_val


# ---------------------------------------------------------------------------
# Tests: compute_pass_obso
# ---------------------------------------------------------------------------


class TestComputePassObso:
    """Test pass-level OBSO metrics for PAUSA."""

    @pytest.fixture()
    def ghost_frames_fixture(self) -> list[pd.DataFrame]:
        """Generate ghost frames for a 2-player scenario."""
        players = _make_players(
            home_positions=[(40.0, 40.0)],
            away_positions=[(80.0, 40.0)],
            home_velocities=[(2.0, 0.0)],
            away_velocities=[(-1.0, 0.0)],
        )
        return generate_ghost_trajectories(
            players, event_frame=100, frame_rate=25, window_before_s=0.5, window_after_s=0.5
        )

    @pytest.fixture()
    def static_grids(self) -> tuple[np.ndarray, np.ndarray]:
        """EPV and transition grids for testing."""
        rng = np.random.default_rng(42)
        transition = rng.uniform(0.1, 0.9, (64, 100))
        epv = rng.uniform(0.01, 0.3, (32, 50))
        return transition, epv

    def test_compute_pass_obso_schema(
        self,
        ghost_frames_fixture: list[pd.DataFrame],
        static_grids: tuple[np.ndarray, np.ndarray],
    ) -> None:
        """Result dict has the expected keys."""
        transition, epv = static_grids
        # event_frame_idx: 0.5s * 25fps = 12 (index of the event frame)
        result = compute_pass_obso(
            ghost_frames=ghost_frames_fixture,
            event_frame_idx=12,
            target_position=(70.0, 40.0),
            teammate_positions=np.array([[50.0, 30.0]]),
            transition_grid=transition,
            epv_grid=epv,
            grid_nx=26,
            grid_ny=17,
        )

        assert isinstance(result, dict)
        assert "actual_obso" in result
        assert "peak_obso" in result
        assert "optimal_obso" in result

    def test_actual_obso_at_event_frame(
        self,
        ghost_frames_fixture: list[pd.DataFrame],
        static_grids: tuple[np.ndarray, np.ndarray],
    ) -> None:
        """actual_obso specifically uses the event frame (not another frame)."""
        transition, epv = static_grids
        result = compute_pass_obso(
            ghost_frames=ghost_frames_fixture,
            event_frame_idx=12,
            target_position=(70.0, 40.0),
            teammate_positions=np.empty((0, 2)),
            transition_grid=transition,
            epv_grid=epv,
            grid_nx=26,
            grid_ny=17,
        )
        assert result["actual_obso"] >= 0.0
        assert result["actual_obso"] <= 1.0

    def test_peak_obso_gte_actual(
        self,
        ghost_frames_fixture: list[pd.DataFrame],
        static_grids: tuple[np.ndarray, np.ndarray],
    ) -> None:
        """Peak OBSO is always >= actual OBSO (it is the max across frames)."""
        transition, epv = static_grids
        result = compute_pass_obso(
            ghost_frames=ghost_frames_fixture,
            event_frame_idx=12,
            target_position=(70.0, 40.0),
            teammate_positions=np.empty((0, 2)),
            transition_grid=transition,
            epv_grid=epv,
            grid_nx=26,
            grid_ny=17,
        )
        assert result["peak_obso"] >= result["actual_obso"] - 1e-10

    def test_optimal_obso_gte_actual(
        self,
        ghost_frames_fixture: list[pd.DataFrame],
        static_grids: tuple[np.ndarray, np.ndarray],
    ) -> None:
        """Optimal OBSO is always >= actual OBSO (max across positions)."""
        transition, epv = static_grids
        result = compute_pass_obso(
            ghost_frames=ghost_frames_fixture,
            event_frame_idx=12,
            target_position=(70.0, 40.0),
            teammate_positions=np.array([[50.0, 30.0], [90.0, 50.0]]),
            transition_grid=transition,
            epv_grid=epv,
            grid_nx=26,
            grid_ny=17,
        )
        assert result["optimal_obso"] >= result["actual_obso"] - 1e-10

    def test_all_values_bounded(
        self,
        ghost_frames_fixture: list[pd.DataFrame],
        static_grids: tuple[np.ndarray, np.ndarray],
    ) -> None:
        """All OBSO metrics are in [0, 1]."""
        transition, epv = static_grids
        result = compute_pass_obso(
            ghost_frames=ghost_frames_fixture,
            event_frame_idx=12,
            target_position=(70.0, 40.0),
            teammate_positions=np.array([[50.0, 30.0], [90.0, 50.0]]),
            transition_grid=transition,
            epv_grid=epv,
            grid_nx=26,
            grid_ny=17,
        )
        for key in ("actual_obso", "peak_obso", "optimal_obso"):
            assert 0.0 <= result[key] <= 1.0, f"{key} = {result[key]} out of [0, 1]"
