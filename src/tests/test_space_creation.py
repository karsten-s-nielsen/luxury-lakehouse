"""Tests for space creation (D14).

Tests for the differential OBSO space creation module based on
Fernandez & Bornn (2018) "Wide Open Spaces."
"""

from __future__ import annotations

import pytest

pytest.importorskip("jax")

import numpy as np
import pandas as pd

from analytics.space_creation import (
    SpaceCreationParams,
    compute_frame_space_creation,
    compute_space_created,
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


# ---------------------------------------------------------------------------
# Tests: compute_space_created
# ---------------------------------------------------------------------------


class TestComputeSpaceCreated:
    """Test the low-level space creation integral."""

    def test_identical_surfaces_zero_creation(self) -> None:
        """Same baseline and removed surfaces produce zero space created."""
        rng = np.random.default_rng(42)
        grid_x = np.linspace(0, 120, 20)
        grid_y = np.linspace(0, 80, 14)
        surface = rng.uniform(0, 0.5, (14, 20))

        result = compute_space_created(surface, surface, grid_x, grid_y)
        assert result == 0.0

    def test_higher_baseline_means_positive_creation(self) -> None:
        """Baseline > removed everywhere produces positive space created."""
        grid_x = np.linspace(0, 120, 20)
        grid_y = np.linspace(0, 80, 14)
        baseline = np.full((14, 20), 0.8)
        removed = np.full((14, 20), 0.3)

        result = compute_space_created(baseline, removed, grid_x, grid_y)
        assert result > 0.0

    def test_lower_baseline_means_zero_creation(self) -> None:
        """Baseline < removed everywhere produces zero space created.

        Only positive differences count — a player whose removal *increases*
        OBSO does not get credit for space creation.
        """
        grid_x = np.linspace(0, 120, 20)
        grid_y = np.linspace(0, 80, 14)
        baseline = np.full((14, 20), 0.2)
        removed = np.full((14, 20), 0.7)

        result = compute_space_created(baseline, removed, grid_x, grid_y)
        assert result == 0.0


# ---------------------------------------------------------------------------
# Tests: compute_frame_space_creation
# ---------------------------------------------------------------------------


class TestFrameSpaceCreation:
    """Test full-frame per-player space creation computation."""

    def test_all_players_get_values(self) -> None:
        """Four players produce four rows with all expected columns."""
        rng = np.random.default_rng(42)
        players = _make_players(
            home_positions=[(30.0, 40.0), (50.0, 20.0)],
            away_positions=[(70.0, 40.0), (90.0, 60.0)],
        )
        transition_grid = rng.uniform(0.1, 0.9, (32, 50))
        epv_grid = rng.uniform(0.01, 0.3, (16, 25))
        small_params = SpaceCreationParams(grid_cells_x=20, grid_cells_y=14)

        result = compute_frame_space_creation(
            players,
            transition_grid,
            epv_grid,
            ball_position=(50.0, 40.0),
            params=small_params,
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        expected_cols = {"player_id", "team", "space_created_m2", "space_destroyed_m2", "net_space_m2"}
        assert set(result.columns) == expected_cols

    def test_space_values_are_non_negative(self) -> None:
        """space_created_m2 and space_destroyed_m2 are always non-negative."""
        rng = np.random.default_rng(99)
        players = _make_players(
            home_positions=[(30.0, 40.0), (50.0, 20.0)],
            away_positions=[(70.0, 40.0), (90.0, 60.0)],
        )
        transition_grid = rng.uniform(0.1, 0.9, (32, 50))
        epv_grid = rng.uniform(0.01, 0.3, (16, 25))
        small_params = SpaceCreationParams(grid_cells_x=20, grid_cells_y=14)

        result = compute_frame_space_creation(
            players,
            transition_grid,
            epv_grid,
            ball_position=(50.0, 40.0),
            params=small_params,
        )

        assert (result["space_created_m2"] >= 0.0).all()
        assert (result["space_destroyed_m2"] >= 0.0).all()

    def test_net_space_equals_created_minus_destroyed(self) -> None:
        """net_space_m2 == space_created_m2 - space_destroyed_m2."""
        rng = np.random.default_rng(7)
        players = _make_players(
            home_positions=[(30.0, 40.0)],
            away_positions=[(70.0, 40.0)],
        )
        transition_grid = rng.uniform(0.1, 0.9, (32, 50))
        epv_grid = rng.uniform(0.01, 0.3, (16, 25))
        small_params = SpaceCreationParams(grid_cells_x=20, grid_cells_y=14)

        result = compute_frame_space_creation(
            players,
            transition_grid,
            epv_grid,
            ball_position=(50.0, 40.0),
            params=small_params,
        )

        expected_net = result["space_created_m2"] - result["space_destroyed_m2"]
        np.testing.assert_allclose(
            result["net_space_m2"].to_numpy(),
            expected_net.to_numpy(),
            atol=1e-10,
        )

    def test_player_ids_preserved(self) -> None:
        """Output player_id and team columns match input ordering."""
        rng = np.random.default_rng(123)
        players = _make_players(
            home_positions=[(30.0, 40.0), (50.0, 20.0)],
            away_positions=[(70.0, 40.0), (90.0, 60.0)],
        )
        transition_grid = rng.uniform(0.1, 0.9, (32, 50))
        epv_grid = rng.uniform(0.01, 0.3, (16, 25))
        small_params = SpaceCreationParams(grid_cells_x=20, grid_cells_y=14)

        result = compute_frame_space_creation(
            players,
            transition_grid,
            epv_grid,
            ball_position=(50.0, 40.0),
            params=small_params,
        )

        assert list(result["player_id"]) == ["h0", "h1", "a0", "a1"]
        assert list(result["team"]) == ["home", "home", "away", "away"]
