"""Space Creation quantification (Fernandez & Bornn 2018).

Measures each player's contribution to the team's off-ball scoring
opportunity by computing differential OBSO: how much the team's
OBSO surface changes when that player is removed.

References:
    Fernandez, J. & Bornn, L. (2018). "Wide Open Spaces: A statistical
    technique for measuring space creation in professional soccer."
    MIT Sloan Sports Analytics Conference.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SpaceCreationParams:
    """Parameters for space creation computation."""

    grid_cells_x: int = 104
    grid_cells_y: int = 68
    pitch_length: float = 120.0  # StatsBomb
    pitch_width: float = 80.0


def compute_space_created(
    baseline_obso: np.ndarray,
    removed_obso: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
) -> float:
    """Compute space created by a player via differential OBSO integration.

    Space created is the integral of *positive* (baseline - removed) OBSO
    values over the pitch surface.  Only positive differences count: they
    indicate cells where the player's presence adds scoring opportunity.

    Parameters
    ----------
    baseline_obso : (ny, nx) OBSO surface with all players present.
    removed_obso : (ny, nx) OBSO surface with the player removed.
    grid_x : (nx,) x-coordinates of grid cells (for cell area).
    grid_y : (ny,) y-coordinates of grid cells (for cell area).

    Returns
    -------
    Non-negative float representing the total space created (area-weighted).
    """
    delta = baseline_obso - removed_obso
    positive_delta = np.maximum(delta, 0.0)

    # Cell area: uniform spacing assumed from linspace grids
    dx = float(grid_x[1] - grid_x[0]) if len(grid_x) > 1 else 1.0
    dy = float(grid_y[1] - grid_y[0]) if len(grid_y) > 1 else 1.0
    cell_area = dx * dy

    return float(np.sum(positive_delta) * cell_area)


def compute_frame_space_creation(
    players_df: pd.DataFrame,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    ball_position: tuple[float, float],
    params: SpaceCreationParams | None = None,
) -> pd.DataFrame:
    """Compute per-player space creation for a single tracking frame.

    For each player, computes differential OBSO by removing the player
    from the pitch control model and measuring the change in scoring
    opportunity surface.

    Parameters
    ----------
    players_df : DataFrame with columns: player_id, team, x, y,
        velocity_x, velocity_y.  Coordinates in StatsBomb 120x80.
    transition_grid : Pre-computed ball transition probability grid
        (any shape; will be interpolated to target grid).
    epv_grid : Pre-computed expected possession value grid
        (any shape; will be interpolated to target grid).
    ball_position : (x, y) ball coordinates in StatsBomb 120x80 space.
    params : Space creation parameters (uses defaults if ``None``).

    Returns
    -------
    DataFrame with columns: player_id, team, space_created_m2,
    space_destroyed_m2, net_space_m2.

    Raises
    ------
    ImportError
        If JAX is not available.
    """
    # Lazy imports to avoid import-time JAX failures
    from analytics.obso import compute_obso_surface, interpolate_grid
    from analytics.pitch_control import (
        PitchControlParams,
        compute_pitch_control_player_removal,
    )

    if params is None:
        params = SpaceCreationParams()

    pc_params = PitchControlParams()

    # Build target grid in StatsBomb coordinates using (ny, nx) convention
    grid_x = np.linspace(0, params.pitch_length, params.grid_cells_x)
    grid_y = np.linspace(0, params.pitch_width, params.grid_cells_y)
    yy, xx = np.meshgrid(grid_y, grid_x, indexing="ij")
    targets = np.column_stack([xx.ravel(), yy.ravel()])

    # Compute pitch control for baseline and all player-removal variants
    baseline_pc, removed_pc = compute_pitch_control_player_removal(players_df, targets, pc_params)

    # Reshape to (ny, nx) grid
    ny = params.grid_cells_y
    nx = params.grid_cells_x
    baseline_surface = baseline_pc.reshape(ny, nx)

    # Interpolate transition and EPV grids to target shape
    transition_interp = interpolate_grid(transition_grid, (ny, nx))
    epv_interp = interpolate_grid(epv_grid, (ny, nx))

    # Compute baseline OBSO surface
    baseline_obso = compute_obso_surface(baseline_surface, transition_interp, epv_interp, ball_position, grid_x, grid_y)

    # Cell dimensions for area integration
    dx = float(grid_x[1] - grid_x[0]) if len(grid_x) > 1 else 1.0
    dy = float(grid_y[1] - grid_y[0]) if len(grid_y) > 1 else 1.0
    cell_area = dx * dy

    # Compute per-player space creation/destruction
    n_players = len(players_df)
    player_ids = list(players_df["player_id"])
    teams = list(players_df["team"])
    results: list[dict[str, object]] = []

    for i in range(n_players):
        removed_surface = removed_pc[i].reshape(ny, nx)
        removed_obso = compute_obso_surface(
            removed_surface, transition_interp, epv_interp, ball_position, grid_x, grid_y
        )

        delta = baseline_obso - removed_obso
        positive_delta = np.maximum(delta, 0.0)
        negative_delta = np.minimum(delta, 0.0)

        space_created = float(np.sum(positive_delta) * cell_area)
        space_destroyed = float(np.sum(np.abs(negative_delta)) * cell_area)
        net_space = space_created - space_destroyed

        results.append(
            {
                "player_id": player_ids[i],
                "team": teams[i],
                "space_created_m2": space_created,
                "space_destroyed_m2": space_destroyed,
                "net_space_m2": net_space,
            }
        )

    return pd.DataFrame(results)
