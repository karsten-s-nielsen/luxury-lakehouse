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
    from analytics.obso import interpolate_grid
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

    # Pre-compute the loop-invariant OBSO multiplier ONCE (F-01 OPT-AUDIT-200).
    # compute_obso_surface internally re-interpolates grids and recomputes
    # the Gaussian distance weight on every call — all constant across the
    # N player-removal variants.  Hoisting converts O(N x grid_size)
    # redundant work to O(1).
    ball_x, ball_y = ball_position
    xx, yy = np.meshgrid(grid_x, grid_y)
    sigma_x, sigma_y = 30.0, 20.0
    distance_weight = np.exp(-((xx - ball_x) ** 2) / (2.0 * sigma_x**2) - (yy - ball_y) ** 2 / (2.0 * sigma_y**2))
    effective_transition = transition_interp * distance_weight
    max_trans = np.max(effective_transition)
    if max_trans > 1e-10:
        effective_transition = effective_transition / max_trans
    obso_multiplier = effective_transition * epv_interp  # (ny, nx) — constant

    # Baseline OBSO via broadcast
    baseline_obso = np.clip(baseline_surface * obso_multiplier, 0.0, 1.0)

    # Vectorized per-player OBSO via broadcast: (n_players, ny, nx)
    n_players = len(players_df)
    all_removed = removed_pc.reshape(n_players, ny, nx)
    all_removed_obso = np.clip(all_removed * obso_multiplier[None, :, :], 0.0, 1.0)

    # Cell dimensions for area integration
    dx = float(grid_x[1] - grid_x[0]) if len(grid_x) > 1 else 1.0
    dy = float(grid_y[1] - grid_y[0]) if len(grid_y) > 1 else 1.0
    cell_area = dx * dy

    # Vectorized delta computation: (n_players, ny, nx)
    delta = baseline_obso[None, :, :] - all_removed_obso
    positive_delta = np.maximum(delta, 0.0)
    negative_delta = np.minimum(delta, 0.0)

    space_created_arr = np.sum(positive_delta, axis=(1, 2)) * cell_area
    space_destroyed_arr = np.sum(np.abs(negative_delta), axis=(1, 2)) * cell_area
    net_space_arr = space_created_arr - space_destroyed_arr

    return pd.DataFrame(
        {
            "player_id": list(players_df["player_id"]),
            "team": list(players_df["team"]),
            "space_created_m2": space_created_arr,
            "space_destroyed_m2": space_destroyed_arr,
            "net_space_m2": net_space_arr,
        }
    )
