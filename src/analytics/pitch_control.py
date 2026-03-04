"""Physics-based pitch control model (Spearman 2017).

Computes a continuous probability surface indicating which team controls each
point on the pitch, accounting for player positions, velocities, and
time-to-intercept kinematic equations.

Reference: Spearman (2017) "Beyond Expected Goals"
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PitchControlParams:
    """Parameters for the physics-based pitch control model."""

    reaction_time: float = 0.7  # seconds before player begins moving
    max_acceleration: float = 7.0  # m/s² acceleration capability
    sigma: float = 0.45  # seconds — controls logistic curve steepness
    grid_cells_x: int = 50
    grid_cells_y: int = 32
    pitch_length_m: float = 105.0  # meters
    pitch_width_m: float = 68.0  # meters
    sb_length: float = 120.0  # StatsBomb pitch length
    sb_width: float = 80.0  # StatsBomb pitch width


def _col_f64(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract a DataFrame column as a float64 numpy array (pyright-safe)."""
    return np.asarray(df[col], dtype=np.float64)


# ---------------------------------------------------------------------------
# Coordinate conversion helpers (StatsBomb 120x80 <-> meters 105x68)
# ---------------------------------------------------------------------------


def _sb_to_meters_x(x: np.ndarray, params: PitchControlParams) -> np.ndarray:
    """Convert StatsBomb x-coordinates to meters."""
    return x * (params.pitch_length_m / params.sb_length)


def _sb_to_meters_y(y: np.ndarray, params: PitchControlParams) -> np.ndarray:
    """Convert StatsBomb y-coordinates to meters."""
    return y * (params.pitch_width_m / params.sb_width)


def _meters_to_sb_x(x: np.ndarray, params: PitchControlParams) -> np.ndarray:
    """Convert meters x-coordinates to StatsBomb."""
    return x * (params.sb_length / params.pitch_length_m)


def _meters_to_sb_y(y: np.ndarray, params: PitchControlParams) -> np.ndarray:
    """Convert meters y-coordinates to StatsBomb."""
    return y * (params.sb_width / params.pitch_width_m)


# ---------------------------------------------------------------------------
# Core kinematic computations
# ---------------------------------------------------------------------------


def _compute_time_to_intercept(
    player_pos_m: np.ndarray,
    player_vel_m: np.ndarray,
    target_m: np.ndarray,
    params: PitchControlParams,
) -> np.ndarray:
    """Compute time-to-intercept for each player to each target point.

    Uses kinematic equation:
        TTI = reaction_time + (-v_proj + sqrt(v_proj² + 2*a_max*d)) / a_max

    Parameters
    ----------
    player_pos_m : (n_players, 2) array of player positions in meters
    player_vel_m : (n_players, 2) array of player velocities in m/s
    target_m : (n_targets, 2) array of target positions in meters
    params : PitchControlParams

    Returns
    -------
    (n_players, n_targets) array of time-to-intercept values in seconds
    """
    # Displacement vectors: (n_players, n_targets, 2)
    displacement = target_m[np.newaxis, :, :] - player_pos_m[:, np.newaxis, :]

    # Distance to each target: (n_players, n_targets)
    distance = np.sqrt(np.sum(displacement**2, axis=2))

    # Unit direction vectors (avoid division by zero)
    safe_distance = np.maximum(distance, 1e-10)
    direction = displacement / safe_distance[:, :, np.newaxis]

    # Project velocity onto direction to target: (n_players, n_targets)
    v_proj = np.sum(player_vel_m[:, np.newaxis, :] * direction, axis=2)

    # Kinematic TTI: time to cover distance d with initial velocity v_proj and max acceleration
    a_max = params.max_acceleration
    discriminant = v_proj**2 + 2.0 * a_max * distance
    # Discriminant is always >= 0 since distance >= 0 and a_max > 0
    tti = params.reaction_time + (-v_proj + np.sqrt(discriminant)) / a_max

    # Clamp to reaction_time minimum (player already at target)
    return np.maximum(tti, params.reaction_time)


def _compute_team_influence(
    team_tti: np.ndarray,
    opponent_min_tti: np.ndarray,
    params: PitchControlParams,
) -> np.ndarray:
    """Compute team influence at each grid cell using logistic sigmoid.

    Parameters
    ----------
    team_tti : (n_players, n_targets) array of TTI values for one team
    opponent_min_tti : (n_targets,) array of minimum opponent TTI per cell
    params : PitchControlParams

    Returns
    -------
    (n_targets,) array of summed team influence values
    """
    # Logistic sigmoid: influence_i = 1 / (1 + exp(-pi/sqrt(3)/sigma * (tau_opp_min - t_i)))
    k = math.pi / math.sqrt(3.0) / params.sigma
    exponent = -k * (opponent_min_tti[np.newaxis, :] - team_tti)
    individual_influence = 1.0 / (1.0 + np.exp(np.clip(exponent, -50.0, 50.0)))

    # Sum across all players in the team
    return np.sum(individual_influence, axis=0)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_pitch_control_frame(
    players_df: pd.DataFrame,
    params: PitchControlParams | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute physics-based pitch control surface for a single frame.

    Parameters
    ----------
    players_df : DataFrame with columns: x, y, velocity_x, velocity_y, team.
        Coordinates in StatsBomb system (120x80). Velocities in StatsBomb units/s.
    params : PitchControlParams (uses defaults if None).

    Returns
    -------
    (grid_x, grid_y, surface) where:
        grid_x : (nx,) array of StatsBomb x-coordinates
        grid_y : (ny,) array of StatsBomb y-coordinates
        surface : (ny, nx) array with values in [0, 1].
            1.0 = full home control, 0.0 = full away control, 0.5 = contested.
    """
    if params is None:
        params = PitchControlParams()

    # Build StatsBomb-coordinate grid
    grid_x_sb = np.linspace(0, params.sb_length, params.grid_cells_x)
    grid_y_sb = np.linspace(0, params.sb_width, params.grid_cells_y)

    # Handle empty input
    if players_df.empty:
        surface = np.full((params.grid_cells_y, params.grid_cells_x), 0.5)
        return grid_x_sb, grid_y_sb, surface

    # Convert grid to meters for kinematic calculations
    grid_x_m = _sb_to_meters_x(grid_x_sb, params)
    grid_y_m = _sb_to_meters_y(grid_y_sb, params)

    # Target points as (nx*ny, 2) array in meters
    gx, gy = np.meshgrid(grid_x_m, grid_y_m)
    targets_m = np.column_stack([gx.ravel(), gy.ravel()])

    # Split into home/away
    home_df = pd.DataFrame(players_df[players_df["team"] == "home"])
    away_df = pd.DataFrame(players_df[players_df["team"] == "away"])

    # Handle edge cases: one team missing
    if home_df.empty and away_df.empty:
        surface = np.full((params.grid_cells_y, params.grid_cells_x), 0.5)
        return grid_x_sb, grid_y_sb, surface
    if home_df.empty:
        surface = np.full((params.grid_cells_y, params.grid_cells_x), 0.0)
        return grid_x_sb, grid_y_sb, surface
    if away_df.empty:
        surface = np.full((params.grid_cells_y, params.grid_cells_x), 1.0)
        return grid_x_sb, grid_y_sb, surface

    # Extract positions and velocities, convert to meters
    home_pos_m = np.column_stack(
        [_sb_to_meters_x(_col_f64(home_df, "x"), params), _sb_to_meters_y(_col_f64(home_df, "y"), params)]
    )
    home_vel_m = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(home_df, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(home_df, "velocity_y"), params),
        ]
    )
    away_pos_m = np.column_stack(
        [_sb_to_meters_x(_col_f64(away_df, "x"), params), _sb_to_meters_y(_col_f64(away_df, "y"), params)]
    )
    away_vel_m = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(away_df, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(away_df, "velocity_y"), params),
        ]
    )

    # Compute TTI for each team: (n_players, n_targets)
    home_tti = _compute_time_to_intercept(home_pos_m, home_vel_m, targets_m, params)
    away_tti = _compute_time_to_intercept(away_pos_m, away_vel_m, targets_m, params)

    # Minimum TTI per cell for each team
    home_min_tti = np.min(home_tti, axis=0)  # (n_targets,)
    away_min_tti = np.min(away_tti, axis=0)  # (n_targets,)

    # Compute influence using opponent's min TTI as reference
    home_influence = _compute_team_influence(home_tti, away_min_tti, params)
    away_influence = _compute_team_influence(away_tti, home_min_tti, params)

    # Combine: home / (home + away), with safe division
    total = home_influence + away_influence
    safe_total = np.where(total > 1e-10, total, 1.0)
    control_flat = np.where(total > 1e-10, home_influence / safe_total, 0.5)

    # Clamp to [0, 1]
    control_flat = np.clip(control_flat, 0.0, 1.0)

    surface = control_flat.reshape(params.grid_cells_y, params.grid_cells_x)
    return grid_x_sb, grid_y_sb, surface


def compute_pitch_control_at_point(
    players_df: pd.DataFrame,
    target_x: float,
    target_y: float,
    params: PitchControlParams | None = None,
) -> float:
    """Compute pitch control value at a single point (StatsBomb coordinates).

    Convenience function for downstream analytics (e.g., EPV at ball position).

    Returns a float in [0, 1]: 1.0 = full home control, 0.0 = full away control.
    """
    if params is None:
        params = PitchControlParams()

    if players_df.empty:
        return 0.5

    home_df = pd.DataFrame(players_df[players_df["team"] == "home"])
    away_df = pd.DataFrame(players_df[players_df["team"] == "away"])

    if home_df.empty and away_df.empty:
        return 0.5
    if home_df.empty:
        return 0.0
    if away_df.empty:
        return 1.0

    # Single target point in meters
    target_m = np.array(
        [
            [
                target_x * (params.pitch_length_m / params.sb_length),
                target_y * (params.pitch_width_m / params.sb_width),
            ]
        ]
    )

    # Positions and velocities in meters
    home_pos_m = np.column_stack(
        [_sb_to_meters_x(_col_f64(home_df, "x"), params), _sb_to_meters_y(_col_f64(home_df, "y"), params)]
    )
    home_vel_m = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(home_df, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(home_df, "velocity_y"), params),
        ]
    )
    away_pos_m = np.column_stack(
        [_sb_to_meters_x(_col_f64(away_df, "x"), params), _sb_to_meters_y(_col_f64(away_df, "y"), params)]
    )
    away_vel_m = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(away_df, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(away_df, "velocity_y"), params),
        ]
    )

    home_tti = _compute_time_to_intercept(home_pos_m, home_vel_m, target_m, params)
    away_tti = _compute_time_to_intercept(away_pos_m, away_vel_m, target_m, params)

    home_min_tti = np.min(home_tti, axis=0)
    away_min_tti = np.min(away_tti, axis=0)

    home_influence = _compute_team_influence(home_tti, away_min_tti, params)
    away_influence = _compute_team_influence(away_tti, home_min_tti, params)

    total = home_influence[0] + away_influence[0]
    if total < 1e-10:
        return 0.5
    return float(np.clip(home_influence[0] / total, 0.0, 1.0))
