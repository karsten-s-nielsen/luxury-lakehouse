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

try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False


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


def _tti_numpy(
    player_pos_m: np.ndarray,
    player_vel_m: np.ndarray,
    target_m: np.ndarray,
    reaction_time: float,
    max_acceleration: float,
) -> np.ndarray:
    """NumPy TTI kernel — extracted from _compute_time_to_intercept.

    Parameters
    ----------
    player_pos_m : (n_players, 2) array of player positions in meters
    player_vel_m : (n_players, 2) array of player velocities in m/s
    target_m : (n_targets, 2) array of target positions in meters
    reaction_time : seconds before player begins moving
    max_acceleration : m/s² acceleration capability

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
    discriminant = v_proj**2 + 2.0 * max_acceleration * distance
    # Discriminant is always >= 0 since distance >= 0 and max_acceleration > 0
    tti = reaction_time + (-v_proj + np.sqrt(discriminant)) / max_acceleration

    # Clamp to reaction_time minimum (player already at target)
    return np.maximum(tti, reaction_time)


if _USE_JAX:

    @jax.jit  # type: ignore[misc]
    def _tti_jax(
        player_pos_m: jax.Array,
        player_vel_m: jax.Array,
        target_m: jax.Array,
        reaction_time: float,
        max_acceleration: float,
    ) -> jax.Array:
        """JAX JIT TTI kernel — identical math to _tti_numpy."""
        displacement = target_m[jnp.newaxis, :, :] - player_pos_m[:, jnp.newaxis, :]
        distance = jnp.sqrt(jnp.sum(displacement**2, axis=2))
        safe_distance = jnp.maximum(distance, 1e-10)
        direction = displacement / safe_distance[:, :, jnp.newaxis]
        v_proj = jnp.sum(player_vel_m[:, jnp.newaxis, :] * direction, axis=2)
        discriminant = v_proj**2 + 2.0 * max_acceleration * distance
        tti = reaction_time + (-v_proj + jnp.sqrt(discriminant)) / max_acceleration
        return jnp.maximum(tti, reaction_time)


def _compute_time_to_intercept(
    player_pos_m: np.ndarray,
    player_vel_m: np.ndarray,
    target_m: np.ndarray,
    params: PitchControlParams,
) -> np.ndarray:
    """Compute time-to-intercept for each player to each target point.

    Dispatches to JAX JIT kernel when available, falling back to NumPy.

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
    if _USE_JAX:
        result = _tti_jax(
            jnp.asarray(player_pos_m),
            jnp.asarray(player_vel_m),
            jnp.asarray(target_m),
            params.reaction_time,
            params.max_acceleration,
        )
        return np.asarray(result)
    return _tti_numpy(player_pos_m, player_vel_m, target_m, params.reaction_time, params.max_acceleration)


def _influence_numpy(
    team_tti: np.ndarray,
    opponent_min_tti: np.ndarray,
    sigma: float,
) -> np.ndarray:
    """NumPy influence kernel — extracted from _compute_team_influence.

    Parameters
    ----------
    team_tti : (n_players, n_targets) array of TTI values for one team
    opponent_min_tti : (n_targets,) array of minimum opponent TTI per cell
    sigma : logistic curve steepness parameter (seconds)

    Returns
    -------
    (n_targets,) array of summed team influence values
    """
    k = math.pi / math.sqrt(3.0) / sigma
    exponent = -k * (opponent_min_tti[np.newaxis, :] - team_tti)
    individual_influence = 1.0 / (1.0 + np.exp(np.clip(exponent, -50.0, 50.0)))
    return np.sum(individual_influence, axis=0)


if _USE_JAX:

    @jax.jit  # type: ignore[misc]
    def _influence_jax(
        team_tti: jax.Array,
        opponent_min_tti: jax.Array,
        sigma: float,
    ) -> jax.Array:
        """JAX JIT influence kernel — identical math to _influence_numpy."""
        k = jnp.pi / jnp.sqrt(3.0) / sigma
        exponent = -k * (opponent_min_tti[jnp.newaxis, :] - team_tti)
        individual = 1.0 / (1.0 + jnp.exp(jnp.clip(exponent, -50.0, 50.0)))
        return jnp.sum(individual, axis=0)


def _compute_team_influence(
    team_tti: np.ndarray,
    opponent_min_tti: np.ndarray,
    params: PitchControlParams,
) -> np.ndarray:
    """Compute team influence at each grid cell using logistic sigmoid.

    Dispatches to JAX JIT kernel when available, falling back to NumPy.

    Parameters
    ----------
    team_tti : (n_players, n_targets) array of TTI values for one team
    opponent_min_tti : (n_targets,) array of minimum opponent TTI per cell
    params : PitchControlParams

    Returns
    -------
    (n_targets,) array of summed team influence values
    """
    if _USE_JAX:
        result = _influence_jax(
            jnp.asarray(team_tti),
            jnp.asarray(opponent_min_tti),
            params.sigma,
        )
        return np.asarray(result)
    return _influence_numpy(team_tti, opponent_min_tti, params.sigma)


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


def compute_pitch_control_at_points(
    players_df: pd.DataFrame,
    target_points: np.ndarray,
    params: PitchControlParams | None = None,
) -> np.ndarray:
    """Compute pitch control at multiple target points in a single pass.

    Vectorised alternative to calling ``compute_pitch_control_at_point`` in a
    loop.  Coordinate conversion, team splitting and TTI computation are each
    performed **once** for the full batch, eliminating redundant work.

    Parameters
    ----------
    players_df : DataFrame with columns: player_id, team, x, y, velocity_x, velocity_y.
        Coordinates in StatsBomb system (120x80).  Velocities in StatsBomb units/s.
    target_points : (N, 2) array of (x, y) target positions in StatsBomb coordinates.
    params : Pitch control parameters (uses defaults if ``None``).

    Returns
    -------
    (N,) array of home-team pitch control values in [0, 1].
    """
    if params is None:
        params = PitchControlParams()
    if len(target_points) == 0:
        return np.empty(0)

    home = pd.DataFrame(players_df[players_df["team"] == "home"])
    away = pd.DataFrame(players_df[players_df["team"] == "away"])

    if home.empty or away.empty:
        fallback = 0.5 if (home.empty and away.empty) else (1.0 if away.empty else 0.0)
        return np.full(len(target_points), fallback)

    # Single coordinate conversion (done ONCE, not per-target)
    home_pos = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(home, "x"), params),
            _sb_to_meters_y(_col_f64(home, "y"), params),
        ]
    )
    home_vel = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(home, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(home, "velocity_y"), params),
        ]
    )
    away_pos = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(away, "x"), params),
            _sb_to_meters_y(_col_f64(away, "y"), params),
        ]
    )
    away_vel = np.column_stack(
        [
            _sb_to_meters_x(_col_f64(away, "velocity_x"), params),
            _sb_to_meters_y(_col_f64(away, "velocity_y"), params),
        ]
    )

    # Convert targets to meters
    targets_m = np.column_stack(
        [
            _sb_to_meters_x(target_points[:, 0], params),
            _sb_to_meters_y(target_points[:, 1], params),
        ]
    )

    # Single TTI computation for all targets (vectorized)
    home_tti = _compute_time_to_intercept(home_pos, home_vel, targets_m, params)
    away_tti = _compute_time_to_intercept(away_pos, away_vel, targets_m, params)

    # Compute min TTI for opponent penalty
    home_min_tti = np.min(home_tti, axis=0)  # (n_targets,)
    away_min_tti = np.min(away_tti, axis=0)  # (n_targets,)

    # _compute_team_influence takes FULL (n_players, n_targets) TTI
    home_influence = _compute_team_influence(home_tti, away_min_tti, params)
    away_influence = _compute_team_influence(away_tti, home_min_tti, params)

    total = home_influence + away_influence
    safe_total = np.where(total > 1e-10, total, 1.0)
    control = np.where(total > 1e-10, home_influence / safe_total, 0.5)
    return np.clip(control, 0.0, 1.0)


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


def generate_ghost_trajectories(
    players_df: pd.DataFrame,
    event_frame: int,
    frame_rate: int = 25,
    window_before_s: float = 3.0,
    window_after_s: float = 1.0,
) -> list[pd.DataFrame]:
    """Generate constant-velocity extrapolation for counterfactual frames.

    For each player, extrapolate position linearly from their velocity at
    ``event_frame``.  The window spans from ``event_frame - window_before_s *
    frame_rate`` to ``event_frame + window_after_s * frame_rate`` inclusive.
    Positions are clamped to StatsBomb pitch bounds [0, 120] x [0, 80].

    Args:
        players_df: Tracking frame with columns: player_id, x, y,
            velocity_x, velocity_y, team.  Positions in StatsBomb 120x80
            coordinates.
        event_frame: The frame number of the event.
        frame_rate: Frames per second (25 for IDSSE).
        window_before_s: Seconds before event to extrapolate.
        window_after_s: Seconds after event to extrapolate.

    Returns:
        List of DataFrames, one per ghost frame.  Each has the same columns
        as the input plus ``ghost_frame_offset`` (int, relative to
        ``event_frame``).

    Reference:
        Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.
    """
    # Extract positions and velocities as arrays
    x = np.asarray(players_df["x"], dtype=np.float64)
    y = np.asarray(players_df["y"], dtype=np.float64)
    vx = np.asarray(players_df["velocity_x"], dtype=np.float64)
    vy = np.asarray(players_df["velocity_y"], dtype=np.float64)

    n_players = len(players_df)

    # Compute frame offsets
    start_offset = -int(window_before_s * frame_rate)
    end_offset = int(window_after_s * frame_rate)

    # Pre-extract metadata columns once (avoid repeated copies)
    meta_cols = [c for c in players_df.columns if c not in {"x", "y", "velocity_x", "velocity_y"}]
    meta_dict = {c: players_df[c].to_numpy() for c in meta_cols}

    frames: list[pd.DataFrame] = []
    for offset in range(start_offset, end_offset + 1):
        dt = offset / frame_rate

        # Constant-velocity extrapolation
        new_x = np.clip(x + vx * dt, 0.0, 120.0)
        new_y = np.clip(y + vy * dt, 0.0, 80.0)

        data: dict[str, object] = {}
        for c in meta_cols:
            data[c] = meta_dict[c].copy()
        data["x"] = new_x.copy()
        data["y"] = new_y.copy()
        data["velocity_x"] = vx.copy()
        data["velocity_y"] = vy.copy()
        data["ghost_frame_offset"] = np.full(n_players, offset, dtype=np.int64)

        frames.append(pd.DataFrame(data))

    return frames


def compute_pitch_control_grid_fast(
    players_df: pd.DataFrame,
    grid_cells_x: int = 104,
    grid_cells_y: int = 68,
    params: PitchControlParams | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Compute pitch control over a dense grid using JAX acceleration.

    Designed for OBSO-style value surfaces where a fine-grained control grid
    is needed (e.g. 104x68 = 7,072 cells).  Uses the JAX JIT kernels for
    the heavy TTI and influence computations, delegating to
    ``compute_pitch_control_at_points`` for the actual math.

    Parameters
    ----------
    players_df : DataFrame with columns: player_id, team, x, y, velocity_x, velocity_y.
        Coordinates in StatsBomb system (120x80).
    grid_cells_x : Number of grid cells along the x-axis.
    grid_cells_y : Number of grid cells along the y-axis.
    params : Pitch control parameters (uses defaults if ``None``).

    Returns
    -------
    grid_x : (grid_cells_x,) array of StatsBomb x-coordinates.
    grid_y : (grid_cells_y,) array of StatsBomb y-coordinates.
    surface : (grid_cells_y, grid_cells_x) array of control values in [0, 1].

    Raises
    ------
    ImportError
        If JAX is not installed.
    """
    if not _USE_JAX:
        raise ImportError("JAX required for compute_pitch_control_grid_fast")

    if params is None:
        params = PitchControlParams()

    # Build StatsBomb-coordinate grid
    grid_x = np.linspace(0, params.sb_length, grid_cells_x)
    grid_y = np.linspace(0, params.sb_width, grid_cells_y)

    # Flatten grid to (grid_cells_x * grid_cells_y, 2) target points
    xx, yy = np.meshgrid(grid_x, grid_y)
    target_points = np.column_stack([xx.ravel(), yy.ravel()])

    # Use the existing batched API which auto-dispatches to JAX
    control = compute_pitch_control_at_points(players_df, target_points, params)

    surface = control.reshape(grid_cells_y, grid_cells_x)
    return grid_x, grid_y, surface
