"""Helper module for compute_epv_transition_hf.py.

Contains OBSOGridParams, coordinate normalization, zone assignment, transition
matrix, value iteration, ball reachability grid, EPV grid, grid validation,
and grid-to-DataFrame converters. The main script handles data loading, MLflow
logging, HF Hub publishing, and pipeline orchestration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from analytics.obso import interpolate_grid

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OBSOGridParams:
    """Configuration for OBSO grid computation."""

    transition_zones_x: int = 64
    transition_zones_y: int = 100
    epv_zones_x: int = 32
    epv_zones_y: int = 50
    intermediate_x: int = 16
    intermediate_y: int = 25
    pitch_length: float = 105.0
    pitch_width: float = 68.0
    max_iterations: int = 100
    tolerance: float = 1e-6


# SPADL action types
_MOVE_TYPES = frozenset(
    {
        "pass",
        "cross",
        "throw_in",
        "freekick_crossed",
        "freekick_short",
        "corner_crossed",
        "corner_short",
        "take_on",
        "dribble",
        "goalkick",
        "clearance",
    }
)
_SHOT_TYPES = frozenset({"shot", "shot_penalty", "shot_freekick"})
RELEVANT_TYPES = _MOVE_TYPES | _SHOT_TYPES


# ---------------------------------------------------------------------------
# Coordinate normalization
# ---------------------------------------------------------------------------


def normalize_attack_direction(df: pd.DataFrame, params: OBSOGridParams) -> pd.DataFrame:
    """Normalize coordinates so all actions attack toward x=pitch_length."""
    pitch_l = params.pitch_length
    pitch_w = params.pitch_width
    midfield = pitch_l / 2.0
    df = df.copy()
    is_shot = df["type_name"].isin(_SHOT_TYPES)
    shots = df[is_shot]
    if shots.empty:
        return df

    shot_means = shots.groupby(["match_id", "team_id", "period"])["start_x"].mean()
    flip_lookup: dict[tuple, bool] = {}
    for key, mean_x in shot_means.items():
        flip_lookup[key] = mean_x < midfield  # type: ignore[index]

    group_keys = set(df.groupby(["match_id", "team_id", "period"]).groups.keys())
    for key in group_keys:
        if key not in flip_lookup:
            match_id, team_id, period = key  # type: ignore[misc]
            other_key = (match_id, team_id, 2 if period == 1 else 1)
            if other_key in flip_lookup:
                flip_lookup[key] = not flip_lookup[other_key]

    n_flip = sum(1 for v in flip_lookup.values() if v)
    logger.info("Coordinate normalization: %d/%d team-period groups flipped", n_flip, len(flip_lookup))
    if n_flip == 0:
        return df

    flip_mask = np.zeros(len(df), dtype=bool)
    for key, should_flip in flip_lookup.items():
        if should_flip:
            match_id, team_id, period = key  # type: ignore[misc]
            group_mask = (df["match_id"] == match_id) & (df["team_id"] == team_id) & (df["period"] == period)
            flip_mask |= group_mask.values

    for col in ["start_x", "end_x"]:
        if col in df.columns:
            df.loc[flip_mask, col] = pitch_l - df.loc[flip_mask, col]
    for col in ["start_y", "end_y"]:
        if col in df.columns:
            df.loc[flip_mask, col] = pitch_w - df.loc[flip_mask, col]

    post_shots = df[is_shot]
    post_pct = (post_shots["start_x"] > midfield).mean() * 100
    logger.info("Post-normalization: %.0f%% shots in attacking half", post_pct)
    return df


# ---------------------------------------------------------------------------
# Zone assignment + transition matrix + value iteration
# ---------------------------------------------------------------------------


def assign_zones(
    x: np.ndarray, y: np.ndarray, n_zones_x: int, n_zones_y: int, pitch_length: float, pitch_width: float
) -> np.ndarray:
    """Map (x, y) coordinates to flat zone indices."""
    zone_x = np.clip((x / pitch_length * n_zones_x).astype(int), 0, n_zones_x - 1)
    zone_y = np.clip((y / pitch_width * n_zones_y).astype(int), 0, n_zones_y - 1)
    return zone_x * n_zones_y + zone_y


def build_transition_matrix(start_zones: np.ndarray, end_zones: np.ndarray, n_zones: int) -> np.ndarray:
    """Build row-normalized transition matrix from zone-to-zone moves."""
    transition = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(transition, (start_zones, end_zones), 1.0)
    row_sums = np.maximum(transition.sum(axis=1, keepdims=True), 1.0)
    return transition / row_sums


def value_iteration(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    max_iters: int,
    tol: float,
) -> tuple[np.ndarray, int]:
    """NumPy Bellman value iteration for EPV."""
    epv = np.zeros_like(shot_prob)
    for i in range(max_iters):
        epv_new = shot_prob * goal_prob + move_prob * (transition @ epv)
        delta = float(np.max(np.abs(epv_new - epv)))
        epv = epv_new
        if delta < tol:
            return epv, i + 1
    return epv, max_iters


# ---------------------------------------------------------------------------
# Grid computation
# ---------------------------------------------------------------------------


def compute_ball_reachability_grid(actions_df: pd.DataFrame, params: OBSOGridParams) -> np.ndarray:
    """Compute a global ball reachability surface from pass completion data."""
    n_int_x = params.intermediate_x
    n_int_y = params.intermediate_y
    n_int_zones = n_int_x * n_int_y

    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"
    succ_move_mask = is_move & is_success

    start_x = np.asarray(actions_df["start_x"], dtype=np.float64)
    start_y = np.asarray(actions_df["start_y"], dtype=np.float64)
    end_x = np.asarray(actions_df["end_x"], dtype=np.float64)
    end_y = np.asarray(actions_df["end_y"], dtype=np.float64)

    start_zones = assign_zones(start_x, start_y, n_int_x, n_int_y, params.pitch_length, params.pitch_width)
    end_zones = assign_zones(end_x, end_y, n_int_x, n_int_y, params.pitch_length, params.pitch_width)

    attempted = np.zeros((n_int_zones, n_int_zones), dtype=np.float64)
    np.add.at(attempted, (start_zones[is_move], end_zones[is_move]), 1.0)
    completed = np.zeros((n_int_zones, n_int_zones), dtype=np.float64)
    np.add.at(completed, (start_zones[succ_move_mask], end_zones[succ_move_mask]), 1.0)
    completion_matrix = completed / np.maximum(attempted, 1.0)

    origin_counts = np.bincount(start_zones[is_move], minlength=n_int_zones).astype(np.float64)
    origin_frequency = origin_counts / np.maximum(origin_counts.sum(), 1.0)
    reachability_flat = origin_frequency @ completion_matrix
    reachability_2d = reachability_flat.reshape(n_int_x, n_int_y).T

    max_val = reachability_2d.max()
    if max_val > 1e-10:
        reachability_2d = reachability_2d / max_val

    output = interpolate_grid(reachability_2d, (params.transition_zones_y, params.transition_zones_x))
    return np.clip(output, 0.0, 1.0)


def compute_epv_grid(actions_df: pd.DataFrame, params: OBSOGridParams) -> np.ndarray:
    """Compute EPV grid via Markov chain value iteration."""
    n_x = params.epv_zones_x
    n_y = params.epv_zones_y
    n_zones = n_x * n_y

    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_shot = np.array([t in _SHOT_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"

    start_zones = assign_zones(
        np.asarray(actions_df["start_x"], dtype=np.float64),
        np.asarray(actions_df["start_y"], dtype=np.float64),
        n_x,
        n_y,
        params.pitch_length,
        params.pitch_width,
    )
    end_zones = assign_zones(
        np.asarray(actions_df["end_x"], dtype=np.float64),
        np.asarray(actions_df["end_y"], dtype=np.float64),
        n_x,
        n_y,
        params.pitch_length,
        params.pitch_width,
    )

    total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
    shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
    goals_per_zone = np.bincount(start_zones[is_shot & is_success], minlength=n_zones).astype(np.float64)
    successful_moves = is_move & is_success
    succ_moves_per_zone = np.bincount(start_zones[successful_moves], minlength=n_zones).astype(np.float64)

    safe_total = np.maximum(total_per_zone, 1.0)
    shot_prob = shots_per_zone / safe_total
    goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
    move_prob = succ_moves_per_zone / safe_total

    transition = build_transition_matrix(start_zones[successful_moves], end_zones[successful_moves], n_zones)
    epv_flat, iters = value_iteration(
        shot_prob, goal_prob, move_prob, transition, params.max_iterations, params.tolerance
    )
    logger.info("EPV value iteration converged in %d iterations", iters)
    return epv_flat.reshape(n_x, n_y).T


# ---------------------------------------------------------------------------
# Grid validation
# ---------------------------------------------------------------------------


def validate_reachability_grid(grid: np.ndarray, params: OBSOGridParams) -> None:
    """Validate computed reachability grid meets data quality requirements."""
    expected = (params.transition_zones_y, params.transition_zones_x)
    if grid.shape != expected:
        raise ValueError(f"Reachability grid shape {grid.shape} != expected {expected}")
    if np.any(np.isnan(grid)):
        raise ValueError("Reachability grid contains NaN values")
    if grid.min() < 0.0 or grid.max() > 1.0:
        raise ValueError(f"Reachability grid values out of [0, 1]: min={grid.min():.4f}, max={grid.max():.4f}")
    if grid.max() - grid.min() < 0.05:
        raise ValueError(f"Reachability grid range too narrow ({grid.max() - grid.min():.4f})")
    cy, cx = grid.shape[0] // 2, grid.shape[1] // 2
    center = grid[cy - 5 : cy + 5, cx - 5 : cx + 5].mean()
    corners = (grid[:5, :5].mean() + grid[-5:, -5:].mean() + grid[:5, -5:].mean() + grid[-5:, :5].mean()) / 4.0
    if center < corners:
        logger.warning("Reachability center (%.4f) lower than corners (%.4f)", center, corners)


def validate_epv_grid(grid: np.ndarray, params: OBSOGridParams) -> None:
    """Validate computed EPV grid meets data quality requirements."""
    expected = (params.epv_zones_y, params.epv_zones_x)
    if grid.shape != expected:
        raise ValueError(f"EPV grid shape {grid.shape} != expected {expected}")
    if np.any(np.isnan(grid)):
        raise ValueError("EPV grid contains NaN values")
    if grid.max() - grid.min() < 0.01:
        raise ValueError(f"EPV grid range too narrow ({grid.max() - grid.min():.4f})")
    if grid.max() > 1.0:
        raise ValueError(f"EPV grid max exceeds 1.0: {grid.max():.4f}")
    col_means = grid.mean(axis=0)
    if col_means[-1] < col_means[0]:
        msg = (
            f"EPV not increasing toward goal: x=0 mean={col_means[0]:.5f} "
            f"> x={params.epv_zones_x - 1} mean={col_means[-1]:.5f}"
        )
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Grid-to-DataFrame converters
# ---------------------------------------------------------------------------


def reachability_grid_to_dataframe(grid: np.ndarray, competition_id: str) -> pd.DataFrame:
    """Convert reachability grid to long-format DataFrame."""
    ny, nx = grid.shape
    zone_y_arr, zone_x_arr = np.mgrid[0:ny, 0:nx]
    return pd.DataFrame(
        {
            "zone_y": zone_y_arr.ravel(),
            "zone_x": zone_x_arr.ravel(),
            "reachability": np.round(grid.ravel(), 6),
            "competition_id": competition_id,
        }
    )


def epv_grid_to_dataframe(grid: np.ndarray, competition_id: str) -> pd.DataFrame:
    """Convert EPV grid to long-format DataFrame."""
    ny, nx = grid.shape
    zone_y_arr, zone_x_arr = np.mgrid[0:ny, 0:nx]
    return pd.DataFrame(
        {
            "zone_y": zone_y_arr.ravel(),
            "zone_x": zone_x_arr.ravel(),
            "epv_value": np.round(grid.ravel(), 6),
            "competition_id": competition_id,
        }
    )


def completion_matrix_to_dataframe(
    actions_df: pd.DataFrame, params: OBSOGridParams, competition_id: str
) -> pd.DataFrame:
    """Build and export the sparse pass completion matrix in long format."""
    n_int_x = params.intermediate_x
    n_int_y = params.intermediate_y
    n_int_zones = n_int_x * n_int_y
    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    succ_move_mask = is_move & (result_names == "success")

    start_zones = assign_zones(
        np.asarray(actions_df["start_x"], dtype=np.float64),
        np.asarray(actions_df["start_y"], dtype=np.float64),
        n_int_x,
        n_int_y,
        params.pitch_length,
        params.pitch_width,
    )
    end_zones = assign_zones(
        np.asarray(actions_df["end_x"], dtype=np.float64),
        np.asarray(actions_df["end_y"], dtype=np.float64),
        n_int_x,
        n_int_y,
        params.pitch_length,
        params.pitch_width,
    )
    transition = build_transition_matrix(start_zones[succ_move_mask], end_zones[succ_move_mask], n_int_zones)
    nonzero = np.nonzero(transition)
    return pd.DataFrame(
        {
            "origin_zone": nonzero[0],
            "target_zone": nonzero[1],
            "probability": np.round(transition[nonzero], 6),
            "competition_id": competition_id,
        }
    )
