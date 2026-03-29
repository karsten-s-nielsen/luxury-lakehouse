# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Compute EPV and ball-reachability grids for OBSO on HuggingFace Jobs (CPU).

Downloads SPADL action data from HF Dataset, normalizes coordinate orientation,
computes per-competition + global EPV grids via Markov chain value iteration
and ball reachability grids via pass completion marginalization.

These grids replace the synthetic Gaussian proxies in the OBSO pipeline with
data-driven surfaces trained on real SPADL action data.

Output grids (2D spatial, y-first convention to match OBSO expectations):
  - Ball reachability: (ny=100, nx=64) — marginalized pass completion surface
  - EPV: (ny=50, nx=32) — expected possession value via value iteration

Reference: Spearman (2018) "Beyond Expected Goals." MIT Sloan.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/compute_epv_transition_hf.py \\
        --flavor cpu-basic --timeout 30m \\
        --secrets HF_TOKEN=$HF_TOKEN
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from analytics.cost import HF_RATE_CPU_BASIC, HFJobsCostRecorder
from analytics.obso import interpolate_grid
from workflows import workflow

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
SPADL_DATASET = f"{HF_ORG}/spadl-vaep-action-values"
OUTPUT_DATASET = f"{HF_ORG}/obso-trained-grids"


@dataclass(frozen=True)
class OBSOGridParams:
    """Configuration for OBSO grid computation.

    Two output grids with different resolutions:
    - Reachability: (transition_zones_y, transition_zones_x) = (100, 64)
    - EPV: (epv_zones_y, epv_zones_x) = (50, 32)

    An intermediate resolution (intermediate_y, intermediate_x) = (25, 16) is
    used for building the zone-to-zone pass completion matrix before
    marginalization and upscaling.
    """

    transition_zones_x: int = 64  # output reachability grid width
    transition_zones_y: int = 100  # output reachability grid height
    epv_zones_x: int = 32  # output EPV grid width
    epv_zones_y: int = 50  # output EPV grid height
    intermediate_x: int = 16  # pass completion matrix zones (x)
    intermediate_y: int = 25  # pass completion matrix zones (y)
    pitch_length: float = 105.0  # SPADL coordinates (meters)
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
_RELEVANT_TYPES = _MOVE_TYPES | _SHOT_TYPES


# ---------------------------------------------------------------------------
# Coordinate normalization (inlined from compute_xt_grid_hf.py)
# ---------------------------------------------------------------------------


def _normalize_attack_direction(df: pd.DataFrame, params: OBSOGridParams) -> pd.DataFrame:
    """Normalize coordinates so all actions attack toward x=pitch_length.

    SPADL coordinates should have x increasing in the attacking direction, but
    some data exports (particularly Wyscout) have inconsistent orientation where
    approximately half the actions have flipped coordinates.

    Fix: for each (match_id, team_id, period), check if shots cluster in the
    defensive half (x < pitch_length/2). If so, flip all coordinates for that group.
    For groups without shots, infer from the same team's other period (teams swap
    sides each half).
    """
    pitch_l = params.pitch_length
    pitch_w = params.pitch_width
    midfield = pitch_l / 2.0

    df = df.copy()

    # Identify shots
    is_shot = df["type_name"].isin(_SHOT_TYPES)
    shots = df[is_shot]

    if shots.empty:
        return df

    # Compute mean shot x per (match_id, team_id, period)
    shot_means = shots.groupby(["match_id", "team_id", "period"])["start_x"].mean()

    # Build flip lookup: True if shots are in defensive half
    flip_lookup: dict[tuple, bool] = {}
    for key, mean_x in shot_means.items():
        flip_lookup[key] = mean_x < midfield  # type: ignore[index]

    # For groups without shots, infer from the same team's other period
    group_keys = set(df.groupby(["match_id", "team_id", "period"]).groups.keys())

    for key in group_keys:
        if key not in flip_lookup:
            match_id, team_id, period = key  # type: ignore[misc]
            # Try the other period (1<->2)
            other_period = 2 if period == 1 else 1
            other_key = (match_id, team_id, other_period)
            if other_key in flip_lookup:
                # Opposite of the other period (teams swap sides)
                flip_lookup[key] = not flip_lookup[other_key]
            # If neither period has shots, don't flip (assume correct)

    # Count flips for logging
    n_flip = sum(1 for v in flip_lookup.values() if v)
    n_total = len(flip_lookup)
    logger.info("Coordinate normalization: %d/%d team-period groups flipped", n_flip, n_total)

    if n_flip == 0:
        return df

    # Apply flips vectorized: build a flip mask
    flip_mask = np.zeros(len(df), dtype=bool)
    for key, should_flip in flip_lookup.items():
        if should_flip:
            match_id, team_id, period = key  # type: ignore[misc]
            group_mask = (df["match_id"] == match_id) & (df["team_id"] == team_id) & (df["period"] == period)
            flip_mask |= group_mask.values

    # Flip coordinates where needed
    for col in ["start_x", "end_x"]:
        if col in df.columns:
            df.loc[flip_mask, col] = pitch_l - df.loc[flip_mask, col]
    for col in ["start_y", "end_y"]:
        if col in df.columns:
            df.loc[flip_mask, col] = pitch_w - df.loc[flip_mask, col]

    # Verify: shots should now cluster in attacking half
    post_shots = df[is_shot]
    post_mean = post_shots["start_x"].mean()
    post_pct_attacking = (post_shots["start_x"] > midfield).mean() * 100
    logger.info("Post-normalization: shot mean x=%.1f, %.0f%% in attacking half", post_mean, post_pct_attacking)

    return df


# ---------------------------------------------------------------------------
# Zone assignment (inlined from src/analytics/expected_threat.py)
# ---------------------------------------------------------------------------


def _assign_zones(
    x: np.ndarray,
    y: np.ndarray,
    n_zones_x: int,
    n_zones_y: int,
    pitch_length: float,
    pitch_width: float,
) -> np.ndarray:
    """Map (x, y) coordinates to flat zone indices.

    Flat index = zone_x * n_zones_y + zone_y (row-major in x).
    """
    zone_x = np.clip((x / pitch_length * n_zones_x).astype(int), 0, n_zones_x - 1)
    zone_y = np.clip((y / pitch_width * n_zones_y).astype(int), 0, n_zones_y - 1)
    return zone_x * n_zones_y + zone_y


# ---------------------------------------------------------------------------
# Transition matrix (inlined from src/analytics/expected_threat.py)
# ---------------------------------------------------------------------------


def _build_transition_matrix(
    start_zones: np.ndarray,
    end_zones: np.ndarray,
    n_zones: int,
) -> np.ndarray:
    """Build row-normalized transition matrix from zone-to-zone moves.

    Args:
        start_zones: 1D array of origin zone flat indices.
        end_zones: 1D array of destination zone flat indices.
        n_zones: Total number of zones.

    Returns:
        (n_zones, n_zones) row-stochastic matrix.
    """
    transition = np.zeros((n_zones, n_zones), dtype=np.float64)
    np.add.at(transition, (start_zones, end_zones), 1.0)
    row_sums = np.maximum(transition.sum(axis=1, keepdims=True), 1.0)
    return transition / row_sums


# ---------------------------------------------------------------------------
# Value iteration (inlined from src/analytics/expected_threat.py)
# ---------------------------------------------------------------------------


def _value_iteration(
    shot_prob: np.ndarray,
    goal_prob: np.ndarray,
    move_prob: np.ndarray,
    transition: np.ndarray,
    max_iters: int,
    tol: float,
) -> tuple[np.ndarray, int]:
    """NumPy Bellman value iteration for EPV.

    EPV(z) = P(shot|z) * P(goal|shot,z) + P(move|z) * sum_j(T(z,j) * EPV(j))

    Returns (EPV vector, iterations used).
    """
    epv = np.zeros_like(shot_prob)
    for i in range(max_iters):
        epv_new = shot_prob * goal_prob + move_prob * (transition @ epv)
        delta = float(np.max(np.abs(epv_new - epv)))
        epv = epv_new
        if delta < tol:
            return epv, i + 1
    return epv, max_iters


# ---------------------------------------------------------------------------
# Ball reachability grid computation
# ---------------------------------------------------------------------------


def compute_ball_reachability_grid(
    actions_df: pd.DataFrame,
    params: OBSOGridParams,
) -> np.ndarray:
    """Compute a global ball reachability surface from pass completion data.

    Strategy:
    1. Build a zone-to-zone pass completion matrix at intermediate resolution
       (intermediate_y x intermediate_x = 25x16 = 400 zones).
    2. Weight each origin zone by its pass frequency (how often passes originate
       from that zone) to get a global reachability surface.
    3. Marginalize: for each target zone, sum over all origin zones weighted by
       frequency. This gives P(ball reaches target) averaged across typical
       ball positions.
    4. Upscale from (intermediate_y, intermediate_x) to
       (transition_zones_y, transition_zones_x) via bilinear interpolation.

    Args:
        actions_df: SPADL actions with type_name, result_name, start_x/y, end_x/y.
        params: Grid dimensions and pitch parameters.

    Returns:
        2D array of shape (transition_zones_y, transition_zones_x) with
        reachability values in [0, 1].
    """
    n_int_x = params.intermediate_x
    n_int_y = params.intermediate_y
    n_int_zones = n_int_x * n_int_y

    # Classify actions
    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"

    # All moves (attempted) and successful moves
    move_mask = is_move
    succ_move_mask = is_move & is_success

    start_x = np.asarray(actions_df["start_x"], dtype=np.float64)
    start_y = np.asarray(actions_df["start_y"], dtype=np.float64)
    end_x = np.asarray(actions_df["end_x"], dtype=np.float64)
    end_y = np.asarray(actions_df["end_y"], dtype=np.float64)

    # Assign zones at intermediate resolution
    start_zones_all = _assign_zones(start_x, start_y, n_int_x, n_int_y, params.pitch_length, params.pitch_width)
    end_zones_all = _assign_zones(end_x, end_y, n_int_x, n_int_y, params.pitch_length, params.pitch_width)

    # Build pass completion matrix: only successful moves
    # completion[i, j] = P(pass from zone i reaches zone j | pass attempted from i)
    # Count attempted passes from each origin to each target
    attempted = np.zeros((n_int_zones, n_int_zones), dtype=np.float64)
    np.add.at(attempted, (start_zones_all[move_mask], end_zones_all[move_mask]), 1.0)

    # Count successful passes from each origin to each target
    completed = np.zeros((n_int_zones, n_int_zones), dtype=np.float64)
    np.add.at(completed, (start_zones_all[succ_move_mask], end_zones_all[succ_move_mask]), 1.0)

    # Completion rate per (origin, target) pair
    safe_attempted = np.maximum(attempted, 1.0)
    completion_matrix = completed / safe_attempted

    # Pass frequency: how often passes originate from each zone (among all moves)
    origin_counts = np.bincount(start_zones_all[move_mask], minlength=n_int_zones).astype(np.float64)
    total_moves = np.maximum(origin_counts.sum(), 1.0)
    origin_frequency = origin_counts / total_moves  # sums to 1

    # Marginalize over origin zones: reachability(target) = sum_i(freq(i) * completion(i, target))
    # This gives a weighted average of "how likely is it that a pass from a typical
    # ball position reaches this target zone?"
    reachability_flat = origin_frequency @ completion_matrix  # (n_int_zones,)

    # Reshape to 2D grid at intermediate resolution: (intermediate_x, intermediate_y)
    # then transpose to (intermediate_y, intermediate_x) for (ny, nx) convention
    reachability_2d = reachability_flat.reshape(n_int_x, n_int_y).T

    # Normalize to [0, 1] range
    max_val = reachability_2d.max()
    if max_val > 1e-10:
        reachability_2d = reachability_2d / max_val

    # Upscale to output resolution via bilinear interpolation
    output = interpolate_grid(reachability_2d, (params.transition_zones_y, params.transition_zones_x))

    return np.clip(output, 0.0, 1.0)


# ---------------------------------------------------------------------------
# EPV grid computation
# ---------------------------------------------------------------------------


def compute_epv_grid(
    actions_df: pd.DataFrame,
    params: OBSOGridParams,
) -> np.ndarray:
    """Compute EPV grid via Markov chain value iteration.

    Same algorithm as xT (Karun Singh 2018) but at the OBSO output resolution
    (epv_zones_y x epv_zones_x = 50 x 32).

    EPV(z) = P(shot|z) * P(goal|shot,z) + P(move|z) * sum_j(T(z,j) * EPV(j))

    Args:
        actions_df: SPADL actions with type_name, result_name, start_x/y, end_x/y.
        params: Grid dimensions and convergence parameters.

    Returns:
        2D array of shape (epv_zones_y, epv_zones_x) with EPV values.
    """
    n_x = params.epv_zones_x
    n_y = params.epv_zones_y
    n_zones = n_x * n_y

    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_shot = np.array([t in _SHOT_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"

    start_zones = _assign_zones(
        np.asarray(actions_df["start_x"], dtype=np.float64),
        np.asarray(actions_df["start_y"], dtype=np.float64),
        n_x,
        n_y,
        params.pitch_length,
        params.pitch_width,
    )
    end_zones = _assign_zones(
        np.asarray(actions_df["end_x"], dtype=np.float64),
        np.asarray(actions_df["end_y"], dtype=np.float64),
        n_x,
        n_y,
        params.pitch_length,
        params.pitch_width,
    )

    # Per-zone counts
    total_per_zone = np.bincount(start_zones, minlength=n_zones).astype(np.float64)
    shots_per_zone = np.bincount(start_zones[is_shot], minlength=n_zones).astype(np.float64)
    goals_per_zone = np.bincount(start_zones[is_shot & is_success], minlength=n_zones).astype(np.float64)

    # Successful moves — failed moves lose possession (contribute EPV=0)
    successful_moves = is_move & is_success
    succ_moves_per_zone = np.bincount(start_zones[successful_moves], minlength=n_zones).astype(np.float64)

    # Probabilities per zone
    safe_total = np.maximum(total_per_zone, 1.0)
    shot_prob = shots_per_zone / safe_total
    goal_prob = np.where(shots_per_zone > 0, goals_per_zone / shots_per_zone, 0.0)
    move_prob = succ_moves_per_zone / safe_total

    # Transition matrix (successful moves only)
    transition = _build_transition_matrix(
        start_zones[successful_moves],
        end_zones[successful_moves],
        n_zones,
    )

    # Value iteration
    epv_flat, iters = _value_iteration(
        shot_prob,
        goal_prob,
        move_prob,
        transition,
        params.max_iterations,
        params.tolerance,
    )
    logger.info("EPV value iteration converged in %d iterations", iters)

    # Reshape: flat index = zone_x * n_y + zone_y -> (n_x, n_y) -> transpose to (n_y, n_x)
    return epv_flat.reshape(n_x, n_y).T


# ---------------------------------------------------------------------------
# Grid validation
# ---------------------------------------------------------------------------


def validate_reachability_grid(grid: np.ndarray, params: OBSOGridParams) -> None:
    """Validate computed reachability grid meets data quality requirements.

    Args:
        grid: 2D array of shape (transition_zones_y, transition_zones_x).
        params: Expected grid dimensions.

    Raises:
        ValueError: If validation fails.
    """
    expected_shape = (params.transition_zones_y, params.transition_zones_x)
    if grid.shape != expected_shape:
        msg = f"Reachability grid shape {grid.shape} != expected {expected_shape}"
        raise ValueError(msg)

    if np.any(np.isnan(grid)):
        msg = "Reachability grid contains NaN values"
        raise ValueError(msg)

    if grid.min() < 0.0 or grid.max() > 1.0:
        msg = f"Reachability grid values out of [0, 1]: min={grid.min():.4f}, max={grid.max():.4f}"
        raise ValueError(msg)

    # Should have meaningful spatial variation (not a flat surface)
    value_range = grid.max() - grid.min()
    if value_range < 0.05:
        msg = f"Reachability grid range too narrow ({value_range:.4f}) — likely data quality issue"
        raise ValueError(msg)

    # Center of pitch should generally have higher reachability than corners
    center_y, center_x = grid.shape[0] // 2, grid.shape[1] // 2
    center_region = grid[center_y - 5 : center_y + 5, center_x - 5 : center_x + 5]
    corner_mean = (grid[:5, :5].mean() + grid[-5:, -5:].mean() + grid[:5, -5:].mean() + grid[-5:, :5].mean()) / 4.0
    if center_region.mean() < corner_mean:
        logger.warning(
            "Reachability center (%.4f) lower than corners (%.4f) — unexpected but not fatal",
            center_region.mean(),
            corner_mean,
        )


def validate_epv_grid(grid: np.ndarray, params: OBSOGridParams) -> None:
    """Validate computed EPV grid meets data quality requirements.

    Args:
        grid: 2D array of shape (epv_zones_y, epv_zones_x).
        params: Expected grid dimensions.

    Raises:
        ValueError: If validation fails.
    """
    expected_shape = (params.epv_zones_y, params.epv_zones_x)
    if grid.shape != expected_shape:
        msg = f"EPV grid shape {grid.shape} != expected {expected_shape}"
        raise ValueError(msg)

    if np.any(np.isnan(grid)):
        msg = "EPV grid contains NaN values"
        raise ValueError(msg)

    value_range = grid.max() - grid.min()
    if value_range < 0.01:
        msg = f"EPV grid range too narrow ({value_range:.4f}) — likely coordinate orientation issue"
        raise ValueError(msg)

    # EPV at higher resolution (32x50) can exceed 0.50 near the goal mouth
    # — this is expected, unlike xT at 12x8 where 0.50 is a ceiling
    if grid.max() > 1.0:
        msg = f"EPV grid max exceeds 1.0: {grid.max():.4f}"
        raise ValueError(msg)

    # EPV should generally increase toward the attacking goal (higher x = right columns)
    # Grid is (ny, nx), so columns correspond to x zones
    col_means = grid.mean(axis=0)  # mean across y for each x column
    if col_means[-1] < col_means[0]:
        msg = (
            f"EPV not increasing toward goal: x=0 mean={col_means[0]:.5f} > "
            f"x={params.epv_zones_x - 1} mean={col_means[-1]:.5f}"
        )
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Grid-to-DataFrame converters
# ---------------------------------------------------------------------------


def reachability_grid_to_dataframe(
    grid: np.ndarray,
    competition_id: str,
) -> pd.DataFrame:
    """Convert reachability grid to long-format DataFrame.

    Args:
        grid: 2D array of shape (ny, nx).
        competition_id: Competition identifier or "global".

    Returns:
        DataFrame with columns: zone_y, zone_x, reachability, competition_id.
    """
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


def epv_grid_to_dataframe(
    grid: np.ndarray,
    competition_id: str,
) -> pd.DataFrame:
    """Convert EPV grid to long-format DataFrame.

    Args:
        grid: 2D array of shape (ny, nx).
        competition_id: Competition identifier or "global".

    Returns:
        DataFrame with columns: zone_y, zone_x, epv_value, competition_id.
    """
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
    actions_df: pd.DataFrame,
    params: OBSOGridParams,
    competition_id: str,
) -> pd.DataFrame:
    """Build and export the sparse pass completion matrix in long format.

    Stores only non-zero entries for future origin-specific OBSO lookups.

    Args:
        actions_df: SPADL actions (already filtered + normalized).
        params: Grid parameters.
        competition_id: Competition identifier or "global".

    Returns:
        DataFrame with columns: origin_zone, target_zone, probability, competition_id.
    """
    n_int_x = params.intermediate_x
    n_int_y = params.intermediate_y
    n_int_zones = n_int_x * n_int_y

    type_names = actions_df["type_name"].values
    result_names = actions_df["result_name"].values
    is_move = np.array([t in _MOVE_TYPES for t in type_names], dtype=bool)
    is_success = result_names == "success"
    succ_move_mask = is_move & is_success

    start_zones = _assign_zones(
        np.asarray(actions_df["start_x"], dtype=np.float64),
        np.asarray(actions_df["start_y"], dtype=np.float64),
        n_int_x,
        n_int_y,
        params.pitch_length,
        params.pitch_width,
    )
    end_zones = _assign_zones(
        np.asarray(actions_df["end_x"], dtype=np.float64),
        np.asarray(actions_df["end_y"], dtype=np.float64),
        n_int_x,
        n_int_y,
        params.pitch_length,
        params.pitch_width,
    )

    # Build row-normalized transition matrix from successful moves
    transition = _build_transition_matrix(
        start_zones[succ_move_mask],
        end_zones[succ_move_mask],
        n_int_zones,
    )

    # Extract non-zero entries (sparse representation)
    nonzero = np.nonzero(transition)
    origin_zones = nonzero[0]
    target_zones = nonzero[1]
    probabilities = transition[origin_zones, target_zones]

    return pd.DataFrame(
        {
            "origin_zone": origin_zones,
            "target_zone": target_zones,
            "probability": np.round(probabilities, 6),
            "competition_id": competition_id,
        }
    )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-epv-reachability", phase="grid_computation")
def main() -> None:
    """Download SPADL actions, compute EPV + reachability grids, publish to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    params = OBSOGridParams()

    recorder = HFJobsCostRecorder(
        workflow_id="wf-epv-reachability",
        phase="grid_computation",
        rate_usd_per_hour=HF_RATE_CPU_BASIC,
        repo_id=OUTPUT_DATASET,
    )
    recorder.start()

    logger.info("=== OBSO Grid Training Script ===")
    logger.info(
        "Output grids: reachability (%d, %d), EPV (%d, %d)",
        params.transition_zones_y,
        params.transition_zones_x,
        params.epv_zones_y,
        params.epv_zones_x,
    )

    # ------------------------------------------------------------------
    # 1. Load SPADL data from HF Hub
    # ------------------------------------------------------------------
    logger.info("=== Loading SPADL actions from HF Hub ===")

    # Find parquet files — use only data.parquet (HF viewer canonical files),
    # skip part-* files which are duplicates of the same data
    all_items = list(api.list_repo_tree(SPADL_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith("/data.parquet")]
    # Fall back: if no data.parquet found, use all parquet files
    if not parquet_files:
        parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    logger.info("Downloading %d parquet files...", len(parquet_files))
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SPADL_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        # Extract data_source from Hive partition path
        if "data_source=" in pf:
            ds = pf.split("data_source=")[1].split("/")[0]
            df["data_source"] = ds
        dfs.append(df)
        logger.info("  %s: %s rows", pf, f"{len(df):,}")

    all_actions = pd.concat(dfs, ignore_index=True)

    # Capture dataset commit hash for reproducibility (E5)
    _dataset_info = api.repo_info(repo_id=SPADL_DATASET, repo_type="dataset")
    _dataset_commit = _dataset_info.sha

    # Deduplicate in case of overlapping exports
    if "action_value_id" in all_actions.columns:
        before = len(all_actions)
        all_actions = all_actions.drop_duplicates(subset=["action_value_id"])
        if len(all_actions) < before:
            logger.info("Deduplicated: %s -> %s rows", f"{before:,}", f"{len(all_actions):,}")

    logger.info("Total actions: %s", f"{len(all_actions):,}")

    # Filter to relevant types and rename columns
    all_actions = all_actions[all_actions["action_type"].isin(_RELEVANT_TYPES)].copy()
    all_actions = all_actions.rename(columns={"action_type": "type_name", "action_result": "result_name"})
    logger.info("Relevant actions: %s", f"{len(all_actions):,}")

    if len(all_actions) < 1000:
        raise ValueError(f"Too few actions ({len(all_actions)}) for meaningful grid computation")

    # ------------------------------------------------------------------
    # 1b. Normalize coordinate orientation
    # ------------------------------------------------------------------
    logger.info("=== Normalizing coordinate orientation ===")
    all_actions = _normalize_attack_direction(all_actions, params)

    # ------------------------------------------------------------------
    # 2. Compute per-competition grids
    # ------------------------------------------------------------------
    logger.info("=== Computing per-competition grids ===")
    competitions = sorted(all_actions["competition_id"].dropna().unique())
    logger.info("%d competitions found", len(competitions))

    all_reachability_dfs: list[pd.DataFrame] = []
    all_epv_dfs: list[pd.DataFrame] = []
    all_completion_dfs: list[pd.DataFrame] = []

    for comp_id in competitions:
        comp_actions = all_actions[all_actions["competition_id"] == comp_id]
        n_events = len(comp_actions)
        if n_events < 500:
            logger.info("Competition %s: %d events -- skipping (too few for stable grids)", comp_id, n_events)
            continue

        # Reachability grid
        reach_grid = compute_ball_reachability_grid(comp_actions, params)
        reach_df = reachability_grid_to_dataframe(reach_grid, str(comp_id))
        all_reachability_dfs.append(reach_df)

        # EPV grid
        epv_grid = compute_epv_grid(comp_actions, params)
        epv_df = epv_grid_to_dataframe(epv_grid, str(comp_id))
        all_epv_dfs.append(epv_df)

        # Completion matrix
        comp_df = completion_matrix_to_dataframe(comp_actions, params, str(comp_id))
        all_completion_dfs.append(comp_df)

        logger.info(
            "Competition %s: %s events, reach max=%.4f, EPV max=%.5f",
            comp_id,
            f"{n_events:,}",
            reach_grid.max(),
            epv_grid.max(),
        )

    # ------------------------------------------------------------------
    # 3. Global grids (all competitions combined)
    # ------------------------------------------------------------------
    logger.info("=== Computing global grids ===")

    global_reach = compute_ball_reachability_grid(all_actions, params)
    validate_reachability_grid(global_reach, params)
    global_reach_df = reachability_grid_to_dataframe(global_reach, "global")
    all_reachability_dfs.append(global_reach_df)

    global_epv = compute_epv_grid(all_actions, params)
    validate_epv_grid(global_epv, params)
    global_epv_df = epv_grid_to_dataframe(global_epv, "global")
    all_epv_dfs.append(global_epv_df)

    global_completion_df = completion_matrix_to_dataframe(all_actions, params, "global")
    all_completion_dfs.append(global_completion_df)

    logger.info(
        "Global reachability: shape=%s, range=[%.4f, %.4f]",
        global_reach.shape,
        global_reach.min(),
        global_reach.max(),
    )
    logger.info(
        "Global EPV: shape=%s, range=[%.5f, %.5f]",
        global_epv.shape,
        global_epv.min(),
        global_epv.max(),
    )

    # EPV spatial summary
    epv_col_means = global_epv.mean(axis=0)
    logger.info("EPV col x=0 (defense): %.5f", epv_col_means[0])
    logger.info("EPV col x=%d (attack): %.5f", params.epv_zones_x - 1, epv_col_means[-1])

    # ------------------------------------------------------------------
    # 3b. Log to MLflow
    # ------------------------------------------------------------------
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        logger.info("=== Logging grids to MLflow ===")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/obso_grids")

        with mlflow.start_run(run_name="obso_grid_training"):
            mlflow.log_params(
                {
                    "transition_zones_x": params.transition_zones_x,
                    "transition_zones_y": params.transition_zones_y,
                    "epv_zones_x": params.epv_zones_x,
                    "epv_zones_y": params.epv_zones_y,
                    "intermediate_x": params.intermediate_x,
                    "intermediate_y": params.intermediate_y,
                    "pitch_length": params.pitch_length,
                    "pitch_width": params.pitch_width,
                    "max_iterations": params.max_iterations,
                    "tolerance": params.tolerance,
                    "n_competitions": len(all_epv_dfs) - 1,
                    "total_actions": len(all_actions),
                    "training_env": "hf_jobs_cpu",
                }
            )
            mlflow.log_param("spadl_vaep_action_values_commit", _dataset_commit)
            mlflow.log_metrics(
                {
                    "global_epv_max": float(global_epv.max()),
                    "global_epv_min": float(global_epv.min()),
                    "global_epv_range": float(global_epv.max() - global_epv.min()),
                    "global_reach_max": float(global_reach.max()),
                    "global_reach_min": float(global_reach.min()),
                    "global_reach_range": float(global_reach.max() - global_reach.min()),
                    "completion_matrix_nonzero": len(global_completion_df),
                }
            )

            # Save grids as temporary JSON artifacts
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                grid_json = {
                    "epv": {
                        "shape": list(global_epv.shape),
                        "values": global_epv.tolist(),
                    },
                    "reachability": {
                        "shape": list(global_reach.shape),
                        "values": global_reach.tolist(),
                    },
                }
                json.dump(grid_json, f, indent=2)
                artifact_path = f.name
            mlflow.log_artifact(artifact_path, "obso_grids")
            os.unlink(artifact_path)

        logger.info("Grids logged to MLflow")
    else:
        logger.info("=== MLflow skipped (MLFLOW_TRACKING_URI not set) ===")

    # ------------------------------------------------------------------
    # 4. Publish to HF Hub
    # ------------------------------------------------------------------
    logger.info("=== Publishing to HF Hub ===")

    combined_reach = pd.concat(all_reachability_dfs, ignore_index=True)
    combined_epv = pd.concat(all_epv_dfs, ignore_index=True)
    combined_completion = pd.concat(all_completion_dfs, ignore_index=True)

    n_competitions_computed = len(all_epv_dfs) - 1  # exclude global

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        # Global grids as separate files for easy download
        global_reach_only = combined_reach[combined_reach["competition_id"] == "global"]
        global_reach_only[["zone_y", "zone_x", "reachability"]].to_parquet(
            str(data_dir / "reachability_grid_global.parquet"), index=False
        )

        global_epv_only = combined_epv[combined_epv["competition_id"] == "global"]
        global_epv_only[["zone_y", "zone_x", "epv_value"]].to_parquet(
            str(data_dir / "epv_grid_global.parquet"), index=False
        )

        global_comp_only = combined_completion[combined_completion["competition_id"] == "global"]
        global_comp_only[["origin_zone", "target_zone", "probability"]].to_parquet(
            str(data_dir / "completion_matrix_global.parquet"), index=False
        )

        # All grids (per-competition + global) in combined files
        combined_reach.to_parquet(str(data_dir / "reachability_grids_all.parquet"), index=False)
        combined_epv.to_parquet(str(data_dir / "epv_grids_all.parquet"), index=False)
        combined_completion.to_parquet(str(data_dir / "completion_matrices_all.parquet"), index=False)

        # Metadata
        metadata: dict[str, object] = {
            "params": asdict(params),
            "competitions": [str(c) for c in competitions],
            "n_competitions_computed": n_competitions_computed,
            "total_actions": len(all_actions),
            "grids": {
                "reachability": {
                    "shape": list(global_reach.shape),
                    "min": round(float(global_reach.min()), 6),
                    "max": round(float(global_reach.max()), 6),
                },
                "epv": {
                    "shape": list(global_epv.shape),
                    "min": round(float(global_epv.min()), 6),
                    "max": round(float(global_epv.max()), 6),
                },
                "completion_matrix": {
                    "intermediate_zones": params.intermediate_x * params.intermediate_y,
                    "nonzero_entries": len(global_comp_only),
                },
            },
            "spadl_dataset_commit": _dataset_commit,
        }
        metadata = recorder.complete(metadata, row_count=len(combined_reach) + len(combined_epv))
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

        # Pre-compute stats strings for the dataset card (line length)
        reach_shape_str = f"{params.transition_zones_y} x {params.transition_zones_x}"
        reach_range_str = f"[{global_reach.min():.4f}, {global_reach.max():.4f}]"
        epv_shape_str = f"{params.epv_zones_y} x {params.epv_zones_x}"
        epv_range_str = f"[{global_epv.min():.5f}, {global_epv.max():.5f}]"

        # Dataset card
        card = f"""---
license: mit
tags:
  - soccer
  - football
  - obso
  - epv
  - transition
  - reachability
  - analytics
size_categories:
  - 10K<n<100K
---

# OBSO Trained Grids (EPV + Ball Reachability)

Data-driven EPV and ball reachability grids for the Off-Ball Scoring Opportunity
(OBSO) pipeline, computed from SPADL action data via Markov chain value iteration
and pass completion marginalization.

These grids replace the synthetic Gaussian proxies with real data-driven surfaces.

## Method

### EPV Grid ({params.epv_zones_y} x {params.epv_zones_x})

Markov chain value iteration (Karun Singh 2018 / Spearman 2018):

```
EPV(z) = P(shot|z) * P(goal|shot,z) + P(move|z) * sum_j(T(z,j) * EPV(j))
```

Convergence: tolerance={params.tolerance}, max iterations={params.max_iterations}.

### Ball Reachability Grid ({params.transition_zones_y} x {params.transition_zones_x})

1. Build zone-to-zone pass completion matrix at intermediate resolution
   ({params.intermediate_y} x {params.intermediate_x} = {params.intermediate_y * params.intermediate_x} zones)
2. Marginalize over origin zones weighted by pass frequency to get a global
   reachability surface
3. Upscale to ({params.transition_zones_y}, {params.transition_zones_x}) via bilinear interpolation

### Completion Matrix ({params.intermediate_y * params.intermediate_x} zones)

Row-normalized zone-to-zone transition probabilities from successful passes.
Stored in sparse long format for future origin-specific OBSO lookups.

## Contents

| File | Description |
|------|-------------|
| `data/reachability_grid_global.parquet` | Global reachability grid (long format) |
| `data/epv_grid_global.parquet` | Global EPV grid (long format) |
| `data/completion_matrix_global.parquet` | Global completion matrix (sparse long format) |
| `data/reachability_grids_all.parquet` | All grids (per-competition + global) |
| `data/epv_grids_all.parquet` | All EPV grids (per-competition + global) |
| `data/completion_matrices_all.parquet` | All completion matrices |
| `metadata.json` | Parameters, statistics, and data provenance |

## Columns

### reachability_grid_global.parquet

| Column | Type | Description |
|--------|------|-------------|
| `zone_y` | int | Y zone index (0-{params.transition_zones_y - 1}, pitch width) |
| `zone_x` | int | X zone index (0-{params.transition_zones_x - 1}, attacking direction) |
| `reachability` | float | Ball reachability probability (0-1, higher = easier to reach) |

### epv_grid_global.parquet

| Column | Type | Description |
|--------|------|-------------|
| `zone_y` | int | Y zone index (0-{params.epv_zones_y - 1}, pitch width) |
| `zone_x` | int | X zone index (0-{params.epv_zones_x - 1}, attacking direction) |
| `epv_value` | float | Expected possession value (0-1, higher = more dangerous) |

### completion_matrix_global.parquet

| Column | Type | Description |
|--------|------|-------------|
| `origin_zone` | int | Flat zone index of pass origin |
| `target_zone` | int | Flat zone index of pass target |
| `probability` | float | Row-normalized completion probability |

## Usage

```python
import numpy as np
import pandas as pd
from huggingface_hub import hf_hub_download

# Download global grids
reach_path = hf_hub_download(
    "luxury-lakehouse/obso-trained-grids",
    "data/reachability_grid_global.parquet",
    repo_type="dataset",
)
epv_path = hf_hub_download(
    "luxury-lakehouse/obso-trained-grids",
    "data/epv_grid_global.parquet",
    repo_type="dataset",
)

# Load and reshape to 2D arrays
reach_df = pd.read_parquet(reach_path)
reach_grid = reach_df.pivot(index="zone_y", columns="zone_x", values="reachability").to_numpy()
# shape: ({params.transition_zones_y}, {params.transition_zones_x})

epv_df = pd.read_parquet(epv_path)
epv_grid = epv_df.pivot(index="zone_y", columns="zone_x", values="epv_value").to_numpy()
# shape: ({params.epv_zones_y}, {params.epv_zones_x})
```

## Grid Statistics

- **Reachability**: {reach_shape_str}, range {reach_range_str}
- **EPV**: {epv_shape_str}, range {epv_range_str}
- **Competitions**: {n_competitions_computed}
- **Total SPADL actions**: {len(all_actions):,}

## References

- Karun Singh (2018). "Introducing Expected Threat (xT)."
- Spearman (2018). "Beyond Expected Goals." MIT Sloan.
- Fernandez & Bornn (2018). "Wide Open Spaces." MIT Sloan.
- Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.

## License

MIT -- derived from StatsBomb and Wyscout open data via SPADL.
"""
        with open(str(Path(tmpdir) / "README.md"), "w") as f:
            f.write(card)

        api.create_repo(OUTPUT_DATASET, repo_type="dataset", exist_ok=True, token=hf_token)
        api.upload_folder(
            folder_path=tmpdir,
            repo_id=OUTPUT_DATASET,
            repo_type="dataset",
            token=hf_token,
        )

    logger.info("Published: https://huggingface.co/datasets/%s", OUTPUT_DATASET)
    logger.info("Competitions: %d", n_competitions_computed)
    logger.info("Global EPV max: %.5f", global_epv.max())
    logger.info("Global reachability range: [%.4f, %.4f]", global_reach.min(), global_reach.max())
    logger.info("OBSO grid training complete!")


if __name__ == "__main__":
    main()
