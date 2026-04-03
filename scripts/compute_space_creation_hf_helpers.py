"""Helper module for compute_space_creation_hf.py.

Contains coordinate conversion, synthetic grid fallback, trained grid loading,
target grid construction, and per-frame space creation computation. The main
script handles data loading, batch GPU computation, MLflow, and publishing.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from analytics.obso import interpolate_grid
from analytics.pitch_control import PitchControlParams

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Grid + pitch configuration (shared with main script)
# ---------------------------------------------------------------------------

GRID_NX = 52
GRID_NY = 34
FRAME_RATE = 25
FRAME_SAMPLE_STEP = 25
SB_LENGTH = 120.0
SB_WIDTH = 80.0
PITCH_LENGTH_M = 105.0
PITCH_WIDTH_M = 68.0
CELL_WIDTH_M = PITCH_LENGTH_M / GRID_NX
CELL_HEIGHT_M = PITCH_WIDTH_M / GRID_NY
CELL_AREA_M2 = CELL_WIDTH_M * CELL_HEIGHT_M

GRIDS_DATASET = "luxury-lakehouse/obso-trained-grids"


# ---------------------------------------------------------------------------
# Coordinate conversion
# ---------------------------------------------------------------------------


def sb_to_meters_x(x: np.ndarray, params: PitchControlParams) -> np.ndarray:
    """Convert StatsBomb x to meters."""
    return x * (params.pitch_length_m / params.sb_length)


def sb_to_meters_y(y: np.ndarray, params: PitchControlParams) -> np.ndarray:
    """Convert StatsBomb y to meters."""
    return y * (params.pitch_width_m / params.sb_width)


# ---------------------------------------------------------------------------
# Synthetic grid fallback
# ---------------------------------------------------------------------------


def make_synthetic_grids() -> tuple[np.ndarray, np.ndarray]:
    """Create synthetic EPV and Transition grids for development."""
    epv = np.zeros((32, 50), dtype=np.float64)
    for i in range(50):
        epv[:, i] = 0.01 + 0.15 * (i / 49.0) ** 2
    for j in range(32):
        center_dist = abs(j - 16) / 16.0
        epv[j, :] *= 1.0 - 0.3 * center_dist

    transition = np.zeros((64, 100), dtype=np.float64)
    for i in range(64):
        for j in range(100):
            transition[i, j] = np.exp(-((i - 32) ** 2 + (j - 50) ** 2) / (2 * 30**2))

    return epv, transition


# ---------------------------------------------------------------------------
# Data loading from HF Hub
# ---------------------------------------------------------------------------


def load_trained_grids(hf_token: str) -> tuple[np.ndarray, np.ndarray]:
    """Load trained EPV and reachability grids from HF Hub.

    Falls back to synthetic grids if the dataset is not yet published.
    """
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError, HfHubHTTPError, RepositoryNotFoundError

    try:
        reachability_path = hf_hub_download(
            repo_id=GRIDS_DATASET,
            filename="data/reachability_grid_global.parquet",
            repo_type="dataset",
            token=hf_token,
        )
        epv_path = hf_hub_download(
            repo_id=GRIDS_DATASET,
            filename="data/epv_grid_global.parquet",
            repo_type="dataset",
            token=hf_token,
        )
        _reach_df = pd.read_parquet(reachability_path)
        reachability_grid = _reach_df.pivot(index="zone_y", columns="zone_x", values="reachability").values.astype(
            np.float64
        )
        _epv_df = pd.read_parquet(epv_path)
        epv_grid = _epv_df.pivot(index="zone_y", columns="zone_x", values="epv_value").values.astype(np.float64)
        print(
            f"  Loaded trained grids from {GRIDS_DATASET} "
            f"(reachability: {reachability_grid.shape}, EPV: {epv_grid.shape})"
        )
        return epv_grid, reachability_grid
    except (HfHubHTTPError, EntryNotFoundError, RepositoryNotFoundError):
        print("  WARNING: Trained grids unavailable -- falling back to synthetic grids")
        epv_grid, transition_grid = make_synthetic_grids()
        print(f"  Using synthetic static grids (EPV: {epv_grid.shape}, Transition: {transition_grid.shape})")
        return epv_grid, transition_grid


# ---------------------------------------------------------------------------
# Target grid construction
# ---------------------------------------------------------------------------


def build_target_grid(params: PitchControlParams) -> np.ndarray:
    """Build (GRID_NY * GRID_NX, 2) array of target points in StatsBomb 120x80."""
    grid_x = np.linspace(0, params.sb_length, GRID_NX)
    grid_y = np.linspace(0, params.sb_width, GRID_NY)
    xx, yy = np.meshgrid(grid_x, grid_y)
    return np.column_stack([xx.ravel(), yy.ravel()])


def get_grid_axes(params: PitchControlParams) -> tuple[np.ndarray, np.ndarray]:
    """Return (grid_x, grid_y) arrays for OBSO surface computation."""
    return np.linspace(0, params.sb_length, GRID_NX), np.linspace(0, params.sb_width, GRID_NY)


# ---------------------------------------------------------------------------
# Per-frame space creation computation
# ---------------------------------------------------------------------------


def compute_frame_space_creation(
    baseline_grid: np.ndarray,
    removed_grids: np.ndarray,
    ball_x: float,
    ball_y: float,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    player_ids: list[str],
    teams: list[str],
    n_real: int,
) -> list[dict[str, object]]:
    """Compute space creation for all players in a single frame.

    Uses pre-computed baseline and player-removed pitch control grids.
    Returns a list of dicts with per-player space creation metrics.
    """
    transition_interp = interpolate_grid(transition_grid, (GRID_NY, GRID_NX))
    epv_interp = interpolate_grid(epv_grid, (GRID_NY, GRID_NX))

    sigma_x, sigma_y = 30.0, 20.0
    xx, yy = np.meshgrid(grid_x, grid_y)
    dw = np.exp(-((xx - ball_x) ** 2) / (2 * sigma_x**2) - (yy - ball_y) ** 2 / (2 * sigma_y**2))
    eff_trans = transition_interp * dw
    mt = np.max(eff_trans)
    if mt > 1e-10:
        eff_trans /= mt
    obso_mult = eff_trans * epv_interp

    all_pc = np.concatenate([baseline_grid[None], removed_grids[:n_real]], axis=0)
    all_obso = np.clip(all_pc * obso_mult[None], 0.0, 1.0)
    baseline_obso = all_obso[0]
    removed_obso_all = all_obso[1:]

    is_home = np.array([t == "home" for t in teams[:n_real]])
    delta_home = baseline_obso[None] - removed_obso_all
    delta = np.where(is_home[:, None, None], delta_home, -delta_home)

    sc = np.sum(np.maximum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
    sd = np.sum(np.minimum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
    ns = np.sum(delta, axis=(1, 2)) * CELL_AREA_M2

    results: list[dict[str, object]] = []
    for i in range(n_real):
        results.append(
            {
                "player_id": player_ids[i],
                "team": teams[i],
                "space_created_m2": round(float(sc[i]), 4),
                "space_destroyed_m2": round(float(sd[i]), 4),
                "net_space_m2": round(float(ns[i]), 4),
            }
        )
    return results
