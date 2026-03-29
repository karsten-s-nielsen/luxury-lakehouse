# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
#     "jax[cuda12]>=0.4.35",
#     "numpy>=1.26.0",
#     "pandas>=2.0.0",
#     "pyarrow>=14.0.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "scipy>=1.11.0",
# ]
# ///
"""Compute per-player space creation values on HuggingFace Jobs (A10G GPU).

Downloads tracking data (IDSSE partition) from HF Hub, computes per-frame
pitch control surfaces with each player removed via JAX vmap, then derives
OBSO-based space creation/destruction metrics per player per frame.

Space Creation quantifies each player's contribution to off-ball scoring
opportunity by measuring the change in OBSO surface when that player is
hypothetically removed from the pitch.

Data sources:
  - Tracking: luxury-lakehouse/pitch-control-tracking (source_provider=idsse partition)
    Coordinates: StatsBomb 120x80 (gold schema fct_tracking_frames)
  - Trained grids: luxury-lakehouse/obso-trained-grids (D23)
    Falls back to synthetic grids if not available

Usage (HF Jobs CLI):
    hf jobs uv run scripts/compute_space_creation_hf.py \
        --flavor a10g --timeout 120m \
        --secrets HF_TOKEN=$HF_TOKEN

References:
    Fernandez & Bornn (2018). "Wide Open Spaces." MIT Sloan.
    Spearman (2018). "Beyond Expected Goals." MIT Sloan.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd

from analytics.cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from analytics.obso import interpolate_grid
from analytics.pitch_control import (
    PitchControlParams,
    compute_pitch_control_player_removal,
)
from workflows import workflow

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
TRACKING_DATASET = f"{HF_ORG}/pitch-control-tracking"
GRIDS_DATASET = f"{HF_ORG}/obso-trained-grids"
OUTPUT_DATASET = f"{HF_ORG}/space-creation-values"

# Half OBSO resolution for speed: 1,768 cells vs 7,072
GRID_NX = 52
GRID_NY = 34

# IDSSE tracking frame rate
FRAME_RATE = 25

# 1fps sampling: every 25th frame at 25fps source (reduces compute 5x)
FRAME_SAMPLE_STEP = 25

# Pitch dimensions
SB_LENGTH = 120.0
SB_WIDTH = 80.0
PITCH_LENGTH_M = 105.0
PITCH_WIDTH_M = 68.0

# Cell area in m^2 for space quantification
CELL_WIDTH_M = PITCH_LENGTH_M / GRID_NX
CELL_HEIGHT_M = PITCH_WIDTH_M / GRID_NY
CELL_AREA_M2 = CELL_WIDTH_M * CELL_HEIGHT_M


try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False


def _sb_to_meters_x(x: np.ndarray, params: PitchControlParams) -> np.ndarray:
    return x * (params.pitch_length_m / params.sb_length)


def _sb_to_meters_y(y: np.ndarray, params: PitchControlParams) -> np.ndarray:
    return y * (params.pitch_width_m / params.sb_width)


# ---------------------------------------------------------------------------
# Synthetic grid fallback
# ---------------------------------------------------------------------------


def _make_synthetic_grids() -> tuple[np.ndarray, np.ndarray]:
    """Create synthetic EPV and Transition grids for development.

    Returns (epv_grid, transition_grid) as numpy arrays.
    """
    # EPV: 32x50, values increase toward x=50 (attacking direction)
    epv = np.zeros((32, 50), dtype=np.float64)
    for i in range(50):
        epv[:, i] = 0.01 + 0.15 * (i / 49.0) ** 2
    for j in range(32):
        center_dist = abs(j - 16) / 16.0
        epv[j, :] *= 1.0 - 0.3 * center_dist

    # Transition: 64x100, Gaussian centered
    transition = np.zeros((64, 100), dtype=np.float64)
    for i in range(64):
        for j in range(100):
            transition[i, j] = np.exp(-((i - 32) ** 2 + (j - 50) ** 2) / (2 * 30**2))

    return epv, transition


# ---------------------------------------------------------------------------
# Data loading from HF Hub
# ---------------------------------------------------------------------------


def _load_trained_grids(hf_token: str) -> tuple[np.ndarray, np.ndarray]:
    """Load trained EPV and reachability grids from HF Hub.

    Falls back to synthetic grids if the dataset is not yet published.

    Returns (epv_grid, transition_grid) as numpy arrays.
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
        transition_grid = reachability_grid
        print(
            f"  Loaded trained grids from {GRIDS_DATASET} "
            f"(reachability: {reachability_grid.shape}, EPV: {epv_grid.shape})"
        )
        return epv_grid, transition_grid
    except (HfHubHTTPError, EntryNotFoundError, RepositoryNotFoundError):
        print("  WARNING: Trained grids unavailable -- falling back to synthetic grids")
        epv_grid, transition_grid = _make_synthetic_grids()
        print(f"  Using synthetic static grids (EPV: {epv_grid.shape}, Transition: {transition_grid.shape})")
        return epv_grid, transition_grid


# ---------------------------------------------------------------------------
# Frame processing
# ---------------------------------------------------------------------------


def _build_target_grid(params: PitchControlParams) -> np.ndarray:
    """Build (GRID_NY * GRID_NX, 2) array of target points in StatsBomb 120x80."""
    grid_x = np.linspace(0, params.sb_length, GRID_NX)
    grid_y = np.linspace(0, params.sb_width, GRID_NY)
    xx, yy = np.meshgrid(grid_x, grid_y)
    return np.column_stack([xx.ravel(), yy.ravel()])


def _get_grid_axes(params: PitchControlParams) -> tuple[np.ndarray, np.ndarray]:
    """Return (grid_x, grid_y) arrays for OBSO surface computation."""
    grid_x = np.linspace(0, params.sb_length, GRID_NX)
    grid_y = np.linspace(0, params.sb_width, GRID_NY)
    return grid_x, grid_y


def _compute_frame_space_creation(
    frame_players: pd.DataFrame,
    ball_x: float,
    ball_y: float,
    target_points: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    params: PitchControlParams,
) -> list[dict[str, object]]:
    """Compute space creation for all players in a single frame.

    Returns a list of dicts with per-player space creation metrics.
    """
    n_players_raw = len(frame_players)
    if n_players_raw < 2:
        return []

    # Need both teams for meaningful pitch control
    if frame_players["team"].nunique() < 2:
        return []

    # Deduplicate by player_id (tracking data may have multiple rows per player per frame)
    if "player_id" in frame_players.columns:
        frame_players = frame_players.drop_duplicates(subset=["player_id"], keep="first")

    # Cap to max_players (30) — compute_pitch_control_player_removal pads to fixed size
    max_players_local = 30
    n_players = min(len(frame_players), max_players_local)
    if len(frame_players) > max_players_local:
        frame_players = frame_players.head(max_players_local)

    # Compute baseline + all player removals via vmap (single GPU dispatch)
    baseline_flat, removed_flat = compute_pitch_control_player_removal(frame_players, target_points, params)

    # Reshape to (ny, nx) surfaces — removed_flat is (n_players, n_targets)
    baseline_grid = baseline_flat.reshape(GRID_NY, GRID_NX)
    removed_grids = removed_flat.reshape(n_players, GRID_NY, GRID_NX)

    # ── Vectorized OBSO computation ──────────────────────────────────
    # The transition/EPV grids and ball-distance weighting are CONSTANT
    # across all 23 player-removal variants. Only PPCF changes.
    # Compute the constant multiplier once, then broadcast.

    # Interpolate grids to match pitch control resolution
    transition_interp = interpolate_grid(transition_grid, (GRID_NY, GRID_NX))
    epv_interp = interpolate_grid(epv_grid, (GRID_NY, GRID_NX))

    # Gaussian ball-distance weighting (constant across variants)
    # grid_x and grid_y are 1D coordinate vectors — meshgrid to 2D (ny, nx)
    sigma_x, sigma_y = 30.0, 20.0
    xx, yy = np.meshgrid(grid_x, grid_y)  # (ny, nx) each
    distance_weight = np.exp(-((xx - ball_x) ** 2) / (2.0 * sigma_x**2) - (yy - ball_y) ** 2 / (2.0 * sigma_y**2))
    effective_transition = transition_interp * distance_weight
    max_trans = np.max(effective_transition)
    if max_trans > 1e-10:
        effective_transition /= max_trans

    # Combined constant multiplier: transition * EPV (ny, nx)
    obso_multiplier = effective_transition * epv_interp

    # Stack all PC surfaces: (n_players+1, ny, nx) — baseline + removed
    all_pc = np.concatenate([baseline_grid[None, :, :], removed_grids], axis=0)

    # Single broadcast: (n_players+1, ny, nx) * (1, ny, nx) → (n_players+1, ny, nx)
    all_obso = np.clip(all_pc * obso_multiplier[None, :, :], 0.0, 1.0)

    baseline_obso = all_obso[0]  # (ny, nx)
    removed_obso_all = all_obso[1:]  # (n_players, ny, nx)

    # ── Vectorized space creation per player ─────────────────────────
    player_ids = frame_players["player_id"].values
    teams = frame_players["team"].values
    is_home = teams == "home"

    # Home perspective: delta = baseline - removed (positive = player adds value)
    # Away perspective: delta = (1-baseline) - (1-removed) = removed - baseline
    delta_home = baseline_obso[None, :, :] - removed_obso_all  # (n_players, ny, nx)

    # Flip sign for away players
    delta = np.where(is_home[:, None, None], delta_home, -delta_home)

    # Sum over spatial dimensions: (n_players,)
    space_created = np.sum(np.maximum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
    space_destroyed = np.sum(np.minimum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
    net_space = np.sum(delta, axis=(1, 2)) * CELL_AREA_M2

    # Build results
    results: list[dict[str, object]] = []
    for i in range(n_players):
        results.append(
            {
                "player_id": str(player_ids[i]),
                "team": str(teams[i]),
                "space_created_m2": round(float(space_created[i]), 4),
                "space_destroyed_m2": round(float(space_destroyed[i]), 4),
                "net_space_m2": round(float(net_space[i]), 4),
            }
        )

    return results


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-space-creation", phase="grid_computation")
def main() -> None:
    """Download data, compute per-player space creation, publish results."""
    from huggingface_hub import HfApi, get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    params = PitchControlParams()

    recorder = HFJobsCostRecorder(
        workflow_id="wf-space-creation",
        phase="grid_computation",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=OUTPUT_DATASET,
    )
    recorder.start()

    print("=== Space Creation Batch Computation on HF Jobs GPU ===", flush=True)
    print(f"  JAX available: {_USE_JAX}", flush=True)
    if _USE_JAX:
        devices = jax.devices()
        print(f"  JAX devices: {[str(d) for d in devices]}", flush=True)
    print(f"  Grid resolution: {GRID_NX}x{GRID_NY} ({GRID_NX * GRID_NY} cells)", flush=True)
    print(f"  Cell area: {CELL_AREA_M2:.2f} m^2", flush=True)
    print(f"  Frame sampling: every {FRAME_SAMPLE_STEP}th frame ({FRAME_RATE // FRAME_SAMPLE_STEP} fps)", flush=True)

    # ------------------------------------------------------------------
    # 1. Load data from HF Hub
    # ------------------------------------------------------------------
    print("\n=== Loading data from HF Hub ===", flush=True)

    # Download trained grids (D23) -- fall back to synthetic if not yet published
    print("  Loading trained grids...", flush=True)
    epv_grid, transition_grid = _load_trained_grids(hf_token)
    print(f"  Grids loaded: EPV {epv_grid.shape}, Transition {transition_grid.shape}", flush=True)

    # Discover IDSSE parquet files (don't load yet — stream per-match)
    print("  Discovering IDSSE tracking files...", flush=True)
    from huggingface_hub import hf_hub_download

    all_repo_files = api.list_repo_files(TRACKING_DATASET, repo_type="dataset")
    idsse_parquet = [f for f in all_repo_files if "idsse" in f and f.endswith(".parquet")]
    print(f"  Found {len(idsse_parquet)} IDSSE parquet files", flush=True)

    if not idsse_parquet:
        print("  WARNING: No IDSSE tracking files found -- exiting gracefully.")
        return

    # Download files to local cache (HF caches them, so re-runs are fast)
    local_parquet_paths: list[str] = []
    for fname in idsse_parquet:
        local_path = hf_hub_download(
            repo_id=TRACKING_DATASET,
            filename=fname,
            repo_type="dataset",
            token=hf_token,
        )
        local_parquet_paths.append(local_path)
    print(f"  Downloaded {len(local_parquet_paths)} files to cache", flush=True)

    # Capture dataset commit hash for reproducibility (E5)
    _tracking_commit = api.repo_info(repo_id=TRACKING_DATASET, repo_type="dataset").sha

    # ------------------------------------------------------------------
    # 2. Build target grid
    # ------------------------------------------------------------------
    print("\n=== Preparing computation ===", flush=True)
    target_points = _build_target_grid(params)
    grid_x, grid_y = _get_grid_axes(params)
    print(f"  Target grid: {len(target_points)} points ({GRID_NY} x {GRID_NX})", flush=True)

    # Load column-selectively, index by match_id, process per-match then discard
    needed_cols = [
        "match_id",
        "frame",
        "period",
        "player_id",
        "team",
        "x",
        "y",
        "ball_x",
        "ball_y",
        "velocity_x",
        "velocity_y",
    ]
    # Load all IDSSE files but only needed columns
    dfs: list[pd.DataFrame] = []
    for fp in local_parquet_paths:
        try:
            df = pd.read_parquet(fp, columns=needed_cols)
        except Exception:
            df = pd.read_parquet(fp)
            available = [c for c in needed_cols if c in df.columns]
            df = df[available]
        dfs.append(df)
    tracking_df = pd.concat(dfs, ignore_index=True)
    del dfs
    print(f"  Loaded {len(tracking_df):,} tracking rows", flush=True)

    # Pre-build per-match index (CLAUDE.md: never filter inside loops)
    match_groups: dict[str, pd.DataFrame] = dict(iter(tracking_df.groupby("match_id")))
    match_ids = sorted(match_groups.keys())
    print(f"  {len(match_ids)} matches indexed", flush=True)
    del tracking_df  # free the monolithic frame

    # ------------------------------------------------------------------
    # 3. Process frames (5fps sampling)
    # ------------------------------------------------------------------
    print("\n=== Processing frames (5fps sampling) ===", flush=True)

    all_results: list[dict[str, object]] = []
    total_start = time.time()
    total_frames_processed = 0
    total_frames_skipped = 0
    total_players_processed = 0

    for match_idx, match_id in enumerate(match_ids):
        match_start = time.time()
        match_tracking = match_groups[match_id]

        # Pre-index frames within match (avoid O(n) filter per frame)
        frame_groups: dict[int, pd.DataFrame] = dict(iter(match_tracking.groupby("frame")))
        match_frames = sorted(frame_groups.keys())

        # Sample at 5fps: every FRAME_SAMPLE_STEP-th frame
        sampled_frames = match_frames[::FRAME_SAMPLE_STEP]
        print(
            f"\n  Match {match_idx + 1}/{len(match_ids)}: {match_id} "
            f"({len(match_frames)} total frames, {len(sampled_frames)} sampled)",
            flush=True,
        )

        match_results: list[dict[str, object]] = []
        frames_this_match = 0

        # ── Pre-extract all sampled frames into padded arrays ────────
        # This avoids per-frame Python→JAX overhead by batching
        max_p = 30  # Fixed player count for JAX shape consistency
        n_targets = len(target_points)

        # Pre-compute OBSO constant multiplier (same for entire match)
        transition_interp = interpolate_grid(transition_grid, (GRID_NY, GRID_NX))
        epv_interp = interpolate_grid(epv_grid, (GRID_NY, GRID_NX))

        # Collect valid frames into batched arrays
        valid_frame_nums: list[int] = []
        valid_periods: list[int] = []
        valid_ball_xy: list[tuple[float, float]] = []
        valid_player_ids: list[list[str]] = []
        valid_teams: list[list[str]] = []
        valid_n_real: list[int] = []
        batch_xy: list[np.ndarray] = []
        batch_vel: list[np.ndarray] = []
        batch_home: list[np.ndarray] = []
        batch_presence: list[np.ndarray] = []

        for frame_num in sampled_frames:
            frame_data = frame_groups.get(frame_num)
            if frame_data is None or frame_data.empty:
                total_frames_skipped += 1
                continue

            # Deduplicate players
            if "player_id" in frame_data.columns:
                frame_data = frame_data.drop_duplicates(subset=["player_id"], keep="first")

            # Ball position
            bx = float(frame_data["ball_x"].iloc[0]) if "ball_x" in frame_data.columns else SB_LENGTH / 2.0
            by = float(frame_data["ball_y"].iloc[0]) if "ball_y" in frame_data.columns else SB_WIDTH / 2.0
            if np.isnan(bx) or np.isnan(by):
                total_frames_skipped += 1
                continue

            # Extract arrays
            x_arr = np.asarray(frame_data["x"], dtype=np.float64)
            y_arr = np.asarray(frame_data["y"], dtype=np.float64)
            vx_arr = np.asarray(frame_data["velocity_x"], dtype=np.float64)
            vy_arr = np.asarray(frame_data["velocity_y"], dtype=np.float64)

            if np.any(np.isnan(x_arr)) or np.any(np.isnan(y_arr)):
                total_frames_skipped += 1
                continue

            n_real = min(len(frame_data), max_p)

            # Pad to max_p
            xy = np.zeros((max_p, 2), dtype=np.float64)
            vel = np.zeros((max_p, 2), dtype=np.float64)
            home = np.zeros(max_p, dtype=bool)
            presence = np.zeros(max_p, dtype=np.float64)

            xy[:n_real, 0] = _sb_to_meters_x(x_arr[:n_real], params)
            xy[:n_real, 1] = _sb_to_meters_y(y_arr[:n_real], params)
            vel[:n_real, 0] = _sb_to_meters_x(vx_arr[:n_real], params)
            vel[:n_real, 1] = _sb_to_meters_y(vy_arr[:n_real], params)
            home[:n_real] = frame_data["team"].values[:n_real] == "home"
            presence[:n_real] = 1.0

            valid_frame_nums.append(frame_num)
            valid_periods.append(int(frame_data["period"].iloc[0]))
            valid_ball_xy.append((bx, by))
            valid_player_ids.append([str(p) for p in frame_data["player_id"].values[:n_real]])
            valid_teams.append([str(t) for t in frame_data["team"].values[:n_real]])
            valid_n_real.append(n_real)
            batch_xy.append(xy)
            batch_vel.append(vel)
            batch_home.append(home)
            batch_presence.append(presence)

        n_valid = len(valid_frame_nums)
        if n_valid == 0:
            print(f"  No valid frames in match {match_id}", flush=True)
            continue

        print(f"    Prepared {n_valid} valid frames (skipped {len(sampled_frames) - n_valid})", flush=True)

        # Stack into batched arrays: (n_valid, max_p, 2) etc.
        all_xy = np.stack(batch_xy)  # (n_valid, 30, 2)
        all_vel = np.stack(batch_vel)  # (n_valid, 30, 2)
        all_home = np.stack(batch_home)  # (n_valid, 30)
        all_presence = np.stack(batch_presence)  # (n_valid, 30)

        # Build masks: (max_p+1, max_p) — same for all frames
        masks = np.ones((max_p + 1, max_p), dtype=np.float64)
        for i in range(max_p):
            masks[i + 1, i] = 0.0

        # Convert targets to meters once
        targets_m = np.column_stack(
            [
                _sb_to_meters_x(target_points[:, 0], params),
                _sb_to_meters_y(target_points[:, 1], params),
            ]
        )

        # ── Batched JAX computation ──────────────────────────────
        # Process in chunks to limit GPU memory
        chunk_size = 256  # frames per GPU batch
        all_baseline = np.zeros((n_valid, n_targets), dtype=np.float64)
        all_removed = np.zeros((n_valid, max_p, n_targets), dtype=np.float64)

        # JAX already imported at module level (jax, jnp)

        # Define the per-frame vmap (same as before, but we'll vmap over frames too)
        @jax.jit
        def _pc_single_variant(mask, xy, vel, home_mask, targets, reaction_time, max_accel, sigma):
            large_tti = 1e6
            disp = targets[None, :, :] - xy[:, None, :]
            dist = jnp.sqrt(jnp.sum(disp**2, axis=-1))
            direction = disp / jnp.maximum(dist[:, :, None], 1e-10)
            v_proj = jnp.sum(vel[:, None, :] * direction, axis=-1)
            discriminant = v_proj**2 + 2.0 * max_accel * dist
            tti = reaction_time + (-v_proj + jnp.sqrt(jnp.maximum(discriminant, 0.0))) / max_accel
            tti = jnp.maximum(tti, reaction_time)
            tti = jnp.where(mask[:, None] > 0.5, tti, large_tti)
            home_tti = jnp.where(home_mask[:, None], tti, large_tti)
            away_tti = jnp.where(~home_mask[:, None], tti, large_tti)
            home_min_tti = jnp.min(home_tti, axis=0)
            away_min_tti = jnp.min(away_tti, axis=0)
            k = jnp.pi / jnp.sqrt(3.0) / sigma
            home_exp = -k * (away_min_tti[None, :] - home_tti)
            home_individual = 1.0 / (1.0 + jnp.exp(jnp.clip(home_exp, -50.0, 50.0)))
            home_influence = jnp.sum(home_individual * home_mask[:, None], axis=0)
            away_exp = -k * (home_min_tti[None, :] - away_tti)
            away_individual = 1.0 / (1.0 + jnp.exp(jnp.clip(away_exp, -50.0, 50.0)))
            away_influence = jnp.sum(away_individual * (~home_mask[:, None]).astype(jnp.float32), axis=0)
            total = home_influence + away_influence
            safe_total = jnp.where(total > 1e-10, total, 1.0)
            return jnp.clip(jnp.where(total > 1e-10, home_influence / safe_total, 0.5), 0.0, 1.0)

        # vmap over masks (player removal variants)
        _pc_all_variants = jax.vmap(
            _pc_single_variant,
            in_axes=(0, None, None, None, None, None, None, None),
        )
        # vmap over frames (outer batch dimension)
        # axis 0 = frames for masks/xy/vel/home; None = shared targets/params
        _pc_batch_frames = jax.jit(
            jax.vmap(
                _pc_all_variants,
                in_axes=(0, 0, 0, 0, None, None, None, None),
            )
        )

        jnp_masks = jnp.array(masks)
        jnp_targets = jnp.array(targets_m)

        for chunk_start in range(0, n_valid, chunk_size):
            chunk_end = min(chunk_start + chunk_size, n_valid)
            chunk_xy = jnp.array(all_xy[chunk_start:chunk_end])
            chunk_vel = jnp.array(all_vel[chunk_start:chunk_end])
            chunk_home = jnp.array(all_home[chunk_start:chunk_end])

            # Apply presence mask to the removal masks per frame
            chunk_presence = jnp.array(all_presence[chunk_start:chunk_end])  # (chunk, 30)
            chunk_masks = jnp_masks[None, :, :] * chunk_presence[:, None, :]  # (chunk, 31, 30)

            # Double vmap: frames x variants in ONE GPU dispatch
            chunk_results = np.asarray(
                _pc_batch_frames(
                    chunk_masks,
                    chunk_xy,
                    chunk_vel,
                    chunk_home,
                    jnp_targets,
                    params.reaction_time,
                    params.max_acceleration,
                    params.sigma,
                )
            )  # (chunk, 31, n_targets)

            all_baseline[chunk_start:chunk_end] = chunk_results[:, 0, :]
            all_removed[chunk_start:chunk_end] = chunk_results[:, 1:, :]

            elapsed = time.time() - match_start
            print(
                f"    GPU chunk [{chunk_end}/{n_valid}] elapsed={elapsed:.1f}s",
                flush=True,
            )

        # ── Vectorized OBSO + space creation per frame ───────────
        for fi in range(n_valid):
            n_real = valid_n_real[fi]
            bx, by = valid_ball_xy[fi]

            baseline_grid = all_baseline[fi].reshape(GRID_NY, GRID_NX)
            removed_grids = all_removed[fi, :n_real].reshape(n_real, GRID_NY, GRID_NX)

            # Gaussian ball-distance weighting
            sigma_x, sigma_y = 30.0, 20.0
            xx, yy = np.meshgrid(grid_x, grid_y)
            dw = np.exp(-((xx - bx) ** 2) / (2 * sigma_x**2) - (yy - by) ** 2 / (2 * sigma_y**2))
            eff_trans = transition_interp * dw
            mt = np.max(eff_trans)
            if mt > 1e-10:
                eff_trans /= mt
            obso_mult = eff_trans * epv_interp

            all_pc = np.concatenate([baseline_grid[None], removed_grids], axis=0)
            all_obso = np.clip(all_pc * obso_mult[None], 0.0, 1.0)
            baseline_obso = all_obso[0]
            removed_obso_all = all_obso[1:]

            teams_arr = valid_teams[fi]
            is_home = np.array([t == "home" for t in teams_arr])
            delta_home = baseline_obso[None] - removed_obso_all
            delta = np.where(is_home[:, None, None], delta_home, -delta_home)

            sc = np.sum(np.maximum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
            sd = np.sum(np.minimum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
            ns = np.sum(delta, axis=(1, 2)) * CELL_AREA_M2

            for pi in range(n_real):
                match_results.append(
                    {
                        "match_id": match_id,
                        "frame_id": int(valid_frame_nums[fi]),
                        "period": valid_periods[fi],
                        "player_id": valid_player_ids[fi][pi],
                        "team": teams_arr[pi],
                        "space_created_m2": round(float(sc[pi]), 4),
                        "space_destroyed_m2": round(float(sd[pi]), 4),
                        "net_space_m2": round(float(ns[pi]), 4),
                    }
                )

            frames_this_match += 1
            total_frames_processed += 1
            total_players_processed += n_real

        all_results.extend(match_results)
        match_elapsed = time.time() - match_start
        print(
            f"  Match complete: {frames_this_match} frames, "
            f"{len(match_results)} player-frame rows, {match_elapsed:.1f}s"
        )

    total_elapsed = time.time() - total_start
    n_rows = len(all_results)

    print("\n=== Computation Summary ===")
    print(f"  Total frames processed: {total_frames_processed:,}")
    print(f"  Total frames skipped: {total_frames_skipped:,}")
    print(f"  Total player-frame rows: {n_rows:,}")
    print(f"  Total players processed: {total_players_processed:,}")
    print(f"  Total time: {total_elapsed:.2f}s")
    if total_frames_processed > 0:
        print(f"  Avg time per frame: {total_elapsed / total_frames_processed:.3f}s")

    if n_rows == 0:
        print("  WARNING: No space creation values computed. Check tracking data.")
        return

    # ------------------------------------------------------------------
    # 4. Build results DataFrame
    # ------------------------------------------------------------------
    results_df = pd.DataFrame(all_results)

    # Reorder columns for clarity
    col_order = [
        "match_id",
        "frame_id",
        "player_id",
        "team",
        "period",
        "space_created_m2",
        "space_destroyed_m2",
        "net_space_m2",
    ]
    results_df = results_df[[c for c in col_order if c in results_df.columns]]

    # Summary statistics
    print("\n=== Result Statistics ===")
    print(f"  Rows: {len(results_df):,}")
    print(f"  Unique players: {results_df['player_id'].nunique()}")
    print(f"  Unique matches: {results_df['match_id'].nunique()}")
    print(f"  Mean space_created_m2: {results_df['space_created_m2'].mean():.2f}")
    print(f"  Mean space_destroyed_m2: {results_df['space_destroyed_m2'].mean():.2f}")
    print(f"  Mean net_space_m2: {results_df['net_space_m2'].mean():.2f}")

    # ------------------------------------------------------------------
    # 5. Log to MLflow (conditional)
    # ------------------------------------------------------------------
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        print("\n=== Logging to MLflow ===")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/space_creation")

        with mlflow.start_run(run_name="space_creation_batch"):
            mlflow.log_params(
                {
                    "grid_nx": GRID_NX,
                    "grid_ny": GRID_NY,
                    "frame_sample_step": FRAME_SAMPLE_STEP,
                    "effective_fps": FRAME_RATE // FRAME_SAMPLE_STEP,
                    "n_matches": len(match_ids),
                    "n_frames_processed": total_frames_processed,
                    "n_player_frame_rows": n_rows,
                    "training_env": "hf_jobs_a10g",
                    "jax_available": _USE_JAX,
                    "tracking_dataset_commit": _tracking_commit,
                }
            )
            mlflow.log_metrics(
                {
                    "mean_space_created_m2": float(results_df["space_created_m2"].mean()),
                    "mean_space_destroyed_m2": float(results_df["space_destroyed_m2"].mean()),
                    "mean_net_space_m2": float(results_df["net_space_m2"].mean()),
                    "std_net_space_m2": float(results_df["net_space_m2"].std()),
                    "total_elapsed_seconds": total_elapsed,
                    "frames_per_second": total_frames_processed / max(total_elapsed, 0.01),
                    "total_frames_processed": total_frames_processed,
                    "total_frames_skipped": total_frames_skipped,
                    "unique_players": int(results_df["player_id"].nunique()),
                }
            )
        print("  Metrics logged to MLflow")
    else:
        print("\n=== MLflow skipped (MLFLOW_TRACKING_URI not set) ===")

    # ------------------------------------------------------------------
    # 6. Publish to HF Hub
    # ------------------------------------------------------------------
    print("\n=== Publishing results to HF Hub ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        # Save space creation values
        results_df.to_parquet(str(data_dir / "space_creation.parquet"), index=False)

        # Save metadata
        metadata: dict[str, object] = {
            "grid_nx": GRID_NX,
            "grid_ny": GRID_NY,
            "cell_area_m2": round(CELL_AREA_M2, 4),
            "frame_rate": FRAME_RATE,
            "frame_sample_step": FRAME_SAMPLE_STEP,
            "effective_fps": FRAME_RATE // FRAME_SAMPLE_STEP,
            "n_frames_processed": total_frames_processed,
            "n_frames_skipped": total_frames_skipped,
            "n_player_frame_rows": n_rows,
            "n_matches": len(match_ids),
            "match_ids": match_ids,
            "n_unique_players": int(results_df["player_id"].nunique()),
            "jax_used": _USE_JAX,
            "total_elapsed_seconds": round(total_elapsed, 2),
            "data_sources": {
                "tracking": TRACKING_DATASET,
                "trained_grids": GRIDS_DATASET,
            },
            "tracking_dataset_commit": _tracking_commit,
        }
        metadata = recorder.complete(metadata, row_count=n_rows)
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

        # Dataset card
        card = f"""---
license: mit
tags:
  - soccer
  - football
  - space-creation
  - pitch-control
  - obso
  - analytics
  - idsse
  - bundesliga
size_categories:
  - 100K-1M
---

# Space Creation Values

Per-player per-frame space creation metrics computed via JAX-accelerated
pitch control with player removal on A10G GPU. **{n_rows:,} player-frame rows**
across **{len(match_ids)} IDSSE Bundesliga matches**.

## Method

Space creation quantifies each player's contribution to off-ball scoring
opportunity (OBSO) by measuring the change in OBSO surface when that player
is hypothetically removed from the pitch (Fernandez & Bornn 2018).

For each sampled frame:
1. Compute baseline pitch control surface with all players via JAX
2. Compute N player-removal variants via `jax.vmap` (one GPU dispatch per frame)
3. Convert each pitch control surface to OBSO surface
4. `space_created_m2`: sum of cells where OBSO increased due to player presence
5. `space_destroyed_m2`: sum of cells where OBSO decreased due to player presence
6. `net_space_m2`: total OBSO contribution in square meters

### Parameters
- **Grid resolution**: {GRID_NX} x {GRID_NY} cells ({GRID_NX * GRID_NY:,} total)
- **Cell area**: {CELL_AREA_M2:.2f} m^2
- **Frame sampling**: {FRAME_RATE // FRAME_SAMPLE_STEP} fps (every {FRAME_SAMPLE_STEP}th frame)
- **Coordinate system**: StatsBomb 120x80

## Contents

- `data/space_creation.parquet` -- Per-player per-frame values ({n_rows:,} rows)
- `metadata.json` -- Computation parameters, timing, and data provenance

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | string | Match identifier (`idsse_J03...`) |
| `frame_id` | int | Tracking frame number |
| `player_id` | string | DFL PersonId |
| `team` | string | Player's team (`home` / `away`) |
| `period` | int | Match half (1 or 2) |
| `space_created_m2` | double | OBSO area added by player presence (m^2, >= 0) |
| `space_destroyed_m2` | double | OBSO area removed by player presence (m^2, <= 0) |
| `net_space_m2` | double | Net OBSO contribution (m^2, positive = beneficial) |

## Input Data

- **Tracking**: [`{TRACKING_DATASET}`](https://huggingface.co/datasets/{TRACKING_DATASET}) (IDSSE partition)
- **Trained grids**: [`{GRIDS_DATASET}`](https://huggingface.co/datasets/{GRIDS_DATASET})

## References

- Fernandez, J. & Bornn, L. (2018). "Wide Open Spaces." MIT Sloan.
- Spearman, W. (2018). "Beyond Expected Goals." MIT Sloan.
- Bassek et al. (2025). "An integrated dataset of spatiotemporal and event data in elite soccer." Sci. Data.

## License

MIT -- computed from IDSSE open data (CC-BY 4.0).
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

    print(f"\n  Published: https://huggingface.co/datasets/{OUTPUT_DATASET}")
    print(f"  Player-frame rows: {n_rows:,}")
    print(f"  Matches: {len(match_ids)}")
    print(f"  Unique players: {results_df['player_id'].nunique()}")
    print("Space creation batch computation complete!")


if __name__ == "__main__":
    import sys
    import traceback

    print("=== Script starting ===", flush=True)
    print(f"JAX available: {_USE_JAX}", flush=True)
    if _USE_JAX:
        print(f"JAX devices: {jax.devices()}", flush=True)
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
