# /// script
# requires-python = ">=3.10,<3.11"
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
"""Compute OBSO value surfaces on HuggingFace Jobs (A10G GPU).

Downloads tracking data (IDSSE partition), ELASTIC sync results, and event data
from HF Hub.  For each pass event in each match, generates ghost trajectories,
computes pitch control via JAX on GPU, and produces OBSO surfaces and
PAUSA-relevant scalar metrics.

Data sources:
  - Tracking: luxury-lakehouse/pitch-control-tracking (source_provider=idsse partition)
    Coordinates: StatsBomb 120x80 (gold schema fct_tracking_frames)
  - Events: luxury-lakehouse/obso-pausa-inputs (events config)
    Coordinates: DFL pitch-origin meters (x: 0-105, y: 0-68) — transformed here
  - ELASTIC sync: luxury-lakehouse/obso-pausa-inputs (elastic_sync config)
    Maps event_id -> frame_id (tracking frame number)

Usage (HF Jobs CLI):
    hf jobs uv run scripts/compute_obso_hf.py \
        --flavor a10g --timeout 60m \
        --secrets HF_TOKEN=$HF_TOKEN

References:
    Spearman (2018). "Beyond Expected Goals." MIT Sloan.
    Fernandez & Bornn (2018). "Wide Open Spaces." MIT Sloan.
    Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd

from analytics.obso import interpolate_grid
from analytics.pitch_control import (
    PitchControlParams,
    compute_pitch_control_grid_fast,
    generate_ghost_trajectories,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from workflows import workflow

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
TRACKING_DATASET = f"{HF_ORG}/pitch-control-tracking"
INPUTS_DATASET = f"{HF_ORG}/obso-pausa-inputs"
OUTPUT_DATASET = f"{HF_ORG}/obso-pausa-values"

GRID_NX = 104
GRID_NY = 68

# IDSSE tracking frame rate
FRAME_RATE = 25

# Ghost trajectory window
WINDOW_BEFORE_S = 3.0
WINDOW_AFTER_S = 1.0


try:
    import jax

    _USE_JAX = True
except ImportError:
    _USE_JAX = False


# ---------------------------------------------------------------------------
# Static grids (synthetic — production would use trained EPV/Transition)
# ---------------------------------------------------------------------------


def _make_synthetic_grids():
    """Create synthetic EPV and Transition grids for development.

    In production, these would be loaded from the PAUSA repository or
    trained on real data.  For now, use reasonable approximations:
    - EPV increases toward the attacking goal
    - Transition decreases with distance from origin
    """
    # EPV: 32x50, values increase toward x=50 (attacking direction)
    epv = np.zeros((32, 50), dtype=np.float64)
    for i in range(50):
        epv[:, i] = 0.01 + 0.15 * (i / 49.0) ** 2
    # Higher in central channels
    for j in range(32):
        center_dist = abs(j - 16) / 16.0
        epv[j, :] *= 1.0 - 0.3 * center_dist

    # Transition: 64x100, Gaussian centered (F-08 OPT-AUDIT-200: vectorized)
    ii, jj = np.mgrid[0:64, 0:100]
    transition = np.exp(-((ii - 32) ** 2 + (jj - 50) ** 2) / (2 * 30**2))

    return epv, transition


# ---------------------------------------------------------------------------
# Data loading from HF Hub
# ---------------------------------------------------------------------------


def _load_tracking_data(hf_token):
    """Download IDSSE tracking data from HF Hub (pitch-control-tracking dataset).

    The tracking data is partitioned by source_provider on HF Hub. We download
    only the IDSSE partition (source_provider=idsse). Coordinates are already
    in StatsBomb 120x80 (gold schema fct_tracking_frames).

    Returns a pandas DataFrame with columns: tracking_id, match_id, player_id,
    team, period, frame, timestamp_seconds, x, y, ball_x, ball_y, velocity_x,
    velocity_y, speed_ms, pitch_control_value, source_provider, frame_rate.
    """

    print("  Downloading IDSSE tracking data from HF Hub ...")
    print(f"  Repo: {TRACKING_DATASET}")

    # Use HfApi.list_repo_files + hf_hub_download instead of snapshot_download
    # because snapshot_download's allow_patterns fails with '=' in Hive partition paths
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_files = api.list_repo_files(TRACKING_DATASET, repo_type="dataset")
    idsse_parquet = [f for f in all_files if "idsse" in f and f.endswith(".parquet")]
    print(f"  Found {len(idsse_parquet)} IDSSE parquet files on HF Hub")

    if not idsse_parquet:
        raise FileNotFoundError(
            f"No IDSSE parquet files in {TRACKING_DATASET}. "
            "Ensure the dataset has data/source_provider=idsse/ partition."
        )

    parquet_files = []
    for fname in idsse_parquet:
        local_path = hf_hub_download(
            repo_id=TRACKING_DATASET,
            filename=fname,
            repo_type="dataset",
            token=hf_token,
        )
        parquet_files.append(Path(local_path))
    print(f"  Downloaded {len(parquet_files)} files")

    # Load tracking per-match to avoid OOM on small instances
    print(f"  Loading {len(parquet_files)} parquet files...")
    # Only load needed columns to reduce memory
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
        "frame_rate",
        "timestamp_seconds",
    ]
    dfs = []
    for i, f in enumerate(parquet_files):
        try:
            df = pd.read_parquet(str(f), columns=needed_cols)
        except Exception:
            # If columns don't match exactly, load all and filter
            df = pd.read_parquet(str(f))
            available = [c for c in needed_cols if c in df.columns]
            df = df[available]
        dfs.append(df)
        if i % 10 == 0:
            print(f"    Loaded {i + 1}/{len(parquet_files)} files ({len(df)} rows)")
    tracking_df = pd.concat(dfs, ignore_index=True)
    print(f"  Total tracking: {len(tracking_df)} rows, {tracking_df.memory_usage(deep=True).sum() / 1e6:.0f} MB")

    # The Hive-style partition column (source_provider) may not be in the
    # parquet files themselves — add it if missing
    if "source_provider" not in tracking_df.columns:
        tracking_df["source_provider"] = "idsse"

    print(f"  Tracking data loaded: {len(tracking_df):,} rows, {tracking_df['match_id'].nunique()} matches")
    return tracking_df


def _load_events_and_sync(hf_token):
    """Download IDSSE events and ELASTIC sync results from HF Hub.

    Events are in DFL pitch-origin meters (x: 0-105, y: 0-68).
    We convert coordinates to StatsBomb 120x80 here.

    ELASTIC sync results map event_id -> frame_id (tracking frame number).

    Returns (events_df, sync_df) as pandas DataFrames.
    """
    from huggingface_hub import HfApi, hf_hub_download

    print("  Downloading events and ELASTIC sync from HF Hub ...")
    api = HfApi(token=hf_token)
    all_files = api.list_repo_files(INPUTS_DATASET, repo_type="dataset")

    # Download event parquets
    event_remote = [f for f in all_files if "events" in f and f.endswith(".parquet")]
    print(f"  Found {len(event_remote)} event parquet files")
    event_files = []  # (local_path, match_id) tuples
    for fname in event_remote:
        local_path = hf_hub_download(INPUTS_DATASET, filename=fname, repo_type="dataset", token=hf_token)
        # Extract match_id from Hive partition path: data/events/match_id=idsse_J03WMX/...
        mid = None
        for part in fname.split("/"):
            if part.startswith("match_id="):
                mid = part.split("=", 1)[1]
        event_files.append((Path(local_path), mid))

    if not event_files:
        raise FileNotFoundError(
            f"No event parquet files found in {INPUTS_DATASET}. Run publish_obso_data notebook first."
        )
    events_dfs = []
    for fpath, mid in event_files:
        df = pd.read_parquet(str(fpath))
        if mid and "match_id" not in df.columns:
            df["match_id"] = mid
        events_dfs.append(df)
    events_df = pd.concat(events_dfs, ignore_index=True)

    # Convert event coordinates from DFL pitch-origin (0-105, 0-68) to
    # StatsBomb 120x80. This matches stg_idsse__events.sql transform.
    events_df["x_sb"] = events_df["x"] / 105.0 * 120.0
    events_df["y_sb"] = events_df["y"] / 68.0 * 80.0

    print(f"  Events loaded: {len(events_df):,} rows, {events_df['match_id'].nunique()} matches")

    # Download ELASTIC sync results
    sync_remote = [f for f in all_files if "elastic_sync" in f and f.endswith(".parquet")]
    print(f"  Found {len(sync_remote)} sync parquet files")
    sync_files = []  # (local_path, match_id) tuples
    for fname in sync_remote:
        local_path = hf_hub_download(INPUTS_DATASET, filename=fname, repo_type="dataset", token=hf_token)
        mid = None
        for part in fname.split("/"):
            if part.startswith("match_id="):
                mid = part.split("=", 1)[1]
        sync_files.append((Path(local_path), mid))
    if not sync_files:
        raise FileNotFoundError(
            f"No sync parquet files found in {INPUTS_DATASET}. Run publish_obso_data notebook first."
        )
    sync_dfs = []
    for fpath, mid in sync_files:
        df = pd.read_parquet(str(fpath))
        if mid and "match_id" not in df.columns:
            df["match_id"] = mid
        sync_dfs.append(df)
    sync_df = pd.concat(sync_dfs, ignore_index=True)

    print(f"  ELASTIC sync loaded: {len(sync_df):,} rows")

    return events_df, sync_df


def _prepare_pass_events(events_df, sync_df):
    """Filter to pass events and join with ELASTIC sync frame mappings.

    DFL event taxonomy: event_type == "Play" represents ball plays.
    In the DFL XML, <Play> elements with <Pass> children are passes.
    Since the bronze table only stores event_type (not the child type),
    we use event_type == "Play" as the pass filter. This captures all
    ball-in-play actions (passes, dribbles, etc.), which is appropriate
    for PAUSA analysis — the algorithm evaluates all ball release moments.

    KickOff events also contain passes but are excluded (set pieces with
    predictable timing are less interesting for PAUSA).

    Returns a DataFrame with pass events joined to their aligned tracking
    frame_id via the ELASTIC sync results.
    """
    # Filter to Play events (passes and other ball actions)
    passes = events_df[events_df["event_type"] == "Play"].copy()
    print(f"  Pass events (event_type=Play): {len(passes):,}")

    if passes.empty:
        return pd.DataFrame()

    # Join with ELASTIC sync to get frame_id for each pass
    # Inner join: only passes that have a successful frame alignment
    passes_with_frames = passes.merge(
        sync_df[["match_id", "event_id", "frame_id", "alignment_confidence"]],
        on=["match_id", "event_id"],
        how="inner",
    )

    print(f"  Passes with ELASTIC frame alignment: {len(passes_with_frames):,}")

    if passes_with_frames.empty:
        print(
            "  WARNING: No passes matched to ELASTIC sync results. Check that event_id values match between datasets."
        )

    return passes_with_frames


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-obso-pausa", phase="grid_computation")
def main() -> None:
    """Download data, compute OBSO surfaces, publish results."""
    from huggingface_hub import HfApi, get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    params = PitchControlParams()

    recorder = HFJobsCostRecorder(
        workflow_id="wf-obso-pausa",
        phase="grid_computation",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=OUTPUT_DATASET,
    )
    recorder.start()

    print("=== OBSO Batch Computation on HF Jobs GPU ===")
    print(f"  JAX available: {_USE_JAX}")
    if _USE_JAX:
        devices = jax.devices()
        print(f"  JAX devices: {[str(d) for d in devices]}")

    # ------------------------------------------------------------------
    # 1. Load data from HF Hub
    # ------------------------------------------------------------------
    print("\n=== Loading data from HF Hub ===")

    # Download trained grids (D23) — fall back to synthetic if not yet published
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError, HfHubHTTPError, RepositoryNotFoundError

    try:
        reachability_path = hf_hub_download(
            repo_id="luxury-lakehouse/obso-trained-grids",
            filename="data/reachability_grid_global.parquet",
            repo_type="dataset",
        )
        epv_path = hf_hub_download(
            repo_id="luxury-lakehouse/obso-trained-grids",
            filename="data/epv_grid_global.parquet",
            repo_type="dataset",
        )
        # Load trained 2D spatial grids
        _reach_df = pd.read_parquet(reachability_path)
        reachability_grid = _reach_df.pivot(index="zone_y", columns="zone_x", values="reachability").values.astype(
            np.float64
        )
        _epv_df = pd.read_parquet(epv_path)
        epv_grid = _epv_df.pivot(index="zone_y", columns="zone_x", values="epv_value").values.astype(np.float64)
        transition_grid = reachability_grid
        print(
            f"  Loaded trained grids from luxury-lakehouse/obso-trained-grids "
            f"(reachability: {reachability_grid.shape}, EPV: {epv_grid.shape})"
        )
    except (HfHubHTTPError, EntryNotFoundError, RepositoryNotFoundError):
        print("  WARNING: Trained grids unavailable — falling back to synthetic grids")
        epv_grid, transition_grid = _make_synthetic_grids()
        print(f"  Using synthetic static grids (EPV: {epv_grid.shape}, Transition: {transition_grid.shape})")

    # Load real tracking data (IDSSE partition, StatsBomb 120x80 coordinates)
    tracking_df = _load_tracking_data(hf_token)

    # Load events (DFL coords, converted to SB 120x80) and ELASTIC sync results
    events_df, sync_df = _load_events_and_sync(hf_token)

    # Capture dataset commit hashes for reproducibility (E5)
    _tracking_commit = api.repo_info(repo_id=TRACKING_DATASET, repo_type="dataset").sha
    _inputs_commit = api.repo_info(repo_id=INPUTS_DATASET, repo_type="dataset").sha

    # Prepare pass events with frame alignments
    passes_df = _prepare_pass_events(events_df, sync_df)

    if passes_df.empty:
        print("\nNo passes with frame alignments found — cannot compute OBSO.")
        print("Ensure the ELASTIC sync pipeline has run and data is published.")
        return

    # ------------------------------------------------------------------
    # 2. Build tracking index for fast frame lookups
    # ------------------------------------------------------------------
    print("\n=== Building tracking index ===")

    # Index tracking by (match_id, period, frame) for O(1) lookups
    # Each lookup returns all players at that frame
    tracking_grouped = tracking_df.groupby(["match_id", "period", "frame"])
    print(f"  Indexed {tracking_grouped.ngroups:,} unique (match, period, frame) groups")

    # ------------------------------------------------------------------
    # 3. Process passes
    # ------------------------------------------------------------------
    print("\n=== Processing passes ===")

    match_ids = sorted(passes_df["match_id"].unique())
    n_passes_total = len(passes_df)
    all_results = []
    total_start = time.time()
    skipped_no_tracking = 0
    skipped_insufficient_players = 0
    pass_counter = 0

    # Pre-build indexed lookup to avoid O(n*m) boolean mask (F-03 OPT-AUDIT-200)
    passes_by_match = dict(iter(passes_df.groupby("match_id")))

    for match_id in match_ids:
        match_passes = passes_by_match.get(match_id)
        if match_passes is None or match_passes.empty:
            continue
        print(f"\n  Match {match_id}: {len(match_passes)} passes")

        for _, pass_row in match_passes.iterrows():
            pass_counter += 1
            event_id = str(pass_row["event_id"])
            player_id = str(pass_row["player_id"])
            team = str(pass_row["team"])
            period = int(pass_row["period"])
            frame_id = int(pass_row["frame_id"])
            ball_x_sb = float(pass_row["x_sb"])
            ball_y_sb = float(pass_row["y_sb"])
            timestamp_seconds = float(pass_row["timestamp_seconds"])
            confidence = float(pass_row["alignment_confidence"])

            # Look up tracking data at the aligned frame
            try:
                frame_players = tracking_grouped.get_group((match_id, period, frame_id))
            except KeyError:
                skipped_no_tracking += 1
                continue

            # Need at least some home and away players for pitch control
            if frame_players["team"].nunique() < 2:
                skipped_insufficient_players += 1
                continue

            # Build player DataFrame for ghost trajectories
            # Tracking data is already in StatsBomb 120x80
            players = pd.DataFrame(
                {
                    "player_id": frame_players["player_id"].values,
                    "team": frame_players["team"].values,
                    "x": np.asarray(frame_players["x"], dtype=np.float64),
                    "y": np.asarray(frame_players["y"], dtype=np.float64),
                    "velocity_x": np.asarray(frame_players["velocity_x"], dtype=np.float64),
                    "velocity_y": np.asarray(frame_players["velocity_y"], dtype=np.float64),
                }
            )

            # Generate ghost trajectories (constant-velocity extrapolation)
            ghost_frames = generate_ghost_trajectories(
                players,
                event_frame=frame_id,
                frame_rate=FRAME_RATE,
                window_before_s=WINDOW_BEFORE_S,
                window_after_s=WINDOW_AFTER_S,
            )

            # The event frame index within ghost_frames:
            # ghost_frames[0] is at offset = -WINDOW_BEFORE_S * FRAME_RATE
            # The event frame (offset=0) is at index = WINDOW_BEFORE_S * FRAME_RATE
            event_idx = int(WINDOW_BEFORE_S * FRAME_RATE)

            # Compute PPCF at the event frame
            grid_x, grid_y, ppcf = compute_pitch_control_grid_fast(
                ghost_frames[event_idx], grid_cells_x=GRID_NX, grid_cells_y=GRID_NY, params=params
            )

            # Pre-compute loop-invariant OBSO multiplier ONCE per pass event
            # (F-05 OPT-AUDIT-200). compute_obso_surface internally re-interpolates
            # grids and recomputes Gaussian distance weight on every call — all constant
            # across the ghost frame loop for a given ball position.
            ny_g, nx_g = GRID_NY, GRID_NX
            xx_g, yy_g = np.meshgrid(grid_x, grid_y)
            sigma_x, sigma_y = 30.0, 20.0
            distance_weight = np.exp(
                -((xx_g - ball_x_sb) ** 2) / (2.0 * sigma_x**2) - (yy_g - ball_y_sb) ** 2 / (2.0 * sigma_y**2)
            )
            trans_interp = interpolate_grid(transition_grid, (ny_g, nx_g))
            epv_interp = interpolate_grid(epv_grid, (ny_g, nx_g))
            eff_trans = trans_interp * distance_weight
            max_trans = np.max(eff_trans)
            if max_trans > 1e-10:
                eff_trans = eff_trans / max_trans
            obso_multiplier = eff_trans * epv_interp  # (ny, nx) — constant

            # OBSO at event frame via broadcast
            obso = np.clip(ppcf * obso_multiplier, 0.0, 1.0)

            # --- Actual OBSO at ball release position ---
            tx_idx = int(np.clip(ball_x_sb / 120.0 * (GRID_NX - 1), 0, GRID_NX - 1))
            ty_idx = int(np.clip(ball_y_sb / 80.0 * (GRID_NY - 1), 0, GRID_NY - 1))
            actual_obso = float(obso[ty_idx, tx_idx])

            # --- Peak OBSO: check ball position across ghost frames (every 5th) ---
            # Uses pre-computed obso_multiplier (F-05 OPT-AUDIT-200)
            peak_obso = actual_obso
            for frame_df in ghost_frames[::5]:
                _, _, frame_ppcf = compute_pitch_control_grid_fast(
                    frame_df, grid_cells_x=GRID_NX, grid_cells_y=GRID_NY, params=params
                )
                frame_obso = np.clip(frame_ppcf * obso_multiplier, 0.0, 1.0)
                frame_val = float(frame_obso[ty_idx, tx_idx])
                peak_obso = max(peak_obso, frame_val)

            # --- Optimal OBSO: max across all off-ball teammate positions ---
            event_frame_df = ghost_frames[event_idx]
            passer_team = team  # home or away
            teammates_at_event = event_frame_df[
                (event_frame_df["team"] == passer_team) & (event_frame_df["player_id"] != player_id)
            ]
            optimal_obso = actual_obso
            best_receiver_x = ball_x_sb
            best_receiver_y = ball_y_sb

            # Vectorized teammate OBSO lookup (F-06 OPT-AUDIT-200)
            if len(teammates_at_event) > 0:
                tm_xs = teammates_at_event["x"].to_numpy()
                tm_ys = teammates_at_event["y"].to_numpy()
                tm_x_idxs = np.clip((tm_xs / 120.0 * (GRID_NX - 1)).astype(int), 0, GRID_NX - 1)
                tm_y_idxs = np.clip((tm_ys / 80.0 * (GRID_NY - 1)).astype(int), 0, GRID_NY - 1)
                tm_obso_vals = obso[tm_y_idxs, tm_x_idxs]
                best_idx = int(np.argmax(tm_obso_vals))
                if tm_obso_vals[best_idx] > optimal_obso:
                    optimal_obso = float(tm_obso_vals[best_idx])
                    best_receiver_x = float(tm_xs[best_idx])
                    best_receiver_y = float(tm_ys[best_idx])

            temporal_judgment = actual_obso / max(peak_obso, 1e-10)
            spatial_selection = actual_obso / max(optimal_obso, 1e-10)

            all_results.append(
                {
                    "match_id": match_id,
                    "pass_id": f"{match_id}_{event_id}",
                    "event_id": event_id,
                    "player_id": player_id,
                    "team": team,
                    "period": period,
                    "timestamp_seconds": round(timestamp_seconds, 4),
                    "frame_id": frame_id,
                    "ball_x": round(ball_x_sb, 4),
                    "ball_y": round(ball_y_sb, 4),
                    "receiver_x": round(best_receiver_x, 4),
                    "receiver_y": round(best_receiver_y, 4),
                    "actual_obso": round(actual_obso, 6),
                    "peak_obso": round(peak_obso, 6),
                    "optimal_obso": round(optimal_obso, 6),
                    "temporal_judgment": round(temporal_judgment, 4),
                    "spatial_selection": round(spatial_selection, 4),
                    "alignment_confidence": round(confidence, 4),
                }
            )

            if pass_counter % 50 == 0:
                elapsed = time.time() - total_start
                print(
                    f"    [{pass_counter}/{n_passes_total}] "
                    f"elapsed={elapsed:.1f}s, "
                    f"actual={actual_obso:.4f}, peak={peak_obso:.4f}"
                )

    total_elapsed = time.time() - total_start
    n_passes = len(all_results)

    print(f"\n  Processed {n_passes} passes in {total_elapsed:.2f}s")
    print(f"  Skipped (no tracking frame): {skipped_no_tracking}")
    print(f"  Skipped (insufficient players): {skipped_insufficient_players}")

    if n_passes == 0:
        print("  WARNING: No passes were processed. Check data alignment.")
        return

    # ------------------------------------------------------------------
    # 4. Log to MLflow
    # ------------------------------------------------------------------
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        print("\n=== Logging to MLflow ===")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/obso")

        results_df = pd.DataFrame(all_results)
        with mlflow.start_run(run_name="obso_batch_computation"):
            mlflow.log_params(
                {
                    "grid_nx": GRID_NX,
                    "grid_ny": GRID_NY,
                    "window_before_s": WINDOW_BEFORE_S,
                    "window_after_s": WINDOW_AFTER_S,
                    "n_passes": n_passes,
                    "n_matches": len(match_ids),
                    "training_env": "hf_jobs_a10g",
                    "jax_available": _USE_JAX,
                }
            )
            mlflow.log_param("pitch_control_tracking_commit", _tracking_commit)
            mlflow.log_param("obso_pausa_inputs_commit", _inputs_commit)
            mlflow.log_metrics(
                {
                    "mean_actual_obso": float(results_df["actual_obso"].mean()),
                    "mean_peak_obso": float(results_df["peak_obso"].mean()),
                    "mean_optimal_obso": float(results_df["optimal_obso"].mean()),
                    "mean_temporal_judgment": float(results_df["temporal_judgment"].mean()),
                    "mean_spatial_selection": float(results_df["spatial_selection"].mean()),
                    "total_elapsed_seconds": total_elapsed,
                    "passes_per_second": n_passes / max(total_elapsed, 0.01),
                }
            )
        print("  Metrics logged to MLflow")
    else:
        print("\n=== MLflow skipped (MLFLOW_TRACKING_URI not set) ===")

    # ------------------------------------------------------------------
    # 5. Publish to HF Hub
    # ------------------------------------------------------------------
    print("\n=== Publishing results to HF Hub ===")
    results_df = pd.DataFrame(all_results)

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        # Save PAUSA raw scores
        results_df.to_parquet(str(data_dir / "pausa_raw_scores.parquet"), index=False)

        # Save metadata
        metadata: dict[str, object] = {
            "grid_nx": GRID_NX,
            "grid_ny": GRID_NY,
            "window_before_s": WINDOW_BEFORE_S,
            "window_after_s": WINDOW_AFTER_S,
            "frame_rate": FRAME_RATE,
            "n_passes": n_passes,
            "n_matches": len(match_ids),
            "match_ids": match_ids,
            "jax_used": _USE_JAX,
            "total_elapsed_seconds": round(total_elapsed, 2),
            "data_sources": {
                "tracking": TRACKING_DATASET,
                "events_and_sync": INPUTS_DATASET,
            },
        }
        metadata = recorder.complete(metadata, row_count=n_passes)
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

        # Dataset card
        card = f"""---
license: mit
tags:
  - soccer
  - football
  - obso
  - pausa
  - pitch-control
  - analytics
  - idsse
  - bundesliga
size_categories:
  - 1K-10K
---

# OBSO & PAUSA Value Surfaces

Off-Ball Scoring Opportunity (OBSO) surfaces and PAUSA raw scores computed via
JAX-accelerated pitch control on A10G GPU from **{n_passes:,} passes** across
**{len(match_ids)} IDSSE Bundesliga matches**.

## Method

- **OBSO**: PPCF x Transition(ball->cell) x EPV(cell) per Spearman (2018) and Fernandez & Bornn (2018)
- **PAUSA**: Temporal judgment x Spatial selection per Lee et al. (MIT Sloan 2026)
- **Ghost trajectories**: Constant-velocity extrapolation, {WINDOW_BEFORE_S}s before to {WINDOW_AFTER_S}s after event
- **Event-frame alignment**: ELASTIC algorithm (Kim et al. 2025) via `obso-pausa-inputs` dataset
- **Grid resolution**: {GRID_NX} x {GRID_NY} cells on StatsBomb 120x80 coordinate system

## Contents

- `data/pausa_raw_scores.parquet` -- Per-pass scalar metrics ({n_passes:,} rows)
- `metadata.json` -- Computation parameters, timing, and data provenance

## Data Fields

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | string | Match identifier (`idsse_J03...`) |
| `pass_id` | string | Composite key: `match_id` + `event_id` |
| `event_id` | string | DFL event identifier |
| `player_id` | string | Passer's DFL PersonId |
| `team` | string | Passer's team (`home` / `away`) |
| `period` | int | Match half (1 or 2) |
| `timestamp_seconds` | double | Event time from period start |
| `frame_id` | int | Aligned tracking frame (via ELASTIC) |
| `ball_x` | double | Ball x at release (StatsBomb 120-yard scale) |
| `ball_y` | double | Ball y at release (StatsBomb 80-yard scale) |
| `receiver_x` | double | Optimal receiver x position |
| `receiver_y` | double | Optimal receiver y position |
| `actual_obso` | double | OBSO at ball release moment and position |
| `peak_obso` | double | Max OBSO across ghost trajectory window |
| `optimal_obso` | double | Max OBSO across all off-ball teammates |
| `temporal_judgment` | double | actual / peak (timing quality, 0-1) |
| `spatial_selection` | double | actual / optimal (target quality, 0-1) |
| `alignment_confidence` | double | ELASTIC alignment confidence (0-1) |

## Input Data

- **Tracking**: [`luxury-lakehouse/pitch-control-tracking`](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking)
  (IDSSE partition)
- **Events + ELASTIC sync**: [`luxury-lakehouse/obso-pausa-inputs`](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs)

## References

- Spearman (2018). "Beyond Expected Goals." MIT Sloan.
- Fernandez & Bornn (2018). "Wide Open Spaces." MIT Sloan.
- Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa." MIT Sloan 2026.
- Kim et al. (2025). "ELASTIC." ECML-PKDD MLSA 2025. arXiv:2508.09238.
- Bassek et al. (2025). "An integrated dataset of spatiotemporal and event data
  in elite soccer." Scientific Data, Nature.

## License

MIT -- computed from IDSSE open data (CC-BY 4.0).
"""
        with open(str(Path(tmpdir) / "README.md"), "w", encoding="utf-8") as f:
            f.write(card)

        api.create_repo(OUTPUT_DATASET, repo_type="dataset", exist_ok=True, token=hf_token)
        # Upload individual files — upload_folder fails on xet storage backend
        # in HF Jobs. upload_file is reliable (proven by CostEstimateHook and
        # all training scripts in this repo).
        for local_path, repo_path in [
            (str(data_dir / "pausa_raw_scores.parquet"), "data/pausa_raw_scores.parquet"),
            (str(Path(tmpdir) / "metadata.json"), "metadata.json"),
            (str(Path(tmpdir) / "README.md"), "README.md"),
        ]:
            api.upload_file(
                path_or_fileobj=local_path,
                path_in_repo=repo_path,
                repo_id=OUTPUT_DATASET,
                repo_type="dataset",
                token=hf_token,
            )

    print(f"\n  Published: https://huggingface.co/datasets/{OUTPUT_DATASET}")
    print(f"  Passes processed: {n_passes}")
    print(f"  Matches: {len(match_ids)}")
    print("OBSO batch computation complete!")


if __name__ == "__main__":
    main()
