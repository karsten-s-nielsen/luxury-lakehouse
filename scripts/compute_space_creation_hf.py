# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.3-py3-none-any.whl#sha256=290c1acc154f891339f938895b3fc9f6badd3647e34b95246e22d6795834a18e",
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
from compute_space_creation_hf_helpers import (
    CELL_AREA_M2,
    FRAME_SAMPLE_STEP,
    GRID_NX,
    GRID_NY,
    SB_LENGTH,
    SB_WIDTH,
    build_target_grid,
    get_grid_axes,
    load_trained_grids,
    sb_to_meters_x,
    sb_to_meters_y,
)

from analytics.obso import interpolate_grid
from analytics.pitch_control import PitchControlParams
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from workflows import workflow

HF_ORG = "luxury-lakehouse"
TRACKING_DATASET = f"{HF_ORG}/pitch-control-tracking"
OUTPUT_DATASET = f"{HF_ORG}/space-creation-values"

try:
    import jax
    import jax.numpy as jnp

    _USE_JAX = True
except ImportError:
    _USE_JAX = False


@workflow("wf-space-creation", phase="grid_computation")
def main() -> None:
    """Download data, compute per-player space creation, publish results."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

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
        print(f"  JAX devices: {[str(d) for d in jax.devices()]}", flush=True)
    print(f"  Grid: {GRID_NX}x{GRID_NY} ({GRID_NX * GRID_NY} cells), cell area: {CELL_AREA_M2:.2f} m^2", flush=True)

    # 1. Load data
    print("\n=== Loading data from HF Hub ===", flush=True)
    epv_grid, transition_grid = load_trained_grids(hf_token)

    all_repo_files = api.list_repo_files(TRACKING_DATASET, repo_type="dataset")
    idsse_parquet = [f for f in all_repo_files if "idsse" in f and f.endswith(".parquet")]
    print(f"  Found {len(idsse_parquet)} IDSSE parquet files", flush=True)
    if not idsse_parquet:
        print("  WARNING: No IDSSE tracking files found -- exiting gracefully.")
        return

    local_paths = [
        hf_hub_download(repo_id=TRACKING_DATASET, filename=f, repo_type="dataset", token=hf_token)
        for f in idsse_parquet
    ]
    _tracking_commit = api.repo_info(repo_id=TRACKING_DATASET, repo_type="dataset").sha

    # 2. Build target grid
    target_points = build_target_grid(params)
    grid_x, grid_y = get_grid_axes(params)

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
    dfs: list[pd.DataFrame] = []
    # Required-columns subset that must be present even on the fallback path.
    # Without this, a production schema migration that removes a column would
    # be silently accepted and cascade into downstream KeyError at compute time.
    _critical_cols = {"match_id", "frame_id", "player_id", "x", "y"}
    for fp in local_paths:
        try:
            dfs.append(pd.read_parquet(fp, columns=needed_cols))
        except ValueError as col_err:
            # pyarrow raises ValueError when a requested column isn't in the file.
            df = pd.read_parquet(fp)
            missing_critical = _critical_cols - set(df.columns)
            if missing_critical:
                msg = (
                    f"Parquet file {fp} is missing critical columns {sorted(missing_critical)} "
                    f"— cannot compute space creation. Check upstream schema. "
                    f"Original error: {col_err}"
                )
                raise RuntimeError(msg) from col_err
            dfs.append(df[[c for c in needed_cols if c in df.columns]])
    tracking_df = pd.concat(dfs, ignore_index=True)
    del dfs

    match_groups: dict[str, pd.DataFrame] = dict(iter(tracking_df.groupby("match_id")))
    match_ids = sorted(match_groups.keys())
    print(f"  {len(match_ids)} matches, {len(tracking_df):,} rows", flush=True)
    del tracking_df

    # 3. Process frames
    print("\n=== Processing frames ===", flush=True)
    all_results: list[dict[str, object]] = []
    total_start = time.time()
    total_processed = 0
    total_skipped = 0
    max_p = 30
    n_targets = len(target_points)

    targets_m = np.column_stack(
        [sb_to_meters_x(target_points[:, 0], params), sb_to_meters_y(target_points[:, 1], params)]
    )

    # Define JAX computation functions
    @jax.jit  # type: ignore[misc]
    def _pc_single(mask, xy, vel, home_mask, targets, rt, ma, sigma):
        large_tti = 1e6
        disp = targets[None, :, :] - xy[:, None, :]
        dist = jnp.sqrt(jnp.sum(disp**2, axis=-1))
        direction = disp / jnp.maximum(dist[:, :, None], 1e-10)
        v_proj = jnp.sum(vel[:, None, :] * direction, axis=-1)
        disc = v_proj**2 + 2.0 * ma * dist
        tti = rt + (-v_proj + jnp.sqrt(jnp.maximum(disc, 0.0))) / ma
        tti = jnp.maximum(tti, rt)
        tti = jnp.where(mask[:, None] > 0.5, tti, large_tti)
        h_tti = jnp.where(home_mask[:, None], tti, large_tti)
        a_tti = jnp.where(~home_mask[:, None], tti, large_tti)
        k = jnp.pi / jnp.sqrt(3.0) / sigma
        h_ind = 1.0 / (1.0 + jnp.exp(jnp.clip(-k * (jnp.min(a_tti, axis=0)[None, :] - h_tti), -50.0, 50.0)))
        h_inf = jnp.sum(h_ind * home_mask[:, None], axis=0)
        a_ind = 1.0 / (1.0 + jnp.exp(jnp.clip(-k * (jnp.min(h_tti, axis=0)[None, :] - a_tti), -50.0, 50.0)))
        a_inf = jnp.sum(a_ind * (~home_mask[:, None]).astype(jnp.float32), axis=0)
        total = h_inf + a_inf
        safe = jnp.where(total > 1e-10, total, 1.0)
        return jnp.clip(jnp.where(total > 1e-10, h_inf / safe, 0.5), 0.0, 1.0)

    _pc_variants = jax.vmap(_pc_single, in_axes=(0, None, None, None, None, None, None, None))
    _pc_batch = jax.jit(jax.vmap(_pc_variants, in_axes=(0, 0, 0, 0, None, None, None, None)))

    jnp_targets = jnp.array(targets_m)

    for mi, match_id in enumerate(match_ids):
        match_start = time.time()
        match_tracking = match_groups[match_id]
        frame_groups: dict[int, pd.DataFrame] = dict(iter(match_tracking.groupby("frame")))
        sampled = sorted(frame_groups.keys())[::FRAME_SAMPLE_STEP]
        print(f"\n  Match {mi + 1}/{len(match_ids)}: {match_id} ({len(sampled)} sampled frames)", flush=True)

        transition_interp = interpolate_grid(transition_grid, (GRID_NY, GRID_NX))
        epv_interp = interpolate_grid(epv_grid, (GRID_NY, GRID_NX))

        # Collect valid frames into batched arrays
        valid_frames: list[int] = []
        valid_periods: list[int] = []
        valid_ball: list[tuple[float, float]] = []
        valid_pids: list[list[str]] = []
        valid_teams: list[list[str]] = []
        valid_nreal: list[int] = []
        b_xy: list[np.ndarray] = []
        b_vel: list[np.ndarray] = []
        b_home: list[np.ndarray] = []
        b_pres: list[np.ndarray] = []

        for fn in sampled:
            fd = frame_groups.get(fn)
            if fd is None or fd.empty:
                total_skipped += 1
                continue
            if "player_id" in fd.columns:
                fd = fd.drop_duplicates(subset=["player_id"], keep="first")
            bx = float(fd["ball_x"].iloc[0]) if "ball_x" in fd.columns else SB_LENGTH / 2.0
            by = float(fd["ball_y"].iloc[0]) if "ball_y" in fd.columns else SB_WIDTH / 2.0
            if np.isnan(bx) or np.isnan(by):
                total_skipped += 1
                continue
            xa = np.asarray(fd["x"], dtype=np.float64)
            ya = np.asarray(fd["y"], dtype=np.float64)
            vxa = np.asarray(fd["velocity_x"], dtype=np.float64)
            vya = np.asarray(fd["velocity_y"], dtype=np.float64)
            if np.any(np.isnan(xa)) or np.any(np.isnan(ya)):
                total_skipped += 1
                continue
            nr = min(len(fd), max_p)
            xy = np.zeros((max_p, 2), dtype=np.float64)
            vel = np.zeros((max_p, 2), dtype=np.float64)
            home = np.zeros(max_p, dtype=bool)
            pres = np.zeros(max_p, dtype=np.float64)
            xy[:nr, 0] = sb_to_meters_x(xa[:nr], params)
            xy[:nr, 1] = sb_to_meters_y(ya[:nr], params)
            vel[:nr, 0] = sb_to_meters_x(vxa[:nr], params)
            vel[:nr, 1] = sb_to_meters_y(vya[:nr], params)
            home[:nr] = fd["team"].values[:nr] == "home"
            pres[:nr] = 1.0

            valid_frames.append(fn)
            valid_periods.append(int(fd["period"].iloc[0]))
            valid_ball.append((bx, by))
            valid_pids.append([str(p) for p in fd["player_id"].values[:nr]])
            valid_teams.append([str(t) for t in fd["team"].values[:nr]])
            valid_nreal.append(nr)
            b_xy.append(xy)
            b_vel.append(vel)
            b_home.append(home)
            b_pres.append(pres)

        nv = len(valid_frames)
        if nv == 0:
            continue

        all_xy = np.stack(b_xy)
        all_vel = np.stack(b_vel)
        all_home = np.stack(b_home)
        all_pres = np.stack(b_pres)

        masks = np.ones((max_p + 1, max_p), dtype=np.float64)
        for i in range(max_p):
            masks[i + 1, i] = 0.0

        jnp_masks = jnp.array(masks)
        all_baseline = np.zeros((nv, n_targets), dtype=np.float64)
        all_removed = np.zeros((nv, max_p, n_targets), dtype=np.float64)
        chunk_size = 256

        for cs in range(0, nv, chunk_size):
            ce = min(cs + chunk_size, nv)
            c_pres = jnp.array(all_pres[cs:ce])
            c_masks = jnp_masks[None, :, :] * c_pres[:, None, :]
            result = np.asarray(
                _pc_batch(
                    c_masks,
                    jnp.array(all_xy[cs:ce]),
                    jnp.array(all_vel[cs:ce]),
                    jnp.array(all_home[cs:ce]),
                    jnp_targets,
                    params.reaction_time,
                    params.max_acceleration,
                    params.sigma,
                )
            )
            all_baseline[cs:ce] = result[:, 0, :]
            all_removed[cs:ce] = result[:, 1:, :]
            print(f"    GPU chunk [{ce}/{nv}] {time.time() - match_start:.1f}s", flush=True)

        # Vectorized OBSO + space creation per frame
        match_results: list[dict[str, object]] = []
        for fi in range(nv):
            nr = valid_nreal[fi]
            bx, by = valid_ball[fi]
            bg = all_baseline[fi].reshape(GRID_NY, GRID_NX)
            rg = all_removed[fi, :nr].reshape(nr, GRID_NY, GRID_NX)

            sigma_x, sigma_y = 30.0, 20.0
            xx, yy = np.meshgrid(grid_x, grid_y)
            dw = np.exp(-((xx - bx) ** 2) / (2 * sigma_x**2) - (yy - by) ** 2 / (2 * sigma_y**2))
            eff = transition_interp * dw
            mx = np.max(eff)
            if mx > 1e-10:
                eff /= mx
            mult = eff * epv_interp

            apc = np.concatenate([bg[None], rg], axis=0)
            aobso = np.clip(apc * mult[None], 0.0, 1.0)
            ih = np.array([t == "home" for t in valid_teams[fi][:nr]])
            dh = aobso[0][None] - aobso[1:]
            delta = np.where(ih[:, None, None], dh, -dh)
            sc = np.sum(np.maximum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
            sd = np.sum(np.minimum(delta, 0.0), axis=(1, 2)) * CELL_AREA_M2
            ns = np.sum(delta, axis=(1, 2)) * CELL_AREA_M2

            for pi in range(nr):
                match_results.append(
                    {
                        "match_id": match_id,
                        "frame_id": int(valid_frames[fi]),
                        "period": valid_periods[fi],
                        "player_id": valid_pids[fi][pi],
                        "team": valid_teams[fi][pi],
                        "space_created_m2": round(float(sc[pi]), 4),
                        "space_destroyed_m2": round(float(sd[pi]), 4),
                        "net_space_m2": round(float(ns[pi]), 4),
                    }
                )
            total_processed += 1

        all_results.extend(match_results)
        print(f"  Match complete: {len(match_results)} rows, {time.time() - match_start:.1f}s", flush=True)

    total_elapsed = time.time() - total_start
    n_rows = len(all_results)
    print(f"\n=== Summary: {total_processed} frames, {n_rows:,} rows, {total_elapsed:.1f}s ===", flush=True)

    if n_rows == 0:
        print("  WARNING: No space creation values computed.")
        return

    # 4. Build results DataFrame
    results_df = pd.DataFrame(all_results)
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

    # 5. MLflow
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/space_creation")
        with mlflow.start_run(run_name="space_creation_batch"):
            mlflow.log_params(
                {
                    "grid_nx": GRID_NX,
                    "grid_ny": GRID_NY,
                    "frame_sample_step": FRAME_SAMPLE_STEP,
                    "n_matches": len(match_ids),
                    "n_frames_processed": total_processed,
                    "jax_available": _USE_JAX,
                    "tracking_dataset_commit": _tracking_commit,
                }
            )
            mlflow.log_metrics(
                {
                    "mean_space_created_m2": float(results_df["space_created_m2"].mean()),
                    "mean_net_space_m2": float(results_df["net_space_m2"].mean()),
                    "total_elapsed_seconds": total_elapsed,
                }
            )

    # 6. Publish
    print("\n=== Publishing to HF Hub ===", flush=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()
        results_df.to_parquet(str(data_dir / "space_creation.parquet"), index=False)
        metadata: dict[str, object] = {
            "grid_nx": GRID_NX,
            "grid_ny": GRID_NY,
            "cell_area_m2": round(CELL_AREA_M2, 4),
            "frame_sample_step": FRAME_SAMPLE_STEP,
            "n_frames_processed": total_processed,
            "n_player_frame_rows": n_rows,
            "n_matches": len(match_ids),
            "tracking_dataset_commit": _tracking_commit,
        }
        metadata = recorder.complete(metadata, row_count=n_rows)
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)
        card = f"""---
license: mit
tags: [soccer, football, space-creation, pitch-control, obso, analytics]
size_categories: [100K-1M]
---
# Space Creation Values
Per-player per-frame space creation metrics. **{n_rows:,} rows** across **{len(match_ids)} IDSSE matches**.

## References
- Fernandez & Bornn (2018). "Wide Open Spaces." MIT Sloan.
- Spearman (2018). "Beyond Expected Goals." MIT Sloan.
"""
        with open(str(Path(tmpdir) / "README.md"), "w", encoding="utf-8") as f:
            f.write(card)
        api.create_repo(OUTPUT_DATASET, repo_type="dataset", exist_ok=True, token=hf_token)
        api.upload_folder(folder_path=tmpdir, repo_id=OUTPUT_DATASET, repo_type="dataset", token=hf_token)
    print(f"\n  Published: https://huggingface.co/datasets/{OUTPUT_DATASET}")


if __name__ == "__main__":
    import sys
    import traceback

    print("=== Script starting ===", flush=True)
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
