# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.83-py3-none-any.whl",
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
from dataclasses import asdict
from pathlib import Path

import pandas as pd
from compute_epv_transition_hf_helpers import (
    RELEVANT_TYPES,
    OBSOGridParams,
    completion_matrix_to_dataframe,
    compute_ball_reachability_grid,
    compute_epv_grid,
    epv_grid_to_dataframe,
    normalize_attack_direction,
    reachability_grid_to_dataframe,
    validate_epv_grid,
    validate_reachability_grid,
)

from ingestion.hf_jobs_cost import HF_RATE_CPU_BASIC, HFJobsCostRecorder
from workflows import workflow

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
SPADL_DATASET = f"{HF_ORG}/spadl-vaep-action-values"
OUTPUT_DATASET = f"{HF_ORG}/obso-trained-grids"


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

    # 1. Load SPADL data
    logger.info("=== Loading SPADL actions from HF Hub ===")
    all_items = list(api.list_repo_tree(SPADL_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith("/data.parquet")]
    if not parquet_files:
        parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    logger.info("Downloading %d parquet files...", len(parquet_files))
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SPADL_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        if "data_source=" in pf:
            df["data_source"] = pf.split("data_source=")[1].split("/")[0]
        dfs.append(df)
        logger.info("  %s: %s rows", pf, f"{len(df):,}")

    all_actions = pd.concat(dfs, ignore_index=True)
    _dataset_commit = api.repo_info(repo_id=SPADL_DATASET, repo_type="dataset").sha

    if "action_value_id" in all_actions.columns:
        before = len(all_actions)
        all_actions = all_actions.drop_duplicates(subset=["action_value_id"])
        if len(all_actions) < before:
            logger.info("Deduplicated: %s -> %s rows", f"{before:,}", f"{len(all_actions):,}")

    logger.info("Total actions: %s", f"{len(all_actions):,}")
    all_actions = all_actions[all_actions["action_type"].isin(RELEVANT_TYPES)].copy()
    all_actions = all_actions.rename(columns={"action_type": "type_name", "action_result": "result_name"})
    logger.info("Relevant actions: %s", f"{len(all_actions):,}")
    if len(all_actions) < 1000:
        raise ValueError(f"Too few actions ({len(all_actions)})")

    # 1b. Normalize coordinate orientation
    logger.info("=== Normalizing coordinate orientation ===")
    all_actions = normalize_attack_direction(all_actions, params)

    # 2. Compute per-competition grids
    logger.info("=== Computing per-competition grids ===")
    competitions = sorted(all_actions["competition_id"].dropna().unique())
    logger.info("%d competitions found", len(competitions))

    all_reach_dfs: list[pd.DataFrame] = []
    all_epv_dfs: list[pd.DataFrame] = []
    all_comp_dfs: list[pd.DataFrame] = []

    for comp_id in competitions:
        comp_actions = all_actions[all_actions["competition_id"] == comp_id]
        if len(comp_actions) < 500:
            continue
        all_reach_dfs.append(
            reachability_grid_to_dataframe(compute_ball_reachability_grid(comp_actions, params), str(comp_id))
        )
        epv_grid = compute_epv_grid(comp_actions, params)
        all_epv_dfs.append(epv_grid_to_dataframe(epv_grid, str(comp_id)))
        all_comp_dfs.append(completion_matrix_to_dataframe(comp_actions, params, str(comp_id)))
        logger.info("Competition %s: %s events, EPV max=%.5f", comp_id, f"{len(comp_actions):,}", epv_grid.max())

    # 3. Global grids
    logger.info("=== Computing global grids ===")
    global_reach = compute_ball_reachability_grid(all_actions, params)
    validate_reachability_grid(global_reach, params)
    all_reach_dfs.append(reachability_grid_to_dataframe(global_reach, "global"))

    global_epv = compute_epv_grid(all_actions, params)
    validate_epv_grid(global_epv, params)
    all_epv_dfs.append(epv_grid_to_dataframe(global_epv, "global"))

    global_comp_df = completion_matrix_to_dataframe(all_actions, params, "global")
    all_comp_dfs.append(global_comp_df)

    logger.info("Global reachability: range=[%.4f, %.4f]", global_reach.min(), global_reach.max())
    logger.info("Global EPV: range=[%.5f, %.5f]", global_epv.min(), global_epv.max())

    # 3b. MLflow
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

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
                    "n_competitions": len(all_epv_dfs) - 1,
                    "total_actions": len(all_actions),
                    "spadl_vaep_action_values_commit": _dataset_commit,
                }
            )
            mlflow.log_metrics(
                {
                    "global_epv_max": float(global_epv.max()),
                    "global_epv_min": float(global_epv.min()),
                    "global_reach_max": float(global_reach.max()),
                    "global_reach_min": float(global_reach.min()),
                    "completion_matrix_nonzero": len(global_comp_df),
                }
            )
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                json.dump(
                    {
                        "epv": {"shape": list(global_epv.shape), "values": global_epv.tolist()},
                        "reachability": {"shape": list(global_reach.shape), "values": global_reach.tolist()},
                    },
                    f,
                )
                artifact_path = f.name
            mlflow.log_artifact(artifact_path, "obso_grids")
            os.unlink(artifact_path)

    # 4. Publish to HF Hub
    logger.info("=== Publishing to HF Hub ===")
    combined_reach = pd.concat(all_reach_dfs, ignore_index=True)
    combined_epv = pd.concat(all_epv_dfs, ignore_index=True)
    combined_comp = pd.concat(all_comp_dfs, ignore_index=True)
    n_comps = len(all_epv_dfs) - 1

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "data"
        data_dir.mkdir()

        # Global grids as separate files
        combined_reach[combined_reach["competition_id"] == "global"][["zone_y", "zone_x", "reachability"]].to_parquet(
            str(data_dir / "reachability_grid_global.parquet"), index=False
        )
        combined_epv[combined_epv["competition_id"] == "global"][["zone_y", "zone_x", "epv_value"]].to_parquet(
            str(data_dir / "epv_grid_global.parquet"), index=False
        )
        combined_comp[combined_comp["competition_id"] == "global"][
            ["origin_zone", "target_zone", "probability"]
        ].to_parquet(str(data_dir / "completion_matrix_global.parquet"), index=False)

        # All grids combined
        combined_reach.to_parquet(str(data_dir / "reachability_grids_all.parquet"), index=False)
        combined_epv.to_parquet(str(data_dir / "epv_grids_all.parquet"), index=False)
        combined_comp.to_parquet(str(data_dir / "completion_matrices_all.parquet"), index=False)

        # Metadata
        metadata: dict[str, object] = {
            "params": asdict(params),
            "competitions": [str(c) for c in competitions],
            "n_competitions_computed": n_comps,
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
                    "nonzero_entries": len(global_comp_df),
                },
            },
            "spadl_dataset_commit": _dataset_commit,
        }
        metadata = recorder.complete(metadata, row_count=len(combined_reach) + len(combined_epv))
        with open(str(Path(tmpdir) / "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)

        # Data and README are published via two separate calls:
        # (1) upload_folder for the data payload (parquet + metadata.json with
        #     per-run grid statistics — grid sizes, value ranges, competition
        #     count, total SPADL actions),
        # (2) upload_hf_readme for the static README from the in-repo source
        #     of truth at docs/huggingface/dataset-cards/obso-trained-grids.md.
        # The shared helper (PR 4c) replaced the prior inline-README pattern
        # that mixed schema documentation with run-specific stats.
        from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

        api.create_repo(OUTPUT_DATASET, repo_type="dataset", exist_ok=True, token=hf_token)
        api.upload_folder(folder_path=tmpdir, repo_id=OUTPUT_DATASET, repo_type="dataset", token=hf_token)
        readme_result = upload_hf_readme(
            repo_id=OUTPUT_DATASET,
            readme_path=get_hf_card_path("obso-trained-grids.md", kind="dataset"),
            hf_token=hf_token,
        )
        logger.info(
            "Uploaded README: %s (sha256=%s)",
            readme_result["commit_url"],
            readme_result["sha256"][:8],
        )

    logger.info("Published: https://huggingface.co/datasets/%s", OUTPUT_DATASET)
    logger.info("OBSO grid training complete!")


if __name__ == "__main__":
    main()
