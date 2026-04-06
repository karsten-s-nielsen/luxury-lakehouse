# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "scikit-learn>=1.3.0",
#     "scipy>=1.11.0",
# ]
# ///
"""Train ScoutGPT decoder (autoregressive + VAEP auxiliary loss) on HF Jobs A10G GPU.

Player-conditioned causal GPT over SPADL possession episodes. The focal player
conditioning token at position 0 enables counterfactual substitution evaluation.

References:
    Hong, S. et al. (2025). "ScoutGPT: A Player-Conditioned GPT for Soccer."
        arXiv:2512.17266.
    Decroos, T. et al. (2019). "Actions Speak Louder than Goals." KDD.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_scoutgpt_hf.py \\
        --flavor l40sx1 --timeout 120m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import time
from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd
import torch

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
from analytics.scoutgpt_training import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_EPOCHS,
    DEFAULT_LR,
    DEFAULT_PATIENCE,
    WEIGHT_DECAY,
    ScoutGPTDataset,
    build_datasets,
    evaluate_and_report,
    load_training_data,
    stratified_split,
    train_loop,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from shared.constants import mlflow_model_uri
from workflows import workflow

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/scoutgpt-training-data"
MODEL_REPO = f"{HF_ORG}/scoutgpt"

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "scoutgpt"


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def _save_checkpoint(
    model: ScoutGPTDecoder,
    config: ScoutGPTConfig,
    hf_token: str,
    metrics: dict[str, Any],
) -> None:
    """Save model weights, config, and metrics to HF Hub.

    Uploads:
    - ``stage1/model.safetensors``
    - ``stage1/config.json``
    - ``metrics.json``
    """
    from huggingface_hub import HfApi
    from safetensors.torch import save_file as _save

    api = HfApi(token=hf_token)
    api.create_repo(MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)

    with tempfile.TemporaryDirectory() as td:
        model_path = os.path.join(td, "model.safetensors")
        _save(model.state_dict(), model_path)

        config_dict = asdict(config)
        config_path = os.path.join(td, "config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_dict, f, indent=2)

        for name, path in [("model.safetensors", model_path), ("config.json", config_path)]:
            api.upload_file(
                path_or_fileobj=path,
                path_in_repo=f"stage1/{name}",
                repo_id=MODEL_REPO,
                repo_type="model",
                token=hf_token,
            )
        logger.info("Checkpoint uploaded to %s/stage1/", MODEL_REPO)

    api.upload_file(
        path_or_fileobj=json.dumps(metrics, indent=2, default=str).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=MODEL_REPO,
        repo_type="model",
        token=hf_token,
    )
    logger.info("metrics.json uploaded to %s", MODEL_REPO)


# ---------------------------------------------------------------------------
# MLflow logging
# ---------------------------------------------------------------------------


def _log_mlflow(
    config: ScoutGPTConfig,
    history: dict[str, list[float]],
    metrics: dict[str, Any],
    model: ScoutGPTDecoder,
    args: argparse.Namespace,
    dataset_commit: str,
    n_train: int,
    n_val: int,
    n_test: int,
) -> None:
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not uri:
        logger.info("MLFLOW_TRACKING_URI not set — skipping MLflow logging")
        return
    import mlflow

    fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("/soccer_analytics/scoutgpt")
    with mlflow.start_run(run_name="scoutgpt_stage1_hf_jobs"):
        mlflow.log_params(
            {
                "architecture": "causal_decoder_transformer",
                "vocab_size": config.vocab_size,
                "hidden_dim": config.hidden_dim,
                "num_layers": config.num_layers,
                "num_heads": config.num_heads,
                "dropout": config.dropout,
                "max_seq_len": config.max_seq_len,
                "num_players": config.num_players,
                "spatial_mlp_dim": config.spatial_mlp_dim,
                "vaep_loss_weight": config.vaep_loss_weight,
                "batch_size": args.batch_size,
                "max_epochs": args.epochs,
                "actual_epochs": len(history["train_loss"]),
                "learning_rate": args.lr,
                "weight_decay": WEIGHT_DECAY,
                "patience": args.patience,
                "n_train": n_train,
                "n_val": n_val,
                "n_test": n_test,
                "n_parameters": sum(p.numel() for p in model.parameters()),
                "training_env": "hf_jobs_l40s",
                "dataset_commit": dataset_commit,
            }
        )
        for name, val in metrics.items():
            if isinstance(val, (int, float)):
                mlflow.log_metric(name, val)
        for key, vals in history.items():
            for i, val in enumerate(vals):
                mlflow.log_metric(key, val, step=i)

        class _Wrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            def predict(self, context: Any, mi: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(mi))

        mlflow.pyfunc.log_model(
            python_model=_Wrapper(),
            artifact_path="scoutgpt_model",
            registered_model_name=fqn,
            input_example=pd.DataFrame({"x": [0.0]}),
        )
        run_id = mlflow.active_run().info.run_id

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{fqn}'")
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.set_registered_model_alias(name=fqn, alias="Champion", version=latest.version)
        logger.info("MLflow complete (version=%s, run=%s)", latest.version, run_id)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-scoutgpt", phase="training")
def main() -> None:
    """Train ScoutGPT: player-conditioned autoregressive decoder over SPADL episodes."""
    parser = argparse.ArgumentParser(description="Train ScoutGPT on HF Jobs A10G GPU")
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    args = parser.parse_args()

    from huggingface_hub import get_token

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN required")

    recorder = HFJobsCostRecorder(
        workflow_id="wf-scoutgpt",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    t0 = time.time()

    try:
        data, _player_id_map, dataset_commit = load_training_data(hf_token, TRAINING_DATASET)
        logger.info("Loaded %d episodes (commit=%s)", len(data), dataset_commit)

        parsed = build_datasets(data)
        (all_atypes, all_sxs, all_sys, all_exs, all_eys, all_res, all_vaeps, all_tds, all_pidxs, all_comp_ids) = parsed

        train_df, val_df, test_df = stratified_split(data)
        ti = train_df.index.tolist()
        vi = val_df.index.tolist()
        tei = test_df.index.tolist()
        logger.info("Split: train=%d val=%d test=%d", len(ti), len(vi), len(tei))

        config = ScoutGPTConfig()

        train_ds = ScoutGPTDataset(
            [all_atypes[i] for i in ti],
            [all_sxs[i] for i in ti],
            [all_sys[i] for i in ti],
            [all_exs[i] for i in ti],
            [all_eys[i] for i in ti],
            [all_res[i] for i in ti],
            [all_vaeps[i] for i in ti],
            [all_tds[i] for i in ti],
            [all_pidxs[i] for i in ti],
            competition_ids=[all_comp_ids[i] for i in ti],
        )
        val_ds = ScoutGPTDataset(
            [all_atypes[i] for i in vi],
            [all_sxs[i] for i in vi],
            [all_sys[i] for i in vi],
            [all_exs[i] for i in vi],
            [all_eys[i] for i in vi],
            [all_res[i] for i in vi],
            [all_vaeps[i] for i in vi],
            [all_tds[i] for i in vi],
            [all_pidxs[i] for i in vi],
            competition_ids=[all_comp_ids[i] for i in vi],
        )
        test_ds = ScoutGPTDataset(
            [all_atypes[i] for i in tei],
            [all_sxs[i] for i in tei],
            [all_sys[i] for i in tei],
            [all_exs[i] for i in tei],
            [all_eys[i] for i in tei],
            [all_res[i] for i in tei],
            [all_vaeps[i] for i in tei],
            [all_tds[i] for i in tei],
            [all_pidxs[i] for i in tei],
            competition_ids=[all_comp_ids[i] for i in tei],
        )

        model, history = train_loop(
            train_ds,
            val_ds,
            config,
            device,
            args.epochs,
            args.batch_size,
            args.lr,
            args.patience,
        )

        test_data = data.iloc[tei].reset_index(drop=True)
        train_data = data.iloc[ti].reset_index(drop=True)
        eval_metrics = evaluate_and_report(
            model,
            test_ds,
            train_data,
            test_data,
            device,
            history,
            config,
            args.batch_size,
        )

        metrics: dict[str, Any] = {
            "dataset_commit": dataset_commit,
            "n_train": len(ti),
            "n_val": len(vi),
            "n_test": len(tei),
            "config": asdict(config),
            **eval_metrics,
        }
        metrics = recorder.complete(metrics, row_count=len(data))
        _save_checkpoint(model, config, hf_token, metrics)
        _log_mlflow(
            config,
            history,
            eval_metrics,
            model,
            args,
            dataset_commit,
            len(ti),
            len(vi),
            len(tei),
        )

    except Exception as exc:
        recorder.fail(exc)
        raise

    logger.info("ScoutGPT training complete in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
