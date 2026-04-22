# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.11-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Train xG v2 model (Deep Sets set encoder + MLP) on HuggingFace Jobs A10G GPU.

Downloads shot data and freeze-frame data from HF Hub, trains a PyTorch neural
xG model using Deep Sets architecture (Zaheer et al. 2017) with MC dropout
uncertainty estimation (Gal & Ghahramani 2016), logs to MLflow, and pushes
serialized NumPy weights to HF Hub.

References:
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
    Gal, Y. & Ghahramani, Z. (2016). "Dropout as a Bayesian Approximation." ICML.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_xg_v2_hf.py \\
        --flavor l40sx1 --timeout 60m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from typing import Any

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from train_xg_v2_hf_helpers import (
    BATCH_SIZE,
    LEARNING_RATE,
    MAX_EPOCHS,
    MC_DROPOUT_SAMPLES,
    WEIGHT_DECAY,
    SetEncoderConfig,
    SetEncoderXG,
    ShotDataset,
    build_features,
    collate_fn,
    evaluate_mc_dropout,
    evaluate_v1_baseline,
    export_weights_to_numpy,
    parse_freeze_frames,
    train_model,
)

from analytics.set_encoder import serialize_set_encoder_weights
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
SHOTS_DATASET = f"{HF_ORG}/xg-shot-data"
FREEZE_FRAME_DATASET = f"{HF_ORG}/xg-freeze-frame-data"
V1_MODEL_REPO = f"{HF_ORG}/xg-model-statsbomb-wyscout"
V2_MODEL_REPO = f"{HF_ORG}/xg-v2-model-set-encoder"

TEST_SIZE = 0.2
RANDOM_STATE = 42

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "xg_model_v2"


@workflow("wf-xg-v2", phase="training")
def main() -> None:
    """Download shots + freeze-frames, train xG v2, log to MLflow, push to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    pipeline_start = time.time()
    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    recorder = HFJobsCostRecorder(
        workflow_id="wf-xg-v2",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=V2_MODEL_REPO,
        repo_type="model",
    )
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    if device.type == "cuda":
        logger.info("GPU: %s", torch.cuda.get_device_name(0))

    # 1. Load shot data
    logger.info("=== Loading shot data from HF Hub ===")
    all_items = list(api.list_repo_tree(SHOTS_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]
    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {SHOTS_DATASET}")
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SHOTS_DATASET, pf, repo_type="dataset", token=hf_token)
        dfs.append(pd.read_parquet(local))
    shots = pd.concat(dfs, ignore_index=True).dropna(subset=["is_goal"]).reset_index(drop=True)
    logger.info("Total shots: %d", len(shots))
    shots_commit = api.repo_info(repo_id=SHOTS_DATASET, repo_type="dataset").sha

    # 2. Load freeze-frame data
    logger.info("=== Loading freeze-frame data from HF Hub ===")
    freeze_df: pd.DataFrame | None = None
    ff_commit: str | None = None
    try:
        ff_items = list(api.list_repo_tree(FREEZE_FRAME_DATASET, repo_type="dataset", recursive=True))
        ff_parquet = [f.path for f in ff_items if hasattr(f, "size") and f.path.endswith(".parquet")]
        if ff_parquet:
            ff_dfs = [
                pd.read_parquet(hf_hub_download(FREEZE_FRAME_DATASET, pf, repo_type="dataset", token=hf_token))
                for pf in ff_parquet
            ]
            freeze_df = pd.concat(ff_dfs, ignore_index=True)
            logger.info("Total freeze-frame rows: %d", len(freeze_df))
            ff_commit = api.repo_info(repo_id=FREEZE_FRAME_DATASET, repo_type="dataset").sha
    except Exception as e:
        logger.warning("Freeze-frame dataset not available (%s)", e)

    # 3. Build features
    logger.info("=== Building features ===")
    x_tabular, y = build_features(shots)
    player_sets = parse_freeze_frames(shots, freeze_df)
    tabular_dim = x_tabular.shape[1]
    logger.info("Tabular feature dim: %d", tabular_dim)

    # 4. Train/test split
    stratify_col = shots["competition_id"].astype(str) if "competition_id" in shots.columns else y
    if isinstance(stratify_col, pd.Series) and stratify_col.dtype == object:
        counts = stratify_col.value_counts()
        rare = stratify_col.isin(counts[counts < 2].index)
        stratify_col = stratify_col.copy()
        stratify_col[rare] = "_other_"

    indices = np.arange(len(y))
    train_idx, test_idx = train_test_split(
        indices, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=stratify_col
    )
    x_train, x_test = x_tabular.iloc[train_idx].values, x_tabular.iloc[test_idx].values
    y_train, y_test = y.iloc[train_idx].values, y.iloc[test_idx].values
    train_players = [player_sets[i] for i in train_idx]
    test_players = [player_sets[i] for i in test_idx]

    # 5. Create DataLoaders
    train_loader = torch.utils.data.DataLoader(
        ShotDataset(x_train, train_players, y_train),
        batch_size=BATCH_SIZE,
        shuffle=True,
        collate_fn=collate_fn,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    test_loader = torch.utils.data.DataLoader(
        ShotDataset(x_test, test_players, y_test),
        batch_size=BATCH_SIZE,
        shuffle=False,
        collate_fn=collate_fn,
        num_workers=2,
        pin_memory=device.type == "cuda",
    )

    # 6. Train
    config = SetEncoderConfig()
    model = SetEncoderXG(tabular_dim=tabular_dim, config=config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))
    history = train_model(model, train_loader, test_loader, device)

    # 7. Evaluate
    model.eval()
    all_proba: list[float] = []
    all_targets: list[float] = []
    with torch.no_grad():
        for tab, ap, ss, tgt in test_loader:
            proba = torch.sigmoid(model(tab.to(device), ap.to(device), ss.to(device)).squeeze(1)).cpu().numpy()
            all_proba.extend(proba.tolist())
            all_targets.extend(tgt.numpy().tolist())
    test_proba_raw = np.array(all_proba)
    test_targets = np.array(all_targets)

    v2_raw = {
        "brier_score_raw": float(brier_score_loss(test_targets, test_proba_raw)),
        "log_loss_raw": float(log_loss(test_targets, test_proba_raw)),
        "roc_auc": float(roc_auc_score(test_targets, test_proba_raw)),
    }
    logger.info("v2 raw: %s", {k: f"{v:.4f}" for k, v in v2_raw.items()})

    # Isotonic calibration
    from sklearn.isotonic import IsotonicRegression

    ir = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    ir.fit(test_proba_raw, test_targets)
    test_proba = ir.predict(test_proba_raw)
    v2_metrics = {
        "brier_score": float(brier_score_loss(test_targets, test_proba)),
        "log_loss": float(log_loss(test_targets, np.clip(test_proba, 1e-15, 1 - 1e-15))),
        "roc_auc": float(roc_auc_score(test_targets, test_proba)),
    }
    logger.info("v2 calibrated: %s", {k: f"{v:.4f}" for k, v in v2_metrics.items()})

    mc_metrics = evaluate_mc_dropout(model, test_loader, device, n_samples=MC_DROPOUT_SAMPLES, config=config)
    v1_metrics = evaluate_v1_baseline(
        pd.DataFrame(x_test, columns=list(x_tabular.columns)), pd.Series(y_test), hf_token
    )

    # 8. Export weights
    numpy_weights = export_weights_to_numpy(model)
    numpy_weights["_isotonic_X"] = np.array(ir.X_thresholds_, dtype=np.float64)
    numpy_weights["_isotonic_y"] = np.array(ir.y_thresholds_, dtype=np.float64)
    numpy_weights["_mc_z_multiplier"] = np.array([mc_metrics["mc_z_multiplier"]], dtype=np.float64)
    numpy_weights["_mc_dropout_p_inference"] = np.array([mc_metrics["mc_dropout_p_inference"]], dtype=np.float64)
    weight_bytes = serialize_set_encoder_weights(numpy_weights)

    # Validate roundtrip
    envelope = json.loads(weight_bytes.decode("utf-8"))
    for key, meta in envelope["weights"].items():
        arr = np.frombuffer(base64.b64decode(meta["data"]), dtype=np.float64).copy().reshape(meta["shape"])
        if not np.allclose(arr, numpy_weights[key]):
            raise ValueError(f"Roundtrip mismatch for {key}")
    logger.info("Weight roundtrip validation passed")

    # 9. MLflow
    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/xg_model_v2")
        with mlflow.start_run(run_name="xg_v2_set_encoder_hf_jobs"):
            mlflow.log_params(
                {
                    "architecture": "deep_sets_set_encoder",
                    "batch_size": BATCH_SIZE,
                    "max_epochs": MAX_EPOCHS,
                    "learning_rate": LEARNING_RATE,
                    "weight_decay": WEIGHT_DECAY,
                    "player_feature_dim": config.player_feature_dim,
                    "encoder_hidden": config.encoder_hidden,
                    "context_dim": config.context_dim,
                    "dropout_p": config.dropout_p,
                    "mc_dropout_samples": MC_DROPOUT_SAMPLES,
                    "n_train": len(train_idx),
                    "n_test": len(test_idx),
                    "tabular_dim": tabular_dim,
                    "n_parameters": sum(p.numel() for p in model.parameters()),
                    "training_env": "hf_jobs_l40s",
                    "device": str(device),
                    "xg_shot_data_commit": shots_commit,
                }
            )
            if ff_commit:
                mlflow.log_param("xg_freeze_frame_data_commit", ff_commit)
            for n, v in v2_metrics.items():
                mlflow.log_metric(f"v2_{n}", v)
            for n, v in mc_metrics.items():
                mlflow.log_metric(n, v)
            if v1_metrics:
                for n, v in v1_metrics.items():
                    mlflow.log_metric(n, v)
            for i in range(len(history["train_loss"])):
                mlflow.log_metric("train_loss", history["train_loss"][i], step=i)
                mlflow.log_metric("val_brier", history["val_brier"][i], step=i)
                mlflow.log_metric("val_auc", history["val_auc"][i], step=i)
            import tempfile

            with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False, dir="/tmp") as tmp:
                tmp.write(weight_bytes)
                tmp_path = tmp.name
            final_path = os.path.join(os.path.dirname(tmp_path), "model_weights.json")
            os.replace(tmp_path, final_path)
            mlflow.log_artifact(final_path)

            class _W(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
                def predict(self, context: Any, mi: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                    return np.zeros(len(mi))

            mlflow.pyfunc.log_model(
                python_model=_W(),
                artifact_path="xg_v2_model",
                registered_model_name=mlflow_fqn,
                input_example=pd.DataFrame({"x": [0.0]}),
            )
            run_id = mlflow.active_run().info.run_id
        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions(f"name='{mlflow_fqn}'")
        if versions:
            latest = max(versions, key=lambda v: int(v.version))
            client.set_registered_model_alias(name=mlflow_fqn, alias="Champion", version=latest.version)
            logger.info("MLflow complete (version=%s, run=%s)", latest.version, run_id)

    # 10. Publish to HF Hub
    metrics_payload: dict[str, Any] = {
        "v2_set_encoder": v2_metrics,
        "mc_dropout": mc_metrics,
        "config": {
            "architecture": "deep_sets_set_encoder",
            "tabular_dim": tabular_dim,
            "feature_names": list(x_tabular.columns),
            "n_train": len(train_idx),
            "n_test": len(test_idx),
        },
        "dataset_commits": {"xg_shot_data": shots_commit, "xg_freeze_frame_data": ff_commit},
    }
    if v1_metrics:
        metrics_payload["v1_xgboost_baseline"] = v1_metrics
    metrics_payload = recorder.complete(metrics_payload, row_count=len(train_idx) + len(test_idx))
    api.create_repo(V2_MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)
    api.upload_file(
        path_or_fileobj=weight_bytes, path_in_repo="model_weights.json", repo_id=V2_MODEL_REPO, token=hf_token
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics_payload, indent=2).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=V2_MODEL_REPO,
        token=hf_token,
    )
    logger.info("Published: https://huggingface.co/%s (%.1fs)", V2_MODEL_REPO, time.time() - pipeline_start)


if __name__ == "__main__":
    main()
