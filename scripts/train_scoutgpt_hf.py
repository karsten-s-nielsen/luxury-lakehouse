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
        --flavor a10g-large --timeout 120m \\
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
import torch.nn as nn
from torch.utils.data import DataLoader
from train_scoutgpt_hf_helpers import (
    BOS_TOKEN_ID,
    DEFAULT_BATCH_SIZE,
    DEFAULT_EPOCHS,
    DEFAULT_LR,
    DEFAULT_PATIENCE,
    WARMUP_FRACTION,
    WEIGHT_DECAY,
    ScoutGPTDataset,
    build_action_type_frequencies,
    build_datasets,
    compute_baselines,
    evaluate_counterfactual_ranking,
    get_cosine_schedule_with_warmup,
    load_training_data,
    stratified_split,
)

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder
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
# Training loop
# ---------------------------------------------------------------------------


def _train_loop(
    train_ds: ScoutGPTDataset,
    val_ds: ScoutGPTDataset,
    config: ScoutGPTConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[ScoutGPTDecoder, dict[str, list[float]]]:
    """Train ScoutGPT with autoregressive action loss + VAEP auxiliary loss.

    Returns:
        Tuple of (best model, training history dict).
    """
    model = ScoutGPTDecoder(config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    tl = DataLoader(train_ds, batch_size=batch_size, shuffle=True, num_workers=0, pin_memory=device.type == "cuda")
    vl = DataLoader(val_ds, batch_size=batch_size, shuffle=False, num_workers=0, pin_memory=device.type == "cuda")

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(tl) * epochs
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)

    action_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    vaep_criterion = nn.MSELoss(reduction="none")

    best_val = float("inf")
    patience_ctr = 0
    best_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {
        "train_loss": [],
        "train_action_loss": [],
        "train_vaep_loss": [],
        "val_loss": [],
        "val_action_loss": [],
        "val_vaep_loss": [],
        "val_top1_accuracy": [],
        "val_top5_accuracy": [],
    }

    for epoch in range(epochs):
        t0 = time.time()
        model.train()
        total_loss = 0.0
        total_action_loss = 0.0
        total_vaep_loss = 0.0
        nb = 0

        for batch in tl:
            optimizer.zero_grad()
            action_logits, vaep_preds = model.predict(
                action_ids=batch["action_ids"].to(device),
                start_x=batch["start_x"].to(device),
                start_y=batch["start_y"].to(device),
                end_x=batch["end_x"].to(device),
                end_y=batch["end_y"].to(device),
                result=batch["result"].to(device),
                time_delta=batch["time_delta"].to(device),
                player_ids=batch["player_ids"].to(device),
                attention_mask=batch["attention_mask"].to(device),
            )
            # Autoregressive cross-entropy over vocab_size classes
            action_loss = action_criterion(
                action_logits.view(-1, config.vocab_size),
                batch["labels"].to(device).view(-1),
            )

            # VAEP MSE on valid (non-BOS, non-PAD) positions
            # BOS is at position 0 (action_ids == BOS_TOKEN_ID); exclude it from VAEP loss
            valid_mask = (batch["action_ids"].to(device) != BOS_TOKEN_ID) & batch["attention_mask"].to(device)
            vaep_raw = vaep_criterion(vaep_preds.squeeze(-1), batch["vaep_targets"].to(device))
            valid_count = valid_mask.sum().clamp(min=1)
            vaep_loss = (vaep_raw * valid_mask.float()).sum() / valid_count

            loss = action_loss + config.vaep_loss_weight * vaep_loss
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            scheduler.step()

            total_loss += loss.item()
            total_action_loss += action_loss.item()
            total_vaep_loss += vaep_loss.item()
            nb += 1

        avg_loss = total_loss / max(nb, 1)
        avg_action = total_action_loss / max(nb, 1)
        avg_vaep = total_vaep_loss / max(nb, 1)

        v_loss, v_action, v_vaep, v_top1, v_top5 = _eval_loop(
            model, vl, config, device, action_criterion, vaep_criterion
        )

        history["train_loss"].append(avg_loss)
        history["train_action_loss"].append(avg_action)
        history["train_vaep_loss"].append(avg_vaep)
        history["val_loss"].append(v_loss)
        history["val_action_loss"].append(v_action)
        history["val_vaep_loss"].append(v_vaep)
        history["val_top1_accuracy"].append(v_top1)
        history["val_top5_accuracy"].append(v_top5)

        logger.info(
            "Epoch %d/%d — loss=%.4f val_loss=%.4f top1=%.4f top5=%.4f (%.1fs)",
            epoch + 1,
            epochs,
            avg_loss,
            v_loss,
            v_top1,
            v_top5,
            time.time() - t0,
        )

        if v_loss < best_val:
            best_val = v_loss
            patience_ctr = 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            patience_ctr += 1
            if patience_ctr >= patience:
                logger.info("Early stopping at epoch %d", epoch + 1)
                break

    if best_state:
        model.load_state_dict(best_state)
    return model, history


def _eval_loop(
    model: ScoutGPTDecoder,
    vl: DataLoader[dict[str, torch.Tensor]],
    config: ScoutGPTConfig,
    device: torch.device,
    action_criterion: nn.CrossEntropyLoss,
    vaep_criterion: nn.MSELoss,
) -> tuple[float, float, float, float, float]:
    """Evaluate model on a DataLoader.

    Returns:
        (combined_loss, action_loss, vaep_loss, top1_accuracy, top5_accuracy)
    """
    model.eval()
    total_loss = 0.0
    total_action = 0.0
    total_vaep = 0.0
    correct_top1 = 0
    correct_top5 = 0
    total_valid = 0
    nb = 0

    with torch.no_grad():
        for batch in vl:
            action_logits, vaep_preds = model.predict(
                action_ids=batch["action_ids"].to(device),
                start_x=batch["start_x"].to(device),
                start_y=batch["start_y"].to(device),
                end_x=batch["end_x"].to(device),
                end_y=batch["end_y"].to(device),
                result=batch["result"].to(device),
                time_delta=batch["time_delta"].to(device),
                player_ids=batch["player_ids"].to(device),
                attention_mask=batch["attention_mask"].to(device),
            )
            labels = batch["labels"].to(device)
            action_loss = action_criterion(action_logits.view(-1, config.vocab_size), labels.view(-1))

            valid_mask = (batch["action_ids"].to(device) != BOS_TOKEN_ID) & batch["attention_mask"].to(device)
            vaep_raw = vaep_criterion(vaep_preds.squeeze(-1), batch["vaep_targets"].to(device))
            valid_count = valid_mask.sum().clamp(min=1)
            vaep_loss = (vaep_raw * valid_mask.float()).sum() / valid_count

            total_action += action_loss.item()
            total_vaep += vaep_loss.item()
            total_loss += (action_loss + config.vaep_loss_weight * vaep_loss).item()
            nb += 1

            # Top-1 and top-5 accuracy on valid label positions
            label_mask = labels != -100
            if label_mask.any():
                valid_logits = action_logits[label_mask]  # (N_valid, vocab_size)
                valid_labels = labels[label_mask]  # (N_valid,)
                preds_top1 = valid_logits.argmax(dim=-1)
                correct_top1 += (preds_top1 == valid_labels).sum().item()
                top5 = valid_logits.topk(min(5, config.vocab_size), dim=-1).indices
                correct_top5 += (top5 == valid_labels.unsqueeze(-1)).any(dim=-1).sum().item()
                total_valid += valid_labels.size(0)

    n = max(nb, 1)
    nv = max(total_valid, 1)
    return (
        total_loss / n,
        total_action / n,
        total_vaep / n,
        correct_top1 / nv,
        correct_top5 / nv,
    )


# ---------------------------------------------------------------------------
# Bucket accuracy helper
# ---------------------------------------------------------------------------


def _accuracy_by_bucket(
    model: ScoutGPTDecoder,
    test_ds: ScoutGPTDataset,
    device: torch.device,
    batch_size: int,
) -> dict[str, float]:
    """Compute top-1 accuracy bucketed by episode length quartile.

    Quartile boundaries are computed over the test set itself.
    """
    model.eval()

    # Compute episode lengths (number of valid non-BOS positions)
    ep_lengths: list[int] = []
    for i in range(len(test_ds)):
        sample = test_ds[i]
        n_valid = int(((sample["action_ids"] != BOS_TOKEN_ID) & sample["attention_mask"]).sum().item())
        ep_lengths.append(n_valid)

    lengths_arr = np.array(ep_lengths, dtype=np.int64)
    q1 = int(np.percentile(lengths_arr, 25))
    q2 = int(np.percentile(lengths_arr, 50))
    q3 = int(np.percentile(lengths_arr, 75))

    bucket_correct: dict[str, int] = {"q1": 0, "q2": 0, "q3": 0, "q4": 0}
    bucket_total: dict[str, int] = {"q1": 0, "q2": 0, "q3": 0, "q4": 0}

    loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=0)
    sample_idx = 0
    with torch.no_grad():
        for batch in loader:
            bs = batch["action_ids"].size(0)
            action_logits, _ = model.predict(
                action_ids=batch["action_ids"].to(device),
                start_x=batch["start_x"].to(device),
                start_y=batch["start_y"].to(device),
                end_x=batch["end_x"].to(device),
                end_y=batch["end_y"].to(device),
                result=batch["result"].to(device),
                time_delta=batch["time_delta"].to(device),
                player_ids=batch["player_ids"].to(device),
                attention_mask=batch["attention_mask"].to(device),
            )
            labels = batch["labels"].to(device)

            for b in range(bs):
                ep_len = ep_lengths[sample_idx]
                if ep_len <= q1:
                    bucket = "q1"
                elif ep_len <= q2:
                    bucket = "q2"
                elif ep_len <= q3:
                    bucket = "q3"
                else:
                    bucket = "q4"
                sample_idx += 1

                lbl = labels[b]
                valid_mask = lbl != -100
                if not valid_mask.any():
                    continue
                preds = action_logits[b].argmax(dim=-1)
                bucket_correct[bucket] += int((preds[valid_mask] == lbl[valid_mask]).sum().item())
                bucket_total[bucket] += int(valid_mask.sum().item())

    return {f"test_top1_accuracy_{bkt}": bucket_correct[bkt] / max(bucket_total[bkt], 1) for bkt in bucket_correct}


# ---------------------------------------------------------------------------
# Evaluation and reporting
# ---------------------------------------------------------------------------


def _evaluate_and_report(
    model: ScoutGPTDecoder,
    test_ds: ScoutGPTDataset,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    device: torch.device,
    history: dict[str, list[float]],
    config: ScoutGPTConfig,
    batch_size: int,
) -> dict[str, Any]:
    """Compute full evaluation suite and return metrics dict.

    Includes:
    - test accuracy (top-1, top-5, by bucket)
    - baselines (most-frequent, bigram)
    - counterfactual ranking (Spearman rho)
    - cross-source accuracy gap
    """
    action_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    vaep_criterion = nn.MSELoss(reduction="none")
    test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=0)

    test_loss, test_action_loss, test_vaep_loss, test_top1, test_top5 = _eval_loop(
        model, test_loader, config, device, action_criterion, vaep_criterion
    )
    logger.info(
        "Test — loss=%.4f top1=%.4f top5=%.4f",
        test_loss,
        test_top1,
        test_top5,
    )

    # Per-bucket accuracy
    bucket_metrics = _accuracy_by_bucket(model, test_ds, device, batch_size)
    logger.info("Bucket accuracies: %s", {k: f"{v:.4f}" for k, v in bucket_metrics.items()})

    # Baselines
    baselines = compute_baselines(test_ds, train_data)
    logger.info(
        "Baselines — most_frequent=%.4f bigram=%.4f",
        baselines["baseline_most_frequent_accuracy"],
        baselines["baseline_bigram_accuracy"],
    )

    # Counterfactual ranking
    action_type_frequencies = build_action_type_frequencies(train_data)
    cf_metrics = evaluate_counterfactual_ranking(
        model,
        test_ds,
        device,
        action_type_frequencies=action_type_frequencies,
    )
    logger.info(
        "Counterfactual ranking — mean_rho=%.4f n=%d std=%.4f",
        cf_metrics["mean_spearman_rho"],
        cf_metrics["n_episodes_evaluated"],
        cf_metrics["rho_std"],
    )

    # Cross-source accuracy gap
    cross_source = _cross_source_accuracy(model, test_data, config, device, batch_size)
    if cross_source:
        source_accs = list(cross_source.values())
        cross_source_gap = max(source_accs) - min(source_accs)
        logger.info("Cross-source accuracy gap: %.4f (%s)", cross_source_gap, cross_source)
    else:
        cross_source_gap = 0.0

    metrics: dict[str, Any] = {
        "actual_epochs": len(history["train_loss"]),
        "test_loss": test_loss,
        "test_action_loss": test_action_loss,
        "test_vaep_loss": test_vaep_loss,
        "test_top1_accuracy": test_top1,
        "test_top5_accuracy": test_top5,
        **bucket_metrics,
        **baselines,
        **cf_metrics,
        "cross_source_gap": cross_source_gap,
        **{f"cross_source_accuracy_{src}": acc for src, acc in cross_source.items()},
    }
    return metrics


def _cross_source_accuracy(
    model: ScoutGPTDecoder,
    test_data: pd.DataFrame,
    config: ScoutGPTConfig,
    device: torch.device,
    batch_size: int,
) -> dict[str, float]:
    """Per-data-source top-1 accuracy on test set.

    For each unique ``data_source`` in *test_data*, build a separate
    ScoutGPTDataset from those rows and compute top-1 accuracy.

    Returns:
        {data_source: top1_accuracy}
    """
    if "data_source" not in test_data.columns:
        logger.warning("data_source column not found — skipping cross-source evaluation")
        return {}

    action_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    vaep_criterion = nn.MSELoss(reduction="none")
    source_accuracies: dict[str, float] = {}

    for source in test_data["data_source"].unique():
        subset = test_data[test_data["data_source"] == source].reset_index(drop=True)
        if len(subset) == 0:
            continue

        parsed = build_datasets(subset)
        (atypes, sxs, sys_, exs, eys, res, vaeps, tds, pidxs, comp_ids) = parsed
        ds = ScoutGPTDataset(atypes, sxs, sys_, exs, eys, res, vaeps, tds, pidxs, competition_ids=comp_ids)
        loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=0)

        _, _, _, top1, _ = _eval_loop(model, loader, config, device, action_criterion, vaep_criterion)
        source_name = str(source).replace(" ", "_").lower()
        source_accuracies[source_name] = top1
        logger.info("  source=%s n=%d top1=%.4f", source, len(subset), top1)

    return source_accuracies


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
                "training_env": "hf_jobs_a10g_large",
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

        model, history = _train_loop(
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
        eval_metrics = _evaluate_and_report(
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
