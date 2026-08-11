# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse[spadl] @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.94-py3-none-any.whl",
#     "numpy>=1.26.0",
#     "pandas>=2.0.0",
#     "pyarrow>=14.0.0",
#     "torch>=2.1.0",
#     "safetensors>=0.4.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Train Football2Vec 360-enriched transformer on HF Jobs A10G GPU.

192d transformer + 16d Deep Sets context = 208d output embeddings.
Stage 1: MLM training with 360 freeze frame context.
Stage 2: Adversarial team debiasing via gradient reversal (Ganin et al. 2016).

References:
    Danesi (2025) Football2Vec, Ganin et al. (2016) Domain-Adversarial Training,
    Zaheer et al. (2017) Deep Sets, Decroos et al. (2019) SPADL/VAEP.
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

from analytics.football2vec_360 import Football2Vec360Config, Football2Vec360Encoder
from analytics.football2vec_transformer import TeamClassifierHead
from ingestion.artifact_deploy import require_mlflow_env, set_and_verify_mlflow_champion
from ingestion.football2vec_360_training import (
    ADVERSARIAL_LAMBDA_MAX,
    ADVERSARIAL_WARMUP_EPOCHS,
    MAX_PLAYERS,
    OUTPUT_DIM,
    VOCAB_SIZE,
    WARMUP_FRACTION,
    WEIGHT_DECAY,
    Football2Vec360Dataset,
    MLMHead,
    get_cosine_schedule_with_warmup,
    load_training_data_sql,
    mlm_forward_360,
    parse_actions,
    parse_freeze_frames,
    stratified_split,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from shared.constants import mlflow_model_uri
from workflows import workflow

# Validated HF Jobs flavor — single source of truth, asserted against
# scripts/sk3_mig_b_retrain.py:_FLAVOR_MAP at CI time. The trainer docstring
# example invocation has no `--flavor` token (per spec §1.3 / Q28); this
# constant is the FIRST canonical declaration of the flavor for f2v_360.
VALIDATED_HF_FLAVOR: str = "l40sx1"

# uv silent-downgrade footgun (CLAUDE.md): a top-level silly-kicks pin in PEP
# 723 deps silently overrides the wheel's transitive pin; explicit pins are an
# active footgun, not a safety net (verified empirically 2026-05-04).
_REQUIRED_SK_MIN: tuple[int, int, int] = (4, 43, 0)


def _assert_silly_kicks_min() -> None:
    import silly_kicks

    actual = tuple(int(p) for p in silly_kicks.__version__.split(".")[:3])
    if actual < _REQUIRED_SK_MIN:
        raise RuntimeError(
            f"silly-kicks {silly_kicks.__version__} < required "
            f"{'.'.join(str(p) for p in _REQUIRED_SK_MIN)} — refusing to train."
        )


logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

HF_ORG = "luxury-lakehouse"
INPUT_DATASET = f"{HF_ORG}/football2vec-360-training-data"
OUTPUT_MODEL = f"{HF_ORG}/football2vec-360"
OUTPUT_EMBEDDINGS = f"{HF_ORG}/football2vec-360-embeddings"
PRETRAINED_MODEL = f"{HF_ORG}/football2vec-v2"

DEFAULT_EPOCHS = 30
DEFAULT_BATCH_SIZE = 256
DEFAULT_LR = 1e-4
DEFAULT_PATIENCE = 5
# Fixed RNG seed for reproducible training (spec §6.8 — enables the §9.8 differential-recompute test).
_TORCH_SEED = 42

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "football2vec_360"


# ---------------------------------------------------------------------------
# Training loops
# ---------------------------------------------------------------------------


def _train_stage1_loop(
    train_dataset: Football2Vec360Dataset,
    val_dataset: Football2Vec360Dataset,
    config: Football2Vec360Config,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2Vec360Encoder, MLMHead, dict[str, list[float]]]:
    """Train the 360 encoder with masked language modeling."""
    model = Football2Vec360Encoder(config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    expanded_embed = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim).to(device)
    with torch.no_grad():
        expanded_embed.weight[:VOCAB_SIZE] = model.base_encoder.token_embedding.weight
    model.base_encoder.token_embedding = expanded_embed

    mlm_head = MLMHead(config.hidden_dim, config.vocab_size).to(device)
    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )

    all_params = list(model.parameters()) + list(mlm_head.parameters())
    optimizer = torch.optim.AdamW(all_params, lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(train_loader) * epochs
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)
    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    best_val_loss = float("inf")
    patience_counter = 0
    best_model_state: dict[str, Any] = {}
    best_mlm_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {"train_loss": [], "val_loss": [], "val_accuracy": []}

    for epoch in range(epochs):
        epoch_start = time.time()
        model.train()
        mlm_head.train()
        total_loss = 0.0
        n_batches = 0

        for batch in train_loader:
            aids = batch["action_ids"].to(device)
            xs = batch["x_coords"].to(device)
            ys = batch["y_coords"].to(device)
            amask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            ff = batch["freeze_frames"].to(device)

            optimizer.zero_grad()
            logits = mlm_forward_360(model, mlm_head, aids, xs, ys, amask, ff)
            loss = criterion(logits.view(-1, config.vocab_size), labels.view(-1))
            loss.backward()
            torch.nn.utils.clip_grad_norm_(all_params, max_norm=1.0)
            optimizer.step()
            scheduler.step()
            total_loss += loss.item()
            n_batches += 1

        avg_train_loss = total_loss / max(n_batches, 1)
        val_loss, val_accuracy = _evaluate_mlm(model, mlm_head, val_loader, criterion, config, device)
        history["train_loss"].append(avg_train_loss)
        history["val_loss"].append(val_loss)
        history["val_accuracy"].append(val_accuracy)

        elapsed = time.time() - epoch_start
        logger.info(
            "Epoch %d/%d — loss=%.4f  val_loss=%.4f  val_acc=%.4f  lr=%.2e  (%.1fs)",
            epoch + 1,
            epochs,
            avg_train_loss,
            val_loss,
            val_accuracy,
            scheduler.get_last_lr()[0],
            elapsed,
        )

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            best_model_state = {k: v.clone() for k, v in model.state_dict().items()}
            best_mlm_state = {k: v.clone() for k, v in mlm_head.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= patience:
                logger.info("Early stopping at epoch %d (patience=%d)", epoch + 1, patience)
                break

    if best_model_state:
        model.load_state_dict(best_model_state)
        mlm_head.load_state_dict(best_mlm_state)
        logger.info("Restored best model weights (val_loss=%.4f)", best_val_loss)
    return model, mlm_head, history


def _evaluate_mlm(
    model: Football2Vec360Encoder,
    mlm_head: MLMHead,
    val_loader: DataLoader[dict[str, torch.Tensor]],
    criterion: nn.CrossEntropyLoss,
    config: Football2Vec360Config,
    device: torch.device,
) -> tuple[float, float]:
    """Evaluate MLM loss and accuracy on masked tokens."""
    model.eval()
    mlm_head.eval()
    total_loss = 0.0
    total_correct = 0
    total_masked = 0
    n_batches = 0
    with torch.no_grad():
        for batch in val_loader:
            aids = batch["action_ids"].to(device)
            xs = batch["x_coords"].to(device)
            ys = batch["y_coords"].to(device)
            amask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            ff = batch["freeze_frames"].to(device)
            logits = mlm_forward_360(model, mlm_head, aids, xs, ys, amask, ff)
            loss = criterion(logits.view(-1, config.vocab_size), labels.view(-1))
            total_loss += loss.item()
            n_batches += 1
            mask = labels != -100
            if mask.any():
                predicted = logits.argmax(dim=-1)
                total_correct += (predicted[mask] == labels[mask]).sum().item()
                total_masked += mask.sum().item()
    return total_loss / max(n_batches, 1), total_correct / max(total_masked, 1)


def _train_stage2_loop(
    model: Football2Vec360Encoder,
    train_dataset: Football2Vec360Dataset,
    val_dataset: Football2Vec360Dataset,
    num_competitions: int,
    config: Football2Vec360Config,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2Vec360Encoder, TeamClassifierHead, dict[str, list[float]]]:
    """Fine-tune with adversarial competition debiasing."""
    model = model.to(device)
    mlm_head = MLMHead(config.hidden_dim, config.vocab_size).to(device)
    adversary = TeamClassifierHead(
        hidden_dim=config.hidden_dim + config.context_dim, num_teams=num_competitions, lambda_val=0.0
    ).to(device)

    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )

    all_params = list(model.parameters()) + list(mlm_head.parameters()) + list(adversary.parameters())
    optimizer = torch.optim.AdamW(all_params, lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(train_loader) * epochs
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)
    mlm_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    adv_criterion = nn.CrossEntropyLoss()

    best_combined_loss = float("inf")
    patience_counter = 0
    best_encoder_state: dict[str, Any] = {}
    best_adversary_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {
        "train_mlm_loss": [],
        "train_adv_loss": [],
        "train_combined_loss": [],
        "val_mlm_loss": [],
        "val_adv_accuracy": [],
        "val_combined_loss": [],
        "lambda_val": [],
    }

    for epoch in range(epochs):
        epoch_start = time.time()
        current_lambda = ADVERSARIAL_LAMBDA_MAX * min(epoch / ADVERSARIAL_WARMUP_EPOCHS, 1.0)
        adversary.grl.lambda_val = current_lambda

        model.train()
        mlm_head.train()
        adversary.train()
        total_mlm = 0.0
        total_adv = 0.0
        n_batches = 0

        for batch in train_loader:
            aids = batch["action_ids"].to(device)
            xs = batch["x_coords"].to(device)
            ys = batch["y_coords"].to(device)
            amask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            ff = batch["freeze_frames"].to(device)
            comp_ids = batch["competition_id"].to(device)

            optimizer.zero_grad()
            mlm_logits = mlm_forward_360(model, mlm_head, aids, xs, ys, amask, ff)
            mlm_loss = mlm_criterion(mlm_logits.view(-1, config.vocab_size), labels.view(-1))
            embeddings = model(aids, xs, ys, amask, context_360=ff)
            adv_loss = adv_criterion(adversary(embeddings), comp_ids)
            (mlm_loss + current_lambda * adv_loss).backward()
            torch.nn.utils.clip_grad_norm_(all_params, max_norm=1.0)
            optimizer.step()
            scheduler.step()
            total_mlm += mlm_loss.item()
            total_adv += adv_loss.item()
            n_batches += 1

        avg_mlm = total_mlm / max(n_batches, 1)
        avg_adv = total_adv / max(n_batches, 1)
        val_mlm, val_adv_acc = _evaluate_stage2(model, mlm_head, adversary, val_loader, mlm_criterion, config, device)
        val_combined = val_mlm + current_lambda * avg_adv

        history["train_mlm_loss"].append(avg_mlm)
        history["train_adv_loss"].append(avg_adv)
        history["train_combined_loss"].append(avg_mlm + current_lambda * avg_adv)
        history["val_mlm_loss"].append(val_mlm)
        history["val_adv_accuracy"].append(val_adv_acc)
        history["val_combined_loss"].append(val_combined)
        history["lambda_val"].append(current_lambda)

        elapsed = time.time() - epoch_start
        logger.info(
            "Epoch %d/%d — mlm=%.4f  adv=%.4f  val_mlm=%.4f  val_adv_acc=%.4f  lambda=%.3f  (%.1fs)",
            epoch + 1,
            epochs,
            avg_mlm,
            avg_adv,
            val_mlm,
            val_adv_acc,
            current_lambda,
            elapsed,
        )

        if val_combined < best_combined_loss:
            best_combined_loss = val_combined
            patience_counter = 0
            best_encoder_state = {k: v.clone() for k, v in model.state_dict().items()}
            best_adversary_state = {k: v.clone() for k, v in adversary.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= patience:
                logger.info("Early stopping at epoch %d (patience=%d)", epoch + 1, patience)
                break

    if best_encoder_state:
        model.load_state_dict(best_encoder_state)
        adversary.load_state_dict(best_adversary_state)
    return model, adversary, history


def _evaluate_stage2(
    model: Football2Vec360Encoder,
    mlm_head: MLMHead,
    adversary: TeamClassifierHead,
    val_loader: DataLoader[dict[str, torch.Tensor]],
    mlm_criterion: nn.CrossEntropyLoss,
    config: Football2Vec360Config,
    device: torch.device,
) -> tuple[float, float]:
    """Evaluate Stage 2: MLM loss + adversary accuracy."""
    model.eval()
    mlm_head.eval()
    adversary.eval()
    total_mlm = 0.0
    total_correct = 0
    total_samples = 0
    n_batches = 0
    with torch.no_grad():
        for batch in val_loader:
            aids = batch["action_ids"].to(device)
            xs = batch["x_coords"].to(device)
            ys = batch["y_coords"].to(device)
            amask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            ff = batch["freeze_frames"].to(device)
            comp_ids = batch["competition_id"].to(device)
            logits = mlm_forward_360(model, mlm_head, aids, xs, ys, amask, ff)
            total_mlm += mlm_criterion(logits.view(-1, config.vocab_size), labels.view(-1)).item()
            emb = model(aids, xs, ys, amask, context_360=ff)
            total_correct += (adversary(emb).argmax(dim=-1) == comp_ids).sum().item()
            total_samples += comp_ids.size(0)
            n_batches += 1
    return total_mlm / max(n_batches, 1), total_correct / max(total_samples, 1)


# ---------------------------------------------------------------------------
# Embedding inference + Model I/O + MLflow (pipeline-specific)
# ---------------------------------------------------------------------------


def _generate_embeddings(
    model: Football2Vec360Encoder,
    data: pd.DataFrame,
    action_ids_all: list[list[int]],
    x_coords_all: list[list[float]],
    y_coords_all: list[list[float]],
    freeze_frames_all: list[list[list[list[float]]]] | None,
    device: torch.device,
    batch_size: int = 512,
) -> pd.DataFrame:
    """Run inference on all data to produce 208d embeddings."""
    model.eval()
    ds = Football2Vec360Dataset(action_ids_all, x_coords_all, y_coords_all, freeze_frames=freeze_frames_all, mlm=False)
    loader = DataLoader(
        ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    all_emb: list[np.ndarray] = []
    with torch.no_grad():
        for batch in loader:
            emb = model(
                batch["action_ids"].to(device),
                batch["x_coords"].to(device),
                batch["y_coords"].to(device),
                batch["attention_mask"].to(device),
                context_360=batch["freeze_frames"].to(device),
            )
            all_emb.append(emb.cpu().numpy())
    arr = np.concatenate(all_emb, axis=0)
    logger.info("Generated embeddings shape: %s", arr.shape)
    return pd.DataFrame(
        {
            "canonical_player_id": data["canonical_player_id"].values,
            "match_id": data["match_id"].values,
            "behavioral_vector": [arr[i].tolist() for i in range(len(arr))],
        }
    )


def _try_load_pretrained(model: Football2Vec360Encoder, device: torch.device, hf_token: str) -> Football2Vec360Encoder:
    """Optionally warm-start transformer weights from football2vec-v2 Stage 2."""
    try:
        from huggingface_hub import hf_hub_download
        from safetensors.torch import load_file as _load_safetensors

        logger.info("Attempting to warm-start from %s/stage2/model.safetensors ...", PRETRAINED_MODEL)
        local = hf_hub_download(PRETRAINED_MODEL, "stage2/model.safetensors", repo_type="model", token=hf_token)
        state = _load_safetensors(local, device=str(device))
        current = model.state_dict()
        loaded = 0
        for k, v in state.items():
            rk = k.replace("encoder.", "transformer.", 1) if k.startswith("encoder.") else k
            if rk in current and current[rk].shape == v.shape:
                current[rk] = v
                loaded += 1
        model.load_state_dict(current)
        logger.info("Warm-started %d/%d parameters from %s", loaded, len(state), PRETRAINED_MODEL)
    except Exception as exc:
        logger.warning("Pretrained weight loading skipped: %s", exc)
    return model


def _save_checkpoint(
    model: Football2Vec360Encoder,
    config: Football2Vec360Config,
    stage: str,
    hf_token: str,
    metrics: dict[str, Any] | None = None,
) -> None:
    """Save 360 model checkpoint to HF Hub in safetensors format."""
    from huggingface_hub import HfApi
    from safetensors.torch import save_file as _save_safetensors

    api = HfApi(token=hf_token)
    api.create_repo(OUTPUT_MODEL, exist_ok=True, repo_type="model", token=hf_token)
    with tempfile.TemporaryDirectory() as tmpdir:
        sp = os.path.join(tmpdir, "model.safetensors")
        _save_safetensors(model.state_dict(), sp)
        cd = asdict(config)
        cd.update(
            {
                "_expanded_vocab_size": VOCAB_SIZE + 2,
                "_mask_token_id": VOCAB_SIZE,
                "_pad_token_id": VOCAB_SIZE + 1,
                "_output_dim": OUTPUT_DIM,
            }
        )
        cp = os.path.join(tmpdir, "config.json")
        with open(cp, "w", encoding="utf-8") as f:
            json.dump(cd, f, indent=2)
        for name, path in [("model.safetensors", sp), ("config.json", cp)]:
            api.upload_file(
                path_or_fileobj=path,
                path_in_repo=f"{stage}/{name}",
                repo_id=OUTPUT_MODEL,
                repo_type="model",
                token=hf_token,
            )
    if metrics:
        api.upload_file(
            path_or_fileobj=json.dumps(metrics, indent=2).encode("utf-8"),
            path_in_repo="metrics.json",
            repo_id=OUTPUT_MODEL,
            repo_type="model",
            token=hf_token,
        )

    # PR 4c: upload model card alongside weights.
    readme_result = upload_hf_readme(
        repo_id=OUTPUT_MODEL,
        readme_path=get_hf_card_path("football2vec-360-model-card.md", kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    logger.info(
        "Uploaded model card: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )
    logger.info("Saved checkpoint to %s/%s/", OUTPUT_MODEL, stage)


def _load_stage1(config: Football2Vec360Config, device: torch.device, hf_token: str) -> Football2Vec360Encoder:
    """Load Stage 1 checkpoint from HF Hub."""
    from huggingface_hub import hf_hub_download
    from safetensors.torch import load_file as _load_safetensors

    model = Football2Vec360Encoder(config)
    expanded = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim)
    with torch.no_grad():
        expanded.weight[:VOCAB_SIZE] = model.base_encoder.token_embedding.weight
    model.base_encoder.token_embedding = expanded
    local = hf_hub_download(OUTPUT_MODEL, "stage1/model.safetensors", repo_type="model", token=hf_token)
    model.load_state_dict(_load_safetensors(local, device=str(device)))
    return model.to(device)


def _publish_embeddings(embeddings_df: pd.DataFrame, hf_token: str, stage: str) -> None:
    """Publish 208d embeddings DataFrame to HF Hub as Parquet."""
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(OUTPUT_EMBEDDINGS, exist_ok=True, repo_type="dataset", token=hf_token)
    with tempfile.TemporaryDirectory() as tmpdir:
        p = os.path.join(tmpdir, "embeddings_360.parquet")
        embeddings_df.to_parquet(p, index=False)
        api.upload_file(
            path_or_fileobj=p,
            path_in_repo="data/embeddings_360.parquet",
            repo_id=OUTPUT_EMBEDDINGS,
            repo_type="dataset",
            token=hf_token,
            commit_message=f"Update 360 embeddings ({stage})",
        )

    # PR 4c: upload dataset card alongside embeddings.
    readme_result = upload_hf_readme(
        repo_id=OUTPUT_EMBEDDINGS,
        readme_path=get_hf_card_path("football2vec-360-embeddings.md", kind="dataset"),
        hf_token=hf_token,
    )
    logger.info(
        "Uploaded embeddings card: %s (sha256=%s)",
        readme_result["commit_url"],
        readme_result["sha256"][:8],
    )
    logger.info("Published %d embeddings to %s", len(embeddings_df), OUTPUT_EMBEDDINGS)


def _log_to_mlflow(
    stage: str,
    config: Football2Vec360Config,
    history: dict[str, list[float]],
    metrics: dict[str, Any],
    model: Football2Vec360Encoder,
    args: argparse.Namespace,
    dataset_commit: str,
    n_train: int,
    n_val: int,
    n_test: int,
) -> None:
    """Log training run to MLflow if MLFLOW_TRACKING_URI is set."""
    # ADR-012 §4: registration is unconditional (require_mlflow_env() at main()
    # entry proved the env present). Read via subscript so a missing value raises.
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow

    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("/soccer_analytics/football2vec_360")
    with mlflow.start_run(run_name=f"football2vec_360_{stage}_hf_jobs"):
        mlflow.log_params(
            {
                "stage": stage,
                "architecture": "transformer_plus_deep_sets",
                "vocab_size": config.vocab_size,
                "hidden_dim": config.hidden_dim,
                "context_dim": config.context_dim,
                "output_dim": OUTPUT_DIM,
                "num_layers": config.num_layers,
                "num_heads": config.num_heads,
                "dropout": config.dropout,
                "max_seq_len": config.max_seq_len,
                "mask_prob": config.mask_prob,
                "spatial_mlp_dim": config.spatial_mlp_dim,
                "deep_sets_hidden": config.deep_sets_hidden,
                "player_feature_dim": config.player_feature_dim,
                "max_players": MAX_PLAYERS,
                "batch_size": args.batch_size,
                "max_epochs": args.epochs,
                "actual_epochs": len(history.get("train_loss", history.get("train_mlm_loss", []))),
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
        if stage == "stage2":
            mlflow.log_params(
                {
                    "adversarial_lambda_max": ADVERSARIAL_LAMBDA_MAX,
                    "adversarial_warmup_epochs": ADVERSARIAL_WARMUP_EPOCHS,
                    "adversary_target": "competition_id",
                    "adversary_input_dim": config.hidden_dim + config.context_dim,
                }
            )
        for name, value in metrics.items():
            if isinstance(value, (int, float)):
                mlflow.log_metric(name, value)
        for key, values in history.items():
            for i, val in enumerate(values):
                mlflow.log_metric(key, val, step=i)

        class _Wrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            def predict(self, context: Any, model_input: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(model_input))

        mlflow.pyfunc.log_model(
            python_model=_Wrapper(),
            artifact_path="football2vec_360_model",
            registered_model_name=mlflow_fqn,
            input_example=pd.DataFrame({"x": [0.0]}),
        )
        run_id = mlflow.active_run().info.run_id
    # ADR-012 §4 zombie-alias guard: round-trip the @Champion alias read.
    client = mlflow.tracking.MlflowClient()
    set_and_verify_mlflow_champion(client, mlflow_fqn=mlflow_fqn, run_id=run_id)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-football2vec-360", phase="training")
def main() -> None:
    """Train Football2Vec 360: Stage 1 (MLM) or Stage 2 (adversarial debiasing)."""
    _assert_silly_kicks_min()
    require_mlflow_env()  # ADR-012 §4: fail loud before training if registration env is missing.

    parser = argparse.ArgumentParser(description="Train Football2Vec 360-enriched transformer")
    parser.add_argument("--stage", type=int, choices=[1, 2], default=1)
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--lr", type=float, default=DEFAULT_LR)
    parser.add_argument("--patience", type=int, default=DEFAULT_PATIENCE)
    parser.add_argument("--no-pretrained", action="store_true")
    args = parser.parse_args()

    from huggingface_hub import get_token

    pipeline_start = time.time()
    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    recorder = HFJobsCostRecorder(
        workflow_id="wf-football2vec-360",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=OUTPUT_MODEL,
        repo_type="model",
    )
    recorder.start()
    # Seed torch RNG: training is stochastic (DataLoader(shuffle=True), dropout, weight init).
    # A fixed seed restores byte-reproducibility so the spec §9.8 differential-recompute test applies and
    # a public-only corpus rebuild is verifiable. See spec §6.8 ("Recommended regardless: seed the model").
    torch.manual_seed(_TORCH_SEED)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    if device.type == "cuda":
        logger.info("GPU: %s", torch.cuda.get_device_name(0))

    try:
        if args.stage == 1:
            _run_stage1(args, hf_token, device, recorder)
        else:
            _run_stage2(args, hf_token, device, recorder)
    except Exception as exc:
        recorder.fail(exc)
        raise
    logger.info("Football2Vec 360 Stage %d complete in %.1f seconds", args.stage, time.time() - pipeline_start)


def _run_stage1(args: argparse.Namespace, hf_token: str, device: torch.device, recorder: HFJobsCostRecorder) -> None:
    """Execute Stage 1: MLM pre-training with 360 context."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    data, dataset_commit = load_training_data_sql(
        host, os.environ["DATABRICKS_TOKEN"], os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
    )
    action_ids_all, x_coords_all, y_coords_all = parse_actions(data["actions"])
    freeze_frames_all = parse_freeze_frames(data["freeze_frames"])
    train_df, val_df, test_df = stratified_split(data)
    ti, vi, tei = train_df.index.tolist(), val_df.index.tolist(), test_df.index.tolist()
    logger.info("Split: train=%d, val=%d, test=%d", len(ti), len(vi), len(tei))

    train_ds = Football2Vec360Dataset(
        [action_ids_all[i] for i in ti],
        [x_coords_all[i] for i in ti],
        [y_coords_all[i] for i in ti],
        freeze_frames=[freeze_frames_all[i] for i in ti],
        mlm=True,
    )
    val_ds = Football2Vec360Dataset(
        [action_ids_all[i] for i in vi],
        [x_coords_all[i] for i in vi],
        [y_coords_all[i] for i in vi],
        freeze_frames=[freeze_frames_all[i] for i in vi],
        mlm=True,
    )

    config = Football2Vec360Config()
    model = Football2Vec360Encoder(config)
    expanded = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim)
    with torch.no_grad():
        expanded.weight[:VOCAB_SIZE] = model.base_encoder.token_embedding.weight
    model.base_encoder.token_embedding = expanded
    if not args.no_pretrained:
        model = _try_load_pretrained(model, device, hf_token)
    model = model.to(device)

    model, mlm_head, history = _train_stage1_loop(
        train_ds, val_ds, config, device, args.epochs, args.batch_size, args.lr, args.patience
    )
    test_ds = Football2Vec360Dataset(
        [action_ids_all[i] for i in tei],
        [x_coords_all[i] for i in tei],
        [y_coords_all[i] for i in tei],
        freeze_frames=[freeze_frames_all[i] for i in tei],
        mlm=True,
    )
    test_loader = DataLoader(
        test_ds,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    test_loss, test_acc = _evaluate_mlm(
        model, mlm_head, test_loader, nn.CrossEntropyLoss(ignore_index=-100), config, device
    )
    logger.info("Test — loss=%.4f  accuracy=%.4f", test_loss, test_acc)

    embeddings_df = _generate_embeddings(
        model, data, action_ids_all, x_coords_all, y_coords_all, freeze_frames_all, device
    )
    metrics: dict[str, Any] = {
        "stage": "stage1",
        "test_loss": test_loss,
        "test_accuracy": test_acc,
        "best_val_loss": min(history["val_loss"]) if history["val_loss"] else None,
        "actual_epochs": len(history["train_loss"]),
        "n_train": len(ti),
        "n_val": len(vi),
        "n_test": len(tei),
        "n_embeddings": len(embeddings_df),
        "embedding_dim": OUTPUT_DIM,
        "dataset_commit": dataset_commit,
        "config": asdict(config),
    }
    metrics = recorder.complete(metrics, row_count=len(embeddings_df))
    _save_checkpoint(model, config, "stage1", hf_token, metrics=metrics)
    _log_to_mlflow(
        "stage1",
        config,
        history,
        {"test_loss": test_loss, "test_accuracy": test_acc},
        model,
        args,
        dataset_commit,
        len(ti),
        len(vi),
        len(tei),
    )


def _run_stage2(args: argparse.Namespace, hf_token: str, device: torch.device, recorder: HFJobsCostRecorder) -> None:
    """Execute Stage 2: Adversarial competition debiasing."""
    host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
    data, dataset_commit = load_training_data_sql(
        host, os.environ["DATABRICKS_TOKEN"], os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
    )
    action_ids_all, x_coords_all, y_coords_all = parse_actions(data["actions"])
    freeze_frames_all = parse_freeze_frames(data["freeze_frames"])
    unique_comp = sorted(data["competition_id"].unique().tolist())
    comp_to_idx: dict[int, int] = {c: i for i, c in enumerate(unique_comp)}
    comp_labels = [comp_to_idx[int(c)] for c in data["competition_id"].values]

    config = Football2Vec360Config()
    model = _load_stage1(config, device, hf_token)
    train_df, val_df, test_df = stratified_split(data)
    ti, vi, tei = train_df.index.tolist(), val_df.index.tolist(), test_df.index.tolist()

    train_ds = Football2Vec360Dataset(
        [action_ids_all[i] for i in ti],
        [x_coords_all[i] for i in ti],
        [y_coords_all[i] for i in ti],
        freeze_frames=[freeze_frames_all[i] for i in ti],
        mlm=True,
        competition_ids=[comp_labels[i] for i in ti],
    )
    val_ds = Football2Vec360Dataset(
        [action_ids_all[i] for i in vi],
        [x_coords_all[i] for i in vi],
        [y_coords_all[i] for i in vi],
        freeze_frames=[freeze_frames_all[i] for i in vi],
        mlm=True,
        competition_ids=[comp_labels[i] for i in vi],
    )

    model, adversary, history = _train_stage2_loop(
        model, train_ds, val_ds, len(unique_comp), config, device, args.epochs, args.batch_size, args.lr, args.patience
    )
    test_ds = Football2Vec360Dataset(
        [action_ids_all[i] for i in tei],
        [x_coords_all[i] for i in tei],
        [y_coords_all[i] for i in tei],
        freeze_frames=[freeze_frames_all[i] for i in tei],
        mlm=True,
        competition_ids=[comp_labels[i] for i in tei],
    )
    test_loader = DataLoader(
        test_ds,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    test_mlm_head = MLMHead(config.hidden_dim, config.vocab_size).to(device)
    test_mlm_loss, test_adv_acc = _evaluate_stage2(
        model, test_mlm_head, adversary, test_loader, nn.CrossEntropyLoss(ignore_index=-100), config, device
    )
    chance = 1.0 / len(unique_comp)
    logger.info("Test — mlm_loss=%.4f  adv_acc=%.4f  chance=%.4f", test_mlm_loss, test_adv_acc, chance)

    embeddings_df = _generate_embeddings(
        model, data, action_ids_all, x_coords_all, y_coords_all, freeze_frames_all, device
    )
    metrics: dict[str, Any] = {
        "stage": "stage2",
        "test_mlm_loss": test_mlm_loss,
        "test_adv_accuracy": test_adv_acc,
        "chance_level": chance,
        "num_competitions": len(unique_comp),
        "adversarial_lambda_max": ADVERSARIAL_LAMBDA_MAX,
        "actual_epochs": len(history["train_mlm_loss"]),
        "n_train": len(ti),
        "n_val": len(vi),
        "n_test": len(tei),
        "n_embeddings": len(embeddings_df),
        "embedding_dim": OUTPUT_DIM,
        "dataset_commit": dataset_commit,
        "config": asdict(config),
    }
    metrics = recorder.complete(metrics, row_count=len(embeddings_df))
    _save_checkpoint(model, config, "stage2", hf_token, metrics=metrics)
    _publish_embeddings(embeddings_df, hf_token, "stage2")
    _log_to_mlflow(
        "stage2",
        config,
        history,
        {"test_mlm_loss": test_mlm_loss, "test_adv_accuracy": test_adv_acc, "chance_level": chance},
        model,
        args,
        dataset_commit,
        len(ti),
        len(vi),
        len(tei),
    )


if __name__ == "__main__":
    main()
