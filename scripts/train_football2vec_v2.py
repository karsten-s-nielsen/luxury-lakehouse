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
# ]
# ///
"""Train Football2Vec v2 transformer (MLM + adversarial debiasing) on HF Jobs A10G GPU.

Downloads SPADL action sequences from HF Hub, trains a tiny encoder-only
transformer via masked language modeling (Stage 1), optionally adds adversarial
competition debiasing via gradient reversal (Stage 2), logs to MLflow, and
pushes model checkpoints + embeddings to HF Hub.

Stage 1 (MLM): Learn contextual player embeddings by predicting masked SPADL
action types from surrounding sequence context. Spatial (x, y) coordinates are
injected via learned MLP projections.

Stage 2 (Adversarial): Fine-tune Stage 1 checkpoint with a gradient reversal
layer (Ganin et al. 2016) that discourages the encoder from learning
competition-specific features. Competition ID is used as the adversary target
because it is the strongest contextual confounder (league style differences).

References:
    Danesi, P. (2025). "Football2Vec: Transformer-Based Player Embeddings."
    Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural Networks."
        JMLR 17(1), pp. 1-35.
    Decroos, T. et al. (2019). "Actions Speak Louder than Goals: Valuing
        Player Actions in Soccer." KDD.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_football2vec_v2.py --stage 1 \\
        --flavor a10g-large --timeout 120m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN

    hf jobs uv run scripts/train_football2vec_v2.py --stage 2 \\
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
import math
import os
import tempfile
import time
from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from analytics.cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from analytics.football2vec_transformer import (
    Football2VecConfig,
    Football2VecEncoder,
    TeamClassifierHead,
)
from workflows import workflow

# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HF_ORG = "luxury-lakehouse"
TRAINING_DATASET = f"{HF_ORG}/football2vec-training-data"
MODEL_REPO = f"{HF_ORG}/football2vec-v2"
EMBEDDINGS_DATASET = f"{HF_ORG}/football2vec-statsbomb-wyscout"

# SPADL 23-type action vocabulary (mirrors export_embeddings_training_data.py)
VOCAB_SIZE = 23
MASK_TOKEN_ID = VOCAB_SIZE  # 23 — dedicated mask token (outside vocab)
PAD_TOKEN_ID = VOCAB_SIZE + 1  # 24 — padding token

# Defaults (overridden by CLI args)
DEFAULT_EPOCHS = 30
DEFAULT_BATCH_SIZE = 256
DEFAULT_LR = 1e-4
DEFAULT_PATIENCE = 5
DEFAULT_MASK_PROB = 0.15
MAX_SEQ_LEN = 512
WEIGHT_DECAY = 0.01
WARMUP_FRACTION = 0.10
RANDOM_STATE = 42

# Stage 2 adversarial
ADVERSARIAL_LAMBDA_MAX = 0.2
ADVERSARIAL_WARMUP_EPOCHS = 5


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_training_data(hf_token: str) -> tuple[pd.DataFrame, str]:
    """Download training data from HF Hub and return DataFrame + commit hash.

    The HF dataset contains Parquet files with columns:
        canonical_player_id, match_id, competition_id, season_id,
        position_group, actions (array of struct)

    Returns:
        Tuple of (DataFrame, dataset_commit_sha).
    """
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)

    # Find all parquet files in the dataset repo
    all_items = list(api.list_repo_tree(TRAINING_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {TRAINING_DATASET}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(TRAINING_DATASET, pf, repo_type="dataset", token=hf_token)
        # Read with pyarrow for struct array support
        table = pq.read_table(local_path)
        df = table.to_pandas()
        dfs.append(df)
        logger.info("  %s: %d rows", pf, len(df))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total player-match sequences: %d", len(data))

    # Dataset commit hash for reproducibility
    dataset_info = api.repo_info(repo_id=TRAINING_DATASET, repo_type="dataset")
    commit_sha = dataset_info.sha

    return data, commit_sha


def _parse_actions(
    actions_col: pd.Series,
) -> tuple[list[list[int]], list[list[float]], list[list[float]]]:
    """Parse the actions struct array column into separate lists.

    Each row in actions_col is a list of dicts (or similar struct) with keys:
        action_type (int), x (float), y (float), result (int)

    Returns:
        Tuple of (action_ids_per_row, x_coords_per_row, y_coords_per_row).
    """
    all_action_ids: list[list[int]] = []
    all_x_coords: list[list[float]] = []
    all_y_coords: list[list[float]] = []

    for actions in actions_col:
        if actions is None or (hasattr(actions, "__len__") and len(actions) == 0):
            all_action_ids.append([])
            all_x_coords.append([])
            all_y_coords.append([])
            continue

        action_ids: list[int] = []
        x_coords: list[float] = []
        y_coords: list[float] = []

        for act in actions:
            # Both dict and pyarrow struct support [] access
            action_ids.append(int(act["action_type"]))
            x_coords.append(float(act["x"]))
            y_coords.append(float(act["y"]))

        all_action_ids.append(action_ids)
        all_x_coords.append(x_coords)
        all_y_coords.append(y_coords)

    return all_action_ids, all_x_coords, all_y_coords


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class Football2VecDataset(Dataset[dict[str, torch.Tensor]]):
    """PyTorch dataset for SPADL action sequences with MLM masking.

    Tokenizes each player-match sequence: action_ids (int), x_coords (float 0-1),
    y_coords (float 0-1). Pads/truncates to max_seq_len. For MLM, randomly masks
    15% of valid tokens and creates labels (-100 for non-masked positions).

    Args:
        action_ids: List of per-row action ID sequences.
        x_coords: List of per-row normalized x coordinate sequences.
        y_coords: List of per-row normalized y coordinate sequences.
        max_seq_len: Maximum sequence length (pad/truncate).
        mask_prob: Probability of masking each valid token for MLM.
        mlm: Whether to apply MLM masking (False for inference).
        competition_ids: Optional competition IDs for adversarial training.
    """

    def __init__(
        self,
        action_ids: list[list[int]],
        x_coords: list[list[float]],
        y_coords: list[list[float]],
        max_seq_len: int = MAX_SEQ_LEN,
        mask_prob: float = DEFAULT_MASK_PROB,
        *,
        mlm: bool = True,
        competition_ids: list[int] | None = None,
    ) -> None:
        self.action_ids = action_ids
        self.x_coords = x_coords
        self.y_coords = y_coords
        self.max_seq_len = max_seq_len
        self.mask_prob = mask_prob
        self.mlm = mlm
        self.competition_ids = competition_ids

    def __len__(self) -> int:
        return len(self.action_ids)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        """Return a single tokenized, padded, optionally masked sample."""
        aids = self.action_ids[idx]
        xs = self.x_coords[idx]
        ys = self.y_coords[idx]

        seq_len = min(len(aids), self.max_seq_len)

        # Truncate
        aids = aids[:seq_len]
        xs = xs[:seq_len]
        ys = ys[:seq_len]

        # Build tensors with padding
        action_tensor = torch.full((self.max_seq_len,), PAD_TOKEN_ID, dtype=torch.long)
        x_tensor = torch.zeros(self.max_seq_len, dtype=torch.float32)
        y_tensor = torch.zeros(self.max_seq_len, dtype=torch.float32)
        attention_mask = torch.zeros(self.max_seq_len, dtype=torch.bool)

        if seq_len > 0:
            action_tensor[:seq_len] = torch.tensor(aids, dtype=torch.long)
            x_tensor[:seq_len] = torch.tensor(xs, dtype=torch.float32)
            y_tensor[:seq_len] = torch.tensor(ys, dtype=torch.float32)
            attention_mask[:seq_len] = True

        result: dict[str, torch.Tensor] = {
            "action_ids": action_tensor,
            "x_coords": x_tensor,
            "y_coords": y_tensor,
            "attention_mask": attention_mask,
        }

        if self.mlm and seq_len > 0:
            # MLM: mask 15% of valid tokens
            labels = torch.full((self.max_seq_len,), -100, dtype=torch.long)
            # Only mask valid (non-padding) positions
            mask_candidates = torch.arange(seq_len)
            n_mask = max(1, int(seq_len * self.mask_prob))
            mask_indices = mask_candidates[torch.randperm(seq_len)[:n_mask]]

            # Store original action IDs as labels at masked positions
            labels[mask_indices] = action_tensor[mask_indices].clone()

            # Replace masked positions with MASK_TOKEN_ID
            action_tensor[mask_indices] = MASK_TOKEN_ID

            result["labels"] = labels
        elif self.mlm:
            result["labels"] = torch.full((self.max_seq_len,), -100, dtype=torch.long)

        if self.competition_ids is not None:
            result["competition_id"] = torch.tensor(self.competition_ids[idx], dtype=torch.long)

        return result


# ---------------------------------------------------------------------------
# Train/val/test splitting
# ---------------------------------------------------------------------------


def stratified_split(
    data: pd.DataFrame,
    train_frac: float = 0.80,
    val_frac: float = 0.10,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split data into train/val/test stratified by competition_id.

    Handles rare competitions (< 3 samples) by merging them into "_other_"
    for stratification purposes. The actual competition_id is preserved.

    Args:
        data: Full DataFrame with competition_id column.
        train_frac: Training fraction (default 0.80).
        val_frac: Validation fraction (default 0.10).

    Returns:
        Tuple of (train_df, val_df, test_df).
    """
    from sklearn.model_selection import train_test_split

    # Build stratification column
    stratify_col = data["competition_id"].astype(str)
    counts = stratify_col.value_counts()
    # Merge rare competitions (< 3 samples needed for 3-way split)
    rare_mask = stratify_col.isin(counts[counts < 3].index)
    stratify_col = stratify_col.copy()
    stratify_col.loc[rare_mask] = "_other_"

    indices = np.arange(len(data))

    # First split: train+val vs test
    test_frac = 1.0 - train_frac - val_frac
    train_val_idx, test_idx = train_test_split(
        indices,
        test_size=test_frac,
        random_state=RANDOM_STATE,
        stratify=stratify_col,
    )

    # Second split: train vs val (from train+val)
    val_relative = val_frac / (train_frac + val_frac)
    stratify_trainval = stratify_col.iloc[train_val_idx]
    # Re-merge rare groups in the subset
    tv_counts = stratify_trainval.value_counts()
    tv_rare = stratify_trainval.isin(tv_counts[tv_counts < 2].index)
    stratify_trainval = stratify_trainval.copy()
    stratify_trainval.loc[tv_rare] = "_other_"

    train_idx, val_idx = train_test_split(
        train_val_idx,
        test_size=val_relative,
        random_state=RANDOM_STATE,
        stratify=stratify_trainval,
    )

    return data.iloc[train_idx], data.iloc[val_idx], data.iloc[test_idx]


# ---------------------------------------------------------------------------
# Learning rate scheduler
# ---------------------------------------------------------------------------


def get_cosine_schedule_with_warmup(
    optimizer: torch.optim.Optimizer,
    num_warmup_steps: int,
    num_training_steps: int,
) -> torch.optim.lr_scheduler.LambdaLR:
    """Cosine annealing with linear warmup.

    Args:
        optimizer: The optimizer.
        num_warmup_steps: Number of warmup steps (linear ramp from 0 to base lr).
        num_training_steps: Total training steps (warmup + cosine decay to 0).

    Returns:
        LambdaLR scheduler instance.
    """

    def lr_lambda(current_step: int) -> float:
        if current_step < num_warmup_steps:
            return float(current_step) / float(max(1, num_warmup_steps))
        progress = float(current_step - num_warmup_steps) / float(max(1, num_training_steps - num_warmup_steps))
        return max(0.0, 0.5 * (1.0 + math.cos(math.pi * progress)))

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)


# ---------------------------------------------------------------------------
# Stage 1: MLM Training
# ---------------------------------------------------------------------------


def train_stage1(
    train_dataset: Football2VecDataset,
    val_dataset: Football2VecDataset,
    config: Football2VecConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2VecEncoder, dict[str, list[float]]]:
    """Train the encoder with masked language modeling.

    Args:
        train_dataset: Training dataset with MLM masking.
        val_dataset: Validation dataset with MLM masking.
        config: Model configuration.
        device: torch device (cuda or cpu).
        epochs: Maximum number of epochs.
        batch_size: Batch size.
        lr: Learning rate.
        patience: Early stopping patience.

    Returns:
        Tuple of (trained_model, training_history).
    """
    model = Football2VecEncoder(config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    # The model expects vocab_size=23, but we use MASK_TOKEN_ID=23 as input.
    # We need to expand the token embedding to accommodate the mask token.
    # Replace the token embedding with one that has vocab_size + 2 (mask + pad)
    expanded_embed = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim).to(device)
    # Copy original weights for the first vocab_size entries
    with torch.no_grad():
        expanded_embed.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded_embed

    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )
    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)

    # Cosine schedule with warmup
    total_steps = len(train_loader) * epochs
    warmup_steps = int(total_steps * WARMUP_FRACTION)
    scheduler = get_cosine_schedule_with_warmup(optimizer, warmup_steps, total_steps)

    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    best_val_loss = float("inf")
    patience_counter = 0
    best_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {
        "train_loss": [],
        "val_loss": [],
        "val_accuracy": [],
    }

    for epoch in range(epochs):
        epoch_start = time.time()

        # --- Training ---
        model.train()
        total_loss = 0.0
        n_batches = 0

        for batch in train_loader:
            action_ids = batch["action_ids"].to(device)
            x_coords = batch["x_coords"].to(device)
            y_coords = batch["y_coords"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            optimizer.zero_grad()

            # MLM forward: get per-token logits
            logits = model.mlm_forward(action_ids, x_coords, y_coords, attention_mask)
            # logits: (batch, seq_len, vocab_size=23) — but we need vocab_size+2
            # Since we expanded the embedding, the mlm_head still outputs vocab_size=23.
            # We only predict the original 23 action types (mask token is never a label).
            loss = criterion(logits.view(-1, config.vocab_size), labels.view(-1))

            loss.backward()
            # Gradient clipping for training stability
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            scheduler.step()

            total_loss += loss.item()
            n_batches += 1

        avg_train_loss = total_loss / max(n_batches, 1)

        # --- Validation ---
        val_loss, val_accuracy = _evaluate_mlm(model, val_loader, criterion, config, device)

        history["train_loss"].append(avg_train_loss)
        history["val_loss"].append(val_loss)
        history["val_accuracy"].append(val_accuracy)

        elapsed = time.time() - epoch_start
        logger.info(
            "Epoch %d/%d — train_loss=%.4f  val_loss=%.4f  val_acc=%.4f  lr=%.2e  (%.1fs)",
            epoch + 1,
            epochs,
            avg_train_loss,
            val_loss,
            val_accuracy,
            scheduler.get_last_lr()[0],
            elapsed,
        )

        # Early stopping on validation loss
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= patience:
                logger.info("Early stopping at epoch %d (patience=%d)", epoch + 1, patience)
                break

    # Restore best weights
    if best_state:
        model.load_state_dict(best_state)
        logger.info("Restored best model weights (val_loss=%.4f)", best_val_loss)

    return model, history


def _evaluate_mlm(
    model: Football2VecEncoder,
    val_loader: DataLoader[dict[str, torch.Tensor]],
    criterion: nn.CrossEntropyLoss,
    config: Football2VecConfig,
    device: torch.device,
) -> tuple[float, float]:
    """Evaluate MLM loss and accuracy on masked tokens.

    Returns:
        Tuple of (avg_val_loss, masked_token_accuracy).
    """
    model.eval()
    total_loss = 0.0
    total_correct = 0
    total_masked = 0
    n_batches = 0

    with torch.no_grad():
        for batch in val_loader:
            action_ids = batch["action_ids"].to(device)
            x_coords = batch["x_coords"].to(device)
            y_coords = batch["y_coords"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            logits = model.mlm_forward(action_ids, x_coords, y_coords, attention_mask)
            loss = criterion(logits.view(-1, config.vocab_size), labels.view(-1))

            total_loss += loss.item()
            n_batches += 1

            # Accuracy on masked tokens only (where labels != -100)
            mask = labels != -100
            if mask.any():
                predicted = logits.argmax(dim=-1)
                total_correct += (predicted[mask] == labels[mask]).sum().item()
                total_masked += mask.sum().item()

    avg_loss = total_loss / max(n_batches, 1)
    accuracy = total_correct / max(total_masked, 1)
    return avg_loss, accuracy


# ---------------------------------------------------------------------------
# Stage 2: Adversarial Debiasing
# ---------------------------------------------------------------------------


def train_stage2(
    model: Football2VecEncoder,
    train_dataset: Football2VecDataset,
    val_dataset: Football2VecDataset,
    num_competitions: int,
    competition_to_idx: dict[int, int],
    config: Football2VecConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2VecEncoder, TeamClassifierHead, dict[str, list[float]]]:
    """Fine-tune with adversarial competition debiasing.

    Combines MLM loss with a gradient-reversed competition classifier.
    Lambda ramps linearly from 0 to ADVERSARIAL_LAMBDA_MAX over the first
    ADVERSARIAL_WARMUP_EPOCHS epochs.

    Args:
        model: Pre-trained Stage 1 encoder.
        train_dataset: Training dataset (must have competition_ids set).
        val_dataset: Validation dataset (must have competition_ids set).
        num_competitions: Number of unique competitions (classifier output dim).
        competition_to_idx: Mapping from competition_id to 0-indexed class label.
        config: Model configuration.
        device: torch device.
        epochs: Maximum epochs.
        batch_size: Batch size.
        lr: Learning rate.
        patience: Early stopping patience.

    Returns:
        Tuple of (fine_tuned_encoder, classifier_head, training_history).
    """
    model = model.to(device)

    # Create adversarial classifier head (using TeamClassifierHead with competition classes)
    adversary = TeamClassifierHead(
        hidden_dim=config.hidden_dim,
        num_teams=num_competitions,
        lambda_val=0.0,  # will be updated each epoch via warmup
    ).to(device)

    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )
    val_loader = DataLoader(
        val_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    # Optimize all parameters: encoder + adversary head
    all_params = list(model.parameters()) + list(adversary.parameters())
    optimizer = torch.optim.AdamW(all_params, lr=lr, weight_decay=WEIGHT_DECAY)

    total_steps = len(train_loader) * epochs
    warmup_steps = int(total_steps * WARMUP_FRACTION)
    scheduler = get_cosine_schedule_with_warmup(optimizer, warmup_steps, total_steps)

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

        # Lambda warmup: linear ramp from 0 to ADVERSARIAL_LAMBDA_MAX
        if epoch < ADVERSARIAL_WARMUP_EPOCHS:
            current_lambda = ADVERSARIAL_LAMBDA_MAX * (epoch / ADVERSARIAL_WARMUP_EPOCHS)
        else:
            current_lambda = ADVERSARIAL_LAMBDA_MAX

        # Update the GRL lambda in the adversary head
        adversary.grl.lambda_val = current_lambda

        # --- Training ---
        model.train()
        adversary.train()
        total_mlm_loss = 0.0
        total_adv_loss = 0.0
        n_batches = 0

        for batch in train_loader:
            action_ids = batch["action_ids"].to(device)
            x_coords = batch["x_coords"].to(device)
            y_coords = batch["y_coords"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            comp_ids = batch["competition_id"].to(device)

            optimizer.zero_grad()

            # MLM forward
            mlm_logits = model.mlm_forward(action_ids, x_coords, y_coords, attention_mask)
            mlm_loss = mlm_criterion(mlm_logits.view(-1, config.vocab_size), labels.view(-1))

            # Sequence embedding for adversary
            embeddings = model(action_ids, x_coords, y_coords, attention_mask)
            adv_logits = adversary(embeddings)
            adv_loss = adv_criterion(adv_logits, comp_ids)

            # Combined loss: GRL handles sign flip internally, so we ADD
            combined_loss = mlm_loss + current_lambda * adv_loss

            combined_loss.backward()
            torch.nn.utils.clip_grad_norm_(all_params, max_norm=1.0)
            optimizer.step()
            scheduler.step()

            total_mlm_loss += mlm_loss.item()
            total_adv_loss += adv_loss.item()
            n_batches += 1

        avg_mlm_loss = total_mlm_loss / max(n_batches, 1)
        avg_adv_loss = total_adv_loss / max(n_batches, 1)
        avg_combined_loss = avg_mlm_loss + current_lambda * avg_adv_loss

        # --- Validation ---
        val_mlm_loss, val_adv_accuracy = _evaluate_stage2(model, adversary, val_loader, mlm_criterion, config, device)
        val_combined_loss = val_mlm_loss + current_lambda * avg_adv_loss

        history["train_mlm_loss"].append(avg_mlm_loss)
        history["train_adv_loss"].append(avg_adv_loss)
        history["train_combined_loss"].append(avg_combined_loss)
        history["val_mlm_loss"].append(val_mlm_loss)
        history["val_adv_accuracy"].append(val_adv_accuracy)
        history["val_combined_loss"].append(val_combined_loss)
        history["lambda_val"].append(current_lambda)

        elapsed = time.time() - epoch_start
        logger.info(
            "Epoch %d/%d — mlm=%.4f  adv=%.4f  combined=%.4f  val_mlm=%.4f  val_adv_acc=%.4f  lambda=%.3f  (%.1fs)",
            epoch + 1,
            epochs,
            avg_mlm_loss,
            avg_adv_loss,
            avg_combined_loss,
            val_mlm_loss,
            val_adv_accuracy,
            current_lambda,
            elapsed,
        )

        # Early stopping on combined validation loss
        if val_combined_loss < best_combined_loss:
            best_combined_loss = val_combined_loss
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
        logger.info("Restored best model weights (val_combined_loss=%.4f)", best_combined_loss)

    return model, adversary, history


def _evaluate_stage2(
    model: Football2VecEncoder,
    adversary: TeamClassifierHead,
    val_loader: DataLoader[dict[str, torch.Tensor]],
    mlm_criterion: nn.CrossEntropyLoss,
    config: Football2VecConfig,
    device: torch.device,
) -> tuple[float, float]:
    """Evaluate Stage 2: MLM loss + adversary accuracy.

    Returns:
        Tuple of (avg_mlm_loss, competition_classification_accuracy).
    """
    model.eval()
    adversary.eval()
    total_mlm_loss = 0.0
    total_adv_correct = 0
    total_adv_samples = 0
    n_batches = 0

    with torch.no_grad():
        for batch in val_loader:
            action_ids = batch["action_ids"].to(device)
            x_coords = batch["x_coords"].to(device)
            y_coords = batch["y_coords"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            comp_ids = batch["competition_id"].to(device)

            # MLM loss
            mlm_logits = model.mlm_forward(action_ids, x_coords, y_coords, attention_mask)
            mlm_loss = mlm_criterion(mlm_logits.view(-1, config.vocab_size), labels.view(-1))
            total_mlm_loss += mlm_loss.item()

            # Adversary accuracy (should decrease if debiasing works)
            embeddings = model(action_ids, x_coords, y_coords, attention_mask)
            adv_logits = adversary(embeddings)
            adv_predicted = adv_logits.argmax(dim=-1)
            total_adv_correct += (adv_predicted == comp_ids).sum().item()
            total_adv_samples += comp_ids.size(0)
            n_batches += 1

    avg_mlm_loss = total_mlm_loss / max(n_batches, 1)
    adv_accuracy = total_adv_correct / max(total_adv_samples, 1)
    return avg_mlm_loss, adv_accuracy


# ---------------------------------------------------------------------------
# Embedding inference
# ---------------------------------------------------------------------------


def generate_embeddings(
    model: Football2VecEncoder,
    data: pd.DataFrame,
    action_ids_all: list[list[int]],
    x_coords_all: list[list[float]],
    y_coords_all: list[list[float]],
    device: torch.device,
    batch_size: int = 512,
) -> pd.DataFrame:
    """Run inference on all data to produce 128d embeddings.

    Args:
        model: Trained encoder.
        data: Full DataFrame with canonical_player_id, match_id columns.
        action_ids_all: All action ID sequences.
        x_coords_all: All x coordinate sequences.
        y_coords_all: All y coordinate sequences.
        device: torch device.
        batch_size: Inference batch size.

    Returns:
        DataFrame with canonical_player_id, match_id, behavioral_vector columns.
    """
    model.eval()

    # Create inference dataset (no MLM masking)
    infer_dataset = Football2VecDataset(action_ids_all, x_coords_all, y_coords_all, mlm=False)
    infer_loader = DataLoader(
        infer_dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    all_embeddings: list[np.ndarray] = []

    with torch.no_grad():
        for batch in infer_loader:
            action_ids = batch["action_ids"].to(device)
            x_coords = batch["x_coords"].to(device)
            y_coords = batch["y_coords"].to(device)
            attention_mask = batch["attention_mask"].to(device)

            embeddings = model(action_ids, x_coords, y_coords, attention_mask)
            all_embeddings.append(embeddings.cpu().numpy())

    embeddings_array = np.concatenate(all_embeddings, axis=0)
    logger.info("Generated embeddings shape: %s", embeddings_array.shape)

    # Build output DataFrame
    result = pd.DataFrame(
        {
            "canonical_player_id": data["canonical_player_id"].values,
            "match_id": data["match_id"].values,
            "behavioral_vector": [embeddings_array[i].tolist() for i in range(len(embeddings_array))],
        }
    )
    return result


# ---------------------------------------------------------------------------
# Model I/O
# ---------------------------------------------------------------------------


def save_checkpoint(
    model: Football2VecEncoder,
    config: Football2VecConfig,
    stage: str,
    hf_token: str,
    metrics: dict[str, Any] | None = None,
) -> None:
    """Save model checkpoint to HF Hub.

    Saves model_state_dict.pt and config.json under the stage subdirectory.

    Args:
        model: Trained encoder.
        config: Model configuration.
        stage: "stage1" or "stage2".
        hf_token: HF Hub token.
        metrics: Optional metrics dict to save alongside.
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Save state dict as safetensors (zero pickle surface)
        from safetensors.torch import save_file as _save_safetensors

        state_path = os.path.join(tmpdir, "model.safetensors")
        _save_safetensors(model.state_dict(), state_path)

        # Save config as JSON
        config_path = os.path.join(tmpdir, "config.json")
        config_dict = asdict(config)
        # Add metadata about the expanded embedding
        config_dict["_expanded_vocab_size"] = VOCAB_SIZE + 2
        config_dict["_mask_token_id"] = MASK_TOKEN_ID
        config_dict["_pad_token_id"] = PAD_TOKEN_ID
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_dict, f, indent=2)

        # Upload state dict (safetensors — no pickle)
        api.upload_file(
            path_or_fileobj=state_path,
            path_in_repo=f"{stage}/model.safetensors",
            repo_id=MODEL_REPO,
            repo_type="model",
            token=hf_token,
        )
        # Upload config
        api.upload_file(
            path_or_fileobj=config_path,
            path_in_repo=f"{stage}/config.json",
            repo_id=MODEL_REPO,
            repo_type="model",
            token=hf_token,
        )

    if metrics:
        api.upload_file(
            path_or_fileobj=json.dumps(metrics, indent=2).encode("utf-8"),
            path_in_repo="metrics.json",
            repo_id=MODEL_REPO,
            repo_type="model",
            token=hf_token,
        )

    logger.info("Saved checkpoint to %s/%s/", MODEL_REPO, stage)


def load_stage1_checkpoint(
    config: Football2VecConfig,
    device: torch.device,
    hf_token: str,
) -> Football2VecEncoder:
    """Load Stage 1 checkpoint from HF Hub.

    Args:
        config: Model configuration.
        device: torch device.
        hf_token: HF Hub token.

    Returns:
        Loaded Football2VecEncoder with expanded token embedding.
    """
    from huggingface_hub import hf_hub_download

    model = Football2VecEncoder(config)

    # Expand token embedding (same as Stage 1 training)
    expanded_embed = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim)
    with torch.no_grad():
        expanded_embed.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded_embed

    # Download state dict (safetensors — zero pickle surface)
    from safetensors.torch import load_file as _load_safetensors

    local_path = hf_hub_download(
        MODEL_REPO,
        "stage1/model.safetensors",
        repo_type="model",
        token=hf_token,
    )
    state_dict = _load_safetensors(local_path, device=str(device))
    model.load_state_dict(state_dict)
    model = model.to(device)

    logger.info("Loaded Stage 1 checkpoint from %s", MODEL_REPO)
    return model


def publish_embeddings(
    embeddings_df: pd.DataFrame,
    hf_token: str,
    stage: str,
) -> None:
    """Publish embeddings DataFrame to HF Hub as Parquet.

    Args:
        embeddings_df: DataFrame with canonical_player_id, match_id,
            behavioral_vector columns.
        hf_token: HF Hub token.
        stage: "stage1" or "stage2" (for commit message).
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(EMBEDDINGS_DATASET, exist_ok=True, repo_type="dataset", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, "embeddings_v2.parquet")
        embeddings_df.to_parquet(parquet_path, index=False)

        api.upload_file(
            path_or_fileobj=parquet_path,
            path_in_repo="data/embeddings_v2.parquet",
            repo_id=EMBEDDINGS_DATASET,
            repo_type="dataset",
            token=hf_token,
            commit_message=f"Update v2 embeddings ({stage})",
        )

    logger.info(
        "Published %d embeddings to %s (data/embeddings_v2.parquet)",
        len(embeddings_df),
        EMBEDDINGS_DATASET,
    )


# ---------------------------------------------------------------------------
# MLflow logging
# ---------------------------------------------------------------------------


def log_to_mlflow(
    stage: str,
    config: Football2VecConfig,
    history: dict[str, list[float]],
    metrics: dict[str, Any],
    model: Football2VecEncoder,
    args: argparse.Namespace,
    dataset_commit: str,
    n_train: int,
    n_val: int,
    n_test: int,
) -> None:
    """Log training run to MLflow if MLFLOW_TRACKING_URI is set.

    Args:
        stage: "stage1" or "stage2".
        config: Model configuration.
        history: Per-epoch training history.
        metrics: Final evaluation metrics.
        model: Trained model.
        args: CLI arguments.
        dataset_commit: Training data commit hash.
        n_train: Training set size.
        n_val: Validation set size.
        n_test: Test set size.
    """
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not tracking_uri:
        logger.info("MLflow skipped (MLFLOW_TRACKING_URI not set)")
        return

    import mlflow

    logger.info("=== Logging to MLflow (%s) ===", tracking_uri)
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("/soccer_analytics/football2vec_v2")

    with mlflow.start_run(run_name=f"football2vec_v2_{stage}_hf_jobs"):
        # Hyperparameters
        mlflow.log_params(
            {
                "stage": stage,
                "architecture": "encoder_only_transformer",
                "vocab_size": config.vocab_size,
                "hidden_dim": config.hidden_dim,
                "num_layers": config.num_layers,
                "num_heads": config.num_heads,
                "dropout": config.dropout,
                "max_seq_len": config.max_seq_len,
                "mask_prob": config.mask_prob,
                "spatial_mlp_dim": config.spatial_mlp_dim,
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
                "training_env": "hf_jobs_a10g_large",
                "dataset_commit": dataset_commit,
            }
        )

        if stage == "stage2":
            mlflow.log_params(
                {
                    "adversarial_lambda_max": ADVERSARIAL_LAMBDA_MAX,
                    "adversarial_warmup_epochs": ADVERSARIAL_WARMUP_EPOCHS,
                    "adversary_target": "competition_id",
                }
            )

        # Final metrics
        for name, value in metrics.items():
            if isinstance(value, (int, float)):
                mlflow.log_metric(name, value)

        # Training history (per-epoch)
        for key, values in history.items():
            for epoch_idx, val in enumerate(values):
                mlflow.log_metric(key, val, step=epoch_idx)

        # Pyfunc wrapper for UC model registry
        class _Football2VecPyfuncWrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            """Thin wrapper for UC model registry signature requirement."""

            def predict(self, context: Any, model_input: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(model_input))  # placeholder

        mlflow.pyfunc.log_model(
            python_model=_Football2VecPyfuncWrapper(),
            artifact_path="football2vec_v2_model",
            registered_model_name="soccer_analytics.dev_gold.football2vec_v2",
            input_example=pd.DataFrame({"x": [0.0]}),
        )

        run_id = mlflow.active_run().info.run_id

    # Set @Champion alias
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions("name='soccer_analytics.dev_gold.football2vec_v2'")
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.set_registered_model_alias(
            name="soccer_analytics.dev_gold.football2vec_v2",
            alias="Champion",
            version=latest.version,
        )
        logger.info(
            "MLflow logging complete (version=%s, alias=@Champion, run=%s)",
            latest.version,
            run_id,
        )
    else:
        logger.warning("No model versions found — @Champion alias not set")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-football2vec-v2", phase="training")
def main() -> None:
    """Train Football2Vec v2: Stage 1 (MLM) or Stage 2 (adversarial debiasing)."""
    parser = argparse.ArgumentParser(description="Train Football2Vec v2 transformer")
    parser.add_argument(
        "--stage",
        type=int,
        choices=[1, 2],
        default=1,
        help="Stage 1 = MLM, Stage 2 = adversarial (default: 1)",
    )
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS, help=f"Max epochs (default: {DEFAULT_EPOCHS})")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--lr",
        type=float,
        default=DEFAULT_LR,
        help=f"Learning rate (default: {DEFAULT_LR})",
    )
    parser.add_argument(
        "--patience",
        type=int,
        default=DEFAULT_PATIENCE,
        help=f"Early stopping patience (default: {DEFAULT_PATIENCE})",
    )
    args = parser.parse_args()

    from huggingface_hub import get_token

    pipeline_start = time.time()

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        msg = "HF_TOKEN environment variable required"
        raise RuntimeError(msg)

    # Cost recorder
    recorder = HFJobsCostRecorder(
        workflow_id="wf-football2vec-v2",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
    recorder.start()

    # Device selection
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

    elapsed_total = time.time() - pipeline_start
    logger.info(
        "Football2Vec v2 Stage %d training complete in %.1f seconds",
        args.stage,
        elapsed_total,
    )


def _run_stage1(
    args: argparse.Namespace,
    hf_token: str,
    device: torch.device,
    recorder: HFJobsCostRecorder,
) -> None:
    """Execute Stage 1: MLM pre-training."""
    logger.info("=== Stage 1: Masked Language Model Pre-Training ===")

    # 1. Load data
    logger.info("=== Loading training data from HF Hub ===")
    data, dataset_commit = load_training_data(hf_token)

    # 2. Parse actions
    logger.info("=== Parsing action sequences ===")
    action_ids_all, x_coords_all, y_coords_all = _parse_actions(data["actions"])

    # Log sequence length statistics
    seq_lengths = [len(a) for a in action_ids_all]
    logger.info(
        "Sequence lengths: min=%d, median=%d, mean=%.1f, max=%d, >512=%d",
        min(seq_lengths) if seq_lengths else 0,
        int(np.median(seq_lengths)) if seq_lengths else 0,
        np.mean(seq_lengths) if seq_lengths else 0.0,
        max(seq_lengths) if seq_lengths else 0,
        sum(1 for s in seq_lengths if s > MAX_SEQ_LEN),
    )

    # 3. Train/val/test split
    logger.info("=== Splitting data (80/10/10 stratified by competition_id) ===")
    train_df, val_df, test_df = stratified_split(data)

    train_indices = train_df.index.tolist()
    val_indices = val_df.index.tolist()
    test_indices = test_df.index.tolist()

    logger.info("Split sizes: train=%d, val=%d, test=%d", len(train_indices), len(val_indices), len(test_indices))

    # Build per-split data
    train_aids = [action_ids_all[i] for i in train_indices]
    train_xs = [x_coords_all[i] for i in train_indices]
    train_ys = [y_coords_all[i] for i in train_indices]

    val_aids = [action_ids_all[i] for i in val_indices]
    val_xs = [x_coords_all[i] for i in val_indices]
    val_ys = [y_coords_all[i] for i in val_indices]

    # 4. Create datasets
    train_dataset = Football2VecDataset(train_aids, train_xs, train_ys, mlm=True)
    val_dataset = Football2VecDataset(val_aids, val_xs, val_ys, mlm=True)

    # 5. Train
    config = Football2VecConfig()
    model, history = train_stage1(
        train_dataset,
        val_dataset,
        config,
        device,
        args.epochs,
        args.batch_size,
        args.lr,
        args.patience,
    )

    # 6. Evaluate on test set
    logger.info("=== Evaluating on test set ===")
    test_aids = [action_ids_all[i] for i in test_indices]
    test_xs = [x_coords_all[i] for i in test_indices]
    test_ys = [y_coords_all[i] for i in test_indices]

    test_dataset = Football2VecDataset(test_aids, test_xs, test_ys, mlm=True)
    test_loader = DataLoader(
        test_dataset,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )
    test_loss, test_accuracy = _evaluate_mlm(model, test_loader, nn.CrossEntropyLoss(ignore_index=-100), config, device)
    logger.info("Test — loss=%.4f  accuracy=%.4f", test_loss, test_accuracy)

    # 7. Generate embeddings on full dataset
    logger.info("=== Generating embeddings (inference on all %d sequences) ===", len(data))
    embeddings_df = generate_embeddings(model, data, action_ids_all, x_coords_all, y_coords_all, device)

    # 8. Save checkpoint + embeddings
    logger.info("=== Saving Stage 1 checkpoint ===")
    metrics: dict[str, Any] = {
        "stage": "stage1",
        "test_loss": test_loss,
        "test_accuracy": test_accuracy,
        "best_val_loss": min(history["val_loss"]) if history["val_loss"] else None,
        "best_val_accuracy": max(history["val_accuracy"]) if history["val_accuracy"] else None,
        "actual_epochs": len(history["train_loss"]),
        "n_train": len(train_indices),
        "n_val": len(val_indices),
        "n_test": len(test_indices),
        "n_embeddings": len(embeddings_df),
        "embedding_dim": config.hidden_dim,
        "dataset_commit": dataset_commit,
        "config": asdict(config),
    }

    # Enrich with cost data
    metrics = recorder.complete(metrics, row_count=len(embeddings_df))

    save_checkpoint(model, config, "stage1", hf_token, metrics=metrics)
    publish_embeddings(embeddings_df, hf_token, "stage1")

    # 9. MLflow
    log_to_mlflow(
        "stage1",
        config,
        history,
        {"test_loss": test_loss, "test_accuracy": test_accuracy},
        model,
        args,
        dataset_commit,
        len(train_indices),
        len(val_indices),
        len(test_indices),
    )

    logger.info("Published model: https://huggingface.co/%s", MODEL_REPO)
    logger.info("Published embeddings: https://huggingface.co/datasets/%s", EMBEDDINGS_DATASET)


def _run_stage2(
    args: argparse.Namespace,
    hf_token: str,
    device: torch.device,
    recorder: HFJobsCostRecorder,
) -> None:
    """Execute Stage 2: Adversarial competition debiasing."""
    logger.info("=== Stage 2: Adversarial Competition Debiasing ===")

    # 1. Load data
    logger.info("=== Loading training data from HF Hub ===")
    data, dataset_commit = load_training_data(hf_token)

    # 2. Parse actions
    logger.info("=== Parsing action sequences ===")
    action_ids_all, x_coords_all, y_coords_all = _parse_actions(data["actions"])

    # 3. Build competition label mapping
    unique_competitions = sorted(data["competition_id"].unique().tolist())
    competition_to_idx: dict[int, int] = {comp: idx for idx, comp in enumerate(unique_competitions)}
    num_competitions = len(unique_competitions)
    logger.info("Adversary target: competition_id (%d unique competitions)", num_competitions)
    logger.info("Competition mapping: %s", competition_to_idx)

    # Map all competition_ids to 0-indexed class labels
    comp_labels_all = [competition_to_idx[int(c)] for c in data["competition_id"].values]

    # 4. Load Stage 1 checkpoint
    logger.info("=== Loading Stage 1 checkpoint ===")
    config = Football2VecConfig()
    model = load_stage1_checkpoint(config, device, hf_token)

    # 5. Train/val/test split
    logger.info("=== Splitting data (80/10/10 stratified by competition_id) ===")
    train_df, val_df, test_df = stratified_split(data)

    train_indices = train_df.index.tolist()
    val_indices = val_df.index.tolist()
    test_indices = test_df.index.tolist()

    logger.info(
        "Split sizes: train=%d, val=%d, test=%d",
        len(train_indices),
        len(val_indices),
        len(test_indices),
    )

    # Build per-split data
    train_aids = [action_ids_all[i] for i in train_indices]
    train_xs = [x_coords_all[i] for i in train_indices]
    train_ys = [y_coords_all[i] for i in train_indices]
    train_comp_ids = [comp_labels_all[i] for i in train_indices]

    val_aids = [action_ids_all[i] for i in val_indices]
    val_xs = [x_coords_all[i] for i in val_indices]
    val_ys = [y_coords_all[i] for i in val_indices]
    val_comp_ids = [comp_labels_all[i] for i in val_indices]

    # 6. Create datasets with competition labels
    train_dataset = Football2VecDataset(train_aids, train_xs, train_ys, mlm=True, competition_ids=train_comp_ids)
    val_dataset = Football2VecDataset(val_aids, val_xs, val_ys, mlm=True, competition_ids=val_comp_ids)

    # 7. Train with adversarial debiasing
    model, adversary, history = train_stage2(
        model,
        train_dataset,
        val_dataset,
        num_competitions,
        competition_to_idx,
        config,
        device,
        args.epochs,
        args.batch_size,
        args.lr,
        args.patience,
    )

    # 8. Evaluate debiasing effectiveness on test set
    logger.info("=== Evaluating debiasing on test set ===")
    test_aids = [action_ids_all[i] for i in test_indices]
    test_xs = [x_coords_all[i] for i in test_indices]
    test_ys = [y_coords_all[i] for i in test_indices]
    test_comp_ids = [comp_labels_all[i] for i in test_indices]

    test_dataset = Football2VecDataset(test_aids, test_xs, test_ys, mlm=True, competition_ids=test_comp_ids)
    test_loader = DataLoader(
        test_dataset,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    test_mlm_loss, test_adv_accuracy = _evaluate_stage2(
        model,
        adversary,
        test_loader,
        nn.CrossEntropyLoss(ignore_index=-100),
        config,
        device,
    )
    chance_level = 1.0 / num_competitions
    logger.info(
        "Test — mlm_loss=%.4f  adv_accuracy=%.4f  chance_level=%.4f",
        test_mlm_loss,
        test_adv_accuracy,
        chance_level,
    )
    logger.info(
        "Debiasing effectiveness: accuracy %.4f vs chance %.4f (ratio=%.2f)",
        test_adv_accuracy,
        chance_level,
        test_adv_accuracy / chance_level if chance_level > 0 else float("inf"),
    )

    # 9. Generate debiased embeddings on full dataset
    logger.info("=== Generating debiased embeddings (inference on all %d sequences) ===", len(data))
    embeddings_df = generate_embeddings(model, data, action_ids_all, x_coords_all, y_coords_all, device)

    # 10. Save checkpoint + embeddings (Stage 2 is the released model)
    logger.info("=== Saving Stage 2 checkpoint (released model) ===")
    metrics: dict[str, Any] = {
        "stage": "stage2",
        "test_mlm_loss": test_mlm_loss,
        "test_adv_accuracy": test_adv_accuracy,
        "chance_level": chance_level,
        "debiasing_ratio": test_adv_accuracy / chance_level if chance_level > 0 else None,
        "num_competitions": num_competitions,
        "competition_mapping": competition_to_idx,
        "adversarial_lambda_max": ADVERSARIAL_LAMBDA_MAX,
        "adversarial_warmup_epochs": ADVERSARIAL_WARMUP_EPOCHS,
        "actual_epochs": len(history["train_mlm_loss"]),
        "n_train": len(train_indices),
        "n_val": len(val_indices),
        "n_test": len(test_indices),
        "n_embeddings": len(embeddings_df),
        "embedding_dim": config.hidden_dim,
        "dataset_commit": dataset_commit,
        "config": asdict(config),
    }

    metrics = recorder.complete(metrics, row_count=len(embeddings_df))

    save_checkpoint(model, config, "stage2", hf_token, metrics=metrics)
    publish_embeddings(embeddings_df, hf_token, "stage2")

    # 11. MLflow
    log_to_mlflow(
        "stage2",
        config,
        history,
        {
            "test_mlm_loss": test_mlm_loss,
            "test_adv_accuracy": test_adv_accuracy,
            "chance_level": chance_level,
        },
        model,
        args,
        dataset_commit,
        len(train_indices),
        len(val_indices),
        len(test_indices),
    )

    logger.info("Published model: https://huggingface.co/%s", MODEL_REPO)
    logger.info("Published embeddings: https://huggingface.co/datasets/%s", EMBEDDINGS_DATASET)


if __name__ == "__main__":
    main()
