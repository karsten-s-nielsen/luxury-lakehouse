# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
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

128d transformer + 16d Deep Sets context = 144d output embeddings.
Stage 1: MLM training with 360 freeze frame context.
Stage 2: Adversarial team debiasing via gradient reversal (Ganin et al. 2016).

Optionally loads pretrained Football2Vec v2 weights into the transformer branch
before Stage 1 to warm-start training.

References:
    Danesi, P. (2025). "Football2Vec: Transformer-Based Player Embeddings."
    Ganin, Y. et al. (2016). "Domain-Adversarial Training of Neural Networks."
        JMLR 17(1), pp. 1-35.
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
    Decroos, T. et al. (2019). "Actions Speak Louder than Goals: Valuing
        Player Actions in Soccer." KDD.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_football2vec_360.py --stage 1 \\
        --flavor a10g-small --timeout 120m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN

    hf jobs uv run scripts/train_football2vec_360.py --stage 2 \\
        --flavor a10g-small --timeout 120m \\
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

from analytics.cost import HF_RATE_A10G_SMALL, HFJobsCostRecorder
from analytics.football2vec_360 import Football2Vec360Config, Football2Vec360Encoder
from analytics.football2vec_transformer import TeamClassifierHead
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
INPUT_DATASET = f"{HF_ORG}/football2vec-360-training-data"
OUTPUT_MODEL = f"{HF_ORG}/football2vec-360"
OUTPUT_EMBEDDINGS = f"{HF_ORG}/football2vec-360-embeddings"
PRETRAINED_MODEL = f"{HF_ORG}/football2vec-v2"

# SPADL 23-type action vocabulary (mirrors export_embeddings_training_data.py)
VOCAB_SIZE = 23
MASK_TOKEN_ID = VOCAB_SIZE  # 23 — dedicated mask token (outside vocab)
PAD_TOKEN_ID = VOCAB_SIZE + 1  # 24 — padding token

# Output embedding dimension: 128d transformer + 16d Deep Sets
OUTPUT_DIM = 144

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

# Maximum number of players in a 360 freeze frame per action
MAX_PLAYERS = 22


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_training_data(hf_token: str) -> tuple[pd.DataFrame, str]:
    """Download 360-enriched training data from HF Hub and return DataFrame + commit hash.

    The HF dataset contains Parquet files with columns:
        canonical_player_id, match_id, competition_id, season_id,
        position_group, actions (array of struct), freeze_frames (array of array of struct)

    Each element of freeze_frames corresponds to one action and contains a list of
    player feature structs with keys: x, y, is_keeper, is_teammate.

    Returns:
        Tuple of (DataFrame, dataset_commit_sha).
    """
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)

    all_items = list(api.list_repo_tree(INPUT_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {INPUT_DATASET}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(INPUT_DATASET, pf, repo_type="dataset", token=hf_token)
        table = pq.read_table(local_path)
        df = table.to_pandas()
        dfs.append(df)
        logger.info("  %s: %d rows", pf, len(df))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total player-match sequences: %d", len(data))

    dataset_info = api.repo_info(repo_id=INPUT_DATASET, repo_type="dataset")
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
            action_ids.append(int(act["action_type"]))
            x_coords.append(float(act["x"]))
            y_coords.append(float(act["y"]))

        all_action_ids.append(action_ids)
        all_x_coords.append(x_coords)
        all_y_coords.append(y_coords)

    return all_action_ids, all_x_coords, all_y_coords


def _parse_freeze_frames(
    freeze_frames_col: pd.Series,
    max_seq_len: int = MAX_SEQ_LEN,
    max_players: int = MAX_PLAYERS,
) -> list[list[list[list[float]]]]:
    """Parse the freeze_frames column into nested float lists.

    Each row is a list (one element per action) of player lists. Each player
    has 4 features: [x, y, is_keeper, is_teammate].

    Args:
        freeze_frames_col: Series where each element is a list of per-action
            player arrays (list[list[dict]] or list[None]).
        max_seq_len: Truncate to this many actions per sequence.
        max_players: Maximum players per action (pad/truncate player axis).

    Returns:
        Nested list: [row][action_idx][player_idx][feature_idx]
    """
    all_frames: list[list[list[list[float]]]] = []

    for frames_per_seq in freeze_frames_col:
        # frames_per_seq: list of per-action player lists, or None
        if frames_per_seq is None or (hasattr(frames_per_seq, "__len__") and len(frames_per_seq) == 0):
            all_frames.append([])
            continue

        seq_frames: list[list[list[float]]] = []
        for action_players in frames_per_seq[:max_seq_len]:
            players: list[list[float]] = []
            if action_players is not None:
                for p in action_players[:max_players]:
                    players.append(
                        [
                            float(p["x"]),
                            float(p["y"]),
                            float(p["is_keeper"]),
                            float(p["is_teammate"]),
                        ]
                    )
            seq_frames.append(players)

        all_frames.append(seq_frames)

    return all_frames


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class Football2Vec360Dataset(Dataset[dict[str, torch.Tensor]]):
    """PyTorch dataset for 360-enriched SPADL action sequences with MLM masking.

    Extends v2 dataset with a freeze_frames tensor encoding per-action 360
    context (player positions from StatsBomb 360 data). Each action position
    carries a variable-size set of players, zero-padded to max_players.

    Args:
        action_ids: List of per-row action ID sequences.
        x_coords: List of per-row normalized x coordinate sequences.
        y_coords: List of per-row normalized y coordinate sequences.
        freeze_frames: List of per-row 360 frames
            (list[list[list[list[float]]]] — [row][action][player][feature]).
        max_seq_len: Maximum sequence length (pad/truncate).
        max_players: Maximum players per action (pad/truncate player axis).
        mask_prob: Probability of masking each valid token for MLM.
        mlm: Whether to apply MLM masking (False for inference).
        competition_ids: Optional competition IDs for adversarial training.
    """

    def __init__(
        self,
        action_ids: list[list[int]],
        x_coords: list[list[float]],
        y_coords: list[list[float]],
        freeze_frames: list[list[list[list[float]]]] | None = None,
        max_seq_len: int = MAX_SEQ_LEN,
        max_players: int = MAX_PLAYERS,
        mask_prob: float = DEFAULT_MASK_PROB,
        *,
        mlm: bool = True,
        competition_ids: list[int] | None = None,
    ) -> None:
        self.action_ids = action_ids
        self.x_coords = x_coords
        self.y_coords = y_coords
        self.freeze_frames = freeze_frames
        self.max_seq_len = max_seq_len
        self.max_players = max_players
        self.mask_prob = mask_prob
        self.mlm = mlm
        self.competition_ids = competition_ids

    def __len__(self) -> int:
        return len(self.action_ids)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        """Return a single tokenized, padded, optionally masked 360-enriched sample."""
        aids = self.action_ids[idx]
        xs = self.x_coords[idx]
        ys = self.y_coords[idx]

        seq_len = min(len(aids), self.max_seq_len)

        # Truncate to max_seq_len
        aids = aids[:seq_len]
        xs = xs[:seq_len]
        ys = ys[:seq_len]

        # Build action tensors with padding
        action_tensor = torch.full((self.max_seq_len,), PAD_TOKEN_ID, dtype=torch.long)
        x_tensor = torch.zeros(self.max_seq_len, dtype=torch.float32)
        y_tensor = torch.zeros(self.max_seq_len, dtype=torch.float32)
        attention_mask = torch.zeros(self.max_seq_len, dtype=torch.bool)

        if seq_len > 0:
            action_tensor[:seq_len] = torch.tensor(aids, dtype=torch.long)
            x_tensor[:seq_len] = torch.tensor(xs, dtype=torch.float32)
            y_tensor[:seq_len] = torch.tensor(ys, dtype=torch.float32)
            attention_mask[:seq_len] = True

        # Build 360 freeze frame tensor: (max_seq_len, max_players, 4)
        # Zero = no player present at that position
        freeze_tensor = torch.zeros(self.max_seq_len, self.max_players, 4, dtype=torch.float32)

        if self.freeze_frames is not None:
            frames = self.freeze_frames[idx]
            for action_idx, players in enumerate(frames[:seq_len]):
                for player_idx, feat in enumerate(players[: self.max_players]):
                    freeze_tensor[action_idx, player_idx] = torch.tensor(feat, dtype=torch.float32)

        result: dict[str, torch.Tensor] = {
            "action_ids": action_tensor,
            "x_coords": x_tensor,
            "y_coords": y_tensor,
            "attention_mask": attention_mask,
            "freeze_frames": freeze_tensor,
        }

        if self.mlm and seq_len > 0:
            labels = torch.full((self.max_seq_len,), -100, dtype=torch.long)
            mask_candidates = torch.arange(seq_len)
            n_mask = max(1, int(seq_len * self.mask_prob))
            mask_indices = mask_candidates[torch.randperm(seq_len)[:n_mask]]

            labels[mask_indices] = action_tensor[mask_indices].clone()
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

    Handles rare competitions (less than 3 samples) by merging them into
    "_other_" for stratification purposes. The actual competition_id is preserved.

    Args:
        data: Full DataFrame with competition_id column.
        train_frac: Training fraction (default 0.80).
        val_frac: Validation fraction (default 0.10).

    Returns:
        Tuple of (train_df, val_df, test_df).
    """
    from sklearn.model_selection import train_test_split

    stratify_col = data["competition_id"].astype(str)
    counts = stratify_col.value_counts()
    rare_mask = stratify_col.isin(counts[counts < 3].index)
    stratify_col = stratify_col.copy()
    stratify_col.loc[rare_mask] = "_other_"

    indices = np.arange(len(data))

    test_frac = 1.0 - train_frac - val_frac
    train_val_idx, test_idx = train_test_split(
        indices,
        test_size=test_frac,
        random_state=RANDOM_STATE,
        stratify=stratify_col,
    )

    val_relative = val_frac / (train_frac + val_frac)
    stratify_trainval = stratify_col.iloc[train_val_idx]
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
# MLM wrapper for 360 encoder
# ---------------------------------------------------------------------------


class _MLMHead(nn.Module):
    """Per-token MLM prediction head applied to transformer outputs.

    Projects (batch, seq_len, hidden_dim) token representations to
    (batch, seq_len, vocab_size) logits for masked language modeling.

    Args:
        hidden_dim: Transformer hidden dimension.
        vocab_size: Number of SPADL action types to predict.
    """

    def __init__(self, hidden_dim: int, vocab_size: int) -> None:
        super().__init__()
        self.head = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
            nn.LayerNorm(hidden_dim),
            nn.Linear(hidden_dim, vocab_size),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Apply MLM head to per-token transformer outputs.

        Args:
            x: (batch, seq_len, hidden_dim) transformer output.

        Returns:
            (batch, seq_len, vocab_size) logits.
        """
        return self.head(x)  # type: ignore[no-any-return]


def _mlm_forward_360(
    model: Football2Vec360Encoder,
    mlm_head: _MLMHead,
    action_ids: torch.Tensor,
    x_coords: torch.Tensor,
    y_coords: torch.Tensor,
    attention_mask: torch.Tensor | None,
    freeze_frames: torch.Tensor | None,
) -> torch.Tensor:
    """Compute per-token MLM logits using the 360 encoder's transformer branch.

    Args:
        model: The 360 encoder (provides _embed and transformer attributes).
        mlm_head: The MLM prediction head.
        action_ids: (batch, seq_len) action type indices.
        x_coords: (batch, seq_len) normalized x coordinates.
        y_coords: (batch, seq_len) normalized y coordinates.
        attention_mask: (batch, seq_len) bool tensor. True = valid.
        freeze_frames: (batch, seq_len, max_players, 4) freeze frame context,
            or None if not available.

    Returns:
        (batch, seq_len, vocab_size) MLM logits.
    """
    # Embed action tokens + spatial features
    embedded = model._embed(action_ids, x_coords, y_coords)

    src_key_padding_mask: torch.Tensor | None = None
    if attention_mask is not None:
        src_key_padding_mask = ~attention_mask

    # Run transformer encoder to get per-token representations
    encoded = model.transformer(embedded, src_key_padding_mask=src_key_padding_mask)

    # MLM logits from per-token representations
    return mlm_head(encoded)


# ---------------------------------------------------------------------------
# Stage 1: MLM Training
# ---------------------------------------------------------------------------


def train_stage1(
    train_dataset: Football2Vec360Dataset,
    val_dataset: Football2Vec360Dataset,
    config: Football2Vec360Config,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2Vec360Encoder, _MLMHead, dict[str, list[float]]]:
    """Train the 360 encoder with masked language modeling.

    The MLM objective targets only the transformer branch (action sequences),
    while the Deep Sets branch is trained jointly on available 360 context.
    An expanded token embedding accommodates the mask token.

    Args:
        train_dataset: Training dataset with MLM masking and freeze frames.
        val_dataset: Validation dataset with MLM masking and freeze frames.
        config: 360 encoder configuration.
        device: torch device (cuda or cpu).
        epochs: Maximum number of epochs.
        batch_size: Batch size.
        lr: Learning rate.
        patience: Early stopping patience.

    Returns:
        Tuple of (trained_model, mlm_head, training_history).
    """
    model = Football2Vec360Encoder(config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    # Expand token embedding to accommodate MASK_TOKEN_ID and PAD_TOKEN_ID
    expanded_embed = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim).to(device)
    with torch.no_grad():
        expanded_embed.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded_embed

    mlm_head = _MLMHead(config.hidden_dim, config.vocab_size).to(device)
    logger.info("MLM head parameters: %d", sum(p.numel() for p in mlm_head.parameters()))

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

    all_params = list(model.parameters()) + list(mlm_head.parameters())
    optimizer = torch.optim.AdamW(all_params, lr=lr, weight_decay=WEIGHT_DECAY)

    total_steps = len(train_loader) * epochs
    warmup_steps = int(total_steps * WARMUP_FRACTION)
    scheduler = get_cosine_schedule_with_warmup(optimizer, warmup_steps, total_steps)

    criterion = nn.CrossEntropyLoss(ignore_index=-100)

    best_val_loss = float("inf")
    patience_counter = 0
    best_model_state: dict[str, Any] = {}
    best_mlm_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {
        "train_loss": [],
        "val_loss": [],
        "val_accuracy": [],
    }

    for epoch in range(epochs):
        epoch_start = time.time()

        # --- Training ---
        model.train()
        mlm_head.train()
        total_loss = 0.0
        n_batches = 0

        for batch in train_loader:
            action_ids = batch["action_ids"].to(device)
            x_coords = batch["x_coords"].to(device)
            y_coords = batch["y_coords"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            freeze_frames = batch["freeze_frames"].to(device)

            optimizer.zero_grad()

            logits = _mlm_forward_360(model, mlm_head, action_ids, x_coords, y_coords, attention_mask, freeze_frames)
            loss = criterion(logits.view(-1, config.vocab_size), labels.view(-1))

            loss.backward()
            torch.nn.utils.clip_grad_norm_(all_params, max_norm=1.0)
            optimizer.step()
            scheduler.step()

            total_loss += loss.item()
            n_batches += 1

        avg_train_loss = total_loss / max(n_batches, 1)

        # --- Validation ---
        val_loss, val_accuracy = _evaluate_mlm(model, mlm_head, val_loader, criterion, config, device)

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
    mlm_head: _MLMHead,
    val_loader: DataLoader[dict[str, torch.Tensor]],
    criterion: nn.CrossEntropyLoss,
    config: Football2Vec360Config,
    device: torch.device,
) -> tuple[float, float]:
    """Evaluate MLM loss and accuracy on masked tokens.

    Returns:
        Tuple of (avg_val_loss, masked_token_accuracy).
    """
    model.eval()
    mlm_head.eval()
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
            freeze_frames = batch["freeze_frames"].to(device)

            logits = _mlm_forward_360(model, mlm_head, action_ids, x_coords, y_coords, attention_mask, freeze_frames)
            loss = criterion(logits.view(-1, config.vocab_size), labels.view(-1))

            total_loss += loss.item()
            n_batches += 1

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
    model: Football2Vec360Encoder,
    train_dataset: Football2Vec360Dataset,
    val_dataset: Football2Vec360Dataset,
    num_competitions: int,
    competition_to_idx: dict[int, int],
    config: Football2Vec360Config,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
) -> tuple[Football2Vec360Encoder, TeamClassifierHead, dict[str, list[float]]]:
    """Fine-tune with adversarial competition debiasing.

    Combines MLM loss with a gradient-reversed competition classifier.
    Lambda ramps linearly from 0 to ADVERSARIAL_LAMBDA_MAX over the first
    ADVERSARIAL_WARMUP_EPOCHS epochs. The adversary operates on 144d embeddings
    (128d transformer + 16d Deep Sets).

    Args:
        model: Pre-trained Stage 1 encoder.
        train_dataset: Training dataset (must have competition_ids set).
        val_dataset: Validation dataset (must have competition_ids set).
        num_competitions: Number of unique competitions (classifier output dim).
        competition_to_idx: Mapping from competition_id to 0-indexed class label.
        config: 360 encoder configuration.
        device: torch device.
        epochs: Maximum epochs.
        batch_size: Batch size.
        lr: Learning rate.
        patience: Early stopping patience.

    Returns:
        Tuple of (fine_tuned_encoder, classifier_head, training_history).
    """
    model = model.to(device)

    # Load a fresh MLM head for Stage 2 joint training
    mlm_head = _MLMHead(config.hidden_dim, config.vocab_size).to(device)

    # Adversary operates on 144d output (hidden_dim + context_dim)
    adversary = TeamClassifierHead(
        hidden_dim=config.hidden_dim + config.context_dim,
        num_teams=num_competitions,
        lambda_val=0.0,
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

    all_params = list(model.parameters()) + list(mlm_head.parameters()) + list(adversary.parameters())
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

        adversary.grl.lambda_val = current_lambda

        # --- Training ---
        model.train()
        mlm_head.train()
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
            freeze_frames = batch["freeze_frames"].to(device)
            comp_ids = batch["competition_id"].to(device)

            optimizer.zero_grad()

            # MLM forward (transformer branch only)
            mlm_logits = _mlm_forward_360(
                model, mlm_head, action_ids, x_coords, y_coords, attention_mask, freeze_frames
            )
            mlm_loss = mlm_criterion(mlm_logits.view(-1, config.vocab_size), labels.view(-1))

            # 144d sequence embedding for adversary
            embeddings = model(action_ids, x_coords, y_coords, attention_mask, context_360=freeze_frames)
            adv_logits = adversary(embeddings)
            adv_loss = adv_criterion(adv_logits, comp_ids)

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
        val_mlm_loss, val_adv_accuracy = _evaluate_stage2(
            model, mlm_head, adversary, val_loader, mlm_criterion, config, device
        )
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
    model: Football2Vec360Encoder,
    mlm_head: _MLMHead,
    adversary: TeamClassifierHead,
    val_loader: DataLoader[dict[str, torch.Tensor]],
    mlm_criterion: nn.CrossEntropyLoss,
    config: Football2Vec360Config,
    device: torch.device,
) -> tuple[float, float]:
    """Evaluate Stage 2: MLM loss + adversary accuracy.

    Returns:
        Tuple of (avg_mlm_loss, competition_classification_accuracy).
    """
    model.eval()
    mlm_head.eval()
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
            freeze_frames = batch["freeze_frames"].to(device)
            comp_ids = batch["competition_id"].to(device)

            mlm_logits = _mlm_forward_360(
                model, mlm_head, action_ids, x_coords, y_coords, attention_mask, freeze_frames
            )
            mlm_loss = mlm_criterion(mlm_logits.view(-1, config.vocab_size), labels.view(-1))
            total_mlm_loss += mlm_loss.item()

            embeddings = model(action_ids, x_coords, y_coords, attention_mask, context_360=freeze_frames)
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
    model: Football2Vec360Encoder,
    data: pd.DataFrame,
    action_ids_all: list[list[int]],
    x_coords_all: list[list[float]],
    y_coords_all: list[list[float]],
    freeze_frames_all: list[list[list[list[float]]]] | None,
    device: torch.device,
    batch_size: int = 512,
) -> pd.DataFrame:
    """Run inference on all data to produce 144d embeddings.

    Args:
        model: Trained 360 encoder.
        data: Full DataFrame with canonical_player_id, match_id columns.
        action_ids_all: All action ID sequences.
        x_coords_all: All x coordinate sequences.
        y_coords_all: All y coordinate sequences.
        freeze_frames_all: All 360 freeze frame data, or None.
        device: torch device.
        batch_size: Inference batch size.

    Returns:
        DataFrame with canonical_player_id, match_id, behavioral_vector columns
        (behavioral_vector is a 144d list).
    """
    model.eval()

    infer_dataset = Football2Vec360Dataset(
        action_ids_all,
        x_coords_all,
        y_coords_all,
        freeze_frames=freeze_frames_all,
        mlm=False,
    )
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
            freeze_frames = batch["freeze_frames"].to(device)

            embeddings = model(action_ids, x_coords, y_coords, attention_mask, context_360=freeze_frames)
            all_embeddings.append(embeddings.cpu().numpy())

    embeddings_array = np.concatenate(all_embeddings, axis=0)
    logger.info("Generated embeddings shape: %s", embeddings_array.shape)

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


def _try_load_pretrained_transformer(
    model: Football2Vec360Encoder,
    config: Football2Vec360Config,
    device: torch.device,
    hf_token: str,
) -> Football2Vec360Encoder:
    """Optionally warm-start transformer weights from football2vec-v2 Stage 2.

    Loads matching parameter keys from the pretrained v2 checkpoint into the
    360 encoder's transformer branch. Skips gracefully if the pretrained model
    is unavailable or parameters don't align.

    Args:
        model: Freshly initialized 360 encoder.
        config: 360 encoder configuration.
        device: torch device.
        hf_token: HF Hub token.

    Returns:
        Model with pretrained weights loaded (or unchanged if load fails).
    """
    try:
        from huggingface_hub import hf_hub_download
        from safetensors.torch import load_file as _load_safetensors

        logger.info("Attempting to warm-start from %s/stage2/model.safetensors ...", PRETRAINED_MODEL)
        local_path = hf_hub_download(
            PRETRAINED_MODEL,
            "stage2/model.safetensors",
            repo_type="model",
            token=hf_token,
        )
        pretrained_state = _load_safetensors(local_path, device=str(device))

        # Map v2 keys (encoder.*) to 360 keys (transformer.*) and load matching params
        current_state = model.state_dict()
        loaded = 0
        for k, v in pretrained_state.items():
            # v2 uses "encoder." prefix for the TransformerEncoder; 360 uses "transformer."
            remapped_key = k.replace("encoder.", "transformer.", 1) if k.startswith("encoder.") else k
            if remapped_key in current_state and current_state[remapped_key].shape == v.shape:
                current_state[remapped_key] = v
                loaded += 1

        model.load_state_dict(current_state)
        logger.info("Warm-started %d/%d parameters from %s", loaded, len(pretrained_state), PRETRAINED_MODEL)

    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Pretrained weight loading skipped: %s", exc)

    return model


def save_checkpoint(
    model: Football2Vec360Encoder,
    config: Football2Vec360Config,
    stage: str,
    hf_token: str,
    metrics: dict[str, Any] | None = None,
) -> None:
    """Save 360 model checkpoint to HF Hub in safetensors format.

    Saves model.safetensors and config.json under the stage subdirectory.

    Args:
        model: Trained 360 encoder.
        config: 360 encoder configuration.
        stage: "stage1" or "stage2".
        hf_token: HF Hub token.
        metrics: Optional metrics dict to save alongside.
    """
    from huggingface_hub import HfApi
    from safetensors.torch import save_file as _save_safetensors

    api = HfApi(token=hf_token)
    api.create_repo(OUTPUT_MODEL, exist_ok=True, repo_type="model", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = os.path.join(tmpdir, "model.safetensors")
        _save_safetensors(model.state_dict(), state_path)

        config_path = os.path.join(tmpdir, "config.json")
        config_dict = asdict(config)
        config_dict["_expanded_vocab_size"] = VOCAB_SIZE + 2
        config_dict["_mask_token_id"] = MASK_TOKEN_ID
        config_dict["_pad_token_id"] = PAD_TOKEN_ID
        config_dict["_output_dim"] = OUTPUT_DIM
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_dict, f, indent=2)

        api.upload_file(
            path_or_fileobj=state_path,
            path_in_repo=f"{stage}/model.safetensors",
            repo_id=OUTPUT_MODEL,
            repo_type="model",
            token=hf_token,
        )
        api.upload_file(
            path_or_fileobj=config_path,
            path_in_repo=f"{stage}/config.json",
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

    logger.info("Saved checkpoint to %s/%s/", OUTPUT_MODEL, stage)


def load_stage1_checkpoint(
    config: Football2Vec360Config,
    device: torch.device,
    hf_token: str,
) -> Football2Vec360Encoder:
    """Load Stage 1 checkpoint from HF Hub.

    Args:
        config: 360 encoder configuration.
        device: torch device.
        hf_token: HF Hub token.

    Returns:
        Loaded Football2Vec360Encoder with expanded token embedding.
    """
    from huggingface_hub import hf_hub_download
    from safetensors.torch import load_file as _load_safetensors

    model = Football2Vec360Encoder(config)

    expanded_embed = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim)
    with torch.no_grad():
        expanded_embed.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded_embed

    local_path = hf_hub_download(
        OUTPUT_MODEL,
        "stage1/model.safetensors",
        repo_type="model",
        token=hf_token,
    )
    state_dict = _load_safetensors(local_path, device=str(device))
    model.load_state_dict(state_dict)
    model = model.to(device)

    logger.info("Loaded Stage 1 checkpoint from %s", OUTPUT_MODEL)
    return model


def publish_embeddings(
    embeddings_df: pd.DataFrame,
    hf_token: str,
    stage: str,
) -> None:
    """Publish 144d embeddings DataFrame to HF Hub as Parquet.

    Args:
        embeddings_df: DataFrame with canonical_player_id, match_id,
            behavioral_vector columns.
        hf_token: HF Hub token.
        stage: "stage1" or "stage2" (for commit message).
    """
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)
    api.create_repo(OUTPUT_EMBEDDINGS, exist_ok=True, repo_type="dataset", token=hf_token)

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = os.path.join(tmpdir, "embeddings_360.parquet")
        embeddings_df.to_parquet(parquet_path, index=False)

        api.upload_file(
            path_or_fileobj=parquet_path,
            path_in_repo="data/embeddings_360.parquet",
            repo_id=OUTPUT_EMBEDDINGS,
            repo_type="dataset",
            token=hf_token,
            commit_message=f"Update 360 embeddings ({stage})",
        )

    logger.info(
        "Published %d embeddings to %s (data/embeddings_360.parquet)",
        len(embeddings_df),
        OUTPUT_EMBEDDINGS,
    )


# ---------------------------------------------------------------------------
# MLflow logging
# ---------------------------------------------------------------------------


def log_to_mlflow(
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
    """Log training run to MLflow if MLFLOW_TRACKING_URI is set.

    Args:
        stage: "stage1" or "stage2".
        config: 360 encoder configuration.
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
                "training_env": "hf_jobs_a10g_small",
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
            for epoch_idx, val in enumerate(values):
                mlflow.log_metric(key, val, step=epoch_idx)

        class _Football2Vec360PyfuncWrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            """Thin wrapper for UC model registry signature requirement."""

            def predict(self, context: Any, model_input: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(model_input))

        mlflow.pyfunc.log_model(
            python_model=_Football2Vec360PyfuncWrapper(),
            artifact_path="football2vec_360_model",
            registered_model_name="soccer_analytics.dev_gold.football2vec_360",
            input_example=pd.DataFrame({"x": [0.0]}),
        )

        run_id = mlflow.active_run().info.run_id

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions("name='soccer_analytics.dev_gold.football2vec_360'")
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.set_registered_model_alias(
            name="soccer_analytics.dev_gold.football2vec_360",
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


@workflow("wf-football2vec-360", phase="training")
def main() -> None:
    """Train Football2Vec 360: Stage 1 (MLM) or Stage 2 (adversarial debiasing)."""
    parser = argparse.ArgumentParser(description="Train Football2Vec 360-enriched transformer")
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
    parser.add_argument(
        "--no-pretrained",
        action="store_true",
        help="Skip warm-starting from football2vec-v2 pretrained weights (Stage 1 only)",
    )
    args = parser.parse_args()

    from huggingface_hub import get_token

    pipeline_start = time.time()

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        msg = "HF_TOKEN environment variable required"
        raise RuntimeError(msg)

    recorder = HFJobsCostRecorder(
        workflow_id="wf-football2vec-360",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_SMALL,
        repo_id=OUTPUT_MODEL,
        repo_type="model",
    )
    recorder.start()

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
        "Football2Vec 360 Stage %d training complete in %.1f seconds",
        args.stage,
        elapsed_total,
    )


def _run_stage1(
    args: argparse.Namespace,
    hf_token: str,
    device: torch.device,
    recorder: HFJobsCostRecorder,
) -> None:
    """Execute Stage 1: MLM pre-training with 360 context."""
    logger.info("=== Stage 1: Masked Language Model Pre-Training (360-enriched) ===")

    # 1. Load data
    logger.info("=== Loading training data from HF Hub ===")
    data, dataset_commit = load_training_data(hf_token)

    # 2. Parse actions and 360 freeze frames
    logger.info("=== Parsing action sequences and 360 freeze frames ===")
    action_ids_all, x_coords_all, y_coords_all = _parse_actions(data["actions"])
    freeze_frames_all = _parse_freeze_frames(data["freeze_frames"])

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

    train_aids = [action_ids_all[i] for i in train_indices]
    train_xs = [x_coords_all[i] for i in train_indices]
    train_ys = [y_coords_all[i] for i in train_indices]
    train_ffs = [freeze_frames_all[i] for i in train_indices]

    val_aids = [action_ids_all[i] for i in val_indices]
    val_xs = [x_coords_all[i] for i in val_indices]
    val_ys = [y_coords_all[i] for i in val_indices]
    val_ffs = [freeze_frames_all[i] for i in val_indices]

    # 4. Create datasets
    train_dataset = Football2Vec360Dataset(train_aids, train_xs, train_ys, freeze_frames=train_ffs, mlm=True)
    val_dataset = Football2Vec360Dataset(val_aids, val_xs, val_ys, freeze_frames=val_ffs, mlm=True)

    # 5. Initialize model — optionally warm-start from pretrained v2
    config = Football2Vec360Config()
    model = Football2Vec360Encoder(config)

    expanded_embed = nn.Embedding(VOCAB_SIZE + 2, config.hidden_dim)
    with torch.no_grad():
        expanded_embed.weight[:VOCAB_SIZE] = model.token_embedding.weight
    model.token_embedding = expanded_embed

    if not args.no_pretrained:
        model = _try_load_pretrained_transformer(model, config, device, hf_token)
    model = model.to(device)

    # 6. Train
    model, mlm_head, history = train_stage1(
        train_dataset,
        val_dataset,
        config,
        device,
        args.epochs,
        args.batch_size,
        args.lr,
        args.patience,
    )

    # 7. Evaluate on test set
    logger.info("=== Evaluating on test set ===")
    test_aids = [action_ids_all[i] for i in test_indices]
    test_xs = [x_coords_all[i] for i in test_indices]
    test_ys = [y_coords_all[i] for i in test_indices]
    test_ffs = [freeze_frames_all[i] for i in test_indices]

    test_dataset = Football2Vec360Dataset(test_aids, test_xs, test_ys, freeze_frames=test_ffs, mlm=True)
    test_loader = DataLoader(
        test_dataset,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )
    test_loss, test_accuracy = _evaluate_mlm(
        model, mlm_head, test_loader, nn.CrossEntropyLoss(ignore_index=-100), config, device
    )
    logger.info("Test — loss=%.4f  accuracy=%.4f", test_loss, test_accuracy)

    # 8. Generate 144d embeddings on full dataset
    logger.info("=== Generating 144d embeddings (inference on all %d sequences) ===", len(data))
    embeddings_df = generate_embeddings(
        model, data, action_ids_all, x_coords_all, y_coords_all, freeze_frames_all, device
    )

    # 9. Save checkpoint + embeddings
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
        "embedding_dim": OUTPUT_DIM,
        "dataset_commit": dataset_commit,
        "config": asdict(config),
    }

    metrics = recorder.complete(metrics, row_count=len(embeddings_df))

    save_checkpoint(model, config, "stage1", hf_token, metrics=metrics)
    publish_embeddings(embeddings_df, hf_token, "stage1")

    # 10. MLflow
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

    logger.info("Published model: https://huggingface.co/%s", OUTPUT_MODEL)
    logger.info("Published embeddings: https://huggingface.co/datasets/%s", OUTPUT_EMBEDDINGS)


def _run_stage2(
    args: argparse.Namespace,
    hf_token: str,
    device: torch.device,
    recorder: HFJobsCostRecorder,
) -> None:
    """Execute Stage 2: Adversarial competition debiasing."""
    logger.info("=== Stage 2: Adversarial Competition Debiasing (360-enriched) ===")

    # 1. Load data
    logger.info("=== Loading training data from HF Hub ===")
    data, dataset_commit = load_training_data(hf_token)

    # 2. Parse actions and 360 freeze frames
    logger.info("=== Parsing action sequences and 360 freeze frames ===")
    action_ids_all, x_coords_all, y_coords_all = _parse_actions(data["actions"])
    freeze_frames_all = _parse_freeze_frames(data["freeze_frames"])

    # 3. Build competition label mapping
    unique_competitions = sorted(data["competition_id"].unique().tolist())
    competition_to_idx: dict[int, int] = {comp: idx for idx, comp in enumerate(unique_competitions)}
    num_competitions = len(unique_competitions)
    logger.info("Adversary target: competition_id (%d unique competitions)", num_competitions)
    logger.info("Competition mapping: %s", competition_to_idx)

    comp_labels_all = [competition_to_idx[int(c)] for c in data["competition_id"].values]

    # 4. Load Stage 1 checkpoint
    logger.info("=== Loading Stage 1 checkpoint ===")
    config = Football2Vec360Config()
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

    train_aids = [action_ids_all[i] for i in train_indices]
    train_xs = [x_coords_all[i] for i in train_indices]
    train_ys = [y_coords_all[i] for i in train_indices]
    train_ffs = [freeze_frames_all[i] for i in train_indices]
    train_comp_ids = [comp_labels_all[i] for i in train_indices]

    val_aids = [action_ids_all[i] for i in val_indices]
    val_xs = [x_coords_all[i] for i in val_indices]
    val_ys = [y_coords_all[i] for i in val_indices]
    val_ffs = [freeze_frames_all[i] for i in val_indices]
    val_comp_ids = [comp_labels_all[i] for i in val_indices]

    # 6. Create datasets with competition labels and freeze frames
    train_dataset = Football2Vec360Dataset(
        train_aids, train_xs, train_ys, freeze_frames=train_ffs, mlm=True, competition_ids=train_comp_ids
    )
    val_dataset = Football2Vec360Dataset(
        val_aids, val_xs, val_ys, freeze_frames=val_ffs, mlm=True, competition_ids=val_comp_ids
    )

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
    test_ffs = [freeze_frames_all[i] for i in test_indices]
    test_comp_ids = [comp_labels_all[i] for i in test_indices]

    # Fresh MLM head for test evaluation (Stage 2 adversary uses one internally)
    test_mlm_head = _MLMHead(config.hidden_dim, config.vocab_size).to(device)

    test_dataset = Football2Vec360Dataset(
        test_aids, test_xs, test_ys, freeze_frames=test_ffs, mlm=True, competition_ids=test_comp_ids
    )
    test_loader = DataLoader(
        test_dataset,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    test_mlm_loss, test_adv_accuracy = _evaluate_stage2(
        model,
        test_mlm_head,
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

    # 9. Generate 144d debiased embeddings on full dataset
    logger.info("=== Generating 144d debiased embeddings (inference on all %d sequences) ===", len(data))
    embeddings_df = generate_embeddings(
        model, data, action_ids_all, x_coords_all, y_coords_all, freeze_frames_all, device
    )

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
        "embedding_dim": OUTPUT_DIM,
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

    logger.info("Published model: https://huggingface.co/%s", OUTPUT_MODEL)
    logger.info("Published embeddings: https://huggingface.co/datasets/%s", OUTPUT_EMBEDDINGS)


if __name__ == "__main__":
    main()
