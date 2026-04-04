"""Helper module for train_football2vec_360.py.

Contains data loading, parsing, dataset class, train/val/test splitting,
learning rate scheduler, and MLM head. The main script handles training
loops, evaluation, model I/O, MLflow logging, and the pipeline orchestration.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
import torch.nn as nn
from torch.utils.data import Dataset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants (shared with main script)
# ---------------------------------------------------------------------------

VOCAB_SIZE = 23
MASK_TOKEN_ID = VOCAB_SIZE  # 23
PAD_TOKEN_ID = VOCAB_SIZE + 1  # 24
OUTPUT_DIM = 144
MAX_SEQ_LEN = 512
MAX_PLAYERS = 22
WEIGHT_DECAY = 0.01
WARMUP_FRACTION = 0.10
RANDOM_STATE = 42
ADVERSARIAL_LAMBDA_MAX = 0.2
ADVERSARIAL_WARMUP_EPOCHS = 5
DEFAULT_MASK_PROB = 0.15


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_training_data(hf_token: str, input_dataset: str) -> tuple[pd.DataFrame, str]:
    """Download 360-enriched training data from HF Hub and return DataFrame + commit hash."""
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_items = list(api.list_repo_tree(input_dataset, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {input_dataset}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(input_dataset, pf, repo_type="dataset", token=hf_token)
        table = pq.read_table(local_path)
        df = table.to_pandas()
        dfs.append(df)
        logger.info("  %s: %d rows", pf, len(df))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total player-match sequences: %d", len(data))
    dataset_info = api.repo_info(repo_id=input_dataset, repo_type="dataset")
    return data, dataset_info.sha


def parse_actions(
    actions_col: pd.Series,
) -> tuple[list[list[int]], list[list[float]], list[list[float]]]:
    """Parse the actions struct array column into separate lists."""
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


def parse_freeze_frames(
    freeze_frames_col: pd.Series,
    max_seq_len: int = MAX_SEQ_LEN,
    max_players: int = MAX_PLAYERS,
) -> list[list[list[list[float]]]]:
    """Parse the freeze_frames column into nested float lists.

    Returns: [row][action_idx][player_idx][feature_idx] where features are [x, y, is_keeper, is_teammate].
    """
    all_frames: list[list[list[list[float]]]] = []

    for frames_per_seq in freeze_frames_col:
        if frames_per_seq is None or (hasattr(frames_per_seq, "__len__") and len(frames_per_seq) == 0):
            all_frames.append([])
            continue
        seq_frames: list[list[list[float]]] = []
        for action_players in frames_per_seq[:max_seq_len]:
            players: list[list[float]] = []
            if action_players is not None:
                for p in action_players["players"][:max_players]:
                    players.append([float(p["x"]), float(p["y"]), float(p["is_keeper"]), float(p["is_teammate"])])
            seq_frames.append(players)
        all_frames.append(seq_frames)

    return all_frames


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class Football2Vec360Dataset(Dataset[dict[str, torch.Tensor]]):
    """PyTorch dataset for 360-enriched SPADL action sequences with MLM masking."""

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
        import numpy as np

        n = len(action_ids)
        sl = max_seq_len
        mp = max_players
        self.mask_prob = mask_prob
        self.mlm = mlm
        self._n = n

        # Pre-tensorize base fields (same pattern as Football2VecDataset)
        t_action = torch.full((n, sl), PAD_TOKEN_ID, dtype=torch.long)
        t_x = torch.zeros(n, sl, dtype=torch.float32)
        t_y = torch.zeros(n, sl, dtype=torch.float32)
        t_mask = torch.zeros(n, sl, dtype=torch.bool)
        t_seq_lens = torch.zeros(n, dtype=torch.long)

        # Pre-tensorize freeze frames: convert nested Python lists to numpy first,
        # then to a single contiguous tensor. Eliminates up to 11,264 torch.tensor()
        # calls per sample that previously ran inside __getitem__.
        freeze_np = np.zeros((n, sl, mp, 4), dtype=np.float32)

        for i in range(n):
            seq_len = min(len(action_ids[i]), sl)
            if seq_len > 0:
                t_action[i, :seq_len] = torch.tensor(action_ids[i][:seq_len], dtype=torch.long)
                t_x[i, :seq_len] = torch.tensor(x_coords[i][:seq_len], dtype=torch.float32)
                t_y[i, :seq_len] = torch.tensor(y_coords[i][:seq_len], dtype=torch.float32)
                t_mask[i, :seq_len] = True
                t_seq_lens[i] = seq_len

            if freeze_frames is not None:
                frames = freeze_frames[i]
                for action_idx, players in enumerate(frames[:seq_len]):
                    for player_idx, feat in enumerate(players[:mp]):
                        freeze_np[i, action_idx, player_idx] = feat

        self._action_ids = t_action
        self._x_coords = t_x
        self._y_coords = t_y
        self._attention_mask = t_mask
        self._seq_lens = t_seq_lens
        self._freeze_frames = torch.from_numpy(freeze_np)
        self._competition_ids = torch.tensor(competition_ids, dtype=torch.long) if competition_ids else None

    def __len__(self) -> int:
        return self._n

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        """Return a single tokenized, padded, optionally masked 360-enriched sample."""
        action_tensor = self._action_ids[idx].clone()  # clone for MLM mutation
        seq_len = int(self._seq_lens[idx].item())

        result: dict[str, torch.Tensor] = {
            "action_ids": action_tensor,
            "x_coords": self._x_coords[idx],
            "y_coords": self._y_coords[idx],
            "attention_mask": self._attention_mask[idx],
            "freeze_frames": self._freeze_frames[idx],
        }

        if self.mlm and seq_len > 0:
            labels = torch.full_like(action_tensor, -100)
            n_mask = max(1, int(seq_len * self.mask_prob))
            mask_indices = torch.randperm(seq_len)[:n_mask]
            labels[mask_indices] = action_tensor[mask_indices].clone()
            action_tensor[mask_indices] = MASK_TOKEN_ID
            result["labels"] = labels
        elif self.mlm:
            result["labels"] = torch.full_like(action_tensor, -100)

        if self._competition_ids is not None:
            result["competition_id"] = self._competition_ids[idx]

        return result


# ---------------------------------------------------------------------------
# Train/val/test splitting
# ---------------------------------------------------------------------------


def stratified_split(
    data: pd.DataFrame,
    train_frac: float = 0.80,
    val_frac: float = 0.10,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split data into train/val/test stratified by competition_id."""
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
    """Cosine annealing with linear warmup."""

    def lr_lambda(current_step: int) -> float:
        if current_step < num_warmup_steps:
            return float(current_step) / float(max(1, num_warmup_steps))
        progress = float(current_step - num_warmup_steps) / float(max(1, num_training_steps - num_warmup_steps))
        return max(0.0, 0.5 * (1.0 + math.cos(math.pi * progress)))

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)


# ---------------------------------------------------------------------------
# MLM wrapper for 360 encoder
# ---------------------------------------------------------------------------


class MLMHead(nn.Module):
    """Per-token MLM prediction head applied to transformer outputs."""

    def __init__(self, hidden_dim: int, vocab_size: int) -> None:
        super().__init__()
        self.head = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.GELU(),
            nn.LayerNorm(hidden_dim),
            nn.Linear(hidden_dim, vocab_size),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.head(x)  # type: ignore[no-any-return]


def mlm_forward_360(
    model: object,
    mlm_head: MLMHead,
    action_ids: torch.Tensor,
    x_coords: torch.Tensor,
    y_coords: torch.Tensor,
    attention_mask: torch.Tensor | None,
    freeze_frames: torch.Tensor | None,
) -> torch.Tensor:
    """Compute per-token MLM logits using the 360 encoder's transformer branch."""
    embedded = model.base_encoder._embed(action_ids, x_coords, y_coords)  # type: ignore[attr-defined]
    src_key_padding_mask: torch.Tensor | None = None
    if attention_mask is not None:
        src_key_padding_mask = ~attention_mask
    encoded = model.base_encoder.encoder(embedded, src_key_padding_mask=src_key_padding_mask)  # type: ignore[attr-defined]
    return mlm_head(encoded)
