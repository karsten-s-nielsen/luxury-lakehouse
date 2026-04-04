"""Helper module for train_football2vec_v2.py.

Contains data loading, parsing, dataset class, train/val/test splitting,
learning rate scheduler. The main script handles training loops, evaluation,
model I/O, MLflow logging, and pipeline orchestration.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import torch
from torch.utils.data import Dataset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants (shared with main script)
# ---------------------------------------------------------------------------

VOCAB_SIZE = 23
MASK_TOKEN_ID = VOCAB_SIZE  # 23
PAD_TOKEN_ID = VOCAB_SIZE + 1  # 24
MAX_SEQ_LEN = 512
WEIGHT_DECAY = 0.01
WARMUP_FRACTION = 0.10
RANDOM_STATE = 42
ADVERSARIAL_LAMBDA_MAX = 0.2
ADVERSARIAL_WARMUP_EPOCHS = 5
DEFAULT_MASK_PROB = 0.15


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_training_data(hf_token: str, training_dataset: str) -> tuple[pd.DataFrame, str]:
    """Download training data from HF Hub and return DataFrame + commit hash."""
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_items = list(api.list_repo_tree(training_dataset, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {training_dataset}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(training_dataset, pf, repo_type="dataset", token=hf_token)
        table = pq.read_table(local_path)
        df = table.to_pandas()
        dfs.append(df)
        logger.info("  %s: %d rows", pf, len(df))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total player-match sequences: %d", len(data))
    dataset_info = api.repo_info(repo_id=training_dataset, repo_type="dataset")
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


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class Football2VecDataset(Dataset[dict[str, torch.Tensor]]):
    """PyTorch dataset for SPADL action sequences with MLM masking."""

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
        n = len(action_ids)
        sl = max_seq_len
        self.mask_prob = mask_prob
        self.mlm = mlm
        self._n = n

        # Pre-tensorize: pad all sequences once at init time.
        t_action = torch.full((n, sl), PAD_TOKEN_ID, dtype=torch.long)
        t_x = torch.zeros(n, sl, dtype=torch.float32)
        t_y = torch.zeros(n, sl, dtype=torch.float32)
        t_mask = torch.zeros(n, sl, dtype=torch.bool)
        t_seq_lens = torch.zeros(n, dtype=torch.long)

        for i in range(n):
            seq_len = min(len(action_ids[i]), sl)
            if seq_len > 0:
                t_action[i, :seq_len] = torch.tensor(action_ids[i][:seq_len], dtype=torch.long)
                t_x[i, :seq_len] = torch.tensor(x_coords[i][:seq_len], dtype=torch.float32)
                t_y[i, :seq_len] = torch.tensor(y_coords[i][:seq_len], dtype=torch.float32)
                t_mask[i, :seq_len] = True
                t_seq_lens[i] = seq_len

        self._action_ids = t_action
        self._x_coords = t_x
        self._y_coords = t_y
        self._attention_mask = t_mask
        self._seq_lens = t_seq_lens
        self._competition_ids = torch.tensor(competition_ids, dtype=torch.long) if competition_ids else None

    def __len__(self) -> int:
        return self._n

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        """Return a single tokenized, padded, optionally masked sample."""
        # Index pre-tensorized data — zero allocation for base fields
        action_tensor = self._action_ids[idx].clone()  # clone for MLM mutation
        seq_len = int(self._seq_lens[idx].item())

        result: dict[str, torch.Tensor] = {
            "action_ids": action_tensor,
            "x_coords": self._x_coords[idx],
            "y_coords": self._y_coords[idx],
            "attention_mask": self._attention_mask[idx],
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
