"""Football2Vec v2 training helpers — dataset, masking, splits, LR schedule.

Contains data loading, parsing, PyTorch Dataset class, train/val/test
splitting, and the cosine learning rate scheduler used by
``scripts/train_football2vec_v2.py`` (HF Jobs L40S training entry point).

Moved from ``scripts/train_football2vec_v2_helpers.py`` into the wheel so
HF Jobs scripts can import it via ``ingestion.football2vec_v2_training``
without sibling-file tricks, and so ``src/tests/test_benchmarks.py`` can
reach ``Football2VecDataset`` without ``sys.path.insert`` on the scripts
directory.

This module is training-only — it is NEVER imported by production
inference code (``player_embeddings_v1.py``, ``player_embeddings_v2.py``,
etc.). Those paths use the frozen on-disk model and do not need the
training dataset class.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
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


# SPADL 23-type action vocabulary mapping (string -> int).
# Canonical ordering matches silly_kicks SPADL and defcon_lite._ACTION_TYPE_IDS.
# Duplicated here (not imported from export_embeddings_training_data) because
# that module pulls Spark imports which fail on HF Jobs.
_ACTION_TYPE_IDS: dict[str, int] = {
    "pass": 0,
    "cross": 1,
    "throw_in": 2,
    "freekick_crossed": 3,
    "freekick_short": 4,
    "corner_crossed": 5,
    "corner_short": 6,
    "take_on": 7,
    "foul": 8,
    "tackle": 9,
    "interception": 10,
    "shot": 11,
    "shot_penalty": 12,
    "shot_freekick": 13,
    "keeper_save": 14,
    "keeper_claim": 15,
    "keeper_punch": 16,
    "keeper_pick_up": 17,
    "clearance": 18,
    "bad_touch": 19,
    "non_action": 20,
    "dribble": 21,
    "goalkick": 22,
}

_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0

_F2V_V2_SQL = """\
SELECT
    CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
    CAST(av.match_id AS STRING)            AS match_id,
    CAST(av.competition_id AS INT)         AS competition_id,
    CAST(av.season_id AS INT)              AS season_id,
    dp.position_group,
    av.action_type,
    av.start_x,
    av.start_y,
    av.action_result,
    av.period,
    av.time_seconds
FROM soccer_analytics.dev_gold.fct_action_values av
INNER JOIN soccer_analytics.dev_gold.dim_players dp
    ON av.player_key = dp.player_key
WHERE av.player_id IS NOT NULL
  AND dp.canonical_player_id IS NOT NULL
  AND av.action_type IS NOT NULL
  AND av.start_x IS NOT NULL
  AND av.start_y IS NOT NULL
"""


def _transform_to_training_data(raw: pd.DataFrame) -> pd.DataFrame:
    """Transform raw SQL output to f2v_v2 training format (one row per player-match)."""
    raw["action_type_id"] = raw["action_type"].map(_ACTION_TYPE_IDS).fillna(20).astype(int)
    raw["x_norm"] = (raw["start_x"] / _PITCH_LENGTH).astype(float)
    raw["y_norm"] = (raw["start_y"] / _PITCH_WIDTH).astype(float)
    raw["result_binary"] = (raw["action_result"] == "success").astype(int)
    raw = raw.sort_values(["canonical_player_id", "match_id", "period", "time_seconds"])

    action_cols = ["action_type_id", "x_norm", "y_norm", "result_binary"]
    rename_map = {"action_type_id": "action_type", "x_norm": "x", "y_norm": "y", "result_binary": "result"}

    grouped = raw.groupby(["canonical_player_id", "match_id"], sort=False)
    actions_series = grouped.apply(
        lambda grp: grp[action_cols].rename(columns=rename_map).to_dict("records"),
        include_groups=False,
    )
    meta = grouped.first()[["competition_id", "season_id", "position_group"]].copy()
    meta["competition_id"] = meta["competition_id"].fillna(0).astype(int)
    meta["season_id"] = meta["season_id"].fillna(0).astype(int)
    meta["actions"] = actions_series
    return meta.reset_index()


def load_training_data_sql(host: str, token: str, warehouse_id: str) -> tuple[pd.DataFrame, str]:
    """Fetch training data directly from gold marts via Databricks SQL.

    Gamma (gold-SQL) replacement for load_training_data() — reads from
    fct_action_values JOIN dim_players instead of a stale HF dataset export.

    Returns:
        (DataFrame with same schema as HF dataset export, SQL-fetch timestamp string)
    """
    from datetime import datetime, timezone

    from analytics.databricks_sql_fetch import query_databricks_sql

    raw = query_databricks_sql(host, token, _F2V_V2_SQL, warehouse_id)
    logger.info("SQL fetch returned %d raw action rows", len(raw))
    data = _transform_to_training_data(raw)
    logger.info("Transformed to %d player-match training rows", len(data))
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return data, f"gold-sql-{ts}"


def load_training_data(
    hf_token: str, training_dataset: str, *, revision: str | None = None
) -> tuple[pd.DataFrame, str]:
    """Download training data from HF Hub and return DataFrame + commit hash."""
    from datasets import Dataset, load_dataset
    from huggingface_hub import HfApi

    # load_dataset is typed to return DatasetDict | IterableDatasetDict | Dataset | IterableDataset;
    # with split="train" explicitly specified it always returns Dataset, but the type stubs do not
    # narrow. Assert-narrow so downstream .to_pandas() + len() are type-safe, and so any future
    # upstream change that drops the narrow-on-split guarantee is caught at the boundary.
    ds = load_dataset(training_dataset, split="train", token=hf_token, revision=revision)
    if not isinstance(ds, Dataset):
        msg = f"expected Dataset from load_dataset(..., split='train'); got {type(ds).__name__}"
        raise TypeError(msg)
    data = ds.to_pandas()
    if not isinstance(data, pd.DataFrame):
        msg = f"expected DataFrame from Dataset.to_pandas(); got {type(data).__name__}"
        raise TypeError(msg)
    logger.info("Loaded %d player-match sequences from %s", len(data), training_dataset)

    api = HfApi(token=hf_token)
    dataset_info = api.repo_info(repo_id=training_dataset, repo_type="dataset")
    commit_sha: str = dataset_info.sha or ""
    return data, commit_sha


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
    # Build rare-label sets via dict comprehension — avoids the pandas boolean
    # Index subscript path that pyright cannot resolve to a concrete type.
    rare_labels = [label for label, cnt in counts.items() if cnt < 3]
    rare_mask = stratify_col.isin(rare_labels)
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
    # train_test_split returns a generic list; cast to int ndarray so the
    # pandas .iloc overload resolution picks the integer-array signature.
    stratify_trainval = stratify_col.iloc[np.asarray(train_val_idx, dtype=np.int64)]
    tv_counts = stratify_trainval.value_counts()
    tv_rare_labels = [label for label, cnt in tv_counts.items() if cnt < 2]
    tv_rare = stratify_trainval.isin(tv_rare_labels)
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
