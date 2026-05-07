"""Training helpers for Football2Vec 360 (freeze-frame enriched transformer).

Contains data loading, parsing, dataset class, train/val/test splitting,
learning rate scheduler, and MLM head. Packaged in the wheel so HF Jobs
PEP 723 scripts can import via ``from ingestion.football2vec_360_training import ...``
(sibling imports do not work on HF Jobs).
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
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
OUTPUT_DIM = 208  # hidden_dim(192) + context_dim(16)
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

_SPADL_PITCH_LENGTH = 105.0
_SPADL_PITCH_WIDTH = 68.0
_SB_PITCH_LENGTH = 120.0
_SB_PITCH_WIDTH = 80.0

_F2V_360_SQL = """\
SELECT
    CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
    CAST(av.match_id AS STRING)            AS match_id,
    CAST(av.competition_id AS INT)         AS competition_id,
    CAST(av.season_id AS INT)              AS season_id,
    dp.position_group,
    av.action_type,
    CAST(av.start_x / 105.0 AS FLOAT)     AS x,
    CAST(av.start_y / 68.0  AS FLOAT)     AS y,
    av.action_result,
    av.period,
    av.time_seconds,
    av.original_event_id,
    CAST(ff.location_x / 120.0 AS FLOAT)  AS ff_x_norm,
    CAST(ff.location_y / 80.0  AS FLOAT)  AS ff_y_norm,
    CAST(ff.is_keeper AS BOOLEAN)          AS ff_is_keeper,
    CAST(ff.is_teammate AS BOOLEAN)        AS ff_is_teammate
FROM soccer_analytics.dev_gold.fct_action_values av
INNER JOIN soccer_analytics.dev_silver.stg_statsbomb__360 ff
    ON av.original_event_id = ff.event_uuid
INNER JOIN soccer_analytics.dev_gold.dim_players dp
    ON av.player_key = dp.player_key
WHERE av.data_source = 'statsbomb'
  AND av.player_id IS NOT NULL
  AND dp.canonical_player_id IS NOT NULL
  AND av.action_type IS NOT NULL
  AND av.start_x IS NOT NULL
  AND av.start_y IS NOT NULL
  AND ff.location_x IS NOT NULL
  AND ff.location_y IS NOT NULL
"""


def _transform_360_to_training_data(raw: pd.DataFrame) -> pd.DataFrame:
    """Transform raw 360 SQL output to training format (one row per player-match).

    Two-pass aggregation mirroring prepare_360_training_data.py:
    1. Per (player, match, event) -> collect freeze-frame players
    2. Per (player, match) -> build aligned actions + freeze_frames arrays
    """
    raw["action_type_id"] = raw["action_type"].map(_ACTION_TYPE_IDS).fillna(20).astype(int)
    raw["result_binary"] = (raw["action_result"] == "success").astype(int)
    raw = raw.sort_values(["canonical_player_id", "match_id", "period", "time_seconds", "original_event_id"])

    # Pass 1: collect freeze-frame players per action
    per_action = raw.groupby(["canonical_player_id", "match_id", "original_event_id"], sort=False)
    first_fields = per_action.first()[
        [
            "competition_id",
            "season_id",
            "position_group",
            "period",
            "time_seconds",
            "action_type_id",
            "x",
            "y",
            "result_binary",
        ]
    ].copy()
    players_series = per_action.apply(
        lambda grp: [
            {
                "x": float(r["ff_x_norm"]),
                "y": float(r["ff_y_norm"]),
                "is_keeper": bool(r["ff_is_keeper"]),
                "is_teammate": bool(r["ff_is_teammate"]),
            }
            for _, r in grp[["ff_x_norm", "ff_y_norm", "ff_is_keeper", "ff_is_teammate"]].iterrows()
        ],
        include_groups=False,
    )
    first_fields["players"] = players_series
    first_fields = first_fields.reset_index()

    # Pass 2: group by (player, match) -> build aligned arrays
    first_fields = first_fields.sort_values(["canonical_player_id", "match_id", "period", "time_seconds"])
    grouped = first_fields.groupby(["canonical_player_id", "match_id"], sort=False)

    action_cols = ["action_type_id", "x", "y", "result_binary"]
    rename_map = {"action_type_id": "action_type", "result_binary": "result"}
    actions_series = grouped.apply(
        lambda grp: grp[action_cols].rename(columns=rename_map).to_dict("records"),
        include_groups=False,
    )
    ff_series = grouped.apply(
        lambda grp: [{"players": p} for p in grp["players"]],
        include_groups=False,
    )
    meta = grouped.first()[["competition_id", "season_id", "position_group"]].copy()
    meta["competition_id"] = meta["competition_id"].fillna(0).astype(int)
    meta["season_id"] = meta["season_id"].fillna(0).astype(int)
    meta["actions"] = actions_series
    meta["freeze_frames"] = ff_series
    return meta.reset_index()


def load_training_data_sql(host: str, token: str, warehouse_id: str) -> tuple[pd.DataFrame, str]:
    """Fetch 360-enriched training data directly from gold marts via SQL.

    Returns:
        (DataFrame with same schema as HF dataset, SQL-fetch timestamp string)
    """
    from datetime import datetime, timezone

    from analytics.databricks_sql_fetch import query_databricks_sql

    raw = query_databricks_sql(host, token, _F2V_360_SQL, warehouse_id)
    logger.info("SQL fetch returned %d raw 360 rows", len(raw))
    data = _transform_360_to_training_data(raw)
    logger.info("Transformed to %d player-match 360 training rows", len(data))
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return data, f"gold-sql-{ts}"


def load_training_data(hf_token: str, input_dataset: str) -> tuple[pd.DataFrame, str]:
    """Download 360-enriched training data from HF Hub and return DataFrame + commit hash."""
    from datasets import load_dataset
    from huggingface_hub import HfApi

    ds = load_dataset(input_dataset, split="train", token=hf_token)
    data: pd.DataFrame = ds.to_pandas()  # type: ignore[union-attr]
    logger.info("Loaded %d player-match sequences from %s", len(data), input_dataset)

    api = HfApi(token=hf_token)
    dataset_info = api.repo_info(repo_id=input_dataset, repo_type="dataset")
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
