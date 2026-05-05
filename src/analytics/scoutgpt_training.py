"""ScoutGPT training: dataset, training loop, evaluation, scheduling.

Domain logic for ScoutGPT decoder training. The HF Jobs entry point
(``scripts/train_scoutgpt_hf.py``) is a thin wrapper that handles I/O,
checkpointing, cost recording, and MLflow logging.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import threading
import time
from collections import Counter
from typing import Any

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from scipy.stats import spearmanr
from torch.utils.data import DataLoader, Dataset

from analytics.scoutgpt_decoder import ScoutGPTConfig, ScoutGPTDecoder

logger = logging.getLogger(__name__)

# Vocabulary constants (must match scoutgpt_decoder.py)
VOCAB_SIZE = 23
PAD_TOKEN_ID = 23
BOS_TOKEN_ID = 24
MAX_SEQ_LEN = 128

# Training constants
WEIGHT_DECAY = 0.01
WARMUP_FRACTION = 0.10
RANDOM_STATE = 42
DEFAULT_EPOCHS = 30
DEFAULT_BATCH_SIZE = 256
DEFAULT_LR = 1e-4
DEFAULT_PATIENCE = 5
VAEP_LOSS_WEIGHT = 0.1

# Evaluation constants
COUNTERFACTUAL_NUM_EPISODES = 1000
COUNTERFACTUAL_NUM_PLAYERS = 100

# Windows spawn-based multiprocessing crashes DataLoader workers unless the
# caller uses ``if __name__ == '__main__':``.  Safe default: 0 on Windows.
_EVAL_NUM_WORKERS = 0 if sys.platform == "win32" else 2


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


# SPADL 23-type action vocabulary mapping (string -> int).
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

_SET_PIECE_TYPES: frozenset[str] = frozenset(
    {
        "goalkick",
        "throw_in",
        "freekick_short",
        "freekick_crossed",
        "corner_short",
        "corner_crossed",
    }
)

_TIME_GAP_THRESHOLD = 10.0
_MIN_EPISODE_LENGTH = 3

_SCOUTGPT_SQL = """\
SELECT
    CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
    CAST(av.match_id AS STRING)            AS match_id,
    CAST(av.team_id AS INT)                AS team_id,
    CAST(av.competition_id AS INT)         AS competition_id,
    CAST(av.season_id AS INT)              AS season_id,
    av.data_source,
    av.action_type,
    av.action_result,
    av.period,
    av.time_seconds,
    av.start_x,
    av.start_y,
    av.end_x,
    av.end_y,
    av.vaep_value
FROM soccer_analytics.dev_gold.fct_action_values av
INNER JOIN soccer_analytics.dev_gold.dim_players dp
    ON av.player_key = dp.player_key
WHERE av.player_id IS NOT NULL
  AND dp.canonical_player_id IS NOT NULL
  AND av.action_type IS NOT NULL
  AND av.start_x IS NOT NULL
  AND av.start_y IS NOT NULL
"""


def _segment_possessions(df: pd.DataFrame) -> pd.DataFrame:
    """Segment actions into possession episodes (pandas reimplementation of Spark window functions)."""
    df = df.sort_values(["match_id", "period", "time_seconds"]).reset_index(drop=True)
    df["prev_team_id"] = df.groupby("match_id")["team_id"].shift(1)
    df["prev_period"] = df.groupby("match_id")["period"].shift(1)
    df["prev_time"] = df.groupby("match_id")["time_seconds"].shift(1)
    df["is_boundary"] = (
        df["prev_team_id"].isna()
        | (df["team_id"] != df["prev_team_id"])
        | (df["period"] != df["prev_period"])
        | df["action_type"].isin(_SET_PIECE_TYPES)
        | ((df["period"] == df["prev_period"]) & ((df["time_seconds"] - df["prev_time"]) > _TIME_GAP_THRESHOLD))
    ).astype(int)
    df["episode_seq"] = df.groupby("match_id")["is_boundary"].cumsum()
    df["episode_id"] = df["match_id"] + "_" + df["period"].astype(str) + "_" + df["episode_seq"].astype(str)
    ep_counts = df.groupby("episode_id").size()
    valid = ep_counts[ep_counts >= _MIN_EPISODE_LENGTH].index
    df = df[df["episode_id"].isin(valid)].copy()
    df["time_delta"] = df["time_seconds"] - df["prev_time"]
    df.loc[df["is_boundary"] == 1, "time_delta"] = 0.0
    df["time_delta"] = df["time_delta"].fillna(0.0).astype(float)
    return df


def _build_player_id_map_from_df(df: pd.DataFrame) -> dict[str, int]:
    """Build canonical_player_id -> contiguous int mapping."""
    unique_players = sorted(df["canonical_player_id"].unique().tolist())
    return {pid: idx for idx, pid in enumerate(unique_players)}


def load_training_data_sql(
    host: str,
    token: str,
    warehouse_id: str,
) -> tuple[pd.DataFrame, dict[str, int], str]:
    """Fetch ScoutGPT training data directly from gold marts via SQL.

    Gamma (gold-SQL) replacement for load_training_data() -- reads from
    fct_action_values JOIN dim_players, segments possessions in pandas,
    and returns the same schema as the HF dataset export.
    """
    from datetime import datetime, timezone

    from ingestion.databricks_sql_fetch import query_databricks_sql

    raw = query_databricks_sql(host, token, _SCOUTGPT_SQL, warehouse_id)
    logger.info("SQL fetch returned %d raw action rows", len(raw))

    # Build player_id_map before transforms
    player_id_map = _build_player_id_map_from_df(raw)
    logger.info("Built player_id_map with %d unique players", len(player_id_map))

    # Possession segmentation
    seg = _segment_possessions(raw)
    logger.info("After segmentation: %d actions in valid episodes", len(seg))

    # Map action_type string -> int, normalize coords, binarize result
    seg["action_type_id"] = seg["action_type"].map(_ACTION_TYPE_IDS).fillna(20).astype(int)
    seg["start_x_norm"] = (seg["start_x"] / _PITCH_LENGTH).astype(float)
    seg["start_y_norm"] = (seg["start_y"] / _PITCH_WIDTH).astype(float)
    seg["end_x_norm"] = (seg["end_x"].fillna(seg["start_x"]) / _PITCH_LENGTH).astype(float)
    seg["end_y_norm"] = (seg["end_y"].fillna(seg["start_y"]) / _PITCH_WIDTH).astype(float)
    seg["result_binary"] = (seg["action_result"] == "success").astype(int)
    seg["vaep_val"] = seg["vaep_value"].fillna(0.0).astype(float)
    seg["player_idx"] = seg["canonical_player_id"].map(player_id_map).fillna(0).astype(int)

    # Group by episode -> collect action struct arrays
    action_fields = [
        "action_type_id",
        "start_x_norm",
        "start_y_norm",
        "end_x_norm",
        "end_y_norm",
        "result_binary",
        "vaep_val",
        "time_delta",
        "player_idx",
    ]
    rename = {
        "action_type_id": "action_type",
        "start_x_norm": "start_x",
        "start_y_norm": "start_y",
        "end_x_norm": "end_x",
        "end_y_norm": "end_y",
        "result_binary": "result",
        "vaep_val": "vaep_value",
    }
    grouped = seg.groupby("episode_id", sort=False)
    actions_series = grouped.apply(
        lambda grp: grp[action_fields].rename(columns=rename).to_dict("records"),
        include_groups=False,
    )
    meta = grouped.first()[
        [
            "match_id",
            "competition_id",
            "season_id",
            "team_id",
            "data_source",
        ]
    ].copy()
    meta["competition_id"] = meta["competition_id"].fillna(0).astype(int)
    meta["season_id"] = meta["season_id"].fillna(0).astype(int)
    meta["team_id"] = meta["team_id"].fillna(0).astype(int)
    meta["actions"] = actions_series
    data = meta.reset_index()
    logger.info("Transformed to %d possession episodes", len(data))

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return data, player_id_map, f"gold-sql-{ts}"


def load_training_data(
    hf_token: str,
    dataset_repo: str,
    revision: str | None = None,
) -> tuple[pd.DataFrame, dict[str, int], str]:
    """Load episodes and player ID map from HF Hub.

    Args:
        hf_token: HuggingFace Hub auth token.
        dataset_repo: Repo id (e.g. ``luxury-lakehouse/scoutgpt-training-data``).
        revision: Optional git SHA / branch / tag to pin the read to. When
            provided, every list_repo_tree / hf_hub_download / repo_info call
            is pinned to the same revision so A/B runs see byte-identical data.

    Returns:
        Tuple of (episodes DataFrame, player_id_map dict, dataset SHA).
    """
    import pyarrow.parquet as pq
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_items = list(api.list_repo_tree(dataset_repo, repo_type="dataset", recursive=True, revision=revision))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {dataset_repo}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(dataset_repo, pf, repo_type="dataset", token=hf_token, revision=revision)
        table = pq.read_table(local_path)
        dfs.append(table.to_pandas())
        logger.info("  %s: %d rows", pf, len(dfs[-1]))

    data = pd.concat(dfs, ignore_index=True)
    logger.info("Total episodes: %d", len(data))

    # Load player ID map
    map_files = [
        f.path for f in all_items if hasattr(f, "size") and "player_id_map" in f.path and f.path.endswith(".json")
    ]
    if not map_files:
        # Fallback: try the text-file format written by Spark
        text_files = [f.path for f in all_items if hasattr(f, "size") and "_player_id_map" in f.path]
        if not text_files:
            msg = "No player_id_map found in dataset"
            raise RuntimeError(msg)
        map_path = hf_hub_download(dataset_repo, text_files[0], repo_type="dataset", token=hf_token, revision=revision)
    else:
        map_path = hf_hub_download(dataset_repo, map_files[0], repo_type="dataset", token=hf_token, revision=revision)

    with open(map_path, encoding="utf-8") as f:
        player_id_map: dict[str, int] = json.load(f)
    logger.info("Player ID map: %d players", len(player_id_map))

    dataset_info = api.repo_info(repo_id=dataset_repo, repo_type="dataset", revision=revision)
    return data, player_id_map, dataset_info.sha or ""


def parse_episode_actions(
    actions_list: list[dict[str, Any]],
) -> tuple[
    list[int],
    list[float],
    list[float],
    list[float],
    list[float],
    list[int],
    list[float],
    list[float],
    list[int],
]:
    """Parse a single episode's action struct array into per-field lists.

    Returns:
        (action_types, start_xs, start_ys, end_xs, end_ys,
         results, vaep_values, time_deltas, player_idxs)
    """
    action_types: list[int] = []
    start_xs: list[float] = []
    start_ys: list[float] = []
    end_xs: list[float] = []
    end_ys: list[float] = []
    results: list[int] = []
    vaep_values: list[float] = []
    time_deltas: list[float] = []
    player_idxs: list[int] = []

    for a in actions_list:
        action_types.append(int(a["action_type"]))
        start_xs.append(float(a["start_x"]))
        start_ys.append(float(a["start_y"]))
        end_xs.append(float(a["end_x"]))
        end_ys.append(float(a["end_y"]))
        results.append(int(a["result"]))
        vaep_values.append(float(a.get("vaep_value", 0.0)))
        time_deltas.append(float(a.get("time_delta", 0.0)))
        player_idxs.append(int(a.get("player_idx", 0)))

    return action_types, start_xs, start_ys, end_xs, end_ys, results, vaep_values, time_deltas, player_idxs


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


class ScoutGPTDataset(Dataset):
    """PyTorch dataset for ScoutGPT autoregressive training.

    Each sample is a possession episode with a focal player conditioning token
    prepended at position 0. Labels are action_ids shifted right by 1.

    Position 0 = BOS token + focal player embedding (player who performs action 1).
    Positions 1..N = actual episode actions with per-action player_ids.
    Labels[t] = action_ids[t+1] (autoregressive: predict next token).
    Labels at position 0 targets the first real action.
    Labels at padding positions = -100 (ignored by CrossEntropyLoss).
    """

    def __init__(
        self,
        action_types: list[list[int]],
        start_xs: list[list[float]],
        start_ys: list[list[float]],
        end_xs: list[list[float]],
        end_ys: list[list[float]],
        results: list[list[int]],
        vaep_values: list[list[float]],
        time_deltas: list[list[float]],
        player_idxs: list[list[int]],
        max_seq_len: int = MAX_SEQ_LEN,
        *,
        competition_ids: list[int] | None = None,
    ) -> None:
        n = len(action_types)
        sl = max_seq_len
        max_actions = sl - 1  # leave room for BOS at position 0

        # Pre-tensorize: pad all episodes once, store as contiguous tensors.
        # __getitem__ becomes a pure index lookup — zero per-sample allocation.
        t_action = torch.full((n, sl), PAD_TOKEN_ID, dtype=torch.long)
        t_sx = torch.zeros(n, sl, dtype=torch.float32)
        t_sy = torch.zeros(n, sl, dtype=torch.float32)
        t_ex = torch.zeros(n, sl, dtype=torch.float32)
        t_ey = torch.zeros(n, sl, dtype=torch.float32)
        t_res = torch.zeros(n, sl, dtype=torch.long)
        t_td = torch.zeros(n, sl, dtype=torch.float32)
        t_pid = torch.zeros(n, sl, dtype=torch.long)
        t_mask = torch.zeros(n, sl, dtype=torch.bool)
        t_labels = torch.full((n, sl), -100, dtype=torch.long)
        t_vaep = torch.zeros(n, sl, dtype=torch.float32)

        for i in range(n):
            ep_len = min(len(action_types[i]), max_actions)
            total_len = ep_len + 1

            # Position 0: BOS + focal player
            t_action[i, 0] = BOS_TOKEN_ID
            t_pid[i, 0] = player_idxs[i][0] if ep_len > 0 else 0
            t_mask[i, 0] = True

            if ep_len > 0:
                t_action[i, 1:total_len] = torch.tensor(action_types[i][:ep_len], dtype=torch.long)
                t_sx[i, 1:total_len] = torch.tensor(start_xs[i][:ep_len], dtype=torch.float32)
                t_sy[i, 1:total_len] = torch.tensor(start_ys[i][:ep_len], dtype=torch.float32)
                t_ex[i, 1:total_len] = torch.tensor(end_xs[i][:ep_len], dtype=torch.float32)
                t_ey[i, 1:total_len] = torch.tensor(end_ys[i][:ep_len], dtype=torch.float32)
                t_res[i, 1:total_len] = torch.tensor(results[i][:ep_len], dtype=torch.long)
                t_td[i, 1:total_len] = torch.tensor(time_deltas[i][:ep_len], dtype=torch.float32)
                t_pid[i, 1:total_len] = torch.tensor(player_idxs[i][:ep_len], dtype=torch.long)
                t_mask[i, 1:total_len] = True
                t_vaep[i, 1:total_len] = torch.tensor(vaep_values[i][:ep_len], dtype=torch.float32)

                # Vectorized label shift: label[t] = action_ids[t+1], PAD → -100
                shifted = t_action[i, 1:total_len]
                t_labels[i, : total_len - 1] = torch.where(shifted == PAD_TOKEN_ID, torch.tensor(-100), shifted)

        self._action_ids = t_action
        self._start_x = t_sx
        self._start_y = t_sy
        self._end_x = t_ex
        self._end_y = t_ey
        self._result = t_res
        self._time_delta = t_td
        self._player_ids = t_pid
        self._attention_mask = t_mask
        self._labels = t_labels
        self._vaep_targets = t_vaep
        self._competition_ids = torch.tensor(competition_ids, dtype=torch.long) if competition_ids else None
        self._n = n

    def __len__(self) -> int:
        return self._n

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        out: dict[str, torch.Tensor] = {
            "action_ids": self._action_ids[idx],
            "start_x": self._start_x[idx],
            "start_y": self._start_y[idx],
            "end_x": self._end_x[idx],
            "end_y": self._end_y[idx],
            "result": self._result[idx],
            "time_delta": self._time_delta[idx],
            "player_ids": self._player_ids[idx],
            "attention_mask": self._attention_mask[idx],
            "labels": self._labels[idx],
            "vaep_targets": self._vaep_targets[idx],
        }
        if self._competition_ids is not None:
            out["competition_id"] = self._competition_ids[idx]
        return out


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def build_datasets(
    data: pd.DataFrame,
) -> tuple[
    list[list[int]],
    list[list[float]],
    list[list[float]],
    list[list[float]],
    list[list[float]],
    list[list[int]],
    list[list[float]],
    list[list[float]],
    list[list[int]],
    list[int],
]:
    """Parse all episodes into per-field lists for ScoutGPTDataset.

    Returns a 10-element tuple: 9 field lists + competition_ids list.
    """
    all_atypes: list[list[int]] = []
    all_sxs: list[list[float]] = []
    all_sys: list[list[float]] = []
    all_exs: list[list[float]] = []
    all_eys: list[list[float]] = []
    all_res: list[list[int]] = []
    all_vaeps: list[list[float]] = []
    all_tds: list[list[float]] = []
    all_pidxs: list[list[int]] = []
    all_comp_ids: list[int] = []

    for _, row in data.iterrows():
        atypes, sxs, sys_, exs, eys, res, vaeps, tds, pidxs = parse_episode_actions(row["actions"])  # type: ignore[arg-type]
        all_atypes.append(atypes)
        all_sxs.append(sxs)
        all_sys.append(sys_)
        all_exs.append(exs)
        all_eys.append(eys)
        all_res.append(res)
        all_vaeps.append(vaeps)
        all_tds.append(tds)
        all_pidxs.append(pidxs)
        all_comp_ids.append(int(row["competition_id"]))

    return (all_atypes, all_sxs, all_sys, all_exs, all_eys, all_res, all_vaeps, all_tds, all_pidxs, all_comp_ids)


def stratified_split(
    data: pd.DataFrame,
    train_frac: float = 0.80,
    val_frac: float = 0.10,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """80/10/10 split stratified by competition_id with rare-class collapsing."""
    from sklearn.model_selection import train_test_split

    stratify_col = data["competition_id"].astype(str)
    counts = stratify_col.value_counts()
    rare_mask = stratify_col.isin(counts[counts < 3].index)  # type: ignore[arg-type]
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
# Scheduler
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
# Evaluation
# ---------------------------------------------------------------------------


def build_action_type_frequencies(
    data: pd.DataFrame | None = None,
    *,
    all_atypes: list[list[int]] | None = None,
    all_pidxs: list[list[int]] | None = None,
    indices: list[int] | None = None,
) -> dict[int, dict[int, float]]:
    """Build per-player action type frequency table.

    Two calling conventions:

    1. ``build_action_type_frequencies(data)`` — parses the ``"actions"``
       column from scratch (legacy path, used by standalone training).
    2. ``build_action_type_frequencies(all_atypes=..., all_pidxs=...,
       indices=...)`` — builds from already-parsed lists (fast path, used
       by the evolve evaluator which has already called ``build_datasets``).

    Returns:
        ``{player_idx: {action_type: count}}``
    """
    freq: dict[int, dict[int, float]] = {}

    if all_atypes is not None and all_pidxs is not None:
        # Fast path: use pre-parsed episode lists directly.
        episode_range = indices if indices is not None else range(len(all_atypes))
        for i in episode_range:
            for atype, pidx in zip(all_atypes[i], all_pidxs[i], strict=True):
                if pidx not in freq:
                    freq[pidx] = {}
                freq[pidx][atype] = freq[pidx].get(atype, 0) + 1
        return freq

    # Legacy path: parse from DataFrame.
    if data is None:
        msg = "Either data or (all_atypes, all_pidxs) must be provided"
        raise ValueError(msg)
    for _, row in data.iterrows():
        atypes, *_, pidxs = parse_episode_actions(row["actions"])  # type: ignore[arg-type]
        for atype, pidx in zip(atypes, pidxs, strict=True):
            if pidx not in freq:
                freq[pidx] = {}
            freq[pidx][atype] = freq[pidx].get(atype, 0) + 1
    return freq


def compute_baselines(
    test_ds: ScoutGPTDataset,
    train_data: pd.DataFrame,
) -> dict[str, float]:
    """Compute naive baselines for comparison.

    - most_frequent: always predict the most common action type
    - bigram: predict the most common next action given the current action
    """
    all_actions: list[int] = []
    bigram_counts: dict[tuple[int, int], int] = {}

    for _, row in train_data.iterrows():
        atypes, *_ = parse_episode_actions(row["actions"])  # type: ignore[arg-type]
        all_actions.extend(atypes)
        for i in range(len(atypes) - 1):
            key = (atypes[i], atypes[i + 1])
            bigram_counts[key] = bigram_counts.get(key, 0) + 1

    action_counter = Counter(all_actions)
    most_frequent = action_counter.most_common(1)[0][0]

    # Bigram: index by first action for O(1) lookup instead of linear scan
    bigram_by_first: dict[int, dict[int, int]] = {}
    for (a1, a2), count in bigram_counts.items():
        if a1 not in bigram_by_first:
            bigram_by_first[a1] = {}
        bigram_by_first[a1][a2] = count

    bigram_next: dict[int, int] = {}
    for action_type in range(VOCAB_SIZE):
        successors = bigram_by_first.get(action_type, {})
        if successors:
            bigram_next[action_type] = max(successors, key=lambda k: successors[k])
        else:
            bigram_next[action_type] = most_frequent

    # Build bigram lookup tensor for vectorized evaluation
    bigram_next_t = torch.full((VOCAB_SIZE,), -1, dtype=torch.long)
    for at, next_at in bigram_next.items():
        bigram_next_t[at] = next_at

    # Vectorized baseline evaluation over all samples
    mf_correct = 0
    bg_correct = 0
    total = 0

    for i in range(len(test_ds)):
        sample = test_ds[i]
        labels = sample["labels"]
        actions = sample["action_ids"]
        valid = labels != -100
        if not valid.any():
            continue
        valid_labels = labels[valid]
        valid_actions = actions[valid]
        n_valid = int(valid_labels.size(0))
        total += n_valid
        mf_correct += int((valid_labels == most_frequent).sum().item())
        # Bigram: look up predicted next action for each current action
        action_clamped = valid_actions.clamp(0, VOCAB_SIZE - 1)
        bg_predicted = bigram_next_t[action_clamped]
        bg_correct += int((bg_predicted == valid_labels).sum().item())

    return {
        "baseline_most_frequent_accuracy": mf_correct / max(total, 1),
        "baseline_bigram_accuracy": bg_correct / max(total, 1),
    }


def evaluate_counterfactual_ranking(
    model: torch.nn.Module,
    test_ds: ScoutGPTDataset,
    device: torch.device,
    num_episodes: int = COUNTERFACTUAL_NUM_EPISODES,
    num_players: int = COUNTERFACTUAL_NUM_PLAYERS,
    action_type_frequencies: dict[int, dict[int, float]] | None = None,
) -> dict[str, float]:
    """Counterfactual ranking correlation.

    For each test episode, swap the focal player at position 0 with top-N
    most active players. Rank by P(actual_next_action | swapped_player).
    Compute Spearman correlation with player's real-world action type
    frequency (plausibility score).

    Args:
        model: Trained ScoutGPTDecoder in eval mode.
        test_ds: Test dataset.
        device: Torch device.
        num_episodes: Episodes to sample.
        num_players: Player swaps per episode.
        action_type_frequencies: Per-player action frequency dict.
            If None, returns zero (no plausibility signal).

    Returns:
        Dict with mean_spearman_rho, n_episodes_evaluated, rho_std.
    """
    model.eval()
    rng = np.random.RandomState(RANDOM_STATE)

    if action_type_frequencies is None:
        return {"mean_spearman_rho": 0.0, "n_episodes_evaluated": 0, "rho_std": 0.0}

    n_episodes = min(num_episodes, len(test_ds))
    episode_indices = rng.choice(len(test_ds), size=n_episodes, replace=False)

    # Top-N most active players by total action count (pre-compute sums once)
    player_total_actions = {pid: sum(freqs.values()) for pid, freqs in action_type_frequencies.items()}
    top_players = sorted(player_total_actions, key=lambda p: player_total_actions[p], reverse=True)[:num_players]

    rho_values: list[float] = []

    with torch.no_grad():
        for ep_idx in episode_indices:
            sample = test_ds[int(ep_idx)]
            labels = sample["labels"]
            valid_positions = (labels != -100).nonzero(as_tuple=True)[0]
            if len(valid_positions) == 0:
                continue
            last_pos = int(valid_positions[-1].item())
            true_action = int(labels[last_pos].item())

            # Move sample to GPU once, clone player_ids for mutation
            batch = {
                k: v.unsqueeze(0).to(device)
                for k, v in sample.items()
                if k not in ("labels", "vaep_targets", "competition_id")
            }
            player_ids_base = batch["player_ids"].clone()

            log_probs: list[float] = []
            plausibility_scores: list[float] = []

            for player_idx in top_players:
                # Mutate only the focal player (position 0) — no re-transfer
                batch["player_ids"] = player_ids_base.clone()
                batch["player_ids"][0, 0] = player_idx

                action_logits, _ = model.predict(**batch)  # type: ignore[arg-type]
                logits_at_pos = action_logits[0, last_pos, :]
                log_prob = float(torch.log_softmax(logits_at_pos, dim=-1)[true_action].item())
                log_probs.append(log_prob)

                player_freqs = action_type_frequencies.get(player_idx, {})
                total_act = player_total_actions.get(player_idx, 1)
                plausibility = player_freqs.get(true_action, 0) / max(total_act, 1)
                plausibility_scores.append(float(plausibility))

            if len(log_probs) >= 2:
                rho, _ = spearmanr(log_probs, plausibility_scores)
                rho_f = float(rho)  # type: ignore[arg-type]
                if not np.isnan(rho_f):
                    rho_values.append(rho_f)

    mean_rho = float(np.mean(rho_values)) if rho_values else 0.0
    return {
        "mean_spearman_rho": mean_rho,
        "n_episodes_evaluated": len(rho_values),
        "rho_std": float(np.std(rho_values)) if rho_values else 0.0,
    }


# ---------------------------------------------------------------------------
# Training loop
# ---------------------------------------------------------------------------


def train_loop(
    train_ds: ScoutGPTDataset,
    val_ds: ScoutGPTDataset,
    config: ScoutGPTConfig,
    device: torch.device,
    epochs: int,
    batch_size: int,
    lr: float,
    patience: int,
    model: ScoutGPTDecoder | None = None,
) -> tuple[ScoutGPTDecoder, dict[str, list[float]]]:
    """Train ScoutGPT with autoregressive action loss + VAEP auxiliary loss.

    Args:
        model: Optional pre-built model.  When provided the model is used
            as-is (already on *device*); when ``None`` a fresh
            ``ScoutGPTDecoder(config)`` is created and moved to *device*.

    Returns:
        Tuple of (best model, training history dict).
    """
    resolved_model: ScoutGPTDecoder = model if model is not None else ScoutGPTDecoder(config).to(device)
    model = resolved_model
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    # Windows spawn-based multiprocessing crashes DataLoader workers unless
    # the caller uses ``if __name__ == '__main__':``.  Safe: 0 on Windows.
    if sys.platform == "win32":
        _nw = 0
    elif hasattr(os, "sched_getaffinity"):
        _nw = min(4, len(os.sched_getaffinity(0)))  # type: ignore[attr-defined]
    else:
        _nw = 4
    # pin_memory accelerates CPU→GPU transfer but is significantly slower on
    # Windows when called from a non-main thread (e.g. OpenEvolve's
    # asyncio.run_in_executor).  Only enable on main thread.
    _pin = device.type == "cuda" and threading.current_thread() is threading.main_thread()
    tl = DataLoader(
        train_ds,
        batch_size=batch_size,
        shuffle=True,
        num_workers=_nw,
        pin_memory=_pin,
        persistent_workers=_nw > 0,
    )
    vl = DataLoader(
        val_ds,
        batch_size=batch_size,
        shuffle=False,
        num_workers=_nw,
        pin_memory=_pin,
        persistent_workers=_nw > 0,
    )

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=WEIGHT_DECAY)
    total_steps = len(tl) * epochs
    scheduler = get_cosine_schedule_with_warmup(optimizer, int(total_steps * WARMUP_FRACTION), total_steps)

    action_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    vaep_criterion = nn.MSELoss(reduction="none")
    vaep_weight = config.vaep_loss_weight

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
            b = {k: v.to(device) for k, v in batch.items()}
            optimizer.zero_grad()
            action_logits, vaep_preds = model.predict(
                action_ids=b["action_ids"],
                start_x=b["start_x"],
                start_y=b["start_y"],
                end_x=b["end_x"],
                end_y=b["end_y"],
                result=b["result"],
                time_delta=b["time_delta"],
                player_ids=b["player_ids"],
                attention_mask=b["attention_mask"],
            )
            action_loss = action_criterion(
                action_logits.view(-1, config.vocab_size),
                b["labels"].view(-1),
            )

            valid_mask = (b["action_ids"] != BOS_TOKEN_ID) & b["attention_mask"]
            vaep_raw = vaep_criterion(vaep_preds.squeeze(-1), b["vaep_targets"])
            valid_count = valid_mask.sum().clamp(min=1)
            vaep_loss = (vaep_raw * valid_mask.float()).sum() / valid_count

            loss = action_loss + vaep_weight * vaep_loss
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

        v_loss, v_action, v_vaep, v_top1, v_top5 = eval_loop(
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


def eval_loop(
    model: ScoutGPTDecoder,
    vl: DataLoader,
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
    vaep_weight = config.vaep_loss_weight
    total_loss = 0.0
    total_action = 0.0
    total_vaep = 0.0
    correct_top1 = 0
    correct_top5 = 0
    total_valid = 0
    nb = 0

    with torch.no_grad():
        for batch in vl:
            b = {k: v.to(device) for k, v in batch.items()}
            action_logits, vaep_preds = model.predict(
                action_ids=b["action_ids"],
                start_x=b["start_x"],
                start_y=b["start_y"],
                end_x=b["end_x"],
                end_y=b["end_y"],
                result=b["result"],
                time_delta=b["time_delta"],
                player_ids=b["player_ids"],
                attention_mask=b["attention_mask"],
            )
            labels = b["labels"]
            action_loss = action_criterion(action_logits.view(-1, config.vocab_size), labels.view(-1))

            valid_mask = (b["action_ids"] != BOS_TOKEN_ID) & b["attention_mask"]
            vaep_raw = vaep_criterion(vaep_preds.squeeze(-1), b["vaep_targets"])
            valid_count = valid_mask.sum().clamp(min=1)
            vaep_loss = (vaep_raw * valid_mask.float()).sum() / valid_count

            total_action += action_loss.item()
            total_vaep += vaep_loss.item()
            total_loss += (action_loss + vaep_weight * vaep_loss).item()
            nb += 1

            label_mask = labels != -100
            if label_mask.any():
                valid_logits = action_logits[label_mask]
                valid_labels = labels[label_mask]
                preds_top1 = valid_logits.argmax(dim=-1)
                correct_top1 += (preds_top1 == valid_labels).sum().item()
                top5 = valid_logits.topk(min(5, config.vocab_size), dim=-1).indices
                correct_top5 += (top5 == valid_labels.unsqueeze(-1)).any(dim=-1).sum().item()
                total_valid += valid_labels.size(0)

    n = max(nb, 1)
    nv = max(total_valid, 1)
    return total_loss / n, total_action / n, total_vaep / n, correct_top1 / nv, correct_top5 / nv


def accuracy_by_bucket(
    model: ScoutGPTDecoder,
    test_ds: ScoutGPTDataset,
    device: torch.device,
    batch_size: int,
) -> dict[str, float]:
    """Compute top-1 accuracy bucketed by episode length quartile."""
    model.eval()

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

    loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=_EVAL_NUM_WORKERS)
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


def evaluate_and_report(
    model: ScoutGPTDecoder,
    test_ds: ScoutGPTDataset,
    train_data: pd.DataFrame,
    test_data: pd.DataFrame,
    device: torch.device,
    history: dict[str, list[float]],
    config: ScoutGPTConfig,
    batch_size: int,
) -> dict[str, Any]:
    """Compute full evaluation suite and return metrics dict."""
    action_criterion = nn.CrossEntropyLoss(ignore_index=-100)
    vaep_criterion = nn.MSELoss(reduction="none")
    test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=_EVAL_NUM_WORKERS)

    test_loss, test_action_loss, test_vaep_loss, test_top1, test_top5 = eval_loop(
        model, test_loader, config, device, action_criterion, vaep_criterion
    )
    logger.info("Test — loss=%.4f top1=%.4f top5=%.4f", test_loss, test_top1, test_top5)

    bucket_metrics = accuracy_by_bucket(model, test_ds, device, batch_size)
    logger.info("Bucket accuracies: %s", {k: f"{v:.4f}" for k, v in bucket_metrics.items()})

    baselines = compute_baselines(test_ds, train_data)
    logger.info(
        "Baselines — most_frequent=%.4f bigram=%.4f",
        baselines["baseline_most_frequent_accuracy"],
        baselines["baseline_bigram_accuracy"],
    )

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

    cross_source = _cross_source_accuracy(model, test_data, config, device, batch_size)
    cross_source_gap = 0.0
    if cross_source:
        source_accs = list(cross_source.values())
        cross_source_gap = max(source_accs) - min(source_accs)
        logger.info("Cross-source accuracy gap: %.4f (%s)", cross_source_gap, cross_source)

    return {
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


def _cross_source_accuracy(
    model: ScoutGPTDecoder,
    test_data: pd.DataFrame,
    config: ScoutGPTConfig,
    device: torch.device,
    batch_size: int,
) -> dict[str, float]:
    """Per-data-source top-1 accuracy on test set."""
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

        parsed = build_datasets(subset)  # type: ignore[arg-type]
        (atypes, sxs, sys_, exs, eys, res, vaeps, tds, pidxs, comp_ids) = parsed
        ds = ScoutGPTDataset(atypes, sxs, sys_, exs, eys, res, vaeps, tds, pidxs, competition_ids=comp_ids)
        loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=_EVAL_NUM_WORKERS)

        _, _, _, top1, _ = eval_loop(model, loader, config, device, action_criterion, vaep_criterion)
        source_name = str(source).replace(" ", "_").lower()
        source_accuracies[source_name] = top1
        logger.info("  source=%s n=%d top1=%.4f", source, len(subset), top1)

    return source_accuracies
