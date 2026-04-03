"""ScoutGPT training: dataset, training loop, evaluation, scheduling.

Domain logic for ScoutGPT decoder training. The HF Jobs entry point
(``scripts/train_scoutgpt_hf.py``) is a thin wrapper that handles I/O,
checkpointing, cost recording, and MLflow logging.
"""

from __future__ import annotations

import json
import logging
import math
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


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_training_data(
    hf_token: str,
    dataset_repo: str,
) -> tuple[pd.DataFrame, dict[str, int], str]:
    """Load episodes and player ID map from HF Hub.

    Returns:
        Tuple of (episodes DataFrame, player_id_map dict, dataset SHA).
    """
    import pyarrow.parquet as pq
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=hf_token)
    all_items = list(api.list_repo_tree(dataset_repo, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        msg = f"No parquet files found in {dataset_repo}"
        raise RuntimeError(msg)

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local_path = hf_hub_download(dataset_repo, pf, repo_type="dataset", token=hf_token)
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
        map_path = hf_hub_download(dataset_repo, text_files[0], repo_type="dataset", token=hf_token)
    else:
        map_path = hf_hub_download(dataset_repo, map_files[0], repo_type="dataset", token=hf_token)

    with open(map_path, encoding="utf-8") as f:
        player_id_map: dict[str, int] = json.load(f)
    logger.info("Player ID map: %d players", len(player_id_map))

    dataset_info = api.repo_info(repo_id=dataset_repo, repo_type="dataset")
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
        self.action_types = action_types
        self.start_xs = start_xs
        self.start_ys = start_ys
        self.end_xs = end_xs
        self.end_ys = end_ys
        self.results = results
        self.vaep_values = vaep_values
        self.time_deltas = time_deltas
        self.player_idxs = player_idxs
        self.max_seq_len = max_seq_len
        self.competition_ids = competition_ids

    def __len__(self) -> int:
        return len(self.action_types)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor]:
        atypes = self.action_types[idx]
        sxs = self.start_xs[idx]
        sys_ = self.start_ys[idx]
        exs = self.end_xs[idx]
        eys = self.end_ys[idx]
        res = self.results[idx]
        vaeps = self.vaep_values[idx]
        tds = self.time_deltas[idx]
        pidxs = self.player_idxs[idx]

        # Truncate to max_seq_len - 1 (leave room for BOS)
        max_actions = self.max_seq_len - 1
        ep_len = min(len(atypes), max_actions)
        total_len = ep_len + 1  # +1 for BOS

        # Initialize padded tensors
        action_ids = torch.full((self.max_seq_len,), PAD_TOKEN_ID, dtype=torch.long)
        start_x = torch.zeros(self.max_seq_len, dtype=torch.float32)
        start_y = torch.zeros(self.max_seq_len, dtype=torch.float32)
        end_x = torch.zeros(self.max_seq_len, dtype=torch.float32)
        end_y = torch.zeros(self.max_seq_len, dtype=torch.float32)
        result = torch.zeros(self.max_seq_len, dtype=torch.long)
        time_delta = torch.zeros(self.max_seq_len, dtype=torch.float32)
        player_ids = torch.zeros(self.max_seq_len, dtype=torch.long)
        attention_mask = torch.zeros(self.max_seq_len, dtype=torch.bool)

        # Position 0: BOS conditioning token
        # Focal player = player who performs the first action
        action_ids[0] = BOS_TOKEN_ID
        player_ids[0] = pidxs[0] if ep_len > 0 else 0
        attention_mask[0] = True

        # Positions 1..ep_len: actual episode actions
        if ep_len > 0:
            action_ids[1:total_len] = torch.tensor(atypes[:ep_len], dtype=torch.long)
            start_x[1:total_len] = torch.tensor(sxs[:ep_len], dtype=torch.float32)
            start_y[1:total_len] = torch.tensor(sys_[:ep_len], dtype=torch.float32)
            end_x[1:total_len] = torch.tensor(exs[:ep_len], dtype=torch.float32)
            end_y[1:total_len] = torch.tensor(eys[:ep_len], dtype=torch.float32)
            result[1:total_len] = torch.tensor(res[:ep_len], dtype=torch.long)
            time_delta[1:total_len] = torch.tensor(tds[:ep_len], dtype=torch.float32)
            player_ids[1:total_len] = torch.tensor(pidxs[:ep_len], dtype=torch.long)
            attention_mask[1:total_len] = True

        # Autoregressive labels: label at position t = action_ids[t+1]
        labels = torch.full((self.max_seq_len,), -100, dtype=torch.long)
        vaep_targets = torch.zeros(self.max_seq_len, dtype=torch.float32)

        if ep_len > 0:
            for t in range(total_len - 1):
                next_id = action_ids[t + 1].item()
                labels[t] = next_id if next_id != PAD_TOKEN_ID else -100

            # VAEP targets aligned with actual actions (positions 1..total_len-1)
            vaep_targets[1:total_len] = torch.tensor(vaeps[:ep_len], dtype=torch.float32)

        out: dict[str, torch.Tensor] = {
            "action_ids": action_ids,
            "start_x": start_x,
            "start_y": start_y,
            "end_x": end_x,
            "end_y": end_y,
            "result": result,
            "time_delta": time_delta,
            "player_ids": player_ids,
            "attention_mask": attention_mask,
            "labels": labels,
            "vaep_targets": vaep_targets,
        }
        if self.competition_ids is not None:
            out["competition_id"] = torch.tensor(self.competition_ids[idx], dtype=torch.long)
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
    data: pd.DataFrame,
) -> dict[int, dict[int, float]]:
    """Build per-player action type frequency table from training data.

    Returns:
        {player_idx: {action_type: count}}
    """
    freq: dict[int, dict[int, float]] = {}
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

    # Bigram: for each action type, most likely successor
    bigram_next: dict[int, int] = {}
    for action_type in range(VOCAB_SIZE):
        candidates = {k: v for k, v in bigram_counts.items() if k[0] == action_type}
        if candidates:
            bigram_next[action_type] = max(candidates, key=lambda k: candidates[k])[1]
        else:
            bigram_next[action_type] = most_frequent

    mf_correct = 0
    bg_correct = 0
    total = 0

    for i in range(len(test_ds)):
        sample = test_ds[i]
        sample_labels = sample["labels"]
        sample_actions = sample["action_ids"]
        for t in range(len(sample_labels)):
            if sample_labels[t].item() == -100:
                continue
            total += 1
            true_label = sample_labels[t].item()
            if true_label == most_frequent:
                mf_correct += 1
            current_action = sample_actions[t].item()
            if current_action < VOCAB_SIZE and bigram_next.get(int(current_action)) == true_label:
                bg_correct += 1

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

    # Top-N most active players by total action count
    player_activity = {pid: sum(freqs.values()) for pid, freqs in action_type_frequencies.items()}
    top_players = sorted(player_activity, key=lambda p: player_activity[p], reverse=True)[:num_players]

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

            log_probs: list[float] = []
            plausibility_scores: list[float] = []

            for player_idx in top_players:
                batch = {
                    k: v.unsqueeze(0).to(device)
                    for k, v in sample.items()
                    if k not in ("labels", "vaep_targets", "competition_id")
                }
                batch["player_ids"][0, 0] = player_idx

                action_logits, _ = model.predict(**batch)  # type: ignore[arg-type]
                logits_at_pos = action_logits[0, last_pos, :]
                log_prob = float(torch.log_softmax(logits_at_pos, dim=-1)[true_action].item())
                log_probs.append(log_prob)

                player_freqs = action_type_frequencies.get(player_idx, {})
                total_actions = sum(player_freqs.values())
                plausibility = player_freqs.get(true_action, 0) / max(total_actions, 1)
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
            action_loss = action_criterion(
                action_logits.view(-1, config.vocab_size),
                batch["labels"].to(device).view(-1),
            )

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
    test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=0)

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
        loader = DataLoader(ds, batch_size=batch_size, shuffle=False, num_workers=0)

        _, _, _, top1, _ = eval_loop(model, loader, config, device, action_criterion, vaep_criterion)
        source_name = str(source).replace(" ", "_").lower()
        source_accuracies[source_name] = top1
        logger.info("  source=%s n=%d top1=%.4f", source, len(subset), top1)

    return source_accuracies
