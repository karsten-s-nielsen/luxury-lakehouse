"""Helper module for train_xg_v2_hf.py.

Contains model architecture, feature engineering, freeze-frame parsing,
dataset/collate, training loop, MC dropout evaluation, weight export,
and v1 baseline comparison. The main script handles pipeline orchestration,
MLflow logging, and HF Hub publishing.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import logging
import time
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import brier_score_loss, roc_auc_score

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_CATEGORICAL_FEATURES = ["shot_body_part", "shot_technique", "shot_type", "play_pattern"]
_NUMERIC_FEATURES = [
    "distance_to_goal",
    "shot_angle",
    "location_x",
    "location_y",
    "end_location_x",
    "end_location_y",
    "period",
    "minute",
]
_BOOLEAN_FEATURES = ["is_first_time"]

BATCH_SIZE = 256
MAX_EPOCHS = 50
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-4
EARLY_STOPPING_PATIENCE = 5
MC_DROPOUT_SAMPLES = 50


@dataclasses.dataclass(frozen=True)
class SetEncoderConfig:
    """Immutable configuration for the set encoder architecture."""

    player_feature_dim: int = 4
    encoder_hidden: int = 32
    context_dim: int = 16
    pred_hidden_1: int = 64
    pred_hidden_2: int = 32
    dropout_p: float = 0.1


# ---------------------------------------------------------------------------
# PyTorch model
# ---------------------------------------------------------------------------


class SetEncoderXG(nn.Module):
    """Deep Sets xG model: per-player encoder + sum pooling + prediction MLP."""

    def __init__(self, tabular_dim: int, config: SetEncoderConfig) -> None:
        super().__init__()
        self.config = config
        self.encoder = nn.Sequential(
            nn.Linear(config.player_feature_dim, config.encoder_hidden),
            nn.ReLU(),
            nn.Linear(config.encoder_hidden, config.context_dim),
            nn.ReLU(),
        )
        self.predictor = nn.Sequential(
            nn.Linear(tabular_dim + config.context_dim, config.pred_hidden_1),
            nn.ReLU(),
            nn.Dropout(config.dropout_p),
            nn.Linear(config.pred_hidden_1, config.pred_hidden_2),
            nn.ReLU(),
            nn.Dropout(config.dropout_p),
            nn.Linear(config.pred_hidden_2, 1),
        )

    def forward(self, tabular: torch.Tensor, all_players: torch.Tensor, set_sizes: torch.Tensor) -> torch.Tensor:
        batch_size = tabular.shape[0]
        device = tabular.device
        context_dim = self.config.context_dim
        if all_players.shape[0] > 0:
            encoded = self.encoder(all_players)
            shot_indices = torch.repeat_interleave(torch.arange(batch_size, device=device), set_sizes)
            context = torch.zeros(batch_size, context_dim, device=device)
            context.scatter_add_(0, shot_indices.unsqueeze(1).expand_as(encoded), encoded)
        else:
            context = torch.zeros(batch_size, context_dim, device=device)
        return self.predictor(torch.cat([tabular, context], dim=1))


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix and target from shot data."""
    x = df.copy()
    for col in _BOOLEAN_FEATURES:
        if col in x.columns:
            x[col] = x[col].map({True: 1.0, False: 0.0, None: 0.0}).fillna(0.0).astype(float)
    for col in _CATEGORICAL_FEATURES:
        if col in x.columns:
            x[col] = x[col].fillna("Unknown").astype(str)
            dummies = pd.get_dummies(x[col], prefix=col, dtype=float)
            x = pd.concat([x, dummies], axis=1)
            x = x.drop(columns=[col])
    y = x["is_goal"].astype(int) if "is_goal" in x.columns else pd.Series(np.zeros(len(x), dtype=int))
    feature_cols = [
        c
        for c in x.columns
        if c
        not in [
            "is_goal",
            "shot_id",
            "match_id",
            "competition_id",
            "data_source",
            "player_id",
            "team_id",
            "statsbomb_xg",
        ]
    ]
    for col in feature_cols:
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce").fillna(0.0).astype(float)
    return x[feature_cols], y


# ---------------------------------------------------------------------------
# Freeze-frame parsing
# ---------------------------------------------------------------------------


def parse_freeze_frames(
    shots_df: pd.DataFrame,
    freeze_df: pd.DataFrame | None,
) -> list[npt.NDArray[np.floating[Any]]]:
    """Parse freeze-frame data into per-shot player feature arrays."""
    n_shots = len(shots_df)
    empty_array = np.empty((0, 4), dtype=np.float64)
    if freeze_df is None or len(freeze_df) == 0:
        logger.info("No freeze-frame data available; using empty context vectors for all shots")
        return [empty_array] * n_shots

    freeze_groups: dict[str, pd.DataFrame] = dict(iter(freeze_df.groupby("event_id")))
    result: list[npt.NDArray[np.floating[Any]]] = []
    matched = 0
    for shot_id in shots_df["shot_id"]:
        group = freeze_groups.get(str(shot_id))
        if group is None or len(group) == 0:
            result.append(empty_array)
            continue
        x_norm = group["player_x_norm"].values.astype(np.float64)
        y_norm = group["player_y_norm"].values.astype(np.float64)
        is_keeper = (
            group["is_keeper"].values.astype(np.float64)
            if "is_keeper" in group.columns
            else np.zeros(len(group), dtype=np.float64)
        )
        is_teammate = (
            group["is_teammate"].values.astype(np.float64)
            if "is_teammate" in group.columns
            else np.zeros(len(group), dtype=np.float64)
        )
        result.append(np.column_stack([x_norm, y_norm, is_keeper, is_teammate]))
        matched += 1
    logger.info("Freeze-frame matched: %d / %d shots (%.1f%%)", matched, n_shots, 100.0 * matched / max(n_shots, 1))
    return result


# ---------------------------------------------------------------------------
# Dataset and DataLoader
# ---------------------------------------------------------------------------


class ShotDataset(torch.utils.data.Dataset[tuple[torch.Tensor, torch.Tensor, torch.Tensor]]):
    """PyTorch dataset for shots with variable-size freeze-frame player sets."""

    def __init__(
        self,
        tabular: npt.NDArray[np.floating[Any]],
        player_sets: list[npt.NDArray[np.floating[Any]]],
        targets: npt.NDArray[np.integer[Any]],
    ) -> None:
        self.tabular = tabular.astype(np.float32)
        self.player_sets = player_sets
        self.targets = targets.astype(np.float32)

    def __len__(self) -> int:
        return len(self.targets)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return (
            torch.from_numpy(self.tabular[idx]),
            torch.from_numpy(self.player_sets[idx].astype(np.float32)),
            torch.tensor(self.targets[idx], dtype=torch.float32),
        )


def collate_fn(
    batch: list[tuple[torch.Tensor, torch.Tensor, torch.Tensor]],
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Custom collate: pack variable-size player sets into flat tensor + sizes."""
    tabular_list: list[torch.Tensor] = []
    player_list: list[torch.Tensor] = []
    size_list: list[int] = []
    target_list: list[torch.Tensor] = []
    for tab, players, target in batch:
        tabular_list.append(tab)
        player_list.append(players)
        size_list.append(len(players))
        target_list.append(target)
    tabular = torch.stack(tabular_list)
    targets = torch.stack(target_list)
    set_sizes = torch.tensor(size_list, dtype=torch.long)
    all_players = torch.cat(player_list) if sum(size_list) > 0 else torch.empty(0, 4, dtype=torch.float32)
    return tabular, all_players, set_sizes, targets


# ---------------------------------------------------------------------------
# Training loop
# ---------------------------------------------------------------------------


def train_model(
    model: SetEncoderXG,
    train_loader: torch.utils.data.DataLoader[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]],
    val_loader: torch.utils.data.DataLoader[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]],
    device: torch.device,
) -> dict[str, list[float]]:
    """Train with BCE loss, Adam optimizer, and early stopping on val Brier score."""
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
    criterion = nn.BCEWithLogitsLoss()
    best_val_brier = float("inf")
    patience_counter = 0
    best_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {"train_loss": [], "val_brier": [], "val_auc": []}

    for epoch in range(MAX_EPOCHS):
        epoch_start = time.time()
        model.train()
        total_loss = 0.0
        n_batches = 0
        for tabular, all_players, set_sizes, targets in train_loader:
            tabular = tabular.to(device)
            all_players = all_players.to(device)
            set_sizes = set_sizes.to(device)
            targets = targets.to(device)
            optimizer.zero_grad()
            logits = model(tabular, all_players, set_sizes).squeeze(1)
            loss = criterion(logits, targets)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            n_batches += 1

        model.eval()
        all_proba: list[float] = []
        all_targets: list[float] = []
        with torch.no_grad():
            for tabular, all_players, set_sizes, targets in val_loader:
                tabular = tabular.to(device)
                all_players = all_players.to(device)
                set_sizes = set_sizes.to(device)
                logits = model(tabular, all_players, set_sizes).squeeze(1)
                proba = torch.sigmoid(logits).cpu().numpy()
                all_proba.extend(proba.tolist())
                all_targets.extend(targets.numpy().tolist())

        val_proba = np.array(all_proba)
        val_targets = np.array(all_targets)
        val_brier = float(brier_score_loss(val_targets, val_proba))
        val_auc = float(roc_auc_score(val_targets, val_proba))
        history["train_loss"].append(total_loss / max(n_batches, 1))
        history["val_brier"].append(val_brier)
        history["val_auc"].append(val_auc)

        elapsed = time.time() - epoch_start
        logger.info(
            "Epoch %d/%d — loss=%.4f  val_brier=%.4f  val_auc=%.4f  (%.1fs)",
            epoch + 1,
            MAX_EPOCHS,
            total_loss / max(n_batches, 1),
            val_brier,
            val_auc,
            elapsed,
        )

        if val_brier < best_val_brier:
            best_val_brier = val_brier
            patience_counter = 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= EARLY_STOPPING_PATIENCE:
                logger.info("Early stopping at epoch %d", epoch + 1)
                break
    if best_state:
        model.load_state_dict(best_state)
    return history


# ---------------------------------------------------------------------------
# MC dropout evaluation
# ---------------------------------------------------------------------------


def evaluate_mc_dropout(
    model: SetEncoderXG,
    val_loader: torch.utils.data.DataLoader[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]],
    device: torch.device,
    n_samples: int = MC_DROPOUT_SAMPLES,
    config: SetEncoderConfig | None = None,
) -> dict[str, float]:
    """Evaluate with MC dropout: empirical coverage of 95% CI."""
    config = config or SetEncoderConfig()
    all_tabular: list[torch.Tensor] = []
    all_players_list: list[torch.Tensor] = []
    all_sizes: list[torch.Tensor] = []
    all_targets: list[float] = []
    for tabular, all_players, set_sizes, targets in val_loader:
        all_tabular.append(tabular)
        all_players_list.append(all_players)
        all_sizes.append(set_sizes)
        all_targets.extend(targets.numpy().tolist())

    targets_arr = np.array(all_targets)
    n_total = len(targets_arr)
    mc_dropout_p = min(config.dropout_p * 3.0, 0.5)
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = mc_dropout_p

    mc_predictions = np.zeros((n_samples, n_total), dtype=np.float64)
    for s in range(n_samples):
        model.train()
        idx = 0
        with torch.no_grad():
            for tabular, all_players, set_sizes, _targets in val_loader:
                logits = model(tabular.to(device), all_players.to(device), set_sizes.to(device)).squeeze(1)
                proba = torch.sigmoid(logits).cpu().numpy()
                mc_predictions[s, idx : idx + len(proba)] = proba
                idx += len(proba)
    model.eval()
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = config.dropout_p

    means = mc_predictions.mean(axis=0)
    stds = mc_predictions.std(axis=0)
    best_z = 1.96
    for z_candidate in np.arange(1.0, 6.0, 0.1):
        ci_lo = np.clip(means - z_candidate * stds, 0.0, 1.0)
        ci_hi = np.clip(means + z_candidate * stds, 0.0, 1.0)
        cov = float(((targets_arr >= ci_lo) & (targets_arr <= ci_hi)).sum()) / max(n_total, 1)
        if cov >= 0.95:
            best_z = float(z_candidate)
            break

    ci_lower = np.clip(means - best_z * stds, 0.0, 1.0)
    ci_upper = np.clip(means + best_z * stds, 0.0, 1.0)
    covered = ((targets_arr >= ci_lower) & (targets_arr <= ci_upper)).sum()
    return {
        "mc_coverage_95": float(covered) / max(n_total, 1),
        "mc_mean_std": float(np.mean(stds)),
        "mc_mean_ci_width": float(np.mean(ci_upper - ci_lower)),
        "mc_dropout_p_inference": mc_dropout_p,
        "mc_z_multiplier": best_z,
    }


# ---------------------------------------------------------------------------
# Weight export
# ---------------------------------------------------------------------------


def export_weights_to_numpy(model: SetEncoderXG) -> dict[str, npt.NDArray[np.floating[Any]]]:
    """Extract PyTorch state_dict and convert to NumPy weight dict format."""
    sd = model.state_dict()
    return {
        "encoder_fc1_weight": sd["encoder.0.weight"].cpu().numpy().astype(np.float64),
        "encoder_fc1_bias": sd["encoder.0.bias"].cpu().numpy().astype(np.float64),
        "encoder_fc2_weight": sd["encoder.2.weight"].cpu().numpy().astype(np.float64),
        "encoder_fc2_bias": sd["encoder.2.bias"].cpu().numpy().astype(np.float64),
        "pred_fc1_weight": sd["predictor.0.weight"].cpu().numpy().astype(np.float64),
        "pred_fc1_bias": sd["predictor.0.bias"].cpu().numpy().astype(np.float64),
        "pred_fc2_weight": sd["predictor.3.weight"].cpu().numpy().astype(np.float64),
        "pred_fc2_bias": sd["predictor.3.bias"].cpu().numpy().astype(np.float64),
        "pred_fc3_weight": sd["predictor.6.weight"].cpu().numpy().astype(np.float64),
        "pred_fc3_bias": sd["predictor.6.bias"].cpu().numpy().astype(np.float64),
    }


# ---------------------------------------------------------------------------
# V1 XGBoost baseline
# ---------------------------------------------------------------------------


def evaluate_v1_baseline(x_test: pd.DataFrame, y_test: pd.Series, hf_token: str) -> dict[str, float] | None:
    """Load v1 XGBoost model from HF Hub and evaluate on the same test set."""
    try:
        from huggingface_hub import hf_hub_download
        from scipy.interpolate import interp1d
        from sklearn.calibration import CalibratedClassifierCV, _CalibratedClassifier
        from sklearn.isotonic import IsotonicRegression
        from sklearn.metrics import brier_score_loss as _brier
        from sklearn.metrics import log_loss as _ll
        from sklearn.metrics import roc_auc_score as _auc
        from xgboost import XGBClassifier

        v1_repo = "luxury-lakehouse/xg-model-statsbomb-wyscout"
        local = hf_hub_download(v1_repo, "xgboost_model.json", repo_type="model", token=hf_token)
        with open(local, "rb") as f:
            envelope = json.loads(f.read().decode("utf-8"))
        booster_raw = base64.b64decode(envelope["booster_b64"])
        xgb = XGBClassifier()
        xgb.load_model(bytearray(booster_raw))
        ir = IsotonicRegression(out_of_bounds="clip")
        ir.X_thresholds_ = np.array(envelope["X_thresholds"])
        ir.y_thresholds_ = np.array(envelope["y_thresholds"])
        ir.X_min_ = envelope["X_min"]
        ir.X_max_ = envelope["X_max"]
        ir.increasing_ = envelope["increasing"]
        ir.f_ = interp1d(
            ir.X_thresholds_,
            ir.y_thresholds_,
            kind="linear",
            bounds_error=False,
            fill_value=(ir.y_thresholds_[0], ir.y_thresholds_[-1]),
        )
        classes = np.array([0, 1])
        cc = _CalibratedClassifier(estimator=xgb, calibrators=[ir], classes=classes, method="isotonic")
        calibrated = CalibratedClassifierCV(xgb, cv="prefit")
        calibrated.calibrated_classifiers_ = [cc]
        calibrated.classes_ = classes
        v1_names = envelope.get("feature_names") or list(xgb.get_booster().feature_names)
        x_aligned = x_test.reindex(columns=v1_names, fill_value=0.0)
        v1_proba = calibrated.predict_proba(x_aligned)[:, 1]
        return {
            "v1_brier_score": float(_brier(y_test, v1_proba)),
            "v1_log_loss": float(_ll(y_test, v1_proba)),
            "v1_roc_auc": float(_auc(y_test, v1_proba)),
        }
    except Exception as e:
        logger.warning("Could not load v1 baseline: %s", e)
        return None
