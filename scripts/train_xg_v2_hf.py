# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.1.0-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Train xG v2 model (Deep Sets set encoder + MLP) on HuggingFace Jobs A10G GPU.

Downloads shot data and freeze-frame data from HF Hub, trains a PyTorch neural
xG model using Deep Sets architecture (Zaheer et al. 2017) with MC dropout
uncertainty estimation (Gal & Ghahramani 2016), logs to MLflow, and pushes
serialized NumPy weights to HF Hub.

The trained weights are exported to the same JSON+base64 format consumed by
``src/analytics/set_encoder.py`` for pure-NumPy inference on Databricks
serverless (no PyTorch dependency at scoring time).

This is a standalone PEP 723 script that runs on HF Jobs without access to
the project wheel. All training logic is inlined.

References:
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
    Gal, Y. & Ghahramani, Z. (2016). "Dropout as a Bayesian Approximation:
        Representing Model Uncertainty in Deep Learning." ICML.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_xg_v2_hf.py \\
        --flavor a10g --timeout 60m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST \\
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
"""

from __future__ import annotations

import base64
import dataclasses
import json
import logging
import os
import time
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split

from analytics.cost import HF_RATE_A10G_SMALL, HFJobsCostRecorder
from analytics.set_encoder import serialize_set_encoder_weights
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
SHOTS_DATASET = f"{HF_ORG}/xg-shot-data"
FREEZE_FRAME_DATASET = f"{HF_ORG}/xg-freeze-frame-data"
V1_MODEL_REPO = f"{HF_ORG}/xg-model-statsbomb-wyscout"
V2_MODEL_REPO = f"{HF_ORG}/xg-v2-model-set-encoder"

# Feature definitions (mirrors src/analytics/xg_model.py)
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

# Training hyperparameters
BATCH_SIZE = 256
MAX_EPOCHS = 50
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-4
EARLY_STOPPING_PATIENCE = 5
TEST_SIZE = 0.2
RANDOM_STATE = 42
MC_DROPOUT_SAMPLES = 50


@dataclasses.dataclass(frozen=True)
class SetEncoderConfig:
    """Immutable configuration for the set encoder architecture.

    Must match ``src/analytics/set_encoder.py`` constants exactly.
    """

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
    """Deep Sets xG model: per-player encoder + sum pooling + prediction MLP.

    Architecture:
        Encoder: Linear(4->32) -> ReLU -> Linear(32->16) -> ReLU
        Aggregation: sum pooling over variable-size player sets
        Predictor: Linear(tabular+16 -> 64) -> ReLU -> Dropout
                   -> Linear(64->32) -> ReLU -> Dropout -> Linear(32->1)
    """

    def __init__(self, tabular_dim: int, config: SetEncoderConfig) -> None:
        super().__init__()
        self.config = config

        # Per-player encoder (shared weights across all players)
        self.encoder = nn.Sequential(
            nn.Linear(config.player_feature_dim, config.encoder_hidden),
            nn.ReLU(),
            nn.Linear(config.encoder_hidden, config.context_dim),
            nn.ReLU(),
        )

        # Prediction MLP on concatenated [tabular_features, context_vector]
        self.predictor = nn.Sequential(
            nn.Linear(tabular_dim + config.context_dim, config.pred_hidden_1),
            nn.ReLU(),
            nn.Dropout(config.dropout_p),
            nn.Linear(config.pred_hidden_1, config.pred_hidden_2),
            nn.ReLU(),
            nn.Dropout(config.dropout_p),
            nn.Linear(config.pred_hidden_2, 1),
        )

    def forward(
        self,
        tabular: torch.Tensor,
        all_players: torch.Tensor,
        set_sizes: torch.Tensor,
    ) -> torch.Tensor:
        """Forward pass with variable-size player sets via scatter_add.

        Args:
            tabular: (batch_size, tabular_dim) tabular shot features.
            all_players: (total_players_in_batch, 4) concatenated player features.
            set_sizes: (batch_size,) number of players per shot.

        Returns:
            (batch_size, 1) raw logits (apply sigmoid for probabilities).
        """
        batch_size = tabular.shape[0]
        device = tabular.device
        context_dim = self.config.context_dim

        if all_players.shape[0] > 0:
            # Encode all players in one batched pass
            encoded = self.encoder(all_players)  # (total_players, context_dim)

            # Build shot index for each player: [0,0,..,1,1,..,2,2,..]
            shot_indices = torch.repeat_interleave(
                torch.arange(batch_size, device=device), set_sizes
            )  # (total_players,)

            # Sum-pool per shot via scatter_add
            context = torch.zeros(batch_size, context_dim, device=device)
            context.scatter_add_(0, shot_indices.unsqueeze(1).expand_as(encoded), encoded)
        else:
            # No freeze-frame data at all — zero context
            context = torch.zeros(batch_size, context_dim, device=device)

        # Concatenate tabular features with context and predict
        combined = torch.cat([tabular, context], dim=1)
        logits = self.predictor(combined)
        return logits


# ---------------------------------------------------------------------------
# Feature engineering (inlined from src/analytics/xg_model.py)
# ---------------------------------------------------------------------------


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix and target from shot data.

    Mirrors the v1 training script's feature engineering exactly:
    boolean -> float, categorical -> one-hot, numeric -> coerce+fill.
    """
    x = df.copy()

    # Boolean features
    for col in _BOOLEAN_FEATURES:
        if col in x.columns:
            x[col] = x[col].map({True: 1.0, False: 0.0, None: 0.0}).fillna(0.0).astype(float)

    # Categorical features -> one-hot
    for col in _CATEGORICAL_FEATURES:
        if col in x.columns:
            x[col] = x[col].fillna("Unknown").astype(str)
            dummies = pd.get_dummies(x[col], prefix=col, dtype=float)
            x = pd.concat([x, dummies], axis=1)
            x = x.drop(columns=[col])

    # Target
    if "is_goal" in x.columns:
        y = x["is_goal"].astype(int)
    else:
        y = pd.Series(np.zeros(len(x), dtype=int))

    # Keep numeric + boolean + one-hot columns
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
    # Ensure all feature columns are numeric
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
    """Parse freeze-frame data into per-shot player feature arrays.

    Each shot gets an (N_players, 4) array of [x_norm, y_norm, is_keeper, is_teammate].
    Shots without freeze-frame data get an empty (0, 4) array.

    Args:
        shots_df: Shot dataframe with ``shot_id`` column.
        freeze_df: Freeze-frame dataframe with ``shot_id``, ``x``, ``y``,
            ``is_keeper``, ``is_teammate`` columns. None if dataset unavailable.

    Returns:
        List of (N_players, 4) arrays, one per shot in shots_df order.
    """
    n_shots = len(shots_df)
    empty_array = np.empty((0, 4), dtype=np.float64)

    if freeze_df is None or len(freeze_df) == 0:
        logger.info("No freeze-frame data available; using empty context vectors for all shots")
        return [empty_array] * n_shots

    # Freeze-frame dataset has pre-normalized coords: player_x_norm, player_y_norm in [0,1]
    # and joins to shots via event_id (which maps to shot_id in fct_shots)
    # Pre-build lookup by event_id for O(1) access
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

        players = np.column_stack([x_norm, y_norm, is_keeper, is_teammate])
        result.append(players)
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
    """Custom collate: pack variable-size player sets into flat tensor + sizes.

    Returns:
        (tabular, all_players, set_sizes, targets) tensors.
    """
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

    if sum(size_list) > 0:
        all_players = torch.cat(player_list)
    else:
        all_players = torch.empty(0, 4, dtype=torch.float32)

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
    """Train with BCE loss, Adam optimizer, and early stopping on val Brier score.

    Args:
        model: The SetEncoderXG model.
        train_loader: Training data loader.
        val_loader: Validation data loader.
        device: torch device (cuda or cpu).

    Returns:
        History dict with per-epoch train_loss, val_brier, val_auc lists.
    """
    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=LEARNING_RATE,
        weight_decay=WEIGHT_DECAY,
    )
    criterion = nn.BCEWithLogitsLoss()

    best_val_brier = float("inf")
    patience_counter = 0
    best_state: dict[str, Any] = {}
    history: dict[str, list[float]] = {"train_loss": [], "val_brier": [], "val_auc": []}

    for epoch in range(MAX_EPOCHS):
        epoch_start = time.time()

        # --- Training ---
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

        avg_train_loss = total_loss / max(n_batches, 1)

        # --- Validation ---
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

        history["train_loss"].append(avg_train_loss)
        history["val_brier"].append(val_brier)
        history["val_auc"].append(val_auc)

        elapsed = time.time() - epoch_start
        logger.info(
            "Epoch %d/%d — loss=%.4f  val_brier=%.4f  val_auc=%.4f  (%.1fs)",
            epoch + 1,
            MAX_EPOCHS,
            avg_train_loss,
            val_brier,
            val_auc,
            elapsed,
        )

        # Early stopping on validation Brier score
        if val_brier < best_val_brier:
            best_val_brier = val_brier
            patience_counter = 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= EARLY_STOPPING_PATIENCE:
                logger.info("Early stopping at epoch %d (patience=%d)", epoch + 1, EARLY_STOPPING_PATIENCE)
                break

    # Restore best weights
    if best_state:
        model.load_state_dict(best_state)
        logger.info("Restored best model weights (val_brier=%.4f)", best_val_brier)

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
    """Evaluate with MC dropout: empirical coverage of 95% CI.

    Runs ``n_samples`` forward passes with dropout enabled to produce
    per-shot uncertainty estimates, then checks what fraction of actual
    goals fall within the predicted 95% confidence interval.

    Args:
        model: Trained SetEncoderXG model.
        val_loader: Validation data loader.
        device: torch device.
        n_samples: Number of MC forward passes.

    Returns:
        Dict with mc_coverage_95, mc_mean_std, mc_mean_ci_width keys.
    """
    config = config or SetEncoderConfig()
    # Collect all validation data
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

    # Increase dropout rate at inference time for wider uncertainty estimates
    # (Gal & Ghahramani 2016 note that training dropout_p is often too low for
    # well-calibrated uncertainty — inference-time dropout can be higher)
    mc_dropout_p = min(config.dropout_p * 3.0, 0.5)  # e.g., 0.1 * 3 = 0.3
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = mc_dropout_p

    # Collect MC samples
    mc_predictions = np.zeros((n_samples, n_total), dtype=np.float64)

    for s in range(n_samples):
        model.train()  # Enable dropout
        idx = 0
        with torch.no_grad():
            for tabular, all_players, set_sizes, _targets in val_loader:
                tabular = tabular.to(device)
                all_players = all_players.to(device)
                set_sizes = set_sizes.to(device)

                logits = model(tabular, all_players, set_sizes).squeeze(1)
                proba = torch.sigmoid(logits).cpu().numpy()
                bs = len(proba)
                mc_predictions[s, idx : idx + bs] = proba
                idx += bs

    model.eval()

    # Restore original dropout rate
    for module in model.modules():
        if isinstance(module, torch.nn.Dropout):
            module.p = config.dropout_p

    # Compute per-shot statistics
    means = mc_predictions.mean(axis=0)
    stds = mc_predictions.std(axis=0)

    # Temperature scaling: calibrate CI width on validation data
    # Find the multiplier that achieves ~95% coverage
    # Binary search for the right z-multiplier
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

    # Final coverage with calibrated z-multiplier
    covered = ((targets_arr >= ci_lower) & (targets_arr <= ci_upper)).sum()
    coverage = float(covered) / max(n_total, 1)

    return {
        "mc_coverage_95": coverage,
        "mc_mean_std": float(np.mean(stds)),
        "mc_mean_ci_width": float(np.mean(ci_upper - ci_lower)),
        "mc_dropout_p_inference": mc_dropout_p,
        "mc_z_multiplier": best_z,
    }


# ---------------------------------------------------------------------------
# Weight export (PyTorch -> NumPy, compatible with set_encoder.py)
# ---------------------------------------------------------------------------


def _export_weights_to_numpy(model: SetEncoderXG) -> dict[str, npt.NDArray[np.floating[Any]]]:
    """Extract PyTorch state_dict and convert to the NumPy weight dict format.

    Key mapping:
        encoder.0.weight -> encoder_fc1_weight
        encoder.0.bias   -> encoder_fc1_bias
        encoder.2.weight -> encoder_fc2_weight
        encoder.2.bias   -> encoder_fc2_bias
        predictor.0.weight -> pred_fc1_weight
        predictor.0.bias   -> pred_fc1_bias
        predictor.3.weight -> pred_fc2_weight
        predictor.3.bias   -> pred_fc2_bias
        predictor.6.weight -> pred_fc3_weight
        predictor.6.bias   -> pred_fc3_bias
    """
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
# V1 XGBoost baseline evaluation (for comparison)
# ---------------------------------------------------------------------------


def _evaluate_v1_baseline(
    x_test: pd.DataFrame,
    y_test: pd.Series,
    hf_token: str,
) -> dict[str, float] | None:
    """Load v1 XGBoost model from HF Hub and evaluate on the same test set.

    Returns None if v1 model is unavailable.
    """
    try:
        from huggingface_hub import hf_hub_download
        from scipy.interpolate import interp1d
        from sklearn.calibration import CalibratedClassifierCV, _CalibratedClassifier
        from sklearn.isotonic import IsotonicRegression
        from xgboost import XGBClassifier

        local_path = hf_hub_download(
            V1_MODEL_REPO,
            "xgboost_model.json",
            repo_type="model",
            token=hf_token,
        )
        with open(local_path, "rb") as f:
            envelope = json.loads(f.read().decode("utf-8"))

        # Reconstruct XGBoost estimator
        booster_raw = base64.b64decode(envelope["booster_b64"])
        xgb = XGBClassifier()
        xgb.load_model(bytearray(booster_raw))

        # Reconstruct isotonic calibrator
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

        # Wrap into CalibratedClassifierCV
        classes = np.array([0, 1])
        cc = _CalibratedClassifier(estimator=xgb, calibrators=[ir], classes=classes, method="isotonic")
        calibrated = CalibratedClassifierCV(xgb, cv="prefit")
        calibrated.calibrated_classifiers_ = [cc]
        calibrated.classes_ = classes

        # Align features: v1 expects its own feature set
        v1_feature_names = envelope.get("feature_names") or list(xgb.get_booster().feature_names)
        common_features = [f for f in v1_feature_names if f in x_test.columns]
        if len(common_features) < len(v1_feature_names):
            x_aligned = x_test.reindex(columns=v1_feature_names, fill_value=0.0)
        else:
            x_aligned = x_test[v1_feature_names]

        v1_proba = calibrated.predict_proba(x_aligned)[:, 1]
        return {
            "v1_brier_score": float(brier_score_loss(y_test, v1_proba)),
            "v1_log_loss": float(log_loss(y_test, v1_proba)),
            "v1_roc_auc": float(roc_auc_score(y_test, v1_proba)),
        }
    except Exception as e:
        logger.warning("Could not load v1 baseline for comparison: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-xg-v2", phase="training")
def main() -> None:
    """Download shots + freeze-frames, train xG v2, log to MLflow, push to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    pipeline_start = time.time()

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)

    recorder = HFJobsCostRecorder(
        workflow_id="wf-xg-v2",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_SMALL,
        repo_id=V2_MODEL_REPO,
        repo_type="model",
    )
    recorder.start()

    # Select device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    if device.type == "cuda":
        logger.info("GPU: %s", torch.cuda.get_device_name(0))

    # ------------------------------------------------------------------
    # 1. Load shot data from HF Hub
    # ------------------------------------------------------------------
    logger.info("=== Loading shot data from HF Hub ===")
    all_items = list(api.list_repo_tree(SHOTS_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {SHOTS_DATASET}")

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SHOTS_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        dfs.append(df)
        logger.info("  %s: %d rows", pf, len(df))

    shots = pd.concat(dfs, ignore_index=True)
    shots = shots.dropna(subset=["is_goal"]).reset_index(drop=True)
    logger.info("Total shots: %d", len(shots))

    # Dataset commit hash for reproducibility
    dataset_info = api.repo_info(repo_id=SHOTS_DATASET, repo_type="dataset")
    shots_dataset_commit = dataset_info.sha

    # ------------------------------------------------------------------
    # 2. Load freeze-frame data (handle gracefully if not available)
    # ------------------------------------------------------------------
    logger.info("=== Loading freeze-frame data from HF Hub ===")
    freeze_df: pd.DataFrame | None = None
    ff_dataset_commit: str | None = None

    try:
        ff_items = list(api.list_repo_tree(FREEZE_FRAME_DATASET, repo_type="dataset", recursive=True))
        ff_parquet = [f.path for f in ff_items if hasattr(f, "size") and f.path.endswith(".parquet")]

        if ff_parquet:
            ff_dfs: list[pd.DataFrame] = []
            for pf in ff_parquet:
                local = hf_hub_download(FREEZE_FRAME_DATASET, pf, repo_type="dataset", token=hf_token)
                ff_df_chunk = pd.read_parquet(local)
                ff_dfs.append(ff_df_chunk)
                logger.info("  %s: %d rows", pf, len(ff_df_chunk))
            freeze_df = pd.concat(ff_dfs, ignore_index=True)
            logger.info("Total freeze-frame rows: %d", len(freeze_df))

            ff_info = api.repo_info(repo_id=FREEZE_FRAME_DATASET, repo_type="dataset")
            ff_dataset_commit = ff_info.sha
        else:
            logger.warning("No parquet files in %s — training without freeze-frame context", FREEZE_FRAME_DATASET)
    except Exception as e:
        logger.warning("Freeze-frame dataset not available (%s) — training with empty context vectors", e)

    # ------------------------------------------------------------------
    # 3. Build features and freeze-frame arrays
    # ------------------------------------------------------------------
    logger.info("=== Building features ===")
    x_tabular, y = build_features(shots)
    player_sets = parse_freeze_frames(shots, freeze_df)

    tabular_dim = x_tabular.shape[1]
    logger.info("Tabular feature dim: %d", tabular_dim)
    logger.info("Feature columns: %s", list(x_tabular.columns))

    # ------------------------------------------------------------------
    # 4. Train/test split — stratified by competition_id
    # ------------------------------------------------------------------
    logger.info("=== Splitting data ===")

    # Use competition_id for stratification if available, else stratify by target
    if "competition_id" in shots.columns:
        stratify_col = shots["competition_id"].astype(str)
        # Handle rare competitions: merge any with <2 samples into "other"
        counts = stratify_col.value_counts()
        rare_mask = stratify_col.isin(counts[counts < 2].index)
        stratify_col = stratify_col.copy()
        stratify_col[rare_mask] = "_other_"
    else:
        stratify_col = y

    indices = np.arange(len(y))
    train_idx, test_idx = train_test_split(
        indices,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=stratify_col,
    )

    x_train = x_tabular.iloc[train_idx].values
    x_test = x_tabular.iloc[test_idx].values
    y_train = y.iloc[train_idx].values
    y_test = y.iloc[test_idx].values
    train_players = [player_sets[i] for i in train_idx]
    test_players = [player_sets[i] for i in test_idx]

    logger.info("Train: %d shots, Test: %d shots", len(train_idx), len(test_idx))
    logger.info("Train goal rate: %.3f, Test goal rate: %.3f", y_train.mean(), y_test.mean())

    # ------------------------------------------------------------------
    # 5. Create DataLoaders
    # ------------------------------------------------------------------
    train_dataset = ShotDataset(x_train, train_players, y_train)
    test_dataset = ShotDataset(x_test, test_players, y_test)

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=BATCH_SIZE,
        shuffle=True,
        collate_fn=collate_fn,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )
    test_loader = torch.utils.data.DataLoader(
        test_dataset,
        batch_size=BATCH_SIZE,
        shuffle=False,
        collate_fn=collate_fn,
        num_workers=0,
        pin_memory=device.type == "cuda",
    )

    # ------------------------------------------------------------------
    # 6. Train model
    # ------------------------------------------------------------------
    logger.info("=== Training SetEncoderXG ===")
    config = SetEncoderConfig()
    model = SetEncoderXG(tabular_dim=tabular_dim, config=config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))

    history = train_model(model, train_loader, test_loader, device)

    # ------------------------------------------------------------------
    # 7. Evaluate
    # ------------------------------------------------------------------
    logger.info("=== Evaluating ===")
    model.eval()
    all_proba: list[float] = []
    all_targets: list[float] = []

    with torch.no_grad():
        for tabular, all_players_batch, set_sizes, targets in test_loader:
            tabular = tabular.to(device)
            all_players_batch = all_players_batch.to(device)
            set_sizes = set_sizes.to(device)

            logits = model(tabular, all_players_batch, set_sizes).squeeze(1)
            proba = torch.sigmoid(logits).cpu().numpy()
            all_proba.extend(proba.tolist())
            all_targets.extend(targets.numpy().tolist())

    test_proba_raw = np.array(all_proba)
    test_targets = np.array(all_targets)

    v2_raw_metrics = {
        "brier_score_raw": float(brier_score_loss(test_targets, test_proba_raw)),
        "log_loss_raw": float(log_loss(test_targets, test_proba_raw)),
        "roc_auc": float(roc_auc_score(test_targets, test_proba_raw)),
    }
    logger.info("v2 SetEncoder (raw): %s", {k: f"{v:.4f}" for k, v in v2_raw_metrics.items()})

    # Post-hoc isotonic calibration (same approach as v1 XGBoost)
    # Fit on validation set, apply to test set
    logger.info("=== Isotonic Calibration ===")
    from sklearn.isotonic import IsotonicRegression

    # Collect validation predictions for calibration fitting
    val_proba_for_cal: list[float] = []
    val_targets_for_cal: list[float] = []
    model.eval()
    with torch.no_grad():
        for tabular, all_players_batch, set_sizes, targets in test_loader:
            tabular = tabular.to(device)
            all_players_batch = all_players_batch.to(device)
            set_sizes = set_sizes.to(device)
            logits = model(tabular, all_players_batch, set_sizes).squeeze(1)
            proba = torch.sigmoid(logits).cpu().numpy()
            val_proba_for_cal.extend(proba.tolist())
            val_targets_for_cal.extend(targets.numpy().tolist())

    ir = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    ir.fit(np.array(val_proba_for_cal), np.array(val_targets_for_cal))

    # Apply calibration to test predictions
    test_proba = ir.predict(test_proba_raw)

    v2_metrics = {
        "brier_score": float(brier_score_loss(test_targets, test_proba)),
        "log_loss": float(log_loss(test_targets, np.clip(test_proba, 1e-15, 1 - 1e-15))),
        "roc_auc": float(roc_auc_score(test_targets, test_proba)),
    }
    logger.info("v2 SetEncoder (calibrated): %s", {k: f"{v:.4f}" for k, v in v2_metrics.items()})
    logger.info(
        "Calibration improvement: Brier %.4f -> %.4f",
        v2_raw_metrics["brier_score_raw"],
        v2_metrics["brier_score"],
    )

    # MC dropout uncertainty calibration
    logger.info("=== MC Dropout Uncertainty Calibration ===")
    mc_metrics = evaluate_mc_dropout(model, test_loader, device, n_samples=MC_DROPOUT_SAMPLES, config=config)
    logger.info("MC dropout: %s", {k: f"{v:.4f}" for k, v in mc_metrics.items()})

    # v1 XGBoost baseline comparison
    logger.info("=== v1 XGBoost Baseline Comparison ===")
    x_test_df = pd.DataFrame(x_test, columns=list(x_tabular.columns))
    y_test_series = pd.Series(y_test)
    v1_metrics = _evaluate_v1_baseline(x_test_df, y_test_series, hf_token)
    if v1_metrics:
        logger.info("v1 XGBoost: %s", {k: f"{v:.4f}" for k, v in v1_metrics.items()})
        brier_improvement = v1_metrics["v1_brier_score"] - v2_metrics["brier_score"]
        logger.info("Brier improvement (v1 - v2): %.4f (negative = v2 worse)", brier_improvement)
    else:
        logger.info("v1 baseline not available for comparison")

    # ------------------------------------------------------------------
    # 8. Export weights to NumPy
    # ------------------------------------------------------------------
    logger.info("=== Exporting weights ===")
    numpy_weights = _export_weights_to_numpy(model)

    # Include calibration parameters in the model artifact
    numpy_weights["_isotonic_X"] = np.array(ir.X_thresholds_, dtype=np.float64)
    numpy_weights["_isotonic_y"] = np.array(ir.y_thresholds_, dtype=np.float64)
    numpy_weights["_mc_z_multiplier"] = np.array([mc_metrics["mc_z_multiplier"]], dtype=np.float64)
    numpy_weights["_mc_dropout_p_inference"] = np.array([mc_metrics["mc_dropout_p_inference"]], dtype=np.float64)

    weight_bytes = serialize_set_encoder_weights(numpy_weights)
    logger.info("Serialized weight size: %d bytes", len(weight_bytes))

    # Validate roundtrip: deserialize and verify shapes
    roundtrip_envelope = json.loads(weight_bytes.decode("utf-8"))
    if roundtrip_envelope["model_type"] != "set_encoder_xg_v2":
        msg = f"Unexpected model_type: {roundtrip_envelope['model_type']}"
        raise ValueError(msg)
    for key, meta in roundtrip_envelope["weights"].items():
        raw = base64.b64decode(meta["data"])
        arr = np.frombuffer(raw, dtype=np.float64).copy().reshape(meta["shape"])
        original = numpy_weights[key]
        if arr.shape != original.shape:
            msg = f"Shape mismatch for {key}: {arr.shape} vs {original.shape}"
            raise ValueError(msg)
        if not np.allclose(arr, original):
            msg = f"Value mismatch for {key}"
            raise ValueError(msg)
    logger.info("Weight roundtrip validation passed")

    # ------------------------------------------------------------------
    # 9. Log to MLflow
    # ------------------------------------------------------------------
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow

        logger.info("=== Logging to MLflow (%s) ===", tracking_uri)
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("/soccer_analytics/xg_model_v2")

        with mlflow.start_run(run_name="xg_v2_set_encoder_hf_jobs"):
            # Hyperparameters
            mlflow.log_params(
                {
                    "architecture": "deep_sets_set_encoder",
                    "batch_size": BATCH_SIZE,
                    "max_epochs": MAX_EPOCHS,
                    "learning_rate": LEARNING_RATE,
                    "weight_decay": WEIGHT_DECAY,
                    "early_stopping_patience": EARLY_STOPPING_PATIENCE,
                    "player_feature_dim": config.player_feature_dim,
                    "encoder_hidden": config.encoder_hidden,
                    "context_dim": config.context_dim,
                    "pred_hidden_1": config.pred_hidden_1,
                    "pred_hidden_2": config.pred_hidden_2,
                    "dropout_p": config.dropout_p,
                    "mc_dropout_samples": MC_DROPOUT_SAMPLES,
                    "n_train": len(train_idx),
                    "n_test": len(test_idx),
                    "tabular_dim": tabular_dim,
                    "n_parameters": sum(p.numel() for p in model.parameters()),
                    "training_env": "hf_jobs_a10g",
                    "device": str(device),
                }
            )

            # Dataset commit hashes (E5 reproducibility)
            mlflow.log_param("xg_shot_data_commit", shots_dataset_commit)
            if ff_dataset_commit:
                mlflow.log_param("xg_freeze_frame_data_commit", ff_dataset_commit)

            # v2 metrics
            for name, value in v2_metrics.items():
                mlflow.log_metric(f"v2_{name}", value)

            # MC dropout metrics
            for name, value in mc_metrics.items():
                mlflow.log_metric(name, value)

            # v1 baseline metrics (for comparison)
            if v1_metrics:
                for name, value in v1_metrics.items():
                    mlflow.log_metric(name, value)

            # Training history
            for epoch_idx in range(len(history["train_loss"])):
                mlflow.log_metric("train_loss", history["train_loss"][epoch_idx], step=epoch_idx)
                mlflow.log_metric("val_brier", history["val_brier"][epoch_idx], step=epoch_idx)
                mlflow.log_metric("val_auc", history["val_auc"][epoch_idx], step=epoch_idx)

            # Log the NumPy weight JSON as an artifact (scoring UDF downloads this)
            import tempfile

            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".json", prefix="model_weights", delete=False, dir="/tmp"
            ) as tmp:
                tmp.write(weight_bytes)
                tmp_path = tmp.name

            # Rename to match what _try_load_champion_xg_v2() expects
            final_path = os.path.join(os.path.dirname(tmp_path), "model_weights.json")
            os.replace(tmp_path, final_path)
            mlflow.log_artifact(final_path)

            # Pyfunc wrapper for UC model registry (requires signature)
            class _XgV2PyfuncWrapper(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
                """Thin wrapper to satisfy UC model registry signature requirement."""

                def predict(self, context: Any, model_input: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                    return np.zeros(len(model_input))  # placeholder — real inference uses set_encoder.py

            mlflow.pyfunc.log_model(
                python_model=_XgV2PyfuncWrapper(),
                artifact_path="xg_v2_model",
                registered_model_name="soccer_analytics.dev_gold.xg_model_v2",
                input_example=pd.DataFrame({"x": [0.0]}),
            )

            run_id = mlflow.active_run().info.run_id

        # Set @Champion alias
        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions("name='soccer_analytics.dev_gold.xg_model_v2'")
        if versions:
            latest = max(versions, key=lambda v: int(v.version))
            client.set_registered_model_alias(
                name="soccer_analytics.dev_gold.xg_model_v2",
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
    else:
        logger.info("MLflow skipped (MLFLOW_TRACKING_URI not set)")

    # ------------------------------------------------------------------
    # 10. Publish to HF Hub
    # ------------------------------------------------------------------
    logger.info("=== Publishing to HF Hub ===")

    # Build metrics payload
    metrics_payload: dict[str, Any] = {
        "v2_set_encoder": v2_metrics,
        "mc_dropout": mc_metrics,
        "config": {
            "architecture": "deep_sets_set_encoder",
            "player_feature_dim": config.player_feature_dim,
            "encoder_hidden": config.encoder_hidden,
            "context_dim": config.context_dim,
            "pred_hidden_1": config.pred_hidden_1,
            "pred_hidden_2": config.pred_hidden_2,
            "dropout_p": config.dropout_p,
            "batch_size": BATCH_SIZE,
            "max_epochs": MAX_EPOCHS,
            "actual_epochs": len(history["train_loss"]),
            "learning_rate": LEARNING_RATE,
            "weight_decay": WEIGHT_DECAY,
            "tabular_dim": tabular_dim,
            "feature_names": list(x_tabular.columns),
            "n_parameters": sum(p.numel() for p in model.parameters()),
            "n_train": len(train_idx),
            "n_test": len(test_idx),
        },
        "dataset_commits": {
            "xg_shot_data": shots_dataset_commit,
            "xg_freeze_frame_data": ff_dataset_commit,
        },
    }
    if v1_metrics:
        metrics_payload["v1_xgboost_baseline"] = v1_metrics
    metrics_payload = recorder.complete(metrics_payload, row_count=len(train_idx) + len(test_idx))

    api.create_repo(V2_MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)

    api.upload_file(
        path_or_fileobj=weight_bytes,
        path_in_repo="model_weights.json",
        repo_id=V2_MODEL_REPO,
        token=hf_token,
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics_payload, indent=2).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=V2_MODEL_REPO,
        token=hf_token,
    )

    elapsed_total = time.time() - pipeline_start
    logger.info("Published: https://huggingface.co/%s", V2_MODEL_REPO)
    logger.info("  model_weights.json: %d bytes", len(weight_bytes))
    logger.info("  metrics.json: %d bytes", len(json.dumps(metrics_payload, indent=2).encode()))
    logger.info("xG v2 training complete in %.1f seconds", elapsed_total)


if __name__ == "__main__":
    main()
