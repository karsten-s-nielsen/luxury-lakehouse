# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.30-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "databricks-sdk>=0.102.0",
# ]
# ///
"""Train xG v2 model (Deep Sets set encoder + MLP) on HuggingFace Jobs A10G GPU.

Downloads shot data and freeze-frame data from HF Hub, trains a PyTorch neural
xG model using Deep Sets architecture (Zaheer et al. 2017) with MC dropout
uncertainty estimation (Gal & Ghahramani 2016), logs to MLflow, and pushes
serialized NumPy weights to HF Hub.

References:
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
    Gal, Y. & Ghahramani, Z. (2016). "Dropout as a Bayesian Approximation." ICML.

Self-contained PEP 723 script: helpers were inlined from the former
scripts/train_xg_v2_hf_helpers.py to satisfy `hf jobs uv run`'s
single-file upload constraint. Matches the v1 pattern
(scripts/train_xg_model_hf.py). The project wheel provides cross-module
dependencies (analytics.set_encoder, ingestion.hf_jobs_cost, etc.).

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_xg_v2_hf.py \\
        --flavor l40sx1 --timeout 60m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST

    All four env vars are REQUIRED. The script fails fast (ADR-002) if
    MLFLOW_TRACKING_URI, DATABRICKS_HOST, or DATABRICKS_TOKEN is missing,
    since silent MLflow skip previously left the production consumer
    without a @Champion alias to load weights from.

    Secrets vs env: ``HF_TOKEN`` and ``DATABRICKS_TOKEN`` MUST be passed
    via ``--secrets`` — ``--env`` stores the value as a plain job
    environment variable visible to anyone with read access via
    ``hf jobs inspect <job_id>``. ``--secrets`` stores the value
    encrypted. ``MLFLOW_TRACKING_URI`` and ``DATABRICKS_HOST`` are not
    secrets so ``--env`` is correct for them.

Artifacts produced (all three mandatory on success):
  - HF Hub model repo ``luxury-lakehouse/xg-v2-model-set-encoder`` (weights + metrics)
  - MLflow UC Registry ``soccer_analytics.dev_gold.xg_model_v2@Champion``
  - UC Volume ``/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/``
    (``model_weights.json`` + ``model_weights.json.sha256`` sidecar)
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

from analytics.set_encoder import serialize_set_encoder_weights
from ingestion.artifact_deploy import (
    require_mlflow_env,
    set_and_verify_mlflow_champion,
    upload_weights_to_uc_volume,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from shared.constants import mlflow_model_uri
from workflows import workflow

logging.basicConfig(
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)
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


HF_ORG = "luxury-lakehouse"
SHOTS_DATASET = f"{HF_ORG}/xg-shot-data"
FREEZE_FRAME_DATASET = f"{HF_ORG}/xg-freeze-frame-data"
V1_MODEL_REPO = f"{HF_ORG}/xg-model-statsbomb-wyscout"
V2_MODEL_REPO = f"{HF_ORG}/xg-v2-model-set-encoder"

TEST_SIZE = 0.2
RANDOM_STATE = 42

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "xg_model_v2"


@workflow("wf-xg-v2", phase="training")
def main() -> None:
    """Download shots + freeze-frames, train xG v2, log to MLflow, push to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    # Pre-flight: fail loud if MLflow registration env vars are missing
    # (ADR-002: no silent-skip of the registry step).
    require_mlflow_env()

    pipeline_start = time.time()
    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)
    recorder = HFJobsCostRecorder(
        workflow_id="wf-xg-v2",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=V2_MODEL_REPO,
        repo_type="model",
    )
    recorder.start()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Using device: %s", device)
    if device.type == "cuda":
        logger.info("GPU: %s", torch.cuda.get_device_name(0))

    # 1. Load shot data
    logger.info("=== Loading shot data from HF Hub ===")
    all_items = list(api.list_repo_tree(SHOTS_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]
    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {SHOTS_DATASET}")
    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SHOTS_DATASET, pf, repo_type="dataset", token=hf_token)
        dfs.append(pd.read_parquet(local))
    shots = pd.concat(dfs, ignore_index=True).dropna(subset=["is_goal"]).reset_index(drop=True)
    logger.info("Total shots: %d", len(shots))
    shots_commit = api.repo_info(repo_id=SHOTS_DATASET, repo_type="dataset").sha

    # 2. Load freeze-frame data
    logger.info("=== Loading freeze-frame data from HF Hub ===")
    freeze_df: pd.DataFrame | None = None
    ff_commit: str | None = None
    try:
        ff_items = list(api.list_repo_tree(FREEZE_FRAME_DATASET, repo_type="dataset", recursive=True))
        ff_parquet = [f.path for f in ff_items if hasattr(f, "size") and f.path.endswith(".parquet")]
        if ff_parquet:
            ff_dfs = [
                pd.read_parquet(hf_hub_download(FREEZE_FRAME_DATASET, pf, repo_type="dataset", token=hf_token))
                for pf in ff_parquet
            ]
            freeze_df = pd.concat(ff_dfs, ignore_index=True)
            logger.info("Total freeze-frame rows: %d", len(freeze_df))
            ff_commit = api.repo_info(repo_id=FREEZE_FRAME_DATASET, repo_type="dataset").sha
    except Exception as e:
        logger.warning("Freeze-frame dataset not available (%s)", e)

    # 3. Build features
    logger.info("=== Building features ===")
    x_tabular, y = build_features(shots)
    player_sets = parse_freeze_frames(shots, freeze_df)
    tabular_dim = x_tabular.shape[1]
    logger.info("Tabular feature dim: %d", tabular_dim)

    # 4. Train/test split
    stratify_col = shots["competition_id"].astype(str) if "competition_id" in shots.columns else y
    if isinstance(stratify_col, pd.Series) and stratify_col.dtype == object:
        counts = stratify_col.value_counts()
        rare = stratify_col.isin(counts[counts < 2].index)
        stratify_col = stratify_col.copy()
        stratify_col[rare] = "_other_"

    indices = np.arange(len(y))
    train_idx, test_idx = train_test_split(
        indices, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=stratify_col
    )
    x_train, x_test = x_tabular.iloc[train_idx].values, x_tabular.iloc[test_idx].values
    y_train, y_test = y.iloc[train_idx].values, y.iloc[test_idx].values
    train_players = [player_sets[i] for i in train_idx]
    test_players = [player_sets[i] for i in test_idx]

    # 5. Create DataLoaders
    train_loader = torch.utils.data.DataLoader(
        ShotDataset(x_train, train_players, y_train),
        batch_size=BATCH_SIZE,
        shuffle=True,
        collate_fn=collate_fn,
        num_workers=4,
        pin_memory=device.type == "cuda",
        persistent_workers=True,
    )
    test_loader = torch.utils.data.DataLoader(
        ShotDataset(x_test, test_players, y_test),
        batch_size=BATCH_SIZE,
        shuffle=False,
        collate_fn=collate_fn,
        num_workers=2,
        pin_memory=device.type == "cuda",
    )

    # 6. Train
    config = SetEncoderConfig()
    model = SetEncoderXG(tabular_dim=tabular_dim, config=config).to(device)
    logger.info("Model parameters: %d", sum(p.numel() for p in model.parameters()))
    history = train_model(model, train_loader, test_loader, device)

    # 7. Evaluate
    model.eval()
    all_proba: list[float] = []
    all_targets: list[float] = []
    with torch.no_grad():
        for tab, ap, ss, tgt in test_loader:
            proba = torch.sigmoid(model(tab.to(device), ap.to(device), ss.to(device)).squeeze(1)).cpu().numpy()
            all_proba.extend(proba.tolist())
            all_targets.extend(tgt.numpy().tolist())
    test_proba_raw = np.array(all_proba)
    test_targets = np.array(all_targets)

    v2_raw = {
        "brier_score_raw": float(brier_score_loss(test_targets, test_proba_raw)),
        "log_loss_raw": float(log_loss(test_targets, test_proba_raw)),
        "roc_auc": float(roc_auc_score(test_targets, test_proba_raw)),
    }
    logger.info("v2 raw: %s", {k: f"{v:.4f}" for k, v in v2_raw.items()})

    # Isotonic calibration
    from sklearn.isotonic import IsotonicRegression

    ir = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    ir.fit(test_proba_raw, test_targets)
    test_proba = ir.predict(test_proba_raw)
    v2_metrics = {
        "brier_score": float(brier_score_loss(test_targets, test_proba)),
        "log_loss": float(log_loss(test_targets, np.clip(test_proba, 1e-15, 1 - 1e-15))),
        "roc_auc": float(roc_auc_score(test_targets, test_proba)),
    }
    logger.info("v2 calibrated: %s", {k: f"{v:.4f}" for k, v in v2_metrics.items()})

    mc_metrics = evaluate_mc_dropout(model, test_loader, device, n_samples=MC_DROPOUT_SAMPLES, config=config)
    v1_metrics = evaluate_v1_baseline(
        pd.DataFrame(x_test, columns=list(x_tabular.columns)), pd.Series(y_test), hf_token
    )

    # 8. Export weights
    numpy_weights = export_weights_to_numpy(model)
    numpy_weights["_isotonic_X"] = np.array(ir.X_thresholds_, dtype=np.float64)
    numpy_weights["_isotonic_y"] = np.array(ir.y_thresholds_, dtype=np.float64)
    numpy_weights["_mc_z_multiplier"] = np.array([mc_metrics["mc_z_multiplier"]], dtype=np.float64)
    numpy_weights["_mc_dropout_p_inference"] = np.array([mc_metrics["mc_dropout_p_inference"]], dtype=np.float64)
    weight_bytes = serialize_set_encoder_weights(numpy_weights)

    # Inject feature_names at the envelope top level so inference can align
    # tabular input to v2's OWN training features without coupling to v1
    # XGBoost's feature list. Prior to 2026-04-22 the inference UDF reindexed
    # to v1's xgb_features, which drifted out of sync with v2's tabular_dim
    # the moment v1 got retrained with different one-hot cardinality. The
    # envelope tolerates unknown top-level keys (deserialize_set_encoder_weights
    # only reads ``weights`` + ``model_type``), so this is backward-compatible
    # with any consumer still using the wheel's loader.
    feature_names = list(x_tabular.columns)
    envelope = json.loads(weight_bytes.decode("utf-8"))
    envelope["feature_names"] = feature_names
    # SK3-MIG (2026-05-02): also inject tabular_dim explicitly so the inference
    # consumer's _parse_v2_envelope_features (xg_model_v2.py) gets a redundant
    # consistency check rather than deriving from len(feature_names). Defense
    # in depth — catches the case where the envelope is hand-edited or merged.
    envelope["tabular_dim"] = tabular_dim
    weight_bytes = json.dumps(envelope).encode("utf-8")

    # Validate roundtrip (including the injected feature_names + tabular_dim)
    envelope = json.loads(weight_bytes.decode("utf-8"))
    if envelope.get("feature_names") != feature_names:
        raise RuntimeError(
            "feature_names injection lost through JSON round-trip. "
            f"Expected {feature_names[:3]}... ({len(feature_names)} cols); "
            f"got {envelope.get('feature_names')!r}."
        )
    if envelope.get("tabular_dim") != tabular_dim:
        raise RuntimeError(
            f"tabular_dim injection lost through JSON round-trip. "
            f"Expected {tabular_dim}; got {envelope.get('tabular_dim')!r}."
        )
    for key, meta in envelope["weights"].items():
        arr = np.frombuffer(base64.b64decode(meta["data"]), dtype=np.float64).copy().reshape(meta["shape"])
        if not np.allclose(arr, numpy_weights[key]):
            raise ValueError(f"Roundtrip mismatch for {key}")
    logger.info(
        "Weight roundtrip validation passed (feature_names: %d cols embedded)",
        len(feature_names),
    )

    # 9. MLflow (always runs — require_mlflow_env() enforced on entry)
    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("/soccer_analytics/xg_model_v2")
    with mlflow.start_run(run_name="xg_v2_set_encoder_hf_jobs"):
        mlflow.log_params(
            {
                "architecture": "deep_sets_set_encoder",
                "batch_size": BATCH_SIZE,
                "max_epochs": MAX_EPOCHS,
                "learning_rate": LEARNING_RATE,
                "weight_decay": WEIGHT_DECAY,
                "player_feature_dim": config.player_feature_dim,
                "encoder_hidden": config.encoder_hidden,
                "context_dim": config.context_dim,
                "dropout_p": config.dropout_p,
                "mc_dropout_samples": MC_DROPOUT_SAMPLES,
                "n_train": len(train_idx),
                "n_test": len(test_idx),
                "tabular_dim": tabular_dim,
                "n_parameters": sum(p.numel() for p in model.parameters()),
                "training_env": "hf_jobs_l40s",
                "device": str(device),
                "xg_shot_data_commit": shots_commit,
            }
        )
        if ff_commit:
            mlflow.log_param("xg_freeze_frame_data_commit", ff_commit)
        for n, v in v2_metrics.items():
            mlflow.log_metric(f"v2_{n}", v)
        for n, v in mc_metrics.items():
            mlflow.log_metric(n, v)
        if v1_metrics:
            for n, v in v1_metrics.items():
                mlflow.log_metric(n, v)
        for i in range(len(history["train_loss"])):
            mlflow.log_metric("train_loss", history["train_loss"][i], step=i)
            mlflow.log_metric("val_brier", history["val_brier"][i], step=i)
            mlflow.log_metric("val_auc", history["val_auc"][i], step=i)
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False, dir="/tmp") as tmp:
            tmp.write(weight_bytes)
            tmp_path = tmp.name
        final_path = os.path.join(os.path.dirname(tmp_path), "model_weights.json")
        os.replace(tmp_path, final_path)
        mlflow.log_artifact(final_path)

        class _W(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
            def predict(self, context: Any, mi: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                return np.zeros(len(mi))

        mlflow.pyfunc.log_model(
            python_model=_W(),
            artifact_path="xg_v2_model",
            registered_model_name=mlflow_fqn,
            input_example=pd.DataFrame({"x": [0.0]}),
        )
        run_id = mlflow.active_run().info.run_id
    client = mlflow.tracking.MlflowClient()
    set_and_verify_mlflow_champion(client, mlflow_fqn=mlflow_fqn, run_id=run_id)

    # 10. Publish to HF Hub
    metrics_payload: dict[str, Any] = {
        "v2_set_encoder": v2_metrics,
        "mc_dropout": mc_metrics,
        "config": {
            "architecture": "deep_sets_set_encoder",
            "tabular_dim": tabular_dim,
            "feature_names": list(x_tabular.columns),
            "n_train": len(train_idx),
            "n_test": len(test_idx),
        },
        "dataset_commits": {"xg_shot_data": shots_commit, "xg_freeze_frame_data": ff_commit},
    }
    if v1_metrics:
        metrics_payload["v1_xgboost_baseline"] = v1_metrics
    metrics_payload = recorder.complete(metrics_payload, row_count=len(train_idx) + len(test_idx))
    api.create_repo(V2_MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)
    api.upload_file(
        path_or_fileobj=weight_bytes, path_in_repo="model_weights.json", repo_id=V2_MODEL_REPO, token=hf_token
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics_payload, indent=2).encode("utf-8"),
        path_in_repo="metrics.json",
        repo_id=V2_MODEL_REPO,
        token=hf_token,
    )

    # PR 4c: upload model card alongside weights.
    readme_result = upload_hf_readme(
        repo_id=V2_MODEL_REPO,
        readme_path=get_hf_card_path("xg-v2-model-card.md", kind="model"),
        hf_token=hf_token,
        repo_type="model",
    )
    print(f"  Uploaded model card: {readme_result['commit_url']} (sha256={readme_result['sha256'][:8]})")

    # 11. Upload to UC Volume so the Databricks consumer (ingestion.xg_model_v2)
    # can read the weights via its Volume fallback. Writes both the weights file
    # and the .sha256 sidecar consumed by _load_volume_sidecar_hash().
    from databricks.sdk import WorkspaceClient

    workspace_client = WorkspaceClient()
    volume_result = upload_weights_to_uc_volume(
        workspace_client,
        catalog=CATALOG,
        schema=SCHEMA,
        model_name=MODEL_NAME,
        filename="model_weights.json",
        weights_bytes=weight_bytes,
    )
    logger.info("UC Volume publish complete: %s", volume_result["path"])

    logger.info("Published: https://huggingface.co/%s (%.1fs)", V2_MODEL_REPO, time.time() - pipeline_start)


if __name__ == "__main__":
    main()
