# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.12-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "databricks-sdk>=0.102.0",
# ]
# ///
"""Train xG model on HuggingFace Jobs (CPU).

Downloads shot data from HF Hub dataset, trains logistic + XGBoost models,
logs to MLflow via remote tracking URI, and pushes weights to HF Hub.

This is a standalone PEP 723 script that runs on HF Jobs. The project wheel
is installed for workflow card support; training logic is inlined.

Reference: Custom xG model — logistic baseline + calibrated XGBoost with
isotonic calibration.

Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_xg_model_hf.py \\
        --flavor cpu-basic --timeout 30m \\
        --secrets HF_TOKEN=$HF_TOKEN \\
        --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \\
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \\
        --env DATABRICKS_HOST=$DATABRICKS_HOST

    All four env vars are REQUIRED. The script fails fast (ADR-002) if
    MLFLOW_TRACKING_URI, DATABRICKS_HOST, or DATABRICKS_TOKEN is missing.

    Secrets vs env: ``HF_TOKEN`` and ``DATABRICKS_TOKEN`` MUST be passed
    via ``--secrets`` — ``--env`` stores the value as a plain job
    environment variable visible via ``hf jobs inspect <job_id>``.

Artifacts produced (all three mandatory on success):
  - HF Hub model repo ``luxury-lakehouse/xg-model-statsbomb-wyscout`` (weights + metrics)
  - MLflow UC Registry ``soccer_analytics.dev_gold.xg_model@Champion``
  - UC Volume ``/Volumes/soccer_analytics/dev_gold/model_weights/xg_model/``
    (``logistic_model.json`` + ``xgboost_model.json`` + ``.sha256`` sidecars each)
"""

from __future__ import annotations

import base64
import json
import os

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from ingestion.artifact_deploy import (
    require_mlflow_env,
    set_and_verify_mlflow_champion,
    upload_weights_to_uc_volume,
)
from ingestion.hf_jobs_cost import HF_RATE_CPU_BASIC, HFJobsCostRecorder
from shared.constants import mlflow_model_uri
from workflows import workflow

# ---------------------------------------------------------------------------
# Configuration (mirrors src/analytics/xg_model.py)
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
SHOTS_DATASET = f"{HF_ORG}/xg-shot-data"
MODEL_REPO = f"{HF_ORG}/xg-model-statsbomb-wyscout"

CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MODEL_NAME = "xg_model"

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
_BASELINE_FEATURES = ["distance_to_goal", "shot_angle"]

N_ESTIMATORS = 100
MAX_DEPTH = 3
LEARNING_RATE = 0.1
CALIBRATION_METHOD = "isotonic"
TEST_SIZE = 0.2
RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# Feature engineering (inlined from src/analytics/xg_model.py)
# ---------------------------------------------------------------------------


def build_features(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Build feature matrix and target from shot data."""
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
# Serialization (inlined from src/analytics/xg_model.py)
# ---------------------------------------------------------------------------


def serialize_xgboost_model(model: CalibratedClassifierCV) -> bytes:
    """Serialize calibrated XGBoost model to JSON bytes (no pickle)."""
    cc = next(iter(model.calibrated_classifiers_))
    xgb_estimator = cc.estimator
    booster_bytes = xgb_estimator.get_booster().save_raw("json")
    calibrator = cc.calibrators[0]

    envelope = {
        "model_type": "xgboost",
        "booster_b64": base64.b64encode(booster_bytes).decode("ascii"),
        "feature_names": list(xgb_estimator.get_booster().feature_names),
        "X_thresholds": calibrator.X_thresholds_.tolist(),
        "y_thresholds": calibrator.y_thresholds_.tolist(),
        "X_min": float(calibrator.X_min_),
        "X_max": float(calibrator.X_max_),
        "increasing": bool(calibrator.increasing_),
    }
    return json.dumps(envelope, indent=2).encode("utf-8")


def serialize_logistic_model(model: CalibratedClassifierCV) -> bytes:
    """Serialize calibrated logistic model to JSON bytes (no pickle)."""
    cc = next(iter(model.calibrated_classifiers_))
    lr = cc.estimator
    calibrator = cc.calibrators[0]

    envelope = {
        "model_type": "logistic",
        "coef": lr.coef_.tolist(),
        "intercept": lr.intercept_.tolist(),
        "feature_names": list(lr.feature_names_in_),
        "classes": lr.classes_.tolist(),
        "X_thresholds": calibrator.X_thresholds_.tolist(),
        "y_thresholds": calibrator.y_thresholds_.tolist(),
        "X_min": float(calibrator.X_min_),
        "X_max": float(calibrator.X_max_),
        "increasing": bool(calibrator.increasing_),
    }
    return json.dumps(envelope, indent=2).encode("utf-8")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


@workflow("wf-xg-v1", phase="training")
def main() -> None:
    """Download shots, train xG models, log to MLflow, push to HF Hub."""
    from huggingface_hub import HfApi, get_token, hf_hub_download

    hf_token = os.environ.get("HF_TOKEN", "") or (get_token() or "")
    if not hf_token:
        raise RuntimeError("HF_TOKEN environment variable required")

    api = HfApi(token=hf_token)

    recorder = HFJobsCostRecorder(
        workflow_id="wf-xg-v1",
        phase="training",
        rate_usd_per_hour=HF_RATE_CPU_BASIC,
        repo_id=MODEL_REPO,
        repo_type="model",
    )
    recorder.start()

    # Pre-flight: fail loud if MLflow registration env vars are missing
    # (ADR-002: no silent-skip of the registry step).
    require_mlflow_env()

    # ------------------------------------------------------------------
    # 1. Load shot data from HF Hub
    # ------------------------------------------------------------------
    print("=== Loading shot data from HF Hub ===")
    all_items = list(api.list_repo_tree(SHOTS_DATASET, repo_type="dataset", recursive=True))
    parquet_files = [f.path for f in all_items if hasattr(f, "size") and f.path.endswith(".parquet")]

    if not parquet_files:
        raise RuntimeError(f"No parquet files found in {SHOTS_DATASET}")

    dfs: list[pd.DataFrame] = []
    for pf in parquet_files:
        local = hf_hub_download(SHOTS_DATASET, pf, repo_type="dataset", token=hf_token)
        df = pd.read_parquet(local)
        dfs.append(df)
        print(f"  {pf}: {len(df):,} rows")

    shots = pd.concat(dfs, ignore_index=True)
    shots = shots.dropna(subset=["is_goal"]).reset_index(drop=True)
    print(f"Total shots: {len(shots):,}")

    # Capture dataset commit hash for reproducibility (E5)
    _dataset_info = api.repo_info(repo_id=SHOTS_DATASET, repo_type="dataset")
    _dataset_commit = _dataset_info.sha

    # ------------------------------------------------------------------
    # 2. Build features and split
    # ------------------------------------------------------------------
    print("\n=== Building features ===")
    x_features, y = build_features(shots)
    x_train, x_test, y_train, y_test = train_test_split(
        x_features,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )
    print(f"Train: {len(x_train):,}, Test: {len(x_test):,}")

    # ------------------------------------------------------------------
    # 3. Train models
    # ------------------------------------------------------------------
    print("\n=== Training models ===")

    # Logistic baseline (cross-validated calibration — sklearn 1.6+ removed cv="prefit")
    lr = LogisticRegression(random_state=RANDOM_STATE, max_iter=1000)
    baseline_cols = [c for c in _BASELINE_FEATURES if c in x_train.columns]
    logistic_model = CalibratedClassifierCV(lr, cv=5, method=CALIBRATION_METHOD, ensemble=False)
    logistic_model.fit(x_train[baseline_cols], y_train)
    print("  Logistic baseline trained")

    # XGBoost (cross-validated calibration, single estimator for serialization)
    xgb = XGBClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        learning_rate=LEARNING_RATE,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=RANDOM_STATE,
    )
    xgboost_model = CalibratedClassifierCV(xgb, cv=5, method=CALIBRATION_METHOD, ensemble=False)
    xgboost_model.fit(x_train, y_train)
    print("  XGBoost (calibrated) trained")

    # ------------------------------------------------------------------
    # 4. Evaluate
    # ------------------------------------------------------------------
    print("\n=== Evaluating ===")
    xgb_proba = xgboost_model.predict_proba(x_test)[:, 1]
    lr_proba = logistic_model.predict_proba(x_test[baseline_cols])[:, 1]

    xgb_metrics = {
        "brier_score": brier_score_loss(y_test, xgb_proba),
        "log_loss": log_loss(y_test, xgb_proba),
        "roc_auc": roc_auc_score(y_test, xgb_proba),
    }
    lr_metrics = {
        "brier_score": brier_score_loss(y_test, lr_proba),
        "log_loss": log_loss(y_test, lr_proba),
        "roc_auc": roc_auc_score(y_test, lr_proba),
    }

    print("  XGBoost:", {k: f"{v:.4f}" for k, v in xgb_metrics.items()})
    print("  Logistic:", {k: f"{v:.4f}" for k, v in lr_metrics.items()})

    # ------------------------------------------------------------------
    # 5. Log to MLflow (always runs — require_mlflow_env() enforced on entry)
    # ------------------------------------------------------------------
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow

    print(f"\n=== Logging to MLflow ({tracking_uri}) ===")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("/soccer_analytics/xg_model")

    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    with mlflow.start_run(run_name="xg_model_hf_jobs") as active_run:
        run_id = active_run.info.run_id
        mlflow.log_params(
            {
                "n_estimators": N_ESTIMATORS,
                "max_depth": MAX_DEPTH,
                "learning_rate": LEARNING_RATE,
                "calibration_method": CALIBRATION_METHOD,
                "n_train": len(x_train),
                "n_test": len(x_test),
                "n_features": len(x_train.columns),
                "training_env": "hf_jobs_cpu",
            }
        )
        mlflow.log_param("xg_shot_data_commit", _dataset_commit)
        for name, value in xgb_metrics.items():
            mlflow.log_metric(f"xgboost_{name}", value)
        for name, value in lr_metrics.items():
            mlflow.log_metric(f"logistic_{name}", value)

        mlflow.sklearn.log_model(
            sk_model=xgboost_model,
            artifact_path="xgboost_model",
            registered_model_name=mlflow_fqn,
        )
        mlflow.sklearn.log_model(
            sk_model=logistic_model,
            artifact_path="logistic_model",
        )

    # Set + verify @Champion alias (zombie-alias guard, ADR-002 alignment)
    client = mlflow.tracking.MlflowClient()
    set_and_verify_mlflow_champion(client, mlflow_fqn=mlflow_fqn, run_id=run_id)
    print("  MLflow logging complete")

    # ------------------------------------------------------------------
    # 6. Serialize and publish to HF Hub
    # ------------------------------------------------------------------
    print("\n=== Publishing to HF Hub ===")
    logistic_bytes = serialize_logistic_model(logistic_model)
    xgboost_bytes = serialize_xgboost_model(xgboost_model)

    metrics_payload: dict[str, object] = {
        "logistic": lr_metrics,
        "xgboost": xgb_metrics,
        "config": {
            "n_estimators": N_ESTIMATORS,
            "max_depth": MAX_DEPTH,
            "learning_rate": LEARNING_RATE,
            "calibration_method": CALIBRATION_METHOD,
            "n_features": len(x_train.columns),
            "feature_names": list(x_train.columns),
            "n_train": len(x_train),
            "n_test": len(x_test),
        },
    }
    metrics_payload = recorder.complete(metrics_payload, row_count=len(x_train) + len(x_test))

    api.create_repo(MODEL_REPO, exist_ok=True, repo_type="model", token=hf_token)

    api.upload_file(
        path_or_fileobj=logistic_bytes,
        path_in_repo="logistic_model.json",
        repo_id=MODEL_REPO,
        token=hf_token,
    )
    api.upload_file(
        path_or_fileobj=xgboost_bytes,
        path_in_repo="xgboost_model.json",
        repo_id=MODEL_REPO,
        token=hf_token,
    )
    api.upload_file(
        path_or_fileobj=json.dumps(metrics_payload, indent=2).encode(),
        path_in_repo="metrics.json",
        repo_id=MODEL_REPO,
        token=hf_token,
    )

    print(f"\n  Published: https://huggingface.co/{MODEL_REPO}")

    # Upload to UC Volume (second leg of the delivery chain)
    from databricks.sdk import WorkspaceClient

    workspace_client = WorkspaceClient()
    logistic_volume = upload_weights_to_uc_volume(
        workspace_client,
        catalog=CATALOG,
        schema=SCHEMA,
        model_name=MODEL_NAME,
        filename="logistic_model.json",
        weights_bytes=logistic_bytes,
    )
    xgboost_volume = upload_weights_to_uc_volume(
        workspace_client,
        catalog=CATALOG,
        schema=SCHEMA,
        model_name=MODEL_NAME,
        filename="xgboost_model.json",
        weights_bytes=xgboost_bytes,
    )
    print(f"  UC Volume logistic: {logistic_volume['path']}")
    print(f"  UC Volume xgboost:  {xgboost_volume['path']}")
    print(f"  Logistic: {len(logistic_bytes):,} bytes")
    print(f"  XGBoost:  {len(xgboost_bytes):,} bytes")
    print("xG model training complete!")


if __name__ == "__main__":
    main()
