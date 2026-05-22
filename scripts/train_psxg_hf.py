# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.90-py3-none-any.whl",
#     "numpy>=1.26.0",
#     "pandas>=2.0.0",
#     "pyarrow>=14.0.0",
#     "scikit-learn>=1.3.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
# ]
# ///
"""Train Post-Shot Expected Goals (PSxG) model on HF Jobs.

Following Butcher et al. (2025), "An Expected Goals On Target (xGOT) Model".

The PSxG logistic regression model predicts goal probability from normalised
goalmouth coordinates (end_location_y, end_location_z) for on-target shots.
Model weights are serialised to JSON (zero pickle surface) and published to
HF Hub alongside per-shot predictions.

Usage (HF Jobs):  hf jobs run train_psxg_hf.py --flavor l40sx1
Usage (local):    uv run scripts/train_psxg_hf.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from huggingface_hub import HfApi
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.model_selection import train_test_split

from analytics.goalkeeper import PSxGModel, predict_psxg, train_psxg_model
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
INPUT_DATASET = f"{HF_ORG}/statsbomb-shots-on-target"
OUTPUT_MODEL = f"{HF_ORG}/psxg-model"
OUTPUT_PREDICTIONS = f"{HF_ORG}/psxg-predictions"

TEST_SIZE = 0.2
RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_shots() -> pd.DataFrame:
    """Download on-target shots dataset from HF Hub.

    Expects the dataset to contain ``end_location_y``, ``end_location_z``,
    ``is_goal``, ``event_id``, ``match_id``, and ``player_id`` columns.

    The dataset may contain Spark-written Parquet part files (``part-*.parquet``)
    or a single ``shots_on_target.parquet``. Both are handled transparently.

    Returns:
        DataFrame of on-target shots ready for training.
    """
    from huggingface_hub import snapshot_download

    local_dir = snapshot_download(
        repo_id=INPUT_DATASET,
        repo_type="dataset",
        allow_patterns="data/*.parquet",
    )
    parquet_files = list(Path(local_dir).glob("data/*.parquet"))
    if not parquet_files:
        msg = f"No Parquet files found in {INPUT_DATASET}/data/"
        raise RuntimeError(msg)

    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    logger.info("Loaded %d on-target shots from %s (%d files)", len(df), INPUT_DATASET, len(parquet_files))
    return df


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate_model(model: PSxGModel, test_df: pd.DataFrame) -> dict[str, float | int]:
    """Evaluate PSxG model on the held-out test set.

    Metrics reported:
      - ``log_loss``: Cross-entropy loss (lower = better).
      - ``brier_score``: Mean squared probability error (lower = better).
      - ``n_test``: Number of test shots.
      - ``goal_rate``: Observed goal rate in the test set.
      - ``mean_psxg``: Mean predicted PSxG (calibration check: should be close to goal_rate).

    Args:
        model: Fitted ``PSxGModel`` from ``train_psxg_model``.
        test_df: Held-out test shots with ``is_goal`` and goalmouth coordinate columns.

    Returns:
        Dict of evaluation metric name → value.
    """
    result = predict_psxg(model, test_df)
    on_target_mask = result["psxg"].notna()
    result = result.loc[on_target_mask]

    y_true = result["is_goal"].to_numpy(dtype=np.int64)
    y_pred = result["psxg"].to_numpy(dtype=np.float64)

    # Clip predictions to avoid log(0) in log_loss.
    y_pred_clipped = np.clip(y_pred, 1e-7, 1 - 1e-7)

    # Compute calibration (reliability: predicted vs. observed) at 5 bins.
    fraction_of_positives, mean_predicted = calibration_curve(y_true, y_pred_clipped, n_bins=5)
    max_calibration_error = float(np.abs(fraction_of_positives - mean_predicted).max())

    metrics: dict[str, float | int] = {
        "log_loss": float(log_loss(y_true, y_pred_clipped)),
        "brier_score": float(brier_score_loss(y_true, y_pred_clipped)),
        "n_test": len(test_df),
        "goal_rate": float(y_true.mean()),
        "mean_psxg": float(y_pred_clipped.mean()),
        "max_calibration_error": max_calibration_error,
    }
    logger.info("Evaluation metrics: %s", json.dumps(metrics, indent=2))
    return metrics


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


def publish_model(model: PSxGModel, metrics: dict[str, float | int]) -> None:
    """Serialise and publish PSxG model weights to HF Hub.

    The model is written as a JSON file (zero pickle surface). Numpy arrays
    are serialised via ``.tolist()``.

    Payload schema::

        {
          "coefficients": [...],   // list[float], shape (n_features,)
          "intercept": float,
          "scaler_mean": [...],    // list[float], shape (n_features,)
          "scaler_scale": [...],   // list[float], shape (n_features,)
          "metrics": { ... }
        }

    Args:
        model: Fitted ``PSxGModel`` dataclass.
        metrics: Evaluation metrics dict from ``evaluate_model``.
    """
    api = HfApi()
    api.create_repo(OUTPUT_MODEL, exist_ok=True, repo_type="model")

    model_data: dict[str, object] = {
        "coefficients": model.coefficients.tolist(),
        "intercept": model.intercept,
        "scaler_mean": model.scaler_mean.tolist(),
        "scaler_scale": model.scaler_scale.tolist(),
        "metrics": metrics,
    }
    payload = json.dumps(model_data, indent=2).encode("utf-8")

    api.upload_file(
        path_or_fileobj=payload,
        path_in_repo="psxg_model.json",
        repo_id=OUTPUT_MODEL,
        repo_type="model",
    )
    logger.info("Published model weights to https://huggingface.co/%s", OUTPUT_MODEL)

    # PR 4c: upload model card alongside weights.
    hf_token = os.environ.get("HF_TOKEN") or ""
    if hf_token:
        readme_result = upload_hf_readme(
            repo_id=OUTPUT_MODEL,
            readme_path=get_hf_card_path("psxg-model.md", kind="model"),
            hf_token=hf_token,
            repo_type="model",
        )
        logger.info(
            "Uploaded model card: %s (sha256=%s)",
            readme_result["commit_url"],
            readme_result["sha256"][:8],
        )


def publish_predictions(shots_df: pd.DataFrame, model: PSxGModel) -> None:
    """Generate PSxG for all shots and publish predictions to HF Hub.

    Writes a Parquet file with columns ``event_id``, ``match_id``,
    ``player_id``, ``psxg`` to the ``OUTPUT_PREDICTIONS`` dataset repo.
    Off-target shots receive ``psxg = NaN``.

    Args:
        shots_df: Full shots DataFrame (train + test) with goalmouth coordinates.
        model: Fitted ``PSxGModel`` to use for inference.
    """
    api = HfApi()
    api.create_repo(OUTPUT_PREDICTIONS, exist_ok=True, repo_type="dataset")

    result = predict_psxg(model, shots_df)
    output_cols = ["event_id", "match_id", "player_id", "psxg"]
    predictions = result[output_cols]

    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "psxg_predictions.parquet"
        predictions.to_parquet(parquet_path, index=False)
        api.upload_file(
            path_or_fileobj=str(parquet_path),
            path_in_repo="data/psxg_predictions.parquet",
            repo_id=OUTPUT_PREDICTIONS,
            repo_type="dataset",
        )
    logger.info("Published %d predictions to https://huggingface.co/datasets/%s", len(predictions), OUTPUT_PREDICTIONS)

    # PR 4c: upload dataset card alongside predictions.
    hf_token = os.environ.get("HF_TOKEN") or ""
    if hf_token:
        readme_result = upload_hf_readme(
            repo_id=OUTPUT_PREDICTIONS,
            readme_path=get_hf_card_path("psxg-predictions.md", kind="dataset"),
            hf_token=hf_token,
        )
        logger.info(
            "Uploaded dataset card: %s (sha256=%s)",
            readme_result["commit_url"],
            readme_result["sha256"][:8],
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Train, evaluate, and publish the PSxG model.

    Pipeline:
      1. Load on-target shots from HF Hub.
      2. Stratified train/test split (80/20).
      3. Train logistic regression PSxG model on train set.
      4. Evaluate on held-out test set (log_loss, brier_score, calibration).
      5. Log params and metrics to MLflow experiment ``psxg-training``.
      6. Publish model JSON and full-dataset predictions to HF Hub.
    """
    recorder = HFJobsCostRecorder(
        workflow_id="wf-psxg",
        phase="training",
        rate_usd_per_hour=HF_RATE_A10G_LARGE,
        repo_id=OUTPUT_MODEL,
        repo_type="model",
    )
    recorder.start()

    try:
        # ------------------------------------------------------------------
        # 1. Load data
        # ------------------------------------------------------------------
        logger.info("=== Loading on-target shots from HF Hub ===")
        shots_df = load_shots()

        if shots_df.empty:
            logger.error("Dataset is empty — aborting.")
            sys.exit(1)

        # ------------------------------------------------------------------
        # 2. Train / test split
        # ------------------------------------------------------------------
        logger.info("=== Splitting data (test_size=%.1f, stratified) ===", TEST_SIZE)
        train_df, test_df = train_test_split(
            shots_df,
            test_size=TEST_SIZE,
            random_state=RANDOM_STATE,
            stratify=shots_df["is_goal"],
        )
        logger.info("Train: %d shots, Test: %d shots", len(train_df), len(test_df))

        # ------------------------------------------------------------------
        # 3. Train model
        # ------------------------------------------------------------------
        logger.info("=== Training PSxG logistic regression ===")
        mlflow.set_experiment("psxg-training")

        with mlflow.start_run(run_name="psxg-logistic"):
            model = train_psxg_model(train_df)
            logger.info(
                "Trained — coefficients: %s, intercept: %.4f",
                model.coefficients.tolist(),
                model.intercept,
            )

            # ------------------------------------------------------------------
            # 4. Evaluate
            # ------------------------------------------------------------------
            logger.info("=== Evaluating on held-out test set ===")
            metrics = evaluate_model(model, test_df)

            # ------------------------------------------------------------------
            # 5. Log to MLflow
            # ------------------------------------------------------------------
            mlflow.log_params(
                {
                    "model_type": "logistic_regression",
                    "features": "end_location_y,end_location_z",
                    "n_train": len(train_df),
                    "n_test": len(test_df),
                    "test_size": TEST_SIZE,
                    "random_state": RANDOM_STATE,
                    "reference": "Butcher et al. (2025) An Expected Goals On Target (xGOT) Model",
                }
            )
            # Log only float-valued metrics to MLflow (n_test is int → cast).
            for key, value in metrics.items():
                mlflow.log_metric(key, float(value))

            # ------------------------------------------------------------------
            # 6. Publish
            # ------------------------------------------------------------------
            logger.info("=== Publishing model and predictions to HF Hub ===")
            publish_model(model, metrics)
            publish_predictions(shots_df, model)

        metrics_payload: dict[str, object] = {
            "psxg": metrics,
            "config": {
                "model_type": "logistic_regression",
                "features": ["end_location_y", "end_location_z"],
                "n_train": len(train_df),
                "n_test": len(test_df),
            },
        }
        recorder.complete(metrics_payload, row_count=len(shots_df))

    except Exception:
        recorder.fail()
        raise

    logger.info("PSxG training complete.")


if __name__ == "__main__":
    main()
