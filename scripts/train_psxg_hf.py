# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.5.54-py3-none-any.whl",
#     "numpy>=1.26.0",
#     "pandas>=2.0.0",
#     "pyarrow>=14.0.0",
#     "scikit-learn>=1.3.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "databricks-sdk>=0.102.0",
# ]
# ///
"""Train Post-Shot Expected Goals (PSxG) model on HF Jobs.

Following Butcher et al. (2025), "An Expected Goals On Target (xGOT) Model".

The PSxG logistic regression model predicts goal probability for on-target shots
from a 4-feature vector (projected-goalmouth distance-from-centre, crossing height,
distance-to-goal, shot angle — see ``analytics.goalkeeper.PSXG_FEATURE_NAMES``).
Model weights are serialised to JSON (zero pickle surface) and published to
HF Hub alongside per-shot predictions.

Usage (HF Jobs):  hf jobs run train_psxg_hf.py --flavor l40sx1
Usage (local):    uv run scripts/train_psxg_hf.py
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
import tempfile
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from huggingface_hub import HfApi

from analytics.goalkeeper import (
    PSXG_FEATURE_NAMES,
    PSxGModel,
    cross_validate_psxg_by_match,
    predict_psxg,
    serialize_psxg_model,
    train_psxg_model,
)
from ingestion.artifact_deploy import (
    require_mlflow_env,
    set_and_verify_mlflow_champion,
    upload_weights_to_uc_volume,
)
from ingestion.hf_jobs_cost import HF_RATE_A10G_LARGE, HFJobsCostRecorder
from ingestion.hf_publish import get_hf_card_path, upload_hf_readme
from shared.constants import mlflow_model_uri

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Make stdout UTF-8 so third-party emoji (e.g. MLflow's "🏃 View run …" line) can't
# crash a non-UTF-8 console (Windows cp1252 raises UnicodeEncodeError). HF Jobs (Linux)
# is already UTF-8, so this is a no-op there.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except (ValueError, OSError):
        pass

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HF_ORG = "luxury-lakehouse"
INPUT_DATASET = f"{HF_ORG}/statsbomb-shots-on-target"
OUTPUT_MODEL = f"{HF_ORG}/psxg-model"
OUTPUT_PREDICTIONS = f"{HF_ORG}/psxg-predictions"

# Delivery targets (ADR-012). The MLflow registry name and the UC Volume model
# directory differ: the registry FQN is the governance handle; the Volume dir +
# filename are what the tracking writer reads via --model-path.
CATALOG = "soccer_analytics"
SCHEMA = "dev_gold"
MLFLOW_MODEL_NAME = "psxg_model"  # -> soccer_analytics.dev_gold.psxg_model
UC_VOLUME_MODEL_DIR = "psxg"  # -> /Volumes/soccer_analytics/dev_gold/model_weights/psxg/
WEIGHTS_FILENAME = "psxg_model.json"

# v1 = legacy `end_location_z IS NOT NULL` population (46% off-target contamination);
# v2 = true on-target population (Goal/Saved/Post/Saved to Post, D-0). Embedded in the
# envelope + logged to MLflow; the GK-page recalibration caption references it.
MODEL_VERSION = "v2-ontarget"

# Out-of-sample CV folds — GroupKFold by match (no same-match leakage, the xT-3 class).
N_CV_SPLITS = 5


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

    # Read ONLY the canonical single file when present. Concatenating stale Spark
    # part-files of a different schema/population silently contaminates training
    # (the ADR-049 stale-part-file class — saved-shot rows with NULL match_key /
    # the old off-target population poisoned a prior retrain). Fall back to the
    # glob only when the canonical file is absent (legacy Spark-publish layout).
    canonical = [p for p in parquet_files if p.name == "shots_on_target.parquet"]
    files_to_read = canonical if canonical else parquet_files
    df = pd.concat([pd.read_parquet(f) for f in files_to_read], ignore_index=True)
    logger.info(
        "Loaded %d on-target shots from %s (%d of %d parquet files)",
        len(df),
        INPUT_DATASET,
        len(files_to_read),
        len(parquet_files),
    )
    return df


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def population_summary(shots_df: pd.DataFrame, model: PSxGModel) -> dict[str, float | int]:
    """In-sample population context (NOT the governed metric).

    The headline, governed metrics are out-of-sample via
    :func:`analytics.goalkeeper.cross_validate_psxg_by_match` (GroupKFold by match).
    This adds population context for the model card / MLflow: on-target shot count,
    observed goal rate, and mean predicted PSxG (a coarse calibration sanity check —
    mean PSxG should sit close to the goal rate).

    Args:
        model: Fitted ``PSxGModel`` from ``train_psxg_model`` (trained on all shots).
        shots_df: Full on-target shots with ``is_goal`` and goalmouth coordinates.

    Returns:
        Dict with ``n_shots``, ``goal_rate``, ``mean_psxg``.
    """
    result = predict_psxg(model, shots_df)
    on_target = result["psxg"].notna()
    y_true = result.loc[on_target, "is_goal"].to_numpy(dtype=np.int64)
    y_pred = result.loc[on_target, "psxg"].to_numpy(dtype=np.float64)
    return {
        "n_shots": int(on_target.sum()),
        "goal_rate": float(y_true.mean()),
        "mean_psxg": float(y_pred.mean()),
    }


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


def publish_model(weight_bytes: bytes) -> None:
    """Publish the serialized PSxG envelope to HF Hub + upload the model card.

    Takes the already-serialized envelope bytes (from
    :func:`analytics.goalkeeper.serialize_psxg_model`) so the HF Hub copy, the
    MLflow artifact, and the UC Volume copy are byte-identical — one serialization,
    three destinations (ADR-012).

    Args:
        weight_bytes: The canonical JSON envelope bytes (carries ``feature_names`` +
            ``model_version`` per ADR-012 §2).
    """
    api = HfApi()
    api.create_repo(OUTPUT_MODEL, exist_ok=True, repo_type="model")
    api.upload_file(
        path_or_fileobj=weight_bytes,
        path_in_repo=WEIGHTS_FILENAME,
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
    """Train, evaluate (out-of-sample), register, and publish the PSxG model.

    Pipeline:
      1. Pre-flight: ``require_mlflow_env()`` (ADR-012 — no silent registry skip).
      2. Load the corrected on-target shots from HF Hub.
      3. Out-of-sample CV: GroupKFold by match (no same-match leakage) — the governed metric.
      4. Train the FINAL model on all shots; serialize the envelope once
         (``feature_names`` + ``model_version``, ADR-012 §2).
      5. MLflow: log params/metrics + artifact + register + ``@Champion`` (zombie-alias guard).
      6. Deliver: HF Hub (weights + predictions) + UC Volume weights (ADR-012 second leg —
         the tracking writer's ``--model-path`` source).
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
        # 1. Pre-flight (ADR-012): fail loud if MLflow/Databricks env vars are missing.
        require_mlflow_env()

        # 2. Load data
        logger.info("=== Loading on-target shots from HF Hub ===")
        shots_df = load_shots()
        if shots_df.empty:
            logger.error("Dataset is empty — aborting.")
            sys.exit(1)

        # 3. Out-of-sample CV (GroupKFold by match) — the governed, leakage-free metric.
        logger.info("=== Cross-validating (GroupKFold by match, %d splits) ===", N_CV_SPLITS)
        oos_metrics = cross_validate_psxg_by_match(shots_df, n_splits=N_CV_SPLITS)
        logger.info("Out-of-sample metrics: %s", json.dumps(oos_metrics, indent=2))
        if not math.isfinite(float(oos_metrics["brier_score"])):
            logger.error("Out-of-sample CV degenerate (insufficient match groups) — aborting.")
            sys.exit(1)

        # 4. Train the FINAL model on ALL shots; serialize the envelope once.
        logger.info("=== Training final PSxG model on all %d shots ===", len(shots_df))
        model = train_psxg_model(shots_df)
        logger.info("Trained — coefficients: %s, intercept: %.4f", model.coefficients.tolist(), model.intercept)

        population = population_summary(shots_df, model)
        metrics: dict[str, float | int] = {**oos_metrics, **population}
        weight_bytes = serialize_psxg_model(model, metrics=metrics, model_version=MODEL_VERSION)

        # 5. MLflow register + @Champion (ADR-012 zombie-alias guard).
        mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MLFLOW_MODEL_NAME)
        mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
        mlflow.set_experiment("/soccer_analytics/psxg_model")
        with mlflow.start_run(run_name="psxg-logistic-hf-jobs"):
            mlflow.log_params(
                {
                    "model_type": "logistic_regression",
                    "model_version": MODEL_VERSION,
                    "features": ",".join(PSXG_FEATURE_NAMES),
                    "n_shots": population["n_shots"],
                    "cv": f"GroupKFold(by match, n_splits={N_CV_SPLITS})",
                    "reference": "Butcher et al. (2025) An Expected Goals On Target (xGOT) Model",
                }
            )
            # MLflow rejects NaN — log only finite metrics (CV is finite at production scale).
            for key, value in metrics.items():
                if math.isfinite(float(value)):
                    mlflow.log_metric(key, float(value))

            with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False, dir=tempfile.gettempdir()) as tmp:
                tmp.write(weight_bytes)
                tmp_path = tmp.name
            artifact_path = os.path.join(os.path.dirname(tmp_path), WEIGHTS_FILENAME)
            os.replace(tmp_path, artifact_path)
            mlflow.log_artifact(artifact_path)

            class _W(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
                def predict(self, context: object, model_input: pd.DataFrame) -> np.ndarray:  # type: ignore[override]
                    # Registry/governance stub — real inference reads the JSON envelope
                    # from the UC Volume / HF Hub (zero pickle surface). Mirrors xg_v2.
                    return np.zeros(len(model_input))

            mlflow.pyfunc.log_model(
                python_model=_W(),
                artifact_path="psxg_model",
                registered_model_name=mlflow_fqn,
                input_example=pd.DataFrame({"x": [0.0]}),
            )
            run_id = mlflow.active_run().info.run_id

        client = mlflow.tracking.MlflowClient()
        champion_version = set_and_verify_mlflow_champion(client, mlflow_fqn=mlflow_fqn, run_id=run_id)
        logger.info("MLflow @Champion set: %s version %s", mlflow_fqn, champion_version)

        # 6a. Publish to HF Hub (weights envelope + predictions).
        logger.info("=== Publishing to HF Hub ===")
        publish_model(weight_bytes)
        publish_predictions(shots_df, model)

        # 6b. UC Volume (ADR-012 second leg) — the tracking writer's --model-path source.
        from databricks.sdk import WorkspaceClient

        workspace_client = WorkspaceClient()
        volume_result = upload_weights_to_uc_volume(
            workspace_client,
            catalog=CATALOG,
            schema=SCHEMA,
            model_name=UC_VOLUME_MODEL_DIR,
            filename=WEIGHTS_FILENAME,
            weights_bytes=weight_bytes,
        )
        logger.info("UC Volume publish complete: %s", volume_result["path"])

        recorder.complete({"psxg": metrics, "model_version": MODEL_VERSION}, row_count=len(shots_df))

    except Exception as exc:
        recorder.fail(exc)
        raise

    logger.info("PSxG training complete (model_version=%s).", MODEL_VERSION)


if __name__ == "__main__":
    main()
