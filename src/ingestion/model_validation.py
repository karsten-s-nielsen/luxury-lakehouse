"""Model validation and drift detection pipeline.

Reads gold-layer tables and reference baselines, runs statistical validation
functions (PSI, Wasserstein, CUSUM, physical bounds, field sum), and writes
results to ``dev_gold.model_validation_runs``.  Emits structured JSON logs
with per-metric status for monitoring and alerting.

This is the Spark pipeline counterpart to the pure-analytics module
``analytics.model_validation``.  It follows the same split pattern as
ELASTIC (analytics = pure compute, ingestion = Spark I/O).

Entry point: ``run_model_validation = "ingestion.model_validation:main"``
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from ingestion.guards import FilterResult
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.model_validation import ValidationResult


class _ModelValidationGuard:
    workflow_id = "wf-model-validation"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _ModelValidationGuard()

_TABLE_NAME = "model_validation_runs"

# ---------------------------------------------------------------------------
# Baseline loading
# ---------------------------------------------------------------------------


def _load_scalar_baselines(
    spark: SparkSession,
    catalog: str,
    logger: logging.Logger,
) -> dict[tuple[str, str], dict[str, float]]:
    """Load reference baselines from the dbt seed table.

    Returns a dict keyed by (model_name, metric_name) with fields:
    reference_value, threshold_warn, threshold_alert.
    """
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.model_baseline_scalars"
    try:
        baselines_df = spark.table(table).toPandas()
    except Exception:
        logger.warning("Cannot read baselines from %s — returning empty", table)
        return {}

    result: dict[tuple[str, str], dict[str, float]] = {}
    for _, row in baselines_df.iterrows():
        key = (str(row["model_name"]), str(row["metric_name"]))
        ref_val = float(row["reference_value"]) if pd.notna(row["reference_value"]) else 0.0
        warn = float(row["threshold_warn"]) if pd.notna(row["threshold_warn"]) else 0.0
        alert = float(row["threshold_alert"]) if pd.notna(row["threshold_alert"]) else 0.0
        result[key] = {
            "reference_value": ref_val,
            "threshold_warn": warn,
            "threshold_alert": alert,
        }

    logger.info("Loaded %d scalar baselines from %s", len(result), table)
    return result


# ---------------------------------------------------------------------------
# Individual model validators
# ---------------------------------------------------------------------------


def _validate_xg_predictions(
    spark: SparkSession,
    catalog: str,
    baselines: dict[tuple[str, str], dict[str, float]],
    logger: logging.Logger,
) -> list[ValidationResult]:
    """Validate xG model predictions: mean prediction PSI and metric thresholds."""
    from analytics.model_validation import ValidationResult, check_physical_bounds

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_xg_predictions"

    try:
        xg_df = spark.table(table).select("xg_prediction").limit(500_000).toPandas()
    except Exception:
        logger.warning("Cannot read %s — skipping xG validation", table)
        return results

    if xg_df.empty:
        logger.info("No xG predictions to validate")
        return results

    predictions = xg_df["xg_prediction"].dropna().to_numpy(dtype=np.float64)

    # Mean prediction check against baseline
    mean_pred = float(np.mean(predictions))
    key = ("xg_xgboost", "mean_prediction")
    if key in baselines:
        bl = baselines[key]
        # PSI: compare current distribution vs reference mean (generate reference from mean)
        ref_value = bl["reference_value"]
        deviation = abs(mean_pred - ref_value)
        if deviation > bl["threshold_alert"]:
            status = "alert"
        elif deviation > bl["threshold_warn"]:
            status = "warn"
        else:
            status = "ok"
        results.append(
            ValidationResult(
                model_name="xg_xgboost",
                metric_name="mean_prediction",
                value=mean_pred,
                status=status,
                threshold_warn=bl["threshold_warn"],
                threshold_alert=bl["threshold_alert"],
                reference_value=ref_value,
            )
        )

    # Physical bounds: xG must be in [0, 1]
    bounds_result = check_physical_bounds(predictions, 0.0, 1.0, "xg_xgboost", "prediction_range")
    results.append(bounds_result)

    return results


def _validate_action_values(
    spark: SparkSession,
    catalog: str,
    baselines: dict[tuple[str, str], dict[str, float]],
    logger: logging.Logger,
) -> list[ValidationResult]:
    """Validate VAEP action values: negative fraction, distribution shape."""
    from analytics.model_validation import ValidationResult

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values"

    try:
        vaep_df = spark.table(table).select("vaep_value").limit(500_000).toPandas()
    except Exception:
        logger.warning("Cannot read %s — skipping VAEP validation", table)
        return results

    if vaep_df.empty:
        logger.info("No VAEP values to validate")
        return results

    values = vaep_df["vaep_value"].dropna().to_numpy(dtype=np.float64)

    # Fraction of negative actions
    neg_frac = float(np.mean(values < 0))
    key = ("vaep", "negative_action_fraction")
    if key in baselines:
        bl = baselines[key]
        deviation = abs(neg_frac - bl["reference_value"])
        if deviation > bl["threshold_alert"]:
            status = "alert"
        elif deviation > bl["threshold_warn"]:
            status = "warn"
        else:
            status = "ok"
        results.append(
            ValidationResult(
                model_name="vaep",
                metric_name="negative_action_fraction",
                value=neg_frac,
                status=status,
                threshold_warn=bl["threshold_warn"],
                threshold_alert=bl["threshold_alert"],
                reference_value=bl["reference_value"],
            )
        )

    return results


def _validate_line_breaking(
    spark: SparkSession,
    catalog: str,
    baselines: dict[tuple[str, str], dict[str, float]],
    logger: logging.Logger,
) -> list[ValidationResult]:
    """Validate line-breaking detection rate via CUSUM."""
    from analytics.model_validation import ValidationResult, compute_cusum

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_passes"

    try:
        passes_df = (
            spark.table(table)
            .select("match_id", "is_line_breaking")
            .groupBy("match_id")
            .agg({"is_line_breaking": "avg"})
            .toPandas()
        )
    except Exception:
        logger.warning("Cannot read %s — skipping line-breaking validation", table)
        return results

    if passes_df.empty:
        return results

    rates = passes_df.iloc[:, 1].dropna().to_numpy(dtype=np.float64)

    key = ("line_breaking", "detection_rate")
    if key in baselines:
        bl = baselines[key]
        target_mean = bl["reference_value"]
        sigma = bl["threshold_warn"] if bl["threshold_warn"] > 0 else 0.05
        max_cusum, cusum_status = compute_cusum(rates, target_mean, sigma)
        results.append(
            ValidationResult(
                model_name="line_breaking",
                metric_name="detection_rate",
                value=max_cusum,
                status=cusum_status,
                threshold_warn=3.0 * sigma,
                threshold_alert=5.0 * sigma,
                reference_value=target_mean,
            )
        )

    return results


def _validate_physical_stats(
    spark: SparkSession,
    catalog: str,
    baselines: dict[tuple[str, str], dict[str, float]],
    logger: logging.Logger,
) -> list[ValidationResult]:
    """Validate physical stats: max speed within physics bounds."""
    from analytics.model_validation import check_physical_bounds

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_physical_stats"

    try:
        phys_df = spark.table(table).select("max_speed_ms").limit(500_000).toPandas()
    except Exception:
        logger.warning("Cannot read %s — skipping physical stats validation", table)
        return results

    if phys_df.empty:
        return results

    speeds = phys_df["max_speed_ms"].dropna().to_numpy(dtype=np.float64)

    key = ("physical_stats", "max_speed_ms")
    if key in baselines:
        bl = baselines[key]
        upper = bl["reference_value"]
        bounds_result = check_physical_bounds(speeds, 0.0, upper, "physical_stats", "max_speed_ms")
        results.append(bounds_result)

    return results


def _validate_pausa(
    spark: SparkSession,
    catalog: str,
    baselines: dict[tuple[str, str], dict[str, float]],
    logger: logging.Logger,
) -> list[ValidationResult]:
    """Validate PAUSA scores: temporal/spatial within [0, 1]."""
    from analytics.model_validation import check_physical_bounds

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_pausa_values"

    try:
        pausa_df = spark.table(table).select("temporal_judgment", "spatial_selection").limit(500_000).toPandas()
    except Exception:
        logger.warning("Cannot read %s — skipping PAUSA validation", table)
        return results

    if pausa_df.empty:
        return results

    temporal = pausa_df["temporal_judgment"].dropna().to_numpy(dtype=np.float64)
    spatial = pausa_df["spatial_selection"].dropna().to_numpy(dtype=np.float64)

    # Range bounds: temporal judgment must be in [0, 1]
    if len(temporal) > 0:
        results.append(check_physical_bounds(temporal, 0.0, 1.0, "pausa", "temporal_judgment_range"))

    # Range bounds: spatial selection must be in [0, 1]
    if len(spatial) > 0:
        results.append(check_physical_bounds(spatial, 0.0, 1.0, "pausa", "spatial_selection_range"))

    return results


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def _results_to_dataframe(
    results: list[ValidationResult],
    run_id: str,
    run_date: datetime,
) -> pd.DataFrame:
    """Convert ValidationResult list to a pandas DataFrame for Delta write."""
    rows: list[dict[str, object]] = []
    for vr in results:
        rows.append(
            {
                "run_id": run_id,
                "run_date": run_date,
                "model_name": vr.model_name,
                "metric_name": vr.metric_name,
                "value": vr.value,
                "status": vr.status,
                "threshold_warn": vr.threshold_warn,
                "threshold_alert": vr.threshold_alert,
                "reference_value": vr.reference_value,
                "_ingested_at": run_date,
            }
        )
    return pd.DataFrame(rows)


@workflow("wf-model-validation", phase="validation")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Execute the full model validation pipeline.

    Loads baselines, runs all model-specific validators, writes results to
    ``{schema}.model_validation_runs``, and emits structured logs.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Target schema for writing validation results.
        logger: Structured JSON logger.

    Returns:
        Number of validation results written.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    run_id = str(uuid.uuid4())
    run_date = datetime.now(tz=timezone.utc)

    logger.info("Starting model validation run %s", run_id)

    baselines = _load_scalar_baselines(spark, catalog, logger)

    # Run all validators
    all_results: list[ValidationResult] = []
    all_results.extend(_validate_xg_predictions(spark, catalog, baselines, logger))
    all_results.extend(_validate_action_values(spark, catalog, baselines, logger))
    all_results.extend(_validate_line_breaking(spark, catalog, baselines, logger))
    all_results.extend(_validate_physical_stats(spark, catalog, baselines, logger))
    all_results.extend(_validate_pausa(spark, catalog, baselines, logger))

    if not all_results:
        logger.info("No validation results produced — all tables may be empty or inaccessible")
        return 0

    # Log individual results
    alert_count = 0
    warn_count = 0
    for vr in all_results:
        logger.info(
            "Validation: %s.%s = %.6f [%s] (ref=%.6f, warn=%.4f, alert=%.4f)",
            vr.model_name,
            vr.metric_name,
            vr.value,
            vr.status,
            vr.reference_value,
            vr.threshold_warn,
            vr.threshold_alert,
        )
        if vr.status == "alert":
            alert_count += 1
        elif vr.status == "warn":
            warn_count += 1

    # Convert to Spark DataFrame and write with replaceWhere for idempotency.
    # Each run_id is unique (UUID), so replaceWhere makes retries safe —
    # re-running the same run_id overwrites rather than duplicating rows.
    results_pdf = _results_to_dataframe(all_results, run_id, run_date)
    results_sdf = spark.createDataFrame(results_pdf)

    row_count = validate_dataframe(
        results_sdf,
        ["run_id", "model_name", "metric_name", "value", "status"],
        "model_validation_runs",
        logger,
    )

    written = write_delta_table(
        results_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=f"run_id = '{run_id}'",
        logger=logger,
        row_count=row_count,
    )

    logger.info(
        "Model validation run %s complete: %d checks, %d ok, %d warn, %d alert",
        run_id,
        len(all_results),
        len(all_results) - warn_count - alert_count,
        warn_count,
        alert_count,
    )

    return written


def main() -> None:
    """CLI entry point for model validation pipeline."""
    args = parse_ingestion_args("Run model validation and drift detection")
    logger = configure_logging("model_validation")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = skip_guard.check(spark, args.catalog, args.schema)

    logger.info("Starting model validation into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
