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

from ingestion.guards import FilterResult, timed_check
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
        from ingestion.guards import (
            check_upstream_freshness,
            ensure_table,
            resolve_upstream_tables_from_card,
        )

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        try:
            upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        except FileNotFoundError:
            # Card file not found — fail open (e.g., running outside Databricks Repos)
            return FilterResult(workflow_id=self.workflow_id, count=1)
        return check_upstream_freshness(spark, catalog, self.workflow_id, upstream)


skip_guard = _ModelValidationGuard()

_TABLE_NAME = "model_validation_runs"
_RESULTS_SCHEMA = (
    "run_id STRING, run_date TIMESTAMP, model_name STRING, metric_name STRING, value DOUBLE, "
    "status STRING, threshold_warn DOUBLE, threshold_alert DOUBLE, reference_value DOUBLE, _ingested_at TIMESTAMP"
)

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
    from ingestion.utils import tolerate_missing_table

    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.model_baseline_scalars"
    baselines_df: pd.DataFrame | None = None
    with tolerate_missing_table(logger, f"Cannot find {table} — returning empty baselines"):
        baselines_df = spark.table(table).toPandas()

    if baselines_df is None:
        return {}

    def _to_float(value: object) -> float:
        """Convert a pandas cell value to float, returning 0.0 for NA / None."""
        if value is None:
            return 0.0
        try:
            if pd.isna(value):  # type: ignore[arg-type]
                return 0.0
        except (TypeError, ValueError):
            # `pd.isna` can raise on non-scalar inputs we do not expect here.
            return 0.0
        return float(value)  # type: ignore[arg-type]

    result: dict[tuple[str, str], dict[str, float]] = {}
    # Convert to list[dict] so pyright types each row cell as `Any` (not
    # `Series | NDArray[bool_]`), which keeps `_to_float`'s guards safe.
    for row_dict in baselines_df.to_dict(orient="records"):
        key = (str(row_dict["model_name"]), str(row_dict["metric_name"]))
        result[key] = {
            "reference_value": _to_float(row_dict.get("reference_value")),
            "threshold_warn": _to_float(row_dict.get("threshold_warn")),
            "threshold_alert": _to_float(row_dict.get("threshold_alert")),
        }

    logger.info("Loaded %d scalar baselines from %s", len(result), table)
    return result


# ---------------------------------------------------------------------------
# Individual model validators
# ---------------------------------------------------------------------------


# _validate_xg_predictions (v1) retired SK3-MIG-B 2026-05-03 per ADR-023.
# v2 validation lives in src/tests/sk3_mig_b/test_xg_v2_post_retrain_smoke.py
# (calibration ECE, bounds, MC dropout CI band) — fired post-retrain by the
# orchestrator's _run_smoke_gate. A follow-up may add a daily Databricks-side
# v2 validator parallel to the other _validate_* functions in this module.


def _validate_action_values(
    spark: SparkSession,
    catalog: str,
    baselines: dict[tuple[str, str], dict[str, float]],
    logger: logging.Logger,
) -> list[ValidationResult]:
    """Validate VAEP action values: negative fraction, distribution shape."""
    from analytics.model_validation import ValidationResult
    from ingestion.utils import tolerate_missing_table

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values"

    vaep_df: pd.DataFrame | None = None
    with tolerate_missing_table(logger, f"Cannot find {table} — skipping VAEP validation"):
        vaep_df = spark.table(table).select("vaep_value").limit(500_000).toPandas()

    if vaep_df is None or vaep_df.empty:
        if vaep_df is not None:
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
    from ingestion.utils import tolerate_missing_table

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_passes"

    passes_df: pd.DataFrame | None = None
    with tolerate_missing_table(logger, f"Cannot find {table} — skipping line-breaking validation"):
        # is_line_breaking is BOOLEAN in the mart contract; Spark refuses
        # implicit bool→numeric cast inside avg(). Cast explicitly to INT
        # (true=1 / false=0) so the per-match avg gives the detection rate.
        passes_df = (
            spark.table(table)
            .selectExpr("match_key", "cast(is_line_breaking as int) as is_line_breaking")
            .groupBy("match_key")
            .agg({"is_line_breaking": "avg"})
            .toPandas()
        )

    if passes_df is None or passes_df.empty:
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
    from ingestion.utils import tolerate_missing_table

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_physical_stats"

    phys_df: pd.DataFrame | None = None
    with tolerate_missing_table(logger, f"Cannot find {table} — skipping physical stats validation"):
        phys_df = spark.table(table).select("max_speed_ms").limit(500_000).toPandas()

    if phys_df is None or phys_df.empty:
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
    from ingestion.utils import tolerate_missing_table

    results: list[ValidationResult] = []
    table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_pausa_values"

    pausa_df: pd.DataFrame | None = None
    with tolerate_missing_table(logger, f"Cannot find {table} — skipping PAUSA validation"):
        pausa_df = spark.table(table).select("temporal_judgment", "spatial_selection").limit(500_000).toPandas()

    if pausa_df is None or pausa_df.empty:
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
    # _validate_xg_predictions (v1) retired SK3-MIG-B 2026-05-03 per ADR-023.
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

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting model validation into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)

    # Record watermarks after successful validation
    from ingestion.guards import record_watermarks, resolve_upstream_tables_from_card

    upstream = resolve_upstream_tables_from_card(skip_guard.workflow_id, args.catalog, args.schema)
    record_watermarks(spark, args.catalog, skip_guard.workflow_id, upstream)


if __name__ == "__main__":
    main()
