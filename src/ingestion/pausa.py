"""PAUSA batch computation pipeline — temporal judgment, spatial selection, composite score.

Reads pre-computed OBSO scalars from ``pausa_raw_scores`` (produced by D16 GPU
batch), enriches with event metadata from ``elastic_sync_results`` and
``stg_idsse__events``, computes PAUSA decomposition, and writes results to
``fct_pausa_values`` in the gold layer.

Architecture: ``applyInPandas`` grouped by ``match_id`` — each of the 7 IDSSE
matches is processed as one group on a Spark executor.

Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying
Optimal Pass Timing Beyond Speed." MIT Sloan 2026.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

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

_TABLE_NAME = "fct_pausa_values"
_RESULTS_SCHEMA = (
    "pass_id STRING, match_id STRING, player_id STRING, team STRING, period INT, "
    "timestamp_seconds DOUBLE, frame_id INT, temporal_judgment DOUBLE, spatial_selection DOUBLE, "
    "pausa_score DOUBLE, actual_obso DOUBLE, peak_obso DOUBLE, optimal_obso DOUBLE, "
    "receiver_x DOUBLE, receiver_y DOUBLE, _ingested_at TIMESTAMP"
)
_guard_logger = logging.getLogger(f"{__name__}.guard")


class _PausaGuard:
    """SkipGuard adapter for PAUSA batch pipeline."""

    workflow_id = "wf-obso-pausa"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which matches need PAUSA computation."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        new_match_ids = find_new_ids(
            spark,
            source_table=f"{catalog}.bronze.pausa_raw_scores",
            results_table=results_table,
        )

        if not new_match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            metadata={"new_match_ids": new_match_ids},
        )


skip_guard = _PausaGuard()


def _make_pausa_udf() -> object:
    """Build the ``applyInPandas`` UDF closure for PAUSA scoring.

    The UDF is a pure closure with no captured state — all computation is
    self-contained via the ``analytics.pausa`` module installed on executors.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Compute PAUSA scores for one match_id group."""
        import pandas as _pd

        from analytics.pausa import compute_pausa_scores

        _empty = _pd.DataFrame(
            columns=_pd.Index(
                [
                    "pass_id",
                    "match_id",
                    "player_id",
                    "team",
                    "period",
                    "timestamp_seconds",
                    "frame_id",
                    "temporal_judgment",
                    "spatial_selection",
                    "pausa_score",
                    "actual_obso",
                    "peak_obso",
                    "optimal_obso",
                    "receiver_x",
                    "receiver_y",
                ]
            )
        )

        if pdf.empty:
            return _empty

        scored = compute_pausa_scores(pdf)

        # Select output columns in the declared schema order
        out_cols = [
            "pass_id",
            "match_id",
            "player_id",
            "team",
            "period",
            "timestamp_seconds",
            "frame_id",
            "temporal_judgment",
            "spatial_selection",
            "pausa_score",
            "actual_obso",
            "peak_obso",
            "optimal_obso",
            "receiver_x",
            "receiver_y",
        ]
        # Only select columns that exist (defensive)
        available = [c for c in out_cols if c in scored.columns]
        return _pd.DataFrame(scored[available])

    return _udf


def _process_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
) -> int:
    """Process all matches from pausa_raw_scores via applyInPandas.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    from workflows.exceptions import WorkflowSkippedError

    raw_table = f"{catalog}.bronze.pausa_raw_scores"

    # Read raw OBSO scalars. If the upstream OBSO batch hasn't run yet the table
    # doesn't exist — raise WorkflowSkippedError (not return 0) so the workflow
    # runner records a SKIPPED state instead of a false-success completion.
    try:
        raw_df = spark.table(raw_table)
    except Exception as exc:
        msg = (
            f"Cannot read table {raw_table} — upstream OBSO batch (wf-import-obso) "
            "has not run yet. Waiting for upstream; skipping PAUSA computation."
        )
        raise WorkflowSkippedError(msg) from exc

    new_ids_str = filter_result.metadata["new_match_ids"]
    logger.info("%d matches to process", len(new_ids_str))

    if not new_ids_str:
        return 0
    filtered_df = raw_df.filter(F.col("match_id").isin(new_ids_str))

    # Ensure required columns exist — fill optional columns with defaults
    required_cols = {"pass_id", "match_id", "actual_obso", "peak_obso", "optimal_obso"}
    available_cols = set(filtered_df.columns)
    missing = required_cols - available_cols
    if missing:
        logger.error("Missing required columns in %s: %s", raw_table, missing)
        return 0

    # Add default values for optional event-metadata columns if not present
    optional_defaults: dict[str, object] = {
        "player_id": F.lit(None).cast("string"),
        "team": F.lit(None).cast("string"),
        "period": F.lit(None).cast("int"),
        "timestamp_seconds": F.lit(None).cast("double"),
        "frame_id": F.lit(None).cast("int"),
        "receiver_x": F.lit(None).cast("double"),
        "receiver_y": F.lit(None).cast("double"),
    }
    for col_name, default_expr in optional_defaults.items():
        if col_name not in available_cols:
            filtered_df = filtered_df.withColumn(col_name, default_expr)  # type: ignore[arg-type]

    # Select only columns we need
    input_cols = [
        "pass_id",
        "match_id",
        "player_id",
        "team",
        "period",
        "timestamp_seconds",
        "frame_id",
        "actual_obso",
        "peak_obso",
        "optimal_obso",
        "receiver_x",
        "receiver_y",
    ]
    filtered_df = filtered_df.select(*[c for c in input_cols if c in filtered_df.columns])

    # Build UDF
    udf_fn = _make_pausa_udf()

    # Output schema
    output_schema = StructType(
        [
            StructField("pass_id", StringType(), nullable=False),
            StructField("match_id", StringType(), nullable=False),
            StructField("player_id", StringType(), nullable=True),
            StructField("team", StringType(), nullable=True),
            StructField("period", IntegerType(), nullable=True),
            StructField("timestamp_seconds", DoubleType(), nullable=True),
            StructField("frame_id", IntegerType(), nullable=True),
            StructField("temporal_judgment", DoubleType(), nullable=False),
            StructField("spatial_selection", DoubleType(), nullable=False),
            StructField("pausa_score", DoubleType(), nullable=False),
            StructField("actual_obso", DoubleType(), nullable=True),
            StructField("peak_obso", DoubleType(), nullable=True),
            StructField("optimal_obso", DoubleType(), nullable=True),
            StructField("receiver_x", DoubleType(), nullable=True),
            StructField("receiver_y", DoubleType(), nullable=True),
        ]
    )

    # applyInPandas grouped by match_id
    result_df = filtered_df.groupBy("match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=output_schema,
    )

    # Validate schema and non-empty data before write
    required_cols = [
        "pass_id",
        "match_id",
        "temporal_judgment",
        "spatial_selection",
        "pausa_score",
    ]
    row_count = validate_dataframe(result_df, required_cols, "pausa", logger)

    # Write with replaceWhere for idempotent incremental writes
    ids_sql = ", ".join(f"'{mid}'" for mid in new_ids_str)
    written = write_delta_table(
        result_df,
        catalog,
        DEFAULT_GOLD_SCHEMA,
        _TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
        row_count=row_count,
    )

    logger.info("PAUSA processing complete: %d rows written", written)
    return written


@workflow("wf-obso-pausa", phase="inference")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Execute the PAUSA computation pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    total = _process_matches(spark, catalog, schema, logger, filter_result=filter_result)
    logger.info("PAUSA pipeline complete — %d total rows written", total)
    return total


def main() -> None:
    """CLI entry point for PAUSA computation."""
    args = parse_ingestion_args("Compute PAUSA scores from OBSO raw data")
    logger = configure_logging("pausa")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting PAUSA pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
