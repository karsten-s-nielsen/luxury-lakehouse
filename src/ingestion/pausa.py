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

from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "fct_pausa_values"
_GOLD_SCHEMA = "dev_gold"


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
) -> int:
    """Process all matches from pausa_raw_scores via applyInPandas.

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    raw_table = f"{catalog}.bronze.pausa_raw_scores"
    results_table = f"{catalog}.{_GOLD_SCHEMA}.{_TABLE_NAME}"

    # Read raw OBSO scalars
    try:
        raw_df = spark.table(raw_table)
    except Exception:
        logger.warning("Cannot read table %s — run OBSO batch (D16) first", raw_table)
        return 0

    # Check which matches have raw data
    try:
        match_id_rows = raw_df.select("match_id").distinct().collect()
    except Exception:
        logger.warning("Cannot read match_id from %s", raw_table)
        return 0

    if not match_id_rows:
        logger.info("No matches in %s", raw_table)
        return 0

    all_match_ids = [row["match_id"] for row in match_id_rows]

    # Incremental skip guard: check already-processed matches
    existing_ids: set[str] = set()
    try:
        existing_rows = spark.table(results_table).select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in all_match_ids if str(mid) not in existing_ids]
    logger.info(
        "%d matches total, %d already processed, %d to process",
        len(all_match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # Filter raw data to new matches only
    new_ids_str = [str(mid) for mid in new_match_ids]
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
        _GOLD_SCHEMA,
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
    ctx=None,
) -> int:
    """Execute the PAUSA computation pipeline."""
    total = _process_matches(spark, catalog, schema, logger)
    logger.info("PAUSA pipeline complete — %d total rows written", total)
    return total


def main() -> None:
    """CLI entry point for PAUSA computation."""
    args = parse_ingestion_args("Compute PAUSA scores from OBSO raw data")
    logger = configure_logging("pausa")
    spark = get_spark_session()

    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook

    register_hook(CostEstimateHook(spark, args.catalog, args.schema))

    logger.info("Starting PAUSA pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
