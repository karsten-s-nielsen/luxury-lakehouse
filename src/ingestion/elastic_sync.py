"""ELASTIC event-tracking synchronization pipeline.

Reads IDSSE event and tracking data from bronze Delta tables, aligns each
event to its best-matching tracking frame using ball acceleration and
player-ball proximity features, and writes alignment results to a new
``elastic_sync_results`` bronze table.

Processes each match sequentially on the driver via per-match
``.toPandas()`` with explicit memory release (``del`` + ``gc.collect()``).
This is the correct pattern for ELASTIC sync because:

1. The algorithm requires the full match timeline — cannot split by frame
   batches without breaking ball-acceleration feature continuity.
2. Only 7 IDSSE matches exist — bounded, sequential is acceptable.
3. Each match is ~170 MB in pandas with column selection (driver has 16 GB).
4. ``applyInPandas`` is infeasible: each 3M-row match exceeds the 1 GB
   serverless executor memory limit.

This follows the entity resolution precedent (CLAUDE.md: "CANNOT migrate
to applyInPandas" for global cross-source operations).

Algorithm reference:
  Kim, H.S. et al. (2025). "ELASTIC: Event-Tracking Data Synchronization
  in Soccer Without Annotated Event Locations." ECML-PKDD MLSA 2025.
  arXiv:2508.09238.

Design: "Read from bronze, compute, write to bronze." No external API calls.
"""

from __future__ import annotations

import gc
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "elastic_sync_results"
_guard_logger = logging.getLogger(f"{__name__}.guard")

_RESULT_COLUMNS = ["match_id", "event_id", "frame_id", "alignment_confidence", "alignment_error_seconds"]


class _ElasticSyncGuard:
    """SkipGuard adapter for ELASTIC event-tracking synchronization."""

    workflow_id = "wf-elastic-sync"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which IDSSE matches need ELASTIC synchronization."""
        from ingestion.guards import find_new_ids

        new_match_ids = find_new_ids(
            spark,
            source_table=f"{catalog}.{schema}.idsse_events",
            results_table=f"{catalog}.{schema}.{_TABLE_NAME}",
        )

        if not new_match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            metadata={"new_match_ids": new_match_ids},
        )


skip_guard = _ElasticSyncGuard()


# ---------------------------------------------------------------------------
# Per-match driver-side processing
# ---------------------------------------------------------------------------


def _process_single_match(
    spark: SparkSession,
    tracking_table: str,
    match_id: str,
    events_pdf: pd.DataFrame,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Align events to tracking frames for a single match on the driver.

    Reads tracking data for one match via ``.toPandas()``, runs the ELASTIC
    alignment algorithm, then releases the tracking DataFrame to free memory.

    Args:
        spark: Active Spark session.
        tracking_table: Fully-qualified tracking table name.
        match_id: Match identifier to process.
        events_pdf: Pre-filtered events for this match (pandas).
        logger: Structured logger instance.

    Returns:
        Alignment results as a pandas DataFrame (may be empty).
    """
    from pyspark.sql import functions as F  # noqa: N812

    from analytics.elastic_sync import ElasticSyncParams, align_events_to_frames

    empty = pd.DataFrame(columns=pd.Index(_RESULT_COLUMNS))

    # Select only needed columns; alias `timestamp` → `timestamp_seconds`
    tracking_pdf = (
        spark.table(tracking_table)
        .filter(F.col("match_id") == match_id)
        .select(
            F.col("match_id"),
            F.col("timestamp").alias("timestamp_seconds"),
            F.col("frame"),
            F.col("period"),
            F.col("player_id"),
            F.col("x"),
            F.col("y"),
            F.col("ball_x"),
            F.col("ball_y"),
            F.col("frame_rate"),
        )
        .toPandas()
    )

    logger.info(
        "Match %s: %d tracking rows (~%.0f MB), %d events",
        match_id,
        len(tracking_pdf),
        tracking_pdf.memory_usage(deep=True).sum() / (1024 * 1024),
        len(events_pdf),
    )

    if tracking_pdf.empty or events_pdf.empty:
        del tracking_pdf
        gc.collect()
        return empty

    frame_rate = int(tracking_pdf["frame_rate"].iloc[0]) if "frame_rate" in tracking_pdf.columns else 25

    try:
        result = align_events_to_frames(
            events_df=events_pdf,
            tracking_df=tracking_pdf,
            frame_rate=frame_rate,
            params=ElasticSyncParams(),
        )
    finally:
        # Release tracking memory immediately — each match is ~170 MB
        del tracking_pdf
        gc.collect()

    if result.empty:
        return empty

    result["match_id"] = match_id
    return pd.DataFrame(result[_RESULT_COLUMNS])


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


@workflow("wf-elastic-sync", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> int:
    """Execute the ELASTIC event-tracking synchronization pipeline.

    Reads ``idsse_events`` to the driver (metadata-scale: ~1,500 rows per
    match).  For each match, reads ``idsse_tracking`` to the driver via
    per-match ``.toPandas()`` (~170 MB), runs alignment, and releases
    memory before the next match.  Results are accumulated in a list of
    small pandas DataFrames, converted to Spark, and written to Delta.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Target schema (e.g. ``bronze``).
        logger: Structured logger instance.

    Returns:
        Number of rows written.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    events_table = f"{catalog}.{schema}.idsse_events"
    tracking_table = f"{catalog}.{schema}.idsse_tracking"

    new_match_ids = filter_result.metadata["new_match_ids"]
    logger.info("ELASTIC sync: %d matches to process", len(new_match_ids))

    if not new_match_ids:
        return 0

    # --- pyspark imports deferred past early-exit guards ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    # -----------------------------------------------------------------------
    # Collect all events to driver (metadata-scale: ~1,500 rows/match)
    # -----------------------------------------------------------------------
    events_pdf = (
        spark.table(events_table)
        .filter(F.col("match_id").isin(new_match_ids))
        .select(
            F.col("match_id"),
            F.col("event_id"),
            F.col("event_type"),
            F.col("timestamp_seconds"),
            F.col("period"),
            F.col("player_id"),
        )
        .toPandas()
    )
    logger.info("Collected %d event rows to driver for %d matches", len(events_pdf), len(new_match_ids))

    if events_pdf.empty:
        logger.info("No events after filter — skipping ELASTIC sync")
        return 0

    # Pre-group events by match_id for O(1) lookup in the per-match loop
    events_by_match: dict[str, pd.DataFrame] = {
        str(mid): pd.DataFrame(group) for mid, group in events_pdf.groupby("match_id")
    }

    # -----------------------------------------------------------------------
    # Process each match sequentially on the driver
    # -----------------------------------------------------------------------
    all_results: list[pd.DataFrame] = []
    for i, match_id in enumerate(new_match_ids, 1):
        logger.info("Processing match %d/%d: %s", i, len(new_match_ids), match_id)

        match_events = events_by_match.get(match_id)
        if match_events is None or match_events.empty:
            logger.info("Match %s: no events — skipping", match_id)
            continue

        result_pdf = _process_single_match(spark, tracking_table, match_id, match_events, logger)

        if not result_pdf.empty:
            all_results.append(result_pdf)
            logger.info("Match %s: %d alignments produced", match_id, len(result_pdf))
        else:
            logger.info("Match %s: no alignments produced", match_id)

    if not all_results:
        logger.info("No alignment results across all matches")
        return 0

    # -----------------------------------------------------------------------
    # Combine results and write to Delta
    # -----------------------------------------------------------------------
    combined_pdf = pd.concat(all_results, ignore_index=True)
    logger.info("Total alignment results: %d rows across %d matches", len(combined_pdf), len(all_results))

    # Convert to Spark DataFrame for Delta write
    result_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=True),
            StructField("event_id", StringType(), nullable=True),
            StructField("frame_id", IntegerType(), nullable=True),
            StructField("alignment_confidence", DoubleType(), nullable=True),
            StructField("alignment_error_seconds", DoubleType(), nullable=True),
        ]
    )
    result_sdf = spark.createDataFrame(combined_pdf, schema=result_schema)

    required_cols = list(_RESULT_COLUMNS)
    row_count = validate_dataframe(result_sdf, required_cols, "elastic_sync_results", logger)

    # Write with replaceWhere scoped to the processed match IDs for idempotency.
    # Without replaceWhere the entire table would be overwritten on every run,
    # destroying results from earlier incremental runs.
    match_ids_str = ", ".join(f"'{mid}'" for mid in new_match_ids)
    replace_expr = f"match_id IN ({match_ids_str})"
    written = write_delta_table(
        result_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=replace_expr,
        logger=logger,
        row_count=row_count,
    )

    logger.info("ELASTIC sync complete: %d alignment rows written", written)
    return written


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for ELASTIC event-tracking synchronization."""
    args = parse_ingestion_args("Synchronize IDSSE events with tracking frames via ELASTIC algorithm")
    logger = configure_logging("elastic_sync")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-elastic-sync")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    logger.info("Starting ELASTIC sync pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("ELASTIC sync pipeline complete")


if __name__ == "__main__":
    main()
