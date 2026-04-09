"""Line-breaking pass detection batch computation pipeline.

Reads pass events and opponent positions from existing bronze Delta tables,
runs the line-breaking detection algorithm, and writes results to a new
``line_breaking_results`` bronze table.

Three data paths:
  - **Path A (360 freeze frames):** StatsBomb matches with per-event opponent
    positions from ``statsbomb_360``.
  - **Path B (Metrica tracking):** Metrica matches with frame-level tracking
    joined to event data.
  - **Path C (IDSSE tracking):** IDSSE (Bundesliga) matches with narrow-format
    tracking joined to DFL events via temporal proximity.

Architecture: Uses ``applyInPandas`` to distribute line-breaking detection
across Spark executors instead of sequential per-match driver loops.  Each
match group is processed independently via ``detect_line_breaking_batch``
with Ward cluster caching.

Design: "Read from bronze, compute, write to bronze." No external API calls.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult
from ingestion.line_breaking_360 import _make_statsbomb_udf as _make_statsbomb_udf
from ingestion.line_breaking_360 import _process_statsbomb_360
from ingestion.line_breaking_common import _RESULT_COLUMNS as _RESULT_COLUMNS
from ingestion.line_breaking_common import _TABLE_NAME
from ingestion.line_breaking_tracking import _make_idsse_udf as _make_idsse_udf
from ingestion.line_breaking_tracking import _make_metrica_udf as _make_metrica_udf
from ingestion.line_breaking_tracking import _process_idsse_tracking, _process_metrica_tracking
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _LineBreakingGuard:
    """SkipGuard adapter for line-breaking pass detection pipeline."""

    workflow_id = "wf-line-breaking"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if any data path has unprocessed matches."""
        from ingestion.guards import find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

        sb_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.statsbomb_360",
            results_table,
            results_filter="data_source = 'statsbomb_360'",
        )
        metrica_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.metrica_events",
            results_table,
            source_filter="type = 'PASS'",
            results_filter="data_source = 'metrica_tracking'",
        )
        idsse_ids = find_new_ids(
            spark,
            f"{catalog}.bronze.idsse_events",
            results_table,
            source_filter="event_type IN ('successfulPassEvent', 'failedPassEvent')",
            results_filter="data_source = 'idsse_tracking'",
        )

        total_new = len(sb_ids) + len(metrica_ids) + len(idsse_ids)
        if total_new == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=total_new,
            metadata={
                "statsbomb_360_ids": sb_ids,
                "metrica_ids": metrica_ids,
                "idsse_ids": idsse_ids,
            },
        )


skip_guard = _LineBreakingGuard()


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-line-breaking", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> None:
    """Execute the line-breaking detection pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from analytics.line_breaking import LineBreakingParams

    params = LineBreakingParams()

    sb_ids = filter_result.metadata.get("statsbomb_360_ids", [])
    metrica_ids = filter_result.metadata.get("metrica_ids", [])
    idsse_ids = filter_result.metadata.get("idsse_ids", [])

    path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params, new_ids=sb_ids)
    path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params, new_ids=metrica_ids)
    path_c_rows = _process_idsse_tracking(spark, catalog, schema, logger, params, new_ids=idsse_ids)

    total = path_a_rows + path_b_rows + path_c_rows
    logger.info("Line-breaking pipeline complete — %d total rows written", total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for line-breaking pass detection."""
    args = parse_ingestion_args("Detect line-breaking passes from 360 and tracking data")
    logger = configure_logging("line_breaking")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-line-breaking")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    logger.info("Starting line-breaking pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
