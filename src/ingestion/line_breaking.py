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

from analytics.line_breaking import LineBreakingParams
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

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_guard_logger = logging.getLogger(f"{__name__}.guard")


class _LineBreakingGuard:
    """SkipGuard adapter for line-breaking pass detection pipeline."""

    workflow_id = "wf-line-breaking"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if any data path has unprocessed matches."""
        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

        total_new = 0

        # Path A: StatsBomb 360
        try:
            a_rows = spark.table(f"{catalog}.bronze.statsbomb_360").select("match_id").distinct().collect()
            a_all = {str(r["match_id"]) for r in a_rows}
        except Exception:
            a_all = set()

        # Path B: Metrica events
        try:
            b_rows = (
                spark.table(f"{catalog}.bronze.metrica_events")
                .filter("event_type = 'PASS'")
                .select("match_id")
                .distinct()
                .collect()
            )
            b_all = {str(r["match_id"]) for r in b_rows}
        except Exception:
            b_all = set()

        # Path C: IDSSE events
        try:
            c_rows = (
                spark.table(f"{catalog}.bronze.idsse_events")
                .filter("event_type = 'successfulPassEvent' OR event_type = 'failedPassEvent'")
                .select("match_id")
                .distinct()
                .collect()
            )
            c_all = {str(r["match_id"]) for r in c_rows}
        except Exception:
            c_all = set()

        # Existing results by data source
        for source, source_ids in [
            ("statsbomb_360", a_all),
            ("metrica_tracking", b_all),
            ("idsse_tracking", c_all),
        ]:
            if not source_ids:
                continue
            existing: set[str] = set()
            try:
                ex_rows = (
                    spark.table(results_table)
                    .filter(f"data_source = '{source}'")
                    .select("match_id")
                    .distinct()
                    .collect()
                )
                existing = {str(r["match_id"]) for r in ex_rows}
            except Exception:
                _guard_logger.debug("Cannot read %s for source %s", results_table, source)
            total_new += len(source_ids - existing)

        if total_new == 0:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(workflow_id=self.workflow_id, count=total_new)


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
    ctx=None,
) -> None:
    """Execute the line-breaking detection pipeline."""
    params = LineBreakingParams()

    path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params)
    path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params)
    path_c_rows = _process_idsse_tracking(spark, catalog, schema, logger, params)

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

    logger.info("Starting line-breaking pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
