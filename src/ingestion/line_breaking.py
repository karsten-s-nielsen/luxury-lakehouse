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
from ingestion.line_breaking_360 import _make_statsbomb_udf as _make_statsbomb_udf
from ingestion.line_breaking_360 import _process_statsbomb_360
from ingestion.line_breaking_common import _RESULT_COLUMNS as _RESULT_COLUMNS
from ingestion.line_breaking_tracking import _make_idsse_udf as _make_idsse_udf
from ingestion.line_breaking_tracking import _make_metrica_udf as _make_metrica_udf
from ingestion.line_breaking_tracking import _process_idsse_tracking, _process_metrica_tracking
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


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
