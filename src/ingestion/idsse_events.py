"""Guard and entry point for IDSSE event data ingestion.

Separated from ``idsse.py`` so it has its own ``skip_guard`` entry
in ``_GUARD_MODULES`` and can be gated independently by the freshness gate.

The ``wf-idsse`` guard covers tracking data; this module covers event
ingestion (DFL_03_02 series XML files) as a separate workflow.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.idsse import IDSSE_MATCH_IDS
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _IdsseEventsGuard:
    workflow_id = "wf-idsse-events"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip if all 7 IDSSE matches already have event data."""
        expected = len(IDSSE_MATCH_IDS)
        try:
            e_count = spark.table(f"{catalog}.{schema}.idsse_events").select("match_id").distinct().count()
            if e_count >= expected:
                return FilterResult(workflow_id=self.workflow_id, count=0)
        except Exception:  # noqa: S110
            pass
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _IdsseEventsGuard()


@workflow("wf-idsse-events", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest IDSSE event data into the bronze layer."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new IDSSE event data to ingest")

    from ingestion.idsse import ingest_idsse_events

    ingest_idsse_events(spark, catalog, schema, logger)
    return 0


def main() -> None:
    """CLI entry point for IDSSE event data ingestion."""
    args = parse_ingestion_args("Ingest IDSSE Bundesliga event data into the bronze layer")
    logger = configure_logging("idsse_events")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting IDSSE event ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("IDSSE event ingestion complete")
