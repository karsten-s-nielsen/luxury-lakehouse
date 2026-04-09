"""Guard and entry point for StatsBomb _raw_extra_json backfill.

Separated from ``statsbomb.py`` so it has its own ``skip_guard`` entry
in ``_GUARD_MODULES`` and can be gated independently by the freshness gate.

The backfill scans the events table for rows where ``_raw_extra_json`` is
NULL or empty, which previously took 19 minutes even when there was no work.
The guard short-circuits with a ``LIMIT 1`` existence check.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, read_gate_result
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _BackfillExtraGuard:
    workflow_id = "wf-backfill-extra"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip if no events need _raw_extra_json backfill.

        Uses ``LIMIT 1`` for an O(1) existence check — avoids a full
        table scan when there is no work (the 19-minute bottleneck).
        """
        table = f"{catalog}.{schema}.statsbomb_events"
        try:
            needs_backfill = (
                spark.table(table).filter("_raw_extra_json IS NULL OR _raw_extra_json = '{}'").limit(1).count()
            )
            return FilterResult(workflow_id=self.workflow_id, count=needs_backfill)
        except Exception:
            # Table may not exist — assume work needed
            return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _BackfillExtraGuard()


@workflow("wf-backfill-extra", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> None:
    """Backfill _raw_extra_json on existing StatsBomb events."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No events need _raw_extra_json backfill")

    from ingestion.statsbomb import backfill_extra_json, ingest_competitions

    competitions_pdf = ingest_competitions(spark, catalog, schema, logger)
    backfill_extra_json(spark, catalog, schema, competitions_pdf, logger)


def main() -> None:
    """CLI entry point for backfilling _raw_extra_json on existing StatsBomb events."""
    args = parse_ingestion_args("Backfill _raw_extra_json for existing StatsBomb events")
    logger = configure_logging("statsbomb_extra_backfill")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = read_gate_result("wf-backfill-extra")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    logger.info("Starting _raw_extra_json backfill into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("_raw_extra_json backfill complete")
