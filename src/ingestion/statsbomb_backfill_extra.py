"""Guard and entry point for StatsBomb _raw_extra_json backfill.

Separated from ``statsbomb.py`` so it has its own ``skip_guard`` entry
in ``_GUARD_MODULES`` and can be gated independently by the freshness gate.

The guard finds matches where ``_raw_extra_json IS NULL`` and returns
their IDs in metadata for targeted backfill.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _BackfillExtraGuard:
    workflow_id = "wf-backfill-extra"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Find matches needing _raw_extra_json backfill.

        Uses ``IS NULL`` only — events with ``'{}'`` are legitimately
        backfilled (no type-specific data). Prevents infinite re-runs.
        """
        table = f"{catalog}.{schema}.statsbomb_events"
        import logging as _logging

        from ingestion.utils import tolerate_missing_table

        _guard_logger = _logging.getLogger(__name__)

        match_ids: list[str] = []
        table_present = False
        with tolerate_missing_table(_guard_logger, f"Table {table} missing — assume work needed"):
            rows = spark.table(table).filter("_raw_extra_json IS NULL").select("match_id").distinct().collect()
            match_ids = sorted({str(row["match_id"]) for row in rows})
            table_present = True

        if not table_present:
            # Table may not exist on first run — assume work needed.
            return FilterResult(workflow_id=self.workflow_id, count=1)

        if not match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(match_ids),
            metadata={"new_match_ids": match_ids},
        )


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
) -> int:
    """Backfill _raw_extra_json on existing StatsBomb events."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No events need _raw_extra_json backfill")

    from ingestion.statsbomb import backfill_extra_json, ingest_competitions

    competitions_pdf = ingest_competitions(spark, catalog, schema, logger)
    guard_match_ids = filter_result.metadata.get("new_match_ids") if filter_result.metadata else None
    backfill_extra_json(spark, catalog, schema, competitions_pdf, logger, match_ids=guard_match_ids)
    return 0


def main() -> None:
    """CLI entry point for backfilling _raw_extra_json on existing StatsBomb events."""
    args = parse_ingestion_args("Backfill _raw_extra_json for existing StatsBomb events")
    logger = configure_logging("statsbomb_extra_backfill")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting _raw_extra_json backfill into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("_raw_extra_json backfill complete")
