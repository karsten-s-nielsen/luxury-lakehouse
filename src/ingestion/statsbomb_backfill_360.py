"""Guard and entry point for StatsBomb 360 freeze-frame backfill.

Separated from ``statsbomb.py`` so it has its own ``skip_guard`` entry
in ``_GUARD_MODULES`` and can be gated independently by the freshness gate.

The backfill targets matches that have events but no 360 data yet,
enabling catchup after the 360 ingestion code was added.
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


class _Backfill360Guard:
    workflow_id = "wf-backfill-360"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip if all event matches already have 360 data.

        Compares distinct match_ids between statsbomb_events and
        statsbomb_360 — returns IDs of matches that need backfill.
        """
        try:
            event_ids = {
                str(row["match_id"])
                for row in spark.table(f"{catalog}.{schema}.statsbomb_events").select("match_id").distinct().collect()
            }
            try:
                three60_ids = {
                    str(row["match_id"])
                    for row in spark.table(f"{catalog}.{schema}.statsbomb_360").select("match_id").distinct().collect()
                }
            except Exception:
                # 360 table doesn't exist — all event matches need backfill
                return FilterResult(
                    workflow_id=self.workflow_id,
                    count=len(event_ids),
                    metadata={"new_match_ids": sorted(event_ids)},
                )

            missing = sorted(event_ids - three60_ids)
            if not missing:
                return FilterResult(workflow_id=self.workflow_id, count=0)
            return FilterResult(
                workflow_id=self.workflow_id,
                count=len(missing),
                metadata={"new_match_ids": missing},
            )
        except Exception:
            # Events table doesn't exist — nothing to backfill
            return FilterResult(workflow_id=self.workflow_id, count=0)


skip_guard = _Backfill360Guard()


@workflow("wf-backfill-360", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> None:
    """Backfill 360 freeze-frame data for matches already ingested."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No matches need 360 backfill")

    from ingestion.statsbomb import backfill_360, ingest_competitions

    competitions_pdf = ingest_competitions(spark, catalog, schema, logger)
    match_ids = filter_result.metadata.get("new_match_ids")
    backfill_360(spark, catalog, schema, competitions_pdf, logger, match_ids=match_ids)


def main() -> None:
    """CLI entry point for backfilling StatsBomb 360 freeze-frame data."""
    args = parse_ingestion_args("Backfill StatsBomb 360 freeze-frame data for existing matches")
    logger = configure_logging("statsbomb_360_backfill")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = read_gate_result("wf-backfill-360")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    logger.info("Starting 360 backfill into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("360 backfill complete")
