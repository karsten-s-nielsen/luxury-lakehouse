"""Gradient Sports ingestion orchestrator.

Discovers matches via the pining-for-the-data REST API,
downloads events + tracking, and writes to bronze.

Bronze tables produced:
  - gradientsports_events   (raw events)
  - gradientsports_tracking (narrow format: one row per player per frame)

Coordinate system (preserved in bronze):
  Center-origin meters. silly-kicks convert_to_frames handles the final transform.

LICENSE GATE: Data approved for internal calibration/training only.
NOT published to HF datasets, gold marts, synced tables, or Taipy UI
until Gradient Sports license confirmed in writing.
"""

from __future__ import annotations

import gc
import logging
from typing import TYPE_CHECKING

from ingestion.gradientsports_common import (
    MatchInfo,
    fetch_artifact,
    fetch_match_list,
    resolve_pining_token,
)
from ingestion.gradientsports_events import parse_events, write_events
from ingestion.gradientsports_tracking import parse_tracking, write_tracking
from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    tolerate_missing_table,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _GradientSportsGuard:
    workflow_id = "wf-gradientsports"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Discover new/modified Gradient Sports matches via API.

        Queries MAX(_ingested_at) from bronze.gradientsports_events to determine
        the updatedSince cutoff. Calls the discovery API. Returns match count.
        """
        import logging as _logging
        from datetime import datetime, timezone

        _guard_logger = _logging.getLogger(__name__)
        token = resolve_pining_token()

        updated_since: str | None = None
        with tolerate_missing_table(_guard_logger, "Gradient Sports events table missing -- full ingestion needed"):
            from pyspark.sql import functions as spark_fn

            row = (
                spark.table(f"{catalog}.bronze.gradientsports_events")
                .select(spark_fn.max("_ingested_at").alias("max_ts"))
                .collect()[0]
            )
            if row["max_ts"] is not None:
                ts: datetime = row["max_ts"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                updated_since = ts.strftime("%Y-%m-%dT%H:%M:%SZ")

        matches = fetch_match_list(token, updated_since=updated_since)
        if not matches:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(matches),
            metadata={"matches": [m.model_dump() for m in matches]},
        )


skip_guard = _GradientSportsGuard()


def ingest_gradientsports(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    matches: list[MatchInfo],
) -> None:
    """Download and ingest Gradient Sports data for discovered matches.

    Processing order per match: events -> tracking.
    """
    token = resolve_pining_token()

    for i, match in enumerate(matches):
        mid = match.id
        logger.info(
            "Processing Gradient Sports match %s (%d/%d): %s vs %s",
            mid,
            i + 1,
            len(matches),
            match.home,
            match.away,
        )

        # 1. Events
        for artifact_key in match.artifacts:
            if "event" in artifact_key.lower():
                events_resp = fetch_artifact(mid, artifact_key, token)
                events_df = parse_events(events_resp.text, match_id=mid)
                write_events(spark, events_df, catalog, schema, mid, logger)
                logger.info("Wrote %d event rows for match %s", len(events_df), mid)
                del events_df
                break
        else:
            logger.warning("No event artifact found for match %s", mid)

        # 2. Tracking
        for artifact_key in match.artifacts:
            if "track" in artifact_key.lower():
                tracking_resp = fetch_artifact(mid, artifact_key, token, stream=True)
                # Read full response — streaming not needed for data size
                tracking_data = tracking_resp.text
                tracking_df = parse_tracking(tracking_data, match_id=mid)
                write_tracking(spark, tracking_df, catalog, schema, mid, logger)
                logger.info("Wrote %d tracking rows for match %s", len(tracking_df), mid)
                del tracking_df, tracking_data
                break
        else:
            logger.warning("No tracking artifact found for match %s", mid)

        gc.collect()

    logger.info("Gradient Sports ingestion complete: %d matches processed", len(matches))


@workflow("wf-gradientsports", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest Gradient Sports match data from pining-for-the-data API."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    raw_matches = filter_result.metadata.get("matches", [])  # type: ignore[union-attr]
    matches = [MatchInfo.model_validate(m) for m in raw_matches]

    ingest_gradientsports(spark, catalog, schema, logger, matches)
    return 0


def main() -> None:
    """CLI entry point for Gradient Sports data ingestion."""
    args = parse_ingestion_args("Ingest Gradient Sports data into the bronze layer")
    _logger = configure_logging("gradientsports")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    _logger.info("Starting Gradient Sports ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, _logger, filter_result=filter_result)
    _logger.info("Gradient Sports ingestion complete")


if __name__ == "__main__":
    main()
