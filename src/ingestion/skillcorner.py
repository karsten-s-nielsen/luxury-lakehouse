"""SkillCorner A-League ingestion orchestrator.

Discovers new/modified matches via the pining-for-the-data REST API,
downloads events + tracking + match metadata, and writes to bronze.

Bronze tables produced:
  - skillcorner_matches  (roster format: one row per player per match)
  - skillcorner_events   (raw dynamic_events CSV, all 294+ source columns)
  - skillcorner_tracking (narrow format: one row per player per frame)

Coordinate system (preserved in bronze):
  SkillCorner center-origin meters. Staging transforms to 120x80.
"""

from __future__ import annotations

import gc
import io
import logging
import os
import tempfile
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.skillcorner_common import MatchInfo, fetch_artifact, fetch_match_list, resolve_pining_token
from ingestion.skillcorner_events import parse_events_csv, write_events
from ingestion.skillcorner_matches import parse_match_json, write_matches
from ingestion.skillcorner_tracking import parse_tracking_jsonl, write_tracking
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


class _SkillcornerGuard:
    workflow_id = "wf-skillcorner"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Discover new/modified SkillCorner matches via API.

        Queries MAX(_ingested_at) from bronze.skillcorner_events to determine
        the updatedSince cutoff. Calls the discovery API. Returns match count.
        """
        import logging as _logging
        from datetime import datetime, timezone

        _guard_logger = _logging.getLogger(__name__)
        token = resolve_pining_token()

        # Determine last ingested timestamp
        updated_since: str | None = None
        with tolerate_missing_table(_guard_logger, "SkillCorner events table missing -- full ingestion needed"):
            from pyspark.sql import functions as spark_fn

            row = (
                spark.table(f"{catalog}.bronze.skillcorner_events")
                .select(spark_fn.max("_ingested_at").alias("max_ts"))
                .collect()[0]
            )
            if row["max_ts"] is not None:
                ts: datetime = row["max_ts"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                updated_since = ts.isoformat()

        matches = fetch_match_list(token, updated_since=updated_since)
        if not matches:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(matches),
            metadata={"matches": [m.model_dump() for m in matches]},
        )


skip_guard = _SkillcornerGuard()


def ingest_skillcorner(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    matches: list[MatchInfo],
) -> None:
    """Download and ingest SkillCorner data for discovered matches.

    Processing order per match: matches -> events -> tracking.
    Matches first because downstream SPADL needs roster data.
    """
    token = resolve_pining_token()

    for i, match in enumerate(matches):
        mid = match.id
        logger.info(
            "Processing SkillCorner match %s (%d/%d): %s vs %s",
            mid,
            i + 1,
            len(matches),
            match.home,
            match.away,
        )

        # 1. Match metadata (needed by SPADL conversion)
        match_resp = fetch_artifact(mid, f"{mid}_match", token)
        match_df = parse_match_json(match_resp.text, match_id=mid)
        write_matches(spark, match_df, catalog, schema, mid, logger)
        logger.info("Wrote %d roster rows for match %s", len(match_df), mid)

        # 2. Events (needed by SPADL conversion)
        events_resp = fetch_artifact(mid, f"{mid}_dynamic_events", token)
        events_df = parse_events_csv(io.StringIO(events_resp.text), match_id=mid)
        write_events(spark, events_df, catalog, schema, mid, logger)
        logger.info("Wrote %d event rows for match %s", len(events_df), mid)

        # 3. Tracking (JSONL -- stream to temp file to avoid holding full response in memory)
        tracking_resp = fetch_artifact(mid, f"{mid}_tracking_extrapolated", token, stream=True)
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".jsonl", delete=False) as tmp:
            for chunk in tracking_resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        try:
            tracking_df = parse_tracking_jsonl(tmp_path, match_id=mid)
            write_tracking(spark, tracking_df, catalog, schema, mid, logger)
            logger.info("Wrote %d tracking rows for match %s", len(tracking_df), mid)
        finally:
            os.unlink(tmp_path)

        del match_df, events_df, tracking_df
        gc.collect()

    logger.info("SkillCorner ingestion complete: %d matches processed", len(matches))


@workflow("wf-skillcorner", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest SkillCorner A-League match data from pining-for-the-data API."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    # Reconstruct MatchInfo objects from guard metadata
    raw_matches = filter_result.metadata.get("matches", [])  # type: ignore[union-attr]
    matches = [MatchInfo.model_validate(m) for m in raw_matches]

    ingest_skillcorner(spark, catalog, schema, logger, matches)
    return 0


def main() -> None:
    """CLI entry point for SkillCorner data ingestion."""
    args = parse_ingestion_args("Ingest SkillCorner A-League data into the bronze layer")
    logger = configure_logging("skillcorner")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting SkillCorner ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("SkillCorner ingestion complete")


if __name__ == "__main__":
    main()
