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
    write_task_value,
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

    Per-match order: parse both artifacts, then write tracking first,
    events last.  The skip guard watermark lives on events._ingested_at,
    so events must be the final commit.
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

        # --- Phase 1: Download & parse both artifacts (no writes yet) ---
        # Parse everything first so a failure in either artifact prevents
        # partial writes. The guard watermark (MAX _ingested_at on events)
        # only advances when BOTH artifacts are committed.

        events_df = None
        for artifact_key in match.artifacts:
            if "event" in artifact_key.lower():
                events_resp = fetch_artifact(mid, artifact_key, token)
                events_df = parse_events(events_resp.text, match_id=mid)
                logger.info("Parsed %d event rows for match %s", len(events_df), mid)
                break
        else:
            logger.warning("No event artifact found for match %s", mid)

        tracking_df = None
        for artifact_key in match.artifacts:
            if "track" in artifact_key.lower():
                tracking_resp = fetch_artifact(mid, artifact_key, token, stream=True)
                tracking_bytes = tracking_resp.content
                tracking_df = parse_tracking(tracking_bytes, match_id=mid)
                logger.info("Parsed %d tracking rows for match %s", len(tracking_df), mid)
                del tracking_bytes
                break
        else:
            logger.warning("No tracking artifact found for match %s", mid)

        # --- Phase 2: Write tracking FIRST, events LAST ---
        # The skip guard reads MAX(_ingested_at) from the EVENTS table to
        # decide which matches need processing. Events must be the last
        # write so the watermark only advances when both artifacts are
        # committed. If tracking succeeds but events fails, the watermark
        # stays put and the match is re-discovered on the next run.
        if tracking_df is not None:
            write_tracking(spark, tracking_df, catalog, schema, mid, logger)
            logger.info("Wrote %d tracking rows for match %s", len(tracking_df), mid)
            del tracking_df

        if events_df is not None:
            write_events(spark, events_df, catalog, schema, mid, logger)
            logger.info("Wrote %d event rows for match %s", len(events_df), mid)
            del events_df

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
    """CLI entry point for Gradient Sports data ingestion.

    Two modes:
        - ``--match-json <JSON>``: Single-match mode (for_each_task iteration).
          Deserializes the JSON to MatchInfo and ingests that one match.
        - No ``--match-json``: Legacy standalone mode. Runs the guard and
          ingests all discovered matches sequentially. Kept for manual CLI
          usage and backward compatibility.
    """
    args = parse_ingestion_args(
        "Ingest Gradient Sports data into the bronze layer",
        extra_args=[
            (
                "--match-json",
                {
                    "type": str,
                    "default": None,
                    "help": (
                        "JSON-serialized MatchInfo for single-match iteration mode. "
                        "Used by the Terraform for_each_task fan-out — each iteration "
                        "receives one match via {{input}}. Omit to run guard + full "
                        "sequential ingestion."
                    ),
                },
            ),
        ],
    )
    _logger = configure_logging("gradientsports")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    if args.match_json is not None:
        # Single-match mode: for_each_task iteration
        match = MatchInfo.model_validate_json(args.match_json)
        _logger.info("Single-match mode: ingesting match %s (%s vs %s)", match.id, match.home, match.away)
        ingest_gradientsports(spark, args.catalog, args.schema, _logger, [match])
    else:
        # Legacy standalone mode: guard + sequential ingestion
        filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)
        _logger.info("Starting Gradient Sports ingestion into %s.%s", args.catalog, args.schema)
        run_pipeline(spark, args.catalog, args.schema, _logger, filter_result=filter_result)

    _logger.info("Gradient Sports ingestion complete")


def main_preflight() -> None:
    """CLI entry point for Gradient Sports preflight task.

    Runs the skip guard to discover matches, serializes each MatchInfo
    as a JSON string, and emits the list as a Databricks task value for
    downstream for_each_task consumption.

    Behavior:
        - N matches found -> emits N-element JSON array
        - 0 matches found -> emits [] (for_each_task spawns 0 iterations)
    """
    args = parse_ingestion_args(
        "Preflight: discover Gradient Sports matches and emit "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    _logger = configure_logging("gradientsports_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    if filter_result.count == 0:
        _logger.info("No new Gradient Sports matches -- emitting empty task value")
        write_task_value("gradientsports_matches", [], _logger)
        return

    raw_matches = filter_result.metadata.get("matches", [])  # type: ignore[union-attr]
    matches = [MatchInfo.model_validate(m) for m in raw_matches]

    # Serialize each MatchInfo as a JSON string for {{input}} consumption.
    # Uses model_dump_json() — NOT json.dumps(model_dump()) which crashes on datetime.
    match_jsons = [m.model_dump_json() for m in matches]

    _logger.info(
        "Gradient Sports preflight: %d matches discovered, emitting task value",
        len(match_jsons),
    )
    write_task_value("gradientsports_matches", match_jsons, _logger)


if __name__ == "__main__":
    main()
