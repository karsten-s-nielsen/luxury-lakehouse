"""Gradient Sports ingestion orchestrator.

Discovers matches via the pining-for-the-data REST API,
downloads events + tracking + metadata + roster, and writes to bronze.

Bronze tables produced:
  - gradientsports_events    (raw events)
  - gradientsports_tracking  (narrow format: one row per player per frame)
  - gradientsports_metadata  (match metadata: one row per match)
  - gradientsports_roster    (player roster: one row per player per match)

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
from ingestion.gradientsports_metadata import parse_metadata, write_metadata
from ingestion.gradientsports_roster import parse_roster, write_roster
from ingestion.gradientsports_tracking import (
    _staging_path,
    stream_tracking_to_parquet,
    write_tracking,
)
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
        """Discover Gradient Sports matches that need ingestion.

        Two-phase discovery:
          Phase A — find MISSING matches: fetch the full match list from the
          API (no updatedSince filter), then anti-join against distinct
          match_ids already in bronze.gradientsports_events. Any match_id
          present in the API but absent from bronze is scheduled for ingestion.

          Phase B — find UPDATED matches (only when Phase A found nothing):
          if all API matches are already in bronze, re-query the API with
          updatedSince = MAX(_ingested_at) from events to catch matches the
          provider re-processed after our last complete ingestion.

        This avoids the timestamp-domain mismatch that made the old guard
        useless: _ingested_at is when WE wrote; updatedSince filters on when
        the PROVIDER last modified the match.
        """
        import logging as _logging
        from datetime import datetime, timezone

        _guard_logger = _logging.getLogger(__name__)
        token = resolve_pining_token()

        # --- Phase A: fetch full match list, anti-join against bronze -------
        all_matches = fetch_match_list(token, updated_since=None)
        if not all_matches:
            _guard_logger.info("API returned 0 matches — nothing to ingest")
            return FilterResult(workflow_id=self.workflow_id, count=0)

        ingested_ids: set[str] = set()
        with tolerate_missing_table(_guard_logger, "Gradient Sports events table missing — full ingestion needed"):
            rows = spark.table(f"{catalog}.bronze.gradientsports_events").select("match_id").distinct().collect()
            ingested_ids = {str(r["match_id"]) for r in rows}

        missing = [m for m in all_matches if m.id not in ingested_ids]
        if missing:
            _guard_logger.info(
                "Phase A: %d of %d matches missing from bronze — scheduling ingestion",
                len(missing),
                len(all_matches),
            )
            return FilterResult(
                workflow_id=self.workflow_id,
                count=len(missing),
                metadata={"matches": [m.model_dump() for m in missing]},
            )

        # --- Phase B: all matches ingested — check for provider updates -----
        updated_since: str | None = None
        with tolerate_missing_table(_guard_logger, "Gradient Sports events table missing"):
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

        if updated_since is None:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        updated_matches = fetch_match_list(token, updated_since=updated_since)
        if not updated_matches:
            _guard_logger.info("Phase B: all %d matches ingested, no provider updates", len(all_matches))
            return FilterResult(workflow_id=self.workflow_id, count=0)

        _guard_logger.info(
            "Phase B: %d matches re-processed by provider since %s",
            len(updated_matches),
            updated_since,
        )
        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(updated_matches),
            metadata={"matches": [m.model_dump() for m in updated_matches]},
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

        # --- Phase 1: Download & parse all artifacts (no writes yet) ---
        events_df = None
        for artifact_key in match.artifacts:
            if "event" in artifact_key.lower():
                events_resp = fetch_artifact(mid, artifact_key, token)
                events_df = parse_events(events_resp.text, match_id=mid)
                logger.info("Parsed %d event rows for match %s", len(events_df), mid)
                break
        else:
            logger.warning("No event artifact found for match %s", mid)

        tracking_staged = False
        tracking_row_count = 0
        staging_path = _staging_path(catalog, schema, mid)
        for artifact_key in match.artifacts:
            if "track" in artifact_key.lower():
                tracking_resp = fetch_artifact(mid, artifact_key, token, stream=True)
                from ingestion.utils import ensure_volume_directory

                ensure_volume_directory(staging_path.rsplit("/", 1)[0])
                tracking_row_count = stream_tracking_to_parquet(
                    tracking_resp,
                    match_id=mid,
                    parquet_path=staging_path,
                    log=logger,
                )
                tracking_staged = True
                break
        else:
            logger.warning("No tracking artifact found for match %s", mid)

        metadata_df = None
        for artifact_key in match.artifacts:
            if "metadata" in artifact_key.lower():
                metadata_resp = fetch_artifact(mid, artifact_key, token)
                metadata_df = parse_metadata(metadata_resp.text, match_id=mid, visibility=match.visibility)
                logger.info("Parsed metadata for match %s", mid)
                break

        roster_df = None
        for artifact_key in match.artifacts:
            if "roster" in artifact_key.lower():
                roster_resp = fetch_artifact(mid, artifact_key, token)
                roster_df = parse_roster(roster_resp.text, match_id=mid)
                logger.info("Parsed %d roster rows for match %s", len(roster_df), mid)
                break

        # --- Phase 2: Write tracking -> metadata -> roster -> events ---
        if tracking_staged:
            write_tracking(spark, catalog, schema, mid, logger, staging_parquet=staging_path)
            logger.info("Wrote %d tracking rows for match %s", tracking_row_count, mid)

        if metadata_df is not None:
            write_metadata(spark, metadata_df, catalog, schema, mid, logger)
            logger.info("Wrote metadata for match %s", mid)

        if roster_df is not None:
            write_roster(spark, roster_df, catalog, schema, mid, logger)
            logger.info("Wrote %d roster rows for match %s", len(roster_df), mid)

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


def _backfill_artifacts(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Backfill metadata + roster artifacts for matches already in bronze.

    Skips the guard entirely. Reads the match ID list from existing
    bronze.gradientsports_events, fetches metadata + roster from the API
    (NOT events or tracking), and writes to the new bronze tables.
    """
    token = resolve_pining_token()

    rows = spark.sql(
        f"SELECT DISTINCT match_id FROM {catalog}.{schema}.gradientsports_events"  # noqa: S608 — catalog/schema are validated identifiers
    ).collect()
    match_ids = [str(r["match_id"]) for r in rows]
    logger.info("Backfill: %d matches to process", len(match_ids))

    all_matches = fetch_match_list(token, updated_since=None)
    match_map = {m.id: m for m in all_matches}

    for i, mid in enumerate(sorted(match_ids)):
        match = match_map.get(mid)
        if match is None:
            logger.warning("Backfill: match %s not in API match list — skipping", mid)
            continue

        logger.info("Backfill match %s (%d/%d)", mid, i + 1, len(match_ids))

        for artifact_key in match.artifacts:
            if "metadata" in artifact_key.lower():
                resp = fetch_artifact(mid, artifact_key, token)
                df = parse_metadata(resp.text, match_id=mid, visibility=match.visibility)
                write_metadata(spark, df, catalog, schema, mid, logger)
                logger.info("Wrote metadata for match %s", mid)
                break

        for artifact_key in match.artifacts:
            if "roster" in artifact_key.lower():
                resp = fetch_artifact(mid, artifact_key, token)
                df = parse_roster(resp.text, match_id=mid)
                write_roster(spark, df, catalog, schema, mid, logger)
                logger.info("Wrote %d roster rows for match %s", len(df), mid)
                break

    logger.info("Backfill complete: %d matches processed", len(match_ids))


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
            (
                "--backfill-artifacts",
                {
                    "action": "store_true",
                    "default": False,
                    "help": (
                        "Backfill metadata + roster artifacts for matches already in "
                        "bronze. Skips the guard, fetches only metadata + roster."
                    ),
                },
            ),
        ],
    )
    _logger = configure_logging("gradientsports")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    if args.backfill_artifacts:
        _logger.info("Backfill mode: ingesting metadata + roster for existing matches")
        _backfill_artifacts(spark, args.catalog, args.schema, _logger)
        return

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
