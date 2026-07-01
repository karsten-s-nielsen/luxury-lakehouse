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
from datetime import datetime, timezone
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


def _select_matches_to_ingest(
    all_matches: list[MatchInfo],
    *,
    ingested_ids: set[str],
    watermark: datetime | None,
    max_matches: int | None,
) -> list[MatchInfo]:
    """Pure discovery core (unit-tested without Spark) — which matches to ingest.

    A match is wanted if it is **MISSING** (``id`` not in ``ingested_ids`` — never
    ingested, regardless of ``updated_at``: the "ingest anything missing" contract)
    OR **MODIFIED** (already ingested but ``updated_at`` is newer than ``watermark``,
    i.e. an upstream re-issue). The result is deterministically capped to
    ``max_matches`` — sorted by ``(date, id)`` before truncation so the capped set is
    reproducible and "next N" walks forward across triggers (the anti-join already
    excludes ingested matches). ``max_matches is None`` => no cap, order preserved
    (the scheduled-run path is byte-for-byte unchanged).
    """

    def _wanted(m: MatchInfo) -> bool:
        if m.id not in ingested_ids:
            return True  # MISSING — never ingested (any updated_at)
        if watermark is not None:
            mu = m.updated_at if m.updated_at.tzinfo else m.updated_at.replace(tzinfo=timezone.utc)
            return mu > watermark  # MODIFIED — upstream re-issue since our last ingest
        return False

    wanted = [m for m in all_matches if _wanted(m)]
    if max_matches is None:
        return wanted
    return sorted(wanted, key=lambda m: (m.date, m.id))[:max_matches]


class _SkillcornerGuard:
    workflow_id = "wf-skillcorner"

    def __init__(self, *, max_matches: int | None = None) -> None:
        """Optional cap for a phased / one-off ingest (e.g. the RM-5 probe).

        ``max_matches`` — ingest at most ``N`` of the discovered matches this run
        (``None`` = ingest every missing/modified match, the scheduled-run default).
        Mirrors the action-context ``max_units`` seam. Deterministic + walks forward
        across triggers because discovery is a missing-anti-join (see ``check``).
        """
        self.max_matches = max_matches

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Discover SkillCorner matches to ingest: anything MISSING or MODIFIED.

        Full discovery + a client-side anti-join on ``bronze.skillcorner_matches``:
        a match is ingested if it is (a) MISSING — its ``match_id`` is not yet in
        bronze, regardless of ``updated_at`` (the "ingest anything missing" contract,
        which the prior modified-since-only guard did NOT honour — a never-ingested
        match with an old ``updated_at`` was silently skipped), OR (b) MODIFIED —
        upstream ``updated_at`` is newer than our last ingest watermark
        (``MAX(_ingested_at)``), catching upstream re-issues of an ingested match.

        The result is capped by ``max_matches`` (deterministic; see
        ``_select_matches_to_ingest``). Missing tables => first run => ingest everything.
        """
        import logging as _logging

        _guard_logger = _logging.getLogger(__name__)
        token = resolve_pining_token()

        # Full discovery — every match the owner token can see (public + private).
        all_matches = fetch_match_list(token, updated_since=None)
        if not all_matches:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        # Already-ingested ids (anti-join source) + last-ingest watermark (modified test).
        ingested_ids: set[str] = set()
        watermark = None
        with tolerate_missing_table(_guard_logger, "SkillCorner matches table missing -- full ingestion needed"):
            from pyspark.sql import functions as spark_fn

            ids_row = (
                spark.table(f"{catalog}.bronze.skillcorner_matches")
                .select(spark_fn.collect_set("match_id").alias("ids"))
                .collect()[0]
            )
            ingested_ids = {str(x) for x in (ids_row["ids"] or [])}
        with tolerate_missing_table(_guard_logger, "SkillCorner events table missing -- full ingestion needed"):
            from pyspark.sql import functions as spark_fn

            ts_row = (
                spark.table(f"{catalog}.bronze.skillcorner_events")
                .select(spark_fn.max("_ingested_at").alias("max_ts"))
                .collect()[0]
            )
            if ts_row["max_ts"] is not None:
                watermark = ts_row["max_ts"]
                if watermark.tzinfo is None:
                    watermark = watermark.replace(tzinfo=timezone.utc)

        to_ingest = _select_matches_to_ingest(
            all_matches,
            ingested_ids=ingested_ids,
            watermark=watermark,
            max_matches=self.max_matches,
        )
        if not to_ingest:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(to_ingest),
            metadata={"matches": [m.model_dump() for m in to_ingest]},
        )


skip_guard = _SkillcornerGuard()


def _parse_max_matches(raw: str | None) -> int | None:
    """Coerce the optional ``--max-matches`` CLI arg to ``int | None``.

    Arrives as a string: the daily job passes an empty job-parameter value, so
    ``""`` (and whitespace) coerces to ``None`` = ingest all missing, leaving the
    scheduled run unchanged. Mirrors action-context's ``_parse_preflight_filters``.
    Raises ``SystemExit`` on a non-positive / non-integer value.
    """
    val = raw.strip() if raw and raw.strip() else None
    if val is None:
        return None
    try:
        n = int(val)
    except ValueError:
        raise SystemExit(f"--max-matches must be a positive integer, got {raw!r}") from None
    if n <= 0:
        raise SystemExit(f"--max-matches must be > 0, got {n}")
    return n


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
        match_df = parse_match_json(match_resp.text, match_id=mid, visibility=match.visibility)
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
    args = parse_ingestion_args(
        "Ingest SkillCorner data into the bronze layer",
        extra_args=[
            (
                "--max-matches",
                {
                    "default": "",
                    "help": (
                        "Cap the number of matches ingested this run (phased rollout — e.g. the RM-5 "
                        "private-match probe). Empty (the daily-job default) = ingest all missing/modified."
                    ),
                },
            ),
        ],
    )
    logger = configure_logging("skillcorner")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    max_matches = _parse_max_matches(getattr(args, "max_matches", None))
    skillcorner_guard = _SkillcornerGuard(max_matches=max_matches)
    filter_result = timed_check(skillcorner_guard, spark, args.catalog, args.schema)

    logger.info(
        "Starting SkillCorner ingestion into %s.%s (max_matches=%s)",
        args.catalog,
        args.schema,
        max_matches if max_matches is not None else "all",
    )
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("SkillCorner ingestion complete")


if __name__ == "__main__":
    main()
