"""Cross-cutting derived-artifact staleness monitor (ADR-063 H4).

Detect-and-alert backstop, orthogonal to any auto-rebuild trigger: for every workflow that records a
Delta-version watermark in ``observability.workflow_watermarks``, compare the recorded watermark
against the upstream table's *current* data version. If a workflow's watermark lags its upstream, the
workflow's output is stale — emit an ERROR-level alert.

This is the safety net that would have caught the 2-month-stale xT grid (the negative-DZV root cause)
in week 1. It covers all tiers, including the deferred Tier B (expensive retrains) and the Tier C
per-id pipelines that otherwise rely on humans remembering a wipe checklist after a re-derivation.

ERROR-level logging is the alert sink per CLAUDE.md (warning-level telemetry is invisible to
error-log queries — the 2026-04-12 warm-tier blocker class).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_WATERMARKS_TABLE = "observability.workflow_watermarks"


@dataclass(frozen=True)
class StaleArtifact:
    """A workflow whose recorded watermark lags its upstream's current data version."""

    workflow_id: str
    upstream_table: str
    recorded_version: int
    current_version: int

    @property
    def lag(self) -> int:
        return self.current_version - self.recorded_version


def find_stale_artifacts(
    stored: list[tuple[str, str, int]],
    current_versions: Mapping[str, int | None],
) -> list[StaleArtifact]:
    """Pure detection (ADR-063 H4 — testable without Spark).

    Args:
        stored: ``(workflow_id, upstream_table, last_seen_version)`` rows from the watermarks table.
        current_versions: ``{upstream_table: latest_data_version_or_None}``.

    Returns the stale entries: a recorded watermark strictly BELOW the upstream's current data version
    (an upstream with no known current version — ``None`` — is skipped, never flagged).
    """
    stale: list[StaleArtifact] = []
    for workflow_id, upstream_table, recorded in stored:
        current = current_versions.get(upstream_table)
        if current is not None and recorded < current:
            stale.append(
                StaleArtifact(
                    workflow_id=workflow_id,
                    upstream_table=upstream_table,
                    recorded_version=recorded,
                    current_version=current,
                )
            )
    return stale


def run_monitor(spark: SparkSession, catalog: str, *, max_lag: int = 0) -> list[StaleArtifact]:
    """Scan the watermarks table; alert (ERROR log) on any artifact lagging by more than ``max_lag``.

    Returns the stale artifacts (also for test/automation). Never raises on a healthy scan.
    """
    from ingestion.guards import _get_latest_data_version

    watermarks = f"{catalog}.{_WATERMARKS_TABLE}"
    rows = spark.sql(
        f"SELECT workflow_id, upstream_table, last_seen_version FROM {watermarks}"  # noqa: S608
    ).collect()
    stored = [(str(r["workflow_id"]), str(r["upstream_table"]), int(r["last_seen_version"])) for r in rows]

    upstream_tables = sorted({t for _, t, _ in stored})
    current_versions: dict[str, int | None] = {}
    for table in upstream_tables:
        try:
            current_versions[table] = _get_latest_data_version(spark, table)
        except Exception:  # noqa: BLE001 — a single unreadable upstream must not abort the whole scan
            logger.warning("staleness-monitor: could not read history for %s — skipping", table)
            current_versions[table] = None

    stale = [s for s in find_stale_artifacts(stored, current_versions) if s.lag > max_lag]
    for s in stale:
        logger.error(
            "STALE derived artifact: workflow=%s is %d version(s) behind upstream %s "
            "(recorded v%d < current v%d) — ADR-063 H4",
            s.workflow_id,
            s.lag,
            s.upstream_table,
            s.recorded_version,
            s.current_version,
        )
    logger.info("staleness-monitor: scanned %d watermark rows, %d stale", len(stored), len(stale))
    return stale


def main() -> None:
    """CLI entry point — scan + alert."""
    from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args

    args = parse_ingestion_args("Cross-cutting derived-artifact staleness monitor (ADR-063)")
    log = configure_logging("staleness_monitor")
    spark = get_spark_session()
    stale = run_monitor(spark, args.catalog)
    log.info("staleness-monitor complete: %d stale artifact(s)", len(stale))


if __name__ == "__main__":
    main()
