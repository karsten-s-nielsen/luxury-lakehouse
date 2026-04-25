"""Metrica Sports sample data ingestion into the Databricks bronze layer.

Downloads tracking and event data for 3 sample games from the Metrica
Sports open-data GitHub repository (HTTPS).

Games 1-2: CSV format with 3-row multi-line header (team names, jersey
numbers, column names). Parsed with ``csv.reader`` + ``pd.read_csv``.

Game 3: FIFA EPTS format with XML metadata (player roster, frame layout,
substitution sections), colon-delimited tracking, and JSON events.

Schema reshape (tracking):
  Wide/EPTS format -> narrow JSON format:
  ``period, frame, timestamp, ball_x, ball_y, match_id,
    home_players (JSON dict), away_players (JSON dict)``

Bronze tables produced:
  - metrica_tracking
  - metrica_events

Implementation split:
  - ``metrica_common`` — shared constants, EPTS parsers, utilities
  - ``metrica_tracking`` — CSV header parsing, wide-to-narrow reshape, tracking ingestion
  - ``metrica_events`` — event CSV parsing, event ingestion

Sample-vs-subscription contract (PR 5a, ADR-011 §4):
  The bronze tables carry an ``is_anonymized`` BOOLEAN column. The sample-CSV
  path implemented here writes ``is_anonymized = True`` for every row
  (players are anonymised "PlayerNN" strings; no real team/player identity).
  A future subscription-API ingestion path would set ``is_anonymized = False``
  and pass real IDs through to ``stg_metrica__team_players`` →
  ``dim_teams`` / ``dim_players``, which branch on the flag to pick
  synthesis vs real-identity columns. Do not remove this flag without first
  updating the dim synthesis branches.
  See ``docs/superpowers/specs/2026-04-24-kimball-pr5-design.md`` §4.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult, timed_check
from ingestion.metrica_events import ingest_events
from ingestion.metrica_tracking import ingest_tracking
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _MetricaGuard:
    workflow_id = "wf-metrica"
    _EXPECTED_MATCH_COUNT = 3  # Metrica sample-data: 3 games (static, never grows)

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip if all 3 Metrica sample games are already ingested."""
        import logging as _logging

        from ingestion.utils import tolerate_missing_table

        _guard_logger = _logging.getLogger(__name__)
        with tolerate_missing_table(_guard_logger, "Metrica tables missing — needs ingestion"):
            t_count = spark.table(f"{catalog}.{schema}.metrica_tracking").select("match_id").distinct().count()
            e_count = spark.table(f"{catalog}.{schema}.metrica_events").select("match_id").distinct().count()
            if t_count >= self._EXPECTED_MATCH_COUNT and e_count >= self._EXPECTED_MATCH_COUNT:
                return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _MetricaGuard()


@workflow("wf-metrica", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest all Metrica Sports sample data (tracking + events)."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    ingest_tracking(spark, catalog, schema, logger)
    ingest_events(spark, catalog, schema, logger)
    return 0


def main() -> None:
    """CLI entry point for Metrica Sports ingestion."""
    args = parse_ingestion_args("Ingest Metrica Sports sample data into the bronze layer")
    logger = configure_logging("metrica")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting Metrica ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("Metrica ingestion complete")


if __name__ == "__main__":
    main()
