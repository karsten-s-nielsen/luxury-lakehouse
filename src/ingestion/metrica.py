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
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult
from ingestion.metrica_events import ingest_events
from ingestion.metrica_tracking import ingest_tracking
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _MetricaGuard:
    workflow_id = "wf-metrica"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _MetricaGuard()


@workflow("wf-metrica", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx: object = None,
) -> None:
    """Ingest all Metrica Sports sample data (tracking + events)."""
    ingest_tracking(spark, catalog, schema, logger)
    ingest_events(spark, catalog, schema, logger)


def main() -> None:
    """CLI entry point for Metrica Sports ingestion."""
    args = parse_ingestion_args("Ingest Metrica Sports sample data into the bronze layer")
    logger = configure_logging("metrica")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    logger.info("Starting Metrica ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)
    logger.info("Metrica ingestion complete")


if __name__ == "__main__":
    main()
