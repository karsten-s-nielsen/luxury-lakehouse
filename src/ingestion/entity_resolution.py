"""Cross-source player entity resolution batch pipeline.

Reads player metadata from StatsBomb lineups and Wyscout players bronze
tables, runs the three-layer hybrid matching pipeline (TF-IDF + rapidfuzz
+ bidirectional), and writes results to ``player_xref_raw`` bronze table.

Bronze table produced:
  - player_xref_raw
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import FilterResult
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession

_guard_logger = logging.getLogger(f"{__name__}.guard")


class _EntityResolutionGuard:
    """SkipGuard adapter for entity resolution pipeline."""

    workflow_id = "wf-entity-resolution"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check if entity resolution needs to run.

        Finds StatsBomb player IDs that lack cross-reference entries.
        """
        from ingestion.guards import find_new_ids

        xref_table = f"{catalog}.{schema}.player_xref_raw"
        lineups_table = f"{catalog}.{schema}.statsbomb_lineups"

        try:
            new_player_ids = find_new_ids(
                spark,
                source_table=lineups_table,
                results_table=xref_table,
                id_column="player_id",
            )
        except Exception:
            _guard_logger.debug("Cannot check %s — needs resolution", xref_table)
            return FilterResult(workflow_id=self.workflow_id, count=1)

        if not new_player_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_player_ids),
            metadata={"new_player_ids": new_player_ids},
        )


skip_guard = _EntityResolutionGuard()


def _load_statsbomb_players(spark: SparkSession, catalog: str, schema: str) -> pd.DataFrame:
    """Load StatsBomb player metadata from lineups bronze table.

    Extracts jersey_number and team_name for Layer 1 team-scoped matching.
    Deduplicates by player_id, keeping the most recent match's metadata.
    """
    df = spark.sql(f"""
        WITH ranked AS (
            SELECT
                CAST(player_id AS INT) AS player_id,
                player_name,
                player_nickname,
                CAST(jersey_number AS STRING) AS jersey_number,
                team_name,
                get(
                    from_json(positions, 'ARRAY<STRUCT<position:STRING>>'),
                    0
                ).position AS position,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id ORDER BY match_id DESC
                ) AS rn
            FROM {catalog}.{schema}.statsbomb_lineups
            WHERE player_id IS NOT NULL
        )
        SELECT player_id, player_name, player_nickname,
               jersey_number, team_name, position
        FROM ranked WHERE rn = 1
    """).toPandas()  # noqa: S608
    # Use nickname as alternate name signal
    df["player_name"] = df["player_name"].fillna(df["player_nickname"])
    return df[["player_id", "player_name", "position", "jersey_number", "team_name"]]


def _load_wyscout_players(spark: SparkSession, catalog: str, schema: str) -> pd.DataFrame:
    """Load Wyscout player metadata from players bronze table.

    Includes short_name for improved matching and currentTeamId for
    potential team-scoped matching in Layer 1.
    """
    df = spark.sql(f"""
        SELECT
            CAST(wyId AS INT) AS player_id,
            CONCAT_WS(' ', firstName, lastName) AS player_name,
            shortName AS short_name,
            birthDate AS birth_date,
            role:name::STRING AS position,
            CAST(currentTeamId AS STRING) AS current_team_id
        FROM {catalog}.{schema}.wyscout_players
        WHERE wyId IS NOT NULL
    """).toPandas()  # noqa: S608
    return df


@workflow("wf-entity-resolution", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx=None,
) -> None:
    """Execute cross-source player entity resolution pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    logger.info("Starting entity resolution for %s.%s", catalog, schema)

    # Load player metadata from each source
    sb_players = _load_statsbomb_players(spark, catalog, schema)
    ws_players = _load_wyscout_players(spark, catalog, schema)

    logger.info("Loaded %d StatsBomb players, %d Wyscout players", len(sb_players), len(ws_players))

    # Run three-layer resolution
    from analytics.entity_resolution import ResolutionConfig, resolve_players

    config = ResolutionConfig(confidence_threshold=70.0)
    xref = resolve_players(sb_players, ws_players, config=config)

    if xref.empty:
        logger.warning("No cross-source matches found")
        return

    # Add source labels
    xref["source_a"] = "statsbomb"
    xref["source_b"] = "wyscout"

    # Write to bronze
    sdf = spark.createDataFrame(xref)
    row_count = validate_dataframe(
        sdf,
        ["player_id_a", "player_id_b", "confidence", "match_method", "match_layer", "source_a", "source_b"],
        "player_xref_raw",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "player_xref_raw",
        mode="overwrite",
        logger=logger,
        row_count=row_count,
    )

    logger.info("Entity resolution complete: %d cross-source matches written", row_count)


def main() -> None:
    """CLI entry point for entity resolution."""
    args = parse_ingestion_args("Run cross-source player entity resolution")
    logger = configure_logging("entity_resolution")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    from ingestion.guards import read_gate_result

    filter_result = read_gate_result("wf-entity-resolution")
    if filter_result is None:
        filter_result = skip_guard.check(spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
