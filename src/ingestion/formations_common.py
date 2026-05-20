"""Shared helpers for formation detection pipelines.

Provides constants, column schemas, and utility functions used by both the
EFPI (``formations_efpi``) and shape graph (``formations_shape_graph``)
modules.

Constants:
  ``TABLE_NAME`` — target Delta table for formation labels.
  ``POSITIONS_TABLE_NAME`` — target Delta table for player positions.
  ``TEMP_TABLE_SUFFIX`` — temp Delta table for materialized tracking data.
  ``RESULT_COLUMNS`` — column schema for formation label rows.
  ``POSITION_COLUMNS`` — column schema for player position rows.
  ``VERTICAL_LEVEL_ORDER`` — ordered vertical levels for label derivation.

Functions:
  ``attacking_direction()`` — returns +1.0 or -1.0 for a team/period.
  ``derive_formation_label()`` — derives "4-4-2" style label from levels.
  ``prepare_tracking_data()`` — queries gold, applies skip guard, materializes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ingestion.guards import ensure_table
from shared.constants import DEFAULT_GOLD_SCHEMA

_FORMATION_LABELS_SCHEMA = (
    "match_id STRING, period INT, team STRING, window_start_s DOUBLE, window_end_s DOUBLE, "
    "formation_label STRING, cost DOUBLE, detector STRING, source_provider STRING, "
    "_ingested_at TIMESTAMP"
)
_POSITIONS_SCHEMA = (
    "match_id STRING, frame_id BIGINT, player_id STRING, team STRING, "
    "position_label STRING, vertical_level STRING, horizontal_level STRING, "
    "detector STRING, source_provider STRING, _ingested_at TIMESTAMP"
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

TABLE_NAME = "formation_labels"
POSITIONS_TABLE_NAME = "player_positions"
TEMP_TABLE_SUFFIX = "__temp_formations_tracking"

RESULT_COLUMNS = [
    "match_id",
    "period",
    "team",
    "window_start_s",
    "window_end_s",
    "formation_label",
    "cost",
    "detector",
    "source_provider",
]

POSITION_COLUMNS = [
    "match_id",
    "frame_id",
    "player_id",
    "team",
    "position_label",
    "vertical_level",
    "horizontal_level",
    "detector",
    "source_provider",
]

# Vertical level ordering for formation label derivation (back -> front).
# Levels with zero players are skipped.
VERTICAL_LEVEL_ORDER: tuple[str, ...] = ("B", "DM", "M", "AM", "F")


# ---------------------------------------------------------------------------
# Shared utility functions
# ---------------------------------------------------------------------------


def attacking_direction(team: str, period: int) -> float:
    """Return attacking direction for *team* in *period*.

    Convention (StatsBomb / Metrica coordinate system):
    * Home attacks left-to-right (+1.0) in period 1, right-to-left (-1.0) in period 2.
    * Away is the opposite.
    """
    home_p1: float = 1.0
    if team == "away":
        home_p1 = -home_p1
    if period == 2:
        home_p1 = -home_p1
    return home_p1


def derive_formation_label(vertical_levels: list[str]) -> str:
    """Derive a formation label string from vertical level assignments.

    Counts players per vertical level, orders by the standard back-to-front
    sequence (B -> DM -> M -> AM -> F), skips levels with zero players, and
    joins with hyphens.  E.g. {B:4, M:4, F:2} -> ``"4-4-2"``.
    """
    from collections import Counter

    counts = Counter(vertical_levels)
    parts = [str(counts[lv]) for lv in VERTICAL_LEVEL_ORDER if counts.get(lv, 0) > 0]
    return "-".join(parts) if parts else "unknown"


def find_incomplete_formation_ids(
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> list[str]:
    """Match IDs in tracking that don't have results from BOTH detectors.

    A match is "fully processed" when it has results from at least 2 distinct
    detectors (EFPI + Shape Graph). Uses Spark-native aggregation to push the
    completeness check to executors.
    """
    from pyspark.sql import functions as F  # noqa: N812

    gold_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{TABLE_NAME}"

    if not spark.catalog.tableExists(gold_table):
        return []

    source_df = spark.table(gold_table).select(F.col("match_id").cast("string").alias("match_id")).distinct()

    ensure_table(spark, results_table, _FORMATION_LABELS_SCHEMA)
    fully_processed = (
        spark.table(results_table)
        .groupBy(F.col("match_id").cast("string").alias("match_id"))
        .agg(F.countDistinct("detector").alias("n_detectors"))
        .filter(F.col("n_detectors") >= 2)
        .select("match_id")
    )
    new_df = source_df.join(fully_processed, on="match_id", how="left_anti")

    rows = new_df.collect()
    return [str(row["match_id"]) for row in rows]


def prepare_tracking_data(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> tuple[SparkDataFrame, list[str], str] | None:
    """Query gold tracking data, apply skip guard, materialize to temp Delta table.

    Returns ``(tracking_df, new_ids_str, temp_table)`` if there are matches to
    process, or ``None`` if everything is already processed.

    The temp table is written to ``{catalog}.{schema}.__temp_formations_tracking``
    so both detector passes can read from it without re-scanning the full
    38M-row gold source.
    """
    gold_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_tracking_frames"
    results_table = f"{catalog}.{schema}.{TABLE_NAME}"

    # Get all distinct match_ids from tracking data
    if not spark.catalog.tableExists(gold_table):
        logger.warning("Source table %s does not exist", gold_table)
        return None

    match_id_rows = spark.table(gold_table).select("match_id").distinct().collect()

    if not match_id_rows:
        logger.info("No matches in %s", gold_table)
        return None

    all_match_ids = [row["match_id"] for row in match_id_rows]

    # Incremental skip guard — only skip matches that have results from BOTH
    # detectors.  A match with only EFPI results (pre-Cycle 2) still needs
    # shape graph processing.
    ensure_table(spark, results_table, _FORMATION_LABELS_SCHEMA)

    from pyspark.sql import functions as _F  # noqa: N812

    detector_counts = (
        spark.table(results_table)
        .groupBy("match_id")
        .agg(_F.countDistinct("detector").alias("n_detectors"))
        .filter(_F.col("n_detectors") >= 2)
        .select("match_id")
        .collect()
    )
    fully_processed: set[str] = {str(row["match_id"]) for row in detector_counts}

    new_match_ids = [mid for mid in all_match_ids if str(mid) not in fully_processed]
    logger.info(
        "%d matches total, %d fully processed (both detectors), %d to process",
        len(all_match_ids),
        len(fully_processed),
        len(new_match_ids),
    )

    if not new_match_ids:
        return None

    # --- pyspark imports deferred past early-exit guards ---
    from pyspark.sql import functions as F  # noqa: N812

    new_ids_str = [str(mid) for mid in new_match_ids]

    # Materialize filtered tracking data to a temp table so both detector
    # passes read from it without re-scanning the full 38M-row source.
    # (OPT-AUDIT: .cache() is forbidden on serverless; temp Delta table
    # is the CLAUDE.md-sanctioned alternative for re-read avoidance.)
    temp_table = f"{catalog}.{schema}.{TEMP_TABLE_SUFFIX}"
    (
        spark.table(gold_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            "match_id",
            "period",
            "team",
            "player_id",
            "timestamp_seconds",
            "x",
            "y",
            "is_goalkeeper",
            "frame",
            "source_provider",  # PR-1.5: propagate to bronze for staging
        )
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(temp_table)
    )
    tracking_df = spark.table(temp_table)
    logger.info("Materialized filtered tracking data to %s", temp_table)

    return tracking_df, new_ids_str, temp_table
