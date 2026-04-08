"""SkillCorner open data ingestion into the Databricks bronze layer.

Downloads broadcast tracking data for 10 A-League matches from the
SkillCorner open data repository via the kloppy library. Converts frame
objects to narrow format (one row per player per frame) for the bronze layer.

Data source:
  SkillCorner Open Data repository (MIT License).
  Copyright (c) 2020 SkillCorner.
  https://github.com/SkillCorner/opendata

Bronze table produced:
  - skillcorner_tracking (narrow format: one row per player per frame)

Coordinate system (preserved in bronze):
  SkillCorner center-origin meters: x in (-52.5, 52.5), y in (-34, 34)
  on 105x68m pitch. Staging layer transforms to the shared 120x80 system.
"""

from __future__ import annotations

import gc
import logging
import math
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _SkillcornerGuard:
    workflow_id = "wf-skillcorner"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _SkillcornerGuard()

# All 10 SkillCorner A-League 2024/25 match IDs
SKILLCORNER_MATCH_IDS: list[str] = [
    "1886347",
    "1899585",
    "1925299",
    "1953632",
    "1996435",
    "2006229",
    "2011166",
    "2013725",
    "2015213",
    "2017461",
]

# SkillCorner broadcast tracking is 10fps
_FRAME_RATE = 10


def _smooth_tracking(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Savitzky-Golay smoothing and clamp to pitch bounds."""
    from analytics.smoothing import smooth_positions

    result = smooth_positions(df)
    # Clamp to SkillCorner center-origin pitch: x ∈ [-52.5, 52.5], y ∈ [-34, 34]
    result["x"] = result["x"].clip(-52.5, 52.5)
    result["y"] = result["y"].clip(-34.0, 34.0)
    return result


def _dataset_to_rows(
    dataset: object,
    match_id: str,
) -> list[dict[str, object]]:
    """Convert kloppy TrackingDataset to narrow-format row dicts.

    Iterates all frames and player coordinates to produce one row per
    player per frame with center-origin meter coordinates.

    Args:
        dataset: kloppy ``TrackingDataset`` returned by ``load_open_data()``.
        match_id: Match identifier to embed in each row.

    Returns:
        List of row dicts ready for DataFrame construction.
    """
    rows: list[dict[str, object]] = []
    prefixed_match_id = f"skillcorner_{match_id}"

    # Build team lookup: team object -> "home" / "away"
    teams = dataset.metadata.teams  # type: ignore[union-attr]
    home_team = teams[0]
    away_team = teams[1]

    for frame in dataset:  # type: ignore[union-attr]
        frame_id: int = frame.frame_id
        period: int = frame.period.id
        timestamp: float = frame.timestamp.total_seconds() if frame.timestamp else 0.0

        # Ball coordinates
        ball_x: float | None = None
        ball_y: float | None = None
        if frame.ball_coordinates is not None:
            bx = frame.ball_coordinates.x
            by = frame.ball_coordinates.y
            if bx is not None and not (isinstance(bx, float) and math.isnan(bx)):
                ball_x = round(float(bx), 4)
            if by is not None and not (isinstance(by, float) and math.isnan(by)):
                ball_y = round(float(by), 4)

        # Player coordinates
        if frame.players_coordinates is None:
            continue

        for player, point in frame.players_coordinates.items():
            px = point.x
            py = point.y

            if px is None or py is None:
                continue
            if isinstance(px, float) and math.isnan(px):
                continue
            if isinstance(py, float) and math.isnan(py):
                continue

            team_str = "home" if player.team == home_team else "away" if player.team == away_team else "unknown"

            # Determine goalkeeper status from kloppy starting_position.
            # kloppy uses starting_position (not position) for the Player's role.
            # SkillCorner's kloppy mapping (role ID 1 → Unknown) does not expose
            # Goalkeeper, so we also fall back to jersey_no == 1 as heuristic.
            sp = getattr(player, "starting_position", None)
            sp_name = getattr(sp, "name", None) if sp is not None else None
            jersey = getattr(player, "jersey_no", None)
            is_gk = sp_name == "Goalkeeper" or (sp_name in (None, "Unknown") and jersey == 1)

            rows.append(
                {
                    "period": period,
                    "frame": frame_id,
                    "timestamp": round(timestamp, 4),
                    "player_id": str(player.player_id),
                    "team": team_str,
                    "x": round(float(px), 4),
                    "y": round(float(py), 4),
                    "ball_x": ball_x,
                    "ball_y": ball_y,
                    "match_id": prefixed_match_id,
                    "frame_rate": _FRAME_RATE,
                    "is_goalkeeper": is_gk,
                }
            )

    return rows


def ingest_skillcorner(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    match_ids: list[str] | None = None,
) -> None:
    """Download and ingest SkillCorner tracking data for all matches.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Target schema (e.g. ``bronze``).
        logger: Structured logger instance.
        match_ids: Optional subset of match IDs to ingest. Defaults to all 10.
    """
    from kloppy import skillcorner

    ids_to_ingest = match_ids or SKILLCORNER_MATCH_IDS
    required_cols = ["period", "frame", "timestamp", "player_id", "team", "x", "y", "match_id", "frame_rate"]

    # Check which matches already have tracking data (incremental skip)
    existing_ids: set[str] = set()
    try:
        existing_rows = spark.table(f"{catalog}.{schema}.skillcorner_tracking").select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing skillcorner_tracking table — processing all matches")

    new_match_ids = [mid for mid in ids_to_ingest if f"skillcorner_{mid}" not in existing_ids]
    logger.info(
        "%d matches total, %d already processed, %d to process",
        len(ids_to_ingest),
        len(ids_to_ingest) - len(new_match_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return

    for i, mid in enumerate(new_match_ids):
        logger.info("Loading SkillCorner match %s (%d/%d) via kloppy", mid, i + 1, len(new_match_ids))

        dataset = skillcorner.load_open_data(
            match_id=mid,
            coordinates="skillcorner",
            include_empty_frames=False,
        )

        rows = _dataset_to_rows(dataset, mid)
        logger.info("Parsed %d tracking rows for SkillCorner match %s", len(rows), mid)

        if rows:
            df = pd.DataFrame(rows)
            df = _smooth_tracking(df)
            sdf = spark.createDataFrame(df)
            row_count = validate_dataframe(sdf, required_cols, "skillcorner_tracking", logger)
            replace_expr = f"match_id = 'skillcorner_{mid}'"
            write_delta_table(
                sdf,
                catalog,
                schema,
                "skillcorner_tracking",
                replace_where=replace_expr,
                logger=logger,
                row_count=row_count,
            )
            del df, sdf, rows
            gc.collect()


@workflow("wf-skillcorner", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx: object = None,
) -> None:
    """Ingest SkillCorner A-League broadcast tracking data."""
    ingest_skillcorner(spark, catalog, schema, logger)


def main() -> None:
    """CLI entry point for SkillCorner tracking data ingestion."""
    args = parse_ingestion_args("Ingest SkillCorner A-League tracking data into the bronze layer")
    logger = configure_logging("skillcorner")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    logger.info("Starting SkillCorner ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)
    logger.info("SkillCorner ingestion complete")


if __name__ == "__main__":
    main()
