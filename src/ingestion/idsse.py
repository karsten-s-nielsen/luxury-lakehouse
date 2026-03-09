"""IDSSE (Bundesliga) tracking data ingestion into the Databricks bronze layer.

Reads pre-downloaded DFL position XML data for 7 Bundesliga matches from a
UC Volume (originally sourced from the IDSSE figshare collection). Parses the
XML directly using xml.etree.ElementTree to produce narrow format (one row per
player per frame) for the bronze layer.

Data source:
  Bassek et al. "An integrated dataset of spatiotemporal and event data in
  elite soccer." Scientific Data, Nature (2025). CC-BY 4.0.
  https://figshare.com/collections/DFL_-_Bundesliga_Data_Shootout/5830772

Bronze table produced:
  - idsse_tracking (narrow format: one row per player per frame)

Coordinate system (preserved in bronze):
  DFL center-origin meters: x in (-52.5, 52.5), y in (-34, 34) on 105x68m pitch.
  Staging layer transforms to the shared 120x80 coordinate system.
"""

from __future__ import annotations

import gc
import logging
import math
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# All 7 IDSSE match IDs from figshare collection
IDSSE_MATCH_IDS: list[str] = [
    "J03WMX",
    "J03WN1",
    "J03WPY",
    "J03WOH",
    "J03WQQ",
    "J03WOY",
    "J03WR9",
]

# Competition ID mapping
_MATCH_COMPETITION: dict[str, str] = {
    "J03WMX": "DFL-COM-000001",
    "J03WN1": "DFL-COM-000001",
    "J03WPY": "DFL-COM-000002",
    "J03WOH": "DFL-COM-000002",
    "J03WQQ": "DFL-COM-000002",
    "J03WOY": "DFL-COM-000002",
    "J03WR9": "DFL-COM-000002",
}

_SECTION_TO_PERIOD = {"firstHalf": 1, "secondHalf": 2}

# Frame rate for all IDSSE matches (DFL position data is 25fps)
_FRAME_RATE = 25


def _smooth_tracking(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Savitzky-Golay smoothing and clamp to pitch bounds."""
    from analytics.smoothing import smooth_positions

    result = smooth_positions(df)
    # Clamp to DFL center-origin pitch: x ∈ [-52.5, 52.5], y ∈ [-34, 34]
    result["x"] = result["x"].clip(-52.5, 52.5)
    result["y"] = result["y"].clip(-34.0, 34.0)
    return result


# Default Volume path for pre-downloaded IDSSE data
_DEFAULT_DATA_DIR = "/Volumes/soccer_analytics/bronze/libs/idsse_data"


def _parse_teams(info_path: str) -> tuple[str, str, dict[str, str]]:
    """Parse match info XML to get home/away team IDs and player-to-team mapping.

    Args:
        info_path: Path to match info XML file.

    Returns:
        Tuple of (home_team_id, away_team_id, {person_id: "home"|"away"}).
    """
    tree = ET.parse(info_path)  # noqa: S314
    root = tree.getroot()

    home_team_id = ""
    away_team_id = ""
    player_team_map: dict[str, str] = {}

    for team_el in root.iter("Team"):
        team_id = team_el.get("TeamId", "")
        role = team_el.get("Role", "")

        if role == "home":
            home_team_id = team_id
            team_label = "home"
        elif role == "guest":
            away_team_id = team_id
            team_label = "away"
        else:
            continue

        for player_el in team_el.iter("Player"):
            person_id = player_el.get("PersonId", "")
            if person_id:
                player_team_map[person_id] = team_label

    return home_team_id, away_team_id, player_team_map


def _parse_positions_xml(
    pos_path: str,
    player_team_map: dict[str, str],
    match_id: str,
    logger: logging.Logger,
) -> list[dict[str, object]]:
    """Parse DFL position XML into narrow-format row dicts.

    Uses iterative XML parsing to avoid loading the entire ~400MB file into
    memory at once. Each FrameSet contains frames for one person in one half.

    Args:
        pos_path: Path to position XML file.
        player_team_map: Mapping of PersonId to "home"/"away".
        match_id: Match identifier to embed in each row.
        logger: Logger instance.

    Returns:
        List of row dicts ready for DataFrame construction.
    """
    rows: list[dict[str, object]] = []
    prefixed_match_id = f"idsse_{match_id}"

    # Collect ball frames per (period, frame_n) for lookup
    ball_coords: dict[tuple[int, int], tuple[float, float]] = {}

    # First pass: collect ball coordinates
    for _event, elem in ET.iterparse(pos_path, events=("end",)):  # noqa: S314
        if elem.tag != "FrameSet":
            continue

        team_id = elem.get("TeamId", "").lower()
        if team_id != "ball":
            elem.clear()
            continue

        section = elem.get("GameSection", "")
        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            elem.clear()
            continue

        for frame_el in elem.iter("Frame"):
            n = int(frame_el.get("N", "0"))
            x_str = frame_el.get("X", "")
            y_str = frame_el.get("Y", "")
            if x_str and y_str:
                bx = float(x_str)
                by = float(y_str)
                if not (math.isnan(bx) or math.isnan(by)):
                    ball_coords[(period, n)] = (round(bx, 4), round(by, 4))

        elem.clear()

    logger.info("Parsed %d ball frames for match %s", len(ball_coords), match_id)

    # Second pass: collect player coordinates
    for _event, elem in ET.iterparse(pos_path, events=("end",)):  # noqa: S314
        if elem.tag != "FrameSet":
            continue

        team_id = elem.get("TeamId", "")
        person_id = elem.get("PersonId", "")
        section = elem.get("GameSection", "")

        # Skip ball and referee FrameSets
        if team_id.lower() in ("ball", "referee"):
            elem.clear()
            continue

        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            elem.clear()
            continue

        team_label = player_team_map.get(person_id, "unknown")

        for frame_el in elem.iter("Frame"):
            n = int(frame_el.get("N", "0"))
            x_str = frame_el.get("X", "")
            y_str = frame_el.get("Y", "")

            if not x_str or not y_str:
                continue

            px = float(x_str)
            py = float(y_str)

            if math.isnan(px) or math.isnan(py):
                continue

            timestamp = n / _FRAME_RATE
            ball = ball_coords.get((period, n))
            ball_x = ball[0] if ball else None
            ball_y = ball[1] if ball else None

            rows.append(
                {
                    "period": period,
                    "frame": n,
                    "timestamp": round(timestamp, 4),
                    "player_id": person_id,
                    "team": team_label,
                    "x": round(px, 4),
                    "y": round(py, 4),
                    "ball_x": ball_x,
                    "ball_y": ball_y,
                    "match_id": prefixed_match_id,
                    "frame_rate": _FRAME_RATE,
                }
            )

        elem.clear()

    return rows


def ingest_idsse(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    match_ids: list[str] | None = None,
    data_dir: str = _DEFAULT_DATA_DIR,
) -> None:
    """Parse and ingest IDSSE tracking data for all matches.

    Reads pre-downloaded DFL XML files from a UC Volume directory and writes
    narrow-format tracking data to Delta. Processes one match at a time to
    limit peak memory usage.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Target schema (e.g. ``bronze``).
        logger: Structured logger instance.
        match_ids: Optional subset of match IDs to ingest. Defaults to all 7.
        data_dir: Directory containing pre-downloaded DFL XML files.
    """
    ids_to_ingest = match_ids or IDSSE_MATCH_IDS
    required_cols = ["period", "frame", "timestamp", "player_id", "team", "x", "y", "match_id", "frame_rate"]

    for i, mid in enumerate(ids_to_ingest):
        logger.info("Parsing IDSSE match %s (%d/%d)", mid, i + 1, len(ids_to_ingest))

        comp = _MATCH_COMPETITION[mid]
        info_path = f"{data_dir}/DFL_02_01_matchinformation_{comp}_DFL-MAT-{mid}.xml"
        pos_path = f"{data_dir}/DFL_04_03_positions_raw_observed_{comp}_DFL-MAT-{mid}.xml"

        _home_id, _away_id, player_team_map = _parse_teams(info_path)
        logger.info("Found %d players in match info", len(player_team_map))

        rows = _parse_positions_xml(pos_path, player_team_map, mid, logger)
        logger.info("Parsed %d tracking rows for IDSSE match %s", len(rows), mid)

        if rows:
            df = pd.DataFrame(rows)
            df = _smooth_tracking(df)
            sdf = spark.createDataFrame(df)
            row_count = validate_dataframe(sdf, required_cols, "idsse_tracking", logger)
            replace_expr = f"match_id = 'idsse_{mid}'"
            write_delta_table(
                sdf,
                catalog,
                schema,
                "idsse_tracking",
                replace_where=replace_expr,
                logger=logger,
                row_count=row_count,
            )
            del df, sdf, rows  # Free memory between matches
            gc.collect()


def main() -> None:
    """CLI entry point for IDSSE tracking data ingestion."""
    args = parse_ingestion_args("Ingest IDSSE Bundesliga tracking data into the bronze layer")
    logger = configure_logging("idsse")
    spark = get_spark_session()

    logger.info("Starting IDSSE ingestion into %s.%s", args.catalog, args.schema)
    ingest_idsse(spark, args.catalog, args.schema, logger)
    logger.info("IDSSE ingestion complete")


if __name__ == "__main__":
    main()
