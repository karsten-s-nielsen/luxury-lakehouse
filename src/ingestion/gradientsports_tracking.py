"""Gradient Sports tracking ingestion — tracking artifact to bronze.

Parses the tracking artifact from the pining-for-the-data API into
narrow format (one row per player per frame) and writes to
bronze.gradientsports_tracking.

Coordinate system: center-origin meters (preserved as-is in bronze).
The silly-kicks ``convert_to_frames`` converter handles the final transform.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_tracking(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports tracking data into narrow-format DataFrame.

    The exact parsing logic depends on the artifact format discovered
    from the pining-for-the-data API. This implementation handles the
    expected format: list of frames, each with player positions.

    Args:
        source: Raw tracking data (JSON string, dict, or list).
        match_id: Native match ID.

    Returns:
        DataFrame in narrow format (one row per player per frame)
        with columns matching silly-kicks EXPECTED_INPUT_COLUMNS.
    """
    import json

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    # Handle both list-of-frames and dict-with-frames-key formats
    if isinstance(data, dict):
        frames_list = data.get("frames", data.get("data", []))
    else:
        frames_list = data

    rows: list[dict] = []
    for frame_obj in frames_list:
        fid = frame_obj.get("frame_id", frame_obj.get("frame"))
        pid = frame_obj.get("period_id", frame_obj.get("period"))
        t = frame_obj.get("time_seconds", frame_obj.get("timestamp"))
        fr = frame_obj.get("frame_rate", 30)
        ball_state = frame_obj.get("ball_state", frame_obj.get("ball_status"))

        # Player rows
        for player in frame_obj.get("players", frame_obj.get("player_data", [])):
            rows.append(
                {
                    "match_id": match_id,
                    "game_id": match_id,
                    "period_id": pid,
                    "frame_id": fid,
                    "time_seconds": t,
                    "frame_rate": fr,
                    "player_id": player.get("player_id"),
                    "team_id": player.get("team_id"),
                    "is_ball": False,
                    "is_goalkeeper": player.get("is_goalkeeper", False),
                    "x_centered": player.get("x"),
                    "y_centered": player.get("y"),
                    "z": player.get("z"),
                    "speed_native": player.get("speed"),
                    "ball_state": ball_state,
                }
            )

        # Ball row
        ball = frame_obj.get("ball", frame_obj.get("ball_data"))
        if ball:
            rows.append(
                {
                    "match_id": match_id,
                    "game_id": match_id,
                    "period_id": pid,
                    "frame_id": fid,
                    "time_seconds": t,
                    "frame_rate": fr,
                    "player_id": None,
                    "team_id": None,
                    "is_ball": True,
                    "is_goalkeeper": False,
                    "x_centered": ball.get("x"),
                    "y_centered": ball.get("y"),
                    "z": ball.get("z"),
                    "speed_native": ball.get("speed"),
                    "ball_state": ball_state,
                }
            )

    df = pd.DataFrame(rows)
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_tracking(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed tracking DataFrame to bronze.gradientsports_tracking."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "frame_id", "period_id"],
        "gradientsports_tracking",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_tracking",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
