"""SkillCorner tracking ingestion -- tracking_extrapolated.jsonl to bronze.

Streams the JSONL artifact line-by-line, reshapes to narrow format
(one row per player per frame), normalizes timestamp from string to
float seconds, and renames is_detected -> is_visible.

Bronze table: bronze.skillcorner_tracking
Coordinate system: center-origin meters (preserved as-is).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_FRAME_RATE = 10

_TIMESTAMP_PATTERN = re.compile(r"^(\d+):(\d+):(\d+(?:\.\d+)?)$")

_TRACKING_DTYPE_OVERRIDES: dict[str, str] = {
    "period": "Int64",
    "frame": "Int64",
    "timestamp": "Float64",
    "player_id": "Int64",
    "x": "Float64",
    "y": "Float64",
    "ball_x": "Float64",
    "ball_y": "Float64",
    "ball_z": "Float64",
    "frame_rate": "Int64",
    "is_visible": "boolean",
    "ball_is_detected": "boolean",
}


def _parse_timestamp(ts_str: str) -> float:
    """Parse 'HH:MM:SS.ms' to float seconds.

    Examples:
        '00:12:34.90' -> 754.9
        '01:30:00.00' -> 5400.0
    """
    m = _TIMESTAMP_PATTERN.match(ts_str)
    if m is None:
        raise ValueError(f"Cannot parse timestamp: {ts_str!r}")
    hours = int(m.group(1))
    minutes = int(m.group(2))
    seconds = float(m.group(3))
    return hours * 3600.0 + minutes * 60.0 + seconds


def parse_tracking_jsonl(source: str, *, match_id: str) -> pd.DataFrame:
    """Parse a tracking_extrapolated.jsonl file to narrow-format DataFrame.

    Streams line-by-line (one JSON object per frame). Each frame contains
    player_data (list of player positions) and ball_data. Reshapes to one
    row per player per frame.

    Args:
        source: File path to the JSONL file.
        match_id: Raw native SkillCorner match ID (e.g. "1886347").

    Returns:
        DataFrame in narrow format with columns per spec section 5.2.
    """
    rows: list[dict[str, object]] = []

    with open(source, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            frame_obj = json.loads(line)

            frame_num = frame_obj["frame"]
            period = frame_obj["period"]
            ts_raw = frame_obj.get("timestamp")
            timestamp = _parse_timestamp(ts_raw) if ts_raw else None

            ball = frame_obj.get("ball_data") or {}
            ball_x = ball.get("x")
            ball_y = ball.get("y")
            ball_z = ball.get("z")
            ball_is_detected = ball.get("is_detected")

            for player in frame_obj.get("player_data", []):
                rows.append(
                    {
                        "match_id": match_id,
                        "period": period,
                        "frame": frame_num,
                        "timestamp": timestamp,
                        "player_id": player["player_id"],
                        "x": player.get("x"),
                        "y": player.get("y"),
                        "is_visible": player.get("is_detected"),
                        "ball_x": ball_x,
                        "ball_y": ball_y,
                        "ball_z": ball_z,
                        "ball_is_detected": ball_is_detected,
                        "frame_rate": _FRAME_RATE,
                    }
                )

    df = pd.DataFrame(rows)
    # Apply dtype overrides for Arrow/Spark compatibility
    for col, dtype in _TRACKING_DTYPE_OVERRIDES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)  # type: ignore[arg-type]
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
    """Write parsed tracking DataFrame to bronze.skillcorner_tracking."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "frame", "period", "player_id", "x", "y"],
        "skillcorner_tracking",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_tracking",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
