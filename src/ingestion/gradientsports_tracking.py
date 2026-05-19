"""Gradient Sports tracking ingestion — tracking artifact to bronze.

Parses the tracking artifact from the pining-for-the-data API into
narrow format (one row per player per frame) and writes to
bronze.gradientsports_tracking.

Artifact format: ``tracking.jsonl.bz2`` — bz2-compressed newline-delimited JSON.
Each line is one frame with ``homePlayers``, ``awayPlayers``, ``balls``,
and their smoothed counterparts, plus frame-level event annotations.

Coordinate system: center-origin meters (preserved as-is in bronze).
The silly-kicks ``convert_to_frames`` converter handles the final transform.
"""

from __future__ import annotations

import bz2
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_tracking(source: bytes | str | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports tracking data into narrow-format DataFrame.

    Args:
        source: Raw tracking data — bz2-compressed bytes (from API),
            or pre-parsed list of frame dicts (for testing).
        match_id: Native match ID.

    Returns:
        DataFrame in narrow format (one row per player/ball per frame).
    """
    if isinstance(source, bytes):
        decompressed = bz2.decompress(source)
        lines = decompressed.decode("utf-8").strip().split("\n")
        frames_list = [json.loads(line) for line in lines]
    elif isinstance(source, str):
        # Backwards compat: if caller passes a JSON string (single array/object)
        data = json.loads(source)
        frames_list = data if isinstance(data, list) else [data]
    else:
        frames_list = source

    rows: list[dict] = []
    for frame in frames_list:
        # Frame-level fields
        frame_num = frame.get("frameNum")
        period = frame.get("period")
        game_ref_id = frame.get("gameRefId")
        period_elapsed = frame.get("periodElapsedTime")
        period_game_clock = frame.get("periodGameClockTime")
        video_time_ms = frame.get("videoTimeMs")
        version = frame.get("version")
        generated_time = frame.get("generatedTime")
        smoothed_time = frame.get("smoothedTime")
        # Event annotations at frame level
        game_event_id = frame.get("game_event_id")
        possession_event_id = frame.get("possession_event_id")
        game_event_json = json.dumps(frame["game_event"]) if frame.get("game_event") else None
        possession_event_json = json.dumps(frame["possession_event"]) if frame.get("possession_event") else None

        base = {
            "match_id": match_id,
            "game_ref_id": game_ref_id,
            "frame_num": frame_num,
            "period": period,
            "period_elapsed_time": period_elapsed,
            "period_game_clock_time": period_game_clock,
            "video_time_ms": video_time_ms,
            "version": version,
            "generated_time": generated_time,
            "smoothed_time": smoothed_time,
            "game_event_id": game_event_id,
            "possession_event_id": possession_event_id,
            "_game_event_json": game_event_json,
            "_possession_event_json": possession_event_json,
        }

        # Home players (raw + smoothed)
        for player in frame.get("homePlayers") or []:
            smoothed = _find_smoothed(frame.get("homePlayersSmoothed") or [], player)
            rows.append(
                {
                    **base,
                    "team_side": "home",
                    "is_ball": False,
                    "jersey_num": player.get("jerseyNum"),
                    "confidence": player.get("confidence"),
                    "visibility": player.get("visibility"),
                    "x": player.get("x"),
                    "y": player.get("y"),
                    "z": None,
                    "x_smoothed": smoothed.get("x") if smoothed else None,
                    "y_smoothed": smoothed.get("y") if smoothed else None,
                    "z_smoothed": None,
                }
            )

        # Away players (raw + smoothed)
        for player in frame.get("awayPlayers") or []:
            smoothed = _find_smoothed(frame.get("awayPlayersSmoothed") or [], player)
            rows.append(
                {
                    **base,
                    "team_side": "away",
                    "is_ball": False,
                    "jersey_num": player.get("jerseyNum"),
                    "confidence": player.get("confidence"),
                    "visibility": player.get("visibility"),
                    "x": player.get("x"),
                    "y": player.get("y"),
                    "z": None,
                    "x_smoothed": smoothed.get("x") if smoothed else None,
                    "y_smoothed": smoothed.get("y") if smoothed else None,
                    "z_smoothed": None,
                }
            )

        # Ball(s)
        for ball in frame.get("balls") or []:
            ball_smoothed = frame.get("ballsSmoothed")
            if isinstance(ball_smoothed, list) and ball_smoothed:
                ball_smoothed = ball_smoothed[0]
            elif not isinstance(ball_smoothed, dict):
                ball_smoothed = None
            rows.append(
                {
                    **base,
                    "team_side": None,
                    "is_ball": True,
                    "jersey_num": None,
                    "confidence": None,
                    "visibility": ball.get("visibility"),
                    "x": ball.get("x"),
                    "y": ball.get("y"),
                    "z": ball.get("z"),
                    "x_smoothed": ball_smoothed.get("x") if ball_smoothed else None,
                    "y_smoothed": ball_smoothed.get("y") if ball_smoothed else None,
                    "z_smoothed": ball_smoothed.get("z") if ball_smoothed else None,
                }
            )

    df = pd.DataFrame(rows)
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def _find_smoothed(
    smoothed_list: list[dict],
    raw_player: dict,
) -> dict | None:
    """Match a smoothed entry to its raw counterpart by jerseyNum."""
    jersey = raw_player.get("jerseyNum")
    if jersey is None:
        return None
    for s in smoothed_list:
        if s.get("jerseyNum") == jersey:
            return s
    return None


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
        ["match_id", "frame_num", "period"],
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
