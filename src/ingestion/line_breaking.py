"""Line-breaking pass detection batch computation pipeline.

Reads pass events and opponent positions from existing bronze Delta tables,
runs the line-breaking detection algorithm, and writes results to a new
``line_breaking_results`` bronze table.

Two data paths:
  - **Path A (360 freeze frames):** StatsBomb matches with per-event opponent
    positions from ``statsbomb_360``.
  - **Path B (Metrica tracking):** Metrica matches with frame-level tracking
    joined to event data.

Design: "Read from bronze, compute, write to bronze." No external API calls.
"""

from __future__ import annotations

import gc
import json
import logging
from typing import TYPE_CHECKING

import pandas as pd

from analytics.line_breaking import LineBreakingParams, detect_line_breaking
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    merge_delta_table,
    parse_ingestion_args,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "line_breaking_results"
_XY_COLS = pd.Index(["x", "y"])


# ---------------------------------------------------------------------------
# JSON parsing helpers
# ---------------------------------------------------------------------------


def _parse_location(loc: object) -> tuple[float, float] | None:
    """Parse a location value to ``(x, y)`` tuple.

    Handles JSON strings (``'[x, y]'``) and Python lists/tuples.
    """
    if loc is None:
        return None
    if isinstance(loc, float) and pd.isna(loc):
        return None
    if isinstance(loc, str) and loc.strip() in ("", "null", "None"):
        return None
    if isinstance(loc, (list, tuple)) and len(loc) >= 2:
        return (float(loc[0]), float(loc[1]))
    try:
        coords = json.loads(str(loc))
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            return (float(coords[0]), float(coords[1]))
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None


def _parse_locations_series(loc_series: pd.Series) -> pd.DataFrame:  # type: ignore[type-arg]
    """Parse a Series of location values to a DataFrame with ``x, y`` columns."""
    positions: list[dict[str, float]] = []
    for loc in loc_series:
        parsed = _parse_location(loc)
        if parsed is not None:
            positions.append({"x": parsed[0], "y": parsed[1]})
    return pd.DataFrame(positions) if positions else pd.DataFrame(columns=_XY_COLS)


def _parse_tracking_json(json_str: object) -> pd.DataFrame:
    """Parse a Metrica tracking JSON dict to DataFrame with ``x, y`` columns.

    Input format: ``'{"11": {"x": 0.43, "y": 0.62}, "7": {"x": 0.51, "y": 0.33}}'``

    Returns DataFrame with one row per player.
    """
    if json_str is None:
        return pd.DataFrame(columns=_XY_COLS)
    if isinstance(json_str, float) and pd.isna(json_str):
        return pd.DataFrame(columns=_XY_COLS)
    try:
        players = json.loads(str(json_str))
        if not isinstance(players, dict):
            return pd.DataFrame(columns=_XY_COLS)
        positions: list[dict[str, float]] = []
        for _jersey, coords in players.items():
            if isinstance(coords, dict) and "x" in coords and "y" in coords:
                x_val, y_val = coords["x"], coords["y"]
                if x_val is not None and y_val is not None:
                    positions.append({"x": float(x_val), "y": float(y_val)})
        return pd.DataFrame(positions) if positions else pd.DataFrame(columns=_XY_COLS)
    except (json.JSONDecodeError, ValueError, TypeError):
        return pd.DataFrame(columns=_XY_COLS)


# ---------------------------------------------------------------------------
# Path A — StatsBomb 360 freeze frames
# ---------------------------------------------------------------------------


def _process_statsbomb_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
) -> int:
    """Detect line-breaking passes using StatsBomb 360 freeze frame data.

    Processes per-match to avoid OOM on the full events/360 tables.
    Reads ``statsbomb_events`` (passes) and ``statsbomb_360`` (opponent
    positions), computes line-breaking per pass, writes to Delta.

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.statsbomb_events"
    ff_table = f"{catalog}.{schema}.statsbomb_360"

    # Get distinct match_ids that have 360 data (small query — just unique IDs)
    try:
        match_ids_pdf = spark.table(ff_table).select("match_id").distinct().toPandas()
    except Exception:
        logger.exception("Cannot read StatsBomb 360 table")
        return 0

    if match_ids_pdf.empty:
        logger.info("No 360 data available — skipping Path A")
        return 0

    match_ids = match_ids_pdf["match_id"].tolist()
    logger.info("Path A: %d matches with 360 data", len(match_ids))

    total_written = 0

    for match_id in match_ids:
        # Pull only passes for this match (Spark-side filter to avoid OOM)
        try:
            passes_pdf = spark.table(events_table).filter(f"type = 'Pass' AND match_id = {int(match_id)}").toPandas()
        except Exception:
            logger.warning("Cannot read events for match %s — skipping", match_id)
            continue

        if passes_pdf.empty:
            continue

        # Pull only opponent 360 frames for this match
        try:
            opponents_pdf = (
                spark.table(ff_table)
                .filter(f"match_id = {int(match_id)} AND teammate = false AND actor = false AND keeper = false")
                .toPandas()
            )
        except Exception:
            logger.warning("Cannot read 360 for match %s — skipping", match_id)
            continue

        if opponents_pdf.empty:
            continue

        logger.info("Match %s: %d passes, %d opponent frames", match_id, len(passes_pdf), len(opponents_pdf))

        results: list[dict[str, object]] = []

        for _, pass_row in passes_pdf.iterrows():
            event_id = str(pass_row["id"])

            # Get opponents for this event (360 id = events id)
            event_opponents = opponents_pdf[opponents_pdf["id"] == pass_row["id"]]
            if event_opponents.empty:
                continue

            # Parse opponent locations from JSON '[x, y]' strings
            opp_positions = _parse_locations_series(event_opponents["location"])
            if len(opp_positions) < params.min_opponents:
                continue

            # Parse pass start/end from JSON location arrays
            start_xy = _parse_location(pass_row.get("location"))
            end_xy = _parse_location(pass_row.get("pass_end_location"))

            if start_xy is None or end_xy is None:
                continue

            result = detect_line_breaking(start_xy[0], start_xy[1], end_xy[0], end_xy[1], opp_positions, params)

            results.append(
                {
                    "event_id": event_id,
                    "match_id": str(match_id),
                    "is_line_breaking": result.is_line_breaking,
                    "lines_broken": result.lines_broken,
                    "line_breaking_type": result.line_breaking_type,
                    "data_source": "statsbomb_360",
                }
            )

        if results:
            result_df = pd.DataFrame(results)
            sdf = spark.createDataFrame(result_df)
            written = merge_delta_table(
                sdf,
                catalog,
                schema,
                _TABLE_NAME,
                merge_key="event_id",
                logger=logger,
            )
            total_written += written
            logger.info("Match %s: %d line-breaking results written", match_id, written)

        del passes_pdf, opponents_pdf
        gc.collect()

    logger.info("Path A complete: %d rows written", total_written)
    return total_written


# ---------------------------------------------------------------------------
# Path B — Metrica tracking + events
# ---------------------------------------------------------------------------


def _process_metrica_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
) -> int:
    """Detect line-breaking passes using Metrica tracking + event data.

    Reads ``metrica_events`` (passes) and ``metrica_tracking`` (player
    positions as JSON dicts), joins at pass frame, computes line-breaking.

    Metrica has only 3 matches — small enough for full toPandas().

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.metrica_events"
    tracking_table = f"{catalog}.{schema}.metrica_tracking"

    try:
        # Filter to PASS events in Spark; Metrica tracking is ~9.5M rows
        # but only 3 matches — pull per-match below
        events_pdf = spark.table(events_table).filter("type = 'PASS'").toPandas()
    except Exception:
        logger.exception("Cannot read Metrica events table")
        return 0

    if events_pdf.empty:
        logger.info("No PASS events in Metrica events — skipping Path B")
        return 0

    match_ids = events_pdf["match_id"].unique()
    logger.info("Path B: %d matches, %d passes", len(match_ids), len(events_pdf))

    total_written = 0

    for match_id in match_ids:
        match_passes = events_pdf[events_pdf["match_id"] == match_id]

        # Pull tracking for this match only (Spark-side filter)
        try:
            match_tracking = spark.table(tracking_table).filter(f"match_id = '{match_id}'").toPandas()
        except Exception:
            logger.warning("Cannot read tracking for match %s — skipping", match_id)
            continue

        if match_tracking.empty:
            continue

        logger.info("Match %s: %d passes, %d tracking frames", match_id, len(match_passes), len(match_tracking))

        results: list[dict[str, object]] = []

        for _, pass_row in match_passes.iterrows():
            event_id = str(pass_row.get("event_id", ""))
            start_frame = pass_row.get("start_frame")

            if start_frame is None or pd.isna(start_frame):
                continue

            # Get tracking data at pass frame
            frame_data = match_tracking[match_tracking["frame"] == int(start_frame)]
            if frame_data.empty:
                continue

            # Determine opponent team from event's team field
            event_team = str(pass_row.get("team", ""))
            if event_team == "Home":
                opp_json_col = "away_players"
            elif event_team == "Away":
                opp_json_col = "home_players"
            else:
                continue

            # Parse opponent positions from JSON dict
            opp_json_str = frame_data.iloc[0].get(opp_json_col)
            opp_positions = _parse_tracking_json(opp_json_str)
            if len(opp_positions) < params.min_opponents:
                continue

            # Convert Metrica 0-1 → StatsBomb 120x80 (y-flip: Metrica y=0 is top)
            opp_positions["x"] = opp_positions["x"] * 120.0
            opp_positions["y"] = (1.0 - opp_positions["y"]) * 80.0

            # Pass coordinates (also 0-1 → 120x80 with y-flip)
            start_x = float(pass_row.get("start_x", 0) or 0) * 120.0
            start_y = (1.0 - float(pass_row.get("start_y", 0) or 0)) * 80.0
            end_x = float(pass_row.get("end_x", 0) or 0) * 120.0
            end_y = (1.0 - float(pass_row.get("end_y", 0) or 0)) * 80.0

            result = detect_line_breaking(start_x, start_y, end_x, end_y, opp_positions, params)

            results.append(
                {
                    "event_id": event_id,
                    "match_id": str(match_id),
                    "is_line_breaking": result.is_line_breaking,
                    "lines_broken": result.lines_broken,
                    "line_breaking_type": result.line_breaking_type,
                    "data_source": "metrica_tracking",
                }
            )

        if results:
            result_df = pd.DataFrame(results)
            sdf = spark.createDataFrame(result_df)
            written = merge_delta_table(
                sdf,
                catalog,
                schema,
                _TABLE_NAME,
                merge_key="event_id",
                logger=logger,
            )
            total_written += written
            logger.info("Match %s: %d line-breaking results written", match_id, written)

        del match_tracking
        gc.collect()

    logger.info("Path B complete: %d rows written", total_written)
    return total_written


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Execute the line-breaking detection pipeline."""
    params = LineBreakingParams()

    path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params)
    path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params)

    total = path_a_rows + path_b_rows
    logger.info("Line-breaking pipeline complete — %d total rows written", total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for line-breaking pass detection."""
    args = parse_ingestion_args("Detect line-breaking passes from 360 and tracking data")
    logger = configure_logging("line_breaking")
    spark = get_spark_session()

    logger.info("Starting line-breaking pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
