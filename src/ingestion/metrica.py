"""Metrica Sports sample data ingestion into the Databricks bronze layer.

Downloads tracking and event CSV files for 2 sample games from the Metrica
Sports open-data GitHub repository (HTTPS).

Tracking data challenge:
  The CSV has a 3-row multi-line header (team names, jersey numbers, column
  names). This module parses that structure with ``csv.reader`` to extract
  the header, then reads the data with ``pd.read_csv(skiprows=3)``.

Schema reshape (tracking):
  Wide format (one column per player coordinate) → narrow JSON format:
  ``period, frame, timestamp, ball_x, ball_y, match_id,
    home_players (JSON dict), away_players (JSON dict)``

Bronze tables produced:
  - metrica_tracking
  - metrica_events
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import (
    configure_logging,
    fetch_url,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# GitHub raw URLs for Metrica open data (HTTPS only)
_BASE_URL = "https://raw.githubusercontent.com/metrica-sports/sample-data/master/data"

_TRACKING_URLS: dict[str, dict[str, str]] = {
    "Sample_Game_1": {
        "home": f"{_BASE_URL}/Sample_Game_1/Sample_Game_1_RawTrackingData_Home_Team.csv",
        "away": f"{_BASE_URL}/Sample_Game_1/Sample_Game_1_RawTrackingData_Away_Team.csv",
    },
    "Sample_Game_2": {
        "home": f"{_BASE_URL}/Sample_Game_2/Sample_Game_2_RawTrackingData_Home_Team.csv",
        "away": f"{_BASE_URL}/Sample_Game_2/Sample_Game_2_RawTrackingData_Away_Team.csv",
    },
}

_EVENT_URLS: dict[str, str] = {
    "Sample_Game_1": f"{_BASE_URL}/Sample_Game_1/Sample_Game_1_RawEventsData.csv",
    "Sample_Game_2": f"{_BASE_URL}/Sample_Game_2/Sample_Game_2_RawEventsData.csv",
}


# ---------------------------------------------------------------------------
# Tracking data parsing
# ---------------------------------------------------------------------------


def _parse_tracking_header(csv_text: str) -> tuple[list[str], list[str], list[str]]:
    """Parse the 3-row multi-line header of a Metrica tracking CSV.

    Row 0: Team names (e.g. "Home" repeated for each player column)
    Row 1: Jersey numbers / player IDs
    Row 2: Column names (x, y alternating for each player)

    Returns:
        Tuple of (team_row, jersey_row, column_row) as lists of strings.
    """
    reader = csv.reader(io.StringIO(csv_text))
    team_row = next(reader)
    jersey_row = next(reader)
    column_row = next(reader)
    return team_row, jersey_row, column_row


def _build_player_columns(
    team_row: list[str],
    jersey_row: list[str],
    column_row: list[str],
) -> list[str]:
    """Build descriptive column names from the 3-row header.

    Produces names like ``Player1_x``, ``Player1_y`` for each tracked player,
    plus ``Period``, ``Frame``, ``Time [s]``, ``Ball_x``, ``Ball_y``.
    """
    columns: list[str] = []
    for i, col_name in enumerate(column_row):
        if col_name.strip() in ("Period", "Frame", "Time [s]"):
            columns.append(col_name.strip())
        elif jersey_row[i].strip() == "Ball":
            coord = "x" if col_name.strip().lower() == "x" else "y"
            columns.append(f"Ball_{coord}")
        elif jersey_row[i].strip():
            player_id = jersey_row[i].strip()
            team = team_row[i].strip()
            coord = "x" if col_name.strip().lower() == "x" else "y"
            columns.append(f"{team}_{player_id}_{coord}")
        else:
            columns.append(f"col_{i}")
    return columns


def _safe_float(val: object) -> float | None:
    """Extract a scalar float from a pandas cell value, returning None for NaN."""
    if val is None:
        return None
    try:
        f = float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return f


def _safe_int(val: object) -> int | None:
    """Extract a scalar int from a pandas cell value, returning None for NaN."""
    f = _safe_float(val)
    return int(f) if f is not None else None


def _reshape_tracking_to_narrow(
    df: pd.DataFrame,
    match_id: str,
) -> pd.DataFrame:
    """Reshape wide tracking data to narrow format with JSON player dicts.

    Input: one column per player coordinate (wide).
    Output: one row per frame with ``home_players`` and ``away_players`` as
    JSON strings containing ``{player_id: {x: float, y: float}}``.
    """
    rows: list[dict[str, object]] = []
    col_set = set(df.columns)

    for _, row in df.iterrows():
        home_players: dict[str, dict[str, float | None]] = {}
        away_players: dict[str, dict[str, float | None]] = {}

        for col in df.columns:
            if col.startswith("Home_") and col.endswith("_x"):
                pid = col.replace("Home_", "").replace("_x", "")
                y_col = f"Home_{pid}_y"
                x_val = _safe_float(row[col])
                y_val = _safe_float(row[y_col]) if y_col in col_set else None
                if x_val is not None or y_val is not None:
                    home_players[pid] = {"x": x_val, "y": y_val}

            elif col.startswith("Away_") and col.endswith("_x"):
                pid = col.replace("Away_", "").replace("_x", "")
                y_col = f"Away_{pid}_y"
                x_val = _safe_float(row[col])
                y_val = _safe_float(row[y_col]) if y_col in col_set else None
                if x_val is not None or y_val is not None:
                    away_players[pid] = {"x": x_val, "y": y_val}

        rows.append(
            {
                "period": _safe_int(row.get("Period")),
                "frame": _safe_int(row.get("Frame")),
                "timestamp": _safe_float(row.get("Time [s]")),
                "ball_x": _safe_float(row.get("Ball_x")) if "Ball_x" in col_set else None,
                "ball_y": _safe_float(row.get("Ball_y")) if "Ball_y" in col_set else None,
                "home_players": json.dumps(home_players),
                "away_players": json.dumps(away_players),
                "match_id": match_id,
            }
        )

    return pd.DataFrame(rows)


def _download_and_parse_tracking(
    home_url: str,
    away_url: str,
    match_id: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Download home + away tracking CSVs, merge, and reshape to narrow format."""
    logger.info("Downloading tracking data for %s", match_id)

    # Download and parse home tracking
    home_resp = fetch_url(home_url)
    home_text = home_resp.text
    team_row, jersey_row, column_row = _parse_tracking_header(home_text)
    home_columns = _build_player_columns(team_row, jersey_row, column_row)
    home_df = pd.read_csv(io.StringIO(home_text), skiprows=3, header=None, names=home_columns)

    # Download and parse away tracking
    away_resp = fetch_url(away_url)
    away_text = away_resp.text
    away_team_row, away_jersey_row, away_column_row = _parse_tracking_header(away_text)
    away_columns = _build_player_columns(away_team_row, away_jersey_row, away_column_row)
    away_df = pd.read_csv(io.StringIO(away_text), skiprows=3, header=None, names=away_columns)

    # Merge on frame-level columns
    merge_cols = ["Period", "Frame", "Time [s]"]
    merged = home_df.merge(away_df, on=merge_cols, how="outer", suffixes=("", "_away"))

    # Use home ball coordinates, fall back to away if missing
    if "Ball_x_away" in merged.columns:
        merged["Ball_x"] = merged["Ball_x"].fillna(merged["Ball_x_away"])
        merged["Ball_y"] = merged["Ball_y"].fillna(merged["Ball_y_away"])
        merged = merged.drop(columns=["Ball_x_away", "Ball_y_away"])

    # Reshape to narrow JSON format
    narrow_df = _reshape_tracking_to_narrow(merged, match_id)
    logger.info("Parsed %d tracking frames for %s", len(narrow_df), match_id)
    return narrow_df


# ---------------------------------------------------------------------------
# Event data parsing
# ---------------------------------------------------------------------------


def _download_and_parse_events(
    url: str,
    match_id: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Download events CSV and normalize column names."""
    logger.info("Downloading event data for %s", match_id)
    resp = fetch_url(url)
    df = pd.read_csv(io.StringIO(resp.text))

    # Rename columns to match dbt source expectations
    rename_map: dict[str, str] = {
        "Event Name": "type",
        "Event Type": "type",
        "Type": "type",
        "Sub Type": "subtype",
        "Sub Event": "subtype",
        "Period": "period",
        "Start Frame": "start_frame",
        "End Frame": "end_frame",
        "Start X": "start_x",
        "Start Y": "start_y",
        "End X": "end_x",
        "End Y": "end_y",
        "From": "player",
        "Player": "player",
        "Team": "team",
        "Start Time [s]": "start_time_s",
        "End Time [s]": "end_time_s",
    }

    # Apply renames only for columns that exist
    actual_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=actual_renames)

    # Sanitize remaining column names: Delta Lake rejects spaces and special chars
    df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", col).strip("_").lower() for col in df.columns]

    # Ensure event_id exists
    if "event_id" not in df.columns:
        df["event_id"] = range(1, len(df) + 1)

    df["match_id"] = match_id

    logger.info("Parsed %d events for %s", len(df), match_id)
    return df


# ---------------------------------------------------------------------------
# Ingestion orchestration
# ---------------------------------------------------------------------------


def ingest_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Download and ingest tracking data for all sample games."""
    all_tracking: list[pd.DataFrame] = []

    for match_id, urls in _TRACKING_URLS.items():
        tracking_df = _download_and_parse_tracking(urls["home"], urls["away"], match_id, logger)
        all_tracking.append(tracking_df)

    if all_tracking:
        combined = pd.concat(all_tracking, ignore_index=True)
        sdf = spark.createDataFrame(combined)
        validate_dataframe(
            sdf,
            ["period", "frame", "timestamp", "ball_x", "ball_y", "home_players", "away_players", "match_id"],
            "metrica_tracking",
            logger,
        )
        write_delta_table(sdf, catalog, schema, "metrica_tracking", mode="overwrite", logger=logger)


def ingest_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Download and ingest event data for all sample games."""
    all_events: list[pd.DataFrame] = []

    for match_id, url in _EVENT_URLS.items():
        events_df = _download_and_parse_events(url, match_id, logger)
        all_events.append(events_df)

    if all_events:
        combined = pd.concat(all_events, ignore_index=True)
        sdf = spark.createDataFrame(combined)
        validate_dataframe(
            sdf,
            ["event_id", "type", "period", "start_frame", "end_frame", "team", "player", "match_id"],
            "metrica_events",
            logger,
        )
        write_delta_table(sdf, catalog, schema, "metrica_events", mode="overwrite", logger=logger)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for Metrica Sports ingestion."""
    args = parse_ingestion_args("Ingest Metrica Sports sample data into the bronze layer")
    logger = configure_logging("metrica")
    spark = get_spark_session()

    logger.info("Starting Metrica ingestion into %s.%s", args.catalog, args.schema)

    ingest_tracking(spark, args.catalog, args.schema, logger)
    ingest_events(spark, args.catalog, args.schema, logger)

    logger.info("Metrica ingestion complete")


if __name__ == "__main__":
    main()
