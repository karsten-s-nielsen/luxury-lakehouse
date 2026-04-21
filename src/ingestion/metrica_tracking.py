"""Metrica Sports tracking data ingestion (Games 1-3).

Games 1-2: CSV format with 3-row multi-line header (team names, jersey
numbers, column names). Parsed with ``csv.reader`` + ``pd.read_csv``.

Game 3: FIFA EPTS format — parsed via shared EPTS utilities in
``metrica_common``.

Bronze table produced: ``metrica_tracking``
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.metrica_common import (
    _BASE_URL,
    _EPTS_URLS,
    _parse_epts_metadata,
    _parse_epts_tracking,
)
from ingestion.utils import (
    fetch_url,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Pre-compiled regex for extracting player (team, id) from tracking column names
_PLAYER_COL_RE = re.compile(r"^(Home|Away)_(.+)_x$")

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


# ---------------------------------------------------------------------------
# CSV tracking data parsing (Games 1-2)
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

    Produces names like ``Home_11_x``, ``Home_11_y`` for each tracked player,
    plus ``Period``, ``Frame``, ``Time [s]``, ``Ball_x``, ``Ball_y``.

    Metrica CSV format: each player has two columns (x, y). The jersey_row
    and team_row only populate the FIRST column of each pair; the second
    column (y) has empty strings. We track the last-seen player/team to
    assign ``_y`` to the trailing empty column.
    """
    columns: list[str] = []
    last_team: str = ""
    last_player: str = ""

    for i, col_name in enumerate(column_row):
        stripped = col_name.strip()
        jersey = jersey_row[i].strip() if i < len(jersey_row) else ""
        team = team_row[i].strip() if i < len(team_row) else ""

        if stripped in ("Period", "Frame", "Time [s]"):
            columns.append(stripped)
        elif jersey == "Ball":
            # Ball columns: first is x, second (empty jersey) is y
            last_team = "Ball"
            last_player = ""
            columns.append("Ball_x")
        elif jersey:
            # First column of a player pair -> x coordinate
            last_team = team
            last_player = jersey
            columns.append(f"{team}_{jersey}_x")
        elif last_player and not stripped:
            # Second column of a player pair -> y coordinate
            columns.append(f"{last_team}_{last_player}_y")
            last_player = ""  # Reset after y
        elif last_team == "Ball" and not stripped:
            columns.append("Ball_y")
            last_team = ""
        else:
            columns.append(f"col_{i}")
    return columns


def _reshape_tracking_to_narrow(
    df: pd.DataFrame,
    match_id: str,
) -> pd.DataFrame:
    """Reshape wide tracking data to narrow format with JSON player dicts.

    Input: one column per player coordinate (wide).
    Output: one row per frame with ``home_players`` and ``away_players`` as
    JSON strings containing ``{player_id: {x: float, y: float}}``.

    Uses vectorized pd.melt + groupby instead of iterrows for performance.
    """
    import numpy as np

    n_rows = len(df)
    col_set = set(df.columns)

    def _numeric_series(col_name: str) -> pd.Series:  # type: ignore[type-arg]
        """Extract a column as a numeric Series, coercing errors to NaN."""
        if col_name not in col_set:
            return pd.Series([None] * n_rows)
        return pd.Series(pd.to_numeric(df[col_name], errors="coerce"))

    # --- Scalar columns (vectorized) ---
    period_s = _numeric_series("Period")
    frame_s = _numeric_series("Frame")
    ts_s = _numeric_series("Time [s]")
    ball_x_s = _numeric_series("Ball_x")
    ball_y_s = _numeric_series("Ball_y")

    # Convert integer-valued floats to int for period/frame, preserve None for NaN
    def _to_opt_int(s: pd.Series) -> list[int | None]:  # type: ignore[type-arg]
        return [int(v) if pd.notna(v) else None for v in s]

    def _to_opt_float(s: pd.Series) -> list[float | None]:  # type: ignore[type-arg]
        return [float(v) if pd.notna(v) else None for v in s]

    period_list = _to_opt_int(period_s)
    frame_list = _to_opt_int(frame_s)
    ts_list = _to_opt_float(ts_s)
    ball_x_list = _to_opt_float(ball_x_s)
    ball_y_list = _to_opt_float(ball_y_s)

    # --- Player columns: identify (team, player_id, x_col, y_col) tuples ---
    _player_col_re = _PLAYER_COL_RE
    player_groups: dict[str, list[tuple[str, str, str]]] = {"Home": [], "Away": []}
    for col in df.columns:
        m = _player_col_re.match(col)
        if m:
            team, pid = m.group(1), m.group(2)
            y_col = f"{team}_{pid}_y"
            if y_col in col_set:
                player_groups[team].append((pid, col, y_col))

    # --- Build JSON player dicts using numpy arrays for fast column access ---
    def _build_player_json_column(
        groups: list[tuple[str, str, str]],
    ) -> list[str]:
        """Build a list of JSON strings, one per row, from player column groups."""
        if not groups:
            return [json.dumps({})] * n_rows

        # Extract numpy arrays for all player x/y columns at once
        x_arrays: list[np.ndarray[tuple[int], np.dtype[np.float64]]] = []
        y_arrays: list[np.ndarray[tuple[int], np.dtype[np.float64]]] = []
        pids: list[str] = []
        for pid, x_col, y_col in groups:
            pids.append(pid)
            x_s = _numeric_series(x_col)
            y_s = _numeric_series(y_col)
            x_arrays.append(np.asarray(x_s.values, dtype=np.float64))
            y_arrays.append(np.asarray(y_s.values, dtype=np.float64))

        result: list[str] = []
        for i in range(n_rows):
            players: dict[str, dict[str, float | None]] = {}
            for j, pid in enumerate(pids):
                x_val = x_arrays[j][i]
                y_val = y_arrays[j][i]
                x_f: float | None = float(x_val) if not np.isnan(x_val) else None
                y_f: float | None = float(y_val) if not np.isnan(y_val) else None
                if x_f is not None or y_f is not None:
                    players[pid] = {"x": x_f, "y": y_f}
            result.append(json.dumps(players))
        return result

    home_json = _build_player_json_column(player_groups["Home"])
    away_json = _build_player_json_column(player_groups["Away"])

    # Identify GK jersey numbers: jersey "1" heuristic for CSV games
    all_pids = [pid for team_pids in player_groups.values() for pid, _, _ in team_pids]
    gk_jerseys = sorted(pid for pid in all_pids if pid == "1")
    gk_json = json.dumps(gk_jerseys)

    # CSV path has no pitch-dim source (EPTS <FieldSize>); emit NaN for schema
    # parity with Game 3. Dense float64 NaN column prevents Spark from
    # inferring NullType, which would collide with EPTS's DoubleType on write.
    return pd.DataFrame(
        {
            "period": period_list,
            "frame": frame_list,
            "timestamp": ts_list,
            "ball_x": ball_x_list,
            "ball_y": ball_y_list,
            "home_players": home_json,
            "away_players": away_json,
            "match_id": match_id,
            "frame_rate": 25,
            "gk_jersey_numbers": gk_json,
            "pitch_length_m": np.full(n_rows, np.nan, dtype=np.float64),
            "pitch_width_m": np.full(n_rows, np.nan, dtype=np.float64),
        }
    )


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
# Ingestion orchestration
# ---------------------------------------------------------------------------


def ingest_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Download and ingest tracking data per match to avoid OOM on batch concat."""
    required_cols = [
        "period",
        "frame",
        "timestamp",
        "ball_x",
        "ball_y",
        "home_players",
        "away_players",
        "match_id",
        "frame_rate",
        "gk_jersey_numbers",
    ]

    from ingestion.utils import tolerate_missing_table

    # Incremental skip: check which matches already exist in the Delta table
    all_match_ids = list(_TRACKING_URLS.keys()) + list(_EPTS_URLS.keys())
    existing_ids: set[str] = set()
    with tolerate_missing_table(logger, "No existing metrica_tracking table — processing all matches"):
        existing_rows = spark.table(f"{catalog}.{schema}.metrica_tracking").select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}

    new_match_ids = [mid for mid in all_match_ids if mid not in existing_ids]
    logger.info(
        "%d matches total, %d already processed, %d to process",
        len(all_match_ids),
        len(all_match_ids) - len(new_match_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return

    # Games 1-2: CSV format
    for match_id, urls in _TRACKING_URLS.items():
        if match_id in existing_ids:
            logger.info("Tracking for %s already ingested — skipping", match_id)
            continue
        tracking_df = _download_and_parse_tracking(urls["home"], urls["away"], match_id, logger)
        sdf = spark.createDataFrame(tracking_df)
        row_count = validate_dataframe(sdf, required_cols, "metrica_tracking", logger)
        write_delta_table(
            sdf,
            catalog,
            schema,
            "metrica_tracking",
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
            row_count=row_count,
        )

    # Game 3: EPTS format
    for match_id, urls in _EPTS_URLS.items():
        if match_id in existing_ids:
            logger.info("Tracking for %s already ingested — skipping", match_id)
            continue
        logger.info("Downloading EPTS metadata for %s", match_id)
        metadata_resp = fetch_url(urls["metadata"])
        metadata = _parse_epts_metadata(metadata_resp.text)

        logger.info("Downloading EPTS tracking for %s", match_id)
        tracking_resp = fetch_url(urls["tracking"], timeout=(10, 120))
        rows = _parse_epts_tracking(tracking_resp.text, metadata, match_id)

        tracking_df = pd.DataFrame(rows)
        logger.info("Parsed %d EPTS tracking frames for %s", len(tracking_df), match_id)
        sdf = spark.createDataFrame(tracking_df)
        row_count = validate_dataframe(sdf, required_cols, "metrica_tracking", logger)
        write_delta_table(
            sdf,
            catalog,
            schema,
            "metrica_tracking",
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
            row_count=row_count,
        )
