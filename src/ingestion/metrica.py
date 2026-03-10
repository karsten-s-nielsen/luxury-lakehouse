"""Metrica Sports sample data ingestion into the Databricks bronze layer.

Downloads tracking and event data for 3 sample games from the Metrica
Sports open-data GitHub repository (HTTPS).

Games 1-2: CSV format with 3-row multi-line header (team names, jersey
numbers, column names). Parsed with ``csv.reader`` + ``pd.read_csv``.

Game 3: FIFA EPTS format with XML metadata (player roster, frame layout,
substitution sections), colon-delimited tracking, and JSON events.

Schema reshape (tracking):
  Wide/EPTS format → narrow JSON format:
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
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING, NamedTuple

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

# Pre-compiled regex for sanitizing DataFrame column names (Delta Lake rejects spaces/special chars)
_COLUMN_CLEAN_RE = re.compile(r"[^a-zA-Z0-9_]")

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

# Game 3 uses FIFA EPTS format (XML metadata + colon-delimited tracking + JSON events)
_EPTS_URLS: dict[str, dict[str, str]] = {
    "Sample_Game_3": {
        "metadata": f"{_BASE_URL}/Sample_Game_3/Sample_Game_3_metadata.xml",
        "tracking": f"{_BASE_URL}/Sample_Game_3/Sample_Game_3_tracking.txt",
        "events": f"{_BASE_URL}/Sample_Game_3/Sample_Game_3_events.json",
    },
}


# ---------------------------------------------------------------------------
# EPTS metadata and parser types
# ---------------------------------------------------------------------------


class _EPTSMetadata(NamedTuple):
    """Parsed FIFA EPTS metadata for Game 3."""

    first_half: tuple[int, int]
    second_half: tuple[int, int]
    frame_rate: int
    # Ordered player channel prefixes per frame-range section
    # [(start_frame, end_frame, ["player1", "player2", ...])]
    data_format_specs: list[tuple[int, int, list[str]]]
    # Mappings: channel prefix → player_id → shirt number / team side
    channel_to_player_id: dict[str, str]
    player_id_to_shirt: dict[str, str]
    player_id_to_side: dict[str, str]


def _parse_epts_metadata(xml_text: str) -> _EPTSMetadata:
    """Parse FIFA EPTS metadata XML to extract player mapping and frame layout.

    Extracts half boundaries, player-to-team mapping, player channel ordering,
    and DataFormatSpecification sections that define which players appear in
    each slot (handling substitutions).
    """
    root = ET.fromstring(xml_text)  # noqa: S314
    metadata = root.find("Metadata")
    if metadata is None:
        msg = "Missing <Metadata> element in EPTS XML"
        raise ValueError(msg)

    # --- GlobalConfig: half boundaries and frame rate ---
    global_config = metadata.find("GlobalConfig")
    if global_config is None:
        msg = "Missing <GlobalConfig> element"
        raise ValueError(msg)

    frame_rate = int(global_config.findtext("FrameRate", "25"))
    provider_params: dict[str, str] = {}
    for param in global_config.findall(".//ProviderParameter"):
        name = param.findtext("Name", "")
        value = param.findtext("Value", "")
        if name and value:
            provider_params[name] = value

    required_params = ("first_half_start", "first_half_end", "second_half_start", "second_half_end")
    missing = [k for k in required_params if k not in provider_params]
    if missing:
        msg = f"EPTS metadata missing required ProviderParameters: {missing}"
        raise ValueError(msg)

    first_half = (int(provider_params["first_half_start"]), int(provider_params["first_half_end"]))
    second_half = (int(provider_params["second_half_start"]), int(provider_params["second_half_end"]))

    # --- Teams: determine home/away from local/visiting ---
    score_el = metadata.find(".//Score")
    local_team_id = score_el.get("idLocalTeam", "") if score_el is not None else ""
    team_id_to_side: dict[str, str] = {}
    for team_el in metadata.findall(".//Team"):
        tid = team_el.get("id", "")
        team_id_to_side[tid] = "home" if tid == local_team_id else "away"

    # --- Players: build player_id → shirt number and team side ---
    player_id_to_shirt: dict[str, str] = {}
    player_id_to_side: dict[str, str] = {}
    for player_el in metadata.findall(".//Player"):
        pid = player_el.get("id", "")
        team_id = player_el.get("teamId", "")
        shirt = player_el.findtext("ShirtNumber", "")
        player_id_to_shirt[pid] = shirt
        player_id_to_side[pid] = team_id_to_side.get(team_id, "unknown")

    # --- PlayerChannels: map channel prefix → player_id ---
    # Channels come in pairs (player1_x, player1_y) — extract the prefix
    channel_to_player_id: dict[str, str] = {}
    for pc_el in metadata.findall(".//PlayerChannel"):
        channel_id = pc_el.get("id", "")
        player_id = pc_el.get("playerId", "")
        # Extract prefix: "player1_x" → "player1"
        prefix = channel_id.rsplit("_", 1)[0]
        channel_to_player_id[prefix] = player_id

    # --- DataFormatSpecifications: ordered player slots per frame range ---
    data_format_specs: list[tuple[int, int, list[str]]] = []
    dfs_root = root.find("DataFormatSpecifications")
    if dfs_root is None:
        msg = "Missing <DataFormatSpecifications> element"
        raise ValueError(msg)

    for spec_el in dfs_root.findall("DataFormatSpecification"):
        start_frame = int(spec_el.get("startFrame", "0"))
        end_frame = int(spec_el.get("endFrame", "0"))

        # Extract ordered player channel prefixes from inner SplitRegisters
        # Structure: outer SplitRegister (;) > inner SplitRegisters (,) > PlayerChannelRef
        outer_split = spec_el.findall("SplitRegister")
        if len(outer_split) < 2:
            continue
        player_split = outer_split[0]  # Player positions section

        ordered_prefixes: list[str] = []
        for inner_split in player_split.findall("SplitRegister"):
            refs = inner_split.findall("PlayerChannelRef")
            if refs:
                # Take the first ref's ID prefix (both _x and _y share it)
                channel_id = refs[0].get("playerChannelId", "")
                prefix = channel_id.rsplit("_", 1)[0]
                ordered_prefixes.append(prefix)

        data_format_specs.append((start_frame, end_frame, ordered_prefixes))

    return _EPTSMetadata(
        first_half=first_half,
        second_half=second_half,
        frame_rate=frame_rate,
        data_format_specs=data_format_specs,
        channel_to_player_id=channel_to_player_id,
        player_id_to_shirt=player_id_to_shirt,
        player_id_to_side=player_id_to_side,
    )


def _parse_epts_tracking(
    tracking_text: str,
    metadata: _EPTSMetadata,
    match_id: str,
) -> list[dict[str, object]]:
    """Parse EPTS colon-delimited tracking file into narrow-format rows.

    Line format: ``frame:p1x,p1y;p2x,p2y;...;p22x,p22y:ballx,bally``

    Coordinates are already [0,1] normalized, matching Games 1-2 convention.
    Output schema is identical to ``_reshape_tracking_to_narrow()`` output.
    """
    rows: list[dict[str, object]] = []

    for line in tracking_text.splitlines():
        line = line.strip()
        if not line:
            continue

        parts = line.split(":")
        if len(parts) < 3:
            continue

        frame = int(parts[0])

        # Determine period from half boundaries
        if metadata.first_half[0] <= frame <= metadata.first_half[1]:
            period = 1
            timestamp = (frame - metadata.first_half[0]) / metadata.frame_rate
        elif metadata.second_half[0] <= frame <= metadata.second_half[1]:
            period = 2
            timestamp = (frame - metadata.second_half[0]) / metadata.frame_rate
        else:
            continue  # Skip frames outside known halves

        # Find the matching DataFormatSpecification for this frame
        active_spec: list[str] | None = None
        for spec_start, spec_end, prefixes in metadata.data_format_specs:
            if spec_start <= frame <= spec_end:
                active_spec = prefixes
                break
        if active_spec is None:
            continue

        # Parse player positions
        player_entries = parts[1].split(";")
        home_players: dict[str, dict[str, float | None]] = {}
        away_players: dict[str, dict[str, float | None]] = {}

        for i, entry in enumerate(player_entries):
            if i >= len(active_spec):
                break
            coords = entry.split(",")
            if len(coords) < 2:
                continue

            x_val = _safe_float(coords[0])
            y_val = _safe_float(coords[1])
            if x_val is None and y_val is None:
                continue

            prefix = active_spec[i]
            player_id = metadata.channel_to_player_id.get(prefix, prefix)
            shirt = metadata.player_id_to_shirt.get(player_id, player_id)
            side = metadata.player_id_to_side.get(player_id, "unknown")

            player_data = {"x": x_val, "y": y_val}
            if side == "home":
                home_players[shirt] = player_data
            else:
                away_players[shirt] = player_data

        # Parse ball coordinates
        ball_coords = parts[2].split(",")
        ball_x = _safe_float(ball_coords[0]) if len(ball_coords) >= 1 else None
        ball_y = _safe_float(ball_coords[1]) if len(ball_coords) >= 2 else None
        rows.append(
            {
                "period": period,
                "frame": frame,
                "timestamp": round(timestamp, 4),
                "ball_x": ball_x,
                "ball_y": ball_y,
                "home_players": json.dumps(home_players),
                "away_players": json.dumps(away_players),
                "match_id": match_id,
                "frame_rate": 25,
            }
        )

    return rows


def _parse_epts_events(
    events_data: list[dict[str, object]],
    match_id: str,
) -> pd.DataFrame:
    """Flatten EPTS JSON events to match Games 1-2 bronze schema.

    Input is the ``data`` array from the events JSON file. Each event has
    nested objects for team, type, subtypes, start/end, from/to.
    """
    _team_map = {"Team A": "Home", "Team B": "Away"}
    rows: list[dict[str, object]] = []

    for event in events_data:
        event_type = event.get("type") or {}
        event_subtypes = event.get("subtypes")
        event_team = event.get("team") or {}
        event_from = event.get("from") or {}
        event_start = event.get("start") or {}
        event_end = event.get("end") or {}

        team_name: str = event_team.get("name", "") if isinstance(event_team, dict) else ""  # type: ignore[union-attr]
        type_name: str = event_type.get("name", "") if isinstance(event_type, dict) else ""  # type: ignore[union-attr]

        subtype_name: str | None = None
        if isinstance(event_subtypes, dict):
            subtype_name = event_subtypes.get("name")  # type: ignore[assignment]

        from_id: str | None = None
        if isinstance(event_from, dict):
            from_id = event_from.get("name")  # type: ignore[assignment]

        start_dict = event_start if isinstance(event_start, dict) else {}
        end_dict = event_end if isinstance(event_end, dict) else {}

        rows.append(
            {
                "event_id": event.get("index"),
                "type": type_name,
                "subtype": subtype_name,
                "period": event.get("period"),
                "start_frame": start_dict.get("frame"),
                "start_time_s": start_dict.get("time"),
                "start_x": start_dict.get("x"),
                "start_y": start_dict.get("y"),
                "end_frame": end_dict.get("frame"),
                "end_time_s": end_dict.get("time"),
                "end_x": end_dict.get("x"),
                "end_y": end_dict.get("y"),
                "team": _team_map.get(team_name, team_name),
                "player": from_id,
                "match_id": match_id,
            }
        )

    return pd.DataFrame(rows)


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
            # First column of a player pair → x coordinate
            last_team = team
            last_player = jersey
            columns.append(f"{team}_{jersey}_x")
        elif last_player and not stripped:
            # Second column of a player pair → y coordinate
            columns.append(f"{last_team}_{last_player}_y")
            last_player = ""  # Reset after y
        elif last_team == "Ball" and not stripped:
            columns.append("Ball_y")
            last_team = ""
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
                "frame_rate": 25,
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
    df.columns = [_COLUMN_CLEAN_RE.sub("_", col).strip("_").lower() for col in df.columns]

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
    ]

    # Games 1-2: CSV format
    for match_id, urls in _TRACKING_URLS.items():
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


def ingest_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Download and ingest event data per match to avoid OOM on batch concat."""
    required_cols = ["event_id", "type", "period", "start_frame", "end_frame", "team", "player", "match_id"]

    # Games 1-2: CSV format
    for match_id, url in _EVENT_URLS.items():
        events_df = _download_and_parse_events(url, match_id, logger)
        sdf = spark.createDataFrame(events_df)
        row_count = validate_dataframe(sdf, required_cols, "metrica_events", logger)
        write_delta_table(
            sdf,
            catalog,
            schema,
            "metrica_events",
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
            row_count=row_count,
        )

    # Game 3: EPTS JSON format
    for match_id, urls in _EPTS_URLS.items():
        logger.info("Downloading EPTS events for %s", match_id)
        resp = fetch_url(urls["events"])
        events_json = resp.json()
        events_data: list[dict[str, object]] = events_json.get("data", events_json)
        events_df = _parse_epts_events(events_data, match_id)
        logger.info("Parsed %d EPTS events for %s", len(events_df), match_id)
        sdf = spark.createDataFrame(events_df)
        row_count = validate_dataframe(sdf, required_cols, "metrica_events", logger)
        write_delta_table(
            sdf,
            catalog,
            schema,
            "metrica_events",
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
            row_count=row_count,
        )


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
