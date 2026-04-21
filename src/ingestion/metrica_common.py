"""Shared constants, EPTS parsers, and utilities for Metrica Sports ingestion.

Contains:
  - URL constants for the Metrica open-data GitHub repository
  - Column-name sanitization regex
  - ``_safe_float`` helper
  - EPTS metadata/tracking/event parsers (used by both tracking and events modules)
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import xml.etree.ElementTree as ET  # nosemgrep: use-defused-xml -- trusted local Metrica EPTS XML, not untrusted input
from typing import NamedTuple

import pandas as pd

# Pre-compiled regex for sanitizing DataFrame column names (Delta Lake rejects spaces/special chars)
_COLUMN_CLEAN_RE = re.compile(r"[^a-zA-Z0-9_]")

# GitHub raw URLs for Metrica open data (HTTPS only)
_BASE_URL = "https://raw.githubusercontent.com/metrica-sports/sample-data/master/data"

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
    # Mappings: channel prefix -> player_id -> shirt number / team side
    channel_to_player_id: dict[str, str]
    player_id_to_shirt: dict[str, str]
    player_id_to_side: dict[str, str]
    # Goalkeeper player IDs (immutable; checked via PlayingPosition, fallback jersey #1)
    gk_player_ids: frozenset[str]
    # Pitch dimensions in meters, from the EPTS <FieldSize> element.
    # None when the metadata doesn't carry the element (shouldn't happen for
    # conforming EPTS files but kept as None for graceful fallback).
    pitch_length_m: float | None
    pitch_width_m: float | None
    # Per-player attributes from the metadata <Player> elements.
    # Captured beyond the GK flag so bronze carries every source field per the
    # bronze-completeness principle (see memory `feedback_bronze_completeness_principle`).
    # Keyed on player_id (DFL-style or EPTS-native IDs).
    player_id_to_position: dict[str, str]  # raw PlayingPosition string ("", "TW", "GK", "CB", etc.)
    player_id_to_jersey: dict[str, str]  # same as `player_id_to_shirt` but exported for downstream use


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

    # --- Players: build player_id -> shirt number and team side, identify GKs ---
    logger = logging.getLogger("metrica")
    player_id_to_shirt: dict[str, str] = {}
    player_id_to_side: dict[str, str] = {}
    player_id_to_position: dict[str, str] = {}
    gk_ids: set[str] = set()
    for player_el in metadata.findall(".//Player"):
        pid = player_el.get("id", "")
        team_id = player_el.get("teamId", "")
        shirt = player_el.findtext("ShirtNumber", "")
        position = player_el.get("PlayingPosition", "")
        player_id_to_shirt[pid] = shirt
        player_id_to_side[pid] = team_id_to_side.get(team_id, "unknown")
        player_id_to_position[pid] = position  # Retain raw string for bronze (CB / LB / RM / TW / etc.)
        if position in ("TW", "GK"):
            gk_ids.add(pid)
        elif shirt == "1" and not position:
            gk_ids.add(pid)
            logger.warning(
                "GK heuristic: assuming player %s (shirt #1) is GK (no PlayingPosition in EPTS XML)",
                pid,
            )

    # --- Pitch dimensions: from <FieldSize> element ---
    # EPTS source typically exposes `Width` + `Length` attributes in meters.
    # Captured here because pitch dims are required to scale Metrica's [0, 1]
    # normalised coords to real-world distance, and previously this section
    # of the metadata was never read — see bronze-gap audit 2026-04-20.
    pitch_length_m: float | None = None
    pitch_width_m: float | None = None
    field_size_el = metadata.find(".//FieldSize")
    if field_size_el is not None:
        length_val = field_size_el.get("Length") or field_size_el.findtext("Length")
        width_val = field_size_el.get("Width") or field_size_el.findtext("Width")
        with contextlib.suppress(TypeError, ValueError):
            if length_val is not None:
                pitch_length_m = float(length_val)
        with contextlib.suppress(TypeError, ValueError):
            if width_val is not None:
                pitch_width_m = float(width_val)

    # --- PlayerChannels: map channel prefix -> player_id ---
    # Channels come in pairs (player1_x, player1_y) -- extract the prefix
    channel_to_player_id: dict[str, str] = {}
    for pc_el in metadata.findall(".//PlayerChannel"):
        channel_id = pc_el.get("id", "")
        player_id = pc_el.get("playerId", "")
        # Extract prefix: "player1_x" -> "player1"
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
        gk_player_ids=frozenset(gk_ids),
        pitch_length_m=pitch_length_m,
        pitch_width_m=pitch_width_m,
        player_id_to_position=player_id_to_position,
        player_id_to_jersey=dict(player_id_to_shirt),
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

    # Pre-compute GK shirt numbers from metadata player IDs
    gk_shirts: list[str] = sorted(
        metadata.player_id_to_shirt[pid] for pid in metadata.gk_player_ids if pid in metadata.player_id_to_shirt
    )
    gk_json = json.dumps(gk_shirts)

    # Denormalize match-level pitch dims onto every row so a single consumer
    # query can access them without joining to a separate metadata table.
    # Coerce None → NaN so the emitted column is dense float64 — otherwise
    # Spark would infer NullType on the first all-None column and collide
    # with the CSV-path Games-1-2 writes that use float NaN. See
    # `metrica_tracking._reshape_tracking_to_narrow` for the mirror.
    pitch_length_m_val: float = float("nan") if metadata.pitch_length_m is None else metadata.pitch_length_m
    pitch_width_m_val: float = float("nan") if metadata.pitch_width_m is None else metadata.pitch_width_m

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
                "gk_jersey_numbers": gk_json,
                "pitch_length_m": pitch_length_m_val,
                "pitch_width_m": pitch_width_m_val,
            }
        )

    return rows


def _parse_epts_events(
    events_data: list[dict[str, object]],
    match_id: str,
    metadata: _EPTSMetadata | None = None,
) -> pd.DataFrame:
    """Flatten EPTS JSON events to match Games 1-2 bronze schema.

    Input is the ``data`` array from the events JSON file. Each event has
    nested objects for team, type, subtypes, start/end, from/to.

    When ``metadata`` is provided, event rows carry the match-level pitch
    dimensions (``pitch_length_m`` / ``pitch_width_m``) for schema parity
    with Games 1-2 CSV events (which emit NaN). Pass ``None`` when metadata
    is unavailable — pitch dim columns are then NaN on every row.
    """
    _team_map = {"Team A": "Home", "Team B": "Away"}
    # Pre-compute match-level pitch dims once (constant across events).
    # Coerce None → NaN so the emitted column is dense float64 even when
    # metadata is missing or lacks a <FieldSize> element — otherwise Spark
    # would infer NullType and collide with the CSV-path Float64 column.
    pitch_length_m_val: float = (
        float("nan") if metadata is None or metadata.pitch_length_m is None else metadata.pitch_length_m
    )
    pitch_width_m_val: float = (
        float("nan") if metadata is None or metadata.pitch_width_m is None else metadata.pitch_width_m
    )
    rows: list[dict[str, object]] = []

    for event in events_data:
        event_type = event.get("type") or {}
        event_subtypes = event.get("subtypes")
        event_team = event.get("team") or {}
        event_from = event.get("from") or {}
        event_to = event.get("to") or {}  # Bronze gap fix (2026-04-20): EPTS path was never reading `to`.
        event_start = event.get("start") or {}
        event_end = event.get("end") or {}

        team_name: str = event_team.get("name", "") if isinstance(event_team, dict) else ""  # type: ignore[union-attr]
        type_name: str = event_type.get("name", "") if isinstance(event_type, dict) else ""  # type: ignore[union-attr]

        # Subtypes may be either a dict (single subtype) or a list of dicts
        # (multi-subtype events like INTERCEPTION+GOAL). Previously only the
        # dict case was handled; list-case events silently lost all but the
        # first. Now we canonicalize to a JSON-stringified list and keep the
        # first name as the scalar `subtype` for back-compat.
        subtype_name: str | None = None
        subtypes_all_json: str | None = None
        if isinstance(event_subtypes, dict):
            subtype_name = event_subtypes.get("name")  # type: ignore[assignment]
            if subtype_name:
                subtypes_all_json = json.dumps([subtype_name])
        elif isinstance(event_subtypes, list):
            names = [s.get("name") for s in event_subtypes if isinstance(s, dict) and s.get("name")]
            if names:
                subtype_name = names[0]
                subtypes_all_json = json.dumps(names)

        from_id: str | None = None
        if isinstance(event_from, dict):
            from_id = event_from.get("name")  # type: ignore[assignment]
        to_id: str | None = None
        if isinstance(event_to, dict):
            to_id = event_to.get("name")  # type: ignore[assignment]

        start_dict = event_start if isinstance(event_start, dict) else {}
        end_dict = event_end if isinstance(event_end, dict) else {}

        rows.append(
            {
                "event_id": event.get("index"),
                "type": type_name,
                "subtype": subtype_name,
                "subtypes_all_json": subtypes_all_json,
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
                "to": to_id,
                "match_id": match_id,
                "pitch_length_m": pitch_length_m_val,
                "pitch_width_m": pitch_width_m_val,
            }
        )

    return pd.DataFrame(rows)


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
