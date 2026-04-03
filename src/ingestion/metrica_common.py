"""Shared constants, EPTS parsers, and utilities for Metrica Sports ingestion.

Contains:
  - URL constants for the Metrica open-data GitHub repository
  - Column-name sanitization regex
  - ``_safe_float`` helper
  - EPTS metadata/tracking/event parsers (used by both tracking and events modules)
"""

from __future__ import annotations

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
    gk_ids: set[str] = set()
    for player_el in metadata.findall(".//Player"):
        pid = player_el.get("id", "")
        team_id = player_el.get("teamId", "")
        shirt = player_el.findtext("ShirtNumber", "")
        position = player_el.get("PlayingPosition", "")
        player_id_to_shirt[pid] = shirt
        player_id_to_side[pid] = team_id_to_side.get(team_id, "unknown")
        if position in ("TW", "GK"):
            gk_ids.add(pid)
        elif shirt == "1" and not position:
            gk_ids.add(pid)
            logger.warning(
                "GK heuristic: assuming player %s (shirt #1) is GK (no PlayingPosition in EPTS XML)",
                pid,
            )

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
