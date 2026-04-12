"""IDSSE (Bundesliga) tracking and event data ingestion into the Databricks bronze layer.

Reads pre-downloaded DFL position and event XML data for 7 Bundesliga matches
from a UC Volume (originally sourced from the IDSSE figshare collection).
Parses the XML directly using xml.etree.ElementTree to produce narrow format
(one row per player per frame / one row per event) for the bronze layer.

Data source:
  Bassek et al. "An integrated dataset of spatiotemporal and event data in
  elite soccer." Scientific Data, Nature (2025). CC-BY 4.0.
  https://figshare.com/collections/DFL_-_Bundesliga_Data_Shootout/5830772

Bronze tables produced:
  - idsse_tracking (narrow format: one row per player per frame)
  - idsse_events (one row per event with position data)

Coordinate systems (preserved in bronze):
  Tracking (DFL_04_03): center-origin meters, x in (-52.5, 52.5), y in (-34, 34).
  Events (DFL_03_02):   pitch-origin meters, x in (0, 105), y in (0, 68).
  Staging layer transforms both to the shared 120x80 coordinate system.
"""

from __future__ import annotations

import gc
import logging
import math
import re
import xml.etree.ElementTree as ET  # nosemgrep: use-defused-xml -- trusted local DFL XML files, not untrusted input
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from ingestion.utils import SparkAnalysisException as _SparkAnalysisException


class _IdsseGuard:
    workflow_id = "wf-idsse"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip if all IDSSE matches are already ingested."""
        expected = len(IDSSE_MATCH_IDS)
        try:
            t_count = spark.table(f"{catalog}.{schema}.idsse_tracking").select("match_id").distinct().count()
            e_count = spark.table(f"{catalog}.{schema}.idsse_events").select("match_id").distinct().count()
            if t_count >= expected and e_count >= expected:
                return FilterResult(workflow_id=self.workflow_id, count=0)
        except Exception:  # noqa: S110
            pass
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _IdsseGuard()

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

# Pre-compiled regex for DFL event XML filenames (DFL_03_02 series)
_EVENT_FILE_RE = re.compile(r"DFL_03_02_.*DFL-MAT-([A-Za-z0-9]+)\.xml$")

# Player attribute lookup order per event child tag.
# For most event types, the primary actor is in the ``Player`` attribute.
# TacklingGame uses ``Winner`` as the primary actor.
_PLAYER_ATTR_ORDER: dict[str, list[str]] = {
    "TacklingGame": ["Winner", "Player"],
}
_DEFAULT_PLAYER_ATTRS: list[str] = ["Player"]

# Team attribute lookup order per event child tag.
_TEAM_ATTR_ORDER: dict[str, list[str]] = {
    "TacklingGame": ["WinnerTeam", "Team"],
}
_DEFAULT_TEAM_ATTRS: list[str] = ["Team"]


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


def _parse_teams(info_path: str) -> tuple[str, str, dict[str, str], set[str]]:
    """Parse match info XML to get home/away team IDs, player-to-team mapping, and GK IDs.

    Args:
        info_path: Path to match info XML file.

    Returns:
        Tuple of (home_team_id, away_team_id, {person_id: "home"|"away"}, gk_player_ids).
        ``gk_player_ids`` contains PersonIds of players with ``PlayingPosition="TW"``
        (DFL standard for Torwart/goalkeeper).
    """
    tree = ET.parse(info_path)  # noqa: S314  # nosemgrep: use-defused-xml-parse
    root = tree.getroot()

    home_team_id = ""
    away_team_id = ""
    player_team_map: dict[str, str] = {}
    gk_player_ids: set[str] = set()

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
                if player_el.get("PlayingPosition") == "TW":
                    gk_player_ids.add(person_id)

    return home_team_id, away_team_id, player_team_map, gk_player_ids


def _parse_positions_xml(
    pos_path: str,
    player_team_map: dict[str, str],
    match_id: str,
    logger: logging.Logger,
    gk_player_ids: set[str] | None = None,
) -> dict[int, list[dict[str, object]]]:
    """Parse DFL position XML into narrow-format row dicts, split by period.

    Uses single-pass iterative XML parsing to avoid loading the entire ~400MB
    file into memory at once. Each FrameSet contains frames for one person in
    one half. Ball FrameSets appear before player FrameSets in the DFL XML
    format, so ball coordinates are available for lookup when player frames are
    processed. If a ball coordinate is not yet available for a given frame, the
    lookup returns None — graceful degradation identical to missing ball data.

    Returns rows grouped by period so callers can process and release each
    half independently, halving peak DataFrame memory.

    Args:
        pos_path: Path to position XML file.
        player_team_map: Mapping of PersonId to "home"/"away".
        match_id: Match identifier to embed in each row.
        logger: Logger instance.
        gk_player_ids: Set of PersonIds identified as goalkeepers. When provided,
            each tracking row includes an ``is_goalkeeper`` boolean field.

    Returns:
        Mapping of period number → list of row dicts.
    """
    rows_by_period: dict[int, list[dict[str, object]]] = {1: [], 2: []}
    prefixed_match_id = f"idsse_{match_id}"

    # Ball frames per (period, frame_n) for lookup — populated as ball FrameSets are encountered
    ball_coords: dict[tuple[int, int], tuple[float, float]] = {}
    ball_miss_count = 0  # Track player frames where ball lookup returned None

    # Single pass: ball FrameSets populate ball_coords, player FrameSets emit rows
    for _event, elem in ET.iterparse(pos_path, events=("end",)):  # noqa: S314
        if elem.tag != "FrameSet":
            continue

        team_id = elem.get("TeamId", "")
        team_id_lower = team_id.lower()

        # Skip referee FrameSets
        if team_id_lower == "referee":
            elem.clear()
            continue

        section = elem.get("GameSection", "")
        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            elem.clear()
            continue

        if team_id_lower == "ball":
            # Collect ball coordinates
            for frame_el in elem.iter("Frame"):
                n = int(frame_el.get("N", "0"))
                x_str = frame_el.get("X", "")
                y_str = frame_el.get("Y", "")
                if x_str and y_str:
                    bx = float(x_str)
                    by = float(y_str)
                    if not (math.isnan(bx) or math.isnan(by)):
                        ball_coords[(period, n)] = (round(bx, 4), round(by, 4))
        else:
            # Player FrameSet — emit tracking rows
            person_id = elem.get("PersonId", "")
            team_label = player_team_map.get(person_id, "unknown")
            period_rows = rows_by_period[period]

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
                if ball is None:
                    ball_miss_count += 1
                ball_x = ball[0] if ball else None
                ball_y = ball[1] if ball else None

                row: dict[str, object] = {
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
                if gk_player_ids is not None:
                    row["is_goalkeeper"] = person_id in gk_player_ids
                period_rows.append(row)

        elem.clear()

    logger.info("Parsed %d ball frames for match %s", len(ball_coords), match_id)
    if ball_miss_count > 0 and len(ball_coords) > 0:
        total_player_frames = sum(len(rows) for rows in rows_by_period.values())
        logger.warning(
            "Ball coordinate lookup missed %d of %d player frames for match %s — "
            "possible ball-after-player FrameSet ordering in XML",
            ball_miss_count,
            total_player_frames,
            match_id,
        )

    return rows_by_period


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

    # Incremental skip: check which matches already exist in the Delta table
    existing_ids: set[str] = set()
    try:
        existing_rows = spark.table(f"{catalog}.{schema}.idsse_tracking").select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing idsse_tracking table — processing all matches")

    new_match_ids = [mid for mid in ids_to_ingest if f"idsse_{mid}" not in existing_ids]
    logger.info(
        "%d matches total, %d already processed, %d to process",
        len(ids_to_ingest),
        len(ids_to_ingest) - len(new_match_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return

    for i, mid in enumerate(new_match_ids):
        logger.info("Parsing IDSSE match %s (%d/%d)", mid, i + 1, len(new_match_ids))

        comp = _MATCH_COMPETITION[mid]
        info_path = f"{data_dir}/DFL_02_01_matchinformation_{comp}_DFL-MAT-{mid}.xml"
        pos_path = f"{data_dir}/DFL_04_03_positions_raw_observed_{comp}_DFL-MAT-{mid}.xml"

        _home_id, _away_id, player_team_map, gk_player_ids = _parse_teams(info_path)
        logger.info("Found %d players in match info (%d GKs)", len(player_team_map), len(gk_player_ids))

        rows_by_period = _parse_positions_xml(pos_path, player_team_map, mid, logger, gk_player_ids=gk_player_ids)
        total_rows = sum(len(r) for r in rows_by_period.values())
        logger.info("Parsed %d tracking rows for IDSSE match %s", total_rows, mid)

        # Process each half independently to halve peak DataFrame memory
        for period, period_rows in rows_by_period.items():
            if not period_rows:
                continue
            df = pd.DataFrame(period_rows)
            del period_rows  # Release raw rows before smoothing
            rows_by_period[period] = []
            df = _smooth_tracking(df)
            sdf = spark.createDataFrame(df)
            row_count = validate_dataframe(sdf, required_cols, "idsse_tracking", logger)
            replace_expr = f"match_id = 'idsse_{mid}' AND period = {period}"
            write_delta_table(
                sdf,
                catalog,
                schema,
                "idsse_tracking",
                replace_where=replace_expr,
                logger=logger,
                row_count=row_count,
            )
            del df, sdf
            gc.collect()
        del rows_by_period
        gc.collect()


@workflow("wf-idsse", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Ingest IDSSE tracking and event data into the bronze layer."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    ingest_idsse(spark, catalog, schema, logger)
    ingest_idsse_events(spark, catalog, schema, logger)
    return 0


def main() -> None:
    """CLI entry point for IDSSE tracking data ingestion."""
    args = parse_ingestion_args("Ingest IDSSE Bundesliga tracking data into the bronze layer")
    logger = configure_logging("idsse")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting IDSSE ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    logger.info("IDSSE ingestion complete")


# ---------------------------------------------------------------------------
# Event XML Parsing (DFL_03_02 series)
# ---------------------------------------------------------------------------


def _find_event_files(data_dir: str, match_ids: list[str]) -> dict[str, str]:
    """Find DFL event XML files in a UC Volume directory.

    Scans the directory for files matching the ``DFL_03_02_*`` naming
    convention used for DFL Bundesliga event data.

    Args:
        data_dir: Directory containing DFL XML files.
        match_ids: Match IDs to search for.

    Returns:
        Mapping of match_id → file path for found event XMLs.
    """
    import os

    found: dict[str, str] = {}
    match_set = set(match_ids)

    try:
        entries = os.listdir(data_dir)
    except OSError:
        return found

    for name in entries:
        m = _EVENT_FILE_RE.search(name)
        if m:
            mid = m.group(1)
            if mid in match_set:
                found[mid] = os.path.join(data_dir, name)

    return found


def _parse_events_xml(
    event_path: str,
    player_team_map: dict[str, str],
    match_id: str,
    logger: logging.Logger,
) -> list[dict[str, object]]:
    """Parse DFL event XML (DFL_03_02 series) into row dicts.

    The DFL event XML has ``<Event>`` children under ``<PutDataRequest>``.
    Each ``<Event>`` carries ``EventId``, ``EventTime`` (ISO 8601),
    ``X-Position``, and ``Y-Position`` as attributes. The event type is
    the tag name of the first child element (e.g. ``KickOff``, ``Play``,
    ``TacklingGame``). Player and team IDs are attributes on that child.

    Period tracking: ``<KickOff>`` elements carry a ``GameSection``
    attribute (``firstHalf`` / ``secondHalf``). Period state is tracked
    across events; events before the first KickOff default to period 1.

    Timestamp computation: ``EventTime`` is parsed as ISO 8601 with
    timezone. Seconds are computed as the offset from the first event
    time within each period.

    Coordinate system: DFL pitch-origin meters (x 0-105, y 0-68).

    Args:
        event_path: Path to DFL event XML file.
        player_team_map: Mapping of DFL PersonId/ObjectId to ``"home"``/``"away"``.
        match_id: Raw match identifier (without ``idsse_`` prefix).
        logger: Logger instance.

    Returns:
        List of row dicts with event data.
    """
    rows: list[dict[str, object]] = []
    prefixed_match_id = f"idsse_{match_id}"

    # Period state: updated when <KickOff GameSection="..."> is encountered
    current_period = 1

    # First event time per period, for computing period-relative seconds
    period_start_time: dict[int, datetime] = {}

    for _ev, elem in ET.iterparse(event_path, events=("end",)):  # noqa: S314
        if elem.tag != "Event":
            # Only clear PutDataRequest (root) to release processed Event children.
            # Do NOT clear sub-Event elements (KickOff, Play, etc.) — their
            # attributes are needed when the parent <Event> end tag fires.
            if elem.tag == "PutDataRequest":
                elem.clear()
            continue

        # --- Event-level attributes ---
        event_id_attr = elem.get("EventId", "")
        x_str = elem.get("X-Position", "")
        y_str = elem.get("Y-Position", "")
        event_time_str = elem.get("EventTime", "")

        # --- First child element determines event type, player, and team ---
        first_child = None
        for child in elem:
            first_child = child
            break

        if first_child is None:
            elem.clear()
            continue

        event_type = first_child.tag

        # Period tracking: KickOff elements carry GameSection
        if event_type == "KickOff":
            section = first_child.get("GameSection", "")
            period_from_section = _SECTION_TO_PERIOD.get(section)
            if period_from_section is not None:
                current_period = period_from_section

        # Player ID: lookup order depends on event type.
        # For KickOff, the Player attr is on the nested <Play> child, not on <KickOff>.
        search_elem = first_child
        if event_type == "KickOff":
            # Look for <Play> child inside <KickOff> for player/team info
            for ko_child in first_child:
                if ko_child.tag == "Play":
                    search_elem = ko_child
                    break

        player_attr_names = _PLAYER_ATTR_ORDER.get(event_type, _DEFAULT_PLAYER_ATTRS)
        player_id = ""
        for attr_name in player_attr_names:
            player_id = search_elem.get(attr_name, "")
            if player_id:
                break

        # Team ID: lookup order depends on event type
        team_attr_names = _TEAM_ATTR_ORDER.get(event_type, _DEFAULT_TEAM_ATTRS)
        team_id = ""
        for attr_name in team_attr_names:
            team_id = search_elem.get(attr_name, "")
            if team_id:
                break

        # Resolve team to home/away label using player_team_map (keyed on PersonId)
        # If player not found, try the team ID directly (DFL-CLU-* format)
        team_label = player_team_map.get(player_id, "unknown")

        # Skip events without position data
        if not x_str or not y_str:
            elem.clear()
            continue

        try:
            x_val = float(x_str)
            y_val = float(y_str)
        except (ValueError, TypeError):
            elem.clear()
            continue

        if math.isnan(x_val) or math.isnan(y_val):
            elem.clear()
            continue

        # Parse ISO 8601 timestamp and compute period-relative seconds
        timestamp_seconds = 0.0
        if event_time_str:
            try:
                event_dt = datetime.fromisoformat(event_time_str)
                # Normalize to UTC for consistent arithmetic
                if event_dt.tzinfo is not None:
                    event_dt = event_dt.astimezone(timezone.utc)
                if current_period not in period_start_time:
                    period_start_time[current_period] = event_dt
                delta = event_dt - period_start_time[current_period]
                timestamp_seconds = delta.total_seconds()
            except (ValueError, TypeError):
                timestamp_seconds = 0.0

        rows.append(
            {
                "match_id": prefixed_match_id,
                "event_id": str(event_id_attr),
                "event_type": event_type,
                "timestamp_seconds": round(timestamp_seconds, 4),
                "period": current_period,
                "player_id": player_id,
                "team": team_label,
                "x": round(x_val, 4),
                "y": round(y_val, 4),
            }
        )

        elem.clear()

    logger.info("Parsed %d events for IDSSE match %s", len(rows), match_id)
    return rows


def ingest_idsse_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    match_ids: list[str] | None = None,
    data_dir: str = _DEFAULT_DATA_DIR,
) -> None:
    """Parse and ingest IDSSE event data for all matches.

    Scans the UC Volume directory for DFL event XML files
    (``DFL_03_02_eventdata_*``), parses event data, and writes to the
    ``idsse_events`` bronze Delta table with ``replaceWhere`` on
    ``match_id`` for idempotent writes.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Target schema (e.g. ``bronze``).
        logger: Structured logger instance.
        match_ids: Optional subset of match IDs to ingest. Defaults to all 7.
        data_dir: Directory containing pre-downloaded DFL XML files.
    """
    ids_to_ingest = match_ids or IDSSE_MATCH_IDS
    required_cols = [
        "match_id",
        "event_id",
        "event_type",
        "timestamp_seconds",
        "period",
        "player_id",
        "team",
        "x",
        "y",
    ]

    # Incremental skip: check which matches already exist
    existing_ids: set[str] = set()
    try:
        existing_rows = spark.table(f"{catalog}.{schema}.idsse_events").select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except _SparkAnalysisException:
        logger.info("No existing idsse_events table — processing all matches")

    new_match_ids = [mid for mid in ids_to_ingest if f"idsse_{mid}" not in existing_ids]
    logger.info(
        "Events: %d matches total, %d already processed, %d to process",
        len(ids_to_ingest),
        len(ids_to_ingest) - len(new_match_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return

    # Find event XML files
    event_files = _find_event_files(data_dir, new_match_ids)
    logger.info("Found %d event XML files in %s", len(event_files), data_dir)

    if not event_files:
        logger.info("No event XML files found — skipping event ingestion")
        return

    for mid, event_path in event_files.items():
        comp = _MATCH_COMPETITION[mid]
        info_path = f"{data_dir}/DFL_02_01_matchinformation_{comp}_DFL-MAT-{mid}.xml"

        _home_id, _away_id, player_team_map, _gk_ids = _parse_teams(info_path)
        rows = _parse_events_xml(event_path, player_team_map, mid, logger)

        if not rows:
            logger.info("No events with position data for match %s", mid)
            continue

        df = pd.DataFrame(rows)
        sdf = spark.createDataFrame(df)
        row_count = validate_dataframe(sdf, required_cols, "idsse_events", logger)
        replace_expr = f"match_id = 'idsse_{mid}'"
        write_delta_table(
            sdf,
            catalog,
            schema,
            "idsse_events",
            replace_where=replace_expr,
            logger=logger,
            row_count=row_count,
        )
        del df, sdf
        gc.collect()


if __name__ == "__main__":
    main()
