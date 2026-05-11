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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    finalize_bronze_df,
    get_spark_session,
    parse_ingestion_args,
    validate_dataframe,
    write_delta_table,
)
from shared.identifiers import idsse_native_match_id
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from ingestion.utils import SparkAnalysisException as _SparkAnalysisException

logger = logging.getLogger(__name__)


class _IdsseGuard:
    """Skip guard + runtime chunk discovery for IDSSE ingestion.

    The guard anti-joins :data:`IDSSE_MATCH_IDS` against the canonical
    ``match_id`` columns of ``bronze.idsse_tracking`` and
    ``bronze.idsse_events`` (intersection — a match is only "complete" when
    BOTH tables have ingested it). The resulting list of missing matches
    is partitioned into chunks of size :attr:`chunk_size` for the Terraform
    ``for_each_task`` fan-out (Cycle A pattern; Cycle B+ extends to other
    workflows).

    Wall-clock budget:
        At ~6.4 min/match wall-clock post-PR-1.8 (TODO D40d, 2026-04-21),
        a 2-match chunk fits ~13 min within the 900 s
        ``ingest_idsse_iteration`` timeout. ``chunk_size = 2`` is the
        largest size that fits with safety margin.
    """

    workflow_id = "wf-idsse"
    chunk_size: int = 2

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Compute missing matches and partition into for_each_task chunks."""
        import logging as _logging

        from ingestion.utils import tolerate_missing_table

        _guard_logger = _logging.getLogger(__name__)

        existing_t: set[str] = set()
        existing_e: set[str] = set()
        with tolerate_missing_table(_guard_logger, "IDSSE tables missing — needs ingestion"):
            t_rows = spark.table(f"{catalog}.{schema}.idsse_tracking").select("match_id").distinct().collect()
            existing_t = {str(row["match_id"]) for row in t_rows}
            e_rows = spark.table(f"{catalog}.{schema}.idsse_events").select("match_id").distinct().collect()
            existing_e = {str(row["match_id"]) for row in e_rows}

        # A match is complete only when present in BOTH tracking AND events.
        # Bronze stores match_id as the canonical bare DFL form (e.g.
        # 'J03WMX') per ADR-018 / Bug #1 (PR-LL2 close-out).
        completed = existing_t & existing_e
        missing = [mid for mid in IDSSE_MATCH_IDS if mid not in completed]

        if not missing:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        chunks = [missing[i : i + self.chunk_size] for i in range(0, len(missing), self.chunk_size)]
        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(missing),
            chunks=chunks,
        )


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

_SECTION_TO_PERIOD: dict[str, int] = {
    "firstHalf": 1,
    "secondHalf": 2,
    "extraTimeFirstHalf": 3,
    "extraTimeSecondHalf": 4,
    "penaltyShootout": 5,
}
# Verified with synthetic fixture only - no production data exercises periods 3-5 yet.

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

# ---------------------------------------------------------------------------
# Bronze-completeness schema constants (used by _parse_events_xml).
# Contract: the IDSSE bronze-coverage test (test_idsse_bronze_coverage.py)
# imports the same maps and asserts every DFL source attribute lands in a
# bronze column through the combination of these constants + `_to_snake_case`.
# See the test for the ground-truth DFL attribute enumeration.
# ---------------------------------------------------------------------------

# Pre-compiled regex for splitting CamelCase / PascalCase at word boundaries.
# Matches (lower→Upper) and (Upper→Upper-before-lower). Shared conceptually
# with src/tests/coverage_utils.to_snake_case — the mirror lives there for
# the coverage-test infrastructure.
_ATTR_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _to_snake_case(name: str) -> str:
    """Normalise a DFL XML attribute name to a bronze column suffix.

    Handles CamelCase (``PlayAngle`` → ``play_angle``) and hyphenated
    (``X-Position`` → ``x_position``) attribute names consistently.
    """
    return _ATTR_CAMEL_BOUNDARY.sub("_", name.replace("-", "_")).lower()


# Raw DFL <Event>-level XML attribute names → bronze column names.
# Thirteen DFL attributes map to thirteen bronze columns. ``match_id`` on the
# row (derived: ``idsse_{match_id}``) is DISTINCT from ``match_id_raw`` (the
# DFL-MAT-* identifier captured here) — both land in bronze per
# bronze-completeness.
_EVENT_LEVEL_ATTR_MAP: dict[str, str] = {
    "MatchId": "match_id_raw",
    "EventId": "event_id",
    "EventTime": "event_time",
    "StartFrame": "start_frame",
    "EndFrame": "end_frame",
    "CalculatedFrame": "calculated_frame",
    "CalculatedTimestamp": "calculated_timestamp",
    "X-Position": "x",
    "Y-Position": "y",
    "X-Source-Position": "x_source_position",
    "Y-Source-Position": "y_source_position",
    "X-PositionFromTracking": "x_position_from_tracking",
    "Y-PositionFromTracking": "y_position_from_tracking",
}

# Event-level bronze cols that must be cast float / int (vs. pass-through string).
_EVENT_LEVEL_FLOAT_COLS: frozenset[str] = frozenset(
    {"x", "y", "x_source_position", "y_source_position", "x_position_from_tracking", "y_position_from_tracking"},
)
_EVENT_LEVEL_INT_COLS: frozenset[str] = frozenset({"start_frame", "end_frame", "calculated_frame"})

# First-child tag → bronze column prefix. Every attribute on a first-child
# element lands as ``{prefix}_{to_snake_case(attr)}``. Prefixes are short,
# readable, and distinct across all first-child types.
_EVENT_TYPE_PREFIX: dict[str, str] = {
    "BallClaiming": "claim",
    "BallDeflection": "deflection",
    "Caution": "caution",
    "CautionTeamofficial": "caution_official",
    "ChanceWithoutShot": "chance",
    "CornerKick": "corner",
    "Delete": "delete",
    "FairPlay": "fairplay",
    "FinalWhistle": "whistle",
    "Foul": "foul",
    "FreeKick": "freekick",
    "GoalDisallowed": "goaldis",
    "GoalKick": "goalkick",
    "KickOff": "kickoff",
    "Nutmeg": "nutmeg",
    "Offside": "offside",
    "OtherBallAction": "otherball",
    "OtherPlayerAction": "other_action",
    "Penalty": "penalty",
    "PenaltyNotAwarded": "penalty_not",
    "Play": "play",
    "PlayerNotSentOff": "not_sent_off",
    "PossessionLossBeforeGoal": "possloss",
    "RefereeBall": "refball",
    "Run": "run",
    "ShotAtGoal": "shot",
    "SitterPrevented": "sitter_prev",
    "SpectacularPlay": "spectacular",
    "Substitution": "sub",
    "TacklingGame": "tackle",
    "ThrowIn": "throwin",
    "VideoAssistantAction": "var",
}

# Nested tag → bronze column prefix. Nested children that reuse a top-level
# event type keep the same prefix (a Play nested inside KickOff writes to
# ``play_*`` — same as a standalone Play). Shot-outcome variants share the
# ``shot_outcome_*`` prefix and use ``shot_outcome_type`` to disambiguate.
_NESTED_PREFIX_MAP: dict[str, str] = {
    "Pass": "pass",
    "Cross": "cross",
    "Play": "play",
    "ShotAtGoal": "shot",
    "FairPlay": "fairplay",
    "FaultExecution": "fault_execution",
    "SuccessfulShot": "shot_outcome",
    "SavedShot": "shot_outcome",
    "ShotWide": "shot_outcome",
    "ShotWoodWork": "shot_outcome",
    "BlockedShot": "shot_outcome",
    "OtherShot": "shot_outcome",
}

# Shot-outcome nested tag name → disambiguator value emitted on
# ``shot_outcome_type`` when a ShotAtGoal event has one of these nested.
_SHOT_OUTCOME_NAMES: dict[str, str] = {
    "SuccessfulShot": "successful",
    "SavedShot": "saved",
    "ShotWide": "wide",
    "ShotWoodWork": "woodwork",
    "BlockedShot": "blocked",
    "OtherShot": "other",
}


def _compute_idsse_events_bronze_cols() -> frozenset[str]:
    """Compute the full set of bronze columns the events parser emits.

    Derived from the DFL schema module (``_dfl_event_schema``) combined with
    the parser's own prefix maps. Pre-declaring this set up-front lets
    :func:`finalize_bronze_df` guarantee every column lands in Delta
    regardless of which event types appear in a given match's slice.
    """
    from ingestion._dfl_event_schema import (
        EVENT_LEVEL_ATTRS,
        FIRST_CHILD_ATTRS,
        NESTED_CHILD_ATTRS,
    )

    cols: set[str] = set()

    # Derived cols — every row carries these unconditionally.
    cols.update(
        {
            "match_id",
            "event_type",
            "period",
            "player_id",
            "team",
            "timestamp_seconds",
            # PR-LL2 Path B: match-level metadata sourced from the DFL
            # <General> element of the matchinformation XML. Same value for
            # every row of a given match. Provider-agnostic naming convention
            # (matches dim_competitions.native_competition_id; aligns with
            # bronze.metrica_events Path B additions).
            "competition_native_id",  # e.g. 'DFL-COM-000001'
            "season_native_id",  # e.g. 'DFL-SEA-0001K6'
            "home_team_id_native",  # DFL CLU id of the home team
            "away_team_id_native",  # DFL CLU id of the guest team
            "team_id_native",  # DFL CLU id of the acting team for THIS row;
            #                    home if `team`='home', away if `team`='away',
            #                    NULL if `team`='unknown'.
        }
    )

    # Event-level attrs → bronze cols via the rename map.
    for dfl_attr in EVENT_LEVEL_ATTRS:
        bronze_col = _EVENT_LEVEL_ATTR_MAP.get(dfl_attr)
        if bronze_col is not None:
            cols.add(bronze_col)

    # First-child tag attrs → {prefix}_{snake(attr)}.
    for tag, attrs in FIRST_CHILD_ATTRS.items():
        prefix = _EVENT_TYPE_PREFIX.get(tag)
        if prefix is None:
            continue
        for attr in attrs:
            cols.add(f"{prefix}_{_to_snake_case(attr)}")

    # Nested children → {nested_prefix}_{snake(attr)}.
    for nested_map in NESTED_CHILD_ATTRS.values():
        for nested_tag, attrs in nested_map.items():
            prefix = _NESTED_PREFIX_MAP.get(nested_tag)
            if prefix is None:
                continue
            for attr in attrs:
                cols.add(f"{prefix}_{_to_snake_case(attr)}")

    # shot_outcome_type disambiguator — emitted by _build_event_row whenever a
    # ShotAtGoal has one of the six nested outcome tags.
    cols.add("shot_outcome_type")

    return frozenset(cols)


_IDSSE_EVENTS_BRONZE_COLS: frozenset[str] = _compute_idsse_events_bronze_cols()
"""Expected bronze columns for bronze.idsse_events (computed at import time)."""


# Single source of truth for which `_MatchMetadata` fields surface to bronze.
# Both `ingest_idsse` (tracking writer) and `ingest_idsse_events` (events
# writer) MUST emit a per-row column for each name here. Asserted by
# `src/tests/test_idsse_match_metadata_parity.py` — closes the asymmetric-
# coverage gap discovered in session 69 (2026-04-30) where PR-LL2 wired
# `_parse_match_metadata` into `bronze.idsse_events` but missed
# `bronze.idsse_tracking`. Both tables share the same upstream
# matchinformation XML; both must surface the same per-match metadata.
_IDSSE_MATCH_METADATA_BRONZE_COLS: frozenset[str] = frozenset(
    {
        "competition_native_id",  # e.g. 'DFL-COM-000001'
        "season_native_id",  # e.g. 'DFL-SEA-0001K6'
        "home_team_id_native",  # DFL CLU id of the home team
        "away_team_id_native",  # DFL CLU id of the guest team
    }
)

# Dtype overrides for the handful of columns that must land as numerics.
# Columns not in this map default to pd.StringDtype() via finalize_bronze_df.
_IDSSE_EVENTS_DTYPE_OVERRIDES: dict[str, str] = {
    "period": "Int64",
    "start_frame": "Int64",
    "end_frame": "Int64",
    "calculated_frame": "Int64",
    "timestamp_seconds": "Float64",
    "x": "Float64",
    "y": "Float64",
    "x_source_position": "Float64",
    "y_source_position": "Float64",
    "x_position_from_tracking": "Float64",
    "y_position_from_tracking": "Float64",
}


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


def _parse_match_ids_arg(raw: str | None) -> list[str] | None:
    """Parse the optional ``--match-ids`` comma-separated CLI value.

    Used by the Terraform ``for_each_task`` fan-out: each child iteration
    receives ``--match-ids "J03WMX,J03WN1"`` (a runtime-discovered subset
    of :data:`IDSSE_MATCH_IDS`). This helper parses the string, validates
    every ID is known, and returns a clean list (or ``None`` when no
    filter was provided — full 7-match run).

    Args:
        raw: Raw CLI string (e.g. ``"J03WMX,J03WN1"`` or ``None``).

    Returns:
        Validated list of match IDs, or ``None`` when ``raw`` is empty.

    Raises:
        SystemExit: When any ID in ``raw`` is not in :data:`IDSSE_MATCH_IDS`.
            Hard-fail-fast — silent filtering would mask preflight/Python
            drift (e.g. an iteration receiving an ID that was removed from
            the constant).
    """
    if raw is None or raw == "":
        return None
    requested = [mid.strip() for mid in raw.split(",") if mid.strip()]
    unknown = [mid for mid in requested if mid not in IDSSE_MATCH_IDS]
    if unknown:
        raise SystemExit(f"Unknown IDSSE match IDs in --match-ids: {unknown}. Valid IDs: {sorted(IDSSE_MATCH_IDS)}")
    return requested


def _parse_teams(info_path: str) -> tuple[str, str, dict[str, str], set[str]]:
    """Parse match info XML to get home/away team IDs, player-to-team mapping, and GK IDs.

    Args:
        info_path: Path to match info XML file.

    Returns:
        Tuple of (home_team_id, away_team_id, {person_id: "home"|"away"}, gk_player_ids).
        ``gk_player_ids`` contains PersonIds of players with ``PlayingPosition="TW"``
        (DFL standard for Torwart/goalkeeper).

    Note:
        The per-row DFL ``TeamId`` that lands in bronze is NOT sourced from
        this mapping — it is taken directly from the enclosing FrameSet's
        ``TeamId`` attribute during position parsing, which is always
        available and avoids an extra map plumb-through.
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


@dataclass(frozen=True)
class _MatchMetadata:
    """Match-level metadata sourced from the DFL ``<General>`` and
    ``<Environment>`` elements in the matchinformation XML.

    PR-LL2 Path B: previously the SPADL/VAEP pipeline had no way to access
    the per-match competition / season / pitch dimensions because none of
    these landed in ``bronze.idsse_events``. This dataclass + parser
    surface them so the bronze writer can populate the LL2-added columns
    (``competition_native_id``, ``season_native_id``,
    ``home_team_id_native``, ``away_team_id_native``).
    """

    competition_id: str
    """DFL CompetitionId, e.g. ``DFL-COM-000001``. Format ``DFL-COM-XXXXXX``."""

    season_id: str
    """DFL SeasonId, e.g. ``DFL-SEA-0001K6``. Format ``DFL-SEA-XXXXXX``."""

    home_team_id: str
    """DFL HomeTeamId, e.g. ``DFL-CLU-000008``. Format ``DFL-CLU-XXXXXX``."""

    away_team_id: str
    """DFL GuestTeamId (DFL spec calls the away team "guest")."""

    pitch_x: float | None
    """Pitch length in meters (e.g. 105.0). NULL if absent from XML."""

    pitch_y: float | None
    """Pitch width in meters (e.g. 68.0). NULL if absent from XML."""


def _parse_match_metadata(info_path: str) -> _MatchMetadata:
    """Parse the ``<General>`` + ``<Environment>`` elements of a DFL
    matchinformation XML, returning competition / season / pitch metadata.

    DFL spec, ``DFL_02_01_matchinformation_*.xml`` shape::

        <PutDataRequest>
          <MatchInformation>
            <General CompetitionId="DFL-COM-000001"
                     SeasonId="DFL-SEA-0001K6"
                     HomeTeamId="DFL-CLU-000008"
                     GuestTeamId="DFL-CLU-00000G"
                     ... />
            <Environment PitchX="105.00" PitchY="68.00" ... />
            <Teams>...</Teams>
            ...

    Args:
        info_path: Filesystem path (or UC Volume path) to the matchinformation XML.

    Returns:
        Populated ``_MatchMetadata``. Empty strings for any missing IDs
        (callers tolerate empty home_team_id today; we surface that as
        empty string rather than raising so a malformed XML doesn't kill
        the whole batch).
    """
    tree = ET.parse(info_path)  # noqa: S314  # nosemgrep: use-defused-xml-parse
    root = tree.getroot()

    general = root.find(".//General")
    environment = root.find(".//Environment")

    competition_id = general.get("CompetitionId", "") if general is not None else ""
    season_id = general.get("SeasonId", "") if general is not None else ""
    home_team_id = general.get("HomeTeamId", "") if general is not None else ""
    away_team_id = general.get("GuestTeamId", "") if general is not None else ""

    pitch_x: float | None = None
    pitch_y: float | None = None
    if environment is not None:
        pitch_x = _parse_float_or_none(environment.get("PitchX", ""))
        pitch_y = _parse_float_or_none(environment.get("PitchY", ""))

    return _MatchMetadata(
        competition_id=competition_id,
        season_id=season_id,
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        pitch_x=pitch_x,
        pitch_y=pitch_y,
    )


# Sentinel used by ``_parse_events_xml`` / ``_build_event_row`` when callers
# don't have a real matchinformation XML available (e.g. unit tests with
# synthetic event XML, no companion matchinfo). Production ingestion always
# passes a populated metadata via ``_parse_match_metadata``.
_EMPTY_MATCH_METADATA: _MatchMetadata = _MatchMetadata(
    competition_id="",
    season_id="",
    home_team_id="",
    away_team_id="",
    pitch_x=None,
    pitch_y=None,
)


# DFL Frame attribute contract — ground truth from
# src/tests/fixtures/idsse_dfl_tracking_attr_enumeration.json (enumerated
# 2026-04-21 from the 7 IDSSE match positions.xml files). Asserted by
# test_idsse_bronze_coverage.TestTrackingCoverage.

# Frame attrs shared across player / referee / ball FrameSets.
_TRACKING_FRAME_ATTRS_SHARED: tuple[str, ...] = ("N", "T", "X", "Y", "S", "A", "D", "M")
# Frame attrs found only on ball FrameSets.
_TRACKING_FRAME_ATTRS_BALL_ONLY: tuple[str, ...] = ("Z", "BallPossession", "BallStatus")


def _parse_float_or_none(raw: str) -> float | None:
    """Parse an XML attribute into a float, returning None on empty/NaN."""
    if not raw:
        return None
    try:
        value = float(raw)
    except (ValueError, TypeError):
        return None
    if math.isnan(value):
        return None
    return round(value, 4)


def _parse_bool_or_none(raw: str) -> bool | None:
    """Parse ``"true"``/``"false"`` (case-insensitive) into a Python bool."""
    if not raw:
        return None
    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return None


def _parse_positions_xml(
    pos_path: str,
    player_team_map: dict[str, str],
    match_id: str,
    logger: logging.Logger,
    gk_player_ids: set[str] | None = None,
    metadata: _MatchMetadata = _EMPTY_MATCH_METADATA,
) -> dict[int, list[dict[str, object]]]:
    """Parse DFL position XML into bronze-complete row dicts, split by period.

    Uses TWO-PASS iterative XML parsing.

    1. **First pass (ball-only)**: Scans ball FrameSets
       (``TeamId="BALL"``) and populates ``ball_by_frame`` keyed on
       ``(period, frame_n)``. Each ball entry captures EVERY DFL
       ``<Frame>`` attribute (X, Y, Z, S, A, D, M, T, BallPossession,
       BallStatus) so downstream joins see the full source schema.
       Also populates ``period_first_frame`` for period-relative timestamps.
    2. **Second pass (players-only)**: Emits one row per player per frame
       with every DFL ``<Frame>`` attribute (X, Y, S, A, D, M, T), the
       player's DFL ``TeamId`` (directly from the enclosing FrameSet),
       and a ``ball_*`` join from pass 1.

    **Bronze-completeness:** every attribute enumerated in
    ``src/tests/fixtures/idsse_dfl_tracking_attr_enumeration.json`` lands
    in a dedicated bronze column. Nothing is dropped silently.

    **Why two-pass (PR 1.7):** real DFL position XMLs have ball FrameSets
    AFTER all player / referee FrameSets in the file. A single-pass parser
    emitting player rows first would see an empty ball lookup dict and
    produce NULL ball coordinates in bronze.

    Returns rows grouped by period so callers can process and release each
    half independently, halving peak DataFrame memory.

    Args:
        pos_path: Path to position XML file.
        player_team_map: Mapping of PersonId to "home"/"away".
        match_id: Raw match identifier (without ``idsse_`` prefix).
        logger: Structured logger instance.
        gk_player_ids: Set of PersonIds identified as goalkeepers. When
            provided, each tracking row carries an ``is_goalkeeper`` bool.

    Returns:
        Mapping of period number → list of row dicts. Each row carries all
        DFL tracking attributes per the bronze-completeness contract.
    """
    rows_by_period: dict[int, list[dict[str, object]]] = {p: [] for p in _SECTION_TO_PERIOD.values()}
    # PR-LL2 Path B close-out (2026-04-29): bronze.idsse_events.match_id /
    # bronze.idsse_tracking.match_id now use the bare DFL MatchId (e.g.
    # 'J03WMX'). Pre-close-out the format was 'idsse_J03WMX' which
    # produced 100% NULL match_key on fct_action_values for IDSSE rows
    # because dim_matches strips the 'idsse_' prefix. ADR-018 + Bug #1.
    canonical_match_id = idsse_native_match_id(match_id)

    # Ball data per (period, frame_n). Each entry is a dict carrying every
    # DFL ball Frame attribute plus the ball-only ones. Populated in pass 1.
    ball_by_frame: dict[tuple[int, int], dict[str, object]] = {}
    ball_miss_count = 0  # Player frames where ball lookup returned None

    # First-seen frame number per period — used to compute period-relative
    # timestamps (see PR 1.6).
    period_first_frame: dict[int, int] = {}

    # ── PASS 1: ball FrameSets → populate ball_by_frame + period_first_frame ──
    for _event, elem in ET.iterparse(pos_path, events=("end",)):  # noqa: S314
        if elem.tag != "FrameSet":
            continue

        team_id_lower = elem.get("TeamId", "").lower()
        if team_id_lower != "ball":
            elem.clear()
            continue

        section = elem.get("GameSection", "")
        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            logger.warning(
                "Unrecognized GameSection %r in match %s — skipping FrameSet",
                section,
                match_id,
            )
            elem.clear()
            continue

        for frame_el in elem.iter("Frame"):
            n = int(frame_el.get("N", "0"))
            ball_entry: dict[str, object] = {
                "ball_x": _parse_float_or_none(frame_el.get("X", "")),
                "ball_y": _parse_float_or_none(frame_el.get("Y", "")),
                "ball_z": _parse_float_or_none(frame_el.get("Z", "")),
                "ball_s": _parse_float_or_none(frame_el.get("S", "")),
                "ball_a": _parse_float_or_none(frame_el.get("A", "")),
                "ball_d": _parse_float_or_none(frame_el.get("D", "")),
                "ball_m": _parse_bool_or_none(frame_el.get("M", "")),
                "ball_t": frame_el.get("T", "") or None,
                "ball_possession": frame_el.get("BallPossession", "") or None,
                "ball_status": frame_el.get("BallStatus", "") or None,
            }
            ball_by_frame[(period, n)] = ball_entry
            cur = period_first_frame.get(period)
            if cur is None or n < cur:
                period_first_frame[period] = n
        elem.clear()

    # ── PASS 2: player FrameSets → emit per-player tracking rows ──
    for _event, elem in ET.iterparse(pos_path, events=("end",)):  # noqa: S314
        if elem.tag != "FrameSet":
            continue

        team_id_lower = elem.get("TeamId", "").lower()
        # Pass 2 skips ball (already handled) and referee (not tracked).
        if team_id_lower in ("ball", "referee"):
            elem.clear()
            continue

        section = elem.get("GameSection", "")
        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            logger.warning(
                "Unrecognized GameSection %r in match %s — skipping FrameSet",
                section,
                match_id,
            )
            elem.clear()
            continue

        team_id = elem.get("TeamId", "") or None
        person_id = elem.get("PersonId", "")
        team_label = player_team_map.get(person_id, "unknown")
        period_rows = rows_by_period[period]

        for frame_el in elem.iter("Frame"):
            n = int(frame_el.get("N", "0"))
            x = _parse_float_or_none(frame_el.get("X", ""))
            y = _parse_float_or_none(frame_el.get("Y", ""))

            # Require at least X/Y to emit a player row — a player tracking
            # row without position is meaningless. Other attrs default to
            # None if missing (bronze-completeness tolerates sparse data).
            if x is None or y is None:
                continue

            cur = period_first_frame.get(period)
            if cur is None or n < cur:
                period_first_frame[period] = n
            period_start = period_first_frame[period]
            timestamp = (n - period_start) / _FRAME_RATE

            ball_lookup = ball_by_frame.get((period, n))
            if ball_lookup is None:
                ball_miss_count += 1
            ball_entry: dict[str, object] = (
                ball_lookup
                if ball_lookup is not None
                else {
                    "ball_x": None,
                    "ball_y": None,
                    "ball_z": None,
                    "ball_s": None,
                    "ball_a": None,
                    "ball_d": None,
                    "ball_m": None,
                    "ball_t": None,
                    "ball_possession": None,
                    "ball_status": None,
                }
            )

            row: dict[str, object] = {
                # Existing columns
                "period": period,
                "frame": n,
                "timestamp": round(timestamp, 4),
                "player_id": person_id,
                "team": team_label,
                "x": x,
                "y": y,
                "match_id": canonical_match_id,
                "frame_rate": _FRAME_RATE,
                # New per-player DFL Frame attrs
                "team_id": team_id,
                "t": frame_el.get("T", "") or None,
                "s": _parse_float_or_none(frame_el.get("S", "")),
                "a": _parse_float_or_none(frame_el.get("A", "")),
                "d": _parse_float_or_none(frame_el.get("D", "")),
                "m": _parse_bool_or_none(frame_el.get("M", "")),
                # Per-match metadata (sourced from <General> in matchinformation XML).
                # Same value for every row of a given match — replicated here for
                # parity with bronze.idsse_events (asserted by
                # test_idsse_match_metadata_parity.py). Empty string when the
                # XML lacked the attribute or the caller passed _EMPTY_MATCH_METADATA
                # (test path with no companion matchinfo).
                "competition_native_id": metadata.competition_id,
                "season_native_id": metadata.season_id,
                "home_team_id_native": metadata.home_team_id,
                "away_team_id_native": metadata.away_team_id,
                # Ball-joined cols
                **ball_entry,
            }
            if gk_player_ids is not None:
                row["is_goalkeeper"] = person_id in gk_player_ids
            period_rows.append(row)

        elem.clear()

    logger.info("Parsed %d ball frames for match %s", len(ball_by_frame), match_id)
    if ball_miss_count > 0 and len(ball_by_frame) > 0:
        total_player_frames = sum(len(rows) for rows in rows_by_period.values())
        miss_pct = 100.0 * ball_miss_count / max(total_player_frames, 1)
        log_fn = logger.warning if miss_pct > 5.0 else logger.info
        log_fn(
            "Ball coordinate lookup missed %d of %d player frames for match %s (%.1f%%)",
            ball_miss_count,
            total_player_frames,
            match_id,
            miss_pct,
        )

    return rows_by_period


# Bronze-completeness contract for bronze.idsse_tracking.
# Every column here is emitted by _parse_positions_xml; finalize_bronze_df
# guarantees it lands in Delta regardless of which Frame attrs happen to be
# populated for a given match's rows. Asserted by the coverage tests.
_IDSSE_TRACKING_BRONZE_COLS: tuple[str, ...] = (
    # Derived / join keys
    "period",
    "frame",
    "timestamp",
    "player_id",
    "team",
    "team_id",
    "match_id",
    "frame_rate",
    "is_goalkeeper",
    # DFL per-player Frame attrs
    "x",
    "y",
    "t",
    "s",
    "a",
    "d",
    "m",
    # Ball-joined DFL Frame attrs
    "ball_x",
    "ball_y",
    "ball_z",
    "ball_s",
    "ball_a",
    "ball_d",
    "ball_m",
    "ball_t",
    "ball_possession",
    "ball_status",
    # Per-match metadata sourced from matchinformation XML's <General> element.
    # Parity with bronze.idsse_events; see _IDSSE_MATCH_METADATA_BRONZE_COLS.
    "competition_native_id",
    "season_native_id",
    "home_team_id_native",
    "away_team_id_native",
)

# Nullable dtype overrides for tracking columns. Columns not in this map
# default to pd.StringDtype() via finalize_bronze_df.
_IDSSE_TRACKING_DTYPE_OVERRIDES: dict[str, str] = {
    "period": "Int64",
    "frame": "Int64",
    "timestamp": "Float64",
    "frame_rate": "Int64",
    "is_goalkeeper": "boolean",
    "x": "Float64",
    "y": "Float64",
    "s": "Float64",
    "a": "Float64",
    "d": "Float64",
    "m": "boolean",
    "ball_x": "Float64",
    "ball_y": "Float64",
    "ball_z": "Float64",
    "ball_s": "Float64",
    "ball_a": "Float64",
    "ball_d": "Float64",
    "ball_m": "boolean",
}


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

    from ingestion.utils import tolerate_missing_table

    # Incremental skip: check which matches already exist in the Delta table
    existing_ids: set[str] = set()
    with tolerate_missing_table(logger, "No existing idsse_tracking table — processing all matches"):
        existing_rows = spark.table(f"{catalog}.{schema}.idsse_tracking").select("match_id").distinct().collect()
        existing_ids = {str(row["match_id"]) for row in existing_rows}

    new_match_ids = [mid for mid in ids_to_ingest if idsse_native_match_id(mid) not in existing_ids]
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
        # Parity with `ingest_idsse_events`: surface match-level metadata
        # (competition / season / home+away DFL TeamIds) onto every bronze row.
        # PR-LL2 wired this for events but missed tracking; session 69 closes
        # the gap. Eliminates the hardcoded `_MATCH_COMPETITION` mirror in
        # `dbt_project/models/staging/idsse/stg_idsse__matches.sql`.
        match_metadata = _parse_match_metadata(info_path)
        logger.info(
            "Found %d players in match info (%d GKs) — competition=%s, season=%s",
            len(player_team_map),
            len(gk_player_ids),
            match_metadata.competition_id or "<missing>",
            match_metadata.season_id or "<missing>",
        )

        rows_by_period = _parse_positions_xml(
            pos_path,
            player_team_map,
            mid,
            logger,
            gk_player_ids=gk_player_ids,
            metadata=match_metadata,
        )
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
            # Guarantee every DFL tracking column lands in bronze regardless of
            # which Frame attrs were populated in this match — Spark would
            # otherwise drop all-None object columns as NullType.
            df = finalize_bronze_df(
                df,
                expected_cols=set(_IDSSE_TRACKING_BRONZE_COLS),
                dtype_overrides=_IDSSE_TRACKING_DTYPE_OVERRIDES,
            )
            sdf = spark.createDataFrame(df)
            row_count = validate_dataframe(sdf, required_cols, "idsse_tracking", logger)
            replace_expr = f"match_id = '{idsse_native_match_id(mid)}' AND period = {period}"
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
    match_ids: list[str] | None = None,
) -> int:
    """Ingest IDSSE tracking and event data into the bronze layer.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name.
        logger: Structured logger.
        filter_result: Skip-guard result; ``count == 0`` raises
            :class:`WorkflowSkippedError`.
        ctx: Optional workflow context (kept for hook parity).
        match_ids: Optional subset of :data:`IDSSE_MATCH_IDS` to ingest.
            ``None`` means process all 7 matches (single-task path or
            backward-compat caller). When set (typically by the
            ``for_each_task`` fan-out passing ``--match-ids "J03WMX,J03WN1"``
            from the preflight task's discovered chunks), only the listed
            matches are ingested for both tracking AND events. Per-match
            incremental skip still applies inside :func:`ingest_idsse`
            and :func:`ingest_idsse_events`.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    ingest_idsse(spark, catalog, schema, logger, match_ids=match_ids)
    ingest_idsse_events(spark, catalog, schema, logger, match_ids=match_ids)
    return 0


def main() -> None:
    """CLI entry point for IDSSE tracking ingestion (single-iteration handler).

    Each iteration of the ``for_each_task`` fan-out invokes this entry point
    with ``--match-ids "J03WMX,J03WN1"`` (a runtime-discovered subset
    written by the ``preflight_idsse`` task). When invoked without
    ``--match-ids`` (e.g., manual standalone run), the function processes
    the full 7-match :data:`IDSSE_MATCH_IDS` set.
    """
    args = parse_ingestion_args(
        "Ingest IDSSE Bundesliga tracking data into the bronze layer",
        extra_args=[
            (
                "--match-ids",
                {
                    "type": str,
                    "default": None,
                    "help": (
                        "Optional comma-separated subset of IDSSE match IDs to "
                        "ingest (e.g. 'J03WMX,J03WN1'). Used by the Terraform "
                        "for_each_task fan-out — runtime-discovered by the "
                        "preflight_idsse task. Omit to process all 7 matches."
                    ),
                },
            ),
        ],
    )
    logger = configure_logging("idsse")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids = _parse_match_ids_arg(getattr(args, "match_ids", None))

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting IDSSE ingestion into %s.%s", args.catalog, args.schema)
    if match_ids is not None:
        logger.info("Restricted to chunk: %s (%d matches)", match_ids, len(match_ids))

    run_pipeline(
        spark,
        args.catalog,
        args.schema,
        logger,
        filter_result=filter_result,
        match_ids=match_ids,
    )
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


def _build_event_row(
    elem: ET.Element,
    first_child: ET.Element,
    event_type: str,
    canonical_match_id: str,
    current_period: int,
    player_team_map: dict[str, str],
    period_start_time: dict[int, datetime],
    metadata: _MatchMetadata = _EMPTY_MATCH_METADATA,
) -> dict[str, object]:
    """Build one bronze row from a DFL <Event> element + its first child.

    Per the bronze-completeness principle, this extracts EVERY XML attribute
    on the Event + first-child + nested-child elements into a prefixed
    bronze column. Type-casts event-level cols that downstream analysis
    treats as numeric (x/y coords, frame numbers). Returns the full row
    dict; callers need not pre-populate any columns.

    Args:
        ...
        metadata: Match-level metadata from ``_parse_match_metadata``.
            PR-LL2 Path B: lifts ``competition_native_id`` /
            ``season_native_id`` / ``home_team_id_native`` /
            ``away_team_id_native`` onto every row. ``team_id_native`` is
            derived per-row from the ``team`` label.
    """
    row: dict[str, object] = {
        "match_id": canonical_match_id,
        "event_type": event_type,
        "period": current_period,
        "player_id": "",
        "team": "unknown",
        # PR-LL2: match-level metadata (same value for every row of a match).
        "competition_native_id": metadata.competition_id,
        "season_native_id": metadata.season_id,
        "home_team_id_native": metadata.home_team_id,
        "away_team_id_native": metadata.away_team_id,
        # team_id_native filled below after `team` label resolves.
    }

    # --- Event-level attrs → bronze cols via EVENT_LEVEL_ATTR_MAP ---
    for dfl_attr, bronze_col in _EVENT_LEVEL_ATTR_MAP.items():
        raw_val = elem.get(dfl_attr)
        if raw_val is None or raw_val == "":
            row[bronze_col] = None
            continue
        if bronze_col in _EVENT_LEVEL_FLOAT_COLS:
            try:
                fv = float(raw_val)
            except (ValueError, TypeError):
                row[bronze_col] = None
                continue
            row[bronze_col] = round(fv, 4) if not math.isnan(fv) else None
        elif bronze_col in _EVENT_LEVEL_INT_COLS:
            try:
                row[bronze_col] = int(raw_val)
            except (ValueError, TypeError):
                row[bronze_col] = None
        else:
            row[bronze_col] = raw_val

    # --- timestamp_seconds: period-relative from EventTime ---
    event_time_str = elem.get("EventTime", "")
    timestamp_seconds: float | None = None
    if event_time_str:
        try:
            event_dt = datetime.fromisoformat(event_time_str)
            if event_dt.tzinfo is not None:
                event_dt = event_dt.astimezone(timezone.utc)
            if current_period not in period_start_time:
                period_start_time[current_period] = event_dt
            delta = event_dt - period_start_time[current_period]
            timestamp_seconds = round(delta.total_seconds(), 4)
        except (ValueError, TypeError):
            pass
    row["timestamp_seconds"] = timestamp_seconds

    # --- Primary player_id + team label (preserving KickOff nested-Play lookup) ---
    search_elem = first_child
    if event_type == "KickOff":
        for ko_child in first_child:
            if ko_child.tag == "Play":
                search_elem = ko_child
                break
    player_attr_names = _PLAYER_ATTR_ORDER.get(event_type, _DEFAULT_PLAYER_ATTRS)
    for attr_name in player_attr_names:
        pid = search_elem.get(attr_name, "")
        if pid:
            row["player_id"] = pid
            break
    pid_val = row["player_id"]
    if isinstance(pid_val, str) and pid_val:
        row["team"] = player_team_map.get(pid_val, "unknown")

    # PR-LL2: derive `team_id_native` (DFL CLU id of the acting team) from
    # the resolved home/away label. NULL when team is unknown.
    team_label = row["team"]
    if team_label == "home":
        row["team_id_native"] = metadata.home_team_id
    elif team_label == "away":
        row["team_id_native"] = metadata.away_team_id
    else:
        row["team_id_native"] = None

    # --- First-child attrs → {prefix}_{snake(attr)} bronze cols ---
    prefix = _EVENT_TYPE_PREFIX.get(event_type)
    if prefix is not None:
        for attr_name, attr_val in first_child.attrib.items():
            row[f"{prefix}_{_to_snake_case(attr_name)}"] = attr_val

    # --- Nested children attrs → {nested_prefix}_{snake(attr)} bronze cols ---
    for nested_child in first_child:
        nested_prefix = _NESTED_PREFIX_MAP.get(nested_child.tag)
        if nested_prefix is None:
            continue
        for attr_name, attr_val in nested_child.attrib.items():
            row[f"{nested_prefix}_{_to_snake_case(attr_name)}"] = attr_val
        # Disambiguator for ShotAtGoal's six mutually-exclusive outcome tags.
        if event_type == "ShotAtGoal" and nested_child.tag in _SHOT_OUTCOME_NAMES:
            row["shot_outcome_type"] = _SHOT_OUTCOME_NAMES[nested_child.tag]

    return row


def _scan_kickoff_times(event_path: str) -> dict[int, datetime]:
    """Pass 1 of the 2-pass DFL event parser (ADR-018 / Bug #6 fix).

    Scans ONLY KickOff events to build a ``{period: kickoff_event_time}`` map.
    Pass 2 uses this map to derive each event's period by comparing its
    EventTime to kickoff times — NOT by relying on XML stream-order
    ``current_period`` state, which DFL XML's secondary blocks (BallClaiming,
    RefereeBall, etc., emitted after the secondHalf KickOff) violate.

    Returns:
        Mapping period_id → first KickOff EventTime for that period (UTC).
        Includes only periods whose ``<KickOff GameSection=...>`` has a
        recognized GameSection per ``_SECTION_TO_PERIOD``. Empty dict for
        inputs with no KickOffs.

    Memory: O(periods) — typically O(2). Pass cost is O(events) but
    we use ET.iterparse so the parsed tree never lives in memory.
    """
    kickoff_times: dict[int, datetime] = {}

    for _ev, elem in ET.iterparse(event_path, events=("end",)):  # noqa: S314
        if elem.tag != "Event":
            if elem.tag == "PutDataRequest":
                elem.clear()
            continue

        first_child: ET.Element | None = None
        for child in elem:
            first_child = child
            break

        if first_child is None or first_child.tag != "KickOff":
            elem.clear()
            continue

        section = first_child.get("GameSection", "")
        period = _SECTION_TO_PERIOD.get(section)
        if period is None:
            logger.warning(
                "Unrecognized GameSection %r in %s — skipping KickOff event",
                section,
                event_path,
            )
            elem.clear()
            continue

        event_time_str = elem.get("EventTime", "")
        if event_time_str:
            try:
                event_dt = datetime.fromisoformat(event_time_str)
                if event_dt.tzinfo is not None:
                    event_dt = event_dt.astimezone(timezone.utc)
                # First KickOff for a period wins (defensively — DFL XML
                # should have only one per period anyway).
                if period not in kickoff_times:
                    kickoff_times[period] = event_dt
            except (ValueError, TypeError):
                pass

        elem.clear()

    return kickoff_times


def _derive_period_from_kickoffs(
    event_dt: datetime,
    kickoff_times: dict[int, datetime],
) -> tuple[int | None, datetime | None]:
    """Given an event's EventTime, return ``(period, period_kickoff_time)``.

    Period = the largest period whose ``kickoff_time`` ≤ ``event_dt``.
    Returns ``(None, None)`` if event_dt precedes all kickoffs (legitimate
    edge case — pre-match warmup events; downstream skips them).
    """
    if not kickoff_times:
        return None, None
    best_period: int | None = None
    best_start: datetime | None = None
    for p, p_start in kickoff_times.items():
        if event_dt >= p_start and (best_start is None or p_start > best_start):
            best_period = p
            best_start = p_start
    return best_period, best_start


def _parse_events_xml(
    event_path: str,
    player_team_map: dict[str, str],
    match_id: str,
    logger: logging.Logger,
    metadata: _MatchMetadata = _EMPTY_MATCH_METADATA,
) -> list[dict[str, object]]:
    """Parse DFL event XML (DFL_03_02 series) into bronze-completeness row dicts.

    Two-pass implementation (ADR-018 / Bug #6 fix, 2026-04-29):

    - **Pass 1** (``_scan_kickoff_times``): scan KickOff events to build
      ``{period: kickoff_event_time}`` map.
    - **Pass 2** (this function body): emit per-event rows with period
      derived from ``event_time`` via ``_derive_period_from_kickoffs``.

    Pre-2026-04-29 used a state-machine ``current_period`` updated at each
    KickOff in stream order. DFL XML emits secondary blocks (BallClaiming,
    RefereeBall, etc.) AFTER the secondHalf KickOff in stream order with
    first-half event_times — these were misclassified as period=2 with
    negative period-relative ``timestamp_seconds``. The 2-pass approach
    derives period from event_time, not stream-order.

    Each ``<Event>`` in the DFL XML has exactly one first-child element
    whose tag name determines the event type (``Play``, ``ShotAtGoal``,
    ``TacklingGame``, etc.). This parser extracts:

    - **Event-level attrs (13)**: renamed to bronze cols via
      ``_EVENT_LEVEL_ATTR_MAP``.
    - **First-child attrs**: prefixed per ``_EVENT_TYPE_PREFIX`` + snake_cased.
    - **Nested-child attrs**: prefixed per ``_NESTED_PREFIX_MAP`` + snake_cased.
      Six shot-outcome tags share ``shot_outcome_*`` columns with
      ``shot_outcome_type`` as the disambiguator.
    - **Derived cols**: ``match_id`` (canonical bare DFL MatchId per
      ``shared.identifiers.idsse_native_match_id``), ``event_type``,
      ``period`` (derived from event_time vs Pass-1 kickoff map),
      ``timestamp_seconds`` (period-relative), ``player_id`` (primary actor
      via ``_PLAYER_ATTR_ORDER``), ``team`` (``home``/``away``/``unknown``
      via ``player_team_map``).

    Events whose ``event_time`` precedes all KickOffs (pre-match warmup) are
    skipped — they cannot be period-attributed. Events without an EventTime
    attribute are also skipped (cannot derive period or timestamp).

    Coordinate system: DFL pitch-origin meters (x 0-105, y 0-68). Staging
    transforms to the shared 120x80 system.
    """
    canonical_match_id = idsse_native_match_id(match_id)

    # PASS 1: build {period: kickoff_time} map.
    kickoff_times = _scan_kickoff_times(event_path)
    if not kickoff_times:
        logger.warning("No KickOff events found in %s — skipping match", event_path)
        return []

    # PASS 2: emit per-event rows.
    rows: list[dict[str, object]] = []

    for _ev, elem in ET.iterparse(event_path, events=("end",)):  # noqa: S314
        if elem.tag != "Event":
            if elem.tag == "PutDataRequest":
                elem.clear()
            continue

        first_child: ET.Element | None = None
        for child in elem:
            first_child = child
            break

        if first_child is None:
            elem.clear()
            continue

        event_type = first_child.tag

        # Derive period from event_time using pass-1 map.
        event_time_str = elem.get("EventTime", "")
        period: int | None = None
        period_start: datetime | None = None
        if event_time_str:
            try:
                event_dt = datetime.fromisoformat(event_time_str)
                if event_dt.tzinfo is not None:
                    event_dt = event_dt.astimezone(timezone.utc)
                period, period_start = _derive_period_from_kickoffs(event_dt, kickoff_times)
            except (ValueError, TypeError):
                pass

        # Skip events that predate all kickoffs (pre-match warmup) or that lack
        # a parseable EventTime — neither can be period-attributed.
        if period is None or period_start is None:
            elem.clear()
            continue

        # Seed _build_event_row's period_start_time dict directly with the
        # derived value (it would otherwise compute it from the first event
        # of the period it sees, which on a single call is just this event).
        period_start_time: dict[int, datetime] = {period: period_start}

        row = _build_event_row(
            elem,
            first_child,
            event_type,
            canonical_match_id,
            period,
            player_team_map,
            period_start_time,
            metadata,
        )
        rows.append(row)
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

    new_match_ids = [mid for mid in ids_to_ingest if idsse_native_match_id(mid) not in existing_ids]
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
        # PR-LL2 Path B: source competition / season / DFL team IDs from the
        # <General> element so the bronze writer can populate the LL2 columns
        # (competition_native_id, season_native_id, home_team_id_native,
        # away_team_id_native, team_id_native).
        metadata = _parse_match_metadata(info_path)
        rows = _parse_events_xml(event_path, player_team_map, mid, logger, metadata)

        if not rows:
            logger.info("No events with position data for match %s", mid)
            continue

        df = pd.DataFrame(rows)
        # Guarantee every DFL event column lands in bronze regardless of which
        # event types appeared in this specific match — the pandas→Arrow→Spark
        # pipeline otherwise drops all-None object columns as NullType, and
        # per-match replaceWhere writes produced a thin bronze schema for months.
        df = finalize_bronze_df(
            df,
            expected_cols=_IDSSE_EVENTS_BRONZE_COLS,
            dtype_overrides=_IDSSE_EVENTS_DTYPE_OVERRIDES,
        )
        sdf = spark.createDataFrame(df)
        row_count = validate_dataframe(sdf, required_cols, "idsse_events", logger)
        replace_expr = f"match_id = '{idsse_native_match_id(mid)}'"
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


# ---------------------------------------------------------------------------
# Preflight entry point — runtime-discovered chunks for for_each_task fan-out
# ---------------------------------------------------------------------------


def _write_match_chunks_task_value(
    chunks_for_inputs: list[str],
    logger: logging.Logger,
) -> None:
    """Write the discovered chunks as a Databricks task value.

    The downstream ``ingest_idsse`` task's ``for_each_task`` reads this
    via ``"{{tasks.preflight_idsse.values.idsse_match_chunks}}"``.
    Empty list → 0 iterations spawned (no-op runs cost only the preflight
    task itself, ~30 s).

    Outside the Databricks runtime (local dev, unit tests), the
    ``dbutils`` import fails; we log a warning and return cleanly so
    the entry point remains testable.

    Args:
        chunks_for_inputs: List of comma-separated match-ID strings,
            e.g. ``["J03WMX,J03WN1", "J03WPY,J03WOH"]``. Each element
            becomes one iteration's ``{{input}}`` value.
        logger: Structured logger.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            logger.warning("No active SparkSession — task value not written")
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="idsse_match_chunks", value=chunks_for_inputs)
        logger.info(
            "Wrote task value 'idsse_match_chunks' (%d chunks)",
            len(chunks_for_inputs),
        )
    except (ImportError, AttributeError, RuntimeError) as exc:
        logger.warning("Task values not available (likely standalone mode) — %s", exc)


def main_preflight() -> None:
    """CLI entry point for the IDSSE preflight task.

    Runs the IDSSE skip guard, partitions any missing matches into
    fan-out chunks (size :attr:`_IdsseGuard.chunk_size`), and writes the
    chunks as a Databricks task value (``idsse_match_chunks``) for the
    downstream ``ingest_idsse`` ``for_each_task`` to consume.

    Behavior:
        - All 7 missing → emits 4 chunks (2,2,2,1)
        - Partial (e.g. 3 missing) → emits 2 chunks (2,1)
        - All 7 done → emits empty list ``[]`` (for_each_task spawns 0 iterations)
        - 8th match added to ``IDSSE_MATCH_IDS`` → automatically picked up
          (chunks regenerate on the next preflight run)

    The same pattern (guard returns ``FilterResult.chunks`` → preflight
    writes task value → for_each_task consumes) is the prototype for
    Cycle B+ broader fan-out activation (TODO D40a — pitch_control,
    off-ball xT, SPADL-VAEP).
    """
    args = parse_ingestion_args(
        "Preflight: discover unprocessed IDSSE matches and emit chunks "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    logger = configure_logging("idsse_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    # Serialize each chunk as a comma-separated string —
    # for_each_task's `{{input}}` interpolates the entire string, and
    # the iteration's CLI splits on comma via _parse_match_ids_arg.
    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    logger.info(
        "IDSSE preflight: %d missing matches across %d chunks (chunk_size=%d)",
        fr.count,
        len(chunks_for_inputs),
        skip_guard.chunk_size,
    )

    _write_match_chunks_task_value(chunks_for_inputs, logger)


if __name__ == "__main__":
    main()
