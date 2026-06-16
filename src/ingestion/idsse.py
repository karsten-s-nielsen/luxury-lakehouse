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
import re
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

# Frame rate for all IDSSE matches (DFL position data is 25fps)
_FRAME_RATE = 25

# Pre-compiled regex for DFL event XML filenames (DFL_03_02 series)
_EVENT_FILE_RE = re.compile(r"DFL_03_02_.*DFL-MAT-([A-Za-z0-9]+)\.xml$")

# ---------------------------------------------------------------------------
# Bronze-completeness schema constants. The DFL parser itself moved to the
# silly-kicks parse port (``silly_kicks.providers.sportec``) under
# delete-and-depend (ADR-031 T3 / Gate B); these maps survive because
# ``_compute_idsse_events_bronze_cols`` still derives the expected
# ``bronze.idsse_events`` column set (passed to ``finalize_bronze_df``) from
# them + ``_to_snake_case``, guaranteeing every DFL attribute lands in Delta.
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

    # shot_outcome_type disambiguator — the parser (now in the silly-kicks port)
    # emits it whenever a ShotAtGoal has one of the six nested outcome tags.
    cols.add("shot_outcome_type")

    return frozenset(cols)


_IDSSE_EVENTS_BRONZE_COLS: frozenset[str] = _compute_idsse_events_bronze_cols()
"""Expected bronze columns for bronze.idsse_events (computed at import time)."""


# Single source of truth for the match-level metadata fields the silly-kicks
# DFL parse port surfaces to BOTH bronze tables. The parser moved to the port
# under delete-and-depend (ADR-031 T3 / Gate B); this constant is retained as
# the documented shared-metadata contract — both `bronze.idsse_events` and
# `bronze.idsse_tracking` carry a per-row column for each name here (closing
# the session-69 asymmetric-coverage gap, 2026-04-30).
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


# Bronze-completeness contract for bronze.idsse_tracking.
# Every column here is emitted by the silly-kicks DFL parse port's tracking
# parser (the lakehouse parser moved to the port under delete-and-depend,
# ADR-031 T3 / Gate B); `ingest_idsse` passes this set to `finalize_bronze_df`
# so every column lands in Delta regardless of which Frame attrs happen to be
# populated for a given match's rows.
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

        # silly-kicks DFL parse port (ADR-031 T3 / Gate B): parse the whole positions XML to RAW
        # bronze (no smoothing — data-quality stays consumer-side per ADR-031 §4.5). The lakehouse
        # keeps _smooth_tracking + finalize + write below. Output is byte-identical to the retired
        # lakehouse parser (parity-guarded by test_dfl_parse_port_golden). NOTE: the port parses the
        # whole match at once, so this gives up the prior per-half peak-memory halving (acceptable at
        # IDSSE scale on the 16 GB driver); the per-period write loop is preserved.
        # Function-body import: idsse.py is a guard module (_GUARD_MODULES) — silly-kicks may
        # NOT be imported at module level (guard-import isolation, test_guard_conformance).
        from silly_kicks.providers.sportec import parse_dfl_match_info, parse_dfl_tracking

        match_info = parse_dfl_match_info(info_path)
        logger.info(
            "Found %d players in match info (%d GKs) — competition=%s, season=%s",
            len(match_info.player_team_map),
            len(match_info.gk_player_ids),
            match_info.competition_id or "<missing>",
            match_info.season_id or "<missing>",
        )
        bronze_all = pd.DataFrame(parse_dfl_tracking(pos_path, match_info=match_info, match_id=mid))
        logger.info("Parsed %d tracking rows for IDSSE match %s", len(bronze_all), mid)

        for period, period_df in bronze_all.groupby("period"):
            period_int = int(period)  # type: ignore[arg-type]  # groupby key is the int `period` column
            df = pd.DataFrame(period_df).reset_index(drop=True)
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
            replace_expr = f"match_id = '{idsse_native_match_id(mid)}' AND period = {period_int}"
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
        del bronze_all
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

        # silly-kicks DFL parse port (ADR-031 T3): parse events XML to RAW bronze rows. The port
        # stamps the LL2 metadata columns (competition_native_id, season_native_id, home/away_team_
        # id_native, team_id_native) internally. Output byte-identical to the retired lakehouse parser.
        # Function-body import (guard-import isolation — see ingest_idsse).
        from silly_kicks.providers.sportec import parse_dfl_events, parse_dfl_match_info

        match_info = parse_dfl_match_info(info_path)
        df = pd.DataFrame(parse_dfl_events(event_path, match_info=match_info, match_id=mid))

        if df.empty:
            logger.info("No events with position data for match %s", mid)
            continue
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
