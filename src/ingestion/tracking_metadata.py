"""Extract tracking player metadata from source data files.

Reads player names and team names from IDSSE DFL match info XMLs and
SkillCorner match metadata via kloppy, writing to a bronze Delta table
for downstream dbt resolution into fct_player_positions/fct_position_maps.

Metrica matches are anonymised — no player names exist in the source data,
so we rely on the COALESCE fallback (raw player_id) in the app queries.

Usage (Databricks):
    extract_tracking_metadata --catalog soccer_analytics --schema bronze

References:
    IDSSE: DFL_02_01 match information XML format.
    SkillCorner: kloppy open data API.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import xml.etree.ElementTree as ET  # nosemgrep: use-defused-xml -- trusted DFL XML from UC Volume, not untrusted input
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult
from shared.constants import IDENTIFIER_RE
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

TABLE_NAME = "tracking_player_metadata"


class _TrackingMetadataGuard:
    workflow_id = "wf-tracking-metadata"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _TrackingMetadataGuard()

# IDSSE match IDs and competition mapping (mirrors idsse.py)
_IDSSE_MATCH_IDS = ["J03WMX", "J03WN1", "J03WPY", "J03WOH", "J03WQQ", "J03WOY", "J03WR9"]
_MATCH_COMPETITION: dict[str, str] = {
    "J03WMX": "DFL-COM-000001",
    "J03WN1": "DFL-COM-000001",
    "J03WPY": "DFL-COM-000002",
    "J03WOH": "DFL-COM-000002",
    "J03WQQ": "DFL-COM-000002",
    "J03WOY": "DFL-COM-000002",
    "J03WR9": "DFL-COM-000002",
}
_IDSSE_DATA_DIR = "/Volumes/soccer_analytics/bronze/libs/idsse_data"

# SkillCorner match IDs (mirrors skillcorner.py)
_SKILLCORNER_MATCH_IDS = [
    "1886347",
    "1899585",
    "1925299",
    "1953632",
    "1996435",
    "2006229",
    "2011166",
    "2013725",
    "2015213",
    "2017461",
]


# ---------------------------------------------------------------------------
# Structured JSON logging (mirrors src/ingestion/utils.py pattern)
# ---------------------------------------------------------------------------


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for Databricks log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, object] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "source": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, default=str)


def _configure_logging() -> logging.Logger:
    """Create a logger that emits JSON lines to stdout."""
    logger = logging.getLogger("tracking_metadata")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


# ---------------------------------------------------------------------------
# IDSSE metadata extraction
# ---------------------------------------------------------------------------


def _extract_idsse_metadata(data_dir: str, logger: logging.Logger) -> list[dict[str, object]]:
    """Extract player + team metadata from IDSSE DFL match info XMLs.

    Reads ``<Team>`` and ``<Player>`` elements from each match info XML.
    Uses ``ShortName`` for player/team display names where available,
    falling back to ``PersonId``/``TeamId``.
    """
    rows: list[dict[str, object]] = []

    for mid in _IDSSE_MATCH_IDS:
        comp = _MATCH_COMPETITION[mid]
        info_path = f"{data_dir}/DFL_02_01_matchinformation_{comp}_DFL-MAT-{mid}.xml"
        match_id = f"idsse_{mid}"

        try:
            tree = ET.parse(info_path)  # noqa: S314  # nosemgrep: use-defused-xml-parse
            root = tree.getroot()
        except Exception:
            logger.warning("Could not parse IDSSE match info XML: %s", info_path, exc_info=True)
            continue

        # Extract team names from <General> element (HomeTeamName, GuestTeamName)
        general_el = next(root.iter("General"), None)
        home_team_name = (general_el.get("HomeTeamName") or "Home") if general_el is not None else "Home"
        guest_team_name = (general_el.get("GuestTeamName") or "Away") if general_el is not None else "Away"

        match_count = 0
        for team_el in root.iter("Team"):
            role = team_el.get("Role", "")

            if role == "home":
                team_side = "home"
                team_name = team_el.get("TeamName") or home_team_name
            elif role == "guest":
                team_side = "away"
                team_name = team_el.get("TeamName") or guest_team_name
            else:
                continue

            for player_el in team_el.iter("Player"):
                person_id = player_el.get("PersonId", "")
                if not person_id:
                    continue

                # DFL uses Shortname (lowercase 'n'), also try FirstName+LastName
                player_name = (
                    player_el.get("Shortname")
                    or player_el.get("ShortName")
                    or f"{player_el.get('FirstName', '')} {player_el.get('LastName', '')}".strip()
                    or person_id
                )

                jersey_str = player_el.get("ShirtNumber", "")
                jersey_number = int(jersey_str) if jersey_str.isdigit() else None

                rows.append(
                    {
                        "match_id": match_id,
                        "player_id": person_id,
                        "player_display_name": player_name,
                        "team_side": team_side,
                        "team_display_name": team_name,
                        "jersey_number": jersey_number,
                        "provider": "idsse",
                    }
                )
                match_count += 1

        logger.info("Extracted %d player metadata rows from IDSSE match %s", match_count, match_id)

    return rows


# ---------------------------------------------------------------------------
# SkillCorner metadata extraction
# ---------------------------------------------------------------------------


def _extract_skillcorner_metadata(logger: logging.Logger) -> list[dict[str, object]]:
    """Extract player + team metadata from SkillCorner via kloppy.

    Uses ``dataset.metadata.teams[*].players`` to access player names
    without iterating tracking frames.
    """
    from kloppy import skillcorner  # type: ignore[import-not-found]

    rows: list[dict[str, object]] = []

    for mid in _SKILLCORNER_MATCH_IDS:
        match_id = f"skillcorner_{mid}"

        try:
            dataset = skillcorner.load_open_data(
                match_id=mid,
                coordinates="skillcorner",
                include_empty_frames=False,
            )
        except Exception:
            logger.warning("Could not load SkillCorner match %s via kloppy", mid, exc_info=True)
            continue

        teams = dataset.metadata.teams  # type: ignore[union-attr]
        home_team = teams[0]
        away_team = teams[1]

        team_entries = [
            (home_team, "home"),
            (away_team, "away"),
        ]

        match_count = 0
        for team_obj, team_side in team_entries:
            team_name = getattr(team_obj, "name", None) or team_side.title()
            players = getattr(team_obj, "players", []) or []

            for player in players:
                player_id = str(getattr(player, "player_id", ""))
                if not player_id:
                    continue

                player_name = getattr(player, "name", None) or getattr(player, "full_name", None) or player_id

                jersey = getattr(player, "jersey_no", None)

                rows.append(
                    {
                        "match_id": match_id,
                        "player_id": player_id,
                        "player_display_name": player_name,
                        "team_side": team_side,
                        "team_display_name": team_name,
                        "jersey_number": int(jersey) if jersey else None,
                        "provider": "skillcorner",
                    }
                )
                match_count += 1

        logger.info("Extracted %d player metadata rows from SkillCorner match %s", match_count, match_id)

    return rows


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


@workflow("wf-tracking-metadata", phase="import")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    data_dir: str = _IDSSE_DATA_DIR,
    *,
    ctx: object = None,
) -> None:
    """Extract tracking metadata from all providers and write to bronze."""
    from pyspark.sql import functions as spark_fn  # type: ignore[import-not-found]

    from ingestion.utils import validate_dataframe, write_delta_table

    logger = logging.getLogger("tracking_metadata")

    # ------------------------------------------------------------------
    # 1. Extract metadata from each provider
    # ------------------------------------------------------------------
    all_rows: list[dict[str, object]] = []

    logger.info("Extracting IDSSE metadata from %s", data_dir)
    all_rows.extend(_extract_idsse_metadata(data_dir, logger))

    logger.info("Extracting SkillCorner metadata via kloppy")
    all_rows.extend(_extract_skillcorner_metadata(logger))

    logger.info("Total metadata rows: %d", len(all_rows))

    if not all_rows:
        logger.info("No metadata extracted — skipping write")
        return

    # ------------------------------------------------------------------
    # 2. Write to bronze Delta table (full overwrite — table is small)
    # ------------------------------------------------------------------
    pdf = pd.DataFrame(all_rows)
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.withColumn("_ingested_at", spark_fn.current_timestamp())

    table = f"{catalog}.{schema}.{TABLE_NAME}"
    required_cols = ["match_id", "player_id", "player_display_name", "team_side", "team_display_name", "provider"]
    row_count = validate_dataframe(sdf, required_cols, TABLE_NAME, logger)

    start = time.time()
    write_delta_table(
        sdf,
        catalog,
        schema,
        TABLE_NAME,
        logger=logger,
        row_count=row_count,
    )
    elapsed = time.time() - start
    logger.info("Wrote %d rows to %s in %.2fs", row_count, table, elapsed)


def main() -> None:
    """CLI entry point for tracking metadata extraction."""
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    _configure_logging()

    parser = argparse.ArgumentParser(description="Extract tracking player metadata to Delta")
    parser.add_argument(
        "--catalog",
        default="soccer_analytics",
        help="Unity Catalog name (default: soccer_analytics)",
    )
    parser.add_argument(
        "--schema",
        default="bronze",
        help="Schema name (default: bronze)",
    )
    parser.add_argument(
        "--data-dir",
        default=_IDSSE_DATA_DIR,
        help="IDSSE data directory on UC Volume (default: %(default)s)",
    )
    args = parser.parse_args()

    catalog: str = args.catalog
    schema: str = args.schema
    data_dir: str = args.data_dir

    for field_name, value in [("catalog", catalog), ("schema", schema)]:
        if not IDENTIFIER_RE.match(value):
            msg = f"Invalid {field_name} name '{value}': must match {IDENTIFIER_RE.pattern}"
            raise SystemExit(msg)

    spark = SparkSession.builder.getOrCreate()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, catalog, schema)

    run_pipeline(spark, catalog, schema, data_dir)


if __name__ == "__main__":
    main()
