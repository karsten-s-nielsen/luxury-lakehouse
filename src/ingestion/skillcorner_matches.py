"""SkillCorner match metadata ingestion -- match.json to bronze.

Parses the match.json artifact, denormalizes to one row per player-match
(roster format), and writes to Delta.

Bronze table: bronze.skillcorner_matches
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import tolerate_missing_table, validate_dataframe, write_delta_table
from shared.access_tier import classify_access_tier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_MATCHES_DTYPE_OVERRIDES: dict[str, str] = {
    "player_id": "Int64",
    "team_id": "Int64",
    "jersey_number": "Int64",
    "home_team_id": "Int64",
    "away_team_id": "Int64",
    "competition_id": "Int64",
    "season_id": "Int64",
    "pitch_length": "Int64",
    "pitch_width": "Int64",
    # Playing time fields (b.1 bronze-completeness fix)
    "start_frame": "Int64",
    "end_frame": "Int64",
    "yellow_card": "Int64",
    "red_card": "Int64",
    "goal": "Int64",
    "own_goal": "Int64",
    "trackable_object": "Int64",
    "team_player_id": "Int64",
}


def parse_match_json(source: str, *, match_id: str, visibility: str) -> pd.DataFrame:
    """Parse match.json content into a roster-format DataFrame.

    Produces one row per player-match with match-level metadata
    denormalized onto every row.

    Args:
        source: JSON string content of the match.json artifact.
        match_id: Raw native SkillCorner match ID (e.g. "1886347").
        visibility: The pining ``MatchInfo.visibility`` for this match ("public" | "private").
            Persisted RAW (provenance) and used to derive ``access_tier`` (per-match HF redistribution
            policy — spec §6.2). REQUIRED: a missing value must hard-error upstream, never default.

    Returns:
        DataFrame in roster format per spec section 5.3.
    """
    data = json.loads(source)

    home_team = data["home_team"]
    away_team = data["away_team"]
    comp_edition = data["competition_edition"]
    competition = comp_edition["competition"]
    season = comp_edition["season"]

    # Build team_id -> team info lookup
    team_info: dict[int, dict[str, str]] = {
        home_team["id"]: {"name": home_team["name"], "short_name": home_team.get("short_name", "")},
        away_team["id"]: {"name": away_team["name"], "short_name": away_team.get("short_name", "")},
    }

    period_boundaries = json.dumps(data.get("match_periods", []))

    rows: list[dict[str, object]] = []
    for player in data.get("players", []):
        tid = player["team_id"]
        team = team_info.get(tid, {"name": "Unknown", "short_name": ""})
        role = player.get("player_role") or {}

        rows.append(
            {
                "match_id": match_id,
                "player_id": player["id"],
                "team_id": tid,
                "player_name": player.get("short_name", ""),
                "first_name": player.get("first_name", ""),
                "last_name": player.get("last_name", ""),
                "jersey_number": player.get("number"),
                "position_name": role.get("name", ""),
                "position_acronym": role.get("acronym", ""),
                "team_name": team["name"],
                "team_short_name": team["short_name"],
                "home_team_id": home_team["id"],
                "away_team_id": away_team["id"],
                "competition_id": competition["id"],
                "competition_name": competition["name"],
                "season_id": season["id"],
                "season_name": season["name"],
                "match_date": data.get("date_time", ""),
                "stadium_name": data.get("stadium", {}).get("name", ""),
                "pitch_length": data.get("pitch_length"),
                "pitch_width": data.get("pitch_width"),
                "period_boundaries": period_boundaries,
                # b.1 bronze-completeness: playing_time + player-level metadata
                "start_time": player.get("start_time", ""),
                "end_time": player.get("end_time", ""),
                "minutes_played": ((player.get("playing_time") or {}).get("total") or {}).get("minutes_played"),
                "start_frame": ((player.get("playing_time") or {}).get("total") or {}).get("start_frame"),
                "end_frame": ((player.get("playing_time") or {}).get("total") or {}).get("end_frame"),
                "minutes_tip": ((player.get("playing_time") or {}).get("total") or {}).get("minutes_tip"),
                "minutes_otip": ((player.get("playing_time") or {}).get("total") or {}).get("minutes_otip"),
                "yellow_card": player.get("yellow_card"),
                "red_card": player.get("red_card"),
                "injured": player.get("injured"),
                "goal": player.get("goal"),
                "own_goal": player.get("own_goal"),
                "trackable_object": player.get("trackable_object"),
                "birthday": player.get("birthday", ""),
                "gender": player.get("gender", ""),
                "team_player_id": player.get("team_player_id"),
            }
        )

    df = pd.DataFrame(rows)
    for col, dtype in _MATCHES_DTYPE_OVERRIDES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)  # type: ignore[arg-type]
    # Per-match HF redistribution policy (spec §6.2 / C1): persist RAW visibility + derived access_tier.
    df["visibility"] = visibility
    df["access_tier"] = classify_access_tier(provider="skillcorner", visibility=visibility).value
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def _assert_visibility_not_flipped(
    spark: SparkSession, catalog: str, schema: str, match_id: str, new_visibility: str, logger: logging.Logger
) -> None:
    """Raise if a non-NULL stored ``visibility`` would change to a DIFFERENT non-NULL value (spec A3 / R1).

    pining forbids in-place re-tiering, so a flip is a producer-side violation. A stored NULL/absent row
    (a backfilled or first-seen match) is "unset" and may be populated — so the Task 24 re-ingest after a
    Task 8b backfill never trips this.
    """
    table = f"{catalog}.{schema}.skillcorner_matches"
    with tolerate_missing_table(logger, f"{table} absent on first ingest — visibility check skipped"):
        rows = spark.sql(
            f"SELECT DISTINCT visibility FROM {table} WHERE match_id = '{match_id}'"  # noqa: S608
        ).collect()
        for r in rows:
            old = r["visibility"]
            if old is not None and old != new_visibility:
                msg = f"visibility flip for skillcorner match {match_id}: {old!r} -> {new_visibility!r}"
                raise RuntimeError(msg)


def write_matches(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed matches DataFrame to bronze.skillcorner_matches."""
    if "visibility" in df.columns and len(df) > 0:
        _assert_visibility_not_flipped(spark, catalog, schema, match_id, str(df["visibility"].iloc[0]), logger)
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "player_id", "team_id"],
        "skillcorner_matches",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_matches",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
