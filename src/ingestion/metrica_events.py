"""Metrica Sports event data ingestion (Games 1-3).

Games 1-2: CSV format with standard column headers.
Game 3: FIFA EPTS JSON format — parsed via shared EPTS utilities in
``metrica_common``.

Bronze table produced: ``metrica_events``
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.metrica_common import (
    _BASE_URL,
    _COLUMN_CLEAN_RE,
    _EPTS_URLS,
    _parse_epts_events,
    _parse_epts_metadata,
)
from ingestion.utils import (
    fetch_url,
    finalize_bronze_df,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_EVENT_URLS: dict[str, str] = {
    "Sample_Game_1": f"{_BASE_URL}/Sample_Game_1/Sample_Game_1_RawEventsData.csv",
    "Sample_Game_2": f"{_BASE_URL}/Sample_Game_2/Sample_Game_2_RawEventsData.csv",
}


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

    # Bronze-completeness: CSV-path events must carry the same schema as
    # EPTS-path events (Game 3) so both write into metrica_events without
    # schema drift. CSV source has no pitch dims or multi-subtype info.
    # float NaN yields dtype float64; pd.array(string) yields StringDtype —
    # both avoid Spark NullType inference that would collide with EPTS writes.
    df["pitch_length_m"] = float("nan")
    df["pitch_width_m"] = float("nan")
    df["subtypes_all_json"] = pd.array([None] * len(df), dtype="string")

    logger.info("Parsed %d events for %s", len(df), match_id)
    return df


def _augment_ll2_metadata(df: pd.DataFrame, match_id: str) -> pd.DataFrame:
    """Append PR-LL2 Path B metadata columns to a Metrica events DataFrame.

    Called from ``ingest_events`` after both CSV (Games 1-2) and EPTS
    (Game 3) parsers, so a single augmentation policy applies. Adds 5
    string columns per the bronze.metrica_events contract:

    - ``competition_native_id`` (constant ``'metrica-sample'``)
    - ``season_native_id`` (constant ``'metrica-open-2017'``)
    - ``home_team_id_native`` (synthetic ``f"{match_id}-Home"``)
    - ``away_team_id_native`` (synthetic ``f"{match_id}-Away"``)
    - ``team_id_native`` (per-row: home id when ``team=='Home'``,
      away id when ``team=='Away'``, NULL otherwise)

    Returns the DataFrame mutated in place + returned for chain-style use.
    """
    home_id_native, away_id_native = _native_team_ids(match_id)
    df["competition_native_id"] = _METRICA_COMPETITION_NATIVE_ID
    df["season_native_id"] = _METRICA_SEASON_NATIVE_ID
    df["home_team_id_native"] = home_id_native
    df["away_team_id_native"] = away_id_native
    if "team" in df.columns:
        df["team_id_native"] = (
            df["team"]
            .map(
                lambda t: home_id_native if t == "Home" else (away_id_native if t == "Away" else None),
            )
            .astype("string")
        )
    else:
        df["team_id_native"] = pd.array([None] * len(df), dtype="string")
    return df


# ---------------------------------------------------------------------------
# Ingestion orchestration
# ---------------------------------------------------------------------------


_METRICA_EVENTS_BRONZE_COLS: frozenset[str] = frozenset(
    {
        "event_id",
        "type",
        "subtype",
        "subtypes_all_json",
        "period",
        "start_frame",
        "end_frame",
        "start_time_s",
        "end_time_s",
        "start_x",
        "start_y",
        "end_x",
        "end_y",
        "team",
        "player",
        "to",
        "match_id",
        "pitch_length_m",
        "pitch_width_m",
        # PR-LL2 Path B: match-level metadata + per-row team identity for
        # downstream SPADL conversion + dim_teams / dim_competitions joins.
        # Provider-agnostic naming convention (matches bronze.idsse_events
        # Path B additions). For Metrica's open-data sample, competition +
        # season + team IDs are synthetic but stable + unique.
        "competition_native_id",  # constant 'metrica-sample' across all rows
        "season_native_id",  # constant 'metrica-open-2017' across all rows
        "home_team_id_native",  # f"{match_id}-Home" (synthetic, per match)
        "away_team_id_native",  # f"{match_id}-Away"
        "team_id_native",  # home/away id of the row's acting team; NULL otherwise
    },
)
"""Bronze-completeness contract for bronze.metrica_events. Union of CSV-path
(Games 1-2) and EPTS-path (Game 3) columns. `subtypes_all_json` carries
multi-subtype events that CSV lacks; `pitch_*_m` carries EPTS metadata.

Source-faithful coordinate frame: ``start_x``/``start_y``/``end_x``/``end_y``
are stored as Metrica-native [0, 1] normalised values (NOT transformed to
SPADL meters). Per the bronze stability principle, transformations live in
silver/staging or in adapters — bronze preserves the source frame.
Downstream consumers wanting SPADL meters multiply by ``pitch_length_m``
(default 105) / ``pitch_width_m`` (default 68)."""


# PR-LL2 Path B: constants for the LL2 metadata columns on bronze.metrica_events.
# Single sample dataset → single competition + season string. dim_competitions
# already uses ``provider='metrica', native_competition_id='metrica-sample'``.
_METRICA_COMPETITION_NATIVE_ID = "metrica-sample"
_METRICA_SEASON_NATIVE_ID = "metrica-open-2017"


def _native_team_ids(match_id: str) -> tuple[str, str]:
    """Synthesize stable home/away team identifiers for a Metrica sample match.

    Metrica's open-data sample doesn't expose actual club identifiers — events
    only carry "Home" / "Away" labels. Returns deterministic synthetic IDs
    keyed on ``match_id`` so downstream ``dim_teams`` lookups can match
    against ``provider='metrica', native_team_id='Sample_Game_1-Home'``.
    """
    return f"{match_id}-Home", f"{match_id}-Away"


_METRICA_EVENTS_DTYPE_OVERRIDES: dict[str, str] = {
    "event_id": "Int64",
    "period": "Int64",
    "start_frame": "Int64",
    "end_frame": "Int64",
    "start_time_s": "Float64",
    "end_time_s": "Float64",
    "start_x": "Float64",
    "start_y": "Float64",
    "end_x": "Float64",
    "end_y": "Float64",
    "pitch_length_m": "Float64",
    "pitch_width_m": "Float64",
}


def ingest_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Download and ingest event data per match to avoid OOM on batch concat."""
    required_cols = ["event_id", "type", "period", "start_frame", "end_frame", "team", "player", "match_id"]

    from ingestion.utils import tolerate_missing_table

    # Incremental skip: check which matches already exist in the Delta table
    all_match_ids = list(_EVENT_URLS.keys()) + list(_EPTS_URLS.keys())
    existing_ids: set[str] = set()
    with tolerate_missing_table(logger, "No existing metrica_events table — processing all matches"):
        existing_rows = spark.table(f"{catalog}.{schema}.metrica_events").select("match_id").distinct().collect()
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
    for match_id, url in _EVENT_URLS.items():
        if match_id in existing_ids:
            logger.info("Events for %s already ingested — skipping", match_id)
            continue
        events_df = _download_and_parse_events(url, match_id, logger)
        events_df = _augment_ll2_metadata(events_df, match_id)
        events_df = finalize_bronze_df(
            events_df,
            expected_cols=_METRICA_EVENTS_BRONZE_COLS,
            dtype_overrides=_METRICA_EVENTS_DTYPE_OVERRIDES,
        )
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
        if match_id in existing_ids:
            logger.info("Events for %s already ingested — skipping", match_id)
            continue
        # Load metadata first so events carry pitch dims (schema parity with CSV).
        logger.info("Downloading EPTS metadata for %s", match_id)
        metadata_resp = fetch_url(urls["metadata"])
        metadata = _parse_epts_metadata(metadata_resp.text)
        logger.info("Downloading EPTS events for %s", match_id)
        resp = fetch_url(urls["events"])
        events_json = resp.json()
        events_data: list[dict[str, object]] = events_json.get("data", events_json)
        events_df = _parse_epts_events(events_data, match_id, metadata)
        events_df = _augment_ll2_metadata(events_df, match_id)
        events_df = finalize_bronze_df(
            events_df,
            expected_cols=_METRICA_EVENTS_BRONZE_COLS,
            dtype_overrides=_METRICA_EVENTS_DTYPE_OVERRIDES,
        )
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
