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
)
from ingestion.utils import (
    fetch_url,
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

    logger.info("Parsed %d events for %s", len(df), match_id)
    return df


# ---------------------------------------------------------------------------
# Ingestion orchestration
# ---------------------------------------------------------------------------


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
