"""Gradient Sports event ingestion — events artifact to bronze.

Parses the event artifact from the pining-for-the-data API and writes to
bronze.gradientsports_events. Format details discovered at runtime from
the API response.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_events(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports events into a DataFrame.

    Args:
        source: Raw event data (JSON string, dict, or list).
        match_id: Native match ID.

    Returns:
        DataFrame with event columns + match_id + _ingested_at.
    """
    import json

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    # Handle both list-of-events and dict-with-events-key formats
    if isinstance(data, dict):
        events = data.get("events", data.get("data", []))
    else:
        events = data

    df = pd.json_normalize(events)  # type: ignore[arg-type]
    df["match_id"] = match_id
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_events(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed events DataFrame to bronze.gradientsports_events."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id"],
        "gradientsports_events",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_events",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
