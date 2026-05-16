"""SkillCorner events ingestion -- dynamic_events.csv to bronze.

Reads the dynamic_events CSV artifact from the pining-for-the-data API,
adds match_id and _ingested_at audit column, and writes to Delta.

Bronze table: bronze.skillcorner_events
Coordinate system: POSSESSION_PERSPECTIVE (center-origin meters, preserved as-is).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import IO, TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_events_csv(source: str | IO[str], *, match_id: str) -> pd.DataFrame:
    """Parse a dynamic_events CSV into a pandas DataFrame.

    All source columns are preserved (bronze-completeness principle).
    Adds ``match_id`` (raw native ID) and ``_ingested_at`` (UTC timestamp).

    Args:
        source: File path or file-like object containing the CSV data.
        match_id: Raw native SkillCorner match ID (e.g. "1886347").

    Returns:
        DataFrame with all source columns plus match_id and _ingested_at.
    """
    df = pd.read_csv(source, low_memory=False)
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
    """Write parsed events DataFrame to bronze.skillcorner_events.

    Uses replaceWhere on match_id for idempotent writes.
    """
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id", "event_id", "event_type"],
        "skillcorner_events",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "skillcorner_events",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
