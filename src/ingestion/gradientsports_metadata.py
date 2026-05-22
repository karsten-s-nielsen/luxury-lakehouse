"""Gradient Sports metadata ingestion — metadata artifact to bronze.

Parses the metadata artifact from the pining-for-the-data API and writes to
bronze.gradientsports_metadata. One row per match.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table
from shared.identifiers import gradientsports_native_match_id

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Fields that json_normalize leaves as list/dict — serialize to JSON strings.
# Note: homeTeamKit/awayTeamKit are dicts that json_normalize flattens to
# individual columns (homeTeamKit.primary, etc.), so they don't need serialization.
# Only stadium.pitches remains as a list that json_normalize keeps as-is.
_COMPLEX_FIELDS = ("stadium.pitches",)


def parse_metadata(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports metadata into a DataFrame.

    Args:
        source: Raw metadata (JSON string, dict, or list).
        match_id: Native match ID — validated via identifiers.py generator.

    Returns:
        DataFrame with metadata columns + match_id + _ingested_at.
    """
    validated_match_id = gradientsports_native_match_id(match_id)

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    # API wraps metadata in a 1-element list
    if isinstance(data, list):
        if len(data) == 0:
            raise ValueError(f"Empty metadata list for match {validated_match_id}")
        data = data[0]

    df = pd.json_normalize(data)  # type: ignore[arg-type]

    # Serialize complex fields to JSON strings for Delta compatibility
    for col in _COMPLEX_FIELDS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v)

    # Widen all integer columns to float64 (same pattern as events)
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype("float64")

    df["match_id"] = validated_match_id
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_metadata(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed metadata DataFrame to bronze.gradientsports_metadata."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id"],
        "gradientsports_metadata",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_metadata",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
