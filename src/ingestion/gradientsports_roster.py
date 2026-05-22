"""Gradient Sports roster ingestion — roster artifact to bronze.

Parses the roster artifact from the pining-for-the-data API and writes to
bronze.gradientsports_roster. One row per player per match (~51 rows/match).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import validate_dataframe, write_delta_table
from shared.identifiers import (
    gradientsports_native_match_id,
    gradientsports_native_player_id,
    gradientsports_native_team_id,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def parse_roster(source: str | dict | list, *, match_id: str) -> pd.DataFrame:
    """Parse Gradient Sports roster into a DataFrame.

    Args:
        source: Raw roster data (JSON string or list of dicts).
        match_id: Native match ID — validated via identifiers.py generator.

    Returns:
        DataFrame with roster columns + match_id + _ingested_at.
    """
    validated_match_id = gradientsports_native_match_id(match_id)

    if isinstance(source, str):
        data = json.loads(source)
    else:
        data = source

    if isinstance(data, dict):
        data = data.get("roster", data.get("data", []))

    df = pd.json_normalize(data)  # type: ignore[arg-type]

    # Validate player.id and team.id via identifiers.py generators (ADR-018)
    if "player.id" in df.columns:
        for val in df["player.id"].dropna().unique():
            gradientsports_native_player_id(val)
    if "team.id" in df.columns:
        for val in df["team.id"].dropna().unique():
            gradientsports_native_team_id(val)

    # Widen all integer columns to float64 (same pattern as events)
    for col in df.select_dtypes(include=["int64", "int32"]).columns:
        df[col] = df[col].astype("float64")

    df["match_id"] = validated_match_id
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def write_roster(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed roster DataFrame to bronze.gradientsports_roster."""
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["match_id"],
        "gradientsports_roster",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "gradientsports_roster",
        replace_where=f"match_id = '{match_id}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count
