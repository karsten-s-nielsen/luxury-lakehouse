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

from ingestion.utils import tolerate_missing_table, validate_dataframe, write_delta_table
from shared.access_tier import classify_access_tier
from shared.identifiers import gradientsports_native_match_id

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Fields that json_normalize leaves as list/dict — serialize to JSON strings.
# Note: homeTeamKit/awayTeamKit are dicts that json_normalize flattens to
# individual columns (homeTeamKit.primary, etc.), so they don't need serialization.
# Only stadium.pitches remains as a list that json_normalize keeps as-is.
_COMPLEX_FIELDS = ("stadium.pitches",)


def parse_metadata(source: str | dict | list, *, match_id: str, visibility: str) -> pd.DataFrame:
    """Parse Gradient Sports metadata into a DataFrame.

    Args:
        source: Raw metadata (JSON string, dict, or list).
        match_id: Native match ID — validated via identifiers.py generator.
        visibility: The pining ``MatchInfo.visibility`` ("public" | "private"). Persisted RAW +
            used to derive ``access_tier`` (per-match HF redistribution policy — spec §6.2). REQUIRED.

    Returns:
        DataFrame with metadata columns + match_id + visibility + access_tier + _ingested_at.
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
    # Per-match HF redistribution policy (spec §6.2 / C1): persist RAW visibility + derived access_tier.
    df["visibility"] = visibility
    df["access_tier"] = classify_access_tier(provider="gradientsports", visibility=visibility).value
    df["_ingested_at"] = datetime.now(timezone.utc)
    return df


def _assert_visibility_not_flipped(
    spark: SparkSession, catalog: str, schema: str, match_id: str, new_visibility: str, logger: logging.Logger
) -> None:
    """Raise if a non-NULL stored ``visibility`` would change to a DIFFERENT non-NULL value (spec A3 / R1).

    A stored NULL/absent row is "unset" and may be populated (so a Task 8b backfill reconciled by a later
    re-ingest never trips this). pining forbids re-tiering, so a real flip is a producer-side violation.
    """
    table = f"{catalog}.{schema}.gradientsports_metadata"
    with tolerate_missing_table(logger, f"{table} absent on first ingest — visibility check skipped"):
        rows = spark.sql(
            f"SELECT DISTINCT visibility FROM {table} WHERE match_id = '{match_id}'"  # noqa: S608
        ).collect()
        for r in rows:
            old = r["visibility"]
            if old is not None and old != new_visibility:
                msg = f"visibility flip for gradientsports match {match_id}: {old!r} -> {new_visibility!r}"
                raise RuntimeError(msg)


def write_metadata(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed metadata DataFrame to bronze.gradientsports_metadata."""
    if "visibility" in df.columns and len(df) > 0:
        _assert_visibility_not_flipped(spark, catalog, schema, match_id, str(df["visibility"].iloc[0]), logger)
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
