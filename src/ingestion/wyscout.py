"""Wyscout public event data ingestion into the Databricks bronze layer.

Supports both local files (``--data-dir`` for dev speed) and HTTPS download
from Figshare for Databricks runtime.

7 competitions (2017/18 season):
  England, Italy, Spain, France, Germany, European Championship, World Cup

The Figshare dataset stores events and matches as ZIP archives containing
one JSON file per competition (e.g. ``events_England.json``).

JSON columns (``positions``, ``tags``) are serialized to JSON strings before
Delta write so dbt staging models can parse them with SQL JSON functions.

Bronze tables produced:
  - wyscout_events
  - wyscout_matches
"""

from __future__ import annotations

import io
import json
import logging
import pathlib
import zipfile
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils import (
    configure_logging,
    fetch_url,
    get_spark_session,
    validate_dataframe,
    write_delta_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Figshare HTTPS URLs for Wyscout open data (ZIP archives)
# Source: https://figshare.com/collections/Soccer_match_event_dataset/4415000
# IMPORTANT: Use ndownloader.figshare.com subdomain (figshare.com/ndownloader returns 202)
_EVENTS_ZIP_URL = "https://ndownloader.figshare.com/files/14464685"
_MATCHES_ZIP_URL = "https://ndownloader.figshare.com/files/14464622"

_COMPETITIONS = [
    "England",
    "Italy",
    "Spain",
    "France",
    "Germany",
    "European_Championship",
    "World_Cup",
]


# ---------------------------------------------------------------------------
# JSON column serialization
# ---------------------------------------------------------------------------


def _serialize_json_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Serialize specified columns from Python objects to JSON strings."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.dumps(v, default=str) if isinstance(v, dict | list) else v)
    return df


def _normalize_mixed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize mixed-type object columns so PyArrow/Spark can convert them.

    After pd.concat across competitions, some columns end up as ``object``
    dtype with a mix of int/float/NaN or date/string values.  PyArrow cannot
    infer a single Arrow type from these heterogeneous Series.  We coerce
    numeric-looking columns to numeric and cast the rest to strings.
    """
    for col in df.columns:
        if df[col].dtype != object:
            continue
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]
        if isinstance(first, int | float):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif isinstance(first, dict | list):
            pass  # already handled by _serialize_json_columns
        else:
            df[col] = df[col].astype(str)
    return df


# ---------------------------------------------------------------------------
# Data loading (local-first with ZIP download fallback)
# ---------------------------------------------------------------------------


def _load_json_local(path: pathlib.Path, logger: logging.Logger) -> pd.DataFrame | None:
    """Attempt to load a JSON file from a local path."""
    if path.exists():
        logger.info("Loading local file: %s", path)
        return pd.read_json(path)
    return None


def _download_and_extract_zip(url: str, logger: logging.Logger) -> dict[str, pd.DataFrame]:
    """Download a ZIP archive and extract JSON files into DataFrames.

    Returns:
        Mapping of competition name → DataFrame.
    """
    logger.info("Downloading ZIP from %s", url)
    resp = fetch_url(url, timeout=(10, 120))
    zf = zipfile.ZipFile(io.BytesIO(resp.content))

    result: dict[str, pd.DataFrame] = {}
    for name in zf.namelist():
        if not name.endswith(".json"):
            continue
        # Extract competition name from filename (e.g. "events_England.json" -> "England")
        base = name.rsplit(".", 1)[0]  # "events_England"
        parts = base.split("_", 1)  # ["events", "England"]
        competition = parts[1] if len(parts) > 1 else base

        data = json.loads(zf.read(name))
        df = pd.DataFrame(data)
        result[competition] = df
        logger.info("Extracted %d rows from %s", len(df), name)

    return result


def _load_all_competitions(
    zip_url: str,
    data_dir: pathlib.Path | None,
    file_prefix: str,
    logger: logging.Logger,
) -> dict[str, pd.DataFrame]:
    """Load data for all competitions, trying local first then ZIP download.

    Args:
        zip_url: Figshare ZIP download URL.
        data_dir: Optional local data directory with pre-extracted JSON files.
        file_prefix: Filename prefix (``events`` or ``matches``).
        logger: Logger instance.

    Returns:
        Mapping of competition name → DataFrame.
    """
    result: dict[str, pd.DataFrame] = {}

    # Try local files first
    if data_dir is not None:
        for competition in _COMPETITIONS:
            local_path = data_dir / f"{file_prefix}_{competition}.json"
            df = _load_json_local(local_path, logger)
            if df is not None:
                result[competition] = df

    # If local provided all competitions, skip download
    if len(result) == len(_COMPETITIONS):
        return result

    # Download ZIP for any missing competitions
    if len(result) < len(_COMPETITIONS):
        try:
            zip_data = _download_and_extract_zip(zip_url, logger)
            for comp, df in zip_data.items():
                if comp not in result:
                    result[comp] = df
        except Exception:
            logger.exception("Failed to download ZIP from %s", zip_url)

    return result


# ---------------------------------------------------------------------------
# Event ingestion
# ---------------------------------------------------------------------------


def ingest_events(
    spark: SparkSession,
    catalog: str,
    schema: str,
    data_dir: pathlib.Path | None,
    logger: logging.Logger,
) -> None:
    """Load and write Wyscout events for all competitions."""
    comp_data = _load_all_competitions(_EVENTS_ZIP_URL, data_dir, "events", logger)

    all_events: list[pd.DataFrame] = []
    for competition, df in comp_data.items():
        if not df.empty:
            df["competition_name"] = competition
            all_events.append(df)
            logger.info("Loaded %d events for %s", len(df), competition)

    if not all_events:
        msg = "No Wyscout event data loaded — all downloads failed"
        raise RuntimeError(msg)

    combined = pd.concat(all_events, ignore_index=True)

    # Serialize JSON columns (positions and tags)
    combined = _serialize_json_columns(combined, ["positions", "tags"])
    combined = _normalize_mixed_types(combined)

    sdf = spark.createDataFrame(combined)
    validate_dataframe(
        sdf,
        ["eventId", "matchId", "eventName", "playerId", "teamId", "matchPeriod", "eventSec"],
        "wyscout_events",
        logger,
    )
    write_delta_table(sdf, catalog, schema, "wyscout_events", mode="overwrite", logger=logger)


# ---------------------------------------------------------------------------
# Match ingestion
# ---------------------------------------------------------------------------


def ingest_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    data_dir: pathlib.Path | None,
    logger: logging.Logger,
) -> None:
    """Load and write Wyscout match metadata for all competitions."""
    comp_data = _load_all_competitions(_MATCHES_ZIP_URL, data_dir, "matches", logger)

    all_matches: list[pd.DataFrame] = []
    for competition, df in comp_data.items():
        if not df.empty:
            df["competition_name"] = competition
            all_matches.append(df)
            logger.info("Loaded %d matches for %s", len(df), competition)

    if not all_matches:
        msg = "No Wyscout match data loaded — all downloads failed"
        raise RuntimeError(msg)

    combined = pd.concat(all_matches, ignore_index=True)

    # Serialize any nested JSON columns (teamsData is typically a dict)
    json_cols: list[str] = []
    for c in combined.columns:
        sample = combined[c].dropna()
        if not sample.empty and isinstance(sample.iloc[0], dict | list):
            json_cols.append(str(c))
    combined = _serialize_json_columns(combined, json_cols)
    combined = _normalize_mixed_types(combined)

    sdf = spark.createDataFrame(combined)
    validate_dataframe(
        sdf,
        ["wyId", "competitionId", "seasonId", "dateutc"],
        "wyscout_matches",
        logger,
    )
    write_delta_table(sdf, catalog, schema, "wyscout_matches", mode="overwrite", logger=logger)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for Wyscout ingestion."""
    import argparse
    import sys

    from ingestion.utils import _IDENTIFIER_RE

    # Extend standard args with optional --data-dir
    parser = argparse.ArgumentParser(description="Ingest Wyscout data into the bronze layer")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Target schema (e.g. bronze)")
    parser.add_argument("--data-dir", default=None, help="Optional local directory with Wyscout JSON files")

    args = parser.parse_args()

    # Validate identifiers (reuses regex from utils)
    for field in ("catalog", "schema"):
        value = getattr(args, field)
        if not _IDENTIFIER_RE.match(value):
            print(f"error: Invalid {field} name '{value}': must match {_IDENTIFIER_RE.pattern}", file=sys.stderr)
            raise SystemExit(2)

    logger = configure_logging("wyscout")
    spark = get_spark_session()

    data_dir = pathlib.Path(args.data_dir) if args.data_dir else None

    logger.info("Starting Wyscout ingestion into %s.%s", args.catalog, args.schema)

    ingest_events(spark, args.catalog, args.schema, data_dir, logger)
    ingest_matches(spark, args.catalog, args.schema, data_dir, logger)

    logger.info("Wyscout ingestion complete")


if __name__ == "__main__":
    main()
