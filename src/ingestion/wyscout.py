"""Wyscout public event data ingestion into the Databricks bronze layer.

Supports both local files (``--data-dir`` for dev speed) and HTTPS download
from Figshare for Databricks runtime.

7 competitions (2017/18 season):
  England, Italy, Spain, France, Germany, European Championship, World Cup

The Figshare dataset stores events and matches as ZIP archives containing
one JSON file per competition (e.g. ``events_England.json``).  Players are
a flat JSON array (not ZIP-wrapped).

JSON columns (``positions``, ``tags``, ``role``, ``passportArea``, ``birthArea``)
are serialized to JSON strings before Delta write so dbt staging models can
parse them with SQL JSON functions.

Bronze tables produced:
  - wyscout_events
  - wyscout_matches
  - wyscout_players
"""

from __future__ import annotations

import gc
import io
import json
import logging
import pathlib
import re
import zipfile
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult
from ingestion.utils import (
    configure_logging,
    fetch_url,
    get_spark_session,
    parse_ingestion_args,
    serialize_json_columns,
    validate_dataframe,
    write_delta_table,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _WyscoutGuard:
    workflow_id = "wf-wyscout"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip if all Wyscout competitions are already ingested."""
        expected = len(_COMPETITIONS)
        try:
            e_count = spark.table(f"{catalog}.{schema}.wyscout_events").select("competition_name").distinct().count()
            m_count = spark.table(f"{catalog}.{schema}.wyscout_matches").select("competition_name").distinct().count()
            p_exists = spark.table(f"{catalog}.{schema}.wyscout_players").limit(1).count() > 0
            if e_count >= expected and m_count >= expected and p_exists:
                return FilterResult(workflow_id=self.workflow_id, count=0)
        except Exception:  # noqa: S110
            pass
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _WyscoutGuard()

# Figshare HTTPS URLs for Wyscout open data (ZIP archives)
# Source: https://figshare.com/collections/Soccer_match_event_dataset/4415000
# IMPORTANT: Use ndownloader.figshare.com subdomain (figshare.com/ndownloader returns 202)
_EVENTS_ZIP_URL = "https://ndownloader.figshare.com/files/14464685"
_MATCHES_ZIP_URL = "https://ndownloader.figshare.com/files/14464622"
_PLAYERS_URL = "https://ndownloader.figshare.com/files/15073721"

_COMPETITIONS = [
    "England",
    "Italy",
    "Spain",
    "France",
    "Germany",
    "European_Championship",
    "World_Cup",
]


_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")


def _decode_unicode_escapes(df: pd.DataFrame) -> pd.DataFrame:
    """Decode literal ``\\uXXXX`` escape sequences in string columns.

    The Wyscout Figshare JSON files contain double-escaped Unicode in player
    name fields (e.g., ``G\\u00f3mez`` instead of ``Gómez``). After
    ``json.loads``, these become literal backslash-u sequences in Python
    strings.  This function converts them to proper Unicode characters.
    """
    for col in df.select_dtypes(include=["object"]).columns:
        sample = df[col].dropna()
        if sample.empty or not isinstance(sample.iloc[0], str):
            continue
        if sample.str.contains(_UNICODE_ESCAPE_RE.pattern, na=False, regex=True).any():
            df[col] = df[col].apply(
                lambda v: _UNICODE_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), v) if isinstance(v, str) else v
            )
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
            pass  # already handled by serialize_json_columns
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
# Per-competition helpers (load one, write one, release)
# ---------------------------------------------------------------------------


def _load_local_competition(
    data_dir: pathlib.Path | None,
    file_prefix: str,
    competition: str,
    logger: logging.Logger,
) -> pd.DataFrame | None:
    """Load a single competition JSON from local disk, or return None."""
    if data_dir is None:
        return None
    local_path = data_dir / f"{file_prefix}_{competition}.json"
    return _load_json_local(local_path, logger)


def _write_events_competition(
    spark: SparkSession,
    catalog: str,
    schema: str,
    df: pd.DataFrame,
    competition: str,
    logger: logging.Logger,
) -> int:
    """Serialize, validate, and write events for one competition. Returns row count."""
    if df.empty:
        return 0
    df["competition_name"] = competition
    df = serialize_json_columns(df, ["positions", "tags"])
    df = _normalize_mixed_types(df)
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["eventId", "matchId", "eventName", "playerId", "teamId", "matchPeriod", "eventSec"],
        "wyscout_events",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "wyscout_events",
        replace_where=f"competition_name = '{competition}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count


def _write_matches_competition(
    spark: SparkSession,
    catalog: str,
    schema: str,
    df: pd.DataFrame,
    competition: str,
    logger: logging.Logger,
) -> int:
    """Serialize, validate, and write matches for one competition. Returns row count."""
    if df.empty:
        return 0
    df["competition_name"] = competition
    json_cols: list[str] = []
    for c in df.columns:
        sample = df[c].dropna()
        if not sample.empty and isinstance(sample.iloc[0], dict | list):
            json_cols.append(str(c))
    df = serialize_json_columns(df, json_cols)
    df = _normalize_mixed_types(df)
    # Cast datetime columns to string to prevent Delta schema merge conflicts
    # across competitions (e.g. league 'date' may infer as DateType while
    # tournament 'date' infers as StringType).
    for c in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
        df[c] = df[c].astype(str)
    sdf = spark.createDataFrame(df)
    row_count = validate_dataframe(
        sdf,
        ["wyId", "competitionId", "seasonId", "dateutc"],
        "wyscout_matches",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "wyscout_matches",
        replace_where=f"competition_name = '{competition}'",
        logger=logger,
        row_count=row_count,
    )
    return row_count


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
    """Load and write Wyscout events one competition at a time.

    Each competition is loaded, written, and released before the next to
    keep peak memory at ~1/7th of loading all competitions at once.
    """
    # Incremental skip: detect competitions already in Delta
    existing_comps: set[str] = set()
    try:
        existing_rows = (
            spark.table(f"{catalog}.{schema}.wyscout_events").select("competition_name").distinct().collect()
        )
        existing_comps = {str(row["competition_name"]) for row in existing_rows}
    except Exception:
        logger.info("No existing wyscout_events table — processing all competitions")

    new_comps = [c for c in _COMPETITIONS if c not in existing_comps]
    logger.info(
        "wyscout_events: %d total, %d already processed, %d to process",
        len(_COMPETITIONS),
        len(existing_comps & set(_COMPETITIONS)),
        len(new_comps),
    )
    if not new_comps:
        logger.info("All competitions already ingested — skipping wyscout_events")
        return

    total_rows = 0
    loaded_comps: set[str] = set()

    # Process local files one at a time (load -> write -> release)
    for competition in new_comps:
        df = _load_local_competition(data_dir, "events", competition, logger)
        if df is not None:
            loaded_comps.add(competition)
            total_rows += _write_events_competition(spark, catalog, schema, df, competition, logger)
            del df
            gc.collect()

    # Download ZIP for any missing competitions (among new_comps only)
    missing = [c for c in new_comps if c not in loaded_comps]
    if missing:
        try:
            zip_data = _download_and_extract_zip(_EVENTS_ZIP_URL, logger)
            for comp in missing:
                if comp in zip_data:
                    df = zip_data.pop(comp)
                    loaded_comps.add(comp)
                    total_rows += _write_events_competition(spark, catalog, schema, df, comp, logger)
                    del df
                    gc.collect()
            del zip_data
            gc.collect()
        except Exception:
            logger.exception("Failed to download events ZIP from Figshare")

    if not loaded_comps:
        msg = "No Wyscout event data loaded — all downloads failed"
        raise RuntimeError(msg)

    logger.info("Wrote %d total events across %d competitions", total_rows, len(loaded_comps))


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
    """Load and write Wyscout match metadata one competition at a time."""
    # Incremental skip: detect competitions already in Delta
    existing_comps: set[str] = set()
    try:
        existing_rows = (
            spark.table(f"{catalog}.{schema}.wyscout_matches").select("competition_name").distinct().collect()
        )
        existing_comps = {str(row["competition_name"]) for row in existing_rows}
    except Exception:
        logger.info("No existing wyscout_matches table — processing all competitions")

    new_comps = [c for c in _COMPETITIONS if c not in existing_comps]
    logger.info(
        "wyscout_matches: %d total, %d already processed, %d to process",
        len(_COMPETITIONS),
        len(existing_comps & set(_COMPETITIONS)),
        len(new_comps),
    )
    if not new_comps:
        logger.info("All competitions already ingested — skipping wyscout_matches")
        return

    total_rows = 0
    loaded_comps: set[str] = set()

    for competition in new_comps:
        df = _load_local_competition(data_dir, "matches", competition, logger)
        if df is not None:
            loaded_comps.add(competition)
            total_rows += _write_matches_competition(spark, catalog, schema, df, competition, logger)
            del df
            gc.collect()

    missing = [c for c in new_comps if c not in loaded_comps]
    if missing:
        try:
            zip_data = _download_and_extract_zip(_MATCHES_ZIP_URL, logger)
            for comp in missing:
                if comp in zip_data:
                    df = zip_data.pop(comp)
                    loaded_comps.add(comp)
                    total_rows += _write_matches_competition(spark, catalog, schema, df, comp, logger)
                    del df
                    gc.collect()
            del zip_data
            gc.collect()
        except Exception:
            logger.exception("Failed to download matches ZIP from Figshare")

    if not loaded_comps:
        msg = "No Wyscout match data loaded — all downloads failed"
        raise RuntimeError(msg)

    logger.info("Wrote %d total matches across %d competitions", total_rows, len(loaded_comps))


# ---------------------------------------------------------------------------
# Player metadata ingestion (flat JSON, not ZIP)
# ---------------------------------------------------------------------------


def _load_players(
    data_dir: pathlib.Path | None,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Load Wyscout player metadata from local file or Figshare download.

    Returns a DataFrame with one row per player. JSON columns (role,
    passportArea, birthArea) are serialized to strings for Spark/Delta.
    """
    if data_dir is not None:
        local_path = data_dir / "players.json"
        if local_path.exists():
            logger.info("Loading local players file: %s", local_path)
            df = pd.read_json(local_path)
            df = _decode_unicode_escapes(df)
            df = serialize_json_columns(df, ["role", "passportArea", "birthArea"])
            return df

    logger.info("Downloading players.json from Figshare")
    resp = fetch_url(_PLAYERS_URL, timeout=(10, 60))
    data = resp.json()
    df = pd.DataFrame(data)
    df = _decode_unicode_escapes(df)
    df = serialize_json_columns(df, ["role", "passportArea", "birthArea"])
    logger.info("Loaded %d players from Figshare", len(df))
    return df


def ingest_players(
    spark: SparkSession,
    catalog: str,
    schema: str,
    data_dir: pathlib.Path | None,
    logger: logging.Logger,
) -> None:
    """Load and write Wyscout player metadata."""
    # Incremental skip: if table already exists, skip entirely
    full_table_name = f"{catalog}.{schema}.wyscout_players"
    try:
        if spark.catalog.tableExists(full_table_name):
            logger.info("wyscout_players already populated — skipping")
            return
    except Exception:
        logger.info("No existing wyscout_players table — will ingest")

    pdf = _load_players(data_dir, logger)
    pdf = _normalize_mixed_types(pdf)

    sdf = spark.createDataFrame(pdf)
    row_count = validate_dataframe(
        sdf,
        ["wyId", "firstName", "lastName", "shortName", "birthDate"],
        "wyscout_players",
        logger,
    )
    write_delta_table(
        sdf,
        catalog,
        schema,
        "wyscout_players",
        mode="overwrite",
        logger=logger,
        row_count=row_count,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


@workflow("wf-wyscout", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    data_dir: pathlib.Path | None = None,
    ctx: object = None,
) -> None:
    """Ingest all Wyscout open data (events, matches, players)."""
    ingest_events(spark, catalog, schema, data_dir, logger)
    ingest_matches(spark, catalog, schema, data_dir, logger)
    ingest_players(spark, catalog, schema, data_dir, logger)


def main() -> None:
    """CLI entry point for Wyscout ingestion."""
    args = parse_ingestion_args(
        "Ingest Wyscout data into the bronze layer",
        extra_args=[("--data-dir", {"default": None, "help": "Optional local directory with Wyscout JSON files"})],
    )

    logger = configure_logging("wyscout")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    data_dir = pathlib.Path(args.data_dir) if args.data_dir else None

    logger.info("Starting Wyscout ingestion into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, data_dir=data_dir)
    logger.info("Wyscout ingestion complete")


if __name__ == "__main__":
    main()
