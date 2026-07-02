"""SkillCorner events ingestion -- dynamic_events.csv to bronze.

Reads the dynamic_events CSV artifact from the pining-for-the-data API,
adds match_id and _ingested_at audit column, and writes to Delta.

Bronze table: bronze.skillcorner_events
Coordinate system: POSSESSION_PERSPECTIVE (center-origin meters, preserved as-is).
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from typing import IO, TYPE_CHECKING

import pandas as pd

from ingestion.utils import tolerate_missing_table, validate_dataframe, write_delta_table

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


def parse_events_parquet(content: bytes, *, match_id: str) -> pd.DataFrame:
    """Parse an RM ``events.parquet`` artifact into a pandas DataFrame.

    RM (private) ships events as parquet vs A-League's CSV — the **same 294-column
    SkillCorner event model** (measured byte-identical column set), different serialization.

    DTYPE PARITY (pure): parquet carries native typed columns (int32/float32/bool/nullable)
    that would otherwise differ from the CSV-established ``bronze.skillcorner_events`` schema.
    To get identical pandas dtypes — and therefore an identical Spark schema, since
    ``spark.createDataFrame`` infers from pandas dtypes — the parquet frame is normalized
    through the **same** ``pd.read_csv`` inference the bronze schema was built from, by
    delegating to ``parse_events_csv`` over an in-memory CSV round-trip. This makes the
    coercion pure-testable in PR CI (``parse_events_parquet(x).dtypes == parse_events_csv(x)``)
    rather than only observable behind the Spark write. Events are small (~6k rows/match), so
    the round-trip cost is negligible. ``_conform_to_bronze_schema`` (write layer) remains the
    authoritative cast to the exact live Delta schema (defense-in-depth, verified in the e2e).

    Args:
        content: Raw bytes of the parquet artifact.
        match_id: Raw native SkillCorner match ID (e.g. "1021404").
    """
    df = pd.read_parquet(io.BytesIO(content))
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return parse_events_csv(buf, match_id=match_id)


def _conform_to_bronze_schema(
    sdf: object,
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> object:
    """Cast a DataFrame's columns to the existing bronze table's types (the fixed contract).

    RM parquet carries native types (int32/float32/bool) that can differ from the
    CSV-established ``bronze.skillcorner_events`` schema (e.g. int32 vs bigint); casting
    to the live Delta types makes RM + A-League rows share ONE schema. No-op when the
    table does not exist yet (first write establishes the schema from the DataFrame) or
    when a column's type already matches (the A-League path is unchanged).
    """
    from pyspark.sql import functions as spark_fn

    conformed = sdf
    with tolerate_missing_table(logger, f"{table} not found -- first write establishes schema"):
        target = {f.name: f.dataType for f in spark.table(f"{catalog}.{schema}.{table}").schema.fields}
        current = {f.name: f.dataType for f in sdf.schema.fields}  # type: ignore[attr-defined]
        select_exprs = [
            spark_fn.col(c).cast(target[c]).alias(c) if c in target and target[c] != current[c] else spark_fn.col(c)
            for c in sdf.columns  # type: ignore[attr-defined]
        ]
        conformed = sdf.select(*select_exprs)  # type: ignore[attr-defined]
    return conformed


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
    # Conform to the fixed bronze schema so RM parquet + A-League CSV rows share ONE
    # schema (parquet's native types can differ from the CSV-established Delta schema).
    sdf = _conform_to_bronze_schema(sdf, spark, catalog, schema, "skillcorner_events", logger)
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
