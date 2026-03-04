"""Shared utilities for data ingestion into the Databricks bronze layer.

Provides CLI argument parsing, structured JSON logging, Delta table write helpers,
an HTTPS-enforcing HTTP client with retry logic, and DataFrame content validation.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Regex for safe SQL identifiers — prevents injection via catalog/schema names
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# HTTP status codes eligible for retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


# ---------------------------------------------------------------------------
# 1. CLI Argument Parsing
# ---------------------------------------------------------------------------


def parse_ingestion_args(
    description: str,
    extra_args: list[tuple[str, dict[str, Any]]] | None = None,
) -> argparse.Namespace:
    """Parse and validate common ingestion CLI arguments.

    Args:
        description: Help text for the argument parser.
        extra_args: Optional list of ``("--flag-name", {argparse_kwargs})`` tuples
            to add source-specific CLI arguments.

    Returns:
        Parsed namespace with validated ``catalog`` and ``schema`` fields.

    Raises:
        SystemExit: If catalog or schema fail identifier validation.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Target schema (e.g. bronze)")

    if extra_args:
        for flag, kwargs in extra_args:
            parser.add_argument(flag, **kwargs)

    args = parser.parse_args()

    for field in ("catalog", "schema"):
        value = getattr(args, field)
        if not _IDENTIFIER_RE.match(value):
            parser.error(f"Invalid {field} name '{value}': must match {_IDENTIFIER_RE.pattern}")

    return args


# ---------------------------------------------------------------------------
# 2. JSON Column Serialization
# ---------------------------------------------------------------------------


def serialize_json_columns(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """Serialize dict/list columns in a pandas DataFrame to JSON strings.

    Spark cannot infer a consistent schema from heterogeneous dict columns,
    so we serialize them before converting to a Spark DataFrame. The dbt
    staging layer parses these JSON strings back out.

    Args:
        df: DataFrame with potential dict/list columns.
        columns: Explicit list of columns to serialize. If ``None``, auto-detects
            all columns whose first non-null value is a ``dict`` or ``list``.

    Returns:
        The DataFrame with specified (or detected) columns serialized to JSON strings.
    """
    if columns is None:
        # Auto-detect: serialize any column whose first non-null value is dict/list
        for col in df.columns:
            sample = df[col].dropna()
            if sample.empty:
                continue
            if isinstance(sample.iloc[0], dict | list):
                df[col] = df[col].apply(lambda v: json.dumps(v, default=str) if isinstance(v, dict | list) else v)
    else:
        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: json.dumps(v, default=str) if isinstance(v, dict | list) else v)
    return df


# ---------------------------------------------------------------------------
# 3. Structured JSON Logging
# ---------------------------------------------------------------------------


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for Databricks log aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, object] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "source": record.name,
            "message": record.getMessage(),
        }
        # Include exception traceback when present (logger.exception calls)
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        # Propagate any extra fields attached to the log record
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)  # type: ignore[arg-type]
        return json.dumps(log_entry, default=str)


def configure_logging(source_name: str) -> logging.Logger:
    """Create a logger that emits JSON lines to stdout.

    Args:
        source_name: Identifier for the ingestion source (e.g. ``statsbomb``).

    Returns:
        Configured :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(f"ingestion.{source_name}")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


# ---------------------------------------------------------------------------
# 4. Spark Session & Delta Write Helpers
# ---------------------------------------------------------------------------


def get_spark_session() -> SparkSession:
    """Return the active Spark session (Databricks runtime provides it)."""
    from pyspark.sql import SparkSession as _SparkSession

    return _SparkSession.builder.getOrCreate()


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Append ``_ingested_at`` UTC timestamp column to *df*."""
    from pyspark.sql import functions as spark_fn

    return df.withColumn("_ingested_at", spark_fn.current_timestamp())


def write_delta_table(
    df: DataFrame,
    catalog: str,
    schema: str,
    table_name: str,
    mode: str = "overwrite",
    replace_where: str | None = None,
    logger: logging.Logger | None = None,
    row_count: int | None = None,
) -> int:
    """Write a Spark DataFrame to a Delta table with audit columns.

    Args:
        df: DataFrame to write.
        catalog: Unity Catalog name.
        schema: Target schema (e.g. ``bronze``).
        table_name: Destination table name.
        mode: Write mode (``overwrite``, ``append``, etc.).
        replace_where: Optional partition predicate for ``replaceWhere``.
        logger: Optional logger for row-count reporting.
        row_count: Pre-computed row count from ``validate_dataframe()``.
            When provided, skips the internal ``df.count()`` call to
            avoid redundant Spark DAG recomputation.

    Returns:
        Number of rows written.

    Raises:
        ValueError: If ``table_name`` fails identifier validation.
    """
    if not _IDENTIFIER_RE.match(table_name):
        msg = f"Invalid table_name '{table_name}': must match {_IDENTIFIER_RE.pattern}"
        raise ValueError(msg)

    full_table = f"{catalog}.{schema}.{table_name}"
    df = add_audit_columns(df)
    if row_count is None:
        row_count = int(df.count())

    writer = df.write.format("delta").option("mergeSchema", "true")

    if replace_where is not None:
        writer = writer.option("replaceWhere", replace_where).mode("overwrite")
    else:
        writer = writer.mode(mode)

    writer.saveAsTable(full_table)

    if logger:
        logger.info("Wrote %d rows to %s", row_count, full_table)

    return row_count


# ---------------------------------------------------------------------------
# 5. HTTP Client
# ---------------------------------------------------------------------------


def fetch_url(
    url: str,
    timeout: tuple[int, int] = (10, 30),
    max_retries: int = 3,
) -> requests.Response:
    """Fetch a URL with HTTPS enforcement, SSL verification, and retry logic.

    Args:
        url: The resource URL. Must use ``https://``.
        timeout: ``(connect, read)`` timeout in seconds.
        max_retries: Maximum number of retries for transient errors.

    Returns:
        Successful :class:`requests.Response`.

    Raises:
        ValueError: If the URL does not use HTTPS.
        requests.HTTPError: If all retries are exhausted or a non-retryable error occurs.
    """
    if not url.startswith("https://"):
        msg = f"Only HTTPS URLs are allowed, got: {url}"
        raise ValueError(msg)

    last_response: requests.Response | None = None

    for attempt in range(max_retries):
        response = requests.get(url, timeout=timeout, verify=True)
        last_response = response

        if response.status_code < 400:
            return response

        if response.status_code in _RETRYABLE_STATUS_CODES and attempt < max_retries - 1:
            wait = 2**attempt
            time.sleep(wait)
            continue

        # Non-retryable error or final attempt — raise immediately
        response.raise_for_status()

    # Should not reach here, but satisfy the type checker
    assert last_response is not None  # noqa: S101
    last_response.raise_for_status()
    return last_response  # pragma: no cover


# ---------------------------------------------------------------------------
# 6. Content Validation
# ---------------------------------------------------------------------------


def validate_dataframe(
    df: DataFrame,
    required_columns: list[str],
    source_name: str,
    logger: logging.Logger | None = None,
) -> int:
    """Verify that *df* contains all required columns and is non-empty.

    Args:
        df: Spark DataFrame to validate.
        required_columns: Column names that must be present.
        source_name: Human-readable data source name for error messages.
        logger: Optional logger for success reporting.

    Returns:
        Number of rows in the DataFrame (avoids redundant ``df.count()``
        in downstream callers).

    Raises:
        ValueError: If required columns are missing or the DataFrame is empty.
    """
    actual_columns = set(df.columns)
    missing = [col for col in required_columns if col not in actual_columns]

    if missing:
        msg = f"[{source_name}] Missing required columns: {missing}. Available: {sorted(actual_columns)}"
        raise ValueError(msg)

    row_count = df.count()
    if row_count == 0:
        msg = f"[{source_name}] DataFrame is empty — no data to write"
        raise ValueError(msg)

    if logger:
        logger.info(
            "[%s] Validation passed: %d columns, %d rows",
            source_name,
            len(actual_columns),
            row_count,
        )

    return row_count
