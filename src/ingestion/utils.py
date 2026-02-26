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
from typing import TYPE_CHECKING

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


def parse_ingestion_args(description: str) -> argparse.Namespace:
    """Parse and validate common ingestion CLI arguments.

    Args:
        description: Help text for the argument parser.

    Returns:
        Parsed namespace with validated ``catalog`` and ``schema`` fields.

    Raises:
        SystemExit: If catalog or schema fail identifier validation.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Target schema (e.g. bronze)")
    args = parser.parse_args()

    for field in ("catalog", "schema"):
        value = getattr(args, field)
        if not _IDENTIFIER_RE.match(value):
            parser.error(f"Invalid {field} name '{value}': must match {_IDENTIFIER_RE.pattern}")

    return args


# ---------------------------------------------------------------------------
# 2. Structured JSON Logging
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
# 3. Spark Session & Delta Write Helpers
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

    Returns:
        Number of rows written.
    """
    full_table = f"{catalog}.{schema}.{table_name}"
    df = add_audit_columns(df)
    row_count = df.count()

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
# 4. HTTP Client
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
# 5. Content Validation
# ---------------------------------------------------------------------------


def validate_dataframe(
    df: DataFrame,
    required_columns: list[str],
    source_name: str,
    logger: logging.Logger | None = None,
) -> None:
    """Verify that *df* contains all required columns and is non-empty.

    Args:
        df: Spark DataFrame to validate.
        required_columns: Column names that must be present.
        source_name: Human-readable data source name for error messages.
        logger: Optional logger for success reporting.

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
