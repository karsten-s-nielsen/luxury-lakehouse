"""Shared utilities for data ingestion into the Databricks bronze layer.

Provides CLI argument parsing, structured JSON logging, Delta table write helpers,
an HTTPS-enforcing HTTP client with retry logic, DataFrame content validation,
and HuggingFace Hub token resolution for serverless Databricks tasks.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests
import requests_cache

from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Spark AnalysisException — used by guards and pipelines for table-not-found
# fallbacks.  Imported at module level so callers can use a single name
# regardless of whether pyspark is installed.
try:
    from pyspark.errors import AnalysisException as SparkAnalysisException
except Exception:
    SparkAnalysisException: type[Exception] = type("SparkAnalysisException", (Exception,), {})  # type: ignore[no-redef]

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
        if not IDENTIFIER_RE.match(value):
            parser.error(f"Invalid {field} name '{value}': must match {IDENTIFIER_RE.pattern}")

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
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, default=str, ensure_ascii=False) if isinstance(v, dict | list) else v
                )
    else:
        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, default=str, ensure_ascii=False) if isinstance(v, dict | list) else v
                )
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
    if not IDENTIFIER_RE.match(table_name):
        msg = f"Invalid table_name '{table_name}': must match {IDENTIFIER_RE.pattern}"
        raise ValueError(msg)

    full_table = f"{catalog}.{schema}.{table_name}"
    df = add_audit_columns(df)
    if row_count is None:
        row_count = int(df.count())

    writer = df.write.format("delta")

    if replace_where is not None:
        writer = writer.option("mergeSchema", "true").option("replaceWhere", replace_where).mode("overwrite")
    elif mode == "overwrite":
        writer = writer.option("overwriteSchema", "true").mode("overwrite")
    else:
        writer = writer.option("mergeSchema", "true").mode(mode)

    writer.saveAsTable(full_table)

    if logger:
        logger.info("Wrote %d rows to %s", row_count, full_table)

    return row_count


def merge_delta_table(
    df: DataFrame,
    catalog: str,
    schema: str,
    table_name: str,
    merge_key: str,
    logger: logging.Logger | None = None,
    row_count: int | None = None,
) -> int:
    """Upsert rows into a Delta table using MERGE on a unique key.

    Matching rows (by *merge_key*) are updated with all source columns;
    non-matching rows are inserted.  The ``_ingested_at`` audit column is
    added automatically, same as :func:`write_delta_table`.

    Falls back to :func:`write_delta_table` with ``mode="overwrite"`` if
    the target table does not yet exist.

    Args:
        df: Spark DataFrame to upsert.
        catalog: Unity Catalog name.
        schema: Target schema (e.g. ``bronze``).
        table_name: Destination table name.
        merge_key: Column name used as the join key for MERGE.
        logger: Optional logger for row-count reporting.
        row_count: Pre-computed row count to avoid redundant ``df.count()``.

    Returns:
        Number of rows in the source DataFrame (upserted).

    Raises:
        ValueError: If *table_name* fails identifier validation.
    """
    if not IDENTIFIER_RE.match(table_name):
        msg = f"Invalid table_name '{table_name}': must match {IDENTIFIER_RE.pattern}"
        raise ValueError(msg)

    full_table = f"{catalog}.{schema}.{table_name}"
    df = add_audit_columns(df)
    if row_count is None:
        row_count = int(df.count())

    try:
        from delta.tables import DeltaTable

        target = DeltaTable.forName(df.sparkSession, full_table)
    except Exception:
        # Table doesn't exist yet — fall back to initial write
        if logger:
            logger.info("Table %s does not exist yet — creating via overwrite", full_table)
        df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(full_table)
        if logger:
            logger.info("Wrote %d rows to %s (initial create)", row_count, full_table)
        return row_count

    (
        target.alias("target")
        .merge(df.alias("source"), f"target.{merge_key} = source.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    if logger:
        logger.info("Merged %d rows into %s (key: %s)", row_count, full_table, merge_key)

    return row_count


# ---------------------------------------------------------------------------
# 5. HTTP Client
# ---------------------------------------------------------------------------

_session: requests.Session | None = None

# Default SQLite cache path for requests-cache (user home directory)
_CACHE_DB_NAME = "luxury_lakehouse_http_cache"


def _get_session() -> requests.Session:
    """Return the module-level shared :class:`requests_cache.CachedSession`, creating it on first call.

    Uses ``requests-cache`` with a persistent SQLite backend to avoid redundant
    HTTP round-trips during development and retry.  Static open-data sources
    (StatsBomb) are cached indefinitely; other sources use a 24-hour TTL.

    The cache is transparent — responses are identical to a plain
    :class:`requests.Session`.  Set the ``LUXURY_LAKEHOUSE_HTTP_CACHE=0``
    environment variable to disable caching entirely.
    """
    global _session
    if _session is None:
        import os

        if os.environ.get("LUXURY_LAKEHOUSE_HTTP_CACHE", "1") == "0":
            _session = requests.Session()
        else:
            _session = requests_cache.CachedSession(
                _CACHE_DB_NAME,
                backend="sqlite",
                expire_after=86400,  # 24h default TTL
                urls_expire_after={
                    "raw.githubusercontent.com": -1,  # never expire (static open data, incl. StatsBomb)
                },
                stale_if_error=True,
            )
        _session.verify = True
    return _session


def fetch_url(
    url: str,
    timeout: tuple[int, int] = (10, 30),
    max_retries: int = 3,
) -> requests.Response:
    """Fetch a URL with HTTPS enforcement, SSL verification, and retry logic.

    Uses a module-level :class:`requests.Session` so that sequential calls to
    the same host benefit from TCP keep-alive and TLS session resumption.

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

    session = _get_session()
    last_response: requests.Response | None = None

    for attempt in range(max_retries):
        response = session.get(url, timeout=timeout)
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


# ---------------------------------------------------------------------------
# 7. HuggingFace Hub Token Resolution
# ---------------------------------------------------------------------------

_hf_logger = logging.getLogger("ingestion.hf_token")


def resolve_hf_token() -> str:
    """Resolve a HuggingFace Hub token from available sources.

    Resolution order:
        1. ``HF_TOKEN`` environment variable
        2. Databricks secret scope ``hf``, key ``token``
           (via :class:`~databricks.sdk.WorkspaceClient` — works on serverless)
        3. Cached CLI login (``huggingface-cli login``)

    Returns:
        The token string, or empty string if no token is found.
    """
    # 1. Environment variable
    token = os.environ.get("HF_TOKEN", "")
    if token:
        _hf_logger.info("HF token resolved from HF_TOKEN environment variable")
        return token

    # 2. Databricks secrets via WorkspaceClient (serverless-compatible)
    #    The SDK returns base64-encoded secret values, unlike dbutils.secrets.get().
    try:
        import base64

        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]

        client = WorkspaceClient()
        resp = client.secrets.get_secret(scope="hf", key="token")
        encoded = resp.value or ""
        if encoded:
            token = base64.b64decode(encoded).decode()
            _hf_logger.info("HF token resolved from Databricks secret scope 'hf'")
            return token
    except Exception:
        _hf_logger.debug("Databricks secrets unavailable — trying cached CLI login", exc_info=True)

    # 3. Cached CLI login
    try:
        from huggingface_hub.utils import get_token  # type: ignore[import-not-found]

        token = get_token() or ""
        if token:
            _hf_logger.info("HF token resolved from cached CLI login")
            return token
    except Exception:
        _hf_logger.debug("huggingface_hub get_token unavailable", exc_info=True)

    _hf_logger.warning("No HF token found from any source")
    return ""


# ---------------------------------------------------------------------------
# 8. HuggingFace Hub Volume Upload
# ---------------------------------------------------------------------------


def upload_volume_to_hf_hub(
    volume_path: str,
    repo_id: str,
    *,
    repo_type: str = "dataset",
    path_in_repo: str = "data",
    logger: logging.Logger | None = None,
) -> str:
    """Upload Spark-written Parquet from a UC Volume to HuggingFace Hub.

    Copies Parquet part files and Spark metadata from the UC Volume FUSE
    mount to a local temp directory, then uploads via ``upload_folder``.
    This pattern works at any scale — Spark writes directories of part
    files, not single files.

    Args:
        volume_path: UC Volume directory containing Spark-written Parquet.
        repo_id: HF Hub repository ID (e.g. ``luxury-lakehouse/my-dataset``).
        repo_type: Repository type (``dataset``, ``model``, etc.).
        path_in_repo: Target path within the repo (default ``data``).
        logger: Optional logger; falls back to module-level ``_hf_logger``.

    Returns:
        URL of the published HF Hub repository.

    Raises:
        RuntimeError: If the volume path does not exist or contains no
            Parquet files.
    """
    import shutil
    import tempfile
    from pathlib import Path

    from huggingface_hub import HfApi  # type: ignore[import-not-found]

    log = logger or _hf_logger

    hf_token = resolve_hf_token()
    if not hf_token:
        log.warning(  # nosemgrep: python-logger-credential-disclosure
            "No HF token found — skipping upload. Data available at UC Volume: %s",
            volume_path,
        )
        return f"file://{volume_path}"

    api = HfApi(token=hf_token)

    api.create_repo(repo_id, exist_ok=True, repo_type=repo_type)
    log.info("Ensured %s repo exists: %s", repo_type, repo_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        staging_dir = Path(tmpdir) / path_in_repo
        staging_dir.mkdir(parents=True, exist_ok=True)

        volume_dir = Path(volume_path)
        if not volume_dir.exists():
            msg = f"UC Volume path does not exist: {volume_path}"
            raise RuntimeError(msg)

        # Copy Parquet part files
        part_count = 0
        for part_file in volume_dir.glob("*.parquet"):
            shutil.copy2(str(part_file), str(staging_dir / part_file.name))
            part_count += 1

        # Copy Spark metadata files (_SUCCESS, _committed_*, etc.)
        for meta_file in volume_dir.glob("_*"):
            if meta_file.is_file():
                shutil.copy2(str(meta_file), str(staging_dir / meta_file.name))

        if part_count == 0:
            msg = f"No Parquet files found at {volume_path}"
            raise RuntimeError(msg)

        log.info("Staged %d Parquet files for HF Hub upload", part_count)

        start = time.time()
        api.upload_folder(
            folder_path=str(staging_dir),
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type=repo_type,
            token=hf_token,
        )
        elapsed = time.time() - start

    url_prefix = {"dataset": "datasets/", "model": "", "space": "spaces/"}.get(repo_type, "")
    repo_url = f"https://huggingface.co/{url_prefix}{repo_id}"
    log.info("Uploaded %d Parquet files to %s in %.2fs", part_count, repo_url, elapsed)
    return repo_url


# ---------------------------------------------------------------------------
# UC Volume directory helpers
# ---------------------------------------------------------------------------

_vol_logger = logging.getLogger("utils.volume")


def ensure_volume_directory(volume_path: str) -> None:
    """Ensure a UC Volume directory exists, creating it via the Files API if needed.

    On Databricks serverless, Python's ``os.makedirs`` / ``Path.mkdir`` on
    FUSE-mounted ``/Volumes/...`` paths may fail for directories that do not
    yet exist.  This function uses the Databricks Files API (REST) which
    reliably creates Volume directories regardless of FUSE state.

    The Files API endpoint is ``PUT /api/2.0/fs/directories/{path}`` and
    returns 204 on success (idempotent — safe to call if directory exists).

    Args:
        volume_path: UC Volume path, e.g. ``/Volumes/catalog/schema/volume/subdir``.

    Raises:
        ValueError: If ``volume_path`` does not start with ``/Volumes/``.
        requests.HTTPError: If the Files API call fails.
    """
    if not volume_path.startswith("/Volumes/"):
        msg = f"volume_path must start with /Volumes/, got: {volume_path}"
        raise ValueError(msg)

    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    if not host or not token:
        # Fallback to FUSE mkdir when running outside Databricks
        # (e.g. local testing with a mounted Volume).
        _vol_logger.debug("No DATABRICKS_HOST/TOKEN — falling back to os.makedirs")
        os.makedirs(volume_path, exist_ok=True)
        return

    # Strip leading slash for the API path (API expects Volumes/... not /Volumes/...)
    api_path = volume_path.lstrip("/")
    url = f"https://{host}/api/2.0/fs/directories/{api_path}/"
    resp = requests.put(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=(10, 30),
        verify=True,
    )
    if resp.status_code == 204:
        _vol_logger.info("Volume directory ready: %s", volume_path)
    elif resp.status_code == 409:
        # 409 = already exists (some API versions)
        _vol_logger.debug("Volume directory already exists: %s", volume_path)
    else:
        resp.raise_for_status()
