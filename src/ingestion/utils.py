"""Shared utilities for data ingestion into the Databricks bronze layer.

Provides CLI argument parsing, structured JSON logging, Delta table write helpers,
an HTTPS-enforcing HTTP client with retry logic, DataFrame content validation,
and HuggingFace Hub token resolution for serverless Databricks tasks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests
import requests_cache

from shared.constants import IDENTIFIER_RE

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

# Spark AnalysisException — used by guards and pipelines for table-not-found
# fallbacks.  Defined unconditionally so pyright sees a stable symbol; the
# try branch overrides with the real pyspark class when available.
SparkAnalysisException: type[Exception] = type("SparkAnalysisException", (Exception,), {})
try:
    from pyspark.errors import AnalysisException as SparkAnalysisException  # type: ignore[no-redef]  # noqa: F401
except Exception:  # noqa: BLE001, S110 — pyspark not installed in CI/test; fallback defined above
    pass

# HTTP status codes eligible for retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Error-message markers that indicate "the table does not exist yet" — the
# only case `tolerate_missing_table` suppresses.  Everything else (permission
# denied, schema corruption, MERGE resolve failure, connection error) must
# propagate so the caller sees the real failure.
_TABLE_NOT_FOUND_MARKERS: tuple[str, ...] = (
    "TABLE_OR_VIEW_NOT_FOUND",  # Spark 3.4+ error class
    "Table or view not found",  # Older Spark message
    "Path does not exist",  # Delta table path not found
    "DELTA_MISSING_DELTA_TABLE",  # Delta-specific
    "DELTA_TABLE_NOT_FOUND",
    "TableNotFoundException",  # Unity Catalog
)


@contextmanager
def tolerate_missing_table(logger: logging.Logger, msg: str) -> Iterator[None]:
    """Context manager that suppresses ONLY 'table does not exist' Spark errors.

    Use in guard / bootstrap code that queries a results table which may not
    exist on first run. Any other exception propagates — including the
    schema-mismatch errors that bare ``except Exception:`` patterns were
    hiding (e.g. the ``DELTA_MERGE_UNRESOLVED_EXPRESSION`` that caused the
    2026-04-12 warm-tier blocker).

    We catch ``Exception`` (not just ``AnalysisException``) because the
    concrete exception class varies between classic PySpark, Spark Connect,
    Delta Lake, and Unity Catalog. Then we check the error message against
    ``_TABLE_NOT_FOUND_MARKERS`` and suppress only when it genuinely matches
    a table-missing case. Non-matching exceptions re-raise with the original
    traceback.

    Example::

        from ingestion.utils import tolerate_missing_table
        existing: set[int] = set()
        with tolerate_missing_table(logger, "No existing X table — starting fresh"):
            existing = {row[0] for row in spark.read.table(table).collect()}
        return existing
    """
    try:
        yield
    except Exception as exc:
        if any(marker in str(exc) for marker in _TABLE_NOT_FOUND_MARKERS):
            logger.info(msg)
            return
        raise


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

    return _SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]


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

    from delta.tables import DeltaTable

    target: DeltaTable | None = None
    with tolerate_missing_table(
        logger or logging.getLogger(__name__),
        f"Table {full_table} does not exist yet — creating via overwrite",
    ):
        target = DeltaTable.forName(df.sparkSession, full_table)

    if target is None:
        # Table doesn't exist yet — fall back to initial write
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
    *,
    headers: dict[str, str] | None = None,
    stream: bool = False,
) -> requests.Response:
    """Fetch a URL with HTTPS enforcement, SSL verification, and retry logic.

    Uses a module-level :class:`requests.Session` so that sequential calls to
    the same host benefit from TCP keep-alive and TLS session resumption.

    Args:
        url: The resource URL. Must use ``https://``.
        timeout: ``(connect, read)`` timeout in seconds.
        max_retries: Maximum number of retries for transient errors.
        headers: Optional extra headers to include in the request.
        stream: If True, don't eagerly download the response body.

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
        response = session.get(url, timeout=timeout, headers=headers, stream=stream)
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
# 6b. Pandas-to-Spark NullType Guard
# ---------------------------------------------------------------------------


def finalize_bronze_df(
    df: pd.DataFrame,
    expected_cols: Iterable[str],
    dtype_overrides: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Protect parser output from pandas→Arrow→Spark NullType column drops.

    The pandas → Arrow → Spark conversion (``spark.createDataFrame(pandas_df)``)
    infers pandas ``object`` columns with 100% None values as Arrow
    ``NullType``. Delta Lake rejects NullType columns — they get silently
    dropped during write. This produces thin bronze tables that don't reflect
    the parser's emitted schema, especially for providers using per-match
    ``replaceWhere`` writes where each batch exercises only a subset of event
    types (e.g. DFL IDSSE events: ``Nutmeg`` rarely occurs so every
    ``nutmeg_*`` column is all-None for most matches and gets dropped).

    This helper guards against the drop by:

    1. Adding any column listed in ``expected_cols`` but missing from ``df``
       as an all-NA column with an explicit nullable pandas dtype.
    2. For every existing column with ``object`` dtype and 100% null values,
       casting it to an explicit nullable dtype so Arrow infers a concrete
       Spark type rather than ``NullType``.

    Call this in every bronze parser immediately before
    ``spark.createDataFrame(df)``.

    Args:
        df: Pandas DataFrame emitted by the parser. Modified in place.
        expected_cols: The complete set of columns the parser emits across
            all possible inputs — not just the current batch. For per-match
            ingestion, include every column from every possible event type
            so matches with sparse event coverage still produce the full
            schema.
        dtype_overrides: Optional per-column dtype names for the nullable
            cast. Valid pandas nullable dtypes: ``"string"``, ``"Int64"``,
            ``"Float64"``, ``"boolean"``. Columns not in this map default
            to ``"string"``.

    Returns:
        The finalized DataFrame (modified in place and also returned).
    """
    overrides = dtype_overrides or {}
    n_rows = len(df)

    for col in expected_cols:
        if col not in df.columns:
            target = overrides.get(col, "string")
            df[col] = pd.array([None] * n_rows, dtype=target)

    for col in list(df.columns):
        if df[col].dtype == object and df[col].isna().all():
            target = overrides.get(col, "string")
            df[col] = df[col].astype(target)  # type: ignore[call-overload]

    return df


# ---------------------------------------------------------------------------
# Bronze schema snapshot loading (G1 — PR #173 drop-safety sweep)
#
# StatsBomb + Wyscout writers load their expected-col + dtype-override
# constants from JSON schema snapshots at import time, rather than
# hardcoding them like IDSSE / Metrica / SkillCorner. These helpers are
# the shared plumbing — provider modules supply only the fixture filename
# and the snapshot table names they care about.
# ---------------------------------------------------------------------------


# Audit columns added post-parser by ``write_delta_table.add_audit_columns``.
# Excluded from expected-col + dtype-override constants so the writer-side
# ``finalize_bronze_df`` doesn't try to pre-create them before the writer
# adds them, and so live-schema tests compare apples-to-apples.
BRONZE_AUDIT_ONLY_COLS: frozenset[str] = frozenset({"_ingested_at"})


# Spark → pandas nullable dtype mapping. Columns whose snapshot type is
# not in this map default to ``"string"`` in ``finalize_bronze_df``.
_PANDAS_NULLABLE_DTYPE: dict[str, str] = {
    "bigint": "Int64",
    "int": "Int64",
    "double": "Float64",
    "float": "Float64",
    "boolean": "boolean",
}


def load_bronze_snapshot(fixture_name: str) -> dict[str, list[dict[str, str]]] | None:
    """Load a bronze schema snapshot from ``src/tests/fixtures/<fixture_name>``.

    Snapshot layout::

        {
            "schema_version": "<provider>_bronze_<YYYY_MM>",
            "snapshot_source": "DESCRIBE TABLE ...",
            "tables": {"<table_name>": [{"name": "...", "type": "..."}, ...]}
        }

    Returns the ``tables`` mapping, or ``None`` when the fixture is not
    present (wheel runtime — the fixture ships with the source tree, not
    the wheel). Callers degrade gracefully to empty expected-col tuples
    and empty dtype-override dicts.
    """
    import json as _json
    from pathlib import Path as _Path

    fixture = _Path(__file__).resolve().parent.parent / "tests" / "fixtures" / fixture_name
    try:
        payload = _json.loads(fixture.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    return payload.get("tables", {})


def expected_cols_from_snapshot(
    tables: dict[str, list[dict[str, str]]] | None,
    table_name: str,
) -> tuple[str, ...]:
    """Return a tuple of column names for ``table_name`` with audit cols dropped."""
    if tables is None:
        return ()
    return tuple(entry["name"] for entry in tables.get(table_name, []) if entry["name"] not in BRONZE_AUDIT_ONLY_COLS)


def dtype_overrides_from_snapshot(
    tables: dict[str, list[dict[str, str]]] | None,
    table_name: str,
) -> dict[str, str]:
    """Return a pandas-nullable dtype override map for ``table_name``.

    Only non-string columns appear in the returned map; string columns
    default to ``"string"`` via ``finalize_bronze_df``'s built-in default.
    """
    if tables is None:
        return {}
    return {
        entry["name"]: _PANDAS_NULLABLE_DTYPE[entry["type"]]
        for entry in tables.get(table_name, [])
        if entry["type"] in _PANDAS_NULLABLE_DTYPE and entry["name"] not in BRONZE_AUDIT_ONLY_COLS
    }


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
    except Exception:  # noqa: BLE001 — intentional multi-source fallback; next source is attempted below
        _hf_logger.debug("Databricks secrets unavailable — trying cached CLI login", exc_info=True)

    # 3. Cached CLI login
    try:
        from huggingface_hub.utils import get_token  # type: ignore[import-not-found]

        token = get_token() or ""
        if token:
            _hf_logger.info("HF token resolved from cached CLI login")
            return token
    except Exception:  # noqa: BLE001 — intentional multi-source fallback; final source is logged below
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
    delete_patterns: list[str] | None = None,
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
        delete_patterns: Optional glob patterns passed to
            ``upload_folder(delete_patterns=...)`` to remove stale files
            before uploading.  E.g. ``["data/*.parquet", "data/_*"]``
            ensures each upload atomically replaces the data directory.

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

        upload_kwargs: dict[str, object] = {
            "folder_path": str(staging_dir),
            "path_in_repo": path_in_repo,
            "repo_id": repo_id,
            "repo_type": repo_type,
            "token": hf_token,
        }
        if delete_patterns is not None:
            upload_kwargs["delete_patterns"] = delete_patterns

        start = time.time()
        api.upload_folder(**upload_kwargs)  # type: ignore[arg-type]
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


# ---------------------------------------------------------------------------
# 8b. Databricks Task Value Writer
# ---------------------------------------------------------------------------


def write_task_value(key: str, value: list[str], logger: logging.Logger | None = None) -> None:
    """Write a Databricks task value for downstream for_each_task consumption.

    Wraps ``dbutils.jobs.taskValues.set()`` with graceful fallback:
    outside the Databricks runtime (local dev, unit tests), the
    ``pyspark.dbutils`` import fails and the function logs a warning
    and returns cleanly so entry points remain testable.

    This is the canonical helper for all preflight task-value emission.
    Existing per-module copies (idsse, spadl_vaep, tracking_context) can
    be migrated to this in a follow-up.

    Args:
        key: Task value key (e.g. ``"gradientsports_matches"``).
        value: Task value payload — list of strings for for_each_task inputs.
        logger: Optional logger. Falls back to module logger if not provided.
    """
    _log = logger or logging.getLogger(__name__)
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            _log.warning("No active SparkSession -- task value '%s' not written", key)
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key=key, value=value)
        _log.info("Wrote task value '%s' (%d elements)", key, len(value))
    except (ImportError, AttributeError, RuntimeError) as exc:
        _log.warning("Task values not available (likely standalone mode) -- %s", exc)


# ---------------------------------------------------------------------------
# 9. Artifact Hash Verification (SEC2 — SEC-AUDIT-v1.12.0 ML-02 / CWE-345)
# ---------------------------------------------------------------------------
#
# Defense-in-depth: verify SHA-256 of model artifacts loaded from MLflow or
# UC Volume. Fail-open on missing hash (first observation / pre-bootstrap),
# fail-closed on mismatch. Bootstrap script at
# ``scripts/bootstrap_artifact_hashes.py`` records initial hashes.

_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class ArtifactHashMismatchError(RuntimeError):
    """Raised when a loaded model artifact's SHA-256 does not match the expected hash.

    The error message includes both the expected and actual hashes plus the
    artifact label so the user can diagnose without re-running the load.
    """


def verify_artifact_hash(
    data: bytes,
    expected_sha256: str | None,
    artifact_label: str,
    logger: logging.Logger,
) -> None:
    """Verify SHA-256 of an in-memory artifact (defense-in-depth, SEC-AUDIT ML-02).

    Args:
        data: The artifact bytes (already loaded into memory).
        expected_sha256: Hex-encoded expected SHA-256, or ``None`` when no
            hash has been recorded yet (the loader is operating before the
            artifact-hash bootstrap has populated the tag / sidecar).
        artifact_label: Human label for log / error messages
            (e.g. ``"xg_model_logistic"``, ``"vaep_scores"``).
        logger: For warning-on-missing-hash messages.

    Raises:
        ArtifactHashMismatchError: When ``expected_sha256`` is non-None and does
            not match the SHA-256 of ``data``.
        ValueError: When ``expected_sha256`` is non-None but is not a valid
            64-character hex string.
    """
    if expected_sha256 is None:
        logger.warning(
            "Artifact %s loaded without recorded SHA-256 hash — verification skipped. "
            "Run scripts/bootstrap_artifact_hashes.py to record hashes for verified loads.",
            artifact_label,
        )
        return

    if not _SHA256_RE.match(expected_sha256):
        msg = f"Invalid expected_sha256 for {artifact_label}: must be 64 hex chars, got {expected_sha256!r}"
        raise ValueError(msg)

    actual = hashlib.sha256(data).hexdigest()
    if actual.lower() != expected_sha256.lower():
        msg = (
            f"ArtifactHashMismatch for {artifact_label}: "
            f"expected={expected_sha256.lower()}, actual={actual.lower()}. "
            f"Artifact bytes do not match the recorded hash — possible tampering or corruption."
        )
        raise ArtifactHashMismatchError(msg)


def _load_mlflow_artifact_hash(
    client: Any,
    model_name: str,
    alias: str = "Champion",
) -> str | None:
    """Read the ``artifact_sha256`` MLflow tag from a model's ``@<alias>`` run.

    Returns the hex string or ``None`` when the tag is absent (loader then
    operates in fail-open mode via :func:`verify_artifact_hash`).

    Defensive: any exception is swallowed and ``None`` returned, so a
    transient MLflow API failure does not break the loader. The swallowed
    exception is logged at WARNING level via this module's logger so that
    operators can distinguish "hash not recorded yet" (common) from
    "MLflow unreachable / authentication failure" (worth investigating).
    """
    try:
        alias_info = client.get_model_version_by_alias(model_name, alias)
        run_id = alias_info.run_id
        run = client.get_run(run_id)
        return run.data.tags.get("artifact_sha256")
    except Exception:  # noqa: BLE001 — MLflow raises many exception types; typed None return is the documented contract
        logging.getLogger(__name__).warning(
            "MLflow artifact-hash lookup failed for %s@%s — treating as 'no hash'. "
            "If this persists across runs it may indicate an MLflow outage or an "
            "authentication problem rather than a missing tag.",
            model_name,
            alias,
            exc_info=True,
        )
        return None


def _load_volume_sidecar_hash(volume_path: str) -> str | None:
    """Read ``<volume_path>.sha256`` if present.

    Returns the stripped hex string or ``None`` when the sidecar is absent.
    Defensive: any read failure returns ``None``.
    """
    try:
        from pathlib import Path

        sidecar = Path(volume_path + ".sha256")
        if not sidecar.exists():
            return None
        return sidecar.read_text(encoding="utf-8").strip()
    except Exception:  # noqa: BLE001 — UC Volume read can raise many IO classes; typed None return is the contract
        return None
