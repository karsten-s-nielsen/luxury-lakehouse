"""Catalog compaction on the DETACHED catalog file (T3 spec D7, decisions 9 and 17).

DuckDB never reclaims space inside a database file, so the catalog file grows with every rewritten metadata row.
`CHECKPOINT` on the DuckLake attach is forbidden (`catalog/working.py`), and the T3 spike measured that `CHECKPOINT`
on a plain open shrinks nothing. The rewrite here copies every metadata table into a fresh file through a plain DuckDB
open (`COPY FROM DATABASE`; encrypted with the same key on the encrypted-local path), verifies each table's row count,
a content checksum and the schema, and only then replaces the old file. It runs no DuckLake maintenance and touches no
data file. A failed check discards the fresh file: the caller publishes the original, and records a warning.
"""

from __future__ import annotations

import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import duckdb

from lakehouse.catalog.duck import keyed_plain_attach, sql_text
from lakehouse.errors import IntegrityError
from lakehouse.logging import get_logger

__all__ = ["COMPACT_MIN_BYTES", "CompactionResult", "compact_catalog_file"]

# The T3 spike (2026-10-31, Mac Mini): the rewrite made catalogs of 9 to 22 MB 31-40 % smaller in at most 0.17 s.
# Below 8 MiB the bytes saved are too small to matter.
COMPACT_MIN_BYTES: Final = 8 * 1024 * 1024

_SCHEMA_VIEWS: Final[dict[str, str]] = {
    "tables": (
        "SELECT schema_name, table_name, column_count, has_primary_key FROM duckdb_tables() WHERE database_name = ?"
    ),
    "columns": (
        "SELECT schema_name, table_name, column_name, column_index, data_type, column_default, is_nullable "
        "FROM duckdb_columns() WHERE database_name = ?"
    ),
    "constraints": (
        "SELECT schema_name, table_name, constraint_type, constraint_text FROM duckdb_constraints() "
        "WHERE database_name = ?"
    ),
    "sequences": (
        "SELECT schema_name, sequence_name, start_value, increment_by, last_value FROM duckdb_sequences() "
        "WHERE database_name = ?"
    ),
    "indexes": "SELECT schema_name, table_name, index_name, sql FROM duckdb_indexes() WHERE database_name = ?",
}

_log = get_logger(__name__)


@dataclass(frozen=True)
class CompactionResult:
    compacted: bool
    failed_check: str | None  # "rows and checksums", "schema", or "error: <class>" when the rewrite failed
    ms: int


def _attach(conn: duckdb.DuckDBPyConnection, path: Path, alias: str, key: str | None) -> None:
    if key is None:
        conn.execute(f"ATTACH {sql_text(path.as_posix())} AS {alias}")
    else:
        keyed_plain_attach(conn, path, alias=alias, key=key)


def _schema(conn: duckdb.DuckDBPyConnection, alias: str) -> dict[str, list[tuple[Any, ...]]]:
    return {name: sorted(conn.execute(sql, [alias]).fetchall(), key=repr) for name, sql in _SCHEMA_VIEWS.items()}


def _content(conn: duckdb.DuckDBPyConnection, alias: str) -> dict[str, Any]:
    found: dict[str, Any] = {}
    tables = conn.execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ?", [alias])
    for schema, table in tables.fetchall():
        query = f'SELECT count(*), coalesce(sum(hash(t)), 0) FROM {alias}."{schema}"."{table}" AS t'  # noqa: S608 - names read from the catalog's own tables
        found[f"{schema}.{table}"] = conn.execute(query).fetchone()
    return found


def compact_catalog_file(path: Path, *, key: str | None) -> CompactionResult:
    """Rewrite the detached catalog file at `path` into a fresh, compact file when it has at least
    `COMPACT_MIN_BYTES`; keep the original when the rewrite fails its verification."""
    started = time.perf_counter()
    if path.stat().st_size < COMPACT_MIN_BYTES:
        return CompactionResult(compacted=False, failed_check=None, ms=0)
    fresh = path.with_name(path.name + ".compact")
    fresh.unlink(missing_ok=True)
    # its own spill folder: DuckDB refuses `temp_file_encryption` while another instance (another process, a reader)
    # has temp files in the shared default `./.tmp` (audit T13)
    temp = path.with_name(f"{path.name}.compact-tmp-{uuid.uuid4().hex}")
    failed: str | None = None
    try:
        conn = duckdb.connect(config={"temp_directory": temp.as_posix()})
        try:
            # the source first, outside the fallback: a working copy that cannot be opened or read in full is never
            # published (Reviewer B T3-IMPL-04); a wrong key keeps its `ConfigError`
            try:
                _attach(conn, path, "src", key)
                source = _content(conn, "src")
            except duckdb.Error as exc:
                raise IntegrityError(
                    f"the working catalog copy cannot be read ({type(exc).__name__}); nothing is published"
                ) from exc
            try:
                _attach(conn, fresh, "dst", key)
                conn.execute("COPY FROM DATABASE src TO dst")
                if source != _content(conn, "dst"):
                    failed = "rows and checksums"
                elif _schema(conn, "src") != _schema(conn, "dst"):
                    failed = "schema"
                conn.execute("DETACH dst")
            except (duckdb.Error, OSError) as exc:
                # a rewrite-side error: the original was just read in full, so the publish goes on with it (Reviewer B
                # T3-IMPL-02); the failure is logged loudly and reported in the result
                failed = f"error: {type(exc).__name__}"
            conn.execute("DETACH src")
        finally:
            conn.close()
    finally:
        shutil.rmtree(temp, ignore_errors=True)
    ms = int((time.perf_counter() - started) * 1000)
    if failed is not None:
        fresh.unlink(missing_ok=True)
        _log.error("catalog compaction failed; the original is kept", extra={"check": failed})
        return CompactionResult(compacted=False, failed_check=failed, ms=ms)
    before = path.stat().st_size
    os.replace(fresh, path)
    _log.info("catalog compacted", extra={"bytes_before": before, "bytes_after": path.stat().st_size, "ms": ms})
    return CompactionResult(compacted=True, failed_check=None, ms=ms)
