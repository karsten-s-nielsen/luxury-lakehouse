"""DuckDB connections and the DuckLake data paths (spec §10.4, §10.5.1, §10.9).

Every DuckDB connection of v2 comes from `connect_duckdb`. It applies the resource caps and, when
`LAKEHOUSE_DUCKDB_EXTENSION_DIR` is set, the extension folder (for example on a read-only home folder).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import duckdb

from lakehouse.config.model import ResourceCaps
from lakehouse.errors import ConfigError
from lakehouse.storage.keys import RootMap

__all__ = [
    "EXTENSION_DIR_ENV",
    "PARQUET_OPTIONS",
    "PARQUET_OPTIONS_STORED",
    "attach_or_config_error",
    "connect_duckdb",
    "encryption_options",
    "ensure_parquet_options",
    "insert_rows",
    "is_wrong_key",
    "keyed_plain_attach",
    "reader_data_path",
    "sql_text",
    "writer_data_path",
]

EXTENSION_DIR_ENV: Final = "LAKEHOUSE_DUCKDB_EXTENSION_DIR"
# The §10.2 file format for files DuckLake writes itself (T3 spec D6): zstd level 3, Parquet version 2 (delta
# encoding on integer columns, dictionary encoding elsewhere). The spike measured DuckLake's defaults: SNAPPY, PLAIN.
PARQUET_OPTIONS: Final[dict[str, str]] = {
    "parquet_compression": "zstd",
    "parquet_compression_level": "3",
    "parquet_version": "2",
}
# How DuckLake reports each option once set: it stores the Parquet version `2` as `V2`. The check compares the stored
# form, so a re-attach that finds the options set calls no `set_option`.
PARQUET_OPTIONS_STORED: Final[dict[str, str]] = {**PARQUET_OPTIONS, "parquet_version": "V2"}
_IDENTIFIER: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def sql_text(value: str) -> str:
    """A SQL string literal. DuckDB `ATTACH` options take no bound parameters, so paths go in as literals."""
    return "'" + value.replace("'", "''") + "'"


def encryption_options(key: str | None) -> list[str]:
    """The DuckLake `ATTACH` options that encrypt the catalog + its data with `key` (spec S1), or `[]` when the
    lake is unencrypted (`key is None`). The key is a validated base64url/hex literal, so `sql_text` quoting is
    enough; the attach SQL is never logged."""
    if key is None:
        return []
    return ["ENCRYPTED", f"METADATA_PARAMETERS MAP {{'encryption_key': {sql_text(key)}}}"]


def is_wrong_key(exc: duckdb.Error) -> bool:
    """True only when DuckDB rejected the encryption KEY. A wrong key and a corrupt encrypted file both raise
    `duckdb.InvalidInputException`; only the message tells them apart ("Wrong encryption key ..." against "Computed
    AES tag differs ..."), measured by the T3 spike and reproduced by Reviewer B (T3 spec D7, T3-SPEC-22)."""
    return isinstance(exc, duckdb.InvalidInputException) and "Wrong encryption key" in str(exc)


def attach_or_config_error(conn: duckdb.DuckDBPyConnection, attach_sql: str, *, key_present: bool) -> None:
    """Run an `ATTACH`; when a key is present and DuckDB rejects the KEY, translate that into a `ConfigError`
    (exit 2), with no key value in the message, instead of an uncaught DuckDB error (exit 1 + traceback). Only the
    key rejection is translated (`is_wrong_key`): a corrupt encrypted catalog raises the same DuckDB class with
    another message, and it, a missing catalog, a format error and the no-key path propagate UNCHANGED, so a non-key
    cause keeps its own message and is never mislabeled as a wrong key (spec S1, plan T3/T4, audit S1-IMPL-08/17,
    T3 spec D7). The attach SQL is never placed in the message."""
    try:
        conn.execute(attach_sql)
    except duckdb.InvalidInputException as exc:
        if key_present and is_wrong_key(exc):
            raise ConfigError("cannot open the encrypted lake: the encryption key is wrong") from exc
        raise


def keyed_plain_attach(conn: duckdb.DuckDBPyConnection, path: Path, *, alias: str, key: str) -> None:
    """Open an encrypted catalog FILE as a plain DuckDB database (no DuckLake attach, so no DuckLake maintenance),
    in the form the S1 and T3 spikes established (T3 spec D7, decision 9). The S1 key rules apply: the key only
    reaches the `ATTACH`, never a log or an error message; only a wrong key maps to `ConfigError`; spill files are
    encrypted (`temp_file_encryption`)."""
    if not _IDENTIFIER.match(alias):
        raise ValueError(f"invalid SQL identifier {alias!r}")
    try:
        conn.execute(
            f"ATTACH {sql_text(path.as_posix())} AS {alias} (ENCRYPTION_KEY {sql_text(key)}, STORAGE_VERSION 'latest')"
        )
    except duckdb.InvalidInputException as exc:
        if is_wrong_key(exc):
            raise ConfigError("cannot open the encrypted catalog file: the encryption key is wrong") from exc
        raise
    conn.execute("SET temp_file_encryption = true")


def connect_duckdb(
    *,
    env: Mapping[str, str],
    caps: ResourceCaps | None,
    extensions: Sequence[str] = ("ducklake",),
    temp_directory: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection with the caps and extension folder applied and `extensions` loaded.
    `temp_directory` is a private spill folder; an instance that sets `temp_file_encryption` needs one, because DuckDB
    refuses that setting while another instance has temp files in the shared default `./.tmp` (audit T13)."""
    config: dict[str, str | bool | int | float | list[str]] = {}
    if caps is not None:
        config["threads"] = caps.threads
        config["memory_limit"] = caps.memory_limit
    if temp_directory is not None:
        config["temp_directory"] = temp_directory.as_posix()
    folder = env.get(EXTENSION_DIR_ENV)
    if folder:
        config["extension_directory"] = folder
    conn = duckdb.connect(config=config)
    for name in extensions:
        conn.install_extension(name)
        conn.load_extension(name)
    return conn


def writer_data_path(roots: RootMap) -> str:
    """The `data_path` of the working catalog: the lake root's `data/`, on HF through the S3 gateway (§10.5.1)."""
    lake = roots.lake
    if lake.local_path is not None:
        return lake.local_path.as_posix().rstrip("/") + "/data/"
    if lake.s3_bucket_path is None:
        raise PermissionError("only a host with the S3 gateway path of the lake can write the catalog")
    return lake.s3_bucket_path.rstrip("/") + "/data/"


def reader_data_path(roots: RootMap) -> str:
    """The `data_path` a reader re-roots DuckLake-written files to (§10.4): a local folder or `hf://`."""
    lake = roots.lake
    if lake.local_path is not None:
        return lake.local_path.as_posix().rstrip("/") + "/data/"
    return f"hf://buckets/{lake.hf_bucket}/data/"


def insert_rows(
    conn: duckdb.DuckDBPyConnection, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
) -> None:
    """Insert `rows` into `lake.main.<table>` with ONE statement (T3 spec D6). `executemany` runs one `INSERT` per row,
    and on the encrypted-local path (`DATA_INLINING_ROW_LIMIT 0`) each `INSERT` stores its own data file; one statement
    stores one file per table per write. `table` and `columns` are code constants, validated as identifiers."""
    for name in (table, *columns):
        if not _IDENTIFIER.match(name):
            raise ValueError(f"invalid SQL identifier {name!r}")
    if not rows:
        return
    width = len(columns)
    if any(len(row) != width for row in rows):
        raise ValueError(f"every row for {table} needs {width} values")
    group = "(" + ", ".join("?" for _ in columns) + ")"
    values = ", ".join(group for _ in rows)
    conn.execute(
        f"INSERT INTO lake.main.{table} ({', '.join(columns)}) VALUES {values}",  # noqa: S608 - validated identifiers, bound values
        [value for row in rows for value in row],
    )


def ensure_parquet_options(conn: duckdb.DuckDBPyConnection) -> None:
    """Make sure the attached catalog `lake` holds `PARQUET_OPTIONS` (T3 spec D6). An option is set only when it is
    absent or differs, so a re-attach that finds them set changes nothing in the catalog."""
    current = {
        str(name): str(value)
        for name, value in conn.execute("SELECT option_name, value FROM ducklake_options('lake')").fetchall()
    }
    for name, value in PARQUET_OPTIONS.items():
        if current.get(name) != PARQUET_OPTIONS_STORED[name]:
            conn.execute(f"CALL lake.set_option({sql_text(name)}, {sql_text(value)})")
