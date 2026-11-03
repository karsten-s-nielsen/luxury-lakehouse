"""Bronze write modes (spec S1 approach A-prime, plan T8).

Two implementations behind one seam, chosen by `select_writer` from the catalog's encryption:

- `RegisterWriter` — today's path, byte-unchanged for **unencrypted-local and HF**: each table is written to a
  parquet file, stored in the root its visibility selects (`place_root`), and registered with
  `ducklake_add_data_files`; the match's old rows are replaced by `swap_match_files`.
- `LocalNativeWriter` — **encrypted-local**: each table's rows are written into the DuckLake table natively
  (`INSERT`), so `ENCRYPTED` covers the data; the swap is delete-by-`match_key` + `INSERT` in one transaction. No
  external file is written (no plaintext window), and DuckLake manages the parquet layout.

`write_table` runs in the caller's write span (returns one `WrittenRow` per table, for the row-count span and the
`ops_match_files` registry); `swap` runs in the caller's commit span (the atomic replace + the caller's `also`).
The writer is used for one match at a time.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import duckdb
import pyarrow as pa

from lakehouse.bronze.evolve import reconcile_columns, relation_columns
from lakehouse.bronze.listings import split_listing, write_listing_parts
from lakehouse.bronze.schemas import BronzeTable
from lakehouse.bronze.writer import (
    BRONZE_COMMENT,
    Visibility,
    WrittenFile,
    assert_bronze_registered_only,
    prepare_bronze_table,
    prepare_tracking_table,
    store_written_file,
    swap_files,
    swap_match_files,
    write_bronze_file,
    write_tracking_file,
)
from lakehouse.catalog.duck import sql_text
from lakehouse.catalog.visibility import VISIBILITY_COLUMN
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.errors import GuardStopError
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import Root

__all__ = [
    "VISIBILITY_COLUMN",
    "BronzeWriter",
    "ListingWriter",
    "LocalNativeWriter",
    "NativeListingWriter",
    "Put",
    "RegisterListingWriter",
    "RegisterWriter",
    "WrittenRow",
    "assert_bronze_integrity",
    "assert_visibility_integrity",
    "native_swap",
    "select_listing_writer",
    "select_writer",
]


@dataclass(frozen=True)
class Put:
    """One table of a match to persist: the parsed arrow plus what the non-tracking file writer needs."""

    table: BronzeTable
    data: pa.Table
    is_tracking: bool
    schema: pa.Schema | None  # non-tracking only
    source_hash: str | frozenset[str] | None  # non-tracking only


@dataclass(frozen=True)
class WrittenRow:
    """What a write produced, for the row-count span and the `ops_match_files` registry."""

    table: BronzeTable
    root: Root
    path: str
    rows: int


class BronzeWriter(Protocol):
    def write_table(
        self,
        put: Put,
        *,
        match_key: int,
        visibility: Visibility,
        content_version: str,
        run_id: str,
        out_dir: Path,
        ingested_at: datetime,
    ) -> WrittenRow: ...

    def swap(
        self, match_key: int, written: Sequence[WrittenRow], old_tables: set[BronzeTable], *, also: Callable[[], None]
    ) -> None: ...


@dataclass
class RegisterWriter:
    """Today's add_data_files path (unencrypted-local and HF), byte-unchanged."""

    cat: WorkingCatalog
    storage: Storage
    _files: dict[BronzeTable, WrittenFile] = field(default_factory=dict[BronzeTable, "WrittenFile"])

    def write_table(
        self,
        put: Put,
        *,
        match_key: int,
        visibility: Visibility,
        content_version: str,
        run_id: str,
        out_dir: Path,
        ingested_at: datetime,
    ) -> WrittenRow:
        if put.is_tracking:
            path = write_tracking_file(
                put.table, put.data, match_key=match_key, content_version=content_version, out_dir=out_dir
            )
        else:
            path = write_bronze_file(
                put.table,
                put.data,
                schema=_require(put.schema),
                match_key=match_key,
                content_version=content_version,
                out_dir=out_dir,
                run_id=run_id,
                source_hash=_require(put.source_hash),
                ingested_at=ingested_at,
            )
        file = store_written_file(
            self.storage,
            path,
            table=put.table,
            visibility=visibility,
            rows=put.data.num_rows,
            content_version=content_version,
        )
        self._files[put.table] = file
        return WrittenRow(put.table, file.root, file.key.path, put.data.num_rows)

    def swap(
        self, match_key: int, written: Sequence[WrittenRow], old_tables: set[BronzeTable], *, also: Callable[[], None]
    ) -> None:
        keys_by_table: dict[BronzeTable, list[Any]] = {table: [] for table in old_tables}
        for row in written:
            keys_by_table.setdefault(row.table, []).append(self._files[row.table].key)
        swap_match_files(self.cat, match_key, keys_by_table, also=also)
        self._files.clear()


def _require(value: Any) -> Any:
    if value is None:
        raise ValueError("a non-tracking bronze put needs its schema and source hash")
    return value


def select_writer(cat: WorkingCatalog, storage: Storage) -> BronzeWriter:
    """`LocalNativeWriter` on an encrypted-local lake (the catalog holds a key), else `RegisterWriter`."""
    if cat.encryption_key is not None:
        return LocalNativeWriter(cat, storage)
    return RegisterWriter(cat, storage)


_NATIVE_VIEW = "s1_native_ins"


def _native_table_exists(conn: duckdb.DuckDBPyConnection, table: BronzeTable) -> bool:
    cursor = conn.cursor()  # isolate the result so a later conn.register has no unfinished result
    try:
        row = cursor.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' "
            "AND table_name = ?",
            [table],
        ).fetchone()
        return row is not None and bool(row[0])
    finally:
        cursor.close()


def _ensure_native_table(conn: duckdb.DuckDBPyConnection, table: BronzeTable, arrow: pa.Table) -> bool:
    """Create an empty encrypted-local bronze table from the arrow's schema, outside any transaction. DuckLake's
    `set_option` refuses a table created inside the same transaction, so the create and the `auto_compact` option
    run before `native_swap` opens its `BEGIN`. Returns whether the table existed already (so the write reconciles
    it, audit T9)."""
    if _native_table_exists(conn, table):
        return True
    conn.register(_NATIVE_VIEW, arrow)
    try:
        conn.execute(f'CREATE TABLE lake.main."{table}" AS SELECT * FROM {_NATIVE_VIEW} LIMIT 0')  # noqa: S608 - bronze_table name
        conn.execute(f'COMMENT ON TABLE lake.main."{table}" IS {sql_text(BRONZE_COMMENT)}')
        conn.execute("CALL lake.set_option('auto_compact', false, table_name => ?)", [table])
    finally:
        conn.unregister(_NATIVE_VIEW)
    return False


def native_swap(
    conn: duckdb.DuckDBPyConnection,
    pending: Mapping[BronzeTable, pa.Table],
    delete_tables: set[BronzeTable],
    *,
    key_column: str,
    key: Any,
    also: Callable[[], None],
) -> None:
    """Replace one key's rows across encrypted-local bronze tables in a single transaction (the atomic unit for
    both a match write, `key_column="match_key"`, and a listing write, `key_column="listing_key"`): delete the
    key's rows from every table that should lose them, insert the pending arrow rows, run `also`, then commit."""
    # `conn.register` (below) refuses to run while the connection has an unfinished result, and an upstream read
    # that early-returns after a `count(*)` `fetchone` (an exists-check such as `read_lineage`/`read_table`) leaves
    # one. A plain `execute` finalizes it, so drain once here before the first `register` (a normal `execute` does
    # this implicitly, which is why only the native-register path needs it).
    conn.execute("SELECT 1").fetchall()
    existed = {table: _ensure_native_table(conn, table, arrow) for table, arrow in pending.items()}
    conn.execute("BEGIN")
    try:
        for table in delete_tables | set(pending):
            if _native_table_exists(conn, table):
                conn.execute(f'DELETE FROM lake.main."{table}" WHERE {key_column} = ?', [key])  # noqa: S608 - bronze_table + constant column
        for table, arrow in pending.items():
            conn.register(_NATIVE_VIEW, arrow)
            try:
                if existed[table]:  # inside the transaction: a failed write rolls an added column back (audit T9)
                    incoming = relation_columns(conn, f"SELECT * FROM {_NATIVE_VIEW}")  # noqa: S608 - constant view name
                    reconcile_columns(conn, table, incoming)
                # by name: an added column sits at the table's end, not where the arrow has it (audit T9)
                conn.execute(f'INSERT INTO lake.main."{table}" BY NAME SELECT * FROM {_NATIVE_VIEW}')  # noqa: S608 - bronze_table name
            finally:
                conn.unregister(_NATIVE_VIEW)
        also()
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


@dataclass
class LocalNativeWriter:
    """Encrypted-local native writes: DuckLake writes + encrypts the parquet. `write_table` validates the arrow and
    stashes it; `swap` deletes the match's old rows and `INSERT`s the new ones in one transaction. No external file
    is written (no plaintext window); DuckLake manages the parquet under `data/main/`, so files are not per-match
    and `match_of()` cannot parse a `match_key` from their names — the file-based consumers skip them."""

    cat: WorkingCatalog
    storage: Storage
    _pending: dict[BronzeTable, pa.Table] = field(default_factory=dict[BronzeTable, "pa.Table"])

    def write_table(
        self,
        put: Put,
        *,
        match_key: int,
        visibility: Visibility,
        content_version: str,
        run_id: str,
        out_dir: Path,
        ingested_at: datetime,
    ) -> WrittenRow:
        if put.is_tracking:
            arrow = prepare_tracking_table(put.table, put.data, match_key=match_key, content_version=content_version)
        else:
            arrow = prepare_bronze_table(
                put.table,
                put.data,
                schema=_require(put.schema),
                match_key=match_key,
                content_version=content_version,
                run_id=run_id,
                source_hash=_require(put.source_hash),
                ingested_at=ingested_at,
            )
        arrow = arrow.append_column(
            pa.field(VISIBILITY_COLUMN, pa.string(), nullable=False),
            pa.array([visibility] * arrow.num_rows, pa.string()),
        )
        self._pending[put.table] = arrow
        return WrittenRow(put.table, "lake", f"data/main/{put.table}", arrow.num_rows)

    def swap(
        self, match_key: int, written: Sequence[WrittenRow], old_tables: set[BronzeTable], *, also: Callable[[], None]
    ) -> None:
        native_swap(self.cat.conn, self._pending, old_tables, key_column="match_key", key=match_key, also=also)
        self._pending.clear()


class ListingWriter(Protocol):
    def write(
        self,
        table: BronzeTable,
        records: pa.Table,
        *,
        key: int,
        visibility_column: str | None,
        source_visibility: Visibility,
        content_version: str,
        out_dir: Path,
    ) -> int: ...

    def swap(self, table: BronzeTable, key: int, *, also: Callable[[], None]) -> None: ...


@dataclass
class RegisterListingWriter:
    """Today's two-root listing path (unencrypted-local and HF), byte-unchanged: one file per visibility, stored
    in the root its visibility selects, registered with `ducklake_add_data_files`; `swap_files` replaces the
    listing's old rows."""

    cat: WorkingCatalog
    storage: Storage
    _parts: list[WrittenFile] = field(default_factory=list["WrittenFile"])

    def write(
        self,
        table: BronzeTable,
        records: pa.Table,
        *,
        key: int,
        visibility_column: str | None,
        source_visibility: Visibility,
        content_version: str,
        out_dir: Path,
    ) -> int:
        self._parts = write_listing_parts(
            self.storage,
            table,
            records,
            key=key,
            visibility_column=visibility_column,
            source_visibility=source_visibility,
            content_version=content_version,
            out_dir=out_dir,
        )
        return records.num_rows

    def swap(self, table: BronzeTable, key: int, *, also: Callable[[], None]) -> None:
        swap_files(self.cat, key, {table: [part.key for part in self._parts]}, key_column="listing_key", also=also)
        self._parts = []


@dataclass
class NativeListingWriter:
    """Encrypted-local native listing writes: the records are split by visibility (`split_listing`), tagged with a
    per-record `_visibility` column and a `listing_key` column, and written into one encrypted DuckLake table, so a
    private listing record never lands in a second root. The swap deletes the listing's old rows and inserts the
    new ones in one transaction; a listing with no records deletes with nothing to insert."""

    cat: WorkingCatalog
    storage: Storage
    _arrow: pa.Table | None = None

    def write(
        self,
        table: BronzeTable,
        records: pa.Table,
        *,
        key: int,
        visibility_column: str | None,
        source_visibility: Visibility,
        content_version: str,
        out_dir: Path,
    ) -> int:
        tagged: list[pa.Table] = []
        parts = split_listing(records, visibility_column=visibility_column, source_visibility=source_visibility)
        for root, part in sorted(parts.items()):
            visibility: Visibility = "public" if root == "lake" else "private"
            part = part.append_column(
                pa.field("listing_key", pa.int64(), nullable=False), pa.array([key] * part.num_rows, pa.int64())
            )
            part = part.append_column(
                pa.field(VISIBILITY_COLUMN, pa.string(), nullable=False),
                pa.array([visibility] * part.num_rows, pa.string()),
            )
            tagged.append(part)
        self._arrow = pa.concat_tables(tagged) if tagged else None
        return self._arrow.num_rows if self._arrow is not None else 0

    def swap(self, table: BronzeTable, key: int, *, also: Callable[[], None]) -> None:
        pending: dict[BronzeTable, pa.Table] = {} if self._arrow is None else {table: self._arrow}
        native_swap(self.cat.conn, pending, {table}, key_column="listing_key", key=key, also=also)
        self._arrow = None


def select_listing_writer(cat: WorkingCatalog, storage: Storage) -> ListingWriter:
    """`NativeListingWriter` on an encrypted-local lake (the catalog holds a key), else `RegisterListingWriter`."""
    if cat.encryption_key is not None:
        return NativeListingWriter(cat, storage)
    return RegisterListingWriter(cat, storage)


def _match_keys_suffix(cursor: duckdb.DuckDBPyConnection, from_where: str, columns: set[str]) -> str:
    """`" (match_key ...)"` naming up to five offending matches, so the operator sees which match broke the guard
    (T3 spec D1). `from_where` is a constant FROM/WHERE clause over a checked bronze table name."""
    if "match_key" not in columns:
        return ""
    rows = cursor.execute(f"SELECT DISTINCT match_key {from_where} ORDER BY match_key LIMIT 5").fetchall()
    return f" (match_key {', '.join(str(row[0]) for row in rows)})" if rows else ""


def assert_visibility_integrity(cat: WorkingCatalog) -> None:
    """Raise `GuardStopError` when an encrypted-local bronze table breaks the `_visibility` invariant (plan T10).

    On an encrypted-local lake the per-row `_visibility` is the authoritative restricted copy, so every bronze
    table must carry it, no row may leave it null, and a match-scoped row must equal its `ops_matches.visibility`.
    A null would make the read seam's filter indeterminate; a disagreement would leak or hide a match.
    """
    # Run every query on a child cursor, closed at the end: a `count(*)` read with `fetchone` leaves an unfinished
    # result on the connection it runs on, and the guard fires after a commit and before the next bronze write's
    # `conn.register`, which refuses to run while a result is pending. The child isolates that from the main conn.
    cursor = cat.conn.cursor()
    try:
        tables = cursor.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' "
            "AND comment = ?",
            [BRONZE_COMMENT],
        ).fetchall()
        # Before the first match write there is no ops_matches (a run that lists sources and selects no match): no row
        # can disagree with it then, so the match check has nothing to compare (T3 real-data diagnostic, 2026-11-01).
        has_matches = bool(
            cursor.execute(
                "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' "
                "AND table_name = 'ops_matches'"
            ).fetchall()
        )
        for (table,) in tables:
            name = str(table)
            columns = {
                str(row[0])
                for row in cursor.execute(
                    "SELECT column_name FROM duckdb_columns() WHERE database_name = 'lake' "
                    "AND schema_name = 'main' AND table_name = ?",
                    [name],
                ).fetchall()
            }
            if VISIBILITY_COLUMN not in columns:
                raise GuardStopError(f"encrypted-local bronze table {name} has no {VISIBILITY_COLUMN} column")
            nulls = cursor.execute(
                f'SELECT count(*) FROM lake.main."{name}" WHERE {VISIBILITY_COLUMN} IS NULL'  # noqa: S608 - bronze_table + constant column
            ).fetchone()
            if nulls is not None and nulls[0]:
                where = f'FROM lake.main."{name}" WHERE {VISIBILITY_COLUMN} IS NULL'
                raise GuardStopError(
                    f"bronze table {name} has {nulls[0]} rows with a null {VISIBILITY_COLUMN}"
                    + _match_keys_suffix(cursor, where, columns)
                )
            if has_matches and "match_key" in columns:
                join = (
                    f'FROM lake.main."{name}" AS b JOIN lake.main.ops_matches AS m USING (match_key) '
                    f"WHERE b.{VISIBILITY_COLUMN} <> m.visibility"
                )
                mismatched = cursor.execute(f"SELECT count(*) {join}").fetchone()
                if mismatched is not None and mismatched[0]:
                    raise GuardStopError(
                        f"bronze table {name} has {mismatched[0]} rows whose {VISIBILITY_COLUMN} disagrees with "
                        "ops_matches" + _match_keys_suffix(cursor, join, columns)
                    )
    finally:
        cursor.close()


def assert_bronze_integrity(cat: WorkingCatalog) -> None:
    """Verify bronze after a write, by mode (plan T10). Encrypted-local checks the `_visibility` invariant; an
    unencrypted-local or HF lake checks that no DuckLake-native file sneaked into a bronze table (its rows could
    mix visibilities under one file). Wired into every bronze write/commit seam, so a break stops the run."""
    if cat.encryption_key is not None:
        assert_visibility_integrity(cat)
    else:
        assert_bronze_registered_only(cat)
