"""The per-match bronze writer, root placement and registration (spec §9, §10.2, §10.3 steps 1 to 4, §29.10).

Every bronze file is written by v2, one file per match per table, then stored in the root that its match visibility
selects, then registered with `ducklake_add_data_files` under the URI that readers use. A file name is
`<match_key>-<content_version>-<write_id>.parquet`; the write id is new for every file write, a move copy included,
so no object key is ever registered twice.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from lakehouse.bronze.arrow import column_types, sort_table, write_parquet
from lakehouse.bronze.evolve import reconcile_columns, relation_columns
from lakehouse.bronze.schemas import AUDIT_COLUMNS, BronzeTable, bronze_table
from lakehouse.bronze.tracking import check_tracking_schema, encoding_options, sort_keys, tracking_kind
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.errors import GuardStopError, IntegrityError
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import CanonicalKey, Root, canonical_key

__all__ = [
    "BRONZE_COMMENT",
    "FileName",
    "Visibility",
    "WrittenFile",
    "assert_bronze_registered_only",
    "parse_file_name",
    "place_root",
    "prepare_bronze_table",
    "prepare_tracking_table",
    "reader_uri",
    "register_match_files",
    "replace_match_files",
    "store_written_file",
    "swap_files",
    "swap_match_files",
    "table_of",
    "write_bronze_file",
    "write_tracking_file",
]

Visibility = Literal["public", "private"]

BRONZE_COMMENT: Final = "lakehouse:bronze"
_CONTENT_VERSION: Final = re.compile(r"^[A-Za-z0-9_.]+$")
_FILE_NAME: Final = re.compile(r"^(-?[0-9]+)-([A-Za-z0-9_.]+)-([0-9a-f]{32})\.parquet$")
_ZSTD_LEVEL: Final = 3
_NATIVE_PREFIX: Final = "data/main/"


@dataclass(frozen=True)
class FileName:
    match_key: int
    content_version: str
    write_id: str


@dataclass(frozen=True)
class WrittenFile:
    table: BronzeTable
    root: Root
    key: CanonicalKey
    local_path: Path
    rows: int
    content_version: str


def place_root(visibility: Visibility) -> Root:
    """The root that a match visibility selects (§9): public to the lake root, private to the restricted root."""
    if visibility == "public":
        return "lake"
    if visibility == "private":
        return "restricted"
    raise ValueError(f"unknown visibility {visibility!r}")


def parse_file_name(name: str) -> FileName:
    match = _FILE_NAME.match(name)
    if match is None:
        raise ValueError(f"{name!r} is not a bronze file name <match_key>-<content_version>-<write_id>.parquet")
    return FileName(match_key=int(match.group(1)), content_version=match.group(2), write_id=match.group(3))


def _check_rows(data: pa.Table, match_key: int, content_version: str) -> None:
    if not _CONTENT_VERSION.match(content_version):
        raise ValueError(f"invalid content version {content_version!r}: letters, digits, '_' and '.' only")
    if data.num_rows == 0:
        raise ValueError("the data has no rows: a bronze file is never empty")
    keys = data.column("match_key") if "match_key" in data.column_names else None
    if keys is None:
        raise ValueError("the data has no match_key column")
    if keys.null_count or any(value != match_key for value in keys.to_pylist()):
        raise ValueError(f"every row must have match_key {match_key}: one file holds one match")


def _target(out_dir: Path, table: BronzeTable, match_key: int, content_version: str) -> Path:
    folder = out_dir / table
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{match_key}-{content_version}-{uuid.uuid4().hex}.parquet"


def _verify_written(path: Path, rows: int) -> Path:
    written = pq.ParquetFile(path).metadata.num_rows
    if written != rows:
        path.unlink(missing_ok=True)
        raise IntegrityError(f"{path.name} holds {written} rows, but {rows} were written")
    return path


def prepare_tracking_table(table: BronzeTable, data: pa.Table, *, match_key: int, content_version: str) -> pa.Table:
    """Validate and sort a tracking table (§10.2), returning the arrow a file write or a native write both use."""
    kind = tracking_kind(table)
    check_tracking_schema(kind, data.schema)
    _check_rows(data, match_key, content_version)
    return sort_table(data, sort_keys(kind))


def write_tracking_file(
    table: BronzeTable, data: pa.Table, *, match_key: int, content_version: str, out_dir: Path
) -> Path:
    """Write one tracking file in the tuned layout (§10.2): sorted, delta and dictionary encoded, zstd 3."""
    ordered = prepare_tracking_table(table, data, match_key=match_key, content_version=content_version)
    kind = tracking_kind(table)
    dictionary, delta = encoding_options(kind, ordered.schema)
    path = _target(out_dir, table, match_key, content_version)
    write_parquet(ordered, path, compression_level=_ZSTD_LEVEL, use_dictionary=dictionary, column_encoding=delta)
    return _verify_written(path, data.num_rows)


def write_bronze_file(
    table: BronzeTable,
    data: pa.Table,
    *,
    schema: pa.Schema,
    match_key: int,
    content_version: str,
    out_dir: Path,
    run_id: str,
    source_hash: str | frozenset[str],
    ingested_at: datetime | None = None,
) -> Path:
    """Write one non-tracking per-match file with the audit columns `_ingested_at`, `_run_id`, `_source_hash`.

    A parser may add the audit columns itself (it knows each artifact's source hash); the file is then written as is,
    after a check that `_run_id` holds exactly `run_id` and that `_source_hash` holds `source_hash` — or, for a table
    built from several deliveries, only hashes of `source_hash` given as the set of the match's delivery hashes.
    Without parser audit columns, `source_hash` must be the one hash to fill in."""
    audited = prepare_bronze_table(
        table,
        data,
        schema=schema,
        match_key=match_key,
        content_version=content_version,
        run_id=run_id,
        source_hash=source_hash,
        ingested_at=ingested_at,
    )
    path = _target(out_dir, table, match_key, content_version)
    write_parquet(audited, path, compression_level=_ZSTD_LEVEL)
    return _verify_written(path, data.num_rows)


def prepare_bronze_table(
    table: BronzeTable,
    data: pa.Table,
    *,
    schema: pa.Schema,
    match_key: int,
    content_version: str,
    run_id: str,
    source_hash: str | frozenset[str],
    ingested_at: datetime | None = None,
) -> pa.Table:
    """Validate a non-tracking table and return it with the audit columns, for a file write or a native write.

    See `write_bronze_file` for the audit-column rules; this function does the validation and the audit-column
    fill, and returns the arrow table, without writing a file."""
    audit_names = list(column_types(AUDIT_COLUMNS))
    carried = all(name in data.column_names for name in audit_names)
    base = data.drop_columns(audit_names) if carried else data
    if not base.schema.equals(schema):
        raise ValueError(f"the data schema of {table} differs from the bronze schema:\n{base.schema}\n!=\n{schema}")
    _check_rows(data, match_key, content_version)
    rows = data.num_rows
    if carried:
        if not data.select(audit_names).schema.equals(AUDIT_COLUMNS):
            raise ValueError(f"the audit columns of {table} do not have the audit types")
        if set(data.column("_run_id").to_pylist()) != {run_id}:
            raise ValueError(f"{table}: every _run_id must be {run_id!r}")
        found = {str(value) for value in data.column("_source_hash").to_pylist()}
        allowed = {source_hash} if isinstance(source_hash, str) else set(source_hash)
        if not found <= allowed:
            raise ValueError(f"{table}: every _source_hash must be one of {sorted(allowed)!r}, not {sorted(found)!r}")
        return data
    if not isinstance(source_hash, str):
        raise ValueError(f"{table}: without parser audit columns, the file needs one source hash")
    when = ingested_at if ingested_at is not None else datetime.now(UTC)
    if when.tzinfo is None:
        raise ValueError("ingested_at must be timezone-aware (UTC)")
    audited = data
    values: tuple[object, ...] = (when, run_id, source_hash)
    for (name, arrow_type), value in zip(column_types(AUDIT_COLUMNS).items(), values, strict=True):
        audited = audited.append_column(
            pa.field(name, arrow_type, nullable=False), pa.array([value] * rows, arrow_type)
        )
    return audited


def store_written_file(
    storage: Storage, path: Path, *, table: BronzeTable, visibility: Visibility, rows: int, content_version: str
) -> WrittenFile:
    """Store a written file in the root that its visibility selects, under `data/bronze/<table>/<name>`."""
    root = place_root(visibility)
    parse_file_name(path.name)
    key = f"data/bronze/{table}/{path.name}"
    storage.put(root, key, path)
    return WrittenFile(
        table=table, root=root, key=CanonicalKey(root, key), local_path=path, rows=rows, content_version=content_version
    )


def reader_uri(storage: Storage, key: CanonicalKey) -> str:
    """The URI under which a stored file is registered and read: a local POSIX path, or `hf://buckets/...`."""
    uri = storage.read_uri(key.root, key.path)
    return uri if uri.startswith("hf://") else Path(uri).as_posix()


def table_of(key: CanonicalKey) -> BronzeTable:
    """The bronze table of a stored bronze key `data/bronze/<table>/<file>`."""
    parts = key.path.split("/")
    if len(parts) != 4 or parts[:2] != ["data", "bronze"]:
        raise ValueError(f"{key} is not a bronze file key data/bronze/<table>/<file>")
    return bronze_table(parts[2])


def _table_exists(cat: WorkingCatalog, table: BronzeTable) -> bool:
    exists = cat.conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        [table],
    ).fetchone()
    return exists is not None and bool(exists[0])


def _require_stored(cat: WorkingCatalog, keys_by_table: Mapping[BronzeTable, Sequence[CanonicalKey]]) -> None:
    """A file is stored before it is registered (§10.3); checked before any file is read."""
    for table, keys in keys_by_table.items():
        for key in keys:
            if table_of(key) != table:
                raise ValueError(f"{key} belongs to {table_of(key)}, not {table}")
            if not cat.storage.exists(key.root, key.path):
                raise IntegrityError(f"{key} is not in storage: a file is stored before it is registered (§10.3)")


def _ensure_table(cat: WorkingCatalog, table: BronzeTable, first: CanonicalKey) -> bool:
    """Create the table from `first`'s schema, outside any transaction: DuckLake refuses `set_option` on a table made
    inside one. Returns whether the table existed already (so the write reconciles it)."""
    if _table_exists(cat, table):
        return True
    cat.conn.execute(
        f'CREATE TABLE lake.main."{table}" AS SELECT * FROM read_parquet(?) LIMIT 0',  # noqa: S608 - name checked by bronze_table
        [reader_uri(cat.storage, first)],
    )
    cat.conn.execute(f"COMMENT ON TABLE lake.main.\"{table}\" IS '{BRONZE_COMMENT}'")
    cat.conn.execute("CALL lake.set_option('auto_compact', false, table_name => ?)", [table])
    return False


def _reconcile(cat: WorkingCatalog, table: BronzeTable, first: CanonicalKey, *, copies: bool) -> None:
    """Inside the write's transaction, so a failed write rolls it back: reconcile an existing table with `first`'s
    columns (additive evolution, audit T9). `copies`: the keys copy files that the table already holds, so an
    older, narrower file is accepted."""
    incoming = relation_columns(cat.conn, "SELECT * FROM read_parquet(?)", [reader_uri(cat.storage, first)])
    reconcile_columns(cat.conn, table, incoming, narrower_ok=copies)


def _add_keys(cat: WorkingCatalog, table: BronzeTable, keys: Sequence[CanonicalKey], *, copies: bool = False) -> None:
    for key in keys:
        cat.conn.execute(
            "CALL ducklake_add_data_files('lake', ?, ?, allow_missing => ?)",
            [table, reader_uri(cat.storage, key), copies],
        )


def swap_match_files(
    cat: WorkingCatalog,
    match_key: int,
    keys_by_table: Mapping[BronzeTable, Sequence[CanonicalKey]],
    *,
    also: Callable[[], None] | None = None,
    copies: bool = False,
) -> None:
    """In one transaction: per table, delete the match's rows and register `keys`; then run `also` (for example
    the move records of a visibility move). One file per match per table, so the delete removes exactly the
    match's old file of each table. `copies`: the keys copy files that the tables already hold (a visibility move),
    so a file older than an added column is accepted, and reads null there as the original did."""
    swap_files(cat, match_key, keys_by_table, key_column="match_key", also=also, copies=copies)


def swap_files(
    cat: WorkingCatalog,
    key: int,
    keys_by_table: Mapping[BronzeTable, Sequence[CanonicalKey]],
    *,
    key_column: Literal["match_key", "listing_key"],
    also: Callable[[], None] | None = None,
    copies: bool = False,
) -> None:
    """`swap_match_files` for any file key: a match (`match_key`) or a source-level listing (`listing_key`, §10.5).
    Every file name starts with the key, and the delete removes exactly the key's rows of each table."""
    for keys in keys_by_table.values():
        for stored in keys:
            if parse_file_name(Path(stored.path).name).match_key != key:
                raise ValueError(f"{stored} is not a file of {key_column} {key}")
    _require_stored(cat, keys_by_table)
    existed = {table: _ensure_table(cat, table, keys[0]) for table, keys in keys_by_table.items() if keys}
    cat.conn.execute("BEGIN")
    try:
        for table, keys in keys_by_table.items():
            if existed.get(table):
                _reconcile(cat, table, keys[0], copies=copies)
            if _table_exists(cat, table):
                delete = f'DELETE FROM lake.main."{table}" WHERE {key_column} = ?'  # noqa: S608 - names checked
                cat.conn.execute(delete, [key])
            _add_keys(cat, table, keys, copies=copies)
        if also is not None:
            also()
        cat.conn.execute("COMMIT")
    except BaseException:
        cat.conn.execute("ROLLBACK")
        raise


def register_match_files(cat: WorkingCatalog, files: Sequence[WrittenFile]) -> None:
    """Register stored files in one transaction; a new table gets `auto_compact = false` and the bronze mark."""
    by_table: dict[BronzeTable, list[CanonicalKey]] = {}
    for file in files:
        if table_of(file.key) != file.table:
            raise ValueError(f"{file.key} is not stored under its table {file.table}")
        by_table.setdefault(file.table, []).append(file.key)
    if not by_table:
        return
    _require_stored(cat, by_table)
    existed = {table: _ensure_table(cat, table, keys[0]) for table, keys in by_table.items()}
    cat.conn.execute("BEGIN")
    try:
        for table, keys in by_table.items():
            if existed[table]:
                _reconcile(cat, table, keys[0], copies=False)
            _add_keys(cat, table, keys)
        cat.conn.execute("COMMIT")
    except BaseException:
        cat.conn.execute("ROLLBACK")
        raise


def replace_match_files(cat: WorkingCatalog, table: BronzeTable, match_key: int, files: Sequence[WrittenFile]) -> None:
    """Replace a match in one transaction: delete its rows, then register the new files (§10.3 step 4)."""
    if not files:
        raise ValueError("a replace needs at least one new file")
    for file in files:
        if file.table != table:
            raise ValueError(f"{file.key} belongs to {file.table}, not {table}")
    swap_match_files(cat, match_key, {table: [file.key for file in files]})


def assert_bronze_registered_only(cat: WorkingCatalog) -> None:
    """Raise `GuardStopError` when a bronze table has a file that the DuckLake native writer wrote (§10.5).

    A native write to a bronze table lands under the lake's `data/main/` prefix and could mix public and private rows.
    """
    tables = cat.conn.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' AND comment = ?",
        [BRONZE_COMMENT],
    ).fetchall()
    for schema, table in tables:
        rows = cat.conn.execute(
            "SELECT data_file FROM ducklake_list_files('lake', ?, schema => ?)", [table, schema]
        ).fetchall()
        for (data_file,) in rows:
            key = canonical_key(str(data_file), roots=cat.storage.roots, relative_to_data_path=False)
            if key.root == "lake" and key.path.startswith(_NATIVE_PREFIX):
                raise GuardStopError(f"bronze table {table} has a DuckLake-written file {key}: bronze is ours only")
