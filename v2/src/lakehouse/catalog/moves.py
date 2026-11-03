"""Visibility moves per match and their move records (spec §9, §29.7, §29.10, §29.11, §29.12).

A public-to-private move works per match: every lake-root bronze object of the match that the current catalog
registers or a kept version names gets a restricted copy under a new write id and a `to_private` record; every
object that only DuckLake still tracks, or that only exists in storage under the match's file-name prefix, gets a
`to_private` record with no new key. One catalog commit registers the copies and writes the records; right after it,
the lake-root objects are deleted. A crash between the two is closed by `delete_moved_leftovers` at the next run's
start. A private-to-public move copies the current restricted files to the lake root and leaves the old restricted
copies for the normal cleanup.

The same private move copies the lake-root raw files of the match's pining deliveries (from `ops_lineage`) to the
restricted root and records them; a lake-root raw object stays when a public-channel delivery has the same content,
because it is public at its source (§29.12). A failed raw delete does not stop the run: its record stays, and the next
run start deletes the object and only then settles the record (§10.8).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Final, Literal

import duckdb

from lakehouse.bronze.schemas import BronzeTable
from lakehouse.bronze.writer import BRONZE_COMMENT, parse_file_name, swap_match_files, table_of
from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.publish import listed_sequences
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.errors import GuardStopError
from lakehouse.lineage.model import public_contents, read_lineage
from lakehouse.logging import get_logger
from lakehouse.raw.archive import RAW_PREFIX, RawArchive, raw_key
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import CanonicalKey, Root, RootMap, canonical_key

__all__ = [
    "MOVES_TABLE",
    "Direction",
    "MatchObjects",
    "MoveRecord",
    "StagedMove",
    "assert_no_moved_key_registered",
    "bronze_tables",
    "copy_with_new_write_id",
    "delete_moved_leftovers",
    "finish_match_move",
    "match_lake_objects",
    "match_of",
    "move_match_files",
    "prune_settled_raw_records",
    "raw_content",
    "read_moves",
    "record_move",
    "stage_match_move",
    "write_moves",
]

MOVES_TABLE: Final = "ops_move_records"
Direction = Literal["to_private", "to_public"]

_CREATE: Final = (
    f"CREATE TABLE IF NOT EXISTS lake.main.{MOVES_TABLE} (match_key BIGINT NOT NULL, direction VARCHAR NOT NULL, "
    "old_key VARCHAR NOT NULL, new_key VARCHAR, moved_at TIMESTAMPTZ NOT NULL, run_id VARCHAR NOT NULL)"
)
_INSERT: Final = f"INSERT INTO lake.main.{MOVES_TABLE} VALUES (?, ?, ?, ?, ?, ?)"  # noqa: S608 - constant table name

_log = get_logger(__name__)


@dataclass(frozen=True)
class MoveRecord:
    match_key: int
    direction: Direction
    old_key: CanonicalKey
    new_key: CanonicalKey | None
    at: datetime
    run_id: str

    def __post_init__(self) -> None:
        if self.new_key is None and self.direction != "to_private":
            raise ValueError("only a to_private record may have no new key (§29.11)")


@dataclass(frozen=True)
class MatchObjects:
    """The match's lake-root bronze objects, as four disjoint sets (§29.11)."""

    registered: frozenset[CanonicalKey]
    kept: frozenset[CanonicalKey]
    tracked_only: frozenset[CanonicalKey]
    stray: frozenset[CanonicalKey]


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        [table],
    ).fetchone()
    return row is not None and row[0] > 0


def write_moves(conn: duckdb.DuckDBPyConnection, records: Sequence[MoveRecord], *, replace: bool = False) -> None:
    """Insert move records into `lake.ops_move_records`; with `replace`, the table holds exactly `records` after."""
    conn.execute(_CREATE)
    if replace:
        conn.execute(f"DELETE FROM lake.main.{MOVES_TABLE}")  # noqa: S608 - constant table name
    for rec in records:
        conn.execute(
            _INSERT,
            [
                rec.match_key,
                rec.direction,
                str(rec.old_key),
                str(rec.new_key) if rec.new_key else None,
                rec.at,
                rec.run_id,
            ],
        )


def record_move(cat: WorkingCatalog, rec: MoveRecord) -> None:
    """Write one move record; call it inside the move's catalog commit."""
    write_moves(cat.conn, [rec])


def read_moves(conn: duckdb.DuckDBPyConnection) -> list[MoveRecord]:
    """Every move record of catalog `lake`, oldest first."""
    if not _table_exists(conn, MOVES_TABLE):
        return []
    rows = conn.execute(
        f"SELECT match_key, direction, old_key, new_key, moved_at, run_id FROM lake.main.{MOVES_TABLE} "  # noqa: S608
        "ORDER BY moved_at, old_key"
    ).fetchall()
    records: list[MoveRecord] = []
    for match_key, direction, old_key, new_key, at, run_id in rows:
        if direction not in ("to_private", "to_public"):
            raise ValueError(f"unknown move direction {direction!r}")
        records.append(
            MoveRecord(
                match_key=int(match_key),
                direction="to_private" if direction == "to_private" else "to_public",
                old_key=CanonicalKey.parse(str(old_key)),
                new_key=CanonicalKey.parse(str(new_key)) if new_key is not None else None,
                at=at,
                run_id=str(run_id),
            )
        )
    return records


def bronze_tables(conn: duckdb.DuckDBPyConnection) -> list[BronzeTable]:
    rows = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND comment = ? "
        "ORDER BY 1",
        [BRONZE_COMMENT],
    ).fetchall()
    return [BronzeTable(str(row[0])) for row in rows]


def match_of(key: CanonicalKey) -> int | None:
    """The match key in a bronze file name, or `None` for any other object."""
    if not key.path.startswith("data/bronze/"):
        return None
    try:
        table_of(key)
        return parse_file_name(PurePosixPath(key.path).name).match_key
    except ValueError:
        return None


def _keys(paths: Iterable[object], roots: RootMap) -> set[CanonicalKey]:
    return {canonical_key(str(path), roots=roots, relative_to_data_path=False) for path in paths}


def registered_keys(cat: WorkingCatalog) -> set[CanonicalKey]:
    """Every file that the head snapshot of the working catalog registers in a bronze table."""
    found: set[CanonicalKey] = set()
    for table in bronze_tables(cat.conn):
        rows = cat.conn.execute("SELECT data_file FROM ducklake_list_files('lake', ?)", [table]).fetchall()
        found |= _keys((row[0] for row in rows), cat.storage.roots)
    return found


def _ducklake_tracked(cat: WorkingCatalog) -> set[CanonicalKey]:
    """Every absolute path in `ducklake_data_file` (current or ended rows) and in the scheduled-deletion list.
    Relative paths are DuckLake-written files under `data/main/`, never bronze."""
    rows = cat.conn.execute(
        "SELECT path FROM __ducklake_metadata_lake.ducklake_data_file WHERE NOT path_is_relative "
        "UNION SELECT path FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion "
        "WHERE NOT path_is_relative"
    ).fetchall()
    return _keys((row[0] for row in rows), cat.storage.roots)


def match_lake_objects(cat: WorkingCatalog, storage: Storage, match_key: int) -> MatchObjects:
    """The match's lake-root bronze objects: registered, kept (named by a kept manifest), tracked only by DuckLake,
    and stray (only in storage). A kept version is every version still in storage (ledger ruling, Task 11)."""

    def ours(key: CanonicalKey) -> bool:
        return key.root == "lake" and match_of(key) == match_key

    registered = {key for key in registered_keys(cat) if ours(key)}
    tables = set(bronze_tables(cat.conn))
    named: set[CanonicalKey] = set()
    for seq in listed_sequences(storage):
        try:
            manifest = read_manifest(storage, seq)
        except FileNotFoundError:
            continue  # a version object without a manifest: a crashed commit, never a restore point
        for key in manifest.canonical_keys:
            if ours(key):
                named.add(key)
                tables.add(table_of(key))
    kept = named - registered
    tracked_only = {key for key in _ducklake_tracked(cat) if ours(key)} - registered - kept
    listed: set[CanonicalKey] = set()
    for table in sorted(tables):
        for info in storage.list("lake", f"data/bronze/{table}/{match_key}-"):
            key = CanonicalKey("lake", info.key)
            if ours(key):
                listed.add(key)
    stray = listed - registered - kept - tracked_only
    return MatchObjects(
        registered=frozenset(registered),
        kept=frozenset(kept),
        tracked_only=frozenset(tracked_only),
        stray=frozenset(stray),
    )


def copy_with_new_write_id(storage: Storage, key: CanonicalKey, to_root: Root) -> CanonicalKey:
    """Copy a bronze object to `to_root` under the same table and content version and a new write id (§29.10)."""
    name = parse_file_name(PurePosixPath(key.path).name)
    folder = PurePosixPath(key.path).parent
    new_path = f"{folder}/{name.match_key}-{name.content_version}-{uuid.uuid4().hex}.parquet"
    storage.put(to_root, new_path, storage.get(key.root, key.path))
    return CanonicalKey(to_root, new_path)


def _raw_records(
    cat: WorkingCatalog, storage: Storage, match_key: int, archive: RawArchive, at: datetime
) -> list[MoveRecord]:
    """A `to_private` record, with a restricted copy, for each lake-root raw file of the match's pining deliveries."""
    pining = {r.content_sha256 for r in read_lineage(cat.conn) if r.match_key == match_key and r.channel == "pining"}
    records: list[MoveRecord] = []
    for content in sorted(pining):
        old = CanonicalKey("lake", raw_key(content))
        if not storage.exists(old.root, old.path):
            continue
        new = archive.copy_to(content, "restricted")
        if new is not None:
            records.append(MoveRecord(match_key, "to_private", old, new, at, cat.run_id))
    return records


def raw_content(key: CanonicalKey) -> str | None:
    """The content hash of a raw-zone key `raw/<content_sha256>.zst`, or `None` for any other key."""
    name = key.path.removeprefix(RAW_PREFIX)
    if name == key.path or not name.endswith(".zst"):
        return None
    return name.removesuffix(".zst")


def _delete_after_move(storage: Storage, records: Sequence[MoveRecord], public: frozenset[str]) -> None:
    for rec in records:
        content = raw_content(rec.old_key)
        if content is None:
            storage.delete("lake", rec.old_key.path, missing_ok=True)
        elif content not in public:
            try:
                storage.delete("lake", rec.old_key.path, missing_ok=True)
            except Exception:  # noqa: BLE001 - the record stays; the next run start deletes the object (§10.8)
                _log.warning("raw delete failed", exc_info=True, extra={"key": str(rec.old_key)})


@dataclass(frozen=True)
class StagedMove:
    """A match's move, copied and swapped in the working catalog but not yet published (T3 spec D2, D3)."""

    match_key: int
    to_root: Root
    records: tuple[MoveRecord, ...]
    public: frozenset[str]


def stage_match_move(
    cat: WorkingCatalog, storage: Storage, match_key: int, to_root: Root, *, archive: RawArchive | None = None
) -> StagedMove | None:
    """Copy a match's files to `to_root` (§9) and swap the catalog to the copies with the move records, without a
    publish. With an enabled `archive`, a private move also moves the match's pining raw files (§29.12). `None`: the
    match has nothing to move."""
    at = datetime.now(UTC)
    records: list[MoveRecord] = []
    new_by_table: dict[BronzeTable, list[CanonicalKey]] = {}
    public: frozenset[str] = frozenset()
    if to_root == "restricted":
        objects = match_lake_objects(cat, storage, match_key)
        for key in sorted(objects.registered | objects.kept, key=str):
            new_key = copy_with_new_write_id(storage, key, "restricted")
            records.append(MoveRecord(match_key, "to_private", key, new_key, at, cat.run_id))
            if key in objects.registered:
                new_by_table.setdefault(table_of(new_key), []).append(new_key)
        for key in sorted(objects.tracked_only | objects.stray, key=str):
            records.append(MoveRecord(match_key, "to_private", key, None, at, cat.run_id))
        if archive is not None and archive.enabled:
            records.extend(_raw_records(cat, storage, match_key, archive, at))
            public = public_contents(read_lineage(cat.conn))
    else:
        current = sorted(
            (k for k in registered_keys(cat) if k.root == "restricted" and match_of(k) == match_key), key=str
        )
        for key in current:
            new_key = copy_with_new_write_id(storage, key, "lake")
            records.append(MoveRecord(match_key, "to_public", key, new_key, at, cat.run_id))
            new_by_table.setdefault(table_of(new_key), []).append(new_key)
    if not records:
        return None
    # the new keys copy files the tables already hold: one older than an added column re-registers (audit T9)
    swap_match_files(cat, match_key, new_by_table, also=lambda: write_moves(cat.conn, records), copies=True)
    return StagedMove(match_key=match_key, to_root=to_root, records=tuple(records), public=public)


def finish_match_move(storage: Storage, staged: StagedMove) -> None:
    """After the publish that carries a staged move: a private move deletes the old public copies (§29.7), so no
    published version names a deleted file and no public copy outlives the publish."""
    if staged.to_root == "restricted":
        _delete_after_move(storage, staged.records, staged.public)
    _log.info(
        "match moved", extra={"match_key": staged.match_key, "to_root": staged.to_root, "files": len(staged.records)}
    )


def move_match_files(
    cat: WorkingCatalog,
    storage: Storage,
    match_key: int,
    to_root: Root,
    *,
    publish: Callable[[], object],
    archive: RawArchive | None = None,
) -> list[MoveRecord]:
    """Stage a match's move, `publish` it, then finish it (§9). The run passes its checkpoint policy's barrier publish,
    so the move runs the integrity guard and the pruning (T3 spec D2). Returns the records; a match with nothing to
    move returns `[]` and publishes nothing."""
    staged = stage_match_move(cat, storage, match_key, to_root, archive=archive)
    if staged is None:
        return []
    publish()
    finish_match_move(storage, staged)
    return list(staged.records)


def assert_no_moved_key_registered(moves: Sequence[MoveRecord], current_manifest: frozenset[CanonicalKey]) -> None:
    """Raise `GuardStopError` when a `to_private` old key is named by the `CURRENT` manifest (§29.10)."""
    bad = sorted(str(r.old_key) for r in moves if r.direction == "to_private" and r.old_key in current_manifest)
    if bad:
        raise GuardStopError(f"moved keys are still registered in CURRENT: {', '.join(bad)}; nothing is deleted")


def delete_moved_leftovers(
    storage: Storage,
    moves: Sequence[MoveRecord],
    *,
    current_manifest: frozenset[CanonicalKey],
    public_contents: frozenset[str] = frozenset(),
) -> int:
    """At every run start: delete each lake-root object that a `to_private` record names, accepting "not found".
    A raw key whose content a public-channel delivery has is skipped: it is public at its source (§29.12). Returns
    the number of objects deleted."""
    assert_no_moved_key_registered(moves, current_manifest)
    deleted = 0
    for rec in moves:
        if rec.direction != "to_private" or rec.old_key.root != "lake":
            continue
        if raw_content(rec.old_key) in public_contents:
            continue
        if storage.exists("lake", rec.old_key.path):
            storage.delete("lake", rec.old_key.path, missing_ok=True)
            deleted += 1
    return deleted


def prune_settled_raw_records(
    conn: duckdb.DuckDBPyConnection, storage: Storage, *, public_contents: frozenset[str]
) -> int:
    """Prune each raw-file record whose lake-root object is gone, or is kept because a public-channel delivery has its
    content; call it after `delete_moved_leftovers` (§10.8). Returns the number pruned."""
    moves = read_moves(conn)
    settled = [
        rec
        for rec in moves
        if (content := raw_content(rec.old_key)) is not None
        and rec.direction == "to_private"
        and (content in public_contents or not storage.exists("lake", rec.old_key.path))
    ]
    if settled:
        write_moves(conn, [rec for rec in moves if rec not in settled], replace=True)
    return len(settled)
