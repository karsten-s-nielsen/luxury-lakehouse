"""Catalog restore (spec §10.8, §29.10, §29.11, §29.12).

A restore points `CURRENT` at a copy of a kept version. It holds the lease. The copy keeps the restored version's
content but gives every match its latest visibility, so every restore point keeps working after visibility moves:

1. every lake-root file that a `to_private` record names is replaced by that record's restricted copy, whatever the
   match's latest visibility (a record with no new key stops the restore);
2. each match with move records is placed by its latest visibility: a private match must have only restricted
   files; for a public match each restricted file is copied to the lake root under a new write id;
3. each match with move records gets its latest visibility as its `ops_matches` flag.

The latest visibility is the direction of the match's newest move record in the pre-restore `CURRENT` (ledger
ruling, Task 11). The `ops_matches` flag of the pre-restore `CURRENT` must agree with it, else the restore stops
before it copies anything (Task 21). The copy carries every move record and every restore record of the
pre-restore `CURRENT`, plus its own. Before it publishes, it checks that every file the copy names exists.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal

import duckdb

from lakehouse.bronze.writer import reader_uri
from lakehouse.catalog.manifest import build_manifest, read_manifest
from lakehouse.catalog.moves import MoveRecord, copy_with_new_write_id, match_of, read_moves, write_moves
from lakehouse.catalog.publish import manifest_key, read_current, version_key
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.relocate import check_format, rewrite_root_paths, roots_from_record, same_roots
from lakehouse.catalog.visibility import reapply_visibility
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import Settings, load_defaults
from lakehouse.errors import ConflictError, GuardStopError, IntegrityError
from lakehouse.logging import get_logger
from lakehouse.runs.history import read_table, read_visibility_changes, set_match_visibility, write_visibility_changes
from lakehouse.storage.base import Storage
from lakehouse.storage.encryption import resolve_encryption_key
from lakehouse.storage.keys import CanonicalKey, canonical_key
from lakehouse.storage.lease import Lease, anchor_intervals, remove_anchors
from lakehouse.telemetry import step_span

__all__ = ["RESTORES_TABLE", "RestoreRecord", "read_restores", "restore"]

RESTORES_TABLE: Final = "ops_restores"
_CREATE: Final = (
    f"CREATE TABLE IF NOT EXISTS lake.main.{RESTORES_TABLE} (operator VARCHAR NOT NULL, "
    "restored_at TIMESTAMPTZ NOT NULL, "
    "restored_seq BIGINT NOT NULL, new_seq BIGINT NOT NULL, dropped_seqs BIGINT[] NOT NULL, "
    "visibility_fixes BIGINT NOT NULL)"
)

_log = get_logger(__name__)


@dataclass(frozen=True)
class RestoreRecord:
    operator: str
    at: datetime
    restored_seq: int
    new_seq: int
    dropped_seqs: list[int]
    visibility_fixes: int


def read_restores(conn: duckdb.DuckDBPyConnection) -> list[RestoreRecord]:
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        [RESTORES_TABLE],
    ).fetchone()
    if exists is None or not exists[0]:
        return []
    rows = conn.execute(
        "SELECT operator, restored_at, restored_seq, new_seq, dropped_seqs, visibility_fixes "  # noqa: S608 - constant name
        f"FROM lake.main.{RESTORES_TABLE} ORDER BY restored_at"
    ).fetchall()
    return [
        RestoreRecord(
            operator=str(op),
            at=at,
            restored_seq=int(rs),
            new_seq=int(ns),
            dropped_seqs=[int(s) for s in ds],
            visibility_fixes=int(vf),
        )
        for op, at, rs, ns, ds, vf in rows
    ]


def _write_restores(conn: duckdb.DuckDBPyConnection, records: list[RestoreRecord]) -> None:
    conn.execute(_CREATE)
    conn.execute(f"DELETE FROM lake.main.{RESTORES_TABLE}")  # noqa: S608 - constant table name
    for rec in records:
        conn.execute(
            f"INSERT INTO lake.main.{RESTORES_TABLE} VALUES (?, ?, ?, ?, ?, ?)",  # noqa: S608 - constant table name
            [rec.operator, rec.at, rec.restored_seq, rec.new_seq, rec.dropped_seqs, rec.visibility_fixes],
        )


def _latest_visibility(moves: list[MoveRecord]) -> dict[int, Literal["public", "private"]]:
    """Each moved match's latest visibility: the direction of its newest move record (records are in move order)."""
    latest: dict[int, Literal["public", "private"]] = {}
    for rec in moves:
        latest[rec.match_key] = "private" if rec.direction == "to_private" else "public"
    return latest


def _check_flags(flags: Mapping[int, str], latest: Mapping[int, Literal["public", "private"]]) -> None:
    """The pre-restore `ops_matches` flag of each moved match must be its latest visibility."""
    for match_key, visibility in sorted(latest.items()):
        flag = flags.get(match_key)
        if flag is not None and flag != visibility:
            direction = "to_private" if visibility == "private" else "to_public"
            raise GuardStopError(
                f"match {match_key}: the current catalog flags it {flag}, but its newest move record is {direction}; "
                "nothing is published"
            )


@dataclass
class _VisibilityFix:
    """The plain-DuckDB edits of the restored copy (passes 1 and 2), before the DuckLake attach."""

    storage: Storage
    moves: list[MoveRecord]
    target_roots: Mapping[str, str]
    run_id: str
    at: datetime
    new_records: list[MoveRecord] = field(default_factory=list[MoveRecord])
    fixes: int = 0

    def _current_rows(self, conn: duckdb.DuckDBPyConnection) -> list[tuple[str, CanonicalKey]]:
        rows = conn.execute(
            "SELECT DISTINCT path FROM ducklake_data_file WHERE end_snapshot IS NULL AND NOT path_is_relative"
        ).fetchall()
        return [
            (str(path), canonical_key(str(path), roots=self.storage.roots, relative_to_data_path=False))
            for (path,) in rows
        ]

    def _repoint(self, conn: duckdb.DuckDBPyConnection, old: CanonicalKey, new: CanonicalKey) -> None:
        for table in ("ducklake_data_file", "ducklake_files_scheduled_for_deletion"):
            conn.execute(
                f"UPDATE {table} SET path = ? WHERE path = ?",  # noqa: S608 - constant table names
                [reader_uri(self.storage, new), reader_uri(self.storage, old)],
            )
        self.fixes += 1

    def apply(self, path: Path) -> None:
        conn = duckdb.connect(str(path))
        try:
            check_format(conn)
            conn.execute("BEGIN")
            try:
                target = roots_from_record(self.target_roots)
                if not same_roots(target, self.storage.roots):
                    rewrite_root_paths(conn, old_roots=target, storage=self.storage)  # restore across a relocate
                self._resolve_moved_keys(conn)
                self._place_by_latest_visibility(conn)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    def _resolve_moved_keys(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Pass 1: a lake-root file that a `to_private` record names becomes that record's restricted copy."""
        current = {key for _, key in self._current_rows(conn)}
        for rec in self.moves:
            if rec.direction != "to_private" or rec.old_key not in current:
                continue
            if rec.new_key is None:
                raise GuardStopError(
                    f"the restored version names {rec.old_key}, whose move record has no restricted copy; "
                    "nothing is published"
                )
            self._repoint(conn, rec.old_key, rec.new_key)
            current.discard(rec.old_key)
            current.add(rec.new_key)

    def _place_by_latest_visibility(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Pass 2: each match with move records gets the files of its latest visibility."""
        latest = _latest_visibility(self.moves)
        for _, key in self._current_rows(conn):
            match_key = match_of(key)
            if match_key is None or match_key not in latest:
                continue
            if latest[match_key] == "private":
                if key.root != "restricted":
                    raise GuardStopError(f"match {match_key} is private now, but the copy names {key}")
            elif key.root == "restricted":
                new_key = copy_with_new_write_id(self.storage, key, "lake")
                self._repoint(conn, key, new_key)
                self.new_records.append(MoveRecord(match_key, "to_public", key, new_key, self.at, self.run_id))


def restore(
    storage: Storage,
    *,
    target_seq: int,
    operator: str,
    settings: Settings | None = None,
    env: Mapping[str, str] | None = None,
    work_dir: Path | None = None,
) -> RestoreRecord:
    """Point `CURRENT` at a copy of version `target_seq` (§10.8). Takes and releases the lease."""
    if not operator:
        raise ValueError("a restore needs the operator's name")
    run_id = f"restore-{uuid.uuid4().hex[:12]}"
    lease = Lease.acquire(storage, holder_id=f"restore@{socket.gethostname()}:{os.getpid()}", run_id=run_id)
    if lease is None:
        raise ConflictError("restore needs the lease, and a run holds it: try again after that run")
    try:
        with step_span("lakehouse.restore", {"lakehouse.run_id": run_id}, log=_log) as step:
            record = _restore_held(storage, lease, target_seq, operator, settings, env, work_dir)
            step.set("lakehouse.catalog_seq", record.new_seq)
            return record
    finally:
        lease.release()


def _restore_held(
    storage: Storage,
    lease: Lease,
    target_seq: int,
    operator: str,
    settings: Settings | None,
    env: Mapping[str, str] | None,
    work_dir: Path | None,
) -> RestoreRecord:
    before = read_current(storage)
    if target_seq == before.seq:
        raise ValueError(f"version {target_seq} is already current")
    target = read_manifest(storage, target_seq)
    if not storage.exists("lake", version_key(target_seq)):
        raise FileNotFoundError(f"version {target_seq} is not in storage")
    reader = CatalogReader(storage, env=env)
    try:
        with reader.session() as conn:
            moves = read_moves(conn)
            restores = read_restores(conn)
            ledger_changes = read_visibility_changes(conn)
            flags = {row["match_key"]: row["visibility"] for row in read_table(conn, "ops_matches")}
    finally:
        reader.close()
    ledger_latest: dict[int, Literal["public", "private"]] = {c.match_key: c.new_visibility for c in ledger_changes}
    latest = _latest_visibility(moves)
    _check_flags(flags, latest)
    at = datetime.now(UTC)
    fix = _VisibilityFix(
        storage=storage, moves=moves, target_roots=target.storage_roots, run_id=run_id_of(lease), at=at
    )
    # On an encrypted-local lake the metadata file is encrypted (no plain-DuckDB open) and `_VisibilityFix`'s
    # file passes are no-ops (there are no move records); the visibility is re-applied as the `_visibility` column
    # after the keyed DuckLake attach (below), so the plain-file `prepare` is skipped (plan T12).
    encrypted_local = resolve_encryption_key(storage.roots, os.environ if env is None else env) is not None
    cat = WorkingCatalog.open(
        storage,
        settings if settings is not None else load_defaults("laptop"),  # only the resource caps are read
        lease,
        work_dir=work_dir,
        env=env,
        base_seq=target_seq,
        prepare=None if encrypted_local else fix.apply,
    )
    try:
        new_seq = cat.next_seq
        flag_fixes = sum(
            set_match_visibility(cat.conn, match_key, visibility, run_id=run_id_of(lease), updated_at=at)
            for match_key, visibility in sorted(latest.items())
        )
        if cat.encryption_key is not None:  # encrypted-local: the restricted copy is the column, not a second root
            for match_key, visibility in sorted(ledger_latest.items()):
                reapply_visibility(cat.conn, match_key, visibility)  # set the restored bronze rows to the latest
                flag_fixes += set_match_visibility(
                    cat.conn, match_key, visibility, run_id=run_id_of(lease), updated_at=at
                )
            write_visibility_changes(cat.conn, ledger_changes, replace=True)  # carry the full flip history forward
        record = RestoreRecord(
            operator=operator,
            at=at,
            restored_seq=target_seq,
            new_seq=new_seq,
            dropped_seqs=list(range(target_seq + 1, new_seq)),
            visibility_fixes=fix.fixes + flag_fixes,
        )
        write_moves(cat.conn, [*moves, *fix.new_records], replace=True)
        _write_restores(cat.conn, [*restores, record])
        missing = [str(k) for k in build_manifest(cat.conn, storage.roots) if not storage.exists(k.root, k.path)]
        if missing:
            raise IntegrityError(f"the restored copy names files that are not in storage: {', '.join(missing)}")
        # before the publish: the undone runs' files count again at the next sweep, and a restore that fails after
        # this only makes the guard count more (fail closed; T3 spec D5)
        _remove_undone_anchors(storage, record.dropped_seqs)
        cat.commit("run_end")
    finally:
        cat.close()
    for seq in record.dropped_seqs:
        storage.delete("lake", version_key(seq), missing_ok=True)
        storage.delete("lake", manifest_key(seq), missing_ok=True)
    _log.info(
        "catalog restored",
        extra={
            "operator": operator,
            "restored_seq": target_seq,
            "new_seq": new_seq,
            "fixes": record.visibility_fixes,
        },
    )
    return record


def _remove_undone_anchors(storage: Storage, dropped: Sequence[int]) -> None:
    """Remove the run anchors of the runs whose versions this restore drops (the run ids of their manifests)."""
    undone: set[str] = set()
    for seq in dropped:
        try:
            undone.add(read_manifest(storage, seq).run_id)
        except FileNotFoundError:
            continue  # a version without a manifest is a crashed commit's copy: it names no run
    remove_anchors(
        storage, [interval.generation for interval in anchor_intervals(storage) if interval.run_id in undone]
    )


def run_id_of(lease: Lease) -> str:
    return lease.run_id
