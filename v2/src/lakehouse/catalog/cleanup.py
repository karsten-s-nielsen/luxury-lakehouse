"""Cleanup at the end of a run: snapshot expiry, version retention, DuckLake's guarded cleanup and the orphan sweep
(spec §10.3 steps 6 and 7, §10.7, §10.8, §29.10, §29.11).

Order, all fail-closed:

1. `assert_held`; no `to_private` old key may be registered in the `CURRENT` manifest (§29.10).
2. Expire DuckLake snapshots older than `snapshot_days`.
3. Plan the version pruning (run-end versions 7 days; in-run versions until a run-end version supersedes them and
   never longer than 7 days; dropped restore versions never; `CURRENT` always kept).
4. Build the kept-version file set from the manifests that stay, with moved keys mapped to their new keys.
5. Read DuckLake's scheduled-deletion list and partition it by the kept-version set: `to_delete` (scheduled files no
   kept version names) and `to_defer` (a merge artifact a kept restore point still names, spec D3.1 option b).
6. Delete the pruned versions, then each `to_delete` file (a "not found" is accepted for a key a `to_private` record
   names, else counted as already-deleted — the step-6 idempotency), and remove the collected rows from the scheduled
   list by `data_file_id`. The `to_defer` rows stay scheduled (deferred); no `cleanup_all => true` (it would take them
   too). A deferred file is deleted on a later run once no kept version names it.
7. Sweep `data/` of both roots: delete an object only when no kept manifest names it, DuckLake does not track it, and
   it is older than the run's `started_storage_time`. Every `CURRENT` key must appear in the listing, and more than
   `sweep_max_fraction` of the objects needs `allow_large_sweep`.
8. Prune move records that no kept version names and DuckLake no longer tracks (raw records excluded).

`sweep_now` is `lakehouse sweep --allow-large`: the same cleanup outside a run, under the lease, with the large-sweep
guard lifted, then a run-end version.
"""

from __future__ import annotations

import itertools
import os
import socket
import sys
import uuid
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, Protocol

import duckdb

from lakehouse.catalog.maintenance import maintain_tables
from lakehouse.catalog.manifest import Manifest, read_manifest
from lakehouse.catalog.moves import MoveRecord, assert_no_moved_key_registered, read_moves, write_moves
from lakehouse.catalog.publish import listed_sequences, manifest_key, version_key
from lakehouse.catalog.restore import read_restores
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import RetentionConfig, Settings
from lakehouse.errors import ConflictError, GuardStopError
from lakehouse.logging import get_logger
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import ROOTS, CanonicalKey, RootMap, canonical_key
from lakehouse.storage.lease import Lease, anchor_intervals, remove_anchors
from lakehouse.telemetry import step_span

__all__ = [
    "CleanupPreview",
    "CleanupReport",
    "SweepLease",
    "delete_versions",
    "kept_file_set",
    "plan_version_pruning",
    "preview_cleanup",
    "prune_move_records",
    "prune_versions",
    "run_cleanup",
    "sweep_now",
    "sweep_preview",
    "tracked_keys",
]

_BATCH: Final = 100
_META: Final = "__ducklake_metadata_lake"

_log = get_logger(__name__)


class SweepLease(Protocol):
    """What cleanup needs from the lease (Task 7's `Lease` gives it)."""

    @property
    def started_storage_time(self) -> datetime: ...

    def assert_held(self) -> None: ...


@dataclass(frozen=True)
class CleanupReport:
    expired_snapshots: int
    pruned_versions: list[int]
    ducklake_deleted: list[CanonicalKey]
    swept: list[CanonicalKey]
    moved_not_found: list[CanonicalKey]
    deferred: int = 0  # scheduled files a kept restore point still names; left scheduled, not deleted (spec D3.1)
    oldest_deferred: datetime | None = None  # the oldest deferred schedule_start; the stuck-deferral signal
    missing_treated_as_deleted: int = 0  # a to_delete file already gone that no move names: the step-6 idempotency
    missing_deferred: int = 0  # a to_defer file gone from storage: a broken restore point (spec T12-SPEC-28)
    exempted: int = 0  # doomed objects a surviving run anchor covered, not counted by the guard (T3 spec D5)
    anchors_removed: list[int] = field(default_factory=list[int])  # generations whose anchor the sweep removed


@dataclass(frozen=True)
class CleanupPreview:
    """What one cleanup would delete, from the same computation (`lakehouse sweep --dry-run`, Reviewer B T12-PLAN-02):
    the pruned versions (each a version and a manifest object), the scheduled files no kept version names (the
    kept-overlapping ones are deferred, not listed here, Reviewer B-20) and the sweep's doomed objects. `counted` is
    the doomed count the large-sweep guard weighs against `objects_listed`."""

    pruned_versions: list[int]
    ducklake_listed: list[CanonicalKey]  # the `to_delete` partition: scheduled files no kept version names
    doomed: list[CanonicalKey]
    exempted: int
    counted: int
    objects_listed: int
    max_fraction: float
    deferred: int = 0  # scheduled files a kept version names, deferred not deleted (spec D3.1, Reviewer B-20)

    def keys(self) -> list[CanonicalKey]:
        """Every object the cleanup would delete, sorted (the `--expect` list)."""
        versions = [
            CanonicalKey("lake", key) for seq in self.pruned_versions for key in (version_key(seq), manifest_key(seq))
        ]
        return sorted({*versions, *self.ducklake_listed, *self.doomed}, key=str)


@dataclass(frozen=True)
class _SweepResult:
    doomed: list[CanonicalKey]
    exempted: int
    anchors_removed: list[int]
    counted: int = 0
    objects_listed: int = 0
    missing_deferred_keys: list[CanonicalKey] = field(default_factory=list[CanonicalKey])


def _batches[T](items: Sequence[T]) -> Iterator[Sequence[T]]:
    for start in range(0, len(items), _BATCH):
        yield items[start : start + _BATCH]


def _manifests(storage: Storage) -> dict[int, Manifest | None]:
    found: dict[int, Manifest | None] = {}
    for seq in listed_sequences(storage):
        try:
            found[seq] = read_manifest(storage, seq)
        except FileNotFoundError:
            found[seq] = None  # a version object without a manifest: a crashed commit
    return found


def plan_version_pruning(
    storage: Storage,
    retention: RetentionConfig,
    *,
    dropped: frozenset[int],
    now: datetime,
    current_seq: int,
    grace: timedelta,
) -> list[int]:
    """The versions that version retention does not keep (§10.8), without deleting anything.

    The grace (T3 spec D4) protects readers: a version with a manifest goes only when its successor (the next
    version with a manifest) was published at least `grace` before `now`, so a reader that opened it a moment earlier
    keeps it. A version without a manifest is a crashed commit, never `CURRENT`, so it goes without the grace."""
    cutoff = now - timedelta(days=retention.version_days)
    manifests = _manifests(storage)
    published = sorted(seq for seq, m in manifests.items() if m is not None)
    successor_at = {
        seq: manifest.published_at
        for seq, nxt in itertools.pairwise(published)
        if (manifest := manifests[nxt]) is not None
    }
    run_ends = [seq for seq, m in manifests.items() if m is not None and m.kind == "run_end"]
    prune: list[int] = []
    for seq, manifest in sorted(manifests.items()):
        if seq == current_seq:
            continue
        if manifest is None:
            prune.append(seq)
            continue
        superseded = successor_at.get(seq)
        if superseded is None or superseded > now - grace:
            continue  # no successor yet, or a reader may still hold this version
        if seq in dropped:
            prune.append(seq)
        elif manifest.kind == "run_end":
            if manifest.published_at < cutoff:
                prune.append(seq)
        elif any(end > seq for end in run_ends) or manifest.published_at < cutoff:
            prune.append(seq)  # an in-run version: superseded by a run end, or capped at 7 days
    return prune


def delete_versions(storage: Storage, seqs: Sequence[int]) -> list[int]:
    """Delete the version objects and manifests of `seqs`; return the sequences now fully gone. A delete that a
    Windows reader blocks is postponed (§10.4): that sequence is not in the result, and the next pruning retries it."""
    gone: list[int] = []
    for seq in seqs:
        postponed = False
        for key in (version_key(seq), manifest_key(seq)):
            try:
                storage.delete("lake", key, missing_ok=True)
            except PermissionError:
                if sys.platform != "win32":
                    raise
                # a reader on Windows still holds this version open: the next pruning deletes it (§10.4)
                _log.warning("version delete postponed", extra={"seq": seq, "key": key})
                postponed = True
        if not postponed:
            gone.append(seq)
    return gone


def prune_versions(
    storage: Storage, retention: RetentionConfig, *, dropped: frozenset[int], now: datetime, grace: timedelta
) -> list[int]:
    """Delete the versions (and manifests) that version retention does not keep; return their sequences."""
    from lakehouse.catalog.publish import read_current

    pruned = plan_version_pruning(
        storage, retention, dropped=dropped, now=now, current_seq=read_current(storage).seq, grace=grace
    )
    delete_versions(storage, pruned)
    return pruned


def kept_file_set(
    storage: Storage, moves: Sequence[MoveRecord], *, excluding: frozenset[int] = frozenset()
) -> frozenset[CanonicalKey]:
    """The union of the kept manifests (every version in storage except `excluding`), in which a `to_private` old
    key becomes its record's new key, or is removed when the record has no new key (§10.3 step 7, §29.10)."""
    mapping = {r.old_key: r.new_key for r in moves if r.direction == "to_private"}
    kept: set[CanonicalKey] = set()
    for seq, manifest in _manifests(storage).items():
        if manifest is None or seq in excluding:
            continue
        for key in manifest.canonical_keys:
            if key in mapping:
                new_key = mapping[key]
                if new_key is not None:
                    kept.add(new_key)
            else:
                kept.add(key)
    return frozenset(kept)


def _tracked_refs(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Every path DuckLake still tracks in catalog `lake`: `ducklake_data_file` (current and ended rows) and the
    scheduled-deletion list, with relative paths resolved under `data_path`, schema path and table path."""
    data_path_row = conn.execute(
        f"SELECT value FROM {_META}.ducklake_metadata WHERE key = 'data_path' AND scope IS NULL"  # noqa: S608
    ).fetchone()
    data_path = str(data_path_row[0]) if data_path_row is not None else ""
    rows = conn.execute(
        f"""
        WITH tables AS (
            SELECT table_id, schema_id, path, path_is_relative FROM {_META}.ducklake_table
            QUALIFY row_number() OVER (PARTITION BY table_id ORDER BY begin_snapshot DESC) = 1
        ), schemas AS (
            SELECT schema_id, path, path_is_relative FROM {_META}.ducklake_schema
            QUALIFY row_number() OVER (PARTITION BY schema_id ORDER BY begin_snapshot DESC) = 1
        )
        SELECT f.path, f.path_is_relative, t.path, t.path_is_relative, s.path, s.path_is_relative
        FROM {_META}.ducklake_data_file f
        LEFT JOIN tables t ON f.table_id = t.table_id
        LEFT JOIN schemas s ON t.schema_id = s.schema_id
        """  # noqa: S608 - constant metadata names
    ).fetchall()
    refs: list[str] = []
    for path, relative, table_path, table_relative, schema_path, schema_relative in rows:
        if not relative:
            refs.append(str(path))
            continue
        prefix = str(table_path or "")
        if table_relative:
            schema = str(schema_path or "")
            prefix = (data_path + schema if schema_relative else schema) + prefix
        refs.append(prefix + str(path))
    scheduled = conn.execute(
        f"SELECT path, path_is_relative FROM {_META}.ducklake_files_scheduled_for_deletion"  # noqa: S608
    ).fetchall()
    refs.extend(str(path) if not relative else data_path + str(path) for path, relative in scheduled)
    return refs


def tracked_keys(conn: duckdb.DuckDBPyConnection, roots: RootMap) -> frozenset[CanonicalKey]:
    """The canonical keys of everything DuckLake still tracks (§10.3 step 7, §29.11)."""
    return frozenset(canonical_key(ref, roots=roots, relative_to_data_path=False) for ref in _tracked_refs(conn))


def _scheduled_for_deletion(cat: WorkingCatalog) -> list[tuple[int, CanonicalKey, datetime]]:
    """The DuckLake scheduled-deletion rows as (data_file_id, canonical key, schedule_start). After a snapshot expiry
    this set equals `_ducklake_dry_run` (round-4 option-(b) probe A). Step 6 partitions it by the kept-version set and
    removes a collected row by `data_file_id` (spec D3.1 option b)."""
    conn = cat.conn
    data_path_row = conn.execute(
        f"SELECT value FROM {_META}.ducklake_metadata WHERE key = 'data_path' AND scope IS NULL"  # noqa: S608
    ).fetchone()
    data_path = str(data_path_row[0]) if data_path_row is not None else ""
    rows = conn.execute(
        f"SELECT data_file_id, path, path_is_relative, schedule_start "  # noqa: S608
        f"FROM {_META}.ducklake_files_scheduled_for_deletion"
    ).fetchall()
    out: list[tuple[int, CanonicalKey, datetime]] = []
    for data_file_id, path, relative, schedule_start in rows:
        ref = (data_path + str(path)) if relative else str(path)
        key = canonical_key(ref, roots=cat.storage.roots, relative_to_data_path=False)
        out.append((int(data_file_id), key, schedule_start))
    return out


def _listing_key(storage: Storage, root: str, key: str) -> CanonicalKey:
    """A storage listing maps through the same function as every reference (§10.3.1)."""
    uri = storage.read_uri("lake" if root == "lake" else "restricted", key)
    ref = uri if uri.startswith("hf://") else Path(uri).as_posix()
    return canonical_key(ref, roots=storage.roots, relative_to_data_path=False)


def _sweep(
    cat: WorkingCatalog,
    storage: Storage,
    *,
    kept: frozenset[CanonicalKey],
    current_keys: frozenset[CanonicalKey],
    lease: SweepLease,
    allow_large_sweep: bool,
    max_fraction: float,
    deferred: frozenset[CanonicalKey] = frozenset(),
    delete: bool = True,
) -> _SweepResult:
    """§10.3 step 7 with the T3 run anchors (spec D5): the guard does not count a doomed object inside a surviving
    anchor's interval; after the last delete, the anchors whose whole interval this sweep examined are removed.
    `delete=False` computes the same result and deletes nothing (the preview, T12). `deferred` are the step-6 deferred
    keys; a deferred key missing from this one listing is a broken restore point (`missing_deferred`, spec T12-SPEC-28).
    Reusing the single step-7 listing never adds a probe and never enters the `sweep_max_fraction` count."""
    lease.assert_held()
    listing: list[tuple[CanonicalKey, datetime]] = []
    for root in ROOTS:
        for info in storage.list(root, "data/"):
            listing.append((_listing_key(storage, root, info.key), info.modified))
    listed = {key for key, _ in listing}
    missing_deferred_keys = sorted((key for key in deferred if key not in listed), key=str)
    unlisted = sorted(str(key) for key in current_keys if key not in listed)
    if unlisted:
        raise GuardStopError(
            f"keys of the CURRENT manifest are missing from the storage listing ({', '.join(unlisted[:5])}): the key "
            "mapping or the listing is wrong; the sweep deletes nothing"
        )
    tracked = tracked_keys(cat.conn, storage.roots)
    started = lease.started_storage_time
    doomed_at = sorted(
        ((key, modified) for key, modified in listing if key not in kept and key not in tracked and modified < started),
        key=lambda item: str(item[0]),
    )
    doomed = [key for key, _ in doomed_at]
    named = sorted(str(key) for key in doomed if key in current_keys)
    if named:  # a kept-set defect: never delete a file the published version names (spec T3-SPEC-21)
        raise GuardStopError(
            f"the sweep would delete files that the CURRENT manifest names ({', '.join(named[:5])}): the kept set is "
            "wrong; the sweep deletes nothing"
        )
    intervals = anchor_intervals(storage)
    exempted = sum(1 for _, modified in doomed_at if any(i.covers(modified) for i in intervals))
    counted = len(doomed) - exempted
    if not delete:
        return _SweepResult(
            doomed=doomed,
            exempted=exempted,
            anchors_removed=[],
            counted=counted,
            objects_listed=len(listing),
            missing_deferred_keys=missing_deferred_keys,
        )
    if counted and counted > max_fraction * len(listing) and not allow_large_sweep:
        raise GuardStopError(
            f"the sweep would delete {counted} of {len(listing)} objects under data/ that no run anchor covers, "
            f"more than {max_fraction:.0%}; check the list and repeat with the explicit flag"
        )
    for batch in _batches(doomed):
        lease.assert_held()
        for key in batch:
            storage.delete(key.root, key.path, missing_ok=True)
    examined = [interval.generation for interval in intervals if interval.examined_before(started)]
    remove_anchors(storage, examined)  # only after the last delete returned: a sweep that raised removes nothing
    return _SweepResult(
        doomed=doomed,
        exempted=exempted,
        anchors_removed=examined,
        counted=counted,
        objects_listed=len(listing),
        missing_deferred_keys=missing_deferred_keys,
    )


def prune_move_records(conn: duckdb.DuckDBPyConnection, storage: Storage) -> int:
    """Prune each move record whose old key no kept version names and DuckLake no longer tracks at all (§10.8).
    Raw-file records are left to the run-start delete (Task 11). Returns the number pruned."""
    moves = read_moves(conn)
    if not moves:
        return 0
    named: set[CanonicalKey] = set()
    for manifest in _manifests(storage).values():
        if manifest is not None:
            named.update(manifest.canonical_keys)
    tracked = tracked_keys(conn, storage.roots)
    keep = [r for r in moves if r.old_key.path.startswith("raw/") or r.old_key in named or r.old_key in tracked]
    if len(keep) != len(moves):
        write_moves(conn, keep, replace=True)
    return len(moves) - len(keep)


def run_cleanup(
    cat: WorkingCatalog,
    storage: Storage,
    *,
    settings: Settings,
    lease: SweepLease,
    allow_large_sweep: bool,
    now: datetime,
    expected: frozenset[CanonicalKey] | None = None,
) -> CleanupReport:
    """§10.3 steps 6 and 7. The caller makes the final catalog commit (step 8). With `expected`, the cleanup first
    computes everything it would delete and stops with `GuardStopError` before any delete when a key is not in it
    (`lakehouse sweep --allow-large --expect`, Reviewer B T12-PLAN-05)."""
    if expected is not None:
        preview = preview_cleanup(cat, storage, settings=settings, lease=lease, now=now)
        unexpected = [str(key) for key in preview.keys() if key not in expected]
        if unexpected:
            raise GuardStopError(
                f"the cleanup would delete {len(unexpected)} objects outside the expected list "
                f"({', '.join(unexpected[:10])}); nothing is deleted"
            )
    lease.assert_held()
    moves = read_moves(cat.conn)
    current = cat.current
    current_keys: frozenset[CanonicalKey] = (
        frozenset(read_manifest(storage, current.seq).canonical_keys) if current is not None else frozenset()
    )
    assert_no_moved_key_registered(moves, current_keys)
    cutoff = now - timedelta(days=settings.retention.snapshot_days)
    expired = len(cat.conn.execute("CALL ducklake_expire_snapshots('lake', older_than => ?)", [cutoff]).fetchall())
    dropped = frozenset(seq for record in read_restores(cat.conn) for seq in record.dropped_seqs)
    grace = timedelta(minutes=settings.checkpoint.grace_minutes)
    pruned = (
        plan_version_pruning(
            storage, settings.retention, dropped=dropped, now=now, current_seq=current.seq, grace=grace
        )
        if current is not None
        else []
    )
    kept = kept_file_set(storage, moves, excluding=frozenset(pruned))
    scheduled = _scheduled_for_deletion(cat)
    to_delete = [(fid, key) for fid, key, _ in scheduled if key not in kept]
    to_defer = [(key, start) for _, key, start in scheduled if key in kept]
    deferred_keys = frozenset(key for key, _ in to_defer)
    delete_versions(storage, pruned)
    moved = {r.old_key for r in moves if r.direction == "to_private"}
    deleted: list[CanonicalKey] = []
    not_found: list[CanonicalKey] = []
    missing_treated_as_deleted = 0
    collected_ids: list[int] = []
    for fid, key in to_delete:
        lease.assert_held()
        if storage.exists(key.root, key.path):
            storage.delete(key.root, key.path)
            deleted.append(key)
        elif key in moved:
            not_found.append(key)  # deleted by its move (§29.7): the one accepted "not found"
        else:
            missing_treated_as_deleted += 1  # idempotent: a crash-partial step 6 already deleted it (spec Crash-safety)
        collected_ids.append(fid)
    # Remove the scheduled rows for everything collected, by data_file_id. The to_defer rows stay scheduled (deferred),
    # so they stay tracked and the sweep never treats them as orphans; no cleanup_all => true (it would take them too).
    for batch in _batches(collected_ids):
        placeholders = ", ".join("?" for _ in batch)
        cat.conn.execute(
            f"DELETE FROM {_META}.ducklake_files_scheduled_for_deletion WHERE data_file_id IN ({placeholders})",  # noqa: S608
            list(batch),
        )
    sweep = (
        _sweep(
            cat,
            storage,
            kept=kept,
            current_keys=current_keys,
            lease=lease,
            allow_large_sweep=allow_large_sweep,
            max_fraction=settings.sweep_max_fraction,
            deferred=deferred_keys,
        )
        if current is not None
        else _SweepResult(doomed=[], exempted=0, anchors_removed=[])
    )
    swept = sweep.doomed
    pruned_records = prune_move_records(cat.conn, storage)
    oldest_deferred = min((start for _, start in to_defer), default=None)
    _log.info(
        "cleanup",
        extra={
            "expired_snapshots": expired,
            "pruned_versions": len(pruned),
            "ducklake_deleted": len(deleted),
            "deferred": len(to_defer),
            "oldest_deferred": oldest_deferred.isoformat() if oldest_deferred is not None else None,
            "missing_treated_as_deleted": missing_treated_as_deleted,
            "missing_deferred": len(sweep.missing_deferred_keys),
            "missing_deferred_sample": [str(key) for key in sweep.missing_deferred_keys[:5]],
            "swept": len(swept),
            "exempted": sweep.exempted,
            "anchors_removed": sweep.anchors_removed,
            "moved_not_found": len(not_found),
            "pruned_move_records": pruned_records,
        },
    )
    return CleanupReport(
        expired_snapshots=expired,
        pruned_versions=pruned,
        ducklake_deleted=deleted,
        swept=swept,
        deferred=len(to_defer),
        oldest_deferred=oldest_deferred,
        missing_treated_as_deleted=missing_treated_as_deleted,
        missing_deferred=len(sweep.missing_deferred_keys),
        exempted=sweep.exempted,
        anchors_removed=sweep.anchors_removed,
        moved_not_found=not_found,
    )


def preview_cleanup(
    cat: WorkingCatalog, storage: Storage, *, settings: Settings, lease: SweepLease, now: datetime
) -> CleanupPreview:
    """What `run_cleanup` would delete, with its own guards and computation, deleting nothing (T12 Task 5). It expires
    snapshots in the working copy like the cleanup; the caller publishes nothing after a preview."""
    lease.assert_held()
    moves = read_moves(cat.conn)
    current = cat.current
    current_keys: frozenset[CanonicalKey] = (
        frozenset(read_manifest(storage, current.seq).canonical_keys) if current is not None else frozenset()
    )
    assert_no_moved_key_registered(moves, current_keys)
    cutoff = now - timedelta(days=settings.retention.snapshot_days)
    cat.conn.execute("CALL ducklake_expire_snapshots('lake', older_than => ?)", [cutoff]).fetchall()
    dropped = frozenset(seq for record in read_restores(cat.conn) for seq in record.dropped_seqs)
    grace = timedelta(minutes=settings.checkpoint.grace_minutes)
    pruned = (
        plan_version_pruning(
            storage, settings.retention, dropped=dropped, now=now, current_seq=current.seq, grace=grace
        )
        if current is not None
        else []
    )
    kept = kept_file_set(storage, moves, excluding=frozenset(pruned))
    scheduled = _scheduled_for_deletion(cat)
    to_delete = sorted((key for _, key, _ in scheduled if key not in kept), key=str)
    deferred_keys = frozenset(key for _, key, _ in scheduled if key in kept)
    sweep = (
        _sweep(
            cat,
            storage,
            kept=kept,
            current_keys=current_keys,
            lease=lease,
            allow_large_sweep=True,
            max_fraction=settings.sweep_max_fraction,
            deferred=deferred_keys,
            delete=False,
        )
        if current is not None
        else _SweepResult(doomed=[], exempted=0, anchors_removed=[])
    )
    return CleanupPreview(
        pruned_versions=list(pruned),
        ducklake_listed=to_delete,
        doomed=sweep.doomed,
        exempted=sweep.exempted,
        counted=sweep.counted,
        objects_listed=sweep.objects_listed,
        max_fraction=settings.sweep_max_fraction,
        deferred=len(deferred_keys),
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


@contextmanager
def _held_catalog(
    storage: Storage,
    settings: Settings,
    *,
    env: Mapping[str, str] | None,
    work_dir: Path | None,
    clock: Callable[[], datetime],
) -> Generator[tuple[WorkingCatalog, Lease, str]]:
    """The lease and a working catalog for a maintenance command; a run that holds the lease stops it first."""
    run_id = f"sweep-{uuid.uuid4().hex[:12]}"
    lease = Lease.acquire(storage, holder_id=f"sweep@{socket.gethostname()}:{os.getpid()}", run_id=run_id)
    if lease is None:
        raise ConflictError("sweep needs the lease, and a run holds it: try again after that run")
    try:
        cat = WorkingCatalog.open(storage, settings, lease, work_dir=work_dir, env=env, clock=clock)
        try:
            yield cat, lease, run_id
        finally:
            cat.close()
    finally:
        lease.release()


def sweep_now(
    storage: Storage,
    *,
    settings: Settings,
    env: Mapping[str, str] | None = None,
    work_dir: Path | None = None,
    clock: Callable[[], datetime] = _utc_now,
    expected: frozenset[CanonicalKey] | None = None,
) -> CleanupReport:
    """`lakehouse sweep --allow-large` (§10.3 step 7): the run-end maintenance without a budget (T12 spec D3), then one
    cleanup with the large-sweep guard lifted, under the lease, then a run-end version. Takes and releases the lease; a
    run that holds it stops the sweep before any change. `expected` (`--expect`) refuses a key outside the list before
    any delete (Reviewer B T12-PLAN-05)."""
    with _held_catalog(storage, settings, env=env, work_dir=work_dir, clock=clock) as (cat, lease, run_id):
        with step_span("lakehouse.sweep", {"lakehouse.run_id": run_id}, log=_log) as step:
            maintain_tables(cat, budget_s=None)
            report = run_cleanup(
                cat, storage, settings=settings, lease=lease, allow_large_sweep=True, now=clock(), expected=expected
            )
            step.set("lakehouse.catalog_seq", cat.commit("run_end").seq)
            return report


def sweep_preview(
    storage: Storage,
    *,
    settings: Settings,
    env: Mapping[str, str] | None = None,
    work_dir: Path | None = None,
    clock: Callable[[], datetime] = _utc_now,
) -> CleanupPreview:
    """`lakehouse sweep --dry-run` (Reviewer B T12-PLAN-02): what `sweep_now` would delete, from the same computation,
    under the lease; no maintenance, no delete and no publish (the working copy is discarded)."""
    with _held_catalog(storage, settings, env=env, work_dir=work_dir, clock=clock) as (cat, lease, _):
        return preview_cleanup(cat, storage, settings=settings, lease=lease, now=clock())
