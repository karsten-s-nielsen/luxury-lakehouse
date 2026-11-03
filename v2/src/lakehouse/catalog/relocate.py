"""Lake relocation: rewrite the absolute paths of v2's own files to new storage roots (spec §10.5.1).

DuckLake has no path-rewrite call. The catalog file holds the DuckLake 1.1 metadata tables, so `relocate` edits a
copy of the working catalog as a plain DuckDB database, with no DuckLake attach open on it, in one transaction:
`ducklake_data_file.path`, `ducklake_delete_file.path`, the scheduled-deletion list and the stored `data_path`.
It refuses any DuckLake format other than 1.1, so an upgrade fails loudly until the mechanism is verified again.
"""

from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import duckdb

from lakehouse.bronze.writer import reader_uri
from lakehouse.catalog.duck import writer_data_path
from lakehouse.catalog.publish import check_roots, read_current, roots_record
from lakehouse.catalog.working import VersionInfo, WorkingCatalog
from lakehouse.config.model import Settings, load_defaults
from lakehouse.errors import ConflictError, GuardStopError
from lakehouse.logging import get_logger
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import ROOTS, RootMap, StorageRootInfo, canonical_key
from lakehouse.storage.lease import Lease, anchor_intervals, remove_anchors
from lakehouse.storage.roots import normalise_root
from lakehouse.telemetry import step_span

__all__ = [
    "SUPPORTED_DUCKLAKE_FORMAT",
    "check_format",
    "relocate",
    "rewrite_root_paths",
    "roots_from_record",
]

SUPPORTED_DUCKLAKE_FORMAT: str = "1.1"
_PATH_TABLES: Final = ("ducklake_data_file", "ducklake_delete_file", "ducklake_files_scheduled_for_deletion")

_log = get_logger(__name__)


def roots_from_record(record: Mapping[str, str]) -> RootMap:
    """The `RootMap` of roots recorded in `CURRENT` or a manifest (local paths or `hf://buckets/<ns>/<bucket>`)."""

    def info(value: str) -> StorageRootInfo:
        if value.startswith("hf://buckets/"):
            return StorageRootInfo(local_path=None, hf_bucket=value.removeprefix("hf://buckets/"), s3_bucket_path=None)
        return StorageRootInfo(local_path=Path(value), hf_bucket=None, s3_bucket_path=None)

    return RootMap(lake=info(record["lake"]), restricted=info(record["restricted"]))


def same_roots(left: RootMap, right: RootMap) -> bool:
    a, b = roots_record(left), roots_record(right)
    return all(normalise_root(a[root]) == normalise_root(b[root]) for root in ROOTS)


def check_format(conn: duckdb.DuckDBPyConnection) -> None:
    """Refuse a catalog file whose DuckLake format is not the verified one (§10.5.1, "Pinning")."""
    row = conn.execute("SELECT value FROM ducklake_metadata WHERE key = 'version' AND scope IS NULL").fetchone()
    found = row[0] if row is not None else None
    if found != SUPPORTED_DUCKLAKE_FORMAT:
        raise GuardStopError(
            f"the catalog has DuckLake format {found}; path rewrites are verified only for {SUPPORTED_DUCKLAKE_FORMAT}"
        )


def rewrite_root_paths(conn: duckdb.DuckDBPyConnection, *, old_roots: RootMap, storage: Storage) -> int:
    """Rewrite every absolute path from `old_roots` to the same key under `storage`'s roots, and the stored
    `data_path`, inside the caller's transaction. A path under neither old root raises `ValueError`."""
    rewritten = 0
    for table in _PATH_TABLES:
        rows = conn.execute(f"SELECT DISTINCT path FROM {table} WHERE NOT path_is_relative").fetchall()  # noqa: S608
        for (path,) in rows:
            key = canonical_key(str(path), roots=old_roots, relative_to_data_path=False)
            new_path = reader_uri(storage, key)
            if new_path != path:
                conn.execute(f"UPDATE {table} SET path = ? WHERE path = ?", [new_path, path])  # noqa: S608
                rewritten += 1
    conn.execute(
        "UPDATE ducklake_metadata SET value = ? WHERE key = 'data_path' AND scope IS NULL",
        [writer_data_path(storage.roots)],
    )
    return rewritten


def _rewrite_file(path: Path, *, old_roots: RootMap, storage: Storage) -> None:
    conn = duckdb.connect(str(path))
    try:
        check_format(conn)
        conn.execute("BEGIN")
        try:
            rewrite_root_paths(conn, old_roots=old_roots, storage=storage)
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def relocate(
    storage: Storage,
    *,
    old_roots: RootMap,
    new_roots: RootMap,
    settings: Settings | None = None,
    env: Mapping[str, str] | None = None,
    work_dir: Path | None = None,
) -> VersionInfo:
    """Publish a new version whose registered paths name `new_roots` (the roots `storage` is opened on)."""
    if not same_roots(storage.roots, new_roots):
        raise ValueError("open the storage on the new roots before relocating")
    run_id = f"relocate-{uuid.uuid4().hex[:12]}"
    lease = Lease.acquire(storage, holder_id=f"relocate@{socket.gethostname()}:{os.getpid()}", run_id=run_id)
    if lease is None:
        raise ConflictError("relocate needs the lease, and a run holds it: try again after that run")
    try:
        with step_span("lakehouse.relocate", {"lakehouse.run_id": run_id}, log=_log) as step:
            info = _relocate_held(storage, lease, old_roots=old_roots, settings=settings, env=env, work_dir=work_dir)
            step.set("lakehouse.catalog_seq", info.seq)
            return info
    finally:
        lease.release()


def _relocate_held(
    storage: Storage,
    lease: Lease,
    *,
    old_roots: RootMap,
    settings: Settings | None,
    env: Mapping[str, str] | None,
    work_dir: Path | None,
) -> VersionInfo:
    before = read_current(storage)
    check_roots(old_roots, before.storage_roots)
    if before.encrypted:
        # S1 does not rewrite an encrypted catalog's paths (the rewrite plain-opens the metadata file, which an
        # encrypted catalog refuses without the key) and HF cannot hold the lakehouse key (add_data_files rejects
        # an encrypted parquet; HF uses platform encryption). An hf:// destination is never allowed; a local move
        # needs the deferred key-aware rewrite. Refuse loudly (spec S1 Non-goals, plan T13).
        destination = "an hf:// root" if storage.roots.lake.hf_bucket is not None else "a new root"
        raise GuardStopError(
            f"an encrypted-local lake cannot relocate to {destination}: S1 does not relocate an encrypted lake "
            "(key-aware path rewrite and HF re-encryption are out of scope; rotation/re-key is deferred)"
        )
    cat = WorkingCatalog.open(
        storage,
        settings if settings is not None else load_defaults("laptop"),  # only the resource caps are read
        lease,
        work_dir=work_dir,
        env=env,
        verify_roots=False,
        prepare=lambda path: _rewrite_file(path, old_roots=old_roots, storage=storage),
    )
    try:
        # the files at the new roots were copied, so their storage times no longer show which generation stored them:
        # drop every earlier run anchor (all but this generation's own) before the publish (fail closed; T3 spec D5)
        others = [
            interval.generation for interval in anchor_intervals(storage) if interval.generation != lease.generation
        ]
        remove_anchors(storage, others)
        return cat.commit("run_end")
    finally:
        cat.close()
