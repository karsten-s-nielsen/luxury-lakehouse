"""The writer's private working copy of the catalog and its catalog commits (spec §10.3, §10.4, §10.6, §10.8).

A run always builds its working copy from the version that `CURRENT` names (or a new catalog on an empty lake),
never from a working file left on disk. A catalog commit runs, in this order: `assert_held`, detach, create the
version with `put_if_absent`, create the manifest with `put_if_absent`, verify the full SHA-256 of the uploaded
version, `assert_held` again, `publish_current`, re-attach.

The commit does not run `CHECKPOINT lake`. On a DuckLake attach, `CHECKPOINT` runs DuckLake maintenance (it merged
data files in a probe on duckdb 2.0.0), and that maintenance could delete files outside the guarded cleanup of
§10.3 step 7. `DETACH` flushes the catalog file, and the commit fails loudly if a WAL file remains.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Protocol

import duckdb

from lakehouse.catalog.compact import CompactionResult, compact_catalog_file
from lakehouse.catalog.duck import (
    attach_or_config_error,
    connect_duckdb,
    encryption_options,
    ensure_parquet_options,
    sql_text,
    writer_data_path,
)
from lakehouse.catalog.manifest import VersionKind, build_manifest, new_manifest, read_manifest
from lakehouse.catalog.publish import (
    CurrentPointer,
    check_encryption,
    check_roots,
    listed_sequences,
    manifest_key,
    publish_current,
    read_current,
    roots_record,
    version_key,
)
from lakehouse.config.model import Settings
from lakehouse.errors import IntegrityError, LeaseLostError, StorageLimitError
from lakehouse.logging import get_logger
from lakehouse.storage.base import Storage
from lakehouse.storage.encryption import resolve_encryption_key
from lakehouse.storage.keys import CanonicalKey

__all__ = ["WORKING_FILE", "HeldLease", "VersionInfo", "WorkingCatalog", "catalog_extensions"]

WORKING_FILE: Final = "working.ducklake"

_log = get_logger(__name__)


class HeldLease(Protocol):
    """What a working catalog needs from the lease (Task 7's `Lease` gives it)."""

    @property
    def generation(self) -> int: ...

    @property
    def run_id(self) -> str: ...

    def assert_held(self) -> None: ...


@dataclass(frozen=True)
class VersionInfo:
    seq: int
    kind: VersionKind
    key: CanonicalKey
    sha256: str
    size: int
    published_at: datetime
    compaction: CompactionResult | None = None  # the catalog-file rewrite before the put (T3 spec D7)


def catalog_extensions(storage: Storage) -> tuple[str, ...]:
    """The DuckDB extensions a catalog connection needs: `httpfs` too when the lake is on HF."""
    return ("ducklake", "httpfs") if storage.roots.lake.hf_bucket is not None else ("ducklake",)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class WorkingCatalog:
    """The attached working copy (`lake`). Create it only with `WorkingCatalog.open`."""

    def __init__(
        self,
        *,
        storage: Storage,
        lease: HeldLease,
        conn: duckdb.DuckDBPyConnection,
        current: CurrentPointer | None,
        next_seq: int,
        path: Path,
        owns_folder: bool,
        clock: Callable[[], datetime],
        encryption_key: str | None = None,
    ) -> None:
        self._storage = storage
        self._lease = lease
        self._conn = conn
        self._current = current
        self._next_seq = next_seq
        self._path = path
        self._owns_folder = owns_folder
        self._clock = clock
        self._encryption_key = encryption_key
        self._attached = False
        self._temp_configured = False
        self._closed = False

    @classmethod
    def open(
        cls,
        storage: Storage,
        settings: Settings,
        lease: HeldLease,
        *,
        work_dir: Path | None = None,
        env: Mapping[str, str] | None = None,
        base_seq: int | None = None,
        prepare: Callable[[Path], None] | None = None,
        verify_roots: bool = True,
        migrating: bool = False,
        clock: Callable[[], datetime] | None = None,
    ) -> WorkingCatalog:
        """Build the working copy from the version that `CURRENT` names, or a new catalog on an empty lake.

        Restore and relocate only: `base_seq` builds it from another version (checked against that version's
        manifest), `prepare` edits the plain catalog file before the DuckLake attach, and `verify_roots=False` skips
        the roots check against `CURRENT`. The next commit still replaces `CURRENT` by compare-and-swap. `clock`
        gives the manifests' `published_at` (default: the UTC wall clock; tests inject simulated days).
        """
        env_map = os.environ if env is None else env
        key = resolve_encryption_key(storage.roots, env_map)
        try:
            current: CurrentPointer | None = read_current(storage)
        except FileNotFoundError:
            current = None
        if current is not None and verify_roots:
            check_roots(storage.roots, current.storage_roots)
        recorded_encrypted: bool | None = current.encrypted if current is not None else None
        owns_folder = work_dir is None
        folder = Path(tempfile.mkdtemp(prefix="lakehouse-work-")) if work_dir is None else work_dir
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / WORKING_FILE
        for stale in (path, path.with_name(path.name + ".wal")):
            stale.unlink(missing_ok=True)  # never reuse a working copy left by an earlier run (§10.8)
        if base_seq is not None:
            base = read_manifest(storage, base_seq)
            recorded_encrypted = base.encrypted
            data = storage.get("lake", version_key(base_seq))
            if len(data) != base.version_size or _sha256(data) != base.version_sha256:
                raise IntegrityError(f"version {base_seq} does not match the size and SHA-256 in its manifest")
            path.write_bytes(data)
        elif current is not None:
            data = storage.get("lake", current.version_key)
            if len(data) != current.size or _sha256(data) != current.sha256:
                raise IntegrityError(f"version {current.seq} does not match the size and SHA-256 in CURRENT")
            path.write_bytes(data)
        if recorded_encrypted is not None:
            check_encryption(recorded=recorded_encrypted, key_present=key is not None, migrating=migrating)
        if prepare is not None:
            prepare(path)
        next_seq = max([*listed_sequences(storage), current.seq if current is not None else 0]) + 1
        conn = connect_duckdb(env=env_map, caps=settings.caps, extensions=catalog_extensions(storage))
        for statement in storage.duckdb_secrets():
            conn.execute(statement)
        catalog = cls(
            storage=storage,
            lease=lease,
            conn=conn,
            current=current,
            next_seq=next_seq,
            path=path,
            owns_folder=owns_folder,
            clock=clock if clock is not None else _utc_now,
            encryption_key=key,
        )
        catalog.attach()
        return catalog

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    @property
    def storage(self) -> Storage:
        return self._storage

    @property
    def encryption_key(self) -> str | None:
        """The lakehouse encryption key this catalog was opened with (S1), or `None` for an unencrypted lake."""
        return self._encryption_key

    @property
    def run_id(self) -> str:
        return self._lease.run_id

    @property
    def current(self) -> CurrentPointer | None:
        """The `CURRENT` that this writer last read or wrote."""
        return self._current

    @property
    def next_seq(self) -> int:
        return self._next_seq

    def attach(self) -> None:
        if self._attached:
            raise RuntimeError("the working catalog is already attached")
        opts: list[str] = []
        if not self._path.exists():
            opts.append(f"DATA_PATH {sql_text(writer_data_path(self._storage.roots))}")
        if self._encryption_key is not None:
            opts.append("DATA_INLINING_ROW_LIMIT 0")  # per-attach (not persisted); keep every bronze write a file
        opts.extend(encryption_options(self._encryption_key))
        suffix = f" ({', '.join(opts)})" if opts else ""
        attach_or_config_error(
            self._conn,
            f"ATTACH {sql_text('ducklake:' + self._path.as_posix())} AS lake{suffix}",
            key_present=self._encryption_key is not None,
        )
        ensure_parquet_options(self._conn)  # the §10.2 file format for files DuckLake writes itself (T3 spec D6)
        if self._encryption_key is not None and not self._temp_configured:
            # Encrypt DuckDB spill/temp files too; keep them in the cleaned work folder (spec S1, plan T7). Set this
            # once per connection: DuckDB rejects a temp_directory change after a spill has used it, and attach() runs
            # again on every commit's re-attach. A gradientsports-scale spill then wedged the next re-attach.
            self._conn.execute(f"SET temp_directory = {sql_text((self._path.parent / 'tmp').as_posix())}")
            self._conn.execute("SET temp_file_encryption = true")
            self._temp_configured = True
        self._attached = True

    def detach(self) -> Path:
        """Detach the working copy (for example for the dbt hand-off) and return its file."""
        if self._attached:
            self._conn.execute("DETACH lake")
            self._attached = False
        wal = self._path.with_name(self._path.name + ".wal")
        if wal.exists():
            raise IntegrityError(f"DETACH left {wal.name}: the catalog file is not complete")
        return self._path

    def _fence_existing(self, seq: int, key: str) -> None:
        """A version or manifest object already exists: stop the run, with the reason (§10.4, §10.6). A lost lease
        or a moved `CURRENT` means another writer published (`LeaseLostError`). An object that neither explains is an
        integrity event, never the benign "a run holds the lease" (`ConflictError`, CLI `skipped`); T6 spec D2."""
        self._lease.assert_held()
        try:
            now = read_current(self._storage)
        except FileNotFoundError:
            now = None
        before = self._current.tag if self._current is not None else None
        if (now.tag if now is not None else None) != before:
            raise LeaseLostError(f"CURRENT changed while this run worked: another writer published ({key} exists)")
        raise IntegrityError(
            f"version {seq} already exists ({key}) and neither a lost lease nor a moved CURRENT explains it"
        )

    def commit(self, kind: VersionKind) -> VersionInfo:
        """Publish the working copy as the next immutable version and point `CURRENT` at it."""
        self._lease.assert_held()
        keys = build_manifest(self._conn, self._storage.roots)
        path = self.detach()
        try:
            info = self._publish(path, kind, keys)
        except BaseException as exc:
            try:
                self.attach()  # the run records the error in the working copy, so it must stay attached (audit T13)
            except Exception as again:  # noqa: BLE001 - the publish error is the one to raise; this one rides on it as a note
                exc.add_note(f"the working copy could not be attached again: {type(again).__name__}: {again}")
            raise
        self.attach()
        _log.info("catalog commit", extra={"seq": info.seq, "kind": kind, "size": info.size, "files": len(keys)})
        return info

    def _publish(self, path: Path, kind: VersionKind, keys: list[CanonicalKey]) -> VersionInfo:
        """The detached copy at `path` as the next version, then `CURRENT` (see `commit`)."""
        # between the detach and the version put, under the lease: rewrite the file when it pays (T3 spec D7)
        compaction = compact_catalog_file(path, key=self._encryption_key)
        data = path.read_bytes()
        digest, size, seq = _sha256(data), len(data), self._next_seq
        limit = self._storage.single_part_limit
        if limit is not None and size > limit:  # the version upload is one conditional part (T6 spec D2)
            raise StorageLimitError(
                f"the catalog copy is {size} bytes, above the storage's single-part limit of {limit} bytes; it cannot "
                "be published (the catalog_growth warning fires earlier)"
            )
        key = version_key(seq)
        # the sequence is used from the put on, whatever happens next: a later publish of this run takes the next one
        # instead of meeting its own stored version at the fence (Reviewer B T3-IMPL-05); a gap is harmless (ADR-007)
        self._next_seq = seq + 1
        if not self._storage.put_if_absent("lake", key, path):
            self._fence_existing(seq, key)
        published_at = self._clock()
        manifest = new_manifest(
            seq=seq,
            kind=kind,
            run_id=self._lease.run_id,
            published_at=published_at,
            version_size=size,
            version_sha256=digest,
            storage_roots=roots_record(self._storage.roots),
            keys=keys,
            encrypted=self._encryption_key is not None,
        )
        if not self._storage.put_if_absent("lake", manifest_key(seq), manifest.to_bytes()):
            self._fence_existing(seq, manifest_key(seq))
        stored = self._storage.get("lake", key)
        if len(stored) != size or _sha256(stored) != digest:
            raise IntegrityError(f"the uploaded version {seq} does not match the working copy")
        self._lease.assert_held()
        pointer = CurrentPointer.build(
            seq=seq,
            version_key=key,
            size=size,
            sha256=digest,
            parent_seq=self._current.seq if self._current is not None else None,
            lease_generation=self._lease.generation,
            storage_roots=roots_record(self._storage.roots),
            encrypted=self._encryption_key is not None,
        )
        self._current = publish_current(self._storage, pointer, expected=self._current)
        return VersionInfo(
            seq=seq,
            kind=kind,
            key=CanonicalKey("lake", key),
            sha256=digest,
            size=size,
            published_at=published_at,
            compaction=compaction,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._attached:
                self._conn.execute("DETACH lake")
                self._attached = False
        finally:
            self._conn.close()
            if self._owns_folder:
                shutil.rmtree(self._path.parent)
