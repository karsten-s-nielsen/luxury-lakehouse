"""Catalog readers (spec §10.4).

A reader reads `CURRENT`, then attaches that version read-only. It keeps one attachment per version and attaches
again only when `CURRENT` names a new version. Each session uses exactly one version, so a page never mixes two
versions; a session that is still open keeps its version after `CURRENT` moves on. DuckLake-written files resolve
under the reader's own root through `override_data_path`.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

import duckdb

from lakehouse.catalog.duck import (
    attach_or_config_error,
    connect_duckdb,
    encryption_options,
    reader_data_path,
    sql_text,
)
from lakehouse.catalog.publish import CurrentPointer, check_encryption, check_roots, read_current
from lakehouse.catalog.working import catalog_extensions
from lakehouse.config.model import ResourceCaps
from lakehouse.errors import IntegrityError
from lakehouse.storage.base import Storage
from lakehouse.storage.encryption import resolve_encryption_key

__all__ = ["CatalogReader"]


class _Attachment:
    def __init__(self, conn: duckdb.DuckDBPyConnection, pointer: CurrentPointer, temp: Path | None) -> None:
        self.conn = conn
        self.pointer = pointer
        self.temp = temp  # the private folder: an encrypted read's spill, a remote version's local copy
        self.users = 0
        self.retired = False

    def close(self) -> None:
        self.conn.close()
        if self.temp is not None:
            shutil.rmtree(self.temp, ignore_errors=True)


class CatalogReader:
    """Read-only sessions on the version that `CURRENT` names. Safe to share between threads."""

    def __init__(
        self, storage: Storage, *, env: Mapping[str, str] | None = None, caps: ResourceCaps | None = None
    ) -> None:
        self._storage = storage
        self._env = os.environ if env is None else env
        self._caps = caps if caps is not None else ResourceCaps()
        self._lock = threading.Lock()
        self._active: _Attachment | None = None
        self.attach_count = 0

    @contextmanager
    def session(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """A connection on which `lake` is one published version, for the length of the `with` block."""
        pointer = read_current(self._storage)
        check_roots(self._storage.roots, pointer.storage_roots)
        key = resolve_encryption_key(self._storage.roots, self._env)
        check_encryption(recorded=pointer.encrypted, key_present=key is not None)
        attachment = self._acquire(pointer)
        cursor = attachment.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            self._release(attachment)

    def _acquire(self, pointer: CurrentPointer) -> _Attachment:
        with self._lock:
            active = self._active
            if active is not None and (active.pointer.seq, active.pointer.tag) == (pointer.seq, pointer.tag):
                active.users += 1
                return active
            fresh = self._attach(pointer)
            if active is not None:
                active.retired = True
                if active.users == 0:
                    active.close()
            self._active = fresh
            fresh.users += 1
            return fresh

    def _release(self, attachment: _Attachment) -> None:
        with self._lock:
            attachment.users -= 1
            if attachment.retired and attachment.users == 0:
                attachment.close()

    def _attach(self, pointer: CurrentPointer) -> _Attachment:
        stored = self._storage.stat("lake", pointer.version_key)
        if stored.size != pointer.size:
            raise IntegrityError(f"version {pointer.seq} has size {stored.size}, but CURRENT names size {pointer.size}")
        key = resolve_encryption_key(self._storage.roots, self._env)
        uri = self._storage.read_uri("lake", pointer.version_key)
        remote = uri.startswith("hf://")
        # One private folder per attachment, removed with it. An encrypted read spills into it: DuckDB refuses
        # `temp_file_encryption` while another instance has temp files in the same folder, and the default `./.tmp`
        # is shared (audit T13). A remote version is attached from a local copy in it (`_local_copy`).
        temp = Path(tempfile.mkdtemp(prefix="lakehouse-reader-")) if key is not None or remote else None
        try:
            conn = connect_duckdb(
                env=self._env,
                caps=self._caps,
                extensions=catalog_extensions(self._storage),
                temp_directory=temp if key is not None else None,
            )
        except BaseException:
            if temp is not None:
                shutil.rmtree(temp, ignore_errors=True)
            raise
        attachment = _Attachment(conn, pointer, temp)
        try:
            for statement in self._storage.duckdb_secrets():
                conn.execute(statement)
            location = self._local_copy(pointer, temp) if remote and temp is not None else Path(uri).as_posix()
            data_path = reader_data_path(self._storage.roots)
            opts = ["READ_ONLY", f"DATA_PATH {sql_text(data_path)}", "OVERRIDE_DATA_PATH true"]
            opts.extend(encryption_options(key))
            attach_or_config_error(
                conn,
                f"ATTACH {sql_text('ducklake:' + location)} AS lake ({', '.join(opts)})",
                key_present=key is not None,
            )
            if key is not None:
                conn.execute("SET temp_file_encryption = true")  # encrypt read-path spill too (spec S1, plan T7)
        except BaseException:
            attachment.close()
            raise
        self.attach_count += 1
        return attachment

    def _local_copy(self, pointer: CurrentPointer, folder: Path) -> str:
        """The path of a local copy of a remote version. DuckDB 2.0.0 deadlocks a DuckLake scan of many data files
        when the catalog itself is read over hf:// (ADR-001 amendment 2026-11-24); with a local catalog the same scan
        completes. A version is create-only, so the copy is exact; its size and sha256 are checked against CURRENT."""
        data = self._storage.get("lake", pointer.version_key)
        digest = hashlib.sha256(data).hexdigest()
        if len(data) != pointer.size or digest != pointer.sha256:
            raise IntegrityError(
                f"version {pointer.seq}: the downloaded copy has size {len(data)} and sha256 {digest}, but CURRENT "
                f"names size {pointer.size} and sha256 {pointer.sha256}"
            )
        target = folder / PurePosixPath(pointer.version_key).name
        target.write_bytes(data)
        return target.as_posix()

    def close(self) -> None:
        with self._lock:
            if self._active is not None:
                self._active.retired = True
                if self._active.users == 0:
                    self._active.close()
                self._active = None
