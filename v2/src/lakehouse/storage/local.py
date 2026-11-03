"""Local storage: a lake folder and a restricted folder on a local or attached drive (spec §10.4, §10.5).

Every write goes to a new temp file in `<root>/.tmp/` first, so a stored object always has a fresh `mtime` and a
reader never sees a half-written object. `put` then renames the temp file over the key. `put_if_absent` hard-links
it to the key, which fails when the key exists; on a file system without hard links (exFAT, FAT32, many SMB shares)
it falls back to `O_CREAT | O_EXCL`. `replace_if_match` holds a `filelock.FileLock` in `<root>/.locks/` around the
compare and the rename. The tag is the SHA-256 of the content. Listings never show `.tmp/` or `.locks/`.
"""

from __future__ import annotations

import builtins
import hashlib
import os
import shutil
import stat
import sys
import tempfile
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final

from filelock import FileLock, Timeout

from lakehouse.errors import ConflictError, MissingRootError
from lakehouse.logging import get_logger
from lakehouse.storage.base import DbtSecret, ObjectInfo, validate_key, validate_prefix
from lakehouse.storage.keys import ROOTS, Root, RootMap

if TYPE_CHECKING:
    from lakehouse.storage.ledger import LeaseLedger

__all__ = ["LocalStorage"]

_TMP_DIR: Final = ".tmp"
_LOCK_DIR: Final = ".locks"
_INTERNAL_DIRS: Final = frozenset({_TMP_DIR, _LOCK_DIR})
_REPLACE_TRIES: Final = 10
_REPLACE_FIRST_WAIT_S: Final = 0.05
_REPLACE_MAX_WAIT_S: Final = 1.0
_LOCK_TIMEOUT_S: Final = 30.0
_COPY_CHUNK: Final = 1024 * 1024
# Windows refuses to rename over a file that another process holds open; a short retry rides that out (§10.4).
_RETRY_LOCKED_REPLACE: bool = sys.platform == "win32"

_log = get_logger(__name__)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fsync_dir(path: Path) -> None:
    """Make a rename or link in `path` durable. Windows has no directory handles for this."""
    if sys.platform == "win32":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _replace(src: Path, dst: Path) -> None:
    for attempt in range(1, _REPLACE_TRIES + 1):
        try:
            os.replace(src, dst)
        except PermissionError:
            if not _RETRY_LOCKED_REPLACE or attempt == _REPLACE_TRIES:
                _log.error("replace failed", extra={"target": str(dst), "attempts": attempt})
                raise
            time.sleep(min(_REPLACE_FIRST_WAIT_S * 2 ** (attempt - 1), _REPLACE_MAX_WAIT_S))
        else:
            return


class LocalStorage:
    """The `Storage` of two local folders. Both roots must exist; only `init_lake` creates them."""

    def __init__(self, roots: RootMap) -> None:
        for root in ROOTS:
            if roots.info(root).local_path is None:
                raise ValueError(f"LocalStorage needs a local path for the {root} root")
        self._roots = roots

    def __repr__(self) -> str:
        return f"LocalStorage(lake={self._base('lake')!s}, restricted={self._base('restricted')!s})"

    @property
    def roots(self) -> RootMap:
        return self._roots

    @property
    def single_part_limit(self) -> int | None:
        """No limit: a local conditional write is a file-system create or rename of any size."""
        return None

    def lease_ledger(self) -> LeaseLedger | None:
        """None: the file system's create-exclusive is atomic, so it fences the lease (T6 spec D1)."""
        return None

    def _base(self, root: Root) -> Path:
        path = self._roots.info(root).local_path
        if path is None:  # pragma: no cover - refused in __init__
            raise ValueError(f"no local path for the {root} root")
        return path

    def _write_target(self, root: Root, key: str) -> Path:
        """The path of `key` for a write. A missing root raises `MissingRootError`: a write never creates a root,
        so an unmounted drive never turns into an empty folder on another disk (spec §10.5, §23.6)."""
        target = self._path(root, key)
        base = self._base(root)
        if not base.is_dir():
            raise MissingRootError(f"the {root} root {base} does not exist: reconnect its drive")
        return target

    def _path(self, root: Root, key: str) -> Path:
        parts = validate_key(key).split("/")
        if parts[0] in _INTERNAL_DIRS:
            raise ValueError(f"invalid storage key {key!r}: {parts[0]}/ is internal to local storage")
        return self._base(root).joinpath(*parts)

    def _write_temp(self, root: Root, data: bytes | Path) -> Path:
        """Write `data` to a new temp file (new file, so a fresh `mtime`); fsync it; return its path."""
        tmp_dir = self._base(root) / _TMP_DIR
        tmp_dir.mkdir(exist_ok=True)
        fd, name = tempfile.mkstemp(dir=tmp_dir, prefix="put-")
        tmp = Path(name)
        try:
            with os.fdopen(fd, "wb") as out:
                if isinstance(data, bytes):
                    out.write(data)
                else:
                    with data.open("rb") as src:
                        shutil.copyfileobj(src, out, _COPY_CHUNK)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return tmp

    def put(self, root: Root, key: str, data: bytes | Path) -> None:
        target = self._write_target(root, key)
        tmp = self._write_temp(root, data)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            _replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        _fsync_dir(target.parent)

    def put_if_absent(self, root: Root, key: str, data: bytes | Path) -> bool:
        target = self._write_target(root, key)
        if target.exists():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._write_temp(root, data)
        try:
            try:
                os.link(tmp, target)
            except FileExistsError:
                return False
            except OSError:
                return self._create_exclusive(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        _fsync_dir(target.parent)
        return True

    def _create_exclusive(self, tmp: Path, target: Path) -> bool:
        """The fallback without hard links: `O_EXCL` gives exactly one creator of `target`.

        A crash during the copy can leave a partial object. A reader treats an unparsable lease generation as held
        until its `mtime` plus `LEASE_TTL` plus `TAKEOVER_GRACE` (Task 7), so a partial object never frees a lease.
        """
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        try:
            fd = os.open(target, flags, 0o644)
        except FileExistsError:
            return False
        try:
            with os.fdopen(fd, "wb") as out, tmp.open("rb") as src:
                shutil.copyfileobj(src, out, _COPY_CHUNK)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            # This process created the file with O_EXCL, so the partial copy is its own to remove.
            target.unlink(missing_ok=True)
            raise
        _fsync_dir(target.parent)
        return True

    def get(self, root: Root, key: str) -> bytes:
        return self._path(root, key).read_bytes()

    def get_versioned(self, root: Root, key: str) -> tuple[bytes, str]:
        data = self.get(root, key)
        return data, _sha256(data)

    def replace_if_match(self, root: Root, key: str, data: bytes, *, expected_tag: str) -> str:
        target = self._write_target(root, key)
        lock_path = self._base(root).joinpath(_LOCK_DIR, *key.split("/")).with_name(target.name + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with FileLock(lock_path, timeout=_LOCK_TIMEOUT_S):
                try:
                    current = target.read_bytes()
                except FileNotFoundError:
                    raise ConflictError(f"{root}:{key} does not exist; expected tag {expected_tag}") from None
                if _sha256(current) != expected_tag:
                    raise ConflictError(f"{root}:{key} changed: its tag is no longer {expected_tag}")
                tmp = self._write_temp(root, data)
                try:
                    _replace(tmp, target)
                finally:
                    tmp.unlink(missing_ok=True)
        except Timeout as exc:
            raise ConflictError(f"could not lock {root}:{key} within {_LOCK_TIMEOUT_S} s") from exc
        _fsync_dir(target.parent)
        return _sha256(data)

    def list(self, root: Root, prefix: str) -> Iterator[ObjectInfo]:
        validate_prefix(prefix)
        base = self._base(root)
        folder, _, _ = prefix.rpartition("/")
        start = base.joinpath(*folder.split("/")) if folder else base
        if folder and folder.split("/")[0] in _INTERNAL_DIRS:
            return
        keys: builtins.list[str] = []
        for dirpath, dirnames, filenames in os.walk(start):
            rel = Path(dirpath).relative_to(base).as_posix()
            rel = "" if rel == "." else rel + "/"
            dirnames[:] = sorted(
                name
                for name in dirnames
                if not (rel == "" and name in _INTERNAL_DIRS)
                and (f"{rel}{name}/".startswith(prefix) or prefix.startswith(f"{rel}{name}/"))
            )
            keys.extend(rel + name for name in filenames if (rel + name).startswith(prefix))
        for key in sorted(keys):
            try:
                result = os.stat(base.joinpath(*key.split("/")))
            except FileNotFoundError:
                continue  # deleted between the walk and the stat: no longer stored
            yield ObjectInfo(key=key, size=result.st_size, modified=datetime.fromtimestamp(result.st_mtime, UTC))

    def stat(self, root: Root, key: str) -> ObjectInfo:
        result = os.stat(self._path(root, key))
        if not stat.S_ISREG(result.st_mode):
            raise FileNotFoundError(f"{root}:{key} is not a stored object")
        return ObjectInfo(key=key, size=result.st_size, modified=datetime.fromtimestamp(result.st_mtime, UTC))

    def delete(self, root: Root, key: str, *, missing_ok: bool = False) -> None:
        self._path(root, key).unlink(missing_ok=missing_ok)

    def exists(self, root: Root, key: str) -> bool:
        return self._path(root, key).is_file()

    def read_uri(self, root: Root, key: str) -> str:
        return os.path.abspath(self._path(root, key))

    def duckdb_secrets(self) -> builtins.list[str]:
        return []

    def dbt_secrets(self) -> builtins.list[DbtSecret]:
        return []
