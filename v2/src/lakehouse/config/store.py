"""The versioned config store (spec §8.3).

The config is one object, `config/settings.json`, in the lake root, outside the DuckLake catalog. Every save also
writes `config/history/<version>.json`, so each version keeps who saved it, when, and what changed. A save checks the
version that the operator loaded and writes the next version, and the check and the write are one atomic step under
`ConfigLock`: an in-process lock per lake (the Space is one process), plus an OS file lock on local storage (the app
and the CLI can both write). A stale save raises `StaleSaveError`, which names the newer version and who saved it.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Final, cast

from filelock import FileLock, Timeout
from pydantic import BaseModel, ConfigDict, ValidationError

from lakehouse.config.model import Settings
from lakehouse.errors import ConflictError, StaleSaveError
from lakehouse.storage.base import Storage
from lakehouse.storage.roots import normalise_root

__all__ = ["CONFIG_KEY", "HISTORY_PREFIX", "ConfigLock", "ConfigStore", "ConfigVersionInfo"]

CONFIG_KEY: Final = "config/settings.json"
HISTORY_PREFIX: Final = "config/history/"
_LOCK_DIR: Final = ".locks"
_LOCK_NAME: Final = "config.lock"
_LOCK_TIMEOUT_S: Final = 30.0
_FIRST_SAVE: Final = "first save"

_registry_lock = threading.Lock()
_process_locks: dict[str, threading.Lock] = {}


class ConfigVersionInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: int
    saved_by: str
    saved_at: datetime
    changes: list[str]


class _Document(ConfigVersionInfo):
    settings: dict[str, Any]


def _lake_identity(storage: Storage) -> str:
    info = storage.roots.lake
    if info.local_path is not None:
        return "local:" + normalise_root(str(info.local_path))
    return f"hf:{info.hf_bucket}"


class ConfigLock:
    """The one lock of the config-side objects of a lake (spec §8.3). Task 22's `DecisionStore` uses it too.

    It is an in-process lock shared by every `ConfigLock` of the same lake, plus `filelock.FileLock` on
    `<lake>/.locks/config.lock` when the lake is local. It never creates the lake root.
    """

    def __init__(self, storage: Storage) -> None:
        identity = _lake_identity(storage)
        with _registry_lock:
            self._thread_lock = _process_locks.setdefault(identity, threading.Lock())
        local = storage.roots.lake.local_path
        self._lock_dir = local / _LOCK_DIR if local is not None else None
        self._file_lock = FileLock(self._lock_dir / _LOCK_NAME, timeout=_LOCK_TIMEOUT_S) if self._lock_dir else None

    def locked(self) -> bool:
        """True while any `ConfigLock` of this lake holds the lock in this process."""
        return self._thread_lock.locked()

    def __enter__(self) -> ConfigLock:
        if not self._thread_lock.acquire(timeout=_LOCK_TIMEOUT_S):
            raise ConflictError(f"the config lock is still busy after {_LOCK_TIMEOUT_S} s")
        try:
            if self._file_lock is not None and self._lock_dir is not None:
                self._lock_dir.mkdir(exist_ok=True)
                self._file_lock.acquire()
        except Timeout as exc:
            self._thread_lock.release()
            raise ConflictError(f"another process holds the config lock for more than {_LOCK_TIMEOUT_S} s") from exc
        except BaseException:
            self._thread_lock.release()
            raise
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        try:
            if self._file_lock is not None:
                self._file_lock.release()
        finally:
            self._thread_lock.release()


def _flatten(value: object, prefix: str, out: dict[str, object]) -> None:
    """Flatten a JSON value to `path: leaf` pairs. The module list is keyed by module id, so a path reads well."""
    if isinstance(value, Mapping):
        for key, item in cast("Mapping[str, object]", value).items():
            _flatten(item, f"{prefix}.{key}" if prefix else key, out)
    elif prefix == "modules" and isinstance(value, list):
        for module in cast("list[object]", value):
            if not isinstance(module, Mapping) or "module_id" not in module:
                raise ValueError("every module entry needs a module_id")
            entry = cast("Mapping[str, object]", module)
            _flatten(entry, f"modules.{entry['module_id']}", out)
    else:
        out[prefix] = value


def _changes(before: Settings, after: Settings) -> list[str]:
    old: dict[str, object] = {}
    new: dict[str, object] = {}
    _flatten(before.model_dump(mode="json"), "", old)
    _flatten(after.model_dump(mode="json"), "", new)
    return [
        f"{path}: {json.dumps(old.get(path))} -> {json.dumps(new.get(path))}"
        for path in sorted(old.keys() | new.keys())
        if old.get(path) != new.get(path)
    ]


def _history_key(version: int) -> str:
    return f"{HISTORY_PREFIX}{version:08d}.json"


class ConfigStore:
    """Load, save and list the versions of the config of one lake."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage
        self._lock = ConfigLock(storage)

    def _read(self) -> _Document | None:
        try:
            raw = self._storage.get("lake", CONFIG_KEY)
        except FileNotFoundError:
            return None
        return self._parse(raw, CONFIG_KEY)

    @staticmethod
    def _parse(raw: bytes, key: str) -> _Document:
        try:
            document = _Document.model_validate_json(raw)
            Settings.model_validate(document.settings)
        except ValidationError as exc:
            raise ValueError(f"{key} is not a valid config (check its schema_version): {exc}") from exc
        return document

    def load(self) -> tuple[Settings, int]:
        """The saved config and its version. Before the first save, raise `FileNotFoundError`."""
        document = self._read()
        if document is None:
            raise FileNotFoundError(f"no config saved yet in this lake ({CONFIG_KEY}): save the install defaults first")
        return Settings.model_validate(document.settings), document.version

    def save(self, draft: Settings, *, expected_version: int, operator: str) -> int:
        """Save `draft` as the next version. `expected_version` is the version the operator loaded (0 before the
        first save). Raises `StaleSaveError` when another save came first."""
        if not operator:
            raise ValueError("a config save needs the operator's name")
        with self._lock:
            current = self._read()
            current_version = current.version if current is not None else 0
            if current_version != expected_version:
                if current is None:
                    raise StaleSaveError(f"no config is saved yet, but the save expected version {expected_version}")
                raise StaleSaveError(
                    f"the config changed: version {current.version} was saved by {current.saved_by} at "
                    f"{current.saved_at:%Y-%m-%d %H:%M:%S} UTC after you loaded version {expected_version}. "
                    "Reload the config and make your change again."
                )
            changes = [_FIRST_SAVE] if current is None else _changes(Settings.model_validate(current.settings), draft)
            document = _Document(
                version=current_version + 1,
                saved_by=operator,
                saved_at=datetime.now(UTC),
                changes=changes,
                settings=draft.model_dump(mode="json"),
            )
            payload = document.model_dump_json(indent=2).encode("utf-8")
            # History first: a crash between the two writes leaves the old version current, never a lost history.
            self._storage.put("lake", _history_key(document.version), payload)
            self._storage.put("lake", CONFIG_KEY, payload)
            return document.version

    def history(self, limit: int = 500) -> list[ConfigVersionInfo]:
        """The saved versions, newest first, at most `limit`."""
        if limit < 1:
            raise ValueError("limit must be 1 or more")
        keys = sorted((info.key for info in self._storage.list("lake", HISTORY_PREFIX)), reverse=True)[:limit]
        result: list[ConfigVersionInfo] = []
        for key in keys:
            document = self._parse(self._storage.get("lake", key), key)
            result.append(ConfigVersionInfo.model_validate(document.model_dump(exclude={"settings"})))
        return result
