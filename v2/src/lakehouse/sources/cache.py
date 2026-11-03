"""A content-addressed cache for source listings (spec §29.13: StatsBomb match lists cached by git blob hash).

Keys are content hashes (a git blob SHA, an LFS oid), so a cached value never goes stale: a changed file has a new
key. The disk cache writes atomically, so a crash never leaves a partial entry.
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Final, Protocol

__all__ = ["DiskCache", "ListingCache", "MemoryCache"]

_KEY: Final = re.compile(r"^[0-9a-f]{20,128}$")


def _check(key: str) -> str:
    if not _KEY.match(key):
        raise ValueError(f"invalid cache key {key!r}: expected a hex content hash")
    return key


class ListingCache(Protocol):
    def get(self, key: str) -> bytes | None: ...

    def put(self, key: str, data: bytes) -> None: ...


class MemoryCache:
    def __init__(self) -> None:
        self._items: dict[str, bytes] = {}

    def get(self, key: str) -> bytes | None:
        return self._items.get(_check(key))

    def put(self, key: str, data: bytes) -> None:
        self._items[_check(key)] = data


class DiskCache:
    """One file per key under `folder`."""

    def __init__(self, folder: Path) -> None:
        self._folder = folder

    def get(self, key: str) -> bytes | None:
        try:
            return (self._folder / _check(key)).read_bytes()
        except FileNotFoundError:
            return None

    def put(self, key: str, data: bytes) -> None:
        self._folder.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(dir=self._folder, prefix=".put-")
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
            os.replace(name, self._folder / _check(key))
        finally:
            Path(name).unlink(missing_ok=True)
