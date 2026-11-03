"""The storage interface (spec §10.4, §10.5, §20).

Every storage (a local folder pair, or a pair of HF buckets) gives the same operations on keys under a root. A key is
a relative POSIX path. `validate_key` checks every key before any storage call, so a key can never leave its root.
A missing object raises `FileNotFoundError`; a failed conditional write returns `False` (`put_if_absent`) or raises
`ConflictError` (`replace_if_match`).
"""

from __future__ import annotations

import builtins
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final, Protocol

from lakehouse.storage.keys import Root, RootMap

if TYPE_CHECKING:
    from lakehouse.storage.ledger import LeaseLedger

__all__ = ["DbtSecret", "ObjectInfo", "Storage", "validate_key", "validate_prefix"]

# ':' is refused everywhere: on Windows "a:b" names an NTFS alternate data stream, and "C:x" is drive-relative.
_FORBIDDEN_CHARS: Final = re.compile(r"[\\:\x00-\x1f\x7f]")


@dataclass(frozen=True)
class DbtSecret:
    """One DuckDB secret of the dbt profile (plan Task 23). An option value names an environment variable through
    dbt's `env_var`, never a secret value, so the rendered profile file holds no secret (spec §20)."""

    type: str
    options: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ObjectInfo:
    """One stored object. `modified` is always the storage's own time (file `mtime`, or the S3 `LastModified`)."""

    key: str
    size: int
    modified: datetime


def validate_key(key: str) -> str:
    """Return `key` when it is a safe relative POSIX key; otherwise raise `ValueError` (spec §20)."""
    if not key or key.startswith("/") or _FORBIDDEN_CHARS.search(key):
        raise ValueError(f"invalid storage key {key!r}: it must be a non-empty relative POSIX path without ':' or '\\'")
    if any(part in ("", ".", "..") for part in key.split("/")):
        raise ValueError(f"invalid storage key {key!r}: empty, '.' or '..' part")
    return key


def validate_prefix(prefix: str) -> str:
    """Return `prefix` when it is a safe listing prefix: empty, or a key start that may end with `/`."""
    if prefix:
        validate_key(prefix.removesuffix("/"))
    return prefix


class Storage(Protocol):
    """The operations every storage gives. Tasks 7, 9, 11 and 12 depend on the conditional writes."""

    @property
    def roots(self) -> RootMap: ...

    @property
    def single_part_limit(self) -> int | None:
        """The largest object a conditional write accepts, or `None` for no limit (T6 spec D2)."""
        ...

    def lease_ledger(self) -> LeaseLedger | None:
        """The lease ledger that fences the lease (an HF writer), or `None`: the storage's own create-if-absent is
        the fence (a local lake) or the caller never takes the lease (an HF reader) (T6 spec D1)."""
        ...

    def put(self, root: Root, key: str, data: bytes | Path) -> None:
        """Write the object; the stored object gets a fresh storage time."""
        ...

    def put_if_absent(self, root: Root, key: str, data: bytes | Path) -> bool:
        """Create the object only when no object has the key. `False` means another writer has it."""
        ...

    def get(self, root: Root, key: str) -> bytes: ...

    def get_versioned(self, root: Root, key: str) -> tuple[bytes, str]:
        """The bytes and an opaque version tag that `replace_if_match` accepts."""
        ...

    def replace_if_match(self, root: Root, key: str, data: bytes, *, expected_tag: str) -> str:
        """Compare-and-swap: replace only when the stored tag is `expected_tag`; return the new tag."""
        ...

    def list(self, root: Root, prefix: str) -> Iterator[ObjectInfo]:
        """Every object whose key starts with `prefix`, sorted by key."""
        ...

    def stat(self, root: Root, key: str) -> ObjectInfo: ...

    def delete(self, root: Root, key: str, *, missing_ok: bool = False) -> None: ...

    def exists(self, root: Root, key: str) -> bool: ...

    def read_uri(self, root: Root, key: str) -> str:
        """The URI that DuckDB readers and DuckLake registrations use for the object."""
        ...

    def duckdb_secrets(self) -> builtins.list[str]:
        """The `CREATE SECRET` statements a DuckDB reader needs. They can hold secrets: never log them."""
        ...

    def dbt_secrets(self) -> builtins.list[DbtSecret]:
        """The secrets of the dbt profile: the same as `duckdb_secrets`, with environment-variable references in
        place of the values."""
        ...
