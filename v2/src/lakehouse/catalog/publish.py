"""Catalog versions and the `CURRENT` pointer (spec §10.4, §10.6, §10.8).

Each catalog commit publishes an immutable version `catalog/v/<seq>.ducklake` and its manifest
`catalog/v/<seq>.files.json`. `catalog/CURRENT` names the version that readers use. It holds its own checksum, so a
reader detects a torn or partial `CURRENT`. `publish_current` is the only writer of `CURRENT`: it creates it once on
an empty lake and afterwards replaces it only by compare-and-swap.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from lakehouse.errors import ConfigError, ConflictError, IntegrityError, LeaseLostError, RootMismatchError
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import ROOTS, RootMap, StorageRootInfo
from lakehouse.storage.roots import normalise_root

__all__ = [
    "CURRENT_KEY",
    "VERSION_PREFIX",
    "CurrentPointer",
    "check_encryption",
    "check_roots",
    "listed_sequences",
    "manifest_key",
    "publish_current",
    "read_current",
    "roots_record",
    "version_key",
]

CURRENT_KEY: Final = "catalog/CURRENT"
VERSION_PREFIX: Final = "catalog/v/"
_VERSION_OBJECT: Final = re.compile(r"^catalog/v/([1-9][0-9]*)\.(?:ducklake|files\.json)$")
_READ_TRIES: Final = 3
_RETRY_WAIT_S: float = 0.2


def version_key(seq: int) -> str:
    return f"{VERSION_PREFIX}{_positive(seq)}.ducklake"


def manifest_key(seq: int) -> str:
    return f"{VERSION_PREFIX}{_positive(seq)}.files.json"


def _positive(seq: int) -> int:
    if seq < 1:
        raise ValueError(f"a catalog version sequence starts at 1, not {seq}")
    return seq


def listed_sequences(storage: Storage) -> list[int]:
    """Every sequence that has a version or a manifest object in storage, sorted."""
    found: set[int] = set()
    for info in storage.list("lake", VERSION_PREFIX):
        match = _VERSION_OBJECT.match(info.key)
        if match:
            found.add(int(match.group(1)))
    return sorted(found)


def _root_value(info: StorageRootInfo) -> str:
    if info.local_path is not None:
        return str(info.local_path)
    return f"hf://buckets/{info.hf_bucket}"


def roots_record(roots: RootMap) -> dict[str, str]:
    """The roots a version records: local paths or HF bucket URIs. S3 paths are not part of it (§10.5)."""
    return {root: _root_value(roots.info(root)) for root in ROOTS}


def check_roots(roots: RootMap, recorded: dict[str, str]) -> None:
    """Raise `RootMismatchError` when the resolved roots differ from the roots that `CURRENT` recorded."""
    resolved = roots_record(roots)
    if set(recorded) != set(ROOTS) or any(
        normalise_root(resolved[root]) != normalise_root(recorded[root]) for root in ROOTS
    ):
        raise RootMismatchError(
            f"the resolved roots (lake={resolved['lake']}, restricted={resolved['restricted']}) differ from the "
            f"roots the catalog recorded (lake={recorded.get('lake')}, restricted={recorded.get('restricted')}); "
            "use the roots of this lake, or run lakehouse relocate"
        )


def check_encryption(*, recorded: bool, key_present: bool, migrating: bool = False) -> None:
    """Fail closed on a key/flag mismatch before the attach (spec S1, plan T3). `ConfigError` maps to exit 2.

    `recorded` is the lake's persisted `encrypted` flag (plaintext, read from `CURRENT`/the manifest before the
    attach); `key_present` is whether `resolve_encryption_key` returned a key. The encrypted-with-no-key case is
    also enforced by DuckDB's attach, but is caught here for a clear exit-2 message; the unencrypted-with-key case
    would otherwise silently write a mixed lake, so it stops — except the migration that is encrypting the lake.
    The flag is not adversarially authenticated: the encrypted case is enforced by DuckDB regardless.
    """
    if recorded and not key_present:
        raise ConfigError("this lake is encrypted: set LAKEHOUSE_ENCRYPTION_KEY to open it")
    if key_present and not recorded and not migrating:
        raise ConfigError(
            "this lake was created unencrypted, but LAKEHOUSE_ENCRYPTION_KEY is set: unset it to open the lake "
            "(encrypting an existing unencrypted lake in place is deferred to a future cycle; recreate the lake "
            "with the key set instead)"
        )


class CurrentPointer(BaseModel):
    """The parsed `CURRENT` JSON, plus the storage tag it was read or written with (not part of the JSON)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    seq: int = Field(ge=1)
    version_key: str
    size: int = Field(ge=0)
    sha256: str
    parent_seq: int | None
    lease_generation: int = Field(ge=1)
    storage_roots: dict[str, str]
    encrypted: bool = False  # S1: whether the lake's data + catalog are encrypted; excluded from the self-checksum
    self_checksum: str
    tag: str | None = Field(default=None, exclude=True)

    @staticmethod
    def _checksum(payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def build(
        cls,
        *,
        seq: int,
        version_key: str,
        size: int,
        sha256: str,
        parent_seq: int | None,
        lease_generation: int,
        storage_roots: dict[str, str],
        encrypted: bool = False,
    ) -> CurrentPointer:
        payload: dict[str, Any] = {
            "seq": seq,
            "version_key": version_key,
            "size": size,
            "sha256": sha256,
            "parent_seq": parent_seq,
            "lease_generation": lease_generation,
            "storage_roots": dict(storage_roots),
        }
        # `encrypted` is excluded from the checksum payload: a pre-S1 CURRENT (no field) keeps its checksum, and
        # the flag need not be adversarially authenticated (DuckDB enforces the encrypted case regardless).
        return cls(**payload, encrypted=encrypted, self_checksum=cls._checksum(payload))

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def parse(cls, data: bytes, tag: str | None) -> CurrentPointer:
        """Parse and check the self checksum; raise `IntegrityError` on a torn, partial or altered `CURRENT`."""
        try:
            pointer = cls.model_validate_json(data)
        except ValidationError as exc:
            raise IntegrityError(f"{CURRENT_KEY} is not a valid pointer: {exc}") from exc
        payload = pointer.model_dump(exclude={"self_checksum", "tag", "encrypted"})
        if cls._checksum(payload) != pointer.self_checksum:
            raise IntegrityError(f"{CURRENT_KEY} fails its own checksum (torn or altered)")
        return pointer.model_copy(update={"tag": tag})


def read_current(storage: Storage) -> CurrentPointer:
    """The checked `CURRENT`. `FileNotFoundError` on an empty lake; `IntegrityError` after 3 failed tries."""
    error: IntegrityError | None = None
    for attempt in range(1, _READ_TRIES + 1):
        data, tag = storage.get_versioned("lake", CURRENT_KEY)
        try:
            return CurrentPointer.parse(data, tag)
        except IntegrityError as exc:
            error = exc
            if attempt < _READ_TRIES:
                time.sleep(_RETRY_WAIT_S * attempt)
    raise IntegrityError(f"{error} after {_READ_TRIES} tries") from error


def publish_current(storage: Storage, pointer: CurrentPointer, *, expected: CurrentPointer | None) -> CurrentPointer:
    """Point `CURRENT` at `pointer`; the only writer of `CURRENT` (commit and restore).

    `expected` is the pointer this writer last read or wrote: `None` creates `CURRENT` on an empty lake, once. A
    changed `CURRENT`, or a second creator, raises `LeaseLostError`. Returns `pointer` with its new storage tag.
    """
    data = pointer.to_bytes()
    if expected is None:
        if not storage.put_if_absent("lake", CURRENT_KEY, data):
            raise LeaseLostError(f"{CURRENT_KEY} already exists: another writer published first")
        stored, tag = storage.get_versioned("lake", CURRENT_KEY)
        if stored != data:
            raise LeaseLostError(f"{CURRENT_KEY} changed right after this writer created it")
        return pointer.model_copy(update={"tag": tag})
    if expected.tag is None:
        raise ValueError("the expected CURRENT has no storage tag: read it with read_current")
    try:
        storage.replace_if_match("lake", CURRENT_KEY, data, expected_tag=expected.tag)
    except ConflictError as exc:
        raise LeaseLostError(f"{CURRENT_KEY} changed since this writer read version {expected.seq}") from exc
    # The HF gateway's If-Match is not atomic under concurrency, so read CURRENT back: another writer's pointer there
    # is a lost update that must stop the run, never pass silently (T6 spec D3).
    stored, tag = storage.get_versioned("lake", CURRENT_KEY)
    if stored != data:
        raise LeaseLostError(
            f"{CURRENT_KEY} read back differs from the pointer this run wrote (version {pointer.seq}): another writer "
            "published"
        )
    return pointer.model_copy(update={"tag": tag})
