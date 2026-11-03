"""`lakehouse verify` (spec §10.4, §10.8; plan Tasks 9 and 26): check every catalog version in storage.

For each version: its catalog file matches the size and SHA-256 that its manifest records, and every file that its
manifest names exists in its root. `CURRENT` must name a stored version with that version's size and hash. The check
reads only object metadata and the catalog files; it writes nothing.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.publish import listed_sequences, read_current, version_key
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import CanonicalKey

__all__ = ["VerifyReport", "verify_lake"]


@dataclass(frozen=True)
class VerifyReport:
    versions: int
    files: int
    current_seq: int | None
    problems: list[str]


def verify_lake(storage: Storage) -> VerifyReport:
    problems: list[str] = []
    try:
        current = read_current(storage)
    except FileNotFoundError:
        return VerifyReport(0, 0, None, ["no CURRENT: the lake has no published version yet"])
    sequences = listed_sequences(storage)
    if current.seq not in sequences:
        problems.append(f"CURRENT names version {current.seq}, which is not in storage")
    present: dict[CanonicalKey, bool] = {}
    for seq in sequences:
        try:
            manifest = read_manifest(storage, seq)
        except FileNotFoundError:
            problems.append(f"version {seq}: its manifest is missing")
            continue
        data = storage.get("lake", version_key(seq))
        if len(data) != manifest.version_size or hashlib.sha256(data).hexdigest() != manifest.version_sha256:
            problems.append(f"version {seq}: the catalog file does not match the size and SHA-256 in its manifest")
        if seq == current.seq and (current.size, current.sha256) != (manifest.version_size, manifest.version_sha256):
            problems.append(f"CURRENT: its size and SHA-256 differ from version {seq}'s manifest")
        for key in manifest.canonical_keys:
            if key not in present:
                present[key] = storage.exists(key.root, key.path)
            if not present[key]:
                problems.append(f"version {seq}: the file {key.root}:{key.path} is missing")
    return VerifyReport(len(sequences), len(present), current.seq, problems)
