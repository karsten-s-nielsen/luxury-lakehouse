"""File manifests of catalog versions (spec §10.3 step 5, §10.3.1).

A manifest lists the canonical object keys of every file that its version's head snapshot names: data files and
delete files, from `ducklake_list_files` on every table. It also records the version's kind and run (version
retention, Task 12), and the version object's size, SHA-256 and storage roots (restore verifies the version and
detects a relocation, Task 11). A manifest never changes, like its version.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import duckdb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from lakehouse.catalog.publish import manifest_key
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import CanonicalKey, RootMap, canonical_key

__all__ = ["Manifest", "VersionKind", "build_manifest", "read_manifest"]

VersionKind = Literal["in_run", "run_end"]


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    seq: int = Field(ge=1)
    kind: VersionKind
    run_id: str
    published_at: AwareDatetime
    version_size: int = Field(ge=0)
    version_sha256: str
    storage_roots: dict[str, str]
    encrypted: bool = False  # S1: whether this version's data + catalog are encrypted
    keys: tuple[str, ...]

    @field_validator("keys")
    @classmethod
    def _canonical(cls, keys: tuple[str, ...]) -> tuple[str, ...]:
        for key in keys:
            CanonicalKey.parse(key)
        return keys

    @property
    def canonical_keys(self) -> list[CanonicalKey]:
        return [CanonicalKey.parse(key) for key in self.keys]

    def to_bytes(self) -> bytes:
        return self.model_dump_json(indent=1).encode("utf-8")


def build_manifest(conn: duckdb.DuckDBPyConnection, roots: RootMap) -> list[CanonicalKey]:
    """The canonical keys of every data and delete file of the head snapshot of catalog `lake`, sorted."""
    tables = conn.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' ORDER BY 1, 2"
    ).fetchall()
    keys: set[str] = set()
    for schema, table in tables:
        rows = conn.execute(
            "SELECT data_file, delete_file FROM ducklake_list_files('lake', ?, schema => ?)", [table, schema]
        ).fetchall()
        for data_file, delete_file in rows:
            for ref in (data_file, delete_file):
                if ref:
                    keys.add(str(canonical_key(str(ref), roots=roots, relative_to_data_path=False)))
    return [CanonicalKey.parse(key) for key in sorted(keys)]


def new_manifest(
    *,
    seq: int,
    kind: VersionKind,
    run_id: str,
    published_at: datetime,
    version_size: int,
    version_sha256: str,
    storage_roots: dict[str, str],
    keys: list[CanonicalKey],
    encrypted: bool = False,
) -> Manifest:
    return Manifest(
        seq=seq,
        kind=kind,
        run_id=run_id,
        published_at=published_at,
        version_size=version_size,
        version_sha256=version_sha256,
        storage_roots=storage_roots,
        encrypted=encrypted,
        keys=tuple(str(k) for k in keys),
    )


def read_manifest(storage: Storage, seq: int) -> Manifest:
    return Manifest.model_validate_json(storage.get("lake", manifest_key(seq)))
