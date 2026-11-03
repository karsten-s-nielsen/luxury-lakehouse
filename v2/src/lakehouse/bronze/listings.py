"""Source-level listings split by visibility (spec §10.5).

A listing that covers many matches or entities becomes one file per visibility: public records go into a lake-root
file and private records into a restricted-root file. A record's visibility comes from its own flag where the source
gives one; a record without a flag, or a source without flags, takes the source's visibility. A listing with only
public records has no restricted part.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any, Final

import pyarrow as pa

from lakehouse.bronze.arrow import write_parquet
from lakehouse.bronze.schemas import BronzeTable
from lakehouse.bronze.writer import Visibility, WrittenFile, place_root, store_written_file
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import Root

__all__ = ["listing_key", "split_listing", "write_listing_parts"]

_VALUES: Final = frozenset({"public", "private"})
_ZSTD_LEVEL: Final = 3


def split_listing(
    records: pa.Table, *, visibility_column: str | None, source_visibility: Visibility
) -> dict[Root, pa.Table]:
    """The records per root; a root with no records is absent."""
    if records.num_rows == 0:
        return {}
    if visibility_column is None:
        return {place_root(source_visibility): records}
    flags: list[Any] = records.column(visibility_column).to_pylist()
    unknown = {str(value) for value in flags if value is not None and value not in _VALUES}
    if unknown:
        raise ValueError(f"unknown visibility values in {visibility_column}: {sorted(unknown)}")
    effective = [source_visibility if value is None else value for value in flags]
    parts: dict[Root, pa.Table] = {}
    for visibility in ("public", "private"):
        part = records.filter(pa.array([value == visibility for value in effective], pa.bool_()))
        if part.num_rows:
            parts[place_root(visibility)] = part
    return parts


def listing_key(provider: str, kind: str) -> int:
    """A stable signed 64-bit key for one source-level listing (`<provider>:<kind>`): the first bytes of its SHA-256.
    Every file of the listing starts with it, and a replace deletes exactly its rows (`swap_files`)."""
    digest = hashlib.sha256(f"{provider}:{kind}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def write_listing_parts(
    storage: Storage,
    table: BronzeTable,
    records: pa.Table,
    *,
    key: int,
    visibility_column: str | None,
    source_visibility: Visibility,
    content_version: str,
    out_dir: Path,
) -> list[WrittenFile]:
    """One file per root (§10.5): the listing's records, split by visibility, with a `listing_key` column; each
    file is stored in its root. A listing with no records writes nothing."""
    written: list[WrittenFile] = []
    for root, part in sorted(
        split_listing(records, visibility_column=visibility_column, source_visibility=source_visibility).items()
    ):
        part = part.append_column(
            pa.field("listing_key", pa.int64(), nullable=False), pa.array([key] * part.num_rows, pa.int64())
        )
        folder = out_dir / table
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{key}-{content_version}-{uuid.uuid4().hex}.parquet"
        write_parquet(part, path, compression_level=_ZSTD_LEVEL)
        visibility: Visibility = "public" if root == "lake" else "private"
        written.append(
            store_written_file(
                storage, path, table=table, visibility=visibility, rows=part.num_rows, content_version=content_version
            )
        )
    return written
