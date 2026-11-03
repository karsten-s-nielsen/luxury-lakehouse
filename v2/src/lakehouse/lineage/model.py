"""The lineage table `ops_lineage` (spec §9, §13.1).

One row per channel delivery of one artifact of one match: the channel and its id, the channel's change signal, the
delivered and decoded hashes, the transport wrapper, when it was first seen and last listed, the delivery's own
visibility, whether this copy is the kept one, and the reason. A match from two channels is stored once; lineage
holds both deliveries. It is a small DuckLake-written table in the lake root.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal, cast

import duckdb

from lakehouse.hashing import Wrapper
from lakehouse.ids.native import Channel, Provider

__all__ = ["LINEAGE_TABLE", "LineageRow", "public_contents", "read_lineage", "write_lineage"]

LINEAGE_TABLE: Final = "ops_lineage"
_COLUMNS: Final = (
    ("match_key", "BIGINT NOT NULL"),
    ("provider", "VARCHAR NOT NULL"),
    ("channel", "VARCHAR NOT NULL"),
    ("channel_id", "VARCHAR NOT NULL"),
    ("artifact", "VARCHAR NOT NULL"),
    ("change_signal", "VARCHAR"),
    ("delivery_sha256", "VARCHAR NOT NULL"),
    ("content_sha256", "VARCHAR NOT NULL"),
    ("wrapper", "VARCHAR NOT NULL"),
    ("first_seen", "TIMESTAMPTZ NOT NULL"),
    ("last_listed", "TIMESTAMPTZ NOT NULL"),
    ("visibility", "VARCHAR NOT NULL"),
    ("kept", "BOOLEAN NOT NULL"),
    ("reason", "VARCHAR NOT NULL"),
)
_NAMES: Final = ", ".join(name for name, _ in _COLUMNS)


@dataclass(frozen=True)
class LineageRow:
    match_key: int
    provider: Provider
    channel: Channel
    channel_id: str
    artifact: str
    change_signal: str | None
    delivery_sha256: str
    content_sha256: str
    wrapper: Wrapper
    first_seen: datetime
    last_listed: datetime
    visibility: Literal["public", "private"]
    kept: bool
    reason: str


def public_contents(rows: Sequence[LineageRow]) -> frozenset[str]:
    """The content hashes that a public-channel delivery has: their raw objects are public at the source (§29.12)."""
    return frozenset(row.content_sha256 for row in rows if row.channel == "public")


def _create(conn: duckdb.DuckDBPyConnection) -> None:
    columns = ", ".join(f"{name} {kind}" for name, kind in _COLUMNS)
    conn.execute(f"CREATE TABLE IF NOT EXISTS lake.main.{LINEAGE_TABLE} ({columns})")


def write_lineage(conn: duckdb.DuckDBPyConnection, rows: Sequence[LineageRow]) -> None:
    """Insert lineage rows into catalog `lake`."""
    _create(conn)
    placeholders = ", ".join("?" for _ in _COLUMNS)
    for row in rows:
        conn.execute(
            f"INSERT INTO lake.main.{LINEAGE_TABLE} ({_NAMES}) VALUES ({placeholders})",  # noqa: S608 - constant names
            [getattr(row, name) for name, _ in _COLUMNS],
        )


def read_lineage(conn: duckdb.DuckDBPyConnection) -> list[LineageRow]:
    """Every lineage row, ordered by match, channel, artifact and content hash; `[]` before the first write."""
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        [LINEAGE_TABLE],
    ).fetchone()
    if exists is None or not exists[0]:
        return []
    rows = conn.execute(
        f"SELECT {_NAMES} FROM lake.main.{LINEAGE_TABLE} "  # noqa: S608 - constant names
        "ORDER BY match_key, channel, artifact, content_sha256"
    ).fetchall()
    return [
        LineageRow(
            match_key=int(r[0]),
            provider=cast("Provider", r[1]),
            channel=cast("Channel", r[2]),
            channel_id=str(r[3]),
            artifact=str(r[4]),
            change_signal=None if r[5] is None else str(r[5]),
            delivery_sha256=str(r[6]),
            content_sha256=str(r[7]),
            wrapper=cast("Wrapper", r[8]),
            first_seen=r[9],
            last_listed=r[10],
            visibility="private" if r[11] == "private" else "public",
            kept=bool(r[12]),
            reason=str(r[13]),
        )
        for r in rows
    ]
