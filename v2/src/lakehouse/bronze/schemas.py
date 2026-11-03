"""Bronze table names, the shared tracking core and the audit columns (spec §10.2, §11.2, §11.5).

The tracking core is the same for every provider: one set of names, types and units. Coordinates are exact integer
millimetres (owner decision 2026-10-27: Gradient Sports delivers 3 decimals). Provider-specific fields stay as extra
typed columns after the core.
"""

from __future__ import annotations

import re
from typing import Final, NewType

import pyarrow as pa

__all__ = [
    "AUDIT_COLUMNS",
    "TRACKING_FRAMES_CORE",
    "TRACKING_PLAYERS_CORE",
    "BronzeTable",
    "bronze_table",
]

BronzeTable = NewType("BronzeTable", str)

_TABLE_NAME: Final = re.compile(r"^[a-z][a-z0-9_]*$")

# One row per player per frame. `is_visible` is the provider's detection flag, mapped to one meaning: true when the
# provider observed the player in that frame, false when it extrapolated or estimated the position.
TRACKING_PLAYERS_CORE: Final = pa.schema(
    [
        pa.field("match_key", pa.int64(), nullable=False),
        pa.field("period", pa.int8(), nullable=False),
        pa.field("frame", pa.int32(), nullable=False),
        pa.field("player_key", pa.int64(), nullable=False),
        pa.field("x_mm", pa.int32()),
        pa.field("y_mm", pa.int32()),
        pa.field("is_visible", pa.bool_()),
    ]
)

# One row per frame. `timestamp` is the time since the start of the period; an absolute wall clock, where the
# source has one, is a provider column `wall_clock` (timestamp, UTC). `ball_z_mm` is null where the source has none.
# `period` is null for a frame outside every period (for example SkillCorner pre-match and half-time frames, which the
# source writes with `period: null`); a player row always has a period.
TRACKING_FRAMES_CORE: Final = pa.schema(
    [
        pa.field("match_key", pa.int64(), nullable=False),
        pa.field("period", pa.int8()),
        pa.field("frame", pa.int32(), nullable=False),
        pa.field("timestamp", pa.duration("us")),  # GS gives 6 decimals of a second
        pa.field("ball_x_mm", pa.int32()),
        pa.field("ball_y_mm", pa.int32()),
        pa.field("ball_z_mm", pa.int32()),
        pa.field("ball_detected", pa.bool_()),
    ]
)

# Every non-tracking bronze row carries these (§11.2). Tracking tables keep them once per file in the match metadata.
AUDIT_COLUMNS: Final = pa.schema(
    [
        pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_run_id", pa.string(), nullable=False),
        pa.field("_source_hash", pa.string(), nullable=False),
    ]
)


def bronze_table(name: str) -> BronzeTable:
    """A checked bronze table name: lower-case letters, digits and `_`, starting with a letter."""
    if not _TABLE_NAME.match(name):
        raise ValueError(f"invalid bronze table name {name!r}")
    return BronzeTable(name)
