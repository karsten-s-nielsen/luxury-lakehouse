"""The tuned tracking layout (spec §10.2): core checks, sort order and per-column encodings.

Players are sorted `(period, player_key, frame)` and frames `(period, frame)`. Coordinates, frame numbers and the
frame time use `DELTA_BINARY_PACKED`; low-cardinality columns use dictionary encoding. No BYTE_STREAM_SPLIT: it
measured 2 to 2.2 times larger. Booleans keep Parquet's bit-packed encoding (1 bit per value), because Parquet has no
dictionary encoding for BOOLEAN and the bit-packed form is already smaller.
"""

from __future__ import annotations

from typing import Final, Literal

import pyarrow as pa

from lakehouse.bronze.arrow import SortOrder, column_types
from lakehouse.bronze.schemas import TRACKING_FRAMES_CORE, TRACKING_PLAYERS_CORE, BronzeTable

__all__ = ["TrackingKind", "check_tracking_schema", "encoding_options", "sort_keys", "tracking_kind"]

TrackingKind = Literal["players", "frames"]

_CORE: Final[dict[TrackingKind, pa.Schema]] = {"players": TRACKING_PLAYERS_CORE, "frames": TRACKING_FRAMES_CORE}
_SORT: Final[dict[TrackingKind, tuple[str, ...]]] = {
    "players": ("period", "player_key", "frame"),
    "frames": ("period", "frame"),
}
_DELTA: Final[dict[TrackingKind, tuple[str, ...]]] = {
    "players": ("frame", "x_mm", "y_mm"),
    "frames": ("frame", "timestamp", "ball_x_mm", "ball_y_mm", "ball_z_mm"),
}
_DICTIONARY: Final[dict[TrackingKind, tuple[str, ...]]] = {
    "players": ("match_key", "period", "player_key"),
    "frames": ("match_key", "period"),
}


def tracking_kind(table: BronzeTable) -> TrackingKind:
    """`players` or `frames`, from the table name suffix (`<provider>_tracking_players|frames`)."""
    if table.endswith("_tracking_players"):
        return "players"
    if table.endswith("_tracking_frames"):
        return "frames"
    raise ValueError(f"{table} is not a tracking table: its name must end in _tracking_players or _tracking_frames")


def check_tracking_schema(kind: TrackingKind, schema: pa.Schema) -> None:
    """Every core column must be present with its exact type. Extra provider columns are allowed."""
    found = column_types(schema)
    for name, wanted in column_types(_CORE[kind]).items():
        if name not in found:
            raise ValueError(f"tracking {kind} data lacks the core column {name}")
        if found[name] != wanted:
            raise ValueError(f"tracking {kind} column {name} has type {found[name]}, the core needs {wanted}")


def sort_keys(kind: TrackingKind) -> list[tuple[str, SortOrder]]:
    return [(name, "ascending") for name in _SORT[kind]]


def encoding_options(kind: TrackingKind, schema: pa.Schema) -> tuple[list[str], dict[str, str]]:
    """The `use_dictionary` column list and the `column_encoding` map for `pyarrow.parquet.write_table`."""
    delta = {name: "DELTA_BINARY_PACKED" for name in _DELTA[kind]}
    dictionary = list(_DICTIONARY[kind])
    for name, arrow_type in column_types(schema).items():
        if name in delta or name in dictionary:
            continue
        if pa.types.is_string(arrow_type) or pa.types.is_dictionary(arrow_type):
            dictionary.append(name)
    return dictionary, delta
