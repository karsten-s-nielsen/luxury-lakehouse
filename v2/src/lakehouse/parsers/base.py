"""The provider parser contract (spec §11, §12; plan Task 14).

A parser turns the deliveries of one match into bronze tables, whatever channel delivered them. Every raw field
becomes a typed column; a field that no bronze column maps raises `UnmappedFieldError` (§12). The coordinate system
comes from the provider's documentation or data, never from a guess (§11.4).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable
from lakehouse.entries import SourceEntry
from lakehouse.ids.native import Provider
from lakehouse.raw.archive import Delivery

__all__ = ["CoordinateSystem", "NamesBundle", "ParsedMatch", "ProviderParser"]


@dataclass(frozen=True)
class CoordinateSystem:
    origin: Literal["centre", "top_left", "bottom_left"]
    units: Literal["m", "mm", "yd", "pct"]
    pitch_length: float | None
    pitch_width: float | None
    x_direction: str
    y_direction: str
    play_direction: str | None
    source_ref: str


@dataclass(frozen=True)
class NamesBundle:
    """Native ids with names, for the dimensions."""

    players: pa.Table
    teams: pa.Table
    competitions: pa.Table


@dataclass(frozen=True)
class ParsedMatch:
    """The bronze tables of one match. `coordinate_systems` maps each delivered artifact that holds coordinates to
    its system: a provider can use different systems per artifact (IDSSE tracking is centre-origin, its events are
    pitch-origin, §11.4)."""

    tables: Mapping[BronzeTable, pa.Table]
    coordinate_systems: Mapping[str, CoordinateSystem]
    names: NamesBundle


class ProviderParser(Protocol):
    @property
    def provider(self) -> Provider: ...

    @property
    def version(self) -> str:
        """The version of the parser's bronze output; `ops_matches.parser_version` records it per match (§13.2)."""
        ...

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch: ...

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]: ...

    def bronze_columns(self, artifact: str) -> frozenset[str]: ...


@runtime_checkable
class ListingParser(Protocol):
    """A parser of source-level listings (spec §10.5): the table of each kind, the column with a per-record
    visibility flag (or `None`), and the listing's rows with keys and audit columns."""

    def listing_table(self, kind: str) -> BronzeTable: ...

    def listing_visibility(self, kind: str) -> str | None: ...

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table: ...
