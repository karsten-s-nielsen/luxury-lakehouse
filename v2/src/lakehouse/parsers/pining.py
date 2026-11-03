"""The pining match and player lists as source-level listings (spec §7.2, §10.5, §12; plan Task 20).

`GET /v1/{provider}/matches` and `GET /v1/{provider}/players` are listings that cover many matches or players, with a
visibility flag per record. Each lands in its own table per provider (`<provider>_pining_matches`,
`<provider>_pining_players`), split by that flag (`split_listing`); a missing flag means public (the API reference),
so no row has a null flag. An entry's `artifacts` and `artifact_meta` are maps keyed by artifact key; the legacy
SkillCorner keys carry the match id, so the key is a value: both maps become lists of structs with a `key` field.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Final, Literal, cast

import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.errors import UnexpectedPayloadError
from lakehouse.exact import loads_exact
from lakehouse.ids.channels import canonical_native_match_id
from lakehouse.ids.keys import match_key, player_key
from lakehouse.ids.native import (
    Provider,
    gradientsports_native_player_id,
    idsse_native_player_id,
    skillcorner_native_player_id,
    statsbomb_native_player_id,
)
from lakehouse.parsers.columns import append_columns, constant, key_array, with_audit
from lakehouse.parsers.inventory import check_mapped, json_field_paths
from lakehouse.parsers.jsonmap import Field, describe_fields, mapped_paths, records_to_table

__all__ = ["LISTING_TABLES", "PiningListingParser", "bronze_docs"]

ListingKind = Literal["matches", "players"]
PROVIDERS: Final[tuple[Provider, ...]] = ("gradientsports", "idsse", "skillcorner", "statsbomb")
LISTING_TABLES: Final[dict[tuple[str, str], BronzeTable]] = {
    (provider, kind): bronze_table(f"{provider}_pining_{kind}")
    for provider in PROVIDERS
    for kind in ("matches", "players")
}

_STR: Final = pa.string()
_I64: Final = pa.int64()
_TS: Final = pa.timestamp("us", tz="UTC")
_SOURCE: Final = (
    Field("source_name", "source.name", _STR),
    Field("source_url", "source.url", _STR),
    Field("source_licence", "source.licence", _STR),
    Field("source_revision", "source.revision", _STR),  # the upstream git commit (pining #79); null when not pinned
)
MATCH_FIELDS: Final = (
    Field("channel_match_id", "id", _STR),
    Field("date", "date", pa.date32(), convert="date"),
    Field("home", "home", _STR),
    Field("away", "away", _STR),
    Field("visibility", "visibility", _STR),
    Field("updated_at", "updated_at", _TS, convert="timestamp_utc"),
    Field("provenance", "provenance", _STR),
    *_SOURCE,
    Field("format_version", "format_version", _I64),
    Field("competition_id", "competition_id", _STR),
    Field("competition_name", "competition_name", _STR),
    Field("season_id", "season_id", _STR),
    Field("season_name", "season_name", _STR),
    Field("artifacts", "artifacts", pa.list_(pa.struct([("key", _STR), ("filename", _STR)]))),
    Field(
        "artifact_meta",
        "artifact_meta",
        pa.list_(pa.struct([("key", _STR), ("sha256", _STR), ("size", _I64)])),
    ),
)
# The canonical PlayerRecord (pining `schemas/players.schema.json`). Pining round-trips provider extension fields
# verbatim; the owner tier of 2026-10-27 has none (2960 StatsBomb, 2746 SkillCorner, 829 Gradient Sports and no IDSSE
# players), so an extension field fails as unmapped until it is declared.
PLAYER_FIELDS: Final = (
    Field("player_id", "id", _STR),
    Field("nickname", "nickname", _STR),
    Field("first_name", "firstName", _STR),
    Field("last_name", "lastName", _STR),
    Field("dob", "dob", pa.date32(), convert="date"),
    Field("height", "height", pa.float64()),
    Field("position", "position", _STR),
    Field("position_group_type", "positionGroupType", _STR),
    Field("nationality", "nationality", _STR),
    Field("visibility", "visibility", _STR),
    Field("updated_at", "updated_at", _TS, convert="timestamp_utc"),
    *_SOURCE,
)
_MAPS: Final = ("artifacts", "artifact_meta")
_PLAYER_IDS: Final[dict[str, Callable[[str], str]]] = {
    "statsbomb": lambda raw: statsbomb_native_player_id(int(raw)),
    "idsse": idsse_native_player_id,
    "skillcorner": skillcorner_native_player_id,
    "gradientsports": gradientsports_native_player_id,
}


def _kind(kind: str) -> ListingKind:
    if kind not in ("matches", "players"):
        raise ValueError(f"unknown pining listing {kind!r}")
    return kind


def _fold(paths: frozenset[str]) -> frozenset[str]:
    """`artifacts.<key>` and `artifact_meta.<key>.<field>` with the key folded to `<key>`: a key is a value."""
    folded: set[str] = set()
    for path in paths:
        head, _, rest = path.partition(".")
        if head in _MAPS and rest:
            _, _, leaf = rest.partition(".")
            path = f"{head}.<key>" + (f".{leaf}" if leaf else "")
        folded.add(path)
    return frozenset(folded)


def _as_list(value: object, name: str) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise UnexpectedPayloadError(f"pining entry field {name} is not an object")
    items = cast("dict[str, Any]", value)
    if name == "artifacts":
        return [{"key": key, "filename": filename} for key, filename in items.items()]
    return [{"key": key, **cast("dict[str, Any]", meta)} for key, meta in items.items()]


class PiningListingParser:
    """The two pining listings of one provider."""

    def __init__(self, provider: str) -> None:
        if provider not in PROVIDERS:
            raise ValueError(f"pining serves no provider {provider!r}")
        self._provider: Provider = cast("Provider", provider)

    def listing_table(self, kind: str) -> BronzeTable:
        return LISTING_TABLES[(self._provider, _kind(kind))]

    def listing_visibility(self, kind: str) -> str | None:
        _kind(kind)
        return "visibility"  # every pining record has its own flag (a missing one is written `public`)

    def _fields(self, kind: ListingKind) -> tuple[Field, ...]:
        return MATCH_FIELDS if kind == "matches" else PLAYER_FIELDS

    def bronze_columns(self, kind: ListingKind) -> frozenset[str]:
        paths = mapped_paths(self._fields(kind))
        if kind == "players":
            return paths
        return (paths - set(_MAPS)) | frozenset(
            {"artifacts", "artifacts.<key>", "artifact_meta", "artifact_meta.<key>.sha256", "artifact_meta.<key>.size"}
        )

    def _records(self, kind: ListingKind, decoded: bytes) -> list[dict[str, Any]]:
        payload = loads_exact(decoded)
        found = cast("dict[str, Any]", payload).get("provider") if isinstance(payload, dict) else None
        if found != self._provider:
            raise UnexpectedPayloadError(f"the pining {kind} list names provider {found!r}, not {self._provider!r}")
        records = cast("dict[str, Any]", payload).get(kind)
        if not isinstance(records, list):
            raise UnexpectedPayloadError(f"the pining {self._provider} {kind} list has no {kind} list")
        return cast("list[dict[str, Any]]", records)

    def field_inventory(self, kind: ListingKind, decoded: bytes) -> frozenset[str]:
        paths = json_field_paths(self._records(kind, decoded))
        return _fold(paths) if kind == "matches" else paths

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table:
        """The listing's rows with keys and audit columns; a missing visibility flag is `public`."""
        kind = _kind(kind)
        records = self._records(kind, decoded)
        paths = json_field_paths(records)
        check_mapped(f"pining {kind}", _fold(paths) if kind == "matches" else paths, self.bronze_columns(kind))
        rows: list[dict[str, Any]] = []
        for record in records:
            row = {"visibility": "public", **record}
            if kind == "matches":
                for name in _MAPS:
                    row[name] = _as_list(record.get(name), name)
            rows.append(row)
        table = records_to_table(rows, self._fields(kind), artifact=f"pining {kind}")
        extra: dict[str, pa.Array[Any]] = {"provider": constant(self._provider, table.num_rows, _STR)}
        if kind == "matches":
            native = [
                canonical_native_match_id(self._provider, "pining", str(raw))
                for raw in table.column("channel_match_id").to_pylist()
            ]
            extra["native_match_id"] = pa.array(native, _STR)
            extra["match_key"] = pa.array([match_key(self._provider, m) for m in native], _I64)
        else:
            make = _PLAYER_IDS[self._provider]
            extra["player_key"] = key_array(
                table.column("player_id").to_pylist(), lambda raw: player_key(self._provider, make(str(raw)))
            )
        return with_audit(append_columns(table, extra), run_id=run_id, source_hash=source_hash, at=datetime.now(UTC))


_AUDIT_DOCS: Final = [
    ("_ingested_at", "When the run parsed the listing (UTC)."),
    ("_run_id", "The run that wrote the row."),
    ("_source_hash", "The content_sha256 of the listing as delivered (spec §13.1)."),
]


def bronze_docs() -> list[tuple[BronzeTable, str, list[tuple[str, str]]]]:
    """Every pining listing table with the description of each column, in table order (spec §12.8)."""
    docs: list[tuple[BronzeTable, str, list[tuple[str, str]]]] = []
    for provider in PROVIDERS:
        match_docs = dict(describe_fields(MATCH_FIELDS))
        match_docs["artifacts"] = "Source field `artifacts` (artifact key -> file name), as a list of {key, filename}."
        match_docs["artifact_meta"] = (
            "Source field `artifact_meta` (artifact key -> SHA-256 and size of the bytes as stored and served), "
            "as a list of {key, sha256, size}."
        )
        match_docs["visibility"] += " A missing flag is `public` (API reference); never null."
        docs.append(
            (
                LISTING_TABLES[(provider, "matches")],
                f"One row per entry of `GET /v1/{provider}/matches` (the pining match list, spec §10.5).",
                [
                    *match_docs.items(),
                    ("provider", f"The provider name (`{provider}`)."),
                    ("native_match_id", "The canonical native match id of `channel_match_id` (spec §7.3)."),
                    ("match_key", "Surrogate match key of `native_match_id` (spec §14)."),
                    *_AUDIT_DOCS,
                ],
            )
        )
        player_docs = dict(describe_fields(PLAYER_FIELDS))
        player_docs["visibility"] += " A missing flag is `public` (API reference); never null."
        docs.append(
            (
                LISTING_TABLES[(provider, "players")],
                f"One row per record of `GET /v1/{provider}/players` (the pining player list, spec §10.5).",
                [
                    *player_docs.items(),
                    ("provider", f"The provider name (`{provider}`)."),
                    ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                    *_AUDIT_DOCS,
                ],
            )
        )
    return docs
