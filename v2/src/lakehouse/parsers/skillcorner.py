"""The SkillCorner parser: match.json, tracking JSONL, dynamic events and phases of play to bronze (spec §10.2, §11,
§12). The parser never knows which channel delivered the files (spec §6).

**match.json** maps through `jsonmap`: one `skillcorner_matches` row and one `skillcorner_players` row per player.
**The CSV files** map through `csvmap`; every row must belong to the match being parsed. **The tracking JSONL** is
read line by line: every number keeps its source text (`json.loads` with text hooks), so coordinates become exact
`int32` millimetres by the text rule of §10.2 and nothing passes through float first. Each frame's keys are checked
against the declared field set, so an unknown field fails the match (§12.3). Every frame is kept, including the
pre-match and half-time frames that the source writes with `period: null` and no player data (IRIS dropped them,
§12.6), and the frame's `possession` and its 8 `image_corners_projection` values. The core `timestamp` is
period-relative (§11.5); the source's match clock (`timestamp`, period 2 starts at 45:00) stays in `match_clock`.
"""

from __future__ import annotations

import json
import re
from array import array
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, cast

import numpy as np
import pyarrow as pa

from lakehouse.bronze.arrow import read_parquet_bytes
from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.entries import SourceEntry
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.exact import loads_exact, text_to_mm
from lakehouse.ids.keys import match_key, player_key, team_key
from lakehouse.ids.native import skillcorner_native_match_id, skillcorner_native_player_id, skillcorner_native_team_id
from lakehouse.parsers.base import CoordinateSystem, NamesBundle, ParsedMatch
from lakehouse.parsers.columns import append_columns, constant, key_array, with_audit
from lakehouse.parsers.csvmap import CsvColumns, csv_header
from lakehouse.parsers.inventory import check_mapped, json_field_paths
from lakehouse.parsers.jsonmap import describe_fields, mapped_paths, records_to_table
from lakehouse.parsers.skillcorner_fields import (
    DYNAMIC_EVENTS_COLUMNS,
    MATCH_FIELDS,
    MATCH_LIST_FIELDS,
    PHASES_OF_PLAY_COLUMNS,
    PHYSICAL_COLUMNS,
    PLAYER_FIELDS,
)
from lakehouse.raw.archive import Delivery

__all__ = [
    "ARTIFACTS",
    "DYNAMIC_EVENTS",
    "MATCHES",
    "MATCH_LIST",
    "PHASES_OF_PLAY",
    "PHYSICAL",
    "PLAYERS",
    "TRACKING_FIELDS",
    "TRACKING_FRAMES",
    "TRACKING_PLAYERS",
    "TRACKING_ROSTER",
    "SkillCornerParser",
    "bronze_docs",
]

PROVIDER: Final = "skillcorner"
PARSER_VERSION: Final = "1"  # the bronze output version (spec §13.2)
Artifact = Literal[
    "match",
    "tracking",
    "dynamic_events",
    "phases_of_play",
    "tracking_parquet",
    "dynamic_events_parquet",
    "physical",
    "matches",
]
ARTIFACTS: Final[tuple[Artifact, ...]] = (
    "match",
    "tracking",
    "dynamic_events",
    "phases_of_play",
    "tracking_parquet",  # pining owner tier, format_version 2 (pining ADR 0011)
    "dynamic_events_parquet",
    "physical",
)
LISTING: Final = "matches"  # data/matches.json, the source-level match list

MATCHES: Final = bronze_table("skillcorner_matches")
PLAYERS: Final = bronze_table("skillcorner_players")
DYNAMIC_EVENTS: Final = bronze_table("skillcorner_dynamic_events")
PHASES_OF_PLAY: Final = bronze_table("skillcorner_phases_of_play")
TRACKING_PLAYERS: Final = bronze_table("skillcorner_tracking_players")
TRACKING_FRAMES: Final = bronze_table("skillcorner_tracking_frames")
TRACKING_ROSTER: Final = bronze_table("skillcorner_tracking_roster")
PHYSICAL: Final = bronze_table("skillcorner_physical")
MATCH_LIST: Final = bronze_table("skillcorner_match_list")

_CORNERS: Final = (
    "x_top_left",
    "y_top_left",
    "x_bottom_left",
    "y_bottom_left",
    "x_bottom_right",
    "y_bottom_right",
    "x_top_right",
    "y_top_right",
)
_FRAME_KEYS: Final = frozenset(
    {"frame", "timestamp", "period", "ball_data", "possession", "image_corners_projection", "player_data"}
)
_BALL_KEYS: Final = frozenset({"x", "y", "z", "is_detected"})
_POSSESSION_KEYS: Final = frozenset({"player_id", "group"})
_CORNER_KEYS: Final = frozenset(_CORNERS)
_PLAYER_KEYS: Final = frozenset({"x", "y", "player_id", "is_detected"})
# The field inventory of a tracking frame, in `json_field_paths` syntax.
TRACKING_FIELDS: Final = frozenset(
    {"frame", "timestamp", "period"}
    | {f"ball_data.{key}" for key in _BALL_KEYS}
    | {f"possession.{key}" for key in _POSSESSION_KEYS}
    | {f"image_corners_projection.{key}" for key in _CORNER_KEYS}
    | {f"player_data[].{key}" for key in _PLAYER_KEYS}
    | {"player_data"}  # an empty list: the frame has no player
)
_CLOCK: Final = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{2})$")
_INT: Final = re.compile(r"^-?\d+$")
_EVENT_COLUMNS: Final = CsvColumns(DYNAMIC_EVENTS_COLUMNS)
_PHASE_COLUMNS: Final = CsvColumns(PHASES_OF_PLAY_COLUMNS)
_PHYSICAL_COLUMNS: Final = CsvColumns(PHYSICAL_COLUMNS)
_MATCH_RAW_PATHS: Final = (
    mapped_paths(MATCH_FIELDS)
    | frozenset(f"players[].{path}" for path in mapped_paths(PLAYER_FIELDS))
    | frozenset({"players"})
)
_STR: Final = pa.string()
_I64: Final = pa.int64()


class _Number(str):
    """A JSON number kept as its source text."""

    __slots__ = ()


def _loads_text_numbers(line: bytes) -> Any:
    return json.loads(line, parse_float=_Number, parse_int=_Number)


def _artifact(name: str) -> Artifact:
    if name not in ARTIFACTS and name != LISTING:
        raise ValueError(f"unknown SkillCorner artifact {name!r}")
    return name


def _player_key(raw: object) -> int | None:
    return player_key(PROVIDER, skillcorner_native_player_id(str(raw)))


def _team_key(raw: object) -> int | None:
    return team_key(PROVIDER, skillcorner_native_team_id(str(raw)))


def _keys(rows: int, native: str) -> dict[str, pa.Array[Any]]:
    return {
        "provider": constant(PROVIDER, rows, _STR),
        "native_match_id": constant(native, rows, _STR),
        "match_key": constant(match_key(PROVIDER, native), rows, _I64),
    }


def _clock_ms(text: str) -> int:
    """The match clock `HH:MM:SS.cc` in milliseconds; the text must round-trip."""
    match = _CLOCK.match(text)
    if match is None:
        raise InexactValueError(f"the clock {text!r} is not in HH:MM:SS.cc form")
    hours, minutes, seconds, hundredths = (int(part) for part in match.groups())
    if minutes > 59 or seconds > 59:
        raise InexactValueError(f"the clock {text!r} is out of range")
    return ((hours * 60 + minutes) * 60 + seconds) * 1000 + hundredths * 10


class _TrackingScan:
    """One pass over the tracking JSONL: checks, then columns (see the module docstring)."""

    def __init__(self, *, collect: bool) -> None:
        self.collect = collect
        self.paths: set[str] = set()
        self.unknown: set[str] = set()
        self._mm: dict[str, int] = {}
        # frames
        self.frame: list[int] = []
        self.period: list[int | None] = []
        self.clock: list[int | None] = []
        self.ball: dict[str, list[int | None]] = {"x": [], "y": [], "z": []}
        self.ball_detected: list[bool | None] = []
        self.possession_player: list[int | None] = []
        self.possession_group: list[str | None] = []
        self.corners: dict[str, list[int | None]] = {name: [] for name in _CORNERS}
        # players
        self.p_period = array("b")
        self.p_frame = array("i")
        self.p_player = array("q")
        self.p_x: list[int | None] = []
        self.p_y: list[int | None] = []
        self.p_visible: list[bool | None] = []

    def _check(self, line: int, where: str, value: Mapping[str, Any], allowed: frozenset[str]) -> None:
        for key, item in value.items():
            path = f"{where}.{key}" if where else key
            if not isinstance(item, dict | list):  # a container has no path of its own (`json_field_paths` syntax)
                self.paths.add(path)
            if key not in allowed:
                self.unknown.add(path)

    def _object(self, line: int, value: object, where: str) -> Mapping[str, Any]:
        if not isinstance(value, dict):
            raise UnexpectedPayloadError(f"tracking: line {line}: {where} is not an object")
        return cast("Mapping[str, Any]", value)

    def _int(self, line: int, value: object, where: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, _Number) or not _INT.match(value):
            raise UnexpectedPayloadError(f"tracking: line {line}: {where}={value!r} is not an integer")
        return int(value)

    def _mm_of(self, line: int, value: object, where: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, _Number):
            raise UnexpectedPayloadError(f"tracking: line {line}: {where}={value!r} is not a number")
        text = str(value)
        found = self._mm.get(text)
        if found is None:
            try:
                found = text_to_mm(text)
            except InexactValueError as exc:
                raise InexactValueError(f"tracking: line {line}: {where}={text}: {exc}") from None
            self._mm[text] = found
        return found

    @staticmethod
    def _flag(line: int, value: object, where: str) -> bool | None:
        if value is None or isinstance(value, bool):
            return value
        raise UnexpectedPayloadError(f"tracking: line {line}: {where}={value!r} is not a boolean")

    def line(self, number: int, raw: bytes) -> None:
        self.record(number, _loads_text_numbers(raw))

    def record(self, number: int, parsed: object) -> None:
        """One frame record, with its numbers as source text (`_Number`)."""
        frame = self._object(number, parsed, "the frame")
        self._check(number, "", frame, _FRAME_KEYS)
        ball = self._object(number, frame.get("ball_data", {}), "ball_data")
        possession = self._object(number, frame.get("possession", {}), "possession")
        corners = self._object(number, frame.get("image_corners_projection", {}), "image_corners_projection")
        self._check(number, "ball_data", ball, _BALL_KEYS)
        self._check(number, "possession", possession, _POSSESSION_KEYS)
        self._check(number, "image_corners_projection", corners, _CORNER_KEYS)
        players = frame.get("player_data", [])
        if not isinstance(players, list):
            raise UnexpectedPayloadError(f"tracking: line {number}: player_data is not a list")
        if not players:
            self.paths.add("player_data")
        items = [self._object(number, item, "player_data[]") for item in cast("list[object]", players)]
        for item in items:
            self._check(number, "player_data[]", item, _PLAYER_KEYS)
        if not self.collect:
            return
        n = self._int(number, frame.get("frame"), "frame")
        if n is None:
            raise UnexpectedPayloadError(f"tracking: line {number}: a frame without a frame number")
        period = self._int(number, frame.get("period"), "period")
        stamp = frame.get("timestamp")
        if stamp is not None and not isinstance(stamp, str):
            raise UnexpectedPayloadError(f"tracking: line {number}: timestamp={stamp!r} is not text")
        try:
            clock = None if stamp is None else _clock_ms(stamp)
        except InexactValueError as exc:
            raise InexactValueError(f"tracking: line {number}: {exc}") from None
        self.frame.append(n)
        self.period.append(period)
        self.clock.append(clock)
        for key in ("x", "y", "z"):
            self.ball[key].append(self._mm_of(number, ball.get(key), f"ball_data.{key}"))
        self.ball_detected.append(self._flag(number, ball.get("is_detected"), "ball_data.is_detected"))
        self.possession_player.append(self._int(number, possession.get("player_id"), "possession.player_id"))
        group = possession.get("group")
        if group is not None and not isinstance(group, str):
            raise UnexpectedPayloadError(f"tracking: line {number}: possession.group={group!r} is not text")
        self.possession_group.append(group)
        for name in _CORNERS:
            self.corners[name].append(self._mm_of(number, corners.get(name), f"image_corners_projection.{name}"))
        if items and period is None:
            raise UnexpectedPayloadError(f"tracking: line {number}: frame {n} has player data but no period")
        for item in items:
            pid = self._int(number, item.get("player_id"), "player_data[].player_id")
            if pid is None:
                raise UnexpectedPayloadError(f"tracking: line {number}: a player without player_id")
            self.p_period.append(cast("int", period))
            self.p_frame.append(n)
            self.p_player.append(pid)
            self.p_x.append(self._mm_of(number, item.get("x"), "player_data[].x"))
            self.p_y.append(self._mm_of(number, item.get("y"), "player_data[].y"))
            self.p_visible.append(self._flag(number, item.get("is_detected"), "player_data[].is_detected"))


def _lines(decoded: bytes) -> Iterator[tuple[int, bytes]]:
    for number, raw in enumerate(decoded.split(b"\n"), start=1):
        if raw.strip():
            yield number, raw


def _scan(decoded: bytes, *, collect: bool) -> _TrackingScan:
    scan = _TrackingScan(collect=collect)
    for number, raw in _lines(decoded):
        try:
            scan.line(number, raw)
        except json.JSONDecodeError as exc:
            raise UnexpectedPayloadError(f"tracking: line {number} is not JSON: {exc}") from exc
        if scan.unknown:
            raise UnmappedFieldError("tracking", sorted(scan.unknown))
    return scan


def _as_text(value: object) -> object:
    """A Parquet value in the JSONL frame shape: numbers become their shortest round-trip text (`repr`), as the
    JSONL writer wrote them, so the same text rule makes the same millimetres; a null object is an absent key."""
    if isinstance(value, dict):
        items = cast("dict[str, object]", value).items()
        return {key: _as_text(item) for key, item in items if not (item is None and key in _OBJECT_KEYS)}
    if isinstance(value, list):
        return [_as_text(item) for item in cast("list[object]", value)]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return _Number(str(value))
    if isinstance(value, float):
        return None if value != value else _Number(repr(value))  # NaN is a missing value (Task 4); inf fails later
    raise UnexpectedPayloadError(f"tracking: a {type(value).__name__} value is not a JSON value")


_OBJECT_KEYS: Final = frozenset({"ball_data", "possession", "image_corners_projection", "player_data"})


def _read_parquet(decoded: bytes, artifact: str) -> pa.Table:
    if not decoded.startswith(b"PAR1"):
        raise UnexpectedPayloadError(f"{artifact} is not a Parquet file")
    return read_parquet_bytes(decoded)


def _scan_parquet(decoded: bytes, *, collect: bool) -> _TrackingScan:
    """The tracking Parquet of format 2, one frame per row, through the same checks as the JSONL."""
    table = _read_parquet(decoded, "tracking_parquet")
    scan = _TrackingScan(collect=collect)
    number = 0
    for batch in table.to_batches(max_chunksize=4096):
        for row in batch.to_pylist():
            number += 1
            scan.record(number, _as_text(row))
            if scan.unknown:
                raise UnmappedFieldError("tracking_parquet", sorted(scan.unknown))
    return scan


def _keys_of(values: list[int | None], make: Callable[[object], int | None]) -> pa.Array[Any]:
    known: dict[int, int | None] = {}
    out: list[int | None] = []
    for value in values:
        if value is None:
            out.append(None)
            continue
        if value not in known:
            known[value] = make(value)
        out.append(known[value])
    return pa.array(out, _I64)


def _frames_table(scan: _TrackingScan, native: str) -> pa.Table:
    rows = len(scan.frame)
    first: dict[int, int] = {}
    for period, clock in zip(scan.period, scan.clock, strict=True):
        if period is not None and clock is not None and (period not in first or clock < first[period]):
            first[period] = clock
    stamps = [
        None if period is None or clock is None else timedelta(milliseconds=clock - first[period])
        for period, clock in zip(scan.period, scan.clock, strict=True)
    ]
    columns: dict[str, pa.Array[Any]] = {
        "match_key": constant(match_key(PROVIDER, native), rows, _I64),
        "period": pa.array(scan.period, pa.int8()),
        "frame": pa.array(scan.frame, pa.int32()),
        "timestamp": pa.array(stamps, pa.duration("us")),
        "ball_x_mm": pa.array(scan.ball["x"], pa.int32()),
        "ball_y_mm": pa.array(scan.ball["y"], pa.int32()),
        "ball_z_mm": pa.array(scan.ball["z"], pa.int32()),
        "ball_detected": pa.array(scan.ball_detected, pa.bool_()),
        "match_clock": pa.array(
            [None if c is None else timedelta(milliseconds=c) for c in scan.clock], pa.duration("ms")
        ),
        "possession_player_key": _keys_of(scan.possession_player, _player_key),
        "possession_group": pa.array(scan.possession_group, _STR),
    }
    for name in _CORNERS:
        columns[f"image_corners_{name}_mm"] = pa.array(scan.corners[name], pa.int32())
    return pa.table(columns)


def _players_table(scan: _TrackingScan, native: str) -> pa.Table:
    rows = len(scan.p_frame)
    players = np.frombuffer(scan.p_player, dtype=np.int64)
    unique, inverse = np.unique(players, return_inverse=True)
    keys = np.array([_player_key(int(u)) for u in unique], dtype=np.int64)[inverse] if rows else np.array([], np.int64)
    return pa.table(
        {
            "match_key": constant(match_key(PROVIDER, native), rows, _I64),
            "period": pa.array(np.frombuffer(scan.p_period, dtype=np.int8), pa.int8()),
            "frame": pa.array(np.frombuffer(scan.p_frame, dtype=np.int32), pa.int32()),
            "player_key": pa.array(keys, _I64),
            "x_mm": pa.array(scan.p_x, pa.int32()),
            "y_mm": pa.array(scan.p_y, pa.int32()),
            "is_visible": pa.array(scan.p_visible, pa.bool_()),
        }
    )


def _roster_table(scan: _TrackingScan) -> pa.Table:
    seen = set(np.unique(np.frombuffer(scan.p_player, dtype=np.int64)).tolist())
    seen |= {p for p in scan.possession_player if p is not None}
    ids = sorted(int(p) for p in seen)
    return pa.table({"player_id": pa.array(ids, _I64), "player_key": key_array(ids, _player_key)})


def _match_payload(decoded: bytes) -> dict[str, Any]:
    payload = loads_exact(decoded)
    if not isinstance(payload, dict):
        raise UnexpectedPayloadError("a SkillCorner match.json must hold a JSON object")
    return cast("dict[str, Any]", payload)


class SkillCornerParser:
    """The SkillCorner `ProviderParser`."""

    @property
    def provider(self) -> Literal["skillcorner"]:
        return "skillcorner"

    @property
    def version(self) -> str:
        """The version of this parser's bronze output: bump `PARSER_VERSION` with any change that alters bronze, and
        `lakehouse reparse --all` re-parses the matches of an older version from the raw archive (spec §13.2)."""
        return PARSER_VERSION

    def bronze_columns(self, artifact: str) -> frozenset[str]:
        kind = _artifact(artifact)
        if kind == "match":
            return _MATCH_RAW_PATHS
        if kind in ("tracking", "tracking_parquet"):
            return TRACKING_FIELDS
        if kind == "matches":
            return mapped_paths(MATCH_LIST_FIELDS)
        if kind == "physical":
            return frozenset(PHYSICAL_COLUMNS)
        return frozenset(DYNAMIC_EVENTS_COLUMNS if kind.startswith("dynamic_events") else PHASES_OF_PLAY_COLUMNS)

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]:
        kind = _artifact(artifact)
        if kind == "match":
            return json_field_paths(_match_payload(decoded))
        if kind == "matches":
            return json_field_paths(loads_exact(decoded))
        if kind == "tracking":
            scan = _TrackingScan(collect=False)
            for number, raw in _lines(decoded):
                scan.line(number, raw)
            return frozenset(scan.paths)
        if kind == "tracking_parquet":
            scan = _TrackingScan(collect=False)
            for number, row in enumerate(_read_parquet(decoded, kind).to_pylist(), start=1):
                scan.record(number, _as_text(row))
            return frozenset(scan.paths)
        if kind in ("dynamic_events_parquet", "physical"):
            return frozenset(_read_parquet(decoded, kind).column_names)
        return csv_header(decoded)

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:
        native = skillcorner_native_match_id(entry.native_match_id)
        match_delivery = deliveries.get("match")
        if match_delivery is None:
            raise IntegrityError(f"SkillCorner match {native} has no match delivery (players, teams and pitch)")
        at = datetime.now(UTC)
        tables: dict[BronzeTable, pa.Table] = {}

        payload = _match_payload(match_delivery.decoded)
        check_mapped("match", json_field_paths(payload), _MATCH_RAW_PATHS)
        if str(payload.get("id")) != native:
            raise IntegrityError(f"the match file holds match {payload.get('id')}, not {native}")
        record = {key: value for key, value in payload.items() if key != "players"}
        match = records_to_table([record], MATCH_FIELDS, artifact="match")
        match = append_columns(
            match,
            {
                **_keys(1, native),
                "home_team_key": key_array(match.column("home_team_id").to_pylist(), _team_key),
                "away_team_key": key_array(match.column("away_team_id").to_pylist(), _team_key),
            },
        )
        tables[MATCHES] = with_audit(match, run_id=run_id, source_hash=match_delivery.content_sha256, at=at)
        people = cast("list[dict[str, Any]]", payload.get("players") or [])
        players = records_to_table(people, PLAYER_FIELDS, artifact="match")
        players = append_columns(
            players,
            {
                **_keys(players.num_rows, native),
                "player_key": key_array(players.column("player_id").to_pylist(), _player_key),
                "team_key": key_array(players.column("team_id").to_pylist(), _team_key),
            },
        )
        tables[PLAYERS] = with_audit(players, run_id=run_id, source_hash=match_delivery.content_sha256, at=at)

        if "dynamic_events" in deliveries and "dynamic_events_parquet" in deliveries:
            raise IntegrityError(f"SkillCorner match {native} has dynamic events as CSV and as Parquet")
        for artifact, name, columns, team_column in (
            ("dynamic_events", DYNAMIC_EVENTS, _EVENT_COLUMNS, "team_id"),
            ("dynamic_events_parquet", DYNAMIC_EVENTS, _EVENT_COLUMNS, "team_id"),
            ("phases_of_play", PHASES_OF_PLAY, _PHASE_COLUMNS, "team_in_possession_id"),
            ("physical", PHYSICAL, _PHYSICAL_COLUMNS, "team_id"),
        ):
            delivery = deliveries.get(artifact)
            if delivery is None:
                continue
            if artifact in ("dynamic_events_parquet", "physical"):
                table = columns.arrow_table(_read_parquet(delivery.decoded, artifact), artifact=artifact)
            else:
                table = columns.table(delivery.decoded, artifact=artifact)
            others = {m for m in table.column("match_id").to_pylist() if m is not None and str(m) != native}
            if others:
                raise IntegrityError(f"{artifact} of {native} holds rows of match {sorted(others)}")
            extra = _keys(table.num_rows, native)
            if artifact.startswith("dynamic_events") or artifact == "physical":
                extra["player_key"] = key_array(table.column("player_id").to_pylist(), _player_key)
            extra["team_key"] = key_array(table.column(team_column).to_pylist(), _team_key)
            tables[name] = with_audit(
                append_columns(table, extra), run_id=run_id, source_hash=delivery.content_sha256, at=at
            )

        if "tracking" in deliveries and "tracking_parquet" in deliveries:
            raise IntegrityError(f"SkillCorner match {native} has tracking as JSONL and as Parquet")
        tracking = deliveries.get("tracking") or deliveries.get("tracking_parquet")
        if tracking is not None:
            scan = (
                _scan(tracking.decoded, collect=True)
                if "tracking" in deliveries
                else _scan_parquet(tracking.decoded, collect=True)
            )
            tables[TRACKING_PLAYERS] = _players_table(scan, native)
            tables[TRACKING_FRAMES] = _frames_table(scan, native)
            roster = _roster_table(scan)
            roster = append_columns(roster, _keys(roster.num_rows, native))
            tables[TRACKING_ROSTER] = with_audit(roster, run_id=run_id, source_hash=tracking.content_sha256, at=at)

        system = _coordinates(payload.get("pitch_length"), payload.get("pitch_width"))
        systems = {
            artifact: system
            for artifact in (
                "tracking",
                "tracking_parquet",
                "dynamic_events",
                "dynamic_events_parquet",
                "phases_of_play",
            )
            if artifact in deliveries
        }
        return ParsedMatch(tables=tables, coordinate_systems=systems, names=_names(tables))

    def listing_table(self, kind: str) -> BronzeTable:
        if kind != LISTING:
            raise ValueError(f"unknown SkillCorner listing {kind!r}")
        return MATCH_LIST

    def listing_visibility(self, kind: str) -> str | None:
        self.listing_table(kind)
        return None  # open data: every record is public

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table:
        """The source-level match list `data/matches.json` (spec §10.5) with its keys and audit columns."""
        self.listing_table(kind)
        payload = loads_exact(decoded)
        if not isinstance(payload, list):
            raise UnexpectedPayloadError("data/matches.json must hold a JSON list")
        table = records_to_table(cast("list[dict[str, Any]]", payload), MATCH_LIST_FIELDS, artifact="matches")
        table = append_columns(
            table,
            {
                "provider": constant(PROVIDER, table.num_rows, _STR),
                "match_key": key_array(
                    table.column("match_id").to_pylist(),
                    lambda m: match_key(PROVIDER, skillcorner_native_match_id(str(m))),
                ),
                "home_team_key": key_array(table.column("home_team_id").to_pylist(), _team_key),
                "away_team_key": key_array(table.column("away_team_id").to_pylist(), _team_key),
            },
        )
        return with_audit(table, run_id=run_id, source_hash=source_hash, at=datetime.now(UTC))


def _names(tables: Mapping[BronzeTable, pa.Table]) -> NamesBundle:
    match = tables[MATCHES].to_pylist()[0]
    teams = pa.table(
        {
            "team_id": pa.array([match["home_team_id"], match["away_team_id"]], _I64),
            "team_name": pa.array([match["home_team_name"], match["away_team_name"]], _STR),
        }
    )
    competitions = pa.table(
        {
            "competition_id": pa.array([match["competition_id"]], _I64),
            "competition_name": pa.array([match["competition_name"]], _STR),
            "season_id": pa.array([match["season_id"]], _I64),
            "season_name": pa.array([match["season_name"]], _STR),
        }
    )
    players = tables[PLAYERS].select(["player_id", "first_name", "last_name", "short_name", "team_id"])
    return NamesBundle(players=players, teams=teams, competitions=competitions)


def _coordinates(length: object, width: object) -> CoordinateSystem:
    return CoordinateSystem(
        origin="centre",
        units="m",
        pitch_length=float(cast("int", length)) if isinstance(length, int | float) else None,
        pitch_width=float(cast("int", width)) if isinstance(width, int | float) else None,
        x_direction="along the long side, -length/2 to +length/2; +x points right in the provider's field diagram",
        y_direction="along the short side, -width/2 to +width/2; +y points up in the provider's field diagram",
        play_direction="fixed axes for both teams; match.json home_team_side gives the home team's direction per "
        "period (left_to_right or right_to_left), and each event row has attacking_side",
        source_ref="SkillCorner/opendata README.md 'Tracking Data Description' and assets/field.jpg; pitch size "
        "from match.json pitch_length and pitch_width",
    )


_PHYSICAL_TYPES: Final = {"float": "float64", "int": "int64", "str": "string", "bool": "bool"}
_AUDIT_DOCS: Final = [
    ("_ingested_at", "When the run parsed the delivery (UTC)."),
    ("_run_id", "The run that wrote the row."),
    ("_source_hash", "The content_sha256 of the delivered file that the row comes from (spec §13.1)."),
]
_MATCH_KEY_DOCS: Final = [
    ("provider", "The provider name (`skillcorner`)."),
    ("native_match_id", "The canonical native match id, the SkillCorner match id as text (Task 2)."),
    ("match_key", "Surrogate match key: xxhash64 of `skillcorner|native_match_id`, seed 42, signed (spec §14)."),
]


def bronze_docs() -> list[tuple[BronzeTable, str, list[tuple[str, str]]]]:
    """Every bronze table with the description of each of its columns, in table order (spec §12.8)."""
    corners = [
        (
            f"image_corners_{name}_mm",
            f"Source field `image_corners_projection.{name}` (metres): a corner of the "
            "detected area, as exact int32 millimetres.",
        )
        for name in _CORNERS
    ]
    return [
        (
            MATCHES,
            "One row per match: every field of {id}_match.json except `players`.",
            [
                *describe_fields(MATCH_FIELDS),
                *_MATCH_KEY_DOCS,
                ("home_team_key", "Surrogate team key of `home_team_id` (spec §14)."),
                ("away_team_key", "Surrogate team key of `away_team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            PLAYERS,
            "One row per element of match.json `players`. Names come from here.",
            [
                *describe_fields(PLAYER_FIELDS, prefix="players[]."),
                *_MATCH_KEY_DOCS,
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            DYNAMIC_EVENTS,
            "One row per record of {id}_dynamic_events.csv. Positions are centre-origin metres (spec §11.4).",
            [
                *_EVENT_COLUMNS.describe(),
                *_MATCH_KEY_DOCS,
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            PHASES_OF_PLAY,
            "One row per record of {id}_phases_of_play.csv.",
            [
                *_PHASE_COLUMNS.describe(),
                *_MATCH_KEY_DOCS,
                ("team_key", "Surrogate team key of `team_in_possession_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            PHYSICAL,
            "One row per player of the pining owner tier's physical.parquet (format 2, pining ADR 0011): per-player "
            "physical metrics of the match (owner decision, 2026-10-27).",
            [
                *(
                    (name, f"Source column `{name}` of physical.parquet, as {_PHYSICAL_TYPES[kind]}.")
                    for name, kind in PHYSICAL_COLUMNS.items()
                ),
                *_MATCH_KEY_DOCS,
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            TRACKING_PLAYERS,
            "One row per element of a frame's `player_data` in {id}_tracking_extrapolated.jsonl (spec §11.5). "
            "Coordinates are centre-origin; the roster table holds the audit values (spec §11.2).",
            [
                ("match_key", "Surrogate match key (spec §14)."),
                ("period", "Source field `period` of the frame (1 or 2); a frame with players always has one."),
                ("frame", "Source field `frame` (10 frames per second)."),
                ("player_key", "Surrogate player key of `player_data[].player_id` (spec §14)."),
                ("x_mm", "Source field `player_data[].x` (metres), as exact int32 millimetres (spec §10.2)."),
                ("y_mm", "Source field `player_data[].y` (metres), as exact int32 millimetres (spec §10.2)."),
                (
                    "is_visible",
                    "Source field `player_data[].is_detected`: true when detected on the screen, false "
                    "when extrapolated.",
                ),
            ],
        ),
        (
            TRACKING_FRAMES,
            "One row per line of {id}_tracking_extrapolated.jsonl, including pre-match and half-time frames "
            "(spec §10.2, §11.5, §12.6).",
            [
                ("match_key", "Surrogate match key (spec §14)."),
                ("period", "Source field `period`; null for a frame outside every period (pre-match, half time)."),
                ("frame", "Source field `frame` (10 frames per second)."),
                ("timestamp", "Time since the first frame of the period: `match_clock` minus the period's first."),
                ("ball_x_mm", "Source field `ball_data.x` (metres), as exact int32 millimetres."),
                ("ball_y_mm", "Source field `ball_data.y` (metres), as exact int32 millimetres."),
                ("ball_z_mm", "Source field `ball_data.z` (height, metres), as exact int32 millimetres."),
                ("ball_detected", "Source field `ball_data.is_detected`."),
                ("match_clock", "Source field `timestamp` (`HH:MM:SS.cc`, the match time; period 2 starts at 45:00)."),
                ("possession_player_key", "Surrogate player key of `possession.player_id` (spec §14)."),
                ("possession_group", "Source field `possession.group`: `home team` or `away team`."),
                *corners,
            ],
        ),
        (
            TRACKING_ROSTER,
            "One row per player id of the tracking file: the native id behind each player_key (spec §11.2).",
            [
                ("player_id", "A `player_data[].player_id` or `possession.player_id` of the tracking file."),
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            MATCH_LIST,
            "One row per element of data/matches.json, the source-level match list (spec §10.5).",
            [
                *describe_fields(MATCH_LIST_FIELDS),
                ("provider", "The provider name (`skillcorner`)."),
                ("match_key", "Surrogate match key of `match_id` (spec §14)."),
                ("home_team_key", "Surrogate team key of `home_team_id` (spec §14)."),
                ("away_team_key", "Surrogate team key of `away_team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
    ]
