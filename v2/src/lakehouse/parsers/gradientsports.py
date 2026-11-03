"""The Gradient Sports (PFF FC) parser: metadata, roster, events and tracking to bronze (spec §10.2, §11, §12, §29.15).

**Metadata, roster and events** map through `jsonmap`. The players and the ball of each event (`homePlayers[]`,
`awayPlayers[]`, `ball[]`) become one `gradientsports_event_objects` row per object, with exact millimetres.

**Tracking** (`<match>.jsonl.bz2`, decoded by the delivery) is read line by line with every number kept as its source
text, so positions become exact int32 millimetres (§10.2, §29.15) and the period-relative `periodElapsedTime` an exact
duration in microseconds. Every line is a frame record; GS writes a frame once per possession event (PFF FC Tracking
Data Specification v2.2, §1), so v2 keeps every record with its `record_index` and leaves the choice to silver.
Players carry only a team side and an estimated jersey number: the roster of the match gives the player id, and a
jersey that the roster does not hold fails the match loudly. Raw and smoothed positions are separate player rows
(`smoothed`). Each frame has at most one ball; more fails loudly.
"""

from __future__ import annotations

import ast
import csv
import io
import json
import re
from array import array
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final, Literal, cast

import numpy as np
import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.entries import SourceEntry
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.exact import float_to_mm, loads_exact, text_to_mm
from lakehouse.ids.keys import competition_key, match_key, player_key, season_key, team_key
from lakehouse.ids.native import (
    gradientsports_native_competition_id,
    gradientsports_native_match_id,
    gradientsports_native_player_id,
    gradientsports_native_team_id,
    native_season_id,
)
from lakehouse.parsers.base import CoordinateSystem, NamesBundle, ParsedMatch
from lakehouse.parsers.columns import append_columns, constant, key_array, with_audit
from lakehouse.parsers.csvmap import CsvColumns
from lakehouse.parsers.gradientsports_fields import (
    BALL_KEYS,
    EVENT_FIELDS,
    EVENT_OBJECT_KEYS,
    FRAME_KINDS,
    GAME_EVENT_KINDS,
    METADATA_FIELDS,
    PLAYERS_CSV,
    POSSESSION_EVENT_KINDS,
    ROSTER_FIELDS,
    SMOOTHED_PLAYER_KEYS,
    TRACKING_PLAYER_KEYS,
    TrackingKind,
)
from lakehouse.parsers.inventory import check_mapped, json_field_paths
from lakehouse.parsers.jsonmap import convert_text, describe_fields, mapped_paths, records_to_table
from lakehouse.raw.archive import Delivery

__all__ = [
    "ARTIFACTS",
    "COMPETITIONS",
    "EVENTS",
    "EVENT_OBJECTS",
    "MATCHES",
    "PLAYER_LIST",
    "ROSTERS",
    "TRACKING_FRAMES",
    "TRACKING_PLAYERS",
    "GradientSportsParser",
    "bronze_docs",
]

PROVIDER: Final = "gradientsports"
PARSER_VERSION: Final = "2"  # the bronze output version (spec §13.2); 2 adds 27 rare GS event grade/player columns
Artifact = Literal["metadata", "roster", "events", "tracking"]
ARTIFACTS: Final[tuple[Artifact, ...]] = ("metadata", "roster", "events", "tracking")

MATCHES: Final = bronze_table("gradientsports_matches")
ROSTERS: Final = bronze_table("gradientsports_rosters")
EVENTS: Final = bronze_table("gradientsports_events")
EVENT_OBJECTS: Final = bronze_table("gradientsports_event_objects")
TRACKING_PLAYERS: Final = bronze_table("gradientsports_tracking_players")
TRACKING_FRAMES: Final = bronze_table("gradientsports_tracking_frames")
COMPETITIONS: Final = bronze_table("gradientsports_competitions")
PLAYER_LIST: Final = bronze_table("gradientsports_player_list")

_STR: Final = pa.string()
_I64: Final = pa.int64()
_VISIBLE: Final = {"VISIBLE": True, "ESTIMATED": False}
_SIDES: Final = ("home", "away")
_OBJECT_LISTS: Final = {"homePlayers": "home", "awayPlayers": "away", "ball": "ball"}
_EVENT_RAW_PATHS: Final = (
    mapped_paths(EVENT_FIELDS)
    | frozenset(f"{name}[].{key}" for name in _OBJECT_LISTS for key in EVENT_OBJECT_KEYS)
    | frozenset(_OBJECT_LISTS)
)
_TRACKING_FIELDS: Final = (
    frozenset(key for key, kind in FRAME_KINDS.items() if kind != "object")
    | frozenset(f"{side}Players[].{key}" for side in _SIDES for key in TRACKING_PLAYER_KEYS)
    | frozenset(f"{side}PlayersSmoothed[].{key}" for side in _SIDES for key in SMOOTHED_PLAYER_KEYS)
    | frozenset(f"balls[].{key}" for key in BALL_KEYS)
    | frozenset(f"ballsSmoothed.{key}" for key in BALL_KEYS)
    | frozenset(f"game_event.{key}" for key in GAME_EVENT_KINDS)
    | frozenset(f"possession_event.{key}" for key in POSSESSION_EVENT_KINDS)
    | frozenset({"balls", "ballsSmoothed", "game_event", "possession_event"})  # empty or null
    | frozenset(f"{side}Players{s}" for side in _SIDES for s in ("", "Smoothed"))
)
_GAMES_KEYS: Final = frozenset({"id", "season"})
_PLAYER_LIST_COLUMNS: Final = CsvColumns(PLAYERS_CSV)
_INT: Final = re.compile(r"^-?\d+$")
_MICRO: Final = Decimal(1_000_000)
_ARROW: Final[dict[TrackingKind, pa.DataType]] = {
    "int": pa.int64(),
    "float": pa.float64(),
    "us": pa.duration("us"),
    "ts": pa.timestamp("us"),
    "ts_utc": pa.timestamp("us", tz="UTC"),
    "str": _STR,
    "bool": pa.bool_(),
}


class _Number(str):
    """A JSON number kept as its source text."""

    __slots__ = ()


def _artifact(name: str) -> Artifact:
    if name not in ARTIFACTS:
        raise ValueError(f"unknown Gradient Sports artifact {name!r}")
    return name


def _player_key(raw: object) -> int | None:
    return player_key(PROVIDER, gradientsports_native_player_id(str(raw)))


def _team_key(raw: object) -> int | None:
    return team_key(PROVIDER, gradientsports_native_team_id(str(raw)))


def _keys(rows: int, native: str) -> dict[str, pa.Array[Any]]:
    return {
        "provider": constant(PROVIDER, rows, _STR),
        "native_match_id": constant(native, rows, _STR),
        "match_key": constant(match_key(PROVIDER, native), rows, _I64),
    }


def _list_payload(decoded: bytes, artifact: str) -> list[dict[str, Any]]:
    payload = loads_exact(decoded)
    if not isinstance(payload, list):
        raise UnexpectedPayloadError(f"a Gradient Sports {artifact} file must hold a JSON list")
    return cast("list[dict[str, Any]]", payload)


def _seconds_to_us(text: str) -> int:
    try:
        value = Decimal(text) * _MICRO
    except InvalidOperation:
        raise InexactValueError(f"{text!r} is not a number of seconds") from None
    if value != value.to_integral_value():
        raise InexactValueError(f"{text!r} has more than 6 decimals, so it is not exact in microseconds")
    return int(value)


def _typed(value: object, kind: TrackingKind, where: str) -> object:
    """A tracking value of a declared kind; numbers arrive as their source text (`_Number`)."""
    if value is None:
        return None
    if kind == "str" or kind == "bool":
        expected = str if kind == "str" else bool
        if not isinstance(value, expected) or isinstance(value, _Number):
            raise UnexpectedPayloadError(f"{where}={value!r} is not a {kind}")
        return value
    if kind in ("ts", "ts_utc"):
        if not isinstance(value, str) or isinstance(value, _Number):
            raise UnexpectedPayloadError(f"{where}={value!r} is not a time")
        return convert_text(value, "timestamp" if kind == "ts" else "timestamp_utc")
    if not isinstance(value, _Number):
        raise UnexpectedPayloadError(f"{where}={value!r} is not a number")
    text = str(value)
    if kind == "int":
        if not _INT.match(text):
            raise UnexpectedPayloadError(f"{where}={text} is not an integer")
        return int(text)
    if kind == "us":
        return timedelta(microseconds=_seconds_to_us(text))
    # GS's JSON serialiser emits floats the xmlmap exactness guard would refuse: trailing digits below the float64 ULP
    # (e.g. a videoTimeMs of 524491.1580000001, whose float64 is 524491.158) and exponent forms (e.g. 9e-06, whose
    # repr keeps the exponent). The owner decided GS floats ingest as the nearest float64 (ADR-001, 2026-10-27), so
    # take the float64 value directly. `value` is a `_Number` token here, so `float(text)` is always a finite float;
    # IDSSE keeps the strict text-exactness guard through `convert`.
    return float(text)


class _Columns:
    """Growing columns of one table: Python lists for the few frame columns, typed arrays for the many player rows."""

    def __init__(self) -> None:
        self.lists: dict[str, list[object]] = {}

    def add(self, name: str, value: object) -> None:
        self.lists.setdefault(name, []).append(value)


class _TrackingScan:
    def __init__(self, *, collect: bool, jerseys: Mapping[tuple[str, str], str]) -> None:
        self.collect = collect
        self.jerseys = jerseys
        self.paths: set[str] = set()
        self.unknown: set[str] = set()
        self._mm: dict[str, int] = {}
        self.frames = _Columns()
        self._keys: dict[tuple[str, str], int] = {}
        self.p_record = array("i")
        self.p_period = array("b")
        self.p_frame = array("i")
        self.p_player = array("q")
        self.p_x = array("i")
        self.p_y = array("i")
        self.p_visible = array("b")
        self.p_side = array("b")
        self.p_smoothed = array("b")
        self.p_index = array("h")
        self.p_speed = array("d")
        self.p_jersey: list[str] = []
        self.p_confidence: list[str | None] = []
        self.p_visibility: list[str] = []
        # one object per distinct value: three columns of 10+ million rows have a handful of values (audit T11)
        self._strings: dict[str, str] = {}

    def _shared(self, value: str) -> str:
        return self._strings.setdefault(value, value)

    def _check(self, where: str, value: Mapping[str, Any], allowed: frozenset[str] | Mapping[str, object]) -> None:
        for key, item in value.items():
            path = f"{where}.{key}" if where else key
            if not isinstance(item, dict | list) or not item:
                self.paths.add(path)
            if key not in allowed:
                self.unknown.add(path)

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

    def _object(self, line: int, value: object, where: str) -> Mapping[str, Any]:
        if not isinstance(value, dict):
            raise UnexpectedPayloadError(f"tracking: line {line}: {where} is not an object")
        return cast("Mapping[str, Any]", value)

    def _list(self, line: int, value: object, where: str) -> list[Mapping[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise UnexpectedPayloadError(f"tracking: line {line}: {where} is not a list")
        return [self._object(line, item, f"{where}[]") for item in cast("list[object]", value)]

    def _visible(self, line: int, value: object, where: str) -> bool | None:
        if value is None:
            return None
        if not isinstance(value, str) or value not in _VISIBLE:
            raise UnexpectedPayloadError(f"tracking: line {line}: {where}={value!r} is neither VISIBLE nor ESTIMATED")
        return _VISIBLE[value]

    def line(self, record: int, line: int, raw: bytes, teams: Mapping[str, str]) -> None:
        frame = self._object(line, json.loads(raw, parse_float=_Number, parse_int=_Number), "the frame")
        self._check("", frame, FRAME_KINDS)
        players = {
            (side, smoothed): self._list(line, frame.get(f"{side}Players{'Smoothed' if smoothed else ''}"), side)
            for side in _SIDES
            for smoothed in (False, True)
        }
        for (side, smoothed), rows in players.items():
            allowed = SMOOTHED_PLAYER_KEYS if smoothed else TRACKING_PLAYER_KEYS
            for row in rows:
                self._check(f"{side}Players{'Smoothed' if smoothed else ''}[]", row, allowed)
        balls = self._list(line, frame.get("balls"), "balls")
        for ball in balls:
            self._check("balls[]", ball, BALL_KEYS)
        smoothed_ball = frame.get("ballsSmoothed")
        if smoothed_ball is not None:
            self._check("ballsSmoothed", self._object(line, smoothed_ball, "ballsSmoothed"), BALL_KEYS)
        events: dict[str, Mapping[str, Any] | None] = {}
        for name, kinds in (("game_event", GAME_EVENT_KINDS), ("possession_event", POSSESSION_EVENT_KINDS)):
            value = frame.get(name)
            events[name] = None if value is None else self._object(line, value, name)
            if events[name] is not None:
                self._check(name, cast("Mapping[str, Any]", events[name]), kinds)
        if not self.collect or self.unknown:
            return
        if len(balls) > 1:
            raise UnexpectedPayloadError(f"tracking: line {line}: {len(balls)} balls in one frame; v2 maps one")
        values = {key: _typed(frame.get(key), kind, key) for key, kind in FRAME_KINDS.items() if kind != "object"}
        period, number = values["period"], values["frameNum"]
        if number is None:
            raise UnexpectedPayloadError(f"tracking: line {line}: a frame without frameNum")
        columns = self.frames
        columns.add("record_index", record)
        for key, value in values.items():
            columns.add(key, value)
        empty: Mapping[str, Any] = {}
        ball: Mapping[str, Any] = balls[0] if balls else empty
        smoothed_data: Mapping[str, Any] = cast("Mapping[str, Any]", smoothed_ball) if smoothed_ball else empty
        for prefix, data in (("ball", ball), ("ball_smoothed", smoothed_data)):
            for axis in ("x", "y", "z"):
                columns.add(f"{prefix}_{axis}_mm", self._mm_of(line, data.get(axis), f"{prefix}.{axis}"))
            visibility = data.get("visibility")
            columns.add(f"{prefix}_visibility", visibility)
            if prefix == "ball":
                columns.add("ball_detected", self._visible(line, visibility, "balls[].visibility"))
        for name, kinds in (("game_event", GAME_EVENT_KINDS), ("possession_event", POSSESSION_EVENT_KINDS)):
            data = events[name] or {}
            for key, kind in kinds.items():
                columns.add(f"{name}_{key}", _typed(data.get(key), kind, f"{name}.{key}"))
        for (side, smoothed), rows in players.items():
            if rows and period is None:
                raise UnexpectedPayloadError(f"tracking: line {line}: player data but no period")
            seen: set[str] = set()
            for index, row in enumerate(rows):
                jersey = str(row.get("jerseyNum"))
                if jersey in seen:
                    if smoothed:
                        # GS's smoothed stream repeats a co-located player (Task 32); keep the first.
                        continue
                    raise UnexpectedPayloadError(f"tracking: line {line}: jersey {jersey} twice in {side} players")
                seen.add(jersey)
                key = self._keys.get((side, jersey))
                if key is None:
                    pid = self.jerseys.get((teams[side], jersey))
                    if pid is None:
                        raise UnexpectedPayloadError(
                            f"tracking: line {line}: the {side} team's jersey {jersey} is not in the roster"
                        )
                    key = cast("int", _player_key(pid))
                    self._keys[(side, jersey)] = key
                x = self._mm_of(line, row.get("x"), f"{side}.x")
                y = self._mm_of(line, row.get("y"), f"{side}.y")
                if x is None or y is None:
                    raise UnexpectedPayloadError(f"tracking: line {line}: a {side} player without x and y")
                self.p_record.append(record)
                self.p_period.append(cast("int", period))
                self.p_frame.append(cast("int", number))
                self.p_player.append(key)
                self.p_x.append(x)
                self.p_y.append(y)
                visible = self._visible(line, row.get("visibility"), f"{side}.visibility")
                self.p_visible.append(2 if visible is None else int(visible))
                self.p_side.append(_SIDES.index(side))
                self.p_smoothed.append(int(smoothed))
                self.p_index.append(index)
                speed = _typed(row.get("speed"), "float", f"{side}.speed")
                self.p_speed.append(float("nan") if speed is None else cast("float", speed))
                self.p_jersey.append(self._shared(jersey))
                confidence = cast("str | None", _typed(row.get("confidence"), "str", f"{side}.confidence"))
                self.p_confidence.append(None if confidence is None else self._shared(confidence))
                self.p_visibility.append(self._shared(str(row.get("visibility"))))


def _lines(decoded: bytes) -> Iterator[tuple[int, bytes]]:
    """Each line of `decoded` with its 1-based number, one at a time: a list of every line would hold a second copy
    of the whole decoded file (1.2 GB for a real match, audit T11)."""
    start, number, size = 0, 0, len(decoded)
    while start < size:
        end = decoded.find(b"\n", start)
        if end < 0:
            end = size
        number += 1
        yield number, decoded[start:end]
        start = end + 1


def _scan(
    decoded: bytes, *, collect: bool, jerseys: Mapping[tuple[str, str], str], teams: Mapping[str, str]
) -> _TrackingScan:
    scan = _TrackingScan(collect=collect, jerseys=jerseys)
    record = 0
    for line, raw in _lines(decoded):
        if not raw.strip():
            continue
        try:
            scan.line(record, line, raw, teams)
        except json.JSONDecodeError as exc:
            raise UnexpectedPayloadError(f"tracking: line {line} is not JSON: {exc}") from exc
        if collect and scan.unknown:
            raise UnmappedFieldError("tracking", sorted(scan.unknown))
        record += 1
    return scan


def _frames_table(scan: _TrackingScan, native: str) -> pa.Table:
    lists = scan.frames.lists
    rows = len(lists.get("record_index", []))
    core: dict[str, pa.Array[Any]] = {
        "match_key": constant(match_key(PROVIDER, native), rows, _I64),
        "period": pa.array(lists.get("period", []), pa.int8()),
        "frame": pa.array(lists.get("frameNum", []), pa.int32()),
        "timestamp": pa.array(lists.get("periodElapsedTime", []), pa.duration("us")),
        "ball_x_mm": pa.array(lists.get("ball_x_mm", []), pa.int32()),
        "ball_y_mm": pa.array(lists.get("ball_y_mm", []), pa.int32()),
        "ball_z_mm": pa.array(lists.get("ball_z_mm", []), pa.int32()),
        "ball_detected": pa.array(lists.get("ball_detected", []), pa.bool_()),
        "record_index": pa.array(lists.get("record_index", []), pa.int32()),
    }
    names = {
        "version": "version",
        "gameRefId": "game_ref_id",
        "generatedTime": "generated_time",
        "smoothedTime": "smoothed_time",
        "videoTimeMs": "video_time_ms",
        "periodGameClockTime": "period_game_clock_time",
        "game_event_id": "game_event_id",
        "possession_event_id": "possession_event_id",
    }
    for source, column in names.items():
        core[column] = pa.array(lists.get(source, []), _ARROW[FRAME_KINDS[source]])
    core["ball_visibility"] = pa.array(lists.get("ball_visibility", []), _STR)
    for axis in ("x", "y", "z"):
        core[f"ball_smoothed_{axis}_mm"] = pa.array(lists.get(f"ball_smoothed_{axis}_mm", []), pa.int32())
    core["ball_smoothed_visibility"] = pa.array(lists.get("ball_smoothed_visibility", []), _STR)
    for name, kinds in (("game_event", GAME_EVENT_KINDS), ("possession_event", POSSESSION_EVENT_KINDS)):
        for key, kind in kinds.items():
            core[f"{name}_{key}"] = pa.array(lists.get(f"{name}_{key}", []), _ARROW[kind])
    return pa.table(core)


def _players_table(scan: _TrackingScan, native: str) -> pa.Table:
    rows = len(scan.p_frame)
    visible = np.frombuffer(scan.p_visible, dtype=np.int8)
    speed = np.frombuffer(scan.p_speed, dtype=np.float64)
    return pa.table(
        {
            "match_key": constant(match_key(PROVIDER, native), rows, _I64),
            "period": pa.array(np.frombuffer(scan.p_period, dtype=np.int8), pa.int8()),
            "frame": pa.array(np.frombuffer(scan.p_frame, dtype=np.int32), pa.int32()),
            "player_key": pa.array(np.frombuffer(scan.p_player, dtype=np.int64), _I64),
            "x_mm": pa.array(np.frombuffer(scan.p_x, dtype=np.int32), pa.int32()),
            "y_mm": pa.array(np.frombuffer(scan.p_y, dtype=np.int32), pa.int32()),
            "is_visible": pa.array(visible == 1, pa.bool_(), mask=visible == 2),
            "record_index": pa.array(np.frombuffer(scan.p_record, dtype=np.int32), pa.int32()),
            "team_side": pa.array([_SIDES[s] for s in scan.p_side], _STR),
            "smoothed": pa.array(np.frombuffer(scan.p_smoothed, dtype=np.int8) == 1, pa.bool_()),
            "list_index": pa.array(np.frombuffer(scan.p_index, dtype=np.int16), pa.int16()),
            "jersey_num": pa.array(scan.p_jersey, _STR),
            "confidence": pa.array(scan.p_confidence, _STR),
            "visibility": pa.array(scan.p_visibility, _STR),
            "speed": pa.array(speed, pa.float64(), mask=np.isnan(speed)),
        }
    )


def _event_objects(events: list[dict[str, Any]], native: str) -> pa.Table:
    columns: dict[str, list[object]] = {
        name: []
        for name in (
            "event_index",
            "object_kind",
            "list_index",
            "jersey_num",
            "player_id",
            "position_group_type",
            "confidence",
            "visibility",
            "speed",
            "x_mm",
            "y_mm",
            "z_mm",
        )
    }
    for event_index, event in enumerate(events):
        for name, kind in _OBJECT_LISTS.items():
            items = cast("list[dict[str, Any]]", event.get(name) or [])
            for list_index, item in enumerate(items):
                columns["event_index"].append(event_index)
                columns["object_kind"].append(kind)
                columns["list_index"].append(list_index)
                columns["jersey_num"].append(item.get("jerseyNum"))
                columns["player_id"].append(item.get("playerId"))
                columns["position_group_type"].append(item.get("positionGroupType"))
                columns["confidence"].append(item.get("confidence"))
                columns["visibility"].append(item.get("visibility"))
                columns["speed"].append(item.get("speed"))
                for axis in ("x", "y", "z"):
                    value = item.get(axis)
                    columns[f"{axis}_mm"].append(None if value is None else float_to_mm(float(value), "float64"))
    types = {
        "event_index": pa.int32(),
        "object_kind": _STR,
        "list_index": pa.int16(),
        "jersey_num": _I64,
        "player_id": _I64,
        "position_group_type": _STR,
        "confidence": _STR,
        "visibility": _STR,
        "speed": pa.float64(),
        "x_mm": pa.int32(),
        "y_mm": pa.int32(),
        "z_mm": pa.int32(),
    }
    table = pa.table({name: pa.array(values, types[name]) for name, values in columns.items()})
    return append_columns(
        table,
        {
            "player_key": key_array(table.column("player_id").to_pylist(), _player_key),
            **_keys(table.num_rows, native),
        },
    )


class GradientSportsParser:
    """The Gradient Sports `ProviderParser`. It never knows which channel delivered the files (spec §6)."""

    @property
    def provider(self) -> Literal["gradientsports"]:
        return "gradientsports"

    @property
    def version(self) -> str:
        """The version of this parser's bronze output: bump `PARSER_VERSION` with any change that alters bronze, and
        `lakehouse reparse --all` re-parses the matches of an older version from the raw archive (spec §13.2)."""
        return PARSER_VERSION

    def bronze_columns(self, artifact: str) -> frozenset[str]:
        kind = _artifact(artifact)
        if kind == "metadata":
            return mapped_paths(METADATA_FIELDS)
        if kind == "roster":
            return mapped_paths(ROSTER_FIELDS)
        if kind == "events":
            return _EVENT_RAW_PATHS
        return _TRACKING_FIELDS

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]:
        if _artifact(artifact) == "tracking":
            scan = _TrackingScan(collect=False, jerseys={})
            for line, raw in enumerate(decoded.split(b"\n"), start=1):
                if raw.strip():
                    scan.line(0, line, raw, {})
            return frozenset(scan.paths)
        return json_field_paths(_list_payload(decoded, artifact))

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:
        native = gradientsports_native_match_id(entry.native_match_id)
        metadata = deliveries.get("metadata")
        if metadata is None:
            raise IntegrityError(f"GS match {native} has no metadata delivery (teams, pitch and fps)")
        at = datetime.now(UTC)
        tables: dict[BronzeTable, pa.Table] = {}
        records = _list_payload(metadata.decoded, "metadata")
        if len(records) != 1 or str(records[0].get("id")) != native:
            raise IntegrityError(f"the GS metadata file holds {[r.get('id') for r in records]}, not match {native}")
        meta = records_to_table(records, METADATA_FIELDS, artifact="metadata")
        first = meta.to_pylist()[0]
        competition = first.get("competition_id")
        native_competition = gradientsports_native_competition_id(str(competition)) if competition else None
        season = first.get("season")
        meta = append_columns(
            meta,
            {
                **_keys(1, native),
                "home_team_key": key_array([first.get("home_team_id")], _team_key),
                "away_team_key": key_array([first.get("away_team_id")], _team_key),
                "competition_key": key_array([native_competition], lambda c: competition_key(PROVIDER, c)),
                "season_key": key_array(
                    [
                        native_season_id(PROVIDER, native_competition, str(season))
                        if native_competition and season
                        else None
                    ],
                    lambda s: season_key(PROVIDER, s),
                ),
            },
        )
        tables[MATCHES] = with_audit(meta, run_id=run_id, source_hash=metadata.content_sha256, at=at)
        teams = {"home": str(first.get("home_team_id")), "away": str(first.get("away_team_id"))}

        jerseys: dict[tuple[str, str], str] = {}
        roster = deliveries.get("roster")
        if roster is not None:
            rows = _list_payload(roster.decoded, "roster")
            table = records_to_table(rows, ROSTER_FIELDS, artifact="roster")
            for team, shirt, pid in zip(
                table.column("team_id").to_pylist(),
                table.column("shirt_number").to_pylist(),
                table.column("player_id").to_pylist(),
                strict=True,
            ):
                if (str(team), str(shirt)) in jerseys:
                    raise IntegrityError(f"the GS roster of {native} gives jersey {shirt} of team {team} twice")
                jerseys[(str(team), str(shirt))] = str(pid)
            table = append_columns(
                table,
                {
                    **_keys(table.num_rows, native),
                    "player_key": key_array(table.column("player_id").to_pylist(), _player_key),
                    "team_key": key_array(table.column("team_id").to_pylist(), _team_key),
                },
            )
            tables[ROSTERS] = with_audit(table, run_id=run_id, source_hash=roster.content_sha256, at=at)

        events = deliveries.get("events")
        if events is not None:
            payload = _list_payload(events.decoded, "events")
            check_mapped("events", json_field_paths(payload), self.bronze_columns("events"))
            flat = [{key: value for key, value in e.items() if key not in _OBJECT_LISTS} for e in payload]
            table = records_to_table(flat, EVENT_FIELDS, artifact="events")
            table = table.add_column(0, "event_index", pa.array(range(len(payload)), pa.int32()))
            table = append_columns(
                table,
                {
                    "player_key": key_array(table.column("game_events_player_id").to_pylist(), _player_key),
                    "team_key": key_array(table.column("game_events_team_id").to_pylist(), _team_key),
                    **_keys(table.num_rows, native),
                },
            )
            tables[EVENTS] = with_audit(table, run_id=run_id, source_hash=events.content_sha256, at=at)
            objects = _event_objects(payload, native)
            tables[EVENT_OBJECTS] = with_audit(objects, run_id=run_id, source_hash=events.content_sha256, at=at)

        tracking = deliveries.get("tracking")
        if tracking is not None:
            if roster is None:
                raise IntegrityError(f"GS match {native} has tracking but no roster: players are named by jersey only")
            scan = _scan(tracking.decoded, collect=True, jerseys=jerseys, teams=teams)
            tables[TRACKING_PLAYERS] = _players_table(scan, native)
            tables[TRACKING_FRAMES] = _frames_table(scan, native)

        system = _coordinates(first)
        systems = {artifact: system for artifact in ("tracking", "events") if artifact in deliveries}
        return ParsedMatch(tables=tables, coordinate_systems=systems, names=_names(tables, first))

    def listing_table(self, kind: str) -> BronzeTable:
        if kind not in ("competitions", "players"):
            raise ValueError(f"unknown Gradient Sports listing {kind!r}")
        return COMPETITIONS if kind == "competitions" else PLAYER_LIST

    def listing_visibility(self, kind: str) -> str | None:
        self.listing_table(kind)
        return None  # the public Drive folder: the source's visibility

    def listing(self, kind: str, decoded: bytes, *, run_id: str, source_hash: str) -> pa.Table:
        """A source-level listing (spec §10.5): `competitions.csv` or `players.csv`, with keys and audit columns."""
        self.listing_table(kind)
        at = datetime.now(UTC)
        if kind == "players":
            table = _PLAYER_LIST_COLUMNS.table(decoded, artifact="players")
            table = append_columns(
                table,
                {
                    "provider": constant(PROVIDER, table.num_rows, _STR),
                    "player_key": key_array(table.column("id").to_pylist(), _player_key),
                },
            )
            return with_audit(table, run_id=run_id, source_hash=source_hash, at=at)
        reader = csv.DictReader(io.StringIO(decoded.decode("utf-8"), newline=""))
        unknown = sorted(set(reader.fieldnames or ()) - {"games", "id", "name"})
        if unknown:
            raise UnmappedFieldError("competitions", unknown)
        games: list[list[dict[str, str]]] = []
        ids: list[str] = []
        names: list[str] = []
        for row in reader:
            value = ast.literal_eval(row["games"])  # a Python-literal list of dicts in the source CSV
            if not isinstance(value, list):
                raise UnexpectedPayloadError("competitions.csv: the games field is not a list")
            items = [cast("dict[str, object]", g) for g in cast("list[object]", value) if isinstance(g, dict)]
            if len(items) != len(cast("list[object]", value)):
                raise UnexpectedPayloadError("competitions.csv: a games entry is not a dict")
            extra = sorted({key for item in items for key in item} - _GAMES_KEYS)
            if extra:
                raise UnmappedFieldError("competitions", [f"games[].{key}" for key in extra])
            games.append([{key: str(item[key]) for key in sorted(_GAMES_KEYS) if key in item} for item in items])
            ids.append(row["id"])
            names.append(row["name"])
        game_type = pa.list_(pa.struct([("id", _STR), ("season", _STR)]))
        table = pa.table(
            {"games": pa.array(games, game_type), "id": pa.array(ids, _STR), "name": pa.array(names, _STR)}
        )
        table = append_columns(
            table,
            {
                "provider": constant(PROVIDER, table.num_rows, _STR),
                "competition_key": key_array(
                    ids, lambda c: competition_key(PROVIDER, gradientsports_native_competition_id(str(c)))
                ),
            },
        )
        return with_audit(table, run_id=run_id, source_hash=source_hash, at=at)


def _pitch(first: Mapping[str, Any]) -> tuple[float | None, float | None]:
    pitches = cast("list[dict[str, Any]]", first.get("stadium_pitches") or [])
    if len(pitches) != 1:
        return None, None  # no pitch, or several: the size is not known without a rule to choose
    pitch = pitches[0]
    return cast("float | None", pitch.get("length")), cast("float | None", pitch.get("width"))


def _coordinates(first: Mapping[str, Any]) -> CoordinateSystem:
    length, width = _pitch(first)
    return CoordinateSystem(
        origin="centre",
        units="m",
        pitch_length=length,
        pitch_width=width,
        x_direction="along the touchlines, increasing from left to right as seen by the main camera",
        y_direction="along the end lines, increasing from bottom to top as seen by the main camera",
        play_direction="fixed axes for both teams; metadata homeTeamStartLeft (and homeTeamStartLeftExtraTime) says "
        "whether the home team started on the left",
        source_ref="PFF FC Tracking Data Specification v2.2 (October 2023), section 7 'Pitch coordinates', in the GS "
        "Drive folder; event positions are the same values as tracking in the live sample; pitch size from metadata "
        "stadium.pitches",
    )


def _names(tables: Mapping[BronzeTable, pa.Table], first: Mapping[str, Any]) -> NamesBundle:
    roster = tables.get(ROSTERS)
    players = (
        roster.select(["player_id", "player_nickname", "team_id"])
        if roster is not None
        else pa.table(
            {"player_id": pa.array([], _STR), "player_nickname": pa.array([], _STR), "team_id": pa.array([], _STR)}
        )
    )
    teams = pa.table(
        {
            "team_id": pa.array([first.get("home_team_id"), first.get("away_team_id")], _STR),
            "team_name": pa.array([first.get("home_team_name"), first.get("away_team_name")], _STR),
        }
    )
    competitions = pa.table(
        {
            "competition_id": pa.array([first.get("competition_id")], _STR),
            "competition_name": pa.array([first.get("competition_name")], _STR),
            "season_id": pa.array([first.get("season")], _STR),
            "season_name": pa.array([first.get("season")], _STR),
        }
    )
    return NamesBundle(players=players, teams=teams, competitions=competitions)


_AUDIT_DOCS: Final = [
    ("_ingested_at", "When the run parsed the delivery (UTC)."),
    ("_run_id", "The run that wrote the row."),
    ("_source_hash", "The content_sha256 of the delivered file that the row comes from (spec §13.1)."),
]
_MATCH_KEY_DOCS: Final = [
    ("provider", "The provider name (`gradientsports`)."),
    ("native_match_id", "The canonical native match id, the GS game id as text (Task 2)."),
    ("match_key", "Surrogate match key: xxhash64 of `gradientsports|native_match_id`, seed 42, signed (spec §14)."),
]
_KIND_TEXT: Final[dict[TrackingKind, str]] = {
    "int": "as int64",
    "float": "as float64 (the source text round-trips exactly)",
    "us": "seconds, as an exact duration in microseconds",
    "ts": "a local ISO time, as timestamp",
    "ts_utc": "an ISO time ending in Z, as timestamp (UTC)",
    "str": "as string",
    "bool": "as bool",
    "object": "",
}


def bronze_docs() -> list[tuple[BronzeTable, str, list[tuple[str, str]]]]:
    """Every bronze table with the description of each of its columns, in table order (spec §12.8)."""
    frame_extra = [
        ("version", "Source field `version` (Sportlogiq tracking output version)."),
        ("game_ref_id", "Source field `gameRefId`, as float64 (the source writes it with `.0`)."),
        ("generated_time", "Source field `generatedTime`, an ISO time ending in Z, as timestamp (UTC)."),
        ("smoothed_time", "Source field `smoothedTime`, an ISO time ending in Z, as timestamp (UTC)."),
        ("video_time_ms", "Source field `videoTimeMs`: milliseconds since the start of the video, as float64."),
        ("period_game_clock_time", "Source field `periodGameClockTime`: the estimated game clock in seconds."),
        ("game_event_id", "Source field `game_event_id`, as float64 (the source writes it with `.0`)."),
        ("possession_event_id", "Source field `possession_event_id`, as float64 (the source writes it with `.0`)."),
        ("ball_visibility", "Source field `balls[].visibility` (VISIBLE or ESTIMATED); null without a ball."),
        ("ball_smoothed_x_mm", "Source field `ballsSmoothed.x` (metres), as exact int32 millimetres."),
        ("ball_smoothed_y_mm", "Source field `ballsSmoothed.y` (metres), as exact int32 millimetres."),
        ("ball_smoothed_z_mm", "Source field `ballsSmoothed.z` (metres), as exact int32 millimetres."),
        ("ball_smoothed_visibility", "Source field `ballsSmoothed.visibility`."),
        *(
            (f"game_event_{key}", f"Source field `game_event.{key}`, {_KIND_TEXT[kind]}.")
            for key, kind in GAME_EVENT_KINDS.items()
        ),
        *(
            (f"possession_event_{key}", f"Source field `possession_event.{key}`, {_KIND_TEXT[kind]}.")
            for key, kind in POSSESSION_EVENT_KINDS.items()
        ),
    ]
    return [
        (
            MATCHES,
            "One row per match: the metadata file (`Metadata/<match>.json`).",
            [
                *describe_fields(METADATA_FIELDS),
                *_MATCH_KEY_DOCS,
                ("home_team_key", "Surrogate team key of `home_team_id` (spec §14)."),
                ("away_team_key", "Surrogate team key of `away_team_id` (spec §14)."),
                ("competition_key", "Surrogate competition key of `competition_id` (spec §14)."),
                ("season_key", "Surrogate season key of `competition_id:season` (GS has no season id, spec §14.1)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            ROSTERS,
            "One row per roster entry (`Rosters/<match>.json`); it maps a team's jersey to the player (tracking).",
            [
                *describe_fields(ROSTER_FIELDS),
                *_MATCH_KEY_DOCS,
                ("player_key", "Surrogate player key of `player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            EVENTS,
            "One row per event of the current root event file (`Event Data/<match>.json`, spec §29.15).",
            [
                ("event_index", "The 0-based position of the event in the file."),
                *describe_fields(EVENT_FIELDS),
                ("player_key", "Surrogate player key of `game_events_player_id` (spec §14)."),
                ("team_key", "Surrogate team key of `game_events_team_id` (spec §14)."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            EVENT_OBJECTS,
            "One row per player and ball of an event (`homePlayers[]`, `awayPlayers[]`, `ball[]`).",
            [
                ("event_index", "The event's `event_index`."),
                ("object_kind", "`home`, `away` or `ball`: the list the object comes from."),
                ("list_index", "The 0-based position in that list."),
                ("jersey_num", "Source field `jerseyNum`, as int64; null for the ball."),
                ("player_id", "Source field `playerId`, as int64; null for the ball."),
                ("position_group_type", "Source field `positionGroupType`."),
                ("confidence", "Source field `confidence` (LOW, MEDIUM or HIGH)."),
                ("visibility", "Source field `visibility` (VISIBLE or ESTIMATED)."),
                ("speed", "Source field `speed`, as float64."),
                ("x_mm", "Source field `x` (metres), as exact int32 millimetres (spec §29.15)."),
                ("y_mm", "Source field `y` (metres), as exact int32 millimetres."),
                ("z_mm", "Source field `z` (metres, the ball only), as exact int32 millimetres."),
                ("player_key", "Surrogate player key of `player_id` (spec §14); null for the ball."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            TRACKING_PLAYERS,
            "One row per player entry of a frame record: the raw lists (`homePlayers`, `awayPlayers`) and the "
            "Kalman-smoothed lists (`...Smoothed`), so a player has two rows per record (spec §11.5).",
            [
                ("match_key", "Surrogate match key (spec §14)."),
                ("period", "Source field `period` (1 and 2, 3 and 4 extra time)."),
                ("frame", "Source field `frameNum`: frames since the start of the video."),
                ("player_key", "Surrogate player key of the roster player with this team and jersey (spec §14)."),
                ("x_mm", "Source field `x` (metres), as exact int32 millimetres (spec §29.15)."),
                ("y_mm", "Source field `y` (metres), as exact int32 millimetres."),
                ("is_visible", "Source field `visibility`: true for VISIBLE, false for ESTIMATED."),
                ("record_index", "The 0-based line of the frame record; GS writes a frame once per possession event."),
                ("team_side", "`home` or `away`: the list the entry comes from."),
                ("smoothed", "True for an entry of a `...Smoothed` list (Kalman filter)."),
                ("list_index", "The 0-based position in that list."),
                ("jersey_num", "Source field `jerseyNum` (estimated), as string."),
                ("confidence", "Source field `confidence` of the jersey estimate (LOW, MEDIUM or HIGH)."),
                ("visibility", "Source field `visibility` (VISIBLE or ESTIMATED)."),
                ("speed", "Source field `speed`, as float64; null in the smoothed lists."),
            ],
        ),
        (
            TRACKING_FRAMES,
            "One row per frame record of the tracking file, duplicates included (spec §10.2, §11.5).",
            [
                ("match_key", "Surrogate match key (spec §14)."),
                ("period", "Source field `period`."),
                ("frame", "Source field `frameNum`."),
                (
                    "timestamp",
                    "Source field `periodElapsedTime`: seconds since the period start, exact in microseconds.",
                ),
                ("ball_x_mm", "Source field `balls[].x` (metres), as exact int32 millimetres; null without a ball."),
                ("ball_y_mm", "Source field `balls[].y` (metres), as exact int32 millimetres."),
                ("ball_z_mm", "Source field `balls[].z` (metres), as exact int32 millimetres."),
                ("ball_detected", "Source field `balls[].visibility`: true for VISIBLE, false for ESTIMATED."),
                ("record_index", "The 0-based line of the frame record."),
                *frame_extra,
            ],
        ),
        (
            COMPETITIONS,
            "One row per competition of `competitions.csv` (the source-level competition list, spec §10.5).",
            [
                ("games", "Source column `games` (a Python-literal list of {id, season}), as list<struct>."),
                ("id", "Source column `id`, as string."),
                ("name", "Source column `name`, as string."),
                ("provider", "The provider name (`gradientsports`)."),
                ("competition_key", "Surrogate competition key of `id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            PLAYER_LIST,
            "One row per player of `players.csv` (the source-level player list, spec §10.5).",
            [
                *_PLAYER_LIST_COLUMNS.describe(),
                ("provider", "The provider name (`gradientsports`)."),
                ("player_key", "Surrogate player key of `id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
    ]
