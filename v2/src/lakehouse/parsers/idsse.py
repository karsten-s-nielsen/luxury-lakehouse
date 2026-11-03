"""The IDSSE (DFL / Sportec) parser: matchinformation, events and positions XML to bronze (spec §10.2, §11, §12).

**Metadata and events** are mapped completely by `xmlmap`: every element and attribute of the field tables in
`idsse_fields` becomes a typed column. silly-kicks is not used for them, because its event parse drops the events
before the first kick-off, nested elements below depth 2 and every element type outside its prefix tables (reuse
audit, `tests/contract/test_idsse_inventory.py`).

**Tracking** goes through silly-kicks `parse_dfl_tracking`, the parsing authority for IDSSE tracking (§12.7). One
streaming pass over the positions text runs first (`_PositionsScan`, defusedxml SAX, no tree). It:

- checks the field inventory, so an unknown attribute fails before any parse (§12.3);
- checks the `X`, `Y` and `Z` text against the millimetre rule (§10.2) and the `S`, `A` and `D` text against the
  float64 rule and silly-kicks' 4-decimal rounding, so the silly-kicks values are exact;
- proves that every FrameSet agrees on `T` and `M` at each frame (`t == ball_t`, §10.2), so the frames table holds
  them once per frame;
- ingests a FrameSet of a metadata referee or official (its person is known, team null) and refuses only a
  FrameSet of an object the metadata does not name,
  an unknown game section, and a player frame without `X` and `Y`;
- collects the frame-level values: the wall clock `T`, the minute of play `M` (silly-kicks casts it to a boolean and
  loses it) and the ball's frames.

The players table comes from the silly-kicks rows; its row count and frame clock are checked against the scan. The
frames table comes from the scan: one row per frame of any FrameSet, with the ball's values where the ball has that
frame. The file headers (`PutDataRequest`, `Positions`, `MetaData`) go to `idsse_files`, one row per delivered file.
"""

from __future__ import annotations

import io
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal, cast
from xml.sax import SAXParseException
from xml.sax.handler import ContentHandler
from xml.sax.xmlreader import AttributesImpl, Locator

import defusedxml
import defusedxml.ElementTree
import defusedxml.sax
import numpy as np
import pyarrow as pa

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.entries import SourceEntry
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError
from lakehouse.exact import column_to_mm, text_to_mm
from lakehouse.ids.keys import competition_key, match_key, player_key, season_key, team_key
from lakehouse.ids.native import (
    idsse_native_competition_id,
    idsse_native_match_id,
    idsse_native_player_id,
    idsse_native_team_id,
    native_season_id,
)
from lakehouse.parsers.base import CoordinateSystem, NamesBundle, ParsedMatch
from lakehouse.parsers.columns import append_columns, constant, key_array, with_audit
from lakehouse.parsers.idsse_fields import (
    EVENTS_ATTRIBUTES,
    EVENTS_ELEMENTS,
    METADATA_ATTRIBUTES,
    METADATA_ELEMENTS,
    POSITIONS_ATTRIBUTES,
    POSITIONS_ELEMENTS,
)
from lakehouse.parsers.inventory import check_mapped
from lakehouse.parsers.sportec import SportecTracking, parse_tracking
from lakehouse.parsers.xmlmap import XmlColumns, XmlKind, convert, element_values, xml_field_paths
from lakehouse.raw.archive import Delivery

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element  # the type only; parsing uses defusedxml

__all__ = [
    "ARTIFACTS",
    "EVENTS",
    "FILES",
    "GAME_SECTIONS",
    "MATCH_INFORMATION",
    "PERSONS",
    "TEAMS",
    "TRACKING_FRAMES",
    "TRACKING_PLAYERS",
    "TRACKING_ROSTER",
    "IdsseParser",
    "bronze_docs",
    "parse_tracking",
]

PROVIDER: Final = "idsse"
PARSER_VERSION: Final = "1"  # the bronze output version (spec §13.2)
Artifact = Literal["metadata", "events", "positions"]
ARTIFACTS: Final[tuple[Artifact, ...]] = ("metadata", "events", "positions")

FILES: Final = bronze_table("idsse_files")
MATCH_INFORMATION: Final = bronze_table("idsse_match_information")
TEAMS: Final = bronze_table("idsse_teams")
PERSONS: Final = bronze_table("idsse_persons")
EVENTS: Final = bronze_table("idsse_events")
TRACKING_PLAYERS: Final = bronze_table("idsse_tracking_players")
TRACKING_FRAMES: Final = bronze_table("idsse_tracking_frames")
TRACKING_ROSTER: Final = bronze_table("idsse_tracking_roster")

# DFL game sections and their periods: the silly-kicks table (`_SECTION_TO_PERIOD`), pinned by a parity test.
GAME_SECTIONS: Final[dict[str, int]] = {
    "firstHalf": 1,
    "secondHalf": 2,
    "extraTimeFirstHalf": 3,
    "extraTimeSecondHalf": 4,
    "penaltyShootout": 5,
}
BALL_TEAM: Final = "BALL"
FRAME_MS: Final = 40  # 25 frames per second (Bassek et al. 2025; HF pysport/idsse-data README)
_SILLY_KICKS_DECIMALS: Final = 4  # silly-kicks `_parse_float_or_none` returns round(value, 4)
_STR: Final = pa.string()
_I64: Final = pa.int64()
_ROOT: Final = "PutDataRequest"
_PERSON_KINDS: Final = {"Player": "player", "Trainer": "trainer", "Official": "official", "Referee": "referee"}
_PERSON_PARENTS: Final = {
    "Player": "Teams/Team/Players/Player",
    "Trainer": "Teams/Team/TrainerStaff/Trainer",
    "Official": "Teams/Team/OfficialStaff/Official",
    "Referee": "Referees/Referee",
}
_FIELDS: Final[dict[Artifact, tuple[Mapping[str, XmlKind], tuple[str, ...]]]] = {
    "metadata": (METADATA_ATTRIBUTES, METADATA_ELEMENTS),
    "events": (EVENTS_ATTRIBUTES, EVENTS_ELEMENTS),
    "positions": (POSITIONS_ATTRIBUTES, POSITIONS_ELEMENTS),
}
_FRAME_PATH: Final = "PutDataRequest/Positions/FrameSet/Frame"
_FRAMESET_PATH: Final = "PutDataRequest/Positions/FrameSet"


def _under(attributes: Mapping[str, XmlKind], base: str) -> dict[str, XmlKind]:
    """The attribute paths below `base`, relative to it (`base@A` becomes `@A`, `base/E@A` becomes `E@A`)."""
    found: dict[str, XmlKind] = {}
    for path, kind in attributes.items():
        if path.startswith(f"{base}@"):
            found[path[len(base) :]] = kind
        elif path.startswith(f"{base}/"):
            found[path[len(base) + 1 :]] = kind
    return found


def _merge(*parts: Mapping[str, XmlKind]) -> dict[str, XmlKind]:
    merged: dict[str, XmlKind] = {}
    for part in parts:
        for path, kind in part.items():
            if merged.setdefault(path, kind) != kind:
                raise ValueError(f"{path} has two kinds: {merged[path]} and {kind}")
    return dict(sorted(merged.items()))


def _header(attributes: Mapping[str, XmlKind]) -> dict[str, XmlKind]:
    """Root-level header fields: everything below `PutDataRequest` except the row elements."""
    rows = ("Event", "MatchInformation", "Positions/FrameSet")
    return {
        path: kind
        for path, kind in _under(attributes, _ROOT).items()
        if not any(path == row or path.startswith((f"{row}@", f"{row}/")) for row in rows)
    }


_FILE_COLUMNS: Final = XmlColumns(
    attributes=_merge(_header(METADATA_ATTRIBUTES), _header(EVENTS_ATTRIBUTES), _header(POSITIONS_ATTRIBUTES)),
    elements=("Positions", "Positions/MetaData", "Positions/MetaData/PitchSize"),
)
_INFO_BASE: Final = f"{_ROOT}/MatchInformation"
_MATCH_COLUMNS: Final = XmlColumns(
    attributes={
        path: kind
        for path, kind in _under(METADATA_ATTRIBUTES, _INFO_BASE).items()
        if not path.startswith(("Teams", "Referees"))
    },
    elements=("General", "Environment", "OtherGameInformation"),
)
_TEAM_COLUMNS: Final = XmlColumns(
    attributes={p: k for p, k in _under(METADATA_ATTRIBUTES, f"{_INFO_BASE}/Teams/Team").items() if p.startswith("@")}
)
_PERSON_COLUMNS: Final = XmlColumns(
    attributes=_merge(*(_under(METADATA_ATTRIBUTES, f"{_INFO_BASE}/{p}") for p in _PERSON_PARENTS.values()))
)
_EVENT_FIELDS: Final = dict(
    sorted(_under(EVENTS_ATTRIBUTES, f"{_ROOT}/Event").items(), key=lambda item: (not item[0].startswith("@"), item[0]))
)  # event-level attributes first
_EVENT_ELEMENTS: Final = tuple(path[len(f"{_ROOT}/Event/") :] for path in EVENTS_ELEMENTS if path.count("/") >= 2)
_EVENT_COLUMNS: Final = XmlColumns(
    attributes=_EVENT_FIELDS,
    presence=tuple(path for path in _EVENT_ELEMENTS if "/" in path),
    elements=tuple(path for path in _EVENT_ELEMENTS if "/" not in path),
)
_FRAME_KINDS: Final = _under(POSITIONS_ATTRIBUTES, _FRAME_PATH)
_FRAMESET_KINDS: Final = {
    path: kind for path, kind in _under(POSITIONS_ATTRIBUTES, _FRAMESET_PATH).items() if path.startswith("@")
}


def _artifact(name: str) -> Artifact:
    if name not in _FIELDS:
        raise ValueError(f"unknown IDSSE artifact {name!r}")
    return name


def _tree(decoded: bytes, artifact: str) -> Element:
    try:
        return defusedxml.ElementTree.fromstring(decoded, forbid_dtd=True)
    except defusedxml.DefusedXmlException as exc:
        raise UnexpectedPayloadError(f"IDSSE {artifact} XML refused: {type(exc).__name__} ({exc})") from exc
    except defusedxml.ElementTree.ParseError as exc:
        raise UnexpectedPayloadError(f"IDSSE {artifact} XML is malformed: {exc}") from exc


def _strip_children(element: Element) -> dict[str, str]:
    return {f"@{name}": value for name, value in element.attrib.items()}


@dataclass
class _FrameSet:
    section: str
    period: int
    team_id: str
    person_id: str
    is_ball: bool
    is_team: bool


class _PositionsScan(ContentHandler):
    """One streaming pass over a positions file (see the module docstring). `collect=False` gathers the inventory
    only."""

    def __init__(
        self, *, collect: bool, team_ids: frozenset[str] = frozenset(), non_team_persons: frozenset[str] = frozenset()
    ) -> None:
        super().__init__()
        self.collect = collect
        self.team_ids = team_ids
        self.non_team_persons = non_team_persons
        self.paths: set[str] = set()
        self.header: dict[str, str] = {}
        self.framesets: list[_FrameSet] = []
        self.frame_clock: dict[tuple[int, int], tuple[str, str]] = {}
        self.ball: dict[tuple[int, int], dict[str, str]] = {}
        self.first_frame: dict[int, int] = {}
        self.player_frames = 0
        self.non_team_rows: list[dict[str, Any]] = []
        self._stack: list[str] = []
        self._seen: set[tuple[str, tuple[str, ...]]] = set()
        self._current: _FrameSet | None = None
        self._locator: Locator | None = None
        self._mm: dict[str, int] = {}
        self._floats: set[str] = set()

    def setDocumentLocator(self, locator: Locator) -> None:  # noqa: N802 - SAX API name
        self._locator = locator

    def _where(self) -> str:
        return f"positions: line {self._locator.getLineNumber() if self._locator is not None else -1}"

    def startElement(self, name: str, attrs: AttributesImpl) -> None:  # noqa: N802 - SAX API name
        self._stack.append(name)
        path = "/".join(self._stack)
        names = tuple(attrs.getNames())
        if (path, names) not in self._seen:
            self._seen.add((path, names))
            self.paths.add(path)
            self.paths.update(f"{path}@{attribute}" for attribute in names)
        if not self.collect:
            return
        if path == _FRAME_PATH:
            self._frame(attrs)
        elif path == _FRAMESET_PATH:
            self._frameset(attrs)
        elif path in (
            _ROOT,
            f"{_ROOT}/Positions",
            f"{_ROOT}/Positions/MetaData",
            f"{_ROOT}/Positions/MetaData/PitchSize",
        ):
            relative = path[len(_ROOT) + 1 :] if path != _ROOT else ""
            self.header.update({f"{relative}@{attribute}": attrs.getValue(attribute) for attribute in names})

    def endElement(self, name: str) -> None:  # noqa: N802 - SAX API name
        self._stack.pop()
        if name == "FrameSet":
            self._current = None

    def characters(self, content: str) -> None:
        if content.strip() and self._stack:
            self.paths.add("/".join(self._stack) + "#text")

    def _frameset(self, attrs: AttributesImpl) -> None:
        section = attrs.get("GameSection", "")
        if section not in GAME_SECTIONS:
            raise UnexpectedPayloadError(f"{self._where()}: unknown GameSection {section!r} (silly-kicks skips it)")
        match_id = attrs.get("MatchId", "")
        if match_id != self.header.get("Positions/MetaData@MatchId"):
            raise IntegrityError(f"{self._where()}: FrameSet MatchId {match_id!r} differs from the file's MetaData")
        team_id, person_id = attrs.get("TeamId", ""), attrs.get("PersonId", "")
        is_ball = team_id == BALL_TEAM
        is_team = team_id in self.team_ids
        if not is_ball and not is_team and person_id not in self.non_team_persons:
            raise UnexpectedPayloadError(
                f"{self._where()}: the FrameSet of {person_id} has TeamId {team_id!r}, neither the ball nor a team of "
                "the match, and the metadata names no such person; v2 refuses an untraceable tracked object"
            )
        self._current = _FrameSet(section, GAME_SECTIONS[section], team_id, person_id, is_ball, is_team)
        self.framesets.append(self._current)

    def _check_mm(self, attribute: str, text: str) -> None:
        if text not in self._mm:
            try:
                self._mm[text] = text_to_mm(text)
            except InexactValueError as exc:
                raise InexactValueError(f"{self._where()}: {attribute}={text!r}: {exc}") from None

    def _check_float(self, attribute: str, text: str) -> None:
        if text in self._floats:
            return
        try:
            convert(text, "float")
        except InexactValueError as exc:
            raise InexactValueError(f"{self._where()}: {attribute}={text!r}: {exc}") from None
        decimals = len(text.partition(".")[2])
        if decimals > _SILLY_KICKS_DECIMALS:
            raise InexactValueError(
                f"{self._where()}: {attribute}={text!r} has {decimals} decimals; "
                f"silly-kicks keeps {_SILLY_KICKS_DECIMALS}"
            )
        self._floats.add(text)

    def _frame(self, attrs: AttributesImpl) -> None:
        current = self._current
        if current is None:  # the inventory check reports a Frame outside a FrameSet
            return
        values = {attribute: attrs.getValue(attribute) for attribute in attrs.getNames()}
        try:
            n = cast("int", convert(values.get("N", ""), "int"))
        except InexactValueError as exc:
            raise InexactValueError(f"{self._where()}: N: {exc}") from None
        if "X" not in values or "Y" not in values:
            if not current.is_ball:
                raise UnexpectedPayloadError(
                    f"{self._where()}: a player Frame without X and Y (N={n}); silly-kicks drops such frames"
                )
        for attribute in ("X", "Y", "Z"):
            if attribute in values:
                self._check_mm(attribute, values[attribute])
        for attribute in ("S", "A", "D"):
            if attribute in values:
                self._check_float(attribute, values[attribute])
        key = (current.period, n)
        clock = (values.get("T", ""), values.get("M", ""))
        known = self.frame_clock.get(key)
        if known is None:
            for attribute, text in zip(("T", "M"), clock, strict=True):
                if text:
                    try:
                        convert(text, "ts" if attribute == "T" else "int")
                    except InexactValueError as exc:
                        raise InexactValueError(f"{self._where()}: {attribute}: {exc}") from None
            self.frame_clock[key] = clock
        else:
            for attribute, seen, text in zip(("T", "M"), known, clock, strict=True):
                if seen != text:
                    raise IntegrityError(
                        f"{self._where()}: {attribute} differs between FrameSets at {current.section} frame {n}: "
                        f"{seen!r} and {text!r}"
                    )
        first = self.first_frame.get(current.period)
        if first is None or n < first:
            self.first_frame[current.period] = n
        if current.is_ball:
            for attribute in ("BallPossession", "BallStatus"):
                if attribute in values:
                    convert(values[attribute], "int")
            self.ball[key] = values
        elif current.is_team:
            self.player_frames += 1
        else:  # a referee or official: silly-kicks drops it, so v2 keeps its own frame row (team null)
            self.non_team_rows.append(
                {
                    "period": current.period,
                    "frame": n,
                    "person_id": current.person_id,
                    "x": values.get("X"),
                    "y": values.get("Y"),
                    "s": values.get("S"),
                    "a": values.get("A"),
                    "d": values.get("D"),
                }
            )


def _scan(
    decoded: bytes,
    *,
    collect: bool,
    team_ids: frozenset[str] = frozenset(),
    non_team_persons: frozenset[str] = frozenset(),
) -> _PositionsScan:
    handler = _PositionsScan(collect=collect, team_ids=team_ids, non_team_persons=non_team_persons)
    try:
        defusedxml.sax.parse(io.BytesIO(decoded), handler, forbid_dtd=True)
    except defusedxml.DefusedXmlException as exc:
        raise UnexpectedPayloadError(f"IDSSE positions XML refused: {type(exc).__name__} ({exc})") from exc
    except SAXParseException as exc:
        raise UnexpectedPayloadError(f"IDSSE positions XML is malformed: {exc}") from exc
    return handler


def _mm_or_none(text: str | None) -> int | None:
    return None if text is None else text_to_mm(text)


def _typed(text: str | None, kind: XmlKind) -> Any:
    return None if text is None else convert(text, kind)


def _keys(rows: int, native: str) -> dict[str, pa.Array[Any]]:
    return {
        "provider": constant(PROVIDER, rows, _STR),
        "native_match_id": constant(native, rows, _STR),
        "match_key": constant(match_key(PROVIDER, native), rows, _I64),
    }


def _player_key(raw: object) -> int | None:
    return player_key(PROVIDER, idsse_native_player_id(str(raw)))


def _team_key(raw: object) -> int | None:
    return team_key(PROVIDER, idsse_native_team_id(str(raw)))


class IdsseParser:
    """The IDSSE `ProviderParser`. It never knows which channel delivered the files (spec §6)."""

    @property
    def provider(self) -> Literal["idsse"]:
        return "idsse"

    @property
    def version(self) -> str:
        """The version of this parser's bronze output: bump `PARSER_VERSION` with any change that alters bronze, and
        `lakehouse reparse --all` re-parses the matches of an older version from the raw archive (spec §13.2)."""
        return PARSER_VERSION

    def bronze_columns(self, artifact: str) -> frozenset[str]:
        attributes, elements = _FIELDS[_artifact(artifact)]
        return frozenset(attributes) | frozenset(elements)

    def field_inventory(self, artifact: str, decoded: bytes) -> frozenset[str]:
        if _artifact(artifact) == "positions":
            return frozenset(_scan(decoded, collect=False).paths)
        return xml_field_paths(_tree(decoded, artifact), "")

    def parse(self, entry: SourceEntry, deliveries: Mapping[str, Delivery], *, run_id: str) -> ParsedMatch:
        native = idsse_native_match_id(entry.native_match_id)
        metadata = deliveries.get("metadata")
        if metadata is None:
            raise IntegrityError(f"IDSSE match {native} has no metadata delivery (names, teams and pitch)")
        at = datetime.now(UTC)
        tables: dict[BronzeTable, pa.Table] = {}
        files: list[pa.Table] = []

        info_root = _tree(metadata.decoded, "metadata")
        check_mapped("metadata", xml_field_paths(info_root, ""), self.bronze_columns("metadata"))
        meta_tables, pitch = self._metadata(info_root, native)
        for name, table in meta_tables.items():
            tables[name] = with_audit(table, run_id=run_id, source_hash=metadata.content_sha256, at=at)
        files.append(self._file_row("metadata", _strip_children(info_root), native, metadata, run_id, at))
        team_ids = frozenset(cast("list[str]", tables[TEAMS].column("team_id").to_pylist()))
        coordinate_systems: dict[str, CoordinateSystem] = {}

        events = deliveries.get("events")
        if events is not None:
            root = _tree(events.decoded, "events")
            check_mapped("events", xml_field_paths(root, ""), self.bronze_columns("events"))
            table = self._events(root)
            table = append_columns(table, _keys(table.num_rows, native))
            tables[EVENTS] = with_audit(table, run_id=run_id, source_hash=events.content_sha256, at=at)
            files.append(self._file_row("events", _strip_children(root), native, events, run_id, at))
            coordinate_systems["events"] = _event_coordinates(*pitch)

        positions = deliveries.get("positions")
        if positions is not None:
            non_team_persons = frozenset(
                str(pid)
                for pid, kind in zip(
                    tables[PERSONS].column("person_id").to_pylist(),
                    tables[PERSONS].column("person_kind").to_pylist(),
                    strict=True,
                )
                if kind != "player"
            )
            scan = _scan(positions.decoded, collect=True, team_ids=team_ids, non_team_persons=non_team_persons)
            check_mapped("positions", frozenset(scan.paths), self.bronze_columns("positions"))
            players = self._players(scan, metadata.decoded, positions.decoded, native)
            tables[TRACKING_PLAYERS] = players
            tables[TRACKING_FRAMES] = _frames(scan, native)
            roster = append_columns(_roster(scan), _keys(len(scan.framesets), native))
            tables[TRACKING_ROSTER] = with_audit(roster, run_id=run_id, source_hash=positions.content_sha256, at=at)
            files.append(self._file_row("positions", scan.header, native, positions, run_id, at))
            coordinate_systems["positions"] = _tracking_coordinates(
                _mm_or_none(scan.header.get("Positions/MetaData/PitchSize@X")),
                _mm_or_none(scan.header.get("Positions/MetaData/PitchSize@Y")),
            )

        tables[FILES] = pa.concat_tables(files)
        return ParsedMatch(tables=tables, coordinate_systems=coordinate_systems, names=self._names(tables))

    @staticmethod
    def _names(tables: Mapping[BronzeTable, pa.Table]) -> NamesBundle:
        persons = tables[PERSONS]
        is_player = pa.array([kind == "player" for kind in persons.column("person_kind").to_pylist()], pa.bool_())
        info = tables[MATCH_INFORMATION]
        competitions = pa.table(
            {
                "competition_id": pa.array(info.column("general_competition_id").to_pylist(), _STR),
                "competition_name": pa.array(info.column("general_competition_name").to_pylist(), _STR),
                "season_id": pa.array(info.column("general_season_id").to_pylist(), _STR),
                "season_name": pa.array(info.column("general_season").to_pylist(), _STR),
            }
        )
        return NamesBundle(
            players=persons.filter(is_player).select(["person_id", "first_name", "last_name", "shortname", "team_id"]),
            teams=tables[TEAMS].select(["team_id", "team_name"]),
            competitions=competitions,
        )

    @staticmethod
    def _file_row(
        artifact: Artifact, header: Mapping[str, str], native: str, delivery: Delivery, run_id: str, at: datetime
    ) -> pa.Table:
        table = _FILE_COLUMNS.table([header], artifact=artifact)
        table = table.add_column(0, "artifact", pa.array([artifact], _STR))
        table = append_columns(table, _keys(1, native))
        return with_audit(table, run_id=run_id, source_hash=delivery.content_sha256, at=at)

    @staticmethod
    def _metadata(root: Element, native: str) -> tuple[dict[BronzeTable, pa.Table], tuple[int | None, int | None]]:
        info = root.find("MatchInformation")
        if info is None:
            raise UnexpectedPayloadError("IDSSE metadata has no MatchInformation element")
        row: dict[str, str] = {}
        for child in info:
            if child.tag in ("Teams", "Referees"):
                continue
            row[child.tag] = ""
            for path, value in element_values(child).items():
                row[f"{child.tag}{path}" if path.startswith("@") else f"{child.tag}/{path}"] = value
        match = _MATCH_COLUMNS.table([row], artifact="metadata")
        competition = cast("str | None", match.column("general_competition_id")[0].as_py())
        season = cast("str | None", match.column("general_season_id")[0].as_py())
        native_competition = idsse_native_competition_id(competition) if competition else None
        match = append_columns(
            match,
            {
                **_keys(1, native),
                "competition_key": key_array([native_competition], lambda c: competition_key(PROVIDER, c)),
                "season_key": key_array(
                    [native_season_id(PROVIDER, native_competition, season) if native_competition and season else None],
                    lambda s: season_key(PROVIDER, s),
                ),
                "home_team_key": key_array(match.column("general_home_team_id").to_pylist(), _team_key),
                "guest_team_key": key_array(match.column("general_guest_team_id").to_pylist(), _team_key),
            },
        )

        teams = info.findall("Teams/Team")
        team_table = _TEAM_COLUMNS.table([_strip_children(team) for team in teams], artifact="metadata")
        team_table = append_columns(
            team_table,
            {**_keys(len(teams), native), "team_key": key_array(team_table.column("team_id").to_pylist(), _team_key)},
        )

        person_rows: list[dict[str, str]] = []
        kinds: list[str] = []
        team_of: list[str | None] = []
        for team in teams:
            for tag, parent in _PERSON_PARENTS.items():
                if tag == "Referee":
                    continue
                for person in team.findall(parent.removeprefix("Teams/Team/")):
                    person_rows.append(_strip_children(person))
                    kinds.append(_PERSON_KINDS[tag])
                    team_of.append(team.get("TeamId"))
        for referee in info.findall(_PERSON_PARENTS["Referee"]):
            person_rows.append(_strip_children(referee))
            kinds.append("referee")
            team_of.append(None)
        persons = _PERSON_COLUMNS.table(person_rows, artifact="metadata")
        persons = persons.add_column(0, "person_kind", pa.array(kinds, _STR))
        persons = persons.add_column(1, "team_id", pa.array(team_of, _STR))
        persons = append_columns(
            persons,
            {
                **_keys(len(person_rows), native),
                "player_key": key_array(persons.column("person_id").to_pylist(), _player_key),
                "team_key": key_array(team_of, _team_key),
            },
        )
        pitch = (
            cast("int | None", match.column("environment_pitch_x_mm")[0].as_py()),
            cast("int | None", match.column("environment_pitch_y_mm")[0].as_py()),
        )
        return {MATCH_INFORMATION: match, TEAMS: team_table, PERSONS: persons}, pitch

    @staticmethod
    def _events(root: Element) -> pa.Table:
        rows: list[dict[str, str]] = []
        types: list[str] = []
        for event in root:
            children = list(event)
            if len(children) != 1:
                raise UnexpectedPayloadError(
                    f"IDSSE event {event.get('EventId')} has {len(children)} child elements; one (its type) is expected"
                )
            types.append(children[0].tag)
            rows.append(element_values(event))
        table = _EVENT_COLUMNS.table(rows, artifact="events")
        table = table.add_column(0, "event_index", pa.array(range(len(rows)), pa.int32()))
        table = table.add_column(1, "event_type", pa.array(types, _STR))
        return append_columns(
            table,
            {
                "player_key": key_array([r.get(f"{t}@Player") for r, t in zip(rows, types, strict=True)], _player_key),
                "team_key": key_array([r.get(f"{t}@Team") for r, t in zip(rows, types, strict=True)], _team_key),
            },
        )

    @staticmethod
    def _players(scan: _PositionsScan, metadata: bytes, positions: bytes, native: str) -> pa.Table:
        with tempfile.TemporaryDirectory(prefix="lakehouse-idsse-") as work:
            info_path, positions_path = Path(work, "matchinformation.xml"), Path(work, "positions.xml")
            info_path.write_bytes(metadata)
            positions_path.write_bytes(positions)
            frame: SportecTracking = parse_tracking(positions_path, info_path, match_id=native)
        rows = len(frame)
        if rows != scan.player_frames:
            raise IntegrityError(
                f"silly-kicks returned {rows} player rows, but the positions file has {scan.player_frames} "
                "player frames"
            )
        periods, frames = frame.ints("period"), frame.ints("frame")
        first = np.array([scan.first_frame[int(p)] for p in periods], dtype=np.int64)
        clock_ms = frame.floats("timestamp") * 1000
        if not np.array_equal(np.rint(clock_ms).astype(np.int64), (frames - first) * FRAME_MS):
            raise IntegrityError("silly-kicks timestamps disagree with the frame numbers of the positions file")
        ids = frame.texts("player_id")
        known = {pid: _player_key(pid) for pid in set(ids)}
        keys = [known[pid] for pid in ids]

        def floats(column: str) -> pa.Array[Any]:
            return pa.array(frame.floats(column), pa.float64(), from_pandas=True)

        players = pa.table(
            {
                "match_key": constant(match_key(PROVIDER, native), rows, _I64),
                "period": pa.array(periods, pa.int8()),
                "frame": pa.array(frames, pa.int32()),
                "player_key": pa.array(keys, _I64),
                "x_mm": column_to_mm(floats("x"), source="float64"),
                "y_mm": column_to_mm(floats("y"), source="float64"),
                "is_visible": pa.nulls(rows, pa.bool_()),
                "speed": floats("s"),
                "acceleration": floats("a"),
                "distance": floats("d"),
            }
        )
        return _append_non_team(players, scan, native)


def _append_non_team(players: pa.Table, scan: _PositionsScan, native: str) -> pa.Table:
    """Append the referee/official tracking rows v2 parsed itself (silly-kicks drops them). Their team is null in
    the roster; here they are keyed by their own person (spec §12 completeness; plan Task 31)."""
    rows = scan.non_team_rows
    if not rows:
        return players

    def _f(value: object) -> float | None:
        return None if value is None else float(cast("str", value))

    non_team = pa.table(
        {
            "match_key": constant(match_key(PROVIDER, native), len(rows), _I64),
            "period": pa.array([r["period"] for r in rows], pa.int8()),
            "frame": pa.array([r["frame"] for r in rows], pa.int32()),
            "player_key": pa.array([_player_key(r["person_id"]) for r in rows], _I64),
            "x_mm": pa.array([text_to_mm(r["x"]) for r in rows], pa.int32()),
            "y_mm": pa.array([text_to_mm(r["y"]) for r in rows], pa.int32()),
            "is_visible": pa.nulls(len(rows), pa.bool_()),
            "speed": pa.array([_f(r["s"]) for r in rows], pa.float64()),
            "acceleration": pa.array([_f(r["a"]) for r in rows], pa.float64()),
            "distance": pa.array([_f(r["d"]) for r in rows], pa.float64()),
        }
    )
    return pa.concat_tables([players, non_team])


def _frames(scan: _PositionsScan, native: str) -> pa.Table:
    keys = sorted(scan.frame_clock)
    rows = len(keys)
    ball = [scan.ball.get(key, {}) for key in keys]
    clocks = [_typed(scan.frame_clock[key][0] or None, "ts") for key in keys]

    def ball_values(attribute: str, kind: XmlKind) -> list[Any]:
        return [_typed(values.get(attribute), kind) for values in ball]

    return pa.table(
        {
            "match_key": constant(match_key(PROVIDER, native), rows, _I64),
            "period": pa.array([p for p, _ in keys], pa.int8()),
            "frame": pa.array([n for _, n in keys], pa.int32()),
            "timestamp": pa.array(
                [timedelta(milliseconds=(n - scan.first_frame[p]) * FRAME_MS) for p, n in keys], pa.duration("us")
            ),
            "ball_x_mm": pa.array(ball_values("X", "mm"), pa.int32()),
            "ball_y_mm": pa.array(ball_values("Y", "mm"), pa.int32()),
            "ball_z_mm": pa.array(ball_values("Z", "mm"), pa.int32()),
            "ball_detected": pa.nulls(rows, pa.bool_()),
            "wall_clock": pa.array([c[0] if c else None for c in clocks], pa.timestamp("ms", tz="UTC")),
            "wall_clock_utc_offset_min": pa.array([c[1] if c else None for c in clocks], pa.int16()),
            "minute": pa.array([_typed(scan.frame_clock[key][1] or None, "int") for key in keys], pa.int16()),
            "ball_speed": pa.array(ball_values("S", "float"), pa.float64()),
            "ball_acceleration": pa.array(ball_values("A", "float"), pa.float64()),
            "ball_distance": pa.array(ball_values("D", "float"), pa.float64()),
            "ball_possession": pa.array(ball_values("BallPossession", "int"), pa.int8()),
            "ball_status": pa.array(ball_values("BallStatus", "int"), pa.int8()),
        }
    )


def _roster(scan: _PositionsScan) -> pa.Table:
    sets = scan.framesets
    return pa.table(
        {
            "game_section": pa.array([s.section for s in sets], _STR),
            "period": pa.array([s.period for s in sets], pa.int8()),
            "team_id": pa.array([s.team_id for s in sets], _STR),
            "person_id": pa.array([s.person_id for s in sets], _STR),
            "player_key": key_array([s.person_id for s in sets], _player_key),
            "team_key": key_array([s.team_id if s.is_team else None for s in sets], _team_key),
        }
    )


_SOURCE: Final = (
    "Bassek et al. 2025, Sci Data 12:195, doi:10.1038/s41597-025-04505-y, 'Data Records' (coordinates in m); axis "
    "directions measured on the 7 public matches (tests/fixtures/idsse/README.md)"
)


def _metres(mm: int | None) -> float | None:
    return None if mm is None else mm / 1000


def _tracking_coordinates(length_mm: int | None, width_mm: int | None) -> CoordinateSystem:
    return CoordinateSystem(
        origin="centre",
        units="m",
        pitch_length=_metres(length_mm),
        pitch_width=_metres(width_mm),
        x_direction="along the pitch length, -length/2 to +length/2; +x points to the goal that the team in the "
        "events' KickOff@TeamRight defends in that game section",
        y_direction="across the pitch width, -width/2 to +width/2; +y is on the left hand of a team that attacks "
        "towards +x",
        play_direction="fixed axes for both teams; the teams change ends between game sections (KickOff@TeamLeft and "
        "@TeamRight)",
        source_ref=f"positions MetaData/PitchSize; {_SOURCE}",
    )


def _event_coordinates(length_mm: int | None, width_mm: int | None) -> CoordinateSystem:
    return CoordinateSystem(
        origin="bottom_left",
        units="m",
        pitch_length=_metres(length_mm),
        pitch_width=_metres(width_mm),
        x_direction="along the pitch length, 0 to length; +x points to the goal that the team in KickOff@TeamRight "
        "defends in that game section",
        y_direction="across the pitch width, 0 to width; +y is on the left hand of a team that attacks towards +x",
        play_direction="fixed axes for both teams, not normalised per team; the teams change ends between game "
        "sections (KickOff@TeamLeft and @TeamRight)",
        source_ref=f"matchinformation Environment@PitchX and @PitchY; {_SOURCE}",
    )


_AUDIT_DOCS: Final = [
    ("_ingested_at", "When the run parsed the delivery (UTC)."),
    ("_run_id", "The run that wrote the row."),
    ("_source_hash", "The content_sha256 of the delivered file that the row comes from (spec §13.1)."),
]
_MATCH_KEY_DOCS: Final = [
    ("provider", "The provider name (`idsse`)."),
    ("native_match_id", "The canonical native match id, the bare DFL MatchId such as `J03WPY` (Task 2)."),
    ("match_key", "Surrogate match key: xxhash64 of `idsse|native_match_id`, seed 42, signed (spec §14)."),
]
_PLAYER_TRACKING_DOCS: Final = [
    ("match_key", "Surrogate match key (spec §14)."),
    (
        "period",
        "The period of the FrameSet's GameSection (1 firstHalf, 2 secondHalf, 3 and 4 extra time, 5 penalties).",
    ),
    ("frame", "The frame number `N`."),
    ("player_key", "Surrogate person key of the FrameSet's `PersonId` (spec §14)."),
    ("x_mm", "Source attribute `Frame@X` (metres), as exact int32 millimetres (spec §10.2)."),
    ("y_mm", "Source attribute `Frame@Y` (metres), as exact int32 millimetres (spec §10.2)."),
    ("is_visible", "Always null: IDSSE has no detection flag, and v2 never guesses one (spec §11.5)."),
    ("speed", "Source attribute `Frame@S`: speed in km/h, as float64."),
    ("acceleration", "Source attribute `Frame@A`: acceleration in m/s2, as float64."),
    ("distance", "Source attribute `Frame@D`: distance covered since the preceding frame in cm, as float64."),
]
_FRAME_TRACKING_DOCS: Final = [
    ("match_key", "Surrogate match key (spec §14)."),
    ("period", "The period of the GameSection."),
    ("frame", "The frame number `N`."),
    ("timestamp", "Time since the first frame of the period: (N - first N) x 40 ms (25 frames per second)."),
    ("ball_x_mm", "The ball's `Frame@X` (metres), as exact int32 millimetres; null without a ball frame."),
    ("ball_y_mm", "The ball's `Frame@Y` (metres), as exact int32 millimetres; null without a ball frame."),
    ("ball_z_mm", "The ball's `Frame@Z` (height, metres), as exact int32 millimetres."),
    ("ball_detected", "Always null: IDSSE has no ball detection flag."),
    ("wall_clock", "Source attribute `Frame@T` as the UTC instant; every FrameSet agrees on it (spec §10.2)."),
    ("wall_clock_utc_offset_min", "The UTC offset in minutes that `Frame@T` was written with."),
    ("minute", "Source attribute `Frame@M`: the minute of play; every FrameSet agrees on it."),
    ("ball_speed", "The ball's `Frame@S`: speed in km/h, as float64."),
    ("ball_acceleration", "The ball's `Frame@A`: acceleration in m/s2, as float64."),
    ("ball_distance", "The ball's `Frame@D`: distance since the preceding frame in cm, as float64."),
    ("ball_possession", "The ball's `Frame@BallPossession`: 1 home team, 2 away team."),
    ("ball_status", "The ball's `Frame@BallStatus`: 0 ball inactive, 1 ball active."),
]


def bronze_docs() -> list[tuple[BronzeTable, str, list[tuple[str, str]]]]:
    """Every bronze table with the description of each of its columns, in table order (spec §12.8)."""
    info = f"{_ROOT}/MatchInformation"
    return [
        (
            FILES,
            "One row per delivered file: the header attributes of the matchinformation, events or positions file.",
            [
                ("artifact", "The delivered file: `metadata`, `events` or `positions`."),
                *_FILE_COLUMNS.describe(_ROOT),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            MATCH_INFORMATION,
            "One row per match: General, Environment and OtherGameInformation of the matchinformation file.",
            [
                *_MATCH_COLUMNS.describe(info),
                *_MATCH_KEY_DOCS,
                ("competition_key", "Surrogate competition key of `general_competition_id` (spec §14)."),
                ("season_key", "Surrogate season key of `general_competition_id:general_season_id` (spec §14)."),
                ("home_team_key", "Surrogate team key of `general_home_team_id` (spec §14)."),
                ("guest_team_key", "Surrogate team key of `general_guest_team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            TEAMS,
            "One row per team of the matchinformation file.",
            [
                *_TEAM_COLUMNS.describe(f"{info}/Teams/Team"),
                *_MATCH_KEY_DOCS,
                ("team_key", "Surrogate team key of `team_id` (spec §14)."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            PERSONS,
            "One row per person of the matchinformation file: players, trainers, officials and referees. Names come "
            "from here (spec §12.7).",
            [
                ("person_kind", "The element of the person: `player`, `trainer`, `official` or `referee`."),
                ("team_id", "The `TeamId` of the enclosing `Team` element; null for a referee."),
                *_PERSON_COLUMNS.describe(f"{info}/<Player|Trainer|Official|Referee>"),
                *_MATCH_KEY_DOCS,
                ("player_key", "Surrogate person key of `person_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id` (spec §14); null for a referee."),
                *_AUDIT_DOCS,
            ],
        ),
        (
            EVENTS,
            "One row per Event element of the events file, in file order. Event positions are pitch-origin metres "
            "(spec §11.4).",
            [
                ("event_index", "The 0-based position of the Event element in the file."),
                ("event_type", "The tag of the Event's one child element, for example `Play` or `ShotAtGoal`."),
                *_EVENT_COLUMNS.describe(f"{_ROOT}/Event"),
                ("player_key", "Surrogate player key of the event type element's `Player` attribute; null without."),
                ("team_key", "Surrogate team key of the event type element's `Team` attribute; null without."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
        (
            TRACKING_PLAYERS,
            "One row per player frame of the positions file, from the silly-kicks parse (spec §11.5, §12.7). "
            "Coordinates are centre-origin; the file registry holds the audit values (spec §11.2).",
            list(_PLAYER_TRACKING_DOCS),
        ),
        (
            TRACKING_FRAMES,
            "One row per frame (period, N) of any FrameSet of the positions file, with the ball's values where the "
            "ball has that frame (spec §10.2, §11.5).",
            list(_FRAME_TRACKING_DOCS),
        ),
        (
            TRACKING_ROSTER,
            "One row per FrameSet of the positions file: the native person (or ball object) id behind each "
            "player_key, per game section (spec §11.2).",
            [
                ("game_section", f"Source attribute `{_FRAMESET_PATH}@GameSection`, as string."),
                ("period", "The period of the GameSection."),
                ("team_id", f"Source attribute `{_FRAMESET_PATH}@TeamId`: a DFL team id, or `BALL`."),
                ("person_id", f"Source attribute `{_FRAMESET_PATH}@PersonId`: the person or the ball object."),
                ("player_key", "Surrogate person key of `person_id` (spec §14)."),
                ("team_key", "Surrogate team key of `team_id`; null for the ball."),
                *_MATCH_KEY_DOCS,
                *_AUDIT_DOCS,
            ],
        ),
    ]
