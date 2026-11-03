"""The IDSSE (DFL) parser (spec §10.2, §11, §12; plan Task 16). Fixtures: real slices of match J03WPY."""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pytest
from defusedxml.ElementTree import fromstring
from idssekit import ENTRY, FILES_BY_ARTIFACT, delivery, fixture_bytes, parse_with

from lakehouse.bronze.arrow import column_types, read_parquet, write_parquet
from lakehouse.bronze.schemas import AUDIT_COLUMNS, TRACKING_FRAMES_CORE, TRACKING_PLAYERS_CORE
from lakehouse.bronze.tracking import check_tracking_schema
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError
from lakehouse.ids.keys import match_key, player_key, team_key
from lakehouse.parsers import idsse
from lakehouse.parsers.base import CoordinateSystem, ParsedMatch
from lakehouse.parsers.idsse import (
    EVENTS,
    FILES,
    GAME_SECTIONS,
    MATCH_INFORMATION,
    PERSONS,
    TEAMS,
    TRACKING_FRAMES,
    TRACKING_PLAYERS,
    TRACKING_ROSTER,
    IdsseParser,
)
from lakehouse.parsers.sportec import SportecTracking, parse_tracking, silly_kicks_game_sections
from lakehouse.raw.archive import Delivery

FIRST_FRAME = (
    b'<Frame N="10000" T="2022-10-15T11:01:29.320+00:00" X="0.57" Y="23.23" D="0.00" S="0.00" A="0.00" M="1"/>'
)


_FRAMESET_PERSON: Final = re.compile(rb'<FrameSet [^>]*PersonId="([^"]+)"')
_FRAME_M: Final = re.compile(rb'GameSection="(\w+)"|<Frame N="(\d+)"[^>]* M="(\d+)"')


def _framesets_text() -> list[tuple[bytes, bytes, bytes]]:
    """(section, N, M) of every Frame, read from the raw text with a regex: an oracle independent of the parser."""
    found: list[tuple[bytes, bytes, bytes]] = []
    section = b""
    for match in _FRAME_M.finditer(fixture_bytes("positions")):
        if match.group(1):
            section = match.group(1)
        else:
            found.append((section, match.group(2), match.group(3)))
    return found


def _positions(old: bytes, new: bytes) -> bytes:
    data = fixture_bytes("positions")
    assert data.count(old) >= 1
    return data.replace(old, new, 1)


@pytest.fixture(scope="module")
def parsed() -> ParsedMatch:
    return parse_with()


def _column(table: pa.Table, name: str) -> list[Any]:
    return table.column(name).to_pylist()


def test_metadata_season_and_competition(parsed: ParsedMatch) -> None:
    info = parsed.tables[MATCH_INFORMATION]
    assert info.num_rows == 1
    assert _column(info, "general_season_id") == ["DFL-SEA-0001K6"]
    assert _column(info, "general_season") == ["2022/2023"]
    assert _column(info, "general_competition_id") == ["DFL-COM-000002"]
    assert _column(info, "general_competition_name") == ["2. Bundesliga"]
    assert _column(info, "general_kickoff_time") == [dt.datetime(2022, 10, 15, 11, 1, 28, 300000, dt.UTC)]
    assert _column(info, "environment_pitch_x_mm") == [105000]
    competitions = parsed.names.competitions.to_pylist()
    assert competitions == [
        {
            "competition_id": "DFL-COM-000002",
            "competition_name": "2. Bundesliga",
            "season_id": "DFL-SEA-0001K6",
            "season_name": "2022/2023",
        }
    ]


def test_idsse_names_unicode_roundtrip(parsed: ParsedMatch, tmp_path: Path) -> None:
    """Review focus 5: names reach bronze unchanged, including letters outside Latin-1 (Vukančić)."""
    root = fromstring(fixture_bytes("metadata"))
    source = {
        (p.get("PersonId"), p.get("FirstName"), p.get("LastName"), p.get("Shortname"))
        for p in root.iter()
        if p.tag in ("Player", "Trainer", "Official", "Referee")
    }
    persons = parsed.tables[PERSONS]
    path = tmp_path / "persons.parquet"
    write_parquet(persons, path, compression_level=3)
    back = read_parquet(path)
    stored = set(
        zip(
            _column(back, "person_id"),
            _column(back, "first_name"),
            _column(back, "last_name"),
            _column(back, "shortname"),
            strict=True,
        )
    )
    assert stored == source
    assert ("DFL-OBJ-J01D0C", "Niko", "Vukančić", "N. Vukančić") in stored
    assert "Fortuna Düsseldorf" in _column(parsed.tables[TEAMS], "team_name")
    players = parsed.names.players.to_pylist()
    assert {"person_id": "DFL-OBJ-J01D0C", "first_name": "Niko", "last_name": "Vukančić"}.items() <= next(
        p for p in players if p["person_id"] == "DFL-OBJ-J01D0C"
    ).items()


def test_persons_kinds_and_teams(parsed: ParsedMatch) -> None:
    persons = parsed.tables[PERSONS]
    kinds = _column(persons, "person_kind")
    assert {k: kinds.count(k) for k in set(kinds)} == {"player": 40, "trainer": 8, "official": 2, "referee": 5}
    referee_teams = {t for t, k in zip(_column(persons, "team_id"), kinds, strict=True) if k == "referee"}
    assert referee_teams == {None}
    assert set(_column(parsed.tables[TEAMS], "team_id")) == {"DFL-CLU-000005", "DFL-CLU-00000P"}


def test_t_equals_ball_t_proof(tmp_path: Path) -> None:
    """§10.2: `t` (each player frame) equals `ball_t` (the ball frame at the same N) on every row of the fixture, so
    one wall clock per frame holds both and nothing is lost when the player `t` is not stored."""
    info, positions = tmp_path / "info.xml", tmp_path / "positions.xml"
    info.write_bytes(fixture_bytes("metadata"))
    positions.write_bytes(fixture_bytes("positions"))
    rows = parse_tracking(positions, info, match_id="J03WPY")
    pairs = [(t, b) for t, b in zip(rows.texts("t"), rows.texts("ball_t"), strict=True) if b is not None]
    assert len(pairs) > 2000
    assert all(t == b for t, b in pairs)


def test_wall_clock_must_agree_across_framesets() -> None:
    """The same proof at run time: a player frame whose T differs from another FrameSet's T at that N fails."""
    changed = _positions(FIRST_FRAME, FIRST_FRAME.replace(b"29.320", b"29.360"))
    with pytest.raises(IntegrityError, match=r"T differs"):
        parse_with(positions=changed)


def test_minute_of_play_kept(parsed: ParsedMatch) -> None:
    """`M` is the minute of play (Bassek et al. 2025). silly-kicks casts it to a boolean and loses it; v2 keeps it
    once per frame, after proving that every FrameSet agrees on it."""
    frames = parsed.tables[TRACKING_FRAMES]
    by_frame = dict(
        zip(
            zip(_column(frames, "period"), _column(frames, "frame"), strict=True),
            _column(frames, "minute"),
            strict=True,
        )
    )
    assert by_frame[(1, 10000)] == 1
    assert by_frame[(2, 100000)] == 46
    raw = {(GAME_SECTIONS[s.decode()], int(n), int(m)) for s, n, m in _framesets_text()}
    assert {(p, n, m) for (p, n), m in by_frame.items()} == raw
    changed = _positions(FIRST_FRAME, FIRST_FRAME.replace(b'M="1"', b'M="2"'))
    with pytest.raises(IntegrityError, match=r"M differs"):
        parse_with(positions=changed)


def test_tracking_players_frames_split(parsed: ParsedMatch) -> None:
    players, frames = parsed.tables[TRACKING_PLAYERS], parsed.tables[TRACKING_FRAMES]
    check_tracking_schema("players", players.schema)
    check_tracking_schema("frames", frames.schema)
    assert players.schema.names[: len(TRACKING_PLAYERS_CORE)] == TRACKING_PLAYERS_CORE.names
    assert frames.schema.names[: len(TRACKING_FRAMES_CORE)] == TRACKING_FRAMES_CORE.names
    assert players.num_rows == 2650  # 53 player FrameSets x 50 frames in the slice
    assert frames.num_rows == 550  # every (period, N) of any FrameSet
    assert sum(v is not None for v in _column(frames, "ball_x_mm")) == 100  # 2 ball FrameSets x 50 frames
    assert set(_column(players, "is_visible")) == {None}  # IDSSE has no detection flag; never a guessed True
    assert set(_column(frames, "ball_detected")) == {None}
    for audit in AUDIT_COLUMNS.names:
        assert audit not in players.schema.names
        assert audit not in frames.schema.names


def test_types_and_exactness(parsed: ParsedMatch) -> None:
    players, frames = parsed.tables[TRACKING_PLAYERS], parsed.tables[TRACKING_FRAMES]
    assert column_types(players.schema)["speed"] == pa.float64()
    key = player_key("idsse", "DFL-OBJ-0000NZ")
    rows = [r for r in players.to_pylist() if r["player_key"] == key and r["frame"] == 10000]
    assert [(r["period"], r["x_mm"], r["y_mm"], r["speed"]) for r in rows] == [(1, 570, 23230, 0.0)]
    first = next(r for r in frames.to_pylist() if (r["period"], r["frame"]) == (1, 10000))
    assert first["wall_clock"] == dt.datetime(2022, 10, 15, 11, 1, 29, 320000, dt.UTC)
    assert first["wall_clock_utc_offset_min"] == 0
    assert first["timestamp"] == dt.timedelta(0)
    assert (first["ball_x_mm"], first["ball_y_mm"], first["ball_z_mm"]) == (6400, -40, 150)
    assert (first["ball_speed"], first["ball_possession"], first["ball_status"]) == (60.77, 1, 1)
    later = next(r for r in frames.to_pylist() if (r["period"], r["frame"]) == (1, 10049))
    assert later["timestamp"] == dt.timedelta(milliseconds=49 * 40)
    events = parsed.tables[EVENTS]
    kickoff = events.to_pylist()[0]
    assert (kickoff["event_type"], kickoff["x_position_mm"], kickoff["y_position_mm"]) == ("KickOff", 52500, 34000)
    assert kickoff["event_time"] == dt.datetime(2022, 10, 15, 11, 1, 28, 310000, dt.UTC)
    assert kickoff["event_time_utc_offset_min"] == 120


def test_xml_attribute_exactness() -> None:
    """§23.6 "IDSSE exactness": a coordinate with more decimals than the millimetre fails on the XML text, even though
    silly-kicks would round 0.570049 to 0.57 and pass a check on its output."""
    changed = _positions(FIRST_FRAME, FIRST_FRAME.replace(b'X="0.57"', b'X="0.570049"'))
    with pytest.raises(InexactValueError, match=r"line \d+.*X"):
        parse_with(positions=changed)
    changed = _positions(FIRST_FRAME, FIRST_FRAME.replace(b'S="0.00"', b'S="0.00001"'))
    with pytest.raises(InexactValueError, match=r"S"):  # silly-kicks keeps 4 decimals
        parse_with(positions=changed)


def test_coordinate_system_idsse(parsed: ParsedMatch) -> None:
    """§11.4: tracking is centre-origin metres, events pitch-origin; the pitch size comes from each file."""
    tracking, events = parsed.coordinate_systems["positions"], parsed.coordinate_systems["events"]
    assert isinstance(tracking, CoordinateSystem)
    assert (tracking.origin, tracking.units, tracking.pitch_length, tracking.pitch_width) == (
        "centre",
        "m",
        105.0,
        68.0,
    )
    assert (events.origin, events.units, events.pitch_length, events.pitch_width) == ("bottom_left", "m", 105.0, 68.0)
    assert "TeamRight" in tracking.x_direction
    assert "left" in tracking.y_direction
    assert "PitchSize" in tracking.source_ref
    assert "PitchX" in events.source_ref
    assert "Bassek" in tracking.source_ref


def test_events_rows_complete(parsed: ParsedMatch) -> None:
    """Every Event element is a row (silly-kicks drops events before the first kick-off), and depth-3 fields stay."""
    events = parsed.tables[EVENTS]
    assert events.num_rows == len(list(fromstring(fixture_bytes("events"))))
    assert _column(events, "event_index") == list(range(events.num_rows))
    assert any(v is not None for v in _column(events, "corner_kick_play_cross_side"))
    assert any(_column(events, "has_corner_kick_play_cross"))


def test_keys_and_audit_columns(parsed: ParsedMatch) -> None:
    key = match_key("idsse", "J03WPY")
    hashes = {artifact: delivery(artifact).content_sha256 for artifact in FILES_BY_ARTIFACT}
    for name, artifact in (
        (MATCH_INFORMATION, "metadata"),
        (TEAMS, "metadata"),
        (PERSONS, "metadata"),
        (EVENTS, "events"),
        (TRACKING_ROSTER, "positions"),
    ):
        table = parsed.tables[name]
        assert set(_column(table, "provider")) == {"idsse"}, name
        assert set(_column(table, "native_match_id")) == {"J03WPY"}, name
        assert set(_column(table, "match_key")) == {key}, name
        assert set(_column(table, "_run_id")) == {"run-1"}, name
        assert set(_column(table, "_source_hash")) == {hashes[artifact]}, name
    files = {r["artifact"]: r for r in parsed.tables[FILES].to_pylist()}
    assert {a: r["_source_hash"] for a, r in files.items()} == hashes
    assert files["positions"]["positions_meta_data_pitch_size_x_mm"] == 105000
    for name in (TRACKING_PLAYERS, TRACKING_FRAMES):
        assert set(_column(parsed.tables[name], "match_key")) == {key}
    teams = parsed.tables[TEAMS]
    assert _column(teams, "team_key") == [team_key("idsse", t) for t in _column(teams, "team_id")]
    roster = parsed.tables[TRACKING_ROSTER]
    assert sorted(_column(roster, "person_id")) == sorted(
        m.decode() for m in _FRAMESET_PERSON.findall(fixture_bytes("positions"))
    )
    assert "BALL" in _column(roster, "team_id")


def test_game_sections_follow_silly_kicks() -> None:
    assert silly_kicks_game_sections() == GAME_SECTIONS


@pytest.mark.parametrize(
    ("old", "new", "error", "pattern"),
    [
        (
            b'TeamId="DFL-CLU-00000P" PersonId="DFL-OBJ-0000NZ"',
            b'TeamId="referee" PersonId="DFL-OBJ-0000NZ"',
            UnexpectedPayloadError,
            "referee",
        ),
        (b'GameSection="firstHalf"', b'GameSection="warmUp"', UnexpectedPayloadError, "warmUp"),
        (FIRST_FRAME, FIRST_FRAME.replace(b' X="0.57"', b""), UnexpectedPayloadError, "X and Y"),
        (
            b'MatchId="DFL-MAT-J03WPY" TeamId="DFL-CLU-00000P"',
            b'MatchId="DFL-MAT-J03WMX" TeamId="DFL-CLU-00000P"',
            IntegrityError,
            "J03WMX",
        ),
    ],
)
def test_positions_shapes_that_silly_kicks_would_drop_fail(
    old: bytes, new: bytes, error: type[Exception], pattern: str
) -> None:
    with pytest.raises(error, match=pattern):
        parse_with(positions=_positions(old, new))


def test_entities_are_refused_before_silly_kicks() -> None:
    bomb = b'<?xml version="1.0"?>\n<!DOCTYPE x [<!ENTITY a "aaaaaaaaaa">]>\n'
    for artifact in FILES_BY_ARTIFACT:
        body = fixture_bytes(artifact).split(b"\n", 1)[1]
        with pytest.raises(UnexpectedPayloadError, match="refused"):
            parse_with(**{artifact: bomb + body})


class _Tampered:
    """Real silly-kicks rows with one simulated fault: one row fewer, or a clock shifted by one frame."""

    def __init__(self, rows: SportecTracking, *, short: bool = False, shift_s: float = 0.0) -> None:
        self._rows, self._short, self._shift_s = rows, short, shift_s

    def __len__(self) -> int:
        return len(self._rows) - (1 if self._short else 0)

    def ints(self, column: str) -> Any:
        return self._rows.ints(column)

    def floats(self, column: str) -> Any:
        values = self._rows.floats(column)
        return values + self._shift_s if column == "timestamp" else values

    def texts(self, column: str) -> list[str | None]:
        return self._rows.texts(column)


@pytest.mark.parametrize(("fault", "pattern"), [({"short": True}, "player rows"), ({"shift_s": 0.04}, "timestamps")])
def test_silly_kicks_disagreement_fails(monkeypatch: pytest.MonkeyPatch, fault: dict[str, Any], pattern: str) -> None:
    def tampered(positions: Path, info: Path, *, match_id: str) -> Any:
        return _Tampered(parse_tracking(positions, info, match_id=match_id), **fault)

    monkeypatch.setattr(idsse, "parse_tracking", tampered)
    with pytest.raises(IntegrityError, match=f"silly-kicks.*{pattern}|{pattern}.*silly-kicks"):
        parse_with()


def test_event_needs_one_type_element() -> None:
    """An Event holds exactly one child, its type; an Event without one fails instead of losing its type."""
    events = fixture_bytes("events")
    first = events.index(b"<Event ")
    end = events.index(b"</Event>", first) + len(b"</Event>")
    head = events[first : events.index(b">", first) + 1]
    with pytest.raises(UnexpectedPayloadError, match="0 child elements"):
        parse_with(events=events[:first] + head.replace(b">", b"/>") + events[end:])


def test_parse_needs_metadata() -> None:
    deliveries: Mapping[str, Delivery] = {"events": delivery("events")}
    with pytest.raises(IntegrityError, match="metadata"):
        IdsseParser().parse(ENTRY, deliveries, run_id="run-1")


_REFEREE_PID: Final = "DFL-OBJ-000235"  # a Referee PersonId in the J03WPY metadata fixture (spec §12, referee tracking)


def _with_referee_frameset() -> bytes:
    """Inject a referee FrameSet: a copy of the first FrameSet relabelled with TeamId 'referee' and a referee's
    PersonId, so the frames line up (same N and T) and only the tracked object differs."""
    data = fixture_bytes("positions")
    match = re.search(rb"<FrameSet\b.*?</FrameSet>", data, re.DOTALL)
    assert match is not None
    block = match.group(0)
    referee = re.sub(rb'TeamId="[^"]+"', b'TeamId="referee"', block, count=1)
    referee = re.sub(rb'PersonId="[^"]+"', b'PersonId="' + _REFEREE_PID.encode() + b'"', referee, count=1)
    return data.replace(block, block + referee, 1)


def test_referee_frameset_ingested() -> None:
    """A FrameSet of a metadata referee is ingested with a null team (spec §12 completeness; plan Task 31 finding),
    not refused. A genuinely unknown tracked object still fails loudly."""
    parsed = parse_with(positions=_with_referee_frameset())
    roster = parsed.tables[TRACKING_ROSTER]
    ids = _column(roster, "person_id")
    assert _REFEREE_PID in ids
    assert _column(roster, "team_key")[ids.index(_REFEREE_PID)] is None  # a referee belongs to no team
    players = parsed.tables[TRACKING_PLAYERS]
    assert player_key("idsse", _REFEREE_PID) in _column(players, "player_key")  # its frames are tracked


def test_unknown_frameset_object_still_refused() -> None:
    data = fixture_bytes("positions")
    match = re.search(rb"<FrameSet\b.*?</FrameSet>", data, re.DOTALL)
    assert match is not None
    block = match.group(0)
    bogus = re.sub(rb'TeamId="[^"]+"', b'TeamId="nobody"', block, count=1)
    bogus = re.sub(rb'PersonId="[^"]+"', b'PersonId="DFL-OBJ-FAKE99"', bogus, count=1)
    with pytest.raises(UnexpectedPayloadError, match="neither the ball nor a team"):
        parse_with(positions=data.replace(block, block + bogus, 1))
