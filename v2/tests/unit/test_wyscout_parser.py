"""The Wyscout parser (spec §11, §12, §29.15; plan Task 19). Fixtures: real slices of the CC BY 4.0 public dataset."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from lakehouse.entries import SourceEntry
from lakehouse.errors import IntegrityError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.ids.keys import match_key, player_key, team_key
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.wyscout import (
    COACHES,
    COMPETITIONS,
    EVENT_NAMES,
    EVENTS,
    LISTING_TABLES,
    MATCH_FORMATIONS,
    MATCH_SUBSTITUTIONS,
    MATCH_TEAMS,
    MATCHES,
    PLAYERS,
    TAG_NAMES,
    TEAMS,
    WyscoutParser,
)
from lakehouse.raw.archive import Delivery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "wyscout"
NOW = dt.datetime(2026, 10, 27, 12, tzinfo=dt.UTC)


def _entry(match: str, competition: str = "28", season: str = "10078") -> SourceEntry:
    return SourceEntry(
        module_id="wyscout_public",
        native_match_id=match,
        competition_id=competition,
        competition_name=None,
        season_id=season,
        season_name=None,
        visibility="public",
        artifacts={},
        listed_at=NOW,
    )


def _delivery(artifact: str, name: str, raw: bytes | None = None) -> Delivery:
    return Delivery.received(
        provider="wyscout",
        channel="public",
        native_match_id="x",
        artifact=artifact,
        raw=(FIXTURES / name).read_bytes() if raw is None else raw,
        visibility="public",
        change_signal=None,
        fetched_at=NOW,
    )


def _parse(match: str = "2058017", *, matches: bytes | None = None, events: bytes | None = None) -> ParsedMatch:
    deliveries = {
        "matches": _delivery("matches", "matches_World_Cup_slice.json", matches),
        "events": _delivery("events", "events_World_Cup_slice.json", events),
    }
    return WyscoutParser().parse(_entry(match), deliveries, run_id="run-1")


def _source(name: str) -> list[dict[str, Any]]:
    return json.loads((FIXTURES / name).read_bytes())


def _column(table: pa.Table, name: str) -> list[Any]:
    return table.column(name).to_pylist()


@pytest.fixture(scope="module")
def parsed() -> ParsedMatch:
    return _parse()


def test_match_teams_formations_and_substitutions(parsed: ParsedMatch) -> None:
    source = next(m for m in _source("matches_World_Cup_slice.json") if m["wyId"] == 2058017)
    assert _column(parsed.tables[MATCHES], "match_id") == [2058017]
    teams = parsed.tables[MATCH_TEAMS]
    assert sorted(_column(teams, "team_id")) == sorted(int(k) for k in source["teamsData"])
    formations = parsed.tables[MATCH_FORMATIONS]
    expected = sum(len(t["formation"][role]) for t in source["teamsData"].values() for role in ("lineup", "bench"))
    assert formations.num_rows == expected
    assert set(_column(formations, "role")) == {"lineup", "bench"}
    subs = parsed.tables[MATCH_SUBSTITUTIONS]
    assert subs.num_rows == sum(len(t["formation"]["substitutions"]) for t in source["teamsData"].values())


def test_substitutions_written_as_text_are_kept() -> None:
    """One source match writes `"substitutions": "null"` (a string) instead of a list; v2 keeps that text."""
    deliveries = {"matches": _delivery("matches", "matches_cover.json")}
    parsed = WyscoutParser().parse(_entry("2500039", "364", "181150"), deliveries, run_id="run-1")
    teams = {r["team_id"]: r for r in parsed.tables[MATCH_TEAMS].to_pylist()}
    assert teams[1628]["substitutions_text"] == "null"
    assert teams[1659]["substitutions_text"] is None
    assert {r["team_id"] for r in parsed.tables[MATCH_SUBSTITUTIONS].to_pylist()} == {1659}
    assert EVENTS not in parsed.tables  # no events delivery


def test_types_and_exactness(parsed: ParsedMatch) -> None:
    match = parsed.tables[MATCHES].to_pylist()[0]
    assert match["dateutc"].tzinfo is not None
    assert match["date"].endswith("GMT+3") or "GMT" in match["date"]
    source = next(m for m in _source("matches_World_Cup_slice.json") if m["wyId"] == 2058017)
    entries = [e for t in source["teamsData"].values() for role in ("lineup", "bench") for e in t["formation"][role]]
    formations = parsed.tables[MATCH_FORMATIONS].to_pylist()
    assert [r["goals"] for r in formations] == [None if e["goals"] == "null" else int(e["goals"]) for e in entries]
    assert [r["yellow_cards"] for r in formations] == [int(e["yellowCards"]) for e in entries]
    assert None in [r["goals"] for r in formations]  # the slice holds the text "null"
    events = parsed.tables[EVENTS]
    source = [e for e in _source("events_World_Cup_slice.json") if e["matchId"] == 2058017]
    assert _column(events, "event_sec") == [e["eventSec"] for e in source]
    assert _column(events, "positions")[0] == source[0]["positions"]
    texts = [r["sub_event_id"] for r in events.to_pylist()]
    assert all(isinstance(t, str) for t in texts)


def test_sub_event_id_text_kept() -> None:
    """`subEventId` is an integer, or the text "" for an event without a subtype; both stay exact as text."""
    other = next(e for e in _source("events_World_Cup_slice.json") if e["subEventId"] == "")
    parsed = _parse(str(other["matchId"]))
    row = next(r for r in parsed.tables[EVENTS].to_pylist() if r["id"] == other["id"])
    assert row["sub_event_id"] == ""


def test_event_sec_integer_accepted() -> None:
    """Some events write `eventSec` as an integer (European Championship); it is the same float64 value."""
    data = (FIXTURES / "events_World_Cup_slice.json").read_bytes()
    first = next(e for e in _source("events_World_Cup_slice.json") if e["matchId"] == 2058017)
    text = b'"eventSec": ' + json.dumps(first["eventSec"]).encode()
    assert data.count(text) == 1
    events = _parse(events=data.replace(text, b'"eventSec": 12')).tables[EVENTS]
    row = next(r for r in events.to_pylist() if r["id"] == first["id"])
    assert row["event_sec"] == 12.0


def test_events_of_the_match_only(parsed: ParsedMatch) -> None:
    assert set(_column(parsed.tables[EVENTS], "match_id")) == {2058017}
    assert parsed.tables[EVENTS].num_rows == sum(
        1 for e in _source("events_World_Cup_slice.json") if e["matchId"] == 2058017
    )


def test_one_parser_serves_every_match_of_a_file() -> None:
    """The parser reads each competition file once and serves its matches from that read (plan Task 19)."""
    parser = WyscoutParser()
    deliveries = {
        "matches": _delivery("matches", "matches_World_Cup_slice.json"),
        "events": _delivery("events", "events_World_Cup_slice.json"),
    }
    source = _source("events_World_Cup_slice.json")
    for match in ("2058017", "2058012", "2058017"):
        events = parser.parse(_entry(match), deliveries, run_id="r").tables[EVENTS]
        assert _column(events, "id") == [e["id"] for e in source if e["matchId"] == int(match)]


def test_a_changed_file_is_read_again() -> None:
    parser = WyscoutParser()
    data = (FIXTURES / "matches_World_Cup_slice.json").read_bytes()
    first = parser.parse(_entry("2058017"), {"matches": _delivery("matches", "x", data)}, run_id="r")
    changed = data.replace(b'"status": "Played"', b'"status": "Replayed"', 1)
    assert changed != data
    second = parser.parse(_entry("2058017"), {"matches": _delivery("matches", "x", changed)}, run_id="r")
    assert _column(first.tables[MATCHES], "status") == ["Played"]
    assert _column(second.tables[MATCHES], "status") == ["Replayed"]


def test_team_key_must_equal_team_id() -> None:
    data = (FIXTURES / "matches_World_Cup_slice.json").read_bytes()
    changed = data.replace(b'"teamId": 9598', b'"teamId": 1', 1)
    with pytest.raises(IntegrityError, match="teamId"):
        _parse(matches=changed)


def test_match_must_be_in_the_file() -> None:
    with pytest.raises(IntegrityError, match="999"):
        _parse("999")


def test_unmapped_team_field_fails() -> None:
    data = (FIXTURES / "matches_World_Cup_slice.json").read_bytes()
    with pytest.raises(UnmappedFieldError, match=r"teamsData\.<id>\.captain"):
        _parse(matches=data.replace(b'"side": ', b'"captain": 1, "side": ', 1))


def test_unmapped_formation_field_fails() -> None:
    data = (FIXTURES / "matches_World_Cup_slice.json").read_bytes()
    assert b'"playerId": ' in data
    with pytest.raises(UnmappedFieldError, match=r"teamsData\.<id>\.formation\.(lineup|bench)\[\]\.shirt"):
        _parse(matches=data.replace(b'"playerId": ', b'"shirt": "9", "playerId": ', 1))


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (b'"goals": "null"', b'"goals": null', "goals=None"),  # a JSON null would collide with the text "null"
        (b'"goals": "null", ', b"", "goals is missing"),
        (b'"yellowCards": "0"', b'"yellowCards": 0', "yellowCards=0"),
    ],
)
def test_formation_counts_stay_one_to_one(old: bytes, new: bytes, message: str) -> None:
    data = (FIXTURES / "matches_World_Cup_slice.json").read_bytes()
    assert old in data
    with pytest.raises(UnexpectedPayloadError, match=message):
        _parse(matches=data.replace(old, new, 1))


def test_coordinate_system_wyscout(parsed: ParsedMatch) -> None:
    """§11.4: percentages from the acting team's view (Pappalardo et al. 2019, Data Records > Events)."""
    events = parsed.coordinate_systems["events"]
    assert (events.origin, events.units, events.pitch_length, events.pitch_width) == ("top_left", "pct", None, None)
    assert "opponent's goal" in events.x_direction
    assert "right side" in events.y_direction
    assert "Pappalardo" in events.source_ref


def test_keys_and_audit_columns(parsed: ParsedMatch) -> None:
    key = match_key("wyscout", "2058017")
    for name in (MATCHES, MATCH_TEAMS, MATCH_FORMATIONS, MATCH_SUBSTITUTIONS, EVENTS):
        table = parsed.tables[name]
        assert set(_column(table, "match_key")) == {key}, name
        assert set(_column(table, "provider")) == {"wyscout"}, name
        assert set(_column(table, "_run_id")) == {"run-1"}, name
    formations = parsed.tables[MATCH_FORMATIONS]
    assert _column(formations, "player_key") == [
        player_key("wyscout", str(p)) for p in _column(formations, "player_id")
    ]
    assert _column(formations, "team_key") == [team_key("wyscout", str(t)) for t in _column(formations, "team_id")]


def test_parse_needs_the_matches_file() -> None:
    with pytest.raises(IntegrityError, match="matches"):
        WyscoutParser().parse(
            _entry("2058017"), {"events": _delivery("events", "events_World_Cup_slice.json")}, run_id="r"
        )


@pytest.mark.parametrize(
    ("kind", "name", "table", "rows"),
    [
        ("competitions", "competitions.json", COMPETITIONS, 7),
        ("teams", "teams.json", TEAMS, 142),
        ("players", "players_slice.json", PLAYERS, 138),
        ("coaches", "coaches_slice.json", COACHES, 6),
        ("event_names", "eventid2name.csv", EVENT_NAMES, 36),
        ("tag_names", "tags2name.csv", TAG_NAMES, 59),
    ],
)
def test_listings(kind: str, name: str, table: str, rows: int) -> None:
    listing = WyscoutParser().listing(kind, (FIXTURES / name).read_bytes(), run_id="r", source_hash="h" * 64)  # type: ignore[arg-type]
    assert listing.num_rows == rows
    assert set(_column(listing, "_source_hash")) == {"h" * 64}
    assert set(_column(listing, "provider")) == {"wyscout"}
    assert LISTING_TABLES[kind] == table  # type: ignore[index]


def test_csv_listings_are_exact_with_lower_case_columns() -> None:
    parser = WyscoutParser()
    tags = parser.listing("tag_names", (FIXTURES / "tags2name.csv").read_bytes(), run_id="r", source_hash="h" * 64)
    assert tags.column_names[:3] == ["tag", "label", "description"]
    assert tags.to_pylist()[0] | {"_ingested_at": None} == {
        "tag": 101,
        "label": "Goal",
        "description": "Goal",
        "provider": "wyscout",
        "_ingested_at": None,
        "_run_id": "r",
        "_source_hash": "h" * 64,
    }
    names = parser.listing("event_names", (FIXTURES / "eventid2name.csv").read_bytes(), run_id="r", source_hash="h")
    lines = (FIXTURES / "eventid2name.csv").read_text(encoding="utf-8").splitlines()[1:]
    assert [
        ",".join(str(r[c]) for c in ("event", "subevent", "event_label", "subevent_label")) for r in names.to_pylist()
    ] == lines


def test_players_listing_keeps_mixed_ids_as_text() -> None:
    players = WyscoutParser().listing(
        "players", (FIXTURES / "players_slice.json").read_bytes(), run_id="r", source_hash="h" * 64
    )
    national = set(_column(players, "current_national_team_id"))
    assert "null" in national  # the source writes the text "null" for some players
    assert all(v is None or isinstance(v, str) for v in national)
    assert _column(players, "player_key")[0] == player_key("wyscout", str(_column(players, "player_id")[0]))


def test_names_escaped_twice_are_kept_as_delivered() -> None:
    """The players, teams and matches files write `"I. Rakiti\\\\u0107"`; bronze keeps the literal `\\u0107` (§12)."""
    raw = (FIXTURES / "players_slice.json").read_bytes()
    assert b'"shortName": "I. Rakiti\\\\u0107"' in raw
    players = WyscoutParser().listing("players", raw, run_id="r", source_hash="h" * 64)
    assert "I. Rakiti\\u0107" in _column(players, "short_name")


def test_a_match_twice_in_the_file_fails() -> None:
    records = _source("matches_World_Cup_slice.json")
    data = json.dumps([*records, records[0]]).encode()
    with pytest.raises(IntegrityError, match="2 matches with id 2058017"):
        _parse(matches=data)


def test_no_coordinate_system_without_events() -> None:
    deliveries = {"matches": _delivery("matches", "matches_World_Cup_slice.json")}
    assert WyscoutParser().parse(_entry("2058017"), deliveries, run_id="r").coordinate_systems == {}


def test_a_changed_events_file_is_read_again() -> None:
    parser = WyscoutParser()
    matches = _delivery("matches", "matches_World_Cup_slice.json")
    data = (FIXTURES / "events_World_Cup_slice.json").read_bytes()
    first = parser.parse(_entry("2058017"), {"matches": matches, "events": _delivery("events", "x", data)}, run_id="r")
    source = [e for e in _source("events_World_Cup_slice.json") if e["matchId"] == 2058017]
    fewer = json.dumps(source[:3]).encode()
    second = parser.parse(
        _entry("2058017"), {"matches": matches, "events": _delivery("events", "x", fewer)}, run_id="r"
    )
    assert first.tables[EVENTS].num_rows == len(source)
    assert second.tables[EVENTS].num_rows == 3
