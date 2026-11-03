"""The Gradient Sports parser (spec §10.2, §11, §12, §29.15; plan Task 18). Fixtures are synthetic: invented values on
the field paths of the live sample (GS data has no licence, §23.4)."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from gskit import ENTRY, FILES, delivery, fixture_bytes, parse_with

from lakehouse.bronze.schemas import AUDIT_COLUMNS, TRACKING_FRAMES_CORE, TRACKING_PLAYERS_CORE
from lakehouse.bronze.tracking import check_tracking_schema
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.ids.keys import match_key, player_key, team_key
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.gradientsports import (
    COMPETITIONS,
    EVENT_OBJECTS,
    EVENTS,
    MATCHES,
    PLAYER_LIST,
    ROSTERS,
    TRACKING_FRAMES,
    TRACKING_PLAYERS,
    GradientSportsParser,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gradientsports"


def _frames_oracle() -> list[dict[str, Any]]:
    return [json.loads(line) for line in fixture_bytes("tracking").splitlines()]


def _column(table: pa.Table, name: str) -> list[Any]:
    return table.column(name).to_pylist()


def _tracking(old: bytes, new: bytes) -> bytes:
    data = fixture_bytes("tracking")
    assert data.count(old) >= 1
    return data.replace(old, new, 1)


@pytest.fixture(scope="module")
def parsed() -> ParsedMatch:
    return parse_with()


def test_tracking_players_frames_split(parsed: ParsedMatch) -> None:
    players, frames = parsed.tables[TRACKING_PLAYERS], parsed.tables[TRACKING_FRAMES]
    check_tracking_schema("players", players.schema)
    check_tracking_schema("frames", frames.schema)
    assert players.schema.names[: len(TRACKING_PLAYERS_CORE)] == TRACKING_PLAYERS_CORE.names
    assert frames.schema.names[: len(TRACKING_FRAMES_CORE)] == TRACKING_FRAMES_CORE.names
    oracle = _frames_oracle()
    assert frames.num_rows == len(oracle) == 5
    lists = ("homePlayers", "homePlayersSmoothed", "awayPlayers", "awayPlayersSmoothed")
    assert players.num_rows == sum(len(f[key]) for f in oracle for key in lists)
    for audit in AUDIT_COLUMNS.names:
        assert audit not in players.schema.names


def test_duplicate_frames_kept(parsed: ParsedMatch) -> None:
    """GS writes a frame once per possession event (PFF FC Tracking Data Specification §1); v2 keeps every record."""
    frames = [r for r in parsed.tables[TRACKING_FRAMES].to_pylist() if r["frame"] == 101]
    assert [r["record_index"] for r in frames] == [1, 2]
    assert {r["possession_event_id"] for r in frames} == {7101.0, frames[0]["possession_event_id"]}
    assert len({r["possession_event_id"] for r in frames}) == 2
    rows = [r for r in parsed.tables[TRACKING_PLAYERS].to_pylist() if r["frame"] == 101]
    assert sorted({r["record_index"] for r in rows}) == [1, 2]


def test_types_and_exactness(parsed: ParsedMatch) -> None:
    frames = {(r["period"], r["frame"], r["record_index"]): r for r in parsed.tables[TRACKING_FRAMES].to_pylist()}
    first = frames[(1, 101, 1)]
    assert first["timestamp"] == dt.timedelta(microseconds=33041)  # 0.033041 s, exact
    assert (first["ball_x_mm"], first["ball_y_mm"], first["ball_z_mm"], first["ball_detected"]) == (
        125,
        -250,
        375,
        False,
    )
    assert (first["ball_smoothed_x_mm"], first["ball_smoothed_visibility"]) == (133, "VISIBLE")
    assert frames[(2, 5000, 4)]["timestamp"] == dt.timedelta(0)
    assert first["generated_time"] == dt.datetime(2030, 6, 2, 8, 0, 0, 123456, tzinfo=dt.UTC)
    assert first["video_time_ms"] == 3333.492
    no_ball = frames[(1, 102, 3)]
    assert (no_ball["ball_x_mm"], no_ball["ball_detected"], no_ball["ball_smoothed_x_mm"]) == (None, None, None)
    keeper = next(
        r
        for r in parsed.tables[TRACKING_PLAYERS].to_pylist()
        if r["record_index"] == 0 and r["player_key"] == player_key("gradientsports", "8001") and not r["smoothed"]
    )
    assert (keeper["x_mm"], keeper["y_mm"], keeper["is_visible"], keeper["speed"], keeper["team_side"]) == (
        -10125,
        -5250,
        True,
        1.375,
        "home",
    )


def test_inexact_coordinate_fails() -> None:
    with pytest.raises(InexactValueError, match=r"line 1.*-10\.1255"):
        parse_with(tracking=_tracking(b'"x": -10.125', b'"x": -10.1255'))


def test_overprecise_float_normalized_to_nearest_float64() -> None:
    """GS's JSON serializer emits floats with sub-ULP noise digits (the owner-tier parse met a videoTimeMs of
    524491.1580000001, whose float64 is 524491.158). The noise is below the float64 ULP, so v2 ingests the value as the
    nearest float64 instead of refusing the match. Player x and y stay on the exact-millimetre path, which still rejects
    a sub-millimetre coordinate (test_inexact_coordinate_fails)."""
    parsed = parse_with(tracking=_tracking(b'"videoTimeMs": 3333.492', b'"videoTimeMs": 3333.4920000000001'))
    frame = next(
        r
        for r in parsed.tables[TRACKING_FRAMES].to_pylist()
        if (r["period"], r["frame"], r["record_index"]) == (1, 101, 1)
    )
    assert frame["video_time_ms"] == 3333.492


def test_exponent_form_float_accepted() -> None:
    """GS serialises a small float in exponent form (the owner-tier parse met a frame float of 9e-06). Python's repr
    keeps the exponent for a magnitude below 1e-4, so a plain-decimal guard would reject it; v2 stores the nearest
    float64 directly."""
    parsed = parse_with(tracking=_tracking(b'"videoTimeMs": 3333.492', b'"videoTimeMs": 9e-06'))
    frame = next(
        r
        for r in parsed.tables[TRACKING_FRAMES].to_pylist()
        if (r["period"], r["frame"], r["record_index"]) == (1, 101, 1)
    )
    assert frame["video_time_ms"] == 9e-06


def test_player_key_from_the_roster() -> None:
    """GS tracking names a player by team and estimated jersey; the roster gives the player id."""
    with pytest.raises(UnexpectedPayloadError, match="jersey 99"):
        parse_with(tracking=_tracking(b'"jerseyNum": "7"', b'"jerseyNum": "99"'))


def test_jersey_twice_in_one_list_fails() -> None:
    """Two entries with one jersey in one team list cannot both map to the roster player; the match fails."""
    with pytest.raises(UnexpectedPayloadError, match="jersey 1 twice"):
        parse_with(tracking=_tracking(b'"jerseyNum": "7"', b'"jerseyNum": "1"'))


def test_unknown_visibility_fails() -> None:
    with pytest.raises(UnexpectedPayloadError, match="HIDDEN"):
        parse_with(tracking=_tracking(b'"visibility": "VISIBLE"', b'"visibility": "HIDDEN"'))


@pytest.mark.parametrize(
    ("old", "new", "path"),
    [
        (b'"frameNum": 100', b'"frameNum": 100, "camera": 1', "camera"),
        (b'"speed": 1.375', b'"speed": 1.375, "heading": 2', "homePlayers[].heading"),
        (b'"game_event_type": "A"', b'"game_event_type": "A", "outcome": "B"', "game_event.outcome"),
    ],
)
def test_unmapped_tracking_field_fails(old: bytes, new: bytes, path: str) -> None:
    with pytest.raises(UnmappedFieldError) as caught:
        parse_with(tracking=_tracking(old, new))
    assert path in caught.value.field_paths


def test_more_than_one_ball_fails() -> None:
    ball = b'"balls": [{"visibility": "VISIBLE", "x": 0.125, "y": -0.25, "z": 0.375}]'
    two = ball[:-1] + b', {"visibility": "VISIBLE", "x": 1, "y": 1, "z": 0}]'
    with pytest.raises(UnexpectedPayloadError, match="2 balls"):
        parse_with(tracking=_tracking(ball, two))


def test_events_and_their_objects(parsed: ParsedMatch) -> None:
    events, objects = parsed.tables[EVENTS], parsed.tables[EVENT_OBJECTS]
    assert events.num_rows == 3
    assert _column(events, "event_index") == [0, 1, 2]
    assert objects.num_rows == 3 * 4 + 2  # 4 players per event, and a ball in 2 of the 3 events
    first = [r for r in objects.to_pylist() if r["event_index"] == 0]
    assert {r["object_kind"] for r in first} == {"home", "away", "ball"}
    keeper = next(r for r in first if r["player_id"] == 8001)
    assert (keeper["x_mm"], keeper["y_mm"], keeper["jersey_num"], keeper["player_key"]) == (
        -10125,
        -5250,
        1,
        player_key("gradientsports", "8001"),
    )
    ball = next(r for r in first if r["object_kind"] == "ball")
    assert (ball["x_mm"], ball["z_mm"], ball["player_key"]) == (125, 375, None)


def test_coordinate_system_gradientsports(parsed: ParsedMatch) -> None:
    systems = parsed.coordinate_systems
    assert set(systems) == {"tracking", "events"}
    tracking = systems["tracking"]
    assert (tracking.origin, tracking.units, tracking.pitch_length, tracking.pitch_width) == (
        "centre",
        "m",
        105.0,
        68.0,
    )
    assert "main camera" in tracking.x_direction
    assert "Tracking Data Specification v2.2" in tracking.source_ref
    assert "homeTeamStartLeft" in (tracking.play_direction or "")


def test_keys_and_audit_columns(parsed: ParsedMatch) -> None:
    key = match_key("gradientsports", "9001")
    hashes = {artifact: delivery(artifact).content_sha256 for artifact in FILES}
    for name, artifact in (
        (MATCHES, "metadata"),
        (ROSTERS, "roster"),
        (EVENTS, "events"),
        (EVENT_OBJECTS, "events"),
    ):
        table = parsed.tables[name]
        assert set(_column(table, "provider")) == {"gradientsports"}, name
        assert set(_column(table, "native_match_id")) == {"9001"}, name
        assert set(_column(table, "match_key")) == {key}, name
        assert set(_column(table, "_source_hash")) == {hashes[artifact]}, name
    rosters = parsed.tables[ROSTERS]
    assert _column(rosters, "player_key") == [player_key("gradientsports", p) for p in _column(rosters, "player_id")]
    assert _column(rosters, "team_key") == [team_key("gradientsports", t) for t in _column(rosters, "team_id")]
    assert set(_column(parsed.tables[TRACKING_FRAMES], "match_key")) == {key}


def test_names(parsed: ParsedMatch) -> None:
    assert {r["player_id"] for r in parsed.names.players.to_pylist()} == {"8001", "8002", "8003", "8004"}
    assert parsed.names.competitions.to_pylist() == [
        {"competition_id": "77", "competition_name": "Invented Cup", "season_id": "2030", "season_name": "2030"}
    ]


def test_metadata_must_be_the_match() -> None:
    with pytest.raises(IntegrityError, match="9002"):
        parse_with(metadata=fixture_bytes("metadata").replace(b'"id": "9001"', b'"id": "9002"', 1))


def test_tracking_needs_metadata_and_roster() -> None:
    with pytest.raises(IntegrityError, match="roster"):
        GradientSportsParser().parse(
            ENTRY, {"metadata": delivery("metadata"), "tracking": delivery("tracking")}, run_id="r"
        )


def test_listings() -> None:
    parser = GradientSportsParser()
    competitions = parser.listing(
        "competitions", (FIXTURES / "competitions.csv").read_bytes(), run_id="r", source_hash="h" * 64
    )
    assert competitions.to_pylist()[0]["games"] == [{"id": "9001", "season": "2030"}, {"id": "9002", "season": "2030"}]
    assert COMPETITIONS == "gradientsports_competitions"
    players = parser.listing("players", (FIXTURES / "players.csv").read_bytes(), run_id="r", source_hash="h" * 64)
    assert _column(players, "height") == [182.0, None]
    assert _column(players, "player_key") == [
        player_key("gradientsports", "8001"),
        player_key("gradientsports", "8002"),
    ]
    assert PLAYER_LIST == "gradientsports_player_list"


def test_duplicate_jersey_in_smoothed_is_deduped() -> None:
    """GS's smoothed stream repeats a player at a co-located position (owner-tier data, Task 32). The parser keeps
    the first and drops the duplicate; the raw list keeps its strict one-jersey-per-side rule."""
    lines = fixture_bytes("tracking").splitlines()
    idx = next(i for i, line in enumerate(lines) if json.loads(line).get("homePlayersSmoothed"))
    frame = json.loads(lines[idx])
    period, number = frame["period"], frame["frameNum"]

    def smoothed_rows(table_rows: list[Any]) -> list[Any]:
        return [r for r in table_rows if r["smoothed"] and r["period"] == period and r["frame"] == number]

    baseline = smoothed_rows(parse_with().tables[TRACKING_PLAYERS].to_pylist())
    frame["homePlayersSmoothed"].append(dict(frame["homePlayersSmoothed"][0]))  # a co-located duplicate
    lines[idx] = json.dumps(frame).encode()
    with_dup = smoothed_rows(parse_with(tracking=b"\n".join(lines)).tables[TRACKING_PLAYERS].to_pylist())
    keys = [r["player_key"] for r in with_dup]
    assert len(with_dup) == len(baseline)  # the duplicate added no row
    assert len(keys) == len(set(keys))  # one smoothed row per player in the frame
