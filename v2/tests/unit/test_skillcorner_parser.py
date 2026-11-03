"""The SkillCorner parser (spec §10.2, §11, §12; plan Task 17). Fixtures: real slices of match 1899585 (MIT)."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from skillcornerkit import ENTRY, FILES, delivery, fixture_bytes, parse_with

from lakehouse.bronze.schemas import AUDIT_COLUMNS, TRACKING_FRAMES_CORE, TRACKING_PLAYERS_CORE
from lakehouse.bronze.tracking import check_tracking_schema
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError, UnmappedFieldError
from lakehouse.ids.keys import match_key, player_key, team_key
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.skillcorner import (
    DYNAMIC_EVENTS,
    MATCH_LIST,
    MATCHES,
    PHASES_OF_PLAY,
    PLAYERS,
    TRACKING_FRAMES,
    TRACKING_PLAYERS,
    TRACKING_ROSTER,
    SkillCornerParser,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "skillcorner"
FRAME_10 = '{"frame": 10, "timestamp": "00:00:00.00", "period": 1, "ball_data": {"x": 0.73,'


def _frames_oracle() -> list[dict[str, Any]]:
    """The fixture frames read with plain `json`: an oracle independent of the parser."""
    return [json.loads(line) for line in fixture_bytes("tracking").splitlines()]


def _column(table: pa.Table, name: str) -> list[Any]:
    return table.column(name).to_pylist()


@pytest.fixture(scope="module")
def parsed() -> ParsedMatch:
    return parse_with()


def _row(table: pa.Table, frame: int) -> dict[str, Any]:
    return next(r for r in table.to_pylist() if r["frame"] == frame)


def test_tracking_players_frames_split(parsed: ParsedMatch) -> None:
    players, frames = parsed.tables[TRACKING_PLAYERS], parsed.tables[TRACKING_FRAMES]
    check_tracking_schema("players", players.schema)
    check_tracking_schema("frames", frames.schema)
    assert players.schema.names[: len(TRACKING_PLAYERS_CORE)] == TRACKING_PLAYERS_CORE.names
    assert frames.schema.names[: len(TRACKING_FRAMES_CORE)] == TRACKING_FRAMES_CORE.names
    oracle = _frames_oracle()
    assert frames.num_rows == len(oracle) == 182
    assert players.num_rows == sum(len(f["player_data"]) for f in oracle)
    for audit in AUDIT_COLUMNS.names:
        assert audit not in players.schema.names
        assert audit not in frames.schema.names


def test_prematch_frames_kept(parsed: ParsedMatch) -> None:
    """§12.6: IRIS dropped the frames without player data (pre-match and half time); v2 keeps every frame."""
    frames = parsed.tables[TRACKING_FRAMES]
    by_frame = {r["frame"]: r for r in frames.to_pylist()}
    for frame in [*range(10), *range(29761, 29770)]:
        assert (by_frame[frame]["period"], by_frame[frame]["timestamp"], by_frame[frame]["ball_x_mm"]) == (
            None,
            None,
            None,
        )
    assert by_frame[10]["period"] == 1


def test_possession_and_image_corners_kept(parsed: ParsedMatch) -> None:
    """§12.6: IRIS dropped `possession` (player_id, group) and the 8 `image_corners_projection` values."""
    first = _row(parsed.tables[TRACKING_FRAMES], 10)
    assert first["possession_player_key"] == player_key("skillcorner", "23909")
    assert first["possession_group"] == "away team"
    corners = {
        name: first[f"image_corners_{name}_mm"] for name in ("x_top_left", "y_top_left", "x_top_right", "y_top_right")
    }
    assert corners == {"x_top_left": -47620, "y_top_left": 39000, "x_top_right": 45430, "y_top_right": 39000}
    group_only = _row(parsed.tables[TRACKING_FRAMES], 29773)
    assert (group_only["possession_player_key"], group_only["possession_group"]) == (None, "home team")


def test_types_and_exactness(parsed: ParsedMatch) -> None:
    first = _row(parsed.tables[TRACKING_FRAMES], 10)
    assert (first["ball_x_mm"], first["ball_y_mm"], first["ball_z_mm"], first["ball_detected"]) == (
        730,
        -930,
        260,
        True,
    )
    assert (first["match_clock"], first["timestamp"]) == (dt.timedelta(0), dt.timedelta(0))
    second_half = _row(parsed.tables[TRACKING_FRAMES], 29771)
    assert second_half["match_clock"] == dt.timedelta(minutes=45, milliseconds=100)
    assert second_half["timestamp"] == dt.timedelta(milliseconds=100)  # period-relative (§11.5)
    rows = [r for r in parsed.tables[TRACKING_PLAYERS].to_pylist() if r["frame"] == 10]
    keeper = next(r for r in rows if r["player_key"] == player_key("skillcorner", "34718"))
    assert (keeper["period"], keeper["x_mm"], keeper["y_mm"], keeper["is_visible"]) == (1, -39610, 710, False)
    events = parsed.tables[DYNAMIC_EVENTS]
    first_event = events.to_pylist()[0]
    assert (first_event["event_id"], first_event["x_start_mm"], first_event["time_start"]) == (
        "8_0",
        490,
        dt.timedelta(milliseconds=100),
    )
    match = parsed.tables[MATCHES].to_pylist()[0]
    assert match["date_time"] == dt.datetime(2024, 12, 7, 4, tzinfo=dt.UTC)
    assert (match["pitch_length"], match["pitch_width"]) == (104, 68)


def test_inexact_coordinate_fails() -> None:
    changed = fixture_bytes("tracking").replace(b'{"x": -39.61, "y": 0.71', b'{"x": -39.6151, "y": 0.71', 1)
    with pytest.raises(InexactValueError, match=r"line \d+.*-39\.6151"):
        parse_with(tracking=changed)


def test_unmapped_tracking_field_fails() -> None:
    changed = fixture_bytes("tracking").replace(b'"is_detected": false}', b'"is_detected": false, "speed": 1.2}', 1)
    with pytest.raises(UnmappedFieldError, match=r"player_data\[\]\.speed"):
        parse_with(tracking=changed)


def test_player_data_needs_a_period() -> None:
    oracle = fixture_bytes("tracking")
    start = oracle.index(FRAME_10.encode())
    end = oracle.index(b"\n", start)
    line = oracle[start:end].replace(b'"period": 1,', b'"period": null,', 1)
    with pytest.raises(UnexpectedPayloadError, match="period"):
        parse_with(tracking=oracle[:start] + line + oracle[end:])


def test_coordinate_system_skillcorner(parsed: ParsedMatch) -> None:
    """§11.4: centre origin, metres, x along the long side (right in the provider's field diagram), y up."""
    systems = parsed.coordinate_systems
    assert set(systems) == {"tracking", "dynamic_events", "phases_of_play"}
    tracking = systems["tracking"]
    assert (tracking.origin, tracking.units, tracking.pitch_length, tracking.pitch_width) == (
        "centre",
        "m",
        104.0,
        68.0,
    )
    assert "long side" in tracking.x_direction
    assert "home_team_side" in (tracking.play_direction or "")
    assert "README" in tracking.source_ref
    assert "field.jpg" in tracking.source_ref
    assert systems["dynamic_events"].origin == "centre"


def test_keys_and_audit_columns(parsed: ParsedMatch) -> None:
    key = match_key("skillcorner", "1899585")
    hashes = {artifact: delivery(artifact).content_sha256 for artifact in FILES}
    for name, artifact in (
        (MATCHES, "match"),
        (PLAYERS, "match"),
        (DYNAMIC_EVENTS, "dynamic_events"),
        (PHASES_OF_PLAY, "phases_of_play"),
        (TRACKING_ROSTER, "tracking"),
    ):
        table = parsed.tables[name]
        assert set(_column(table, "provider")) == {"skillcorner"}, name
        assert set(_column(table, "native_match_id")) == {"1899585"}, name
        assert set(_column(table, "match_key")) == {key}, name
        assert set(_column(table, "_source_hash")) == {hashes[artifact]}, name
        assert set(_column(table, "_run_id")) == {"run-1"}, name
    players = parsed.tables[PLAYERS]
    assert _column(players, "player_key") == [player_key("skillcorner", str(p)) for p in _column(players, "player_id")]
    assert _column(players, "team_key") == [team_key("skillcorner", str(t)) for t in _column(players, "team_id")]
    roster = parsed.tables[TRACKING_ROSTER]
    seen = {p["player_id"] for f in _frames_oracle() for p in f["player_data"]}
    seen |= {f["possession"]["player_id"] for f in _frames_oracle() if f["possession"]["player_id"] is not None}
    assert set(_column(roster, "player_id")) == seen
    for name in (TRACKING_PLAYERS, TRACKING_FRAMES):
        assert set(_column(parsed.tables[name], "match_key")) == {key}


def test_match_file_belongs_to_the_match() -> None:
    changed = fixture_bytes("match").replace(b'{"id":1899585,', b'{"id":1899586,', 1)
    with pytest.raises(IntegrityError, match="holds match 1899586"):
        parse_with(match=changed)


def test_roster_holds_possession_only_players() -> None:
    """A player id that appears only in `possession` still gets a roster row, so its key resolves (§11.2)."""
    changed = fixture_bytes("tracking").replace(
        b'"possession": {"player_id": 23909,', b'"possession": {"player_id": 777001,', 1
    )
    roster = parse_with(tracking=changed).tables[TRACKING_ROSTER]
    assert 777001 in roster.column("player_id").to_pylist()


def test_events_belong_to_the_match() -> None:
    changed = fixture_bytes("dynamic_events").replace(b",1899585,", b",1899586,", 1)
    with pytest.raises(IntegrityError, match="1899586"):
        parse_with(dynamic_events=changed)


def test_names_from_the_match_file(parsed: ParsedMatch) -> None:
    players = {p["player_id"]: p for p in parsed.names.players.to_pylist()}
    assert players[23909]["short_name"] == "K. Barbarouses"
    assert parsed.names.teams.to_pylist()[0] == {"team_id": 4177, "team_name": "Auckland FC"}
    assert parsed.names.competitions.to_pylist() == [
        {"competition_id": 61, "competition_name": "A-League", "season_id": 95, "season_name": "2024/2025"}
    ]


def test_match_list() -> None:
    table = SkillCornerParser().listing(
        "matches", (FIXTURES / "matches.json").read_bytes(), run_id="run-1", source_hash="h" * 64
    )
    assert table.num_rows == 20
    rows = {r["match_id"]: r for r in table.to_pylist()}
    assert rows[1899585]["match_key"] == match_key("skillcorner", "1899585")
    assert rows[1899585]["home_team_key"] == team_key("skillcorner", "4177")
    assert set(_column(table, "_source_hash")) == {"h" * 64}
    assert MATCH_LIST == "skillcorner_match_list"


def test_parse_needs_the_match_file() -> None:
    with pytest.raises(IntegrityError, match="match"):
        SkillCornerParser().parse(ENTRY, {"tracking": delivery("tracking")}, run_id="run-1")
