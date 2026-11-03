"""The StatsBomb parser (spec §11.2, §11.4, §12; plan Task 15)."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from lakehouse.bronze.arrow import column_types
from lakehouse.entries import ArtifactRef, SourceEntry
from lakehouse.errors import InexactValueError, IntegrityError, UnexpectedPayloadError
from lakehouse.ids.keys import match_key, player_key, season_key
from lakehouse.parsers.base import ParsedMatch
from lakehouse.parsers.statsbomb import (
    COORDINATES,
    EVENTS,
    LINEUP_TEAMS,
    LINEUPS,
    MATCH_METADATA,
    THREE_SIXTY,
    StatsBombParser,
)
from lakehouse.raw.archive import Delivery

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "statsbomb"


def _delivery(artifact: str, name: str) -> Delivery:
    return Delivery.received(
        provider="statsbomb",
        channel="public",
        native_match_id="9100001",
        artifact=artifact,
        raw=(FIXTURES / name).read_bytes(),
        visibility="public",
        change_signal="blob",
        fetched_at=datetime.now(UTC),
    )


def _entry() -> SourceEntry:
    return SourceEntry(
        module_id="statsbomb_public",
        native_match_id="9100001",
        competition_id="43",
        competition_name="FIFA World Cup",
        season_id="106",
        season_name="2022",
        visibility="public",
        artifacts={
            a: ArtifactRef(key=a, change_signal="blob", expected_sha256=None, size=None)
            for a in ("events", "lineups", "three_sixty")
        },
        listed_at=datetime.now(UTC),
        has_360=True,
    )


def _parsed() -> tuple[ParsedMatch, dict[str, Delivery]]:
    deliveries = {
        "events": _delivery("events", "events_9100001.json"),
        "lineups": _delivery("lineups", "lineups_9100001.json"),
        "three_sixty": _delivery("three_sixty", "three_sixty_9100001.json"),
    }
    return StatsBombParser().parse(_entry(), deliveries, run_id="run-1"), deliveries


def test_types_and_exactness() -> None:
    parsed, deliveries = _parsed()
    events = parsed.tables[EVENTS]
    types = column_types(events.schema)
    assert types["location"] == pa.list_(pa.float64())
    assert types["timestamp"] == pa.duration("ms")
    assert types["pass_end_location"] == pa.list_(pa.float64())
    assert types["shot_statsbomb_xg"] == pa.float64()
    rows = events.to_pylist()
    assert rows[1]["location"] == [60.5, 40.1]
    assert rows[1]["timestamp"] == dt.timedelta(seconds=1, milliseconds=1)
    lineups = parsed.tables[LINEUPS].to_pylist()
    assert lineups[0]["positions"][0]["to"] == dt.timedelta(minutes=105, seconds=30)
    assert lineups[0]["cards"][0]["time"] == dt.timedelta(minutes=62, seconds=15)
    # a number whose text a float64 cannot hold fails loudly
    raw = json.loads(deliveries["events"].decoded)
    text = json.dumps(raw).replace("60.5", "60.50000000000000000001", 1)
    with pytest.raises(InexactValueError):
        StatsBombParser().table("events", text.encode())


def test_keys_and_audit_columns() -> None:
    parsed, deliveries = _parsed()
    events = parsed.tables[EVENTS]
    key = match_key("statsbomb", "9100001")
    assert set(events.column("match_key").to_pylist()) == {key}
    assert set(events.column("provider").to_pylist()) == {"statsbomb"}
    first = events.to_pylist()[1]
    assert first["player_key"] == player_key("statsbomb", str(first["player_id"]))
    assert set(events.column("_source_hash").to_pylist()) == {deliveries["events"].content_sha256}
    assert set(events.column("_run_id").to_pylist()) == {"run-1"}
    assert set(parsed.tables[LINEUPS].column("_source_hash").to_pylist()) == {deliveries["lineups"].content_sha256}
    assert column_types(events.schema)["_ingested_at"] == pa.timestamp("us", tz="UTC")


def test_three_sixty_frames_kept() -> None:
    parsed, deliveries = _parsed()
    frames = parsed.tables[THREE_SIXTY].to_pylist()
    raw = json.loads(deliveries["three_sixty"].decoded)
    assert len(frames) == len(raw)
    for row, source in zip(frames, raw, strict=True):
        assert row["event_uuid"] == source["event_uuid"]
        assert row["visible_area"] == source["visible_area"]
        assert row["freeze_frame"] == source["freeze_frame"]


def test_coordinate_system_statsbomb() -> None:
    parsed, _ = _parsed()
    assert parsed.coordinate_systems == {"events": COORDINATES, "three_sixty": COORDINATES}  # lineups: none
    assert (COORDINATES.origin, COORDINATES.units) == ("top_left", "yd")
    assert (COORDINATES.pitch_length, COORDINATES.pitch_width) == (120.0, 80.0)
    assert "Open Data Events" in COORDINATES.source_ref


def test_names_bundle_from_lineups() -> None:
    parsed, _ = _parsed()
    names = parsed.names
    assert sorted(str(name) for name in names.players.column("player_name").to_pylist()) == [
        "Ana Example",
        "Bo Sample",
        "Cé Placeholder",
        "Dürer Test",
    ]
    assert sorted({str(name) for name in names.teams.column("team_name").to_pylist()}) == [
        "Example Rovers",
        "Synthetic United",
    ]
    assert names.competitions.to_pylist() == [
        {"competition_id": "43", "competition_name": "FIFA World Cup", "season_id": "106", "season_name": "2022"}
    ]


def test_season_281_five_competitions() -> None:
    """Season 281 belongs to five competitions; season keys include the competition (Task 2)."""
    listing = StatsBombParser().listing(
        "competitions", (FIXTURES / "competitions.json").read_bytes(), run_id="run-1", source_hash="s" * 64
    )
    rows = [r for r in listing.to_pylist() if r["season_id"] == 281]
    assert len(rows) == 5
    assert len({r["season_key"] for r in rows}) == 5
    assert rows[0]["season_key"] == season_key("statsbomb", f"{rows[0]['competition_id']}:281")


def test_matches_listing_types_and_keys() -> None:
    listing = StatsBombParser().listing(
        "matches", (FIXTURES / "matches_43_106.json").read_bytes(), run_id="run-1", source_hash="m" * 64
    )
    row = listing.to_pylist()[0]
    assert row["match_date"] == dt.date(2022, 11, 20)
    assert row["kick_off"] == dt.time(19, 0)
    assert row["last_updated"] == dt.datetime(2023, 4, 26, 23, 49, 58, 956186)
    assert row["home_team_managers"][0]["dob"] == dt.date(1970, 1, 31)
    assert row["match_key"] == match_key("statsbomb", "9100001")


def _commercial(extra: dict[str, Delivery] | None = None) -> ParsedMatch:
    """The pining owner tier's commercial shape (provenance `original`; synthetic values, measured paths)."""
    deliveries = {
        "events": _delivery("events", "commercial_events_9100001.json"),
        "lineups": _delivery("lineups", "commercial_lineups_9100001.json"),
        "three_sixty": _delivery("three_sixty", "commercial_three_sixty_9100001.json"),
        "metadata": _delivery("metadata", "commercial_metadata_9100001.json"),
        **(extra or {}),
    }
    return StatsBombParser().parse(_entry(), deliveries, run_id="run-1")


def test_commercial_event_fields() -> None:
    events = _commercial().tables[EVENTS]
    source = json.loads((FIXTURES / "commercial_events_9100001.json").read_bytes())
    assert events.column("obv_total_net").to_pylist() == [e.get("obv_total_net") for e in source]
    passing = next(r for r in events.to_pylist() if r["pass_pass_cluster_id"] is not None)
    assert (passing["pass_pass_cluster_label"], passing["pass_xclaim"]) == ("Example cluster - short", 0.0012345678)
    shot = next(r for r in events.to_pylist() if r["shot_shot_execution_xg"] is not None)
    assert (shot["shot_shot_goal_assist"], shot["shot_gk_save_difficulty_xg"]) == (True, 0.2345678901)
    open_events = _parsed()[0].tables[EVENTS]
    assert True in open_events.column("goalkeeper_success_out").to_pylist()


def test_lineup_teams_table() -> None:
    teams = _commercial().tables[LINEUP_TEAMS].to_pylist()
    assert [t["team_id"] for t in teams] == [t["team_id"] for t in json.loads(_raw("commercial_lineups_9100001.json"))]
    card = teams[0]["events"][0]
    assert (card["type"], card["outcome"], card["period"]) == ("Card", "Yellow Card", 1)
    assert card["timestamp"] == dt.timedelta(minutes=31, seconds=12, milliseconds=400)
    assert teams[0]["events"][1]["player_id"] is None
    assert [f["formation"] for f in teams[0]["formations"]] == [433, 4231]
    assert teams[0]["team_key"] is not None
    open_teams = _parsed()[0].tables[LINEUP_TEAMS].to_pylist()
    assert [t["events"] for t in open_teams] == [None, None]  # open data has no team events


def test_lineup_player_extras() -> None:
    rows = _commercial().tables[LINEUPS].to_pylist()
    assert rows[0]["birth_date"] == dt.date(1999, 4, 1)
    assert rows[1]["birth_date"] is None
    assert rows[0]["player_gender"] == "female"
    source = [p for t in json.loads(_raw("commercial_lineups_9100001.json")) for p in t["lineup"]]
    assert [r["player_height"] for r in rows] == [p["player_height"] for p in source]
    assert None in [r["player_height"] for r in rows]
    assert rows[0]["stats"] == {
        "assists": 0,
        "goals": 1,
        "own_goals": 0,
        "penalties_missed": 0,
        "penalties_saved": 0,
        "penalties_scored": 0,
    }
    assert rows[1]["stats"] is None  # the source writes [] for a player without stats
    assert rows[0]["positions"][0]["counterpart_name"] == "Counterpart Player"


def test_lineup_stats_must_be_an_object_or_empty() -> None:
    lineups = json.loads(_raw("commercial_lineups_9100001.json"))
    lineups[0]["lineup"][0]["stats"] = [1]
    bad = Delivery.received(
        provider="statsbomb",
        channel="pining",
        native_match_id="9100001",
        artifact="lineups",
        raw=json.dumps(lineups).encode(),
        visibility="private",
        change_signal=None,
        fetched_at=datetime.now(UTC),
    )
    with pytest.raises(UnexpectedPayloadError, match="stats"):
        _commercial({"lineups": bad})


def test_three_sixty_extras() -> None:
    frames = _commercial().tables[THREE_SIXTY].to_pylist()
    assert frames[0]["ball_receipt_exceeds_distance"] == 3
    assert frames[0]["distances_from_edge_of_visible_area"] == [
        {"distance": 12.5, "point_id": 1},
        {"distance": 3.75, "point_id": 2},
    ]
    assert frames[1]["distances_from_edge_of_visible_area"] == []
    assert frames[2]["num_defenders_on_goal_side_of_actor"] is None
    assert frames[0]["visible_player_counts"][0] == {"count": 6, "team_id": 9201}


def test_match_metadata_table() -> None:
    """The pining `metadata` artifact: one match object in StatsBomb feed shape plus five credentialed fields."""
    row = _commercial().tables[MATCH_METADATA].to_pylist()
    assert len(row) == 1
    meta = row[0]
    assert (meta["match_id"], meta["attendance"], meta["behind_closed_doors"], meta["play_status"]) == (
        9100001,
        1234.0,
        False,
        "Normal",
    )
    assert (meta["home_team_country_id"], meta["stadium_id"], meta["stadium_name"]) == (None, None, "Example Park")
    assert meta["home_team_managers"] == [
        {"id": None, "name": "A. Coach", "nickname": None, "dob": None, "country": None}
    ]
    assert meta["match_key"] == match_key("statsbomb", "9100001")
    assert meta["season_key"] == season_key("statsbomb", "43:106")
    assert MATCH_METADATA not in _parsed()[0].tables  # the public channel has no metadata artifact


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_names_competition_from_metadata() -> None:
    """With a metadata artifact, the competition and season names come from it, not from the listing entry, so the
    list-fields check (§29.1) compares two sources."""
    listed = dataclasses.replace(_entry(), competition_name="listed name", season_name="listed season")
    deliveries = {
        "events": _delivery("events", "commercial_events_9100001.json"),
        "metadata": _delivery("metadata", "commercial_metadata_9100001.json"),
    }
    competitions = StatsBombParser().parse(listed, deliveries, run_id="r").names.competitions.to_pylist()
    meta = json.loads(_raw("commercial_metadata_9100001.json"))
    assert competitions == [
        {
            "competition_id": str(meta["competition"]["competition_id"]),
            "competition_name": meta["competition"]["competition_name"],
            "season_id": str(meta["season"]["season_id"]),
            "season_name": meta["season"]["season_name"],
        }
    ]


def test_position_times_in_both_forms() -> None:
    """Open data writes position times as `MM:SS`, commercial deliveries as `HH:MM:SS.fff`; both are milliseconds."""

    def times(table: pa.Table) -> list[tuple[object, object]]:
        rows: list[dict[str, Any]] = table.to_pylist()
        return [
            (pos["from"], pos["to"]) for row in rows for pos in cast("list[dict[str, Any]]", row["positions"] or [])
        ]

    public = times(_parsed()[0].tables[LINEUPS])
    assert public == times(_commercial().tables[LINEUPS])
    assert all(isinstance(start, dt.timedelta) for start, _ in public)


def test_metadata_of_another_match_fails() -> None:
    meta = json.loads(_raw("commercial_metadata_9100001.json"))
    meta["match_id"] = 9100002
    other = Delivery.received(
        provider="statsbomb",
        channel="pining",
        native_match_id="9100001",
        artifact="metadata",
        raw=json.dumps(meta).encode(),
        visibility="private",
        change_signal=None,
        fetched_at=datetime.now(UTC),
    )
    with pytest.raises(IntegrityError, match="9100002"):
        _commercial({"metadata": other})
