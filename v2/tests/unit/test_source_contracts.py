"""The source adapter and parser contracts (plan Task 14): preview display text and the field inventory."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lakehouse.errors import UnmappedFieldError
from lakehouse.parsers.inventory import check_mapped, json_field_paths
from lakehouse.sources.base import ArtifactRef, PreviewTournament, SourceEntry


def _tournament(**kwargs: object) -> PreviewTournament:
    base: dict[str, object] = {
        "competition_id": "43",
        "competition_name": "FIFA World Cup",
        "season_id": "106",
        "season_name": "2022",
        "match_count": 64,
        "count_note": None,
    }
    return PreviewTournament(**{**base, **kwargs})  # type: ignore[arg-type]


def test_share_360_text() -> None:
    assert _tournament(match_count_360=64).share_360_text() == "64 matches · 64 with 360 (100 %)"
    assert _tournament(match_count=306, match_count_360=203).share_360_text() == "306 matches · 203 with 360 (66 %)"
    unknown = _tournament(match_count_360=None, share_360_note="360 share not available: the match list failed")
    assert unknown.share_360_text() == "360 share not available: the match list failed"
    assert _tournament().share_360_text() is None  # not a StatsBomb tournament


def test_available_360_text() -> None:
    when = datetime(2024, 6, 1, tzinfo=UTC)
    assert _tournament(competition_available_360=when).available_360_text() == "360 data since 2024-06-01"
    assert _tournament(competition_available_360=None, match_count_360=0).available_360_text() == (
        "no 360 data for this season"
    )


def test_source_entry_defaults() -> None:
    entry = SourceEntry(
        module_id="idsse_public",
        native_match_id="J03WPY",
        competition_id="DFL-COM-000002",
        competition_name="2. BL",
        season_id="DFL-SEA-0001K6",
        season_name="2022/2023",
        visibility="public",
        artifacts={"positions": ArtifactRef(key="positions", change_signal="sha", expected_sha256=None, size=None)},
        listed_at=datetime(2026, 10, 26, tzinfo=UTC),
    )
    assert entry.has_360 is None


def test_json_field_paths() -> None:
    payload = [
        {
            "id": 1,
            "type": {"id": 30, "name": "Pass"},
            "location": [60.0, 40.0],
            "freeze_frame": [{"location": [1, 2], "teammate": True}],
        }
    ]
    assert json_field_paths(payload) == frozenset(
        {
            "id",
            "type.id",
            "type.name",
            "location",
            "freeze_frame[].location",
            "freeze_frame[].teammate",
        }
    )


def test_unmapped_field_fails_loudly() -> None:
    check_mapped("events", frozenset({"id", "type.id"}), frozenset({"id", "type.id", "type.name"}))
    with pytest.raises(UnmappedFieldError) as caught:
        check_mapped("events", frozenset({"id", "type.id", "new_field"}), frozenset({"id", "type.id"}))
    assert caught.value.field_paths == ["new_field"]
    assert caught.value.artifact == "events"


def test_empty_list_of_mapped_structs_is_mapped() -> None:
    """Real StatsBomb lineups hold `cards: []`; the empty list is the mapped struct-list field, not a new field."""
    columns = frozenset({"lineup[].cards[].time", "lineup[].cards[].card_type", "location"})
    payload: list[object] = [{"lineup": [{"cards": []}], "location": []}]
    inventory = json_field_paths(payload)
    assert inventory == frozenset({"lineup[].cards", "location"})
    check_mapped("lineups", inventory, columns)
    with pytest.raises(UnmappedFieldError) as caught:
        new_list: list[object] = [{"lineup": [{"new_list": []}]}]
        check_mapped("lineups", json_field_paths(new_list), columns)
    assert caught.value.field_paths == ["lineup[].new_list"]


def test_empty_list_at_any_depth_is_mapped() -> None:
    columns = frozenset({"a[].b[].c[].d"})
    payloads: list[object] = [{"a": []}, {"a": [{"b": []}]}, {"a": [{"b": [{"c": []}]}]}]
    for payload in payloads:
        check_mapped("x", json_field_paths([payload]), columns)
