"""Tests for ingestion.spadl_adapter — bronze-to-SPADL-converter format mapping."""

from __future__ import annotations

import importlib.util
import json

import pandas as pd
import pytest

from ingestion.spadl_adapter import (
    adapt_statsbomb_events,
    adapt_wyscout_events,
    resolve_statsbomb_home_team_ids,
    resolve_wyscout_home_team_ids,
)

# ---------------------------------------------------------------------------
# StatsBomb adapter
# ---------------------------------------------------------------------------


class TestAdaptStatsbombEvents:
    """Test adapt_statsbomb_events column mapping and transforms."""

    def _make_events(self, **overrides: object) -> pd.DataFrame:
        defaults = {
            "match_id": [123],
            "id": ["uuid-1"],
            "period": [1],
            "type": ["Pass"],
            "team_id": [10],
            "team": ["Barcelona"],
            "player_id": [5],
            "timestamp": ["00:15:30.123"],
            "location": ["[50.0, 40.0]"],
            "_raw_extra_json": ['{"pass": {"end_location": [75, 30]}}'],
        }
        defaults.update(overrides)  # type: ignore[arg-type]
        return pd.DataFrame(defaults)

    def test_renames_columns(self) -> None:
        events = self._make_events()
        result = adapt_statsbomb_events(events, home_team_id=10)
        assert "game_id" in result.columns
        assert "event_id" in result.columns
        assert "period_id" in result.columns
        assert "type_name" in result.columns
        # Original names should be gone
        assert "match_id" not in result.columns
        assert "id" not in result.columns

    def test_reconstructs_extra_dict(self) -> None:
        events = self._make_events()
        result = adapt_statsbomb_events(events, home_team_id=10)
        extra = result["extra"].iloc[0]
        assert isinstance(extra, dict)
        assert "pass" in extra
        assert extra["pass"]["end_location"] == [75, 30]

    def test_parses_location_json(self) -> None:
        events = self._make_events()
        result = adapt_statsbomb_events(events, home_team_id=10)
        loc = result["location"].iloc[0]
        assert isinstance(loc, list)
        assert loc == [50.0, 40.0]

    def test_handles_null_extra(self) -> None:
        events = self._make_events(_raw_extra_json=[None])
        result = adapt_statsbomb_events(events, home_team_id=10)
        assert result["extra"].iloc[0] == {}

    def test_handles_empty_extra(self) -> None:
        events = self._make_events(_raw_extra_json=["{}"])
        result = adapt_statsbomb_events(events, home_team_id=10)
        assert result["extra"].iloc[0] == {}

    def test_handles_null_location(self) -> None:
        events = self._make_events(location=["null"])
        result = adapt_statsbomb_events(events, home_team_id=10)
        # "null" should not be parsed — stays as-is
        assert result["location"].iloc[0] == "null"

    def test_sets_home_team_id(self) -> None:
        events = self._make_events()
        result = adapt_statsbomb_events(events, home_team_id=42)
        assert result["home_team_id"].iloc[0] == 42

    def test_coerces_timestamp_to_timedelta(self) -> None:
        events = self._make_events()
        result = adapt_statsbomb_events(events, home_team_id=10)
        assert pd.api.types.is_timedelta64_dtype(result["timestamp"])


class TestResolveStatsbombHomeTeamIds:
    """Test resolve_statsbomb_home_team_ids join logic."""

    def test_resolves_home_team_id(self) -> None:
        matches = pd.DataFrame(
            {
                "match_id": [1, 2],
                "home_team": ["Barcelona", "Real Madrid"],
            }
        )
        events = pd.DataFrame(
            {
                "match_id": [1, 1, 2, 2],
                "team_id": [10, 20, 30, 40],
                "team": ["Barcelona", "Atletico", "Real Madrid", "Sevilla"],
            }
        )
        result = resolve_statsbomb_home_team_ids(matches, events)
        assert result[1] == 10
        assert result[2] == 30

    def test_handles_missing_team_name(self) -> None:
        matches = pd.DataFrame({"match_id": [1], "home_team": ["Unknown FC"]})
        events = pd.DataFrame(
            {
                "match_id": [1, 1],
                "team_id": [10, 20],
                "team": ["Barcelona", "Atletico"],
            }
        )
        result = resolve_statsbomb_home_team_ids(matches, events)
        # Unknown FC not found — should default to 0
        assert result[1] == 0


# ---------------------------------------------------------------------------
# Wyscout adapter
# ---------------------------------------------------------------------------


class TestAdaptWyscoutEvents:
    """Test adapt_wyscout_events column mapping and transforms."""

    def _make_events(self, **overrides: object) -> pd.DataFrame:
        defaults = {
            "matchId": [100],
            "id": [999],
            "eventId": [8],
            "subEventId": [85],
            "playerId": [50],
            "teamId": [10],
            "matchPeriod": ["1H"],
            "eventSec": [65.5],
            "positions": ['[{"x": 50, "y": 40}]'],
            "tags": ['[{"id": 101}]'],
        }
        defaults.update(overrides)  # type: ignore[arg-type]
        return pd.DataFrame(defaults)

    def test_renames_columns(self) -> None:
        events = self._make_events()
        result = adapt_wyscout_events(events)
        assert "game_id" in result.columns
        assert "event_id" in result.columns
        assert "type_id" in result.columns
        assert "subtype_id" in result.columns
        assert "player_id" in result.columns
        assert "team_id" in result.columns

    def test_maps_periods(self) -> None:
        events = pd.DataFrame(
            {
                "matchId": [1, 2, 3, 4, 5],
                "id": [1, 2, 3, 4, 5],
                "eventId": [8, 8, 8, 8, 8],
                "subEventId": [85, 85, 85, 85, 85],
                "playerId": [1, 1, 1, 1, 1],
                "teamId": [1, 1, 1, 1, 1],
                "matchPeriod": ["1H", "2H", "E1", "E2", "P"],
                "eventSec": [10.0, 20.0, 30.0, 40.0, 50.0],
                "positions": ["[]"] * 5,
                "tags": ["[]"] * 5,
            }
        )
        result = adapt_wyscout_events(events)
        assert list(result["period_id"]) == [1, 2, 3, 4, 5]

    def test_converts_milliseconds(self) -> None:
        events = self._make_events(eventSec=[65.5])
        result = adapt_wyscout_events(events)
        assert result["milliseconds"].iloc[0] == pytest.approx(65500.0)

    def test_parses_json_columns(self) -> None:
        events = self._make_events()
        result = adapt_wyscout_events(events)
        pos = result["positions"].iloc[0]
        assert isinstance(pos, list)
        assert pos[0]["x"] == 50

        tags = result["tags"].iloc[0]
        assert isinstance(tags, list)
        assert tags[0]["id"] == 101


class TestResolveWyscoutHomeTeamIds:
    """Test resolve_wyscout_home_team_ids JSON parsing."""

    def test_extracts_home_team_id(self) -> None:
        matches = pd.DataFrame(
            {
                "wyId": [1, 2],
                "teamsData": [
                    json.dumps({"100": {"side": "home"}, "200": {"side": "away"}}),
                    json.dumps({"300": {"side": "home"}, "400": {"side": "away"}}),
                ],
            }
        )
        result = resolve_wyscout_home_team_ids(matches)
        assert result[1] == 100
        assert result[2] == 300

    def test_handles_empty_teams_data(self) -> None:
        matches = pd.DataFrame({"wyId": [1], "teamsData": ["{}"]})
        result = resolve_wyscout_home_team_ids(matches)
        assert 1 not in result


# ---------------------------------------------------------------------------
# _build_raw_extra_json (statsbomb.py helper)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not importlib.util.find_spec("statsbombpy"),
    reason="statsbombpy not installed (optional dependency)",
)
class TestBuildRawExtraJson:
    """Test _build_raw_extra_json extraction logic."""

    def test_pass_event_extracts_pass_payload(self) -> None:
        """Verify a pass event produces {"pass": {...}} in extra."""
        from unittest.mock import patch

        raw_events = {
            "uuid-1": {
                "type": {"id": 30, "name": "Pass"},
                "pass": {"end_location": [75, 30], "recipient": {"id": 5, "name": "Player"}},
                "related_events": ["uuid-2"],
            },
        }
        with patch("ingestion.statsbomb.sb.events", return_value=raw_events):
            import logging

            from ingestion.statsbomb import _build_raw_extra_json

            result = _build_raw_extra_json(match_id=1, logger=logging.getLogger("test"))

        parsed = json.loads(result["uuid-1"])
        assert "pass" in parsed
        assert parsed["pass"]["end_location"] == [75, 30]
        assert "related_events" in parsed

    def test_shot_event_extracts_shot_payload(self) -> None:
        from unittest.mock import patch

        raw_events = {
            "uuid-2": {
                "type": {"id": 16, "name": "Shot"},
                "shot": {"outcome": {"id": 97, "name": "Goal"}, "statsbomb_xg": 0.15},
            },
        }
        with patch("ingestion.statsbomb.sb.events", return_value=raw_events):
            import logging

            from ingestion.statsbomb import _build_raw_extra_json

            result = _build_raw_extra_json(match_id=1, logger=logging.getLogger("test"))

        parsed = json.loads(result["uuid-2"])
        assert "shot" in parsed
        assert parsed["shot"]["outcome"]["name"] == "Goal"

    def test_empty_type_produces_empty_extra(self) -> None:
        from unittest.mock import patch

        raw_events = {
            "uuid-3": {
                "type": {"id": 34, "name": "Half Start"},
            },
        }
        with patch("ingestion.statsbomb.sb.events", return_value=raw_events):
            import logging

            from ingestion.statsbomb import _build_raw_extra_json

            result = _build_raw_extra_json(match_id=1, logger=logging.getLogger("test"))

        parsed = json.loads(result["uuid-3"])
        assert parsed == {}
