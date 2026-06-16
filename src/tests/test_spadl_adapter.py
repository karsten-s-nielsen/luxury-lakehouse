"""Tests for ingestion.spadl_adapter — bronze-to-SPADL-converter format mapping."""

from __future__ import annotations

import importlib.util
import json

import pandas as pd
import pytest

# IDSSE events shaping moved to the silly-kicks DFL parse port under
# delete-and-depend (ADR-031 T3 / Gate B). The integration test below now
# exercises the port's `shape_events_to_native` (the production adapter).
from silly_kicks.providers.sportec import shape_events_to_native as adapt_idsse_events_for_silly_kicks

from ingestion.spadl_adapter import (
    _resolve_idsse_player_from_qualifiers,
    _resolve_idsse_team_from_qualifiers,
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
# IDSSE adapter — qualifier-based team/player resolution
# ---------------------------------------------------------------------------


class TestResolveIdsseTeamFromQualifiers:
    """Test _resolve_idsse_team_from_qualifiers fills team from DFL qualifiers."""

    @staticmethod
    def _make_events(**overrides: object) -> pd.DataFrame:
        defaults: dict = {
            "team": ["unknown"],
            "player_id": [""],
            "event_type": ["ThrowIn"],
            "home_team_id_native": ["DFL-CLU-HOME"],
            "away_team_id_native": ["DFL-CLU-AWAY"],
            "play_team": [None],
            "throwin_team": [None],
            "foul_team_fouler": [None],
        }
        defaults.update(overrides)
        return pd.DataFrame(defaults)

    def test_resolves_home_from_play_team(self) -> None:
        df = self._make_events(play_team=["DFL-CLU-HOME"])
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "home"

    def test_resolves_away_from_play_team(self) -> None:
        df = self._make_events(play_team=["DFL-CLU-AWAY"])
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "away"

    def test_resolves_from_throwin_team_when_play_team_missing(self) -> None:
        df = self._make_events(throwin_team=["DFL-CLU-HOME"])
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "home"

    def test_resolves_from_foul_team_fouler(self) -> None:
        df = self._make_events(foul_team_fouler=["DFL-CLU-AWAY"])
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "away"

    def test_play_team_takes_priority_over_throwin_team(self) -> None:
        df = self._make_events(play_team=["DFL-CLU-HOME"], throwin_team=["DFL-CLU-AWAY"])
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "home"

    def test_stays_unknown_when_no_qualifiers(self) -> None:
        df = self._make_events()
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "unknown"

    def test_skips_rows_already_resolved(self) -> None:
        df = pd.DataFrame(
            {
                "team": ["home", "unknown"],
                "home_team_id_native": ["DFL-CLU-HOME", "DFL-CLU-HOME"],
                "away_team_id_native": ["DFL-CLU-AWAY", "DFL-CLU-AWAY"],
                "play_team": [None, "DFL-CLU-AWAY"],
            }
        )
        _resolve_idsse_team_from_qualifiers(df)
        assert df["team"].iloc[0] == "home"
        assert df["team"].iloc[1] == "away"

    def test_noop_when_no_unknown_teams(self) -> None:
        df = pd.DataFrame(
            {
                "team": ["home", "away"],
                "home_team_id_native": ["DFL-CLU-HOME", "DFL-CLU-HOME"],
                "away_team_id_native": ["DFL-CLU-AWAY", "DFL-CLU-AWAY"],
                "play_team": [None, None],
            }
        )
        _resolve_idsse_team_from_qualifiers(df)
        assert list(df["team"]) == ["home", "away"]


class TestResolveIdssePlayerFromQualifiers:
    """Test _resolve_idsse_player_from_qualifiers fills player_id from DFL qualifiers."""

    @staticmethod
    def _make_events(**overrides: object) -> pd.DataFrame:
        defaults: dict = {
            "player_id": [""],
            "play_player": [None],
            "foul_fouler": [None],
        }
        defaults.update(overrides)
        return pd.DataFrame(defaults)

    def test_resolves_from_play_player(self) -> None:
        df = self._make_events(play_player=["DFL-OBJ-ABC"])
        _resolve_idsse_player_from_qualifiers(df)
        assert df["player_id"].iloc[0] == "DFL-OBJ-ABC"

    def test_resolves_from_foul_fouler(self) -> None:
        df = self._make_events(foul_fouler=["DFL-OBJ-XYZ"])
        _resolve_idsse_player_from_qualifiers(df)
        assert df["player_id"].iloc[0] == "DFL-OBJ-XYZ"

    def test_play_player_takes_priority_over_foul_fouler(self) -> None:
        df = self._make_events(play_player=["DFL-OBJ-ABC"], foul_fouler=["DFL-OBJ-XYZ"])
        _resolve_idsse_player_from_qualifiers(df)
        assert df["player_id"].iloc[0] == "DFL-OBJ-ABC"

    def test_stays_empty_when_no_qualifiers(self) -> None:
        df = self._make_events()
        _resolve_idsse_player_from_qualifiers(df)
        assert df["player_id"].iloc[0] == ""

    def test_skips_rows_already_populated(self) -> None:
        df = pd.DataFrame(
            {
                "player_id": ["DFL-OBJ-EXISTING", ""],
                "play_player": [None, "DFL-OBJ-NEW"],
            }
        )
        _resolve_idsse_player_from_qualifiers(df)
        assert df["player_id"].iloc[0] == "DFL-OBJ-EXISTING"
        assert df["player_id"].iloc[1] == "DFL-OBJ-NEW"

    def test_handles_null_player_id(self) -> None:
        df = pd.DataFrame(
            {
                "player_id": [None],
                "play_player": ["DFL-OBJ-ABC"],
            }
        )
        _resolve_idsse_player_from_qualifiers(df)
        assert df["player_id"].iloc[0] == "DFL-OBJ-ABC"


class TestAdaptIdsseEventsForSillyKicks:
    """Integration test: adapt_idsse_events_for_silly_kicks applies qualifier resolution."""

    def test_resolves_team_and_player_on_set_piece(self) -> None:
        events = pd.DataFrame(
            {
                "match_id": ["J03WR9"],
                "event_id": ["18242100000792"],
                "event_type": ["ThrowIn"],
                "timestamp_seconds": [244.991],
                "period": [2],
                "player_id": [""],
                "team": ["unknown"],
                "x": [48.19],
                "y": [0.0],
                "home_team_id_native": ["DFL-CLU-HOME"],
                "away_team_id_native": ["DFL-CLU-AWAY"],
                "play_team": ["DFL-CLU-HOME"],
                "play_player": ["DFL-OBJ-J01KJ5"],
                "throwin_team": ["DFL-CLU-HOME"],
            }
        )
        result = adapt_idsse_events_for_silly_kicks(events)
        assert result["team"].iloc[0] == "home"
        assert result["player_id"].iloc[0] == "DFL-OBJ-J01KJ5"

    def test_does_not_mutate_input(self) -> None:
        events = pd.DataFrame(
            {
                "match_id": ["J03WR9"],
                "event_id": ["1"],
                "event_type": ["ThrowIn"],
                "timestamp_seconds": [10.0],
                "period": [1],
                "player_id": [""],
                "team": ["unknown"],
                "x": [50.0],
                "y": [30.0],
                "home_team_id_native": ["DFL-CLU-HOME"],
                "away_team_id_native": ["DFL-CLU-AWAY"],
                "play_team": ["DFL-CLU-HOME"],
                "play_player": ["DFL-OBJ-ABC"],
            }
        )
        _ = adapt_idsse_events_for_silly_kicks(events)
        assert events["team"].iloc[0] == "unknown"
        assert events["player_id"].iloc[0] == ""


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
