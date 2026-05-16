"""Unit tests for SkillCorner match metadata ingestion."""

from __future__ import annotations

import json

from ingestion.skillcorner_matches import parse_match_json


def _make_match_json() -> dict:
    """Build a minimal but complete match.json fixture."""
    return {
        "id": 1886347,
        "date_time": "2024-11-30T15:00:00Z",
        "pitch_length": 105,
        "pitch_width": 68,
        "stadium": {"name": "Go Media Stadium"},
        "home_team": {"id": 4177, "name": "Auckland FC", "short_name": "AUK"},
        "away_team": {"id": 4262, "name": "Newcastle Jets", "short_name": "NEW"},
        "competition_edition": {
            "competition": {"id": 382, "name": "A-League Men"},
            "season": {"id": 74, "name": "2024/2025"},
        },
        "match_periods": [
            {"period": 1, "start_time": "00:00:00.00", "end_time": "00:47:23.50"},
            {"period": 2, "start_time": "00:00:00.00", "end_time": "00:49:12.30"},
        ],
        "players": [
            {
                "id": 38673,
                "team_id": 4177,
                "short_name": "A. Player",
                "first_name": "Andrew",
                "last_name": "Player",
                "number": 10,
                "player_role": {"name": "Midfielder", "acronym": "MF"},
            },
            {
                "id": 44001,
                "team_id": 4177,
                "short_name": "B. Keeper",
                "first_name": "Bob",
                "last_name": "Keeper",
                "number": 1,
                "player_role": {"name": "Goalkeeper", "acronym": "GK"},
            },
            {
                "id": 50200,
                "team_id": 4262,
                "short_name": "C. Forward",
                "first_name": "Charlie",
                "last_name": "Forward",
                "number": 9,
                "player_role": {"name": "Forward", "acronym": "FW"},
            },
        ],
    }


class TestParseMatchJson:
    def test_roster_row_count(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        # 3 players = 3 rows
        assert len(df) == 3

    def test_match_id_is_raw_native(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        assert df["match_id"].iloc[0] == "1886347"

    def test_player_fields(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        row = df[df["player_id"] == 38673].iloc[0]
        assert row["player_name"] == "A. Player"
        assert row["first_name"] == "Andrew"
        assert row["last_name"] == "Player"
        assert row["jersey_number"] == 10
        assert row["position_name"] == "Midfielder"
        assert row["position_acronym"] == "MF"

    def test_team_resolution(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        # Home player
        home_row = df[df["player_id"] == 38673].iloc[0]
        assert home_row["team_id"] == 4177
        assert home_row["team_name"] == "Auckland FC"
        assert home_row["home_team_id"] == 4177
        assert home_row["away_team_id"] == 4262
        # Away player
        away_row = df[df["player_id"] == 50200].iloc[0]
        assert away_row["team_id"] == 4262
        assert away_row["team_name"] == "Newcastle Jets"

    def test_competition_metadata(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        row = df.iloc[0]
        assert row["competition_id"] == 382
        assert row["competition_name"] == "A-League Men"
        assert row["season_id"] == 74
        assert row["season_name"] == "2024/2025"

    def test_pitch_dimensions(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        row = df.iloc[0]
        assert row["pitch_length"] == 105
        assert row["pitch_width"] == 68

    def test_period_boundaries_serialized(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        periods = json.loads(df["period_boundaries"].iloc[0])
        assert len(periods) == 2
        assert periods[0]["period"] == 1

    def test_goalkeeper_position(self) -> None:
        data = _make_match_json()
        df = parse_match_json(json.dumps(data), match_id="1886347")
        gk_row = df[df["player_id"] == 44001].iloc[0]
        assert gk_row["position_name"] == "Goalkeeper"
