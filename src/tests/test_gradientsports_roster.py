"""Tests for gradientsports_roster.py parser."""

from __future__ import annotations

import json

import pytest

_SAMPLE_ROSTER = [
    {
        "player": {"id": "3861", "nickname": "Xavi Simons"},
        "team": {"id": "366", "name": "Netherlands"},
        "positionGroupType": "AM",
        "shirtNumber": "7",
        "started": True,
    },
    {
        "player": {"id": "4200", "nickname": "Memphis Depay"},
        "team": {"id": "366", "name": "Netherlands"},
        "positionGroupType": "CF",
        "shirtNumber": "10",
        "started": True,
    },
]


class TestParseRoster:
    def test_basic_parse(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        df = parse_roster(_SAMPLE_ROSTER, match_id="10502")
        assert len(df) == 2
        assert df["match_id"].iloc[0] == "10502"
        assert "_ingested_at" in df.columns
        assert "player.id" in df.columns
        assert "team.id" in df.columns

    def test_from_json_string(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        df = parse_roster(json.dumps(_SAMPLE_ROSTER), match_id="10502")
        assert len(df) == 2

    def test_match_id_validated(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        with pytest.raises(ValueError, match="invalid Gradient Sports match id"):
            parse_roster(_SAMPLE_ROSTER, match_id="bad_id")

    def test_player_id_validated(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        bad_roster = [
            {
                "player": {"id": "player_3861", "nickname": "Bad ID"},
                "team": {"id": "366", "name": "Netherlands"},
                "positionGroupType": "AM",
                "shirtNumber": "7",
                "started": True,
            },
        ]
        with pytest.raises(ValueError, match="invalid Gradient Sports player id"):
            parse_roster(bad_roster, match_id="10502")

    def test_team_id_validated(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        bad_roster = [
            {
                "player": {"id": "3861", "nickname": "OK Player"},
                "team": {"id": "team_366", "name": "Netherlands"},
                "positionGroupType": "AM",
                "shirtNumber": "7",
                "started": True,
            },
        ]
        with pytest.raises(ValueError, match="invalid Gradient Sports team id"):
            parse_roster(bad_roster, match_id="10502")

    def test_int_columns_widened_to_float64(self) -> None:
        from ingestion.gradientsports_roster import parse_roster

        df = parse_roster(_SAMPLE_ROSTER, match_id="10502")
        int_cols = df.select_dtypes(include=["int64", "int32"]).columns
        assert len(int_cols) == 0, f"Expected no int columns, got: {list(int_cols)}"
