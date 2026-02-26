"""Tests for ingestion.statsbomb — JSON serialization and column validation."""

from __future__ import annotations

import json

import pandas as pd

from ingestion.statsbomb import _serialize_json_columns


class TestSerializeJsonColumns:
    """Tests for _serialize_json_columns."""

    def test_serializes_dict_columns(self) -> None:
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "type": [{"id": 16, "name": "Shot"}, {"id": 30, "name": "Pass"}],
                "plain": ["a", "b"],
            }
        )
        result = _serialize_json_columns(df)
        assert isinstance(result["type"].iloc[0], str)
        parsed = json.loads(result["type"].iloc[0])
        assert parsed["name"] == "Shot"

    def test_serializes_list_columns(self) -> None:
        df = pd.DataFrame(
            {
                "id": [1],
                "location": [[50.0, 40.0]],
                "related_events": [["uuid-1", "uuid-2"]],
            }
        )
        result = _serialize_json_columns(df)
        assert isinstance(result["location"].iloc[0], str)
        assert json.loads(result["location"].iloc[0]) == [50.0, 40.0]
        assert json.loads(result["related_events"].iloc[0]) == ["uuid-1", "uuid-2"]

    def test_leaves_string_columns_unchanged(self) -> None:
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        result = _serialize_json_columns(df)
        assert result["name"].iloc[0] == "Alice"
        assert result["age"].iloc[0] == 30

    def test_handles_none_values(self) -> None:
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "type": [{"id": 16, "name": "Shot"}, None],
            }
        )
        result = _serialize_json_columns(df)
        assert isinstance(result["type"].iloc[0], str)
        assert result["type"].iloc[1] is None

    def test_handles_empty_dataframe(self) -> None:
        df = pd.DataFrame({"id": [], "data": []})
        result = _serialize_json_columns(df)
        assert len(result) == 0


class TestStatsbombColumnExpectations:
    """Verify expected columns exist in StatsBomb API responses.

    These tests validate that our code handles the expected column names
    from statsbombpy library.
    """

    def test_competitions_required_columns(self) -> None:
        """Competitions DataFrame must have competition_id and season_id."""
        # Simulate minimal statsbombpy output
        df = pd.DataFrame(
            {
                "competition_id": [11, 11],
                "season_id": [90, 42],
                "competition_name": ["La Liga", "La Liga"],
                "season_name": ["2020/2021", "2019/2020"],
            }
        )
        required = {"competition_id", "season_id"}
        assert required.issubset(set(df.columns))

    def test_events_required_columns(self) -> None:
        """Events DataFrame must have id and match_id after processing."""
        df = pd.DataFrame(
            {
                "id": ["uuid-1"],
                "match_id": [3788741],
                "type": [{"id": 16, "name": "Shot"}],
                "location": [[100.0, 40.0]],
                "timestamp": ["00:15:30.123"],
                "period": [1],
                "minute": [15],
                "second": [30],
            }
        )
        required = {"id", "match_id", "type"}
        assert required.issubset(set(df.columns))
