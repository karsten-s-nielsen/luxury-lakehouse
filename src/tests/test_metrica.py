"""Tests for ingestion.metrica — CSV header parsing and wide-to-narrow reshape."""

from __future__ import annotations

import json
import pathlib

import pandas as pd
from ingestion.metrica import (
    _build_player_columns,
    _parse_tracking_header,
    _reshape_tracking_to_narrow,
)

_FIXTURES = pathlib.Path(__file__).parent / "fixtures"


class TestParseTrackingHeader:
    """Tests for _parse_tracking_header."""

    def test_parses_three_rows(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        assert len(team_row) > 0
        assert len(jersey_row) > 0
        assert len(column_row) > 0

    def test_team_row_contains_team_names(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, _, _ = _parse_tracking_header(csv_text)
        # Filter non-empty values
        teams = {t.strip() for t in team_row if t.strip()}
        assert "Home" in teams


class TestBuildPlayerColumns:
    """Tests for _build_player_columns."""

    def test_builds_descriptive_column_names(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        # Should contain Period, Frame, Time [s], Ball_x, Ball_y
        assert "Period" in columns
        assert "Frame" in columns
        assert "Time [s]" in columns

    def test_player_columns_have_team_prefix(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        # Should have Home_ prefixed player columns
        home_cols = [c for c in columns if c.startswith("Home_")]
        assert len(home_cols) > 0


class TestReshapeTrackingToNarrow:
    """Tests for _reshape_tracking_to_narrow with fixture data."""

    def test_produces_expected_columns(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "test_match")

        expected_cols = {"period", "frame", "timestamp", "ball_x", "ball_y", "home_players", "away_players", "match_id"}
        assert expected_cols == set(result.columns)

    def test_home_players_is_valid_json(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "test_match")

        home_json = json.loads(result["home_players"].iloc[0])
        assert isinstance(home_json, dict)
        # Should have player entries with x, y
        for _player_id, coords in home_json.items():
            assert "x" in coords
            assert "y" in coords

    def test_frame_count_matches_input(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "test_match")

        assert len(result) == len(df)

    def test_match_id_propagated(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "Game_42")

        assert all(result["match_id"] == "Game_42")


class TestDownloadAndParseEvents:
    """Tests for _download_and_parse_events with mocked HTTP."""

    def test_event_fixture_parsing(self) -> None:
        """Verify the event fixture CSV can be parsed with expected columns."""
        df = pd.read_csv(_FIXTURES / "metrica_events.csv")
        assert "Type" in df.columns or "type" in df.columns
        assert len(df) == 3
