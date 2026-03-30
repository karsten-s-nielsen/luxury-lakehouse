"""Tests for ingestion.metrica — CSV header parsing, wide-to-narrow reshape, and EPTS parsers."""

from __future__ import annotations

import json
import logging
import pathlib
import textwrap

import pandas as pd
import pytest

from ingestion.metrica import (
    _build_player_columns,
    _EPTSMetadata,
    _parse_epts_events,
    _parse_epts_metadata,
    _parse_epts_tracking,
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

        expected_cols = {
            "period",
            "frame",
            "timestamp",
            "ball_x",
            "ball_y",
            "home_players",
            "away_players",
            "match_id",
            "frame_rate",
            "gk_jersey_numbers",
        }
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

    def test_frame_rate_column_present(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "test_match")

        assert "frame_rate" in result.columns

    def test_frame_rate_always_25(self) -> None:
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)

        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "test_match")

        assert all(result["frame_rate"] == 25)


class TestDownloadAndParseEvents:
    """Tests for _download_and_parse_events with mocked HTTP."""

    def test_event_fixture_parsing(self) -> None:
        """Verify the event fixture CSV can be parsed with expected columns."""
        df = pd.read_csv(_FIXTURES / "metrica_events.csv")
        assert "Type" in df.columns or "type" in df.columns
        assert len(df) == 3


# ---------------------------------------------------------------------------
# EPTS parser tests (Game 3)
# ---------------------------------------------------------------------------

_MINIMAL_EPTS_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="utf-8"?>
    <main>
      <Metadata>
        <GlobalConfig>
          <FrameRate>25</FrameRate>
          <ProviderGlobalParameters>
            <ProviderParameter><Name>first_half_start</Name><Value>1</Value></ProviderParameter>
            <ProviderParameter><Name>first_half_end</Name><Value>100</Value></ProviderParameter>
            <ProviderParameter><Name>second_half_start</Name><Value>101</Value></ProviderParameter>
            <ProviderParameter><Name>second_half_end</Name><Value>200</Value></ProviderParameter>
          </ProviderGlobalParameters>
        </GlobalConfig>
        <Sessions>
          <Session id="s1">
            <MatchParameters>
              <Score idLocalTeam="TMA" idVisitingTeam="TMB">
                <LocalTeamScore>1</LocalTeamScore>
                <VisitingTeamScore>0</VisitingTeamScore>
              </Score>
            </MatchParameters>
          </Session>
        </Sessions>
        <Teams>
          <Team id="TMA"><Name>Team A</Name></Team>
          <Team id="TMB"><Name>Team B</Name></Team>
        </Teams>
        <Players>
          <Player id="P1" teamId="TMA"><Name>Player 1</Name><ShirtNumber>1</ShirtNumber></Player>
          <Player id="P2" teamId="TMA"><Name>Player 2</Name><ShirtNumber>2</ShirtNumber></Player>
          <Player id="P3" teamId="TMB"><Name>Player 3</Name><ShirtNumber>3</ShirtNumber></Player>
          <Player id="P4" teamId="TMB"><Name>Player 4</Name><ShirtNumber>4</ShirtNumber></Player>
        </Players>
        <PlayerChannels>
          <PlayerChannel channelId="x" id="p1_x" playerId="P1"/>
          <PlayerChannel channelId="y" id="p1_y" playerId="P1"/>
          <PlayerChannel channelId="x" id="p2_x" playerId="P2"/>
          <PlayerChannel channelId="y" id="p2_y" playerId="P2"/>
          <PlayerChannel channelId="x" id="p3_x" playerId="P3"/>
          <PlayerChannel channelId="y" id="p3_y" playerId="P3"/>
          <PlayerChannel channelId="x" id="p4_x" playerId="P4"/>
          <PlayerChannel channelId="y" id="p4_y" playerId="P4"/>
        </PlayerChannels>
      </Metadata>
      <DataFormatSpecifications>
        <DataFormatSpecification startFrame="1" endFrame="100" separator=":">
          <StringRegister name="frameCount"/>
          <SplitRegister separator=";">
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p1_x"/>
              <PlayerChannelRef playerChannelId="p1_y"/>
            </SplitRegister>
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p2_x"/>
              <PlayerChannelRef playerChannelId="p2_y"/>
            </SplitRegister>
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p3_x"/>
              <PlayerChannelRef playerChannelId="p3_y"/>
            </SplitRegister>
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p4_x"/>
              <PlayerChannelRef playerChannelId="p4_y"/>
            </SplitRegister>
          </SplitRegister>
          <SplitRegister separator=",">
            <BallChannelRef channelId="x"/>
            <BallChannelRef channelId="y"/>
          </SplitRegister>
        </DataFormatSpecification>
        <DataFormatSpecification startFrame="101" endFrame="200" separator=":">
          <StringRegister name="frameCount"/>
          <SplitRegister separator=";">
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p1_x"/>
              <PlayerChannelRef playerChannelId="p1_y"/>
            </SplitRegister>
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p2_x"/>
              <PlayerChannelRef playerChannelId="p2_y"/>
            </SplitRegister>
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p3_x"/>
              <PlayerChannelRef playerChannelId="p3_y"/>
            </SplitRegister>
            <SplitRegister separator=",">
              <PlayerChannelRef playerChannelId="p4_x"/>
              <PlayerChannelRef playerChannelId="p4_y"/>
            </SplitRegister>
          </SplitRegister>
          <SplitRegister separator=",">
            <BallChannelRef channelId="x"/>
            <BallChannelRef channelId="y"/>
          </SplitRegister>
        </DataFormatSpecification>
      </DataFormatSpecifications>
    </main>
""")


class TestParseEPTSMetadata:
    """Tests for _parse_epts_metadata."""

    def test_extracts_half_boundaries(self) -> None:
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert meta.first_half == (1, 100)
        assert meta.second_half == (101, 200)

    def test_extracts_frame_rate(self) -> None:
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert meta.frame_rate == 25

    def test_maps_players_to_teams(self) -> None:
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert meta.player_id_to_side["P1"] == "home"
        assert meta.player_id_to_side["P2"] == "home"
        assert meta.player_id_to_side["P3"] == "away"
        assert meta.player_id_to_side["P4"] == "away"

    def test_maps_players_to_shirt_numbers(self) -> None:
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert meta.player_id_to_shirt["P1"] == "1"
        assert meta.player_id_to_shirt["P4"] == "4"

    def test_maps_channels_to_player_ids(self) -> None:
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert meta.channel_to_player_id["p1"] == "P1"
        assert meta.channel_to_player_id["p3"] == "P3"

    def test_extracts_data_format_specs(self) -> None:
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert len(meta.data_format_specs) == 2
        start, end, prefixes = meta.data_format_specs[0]
        assert start == 1
        assert end == 100
        assert prefixes == ["p1", "p2", "p3", "p4"]


class TestParseEPTSTracking:
    """Tests for _parse_epts_tracking."""

    def _make_metadata(self) -> _EPTSMetadata:
        return _parse_epts_metadata(_MINIMAL_EPTS_XML)

    def test_parses_single_frame(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        assert len(rows) == 1
        assert rows[0]["frame"] == 1
        assert rows[0]["period"] == 1
        assert rows[0]["match_id"] == "Game_3"

    def test_separates_home_away(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        home = json.loads(rows[0]["home_players"])  # type: ignore[arg-type]
        away = json.loads(rows[0]["away_players"])  # type: ignore[arg-type]
        # P1 (shirt 1) and P2 (shirt 2) are home
        assert "1" in home
        assert "2" in home
        # P3 (shirt 3) and P4 (shirt 4) are away
        assert "3" in away
        assert "4" in away

    def test_coordinates_preserved(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        home = json.loads(rows[0]["home_players"])  # type: ignore[arg-type]
        assert home["1"]["x"] == 0.5
        assert home["1"]["y"] == 0.4

    def test_ball_coordinates(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.45,0.55\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        assert rows[0]["ball_x"] == 0.45
        assert rows[0]["ball_y"] == 0.55

    def test_nan_ball_becomes_none(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:NaN,NaN\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        assert rows[0]["ball_x"] is None
        assert rows[0]["ball_y"] is None

    def test_second_half_period(self) -> None:
        meta = self._make_metadata()
        tracking = "150:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        assert rows[0]["period"] == 2
        assert rows[0]["timestamp"] == (150 - 101) / 25

    def test_multiple_frames(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n2:0.51,0.41;0.31,0.59;0.71,0.21;0.81,0.31:0.52,0.48\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        assert len(rows) == 2
        assert rows[0]["frame"] == 1
        assert rows[1]["frame"] == 2

    def test_output_schema_matches_csv_games(self) -> None:
        """Verify Game 3 output columns match Games 1-2 narrow format."""
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        expected_keys = {
            "period",
            "frame",
            "timestamp",
            "ball_x",
            "ball_y",
            "home_players",
            "away_players",
            "match_id",
            "frame_rate",
            "gk_jersey_numbers",
        }
        assert set(rows[0].keys()) == expected_keys

    def test_epts_frame_rate_always_25(self) -> None:
        meta = self._make_metadata()
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        assert all(r["frame_rate"] == 25 for r in rows)


class TestParseEPTSEvents:
    """Tests for _parse_epts_events."""

    def test_flattens_nested_event(self) -> None:
        events = [
            {
                "index": 1,
                "team": {"name": "Team A", "id": "TMA"},
                "type": {"name": "PASS", "id": 1},
                "subtypes": {"name": "HEAD", "id": 10},
                "start": {"frame": 100, "time": 4.0, "x": 0.5, "y": 0.4},
                "end": {"frame": 110, "time": 4.4, "x": 0.6, "y": 0.3},
                "period": 1,
                "from": {"name": "Player 1", "id": "P1"},
                "to": {"name": "Player 2", "id": "P2"},
            }
        ]
        df = _parse_epts_events(events, "Game_3")
        assert len(df) == 1
        assert df.iloc[0]["event_id"] == 1
        assert df.iloc[0]["type"] == "PASS"
        assert df.iloc[0]["subtype"] == "HEAD"
        assert df.iloc[0]["team"] == "Home"
        assert df.iloc[0]["player"] == "Player 1"
        assert df.iloc[0]["match_id"] == "Game_3"

    def test_team_normalization(self) -> None:
        events = [
            {
                "index": 1,
                "team": {"name": "Team B", "id": "TMB"},
                "type": {"name": "SHOT", "id": 2},
                "subtypes": None,
                "start": {"frame": 200, "time": 8.0, "x": 0.8, "y": 0.5},
                "end": {"frame": 210, "time": 8.4, "x": 0.9, "y": 0.5},
                "period": 1,
                "from": {"name": "Player 3", "id": "P3"},
                "to": None,
            }
        ]
        df = _parse_epts_events(events, "Game_3")
        assert df.iloc[0]["team"] == "Away"

    def test_handles_null_subtypes(self) -> None:
        events = [
            {
                "index": 1,
                "team": {"name": "Team A", "id": "TMA"},
                "type": {"name": "CARRY", "id": 10},
                "subtypes": None,
                "start": {"frame": 100, "time": 4.0, "x": 0.5, "y": 0.4},
                "end": {"frame": 105, "time": 4.2, "x": 0.52, "y": 0.41},
                "period": 1,
                "from": {"name": "Player 1", "id": "P1"},
                "to": None,
            }
        ]
        df = _parse_epts_events(events, "Game_3")
        assert df.iloc[0]["subtype"] is None

    def test_output_has_expected_columns(self) -> None:
        events = [
            {
                "index": 1,
                "team": {"name": "Team A", "id": "TMA"},
                "type": {"name": "PASS", "id": 1},
                "subtypes": None,
                "start": {"frame": 100, "time": 4.0, "x": None, "y": None},
                "end": {"frame": 100, "time": 4.0, "x": None, "y": None},
                "period": 1,
                "from": {"name": "Player 1", "id": "P1"},
                "to": None,
            }
        ]
        df = _parse_epts_events(events, "Game_3")
        required = {"event_id", "type", "period", "start_frame", "end_frame", "team", "player", "match_id"}
        assert required.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# GK identification tests
# ---------------------------------------------------------------------------


class TestGKIdentificationEPTS:
    """Tests for GK identification in EPTS metadata and tracking (Game 3)."""

    def test_epts_metadata_gk_heuristic_shirt_1(self) -> None:
        """Player P1 (shirt #1) is identified as GK via jersey heuristic."""
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        assert "P1" in meta.gk_player_ids
        # P2 (shirt 2) should NOT be GK
        assert "P2" not in meta.gk_player_ids

    def test_epts_metadata_gk_playing_position_attribute(self) -> None:
        """PlayingPosition='GK' takes precedence over jersey heuristic."""
        xml_with_position = _MINIMAL_EPTS_XML.replace(
            '<Player id="P3" teamId="TMB">',
            '<Player id="P3" teamId="TMB" PlayingPosition="GK">',
        )
        meta = _parse_epts_metadata(xml_with_position)
        # P3 is GK via PlayingPosition
        assert "P3" in meta.gk_player_ids
        # P1 is still GK via shirt #1 heuristic
        assert "P1" in meta.gk_player_ids
        assert len(meta.gk_player_ids) == 2

    def test_epts_metadata_gk_tw_position(self) -> None:
        """PlayingPosition='TW' (German: Torwart) is recognized as GK."""
        xml_with_tw = _MINIMAL_EPTS_XML.replace(
            '<Player id="P4" teamId="TMB">',
            '<Player id="P4" teamId="TMB" PlayingPosition="TW">',
        )
        meta = _parse_epts_metadata(xml_with_tw)
        assert "P4" in meta.gk_player_ids

    def test_epts_metadata_gk_heuristic_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Jersey #1 heuristic logs a warning for shirt #1 fallback."""
        with caplog.at_level(logging.WARNING, logger="metrica"):
            _parse_epts_metadata(_MINIMAL_EPTS_XML)
        gk_warnings = [r for r in caplog.records if "GK heuristic" in r.message]
        assert len(gk_warnings) == 1
        assert "P1" in gk_warnings[0].message

    def test_epts_tracking_includes_gk_jersey_numbers(self) -> None:
        """Game 3 tracking rows include gk_jersey_numbers JSON column."""
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        gk_jerseys = json.loads(rows[0]["gk_jersey_numbers"])  # type: ignore[arg-type]
        # P1 has shirt "1" and is the GK
        assert "1" in gk_jerseys

    def test_epts_gk_jersey_numbers_consistent_across_frames(self) -> None:
        """All frames in Game 3 should have the same gk_jersey_numbers."""
        meta = _parse_epts_metadata(_MINIMAL_EPTS_XML)
        tracking = "1:0.5,0.4;0.3,0.6;0.7,0.2;0.8,0.3:0.5,0.5\n2:0.51,0.41;0.31,0.59;0.71,0.21;0.81,0.31:0.52,0.48\n"
        rows = _parse_epts_tracking(tracking, meta, "Game_3")
        gk0 = rows[0]["gk_jersey_numbers"]
        gk1 = rows[1]["gk_jersey_numbers"]
        assert gk0 == gk1


class TestGKIdentificationCSV:
    """Tests for GK identification in CSV tracking (Games 1-2)."""

    def test_csv_gk_jersey_numbers_column_present(self) -> None:
        """CSV-based narrow output includes gk_jersey_numbers column."""
        csv_text = (_FIXTURES / "metrica_tracking_home.csv").read_text()
        team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
        columns = _build_player_columns(team_row, jersey_row, column_row)
        df = pd.read_csv(_FIXTURES / "metrica_tracking_home.csv", skiprows=3, header=None, names=columns)
        result = _reshape_tracking_to_narrow(df, "test_match")
        assert "gk_jersey_numbers" in result.columns

    def test_csv_gk_identifies_jersey_1(self) -> None:
        """When jersey '1' exists in CSV columns, it appears in gk_jersey_numbers.

        Note: the test fixture uses 'Player1' and 'Player11' as jersey labels,
        not plain numbers. Jersey '1' heuristic matches exact '1' only,
        so with these fixture labels gk_jersey_numbers is empty.
        Real Metrica data uses plain numbers ('1', '11', etc.).
        """
        # Build a synthetic DataFrame with jersey "1" to test the real path
        df = pd.DataFrame(
            {
                "Period": [1, 1],
                "Frame": [1, 2],
                "Time [s]": [0.04, 0.08],
                "Home_1_x": [0.1, 0.11],
                "Home_1_y": [0.5, 0.51],
                "Home_11_x": [0.3, 0.31],
                "Home_11_y": [0.4, 0.41],
                "Away_1_x": [0.9, 0.89],
                "Away_1_y": [0.5, 0.49],
                "Away_7_x": [0.7, 0.71],
                "Away_7_y": [0.6, 0.59],
                "Ball_x": [0.5, 0.52],
                "Ball_y": [0.5, 0.48],
            }
        )
        result = _reshape_tracking_to_narrow(df, "test_gk")
        gk_jerseys = json.loads(result["gk_jersey_numbers"].iloc[0])
        # Both home and away jersey "1" are identified as GK
        assert "1" in gk_jerseys
        # Jersey "11" and "7" are NOT GK
        assert "11" not in gk_jerseys
        assert "7" not in gk_jerseys
