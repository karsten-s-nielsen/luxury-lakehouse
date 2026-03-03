"""Tests for ingestion.skillcorner — SkillCorner tracking data conversion to narrow format."""

from __future__ import annotations

import json
import pathlib
from datetime import timedelta
from unittest.mock import MagicMock

from ingestion.skillcorner import SKILLCORNER_MATCH_IDS, _dataset_to_rows

_FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _load_fixture() -> dict:
    """Load the synthetic SkillCorner fixture data."""
    return json.loads((_FIXTURES / "skillcorner_sample.json").read_text())


def _build_mock_dataset(fixture: dict) -> MagicMock:
    """Build a mock kloppy TrackingDataset from fixture data."""
    # Build team objects
    home_team = MagicMock()
    home_team.team_id = fixture["teams"]["home"]["team_id"]
    home_team.name = fixture["teams"]["home"]["name"]

    away_team = MagicMock()
    away_team.team_id = fixture["teams"]["away"]["team_id"]
    away_team.name = fixture["teams"]["away"]["name"]

    # Build player objects lookup
    player_lookup: dict[str, MagicMock] = {}
    for team_key, team_obj in [("home", home_team), ("away", away_team)]:
        for p in fixture["players"][team_key]:
            mock_player = MagicMock()
            mock_player.player_id = p["player_id"]
            mock_player.name = p["name"]
            mock_player.team = team_obj
            player_lookup[p["player_id"]] = mock_player

    # Build frame objects
    frames: list[MagicMock] = []
    for f in fixture["frames"]:
        mock_frame = MagicMock()
        mock_frame.frame_id = f["frame_id"]
        mock_frame.timestamp = timedelta(seconds=f["timestamp_seconds"])

        mock_period = MagicMock()
        mock_period.id = f["period"]
        mock_frame.period = mock_period

        # Ball coordinates
        if f["ball"]:
            mock_ball = MagicMock()
            mock_ball.x = f["ball"]["x"]
            mock_ball.y = f["ball"]["y"]
            mock_frame.ball_coordinates = mock_ball
        else:
            mock_frame.ball_coordinates = None

        # Player coordinates
        players_coords: dict[MagicMock, MagicMock] = {}
        for pid, coords in f["players"].items():
            player_obj = player_lookup[pid]
            mock_point = MagicMock()
            mock_point.x = coords["x"]
            mock_point.y = coords["y"]
            players_coords[player_obj] = mock_point
        mock_frame.players_coordinates = players_coords

        frames.append(mock_frame)

    # Build dataset
    dataset = MagicMock()
    metadata = MagicMock()
    metadata.teams = (home_team, away_team)
    dataset.metadata = metadata
    dataset.__iter__ = MagicMock(return_value=iter(frames))

    return dataset


class TestDatasetToRows:
    """Tests for _dataset_to_rows conversion."""

    def test_produces_rows(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        assert len(rows) > 0

    def test_match_id_prefixed(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        assert all(r["match_id"] == "skillcorner_1925299" for r in rows)

    def test_frame_rate_always_10(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        assert all(r["frame_rate"] == 10 for r in rows)

    def test_home_away_separation(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        teams = {r["team"] for r in rows}
        assert teams == {"home", "away"}

    def test_player_ids_present(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        pids = {r["player_id"] for r in rows}
        assert "P101" in pids
        assert "P201" in pids

    def test_periods_present(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        periods = {r["period"] for r in rows}
        assert periods == {1, 2}

    def test_coordinates_preserved(self) -> None:
        """Verify center-origin meter coordinates pass through."""
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        # First frame, P101 should have x=-20.0, y=10.0
        p101_frame0 = [r for r in rows if r["player_id"] == "P101" and r["frame"] == 0]
        assert len(p101_frame0) == 1
        assert p101_frame0[0]["x"] == -20.0
        assert p101_frame0[0]["y"] == 10.0

    def test_ball_coordinates_extracted(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        # First frame ball: x=0.5, y=-1.0
        frame0 = [r for r in rows if r["frame"] == 0]
        assert len(frame0) > 0
        assert frame0[0]["ball_x"] == 0.5
        assert frame0[0]["ball_y"] == -1.0

    def test_timestamp_from_timedelta(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        # Second frame has timestamp_seconds=0.1
        frame1 = [r for r in rows if r["frame"] == 1]
        assert len(frame1) > 0
        assert frame1[0]["timestamp"] == 0.1

    def test_row_schema(self) -> None:
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        expected_keys = {
            "period",
            "frame",
            "timestamp",
            "player_id",
            "team",
            "x",
            "y",
            "ball_x",
            "ball_y",
            "match_id",
            "frame_rate",
        }
        assert set(rows[0].keys()) == expected_keys


class TestSkillCornerMatchIDs:
    """Tests for match ID constants."""

    def test_ten_match_ids(self) -> None:
        assert len(SKILLCORNER_MATCH_IDS) == 10

    def test_default_match_id_present(self) -> None:
        assert "1925299" in SKILLCORNER_MATCH_IDS
