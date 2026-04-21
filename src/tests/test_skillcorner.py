"""Tests for ingestion.skillcorner — SkillCorner tracking data conversion to narrow format."""

from __future__ import annotations

import json
import pathlib
from datetime import timedelta
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from ingestion.skillcorner import SKILLCORNER_MATCH_IDS, _dataset_to_rows, _smooth_tracking

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
            # kloppy uses starting_position (not position) for the Player's role
            mock_player.jersey_no = p.get("jersey_no")
            if p.get("position"):
                mock_position = MagicMock()
                mock_position.name = p["position"]
                mock_player.starting_position = mock_position
            else:
                mock_player.starting_position = None
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
            "home_team_id",
            "away_team_id",
            "x",
            "y",
            "ball_x",
            "ball_y",
            "ball_z",
            "ball_state",
            "ball_owning_team_id",
            "match_id",
            "frame_rate",
            "is_goalkeeper",
            "position_name",
            "is_visible",
        }
        assert set(rows[0].keys()) == expected_keys

    def test_is_goalkeeper_flag(self) -> None:
        """P101 and P201 are GKs (position=Goalkeeper); P102 and P202 are not."""
        fixture = _load_fixture()
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        gk_rows = [r for r in rows if r["is_goalkeeper"] is True]
        non_gk_rows = [r for r in rows if r["is_goalkeeper"] is False]
        gk_pids = {r["player_id"] for r in gk_rows}
        non_gk_pids = {r["player_id"] for r in non_gk_rows}
        assert gk_pids == {"P101", "P201"}
        assert non_gk_pids == {"P102", "P202"}

    def test_is_goalkeeper_jersey_fallback_when_no_position(self) -> None:
        """Players without position metadata fall back to jersey_no == 1."""
        fixture = _load_fixture()
        # Remove position data but set jersey numbers (GKs get #1)
        for team_key in ("home", "away"):
            for p in fixture["players"][team_key]:
                p.pop("position", None)
                # GK players (P101, P201) get jersey 1; others get non-1
                p["jersey_no"] = 1 if p["player_id"] in ("P101", "P201") else 7
        dataset = _build_mock_dataset(fixture)
        rows = _dataset_to_rows(dataset, "1925299")
        gk_pids = {r["player_id"] for r in rows if r["is_goalkeeper"]}
        non_gk_pids = {r["player_id"] for r in rows if not r["is_goalkeeper"]}
        assert gk_pids == {"P101", "P201"}
        assert non_gk_pids == {"P102", "P202"}


class TestSmoothTracking:
    """Tests for _smooth_tracking integration."""

    def test_per_player_independence(self) -> None:
        """Smoothing is applied per player — no cross-player contamination."""
        rng = np.random.default_rng(42)
        n = 15

        df = pd.DataFrame(
            {
                "player_id": ["P1"] * n + ["P2"] * n,
                "period": [1] * n + [1] * n,
                "match_id": ["skillcorner_M1"] * (2 * n),
                "frame": list(range(n)) * 2,
                "timestamp": [i / 10.0 for i in range(n)] * 2,
                "team": ["home"] * (2 * n),
                "x": np.concatenate(
                    [
                        np.linspace(-20, -10, n) + rng.normal(0, 0.02, n),
                        np.linspace(30, 40, n) + rng.normal(0, 0.02, n),
                    ]
                ),
                "y": np.zeros(2 * n),
                "ball_x": [0.0] * (2 * n),
                "ball_y": [0.0] * (2 * n),
                "frame_rate": [10] * (2 * n),
            }
        )

        smoothed = _smooth_tracking(df)

        p1 = smoothed[smoothed["player_id"] == "P1"]
        p2 = smoothed[smoothed["player_id"] == "P2"]
        assert p1["x"].max() < 0, "P1 x contaminated by P2"
        assert p2["x"].min() > 25, "P2 x contaminated by P1"

    def test_reduces_noise(self) -> None:
        """Smoothing reduces frame-to-frame jitter."""
        rng = np.random.default_rng(99)
        n = 30
        df = pd.DataFrame(
            {
                "player_id": "P1",
                "period": 1,
                "match_id": "skillcorner_M1",
                "frame": range(n),
                "timestamp": [i / 10.0 for i in range(n)],
                "team": "home",
                "x": np.linspace(0, 20, n) + rng.normal(0, 0.05, n),
                "y": np.linspace(0, 10, n) + rng.normal(0, 0.05, n),
                "ball_x": 0.0,
                "ball_y": 0.0,
                "frame_rate": 10,
            }
        )
        raw_jitter = df["x"].diff().std()

        smoothed = _smooth_tracking(df)

        assert smoothed["x"].diff().std() < raw_jitter


class TestSkillCornerMatchIDs:
    """Tests for match ID constants."""

    def test_ten_match_ids(self) -> None:
        assert len(SKILLCORNER_MATCH_IDS) == 10

    def test_default_match_id_present(self) -> None:
        assert "1925299" in SKILLCORNER_MATCH_IDS
