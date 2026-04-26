"""Tests for Off-Ball Expected Threat analytics module."""

from __future__ import annotations

import pytest

pytest.importorskip("jax")

from collections import namedtuple
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from analytics.expected_threat import XTGrid
from analytics.off_ball_xt import (
    OffBallXtParams,
    compute_off_ball_xt_frame,
    compute_off_ball_xt_match,
)
from analytics.pitch_control import PitchControlParams

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Production xT grids are SPADL 105x68 — match that here so tests exercise
# the cross-coordinate-system lookup path that production code takes
# (StatsBomb 120x80 tracking input → SPADL grid lookup via XTGrid.lookup).
_TEST_VALUES = np.zeros((12, 8), dtype=np.float64)
for _zx in range(12):
    for _zy in range(8):
        _TEST_VALUES[_zx, _zy] = (_zx + 1) * 0.01  # 0.01 to 0.12

_TEST_GRID = XTGrid(
    values=_TEST_VALUES,
    pitch_length=105.0,
    pitch_width=68.0,
    coord_system="spadl",
    competition_id="test",
)


def _make_frame(
    n_home: int = 3,
    n_away: int = 3,
    home_x: float = 60.0,
    away_x: float = 40.0,
) -> pd.DataFrame:
    """Build a minimal frame DataFrame with home and away players."""
    rows: list[dict[str, object]] = []
    for i in range(n_home):
        rows.append(
            {
                "player_id": f"home_{i}",
                "team": "home",
                "x": home_x + i * 5.0,
                "y": 40.0 + i * 5.0,
                "velocity_x": 1.0,
                "velocity_y": 0.0,
            }
        )
    for i in range(n_away):
        rows.append(
            {
                "player_id": f"away_{i}",
                "team": "away",
                "x": away_x + i * 5.0,
                "y": 40.0 + i * 5.0,
                "velocity_x": -1.0,
                "velocity_y": 0.0,
            }
        )
    return pd.DataFrame(rows)


def _make_match_tracking(
    n_frames: int = 50,
    frame_rate: int = 25,
    match_id: str = "test_match_1",
) -> pd.DataFrame:
    """Build a minimal multi-frame tracking DataFrame."""
    rows: list[dict[str, object]] = []
    for frame in range(n_frames):
        for team, prefix, base_x in [("home", "h", 60.0), ("away", "a", 40.0)]:
            for i in range(3):
                rows.append(
                    {
                        "player_id": f"{prefix}_{i}",
                        "team": team,
                        "x": base_x + i * 5.0 + frame * 0.1,
                        "y": 40.0 + i * 5.0,
                        "velocity_x": 1.0 if team == "home" else -1.0,
                        "velocity_y": 0.0,
                        "frame": frame,
                        "period": 1,
                        "frame_rate": frame_rate,
                        "match_id": match_id,
                    }
                )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TestOffBallXtParams
# ---------------------------------------------------------------------------


class TestOffBallXtParams:
    """Test OffBallXtParams dataclass."""

    def test_defaults(self) -> None:
        params = OffBallXtParams()
        assert params.sample_fps == 1.0

    def test_custom_override(self) -> None:
        params = OffBallXtParams(sample_fps=5.0)
        assert params.sample_fps == 5.0


# ---------------------------------------------------------------------------
# TestComputeOffBallXtFrame
# ---------------------------------------------------------------------------


class TestComputeOffBallXtFrame:
    """Test per-frame Off-Ball xT computation."""

    def test_basic_frame(self) -> None:
        frame = _make_frame()
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert len(result) == 6
        assert "off_ball_xt" in result.columns
        assert all(result["off_ball_xt"] >= 0)

    def test_high_xt_zone(self) -> None:
        # All players near attacking goal (StatsBomb x≈115 → last grid zone)
        frame = _make_frame(home_x=115.0, away_x=115.0)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert all(result["xt_value"] > 0.1)

    def test_low_xt_zone(self) -> None:
        # All players in own half (StatsBomb x≈5 → first grid zone)
        frame = _make_frame(home_x=5.0, away_x=5.0)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert all(result["xt_value"] <= 0.02)

    def test_empty_frame(self) -> None:
        empty = pd.DataFrame(columns=pd.Index(["player_id", "team", "x", "y", "velocity_x", "velocity_y"]))
        result = compute_off_ball_xt_frame(empty, _TEST_GRID)
        assert len(result) == 0

    def test_output_columns(self) -> None:
        frame = _make_frame()
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        expected_cols = {"player_id", "team", "x", "y", "xt_value", "pitch_control", "off_ball_xt"}
        assert set(result.columns) == expected_cols

    def test_pitch_control_bounded(self) -> None:
        frame = _make_frame()
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert all(result["pitch_control"] >= 0)
        assert all(result["pitch_control"] <= 1)


# ---------------------------------------------------------------------------
# TestComputeOffBallXtMatch
# ---------------------------------------------------------------------------


class TestComputeOffBallXtMatch:
    """Test match-level Off-Ball xT aggregation."""

    def test_full_match(self) -> None:
        tracking = _make_match_tracking(n_frames=50, frame_rate=25)
        result = compute_off_ball_xt_match(tracking, _TEST_GRID)
        assert len(result) > 0
        assert "total_off_ball_xt" in result.columns
        assert "avg_off_ball_xt" in result.columns
        assert "frames_sampled" in result.columns

    def test_sampling_rate(self) -> None:
        # 50 frames at 25fps = 2 seconds. At 1fps sampling → ~2 frames sampled
        tracking = _make_match_tracking(n_frames=50, frame_rate=25)
        result = compute_off_ball_xt_match(tracking, _TEST_GRID)
        if not result.empty:
            sampled = int(result["frames_sampled"].iloc[0])
            assert sampled >= 1
            assert sampled <= 5  # rough bound

    def test_empty_tracking(self) -> None:
        empty = pd.DataFrame(
            columns=pd.Index(
                [
                    "player_id",
                    "team",
                    "x",
                    "y",
                    "velocity_x",
                    "velocity_y",
                    "frame",
                    "period",
                    "frame_rate",
                    "match_id",
                ]
            )
        )
        result = compute_off_ball_xt_match(empty, _TEST_GRID)
        assert len(result) == 0

    def test_match_id_preserved(self) -> None:
        tracking = _make_match_tracking(match_id="custom_match")
        result = compute_off_ball_xt_match(tracking, _TEST_GRID)
        if not result.empty:
            assert all(result["match_id"] == "custom_match")

    def test_all_players_represented(self) -> None:
        tracking = _make_match_tracking(n_frames=50, frame_rate=25)
        result = compute_off_ball_xt_match(tracking, _TEST_GRID)
        # 3 home + 3 away players
        assert len(result) == 6


# ---------------------------------------------------------------------------
# TestIntegrationWithPitchControl
# ---------------------------------------------------------------------------


class TestIntegrationWithPitchControl:
    """Test Off-Ball xT integration with pitch control model."""

    def test_home_dominated_frame(self) -> None:
        # 5 home players, 1 away → home should dominate PC
        frame = _make_frame(n_home=5, n_away=1)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        home_result = result[result["team"] == "home"]
        assert all(home_result["pitch_control"] > 0.3)

    def test_away_dominated_frame(self) -> None:
        # 1 home player, 5 away → away should dominate PC
        frame = _make_frame(n_home=1, n_away=5)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        away_result = result[result["team"] == "away"]
        assert all(away_result["pitch_control"] > 0.3)

    def test_custom_pc_params(self) -> None:
        frame = _make_frame()
        pc_params = PitchControlParams(grid_cells_x=10, grid_cells_y=8)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID, pc_params)
        assert len(result) == 6


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_player_each_team(self) -> None:
        frame = _make_frame(n_home=1, n_away=1)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert len(result) == 2

    def test_same_position_all_players(self) -> None:
        frame = _make_frame(home_x=60.0, away_x=60.0)
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert len(result) == 6
        # Players share same x-zone but y-offsets may span zone boundaries
        # All should still be in the same x-zone
        assert all(result["xt_value"] > 0)

    def test_zero_velocity(self) -> None:
        frame = _make_frame()
        frame["velocity_x"] = 0.0
        frame["velocity_y"] = 0.0
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert len(result) == 6
        assert all(result["off_ball_xt"] >= 0)

    def test_off_ball_xt_non_negative(self) -> None:
        """Off-Ball xT should never be negative (PC * xT, both >= 0)."""
        frame = _make_frame()
        result = compute_off_ball_xt_frame(frame, _TEST_GRID)
        assert all(result["off_ball_xt"] >= 0)

    def test_total_greater_than_avg(self) -> None:
        """Total Off-Ball xT >= avg for multi-frame match."""
        tracking = _make_match_tracking(n_frames=100, frame_rate=25)
        result = compute_off_ball_xt_match(tracking, _TEST_GRID)
        if not result.empty:
            for _, row in result.iterrows():
                assert row["total_off_ball_xt"] >= row["avg_off_ball_xt"]


# ---------------------------------------------------------------------------
# TestLoadXtGridFromSpark
# ---------------------------------------------------------------------------


class TestLoadXtGridFromSpark:
    """Tests for Delta table grid loading. Returns XTGrid (not raw ndarray)."""

    def test_loads_from_delta_table(self) -> None:
        from ingestion.off_ball_xt import _load_xt_grid_from_spark

        Row = namedtuple("Row", ["zone_x", "zone_y", "xt_value"])
        mock_rows = [Row(x, y, 0.01 * (x + 1)) for x in range(12) for y in range(8)]
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = mock_rows
        grid = _load_xt_grid_from_spark(mock_spark, "soccer_analytics")
        assert isinstance(grid, XTGrid)
        assert grid.shape == (12, 8)
        assert grid.coord_system == "spadl"
        assert grid.competition_id == "global"
        assert grid.values[0, 0] == pytest.approx(0.01)
        assert grid.values[11, 7] == pytest.approx(0.12)

    def test_loads_arbitrary_resolution(self) -> None:
        """Bug 2 fix: loader derives shape from query results, not hardcoded 12x8."""
        from ingestion.off_ball_xt import _load_xt_grid_from_spark

        Row = namedtuple("Row", ["zone_x", "zone_y", "xt_value"])
        # Simulate a 24x16 grid (ExT v2 resolution) in the bronze table
        mock_rows = [Row(x, y, 0.01 * (x + 1)) for x in range(24) for y in range(16)]
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = mock_rows
        grid = _load_xt_grid_from_spark(mock_spark, "soccer_analytics")
        assert grid.shape == (24, 16)
        assert grid.values[23, 15] == pytest.approx(0.24)

    def test_raises_on_missing_table(self) -> None:
        from ingestion.off_ball_xt import _load_xt_grid_from_spark

        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Table not found")
        with pytest.raises(RuntimeError, match="Run compute_expected_threat"):
            _load_xt_grid_from_spark(mock_spark, "soccer_analytics")

    def test_raises_on_empty_global_grid(self) -> None:
        """Bug 2 follow-on: explicit error when table exists but has no rows."""
        from ingestion.off_ball_xt import _load_xt_grid_from_spark

        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        with pytest.raises(RuntimeError, match="no rows for competition_id='global'"):
            _load_xt_grid_from_spark(mock_spark, "soccer_analytics")
