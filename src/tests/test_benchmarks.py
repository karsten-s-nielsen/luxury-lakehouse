"""Performance benchmarks for critical-path functions."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from analytics.defcon_lite import DefconLiteParams, assign_defensive_credits
from analytics.line_breaking import LineBreakingParams, detect_line_breaking
from analytics.off_ball_xt import compute_off_ball_xt_frame
from analytics.pitch_control import compute_pitch_control_at_points


def _make_players_df(n_home: int = 11, n_away: int = 11) -> pd.DataFrame:
    """Create a realistic players DataFrame for benchmarking."""
    rng = np.random.default_rng(42)
    n = n_home + n_away
    return pd.DataFrame(
        {
            "player_id": range(n),
            "team": ["home"] * n_home + ["away"] * n_away,
            "x": rng.uniform(0, 120, n),
            "y": rng.uniform(0, 80, n),
            "velocity_x": rng.uniform(-5, 5, n),
            "velocity_y": rng.uniform(-5, 5, n),
        }
    )


class TestBenchmarks:
    def test_bench_batched_pitch_control(self, benchmark: Any) -> None:
        """Benchmark: batched pitch control for 60 target points."""
        players = _make_players_df()
        targets = np.array([[x, y] for x in range(10, 111, 20) for y in range(10, 71, 20)], dtype=np.float64)
        benchmark(compute_pitch_control_at_points, players, targets)

    def test_bench_off_ball_xt_frame(self, benchmark: Any) -> None:
        """Benchmark: off-ball xT for one frame (22 players)."""
        players = _make_players_df()
        xt_grid = np.random.default_rng(42).random((12, 8))
        benchmark(compute_off_ball_xt_frame, players, xt_grid)

    def test_bench_defcon_credit_assignment(self, benchmark: Any) -> None:
        """Benchmark: DEFCON credit assignment for one action."""
        rng = np.random.default_rng(42)
        action: dict[str, object] = {
            "event_id": "test_1",
            "match_id": "1",
            "competition_id": "2",
            "season_id": "3",
            "action_player_id": 1,
            "action_type": "pass",
            "action_x": 60.0,
            "action_y": 34.0,
            "offensive_value": 0.05,
        }
        defenders = pd.DataFrame(
            {
                "player_id": range(11),
                "team_id": [0] * 11,
                "x": rng.uniform(30, 105, 11),
                "y": rng.uniform(0, 68, 11),
                "velocity_x": rng.uniform(-3, 3, 11),
                "velocity_y": rng.uniform(-3, 3, 11),
            }
        )
        benchmark(assign_defensive_credits, action, defenders, DefconLiteParams())

    def test_bench_line_breaking_detection(self, benchmark: Any) -> None:
        """Benchmark: line-breaking detection for one pass."""
        rng = np.random.default_rng(42)
        opponents = pd.DataFrame(
            {
                "x": rng.uniform(30, 105, 10),
                "y": rng.uniform(0, 68, 10),
            }
        )
        benchmark(detect_line_breaking, 40.0, 40.0, 80.0, 40.0, opponents, LineBreakingParams())
