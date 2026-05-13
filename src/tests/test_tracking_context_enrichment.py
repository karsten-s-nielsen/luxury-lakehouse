"""TC-1 — UDF enrichment chain integration test.

Validates that _enrich_match produces the correct output column set
using synthetic data. Does NOT require Spark or Databricks.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def _make_synthetic_actions(n: int = 20) -> pd.DataFrame:
    """Minimal SPADL actions with game_id for silly-kicks compat."""
    rng = np.random.default_rng(42)
    team_ids = rng.choice([100, 200], n)
    player_ids = rng.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], n)
    return pd.DataFrame(
        {
            "game_id": [1] * n,
            "action_id": list(range(n)),
            "period_id": [1] * n,
            "time_seconds": np.linspace(0, 90 * 60, n),
            "team_id": team_ids,
            "player_id": player_ids,
            "team_id_native": [str(t) for t in team_ids],
            "player_id_native": [str(p) for p in player_ids],
            "type_id": rng.choice([0, 1, 2, 3], n),
            "result_id": rng.choice([0, 1], n),
            "bodypart_id": [0] * n,
            "start_x": rng.uniform(0, 105, n),
            "start_y": rng.uniform(0, 68, n),
            "end_x": rng.uniform(0, 105, n),
            "end_y": rng.uniform(0, 68, n),
            "original_event_id": [f"evt_{i}" for i in range(n)],
        }
    )


def _make_synthetic_frames(n_frames: int = 100) -> pd.DataFrame:
    """Minimal tracking frames in TRACKING_FRAMES_COLUMNS schema.

    Column names match silly_kicks.tracking.schema.TRACKING_FRAMES_COLUMNS:
    - is_goalkeeper (NOT is_gk)
    - time_seconds (NOT timestamp)
    - game_id must match actions' game_id
    - is_ball column (True for ball row, False for players)
    """
    rng = np.random.default_rng(42)
    rows = []
    for f in range(n_frames):
        t = f * 0.04  # 25 fps
        for p in range(1, 23):  # 22 players
            rows.append(
                {
                    "game_id": 1,  # Must match _make_synthetic_actions game_id
                    "frame_id": f,
                    "period_id": 1,
                    "time_seconds": t,
                    "player_id": p,
                    "team_id": 100 if p <= 11 else 200,
                    "x": rng.uniform(0, 105),
                    "y": rng.uniform(0, 68),
                    "vx": rng.uniform(-5, 5),
                    "vy": rng.uniform(-5, 5),
                    "is_goalkeeper": p in (1, 12),
                    "is_ball": False,
                }
            )
        # Ball row — is_ball=True, is_goalkeeper=False
        rows.append(
            {
                "game_id": 1,
                "frame_id": f,
                "period_id": 1,
                "time_seconds": t,
                "player_id": None,
                "team_id": None,
                "x": rng.uniform(0, 105),
                "y": rng.uniform(0, 68),
                "vx": rng.uniform(-5, 5),
                "vy": rng.uniform(-5, 5),
                "is_goalkeeper": False,
                "is_ball": True,
            }
        )
    df = pd.DataFrame(rows)
    # All required TRACKING_FRAMES_COLUMNS — link_actions_to_frames
    # hard-selects source_provider, so KeyError without it.
    df["source_provider"] = "sportec"
    df["is_goalkeeper_source"] = "native"
    df["frame_rate"] = 25.0
    df["z"] = np.nan
    df["speed"] = np.sqrt(df["vx"] ** 2 + df["vy"] ** 2)
    df["speed_source"] = "derived"
    df["ball_state"] = "alive"
    df["team_attacking_direction"] = None
    df["confidence"] = None
    df["visibility"] = None
    return df


class TestEnrichmentChain:
    """Verify _enrich_match output matches _RESULT_COLUMNS."""

    @pytest.fixture
    def actions(self) -> pd.DataFrame:
        return _make_synthetic_actions()

    @pytest.fixture
    def frames(self) -> pd.DataFrame:
        return _make_synthetic_frames()

    def test_output_columns_match_spec(self, actions: pd.DataFrame, frames: pd.DataFrame) -> None:
        pytest.importorskip("silly_kicks")
        # Minimal xT stub (12x16 grid of zeros)
        from silly_kicks.xthreat import ExpectedThreat

        from ingestion.tracking_context import _RESULT_COLUMNS, _enrich_match

        xt = ExpectedThreat(l=16, w=12)
        # Fit on synthetic actions (won't converge, but produces valid grid)
        xt.fit(actions)

        result = _enrich_match(
            actions=actions,
            frames=frames,
            xt=xt,
            home_team_id=100,
            match_id_native="test_match_1",
            data_source="idsse",
        )

        expected = set(_RESULT_COLUMNS) - {"_ingested_at"}
        actual = set(result.columns)
        missing = expected - actual
        extra = actual - expected
        assert not missing, f"Missing columns: {missing}"
        assert not extra, f"Extra columns: {extra}"
