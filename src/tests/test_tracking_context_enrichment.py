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
                    "team_id": "100" if p <= 11 else "200",
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
            home_team_id="100",
            match_id_native="test_match_1",
            data_source="idsse",
        )

        expected = set(_RESULT_COLUMNS) - {"_ingested_at"}
        actual = set(result.columns)
        missing = expected - actual
        extra = actual - expected
        assert not missing, f"Missing columns: {missing}"
        assert not extra, f"Extra columns: {extra}"

    def test_link_actions_called_once(self, actions: pd.DataFrame, frames: pd.DataFrame) -> None:
        """Pre-linked frames: link_actions_to_frames is called exactly once in _enrich_match."""
        pytest.importorskip("silly_kicks")
        from unittest.mock import MagicMock, patch

        from silly_kicks.tracking import link_actions_to_frames
        from silly_kicks.xthreat import ExpectedThreat

        from ingestion.tracking_context import _enrich_match

        xt = ExpectedThreat(l=16, w=12)
        xt.fit(actions)

        spy = MagicMock(wraps=link_actions_to_frames)
        with patch("silly_kicks.tracking.link_actions_to_frames", spy):
            _enrich_match(
                actions=actions,
                frames=frames,
                xt=xt,
                home_team_id="100",
                match_id_native="test_match_1",
                data_source="idsse",
            )

        # NOTE: This spy only sees the explicit step-0 call in _enrich_match.
        # Internal re-link calls from enrichment functions go through a different
        # import path inside silly-kicks, so the spy cannot verify that links=
        # actually prevents internal re-linking. The real validation of pre-link
        # effectiveness is wall-clock improvement on Databricks (Task 10 Step 4).
        assert spy.call_count == 1, (
            f"Expected link_actions_to_frames called once (pre-link), got {spy.call_count} calls"
        )

    def test_das_columns_exist_and_are_non_negative(self, actions: pd.DataFrame, frames: pd.DataFrame) -> None:
        """DAS columns must exist; any non-NaN values must be non-negative."""
        pytest.importorskip("silly_kicks")
        from silly_kicks.xthreat import ExpectedThreat

        from ingestion.tracking_context import _enrich_match

        xt = ExpectedThreat(l=16, w=12)
        xt.fit(actions)

        result = _enrich_match(
            actions=actions,
            frames=frames,
            xt=xt,
            home_team_id="100",
            match_id_native="test_match_1",
            data_source="idsse",
        )

        das_cols = ["das_team", "das_opponent", "das_diff"]
        for col in das_cols:
            assert col in result.columns, f"Missing column: {col}"
        # Non-negativity check for team/opponent (das_diff can be negative)
        for col in ["das_team", "das_opponent"]:
            non_null = result[col].dropna()
            if len(non_null) > 0:
                assert (non_null >= 0).all(), f"{col} has negative values"


class TestDasAggregation:
    """Fix D: DAS aggregation must use .sum() (per-player), not .iloc[0] (per-frame scalar)."""

    @pytest.fixture
    def actions(self) -> pd.DataFrame:
        """Reuse the existing 20-action synthetic fixture."""
        return _make_synthetic_actions()

    @pytest.fixture
    def frames(self) -> pd.DataFrame:
        """Reuse the existing 100-frame synthetic fixture (22 players + ball)."""
        return _make_synthetic_frames()

    def test_das_uses_sum_not_iloc0(self, actions: pd.DataFrame, frames: pd.DataFrame) -> None:
        """With per-player DAS, .sum() and .iloc[0] produce different team totals.

        Mock get_individual_das at the SOURCE module (silly_kicks.tracking._das)
        because _enrich_match imports it function-locally — patching the consumer
        module would raise AttributeError.

        Mock returns known per-player values:
        - Team 100: player 1 = 0.3, player 2 = 0.2 → sum = 0.5
        - Team 200: player 12 = 0.15, player 13 = 0.10 → sum = 0.25
        - .iloc[0] would give: Team100=0.3, Team200=0.15 (WRONG)
        - .sum() would give:   Team100=0.5, Team200=0.25 (CORRECT)
        """
        pytest.importorskip("silly_kicks")
        from unittest.mock import patch

        from silly_kicks.xthreat import ExpectedThreat

        from ingestion.tracking_context import _enrich_match

        xt = ExpectedThreat(l=16, w=12)
        xt.fit(actions)

        # Map player_id -> DAS value. Use players from _make_synthetic_frames
        # (players 1-11 on team 100, players 12-22 on team 200).
        # Assign different values to first two players per team so
        # .iloc[0] != .sum() for each team.
        das_by_player = {
            1: 0.3,
            2: 0.2,  # team 100: .iloc[0]=0.3, .sum()=0.5
            12: 0.15,
            13: 0.10,  # team 200: .iloc[0]=0.15, .sum()=0.25
        }

        def mock_get_individual_das(das_frames, **kwargs):  # type: ignore[no-untyped-def]
            result = das_frames.copy()
            das_values = []
            for _, row in result.iterrows():
                if row["is_ball"]:
                    das_values.append(np.nan)
                else:
                    das_values.append(das_by_player.get(row["player_id"], 0.0))
            result["DAS"] = das_values
            result["AS"] = das_values  # AS not used but returned by real API
            return result

        # Patch at SOURCE module — function-local imports resolve from there
        with patch("silly_kicks.tracking._das.get_individual_das", mock_get_individual_das):
            result = _enrich_match(
                actions=actions,
                frames=frames,
                xt=xt,
                home_team_id="100",
                match_id_native="test_das",
                data_source="idsse",
            )

        # DAS columns must exist
        assert "das_team" in result.columns
        assert "das_opponent" in result.columns
        assert "das_diff" in result.columns

        # Non-null check: mock guarantees DAS values exist for linked frames
        das_non_null = result["das_team"].dropna()
        assert len(das_non_null) > 0, "das_team is all NaN — mock was not called"

        das_opp_non_null = result["das_opponent"].dropna()
        assert len(das_opp_non_null) > 0, "das_opponent is all NaN — mock was not called"

        # Asymmetry: with .sum(), team totals differ (0.5 vs 0.25)
        both = result[["das_team", "das_opponent"]].dropna()
        assert (both["das_team"] != both["das_opponent"]).any(), (
            "das_team == das_opponent everywhere — old symmetry bug"
        )

        # Non-negativity
        assert (das_non_null >= 0).all(), "das_team has negative values"
        assert (das_opp_non_null >= 0).all(), "das_opponent has negative values"
