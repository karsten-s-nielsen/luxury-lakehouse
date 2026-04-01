"""Tests for goalkeeper analytics module."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.goalkeeper import (
    PSxGModel,
    compute_gk_action_summary,
    compute_gk_collection_stats,
    compute_gk_distribution_xt,
    compute_goals_prevented,
    compute_sweeper_metrics,
    predict_psxg,
    train_psxg_model,
)

# 12x8 xT grid with known values — zone (11,7) = 0.30 (near opponent goal)
_TEST_GRID = np.zeros((12, 8), dtype=np.float64)
_TEST_GRID[11, 4] = 0.30  # opponent goal area
_TEST_GRID[0, 4] = 0.01  # own goal area
_TEST_GRID[6, 4] = 0.10  # midfield


def _make_gk_passes(
    n: int = 3,
    start_x: float = 5.0,
    start_y: float = 34.0,
    end_xs: list[float] | None = None,
    end_ys: list[float] | None = None,
) -> pd.DataFrame:
    """Build synthetic GK pass DataFrame in SPADL 105x68 coords."""
    end_xs = end_xs or [52.5] * n  # midfield
    end_ys = end_ys or [34.0] * n
    return pd.DataFrame(
        {
            "player_id": [1] * n,
            "match_id": ["m1"] * n,
            "start_x": [start_x] * n,
            "start_y": [start_y] * n,
            "end_x": end_xs,
            "end_y": end_ys,
            "action_result": ["success"] * n,
        }
    )


class TestComputeGKDistributionXT:
    """Tests for compute_gk_distribution_xt."""

    def test_basic_xt_delta(self) -> None:
        passes = _make_gk_passes(n=1, start_x=5.0, start_y=34.0, end_xs=[96.25], end_ys=[34.0])
        result = compute_gk_distribution_xt(passes, _TEST_GRID)
        assert len(result) == 1
        row = result.iloc[0]
        # start zone (0,4) = 0.01, end zone (11,4) = 0.30 → delta = 0.29
        assert pytest.approx(row["total_xt_added"], abs=0.01) == 0.29

    def test_pass_length_classification(self) -> None:
        # short (<32m), medium (32-60m), long (>60m)
        passes = _make_gk_passes(
            n=3,
            start_x=5.0,
            start_y=34.0,
            end_xs=[20.0, 50.0, 90.0],  # ~15m, ~45m, ~85m
            end_ys=[34.0, 34.0, 34.0],
        )
        result = compute_gk_distribution_xt(passes, _TEST_GRID)
        row = result.iloc[0]
        assert pytest.approx(row["short_pct"], abs=0.01) == 1.0 / 3.0
        assert pytest.approx(row["medium_pct"], abs=0.01) == 1.0 / 3.0
        assert pytest.approx(row["launch_rate"], abs=0.01) == 1.0 / 3.0

    def test_empty_passes(self) -> None:
        empty = pd.DataFrame(columns=["player_id", "match_id", "start_x", "start_y", "end_x", "end_y", "action_result"])
        result = compute_gk_distribution_xt(empty, _TEST_GRID)
        assert len(result) == 0

    def test_only_successful_passes_contribute_to_xt(self) -> None:
        """Failed passes are excluded — their end coords are interception points, not targets."""
        passes = pd.DataFrame(
            {
                "player_id": [1, 1, 1],
                "match_id": ["m1", "m1", "m1"],
                "start_x": [5.0, 5.0, 5.0],
                "start_y": [34.0, 34.0, 34.0],
                # All three passes target zone (11,4) = 0.30
                "end_x": [96.25, 96.25, 96.25],
                "end_y": [34.0, 34.0, 34.0],
                "action_result": ["success", "fail", "success"],
            }
        )
        result = compute_gk_distribution_xt(passes, _TEST_GRID)
        assert len(result) == 1
        row = result.iloc[0]
        # Only 2 successful passes should be counted, not 3.
        assert row["pass_count"] == 2
        # Each successful pass: end zone (11,4)=0.30 - start zone (0,4)=0.01 = 0.29
        assert pytest.approx(row["total_xt_added"], abs=0.001) == 0.58
        assert pytest.approx(row["xt_per_pass"], abs=0.001) == 0.29

    def test_all_failed_passes_returns_empty(self) -> None:
        """If all passes failed, no rows should be returned."""
        passes = pd.DataFrame(
            {
                "player_id": [1],
                "match_id": ["m1"],
                "start_x": [5.0],
                "start_y": [34.0],
                "end_x": [52.5],
                "end_y": [34.0],
                "action_result": ["fail"],
            }
        )
        result = compute_gk_distribution_xt(passes, _TEST_GRID)
        assert len(result) == 0


def _make_gk_actions(
    action_types: list[str] | None = None,
    results: list[str] | None = None,
) -> pd.DataFrame:
    """Build synthetic GK actions DataFrame."""
    action_types = action_types or ["keeper_claim", "keeper_claim", "keeper_punch", "keeper_save", "keeper_pick_up"]
    results = results or ["success", "fail", "success", "success", "success"]
    n = len(action_types)
    return pd.DataFrame(
        {
            "player_id": [1] * n,
            "match_id": ["m1"] * n,
            "action_type": action_types,
            "action_result": results,
            "start_x": [10.0] * n,
            "start_y": [34.0] * n,
            "end_x": [10.0] * n,
            "end_y": [34.0] * n,
            "time_seconds": list(range(n)),
        }
    )


class TestComputeGKCollectionStats:
    """Tests for compute_gk_collection_stats."""

    def test_basic_collection(self) -> None:
        actions = _make_gk_actions()
        result = compute_gk_collection_stats(actions)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["claims"] == 2
        assert row["punches"] == 1
        assert pytest.approx(row["claim_success_rate"], abs=0.01) == 0.5  # 1 success / 2 claims

    def test_empty_actions(self) -> None:
        empty = pd.DataFrame(columns=["player_id", "match_id", "action_type", "action_result"])
        result = compute_gk_collection_stats(empty)
        assert len(result) == 0


class TestComputeGKActionSummary:
    """Tests for compute_gk_action_summary."""

    def test_combines_all_metrics(self) -> None:
        passes = _make_gk_passes(n=2, end_xs=[50.0, 90.0], end_ys=[34.0, 34.0])
        actions = _make_gk_actions()
        result = compute_gk_action_summary(passes, actions, _TEST_GRID)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["pass_count"] == 2
        assert row["saves"] == 1
        assert row["claims"] == 2
        assert row["keeper_pick_ups"] == 1
        assert "total_xt_added" in row.index
        assert "claim_success_rate" in row.index


# ---------------------------------------------------------------------------
# PSxG model tests (D39)
# ---------------------------------------------------------------------------


def _make_on_target_shots(n: int = 100) -> pd.DataFrame:
    """Synthetic on-target shots with known goalmouth coordinates.

    Goals clustered at corners (high z, extreme y).
    Saves clustered at center (low z, mid y).
    """
    rng = np.random.default_rng(42)
    goals = n // 2
    saves = n - goals
    goal_y = rng.uniform(36.0, 37.0, size=goals)
    goal_z = rng.uniform(6.0, 8.0, size=goals)
    save_y = rng.uniform(39.0, 41.0, size=saves)
    save_z = rng.uniform(0.5, 2.0, size=saves)
    return pd.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(n)],
            "match_id": ["m1"] * n,
            "player_id": [10] * n,
            "end_location_y": np.concatenate([goal_y, save_y]),
            "end_location_z": np.concatenate([goal_z, save_z]),
            "shot_outcome": ["Goal"] * goals + ["Saved"] * saves,
            "is_goal": [1] * goals + [0] * saves,
        }
    )


class TestPSxGModel:
    """Tests for PSxG training, prediction, and goals prevented."""

    def test_train_returns_model(self) -> None:
        shots = _make_on_target_shots(100)
        model = train_psxg_model(shots)
        assert isinstance(model, PSxGModel)

    def test_predict_returns_probabilities(self) -> None:
        shots = _make_on_target_shots(100)
        model = train_psxg_model(shots)
        result = predict_psxg(model, shots)
        assert "psxg" in result.columns
        assert result["psxg"].between(0.0, 1.0).all()
        goal_mask = result["is_goal"] == 1
        assert result.loc[goal_mask, "psxg"].mean() > result.loc[~goal_mask, "psxg"].mean()

    def test_predict_null_for_off_target(self) -> None:
        shots = _make_on_target_shots(50)
        off = pd.DataFrame(
            {
                "event_id": ["off1", "off2"],
                "match_id": ["m1", "m1"],
                "player_id": [10, 10],
                "end_location_y": [40.0, 40.0],
                "end_location_z": [np.nan, np.nan],
                "shot_outcome": ["Off T", "Off T"],
                "is_goal": [0, 0],
            }
        )
        combined = pd.concat([shots, off], ignore_index=True)
        model = train_psxg_model(shots)
        result = predict_psxg(model, combined)
        assert result.loc[result["event_id"] == "off1", "psxg"].isna().all()

    def test_goals_prevented(self) -> None:
        gk_df = pd.DataFrame(
            {
                "player_id": [99],
                "match_id": ["m1"],
                "psxg_faced": [2.5],
                "goals_conceded": [2],
            }
        )
        result = compute_goals_prevented(gk_df)
        assert pytest.approx(result.iloc[0]["goals_prevented"], abs=0.01) == 0.5


# ---------------------------------------------------------------------------
# Sweeper-keeper metrics tests (D39)
# ---------------------------------------------------------------------------


def _make_gk_events_with_position() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "player_id": [99, 99, 99, 99, 99],
            "match_id": ["m1"] * 5,
            "start_x": [8.0, 12.0, 20.0, 5.0, 10.0],
            "start_y": [34.0, 34.0, 34.0, 34.0, 34.0],
            "action_type": ["keeper_save", "clearance", "interception", "keeper_pick_up", "keeper_save"],
            "minutes_played": [90.0] * 5,
        }
    )


class TestComputeSweeperMetrics:
    """Tests for compute_sweeper_metrics."""

    def test_basic_sweeper(self) -> None:
        events = _make_gk_events_with_position()
        result = compute_sweeper_metrics(events)
        assert len(result) == 1
        row = result.iloc[0]
        # Average distance from own goal line = mean(8, 12, 20, 5, 10) = 11.0
        assert pytest.approx(row["avg_defensive_action_distance"], abs=0.1) == 11.0
        # Penalty area extends to x=16.5. Action at x=20 is outside.
        # actions_outside_box_per_90: 1 action outside in 90 min = 1.0
        assert pytest.approx(row["actions_outside_box_per_90"], abs=0.1) == 1.0

    def test_empty(self) -> None:
        empty = pd.DataFrame(columns=["player_id", "match_id", "start_x", "start_y", "action_type", "minutes_played"])
        result = compute_sweeper_metrics(empty)
        assert len(result) == 0
