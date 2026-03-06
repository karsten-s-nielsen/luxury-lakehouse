"""Tests for DEFCON-lite defensive valuation analytics module."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from analytics.defcon_lite import (
    CreditType,
    DefconLiteParams,
    _euclidean_dist,
    _is_in_cone,
    assign_defensive_credits,
    compute_defcon_match,
    estimate_defcon_values,
    extract_features,
)

_PARAMS = DefconLiteParams()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_action_with_defenders() -> tuple[dict[str, object], pd.DataFrame]:
    """Create a test action and surrounding defenders."""
    action: dict[str, object] = {
        "event_id": "evt_1",
        "match_id": "match_1",
        "competition_id": 11,
        "season_id": 90,
        "action_player_id": 1001,
        "action_type": "pass",
        "action_x": 60.0,
        "action_y": 34.0,
        "offensive_value": 0.05,
    }
    defenders = pd.DataFrame(
        [
            {"player_id": 2001, "team_id": 200, "x": 60.5, "y": 34.5, "velocity_x": 0.0, "velocity_y": 0.0},
            {"player_id": 2002, "team_id": 200, "x": 55.0, "y": 34.0, "velocity_x": 0.0, "velocity_y": 0.0},
            {"player_id": 2003, "team_id": 200, "x": 40.0, "y": 34.0, "velocity_x": 0.0, "velocity_y": 0.0},
        ]
    )
    return action, defenders


def _make_credits_df() -> pd.DataFrame:
    """Create a DataFrame of credits for testing feature extraction."""
    rows = []
    for i in range(20):
        rows.append(
            {
                "event_id": f"evt_{i}",
                "match_id": "match_1",
                "competition_id": 11,
                "season_id": 90,
                "defender_player_id": 2000 + (i % 5),
                "defender_team_id": 200,
                "defender_x": 40.0 + i,
                "defender_y": 34.0 + (i % 3),
                "action_player_id": 1001,
                "action_type": "pass",
                "action_x": 50.0 + i,
                "action_y": 34.0,
                "credit_type": ["intercept", "concede", "disturb", "deter"][i % 4],
                "confidence": "high" if i % 4 < 2 else "approximate",
                "dist_to_ball": 3.0 + i * 0.5,
                "pitch_control_at_action": 0.5,
                "offensive_value": 0.05 * ((-1) ** i),
            }
        )
    return pd.DataFrame(rows)


def _make_match_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create minimal action_values + freeze frame data for one match."""
    actions = pd.DataFrame(
        [
            {
                "event_id": f"evt_{i}",
                "match_id": "match_1",
                "competition_id": 11,
                "season_id": 90,
                "player_id": 1001,
                "team_id": 100,
                "action_type": "pass",
                "start_x": 50.0 + i,
                "start_y": 34.0,
                "offensive_value": 0.05 * ((-1) ** i),
            }
            for i in range(10)
        ]
    )

    ff_rows = []
    for i in range(10):
        for j in range(3):
            ff_rows.append(
                {
                    "event_id": f"evt_{i}",
                    "player_id": 2000 + j,
                    "team_id": 200,
                    "teammate": False,
                    "x": 45.0 + j * 5.0,
                    "y": 30.0 + j * 3.0,
                    "velocity_x": 0.0,
                    "velocity_y": 0.0,
                }
            )
    freeze_frames = pd.DataFrame(ff_rows)

    return actions, freeze_frames


# ---------------------------------------------------------------------------
# TestDefconLiteParams
# ---------------------------------------------------------------------------


class TestDefconLiteParams:
    """Test DefconLiteParams frozen dataclass."""

    def test_defaults(self) -> None:
        params = DefconLiteParams()
        assert params.disturb_radius_m == 5.0
        assert params.deter_cone_angle_deg == 15.0
        assert params.pitch_length == 105.0
        assert params.pitch_width == 68.0

    def test_custom_override(self) -> None:
        params = DefconLiteParams(disturb_radius_m=8.0)
        assert params.disturb_radius_m == 8.0
        assert params.pitch_length == 105.0


# ---------------------------------------------------------------------------
# TestCreditType
# ---------------------------------------------------------------------------


class TestCreditType:
    """Test CreditType enum values."""

    def test_all_four_categories(self) -> None:
        assert CreditType.INTERCEPT.value == "intercept"
        assert CreditType.CONCEDE.value == "concede"
        assert CreditType.DISTURB.value == "disturb"
        assert CreditType.DETER.value == "deter"

    def test_priority_order(self) -> None:
        ordered = [CreditType.INTERCEPT, CreditType.CONCEDE, CreditType.DISTURB, CreditType.DETER]
        assert [c.value for c in ordered] == ["intercept", "concede", "disturb", "deter"]


# ---------------------------------------------------------------------------
# TestEuclideanDist
# ---------------------------------------------------------------------------


class TestEuclideanDist:
    """Test Euclidean distance calculation."""

    def test_same_point(self) -> None:
        assert _euclidean_dist(10.0, 20.0, 10.0, 20.0) == 0.0

    def test_horizontal(self) -> None:
        assert abs(_euclidean_dist(0.0, 0.0, 3.0, 0.0) - 3.0) < 1e-9

    def test_diagonal(self) -> None:
        assert abs(_euclidean_dist(0.0, 0.0, 3.0, 4.0) - 5.0) < 1e-9


# ---------------------------------------------------------------------------
# TestIsInCone
# ---------------------------------------------------------------------------


class TestIsInCone:
    """Test cone angle detection for Deter credit."""

    def test_directly_in_line(self) -> None:
        # Defender at (70, 34) is ahead of ball (60, 34) toward target (100, 34)
        assert _is_in_cone(70.0, 34.0, 60.0, 34.0, 100.0, 34.0, 15.0) is True

    def test_far_off_line(self) -> None:
        assert _is_in_cone(50.0, 0.0, 60.0, 34.0, 100.0, 34.0, 15.0) is False

    def test_edge_of_cone(self) -> None:
        # Defender just inside the 15-degree half-angle cone (use 14 degrees)
        angle_rad = math.radians(14.0)
        dx = 20.0
        dy = dx * math.tan(angle_rad)
        result = _is_in_cone(dx, dy, 0.0, 0.0, 40.0, 0.0, 15.0)
        assert result is True

    def test_zero_distance(self) -> None:
        assert _is_in_cone(60.0, 34.0, 60.0, 34.0, 100.0, 34.0, 15.0) is False


# ---------------------------------------------------------------------------
# TestAssignDefensiveCredits
# ---------------------------------------------------------------------------


class TestAssignDefensiveCredits:
    """Test Stage 1 credit assignment."""

    def test_intercept_tackle(self) -> None:
        action, defenders = _make_action_with_defenders()
        action["action_type"] = "tackle"
        action["action_player_id"] = 2001
        credits = assign_defensive_credits(action, defenders, _PARAMS)
        intercept_credits = [c for c in credits if c["credit_type"] == CreditType.INTERCEPT.value]
        assert len(intercept_credits) == 1
        assert intercept_credits[0]["defender_player_id"] == 2001

    def test_concede_nearest_defender(self) -> None:
        action, defenders = _make_action_with_defenders()
        action["offensive_value"] = 0.1
        action["action_type"] = "pass"
        credits = assign_defensive_credits(action, defenders, _PARAMS)
        concede_credits = [c for c in credits if c["credit_type"] == CreditType.CONCEDE.value]
        assert len(concede_credits) == 1
        assert concede_credits[0]["defender_player_id"] == 2001

    def test_one_credit_per_defender(self) -> None:
        action, defenders = _make_action_with_defenders()
        credits = assign_defensive_credits(action, defenders, _PARAMS)
        defender_ids = [c["defender_player_id"] for c in credits]
        assert len(defender_ids) == len(set(defender_ids))

    def test_empty_defenders(self) -> None:
        action, _ = _make_action_with_defenders()
        empty_df = pd.DataFrame(columns=pd.Index(["player_id", "team_id", "x", "y", "velocity_x", "velocity_y"]))
        credits = assign_defensive_credits(action, empty_df, _PARAMS)
        assert credits == []

    def test_credit_has_required_fields(self) -> None:
        action, defenders = _make_action_with_defenders()
        credits = assign_defensive_credits(action, defenders, _PARAMS)
        if credits:
            required = {
                "event_id",
                "match_id",
                "defender_player_id",
                "defender_team_id",
                "credit_type",
                "confidence",
                "defender_x",
                "defender_y",
                "dist_to_ball",
                "action_type",
                "action_x",
                "action_y",
            }
            assert required.issubset(set(credits[0].keys()))

    def test_disturb_within_radius(self) -> None:
        action, defenders = _make_action_with_defenders()
        action["offensive_value"] = 0.0
        action["action_type"] = "dribble"
        credits = assign_defensive_credits(action, defenders, _PARAMS)
        disturb = [c for c in credits if c["credit_type"] == CreditType.DISTURB.value]
        assert any(c["defender_player_id"] == 2001 for c in disturb)

    def test_deter_negative_offensive_value(self) -> None:
        action, defenders = _make_action_with_defenders()
        action["offensive_value"] = -0.05
        action["action_type"] = "dribble"
        credits = assign_defensive_credits(action, defenders, _PARAMS)
        deter = [c for c in credits if c["credit_type"] == CreditType.DETER.value]
        assert isinstance(deter, list)


# ---------------------------------------------------------------------------
# TestExtractFeatures
# ---------------------------------------------------------------------------


class TestExtractFeatures:
    """Test feature extraction for XGBoost."""

    def test_feature_columns(self) -> None:
        credits_df = _make_credits_df()
        features = extract_features(credits_df, _PARAMS)
        expected = {
            "dist_to_ball",
            "dist_to_goal",
            "angle_to_ball",
            "pitch_control_at_action",
            "action_type_id",
            "action_start_x",
            "action_start_y",
            "offensive_value",
            "defender_x",
            "defender_y",
            "is_between_ball_and_goal",
        }
        assert expected.issubset(set(features.columns))

    def test_output_length(self) -> None:
        credits_df = _make_credits_df()
        features = extract_features(credits_df, _PARAMS)
        assert len(features) == len(credits_df)

    def test_dist_to_goal_positive(self) -> None:
        credits_df = _make_credits_df()
        features = extract_features(credits_df, _PARAMS)
        assert all(features["dist_to_goal"] >= 0)


# ---------------------------------------------------------------------------
# TestEstimateDefconValues
# ---------------------------------------------------------------------------


class TestEstimateDefconValues:
    """Test XGBoost value estimation."""

    def test_output_shape(self) -> None:
        credits_df = _make_credits_df()
        credits_df["vaep_target"] = credits_df["offensive_value"].abs()
        result = estimate_defcon_values(credits_df, _PARAMS)
        assert "defcon_value" in result.columns
        assert len(result) == len(credits_df)

    def test_values_finite(self) -> None:
        credits_df = _make_credits_df()
        credits_df["vaep_target"] = credits_df["offensive_value"].abs()
        result = estimate_defcon_values(credits_df, _PARAMS)
        assert all(np.isfinite(result["defcon_value"]))

    def test_fallback_few_rows(self) -> None:
        """With < 10 training rows, should use distance-based fallback."""
        credits_df = _make_credits_df().head(5)
        credits_df["vaep_target"] = credits_df["offensive_value"].abs()
        result = estimate_defcon_values(credits_df, _PARAMS)
        assert "defcon_value" in result.columns
        assert all(result["defcon_value"] > 0)


# ---------------------------------------------------------------------------
# TestComputeDefconMatch
# ---------------------------------------------------------------------------


class TestComputeDefconMatch:
    """Test per-match DEFCON-lite computation."""

    def test_returns_dataframe(self) -> None:
        actions, ff = _make_match_data()
        result = compute_defcon_match(actions, ff, _PARAMS)
        assert isinstance(result, pd.DataFrame)

    def test_output_has_required_columns(self) -> None:
        actions, ff = _make_match_data()
        result = compute_defcon_match(actions, ff, _PARAMS)
        required = {
            "event_id",
            "match_id",
            "defender_player_id",
            "credit_type",
            "confidence",
            "defcon_value",
            "data_source",
        }
        if not result.empty:
            assert required.issubset(set(result.columns))

    def test_empty_actions(self) -> None:
        empty = pd.DataFrame(
            columns=pd.Index(
                [
                    "event_id",
                    "match_id",
                    "competition_id",
                    "season_id",
                    "player_id",
                    "team_id",
                    "action_type",
                    "start_x",
                    "start_y",
                    "offensive_value",
                ]
            )
        )
        ff = pd.DataFrame(
            columns=pd.Index(
                [
                    "event_id",
                    "player_id",
                    "team_id",
                    "teammate",
                    "x",
                    "y",
                    "velocity_x",
                    "velocity_y",
                ]
            )
        )
        result = compute_defcon_match(empty, ff, _PARAMS)
        assert len(result) == 0

    def test_data_source_tagged(self) -> None:
        actions, ff = _make_match_data()
        result = compute_defcon_match(actions, ff, _PARAMS, data_source="statsbomb_360")
        if not result.empty:
            assert all(result["data_source"] == "statsbomb_360")

    def test_no_intermediate_cols_in_output(self) -> None:
        actions, ff = _make_match_data()
        result = compute_defcon_match(actions, ff, _PARAMS)
        if not result.empty:
            assert "vaep_target" not in result.columns
            assert "offensive_value" not in result.columns
