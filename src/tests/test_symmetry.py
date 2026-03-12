"""Tests for analytics.symmetry — TacticAI symmetry augmentation."""

from __future__ import annotations

import pandas as pd
import pytest

from analytics.symmetry import (
    AugmentationConfig,
    augment_tracking_frame,
    flip_horizontal,
    flip_vertical,
    swap_teams,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG = AugmentationConfig()  # StatsBomb 120x80


def _make_frame() -> pd.DataFrame:
    """Minimal 4-player tracking frame with velocity and ball columns."""
    return pd.DataFrame(
        {
            "player_id": ["P1", "P2", "P3", "P4"],
            "team": ["home", "home", "away", "away"],
            "x": [30.0, 50.0, 70.0, 90.0],
            "y": [20.0, 40.0, 60.0, 10.0],
            "velocity_x": [1.0, -2.0, 3.0, -4.0],
            "velocity_y": [0.5, -1.5, 2.5, -3.5],
            "ball_x": [60.0, 60.0, 60.0, 60.0],
            "ball_y": [40.0, 40.0, 40.0, 40.0],
        }
    )


# ---------------------------------------------------------------------------
# TestFlipHorizontal
# ---------------------------------------------------------------------------


class TestFlipHorizontal:
    """Horizontal (left-right) pitch mirror."""

    def test_x_coordinates_mirrored(self) -> None:
        """x -> pitch_length - x for StatsBomb 120x80."""
        df = _make_frame()
        result = flip_horizontal(df)

        expected_x = [90.0, 70.0, 50.0, 30.0]
        assert result["x"].tolist() == pytest.approx(expected_x)

    def test_velocity_x_negated(self) -> None:
        """velocity_x -> -velocity_x."""
        df = _make_frame()
        result = flip_horizontal(df)

        expected_vx = [-1.0, 2.0, -3.0, 4.0]
        assert result["velocity_x"].tolist() == pytest.approx(expected_vx)

    def test_ball_x_mirrored(self) -> None:
        """ball_x -> pitch_length - ball_x."""
        df = _make_frame()
        result = flip_horizontal(df)

        assert result["ball_x"].tolist() == pytest.approx([60.0, 60.0, 60.0, 60.0])

    def test_y_unchanged(self) -> None:
        """y coordinates NOT affected by H-flip."""
        df = _make_frame()
        original_y = df["y"].tolist()

        result = flip_horizontal(df)

        assert result["y"].tolist() == pytest.approx(original_y)

    def test_does_not_mutate_input(self) -> None:
        """Input DataFrame is not modified."""
        df = _make_frame()
        original_x = df["x"].copy()

        flip_horizontal(df)

        pd.testing.assert_series_equal(df["x"], original_x)

    def test_missing_velocity_column(self) -> None:
        """Gracefully skips velocity_x when column is absent."""
        df = _make_frame().drop(columns=["velocity_x"])
        result = flip_horizontal(df)

        assert "velocity_x" not in result.columns
        assert result["x"].tolist() == pytest.approx([90.0, 70.0, 50.0, 30.0])


# ---------------------------------------------------------------------------
# TestFlipVertical
# ---------------------------------------------------------------------------


class TestFlipVertical:
    """Vertical (top-bottom) pitch mirror."""

    def test_y_coordinates_mirrored(self) -> None:
        """y -> pitch_width - y."""
        df = _make_frame()
        result = flip_vertical(df)

        expected_y = [60.0, 40.0, 20.0, 70.0]
        assert result["y"].tolist() == pytest.approx(expected_y)

    def test_velocity_y_negated(self) -> None:
        """velocity_y -> -velocity_y."""
        df = _make_frame()
        result = flip_vertical(df)

        expected_vy = [-0.5, 1.5, -2.5, 3.5]
        assert result["velocity_y"].tolist() == pytest.approx(expected_vy)

    def test_x_unchanged(self) -> None:
        """x coordinates NOT affected by V-flip."""
        df = _make_frame()
        original_x = df["x"].tolist()

        result = flip_vertical(df)

        assert result["x"].tolist() == pytest.approx(original_x)

    def test_ball_y_mirrored(self) -> None:
        """ball_y -> pitch_width - ball_y."""
        df = _make_frame()
        result = flip_vertical(df)

        assert result["ball_y"].tolist() == pytest.approx([40.0, 40.0, 40.0, 40.0])

    def test_does_not_mutate_input(self) -> None:
        """Input DataFrame is not modified."""
        df = _make_frame()
        original_y = df["y"].copy()

        flip_vertical(df)

        pd.testing.assert_series_equal(df["y"], original_y)

    def test_missing_velocity_column(self) -> None:
        """Gracefully skips velocity_y when column is absent."""
        df = _make_frame().drop(columns=["velocity_y"])
        result = flip_vertical(df)

        assert "velocity_y" not in result.columns
        assert result["y"].tolist() == pytest.approx([60.0, 40.0, 20.0, 70.0])


# ---------------------------------------------------------------------------
# TestSwapTeams
# ---------------------------------------------------------------------------


class TestSwapTeams:
    """Team label swapping."""

    def test_home_becomes_away(self) -> None:
        """team column: 'home' -> 'away', 'away' -> 'home'."""
        df = _make_frame()
        result = swap_teams(df)

        expected_teams = ["away", "away", "home", "home"]
        assert result["team"].tolist() == expected_teams

    def test_other_columns_unchanged(self) -> None:
        """Non-team columns are untouched."""
        df = _make_frame()
        result = swap_teams(df)

        pd.testing.assert_series_equal(result["x"], df["x"], check_names=False)
        pd.testing.assert_series_equal(result["y"], df["y"], check_names=False)
        pd.testing.assert_series_equal(result["velocity_x"], df["velocity_x"], check_names=False)
        pd.testing.assert_series_equal(result["velocity_y"], df["velocity_y"], check_names=False)
        pd.testing.assert_series_equal(result["player_id"], df["player_id"], check_names=False)

    def test_does_not_mutate_input(self) -> None:
        """Input DataFrame is not modified."""
        df = _make_frame()
        original_team = df["team"].copy()

        swap_teams(df)

        pd.testing.assert_series_equal(df["team"], original_team)


# ---------------------------------------------------------------------------
# TestAugmentTrackingFrame
# ---------------------------------------------------------------------------


class TestAugmentTrackingFrame:
    """Full augmentation pipeline (8 variants)."""

    def test_eight_variants_produced(self) -> None:
        """Original + 7 augmentations = 8 total."""
        df = _make_frame()
        variants = augment_tracking_frame(df)

        assert len(variants) == 8

    def test_seven_without_original(self) -> None:
        """include_original=False yields 7 variants."""
        df = _make_frame()
        variants = augment_tracking_frame(df, include_original=False)

        assert len(variants) == 7
        labels = [v["augmentation"].iloc[0] for v in variants]
        assert "original" not in labels

    def test_all_variants_same_shape(self) -> None:
        """All 8 have identical shape."""
        df = _make_frame()
        variants = augment_tracking_frame(df)

        # All variants should have one extra column ('augmentation')
        expected_cols = len(df.columns) + 1
        for variant in variants:
            assert variant.shape[0] == df.shape[0]
            assert len(variant.columns) == expected_cols

    def test_original_included_unchanged(self) -> None:
        """First variant matches original (except augmentation column)."""
        df = _make_frame()
        variants = augment_tracking_frame(df)

        original_variant = variants[0]
        assert original_variant["augmentation"].iloc[0] == "original"

        for col in df.columns:
            pd.testing.assert_series_equal(
                original_variant[col].reset_index(drop=True),
                df[col].reset_index(drop=True),
                check_names=False,
            )

    def test_round_trip_double_flip(self) -> None:
        """H-flip(H-flip(frame)) == original."""
        df = _make_frame()
        double_flipped = flip_horizontal(flip_horizontal(df))

        pd.testing.assert_frame_equal(double_flipped, df)

    def test_round_trip_double_v_flip(self) -> None:
        """V-flip(V-flip(frame)) == original."""
        df = _make_frame()
        double_flipped = flip_vertical(flip_vertical(df))

        pd.testing.assert_frame_equal(double_flipped, df)

    def test_round_trip_double_swap(self) -> None:
        """swap_teams(swap_teams(frame)) == original."""
        df = _make_frame()
        double_swapped = swap_teams(swap_teams(df))

        pd.testing.assert_frame_equal(double_swapped, df)

    def test_unique_augmentation_labels(self) -> None:
        """All 8 variants have distinct augmentation labels."""
        df = _make_frame()
        variants = augment_tracking_frame(df)

        labels = [v["augmentation"].iloc[0] for v in variants]
        assert len(set(labels)) == 8

    def test_custom_config(self) -> None:
        """Custom pitch dimensions are respected."""
        cfg = AugmentationConfig(pitch_length=105.0, pitch_width=68.0)
        df = pd.DataFrame({"x": [10.0], "y": [20.0]})

        h_result = flip_horizontal(df, config=cfg)
        v_result = flip_vertical(df, config=cfg)

        assert h_result["x"].iloc[0] == pytest.approx(95.0)
        assert v_result["y"].iloc[0] == pytest.approx(48.0)
