"""Tests for analytics.smoothing — Savitzky-Golay position smoother."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.smoothing import smooth_positions


def _make_noisy_trajectory(
    n_frames: int = 50,
    noise_std: float = 0.01,
    seed: int = 42,
) -> pd.DataFrame:
    """Create a straight-line trajectory with Gaussian noise."""
    rng = np.random.default_rng(seed)
    frames = np.arange(n_frames)
    x_true = np.linspace(0.0, 10.0, n_frames)
    y_true = np.linspace(0.0, 5.0, n_frames)
    return pd.DataFrame(
        {
            "player_id": "P1",
            "period": 1,
            "frame": frames,
            "x": x_true + rng.normal(0, noise_std, n_frames),
            "y": y_true + rng.normal(0, noise_std, n_frames),
        }
    )


class TestSmoothPositions:
    """Core smoothing behavior tests."""

    def test_reduces_noise(self) -> None:
        """Smoothed positions have lower frame-to-frame jitter than raw."""
        df = _make_noisy_trajectory(n_frames=50, noise_std=0.05)

        smoothed = smooth_positions(df)

        raw_jitter_x = df["x"].diff().std()
        smooth_jitter_x = smoothed["x"].diff().std()
        assert smooth_jitter_x < raw_jitter_x

        raw_jitter_y = df["y"].diff().std()
        smooth_jitter_y = smoothed["y"].diff().std()
        assert smooth_jitter_y < raw_jitter_y

    def test_preserves_trajectory(self) -> None:
        """Smoothed path stays close to original (no large drift)."""
        df = _make_noisy_trajectory(n_frames=50, noise_std=0.01)

        smoothed = smooth_positions(df)

        max_drift_x = (smoothed["x"] - df["x"]).abs().max()
        max_drift_y = (smoothed["y"] - df["y"]).abs().max()
        assert max_drift_x < 0.05, f"X drift too large: {max_drift_x}"
        assert max_drift_y < 0.05, f"Y drift too large: {max_drift_y}"

    def test_short_sequence_passthrough(self) -> None:
        """Sequences shorter than window_length are returned unmodified."""
        df = pd.DataFrame(
            {
                "player_id": ["P1"] * 5,
                "period": [1] * 5,
                "frame": list(range(5)),
                "x": [1.0, 2.0, 3.0, 4.0, 5.0],
                "y": [1.0, 1.0, 1.0, 1.0, 1.0],
            }
        )

        smoothed = smooth_positions(df, window_length=7)

        pd.testing.assert_series_equal(smoothed["x"], df["x"], check_names=False)
        pd.testing.assert_series_equal(smoothed["y"], df["y"], check_names=False)

    def test_single_frame(self) -> None:
        """Single-frame sequences are returned as-is."""
        df = pd.DataFrame(
            {
                "player_id": ["P1"],
                "period": [1],
                "frame": [0],
                "x": [5.0],
                "y": [3.0],
            }
        )

        smoothed = smooth_positions(df)

        assert smoothed["x"].iloc[0] == pytest.approx(5.0)
        assert smoothed["y"].iloc[0] == pytest.approx(3.0)

    def test_empty_dataframe(self) -> None:
        """Empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame({"player_id": [], "period": [], "frame": [], "x": [], "y": []})

        smoothed = smooth_positions(df)

        assert len(smoothed) == 0

    def test_per_player_per_period_independence(self) -> None:
        """Smoothing is applied independently per (player_id, period) group."""
        rng = np.random.default_rng(99)
        n = 20

        df1 = pd.DataFrame(
            {
                "player_id": "P1",
                "period": 1,
                "frame": range(n),
                "x": np.linspace(0, 10, n) + rng.normal(0, 0.05, n),
                "y": np.linspace(0, 5, n) + rng.normal(0, 0.05, n),
            }
        )
        df2 = pd.DataFrame(
            {
                "player_id": "P2",
                "period": 1,
                "frame": range(n),
                "x": np.linspace(50, 60, n) + rng.normal(0, 0.05, n),
                "y": np.linspace(30, 35, n) + rng.normal(0, 0.05, n),
            }
        )

        combined = pd.concat([df1, df2], ignore_index=True)
        smoothed = smooth_positions(combined, window_length=7)

        # P1 and P2 smoothed independently — P1 values should still be near 0-10
        p1_smoothed = smoothed[smoothed["player_id"] == "P1"]
        p2_smoothed = smoothed[smoothed["player_id"] == "P2"]

        assert p1_smoothed["x"].max() < 15, "P1 x contaminated by P2"
        assert p2_smoothed["x"].min() > 45, "P2 x contaminated by P1"

    def test_preserves_row_count(self) -> None:
        """Output has same number of rows as input."""
        df = _make_noisy_trajectory(n_frames=30)

        smoothed = smooth_positions(df)

        assert len(smoothed) == len(df)

    def test_does_not_mutate_input(self) -> None:
        """Input DataFrame is not modified in place."""
        df = _make_noisy_trajectory(n_frames=20)
        original_x = df["x"].copy()

        smooth_positions(df)

        pd.testing.assert_series_equal(df["x"], original_x)

    def test_custom_group_cols(self) -> None:
        """Smoothing respects custom group_cols (e.g., including match_id)."""
        rng = np.random.default_rng(7)
        n = 15

        df = pd.DataFrame(
            {
                "player_id": ["P1"] * n + ["P1"] * n,
                "period": [1] * n + [1] * n,
                "match_id": ["M1"] * n + ["M2"] * n,
                "frame": list(range(n)) * 2,
                "x": np.concatenate(
                    [
                        np.linspace(0, 10, n) + rng.normal(0, 0.05, n),
                        np.linspace(80, 90, n) + rng.normal(0, 0.05, n),
                    ]
                ),
                "y": np.zeros(2 * n),
            }
        )

        smoothed = smooth_positions(
            df,
            window_length=7,
            group_cols=("player_id", "period", "match_id"),
        )

        m1 = smoothed[smoothed["match_id"] == "M1"]
        m2 = smoothed[smoothed["match_id"] == "M2"]
        assert m1["x"].max() < 15, "M1 contaminated by M2"
        assert m2["x"].min() > 75, "M2 contaminated by M1"

    def test_acceleration_noise_reduction(self) -> None:
        """Smoothing reduces acceleration noise (the actual goal)."""
        df = _make_noisy_trajectory(n_frames=100, noise_std=0.03)

        def compute_accel_std(positions: pd.Series) -> float:  # type: ignore[type-arg]
            speed = positions.diff()
            accel = speed.diff()
            return float(accel.std())

        raw_x: pd.Series = df["x"]  # type: ignore[assignment]
        raw_accel_std = compute_accel_std(raw_x)
        smoothed = smooth_positions(df)
        smooth_x: pd.Series = smoothed["x"]  # type: ignore[assignment]
        smooth_accel_std = compute_accel_std(smooth_x)

        assert smooth_accel_std < raw_accel_std * 0.5, (
            f"Expected >50% acceleration noise reduction, got {raw_accel_std:.6f} -> {smooth_accel_std:.6f}"
        )
