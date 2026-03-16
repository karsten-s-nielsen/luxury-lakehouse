"""Tests for analytics.augmentation — physics-based tracking augmentation."""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.augmentation import (
    PerturbationConfig,
    augment_full,
    perturb_positions,
)
from analytics.symmetry import AugmentationConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# StatsBomb 120x80 coordinate space
_SB_LENGTH = 120.0
_SB_WIDTH = 80.0

# Meter-space bounds (centered origin)
_HALF_LENGTH_M = 52.5
_HALF_WIDTH_M = 34.0


def _make_22_player_frame() -> pd.DataFrame:
    """Synthetic 22-player single-frame DataFrame in StatsBomb 120x80 coords."""
    rng = np.random.default_rng(42)
    n_home, n_away = 11, 11
    home = pd.DataFrame(
        {
            "player_id": [f"home_{i}" for i in range(n_home)],
            "team": "home",
            "x": rng.uniform(10, 110, n_home),
            "y": rng.uniform(5, 75, n_home),
            "velocity_x": rng.uniform(-3, 3, n_home),
            "velocity_y": rng.uniform(-3, 3, n_home),
            "ball_x": [60.0] * n_home,
            "ball_y": [40.0] * n_home,
        }
    )
    away = pd.DataFrame(
        {
            "player_id": [f"away_{i}" for i in range(n_away)],
            "team": "away",
            "x": rng.uniform(10, 110, n_away),
            "y": rng.uniform(5, 75, n_away),
            "velocity_x": rng.uniform(-3, 3, n_away),
            "velocity_y": rng.uniform(-3, 3, n_away),
            "ball_x": [60.0] * n_away,
            "ball_y": [40.0] * n_away,
        }
    )
    return pd.concat([home, away], ignore_index=True)


def _sb_to_centered_meters_x(x: float) -> float:
    """Convert a single SB x to centered meter-space."""
    return (x / _SB_LENGTH) * 105.0 - _HALF_LENGTH_M


def _sb_to_centered_meters_y(y: float) -> float:
    """Convert a single SB y to centered meter-space."""
    return (y / _SB_WIDTH) * 68.0 - _HALF_WIDTH_M


# ---------------------------------------------------------------------------
# TestPerturbPositions
# ---------------------------------------------------------------------------


class TestPerturbPositions:
    """Tests for perturb_positions — Gaussian position jitter with physics clamping."""

    def test_output_count(self) -> None:
        """10 perturbations -> 10 DataFrames."""
        df = _make_22_player_frame()
        config = PerturbationConfig(n_perturbations=10)
        rng = np.random.default_rng(123)

        results = perturb_positions(df, config, rng)

        assert len(results) == 10

    def test_positions_within_pitch_bounds(self) -> None:
        """All perturbed x, y within StatsBomb [0, 120] x [0, 80] bounds."""
        df = _make_22_player_frame()
        config = PerturbationConfig(n_perturbations=10)
        rng = np.random.default_rng(456)

        results = perturb_positions(df, config, rng)

        for result_df in results:
            assert (result_df["x"] >= 0.0).all(), "x below 0"
            assert (result_df["x"] <= _SB_LENGTH).all(), "x above 120"
            assert (result_df["y"] >= 0.0).all(), "y below 0"
            assert (result_df["y"] <= _SB_WIDTH).all(), "y above 80"

    def test_speed_within_physical_limit(self) -> None:
        """No player speed exceeds max_speed_ms after perturbation."""
        df = _make_22_player_frame()
        max_speed = 12.0
        config = PerturbationConfig(n_perturbations=10, max_speed_ms=max_speed)
        rng = np.random.default_rng(789)

        results = perturb_positions(df, config, rng)

        for result_df in results:
            vx = result_df["velocity_x"].to_numpy()
            vy = result_df["velocity_y"].to_numpy()
            # Velocities are in SB units/s — convert to m/s for comparison
            vx_ms = vx * (105.0 / _SB_LENGTH)
            vy_ms = vy * (68.0 / _SB_WIDTH)
            speed_ms = np.sqrt(vx_ms**2 + vy_ms**2)
            assert (speed_ms <= max_speed + 1e-9).all(), f"Speed exceeds limit: max={speed_ms.max():.4f} m/s"

    def test_reproducibility(self) -> None:
        """Same RNG seed -> identical output."""
        df = _make_22_player_frame()
        config = PerturbationConfig(n_perturbations=5)

        rng1 = np.random.default_rng(42)
        results1 = perturb_positions(df, config, rng1)

        rng2 = np.random.default_rng(42)
        results2 = perturb_positions(df, config, rng2)

        assert len(results1) == len(results2)
        for r1, r2 in zip(results1, results2, strict=True):
            pd.testing.assert_frame_equal(r1, r2)

    def test_augmentation_column_present(self) -> None:
        """Each DF has 'augmentation' and 'jitter_seed' columns."""
        df = _make_22_player_frame()
        config = PerturbationConfig(n_perturbations=3)
        rng = np.random.default_rng(99)

        results = perturb_positions(df, config, rng)

        for i, result_df in enumerate(results):
            assert "augmentation" in result_df.columns
            assert "jitter_seed" in result_df.columns
            # augmentation column should be "jitter_0", "jitter_1", etc.
            assert result_df["augmentation"].iloc[0] == f"jitter_{i}"

    def test_different_seeds_different_results(self) -> None:
        """Different seeds -> different positions."""
        df = _make_22_player_frame()
        config = PerturbationConfig(n_perturbations=1)

        rng1 = np.random.default_rng(100)
        results1 = perturb_positions(df, config, rng1)

        rng2 = np.random.default_rng(200)
        results2 = perturb_positions(df, config, rng2)

        # At least one position should differ
        assert not results1[0]["x"].equals(results2[0]["x"])

    def test_preserves_non_position_columns(self) -> None:
        """player_id, team, ball_x, ball_y are unchanged."""
        df = _make_22_player_frame()
        config = PerturbationConfig(n_perturbations=1)
        rng = np.random.default_rng(55)

        results = perturb_positions(df, config, rng)

        result_df = results[0]
        pd.testing.assert_series_equal(result_df["player_id"], df["player_id"], check_names=False)
        pd.testing.assert_series_equal(result_df["team"], df["team"], check_names=False)
        pd.testing.assert_series_equal(result_df["ball_x"], df["ball_x"], check_names=False)
        pd.testing.assert_series_equal(result_df["ball_y"], df["ball_y"], check_names=False)

    def test_does_not_mutate_input(self) -> None:
        """Input DataFrame is not modified."""
        df = _make_22_player_frame()
        original_x = df["x"].copy()
        original_y = df["y"].copy()
        config = PerturbationConfig(n_perturbations=3)
        rng = np.random.default_rng(77)

        perturb_positions(df, config, rng)

        pd.testing.assert_series_equal(df["x"], original_x)
        pd.testing.assert_series_equal(df["y"], original_y)

    def test_custom_perturbation_count(self) -> None:
        """Configurable n_perturbations."""
        df = _make_22_player_frame()
        for n in [1, 5, 20]:
            config = PerturbationConfig(n_perturbations=n)
            rng = np.random.default_rng(42)
            results = perturb_positions(df, config, rng)
            assert len(results) == n


# ---------------------------------------------------------------------------
# TestAugmentFull
# ---------------------------------------------------------------------------


class TestAugmentFull:
    """Tests for augment_full — symmetry x jitter composition."""

    def test_total_count(self) -> None:
        """8 symmetry x (1 + 10 jitter) = 88 DataFrames."""
        df = _make_22_player_frame()
        sym_config = AugmentationConfig()
        pert_config = PerturbationConfig(n_perturbations=10)
        rng = np.random.default_rng(42)

        results = augment_full(df, sym_config, pert_config, rng)

        assert len(results) == 88

    def test_coordinate_consistency(self) -> None:
        """Output in StatsBomb 120x80 (same as input)."""
        df = _make_22_player_frame()
        sym_config = AugmentationConfig()
        pert_config = PerturbationConfig(n_perturbations=3)
        rng = np.random.default_rng(42)

        results = augment_full(df, sym_config, pert_config, rng)

        for result_df in results:
            assert (result_df["x"] >= 0.0).all(), "x below 0"
            assert (result_df["x"] <= _SB_LENGTH).all(), "x above 120"
            assert (result_df["y"] >= 0.0).all(), "y below 0"
            assert (result_df["y"] <= _SB_WIDTH).all(), "y above 80"

    def test_original_variants_preserved(self) -> None:
        """8 un-jittered symmetry variants present."""
        df = _make_22_player_frame()
        sym_config = AugmentationConfig()
        pert_config = PerturbationConfig(n_perturbations=10)
        rng = np.random.default_rng(42)

        results = augment_full(df, sym_config, pert_config, rng)

        # The 8 symmetry originals should be at positions 0, 11, 22, 33, 44, 55, 66, 77
        # (each symmetry variant followed by 10 jittered copies)
        symmetry_labels = {
            "original",
            "h_flip",
            "v_flip",
            "team_swap",
            "h_flip+v_flip",
            "h_flip+team_swap",
            "v_flip+team_swap",
            "h_flip+v_flip+team_swap",
        }
        found_labels: set[str] = set()
        for i in range(8):
            idx = i * 11  # 1 original + 10 jittered per symmetry variant
            label = str(results[idx]["augmentation"].iloc[0])
            found_labels.add(label)

        assert found_labels == symmetry_labels

    def test_jitter_variants_labeled(self) -> None:
        """Jittered variants have combined augmentation labels."""
        df = _make_22_player_frame()
        sym_config = AugmentationConfig()
        pert_config = PerturbationConfig(n_perturbations=2)
        rng = np.random.default_rng(42)

        results = augment_full(df, sym_config, pert_config, rng)

        # 8 * (1 + 2) = 24 total
        assert len(results) == 24

        # First symmetry variant: original, then jitter_0, jitter_1
        assert results[0]["augmentation"].iloc[0] == "original"
        assert results[1]["augmentation"].iloc[0] == "original+jitter_0"
        assert results[2]["augmentation"].iloc[0] == "original+jitter_1"

    def test_speed_within_physical_limit(self) -> None:
        """No player speed exceeds max_speed_ms across all augmented variants."""
        df = _make_22_player_frame()
        sym_config = AugmentationConfig()
        max_speed = 12.0
        pert_config = PerturbationConfig(n_perturbations=5, max_speed_ms=max_speed)
        rng = np.random.default_rng(42)

        results = augment_full(df, sym_config, pert_config, rng)

        for result_df in results:
            vx = result_df["velocity_x"].to_numpy()
            vy = result_df["velocity_y"].to_numpy()
            vx_ms = vx * (105.0 / _SB_LENGTH)
            vy_ms = vy * (68.0 / _SB_WIDTH)
            speed_ms = np.sqrt(vx_ms**2 + vy_ms**2)
            assert (speed_ms <= max_speed + 1e-9).all()

    def test_smaller_perturbation_count(self) -> None:
        """Works with non-default perturbation count: 8 * (1 + 5) = 48."""
        df = _make_22_player_frame()
        sym_config = AugmentationConfig()
        pert_config = PerturbationConfig(n_perturbations=5)
        rng = np.random.default_rng(42)

        results = augment_full(df, sym_config, pert_config, rng)

        assert len(results) == 48
