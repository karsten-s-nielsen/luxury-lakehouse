"""Tests for PAUSA (Passing Ability Under Spatiotemporal Awareness) scoring.

TDD tests for the PAUSA temporal judgment, spatial selection, and composite
scoring logic. These functions are pure arithmetic — no Spark dependency.

Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying
Optimal Pass Timing Beyond Speed." MIT Sloan 2026.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.pausa import compute_pausa_scores


class TestPausaScoring:
    """Verify PAUSA scoring arithmetic and edge cases."""

    def test_temporal_judgment_perfect(self) -> None:
        """actual == peak -> temporal judgment = 1.0."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.5],
                "peak_obso": [0.5],
                "optimal_obso": [0.8],
            }
        )
        result = compute_pausa_scores(df)
        assert result["temporal_judgment"].iloc[0] == pytest.approx(1.0)

    def test_spatial_selection_perfect(self) -> None:
        """actual == optimal -> spatial selection = 1.0."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.8],
                "peak_obso": [1.0],
                "optimal_obso": [0.8],
            }
        )
        result = compute_pausa_scores(df)
        assert result["spatial_selection"].iloc[0] == pytest.approx(1.0)

    def test_pausa_composite(self) -> None:
        """pausa = temporal x spatial."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.4],
                "peak_obso": [0.8],
                "optimal_obso": [0.5],
            }
        )
        result = compute_pausa_scores(df)
        temporal = 0.4 / 0.8  # 0.5
        spatial = 0.4 / 0.5  # 0.8
        expected_pausa = temporal * spatial  # 0.4
        assert result["temporal_judgment"].iloc[0] == pytest.approx(temporal)
        assert result["spatial_selection"].iloc[0] == pytest.approx(spatial)
        assert result["pausa_score"].iloc[0] == pytest.approx(expected_pausa)

    def test_values_bounded_zero_one(self) -> None:
        """All PAUSA components are in [0, 1]."""
        rng = np.random.default_rng(42)
        n = 100
        # actual <= peak and actual <= optimal by construction
        peak = rng.uniform(0.1, 1.0, n)
        optimal = rng.uniform(0.1, 1.0, n)
        actual = rng.uniform(0.0, 1.0, n)
        # Clamp actual to be <= min(peak, optimal) so ratios stay in [0, 1]
        actual = np.minimum(actual, np.minimum(peak, optimal))

        df = pd.DataFrame(
            {
                "pass_id": [f"p{i}" for i in range(n)],
                "actual_obso": actual,
                "peak_obso": peak,
                "optimal_obso": optimal,
            }
        )
        result = compute_pausa_scores(df)

        assert (result["temporal_judgment"] >= 0.0).all()
        assert (result["temporal_judgment"] <= 1.0).all()
        assert (result["spatial_selection"] >= 0.0).all()
        assert (result["spatial_selection"] <= 1.0).all()
        assert (result["pausa_score"] >= 0.0).all()
        assert (result["pausa_score"] <= 1.0).all()

    def test_zero_peak_obso_handled(self) -> None:
        """peak=0 -> temporal=0 (no division by zero)."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.3],
                "peak_obso": [0.0],
                "optimal_obso": [0.5],
            }
        )
        result = compute_pausa_scores(df)
        assert result["temporal_judgment"].iloc[0] == pytest.approx(0.0)
        assert np.isfinite(result["pausa_score"].iloc[0])

    def test_zero_optimal_obso_handled(self) -> None:
        """optimal=0 -> spatial=0 (no division by zero)."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.3],
                "peak_obso": [0.5],
                "optimal_obso": [0.0],
            }
        )
        result = compute_pausa_scores(df)
        assert result["spatial_selection"].iloc[0] == pytest.approx(0.0)
        assert np.isfinite(result["pausa_score"].iloc[0])

    def test_both_denominators_zero(self) -> None:
        """peak=0 and optimal=0 -> temporal=0, spatial=0, pausa=0."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.0],
                "peak_obso": [0.0],
                "optimal_obso": [0.0],
            }
        )
        result = compute_pausa_scores(df)
        assert result["temporal_judgment"].iloc[0] == pytest.approx(0.0)
        assert result["spatial_selection"].iloc[0] == pytest.approx(0.0)
        assert result["pausa_score"].iloc[0] == pytest.approx(0.0)

    def test_clamped_above_one(self) -> None:
        """If actual > peak (numerical noise), temporal is clamped to 1.0."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1"],
                "actual_obso": [0.51],
                "peak_obso": [0.50],
                "optimal_obso": [0.60],
            }
        )
        result = compute_pausa_scores(df)
        assert result["temporal_judgment"].iloc[0] <= 1.0
        assert result["spatial_selection"].iloc[0] <= 1.0
        assert result["pausa_score"].iloc[0] <= 1.0

    def test_empty_dataframe(self) -> None:
        """Empty input returns empty output with correct columns."""
        df = pd.DataFrame(columns=pd.Index(["pass_id", "actual_obso", "peak_obso", "optimal_obso"]))
        result = compute_pausa_scores(df)
        assert len(result) == 0
        assert "temporal_judgment" in result.columns
        assert "spatial_selection" in result.columns
        assert "pausa_score" in result.columns

    def test_multiple_passes(self) -> None:
        """Vectorized computation works for multiple passes."""
        df = pd.DataFrame(
            {
                "pass_id": ["p1", "p2", "p3"],
                "actual_obso": [0.2, 0.5, 0.0],
                "peak_obso": [0.4, 0.5, 0.3],
                "optimal_obso": [0.5, 0.5, 0.4],
            }
        )
        result = compute_pausa_scores(df)
        assert len(result) == 3
        # p1: temporal=0.5, spatial=0.4, pausa=0.2
        assert result["pausa_score"].iloc[0] == pytest.approx(0.2)
        # p2: temporal=1.0, spatial=1.0, pausa=1.0
        assert result["pausa_score"].iloc[1] == pytest.approx(1.0)
        # p3: temporal=0.0, spatial=0.0, pausa=0.0
        assert result["pausa_score"].iloc[2] == pytest.approx(0.0)
