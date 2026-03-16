"""Tests for analytics.model_validation — model drift detection and validation."""

from __future__ import annotations

import numpy as np

from analytics.model_validation import (
    ValidationResult,
    check_field_sum_constraint,
    check_ks_test,
    check_physical_bounds,
    compute_cusum,
    compute_psi,
    compute_wasserstein_drift,
)

# ---------------------------------------------------------------------------
# TestComputePSI
# ---------------------------------------------------------------------------


class TestComputePSI:
    """Tests for Population Stability Index computation."""

    def test_identical_distributions_zero(self) -> None:
        """PSI(A, A) = 0 (or near-zero with epsilon)."""
        rng = np.random.default_rng(42)
        data = rng.normal(0.1, 0.05, 1000)
        psi = compute_psi(data, data, n_bins=10)
        assert psi < 0.001, f"PSI of identical distributions should be ~0, got {psi}"

    def test_shifted_distribution_positive(self) -> None:
        """A known shift produces PSI > 0."""
        rng = np.random.default_rng(42)
        reference = rng.normal(0.1, 0.05, 1000)
        current = rng.normal(0.15, 0.05, 1000)
        psi = compute_psi(reference, current, n_bins=10)
        assert psi > 0.0, f"Shifted distribution should have PSI > 0, got {psi}"

    def test_psi_non_negative(self) -> None:
        """PSI is always >= 0 regardless of inputs."""
        rng = np.random.default_rng(99)
        for _ in range(10):
            ref = rng.normal(0, 1, 500)
            cur = rng.normal(rng.uniform(-2, 2), rng.uniform(0.5, 2), 500)
            psi = compute_psi(ref, cur, n_bins=10)
            assert psi >= 0.0, f"PSI must be non-negative, got {psi}"

    def test_significant_shift(self) -> None:
        """A large distributional shift produces PSI > 0.2."""
        rng = np.random.default_rng(42)
        reference = rng.normal(0.1, 0.05, 1000)
        current = rng.normal(0.5, 0.05, 1000)
        psi = compute_psi(reference, current, n_bins=10)
        assert psi > 0.2, f"Large shift should produce PSI > 0.2, got {psi}"

    def test_small_sample_does_not_crash(self) -> None:
        """PSI handles very small samples without error."""
        ref = np.array([0.1, 0.2, 0.3])
        cur = np.array([0.1, 0.2, 0.35])
        psi = compute_psi(ref, cur, n_bins=3)
        assert psi >= 0.0

    def test_default_n_bins(self) -> None:
        """Default n_bins=10 is used when not specified."""
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, 500)
        psi = compute_psi(data, data)
        assert psi < 0.01


# ---------------------------------------------------------------------------
# TestComputeWassersteinDrift
# ---------------------------------------------------------------------------


class TestComputeWassersteinDrift:
    """Tests for Wasserstein distance drift detection."""

    def test_identical_zero(self) -> None:
        """Wasserstein distance of identical distributions is 0."""
        data = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
        dist = compute_wasserstein_drift(data, data)
        assert dist == 0.0, f"Identical distributions should have distance 0, got {dist}"

    def test_shifted_positive(self) -> None:
        """Shifted distribution produces positive Wasserstein distance."""
        reference = np.array([0.0, 0.1, 0.2, 0.3, 0.4])
        current = np.array([0.5, 0.6, 0.7, 0.8, 0.9])
        dist = compute_wasserstein_drift(reference, current)
        assert dist > 0.0, f"Shifted distribution should have distance > 0, got {dist}"

    def test_known_shift_magnitude(self) -> None:
        """Wasserstein distance for uniform shift equals the shift amount."""
        reference = np.array([0.0, 1.0, 2.0, 3.0, 4.0])
        current = reference + 2.0
        dist = compute_wasserstein_drift(reference, current)
        np.testing.assert_allclose(dist, 2.0, atol=1e-10)

    def test_non_negative(self) -> None:
        """Wasserstein distance is always non-negative."""
        rng = np.random.default_rng(42)
        for _ in range(10):
            ref = rng.normal(0, 1, 100)
            cur = rng.normal(0, 1, 100)
            dist = compute_wasserstein_drift(ref, cur)
            assert dist >= 0.0


# ---------------------------------------------------------------------------
# TestComputeCUSUM
# ---------------------------------------------------------------------------


class TestComputeCUSUM:
    """Tests for CUSUM control chart."""

    def test_no_drift_below_threshold(self) -> None:
        """Stable process stays below 3*sigma (ok status)."""
        rng = np.random.default_rng(42)
        target_mean = 0.18
        sigma = 0.05
        # Generate values close to target mean
        values = rng.normal(target_mean, sigma * 0.3, 50)
        max_cusum, status = compute_cusum(values, target_mean, sigma)
        assert status == "ok", f"Stable process should be ok, got {status} (cusum={max_cusum})"

    def test_sustained_shift_triggers(self) -> None:
        """A sustained mean shift of 2*sigma triggers warn or alert."""
        target_mean = 0.18
        sigma = 0.05
        # 30 observations all shifted by 2*sigma above the mean
        shift = 2.0 * sigma
        values = np.full(30, target_mean + shift)
        _max_cusum, status = compute_cusum(values, target_mean, sigma)
        # 30 * 0.1 = 3.0, which is >= 3*sigma=0.15 → at least warn
        assert status in {"warn", "alert"}, f"Sustained shift should trigger, got {status}"

    def test_single_outlier_recovers(self) -> None:
        """A single outlier followed by on-target values stays ok."""
        target_mean = 0.18
        sigma = 0.05
        # One large outlier then 20 values at the target mean
        values = np.concatenate([[target_mean + 3.0 * sigma], np.full(20, target_mean)])
        max_cusum, status = compute_cusum(values, target_mean, sigma)
        # Single spike of 3*sigma = 0.15, which equals the 3*sigma threshold exactly.
        # The CUSUM accumulates but the subsequent at-target values don't push it higher.
        # With target_mean observations the CUSUM resets to 0 over time.
        # max_cusum = 0.15 = 3*sigma → threshold is "warn" at 3*sigma
        assert status in {"ok", "warn"}, f"Single outlier should recover, got {status} (cusum={max_cusum})"

    def test_large_sustained_shift_alert(self) -> None:
        """A large sustained shift triggers alert status."""
        target_mean = 0.18
        sigma = 0.05
        # 50 observations shifted by 5*sigma — massive drift
        values = np.full(50, target_mean + 5.0 * sigma)
        _max_cusum, status = compute_cusum(values, target_mean, sigma)
        assert status == "alert", f"Large sustained shift should be alert, got {status}"

    def test_cusum_non_negative(self) -> None:
        """CUSUM max value is always non-negative."""
        rng = np.random.default_rng(42)
        values = rng.normal(0.18, 0.05, 100)
        max_cusum, _ = compute_cusum(values, 0.18, 0.05)
        assert max_cusum >= 0.0

    def test_empty_values(self) -> None:
        """Empty input returns (0.0, 'ok')."""
        max_cusum, status = compute_cusum(np.array([]), 0.18, 0.05)
        assert max_cusum == 0.0
        assert status == "ok"

    def test_sigma_must_be_positive(self) -> None:
        """Sigma <= 0 raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="sigma must be positive"):
            compute_cusum(np.array([1.0, 2.0]), 1.5, 0.0)


# ---------------------------------------------------------------------------
# TestCheckKSTest
# ---------------------------------------------------------------------------


class TestCheckKSTest:
    """Tests for Kolmogorov-Smirnov two-sample test."""

    def test_identical_distributions_ok(self) -> None:
        """Identical samples produce high p-value and ok status."""
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, 500)
        stat, p_value, status = check_ks_test(data, data)
        assert stat == 0.0
        assert p_value == 1.0
        assert status == "ok"

    def test_different_distributions_alert(self) -> None:
        """Very different distributions produce alert."""
        rng = np.random.default_rng(42)
        ref = rng.normal(0, 1, 500)
        cur = rng.normal(5, 1, 500)
        _stat, p_value, status = check_ks_test(ref, cur)
        assert p_value < 0.05
        assert status == "alert"

    def test_returns_three_values(self) -> None:
        """check_ks_test returns (statistic, p_value, status) tuple."""
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, 100)
        result = check_ks_test(data, data)
        assert len(result) == 3
        stat, p_value, status = result
        assert isinstance(stat, float)
        assert isinstance(p_value, float)
        assert isinstance(status, str)
        assert status in {"ok", "warn", "alert"}

    def test_custom_alpha(self) -> None:
        """Custom alpha level is respected."""
        rng = np.random.default_rng(42)
        ref = rng.normal(0, 1, 100)
        cur = rng.normal(0.3, 1, 100)
        # With very lenient alpha, should be ok
        _, _, status = check_ks_test(ref, cur, alpha=0.001)
        # p-value from mild shift is usually > 0.001
        assert status in {"ok", "warn"}


# ---------------------------------------------------------------------------
# TestCheckPhysicalBounds
# ---------------------------------------------------------------------------


class TestCheckPhysicalBounds:
    """Tests for physical bounds checking."""

    def test_within_bounds_ok(self) -> None:
        """All values in range produce ok status."""
        values = np.array([1.0, 5.0, 10.0, 14.9])
        result = check_physical_bounds(values, 0.0, 15.0, "physical_stats", "max_speed_ms")
        assert result.status == "ok"
        assert result.model_name == "physical_stats"
        assert result.metric_name == "max_speed_ms"

    def test_exceeds_upper_alert(self) -> None:
        """Value above upper bound triggers alert."""
        values = np.array([1.0, 5.0, 16.0])
        result = check_physical_bounds(values, 0.0, 15.0, "physical_stats", "max_speed_ms")
        assert result.status == "alert"

    def test_below_lower_alert(self) -> None:
        """Value below lower bound triggers alert."""
        values = np.array([-1.0, 5.0, 10.0])
        result = check_physical_bounds(values, 0.0, 15.0, "physical_stats", "max_speed_ms")
        assert result.status == "alert"

    def test_empty_values_ok(self) -> None:
        """Empty input produces ok status."""
        values = np.array([])
        result = check_physical_bounds(values, 0.0, 15.0, "physical_stats", "max_speed_ms")
        assert result.status == "ok"

    def test_boundary_values_ok(self) -> None:
        """Values exactly at bounds are ok (inclusive)."""
        values = np.array([0.0, 15.0])
        result = check_physical_bounds(values, 0.0, 15.0, "physical_stats", "max_speed_ms")
        assert result.status == "ok"

    def test_result_contains_max_value(self) -> None:
        """ValidationResult.value contains the maximum observed value."""
        values = np.array([3.0, 7.0, 12.0])
        result = check_physical_bounds(values, 0.0, 15.0, "physical_stats", "max_speed_ms")
        assert result.value == 12.0


# ---------------------------------------------------------------------------
# TestCheckFieldSumConstraint
# ---------------------------------------------------------------------------


class TestCheckFieldSumConstraint:
    """Tests for pitch control field sum constraint."""

    def test_valid_grid_ok(self) -> None:
        """Grid with mean ~1.0 produces ok status."""
        grid = np.ones((68, 104))
        result = check_field_sum_constraint(grid, tolerance=0.05)
        assert result.status == "ok"
        np.testing.assert_allclose(result.value, 1.0)

    def test_invalid_grid_alert(self) -> None:
        """Grid with mean far from 1.0 produces alert status."""
        grid = np.full((68, 104), 1.3)
        result = check_field_sum_constraint(grid, tolerance=0.05)
        assert result.status == "alert"
        np.testing.assert_allclose(result.value, 1.3)

    def test_zero_grid_alert(self) -> None:
        """All-zero grid triggers alert (deviation = 1.0)."""
        grid = np.zeros((68, 104))
        result = check_field_sum_constraint(grid, tolerance=0.05)
        assert result.status == "alert"

    def test_within_tolerance_ok(self) -> None:
        """Grid mean slightly off 1.0 but within tolerance is ok."""
        grid = np.full((68, 104), 1.03)
        result = check_field_sum_constraint(grid, tolerance=0.05)
        assert result.status == "ok"

    def test_result_model_name(self) -> None:
        """Result always has model_name='pitch_control'."""
        grid = np.ones((10, 10))
        result = check_field_sum_constraint(grid)
        assert result.model_name == "pitch_control"
        assert result.metric_name == "field_sum"

    def test_reference_value_is_one(self) -> None:
        """Reference value for field sum is always 1.0."""
        grid = np.ones((10, 10))
        result = check_field_sum_constraint(grid)
        assert result.reference_value == 1.0


# ---------------------------------------------------------------------------
# TestValidationResult
# ---------------------------------------------------------------------------


class TestValidationResult:
    """Tests for the ValidationResult dataclass."""

    def test_dataclass_fields(self) -> None:
        """All expected fields are present and accessible."""
        vr = ValidationResult(
            model_name="xg_xgboost",
            metric_name="roc_auc",
            value=0.975,
            status="ok",
            threshold_warn=0.95,
            threshold_alert=0.92,
            reference_value=0.979,
        )
        assert vr.model_name == "xg_xgboost"
        assert vr.metric_name == "roc_auc"
        assert vr.value == 0.975
        assert vr.status == "ok"
        assert vr.threshold_warn == 0.95
        assert vr.threshold_alert == 0.92
        assert vr.reference_value == 0.979

    def test_frozen_dataclass(self) -> None:
        """ValidationResult is frozen (immutable)."""
        import pytest

        vr = ValidationResult(
            model_name="test",
            metric_name="test",
            value=1.0,
            status="ok",
            threshold_warn=0.5,
            threshold_alert=0.3,
            reference_value=1.0,
        )
        with pytest.raises(AttributeError):
            vr.status = "alert"  # type: ignore[misc]

    def test_status_values(self) -> None:
        """Status field accepts the three expected values."""
        for status in ("ok", "warn", "alert"):
            vr = ValidationResult(
                model_name="test",
                metric_name="test",
                value=0.5,
                status=status,
                threshold_warn=0.3,
                threshold_alert=0.2,
                reference_value=0.5,
            )
            assert vr.status == status
