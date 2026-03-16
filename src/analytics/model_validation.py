"""Model validation and drift detection — pure scipy/numpy.

Provides statistical tests for monitoring model health:
- PSI (Population Stability Index) for distribution shift
- Wasserstein distance for distributional drift
- CUSUM (Cumulative Sum) control chart for process drift
- KS test for distribution shape comparison
- Physical bounds checks for hard range constraints
- Field sum constraint for pitch control grid integrity

All functions are pure (no Spark, no I/O) — designed to be called from
the ingestion pipeline or unit tests.

References:
    Shewhart, W. (1931). "Economic Control of Quality of Manufactured Product."
    Page, E.S. (1954). "Continuous Inspection Schemes." Biometrika 41(1-2).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy.stats import ks_2samp, wasserstein_distance


@dataclass(frozen=True)
class ValidationResult:
    """Immutable result of a single validation check.

    Attributes:
        model_name: Identifier for the model being validated.
        metric_name: Identifier for the specific metric.
        value: Computed metric value.
        status: One of ``"ok"``, ``"warn"``, ``"alert"``.
        threshold_warn: Warning threshold (value exceeding this triggers warn).
        threshold_alert: Alert threshold (value exceeding this triggers alert).
        reference_value: Expected/baseline value for comparison.
    """

    model_name: str
    metric_name: str
    value: float
    status: str  # "ok" | "warn" | "alert"
    threshold_warn: float
    threshold_alert: float
    reference_value: float


# ---------------------------------------------------------------------------
# PSI — Population Stability Index
# ---------------------------------------------------------------------------


def compute_psi(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    """Population Stability Index between two distributions.

    PSI quantifies how much the distribution of model outputs has shifted
    from a reference baseline.  Values:
    - PSI < 0.1: insignificant shift
    - 0.1 <= PSI < 0.2: minor shift (monitor)
    - PSI >= 0.2: significant shift (investigate)

    Uses histogram binning with shared bin edges derived from the reference
    distribution.  A small epsilon is added to prevent log(0).

    Args:
        reference: 1-D array of reference (baseline) values.
        current: 1-D array of current (production) values.
        n_bins: Number of histogram bins.

    Returns:
        Non-negative PSI value.
    """
    eps = 1e-6

    # Compute bin edges from the reference distribution
    bin_edges = np.histogram_bin_edges(reference, bins=n_bins)

    # Compute proportions in each bin
    ref_counts, _ = np.histogram(reference, bins=bin_edges)
    cur_counts, _ = np.histogram(current, bins=bin_edges)

    ref_proportions = ref_counts / len(reference) + eps
    cur_proportions = cur_counts / len(current) + eps

    # PSI formula: sum((P_i - Q_i) * ln(P_i / Q_i))
    psi = float(np.sum((cur_proportions - ref_proportions) * np.log(cur_proportions / ref_proportions)))
    return max(psi, 0.0)


# ---------------------------------------------------------------------------
# Wasserstein distance
# ---------------------------------------------------------------------------


def compute_wasserstein_drift(reference: np.ndarray, current: np.ndarray) -> float:
    """Wasserstein (earth mover's) distance between two 1-D distributions.

    The Wasserstein-1 distance measures the minimum "work" required to
    transform one distribution into another.  A value of 0 means the
    distributions are identical.

    Args:
        reference: 1-D array of reference values.
        current: 1-D array of current values.

    Returns:
        Non-negative Wasserstein distance.
    """
    return float(wasserstein_distance(reference, current))


# ---------------------------------------------------------------------------
# CUSUM — Cumulative Sum Control Chart
# ---------------------------------------------------------------------------


def compute_cusum(
    values: np.ndarray,
    target_mean: float,
    sigma: float,
) -> tuple[float, str]:
    """Cumulative sum control chart for detecting sustained process shifts.

    Implements the one-sided upper CUSUM (Page 1954).  Tracks cumulative
    deviation from ``target_mean`` and resets to zero when negative.  The
    maximum CUSUM value across all observations indicates the severity of
    any sustained shift.

    Status thresholds (industry standard):
    - ``"ok"``    if max CUSUM < 3 * sigma
    - ``"warn"``  if max CUSUM < 5 * sigma
    - ``"alert"`` otherwise

    Args:
        values: 1-D array of observed values (e.g., per-match detection rates).
        target_mean: Expected process mean.
        sigma: Process standard deviation.

    Returns:
        Tuple of (max_cusum, status).
    """
    if sigma <= 0:
        msg = "sigma must be positive"
        raise ValueError(msg)

    cusum = 0.0
    max_cusum = 0.0
    for v in values:
        cusum = max(0.0, cusum + (float(v) - target_mean))
        if cusum > max_cusum:
            max_cusum = cusum

    threshold_warn = 3.0 * sigma
    threshold_alert = 5.0 * sigma

    if max_cusum >= threshold_alert:
        status = "alert"
    elif max_cusum >= threshold_warn:
        status = "warn"
    else:
        status = "ok"

    return max_cusum, status


# ---------------------------------------------------------------------------
# KS test
# ---------------------------------------------------------------------------


def check_ks_test(
    reference: np.ndarray,
    current: np.ndarray,
    alpha: float = 0.05,
) -> tuple[float, float, str]:
    """Two-sample Kolmogorov-Smirnov test for distribution shape comparison.

    Tests whether two samples come from the same continuous distribution.

    Args:
        reference: 1-D array of reference values.
        current: 1-D array of current values.
        alpha: Significance level.  p < alpha triggers ``"alert"``.

    Returns:
        Tuple of (statistic, p_value, status).
    """
    stat_result: Any = ks_2samp(reference, current)
    statistic = float(stat_result.statistic)
    p_value = float(stat_result.pvalue)

    if p_value < alpha:
        status = "alert"
    elif p_value < alpha * 2:
        status = "warn"
    else:
        status = "ok"

    return statistic, p_value, status


# ---------------------------------------------------------------------------
# Physical bounds check
# ---------------------------------------------------------------------------


def check_physical_bounds(
    values: np.ndarray,
    lower: float,
    upper: float,
    model_name: str,
    metric_name: str,
) -> ValidationResult:
    """Check that all values fall within physical bounds [lower, upper].

    Returns a :class:`ValidationResult` with ``status="alert"`` if any value
    exceeds the bounds, ``"ok"`` otherwise.

    Args:
        values: 1-D array of observed values.
        lower: Lower bound (inclusive).
        upper: Upper bound (inclusive).
        model_name: Model identifier for the result.
        metric_name: Metric identifier for the result.

    Returns:
        :class:`ValidationResult` with the maximum observed value and status.
    """
    max_val = float(np.max(values)) if len(values) > 0 else 0.0
    min_val = float(np.min(values)) if len(values) > 0 else 0.0

    if max_val > upper or min_val < lower:
        status = "alert"
    else:
        status = "ok"

    return ValidationResult(
        model_name=model_name,
        metric_name=metric_name,
        value=max_val,
        status=status,
        threshold_warn=upper,
        threshold_alert=upper,
        reference_value=(upper + lower) / 2.0,
    )


# ---------------------------------------------------------------------------
# Field sum constraint (pitch control grid integrity)
# ---------------------------------------------------------------------------


def check_field_sum_constraint(
    ppcf_grid: np.ndarray,
    tolerance: float = 0.05,
) -> ValidationResult:
    """Check that pitch control grid cells sum to approximately 1.0.

    Each cell in the PPCF grid represents the probability of one team
    controlling that area.  The home + away probabilities at each cell
    should sum to ~1.0.  This function checks the mean per-cell sum
    against the tolerance.

    For a single-team PPCF grid, this checks that the mean value is
    reasonable (i.e., not all 0 or all > 1).

    Args:
        ppcf_grid: 2-D pitch control grid (values in [0, 1]).
        tolerance: Acceptable deviation from 1.0.

    Returns:
        :class:`ValidationResult` with the mean cell sum and status.
    """
    mean_sum = float(np.mean(ppcf_grid))
    deviation = abs(mean_sum - 1.0)

    if deviation > tolerance:
        status = "alert"
    else:
        status = "ok"

    return ValidationResult(
        model_name="pitch_control",
        metric_name="field_sum",
        value=mean_sum,
        status=status,
        threshold_warn=tolerance,
        threshold_alert=tolerance,
        reference_value=1.0,
    )
