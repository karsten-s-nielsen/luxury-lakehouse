"""Unit tests for the pure GK-insight domain functions (services/gk_insight.py).

Pure (stdlib + numpy); no Taipy/DB. Branch-complete on the verdict templaters.
"""

from services.gk_insight import (
    ReferenceBand,
    Verdict,
    cohort_values,
    defensive_verdict,
    distribution_quadrant,
    reference_band,
    sweeping_command,
    tercile_position,
)


# --- reference_band -------------------------------------------------------
def test_reference_band_returns_iqr_when_enough_members():
    band = reference_band([float(i) for i in range(1, 21)], min_cohort=8)
    assert isinstance(band, ReferenceBand)
    assert band.n == 20 and band.median == 10.5 and band.q1 < band.median < band.q3


def test_reference_band_none_below_min_cohort():
    assert reference_band([1.0, 2.0, 3.0], min_cohort=8) is None


def test_reference_band_excludes_nan_then_applies_floor():
    vals = [1.0, float("nan"), 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]  # 7 finite < 8
    assert reference_band(vals, min_cohort=8) is None


# --- cohort_values --------------------------------------------------------
def test_cohort_values_volume_weights_per_gk_and_drops_sub_floor():
    rows = [("A", 0.04, 30), ("A", 0.00, 10), ("B", 0.99, 5)]
    vals = cohort_values(rows, floor=20)
    assert len(vals) == 1
    assert abs(vals[0] - (0.04 * 30 + 0.0 * 10) / 40) < 1e-9


def test_cohort_values_skips_nan():
    rows = [("A", float("nan"), 50), ("A", 0.02, 50)]
    assert abs(cohort_values(rows, floor=20)[0] - 0.02) < 1e-9


# --- tercile_position -----------------------------------------------------
def test_tercile_position_low_mid_high():
    cohort = [float(i) for i in range(1, 100)]
    assert tercile_position(10, cohort) == "low"
    assert tercile_position(50, cohort) == "mid"
    assert tercile_position(90, cohort) == "high"


def test_tercile_position_lower_is_better_flips():
    cohort = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    assert tercile_position(1.0, cohort, lower_is_better=True) == "high"
    assert tercile_position(9.0, cohort, lower_is_better=True) == "low"


def test_tercile_position_tiny_cohort_returns_mid():
    assert tercile_position(5.0, [5.0]) == "mid"


# --- sweeping_command -----------------------------------------------------
def test_sweeping_command_majority_upper():
    assert sweeping_command(reach="high", pc="high", closing="high") == "upper"


def test_sweeping_command_mixed_is_mid():
    assert sweeping_command(reach="high", pc="mid", closing="low") == "mid"


def test_sweeping_command_lower():
    assert sweeping_command(reach="low", pc="low", closing="mid") == "lower"


# --- distribution_quadrant (offensive, branch-complete) -------------------
def test_distribution_quadrant_low_sample():
    v = distribution_quadrant(share_adds=0.2, progress_m=25, n=10, share_median=0.12, progress_median=22)
    assert v.phrase == "Indicative only — small sample"


def test_distribution_quadrant_cohort_too_small():
    v = distribution_quadrant(share_adds=0.2, progress_m=25, n=50, share_median=None, progress_median=None)
    assert v.phrase == "Cohort too small"


def test_distribution_quadrant_four_quadrants():
    # high threat + direct
    assert (
        distribution_quadrant(share_adds=0.24, progress_m=26, n=50, share_median=0.12, progress_median=22).phrase
        == "Proactive distributor"
    )
    # high threat + short
    assert (
        distribution_quadrant(share_adds=0.24, progress_m=15, n=50, share_median=0.12, progress_median=22).phrase
        == "Proactive recycler"
    )
    # low threat + direct
    assert (
        distribution_quadrant(share_adds=0.06, progress_m=26, n=50, share_median=0.12, progress_median=22).phrase
        == "Risk without reward"
    )
    # low threat + short
    assert (
        distribution_quadrant(share_adds=0.06, progress_m=15, n=50, share_median=0.12, progress_median=22).phrase
        == "Secure recycler"
    )


# --- defensive_verdict ----------------------------------------------------
def test_defensive_low_sample():
    assert defensive_verdict(command="upper", n_defended=10).phrase == "Indicative only — small sample"


def test_defensive_command_based_not_line_coupled():
    # Verdict is command-only now (line height is descriptive, not bucketed into the verdict).
    assert defensive_verdict(command="upper", n_defended=152).phrase == "Commands his box"
    assert defensive_verdict(command="lower", n_defended=152).phrase == "Line-keeper profile"
    assert defensive_verdict(command="mid", n_defended=152).phrase == "Typical box-keeper"
    # no detail asserts a deep/high line or a system mismatch
    for cmd in ("upper", "mid", "lower"):
        d = defensive_verdict(command=cmd, n_defended=152).detail.lower()
        assert "deep line" not in d and "high line" not in d and "unused" not in d


def test_verdict_is_frozen_dataclass():
    assert isinstance(defensive_verdict(command="mid", n_defended=99), Verdict)
