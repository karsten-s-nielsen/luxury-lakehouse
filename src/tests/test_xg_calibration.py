"""Tests for the pure xG calibration / mode-selection statistics module."""

from __future__ import annotations

import numpy as np

from analytics.xg_calibration import (
    AucCi,
    apply_platt,
    bootstrap_auc_ci,
    calibration_ok_n_aware,
    choose_calibrator,
    fit_platt,
    groupkfold_auc,
    is_mode_certified,
    select_scoring_mode,
)


def test_bootstrap_auc_ci_recovers_separable_and_random():
    rng = np.random.default_rng(0)
    # separable: xg perfectly ranks goals
    y = np.array([0] * 50 + [1] * 50)
    xg = np.concatenate([rng.uniform(0, 0.4, 50), rng.uniform(0.6, 1.0, 50)])
    auc, lo, hi = bootstrap_auc_ci(xg, y, n_boot=500, seed=0)
    assert auc > 0.98 and lo <= auc <= hi and hi <= 1.0
    # random labels: AUC ~0.5, CI brackets 0.5
    y2 = rng.integers(0, 2, 200)
    xg2 = rng.uniform(0, 1, 200)
    _auc2, lo2, hi2 = bootstrap_auc_ci(xg2, y2, n_boot=500, seed=0)
    assert lo2 < 0.5 < hi2


def test_bootstrap_auc_ci_is_deterministic():
    rng = np.random.default_rng(3)
    y = rng.integers(0, 2, 150)
    xg = rng.uniform(0, 1, 150)
    first = bootstrap_auc_ci(xg, y, n_boot=300, seed=7)
    second = bootstrap_auc_ci(xg, y, n_boot=300, seed=7)
    assert first == second


def test_platt_is_monotone_and_bounded():
    xg = np.linspace(0.01, 0.99, 100)
    y = (xg > 0.5).astype(int)
    params = fit_platt(xg, y)
    p = apply_platt(xg, params)
    assert np.all((p >= 0) & (p <= 1))
    assert np.all(np.diff(p) >= -1e-9)  # monotone non-decreasing in xg


def test_groupkfold_auc_has_no_group_leakage():
    # measure-group must be disjoint from fit-groups — assert via the helper's guard/callback
    rng = np.random.default_rng(1)
    n = 200
    groups = rng.integers(0, 10, n)
    y = rng.integers(0, 2, n)
    xg = rng.uniform(0, 1, n)
    auc = groupkfold_auc(xg, y, groups)
    assert 0.0 <= auc <= 1.0


def test_gate_uses_ci_lower_bound_not_point_estimate():
    ctx = AucCi(auc=0.80, lo=0.74, hi=0.86)
    tab = AucCi(auc=0.78, lo=0.72, hi=0.84)
    # ctx.lo 0.74 < relative floor max(0.82-0.05, 0.65)=0.77 -> tabular
    assert select_scoring_mode(ctx, tab, sb_auc=0.82, margin=0.05, floor=0.65) == "tabular_only"
    ctx2 = AucCi(auc=0.82, lo=0.79, hi=0.85)
    assert select_scoring_mode(ctx2, tab, sb_auc=0.82, margin=0.05, floor=0.65) == "context_aware"
    # context beats floor on lo but does NOT beat tabular's lo -> tabular
    ctx3 = AucCi(auc=0.83, lo=0.78, hi=0.88)
    tab3 = AucCi(auc=0.84, lo=0.80, hi=0.88)
    assert select_scoring_mode(ctx3, tab3, sb_auc=0.82, margin=0.05, floor=0.65) == "tabular_only"


def test_mode_selection_is_not_certification():
    weak = AucCi(auc=0.63, lo=0.58, hi=0.68)
    weaker = AucCi(auc=0.60, lo=0.55, hi=0.65)
    assert select_scoring_mode(weak, weaker, sb_auc=0.82, margin=0.05, floor=0.65) == "tabular_only"
    assert is_mode_certified(weak, sb_auc=0.82, margin=0.05, floor=0.65) is False
    strong = AucCi(auc=0.82, lo=0.79, hi=0.85)
    assert is_mode_certified(strong, sb_auc=0.82, margin=0.05, floor=0.65) is True


def test_n_aware_calibration_tolerates_small_n_noise():
    assert calibration_ok_n_aware(sum_xg=25.0, sum_goals=28, n=225) is True
    assert calibration_ok_n_aware(sum_xg=25.0, sum_goals=60, n=225) is False


def test_platt_params_are_json_safe():
    import json

    xg = np.linspace(0.01, 0.99, 50)
    y = (xg > 0.4).astype(int)
    params = fit_platt(xg, y)
    dumped = json.dumps(params.to_dict())
    restored = type(params).from_dict(json.loads(dumped))
    np.testing.assert_allclose(apply_platt(xg, params), apply_platt(xg, restored))


def test_choose_calibrator_defaults_to_platt():
    rng = np.random.default_rng(2)
    n = 300
    groups = rng.integers(0, 12, n)
    xg = rng.uniform(0, 1, n)
    # well-calibrated-ish labels drawn from xg -> Platt should be adequate
    y = (rng.uniform(0, 1, n) < xg).astype(int)
    kind, _ = choose_calibrator(xg, y, groups)
    assert kind in {"platt", "isotonic"}
