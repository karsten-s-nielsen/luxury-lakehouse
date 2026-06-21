"""Tests for the tracking PSxG scorer + out-of-sample calibration (tasks 1.1 / 1.2)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analytics.goalkeeper import PSxGModel, build_psxg_features_tracking
from analytics.psxg_tracking import (
    PlattParams,
    TrackingGateParams,
    apply_platt,
    fit_platt_calibration,
    score_tracking_psxg,
)


def _identity_scaler_model() -> PSxGModel:
    # 4-feature model; identity scaler so x_scaled == features; logits = feats @ coef + b.
    return PSxGModel(
        coefficients=np.array([1.0, 2.0, -0.05, 0.5]),
        intercept=-1.0,
        scaler_mean=np.zeros(4),
        scaler_scale=np.ones(4),
    )


def _shots() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "start_x": [90.0, 90.0],  # 15 m out, central → distance/angle from spadl_shot_geometry
            "start_y": [34.0, 34.0],
            "shot_crossing_y": [34.0, 34.0],  # centre → y_norm 0.5 → dist_from_centre 0
            "shot_crossing_z": [1.0, 1.0],  # z_norm 1/7.32 ≈ 0.1366
            "shot_crossing_confidence": [0.9, 0.2],  # row0 passes, row1 fails
            "shot_fit_rmse": [0.1, 2.0],  # row0 passes, row1 fails
        }
    )


def test_scorer_gates_as_flag_not_drop() -> None:
    out = score_tracking_psxg(_shots(), _identity_scaler_model(), gate=TrackingGateParams())
    assert len(out) == 2  # no rows dropped
    assert bool(out["psxg_gated"].iloc[0]) is False
    assert bool(out["psxg_gated"].iloc[1]) is True
    assert np.isnan(out["psxg"].iloc[1])  # gated row → NaN psxg


def test_scorer_math_matches_manual_logistic() -> None:
    shots = _shots()
    model = _identity_scaler_model()
    out = score_tracking_psxg(shots, model, gate=TrackingGateParams())
    # Recompute via the same 4-feature builder the scorer uses + the manual logistic.
    feats = build_psxg_features_tracking(shots)
    x_scaled = (feats - model.scaler_mean) / model.scaler_scale
    expected = 1.0 / (1.0 + np.exp(-(x_scaled @ model.coefficients + model.intercept)))
    assert out["psxg"].iloc[0] == pytest.approx(expected[0], abs=1e-6)


def test_scorer_empty_input() -> None:
    empty = _shots().iloc[0:0]
    out = score_tracking_psxg(empty, _identity_scaler_model())
    assert len(out) == 0
    assert "psxg" in out.columns and "psxg_gated" in out.columns


def test_apply_platt_identity_is_passthrough() -> None:
    p = np.array([0.1, 0.3, 0.5, 0.9])
    out = apply_platt(p, PlattParams(slope=1.0, intercept=0.0))
    assert out == pytest.approx(p, abs=1e-5)


def test_calibration_is_out_of_sample_and_well_formed() -> None:
    rng = np.random.default_rng(0)
    n = 200
    raw = rng.uniform(0.05, 0.6, size=n)
    is_goal = (rng.uniform(size=n) < raw).astype(int)  # raw is roughly calibrated
    match_keys = rng.integers(0, 20, size=n)  # 20 match groups
    report = fit_platt_calibration(raw, is_goal, match_keys, n_splits=5)
    assert report.n_shots == n
    assert report.n_groups == len(np.unique(match_keys))
    assert np.isfinite(report.cv_brier)  # held-out Brier computed
    assert isinstance(report.params, PlattParams)


def test_calibration_rejects_empty() -> None:
    with pytest.raises(ValueError, match="no gate-passed shots"):
        fit_platt_calibration(np.array([]), np.array([]), np.array([]))
