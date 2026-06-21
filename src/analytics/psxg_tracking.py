"""Tracking-modality PSxG scoring + out-of-sample calibration.

The PSxG model (2-feature logistic) lives in :mod:`analytics.goalkeeper`; this
module applies it to the *tracking* modality (TF-48 ``shot_crossing_y/z`` in
SPADL metres) and provides the out-of-sample Platt recalibration used for the
tracking cohort. See spec ``docs/superpowers/specs/2026-06-20-psxg-tracking-extension-design.md``
(D-C / D-F) and plan tasks 1.1 / 1.2.

Design notes
------------
- **Gating is a flag, not a row-drop (D-F):** a shot whose trajectory fit fails
  the confidence/RMSE gate is kept with ``psxg = NaN`` and ``psxg_gated = True``,
  so the downstream GK aggregate can compute ``shots_faced_total`` (pre-gate) and
  ``coverage_pct`` rather than silently shrinking the denominator.
- **Calibration is out-of-sample via GroupKFold by ``match_key`` (B2/C1):** plain
  k-fold would leak shots from the same match across train/test (the xT-3 leak
  class); grouping by match prevents it. The reported Brier is the cross-validated
  (held-out) value; the final Platt fit uses all gate-passed shots.
- Tiny driver-side workload (~hundreds of on-target tracking shots) — pure
  pandas/numpy + sklearn, no Spark.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import numpy as np

from analytics.goalkeeper import PSxGModel, normalise_tracking_goalmouth

if TYPE_CHECKING:
    import pandas as pd

# Gate defaults — provisional, tuned at calibration time from the per-provider fit
# distribution (1.2 reports per-provider reliability). Verified ranges (2026-06-21):
# GS avg_conf 0.795 / median rmse 0.199; SkillCorner 0.669 / 0.471; IDSSE 0.711 / 0.119.
# These keep the bulk of GS/IDSSE and most of (noisier) SkillCorner.
_DEFAULT_MIN_CONFIDENCE = 0.5
_DEFAULT_MAX_FIT_RMSE = 1.0


@dataclasses.dataclass(frozen=True)
class TrackingGateParams:
    """Confidence/RMSE gate for tracking-derived goalmouth crossings (D-F)."""

    min_confidence: float = _DEFAULT_MIN_CONFIDENCE
    max_fit_rmse: float = _DEFAULT_MAX_FIT_RMSE


@dataclasses.dataclass(frozen=True)
class PlattParams:
    """Platt-scaling parameters: ``calibrated = sigmoid(slope * raw + intercept)``."""

    slope: float
    intercept: float


def score_tracking_psxg(
    shots: pd.DataFrame,
    model: PSxGModel,
    *,
    gate: TrackingGateParams | None = None,
) -> pd.DataFrame:
    """Score on-target tracking shots with the PSxG model (raw, pre-calibration).

    Args:
        shots: on-target tracking shot rows with ``shot_crossing_y``,
            ``shot_crossing_z``, ``shot_crossing_confidence``, ``shot_fit_rmse``
            (SPADL metres / fit diagnostics).
        model: the trained :class:`~analytics.goalkeeper.PSxGModel`.
        gate: confidence/RMSE gate; defaults to :class:`TrackingGateParams`.

    Returns:
        Copy of ``shots`` with two added columns:
        - ``psxg`` (float64): raw model probability; ``NaN`` for gate-failed rows.
        - ``psxg_gated`` (bool): True where the trajectory fit failed the gate.

    The row is **never dropped** for gating (D-F) — gated rows stay with
    ``psxg = NaN`` so coverage is computable downstream.
    """
    import pandas as pd

    gate = gate or TrackingGateParams()
    out = shots.copy()
    n = len(out)
    if n == 0:
        out["psxg"] = pd.array([], dtype="float64")
        out["psxg_gated"] = pd.array([], dtype="boolean")
        return out

    feats = normalise_tracking_goalmouth(
        out["shot_crossing_y"].to_numpy(dtype=np.float64),
        out["shot_crossing_z"].to_numpy(dtype=np.float64),
    )
    # Same scale + logistic the model applies (mirrors goalkeeper.predict_psxg).
    x_scaled = (feats - model.scaler_mean) / model.scaler_scale
    logits = x_scaled @ model.coefficients + model.intercept
    raw = 1.0 / (1.0 + np.exp(-logits))

    conf = out["shot_crossing_confidence"].to_numpy(dtype=np.float64)
    rmse = out["shot_fit_rmse"].to_numpy(dtype=np.float64)
    # NaN diagnostics fail the gate (comparison with NaN is False either way).
    passed = (conf >= gate.min_confidence) & (rmse <= gate.max_fit_rmse)
    gated = ~passed

    out["psxg"] = np.where(gated, np.nan, raw)
    out["psxg_gated"] = pd.array(gated.tolist(), dtype="boolean")
    return out


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-z))


def _logit(p: np.ndarray, *, eps: float = 1e-6) -> np.ndarray:
    p = np.clip(p, eps, 1.0 - eps)
    return np.log(p / (1.0 - p))


def apply_platt(raw_psxg: np.ndarray, params: PlattParams) -> np.ndarray:
    """Apply Platt scaling to raw PSxG on the logit scale."""
    return _sigmoid(params.slope * _logit(np.asarray(raw_psxg, dtype=np.float64)) + params.intercept)


@dataclasses.dataclass(frozen=True)
class CalibrationReport:
    """Out-of-sample calibration result for the tracking cohort."""

    params: PlattParams
    cv_brier: float
    cv_brier_uncalibrated: float
    n_shots: int
    n_groups: int


def fit_platt_calibration(
    raw_psxg: np.ndarray,
    is_goal: np.ndarray,
    match_keys: np.ndarray,
    *,
    n_splits: int = 5,
) -> CalibrationReport:
    """Fit Platt scaling with GroupKFold-by-match out-of-sample evaluation.

    Operates on the GATE-PASSED shots only (callers filter ``~psxg_gated`` and
    drop ``NaN`` psxg first). The final :class:`PlattParams` are fit on all rows;
    ``cv_brier`` is the held-out (GroupKFold by ``match_keys``) Brier, never the
    in-sample value (B2 — the xT-3 cross-game leak guard).

    Args:
        raw_psxg: raw model probabilities (gate-passed, non-NaN).
        is_goal: 0/1 outcomes aligned with ``raw_psxg``.
        match_keys: grouping key (one match → one group; never split).
        n_splits: GroupKFold splits (capped at the number of distinct groups).

    Returns:
        :class:`CalibrationReport` with the final Platt params + CV Brier.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import brier_score_loss
    from sklearn.model_selection import GroupKFold

    raw = np.asarray(raw_psxg, dtype=np.float64)
    y = np.asarray(is_goal, dtype=np.int64)
    groups = np.asarray(match_keys)
    n = len(raw)
    if n != len(y) or n != len(groups):
        raise ValueError("raw_psxg, is_goal, match_keys must be the same length")
    if n == 0:
        raise ValueError("no gate-passed shots to calibrate")

    n_groups = len(np.unique(groups))
    x = _logit(raw).reshape(-1, 1)

    def _fit(features: np.ndarray, target: np.ndarray) -> PlattParams:
        clf = LogisticRegression(max_iter=1000, solver="lbfgs")
        clf.fit(features, target)
        coef = np.asarray(clf.coef_, dtype=np.float64).ravel()
        intercept = np.asarray(clf.intercept_, dtype=np.float64).ravel()
        return PlattParams(slope=float(coef[0]), intercept=float(intercept[0]))

    # Out-of-sample CV Brier (GroupKFold by match). Needs >=2 groups and both classes.
    splits = min(n_splits, n_groups)
    oos_pred = np.full(n, np.nan, dtype=np.float64)
    if splits >= 2 and len(np.unique(y)) > 1:
        gkf = GroupKFold(n_splits=splits)
        for train_idx, test_idx in gkf.split(x, y, groups):
            if len(np.unique(y[train_idx])) < 2:
                continue  # degenerate fold (single class) — leave NaN, excluded below
            params = _fit(x[train_idx], y[train_idx])
            oos_pred[test_idx] = apply_platt(raw[test_idx], params)
    scored = ~np.isnan(oos_pred)
    cv_brier = float(brier_score_loss(y[scored], oos_pred[scored])) if scored.sum() > 0 else float("nan")
    cv_brier_uncal = float(brier_score_loss(y, raw))

    final_params = _fit(x, y)
    return CalibrationReport(
        params=final_params,
        cv_brier=cv_brier,
        cv_brier_uncalibrated=cv_brier_uncal,
        n_shots=n,
        n_groups=n_groups,
    )
