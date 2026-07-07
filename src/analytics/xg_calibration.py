"""Pure statistics for per-provider xG calibration and scoring-mode selection.

This module underpins the "Canonical-SPADL Pre-Shot xG Unification" feature. Each
tracking provider's xG is evaluated in two scoring modes — context-aware
(freeze-frame) versus tabular-only (zero-context) — and this module answers four
questions with pure statistics (no Spark, no I/O, no pickle):

1. **How good is a mode, with uncertainty?** ``bootstrap_auc_ci`` — a seeded,
   deterministic percentile bootstrap of out-of-sample ROC-AUC. Bootstrap (not a
   hand-rolled DeLong CI) is trivially correct at the small n (~225 shots per
   provider) this feature operates at.
2. **How do we map raw xG onto honest probabilities?** ``fit_platt`` /
   ``apply_platt`` — 1-feature Platt (logistic) scaling, monotone non-decreasing
   in xG, output in ``[0, 1]``, params serialised JSON-safe (no pickle).
   ``choose_calibrator`` prefers Platt and only switches to isotonic when isotonic
   strictly beats Platt on group-disjoint out-of-fold reliability (Brier).
3. **Which mode ships?** ``select_scoring_mode`` — gates on the AUC **CI lower
   bound** (not the point estimate) relative to StatsBomb, so a mode only ships
   when it is *reliably* good, not merely lucky on a point estimate.
4. **Is the shipped mode good on its own terms?** ``is_mode_certified`` — an
   independent certification (mode selection is a comparison; certification is an
   absolute bar). ``calibration_ok_n_aware`` checks aggregate calibration with a
   binomial test so small-n sampling noise is tolerated rather than flagged.

References:
    Platt, J. (1999). "Probabilistic Outputs for Support Vector Machines and
        Comparisons to Regularized Likelihood Methods."
    Efron, B. & Tibshirani, R. (1993). "An Introduction to the Bootstrap."
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import ArrayLike, NDArray
from scipy.stats import binomtest
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, roc_auc_score
from sklearn.model_selection import GroupKFold

# Weak regularisation so LogisticRegression approximates the Platt MLE rather than
# shrinking the slope toward zero (which would flatten calibration).
_PLATT_C = 1.0e10
_PLATT_MAX_ITER = 1000
# Floor for a binomial success probability — binomtest requires p in (0, 1).
_PROB_EPS = 1.0e-9


@dataclass(frozen=True)
class AucCi:
    """Point ROC-AUC with a bootstrap confidence interval.

    Attributes:
        auc: Point estimate of ROC-AUC on the full sample.
        lo: Lower confidence bound (e.g. 2.5th bootstrap percentile).
        hi: Upper confidence bound (e.g. 97.5th bootstrap percentile).
    """

    auc: float
    lo: float
    hi: float


@dataclass(frozen=True)
class PlattParams:
    """Serializable Platt-scaling parameters: ``sigmoid(a * xg + b)``.

    JSON-safe by construction (two floats, no pickle).
    """

    a: float
    b: float

    def to_dict(self) -> dict[str, float]:
        return {"a": self.a, "b": self.b}

    @classmethod
    def from_dict(cls, d: dict[str, float]) -> PlattParams:
        return cls(a=float(d["a"]), b=float(d["b"]))


@dataclass(frozen=True)
class IsotonicParams:
    """Serializable isotonic-regression knots for piecewise-linear interpolation.

    JSON-safe (two float lists, no pickle). Apply via ``apply_isotonic``.
    """

    x_thresholds: list[float]
    y_thresholds: list[float]

    def to_dict(self) -> dict[str, list[float]]:
        return {"x_thresholds": self.x_thresholds, "y_thresholds": self.y_thresholds}

    @classmethod
    def from_dict(cls, d: dict[str, list[float]]) -> IsotonicParams:
        return cls(
            x_thresholds=[float(x) for x in d["x_thresholds"]],
            y_thresholds=[float(y) for y in d["y_thresholds"]],
        )


def _as_1d(a: ArrayLike) -> NDArray[np.float64]:
    arr = np.asarray(a, dtype=np.float64).ravel()
    return arr


def bootstrap_auc_ci(
    xg: ArrayLike,
    y: ArrayLike,
    *,
    n_boot: int = 2000,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple[float, float, float]:
    """Percentile-bootstrap confidence interval for ROC-AUC.

    Deterministic: driven by a seeded ``np.random.default_rng(seed)``. The point
    AUC is computed on the full sample; the CI comes from resampling row indices
    with replacement ``n_boot`` times, recomputing AUC on each resample. Resamples
    that collapse to a single class (AUC undefined) are skipped.

    Args:
        xg: Predicted scores (higher = more likely a goal).
        y: Binary outcomes (1 = goal, 0 = no goal). Must contain both classes.
        n_boot: Number of bootstrap resamples.
        alpha: Two-sided miscoverage; the CI is the ``[alpha/2, 1-alpha/2]``
            percentile band (default 0.05 -> 2.5th / 97.5th).
        seed: RNG seed for reproducibility.

    Returns:
        ``(point_auc, lo, hi)``.

    Raises:
        ValueError: If the full sample is single-class (point AUC undefined) or no
            bootstrap resample was two-class.
    """
    scores = _as_1d(xg)
    labels = _as_1d(y)
    if scores.shape != labels.shape:
        raise ValueError("xg and y must have the same length")
    if np.unique(labels).size < 2:
        raise ValueError("y must contain both classes to compute ROC-AUC")

    point = float(roc_auc_score(labels, scores))

    rng = np.random.default_rng(seed)
    n = labels.shape[0]
    boot_aucs: list[float] = []
    for _ in range(n_boot):
        idx = rng.integers(0, n, size=n)
        y_b = labels[idx]
        if np.unique(y_b).size < 2:
            # Single-class resample -> AUC undefined; skip (percentile bootstrap).
            continue
        boot_aucs.append(float(roc_auc_score(y_b, scores[idx])))

    if not boot_aucs:
        raise ValueError("every bootstrap resample was single-class; increase n or check data")

    arr = np.asarray(boot_aucs, dtype=np.float64)
    lo = float(np.percentile(arr, 100.0 * (alpha / 2.0)))
    hi = float(np.percentile(arr, 100.0 * (1.0 - alpha / 2.0)))
    return point, lo, hi


def fit_platt(xg: ArrayLike, y: ArrayLike) -> PlattParams:
    """Fit 1-feature Platt scaling ``P = sigmoid(a * xg + b)``.

    Uses ``LogisticRegression`` with weak regularisation so the fit approximates
    the Platt maximum-likelihood slope/intercept. If the labels are single-class,
    returns a degenerate constant calibrator (``a = 0``, ``b = logit(prevalence)``)
    rather than raising, so per-fold use in ``choose_calibrator`` is robust.
    """
    scores = _as_1d(xg)
    labels = _as_1d(y).astype(int)
    classes = np.unique(labels)
    if classes.size < 2:
        # Degenerate: no discrimination possible; emit the constant base rate.
        prevalence = float(np.clip(labels.mean(), _PROB_EPS, 1.0 - _PROB_EPS))
        b = float(np.log(prevalence / (1.0 - prevalence)))
        return PlattParams(a=0.0, b=b)

    model = LogisticRegression(C=_PLATT_C, solver="lbfgs", max_iter=_PLATT_MAX_ITER)
    model.fit(scores.reshape(-1, 1), labels)
    a = float(np.asarray(model.coef_, dtype=np.float64).ravel()[0])
    b = float(np.asarray(model.intercept_, dtype=np.float64).ravel()[0])
    return PlattParams(a=a, b=b)


def apply_platt(xg: ArrayLike, params: PlattParams) -> NDArray[np.float64]:
    """Apply Platt scaling: ``sigmoid(a * xg + b)``, output in ``[0, 1]``.

    Monotone non-decreasing in ``xg`` whenever ``a >= 0`` (the expected regime for
    xG, where a higher score means a higher goal probability).
    """
    scores = _as_1d(xg)
    z = params.a * scores + params.b
    # Numerically stable logistic sigmoid.
    return np.where(z >= 0, 1.0 / (1.0 + np.exp(-z)), np.exp(z) / (1.0 + np.exp(z)))


def apply_isotonic(xg: ArrayLike, params: IsotonicParams) -> NDArray[np.float64]:
    """Apply an isotonic calibrator via piecewise-linear interpolation of knots."""
    scores = _as_1d(xg)
    x_knots = np.asarray(params.x_thresholds, dtype=np.float64)
    y_knots = np.asarray(params.y_thresholds, dtype=np.float64)
    if x_knots.size == 0:
        return np.full_like(scores, np.nan)
    return np.interp(scores, x_knots, y_knots).astype(np.float64)


def _assert_group_disjoint(train_groups: NDArray[Any], test_groups: NDArray[Any]) -> None:
    """Guard: fit-fold and measure-fold groups must be disjoint (no leakage)."""
    overlap = np.intersect1d(train_groups, test_groups)
    if overlap.size > 0:
        raise ValueError(f"group leakage between train and test folds: {overlap.tolist()}")


def groupkfold_auc(
    xg: ArrayLike,
    y: ArrayLike,
    groups: ArrayLike,
    n_splits: int = 5,
) -> float:
    """Pooled out-of-fold ROC-AUC under group-disjoint cross-validation.

    ``xg`` is already the score, so nothing is *fit* per fold — each fold simply
    contributes its held-out rows' scores to a pooled out-of-sample prediction
    vector, and a single ROC-AUC is computed over all held-out rows. Because every
    row is held out exactly once, with a fixed score this pooled OOS AUC equals the
    global AUC; the load-bearing invariant is the enforced train/test group
    disjointness (``GroupKFold`` guarantees it; we assert it explicitly), which is
    what makes per-fold calibrator fitting in ``choose_calibrator`` leak-free.

    Pooled (single AUC over all OOF rows) is chosen over mean-per-fold because it
    is stable when small folds land single-class, where per-fold AUC is undefined.
    """
    scores = _as_1d(xg)
    labels = _as_1d(y)
    grp = np.asarray(groups).ravel()
    n_groups = np.unique(grp).size
    splits = min(n_splits, n_groups)
    if splits < 2:
        raise ValueError("need at least 2 distinct groups for GroupKFold")

    gkf = GroupKFold(n_splits=splits)
    oof = np.full(scores.shape[0], np.nan, dtype=np.float64)
    for train_idx, test_idx in gkf.split(scores, labels, grp):
        _assert_group_disjoint(grp[train_idx], grp[test_idx])
        oof[test_idx] = scores[test_idx]

    if np.unique(labels).size < 2:
        raise ValueError("y must contain both classes to compute pooled OOS AUC")
    return float(roc_auc_score(labels, oof))


def select_scoring_mode(
    context_ci: AucCi,
    tabular_ci: AucCi,
    *,
    sb_auc: float,
    margin: float,
    floor: float,
) -> str:
    """Pick the scoring mode using AUC **CI lower bounds** (not point estimates).

    Ships ``"context_aware"`` iff the context mode's CI lower bound clears the
    relative floor ``max(sb_auc - margin, floor)`` AND strictly exceeds the tabular
    mode's CI lower bound. Otherwise ships ``"tabular_only"``. Using ``lo`` (not
    ``auc``) means a mode ships only when it is *reliably* good — a wide, lucky
    point estimate does not qualify.
    """
    relative_floor = max(sb_auc - margin, floor)
    if context_ci.lo >= relative_floor and context_ci.lo > tabular_ci.lo:
        return "context_aware"
    return "tabular_only"


def is_mode_certified(
    shipped_ci: AucCi,
    *,
    sb_auc: float,
    margin: float,
    floor: float,
) -> bool:
    """Certify the shipped mode on its own terms (independent of mode selection).

    Certification is an absolute bar: the shipped mode's CI lower bound must clear
    ``max(sb_auc - margin, floor)``. Selection is a *comparison* between modes;
    certification is a *threshold* on the one that shipped — a mode can be selected
    (best available) yet fail certification (not good enough absolutely).
    """
    return shipped_ci.lo >= max(sb_auc - margin, floor)


def calibration_ok_n_aware(sum_xg: float, sum_goals: float, n: int) -> bool:
    """Aggregate-calibration check that tolerates small-n sampling noise.

    Treats the ``n`` shots as Bernoulli trials with success probability
    ``p = sum_xg / n`` and asks whether the observed goal count ``round(sum_goals)``
    is plausible under that model via a two-sided binomial test. Returns True when
    ``p_value >= 0.05`` (observed goals within sampling noise of expected), False
    when the goal count is implausibly far from expectation. The binomial test is
    n-aware by construction: the same absolute gap is tolerated at small n and
    flagged at large n.

    Args:
        sum_xg: Sum of predicted xG over the sample (expected goals).
        sum_goals: Observed goal total (rounded to an integer count of successes).
        n: Number of shots (Bernoulli trials).
    """
    if n <= 0:
        raise ValueError("n must be positive")
    p = float(np.clip(sum_xg / n, _PROB_EPS, 1.0 - _PROB_EPS))
    k = round(float(sum_goals))  # float() first so a numpy scalar rounds to a Python int
    k = max(0, min(k, n))
    result = binomtest(k=k, n=n, p=p, alternative="two-sided")
    return bool(result.pvalue >= 0.05)


def choose_calibrator(
    xg: ArrayLike,
    y: ArrayLike,
    groups: ArrayLike,
    n_splits: int = 5,
) -> tuple[str, PlattParams | IsotonicParams]:
    """Return the calibrator to ship: Platt by default, isotonic only if it wins.

    Platt is the default (2-parameter, robust at small n, JSON-safe). Isotonic is
    selected only when it **strictly** beats Platt on group-disjoint out-of-fold
    reliability (Brier score) — a higher bar that guards against isotonic's
    tendency to overfit the empirical CDF at small n. The returned params are
    always JSON-safe (``PlattParams`` or ``IsotonicParams``); the final model is
    refit on the full sample once the winner is decided.

    Returns:
        ``(kind, params)`` where ``kind`` is ``"platt"`` or ``"isotonic"``.
    """
    scores = _as_1d(xg)
    labels = _as_1d(y).astype(int)
    grp = np.asarray(groups).ravel()
    n_groups = np.unique(grp).size
    splits = min(n_splits, n_groups)

    platt_kind: str = "platt"
    if splits >= 2 and np.unique(labels).size >= 2:
        gkf = GroupKFold(n_splits=splits)
        platt_oof = np.full(scores.shape[0], np.nan, dtype=np.float64)
        iso_oof = np.full(scores.shape[0], np.nan, dtype=np.float64)
        for train_idx, test_idx in gkf.split(scores, labels, grp):
            _assert_group_disjoint(grp[train_idx], grp[test_idx])
            platt_oof[test_idx] = apply_platt(scores[test_idx], fit_platt(scores[train_idx], labels[train_idx]))
            iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
            iso.fit(scores[train_idx], labels[train_idx])
            iso_oof[test_idx] = np.asarray(iso.predict(scores[test_idx]), dtype=np.float64)

        # Compare only where both produced a finite OOF prediction.
        mask = np.isfinite(platt_oof) & np.isfinite(iso_oof)
        if mask.sum() > 0 and np.unique(labels[mask]).size >= 2:
            platt_brier = float(brier_score_loss(labels[mask], platt_oof[mask]))
            iso_brier = float(brier_score_loss(labels[mask], iso_oof[mask]))
            if iso_brier < platt_brier:
                platt_kind = "isotonic"

    if platt_kind == "isotonic":
        full_iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        full_iso.fit(scores, labels)
        x_knots = np.asarray(full_iso.X_thresholds_, dtype=np.float64)
        y_knots = np.asarray(full_iso.y_thresholds_, dtype=np.float64)
        return "isotonic", IsotonicParams(
            x_thresholds=[float(v) for v in x_knots],
            y_thresholds=[float(v) for v in y_knots],
        )

    return "platt", fit_platt(scores, labels)
