"""Custom xG model -- logistic regression baseline + gradient-boosted XGBoost.

Provides training, evaluation, and pickle-free serialization for two xG models:
1. Logistic baseline (distance + angle only) -- interpretable reference point.
2. XGBoost with isotonic calibration (all 13 features) -- production model.

Also provides freeze-frame parsing for v2 set encoder model inference.

Serialization uses JSON envelopes with base64-encoded XGBoost booster bytes,
avoiding pickle entirely (banned by project security policy).
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from sklearn.calibration import CalibratedClassifierCV, _CalibratedClassifier, calibration_curve
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from xgboost import XGBClassifier

# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------

_CATEGORICAL_FEATURES = ["shot_body_part", "shot_technique", "shot_type", "play_pattern"]
_NUMERIC_FEATURES = [
    "distance_to_goal",
    "shot_angle",
    "location_x",
    "location_y",
    "end_location_x",
    "end_location_y",
    "period",
    "minute",
]
_BOOLEAN_FEATURES = ["is_first_time"]
_BASELINE_FEATURES = ["distance_to_goal", "shot_angle"]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class XGModelConfig:
    """Immutable configuration for xG model training."""

    features: tuple[str, ...] = tuple(_NUMERIC_FEATURES + _BOOLEAN_FEATURES + _CATEGORICAL_FEATURES)
    categorical_features: tuple[str, ...] = tuple(_CATEGORICAL_FEATURES)
    target: str = "is_goal"
    n_estimators: int = 100
    max_depth: int = 3
    learning_rate: float = 0.1
    calibration_method: str = "isotonic"
    test_size: float = 0.2
    random_state: int = 42


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------


def build_features(
    shots_df: pd.DataFrame,
    config: XGModelConfig,
    expected_features: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """One-hot encode categoricals, fill nulls, return (X, y).

    - Categorical NULLs filled with ``"Unknown"``
    - Boolean features coerced to float (True=1.0, False/None=0.0)
    - Numeric features coerced and NaN-filled with 0.0
    - One-hot encoding applied to all categorical columns present
    - If ``expected_features`` is provided, reindex to match training-time
      columns (pads missing one-hot columns with 0.0).  This handles the case
      where ``pd.get_dummies`` produces fewer columns at scoring time than at
      training time (e.g., a competition with no kick-off shots).
    """
    df = shots_df.copy()

    # Fill NULL categoricals with "Unknown"
    for cat_col in config.categorical_features:
        if cat_col in df.columns:
            df[cat_col] = df[cat_col].fillna("Unknown")

    # Boolean features -> float (True=1.0, False/None=0.0)
    for bool_col in _BOOLEAN_FEATURES:
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].astype(float).fillna(0.0)

    # Numeric features -> coerce and fill NaN
    for num_col in _NUMERIC_FEATURES:
        if num_col in df.columns:
            df[num_col] = cast(pd.Series, pd.to_numeric(df[num_col], errors="coerce")).fillna(0.0)

    # One-hot encode categoricals
    cats_present = [c for c in config.categorical_features if c in df.columns]
    df_encoded = pd.get_dummies(df, columns=cats_present, dtype=float)

    # Collect feature columns: numeric + boolean + one-hot encoded
    feature_cols: list[str] = []
    for col in _NUMERIC_FEATURES + _BOOLEAN_FEATURES:
        if col in df_encoded.columns:
            feature_cols.append(col)
    for cat in config.categorical_features:
        for col in df_encoded.columns:
            if col.startswith(f"{cat}_"):
                feature_cols.append(col)

    x = cast(pd.DataFrame, df_encoded[feature_cols].astype(float))

    # Align to training-time feature set (pad missing one-hot columns with 0.0)
    if expected_features is not None:
        x = x.reindex(columns=expected_features, fill_value=0.0)

    y: pd.Series
    if config.target in df.columns:
        target_series = cast(pd.Series, pd.to_numeric(df[config.target], errors="coerce")).fillna(0)
        y = cast(pd.Series, target_series.astype(int))
    else:
        y = pd.Series(np.zeros(len(df)), dtype=int)

    return x, y


# ---------------------------------------------------------------------------
# Freeze-frame parsing (v2 set encoder)
# ---------------------------------------------------------------------------

_log = logging.getLogger(__name__)


def parse_freeze_frame(json_str: str | None) -> np.ndarray:
    """Parse shot_freeze_frame JSON to (N, 4) player feature array.

    Columns: [x_norm, y_norm, is_keeper, is_teammate], all normalized to
    StatsBomb pitch dimensions (120x80).

    Returns empty (0, 4) array for None, empty, or invalid input.
    """
    if json_str is None:
        return np.empty((0, 4), dtype=np.float64)
    try:
        players = json.loads(json_str)
        if not players:
            return np.empty((0, 4), dtype=np.float64)
        features: list[list[float]] = []
        for p in players:
            loc = p.get("location", [0, 0])
            features.append(
                [
                    loc[0] / 120.0,
                    loc[1] / 80.0,
                    float(p.get("keeper", False)),
                    float(p.get("teammate", False)),
                ]
            )
        return np.array(features, dtype=np.float64)
    except (json.JSONDecodeError, TypeError, IndexError):
        _log.debug("Failed to parse freeze frame JSON: %s", json_str[:80] if json_str else "None")
        return np.empty((0, 4), dtype=np.float64)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------


def train_logistic_baseline(
    x: pd.DataFrame,
    y: pd.Series,
    *,
    random_state: int = 42,
) -> CalibratedClassifierCV:
    """Train logistic regression on distance_to_goal + shot_angle only.

    Uses 5-fold cross-validated isotonic calibration with ``ensemble=False``
    to produce a single calibrated estimator matching the serialization format.
    """
    baseline_cols = [c for c in _BASELINE_FEATURES if c in x.columns]
    x_baseline = x[baseline_cols]

    base_model = LogisticRegression(random_state=random_state, max_iter=1000)
    calibrated = CalibratedClassifierCV(base_model, cv=5, method="isotonic", ensemble=False)  # type: ignore[arg-type]
    calibrated.fit(x_baseline, y)
    return calibrated


def train_xgboost_model(
    x: pd.DataFrame,
    y: pd.Series,
    config: XGModelConfig | None = None,
) -> CalibratedClassifierCV:
    """Train XGBoost classifier with cross-validated isotonic calibration.

    Uses ``ensemble=False`` to produce a single calibrated estimator (trained
    on the full dataset after cross-validated calibrator fitting), matching
    the serialization format that stores one estimator + one calibrator.
    """
    if config is None:
        config = XGModelConfig()

    base_model = XGBClassifier(
        n_estimators=config.n_estimators,
        max_depth=config.max_depth,
        learning_rate=config.learning_rate,
        random_state=config.random_state,
        eval_metric="logloss",
    )
    calibrated = CalibratedClassifierCV(base_model, cv=5, method=config.calibration_method, ensemble=False)  # type: ignore[arg-type]
    calibrated.fit(x, y)
    return calibrated


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def evaluate_model(model: Any, x: pd.DataFrame, y: pd.Series) -> dict[str, float]:
    """Evaluate model: brier_score, log_loss, roc_auc, calibration_error (ECE).

    Returns a dict with four metric keys.  Calibration error is the mean
    absolute deviation across 10 uniform-width bins (expected calibration error).
    """
    proba: np.ndarray = model.predict_proba(x)[:, 1]

    # Expected calibration error (ECE)
    fraction_positives, mean_predicted = calibration_curve(y, proba, n_bins=10, strategy="uniform")
    ece = float(np.mean(np.abs(fraction_positives - mean_predicted)))

    return {
        "brier_score": float(brier_score_loss(y, proba)),
        "log_loss": float(log_loss(y, proba)),
        "roc_auc": float(roc_auc_score(y, proba)),
        "calibration_error": ece,
    }


# ---------------------------------------------------------------------------
# Isotonic calibrator (de)serialization helpers
# ---------------------------------------------------------------------------


def _serialize_isotonic(calibrator: IsotonicRegression) -> dict[str, Any]:
    """Serialize an isotonic regression calibrator to a JSON-safe dict."""
    return {
        "X_thresholds": calibrator.X_thresholds_.tolist(),
        "y_thresholds": calibrator.y_thresholds_.tolist(),
        "X_min": float(calibrator.X_min_),
        "X_max": float(calibrator.X_max_),
        "increasing": bool(calibrator.increasing_),
    }


def _deserialize_isotonic(data: dict[str, Any]) -> IsotonicRegression:
    """Reconstruct a fitted isotonic regression calibrator from serialized data."""
    ir = IsotonicRegression(out_of_bounds="clip")
    ir.X_thresholds_ = np.array(data["X_thresholds"])
    ir.y_thresholds_ = np.array(data["y_thresholds"])
    ir.X_min_ = data["X_min"]
    ir.X_max_ = data["X_max"]
    ir.increasing_ = data["increasing"]
    # Rebuild the interpolation function that IsotonicRegression.predict uses
    ir.f_ = interp1d(
        ir.X_thresholds_,
        ir.y_thresholds_,
        kind="linear",
        bounds_error=False,
        fill_value=cast(float, (ir.y_thresholds_[0], ir.y_thresholds_[-1])),  # type: ignore[arg-type]
    )
    return ir


def _wrap_calibrated(estimator: Any, calibrator: IsotonicRegression) -> CalibratedClassifierCV:
    """Wrap an estimator + isotonic calibrator into a CalibratedClassifierCV.

    Reconstructs the sklearn internal ``_CalibratedClassifier`` wrapper directly,
    avoiding a redundant ``.fit()`` call.  The outer ``CalibratedClassifierCV``
    is constructed with ``cv=5`` (never fitted) — only its internal attributes
    are populated manually.
    """
    classes = np.array([0, 1])
    cc = _CalibratedClassifier(estimator=estimator, calibrators=[calibrator], classes=classes, method="isotonic")

    calibrated = CalibratedClassifierCV(estimator, cv=5)
    calibrated.calibrated_classifiers_ = [cc]
    calibrated.classes_ = classes
    return calibrated


# ---------------------------------------------------------------------------
# XGBoost serialization (no pickle)
# ---------------------------------------------------------------------------


def serialize_xgboost_model(model: CalibratedClassifierCV) -> bytes:
    """Serialize calibrated XGBoost to JSON bytes.

    Envelope structure:
    - ``booster_b64``: XGBoost booster via ``save_raw("json")`` -> base64
    - Isotonic calibrator thresholds as JSON arrays

    No pickle is used (banned by project security policy).
    """
    classifiers: list[Any] = list(model.calibrated_classifiers_)
    cc = cast(_CalibratedClassifier, classifiers[0])
    estimator = cast(XGBClassifier, cc.estimator)
    calibrator = cast(IsotonicRegression, cc.calibrators[0])

    booster_raw = estimator.get_booster().save_raw("json")

    envelope: dict[str, Any] = {
        "model_type": "xgboost",
        "booster_b64": base64.b64encode(booster_raw).decode("ascii"),
    }
    envelope.update(_serialize_isotonic(calibrator))

    return json.dumps(envelope).encode("utf-8")


def deserialize_xgboost_model(model_bytes: bytes) -> CalibratedClassifierCV:
    """Deserialize JSON bytes back to a calibrated XGBoost model.

    No pickle is used.
    """
    envelope = json.loads(model_bytes.decode("utf-8"))

    # Reconstruct XGBoost estimator
    booster_raw = base64.b64decode(envelope["booster_b64"])
    xgb = XGBClassifier()
    xgb.load_model(bytearray(booster_raw))

    # Reconstruct isotonic calibrator
    calibrator = _deserialize_isotonic(envelope)

    return _wrap_calibrated(xgb, calibrator)


# ---------------------------------------------------------------------------
# Logistic baseline serialization (no pickle)
# ---------------------------------------------------------------------------


def serialize_logistic_model(model: CalibratedClassifierCV) -> bytes:
    """Serialize calibrated logistic regression to JSON bytes.

    Envelope structure:
    - ``coef``, ``intercept``, ``classes``: logistic regression parameters
    - ``feature_names``: baseline feature column names
    - Isotonic calibrator thresholds as JSON arrays

    No pickle is used (banned by project security policy).
    """
    classifiers: list[Any] = list(model.calibrated_classifiers_)
    cc = cast(_CalibratedClassifier, classifiers[0])
    estimator = cast(LogisticRegression, cc.estimator)
    calibrator = cast(IsotonicRegression, cc.calibrators[0])

    envelope: dict[str, Any] = {
        "model_type": "logistic",
        "coef": np.asarray(estimator.coef_).tolist(),
        "intercept": np.asarray(estimator.intercept_).tolist(),
        "classes": np.asarray(estimator.classes_).tolist(),
        "feature_names": list(_BASELINE_FEATURES),
    }
    envelope.update(_serialize_isotonic(calibrator))

    return json.dumps(envelope).encode("utf-8")


def deserialize_logistic_model(model_bytes: bytes) -> CalibratedClassifierCV:
    """Deserialize JSON bytes back to a calibrated logistic regression model.

    No pickle is used.
    """
    envelope = json.loads(model_bytes.decode("utf-8"))

    # Reconstruct logistic regression estimator
    lr = LogisticRegression()
    lr.coef_ = np.array(envelope["coef"])
    lr.intercept_ = np.array(envelope["intercept"])
    lr.classes_ = np.array(envelope["classes"])

    # Reconstruct isotonic calibrator
    calibrator = _deserialize_isotonic(envelope)

    return _wrap_calibrated(lr, calibrator)
