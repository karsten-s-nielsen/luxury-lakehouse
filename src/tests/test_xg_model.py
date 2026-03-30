"""Tests for the custom xG model analytics module."""

from __future__ import annotations

import json

import numpy as np
import numpy.testing as npt
import pandas as pd
import pytest
from sklearn.metrics import brier_score_loss

from analytics.xg_model import (
    _BASELINE_FEATURES,
    _BOOLEAN_FEATURES,
    _CATEGORICAL_FEATURES,
    _NUMERIC_FEATURES,
    XGModelConfig,
    build_features,
    deserialize_logistic_model,
    deserialize_xgboost_model,
    evaluate_model,
    parse_freeze_frame,
    serialize_logistic_model,
    serialize_xgboost_model,
    train_logistic_baseline,
    train_xgboost_model,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_synthetic_shots(n: int = 500, *, random_state: int = 42) -> pd.DataFrame:
    """Create realistic synthetic shot data for testing."""
    rng = np.random.default_rng(random_state)

    body_parts = ["Right Foot", "Left Foot", "Head"]
    techniques = ["Normal", "Volley", "Half Volley", "Overhead Kick"]
    shot_types = ["Open Play", "Free Kick", "Penalty", "Corner"]
    play_patterns: list[str | None] = ["Regular Play", "From Corner", "From Free Kick", "From Goal Kick", None]

    distance = rng.uniform(5, 50, n)
    angle = rng.uniform(0.05, 1.5, n)

    # xG is correlated with distance and angle
    base_prob = 0.3 - 0.005 * distance + 0.1 * angle
    base_prob = np.clip(base_prob, 0.02, 0.95)
    is_goal = rng.binomial(1, base_prob)

    return pd.DataFrame(
        {
            "distance_to_goal": distance,
            "shot_angle": angle,
            "location_x": rng.uniform(90, 120, n),
            "location_y": rng.uniform(10, 70, n),
            "end_location_x": rng.uniform(118, 121, n),
            "end_location_y": rng.uniform(30, 50, n),
            "period": rng.choice([1, 2], n),
            "minute": rng.integers(0, 90, n),
            "is_first_time": rng.choice(np.array([True, False, None], dtype=object), n),
            "shot_body_part": rng.choice(body_parts, n),
            "shot_technique": rng.choice(techniques, n),
            "shot_type": rng.choice(shot_types, n),
            "play_pattern": rng.choice(np.array(play_patterns, dtype=object), n),
            "is_goal": is_goal,
            "statsbomb_xg": np.clip(base_prob + rng.normal(0, 0.02, n), 0.01, 0.99),
        }
    )


# ---------------------------------------------------------------------------
# XGModelConfig
# ---------------------------------------------------------------------------


class TestXGModelConfig:
    def test_frozen_dataclass(self) -> None:
        config = XGModelConfig()
        with pytest.raises(AttributeError):
            config.target = "something"  # type: ignore[misc]

    def test_default_features(self) -> None:
        config = XGModelConfig()
        expected_count = len(_NUMERIC_FEATURES) + len(_BOOLEAN_FEATURES) + len(_CATEGORICAL_FEATURES)
        assert len(config.features) == expected_count
        assert expected_count == 13  # 8 + 1 + 4
        assert config.target == "is_goal"

    def test_default_hyperparameters(self) -> None:
        config = XGModelConfig()
        assert config.n_estimators == 100
        assert config.max_depth == 3
        assert config.learning_rate == 0.1
        assert config.calibration_method == "isotonic"
        assert config.test_size == 0.2
        assert config.random_state == 42

    def test_custom_config(self) -> None:
        config = XGModelConfig(n_estimators=50, max_depth=5)
        assert config.n_estimators == 50
        assert config.max_depth == 5


# ---------------------------------------------------------------------------
# build_features
# ---------------------------------------------------------------------------


class TestBuildFeatures:
    def test_output_shape(self) -> None:
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        assert len(x) == 100
        assert len(y) == 100

    def test_handles_null_play_pattern(self) -> None:
        shots = _make_synthetic_shots(50)
        shots["play_pattern"] = None
        config = XGModelConfig()
        x, _y = build_features(shots, config)
        # Should not have any NaN after filling with "Unknown"
        assert int(x.isna().sum().sum()) == 0

    def test_handles_all_null_categoricals(self) -> None:
        shots = _make_synthetic_shots(50)
        for cat in _CATEGORICAL_FEATURES:
            shots[cat] = None
        config = XGModelConfig()
        x, _y = build_features(shots, config)
        assert int(x.isna().sum().sum()) == 0
        # Should have one-hot columns for "Unknown"
        unknown_cols = [c for c in x.columns if c.endswith("_Unknown")]
        assert len(unknown_cols) == len(_CATEGORICAL_FEATURES)

    def test_one_hot_encoding(self) -> None:
        shots = _make_synthetic_shots(50)
        config = XGModelConfig()
        x, _y = build_features(shots, config)
        # Should have one-hot encoded columns for categoricals
        assert any(c.startswith("shot_body_part_") for c in x.columns)
        assert any(c.startswith("shot_technique_") for c in x.columns)
        assert any(c.startswith("shot_type_") for c in x.columns)
        assert any(c.startswith("play_pattern_") for c in x.columns)

    def test_boolean_features_are_float(self) -> None:
        shots = _make_synthetic_shots(50)
        config = XGModelConfig()
        x, _y = build_features(shots, config)
        assert x["is_first_time"].dtype == float

    def test_numeric_features_present(self) -> None:
        shots = _make_synthetic_shots(50)
        config = XGModelConfig()
        x, _y = build_features(shots, config)
        for col in _NUMERIC_FEATURES:
            assert col in x.columns

    def test_target_missing_returns_zeros(self) -> None:
        shots = _make_synthetic_shots(20)
        shots = shots.drop(columns=["is_goal"])
        config = XGModelConfig()
        _x, y = build_features(shots, config)
        assert (y == 0).all()

    def test_all_features_float_dtype(self) -> None:
        shots = _make_synthetic_shots(50)
        config = XGModelConfig()
        x, _y = build_features(shots, config)
        for col in x.columns:
            assert x[col].dtype == float, f"Column {col} has dtype {x[col].dtype}, expected float"


# ---------------------------------------------------------------------------
# train_logistic_baseline
# ---------------------------------------------------------------------------


class TestTrainLogistic:
    def test_returns_calibrated_model(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)
        assert hasattr(model, "predict_proba")
        assert hasattr(model, "calibrated_classifiers_")

    def test_predictions_in_range(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)
        baseline_cols = [c for c in _BASELINE_FEATURES if c in x.columns]
        proba = model.predict_proba(x[baseline_cols])[:, 1]
        assert np.all(proba >= 0) and np.all(proba <= 1)

    def test_uses_only_baseline_features(self) -> None:
        """The logistic baseline should only use distance_to_goal + shot_angle."""
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)

        # The underlying estimator should have 2 coefficients
        cc = next(iter(model.calibrated_classifiers_))
        lr = cc.estimator  # type: ignore[union-attr]
        assert lr.coef_.shape[1] == len(_BASELINE_FEATURES)


# ---------------------------------------------------------------------------
# train_xgboost_model
# ---------------------------------------------------------------------------


class TestTrainXGBoost:
    def test_returns_calibrated_model(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        assert hasattr(model, "predict_proba")
        assert hasattr(model, "calibrated_classifiers_")

    def test_predictions_in_range(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        proba = model.predict_proba(x)[:, 1]
        assert np.all(proba >= 0) and np.all(proba <= 1)

    def test_custom_config(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig(n_estimators=10, max_depth=2)
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        proba = model.predict_proba(x)[:, 1]
        assert len(proba) == 200

    def test_none_config_uses_defaults(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, None)
        assert hasattr(model, "predict_proba")

    def test_single_calibrated_classifier(self) -> None:
        """cv='prefit' should produce exactly one calibrated classifier."""
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        assert len(list(model.calibrated_classifiers_)) == 1


# ---------------------------------------------------------------------------
# evaluate_model
# ---------------------------------------------------------------------------


class TestEvaluateModel:
    def test_returns_all_metrics(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        metrics = evaluate_model(model, x, y)
        assert "brier_score" in metrics
        assert "log_loss" in metrics
        assert "roc_auc" in metrics
        assert "calibration_error" in metrics

    def test_brier_score_reasonable(self) -> None:
        shots = _make_synthetic_shots(500)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        metrics = evaluate_model(model, x, y)
        # On training data, Brier score should be well below 0.25 (random)
        assert metrics["brier_score"] < 0.25

    def test_roc_auc_above_random(self) -> None:
        shots = _make_synthetic_shots(500)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        metrics = evaluate_model(model, x, y)
        assert metrics["roc_auc"] > 0.5

    def test_all_metrics_are_float(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        metrics = evaluate_model(model, x, y)
        for key, value in metrics.items():
            assert isinstance(value, float), f"{key} is {type(value)}, expected float"

    def test_logistic_baseline_metrics(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)
        baseline_cols = [c for c in _BASELINE_FEATURES if c in x.columns]
        metrics = evaluate_model(model, pd.DataFrame(x[baseline_cols]), y)
        assert metrics["roc_auc"] > 0.5


# ---------------------------------------------------------------------------
# XGBoost serialization
# ---------------------------------------------------------------------------


class TestSerializeDeserializeXGBoost:
    def test_roundtrip(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)

        model_bytes = serialize_xgboost_model(model)
        restored = deserialize_xgboost_model(model_bytes)

        original_proba = model.predict_proba(x)[:, 1]
        restored_proba = restored.predict_proba(x)[:, 1]
        np.testing.assert_allclose(original_proba, restored_proba, atol=1e-6)

    def test_bytes_are_json(self) -> None:
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        model_bytes = serialize_xgboost_model(model)
        # Should be valid JSON
        parsed = json.loads(model_bytes.decode("utf-8"))
        assert "booster_b64" in parsed
        assert "model_type" in parsed
        assert parsed["model_type"] == "xgboost"

    def test_no_pickle_in_bytes(self) -> None:
        """Serialized bytes must not contain pickle opcodes."""
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        model_bytes = serialize_xgboost_model(model)
        # Pickle protocol magic bytes: \x80\x05 (protocol 5), or "cos\n" (protocol 0)
        assert b"\x80\x05" not in model_bytes
        # Valid JSON should start with '{'
        assert model_bytes[:1] == b"{"

    def test_envelope_contains_calibrator_data(self) -> None:
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)
        model_bytes = serialize_xgboost_model(model)
        parsed = json.loads(model_bytes.decode("utf-8"))
        assert "X_thresholds" in parsed
        assert "y_thresholds" in parsed
        assert "X_min" in parsed
        assert "X_max" in parsed
        assert "increasing" in parsed

    def test_deserialized_model_can_predict(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)

        model_bytes = serialize_xgboost_model(model)
        restored = deserialize_xgboost_model(model_bytes)

        proba = restored.predict_proba(x)[:, 1]
        assert len(proba) == 200
        assert np.all(proba >= 0) and np.all(proba <= 1)


# ---------------------------------------------------------------------------
# Logistic serialization
# ---------------------------------------------------------------------------


class TestSerializeDeserializeLogistic:
    def test_roundtrip(self) -> None:
        shots = _make_synthetic_shots(200)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)

        model_bytes = serialize_logistic_model(model)
        restored = deserialize_logistic_model(model_bytes)

        baseline_cols = [c for c in _BASELINE_FEATURES if c in x.columns]
        original_proba = model.predict_proba(x[baseline_cols])[:, 1]
        restored_proba = restored.predict_proba(x[baseline_cols])[:, 1]
        np.testing.assert_allclose(original_proba, restored_proba, atol=1e-6)

    def test_bytes_are_json(self) -> None:
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)
        model_bytes = serialize_logistic_model(model)
        parsed = json.loads(model_bytes.decode("utf-8"))
        assert parsed["model_type"] == "logistic"
        assert "coef" in parsed
        assert "intercept" in parsed
        assert "feature_names" in parsed

    def test_no_pickle_in_bytes(self) -> None:
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_logistic_baseline(x, y)
        model_bytes = serialize_logistic_model(model)
        assert b"\x80\x05" not in model_bytes
        assert model_bytes[:1] == b"{"


# ---------------------------------------------------------------------------
# Benchmark vs StatsBomb
# ---------------------------------------------------------------------------


class TestBenchmarkVsStatsBomb:
    def test_custom_xg_within_10pct_of_statsbomb(self) -> None:
        """Custom xG Brier score should be within 10% of synthetic StatsBomb xG."""
        shots = _make_synthetic_shots(500)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        model = train_xgboost_model(x, y, config)

        custom_proba = model.predict_proba(x)[:, 1]
        custom_brier = brier_score_loss(y, custom_proba)

        statsbomb_xg = shots["statsbomb_xg"].clip(0.01, 0.99)
        sb_brier = brier_score_loss(y, statsbomb_xg)

        # Custom xG Brier score should be within 10% of StatsBomb xG
        assert custom_brier <= sb_brier * 1.10, (
            f"Custom Brier {custom_brier:.4f} > 110% of StatsBomb Brier {sb_brier:.4f}"
        )

    def test_xgboost_beats_logistic_baseline(self) -> None:
        """XGBoost model should outperform the logistic baseline (lower Brier)."""
        shots = _make_synthetic_shots(500)
        config = XGModelConfig()
        x, y = build_features(shots, config)

        logistic = train_logistic_baseline(x, y)
        xgboost = train_xgboost_model(x, y, config)

        baseline_cols = [c for c in _BASELINE_FEATURES if c in x.columns]
        logistic_brier = brier_score_loss(y, logistic.predict_proba(x[baseline_cols])[:, 1])
        xgboost_brier = brier_score_loss(y, xgboost.predict_proba(x)[:, 1])

        assert xgboost_brier <= logistic_brier, (
            f"XGBoost Brier {xgboost_brier:.4f} > Logistic Brier {logistic_brier:.4f}"
        )


# ---------------------------------------------------------------------------
# parse_freeze_frame
# ---------------------------------------------------------------------------


class TestParseFreezeFrame:
    """Test the freeze-frame JSON parsing for v2 set encoder input."""

    def test_valid_json(self) -> None:
        ff_json = json.dumps(
            [
                {"location": [60, 40], "teammate": True, "keeper": False, "actor": False},
                {"location": [90, 20], "teammate": False, "keeper": True, "actor": False},
            ]
        )
        result = parse_freeze_frame(ff_json)
        assert result.shape == (2, 4)
        npt.assert_allclose(result[0, :2], [60 / 120, 40 / 80])
        assert result[1, 2] == 1.0  # is_keeper

    def test_none_returns_empty(self) -> None:
        result = parse_freeze_frame(None)
        assert result.shape == (0, 4)

    def test_invalid_json_returns_empty(self) -> None:
        result = parse_freeze_frame("{bad json")
        assert result.shape == (0, 4)

    def test_empty_array(self) -> None:
        result = parse_freeze_frame("[]")
        assert result.shape == (0, 4)

    def test_missing_location_defaults_to_zero(self) -> None:
        ff_json = json.dumps([{"teammate": True, "keeper": False}])
        result = parse_freeze_frame(ff_json)
        assert result.shape == (1, 4)
        npt.assert_allclose(result[0, :2], [0.0, 0.0])

    def test_missing_boolean_fields_default_to_false(self) -> None:
        ff_json = json.dumps([{"location": [60, 40]}])
        result = parse_freeze_frame(ff_json)
        assert result.shape == (1, 4)
        assert result[0, 2] == 0.0  # is_keeper
        assert result[0, 3] == 0.0  # is_teammate

    def test_dtype_is_float64(self) -> None:
        ff_json = json.dumps([{"location": [60, 40], "teammate": True, "keeper": False}])
        result = parse_freeze_frame(ff_json)
        assert result.dtype == np.float64

    def test_multiple_players(self) -> None:
        players = [{"location": [i * 10, i * 5], "teammate": i % 2 == 0, "keeper": i == 0} for i in range(11)]
        result = parse_freeze_frame(json.dumps(players))
        assert result.shape == (11, 4)


# ---------------------------------------------------------------------------
# Pipeline tests (ingestion module)
# ---------------------------------------------------------------------------


class TestMakeScoringUdf:
    """Test the applyInPandas UDF factory."""

    def _train_and_serialize(self) -> tuple[bytes, bytes]:
        """Train both models and serialize to bytes for UDF tests."""
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        logistic = train_logistic_baseline(x, y)
        xgboost_model = train_xgboost_model(x, y, config)
        return serialize_logistic_model(logistic), serialize_xgboost_model(xgboost_model)

    def test_udf_returns_callable(self) -> None:
        logistic_bytes, xgboost_bytes = self._train_and_serialize()

        from ingestion.xg_model import _make_scoring_udf

        udf = _make_scoring_udf(logistic_bytes, xgboost_bytes)
        assert callable(udf)

    def test_udf_output_schema(self) -> None:
        logistic_bytes, xgboost_bytes = self._train_and_serialize()

        from ingestion.xg_model import _make_scoring_udf

        udf = _make_scoring_udf(logistic_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(50)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1

        result = udf(shots)
        expected_columns = {"shot_id", "match_id", "competition_id", "xg_logistic", "xg_gradient_boosted"}
        assert set(result.columns) == expected_columns
        assert len(result) == len(shots)

    def test_udf_predictions_in_range(self) -> None:
        logistic_bytes, xgboost_bytes = self._train_and_serialize()

        from ingestion.xg_model import _make_scoring_udf

        udf = _make_scoring_udf(logistic_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(50)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1

        result = udf(shots)
        assert np.all(result["xg_logistic"].between(0, 1))
        assert np.all(result["xg_gradient_boosted"].between(0, 1))


# ---------------------------------------------------------------------------
# MLflow Champion loading
# ---------------------------------------------------------------------------


class TestTryLoadChampionXg:
    """Test _try_load_champion_xg fallback behavior."""

    def test_returns_none_when_mlflow_not_importable(self) -> None:
        """Should return None gracefully when mlflow is not available."""
        import logging
        import sys
        from unittest.mock import patch

        from ingestion.xg_model import _try_load_champion_xg

        with patch.dict(sys.modules, {"mlflow": None, "mlflow.sklearn": None}):
            result = _try_load_champion_xg(logging.getLogger("test"))
        assert result is None

    def test_returns_none_when_champion_not_found(self) -> None:
        """Should return None when mlflow is available but no Champion registered."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.xg_model import _try_load_champion_xg

        mock_mlflow = MagicMock()
        mock_sklearn = MagicMock()
        mock_sklearn.load_model.side_effect = Exception("Model not found")

        with patch.dict("sys.modules", {"mlflow": mock_mlflow, "mlflow.sklearn": mock_sklearn}):
            result = _try_load_champion_xg(logging.getLogger("test"))
        assert result is None

    def test_returns_bytes_when_champion_found(self) -> None:
        """Should return (logistic_bytes, xgboost_bytes) when Champion exists."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.xg_model import _try_load_champion_xg

        # Create real trained models for serialization
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        logistic = train_logistic_baseline(x, y)
        xgboost_model = train_xgboost_model(x, y, config)

        # Mock MLflow modules — importlib.import_module uses sys.modules
        mock_sklearn = MagicMock()
        mock_sklearn.load_model.side_effect = [xgboost_model, logistic]

        mock_alias_info = MagicMock()
        mock_alias_info.run_id = "test_run_id"

        mock_client_instance = MagicMock()
        mock_client_instance.get_model_version_by_alias.return_value = mock_alias_info

        mock_tracking = MagicMock()
        mock_tracking.MlflowClient.return_value = mock_client_instance

        mock_mlflow = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "mlflow": mock_mlflow,
                "mlflow.sklearn": mock_sklearn,
                "mlflow.tracking": mock_tracking,
            },
        ):
            result = _try_load_champion_xg(logging.getLogger("test"))

        assert result is not None
        logistic_bytes, xgboost_bytes = result
        assert isinstance(logistic_bytes, bytes)
        assert isinstance(xgboost_bytes, bytes)
        assert len(logistic_bytes) > 0
        assert len(xgboost_bytes) > 0
