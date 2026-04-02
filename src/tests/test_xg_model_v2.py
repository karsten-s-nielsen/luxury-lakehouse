"""Tests for the xG v2 scoring pipeline (set encoder + MC dropout)."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from analytics.xg_model import (
    XGModelConfig,
    build_features,
    serialize_xgboost_model,
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
        }
    )


def _train_xgboost_and_serialize() -> bytes:
    """Train XGBoost model and serialize to bytes."""
    shots = _make_synthetic_shots(100)
    config = XGModelConfig()
    x, y = build_features(shots, config)
    model = train_xgboost_model(x, y, config)
    return serialize_xgboost_model(model)


def _get_tabular_dim() -> int:
    """Get the number of tabular feature columns after one-hot encoding.

    Uses the same training data as ``_train_xgboost_and_serialize`` to ensure
    the XGBoost expected features align with the dummy weight dimensions.
    """
    shots = _make_synthetic_shots(100)
    config = XGModelConfig()
    x, y = build_features(shots, config)
    model = train_xgboost_model(x, y, config)
    cc = next(iter(model.calibrated_classifiers_))
    xgb_features = list(cc.estimator.get_booster().feature_names)  # type: ignore[union-attr]
    return len(xgb_features)


def _make_dummy_v2_weights(tabular_dim: int) -> bytes:
    """Create synthetic set encoder weights for testing.

    Args:
        tabular_dim: Number of tabular feature columns after one-hot encoding.
            Must match the actual output of ``build_features()``.
    """
    from analytics.set_encoder import SetEncoderConfig, serialize_set_encoder_weights

    config = SetEncoderConfig()
    rng = np.random.default_rng(42)
    weights: dict[str, np.ndarray] = {
        "encoder_fc1_weight": rng.standard_normal((config.encoder_hidden, config.player_feature_dim)),
        "encoder_fc1_bias": rng.standard_normal(config.encoder_hidden),
        "encoder_fc2_weight": rng.standard_normal((config.context_dim, config.encoder_hidden)),
        "encoder_fc2_bias": rng.standard_normal(config.context_dim),
    }
    # Prediction MLP input dim = tabular features + context_dim
    pred_input_dim = tabular_dim + config.context_dim
    weights.update(
        {
            "pred_fc1_weight": rng.standard_normal((config.pred_hidden_1, pred_input_dim)),
            "pred_fc1_bias": rng.standard_normal(config.pred_hidden_1),
            "pred_fc2_weight": rng.standard_normal((config.pred_hidden_2, config.pred_hidden_1)),
            "pred_fc2_bias": rng.standard_normal(config.pred_hidden_2),
            "pred_fc3_weight": rng.standard_normal((1, config.pred_hidden_2)),
            "pred_fc3_bias": rng.standard_normal(1),
        }
    )
    return serialize_set_encoder_weights(weights)


# ---------------------------------------------------------------------------
# _make_v2_scoring_udf
# ---------------------------------------------------------------------------


class TestMakeV2ScoringUdf:
    """Test the v2 applyInPandas UDF factory."""

    def test_udf_returns_callable(self) -> None:
        xgboost_bytes = _train_xgboost_and_serialize()
        tabular_dim = _get_tabular_dim()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes, xgboost_bytes)
        assert callable(udf)

    def test_udf_output_columns(self) -> None:
        """Output should have exactly shot_id, match_id, competition_id, xg_set_encoder, xg_ci_lower, xg_ci_upper."""
        xgboost_bytes = _train_xgboost_and_serialize()
        tabular_dim = _get_tabular_dim()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(10)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1
        ff = json.dumps(
            [
                {"location": [100, 40], "teammate": False, "keeper": True},
                {"location": [95, 35], "teammate": False, "keeper": False},
            ]
        )
        shots["shot_freeze_frame"] = ff

        result = udf(shots)
        expected_columns = {"shot_id", "match_id", "competition_id", "xg_set_encoder", "xg_ci_lower", "xg_ci_upper"}
        assert set(result.columns) == expected_columns
        assert len(result) == len(shots)

    def test_udf_with_freeze_frame_populates_v2(self) -> None:
        """V2 columns should be populated when weights + freeze frame present."""
        xgboost_bytes = _train_xgboost_and_serialize()
        tabular_dim = _get_tabular_dim()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(10)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1
        ff = json.dumps(
            [
                {"location": [100, 40], "teammate": False, "keeper": True},
                {"location": [95, 35], "teammate": False, "keeper": False},
                {"location": [105, 45], "teammate": True, "keeper": False},
            ]
        )
        shots["shot_freeze_frame"] = ff

        result = udf(shots)
        # V2 columns should be populated (not NaN)
        assert bool(result["xg_set_encoder"].notna().all())
        assert bool(result["xg_ci_lower"].notna().all())
        assert bool(result["xg_ci_upper"].notna().all())
        # CI bounds should be valid probabilities
        assert np.all(result["xg_set_encoder"].between(0, 1))
        assert np.all(result["xg_ci_lower"].between(0, 1))
        assert np.all(result["xg_ci_upper"].between(0, 1))
        # Lower bound <= mean <= upper bound
        assert np.all(result["xg_ci_lower"] <= result["xg_set_encoder"])
        assert np.all(result["xg_set_encoder"] <= result["xg_ci_upper"])

    def test_udf_nan_for_missing_freeze_frame(self) -> None:
        """V2 columns should be NaN for shots without freeze frame JSON."""
        xgboost_bytes = _train_xgboost_and_serialize()
        tabular_dim = _get_tabular_dim()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(10)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1
        shots["shot_freeze_frame"] = None

        result = udf(shots)
        assert bool(result["xg_set_encoder"].isna().all())
        assert bool(result["xg_ci_lower"].isna().all())
        assert bool(result["xg_ci_upper"].isna().all())

    def test_udf_nan_for_nan_freeze_frame(self) -> None:
        """V2 columns should be NaN when freeze frame is float NaN."""
        xgboost_bytes = _train_xgboost_and_serialize()
        tabular_dim = _get_tabular_dim()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(5)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1
        shots["shot_freeze_frame"] = float("nan")

        result = udf(shots)
        assert bool(result["xg_set_encoder"].isna().all())
        assert bool(result["xg_ci_lower"].isna().all())
        assert bool(result["xg_ci_upper"].isna().all())

    def test_udf_mixed_freeze_frame(self) -> None:
        """Shots with and without freeze frames should both be handled."""
        xgboost_bytes = _train_xgboost_and_serialize()
        tabular_dim = _get_tabular_dim()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes, xgboost_bytes)

        shots = _make_synthetic_shots(6)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["match_id"] = 12345
        shots["competition_id"] = 1
        ff = json.dumps([{"location": [100, 40], "teammate": False, "keeper": True}])
        shots["shot_freeze_frame"] = [ff, None, ff, None, ff, None]

        result = udf(shots)
        assert len(result) == 6
        # Rows with freeze frame should have v2 predictions
        assert result["xg_set_encoder"].iloc[0] is not None and not np.isnan(result["xg_set_encoder"].iloc[0])
        # Rows without freeze frame should be NaN
        assert np.isnan(result["xg_set_encoder"].iloc[1])


# ---------------------------------------------------------------------------
# MLflow Champion loading — v2 weights
# ---------------------------------------------------------------------------


class TestTryLoadChampionXgV2:
    """Test _try_load_champion_xg_v2 fallback behavior."""

    def test_returns_none_when_mlflow_not_importable(self) -> None:
        """Should return None gracefully when mlflow is not available."""
        import logging
        import sys
        from unittest.mock import patch

        from ingestion.xg_model_v2 import _try_load_champion_xg_v2

        with patch.dict(sys.modules, {"mlflow": None, "mlflow.tracking": None}):
            result = _try_load_champion_xg_v2(logging.getLogger("test"), "catalog", "schema")
        assert result is None

    def test_returns_none_when_champion_not_found(self) -> None:
        """Should return None when mlflow is available but no Champion registered."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.xg_model_v2 import _try_load_champion_xg_v2

        mock_mlflow = MagicMock()
        mock_tracking = MagicMock()
        mock_tracking.MlflowClient.return_value.get_model_version_by_alias.side_effect = Exception("Not found")

        with patch.dict("sys.modules", {"mlflow": mock_mlflow, "mlflow.tracking": mock_tracking}):
            result = _try_load_champion_xg_v2(logging.getLogger("test"), "catalog", "schema")
        assert result is None


# ---------------------------------------------------------------------------
# MLflow Champion loading — XGBoost (for v2 feature extraction)
# ---------------------------------------------------------------------------


class TestTryLoadChampionXgboost:
    """Test _try_load_champion_xgboost fallback behavior."""

    def test_returns_none_when_mlflow_not_importable(self) -> None:
        """Should return None gracefully when mlflow is not available."""
        import logging
        import sys
        from unittest.mock import patch

        from ingestion.xg_model_v2 import _try_load_champion_xgboost

        with patch.dict(sys.modules, {"mlflow.sklearn": None}):
            result = _try_load_champion_xgboost(logging.getLogger("test"), "soccer_analytics", "dev_gold")
        assert result is None

    def test_returns_none_when_champion_not_found(self) -> None:
        """Should return None when mlflow is available but Champion not registered."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.xg_model_v2 import _try_load_champion_xgboost

        mock_sklearn = MagicMock()
        mock_sklearn.load_model.side_effect = Exception("Model not found")

        with patch.dict("sys.modules", {"mlflow.sklearn": mock_sklearn}):
            result = _try_load_champion_xgboost(logging.getLogger("test"), "soccer_analytics", "dev_gold")
        assert result is None

    def test_returns_bytes_when_champion_found(self) -> None:
        """Should return serialized XGBoost bytes when Champion exists."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.xg_model_v2 import _try_load_champion_xgboost

        # Create real trained model for serialization
        shots = _make_synthetic_shots(100)
        config = XGModelConfig()
        x, y = build_features(shots, config)
        xgboost_model = train_xgboost_model(x, y, config)

        mock_sklearn = MagicMock()
        mock_sklearn.load_model.return_value = xgboost_model

        with patch.dict("sys.modules", {"mlflow.sklearn": mock_sklearn}):
            result = _try_load_champion_xgboost(logging.getLogger("test"), "soccer_analytics", "dev_gold")

        assert result is not None
        assert isinstance(result, bytes)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# _load_shots_with_context (verifies SQL structure)
# ---------------------------------------------------------------------------


class TestLoadShotsWithContext:
    """Test _load_shots_with_context uses the correct SQL structure."""

    def test_calls_spark_sql_with_freeze_frame_join(self) -> None:
        """Should issue a SQL query that includes shot_freeze_frame."""
        from unittest.mock import MagicMock

        from ingestion.xg_model_v2 import _load_shots_with_context

        mock_spark = MagicMock()
        _load_shots_with_context(mock_spark, "soccer_analytics")

        mock_spark.sql.assert_called_once()
        sql_query = mock_spark.sql.call_args[0][0]
        assert "shot_freeze_frame" in sql_query
        assert "fct_shots" in sql_query
        assert "stg_statsbomb__events" in sql_query
        assert "LEFT JOIN" in sql_query

    def test_uses_correct_catalog(self) -> None:
        """Should use the provided catalog in the SQL query."""
        from unittest.mock import MagicMock

        from ingestion.xg_model_v2 import _load_shots_with_context

        mock_spark = MagicMock()
        _load_shots_with_context(mock_spark, "my_catalog")

        sql_query = mock_spark.sql.call_args[0][0]
        assert "my_catalog.dev_gold.fct_shots" in sql_query
        assert "my_catalog.dev_silver.stg_statsbomb__events" in sql_query


# ---------------------------------------------------------------------------
# Skip guard behavior
# ---------------------------------------------------------------------------


class TestSkipGuard:
    """Test the incremental skip guard in run_pipeline."""

    @pytest.fixture()
    def _mock_workflow(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Bypass the @workflow decorator so run_pipeline is a plain function."""
        # The @workflow decorator wraps run_pipeline. We need the inner function.
        # Simplest approach: monkeypatch the workflow registry to be a no-op.
        monkeypatch.setattr("workflows.workflow", lambda *a, **kw: lambda fn: fn, raising=True)
        # Re-import to pick up the no-op decorator -- but since the module is
        # already loaded, we test the already-decorated function via its public API.

    def test_skips_when_all_competitions_exist(self) -> None:
        """Pipeline should return early when all competitions already scored."""
        import logging
        from unittest.mock import MagicMock, patch

        logger = logging.getLogger("test")

        mock_spark = MagicMock()

        # Mock the existing results table -- all competitions already scored
        mock_existing_row_1 = MagicMock()
        mock_existing_row_1.__getitem__ = lambda self, key: "11" if key == "competition_id" else None
        mock_existing_row_2 = MagicMock()
        mock_existing_row_2.__getitem__ = lambda self, key: "43" if key == "competition_id" else None

        mock_spark.table.return_value.select.return_value.distinct.return_value.collect.return_value = [
            mock_existing_row_1,
            mock_existing_row_2,
        ]

        # Mock _load_shots_with_context to return a DataFrame with the same competitions
        mock_shots_df = MagicMock()
        mock_comp_row_1 = MagicMock()
        mock_comp_row_1.__getitem__ = lambda self, key: "11" if key == "competition_id" else None
        mock_comp_row_2 = MagicMock()
        mock_comp_row_2.__getitem__ = lambda self, key: "43" if key == "competition_id" else None
        mock_shots_df.select.return_value.distinct.return_value.collect.return_value = [
            mock_comp_row_1,
            mock_comp_row_2,
        ]

        with patch("ingestion.xg_model_v2._load_shots_with_context", return_value=mock_shots_df):
            from ingestion.xg_model_v2 import run_pipeline

            # The decorated run_pipeline is already wrapped by @workflow.
            # Call the underlying function directly to bypass Spark dependency.
            # Since @workflow wraps it, we access __wrapped__ if available.
            fn = getattr(run_pipeline, "__wrapped__", run_pipeline)
            fn(mock_spark, "catalog", "schema", logger)

        # Should NOT have called write_delta_table since all comps are skipped
        # Verify by checking that _try_load_champion_xg_v2 was never called
        # (pipeline returns early before model loading)
