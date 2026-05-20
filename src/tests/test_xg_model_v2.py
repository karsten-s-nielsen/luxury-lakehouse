"""Tests for the xG v2 scoring pipeline (set encoder + MC dropout)."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from analytics.xg_model import (
    XGModelConfig,
    build_features,
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


def _get_tabular_dim_and_features() -> tuple[int, list[str]]:
    """Get the tabular feature column count and names after one-hot encoding."""
    shots = _make_synthetic_shots(100)
    config = XGModelConfig()
    x, _ = build_features(shots, config)
    return x.shape[1], list(x.columns)


def _make_dummy_v2_weights(tabular_dim: int, feature_names: list[str] | None = None) -> bytes:
    """Create synthetic set encoder weights for testing.

    Mirrors production envelope shape (post-SK3-MIG): every envelope MUST
    carry ``feature_names`` (per ADR-012 §2 grace-period closure 2026-05-02);
    ``tabular_dim`` is also injected as defense-in-depth (matches what
    ``scripts/train_xg_v2_hf.py`` writes).

    Args:
        tabular_dim: Number of tabular feature columns after one-hot encoding.
            Must match the actual output of ``build_features()``.
        feature_names: Optional explicit feature list. Defaults to
            ``["feat_0", "feat_1", ..., "feat_{tabular_dim-1}"]`` — synthetic
            names that exercise the envelope path without coupling to any
            specific production feature ordering.
    """
    import json

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
    weight_bytes = serialize_set_encoder_weights(weights)
    # Inject feature_names + tabular_dim to mirror production envelope shape.
    envelope = json.loads(weight_bytes.decode("utf-8"))
    envelope["feature_names"] = (
        feature_names if feature_names is not None else [f"feat_{i}" for i in range(tabular_dim)]
    )
    envelope["tabular_dim"] = tabular_dim
    return json.dumps(envelope).encode("utf-8")


# ---------------------------------------------------------------------------
# _make_v2_scoring_udf
# ---------------------------------------------------------------------------


class TestMakeV2ScoringUdf:
    """Test the v2 applyInPandas UDF factory."""

    def test_udf_returns_callable(self) -> None:
        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes)
        assert callable(udf)

    def test_udf_output_columns(self) -> None:
        """Output should have exactly shot_id, competition_id, xg_set_encoder, xg_ci_lower, xg_ci_upper."""
        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes)

        shots = _make_synthetic_shots(10)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["competition_id"] = 1
        ff = json.dumps(
            [
                {"location": [100, 40], "teammate": False, "keeper": True},
                {"location": [95, 35], "teammate": False, "keeper": False},
            ]
        )
        shots["shot_freeze_frame"] = ff

        result = udf(shots)
        expected_columns = {"shot_id", "competition_id", "xg_set_encoder", "xg_ci_lower", "xg_ci_upper"}
        assert set(result.columns) == expected_columns
        assert len(result) == len(shots)

    def test_udf_with_freeze_frame_populates_v2(self) -> None:
        """V2 columns should be populated when weights + freeze frame present."""
        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes)

        shots = _make_synthetic_shots(10)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
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
        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes)

        shots = _make_synthetic_shots(10)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["competition_id"] = 1
        shots["shot_freeze_frame"] = None

        result = udf(shots)
        assert bool(result["xg_set_encoder"].isna().all())
        assert bool(result["xg_ci_lower"].isna().all())
        assert bool(result["xg_ci_upper"].isna().all())

    def test_udf_nan_for_nan_freeze_frame(self) -> None:
        """V2 columns should be NaN when freeze frame is float NaN."""
        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes)

        shots = _make_synthetic_shots(5)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
        shots["competition_id"] = 1
        shots["shot_freeze_frame"] = float("nan")

        result = udf(shots)
        assert bool(result["xg_set_encoder"].isna().all())
        assert bool(result["xg_ci_lower"].isna().all())
        assert bool(result["xg_ci_upper"].isna().all())

    def test_udf_mixed_freeze_frame(self) -> None:
        """Shots with and without freeze frames should both be handled."""
        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes)

        shots = _make_synthetic_shots(6)
        shots["shot_id"] = [f"shot_{i}" for i in range(len(shots))]
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
        """Pipeline raises WorkflowSkippedError when guard says count=0."""
        import logging
        from unittest.mock import MagicMock

        from ingestion.guards import FilterResult
        from workflows.exceptions import WorkflowSkippedError

        logger = logging.getLogger("test")

        mock_spark = MagicMock()

        from ingestion.xg_model_v2 import run_pipeline

        # The decorated run_pipeline is already wrapped by @workflow.
        # Call the underlying function directly to bypass Spark dependency.
        fn = getattr(run_pipeline, "__wrapped__", run_pipeline)

        # Guard returns count=0 — all competitions already scored
        with pytest.raises(WorkflowSkippedError):
            fn(
                mock_spark,
                "catalog",
                "schema",
                logger,
                filter_result=FilterResult(workflow_id="wf-xg-v2", count=0),
            )


# ---------------------------------------------------------------------------
# Regression: MLflow lookups use DEFAULT_GOLD_SCHEMA, not pipeline's write schema
# ---------------------------------------------------------------------------


class TestV2EnvelopeFeatureNames:
    """Regression: v2 weights envelope carries its own ``feature_names``
    field so inference can align tabular input without depending on v1
    XGBoost's feature list.

    Pre-2026-04-22: the inference UDF reindexed tabular features to v1
    XGBoost's feature_names, silently coupling v2's tabular_dim to
    whatever v1 one-hot cardinality happened to be on disk. When v1 got
    retrained with extra categorical levels, the v2 UDF would matmul
    v2-weights against v1-sized tabular input and blow up with
    ``matmul: size X is different from Y``. This test locks in the
    replacement contract: v2 carries its own feature list; v1 becomes a
    legacy fallback only.
    """

    def test_udf_uses_envelope_feature_names_when_present(self) -> None:
        """When v2 envelope has feature_names, UDF caches + uses them."""
        import json

        tabular_dim, _ = _get_tabular_dim_and_features()
        v2_bytes = _make_dummy_v2_weights(tabular_dim)

        # Inject feature_names into the envelope. Use a marker list we can
        # distinguish from xgb_features: same length (so the weights still
        # matmul) but a reordered/relabeled set. The behavior under test is
        # that the UDF RESPECTS this field — not that the model math works
        # when feature lists genuinely differ in length.
        envelope = json.loads(v2_bytes.decode("utf-8"))
        envelope["feature_names"] = [f"synthetic_feat_{i}" for i in range(tabular_dim)]
        v2_bytes_with_features = json.dumps(envelope).encode("utf-8")

        from ingestion.xg_model_v2 import _make_v2_scoring_udf

        udf = _make_v2_scoring_udf(v2_bytes_with_features)

        # Call the UDF once to trigger cache population. Use no freeze frames
        # so we exercise the cache path but don't depend on the matmul
        # succeeding (the dummy weights were built for xgb_features shape).
        shots = _make_synthetic_shots(3)
        shots["shot_id"] = [f"s_{i}" for i in range(3)]
        shots["competition_id"] = 1
        shots["shot_freeze_frame"] = None
        udf(shots)

        cached_v2_features = udf._model_cache["v2_features"]  # type: ignore[attr-defined]
        assert cached_v2_features == envelope["feature_names"], (
            f"UDF cached v2_features={cached_v2_features!r}; expected envelope's injected list"
        )
        # SK3-MIG (2026-05-02): xgb_features is no longer cached as the legacy
        # fallback (ADR-012 §2 grace-period closure). v2_features comes from
        # the envelope only.
        assert "xgb_features" not in udf._model_cache, (  # type: ignore[attr-defined]
            "xgb_features cache slot was retired in SK3-MIG (ADR-012 §2 closure) — "
            "v2 envelopes must carry their own feature_names."
        )

    def test_parse_v2_envelope_features_raises_on_missing_feature_names(self) -> None:
        """ADR-012 §2 grace-period closure (SK3-MIG, 2026-05-02).

        v2 envelopes that lack ``feature_names`` must raise RuntimeError with a
        clear pointer to the retraining script. The pre-2026-04-22 fallback
        to v1's XGBoost ``xgb_features`` was removed; envelopes must carry their
        own feature list. ADR-012 §2 grace-period (one release window) has
        long expired.
        """
        import json

        legacy_envelope_bytes = json.dumps(
            {
                "tabular_dim": 41,
                # Note: no "feature_names" key — this is the legacy shape.
            }
        ).encode("utf-8")

        from ingestion.xg_model_v2 import _parse_v2_envelope_features

        with pytest.raises(RuntimeError, match="missing 'feature_names'"):
            _parse_v2_envelope_features(legacy_envelope_bytes)

    def test_parse_v2_envelope_features_raises_on_inconsistent_dim(self) -> None:
        """Envelope with feature_names but wrong tabular_dim signals corruption."""
        import json

        bad_envelope_bytes = json.dumps(
            {
                "tabular_dim": 41,
                "feature_names": ["a", "b", "c"],  # length 3, not 41
            }
        ).encode("utf-8")

        from ingestion.xg_model_v2 import _parse_v2_envelope_features

        with pytest.raises(AssertionError, match="inconsistent"):
            _parse_v2_envelope_features(bad_envelope_bytes)

    def test_parse_v2_envelope_features_returns_list_and_dim(self) -> None:
        """Happy path: envelope with feature_names + matching tabular_dim returns both."""
        import json

        good_envelope_bytes = json.dumps(
            {
                "tabular_dim": 3,
                "feature_names": ["feat_a", "feat_b", "feat_c"],
            }
        ).encode("utf-8")

        from ingestion.xg_model_v2 import _parse_v2_envelope_features

        features, dim = _parse_v2_envelope_features(good_envelope_bytes)
        assert features == ["feat_a", "feat_b", "feat_c"]
        assert dim == 3


class TestMlflowLookupsUseGoldSchema:
    """Regression: both v1 XGBoost and v2 set-encoder MLflow @Champion lookups
    must resolve against DEFAULT_GOLD_SCHEMA (where model registry lives),
    NOT the pipeline's ``schema`` arg (which is "bronze" for the output table).

    Before 2026-04-22 the v2 call passed ``schema`` (="bronze"), so every
    MLflow lookup hit ``{catalog}.bronze.xg_model_v2`` — a path that never
    exists — forcing a silent fallback to UC Volume every time. This test
    locks in the post-fix contract at the source-inspection level so a
    future refactor can't silently regress.
    """

    def test_v2_champion_lookup_uses_default_gold_schema(self) -> None:
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1] / "ingestion" / "xg_model_v2.py").read_text(encoding="utf-8")
        assert "_try_load_champion_xg_v2(logger, catalog, DEFAULT_GOLD_SCHEMA)" in source, (
            "xg_model_v2.run_pipeline must call _try_load_champion_xg_v2 with "
            "DEFAULT_GOLD_SCHEMA (where the UC model registry lives), not the "
            "pipeline's `schema` arg (which is 'bronze' for xg_predictions_v2). "
            "See src/ingestion/xg_model_v2.py:319 comment."
        )
        assert "_try_load_champion_xg_v2(logger, catalog, schema)" not in source, (
            "Found the pre-2026-04-22 bug pattern `_try_load_champion_xg_v2"
            "(logger, catalog, schema)`. Use DEFAULT_GOLD_SCHEMA instead."
        )

    def test_v1_xgboost_removed(self) -> None:
        """v1 XGBoost dependency was removed — _try_load_champion_xgboost must not exist."""
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1] / "ingestion" / "xg_model_v2.py").read_text(encoding="utf-8")
        assert "_try_load_champion_xgboost" not in source, (
            "v1 XGBoost loading was retired — _try_load_champion_xgboost must not appear in xg_model_v2.py."
        )
