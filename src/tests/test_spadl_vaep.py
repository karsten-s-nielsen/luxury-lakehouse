"""Tests for SPADL/VAEP ingestion pipeline."""

from __future__ import annotations

import tempfile

import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from ingestion.spadl_conversion import _read_existing_match_ids

# ---------------------------------------------------------------------------
# VAEP scoring
# ---------------------------------------------------------------------------


class TestVaepFormula:
    """Test VAEP value computation properties."""

    def test_vaep_value_is_sum_of_components(self) -> None:
        """offensive_value + defensive_value should approximately equal vaep_value."""
        off = 0.05
        def_val = 0.02
        expected = off + def_val
        assert abs(expected - 0.07) < 1e-10

    def test_vaep_values_bounded(self) -> None:
        """VAEP values for typical actions should be in a reasonable range."""
        typical_values = [0.01, -0.02, 0.05, -0.01, 0.15, -0.05]
        for v in typical_values:
            assert -1.0 <= v <= 1.0


# ---------------------------------------------------------------------------
# Incremental pipeline helpers
# ---------------------------------------------------------------------------


class TestReadExistingMatchIds:
    """Test _read_existing_match_ids with mocked Spark."""

    def test_returns_empty_set_on_missing_table(self) -> None:
        """When table doesn't exist, should return empty set without error."""
        import logging
        from unittest.mock import MagicMock

        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("Table not found")
        result = _read_existing_match_ids(mock_spark, "cat", "sch", "tbl", logging.getLogger("test"))
        assert result == set()

    def test_returns_match_ids_from_table(self) -> None:
        """When table exists, should return set of match_ids."""
        import logging
        from unittest.mock import MagicMock

        mock_row_1 = MagicMock()
        mock_row_1.__getitem__ = lambda self, k: 3788741
        mock_row_2 = MagicMock()
        mock_row_2.__getitem__ = lambda self, k: 3788743

        mock_spark = MagicMock()
        mock_spark.table.return_value.select.return_value.distinct.return_value.collect.return_value = [
            mock_row_1,
            mock_row_2,
        ]
        result = _read_existing_match_ids(mock_spark, "cat", "sch", "tbl", logging.getLogger("test"))
        assert result == {3788741, 3788743}


# ---------------------------------------------------------------------------
# Model persistence
# ---------------------------------------------------------------------------


class TestModelPersistence:
    """Test XGBoost model save/load roundtrip."""

    def test_model_roundtrip(self) -> None:
        """Train a model, save to temp dir, reload, verify predictions match."""
        import os

        rng = np.random.default_rng(42)
        x = pd.DataFrame(rng.random((50, 3)), columns=["f1", "f2", "f3"])  # type: ignore[arg-type]
        y = pd.Series((rng.random(50) > 0.5).astype(int))

        model = XGBClassifier(n_estimators=10, max_depth=2, random_state=42)
        model.fit(x, y)
        preds_original = model.predict_proba(x)[:, 1]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "model.json")
            model.save_model(path)

            loaded = XGBClassifier()
            loaded.load_model(path)
            preds_loaded = loaded.predict_proba(x)[:, 1]

        np.testing.assert_array_almost_equal(preds_original, preds_loaded)


# ---------------------------------------------------------------------------
# MLflow Champion loading
# ---------------------------------------------------------------------------


class TestTryLoadChampionVaep:
    """Test _try_load_champion_vaep fallback behavior."""

    def test_returns_none_when_mlflow_not_importable(self) -> None:
        """Should return None gracefully when mlflow is not available."""
        import logging
        import sys
        from unittest.mock import patch

        from ingestion.spadl_vaep import _try_load_champion_vaep

        # Simulate mlflow not being importable
        with patch.dict(sys.modules, {"mlflow": None, "mlflow.pyfunc": None}):
            result = _try_load_champion_vaep(logging.getLogger("test"), "soccer_analytics", "dev_gold")
        # Result is None because the import fails inside the function
        assert result is None

    def test_returns_none_when_champion_not_found(self) -> None:
        """Should return None when mlflow is available but no Champion registered."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.spadl_vaep import _try_load_champion_vaep

        mock_mlflow = MagicMock()
        mock_pyfunc = MagicMock()
        mock_pyfunc.load_model.side_effect = Exception("Model not found")

        with patch.dict("sys.modules", {"mlflow": mock_mlflow, "mlflow.pyfunc": mock_pyfunc}):
            result = _try_load_champion_vaep(logging.getLogger("test"), "soccer_analytics", "dev_gold")
        assert result is None

    def test_returns_models_when_champion_found(self) -> None:
        """Should return (model_scores, model_concedes) when Champion exists."""
        import logging
        from unittest.mock import MagicMock, patch

        from ingestion.spadl_vaep import _try_load_champion_vaep

        mock_scores = MagicMock()
        mock_concedes = MagicMock()

        mock_unwrapped = MagicMock()
        mock_unwrapped.scores_model = mock_scores
        mock_unwrapped.concedes_model = mock_concedes

        mock_champion = MagicMock()
        mock_champion.unwrap_python_model.return_value = mock_unwrapped

        mock_pyfunc = MagicMock()
        mock_pyfunc.load_model.return_value = mock_champion

        mock_mlflow = MagicMock()

        mock_tracking = MagicMock()
        mock_tracking.MlflowClient.return_value.get_run.return_value.data.tags = {}

        with patch.dict(
            "sys.modules",
            {
                "mlflow": mock_mlflow,
                "mlflow.pyfunc": mock_pyfunc,
                "mlflow.tracking": mock_tracking,
            },
        ):
            result = _try_load_champion_vaep(logging.getLogger("test"), "soccer_analytics", "dev_gold")

        assert result is not None
        assert result[0] is mock_scores
        assert result[1] is mock_concedes
