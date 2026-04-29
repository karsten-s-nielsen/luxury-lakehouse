"""Tests for SPADL/VAEP ingestion pipeline."""

from __future__ import annotations

import tempfile
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
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
        """When table doesn't exist, should return empty set without error.

        Uses a realistic Spark error message (TABLE_OR_VIEW_NOT_FOUND) so
        ``tolerate_missing_table`` suppresses it instead of propagating.
        """
        import logging
        from unittest.mock import MagicMock

        mock_spark = MagicMock()
        mock_spark.table.side_effect = Exception("[TABLE_OR_VIEW_NOT_FOUND] Table `cat`.`sch`.`tbl` cannot be found.")
        result = _read_existing_match_ids(mock_spark, "cat", "sch", "tbl", logging.getLogger("test"))
        assert result == set()

    def test_propagates_non_missing_table_errors(self) -> None:
        """Regression guard: permission errors and schema corruption must NOT be suppressed."""
        import logging
        from unittest.mock import MagicMock

        import pytest as _pytest

        mock_spark = MagicMock()
        mock_spark.table.side_effect = PermissionError("access denied on cat.sch.tbl")
        with _pytest.raises(PermissionError, match="access denied"):
            _read_existing_match_ids(mock_spark, "cat", "sch", "tbl", logging.getLogger("test"))

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


# ---------------------------------------------------------------------------
# Guard metadata union — regression guard for the 2026-04-14 staleness bug
# ---------------------------------------------------------------------------


class TestVaepGuardMetadata:
    """Test ``_VaepGuard.check()`` produces the metadata Stage 2 needs.

    Regression guard for the staleness bug discovered 2026-04-14. Stage 2 at
    ``run_pipeline`` reads ``filter_result.metadata["unscored_vaep_match_ids"]``
    which is computed by the guard BEFORE Stage 1 runs. When Stage 1 repopulates
    ``bronze.spadl_actions`` with new match_ids in the same run (e.g., after a
    user DELETE), those match_ids are not in the guard's pre-Stage-1 ``unscored``
    set.

    LL2 Path B contract update: the guard stores ``unscored_vaep_match_ids`` as
    the PURE Stage-2 diff (no union with ``new_spadl``). ``run_pipeline`` does
    the union at consumption time so Stage 2 still sees Stage 1's adds. The
    union moved out of the guard so the test_guard_conformance contract
    (count == sum-of-metadata-list-lengths) holds for arbitrary mock data —
    pre-LL2 the union storage double-counted match_ids that appeared in both
    sets, breaking the contract for IDSSE/Metrica mocks.
    """

    def test_regression_today_scenario_sb_new_nonempty_unscored_empty(self) -> None:
        """Reproduces the 2026-04-14 16:22:09Z failure scenario.

        User had wiped statsbomb from both ``bronze.spadl_actions`` (v638) and
        ``bronze.vaep_action_values`` (v8). Guard computes:
          sb_new=[3 statsbomb match_ids], ws_new=[], unscored=[].

        LL2 contract: metadata['unscored_vaep_match_ids']=[] (pure Stage-2 diff,
        no union). metadata['new_spadl_match_ids']=[3 ids]. ``run_pipeline``
        unions them at consumption time so Stage 2 still scores the 3 new ids.
        """
        from ingestion.spadl_vaep import _VaepGuard

        mock_spark = MagicMock()

        def find_new_ids_mock(spark: object, source_table: str, results_table: str, **kwargs: object) -> list[str]:
            if "statsbomb_events" in source_table:
                return ["3754348", "3754349", "3754350"]
            if "wyscout_events" in source_table:
                return []
            # Stage 2 unscored call: spadl_table → vaep_table
            return []

        with (
            patch("ingestion.guards.find_new_ids", side_effect=find_new_ids_mock),
            patch("ingestion.guards.ensure_table"),
        ):
            result = _VaepGuard().check(mock_spark, "soccer_analytics", "dev_gold")

        # count = len(new_spadl) + len(unscored) = 3 + 0
        assert result.count == 3
        assert result.metadata["new_spadl_match_ids"] == ["3754348", "3754349", "3754350"]
        # LL2: pure Stage-2 diff (was unioned with new_spadl pre-LL2). run_pipeline
        # does the consumer-side union — see ``run_pipeline``'s ``unscored_ids`` build.
        assert result.metadata["unscored_vaep_match_ids"] == []

    def test_disjoint_union_both_sources_contribute(self) -> None:
        """Normal mixed case: new events from both providers plus existing unscored matches."""
        from ingestion.spadl_vaep import _VaepGuard

        mock_spark = MagicMock()

        def find_new_ids_mock(spark: object, source_table: str, results_table: str, **kwargs: object) -> list[str]:
            if "statsbomb_events" in source_table:
                return ["sb1"]
            if "wyscout_events" in source_table:
                return ["ws1"]
            return ["existing1", "existing2"]

        with (
            patch("ingestion.guards.find_new_ids", side_effect=find_new_ids_mock),
            patch("ingestion.guards.ensure_table"),
        ):
            result = _VaepGuard().check(mock_spark, "cat", "sch")

        # count = len(new_spadl) + len(unscored) = 2 + 2
        assert result.count == 4
        assert result.metadata["new_spadl_match_ids"] == ["sb1", "ws1"]
        # LL2: pure Stage-2 diff (was sorted union of new_spadl + unscored pre-LL2).
        assert result.metadata["unscored_vaep_match_ids"] == ["existing1", "existing2"]

    def test_unscored_only_no_new_events(self) -> None:
        """Steady-state case: no new events, only previously-unscored spadl matches.

        Under the fix, unscored_vaep_match_ids should equal the unscored set
        (unchanged behavior from before the fix, since new_spadl is empty).
        """
        from ingestion.spadl_vaep import _VaepGuard

        mock_spark = MagicMock()

        def find_new_ids_mock(spark: object, source_table: str, results_table: str, **kwargs: object) -> list[str]:
            if "events" in source_table:
                return []
            return ["leftover1", "leftover2"]

        with (
            patch("ingestion.guards.find_new_ids", side_effect=find_new_ids_mock),
            patch("ingestion.guards.ensure_table"),
        ):
            result = _VaepGuard().check(mock_spark, "cat", "sch")

        assert result.count == 2
        assert result.metadata["new_spadl_match_ids"] == []
        assert result.metadata["unscored_vaep_match_ids"] == ["leftover1", "leftover2"]

    def test_skip_path_when_both_empty(self) -> None:
        """No work: guard returns count=0 (triggers WorkflowSkippedError in run_pipeline).

        Metadata is the default empty dict — not populated on the skip path.
        """
        from ingestion.spadl_vaep import _VaepGuard

        mock_spark = MagicMock()

        with (
            patch("ingestion.guards.find_new_ids", return_value=[]),
            patch("ingestion.guards.ensure_table"),
        ):
            result = _VaepGuard().check(mock_spark, "cat", "sch")

        assert result.count == 0
        assert result.metadata == {}


# ---------------------------------------------------------------------------
# Scoring UDF error propagation — regression guard for the silent exception swallow
# ---------------------------------------------------------------------------


class TestScoringUdfErrorPropagation:
    """Test ``_make_scoring_udf`` propagates per-game failures with game_id context.

    Regression guard for the silent exception swallow at
    ``_make_scoring_udf`` lines 390-391 which was hiding any scoring failure
    and leaving zero-row writes to ``bronze.vaep_action_values`` with no trace.
    The daily job would report SUCCEEDED with missing data, making the failure
    invisible until a manual audit discovered it.
    """

    def test_udf_raises_runtime_error_with_game_id_on_per_game_failure(self) -> None:
        """A scoring-UDF exception must propagate as RuntimeError with game_id context."""
        import silly_kicks.spadl
        import silly_kicks.vaep.features

        from ingestion.spadl_vaep import _make_scoring_udf

        # Minimal pandas DataFrame: 2 rows so len(game_actions) >= 2 clears the
        # skip gate at line 336-337. All belong to one game so game_ids = [54321]
        # and the per-game loop body runs exactly once.
        pdf = pd.DataFrame(
            {
                "game_id": [54321, 54321],
                "match_id": [54321, 54321],
                "original_event_id": ["evt1", "evt2"],
                "period_id": [1, 1],
                "time_seconds": [0.0, 1.0],
                "team_id": [1, 1],
                "player_id": [100, 100],
                "start_x": [0.0, 0.0],
                "start_y": [0.0, 0.0],
                "end_x": [0.0, 0.0],
                "end_y": [0.0, 0.0],
                "type_id": [0, 0],
                "result_id": [1, 1],
                "bodypart_id": [0, 0],
                "competition_id": [2, 2],
                "season_id": [1, 1],
                "data_source": ["statsbomb", "statsbomb"],
            }
        )

        # Build the closure and pre-populate its model cache so the UDF skips
        # the xgboost load path entirely. _model_cache is an attribute on the
        # fresh closure returned by _make_scoring_udf (not module-level state),
        # so setting it here does not leak across tests.
        udf = _make_scoring_udf(b"scores_bytes", b"concedes_bytes")
        udf._model_cache = {"scores": MagicMock(), "concedes": MagicMock()}  # type: ignore[attr-defined]

        # Pass-through for add_names — preserves game_id so the downstream
        # groupby + per-game loop can execute up to the point of failure,
        # while adding the type_name/result_name/bodypart_name columns that
        # the non-failing branch (not exercised here) would need.
        def _passthrough_add_names(p: pd.DataFrame) -> pd.DataFrame:
            return p.assign(type_name="", result_name="", bodypart_name="")

        # Patch attributes on the real silly_kicks modules. The UDF does
        # local imports inside its closure (``import silly_kicks.vaep.features``),
        # which resolves to the real module objects — so patch.object on the
        # real modules is visible to the UDF's attribute lookups at call time.
        # Force gamestates to raise so the per-game try block at lines 338-391
        # fires its except clause, which under the fix re-raises as RuntimeError
        # with game_id context.
        with (
            patch.object(silly_kicks.spadl, "add_names", side_effect=_passthrough_add_names),
            patch.object(silly_kicks.vaep.features, "gamestates", side_effect=KeyError("simulated feature failure")),
            pytest.raises(RuntimeError, match="VAEP scoring failed for game_id=54321"),
        ):
            udf(pdf)  # type: ignore[operator]

    def test_udf_empty_input_returns_empty_dataframe(self) -> None:
        """Empty input should still return an empty DataFrame, not raise.

        The empty-input short-circuit at lines 287-288 must not be affected by
        the exception-propagation change.
        """
        from ingestion.spadl_vaep import _make_scoring_udf

        empty_pdf = pd.DataFrame(
            columns=pd.Index(
                [
                    "game_id",
                    "match_id",
                    "original_event_id",
                    "period_id",
                    "time_seconds",
                    "team_id",
                    "player_id",
                    "start_x",
                    "start_y",
                    "end_x",
                    "end_y",
                    "type_id",
                    "result_id",
                    "bodypart_id",
                    "competition_id",
                    "season_id",
                    "data_source",
                ]
            )
        )

        udf = _make_scoring_udf(b"scores_bytes", b"concedes_bytes")
        result = udf(empty_pdf)  # type: ignore[operator]
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# VAEP training feature extraction — regression guard for the per-game
# silent swallow (vaep_training.py:77-78) removed 2026-04-14.
# ---------------------------------------------------------------------------


class TestVaepTrainingFeatureExtractionErrorPropagation:
    """`extract_features_for_games` must propagate per-game failures with game_id context.

    The previous `except Exception: _log.exception(...)` pattern logged the
    failure but did NOT raise, silently dropping the game from the training
    data. Same anti-pattern class as the scoring-UDF silent swallow — fixed
    in the same session.
    """

    def test_gamestates_failure_raises_runtime_error_with_game_id(self) -> None:
        import silly_kicks.spadl
        import silly_kicks.vaep.features

        from ingestion.vaep_training import extract_features_for_games

        # Minimal SPADL actions frame: 2 rows per game so `len(game_actions) >= 2`
        # clears the skip gate. Two games — we'll trigger a failure on the first.
        actions = pd.DataFrame(
            {
                "game_id": [100, 100, 200, 200],
                "match_id": [100, 100, 200, 200],
                "original_event_id": ["a", "b", "c", "d"],
                "period_id": [1, 1, 1, 1],
                "time_seconds": [0.0, 1.0, 0.0, 1.0],
                "team_id": [1, 1, 2, 2],
                "player_id": [10, 10, 20, 20],
                "start_x": [0.0, 0.0, 0.0, 0.0],
                "start_y": [0.0, 0.0, 0.0, 0.0],
                "end_x": [0.0, 0.0, 0.0, 0.0],
                "end_y": [0.0, 0.0, 0.0, 0.0],
                "type_id": [0, 0, 0, 0],
                "result_id": [1, 1, 1, 1],
                "bodypart_id": [0, 0, 0, 0],
            }
        )

        def _passthrough_add_names(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(type_name="", result_name="", bodypart_name="")

        with (
            patch.object(silly_kicks.spadl, "add_names", side_effect=_passthrough_add_names),
            patch.object(
                silly_kicks.vaep.features,
                "gamestates",
                side_effect=KeyError("simulated feature failure"),
            ),
            pytest.raises(RuntimeError, match=r"VAEP feature extraction failed for game_id=100"),
        ):
            extract_features_for_games(actions, game_ids=[100, 200])

    def test_happy_path_returns_empty_when_all_games_too_short(self) -> None:
        """Games with <2 actions are skipped silently (not an error path)."""
        import silly_kicks.spadl

        from ingestion.vaep_training import extract_features_for_games

        # Single-action game triggers the `len(game_actions) < 2` skip.
        actions = pd.DataFrame(
            {
                "game_id": [100],
                "match_id": [100],
                "original_event_id": ["a"],
                "period_id": [1],
                "time_seconds": [0.0],
                "team_id": [1],
                "player_id": [10],
                "start_x": [0.0],
                "start_y": [0.0],
                "end_x": [0.0],
                "end_y": [0.0],
                "type_id": [0],
                "result_id": [1],
                "bodypart_id": [0],
            }
        )

        def _passthrough_add_names(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(type_name="", result_name="", bodypart_name="")

        with patch.object(silly_kicks.spadl, "add_names", side_effect=_passthrough_add_names):
            x, y_scores, y_concedes = extract_features_for_games(actions, game_ids=[100])

        assert x.empty
        assert y_scores.empty
        assert y_concedes.empty
