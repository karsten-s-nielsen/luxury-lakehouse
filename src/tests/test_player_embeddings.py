"""Tests for player embedding ingestion pipeline."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from ingestion.player_embeddings import (
    STAT_FEATURES,
    _build_bronze_dataframe,
    _compute_stat_vectors,
    _load_events,
    _merge_vectors,
    _zscore_normalize,
)

# ---------------------------------------------------------------------------
# Stat vector feature list
# ---------------------------------------------------------------------------


class TestStatVectorFeatures:
    """Verify the 13 stat features used for stat vectors."""

    def test_feature_count(self) -> None:
        assert len(STAT_FEATURES) == 13

    def test_expected_features_present(self) -> None:
        expected = {
            "goals_per_90",
            "xg_per_90",
            "passes_per_90",
            "pass_completion_pct",
            "progressive_passes_per_90",
            "line_breaking_per_90",
            "vaep_per_90",
            "offensive_vaep_per_90",
            "defensive_vaep_per_90",
            "defcon_per_90",
            "intercept_per_90",
            "deter_per_90",
            "xg_overperformance",
        }
        assert set(STAT_FEATURES) == expected

    def test_no_duplicates(self) -> None:
        assert len(STAT_FEATURES) == len(set(STAT_FEATURES))


# ---------------------------------------------------------------------------
# Z-score normalization
# ---------------------------------------------------------------------------


class TestZScoreNormalization:
    """Z-score normalization with NULL handling and zero-std protection."""

    def test_basic_normalization(self) -> None:
        """After z-score: mean ~ 0, std ~ 1."""
        df = pd.DataFrame(
            {
                "goals_per_90": [1.0, 2.0, 3.0, 4.0, 5.0],
                "xg_per_90": [0.5, 1.0, 1.5, 2.0, 2.5],
            }
        )
        result, _params = _zscore_normalize(df, ["goals_per_90", "xg_per_90"])
        assert abs(result["goals_per_90"].mean()) < 1e-10
        assert abs(result["goals_per_90"].std(ddof=0) - 1.0) < 0.01
        assert abs(result["xg_per_90"].mean()) < 1e-10

    def test_zero_std_returns_zero(self) -> None:
        """If all values identical, z-score should be 0."""
        df = pd.DataFrame({"goals_per_90": [5.0, 5.0, 5.0]})
        result, _params = _zscore_normalize(df, ["goals_per_90"])
        assert all(result["goals_per_90"] == 0.0)

    def test_null_values_stay_null(self) -> None:
        """NaN/None values should not be altered."""
        df = pd.DataFrame({"defcon_per_90": [1.0, None, 3.0]})
        result, _params = _zscore_normalize(df, ["defcon_per_90"])
        assert pd.isna(result["defcon_per_90"].iloc[1])
        # Non-null values should be normalized
        assert not pd.isna(result["defcon_per_90"].iloc[0])
        assert not pd.isna(result["defcon_per_90"].iloc[2])

    def test_params_contain_mean_and_std(self) -> None:
        df = pd.DataFrame({"goals_per_90": [1.0, 2.0, 3.0]})
        _, params = _zscore_normalize(df, ["goals_per_90"])
        assert "goals_per_90" in params
        assert "mean" in params["goals_per_90"]
        assert "std" in params["goals_per_90"]

    def test_params_values_correct(self) -> None:
        df = pd.DataFrame({"goals_per_90": [2.0, 4.0, 6.0]})
        _, params = _zscore_normalize(df, ["goals_per_90"])
        assert abs(params["goals_per_90"]["mean"] - 4.0) < 1e-10
        assert abs(params["goals_per_90"]["std"] - np.std([2.0, 4.0, 6.0])) < 1e-10


# ---------------------------------------------------------------------------
# Normalization parameter serialization
# ---------------------------------------------------------------------------


class TestNormalizationParams:
    """Mean/std computation and JSON serialization."""

    def test_serialization_roundtrip(self) -> None:
        df = pd.DataFrame(
            {
                "goals_per_90": [1.0, 2.0, 3.0],
                "xg_per_90": [0.5, 1.0, 1.5],
            }
        )
        _, params = _zscore_normalize(df, ["goals_per_90", "xg_per_90"])
        # Should be JSON-serializable
        serialized = json.dumps(params)
        deserialized = json.loads(serialized)
        assert deserialized["goals_per_90"]["mean"] == params["goals_per_90"]["mean"]
        assert deserialized["xg_per_90"]["std"] == params["xg_per_90"]["std"]

    def test_all_features_have_params(self) -> None:
        """Every feature should have mean/std in params."""
        features = ["goals_per_90", "xg_per_90", "xg_overperformance"]
        df = pd.DataFrame({f: [1.0, 2.0, 3.0] for f in features})
        _, params = _zscore_normalize(df, features)
        for f in features:
            assert f in params
            assert "mean" in params[f]
            assert "std" in params[f]


# ---------------------------------------------------------------------------
# Build bronze DataFrame
# ---------------------------------------------------------------------------


class TestBuildBronzeDataFrame:
    """Verify bronze DataFrame schema and vector dimensions."""

    def test_correct_columns(self) -> None:
        behavioral = {
            ("p1", "m1"): [0.1] * 32,
            ("p2", "m1"): [0.2] * 32,
        }
        stat = {
            ("p1", "m1"): [0.5] * 13,
            ("p2", "m1"): [0.6] * 13,
        }
        source_map = {"m1": "statsbomb"}
        result = _build_bronze_dataframe(behavioral, stat, source_map)
        expected_cols = {
            "canonical_player_id",
            "match_id",
            "data_source",
            "behavioral_vector",
            "stat_vector",
        }
        assert expected_cols == set(result.columns)

    def test_behavioral_vector_dimension(self) -> None:
        behavioral = {("p1", "m1"): [0.1] * 32}
        stat = {("p1", "m1"): [0.5] * 13}
        source_map = {"m1": "statsbomb"}
        result = _build_bronze_dataframe(behavioral, stat, source_map)
        assert len(result.iloc[0]["behavioral_vector"]) == 32

    def test_stat_vector_dimension(self) -> None:
        behavioral = {("p1", "m1"): [0.1] * 32}
        stat = {("p1", "m1"): [0.5] * 13}
        source_map = {"m1": "statsbomb"}
        result = _build_bronze_dataframe(behavioral, stat, source_map)
        assert len(result.iloc[0]["stat_vector"]) == 13

    def test_null_stat_vector_when_missing(self) -> None:
        """If no stat vector exists for a player-match, stat_vector should be None."""
        behavioral = {("p1", "m1"): [0.1] * 32}
        stat: dict[tuple[str, str], list[float]] = {}
        source_map = {"m1": "statsbomb"}
        result = _build_bronze_dataframe(behavioral, stat, source_map)
        assert result.iloc[0]["stat_vector"] is None

    def test_data_source_from_map(self) -> None:
        behavioral = {("p1", "m1"): [0.1] * 32}
        stat: dict[tuple[str, str], list[float]] = {}
        source_map = {"m1": "wyscout"}
        result = _build_bronze_dataframe(behavioral, stat, source_map)
        assert result.iloc[0]["data_source"] == "wyscout"

    def test_row_count_matches_behavioral(self) -> None:
        """One row per player-match from behavioral vectors."""
        behavioral = {
            ("p1", "m1"): [0.1] * 32,
            ("p1", "m2"): [0.2] * 32,
            ("p2", "m1"): [0.3] * 32,
        }
        stat: dict[tuple[str, str], list[float]] = {}
        source_map = {"m1": "statsbomb", "m2": "statsbomb"}
        result = _build_bronze_dataframe(behavioral, stat, source_map)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Stat vector grain join
# ---------------------------------------------------------------------------


class TestStatVectorGrainJoin:
    """Same stat vector for all matches in a competition-season."""

    def test_same_stat_vector_for_same_comp_season(self) -> None:
        """Player should get same stat vector for all matches in same comp-season."""
        # Mock stat data: player p1 has stats for comp_1/season_1
        stat_df = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "competition_id": ["comp_1"],
                "season_id": ["season_1"],
                "stat_vector": [[1.0, 2.0, 3.0]],
            }
        )
        match_competition_map: dict[str, tuple[str, str]] = {
            "m1": ("comp_1", "season_1"),
            "m2": ("comp_1", "season_1"),
        }
        behavioral_keys = [("p1", "m1"), ("p1", "m2")]
        result = _merge_vectors(behavioral_keys, stat_df, match_competition_map)
        assert result[("p1", "m1")] == [1.0, 2.0, 3.0]
        assert result[("p1", "m2")] == [1.0, 2.0, 3.0]

    def test_different_comp_season_gets_different_vector(self) -> None:
        stat_df = pd.DataFrame(
            {
                "canonical_player_id": ["p1", "p1"],
                "competition_id": ["comp_1", "comp_2"],
                "season_id": ["s1", "s2"],
                "stat_vector": [[1.0, 2.0], [3.0, 4.0]],
            }
        )
        match_competition_map: dict[str, tuple[str, str]] = {
            "m1": ("comp_1", "s1"),
            "m2": ("comp_2", "s2"),
        }
        behavioral_keys = [("p1", "m1"), ("p1", "m2")]
        result = _merge_vectors(behavioral_keys, stat_df, match_competition_map)
        assert result[("p1", "m1")] == [1.0, 2.0]
        assert result[("p1", "m2")] == [3.0, 4.0]

    def test_missing_stat_returns_none(self) -> None:
        stat_df = pd.DataFrame(
            {
                "canonical_player_id": pd.Series(dtype="str"),
                "competition_id": pd.Series(dtype="str"),
                "season_id": pd.Series(dtype="str"),
                "stat_vector": pd.Series(dtype="object"),
            }
        )
        match_competition_map: dict[str, tuple[str, str]] = {"m1": ("comp_1", "s1")}
        behavioral_keys = [("p1", "m1")]
        result = _merge_vectors(behavioral_keys, stat_df, match_competition_map)
        assert result[("p1", "m1")] is None

    def test_match_not_in_map_returns_none(self) -> None:
        """If match_id not in competition map, stat vector should be None."""
        stat_df = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "competition_id": ["comp_1"],
                "season_id": ["s1"],
                "stat_vector": [[1.0, 2.0]],
            }
        )
        match_competition_map: dict[str, tuple[str, str]] = {}
        behavioral_keys = [("p1", "m1")]
        result = _merge_vectors(behavioral_keys, stat_df, match_competition_map)
        assert result[("p1", "m1")] is None


# ---------------------------------------------------------------------------
# _load_events (mocked Spark)
# ---------------------------------------------------------------------------


class TestLoadEvents:
    """Event loading with mocked Spark."""

    def test_returns_expected_columns(self) -> None:
        mock_spark = MagicMock()
        expected_cols = [
            "canonical_player_id",
            "match_id",
            "event_type",
            "x",
            "y",
            "event_index",
            "data_source",
            "play_pattern",
            "pass_cross",
            "sub_event_type",
            "competition_id",
            "season_id",
        ]
        mock_pdf = pd.DataFrame({col: ["val"] for col in expected_cols})
        mock_spark.sql.return_value.toPandas.return_value = mock_pdf

        result = _load_events(mock_spark, "cat", "bronze")
        assert set(expected_cols).issubset(set(result.columns))

    def test_calls_spark_sql(self) -> None:
        mock_spark = MagicMock()
        mock_pdf = pd.DataFrame(
            {
                "canonical_player_id": [],
                "match_id": [],
                "event_type": [],
                "x": [],
                "y": [],
                "event_index": [],
                "data_source": [],
                "play_pattern": [],
                "pass_cross": [],
                "sub_event_type": [],
                "competition_id": [],
                "season_id": [],
            }
        )
        mock_spark.sql.return_value.toPandas.return_value = mock_pdf
        _load_events(mock_spark, "catalog", "schema")
        assert mock_spark.sql.called


# ---------------------------------------------------------------------------
# _compute_stat_vectors (mocked Spark)
# ---------------------------------------------------------------------------


class TestComputeStatVectors:
    """Stat vector computation with mocked Spark."""

    def test_returns_dataframe_with_stat_vector(self) -> None:
        mock_spark = MagicMock()
        mock_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1", "p2"],
                "competition_id": ["c1", "c1"],
                "season_id": ["s1", "s1"],
                **{f: [1.0, 2.0] for f in STAT_FEATURES},
            }
        )
        mock_spark.sql.return_value.toPandas.return_value = mock_pdf

        result, _params = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        assert "stat_vector" in result.columns
        assert "canonical_player_id" in result.columns
        assert "competition_id" in result.columns
        assert "season_id" in result.columns

    def test_stat_vector_length_is_13(self) -> None:
        mock_spark = MagicMock()
        mock_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1", "p2"],
                "competition_id": ["c1", "c1"],
                "season_id": ["s1", "s1"],
                **{f: [1.0, 2.0] for f in STAT_FEATURES},
            }
        )
        mock_spark.sql.return_value.toPandas.return_value = mock_pdf

        result, _ = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        assert len(result.iloc[0]["stat_vector"]) == 13

    def test_null_defcon_features_preserved(self) -> None:
        """Nullable DEFCON features should survive as None in vector."""
        mock_spark = MagicMock()
        data: dict[str, Any] = {
            "canonical_player_id": ["p1"],
            "competition_id": ["c1"],
            "season_id": ["s1"],
        }
        for f in STAT_FEATURES:
            if f in ("defcon_per_90", "intercept_per_90", "deter_per_90"):
                data[f] = [None]
            else:
                data[f] = [1.0]
        mock_pdf = pd.DataFrame(data)
        mock_spark.sql.return_value.toPandas.return_value = mock_pdf

        result, _ = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        vec = result.iloc[0]["stat_vector"]
        # DEFCON features should be None (set to None in input)
        defcon_indices = [STAT_FEATURES.index(f) for f in ("defcon_per_90", "intercept_per_90", "deter_per_90")]
        for idx in defcon_indices:
            assert vec[idx] is None or (isinstance(vec[idx], float) and np.isnan(vec[idx]))


# ---------------------------------------------------------------------------
# main() — mock Spark + Delta writes
# ---------------------------------------------------------------------------


class TestMainFunction:
    """End-to-end pipeline with mocked Spark and file I/O."""

    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events")
    @patch("ingestion.player_embeddings._compute_stat_vectors")
    @patch("ingestion.player_embeddings.validate_dataframe")
    @patch("ingestion.player_embeddings.write_delta_table")
    @patch("ingestion.player_embeddings._load_model")
    def test_writes_with_replace_where(
        self,
        mock_load_model: MagicMock,
        mock_write: MagicMock,
        mock_validate: MagicMock,
        mock_stat: MagicMock,
        mock_events: MagicMock,
        mock_args: MagicMock,
        mock_spark: MagicMock,
    ) -> None:
        from analytics.football2vec import TrainingConfig, train_model

        # Setup mock args
        args = MagicMock()
        args.catalog = "soccer_analytics"
        args.schema = "bronze"
        mock_args.return_value = args

        # Setup mock Spark
        spark = MagicMock()
        mock_spark.return_value = spark

        # Setup events
        events = pd.DataFrame(
            {
                "canonical_player_id": ["p1", "p1", "p1", "p1"],
                "match_id": ["m1", "m1", "m1", "m1"],
                "event_type": ["Pass", "Pass", "Shot", "Pass"],
                "x": [60.0, 30.0, 110.0, 60.0],
                "y": [40.0, 20.0, 40.0, 40.0],
                "event_index": [1, 2, 3, 4],
                "data_source": ["statsbomb", "statsbomb", "statsbomb", "statsbomb"],
                "play_pattern": [None, None, None, None],
                "pass_cross": [None, None, None, None],
                "sub_event_type": [None, None, None, None],
                "competition_id": ["c1", "c1", "c1", "c1"],
                "season_id": ["s1", "s1", "s1", "s1"],
            }
        )
        mock_events.return_value = events

        # Setup stat vectors
        stat_df = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "competition_id": ["c1"],
                "season_id": ["s1"],
                "stat_vector": [[0.5] * 13],
            }
        )
        mock_stat.return_value = (stat_df, {"goals_per_90": {"mean": 0.5, "std": 0.1}})

        # Setup model — build a real tiny model
        seqs = {
            ("p1", "m1"): ["pass_6_4", "pass_3_2", "shot_11_4", "pass_6_4"],
            ("p2", "m2"): ["pass_6_4", "shot_11_4", "pass_6_4", "shot_11_4"],
        }
        model = train_model(seqs, TrainingConfig())
        mock_load_model.return_value = model

        # Mock validate
        mock_validate.return_value = 1

        # Mock Spark createDataFrame
        spark.createDataFrame.return_value = MagicMock()

        from ingestion.player_embeddings import main

        main()

        # Verify write was called
        assert mock_write.called

        # Verify replace_where was used with data_source filter
        call_kwargs = mock_write.call_args
        assert call_kwargs is not None
        assert "replace_where" in call_kwargs.kwargs
        assert "data_source" in call_kwargs.kwargs["replace_where"]

    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events")
    def test_skips_when_all_matches_have_embeddings(
        self,
        mock_events: MagicMock,
        mock_args: MagicMock,
        mock_spark: MagicMock,
    ) -> None:
        """Pipeline returns early when all source matches already have embeddings."""
        args = MagicMock()
        args.catalog = "cat"
        args.schema = "bronze"
        mock_args.return_value = args

        spark = MagicMock()
        mock_spark.return_value = spark

        # existing_matches: spark.table().select().distinct().collect() returns m1, m2
        existing_row_1 = MagicMock()
        existing_row_1.__getitem__ = lambda self, k: "m1"
        existing_row_2 = MagicMock()
        existing_row_2.__getitem__ = lambda self, k: "m2"
        spark.table.return_value.select.return_value.distinct.return_value.collect.return_value = [
            existing_row_1,
            existing_row_2,
        ]

        # source_matches: spark.sql().collect() returns m1, m2 (same set)
        source_row_1 = MagicMock()
        source_row_1.__getitem__ = lambda self, k: "m1"
        source_row_2 = MagicMock()
        source_row_2.__getitem__ = lambda self, k: "m2"
        spark.sql.return_value.collect.return_value = [source_row_1, source_row_2]

        from ingestion.player_embeddings import main

        main()

        # _load_events should NOT be called — pipeline skipped
        mock_events.assert_not_called()

    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events")
    def test_defensive_fallback_no_source_matches_but_existing_embeddings(
        self,
        mock_events: MagicMock,
        mock_args: MagicMock,
        mock_spark: MagicMock,
    ) -> None:
        """If source_matches query returns empty but existing embeddings exist, skip."""
        args = MagicMock()
        args.catalog = "cat"
        args.schema = "bronze"
        mock_args.return_value = args

        spark = MagicMock()
        mock_spark.return_value = spark

        # existing_matches: spark.table().select().distinct().collect() returns m1
        existing_row = MagicMock()
        existing_row.__getitem__ = lambda self, k: "m1"
        spark.table.return_value.select.return_value.distinct.return_value.collect.return_value = [existing_row]

        # source_matches: spark.sql().collect() returns empty (simulating query failure/mismatch)
        spark.sql.return_value.collect.return_value = []

        from ingestion.player_embeddings import main

        main()

        # _load_events should NOT be called — defensive fallback triggered
        mock_events.assert_not_called()

    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events")
    def test_empty_events_exits_early(
        self,
        mock_events: MagicMock,
        mock_args: MagicMock,
        mock_spark: MagicMock,
    ) -> None:
        args = MagicMock()
        args.catalog = "cat"
        args.schema = "bronze"
        mock_args.return_value = args
        mock_spark.return_value = MagicMock()

        # Return empty events
        mock_events.return_value = pd.DataFrame(
            {
                "canonical_player_id": pd.Series(dtype="str"),
                "match_id": pd.Series(dtype="str"),
                "event_type": pd.Series(dtype="str"),
                "x": pd.Series(dtype="float"),
                "y": pd.Series(dtype="float"),
                "event_index": pd.Series(dtype="int"),
                "data_source": pd.Series(dtype="str"),
                "play_pattern": pd.Series(dtype="str"),
                "pass_cross": pd.Series(dtype="str"),
                "sub_event_type": pd.Series(dtype="str"),
                "competition_id": pd.Series(dtype="str"),
                "season_id": pd.Series(dtype="str"),
            }
        )

        from ingestion.player_embeddings import main

        # Should not raise — just log and return
        main()
