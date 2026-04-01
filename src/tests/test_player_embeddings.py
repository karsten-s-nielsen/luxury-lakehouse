"""Tests for player embedding ingestion pipeline."""
# pyright: reportArgumentType=false

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from ingestion.player_embeddings import (
    STAT_FEATURES,
    STAT_FEATURES_BY_GROUP,
    _build_bronze_dataframe,
    _compute_stat_vectors,
    _load_events,
    _load_events_sdf,
    _make_behavioral_udf,
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

    def test_backwards_compat_alias(self) -> None:
        """STAT_FEATURES is an alias for the Defender group (backwards compatibility)."""
        assert STAT_FEATURES is STAT_FEATURES_BY_GROUP["Defender"]

    def test_all_position_groups_present(self) -> None:
        assert set(STAT_FEATURES_BY_GROUP.keys()) == {"Goalkeeper", "Defender", "Midfielder", "Forward"}

    def test_goalkeeper_features(self) -> None:
        expected_gk = {"save_pct", "gk_xt_per_pass", "launch_rate", "claim_success_rate"}
        assert set(STAT_FEATURES_BY_GROUP["Goalkeeper"]) == expected_gk

    def test_outfield_groups_share_features(self) -> None:
        """Defender, Midfielder, Forward currently share the same feature set."""
        assert STAT_FEATURES_BY_GROUP["Defender"] == STAT_FEATURES_BY_GROUP["Midfielder"]
        assert STAT_FEATURES_BY_GROUP["Defender"] == STAT_FEATURES_BY_GROUP["Forward"]

    def test_no_duplicates_per_group(self) -> None:
        for group, features in STAT_FEATURES_BY_GROUP.items():
            assert len(features) == len(set(features)), f"Duplicates in {group}"


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
# Per-position-group z-score normalization
# ---------------------------------------------------------------------------


class TestPositionGroupZScoring:
    """Verify per-group z-scoring prevents goalkeeper contamination.

    With global z-scoring, a goalkeeper's goals_per_90 is pulled far negative
    by the outfield mean.  With per-group z-scoring, the same goalkeeper's
    z-score stays close to 0 within its group.
    """

    def test_global_vs_per_group_goalkeeper_contamination(self) -> None:
        """Demonstrate that per-group z-scoring eliminates GK contamination."""
        # Outfield players: goals_per_90 ~ 0.3-0.5 (typical strikers)
        # Goalkeepers: goals_per_90 ~ 0.0 (almost never score)
        features = ["goals_per_90"]
        outfield_goals = [0.3, 0.35, 0.4, 0.45, 0.5, 0.32, 0.38, 0.42]
        gk_goals = [0.0, 0.01, 0.0, 0.02]

        all_goals = outfield_goals + gk_goals
        position_groups = ["Forward"] * len(outfield_goals) + ["Goalkeeper"] * len(gk_goals)

        df = pd.DataFrame({"goals_per_90": all_goals, "position_group": position_groups})

        # --- Global z-scoring (old behavior) ---
        global_norm, _ = _zscore_normalize(df, features)
        # GK goals_per_90 z-scores are very negative under global normalization
        gk_global_zscores = global_norm.iloc[len(outfield_goals) :]["goals_per_90"]
        assert all(z < -1.0 for z in gk_global_zscores), (
            f"Expected GK global z-scores far below -1, got {gk_global_zscores.tolist()}"
        )

        # --- Per-group z-scoring (new behavior) ---
        gk_df = df[df["position_group"] == "Goalkeeper"]
        gk_norm, _ = _zscore_normalize(gk_df, features)
        gk_group_zscores = gk_norm["goals_per_90"]
        # Within-group, GK z-scores should be close to 0 (mean of their own group)
        assert abs(gk_group_zscores.mean()) < 1e-10, (
            f"Expected GK per-group z-score mean ~ 0, got {gk_group_zscores.mean()}"
        )

    def test_per_group_normalization_produces_group_keyed_params(self) -> None:
        """Per-group normalization returns params keyed by position group."""
        features = ["goals_per_90"]
        df = pd.DataFrame(
            {
                "goals_per_90": [0.4, 0.5, 0.0, 0.01],
                "position_group": ["Forward", "Forward", "Goalkeeper", "Goalkeeper"],
            }
        )

        all_params: dict[str, dict[str, dict[str, float]]] = {}
        for group_name, group_df in df.groupby("position_group"):
            _, group_params = _zscore_normalize(group_df, features)
            all_params[str(group_name)] = group_params

        assert "Forward" in all_params
        assert "Goalkeeper" in all_params
        assert "goals_per_90" in all_params["Forward"]
        assert "goals_per_90" in all_params["Goalkeeper"]
        # Means should differ significantly between groups
        fwd_mean = all_params["Forward"]["goals_per_90"]["mean"]
        gk_mean = all_params["Goalkeeper"]["goals_per_90"]["mean"]
        assert fwd_mean > 0.3, f"Expected Forward mean > 0.3, got {fwd_mean}"
        assert gk_mean < 0.02, f"Expected GK mean < 0.02, got {gk_mean}"


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
# _load_events_sdf (mocked Spark — returns Spark DF)
# ---------------------------------------------------------------------------


class TestLoadEventsSdf:
    """Event loading as Spark DataFrame with mocked Spark."""

    def test_calls_spark_sql(self) -> None:
        """_load_events_sdf should call spark.sql and return the SDF."""
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.sql.return_value = mock_sdf

        result = _load_events_sdf(mock_spark, "catalog", "schema")
        assert mock_spark.sql.called
        assert result is mock_sdf

    @patch.dict("sys.modules", {"pyspark.sql": MagicMock(), "pyspark.sql.functions": MagicMock()})
    def test_filters_by_match_ids_when_provided(self) -> None:
        """When match_ids is provided, a .filter() is applied."""
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.sql.return_value = mock_sdf

        mock_filtered_sdf = MagicMock()
        mock_sdf.filter.return_value = mock_filtered_sdf

        result = _load_events_sdf(mock_spark, "catalog", "schema", match_ids={"m1", "m2"})

        # filter() should have been called on the SQL result
        mock_sdf.filter.assert_called_once()
        # The return value should be the filtered SDF
        assert result is mock_filtered_sdf

    def test_no_filter_when_match_ids_none(self) -> None:
        """When match_ids is None, no filter is applied."""
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.sql.return_value = mock_sdf

        result = _load_events_sdf(mock_spark, "catalog", "schema", match_ids=None)

        # filter() should NOT be called
        mock_sdf.filter.assert_not_called()
        # Should return the SQL result directly
        assert result is mock_sdf


# ---------------------------------------------------------------------------
# _load_events backward-compatible wrapper (mocked Spark)
# ---------------------------------------------------------------------------


class TestLoadEvents:
    """Event loading with mocked Spark (backward-compatible wrapper)."""

    def test_returns_expected_columns(self) -> None:
        mock_spark = MagicMock()
        expected_cols = [
            "canonical_player_id",
            "match_id",
            "action_type",
            "start_x",
            "start_y",
            "event_index",
            "data_source",
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
                "action_type": [],
                "start_x": [],
                "start_y": [],
                "event_index": [],
                "data_source": [],
                "competition_id": [],
                "season_id": [],
            }
        )
        mock_spark.sql.return_value.limit.return_value.toPandas.return_value = mock_pdf
        _load_events(mock_spark, "catalog", "schema")
        assert mock_spark.sql.called

    @patch.dict("sys.modules", {"pyspark.sql": MagicMock(), "pyspark.sql.functions": MagicMock()})
    def test_filters_by_match_ids_when_provided(self) -> None:
        """When match_ids is provided, a .filter() is applied before .toPandas()."""
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.sql.return_value = mock_sdf

        # The filtered SDF should have toPandas called on it, not the original
        mock_filtered_sdf = MagicMock()
        mock_sdf.filter.return_value = mock_filtered_sdf

        empty_pdf = pd.DataFrame(
            {
                "canonical_player_id": [],
                "match_id": [],
                "action_type": [],
                "start_x": [],
                "start_y": [],
                "event_index": [],
                "data_source": [],
                "competition_id": [],
                "season_id": [],
            }
        )
        mock_filtered_sdf.toPandas.return_value = empty_pdf

        _load_events(mock_spark, "catalog", "schema", match_ids={"m1", "m2"})

        # filter() should have been called on the SQL result
        mock_sdf.filter.assert_called_once()
        # toPandas() should be called on the filtered DF, not the original
        mock_filtered_sdf.toPandas.assert_called_once()

    def test_no_filter_when_match_ids_none(self) -> None:
        """When match_ids is None, no filter is applied."""
        mock_spark = MagicMock()
        mock_sdf = MagicMock()
        mock_spark.sql.return_value = mock_sdf

        empty_pdf = pd.DataFrame(
            {
                "canonical_player_id": [],
                "match_id": [],
                "action_type": [],
                "start_x": [],
                "start_y": [],
                "event_index": [],
                "data_source": [],
                "competition_id": [],
                "season_id": [],
            }
        )
        mock_sdf.toPandas.return_value = empty_pdf

        _load_events(mock_spark, "catalog", "schema", match_ids=None)

        # filter() should NOT be called
        mock_sdf.filter.assert_not_called()
        # toPandas() called directly on SQL result
        mock_sdf.toPandas.assert_called_once()


# ---------------------------------------------------------------------------
# _compute_stat_vectors (mocked Spark)
# ---------------------------------------------------------------------------


class TestComputeStatVectors:
    """Stat vector computation with mocked Spark.

    ``_compute_stat_vectors`` now issues one SQL query per position group
    (GK from fct_goalkeeper_stats, outfield from fct_player_stats).  The
    mock returns data only for queries whose SQL contains the relevant
    position group or table name.
    """

    @staticmethod
    def _make_mock_spark(
        outfield_pdf: pd.DataFrame | None = None,
        gk_pdf: pd.DataFrame | None = None,
    ) -> MagicMock:
        """Build a mock Spark session that returns per-group DataFrames.

        Routes based on the SQL string: queries containing
        'fct_goalkeeper_stats' return ``gk_pdf``; others return
        ``outfield_pdf``.  Empty DataFrames are returned for groups
        without data.
        """
        mock_spark = MagicMock()
        empty_pdf = pd.DataFrame()

        def _sql_side_effect(query: str) -> MagicMock:
            mock_sdf = MagicMock()
            if "fct_goalkeeper_stats" in query:
                pdf = gk_pdf if gk_pdf is not None else empty_pdf
            else:
                pdf = outfield_pdf if outfield_pdf is not None else empty_pdf
            mock_sdf.limit.return_value.toPandas.return_value = pdf
            mock_sdf.filter.return_value.limit.return_value.toPandas.return_value = pdf
            return mock_sdf

        mock_spark.sql.side_effect = _sql_side_effect
        return mock_spark

    def test_returns_dataframe_with_stat_vector(self) -> None:
        outfield_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1", "p2"],
                "competition_id": ["c1", "c1"],
                "season_id": ["s1", "s1"],
                "position_group": ["Forward", "Forward"],
                **{f: [1.0, 2.0] for f in STAT_FEATURES},
            }
        )
        mock_spark = self._make_mock_spark(outfield_pdf=outfield_pdf)

        result, _params = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        assert "stat_vector" in result.columns
        assert "canonical_player_id" in result.columns
        assert "competition_id" in result.columns
        assert "season_id" in result.columns

    def test_stat_vector_length_is_13(self) -> None:
        outfield_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1", "p2"],
                "competition_id": ["c1", "c1"],
                "season_id": ["s1", "s1"],
                "position_group": ["Forward", "Forward"],
                **{f: [1.0, 2.0] for f in STAT_FEATURES},
            }
        )
        mock_spark = self._make_mock_spark(outfield_pdf=outfield_pdf)

        result, _ = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        # Outfield vectors have 13 features
        assert len(result.iloc[0]["stat_vector"]) == 13

    def test_goalkeeper_stat_vector_length_is_4(self) -> None:
        """Goalkeeper stat vectors have 4 features (save_pct, gk_xt_per_pass, launch_rate, claim_success_rate)."""
        gk_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["gk1"],
                "competition_id": ["c1"],
                "season_id": ["s1"],
                "position_group": ["Goalkeeper"],
                "save_pct": [75.0],
                "gk_xt_per_pass": [0.02],
                "launch_rate": [30.0],
                "claim_success_rate": [85.0],
            }
        )
        mock_spark = self._make_mock_spark(gk_pdf=gk_pdf)

        result, params = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        assert len(result) >= 1
        gk_row = result.iloc[0]
        assert len(gk_row["stat_vector"]) == 4
        assert "Goalkeeper" in params

    def test_null_defcon_features_preserved(self) -> None:
        """Nullable DEFCON features should survive as None in vector."""
        data: dict[str, Any] = {
            "canonical_player_id": ["p1"],
            "competition_id": ["c1"],
            "season_id": ["s1"],
            "position_group": ["Forward"],
        }
        for f in STAT_FEATURES:
            if f in ("defcon_per_90", "intercept_per_90", "deter_per_90"):
                data[f] = [None]
            else:
                data[f] = [1.0]
        outfield_pdf = pd.DataFrame(data)
        mock_spark = self._make_mock_spark(outfield_pdf=outfield_pdf)

        result, _ = _compute_stat_vectors(mock_spark, "cat", "dev_gold")
        # Find the Forward row (only outfield data provided)
        vec = result.iloc[0]["stat_vector"]
        # DEFCON features should be None (set to None in input)
        defcon_indices = [STAT_FEATURES.index(f) for f in ("defcon_per_90", "intercept_per_90", "deter_per_90")]
        for idx in defcon_indices:
            assert vec[idx] is None or (isinstance(vec[idx], float) and np.isnan(vec[idx]))

    def test_player_ids_filter_applied_via_dataframe(self) -> None:
        """When player_ids is provided, a DataFrame .filter() should be applied."""
        outfield_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "competition_id": ["c1"],
                "season_id": ["s1"],
                "position_group": ["Forward"],
                **{f: [1.0] for f in STAT_FEATURES},
            }
        )
        gk_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["gk1"],
                "competition_id": ["c1"],
                "season_id": ["s1"],
                "position_group": ["Goalkeeper"],
                "save_pct": [75.0],
                "gk_xt_per_pass": [0.02],
                "launch_rate": [30.0],
                "claim_success_rate": [85.0],
            }
        )
        mock_spark = MagicMock()

        def _sql_side_effect(query: str) -> MagicMock:
            mock_sdf = MagicMock()
            if "fct_goalkeeper_stats" in query:
                pdf = gk_pdf
            else:
                pdf = outfield_pdf
            mock_sdf.filter.return_value.limit.return_value.toPandas.return_value = pdf
            mock_sdf.limit.return_value.toPandas.return_value = pdf
            return mock_sdf

        mock_spark.sql.side_effect = _sql_side_effect

        _compute_stat_vectors(mock_spark, "cat", "dev_gold", player_ids={42, 99})

        # All SQL calls should NOT contain IN clause (filter is via DataFrame API)
        for call_args in mock_spark.sql.call_args_list:
            sql_called = call_args[0][0]
            assert "canonical_player_id IN" not in sql_called

    def test_no_player_ids_filter_when_none(self) -> None:
        """When player_ids is None, the SQL should NOT include an IN clause."""
        outfield_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "competition_id": ["c1"],
                "season_id": ["s1"],
                "position_group": ["Forward"],
                **{f: [1.0] for f in STAT_FEATURES},
            }
        )
        mock_spark = self._make_mock_spark(outfield_pdf=outfield_pdf)

        _compute_stat_vectors(mock_spark, "cat", "dev_gold", player_ids=None)

        for call_args in mock_spark.sql.call_args_list:
            sql_called = call_args[0][0]
            assert "canonical_player_id IN" not in sql_called

    def test_empty_result_when_all_groups_empty(self) -> None:
        """When all groups return empty DataFrames, result should be empty."""
        mock_spark = self._make_mock_spark()

        result, params = _compute_stat_vectors(mock_spark, "cat", "dev_gold", player_ids=set())
        assert result.empty
        assert params == {}


# ---------------------------------------------------------------------------
# _make_behavioral_udf (unit test with real Doc2Vec)
# ---------------------------------------------------------------------------


class TestBehavioralUdf:
    """Test the applyInPandas UDF for behavioral inference."""

    def test_udf_returns_expected_columns(self) -> None:
        """UDF should produce the expected output columns."""
        from analytics.football2vec import TrainingConfig, train_model

        # Train a tiny model and save to tempdir
        seqs = {
            ("p1", "m1"): ["pass_6_4", "pass_3_2", "shot_11_4"],
            ("p2", "m2"): ["pass_6_4", "shot_11_4", "pass_6_4"],
        }
        model = train_model(seqs, TrainingConfig())

        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, "player2vec.model")
            model.save(model_path)

            udf_fn = _make_behavioral_udf(model_path)

            # Build input DataFrame matching SPADL events schema
            pdf = pd.DataFrame(
                {
                    "canonical_player_id": ["p1", "p1", "p1"],
                    "match_id": ["m1", "m1", "m1"],
                    "action_type": ["pass", "pass", "shot"],
                    "start_x": [60.0, 30.0, 100.0],
                    "start_y": [40.0, 20.0, 40.0],
                    "event_index": [1, 2, 3],
                    "data_source": ["statsbomb", "statsbomb", "statsbomb"],
                    "competition_id": ["c1", "c1", "c1"],
                    "season_id": ["s1", "s1", "s1"],
                    "batch_id": [0, 0, 0],
                }
            )

            result = udf_fn(pdf)  # type: ignore[operator]

            expected_cols = {
                "canonical_player_id",
                "match_id",
                "data_source",
                "behavioral_vector",
                "competition_id",
                "season_id",
            }
            assert expected_cols == set(result.columns)
            assert len(result) == 1  # one player-match pair

            # behavioral_vector should be a JSON string of 32 floats
            vec = json.loads(result.iloc[0]["behavioral_vector"])
            assert len(vec) == 32
            assert all(isinstance(v, float) for v in vec)

    def test_udf_handles_empty_input(self) -> None:
        """UDF should return empty DataFrame for empty input."""
        udf_fn = _make_behavioral_udf("/nonexistent/path")
        result = udf_fn(pd.DataFrame())  # type: ignore[operator]
        assert result.empty

    def test_udf_handles_wyscout_events(self) -> None:
        """UDF should tokenize Wyscout SPADL actions correctly."""
        from analytics.football2vec import TrainingConfig, train_model

        seqs = {
            ("p1", "m1"): ["pass_6_4", "shot_3_2"],
            ("p2", "m2"): ["pass_6_4", "shot_11_4"],
        }
        model = train_model(seqs, TrainingConfig())

        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, "player2vec.model")
            model.save(model_path)

            udf_fn = _make_behavioral_udf(model_path)

            pdf = pd.DataFrame(
                {
                    "canonical_player_id": ["p1", "p1"],
                    "match_id": ["m1", "m1"],
                    "action_type": ["pass", "shot"],
                    "start_x": [60.0, 50.0],
                    "start_y": [40.0, 40.0],
                    "event_index": [1, 2],
                    "data_source": ["wyscout", "wyscout"],
                    "competition_id": ["c1", "c1"],
                    "season_id": ["s1", "s1"],
                    "batch_id": [0, 0],
                }
            )

            result = udf_fn(pdf)  # type: ignore[operator]
            assert len(result) == 1
            assert result.iloc[0]["data_source"] == "wyscout"

    def test_udf_multiple_players_in_batch(self) -> None:
        """UDF should handle multiple players within a single batch."""
        from analytics.football2vec import TrainingConfig, train_model

        seqs = {
            ("p1", "m1"): ["pass_6_4", "shot_3_2"],
            ("p2", "m1"): ["pass_6_4", "shot_11_4"],
        }
        model = train_model(seqs, TrainingConfig())

        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, "player2vec.model")
            model.save(model_path)

            udf_fn = _make_behavioral_udf(model_path)

            pdf = pd.DataFrame(
                {
                    "canonical_player_id": ["p1", "p1", "p2", "p2"],
                    "match_id": ["m1", "m1", "m1", "m1"],
                    "action_type": ["pass", "shot", "pass", "shot"],
                    "start_x": [60.0, 100.0, 30.0, 95.0],
                    "start_y": [40.0, 40.0, 20.0, 30.0],
                    "event_index": [1, 2, 3, 4],
                    "data_source": ["statsbomb"] * 4,
                    "competition_id": ["c1"] * 4,
                    "season_id": ["s1"] * 4,
                    "batch_id": [0] * 4,
                }
            )

            result = udf_fn(pdf)  # type: ignore[operator]
            # Two player-match pairs
            assert len(result) == 2
            player_ids = set(result["canonical_player_id"])
            assert player_ids == {"p1", "p2"}


# ---------------------------------------------------------------------------
# main() — mock Spark + Delta writes
# ---------------------------------------------------------------------------


class TestMainFunction:
    """End-to-end pipeline with mocked Spark and file I/O."""

    @patch.dict(
        "sys.modules",
        {
            "pyspark.sql": MagicMock(),
            "pyspark.sql.functions": MagicMock(),
            "pyspark.sql.types": MagicMock(),
        },
    )
    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events_sdf")
    @patch("ingestion.player_embeddings._compute_stat_vectors")
    @patch("ingestion.player_embeddings.validate_dataframe")
    @patch("ingestion.player_embeddings.write_delta_table")
    @patch("ingestion.player_embeddings._make_behavioral_udf")
    def test_writes_with_replace_where(
        self,
        mock_make_udf: MagicMock,
        mock_write: MagicMock,
        mock_validate: MagicMock,
        mock_stat: MagicMock,
        mock_events_sdf: MagicMock,
        mock_args: MagicMock,
        mock_spark: MagicMock,
    ) -> None:
        # Setup mock args
        args = MagicMock()
        args.catalog = "soccer_analytics"
        args.schema = "bronze"
        mock_args.return_value = args

        # Setup mock Spark
        spark = MagicMock()
        mock_spark.return_value = spark

        # Setup events SDF: non-empty (limit(1).count() > 0)
        mock_sdf = MagicMock()
        mock_sdf.limit.return_value.count.return_value = 1
        # select("canonical_player_id").distinct() for player batching
        mock_player_sdf = MagicMock()
        mock_sdf.select.return_value.distinct.return_value = mock_player_sdf
        mock_player_sdf.count.return_value = 2  # 2 players
        mock_player_sdf.withColumn.return_value = mock_player_sdf
        # join returns batched SDF
        mock_batched_sdf = MagicMock()
        mock_sdf.join.return_value = mock_batched_sdf
        mock_events_sdf.return_value = mock_sdf

        # Setup applyInPandas result: behavioral_sdf.toPandas()
        behavioral_pdf = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "match_id": ["m1"],
                "data_source": ["statsbomb"],
                "behavioral_vector": [json.dumps([0.1] * 32)],
                "competition_id": ["c1"],
                "season_id": ["s1"],
            }
        )
        mock_behavioral_result = MagicMock()
        mock_behavioral_result.toPandas.return_value = behavioral_pdf
        mock_batched_sdf.groupBy.return_value.applyInPandas.return_value = mock_behavioral_result

        # Setup stat vectors
        stat_df = pd.DataFrame(
            {
                "canonical_player_id": ["p1"],
                "competition_id": ["c1"],
                "season_id": ["s1"],
                "stat_vector": [[0.5] * 13],
            }
        )
        mock_stat.return_value = (stat_df, {"Forward": {"goals_per_90": {"mean": 0.5, "std": 0.1}}})

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
    @patch("ingestion.player_embeddings._load_events_sdf")
    def test_skips_when_all_matches_have_embeddings(
        self,
        mock_events_sdf: MagicMock,
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

        # _load_events_sdf should NOT be called — pipeline skipped
        mock_events_sdf.assert_not_called()

    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events_sdf")
    def test_defensive_fallback_no_source_matches_but_existing_embeddings(
        self,
        mock_events_sdf: MagicMock,
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

        # _load_events_sdf should NOT be called — defensive fallback triggered
        mock_events_sdf.assert_not_called()

    @patch("ingestion.player_embeddings.get_spark_session")
    @patch("ingestion.player_embeddings.parse_ingestion_args")
    @patch("ingestion.player_embeddings._load_events_sdf")
    def test_empty_events_exits_early(
        self,
        mock_events_sdf: MagicMock,
        mock_args: MagicMock,
        mock_spark: MagicMock,
    ) -> None:
        args = MagicMock()
        args.catalog = "cat"
        args.schema = "bronze"
        mock_args.return_value = args
        mock_spark.return_value = MagicMock()

        # Return SDF with no rows (limit(1).count() == 0)
        mock_sdf = MagicMock()
        mock_sdf.limit.return_value.count.return_value = 0
        mock_events_sdf.return_value = mock_sdf

        from ingestion.player_embeddings import main

        # Should not raise — just log and return
        main()
