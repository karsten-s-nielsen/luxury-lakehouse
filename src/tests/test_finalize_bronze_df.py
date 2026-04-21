"""Unit tests for the finalize_bronze_df helper.

The helper protects bronze parsers from the pandas→Arrow→Spark NullType
column drop. See src/ingestion/utils.py:finalize_bronze_df for the full
root-cause write-up.
"""

from __future__ import annotations

import pandas as pd

from ingestion.utils import finalize_bronze_df


class TestAddsMissingColumns:
    def test_missing_columns_added_with_default_string_dtype(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        finalize_bronze_df(df, expected_cols={"a", "b", "c"})
        assert "b" in df.columns
        assert "c" in df.columns
        assert df["b"].dtype == pd.StringDtype()
        assert df["c"].dtype == pd.StringDtype()
        assert df["b"].isna().all()
        assert df["c"].isna().all()
        assert len(df) == 3

    def test_missing_columns_respect_dtype_overrides(self) -> None:
        df = pd.DataFrame({"a": [1, 2]})
        finalize_bronze_df(
            df,
            expected_cols={"a", "count", "xg"},
            dtype_overrides={"count": "Int64", "xg": "Float64"},
        )
        assert df["count"].dtype == pd.Int64Dtype()
        assert df["xg"].dtype == pd.Float64Dtype()

    def test_existing_columns_not_overwritten(self) -> None:
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        finalize_bronze_df(df, expected_cols={"a", "b", "c"})
        assert df["b"].tolist() == ["x", "y"]
        assert df["a"].tolist() == [1, 2]


class TestCastsAllNullObjectColumns:
    def test_all_none_object_col_cast_to_string(self) -> None:
        df = pd.DataFrame({"a": [1, 2], "b": [None, None]})
        # Confirm the all-None column is object dtype before the helper runs.
        assert df["b"].dtype == object
        finalize_bronze_df(df, expected_cols={"a", "b"})
        # Helper cast it to nullable string.
        assert df["b"].dtype == pd.StringDtype()
        assert df["b"].isna().all()

    def test_mixed_null_object_col_unchanged(self) -> None:
        """Columns with at least one non-null value are left alone."""
        df = pd.DataFrame({"a": [None, "value", None]})
        assert df["a"].dtype == object
        finalize_bronze_df(df, expected_cols={"a"})
        # Object dtype with real values can already be converted by Arrow,
        # so the helper doesn't force a cast.
        assert df["a"].tolist()[1] == "value"

    def test_all_none_col_cast_respects_override(self) -> None:
        df = pd.DataFrame({"a": [None, None, None]})
        assert df["a"].dtype == object
        finalize_bronze_df(
            df,
            expected_cols={"a"},
            dtype_overrides={"a": "Int64"},
        )
        assert df["a"].dtype == pd.Int64Dtype()

    def test_non_object_col_untouched(self) -> None:
        df = pd.DataFrame({"x": [1.0, 2.0], "y": [3, 4]})
        finalize_bronze_df(df, expected_cols={"x", "y"})
        assert df["x"].dtype.kind == "f"  # still float
        assert df["y"].dtype.kind == "i"  # still int


class TestEmptyDataFrame:
    def test_empty_df_gets_expected_cols(self) -> None:
        df = pd.DataFrame()
        finalize_bronze_df(df, expected_cols={"a", "b"})
        assert set(df.columns) == {"a", "b"}
        assert len(df) == 0
        assert df["a"].dtype == pd.StringDtype()


class TestReturnValue:
    def test_returns_same_df(self) -> None:
        """The helper modifies in place but also returns df for fluent use."""
        df = pd.DataFrame({"a": [1]})
        result = finalize_bronze_df(df, expected_cols={"a", "b"})
        assert result is df
        assert "b" in result.columns


class TestRealisticScenario:
    """Simulate the IDSSE events failure mode: per-match parse with sparse cols."""

    def test_sparse_match_gets_full_schema(self) -> None:
        # One match with only Play events — nutmeg_* / shot_* cols all None
        match_rows = [
            {
                "match_id": "m1",
                "event_id": "e1",
                "event_type": "Play",
                "play_player": "P1",
                "play_team": "home",
                "nutmeg_player": None,
                "shot_x_g": None,
            },
            {
                "match_id": "m1",
                "event_id": "e2",
                "event_type": "Play",
                "play_player": "P2",
                "play_team": "away",
                "nutmeg_player": None,
                "shot_x_g": None,
            },
        ]
        df = pd.DataFrame(match_rows)
        # Before helper: nutmeg_player and shot_x_g are object dtype (all None)
        assert df["nutmeg_player"].dtype == object
        assert df["shot_x_g"].dtype == object

        expected = {
            "match_id",
            "event_id",
            "event_type",
            "play_player",
            "play_team",
            "nutmeg_player",
            "shot_x_g",
            "pass_recipient",
            "cross_goal_keeper",  # cols from event types not in this match
        }
        finalize_bronze_df(
            df,
            expected_cols=expected,
            dtype_overrides={"shot_x_g": "Float64"},
        )

        # All expected cols now present with explicit dtype — no NullType risk.
        for col in expected:
            assert col in df.columns, f"missing {col}"
            # If the column is all-null, it must have a nullable dtype (not object).
            if df[col].isna().all():
                assert df[col].dtype != object, f"{col} still object dtype"

        assert df["shot_x_g"].dtype == pd.Float64Dtype()
