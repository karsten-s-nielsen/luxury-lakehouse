"""Tests for FilterResult and SkipGuard protocol."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from ingestion.guards import FilterResult, ensure_table, find_new_ids


class TestFilterResult:
    """FilterResult is a frozen dataclass."""

    def test_skip_when_count_zero(self) -> None:
        """count=0 signals the freshness gate to skip this workflow."""
        result = FilterResult(workflow_id="wf-spadl", count=0)
        assert result.count == 0
        assert result.chunks is None
        assert result.metadata == {}

    def test_single_task_when_no_chunks(self) -> None:
        """count>0 with chunks=None means single-task execution."""
        result = FilterResult(workflow_id="wf-xg", count=5)
        assert result.count == 5
        assert result.chunks is None

    def test_fan_out_with_chunks(self) -> None:
        """chunks list triggers for_each_task fan-out."""
        chunks = [["m1", "m2"], ["m3", "m4"], ["m5"]]
        result = FilterResult(workflow_id="wf-pausa", count=5, chunks=chunks)
        assert result.chunks is not None
        assert len(result.chunks) == 3
        assert result.chunks[0] == ["m1", "m2"]

    def test_metadata_passthrough(self) -> None:
        """Arbitrary metadata dict is preserved for downstream pipelines."""
        meta = {"need_global": True, "competitions": [43, 11]}
        result = FilterResult(workflow_id="wf-defcon", count=12, metadata=meta)
        assert result.metadata["need_global"] is True
        assert result.metadata["competitions"] == [43, 11]

    def test_frozen_immutability(self) -> None:
        """Frozen dataclass prevents accidental mutation."""
        result = FilterResult(workflow_id="wf-spadl", count=3)
        with pytest.raises(FrozenInstanceError):
            result.count = 10  # type: ignore[misc]


class TestEnsureTable:
    """ensure_table issues CREATE TABLE IF NOT EXISTS DDL."""

    def test_creates_table_with_delta_and_properties(self) -> None:
        """Verify the DDL includes table name, schema, USING DELTA, and tblproperties."""
        spark = MagicMock()
        ensure_table(spark, "cat.schema.my_table", "match_id STRING, value DOUBLE")
        spark.sql.assert_called_once()
        sql = spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS cat.schema.my_table" in sql
        assert "match_id STRING, value DOUBLE" in sql
        assert "USING DELTA" in sql
        assert "delta.autoOptimize.autoCompact" in sql
        assert "delta.autoOptimize.optimizeWrite" in sql

    def test_is_idempotent(self) -> None:
        """Calling twice issues SQL twice (metadata-only no-op on second call)."""
        spark = MagicMock()
        ensure_table(spark, "cat.schema.t", "id STRING")
        ensure_table(spark, "cat.schema.t", "id STRING")
        assert spark.sql.call_count == 2


def _row(match_id: str) -> dict[str, str]:
    """Simulate a Spark Row with the join alias used by find_new_ids."""
    return {"_join_id": match_id}


class TestPitchControlGuard:
    """Pitch control skip guard adapter — delegates to find_new_ids()."""

    def test_returns_skip_when_all_processed(self) -> None:
        from ingestion.pitch_control_batch import skip_guard

        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        # Anti-join returns empty — all matches processed
        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = []
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if "fct_tracking_frames" in name:
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 0
        assert result.workflow_id == "wf-pitch-control"

    def test_returns_new_matches_no_fanout(self) -> None:
        """Two new matches — below chunk threshold, no fan-out."""
        from ingestion.pitch_control_batch import skip_guard

        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        # Anti-join returns 2 new matches
        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [_id_row("m2"), _id_row("m3")]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if "fct_tracking_frames" in name:
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 2
        assert result.chunks is None  # Only 1 chunk of 2 — no fan-out
        assert set(result.metadata["new_match_ids"]) == {"m2", "m3"}

    def test_returns_chunks_for_fanout(self) -> None:
        """Five new matches at 2/chunk = 3 chunks — fan-out."""
        from ingestion.pitch_control_batch import skip_guard

        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        # Anti-join returns 5 new matches
        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [_id_row(f"m{i}") for i in range(2, 7)]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if "fct_tracking_frames" in name:
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 5
        assert result.chunks is not None
        assert len(result.chunks) == 3  # [m2,m3], [m4,m5], [m6]
        assert len(result.chunks[0]) == 2
        assert len(result.chunks[2]) == 1  # Last chunk may be smaller

    def test_returns_all_when_results_table_empty(self) -> None:
        """Guard uses ensure_table, so results table always exists.

        After ``ensure_table`` creates an empty table, ``find_new_ids``
        returns all source IDs because the anti-join finds no matches.
        """
        from ingestion.pitch_control_batch import skip_guard

        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([_id_row("m1"), _id_row("m2")])
        # Empty results table — anti-join returns all source IDs
        empty_results_df = _make_chainable_df([])
        # Anti-join of source against empty results returns all source rows
        source_df.join.return_value = source_df

        def table_side_effect(name: str) -> MagicMock:
            if "fct_tracking_frames" in name:
                return source_df
            return empty_results_df

        spark.table.side_effect = table_side_effect

        result = skip_guard.check(spark, "soccer_analytics", "bronze")
        assert result.count == 2
        assert result.chunks is None  # Only 1 chunk of 2 — no fan-out


def _id_row(value: str) -> dict[str, str]:
    """Simulate a Spark Row with the join alias used by find_new_ids."""
    return {"_join_id": value}


def _make_chainable_df(collect_rows: list[dict[str, str]]) -> MagicMock:
    """Build a mock DataFrame where filter/select/distinct/join all chain and collect returns rows."""
    mock_df = MagicMock()
    # Make filter/select/distinct return self for chaining
    mock_df.filter.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.distinct.return_value = mock_df
    mock_df.collect.return_value = collect_rows
    return mock_df


def _mock_pyspark_functions() -> MagicMock:
    """Create a mock pyspark.sql.functions module and register it in sys.modules.

    Returns the mock ``functions`` module so tests can assert on ``F.col()`` calls.
    Must be called before ``find_new_ids()`` because the function uses a local
    ``from pyspark.sql import functions as F`` that resolves via ``sys.modules``.
    """
    import sys

    mock_functions = MagicMock()
    # Ensure pyspark module hierarchy exists in sys.modules
    if "pyspark" not in sys.modules:
        sys.modules["pyspark"] = MagicMock()
    mock_sql = MagicMock()
    # ``from pyspark.sql import functions`` resolves via getattr on the
    # pyspark.sql module object, so we must set the attribute explicitly.
    mock_sql.functions = mock_functions
    sys.modules["pyspark.sql"] = mock_sql
    sys.modules["pyspark.sql.functions"] = mock_functions
    return mock_functions


class TestFindNewIds:
    """Tests for the Spark-native LEFT ANTI JOIN helper."""

    def test_basic_anti_join(self) -> None:
        """5 source IDs, 3 in results -> returns 2 new."""
        mock_f = _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])  # collect not called on source directly
        results_df = _make_chainable_df([])

        # The anti-join result contains only the new IDs
        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [_id_row("m4"), _id_row("m5")]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(spark, "catalog.schema.source", "catalog.schema.results")

        assert sorted(result) == ["m4", "m5"]
        source_df.join.assert_called_once_with(results_df, on="_join_id", how="left_anti")
        mock_f.col.assert_called_with("match_id")

    def test_missing_results_table(self) -> None:
        """Results table raises Exception -> propagates to caller.

        Since the new contract requires callers to use ``ensure_table``
        before ``find_new_ids``, a missing results table is an error.
        """
        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([_id_row("m1"), _id_row("m2"), _id_row("m3")])

        call_count = 0

        def table_side_effect(name: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return source_df
            raise Exception("Table not found")

        spark.table.side_effect = table_side_effect

        with pytest.raises(Exception, match="Table not found"):
            find_new_ids(spark, "catalog.schema.source", "catalog.schema.results")

    def test_empty_source(self) -> None:
        """Source has 0 rows -> returns empty list."""
        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = []
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(spark, "catalog.schema.source", "catalog.schema.results")

        assert result == []

    def test_all_already_processed(self) -> None:
        """Source IDs == results IDs -> returns empty list."""
        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = []
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(spark, "catalog.schema.source", "catalog.schema.results")

        assert result == []
        source_df.join.assert_called_once()

    def test_with_source_filter(self) -> None:
        """Verify .filter() called on source DataFrame when source_filter provided."""
        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [_id_row("m1")]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(
            spark, "catalog.schema.source", "catalog.schema.results", source_filter="competition_id = 43"
        )

        assert result == ["m1"]
        source_df.filter.assert_called_once_with("competition_id = 43")

    def test_with_results_filter(self) -> None:
        """Verify .filter() called on results DataFrame when results_filter provided."""
        _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [_id_row("m2")]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(
            spark, "catalog.schema.source", "catalog.schema.results", results_filter="status = 'complete'"
        )

        assert result == ["m2"]
        results_df.filter.assert_called_once_with("status = 'complete'")

    def test_custom_id_column(self) -> None:
        """Pass id_column='competition_id', verify it's used in select and join."""
        mock_f = _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [_id_row("43")]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(spark, "catalog.schema.source", "catalog.schema.results", id_column="competition_id")

        assert result == ["43"]
        # Verify F.col was called with the custom column name
        mock_f.col.assert_called_with("competition_id")
        # Verify join uses the join alias
        source_df.join.assert_called_once_with(results_df, on="_join_id", how="left_anti")

    def test_results_id_column_different_from_source(self) -> None:
        """results_id_column allows different column names in source vs results (e.g., matchId vs match_id)."""
        mock_f = _mock_pyspark_functions()
        spark = MagicMock()

        source_df = _make_chainable_df([])
        results_df = _make_chainable_df([])

        anti_join_df = MagicMock()
        anti_join_df.collect.return_value = [{"_join_id": "99"}]
        source_df.join.return_value = anti_join_df

        def table_side_effect(name: str) -> MagicMock:
            if name == "catalog.schema.source":
                return source_df
            return results_df

        spark.table.side_effect = table_side_effect

        result = find_new_ids(
            spark,
            "catalog.schema.source",
            "catalog.schema.results",
            id_column="matchId",
            results_id_column="match_id",
        )

        assert result == ["99"]
        # F.col should be called with both column names (source first, then results)
        col_calls = [str(c) for c in mock_f.col.call_args_list]
        assert any("matchId" in c for c in col_calls), f"Expected matchId in col calls: {col_calls}"
        assert any("match_id" in c for c in col_calls), f"Expected match_id in col calls: {col_calls}"
        # Join uses the shared alias
        source_df.join.assert_called_once_with(results_df, on="_join_id", how="left_anti")
