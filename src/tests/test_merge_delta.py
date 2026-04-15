"""Tests for ingestion.utils.merge_delta_table."""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest

from ingestion.utils import merge_delta_table


def _mock_df(columns: list[str], count: int = 5) -> MagicMock:
    """Create a mock Spark DataFrame with given columns."""
    df = MagicMock()
    df.columns = columns
    df.count.return_value = count
    df.alias.return_value = df
    # add_audit_columns returns a new df with _ingested_at
    df.withColumn.return_value = df
    df.sparkSession = MagicMock()
    # write chain for fallback path
    writer = MagicMock()
    df.write.format.return_value = writer
    writer.option.return_value = writer
    writer.mode.return_value = writer
    return df


@pytest.fixture(autouse=True)
def _mock_delta_module() -> Iterator[MagicMock]:
    """Inject a mock delta.tables module into sys.modules.

    The ``merge_delta_table`` function uses a lazy ``from delta.tables
    import DeltaTable`` inside the function body. Since ``delta`` is not
    installed locally (Databricks-only), we inject a mock module so the
    import resolves.
    """
    mock_dt = MagicMock()
    delta_mod = types.ModuleType("delta")
    delta_tables_mod = types.ModuleType("delta.tables")
    delta_tables_mod.DeltaTable = mock_dt  # type: ignore[attr-defined]
    delta_mod.tables = delta_tables_mod  # type: ignore[attr-defined]

    with patch.dict(sys.modules, {"delta": delta_mod, "delta.tables": delta_tables_mod}):
        yield mock_dt


class TestMergeDeltaTable:
    """Tests for the merge_delta_table utility."""

    def test_invalid_table_name_raises(self) -> None:
        """Table names with special characters are rejected."""
        df = _mock_df(["event_id", "value"])
        with pytest.raises(ValueError, match="Invalid table_name"):
            merge_delta_table(df, "cat", "schema", "bad;table", merge_key="event_id")

    @patch("ingestion.utils.add_audit_columns")
    def test_fallback_to_overwrite_when_table_missing(
        self,
        mock_audit: MagicMock,
        _mock_delta_module: MagicMock,
    ) -> None:
        """Falls back to overwrite when target table doesn't exist.

        Uses a realistic Spark error message so ``tolerate_missing_table``
        suppresses it. Non-table-missing errors now propagate (regression
        guard for the 2026-04-14 silent-swallow remediation).
        """
        df = _mock_df(["event_id", "value"])
        mock_audit.return_value = df
        _mock_delta_module.forName.side_effect = Exception(
            "[TABLE_OR_VIEW_NOT_FOUND] Table `cat`.`schema`.`test_table` not found"
        )

        result = merge_delta_table(df, "cat", "schema", "test_table", merge_key="event_id")

        assert result == 5
        df.write.format.assert_called_once_with("delta")

    @patch("ingestion.utils.add_audit_columns")
    def test_propagates_non_missing_table_errors(
        self,
        mock_audit: MagicMock,
        _mock_delta_module: MagicMock,
    ) -> None:
        """Permission errors and schema mismatches must NOT be suppressed.

        Regression guard: the old bare ``except Exception:`` would have
        treated a permission-denied error as "table missing" and then
        attempted an overwrite, masking the real problem.
        """
        df = _mock_df(["event_id", "value"])
        mock_audit.return_value = df
        _mock_delta_module.forName.side_effect = PermissionError("access denied")

        with pytest.raises(PermissionError, match="access denied"):
            merge_delta_table(df, "cat", "schema", "test_table", merge_key="event_id")

    @patch("ingestion.utils.add_audit_columns")
    def test_merge_called_on_existing_table(
        self,
        mock_audit: MagicMock,
        _mock_delta_module: MagicMock,
    ) -> None:
        """MERGE is executed when the target table exists."""
        df = _mock_df(["event_id", "value", "_ingested_at"])
        mock_audit.return_value = df

        mock_target = MagicMock()
        mock_merge_builder = MagicMock()
        mock_target.alias.return_value.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
        _mock_delta_module.forName.return_value = mock_target

        result = merge_delta_table(df, "cat", "schema", "test_table", merge_key="event_id")

        assert result == 5
        mock_target.alias.assert_called_with("target")
        mock_merge_builder.execute.assert_called_once()

    @patch("ingestion.utils.add_audit_columns")
    def test_merge_key_used_in_condition(
        self,
        mock_audit: MagicMock,
        _mock_delta_module: MagicMock,
    ) -> None:
        """MERGE condition references the correct merge key column."""
        df = _mock_df(["my_key", "data"])
        mock_audit.return_value = df

        mock_target = MagicMock()
        mock_alias = MagicMock()
        mock_target.alias.return_value = mock_alias
        mock_merge_builder = MagicMock()
        mock_alias.merge.return_value = mock_merge_builder
        mock_merge_builder.whenMatchedUpdateAll.return_value = mock_merge_builder
        mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
        _mock_delta_module.forName.return_value = mock_target

        merge_delta_table(df, "cat", "schema", "tbl", merge_key="my_key")

        # Verify the merge condition string
        call_args = mock_alias.merge.call_args
        condition = call_args[0][1]  # second positional arg
        assert "target.my_key = source.my_key" in condition

    @patch("ingestion.utils.add_audit_columns")
    def test_returns_row_count(
        self,
        mock_audit: MagicMock,
        _mock_delta_module: MagicMock,
    ) -> None:
        """Return value matches the source DataFrame row count."""
        df = _mock_df(["event_id"], count=42)
        mock_audit.return_value = df
        _mock_delta_module.forName.side_effect = Exception("[TABLE_OR_VIEW_NOT_FOUND] no table")

        result = merge_delta_table(df, "cat", "schema", "tbl", merge_key="event_id")

        assert result == 42
