"""The shared parser column helpers (plan Tasks 15-20)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from lakehouse.parsers.columns import append_columns


def test_append_columns_refuses_a_duplicate_name() -> None:
    """A second column with the same name would make two `match_key` columns; it fails instead."""
    table = pa.table({"match_key": pa.array([1], pa.int64())})
    with pytest.raises(ValueError, match="match_key"):
        append_columns(table, {"match_key": pa.array([2], pa.int64())})
    assert append_columns(table, {"team_key": pa.array([3], pa.int64())}).column_names == ["match_key", "team_key"]
