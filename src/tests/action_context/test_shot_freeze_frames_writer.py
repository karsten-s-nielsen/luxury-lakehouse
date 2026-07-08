"""Schema-drift guard for the ``bronze.shot_freeze_frames`` writer (Task 0.5, ADR-002 §4).

The persisted per-(shot, player) freeze-frame set is emitted by
``build_tracking_snapshots`` (Task 0.4) and written to ``bronze.shot_freeze_frames`` by the
Spark writer in ``analytics.action_context.tracking_snapshots``. This test pins the writer's
declared column list (``_SHOT_FF_COLUMNS``) against the canonical ``CREATE TABLE`` DDL in
``scripts/migrations/2026-07-05-shot-freeze-frames-ddl.sql`` so the two sources of truth cannot
drift — the exact ADR-002 §4 pattern used by ``cost_hook._COST_LIVE_COLUMNS`` and
``xg_model_v2._XG_V2_BRONZE_COLS``.

``_ingested_at`` is appended by ``write_delta_table`` (NOT emitted by the builder), so it lives
in the DDL but must be absent from ``_SHOT_FF_COLUMNS``.
"""

from __future__ import annotations

import re
from pathlib import Path

from analytics.action_context.tracking_snapshots import _SHOT_FF_COLUMNS, _SHOT_FF_TYPES

_DDL_PATH = Path("scripts/migrations/2026-07-05-shot-freeze-frames-ddl.sql")

# The single audit column the writer appends; present in the DDL, absent from the constant.
_WRITER_ADDED_COLUMN = "_ingested_at"


def _parse_ddl_columns() -> list[str]:
    """Extract ordered column names from the CREATE TABLE block in the migration DDL."""
    sql = _DDL_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"CREATE TABLE[^(]*\(\s*(.*?)\s*\)\s*USING",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    assert match, f"Could not find CREATE TABLE ... USING block in {_DDL_PATH}"
    columns_block = match.group(1)

    columns: list[str] = []
    for raw_line in columns_block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        # First token is the column name.
        columns.append(line.split()[0].strip())
    return columns


class TestShotFreezeFramesSchemaDriftGuard:
    """``_SHOT_FF_COLUMNS`` must match the persisted DDL exactly (minus ``_ingested_at``)."""

    def test_ddl_file_exists(self) -> None:
        assert _DDL_PATH.is_file(), f"Migration DDL missing: {_DDL_PATH}"

    def test_columns_match_ddl_names_and_order(self) -> None:
        ddl_cols = _parse_ddl_columns()
        # The writer emits every DDL column except the writer-added audit column, in the same order.
        expected = [c for c in ddl_cols if c != _WRITER_ADDED_COLUMN]
        assert list(_SHOT_FF_COLUMNS) == expected, (
            "Schema drift between shot_freeze_frames DDL and _SHOT_FF_COLUMNS.\n"
            f"  DDL (minus {_WRITER_ADDED_COLUMN}): {expected}\n"
            f"  _SHOT_FF_COLUMNS:                  {list(_SHOT_FF_COLUMNS)}\n"
            f"  In DDL but not constant: {set(expected) - set(_SHOT_FF_COLUMNS)}\n"
            f"  In constant but not DDL: {set(_SHOT_FF_COLUMNS) - set(expected)}"
        )

    def test_ingested_at_in_ddl_but_not_constant(self) -> None:
        ddl_cols = _parse_ddl_columns()
        assert _WRITER_ADDED_COLUMN in ddl_cols, f"{_WRITER_ADDED_COLUMN} must be declared in the DDL"
        assert _WRITER_ADDED_COLUMN not in _SHOT_FF_COLUMNS, (
            f"{_WRITER_ADDED_COLUMN} is writer-added and must NOT appear in _SHOT_FF_COLUMNS"
        )

    def test_ingested_at_is_last_ddl_column(self) -> None:
        ddl_cols = _parse_ddl_columns()
        assert ddl_cols[-1] == _WRITER_ADDED_COLUMN, (
            f"{_WRITER_ADDED_COLUMN} must be the LAST DDL column (write_delta_table appends it)"
        )

    def test_no_duplicate_columns(self) -> None:
        assert len(_SHOT_FF_COLUMNS) == len(set(_SHOT_FF_COLUMNS)), "_SHOT_FF_COLUMNS has duplicates"

    def test_access_tier_is_last_data_column(self) -> None:
        # ``access_tier`` (ADR-064) is the driver-stamped per-match tier used by the downstream HF
        # publisher to split public vs restricted rows. It is the LAST data column (immediately
        # before the writer-added ``_ingested_at`` in the DDL), so it must be the last entry of
        # ``_SHOT_FF_COLUMNS`` and typed ``string``.
        assert "access_tier" in _SHOT_FF_COLUMNS, "access_tier must be a persisted column"
        assert _SHOT_FF_COLUMNS[-1] == "access_tier", "access_tier must be the LAST _SHOT_FF_COLUMNS entry"
        assert _SHOT_FF_TYPES["access_tier"] == "string", "access_tier must be a STRING column"


class TestShotFreezeFramesStructType:
    """The lazy StructType factory must cover exactly ``_SHOT_FF_COLUMNS`` (pyspark permitting)."""

    def test_struct_type_matches_columns(self) -> None:
        import importlib.util

        if importlib.util.find_spec("pyspark") is None:
            import pytest

            pytest.skip("pyspark not installed — StructType factory covered in the Databricks runtime")

        from analytics.action_context.tracking_snapshots import _shot_ff_struct_type

        struct = _shot_ff_struct_type()
        assert [f.name for f in struct.fields] == list(_SHOT_FF_COLUMNS)
