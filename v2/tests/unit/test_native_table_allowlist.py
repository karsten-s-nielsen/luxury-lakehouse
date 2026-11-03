"""Every table that DuckDB itself writes into the lake root has a name in the allowlist (spec §9 "Row-level data
never enters a lake-root table"; plan Task 23): `dim_*`, `*_links`, `ops_*`, `lineage_*`. The e2e test
`test_no_private_rows_in_lake_native_files` checks the written files; this test checks every name the code defines,
so a new native table with another name fails at once."""

from __future__ import annotations

import fnmatch

from lakehouse.catalog.moves import MOVES_TABLE
from lakehouse.catalog.restore import RESTORES_TABLE
from lakehouse.lineage.model import LINEAGE_TABLE
from lakehouse.resolve.tables import TABLES as RESOLUTION_TABLES
from lakehouse.runs.dbt import NATIVE_TABLE_ALLOWLIST
from lakehouse.runs.history import TABLES as HISTORY_TABLES


def _allowed(name: str) -> bool:
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in NATIVE_TABLE_ALLOWLIST)


def test_allowlist_is_the_spec_list() -> None:
    assert NATIVE_TABLE_ALLOWLIST == ("dim_*", "*_links", "ops_*", "lineage_*")


def test_every_native_table_name_is_allowed() -> None:
    names = {*HISTORY_TABLES, *RESOLUTION_TABLES, MOVES_TABLE, RESTORES_TABLE, LINEAGE_TABLE}
    assert sorted(name for name in names if not _allowed(name)) == []
