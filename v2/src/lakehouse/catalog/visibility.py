"""The encrypted-local `_visibility` audit column and its native re-apply (spec S1 approach A-prime, plan T9-T12).

On an encrypted-local lake a bronze match row carries `_visibility` — the authoritative restricted copy. The
column name and the per-match `UPDATE` live here, in the catalog layer, so `restore` (also catalog) can re-apply a
visibility after a data rollback without importing the run engine, which would reverse the import-linter layering.
The run engine's native writer and move seam, and the write-side guard, import the name from here too.
"""

from __future__ import annotations

from typing import Literal

import duckdb

__all__ = ["VISIBILITY_COLUMN", "reapply_visibility", "visibility_tables"]

VISIBILITY_COLUMN = "_visibility"


def visibility_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """The `lake.main` bronze tables that carry BOTH `_visibility` and `match_key` (the native match tables). A
    listing table (no `match_key`) and an `ops_*` table (`visibility`, not `_visibility`) are excluded by the
    column filter. Run on a child cursor so the caller's connection keeps no open result."""
    cursor = conn.cursor()
    try:
        rows = cursor.execute(
            "SELECT table_name FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = 'main' "
            "AND column_name IN (?, 'match_key') GROUP BY table_name HAVING count(DISTINCT column_name) = 2",
            [VISIBILITY_COLUMN],
        ).fetchall()
        return [str(row[0]) for row in rows]
    finally:
        cursor.close()


def reapply_visibility(
    conn: duckdb.DuckDBPyConnection, match_key: int, visibility: Literal["public", "private"]
) -> None:
    """Set `_visibility` for a match's rows in every native bronze table. The move seam calls it on a flip (plan
    T9); `restore` calls it to re-apply the latest visibility after a data rollback (plan T12)."""
    for table in visibility_tables(conn):
        conn.execute(
            f'UPDATE lake.main."{table}" SET {VISIBILITY_COLUMN} = ? WHERE match_key = ?',  # noqa: S608 - bronze_table + constant column
            [visibility, match_key],
        )
