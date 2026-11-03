"""Additive bronze schema evolution (audit T9).

A source field that no parser maps fails loudly (spec §12.3) until a parser maps it. The new column must then land on
a lake that already holds the table, on both write paths: `ducklake_add_data_files` refuses a file with a column the
table lacks, and a positional `INSERT` would misplace it. `reconcile_columns` runs inside each write's transaction
for a table that existed before it, so a failed write rolls the change back:

- a column that the write brings and the table lacks is added as a nullable column (`ALTER TABLE ... ADD COLUMN`); the
  rows written before read null, which is what they held;
- a shared column whose type differs raises `IntegrityError`: a type change is not additive and needs a migration;
- a table column that a fresh write no longer carries raises `IntegrityError` (a parser that drops a field), unless the
  write re-registers copies of files that the table already holds (a visibility move), which predate the column.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import duckdb

from lakehouse.bronze.schemas import BronzeTable
from lakehouse.errors import IntegrityError
from lakehouse.logging import get_logger

__all__ = ["reconcile_columns", "relation_columns", "table_columns"]

_log = get_logger(__name__)


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_columns(conn: duckdb.DuckDBPyConnection, table: BronzeTable) -> dict[str, str]:
    """The table's columns in order, each with its DuckDB type."""
    rows = conn.execute(
        "SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = 'lake' AND schema_name = 'main' "
        "AND table_name = ? ORDER BY column_index",
        [table],
    ).fetchall()
    return {str(name): str(kind) for name, kind in rows}


def relation_columns(conn: duckdb.DuckDBPyConnection, query: str, params: Sequence[object] = ()) -> dict[str, str]:
    """The columns of `query` in order, each with the DuckDB type a write of it would bring (`DESCRIBE`)."""
    rows = conn.execute(f"DESCRIBE {query}", list(params)).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def reconcile_columns(
    conn: duckdb.DuckDBPyConnection,
    table: BronzeTable,
    incoming: Mapping[str, str],
    *,
    narrower_ok: bool = False,
) -> tuple[str, ...]:
    """Make the existing `table` admit a write with the `incoming` columns (see the module docstring); return the
    names of the columns it added. `narrower_ok` is for copies of files the table already holds."""
    have = table_columns(conn, table)
    for name, kind in incoming.items():
        if name in have and have[name] != kind:
            raise IntegrityError(
                f"bronze column {name} of {table} is {have[name]}; the write brings {kind}: a type change is not "
                "additive and needs a migration"
            )
    if not narrower_ok:
        for name in have:
            if name not in incoming:
                raise IntegrityError(
                    f"the write no longer carries column {name} of {table}: dropping a bronze column is not additive "
                    "and needs a migration"
                )
    added = tuple(name for name in incoming if name not in have)
    for name in added:
        conn.execute(f'ALTER TABLE lake.main."{table}" ADD COLUMN {_quoted(name)} {incoming[name]}')
    if added:
        _log.info("bronze table evolved", extra={"table": table, "added_columns": list(added)})
    return added
