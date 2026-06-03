"""Schema drift guard (P5): the work-queue migration DDL must match _QUEUE_COLUMNS.

Offline — both sides are pyspark-free (the column list is a plain tuple list, and the
migration is parsed as text), so this runs in CI without a Spark runtime.
"""

from __future__ import annotations

from pathlib import Path

from ingestion.action_context_queue import _QUEUE_COLUMNS

_MIGRATION = (
    Path(__file__).resolve().parents[3] / "scripts" / "migrations" / "2026-06-02-create-action-context-work-queue.sql"
)


def _ddl_columns(sql: str) -> list[tuple[str, str]]:
    # Drop comment lines FIRST so a "(" inside a comment (e.g. "(ADR-037)") can't fool
    # the paren scan that locates the column list.
    sql = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    body = sql[sql.index("(") + 1 : sql.rindex(")")]
    cols: list[tuple[str, str]] = []
    for raw in body.splitlines():
        line = raw.strip().rstrip(",")
        if not line:
            continue
        name, sql_type = line.split(None, 1)
        cols.append((name, sql_type.lower()))
    return cols


def test_migration_ddl_matches_queue_columns() -> None:
    ddl = _ddl_columns(_MIGRATION.read_text(encoding="utf-8"))
    expected = [(name, sql_type) for name, sql_type, _ in _QUEUE_COLUMNS]
    expected.append(("_ingested_at", "timestamp"))  # auto-added by write_delta_table
    assert ddl == expected, (
        "migration DDL drifted from _QUEUE_COLUMNS. Use simpleString spellings in the .sql: "
        "int (not integer), bigint (not long), double (not 'double precision'), string, timestamp; "
        f"\nDDL={ddl}\nEXPECTED={expected}"
    )
