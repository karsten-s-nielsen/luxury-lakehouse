"""Schema drift guard (P5): the work-queue migration DDL must match _QUEUE_COLUMNS.

Offline — both sides are pyspark-free (the column list is a plain tuple list, and the
migration is parsed as text), so this runs in CI without a Spark runtime.
"""

from __future__ import annotations

from pathlib import Path

from ingestion.action_context_queue import _QUEUE_COLUMNS
from tests._ddl import ddl_columns  # W5: SHARED parser — the D9 event parity test uses the same one

_MIGRATION = (
    Path(__file__).resolve().parents[3] / "scripts" / "migrations" / "2026-06-02-create-action-context-work-queue.sql"
)


def test_migration_ddl_matches_queue_columns() -> None:
    ddl = ddl_columns(_MIGRATION.read_text(encoding="utf-8"))
    expected = [(name, sql_type) for name, sql_type, _ in _QUEUE_COLUMNS]
    expected.append(("_ingested_at", "timestamp"))  # auto-added by write_delta_table
    assert ddl == expected, (
        "migration DDL drifted from _QUEUE_COLUMNS. Use simpleString spellings in the .sql: "
        "int (not integer), bigint (not long), double (not 'double precision'), string, timestamp; "
        f"\nDDL={ddl}\nEXPECTED={expected}"
    )
