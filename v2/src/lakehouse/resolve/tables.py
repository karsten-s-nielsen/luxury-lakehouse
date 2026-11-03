"""The catalog tables of entity resolution (spec §11.6, §14.3, §15.5, §15.6; plan Task 22).

Every name is in the lake-root allowlist `{dim_*, *_links, ops_*, lineage_*}` of plan Task 23: the link tables are
`gold_<entity>_links`; the queue, holds, applied requests, canonical pointers, pointer moves and measured bands are
`ops_*` tables. They hold keys, native ids, scores and the operators' names, never event, tracking or lineup content.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

import duckdb

from lakehouse.catalog.duck import insert_rows
from lakehouse.resolve.model import Entity

__all__ = ["TABLES", "create", "insert", "link_table", "read_rows", "replace_rows", "table_exists"]

_LINK_COLUMNS: Final = (
    ("provider_a", "VARCHAR NOT NULL"),
    ("native_id_a", "VARCHAR NOT NULL"),
    ("key_a", "BIGINT NOT NULL"),
    ("provider_b", "VARCHAR NOT NULL"),
    ("native_id_b", "VARCHAR NOT NULL"),
    ("key_b", "BIGINT NOT NULL"),
    ("confidence", "DOUBLE NOT NULL"),
    ("layer", "INTEGER NOT NULL"),
    ("method", "VARCHAR NOT NULL"),
    ("accepted_by", "VARCHAR NOT NULL"),  # auto (the automatic band), hand, or recommended
    ("batch_id", "VARCHAR"),  # set for a link accepted as recommended
    ("decided_by", "VARCHAR"),  # the operator of a hand decision
    ("run_id", "VARCHAR NOT NULL"),
    ("linked_at", "TIMESTAMPTZ NOT NULL"),
)

TABLES: Final[dict[str, tuple[tuple[str, str], ...]]] = {
    "gold_player_links": _LINK_COLUMNS,
    "gold_team_links": _LINK_COLUMNS,
    "gold_competition_links": _LINK_COLUMNS,
    "gold_match_links": _LINK_COLUMNS,
    "ops_review_queue": (
        ("item_id", "VARCHAR NOT NULL"),
        ("entity", "VARCHAR NOT NULL"),
        ("provider_a", "VARCHAR NOT NULL"),
        ("native_id_a", "VARCHAR NOT NULL"),
        ("key_a", "BIGINT NOT NULL"),
        ("provider_b", "VARCHAR NOT NULL"),
        ("native_id_b", "VARCHAR NOT NULL"),
        ("key_b", "BIGINT NOT NULL"),
        ("score", "DOUBLE NOT NULL"),
        ("runner_up", "DOUBLE"),
        ("layer", "INTEGER NOT NULL"),
        ("method", "VARCHAR NOT NULL"),
        ("recommended", "BOOLEAN NOT NULL"),
        ("held", "BOOLEAN NOT NULL"),
        ("reason", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("queued_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_holds": (
        ("entity", "VARCHAR NOT NULL"),
        ("provider_a", "VARCHAR NOT NULL"),
        ("native_id_a", "VARCHAR NOT NULL"),
        ("provider_b", "VARCHAR NOT NULL"),
        ("native_id_b", "VARCHAR NOT NULL"),
        ("batch_id", "VARCHAR NOT NULL"),
        ("request_id", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("held_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_applied_requests": (
        ("request_id", "VARCHAR NOT NULL"),
        ("kind", "VARCHAR NOT NULL"),  # accept or undo
        ("batch_id", "VARCHAR NOT NULL"),
        ("links", "BIGINT NOT NULL"),
        ("applied_at", "TIMESTAMPTZ NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
    ),
    "ops_canonical_pointers": (
        ("entity", "VARCHAR NOT NULL"),
        ("provider", "VARCHAR NOT NULL"),
        ("native_id", "VARCHAR NOT NULL"),
        ("record_key", "BIGINT NOT NULL"),
        ("canonical_provider", "VARCHAR NOT NULL"),
        ("canonical_native_id", "VARCHAR NOT NULL"),
        ("canonical_key", "BIGINT NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("updated_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_pointer_moves": (
        ("entity", "VARCHAR NOT NULL"),
        ("provider", "VARCHAR NOT NULL"),
        ("native_id", "VARCHAR NOT NULL"),
        ("record_key", "BIGINT NOT NULL"),
        ("old_canonical_key", "BIGINT NOT NULL"),
        ("new_canonical_key", "BIGINT NOT NULL"),
        ("reason", "VARCHAR NOT NULL"),
        ("run_id", "VARCHAR NOT NULL"),
        ("moved_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_entity_records": (
        ("entity", "VARCHAR NOT NULL"),
        ("provider", "VARCHAR NOT NULL"),
        ("native_id", "VARCHAR NOT NULL"),
        ("record_key", "BIGINT NOT NULL"),
        ("name", "VARCHAR NOT NULL"),
        ("short_name", "VARCHAR"),
        ("birth_date", "DATE"),
        ("team", "VARCHAR"),
        ("jersey", "VARCHAR"),
        ("position", "VARCHAR"),
        ("area", "VARCHAR"),
        ("matches", "BIGINT"),
        ("match_date", "DATE"),
        ("home_team_key", "BIGINT"),
        ("away_team_key", "BIGINT"),
        ("native_competition_id", "VARCHAR"),
        ("competition_key", "BIGINT"),
        ("native_season_id", "VARCHAR"),
        ("season_key", "BIGINT"),
        ("season_name", "VARCHAR"),
        ("run_id", "VARCHAR NOT NULL"),
        ("updated_at", "TIMESTAMPTZ NOT NULL"),
    ),
    "ops_resolution_bands": (
        ("entity", "VARCHAR NOT NULL"),
        ("auto_link_min", "DOUBLE NOT NULL"),
        ("reject_max", "DOUBLE NOT NULL"),
        ("margin", "DOUBLE NOT NULL"),
        ("truthset_rows", "BIGINT NOT NULL"),
        ("measured_by", "VARCHAR NOT NULL"),
        ("measured_at", "TIMESTAMPTZ NOT NULL"),
    ),
}


def link_table(entity: Entity) -> str:
    return f"gold_{entity}_links"


def table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    found = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = ?",
        [table],
    ).fetchone()
    return found is not None and found[0] > 0


def create(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    columns = ", ".join(f"{name} {kind}" for name, kind in TABLES[table])
    conn.execute(f"CREATE TABLE IF NOT EXISTS lake.main.{table} ({columns})")


def insert(conn: duckdb.DuckDBPyConnection, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
    create(conn, table)
    if not rows:
        return
    names = [name for name, _ in TABLES[table]]
    # one statement, so one file per write on the encrypted-local path, not one per row (T3 spec D6)
    insert_rows(conn, table, names, [[row[name] for name in names] for row in rows])


def replace_rows(
    conn: duckdb.DuckDBPyConnection, table: str, rows: Sequence[Mapping[str, Any]], *, entity: Entity | None = None
) -> None:
    """Replace the rows of `table` (only those of `entity` when the table has an `entity` column and one is given)."""
    create(conn, table)
    if entity is None:
        conn.execute(f"DELETE FROM lake.main.{table}")  # noqa: S608 - a constant table name
    else:
        conn.execute(f"DELETE FROM lake.main.{table} WHERE entity = ?", [entity])  # noqa: S608 - a constant table name
    insert(conn, table, rows)


def read_rows(conn: duckdb.DuckDBPyConnection, table: str, *, entity: Entity | None = None) -> list[dict[str, Any]]:
    """Every row of `table` (of `entity` when given); `[]` before the table exists."""
    if table not in TABLES:
        raise ValueError(f"unknown resolution table {table!r}")
    if not table_exists(conn, table):
        return []
    names = [name for name, _ in TABLES[table]]
    sql = f"SELECT {', '.join(names)} FROM lake.main.{table}"  # noqa: S608 - constant names
    cursor = conn.execute(f"{sql} WHERE entity = ?", [entity]) if entity is not None else conn.execute(sql)
    return [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
