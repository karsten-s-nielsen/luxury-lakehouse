"""An in-memory `lake` catalog with minimal bronze tables for the resolution tests (plan Task 22).

The tables carry the real bronze column names that the resolution queries read (a subset of each table); the
contract test `tests/contract/test_resolution_records.py` runs the same queries on the real parser output.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import duckdb

from lakehouse.config.model import Settings, load_defaults
from lakehouse.ids.keys import team_key
from lakehouse.ids.native import Provider
from lakehouse.resolve.model import RecordRef

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def lake() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS lake")
    return conn


def _insert(conn: duckdb.DuckDBPyConnection, table: str, columns: str, rows: Sequence[Sequence[Any]]) -> None:
    conn.execute(f"CREATE TABLE IF NOT EXISTS lake.main.{table} ({columns})")
    if rows:
        marks = ", ".join("?" for _ in rows[0])
        sql = f"INSERT INTO lake.main.{table} VALUES ({marks})"  # noqa: S608 - a test table name and placeholders
        conn.executemany(sql, [list(row) for row in rows])


def statsbomb_teams(conn: duckdb.DuckDBPyConnection, teams: Sequence[tuple[int, str]]) -> None:
    _insert(
        conn,
        "statsbomb_lineup_teams",
        "team_id BIGINT, team_name VARCHAR, match_key BIGINT, native_match_id VARCHAR",
        [(team_id, name, 1, "1") for team_id, name in teams],
    )


def wyscout_teams(conn: duckdb.DuckDBPyConnection, teams: Sequence[tuple[int, str]]) -> None:
    _insert(
        conn,
        "wyscout_teams",
        "team_id BIGINT, name VARCHAR, official_name VARCHAR, area_name VARCHAR",
        [(team_id, name, name, None) for team_id, name in teams],
    )


def idsse_teams(conn: duckdb.DuckDBPyConnection, teams: Sequence[tuple[str, str]]) -> None:
    _insert(
        conn,
        "idsse_teams",
        "team_id VARCHAR, team_name VARCHAR, match_key BIGINT, native_match_id VARCHAR",
        [(team_id, name, 1, "J03WPY") for team_id, name in teams],
    )


def settings(*, auto_accept: bool) -> Settings:
    return load_defaults("laptop").model_copy(update={"auto_accept_recommended": auto_accept})


def team(provider: Provider, native: str) -> RecordRef:
    return RecordRef(provider, native)


def key(ref: RecordRef) -> int:
    found = team_key(ref.provider, ref.native_id)
    assert found is not None
    return found


def rows(conn: duckdb.DuckDBPyConnection, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, list(params))
    names = [column[0] for column in cursor.description or []]
    return [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
