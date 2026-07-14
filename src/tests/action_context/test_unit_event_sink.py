"""D9 — unit-event schema, migration parity, pure row builder, and the append-mode guard.

Offline: every assertion here is pyspark-free (the column list is a plain tuple list, the
migration is parsed as text, the row builder is pure, and the write-mode guard reads source).

THE GUARD RULE (plan §0b): every guard in this module ships with a companion test that plants a
violation of the thing it guards and asserts the guard FAILS. An invariant guard that has never
failed is not a guard.
"""

from __future__ import annotations

import inspect
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from analytics.action_context.drain import SB360_WORKER_ID
from ingestion.action_context_queue import (
    _EVENT_COLUMNS,
    _EVENT_TABLE,
    _N_EVENT_WORKERS,
    DeltaUnitEventSink,
    _event_row,
    event_columns_sql,
    event_table_for_worker,
    event_table_names,
)
from tests._ddl import ddl_columns
from tests._delta_write_ast import write_delta_table_calls, writes_without_append

_MIGRATION = Path(__file__).resolve().parents[3] / "scripts" / "migrations" / "2026-07-13-create-ac-unit-events.sql"

_CREATE_TABLE_RE = re.compile(r"CREATE TABLE IF NOT EXISTS\s+([\w.]+)\s*\(", re.IGNORECASE)


def _expected_columns() -> list[tuple[str, str]]:
    expected = [(name, sql_type) for name, sql_type, _ in _EVENT_COLUMNS]
    expected.append(("_ingested_at", "timestamp"))  # auto-added by write_delta_table
    return expected


def _statements(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


# ---------------------------------------------------------------- schema


def test_event_columns_include_event_date_and_write_failures() -> None:
    """M4: ``event_date`` MUST exist as a column — the table is PARTITIONED BY it, so a DDL without
    it cannot execute. ``write_failures`` is populated ONLY on ``slice_completed`` rows: it is the
    sole channel by which fail-open unit-event losses reach the gate (which reads persisted tables
    only).
    """
    names = [c[0] for c in _EVENT_COLUMNS]
    for required in (
        "run_id",
        "worker_id",
        "provider",
        "match_id",
        "period",
        "state",
        "started_at",
        "ended_at",
        "rows_written",
        "error",
        "write_failures",
        "event_date",
    ):
        assert required in names, f"event schema missing {required}"


def test_event_columns_sql_mirrors_the_column_list() -> None:
    """``event_columns_sql`` is the queue's ``queue_columns_sql`` convention, applied to D9."""
    sql = event_columns_sql()
    assert sql.split(", ") == [f"{n} {t}" for n, t, _ in _EVENT_COLUMNS] + ["_ingested_at timestamp"]


# ---------------------------------------------------------------- per-worker topology


def test_per_worker_table_names_include_a_table_per_worker_and_sb360() -> None:
    """Task 2 spike: per-worker tables (ADR-038 elimination route (b)) — one ``_delta_log`` each."""
    names = event_table_names()
    assert names == [f"{_EVENT_TABLE}_w{i}" for i in range(_N_EVENT_WORKERS)] + [f"{_EVENT_TABLE}_sb360"]
    assert event_table_for_worker(0) == f"{_EVENT_TABLE}_w0"
    assert event_table_for_worker(SB360_WORKER_ID) == f"{_EVENT_TABLE}_sb360"


def test_event_table_for_worker_rejects_an_unknown_worker() -> None:
    """An out-of-range worker id has no table — fail loud rather than silently write nowhere."""
    with pytest.raises(ValueError, match="worker_id"):
        event_table_for_worker(_N_EVENT_WORKERS)


def test_event_worker_count_matches_the_drain_fan_out() -> None:
    """Drift guard: one event table per for_each drain worker (``_N_DRAIN_WORKERS``)."""
    from ingestion.action_context import _ActionContextGuard

    assert _N_EVENT_WORKERS == _ActionContextGuard._N_DRAIN_WORKERS


# ---------------------------------------------------------------- migration parity (V4 + W5)


def test_event_ddl_matches_the_migration() -> None:
    """M4 + V4: mirror the queue's convention EXACTLY — REUSE its ``ddl_columns()`` parser and
    compare ORDERED (name, type) tuples, for EVERY per-worker table.

    A substring check (``assert name in migration``) is strictly weaker than the convention it
    claims to mirror: it passes on a WRONG TYPE, a WRONG ORDER, or a column that appears only in a
    COMMENT — so it would NOT reliably catch the very class it was added for (the missing
    ``event_date``).
    """
    sql = _MIGRATION.read_text(encoding="utf-8")
    creates = [s for s in _statements(sql) if _CREATE_TABLE_RE.search(s)]
    tables = [_CREATE_TABLE_RE.search(s).group(1).split(".")[-1] for s in creates]  # type: ignore[union-attr]
    assert tables == event_table_names(), "migration tables drifted from event_table_names()"
    for stmt, table in zip(creates, tables, strict=True):
        assert ddl_columns(stmt) == _expected_columns(), f"migration DDL for {table} drifted from _EVENT_COLUMNS"


def test_migration_creates_the_union_view_the_gate_reads() -> None:
    """The gate reads the VIEW; the per-worker table names must not leak into it."""
    sql = _MIGRATION.read_text(encoding="utf-8")
    assert f"CREATE OR REPLACE VIEW soccer_analytics.observability.{_EVENT_TABLE} " in sql
    for table in event_table_names():
        assert f"soccer_analytics.observability.{table}" in sql


def test_parity_guard_FAILS_on_a_planted_violation() -> None:  # noqa: N802 -- names the guard it proves
    """THE GUARD RULE (§0b): prove the guard can fail.

    Plant a DDL with ``event_date`` REMOVED and a wrong type — assert the ordered-tuple comparison
    rejects it. A substring check passes on a wrong type, a wrong order, or a column that appears
    only in a COMMENT — i.e. it would NOT have caught the very defect (missing ``event_date``) it
    was added for.
    """
    planted = "CREATE TABLE t (\n  run_id string,\n  worker_id bigint\n)"  # wrong type, missing event_date
    assert ddl_columns(planted) != _expected_columns()


def test_parity_guard_FAILS_when_a_column_appears_only_in_a_comment() -> None:  # noqa: N802
    """§0b, second shape: the substring check's exact blind spot."""
    planted = "\n".join(
        ["CREATE TABLE t ("]
        + [f"  {n} {t}," for n, t, _ in _EVENT_COLUMNS if n != "event_date"]
        + ["  -- event_date date", "  _ingested_at timestamp", ")"]
    )
    assert "event_date" in planted  # a substring guard would PASS this
    assert ddl_columns(planted) != _expected_columns()  # the ordered-tuple guard does not


def test_ddl_columns_is_not_fooled_by_a_partitioned_by_clause() -> None:
    """The shared parser must find the column list's MATCHING close paren — ``PARTITIONED BY
    (event_date)`` has its own, and a ``rindex(')')`` scan would swallow the whole statement."""
    planted = "CREATE TABLE t (\n  run_id string,\n  event_date date\n)\nUSING DELTA\nPARTITIONED BY (event_date)"
    assert ddl_columns(planted) == [("run_id", "string"), ("event_date", "date")]


# ---------------------------------------------------------------- V5: the pure row builder


def _row(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "run_id": "RUN_1",
        "worker_id": 3,
        "provider": "skillcorner",
        "match_id": "1552423",
        "period": 2,
        "state": "succeeded",
        "started_at": datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc),
        "ended_at": datetime(2026, 7, 13, 10, 5, tzinfo=timezone.utc),
        "rows_written": 550,
        "error": None,
        "write_failures": None,
    }
    kwargs.update(overrides)
    return _event_row(**kwargs)  # type: ignore[arg-type]


def test_event_row_populates_every_NOT_NULL_column() -> None:  # noqa: N802
    """V5 + §0b. ``event_date`` is NOT NULL and ``write_delta_table`` auto-adds ONLY
    ``_ingested_at`` — nothing else populates it, so the first production write would fail on a
    NOT-NULL partition column. Assert every NOT-NULL column is present and non-None.
    """
    row = _row()
    for name, _type, nullable in _EVENT_COLUMNS:
        if not nullable:
            assert row.get(name) is not None, f"NOT NULL column {name} unpopulated"
    assert row["event_date"] == date(2026, 7, 13)


def test_event_row_NOT_NULL_guard_FAILS_on_a_planted_omission() -> None:  # noqa: N802
    """§0b: plant the exact defect (drop ``event_date``) and prove the guard catches it."""
    planted = {k: v for k, v in _row().items() if k != "event_date"}
    missing = [n for n, _t, nullable in _EVENT_COLUMNS if not nullable and planted.get(n) is None]
    assert missing == ["event_date"]


def test_event_row_keys_are_exactly_the_event_columns() -> None:
    """No stray / missing keys — the row is written positionally against ``_EVENT_COLUMNS``."""
    assert list(_row()) == [n for n, _t, _null in _EVENT_COLUMNS]


def test_slice_completed_row_still_populates_the_NOT_NULL_columns() -> None:  # noqa: N802
    """A ``slice_completed`` event has no unit: ``provider``/``match_id`` are NOT NULL, so they
    carry a sentinel, and ``event_date`` must still resolve with both timestamps absent."""
    row = _row(
        provider=None,
        match_id=None,
        period=None,
        state="slice_completed",
        started_at=None,
        ended_at=None,
        rows_written=None,
        write_failures=2,
    )
    for name, _type, nullable in _EVENT_COLUMNS:
        if not nullable:
            assert row.get(name) is not None, f"NOT NULL column {name} unpopulated on slice_completed"
    assert row["write_failures"] == 2


# ---------------------------------------------------------------- §0d: mode="append"


def test_sink_writes_are_APPEND_not_the_overwrite_DEFAULT() -> None:  # noqa: N802
    """§0d — the spike proved this: 392 default-mode 'appends' left ONE row in the table."""
    src = inspect.getsource(DeltaUnitEventSink)
    assert write_delta_table_calls(src), "guard is vacuous — the sink makes no write_delta_table call"
    assert writes_without_append(src) == []


def test_append_guard_FAILS_on_the_planted_DEFAULT_mode() -> None:  # noqa: N802
    """§0b + §0d: plant the natural (silently destructive) call and prove the guard rejects it."""
    planted_default = "write_delta_table(sdf, catalog, schema, table, row_count=1)"
    planted_overwrite = "write_delta_table(sdf, catalog, schema, table, mode='overwrite')"
    assert writes_without_append(planted_default)
    assert writes_without_append(planted_overwrite)
    assert writes_without_append("write_delta_table(sdf, c, s, t, mode='append', row_count=1)") == []
