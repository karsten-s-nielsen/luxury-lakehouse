"""Strand-state store: recurrence semantics + fail-open backend (spec H3 / review P1, R1a)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from ingestion.synced_table_strand_state import (
    HEALED,
    STRANDED,
    SparkStrandStateBackend,
    StrandStateStore,
    WarehouseStrandStateBackend,
)

_T0 = datetime(2026, 6, 1)


def _t(hours: int) -> datetime:
    return _T0 + timedelta(hours=hours)


class _FakeBackend:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, datetime]] = []

    def append_event(self, table_name: str, event_type: str, event_at: datetime) -> None:
        self.events.append((table_name, event_type, event_at))

    def read_latest(self, table_name: str):
        s = [at for (t, et, at) in self.events if t == table_name and et == STRANDED]
        h = [at for (t, et, at) in self.events if t == table_name and et == HEALED]
        return (max(s) if s else None, max(h) if h else None)


def test_stranded_unhealed_after_strand() -> None:
    s = StrandStateStore(_FakeBackend())
    s.mark_stranded("x", _t(1))
    assert s.was_stranded_unhealed("x") is True  # recurrence signal: strand with no later heal


def test_cleared_after_heal() -> None:
    s = StrandStateStore(_FakeBackend())
    s.mark_stranded("x", _t(1))
    s.mark_healed("x", _t(2))
    assert s.was_stranded_unhealed("x") is False  # healed since the strand


def test_heal_before_strand_does_not_clear() -> None:
    # heal->clear->re-strand: at the NEW strand's detection, classify checks BEFORE recording it,
    # so the store still shows the prior (healed) state -> False -> green-with-warning (Task 8).
    # If a heal predates the most recent strand, the table is still broken -> True.
    s = StrandStateStore(_FakeBackend())
    s.mark_healed("x", _t(1))
    s.mark_stranded("x", _t(2))
    assert s.was_stranded_unhealed("x") is True


def test_two_strands_no_heal_is_recurrence() -> None:
    s = StrandStateStore(_FakeBackend())
    s.mark_stranded("x", _t(1))
    s.mark_stranded("x", _t(2))
    assert s.was_stranded_unhealed("x") is True


def test_absent_state_is_false() -> None:
    assert StrandStateStore(_FakeBackend()).was_stranded_unhealed("never-seen") is False


def test_backend_read_fail_open_on_missing_table() -> None:
    # R1a: a missing state table (first run before migration) must read as (None, None), not crash.
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: x")
    assert SparkStrandStateBackend(spark, "cat").read_latest("fct_x_synced") == (None, None)


# -------------------------------------------------------------- WarehouseStrandStateBackend (no Spark)
# The heal runs in the GitHub Actions maintenance env, which has NO pyspark. It records `healed` via
# the SQL warehouse instead of Spark. Regression context: 2026-06-06 `ModuleNotFoundError: No module
# named 'pyspark'` crashed the heal step before it healed anything.
def _capturing_sql_exec() -> tuple[list[str], Callable[[str], None]]:
    statements: list[str] = []
    return statements, statements.append


def test_warehouse_backend_append_creates_then_inserts() -> None:
    statements, sql_exec = _capturing_sql_exec()
    WarehouseStrandStateBackend(sql_exec, "soccer_analytics").append_event("fct_x_synced", HEALED, _t(3))
    joined = "\n".join(statements)
    assert any("CREATE TABLE IF NOT EXISTS" in s and "synced_table_strand_state" in s for s in statements)
    assert any("INSERT INTO" in s for s in statements)
    assert "fct_x_synced" in joined and "healed" in joined
    assert "2026-06-01 03:00:00" in joined  # event_at rendered as a TIMESTAMP literal


def test_warehouse_backend_ensures_table_only_once() -> None:
    statements, sql_exec = _capturing_sql_exec()
    backend = WarehouseStrandStateBackend(sql_exec, "soccer_analytics")
    backend.append_event("fct_x_synced", HEALED, _t(1))
    backend.append_event("fct_y_synced", HEALED, _t(2))
    creates = [s for s in statements if "CREATE TABLE IF NOT EXISTS" in s]
    inserts = [s for s in statements if "INSERT INTO" in s]
    assert len(creates) == 1 and len(inserts) == 2  # ensure-table is idempotent + done once


def test_warehouse_backend_read_latest_unsupported() -> None:
    _, sql_exec = _capturing_sql_exec()
    with pytest.raises(NotImplementedError):
        WarehouseStrandStateBackend(sql_exec, "cat").read_latest("fct_x_synced")


def test_warehouse_store_mark_healed_inserts_healed_event() -> None:
    statements, sql_exec = _capturing_sql_exec()
    StrandStateStore(WarehouseStrandStateBackend(sql_exec, "cat")).mark_healed("fct_x_synced", _t(5))
    assert any("INSERT INTO" in s and "healed" in s and "fct_x_synced" in s for s in statements)
