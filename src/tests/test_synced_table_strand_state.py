"""Strand-state store: recurrence semantics + fail-open backend (spec H3 / review P1, R1a)."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

from ingestion.synced_table_strand_state import (
    HEALED,
    STRANDED,
    SparkStrandStateBackend,
    StrandStateStore,
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
