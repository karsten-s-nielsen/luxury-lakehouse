"""Detect-only refresh classify: precedence + recurrence-RED + freshness-honest summary (§1/H3/M7/P1/P6)."""

from __future__ import annotations

from datetime import datetime

from ingestion.refresh_synced_tables import classify_and_exit_code

_NOW = datetime(2026, 6, 5, 12, 0, 0)


class _FakeState:
    def __init__(self, prev_unhealed: set[str] | None = None) -> None:
        self._prev = prev_unhealed or set()
        self.marked: list[str] = []

    def was_stranded_unhealed(self, table_name: str) -> bool:
        return table_name in self._prev

    def mark_stranded(self, table_name: str, event_at: datetime) -> None:
        self.marked.append(table_name)


def test_all_complete_is_green() -> None:
    rc, summary = classify_and_exit_code({"a": "COMPLETE"}, set(), _FakeState(), now=_NOW)
    assert rc == 0 and "fresh" in summary


def test_first_strand_is_green_with_warning_and_records() -> None:
    state = _FakeState()
    rc, summary = classify_and_exit_code({"a": "FAILED"}, {"a"}, state, now=_NOW)
    assert rc == 0 and "stranded" in summary and "dispatched" in summary
    assert state.marked == ["a"]  # recorded for next-run recurrence check


def test_recurrence_is_red() -> None:
    rc, summary = classify_and_exit_code({"a": "FAILED"}, {"a"}, _FakeState(prev_unhealed={"a"}), now=_NOW)
    assert rc == 1 and "recurrence" in summary


def test_heal_then_clear_then_restrand_is_green() -> None:
    # P1: prior heal cleared the strand, so was_stranded_unhealed is False -> new incident -> green.
    state = _FakeState(prev_unhealed=set())  # healed since -> not unhealed
    rc, _ = classify_and_exit_code({"a": "FAILED"}, {"a"}, state, now=_NOW)
    assert rc == 0 and state.marked == ["a"]


def test_non_checkpoint_failure_is_red() -> None:
    rc, summary = classify_and_exit_code({"a": "FAILED"}, set(), _FakeState(), now=_NOW)
    assert rc == 1 and "not self-healable" in summary


def test_real_failure_dominates_first_strand() -> None:
    # P6 precedence: a real (non-stranded) failure RED beats a first-strand green-warning.
    state = _FakeState()
    rc, _ = classify_and_exit_code({"a": "FAILED", "b": "FAILED"}, {"b"}, state, now=_NOW)
    assert rc == 1
    assert state.marked == []  # real-failure path returns before recording strands


def test_complete_plus_first_strand_is_green() -> None:
    rc, summary = classify_and_exit_code({"a": "COMPLETE", "b": "FAILED"}, {"b"}, _FakeState(), now=_NOW)
    assert rc == 0 and "1/2 fresh" in summary
