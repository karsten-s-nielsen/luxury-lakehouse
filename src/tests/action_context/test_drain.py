from __future__ import annotations

import logging
import random

import pytest

from analytics.action_context.drain import (
    GameTimeoutError,
    WorkAssignment,
    assign_workers,
    drain_worker,
    tier_cost_fn,
    unit_label,
)
from analytics.action_context.work_unit import WorkUnit

# ── Task 1: types + cost model ─────────────────────────────────────────


def test_tier_cost_fn_rank_order() -> None:
    tracking = WorkUnit(provider="metrica", match_id="m1")
    idsse_half = WorkUnit(provider="idsse", match_id="i1", period=1)
    sb = WorkUnit(provider="statsbomb", match_id="s1")
    # Frames-required (ADR-057): idsse-half and tracking are the expensive tier; statsbomb
    # (sb360) is the cheaper tier. There is no event-only tier.
    assert tier_cost_fn(idsse_half) == tier_cost_fn(tracking)
    assert tier_cost_fn(tracking) > tier_cost_fn(sb)


def test_work_assignment_is_frozen() -> None:
    a = WorkAssignment(unit=WorkUnit(provider="metrica", match_id="m1"), worker_id=0, seq=0, est_cost=1800.0)
    assert a.worker_id == 0 and a.seq == 0
    assert issubclass(GameTimeoutError, RuntimeError)


# ── Task 2: assign_workers ─────────────────────────────────────────────


def _units(n: int, provider: str = "statsbomb") -> list[WorkUnit]:
    # statsbomb is the cheap (sb360) tier — a valid non-tracking AC provider (ADR-057);
    # used here as the generic "cheap unit" for assign_workers/cost-ranking tests.
    return [WorkUnit(provider=provider, match_id=f"{provider}-{i}") for i in range(n)]


def test_assign_workers_every_unit_once() -> None:
    units = _units(20)
    assignments = assign_workers(units, n_workers=8)
    got = sorted(a.unit.match_id for a in assignments)
    assert got == sorted(u.match_id for u in units)  # each exactly once
    assert {a.worker_id for a in assignments} <= set(range(8))


def test_assign_workers_seq_contiguous_per_worker() -> None:
    assignments = assign_workers(_units(20), n_workers=4)
    by_worker: dict[int, list[int]] = {}
    for a in assignments:
        by_worker.setdefault(a.worker_id, []).append(a.seq)
    for seqs in by_worker.values():
        assert sorted(seqs) == list(range(len(seqs)))  # 0..k-1, contiguous


def test_assign_workers_expensive_units_spread() -> None:
    # 8 tracking (expensive) + 40 event (cheap) -> the 8 tracking land on 8 distinct workers.
    tracking = [WorkUnit(provider="metrica", match_id=f"t{i}") for i in range(8)]
    units = tracking + _units(40)
    assignments = assign_workers(units, n_workers=8)
    tracking_workers = {a.worker_id for a in assignments if a.unit.provider == "metrica"}
    assert len(tracking_workers) == 8


def test_assign_workers_deterministic_under_shuffle() -> None:
    units = _units(15, "statsbomb") + [WorkUnit(provider="idsse", match_id="i", period=p) for p in (1, 2)]
    base = assign_workers(units, n_workers=4)
    shuffled = list(units)
    random.Random(7).shuffle(shuffled)  # noqa: S311 -- test shuffle, not cryptographic
    other = assign_workers(shuffled, n_workers=4)

    def key(a: WorkAssignment) -> tuple[str, str, int | None]:
        return (a.unit.provider, a.unit.match_id, a.unit.period)

    assert {key(a): (a.worker_id, a.seq) for a in base} == {key(a): (a.worker_id, a.seq) for a in other}


def test_assign_workers_more_workers_than_units() -> None:
    assignments = assign_workers(_units(3), n_workers=8)
    assert len(assignments) == 3


def test_assign_workers_empty() -> None:
    assert assign_workers([], n_workers=8) == []


# ── Task 3: drain_worker ───────────────────────────────────────────────


class _FakeQueue:
    def __init__(self, mapping: dict[tuple[str, int], list[WorkUnit]]) -> None:
        self._m = mapping

    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
        return list(self._m.get((run_id, worker_id), []))


class _FakeProcessor:
    def __init__(self, rows: int = 5, fail: frozenset[str] = frozenset()) -> None:
        self.rows = rows
        self.fail = fail
        self.processed: list[str] = []

    def process(self, unit: WorkUnit) -> int:
        if unit.match_id in self.fail:
            raise ValueError(f"boom {unit.match_id}")
        self.processed.append(unit.match_id)
        return self.rows


class _InlineWatchdog:
    """Runs fn inline; re-raises naturally. No timeout."""

    live_abandoned_count = 0

    def run(self, fn, label, timeout_s):
        return fn()


class _NullSink:
    """No-op ``UnitEventSink`` — these tests are about the drain's CONTROL FLOW, not its events.

    ``sink`` is a mandatory injection (no default), so it must be supplied here; the event contract
    itself is tested in ``test_drain_events.py``.
    """

    write_failures = 0

    def unit_started(self, run_id: str, worker_id: int, unit: WorkUnit) -> None: ...

    def unit_finished(self, run_id, worker_id, unit, *, state, rows_written, error) -> None: ...

    def flush_terminals(self) -> None: ...

    def slice_completed(self, run_id: str, worker_id: int) -> None: ...


class _TimeoutWatchdog:
    """Times out (and 'abandons') a configured set of unit labels.

    ``live_abandoned_count`` simulates concurrently-alive abandoned threads; the
    fake never 'frees' them, so for the ceiling test live == cumulative.
    """

    def __init__(self, timeout_labels: frozenset[str]) -> None:
        self._timeout = timeout_labels
        self._abandoned = 0

    @property
    def live_abandoned_count(self) -> int:
        return self._abandoned

    def run(self, fn, label, timeout_s):
        if label in self._timeout:
            self._abandoned += 1
            raise GameTimeoutError(label)
        return fn()


def test_drain_worker_processes_own_slice_only() -> None:
    u0 = [WorkUnit(provider="wyscout", match_id="a"), WorkUnit(provider="wyscout", match_id="b")]
    u1 = [WorkUnit(provider="wyscout", match_id="c")]
    q = _FakeQueue({("R", 0): u0, ("R", 1): u1})
    proc = _FakeProcessor()
    s = drain_worker(
        q, proc, _InlineWatchdog(), run_id="R", worker_id=0, logger=logging.getLogger("t"), sink=_NullSink()
    )
    assert proc.processed == ["a", "b"]
    assert s.processed == 2 and s.total_rows == 10 and s.failed == 0


def test_drain_worker_failure_continues_with_key(caplog) -> None:
    units = [
        WorkUnit(provider="wyscout", match_id="ok1"),
        WorkUnit(provider="wyscout", match_id="bad"),
        WorkUnit(provider="wyscout", match_id="ok2"),
    ]
    q = _FakeQueue({("R", 0): units})
    proc = _FakeProcessor(fail=frozenset({"bad"}))
    with caplog.at_level(logging.ERROR):
        s = drain_worker(
            q, proc, _InlineWatchdog(), run_id="R", worker_id=0, logger=logging.getLogger("t"), sink=_NullSink()
        )
    assert proc.processed == ["ok1", "ok2"]
    assert s.processed == 2 and s.failed == 1
    assert "wyscout:bad" in s.failed_units
    assert any("wyscout:bad" in r.message for r in caplog.records)


def test_drain_worker_timeout_continues() -> None:
    units = [WorkUnit(provider="metrica", match_id="x"), WorkUnit(provider="metrica", match_id="y")]
    q = _FakeQueue({("R", 0): units})
    proc = _FakeProcessor()
    wd = _TimeoutWatchdog(timeout_labels=frozenset({"metrica:x"}))
    s = drain_worker(q, proc, wd, run_id="R", worker_id=0, logger=logging.getLogger("t"), sink=_NullSink())
    assert proc.processed == ["y"]
    assert s.timed_out == 1 and s.processed == 1 and "metrica:x" in s.timed_out_units


def test_drain_worker_abandonment_ceiling_fails_fast() -> None:
    units = [WorkUnit(provider="wyscout", match_id=f"h{i}") for i in range(10)]
    q = _FakeQueue({("R", 0): units})
    wd = _TimeoutWatchdog(timeout_labels=frozenset(unit_label(u) for u in units))
    with pytest.raises(RuntimeError, match="abandoned-thread ceiling"):
        drain_worker(
            q,
            _FakeProcessor(),
            wd,
            run_id="R",
            worker_id=0,
            logger=logging.getLogger("t"),
            sink=_NullSink(),
            max_abandoned=3,
        )


def test_drain_worker_uses_prefetched_units() -> None:
    """When the caller pre-fetches units (entry-point short-circuit), drain_worker must
    NOT re-read the queue."""

    class _BoomQueue:
        def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
            raise AssertionError("must not re-fetch when units are pre-supplied")

    units = [WorkUnit(provider="wyscout", match_id="a"), WorkUnit(provider="wyscout", match_id="b")]
    proc = _FakeProcessor()
    s = drain_worker(
        _BoomQueue(),
        proc,
        _InlineWatchdog(),
        run_id="R",
        worker_id=0,
        logger=logging.getLogger("t"),
        sink=_NullSink(),
        units=units,
    )
    assert proc.processed == ["a", "b"]
    assert s.processed == 2


def test_unit_label_includes_period() -> None:
    assert unit_label(WorkUnit(provider="idsse", match_id="m", period=2)) == "idsse:m:2"
    assert unit_label(WorkUnit(provider="wyscout", match_id="w")) == "wyscout:w"
