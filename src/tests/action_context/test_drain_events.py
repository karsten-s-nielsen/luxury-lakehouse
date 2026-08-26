"""Task 4 — the unit-event sink wired into the drain (D9).

The drain's contract with the sink is a set of DELIBERATELY DIFFERENT failure policies, and each
one exists because a specific failure must stay VISIBLE:

* ``unit_started`` is written BEFORE the unit is processed -> an OOM-killed driver's in-flight
  units stay distinguishable from units that never began. (If it were written after, the OOM'd
  unit would look like it was never started -- the exact invisibility D9 exists to kill.)
* the per-unit emits + ``flush_terminals`` are FAIL-OPEN -> telemetry loss never becomes data
  loss (ADR-002), and -- M1 -- were the flush fail-loud, the gate's ``UNVERIFIABLE`` verdict
  (whose entire purpose is *lost unit events*) could never be reached: a lossy worker would have
  died instead of reporting its loss.
* ``slice_completed`` is FAIL-LOUD -> it is the ONLY channel by which ``write_failures`` reaches
  the gate (a different task, reading persisted tables only). If it cannot land, the gate's
  evidence is unusable, so the worker task must fail rather than let the gate reason on a
  half-truth.

No Spark: the sink is injected, and the drain is exercised through a recording fake.
"""

from __future__ import annotations

import argparse
import inspect
import logging
from typing import Any

import pytest

from analytics.action_context.drain import GameTimeoutError, drain_worker
from analytics.action_context.work_unit import WorkUnit

# ── fakes ──────────────────────────────────────────────────────────────


class _FakeQueue:
    def __init__(self, units: list[WorkUnit]) -> None:
        self._units = units

    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
        return list(self._units)


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
    live_abandoned_count = 0

    def run(self, fn, label, timeout_s):
        return fn()


class _TimeoutWatchdog:
    live_abandoned_count = 0

    def __init__(self, timeout_labels: frozenset[str]) -> None:
        self._timeout = timeout_labels

    def run(self, fn, label, timeout_s):
        if label in self._timeout:
            raise GameTimeoutError(label)
        return fn()


class _RecordingSink:
    """Records every event in call order. Optionally EXPLODES on a chosen method.

    ``write_failures`` counts rows lost to a raising fail-open write -- exactly what the real
    ``DeltaUnitEventSink`` counts internally, so a drain-level catch and a sink-level catch are
    observationally identical to the gate.
    """

    def __init__(
        self,
        *,
        explode_unit_events: bool = False,
        explode_flush: bool = False,
        explode_slice: bool = False,
    ) -> None:
        self.calls: list[tuple[str, Any]] = []  # (state|"flush"|"slice_completed", match_id|worker_id|None)
        self.terminals: list[dict[str, Any]] = []
        self._explode_unit_events = explode_unit_events
        self._explode_flush = explode_flush
        self._explode_slice = explode_slice
        self._write_failures = 0

    @property
    def write_failures(self) -> int:
        return self._write_failures

    def unit_started(self, run_id: str, worker_id: int, unit: WorkUnit) -> None:
        self.calls.append(("running", unit.match_id))
        if self._explode_unit_events:
            self._write_failures += 1
            raise RuntimeError("unit_started write blew up")

    def unit_finished(
        self,
        run_id: str,
        worker_id: int,
        unit: WorkUnit,
        *,
        state: str,
        rows_written: int | None,
        error: str | None,
    ) -> None:
        self.calls.append((state, unit.match_id))
        self.terminals.append({"match_id": unit.match_id, "state": state, "rows_written": rows_written, "error": error})
        if self._explode_unit_events:
            self._write_failures += 1
            raise RuntimeError("unit_finished write blew up")

    def flush_terminals(self) -> None:
        self.calls.append(("flush", None))
        if self._explode_flush:
            self._write_failures += len(self.terminals)
            raise RuntimeError("flush blew up")

    def slice_completed(self, run_id: str, worker_id: int) -> None:
        self.calls.append(("slice_completed", worker_id))
        if self._explode_slice:
            raise RuntimeError("slice_completed write blew up")


def _drain(queue, processor, watchdog, sink, **kw):
    return drain_worker(
        queue,
        processor,
        watchdog,
        run_id="R",
        worker_id=3,
        logger=logging.getLogger("t"),
        sink=sink,
        **kw,
    )


# ── the OOM-visibility guarantee ───────────────────────────────────────


def test_running_is_written_BEFORE_the_unit_is_processed() -> None:  # noqa: N802 -- names the guarantee it proves
    """A unit that RAISES must still have its ``running`` event.

    This is the whole reason ``unit_started`` cannot be batched or moved after the call: an
    OOM-killed driver flushes no buffer, so a unit whose ``running`` was not already PERSISTED
    is indistinguishable from a unit that never began -- and the gate would return COMPLETE on a
    worker that died mid-slice.
    """
    unit = WorkUnit(provider="skillcorner", match_id="1552423", period=2)
    sink = _RecordingSink()
    proc = _FakeProcessor(fail=frozenset({"1552423"}))

    summary = _drain(_FakeQueue([unit]), proc, _InlineWatchdog(), sink)

    assert summary.failed == 1
    # the running event exists, and it precedes the terminal (order is the guarantee)
    assert sink.calls[0] == ("running", "1552423")
    assert ("failed", "1552423") in sink.calls
    assert sink.calls.index(("running", "1552423")) < sink.calls.index(("failed", "1552423"))


def test_terminal_states_recorded() -> None:
    """succeeded carries rows_written; failed carries the error; timed_out is its own state."""
    ok = WorkUnit(provider="metrica", match_id="ok", period=1)
    bad = WorkUnit(provider="metrica", match_id="bad", period=1)
    slow = WorkUnit(provider="metrica", match_id="slow", period=1)
    sink = _RecordingSink()
    proc = _FakeProcessor(rows=550, fail=frozenset({"bad"}))
    wd = _TimeoutWatchdog(timeout_labels=frozenset({"metrica:slow:1"}))

    summary = _drain(_FakeQueue([ok, bad, slow]), proc, wd, sink)

    assert (summary.processed, summary.failed, summary.timed_out) == (1, 1, 1)
    by_match = {t["match_id"]: t for t in sink.terminals}
    assert by_match["ok"]["state"] == "succeeded"
    assert by_match["ok"]["rows_written"] == 550
    assert by_match["ok"]["error"] is None
    assert by_match["bad"]["state"] == "failed"
    assert by_match["bad"]["error"] and "boom bad" in by_match["bad"]["error"]
    assert by_match["bad"]["rows_written"] is None
    assert by_match["slow"]["state"] == "timed_out"
    # every unit got a running event first
    assert [c for c in sink.calls if c[0] == "running"] == [
        ("running", "ok"),
        ("running", "bad"),
        ("running", "slow"),
    ]
    # and the slice is closed exactly once, after the flush
    assert sink.calls[-2:] == [("flush", None), ("slice_completed", 3)]


# ── fail-open (unit events) ────────────────────────────────────────────


def test_unit_event_failure_does_not_break_the_drain(caplog: pytest.LogCaptureFixture) -> None:
    """FAIL-OPEN: telemetry loss must NEVER become data loss (ADR-002).

    Every unit-event write raises -> the drain still processes EVERY unit, logs at ERROR (never
    warning: warnings are invisible in error-log queries -- the 2026-04-12 lesson), and the loss
    is COUNTED so it can ride to the gate on ``slice_completed``.
    """
    units = [WorkUnit(provider="idsse", match_id=f"m{i}", period=1) for i in range(3)]
    sink = _RecordingSink(explode_unit_events=True)
    proc = _FakeProcessor()

    with caplog.at_level(logging.ERROR):
        summary = _drain(_FakeQueue(units), proc, _InlineWatchdog(), sink)

    assert proc.processed == ["m0", "m1", "m2"]  # the drain is UNHARMED
    assert summary.processed == 3 and summary.failed == 0
    assert sink.write_failures == 6  # 3 running + 3 terminals
    assert sink.calls[-1] == ("slice_completed", 3)  # the slice still closes
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, "a lost unit event must be logged at ERROR, not WARNING"


# ── fail-loud (slice_completed) vs fail-open (flush) ───────────────────


def test_slice_completed_failure_FAILS_the_worker() -> None:  # noqa: N802 -- names the policy it proves
    """FAIL-LOUD: ``slice_completed`` is the ONLY channel by which ``write_failures`` reaches the
    gate, which runs in a different task and reads persisted tables only. If it cannot land, the
    gate's evidence is unusable -> the worker task must fail rather than let the gate reason on a
    half-truth (a missing slice_completed reads as a DEAD worker)."""
    units = [WorkUnit(provider="idsse", match_id="m0", period=1)]
    sink = _RecordingSink(explode_slice=True)

    with pytest.raises(RuntimeError, match="slice_completed"):
        _drain(_FakeQueue(units), _FakeProcessor(), _InlineWatchdog(), sink)


def test_flush_terminals_is_fail_open_but_slice_completed_is_fail_loud(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """M1 — OPPOSITE policies, which is why they MUST be separate writes.

    Were the flush fail-loud, the ``UNVERIFIABLE`` verdict -- whose entire purpose is *lost unit
    events* -- could NEVER be reached: the worker would have died instead of reporting its loss,
    and the gate would see a dead worker (DRAIN_FAILED) instead of a lossy one.
    """
    units = [WorkUnit(provider="idsse", match_id="m0", period=1)]
    sink = _RecordingSink(explode_flush=True)

    with caplog.at_level(logging.ERROR):
        summary = _drain(_FakeQueue(units), _FakeProcessor(), _InlineWatchdog(), sink)

    assert summary.processed == 1  # flush blew up; the drain did NOT
    assert sink.write_failures == 1  # the loss is counted ...
    assert sink.calls[-1] == ("slice_completed", 3)  # ... and still carried to the gate
    assert [r for r in caplog.records if r.levelno >= logging.ERROR]

    # ... and the SAME sink, exploding on the slice write instead, RAISES.
    loud = _RecordingSink(explode_slice=True)
    with pytest.raises(RuntimeError, match="slice_completed"):
        _drain(_FakeQueue(units), _FakeProcessor(), _InlineWatchdog(), loud)


# ── terminals must not be lost with the worker that produced them ──────


class _AbandoningWatchdog:
    """Times out every unit and leaks one abandoned (non-interruptible) thread each time."""

    def __init__(self) -> None:
        self.live_abandoned_count = 0

    def run(self, fn, label, timeout_s):
        self.live_abandoned_count += 1
        raise GameTimeoutError(label)


def test_terminals_are_FLUSHED_BEFORE_the_abandon_ceiling_raise() -> None:  # noqa: N802
    """A planned raise must not destroy its own evidence.

    ``drain_worker`` raises deliberately once too many abandoned threads are alive (the slice rolls
    to the next run). Terminals are BUFFERED, so before this fix that raise threw away EVERY
    terminal the worker had produced -- including the units that succeeded and wrote rows -- and the
    ``slice_completed`` that carries ``write_failures``. The worker then read to the gate as DEAD
    and V6 had to reconstruct from the mart what the worker already knew: a planned raise made
    indistinguishable from an OOM.
    """
    units = [WorkUnit(provider="idsse", match_id=f"m{i}", period=1) for i in range(4)]
    sink = _RecordingSink()

    with pytest.raises(RuntimeError, match="abandoned-thread ceiling"):
        _drain(_FakeQueue(units), _FakeProcessor(), _AbandoningWatchdog(), sink, max_abandoned=1)

    assert ("flush", None) in sink.calls, "the abandon-ceiling raise threw away every buffered terminal"
    assert [t["state"] for t in sink.terminals] == ["timed_out", "timed_out"]
    # the flush comes AFTER the terminals it must persist
    assert sink.calls.index(("flush", None)) > max(i for i, c in enumerate(sink.calls) if c[0] == "timed_out")


def test_terminals_are_flushed_PERIODICALLY_not_once_at_the_very_end() -> None:  # noqa: N802
    """Defence in depth for the same class: ANY escape from ``drain_worker`` (an OOM, an adapter
    bug, a future raise) loses only the units since the last flush, not the whole slice. Each worker
    owns its OWN Delta table, so a flush costs no cross-worker ``_delta_log`` contention."""
    units = [WorkUnit(provider="idsse", match_id=f"m{i}", period=1) for i in range(25)]
    sink = _RecordingSink()

    summary = _drain(_FakeQueue(units), _FakeProcessor(), _InlineWatchdog(), sink, flush_every=10)

    assert summary.processed == 25
    flushes = [i for i, c in enumerate(sink.calls) if c == ("flush", None)]
    assert len(flushes) == 3, "expected a flush after units 10 and 20, plus the end-of-slice flush"
    # the first flush lands before the LAST unit is even started (i.e. it is genuinely mid-slice)
    assert flushes[0] < sink.calls.index(("running", "m24"))
    assert sink.calls[-2:] == [("flush", None), ("slice_completed", 3)]


# ── P4: the IDLE worker ────────────────────────────────────────────────


def test_idle_worker_STILL_emits_slice_completed(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802 -- P4
    """P4 — the gate misfires on every small daily run without this.

    ``main_drain_worker`` returns BEFORE ``drain_worker`` when a worker has no units. Terraform's
    own comment says "daily runs are tiny": on a 3-unit run FIVE of eight workers are idle. If an
    idle worker emits NOTHING it is indistinguishable from a DEAD one, the gate returns
    DRAIN_FAILED, and it cries wolf on a healthy run -- the muting failure this design exists to
    prevent, arriving through the front door.
    """
    import ingestion.action_context as ac
    import ingestion.bootstrap as bs
    import ingestion.drain_adapters as q

    ns = argparse.Namespace(catalog="cat", schema="bronze", worker_id="5", run_id="JOBRUN42")
    monkeypatch.setattr(ac, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)

    class _EmptyQ:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
            return []

    sink = _RecordingSink()
    ensured: list[bool] = []

    class _Sink(_RecordingSink):
        def __init__(self, *a: object, **k: object) -> None:
            super().__init__()

        def ensure_tables(self) -> None:
            ensured.append(True)

        def ensure_own_table(self, worker_id: int) -> None:
            ensured.append(True)

        def slice_completed(self, run_id: str, worker_id: int) -> None:
            sink.slice_completed(run_id, worker_id)

    def _boom(*a: object, **k: object) -> object:
        raise AssertionError("empty worker must NOT build the processor / call drain_worker")

    monkeypatch.setattr(q, "DeltaWorkQueue", _EmptyQ)
    monkeypatch.setattr(q, "DeltaUnitEventSink", _Sink)
    monkeypatch.setattr(q, "SparkGameProcessor", _boom)
    monkeypatch.setattr(q, "SparkInterruptWatchdog", _boom)
    monkeypatch.setattr(ac, "drain_worker", _boom)

    ac.main_drain_worker()

    assert ensured == [], (
        "the drain worker must NOT create event tables (neither ensure_tables NOR ensure_own_table): it runs "
        "as an 8-way for_each, so this would put 8 concurrent drivers on CREATE-IF-NOT-EXISTS + a view. "
        "Preflight -- which the drain DOES depend on, so the ordering holds -- owns creation, before its own "
        "nothing-to-do early-return. See test_main_preflight_ensures_event_tables."
    )
    assert sink.calls == [("slice_completed", 5)], "an IDLE worker must still SAY IT RAN"


def test_idle_short_circuit_emits_before_returning() -> None:
    """Source-level backstop for P4: the emit must sit BEFORE the ``return`` in the short-circuit.

    ``main_drain_worker`` is an entry point (argparse + Spark + bootstrap), so the behavioural test
    above drives it through monkeypatch; this asserts the ORDER a future edit could silently invert
    (an emit after the return is dead code that no fake would catch).
    """
    from ingestion import action_context as ac

    src = inspect.getsource(ac.main_drain_worker)
    head = src[: src.index("if not units:")]
    body = src[src.index("if not units:") :]
    short_circuit = body[: body.index("processor =")]
    assert "DeltaUnitEventSink" in head, "the sink must be constructed before the short-circuit"
    assert "slice_completed" in short_circuit
    assert short_circuit.index("slice_completed") < short_circuit.rindex("return")
