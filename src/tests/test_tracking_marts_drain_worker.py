"""Task 6 — ``ingestion.tracking_marts_drain._run_worker`` drives the pure drain + fails on a failed unit.

``_run_worker`` is the testable core of ``main_tracking_marts_drain_worker``. It builds the
``drain_name="tracking_marts"`` adapters + the ``TrackingMartsProcessor``, runs the REAL ``drain_worker``
(so events flow through the sink), and calls the REAL ``raise_on_failed_units`` (ADR-067) — so a swallowed
per-unit failure still FAILS THE TASK.

No Spark: every adapter is faked at its source module. The processor raises on the 2nd unit; the drain
isolates it (``failed=1``, the slice rolls forward) and ``raise_on_failed_units`` then raises.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from analytics.action_context.work_unit import WorkUnit


class _FakeQueue:
    def __init__(self, *a: object, **k: object) -> None:
        self._units = [
            WorkUnit(provider="idsse", match_id="M1", period=1),
            WorkUnit(provider="idsse", match_id="M2", period=1),
        ]

    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
        return list(self._units)


class _FakeProcessor:
    """Processes every unit; unit #2 (``M2``) raises — so the drain records a ``failed`` terminal."""

    def __init__(self, *a: object, **k: object) -> None:
        self.processed: list[str] = []

    def process(self, unit: WorkUnit) -> int:
        if unit.match_id == "M2":
            raise ValueError(f"boom {unit.match_id}")
        self.processed.append(unit.match_id)
        return 5


class _InlineWatchdog:
    live_abandoned_count = 0

    def __init__(self, *a: object, **k: object) -> None:
        pass

    def run(self, fn: Any, label: str, timeout_s: float) -> int:
        return fn()


class _RecordingSink:
    """Records every event in call order; satisfies the ``UnitEventSink`` port."""

    def __init__(self, *a: object, **k: object) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._write_failures = 0

    @property
    def write_failures(self) -> int:
        return self._write_failures

    def unit_started(self, run_id: str, worker_id: int, unit: WorkUnit) -> None:
        self.calls.append(("running", unit.match_id))

    def unit_finished(
        self, run_id: str, worker_id: int, unit: WorkUnit, *, state: str, rows_written: int | None, error: str | None
    ) -> None:
        self.calls.append((state, unit.match_id))

    def flush_terminals(self) -> None:
        self.calls.append(("flush", None))

    def slice_completed(self, run_id: str, worker_id: int) -> None:
        self.calls.append(("slice_completed", worker_id))


def _patch_adapters(monkeypatch: pytest.MonkeyPatch, sink: _RecordingSink) -> _FakeProcessor:
    import ingestion.drain_adapters as q
    import ingestion.tracking_marts_processor as tmp

    processor = _FakeProcessor()
    monkeypatch.setattr(q, "DeltaWorkQueue", _FakeQueue)
    monkeypatch.setattr(q, "DeltaUnitEventSink", lambda *a, **k: sink)
    monkeypatch.setattr(q, "SparkInterruptWatchdog", _InlineWatchdog)
    monkeypatch.setattr(tmp, "TrackingMartsProcessor", lambda *a, **k: processor)
    return processor


def test_run_worker_drains_both_emits_events_and_FAILS_on_a_failed_unit(  # noqa: N802
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ingestion.tracking_marts_drain import _run_worker

    sink = _RecordingSink()
    processor = _patch_adapters(monkeypatch, sink)

    with pytest.raises(RuntimeError, match="idsse:M2:1"):
        _run_worker(
            spark=object(),
            catalog="cat",
            schema="bronze",
            worker_id=3,
            run_id="R",
            budget_s=2700,
            task_logger=logging.getLogger("t"),
        )

    # Both units were drained (the good one processed; the bad one attempted, isolated, rolled forward).
    assert processor.processed == ["M1"]
    running = [c for c in sink.calls if c[0] == "running"]
    assert running == [("running", "M1"), ("running", "M2")]  # unit_started x2 (OOM-visibility, per unit)
    # A `failed` terminal was emitted for the bad unit, and the slice was closed.
    assert ("failed", "M2") in sink.calls
    assert ("succeeded", "M1") in sink.calls
    assert sink.calls[-1] == ("slice_completed", 3)


def test_idle_worker_still_emits_slice_completed_and_does_not_build_processor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P4 — an idle worker (empty slice) must SAY IT RAN, and must NOT build the processor (no xT-grid
    load). Otherwise the gate reads an idle worker as a DEAD one and cries wolf on a tiny daily run."""
    import ingestion.drain_adapters as q
    import ingestion.tracking_marts_processor as tmp
    from ingestion.tracking_marts_drain import _run_worker

    class _EmptyQueue:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
            return []

    sink = _RecordingSink()

    def _boom(*a: object, **k: object) -> object:
        raise AssertionError("an idle worker must NOT build the processor")

    monkeypatch.setattr(q, "DeltaWorkQueue", _EmptyQueue)
    monkeypatch.setattr(q, "DeltaUnitEventSink", lambda *a, **k: sink)
    monkeypatch.setattr(q, "SparkInterruptWatchdog", _boom)
    monkeypatch.setattr(tmp, "TrackingMartsProcessor", _boom)

    _run_worker(
        spark=object(),
        catalog="cat",
        schema="bronze",
        worker_id=5,
        run_id="R",
        budget_s=2700,
        task_logger=logging.getLogger("t"),
    )

    assert sink.calls == [("slice_completed", 5)], "an IDLE worker must still emit exactly one slice_completed"
