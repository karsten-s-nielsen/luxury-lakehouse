"""Task 8 — ``ingestion.tracking_marts_gate.main``: the drain-completeness gate's IMPURE half.

The entry point holds NO rules — it adapts the persisted tables into the pure
``analytics.action_context.drain_gate.evaluate`` inputs and turns ``must_raise`` into an exit code. The
verdict logic itself is pinned Spark-free in ``test_drain_gate.py``.

Two tracking-marts-specific things this file pins:

* **G1 — ``extra_expected_workers=frozenset()``.** This drain has NO sb360 task. A COMPLETE fixture here
  carries NO sb360 ``slice_completed``; if the gate passed the AC default (the sb360 sentinel) it would
  report DRAIN_FAILED. That the COMPLETE fixture returns COMPLETE proves the empty set is wired — and a
  direct spy on ``evaluate`` makes it explicit.
* **The raise/report split** — only INCOMPLETE fails the task.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import pytest

import ingestion.tracking_marts_gate as gate
from analytics.action_context.drain_gate import DrainGateError, QueueRow, UnitEvent, UnitKey


def _slice(worker_id: int, write_failures: int = 0) -> UnitEvent:
    return UnitEvent(
        worker_id=worker_id,
        provider="__slice__",
        match_id="__slice__",
        period=None,
        state="slice_completed",
        write_failures=write_failures,
    )


def _drive(
    monkeypatch: pytest.MonkeyPatch,
    *,
    queue: list[QueueRow],
    events: list[UnitEvent],
    result_counts: dict[tuple[str, str, int | None], int] | None = None,
    remaining: frozenset[UnitKey] = frozenset(),
    run_id: str = "JOBRUN42",
) -> list[tuple[int, str]]:
    """Run ``main()`` with every Spark seam faked; return the (level, message) log records."""
    ns = argparse.Namespace(catalog="cat", schema="bronze", run_id=run_id)
    records: list[tuple[int, str]] = []

    class _Log(logging.Logger):
        def __init__(self) -> None:
            super().__init__("test", logging.DEBUG)

        def _log(self, level: int, msg: object, args: Any, **kw: Any) -> None:  # type: ignore[override]
            records.append((level, str(msg) % args if args else str(msg)))

    monkeypatch.setattr(gate, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(gate, "get_spark_session", lambda: object())
    monkeypatch.setattr(gate, "configure_logging", lambda *a, **k: _Log())
    import ingestion.bootstrap as bs

    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(gate, "_read_queue", lambda *a, **k: list(queue))
    monkeypatch.setattr(gate, "_read_events", lambda *a, **k: list(events))
    monkeypatch.setattr(gate, "_read_result_counts", lambda *a, **k: dict(result_counts or {}))
    monkeypatch.setattr(gate, "_remaining_units", lambda *a, **k: remaining)

    gate.main()
    return records


def test_gate_reports_COMPLETE_and_does_not_raise_with_no_sb360(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """Every enqueued unit has a terminal + its worker emitted ``slice_completed``, and there is NO
    sb360 slice — which is COMPLETE only because ``extra_expected_workers=frozenset()`` (G1)."""
    records = _drive(
        monkeypatch,
        queue=[QueueRow(0, "idsse", "M1", 1)],
        events=[
            UnitEvent(0, "idsse", "M1", 1, "running"),
            UnitEvent(0, "idsse", "M1", 1, "succeeded", rows_written=5),
            _slice(0),  # NB: no _slice(-1) — this drain has no sb360 worker
        ],
        result_counts={("idsse", "M1", 1): 5},
    )
    assert any("COMPLETE" in msg for _lvl, msg in records)
    assert all("DRAIN_FAILED" not in msg for _lvl, msg in records)


def test_gate_RAISES_on_a_missing_terminal(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """A CLEAN worker (slice emitted, no write failures) ran and an enqueued unit has NO terminal — the
    silent-skip class. That is INCOMPLETE and it fails the task."""
    with pytest.raises(DrainGateError, match="idsse:SKIPPED:1"):
        _drive(
            monkeypatch,
            queue=[QueueRow(0, "idsse", "SKIPPED", 1)],
            events=[_slice(0)],
        )


def test_gate_passes_the_EMPTY_extra_expected_workers_set(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """G1 made explicit: without ``frozenset()`` the sb360 sentinel would be expected, read as dead
    (never emits ``slice_completed``), and the gate would report DRAIN_FAILED on EVERY run."""
    captured: dict[str, Any] = {}
    real_evaluate = gate.evaluate

    def _spy(**kwargs: Any):
        captured.update(kwargs)
        return real_evaluate(**kwargs)

    monkeypatch.setattr(gate, "evaluate", _spy)
    _drive(
        monkeypatch,
        queue=[QueueRow(0, "idsse", "M1", 1)],
        events=[
            UnitEvent(0, "idsse", "M1", 1, "running"),
            UnitEvent(0, "idsse", "M1", 1, "succeeded", rows_written=5),
            _slice(0),
        ],
        result_counts={("idsse", "M1", 1): 5},
    )
    assert captured["extra_expected_workers"] == frozenset()


def test_gate_does_not_raise_on_DRAIN_FAILED(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """``run_if = ALL_DONE`` — a dead worker (no ``slice_completed``) REPORTS at ERROR, exit 0. The job
    already failed; the gate must not mask the drain's real exception with its own."""
    records = _drive(
        monkeypatch,
        queue=[QueueRow(0, "idsse", "M1", 1)],
        events=[UnitEvent(0, "idsse", "M1", 1, "running")],  # worker 0 never emitted slice_completed
    )
    errors = [msg for lvl, msg in records if lvl >= logging.ERROR]
    assert any("DRAIN_FAILED" in msg for msg in errors)


def test_gate_REFUSES_an_empty_run_id(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """run_id is ``{{job.run_id}}`` — an empty value dies LOUD naming the correct source rather than
    silently auditing a run with no events."""
    with pytest.raises(SystemExit, match=r"\{\{job.run_id\}\}"):
        _drive(monkeypatch, queue=[], events=[], run_id="")
