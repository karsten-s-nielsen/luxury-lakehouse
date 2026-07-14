"""Task 7 — D8's IMPURE half: ``ingestion.action_context_gate``.

The entry point holds NO rules. It (a) resolves ``run_id``, (b) adapts three persisted tables into
the pure core's inputs, (c) re-runs the planner for the diagnostic, (d) logs the verdict, and (e)
turns ``must_raise`` into an exit code. Everything asserted here is about THAT adaptation — the
verdict logic itself is pinned, Spark-free, in ``test_drain_gate.py``.

TWO defects this file pins, both of which shipped in a prior round of this same workstream:

* **P1 / spec §6** — ``run_if = ALL_DONE`` means the gate runs even when the drain FAILED. Only
  ``INCOMPLETE`` (and the planner's raising diagnostics) may fail the task; ``DRAIN_FAILED`` and
  ``UNVERIFIABLE`` REPORT. A gate that raises on a dead worker masks the drain's real exception
  with its own.
* **§0c-bis** — ``run_id`` is ``{{job.run_id}}``, **NOT** the preflight task value. The preflight
  value is ``""`` on a nothing-to-do run, while sb360 files its events under the real job run id —
  a gate reading the task value would report **DRAIN_FAILED every quiet day**. So an EMPTY
  ``--run-id`` must die LOUD, naming the correct source, rather than silently evaluating a run that
  has no events.
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

import pytest

import ingestion.action_context_gate as gate
from analytics.action_context.drain import SB360_WORKER_ID
from analytics.action_context.drain_gate import (
    DrainGateError,
    GateReport,
    QueueRow,
    UnitEvent,
    UnitKey,
    Verdict,
)


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
    counts_fn: Any = None,
    remaining: frozenset[UnitKey] = frozenset(),
    run_id: str = "JOBRUN42",
) -> list[tuple[int, str]]:
    """Run ``main()`` with every Spark seam faked; return the (level, message) log records."""
    ns = argparse.Namespace(catalog="cat", schema="bronze", run_id=run_id)
    records: list[tuple[int, str]] = []

    class _Log(logging.Logger):
        def __init__(self) -> None:
            super().__init__("test", logging.DEBUG)  # explicit: a NOTSET logger drops INFO via isEnabledFor

        def _log(self, level: int, msg: object, args: Any, **kw: Any) -> None:  # type: ignore[override]
            records.append((level, str(msg) % args if args else str(msg)))

    monkeypatch.setattr(gate, "parse_ingestion_args", lambda *a, **k: ns)
    monkeypatch.setattr(gate, "get_spark_session", lambda: object())
    monkeypatch.setattr(gate, "configure_logging", lambda *a, **k: _Log())
    import ingestion.bootstrap as bs

    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(gate, "_read_queue", lambda *a, **k: list(queue))
    monkeypatch.setattr(gate, "_read_events", lambda *a, **k: list(events))
    monkeypatch.setattr(gate, "_read_result_counts", counts_fn or (lambda *a, **k: dict(result_counts or {})))
    monkeypatch.setattr(gate, "_remaining_units", lambda *a, **k: remaining)

    gate.main()
    return records


# ── the raise/report split (P1 / spec §6) ──────────────────────────────


def test_entrypoint_does_NOT_raise_on_COMPLETE(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    records = _drive(
        monkeypatch,
        queue=[QueueRow(0, "skillcorner", "M1", 1)],
        events=[
            UnitEvent(0, "skillcorner", "M1", 1, "running"),
            UnitEvent(0, "skillcorner", "M1", 1, "succeeded", rows_written=7),
            _slice(0),
            _slice(SB360_WORKER_ID),
        ],
        result_counts={("skillcorner", "M1", 1): 7},
    )
    assert any("COMPLETE" in msg for _lvl, msg in records)


def test_entrypoint_raises_on_INCOMPLETE(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """The one verdict that fails the task: a CLEAN worker ran and an enqueued unit has no terminal.
    That is the silent-skip class (`skillcorner:1552423:2` wrote 0 of 550 actions inside a run that
    reported SUCCESS), and the accusation is trustworthy."""
    with pytest.raises(DrainGateError, match="skillcorner:SKIPPED:1"):
        _drive(
            monkeypatch,
            queue=[QueueRow(0, "skillcorner", "SKIPPED", 1)],
            events=[_slice(0), _slice(SB360_WORKER_ID)],
        )


def test_entrypoint_does_NOT_raise_on_DRAIN_FAILED(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """P1 — under ``run_if = ALL_DONE`` the gate runs even when the drain failed. The job has
    ALREADY failed; the gate's job here is to SAY WHAT DIED, not to fail it again and mask the
    drain's real exception with its own. It must log at ERROR and exit 0."""
    records = _drive(
        monkeypatch,
        queue=[QueueRow(0, "skillcorner", "M1", 1)],
        events=[UnitEvent(0, "skillcorner", "M1", 1, "running"), _slice(SB360_WORKER_ID)],
    )
    report = [msg for lvl, msg in records if lvl >= logging.ERROR]
    assert report, "a DRAIN_FAILED verdict must be logged at ERROR (warnings are invisible in error-log queries)"
    assert any("DRAIN_FAILED" in msg for msg in report)


def test_entrypoint_does_NOT_raise_on_UNVERIFIABLE(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """Known-lossy evidence must never produce a confident accusation (spec §4). Report, exit 0."""
    records = _drive(
        monkeypatch,
        queue=[QueueRow(2, "skillcorner", "MAYBE", 1)],
        events=[
            UnitEvent(2, "skillcorner", "MAYBE", 1, "running"),
            _slice(2, write_failures=1),
            _slice(SB360_WORKER_ID),
        ],
    )
    assert any("UNVERIFIABLE" in msg for lvl, msg in records if lvl >= logging.ERROR)


def test_entrypoint_raises_on_a_planner_alarm_even_when_the_verdict_is_COMPLETE(  # noqa: N802
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """V7 — the drain's self-report is self-consistent (COMPLETE), and the planner still sees the
    unit as unwritten ⇒ its rows did not land. The alarm is derived from the RESULTS mart + the
    planner, NOT from the (fail-open, lossy) event log, so it raises on its own authority."""
    with pytest.raises(DrainGateError, match="planner"):
        _drive(
            monkeypatch,
            queue=[QueueRow(0, "skillcorner", "M1", 1)],
            events=[
                UnitEvent(0, "skillcorner", "M1", 1, "running"),
                UnitEvent(0, "skillcorner", "M1", 1, "succeeded", rows_written=5),
                _slice(0),
                _slice(SB360_WORKER_ID),
            ],
            result_counts={("skillcorner", "M1", 1): 5},
            remaining=frozenset({UnitKey("skillcorner", "M1", 1)}),
        )


# ── §0c-bis: run_id is {{job.run_id}}, never the preflight task value ──


def test_entrypoint_REFUSES_an_empty_run_id(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """§0c-bis — found in Task 6, and it BINDS THE CONSUMER.

    ``preflight_action_context`` sets its ``action_context_run_id`` task value to ``""`` on a
    nothing-to-do run, while sb360 files its events under ``{{job.run_id}}`` (it does not even
    depend on preflight, so the task value is not resolvable there). A gate wired to the preflight
    task value would therefore evaluate run ``""``, find NO sb360 ``slice_completed``, and report
    **DRAIN_FAILED every quiet day** — X2 relocated into the consumer.

    So an empty ``--run-id`` dies LOUD and names the correct source, instead of silently evaluating
    a run that has no events.
    """
    with pytest.raises(SystemExit, match=r"\{\{job.run_id\}\}"):
        _drive(monkeypatch, queue=[], events=[], run_id="")


def test_entrypoint_declares_run_id_and_documents_the_JOB_run_id_source(  # noqa: N802
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``--run-id`` help text is the only place a terraform author reads before wiring the task.
    It must say ``{{job.run_id}}`` — the preflight task value is the trap (§0c-bis)."""
    captured: list[list[tuple[str, dict[str, Any]]]] = []

    def _capture(description: str, extra_args: list[tuple[str, dict[str, Any]]] | None = None) -> argparse.Namespace:
        captured.append(list(extra_args or []))
        return argparse.Namespace(catalog="cat", schema="bronze", run_id="R")

    monkeypatch.setattr(gate, "parse_ingestion_args", _capture)
    monkeypatch.setattr(gate, "get_spark_session", lambda: object())
    import ingestion.bootstrap as bs

    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)
    monkeypatch.setattr(gate, "_read_queue", lambda *a, **k: [])
    monkeypatch.setattr(gate, "_read_events", lambda *a, **k: [_slice(SB360_WORKER_ID)])
    monkeypatch.setattr(gate, "_read_result_counts", lambda *a, **k: {})
    monkeypatch.setattr(gate, "_remaining_units", lambda *a, **k: frozenset())

    gate.main()

    declared = dict(captured[0])
    assert "--run-id" in declared
    assert "{{job.run_id}}" in declared["--run-id"]["help"]


# ── the adaptation itself ──────────────────────────────────────────────


def test_the_mart_read_is_SCOPED_to_the_run_and_reuses_the_pure_expectation_set(  # noqa: N802
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cross-check must not scan the whole mart (spec §8 / Task 7 step 4) — and it must scope to
    the SAME unit set the rules evaluate, or a unit could be judged against a count that was never
    fetched. So the scope comes from the pure ``expected_units`` helper, not from a second, drifting
    definition in the entry point."""
    seen: list[list[UnitKey]] = []

    def _spy(spark: object, catalog: str, schema: str, keys: list[UnitKey]) -> dict[Any, int]:
        seen.append(sorted(keys, key=lambda k: (k.provider, k.match_id)))
        return {}

    # NB: sb360's terminal claims 12 rows, NOT 0. A 0-row sb360 match is an ANOMALY (rule 3) and
    # would RAISE here -- this fixture previously encoded that very shape as the expected/passing
    # one, which is how the false pass hid.
    _drive(
        monkeypatch,
        counts_fn=_spy,
        queue=[QueueRow(0, "skillcorner", "M1", 1)],
        events=[
            UnitEvent(0, "skillcorner", "M1", 1, "running"),
            UnitEvent(0, "skillcorner", "M1", 1, "timed_out"),
            UnitEvent(SB360_WORKER_ID, "statsbomb", "3788741", None, "running"),
            UnitEvent(SB360_WORKER_ID, "statsbomb", "3788741", None, "succeeded", rows_written=12),
            _slice(0),
            _slice(SB360_WORKER_ID),
        ],
    )
    assert seen == [[UnitKey("skillcorner", "M1", 1), UnitKey("statsbomb", "3788741", None)]]


def test_entrypoint_RAISES_on_a_zero_row_sb360_match(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """The false-pass this file used to encode. sb360 stamps EVERY discovered match ``succeeded``
    with a mart read-back, so a match that silently produced nothing compares ``0 == 0``; and
    ``PlannerInputs.remaining`` excludes statsbomb, so V7 is blind to it. It would be re-enumerated
    forever under a green verdict. Discovery requires the match in ``bronze.statsbomb_360``
    (ADR-057), so 0 rows is anomalous -- and the gate now fails the task on it."""
    with pytest.raises(DrainGateError, match="3788741"):
        _drive(
            monkeypatch,
            queue=[],
            events=[
                UnitEvent(SB360_WORKER_ID, "statsbomb", "3788741", None, "running"),
                UnitEvent(SB360_WORKER_ID, "statsbomb", "3788741", None, "succeeded", rows_written=0),
                _slice(SB360_WORKER_ID),
            ],
        )


def test_the_verdict_is_the_PURE_cores(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: N802
    """The entry point must hold no rules of its own: whatever ``evaluate`` returns is what gets
    logged and enforced. Swap the core for a stub and prove the entry point obeys it."""
    stub = GateReport(
        verdict=Verdict.INCOMPLETE,
        run_id="JOBRUN42",
        expected_workers=(),
        dead_workers=(),
        lossy_workers=(),
        write_failures_total=0,
        missing_terminals=("stub:unit:9",),
        empty_sb360_matches=(),
        untrusted_anomalies=(),
        clean_worker_anomalies=(),
        failed_units=(),
        succeeded_with_zero_rows=(),
        completed_terminal_lost=(),
        in_flight=(),
        never_started=(),
        planner_alarms=(),
        planner_remaining=0,
    )
    monkeypatch.setattr(gate, "evaluate", lambda **_kw: stub)
    with pytest.raises(DrainGateError, match="stub:unit:9"):
        _drive(monkeypatch, queue=[], events=[_slice(SB360_WORKER_ID)])
