"""Task 7 — D8, the drain-completeness gate: the PURE verdict logic.

Every verdict is reachable here with NO Spark: the gate's rules are a pure function of three
already-materialised inputs (queue rows, unit events, per-unit persisted row counts) plus the
planner's re-run. The impure entry point (``ingestion.action_context_gate``) only adapts live
tables into these structures and turns the verdict into an exit code — it holds no rules.

THE DEFECT LEDGER THIS FILE PINS (plan §0 — every one of these shipped in a prior review round):

* **V1** — ``UNVERIFIABLE`` was RUN-scoped, so one lost event on worker 1 muted a genuine
  silent-skip accusation about worker 5. Taint is **per-worker**.
  → ``test_lossy_worker_does_not_mute_a_CLEAN_worker``
* **W1** — rule 1 was VACUOUSLY TRUE on a clean run (``all([])`` is ``True``), so a run with a
  single lost ``running`` event and no anomalies returned ``UNVERIFIABLE``. With ~390 fail-open
  commits, *some* loss is the EXPECTED case — the gate would have been muted by its own success.
  → ``test_lossy_but_NO_anomalies_is_COMPLETE_not_UNVERIFIABLE``
* **V6** — terminals are BATCHED, so an OOM-killed worker flushes NONE of them: its units,
  INCLUDING the ones that succeeded and wrote rows, all look like ``running`` with no terminal. A
  naive ``DRAIN_FAILED`` report calls them all "in-flight" and LIES.
  → ``test_DRAIN_FAILED_report_splits_completed_from_in_flight``
* **V7** — the planner tooth was too narrow. A unit with a ``succeeded`` event that CLAIMED rows and
  is STILL in the planner's ``remaining`` set means its rows did not land.
  → ``test_diagnostic_raises_when_a_SUCCEEDED_unit_is_still_remaining``
  ...and too WIDE: a legitimately-0-row unit (all SPADL actions NULL-``time_seconds``) stays
  ``remaining`` forever, so the alarm RAISED on a healthy run.
  → ``test_a_remaining_unit_that_SUCCEEDED_with_ZERO_rows_is_not_an_alarm``
* **The DELETED rule 3** (2026-07-13 review) — a mart row-count cross-check against ``rows_written``
  is TAUTOLOGICAL: ``rows_written`` IS a post-write count of the same mart slice (ADR-045). Per-unit
  row completeness is the DRAIN's job (ADR-067). The gate's one row-count rule is sb360-only.
  → ``test_a_ROW_COUNT_CROSS_CHECK_against_the_mart_is_NOT_a_rule``,
    ``test_sb360_succeeded_with_ZERO_rows_is_an_ANOMALY``
* **P2 / V2 / W3 / X1 / Y2 — the sb360 seam, defective in EVERY review round.** sb360 is never
  enqueued, so a queue-only gate says NOTHING about statsbomb. It must be an expected WORKER (via
  the imported sentinel) *and* its ``running`` events must be expected UNITS.
  → ``test_sb360_units_are_GATED``, ``test_sb360_task_death_before_ANY_event_is_DRAIN_FAILED``,
    ``test_gate_IMPORTS_the_sentinel_and_never_writes_the_literal``
* **P1** — a dead worker must REPORT, not RAISE: the job already failed; the gate's job is to say
  what died, not to mask the drain's exception with its own.
  → ``test_DRAIN_FAILED_reports_and_does_NOT_raise``
* **P4** — idle workers. The expected-worker set is QUEUE-DERIVED; hard-coding 8 makes the gate cry
  wolf on every small daily run (terraform's own comment: "daily runs are tiny").
  → ``test_idle_workers_do_not_look_dead``
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from analytics.action_context import drain_gate as dg
from analytics.action_context.drain import SB360_WORKER_ID
from analytics.action_context.drain_gate import (
    DrainGateError,
    PlannerInputs,
    QueueRow,
    UnitEvent,
    UnitKey,
    Verdict,
    enforce,
    evaluate,
    expected_units,
)

# ── builders ───────────────────────────────────────────────────────────


def _q(worker_id: int, match_id: str, period: int | None = 1, provider: str = "skillcorner") -> QueueRow:
    return QueueRow(worker_id=worker_id, provider=provider, match_id=match_id, period=period)


def _ev(
    worker_id: int,
    state: str,
    match_id: str,
    period: int | None = 1,
    provider: str = "skillcorner",
    rows_written: int | None = None,
    write_failures: int | None = None,
) -> UnitEvent:
    return UnitEvent(
        worker_id=worker_id,
        provider=provider,
        match_id=match_id,
        period=period,
        state=state,
        rows_written=rows_written,
        write_failures=write_failures,
    )


def _slice(worker_id: int, write_failures: int = 0) -> UnitEvent:
    """A ``slice_completed`` — the per-worker "I ran" stamp, carrying its lost-event count."""
    return UnitEvent(
        worker_id=worker_id,
        provider="__slice__",
        match_id="__slice__",
        period=None,
        state="slice_completed",
        write_failures=write_failures,
    )


def _unit_ran(worker_id: int, match_id: str, rows: int, period: int | None = 1) -> list[UnitEvent]:
    """The happy path for one unit: ``running`` then ``succeeded``."""
    return [
        _ev(worker_id, "running", match_id, period),
        _ev(worker_id, "succeeded", match_id, period, rows_written=rows),
    ]


def _sb360_slice() -> UnitEvent:
    """sb360's ``slice_completed``. The sentinel task is UNCONDITIONAL in the DAG, so the gate
    expects this on EVERY run — including a quiet day where sb360 discovered nothing."""
    return UnitEvent(
        worker_id=SB360_WORKER_ID,
        provider="__slice__",
        match_id="__slice__",
        period=None,
        state="slice_completed",
        write_failures=0,
    )


def _clean_run() -> dict[str, object]:
    """Two units on worker 0 (workers 1..7 IDLE), sb360 quiet. The ordinary daily shape."""
    return {
        "run_id": "JOBRUN1",
        "queue": [_q(0, "M1"), _q(0, "M2")],
        "events": [*_unit_ran(0, "M1", 10), *_unit_ran(0, "M2", 20), _slice(0), _sb360_slice()],
        "result_counts": {("skillcorner", "M1", 1): 10, ("skillcorner", "M2", 1): 20},
        "planner": PlannerInputs(enqueued=2, remaining=frozenset()),
    }


# ── baseline ───────────────────────────────────────────────────────────


def test_a_clean_run_is_COMPLETE_and_does_not_raise() -> None:  # noqa: N802
    report = evaluate(**_clean_run())  # type: ignore[arg-type]
    assert report.verdict is Verdict.COMPLETE
    assert report.must_raise is False
    enforce(report)  # a COMPLETE report must not raise (the test's own name promises this)


# ── G1 (review #1): the sb360 worker-topology axis is parameterized (tracking-marts has no sb360) ──


def test_no_sb360_drain_is_COMPLETE_when_extra_expected_workers_is_empty() -> None:  # noqa: N802
    # A tracking-marts-shaped run: worker 0 ran two units, its slice_completed landed, NO sb360 task.
    # extra_expected_workers=frozenset() -> worker -1 is NOT expected -> not dead -> COMPLETE (G1).
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1"), _q(0, "M2")],
        events=[*_unit_ran(0, "M1", 10), *_unit_ran(0, "M2", 20), _slice(0)],  # no sb360 slice
        result_counts={("skillcorner", "M1", 1): 10, ("skillcorner", "M2", 1): 20},
        planner=PlannerInputs(enqueued=2, remaining=frozenset()),
        extra_expected_workers=frozenset(),
    )
    assert report.verdict is Verdict.COMPLETE
    assert SB360_WORKER_ID not in report.expected_workers


def test_default_extra_expected_workers_still_expects_the_sb360_sentinel() -> None:
    # SAME run under the DEFAULT: the sb360 sentinel is expected, emits no slice -> dead -> DRAIN_FAILED.
    # Proves AC behaviour is byte-identical at the default (the sb360 seam stays welded for AC).
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1"), _q(0, "M2")],
        events=[*_unit_ran(0, "M1", 10), *_unit_ran(0, "M2", 20), _slice(0)],  # no sb360 slice
        result_counts={("skillcorner", "M1", 1): 10, ("skillcorner", "M2", 1): 20},
        planner=PlannerInputs(enqueued=2, remaining=frozenset()),
    )
    assert report.verdict is Verdict.DRAIN_FAILED
    assert SB360_WORKER_ID in report.expected_workers
    assert report.must_raise is False  # P1 — DRAIN_FAILED reports, does not raise
    enforce(report)  # must not raise


# ── P4: the expected-worker set is QUEUE-DERIVED ───────────────────────


def test_idle_workers_do_not_look_dead() -> None:
    """P4 — only ONE of eight workers has queue rows. Terraform's own comment says "daily runs are
    tiny", so most workers are idle most days. A gate that hard-codes 8 expected workers returns
    DRAIN_FAILED on every healthy daily run — the muting failure this design exists to prevent,
    arriving through the front door."""
    report = evaluate(**_clean_run())  # type: ignore[arg-type]
    assert report.verdict is Verdict.COMPLETE
    assert report.dead_workers == ()
    assert report.expected_workers == (SB360_WORKER_ID, 0), "expected workers must come from the QUEUE (+ sb360)"


def test_the_number_8_is_never_written_into_the_gate() -> None:
    """P4, at the source level: the drain fan-out width must not be a constant in the gate. The
    behavioural test above passes just as well on a gate that happens to be given a 1-worker run;
    only the source assertion pins that the set is DERIVED."""
    src = inspect.getsource(dg)
    literals = {
        node.value
        for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.Constant) and isinstance(node.value, int) and not isinstance(node.value, bool)
    }
    assert 8 not in literals, "the gate hard-codes the drain fan-out width; it must derive it from the queue"


# ── P1 + V6: a dead worker REPORTS, and the report must not lie ────────


def test_DRAIN_FAILED_reports_and_does_NOT_raise() -> None:  # noqa: N802
    """P1 — under ``run_if = ALL_DONE`` the gate runs even when the drain FAILED. An OOM-killed
    worker leaves ``running`` events with no terminal. Without this rule, "unit with no terminal"
    → INCOMPLETE → RAISE, and the gate masks the drain's real exception with its own. The job has
    already failed; the gate's job here is to SAY WHAT DIED, not to fail it again."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1"), _q(1, "M2")],
        events=[
            *_unit_ran(0, "M1", 10),
            _slice(0),
            _ev(1, "running", "M2"),  # worker 1 was OOM-killed: running, no terminal, NO slice_completed
            _sb360_slice(),
        ],
        result_counts={("skillcorner", "M1", 1): 10},
        planner=PlannerInputs(enqueued=2, remaining=frozenset({UnitKey("skillcorner", "M2", 1)})),
    )
    assert report.verdict is Verdict.DRAIN_FAILED
    assert report.must_raise is False
    assert report.dead_workers == (1,)
    enforce(report)  # must NOT raise


def test_DRAIN_FAILED_report_splits_completed_from_in_flight() -> None:  # noqa: N802
    """V6 — terminals are BATCHED, so an OOM-killed worker flushes NONE of them: its units,
    INCLUDING THE ONES THAT SUCCEEDED AND WROTE ROWS, all look like ``running`` with no terminal.
    A naive report names them all "in-flight" → the ALL_DONE payoff ships INACCURATE.

    The licence to batch was that terminal state is RECONSTRUCTIBLE from results. So reconstruct it.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(1, "DONE"), _q(1, "INFLIGHT"), _q(1, "NEVER")],
        events=[
            _ev(1, "running", "DONE"),  # started, rows LANDED, terminal lost with the driver
            _ev(1, "running", "INFLIGHT"),  # started, no rows -> genuinely in flight when it died
            # "NEVER" has no running event at all -> the worker died before it got there
            _sb360_slice(),
        ],
        result_counts={("skillcorner", "DONE", 1): 550},
        planner=PlannerInputs(enqueued=3, remaining=frozenset()),
    )
    assert report.verdict is Verdict.DRAIN_FAILED
    assert report.completed_terminal_lost == ("skillcorner:DONE:1",)
    assert report.in_flight == ("skillcorner:INFLIGHT:1",)
    assert report.never_started == ("skillcorner:NEVER:1",)
    assert report.must_raise is False


def test_DRAIN_FAILED_report_ALSO_names_clean_worker_anomalies() -> None:  # noqa: N802
    """Rule 0 pre-empts rules 2-3, so a run where one worker DIED *and* another has a genuine
    missing terminal would otherwise hide the second defect until the next run."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "GHOST"), _q(1, "DEAD")],
        events=[
            _ev(0, "running", "GHOST"),  # worker 0 finished its slice but this unit has NO terminal
            _slice(0),
            _ev(1, "running", "DEAD"),  # worker 1 died
            _sb360_slice(),
        ],
        result_counts={},
        planner=PlannerInputs(enqueued=2, remaining=frozenset()),
    )
    assert report.verdict is Verdict.DRAIN_FAILED
    assert report.dead_workers == (1,)
    assert report.clean_worker_anomalies == ("skillcorner:GHOST:1 — no terminal event",)


# ── the sb360 seam (P2 / V2 / W3 / X1 / Y2) ────────────────────────────


def test_sb360_units_are_GATED() -> None:  # noqa: N802
    """P2 — sb360 is NEVER enqueued, so a queue-only expectation set never examines it. The
    expected set is a UNION: queue rows (drain units) + sb360's ``running`` events (its persisted
    queue-equivalent). Here sb360 started two matches and only terminated one."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[],
        events=[
            _ev(SB360_WORKER_ID, "running", "3788741", period=None, provider="statsbomb"),
            _ev(SB360_WORKER_ID, "running", "3788746", period=None, provider="statsbomb"),
            _ev(SB360_WORKER_ID, "succeeded", "3788741", period=None, provider="statsbomb", rows_written=12),
            _sb360_slice(),
        ],
        result_counts={("statsbomb", "3788741", 1): 12},
        planner=PlannerInputs(enqueued=0, remaining=frozenset()),
    )
    assert report.verdict is Verdict.INCOMPLETE
    assert report.missing_terminals == ("statsbomb:3788746",)
    with pytest.raises(DrainGateError, match="3788746"):
        enforce(report)


def test_sb360_task_death_before_ANY_event_is_DRAIN_FAILED() -> None:  # noqa: N802
    """V2 — sb360 must be an expected WORKER, not merely expected UNITS.

    If the sb360 task dies BEFORE emitting its ``running`` events it contributes ZERO expected
    units — so a unit-level-only fix misses no unit, and the gate returns COMPLETE while statsbomb
    did nothing at all. The sb360 task is UNCONDITIONAL in the DAG, so it is ALWAYS expected.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1")],
        events=[*_unit_ran(0, "M1", 10), _slice(0)],  # NO sb360 slice_completed at all
        result_counts={("skillcorner", "M1", 1): 10},
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.DRAIN_FAILED
    assert report.dead_workers == (SB360_WORKER_ID,)
    assert report.must_raise is False


def test_sb360_period_NULL_units_ROLL_UP_the_marts_periods_in_the_V6_reconstruction() -> None:  # noqa: N802
    """sb360 is MATCH grain (``period`` NULL — it exits the per-period drain) while the results mart
    is PER-PERIOD. ``_persisted_rows`` must roll a match's periods up, or a dead sb360 task's
    COMPLETED matches would all be reconstructed as "in flight" and the DRAIN_FAILED report would
    lie about what landed (V6). This is now the ONLY consumer of the mart read."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1")],
        events=[
            *_unit_ran(0, "M1", 10),
            _slice(0),
            # sb360 started the match, wrote both periods, then DIED before flushing its terminals
            # and its slice_completed.
            _ev(SB360_WORKER_ID, "running", "3788741", period=None, provider="statsbomb"),
        ],
        result_counts={
            ("skillcorner", "M1", 1): 10,
            ("statsbomb", "3788741", 1): 12,
            ("statsbomb", "3788741", 2): 18,
        },
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.DRAIN_FAILED
    assert report.dead_workers == (SB360_WORKER_ID,)
    assert report.completed_terminal_lost == ("statsbomb:3788741",), "the match's periods must roll up"
    assert report.in_flight == ()


def test_gate_IMPORTS_the_sentinel_and_never_writes_the_literal() -> None:  # noqa: N802
    """X1/Y2 — the CONSUMER half. Writing ``-1`` in the gate is the exact drift W3 was raised to
    kill, and it is the path an implementer takes when the constant sits somewhere the gate cannot
    import from. ``analytics`` cannot import ``ingestion``, so the sentinel lives in
    ``analytics.action_context.drain`` and BOTH sides import it from there."""
    import ingestion.action_context_gate as gate_entry

    assert dg.SB360_WORKER_ID is SB360_WORKER_ID
    for module in (dg, gate_entry):
        src = inspect.getsource(module)
        assert _minus_one_literals(src) == [], f"{module.__name__} writes the literal -1 instead of importing it"
        assert _sentinel_redefinitions(src) == [], f"{module.__name__} REDEFINES the sentinel"


def _minus_one_literals(src: str) -> list[int]:
    """Every ``-1`` written as a literal (``ast`` renders it as ``USub(Constant(1))``)."""
    return [
        node.lineno
        for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and _is_int_const(node.operand, 1)
    ] + [
        node.lineno
        for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.Constant) and node.value == -1 and isinstance(node.value, int)
    ]


def _is_int_const(node: ast.expr, value: int) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, int) and node.value == value


def _sentinel_redefinitions(src: str) -> list[int]:
    return [
        node.lineno
        for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.Assign)
        for t in node.targets
        if isinstance(t, ast.Name) and t.id == "SB360_WORKER_ID"
    ]


def test_sentinel_guard_FAILS_on_a_planted_literal() -> None:  # noqa: N802 -- §0b
    """§0b: an invariant guard that has never failed is not a guard. Plant the exact drift."""
    assert _minus_one_literals("def f(w):\n    return w == -1\n"), "guard missed the hardcoded sentinel literal"
    assert _sentinel_redefinitions("SB360_WORKER_ID = -1\n"), "guard missed a redefinition"
    assert _minus_one_literals("from analytics.action_context.drain import SB360_WORKER_ID\n") == []


# ── INCOMPLETE (rules 2 + 3) ───────────────────────────────────────────


def test_INCOMPLETE_raises_and_names_the_units() -> None:  # noqa: N802
    """Rule 2 — a CLEAN worker (``write_failures == 0``, ``slice_completed`` present) has an
    enqueued unit with NO terminal. The worker RAN and the queue read returned nothing for that
    unit: that is the silent-skip class, and the accusation is trustworthy."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1"), _q(0, "SKIPPED")],
        events=[*_unit_ran(0, "M1", 10), _slice(0), _sb360_slice()],
        result_counts={("skillcorner", "M1", 1): 10},
        planner=PlannerInputs(enqueued=2, remaining=frozenset()),
    )
    assert report.verdict is Verdict.INCOMPLETE
    assert report.missing_terminals == ("skillcorner:SKIPPED:1",)
    assert report.must_raise is True
    with pytest.raises(DrainGateError, match="skillcorner:SKIPPED:1"):
        enforce(report)


def test_a_ROW_COUNT_CROSS_CHECK_against_the_mart_is_NOT_a_rule() -> None:  # noqa: N802
    """The DELETED rule 3 (2026-07-13 review). It compared a ``succeeded`` unit's ``rows_written``
    against a fresh read of the results mart and was sold as an independent "did the rows land"
    cross-check. It was **tautological**: ``_process_tracking_match`` calls ``write_delta_table``
    with NO ``row_count``, so (ADR-045) the value it returns — and hence ``rows_written`` — is
    ITSELF a post-write count of the ``replaceWhere`` mart slice. The gate then re-read the same
    mart with the same predicate. Same quantity, both sides.

    Per-unit row completeness is owned by the DRAIN (ADR-067's bronze-anchored invariant, which
    RAISES and produces a ``failed`` terminal — a state the deleted rule skipped, so it could not
    even detect ``skillcorner:1552423:2``, the incident it named).

    So: the drain says 550, the mart holds 0, and the gate is silent. That is CORRECT — and it is
    pinned, because "add a mart cross-check" is the obvious thing for the next reader to re-add.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "1552423", period=2)],
        events=[*_unit_ran(0, "1552423", 550, period=2), _slice(0), _sb360_slice()],
        result_counts={},  # nothing landed -- and the gate does not (and must not) care
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.COMPLETE
    assert report.must_raise is False
    assert not hasattr(report, "count_mismatches"), "rule 3 is deleted -- do not re-add the mart cross-check"


# ── rule 3: sb360's zero-row match (the ONE row-count rule the gate owns) ──


def test_sb360_succeeded_with_ZERO_rows_is_an_ANOMALY() -> None:  # noqa: N802
    """sb360 is the ONE producer with no per-unit completeness invariant AND no planner alarm.

    ``_sb360_terminals`` stamps EVERY discovered match ``succeeded``, with ``rows_written`` read
    back from the mart — so a match that silently produced nothing compares ``0 == 0`` and reads as
    COMPLETE. ``PlannerInputs.remaining`` deliberately excludes statsbomb, so V7 is blind to it too.
    Net, before this rule: an sb360 match that produced no AC rows is re-enumerated forever under a
    green verdict — the exact class D8 exists for.

    Discovery REQUIRES the match in ``bronze.statsbomb_360`` (ADR-057, frames-required), so zero
    rows is ANOMALOUS, not legitimate.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[],
        events=[
            _ev(SB360_WORKER_ID, "running", "3788746", period=None, provider="statsbomb"),
            _ev(SB360_WORKER_ID, "succeeded", "3788746", period=None, provider="statsbomb", rows_written=0),
            _sb360_slice(),
        ],
        result_counts={},
        planner=PlannerInputs(enqueued=0, remaining=frozenset()),
    )
    assert report.verdict is Verdict.INCOMPLETE
    assert report.empty_sb360_matches == ("statsbomb:3788746 — sb360 'succeeded' having written 0 rows",)
    assert report.must_raise is True
    with pytest.raises(DrainGateError, match="3788746"):
        enforce(report)


def test_a_LOSSY_sb360_worker_mutes_its_own_zero_row_anomaly() -> None:  # noqa: N802
    """Same clean/lossy taint semantics as every other anomaly class (V1): if the sb360 writer ITSELF
    lost events, its evidence is known-lossy and the accusation must be reported, not made."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[],
        events=[
            _ev(SB360_WORKER_ID, "running", "3788746", period=None, provider="statsbomb"),
            _ev(SB360_WORKER_ID, "succeeded", "3788746", period=None, provider="statsbomb", rows_written=0),
            UnitEvent(
                worker_id=SB360_WORKER_ID,
                provider="__slice__",
                match_id="__slice__",
                period=None,
                state="slice_completed",
                write_failures=1,
            ),
        ],
        result_counts={},
        planner=PlannerInputs(enqueued=0, remaining=frozenset()),
    )
    assert report.verdict is Verdict.UNVERIFIABLE
    assert report.must_raise is False
    assert report.empty_sb360_matches == ()
    assert report.untrusted_anomalies == ("statsbomb:3788746 — sb360 'succeeded' having written 0 rows",)


def test_a_DRAIN_unit_with_ZERO_rows_is_REPORTED_not_ACCUSED() -> None:  # noqa: N802
    """Rule 3 is sb360-ONLY, deliberately. A drain ``(match, period)`` whose SPADL actions all carry
    a NULL ``time_seconds`` has ``bronze_expected == 0``, the completeness invariant returns early,
    and the unit legitimately writes 0 rows. Accusing it would fail a healthy run."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "EMPTY", period=2)],
        events=[*_unit_ran(0, "EMPTY", 0, period=2), _slice(0), _sb360_slice()],
        result_counts={},
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.COMPLETE
    assert report.must_raise is False
    assert report.succeeded_with_zero_rows == ("skillcorner:EMPTY:2",)
    assert "ZERO rows" in report.render()


# ── failed units are NAMED (they were silently COMPLETE before) ────────


def test_a_FAILED_unit_is_NAMED_in_the_report_with_its_error() -> None:  # noqa: N802
    """``failed`` is a TERMINAL, so a run with a demonstrably failed unit produced COMPLETE and the
    report never mentioned it. That is only *safe* because ADR-067's ``raise_on_failed_units``
    independently fails the worker's task — i.e. the gate, sold as defence-in-depth for exactly this
    class, provided none. The verdict stays non-raising (raising here would only mask the drain's
    real exception with the gate's), but the report MUST name it, with the error text that
    ``UnitEvent.error`` was already carrying to the driver and throwing away."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "BOOM", period=2)],
        events=[
            _ev(0, "running", "BOOM", period=2),
            UnitEvent(0, "skillcorner", "BOOM", 2, "failed", error="completeness invariant: 0 of 550 actions"),
            _slice(0),
            _sb360_slice(),
        ],
        result_counts={},
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.failed_units == ("skillcorner:BOOM:2 — completeness invariant: 0 of 550 actions",)
    rendered = report.render()
    assert "skillcorner:BOOM:2" in rendered
    assert "0 of 550 actions" in rendered, "the error text is collected -- it must also be RENDERED"
    assert report.must_raise is False  # ADR-067 already failed the worker's task


def test_the_report_names_the_EXPECTED_WORKERS() -> None:  # noqa: N802
    """``expected_workers`` was populated and asserted in tests, but never rendered — so an operator
    reading a DRAIN_FAILED log could not see which workers were even expected."""
    report = evaluate(**_clean_run())  # type: ignore[arg-type]
    assert f"{[SB360_WORKER_ID, 0]}" in report.render()


# ── timeouts (excused everywhere; skipped by the cross-check) ──────────


def test_timed_out_is_excused() -> None:
    """Timeouts roll forward by design (a capacity signal, not a correctness one) — ADR-067's
    ``raise_on_failed_units`` excludes them too."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "SLOW")],
        events=[_ev(0, "running", "SLOW"), _ev(0, "timed_out", "SLOW"), _slice(0), _sb360_slice()],
        result_counts={},
        planner=PlannerInputs(enqueued=1, remaining=frozenset({UnitKey("skillcorner", "SLOW", 1)})),
    )
    assert report.verdict is Verdict.COMPLETE
    assert report.must_raise is False


def test_timed_out_WITH_rows_present_is_LEGAL() -> None:  # noqa: N802
    """Spec §9: the watchdog ABANDONS non-interruptible threads that are still alive
    (``drain.py``), so a zombie can write its unit's rows AFTER its ``timed_out`` event. The write
    is ``replaceWhere``-scoped, hence idempotent. A ``timed_out`` unit with rows present is a legal
    state, not a contradiction — no rule may key on it."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "ZOMBIE")],
        events=[_ev(0, "running", "ZOMBIE"), _ev(0, "timed_out", "ZOMBIE"), _slice(0), _sb360_slice()],
        result_counts={("skillcorner", "ZOMBIE", 1): 4242},  # the zombie's late write
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.COMPLETE
    assert report.must_raise is False


# ── V1 + W1: per-worker taint ──────────────────────────────────────────


def test_lossy_worker_does_not_mute_a_CLEAN_worker() -> None:  # noqa: N802
    """V1 — taint is PER-WORKER. Worker 1 lost an event; worker 5 has a genuine missing terminal.
    A RUN-scoped ``UNVERIFIABLE`` would have SUPPRESSED a real accusation — one lost event on one
    worker muting the gate for the other seven. That is the muting failure this design exists to
    prevent, arriving by a third route."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(1, "LOSSY"), _q(5, "SKIPPED")],
        events=[
            _ev(1, "running", "LOSSY"),  # worker 1's TERMINAL was lost (write_failures=1 below)
            _slice(1, write_failures=1),
            _slice(5, write_failures=0),  # worker 5 is CLEAN and never terminated its unit
            _sb360_slice(),
        ],
        result_counts={("skillcorner", "LOSSY", 1): 99},
        planner=PlannerInputs(enqueued=2, remaining=frozenset()),
    )
    assert report.verdict is Verdict.INCOMPLETE
    assert report.lossy_workers == (1,)
    assert report.missing_terminals == ("skillcorner:SKIPPED:1",), "the LOSSY worker's anomaly must not be accused"
    assert report.untrusted_anomalies == ("skillcorner:LOSSY:1 — no terminal event",)
    with pytest.raises(DrainGateError, match="skillcorner:SKIPPED:1"):
        enforce(report)


def test_lossy_but_NO_anomalies_is_COMPLETE_not_UNVERIFIABLE() -> None:  # noqa: N802
    """W1 — ``all([])`` is ``True``. Every unit terminal, every count matching, worker 3 merely
    lost one ``running`` event → COMPLETE (with a warning). Without the NON-EMPTY clause this
    returns UNVERIFIABLE — and since SOME loss is the EXPECTED case at ~390 fail-open commits, the
    gate would return a non-verdict most days and be muted by its own success.

    A lost *terminal* would itself have produced an anomaly, so "no anomalies despite losses" means
    only ``running`` events were lost — which costs OOM-visibility, not correctness.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(3, "M1")],
        events=[
            # the `running` event was LOST (write_failures=1); the terminal landed
            _ev(3, "succeeded", "M1", rows_written=10),
            _slice(3, write_failures=1),
            _sb360_slice(),
        ],
        result_counts={("skillcorner", "M1", 1): 10},
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.COMPLETE
    assert report.write_failures_total == 1
    assert report.must_raise is False


def test_every_anomaly_inside_a_LOSSY_worker_is_UNVERIFIABLE() -> None:  # noqa: N802
    """Rule 1 — the evidence for THIS worker is known-lossy, so its missing terminal may be a lost
    event rather than a skipped unit. No signal must never masquerade as negative signal: report,
    do not accuse."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(2, "MAYBE")],
        events=[_ev(2, "running", "MAYBE"), _slice(2, write_failures=1), _sb360_slice()],
        result_counts={("skillcorner", "MAYBE", 1): 77},
        planner=PlannerInputs(enqueued=1, remaining=frozenset()),
    )
    assert report.verdict is Verdict.UNVERIFIABLE
    assert report.must_raise is False
    assert report.untrusted_anomalies == ("skillcorner:MAYBE:1 — no terminal event",)
    enforce(report)  # must NOT raise


# ── the planner diagnostic (M2 + V7) ───────────────────────────────────


def test_diagnostic_raises_on_planner_collapse() -> None:
    """M2 — ``enqueued == 0`` AND ``remaining > 0``: "the planner stopped seeing work" is not a
    backlog. NOTE (W2): this canNOT catch an UNDER-enumerating planner, because ``remaining``
    re-runs the SAME function — a broken planner returns 0 for both, and the tooth never fires. Its
    real job is an empty queue caused by something OTHER than the planner (e.g. a failed enqueue)."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[],
        events=[_slice(0), _sb360_slice()],
        result_counts={},
        planner=PlannerInputs(enqueued=0, remaining=frozenset({UnitKey("skillcorner", "M1", 1)})),
    )
    assert report.planner_alarms
    assert report.must_raise is True
    with pytest.raises(DrainGateError, match="planner"):
        enforce(report)


def test_diagnostic_raises_when_a_SUCCEEDED_unit_is_still_remaining() -> None:  # noqa: N802
    """V7 — the independent WRITE-LANDED check: we said the unit succeeded, and the planner STILL
    sees it as unwritten ⇒ its rows did not land. Sound only because the Task-5 planner grain fix
    removes the zero-action class (a zero-action unit would legitimately succeed with 0 rows and
    stay remaining forever), which is why it sits BEHIND Task 5's 374-count hard gate."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "M1")],
        events=[*_unit_ran(0, "M1", 550), _slice(0), _sb360_slice()],
        result_counts={("skillcorner", "M1", 1): 550},
        planner=PlannerInputs(enqueued=1, remaining=frozenset({UnitKey("skillcorner", "M1", 1)})),
    )
    assert report.verdict is Verdict.COMPLETE, "the drain's own evidence is self-consistent — only the planner dissents"
    assert report.planner_alarms == (
        "skillcorner:M1:1 — event says 'succeeded' but the planner still sees it as unwritten",
    )
    assert report.must_raise is True
    with pytest.raises(DrainGateError, match="M1"):
        enforce(report)


def test_a_remaining_unit_that_SUCCEEDED_with_ZERO_rows_is_not_an_alarm() -> None:  # noqa: N802
    """V7 is the FALSE-ACCUSATION class, and it RAISES — so it must not fire on a healthy run.

    A ``(match, period)`` whose SPADL actions all carry a NULL ``time_seconds`` has
    ``bronze_expected == 0``: the completeness invariant returns early, the unit succeeds having
    written 0 rows, it never lands in the mart, and the planner therefore keeps enumerating it —
    FOREVER. ``succeeded ∩ remaining`` would then RAISE on every subsequent run. The alarm keys on
    units that CLAIMED rows; the 0-row ones are reported separately instead.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "NULLTIME")],
        events=[*_unit_ran(0, "NULLTIME", 0), _slice(0), _sb360_slice()],
        result_counts={},
        planner=PlannerInputs(enqueued=1, remaining=frozenset({UnitKey("skillcorner", "NULLTIME", 1)})),
    )
    assert report.planner_alarms == ()
    assert report.must_raise is False
    assert report.succeeded_with_zero_rows == ("skillcorner:NULLTIME:1",), "suspicious -- so still REPORTED"


def test_a_remaining_unit_that_TIMED_OUT_is_not_an_alarm() -> None:  # noqa: N802
    """The V7 tooth keys on ``succeeded`` ONLY. A timed-out unit rolls forward BY DESIGN — it is
    still remaining, and that is the expected state, not a write-landing failure."""
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "SLOW")],
        events=[_ev(0, "running", "SLOW"), _ev(0, "timed_out", "SLOW"), _slice(0), _sb360_slice()],
        result_counts={},
        planner=PlannerInputs(enqueued=1, remaining=frozenset({UnitKey("skillcorner", "SLOW", 1)})),
    )
    assert report.planner_alarms == ()
    assert report.must_raise is False


# ── expected_units: the shared union (the entry point reuses it) ───────


def test_expected_units_is_the_UNION_of_queue_rows_and_sb360_running_events() -> None:  # noqa: N802
    """The expectation set has TWO producers with different lifecycles — the drain (queue rows) and
    sb360 (never enqueued; its ``running`` events ARE its persisted queue-equivalent). One pure
    helper owns the union, so the entry point cannot scope its mart read to a DIFFERENT set than
    the one the rules evaluate."""
    owners = expected_units(
        queue=[_q(0, "M1"), _q(4, "M2")],
        events=[
            _ev(SB360_WORKER_ID, "running", "3788741", period=None, provider="statsbomb"),
            _ev(0, "running", "M1"),  # a DRAIN worker's running event adds nothing — the queue owns that set
            _ev(0, "running", "GHOST"),  # not enqueued -> NOT expected (the queue is the drain's contract)
        ],
    )
    assert owners == {
        UnitKey("skillcorner", "M1", 1): 0,
        UnitKey("skillcorner", "M2", 1): 4,
        UnitKey("statsbomb", "3788741", None): SB360_WORKER_ID,
    }


def test_a_provider_with_BOTH_grains_does_not_crash_the_gate() -> None:  # noqa: N802
    """The gate sorts units to render a stable report, and ``period`` is NULL for match-grain units.
    An ``order=True`` dataclass would compare ``None < 1`` on two units of the SAME match and raise
    ``TypeError`` — the gate CRASHING instead of reporting, on exactly the day it is needed.

    No provider mixes grains today (sb360 is match-grain, the drain is period-grain), so this is
    LATENT — and latent is not safe: it is a landmine under a gate whose entire value is being
    trusted. Pinned rather than argued.
    """
    report = evaluate(
        run_id="JOBRUN1",
        queue=[_q(0, "MIX", period=None), _q(0, "MIX", period=1)],
        events=[_slice(0), _sb360_slice()],
        result_counts={},
        planner=PlannerInputs(enqueued=2, remaining=frozenset()),
    )
    assert report.verdict is Verdict.INCOMPLETE
    assert report.missing_terminals == ("skillcorner:MIX", "skillcorner:MIX:1")


# ── the module is PURE ─────────────────────────────────────────────────


def _imported_roots(src: str) -> set[str]:
    """Every top-level package this module imports — including FUNCTION-LOCAL imports, which is how
    ``ingestion.action_context`` legitimately pulls pyspark and how the pure core would smuggle it."""
    roots: set[str] = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            roots |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".")[0])
    return roots


def test_the_gate_core_is_PURE() -> None:  # noqa: N802
    """``analytics`` must not import ``ingestion`` (.importlinter ``analytics-isolation``), and the
    verdict logic must be reachable with NO Spark at all — every test in this file is that proof.

    The AST check catches the shape ``lint-imports`` would miss and the plan explicitly warns about:
    a FUNCTION-LOCAL ``from pyspark.sql import functions`` (the house style everywhere else in this
    package). It is the whole reason the gate splits into a pure core and an impure entry point.
    """
    src = Path(inspect.getsourcefile(dg) or "").read_text(encoding="utf-8")
    assert _imported_roots(src) & {"ingestion", "pyspark", "workflows"} == set()


def test_purity_guard_FAILS_on_a_planted_function_local_pyspark_import() -> None:  # noqa: N802 -- §0b
    """§0b: prove the guard fires on the exact shape it exists for."""
    planted = "def read(spark):\n    from pyspark.sql import functions as F\n    return F\n"
    assert "pyspark" in _imported_roots(planted)
