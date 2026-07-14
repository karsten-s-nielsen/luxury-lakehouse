"""D8 — the action-context drain-completeness gate: the PURE verdict logic.

See ``docs/superpowers/specs/2026-07-13-ac-unit-events-and-drain-completeness-gate-design.md``
(v3) and the plan of the same date. The impure entry point is ``ingestion.action_context_gate``.

WHY THIS EXISTS
---------------
``skillcorner:1552423:2`` wrote **0 of its 550 actions** while the job reported SUCCESS
(2026-07-11). ADR-067 fixed the cause and made a *failing* unit fail its task. Two gaps remained:
nothing asserted that the drain **finished its work**, and the work-queue records only what was
**planned** (``enqueue`` is its sole write path). D9 persists a per-unit lifecycle event log; D8 —
this module — reads it and answers one question:

    *For this ``run_id``: did every expected unit actually RUN, and reach a terminal?*

Note what that question does NOT include: *"and did its rows land?"*. That belongs to the DRAIN — see
"WHAT THIS GATE DOES NOT OWN" below. Overstating it is how the tautological rule 3 got written.

THE SHAPE OF THE MODULE (and why the split is load-bearing)
-----------------------------------------------------------
Everything here is **pure**: it takes already-materialised inputs and returns a ``GateReport``.
``analytics`` cannot import ``ingestion`` (.importlinter ``analytics-isolation``), and — more to
the point — a verdict that can only be produced by a live Spark session is a verdict that is never
tested. Every one of the four verdicts below is reachable in an offline unit test.

THE EVIDENCE INVARIANT (spec §0)
--------------------------------
The gate's evidence comes ONLY from persisted tables (the work queue, the unit-event log, and the
results mart, which the dead-worker reconstruction reads). **Nothing from process memory.** The same
defect was introduced twice before this rule existed: v1 fed the gate ``summary.timed_out``, v2 fed
it ``sink.write_failures`` — both in-memory objects inside a *drain worker*, read by a gate that runs
in a **different task, in a different process**. That is why ``write_failures`` rides to the gate as
a *column on the fail-loud* ``slice_completed`` *event* and not as a task value.

WHAT THIS GATE DOES **NOT** OWN (read this before adding a rule)
----------------------------------------------------------------
**Per-unit ROW COMPLETENESS is NOT the gate's job — it belongs to the drain** (ADR-067's
bronze-anchored completeness invariant inside ``_process_tracking_match``, which *raises* and turns
the unit into a ``failed`` terminal). An earlier revision of this module carried a "rule 3" that
compared a ``succeeded`` unit's ``rows_written`` against a fresh read of the results mart, and sold
it as an independent "did the rows land" cross-check. **It was tautological.**
``_process_tracking_match`` calls ``write_delta_table(...)`` with **no ``row_count``**, so per
ADR-045 the number it returns — and therefore ``rows_written`` — is *itself* a POST-WRITE count of
the ``replaceWhere`` mart slice. The gate then re-read the same mart with the same predicate and
compared the two: the same quantity on both sides. It could not fail except on a mart mutation
between the write and the gate, and it structurally could NOT detect the ``skillcorner:1552423:2``
incident its own docstring named (that surfaces as a ``failed`` terminal, which the rule skipped).
It is deleted. Do not re-add it.

The gate's INDEPENDENT teeth are: rules 0/1/2 (did every expected unit run, and did it reach a
terminal — the event log vs the queue) and the V7 planner alarm (the planner re-run vs the event
log — genuinely independent sources). Plus rule 3, the one exception below.

THE FOUR VERDICTS
-----------------
+---+------------------------------------------------------------------+----------------+--------+
| # | condition                                                        | verdict        | raises |
+===+==================================================================+================+========+
| 0 | an EXPECTED WORKER (incl. the sb360 sentinel) has no             | DRAIN_FAILED   | no     |
|   | ``slice_completed``                                               |                |        |
| 1 | ``anomalies`` is NON-EMPTY *and* EVERY anomaly sits inside a      | UNVERIFIABLE   | no     |
|   | LOSSY worker (``write_failures > 0``)                             |                |        |
| 2 | a CLEAN worker has an EXPECTED unit with NO terminal              | INCOMPLETE     | yes    |
| 3 | a CLEAN sb360's ``succeeded`` unit wrote ZERO rows                | INCOMPLETE     | yes    |
| 4 | —                                                                | COMPLETE       | no     |
+---+------------------------------------------------------------------+----------------+--------+

``anomalies`` = [expected units with no terminal] + [sb360 ``succeeded`` units with
``rows_written == 0``], **partitioned by the worker that owns the unit**. ``timed_out`` is excused
everywhere.

RULE 3 IS sb360-ONLY, AND THAT IS THE WHOLE POINT
--------------------------------------------------
sb360 is the ONE producer with **no per-unit completeness invariant**: ``_sb360_terminals`` stamps
EVERY discovered match ``succeeded`` with a row count read back from the mart, so a match that
silently produced nothing compares ``0 == 0`` and reads as healthy — and ``PlannerInputs.remaining``
deliberately excludes statsbomb, so the V7 alarm is blind to it too. It would be re-enumerated
forever under a green verdict: exactly the class this gate exists for. sb360 discovery REQUIRES the
match to be present in ``bronze.statsbomb_360`` (ADR-057, frames-required), so **zero rows is
anomalous, not legitimate**.

It must NOT be generalised to drain units: a drain ``(match, period)`` with zero SPADL actions in
that period (e.g. every action NULL-``time_seconds``) legitimately writes 0 rows. Those are
*reported* (``succeeded_with_zero_rows``), never accused.

THE FOUR THINGS THAT WENT WRONG BEFORE, AND WHERE THEY ARE PINNED
------------------------------------------------------------------
* **P1 — a dead worker REPORTS, it does not RAISE.** The gate runs under ``run_if = ALL_DONE``, so
  it is reached even when the drain failed. An OOM-killed worker leaves ``running`` events with no
  terminal; treating that as INCOMPLETE would fail an already-failed job with the gate's own,
  *wrong* exception and mask the drain's real one. The job already failed — the gate's job here is
  to **say what died**.
* **V1 — taint is PER-WORKER, never per-run.** A run-scoped ``UNVERIFIABLE`` means one lost event on
  worker 1 SUPPRESSES a genuine silent-skip accusation about worker 5. With ~390 fail-open one-row
  commits, losses are plausible; if lossy runs are common, a run-scoped ``UNVERIFIABLE`` becomes the
  common verdict — and a gate that never accuses is a muted gate.
* **W1 — rule 1 requires a NON-EMPTY anomaly set.** ``all([])`` is ``True``: without the clause, a
  run where every unit is terminal, every count matches, and one worker merely lost a ``running``
  event returns ``UNVERIFIABLE`` *vacuously*. Since SOME loss is the EXPECTED case, the gate would
  be muted by its own success. No anomalies + losses → **COMPLETE, with the loss count as a
  warning** (a lost *terminal* would itself have produced an anomaly, so "no anomalies despite
  losses" means only ``running`` events were lost — that costs OOM-visibility, not correctness).
* **V6 — the ``DRAIN_FAILED`` report must not lie.** Terminals are BATCHED, so an OOM-killed worker
  flushes NONE of them: its units — *including the many that succeeded and wrote rows* — all look
  like ``running`` with no terminal. The licence to batch was that terminal state is
  **reconstructible from results**, so the report reconstructs it: ``completed_terminal_lost`` vs
  ``in_flight`` vs ``never_started``.

THE sb360 SEAM (a defect landed here in EVERY review round: P2 → V2 → W3 → X1/X2 → Y1/Y2)
------------------------------------------------------------------------------------------
sb360 **exits the per-match drain** (ADR-058): it has no queue rows, no ``worker_id``, its own
terraform task and its own lifecycle. So *every rule written while looking at the drain silently
fails to apply to it*. Two halves, both mandatory:

* **expected WORKERS** = ``DISTINCT worker_id`` from the queue **UNION the sb360 sentinel** — because
  if the sb360 task dies *before* emitting its ``running`` events it contributes ZERO expected units,
  no rule fires, and the gate returns COMPLETE while statsbomb did nothing at all. Its task is
  unconditional in the DAG, so it is **always** expected.
* **expected UNITS** = queue rows **UNION sb360's ``running`` events** (its persisted
  queue-equivalent).

``SB360_WORKER_ID`` is **imported** from ``analytics.action_context.drain`` — the one home BOTH the
producer (``ingestion``) and this consumer can read. **Never write the literal.**
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum

from analytics.action_context.drain import SB360_WORKER_ID

__all__ = [
    "DrainGateError",
    "GateReport",
    "PlannerInputs",
    "QueueRow",
    "UnitEvent",
    "UnitKey",
    "Verdict",
    "enforce",
    "evaluate",
    "expected_units",
]

FAILED = "failed"
RUNNING = "running"
SLICE_COMPLETED = "slice_completed"
SUCCEEDED = "succeeded"
TIMED_OUT = "timed_out"

#: A unit is settled once one of these lands. ``timed_out`` counts as a terminal (the unit rolls
#: forward BY DESIGN — a capacity signal, not a correctness one; ADR-067's ``raise_on_failed_units``
#: excludes it too) and ``failed`` counts as one (D2 already failed the worker's task).
TERMINAL_STATES = frozenset({SUCCEEDED, FAILED, TIMED_OUT})


class Verdict(str, Enum):
    """The gate's four answers. Only ``INCOMPLETE`` fails the task (plus a planner alarm)."""

    COMPLETE = "COMPLETE"
    DRAIN_FAILED = "DRAIN_FAILED"  # P1 — report, do NOT raise: the job already failed
    INCOMPLETE = "INCOMPLETE"  # raise
    UNVERIFIABLE = "UNVERIFIABLE"  # report, do NOT raise: the evidence is known-lossy


class DrainGateError(RuntimeError):
    """The drain did not finish its work (or its writes did not land). Raised by ``enforce``."""


@dataclass(frozen=True)
class UnitKey:
    """The identity of one unit of work. ``period`` is ``None`` for sb360 (it is MATCH grain).

    Deliberately NOT ``order=True``: a generated ``__lt__`` compares the fields as a tuple, so a
    MATCH-grain unit (``period=None``) and a PERIOD-grain unit of the same match would compare
    ``None < 1`` and blow up with a ``TypeError`` inside the gate — i.e. the gate would crash
    instead of reporting, on exactly the day it is needed. Sort through ``_sort_key``.
    """

    provider: str
    match_id: str
    period: int | None

    @property
    def label(self) -> str:
        base = f"{self.provider}:{self.match_id}"
        return base if self.period is None else f"{base}:{self.period}"


def _sort_key(key: UnitKey) -> tuple[str, str, bool, int]:
    """Total order over ``UnitKey`` that survives a NULL ``period`` (match-grain units sort first)."""
    return (key.provider, key.match_id, key.period is not None, key.period or 0)


@dataclass(frozen=True)
class QueueRow:
    """One row of ``observability.action_context_work_queue`` for the run under audit."""

    worker_id: int
    provider: str
    match_id: str
    period: int | None

    @property
    def key(self) -> UnitKey:
        return UnitKey(self.provider, self.match_id, self.period)


@dataclass(frozen=True)
class UnitEvent:
    """One row of the ``observability.action_context_unit_events`` VIEW for the run under audit.

    ``write_failures`` is populated on ``slice_completed`` rows ONLY: it is the sole channel by
    which the fail-open sink's telemetry losses reach a gate that reads persisted tables only.
    """

    worker_id: int
    provider: str
    match_id: str
    period: int | None
    state: str
    rows_written: int | None = None
    error: str | None = None
    write_failures: int | None = None

    @property
    def key(self) -> UnitKey:
        return UnitKey(self.provider, self.match_id, self.period)


@dataclass(frozen=True)
class PlannerInputs:
    """The M2/V7 diagnostic's inputs — the planner RE-RUN at gate time.

    ``enqueued`` is the queue-row count for this run. ``remaining`` is what the planner *still*
    enumerates now that the drain has (supposedly) written its rows.

    **sb360 is deliberately NOT in ``remaining``.** Its discovery is a different function and it is
    never enqueued, so mixing it in would make ``enqueued == 0 and remaining > 0`` fire on the
    perfectly ordinary state "the drain is caught up and new statsbomb matches arrived" — a false
    RAISE on a healthy run.
    """

    enqueued: int
    remaining: frozenset[UnitKey]


@dataclass(frozen=True)
class GateReport:
    """The verdict, plus everything an operator needs to act on it without opening a notebook."""

    verdict: Verdict
    run_id: str
    expected_workers: tuple[int, ...]
    dead_workers: tuple[int, ...]
    lossy_workers: tuple[int, ...]
    write_failures_total: int
    #: Anomalies on CLEAN workers — the trustworthy accusations (rules 2 + 3).
    missing_terminals: tuple[str, ...]
    empty_sb360_matches: tuple[str, ...]
    #: Anomalies on LOSSY workers — reported, never accused (their evidence is known-lossy).
    untrusted_anomalies: tuple[str, ...]
    #: Clean-worker anomalies, rendered — surfaced in a DRAIN_FAILED report, which pre-empts rules 2-3.
    clean_worker_anomalies: tuple[str, ...]
    #: ``failed`` terminals, WITH their error text. NOT a verdict input (ADR-067's
    #: ``raise_on_failed_units`` already fails the worker's task) — but the gate is sold as
    #: defence-in-depth for exactly this class, so a run with a demonstrably failed unit must at
    #: minimum NAME it. Before this, ``UnitEvent.error`` was fetched to the driver and discarded.
    failed_units: tuple[str, ...]
    #: DRAIN units that succeeded having written ZERO rows. Suspicious, not accusable: a period whose
    #: SPADL actions all carry NULL ``time_seconds`` legitimately writes nothing (which is also why
    #: the V7 alarm skips them — a 0-row unit stays in the planner's ``remaining`` set forever, and
    #: alarming on it is a FALSE ACCUSATION that RAISES). sb360's 0-row matches are NOT here: they
    #: are a genuine anomaly (rule 3).
    succeeded_with_zero_rows: tuple[str, ...]
    #: V6 — a dead worker's started units, reconstructed from what LANDED rather than from its
    #: (unflushed, hence absent) terminals.
    completed_terminal_lost: tuple[str, ...]
    in_flight: tuple[str, ...]
    never_started: tuple[str, ...]
    planner_alarms: tuple[str, ...]
    planner_remaining: int

    @property
    def must_raise(self) -> bool:
        """``INCOMPLETE`` fails the task; so does a planner alarm.

        A planner alarm raises on its own authority even when the *verdict* reports, because it is
        derived from the RESULTS MART and the planner — not from the fail-open, loss-prone event
        log. "We said it succeeded and the planner still sees it as unwritten" is evidence that
        survives a lost event and a dead peer worker alike, and suppressing it because some *other*
        worker died is V1's disease one layer up.
        """
        return self.verdict is Verdict.INCOMPLETE or bool(self.planner_alarms)

    def render(self) -> str:
        """A single human-readable block — this is what lands in the task log and in the exception.

        Everything the report COLLECTS, it RENDERS. ``expected_workers`` and the ``failed`` units'
        error text were both gathered and then thrown away: an operator reading a ``DRAIN_FAILED``
        log could not see which workers were even expected, and the failure text of every failed
        unit was fetched to the driver and dropped on the floor.
        """
        lines = [f"D8 action-context drain gate: {self.verdict.value} (run_id={self.run_id})"]
        lines.append(f"  expected workers (queue + sb360 sentinel): {list(self.expected_workers)}")
        if self.dead_workers:
            lines.append(
                f"  DEAD workers (no slice_completed): {list(self.dead_workers)} "
                "-- the job already failed; this is a REPORT, not a new accusation"
            )
            lines += _section("completed before death (rows landed, terminal lost)", self.completed_terminal_lost)
            lines += _section("genuinely IN FLIGHT when the worker died (no rows)", self.in_flight)
            lines += _section("never started", self.never_started)
            lines += _section("ALSO: anomalies on workers that DID finish", self.clean_worker_anomalies)
        if self.lossy_workers:
            lines.append(
                f"  LOSSY workers (unit events lost): {list(self.lossy_workers)} "
                f"-- {self.write_failures_total} event row(s) lost in total"
            )
            lines += _section("anomalies on lossy workers (UNTRUSTED -- not accused)", self.untrusted_anomalies)
        lines += _section("units with NO terminal event (the SILENT-SKIP class)", self.missing_terminals)
        lines += _section("sb360 matches that 'succeeded' having written ZERO rows", self.empty_sb360_matches)
        # NOT a verdict input: ADR-067's `raise_on_failed_units` already fails the worker's task, so
        # raising here as well would only mask the drain's real exception with the gate's. But the
        # gate is sold as defence-in-depth for the failed-unit class, and a report that never NAMES
        # a failed unit provides none.
        lines += _section("units that FAILED (the worker's own task already failed -- ADR-067)", self.failed_units)
        lines += _section(
            "units that succeeded with ZERO rows (suspicious, not accused)", self.succeeded_with_zero_rows
        )
        lines += _section("PLANNER DIAGNOSTIC", self.planner_alarms)
        lines.append(f"  planner still sees {self.planner_remaining} unit(s) remaining")
        return "\n".join(lines)


def _section(title: str, items: Sequence[str]) -> list[str]:
    if not items:
        return []
    return [f"  {title}:", *(f"    - {item}" for item in items)]


def expected_units(
    *,
    queue: Iterable[QueueRow],
    events: Iterable[UnitEvent],
) -> dict[UnitKey, int]:
    """The units this run OWES, mapped to the worker that owns each — the UNION of two producers.

    The drain's contract is the **queue** (a ``running`` event for a unit nobody enqueued is not an
    obligation). sb360's contract is its **``running`` events**: it is NEVER enqueued (ADR-058), so
    a queue-only expectation set says NOTHING about statsbomb — and a queue-only gate therefore left
    it completely unchecked (P2).

    Shared by the entry point, which scopes its results-mart read to exactly this set: two
    definitions of "expected" would let a unit be judged against a count that was never fetched.
    """
    owners: dict[UnitKey, int] = {row.key: row.worker_id for row in queue}
    for event in events:
        if event.worker_id == SB360_WORKER_ID and event.state == RUNNING:
            owners.setdefault(event.key, SB360_WORKER_ID)
    return owners


def _persisted_rows(key: UnitKey, per_period: Mapping[tuple[str, str, int | None], int]) -> int:
    """Rows the RESULTS MART actually holds for ``key``. Used ONLY by the V6 dead-worker
    reconstruction (rule 3 keys off the EVENT's ``rows_written``, not the mart).

    A ``period=None`` unit is sb360's MATCH grain, and the mart is per-period — so its periods are
    rolled up. Without this, a dead sb360 task's completed matches all read as "in flight".
    """
    if key.period is not None:
        return per_period.get((key.provider, key.match_id, key.period), 0)
    match = (key.provider, key.match_id)
    return sum(n for (provider, match_id, _period), n in per_period.items() if (provider, match_id) == match)


def evaluate(
    *,
    run_id: str,
    queue: Sequence[QueueRow],
    events: Sequence[UnitEvent],
    result_counts: Mapping[tuple[str, str, int | None], int],
    planner: PlannerInputs,
) -> GateReport:
    """Apply rules 0 → 4 (see the module docstring) and return the verdict. NEVER raises.

    The raise lives in ``enforce`` — so that every verdict, including the raising one, is reachable
    and assertable without a ``pytest.raises`` wrapper, and so that the entry point owns the exit
    code. ``events`` and ``queue`` MUST already be filtered to ``run_id`` by the caller (units are
    re-enqueued across runs; a latest-across-all-runs read would misattribute a prior terminal to a
    fresh unit — spec §3).
    """
    owners = expected_units(queue=queue, events=events)

    # Expected WORKERS: derived from the queue -- NEVER a hard-coded fan-out width. Terraform's own
    # comment says "daily runs are tiny", so on a typical run most workers are idle and have no queue
    # rows at all; a gate that expects a fixed number of workers cries wolf on every healthy day (P4).
    # Plus the sb360 sentinel, whose task is UNCONDITIONAL in the DAG (V2).
    expected_workers = {row.worker_id for row in queue} | {SB360_WORKER_ID}

    slices = {e.worker_id: (e.write_failures or 0) for e in events if e.state == SLICE_COMPLETED}
    dead = sorted(w for w in expected_workers if w not in slices)
    lossy = sorted(w for w, failures in slices.items() if failures > 0)
    lossy_set = set(lossy)
    write_failures_total = sum(slices.values())

    started = {e.key for e in events if e.state == RUNNING}
    terminals: dict[UnitKey, UnitEvent] = {e.key: e for e in events if e.state in TERMINAL_STATES}

    # ── the anomaly set, partitioned by the worker that OWNS the unit (V1) ──
    missing: list[tuple[int, UnitKey]] = []
    empty_sb360: list[tuple[int, UnitKey, str]] = []
    zero_row_drain: list[str] = []
    for key, worker_id in sorted(owners.items(), key=lambda item: _sort_key(item[0])):
        terminal = terminals.get(key)
        if terminal is None:
            missing.append((worker_id, key))
            continue
        # `timed_out` is excused (the watchdog abandons live threads and the unit rolls forward by
        # design); `failed` already failed its worker's task (ADR-067) and is only REPORTED.
        if terminal.state != SUCCEEDED or terminal.rows_written != 0:
            continue
        if worker_id == SB360_WORKER_ID:
            # RULE 3 -- sb360 ONLY. It is the one producer with no per-unit completeness invariant
            # (`_sb360_terminals` stamps every discovered match `succeeded` from a mart read-back, so
            # 0 rows compares 0 == 0 and reads healthy) AND it is excluded from `planner.remaining`,
            # so the V7 alarm cannot see it either. Discovery REQUIRES the match in
            # `bronze.statsbomb_360` (ADR-057), so zero rows means it silently produced nothing.
            empty_sb360.append((worker_id, key, f"{key.label} — sb360 'succeeded' having written 0 rows"))
        else:
            # A DRAIN unit CAN legitimately write 0 rows (every SPADL action in the period carrying
            # a NULL `time_seconds` -> the completeness invariant returns early). Report, never accuse.
            zero_row_drain.append(key.label)

    dead_set = set(dead)
    clean_missing = tuple(k.label for w, k in missing if w not in dead_set and w not in lossy_set)
    clean_empty_sb360 = tuple(msg for w, _k, msg in empty_sb360 if w not in dead_set and w not in lossy_set)
    untrusted = tuple(
        [f"{k.label} — no terminal event" for w, k in missing if w in lossy_set]
        + [msg for w, _k, msg in empty_sb360 if w in lossy_set]
    )
    clean_anomalies = tuple([f"{label} — no terminal event" for label in clean_missing] + list(clean_empty_sb360))

    # Every `failed` terminal, WITH the error text the event log carried (previously read and
    # discarded). The verdict deliberately does not key on this -- ADR-067 already fails the task.
    failed_units = tuple(
        f"{k.label} — {t.error or '<no error text recorded>'}"
        for k, t in sorted(terminals.items(), key=lambda item: _sort_key(item[0]))
        if t.state == FAILED
    )

    # ── V6: reconstruct a DEAD worker's outcome from what LANDED ──
    # Terminals are BATCHED, so a worker killed mid-slice flushed NONE of them: its units --
    # INCLUDING the ones that succeeded and wrote rows -- all look like `running` with no terminal.
    # The licence to batch was that terminal state is reconstructible from results. So reconstruct it,
    # or the ALL_DONE payoff ships a report that LIES about what completed.
    dead_units = sorted((k for k, w in owners.items() if w in dead_set), key=_sort_key)
    completed_terminal_lost = tuple(
        k.label for k in dead_units if k in started and _persisted_rows(k, result_counts) > 0
    )
    in_flight = tuple(k.label for k in dead_units if k in started and _persisted_rows(k, result_counts) <= 0)
    never_started = tuple(k.label for k in dead_units if k not in started)

    # ── the planner diagnostic (M2 + V7) ──
    alarms: list[str] = []
    if planner.enqueued == 0 and planner.remaining:
        alarms.append(
            f"planner COLLAPSE — the queue is empty for this run yet the planner still enumerates "
            f"{len(planner.remaining)} unit(s). 'The planner stopped seeing work' is not a backlog."
        )
    # `rows_written == 0` is EXCLUDED: a unit whose SPADL actions all carry a NULL `time_seconds`
    # legitimately writes 0 rows, never lands in the mart, and so stays in the planner's `remaining`
    # set FOREVER -- `succeeded ∩ remaining` would then RAISE on a perfectly healthy run. This alarm
    # is the FALSE-ACCUSATION class and it RAISES, so it must key on units that CLAIMED rows and
    # still look unwritten. The 0-row units are reported instead (`succeeded_with_zero_rows`).
    succeeded_keys = {k for k, e in terminals.items() if e.state == SUCCEEDED and e.rows_written != 0}
    alarms += [
        f"{k.label} — event says 'succeeded' but the planner still sees it as unwritten"
        for k in sorted(succeeded_keys & planner.remaining, key=_sort_key)
    ]

    anomaly_workers = [w for w, _k in missing] + [w for w, _k, _msg in empty_sb360]
    # W1: the NON-EMPTY clause is load-bearing. `all([])` is True, so without `anomaly_workers` a run
    # with zero anomalies and one lost `running` event would VACUOUSLY satisfy "every anomaly sits
    # inside a lossy worker" and return UNVERIFIABLE. Since SOME loss is the EXPECTED case at ~390
    # fail-open commits, the gate would return a non-verdict most days -- muted by its own success.
    all_anomalies_lossy = bool(anomaly_workers) and all(w in lossy_set for w in anomaly_workers)

    if dead:  # rule 0
        verdict = Verdict.DRAIN_FAILED
    elif all_anomalies_lossy:  # rule 1 — W1: `anomalies_exist` is the NON-EMPTY clause. all([]) is True.
        verdict = Verdict.UNVERIFIABLE
    elif clean_missing or clean_empty_sb360:  # rules 2 + 3
        verdict = Verdict.INCOMPLETE
    else:  # rule 4
        verdict = Verdict.COMPLETE

    return GateReport(
        verdict=verdict,
        run_id=run_id,
        expected_workers=tuple(sorted(expected_workers)),
        dead_workers=tuple(dead),
        lossy_workers=tuple(lossy),
        write_failures_total=write_failures_total,
        missing_terminals=clean_missing,
        empty_sb360_matches=clean_empty_sb360,
        untrusted_anomalies=untrusted,
        clean_worker_anomalies=clean_anomalies,
        failed_units=failed_units,
        succeeded_with_zero_rows=tuple(zero_row_drain),
        completed_terminal_lost=completed_terminal_lost,
        in_flight=in_flight,
        never_started=never_started,
        planner_alarms=tuple(alarms),
        planner_remaining=len(planner.remaining),
    )


def enforce(report: GateReport) -> None:
    """Raise ``DrainGateError`` iff the report says the task must fail. Pure — no I/O.

    Kept OUT of ``evaluate`` so the verdict is a value (assertable offline, loggable in full) and
    the decision to fail the task is one explicit call at the entry point.
    """
    if report.must_raise:
        raise DrainGateError(report.render())
