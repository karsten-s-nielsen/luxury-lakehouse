"""Worker-drain fan-out: pure orchestration core (no Spark / Delta / dbutils).

See docs/superpowers/specs/2026-06-02-action-context-worker-drain-fanout-design.md
and ADR-037. Adapters live in ``ingestion.action_context_queue``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from analytics.action_context.work_unit import WorkUnit, provider_tier

# Per-game (now per-half — all tracking providers enqueue per-period units, ADR-037 amendment) watchdog
# budget. 1800 -> 2700 (2026-06-03): headroom for slower exact ghost-GK backends and per-experiment slack;
# overridable per run via the drain worker's --watchdog-budget-s. _TIER_COST_S (below) is the SEPARATE LPT
# load-balancing estimate (rank order only) and is intentionally left unchanged.
WATCHDOG_BUDGET_S = 2700
MAX_ABANDONED_THREADS = 3

# D9 — flush the buffered terminals every N units, not once at the very end of the slice.
#
# A single end-of-slice flush means ANY exception out of ``drain_worker`` -- including its OWN
# deliberate abandon-ceiling ``raise`` -- destroys EVERY terminal for that worker plus its
# ``write_failures`` count (which rides on ``slice_completed``, also never emitted). The worker then
# reads to the gate as DEAD, and V6 reconstructs its units from the mart: a planned, well-understood
# raise made its own evidence unreadable.
#
# N = 10: each worker owns its OWN Delta table (the Task-2 spike's per-worker topology), so a flush
# costs no cross-worker ``_delta_log`` contention -- the only cost is commits. At the measured ~47
# units/worker that is ~5 commits per worker instead of 1, against ~47 ``running`` commits the
# worker already pays. Cheap; and it bounds the loss from an OOM-killed driver to <= 9 terminals
# (which V6 can reconstruct) instead of the whole slice.
TERMINAL_FLUSH_EVERY = 10

# sb360 EXITS the per-match drain (ADR-058): it is one distributed cogroup job with its own task,
# so it has NO queue rows and NO worker_id — yet the drain-completeness gate (D8) must treat it as
# an EXPECTED WORKER (a dead sb360 task must not yield COMPLETE). Hence a sentinel.
#
# ITS HOME IS LOAD-BEARING: the pure gate lives in ``analytics`` and ``analytics`` CANNOT import
# ``ingestion`` (.importlinter ``analytics-isolation``), while ``ingestion`` already imports from
# this module. So the constant must live HERE for BOTH the producer (ingestion) and the consumer
# (the gate) to import it. Never write the literal ``-1`` on either side.
#
# -1 (never NULL): ``_EVENT_COLUMNS`` marks ``worker_id`` NOT NULL, and 0..N-1 are real workers.
SB360_WORKER_ID = -1

_TIER_COST_S: dict[str, float] = {"tracking": 1800.0, "statsbomb": 120.0}


class GameTimeoutError(RuntimeError):
    """Raised by a WatchdogPort when a unit exceeds its per-game budget."""


@dataclass(frozen=True)
class WorkAssignment:
    """One queue row: a unit bound to a worker with a drain order + cost estimate."""

    unit: WorkUnit
    worker_id: int
    seq: int
    est_cost: float


@dataclass
class DrainSummary:
    """Outcome of one worker draining its slice."""

    worker_id: int
    processed: int = 0
    failed: int = 0
    timed_out: int = 0
    total_rows: int = 0
    failed_units: list[str] = field(default_factory=list)
    timed_out_units: list[str] = field(default_factory=list)


def tier_cost_fn(unit: WorkUnit) -> float:
    """Rough per-unit cost estimate (seconds) for LPT load-balancing.

    Rank order is what matters, not accuracy: IDSSE halves + tracking matches are
    the expensive tier; statsbomb (sb360) is the cheaper tier. Event-only providers
    are out of action-context scope (frames-required; ADR-057). Upgrade path: a
    historical-median cost_fn (the param is the injection seam).
    """
    return _TIER_COST_S[provider_tier(unit)]


def _stable_key(unit: WorkUnit) -> tuple[str, str, int, tuple[int, int]]:
    """Tiebreak key independent of discovery (Spark anti-join) row order."""
    fr = unit.frame_range if unit.frame_range is not None else (-1, -1)
    return (unit.provider, unit.match_id, unit.period if unit.period is not None else -1, fr)


def assign_workers(
    units: Sequence[WorkUnit],
    n_workers: int,
    cost_fn: Callable[[WorkUnit], float] = tier_cost_fn,
) -> list[WorkAssignment]:
    """Greedy Longest-Processing-Time bin-packing across ``n_workers``.

    Heaviest unit first; each unit to the currently-least-loaded worker. A stable
    tiebreak makes the result independent of input order (so two preflights over the
    same SET produce identical worker_id + seq). Total: every unit assigned once.
    """
    if n_workers < 1:
        raise ValueError(f"n_workers must be >= 1, got {n_workers}")
    ordered = sorted(units, key=lambda u: (-cost_fn(u), _stable_key(u)))
    loads = [0.0] * n_workers
    buckets: list[list[WorkUnit]] = [[] for _ in range(n_workers)]
    for unit in ordered:
        w = min(range(n_workers), key=lambda i: (loads[i], i))
        buckets[w].append(unit)
        loads[w] += cost_fn(unit)
    assignments: list[WorkAssignment] = []
    for w in range(n_workers):
        for seq, unit in enumerate(buckets[w]):
            assignments.append(WorkAssignment(unit=unit, worker_id=w, seq=seq, est_cost=cost_fn(unit)))
    return assignments


class WorkQueuePort(Protocol):
    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]: ...


class GameProcessorPort(Protocol):
    def process(self, unit: WorkUnit) -> int: ...


class WatchdogPort(Protocol):
    def run(self, fn: Callable[[], int], label: str, timeout_s: float) -> int:
        """Run ``fn`` under ``timeout_s``.

        Returns ``fn()`` on success; raises ``GameTimeoutError`` on budget expiry;
        otherwise RE-RAISES ``fn``'s exception (type + traceback) on the caller's
        thread. ``live_abandoned_count`` is the number of abandoned non-interruptible
        threads STILL ALIVE (concurrent memory-pressure bound, P3).
        """
        ...

    @property
    def live_abandoned_count(self) -> int:
        """Count of abandoned (non-interruptible) threads still alive RIGHT NOW --
        a CONCURRENT memory-pressure bound, not a lifetime count (P3)."""
        ...


class UnitEventSink(Protocol):
    """Persists per-unit lifecycle events (D9). Adapter: ``ingestion.action_context_queue``.

    FOUR methods, THREE write policies — and the differences are LOAD-BEARING. Do not "simplify"
    them into one write; each policy is the reason a specific failure stays visible:

    * ``unit_started`` — per-unit, **fail-open**, counted. Written BEFORE processing: it IS the
      OOM-visibility guarantee (an OOM-killed driver's in-flight units stay distinguishable from
      units that never began). Reconstructible from nothing, so it cannot be batched.
    * ``unit_finished`` — buffered; **fail-open**, counted. Terminal state IS reconstructible
      (rows exist in results; a failed unit fails the task), so it may be batched.
    * ``flush_terminals`` — writes the buffer. **FAIL-OPEN** and counted: terminals are telemetry,
      and ADR-002 says telemetry loss must never become data loss. (Were this fail-loud, the
      ``UNVERIFIABLE`` verdict — whose entire purpose is *lost unit events* — could never be
      reached, because a worker that lost events would have died instead.)
    * ``slice_completed`` — **FAIL-LOUD**, and it carries ``write_failures``. It is the ONLY channel
      by which the loss count reaches the gate, which runs in a DIFFERENT TASK and reads persisted
      tables only. If it cannot land, the gate's evidence is unusable → the worker task must fail.

    ``write_failures`` is the running count of unit-event ROWS lost to the fail-open writes above.
    """

    def unit_started(self, run_id: str, worker_id: int, unit: WorkUnit) -> None: ...

    def unit_finished(
        self,
        run_id: str,
        worker_id: int,
        unit: WorkUnit,
        *,
        state: str,
        rows_written: int | None,
        error: str | None,
    ) -> None: ...

    def flush_terminals(self) -> None: ...

    def slice_completed(self, run_id: str, worker_id: int) -> None: ...

    @property
    def write_failures(self) -> int: ...


def unit_label(unit: WorkUnit) -> str:
    base = f"{unit.provider}:{unit.match_id}"
    return f"{base}:{unit.period}" if unit.period is not None else base


def _emit_fail_open(
    emit: Callable[[], None],
    logger: logging.Logger,
    run_id: str,
    worker_id: int,
    what: str,
) -> None:
    """Run a FAIL-OPEN sink write: telemetry loss must never become data loss (ADR-002).

    ``DeltaUnitEventSink`` already swallows-and-counts its own write failures, so in production
    nothing normally reaches this catch. It is defence-in-depth for the port CONTRACT: a sink whose
    own guard fails (a driver OOM inside ``createDataFrame``, a bad adapter) must not be able to
    destroy a 5.5 h drain. ERROR-level, never warning (ADR-002: warnings are invisible in error-log
    queries -- that is what hid the 2026-04-12 blocker for 62 h).
    """
    try:
        emit()
    except Exception as exc:
        logger.error(
            "ac1_unit_event_emit_failed op=%s run_id=%s worker_id=%d err=%s",
            what,
            run_id,
            worker_id,
            exc,
            exc_info=True,
        )


def drain_worker(
    queue: WorkQueuePort,
    processor: GameProcessorPort,
    watchdog: WatchdogPort,
    run_id: str,
    worker_id: int,
    logger: logging.Logger,
    *,
    sink: UnitEventSink,
    budget_s: int = WATCHDOG_BUDGET_S,
    max_abandoned: int = MAX_ABANDONED_THREADS,
    units: list[WorkUnit] | None = None,
    flush_every: int = TERMINAL_FLUSH_EVERY,
) -> DrainSummary:
    """Drain one worker's queue slice; per-unit isolation; bounded abandonment.

    ``units`` may be pre-fetched by the caller (e.g. the entry point's empty-slice
    short-circuit) to avoid re-reading the queue; otherwise it is fetched here.

    ``sink`` is a MANDATORY injection (no default -- same shape as the guard injection in
    ``run_pipeline()``): a drain that silently forgets to persist its unit events is exactly the
    invisibility D9 exists to kill, and a default would let a caller acquire it by omission.
    """
    summary = DrainSummary(worker_id=worker_id)
    if units is None:
        units = queue.units_for_worker(run_id, worker_id)
    logger.info("ac1_drain_start run_id=%s worker_id=%d units=%d", run_id, worker_id, len(units))

    def _flush() -> None:
        _emit_fail_open(sink.flush_terminals, logger, run_id, worker_id, "flush_terminals")

    for index, unit in enumerate(units, start=1):
        label = unit_label(unit)
        # PERIODIC FLUSH (checked at the TOP of the iteration, so it is reached whichever branch the
        # PREVIOUS unit took -- both the timeout and the failure branch `continue`). A single
        # end-of-slice flush loses every terminal for the worker if anything escapes this function.
        if index > 1 and (index - 1) % flush_every == 0:
            _flush()
        # BEFORE processing -- the OOM-visibility guarantee (D9). An OOM-killed driver flushes no
        # buffer, so a unit whose `running` was not ALREADY persisted is indistinguishable from a
        # unit that never began. This is why it can never be batched or moved below the call.
        _emit_fail_open(
            lambda u=unit: sink.unit_started(run_id, worker_id, u), logger, run_id, worker_id, "unit_started"
        )
        try:
            rows = watchdog.run(lambda u=unit: processor.process(u), label, budget_s)
        except GameTimeoutError:
            _emit_fail_open(
                lambda u=unit: sink.unit_finished(
                    run_id, worker_id, u, state="timed_out", rows_written=None, error=None
                ),
                logger,
                run_id,
                worker_id,
                "unit_finished",
            )
            summary.timed_out += 1
            summary.timed_out_units.append(label)
            logger.error("ac1_drain_unit_timeout run_id=%s worker_id=%d unit=%s", run_id, worker_id, label)
            if watchdog.live_abandoned_count > max_abandoned:  # CONCURRENT, not cumulative (P3)
                logger.error(
                    "ac1_drain_abandon_ceiling run_id=%s worker_id=%d live_abandoned=%d ceiling=%d -- failing fast",
                    run_id,
                    worker_id,
                    watchdog.live_abandoned_count,
                    max_abandoned,
                )
                # FLUSH BEFORE THE RAISE. This raise is DELIBERATE (the slice rolls to the next run),
                # but without the flush it also destroys its own evidence: every buffered terminal --
                # including the many units that SUCCEEDED and wrote rows -- plus the slice_completed
                # that carries write_failures. The worker would then read to the gate as DEAD, and V6
                # would have to reconstruct from the mart what the worker already knew. A planned
                # raise must not be indistinguishable from an OOM.
                _flush()
                raise RuntimeError(
                    f"drain worker {worker_id} exceeded concurrent abandoned-thread ceiling "
                    f"({watchdog.live_abandoned_count} > {max_abandoned}); slice rolls to next run"
                ) from None
            continue
        except Exception as exc:
            # The per-unit swallow is DELIBERATE and STAYS (one bad unit must not destroy a 5.5 h
            # drain); ``raise_on_failed_units`` (ADR-067 D2) still fails the TASK at the end.
            _emit_fail_open(
                lambda u=unit, e=exc: sink.unit_finished(
                    run_id, worker_id, u, state="failed", rows_written=None, error=str(e)
                ),
                logger,
                run_id,
                worker_id,
                "unit_finished",
            )
            summary.failed += 1
            summary.failed_units.append(label)
            logger.error(
                "ac1_drain_unit_failed run_id=%s worker_id=%d unit=%s err=%s",
                run_id,
                worker_id,
                label,
                exc,
                exc_info=True,
            )
            continue
        _emit_fail_open(
            lambda u=unit, r=rows: sink.unit_finished(
                run_id, worker_id, u, state="succeeded", rows_written=r, error=None
            ),
            logger,
            run_id,
            worker_id,
            "unit_finished",
        )
        summary.processed += 1
        summary.total_rows += rows
    # FAIL-OPEN (M1): were this loud, the gate's UNVERIFIABLE verdict -- whose entire purpose is
    # *lost unit events* -- could never be reached, because a lossy worker would have DIED instead
    # of reporting its loss (and read as a dead worker, not a lossy one).
    _flush()
    # FAIL-LOUD (M1, opposite policy -- hence a SEPARATE write): this is the ONLY channel by which
    # ``write_failures`` reaches the gate, which runs in a DIFFERENT TASK and reads persisted tables
    # only. If it cannot land, the gate's evidence is unusable, so the worker must fail rather than
    # let the gate reason on a half-truth. Deliberately NOT wrapped.
    sink.slice_completed(run_id, worker_id)
    logger.info(
        "ac1_drain_end run_id=%s worker_id=%d processed=%d failed=%d timed_out=%d rows=%d",
        run_id,
        worker_id,
        summary.processed,
        summary.failed,
        summary.timed_out,
        summary.total_rows,
    )
    return summary
