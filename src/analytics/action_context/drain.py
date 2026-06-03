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

WATCHDOG_BUDGET_S = 1800
MAX_ABANDONED_THREADS = 3

_TIER_COST_S: dict[str, float] = {"tracking": 1800.0, "statsbomb": 120.0, "event_only": 60.0}


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
    the expensive tier, event-only the cheap tier, statsbomb between (sb360 subset).
    Upgrade path: a historical-median cost_fn (the param is the injection seam).
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


def unit_label(unit: WorkUnit) -> str:
    base = f"{unit.provider}:{unit.match_id}"
    return f"{base}:{unit.period}" if unit.period is not None else base


def drain_worker(
    queue: WorkQueuePort,
    processor: GameProcessorPort,
    watchdog: WatchdogPort,
    run_id: str,
    worker_id: int,
    logger: logging.Logger,
    *,
    budget_s: int = WATCHDOG_BUDGET_S,
    max_abandoned: int = MAX_ABANDONED_THREADS,
    units: list[WorkUnit] | None = None,
) -> DrainSummary:
    """Drain one worker's queue slice; per-unit isolation; bounded abandonment.

    ``units`` may be pre-fetched by the caller (e.g. the entry point's empty-slice
    short-circuit) to avoid re-reading the queue; otherwise it is fetched here.
    """
    summary = DrainSummary(worker_id=worker_id)
    if units is None:
        units = queue.units_for_worker(run_id, worker_id)
    logger.info("ac1_drain_start run_id=%s worker_id=%d units=%d", run_id, worker_id, len(units))
    for unit in units:
        label = unit_label(unit)
        try:
            rows = watchdog.run(lambda u=unit: processor.process(u), label, budget_s)
        except GameTimeoutError:
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
                raise RuntimeError(
                    f"drain worker {worker_id} exceeded concurrent abandoned-thread ceiling "
                    f"({watchdog.live_abandoned_count} > {max_abandoned}); slice rolls to next run"
                ) from None
            continue
        except Exception as exc:
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
        summary.processed += 1
        summary.total_rows += rows
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
