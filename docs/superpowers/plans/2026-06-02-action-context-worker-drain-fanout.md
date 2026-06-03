# AC-1 Worker-Drain Fan-Out Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Commit policy (project hard rule):** every `git commit` requires explicit user approval at the moment + the `~/.claude-git-approval` sentinel. The "Commit" steps below are the intended rhythm, NOT authorization — batch them and ask. Default is a single squash-merged PR.

**Goal:** Replace the 48 KB-bounded chunk-list fan-out for AC-1 action-context with a durable work-queue that a fixed set of persistent for-each workers drain to completion, removing any static cap on game count and collapsing ~633 cold-starts to 8.

**Architecture:** Preflight discovers unprocessed *units*, LPT-bin-packs them across `_N_DRAIN_WORKERS` by estimated cost into `{catalog}.observability.action_context_work_queue`, and emits a tiny constant worker-id task value + the run_id. A for-each over the worker ids runs one persistent driver per worker; each drains its queue slice, processing one unit at a time under a per-game watchdog. Pure orchestration core in `analytics`, Spark/Delta/dbutils adapters in `ingestion`.

**Tech Stack:** Python 3.10, PySpark (Spark Connect serverless), Delta Lake, Databricks Jobs for-each + task values, pytest, Terraform.

**Spec:** `docs/superpowers/specs/2026-06-02-action-context-worker-drain-fanout-design.md` (approved + twice-reviewed; §13 has the review-resolution tables).

---

## File Structure

**Create:**
- `src/analytics/action_context/drain.py` — pure core: ports, `WorkAssignment`, `DrainSummary`, `GameTimeoutError`, `tier_cost_fn`, `assign_workers`, `drain_worker`. No Spark/Delta/dbutils imports.
- `src/ingestion/action_context_queue.py` — adapters: `DeltaWorkQueue`, `SparkInterruptWatchdog`, `SparkGameProcessor`.
- `src/tests/action_context/test_drain.py` — pure-core unit tests.
- `src/tests/action_context/test_action_context_queue.py` — adapter integration tests (serverless-marked) + the thread/box re-raise unit test (stub spark).
- `scripts/migrations/2026-06-02-create-action-context-work-queue.sql` — idempotent DDL.
- `docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md`.

**Modify:**
- `src/ingestion/action_context.py` — add `_N_DRAIN_WORKERS`, `_ActionContextGuard.discover_units`, simplify `check`, remove `chunk_sizes` + the chunk-building loop, rework `main_preflight`, add `main_drain_worker`. (`main` / `--match-ids` kept.)
- `src/tests/test_action_context_enrichment.py` — delete `test_guard_chunk_sizes_keep_task_value_under_limit`; add the size-invariance + all-units-retained tests; the affected max_units/preflight tests now assert against `discover_units`/assignments.
- `pyproject.toml` — add `compute_action_context_drain_worker` entry point.
- `terraform/modules/workflows/main.tf` — rewire the `compute_action_context` for-each + the `preflight_action_context` run_id param.
- `src/tests/test_workflow_terraform_parity.py` (or the existing TF-parity test file) — add the `concurrency == _N_DRAIN_WORKERS` assertion.
- `CLAUDE.md`, `docs/c4/architecture.dsl` — governance.
- version via `scripts/bump_wheel.py`.

---

## Task 1: Pure core — types, ports, cost model

**Files:**
- Create: `src/analytics/action_context/drain.py`
- Test: `src/tests/action_context/test_drain.py`

- [ ] **Step 1: Write the failing test (types + cost model)**

```python
# src/tests/action_context/test_drain.py
from __future__ import annotations

from analytics.action_context.drain import (
    WorkAssignment,
    GameTimeoutError,
    tier_cost_fn,
)
from analytics.action_context.work_unit import WorkUnit


def test_tier_cost_fn_rank_order() -> None:
    tracking = WorkUnit(provider="metrica", match_id="m1")
    idsse_half = WorkUnit(provider="idsse", match_id="i1", period=1)
    event = WorkUnit(provider="wyscout", match_id="w1")
    sb = WorkUnit(provider="statsbomb", match_id="s1")
    # idsse-half and tracking are the expensive tier; event-only the cheap tier.
    assert tier_cost_fn(idsse_half) == tier_cost_fn(tracking)
    assert tier_cost_fn(tracking) > tier_cost_fn(sb) > tier_cost_fn(event)


def test_work_assignment_is_frozen() -> None:
    a = WorkAssignment(unit=WorkUnit(provider="metrica", match_id="m1"), worker_id=0, seq=0, est_cost=1800.0)
    assert a.worker_id == 0 and a.seq == 0
    assert issubclass(GameTimeoutError, RuntimeError)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/action_context/test_drain.py -q`
Expected: FAIL — `ModuleNotFoundError: analytics.action_context.drain`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/analytics/action_context/drain.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/action_context/test_drain.py -q`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/analytics/action_context/drain.py src/tests/action_context/test_drain.py
git commit -m "feat(ac-1): drain core types + tier cost model"
```

---

## Task 2: `assign_workers` (LPT bin-packing, stable + deterministic)

**Files:**
- Modify: `src/analytics/action_context/drain.py`
- Test: `src/tests/action_context/test_drain.py`

- [ ] **Step 1: Write the failing tests**

```python
# append to src/tests/action_context/test_drain.py
import random

from analytics.action_context.drain import assign_workers


def _units(n: int, provider: str = "wyscout") -> list[WorkUnit]:
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
    units = _units(15, "wyscout") + [WorkUnit(provider="idsse", match_id="i", period=p) for p in (1, 2)]
    base = assign_workers(units, n_workers=4)
    shuffled = list(units)
    random.Random(7).shuffle(shuffled)
    other = assign_workers(shuffled, n_workers=4)
    key = lambda a: (a.unit.provider, a.unit.match_id, a.unit.period)
    assert {key(a): (a.worker_id, a.seq) for a in base} == {key(a): (a.worker_id, a.seq) for a in other}


def test_assign_workers_more_workers_than_units() -> None:
    assignments = assign_workers(_units(3), n_workers=8)
    assert len(assignments) == 3


def test_assign_workers_empty() -> None:
    assert assign_workers([], n_workers=8) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/action_context/test_drain.py -k assign -q`
Expected: FAIL — `ImportError: cannot import name 'assign_workers'`.

- [ ] **Step 3: Write minimal implementation (append to `drain.py`)**

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/action_context/test_drain.py -k assign -q`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/analytics/action_context/drain.py src/tests/action_context/test_drain.py
git commit -m "feat(ac-1): LPT worker assignment (stable, deterministic)"
```

---

## Task 3: `drain_worker` use-case (ports, isolation, abandonment ceiling)

**Files:**
- Modify: `src/analytics/action_context/drain.py`
- Test: `src/tests/action_context/test_drain.py`

- [ ] **Step 1: Write the failing tests**

```python
# append to src/tests/action_context/test_drain.py
import logging

from analytics.action_context.drain import drain_worker, unit_label


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

    def run(self, fn, label, timeout_s):  # noqa: ANN001, ANN201
        return fn()


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

    def run(self, fn, label, timeout_s):  # noqa: ANN001, ANN201
        if label in self._timeout:
            self._abandoned += 1
            raise GameTimeoutError(label)
        return fn()


def test_drain_worker_processes_own_slice_only() -> None:
    u0 = [WorkUnit(provider="wyscout", match_id="a"), WorkUnit(provider="wyscout", match_id="b")]
    u1 = [WorkUnit(provider="wyscout", match_id="c")]
    q = _FakeQueue({("R", 0): u0, ("R", 1): u1})
    proc = _FakeProcessor()
    s = drain_worker(q, proc, _InlineWatchdog(), run_id="R", worker_id=0, logger=logging.getLogger("t"))
    assert proc.processed == ["a", "b"]
    assert s.processed == 2 and s.total_rows == 10 and s.failed == 0


def test_drain_worker_failure_continues_with_key(caplog) -> None:  # noqa: ANN001
    units = [WorkUnit(provider="wyscout", match_id="ok1"), WorkUnit(provider="wyscout", match_id="bad"),
             WorkUnit(provider="wyscout", match_id="ok2")]
    q = _FakeQueue({("R", 0): units})
    proc = _FakeProcessor(fail=frozenset({"bad"}))
    with caplog.at_level(logging.ERROR):
        s = drain_worker(q, proc, _InlineWatchdog(), run_id="R", worker_id=0, logger=logging.getLogger("t"))
    assert proc.processed == ["ok1", "ok2"]
    assert s.processed == 2 and s.failed == 1
    assert "wyscout:bad" in s.failed_units
    assert any("wyscout:bad" in r.message for r in caplog.records)


def test_drain_worker_timeout_continues() -> None:
    units = [WorkUnit(provider="metrica", match_id="x"), WorkUnit(provider="metrica", match_id="y")]
    q = _FakeQueue({("R", 0): units})
    proc = _FakeProcessor()
    wd = _TimeoutWatchdog(timeout_labels=frozenset({"metrica:x"}))
    s = drain_worker(q, proc, wd, run_id="R", worker_id=0, logger=logging.getLogger("t"))
    assert proc.processed == ["y"]
    assert s.timed_out == 1 and s.processed == 1 and "metrica:x" in s.timed_out_units


def test_drain_worker_abandonment_ceiling_fails_fast() -> None:
    units = [WorkUnit(provider="wyscout", match_id=f"h{i}") for i in range(10)]
    q = _FakeQueue({("R", 0): units})
    wd = _TimeoutWatchdog(timeout_labels=frozenset(unit_label(u) for u in units))
    import pytest

    with pytest.raises(RuntimeError, match="abandoned-thread ceiling"):
        drain_worker(q, _FakeProcessor(), wd, run_id="R", worker_id=0,
                     logger=logging.getLogger("t"), max_abandoned=3)


def test_unit_label_includes_period() -> None:
    assert unit_label(WorkUnit(provider="idsse", match_id="m", period=2)) == "idsse:m:2"
    assert unit_label(WorkUnit(provider="wyscout", match_id="w")) == "wyscout:w"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/action_context/test_drain.py -k "drain_worker or unit_label" -q`
Expected: FAIL — `ImportError: cannot import name 'drain_worker'`.

- [ ] **Step 3: Write minimal implementation (append to `drain.py`)**

```python
class WorkQueuePort(Protocol):
    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]: ...


class GameProcessorPort(Protocol):
    def process(self, unit: WorkUnit) -> int: ...


class WatchdogPort(Protocol):
    def run(self, fn: Callable[[], int], label: str, timeout_s: int) -> int:
        """Run ``fn`` under ``timeout_s``.

        Returns ``fn()`` on success; raises ``GameTimeoutError`` on budget expiry;
        otherwise RE-RAISES ``fn``'s exception (type + traceback) on the caller's
        thread. ``live_abandoned_count`` is the number of abandoned non-interruptible
        threads STILL ALIVE (concurrent memory-pressure bound, P3).
        """

    @property
    def live_abandoned_count(self) -> int:
        """Count of abandoned (non-interruptible) threads still alive RIGHT NOW —
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
) -> DrainSummary:
    """Drain one worker's queue slice; per-unit isolation; bounded abandonment."""
    summary = DrainSummary(worker_id=worker_id)
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
                    run_id, worker_id, watchdog.live_abandoned_count, max_abandoned,
                )
                raise RuntimeError(
                    f"drain worker {worker_id} exceeded concurrent abandoned-thread ceiling "
                    f"({watchdog.live_abandoned_count} > {max_abandoned}); slice rolls to next run"
                ) from None
            continue
        except Exception as exc:  # noqa: BLE001 -- per-unit isolation; ERROR-logged with key, drain continues
            summary.failed += 1
            summary.failed_units.append(label)
            logger.error(
                "ac1_drain_unit_failed run_id=%s worker_id=%d unit=%s err=%s",
                run_id, worker_id, label, exc, exc_info=True,
            )
            continue
        summary.processed += 1
        summary.total_rows += rows
    logger.info(
        "ac1_drain_end run_id=%s worker_id=%d processed=%d failed=%d timed_out=%d rows=%d",
        run_id, worker_id, summary.processed, summary.failed, summary.timed_out, summary.total_rows,
    )
    return summary
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/action_context/test_drain.py -q`
Expected: PASS (all). Then `uv run ruff check src/analytics/action_context/drain.py` → clean.

- [ ] **Step 5: Commit**

```bash
git add src/analytics/action_context/drain.py src/tests/action_context/test_drain.py
git commit -m "feat(ac-1): drain_worker use-case with bounded abandonment"
```

---

## Task 4: `discover_units` on the guard (typed discovery)

**Files:**
- Modify: `src/ingestion/action_context.py` (add method to `_ActionContextGuard`, simplify `check`, add `_N_DRAIN_WORKERS`)
- Test: `src/tests/test_action_context_enrichment.py`

- [ ] **Step 1: Write the failing test**

```python
# add to src/tests/test_action_context_enrichment.py
def test_discover_units_wraps_providers(monkeypatch) -> None:  # noqa: ANN001
    from analytics.action_context.work_unit import WorkUnit
    from ingestion import action_context as ac

    calls = {"idsse": 0, "tracking": 0, "event": 0}

    def _idsse(*a, **k):  # noqa: ANN002, ANN003, ANN202
        calls["idsse"] += 1
        return [("idm", 1), ("idm", 2)]

    def _tracking(*a, **k):  # noqa: ANN002, ANN003, ANN202
        calls["tracking"] += 1
        return ["t1"]

    def _event(*a, **k):  # noqa: ANN002, ANN003, ANN202
        calls["event"] += 1
        return ["e1", "e2"]

    monkeypatch.setattr(ac, "_find_idsse_new_period_pairs", _idsse)
    monkeypatch.setattr(ac, "_find_tracking_new_ids", _tracking)
    monkeypatch.setattr(ac, "_find_event_only_new_ids", _event)
    import ingestion.guards as g  # discover_units does `from ingestion.guards import ensure_table`
    monkeypatch.setattr(g, "ensure_table", lambda *a, **k: None)

    guard = ac._ActionContextGuard()
    units = guard.discover_units(spark=None, catalog="c", schema="bronze")  # type: ignore[arg-type]
    assert WorkUnit(provider="idsse", match_id="idm", period=1) in units
    assert WorkUnit(provider="idsse", match_id="idm", period=2) in units
    assert sum(u.provider in {"metrica", "skillcorner", "gradientsports"} for u in units) == 3
    assert sum(u.provider in {"statsbomb", "wyscout"} for u in units) == 4

    # P1: check() + a second discover_units() must NOT re-run the anti-joins (memoised once).
    assert guard.check(spark=None, catalog="c", schema="bronze").count == len(units)  # type: ignore[arg-type]
    assert guard.discover_units(spark=None, catalog="c", schema="bronze") is units  # type: ignore[arg-type]
    assert calls == {"idsse": 1, "tracking": 3, "event": 2}  # discovery ran exactly once

    # R1: a DIFFERENT (catalog, schema) self-invalidates the memo -> re-discovers (no stale cache).
    guard.discover_units(spark=None, catalog="OTHER", schema="bronze")  # type: ignore[arg-type]
    assert calls == {"idsse": 2, "tracking": 6, "event": 4}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k discover_units -q`
Expected: FAIL — `AttributeError: '_ActionContextGuard' object has no attribute 'discover_units'`.

- [ ] **Step 3: Implement — add the constant + method, simplify `check`, remove `chunk_sizes`**

In `src/ingestion/action_context.py`, replace the `chunk_sizes` ClassVar + its big comment block (currently lines ~516–571) with:

```python
    workflow_id = "wf-action-context"
    # Number of persistent drain workers (the for-each width). Single source of
    # truth: the preflight builds this many worker-id task-value entries and the
    # Terraform for-each concurrency is pinned to it (test_terraform_concurrency_
    # matches_n_workers). See ADR-037. chunk_size is gone — the per-game watchdog
    # + persistent worker removed the per-iteration budget chunk_size packed against.
    _N_DRAIN_WORKERS = 8
```

In `_ActionContextGuard.__init__`, add the per-instance memo field (P1):

```python
        self._units_cache: list[WorkUnit] | None = None  # set by discover_units()
        self._units_cache_key: tuple[str, str] | None = None  # (catalog, schema) of the cache (R1)
```

(If the guard has no explicit `__init__` today, add one that takes `provider_filter=None, max_units=None`, stores them, and initialises both cache fields to `None`. Use a `TYPE_CHECKING`/local `WorkUnit` import for the annotation, or annotate as `list | None`.)

Then **replace** the body of `check` and **add** `discover_units` (keep the existing `_cap` / `_selected` helpers):

```python
    def discover_units(self, spark: SparkSession, catalog: str, schema: str) -> list[WorkUnit]:
        """Discover unprocessed action-context units across all 6 providers.

        A unit is a match (most providers) or an (match, period) half (IDSSE).
        Honors provider_filter + max_units (per-provider cap), same as check().

        **Memoised on (catalog, schema) (P1/R1):** the 6 anti-joins are expensive;
        ``check()`` (skip-guard count) and the preflight body (units) share ONE discovery.
        Keying on the target — not an unconditional flag — makes the cache safe BY
        CONSTRUCTION even on the long-lived module-level ``skip_guard`` singleton: a
        different (catalog, schema) self-invalidates, so it can never serve stale
        discovery across runs/targets.
        """
        if self._units_cache is not None and self._units_cache_key == (catalog, schema):
            return self._units_cache

        from analytics.action_context.work_unit import WorkUnit
        from ingestion.guards import ensure_table

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        spadl_table = f"{catalog}.bronze.spadl_actions"
        ensure_table(spark, results_table, _ACTION_CONTEXT_DDL)

        units: list[WorkUnit] = []
        if self._selected("idsse"):
            pairs = self._cap(
                _find_idsse_new_period_pairs(spark, f"{catalog}.bronze.idsse_tracking", spadl_table, results_table)
            )
            units += [WorkUnit(provider="idsse", match_id=mid, period=period) for mid, period in pairs]

        for prov, table in (
            ("metrica", "metrica_tracking"),
            ("skillcorner", "skillcorner_tracking"),
            ("gradientsports", "gradientsports_tracking"),
        ):
            if self._selected(prov):
                ids = self._cap(
                    _find_tracking_new_ids(spark, f"{catalog}.bronze.{table}", spadl_table, results_table, prov)
                )
                units += [WorkUnit(provider=prov, match_id=mid) for mid in ids]

        for prov in ("statsbomb", "wyscout"):
            if self._selected(prov):
                ids = self._cap(_find_event_only_new_ids(spark, spadl_table, results_table, prov))
                units += [WorkUnit(provider=prov, match_id=mid) for mid in ids]

        self._units_cache = units
        self._units_cache_key = (catalog, schema)
        return units

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Skip-guard hook: count of unprocessed units (0 => skip).

        Returns only the generic count; FilterResult.chunks (the shared fan-out
        field used by other guards) is intentionally NOT populated for AC-1 — the
        worker-drain fan-out reads structured units via discover_units() instead.
        """
        units = self.discover_units(spark, catalog, schema)
        return FilterResult(workflow_id=self.workflow_id, count=len(units))
```

Add the import near the top of the module if not present: `from analytics.action_context.work_unit import WorkUnit` is imported locally inside `discover_units` (keeps module import-light); leave it local.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k discover_units -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context.py src/tests/test_action_context_enrichment.py
git commit -m "feat(ac-1): typed discover_units + drop chunk_sizes from guard"
```

---

## Task 5: Replace the obsolete guard test + add size-invariance/all-units tests

**Files:**
- Modify: `src/tests/test_action_context_enrichment.py`

- [ ] **Step 1: Delete the obsolete test + write replacements**

Delete `test_guard_chunk_sizes_keep_task_value_under_limit` (it guarded the removed 48 KB chunk-list). Add:

```python
def test_worker_id_task_value_is_constant_size() -> None:
    """The for-each task value is O(_N_DRAIN_WORKERS), independent of game count."""
    from ingestion.action_context import _ActionContextGuard

    small = [str(i) for i in range(_ActionContextGuard._N_DRAIN_WORKERS)]
    # the emitted worker-id list does not grow with the number of discovered units
    assert len(small) == _ActionContextGuard._N_DRAIN_WORKERS
    assert all(s.isdigit() for s in small)


def test_assignment_retains_every_unit_at_scale() -> None:
    from analytics.action_context.drain import assign_workers
    from analytics.action_context.work_unit import WorkUnit

    units = [WorkUnit(provider="wyscout", match_id=f"w{i}") for i in range(100_000)]
    assignments = assign_workers(units, n_workers=8)
    assert len(assignments) == 100_000  # no truncation at any scale
    assert len({a.unit.match_id for a in assignments}) == 100_000
```

- [ ] **Step 2: Run**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k "task_value or retains_every_unit" -q`
Expected: PASS. And confirm the deleted test is gone: `grep -n chunk_sizes_keep src/tests/test_action_context_enrichment.py` → no output.

- [ ] **Step 3: Fix the other chunk-referencing tests + audit for same-instance reuse (R1)**

The max_units/preflight tests at ~lines 779/796 reference chunk strings + `guard.chunk_sizes`. Re-point them at `discover_units`/`assign_workers` (they assert "each provider contributes its units" and "all ids retained"). Update assertions to use `discover_units` output.

**R1 reuse audit:** grep this test file for any test that constructs ONE `_ActionContextGuard` and calls `check()` and/or `discover_units()` more than once **with different monkeypatched `_find_*` results for the same (catalog, schema)** — the memo would now serve the first (cached) result on the second call. For any such test, give it a fresh guard instance per discovery (or assert the cached behavior intentionally). The memo keys on `(catalog, schema)`, so reuse across *different* targets is already safe.

```bash
grep -n "_ActionContextGuard(" src/tests/test_action_context_enrichment.py  # inspect each for double-discovery reuse
```

Run the whole file:

Run: `uv run pytest src/tests/test_action_context_enrichment.py -q`
Expected: PASS (no references to `chunk_sizes` remain — `grep -n "chunk_sizes" src/tests/test_action_context_enrichment.py` empty).

- [ ] **Step 4: Commit**

```bash
git add src/tests/test_action_context_enrichment.py
git commit -m "test(ac-1): replace chunk-size guard with size-invariance + all-units tests"
```

---

## Task 6: `DeltaWorkQueue` adapter + integration tests

**Files:**
- Create: `src/ingestion/action_context_queue.py`
- Test: `src/tests/action_context/test_action_context_queue.py`

- [ ] **Step 1: Write the failing integration tests (serverless-marked)**

```python
# src/tests/action_context/test_action_context_queue.py
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_SERVERLESS_TESTS") != "1",
    reason="requires a Spark/Databricks serverless session (RUN_SERVERLESS_TESTS=1)",
)


def test_delta_work_queue_roundtrip_and_run_id_isolation(spark, tmp_catalog) -> None:  # noqa: ANN001
    from analytics.action_context.drain import assign_workers
    from analytics.action_context.work_unit import WorkUnit
    from ingestion.action_context_queue import DeltaWorkQueue

    q = DeltaWorkQueue(spark, catalog=tmp_catalog)
    q.ensure_table()

    units_a = [WorkUnit(provider="wyscout", match_id=f"a{i}") for i in range(5)]
    units_b = [WorkUnit(provider="idsse", match_id="bm", period=p) for p in (1, 2)]
    q.enqueue("RUN_A", assign_workers(units_a, n_workers=2))
    q.enqueue("RUN_B", assign_workers(units_b, n_workers=2))

    got_b = [u for w in (0, 1) for u in q.units_for_worker("RUN_B", w)]
    assert sorted((u.provider, u.match_id, u.period) for u in got_b) == [
        ("idsse", "bm", 1), ("idsse", "bm", 2),
    ]  # only RUN_B's rows; period preserved (run-id isolation, B1/L2)
```

- [ ] **Step 2: Run to verify it fails (or skips without serverless)**

Run: `RUN_SERVERLESS_TESTS=1 uv run pytest src/tests/action_context/test_action_context_queue.py -k roundtrip -q`
Expected: FAIL — `ModuleNotFoundError: ingestion.action_context_queue`. (Without the env var: SKIPPED.)

- [ ] **Step 3: Implement `DeltaWorkQueue`**

```python
# src/ingestion/action_context_queue.py
"""Spark/Delta/dbutils adapters for the AC-1 worker-drain fan-out (ADR-037).

Implements the pure ports from analytics.action_context.drain.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from pyspark.sql import SparkSession
from pyspark.sql import types as T  # noqa: N812

from analytics.action_context.drain import GameTimeoutError, WorkAssignment
from analytics.action_context.work_unit import WorkUnit

_QUEUE_TABLE = "action_context_work_queue"
_QUEUE_SCHEMA = "observability"

# SINGLE SOURCE OF TRUTH for the queue columns (P5). Excludes _ingested_at, which
# write_delta_table auto-adds. The CREATE TABLE DDL string and the migration are
# both derived from / parity-tested against this StructType.
_QUEUE_STRUCT = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("worker_id", T.IntegerType(), False),
    T.StructField("seq", T.LongType(), False),
    T.StructField("provider", T.StringType(), False),
    T.StructField("match_id", T.StringType(), False),
    T.StructField("period", T.IntegerType(), True),
    T.StructField("frame_range_lo", T.LongType(), True),
    T.StructField("frame_range_hi", T.LongType(), True),
    T.StructField("est_cost", T.DoubleType(), False),
])


def queue_columns_sql() -> str:
    """Column list for CREATE TABLE, derived from _QUEUE_STRUCT (P5).

    simpleString() maps IntegerType->'int', LongType->'bigint', StringType->'string',
    DoubleType->'double' (see project memory reference_spark_type_simplestring).
    Appends the auto-audit _ingested_at column.
    """
    cols = [f"{f.name} {f.dataType.simpleString()}" for f in _QUEUE_STRUCT.fields]
    cols.append("_ingested_at timestamp")
    return ", ".join(cols)


class DeltaWorkQueue:
    """Durable work-queue over ``{catalog}.observability.action_context_work_queue``."""

    def __init__(self, spark: SparkSession, catalog: str) -> None:
        self._spark = spark
        self._catalog = catalog  # P7: store directly, don't re-split the FQN
        self._table = f"{catalog}.{_QUEUE_SCHEMA}.{_QUEUE_TABLE}"

    def ensure_table(self) -> None:
        # P4: create the schema too, so a fresh catalog (test tmp_catalog) works;
        # idempotent + cheap. In dev/prod observability already exists (watermarks).
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._catalog}.{_QUEUE_SCHEMA}")
        self._spark.sql(f"CREATE TABLE IF NOT EXISTS {self._table} ({queue_columns_sql()}) USING DELTA")

    def enqueue(self, run_id: str, assignments: list[WorkAssignment]) -> None:
        from ingestion.utils import write_delta_table

        rows = []
        for a in assignments:
            lo, hi = (a.unit.frame_range or (None, None))
            rows.append((run_id, a.worker_id, a.seq, a.unit.provider, a.unit.match_id,
                         a.unit.period, lo, hi, a.est_cost))
        sdf = self._spark.createDataFrame(rows, schema=_QUEUE_STRUCT)
        write_delta_table(
            sdf,
            self._catalog,  # P7
            _QUEUE_SCHEMA,
            _QUEUE_TABLE,
            replace_where=f"run_id = '{run_id}'",
            row_count=len(rows),
        )

    def units_for_worker(self, run_id: str, worker_id: int) -> list[WorkUnit]:
        from pyspark.sql import functions as F  # noqa: N812

        df = (
            self._spark.table(self._table)
            .where((F.col("run_id") == run_id) & (F.col("worker_id") == worker_id))
            .orderBy("seq")
        )
        out: list[WorkUnit] = []
        for r in df.collect():
            fr = (r["frame_range_lo"], r["frame_range_hi"]) if r["frame_range_lo"] is not None else None
            out.append(WorkUnit(provider=r["provider"], match_id=r["match_id"], period=r["period"], frame_range=fr))
        return out
```

> Note: confirm `write_delta_table`'s positional signature (`df, catalog, schema, table_name, mode="overwrite", replace_where, logger, row_count`) against `src/ingestion/utils.py`; the `enqueue` call must match it. (Reviewer verified this signature; `_ingested_at` is auto-added, so `_QUEUE_STRUCT` correctly omits it.)

- [ ] **Step 4: Run**

Run: `RUN_SERVERLESS_TESTS=1 uv run pytest src/tests/action_context/test_action_context_queue.py -k roundtrip -q`
Expected: PASS on serverless. Offline: SKIPPED. Always: `uv run ruff check src/ingestion/action_context_queue.py` clean, `uv run pyright src/ingestion/action_context_queue.py` clean.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context_queue.py src/tests/action_context/test_action_context_queue.py
git commit -m "feat(ac-1): DeltaWorkQueue adapter (replaceWhere, run-id isolated)"
```

---

## Task 7: `SparkInterruptWatchdog` + `SparkGameProcessor` adapters

**Files:**
- Modify: `src/ingestion/action_context_queue.py`
- Test: `src/tests/action_context/test_action_context_queue.py`

- [ ] **Step 1: Write the failing tests (re-raise contract via stub spark; smoke test serverless)**

```python
# append to src/tests/action_context/test_action_context_queue.py
class _StubSpark:
    def addTag(self, tag): pass  # noqa: N802, ANN001, ANN201
    def interruptTag(self, tag): pass  # noqa: N802, ANN001, ANN201


def test_watchdog_run_returns_value_offline() -> None:  # not serverless-gated below
    pytestmark_local = None  # noqa: F841
    from ingestion.action_context_queue import SparkInterruptWatchdog

    wd = SparkInterruptWatchdog(_StubSpark())  # type: ignore[arg-type]
    assert wd.run(lambda: 7, "lbl", timeout_s=5) == 7


def test_watchdog_run_reraises_fn_exception_offline() -> None:
    from ingestion.action_context_queue import SparkInterruptWatchdog

    wd = SparkInterruptWatchdog(_StubSpark())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="boom"):
        wd.run(lambda: (_ for _ in ()).throw(ValueError("boom")), "lbl", timeout_s=5)


def test_watchdog_timeout_abandons_noninterruptible_offline() -> None:
    import time

    from analytics.action_context.drain import GameTimeoutError
    from ingestion.action_context_queue import SparkInterruptWatchdog

    # stub interruptTag is a no-op (simulates the non-interruptible / event-only path):
    # the fn keeps sleeping past the grace join -> thread abandoned, counted LIVE (P3).
    wd = SparkInterruptWatchdog(_StubSpark(), interrupt_grace_s=0.1)  # type: ignore[arg-type]
    with pytest.raises(GameTimeoutError):
        wd.run(lambda: time.sleep(10) or 1, "lbl", timeout_s=0.2)  # type: ignore[func-returns-value]
    assert wd.live_abandoned_count == 1  # still sleeping -> alive -> counted
```

These three are **not** serverless-gated — move them above the module-level `pytestmark` skip, or give them their own non-skipped module `src/tests/action_context/test_watchdog_threading.py`. (Recommended: separate file, no skip marker, since they need no Spark.)

```python
# append (serverless-marked, in the queue test module)
def test_spark_interrupt_watchdog_real_processor_smoke(spark, tmp_catalog) -> None:  # noqa: ANN001
    """Drive the REAL processor path: tag must propagate to the deep applyInPandas (N3)."""
    from analytics.action_context.drain import GameTimeoutError
    from analytics.action_context.work_unit import WorkUnit
    from ingestion.action_context_queue import SparkGameProcessor, SparkInterruptWatchdog

    import time

    proc = SparkGameProcessor(spark, catalog=tmp_catalog, schema="bronze")  # real processor
    wd = SparkInterruptWatchdog(spark)
    unit = WorkUnit(provider="metrica", match_id="<a real tracking match in tmp_catalog>")
    start = time.monotonic()
    with pytest.raises(GameTimeoutError):
        wd.run(lambda: proc.process(unit), "metrica:smoke", timeout_s=5)  # 5s << real ~minutes
    elapsed = time.monotonic() - start
    # R3 PRIMARY proof: the controller regained control near the budget, not after the job's
    # full ~minutes -> the watchdog actually returned control (robust across runtimes).
    assert elapsed < 5 + wd._grace + 5
    # CORROBORATING (not load-bearing): interruptTag's returned op-ids. A correct serverless
    # build returns the cancelled op-ids, but the return contract isn't guaranteed across
    # runtimes (interruptTag has no in-repo precedent), so DON'T fail the test on []—just record.
    if not wd._last_interrupted_ops:
        import logging
        logging.getLogger("t").warning("interruptTag returned no op-ids; relying on timing proof (R3)")
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/action_context/test_watchdog_threading.py -q`
Expected: FAIL — `cannot import name 'SparkInterruptWatchdog'`.

- [ ] **Step 3: Implement both adapters (append to `action_context_queue.py`)**

```python
class SparkInterruptWatchdog:
    """Per-game watchdog: runs fn on a worker thread; interruptTag on timeout.

    Thread-locality invariant (B2): addTag is thread-local to the ops issued on
    that thread, so it is called INSIDE the worker thread that runs fn; interruptTag
    is cross-thread by tag string. Do NOT refactor fn onto a shared pool without
    preserving addTag-on-the-fn-thread.

    Tracking (Spark job) -> interruptTag cancels -> thread returns (no leak).
    Event-only (driver pandas) -> not cancellable -> thread abandoned (bounded by
    drain_worker's _MAX_ABANDONED_THREADS); ``live_abandoned_count`` counts the alive ones.
    """

    def __init__(self, spark: SparkSession, interrupt_grace_s: float = 5.0) -> None:
        self._spark = spark
        self._grace = interrupt_grace_s  # post-interrupt join: interrupted threads return here
        self._abandoned: list[threading.Thread] = []  # P3: track refs, count LIVE ones
        self._last_interrupted_ops: list[str] = []  # P6: interruptTag's return (proof of cancel)

    @property
    def live_abandoned_count(self) -> int:
        # P3: prune threads that have since finished (freed memory) and count only
        # those still alive -> a CONCURRENT memory-pressure bound, not a lifetime tally.
        self._abandoned = [t for t in self._abandoned if t.is_alive()]
        return len(self._abandoned)

    def run(self, fn: Callable[[], int], label: str, timeout_s: int) -> int:
        tag = f"ac1-{label}"
        box: dict[str, object] = {}

        def _target() -> None:
            try:
                self._spark.addTag(tag)  # thread-local: must be on this thread (B2)
                box["value"] = fn()
            except Exception as exc:  # noqa: BLE001 -- captured to replay on controller thread (N2/P8)
                box["error"] = exc

        t = threading.Thread(target=_target, name=f"ac1-watchdog-{label}", daemon=True)
        t.start()
        t.join(timeout_s)
        if t.is_alive():
            try:
                # interruptTag returns the ids of operations it cancelled (P6: proof of cancel)
                self._last_interrupted_ops = list(self._spark.interruptTag(tag) or [])
            finally:
                t.join(self._grace)  # interruptible (tracking) threads return here; non-interruptible stay alive
            if t.is_alive():
                self._abandoned.append(t)  # event-only / driver hang; bounded by drain loop (P3)
            raise GameTimeoutError(label)
        if "error" in box:
            raise box["error"]  # type: ignore[misc]  # re-raise fn's exception (type+traceback, N2)
        return int(box["value"])  # type: ignore[arg-type]


class SparkGameProcessor:
    """Dispatches a WorkUnit to the existing _process_* functions (unchanged).

    Loads the xT grid ONCE per worker (at construction), not per unit.
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str) -> None:
        from ingestion.action_context import _load_xt_grid_from_delta

        self._spark = spark
        self._catalog = catalog
        self._schema = schema
        self._logger = logging.getLogger("action_context_drain")
        self._xt_grid, self._xt_l, self._xt_w = _load_xt_grid_from_delta(spark, catalog, schema, self._logger)

    def process(self, unit: WorkUnit) -> int:
        from ingestion.action_context import (
            _is_tracking_provider,
            _process_event_only_match,
            _process_statsbomb_match,
            _process_tracking_match,
        )

        if _is_tracking_provider(unit.provider):
            return _process_tracking_match(
                self._spark, self._catalog, self._schema, unit.provider, unit.match_id,
                unit.period, self._xt_grid, self._xt_l, self._xt_w, self._logger,
            )
        if unit.provider == "statsbomb":
            return _process_statsbomb_match(self._spark, self._catalog, self._schema, unit.match_id, self._logger)
        if unit.provider == "wyscout":
            return _process_event_only_match(self._spark, self._catalog, self._schema, "wyscout", unit.match_id, self._logger)
        raise ValueError(f"unknown provider: {unit.provider}")
```

> **N7 — confirmed, no change needed:** `_process_tracking_match` builds `hb = PhaseHeartbeat(...)` as a **local** (`action_context.py:1138`), so each unit/thread already gets its own heartbeat — an abandoned thread cannot stomp the next unit's phase telemetry. Just add a one-line code comment noting this invariant must hold (don't hoist `hb` to module/closure scope).

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/action_context/test_watchdog_threading.py -q` → PASS (3, offline).
Run (serverless): `RUN_SERVERLESS_TESTS=1 uv run pytest src/tests/action_context/test_action_context_queue.py -k smoke -q` → PASS (proves `interruptTag` cancels the real deep `applyInPandas`).
Lint/type: `uv run ruff check src/ingestion/action_context_queue.py` + `uv run pyright src/ingestion/action_context_queue.py` clean.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context_queue.py src/tests/action_context/test_watchdog_threading.py src/tests/action_context/test_action_context_queue.py
git commit -m "feat(ac-1): SparkInterruptWatchdog (thread+interruptTag) + SparkGameProcessor"
```

---

## Task 8: Rework `main_preflight` (discover → assign → enqueue → task values)

**Files:**
- Modify: `src/ingestion/action_context.py`
- Test: `src/tests/test_action_context_enrichment.py`

- [ ] **Step 1: Write the failing test**

```python
def test_main_preflight_writes_run_id_and_worker_ids(monkeypatch) -> None:  # noqa: ANN001
    from ingestion import action_context as ac

    captured: dict[str, object] = {}

    class _FakeQueue:
        def __init__(self, *a, **k): pass
        def ensure_table(self): pass
        def enqueue(self, run_id, assignments): captured["run_id"] = run_id; captured["n"] = len(assignments)

    set_values: dict[str, object] = {}
    monkeypatch.setattr(ac, "DeltaWorkQueue", _FakeQueue)
    monkeypatch.setattr(ac, "_resolve_run_id", lambda args: "JOBRUN42")
    monkeypatch.setattr(ac, "_set_task_value", lambda k, v, log: set_values.__setitem__(k, v))
    monkeypatch.setattr(
        ac._ActionContextGuard, "discover_units",
        lambda self, spark, catalog, schema: [
            __import__("analytics.action_context.work_unit", fromlist=["WorkUnit"]).WorkUnit(provider="wyscout", match_id=f"w{i}")
            for i in range(20)
        ],
    )
    # stub spark/bootstrap/timed_check internals as the existing preflight tests do
    ac._run_preflight_for_test(catalog="c", schema="bronze")  # see helper note below

    assert captured["run_id"] == "JOBRUN42"
    assert captured["n"] == 20
    assert set_values["action_context_run_id"] == "JOBRUN42"
    assert set_values["action_context_worker_ids"] == [str(i) for i in range(ac._ActionContextGuard._N_DRAIN_WORKERS)]
```

> The exact stubbing mirrors the existing preflight tests in this file (spark session, `bootstrap_hooks`, `parse_ingestion_args`). Reuse their fixtures/monkeypatches; the assertions above are the contract.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k main_preflight_writes -q`
Expected: FAIL (helpers/symbols not defined).

- [ ] **Step 3: Implement — add module-level imports + helpers + rework `main_preflight`**

**P2 — add these at module top of `action_context.py`** (NOT function-local), so tests can
`monkeypatch.setattr(ac, "DeltaWorkQueue", …)` and the worker entry (Task 9) is patchable too.
The reviewer confirmed no import cycle: `action_context_queue` imports only pure `analytics.*` +
`ingestion.utils` at module level (its `_process_*` / `_load_xt_grid` imports are function-local).

```python
from analytics.action_context.drain import assign_workers, drain_worker
from ingestion.action_context_queue import DeltaWorkQueue, SparkGameProcessor, SparkInterruptWatchdog
```

Add module-level helpers near `_write_action_chunks_task_value` (which you can now delete or repurpose):

```python
def _resolve_run_id(args: argparse.Namespace) -> str:
    """The job-level run id, passed as --run-id (from {{job.run_id}}). Falls back
    to a timestamp only for standalone/manual invocation."""
    raw = getattr(args, "run_id", None)
    if raw and str(raw).strip():
        return str(raw).strip()
    import time
    return f"local-{int(time.time())}"


def _set_task_value(key: str, value: object, task_logger: logging.Logger) -> None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(get_spark_session())
        dbutils.jobs.taskValues.set(key=key, value=value)
        task_logger.info("Wrote task value %r", key)
    except (ImportError, AttributeError, RuntimeError) as exc:
        task_logger.warning("Task values not available (standalone) -- %s", exc)
```

Add `--run-id` to `main_preflight`'s `parse_ingestion_args` extra_args, then replace the chunk-building tail of `main_preflight` with:

```python
    guard = _ActionContextGuard(provider_filter=provider_filter, max_units=max_units)
    fr = timed_check(guard, spark, args.catalog, args.schema)  # telemetry (count + skip)
    if fr.count == 0:
        task_logger.info("Action context preflight: nothing to do")
        _set_task_value("action_context_run_id", "", task_logger)
        _set_task_value("action_context_worker_ids", [], task_logger)
        return

    units = guard.discover_units(spark, args.catalog, args.schema)  # memoised; check() already ran it
    assignments = assign_workers(units, _ActionContextGuard._N_DRAIN_WORKERS)
    run_id = _resolve_run_id(args)
    queue = DeltaWorkQueue(spark, args.catalog)
    queue.ensure_table()
    queue.enqueue(run_id, assignments)

    worker_ids = [str(i) for i in range(_ActionContextGuard._N_DRAIN_WORKERS)]
    _set_task_value("action_context_run_id", run_id, task_logger)
    _set_task_value("action_context_worker_ids", worker_ids, task_logger)
    task_logger.info(
        "Action context preflight: %d units across %d workers (run_id=%s)",
        len(units), len(worker_ids), run_id,
    )
```

Ensure `import argparse` is present at module top (it likely is via parse helpers; add if pyright complains).

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k "preflight" -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context.py src/tests/test_action_context_enrichment.py
git commit -m "feat(ac-1): preflight builds work-queue + worker-id/run-id task values"
```

---

## Task 9: Worker entry point `main_drain_worker` + pyproject

**Files:**
- Modify: `src/ingestion/action_context.py`, `pyproject.toml`
- Test: `src/tests/test_action_context_enrichment.py`

- [ ] **Step 1: Write the failing test**

```python
def test_main_drain_worker_calls_drain(monkeypatch) -> None:  # noqa: ANN001
    from ingestion import action_context as ac

    seen: dict[str, object] = {}

    def _fake_drain(queue, processor, watchdog, run_id, worker_id, logger, **kw):  # noqa: ANN001, ANN202
        seen["run_id"] = run_id
        seen["worker_id"] = worker_id
        from analytics.action_context.drain import DrainSummary
        return DrainSummary(worker_id=worker_id, processed=3, total_rows=9)

    monkeypatch.setattr(ac, "drain_worker", _fake_drain)
    monkeypatch.setattr(ac, "DeltaWorkQueue", lambda *a, **k: object())
    monkeypatch.setattr(ac, "SparkInterruptWatchdog", lambda *a, **k: object())
    monkeypatch.setattr(ac, "SparkGameProcessor", lambda *a, **k: object())
    # parse_ingestion_args / get_spark_session / configure_logging are module-level on ac.
    monkeypatch.setattr(ac, "parse_ingestion_args",
                        lambda *a, **k: _ns(worker_id="2", run_id="JOBRUN42", catalog="c", schema="bronze"))
    monkeypatch.setattr(ac, "get_spark_session", lambda: object())
    # bootstrap_hooks is imported function-local -> patch it at its SOURCE module, not on ac.
    import ingestion.bootstrap as bs
    monkeypatch.setattr(bs, "bootstrap_hooks", lambda *a, **k: None)

    ac.main_drain_worker()
    assert seen == {"run_id": "JOBRUN42", "worker_id": 2}
```

> `_ns(...)` builds an `argparse.Namespace`; reuse the file's existing namespace helper if present.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k drain_worker_calls -q`
Expected: FAIL — `main_drain_worker` undefined.

- [ ] **Step 3: Implement `main_drain_worker`**

```python
def main_drain_worker() -> None:
    """for-each worker: drain this worker's slice of the action-context queue.

    Receives --worker-id "{{input}}" and --run-id from the preflight task value
    (NOT from env -- see ADR-037 B1). The per-game watchdog (1800 s) bounds each
    unit; the task timeout (8 h) bounds the whole drain.

    DeltaWorkQueue / SparkGameProcessor / SparkInterruptWatchdog / drain_worker are
    imported at MODULE level (Task 8, P2) so tests can monkeypatch them on ``ac``.
    """
    args = parse_ingestion_args(
        "Drain a worker's action-context queue slice",
        extra_args=[
            ("--worker-id", {"type": str, "default": None, "help": "for-each worker index"}),
            ("--run-id", {"type": str, "default": None, "help": "preflight run id (task value)"}),
        ],
    )
    task_logger = configure_logging("action_context_drain")
    spark = get_spark_session()
    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    raw_wid = getattr(args, "worker_id", None)
    run_id = getattr(args, "run_id", None)
    if raw_wid is None or not str(raw_wid).strip():
        raise SystemExit("--worker-id is required")
    if not run_id or not str(run_id).strip():
        task_logger.info("Empty run_id (preflight found nothing) -- drain worker exits cleanly")
        return
    worker_id = int(str(raw_wid).strip())
    run_id = str(run_id).strip()

    queue = DeltaWorkQueue(spark, args.catalog)
    processor = SparkGameProcessor(spark, args.catalog, args.schema)
    watchdog = SparkInterruptWatchdog(spark)
    summary = drain_worker(queue, processor, watchdog, run_id, worker_id, task_logger)
    task_logger.info(
        "Drain worker %d complete: processed=%d failed=%d timed_out=%d rows=%d",
        worker_id, summary.processed, summary.failed, summary.timed_out, summary.total_rows,
    )
```

For the test, monkeypatch `ac.parse_ingestion_args` (module-level) to return a namespace, and patch `ingestion.bootstrap.bootstrap_hooks` at its source — matching the file's existing preflight-test approach.

Add to `pyproject.toml` `[project.scripts]` (next to `compute_action_context`):

```toml
compute_action_context_drain_worker = "ingestion.action_context:main_drain_worker"
```

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/test_action_context_enrichment.py -k drain_worker_calls -q` → PASS.
Run: `uv run pip install -e . -q || uv sync -q` then `python -c "import importlib.metadata as m; print('compute_action_context_drain_worker' in [e.name for e in m.entry_points(group='console_scripts')])"` → `True`.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/action_context.py pyproject.toml src/tests/test_action_context_enrichment.py
git commit -m "feat(ac-1): main_drain_worker entry point"
```

---

## Task 10: Work-queue migration

**Files:**
- Create: `scripts/migrations/2026-06-02-create-action-context-work-queue.sql`

- [ ] **Step 1: Inspect an existing migration for the exact placeholder/runner convention**

Run: `cat scripts/migrations/2026-05-29-add-ghost-gk-to-action-context.sql` — note how `{catalog}`/schema is templated and that ops are idempotent.

- [ ] **Step 2: Write the migration (mirror that convention)**

```sql
-- Create the AC-1 worker-drain queue (ADR-037). Run-scoped orchestration scratch
-- in observability (NOT bronze). Idempotent: CREATE TABLE IF NOT EXISTS.
CREATE TABLE IF NOT EXISTS {catalog}.observability.action_context_work_queue (
  run_id          STRING,
  worker_id       INT,
  seq             BIGINT,
  provider        STRING,
  match_id        STRING,
  period          INT,
  frame_range_lo  BIGINT,
  frame_range_hi  BIGINT,
  est_cost        DOUBLE,
  _ingested_at    TIMESTAMP
) USING DELTA;
```

> Match the exact `{catalog}` token the runner substitutes (verify in `scripts/migrations/_runner.py`). If the runner requires a fully-qualified literal instead, follow the existing files' convention.

- [ ] **Step 3: Verify idempotence locally if a runner dry-run exists**

Run: `grep -n "diff-filter=A\|_runner" .github/workflows/dbt-live-ci.yml` to confirm it'll be auto-applied; no destructive ops present (only `CREATE TABLE IF NOT EXISTS`).

- [ ] **Step 4: Add the schema-parity test (P5 — kill the 3-way drift)**

The columns now live in two derived places (the migration SQL + `_QUEUE_STRUCT`, with
`queue_columns_sql()` deriving the adapter's CREATE from the struct). Pin them together so a
future column add can't drift — mirror the existing cost-hook DDL-parity test.

```python
# src/tests/action_context/test_work_queue_schema_parity.py
from __future__ import annotations

import re
from pathlib import Path

from ingestion.action_context_queue import _QUEUE_STRUCT

_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "scripts" / "migrations" / "2026-06-02-create-action-context-work-queue.sql"
)


def _ddl_columns(sql: str) -> list[tuple[str, str]]:
    body = sql[sql.index("(") + 1 : sql.rindex(")")]
    cols: list[tuple[str, str]] = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        name, sql_type = line.split(None, 1)
        cols.append((name, sql_type.lower()))
    return cols


def test_migration_ddl_matches_queue_struct() -> None:
    ddl = _ddl_columns(_MIGRATION.read_text(encoding="utf-8"))
    struct = [(f.name, f.dataType.simpleString()) for f in _QUEUE_STRUCT.fields]
    struct.append(("_ingested_at", "timestamp"))  # auto-added by write_delta_table
    assert ddl == struct, (
        "migration DDL drifted from _QUEUE_STRUCT. Use simpleString spellings in the .sql: "
        "int (not integer), bigint (not long), double (not 'double precision'), string, timestamp; "
        f"\nDDL={ddl}\nSTRUCT={struct}"
    )
```

Run: `uv run pytest src/tests/action_context/test_work_queue_schema_parity.py -q`
Expected: PASS (offline — no Spark needed). If it fails, the migration DDL and `_QUEUE_STRUCT`
have drifted — fix whichever is wrong.

- [ ] **Step 5: Commit**

```bash
git add scripts/migrations/2026-06-02-create-action-context-work-queue.sql src/tests/action_context/test_work_queue_schema_parity.py
git commit -m "feat(ac-1): work-queue migration (observability) + schema-parity guard"
```

---

## Task 11: Terraform rewiring + concurrency-parity test

**Files:**
- Modify: `terraform/modules/workflows/main.tf` (lines ~154–187 for-each; ~1046–1064 preflight)
- Test: the existing TF-parity test file (e.g. `src/tests/test_workflow_terraform_parity.py`; if none, create `src/tests/test_action_context_terraform.py`)

- [ ] **Step 1: Write the failing parity test**

```python
# src/tests/test_action_context_terraform.py
from __future__ import annotations

import re
from pathlib import Path

from ingestion.action_context import _ActionContextGuard

_TF = Path(__file__).resolve().parents[2] / "terraform" / "modules" / "workflows" / "main.tf"


def test_terraform_concurrency_matches_n_workers() -> None:
    text = _TF.read_text(encoding="utf-8")
    # the compute_action_context for_each block's concurrency must equal _N_DRAIN_WORKERS
    block = text[text.index('task_key = "compute_action_context"') :][:1200]
    m = re.search(r"concurrency\s*=\s*(\d+)", block)
    assert m is not None, "no concurrency in compute_action_context for_each"
    assert int(m.group(1)) == _ActionContextGuard._N_DRAIN_WORKERS


def test_terraform_drain_worker_entry_point_and_params() -> None:
    text = _TF.read_text(encoding="utf-8")
    assert "compute_action_context_drain_worker" in text
    assert "action_context_worker_ids" in text
    assert "action_context_run_id" in text
    assert "timeout_seconds = 28800" in text
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest src/tests/test_action_context_terraform.py -q`
Expected: FAIL (old TF still references `action_context_chunks`, `compute_action_context`/1800).

- [ ] **Step 3: Edit the Terraform**

In the `compute_action_context` for-each task (current lines 161–186), replace with:

```hcl
    for_each_task {
      inputs      = "{{tasks.preflight_action_context.values.action_context_worker_ids}}"
      concurrency = 8  # == _N_DRAIN_WORKERS (pinned by test_terraform_concurrency_matches_n_workers)

      task {
        task_key        = "compute_action_context_iteration"
        timeout_seconds = 28800  # 8 h: a worker drains its slice to completion (per-game 1800s
                                 # watchdog is inside the worker, ADR-037). One-time cold start
                                 # ~5.5 h on the slowest worker; daily runs tiny.
        max_retries = 0

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "compute_action_context_drain_worker"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--worker-id", "{{input}}",
            "--run-id", "{{tasks.preflight_action_context.values.action_context_run_id}}",
          ]
        }

        environment_key = "analytics"
      }
    }
```

In the `preflight_action_context` task (~1046–1064), add the run-id job parameter to its `parameters` list:

```hcl
          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--run-id", "{{job.run_id}}",
            # ... keep existing --provider / --max-units job-parameter wiring ...
          ]
```

> Keep the existing `--provider`/`--max-units` parameter wiring (lines ~71 + the preflight task). Only ADD `--run-id`.

- [ ] **Step 4: Run**

Run: `uv run pytest src/tests/test_action_context_terraform.py -q` → PASS.
Run: `cd terraform/environments/dev && terraform validate` (if tooling available) → success. Skip if Terraform not installed locally; CI validates.

- [ ] **Step 5: Commit**

```bash
git add terraform/modules/workflows/main.tf src/tests/test_action_context_terraform.py
git commit -m "feat(ac-1): rewire for-each to worker-drain (8 workers, 8h, run-id param)"
```

---

## Task 12: IDSSE half-survival regression test (B2 precision)

**Files:**
- Test: `src/tests/action_context/test_action_context_queue.py` (serverless-marked) — or wherever real `_process_tracking_match` integration tests live.

- [ ] **Step 1: Write the test**

```python
def test_idsse_halves_survive_each_other(spark, tmp_catalog) -> None:  # noqa: ANN001
    """period-aware replaceWhere: writing period 2 must NOT delete period 1 (B2)."""
    from ingestion.action_context import _process_tracking_match, _load_xt_grid_from_delta
    import logging

    log = logging.getLogger("t")
    grid, l, w = _load_xt_grid_from_delta(spark, tmp_catalog, "bronze", log)
    mid = "<a real IDSSE match in tmp_catalog with periods 1 and 2>"
    _process_tracking_match(spark, tmp_catalog, "bronze", "idsse", mid, 1, grid, l, w, log)
    _process_tracking_match(spark, tmp_catalog, "bronze", "idsse", mid, 2, grid, l, w, log)
    rows = spark.table(f"{tmp_catalog}.bronze.spadl_action_context").where(
        f"match_id = '{mid}'"
    )
    periods = {r["period_id"] for r in rows.select("period_id").distinct().collect()}
    assert periods == {1, 2}  # period 1 survived period 2's write
```

- [ ] **Step 2: Run (serverless)**

Run: `RUN_SERVERLESS_TESTS=1 uv run pytest src/tests/action_context/test_action_context_queue.py -k idsse_halves -q`
Expected: PASS — proves the `match_id AND period_id` replaceWhere key, on which the drain "interrupt ⇒ wrote nothing" reasoning depends.

- [ ] **Step 3: Commit**

```bash
git add src/tests/action_context/test_action_context_queue.py
git commit -m "test(ac-1): IDSSE halves survive each other's replaceWhere"
```

---

## Task 13: Governance — ADR-037, CLAUDE.md, C4, wheel bump

**Files:**
- Create: `docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md`
- Modify: `CLAUDE.md`, `docs/c4/architecture.dsl`
- version: `scripts/bump_wheel.py`

- [ ] **Step 1: Write ADR-037**

Use `docs/superpowers/adrs/ADR-TEMPLATE.md`. Context: 48 KB task-value cap + "never cap games". Decision: worker-drain (durable queue + N persistent workers + per-game watchdog). Cover: 1800 s → per-game watchdog reinterpretation; `chunk_size` deletion; static assignment (A) with path to B; **Consequences must name (N8): 8 × 16 GB serverless drivers × ~5.5 h one-time cold start**, the 2 hr-budget exception, and the ~633 → 8 cold-start collapse. **R2 wording:** describe the concurrent abandonment ceiling as a *mitigation that lowers peak-memory risk*, NOT a guarantee against OOM — it is only re-evaluated on a timeout event (a long stretch of successful units after 3 live-abandoned threads can still OOM); the deferred subprocess approach (spec §5.2) is the real bound if event-only hangs are ever observed. Link the spec.

- [ ] **Step 2: Amend CLAUDE.md**

Under Performance Budgets / the 1800 s references: note `compute_action_context_iteration` is now a **drain worker** — the 1800 s is a **per-game watchdog** (not the iteration timeout), and this task is a **documented exception to the "compute task ≤ 2 hr" budget** (one-time cold start ~5.5 h). Remove any `chunk_sizes` references that are now stale.

- [ ] **Step 3: Update C4 (≤ ~200 chars per element, edit in place)**

In `docs/c4/architecture.dsl`, update the AC-1 / action-context element description to reflect the worker-drain fan-out (durable queue + N persistent workers). Keep it human-readable, ≤ ~200 chars (per project rule). Do not append — edit the existing box.

- [ ] **Step 4: Bump the wheel**

Run: `uv run python scripts/bump_wheel.py` (patch). NEVER edit `pyproject.toml version=` by hand. Confirm version propagated: `uv run python -c "from shared.wheel import WHEEL_VERSION; print(WHEEL_VERSION)"`.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/adrs/ADR-037-action-context-worker-drain-fanout.md CLAUDE.md docs/c4/architecture.dsl pyproject.toml src/shared/wheel.py
git commit -m "docs(ac-1): ADR-037 + CLAUDE.md + C4 + wheel bump for worker-drain"
```

---

## Task 14: Full local gate + spec-conformance sweep

**Files:** none (verification)

- [ ] **Step 1: Run the full quality gate**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/action_context/ src/tests/test_action_context_enrichment.py src/tests/test_action_context_terraform.py -q
```
Expected: all clean/PASS (serverless-marked tests SKIP offline).

- [ ] **Step 2: Confirm no stale references remain**

```bash
grep -rn "action_context_chunks" src/ terraform/   # expect: none
grep -rn "chunk_sizes" src/ingestion/action_context.py src/tests/  # expect: none
```

- [ ] **Step 3: import-linter (hexagon isolation)**

Run: `uv run lint-imports` (or the project's import-linter command). Expect: `drain.py` (analytics) has NO Spark/Delta/ingestion imports; adapters in ingestion. PASS.

- [ ] **Step 4: Re-run the AC-1 golden gates (unaffected, must stay green)**

Run: `uv run pytest src/tests/action_context/test_mini_golden.py -q`
Expected: PASS (the fan-out change is upstream of `run_work_unit`).

- [ ] **Step 5: Commit any fixups, then stop for review**

The plan ends here. Before the PR claims verified end-state, run on Databricks serverless:
- the serverless-marked tests (watchdog smoke, queue round-trip + run-id isolation, IDSSE halves) — proofs for B2/N3/B1/L2;
- **R5: confirm the for-each tolerates an empty input list** (a no-op preflight emits `action_context_worker_ids = []`). Most Databricks for-each builds treat `[]` as zero iterations (and the worker's empty-`run_id` guard is a second net), but a for-each that errors on empty input would fail every no-op daily run — so verify a no-op run goes green, not errored.

---

## Self-Review (spec coverage)

| Spec section | Task(s) |
|---|---|
| §4 topology / cold-start collapse | 8, 9, 11 |
| §5.1 pure core (ports, WorkAssignment, DrainSummary, cost_fn) | 1, 2, 3 |
| §5.2 adapters (DeltaWorkQueue, SparkInterruptWatchdog, SparkGameProcessor) | 6, 7 |
| §6 queue table (observability, replaceWhere, run_id task value) | 6, 8, 10 |
| §7 watchdog (thread model, interruptTag, atomic-write/IDSSE key) | 7, 12 |
| §8 error handling / abandonment ceiling / guarantee | 3 |
| §9 Terraform (worker-ids, run-id param, 8h, concurrency parity) | 11 |
| §10 tests (assign, drain, re-raise, ceiling, size-invariance, run-id isolation, smoke, IDSSE halves) | 2,3,5,6,7,12 |
| §11 governance (ADR-037, CLAUDE.md, C4, wheel) | 13 |
| chunk_size deletion + guard-test retirement | 4, 5 |
| B1 run-id, B2 atomic-write, H1 reuse WorkUnit, H2/N1 bounded abandon, H3 cost, N2 re-raise, N3 real-proc smoke, N4 stable tiebreak, N5 schema, N7 heartbeat | 4/8/11, 7/12, 1, 3/7, 1, 7, 7, 2, 6/10, 7 |

## Plan-review resolution (parallel-session review, 2026-06-02)

| # | Item | Resolution |
|---|---|---|
| **P1** | `main_preflight` ran discovery TWICE (check + body) | **Fixed.** `discover_units` memoises per guard instance; `check()` populates the cache, the preflight body reuses it. New test asserts the anti-joins run exactly once (Task 4). |
| **P2** | Monkeypatch `ac.X` vs function-local imports → dead stubs | **Fixed.** `DeltaWorkQueue`/`SparkInterruptWatchdog`/`SparkGameProcessor`/`assign_workers`/`drain_worker` hoisted to module-level imports in `action_context.py` (no cycle — reviewer-confirmed). Tasks 8/9. |
| **P3** | Abandonment ceiling counted cumulative, not concurrent | **Fixed.** Watchdog tracks thread refs; `live_abandoned_count` prunes-dead + counts alive; drain fails on **concurrent** > ceiling. Tasks 3/7. |
| **P4** | `ensure_table` didn't create the schema → test fails on fresh catalog | **Fixed.** `ensure_table` now `CREATE SCHEMA IF NOT EXISTS` first (idempotent). Task 6. |
| **P5** | Schema in 3 places, no parity test | **Fixed.** `_QUEUE_STRUCT` is the single source; `queue_columns_sql()` derives the CREATE; parity test pins migration DDL == struct. Tasks 6/10. |
| **P6** | Smoke cancellation assertion timing-flaky | **Fixed.** Adapter captures `interruptTag`'s returned op-ids; smoke asserts non-empty + controller-regained-control timing. Task 7. |
| **P7** | Cryptic catalog re-split in `enqueue` | **Fixed.** Store `self._catalog`. Task 6. |
| **P8** | `except BaseException` in watchdog target | **Fixed.** Narrowed to `except Exception`. Task 7. |
| smaller | dead `ac.ensure_table` patch; `bootstrap_hooks` patch site | **Fixed.** Dropped dead patch (Task 4); patch `ingestion.bootstrap.bootstrap_hooks` at source (Task 9). |
| GOOD | signatures, observability schema, N7 per-call heartbeat, import-linter, commit cadence | Confirmed by reviewer; N7 closed with a comment-only invariant note (Task 7). |

### Plan-review #3 resolution (2026-06-02)

| # | Item | Resolution |
|---|---|---|
| **R1** | Memo unconditional → stale-singleton footgun + test reuse | **Fixed.** Memo keyed on `(catalog, schema)` → self-invalidates, safe by construction even on the long-lived singleton. New test asserts a different target re-discovers; Task 5 gains a same-instance-reuse audit step. |
| **R2** | ADR shouldn't call the ceiling an OOM guarantee | **Fixed.** ADR instructions say "mitigation, not guarantee" (only re-evaluated on timeout; subprocess is the real bound). |
| **R3** | Smoke proof hinged solely on `interruptTag` return | **Fixed.** Timing (controller regained control) is the PRIMARY assert; op-ids are corroborating-only (warn, don't fail, on `[]`). |
| **R4** | Parity test couples to `simpleString` spellings opaquely | **Fixed.** Failure message spells out the required `int/bigint/double/string/timestamp` types. |
| **R5** | Empty-preflight for-each zero-iteration tolerance | **Added** to Task 14 serverless must-verify list (no-op run must go green, not error). |
