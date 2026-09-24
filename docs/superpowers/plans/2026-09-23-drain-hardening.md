# Drain Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the `gk_decision` `is_actor` crash and harden the shared worker-drain so a systemic per-unit failure fails fast (at a pre-fan-out canary, then via an in-loop circuit-breaker) instead of burning the full retry budget.

**Architecture:** Three changes on the shared drain machinery used by BOTH the AC drain and the tracking-marts drain: (1) a one-line guard so the SB360-only actor-bridge is skipped for real tracking frames; (2) a hybrid circuit-breaker inside `drain_worker` (consecutive-failure OR failure-rate-after-min-sample), per-worker, flush-before-raise; (3) an in-preflight dry-run canary (one unit per provider) that fails preflight before the fan-out. No timeout change (the 8 h per-iteration bound already exists on both drains).

**Tech Stack:** Python 3.10, pandas, PySpark (Databricks serverless), pytest, ruff, pyright, Terraform (workflow module), silly-kicks (read-only dependency).

**Spec:** `docs/superpowers/specs/2026-09-23-drain-hardening-design.md` (approved r2). Read it alongside this plan.

## Global Constraints

- **Commit discipline (owner rule, overrides the skill's per-task commit cadence):** NO per-task commits. Build the whole coherent change with tests green throughout; a SINGLE commit at the end, and only after explicit owner approval at the commit gate (Task 6). Do not add `git commit` steps to Tasks 1–5.
- **One feature branch** off `main` for the whole cycle: `feat/drain-hardening`.
- **silly-kicks is READ-ONLY** — the `is_actor` fix is lakehouse-side; do not edit `D:\Development\karstenskyt__silly-kicks`.
- **Full local gate before the commit gate** (CLAUDE.md): `uv run ruff check src/ scripts/`, `uv run ruff format --check src/ scripts/`, `uv run lint-imports`, `uv run python scripts/bump_wheel.py --check`, `uv run python scripts/pip_audit_ignores.py --check`, `uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py`, `uv run pytest src/tests/ -q`. Run pytest/pyright as background commands (>30 s) and poll.
- **`analytics` cannot import `ingestion`** (import-linter `analytics-isolation`): the breaker code lives in `src/analytics/action_context/drain.py` and must stay pure (stdlib + `analytics.*` only). `DrainCircuitBreakerError` is defined there.
- **Wheel bump is all-or-nothing:** edit `pyproject.toml` then run `scripts/bump_wheel.py` (never hand-edit consumers).
- **No wheel-consuming job run** until post-merge main CI is green (operator rollout, Task 6 note).

---

### Task 1: Fix Bug 1 — wire gk_decision for tracking frames (guard the bridge + auto-select the frame convention)

**Files:**
- Modify: `src/ingestion/gk_decision_writer.py:150-198` (`score_gk_decision_reconstructed` + `score_gk_decision_unit`)
- Test: `src/tests/test_gk_decision_writer.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: both functions take `frame_convention: Literal["per_action_ltr", "match_ltr"] | None = None`; when `None`, inferred from the `is_actor` signal (`per_action_ltr` if `"is_actor" in frames.columns` else `match_ltr`). The bridge is skipped when `is_actor` is absent. Result: tracking frames produce real gk_decision rows; SB360 frames unchanged.

- [ ] **Step 1: Write the failing tests** in `src/tests/test_gk_decision_writer.py`.

(a) A **tracking-shaped** fixture builder — raw per-frame frames (NO `is_actor`), real `player_id`s, `is_goalkeeper`, a ball row, `game_id`/`period_id`/`frame_id`/`timestamp`, two teams, a GK-distribution action with several frames around its timestamp so `link_actions_to_frames` links it and `resolve_defended_goals` can infer orientation:

```python
def _tracking_shaped_actions_and_frames():
    """Raw per-frame tracking frames (home-LTR), no is_actor — the shape build_unit_inputs emits.
    One GK-distribution goalkick by a real keeper, with frames spanning its timestamp."""
    actions = pd.DataFrame(
        [
            dict(
                game_id=1,
                period_id=1,
                action_id=1,
                time_seconds=10.0,
                type_id=_GOALKICK,
                player_id=_KEEPER_ID,
                team_id=1,
                start_x=8.0,
                start_y=34.0,
                end_x=30.0,
                end_y=34.0,
            )
        ]
    )
    rows = []
    for fid, t in enumerate([9.6, 9.8, 10.0, 10.2], start=1):  # frames around the action
        rows += [
            dict(
                game_id=1,
                period=1,
                frame_id=fid,
                timestamp=t,
                team_id=1,
                player_id=_KEEPER_ID,
                is_goalkeeper=True,
                x=8.0,
                y=34.0,
                ball=False,
            ),
            dict(
                game_id=1,
                period=1,
                frame_id=fid,
                timestamp=t,
                team_id=1,
                player_id=101,
                is_goalkeeper=False,
                x=30.0,
                y=34.0,
                ball=False,
            ),
            dict(
                game_id=1,
                period=1,
                frame_id=fid,
                timestamp=t,
                team_id=1,
                player_id=102,
                is_goalkeeper=False,
                x=45.0,
                y=20.0,
                ball=False,
            ),
            dict(
                game_id=1,
                period=1,
                frame_id=fid,
                timestamp=t,
                team_id=2,
                player_id=201,
                is_goalkeeper=False,
                x=20.0,
                y=34.0,
                ball=False,
            ),
            dict(
                game_id=1,
                period=1,
                frame_id=fid,
                timestamp=t,
                team_id=None,
                player_id=None,
                is_goalkeeper=False,
                x=8.0,
                y=34.0,
                ball=True,
            ),
        ]
    return actions, pd.DataFrame(rows)
```

> NOTE (implementer): the exact required frame columns are whatever sk `link_actions_to_frames` + `resolve_defended_goals` + `ReconstructedOptionSet` consume. Build the fixture, run it, and add any missing column sk asks for (e.g. a `ball`/is-ball marker under sk's expected name) until it produces a scored row. The fixture is correct when the assertion below passes — do not weaken the assertion to fit a thin fixture.

```python
def test_gk_decision_unit_tracking_frames_produces_rows():
    """Raw tracking frames (no is_actor) must score via match_ltr — NON-EMPTY output (proves link +
    orientation work, not just that the crash is gone)."""
    actions, frames = _tracking_shaped_actions_and_frames()
    assert "is_actor" not in frames.columns
    out = score_gk_decision_unit(
        actions,
        frames,
        _bundled_completion_model(),
        data_source="skillcorner",
        match_id="1",
        access_tier="restricted",
        params=_PARAMS,
    )
    assert list(out.columns) == list(OUTPUT_COLUMNS)
    assert not out.empty  # NON-VACUOUS: match_ltr linked + scored ≥1 decision
    assert (out["option_set_source"] == "reconstructed").all()


def test_actor_bridge_raises_on_frames_without_is_actor():
    """Pins the pre-fix mechanism: the SB360 bridge on tracking frames raises KeyError('is_actor')."""
    from silly_kicks.keeper_identity import apply_actor_identities_to_frames

    actions, frames = _tracking_shaped_actions_and_frames()
    with pytest.raises(KeyError):
        apply_actor_identities_to_frames(frames, actions)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest src/tests/test_gk_decision_writer.py -k "tracking_frames_produces_rows or actor_bridge_raises" -v`
Expected: `..._produces_rows` FAILS with `KeyError: 'is_actor'` (pre-guard); the companion PASSES.

- [ ] **Step 3: Apply the fix** in `score_gk_decision_reconstructed` (`gk_decision_writer.py:150-184`) — add `frame_convention: Literal["per_action_ltr", "match_ltr"] | None = None`, infer from `is_actor`, guard the bridge, pass the resolved convention:

```python
is_snapshot = "is_actor" in frames.columns
conv = frame_convention if frame_convention is not None else ("per_action_ltr" if is_snapshot else "match_ltr")
# SB360 snapshots carry an anonymous actor row bridged to the real keeper id; tracking frames already
# carry real player_ids (no is_actor) and link via match_ltr. Both derive from the same signal.
frames_with_ids = apply_actor_identities_to_frames(frames, actions) if is_snapshot else frames
gk_actions = actions[gk_distribution_mask(actions, frames_with_ids, resolve_gk="robust").to_numpy()]
option_set = ReconstructedOptionSet(
    gk_actions,
    frames_with_ids,
    xpass=completion_model,
    params=resolved_params,
    keeper_ids=keeper_ids,
    frame_convention=conv,
    visible_area=visible_area,
)
return compute_gk_decision_value(option_set, extra_drops=option_set.drop_counts())
```

And in `score_gk_decision_unit` (`:187-198`): change its `frame_convention` param to `Literal["per_action_ltr", "match_ltr"] | None = None` and forward it unchanged to `score_gk_decision_reconstructed`. Add `from typing import Literal` if not already imported.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest src/tests/test_gk_decision_writer.py -v`
Expected: the two new tests PASS (`..._produces_rows` non-empty); ALL existing SB360 tests (which have `is_actor` → inferred `per_action_ltr`, or pass it explicitly) still PASS.

---

### Task 2: `dry_run` on the processor port + both implementations

**Files:**
- Modify: `src/analytics/action_context/drain.py:128` (`GameProcessorPort.process` signature)
- Modify: `src/ingestion/tracking_marts_processor.py:225` (`TrackingMartsProcessor.process`)
- Modify: `src/ingestion/drain_adapters.py` (`SparkGameProcessor.process` — the AC processor)
- Test: `src/tests/test_tracking_marts_processor.py`, `src/tests/action_context/` (AC processor test if present; otherwise a focused new test)

**Interfaces:**
- Consumes: nothing new.
- Produces: `GameProcessorPort.process(self, unit, *, dry_run: bool = False) -> int`. When `dry_run=True`: run the full compute path, write nothing, return `0`. Both `SparkGameProcessor` and `TrackingMartsProcessor` honor it. Task 4's canary depends on this.

- [ ] **Step 1: Write the failing test (tracking-marts)** — assert `process(unit, dry_run=True)` computes but calls no `_write`.

```python
def test_tracking_marts_process_dry_run_writes_nothing(monkeypatch):
    proc = _build_tracking_marts_processor_with_fake_inputs(...)  # inputs non-empty
    writes = []
    monkeypatch.setattr(proc, "_write", lambda *a, **k: writes.append(a) or 0)
    rows = proc.process(_a_unit(), dry_run=True)
    assert rows == 0
    assert writes == []  # compute ran, nothing written
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest src/tests/test_tracking_marts_processor.py -k dry_run -v`
Expected: FAIL — `process()` currently takes no `dry_run` kwarg (TypeError) or writes.

- [ ] **Step 3: Update the port** `src/analytics/action_context/drain.py:128`

```python
class GameProcessorPort(Protocol):
    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int: ...
```

- [ ] **Step 4: Implement in `TrackingMartsProcessor.process`** (`tracking_marts_processor.py:225`): add `*, dry_run: bool = False`; guard every `self._write(...)` so the compute runs but the write is skipped when `dry_run`. Pattern for each of the writer blocks (off_ball_runs, defensive_credit long+agg, gkdv-if-enabled, gk_decision, rest_defense):

```python
    def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
        ...
        # gk_decision block (mirror for every writer block):
        try:
            gk = score_gk_decision_unit(inputs.actions, inputs.frames, self._completion_model, ...)
            if not dry_run:
                total += self._write(gk, self._gk_decision_schema, GK_DECISION_TABLE, where)
        except Exception as exc:  # noqa: BLE001 — attributed + re-raised as a combined unit failure below
            errors.append(f"gk_decision: {exc}")
        ...
        return total  # 0 under dry_run
```

Keep the existing combined-failure `raise` at the end of `process` so a dry-run compute error still propagates (the canary needs the raise).

- [ ] **Step 5: Implement in `SparkGameProcessor.process`** (`drain_adapters.py`): add `*, dry_run: bool = False`. The AC compute is executor-side (`applyInPandas`), so `dry_run` must FORCE materialization (trigger the UDF) without persisting: build the scored DataFrame as today, then `if dry_run: scored_sdf.count(); return 0` else write. `count()` runs the UDF over every group of the (single) canary match, surfacing any executor-side compute error.

```python
def process(self, unit: WorkUnit, *, dry_run: bool = False) -> int:
    ...
    scored_sdf = ...  # unchanged build
    if dry_run:
        scored_sdf.count()  # force the applyInPandas UDF to run; surfaces compute errors, writes nothing
        return 0
    return self._write(scored_sdf, ...)  # unchanged
```

`count()` cannot be optimized past an opaque grouped `applyInPandas` UDF (variable output rows), so it materializes the UDF. If a future Spark optimizer ever short-circuits it, the guaranteed-materialization fallback is `scored_sdf.write.format("noop").mode("overwrite").save()` — confirm `count()` triggers the UDF in the Spark-marked test (Step 6) and switch to `noop` only if it does not.

- [ ] **Step 6: AC dry-run test** — mirror Step 1 for `SparkGameProcessor`: a fake/patched write asserts `dry_run=True` triggers `count()` and does not write. If the AC processor is Spark-bound and hard to unit-test offline, add the assertion at the port level (a fake processor honoring `dry_run`) and cover `SparkGameProcessor` in the existing Spark-marked suite; do not leave it untested.

- [ ] **Step 7: Run to verify pass**

Run: `uv run pytest src/tests/test_tracking_marts_processor.py -k dry_run -v` (+ the AC dry-run test)
Expected: PASS.

---

### Task 3: Hybrid circuit-breaker in the shared `drain_worker`

**Files:**
- Modify: `src/analytics/action_context/drain.py` (`DrainSummary`, `drain_worker`, new `DrainCircuitBreakerError`, `UnitEventSink.slice_completed` signature)
- Modify: `src/ingestion/drain_adapters.py:393` (`DeltaUnitEventSink.slice_completed`)
- Modify: `src/ingestion/action_context.py:1418` + `src/ingestion/tracking_marts_drain.py:295` (worker entry points: three CLI args)
- Test: `src/tests/action_context/test_drain.py` (breaker unit tests against fakes)
- Update fakes: `src/tests/action_context/test_drain.py::_NullSink`, `src/tests/test_action_context_enrichment.py::_FakeUnitEventSink`, `src/tests/test_tracking_marts_drain_worker.py::_RecordingSink` (new `slice_completed` kwarg)

**Interfaces:**
- Consumes: `GameProcessorPort.process` (Task 2 — unchanged usage here).
- Produces:
  - `DrainCircuitBreakerError(RuntimeError)` in `drain.py`.
  - `DrainSummary` gains `circuit_broken: bool = False` and `abort_reason: str | None = None`.
  - `drain_worker(..., max_consecutive_failures: int = 6, systemic_failure_rate: float = 0.5, systemic_min_sample: int = 20)`.
  - `UnitEventSink.slice_completed(self, run_id, worker_id, *, abort_reason: str | None = None)` — writes `abort_reason` into the existing `error` column; no schema change.

- [ ] **Step 1: Write the failing tests** in `src/tests/action_context/test_drain.py`, using the existing fake ports (`_NullSink` / a recording sink + a fake processor + a pass-through watchdog). Four tests:

```python
def test_breaker_trips_on_consecutive_failures():
    proc = _AlwaysFailProcessor()  # every unit raises
    sink = _RecordingSink()
    with pytest.raises(DrainCircuitBreakerError):
        drain_worker(
            _queue(units=_n_units(50)),
            proc,
            _PassthroughWatchdog(),
            "run",
            0,
            _log(),
            sink=sink,
            units=_n_units(50),
            max_consecutive_failures=6,
            systemic_min_sample=20,
            systemic_failure_rate=0.5,
        )
    # tripped EARLY, not at slice end:
    assert sink.count_state("failed") == 6  # ~K, far below 50
    assert sink.flushed_before_raise is True  # terminals preserved


def test_breaker_trips_on_rate_after_min_sample():
    # 12 fail / 8 succeed interleaved so consecutive never reaches 6, but rate>0.5 after >=20 attempted
    proc = _ScriptedProcessor(pattern=_interleave(fail=12, ok=8))
    with pytest.raises(DrainCircuitBreakerError):
        drain_worker(
            ..., units=_n_units(20), max_consecutive_failures=6, systemic_min_sample=20, systemic_failure_rate=0.5
        )


def test_breaker_does_not_trip_below_thresholds():
    # 3 scattered failures among 30 successes: neither consecutive>=6 nor rate>0.5 -> NO raise (non-vacuous)
    proc = _ScriptedProcessor(pattern=_scatter(fail=3, ok=27))
    summary = drain_worker(
        ..., units=_n_units(30), max_consecutive_failures=6, systemic_min_sample=20, systemic_failure_rate=0.5
    )
    assert summary.failed == 3 and summary.circuit_broken is False


def test_breaker_emits_slice_completed_with_abort_reason():
    proc = _AlwaysFailProcessor()
    sink = _RecordingSink()
    with pytest.raises(DrainCircuitBreakerError):
        drain_worker(..., sink=sink, units=_n_units(50))
    assert sink.slice_abort_reason is not None and "circuit" in sink.slice_abort_reason
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest src/tests/action_context/test_drain.py -k breaker -v`
Expected: FAIL — `DrainCircuitBreakerError` undefined / no breaker logic / `slice_completed` has no `abort_reason`.

- [ ] **Step 3: Add the error + summary fields** in `drain.py`:

```python
class DrainCircuitBreakerError(RuntimeError):
    """Raised when a worker aborts early because failures are systemic (not one bad unit)."""


@dataclass
class DrainSummary:
    worker_id: int
    processed: int = 0
    failed: int = 0
    timed_out: int = 0
    total_rows: int = 0
    failed_units: list[str] = field(default_factory=list)
    timed_out_units: list[str] = field(default_factory=list)
    circuit_broken: bool = False
    abort_reason: str | None = None
```

- [ ] **Step 4: Add the breaker to `drain_worker`** — signature params + tracking + trip check inside the failure branch (`drain.py:305`, after `summary.failed += 1`). `attempted = summary.processed + summary.failed` (timeouts excluded). Reset `consecutive_failures` to 0 on a `succeeded` unit (at `drain.py:337` area).

```python
def drain_worker(..., *, sink, budget_s=WATCHDOG_BUDGET_S, max_abandoned=MAX_ABANDONED_THREADS,
                 units=None, flush_every=TERMINAL_FLUSH_EVERY,
                 max_consecutive_failures: int = 6, systemic_failure_rate: float = 0.5,
                 systemic_min_sample: int = 20) -> DrainSummary:
    ...
    consecutive_failures = 0
    ...
    # in the `except Exception` failure branch, AFTER summary.failed += 1 and the log:
        consecutive_failures += 1
        attempted = summary.processed + summary.failed
        rate_trip = attempted >= systemic_min_sample and summary.failed / attempted > systemic_failure_rate
        if consecutive_failures >= max_consecutive_failures or rate_trip:
            reason = (f"circuit-breaker: worker {worker_id} {summary.failed}/{attempted} units failed "
                      f"(consecutive={consecutive_failures}); last error: {exc}")
            summary.circuit_broken = True
            summary.abort_reason = reason
            _flush()                                   # preserve terminals (same as abandon-ceiling)
            sink.slice_completed(run_id, worker_id, abort_reason=reason)
            logger.error("ac1_drain_circuit_broken run_id=%s worker_id=%d %s", run_id, worker_id, reason)
            raise DrainCircuitBreakerError(reason) from exc
        continue
    # in the success path (after summary.processed += 1): consecutive_failures = 0
```

Note: this raise emits `slice_completed(abort_reason=...)` BEFORE raising (unlike the abandon-ceiling, which does not) so the gate can see WHY; then it bypasses `raise_on_failed_units` (it is the fast failure).

- [ ] **Step 5: Extend `slice_completed`** — port (`drain.py:187`) + `DeltaUnitEventSink` (`drain_adapters.py:393`): add `*, abort_reason: str | None = None`, and set `error=abort_reason` in the `_event_row(...)` call (uses the existing `error` column — no schema change). Update the three test fakes' `slice_completed` to accept + record `abort_reason`.

- [ ] **Step 6: Thread the three params through both worker entry points** — `action_context.main_drain_worker` (`:1418`) and `tracking_marts_drain.main_tracking_marts_drain_worker` (`:295`): add `--max-consecutive-failures`, `--systemic-failure-rate`, `--systemic-min-sample` CLI args (empty → defaults), parse (mirror the `_parse_budget` pattern at `tracking_marts_drain.py:224`), pass into `drain_worker`/`_run_worker`. `_run_worker` (`tracking_marts_drain.py:238`) forwards them.

- [ ] **Step 7: Run to verify pass**

Run: `uv run pytest src/tests/action_context/test_drain.py src/tests/test_tracking_marts_drain_worker.py -v`
Expected: all PASS, including the below-threshold non-vacuity test.

---

### Task 4: In-preflight dry-run canary (one unit per provider) on both preflights

**Files:**
- Create: `src/analytics/action_context/canary.py` (pure helper: pick one unit per provider)
- Modify: `src/ingestion/tracking_marts_drain.py:191` (AFTER the `if not units: return` block at 186-190, before `assign_workers` at :192)
- Modify: `src/ingestion/action_context.py:953` (`main_preflight`, after discovery, before enqueue)
- Test: `src/tests/action_context/test_canary.py` (pure selection) + preflight-level tests

**Interfaces:**
- Consumes: `GameProcessorPort.process(unit, dry_run=True)` (Task 2).
- Produces: `select_canary_units(units: list[WorkUnit]) -> list[WorkUnit]` (first unit per distinct provider, stable order); `run_canary(processor, units, logger) -> None` (raises on the first canary failure with the unit label + error).

- [ ] **Step 1: Write the failing test (pure selection)** in `src/tests/action_context/test_canary.py`:

```python
def test_select_canary_units_one_per_provider():
    units = [_u("skillcorner", "1", 1), _u("skillcorner", "2", 1), _u("gradientsports", "3", 1), _u("idsse", "J", 1)]
    got = select_canary_units(units)
    assert {u.provider for u in got} == {"skillcorner", "gradientsports", "idsse"}
    assert len(got) == 3  # one per provider


def test_run_canary_raises_on_processor_failure():
    with pytest.raises(RuntimeError, match="canary"):
        run_canary(_AlwaysFailProcessor(), [_u("skillcorner", "1", 1)], _log())


def test_run_canary_passes_when_processor_ok():
    run_canary(_OkProcessor(), [_u("skillcorner", "1", 1)], _log())  # no raise
```

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest src/tests/action_context/test_canary.py -v`
Expected: FAIL — `canary` module absent.

- [ ] **Step 3: Implement `src/analytics/action_context/canary.py`** (pure; `analytics`-isolation safe):

```python
from __future__ import annotations
import logging
from analytics.action_context.drain import GameProcessorPort, unit_label
from analytics.action_context.work_unit import WorkUnit


def select_canary_units(units: list[WorkUnit]) -> list[WorkUnit]:
    """One unit per distinct provider, first-seen order (deterministic)."""
    seen: dict[str, WorkUnit] = {}
    for u in units:
        seen.setdefault(u.provider, u)
    return list(seen.values())


def run_canary(processor: GameProcessorPort, units: list[WorkUnit], logger: logging.Logger) -> None:
    """Dry-run one unit per provider through the FULL processor before the fan-out. Raise on the first
    failure (compute-path bugs like the gk_decision is_actor crash surface here, before any writes)."""
    for u in select_canary_units(units):
        label = unit_label(u)
        try:
            processor.process(u, dry_run=True)
        except Exception as exc:
            logger.error("drain_canary_failed unit=%s err=%s", label, exc, exc_info=True)
            raise RuntimeError(f"drain canary FAILED on {label}: {exc}") from exc
        logger.info("drain_canary_ok unit=%s", label)
```

- [ ] **Step 4: Wire into the tracking-marts preflight** (`tracking_marts_drain.py`, AFTER the `if not units: return` early-return block at 186-190, i.e. at `:191`, before `assign_workers` at `:192`). Placing it after the return means a quiet preflight (nothing to do) returns without building the processor — no model/xT load for a no-op, matching the AC rule in Step 5 and spec Change-3 step 1:

```python
# units is guaranteed non-empty here (the `if not units: return` above handled the quiet case).
from ingestion.tracking_marts_processor import TrackingMartsProcessor
from analytics.action_context.canary import run_canary

run_canary(TrackingMartsProcessor(spark, args.catalog, args.schema), units, task_logger)
```

- [ ] **Step 5: Wire into the AC preflight** (`action_context.main_preflight` at `:953`, after discovery, before the enqueue round-trip; skip when zero units):

```python
from ingestion.drain_adapters import SparkGameProcessor
from analytics.action_context.canary import run_canary

run_canary(SparkGameProcessor(spark, args.catalog, args.schema), units, task_logger)
```

Place AFTER `ensure_tables` and the nothing-to-do check so a quiet run does not build a processor. (`compute_*` already depends on preflight, so a raised preflight skips the fan-out.)

- [ ] **Step 6: Preflight-level tests** — patch the processor constructor to a fake that raises on the canary unit → assert preflight raises (and never reaches `assign_workers`/`enqueue`); a healthy fake → assert the queue is filled. Non-vacuous: assert the broken processor is actually caught.

- [ ] **Step 7: Run to verify pass**

Run: `uv run pytest src/tests/action_context/test_canary.py src/tests/test_tracking_marts_preflight.py -v`
Expected: PASS.

---

### Task 5: ADR + wheel bump + TODO

**Files:**
- Create: `docs/superpowers/adrs/ADR-087-drain-circuit-breaker-and-canary.md` (next free ADR number — verify with `ls docs/superpowers/adrs/`)
- Modify: `pyproject.toml` (version) then run `scripts/bump_wheel.py`
- Modify: `TODO.md`

- [ ] **Step 1: Write the ADR** (Nygard format, `docs/superpowers/adrs/ADR-TEMPLATE.md`), extending ADR-037/067/068/082. Content: the `gk_decision` `is_actor` root cause + guard; the failure-vs-timeout distinction (8 h per-iteration timeout already exists + is the wrong tool for a failing run); the hybrid breaker (thresholds, per-worker semantics, flush-before-raise, `slice_completed(abort_reason)` via the existing `error` column, bypass of `raise_on_failed_units`); the in-preflight dry-run canary (one-per-provider, non-skippable, the bad-canary-unit edge case). Reference the incident (cancelled run `622418943470489`, 654 `gk_decision: 'is_actor'` failures).

- [ ] **Step 2: Bump the wheel**

```bash
# edit pyproject.toml version 0.5.114 -> 0.5.115, then:
uv run python scripts/bump_wheel.py
uv run python scripts/bump_wheel.py --check
```

- [ ] **Step 3: Update `TODO.md`** — add a block for this cycle (branch `feat/drain-hardening`, ADR-087, the three changes + the gk_decision fix); note the P2 tracking-marts re-run happens post-merge. Verify `git status TODO.md` shows the edit.

---

### Task 6: Full gate + commit gate (approval-gated) + operator rollout

- [ ] **Step 1: Run the full local gate** (all seven + card validation), each long one backgrounded + polled:

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run lint-imports
uv run python scripts/bump_wheel.py --check
uv run python scripts/pip_audit_ignores.py --check
uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py
uv run pytest src/tests/ -q
```
Expected: all green. Fix any failure before proceeding.

- [ ] **Step 2: Plan self-review vs spec** — confirm every spec section maps to a task (Change 1→T1, Change 2→T3, Change 3→T2+T4, tests→each, ADR/release→T5); no placeholders; type/name consistency (`process(dry_run=)`, `DrainCircuitBreakerError`, `slice_completed(abort_reason=)`, `select_canary_units`/`run_canary`).

- [ ] **Step 3: STOP — commit gate.** Present the diff / file list to the owner and wait for explicit approval for THIS commit (CLAUDE.md: no `git commit`/`push`/`gh pr create`/`gh pr merge` without separate explicit approval). Do not commit before the yes.

- [ ] **Step 4 (after approval): single commit** on `feat/drain-hardening` (message summarizing the gk_decision fix + drain hardening, ending with the session's attribution line), then push + PR only on the owner's further explicit go, and watch CI to green.

- [ ] **Step 5 (post-merge, wheel deployed, main CI green — operator):** re-run `preflight_tracking_marts` + `compute_tracking_marts` with `tracking_marts_full=true`. Expected: canary green (gk_decision fixed), breaker idle, all 182 tracking matches produce `off_ball_runs` + `gk_decision` + `rest_defense` + `defensive_credit`. Verify per-mart row counts AND — per D1-IMPL-06 — `gk_decision` **coverage per provider**, NOT just non-empty: compare `fct_gk_decision` decision rows against the count of GK-distribution decisions (production full-match frames link at a high rate, unlike the fixture snippet, so coverage should be near-complete); a sparse mart (the silent-sparse variant this cycle targets) is a failure, not a pass. Then resume the remaining P2 steps (build marts + synced tables, HF publishes, end-state verify + spend). The clean re-run supersedes the cancelled run `622418943470489` (idempotent `replace_where`); no interim cleanup.

## Self-Review

- **Spec coverage:** Change 1 → Task 1. Change 2 (breaker) → Task 3. Change 3 (dry-run + canary) → Task 2 + Task 4. Timeout → intentionally none (spec: already exists). Tests → each task's TDD steps (breaker non-vacuity in T3 Step 1; dry-run-writes-nothing in T2). ADR + wheel + TODO → Task 5. Rollout → Task 6 Step 5. No gaps.
- **Placeholder scan:** none — every code/test step carries real code; the one "if hard to unit-test offline" note (T2 Step 6) is a documented judgment with a concrete fallback, not a placeholder.
- **Type/name consistency:** `process(self, unit, *, dry_run=False)` (port + both impls); `DrainCircuitBreakerError`; `DrainSummary.circuit_broken/abort_reason`; `slice_completed(..., *, abort_reason=None)`; `select_canary_units`/`run_canary`; breaker params `max_consecutive_failures`/`systemic_failure_rate`/`systemic_min_sample` — consistent across Tasks 2/3/4.
