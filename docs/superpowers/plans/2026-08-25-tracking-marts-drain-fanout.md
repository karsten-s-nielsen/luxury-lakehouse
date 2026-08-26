# Tracking-Marts Drain Fan-Out — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the three driver-sequential tracking-grain writers with ONE consolidated drain that
reuses `analytics.action_context.drain`'s pure fan-out core, so they complete within budget, run
incrementally, and prove completeness (ADR-068).

**Architecture:** A new `tracking_marts` drain — preflight (events-based skip-guard + `assign_workers`) →
`for_each_task` N=8 workers (`TrackingMartsProcessor.process(unit)` builds inputs once + runs all three
scorers + per-unit `replaceWhere`) → completeness gate → separate single-driver `gkdv_pool` reduce. Reuses
`drain.py` verbatim via adapters generalized by a `drain_name` parameter.

**Tech Stack:** PySpark serverless (16 GB driver-mode), Delta, silly-kicks tracking/gkdv, `analytics.action_context.drain` + `.drain_gate` pure cores, Terraform `for_each_task`.

## Global Constraints

- **`analytics.action_context.drain` and `.drain_gate` are reused UNCHANGED** — pure cores; do not fork.
- **`drain_name` generalization is backward-compatible** — `action_context` remains the default on every
  adapter/helper; the AC drain's behaviour and its tests must be byte-unchanged.
- **ADR-068 events mandatory**: `unit_started` (fail-open, pre-process), buffered `unit_finished`, periodic
  `flush_terminals` (fail-open), `slice_completed` (fail-loud). Event writes are `mode="append"` explicitly.
- **`raise_on_failed_units` after the drain** (ADR-067). **Skip-guard is events-based** (spec §3.1).
- **`run_id` is `{{job.run_id}}` everywhere** (preflight, worker, gate) — never the preflight task value.
- **N=8 workers** (`_N_TRACKING_MARTS_WORKERS`), pinned == TF `concurrency` == event-worker count, with
  parity tests. **Watchdog 2700 s**, 8 h task timeout.
- **silly-kicks scoring math unchanged** — the pure functions (`compute_off_ball_runs`,
  `compute_action_defensive_credit`, `compute_defensive_credit_long`, gkdv `score_unit`/`pool_keepers`) are
  called as today, just per-unit in a processor.
- **7-gate green + wheel bump + env-pin sync + cards + ADR** before PR (CLAUDE.md).

## File Structure

- **Modify** `src/ingestion/action_context_queue.py` — add `drain_name: str = "action_context"` to
  `DeltaWorkQueue.__init__`, `DeltaUnitEventSink.__init__`, and `event_table_for_worker`/`event_table_names`/
  `event_view_sql` (default preserves AC), plus `include_sb360: bool = True` on the event-table helpers +
  sink (tracking-marts passes `False` — the worker-topology axis, G1). Table names → `f"{drain_name}_work_queue"` /
  `f"{drain_name}_unit_events_w{id}"` / view `f"{drain_name}_unit_events"`. (Module keeps its name to avoid
  import churn; a rename is an out-of-scope follow-up — noted in the ADR.)
- **Modify** `src/analytics/action_context/drain_gate.py` (Task 1B, G1) — add
  `extra_expected_workers: frozenset[int] = frozenset({SB360_WORKER_ID})` to `evaluate` (`:401` uses it instead
  of the hard-coded `| {SB360_WORKER_ID}`). AC passes the default → byte-identical; tracking-marts passes
  `frozenset()`. The one edit to a pure core — minimal, backward-compatible, guarded by `test_drain_gate.py`.
- **Modify** `src/ingestion/tracking_marts_driver.py` — extract the per-unit body of `iter_unit_inputs`
  into `read_and_build_unit_inputs(spark, catalog, unit) -> UnitInputs | None` (returns `None` for an empty
  unit); keep `discover_tracking_units`, `resolve_unit_meta`, `ac_xt_grid`. `iter_unit_inputs` may remain
  (now a thin generator over the new fn) for any test that still uses it, or be deleted with its callers.
- **Create** `src/ingestion/tracking_marts_processor.py` — `TrackingMartsProcessor` (`GameProcessorPort`);
  `pool_gkdv_observations` (pure reduce, factored from `gkdv_writer.run_pipeline`); the `gkdv_observations`
  intermediate schema constant.
- **Create** `src/ingestion/tracking_marts_drain.py` — `main_tracking_marts_preflight`,
  `main_tracking_marts_drain_worker`, `main_gkdv_pool`; the `_N_TRACKING_MARTS_WORKERS = 8` constant;
  `discover_units` + events-based skip-guard helper.
- **Create** `src/ingestion/tracking_marts_gate.py` — `main` (`verify_tracking_marts_drain`), reusing
  `analytics.action_context.drain_gate`.
- **Modify** `src/ingestion/{off_ball_runs,defensive_credit,gkdv}_writer.py` — keep the pure scoring
  functions; delete the driver-loop `run_pipeline` + `main()` + `iter_unit_inputs` usage. Move the gkdv
  per-unit `score_unit` + observation-stamping into a reusable `score_gkdv_unit(...)` the processor calls.
- **Modify** `terraform/modules/workflows/main.tf` — remove `off_ball_runs_writer`,
  `defensive_credit_writer`, `gkdv_writer`; add `preflight_tracking_marts` →
  `compute_tracking_marts` (`for_each_task`) → `verify_tracking_marts_drain` → `compute_gkdv_pool`; rewire
  `dbt_build_output_marts.depends_on`.
- **Modify** `pyproject.toml` — swap the 3 writer scripts for the 4 new entry points; version bump.
- **Create** `workflow-cards/wf-tracking-marts.yaml`; delete the 3 old writer cards; update the parity map.
- **Create** `scripts/migrations/2026-08-25-create-gkdv-observations.sql` (idempotent `CREATE TABLE IF NOT EXISTS`).
- **Create** `docs/superpowers/adrs/ADR-0XX-tracking-marts-drain-fanout.md`.
- **Create/modify tests** per task (see below).

---

### Task 1: Generalize the drain adapters by `drain_name`

**Files:**
- Modify: `src/ingestion/action_context_queue.py`
- Test: `src/tests/test_drain_adapter_drain_name.py` (new); existing `src/tests/test_action_context_queue.py` must still pass unchanged.

**Interfaces:** (TWO generalization axes — `drain_name` for table *names*, `include_sb360` for worker *topology*, per G1)
- Produces: `DeltaWorkQueue(spark, catalog, *, drain_name="action_context")`,
  `DeltaUnitEventSink(spark, catalog, logger=None, *, drain_name="action_context", include_sb360=True)`,
  `event_table_for_worker(worker_id, *, drain_name="action_context")`,
  `event_table_names(*, drain_name="action_context", include_sb360=True)`,
  `event_view_sql(catalog, *, drain_name="action_context", include_sb360=True)`.

- [ ] **Step 1: Failing test — new drain_name yields new names; a NO-sb360 drain omits the phantom table.**
```python
# src/tests/test_drain_adapter_drain_name.py
from ingestion.action_context_queue import event_table_for_worker, event_table_names, event_view_sql

def test_default_drain_name_is_action_context_unchanged():
    assert event_table_for_worker(0) == "action_context_unit_events_w0"
    names = event_table_names()  # AC default -> INCLUDES sb360 (byte-identical)
    assert names[0] == "action_context_unit_events_w0" and names[-1].endswith("_sb360")
    assert "action_context_unit_events" in event_view_sql("cat")

def test_tracking_marts_drain_name_namespaces_tables_without_sb360():
    assert event_table_for_worker(0, drain_name="tracking_marts") == "tracking_marts_unit_events_w0"
    names = event_table_names(drain_name="tracking_marts", include_sb360=False)
    assert names[0] == "tracking_marts_unit_events_w0"
    assert not any(n.endswith("_sb360") for n in names)   # G1: no phantom sb360 worker for a no-sb360 drain
    assert "tracking_marts_unit_events" in event_view_sql("cat", drain_name="tracking_marts", include_sb360=False)
```
- [ ] **Step 2: Run — expect FAIL** (`TypeError: unexpected keyword`). `uv run pytest src/tests/test_drain_adapter_drain_name.py -v`
- [ ] **Step 3: Implement.** Thread `drain_name` through: the two `__init__`s store `self._drain_name`; `self._table = f"{catalog}.{_QUEUE_SCHEMA}.{drain_name}_work_queue"`; `event_table_for_worker`/`event_table_names`/`event_view_sql` take `*, drain_name="action_context"`. Thread `include_sb360` through `event_table_names`/`event_view_sql`/`DeltaUnitEventSink.__init__`→`ensure_tables`: when `False`, DROP the `+ [event_table_for_worker(SB360_WORKER_ID)]` tail so no `_sb360` table/view-arm is created. `_ensure_one_table`/`_write`/`slice_completed` pass `drain_name=self._drain_name` (+ `include_sb360=self._include_sb360` where the helper needs it). **Do not change the column schemas.**
- [ ] **Step 4: Run new test + the full existing suite for this module.** `uv run pytest src/tests/test_drain_adapter_drain_name.py src/tests/test_action_context_queue.py -v` — both PASS (AC defaults byte-unchanged).

### Task 1B: Parameterize the sb360 worker-topology axis in the pure gate (G1 blocker)

**Files:**
- Modify: `src/analytics/action_context/drain_gate.py`
- Test: `src/tests/test_drain_gate.py` (extend — do NOT alter existing AC assertions)

**Why:** `evaluate:401` unconditionally unions `SB360_WORKER_ID` into `expected_workers`, and `:404` marks
any expected worker with no `slice_completed` as `dead` → `DRAIN_FAILED` (rule 0). A drain with **no** sb360
task (tracking-marts) would report `DRAIN_FAILED` on every run, muting the real verdict. Parameterize the
axis; AC keeps the default → byte-identical.

**Interfaces:**
- Produces: `evaluate(*, run_id, queue, events, result_counts, planner, extra_expected_workers: frozenset[int] = frozenset({SB360_WORKER_ID}))`.

- [ ] **Step 1: Failing test** — a no-sb360 fixture (a queue with workers {0,1}, matching `slice_completed`
  for 0 and 1, all units terminal) returns:
```python
def test_evaluate_without_sb360_worker_is_complete():
    report = evaluate(run_id="r", queue=[QueueRow(0,"idsse","m",1)],
                      events=[UnitEvent(0,"idsse","m",1,state="succeeded",rows_written=5),
                              UnitEvent(0,"idsse","m",1,state="slice_completed",write_failures=0)],
                      result_counts={("idsse","m",1):5}, planner=PlannerInputs(enqueued=1, remaining=frozenset()),
                      extra_expected_workers=frozenset())
    assert report.verdict is Verdict.COMPLETE          # NOT DRAIN_FAILED
    assert -1 not in report.expected_workers

def test_evaluate_default_still_expects_sb360_sentinel():   # AC byte-identical
    report = evaluate(run_id="r", queue=[QueueRow(0,"idsse","m",1)],
                      events=[UnitEvent(0,"idsse","m",1,state="succeeded",rows_written=5),
                              UnitEvent(0,"idsse","m",1,state="slice_completed",write_failures=0)],
                      result_counts={("idsse","m",1):5}, planner=PlannerInputs(enqueued=1, remaining=frozenset()))
    assert report.verdict is Verdict.DRAIN_FAILED and -1 in report.expected_workers  # sb360 sentinel dead
```
- [ ] **Step 2: Run — expect FAIL** (`TypeError: unexpected keyword 'extra_expected_workers'`).
- [ ] **Step 3: Implement.** Add the `extra_expected_workers` kw-only param (default `frozenset({SB360_WORKER_ID})`);
  line 401 becomes `expected_workers = {row.worker_id for row in queue} | extra_expected_workers`. Nothing
  else changes — `expected_units`'s sb360 branch (`:361`) is a no-op when no event has `worker_id == -1`, and
  rule 3 (`:425`) never fires when no unit is owned by `-1`. Update the module docstring's sb360 section to
  note the parameterization (AC default preserves the union).
- [ ] **Step 4: Run** the new tests + the FULL existing `test_drain_gate.py` — all PASS (AC verdicts byte-unchanged).

### Task 2: `gkdv_observations` intermediate table + pure pooling reduce

**Files:**
- Create: `scripts/migrations/2026-08-25-create-gkdv-observations.sql`
- Modify: `src/ingestion/tracking_marts_processor.py` (new — add `_GKDV_OBS_COLUMNS` + `pool_gkdv_observations`)
- Modify: `src/ingestion/gkdv_writer.py` (expose `score_gkdv_unit`, keep `pool_keepers` call factored)
- Test: `src/tests/test_gkdv_pool_split.py` (new)

**Interfaces:**
- Produces: `pool_gkdv_observations(observations: pd.DataFrame, *, providers) -> pd.DataFrame` (per-provider
  `pool_keepers`, mirroring today's `run_pipeline` end-stage) → the existing `gkdv_keeper_pooled` schema.
- `score_gkdv_unit(frames, home_team_id, xt, *, want_threat=True) -> pd.DataFrame` (the per-unit
  observations with `data_source/game_id/competition_id/season_id` stamped — the current loop body).

- [ ] **Step 1: Failing golden test** — scoring a small fixture corpus per-unit then `pool_gkdv_observations`
  reproduces exactly what today's `gkdv_writer.run_pipeline` end-stage produces (value equality on the pooled
  keeper rows). Reuse the existing gkdv test fixture (grep `test_gkdv`).
- [ ] **Step 2: Run — expect FAIL** (functions absent).
- [ ] **Step 3: Implement.** Factor the current `run_pipeline` (gkdv_writer.py:379-445) into (a)
  `score_gkdv_unit` (lines 405-422 body: `score_unit` + stamp), (b) `pool_gkdv_observations` (lines 428-444:
  `pd.concat` is the caller's job; this fn takes the concatenated observations and does
  `pool_keepers` + per-provider slice). Define `_GKDV_OBS_COLUMNS` (observation columns + `data_source,
  match_id, period_id, game_id, competition_id, season_id`). Write the migration DDL for
  `bronze.gkdv_observations` (`CREATE TABLE IF NOT EXISTS ... USING DELTA`, columns from `_GKDV_OBS_COLUMNS`
  + `_ingested_at`).
- [ ] **Step 4: Run** — golden test PASSES (pooled output identical to today's).

### Task 3: Single-unit input builder

**Files:**
- Modify: `src/ingestion/tracking_marts_driver.py`
- Test: `src/tests/test_tracking_marts_driver.py` (extend)

**Interfaces:**
- Produces: `read_and_build_unit_inputs(spark, catalog, unit: WorkUnit) -> UnitInputs | None` — reads the
  unit's tracking + actions, resolves meta, builds oriented inputs; returns `None` when the unit is empty
  (mirrors the current `if trk_pdf.empty or actions_pdf.empty: continue`).

- [ ] **Step 1: Failing test** — a mocked-Spark or fixture test asserting `read_and_build_unit_inputs` for a
  `WorkUnit(provider, match_id, period)` returns a `UnitInputs` with `actions/frames/xt`, and `None` for an
  empty unit. (Follow the existing module's test posture; Spark reads are validated live — this test covers
  the empty-guard + wiring on a fixture, not a live read.)
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement.** Extract the per-unit LOOP BODY of `iter_unit_inputs` (lines ~235-254) into
  `read_and_build_unit_inputs`. **G5:** `WorkUnit`, `UnitInputs`, and `build_unit_inputs` are NOT local to
  `tracking_marts_driver` — they are imported from `analytics.action_context.work_unit` /
  `analytics.action_context.unit_inputs` (already imported at `tracking_marts_driver.py:23,29`). The
  extraction moves the CALLING code (`_read_unit` + empty-guard + the `build_unit_inputs(...)` call +
  `FrameBundle` construction), not those definitions. Signature takes a `WorkUnit`; `resolve_unit_meta` stays
  a separate public helper (gkdv needs it). Re-point `iter_unit_inputs` to loop `discover_tracking_units`
  and delegate to the new fn (keep it working, or mark for deletion in Task 12).
- [ ] **Step 4: Run** — PASS.

### Task 4: `TrackingMartsProcessor.process(unit)`

**Files:**
- Modify: `src/ingestion/tracking_marts_processor.py`
- Test: `src/tests/test_tracking_marts_processor.py` (new)

**Interfaces:**
- Consumes: `read_and_build_unit_inputs` (T3), the pure scorers (`compute_off_ball_runs`,
  `compute_action_defensive_credit`, `compute_defensive_credit_long`, `attach_xg`, `_read_xg_preds`,
  `score_gkdv_unit`), `resolve_unit_meta`, `write_delta_table`.
- Produces: `class TrackingMartsProcessor: def __init__(self, spark, catalog, schema); def process(self, unit: WorkUnit) -> int`.

- [ ] **Step 1: Failing test** — a fake-input `TrackingMartsProcessor` (inject a stub
  `read_and_build_unit_inputs` + a capturing `write_delta_table`) asserts `process(unit)` (a) calls all
  three scorers on the same `(actions, frames, xt)`, (b) writes to `off_ball_runs`,
  `action_defensive_credit`, `defensive_credit_attributions`, `gkdv_observations` with per-unit
  `replaceWhere = data_source AND match_id AND period_id`, (c) returns the summed row count, and (d) a
  per-scorer exception is attributed and re-raised as a unit-level failure (so the unit rolls forward).
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement.** `process(unit)`:
```python
def process(self, unit):
    inputs = read_and_build_unit_inputs(self._spark, self._catalog, unit)
    if inputs is None:
        return 0
    where = (f"data_source = '{unit.provider}' AND match_id = '{unit.match_id}' "
             f"AND period_id = {int(unit.period or 0)}")
    total = 0
    errors = []
    # off_ball_runs
    try:
        obr = compute_off_ball_runs(inputs.actions, inputs.frames, inputs.xt)
        total += self._write(obr, off_ball_schema, OFF_BALL_TABLE, where)
    except Exception as exc:  # noqa: BLE001 — attributed + re-raised below
        errors.append(f"off_ball_runs: {exc}")
    # defensive_credit (needs per-unit xG)
    try:
        xg_preds = _read_xg_preds(self._spark, self._catalog, unit.provider, unit.match_id)
        actions = attach_xg(inputs.actions, xg_preds)
        total += self._write(compute_action_defensive_credit(actions, inputs.frames, inputs.xt), agg_schema, AGG_TABLE, where)
        total += self._write(compute_defensive_credit_long(actions, inputs.frames, inputs.xt), long_schema, LONG_TABLE, where)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"defensive_credit: {exc}")
    # gkdv scoring -> observations (pooled in a separate task)
    try:
        meta = resolve_unit_meta(self._spark, self._catalog, unit.provider, unit.match_id)
        obs = score_gkdv_unit(inputs.frames, meta.home_team_id, inputs.xt)  # stamps ids
        total += self._write(obs, gkdv_obs_schema, GKDV_OBS_TABLE, where)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"gkdv: {exc}")
    if errors:
        raise RuntimeError(f"tracking-marts unit {unit.provider}:{unit.match_id}:{unit.period} failed: " + "; ".join(errors))
    return total
```
  (`_write` wraps `spark.createDataFrame(pdf, schema) + write_delta_table(..., replace_where=where, row_count=len(pdf))`.)
  **G3:** the `where` predicate is byte-for-byte the one the existing writers already use
  (`off_ball_runs_writer.py:182`, `defensive_credit_writer.py:274`) — `provider`/`match_id` come from
  discovered bronze units (trusted, not user input), and `period_id` is verified 1-based (values 1–5, zero
  rows at 0), so `int(unit.period or 0)` uses `0` as the match-grain (`None`) sentinel without conflating a
  real period. Preserve it as-is (do not add validation that the existing writers lack — no new risk).
- [ ] **Step 4: Run** — PASS.

### Task 5: Preflight entry point

**Files:**
- Modify: `src/ingestion/tracking_marts_drain.py`
- Test: `src/tests/test_tracking_marts_preflight.py` (new)

**Interfaces:**
- Produces: `main_tracking_marts_preflight()`; `_N_TRACKING_MARTS_WORKERS = 8`;
  `discover_open_units(spark, catalog, *, full: bool) -> list[WorkUnit]`.
- **G2 — the skip-guard is CROSS-RUN, `succeeded`-only.** "Done" = a unit has a **`succeeded`** terminal in
  `tracking_marts_unit_events` under **ANY** `run_id` (a `failed`/`timed_out` unit, or one that never ran, is
  OPEN). The events read here is deliberately NOT filtered to the current `run_id` (opposite of the gate,
  which is per-run) — a per-run read would leave every unit always-open and the chronic timeout would go
  unfixed silently. `full=True` returns the whole universe.

- [ ] **Step 1: Failing, DISCRIMINATING test** — factor the pure `open_units(universe, done_keys, *, full)`
  and its evidence reader. Assert:
  - a unit with a `succeeded` terminal under a **DIFFERENT** `run_id` is still skipped (cross-run — the
    load-bearing case; a per-run bug passes a naive "unit A done ⇒ skip {B,C}" test but fails THIS one);
  - a unit whose only terminals are `failed` / `timed_out` is **OPEN** (re-enumerated);
  - `full=True` returns the whole universe regardless of terminals.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement.** `discover_open_units` reads `SELECT DISTINCT provider, match_id, period FROM
  {catalog}.observability.tracking_marts_unit_events WHERE state = 'succeeded'` (NO `run_id` filter) as the
  done-set, subtracts it from `discover_tracking_units(...)`; `full` bypasses the subtraction. Then mirror
  `action_context.main_preflight` exactly:
  argparse (`--catalog --schema --run-id --full`); `bootstrap_hooks`;
  `DeltaUnitEventSink(spark, catalog, logger, drain_name="tracking_marts").ensure_tables()` BEFORE the
  early-return; compute `units = discover_open_units(...)`; if empty → set empty task values + return;
  `assign_workers(units, _N_TRACKING_MARTS_WORKERS)`; `queue = DeltaWorkQueue(spark, catalog, drain_name="tracking_marts")`;
  `ensure_table` + `prune` + `enqueue(run_id, assignments)` + round-trip `count_for_run` check;
  `_set_task_value("tracking_marts_run_id", run_id)` + `_set_task_value("tracking_marts_worker_ids", [str(i) for i in range(N)])`.
  Import `_set_task_value`/`_resolve_run_id`/`assign_workers` (the last from `analytics.action_context.drain`).
- [ ] **Step 4: Run** — PASS.

### Task 6: Drain worker entry point

**Files:**
- Modify: `src/ingestion/tracking_marts_drain.py`
- Test: `src/tests/test_tracking_marts_drain_worker.py` (new)

- [ ] **Step 1: Failing test** — with fakes (a `WorkQueuePort` returning 2 units, a `GameProcessorPort`
  where unit #2 raises), `main`'s core (factor a `_run_worker(spark, catalog, schema, worker_id, run_id, budget_s)`)
  drains both, emits events, and `raise_on_failed_units` raises (worker task fails). Assert the sink saw
  `unit_started`×2, a `failed` terminal, and `slice_completed`.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** `main_tracking_marts_drain_worker`, mirroring `main_drain_worker`:
  argparse (`--worker-id --run-id --watchdog-budget-s`); empty-run_id clean exit; build
  `DeltaWorkQueue(...,drain_name="tracking_marts")`, `DeltaUnitEventSink(...,drain_name="tracking_marts")`,
  `SparkInterruptWatchdog(spark)`, `TrackingMartsProcessor(spark, catalog, schema)`;
  `units = queue.units_for_worker(run_id, worker_id)`; empty → `sink.slice_completed` + return;
  `summary = drain_worker(queue, processor, watchdog, run_id, worker_id, logger, sink=sink, units=units, budget_s=budget_s)`;
  `raise_on_failed_units(summary, run_id=run_id)` (import from `ingestion.action_context`).
- [ ] **Step 4: Run** — PASS.

### Task 7: gkdv pooling entry point

**Files:**
- Modify: `src/ingestion/tracking_marts_drain.py`
- Test: `src/tests/test_gkdv_pool_entry.py` (new)

- [ ] **Step 1: Failing test** — `_pool_gkdv(spark, catalog)` reads `bronze.gkdv_observations` (stub),
  calls `pool_gkdv_observations`, writes `gkdv_keeper_pooled` per-provider `replaceWhere`; assert the write
  targets + row count.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** `main_gkdv_pool`: read all `bronze.gkdv_observations` → `.toPandas()` (bounded:
  it's a keeper-frame table, add a `_topandas_exemptions.yml` entry with the rationale + rerun the boundedness
  test), `pool_gkdv_observations`, per-provider `replaceWhere` write to `gkdv_keeper_pooled`. (No fan-out.)
- [ ] **Step 4: Run** — PASS + `uv run pytest src/tests/test_topandas_boundedness.py`.

### Task 8: Completeness gate entry point

**Files:**
- Create: `src/ingestion/tracking_marts_gate.py`
- Test: `src/tests/test_tracking_marts_gate.py` (new)

- [ ] **Step 1: Failing test** — feed the pure `drain_gate.evaluate` (reused) queue+events fixtures via the
  new adapter readers; assert COMPLETE when every enqueued unit has a terminal event + `slice_completed`, and
  the enforced failure when a unit is missing.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** (depends on **Task 1B**), mirroring `action_context_gate.main`: `_resolve_run_id`
  (require `{{job.run_id}}`), `_read_queue`/`_read_events` over `tracking_marts_work_queue` /
  `tracking_marts_unit_events` (the sb360-free UNION view built with `include_sb360=False`), the result
  cross-check summing rows across **all four** of the unit's output tables (**N1**: off_ball_runs +
  `action_defensive_credit` + defensive_credit_attributions + gkdv_observations — do NOT omit the AGG table,
  or a dead worker's agg-only unit is misclassified `in_flight` instead of `completed_terminal_lost` in the
  V6 reconstruction), then `report = evaluate(..., extra_expected_workers=frozenset())` **(G1 — without the
  empty set the gate reports DRAIN_FAILED every run)**, `_log_report`, `enforce(report)`. Reuse
  `analytics.action_context.drain_gate` (`evaluate`, `enforce`, `expected_units`, `PlannerInputs`, `QueueRow`,
  `UnitEvent`) with only the Task-1B `extra_expected_workers` parameter — no other change.
- [ ] **Step 4: Run** — PASS.

### Task 9: Entry points + wheel bump + env pins

**Files:** Modify `pyproject.toml`; run bump tooling.

- [ ] **Step 1:** In `[project.scripts]` remove `off_ball_runs_writer`/`defensive_credit_writer`/`gkdv_writer`;
  add `preflight_tracking_marts = "ingestion.tracking_marts_drain:main_tracking_marts_preflight"`,
  `compute_tracking_marts_drain_worker = "ingestion.tracking_marts_drain:main_tracking_marts_drain_worker"`,
  `compute_gkdv_pool = "ingestion.tracking_marts_drain:main_gkdv_pool"`,
  `verify_tracking_marts_drain = "ingestion.tracking_marts_gate:main"`.
- [ ] **Step 2:** Bump version in `pyproject.toml`; `uv lock`; `uv run python scripts/sync_tf_env_pins.py`;
  `uv run python scripts/bump_wheel.py`; `uv pip install -e . --no-deps` (refresh installed metadata).
- [ ] **Step 3:** `uv run python scripts/bump_wheel.py --check` + `uv run python scripts/sync_tf_env_pins.py --check`
  + `uv run pytest src/tests/test_wheel_constants.py -v` — all clean.

### Task 10: Terraform

**Files:** Modify `terraform/modules/workflows/main.tf`; Test: `src/tests/test_workflows_tf_*.py`,
`src/tests/test_card_parity_with_terraform.py`.

- [ ] **Step 1: Failing conformance test** — add a `_N_TRACKING_MARTS_WORKERS`↔`concurrency`↔event-worker
  parity test (mirror `test_terraform_concurrency_matches_n_workers`) + a test asserting the 3 old writer
  task_keys are gone and the 4 new task_keys exist with the right deps.
- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement HCL** (mirror the AC blocks verbatim): `preflight_tracking_marts` (600 s, deps =
  `compute_action_context` + `compute_action_context_statsbomb` + `compute_xg_shot_scores`, alphabetical;
  params incl. `--run-id {{job.run_id}}` `--full {{job.parameters.tracking_marts_full}}`);
  `compute_tracking_marts` `for_each_task` (`inputs = {{tasks.preflight_tracking_marts.values.tracking_marts_worker_ids}}`,
  `concurrency = 8`, inner `compute_tracking_marts_drain_worker`, 28800 s, `environment_key = "analytics"`,
  `--run-id {{tasks.preflight_tracking_marts.values.tracking_marts_run_id}}`);
  `verify_tracking_marts_drain` (3600 s, `run_if = ALL_DONE`, dep = `compute_tracking_marts`,
  `--run-id {{job.run_id}}`); `compute_gkdv_pool` (dep = `verify_tracking_marts_drain`, `environment_key = "analytics"`).
  Remove the 3 old writer tasks. Rewire `dbt_build_output_marts.depends_on`: drop the 3 writer keys, add
  `compute_gkdv_pool` + `verify_tracking_marts_drain` (keep `ALL_DONE`). Add the `tracking_marts_full`
  job parameter (default `""`).
- [ ] **Step 4: Run** conformance tests + `terraform -chdir=terraform/environments/dev validate` (or the repo's
  validate wrapper) — PASS.

### Task 11: Workflow card

**Files:** Create `workflow-cards/wf-tracking-marts.yaml`; delete the 3 old writer cards; modify the parity map.

- [ ] **Step 1:** Write `wf-tracking-marts.yaml` with `execution` phases `drain` (entry_point
  `compute_tracking_marts`, module `ingestion.tracking_marts_drain`, timeout `28800s`, environment
  `analytics`, distribution `worker-drain`), `verification` (entry_point `verify_tracking_marts_drain`,
  module `ingestion.tracking_marts_gate`, `3600s`, `analytics`), `pooling` (entry_point `compute_gkdv_pool`,
  module `ingestion.tracking_marts_drain`, `analytics`). Inputs: `spadl_action_context`, the 4 tracking
  bronze tables, `xg_shot_predictions`, `expected_threat_grids`. Outputs: `off_ball_runs`,
  `action_defensive_credit`, `defensive_credit_attributions`, `gkdv_observations`, `gkdv_keeper_pooled`.
  Set `distribution` on every phase (the model requires it). `dbt_model` — one of the four output marts (or
  omit if the parity test allows a multi-mart card; verify against `test_card_parity_with_terraform`).
- [ ] **Step 2:** In `test_card_parity_with_terraform.py` `_DIRECT_TASK_ENTRY_POINT_TO_CARD`: add
  `preflight_tracking_marts: None`, `compute_tracking_marts: "wf-tracking-marts"`,
  `verify_tracking_marts_drain: "wf-tracking-marts"`, `compute_gkdv_pool: "wf-tracking-marts"`; remove the 3
  old writer entries. Delete the 3 old writer card files.
- [ ] **Step 3: Run** `uv run pytest src/tests/test_card_parity_with_terraform.py -v` + the card-schema test — PASS.

### Task 12: Remove obsolete writer driver loops

**Files:** Modify `src/ingestion/{off_ball_runs,defensive_credit,gkdv}_writer.py`; modify
`src/ingestion/tracking_marts_driver.py`; update their tests.

- [ ] **Step 1:** Delete each writer's `run_pipeline` (driver loop) + `main()`. Keep the pure scoring
  functions (`compute_off_ball_runs`, `compute_action_defensive_credit`, `compute_defensive_credit_long`,
  `score_gkdv_unit`, `pool_gkdv_observations`), the schema constants, and `_assert_silly_kicks_min`.
  If nothing else uses `iter_unit_inputs`, delete it (keep `read_and_build_unit_inputs` +
  `discover_tracking_units` + `resolve_unit_meta` + `ac_xt_grid`).
- [ ] **Step 2:** Update/remove tests that invoked the deleted `main`/`run_pipeline`; keep the pure-scoring
  tests. Grep `run_pipeline`, `off_ball_runs_writer`, `iter_unit_inputs` across `src/tests/` and fix.
- [ ] **Step 3: Run** the full suite for these modules — PASS.

### Task 13: ADR

**Files:** Create `docs/superpowers/adrs/ADR-0XX-tracking-marts-drain-fanout.md` (next number; check the dir).

- [ ] **Step 1:** Nygard-format ADR: context (the 3 writers were driver-sequential, no incremental gating,
  chronically timed out, shipped empty); decision (consolidated tracking-marts drain reusing the ADR-037/068
  fan-out via `drain_name`-generalized adapters + the `include_sb360`/`extra_expected_workers` worker-topology
  axis; cross-run events-based skip-guard; gkdv scoring/pooling split as an invariant); consequences (N×
  throughput; the `action_context_queue` module is now a shared drain adapter — rename deferred; gkdv pooling
  is a mandatory separate reduce; the `dbt_build_output_marts` deps changed). **N3 — DURABLE OPERATOR
  FOOT-GUN (must be a prominent Consequence, not a footnote):** because "done" is a cross-run `succeeded`
  unit-event (NOT output-`left_anti`), **truncating or dropping any of the four output bronze tables leaves
  its units marked done — daily incremental runs will SKIP them and the table stays empty until a `--full`
  run.** Any operation that clears one of these tables (a mart rebuild, a schema migration, a backfill) MUST
  be followed by `preflight_tracking_marts --full`. This is correct design (it's exactly why events beat
  output-`left_anti` for a 0-row-legitimate multi-output drain), but silent if forgotten.
  Cross-link ADR-037, ADR-045, ADR-067, ADR-068.
- [ ] **Step 2:** Add the ADR to any ADR-index test if one exists (grep `ADR-` in `src/tests/`).

---

## Global validation (run before proposing the PR — CLAUDE.md 7 gates)

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run lint-imports
uv run python scripts/bump_wheel.py --check
uv run python scripts/pip_audit_ignores.py --check
uv run pyright src/ hf_taipy_app/src/ scripts/_tf_env_pins.py scripts/sync_tf_env_pins.py
uv run pytest src/tests/ -v
```

## Post-merge operator sequence (NOT code — the held recovery tail)

1. Post-merge CI publishes the wheel + `terraform apply` wires the new tasks.
2. Apply the `gkdv_observations` migration (`scripts/migrations/_runner.py`).
3. Operator `--full` run: `run_now(only=[preflight_tracking_marts, compute_tracking_marts,
   verify_tracking_marts_drain, compute_gkdv_pool])` with `tracking_marts_full="1"`. Verify per-provider
   coverage + gate COMPLETE + gkdv pooled.
4. **The held Phase-2 DAG rebuild** (per the recovery): full-refresh the 4 writer marts (overwrites the
   partial GS-only bronze) + the 12 feature marts; strand-safe rederive the 6 in-scope TRIGGERED marts
   (`fct_passes`, `fct_player_embeddings`, `fct_defensive_values`, `fct_defcon_actions`,
   `fct_defcon_pressure`, `fct_pausa_values`). Then HF republish → synced refresh → verify →
   `normalize_training_model_ownership.py` → delete `scratchpad/sp_m2m.env`.

## Self-review notes

- **Spec coverage:** every §3 component maps to a task (adapters→T1, sb360-axis→T1B, gkdv split→T2/T7,
  inputs→T3, processor→T4, preflight→T5, worker→T6, gate→T8, TF→T10, cards→T11, cleanup→T12, ADR→T13). ✔
- **Type consistency:** `process(unit)->int`, `read_and_build_unit_inputs(...)->UnitInputs|None`,
  `pool_gkdv_observations(...)->pd.DataFrame` used consistently across tasks. ✔
- **Parallel-critic review (2026-08-25) incorporated:**
  - **G1 (showstopper) — FIXED:** sb360 is a worker-topology axis welded into the pure gate + event-table
    helpers; `drain_name` alone can't reach it. Added Task 1B (`evaluate(extra_expected_workers=…)`) +
    `include_sb360` in Task 1; Global Constraint relaxed from "reuse `drain_gate` unchanged" to "reuse with
    sb360 parameterized, AC byte-identical at default." Task 1's test no longer asserts a phantom `_sb360`
    table for tracking-marts.
  - **G2 — FIXED:** the skip-guard is now stated CROSS-RUN + `succeeded`-only (Task 5), with a discriminating
    test (a terminal under a different `run_id` must still skip; a `failed`/`timed_out`-only unit stays open).
    Spec §3.1 documents the cross-run-vs-per-run distinction + the `--full`-after-output-drop caveat.
  - **G3 — verified benign:** the `replaceWhere` predicate is the existing writers' exact one; periods are
    1-based (no `0`), values trusted. Noted in Task 4; no change.
  - **G4 — FIXED:** spec §3.3 realigned to the real wrapper names (`compute_off_ball_runs` /
    `compute_action_defensive_credit` / `compute_defensive_credit_long` / `score_gkdv_unit`).
  - **G5 — FIXED:** Task 3 clarified — `WorkUnit`/`UnitInputs`/`build_unit_inputs` are imported from
    `analytics.action_context`, not local defs; the extraction is the loop body.
- **Open verify-at-implementation items** (flagged inline): the card `distribution` requirement, whether
  `dbt_model` may be multi-mart on one card, and the exact `_read_xg_preds`/`attach_xg` import surface after
  Task 12's deletions.
