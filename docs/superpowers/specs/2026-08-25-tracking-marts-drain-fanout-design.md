# Tracking-Marts Drain Fan-Out — Design

> **Status:** DRAFT for review (2026-08-25). Supersedes the driver-sequential `tracking_marts_driver`
> loop for the three Rev-6 tracking-grain writers. Warrants an ADR (new drain subsystem + cross-cutting
> reuse of the ADR-037/068 fan-out framework).

**Goal:** Make `off_ball_runs`, `defensive_credit`, and `gkdv` production-grade: complete a full run
within the task budget, run incrementally on the daily job, and prove completeness — by moving them off
the single-threaded driver loop onto the proven ADR-037 worker-drain fan-out (ADR-068 unit-events + gate).

**Architecture:** One new **consolidated tracking-marts drain** that reuses `analytics.action_context.drain`
(the pure ports-and-adapters core, verbatim) via new Spark adapters. A preflight enqueues the incremental
unit set and LPT-assigns it across N workers; a `for_each_task` fans out N concurrent 16 GB driver-mode
workers, each of which — per `(provider, match_id, period)` unit — builds the shared oriented inputs
**once** and runs all three scorers, writing their bronze tables with per-unit `replaceWhere`. gkdv's
cross-corpus pooling stays a separate single-driver reduce task. A fan-in gate verifies every enumerated
unit ran and landed.

**Tech Stack:** PySpark (serverless, 16 GB driver, 1 GB UDF cap — driver-mode here, no UDF), Delta,
silly-kicks tracking/gkdv, the `analytics.action_context.drain` pure core, Terraform `for_each_task`.

## Global Constraints

- **Reuse `analytics.action_context.drain` UNCHANGED** — the pure core (`assign_workers`, `drain_worker`,
  the four ports, `DrainSummary`, watchdog/abandon-ceiling/flush policy) is the hard-earned knowledge; do
  not fork or thin it. The writers supply adapters only.
- **Reuse `.drain_gate` with the sb360 worker-topology axis PARAMETERIZED (not "unchanged").** sb360 is
  welded into the pure gate as an unconditional expected worker (`drain_gate.py:401`
  `expected_workers = {…queue…} | {SB360_WORKER_ID}`; `dead = [w … if w not in slices]:404`) and into
  `event_table_names` as an unconditional extra table (`action_context_queue.py:145`). It is a
  worker-topology axis (does this drain have an unconditional match-grain extra worker?), orthogonal to
  `drain_name`. tracking-marts is the FIRST drain with **no** sb360 task, so without parameterization its
  gate reports `DRAIN_FAILED` every run (worker `-1` never emits `slice_completed`) and it creates a phantom
  `tracking_marts_unit_events_sb360` table. Fix: add `extra_expected_workers: frozenset[int] =
  frozenset({SB360_WORKER_ID})` to `evaluate` and `include_sb360: bool = True` to the event-table helpers
  (`event_table_for_worker`/`event_table_names`/`event_view_sql`/`DeltaUnitEventSink.ensure_tables`). **AC
  passes the defaults → byte-identical behaviour, proven by its existing tests.** tracking-marts passes
  `frozenset()` / `include_sb360=False`. Do NOT fork the gate — a bespoke tracking-marts gate discards the
  hard-won four-verdict logic (rules 0–4, V1/V6/W1). This is a deliberate, minimal generalization of the
  pure core, not a rewrite.
- **ADR-068 unit-events are mandatory, not optional** — a `unit_started` (fail-open, pre-process,
  OOM-visibility), buffered `unit_finished`, periodic `flush_terminals` (fail-open), and `slice_completed`
  (fail-loud, carries `write_failures`). No lighter substitute: an output-presence check cannot tell
  "ran, legitimately 0 rows" from "never ran" (the ADR-068 period-grain amendment; the "half-match shipped
  as SUCCESS" incident).
- **`raise_on_failed_units` (ADR-067) after the drain** — a per-unit swallow must still fail the task.
- **`layer schema` constants + `DEFAULT_BRONZE_SCHEMA`** (ADR-073); **exact serverless env pins** (ADR-046);
  **HF/leak seams untouched** (these are bronze writers, no HF publish here).
- **silly-kicks per-unit functions unchanged** — this is an orchestration refactor; the scoring math
  (`detect_off_ball_runs`/`value_off_ball_runs`, `compute_defensive_credits`, `build_ghost_frames`/
  `delta_das`/`delta_threat_suppression`, `pool_keepers`) is called exactly as today, just per-unit inside
  a processor instead of a driver loop.

---

## 1. Problem

The three writers (`src/ingestion/{off_ball_runs,defensive_credit,gkdv}_writer.py`) each loop
`tracking_marts_driver.iter_unit_inputs(...)` **sequentially on the driver**, scoring + writing one
`(provider, match_id, period)` unit at a time. Consequences, all confirmed live (2026-08-25):

1. **No executor parallelism.** ~1.5 min/unit measured isolated → a full run over ~180–500 matches is
   ~4.7 h (off_ball_runs) / ~13–15 h (defensive_credit) / OOM-risk (gkdv). All three exceed their
   90–120 min task timeouts and **have never completed a full run** — which is why their bronze was empty.
2. **No incremental gating.** `iter_unit_inputs` processes *every* AC-materialised unit every run
   (`discover_tracking_units` = `spadl_action_context` distinct units, no skip-guard), so the timeout is
   chronic, not first-run-only.
3. **gkdv accumulates the whole corpus in driver memory** (`all_obs.append(...)` then one `pd.concat`) —
   a 16 GB OOM risk on top of the timeout; it never emitted a single unit in 120 min.

The recovery's 12 feature computes + `bravery` + `psxg_tracking` succeeded; **only these three writers are
broken.** The DAG rebuild waits on this fix (user decision, 2026-08-25).

---

## 2. Architecture decision

### Chosen: one consolidated tracking-marts drain, reusing `drain.py`

The three writers already share `tracking_marts_driver`, whose docstring states its purpose is *"so the
three writers do not each re-derive"* the oriented `(actions, frames, xt)`. The current per-writer `main()`
loops defeat that (3× re-derivation). A **single drain** whose processor builds the inputs once per unit
and runs all three scorers realises that original intent (compute-once), stays fully isolated from the AC
drain, and mirrors the AC drain's single-drain shape.

### Rejected alternatives (with reasons)

- **Fold scoring into the AC drain (`_process_tracking_match`).** Rejected: the writers' cost is dominated
  by *scoring* (defensive_credit's Ward-clustering `precompute_line_break_between_lines`; gkdv's doubled
  DAS on actual+ghost frames), not input-building — so folding saves only the cheap orientation pass while
  it (a) breaks the AC drain's ADR-068 event schema (one `rows_written`/unit → four tables), (b) forces an
  `xg_shot_predictions` dependency the AC drain's DAG position lacks, and (c) adds independently-failing
  sub-steps to a load-bearing, heavily-hardened module (one sub-writer's OOM could poison
  `fct_action_context` for that unit). Bad risk/reward.
- **Per-unit `applyInPandas` (no worker fan-out).** Rejected: a whole unit ≈ a whole AC unit, which already
  cannot fit the 1 GB serverless UDF group cap (the AC drain sub-batches to 250 frames; 2500 OOMed 13/16
  units, `batching.py`), and these functions are **not** frame-batchable (`compute_defensive_credits` needs
  one `link_actions_to_frames` + one Ward pass over the whole unit; `build_ghost_frames`/`infer_ball_carrier`
  need frame continuity). Would OOM on dense IDSSE/GS (25 fps) units. Closure-capture of meta/xT is solvable;
  the group-size is not.
- **Three separate drains (one per writer).** Viable and maximally mirrors the AC drain's single-output
  shape, but costs ~10 new mega-job tasks and re-derives the shared inputs 3× (against
  `tracking_marts_driver`'s own compute-once intent). Consolidation is preferred; **this is the one
  decision to confirm at review** (see §12 Open Questions).

---

## 3. Components

### 3.1 Preflight — `compute_tracking_marts_preflight`

- Discover the unit universe: `discover_tracking_units(spark, catalog)` (unchanged — distinct
  `(data_source, match_id, period_id)` over `bronze.spadl_action_context`).
- **Incremental gating (events-based, deliberately):** a unit is "done" iff it has a terminal lifecycle
  record in `observability.tracking_marts_unit_events`, and the preflight enumerates
  `universe ∖ done`. `--full` forces the whole universe (recompute path — this run).
  - **Why events, not the AC drain's output-based `left_anti`.** The AC drain skips a unit already present
    in its single output (`spadl_action_context`), which is safe *only* because its period-grain invariant
    guarantees every enumerated unit writes ≥1 row (the exact invariant whose earlier absence caused the
    ADR-068 match-grain bug — a 0-action period enumerated forever because it could never write a row). A
    multi-output drain has no such single-table invariant (a period may legitimately yield 0
    `defensive_credit` rows while yielding off-ball runs), so output-presence cannot mean "done." Using the
    unit-events terminal record — the ADR-068 source of truth for *ran vs never-ran* — is the principled
    application of that same hard-earned lesson to the skip-guard, and is robust to legitimate 0-row units.
  - **CROSS-RUN vs PER-RUN — the load-bearing distinction.** "Done" = a unit has a **`succeeded`** terminal
    in the unit-events table under **ANY** `run_id` (a `failed`/`timed_out` unit, or one that never ran, is
    OPEN and re-enumerated). The preflight reads terminals **across all runs**. This is deliberately the
    OPPOSITE of the gate, whose `evaluate` docstring requires events "already filtered to `run_id`" (a
    per-run read — a prior terminal must not be misattributed to a freshly-enqueued unit). The two are
    consistent: the cross-run skip-guard decides what THIS run enqueues; the per-run gate verifies THIS
    run's queue all ran. **If the skip-guard were accidentally per-`run_id`, every unit would always read
    OPEN → nothing ever skips → the chronic timeout this whole design exists to kill is NOT fixed, silently
    (CI stays green).** A discriminating test (a terminal under a DIFFERENT `run_id` must still skip the
    unit) is mandatory. **Self-healing caveat:** because "done" is event-derived (not output-`left_anti`
    like AC), a unit whose bronze output is later dropped (e.g. a coordinated mart rebuild) stays "done" —
    so an output drop REQUIRES a subsequent `--full` re-run. Output drops are operator-coordinated, so this
    is acceptable; it is documented in the ADR + the workflow card.
- `assign_workers(units, N)` (verbatim) → persist `WorkAssignment` rows to a new
  `observability.tracking_marts_work_queue` (same schema as `action_context_work_queue`).
- **`ensure_tables()` before any early-return** (ADR-068 lesson): create the per-worker event tables + the
  UNION view here (single writer), so the gate can read them even on a nothing-to-do run.
- Emit constant-size task value `worker_ids = [str(i) for i in range(N)]` and a `run_id = {{job.run_id}}`
  (ADR-068: never the preflight task value on a quiet-day consumer).

### 3.2 Drain worker — `compute_tracking_marts_drain_worker` (`for_each_task`, concurrency = N)

- CLI: `--worker-id {{input}} --run-id {{tasks.…preflight….run_id}} --watchdog-budget-s … --full`.
- Build the adapters (§3.5) and call the **verbatim** `drain_worker(queue, processor, watchdog, run_id,
  worker_id, logger, sink=sink)`. Then `raise_on_failed_units(summary, run_id=run_id)` (ADR-067).
- `timeout_seconds = 28800` (8 h — the slice drains to completion; watchdog is the per-unit budget).
- Environment: `analytics` (the writers already run there; silly-kicks tracking/gkdv present).

### 3.3 The processor — `TrackingMartsProcessor.process(unit) -> int`

Per unit, once:
1. Build shared inputs: `trk, actions, meta = _read_unit(...)`; `inputs = build_unit_inputs(...)`
   (reuse `tracking_marts_driver` internals, factored to a single-unit call — no behaviour change).
2. Run the three scorers on the shared `(actions, frames, xt)`, each in **its own try/except so a
   per-scorer failure is attributed but does not silently drop the others** — and re-raise a combined
   error (unit-level failure) so the unit rolls forward (idempotent `replaceWhere` re-runs are safe):
   - **off_ball_runs:** `compute_off_ball_runs(actions, frames, xt)` (the writer's pure wrapper over
     silly-kicks `detect_off_ball_runs`/`value_off_ball_runs`) → `replaceWhere` `bronze.off_ball_runs`.
   - **defensive_credit:** attach per-shot `xg` (`_read_xg_preds` on `bronze.xg_shot_predictions` for the
     unit + `attach_xg`) → `compute_action_defensive_credit(actions, frames, xt)` +
     `compute_defensive_credit_long(actions, frames, xt)` → `replaceWhere` `bronze.action_defensive_credit`
     **and** `bronze.defensive_credit_attributions`.
   - **gkdv (scoring only):** `score_gkdv_unit(frames, home_team_id, xt)` (the writer's per-unit wrapper
     over silly-kicks `build_ghost_frames` + the delta-DAS/threat arms) → per-unit **observations** →
     `replaceWhere` new intermediate `bronze.gkdv_observations` (keyed `data_source, match_id, period_id`).
     **No pooling here** (§3.4). (These are the writers' existing pure functions — the scoring math is
     unchanged; only the driver loop is replaced.)
3. Return the total rows written across the unit's tables (the ADR-068 `rows_written` for the event).

### 3.4 gkdv pooling — `compute_gkdv_pool` (single-driver, runs after the drain gate)

`pool_keepers`/`aggregate_by_keeper` enforce a cross-match `min_games=2` floor per keeper ×
`(data_source, competition_id, season_id)` — an **irreducible whole-corpus reduce** (a single unit cannot
produce a valid pooled row). So a small single-driver task reads all `bronze.gkdv_observations`, runs
`pool_keepers` per provider, and `replaceWhere`-writes `bronze.gkdv_keeper_pooled` (the existing output).
Cheap (aggregation over a keeper-frame table, not tracking frames); no fan-out needed. Depends on the gate.

### 3.5 Adapters (new; generalise `ingestion.action_context_queue` by `drain_name`)

The AC drain's Spark adapters are AC-specific only in their **table names**. Parameterise them by a
`drain_name` (`"action_context"` unchanged; `"tracking_marts"` new) — a backward-compatible extraction, not
a rewrite:
- **`DeltaWorkQueue(drain_name)`** → `WorkQueuePort` over `observability.{drain_name}_work_queue`.
- **`DeltaUnitEventSink(drain_name, worker_id)`** → `UnitEventSink` over
  `observability.{drain_name}_unit_events_w{worker_id}` + the UNION view (reuse `event_table_for_worker`,
  the fail-open/fail-loud policy, `write_failures`).
- **`SparkInterruptWatchdog`** → `WatchdogPort` (reuse verbatim — it is already writer-agnostic).
- **`TrackingMartsProcessor`** → `GameProcessorPort` (§3.3, new).

### 3.6 Completeness gate — `verify_tracking_marts_drain` (`run_if = ALL_DONE`)

Reuse the pure `analytics.action_context.drain_gate` (`evaluate`/`enforce`/`expected_units`/`PlannerInputs`/
`QueueRow`/`UnitEvent`) against `observability.tracking_marts_unit_events` (UNION view, sb360-free) +
`tracking_marts_work_queue`, calling `evaluate(..., extra_expected_workers=frozenset())` (the sb360-axis
generalization — without it the gate reports `DRAIN_FAILED` every run; see Global Constraints). Assert every
enumerated unit has a terminal event and every real worker emitted `slice_completed`; surface
`write_failures` → `UNVERIFIABLE` (non-empty-anomaly rule, ADR-068). The result-mart cross-check
(`result_counts`) sums rows across the unit's output tables (`off_ball_runs` +
`defensive_credit_attributions` + `gkdv_observations`); a drain unit that legitimately wrote 0 rows is
`succeeded_with_zero_rows` (reported, never accused — the same rule the pure gate already applies to drain
units). Fail the build on any `INCOMPLETE` (clean-worker missing terminal) or planner alarm.

---

## 4. Data flow

```
spadl_action_context (unit universe)
      │  preflight: discover ∖ done(events)  →  assign_workers(N)  →  tracking_marts_work_queue
      ▼
for_each worker w ∈ 0..N-1  (8h, driver-mode 16GB)
      │  per unit: build_unit_inputs ONCE
      │    ├─ off_ball_runs        → replaceWhere bronze.off_ball_runs
      │    ├─ defensive_credit     → replaceWhere bronze.action_defensive_credit + defensive_credit_attributions
      │    └─ gkdv scoring         → replaceWhere bronze.gkdv_observations           (intermediate)
      │  emit unit_started/finished (ADR-068);  raise_on_failed_units at slice end
      ▼
verify_tracking_marts_drain (ALL_DONE)  →  completeness proven
      ▼
compute_gkdv_pool (single driver)  →  pool_keepers  →  replaceWhere bronze.gkdv_keeper_pooled
      ▼
(post-merge) DAG rebuild → fct_off_ball_runs, fct_defensive_credit_attributions, fct_action_defensive,
                           fct_gk_shot_stopping_pooled  (+ full-refresh to overwrite partial GS-only bronze)
```

## 5. Terraform / mega-job changes

- **Remove** the three single tasks `off_ball_runs_writer`, `defensive_credit_writer`, `gkdv_writer`.
- **Add** `compute_tracking_marts_preflight` → `compute_tracking_marts_drain_worker` (`for_each_task`,
  `concurrency = N`, mirror the `compute_action_context_iteration` block) → `verify_tracking_marts_drain`
  (`ALL_DONE`) → `compute_gkdv_pool`.
- Rewire `dbt_build_output_marts.depends_on`: drop the three old writer keys, add
  `verify_tracking_marts_drain` + `compute_gkdv_pool` (keep `ALL_DONE`).
- New entry points in `pyproject.toml`; new workflow cards (phase-parity, correct `environment`); env pins
  unchanged (same `analytics` env). `bravery_writer` is untouched (it already succeeds).

## 6. Error handling

- Per-unit isolation + `raise_on_failed_units` (ADR-067); watchdog per unit (`WATCHDOG_BUDGET_S`, gkdv units
  may warrant a higher `--watchdog-budget-s` given doubled DAS — measure in validation).
- Concurrent `replaceWhere` on disjoint partitions is idempotent + retry-safe (ADR-038 is already inside
  `write_delta_table`).
- gkdv per-scorer failure attributed in the event `error`; unit fails → rolls forward.

## 7. Testing

- **Pure/unit (fixtures, no Spark):** `assign_workers` slice determinism for the tracking unit set; the
  incremental "done" set from events (0-row unit counts as done); `TrackingMartsProcessor.process` on a
  synthetic unit writes the four tables with correct keys; gkdv scoring→observations and the separate
  `pool_keepers` reduce reproduce today's `gkdv_keeper_pooled` on a fixture corpus (golden equality).
- **Anti-drift guards:** the `write_delta_table(mode="append")`-style AST guard for event writes; a
  `drain.py`-is-imported-verbatim guard; `raise_on_failed_units` present in the worker entry point.
- **Live validation task:** a scoped `--full` recompute over a small provider (idsse, 7 matches)
  → assert full per-provider coverage + `verify_tracking_marts_drain` COMPLETE, then the full run. (Spark
  reads validate live, matching the existing `tracking_marts_driver` posture; unit tests cover the pure
  cores.) No production-scale false-green: validate coverage over the real surface before the DAG rebuild.

## 8. Rollout

1. Ship the PR (all gates: ruff/format/lint-imports/pyright/pytest/bump_wheel/pip_audit; wheel bump; cards;
   TF plan). ADR added.
2. Post-merge CI publishes the wheel + `terraform apply` wires the new tasks.
3. Operator `--full` recompute run → verify coverage + gate COMPLETE + gkdv pooled.
4. **DAG rebuild** (the held Phase 2): full-refresh the four writer marts (overwrites the current partial
   GS-only bronze) + the 12 already-fresh feature marts; strand-safe rederive the 6 in-scope TRIGGERED
   marts. Then HF republish → synced refresh → verify.

## 9. ADR

Warrants an ADR (new drain subsystem; reuse/generalisation of the ADR-037/068 fan-out; gkdv
scoring/pooling split as a stated invariant). Draft alongside the PR.

## 10. Risks

- **gkdv unit memory** (ghost frames) under the 8 h worker — mitigated by driver-mode 16 GB (unchanged from
  today) + a higher watchdog budget if validation shows it; still the riskiest unit.
- **Adapter generalisation touches `action_context_queue`** — backward-compat is asserted by the AC drain's
  existing tests continuing to pass (its `drain_name="action_context"` path is byte-unchanged).
- **Task-count growth** (~4 new tasks) — accepted; each mirrors the proven AC pattern.

## 11. Non-goals

- No change to silly-kicks scoring math. No change to the AC drain's compute. No HF/publish changes. No
  new marts (the four output tables already exist).

## 12. Open questions (for review)

1. **Consolidated vs 3 separate drains** (§2). Consolidated is recommended (compute-once, fewer tasks,
   realises `tracking_marts_driver`'s intent); 3-separate maximally mirrors the AC single-output shape at
   ~10 tasks + 3× input-building. **Confirm before planning.**
2. **N (workers).** Default 8 (AC drain's `_N_DRAIN_WORKERS`). off_ball_runs ~35 min / defensive_credit
   ~2 h at N=8 — comfortable under 8 h. Keep 8 unless validation says otherwise.
3. **gkdv watchdog budget** — start at 2700 s; raise per validation if the doubled-DAS unit needs it.
