# AC-1 Worker-Drain Fan-Out — Design Spec

| Field | Value |
|---|---|
| **Date** | 2026-06-02 |
| **Status** | Approved (brainstorming) — pending implementation plan |
| **Author** | Karsten Nielsen (architect) + Claude |
| **Supersedes** | the `chunk_sizes` task-value fan-out in `src/ingestion/action_context.py` |
| **ADR** | ADR-037 (to be written bundled with the implementation PR) |

## 1. Problem

The AC-1 action-context fan-out enumerates every unprocessed game into a Databricks
**task value** (`action_context_chunks`), which a `for_each_task` iterates one element at a
time. The task value is bounded by the **~48 KB Databricks limit**. At a full cold start
(~5,500 games) the chunk strings (`"provider:id1,…,idN"`) — dominated by the match-ids
themselves (~5,500 × ~8 B ≈ 44 KB) — sit **at or over** the cap regardless of `chunk_size`.

The product requirement is **no static cap on game count** — truncating the fan-out to fit
48 KB is a no-go. Stated honestly and once (so it is not overclaimed downstream):

> **The guarantee.** (a) No static cap on how many games a run enumerates — the 48 KB ceiling
> is gone. (b) **Within-run completion absent worker crashes** — a run drains every enumerated
> game to completion unless a worker process dies. (c) **Eventual completion across runs** —
> any unit left unprocessed (crash, timeout) is re-discovered and finished by a later run via
> the idempotent skip-guard. The product requirement ("never *cap*") is met by (a); single-run
> completion is best-effort, not absolute (see §8).

Two constraints make this non-trivial on Databricks:
- A `for_each_task` input is *itself* a bounded reference (task value / job param) — you
  cannot stream an unbounded element list into one for-each.
- The standing rule "**never extend the 1800 s per-iteration timeout**" (it exists so a slow
  game can't hide behind a longer budget).

## 2. Current mechanism (what we are replacing)

- `main_preflight()` (`src/ingestion/action_context.py`): discovery (`_ActionContextGuard.check`)
  → builds `chunks: list[list[str]]` via the `chunk_sizes` dict → flattens to
  `chunks_for_inputs: list[str]` → `dbutils.jobs.taskValues.set("action_context_chunks", …)`.
- Terraform (`terraform/modules/workflows/main.tf:154-187`): `compute_action_context` is a
  `for_each_task` with `inputs = "{{tasks.preflight_action_context.values.action_context_chunks}}"`,
  `concurrency = 8`, nested `compute_action_context_iteration` with `timeout_seconds = 1800`,
  `max_retries = 0`, entry point `compute_action_context`, `--match-ids "{{input}}"`.
- `main()`: parses `--match-ids` (`_parse_action_match_ids_arg`), loops over the ids,
  dispatches per-game to `_process_tracking_match` / `_process_statsbomb_match` /
  `_process_event_only_match`.

Each chunk is a **separate task run** → a fresh 16 GB serverless driver + wheel install +
bootstrap. ~633 chunks = ~633 cold starts (the 120–300 s provisioning variance, ×633).

## 3. Decisions (locked in brainstorming)

1. **Guarantee = no static count cap + within-run completion absent crashes + eventual
   completion across runs** (the honest three-part statement in §1; not an absolute
   single-run-completion claim).
2. **1800 s moves from per-iteration timeout to a per-game watchdog.** Each game must still
   finish in ≤1800 s (slow-game-hiding still impossible — the rule's *spirit* is preserved);
   the worker iteration itself runs for hours. The rule's *letter* (where the 1800 s lives)
   moves.
3. **Work pickup = static cost-aware assignment (Approach A), with a clean path to dynamic
   claiming (Approach B).** A bin-packs units across fixed workers at preflight time; B (a
   future drop-in `WorkQueuePort` adapter) would let idle workers steal a crashed worker's
   work *within* the same run.
4. **Worker count = a single Python constant** `_N_DRAIN_WORKERS = 8`, surfaced via the
   worker-ids task value so it lives in Python only (no Terraform literal to drift).

## 4. Architecture

The fan-out inverts from **N chunks → N short iterations** to **N persistent workers
draining a durable queue**:

```
preflight_action_context (one task run)
  ├─ run_id = {{job.run_id}}  (job-level, passed as a job PARAM — see B1/§5.3)
  ├─ units = guard.discover_units(...)            # structured, no chunk batching
  ├─ assignments = assign_workers(units, N=8, cost_fn)   # pure LPT bin-packing
  ├─ DeltaWorkQueue.enqueue(run_id, assignments) # replaceWhere run_id (NOT overwrite)
  ├─ taskValues.set("action_context_run_id", run_id)            # worker reads this back
  └─ taskValues.set("action_context_worker_ids", ["0".."7"])   # tiny, constant-size

compute_action_context  (for_each over ["0".."7"], concurrency = _N_DRAIN_WORKERS)
  └─ worker K (one persistent 16 GB driver):
       run_id  = --run-id   {{tasks.preflight.values.action_context_run_id}}   # NOT an env var
       drain_worker(queue, processor, watchdog, run_id, worker_id=K)
         for unit in queue.units_for_worker(run_id, K):     # WHERE run_id, worker_id=K ORDER BY seq
           result = watchdog.run(lambda: processor.process(unit), 1800)  # runs in a worker thread
           # on timeout: interruptTag cancels Spark jobs, GameTimeoutError raised, loop continues
```

### 4.1 Structural wins (beyond removing the cap)

- **Cold-starts collapse ~633 → 8.** A worker is one persistent driver draining hundreds of
  units in-process; the wheel install + bootstrap + xT-grid load happen **once per worker**,
  not once per unit. This is the primary architectural benefit.
- **`chunk_size` is deleted.** It only ever existed to pack games under the 1800 s
  *per-iteration* timeout. With a per-game watchdog and a persistent worker there is no
  per-iteration budget to pack against. One unit = one queue row. This retires the failing
  `test_guard_chunk_sizes_keep_task_value_under_limit` and the entire `chunk_sizes` dict.
- **IDSSE half-unit granularity stays.** A unit is a *match* for most providers, a
  *(match, period) half* for IDSSE — that is the 1 GB applyInPandas memory cap (ADR-028
  lineage), a separate constraint that survives.

## 5. Hexagonal components

### 5.1 Pure core — `src/analytics/action_context/drain.py` (no Spark / Delta / dbutils)

- **Reuse the canonical `WorkUnit`** from `analytics/action_context/work_unit.py`
  (`provider`, `match_id`, `period`, `frame_range`) — do **not** shadow it (H1). `est_cost` is
  an *assignment* concern, not an intrinsic unit property, so it lives on a wrapper:
  - `WorkAssignment` (frozen): `unit: WorkUnit`, `worker_id: int`, `seq: int`, `est_cost: float`.
    These map 1:1 to queue rows.
- `DrainSummary` (frozen): counts (`processed`, `failed`, `timed_out`, `total_rows`) +
  per-unit outcomes for logging.
- Protocols:
  - `WorkQueuePort`: `units_for_worker(run_id: str, worker_id: int) -> list[WorkUnit]` (ordered
    by `seq`). *(B path: a future adapter adds `claim_next(run_id, worker_id) -> WorkUnit | None`
    and `drain_worker` switches loop style via a flag — the existing method is unchanged.)*
  - `WatchdogPort`: `run(fn: Callable[[], int], label: str, timeout_s: int) -> int`.
    **Contract (N2 — explicit, because a bare `threading.Thread` swallows exceptions):**
    returns `fn()`'s result on success; raises `GameTimeoutError` on budget expiry; **otherwise
    re-raises `fn`'s exception with type and traceback preserved, on the calling thread.** The
    adapter captures the thread's result/exception in a box and replays it on the controller —
    without this, a real processing error becomes a silent no-op (neither `processed` nor
    `failed`) and the ADR-002 hard-fail-with-key discipline is defeated. (A `run`-style port,
    not a context manager, because the production adapter executes `fn` on a worker thread so
    the controller can survive a non-interruptible hang — see §5.2 / H2 / N1.)
  - `GameProcessorPort`: `process(unit: WorkUnit) -> int` (rows written).
- `assign_workers(units, n_workers, cost_fn) -> list[WorkAssignment]` — greedy
  **Longest-Processing-Time** bin-packing: sort by `cost_fn(unit)` desc **with a stable
  tiebreak key `(provider, match_id, period, frame_range)` (N4)** so the result is independent
  of `discover_units`' Spark row order (which is not guaranteed run-to-run); assign each unit to
  the currently-least-loaded worker; `seq` = the unit's index within that worker's assigned list
  (deterministic, L3). Pure, total (every unit assigned exactly once), handles
  `n_units < n_workers` and empty input.
- **Cost model `cost_fn` (H3)** — defined here, pure, injectable:
  - Default `tier_cost_fn`: keyed on `provider_tier(unit)` (the existing helper) →
    `{idsse-half: 1800, tracking-match: 1800, event-only: 60}` seconds, with a `frame_range`
    span / row-count secondary key as a tiebreaker so equal-tier units still spread. The point
    of the model is *rank order*, not accuracy.
  - Documented upgrade path: a `historical_cost_fn` adapter that reads a rolling median of
    observed per-unit durations (we already emit `AC1_SUMMARY … elapsed_seconds`) — deferred,
    but the injection seam is the whole reason `cost_fn` is a parameter.
- `drain_worker(queue, processor, watchdog, run_id, worker_id, logger) -> DrainSummary`
  — the use-case. For each unit in `queue.units_for_worker(...)`:
  `rows = watchdog.run(lambda: processor.process(unit), unit_label, 1800)`.
  On `GameTimeoutError` → ERROR-log with unit key, record `timed_out`, **continue**.
  On any other exception → ERROR-log with unit key (hard-fail-with-key discipline), record
  `failed`, **continue**. A failed/timed-out unit wrote nothing (single atomic Delta write,
  verified §7/B2) and is re-discovered next run by the skip-guard. No Spark imports.

### 5.2 Adapters — `src/ingestion` (Delta / Spark / dbutils)

- `DeltaWorkQueue` (over `action_context_work_queue`, schema per §6/M2): `enqueue(run_id,
  assignments)` via `replaceWhere(run_id=<this run>)` (M1 — race-safe vs overlapping
  preflights, idempotent-on-retry), `units_for_worker(run_id, worker_id)` (`SELECT … WHERE
  run_id=? AND worker_id=? ORDER BY seq`). The **B-path extension point**.
- `SparkInterruptWatchdog.run(fn, label, timeout_s)` — runs `fn` on a **dedicated worker
  thread** that first calls `spark.addTag(tag)` then `fn()`; the controller joins with
  `timeout_s`. On timeout: `spark.interruptTag(tag)` then a short second join.
  **Thread-locality invariant (B2):** `addTag` is thread-local to the ops issued on that
  thread, so it MUST be called *inside* the worker thread that runs `fn` (it is, by
  construction); `interruptTag(tag)` is cross-thread by tag string, so the controller firing it
  is fine. Documented in the adapter docstring so a future pool-offload refactor can't break it
  silently. Two distinct timeout outcomes:
  - **Tracking (interruptible):** `interruptTag` cancels the `applyInPandas` Spark job → the
    worker thread's Spark action raises → the thread **returns** within the short second join.
    **No leak.** `run` raises `GameTimeoutError`; the in-flight Delta write was cancelled →
    **wrote nothing** → re-discovered next run.
  - **Event-only (non-interruptible driver pandas):** `interruptTag` has no Spark job to cancel,
    so the worker thread does **not** return. The controller **abandons** it and continues
    draining (vs. the do-nothing alternative where one hang blocks the worker for 8 h and rolls
    its *whole* slice). N1 hardening:
    - **Explicit bound (N1.2):** the drain loop tracks `abandoned_thread_count`; when it exceeds
      a configured ceiling `_MAX_ABANDONED_THREADS` (default 3), the worker **fails fast**
      (raises) → the task fails → its remaining slice rolls to next run **deliberately and
      observably**, instead of accumulating leaked threads to an unpredictable OOM. The bound is
      named and enforced, not asserted.
    - **Late-write honesty (N1.1):** an abandoned thread may *later* complete its
      `write_delta_table` (idempotent `replaceWhere`, so no corruption). `DrainSummary` records
      such a unit as `timed_out` meaning "exceeded budget; may have completed post-abandonment";
      either way the next run's skip-guard is correct. The §8 "wrote nothing" reasoning is
      stated per-path (tracking: guaranteed nothing; event-only: idempotent-either-way).
    - **Per-unit heartbeat (N7):** each `run()` constructs a **fresh** heartbeat instance, so an
      abandoned thread's `hb.set_phase(...)` cannot stomp the next unit's phase telemetry. (Today
      `_process_tracking_match` builds its own `hb` per call — confirm it is not module/closure
      shared.)
    - **Escalation path:** if the ceiling proves insufficient in practice, replace the event-only
      thread with a killable `multiprocessing.Process(join+terminate)` (clean memory reclaim).
      Deferred — the ceiling is the proportionate first cut for a fast, rarely-hanging path.
- `SparkGameProcessor`: dispatches a `WorkUnit` to the existing `_process_tracking_match` /
  `_process_statsbomb_match` / `_process_event_only_match` — **those functions are unchanged**.
  Loads the xT grid once at construction (per worker), not per unit.
- `HistoricalCostFn` (optional, deferred): the `cost_fn` adapter reading observed durations
  (H3 upgrade path). Default ships with the pure `tier_cost_fn`.

### 5.3 Driver entry points — `src/ingestion/action_context.py`

- `_ActionContextGuard` gains `discover_units(spark, catalog, schema) -> list[WorkUnit]`
  (the structured discovery). `check()` calls it internally and still returns the **generic**
  `FilterResult(count=…)` for skip-guard telemetry — `FilterResult.chunks` (the shared
  fan-out field used by *all* guards) is **no longer populated by AC-1** and its shape is
  untouched (Hyrum's law). The `chunk_sizes` ClassVar and the chunk-batching loop are removed.
- `main_preflight()` reworked: `units = guard.discover_units(…)` → `assign_workers` →
  `DeltaWorkQueue.enqueue` →
  `taskValues.set("action_context_worker_ids", [str(i) for i in range(_N_DRAIN_WORKERS)])`.
- `main_drain_worker()` (NEW entry point `compute_action_context_drain_worker`): parse
  `--worker-id`, read `run_id` from `DATABRICKS_RUN_ID` env, build the three adapters, call
  `drain_worker(...)`, emit a worker-level start/progress/end structured log.
- `main()` (the `--match-ids` single-shot path) is **kept** for ad-hoc/manual single-match
  runs and existing tests; it shares the `_process_*` functions.

## 6. Work-queue table

`{catalog}.observability.action_context_work_queue` — **schema placement DECIDED (M2/N5):**
this is run-scoped *orchestration scratch*, not ingested source truth, so it sits in the
existing **`observability`** schema ("platform operational metadata" — already home to
`workflow_cost_live`), **not** `bronze.` (keeps bronze the truth layer and off C4/lineage).
Evidence the job identity can write it: the `CostEstimateHook` already writes
`{catalog}.observability.workflow_cost_live` during every workflow run. No runtime verify — the
name is fixed here so the migration, `DeltaWorkQueue` table name, C4 element, and parity tests
all bind to it.

| column | type | note |
|---|---|---|
| `run_id` | STRING | the job run id preflight wrote (B1 — passed, not env-derived) |
| `worker_id` | INT | 0 .. `_N_DRAIN_WORKERS`-1 |
| `seq` | BIGINT | drain order = unit index within the worker's LPT-sorted slice (L3, deterministic) |
| `provider` | STRING | |
| `match_id` | STRING | native id |
| `period` | INT | nullable; non-null only for IDSSE halves |
| `frame_range_lo` / `frame_range_hi` | BIGINT | nullable; the `WorkUnit.frame_range` tuple, if any |
| `est_cost` | DOUBLE | seconds estimate used for assignment + observability |
| `_ingested_at` | TIMESTAMP | UTC audit column (project convention) |

**run_id (B1 — the highest-risk item):** the preflight task and each for-each worker task do
**not** reliably share `DATABRICKS_RUN_ID` (it is per-task / has `"unknown"` + `time.time()`
fallbacks in this codebase, and `WorkflowContext.run_id` is a per-instance `uuid4`). So the
run_id is **never** read from the worker's env. Preflight resolves it from the Databricks
job-level `{{job.run_id}}` (passed as an explicit job parameter, identical across all tasks in
the run), writes it to the `action_context_run_id` task value, and the worker receives it as
`--run-id "{{tasks.preflight_action_context.values.action_context_run_id}}"`. The worker filters
`WHERE run_id = <that value>` — guaranteed to match what preflight wrote. **(N6)** This
task-value round-trip is *intentional redundancy*, not a second source of truth: it carries
exactly the value preflight used (robust even in a standalone fallback where `{{job.run_id}}`
is unset). `{{job.run_id}}` remains the authoritative origin.

Preflight writes via `replaceWhere(run_id=<this run>)` (M1 — race-safe vs. an overlapping
manual+scheduled preflight; idempotent on retry), **not** a full overwrite. DDL ships as an
idempotent migration `scripts/migrations/2026-06-02-create-action-context-work-queue.sql`
(auto-applied at live-build CI per the project convention).

## 7. Per-game watchdog details

- Default budget **1800 s**, configurable (a constant, not a magic literal).
- **Thread model (B2/H2):** `fn` runs on a dedicated worker thread that calls `addTag` then
  `fn`; the controller joins with the budget. This is what lets a non-interruptible hang be
  *abandoned* rather than block the worker (§5.2). Thread-locality invariant documented in the
  adapter.
- **Effective for tracking** — `_process_tracking_match` runs `applyInPandas`, a Spark job,
  which `interruptTag` cancels (frees executors).
- **Event-only is driver-side pandas** (~5–60 s/game) — `interruptTag` cannot cancel it; the
  watchdog instead abandons the thread after the budget. Rare in practice, but the blast-radius
  change is **named, not hand-waved** (see H2 / §8): a leaked thread, not a blocked worker.
- **Atomic-write claim — VERIFIED (B2), key stated precisely:** each `_process_*` performs
  exactly **one** `write_delta_table(...)` to the single results table `_TABLE_NAME`
  (`spadl_action_context`). The `replace_where` key is **period-aware for tracking** —
  `match_id = '…' AND period_id = {period_filter}` (`action_context.py:1356`), falling back to
  `match_id` only when there is no period filter; event-only (1858) and statsbomb (1729) are
  `match_id` only. **This precision is load-bearing:** IDSSE units are *(match, period)* halves,
  so a `match_id`-only key would make period-2's write **delete period-1's rows** — silent
  half-game loss. The code is correct; the design's "interrupt ⇒ wrote nothing ⇒ re-discovered"
  reasoning depends on it, so a regression test asserts **two halves of one IDSSE match each
  survive the other's write** (§10). The skip-guard keys on this same table; the cost-hook is a
  separate table on a different grain. (Re-confirm during impl that no per-unit side-write is
  added ahead of the results write.)
- **Pre-fft-cic edge note:** a tracking game currently measures ~1794 s — at the 1800 s edge.
  A game that trips the watchdog rolls to the next run (no data loss, idempotent). The upcoming
  fft-cic ghost-GK switch collapses tracking per-game time and removes the edge.
- **VERIFY-ON-SERVERLESS (load-bearing, no repo precedent — `interruptTag`/`addTag` appear
  nowhere today):** confirm `spark.interruptTag` actually cancels a Spark-Connect serverless
  `applyInPandas`. The integration smoke test (§10/L1) is the proof. If it does **not** work,
  the degraded fallback is: soft-log on timeout + rely on the worker-level Databricks
  `timeout_seconds` as the hard ceiling — which loses per-game granularity **and throughput**
  (a straggler then blocks its worker until the 8 h timeout, rolling its whole remaining slice;
  same failure shape as a non-interruptible hang, H2).

## 8. Error handling, idempotency, crash recovery

- Per-unit failure/timeout → ERROR log + continue; unit wrote nothing → re-discovered next
  run by the skip-guard.
- **Blast-radius change vs. today (H2):** today a hung game kills only its own
  `compute_action_context_iteration` (per-iteration isolation). Under drain, the thread-based
  watchdog (§5.2/§7) restores per-unit isolation for the *interruptible* (tracking) case and
  for the *abandon-the-thread* (event-only) case — so a single bad unit still costs ~one unit,
  not the worker. The **only** path that loses a worker's whole remaining slice is a genuine
  worker-process crash (OOM) or the degraded no-`interruptTag` fallback (§7).
- Worker crash (OOM etc.) → its remaining units roll to the next run.
- This is the §1 guarantee: **no static count cap; within-run completion absent worker
  crashes; eventual completion across runs.** `max_retries = 0` stays (idempotent writes make
  retry safe, but we do not rely on it).
- All telemetry on exception paths is ERROR-level (never warning) per ADR-002.

## 9. Terraform / job changes

- `preflight_action_context` gains a job parameter carrying `{{job.run_id}}` (B1).
- `compute_action_context` for-each `inputs` →
  `"{{tasks.preflight_action_context.values.action_context_worker_ids}}"`.
- nested task: entry point `compute_action_context_drain_worker`, params
  `--worker-id "{{input}}"` **and** `--run-id "{{tasks.preflight_action_context.values.action_context_run_id}}"`,
  `timeout_seconds` 1800 → **28800 (8 h)**, `max_retries = 0` (unchanged;
  `patch_job_retries.py` still enforces the zero-value).
- **`concurrency` invariant (M4):** the for-each spawns `len(worker_ids)` =
  `_N_DRAIN_WORKERS` tasks; `concurrency` only caps *simultaneity*. If they differ, workers run
  serialized in waves (correct but silently slower). To prevent drift, `concurrency` is set
  equal to `_N_DRAIN_WORKERS` and a parity test asserts the Terraform `concurrency` literal ==
  the Python constant (mirrors existing TF-parity tests). The Python constant remains the
  single control; the test pins the TF side to it.
- New entry point in `pyproject.toml`: `compute_action_context_drain_worker`.

The one-time cold start (84 tracking ÷ 8 ≈ 11 tracking games × 1800 s ≈ **~5.5 h** on the
slowest worker) exceeds the "compute task ≤ 2 hr" budget — an **intentional, documented
exception** for this task (recorded in ADR-037 + CLAUDE.md). Daily runs after cold start are
tiny (only newly-ingested games).

## 10. Testing (TDD)

**Unit (pure, no Spark):**
- `test_assign_workers`: balanced bins within tolerance; every unit assigned exactly once;
  `n_units < n_workers`; empty input. **Cost distribution (H3):** the `N` most expensive units
  land on `N` distinct workers. **Determinism (L3/N4):** `assign_workers` over a **shuffled**
  copy of the same unit *set* produces identical `worker_id` *and* `seq` (proves the stable
  tiebreak, not just identical-input stability).
- `test_drain_worker`: drains only its own slice; preserves `seq` order; watchdog-timeout →
  `timed_out` + continue; processor exception → `failed` + continue + unit-key in log;
  `DrainSummary` totals correct.
- `test_watchdog_run_contract` (N2): with a fake/real watchdog, `fn` raising `ValueError("boom")`
  → `run` re-raises `ValueError` (preserved type, **not** swallowed, **not** wrapped as
  `GameTimeoutError`); `drain_worker` then records `failed` with the unit key in the log.
- `test_drain_abandonment_ceiling` (N1): a watchdog stub that reports abandonment on every unit
  → `drain_worker` fails fast once `abandoned_thread_count > _MAX_ABANDONED_THREADS`, with the
  count in the ERROR log (deliberate slice rollover, not silent accumulation).
- `test_tier_cost_fn`: rank order is `idsse-half ≈ tracking-match > event-only`; tiebreaker
  spreads equal-tier units.
- Replace `test_guard_chunk_sizes_keep_task_value_under_limit` with:
  - `test_worker_id_task_value_is_constant_size`: the emitted task value is O(`_N_DRAIN_WORKERS`),
    independent of game count (size stable across a 10-unit vs a 100 000-unit discovery).
  - `test_workqueue_holds_all_units`: assignment retains **every** discovered unit (no
    truncation) at scale.
- `test_terraform_concurrency_matches_n_workers` (M4): TF `concurrency` literal ==
  `_N_DRAIN_WORKERS`.

**Integration (Spark, serverless-marked):**
- `DeltaWorkQueue` enqueue → `units_for_worker` round-trip; `replaceWhere` semantics; period +
  frame_range nullability.
- **`test_units_for_worker_run_id_isolation` (L2):** enqueue rows for run A and run B; assert
  `units_for_worker(run=B, worker=K)` returns only B's rows. Pins the exact thing B1 is about.
- **`test_idsse_halves_survive_each_other` (B2 precision):** write period-1 then period-2 of one
  IDSSE match via the real `_process_tracking_match` path; assert period-1 rows still present
  after period-2's `replaceWhere` (the half-game no-clobber property the drain reasoning leans
  on).
- **`test_spark_interrupt_watchdog_smoke` (L1/N3, the B2 proof):** drive the **real**
  `SparkGameProcessor.process(unit)` on a tracking unit (or a faithful replica issuing the
  `applyInPandas` at the *same call depth* as `_process_tracking_match`), wrapped in the **real**
  `SparkInterruptWatchdog` with a ~5 s budget; assert `GameTimeoutError` raises **and** the Spark
  job was actually cancelled. Driving the real processor (not a synthetic `applyInPandas`) is the
  point — it proves the tag set on the watchdog thread propagates to the Spark job issued frames
  deep, which is the contract the worker depends on. Serverless-marked.

**End-to-end:**
- `test_drain_e2e`: `assign_workers` over a multi-provider unit set, then run `drain_worker`
  for **every** worker_id against an in-memory queue + counting fake processor; assert the
  union of processed units == the full input set, **each exactly once** (the "never capped"
  guarantee, executable). The **headline regression gate.**
- The existing AC-1 mini-golden gate (`test_mini_golden`) and full golden are unaffected
  (they exercise `run_work_unit`, downstream of the fan-out).

## 11. Governance / docs

- **ADR-037** "AC-1 worker-drain fan-out" — bundled with the implementation PR. Covers: the
  1800 s → per-game-watchdog reinterpretation, the 48 KB cap removal, the `chunk_size`
  deletion, static-assignment-with-path-to-B, the 2 hr-budget exception, and the
  ~633 → 8 cold-start reduction. **Consequences must name the concurrency cost (N8):** on a
  cold start, **8 simultaneous 16 GB serverless drivers run for ~5.5 h** (the slowest worker) —
  a one-time, justified spend, but named so it is not a billing surprise; daily runs after that
  are tiny.
- **CLAUDE.md** amendment: the 1800 s rule wording (now per-game watchdog), the 2 hr-budget
  documented exception for `compute_action_context`, and removal of the `chunk_sizes`
  references.
- **C4** (`docs/c4/architecture.dsl`): update the AC-1 / action-context element descriptions
  (edit in place, ≤ ~200 chars).
- **Wheel bump** via `scripts/bump_wheel.py` (src + scripts changed).
- **Migration**: `scripts/migrations/2026-06-02-create-action-context-work-queue.sql`
  (idempotent `CREATE TABLE IF NOT EXISTS`, date-prefixed kebab convention).

## 12. Out of scope / explicitly deferred

- **Approach B (dynamic claiming)** — designed-for (the `WorkQueuePort` + `claim_next`
  extension point) but not built. Promote if single-run crash-completion or severe load
  imbalance becomes a real requirement.
- The fft-cic ghost-GK switch (separate workstream B) and the v2 statsbomb stringify fix
  (separate, uncommitted) — independent of this change.
- Removing `main()` (`--match-ids`) — kept for manual runs.

## 13. Review resolution (parallel-session critical review, 2026-06-02)

| # | Item | Resolution |
|---|---|---|
| **B1** | `DATABRICKS_RUN_ID` not shared across tasks → silent zero-rows | **Fixed.** run_id is never env-derived in the worker; preflight resolves `{{job.run_id}}` (job param), writes the `action_context_run_id` task value, worker reads it as `--run-id`. Verified: `WorkflowContext.run_id` is per-instance `uuid4` and `DATABRICKS_RUN_ID` has `"unknown"`/`time.time()` fallbacks. Pinned by `test_units_for_worker_run_id_isolation` (§10/L2). |
| **B2** | `interruptTag` unproven; thread-locality; atomic-write | **Atomic-write VERIFIED** (single `replace_where` write to `_TABLE_NAME`; cost-hook is separate-table/different-grain). Thread-locality invariant documented (§5.2/§7). `interruptTag` proof = the serverless smoke test (§10/L1); honest degraded fallback restated (§7). |
| **H1** | Duplicate `WorkUnit` | **Fixed.** Reuse `work_unit.WorkUnit` + `provider_tier`; `est_cost` moved to a `WorkAssignment` wrapper. No name shadowing. |
| **H2** | Blast radius for non-interruptible event-only hang | **Addressed.** Watchdog runs `fn` on a worker thread; a non-interruptible hang is *abandoned* (one leaked thread) not blocking the worker. Named explicitly in §5.2/§7/§8. |
| **H3** | `est_cost` model undefined | **Defined.** Pure `tier_cost_fn` via `provider_tier` + tiebreaker; injectable `cost_fn` with a documented `historical_cost_fn` upgrade path. Tested (`test_tier_cost_fn`, cost-distribution assertion). |
| **M1** | Use `replaceWhere`, not overwrite | **Fixed** (§5.2/§6). |
| **M2** | Queue in `bronze.` fights truth-layer principle | **Addressed.** Target an ops/telemetry schema, not `bronze.`; verify-writable + documented fallback (§6). |
| **M3** | Guarantee wording contradiction | **Fixed.** Single honest three-part statement up front (§1), referenced from §3/§8. |
| **M4** | `_N_DRAIN_WORKERS` vs TF `concurrency` drift | **Fixed.** `concurrency == _N_DRAIN_WORKERS` + parity test (§9, `test_terraform_concurrency_matches_n_workers`). |
| **L1** | Watchdog integration smoke test | **Added** (§10). |
| **L2** | run-id isolation test | **Added** (§10). |
| **L3** | `seq` determinism | **Defined** (index within worker's LPT slice) **+ tested** (§5.1/§10). |

### Review #2 resolution (2026-06-02)

| # | Item | Resolution |
|---|---|---|
| **§7** | IDSSE `replaceWhere` key precision | **Fixed.** §7 now states the period-aware key (`match_id AND period_id`) and the half-game no-clobber property; regression test `test_idsse_halves_survive_each_other` (§10). |
| **N1** | Thread abandonment unbounded + owns SparkSession | **Bounded.** Tracking is interrupted cleanly (no leak); event-only abandonment capped by `_MAX_ABANDONED_THREADS` (default 3) → deliberate fail-fast/rollover, not OOM. Late-write honesty + per-unit heartbeat (N7) + subprocess escalation path documented (§5.2). |
| **N2** | `WatchdogPort.run` exception contract | **Specified.** Re-raises `fn`'s exception (type+traceback) on the controller thread; `test_watchdog_run_contract` (§5.1/§10). |
| **N3** | L1 smoke test must use the real processor path | **Fixed.** L1 drives the real `SparkGameProcessor.process` at production call depth, real watchdog (§10). |
| **N4** | LPT tiebreak unstable vs Spark row order | **Fixed.** Stable tiebreak `(provider, match_id, period, frame_range)`; determinism tested over a **shuffled** input (§5.1/§10). |
| **N5** | Ops schema is a step-zero decision, not runtime verify | **Decided.** `{catalog}.observability.*` (cost-hook already writes it → job identity can) — name fixed in the spec (§6). |
| **N6** | B1 task-value redundancy | **Documented** as intentional redundancy; `{{job.run_id}}` authoritative (§6). |
| **N7** | Shared `hb` heartbeat under thread model | **Fixed.** Fresh heartbeat per `run()`; confirm `_process_tracking_match` isn't shared (§5.2). |
| **N8** | Cold-start concurrency cost in ADR | **Added** to ADR consequences: 8 × 16 GB drivers × ~5.5 h one-time (§11). |
