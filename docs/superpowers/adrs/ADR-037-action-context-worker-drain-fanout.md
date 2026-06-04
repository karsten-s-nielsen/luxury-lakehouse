# ADR-037: AC-1 action-context fan-out — worker-drain over a durable queue

| Field | Value |
|---|---|
| **Date** | 2026-06-02 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The AC-1 action-context fan-out enumerated every unprocessed game into a Databricks
**task value** (`action_context_chunks`), which a `for_each_task` iterated one chunk at a
time. The task value is bounded by the **~48 KB Databricks limit**, and a `for_each` input
is itself a bounded reference (task value / job param) — you cannot stream an unbounded
element list into one for-each. At a full cold start (~5,500 games) the chunk strings
(`"provider:id1,…,idN"`) are dominated by the match-ids themselves (~5,500 × ~8 B ≈ 44 KB) and
sit at/over the cap **regardless of `chunk_size`** — `chunk_size` only changed the prefix
overhead on top. The product requirement is absolute: **a daily run must never cap how many
games are processed.** Truncating the fan-out to fit 48 KB is a no-go.

A second cost: every chunk was a separate task run = a fresh 16 GB serverless driver + wheel
install + bootstrap. ~633 chunks ⇒ ~633 cold starts (the 120–300 s provisioning variance, ×633).

## Decision

Replace the chunk-list fan-out with a **worker-drain** design:

1. **Durable work-queue.** Preflight discovers unprocessed *units* (a match, or a
   `(match, period)` half for IDSSE — the 1 GB applyInPandas memory cap), LPT-bin-packs them
   across `_N_DRAIN_WORKERS` (=8) by estimated cost into
   `soccer_analytics.observability.action_context_work_queue` (run-scoped orchestration
   scratch, NOT bronze), and emits a **constant** worker-id task value + the job run id.
2. **Persistent drain workers.** `compute_action_context` is a `for_each_task` over the
   constant worker-id list (concurrency = `_N_DRAIN_WORKERS`). Each iteration is one
   *persistent* driver that drains its slice (`WHERE run_id, worker_id ORDER BY seq`) to
   completion, processing one unit at a time. The for-each input never approaches 48 KB
   regardless of game count.
3. **Per-game watchdog (the 1800 s relocates).** The 1800 s budget moves from the *iteration*
   timeout to a per-game watchdog *inside* the worker (`SparkInterruptWatchdog`): each game
   must still finish in ≤1800 s (the rule's spirit — no slow-game-hiding — is preserved), but
   the worker iteration runs as long as the queue takes (`timeout_seconds = 28800`).
4. **Hexagonal split.** Pure core in `analytics.action_context.drain` (`assign_workers`,
   `drain_worker`, ports) — Spark-free, unit-tested. Adapters in
   `ingestion.action_context_queue` (`DeltaWorkQueue`, `SparkInterruptWatchdog`,
   `SparkGameProcessor`) — kept offline-importable (pyspark is Databricks-runtime-only) via
   TYPE_CHECKING/function-local pyspark imports + a pyspark-free column list.
5. **Run-id via task value, not env.** `DATABRICKS_RUN_ID` is per-task / unreliable;
   preflight resolves `{{job.run_id}}` (a job param), writes it to the
   `action_context_run_id` task value, and the worker reads it as `--run-id`. The worker never
   touches its own env.
6. **`chunk_size` is deleted.** It only ever existed to pack games under the per-iteration
   1800 s timeout, which no longer exists.

## Alternatives considered

| Option | Why rejected |
|---|---|
| Keep chunks, shrink `chunk_size` | The match-ids alone are ~44 KB at cold start — `chunk_size` can't fix the cap; smaller chunks make it worse (more prefixes) and explode the chunk count. |
| Compact index-refs in the task value, 1 chunk = 1 iteration | Removes the content-size pressure but leaves the for-each iteration-count ceiling; not truly unbounded. |
| Dynamic claiming (workers claim from the queue) | Stronger within-run completion (work-stealing) but per-game Delta commit churn + conflict-retries + concurrency-hard tests. Deferred — the `WorkQueuePort` is designed so a `claim_next` adapter is a drop-in (Approach B). |

## Consequences

### Positive
- **No static cap on game count** — the 48 KB ceiling is gone.
- **Cold-starts collapse ~633 → 8** — one persistent driver per worker; wheel install +
  bootstrap + xT-grid load happen once per worker, not once per unit.
- Pure/adapter split: the orchestration logic is unit-tested without Spark.

### Negative / costs
- **8 × 16 GB serverless drivers run ~5.5 h on a cold start** (the slowest worker: ~11 tracking
  games × 1800 s). A one-time, justified spend; daily runs after cold start are tiny. This is a
  **documented exception to the "compute task ≤ 2 hr" budget** for `compute_action_context`.
- **Blast-radius / abandonment.** Tracking timeouts are cancelled cleanly by `interruptTag`
  (no leak). Event-only is driver-side pandas — `interruptTag` can't cancel it, so a hung
  event-only game's thread is *abandoned* and the worker continues. The concurrent
  `_MAX_ABANDONED_THREADS` ceiling (=3) is a **mitigation that lowers peak-memory risk, NOT a
  guarantee against OOM** — it is only re-evaluated on a timeout event. The real bound, if
  event-only hangs are ever observed, is the deferred killable-subprocess approach.
- Drain-to-completion is **guaranteed-across-runs, best-effort-within-a-run** (a worker crash
  rolls its remaining slice to the next run via the idempotent skip-guard).

### Neutral / verify-on-serverless
- `spark.interruptTag` has no prior in-repo use; the serverless smoke test
  (`test_spark_interrupt_watchdog_real_processor_smoke`) is its proof. Degraded fallback if it
  does not cancel: soft-log + the worker-level `timeout_seconds` as the hard ceiling.
- The atomic-write guarantee rests on each `_process_*` doing a single period-aware
  `replace_where` (`match_id AND period_id` for tracking) — regression-tested by
  `test_idsse_halves_survive_each_other`.

## CLAUDE.md Amendment

Performance Budgets: note that `compute_action_context` is a worker-drain task — the 1800 s is
now a **per-game watchdog inside the worker**, and the task is a **documented exception** to the
"compute task ≤ 2 hr" budget (one-time cold start ~5.5 h). The `chunk_sizes` concept is removed.

## Amendment (2026-06-03): period work-units for all tracking providers + watchdog 1800→2700 + override

- **All tracking providers now enqueue per-`(match, period)` units** like IDSSE already did (`metrica`/`skillcorner`/`gradientsports`). `discover_units` uses `_find_tracking_new_period_pairs` (replacing `_find_tracking_new_ids`); the processing + write path already supported this (the `replaceWhere` predicate — now the pure `_period_replace_where(match_id, period_filter)` helper — is period-scoped when a period is set, and `enrich_batch` filters actions to the period, so two per-period units of one match replace **disjoint** Delta partitions). Smaller units parallelise better under the persistent-worker drain (cold start already amortised) and give the per-game watchdog **per-half** headroom — which is what makes the exact ghost-GK backends (ADR-035 third amendment) fit.
- **Per-game watchdog `WATCHDOG_BUDGET_S` 1800 → 2700 s** (`src/analytics/action_context/drain.py`), with a per-run override: the drain worker takes `--watchdog-budget-s` (job parameter `watchdog_budget_s` ← `var.watchdog_budget_s`), passed to `drain_worker(budget_s=…)`. `_TIER_COST_S` (the LPT load-balancing estimate) is intentionally left unchanged — only rank order matters there. The oneshot/for-each path has **no** in-process watchdog, so its escape hatch for a slow exact-backend run is `submit_ac1_oneshot.py --timeout-seconds` → `SubmitTask(timeout_seconds=…)`, not a watchdog.
- **Preflight task timeout 300 → 600 s** on all 5 preflight tasks: 300 s includes serverless cold-start env setup, and the analytics env's 11-dep pip resolution exceeded it on cache-cold builds (observed TIMEDOUT live 2026-06-03).

CLAUDE.md "Performance Budgets" updated: the per-game watchdog is now **2700 s** (per-half), overridable via `--watchdog-budget-s`.

## Related

- **Spec:** `docs/superpowers/specs/2026-06-02-action-context-worker-drain-fanout-design.md`
- **ADRs:** complements ADR-028 (AC-1 hexagon); independent of ADR-033/034 (schema/coercion).
- **ADR-038** — Delta concurrent-commit retry in `write_delta_table`, the fix for the
  concurrent-commit contention this worker-drain exposed (run 730644476818402: 1/5 written, 4/5
  identical S3-400 racing `_delta_log/00035.json`).
- **Code:** `src/analytics/action_context/drain.py`, `src/ingestion/action_context_queue.py`,
  `src/ingestion/action_context.py` (`discover_units`, `main_preflight`, `main_drain_worker`),
  `scripts/migrations/2026-06-02-create-action-context-work-queue.sql`,
  `terraform/modules/workflows/main.tf`.
- **Tests:** `src/tests/action_context/test_drain.py`,
  `src/tests/action_context/test_watchdog_threading.py`,
  `src/tests/action_context/test_work_queue_schema_parity.py`,
  `src/tests/test_action_context_terraform.py`, and the serverless proofs in
  `src/tests/action_context/test_action_context_queue.py`.
