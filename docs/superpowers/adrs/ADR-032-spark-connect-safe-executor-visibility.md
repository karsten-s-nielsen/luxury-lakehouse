# ADR-032: Spark-Connect-safe executor→driver visibility for AC-1

| Field | Value |
|---|---|
| **Date** | 2026-05-31 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

`compute_action_context` dispatches per-match `applyInPandas` UDFs on Databricks
serverless and can run for tens of minutes per match. Operators need two things
the platform does not give for free: (1) per-phase/per-batch **progress** during
the long `write_delta_table` action, and (2) enough **diagnostic signal** to
locate the still-open silent hang (the UDF wedges with no error, no trace; see
project memory `ac1-serverless-hang-open`).

Two prior attempts failed against Spark Connect's constraints:

- **PR #320** — driver-side `_BatchHeartbeat` thread polling a
  `spark.sparkContext.accumulator(0)`. Crashed at 16 s: serverless forbids
  `sparkContext` (`PySparkAttributeError`).
- **ADR-031** — emit `AC1_BATCH` log lines from inside the UDF and read them
  from the driver log stream. Verified non-functional (iteration
  `961253300571334`): executor stdout/stderr goes to the *executor* log, which
  `jobs.get_run_output` / the Jobs UI "Task logs" view never surface. Only
  *driver*-process stdout reaches the task log.

The binding constraint: **the only Spark-Connect-safe executor→driver channel is
a shared durable store the executor writes and the driver reads** — not logs,
not accumulators, not `StreamingQueryListener`/`recentProgress` (all forbidden),
not `DataFrame.observe()` (post-stage only, useless mid-hang).

## Decision

Add `src/ingestion/exec_visibility.py` with a driver-poller + executor-marker
rendezvous, wired into the `compute_action_context` UDF:

1. **Driver-side `PhaseHeartbeat`** — a daemon thread that every N seconds prints
   to driver stdout (which IS the task log): elapsed, current driver phase, an
   optional target-table `COUNT(*)` (pure `spark.sql`), and the **content of the
   newest executor rendezvous marker**.
2. **Executor-side markers** — from inside the UDF, raw `open()` writes to a
   driver-pre-created UC Volume dir (no token/internet/`spark` needed): an
   environment fingerprint at entry (numba threading layer, fork/spawn,
   library versions, internet reachability), and an `_ERROR` marker carrying the
   traceback on failure. `faulthandler.dump_traceback_later` dumps stuck-thread
   stacks to executor stderr for a true hang.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. `sparkContext` LongAccumulator (PR #320) | Driver-aggregated count | `sparkContext` forbidden on serverless | Crashes at dispatch |
| B. UDF `logger.info` → grep driver log (ADR-031) | Zero infra | Executor logs never reach the task log on Connect | Invisible to operators |
| C. `DataFrame.observe()` | Connect-safe | Metrics only emit after the stage completes | Useless during a hang |
| D. Driver poller + executor UC-Volume markers (chosen) | Connect-safe; real-time in the task log; carries env-fingerprint + error traceback; doubles as the executor-FUSE-write capability probe | Stuck *stack* still only in executor stderr (Spark UI thread dump); depends on executor FUSE-write working on serverless | — |

## Consequences

### Positive

- Per-phase progress + executor diagnostics visible in the **task log** in real
  time, including while a stage is mid-flight or hanging.
- Env fingerprint surfaces the leading hang hypotheses (numba `prange` under
  fork, no-internet model download, version drift) the instant a worker starts.
- `diagnose_ac1_run.py` already reads the task log via `get_run_output`, so
  echoed marker content flows to the existing diagnostic with no new tooling.

### Negative

- Rendezvous markers depend on executor FUSE-write to a UC Volume working on
  serverless; the production ingestion SP holds `READ/WRITE VOLUME` on
  `bronze._staging`, and the first run's `markers=N` field confirms capability.
- The stuck *stack* (faulthandler) lands in executor stderr, not the task log —
  reading it needs the Spark UI thread dump (writing it from the watchdog thread
  to a FUSE file while the main thread is wedged is unsafe).

### Neutral

- This ADR is about *observing* the hang, not fixing it. The serverless
  `applyInPandas` hang remains OPEN; `bronze.spadl_action_context` is currently
  populated via the local `scripts/run_action_context_local.py` fallback
  (ADR-028 hexagon).

## CLAUDE.md Amendment

None. Reinforces the existing rule (project memory
`test-production-driver-entry-point`) that visibility instrumentation for
`applyInPandas`/`mapInPandas` must be driver-side; this ADR is the canonical
implementation of that rule.

## Related

- **ADRs:** supersedes ADR-031; builds on ADR-028 (AC-1 hexagon).
- **Code:** `src/ingestion/exec_visibility.py`, `src/ingestion/action_context.py`
  (`_make_action_context_udf`, `_process_tracking_match`),
  `scripts/diagnose_ac1_run.py`.
- **External references:** Databricks Spark Connect limitations —
  https://docs.databricks.com/release-notes/serverless.html#limitations
