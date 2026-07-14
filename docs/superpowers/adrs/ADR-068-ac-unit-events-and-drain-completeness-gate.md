# ADR-068: Action-context unit events and the drain completeness gate

| Field | Value |
|---|---|
| **Date** | 2026-07-13 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

[ADR-067](ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md) closed the bug that silently
zeroed a half-match (`skillcorner:1552423:2` wrote 0 of its 550 actions) and made the drain **raise** when
any unit fails. That fixes the *known* failure mode. It does not answer the question the incident actually
posed:

> After the drain runs, how do we know it processed everything it was supposed to?

ADR-067 left two gaps open, deferred with owner approval:

- **D8 — there is no mart-level completeness gate.** Nothing compares "what the planner enumerated" against
  "what actually landed in `fct_action_context`". The 550-row hole was found by hand, a month late.
- **D9 — the work queue has no terminal state.** `DeltaWorkQueue`'s only write path is `enqueue`. A unit that
  is dequeued and then dies with its driver leaves **no trace at all** — the queue row looks identical to a
  unit that was never picked up. After an OOM you cannot answer "how far did the drain get?"

D8 is unsound without D9. A gate that only reads the mart cannot distinguish *"the drain processed this unit
and it legitimately produced no rows"* from *"the driver died before reaching it"* — and a gate that cannot
tell those apart either accuses healthy runs or excuses dead ones. The run-completion signal has to come from
the drain itself.

## Decision

**Emit a unit-event log from the drain, and gate the mega-job on it.**

### 1. Unit events (D9)

A new append-only event log, `observability.action_context_unit_events`, written through a
`UnitEventSink` port (`analytics/action_context/drain.py`) with a Delta adapter
(`ingestion/action_context_queue.py::DeltaUnitEventSink`). The drain emits:

| event | when |
|---|---|
| `running` | before the watchdog starts the unit |
| terminal (`succeeded` / `failed` / `timed_out`, with `rows_written`) | after each unit, in all three outcomes |
| `slice_completed` | once per worker, **even when the worker is idle** |

`running` (not just a terminal stamp) is what makes the log answer the OOM question: a unit with a `running`
event and no terminal was **in flight when the driver died**. A terminal-only design cannot express that.

**Fail-open on the unit events, fail-loud on `slice_completed`.** A lost `running`/terminal event must never
kill a 5.5-hour drain over telemetry; but a worker that cannot say *"I ran"* is indistinguishable from a
worker that never started, and the gate must be allowed to treat that as a failure.

**Per-worker tables** (`..._w0` … `..._w7`, `..._sb360`) behind a UNION view — not one shared table. This is
not cargo-culting [ADR-038](ADR-038-delta-concurrent-commit-retry.md); it was **measured**. A spike ran the
real append shape from 8 concurrent Databricks drivers:

| topology | p50 per append |
|---|---|
| 8 concurrent writers, one Delta table | **9,700 ms** |
| 1 writer, same table (control) | **1,656 ms** |

Contention is a **5.9× slowdown**, and the pre-registered threshold (750 ms, fixed before the measurement)
was breached **13×**. At ~47 units per worker that is 7.6 min/worker of pure `_delta_log` contention versus
1.3 min. The control is what makes this a finding rather than a guess: the cost is contention, not Spark
job overhead, so sharding the log genuinely removes it.

### 2. The completeness gate (D8)

A pure verdict function (`analytics/action_context/drain_gate.py`) with a thin Spark entry point
(`ingestion/action_context_gate.py`), wired as a **fan-in task** `verify_action_context_drain` with
`run_if = ALL_DONE`, depending on **both** the 8-way drain `for_each` and the separate statsbomb-360 task.
`ALL_DONE` is the point: the gate must run precisely when an upstream **failed**, because that is when its
report matters most.

**Expected workers** = `DISTINCT worker_id` from the queue ∪ the sb360 sentinel.
**Expected units** = queue rows ∪ sb360's `running` events.

| verdict | condition | raises |
|---|---|---|
| `DRAIN_FAILED` | an expected worker never emitted `slice_completed` | no — **report**, don't pile on |
| `UNVERIFIABLE` | anomalies exist **and every one** sits inside a lossy worker | no |
| `INCOMPLETE` | a **clean** worker has an expected unit with **no terminal**, or a **clean sb360** has a `succeeded` match that wrote **zero rows** | **yes** |
| `COMPLETE` | otherwise | no |

`anomalies` = [expected units with no terminal] + [sb360 `succeeded` units with `rows_written == 0`],
partitioned by the worker that owns the unit. `failed` units are **named in the report** but do not move the
verdict (ADR-067's `raise_on_failed_units` has already failed that worker's task; raising again would only
mask the drain's real exception with the gate's). `timed_out` is excused everywhere — it rolls forward by
design.

#### What the gate does **not** own: per-unit row completeness

The first cut of this gate carried a rule that compared a `succeeded` unit's `rows_written` against a fresh
read of `fct_action_context`, and sold it as an independent *"did the rows actually land"* cross-check. **It
was tautological, and it is deleted.** `_process_tracking_match` calls `write_delta_table(...)` with **no
`row_count`**, so — per [ADR-045](ADR-045-ac1-single-pass-write-and-aqe-proof-dispatch.md) — the value it
returns is *itself* a POST-WRITE count of the `replaceWhere` mart slice. That number becomes `rows_written`.
The gate then re-read the same mart with the same predicate and compared the two. Same quantity on both
sides: the check could not fail except on a mart mutation between the write and the gate, and it structurally
could **not** detect `skillcorner:1552423:2` — the incident it was named after — because that surfaces as a
`failed` terminal, which the rule skipped.

**Per-unit row completeness is owned by the DRAIN**, not by the gate: ADR-067's bronze-anchored completeness
invariant (emitted rows vs the actions the frames cover) runs inside the unit, **raises**, and turns the unit
into a `failed` terminal — which `raise_on_failed_units` then turns into a failed task. The gate's job is the
question the drain cannot answer about itself: *did every expected unit run at all?*

So the gate's **independent teeth** are exactly:

1. **rules 0/1/2** — the event log vs the work queue: did every expected worker run, and did every expected
   unit reach a terminal (the silent-skip class);
2. **the V7 planner alarm** — the planner re-run vs the event log: a unit whose event says `succeeded` and
   which the planner *still* enumerates as unwritten. Two genuinely independent sources. It keys on units
   that **claimed rows**: a unit that legitimately wrote 0 rows (all its SPADL actions carrying a NULL
   `time_seconds`) never lands in the mart, so it stays `remaining` forever and would otherwise raise on a
   perfectly healthy run. Those are reported, not accused;
3. **rule 3 — sb360, and sb360 only.** It is the one producer with *no* per-unit invariant: `_sb360_terminals`
   stamps **every** discovered match `succeeded` with a row count read back from the mart, so a match that
   silently produced nothing compares `0 == 0` and reads as healthy — and `PlannerInputs.remaining`
   deliberately excludes statsbomb, so V7 is blind to it too. It would be re-enumerated forever under a green
   verdict. Discovery *requires* the match to be present in `bronze.statsbomb_360` (ADR-057,
   frames-required), so **zero rows is anomalous**. This is **not** generalised to drain units, where zero
   rows can be legitimate.

Two further properties are load-bearing and both are pinned by name:

- **Per-worker taint.** Event loss is scoped to the worker that lost it. A run-scoped `UNVERIFIABLE` would let
  one dropped event mute the gate for all eight workers — the gate would go quiet exactly when it is needed.
- **`UNVERIFIABLE` requires a non-empty anomaly set.** `all([])` is `True`, so the naive rule silently
  reports `UNVERIFIABLE` on a *clean* run. A gate that cries wolf on healthy runs gets ignored, and an
  ignored gate is worse than no gate.

Separately, and **independent of the verdict**, the two planner alarms raise on their own authority: planner
collapse (`enqueued == 0` while `remaining > 0`) and the V7 alarm above. These derive from the **results mart
and the planner** — not from the fail-open event log — so suppressing them because some *other* worker died
would reintroduce the same disease one level up.

**Terminals are flushed periodically** (every 10 units), not once at the end of the slice — and explicitly
**before the abandon-ceiling raise**. A single end-of-slice flush means any escape from `drain_worker`,
*including its own deliberate raise*, destroys every terminal that worker produced plus its `write_failures`
count; the worker then reads as DEAD and V6 has to reconstruct from the mart what the worker already knew.
A planned raise must not be indistinguishable from an OOM. Each worker owns its own Delta table, so the extra
commits cost no cross-worker contention.

### 3. Planner grain fix

`_find_tracking_new_period_pairs` and `_find_idsse_new_period_pairs` joined tracking frames (at
`(match, period)` grain) against SPADL actions at **match** grain, so a period with frames but zero actions
was enumerated as a unit that could never produce rows. Both legs are now period-grain.

This ships here rather than separately because the **V7 alarm** depends on it. V7 raises when a unit the event
log calls `succeeded` is *still* enumerated by the planner. A zero-action unit writes nothing, so it never
leaves the planner's `remaining` set — under the old match-grain join it would be enumerated forever and V7
would raise on a **perfectly healthy run**. The grain fix removes that class at the source; the residual
(a unit whose SPADL actions all carry a NULL `time_seconds`) is handled by V7 keying only on units that
claimed rows.

## Consequences

**Positive.** "How far did the drain get?" is answerable after an OOM. A unit that never ran, a worker that
never reported, and an sb360 match that silently produced nothing are all **raising** verdicts on the next
run rather than a month-long silence — and a failed unit is at least *named*. The queue gains a terminal
state without gaining a mutable writer — `enqueue` remains the only queue write path, and the events live in
their own tables.

Note precisely what this does **not** claim: the 550-row hole itself (`skillcorner:1552423:2` writing 0 of
550 actions) is caught by **ADR-067**'s per-unit completeness invariant, which raises inside the unit. This
gate's contribution to that incident is that the resulting `failed` terminal is now visible in a report
instead of only in a task exit code. Overstating the gate — claiming it independently re-checks that every
unit's rows landed — is exactly the error the deleted rule 3 embodied.

**Negative.** ~390 extra one-row Delta commits per run, sharded across 9 tables (~1.3 min/worker, measured).
A new task in the mega-job. The event log is fail-open, so it is **evidence, not truth** — which is exactly
why `UNVERIFIABLE` exists as a verdict instead of the gate pretending the log is complete.

**`run_id` is `{{job.run_id}}` everywhere — never the preflight task value.** `compute_action_context_statsbomb`
does **not** depend on `preflight_action_context`, so that task value cannot resolve for it; and it is `""` on
a nothing-to-do preflight, while sb360 still files events under the real run id. A consumer reading the task
value would report `DRAIN_FAILED` **every quiet day**. Preflight is itself passed `{{job.run_id}}`, so the
values are identical — this is purely about which reference *resolves*.

**Table creation belongs to preflight, and must precede its own early-return.** The drain is an 8-way
`for_each`, so creating the tables there would put 8 concurrent drivers on 9 `CREATE TABLE IF NOT EXISTS`
statements plus a view — the contention class the spike exists to avoid. It must sit *before* preflight's
nothing-to-do return, because the gate reads the view even on a quiet run.

**But preflight is NOT the only writer, and the view is the reason that matters.** `compute_action_context_statsbomb`
does **not** depend on `preflight_action_context`, so the two tasks *overlap* — and `ensure_tables()` issues,
besides the 9 idempotent `CREATE TABLE IF NOT EXISTS`, one **`CREATE OR REPLACE VIEW`**, which is *not*
idempotent under concurrency: two tasks replacing the same view at once is a metastore race that can throw,
and both tasks run at `max_retries = 0`. sb360 still cannot inherit preflight's creation (no dependency ⇒ no
ordering) and its `slice_completed` is fail-loud, so it must create *something*: it calls the narrow
`DeltaUnitEventSink.ensure_own_table(worker_id)`, which issues `CREATE TABLE IF NOT EXISTS` for **its own
`_sb360` table only** and never touches the view. **Preflight owns the view, alone.**

**`write_delta_table` defaults to `mode="overwrite"`.** Every event write passes `mode="append"` explicitly.
This is not hypothetical: the spike's first measurement run silently wiped its own log on every write and
left **1 row** from 392 appends. Had the sink shipped that way, the gate would have accused a perfectly
healthy drain on every single run. An AST guard now fails the build on any event write that omits it.
