# AC unit-event log (D9) + drain completeness gate (D8) — design (v3)

**Date:** 2026-07-13
**Status:** Proposed
**Supersedes:** v1, v2. §12 records what each got wrong. The corrections came from review, before any code.
**Follows:** [ADR-067](../adrs/ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md) — D8 + D9 ship
**together**.

---

## 0. THE INVARIANT (read this first)

> **D8's inputs are EXACTLY two persisted tables: `action_context_work_queue` and
> `action_context_unit_events`. Nothing in process memory. Any new gate input must be persisted, or it is not
> an input.**

This is a **checkable rule, not an aspiration**. It exists because the same defect was introduced twice: v1 fed
the gate `summary.timed_out`, v2 fed it `sink.write_failures` — both **in-memory objects inside a drain worker**,
both read by a gate that **runs in a different task, in a different process**. Fixing the instance did not stop
the class. This rule does, mechanically. A reviewer (or a test) can check it without understanding the design.

## 1. Why these ship together

`skillcorner:1552423:2` wrote **0 of its 550 actions** while the job reported SUCCESS (2026-07-11). ADR-067 fixed
the cause and made a failing unit **fail its task** (D2). Two gaps remain: nothing asserts the drain *finished its
work*, and the queue records only what was **planned** (`enqueue` is the only write path — one commit per run).

## 2. What the gate asserts — and why NOT the planner anti-join

The planner (`action_context.py:528-552`, and `:611-616` for idsse) joins:

```
tracking(_mid, _period)  ⋈ INNER on _mid ONLY  spadl(_mid)   ← selects match_id_native, NO period
                         ⟖ LEFT ANTI (_mid,_period)  results
```

**The SPADL leg is MATCH grain.** So a `(match, period)` with **frames but zero SPADL actions in that period** is
enumerated → processed → `pipeline.py:403` `if actions.empty: return _empty_result()` (silent 0-row return) →
never lands in `results` → **re-enumerated forever**. D3 does not catch it (`completeness.py:141` early-returns on
`bronze_expected == 0`). An anti-join gate would raise on it **every run, permanently** — red on a correct state,
the exact "muted within a month" failure this design exists to prevent.

**Measured live, 2026-07-13** (all four drain providers; total **374 enumerable units** — which matches the `374
units` the real preflight logged, cross-validating the query):

| provider | enumerable units | zero-action units |
|---|---|---|
| skillcorner | 220 | **0** |
| gradientsports | 134 | **0** |
| idsse | 14 | **0** |
| metrica | 6 | **0** |

**Latent, not live.** Latent is not safe: it is a landmine under a gate whose entire value is being trusted.

**The gate therefore asserts the DRAIN:**

> **D8 = for this `run_id`: every enqueued unit has a terminal event, and every terminal is `succeeded`.**
> Timeouts excused (they roll forward by design). Failures already fail the task via D2.

Immune to zero-row units (`succeeded, rows_written=0`), catches exactly the silent-skip class, needs no anti-join
semantics, and is the only formulation that can run in a fan-in task (§5).

**Fix the source too:** make the SPADL leg **period-grain** in both planners (`select(match_id_native, period_id)
.distinct()`, join on `["_mid","_period"]`). Then a zero-action period is never enumerated and the queue stops
lying.

## 3. D9 — append-only unit event log

Table `observability.action_context_unit_events`, **append-only**, **partitioned by date** (retention = partition
drop, not a tombstone-generating DELETE):

| column | notes |
|---|---|
| `run_id`, `worker_id` | |
| `provider`, `match_id`, `period` | unit key (queue grain); NULL `period` for sb360 (match-grain) |
| `state` | `running` / `succeeded` / `failed` / `timed_out` / `slice_completed` |
| `started_at`, `ended_at` | `ended_at` NULL on `running` |
| `rows_written` | terminal only |
| `error` | `failed` only |
| `write_failures` | **`slice_completed` only** — see §4 |
| `_ingested_at` | auto-added by `write_delta_table` |

**`running` is written BEFORE the unit is processed.** If a driver is OOM-killed, the units it *started* remain
visible and distinguishable from units never begun. A terminal-only stamp cannot answer that.

**Status is a run-scoped VIEW:** `queue LEFT JOIN latest-event-per-unit ON (run_id, provider, match_id, period)`.
Keyed on `run_id` — units are re-enqueued across runs, so a latest-across-all-runs join would misattribute a prior
terminal to a fresh unit.

**`action_context_work_queue` is UNCHANGED — no ALTER.** `test_work_queue_schema_parity.py:33-41` pins the
2026-06-02 CREATE migration DDL against `_QUEUE_COLUMNS`; adding a column forces editing a historical migration.

**No `skipped_no_frames`** — frames drive the planner; a no-frames period is never enumerated, so there is no moment
to stamp it.

## 4. The one event that MUST land — and the three verdicts

The sink is **fail-open** for unit events (telemetry loss must not become data loss, ADR-002). But a fail-open sink
feeding a *raising* gate turns a **lost event into a false accusation**. And the naive patch — "write the failure
count into the event log" — is self-defeating, because the failing thing *is* the event log.

**So exactly one write is fail-loud:**

- **`slice_completed` carries `write_failures`** (per worker, a column).
- **That single write retries hard, and if it cannot land, the worker task FAILS.** It costs nothing in data terms
  — all unit work is already committed; it is one commit at slice end — and it is the only way the gate can ever
  trust its own inputs. It also satisfies §0: the count is **persisted**, not in memory.

| observed (from the two tables ONLY) | verdict |
|---|---|
| `slice_completed` present · `write_failures = 0` · every enqueued unit has a terminal · all `succeeded`/`timed_out` | **COMPLETE** |
| `slice_completed` present · `write_failures > 0` | **UNVERIFIABLE** — loud, **non-raising**. Evidence is known-lossy; it must never produce a confident accusation. |
| `slice_completed` present · `write_failures = 0` · a unit has **no terminal** | **INCOMPLETE** → **RAISE**, naming the units. Now a *trustworthy* accusation. |
| `slice_completed` **absent** | **impossible** — the worker task would have failed (fail-loud), so the gate is skipped or reports (§6). |

*No signal must never masquerade as negative signal.*

## 5. Where D8 runs — a NEW fan-in task

`compute_action_context` is a **`for_each_task`, `concurrency = 8`** (`main.tf:193`). **There is no post-drain
task.** A gate inside a drain worker would evaluate a **global** condition while 7 peers still drain → false fire
every run, and it could not see another worker's state at all.

D8 is a **new terraform task**, `depends_on` **both** `compute_action_context` **and**
`compute_action_context_statsbomb`, reading **only the two tables** (§0).

## 6. `run_if` — ALL_DONE, and why

Databricks defaults to **ALL_SUCCESS**: if a worker dies (OOM / 8 h kill) its task fails → the for_each fails →
**the gate is skipped**. Under that default, D9's OOM-visibility — the whole reason `running` is written before
processing — would be delivered to *nobody*, left to a human running an ad-hoc query.

**Decision: `run_if = ALL_DONE`.**

- Upstreams **succeeded** → the gate applies §4's verdicts, and **RAISES** on INCOMPLETE.
- Any upstream **failed** → the job has already failed; the gate **REPORTS and never raises** — it names the
  in-flight units (`running` with no terminal) and the workers with no `slice_completed`. This is the *only*
  version where the gate itself delivers D9's OOM payoff.

## 7. StatsBomb — sb360 emits events too

**sb360 units are NEVER enqueued.** `action_context.py:731-735`: sb360 "EXITS the per-match drain… therefore NOT
enqueued as drain units here"; it runs as one distributed cogroup job (ADR-058). So a queue⋈events gate says
**nothing whatsoever** about statsbomb — and v2's `depends_on compute_action_context_statsbomb` bought **zero
assertions**, merely delaying the gate. v1's anti-join *did* cover sb360 (`_find_sb360_new_ids`), so dropping it
**silently removed statsbomb's only completeness check**. That is a regression, not a simplification.

**Fix:** the sb360 task emits events too — it knows its match-id set up front. Per-match `succeeded` (`period`
NULL) plus one `slice_completed` with `write_failures`. It is a **single writer** → no contention → the same gate
covers it unchanged. sb360's queue-equivalent is its discovered match set, persisted as `running` events at start.

## 8. Cross-check: what LANDED, not what was reported

The gate otherwise audits the drain using the drain's own self-report. Add, for each `succeeded` unit:

> `count(results rows for (match_id, period)) == event.rows_written` — else **RAISE**.

**Correcting the reviewer's premise (verified):** `rows_written` is **already outcome, not intent**.
`write_delta_table` is called (`:1801-1808`) with `replace_where` and **no `row_count`** — and per **ADR-045**,
without a caller `row_count` it counts the **materialized Delta slice POST-write** when the slice is identifiable
(`replaceWhere`). So `written` is what landed in Delta, not the DataFrame length.

The cross-check is still worth having — it re-reads **independently at gate time** and would catch a later
clobbering or a partial commit — but it is **defence in depth, not the closing of a hole**. It needs no expectation
model (so it re-implements neither D3 nor the planner); zero-action units satisfy it trivially (`0 == 0`); the
frame-coverage excuse class satisfies it trivially (`rows_written` already reflects the excuse). **Skip `timed_out`
units** — see §9.

## 9. Abandoned threads (closed, not deferred)

The watchdog **abandons non-interruptible threads that are still alive** (`drain.py:157`), so a zombie can write its
unit's rows **after** the worker moved on. The per-unit write **is** `replaceWhere`-scoped (`_period_replace_where`,
`:1793`, `:1806`), so a late write is **idempotent** w.r.t. that unit's own output and next-run reprocessing does not
duplicate.

**Consequence the gate must honour:** a zombie's late write lands **after** its `timed_out` event, so
**`timed_out` + rows-present is a LEGAL state**, not a contradiction. §8's cross-check therefore **skips
`timed_out`**.

## 10. Commit contention — and the pre-registered threshold

**Partitioning is NOT a contention control.** ADR-038:61-63, verbatim: *"`_delta_log` serialization is inherent to a
single Delta table, so the only ways to **eliminate** (not mitigate) contention are: (a) serialize commits via a
single-committer / the work-queue, or (b) **split into multiple tables**."* Partitioning is on neither list. Blind
appends contend on the **commit log**, which partitioning does not shard — and ADR-038's incident was **5 workers
writing DISJOINT data** racing one `_delta_log` version → S3 400 → **4 of 5 games silently failed**. Disjointness is
what partitioning buys, and disjointness is what already failed.

**Design:** per-unit `running`; **BATCHED terminals** (flushed per worker). This is the *default*, not a fallback:
terminal state is **reconstructible** (rows exist in results; failures fail the task), `running` is
**reconstructible from nothing**. Follow the asymmetry.

**Post-design commit count: 374 `running` + 8 `slice_completed` ≈ 382 one-row commits** from 8 concurrent writers on
one `_delta_log`. ADR-038's incident had **5**. This is the repo's first high-frequency multi-writer pattern, so it
gets measured, not assumed.

**Spike (Task 0) — threshold pre-registered BEFORE the measurement:**

> **Route to per-worker tables (`…_unit_events_w{n}` + UNION view — ADR-038's own elimination route (b)) if
> EITHER: p50 per-append latency at 8-way concurrency > 750 ms, OR any append exhausts ADR-038's 10 retries in a
> 382-append simulation.**

Any defensible number would do; the point is that it exists **before** the result, so the result cannot be
rationalised.

**File growth, not row growth, is the cost:** ~382 one-row appends/run ⇒ ~382 files/run. Needs `OPTIMIZE` +
`VACUUM`; the queue's `prune` pattern does not cover this (the queue is one commit/run).

## 11. Remaining holes, named

- **Planner discovered N, `enqueue` persisted M < N.** The gate compares queue⋈events — both would be
  self-consistently short. **Fix in preflight:** after enqueue, assert `count(assignments) == count(queue rows for
  run_id)` (one round-trip count).
- **`prune` must never remove the current `run_id`** before the gate reads it. State and test the ordering.

## 12. Architecture — the sink is a PORT

`analytics/` must not import `ingestion/` — `.importlinter` contract `analytics-isolation` (`type = forbidden`).
The sink is an **injected port** like `queue`/`processor`/`watchdog`; the Spark impl (`DeltaUnitEventSink`) lives in
`ingestion/action_context_queue.py`. Tests inject a recording fake.

Unit-event writes are **fail-open** (ERROR log — warning-level telemetry swallows are forbidden; they hid the
2026-04-12 cost-hook blocker for 62 h) **and increment a counter**. The `slice_completed` write is **fail-loud**
(§4). The counter reaches the gate only by being **persisted** in `slice_completed` (§0).

## 13. Testing

| what | test |
|---|---|
| `running` before processing | fake sink records ordering; a unit that RAISES still has its `running` event |
| zero-row unit | empty-actions unit → `succeeded, rows_written=0` → gate PASSES (the §2 class) |
| sink fail-open | unit-event sink raises → drain continues, logs at **ERROR**, counter increments |
| `slice_completed` fail-loud | that write cannot land → **worker task FAILS** |
| **`UNVERIFIABLE` — at the GATE's layer** | construct queue + event tables with `slice_completed.write_failures > 0` → gate reports, does **NOT** raise. (A fake-sink in-process test would exercise the layer where the defect *isn't*.) |
| **`INCOMPLETE` — at the GATE's layer** | queue + events with a missing terminal, `write_failures = 0` → **RAISES**, names the unit |
| timeout excused | `timed_out` unit → no raise |
| `timed_out` + rows present | LEGAL (zombie late write) → no raise; cross-check skips it |
| cross-check | `succeeded` unit whose persisted row count ≠ `rows_written` → **RAISES** |
| sb360 coverage | sb360 emits `running` + `succeeded` + `slice_completed`; gate covers it |
| enqueue round-trip | assignments != persisted queue rows → preflight RAISES |
| run-scoped view | same unit across two runs → no cross-run misattribution |
| planner grain fix | period with frames + zero actions → NOT enumerated |
| §0 invariant | gate module imports/reads ONLY the two tables (source-level assertion) |
| import boundary | `analytics/` ⇏ `ingestion/` (import-linter) |

## 14. Sequencing

0. **Spike** the append cost (§10) against the pre-registered threshold. Nothing is built until it reports.
1. **D9** — sink port + Delta impl + fail-loud `slice_completed` carrying `write_failures`.
2. **Planner SPADL leg → period grain** (both planners; §2 source fix).
3. **sb360 events** (§7).
4. **D8** — new fan-in task, `run_if = ALL_DONE`, three verdicts, cross-check, planner re-run as a **non-raising**
   diagnostic.

## 15. Not in scope

- **TC-1 non-determinism** — arbitrary pick among **content-divergent** duplicates (4,052 keys, ~99% divergent);
  ADR-030-class, unknown size, mart is a retirement candidate.
- **The 38-action residual** (7 SC + 31 GS); **GS extra-time (891)** — unrecoverable (ET events, no ET frames).

## 16. What v1 and v2 got wrong (kept deliberately)

**v1:** built the contention mitigation on a property ADR-038 explicitly rules out (partitioning), while citing that
same ADR for its retry; put the gate in "the compute task", which does not exist (the drain is an 8-way `for_each`);
asserted the planner anti-join without tracing that the planner can enumerate zero-action periods forever.

**v2:** fixed the *instance* and repeated the *class* — deleted `summary.timed_out` as a gate input because it is
in-memory and the gate is a separate task, then introduced `sink.write_failures` as a gate input, in-memory, in the
same design. And dropping the anti-join **silently ungated sb360** (assumed covered because the predecessor covered
it).

The shape, every time: **reasoning from what the code should do instead of reading what it does** — the same failure
that produced the wrong root cause twice during ADR-067.

The response is not "be more careful". It is **§0**: a checkable invariant that catches this class mechanically,
including the next instance nobody has thought of yet.
