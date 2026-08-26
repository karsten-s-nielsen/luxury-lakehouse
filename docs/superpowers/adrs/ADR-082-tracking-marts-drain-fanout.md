# ADR-082: Tracking-marts fan-out — one worker-drain replaces three driver-sequential writers

| Field | Value |
|---|---|
| **Date** | 2026-08-25 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The three Rev-6 tracking-grain ADR-013 writers — `off_ball_runs_writer`,
`defensive_credit_writer`, `gkdv_writer` — were each a **driver-sequential loop** over the whole
tracking corpus (`iter_unit_inputs` → score → write), with no `applyInPandas` fan-out and no
incremental gating. Every run re-derived the oriented `(actions, frames, xt)` for the *entire*
~180–500-match tracking surface from scratch, even though nothing had changed since the previous
run. At a measured ~1.5 min per `(match, period)` unit in isolation, a single-driver pass over the
corpus projects to **~4.7 h** (off-ball runs) / **~15 h** (defensive credit, two scorers + the
per-unit xG read) and **OOM** for gkdv — so all three **timed out every run** and shipped their
bronze tables **empty**. `gkdv_writer` was doubly exposed: its `run_pipeline` accumulated the whole
corpus of per-frame keeper observations in a single `list[pd.DataFrame]` and `pd.concat`-ed it on
the 16 GB serverless driver before pooling, so even ignoring wall-clock it could not hold the corpus.

The forcing function is the same one ADR-037 solved for action-context: a daily run must complete
within budget and must not re-do work it already did. The AC drain already has a proven pure
fan-out core (`analytics.action_context.drain`), a Delta work-queue + unit-events adapter
(`ingestion.action_context_queue`), and a completeness gate (`analytics.action_context.drain_gate`,
ADR-068). The three tracking-grain writers need exactly that machinery — but the AC adapters were
hard-wired to the string literal `action_context` for their table names and to the sb360 worker as
a fixed part of the worker topology.

## Decision

Replace the three driver loops with **one consolidated `tracking_marts` worker-drain** that reuses
`analytics.action_context.drain` (the pure ADR-037 fan-out core) **verbatim** via new adapters. The
AC Spark adapters (`action_context_queue`) and the pure gate (`drain_gate`) are generalized along
**two orthogonal axes**, both defaulting to the AC behaviour so the AC drain stays byte-identical:

1. **`drain_name` (table-name axis).** `DeltaWorkQueue`, `DeltaUnitEventSink`, and the
   `event_table_*` / `event_view_sql` helpers take `drain_name="action_context"`; the tracking drain
   passes `drain_name="tracking_marts"`, namespacing its `tracking_marts_work_queue` /
   `tracking_marts_unit_events_w{id}` / `tracking_marts_unit_events` tables. The module keeps its
   name until this PR; it is now **renamed to the drain-neutral `drain_adapters`** (both drains use it).
2. **The worker-topology axis (`include_sb360` / `extra_expected_workers`).** The sb360 sentinel
   worker (`-1`) is welded into the AC event-table set and the pure gate's `expected_workers`. A
   drain with no sb360 task would otherwise report `DRAIN_FAILED` every run. `event_table_names` /
   `event_view_sql` / `DeltaUnitEventSink` take `include_sb360=True`, and `drain_gate.evaluate` takes
   `extra_expected_workers=frozenset({SB360_WORKER_ID})`. The tracking drain passes `include_sb360=
   False` and `extra_expected_workers=frozenset()`; AC keeps the defaults.

The skip-guard is **events-based, cross-run, and `succeeded`-only.** "Done" means a unit has a
`succeeded` terminal in `tracking_marts_unit_events` under **any** `run_id` — NOT an
output-`left_anti` on a mart. A multi-output drain (four bronze tables, several of which are
legitimately zero-row for a given unit) has no single-table "≥1 row ⇒ done" invariant, so
output-presence is not a usable done-signal; the unit-event is. A `failed` / `timed_out` unit, or
one that never ran, stays OPEN and is re-enumerated.

**gkdv scoring and pooling are SPLIT.** Per-unit `score_gkdv_unit` writes per-frame keeper
observations to a new `bronze.gkdv_observations` intermediate inside the drain; a **separate
single-driver `compute_gkdv_pool` reduce** reads the whole `gkdv_observations` corpus and runs
`pool_keepers` once. Pooling cannot be per-unit: `aggregate_by_keeper`'s `min_games=2` per-keeper
floor is an irreducible whole-corpus reduce (a keeper's games span multiple units/matches), so it
must run after every unit has landed its observations.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Keep three driver loops, add incremental gating to each | Minimal blast radius | Still driver-sequential (no fan-out); triples the corpus reads (each writer re-derives the same oriented inputs); three separate timeout budgets | Does not fix the root cause — the surface is still re-scored serially on one driver every run. |
| B. Three separate drains (one per writer) | Each mart isolated | Triples the oriented-input rebuild (the expensive part) and triples the queue/gate/event infrastructure; 3× cold-start | The three writers need the *same* `(actions, frames, xt)` per unit — building it once and running all scorers is the whole point. |
| C. Output-`left_anti` skip-guard (a unit is done when its mart has ≥1 row) | No events dependency | A multi-output drain has no single "≥1 row ⇒ done" table (units legitimately produce 0 rows for some outputs); can't distinguish "ran, wrote nothing" from "never ran" | Ambiguous for zero-row-legitimate outputs — exactly what ADR-068 unit-events exist to disambiguate. |
| D. Fork `drain` / `drain_gate` for tracking-marts | No parameter threading | Two copies of the pure fan-out core to keep in sync forever | The cores are pure and reused verbatim; two backward-compatible parameters keep AC byte-identical and avoid a fork. |
| E. One consolidated `tracking_marts` drain reusing the AC cores + gkdv pool split (chosen) | Single oriented-input rebuild per unit; N×8 fan-out; ADR-068 completeness proof; AC untouched at defaults | Adds two parameters to two pure cores; gkdv needs a separate reduce task | — |

## Consequences

### Positive

- **N×(=8) throughput.** The corpus fans out across 8 persistent drain workers instead of one
  driver loop, bringing all four outputs under the 8 h worker task budget (the same
  `for_each_task` / `concurrency = 8` topology ADR-037 established for action-context).
- **Incremental by construction.** After the first `--full` cold start, a daily run only enumerates
  units without a `succeeded` terminal — the chronic re-score-everything cost is gone.
- **One oriented-input rebuild per unit.** `TrackingMartsProcessor.process(unit)` builds
  `(actions, frames, xt)` once and runs all four scorers on it, instead of three writers each
  rebuilding it.
- **Completeness is provable (ADR-068).** `verify_tracking_marts_drain` reconstructs, per enumerated
  unit, whether it produced a terminal event + `slice_completed`, summing rows across **all four**
  output tables so a dead worker's agg-only unit is not misclassified.
- **The AC drain is untouched.** Every generalization defaults to the AC behaviour; `drain` and
  `drain_gate` remain the reused pure cores.

### Negative

- **`action_context_queue` was renamed to `drain_adapters`** (this PR) — it is now a shared drain
  adapter serving both the action-context and tracking-marts drains, not an AC-only queue.
- **gkdv pooling is a mandatory separate reduce.** `compute_gkdv_pool` must run after the drain and
  before the pooled mart is consumed; forgetting it leaves `gkdv_keeper_pooled` stale even when
  `gkdv_observations` is fresh.
- **The sk-version guard relocated.** The retired writer `run_pipeline`s each asserted
  `_assert_silly_kicks_min()` at start; that guard now lives in `TrackingMartsProcessor.__init__`
  (the single scoring entry). A future writer-level scorer added outside the processor must re-assert
  it.
- **`dbt_build_output_marts` dependencies changed** — the three old writer task keys are removed and
  `compute_gkdv_pool` + `verify_tracking_marts_drain` are added (still `ALL_DONE`).

### Neutral — DURABLE OPERATOR FOOT-GUN

**"Done" is a cross-run `succeeded` unit-event, NOT output presence.** This is correct design — it
is exactly why events beat output-`left_anti` for a multi-output, zero-row-legitimate drain — but it
has a sharp edge: **truncating or dropping any of the four output bronze tables
(`off_ball_runs`, `action_defensive_credit`, `defensive_credit_attributions`, `gkdv_observations`)
leaves its units still marked done, so daily incremental runs will SKIP them and the table stays
empty** until someone forces a re-enumeration.

**Any operation that clears one of these tables — a mart rebuild, a schema migration, a backfill,
a manual TRUNCATE/DROP — MUST be followed by a `--full` run:
`preflight_tracking_marts --full` (job parameter `tracking_marts_full="1"`), then
`compute_tracking_marts` → `verify_tracking_marts_drain` → `compute_gkdv_pool`.** A `--full`
preflight bypasses the `succeeded`-terminal subtraction and re-enqueues the whole universe. Silent
if forgotten.

## Amendment (2026-08-26) — gkdv gated off pending a dedicated perf project

The first full post-merge drain surfaced that **gkdv cannot finish at production scale** — a
pre-existing compute wall this consolidation inherited, not a regression it introduced.

**Diagnosis.** gkdv's `delta_das` runs an accessible-space DAS **twice per scored frame** (actual +
ghost) under `spearman` pitch control, and `spearman` is the **only** GK-aware method
(`GkdvParams._GK_AWARE_METHODS = ("spearman",)`; `lambda_gk` exists only on `SpearmanParams`), so it
cannot be swapped for a faster backend. `fft-cic` — the fast ghost-GK **KDE** backend the AC drain
uses — accelerates keeper *placement*, not the DAS bottleneck. `possession_stride = 5` is already the
built-in cost control. Measured, gkdv units exceed the 2700 s per-unit watchdog (they abandon with
zero output); at **>45 min/unit × 374 units ÷ 8 workers = >35 h** gkdv overflows the 8 h task budget
regardless of the watchdog. The retired driver-sequential `gkdv_writer` had the same wall (it
"stalled 120 min") — **gkdv has never produced output.**

**Decision.** gkdv scoring is **gated off** behind a single module constant
`tracking_marts_processor.GKDV_ENABLED = False`. `off_ball_runs` + `defensive_credit` compute fast and
ship now. Consuming the one flag: `TrackingMartsProcessor.process` skips the gkdv arm (never scored,
never written, cannot fail a unit); `tracking_marts_gate._OUTPUT_TABLES` **excludes**
`gkdv_observations` (else the write-landed alarm cries wolf over an intentionally-empty table on every
unit); `tracking_marts_drain.main_gkdv_pool` no-ops before touching Spark. The `compute_gkdv_pool`
task stays in the job so the perf project re-enables the whole path by flipping the one constant
(Chesterton's Fence — the pooling reduce and per-frame scorer are retained, not deleted).

**Supersedes** the "summing rows across **all four** output tables" wording under Consequences →
Positive: while gated off the gate sums **three** (the two shipping surfaces), which is exactly what
excluding an un-produced table means. The `gkdv_keeper_pooled` mart stays empty — the status quo,
since gkdv never produced.

**Follow-up (TODO `GKDV-PERF`).** A dedicated gkdv perf project: measure the true per-unit rate + the
scored-frame count, then tune the viable levers (higher `possession_stride`, more workers, a bigger
per-unit watchdog + task timeout, or vectorizing/coarsening the DAS grid — likely a `silly-kicks`
change). **Any change to gkdv's frame sampling is a methodology change to a per-player evaluative
model under EU AI Act governance** — it requires a model-card + `AI_GOVERNANCE.md` review, not a config
flip. gkdv remains an in-scope governed system (`wf-tracking-marts` → `gkdv.md`); it is operationally
paused, not removed.

## CLAUDE.md Amendment

Performance Budgets: `compute_tracking_marts` is a worker-drain task like `compute_action_context`
(ADR-037) — `timeout_seconds = 28800` (8 h), a per-unit watchdog of 2700 s inside the worker, and a
**documented exception** to the "compute task ≤ 2 hr" budget for the one-time `--full` cold start.
The events-based skip-guard makes the daily incremental run tiny.

## Related

- **Plan:** `docs/superpowers/plans/2026-08-25-tracking-marts-drain-fanout.md`
- **ADRs:**
  - **ADR-037** (`ADR-037-action-context-worker-drain-fanout.md`) — the drain fan-out precedent
    (durable work-queue + persistent workers + per-unit watchdog) reused here verbatim.
  - **ADR-045** (`ADR-045-ac1-single-pass-write-and-aqe-proof-dispatch.md`) — the single-pass
    `replaceWhere` write / `row_count` dispatch the processor's `_write` follows.
  - **ADR-067** (`ADR-067-velocity-delete-and-depend-and-unit-write-atomicity.md`) — a drain worker
    that swallows a per-unit failure must still fail its task via `raise_on_failed_units`; applied to
    `main_tracking_marts_drain_worker`.
  - **ADR-068** (`ADR-068-ac-unit-events-and-drain-completeness-gate.md`) — the per-unit lifecycle
    events + fan-in completeness gate; the tracking drain emits the same events and reuses `evaluate`.
  - **ADR-013** (`ADR-013-ml-inference-outputs-dbt-mart.md`) — the Python-writer → bronze → dbt
    staging → gold mart contract the four tracking-grain outputs follow.
- **Code:** `src/ingestion/tracking_marts_processor.py`, `src/ingestion/tracking_marts_drain.py`,
  `src/ingestion/tracking_marts_gate.py`, `src/ingestion/tracking_marts_driver.py`,
  `src/ingestion/drain_adapters.py` (renamed from `action_context_queue`; `drain_name` / `include_sb360`),
  `src/analytics/action_context/drain_gate.py` (`extra_expected_workers`),
  `src/ingestion/{off_ball_runs,defensive_credit,gkdv}_writer.py` (pure scorers retained; driver
  loops deleted), `scripts/migrations/2026-08-25-create-gkdv-observations.sql`,
  `terraform/modules/workflows/main.tf`.
- **Tests:** `src/tests/test_tracking_marts_processor.py`, `src/tests/test_gkdv_pool_split.py`,
  `src/tests/test_gkdv_pool_entry.py`, `src/tests/test_tracking_marts_driver.py`,
  `src/tests/test_tracking_marts_gate.py`, `src/tests/test_drain_adapter_drain_name.py`,
  `src/tests/action_context/test_drain_gate.py`, `src/tests/test_tracking_marts_terraform.py`.
