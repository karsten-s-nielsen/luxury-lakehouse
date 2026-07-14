# AC Unit-Event Log (D9) + Drain Completeness Gate (D8) — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make "a unit was enqueued and never ran" impossible to ship silently — persist per-unit lifecycle events (D9), and add a fan-in task that asserts the drain finished its work (D8).

**Git flow:** ONE feature branch → ONE commit → ONE PR. Spec + plan + ADR bundled. Commit / push / PR / merge each need separate explicit approval.

**Spec:** `docs/superpowers/specs/2026-07-13-ac-unit-events-and-drain-completeness-gate-design.md` (v3, **amended** — see the traceability table; spec §4 and §6 are corrected by P1).

---

## 0. TRACEABILITY — every review item, the TASK, and the ARTIFACT it changes

The recurring failure in this workstream is **a fix that lands anywhere other than the artifact that produces the
behaviour**: sb360's fix landed in the producer task but not the consumer's rules; ALL_DONE landed in the spec but
not the rules the implementer codes; the planner diagnostic landed in the spec's sequencing and in no task at all.

**A spec section is not an implementation. A rule the implementer never reads is not a rule.**

| item | task | **artifact that must change** |
|---|---|---|
| P1 — dead worker must REPORT, not RAISE | 7 | `drain_gate.py` **rule list** (`DRAIN_FAILED` verdict + **rule 0**) |
| P2 — sb360 must be *read*, not just *emitted* | 7 | `drain_gate.py` **expectation set** = queue rows ∪ sb360 `running` events |
| P3 — §0 invariant false / mis-enforced | 8 | `test_gate_inputs_invariant.py` — **AST**, not substring |
| P4 — idle workers never emit `slice_completed` | 4, 7 | `action_context.py:1293-1296` **emit before short-circuit** + gate's **queue-derived** worker set |
| M1 — terminal flush ≠ `slice_completed` | 3, 4 | `DeltaUnitEventSink` — **separate** writes, opposite failure policies |
| M2 — planner diagnostic dropped | 7 | `drain_gate.py` — diagnostic **with teeth** |
| M3 — gate has no `--run-id` | 8 | `main.tf` gate task **`parameters`** |
| M4 — `event_date` absent; no migration/parity test | 3 | `_EVENT_COLUMNS` + `scripts/migrations/` + parity test |
| **V1 — `UNVERIFIABLE` run-scoped → one lost event mutes the whole gate** | 7 | **rule:** per-worker taint · **test:** `test_lossy_worker_does_not_mute_a_clean_worker` |
| **V2 — sb360's `slice_completed` written but never read** | 6 **+** 7 | **producer:** sb360 emits `SB360_WORKER_ID` · **consumer:** sentinel in the expected-worker set |
| **V3 — the AST guard is VACUOUS (house style has no literals)** | 8 | **guard:** resolve names/forbid shapes · **self-test:** 3 planted violations |
| **V4 — the parity test is a substring check** | 3 | **guard:** `_ddl_columns` ordered tuples · **self-test:** planted bad DDL |
| **W1 — rule 1 is VACUOUSLY TRUE on a clean run (`all([]) is True`)** | 7 | **rule:** `UNVERIFIABLE` requires ≥1 anomaly · **test:** `test_lossy_but_no_anomalies_is_COMPLETE` |
| **W2 — the M2 tooth CANNOT catch an under-enumerating planner (shares the function)** | 5 | **test:** two-sided + **dtype variant** · **doc:** state the tooth's blind spot |
| **W3 — sb360 sentinel defined only in the CONSUMER** | 6 **+** 7 | **shared constant** `SB360_WORKER_ID` imported by **both** · **test** on the producer |
| **V5 — `event_date` NOT NULL, nothing populates it** | 3 | **artifact:** pure `_event_row()` · **test:** every NOT-NULL column populated |
| **V6 — dead worker loses ALL terminals → `DRAIN_FAILED` report LIES** | 7 | **rule:** reconstruct from results · **test:** `test_DRAIN_FAILED_report_splits_completed_from_in_flight` |
| **V7 — planner tooth too narrow** | 7 | **rule:** `succeeded` unit still in `remaining` → RAISE · **test:** its own case |
| **W5 — parity test imports from another TEST module** | 3 | **create** `src/tests/_ddl.py` · **modify** `test_work_queue_schema_parity.py` to import it |
| **W6 — planted violations cover 2 of the guard's 3 shapes** | 8 | third planted module: a gate importing `DrainSummary` |
| **X1 / Y2 — sentinel unimportable by the consumer (`analytics` ⇏ `ingestion`)** | 6 **+** 7 | **home:** `analytics/action_context/drain.py` · **producer test:** imports it from there · **consumer:** imports it — **never the literal `-1`** |
| **X2 / Y1 — sb360 early-returns on no work → `DRAIN_FAILED` EVERY QUIET DAY** | 6 **+** 8 | **producer:** emit `slice_completed` before `if not ids: return` · **test:** empty-discovery · **`main.tf`:** `--run-id` param · **argparse:** `main_statsbomb` must ACCEPT `--run-id` (terraform-passes + python-accepts is a PAIR) |

## 0c-bis. ⚠️ `run_id` IS `{{job.run_id}}` EVERYWHERE — **NOT** the preflight task value (found in Task 6)

**The plan's own Task 6/8 HCL was wrong and would not have resolved.** `compute_action_context_statsbomb`
depends on `backfill_statsbomb_360` / `compute_expected_threat` / `compute_spadl_vaep` — **not on
`preflight_action_context`** — and a Databricks task value resolves only from an *upstream* task.

The correct reference is **`{{job.run_id}}`**, and it is the *same value*: preflight is itself passed
`--run-id "{{job.run_id}}"` (`main.tf`:1239) and `_resolve_run_id` returns it verbatim.

**This binds the CONSUMER too (Task 7/8).** The gate MUST resolve `run_id` as `{{job.run_id}}`. If it reads the
preflight task value instead, then on a **nothing-to-do preflight** that value is `""` while sb360 has filed its
events under the real run id — the gate would find no sb360 `slice_completed` and report **`DRAIN_FAILED` every
quiet day.** That is X2 relocated into the consumer, and it is exactly the class this table shows recurring in
*every* review round.

## 0d. ⚠️ `write_delta_table` DEFAULTS TO `mode="overwrite"` — THE SINK MUST PASS `mode="append"`

**Found during the Task 2 spike (2026-07-13), and it would have been catastrophic.**

```python
def write_delta_table(df, catalog, schema, table_name,
                      mode: str = "overwrite",      # <-- utils.py:359. THE DEFAULT.
                      replace_where: str | None = None, ...)
```

Every other caller in this repo relies on that default (they overwrite a partition via `replace_where`). So
the natural way to write `DeltaUnitEventSink` — `write_delta_table(sdf, catalog, schema, _EVENT_TABLE)` —
**silently OVERWRITES the entire event log on every single event write.**

The consequence is not a slow gate; it is an **actively lying** one: the table would hold **one row**, the gate
would find no terminal event for any unit, and it would return **`INCOMPLETE` — accusing a healthy drain — on
every run.** An append-only table whose writer overwrites is the worst possible failure for this design.

**Proof it is real:** the first spike ran 392 "appends" with the default and left **1 row** in the table
(51 commits, the rest lost to overwrite conflicts).

**Rules (Task 3):**
1. `DeltaUnitEventSink` **MUST** pass `mode="append"` explicitly on every write.
2. **A guard that fails without it** (§0b): a test asserting the sink's write call uses `mode="append"` — plant
   the default and prove the test rejects it. This is not paranoia: the default is the *natural* thing to write,
   and it is silently destructive.

## 0c. THE sb360 SWEEP (do this before writing ANY gate code)

**Four rounds, four defects, all on the sb360 side: P2 → V2 → W3 → X1/X2.** The signature is always the same:
**sb360 is NOT the drain** — it exits the per-match drain (ADR-058), has its own task, its own lifecycle, its own
short-circuit, its own parameters — so a rule written while looking at the drain **silently does not apply to it**,
and the fix keeps getting written on the side of the boundary the author was already looking at.

**Before writing gate code, answer all four for sb360 explicitly. Any "we'll see" is an unlanded defect.**

| # | question about the drain | **and what does sb360 do here?** |
|---|---|---|
| 1 | idle worker emits `slice_completed` before its short-circuit (P4) | **X2:** `main_statsbomb` `:1197-1199` **early-returns on `if not ids`** — the COMMON daily case. It must emit `slice_completed` **before** returning, or rule 0 fires **every quiet day**. |
| 2 | drain workers get `--run-id` from the preflight task value (`main.tf:217`) | **X2b:** the sb360 task has **NO `--run-id`** (only `--catalog`, `--schema`, `--max-units`). Its events cannot carry the run the gate verifies. **`main.tf` must pass it.** |
| 3 | drain workers have a `worker_id` from the queue | **W3:** sb360 has **no queue rows and no `worker_id`** → it needs the `SB360_WORKER_ID` sentinel on **every** event. |
| 4 | who imports what | **X1:** `analytics/` **cannot** import `ingestion/` (`.importlinter` `analytics-isolation`; verified: analytics imports ingestion **nowhere**). So the sentinel **cannot** live in `ingestion/`. |

## 0a. THE PAIR RULE (how to read the artifact column)

> **Every item's artifact is a PAIR, and an item whose row names only one half is UNLANDED.**
>
> - a **rule** ⇒ *and the test that fails without it*
> - a **consumer** ⇒ *and the producer that satisfies it*

§0's traceability table (round 3) named **one** artifact per item — and that is exactly why the class kept
recurring: V5/V6/V7 landed as **prose beside the artifact** instead of **in a test**, and the sb360 contract landed
in the consumer's task for the **third** time (P2 → V2 → W3) because the row only ever named `drain_gate.py`.
Defects in this workstream live in pairs. Name both halves, or it has not landed.

## 0b. THE GUARD RULE (plan-wide — applies to every test added by this plan)

> **Every guard must be shown to FAIL on a planted violation of the thing it guards — including guards added by
> a review.**

This exists because the class §0 was created to kill (*a guard that checks the spelling, not the mechanism*)
**reappeared inside the guards themselves**: the AST test cannot fail (V3 — the repo passes
`self._spark.table(self._table)`, an attribute, so a literal-collecting walker gathers **∅** and `∅ ⊆ ALLOWED`
always holds); the schema-parity test is a substring check (V4 — it passes on a wrong type, a wrong order, or a
column mentioned only in a **comment**).

**Therefore: each guard below ships with a companion test that plants a violation and asserts the guard FAILS.**
An invariant guard that has never failed is not a guard.

---

## THE INVARIANT (spec §0 — CORRECTED by P3)

> **The gate's EVIDENCE comes only from persisted tables, on an explicit allowlist:
> `action_context_work_queue`, `action_context_unit_events`, and `spadl_action_context` (the cross-check).
> Its only task-value inputs are PARAMETERS (`run_id`, `catalog`) — never evidence. Nothing from process memory.**

v1 of this rule said "exactly two tables" — which the design itself violated (§8's cross-check reads a third) and
whose test enforced a *spelling* (a substring grep) rather than the *mechanism*. The rule now states what is true,
and Task 7 enforces it by **AST**, so the next instance — a Databricks **task value** smuggling evidence across
tasks, the idiomatic way to do exactly that — cannot sail through.

---

## Context the engineer needs

**Why this exists.** `skillcorner:1552423:2` wrote **0 of its 550 actions** while the job reported SUCCESS
(2026-07-11). ADR-067 fixed the cause and made a failing unit fail its task. But nothing asserts the drain
*finished*: a unit can be enqueued and never run, and the queue records only what was **planned**.

**Facts, all verified against source:**

| fact | evidence |
|---|---|
| `enqueue` is the only queue write; **one commit per run** | `action_context_queue.py:152-180` |
| Drain is an **8-way `for_each`**; **no post-drain task** exists | `main.tf:187-228` (`for_each_task`, `concurrency = 8`) |
| **Idle workers return BEFORE `drain_worker`** — they can never emit anything | `action_context.py:1293-1296` (`if not units: … return`) |
| Terraform: **"daily runs are tiny"** — so most workers are idle most days | `main.tf` comment at the sb360 task |
| `DrainSummary` is **in-memory, never persisted** | `drain.py:40-50` |
| **sb360 is NEVER enqueued**; it runs as one distributed cogroup job | `action_context.py:731-735`; `main.tf:236` |
| Drain workers get `--run-id` from a **preflight task value** | `main.tf:217` — the gate needs the same |
| Planner's SPADL leg is **match grain** (no `period_id`) | `:536-541` (tracking), `:611-616` (idsse) |
| Empty-actions unit returns **0 rows silently**; D3 skips it | `pipeline.py:403`; `completeness.py:141` |
| Per-unit write **is `replaceWhere`-scoped** → a zombie's late write is idempotent | `_period_replace_where`, `:1793`, `:1806` |
| `rows_written` is **already POST-WRITE outcome**, not intent | `:1801-1808` — `replace_where`, **no `row_count`** → ADR-045 counts the materialized slice |
| `analytics/` must not import `ingestion/` | `.importlinter` `analytics-isolation` (`type = forbidden`) |
| Drain's ports live in `drain.py` (not `ports.py`) | `WorkQueuePort` `:97` … |

**Measured live (2026-07-13):** **374 enumerable units** (skillcorner 220, gradientsports 134, idsse 14, metrica 6)
— matches the `374 units` the real preflight logged. **Zero-action units: 0** everywhere → the class the gate must
survive is **latent, not live**.

**Verification** (never `| tail` — it masks the exit code):
```bash
uv run ruff check src/ scripts/ && uv run pyright src/ && uv run lint-imports && uv run pytest src/tests/
```

---

## Task 1: Branch and green baseline

- [ ] **Step 1**
```bash
git fetch origin && git checkout main && git pull --ff-only origin main
git checkout -b feat/ac-unit-events-and-drain-gate
```
- [ ] **Step 2: baseline** — `uv run pytest src/tests/ -q > /tmp/baseline.txt 2>&1; echo "EXIT=$?" >> /tmp/baseline.txt`.
Must be `EXIT=0`.

> **Never run the suite in the background while editing.** During ADR-067 that produced 9 phantom failures (the
> `inspect.getsource` lockstep sentinels read files mid-write).

---

## Task 2: THE SPIKE — measure the commit cost (GATE)

**Runs on the feature branch (Task 1). Files:** `scripts/spike_unit_event_append_cost.py` (**delete before commit**).

D9 adds **374 `running` + 8 terminal-flushes + 8 `slice_completed` = 390 one-row commits** (M1: the flush and the
slice write are **separate**) from **8 concurrent writers** on one `_delta_log`. ADR-038's incident was **5**
writers with **disjoint** data racing one `_delta_log` version → S3 400 → **4 of 5 games silently failed**.
Disjointness is what this design has, and **partitioning does not shard the commit log** (ADR-038:61-63).

**THRESHOLD, PRE-REGISTERED — do not adjust after seeing the result:**

> **Route to per-worker tables (`…_unit_events_w{n}` + UNION view — ADR-038's own elimination route (b)) if EITHER:
> p50 per-append latency at 8-way concurrency > 750 ms, OR any append exhausts ADR-038's 10 retries in a
> 390-append simulation.**

- [x] **Step 1:** 8 concurrent writers × 49 one-row appends each, through `ingestion.utils.write_delta_table` (so
the ADR-038 retry path is exercised, not bypassed), as **8 separate Databricks tasks** (separate drivers — threads
in one JVM would not reproduce the multi-driver commit race).
- [x] **Step 2: RESULT (measured 2026-07-13, run `902922628896167`)**

| configuration | p50 / append | p95 | max | failures |
|---|---|---|---|---|
| **8 writers, concurrent** (the D9 shape) | **~9,700 ms** | 12.9–18.0 s | 14.3–21.8 s | **0** |
| **1 writer, uncontended** (control, run `893879540958635`) | **1,656 ms** | 9,454 ms | 9,454 ms | **0** |

Per-worker p50s at 8-way: 9317.8 · 10002.3 · 9738.9 · 9695.8 · 10020.1 · 9976.2 · 9296.0 · 9750.0 ms.

- [x] **Step 3: VERDICT — threshold breached 13×; ROUTE TO PER-WORKER TABLES** (owner-confirmed 2026-07-13).

The pre-registered threshold (**p50 > 750 ms at 8-way**) is breached by **~13×**. Zero appends exhausted the
retries — ADR-038's jittered backoff absorbed the contention into **latency**, not errors.

**The control proves the fix is not cargo-culting the rule.** Contention accounts for a **5.9× slowdown**
(1.66 s → 9.7 s), which is exactly what sharding the `_delta_log` removes. Had the uncontended cost also been
~9 s, per-worker tables would have fixed *nothing* and the real answer would have been fewer commits.

**But note the second, independent cost:** even uncontended, a one-row `write_delta_table` costs **1.66 s** —
the Spark-job floor (one full job per row). **No table topology can remove that.** Projected per full drain
(374 `running` ÷ 8 workers ≈ 47 each): single table ≈ **7.6 min**/worker; per-worker tables ≈ **1.3 min**/worker.

**Rejected alternative: batching the `running` events.** It would cut commits far more aggressively — but
`running`-before-processing **IS** the OOM-visibility guarantee (the entire reason D9 has its current shape).
Batching it makes an OOM'd worker's in-flight units invisible again. Not trading the feature away to save ~6 min
on a 5.5 h job.

---

## Task 3: D9 — event table + sink port + Delta impl

**Files:** `drain.py` (Protocol, beside the ports at `:97-105`) · `ingestion/action_context_queue.py`
(`_EVENT_COLUMNS`, `event_columns_sql`, `DeltaUnitEventSink`) · `scripts/migrations/2026-07-13-create-ac-unit-events.sql`
(**M4**) · tests `test_unit_event_sink.py`, `test_unit_event_schema_parity.py`.

- [ ] **Step 1: Failing tests**

```python
def test_event_columns_include_event_date_and_write_failures() -> None:
    """M4: `event_date` MUST exist as a column — the table is PARTITIONED BY it, so a DDL without it
    cannot execute. `write_failures` is populated ONLY on `slice_completed` rows: it is the sole channel
    by which fail-open unit-event losses reach the gate (which reads persisted tables only)."""
    from ingestion.action_context_queue import _EVENT_COLUMNS

    names = [c[0] for c in _EVENT_COLUMNS]
    for required in ("run_id", "worker_id", "provider", "match_id", "period", "state",
                     "started_at", "ended_at", "rows_written", "error", "write_failures", "event_date"):
        assert required in names, f"event schema missing {required}"


def test_event_ddl_matches_the_migration() -> None:
    """M4 + V4: mirror the queue's convention EXACTLY — REUSE its `_ddl_columns()` parser and compare
    ORDERED (name, type) tuples.

    A substring check (`assert name in migration`) is strictly weaker than the convention it claims to
    mirror: it passes on a WRONG TYPE, a WRONG ORDER, or a column that appears only in a COMMENT — so it
    would NOT reliably catch the very class it was added for (the missing `event_date`).
    """
    from tests._ddl import ddl_columns   # W5: SHARED helper, not an import from another test module

    ddl = ddl_columns(_MIGRATION.read_text(encoding="utf-8"))
    expected = [(name, sql_type) for name, sql_type, _ in _EVENT_COLUMNS]
    expected.append(("_ingested_at", "timestamp"))  # auto-added by write_delta_table
    assert ddl == expected


def test_parity_guard_FAILS_on_a_planted_violation() -> None:
    """THE GUARD RULE (§0b): prove the guard can fail.

    Plant a DDL with `event_date` REMOVED and a wrong type — assert the ordered-tuple comparison rejects it.
    A substring check (`assert name in migration`) passes on a wrong type, a wrong order, or a column that
    appears only in a COMMENT — i.e. it would NOT have caught the very defect (missing `event_date`) it was
    added for.
    """
    from tests._ddl import ddl_columns

    planted = "CREATE TABLE t (\n  run_id string,\n  worker_id bigint\n)"   # wrong type, missing event_date
    assert ddl_columns(planted) != [(n, t) for n, t, _ in _EVENT_COLUMNS]
```

> **W5 — extract the parser.** `_ddl_columns` currently lives inside `test_work_queue_schema_parity.py`. Importing
> it *from another test module* is fragile under pytest collection and creates a test→test dependency. Move it to
> **`src/tests/_ddl.py`** and have **both** parity tests import it — which also makes the shared parser itself
> testable.

- [ ] **Step 1b: V5 — `event_date` must be a TESTED artifact, not a blockquote**

`event_date` is `NOT NULL`, and `write_delta_table` auto-adds **only** `_ingested_at` (which is exactly why the
queue's parity test appends it by hand). Nothing else populates it → **the first production write fails on a
NOT-NULL partition column.** The sink's row construction is never exercised in CI (the drain tests inject a
recording fake), so this would surface **in production, not in the suite**.

**Make the row-builder pure and test it:**

```python
def _event_row(*, run_id, worker_id, provider, match_id, period, state,
               started_at, ended_at, rows_written, error, write_failures) -> dict:
    """Pure row builder — so the NOT-NULL contract is testable without Spark."""


def test_event_row_populates_every_NOT_NULL_column() -> None:
    """V5 + §0b. Assert every column `_EVENT_COLUMNS` marks NOT NULL is present and non-None — and that the
    guard FAILS on the planted omission (drop `event_date` -> the test must catch it)."""
    row = _event_row(...)
    for name, _type, nullable in _EVENT_COLUMNS:
        if not nullable:
            assert row.get(name) is not None, f"NOT NULL column {name} unpopulated"
```

- [ ] **Step 2: Run — expect ImportError.**

- [ ] **Step 3: `UnitEventSink` Protocol in `drain.py`** — with **four** methods (M1: the flush and the slice write
are separate, because they have **opposite failure policies**):

```python
class UnitEventSink(Protocol):
    """Persists per-unit lifecycle events (D9).

    THREE write policies, and the differences are load-bearing:

    * ``unit_started`` — per-unit, **fail-open**, counted. Written BEFORE processing: it is the entire
      OOM-visibility guarantee (an OOM-killed driver's in-flight units stay distinguishable from units
      never begun). Reconstructible from nothing, so it cannot be batched.
    * ``unit_finished`` — buffered; **fail-open**, counted. Terminal state is reconstructible (rows exist
      in results; failures fail the task), so it may be batched.
    * ``flush_terminals`` — writes the buffer. **FAIL-OPEN** and counted: terminals are unit events, and
      ADR-002 says telemetry loss must never become data loss. (If this were fail-loud, the UNVERIFIABLE
      verdict — whose whole purpose is *lost unit events* — could never be reached.)
    * ``slice_completed`` — **FAIL-LOUD**, carries ``write_failures``. It is the ONLY way that count can
      reach the gate, which runs in a DIFFERENT TASK and reads persisted tables only. If it cannot land,
      the evidence is unusable → the worker task must fail.
    """

    def unit_started(self, run_id: str, worker_id: int, unit: WorkUnit) -> None: ...
    def unit_finished(self, run_id: str, worker_id: int, unit: WorkUnit, *,
                      state: str, rows_written: int | None, error: str | None) -> None: ...
    def flush_terminals(self) -> None: ...
    def slice_completed(self, run_id: str, worker_id: int) -> None: ...
    @property
    def write_failures(self) -> int: ...
```

- [ ] **Step 4: `_EVENT_COLUMNS` + migration + `DeltaUnitEventSink`** (mirror `_QUEUE_COLUMNS` / `queue_columns_sql`,
`:50-72`):

```python
_EVENT_TABLE = "action_context_unit_events"

_EVENT_COLUMNS: list[tuple[str, str, bool]] = [
    ("run_id", "string", False),
    ("worker_id", "int", False),
    ("provider", "string", False),
    ("match_id", "string", False),
    ("period", "int", True),           # NULL for sb360 (match-grain; it exits the per-period drain)
    ("state", "string", False),        # running | succeeded | failed | timed_out | slice_completed
    ("started_at", "timestamp", True),
    ("ended_at", "timestamp", True),
    ("rows_written", "bigint", True),
    ("error", "string", True),
    ("write_failures", "int", True),   # `slice_completed` ONLY
    ("event_date", "date", False),     # M4: partition key — MUST be a real column
]
```

### ⚠️ TOPOLOGY: PER-WORKER TABLES (decided by the Task 2 spike — 13× over threshold)

**One table per writer**, which is ADR-038's own elimination route (b) — *"split into multiple tables"* — the only
thing that **removes** `_delta_log` contention rather than mitigating it:

```
observability.action_context_unit_events_w0 .. _w7      (one per for_each worker)
observability.action_context_unit_events_sb360           (the sb360 sentinel writer)
observability.action_context_unit_events                 (VIEW: UNION ALL of the above)
```

- Each writer touches **only its own table** → separate `_delta_log` → contention is **structurally impossible**,
  not merely retried. Measured: **9.7 s → 1.66 s** per append.
- **The gate is UNCHANGED**: it reads the **view**, which has the same columns as the single table would have.
  (This is why the topology decision could be deferred behind a spike without reshaping the gate.)
- `ensure_table` creates the N tables **and** the view. The view is `CREATE OR REPLACE VIEW ... AS SELECT * FROM
  _w0 UNION ALL ... UNION ALL SELECT * FROM _sb360`.
- **The invariant test's allowlist (§0/Task 8) must reference the VIEW name via the module constant**, not a
  literal — the per-worker table names must not leak into the gate.

`PARTITIONED BY (event_date)` — **for retention (a partition drop, not a tombstone-generating DELETE) and
read-pruning ONLY. NOT a contention control** (ADR-038; and now measured — partitioning would not have helped,
splitting tables did).

Also create the **run-scoped status view** (spec §3) — the human-facing artifact:

```sql
CREATE OR REPLACE VIEW {catalog}.observability.action_context_unit_status AS
SELECT q.run_id, q.worker_id, q.provider, q.match_id, q.period,
       COALESCE(e.state, 'queued') AS state,      -- no event => never started
       e.started_at, e.ended_at, e.rows_written, e.error
FROM {catalog}.observability.action_context_work_queue q
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY run_id, provider, match_id, period   -- RUN-SCOPED: units are re-enqueued across runs;
        ORDER BY _ingested_at DESC                         -- a latest-across-all-runs join would misattribute
    ) AS rn                                                -- a prior terminal to a fresh unit
    FROM {catalog}.observability.action_context_unit_events
    WHERE state <> 'slice_completed'
) e ON e.run_id = q.run_id AND e.provider = q.provider
   AND e.match_id = q.match_id AND e.period <=> q.period AND e.rn = 1;
```

> That `ORDER BY … DESC` is the **same tiebreaker-less shape** that made `fct_tracking_context` non-deterministic
> (see Deferred). It is safe here **only** because a unit emits at most one `running` and one terminal. If that ever
> changes, add a deterministic tiebreaker. **Do not copy this pattern blindly.**

- [ ] **Step 5: Verify.**

---

## Task 4: Wire the sink into the drain — including the IDLE worker (P4)

**Files:** `drain.py` (`drain_worker`) · `ingestion/action_context.py` (`main_drain_worker` **`:1293-1296`**) ·
test `test_drain_events.py`.

- [ ] **Step 1: Failing tests**

```python
def test_running_is_written_BEFORE_the_unit_is_processed() -> None:
    """The OOM-visibility guarantee: a unit that RAISES must still have its `running` event."""


def test_idle_worker_STILL_emits_slice_completed() -> None:
    """P4 — the gate misfires on every small daily run without this.

    `action_context.py:1293-1296` returns BEFORE `drain_worker` when a worker has no units. Terraform's own
    comment says "daily runs are tiny": on a 3-unit run, FIVE of eight workers are idle. If they emit nothing,
    the gate sees them as DEAD and cries wolf on a healthy run — the muting failure this design exists to
    prevent, arriving through the front door.
    """


def test_terminal_flush_is_fail_open_but_slice_completed_is_fail_loud() -> None:
    """M1 — opposite policies, so they MUST be separate writes.
    Flush fails  -> drain continues, ERROR log, write_failures increments (=> UNVERIFIABLE later).
    Slice fails  -> worker task RAISES (the gate could not trust its own inputs otherwise).
    """
```

- [ ] **Step 2: Run — expect failures.**

- [ ] **Step 3: Implement.** `drain_worker(..., sink: UnitEventSink)`; in the loop: `sink.unit_started(...)` **before**
`watchdog.run(...)`; `sink.unit_finished(...)` in each of the three outcomes; then **`sink.flush_terminals()`
(fail-open)**, then **`sink.slice_completed(...)` (fail-loud)**.

**And in `main_drain_worker` (`:1293-1296`) — the idle path must ALSO emit:**

```python
    units = queue.units_for_worker(run_id, worker_id)
    if not units:
        task_logger.info("Drain worker %d: no units assigned for run %s -- exiting", worker_id, run_id)
        # P4: an IDLE worker must still say it ran. Daily runs are tiny, so most workers are idle most
        # days; a silent idle worker is indistinguishable from a DEAD one, and the gate would cry wolf.
        sink = DeltaUnitEventSink(spark, args.catalog)
        sink.slice_completed(run_id, worker_id)
        return
```

> Do **not** change the existing per-unit `except` semantics: the swallow is deliberate (one bad unit must not
> destroy a 5.5 h drain) and `raise_on_failed_units` (ADR-067 D2) still fails the task at the end.

- [ ] **Step 4: Verify.**

---

## Task 5: Planner SPADL leg → period grain, + enqueue round-trip

**Files:** `action_context.py` (`_find_tracking_new_period_pairs` `:536-551`; `_find_idsse_new_period_pairs`
`:611-616`; preflight) · test `test_planner_grain.py`.

> ## ⚠️ W2 — READ THIS BEFORE WRITING THE TESTS
>
> This is **the riskiest edit in the PR**, and the defence I previously claimed **does not exist**:
>
> **The M2 diagnostic CANNOT catch an under-enumerating planner, because it re-runs the SAME function.** If the
> period-grain join matches nothing, `_find_*_new_period_pairs` returns ∅ — so `enqueued == 0` **and**
> `remaining == 0`, and the tooth (`enqueued == 0 AND remaining > 0`) **never fires**. The diagnostic is
> structurally blind to the one failure it was added for. Its real job is the *empty-queue-with-work-remaining*
> case caused by something **other** than the planner (a failed enqueue).
>
> **And the CI net had the same hole:** the test asserted only the NEGATIVE ("a zero-action period must not be
> enumerated") — which a join matching **nothing at all** passes trivially.
>
> So the only protection was a **one-time manual 374-count check**. That is not a guard; it is a hope.
> **The tests below must be two-sided, and must include a dtype/encoding variant** — because
> `spadl.period_id` vs `tracking.period` disagreeing in encoding is *precisely* the failure mode (the repo's own
> ADR-019 canonical-id class), and it is what would make the join silently match nothing.

- [ ] **Step 1: Failing tests — TWO-SIDED (W2)**

```python
def test_period_WITH_actions_IS_still_enumerated() -> None:
    """W2 — the POSITIVE case. A join that matches NOTHING passes the negative test trivially; without this,
    a silently-dead planner ships green and the drain does nothing for a week."""


def test_period_with_frames_but_ZERO_actions_is_NOT_enumerated() -> None:
    """The zero-action class: enumerated today, processed to `_empty_result()` (0 rows), never lands in
    results, RE-ENUMERATED FOREVER. Latent (measured: 0 live), not safe."""


def test_period_encoding_variants_still_join() -> None:
    """W2 — the actual failure mode. `spadl.period_id` and `tracking.period` must join across dtype/encoding
    variants (string vs bigint). This is the repo's ADR-019 canonical-id class, and it is what would make the
    new period-grain join match NOTHING while every other test stays green.

    Both sides are `.cast(...)` in the planner — this test proves the casts actually reconcile.
    """


def test_enqueue_round_trip_count_is_asserted() -> None:
    """Planner discovered N, enqueue persisted M < N -> the gate compares queue vs events and sees a
    self-consistently SHORT run."""
```

- [ ] **Step 2: Run — expect FAIL** (the zero-action and encoding cases).

- [ ] **Step 3: Implement** — both planners:

```python
    spadl_df = (
        spark.table(spadl_table)
        .filter(F.col("data_source") == provider)
        .select(
            F.col("match_id_native").cast("string").alias("_mid"),
            F.col("period_id").cast("bigint").alias("_period"),   # PERIOD GRAIN (was match-only)
        )
        .distinct()
    )
    new_df = (
        tracking_df.join(spadl_df, ["_mid", "_period"], "inner")   # was: on "_mid"
        .join(results_df, ["_mid", "_period"], "left_anti")
    )
```

- [ ] **Step 4: HARD GATE — re-run the live count.** This is the **riskiest edit in the PR** and the gate is
structurally blind to its failure mode (M2): if `spadl.period_id` and `tracking.period` disagree in encoding for
**any** provider, the new join silently matches **nothing**, the planner enumerates **zero** units, and D8 returns
COMPLETE while the drain does nothing.

Run the spec §2 query. **The enumerable set must remain exactly 374** (skillcorner 220, gradientsports 134, idsse 14,
metrica 6) — there are **no zero-action units today**, so the fix must remove none. **Any movement = a
period-encoding mismatch. STOP.**

> **AMENDED AT EXECUTION (2026-07-13).** The spec §2 query as written includes the **results anti-join**, i.e. it
> measures the *unprocessed remainder*. Post-ADR-067 the drain is fully caught up, so that query now returns
> **`0` for every provider both before and after the edit** — it would have passed a **completely dead join**,
> which is precisely the W2 false-pass this gate exists to catch. The quantity the 374 actually refers to, and the
> only one that discriminates a dead join, is the **enumerable universe** (`tracking ⋈ spadl`, *before* the
> anti-join). That is what was run.
>
> **Result: HELD at exactly 374** — skillcorner 220, gradientsports 134, idsse 14, metrica 6; **zero** units
> removed by the grain fix, matching §2's "zero-action units: 0". `period_id`/`period` reconcile under the casts
> for every provider.
>
> Incidental: gradientsports has **138** SPADL (match, period) pairs vs **134** tracking pairs — the 4 known GS
> extra-time periods (events, no ET frames). Tracking is the driving table, so these were never enumerable, before
> or after. Not a regression; consistent with the Deferred list.

---

## Task 6: sb360 emits events (it is otherwise UNGATED)

**Files:** `action_context.py` (`main_statsbomb` / `_process_statsbomb_matches`) · test `test_sb360_events.py`.

sb360 is **never enqueued** (`:731-735`) — so the queue says nothing about it, and a queue-only gate leaves
statsbomb **completely unchecked**.

> ## ⚠️ W3 — THE PRODUCER CONTRACT (this is the THIRD recurrence: P2 → V2 → W3)
>
> The gate's rule 0 looks for the sb360 **worker sentinel**. If this task emits `worker_id = 0` (the obvious
> default), rule 0 finds no sentinel and returns **`DRAIN_FAILED` on EVERY run** — a gate that cries wolf
> permanently, from day one.
>
> **The sentinel is a SHARED CONSTANT that BOTH sides import — and its HOME is load-bearing (X1).**
>
> **It must live in `analytics/`, NOT `ingestion/`.** The pure gate is `analytics/action_context/drain_gate.py`,
> and **`analytics/` cannot import `ingestion/`** (`.importlinter` `analytics-isolation`; verified — analytics
> imports ingestion **nowhere**). A constant in `ingestion/action_context_queue.py` is therefore **unimportable by
> the consumer**, and the implementer would either hardcode `-1` in the gate (**drift returns — the exact defect
> W3 fixed**) or add the import and **fail `lint-imports` at Task 9**.
>
> `ingestion` already imports *from* `analytics.action_context.drain` (`action_context.py:27` —
> `WATCHDOG_BUDGET_S`, `DrainSummary`, `assign_workers`, `drain_worker`). So:
>
> ```python
> # src/analytics/action_context/drain.py   <-- BOTH sides can import this; the boundary holds
> SB360_WORKER_ID = -1   # sb360 EXITS the per-match drain (ADR-058): no queue rows, no worker_id. It needs a
>                        # sentinel to be an EXPECTED WORKER in the gate's rule 0. `_EVENT_COLUMNS` has
>                        # worker_id NOT NULL, so -1 (never NULL) is the right choice.
> ```
>
> Task 6 (producer) emits it on **every** sb360 event; Task 7 (consumer) puts it in the expected-worker set —
> **by importing the constant, never by writing the literal `-1`.**

- [ ] **Step 1: Failing test**

```python
def test_sb360_emits_the_SHARED_sentinel_worker_id() -> None:
    """W3 — the producer half of the contract. If sb360 emits worker_id=0, the gate looks for the sentinel,
    finds none, and returns DRAIN_FAILED on EVERY run."""
    # Y2 / X1: the sentinel lives in ANALYTICS, because the pure gate (analytics/…/drain_gate.py) CANNOT
    # import ingestion (.importlinter `analytics-isolation`). Importing it from `ingestion` here would send
    # the implementer to the one home that makes the consumer unable to read it.
    from analytics.action_context.drain import SB360_WORKER_ID

    # every sb360 event (running, terminals, slice_completed) carries worker_id == SB360_WORKER_ID
    assert all(e["worker_id"] == SB360_WORKER_ID for e in emitted_events)
    assert SB360_WORKER_ID == -1


def test_sb360_emits_running_terminal_and_slice_completed() -> None:
    """`running` per discovered match (`period` NULL — sb360 is match-grain), a terminal per match, one
    `slice_completed`."""


def test_sb360_with_NO_matches_STILL_emits_slice_completed() -> None:
    """X2 — THIS IS P4, FOR sb360, AND IT IS THE COMMON CASE.

    `main_statsbomb` (action_context.py:1197-1199) early-returns on `if not ids:` BEFORE any processing. On any
    run with no new sb360 matches — the ordinary daily shape — statsbomb would emit NOTHING: no `running`, no
    terminals, no `slice_completed`. Rule 0 expects the sentinel's `slice_completed` unconditionally (the sb360
    task is unconditional in the DAG), so the gate would return DRAIN_FAILED **every quiet day** — crying wolf on
    the most common run there is, which is the muting failure P4 was raised to prevent.
    """
```

**The producer fix — mirror P4 exactly** (`action_context.py:1197-1199`):

```python
    if not ids:
        task_logger.info("No pending statsbomb sb360 matches — skipping")
        # X2: sb360 must still SAY IT RAN. Zero discovered matches is the COMMON daily case; a silent sb360 is
        # indistinguishable from a DEAD one, and rule 0 would return DRAIN_FAILED every quiet day.
        sink.slice_completed(run_id, SB360_WORKER_ID)
        return
```

**Third artifact (X2b) — `main.tf`:** the sb360 task currently takes **no `--run-id`** (only `--catalog`,
`--schema`, `--max-units`), so its events could not carry the run the gate verifies. Add the same task value the
drain workers get (`main.tf:217`):

```hcl
        "--run-id", "{{tasks.preflight_action_context.values.action_context_run_id}}",
```

**FOURTH artifact (Y1) — the argparse that must ACCEPT it.** Verified: `main_statsbomb`'s
`parse_ingestion_args` declares **only `--max-units`** (`action_context.py:1169-1175`). Terraform passing a flag and
Python accepting it are a **producer/consumer pair** (§0a) — adding the flag alone makes the task die on
`unrecognized arguments` **on the first run after apply**, and the gate then piles `DRAIN_FAILED` on top of it.

```python
    args = parse_ingestion_args(
        "Compute action context for statsbomb (sb360) — single distributed cogroup job",
        extra_args=[
            ("--max-units", {"type": str, "default": "", "help": "Cap the number of sb360 matches (empty = all)."}),
            # Y1/X2b: sb360's events MUST carry the run the gate verifies. Without this the flag added to
            # main.tf is an unrecognized argument and the task fails immediately.
            ("--run-id", {"type": str, "required": True,
                          "help": "Preflight run id — sb360's unit events must carry the run D8 verifies."}),
        ],
    )
```

- [ ] **Step 2: Implement — and note the shape.** `_process_statsbomb_matches` is **one distributed
`applyInPandas` job** (ADR-058): **the driver never observes per-match completion**, so there is no per-match loop
to hook. Terminals must be **derived post-hoc** from the job's output (group written rows by `match_id`). Three
commits total: `running` batch (before), terminal batch (after), `slice_completed`. Single writer → no contention.

- [ ] **Step 3: Verify.**

---

## Task 7: D8 — the gate

**Files:** create `analytics/action_context/drain_gate.py` (**pure**) · create `ingestion/action_context_gate.py`
(entry point) · tests `test_drain_gate.py`, `test_drain_gate_entrypoint.py`.

- [ ] **Step 1: Failing tests — AT THE GATE'S LAYER** (rows in tables, not fakes in memory)

```python
def test_DRAIN_FAILED_reports_and_does_NOT_raise() -> None:
    """P1 — the rule the plan previously OMITTED, which made the gate contradict spec §6.

    Under run_if=ALL_DONE the gate runs even when the drain FAILED. An OOM-killed worker leaves `running`
    events with no terminal. Without this rule, "unit with no terminal" -> INCOMPLETE -> RAISE, and the gate
    masks the drain's real exception with its own. The job already failed; the gate's job here is to SAY WHAT
    DIED, not to fail it again."""


def test_idle_workers_do_not_look_dead() -> None:
    """P4 — only 2 of 8 workers have queue rows -> COMPLETE, not DRAIN_FAILED.
    The EXPECTED WORKER SET IS DERIVED FROM THE QUEUE (`SELECT DISTINCT worker_id ... WHERE run_id`).
    NEVER hard-code 8."""


def test_sb360_units_are_GATED() -> None:
    """P2 — sb360 is NEVER enqueued, so a queue-only expectation set never examines it. The expected set is a
    UNION: queue rows (drain units) + sb360 `running` events (its persisted queue-equivalent)."""


def test_INCOMPLETE_raises_and_names_the_units() -> None: ...

def test_lossy_worker_does_not_mute_a_CLEAN_worker() -> None:
    """V1 — taint is PER-WORKER. Worker 1 lost an event; worker 5 has a genuine missing terminal.
    -> INCOMPLETE (raise). A run-scoped UNVERIFIABLE would have suppressed a real accusation."""

def test_lossy_but_NO_anomalies_is_COMPLETE_not_UNVERIFIABLE() -> None:
    """W1 — `all([])` is True. Every unit terminal, every count matching, worker 3 merely lost one `running`
    event -> COMPLETE (with a warning). Without the non-empty clause this returns UNVERIFIABLE, and since
    SOME loss is the expected case at ~390 fail-open commits, the gate would be muted by its own success."""

def test_timed_out_is_excused() -> None: ...

def test_timed_out_WITH_rows_present_is_LEGAL() -> None:
    """The watchdog ABANDONS live threads (drain.py:157); a zombie can write rows AFTER its `timed_out` event.
    The write is replaceWhere-scoped -> idempotent. The cross-check must SKIP timed_out."""

def test_DRAIN_FAILED_report_splits_completed_from_in_flight() -> None:
    """V6 — terminals are BATCHED, so an OOM-killed worker flushes NONE of them: its units, INCLUDING THE ONES
    THAT SUCCEEDED AND WROTE ROWS, all look like `running` with no terminal. A naive report names them all
    'in-flight' -> the ALL_DONE payoff ships INACCURATE.

    The licence to batch was that terminal state is RECONSTRUCTIBLE from results. So reconstruct it:
    dead worker, two started units — one with rows in results, one without -> assert they are classified as
    'completed (terminal lost)' and 'genuinely in-flight' respectively."""

def test_diagnostic_raises_on_planner_collapse() -> None:
    """M2 — enqueued == 0 AND remaining > 0. NOTE (W2): this canNOT catch an UNDER-enumerating planner, because
    `remaining` re-runs the SAME function — a broken planner returns 0 for both. Its real job is an empty queue
    caused by something OTHER than the planner (e.g. a failed enqueue)."""

def test_diagnostic_raises_when_a_SUCCEEDED_unit_is_still_remaining() -> None:
    """V7 — the independent WRITE-LANDED check: we said the unit succeeded, and the planner still sees it as
    unwritten => its rows did not land. Sound only because Task 5 removes the zero-action class — hence it sits
    BEHIND Task 5's 374-count hard gate."""


# entry-point layer — where the `raise` actually LIVES (the pure logic only returns a verdict):
def test_entrypoint_raises_on_INCOMPLETE() -> None: ...
def test_entrypoint_does_NOT_raise_on_DRAIN_FAILED_or_UNVERIFIABLE() -> None: ...
```

- [ ] **Step 2: Run — expect ImportError.**

- [ ] **Step 3: Implement the pure verdict logic** (`drain_gate.py` — stdlib/pandas only; no Spark, no `ingestion`).

```python
class Verdict(str, Enum):
    COMPLETE = "COMPLETE"
    DRAIN_FAILED = "DRAIN_FAILED"    # P1 — report, do NOT raise
    INCOMPLETE = "INCOMPLETE"        # raise
    UNVERIFIABLE = "UNVERIFIABLE"    # report, do NOT raise
```

**EXPECTED WORKERS** = `DISTINCT worker_id` from the queue for `run_id` (**never hard-code 8** — P4) **∪ the sb360
sentinel**, obtained as `from analytics.action_context.drain import SB360_WORKER_ID` — **import the constant, never
write the literal `-1`** (Y2/X1: writing the literal is exactly the drift W3 was raised to kill, and it is the path
an implementer takes when the constant sits somewhere the gate cannot import from).

> **V2 — sb360 must be an expected WORKER, not merely expected UNITS.** P2 fixed the *unit* level (rule 2's expected
> set includes sb360's `running` events). But rule 0's worker set comes from the **queue**, and **sb360 has no queue
> rows and no `worker_id`** — so its `slice_completed` was written by Task 6 and **read by nothing**. If the sb360
> task dies **before** emitting its `running` events, it contributes **zero** expected units, rule 0 misses no
> worker, and the gate returns **`COMPLETE` while statsbomb did nothing at all**. The sb360 task is unconditional in
> the DAG, so it is **always** expected. (This is P2's exact shape, one level down: fixed in the producer, not in the
> consumer.)

**TAINT IS PER-WORKER, NOT PER-RUN (V1).** A unit belongs to a worker; a worker's lost events taint **that worker's**
units only.

**Rules, in order:**

**EVALUATION ORDER (state it, or the rule table reads as circular and the implementer will guess):**

```
anomalies = [missing-terminal units]  +  [succeeded units whose persisted count != rows_written]
anomalies are then PARTITIONED BY WORKER; rules 0 -> 4 are applied in order.
```

| # | condition | verdict | raises? |
|---|---|---|---|
| **0** | an **expected worker** (incl. the **sb360 sentinel**, imported — never the literal `-1`) has **no `slice_completed`** | `DRAIN_FAILED` | **no** — the job already failed; **say what died** (P1). **The report must ALSO name any clean-worker anomalies**: rule 0 pre-empts rules 2–3, so a run where one worker died *and* another has a genuine missing terminal would otherwise hide the second defect until the next run. |
| **1** | **`anomalies` is NON-EMPTY** *and* **every** anomaly sits inside a **lossy** worker (`write_failures > 0`) | `UNVERIFIABLE` | **no** |
| **2** | a **clean** worker (`write_failures = 0`) has an **expected** unit with **no terminal** — expected units = **queue rows ∪ sb360 `running` events** (P2) | `INCOMPLETE` | **yes**, naming units |
| **3** | a **clean** worker's `succeeded` unit has persisted row count ≠ `rows_written` | `INCOMPLETE` | **yes** |
| **4** | — | `COMPLETE` (warn if any losses) | no |

> **W1 — rule 1 MUST require at least one anomaly. `all([])` is `True`.** Without the non-empty clause, a run where
> every unit has its terminal, every count matches, and worker 3 merely lost one `running` event is
> **vacuously** "every anomaly inside a lossy worker" → **`UNVERIFIABLE` instead of `COMPLETE`**. With ~390
> fail-open commits on a contended `_delta_log`, *some* loss is the **expected** case — so the gate would return a
> non-verdict most days and be muted. That is V1's disease, re-acquired one layer down.
>
> **No anomalies + losses → `COMPLETE`, with the loss count as a WARNING.** That is honest and safe: a lost
> *terminal* would itself have produced an anomaly, so "no anomalies despite losses" means only `running` events
> were lost — which costs OOM-visibility, not correctness.

> **V1 — why the taint must be per-worker.** A run-scoped `UNVERIFIABLE` means **one lost event on worker 1
> suppresses a genuine silent-skip accusation about worker 5**. And this design runs **~390 fail-open one-row commits
> across 8 concurrent writers** on the `_delta_log` surface ADR-038 proved is contended — the spike exists precisely
> because losses are *plausible*. If lossy runs are common, `UNVERIFIABLE` becomes the **common verdict**, and a gate
> that never accuses is a muted gate: the failure this whole design exists to prevent, arriving by a third route.
> Per-worker taint keeps M1's principle (*no signal must not masquerade as negative signal*) **and** keeps the gate's
> teeth on the seven workers whose evidence is intact.

`timed_out` is excused everywhere and **skipped by rule 3**.

**V6 — the `DRAIN_FAILED` report must not lie.** Terminals are **batched**, so an OOM-killed worker flushes
**none** of them: its units — *including the many that succeeded and wrote rows* — all look like `running` with no
terminal, and a naive report would name them all "in-flight". That is the ALL_DONE payoff shipping **inaccurate**.
The design's own licence to batch was that *"terminal state is reconstructible — rows exist in results"*. **So
reconstruct it:** the gate already reads per-unit result counts (rule 3). Split a dead worker's started units into
**"completed (rows present, terminal lost)"** vs **"genuinely in-flight (no rows)"**.

**Planner diagnostic (M2 + V7).** Re-run `_find_*_new_period_pairs`; **report** `remaining`. It **RAISES** on
either:
- `enqueued == 0 AND remaining > 0` — total planner collapse ("the planner stopped seeing work" is not a backlog); **or**
- **V7:** a unit with a **`succeeded` event this run that is STILL in `remaining`** — we said it succeeded and the
  planner still sees it unwritten, so **its rows did not land**. This is the independent *write-landed* check, and it
  is sound **only because Task 5 removes the zero-action class** — hence it sits **behind** Task 5's 374-count hard
  gate.

- [ ] **Step 4: Entry point** — reads queue + events for `run_id`, reads the per-unit result counts **scoped to the
run's `match_id`s** (unscoped, it scans the whole mart), calls the pure logic, logs the verdict, raises on
INCOMPLETE only.

- [ ] **Step 5: Verify.**

---

## Task 8: Terraform + the new-task checklist + the §0 invariant test (AST)

**Files:** `main.tf` · `pyproject.toml` (`[project.scripts]`) · `workflow-cards/wf-action-context.yaml` ·
`dbt_project/seeds/task_workflow_mapping.csv` · test `test_gate_inputs_invariant.py`.

- [ ] **Step 1: The invariant test — AST, not substring (P3)**

> **V3 — the sketched AST test was VACUOUS, and this is the sharpest finding in the review.** The repo does **not**
> pass string literals to `spark.table`: `action_context_queue.py:186` is **`self._spark.table(self._table)`** — an
> **instance attribute**, assembled from an f-string over module constants. A gate written in house style contains
> **zero string literals at the call site**, so a literal-collecting walker gathers **∅**, `∅ ⊆ ALLOWED` holds, and
> the guard **passes no matter what the gate reads** — including a fourth table, i.e. the very thing §0 forbids.
> The guard added to enforce §0 was itself an instance of the class §0 exists to kill.

```python
_ALLOWED_TABLES = {_QUEUE_TABLE, _EVENT_TABLE, "spadl_action_context"}   # module constants, never literals
_ALLOWED_TASK_VALUES = {"run_id", "catalog"}                            # PARAMETERS, never evidence
_GATE = Path(__file__).resolve().parents[3] / "ingestion" / "action_context_gate.py"


def _gate_violations(src: str) -> list[str]:
    """Return every §0 violation in the gate's source. FORBID THE SHAPES, don't collect the spellings.

    Resolves module-level table constants and f-string parts, so `self._table = f"{c}.observability.{_X}"`
    followed by `spark.table(self._table)` is caught -- which a literal-collecting walker cannot see.
    """
    tree = ast.parse(src)
    bad: list[str] = []
    # 1. every spark.table(...) / spark.read... argument must resolve to an allowlisted constant
    # 2. dbutils.jobs.taskValues.get(key=...) only for _ALLOWED_TASK_VALUES  (a task value is the
    #    idiomatic way to smuggle EVIDENCE across tasks -- it is exactly where instance #4 will come from)
    # 3. no import from analytics.action_context.drain (DrainSummary / in-memory state lives there)
    return bad


def test_gate_evidence_comes_only_from_allowlisted_tables() -> None:
    assert _gate_violations(_GATE.read_text("utf-8")) == []


def test_invariant_guard_FAILS_on_planted_violations() -> None:
    """THE GUARD RULE (§0b). An invariant guard that has never failed is not a guard.

    Plant BOTH shapes the real defect took, and assert the guard rejects each:
      (a) a FOURTH table read via an attribute (the house style that made the literal-walker vacuous);
      (b) EVIDENCE pulled from a Databricks task value (the next instance, and the idiomatic way to do it).
    """
    planted_table = (
        "class G:\n"
        "    def __init__(self, spark, catalog):\n"
        "        self._t = f'{catalog}.gold.fct_action_context'\n"   # a FOURTH table
        "    def run(self):\n"
        "        return self._spark.table(self._t)\n"
    )
    planted_taskvalue = (
        "def run(dbutils):\n"
        "    n = dbutils.jobs.taskValues.get(taskKey='compute', key='failed_units')\n"  # EVIDENCE, not a param
        "    return n\n"
    )
    # W6: the guard's contract has THREE clauses; plant all three, or one branch is unproven (§0b applied to
    # two-thirds of a guard is not §0b).
    planted_import = (
        "from analytics.action_context.drain import DrainSummary\n"   # in-memory state — the ORIGINAL defect
        "def run(s: DrainSummary):\n"
        "    return s.timed_out\n"
    )
    assert _gate_violations(planted_table), "guard missed a fourth table read via an attribute"
    assert _gate_violations(planted_taskvalue), "guard missed evidence smuggled through a task value"
    assert _gate_violations(planted_import), "guard missed an import of in-memory drain state"
```

- [ ] **Step 2: Terraform task — WITH `--run-id` (M3)**

```hcl
  task {
    task_key        = "verify_action_context_drain"
    timeout_seconds = 3600
    max_retries     = 0
    run_if          = "ALL_DONE"      # deliver D9's OOM payoff even when the drain FAILED (spec §6)

    depends_on { task_key = "compute_action_context" }
    depends_on { task_key = "compute_action_context_statsbomb" }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "verify_action_context_drain"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        # M3: without this the gate cannot know WHICH run to verify. "Latest run_id" is a race, not a fix.
        "--run-id", "{{tasks.preflight_action_context.values.action_context_run_id}}",
      ]
    }
    environment_key = "analytics"
  }
```
Task block goes in **alphabetical** position (`test_workflows_tf_ordering.py`).

- [ ] **Step 3: Checklist** — entry point in `pyproject.toml`; a phase in the workflow card; the
`task_workflow_mapping.csv` row. Then:
```bash
uv run pytest src/tests/ -q -k "card_parity or workflows_tf_ordering or workflow_dag or gate_inputs"
```

- [ ] **Step 4: `prune` ordering** — assert `prune` never removes the **current** `run_id` before the gate reads it.

---

## Task 9: ADR, full verification, commit (APPROVAL GATES)

- [ ] **Step 1: ADR-068.** Must record:

- **The GUARD RULE (§0b)** — *every guard must be shown to FAIL on a planted violation of the thing it guards,
  including guards added by a review.* This is the generalisation of §0, and it is the load-bearing lesson: the
  "wrong artifact" class was caught by §0's traceability table, then **recurred inside the guards themselves** (an
  AST test that could not fail; a parity test that checked a substring). One extra test per guard is the only thing
  that catches this.
- **THE sb360 SEAM (§0c) — the lesson of this entire review.** sb360 produced a defect in **every single round**
  (P2 → V2 → W3 → X1/X2 → Y1/Y2) and **never the same one twice**: read but not emitted; emitted but not read;
  sentinel in the consumer only; sentinel in a module the consumer cannot import; no emit on the empty path; a
  terraform flag its argparse rejects. The constant is not any individual fix — it is that **a second producer with a
  different lifecycle silently falls outside every rule written while looking at the first one.** A pair rule catches
  rule/test pairs; it does not catch a second producer. §0c's four-question sweep does — *and it must be run over the
  code blocks, not just the narrative*, which is exactly how Y1/Y2 slipped through a plan whose prose already had
  them right.
- The corrected **§0 invariant**: evidence from allowlisted **tables**; task values are **parameters, never
  evidence** — and *why* (the same in-memory-input defect was introduced twice, then a third time as a spec-only fix).
- **Three write policies** — per-unit `running` (fail-open, unbatchable: it is the OOM guarantee); batched terminals
  (fail-open — if the flush were loud, `UNVERIFIABLE`, whose purpose is *lost unit events*, could never be reached);
  `slice_completed` (**fail-loud**, carries `write_failures` — the only channel by which loss reaches a gate that
  reads persisted tables only).
- **The idle worker's `slice_completed` is fail-loud on purpose** (minor, but write it down or someone will
  "optimise" the emit away): it is what lets the gate distinguish *"worker ran, queue read returned nothing"* →
  **INCOMPLETE, raise** (the silent-skip class!) from *"worker died"* → **DRAIN_FAILED, report**. Delete the emit and
  the gate's core accusation silently downgrades to a report.
- **Four verdicts**, with **per-worker taint** (V1): `DRAIN_FAILED` reports (the job already failed — say what died);
  `UNVERIFIABLE` only when every anomaly sits inside a lossy worker, so one lost event cannot mute the gate for the
  other seven.
- **The planner grain fix** + the **374-count hard gate**, and the two raising diagnostics (planner collapse; a
  `succeeded` unit still in `remaining` — its rows did not land).
- **Partitioning is NOT a contention control** (ADR-038: `_delta_log` serialization is inherent to a single table) +
  the spike's **measured numbers**.
- `run_if = ALL_DONE`, and sb360 as an **expected worker** (a dead sb360 task must not yield `COMPLETE`).

- [ ] **Step 2: Full gates.** ruff · ruff format · pyright · lint-imports · **full pytest** — capture `EXIT=` for each;
all must be 0. **Delete the spike script.**

- [ ] **Step 3: Wheel bump.** New entry point → the wheel MUST move or the task cannot start. `pyproject.toml`
version, then `uv run python scripts/bump_wheel.py`. **`bump_wheel --check` only detects DRIFT — it will NOT catch a
missing bump.** (ADR-067 shipped inert without it.)

- [ ] **Step 4: STOP — explicit approval to commit.** Plan approval is not commit authority.
- [ ] **Step 5: Commit.** Then **STOP** — push and PR are separate approvals.

---

## Task 10: Post-merge (operator)

- [ ] Terraform Apply creates the task. Run the mega-job; confirm the gate runs after **both** upstreams and returns
**COMPLETE** on a clean drain. `action_context_unit_events` must hold: a `running` + terminal per unit, and
**one `slice_completed` per for_each worker — IDLE WORKERS INCLUDED — plus one for sb360**. (Do not expect a fixed
count: on a tiny daily run most workers are idle, and they emit *because of P4*. Phrase the check that way so the
operator doesn't chase a phantom.) `write_failures` should be **0**.
- [ ] Confirm the spike's prediction held (production append latency ≈ measured p50).

---

## Deferred (NOT in this PR)

- **TC-1 non-determinism** — `fct_tracking_context` holds an arbitrary pick among **content-divergent** duplicates
  (4,052 keys, ~99% divergent). ADR-030-class, unknown size; the mart is a retirement candidate.
- **The 38-action residual** (7 SkillCorner + 31 GS).
- **GS extra-time (891 actions)** — unrecoverable (provider ships ET events with no ET tracking frames).
- **sb360 DISCOVERY completeness** — the gate proves every *discovered* sb360 match ran; it does **not** prove
  discovery found every match. Only the planner re-run covers that, and only as a diagnostic.
