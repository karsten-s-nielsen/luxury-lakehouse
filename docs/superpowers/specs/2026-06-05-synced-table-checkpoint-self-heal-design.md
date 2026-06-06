# Self-Healing Synced-Table Checkpoint Recovery — Design (v3)

**Date:** 2026-06-05
**Status:** Draft — plan-ready (two review rounds incorporated)
**Owner:** Lakebase / synced-tables

> **Revision history**
> - v1: "SP self-heals inside the daily task." Falsified by review (SP can't do the PG/warehouse ops).
> - v2: pivot to **detect-in-task + remediate-in-privileged-path** (backed by verified findings F1–F3).
> - v3 (this): re-review hardening — silent-strand escalation (H3), heal concurrency (H4), partial-
>   failure recovery (M4), CDF-as-required-root-fix + contract test (M5), e2e gating + real-event
>   fixture (M6), freshness-signal honesty (M7), and L4–L7.

## Problem

`refresh_synced_tables` triggers a pipeline update on each Lakebase synced table. For TRIGGERED-policy
tables the update runs a streaming source whose checkpoint records the **source Delta table id**. When
a source gold mart is **dropped and recreated** (any `dbt build --full-refresh`), the new table gets a
new id and the streaming source fails permanently:

```
[STREAM_FAILED] [DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE]
... reading from an unexpected Delta table (id='<new>'); used to read id='<old>' per checkpoint ...
SQLSTATE: XXKST
```

The table is left `SYNCED_TABLE_ONLINE_PIPELINE_FAILED` (online-but-stale). Clearing the checkpoint
requires **recreating** the synced table — today a manual operator step — so every full-refresh rebuild
strands the affected tables and fails the daily `refresh_synced_tables` task.

### Observed instance (2026-06-05)

Full-refresh rebuild recreated 12 marts. The subsequent `refresh_synced_tables` job task
(run `971600085758769`, validated in the new `lakebase` env / databricks-sdk 0.114.0 — that fix worked:
all 42 *triggered*) saw **30/42 COMPLETE, 12 FAILED**, all 12 with the checkpoint mismatch — exactly the
TRIGGERED tables whose source was full-refreshed.

## Goal / non-goals

Automatically self-heal the checkpoint break so a full-refresh rebuild never strands synced tables and
never needs a manual recreate, **preserving TRIGGERED/CDF incremental sync**. Non-goals: changing dbt
materialization; eliminating full-refresh; switching to SNAPSHOT.

## Verified findings (backend evidence, 2026-06-05)

- **F1 — reliable delete is two-part.** `delete_synced_table.py` does SDK `delete_synced_table` (l.90)
  **and** `psycopg2 DROP TABLE IF EXISTS dev_gold."<t>"` of the **PG ghost** (l.96–114).
  `migrate._delete_synced_table` (l.127–140) is SDK-only and the create loop **tolerates "already
  exists"** (l.284) → an SDK-only delete leaving a ghost yields a **false-positive heal**. Ghost drop is
  a PostgreSQL op.
- **F2 — CDF does not reliably survive full-refresh.** `SHOW TBLPROPERTIES`: `fct_action_values`,
  `fct_passes` → CDF **true**; `fct_action_context`, `fct_off_ball_xt`, `fct_player_embeddings` →
  property **absent**. Only marts whose dbt model sets the tblproperty keep it. A TRIGGERED synced table
  can't be created over a CDF-off source — hence the operator Phase 2 (`ALTER … enableChangeDataFeed`,
  `migrate:246–270`). Heal must ensure CDF (warehouse op) before create.
- **F3 — the daily job runs as a service principal** that is **not a PG role**. It cannot do F1's ghost
  drop, so the recreate cannot run in the daily task.

**Conclusion:** the destructive recreate must run in a **PG- and warehouse-capable identity** (the
existing maintenance/operator path). The daily task can only **detect** (read-only) and **trigger**.

## Architecture

### 1. Detection — daily `refresh_synced_tables` task (service principal, read-only)

After the existing trigger + wait-poll, for each `FAILED` table classify via **SQLSTATE `XXKST`**
(primary; message marker secondary):

- **Checkpoint-mismatch → STRANDED** (auto-healable): log loudly, **never delete**, record to the
  existing **watermark/state** store (so recurrence is detectable — see H3).
- **Other failure → real error** (task red, as today).
- **Trigger remediation** for the STRANDED set (§3).

**Exit semantics (resolves open #4 / H3 / M7) — "green-with-warning on first, RED on recurrence":**
- All COMPLETE → **green**, summary "N fresh".
- Only STRANDED, **and not STRANDED on the prior run** → **green-with-warning**, summary explicitly
  `"M of N fresh; K stranded (checkpoint reset pending heal — dispatched)"`. The summary must
  distinguish *all-fresh* from *K-stranded-stale* so no downstream freshness consumer reads green as
  "everything fresh" (M7, Hyrum's Law). Reconcile with the watermark/freshness reporting the task
  already writes.
- A table STRANDED on **two consecutive runs** (prior dispatch did not resolve it) → **RED** + ERROR.
  This converts a silently-looping failed heal into a paging signal (H3; the ADR-002 anti-pattern of
  invisible warning-level logs).

The SP uses only read APIs it already holds (synced-table status, pipeline events) + a write to the
existing state store. **Zero destructive ops** (H1).

### 2. Remediation — maintenance/operator path (PG + warehouse + SDK)

`lakebase-grants.yml` / `maintain_synced_tables.py` gain a heal step. Per stranded table,
**verify-before-destroy**, then:

```
preflight:  source mart exists; mismatch still present; CDF on OR ensurable; create privilege present.
            ANY doubt -> SKIPPED_PREFLIGHT, do NOT delete (leave stale, surface).
ensure_cdf: idempotent ALTER ... enableChangeDataFeed=true                         [warehouse]
delete:     ws.postgres.delete_synced_table  +  psycopg2 DROP ghost                [SDK + PG]
create:     create_synced_table  -> "already exists" => HEAL_FAILED (delete didn't take)   [L1]
trigger+wait: trigger_refresh -> wait_until_online -> HEALED, else HEAL_FAILED
restore:    PG SELECT grants (ADR-005) + custom indexes — the maintenance pass already does this for
            all tables; sequence heal BEFORE the grants/indexes pass.
```

- **HEAL_FAILED emits ERROR-level / paging, never warning** (H3 / ADR-002).
- **Mid-sequence recovery (M4):** if `create` fails *after* `delete` succeeded, the table is absent.
  Recovery is explicit and already-present: the maintenance **create-all** pass recreates any missing
  `SYNCED_TABLES` entry on its next run (the "already exists" tolerance means present tables are no-ops
  and absent ones are created). Keep `delete→create` as tight as possible; document this recovery path
  in the ADR. (The risk moved identities; it did not vanish.)
- **CDF-at-heal-time is sufficient (L7):** a recreated TRIGGERED synced table does an initial full load
  then go-forward CDF incremental — enabling CDF immediately before create covers the incremental phase.

### 3. Triggers, immediacy & concurrency

- **Backstop (always):** the maintenance heal runs on the existing cadence (07:00 UTC daily + after
  every terraform apply), so a strand heals within ≤24 h with zero human action.
- **Immediacy (H2):** the daily detection task fires `workflow_dispatch` on the maintenance workflow
  (GitHub API call — control-plane, via a scoped GH-token Databricks secret) with the stranded list,
  shrinking the window to minutes.
- **Concurrency (H4):** the maintenance workflow gets a GitHub Actions `concurrency: { group: lakebase-
  maintenance, cancel-in-progress: false }` so the three triggers (daily / terraform-apply / dispatch)
  **never overlap** on the same synced tables — closing the two-runs-both-pass-preflight-both-delete
  race, and de-duping repeated dispatches from H3.

### 4. CDF root-cause fix — REQUIRED, lands with the heal (M5)

F2 proves CDF-off is a **standalone latent bug**: any TRIGGERED synced table over a CDF-off source is
unrecreatable, independent of the checkpoint issue. Therefore:

- Persist `tblproperties={'delta.enableChangeDataFeed':'true'}` in **all** source dbt models backing
  TRIGGERED synced tables (today only a subset do). This is the actual root-cause fix and ships **with**
  the heal, not "later."
- **Shift-left contract test (ADR-018 culture):** assert every `SYNCED_TABLES` entry with
  `scheduling_policy=="TRIGGERED"` has a source dbt model carrying the CDF tblproperty. A cheap unit test
  over model configs × the synced-table list; prevents anyone silently reopening F2 with a new TRIGGERED
  table.
- **Keep operator Phase 2 (L4, Chesterton's Fence):** §4 makes the `ALTER`-all idempotent step
  belt-and-suspenders (covers tables added later / sources that lose CDF via schema evolution), not
  redundant. Do **not** remove it.

## Module structure (hexagonal — M1)

Domain policy depends on **ports (Protocols)**, not the SDK, so SDK/PG/warehouse drift (e.g. 0.113→0.114;
`ws.postgres.*` vs `ws.database.*`) touches only adapters.

### `src/ingestion/synced_table_lifecycle.py` (new)

- **`DetectionPort`** (read-only): `get_synced_table_status`, `get_pipeline_id`, `latest_failed_events`
  — SDK adapter usable by the SP.
- **`HealPort`** (full): the read ops + `ensure_cdf` (warehouse), `sdk_delete` + `drop_pg_ghost`
  (SDK + psycopg2), `create_synced_table`, `trigger_refresh`, `wait_until_online` — SDK+PG+warehouse
  adapter usable only by the privileged path. **L5:** the test fake must simulate **per-sub-op failure**
  (e.g. SDK-delete OK but ghost-drop fails) so M4 partial states are testable. (Future refinement: split
  into three thin driven ports — Sdk / Postgres / Warehouse — composed by the heal use-case.)
- Adapters pin the **post-0.114.0** SDK method names with a comment (L2).
- The canonical two-part delete + create + poll primitives consolidate here (moved from
  `refresh_synced_tables.py`, `migrate_synced_tables.py`, `delete_synced_table.py`) as one source of
  truth.

### `src/ingestion/synced_table_heal.py` (new)

- `_CHECKPOINT_MISMATCH_SQLSTATE = "XXKST"` (+ message marker secondary).
- `is_checkpoint_mismatch_failure(port: DetectionPort, pipeline_id) -> bool` — fail-safe: query error /
  inconclusive → `False`.
- `class HealOutcome(Enum): HEALED, UNHEALABLE, HEAL_FAILED, SKIPPED_PREFLIGHT`.
- `heal_synced_table(port: HealPort, config, ...) -> HealOutcome` — the verify-before-destroy flow.

### Consumers (refactored)

- `refresh_synced_tables.py` → detect-only over `DetectionPort` + state-write + workflow_dispatch.
- `maintain_synced_tables.py` / `lakebase-grants.yml` → heal step over `HealPort`, before grants/indexes.
- `scripts/migrate_synced_tables.py` + `scripts/delete_synced_table.py` → import consolidated lifecycle.
  **L6:** preserve their CLI signatures/behavior exactly (operator muscle-memory is an observable
  interface — Hyrum's Law).

## Safety invariants

- Detect never destroys (SP path) — H1.
- Verify-before-destroy → `SKIPPED_PREFLIGHT`, no delete on any doubt.
- Marker-gated by SQLSTATE `XXKST`; every other failure surfaces untouched.
- No false-positive heal: `"already exists"` on create → `HEAL_FAILED`, never `HEALED` (L1).
- HEAL_FAILED + two-consecutive-strand → **ERROR-level / RED**, never silent warning (H3 / ADR-002).
- Heals never overlap (workflow `concurrency` group) — H4.
- Bounded: each table healed at most once per maintenance run.

## Testing (TDD, offline-first)

- **Detection (offline, fake `DetectionPort`):** `XXKST` present → True; absent → False; query raises →
  False. Sentinel: never True without a marker.
- **Real-event fixture (M6):** capture the actual failed pipeline-event payload from run
  `971600085758769` as a committed fixture; test `is_checkpoint_mismatch_failure` against it — guards the
  classifier against real Databricks event shapes (the M2 Hyrum concern) that synthetic markers miss.
- **Heal (offline, fake `HealPort` with per-sub-op failure — L5):** happy → `HEALED`; create
  "already exists" → `HEAL_FAILED`; pre-flight doubt → `SKIPPED_PREFLIGHT` with **delete not called**;
  ghost-drop fails after sdk-delete → `HEAL_FAILED` + asserts the M4 absent-table state is surfaced;
  permission/timeout → `HEAL_FAILED`.
- **Detect-only refresh (offline):** mixed results → STRANDED classified, dispatch fired only for
  STRANDED, **no destructive call** from the SP path; first-detection → green-with-warning, second
  consecutive → RED (state-store read).
- **CDF contract test (M5):** every TRIGGERED `SYNCED_TABLES` entry ↔ source model with CDF tblproperty.
- **e2e (serverless, M6) — the only test that catches F1/F2/checkpoint-reset:** (a) reproduce the failed
  state (full-refresh the source first); (b) run as the appropriate identity; (c) assert the checkpoint
  is **actually reset** — a fresh full sync, **not** an "already exists" no-op; (d) confirm go-forward
  **incremental CDF** sync works. **Make it a required gate** for changes touching `lifecycle`/`heal`
  (nightly or pre-merge-on-path), not merely "available" — else the offline suite goes green and a
  ghost/CDF regression ships. *(Dependency: this needs serverless CI creds + the `spark`/`tmp_catalog`
  fixtures that do not yet exist; if that infra is a follow-on, the offline real-event fixture + contract
  test are the immediately-shippable guards and the e2e remains `RUN_SERVERLESS`-gated until the CI job
  lands.)*
- **Regression:** existing `test_migrate_synced_tables` + `test_refresh_synced_tables` pass after
  consolidation; operator CLIs unchanged (L6).

## Privilege (verify in plan)

- **SP (detection):** can read synced-table status + pipeline events (has CAN_RUN on pipelines today;
  CAN_VIEW is the open question) + write the state store + hold a scoped GH-token secret for dispatch.
- **Privileged identity (remediation):** can `ALTER` dev_gold marts via warehouse,
  `ws.postgres.delete/create_synced_table`, and `psycopg2` DROP — it already does grants/indexes/
  ghost-drops, so expected, but verify the full matrix empirically before coding.

## ADR + docs + release

- **New ADR** — "Self-healing synced-table checkpoint recovery": the detect/remediate split; findings
  **F1** (PG ghost / two-part delete), **F2** (CDF not surviving full-refresh / Phase-2 rationale),
  **F3** (SP not a PG role); plus verify-before-destroy, no-false-positive, **H3** silent-strand
  escalation, **H4** concurrency, and **M4** mid-sequence-failure recovery. References ADR-002 (no silent
  warning-swallows), ADR-005 (grants), ADR-018 (format-contract tests), ADR-026 (SDK-managed lifecycle).
- **`docs/engineering/conventions.md`** Lakebase Ops — auto-heal behavior, detect/remediate split, the
  CDF-in-dbt-model requirement + contract test, the freshness-signal semantics (M7).
- **Wheel bump** + `bump_wheel.py` sync.
- **C4** — `refreshSyncedTables` description (detect-only + dispatch); heal note on the maintenance/
  `lakebase-grants` element.

## Plan sequencing (suggested)

1. CDF root-cause: persist tblproperty in the 12 models + the M5 contract test (independently valuable;
   unblocks recreate reliability).
2. `synced_table_lifecycle` (ports + adapters + consolidated primitives) with offline adapter tests;
   operator CLIs preserved.
3. `synced_table_heal` (detection + verify-before-destroy heal) with offline + real-event-fixture tests.
4. Maintenance heal step + workflow `concurrency` group + ERROR-on-HEAL_FAILED.
5. Detect-only `refresh_synced_tables` + state-store recurrence + green/red exit semantics + dispatch.
6. e2e (serverless) + required-gate CI job (or defer the gate, keeping offline guards).
7. ADR + conventions + wheel bump + C4.

## Open items for the reviewing session

1. **Immediacy mechanism:** `workflow_dispatch` (GH-token secret, minutes) vs backstop-only (≤24 h).
   Confirm token scoping.
2. **SP CAN_VIEW on pipelines** + the privileged-identity capability matrix — verify empirically.
3. **State store for recurrence (H3):** confirm reuse of the existing watermark/observability mechanism
   (vs a small dedicated marker) and where it's keyed (per synced table).
4. **e2e required-gate infra (M6):** is the serverless CI job in scope now, or does the gate land as a
   follow-on with offline guards holding the line meanwhile?
