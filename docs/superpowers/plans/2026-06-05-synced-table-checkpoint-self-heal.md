# Synced-Table Checkpoint Self-Heal — Implementation Plan (v3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Commit authority (P12):** the per-task "Commit" steps are the TDD checkpoint cadence. Actual `git commit` requires the project's standing commit approval (CLAUDE.md: commits need separate approval; the PR is the gate). Execute on a feature branch `feat/synced-table-self-heal`; the executor must confirm standing commit authority for that branch before batching commits.

**Goal:** A `dbt --full-refresh` of a gold mart must never permanently strand its TRIGGERED Lakebase synced table — detect the checkpoint break in the daily job, auto-remediate it in the privileged maintenance path, preserve TRIGGERED/CDF incremental sync.

**Architecture:** Split by privilege (spec F1–F3). Daily `refresh_synced_tables` (service principal, **DetectionPort only — cannot delete by construction**) detects the `XXKST` checkpoint failure, records per-table strand state, and dispatches remediation. The PG+warehouse maintenance path (**HealPort**) remediates verify-before-destroy (ensure-CDF → two-part delete → create → re-trigger) behind a kill-switch, then regrants/reindexes. Recurrence-RED is driven by a timestamped strand-state store cleared on successful heal.

**Tech Stack:** Python 3.10, databricks-sdk 0.114.0 (`ws.postgres.*`), psycopg2 (PG ghost), Databricks SQL warehouse (`statement_execution`), dbt-databricks, pytest, GitHub Actions.

**Reference spec:** `docs/superpowers/specs/2026-06-05-synced-table-checkpoint-self-heal-design.md`

---

## Conventions

- New modules in `src/ingestion/`; offline tests in `src/tests/`. Offline tests must not import the SDK at module load (use the `try/except ImportError` + Protocol pattern from `refresh_synced_tables.py`).
- Dev venv = SDK 0.77.0; run SDK-touching code with `uv run --extra sdk …` (0.114.0). `test_migrate_synced_tables` only passes under `--extra sdk` (known local-env gotcha).
- **Move-verbatim steps: grep by symbol, never trust line numbers (P10)** — e.g. `grep -n "def _get_pipeline_id" src/ingestion/refresh_synced_tables.py`.
- Run: `uv run pytest <p> -v`; `uv run ruff check <p>`; `uv run pyright src/`.

---

## Task 0: Verification spikes (gate the empirical assumptions)

**Files:** Create `src/tests/fixtures/synced_table/pipeline_events_checkpoint_mismatch.json`

- [ ] **0a — capture the real failed-event fixture (M6).**

```bash
uv run --extra sdk python -c "
import json
from databricks.sdk import WorkspaceClient
w=WorkspaceClient()
pid='08955359-efd3-4820-94f9-c9b8bfac4e8f'  # fct_action_values_synced pipeline, run 971600085758769
evs=[e.as_dict() for e in w.pipelines.list_pipeline_events(pipeline_id=pid, max_results=250)]
err=[e for e in evs if e.get('error')]
json.dump(err, open(r'src/tests/fixtures/synced_table/pipeline_events_checkpoint_mismatch.json','w'), indent=2)
print('events:', len(err), '| XXKST present:', any('XXKST' in json.dumps(e) for e in err))
"
```
Expected: nonzero count, `XXKST present: True`. Commit the fixture. **Also note the JSON path to the error message** (e.g. `error.exceptions[].message`) — Task 3 scopes the matcher to it (P9).

- [ ] **0b — confirm post-0.114.0 SDK symbol names (L2).** `uv run --extra sdk python -c "from databricks.sdk import WorkspaceClient as W; w=W(); print([m for m in dir(w.postgres) if 'synced' in m.lower()]); print(hasattr(w.pipelines,'list_pipeline_events'))"` → confirm `get_synced_table`, `create_synced_table`, `delete_synced_table`. If any differ from `migrate_synced_tables.py`, STOP and reconcile.

- [ ] **0c — privilege matrix + pre-written grant task (P11).** Verify: (1) SP has `CAN_VIEW` on synced-table pipelines (`w.pipelines.get_permission_levels`); (2) SP can write the observability strand-state table (the cost hook already writes observability as the SP → evidence); (3) privileged identity can `ALTER` dev_gold marts + `ws.postgres.delete/create_synced_table` + `psycopg2 DROP` (it runs grants/indexes today → evidence). **If the SP lacks `CAN_VIEW`,** apply this pre-written grant (do not improvise): add `CAN_VIEW` for the run-as SP on each synced-table pipeline via `grant_synced_table_permissions.py` (extend its grant loop) **or** a terraform `databricks_permissions` block on the pipelines — whichever matches the existing grant mechanism in that script. Record the chosen path in the PR.
  - **(4) GH-token for dispatch (R5):** the H2 immediacy `workflow_dispatch` (Task 8) needs a scoped GitHub token secret readable by the daily job. Verify it exists (or decide **backstop-only** — the Task 8.3 try/except-degrade handles token absence gracefully, which is the right default if no token is provisioned). Record the decision.

---

## Task 1: CDF contract test + persist CDF in TRIGGERED sources (spec §4 / M5, P7)

**Files:** Create `src/tests/test_synced_table_cdf_contract.py`; Modify the CDF-off mart models.

- [ ] **1.1 — failing contract test (graceful on non-model sources — P7):**

```python
# src/tests/test_synced_table_cdf_contract.py
"""Every TRIGGERED synced-table source dbt model must persist CDF (spec F2): a full-refresh
strips delta.enableChangeDataFeed otherwise -> the table becomes unrecreatable."""
from __future__ import annotations

import re
from pathlib import Path

from ingestion.refresh_synced_tables import SYNCED_TABLES

_MODELS = Path(__file__).resolve().parents[2] / "dbt_project" / "models"
_CDF_RE = re.compile(r"delta\.enableChangeDataFeed['\"]\s*:\s*['\"]true['\"]")


def test_triggered_sources_persist_cdf() -> None:
    missing, non_model = [], []
    for cfg in SYNCED_TABLES:
        if cfg.scheduling_policy != "TRIGGERED":
            continue
        hits = list(_MODELS.rglob(f"{cfg.source_table}.sql"))
        if len(hits) != 1:
            non_model.append((cfg.name, cfg.source_table, len(hits)))  # P7: clear, not a crash
            continue
        if not _CDF_RE.search(hits[0].read_text(encoding="utf-8")):
            missing.append(cfg.source_table)
    assert not non_model, (
        "TRIGGERED synced tables whose source is not exactly one dbt model — a TRIGGERED "
        f"non-mart source cannot be CDF-guaranteed here and needs explicit handling: {non_model}"
    )
    assert not missing, f"TRIGGERED sources missing delta.enableChangeDataFeed (full-refresh strips CDF): {sorted(missing)}"
```

- [ ] **1.2 — run; verify FAIL** listing ≥ `fct_action_context`, `fct_off_ball_xt`, `fct_player_embeddings`. `uv run pytest src/tests/test_synced_table_cdf_contract.py -v`
- [ ] **1.3 — add the tblproperty** to each listed model, matching `dbt_project/models/marts/fct_action_values.sql` style (`'delta.enableChangeDataFeed': 'true'` inside `{{ config(tblproperties={...}) }}`). Change nothing else.
- [ ] **1.4 — run; verify PASS.**
- [ ] **1.5 — commit:** `feat(lakebase): require CDF tblproperty on TRIGGERED synced-table sources + contract test (F2/M5)`

> Live tables already CDF-off stay so until their next full-refresh (dbt incrementals don't re-apply tblproperties — P8); the heal's `ensure_cdf` is **load-bearing** for them, not defensive.

---

## Task 2: Ports + HealOutcome (spec M1)

**Files:** Create `src/ingestion/synced_table_lifecycle.py` (Protocols only) + `src/ingestion/synced_table_heal.py` (enum); Test `src/tests/test_synced_table_lifecycle_ports.py`.

- [ ] **2.1 — failing test:** assert `DetectionPort` has `{get_synced_table_status, get_pipeline_id, latest_failed_events}`; `HealPort` additionally has `{ensure_cdf, sdk_delete, drop_pg_ghost, create_synced_table, trigger_refresh, wait_until_online}`; `HealOutcome` == `{HEALED, UNHEALABLE, HEAL_FAILED, SKIPPED_PREFLIGHT}`.
- [ ] **2.2 — run; verify FAIL** (ImportError).
- [ ] **2.3 — implement** the two Protocols (as in spec §"Module structure"; `HealPort(DetectionPort, Protocol)`) and `HealOutcome(Enum)`. Module docstrings note: import-safe offline; domain depends on Protocols not the SDK.
- [ ] **2.4 — run; verify PASS.**
- [ ] **2.5 — commit:** `feat(lakebase): DetectionPort/HealPort protocols + HealOutcome`

---

## Task 3: Detection classifier — latest-update-scoped, error-field match (spec M2/M6, P9)

**Files:** Modify `synced_table_heal.py`; Test `src/tests/test_synced_table_heal_detection.py`.

- [ ] **3.1 — failing tests** (synthetic + real-event fixture). Key cases: `XXKST` in the **latest update's error field** → True; other failure → False; query raises → False; the **real fixture** → True; and a case where `XXKST` appears only in **non-latest history** → False (P9 — must not match stale events).
- [ ] **3.2 — run; verify FAIL.**
- [ ] **3.3 — implement** `is_checkpoint_mismatch_failure(port, pipeline_id)`:
  - `port.latest_failed_events` returns events already scoped to the **latest** update (adapter responsibility — Task 4).
  - Match `_CHECKPOINT_MISMATCH_SQLSTATE = "XXKST"` (primary) / `_..._MESSAGE_MARKER = "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE"` (secondary) against the **error/exception message field** of those events (the path recorded in Task 0a), not a whole-blob `json.dumps` (P9).
  - Wrap in `try/except` → ERROR log + `False` (fail-safe).
- [ ] **3.4 — run; verify PASS** (incl. fixture). If the fixture fails, adjust the field path to the real shape (the point of M6).
- [ ] **3.5 — commit:** `feat(lakebase): latest-update-scoped SQLSTATE detection + real-event fixture (M2/M6/P9)`

---

## Task 4: Split adapters — DetectionAdapter (SP) vs HealAdapter (privileged) (spec M1/H1, P2/P10)

**Two adapters so "detect never destroys" is a type guarantee (P2):** the SP constructs only `SdkDetectionAdapter(ws)`, which has no destructive methods to call.

**Files:** Modify `synced_table_lifecycle.py`; re-point `migrate_synced_tables.py`, `refresh_synced_tables.py`, `delete_synced_table.py` (preserve CLIs — L6); Test `src/tests/test_synced_table_lifecycle_adapter.py`.

- [ ] **4.1 — failing tests** (mock injected `ws`/`pg_connect`/`sql_exec`):
  - `SdkDetectionAdapter(ws)` implements `get_synced_table_status`/`get_pipeline_id`/`latest_failed_events`; **assert it has no `sdk_delete`/`drop_pg_ghost`/`ensure_cdf`/`create_synced_table` attributes** (type-guarantee test).
  - `SdkHealAdapter(ws, pg_connect, sql_exec)`: `sdk_delete` → `ws.postgres.delete_synced_table` called; `drop_pg_ghost` → cursor executes `DROP TABLE IF EXISTS`; `ensure_cdf` → `sql_exec` statement contains `enableChangeDataFeed`; `latest_failed_events` returns only the latest update's events.
- [ ] **4.2 — run; verify FAIL.**
- [ ] **4.3 — implement both adapters.** **Grep by symbol (P10)** to move bodies verbatim:
  - Detection ops from `refresh_synced_tables.py`: `def _get_pipeline_id`, `def wait_until_online`, the SNAPSHOT-trigger helper; build `latest_failed_events` to fetch `list_pipeline_events` and **filter to the latest `update_id`** (P9) before returning the error events.
  - Heal-only ops: `create_synced_table` from `migrate_synced_tables.py:_create_synced_table`; `sdk_delete` from `migrate_synced_tables.py:_delete_synced_table`; `drop_pg_ghost` (psycopg2 `_get_pg_token` + `DROP TABLE IF EXISTS {schema}."{table}"`) from `delete_synced_table.py`; `ensure_cdf` = `sql_exec("ALTER TABLE {src} SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")` (same as `migrate_synced_tables.py` Phase 2).
  - Pin SDK names with a comment (L2). `SdkHealAdapter` may subclass `SdkDetectionAdapter` for the read ops (shared), adding the destructive ops — but `SdkDetectionAdapter` must NOT expose them.
- [ ] **4.4 — re-point consumers, preserve CLIs (L6):** `migrate`/`delete_synced_table` call the heal adapter; their `main()` arg surfaces stay byte-identical. `refresh_synced_tables` imports the **detection** adapter only.
- [ ] **4.5 — run** `uv run pytest src/tests/test_synced_table_lifecycle_adapter.py src/tests/test_refresh_synced_tables.py -v`; and `uv run --extra sdk pytest src/tests/test_migrate_synced_tables.py -v` → PASS.
- [ ] **4.6 — commit:** `refactor(lakebase): split Detection/Heal adapters (type-level no-destroy for SP) + consolidate primitives (M1/H1/P2)`

---

## Task 5: `heal_synced_table` — verify-before-destroy (spec H1/L1/L5/L7/M4)

**Files:** Modify `synced_table_heal.py`; Test `src/tests/test_synced_table_heal.py`.

- [ ] **5.1 — failing tests** with a fake `HealPort` supporting **per-sub-op failure injection (L5)**: happy → `HEALED` (asserts call order `ensure_cdf, sdk_delete, drop_pg_ghost, create, trigger`); mismatch-cleared-at-heal-time → `SKIPPED_PREFLIGHT` with **no delete called**; create "already exists" → `HEAL_FAILED` (L1); `drop_pg_ghost` fails after `sdk_delete` → `HEAL_FAILED` (M4 partial state surfaced); not-online-after → `HEAL_FAILED`.
- [ ] **5.2 — run; verify FAIL.**
- [ ] **5.3 — implement** `heal_synced_table(port, config, catalog, schema)` exactly as in the spec (preflight re-checks `is_checkpoint_mismatch_failure`; ensure_cdf → sdk_delete → drop_pg_ghost → create [already-exists ⇒ HEAL_FAILED] → trigger → wait_until_online; any mid-sequence exception ⇒ `logger.exception` + `HEAL_FAILED`). CDF-at-heal-time covers the recreated table's initial-full-load-then-go-forward-CDF (L7).
- [ ] **5.4 — run; verify PASS** (5 tests).
- [ ] **5.5 — commit:** `feat(lakebase): verify-before-destroy heal_synced_table (H1/L1/M4)`

---

## Task 6: Strand-state store — timestamped, cleared on heal (spec H3 / P1)

The store is the heart of the recurrence-RED correctness fix (P1). It is **timestamp-based** so "recurrence" means "stranded again with no successful heal since," not merely "in the last set."

**Files:** Create `src/ingestion/synced_table_strand_state.py`; Test `src/tests/test_synced_table_strand_state.py`. Migration: `scripts/migrations/<date>_synced_table_strand_state.sql` (idempotent `CREATE TABLE IF NOT EXISTS observability.synced_table_strand_state`).

- [ ] **6.1 — failing tests** against a **fake row store** (offline; the Spark/Delta IO is an injected seam):
  - `mark_stranded(t, ts)` then `was_stranded_unhealed(t)` → True.
  - `mark_stranded(t, ts1)` → `mark_healed(t, ts2>ts1)` → `was_stranded_unhealed(t)` → **False** (cleared).
  - heal→clear→re-strand: `mark_stranded(ts1)`, `mark_healed(ts2)`, `mark_stranded(ts3>ts2)` → `was_stranded_unhealed` → **False** (new incident — P1: must be green-with-warning, NOT RED).
  - stranded-then-stranded-again-without-heal: `mark_stranded(ts1)`, `mark_stranded(ts2)` with no heal → **True** (recurrence — RED).
  - **state-table-absent (R1a — first-run bootstrap):** `was_stranded_unhealed(t)` when the backing Delta table does not exist → **False**, **no exception** (fail-open → first-strand-green, never crash, never false-RED).
- [ ] **6.2 — run; verify FAIL.**
- [ ] **6.3 — implement** `StrandStateStore` over `observability.synced_table_strand_state(table_name STRING, last_stranded_at TIMESTAMP, last_healed_at TIMESTAMP, _ingested_at TIMESTAMP)`:
  - `mark_stranded(table, ts)` — upsert `last_stranded_at = ts`.
  - `mark_healed(table, ts)` — upsert `last_healed_at = ts` (written by the **privileged heal pass**, P1).
  - `was_stranded_unhealed(table)` ⇔ `last_stranded_at IS NOT NULL AND (last_healed_at IS NULL OR last_healed_at < last_stranded_at)`.
  - Spark/Delta IO behind an injected port so tests are offline. **Do NOT reuse `record_watermarks`** (phantom-row crash modes — P1 caveat); this is a dedicated table.
  - **Fail-open reads (R1a):** wrap every read (`was_stranded_unhealed`) in `ingestion.utils.tolerate_missing_table` (utils.py, ADR-002) → a missing/unreadable state table returns "no prior strand" (False), never raises. This extends the "detection must never raise" discipline to the state store and survives first-run-before-migration.
  - **Retry-safe writes (R3):** route `mark_stranded`/`mark_healed` upserts through `ingestion.utils.write_delta_table` (ADR-038 `_COMMIT_MAX_ATTEMPTS=10`) so a `Concurrent*Exception` retries — the SP and the privileged heal pass write the same rows and the SP is NOT in the H4 concurrency group. The timestamp-compare is order-robust (independent columns, last-write-wins per column), so only the write needs retry-safety.
  - Both identities write it: SP writes `last_stranded_at` (Task 8), privileged path writes `last_healed_at` (Task 7). The SP already does Spark/Delta IO in `main()` (`record_watermarks`, `get_spark_session`), so no new capability is needed — only the missing-table tolerance above.
- [ ] **6.4 — run; verify PASS.**
- [ ] **6.5 — commit:** `feat(lakebase): timestamped strand-state store, cleared on heal (H3/P1)`

---

## Task 7: Maintenance heal pass — kill-switch, clear-on-heal, ordering test (spec H3/H4, P1/P3/P5)

**Files:** Modify `synced_table_heal.py` (`run_heal_pass`) + `scripts/maintain_synced_tables.py` + `.github/workflows/lakebase-grants.yml`; Test `src/tests/test_maintain_synced_tables_heal.py`.

- [ ] **7.1 — failing tests:**
  - `run_heal_pass` calls `heal_synced_table` per stranded table; on `HEAL_FAILED` logs **ERROR** (H3); on `HEALED` calls `state.mark_healed(table, ts)` (P1 write-back).
  - **kill-switch (P3):** with `SYNCED_TABLE_HEAL_ENABLED=0`, `run_heal_pass` does nothing destructive (asserts `heal_synced_table` not called) and logs that it's disabled.
  - **ordering (P5):** a fake maintenance driver records pass order; assert `heal` runs **before** `grants` and `indexes`.
- [ ] **7.2 — run; verify FAIL.**
- [ ] **7.3 — implement** `run_heal_pass(port, stranded, catalog, schema, state, *, now, enabled: bool)`:
  - **Kill-switch is injected, not read inside the policy (P3/R6):** first line `if not enabled: logger.warning("heal disabled by kill-switch"); return {}`. The `enabled` flag is resolved at the CLI/adapter boundary in `maintain_synced_tables.py` (`os.environ.get("SYNCED_TABLE_HEAL_ENABLED", "1") == "1"`) and passed in — keeps the domain function pure + trivially testable.
  - Loop: `heal_synced_table(...)`; `HEALED` → `state.mark_healed(t, now)`; `HEAL_FAILED` → `logger.error(...)` (H3); tally + return outcome map.
  - In `maintain_synced_tables.py`: call `run_heal_pass` **before** the grants + indexes passes (P5).
- [ ] **7.4 — workflow (H4):** add to `lakebase-grants.yml` top-level `concurrency: {group: lakebase-maintenance, cancel-in-progress: false}`; add `workflow_dispatch` with a `tables` input; add the heal step. Validate YAML: `uv run python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/lakebase-grants.yml'))"`.
- [ ] **7.5 — run; verify PASS.**
- [ ] **7.6 — commit:** `feat(lakebase): maintenance heal pass w/ kill-switch + clear-on-heal + ordering guard + concurrency (H3/H4/P1/P3/P5)`

---

## Task 8: Detect-only `refresh_synced_tables` — corrected recurrence + precedence (spec §1/H3/M7, P1/P6)

**Files:** Modify `refresh_synced_tables.py`; Test `src/tests/test_refresh_synced_tables_detect.py`.

- [ ] **8.1 — failing tests** for `classify_and_exit_code(results, stranded, state)` — lock the **precedence (P6):** real-failure RED > recurrence RED > first-strand green-warning > all-fresh:
  - all COMPLETE → `(0, "…fresh")`.
  - first strand (`stranded={a}`, `was_stranded_unhealed(a)=False`) → `(0, "…stranded…dispatched")` (M7 distinct summary).
  - recurrence (`was_stranded_unhealed(a)=True`) → `(1, …)` (H3).
  - **heal→clear→re-strand** (`stranded={a}`, `was_stranded_unhealed(a)=False` because healed since) → `(0, …)` — **new incident, green-with-warning, NOT RED** (P1).
  - non-checkpoint FAILED present → `(1, …)`.
  - **mixed precedence (P6):** `{a: FAILED(non-stranded), b: FAILED(stranded, first)}` → `(1, …)` (real failure dominates); `{a: COMPLETE, b: stranded-first}` → `(0, …)`.
- [ ] **8.2 — run; verify FAIL.**
- [ ] **8.3 — implement.** `stranded` = FAILED tables where `is_checkpoint_mismatch_failure` is True (computed in `main()` via the **DetectionPort** — SP cannot delete). Precedence: any non-stranded FAILED → RED; else any `state.was_stranded_unhealed(t)` for a currently-stranded `t` → RED; else stranded present → green-with-warning + `state.mark_stranded(t, now)` for each; else all-fresh green. Summary always distinguishes all-fresh from K-stranded (M7). In `main()` (only `--wait`): build `SdkDetectionAdapter`, compute `stranded`, `classify_and_exit_code`, then (unless `--no-dispatch`) fire `workflow_dispatch` on `lakebase-grants.yml` with the stranded list (H2) — wrapped in `try/except` that degrades to "backstop will heal" on failure, never crashing the task.
- [ ] **8.4 — run; verify PASS.**
- [ ] **8.5 — commit:** `feat(lakebase): detect-only refresh w/ corrected recurrence-RED + precedence + dispatch (H3/M7/H2/P1/P6)`

---

## Task 9: Serverless e2e + CI gate as a deliverable (spec M3/M6, P4)

**Files:** Create `src/tests/test_synced_table_heal_e2e.py` (RUN_SERVERLESS-gated) + the `spark`/`tmp_catalog` fixtures it needs + `.github/workflows/synced-table-heal-e2e.yml` (nightly + path-trigger).

- [ ] **9.1 — write the gated e2e (decoupled from dbt — R2).** Skip unless `RUN_SERVERLESS_TESTS=1`. The failure trigger is *only* a new source-table id, so reproduce it with plain DDL — **no dbt build, no UC-catalog-creation orchestration** (which would make the test heavy/flaky and thus perpetually skipped, the P4 trap). Against a small disposable source table + its synced table: (a) `CREATE OR REPLACE TABLE`/DROP+CREATE the test source with `delta.enableChangeDataFeed=true` to **mint a new table id**; (b) assert the synced table reaches `SYNCED_TABLE_ONLINE_PIPELINE_FAILED` + `XXKST`; (c) `heal_synced_table` via the full `HealPort` adapter; (d) assert `HEALED` **and a fresh full sync occurred** (row-count matches the recreated source; pipeline `latest_update` is a NEW update, not an "already exists" no-op); (e) append a source row, trigger, assert **incremental CDF sync** propagates it (L7). The proof is about the **heal**, not about dbt.
- [ ] **9.2 — minimal fixture.** A small `conftest`/helper that creates the throwaway source + synced table via the `HealPort` adapter primitives (and tears them down) — no Spark-Connect session or dbt dependency required beyond the SDK + warehouse `sql_exec` the heal already uses.
- [ ] **9.3 — CI gate (P4 — a deliverable, not deferred):** add `.github/workflows/synced-table-heal-e2e.yml` running this with `RUN_SERVERLESS_TESTS=1` + serverless creds on a **nightly schedule AND on PRs touching `synced_table_lifecycle.py`/`synced_table_heal.py`** (paths filter). For a feature that deletes prod tables, this is the only proof of the destructive path; it must land with the feature.
- [ ] **9.4 — commit:** `test(lakebase): serverless e2e (checkpoint reset + incremental CDF) + nightly/path CI gate (M3/M6/P4)`

---

## Task 10: ADR + conventions + C4 (spec docs, P8)

**Files:** Create `docs/superpowers/adrs/ADR-0XX-synced-table-checkpoint-self-heal.md`; Modify `docs/engineering/conventions.md`, `docs/c4/architecture.dsl`.

- [ ] **10.1 — ADR** records: detect/remediate split; F1 (PG ghost/two-part delete), F2 (CDF not surviving full-refresh; **`ensure_cdf` is load-bearing until each CDF-off mart is next full-refreshed — P8**, not "defensive"); F3 (SP not a PG role); decisions — verify-before-destroy, already-exists⇒HEAL_FAILED, H3 (timestamped recurrence-RED + ERROR-not-warning), H4 concurrency, M4 mid-sequence recovery (maintenance create-all re-creates absent table), P3 kill-switch. Refs ADR-002/005/018/026.
- [ ] **10.2 — conventions** Lakebase Ops: auto-heal behavior, detect/remediate split, CDF-in-dbt contract, freshness-signal semantics (M7), kill-switch, and the **on-call runbook note (R4):** recurrence-RED fires when a strand is still `was_stranded_unhealed` at the *next* detect run (~one detect interval, ≈24 h — confirm this is the intended SLO; a heal that legitimately exceeds it, e.g. a slow `wait_until_online` queued behind the H4 concurrency group, would false-RED). **Detect-side RED and the maintenance heal-pass ERROR log (H3) are emitted by two different identities and must be correlated to diagnose** — document this cross-signal pairing rather than adding a `heal_attempted_at` column.
- [ ] **10.3 — C4:** edit (not append) `refreshSyncedTables` → "detect-only + dispatch"; heal note on the `lakebaseGrantsWorkflow` element; regen `architecture.html` per the c4 skill.
- [ ] **10.4 — run doc-guard tests** (`test_architecture_md_appendix.py`, `test_workflows_tf_ordering.py`, any ADR-presence test); commit `docs(lakebase): ADR + conventions + C4 for synced-table self-heal`.

---

## Task 11: Migration apply + wheel bump + full gate (release)

- [ ] **11.0 — apply the strand-state migration BEFORE the wheel ships (R1b).** No workflow auto-applies `scripts/migrations/` (verified — the auto-apply path only fires for PRs touching `dbt_project/**`, and is bronze-oriented). So the Task 6 `observability.synced_table_strand_state` migration must be **operator-applied**: `uv run --extra sdk python scripts/migrations/_runner.py <migration-file>` (or the project's standard runner invocation), and **confirm `_runner.py` targets/creates the `observability` schema** (the migrations tree is bronze-oriented — if the runner hard-codes `bronze`, parameterise it or apply the `CREATE TABLE IF NOT EXISTS observability.synced_table_strand_state` directly via `statement_execution`). The Task 6 fail-open read (R1a) is the safety net if this step is missed, but the table should exist before the first daily run. Record the applied migration + verification query in the PR.
- [ ] **11.1 — bump** `pyproject.toml` patch version (read current post-`0.5.20` value; +1 patch); `uv run python scripts/bump_wheel.py` then `--check` (consistent).
- [ ] **11.2 — full gate** (capture RC; never mask with `| tail` on the pytest exit):

```bash
uv run ruff check src/ scripts/; echo "RC=$?"
uv run ruff format --check src/ scripts/; echo "RC=$?"
uv run pyright src/ 2>&1 | tail -3
uv run --extra sdk pytest src/tests/ --benchmark-skip -q; echo "RC=${PIPESTATUS[0]}"
```
Expected: clean + green (`--extra sdk` so `test_migrate_synced_tables` resolves 0.114.0). Update `_topandas_exemptions.yml` if any `.toPandas()` line shifted (none expected).

- [ ] **11.3 — commit** `chore: bump wheel for synced-table self-heal`.

---

## Self-review (completed)

**Spec + review coverage:** F1→4/5; F2→1/4; F3→architecture(7-vs-8 split); H1→4(type-guarantee)/5; H2→8; H3→6/7/8; H4→7; M1→2/4; M2→3; M3→9; M4→5+10(ADR); M5→1; M6→3/9; M7→8; L1→5; L2→4; L3→10; L4→(no task removes Phase 2 — explicit keep); L5→5(per-sub-op fake); L6→4.4; L7→5/9. **Review:** P1→6+7+8 (state store + clear-on-heal + corrected recurrence/heal→clear→re-strand test); P2→4 (split adapters + no-destructive-attr test); P3→7 (kill-switch); P4→9 (CI gate deliverable); P5→7 (ordering test); P6→8 (precedence test); P7→1 (graceful non-model); P8→1 note+10 ADR; P9→3 (latest-update + error-field); P10→Conventions (grep-by-symbol); P11→0c (pre-written grant); P12→header note. **Re-review v2:** R1→6.1/6.3 (fail-open read via tolerate_missing_table) + 11.0 (explicit migration apply); R2→9.1/9.2 (e2e reproduces via DDL table-id mint, no dbt); R3→6.3 (write_delta_table retry-safe upserts); R4→10.2 (recurrence-window SLO + cross-signal on-call note); R5→0c (GH-token check / backstop-only default); R6→7.3 (kill-switch injected, domain fn pure).

**Type consistency:** `HealOutcome` members; `DetectionPort`/`HealPort` methods; `is_checkpoint_mismatch_failure`/`heal_synced_table`/`run_heal_pass`/`classify_and_exit_code`/`StrandStateStore.{mark_stranded,mark_healed,was_stranded_unhealed}` signatures consistent across Tasks 2–8. `SdkDetectionAdapter` exposes only read ops (asserted in 4.1).

**Residual verification (not placeholders):** SDK symbol names (0b), SP `CAN_VIEW` + grant path (0c), real-event error-field JSON path (0a→3), serverless fixtures (9.2).
```
