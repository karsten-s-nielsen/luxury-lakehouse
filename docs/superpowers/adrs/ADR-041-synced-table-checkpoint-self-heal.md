# ADR-041: Self-healing synced-table checkpoint recovery

## Status

Accepted (2026-06-05).

## Context

Lakebase synced tables with `scheduling_policy="TRIGGERED"` sync incrementally via a Delta streaming
source whose checkpoint records the **source Delta table id**. When a source gold mart is dropped and
recreated — which any `dbt build --full-refresh` of that mart does — the new table gets a new id and
the streaming source fails permanently:

```
[STREAM_FAILED] [DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE] ... SQLSTATE: XXKST
```

The synced table is left `SYNCED_TABLE_ONLINE_PIPELINE_FAILED` (online but stale). The only fix is to
**recreate** the synced table (reset the checkpoint). This was a manual operator step, so every
full-refresh rebuild stranded the affected synced tables and failed the daily job's
`refresh_synced_tables` task (observed 2026-06-05: 12 of 42 tables stranded after a full-refresh
rebuild; run `971600085758769`).

Three findings, verified against the backend, shape the design:

- **F1 — reliable delete is two-part.** An SDK `delete_synced_table` leaves a PostgreSQL "ghost" table
  that must be dropped via `psycopg2` (`scripts/delete_synced_table.py`). An SDK-only delete that
  leaves a ghost lets the subsequent create hit "already exists" (the migrate create-all tolerates it)
  — a **false-positive heal** that reports success while the checkpoint is not reset.
- **F2 — CDF does not reliably survive a full-refresh.** Only marts whose dbt model sets
  `tblproperties={'delta.enableChangeDataFeed':'true'}` keep CDF after a recreate (`SHOW TBLPROPERTIES`
  on the stranded marts: 10 of 12 lacked it). A TRIGGERED synced table cannot be created over a CDF-off
  source — this is why the operator path has an explicit `ALTER … enableChangeDataFeed=true` step.
- **F3 — the daily job runs as a service principal that is not a PostgreSQL role**, so it cannot do
  F1's ghost drop. The destructive recreate therefore cannot run in the daily task.

## Decision

**Split the heal by privilege.** The daily `refresh_synced_tables` task (service principal) **detects**
the checkpoint mismatch (read-only) and **dispatches** remediation; the PG- and warehouse-capable
maintenance path (`lakebase-grants.yml` / `maintain_synced_tables.py` / `heal_synced_tables`)
**remediates** verify-before-destroy (ensure-CDF → SDK delete + PG ghost drop → create → re-trigger →
wait), then the existing grants + indexes passes restore the recreated table.

Hexagonal: four thin interface-segregated ports (`SyncedTableReaderPort` / `SyncedTableWriterPort` /
`PostgresGhostPort` / `WarehousePort`) with adapters (`src/ingestion/synced_table_lifecycle.py`).
Detection depends only on the read port, so the SP — which constructs only `SdkReaderAdapter` — has no
destructive method to call **by construction** (a type guarantee, not a convention). The operator
scripts (`migrate_synced_tables`, `delete_synced_table`) re-point to the same adapters (single source
of truth).

Key decisions and the findings/risks they answer:

- **Verify-before-destroy + no false positive.** The heal re-checks the mismatch is still present
  before deleting, aborts without deleting on any doubt, and treats a create "already exists" as
  `HEAL_FAILED`, never `HEALED` (closes F1's false-positive class).
- **`ensure_cdf` is load-bearing, not defensive.** Because CDF is off on some full-refreshed marts
  (F2), the heal's idempotent `ALTER` is required until each mart is next full-refreshed under the new
  contract test; persisting CDF in all 12 dbt models is the root-cause fix that ships with this change.
- **Detection never destroys (H1).** The SP path performs zero destructive ops; a half-done recreate is
  worse than stale-but-online, so the SP only detects + dispatches.
- **Recurrence-RED via timestamped strand state (H3).** A per-table append-only event log
  (`observability.synced_table_strand_state`) records `stranded` (SP) and `healed` (maintenance)
  events. The daily task is green-with-warning on first detection of an incident and RED only when a
  strand is still unhealed at the next detection (`last_stranded_at` newer than `last_healed_at`).
  A heal clears it, so a strand→heal→re-strand weeks later is a new incident (green), not a failure.
  `HEAL_FAILED` logs at ERROR (never warning) so a permanently-failing heal is visible to error-log
  queries / paging — not a silent loop (the ADR-002 anti-pattern). Detect-side RED and the maintenance
  heal-pass ERROR log are emitted by two identities and must be correlated to diagnose; the recurrence
  window is one detect interval (~24 h).
- **Concurrency (H4).** The maintenance workflow has a `concurrency: lakebase-maintenance` group
  (`cancel-in-progress: false`) so its three triggers (daily cron / post-Terraform-Apply /
  `workflow_dispatch`) never delete+recreate the same table concurrently.
- **Strand-state has two writers in two runtimes (no shared Spark).** The detect task runs on
  Databricks (has pyspark) and appends `stranded` via `SparkStrandStateBackend` /`write_delta_table`
  (ADR-038 concurrent-commit retry). The heal runs in the GitHub Actions maintenance workflow, which
  installs the `[sdk]` extra but **NOT pyspark** — so it appends `healed` via a **SQL-warehouse INSERT**
  (`WarehouseStrandStateBackend`, reusing the heal's `sql_exec`), with a defensive
  `CREATE TABLE IF NOT EXISTS`. A Spark dependency in the heal was the 2026-06-06 regression
  (`ModuleNotFoundError: No module named 'pyspark'`) that crashed the heal before it healed anything;
  `test_heal_entry_is_pyspark_free` guards against re-introducing it. Recurrence READS stay on the
  Spark side; the warehouse backend's `read_latest` is unsupported by design. Appends (not upserts)
  keep both identities safe under the concurrency group, and a strand-state write failure in the heal
  is best-effort (logged at ERROR) — it never downgrades a successful heal.
- **Mid-sequence-failure recovery (M4).** If create fails after delete, the table is absent; the
  maintenance create-all pass recreates any missing `SYNCED_TABLES` entry on its next run.
- **Kill-switch (P3).** `SYNCED_TABLE_HEAL_ENABLED=0` disables all destructive heal without a deploy.

## Consequences

- A full-refresh rebuild no longer strands synced tables: the daily detect task flags + dispatches, and
  the maintenance path recreates + regrants + reindexes. The app-invisibility window for a recreated
  table shrinks from ≤24 h to minutes when the `workflow_dispatch` GH token is configured (else the
  daily-maintenance backstop heals within ≤24 h).
- The destructive automation deletes production synced tables; this is bounded by marker-gating
  (SQLSTATE `XXKST` only), verify-before-destroy, the kill-switch, the workflow concurrency group, and
  the serverless e2e (`synced-table-heal-e2e.yml`) that proves the live path. That e2e runs **nightly +
  on-demand (`workflow_dispatch`), not as a PR gate**: its reproduction depends on non-deterministic
  live DLT pipeline scheduling (empirically, the same code reached the heal on one run and timed out at
  the repro step on the next) and a run takes ~15 min against real infra, so the deterministic offline
  suite is the merge gate. The e2e earned its keep during this ADR's own development — it surfaced a real
  heal bug (an explicit `start_update` after recreate races the auto-started initial sync → `ResourceConflict`;
  fixed by tolerating that conflict in `SdkWriterAdapter.trigger_refresh`).
- The Lakebase PG host is **derived** from the Databricks REST API (`ingestion.lakebase_endpoint.derive_lakebase_dns`,
  the importable home of the contract already used by `create_indexes.py` / `run_lakebase_grants.py`);
  `LAKEBASE_HOST` is honoured only as a local-dev override, so neither the heal step nor the e2e needs a
  hand-set host var in CI. The e2e's **only** operator input is the repo variable `HEAL_E2E_SCHEMA`
  (recommended `heal_e2e`) — a disposable UC schema the test creates/breaks/heals a throwaway table in.
- TRIGGERED/CDF incremental sync is preserved (no fallback to SNAPSHOT). A new contract test
  (`test_synced_table_cdf_contract`) prevents anyone re-opening F2 by adding a TRIGGERED synced table
  over a CDF-off source.

References: ADR-002 (no silent warning-swallows), ADR-005 (synced-table grants), ADR-018 (format-contract
tests), ADR-026 (SDK-managed synced-table lifecycle), ADR-038 (concurrent-commit retry).
