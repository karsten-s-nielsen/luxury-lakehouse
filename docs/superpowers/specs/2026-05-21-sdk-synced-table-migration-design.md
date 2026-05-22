# SDK Synced Table Migration — Design Spec

## Goal

Migrate all 41 Lakebase synced tables from UI-created / Terraform-imported to SDK-managed via `w.postgres.create_synced_table()`. Remove Terraform synced_tables module. Promote 12 additional fact tables from SNAPSHOT to TRIGGERED (CDF-based) scheduling. Close TODO #1, G2, G3, and partially close PR-gamma.

## Motivation

- **Public repo blocker:** The Terraform `lifecycle { ignore_changes = all }` workaround and the "must create synced tables via UI" limitation are the last remaining issues preventing the repo from going public.
- **Operational overhead:** Every synced table recreation (post `--full-refresh`, schema change, or failure) requires manual UI creation in Databricks, followed by TF import, grants, and index recreation. SDK-managed tables make this fully scriptable.
- **G2 gap (confirmed):** `refresh_synced_tables.py` hits `/api/2.0/database/synced_tables/` (legacy Provisioned endpoint). SDK-created tables live under `/api/2.0/postgres/synced_tables/` (Autoscaling endpoint). The two paths are not interchangeable — an SDK-created table is not addressable by the current refresh module.
- **G3 gap (unverified):** Grants and event_log ownership behavior against SDK-created tables is unverified. Empirical smoke test required before committing to a full migration.
- **TRIGGERED opportunity:** Recreating all 41 tables is the zero-marginal-cost moment to switch 12 large append-mostly fact tables from SNAPSHOT (full-copy-on-every-refresh) to TRIGGERED (CDF-based incremental).

## Context: SDK Investigation (2026-05-21)

Databricks SDK 0.110.0 `PostgresAPI` supports full CRUD for synced tables:

| Operation | Method | Endpoint | Status |
|-----------|--------|----------|--------|
| Create | `w.postgres.create_synced_table()` | `POST /api/2.0/postgres/synced_tables` | Working |
| Get | `w.postgres.get_synced_table()` | `GET /api/2.0/postgres/synced_tables/{name}` | Working |
| Delete | `w.postgres.delete_synced_table()` | `DELETE /api/2.0/postgres/synced_tables/{name}` | Working |
| List | `w.database.list_synced_database_tables()` | `GET /api/2.0/database/synced_tables` | Not implemented server-side |

Empirically verified: created `dim_competitions_synced_sdk_test` via SDK, confirmed provisioning, deleted cleanly.

Required fields for `create_synced_table`:

```python
SyncedTableSyncedTableSpec(
    source_table_full_name="catalog.schema.source_table",
    branch="projects/<project>/branches/production",
    primary_key_columns=["pk_col"],
    scheduling_policy=SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy.SNAPSHOT,
    postgres_database="databricks_postgres",
    create_database_objects_if_missing=True,
)
```

Name formats:
- `synced_table_id` (for create): `catalog.schema.synced_table_name`
- `name` (for get/delete): `synced_tables/catalog.schema.synced_table_name`

## Architecture

### 1. SyncedTableConfig — Single Source of Truth

The `SYNCED_TABLES` constant in `src/ingestion/refresh_synced_tables.py` is promoted from `list[tuple[str, str | None]]` to `list[SyncedTableConfig]`:

```python
@dataclass(frozen=True)
class SyncedTableConfig:
    name: str                           # e.g. "fct_shots_synced"
    source_table: str                   # e.g. "fct_shots"
    primary_key_columns: tuple[str, ...]
    scheduling_policy: str = "SNAPSHOT"  # "SNAPSHOT" | "TRIGGERED"
    schema_override: str | None = None   # None -> DEFAULT_SCHEMA ("dev_gold")
```

All 41 table definitions migrated from Terraform HCL into this list. `source_table` is the short name — the full UC path is derived at runtime from `DEFAULT_CATALOG` + schema.

Every consumer — create, delete, refresh, grants, indexes — reads from this single list. No metadata split between TF and Python.

### 2. Scheduling Policy Assignments

**TRIGGERED (15 tables)** — large, append-mostly facts:

| Table | Reason |
|-------|--------|
| `fct_passes` | Already TRIGGERED (PR-Cycle-C pilot) |
| `fct_action_values` | Already TRIGGERED (PR-Cycle-C pilot) |
| `fct_tracking_frames` | Already TRIGGERED (PR-Cycle-C pilot) |
| `fct_defensive_values` | ~3.75M rows, append per match |
| `fct_defcon_actions` | ~5.67M rows, append per match |
| `fct_defcon_pressure` | ~3.5M rows, append per match |
| `fct_player_embeddings` | ~5.68M rows, append per match |
| `fct_line_breaking_results` | ~2M rows, append per match |
| `fct_off_ball_xt` | ~1M rows, append per match |
| `fct_pausa_values` | ~5M rows, append per match |
| `fct_tracking_shape_timeline` | ~3.5M rows, append per match |
| `fct_space_creation` | ~1M rows, append per match |

**SNAPSHOT (26 tables)** — small dims, full-rebuild aggregations, observability:

All `dim_*` tables (4), pre-agg marts (`fct_heatmap_agg`, `fct_vaep_breakdown_agg`, `fct_funnel_stages_agg`, `fct_gk_actions_detail`), cost/observability tables, ranking/percentile tables, embedding season/career tables (full rebuild), `fct_match_summary`, `fct_formation_labels`, `fct_pass_timing`, `fct_player_stats`, `fct_physical_stats`, `fct_tracking_avg_positions`, `fct_tracking_context`, `fct_goalkeeper_stats`, `fct_shots`, `fct_xg_predictions_v2`, `fct_discipline_events`, `fct_player_positions`, `fct_position_maps`.

### 3. CDF Enablement — Declarative, Derived from Config

Any code path that creates synced tables runs a pre-flight step:

```python
for config in SYNCED_TABLES:
    if config.scheduling_policy == "TRIGGERED":
        spark.sql(f"ALTER TABLE {source_fqn} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
```

Idempotent — already-enabled tables are a no-op. CDF enablement is derived from `SyncedTableConfig.scheduling_policy`, not tracked separately. Future table additions with `scheduling_policy="TRIGGERED"` get CDF automatically.

For the migration script (runs from local machine, not Spark), CDF enablement uses the Databricks SQL Statement Execution API (`w.statement_execution.execute_statement()`).

### 4. Migration Script — `scripts/migrate_synced_tables.py`

Standalone one-shot script. Four phases:

**Phase 0 — Smoke test.**
Create a throwaway synced table (`dim_competitions_synced_sdk_test`) via SDK. Run grants (PG SELECT) + ownership check (event_log pipeline ownership) against it. Delete it. If anything fails, abort before touching real tables. This empirically closes G3.

**Phase 1 — Delete all 41.**
Call `w.postgres.delete_synced_table()` sequentially. Each returns a `DeleteSyncedTableOperation`. Tolerate "not found" errors (table may already be gone from a prior partial run).

**Phase 2 — Enable CDF on TRIGGERED source tables.**
For the 12 newly-TRIGGERED tables (the 3 existing ones already have CDF), run `ALTER TABLE SET TBLPROPERTIES` via Statement Execution API. Idempotent.

**Phase 3 — Create all 41.**
Call `w.postgres.create_synced_table()` sequentially for each `SyncedTableConfig`, passing:
- `source_table_full_name` — derived from config
- `branch` — `"projects/soccer-analytics-dev/branches/production"`
- `primary_key_columns` — from config
- `scheduling_policy` — from config
- `postgres_database` — `"databricks_postgres"`
- `create_database_objects_if_missing` — `True`

**Phase 4 — Wait + maintain.**
Poll all 41 tables in parallel (`ThreadPoolExecutor`) using `wait_until_online()` (G1 helper, already in codebase but needs postgres API path update). Once all online, run the full `maintain_synced_tables.py --skip-refresh` pipeline (ownership -> grants -> indexes -> verify).

Idempotent — re-running after partial failure picks up where it left off.

### 5. Refresh Module Update (G2 Closure)

`src/ingestion/refresh_synced_tables.py` changes:

- `refresh_pipeline()` switches from raw REST calls to `w.postgres.get_synced_table()` for status + pipeline ID retrieval
- Pipeline trigger stays on `/api/2.0/pipelines/{id}/updates` (same for both paths — the pipeline ID is the bridge)
- `wait_until_online()` switches from raw REST to `w.postgres.get_synced_table()`, checks `status.detailed_state`
- No dual-path logic — big-bang migration means all 41 tables are on the postgres path
- The `_derive_upstream_tables()` helper updated to iterate `SyncedTableConfig` objects

### 6. Grants & Ownership (G3 Closure)

**`scripts/run_lakebase_grants.py`** — No changes expected. Operates on PG-side objects via psycopg2 (`GRANT SELECT`). PG table name is the same regardless of creation path.

**`scripts/fix_event_log_ownership.py`** — Switches pipeline ID lookup from `w.database.get_synced_database_table()` to `w.postgres.get_synced_table()`. Name format changes from `catalog.schema.table` to `synced_tables/catalog.schema.table`.

**`scripts/grant_synced_table_permissions.py`** — Same switch for pipeline ID lookup. CAN_RUN grants on pipelines and CAN_USE on database project are pipeline/project-level — unaffected by creation path.

### 7. Terraform Cleanup

**Delete:** entire `terraform/modules/synced_tables/` module (main.tf, variables.tf).

**Modify:** `terraform/environments/dev/main.tf` — remove `module "synced_tables" { ... }` block.

**State cleanup:** Before `terraform apply`, remove all 41 resources from state:
```bash
terraform state rm 'module.synced_tables.databricks_database_synced_database_table.fct_shots'
# ... (41 resources)
```
Script or loop to batch this. Must run before `terraform apply` — otherwise TF attempts to destroy the resources (which fails with "Database instance is not found").

### 8. CI & Test Updates

**`tests/data_quality/test_synced_tables_online.py`:**
- Switch from raw `requests.get()` against `/api/2.0/database/synced_tables/` to `w.postgres.get_synced_table()` SDK method (consistent with all other consumers — no raw REST calls remain)
- Update iteration to unpack `SyncedTableConfig` instead of `(name, schema_override)` tuples

**`.github/workflows/lakebase-grants.yml`:**
- No workflow-level changes needed. The 4-step pipeline calls Python scripts, which we're updating. Daily 07:00 UTC schedule trigger and post-TF-apply trigger both still valid.

**`src/tests/test_refresh_synced_tables.py`:**
- Update mocks for postgres API path instead of database path
- Add tests for `SyncedTableConfig` dataclass
- Add tests for CDF-enablement pre-flight logic

### 9. ADR & Documentation Updates

**Create:** `docs/superpowers/adrs/ADR-026-sdk-managed-synced-table-lifecycle.md`
- Decision: SDK-managed via `w.postgres.create_synced_table()`
- Context: TF provider limitation (#5456), public repo blocker, G2+G3 gaps
- Consequences: TF module removed, Python SSOT, 12 tables promoted to TRIGGERED

**Update:**
- `TODO.md` — Close #1 (TF workaround), G2G3 (SDK hardening). Update PR-gamma to note 12 tables already migrated, remaining candidates are follow-up.
- `docs/superpowers/adrs/ADR-005-lakebase-synced-table-grants.md` — Note SDK path replaces UI+TF workflow
- `docs/engineering/conventions.md` — Update Lakebase Ops recreation procedure
- `CLAUDE.md` — Update index recreation bullet to reference SDK path
- `memory/feedback_synced_table_triggered_mode_requires_cdf.md` — Note CDF enablement is now automatic

## File Change Inventory

| File | Action | What changes |
|------|--------|-------------|
| `src/ingestion/refresh_synced_tables.py` | Modify | `SyncedTableConfig` dataclass, 41 configs with PK + policy, switch `/database/` to `/postgres/` SDK, CDF pre-flight |
| `scripts/migrate_synced_tables.py` | Create | One-shot: smoke test -> delete 41 -> enable CDF -> create 41 -> wait -> maintain |
| `scripts/delete_synced_table.py` | Modify | Switch to `w.postgres.delete_synced_table()` |
| `scripts/fix_event_log_ownership.py` | Modify | Switch pipeline ID lookup to postgres API |
| `scripts/grant_synced_table_permissions.py` | Modify | Switch pipeline ID lookup to postgres API |
| `tests/data_quality/test_synced_tables_online.py` | Modify | Switch to postgres API, unpack `SyncedTableConfig` |
| `src/tests/test_refresh_synced_tables.py` | Modify | Update mocks for postgres API, add config + CDF tests |
| `terraform/modules/synced_tables/` | Delete | Entire module |
| `terraform/environments/dev/main.tf` | Modify | Remove `module "synced_tables"` block |
| `docs/superpowers/adrs/ADR-026-*.md` | Create | SDK-managed synced table lifecycle |
| `TODO.md` | Modify | Close #1, G2G3, update PR-gamma |
| `docs/superpowers/adrs/ADR-005-*.md` | Modify | Note SDK replaces UI+TF |
| `docs/engineering/conventions.md` | Modify | Update Lakebase Ops |
| `CLAUDE.md` | Modify | Update index recreation bullet |
| `pyproject.toml` | Modify | Pin `databricks-sdk==0.110.0` (already done) |

## Out of Scope

- PR-gamma remaining candidates (tables not in the 12 high-confidence set)
- Terraform provider #5456 follow-up (if/when Autoscaling support ships)
- CONTINUOUS scheduling mode
- Synced table list endpoint (server-side `NotImplemented`)

## Success Criteria

1. All 41 synced tables online via SDK-created path (`SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`)
2. Phase 0 smoke test passes (create + grants + ownership + delete)
3. Full `maintain_synced_tables.py` pipeline passes (ownership -> grants -> indexes -> verify)
4. `test_synced_tables_online.py` green in CI
5. `terraform plan` shows no synced-table drift (resources removed from state + config)
6. No legacy `/api/2.0/database/synced_tables/` calls remain in codebase
7. 15 tables TRIGGERED (3 existing + 12 promoted), 26 tables SNAPSHOT
8. CDF enabled on all 15 TRIGGERED source tables

## Migration Approach

**Approach 1 — Sequential delete-then-create with parallelized polling.**

Delete all 41 sequentially (each is an async Operation — fast). Create all 41 sequentially. Poll all in parallel for ONLINE state via ThreadPoolExecutor. Grants + indexes after all are online.

Rationale: API calls are fast (async Operations). Wall-clock is dominated by DLT pipeline provisioning, which happens in parallel on Databricks' side. Sequential dispatch + parallel polling gives shortest wall-clock with simplest code.

No downtime coordination (app is low-traffic, ~30 min degraded window acceptable).

## Dependencies

- `databricks-sdk==0.110.0` (pin already applied in this session)
- Databricks workspace with admin PAT (same as current)
- Live Databricks SQL warehouse for CDF enablement (Statement Execution API)

## Clean Path to PR-gamma

PR-gamma (SNAPSHOT -> TRIGGERED migration for additional candidates) is a subset of this cycle's functionality. After this cycle ships:
- Any new TRIGGERED candidate: add a `SyncedTableConfig` with `scheduling_policy="TRIGGERED"`, run `migrate_synced_tables.py` (or a future `recreate_synced_table.py` helper). CDF enablement is automatic.
- No manual UI steps, no TF imports, no coordinated multi-tool workflow.
