# ADR-026: SDK-Managed Synced Table Lifecycle

| Field | Value |
|---|---|
| **Date** | 2026-05-22 |
| **Status** | Accepted |
| **Deciders** | Karsten Skyt Nielsen |

## Context

Lakebase synced tables were created via the Databricks UI (the only supported path for Autoscaling projects) and imported into Terraform state. The Terraform provider (`databricks_database_synced_database_table`) only exposed `database_instance_name` (Provisioned path), not the project/branch selection needed for Autoscaling. The `lifecycle { ignore_changes = all }` workaround made TF a config-free import shell: no updates, no real management.

Three gaps blocked the public-repo goal:

- **G1 (closed PR 4b):** `wait_until_online()` helper for post-creation polling.
- **G2:** `refresh_synced_tables.py` hit `/api/2.0/database/synced_tables/` (legacy Provisioned endpoint). SDK-created tables live under `/api/2.0/postgres/synced_tables/` (Autoscaling endpoint). The two paths are not interchangeable.
- **G3:** Grants and event_log ownership against SDK-created tables was unverified.

Databricks SDK 0.110.0 shipped `PostgresAPI` with full CRUD: `create_synced_table`, `get_synced_table`, `delete_synced_table`. Empirically verified (2026-05-21) on a throwaway `dim_competitions_synced_sdk_test` table.

## Decision

All 41 Lakebase synced tables are managed via `w.postgres.create_synced_table()` from the Databricks SDK. The Terraform `synced_tables` module is removed entirely. The `SYNCED_TABLES` constant in `src/ingestion/refresh_synced_tables.py` is the single source of truth, promoted from `list[tuple[str, str | None]]` to `list[SyncedTableConfig]` with frozen dataclass carrying name, source table, primary key columns, scheduling policy, and schema override.

12 tables use TRIGGERED (CDF-based) scheduling; 29 use SNAPSHOT.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Wait for TF provider Autoscaling support | Zero custom tooling | Provider issue #5456 has no timeline; blocks public repo indefinitely | Indefinite wait |
| B. Raw REST API wrapper | No SDK dependency | Fragile, no type safety, duplicates SDK work, two API paths to maintain | SDK exists and works |
| C. SDK-managed (chosen) | Type-safe, single API path, full CRUD, idempotent migration script | SDK version pin (0.110.0) | -- |

## Consequences

### Positive

- Public repo blocker removed (no `lifecycle { ignore_changes = all }` hack).
- Synced table creation, deletion, and recreation are fully scriptable.
- 12 fact tables promoted to TRIGGERED (CDF-based incremental sync).
- Single source of truth for all synced table metadata (`SyncedTableConfig`).
- No legacy `/api/2.0/database/synced_tables/` calls remain in the codebase.

### Negative

- Hard dependency on `databricks-sdk>=0.110.0` (already in `[sdk]` optional extra).
- One-shot migration requires ~30 min downtime (delete + recreate + wait for ONLINE).
- Terraform state cleanup is a manual operator action before `terraform apply`.

### Neutral

- Migration script (`scripts/migrate_synced_tables.py`) is a one-shot tool that remains for future table additions/recreations.
- CDF enablement is derived from `SyncedTableConfig.scheduling_policy` -- adding a TRIGGERED table automatically enables CDF.

## Related

- **Specs:** `docs/superpowers/specs/2026-05-21-sdk-synced-table-migration-design.md`
- **ADRs:** supersedes the TF workaround documented in `ADR-005`
- **External references:** Databricks SDK `PostgresAPI` (v0.110.0), TF provider issue #5456
