# ADR-020: Lakebase CAN_RUN Workspace-ACL Auto-Heal in `lakebase-grants.yml`

| Field | Value |
|---|---|
| **Date** | 2026-05-02 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

`lakebase-grants.yml` (the daily 07:00 UTC self-healing workflow) ran three idempotent passes through 2026-05-01: `fix_event_log_ownership.py` → `run_lakebase_grants.py` (PG SELECT grants) → `create_indexes.py --verify`. None of those re-applied the **workspace-API ACL** surface — specifically the `CAN_RUN` permission on each synced table's backing pipeline that the Lakebase Refresh API checks.

PR-Cycle-C PR-α (2026-05-01) ran the PR-γ pilot's first synced-table UI recreation (career + season embedding tables, plus the 3 SNAPSHOT→TRIGGERED conversions). Within 24 hours the daily-job's `refresh_synced_tables` task and the Taipy admin endpoint's pipeline-refresh path both 403-ed silently on the recreated tables. Diagnosis: UI recreation creates new `pipeline_id`s AND transfers ownership to whoever performed the recreation. Both side effects reset the workspace-API ACL surface to default. The 3 self-healing steps in `lakebase-grants.yml` cover the PG schema surface and the dbt-owners surface — but not the workspace ACL surface.

`scripts/grant_synced_table_permissions.py` already exists, is idempotent, and handles all three SP CAN_RUN grants in ~17s. It runs on demand from operator workstations (and as Step 0 of `scripts/maintain_synced_tables.py`), but had no scheduled cadence.

The forcing function is empirical: 2026-05-01 403 errors on 2 embedding synced tables that were UI-recreated yesterday. Manual re-running of the script restored access; without a scheduled re-run, every future UI recreation reopens the 24-hour failure window.

## Decision

Add `scripts/grant_synced_table_permissions.py` as a fourth idempotent step in `.github/workflows/lakebase-grants.yml`, slotted between `Apply Lakebase grants` (PG SELECT, step 2) and `Apply Lakebase indexes` (step 4). The new step inherits the workflow's existing triggers (daily 07:00 UTC cron + post-Terraform-Apply chained run + workflow_dispatch). No new secrets, no new permissions — the workflow already runs as an admin SP via `DATABRICKS_TOKEN`, which has `CAN_MANAGE` on the database project + `IS_OWNER` on each pipeline. The script's `--status` mode is documented as the verify pathway; this ADR does not add an explicit verify step (the script's idempotent grant-or-noop output is sufficient).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| **A.** Manual operator re-run after every UI recreation | Zero infrastructure change | Relies on 100% operator discipline; the 2026-05-01 incident proves this fails in practice; 24-hour failure window per gap | Rejected — same forcing-function structure as the original ADR-005 (PG grants needed daily re-apply because operator discipline failed) |
| **B.** Bake CAN_RUN into the synced-table TF declaration so a `terraform apply` re-applies | Single source of truth | TF synced-table resources have `lifecycle.ignore_changes = all` (because the Lakebase backing pipeline is auto-managed); CAN_RUN would not actually be applied through TF; would also fail on UI recreations that bypass TF | Rejected — TF doesn't own the live backing pipeline_id, so it can't apply ACLs against the right target |
| **C.** Daily auto-heal step in `lakebase-grants.yml` **(chosen)** | Same self-healing pattern as the 3 existing steps; idempotent; ~17s wall-clock; covers both UI recreations AND TF-driven pipeline_id rotation | Adds a 4th step (minor) | — |
| **D.** Move CAN_RUN application into the `refresh_synced_tables` Databricks task itself | One-time cost per refresh | The Databricks task runs as the ingestion SP, which doesn't have `CAN_MANAGE` on the database project (would require granting it). Granting `CAN_MANAGE` to the runtime SP is a privilege escalation we explicitly avoid (SEC4 cycle). The maintenance workflow runs as a more privileged SP. | Rejected — privilege boundary violation |

## Consequences

### Positive

- CAN_RUN drift after UI recreation auto-heals within 24 hours (worst case — depends on how soon the recreation happens after the 07:00 UTC cron). The `workflow_run` chained trigger from Terraform Apply also catches TF-driven recreations within minutes.
- Same self-healing pattern as ADR-005 (PG grants), `fix_event_log_ownership.py` (event-log ownership), and `create_indexes.py --verify` (PG indexes). One mental model covers all four ACL/permission surfaces.
- Operator discipline is no longer the SLA-keeper — the cron is. The script remains available for on-demand operator use.
- Adding a new synced table to `ingestion.refresh_synced_tables.SYNCED_TABLES` is automatically covered (the script reads from that single registry).

### Negative

- 17s added wall-clock to the daily 07:00 UTC workflow (negligible — total workflow is now ~3-4 min).
- A new failure mode: if `grant_synced_table_permissions.py` itself breaks (e.g. SDK API change), the workflow fails AND the next operator who tries to refresh a UI-recreated table will be hit by the 403. Mitigation: the script is exercised on every workflow run, so breakage is caught within 24 hours rather than at the next operator demand.

### Neutral

- The script already runs as Step 0 of `scripts/maintain_synced_tables.py` — that local-dev path is unchanged.
- No CLAUDE.md amendment needed — the lakebase-grants.yml workflow's design philosophy ("coverage before blame; daily re-apply because pg_default_acl is structurally unavailable") is unchanged.

## Related

- **Predecessors**:
  - ADR-005 — Lakebase Synced Table Grants (the original PG-side compensating control); this ADR extends the same pattern to the workspace-ACL surface.
  - PR-Cycle-C PR-α (PR #243, commit `fb52bdc`, 2026-05-01) — UI recreation of pilot synced tables exposed the gap.
- **Empirical motivation**: 2026-05-01 403 errors on `fct_player_embeddings_career_synced` + `fct_player_embeddings_season_synced` after UI recreation; 24-hour resolution gap pre-fix.
- **Implementation**: `.github/workflows/lakebase-grants.yml` step 3; `scripts/grant_synced_table_permissions.py` (unchanged).
- **Memory**:
  - `feedback_synced_table_deletion.md` — UI-recreation pattern that triggers the drift.
  - `project_pr_cycle_c_alpha_complete.md` § "CAN_RUN grant gap confirmed" — diagnosis log.

## Notes

The script ships **without** a `--status`-only step in this workflow because the operator-friendly idempotent-grant-or-noop output covers the verification path naturally. If a separate verify-only step becomes useful later (e.g. for incident response dashboards), it can be added under the `verify_only` workflow_dispatch input — same pattern as the existing index verify step.

Same structural mitigation applies to future ACL surfaces (Unity Catalog, Lakebase database-project SELECT escalation, etc.): if any new ACL drifts after UI / TF lifecycle events, the canonical answer is "add an idempotent re-apply step to lakebase-grants.yml" rather than relying on operator discipline.
