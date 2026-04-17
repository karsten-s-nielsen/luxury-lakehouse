# SEC4 Workspace Resource Inventory — CI SP Auth Paths

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Living document — update on every TF-adding PR |
| **Verification identity** | `karstenskyt@gmail.com` (account admin) via `databricks-cli` v0.290.1 DEFAULT profile — see §Verification method for the "Option B" note |
| **Verification time** | 2026-04-17 17:46 UTC |
| **Spec** | [2026-04-17-sec4-ci-sp-least-privilege-design.md](2026-04-17-sec4-ci-sp-least-privilege-design.md) |
| **Plan** | [../plans/2026-04-17-sec4-ci-sp-least-privilege.md](../plans/2026-04-17-sec4-ci-sp-least-privilege.md) |

## Purpose

Enumerate every Terraform-managed resource the Terraform CI service principal
(`luxury-lakehouse-terraform-ci-dev`, application_id `521f5d6a-cfd4-4fe1-a5cb-d5b12e247276`,
internal id `74583001200167`) touches, classify its current auth path (admins-group
transitive vs explicit ACL/grant vs account-admin), and document the target post-SEC4
state. A future TF-adding PR must add a row here and confirm the CI SP has explicit
auth before merge.

## Verification method

The Terraform CI SP is configured for OIDC-federation-only authentication
(`terraform/modules/service_principals/main.tf:97-113`). It has **zero persistent
client secrets** — verified via:

```bash
databricks --profile ACCOUNT account service-principal-secrets list 74583001200167
# → []
```

Consequently, live "run as CI SP" probes are impossible without minting a
temporary secret (which the user declined for this audit — see Option B in the
SEC4 cycle brainstorming). All verification commands below run as the account
admin identity (`karstenskyt@gmail.com`) and read ACLs from the controller side
(`databricks permissions get` / `databricks grants get`). This returns identical
data to what the CI SP itself would see when introspecting an object's ACL, but
does NOT empirically prove the CI SP can exercise those permissions. For that,
`terraform plan -refresh-only` after Phase 2 apply is the authoritative test,
augmented by the PR's `terraform-plan.yml` CI workflow which runs as the CI SP
via OIDC.

## Inventory

### Workspace-scoped Databricks resources

| Resource | CI SP action | Current auth | Target auth | Action needed |
|---|---|---|---|---|
| `module.workflows.databricks_job.data_ingestion` (id `302697362345215`) | create/update | Explicit `IS_OWNER` (PR #128) | Same | **None** |
| `module.sql_warehouse.databricks_sql_endpoint.serverless` (id `6c3b36ca64d183fe`) | create/update | Explicit `CAN_MANAGE` (PR #126) | Same | **None** |
| `module.lakebase.databricks_postgres_project.soccer_analytics` (uuid `342068ec-4162-4798-bed5-0aa4cbf326ba`, name `soccer-analytics-dev`) | create/update | **Admins-group transitive CAN_MANAGE** (inherited from `/database-projects`) — CI SP has NO explicit entry | Explicit `CAN_MANAGE` via `databricks_permissions` on `database-projects` object type | **Item A — add ACL block** |
| `module.lakebase.databricks_postgres_endpoint.primary` (`projects/soccer-analytics-dev/branches/production/endpoints/primary`) | create/update | No separate ACL surface (endpoints inherit from parent project) | Same (inherit from project ACL in item A) | **None** (resolved via parent project ACL) |
| `module.synced_tables.databricks_database_synced_database_table.*` (34 resources) | create/update (initial import only) | `lifecycle { ignore_changes = all }` — TF doesn't apply updates post-import | Same | N/A (documented in TODO.md technical debt #1) |

### Unity-Catalog–scoped Databricks resources

| Resource | CI SP action | Current auth | Target auth | Action needed |
|---|---|---|---|---|
| `module.workspace.databricks_catalog.soccer_analytics` | create/update | `ALL_PRIVILEGES` grant (explicit, `terraform/environments/dev/main.tf:172-176`) + MANAGE via `dbt-owners-dev` group ownership (established by one-time `ALTER CATALOG`) | Same | **None** |
| `module.catalog.databricks_schema.bronze` | create/update | Same as catalog (CI SP `ALL_PRIVILEGES` inherits; schema owned by `dbt-owners-dev` group with CI SP as member) | Same | **None** |
| `module.catalog.databricks_schema.silver` | create/update | Same as catalog | Same | **None** |
| `module.catalog.databricks_schema.gold` | create/update | Same as catalog | Same | **None** |
| `module.catalog.databricks_schema.observability` | create/update | Same as catalog | Same | **None** |
| `module.catalog.databricks_volume.libs` | create/update | Same as catalog | Same | **None** |
| `module.catalog.databricks_volume.training_data` | create/update | Same as catalog | Same | **None** |
| `module.catalog.databricks_grant.*` (16 grant resources) | create/update | MANAGE via ownership (`dbt-owners-dev`) | Same | **None** |

### Account-scoped Databricks resources

These 11 resources are managed exclusively through the `databricks.account` provider
alias, which requires account-level auth. The CI SP holds `account_admin` via
`databricks_service_principal_role.terraform_ci_account_admin` (`terraform/modules/service_principals/main.tf:91-95`).
This role is retained as the floor per [ADR-006](../adrs/ADR-006-account-admin-floor.md) —
the SEC4 spike confirmed empirically that every one of the 11 resources below
returns HTTP 403 "This API is disabled for users without account admin status"
when called as the CI SP without the role (4 representative probe ReqIds cited
in the ADR §Notes).

| Resource | CI SP action | Current auth | Target auth (post-SEC4) |
|---|---|---|---|
| `module.service_principals.databricks_service_principal.ingestion` | create | `account_admin` | `account_admin` ([ADR-006](../adrs/ADR-006-account-admin-floor.md)) |
| `module.service_principals.databricks_service_principal.terraform_ci` | create | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_service_principal.hf_app` | create | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_service_principal_role.terraform_ci_account_admin` | self-manage | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_service_principal_federation_policy.github_actions` | create/update | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_access_control_rule_set.ingestion_sp_user_role` | create/update | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_group.dbt_owners` | create/update | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_group_member.dbt_owners_deployer` | create | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_group_member.dbt_owners_ingestion_sp` | create | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_group_member.dbt_owners_terraform_ci_sp` | create | `account_admin` | `account_admin` (ADR-006) |
| `module.service_principals.databricks_mws_permission_assignment.dbt_owners_workspace` | create | `account_admin` | `account_admin` (ADR-006) |

Count correction vs design spec's starting-hypothesis: the spec said 8; the audit
found 11 (the design undercounted by conflating the 3 SP resources and 3 group-
member resources). Captured in §Surprises item 5 below.

### AWS resources (no Databricks auth involved)

| Resource | CI SP action | Current auth | Target auth | Action needed |
|---|---|---|---|---|
| `module.state_kms.aws_kms_key.terraform_state` | create/update | IAM role (`luxury-lakehouse-github-actions-dev`) via OIDC | Same | **None** |
| `module.state_kms.aws_kms_alias.terraform_state` | create/update | Same IAM role | Same | **None** |
| `module.state_kms.aws_s3_bucket_lifecycle_configuration.state` | create/update | Same IAM role | Same | **None** |
| `module.state_kms.aws_s3_bucket_server_side_encryption_configuration.state` | create/update | Same IAM role | Same | **None** |
| `module.github_oidc.aws_iam_openid_connect_provider.github` | create/update | Same IAM role | Same | **None** |
| `module.github_oidc.aws_iam_role.github_actions` | create/update | Same IAM role | Same | **None** |
| `module.github_oidc.aws_iam_role_policy.terraform_state_access` | create/update | Same IAM role | Same | **None** |
| `aws_budgets_budget.monthly` | create/update | Same IAM role | Same | **None** |

### Data sources (read-only)

| Data source | Scope | Current auth | Status post-SEC4 |
|---|---|---|---|
| `data.databricks_group.admins` (`service_principals/main.tf:80-82`) | workspace | Any SCIM-read | **REMOVED** (no resource references it after `terraform_ci_admin` is deleted) |
| `data.databricks_user.deployer` | account | `account_admin` → Item B | Unchanged |
| `data.databricks_service_principal.ingestion_account` | account | `account_admin` → Item B | Unchanged |
| `data.databricks_service_principal.terraform_ci_account` | account | `account_admin` → Item B | Unchanged |

## Orphan PG role (item H — separate layer, Lakebase Postgres internal)

Pre-existing finding originally documented in [ADR-005 §Neutral (line 69)](../adrs/ADR-005-lakebase-synced-table-grants.md#neutral),
flagged as "a separate decision" during the 2026-04-17 warm-tier incident.
SEC4 absorbs the cleanup per the cycle's "kill pre-existing findings mid-cycle
when cheap and thematically adjacent" policy.

| Item | Pre-drop state (2026-04-17 17:xx UTC) | Post-drop state | Evidence |
|---|---|---|---|
| PG role `be66af99-5296-4fd9-887a-c081bce38bfa` | Present in `pg_roles`; login role; non-superuser; no memberships; no owned objects; no default privileges | Absent (`SELECT COUNT(*) FROM pg_roles WHERE rolname = '<uuid>'` → 0) | `src/tests/test_orphan_pg_role_absent.py` PASS |

### Pre-drop grant enumeration

**Schema USAGE**: `dev_gold`, `observability` (plus the 3 default schemas `pg_catalog`, `information_schema`, `public` that every role has via `PUBLIC`).

**Table-level SELECT grants** (17 total, all granted by `karstenskyt@gmail.com` during 2026-04-17 incident):

| Schema | Table |
|---|---|
| dev_gold | dim_players_synced |
| dev_gold | dim_teams_synced |
| dev_gold | fct_action_values_synced |
| dev_gold | fct_defcon_actions_synced |
| dev_gold | fct_defcon_pressure_synced |
| dev_gold | fct_defensive_values_synced |
| dev_gold | fct_gk_actions_detail_synced |
| dev_gold | fct_heatmap_agg_synced |
| dev_gold | fct_match_summary_synced |
| dev_gold | fct_passes_synced |
| dev_gold | fct_physical_stats_synced |
| dev_gold | fct_player_embeddings_synced |
| dev_gold | fct_player_stats_synced |
| dev_gold | fct_shots_synced |
| dev_gold | fct_vaep_breakdown_agg_synced |
| dev_gold | fct_xg_predictions_synced |
| observability | workflow_cost_live_synced |

### Drop execution (as `karstenskyt@gmail.com`, the historical grantor)

`REASSIGN OWNED` + `DROP OWNED` as written in the plan failed with `DependentObjectsStillExist` because the orphan role held privileges granted **to** it (by me) rather than privileges it granted to others or owned objects. `DROP OWNED` revokes privileges *granted by* the role, not *granted to* it. Corrected sequence:

1. `GRANT "be66af99-..." TO current_user` — needed per Postgres 14+ ACL rules, `DROP OWNED` requires membership.
2. For each of the 17 tables: `REVOKE SELECT ON TABLE "<schema>"."<table>" FROM "be66af99-..."`.
3. `REVOKE USAGE ON SCHEMA "dev_gold" FROM "be66af99-..."` + `REVOKE USAGE ON SCHEMA "observability" FROM "be66af99-..."`.
4. `DROP ROLE "be66af99-..."` — succeeded; `pg_roles` post-count = 0.

Regression guard: `src/tests/test_orphan_pg_role_absent.py` asserts `pg_roles` does not contain the UUID; skipped when `DATABRICKS_HOST` / `DATABRICKS_TOKEN` are absent.

## Verification commands (reproducible)

```bash
# Resource enumeration
grep -rn '^resource "databricks_\|^resource "aws_\|^data "databricks_' terraform/ --include='*.tf' | sort

# Workspace-scoped permissions (as admin, Option B)
databricks permissions get jobs 302697362345215
databricks permissions get warehouses 6c3b36ca64d183fe
databricks permissions get database-projects soccer-analytics-dev
# No endpoint probe — endpoint auth inherits from parent project.

# UC grants
databricks grants get catalog soccer_analytics
databricks grants get schema soccer_analytics.dev_gold
databricks grants get schema soccer_analytics.dev_silver
databricks grants get schema soccer_analytics.bronze
databricks grants get schema soccer_analytics.observability

# Workspace admins group membership
databricks groups list --filter 'displayName eq admins'          # → 84392195504304
databricks groups get 84392195504304
# Members: luxury-lakehouse-terraform-ci-dev (74583001200167) + karstenskyt@gmail.com

# CI SP secrets (proves OIDC-only posture)
DATABRICKS_HOST=https://accounts.cloud.databricks.com \
DATABRICKS_ACCOUNT_ID=7fc38190-9955-439c-b994-d96df7a1a4ab \
DATABRICKS_CONFIG_PROFILE=ACCOUNT \
    databricks account service-principal-secrets list 74583001200167
# → []
```

## Admin-removal reverted (SEC4 outcome, 2026-04-17)

The SEC4 cycle attempted to remove `databricks_group_member.terraform_ci_admin`
(CI SP workspace-admins-group membership) as its primary reduction step.
Phase 2 apply succeeded — admin membership was removed from both state and
live infrastructure. The subsequent `terraform plan` in CI (PR #146)
revealed two transitive-admin gaps not caught by local plan (which runs
as my admin identity, not as the CI SP):

1. **Lakebase pipeline VIEW** — Fixed by extending
   `scripts/grant_synced_table_permissions.py` with a new
   `_apply_ci_sp_pipeline_grants()` function granting CI SP `CAN_VIEW`
   on all 37 synced-table backing pipelines. This fix remains.
2. **Workspace SCIM reads on service principals** — 3 `databricks_service_principal`
   resources need admin-gated workspace-SCIM Get() calls during plan.
   Three attempts to reroute via account-SCIM (`api = "account"`,
   `provider = databricks.account`, targeted apply) all failed on
   TF-planner cascade: pending SP updates mark the SP's `id` as
   "known after apply", propagating through `data.databricks_service_principal.*_account`
   to `databricks_group_member.dbt_owners_*.member_id` (force-new),
   forcing destructive replacement of 2 group members. Full evidence
   in [ADR-007](../adrs/ADR-007-workspace-admin-floor.md) §Alternatives.

**Recovery**: admin-group membership was restored via `terraform apply`
(1 resource add, 0 changes, 0 destroys). The provider-lock was also
reverted from 1.113 (attempted for the `api` field) back to 1.112 to
clean up schema residue from the failed targeted apply. See ADR-007 §Notes
for the state-corruption details.

**Final SEC4 outcome (partial closure)**: the cycle closed 3 transitive paths
(Lakebase project, 37 pipelines, orphan PG role) and documented 2 irreducible
co-floors with empirical evidence (account_admin per ADR-006, workspace-admin
per ADR-007). The Terraform CI SP retains workspace-admins-group membership.

## Surprises & deviations from starting-hypothesis

1. **Lakebase `databricks_permissions` IS supported** — but via object type `database-projects`, not `postgres_project`. The provider/CLI uses the Databricks API's internal naming, which differs from the Terraform resource name (`databricks_postgres_project`). Confirmed via error message enumeration: `Expected one of {..., database-projects, database-instances, ...}`. This means item A's "primary path" (`databricks_permissions`) is viable — no fallback to `databricks_access_control_rule_set` needed.

2. **Lakebase endpoints have NO separate ACL surface** — `database-instances` is for the Provisioned product, not Autoscaling (which we use); the endpoint object has no `databricks_permissions` entry. Endpoints inherit auth from their parent project. **Item A shrinks from two resources to one**: a single `databricks_permissions` block on the `database-projects` object, not two (project + endpoint).

3. **CI SP has NO explicit Lakebase ACL today** — the only path it has is transitive via `admins` group (inherited `CAN_MANAGE` at `/database-projects` parent). This is the exact kind of gap SEC4 exists to close.

4. **Admins-group CAN_MANAGE is inherited at `/database-projects` parent path**, not set on the specific project. When we add CI SP CAN_MANAGE via `databricks_permissions` on the project, this inherited grant for `admins` group remains in place — SEC4 doesn't touch other admins-group members, only the CI SP. `databricks_permissions` being "authoritative per target object" in Terraform does NOT remove inherited permissions from parent paths.

5. **Account-scoped resource count is 11, not 8** — the spec conflated SP resources (3) + group-member resources (3) into fewer buckets. The spike in Task 2 needs to evaluate all 11 for narrower-role feasibility.

6. **Admins group has exactly 2 members** — the CI SP (to be removed) and me. Removing the CI SP leaves the user in. Regression tests should not overfit to "admins group must have exactly 1 member" because that is an account-admin concern unrelated to SEC4.

7. **SQL warehouse and ingestion job also have `admins` group inherited `CAN_MANAGE`** — same pattern as Lakebase. The SEC4 removal doesn't touch these inherited entries (they apply to *other* admins-group members, not the CI SP); it just removes the CI SP from being in the group.

8. **UC gold/silver/bronze schemas have NO explicit CI SP grant row** — the CI SP accesses them via (a) catalog-level `ALL_PRIVILEGES`, (b) membership in `dbt-owners-dev` group which owns them. This is an expected layered authorization pattern, not a gap. The audit probe in spec Task 1 Step 1.5 ("attempt a no-op schema operation as CI SP") was skipped per Option B; the TF state + dbt daily-job success history establishes this path works.
