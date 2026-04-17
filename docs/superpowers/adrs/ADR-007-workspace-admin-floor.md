# ADR-007: Terraform CI SP Workspace-Admins-Group Membership Accepted as Co-Floor

| Field | Value |
|---|---|
| **Date** | 2026-04-17 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

ADR-006 accepted `account_admin` as the floor for the Terraform CI service
principal. The original SEC4 cycle (2026-04-17) also attempted to eliminate
the CI SP's workspace-admins-group membership via `databricks_group_member.terraform_ci_admin`
(`terraform/modules/service_principals/main.tf:84-87` pre-revert). Phase 2 apply
succeeded and the membership was removed. The subsequent `terraform plan` in
GitHub Actions CI (PR #146) then failed with multiple error classes, each
traced back to the now-absent transitive workspace admin:

1. **Workspace SCIM reads on service principals** —
   `terraform plan` calls `Get()` on every `databricks_service_principal`
   resource. The default (workspace) provider routes SP reads to
   `/api/2.0/preview/scim/v2/ServicePrincipals/{id}`, which the Databricks
   platform gates on workspace admin. Without admins-group membership the
   CI SP hits HTTP 403 "is only accessible by admins" on all three SP reads.
2. **Lakebase pipeline reads** — `terraform plan` calls `Get()` on every
   `databricks_database_synced_database_table` (37 resources in dev),
   which requires `CAN_VIEW` on the backing pipeline. Before SEC4 this was
   transitive via admins-group at the `/pipelines/` parent path. (Addressed
   separately by adding explicit CAN_VIEW grants via
   `scripts/grant_synced_table_permissions.py` — this ADR is about the
   SP-read failure that remained after the pipeline fix.)

Attempted fixes and their failure modes:

**Attempt 1: `api = "account"` on the 3 SP resources** (provider v1.113
feature, CHANGELOG: "Added `api` field to dual account/workspace resources").
Targeted `terraform apply` returned HTTP 404 ("Endpoint not found") on all
3 SPs, because the default provider has no `account_id` and routes the
account-SCIM call to the workspace host. The failed apply wrote `api = "account"`
to state anyway. Subsequent plan proposed a cascade (`2 to add, 3 to change,
2 to destroy`) involving forced replacement of `databricks_group_member.dbt_owners_{ingestion,terraform_ci}_sp`
because the SP's `id` became "known after apply", propagating through
`data.databricks_service_principal.*_account.id` to the group members'
`member_id` (a force-new attribute).

**Attempt 2: `provider = databricks.account` on the 3 SP resources**
(matching the existing account-scoped resource pattern — `databricks_group`,
`databricks_service_principal_role`, etc. use this alias). Plan produced
the same cascade: `2 to add, 3 to change, 2 to destroy`. Root cause is
the same TF plan-time graph pessimism — any pending update on an SP
resource marks its `id` as "known after apply" regardless of routing.

**Attempt 3: Targeted `terraform apply -target=<3 SPs>`** to avoid
applying the cascade at once. Failed with the same HTTP 404 as Attempt 1
because `api = "account"` + default provider routes to workspace host.
State was corrupted: `api = "account"` was persisted but live SPs were
unchanged. Provider v1.113 subsequently interpreted the state+config
divergence as "resource deleted" during refresh, proposing to destroy and
recreate all 3 SPs and every downstream resource dependent on their IDs
(11 `databricks_grant`s, 3 job/warehouse/Lakebase ACL `access_control` blocks,
2 group members, 1 federation policy, 1 service_principal_role — plan summary:
`22 to add, 4 to change, 18 to destroy`).

Recovery required reverting the provider lock from 1.113 back to 1.112 so
the `api` attribute exited the schema, allowing the residue in state to
become invisible to the provider. Subsequent plan was clean with only the
admins-group membership re-addition remaining (1 resource add). Admin
membership was re-applied against live state via `terraform apply`, which
restored the pre-SEC4 posture for the workspace-admin axis.

## Decision

Accept `databricks_group_member.terraform_ci_admin` (CI SP membership in
the workspace `admins` group) as a co-floor alongside `account_admin`
(ADR-006) for the Terraform CI service principal. The membership resource
is restored in `terraform/modules/service_principals/main.tf`. The three
transitive paths that SEC4 did successfully reduce — Lakebase project
ACL, Lakebase pipeline ACLs via the grants script, and the orphan
Postgres role cleanup — remain in place.

## Alternatives considered

### A. `api = "account"` attribute on the 3 SP resources (provider v1.113)

| Attribute | Value |
|---|---|
| **Pros** | Provider feature added specifically for this dual-API scenario. Non-invasive resource-level attribute. |
| **Cons** | Default workspace provider has no `account_id`; the provider constructs a malformed URL on the workspace host and returns HTTP 404. Failed apply corrupts state. |
| **Why rejected** | Empirically failed: 3× `Error: cannot update service principal: Endpoint not found for /2.0/accounts/7fc38190-.../scim/v2/ServicePrincipals/...` during `terraform apply -target=<SPs>`. State corruption required provider-lock rollback to recover. |

### B. `provider = databricks.account` alias on the 3 SP resources

| Attribute | Value |
|---|---|
| **Pros** | Matches the existing account-scoped resource pattern in the same module (group, group_member, service_principal_role, federation_policy, mws_permission_assignment all use this alias). |
| **Cons** | Still triggers the TF plan-time cascade. Any pending update on an SP resource marks its `id` as "known after apply", which propagates through `data.databricks_service_principal.*_account` data sources to their `id` output, which propagates to `databricks_group_member.dbt_owners_*.member_id` — a force-new attribute — triggering replacement. |
| **Why rejected** | Plan: `2 to add, 3 to change, 2 to destroy` — the 2 destroys are forced `databricks_group_member` replacements. Group membership destroy-then-create leaves a few-second window where the dbt-owners group is incomplete, risking concurrent dbt builds losing access. |

### C. Hardcode the 3 application_ids in the `data.databricks_service_principal.*_account` data sources

| Attribute | Value |
|---|---|
| **Pros** | Breaks the data-source dependency on the SP resource, so SP updates no longer cascade to group_members. |
| **Cons** | Loses the single-source-of-truth for the application_id. Hardcoded UUIDs are fragile to SP recreation. Ugly; introduces latent drift risk. |
| **Why rejected** | Trade-off of clarity for a fix that's already risky. Not pursued beyond analysis — user preferred the revert path. |

### D. Replace admins-group membership with `databricks_mws_permission_assignment` at ADMIN level

| Attribute | Value |
|---|---|
| **Pros** | Declarative, explicit in TF. Avoids dependency on the `admins` group's existence. |
| **Cons** | Still grants the CI SP workspace-admin privilege — effectively equivalent to the admin-group membership, just refactored. Doesn't reduce the privilege floor; only changes the grant mechanism. |
| **Why rejected** | Defeats the SEC4 privilege-reduction intent. Functionally identical outcome to keeping admin-group membership, with additional refactor cost. |

### E. Accept workspace admin as co-floor (**chosen**)

| Attribute | Value |
|---|---|
| **Pros** | Preserves the SEC4 wins that don't fight with TF graph cascades: Lakebase project ACL, pipeline VIEW grants, orphan PG cleanup, living inventory, ADR-006. Matches the SEC4 design spec's explicit R3 rollback contingency: "If Phase 2 apply breaks, rollback restores the state Phase 1 ended in." |
| **Cons** | `SEC-AUDIT-v1.12.0 INF-01` is only partially closed. The CI SP retains transitive workspace-admin access via admins-group membership. |
| **Why chosen** | The cascade is a TF-planner pessimism problem rooted in provider-schema limitations; none of Alternatives A–D produce a clean reduction without either (a) state corruption risk, (b) transient group_member absence, or (c) pointless refactoring. The partial closure is the highest-value outcome that doesn't require upstream provider improvements. |

## Consequences

### Positive

- **Four transitive paths closed by SEC4 remain** as explicit ACLs:
  - Lakebase project `CAN_MANAGE` via `databricks_permissions.lakebase_project_acl`.
  - 37 Lakebase pipeline `CAN_VIEW` grants via `scripts/grant_synced_table_permissions.py` (SEC4 extension).
  - Orphan Postgres role `be66af99-...` dropped.
  - SQL warehouse `CAN_MANAGE` (PR #126) + ingestion job `IS_OWNER` (PR #128) — pre-existing.
- **Living inventory** (`docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md`)
  catalogues every resource + its auth path, so future work can target specific
  transitive paths rather than rediscovering them.
- **Upstream path documented**: Attempts A and B failed on TF-planner
  pessimism, not on principle. A future provider release that addresses
  plan-time cascade on dual-API resources, or a TF language feature
  suppressing cascade on compatible schema changes, would reopen the
  reduction.

### Negative

- **SEC-AUDIT-v1.12.0 INF-01 is only partially closed.** The CI SP retains
  workspace-admins-group membership. A future compromise of the CI SP's
  OAuth-federation trust would confer workspace admin alongside the
  documented account_admin (ADR-006) and the pre-existing Unity Catalog
  `ALL_PRIVILEGES`.
- **State corruption risk demonstrated**: any future attempt to add
  `api = "account"` on SP resources without an accompanying
  `account_id`-configured provider instance will corrupt state again.
- **Provider lock rollback**: the terraform-provider-databricks version is
  pinned back to 1.112 (from the attempted 1.113 bump). 1.113 added
  `databricks_postgres_catalog` + `databricks_postgres_synced_table`
  resources which are now unavailable until a future deliberate upgrade.

### Neutral

- TF plan-time pessimism (marking downstream `id` as "known after apply"
  for any pending resource update) is fundamental Terraform behaviour.
  Addressing it would require either schema-level assertions that
  specific attributes are stable, or provider-level inference.

## CLAUDE.md Amendment

None.

## Related

- **Commits:** filled at commit time — this ADR is part of the SEC4 revert commit.
- **PR:** https://github.com/karsten-s-nielsen/luxury-lakehouse/pull/146 — original SEC4 PR; CI plan failures that led to this ADR are visible in the PR's Actions tab.
- **ADRs:** [ADR-006](ADR-006-account-admin-floor.md) — companion ADR for the account_admin floor.
- **Specs:** [2026-04-17-sec4-ci-sp-least-privilege-design.md](../specs/2026-04-17-sec4-ci-sp-least-privilege-design.md), [2026-04-17-sec4-workspace-resource-inventory.md](../specs/2026-04-17-sec4-workspace-resource-inventory.md) § post-revert updates.
- **Plan:** [2026-04-17-sec4-ci-sp-least-privilege.md](../plans/2026-04-17-sec4-ci-sp-least-privilege.md) § Risk R3 — the materialized risk that this ADR documents.
- **External references:**
  - Databricks Terraform provider: `github.com/databricks/terraform-provider-databricks@v1.112.0` (pinned) / `v1.113.0` (attempted-and-reverted).
  - Provider CHANGELOG v1.113.0 — "Added `api` field to dual account/workspace resources".

## Notes

### CI failure evidence (PR #146 Actions run 24585427175)

**Commit `a1a1275` (SEC4 pipeline CAN_VIEW fix), plan step output (excerpt):**

```
Error: cannot read service principal: https://dbc-48322be9-16be.cloud.databricks.com/api/2.0/preview/scim/v2/ServicePrincipals/77407294662421?attributes=userName,displayName,active,externalId,entitlements is only accessible by admins.
  with module.service_principals.databricks_service_principal.ingestion,
  on ../../modules/service_principals/main.tf line 13, in resource "databricks_service_principal" "ingestion":
  13: resource "databricks_service_principal" "ingestion" {
```

Plus two identical errors for `databricks_service_principal.terraform_ci` and
`databricks_service_principal.hf_app`.

### Targeted apply failure evidence (local, 2026-04-17)

```
Error: cannot update service principal: Endpoint not found for /2.0/accounts/7fc38190-9955-439c-b994-d96df7a1a4ab/scim/v2/ServicePrincipals/77407294662421
Error: cannot update service principal: Endpoint not found for /2.0/accounts/7fc38190-9955-439c-b994-d96df7a1a4ab/scim/v2/ServicePrincipals/74583001200167
Error: cannot update service principal: Endpoint not found for /2.0/accounts/7fc38190-9955-439c-b994-d96df7a1a4ab/scim/v2/ServicePrincipals/76742690586374
```

### Post-revert live-state verification

- `databricks groups get 84392195504304` — admins group contains both
  `luxury-lakehouse-terraform-ci-dev` (restored) and `karstenskyt@gmail.com`.
- `databricks permissions get database-projects soccer-analytics-dev` — CI SP retains explicit CAN_MANAGE from Phase 1 (Lakebase ACL unchanged).
- Sample pipeline ACL (`dim_teams_synced` backing) — CI SP retains explicit CAN_VIEW from the grants script run.
- `pg_roles` — orphan `be66af99-...` still absent.

### Forward-looking

Revisiting the workspace-admin reduction would require either:

1. An upstream provider change that decouples SP update plans from downstream
   `databricks_group_member.member_id` reference stability (file a bug in
   `github.com/databricks/terraform-provider-databricks` if this is not
   already tracked).
2. Restructuring the `databricks_group_member.dbt_owners_{ingestion,terraform_ci}_sp`
   resources to not depend on the SP's `id` — e.g., hardcoding application_ids
   in their data sources (Alternative C above, rejected for clarity reasons
   but technically viable).
3. Moving SP resource creation out of Terraform entirely (similar to the
   ADR-006 §Alternative C proposal), then granting CI SP `group.manager`
   and `servicePrincipal.manager` via rule sets — large refactor, separate
   cycle.
