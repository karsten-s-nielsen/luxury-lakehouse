# ──────────────────────────────────────────────────────────────────────────────
# Module: Service Principals — Least-Privilege Identities
# ──────────────────────────────────────────────────────────────────────────────
# Creates dedicated service principals for automated workloads so that
# jobs and apps run with scoped permissions instead of a PAT owner's
# full workspace privileges.
#
# Requires two provider configurations:
#   databricks         — workspace-level (SP creation, grants)
#   databricks.account — account-level (federation policies)
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_service_principal" "ingestion" {
  display_name = "luxury-lakehouse-ingestion-${var.environment}"
  active       = true
}

# ── Grant deploying user(s) the servicePrincipal.user role ───────────────────
# Required so that deployers can set `run_as` on jobs to this SP.
# L-9: Principals are configurable via var.deployer_user_names. Falls back
# to var.deployer_account_email when no explicit list is provided.
#
# Why not data.databricks_current_user.me?  When Terraform runs as a service
# principal (CI via OIDC federation), `databricks_current_user.me.user_name`
# returns the SP's application_id (UUID), NOT an email. Downstream
# data.databricks_user lookups by SP UUID return 404 because UUIDs aren't
# user names. Using the explicit var.deployer_account_email instead makes
# the lookup work identically for human-deployer (local) and SP-deployer (CI).

locals {
  deployer_principals = length(var.deployer_user_names) > 0 ? [
    for u in var.deployer_user_names : "users/${u}"
  ] : ["users/${var.deployer_account_email}"]
}

resource "databricks_access_control_rule_set" "ingestion_sp_user_role" {
  name = "accounts/${var.account_id}/servicePrincipals/${databricks_service_principal.ingestion.application_id}/ruleSets/default"

  grant_rules {
    principals = local.deployer_principals
    role       = "roles/servicePrincipal.user"
  }
}

# ── Terraform CI Service Principal ───────────────────────────────────────────
# Dedicated SP for CI/Terraform with GitHub OIDC federation so GitHub Actions
# can authenticate to Databricks without storing secrets (M-6).

resource "databricks_service_principal" "terraform_ci" {
  display_name = "luxury-lakehouse-terraform-ci-${var.environment}"
  active       = true
}

# ── CI SP Roles: Three-Privilege Floor (SEC4 partial closure, 2026-04-17) ──
# The SEC4 cycle attempted to eliminate workspace-admins-group membership
# but empirically found it irreducible in the current Terraform structure.
# See ADR-007 (docs/superpowers/adrs/ADR-007-workspace-admin-floor.md) for
# the evidence trail — plan-time TF graph pessimism cascades any SP update
# into forced replacement of downstream `databricks_group_member` resources,
# and workspace-SCIM reads of SPs are admin-gated so CI `terraform plan`
# cannot run without workspace admin.
#
# SEC4 DID close three transitive paths: Lakebase project (explicit CAN_MANAGE
# via databricks_permissions.lakebase_project_acl in environments/dev/main.tf),
# Lakebase synced-table pipelines (CAN_VIEW via scripts/grant_synced_table_permissions.py),
# and the orphan PG role be66af99-... cleanup. The SQL warehouse, ingestion
# job, and UC catalog paths were already explicit pre-SEC4 (PR #126, #128).
#
# Three floor privileges remain for the CI SP:
#
# 1. Workspace admin (admins-group membership, below):
#    Required by workspace-SCIM reads on `databricks_service_principal`
#    (Get() during plan refresh) and by downstream `databricks_group_member`
#    stability (any SP update cascades to member_id "known after apply"
#    forcing replacement, which breaks group ownership mid-apply). See
#    ADR-007 for empirical evidence (4 workspace-SCIM 403 responses from
#    the PR-#146 failed plan; subsequent `provider = databricks.account` /
#    `api = "account"` experiments both triggered the cascade).
#
# 2. Account admin (databricks_service_principal_role.terraform_ci_account_admin
#    below):
#    Required for the 11 account-scoped resources this module and the dev
#    root manage — service principals, groups, group members, federation
#    policy, access control rule sets, and workspace-permission assignment.
#    Every Databricks account-level SCIM / workspace-assignment API returns
#    HTTP 403 "This API is disabled for users without account admin status"
#    when called without account_admin — verified empirically during the
#    SEC4 spike (ReqIds 9345b222, 525df54e, 529f562d, 44b74a79 — see
#    ADR-006 §Notes). Narrower named roles do not exist in the Databricks
#    IAM model as of provider v1.113 / SDK v0.127; rule-set-based roles
#    (roles/group.manager, roles/servicePrincipal.manager, etc.) delegate
#    management of existing objects but cannot grant create authority on
#    new account-level parents. See ADR-006 for the full decision record.
#
# 3. Catalog ALL_PRIVILEGES (databricks_grant.ci_sp_catalog in
#    environments/dev/main.tf) + MANAGE via dbt-owners-{env} group
#    ownership (below, see "dbt-owners group" section).
#    Workspace admin does NOT confer Unity Catalog privileges; the
#    explicit grant + group ownership together give the CI SP read (plan)
#    and write (apply) access across the catalog. Verified 2026-04-16:
#    removing the grant broke terraform plan with "does not have
#    USE CATALOG" / "USE SCHEMA" on 10+ resources.

data "databricks_group" "admins" {
  display_name = "admins"
}

resource "databricks_group_member" "terraform_ci_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.terraform_ci.id
}

# Account admin — see item 2 above.
resource "databricks_service_principal_role" "terraform_ci_account_admin" {
  provider             = databricks.account
  service_principal_id = databricks_service_principal.terraform_ci.id
  role                 = "account_admin"
}

resource "databricks_service_principal_federation_policy" "github_actions" {
  provider = databricks.account

  service_principal_id = databricks_service_principal.terraform_ci.id
  policy_id            = "github-actions"

  # Match on the `repository` claim instead of `sub` so the policy works for
  # all GitHub trigger types (push, pull_request, etc.) without wildcards.
  # The `sub` claim varies per trigger (e.g. "repo:…:pull_request",
  # "repo:…:ref:refs/heads/main") and Databricks requires exact match.
  oidc_policy = {
    issuer        = "https://token.actions.githubusercontent.com"
    audiences     = [var.account_id, "${var.databricks_host}/oidc/v1/token"]
    subject       = var.github_repository
    subject_claim = "repository"
  }
}

# ── HF Spaces App (OAuth M2M) ───────────────────────────────────────────────
# Taipy app on HF Spaces authenticates to Databricks and Lakebase via OAuth
# M2M (no expiring PAT). Read-only: SELECT on gold + observability schemas.

resource "databricks_service_principal" "hf_app" {
  display_name = "luxury-lakehouse-hf-app-v2-${var.environment}"
  active       = true
}

# ── dbt-owners group: shared write access for dev_silver + dev_gold ─────────
# D59 (2026-04-13): dbt build needs to REPLACE existing tables/views in the
# dbt-managed dev_silver + dev_gold schemas. Unity Catalog requires the caller
# to be the object owner OR the schema owner. Without group ownership, the
# ingestion SP and developer users would fight over per-table ownership on
# every build (developer runs dbt locally → owns the table; SP runs daily-job
# dbt → can't replace the developer-owned table).
#
# Solution: dbt-owners group with both the deploying user and the ingestion
# SP as members. The dev_silver and dev_gold schemas (which are NOT Terraform-
# managed — they are created at runtime by dbt with the dbt-config-driven
# `{target.schema}_{model.+schema}` naming) are owned by this group via a
# one-time SQL ALTER SCHEMA. All members of the group can replace any object
# in the schema.
#
# A dbt `+post-hook` in `dbt_project.yml` transfers per-object ownership of
# every newly-built model back to the group, keeping ownership stable across
# runs and preventing per-object owner drift.
#
# See CLAUDE.md "## dbt Ownership Model" section for the operator runbook.

resource "databricks_group" "dbt_owners" {
  provider     = databricks.account
  display_name = "dbt-owners-${var.environment}"
}

data "databricks_user" "deployer" {
  provider  = databricks.account
  user_name = var.deployer_account_email
}

resource "databricks_group_member" "dbt_owners_deployer" {
  provider  = databricks.account
  group_id  = databricks_group.dbt_owners.id
  member_id = data.databricks_user.deployer.id
}

# The ingestion SP is a workspace-level resource; for account-level group
# membership it needs an account-level principal lookup.
data "databricks_service_principal" "ingestion_account" {
  provider       = databricks.account
  application_id = databricks_service_principal.ingestion.application_id
}

resource "databricks_group_member" "dbt_owners_ingestion_sp" {
  provider  = databricks.account
  group_id  = databricks_group.dbt_owners.id
  member_id = data.databricks_service_principal.ingestion_account.id
}

# The CI SP needs catalog ownership (via group) to issue GRANT/REVOKE on
# UC objects during terraform apply. ALL_PRIVILEGES does not confer MANAGE
# — only ownership does. Adding the CI SP to dbt-owners lets us transfer
# catalog ownership to the group, giving both the deployer and CI SP the
# ability to manage grants.
data "databricks_service_principal" "terraform_ci_account" {
  provider       = databricks.account
  application_id = databricks_service_principal.terraform_ci.application_id
}

resource "databricks_group_member" "dbt_owners_terraform_ci_sp" {
  provider  = databricks.account
  group_id  = databricks_group.dbt_owners.id
  member_id = data.databricks_service_principal.terraform_ci_account.id
}

# Assign the dbt-owners group to this workspace so UC ownership grants resolve.
# Account-level groups must be explicitly granted access to a workspace via
# `databricks_mws_permission_assignment` before they can be referenced in
# workspace SQL (ALTER ... OWNER TO ...).
resource "databricks_mws_permission_assignment" "dbt_owners_workspace" {
  provider     = databricks.account
  workspace_id = var.workspace_id
  principal_id = databricks_group.dbt_owners.id
  permissions  = ["USER"]
}

# ── lakehouse-operators group: human ad-hoc MODIFY access on bronze ─────────
# PR 1.5 (2026-04-21): Unity Catalog decouples schema OWNERSHIP from data
# MODIFY — the schema owner can GRANT MODIFY but does not automatically
# HOLD MODIFY. Operators who need to do ad-hoc bronze operations (e.g.
# `DELETE FROM bronze.* ` to force re-ingestion after a schema-change) hit
# `PERMISSION_DENIED: User does not have MODIFY on Table` until they
# self-grant. This group codifies the self-grant so the first operator
# to deploy doesn't have to rediscover the UC quirk, and adding a second
# operator becomes a one-line `databricks_group_member` change instead of
# a manual `GRANT MODIFY ON SCHEMA` step per operator.
#
# Scope is intentionally narrow: MODIFY + SELECT on BRONZE only. Silver
# and gold are dbt-managed — the `dbt-owners-{env}` group handles their
# ownership-chain semantics, and ad-hoc human DML against dbt-built marts
# is discouraged (it would drift from dbt's model contracts).
#
# Members: the deploying user (always) + any future operators via
# explicit `databricks_group_member` additions. The ingestion SP is NOT
# a member — it already has MODIFY via per-schema grants and doesn't
# need group-based inheritance.

resource "databricks_group" "lakehouse_operators" {
  provider     = databricks.account
  display_name = "lakehouse-operators-${var.environment}"
}

resource "databricks_group_member" "lakehouse_operators_deployer" {
  provider  = databricks.account
  group_id  = databricks_group.lakehouse_operators.id
  member_id = data.databricks_user.deployer.id
}

# Assign the group to the workspace so UC grants resolve.
resource "databricks_mws_permission_assignment" "lakehouse_operators_workspace" {
  provider     = databricks.account
  workspace_id = var.workspace_id
  principal_id = databricks_group.lakehouse_operators.id
  permissions  = ["USER"]
}
