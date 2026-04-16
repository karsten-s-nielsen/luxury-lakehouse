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

# ── CI SP Roles: Workspace Admin + Account Admin ──────────────────────────
# These two roles are the verified minimum floor for terraform plan/apply.
# Investigated 2026-04-13 (SEC4 / D59) — each was individually tested for
# removal; both are mandatory for the resources they gate:
#
# 1. Workspace admin (admins group membership):
#    Required by databricks_permissions on workspace-scoped objects —
#    specifically sql_warehouse (environments/dev/main.tf) and job ACLs
#    (hf_app_view_ingestion_job, hf_app_view_sync_hf_costs_job). Workspace
#    admin is the only role granting MANAGE on workspace-level objects.
#    Cannot be replaced without adding explicit databricks_permissions
#    resources for every workspace object across all TF modules.
#
# 2. Account admin (databricks_service_principal_role):
#    Required for two account-scoped resources that cannot be managed
#    without account-level API access:
#    - databricks_service_principal_federation_policy.github_actions (below)
#    - databricks_access_control_rule_set.ingestion_sp_user_role (above)
#
# 3. Catalog ALL_PRIVILEGES (databricks_grant in environments/dev/main.tf):
#    Workspace admin does NOT cover Unity Catalog privileges. The CI SP
#    manages schemas, volumes, and grants via Terraform — it needs both
#    read (plan) and write (apply) UC access across the catalog.
#    Verified 2026-04-16: removing the grant broke terraform plan with
#    "does not have USE CATALOG" / "USE SCHEMA" on 10+ resources.

data "databricks_group" "admins" {
  display_name = "admins"
}

resource "databricks_group_member" "terraform_ci_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.terraform_ci.id
}

# Account admin — needed to read account-level resources (federation policies,
# access control rule sets) during terraform plan.
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
