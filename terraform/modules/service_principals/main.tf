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
# to the current Terraform user when no explicit list is provided.

data "databricks_current_user" "me" {}

locals {
  deployer_principals = length(var.deployer_user_names) > 0 ? [
    for u in var.deployer_user_names : "users/${u}"
  ] : ["users/${data.databricks_current_user.me.user_name}"]
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

# ── Workspace Admin for CI SP ──────────────────────────────────────────────
# terraform plan needs to refresh all managed resources (service principals
# via SCIM, catalogs, SQL endpoints, etc.).  Workspace admin is the minimum
# role that grants read access to everything terraform manages.

data "databricks_group" "admins" {
  display_name = "admins"
}

resource "databricks_group_member" "terraform_ci_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.terraform_ci.id
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
