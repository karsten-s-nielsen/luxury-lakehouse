# ──────────────────────────────────────────────────────────────────────────────
# Module: Service Principals — Least-Privilege Identities
# ──────────────────────────────────────────────────────────────────────────────
# Creates dedicated service principals for automated workloads so that
# jobs and apps run with scoped permissions instead of a PAT owner's
# full workspace privileges.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_service_principal" "ingestion" {
  display_name = "luxury-lakehouse-ingestion-${var.environment}"
  active       = true
}

# ── Grant deploying user the servicePrincipal.user role ──────────────────────
# Required so that the PAT owner can set `run_as` on jobs to this SP.

data "databricks_current_user" "me" {}

resource "databricks_access_control_rule_set" "ingestion_sp_user_role" {
  name = "accounts/${var.account_id}/servicePrincipals/${databricks_service_principal.ingestion.application_id}/ruleSets/default"

  grant_rules {
    principals = ["users/${data.databricks_current_user.me.user_name}"]
    role       = "roles/servicePrincipal.user"
  }
}
