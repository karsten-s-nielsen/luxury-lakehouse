# ──────────────────────────────────────────────────────────────────────────────
# Module: Service Principals — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "account_id" {
  description = "Databricks account ID (UUID from accounts.cloud.databricks.com)"
  type        = string
}

variable "deployer_user_names" {
  description = "List of user principals to grant servicePrincipal.user role (L-9: configurable, not hardcoded to current user)"
  type        = list(string)
  default     = []
}

variable "deployer_account_email" {
  description = <<-EOT
    Account-level email of the human deployer, used as the dbt-owners group's
    user member and as the fallback principal for the ingestion SP user role.
    MUST be set to a real account-level email — looking up via
    data.databricks_current_user.me.user_name fails in CI because the TF CI SP's
    user_name is its application_id (UUID), not an email, and account-level
    databricks_user lookup by SP app_id returns 404.
  EOT
  type        = string
  default     = "karstenskyt@gmail.com"
}

variable "github_repository" {
  description = "GitHub repository in org/repo format for OIDC federation"
  type        = string
  default     = "karsten-s-nielsen/luxury-lakehouse"
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://<workspace>.cloud.databricks.com) — used as OIDC audience for workspace-level federation"
  type        = string
}

variable "workspace_id" {
  description = "Databricks workspace numeric ID — required for account-level group → workspace permission assignment (D59 dbt-owners group)"
  type        = string
}
