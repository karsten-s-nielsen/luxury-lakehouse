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
