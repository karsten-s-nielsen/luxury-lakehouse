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
