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
