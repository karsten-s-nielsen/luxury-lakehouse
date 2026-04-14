# ──────────────────────────────────────────────────────────────────────────────
# Dev Environment Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://<workspace>.cloud.databricks.com)"
  type        = string
}

variable "databricks_workspace_id" {
  description = "Databricks workspace numeric ID — find via account console or `databricks workspaces list`. Required for D59 dbt-owners account-level group → workspace assignment."
  type        = string
  default     = "7474660814094441"
}

variable "databricks_account_id" {
  description = "Databricks account ID (UUID from accounts.cloud.databricks.com)"
  type        = string
}

variable "databricks_client_id" {
  description = "Databricks service principal client ID (OAuth M2M). Empty during bootstrap — falls back to DATABRICKS_TOKEN env var."
  type        = string
  default     = ""
}

variable "databricks_client_secret" {
  description = "Databricks SP client secret (empty string in CI — OIDC provides auth)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "notification_emails" {
  description = "Email addresses for job failure/success notifications"
  type        = list(string)
  default     = []
}

variable "alert_email" {
  description = "Email address for AWS budget alarm notifications"
  type        = string
  default     = ""
}
