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

variable "databricks_token" {
  description = "Databricks personal access token for authentication"
  type        = string
  sensitive   = true
}

variable "notification_emails" {
  description = "Email addresses for job failure/success notifications"
  type        = list(string)
  default     = []
}
