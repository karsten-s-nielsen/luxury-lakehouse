# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "catalog_name" {
  description = "Unity Catalog name for target schemas"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "wheel_path" {
  description = "Full path to the luxury_lakehouse wheel on a UC Volume (e.g. /Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.3.43-py3-none-any.whl). Do NOT append a #sha256= fragment — serverless pip rejects it on UC Volume paths."
  type        = string
}

variable "notification_emails" {
  description = "Email addresses to notify on job start, success, and failure"
  type        = list(string)
  default     = []
}

variable "run_as_sp_application_id" {
  description = "Service principal application ID to run the ingestion job as (empty = run as job owner)"
  type        = string
  default     = ""
}
