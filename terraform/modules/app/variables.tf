# ──────────────────────────────────────────────────────────────────────────────
# Module: App — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "sql_warehouse_id" {
  description = "SQL warehouse ID for the app to use"
  type        = string
  default     = ""
}
