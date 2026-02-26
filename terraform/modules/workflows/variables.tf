# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "catalog_name" {
  description = "Unity Catalog name for target schemas"
  type        = string
}

variable "warehouse_id" {
  description = "SQL warehouse ID for warehouse-backed tasks"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "wheel_path" {
  description = "Full path to the luxury_lakehouse wheel on a UC Volume (e.g. /Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.1.0-py3-none-any.whl)"
  type        = string
}
