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
