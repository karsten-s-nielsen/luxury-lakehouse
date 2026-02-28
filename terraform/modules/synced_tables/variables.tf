# ──────────────────────────────────────────────────────────────────────────────
# Module: Synced Tables — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "catalog_name" {
  description = "Unity Catalog name containing the gold-layer source tables"
  type        = string
}

variable "database_instance_name" {
  description = "Name of the Lakebase instance or Autoscaling project ID to sync into"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "gold_schema" {
  description = "Name of the gold-layer schema (dbt prepends target name, e.g. dev_gold)"
  type        = string
  default     = "gold"
}
