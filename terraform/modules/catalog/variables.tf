# ──────────────────────────────────────────────────────────────────────────────
# Module: Catalog — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "catalog_name" {
  description = "Name of the Unity Catalog to create schemas in"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "ingestion_sp_application_id" {
  description = "Application ID of the ingestion service principal (empty = skip grants)"
  type        = string
  default     = ""
}

variable "enable_ingestion_sp_grants" {
  description = "Whether to create grants for the ingestion service principal (avoids unknown count at plan time)"
  type        = bool
  default     = false
}

variable "app_sp_application_id" {
  description = "Application ID of the HF Spaces app service principal (empty = skip grants)"
  type        = string
  default     = ""
}

variable "silver_schema_override" {
  description = "Override for the silver schema name (e.g. dev_silver when dbt prefixes with environment)"
  type        = string
  default     = ""
}

variable "gold_schema_override" {
  description = "Override for the gold schema name (e.g. dev_gold when dbt prefixes with environment)"
  type        = string
  default     = ""
}
