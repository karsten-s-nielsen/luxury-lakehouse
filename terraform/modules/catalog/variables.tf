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
