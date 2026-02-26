# ──────────────────────────────────────────────────────────────────────────────
# Standard Tags
# ──────────────────────────────────────────────────────────────────────────────
# Every AWS and Databricks resource should carry these tags for cost
# attribution, ownership tracking, and lifecycle management.
#
# Usage in modules:
#   tags = local.standard_tags
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

locals {
  standard_tags = {
    project     = "luxury-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
  }
}
