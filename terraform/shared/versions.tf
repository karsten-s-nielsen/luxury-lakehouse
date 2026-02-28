# ──────────────────────────────────────────────────────────────────────────────
# Provider & Terraform Version Constraints
# ──────────────────────────────────────────────────────────────────────────────
# Pinned floor versions ensure reproducible plans across developer machines
# and CI runners.  Bump these intentionally, never with "latest".
# ──────────────────────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }

    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.110.0"
    }
  }
}
