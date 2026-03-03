# ──────────────────────────────────────────────────────────────────────────────
# Module: GitHub OIDC — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "github_repository" {
  description = "GitHub repository in org/repo format for OIDC trust policy"
  type        = string
  default     = "karsten-s-nielsen/luxury-lakehouse"
}

variable "state_bucket" {
  description = "Name of the S3 bucket holding Terraform remote state"
  type        = string
  default     = "karstenskyt-terraform-state"
}

variable "kms_key_arn" {
  description = "ARN of the KMS key used for state encryption (for IAM policy)"
  type        = string
}
