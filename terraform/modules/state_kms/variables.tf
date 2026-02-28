# ──────────────────────────────────────────────────────────────────────────────
# Module: State KMS — Input Variables
# ──────────────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "state_bucket" {
  description = "Name of the S3 bucket holding Terraform remote state"
  type        = string
  default     = "karstenskyt-terraform-state"
}
