# ──────────────────────────────────────────────────────────────────────────────
# Remote State Backend (S3 Native Locking)
# ──────────────────────────────────────────────────────────────────────────────
# Prerequisites:
#   1. S3 bucket with versioning enabled
#   2. Terraform >= 1.10 (native S3 locking via conditional writes)
# ──────────────────────────────────────────────────────────────────────────────

terraform {
  backend "s3" {
    bucket       = "karstenskyt-terraform-state"
    key          = "luxury-lakehouse/terraform.tfstate"
    region       = "us-east-1"
    profile      = "devops-agent"
    encrypt      = true
    use_lockfile = true
  }
}
