# ──────────────────────────────────────────────────────────────────────────────
# Remote State Backend (S3 Native Locking)
# ──────────────────────────────────────────────────────────────────────────────
# Prerequisites:
#   1. S3 bucket with versioning enabled
#   2. Terraform >= 1.10 (native S3 locking via conditional writes)
#
# Auth: use AWS_PROFILE env var locally (e.g. AWS_PROFILE=devops-agent).
# In CI, the aws-actions/configure-aws-credentials step provides credentials.
#
# KMS: After the first apply, replace the kms_key_id placeholder below with
# the ARN from `terraform output state_kms_key_arn`, then run
# `terraform init -reconfigure` to migrate state encryption to the CMK.
# ──────────────────────────────────────────────────────────────────────────────

terraform {
  backend "s3" {
    bucket       = "karstenskyt-terraform-state"
    key          = "luxury-lakehouse/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
    kms_key_id   = "arn:aws:kms:us-east-1:454762693631:key/e7c711c7-ed8c-43d8-9242-856e13564baa"
  }
}
