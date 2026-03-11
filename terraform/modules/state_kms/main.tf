# ──────────────────────────────────────────────────────────────────────────────
# Module: State KMS — Customer Managed Key for Terraform State Encryption
# ──────────────────────────────────────────────────────────────────────────────
# Replaces AWS-managed SSE-S3 encryption with a KMS CMK and S3 Bucket Key
# for full key lifecycle control (L-10).  Bucket Key reduces KMS API costs
# by ~99 % for SSE-KMS workloads.
# ──────────────────────────────────────────────────────────────────────────────

resource "aws_kms_key" "terraform_state" {
  description             = "Encrypts luxury-lakehouse Terraform state in S3"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = { Name = "luxury-lakehouse-terraform-state-${var.environment}" }
}

resource "aws_kms_alias" "terraform_state" {
  name          = "alias/luxury-lakehouse-terraform-state-${var.environment}"
  target_key_id = aws_kms_key.terraform_state.key_id
}

# Lifecycle rule: expire non-current state versions after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "state" {
  bucket = var.state_bucket

  rule {
    id     = "expire-noncurrent-state-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# Enable S3 Bucket Key to reduce KMS API costs (~99 % fewer KMS calls)
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = var.state_bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.terraform_state.arn
    }
    bucket_key_enabled = true
  }
}
