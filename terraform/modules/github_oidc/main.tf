# ──────────────────────────────────────────────────────────────────────────────
# Module: GitHub OIDC — Secretless CI Authentication
# ──────────────────────────────────────────────────────────────────────────────
# Creates an AWS IAM OIDC provider for GitHub Actions and a scoped IAM role
# so CI can authenticate via short-lived OIDC tokens instead of long-lived
# access keys.  The trust policy is scoped to a specific repository so it
# remains safe even when the repo goes public.
# ──────────────────────────────────────────────────────────────────────────────

# ── GitHub OIDC Provider (idempotent — one per AWS account) ──────────────────

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # GitHub manages TLS certificates for token.actions.githubusercontent.com.
  # AWS ignores this thumbprint for GitHub OIDC (verified via GitHub's JWKS
  # directly), so the all-f placeholder is the documented convention.
  # See: https://github.com/aws-actions/configure-aws-credentials#OIDC
  thumbprint_list = ["ffffffffffffffffffffffffffffffffffffffff"]
}

# ── IAM Role for GitHub Actions ──────────────────────────────────────────────

resource "aws_iam_role" "github_actions" {
  name = "luxury-lakehouse-github-actions-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repository}:*"
        }
      }
    }]
  })
}

# ── Permissions: S3 state + KMS + IAM/S3 read for plan ───────────────────────
# S3 PutObject/DeleteObject: Terraform native S3 locking (.tflock files).
# KMS GenerateDataKey: state bucket uses KMS-SSE, lock writes need encryption.
# IAM/KMS/S3 read: terraform plan reads managed resources (OIDC provider, role,
# KMS key metadata, S3 bucket encryption config).

resource "aws_iam_role_policy" "terraform_state_access" {
  name = "terraform-state-access"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3StateAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
          "s3:GetBucketVersioning", "s3:GetEncryptionConfiguration",
          "s3:GetLifecycleConfiguration"
        ]
        Resource = [
          "arn:aws:s3:::${var.state_bucket}",
          "arn:aws:s3:::${var.state_bucket}/*"
        ]
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = [
          "kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey",
          "kms:GetKeyRotationStatus", "kms:GetKeyPolicy",
          "kms:ListResourceTags"
        ]
        Resource = [var.kms_key_arn]
      },
      {
        Sid      = "KMSAliasList"
        Effect   = "Allow"
        Action   = ["kms:ListAliases"]
        Resource = ["*"]
      },
      {
        Sid      = "BudgetRead"
        Effect   = "Allow"
        Action   = ["budgets:ViewBudget", "budgets:ListTagsForResource"]
        Resource = ["arn:aws:budgets::*:budget/luxury-lakehouse-*"]
      },
      {
        Sid    = "IAMReadOIDC"
        Effect = "Allow"
        Action = [
          "iam:GetOpenIDConnectProvider",
          "iam:GetRole",
          "iam:GetRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies"
        ]
        Resource = [
          aws_iam_openid_connect_provider.github.arn,
          aws_iam_role.github_actions.arn
        ]
      }
    ]
  })
}
