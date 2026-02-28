# ──────────────────────────────────────────────────────────────────────────────
# Module: GitHub OIDC — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "role_arn" {
  description = "ARN of the IAM role for GitHub Actions to assume via OIDC"
  value       = aws_iam_role.github_actions.arn
}

output "oidc_provider_arn" {
  description = "ARN of the GitHub OIDC identity provider in AWS IAM"
  value       = aws_iam_openid_connect_provider.github.arn
}
