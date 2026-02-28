# ──────────────────────────────────────────────────────────────────────────────
# Module: State KMS — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "kms_key_arn" {
  description = "ARN of the KMS key used for Terraform state encryption"
  value       = aws_kms_key.terraform_state.arn
}

output "kms_key_alias" {
  description = "Alias of the KMS key (for human-friendly reference)"
  value       = aws_kms_alias.terraform_state.name
}
