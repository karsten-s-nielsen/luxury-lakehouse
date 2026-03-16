# ──────────────────────────────────────────────────────────────────────────────
# Dev Environment — Outputs
# ──────────────────────────────────────────────────────────────────────────────
# Key resource identifiers and connection details surfaced after terraform apply.
# ──────────────────────────────────────────────────────────────────────────────

output "catalog_name" {
  description = "Unity Catalog name for all medallion schemas"
  value       = module.workspace.catalog_name
}

output "warehouse_id" {
  description = "SQL warehouse ID for dbt and ad-hoc queries"
  value       = module.sql_warehouse.warehouse_id
}

output "lakebase_project_id" {
  description = "Lakebase Autoscaling project ID"
  value       = module.lakebase.project_id
}

output "lakebase_endpoint_name" {
  description = "Lakebase endpoint path for credential API"
  value       = module.lakebase.endpoint_name
}

output "lakebase_read_write_dns" {
  description = "PostgreSQL read/write endpoint for the Streamlit app"
  value       = module.lakebase.read_write_dns
}

output "ingestion_job_id" {
  description = "Databricks job ID for the data ingestion pipeline"
  value       = module.workflows.ingestion_job_id
}

output "ingestion_sp_application_id" {
  description = "Application ID of the ingestion service principal"
  value       = module.service_principals.ingestion_sp_application_id
}

output "github_actions_role_arn" {
  description = "IAM role ARN for GitHub Actions OIDC authentication"
  value       = module.github_oidc.role_arn
}

output "state_kms_key_arn" {
  description = "KMS key ARN used for Terraform state encryption"
  value       = module.state_kms.kms_key_arn
}

output "terraform_ci_sp_application_id" {
  description = "Application ID of the Terraform CI service principal"
  value       = module.service_principals.terraform_ci_sp_application_id
}
