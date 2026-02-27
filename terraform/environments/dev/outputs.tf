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

output "lakebase_instance_name" {
  description = "Lakebase database instance name"
  value       = module.lakebase.instance_name
}

output "lakebase_read_write_dns" {
  description = "PostgreSQL read/write endpoint for the Streamlit app"
  value       = module.lakebase.read_write_dns
}

output "ingestion_job_id" {
  description = "Databricks job ID for the data ingestion pipeline"
  value       = module.workflows.ingestion_job_id
}

output "app_name" {
  description = "Deployed Streamlit app name"
  value       = module.app.app_name
}

output "app_url" {
  description = "URL of the deployed Streamlit dashboard"
  value       = module.app.app_url
}

output "ingestion_sp_application_id" {
  description = "Application ID of the ingestion service principal"
  value       = module.service_principals.ingestion_sp_application_id
}

output "app_sp_application_id" {
  description = "Application ID of the app's auto-provisioned service principal"
  value       = module.app.service_principal_client_id
}
