# ──────────────────────────────────────────────────────────────────────────────
# Module: Service Principals — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "ingestion_sp_application_id" {
  description = "Application (client) ID of the ingestion service principal"
  value       = databricks_service_principal.ingestion.application_id
}

output "terraform_ci_sp_application_id" {
  description = "Application (client) ID of the Terraform CI service principal"
  value       = databricks_service_principal.terraform_ci.application_id
}

output "hf_app_sp_application_id" {
  description = "Application (client) ID of the HF Spaces app service principal"
  value       = databricks_service_principal.hf_app.application_id
}

output "dbt_owners_group_display_name" {
  description = "Display name of the dbt-owners group (D59 — shared dev_silver/dev_gold ownership)"
  value       = databricks_group.dbt_owners.display_name
}
