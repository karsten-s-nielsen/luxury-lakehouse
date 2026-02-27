# ──────────────────────────────────────────────────────────────────────────────
# Module: Service Principals — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "ingestion_sp_application_id" {
  description = "Application (client) ID of the ingestion service principal"
  value       = databricks_service_principal.ingestion.application_id
}
