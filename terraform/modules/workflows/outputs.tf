# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "ingestion_job_id" {
  description = "ID of the data ingestion job"
  value       = databricks_job.data_ingestion.id
}

output "ingestion_job_url" {
  description = "URL to the job in the Databricks workspace"
  value       = databricks_job.data_ingestion.url
}
