# ──────────────────────────────────────────────────────────────────────────────
# Module: Lakebase — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "project_id" {
  description = "Lakebase Autoscaling project ID"
  value       = databricks_postgres_project.soccer_analytics.project_id
}

output "project_name" {
  description = "Full resource path of the Lakebase project (projects/{id})"
  value       = databricks_postgres_project.soccer_analytics.name
}

output "instance_name" {
  description = "Full project path — backward-compatible alias for synced_tables module"
  value       = databricks_postgres_project.soccer_analytics.name
}

output "endpoint_name" {
  description = "Full endpoint path for credential API (projects/{id}/branches/{branch}/endpoints/{endpoint})"
  value       = databricks_postgres_endpoint.primary.name
}

output "read_write_dns" {
  description = "PostgreSQL connection endpoint for read/write access"
  value       = databricks_postgres_endpoint.primary.status.hosts.host
}
