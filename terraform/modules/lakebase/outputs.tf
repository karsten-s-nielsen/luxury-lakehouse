# ──────────────────────────────────────────────────────────────────────────────
# Module: Lakebase — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "instance_name" {
  description = "Name of the Lakebase database instance"
  value       = databricks_database_instance.soccer_analytics.name
}

output "instance_uid" {
  description = "Unique identifier of the Lakebase database instance"
  value       = databricks_database_instance.soccer_analytics.uid
}

output "read_write_dns" {
  description = "PostgreSQL connection endpoint for read/write access"
  value       = databricks_database_instance.soccer_analytics.read_write_dns
}

output "read_only_dns" {
  description = "PostgreSQL connection endpoint for read-only access"
  value       = databricks_database_instance.soccer_analytics.read_only_dns
}
