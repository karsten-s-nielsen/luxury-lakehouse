# ──────────────────────────────────────────────────────────────────────────────
# Module: SQL Warehouse — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "warehouse_id" {
  description = "ID of the serverless SQL warehouse (used by workflows and dbt)"
  value       = databricks_sql_endpoint.serverless.id
}

output "warehouse_name" {
  description = "Display name of the SQL warehouse"
  value       = databricks_sql_endpoint.serverless.name
}

output "warehouse_data_source_id" {
  description = "Data source ID for JDBC/ODBC connections"
  value       = databricks_sql_endpoint.serverless.data_source_id
}
