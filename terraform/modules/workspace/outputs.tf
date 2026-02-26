# ──────────────────────────────────────────────────────────────────────────────
# Module: Workspace — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "catalog_name" {
  description = "Name of the Unity Catalog created for soccer analytics"
  value       = databricks_catalog.soccer_analytics.name
}

output "catalog_id" {
  description = "Unique identifier of the Unity Catalog"
  value       = databricks_catalog.soccer_analytics.id
}
