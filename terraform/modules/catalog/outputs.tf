# ──────────────────────────────────────────────────────────────────────────────
# Module: Catalog — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "bronze_schema_name" {
  description = "Fully qualified name of the bronze schema"
  value       = "${var.catalog_name}.${databricks_schema.bronze.name}"
}

output "silver_schema_name" {
  description = "Fully qualified name of the silver schema"
  value       = "${var.catalog_name}.${databricks_schema.silver.name}"
}

output "gold_schema_name" {
  description = "Fully qualified name of the gold schema"
  value       = "${var.catalog_name}.${databricks_schema.gold.name}"
}

output "schema_names" {
  description = "Map of layer name to fully qualified schema name"
  value = {
    bronze = "${var.catalog_name}.${databricks_schema.bronze.name}"
    silver = "${var.catalog_name}.${databricks_schema.silver.name}"
    gold   = "${var.catalog_name}.${databricks_schema.gold.name}"
  }
}

output "libs_volume_path" {
  description = "Volume path for wheel uploads (e.g. /Volumes/soccer_analytics/bronze/libs)"
  value       = "/Volumes/${var.catalog_name}/${databricks_schema.bronze.name}/${databricks_volume.libs.name}"
}
