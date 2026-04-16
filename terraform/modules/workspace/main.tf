# ──────────────────────────────────────────────────────────────────────────────
# Module: Workspace — Unity Catalog Setup
# ──────────────────────────────────────────────────────────────────────────────
# Creates the top-level Unity Catalog for the soccer analytics platform.
#
# Unity Catalog provides centralized governance across all Databricks
# workspaces, enabling:
#   - Fine-grained access control (row/column-level)
#   - Data lineage tracking across the entire medallion architecture
#   - Audit logging for compliance
#   - Cross-workspace data sharing
#
# Hierarchy: Catalog > Schema > Table/View/Volume
# This module creates the catalog; schemas are created in the catalog module.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_catalog" "soccer_analytics" {
  name    = "soccer_analytics"
  comment = "Unity Catalog for the Luxury Lakehouse soccer analytics platform. Contains bronze/silver/gold medallion schemas."

  properties = {
    project     = "luxury-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
  }

  enable_predictive_optimization = "ENABLE"

  # Default Storage catalogs are created via SQL (CREATE CATALOG) and imported
  # into state. The storage_root is Databricks-managed and must not trigger
  # replacement — the Databricks API cannot recreate Default Storage catalogs.
  #
  # Owner is managed outside Terraform: transferred to dbt-owners-{env} group
  # via one-time ALTER CATALOG so both the deployer and CI SP can issue
  # GRANT/REVOKE (MANAGE privilege requires ownership, not ALL_PRIVILEGES).
  lifecycle {
    ignore_changes = [storage_root, owner]
  }
}
