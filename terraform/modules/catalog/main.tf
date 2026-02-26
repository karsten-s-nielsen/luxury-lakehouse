# ──────────────────────────────────────────────────────────────────────────────
# Module: Catalog — Medallion Architecture Schemas
# ──────────────────────────────────────────────────────────────────────────────
# Creates the three-layer medallion schema structure inside the
# soccer_analytics Unity Catalog:
#
#   bronze  — Raw ingested data, append-only.  Source-of-truth for replay.
#             Data arrives as-is from StatsBomb, Metrica, and Wyscout APIs.
#
#   silver  — Cleaned, typed, and deduplicated data.  Conformed schemas,
#             null handling, and incremental merge logic applied.
#
#   gold    — Analytics-ready facts and dimensions.  Star-schema tables
#             optimized for BI dashboards and the Streamlit app.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_schema" "bronze" {
  catalog_name = var.catalog_name
  name         = "bronze"
  comment      = "Raw ingested data — append-only, no transformations. Source-of-truth for data replay and auditing."

  properties = {
    layer       = "bronze"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "databricks_schema" "silver" {
  catalog_name = var.catalog_name
  name         = "silver"
  comment      = "Cleaned, typed, and deduplicated data. Conformed schemas with null handling and incremental merge logic."

  properties = {
    layer       = "silver"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "databricks_schema" "gold" {
  catalog_name = var.catalog_name
  name         = "gold"
  comment      = "Analytics-ready facts and dimensions. Star-schema tables optimized for dashboards and the Streamlit app."

  properties = {
    layer       = "gold"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ── Volume: Wheel storage for ingestion jobs ─────────────────────────────────
# Stores Python wheel packages uploaded by CI/CD or deployment scripts.
# Referenced by serverless job environments via /Volumes/<catalog>/bronze/libs/

resource "databricks_volume" "libs" {
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.bronze.name
  name         = "libs"
  volume_type  = "MANAGED"
  comment      = "Python wheel packages for serverless job environments"
}
