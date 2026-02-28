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

# ── Unity Catalog Grants: Ingestion Service Principal ────────────────────────
# Least-privilege access for the ingestion job: catalog traversal, schema
# read/write on bronze, and volume access for wheel storage.
#
# L-8: Schema-level MODIFY is intentional — the ingestion job creates tables
# dynamically (CREATE_TABLE) and writes to all 9 bronze tables. Per-table
# grants would require Terraform changes for every new source table and would
# prevent the job from creating tables on first run. Since the bronze schema
# is dedicated to raw ingestion data and only this SP writes to it, schema-
# level scope is the appropriate granularity.

resource "databricks_grant" "ingestion_sp_use_catalog" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  catalog = var.catalog_name

  principal  = var.ingestion_sp_application_id
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "ingestion_sp_bronze_schema" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  schema = "${var.catalog_name}.${databricks_schema.bronze.name}"

  principal  = var.ingestion_sp_application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY"]
}

resource "databricks_grant" "ingestion_sp_libs_volume" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  volume = "${var.catalog_name}.${databricks_schema.bronze.name}.${databricks_volume.libs.name}"

  principal  = var.ingestion_sp_application_id
  privileges = ["READ_VOLUME"]
}

# ── Unity Catalog Grants: App Service Principal ──────────────────────────────
# Read-only access for the Streamlit dashboard: catalog traversal and
# SELECT on the gold schema (which may be dbt-prefixed, e.g. dev_gold).

resource "databricks_grant" "app_sp_use_catalog" {
  count = var.app_sp_application_id != "" ? 1 : 0

  catalog = var.catalog_name

  principal  = var.app_sp_application_id
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "app_sp_gold_schema" {
  count = var.app_sp_application_id != "" ? 1 : 0

  schema = "${var.catalog_name}.${var.gold_schema_override != "" ? var.gold_schema_override : databricks_schema.gold.name}"

  principal  = var.app_sp_application_id
  privileges = ["USE_SCHEMA", "SELECT"]
}
