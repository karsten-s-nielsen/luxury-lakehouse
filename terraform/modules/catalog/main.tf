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

resource "databricks_schema" "observability" {
  catalog_name = var.catalog_name
  name         = "observability"
  comment      = "Platform operational metadata: cost tracking, run history, SLIs, alerts."

  properties = {
    layer       = "observability"
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
#
# SELECT is required because Spark's saveAsTable(mode="overwrite") reads
# existing table metadata before writing (loadTable → getTableMetadata).

resource "databricks_grant" "ingestion_sp_use_catalog" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  catalog = var.catalog_name

  principal  = var.ingestion_sp_application_id
  privileges = ["USE_CATALOG", "BROWSE"]
}

resource "databricks_grant" "ingestion_sp_bronze_schema" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  schema = "${var.catalog_name}.${databricks_schema.bronze.name}"

  principal  = var.ingestion_sp_application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
}

resource "databricks_grant" "ingestion_sp_silver_schema" {
  count = var.enable_ingestion_sp_grants && var.silver_schema_override != "" ? 1 : 0

  schema = "${var.catalog_name}.${var.silver_schema_override}"

  principal = var.ingestion_sp_application_id
  # D59 (2026-04-13): expanded from USE_SCHEMA + SELECT to also include
  # CREATE_TABLE and MODIFY so the daily-job dbt_build task can materialize
  # the staging-layer models (views) and seeds in dev_silver.
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
}

resource "databricks_grant" "ingestion_sp_gold_schema" {
  count = var.enable_ingestion_sp_grants && var.gold_schema_override != "" ? 1 : 0

  schema = "${var.catalog_name}.${var.gold_schema_override}"

  principal = var.ingestion_sp_application_id
  # D59 (2026-04-13): expanded from USE_SCHEMA + SELECT to also include
  # CREATE_TABLE and MODIFY so the daily-job dbt_build task can materialize
  # the 36 gold mart tables. Replaces the previous developer-machine-only
  # dbt build flow. See spec § Item 1 D59 component 5.
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
}

resource "databricks_grant" "ingestion_sp_gold_model_weights_volume" {
  count = var.enable_ingestion_sp_grants && var.gold_schema_override != "" ? 1 : 0

  volume = "${var.catalog_name}.${var.gold_schema_override}.model_weights"

  principal  = var.ingestion_sp_application_id
  privileges = ["READ_VOLUME", "WRITE_VOLUME"]
}

# ── Volume: Training data exports for HF Jobs ──────────────────────────────
# Stores SPADL action sequences (Parquet) for Football2Vec v2 training.
# Written by export_embeddings_training_data pipeline, read by HF Jobs scripts.

resource "databricks_volume" "training_data" {
  count = var.gold_schema_override != "" ? 1 : 0

  catalog_name = var.catalog_name
  schema_name  = var.gold_schema_override
  name         = "training_data"
  volume_type  = "MANAGED"
  comment      = "Training data exports (SPADL sequences for Football2Vec v2)"
}

resource "databricks_grant" "ingestion_sp_gold_training_data_volume" {
  count = var.enable_ingestion_sp_grants && var.gold_schema_override != "" ? 1 : 0

  volume = "${var.catalog_name}.${var.gold_schema_override}.training_data"

  principal  = var.ingestion_sp_application_id
  privileges = ["READ_VOLUME", "WRITE_VOLUME"]

  depends_on = [databricks_volume.training_data]
}

resource "databricks_grant" "ingestion_sp_observability_schema" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  schema = "${var.catalog_name}.${databricks_schema.observability.name}"

  principal  = var.ingestion_sp_application_id
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
}

# ── dbt-owners group: catalog + schema read access (D59) ─────────────────
# The dbt-owners-{env} group owns the dev_silver + dev_gold schemas and
# all dbt-managed objects within them (via post-hook ownership transfer).
# UC's chain-of-ownership authorization for views requires the view owner
# to ALSO have read access to upstream sources. Without these grants, dbt
# tests fail with "Insufficient privileges: User does not have USE CATALOG
# on Catalog 'soccer_analytics'" because the test queries traverse from
# the staging view (group-owned) to bronze sources (different ownership).

resource "databricks_grant" "dbt_owners_use_catalog" {
  count = var.dbt_owners_group_name != "" ? 1 : 0

  catalog    = var.catalog_name
  principal  = var.dbt_owners_group_name
  privileges = ["USE_CATALOG", "BROWSE"]
}

resource "databricks_grant" "dbt_owners_bronze_select" {
  count = var.dbt_owners_group_name != "" ? 1 : 0

  schema     = "${var.catalog_name}.${databricks_schema.bronze.name}"
  principal  = var.dbt_owners_group_name
  privileges = ["USE_SCHEMA", "SELECT"]
}

# ── Unity Catalog Grants: lakehouse-operators group (PR 1.5) ─────────────────
# Human operators need MODIFY + SELECT on bronze for ad-hoc operations:
#
#   - DELETE FROM bronze.* to force re-ingestion after a parser schema change.
#     Example: PR 1.5 (2026-04-21) rewrote the IDSSE parser to emit ~200
#     bronze cols (was 9); the existing rows had to be DELETEd before
#     re-ingestion could populate the new schema. Without MODIFY, the
#     operator gets "PERMISSION_DENIED: User does not have MODIFY on Table".
#   - Manual data repair (bad row from a source that doesn't re-serve it).
#   - Emergency backfill overrides.
#
# UC does NOT auto-grant MODIFY to the schema owner — ownership gives GRANT
# authority but not MODIFY itself. This grant codifies what would otherwise
# be a manual `GRANT MODIFY ON SCHEMA bronze TO <user>` step after every
# fresh deploy.
#
# Scope is bronze only. Silver + gold are dbt-managed; ad-hoc human DML
# against dbt-built marts is discouraged (drifts from dbt model contracts).

resource "databricks_grant" "lakehouse_operators_bronze_modify" {
  count = var.lakehouse_operators_group_name != "" ? 1 : 0

  schema     = "${var.catalog_name}.${databricks_schema.bronze.name}"
  principal  = var.lakehouse_operators_group_name
  privileges = ["USE_SCHEMA", "SELECT", "MODIFY"]
}

resource "databricks_grant" "dbt_owners_silver_select" {
  count = var.dbt_owners_group_name != "" && var.silver_schema_override != "" ? 1 : 0

  schema     = "${var.catalog_name}.${var.silver_schema_override}"
  principal  = var.dbt_owners_group_name
  privileges = ["USE_SCHEMA", "SELECT"]
}

resource "databricks_grant" "dbt_owners_gold_select" {
  count = var.dbt_owners_group_name != "" && var.gold_schema_override != "" ? 1 : 0

  schema     = "${var.catalog_name}.${var.gold_schema_override}"
  principal  = var.dbt_owners_group_name
  privileges = ["USE_SCHEMA", "SELECT"]
}

resource "databricks_grant" "ingestion_sp_libs_volume" {
  count = var.enable_ingestion_sp_grants ? 1 : 0

  volume = "${var.catalog_name}.${databricks_schema.bronze.name}.${databricks_volume.libs.name}"

  principal  = var.ingestion_sp_application_id
  privileges = ["READ_VOLUME"]
}

# ── Unity Catalog Grants: MLflow model access ────────────────────────────────
# Pipeline tasks load pre-trained models from the UC model registry via
# mlflow.pyfunc.load_model("models:/<model>@Champion"). EXECUTE is required.
# Models that don't exist yet are guarded by try/except in the pipeline code;
# the grant is pre-provisioned so they work once registered.

resource "databricks_grant" "ingestion_sp_vaep_model" {
  count = var.enable_ingestion_sp_grants && var.gold_schema_override != "" ? 1 : 0

  model = "${var.catalog_name}.${var.gold_schema_override}.vaep_model"

  principal  = var.ingestion_sp_application_id
  privileges = ["EXECUTE"]
}

# TODO: Add grants for xg_model and defcon_model once registered in UC.
# Both pipelines (xg_model.py, defcon_lite.py) fall back to inline training
# when the @Champion model is not found, so these are not blocking.
# resource "databricks_grant" "ingestion_sp_xg_model" { model = "...xg_model" }
# resource "databricks_grant" "ingestion_sp_defcon_model" { model = "...defcon_model" }

# ── Unity Catalog Grants: Account Users (dbt / developer access) ─────────────
# dbt builds staging views that SELECT from bronze source tables. Without
# this grant, any user running `dbt build` locally gets INSUFFICIENT_PERMISSIONS
# on bronze tables created by the ingestion SP. Read-only — no writes.

resource "databricks_grant" "account_users_bronze_schema" {
  schema = "${var.catalog_name}.${databricks_schema.bronze.name}"

  principal  = "account users"
  privileges = ["USE_SCHEMA", "SELECT"]
}

resource "databricks_grant" "account_users_observability_schema" {
  schema = "${var.catalog_name}.${databricks_schema.observability.name}"

  principal  = "account users"
  privileges = ["USE_SCHEMA", "SELECT"]
}

# ── Unity Catalog Grants: App Service Principal ──────────────────────────────
# Read-only access for the Streamlit dashboard: catalog traversal and
# SELECT on the gold schema (which may be dbt-prefixed, e.g. dev_gold).

resource "databricks_grant" "app_sp_use_catalog" {
  count = var.enable_app_sp_grants ? 1 : 0

  catalog = var.catalog_name

  principal  = var.app_sp_application_id
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "app_sp_gold_schema" {
  count = var.enable_app_sp_grants ? 1 : 0

  schema = "${var.catalog_name}.${var.gold_schema_override != "" ? var.gold_schema_override : databricks_schema.gold.name}"

  principal  = var.app_sp_application_id
  privileges = ["USE_SCHEMA", "SELECT"]
}

resource "databricks_grant" "app_sp_observability_schema" {
  count = var.enable_app_sp_grants ? 1 : 0

  schema = "${var.catalog_name}.${databricks_schema.observability.name}"

  principal  = var.app_sp_application_id
  privileges = ["USE_SCHEMA", "SELECT"]
}
