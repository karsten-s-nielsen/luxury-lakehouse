# ──────────────────────────────────────────────────────────────────────────────
# Dev Environment — Composition Root
# ──────────────────────────────────────────────────────────────────────────────
# This is the entry point for `terraform apply` in the dev environment.
# It wires together all modules with dev-appropriate settings (small sizes,
# aggressive auto-stop, scale-to-zero) to stay within a ~$100/month budget.
# ──────────────────────────────────────────────────────────────────────────────

# ── Shared version constraints ───────────────────────────────────────────────
# Loaded automatically by Terraform from the shared directory (symlinked or
# included via -chdir).  The required_providers block lives in
# ../../../shared/versions.tf — we re-declare it here so the dev root is
# self-contained.

terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.110.0"
    }
  }
}

# ── Provider Configuration ───────────────────────────────────────────────────

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      project     = "luxury-lakehouse"
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# ── Module: Workspace ────────────────────────────────────────────────────────
# Creates the Unity Catalog and top-level governance objects.

module "workspace" {
  source = "../../modules/workspace"

  environment = var.environment
}

# ── Module: Service Principals ──────────────────────────────────────────────
# Dedicated service principals for automated workloads (least privilege).

module "service_principals" {
  source = "../../modules/service_principals"

  environment = var.environment
  account_id  = var.databricks_account_id
}

# ── Module: Catalog (Medallion Schemas) ──────────────────────────────────────
# Creates bronze / silver / gold schemas inside the soccer_analytics catalog.
# Also manages Unity Catalog grants for the ingestion and app service principals.

module "catalog" {
  source = "../../modules/catalog"

  catalog_name                = module.workspace.catalog_name
  environment                 = var.environment
  ingestion_sp_application_id = module.service_principals.ingestion_sp_application_id
  enable_ingestion_sp_grants  = true
  app_sp_application_id       = module.app.service_principal_client_id
  gold_schema_override        = "${var.environment}_gold"
}

# ── Module: Lakebase ─────────────────────────────────────────────────────────
# Lakebase Autoscaling (PG 17) with true scale-to-zero for dev.

module "lakebase" {
  source = "../../modules/lakebase"

  environment              = var.environment
  autoscaling_min_cu       = 0.5
  autoscaling_max_cu       = 4
  suspend_timeout_duration = "300s"
}

# ── Module: SQL Warehouse ────────────────────────────────────────────────────
# Serverless SQL warehouse with aggressive auto-stop for cost control.

module "sql_warehouse" {
  source = "../../modules/sql_warehouse"

  environment    = var.environment
  auto_stop_mins = 10
  cluster_size   = "2X-Small"
}

# ── Module: Workflows (Ingestion Jobs) ───────────────────────────────────────
# Daily ingestion pipeline: StatsBomb, Metrica, and Wyscout in parallel.

module "workflows" {
  source = "../../modules/workflows"

  catalog_name             = module.workspace.catalog_name
  wheel_path               = "${module.catalog.libs_volume_path}/luxury_lakehouse-0.1.0-py3-none-any.whl"
  environment              = var.environment
  notification_emails      = var.notification_emails
  run_as_sp_application_id = module.service_principals.ingestion_sp_application_id
}

# ── Module: Synced Tables ────────────────────────────────────────────────────
# Mirrors gold-layer Delta tables into Lakebase for low-latency app queries.

module "synced_tables" {
  source = "../../modules/synced_tables"

  catalog_name           = module.workspace.catalog_name
  database_instance_name = module.lakebase.instance_name
  environment            = var.environment
  gold_schema            = "${var.environment}_gold"
}

# ── Module: App (Streamlit Dashboard) ────────────────────────────────────────
# Deploys the soccer analytics Streamlit app on Databricks Apps.

module "app" {
  source = "../../modules/app"

  environment      = var.environment
  sql_warehouse_id = module.sql_warehouse.warehouse_id
}
