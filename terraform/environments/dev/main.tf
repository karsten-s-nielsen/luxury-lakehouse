# ──────────────────────────────────────────────────────────────────────────────
# Dev Environment — Composition Root
# ──────────────────────────────────────────────────────────────────────────────
# This is the entry point for `terraform apply` in the dev environment.
# It wires together all modules with dev-appropriate settings (small sizes,
# aggressive auto-stop, scale-to-zero) to stay within a ~$250/month budget.
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
  host          = var.databricks_host
  client_id     = var.databricks_client_id != "" ? var.databricks_client_id : null
  client_secret = var.databricks_client_secret != "" ? var.databricks_client_secret : null
}

# Account-level provider for resources like federation policies that
# require the accounts API endpoint (not the workspace API).
# Auth: uses CLI profile "ACCOUNT" locally (databricks auth login --host
# https://accounts.cloud.databricks.com --account-id <id> --profile ACCOUNT).
# In CI, DATABRICKS_AUTH_TYPE=github-oidc provides account-level auth.
provider "databricks" {
  alias         = "account"
  host          = "https://accounts.cloud.databricks.com"
  account_id    = var.databricks_account_id
  client_id     = var.databricks_client_id != "" ? var.databricks_client_id : null
  client_secret = var.databricks_client_secret != "" ? var.databricks_client_secret : null
  profile       = var.databricks_client_id != "" ? null : "ACCOUNT"
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

  providers = {
    databricks         = databricks
    databricks.account = databricks.account
  }

  environment       = var.environment
  account_id        = var.databricks_account_id
  github_repository = "karsten-s-nielsen/luxury-lakehouse"
  databricks_host   = var.databricks_host
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
  app_sp_application_id       = module.service_principals.hf_app_sp_application_id
  silver_schema_override      = "${var.environment}_silver"
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
  wheel_path               = "${module.catalog.libs_volume_path}/luxury_lakehouse-0.3.0-py3-none-any.whl"
  environment              = var.environment
  notification_emails      = var.notification_emails
  run_as_sp_application_id = module.service_principals.ingestion_sp_application_id
}

# ── Daily HF Costs Sync (catch-all backup) ──────────────────────────────────
# Reads _cost_history/*.json from HF Hub repos and MERGEs into
# workflow_cost_live. Ensures HF Jobs costs reach the cold-tier dbt model
# even if no dbt build runs that day. Primary display path is direct HF Hub
# read from the Taipy app — this is the belt-and-suspenders backup.

resource "databricks_job" "sync_hf_costs_daily" {
  name                = "sync-hf-costs-daily-${var.environment}"
  max_concurrent_runs = 1

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "UTC"
    pause_status           = var.environment == "dev" ? "PAUSED" : "UNPAUSED"
  }

  task {
    task_key        = "sync_hf_costs"
    timeout_seconds = 600

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "sync_hf_costs"

      parameters = [
        "--catalog", module.workspace.catalog_name,
        "--cards-dir", "/Workspace/Repos/luxury-lakehouse/workflow-cards"
      ]
    }

    environment_key = "hf-sync"
  }

  environment {
    environment_key = "hf-sync"

    spec {
      client = "1"

      dependencies = [
        "${module.catalog.libs_volume_path}/luxury_lakehouse-0.3.0-py3-none-any.whl",
        "huggingface_hub>=0.25.0",
        "pyyaml>=6.0"
      ]
    }
  }
}

# ── Module: Synced Tables ────────────────────────────────────────────────────
# Mirrors gold-layer Delta tables into Lakebase for low-latency app queries.

module "synced_tables" {
  source = "../../modules/synced_tables"

  catalog_name           = module.workspace.catalog_name
  database_instance_name = module.lakebase.instance_name
  environment            = var.environment
  gold_schema            = "${var.environment}_gold"
  observability_schema   = "observability"
}

# ── Databricks App (DEPRECATED) ──────────────────────────────────────────────
# Streamlit dashboard migrated to HF Spaces (luxury-lakehouse/soccer-analytics-app).
# The databricks_app resource and terraform/modules/app/ have been removed.
# Lakebase auth uses PAT-based OAuth via HF Space secrets.

# ── CI Service Principal: Catalog Access ───────────────────────────────────
# The terraform_ci SP needs ALL_PRIVILEGES on the catalog so terraform plan
# can read catalog, schema, and grant resources.  This is a composition-level
# grant because the SP and catalog come from separate modules.

resource "databricks_grant" "ci_sp_catalog" {
  catalog    = module.workspace.catalog_name
  principal  = module.service_principals.terraform_ci_sp_application_id
  privileges = ["ALL_PRIVILEGES"]
}

# ── SQL Warehouse: Explicit ACL Grants ───────────────────────────────────
# D40 (SEC-AUDIT): Warehouse had no grants — access relied on workspace
# defaults.  Explicit grants scoped to least-privilege per principal.
# SQL warehouses are workspace-level objects → databricks_permissions,
# not databricks_grant (which is for Unity Catalog objects).

resource "databricks_permissions" "sql_warehouse" {
  sql_endpoint_id = module.sql_warehouse.warehouse_id

  access_control {
    service_principal_name = module.service_principals.ingestion_sp_application_id
    permission_level       = "CAN_USE"
  }

  access_control {
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "CAN_USE"
  }
}

# ── Module: State KMS ──────────────────────────────────────────────────────
# Customer Managed Key for Terraform state encryption in S3 (L-10).

module "state_kms" {
  source = "../../modules/state_kms"

  environment  = var.environment
  state_bucket = "karstenskyt-terraform-state"
}

# ── Module: GitHub OIDC ──────────────────────────────────────────────────────
# IAM OIDC provider + scoped role for secretless GitHub Actions CI.

module "github_oidc" {
  source = "../../modules/github_oidc"

  environment       = var.environment
  github_repository = "karsten-s-nielsen/luxury-lakehouse"
  state_bucket      = "karstenskyt-terraform-state"
  kms_key_arn       = module.state_kms.kms_key_arn
}

# ── AWS Budget: Monthly cost alarm (COST-01) ─────────────────────────────────
# Alerts at 80% and 100% of $250/month budget. Subscriber email is set via
# var.alert_email in terraform.tfvars (not checked in).

resource "aws_budgets_budget" "monthly" {
  count = var.alert_email != "" ? 1 : 0

  name         = "luxury-lakehouse-monthly-${var.environment}"
  budget_type  = "COST"
  limit_amount = "250"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
