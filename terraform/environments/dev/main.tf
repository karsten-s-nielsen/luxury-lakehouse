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
  workspace_id      = var.databricks_workspace_id
}

# ── Module: Secret Scopes ───────────────────────────────────────────────────��
# Creates Databricks secret scopes and grants READ to the ingestion SP.
# Secret VALUES must be populated manually after apply:
#   databricks secrets put-secret --scope hf --key token
#   databricks secrets put-secret --scope pining --key token

module "secret_scopes" {
  source = "../../modules/secret_scopes"

  scopes = {
    hf = {
      keys         = ["token"]
      read_acl_sps = [module.service_principals.ingestion_sp_application_id]
    }
    pining = {
      keys         = ["token"]
      read_acl_sps = [module.service_principals.ingestion_sp_application_id]
    }
  }
}

import {
  to = module.secret_scopes.databricks_secret_scope.scope["hf"]
  id = "hf"
}

import {
  to = module.secret_scopes.databricks_secret_scope.scope["pining"]
  id = "pining"
}

import {
  to = module.secret_scopes.databricks_secret_acl.read["hf-008b207b-96a8-4d54-b185-a77479a55abe"]
  id = "hf|||008b207b-96a8-4d54-b185-a77479a55abe"
}

import {
  to = module.secret_scopes.databricks_secret_acl.read["pining-008b207b-96a8-4d54-b185-a77479a55abe"]
  id = "pining|||008b207b-96a8-4d54-b185-a77479a55abe"
}

import {
  to = databricks_permissions.vaep_experiment_acl
  id = "/experiments/1644169474913777"
}

# ── Module: Catalog (Medallion Schemas) ──────────────────────────────────────
# Creates bronze / silver / gold schemas inside the soccer_analytics catalog.
# Also manages Unity Catalog grants for the ingestion and app service principals.

module "catalog" {
  source = "../../modules/catalog"

  catalog_name                   = module.workspace.catalog_name
  environment                    = var.environment
  ingestion_sp_application_id    = module.service_principals.ingestion_sp_application_id
  enable_ingestion_sp_grants     = true
  app_sp_application_id          = module.service_principals.hf_app_sp_application_id
  enable_app_sp_grants           = true
  silver_schema_override         = "${var.environment}_silver"
  gold_schema_override           = "${var.environment}_gold"
  dbt_owners_group_name          = module.service_principals.dbt_owners_group_display_name
  lakehouse_operators_group_name = module.service_principals.lakehouse_operators_group_display_name
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
  wheel_path               = "${module.catalog.libs_volume_path}/luxury_lakehouse-0.3.68-py3-none-any.whl"
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
  observability_schema   = "observability"
}

# ── Databricks App (DEPRECATED) ──────────────────────────────────────────────
# Streamlit dashboard migrated to HF Spaces (luxury-lakehouse/soccer-analytics-app).
# The databricks_app resource and terraform/modules/app/ have been removed.
# Lakebase auth uses PAT-based OAuth via HF Space secrets.

# ── CI Service Principal: Catalog Access ───────────────────────────────────
# Workspace admin covers workspace-scoped objects (SQL warehouses, jobs,
# SCIM) but does NOT cover Unity Catalog privileges (catalog, schema,
# volume, model, grant). The CI SP manages 4 schemas, 2 volumes, and
# 15+ grants via Terraform — it needs both read (terraform plan) and
# write (terraform apply) UC privileges across the catalog.
# ALL_PRIVILEGES is the pragmatic grant; per-schema scoping would require
# a databricks_grant per schema/volume/model and break on every new resource.

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

  # databricks_permissions is authoritative — it replaces the full ACL.
  # The deployer must be listed explicitly or the apply will fail with
  # "cannot remove management permissions for the current user".
  access_control {
    user_name        = module.service_principals.deployer_account_email
    permission_level = "IS_OWNER"
  }

  access_control {
    service_principal_name = module.service_principals.ingestion_sp_application_id
    permission_level       = "CAN_USE"
  }

  # CAN_MANAGE (not CAN_USE): the CI SP runs terraform apply, which calls
  # the permissions API. The Databricks safety guard blocks setting an ACL
  # that would demote the caller below CAN_MANAGE.
  access_control {
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "CAN_MANAGE"
  }
}

# ── Jobs API: Read-only access for Taipy app SP ──────────────────────────
# The Workflows dashboard calls jobs.list_runs() to show Status, Last Run,
# and Last Duration. CAN_VIEW is the minimum permission — it allows reading
# run metadata but not triggering or modifying jobs.

# Authoritative ACL for the daily ingestion job. Every non-default grant on
# this job is declared here; databricks_permissions is exclusive per target
# object. Blocks are sorted alphabetically by service_principal_name to
# preempt the Databricks provider's positional-block-matching drift.
resource "databricks_permissions" "ingestion_job_acl" {
  job_id = module.workflows.ingestion_job_id

  access_control {
    # hf_app_v2 SP — Taipy app reads job run history for the AI/ML Workflows page.
    service_principal_name = module.service_principals.hf_app_sp_application_id
    permission_level       = "CAN_VIEW"
  }

  access_control {
    # CI SP — terraform apply modifies the job definition; IS_OWNER is the
    # least-privilege grant that permits this without workspace admin.
    # Precondition for SEC4 admins-group removal (deferred follow-up).
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "IS_OWNER"
  }
}

# Rename hf_app_view_* -> *_acl via `moved` block — Terraform treats this as
# a state-address migration, not destroy/create, so no ACL gap during apply.
# The sync_hf_costs_job_acl moved block + resource were removed alongside the
# standalone sync_hf_costs_daily job — the sub-operation still runs daily via
# the hf_sync super-task inside data_ingestion, so the standalone job was
# redundant. Destroy on next apply.
moved {
  from = databricks_permissions.hf_app_view_ingestion_job
  to   = databricks_permissions.ingestion_job_acl
}

# ── Lakebase: Explicit CI SP Access (SEC4, 2026-04-17) ──────────────────────
# Before SEC4, the CI SP managed Lakebase resources transitively via the
# workspace `admins` group. SEC4 removes that membership; the CI SP now
# needs an explicit CAN_MANAGE grant so `terraform apply` can modify the
# project. Endpoints inherit this grant from the parent project — no
# separate endpoint ACL is needed (confirmed via empirical audit: Lakebase
# Autoscaling has no `database-instances` ACL surface; endpoints authorize
# through the parent project).
#
# databricks_permissions is authoritative per target object — every non-
# inherited grant on this project must be declared here. Inherited permissions
# at `/database-projects` (admins CAN_MANAGE, users CAN_CREATE) are unaffected.
# Provider supports only {CAN_USE, CAN_MANAGE} for `database_project_name`
# (no IS_OWNER), per
# terraform-provider-databricks v1.113 permissions/permission_definitions.go:772-782.
#
# access_control blocks are sorted alphabetically by principal reference to
# preempt positional-block-matching drift.
#
# See: docs/superpowers/specs/2026-04-17-sec4-ci-sp-least-privilege-design.md
#      docs/superpowers/specs/2026-04-17-sec4-workspace-resource-inventory.md
resource "databricks_permissions" "lakebase_project_acl" {
  database_project_name = module.lakebase.project_id

  access_control {
    user_name        = module.service_principals.deployer_account_email
    permission_level = "CAN_MANAGE"
  }

  access_control {
    # hf_app_v2 SP — Taipy app reads synced tables via the Lakebase endpoint.
    service_principal_name = module.service_principals.hf_app_sp_application_id
    permission_level       = "CAN_USE"
  }

  access_control {
    # Ingestion SP — `refresh_synced_tables` daily job uses the endpoint.
    service_principal_name = module.service_principals.ingestion_sp_application_id
    permission_level       = "CAN_USE"
  }

  access_control {
    # CI SP — `terraform apply` modifies the project. CAN_MANAGE is the
    # minimum level permitting modification; provider supports no IS_OWNER
    # for database-projects. Precondition for SEC4 admins-group removal.
    service_principal_name = module.service_principals.terraform_ci_sp_application_id
    permission_level       = "CAN_MANAGE"
  }
}

# ── MLflow Experiments: Ingestion SP Read Access ──────────────────────────────
# The ingestion SP loads @Champion models (VAEP, xG v2) at scoring time.
# Unity Catalog grants cover the registered model alias, but artifact-hash
# verification (ADR-012 §2) calls mlflow.get_run() which requires CAN_READ
# on the backing workspace experiment. Without this, the hash check silently
# skips — defeating integrity verification.
#
# databricks_permissions is authoritative per experiment — every non-inherited
# ACL must be declared. The deployer IS_OWNER is inherited (experiment creator)
# and does not need to be re-declared.

data "databricks_mlflow_experiment" "vaep" {
  name = "/soccer_analytics/vaep_model"
}

data "databricks_mlflow_experiment" "xg_v2" {
  name = "/soccer_analytics/xg_model_v2"
}

resource "databricks_permissions" "vaep_experiment_acl" {
  experiment_id = data.databricks_mlflow_experiment.vaep.id

  access_control {
    service_principal_name = module.service_principals.ingestion_sp_application_id
    permission_level       = "CAN_READ"
  }
}

resource "databricks_permissions" "xg_v2_experiment_acl" {
  experiment_id = data.databricks_mlflow_experiment.xg_v2.id

  access_control {
    service_principal_name = module.service_principals.ingestion_sp_application_id
    permission_level       = "CAN_READ"
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
