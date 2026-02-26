# ──────────────────────────────────────────────────────────────────────────────
# Module: SQL Warehouse — Serverless Compute
# ──────────────────────────────────────────────────────────────────────────────
# Creates a serverless SQL warehouse for ad-hoc queries, dbt transformations,
# and dashboard data access.
#
# Cost control strategy for the $100/month dev budget:
#   - Serverless: no idle cluster costs, pay only for query time
#   - 2X-Small:   smallest available size, sufficient for dev workloads
#   - auto_stop:  10 minutes — aggressively reclaim resources when idle
#
# The warehouse is referenced by the workflows module (for dbt/SQL tasks)
# and can be used interactively via the Databricks SQL editor.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_sql_endpoint" "serverless" {
  name = "soccer-analytics-warehouse-${var.environment}"

  # Serverless removes the need for a running cluster — pay per query
  enable_serverless_compute = true

  # Smallest available cluster size for cost efficiency
  cluster_size = var.cluster_size

  # Aggressive auto-stop: shut down after 10 minutes of inactivity
  auto_stop_mins = var.auto_stop_mins

  # Warehouse type: PRO required for serverless on AWS
  warehouse_type = "PRO"

  tags {
    custom_tags {
      key   = "project"
      value = "luxury-lakehouse"
    }
    custom_tags {
      key   = "environment"
      value = var.environment
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}
