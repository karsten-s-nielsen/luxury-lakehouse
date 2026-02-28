# ──────────────────────────────────────────────────────────────────────────────
# Module: Lakebase — Autoscaling PostgreSQL 17 (Project + Endpoint)
# ──────────────────────────────────────────────────────────────────────────────
# Lakebase Autoscaling provides a managed PostgreSQL 17-compatible interface
# on top of Delta Lake tables, enabling:
#   - Standard SQL/psql/JDBC/ODBC access to lakehouse data
#   - Low-latency point lookups for the Streamlit dashboard
#   - True scale-to-zero with usage-based billing
#   - Automatic scaling from min to max compute units
#
# The Streamlit app connects to Lakebase via standard PostgreSQL drivers,
# querying synced gold-layer tables without spinning up a SQL warehouse.
#
# Resources: databricks_postgres_project, databricks_postgres_endpoint
#            (added in provider v1.110.0)
#
# NOTE: The "production" branch is auto-created as the root branch when the
# project is provisioned. It cannot be deleted or managed via Terraform, so
# we reference it by convention in the endpoint parent path.
#
# Docs: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/postgres_project
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_postgres_project" "soccer_analytics" {
  project_id = "soccer-analytics-${var.environment}"
  spec = {
    pg_version   = 17
    display_name = "Soccer Analytics Lakebase (${var.environment})"
    default_endpoint_settings = {
      autoscaling_limit_min_cu = var.autoscaling_min_cu
      autoscaling_limit_max_cu = var.autoscaling_max_cu
      suspend_timeout_duration = var.suspend_timeout_duration
    }
  }
}

resource "databricks_postgres_endpoint" "primary" {
  endpoint_id = "primary"
  parent      = "${databricks_postgres_project.soccer_analytics.name}/branches/production"
  spec = {
    endpoint_type            = "ENDPOINT_TYPE_READ_WRITE"
    autoscaling_limit_min_cu = var.autoscaling_min_cu
    autoscaling_limit_max_cu = var.autoscaling_max_cu
    suspend_timeout_duration = var.suspend_timeout_duration
  }

  # The primary endpoint is auto-created when the project is provisioned.
  # After `terraform import`, endpoint_id and spec are not in state, causing
  # a ForceNew diff. Ignoring these fields prevents unnecessary recreation.
  #
  # LIMITATION: Changes to autoscaling_limit_min_cu, autoscaling_limit_max_cu,
  # and suspend_timeout_duration on the endpoint will be silently ignored.
  # To update endpoint settings: modify via Databricks UI, or temporarily
  # remove this lifecycle block, apply, then re-add it.
  lifecycle {
    ignore_changes = [endpoint_id, spec]
  }
}
