# ──────────────────────────────────────────────────────────────────────────────
# Module: Lakebase — PostgreSQL-Compatible Database Instance
# ──────────────────────────────────────────────────────────────────────────────
# Lakebase provides a managed PostgreSQL wire-compatible interface on top of
# Delta Lake tables, enabling:
#   - Standard SQL/psql/JDBC/ODBC access to lakehouse data
#   - Low-latency point lookups for the Streamlit dashboard
#   - Scale managed by capacity units (CU_1 through CU_8)
#
# The Streamlit app connects to Lakebase via standard PostgreSQL drivers,
# querying synced gold-layer tables without spinning up a SQL warehouse.
#
# Resource: databricks_database_instance (added in provider v1.98.0)
# Docs: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/database_instance
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_database_instance" "soccer_analytics" {
  name     = "soccer-analytics-lakebase-${var.environment}"
  capacity = var.capacity
  stopped  = var.stopped
}
