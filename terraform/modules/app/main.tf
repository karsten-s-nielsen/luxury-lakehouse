# ──────────────────────────────────────────────────────────────────────────────
# Module: App — Databricks Apps (Streamlit Dashboard)
# ──────────────────────────────────────────────────────────────────────────────
# Deploys the soccer analytics Streamlit dashboard as a Databricks App.
#
# Databricks Apps provides:
#   - Managed hosting with workspace-level authentication
#   - Direct access to Unity Catalog data via Lakebase/SQL warehouse
#   - Automatic HTTPS and SSO integration
#   - No separate infrastructure to manage
#
# The app source code lives in src/luxury_lakehouse/app/ and is deployed
# as part of the CI/CD pipeline.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_app" "streamlit" {
  name        = "soccer-analytics-dashboard-${var.environment}"
  description = "Soccer analytics Streamlit dashboard — explore shots, passes, player stats, and match summaries with interactive visualizations."

  # Grant the app's service principal explicit access to the SQL warehouse.
  # This follows least-privilege: the app can query via SQL but has no
  # broader workspace permissions.
  resources {
    name = "sql-warehouse"
    sql_warehouse {
      id         = var.sql_warehouse_id
      permission = "CAN_USE"
    }
  }
}
