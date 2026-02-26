# ──────────────────────────────────────────────────────────────────────────────
# Module: App — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "app_name" {
  description = "Name of the deployed Databricks App"
  value       = databricks_app.streamlit.name
}

output "app_url" {
  description = "URL of the deployed Streamlit dashboard"
  value       = databricks_app.streamlit.url
}
