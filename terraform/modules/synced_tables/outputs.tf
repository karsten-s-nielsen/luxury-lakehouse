# ──────────────────────────────────────────────────────────────────────────────
# Module: Synced Tables — Outputs
# ──────────────────────────────────────────────────────────────────────────────

output "synced_fact_tables" {
  description = "Map of fact table names to their synced table resource names"
  value = {
    fct_shots             = databricks_database_synced_database_table.fct_shots.name
    fct_passes            = databricks_database_synced_database_table.fct_passes.name
    fct_player_stats      = databricks_database_synced_database_table.fct_player_stats.name
    fct_match_summary     = databricks_database_synced_database_table.fct_match_summary.name
    fct_player_embeddings = databricks_database_synced_database_table.fct_player_embeddings.name
  }
}

output "synced_dimension_tables" {
  description = "Map of dimension table names to their synced table resource names"
  value = {
    dim_players      = databricks_database_synced_database_table.dim_players.name
    dim_teams        = databricks_database_synced_database_table.dim_teams.name
    dim_competitions = databricks_database_synced_database_table.dim_competitions.name
  }
}
