# ──────────────────────────────────────────────────────────────────────────────
# Module: Synced Tables — Delta-to-Lakebase Synchronization
# ──────────────────────────────────────────────────────────────────────────────
# Synced Database Tables mirror gold-layer Delta tables into the Lakebase
# PostgreSQL instance, enabling sub-second point lookups from the Streamlit
# dashboard without requiring a running SQL warehouse.
#
# Resource: databricks_database_synced_database_table
# Docs: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/database_synced_database_table
#
# Fact tables (events and metrics):
#   - fct_shots:            Shot events with xG, outcome, body part
#   - fct_passes:           Pass events with success rate, distance, angle
#   - fct_player_stats:     Aggregated per-player-per-match statistics
#   - fct_match_summary:    Match-level aggregates (score, possession, xG)
#   - fct_player_embeddings: Vector embeddings for player similarity search
#
# Dimension tables (entities):
#   - dim_players:          Player master data (name, position, birth date)
#   - dim_teams:            Team master data (name, country, league)
#   - dim_competitions:     Competition/season master data
# ──────────────────────────────────────────────────────────────────────────────

# ── Fact Tables ──────────────────────────────────────────────────────────────

resource "databricks_database_synced_database_table" "fct_shots" {
  name                   = "${var.catalog_name}.gold.fct_shots_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.fct_shots"
    primary_key_columns    = ["shot_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

resource "databricks_database_synced_database_table" "fct_passes" {
  name                   = "${var.catalog_name}.gold.fct_passes_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.fct_passes"
    primary_key_columns    = ["pass_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

resource "databricks_database_synced_database_table" "fct_player_stats" {
  name                   = "${var.catalog_name}.gold.fct_player_stats_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.fct_player_stats"
    primary_key_columns    = ["player_id", "match_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

resource "databricks_database_synced_database_table" "fct_match_summary" {
  name                   = "${var.catalog_name}.gold.fct_match_summary_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.fct_match_summary"
    primary_key_columns    = ["match_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings" {
  name                   = "${var.catalog_name}.gold.fct_player_embeddings_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.fct_player_embeddings"
    primary_key_columns    = ["player_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

# ── Dimension Tables ─────────────────────────────────────────────────────────

resource "databricks_database_synced_database_table" "dim_players" {
  name                   = "${var.catalog_name}.gold.dim_players_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.dim_players"
    primary_key_columns    = ["player_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

resource "databricks_database_synced_database_table" "dim_teams" {
  name                   = "${var.catalog_name}.gold.dim_teams_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.dim_teams"
    primary_key_columns    = ["team_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}

resource "databricks_database_synced_database_table" "dim_competitions" {
  name                   = "${var.catalog_name}.gold.dim_competitions_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.gold.dim_competitions"
    primary_key_columns    = ["competition_id"]
    scheduling_policy      = "SNAPSHOT"
  }
}
