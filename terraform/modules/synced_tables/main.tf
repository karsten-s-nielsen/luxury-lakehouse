# ──────────────────────────────────────────────────────────────────────────────
# Module: Synced Tables — Delta-to-Lakebase Synchronization
# ──────────────────────────────────────────────────────────────────────────────
# Synced Database Tables mirror gold-layer Delta tables into the Lakebase
# PostgreSQL endpoint, enabling sub-second point lookups from the Streamlit
# dashboard without requiring a running SQL warehouse.
#
# Resource: databricks_database_synced_database_table
# Docs: https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/database_synced_database_table
#
# IMPORTANT: As of provider v1.110.0, this resource only exposes
# `database_instance_name` (Provisioned). For Lakebase Autoscaling projects,
# synced tables must be created via the Databricks UI (which supports
# project + branch selection), then imported into Terraform state:
#
#   terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_shots' \
#     'soccer_analytics.dev_gold.fct_shots_synced'
#
# The lifecycle block uses `ignore_changes = all` because the provider does
# not support updates to synced tables ("Update is not yet implemented").
#
# Fact tables (events and metrics):
#   - fct_shots:            Shot events with xG, outcome, body part
#   - fct_passes:           Pass events with success rate, distance, angle
#   - fct_player_stats:     Aggregated per-player-per-match statistics
#   - fct_match_summary:    Match-level aggregates (score, possession, xG)
#   - fct_player_embeddings: Vector embeddings for player similarity search
#   - fct_action_values:    SPADL/VAEP action-level offensive and defensive values
#   - fct_tracking_frames:  Tracking data (Metrica, IDSSE, SkillCorner) with velocity metrics
#
# Dimension tables (entities):
#   - dim_players:          Player master data (name, position, birth date)
#   - dim_teams:            Team master data (name, country, league)
#   - dim_competitions:     Competition/season master data
# ──────────────────────────────────────────────────────────────────────────────

# ── Fact Tables ──────────────────────────────────────────────────────────────

resource "databricks_database_synced_database_table" "fct_shots" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_shots_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_shots"
    primary_key_columns    = ["shot_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_passes" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_passes_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_passes"
    primary_key_columns    = ["pass_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_stats" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_stats_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_stats"
    primary_key_columns    = ["player_stats_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_match_summary" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_match_summary_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_match_summary"
    primary_key_columns    = ["match_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings"
    primary_key_columns    = ["embedding_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings_season" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_season_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_season"
    primary_key_columns    = ["embedding_season_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings_career" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_career_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_career"
    primary_key_columns    = ["canonical_player_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_action_values" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_action_values_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_action_values"
    primary_key_columns    = ["action_value_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_tracking_frames" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_tracking_frames_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_tracking_frames"
    primary_key_columns    = ["tracking_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_physical_stats" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_physical_stats_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_physical_stats"
    primary_key_columns    = ["physical_stats_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_defensive_values" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_defensive_values_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_defensive_values"
    primary_key_columns    = ["defensive_value_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_defcon_pressure" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_defcon_pressure_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_defcon_pressure"
    primary_key_columns    = ["pressure_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_defcon_actions" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_defcon_actions_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_defcon_actions"
    primary_key_columns    = ["defcon_action_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

# ── Dimension Tables ─────────────────────────────────────────────────────────

resource "databricks_database_synced_database_table" "dim_players" {
  name                   = "${var.catalog_name}.${var.gold_schema}.dim_players_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.dim_players"
    primary_key_columns    = ["player_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "dim_teams" {
  name                   = "${var.catalog_name}.${var.gold_schema}.dim_teams_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.dim_teams"
    primary_key_columns    = ["team_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "dim_competitions" {
  name                   = "${var.catalog_name}.${var.gold_schema}.dim_competitions_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.dim_competitions"
    primary_key_columns    = ["competition_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}
