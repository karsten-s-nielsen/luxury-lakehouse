#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Import UI-created synced tables into Terraform state
# ──────────────────────────────────────────────────────────────────────────────
# Run from: terraform/environments/dev/
# Prereq: synced tables must already exist (created via Databricks UI)
#
# The databricks_database_synced_database_table Terraform resource (provider
# v1.110.0) does not support Lakebase Autoscaling projects — it only exposes
# database_instance_name for Provisioned instances. Synced tables targeting
# Autoscaling projects must be created via the UI and imported here.
#
# The lifecycle { ignore_changes = all } block in the module prevents
# Terraform from attempting to update any field after import (the provider
# does not support updates: "Update Synced Database Table is not yet implemented").
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

cd "$(dirname "$0")/../terraform/environments/dev" || exit 1

export TF_VAR_databricks_token="${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN}"

# Fact tables
terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_shots' \
  'soccer_analytics.dev_gold.fct_shots_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_passes' \
  'soccer_analytics.dev_gold.fct_passes_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_player_stats' \
  'soccer_analytics.dev_gold.fct_player_stats_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_match_summary' \
  'soccer_analytics.dev_gold.fct_match_summary_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_player_embeddings' \
  'soccer_analytics.dev_gold.fct_player_embeddings_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_action_values' \
  'soccer_analytics.dev_gold.fct_action_values_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_tracking_frames' \
  'soccer_analytics.dev_gold.fct_tracking_frames_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_physical_stats' \
  'soccer_analytics.dev_gold.fct_physical_stats_synced'

# Dimension tables
terraform import 'module.synced_tables.databricks_database_synced_database_table.dim_players' \
  'soccer_analytics.dev_gold.dim_players_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.dim_teams' \
  'soccer_analytics.dev_gold.dim_teams_synced'

terraform import 'module.synced_tables.databricks_database_synced_database_table.dim_competitions' \
  'soccer_analytics.dev_gold.dim_competitions_synced'

echo "All 11 synced tables imported. Run 'terraform plan' to verify."
