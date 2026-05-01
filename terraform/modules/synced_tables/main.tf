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
#   - fct_xg_predictions:   xG model predictions (logistic + gradient boosted) per shot
#   - fct_action_values:    SPADL/VAEP action-level offensive and defensive values
#   - fct_tracking_frames:  Tracking data (Metrica, IDSSE, SkillCorner) with velocity metrics
#   - fct_formation_labels: Formation detection windows (EFPI + shape graph)
#   - fct_goalkeeper_stats: Per-match goalkeeper statistics (saves, claims, xT, PSxG)
#   - fct_line_breaking_results: Line-breaking detection per pass event
#   - fct_off_ball_xt:      Off-ball expected threat per player per match
#   - fct_pausa_rankings:   Player-level PAUSA aggregate rankings
#   - fct_player_percentiles: Per-competition percentile ranks for player metrics
#   - fct_player_positions: Per-frame tactical position labels (shape graph)
#   - fct_position_maps:    Aggregated position maps per player per match
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

resource "databricks_database_synced_database_table" "fct_xg_predictions" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions"
    primary_key_columns    = ["shot_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

# fct_xg_predictions_v2_synced — PR 3 / ADR-013 (2026-04-22).
# First mart under ADR-013 ("ML inference outputs flow Python → bronze → dbt
# staging → gold mart with contract enforced"). Deep Sets + MC dropout xG.
# Per ADR-005 Path A: create in the Databricks UI first, then `terraform import`:
#   terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_xg_predictions_v2' \
#     'soccer_analytics.dev_gold.fct_xg_predictions_v2_synced'
resource "databricks_database_synced_database_table" "fct_xg_predictions_v2" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_v2_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_xg_predictions_v2"
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
    # PR-Cycle-C PR-γ pilot (2026-05-01): TRIGGERED + Delta CDF on source.
    # `lifecycle.ignore_changes = all` means this is declared intent only —
    # the actual mode lives on the UI-created resource. ADR-021 codifies the
    # per-mart sync policy triage.
    scheduling_policy = "TRIGGERED"
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

# fct_discipline_events_synced — Match Summary redesign (2026-04-19).
# Per-event discipline mart used for Row 1 red-card auto-inclusion and
# Row 2 red-card markers on the xG race chart. Per ADR-005 Path A: create
# in the Databricks UI first, then `terraform import`:
#   terraform import 'module.synced_tables.databricks_database_synced_database_table.fct_discipline_events' \
#     'soccer_analytics.dev_gold.fct_discipline_events_synced'
resource "databricks_database_synced_database_table" "fct_discipline_events" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_discipline_events_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_discipline_events"
    primary_key_columns    = ["event_id"]
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
    # PR-Cycle-C PR-γ pilot (2026-05-01): TRIGGERED + Delta CDF on source.
    # `lifecycle.ignore_changes = all` means this is declared intent only —
    # the actual mode lives on the UI-created resource. ADR-021 codifies the
    # per-mart sync policy triage.
    scheduling_policy = "TRIGGERED"
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
    # PR-Cycle-C PR-γ pilot (2026-05-01): TRIGGERED + Delta CDF on source.
    # `lifecycle.ignore_changes = all` means this is declared intent only —
    # the actual mode lives on the UI-created resource. ADR-021 codifies the
    # per-mart sync policy triage.
    scheduling_policy = "TRIGGERED"
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

resource "databricks_database_synced_database_table" "fct_pausa_values" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_pausa_values_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_pausa_values"
    primary_key_columns    = ["pass_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_pass_timing" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_pass_timing_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_pass_timing"
    primary_key_columns    = ["player_id", "match_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_tracking_avg_positions" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_tracking_avg_positions_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_tracking_avg_positions"
    primary_key_columns    = ["avg_position_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_tracking_shape_timeline" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_tracking_shape_timeline_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_tracking_shape_timeline"
    primary_key_columns    = ["shape_timeline_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_formation_labels" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_formation_labels_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_formation_labels"
    primary_key_columns    = ["formation_label_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_goalkeeper_stats" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_goalkeeper_stats_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_goalkeeper_stats"
    primary_key_columns    = ["gk_stat_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_line_breaking_results" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_line_breaking_results_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_line_breaking_results"
    primary_key_columns    = ["line_breaking_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_off_ball_xt" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_off_ball_xt_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_off_ball_xt"
    primary_key_columns    = ["off_ball_xt_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_pausa_rankings" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_pausa_rankings_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_pausa_rankings"
    primary_key_columns    = ["player_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_percentiles" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_percentiles_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_percentiles"
    primary_key_columns    = ["player_id", "competition_id", "season_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_positions" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_positions_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_positions"
    primary_key_columns    = ["position_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_position_maps" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_position_maps_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_position_maps"
    primary_key_columns    = ["position_map_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_space_creation" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_space_creation_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_space_creation"
    primary_key_columns    = ["space_creation_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings_career_360" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_career_360_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_career_360"
    primary_key_columns    = ["canonical_player_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_player_embeddings_season_360" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_season_360_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_player_embeddings_season_360"
    primary_key_columns    = ["embedding_season_360_id"]
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

# ── Cost / Observability tables ──────────────────────────────────────────

resource "databricks_database_synced_database_table" "fct_workflow_costs" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_workflow_costs_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_workflow_costs"
    primary_key_columns    = ["task_key", "usage_date", "job_run_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "workflow_cost_live" {
  name                   = "${var.catalog_name}.${var.observability_schema}.workflow_cost_live_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.observability_schema}.workflow_cost_live"
    primary_key_columns    = ["run_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

# ── Dimension Tables ───────────────────────────────────────────────────────

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

# ── Pre-aggregated marts (2026-04-16 optimization audit) ───────────────────
# Each mart below replaces a verified Taipy query bottleneck where a comp-only
# filter on a >1M-row fact table triggered Parallel Seq Scan (3-13 s).  The
# pre-aggregated grain serves the same filter combos in <10 ms.

resource "databricks_database_synced_database_table" "fct_heatmap_agg" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_heatmap_agg_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    # Composite PK — every row is uniquely identified by this tuple
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_heatmap_agg"
    primary_key_columns    = ["competition_id", "team_id", "action_type", "x_bin", "y_bin"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_vaep_breakdown_agg" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_vaep_breakdown_agg_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_vaep_breakdown_agg"
    primary_key_columns    = ["competition_id", "team_id", "player_id", "action_type"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_gk_actions_detail" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_gk_actions_detail_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_gk_actions_detail"
    primary_key_columns    = ["gk_action_id"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}

resource "databricks_database_synced_database_table" "fct_funnel_stages_agg" {
  name                   = "${var.catalog_name}.${var.gold_schema}.fct_funnel_stages_agg_synced"
  database_instance_name = var.database_instance_name
  logical_database_name  = "databricks_postgres"

  spec = {
    source_table_full_name = "${var.catalog_name}.${var.gold_schema}.fct_funnel_stages_agg"
    primary_key_columns    = ["match_id", "team_id", "game_state"]
    scheduling_policy      = "SNAPSHOT"
  }

  lifecycle {
    ignore_changes = all
  }
}
