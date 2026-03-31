# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Data Ingestion Pipeline
# ──────────────────────────────────────────────────────────────────────────────
# Creates a Databricks job that ingests data from five soccer data providers
# in parallel, then runs SPADL/VAEP action valuation:
#
#   statsbomb         — Free open-data events (shots, passes, lineups)
#   metrica           — Tracking data (player coordinates at 25fps)
#   wyscout           — Match events and player attributes
#   idsse             — Bundesliga DFL tracking (25fps, 7 matches from UC Volume)
#   idsse_events      — Bundesliga DFL event XML (7 matches, depends on idsse)
#   skillcorner       — A-League broadcast tracking (10fps, 10 matches via kloppy)
#   compute_spadl_vaep — SPADL conversion + VAEP scoring (depends on statsbomb + wyscout)
#   compute_xg_model   — Custom xG model scoring (depends on SPADL/VAEP)
#   compute_off_ball_xt — Off-Ball xT from tracking + pitch control (depends on tracking tasks)
#   compute_pitch_control — Spearman 2017 pitch control values (depends on tracking tasks)
#   compute_defcon_lite — DEFCON-lite defensive valuation (depends on SPADL/VAEP)
#   compute_elastic_sync — ELASTIC event-tracking alignment (depends on idsse_events)
#   compute_pausa     — PAUSA pass timing pipeline (depends on elastic_sync + OBSO import)
#   resolve_players   — Cross-source entity resolution (depends on statsbomb + wyscout)
#   compute_embeddings — Player behavioral + statistical embeddings (depends on entity resolution)
#   export_embeddings_training_data — SPADL sequences for Football2vec v2 (depends on SPADL + entity resolution)
#   compute_formations — Formation detection: EFPI + shape graph (depends on pitch control)
#   run_model_validation — Model drift detection (depends on compute_pausa)
#
# Schedule: Daily at 06:00 UTC (before business hours in US/EU timezones)
#
# Each task uses python_wheel_task to invoke entry points from the
# luxury_lakehouse Python package, ensuring consistent dependency management.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_job" "data_ingestion" {
  name                = "soccer-analytics-ingestion-${var.environment}"
  max_concurrent_runs = 1

  # ── Run as: dedicated ingestion service principal ────────────────────────
  dynamic "run_as" {
    for_each = var.run_as_sp_application_id != "" ? [1] : []
    content {
      service_principal_name = var.run_as_sp_application_id
    }
  }

  # ── Schedule: Daily at 6am UTC ───────────────────────────────────────────
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "UTC"
    pause_status           = var.environment == "dev" ? "PAUSED" : "UNPAUSED"
  }

  # ── Task: Ingest StatsBomb data ──────────────────────────────────────────
  task {
    task_key        = "ingest_statsbomb"
    timeout_seconds = 900
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_statsbomb"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    # Uses statsbomb environment (statsbombpy not in core deps)
    environment_key = "statsbomb"
  }

  # ── Task: Ingest Metrica tracking data ───────────────────────────────────
  task {
    task_key        = "ingest_metrica"
    timeout_seconds = 900
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_metrica"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Ingest Wyscout data ────────────────────────────────────────────
  task {
    task_key        = "ingest_wyscout"
    timeout_seconds = 900
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_wyscout"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        "--data-dir", "/Volumes/${var.catalog_name}/bronze/libs/wyscout"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Ingest IDSSE Bundesliga tracking data ─────────────────────────
  # Uses stdlib XML parser — reads pre-downloaded DFL XML from UC Volume.
  # No floodlight dependency needed (only pandas from default env).
  task {
    task_key        = "ingest_idsse"
    timeout_seconds = 900
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_idsse"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Ingest SkillCorner A-League tracking data ────────────────────
  task {
    task_key        = "ingest_skillcorner"
    timeout_seconds = 900
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_skillcorner"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "tracking"
  }

  # ── Task: Compute SPADL actions and VAEP scores ─────────────────────────
  task {
    task_key        = "compute_spadl_vaep"
    timeout_seconds = 7200
    max_retries     = 1

    depends_on {
      task_key = "ingest_statsbomb"
    }
    depends_on {
      task_key = "ingest_wyscout"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_spadl_vaep"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Compute Expected Threat grids from SPADL actions ─────────
  task {
    task_key        = "compute_expected_threat"
    timeout_seconds = 900
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_expected_threat"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Score shots with custom xG models ─────────────────────────
  task {
    task_key        = "compute_xg_model"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_xg_model"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Score shots with xG v2 set encoder (Deep Sets + MC dropout) ──
  task {
    task_key        = "compute_xg_model_v2"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_xg_model_v2"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Compute Off-Ball xT from tracking data ───────────────────
  # Depends on all three tracking providers + xT grid computation.
  task {
    task_key        = "compute_off_ball_xt"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }
    depends_on {
      task_key = "compute_expected_threat"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_off_ball_xt"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Compute pitch control values for tracking data ───────────────
  # Reads gold fct_tracking_frames, computes Spearman 2017 pitch control
  # at each player's position, writes bronze.pitch_control_values.
  task {
    task_key        = "compute_pitch_control"
    timeout_seconds = 7200
    max_retries     = 1

    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_pitch_control"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Detect team formations (EFPI + shape graph dual-detector) ──
  # Reads fct_tracking_frames, runs EFPI template matching (Bekkers &
  # Dabadghao 2025) and shape graph geometric detection (Sotudeh 2026).
  # Writes formation_labels (both detectors) + player_positions (shape graph).
  task {
    task_key        = "compute_formations"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_pitch_control"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_formations"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Detect line-breaking passes ────────────────────────────────
  # Path A: StatsBomb 360 freeze-frame defender positions.
  # Path B: Metrica tracking data for defender line estimation.
  # Writes bronze.line_breaking_results.
  task {
    task_key        = "compute_line_breaking"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "ingest_statsbomb"
    }
    depends_on {
      task_key = "ingest_metrica"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_line_breaking"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Compute DEFCON-lite defensive valuation ──────────────────────
  # Reads gold fct_action_values + bronze statsbomb_360, assigns defensive
  # credits per-defender per-action, trains XGBoost value estimators.
  task {
    task_key        = "compute_defcon_lite"
    timeout_seconds = 7200
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_defcon_lite"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Resolve cross-source player identity ───────────────────────────
  # Matches StatsBomb and Wyscout players via TF-IDF + rapidfuzz.
  # Writes player_xref_raw bronze table for dbt int_player_xref → dim_players.
  task {
    task_key        = "resolve_players"
    timeout_seconds = 900
    max_retries     = 1

    depends_on {
      task_key = "ingest_statsbomb"
    }
    depends_on {
      task_key = "ingest_wyscout"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "resolve_players"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Compute player embeddings ─────────────────────────────────────
  # Generates behavioral (Doc2Vec action sequences) and statistical (z-score)
  # player embedding vectors for similarity search via pgvector.
  task {
    task_key        = "compute_embeddings"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "resolve_players"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
  }

  # ── Task: Export SPADL action sequences for Football2Vec v2 training ──
  # Reads fct_action_values joined to dim_players, groups by player-match,
  # writes Parquet to UC Volume and uploads to HF Hub.
  task {
    task_key        = "export_embeddings_training_data"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "resolve_players"
    }
    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "export_embeddings_training_data"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "default"
  }

  # ── Task: Ingest IDSSE event data (DFL event XML) ──────────────────────
  # Parses DFL event XML from UC Volume for the same 7 Bundesliga matches.
  # Separate from tracking ingestion — different XML schema.
  task {
    task_key        = "ingest_idsse_events"
    timeout_seconds = 900
    max_retries     = 1

    depends_on {
      task_key = "ingest_idsse"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_idsse_events"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Compute ELASTIC event-tracking alignment ────────────────────
  # Kim et al. (2025) ELASTIC sync: aligns discrete events with 25fps
  # tracking frames via ball acceleration + player-ball distance features.
  task {
    task_key        = "compute_elastic_sync"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "ingest_idsse_events"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_elastic_sync"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Compute PAUSA pass timing values ────────────────────────────
  # Lee et al. (2026) PAUSA: temporal judgment × spatial selection from
  # OBSO surfaces. Depends on ELASTIC sync results and pre-computed OBSO
  # values (imported from HF Jobs GPU run).
  task {
    task_key        = "compute_pausa"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_elastic_sync"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_pausa"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Run model validation and drift detection ────────────────────
  # PSI, Wasserstein, CUSUM, and hard bounds across all ML models.
  # Runs post-dbt and post-PAUSA to validate all model outputs.
  task {
    task_key        = "run_model_validation"
    timeout_seconds = 900
    max_retries     = 1

    depends_on {
      task_key = "compute_pausa"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "run_model_validation"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Sync HF Jobs costs to Delta (independent, no dependencies) ────
  task {
    task_key        = "sync_hf_costs"
    timeout_seconds = 600

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "sync_hf_costs"

      parameters = [
        "--catalog", var.catalog_name,
        "--cards-dir", "/Workspace/Repos/luxury-lakehouse/workflow-cards"
      ]
    }

    environment_key = "default"
  }

  # ── Environment definition for serverless tasks ──────────────────────────
  environment {
    environment_key = "default"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path
      ]
    }
  }

  # ── Environment for StatsBomb ingestion (statsbombpy API client) ────────
  environment {
    environment_key = "statsbomb"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "statsbombpy>=1.13.0"
      ]
    }
  }

  # ── Environment for SPADL/VAEP task (includes analytics extras) ─────────
  # No statsbombpy needed — pipeline reads from bronze, not the API.
  environment {
    environment_key = "analytics"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "socceraction==1.5.3",
        "xgboost==3.2.0",
        "multimethod==1.12",
        "rapidfuzz>=3.6.0",
        "unidecode>=1.3.0",
        "sparse-dot-topn>=1.1.0",
        "mlflow>=2.17.0"
      ]
    }
  }

  # ── Environment for SkillCorner tracking task (kloppy for open data download)
  environment {
    environment_key = "tracking"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "kloppy>=3.17.0,<4.0"
      ]
    }
  }

  # ── Environment for player embeddings (gensim Doc2Vec + HF Hub) ────────
  environment {
    environment_key = "embeddings"

    spec {
      client = "1"

      dependencies = concat(
        [var.wheel_path],
        [
          "gensim>=4.3.0",
          "huggingface_hub>=0.25.0",
        ]
      )
    }
  }

  # ── Job-level settings ───────────────────────────────────────────────────

  notification_settings {
    no_alert_for_skipped_runs = true
  }

  dynamic "email_notifications" {
    for_each = length(var.notification_emails) > 0 ? [1] : []

    content {
      on_start   = var.notification_emails
      on_success = var.notification_emails
      on_failure = var.notification_emails
    }
  }

  tags = {
    project     = "luxury-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
    pipeline    = "ingestion"
  }
}
