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
#   skillcorner       — A-League broadcast tracking (10fps, 10 matches via kloppy)
#   compute_spadl_vaep — SPADL conversion + VAEP scoring (depends on statsbomb + wyscout)
#   compute_off_ball_xt — Off-Ball xT from tracking + pitch control (depends on tracking tasks)
#
# Schedule: Daily at 06:00 UTC (before business hours in US/EU timezones)
#
# Each task uses python_wheel_task to invoke entry points from the
# luxury_lakehouse Python package, ensuring consistent dependency management.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_job" "data_ingestion" {
  name = "soccer-analytics-ingestion-${var.environment}"

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
    timeout_seconds = 3600
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_statsbomb"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    # Use serverless compute for the task
    environment_key = "default"
  }

  # ── Task: Ingest Metrica tracking data ───────────────────────────────────
  task {
    task_key        = "ingest_metrica"
    timeout_seconds = 1800
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
    timeout_seconds = 1800
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
    timeout_seconds = 3600
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
    timeout_seconds = 1800
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

  # ── Task: Compute Off-Ball xT from tracking data ───────────────────
  # Depends on all three tracking providers completing first.
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
        "multimethod==1.12"
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
