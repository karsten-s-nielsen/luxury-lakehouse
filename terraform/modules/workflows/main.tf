# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Data Ingestion Pipeline
# ──────────────────────────────────────────────────────────────────────────────
# Creates a Databricks job that ingests data from three soccer data providers
# in parallel:
#
#   statsbomb  — Free open-data events (shots, passes, lineups)
#   metrica    — Tracking data (player coordinates at 25fps)
#   wyscout    — Match events and player attributes
#
# Schedule: Daily at 06:00 UTC (before business hours in US/EU timezones)
#
# Each task uses python_wheel_task to invoke entry points from the
# luxury_lakehouse Python package, ensuring consistent dependency management.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_job" "data_ingestion" {
  name = "soccer-analytics-ingestion-${var.environment}"

  # ── Schedule: Daily at 6am UTC ───────────────────────────────────────────
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "UTC"
    pause_status           = var.environment == "dev" ? "PAUSED" : "UNPAUSED"
  }

  # ── Task: Ingest StatsBomb data ──────────────────────────────────────────
  task {
    task_key = "ingest_statsbomb"

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
    task_key = "ingest_metrica"

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
    task_key = "ingest_wyscout"

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

  # ── Job-level settings ───────────────────────────────────────────────────

  # Email notifications (configure when ready)
  # notification_settings {
  #   no_alert_for_skipped_runs = true
  # }

  tags = {
    project     = "luxury-lakehouse"
    environment = var.environment
    managed_by  = "terraform"
    pipeline    = "ingestion"
  }
}
