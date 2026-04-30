# ──────────────────────────────────────────────────────────────────────────────
# Module: Workflows — Data Ingestion Pipeline
# ──────────────────────────────────────────────────────────────────────────────
# Creates a Databricks job that ingests data from five soccer data providers
# in parallel, then runs compute pipelines. Each task runs its own skip guard
# at startup and raises WorkflowSkippedError when there is no new work.
#
#   statsbomb         — Free open-data events (shots, passes, lineups)
#   metrica           — Tracking data (player coordinates at 25fps)
#   wyscout           — Match events and player attributes
#   idsse             — Bundesliga DFL tracking (25fps, 7 matches from UC Volume)
#   idsse_events      — Bundesliga DFL event XML (7 matches, depends on idsse)
#   skillcorner       — A-League broadcast tracking (10fps, 10 matches via kloppy)
#   backfill_statsbomb_extra — Backfill _raw_extra_json for GK sub-types (depends on statsbomb)
#   compute_spadl_vaep — SPADL conversion + VAEP scoring (depends on backfill_extra + wyscout)
#   compute_xg_model   — Custom xG model scoring (depends on SPADL/VAEP)
#   compute_off_ball_xt — Off-Ball xT from tracking + pitch control (depends on tracking tasks)
#   compute_pitch_control — Spearman 2017 pitch control values (depends on tracking tasks)
#   compute_defcon_lite — DEFCON-lite defensive valuation (depends on SPADL/VAEP)
#   compute_elastic_sync — ELASTIC event-tracking alignment (depends on idsse_events)
#   compute_pausa     — PAUSA pass timing pipeline (depends on elastic_sync + OBSO import)
#   resolve_players   — Cross-source entity resolution (depends on statsbomb + wyscout)
#   compute_embeddings_v2 — Transformer (128d) player embeddings with adversarial debiasing (depends on entity resolution)
#   compute_embeddings_v1 — Doc2Vec (gensim) player embeddings, deprecated (depends on compute_embeddings_v2)
#   compute_formations_efpi — EFPI template-matching formation detection (depends on pitch control)
#   compute_formations_shape_graph — Shape graph geometric formation detection (depends on EFPI)
#   run_model_validation — Model drift detection (depends on compute_pausa)
#   backfill_statsbomb_360 — Catchup 360 freeze frames for already-ingested matches (depends on statsbomb)
#   hf_sync — Combined HF Hub imports + exports (depends on gate + compute tasks)
#
# HF Hub tasks use the "hf" environment (huggingface_hub + wheel).
# Write tasks require HF_TOKEN from Databricks secret scope "hf", key "token".
# Setup: databricks secrets put-secret --scope hf --key token
#
# Schedule: Daily at 06:00 UTC (before business hours in US/EU timezones)
#
# Each task uses python_wheel_task to invoke entry points from the
# luxury_lakehouse Python package, ensuring consistent dependency management.
# ──────────────────────────────────────────────────────────────────────────────

resource "databricks_job" "data_ingestion" {
  name                = "soccer-analytics-ingestion-${var.environment}"
  max_concurrent_runs = 1
  performance_target  = "PERFORMANCE_OPTIMIZED"

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

  # ── Task: Backfill StatsBomb 360 freeze-frame data ──────────────────────
  # The main ingestion skips already-ingested competition/seasons, so 360
  # data added after the initial ingest is never fetched. This backfill
  # targets matches that have events but no 360 data yet.
  task {
    task_key        = "backfill_statsbomb_360"
    timeout_seconds = 1800
    max_retries     = 1

    depends_on {
      task_key = "ingest_statsbomb"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "backfill_statsbomb_360"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    # Needs statsbombpy to call sb.frames()
    environment_key = "statsbomb"
  }

  # ── Task: Backfill StatsBomb _raw_extra_json ─────────────────────────────
  # Ensures the goalkeeper sub-dict (and other type-specific extras) are
  # present in statsbomb_events._raw_extra_json. Without this, the SPADL
  # converter cannot distinguish keeper_claim/keeper_punch/keeper_save sub-types.
  # Idempotent: only processes matches where _raw_extra_json IS NULL or '{}'.
  task {
    task_key        = "backfill_statsbomb_extra"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "ingest_statsbomb"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "backfill_statsbomb_extra"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "statsbomb"
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

  # ── Task: Compute player embeddings 360-enriched (Deep Sets + transformer) ───
  # Football2vec 360: imports pre-trained 144d 360-enriched embeddings from
  # HF Hub, writes to bronze.player_embeddings_raw with
  # data_source='football2vec_360'. Depends on compute_embeddings_v2 running
  # first (shared HF Hub auth + stat vector cache path).
  task {
    task_key        = "compute_embeddings_360"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_embeddings_v2"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings_360"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
  }

  # ── Task: Compute player embeddings v1 (Doc2Vec, deprecated) ────────
  # Football2vec v1: Doc2Vec action sequences + statistical z-score vectors.
  # Retained for comparison; superseded by v2 transformer embeddings.
  task {
    task_key        = "compute_embeddings_v1"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_embeddings_v2"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings_v1"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
  }

  # ── Task: Compute player embeddings v2 (transformer + adversarial) ───
  # Football2vec v2: imports pre-trained 128d transformer embeddings from
  # HF Hub, writes to bronze.player_embeddings_raw with model_version='v2'.
  task {
    task_key        = "compute_embeddings_v2"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "resolve_players"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings_v2"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
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

  # ── Task: Detect team formations via EFPI template matching ──────────
  # Reads fct_tracking_frames, runs EFPI template matching (Bekkers &
  # Dabadghao 2025). Writes formation_labels (detector='efpi') and
  # EFPI temp table consumed by shape graph detector.
  task {
    task_key        = "compute_formations_efpi"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_pitch_control"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_formations_efpi"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Detect formations via shape graph geometric detector ──────
  # Reads fct_tracking_frames, runs Sotudeh (2026) Delaunay-based shape
  # graph detector. Writes formation_labels (detector='shape_graph'),
  # player_positions, and position_maps.
  task {
    task_key        = "compute_formations_shape_graph"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_formations_efpi"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_formations_shape_graph"

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
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_statsbomb"
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

  # ── Task: Compute Off-Ball xT from tracking data ───────────────────
  # Depends on all three tracking providers + xT grid computation.
  task {
    task_key        = "compute_off_ball_xt"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_expected_threat"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_metrica"
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

  # ── Task: Compute pitch control values for tracking data ───────────────
  # Reads gold fct_tracking_frames, computes Spearman 2017 pitch control
  # at each player's position, writes bronze.pitch_control_values.
  task {
    task_key        = "compute_pitch_control"
    timeout_seconds = 7200
    max_retries     = 1

    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_metrica"
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

  # ── Task: Compute SPADL actions and VAEP scores ─────────────────────────
  task {
    task_key        = "compute_spadl_vaep"
    timeout_seconds = 7200
    max_retries     = 1

    depends_on {
      task_key = "backfill_statsbomb_extra"
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

  # ── Task: dbt build (gold layer materialization) ─────────────────────
  # D59 (2026-04-13): runs `dbt build` against the SQL warehouse to materialize
  # the 36 gold mart tables from bronze sources. Bundled dbt_project/ ships in
  # the wheel via Hatch force-include; auth uses dbt-databricks 1.10+ runtime
  # OAuth M2M identity discovery. See src/ingestion/dbt_runner.py.
  task {
    task_key        = "dbt_build"
    timeout_seconds = 3600

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
    }

    # Same 9 leaf compute tasks that refresh_synced_tables previously depended on.
    # NOTE: run_model_validation is intentionally NOT a dependency. Validation
    # reads from gold marts (which dbt_build PRODUCES), so any "validation
    # before mart refresh" gating is semantically reading yesterday's data.
    # Keeping it independent ensures a single validator regression cannot
    # block today's mart refresh + Lakebase synced-table propagation. See
    # docs/superpowers/adrs/ADR-017-model-validation-as-signal-not-gate.md.
    depends_on { task_key = "compute_defcon_lite" }
    depends_on { task_key = "compute_embeddings_v1" }
    depends_on { task_key = "compute_formations_shape_graph" }
    depends_on { task_key = "compute_line_breaking" }
    depends_on { task_key = "compute_off_ball_xt" }
    depends_on { task_key = "compute_xg_model_v2" }
    depends_on { task_key = "extract_tracking_metadata" }
    depends_on { task_key = "hf_sync" }

    environment_key = "dbt"
  }

  # ── Task: Extract tracking player metadata ─────────────────────────────
  # Reads IDSSE DFL match info XMLs and SkillCorner kloppy metadata to
  # populate tracking_player_metadata bronze table with player/team names.
  task {
    task_key        = "extract_tracking_metadata"
    timeout_seconds = 900
    max_retries     = 1

    depends_on {
      task_key = "ingest_idsse"
    }

    depends_on {
      task_key = "ingest_skillcorner"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "extract_tracking_metadata"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "tracking"
  }

  # ── Task: HF Hub sync — combined imports + exports ───────────────────
  task {
    task_key        = "hf_sync"
    timeout_seconds = 1800

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "hf_sync"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    # Depends on all compute tasks that produce data for exports
    depends_on {
      task_key = "backfill_statsbomb_360"
    }
    depends_on {
      task_key = "compute_elastic_sync"
    }
    depends_on {
      task_key = "compute_spadl_vaep"
    }
    depends_on {
      task_key = "compute_xg_model"
    }
    depends_on {
      task_key = "resolve_players"
    }

    environment_key = "hf"
  }

  # ── Task: Ingest IDSSE Bundesliga tracking data (for_each_task fan-out) ──
  # PR-Cycle-A (2026-04-30): Runtime-discovered fan-out. The chunk array
  # comes from `preflight_idsse` via task-value substitution; each chunk
  # is a comma-separated match-ID list (e.g. "J03WMX,J03WN1"), forwarded
  # to `--match-ids` of the iteration's `ingest_idsse` entry point.
  #
  # Behavior:
  #   - All 7 missing → 4 chunks → 4 parallel iterations → ~13 min wall-clock
  #   - Partial (e.g. 3 missing) → 2 chunks → 2 iterations
  #   - No missing → 0 iterations spawned (preflight emitted [])
  #
  # Downstream tasks reference this task as `ingest_idsse` (the parent);
  # Databricks resolves dependencies against the for_each_task parent
  # rather than individual iterations.
  task {
    task_key = "ingest_idsse"

    depends_on {
      task_key = "preflight_idsse"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_idsse.values.idsse_match_chunks}}"
      concurrency = 4

      task {
        task_key        = "ingest_idsse_iteration"
        timeout_seconds = 900
        max_retries     = 1

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "ingest_idsse"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-ids", "{{input}}",
          ]
        }

        environment_key = "default"
      }
    }
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

  # ── Task: IDSSE preflight — discover unprocessed matches + emit chunks ────
  # PR-Cycle-A (2026-04-30): Runtime chunk discovery for the for_each_task
  # fan-out. Anti-joins IDSSE_MATCH_IDS against bronze.idsse_tracking ∩
  # bronze.idsse_events, partitions missing matches into chunks of size 2
  # (per `_IdsseGuard.chunk_size` in src/ingestion/idsse.py), and writes
  # the chunks as a Databricks task value `idsse_match_chunks`.
  #
  # The downstream `ingest_idsse` for_each_task consumes the task value
  # via `{{tasks.preflight_idsse.values.idsse_match_chunks}}` — no
  # hardcoded chunks, no Terraform changes when adding/removing matches.
  task {
    task_key        = "preflight_idsse"
    timeout_seconds = 300
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_idsse"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "default"
  }

  # ── Task: Refresh Lakebase synced tables (final stage) ───────────────
  # SNAPSHOT-mode synced tables do not auto-refresh. This task closes the
  # propagation loop after dbt_build completes by refreshing all 37 synced
  # tables, ensuring both gold and observability data reach Lakebase.
  # D59 (2026-04-13): now depends solely on dbt_build (which itself depends
  # on the 9 leaf compute tasks). Previous 9-way fan-in collapsed to 1 edge.
  # See `src/ingestion/refresh_synced_tables.py`.
  task {
    task_key        = "refresh_synced_tables"
    timeout_seconds = 2400 # 30 min refresh window + overhead

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "refresh_synced_tables"
      parameters   = ["--wait"]
    }

    depends_on {
      task_key = "dbt_build"
    }

    environment_key = "default"
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

  # ── Environment for SPADL/VAEP task (includes analytics extras) ─────────
  # No statsbombpy needed — pipeline reads from bronze, not the API.
  environment {
    environment_key = "analytics"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "silly-kicks>=1.0.0,<2.0",
        "numpy<2.0",
        "xgboost==3.2.0",
        "rapidfuzz>=3.6.0",
        "unidecode>=1.3.0",
        "sparse-dot-topn>=1.1.0",
        "mlflow>=2.17.0",
        "mplsoccer>=1.1.3",
        "matplotlib>=3.8.0",
        "scipy>=1.11.0"
      ]
    }
  }

  # ── Environment for dbt build task (D59) ──────────────────────────────
  # dbt-databricks 1.10+ supports runtime OAuth M2M identity discovery via
  # the databricks-sdk WorkspaceClient. No client_id/secret env vars needed —
  # the daily job's run_as SP identity is auto-detected inside the runtime.
  environment {
    environment_key = "dbt"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "dbt-core>=1.10.0",
        "dbt-databricks>=1.10.0",
      ]
    }
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

  # ── Environment for HF Hub tasks (huggingface_hub + wheel) ────────────
  # Write tasks call dbutils.secrets.get(scope="hf", key="token") at runtime.
  # Read-only tasks (imports from public repos) need no token.
  # Setup: databricks secrets put-secret --scope hf --key token
  environment {
    environment_key = "hf"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "huggingface_hub>=0.25.0"
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
