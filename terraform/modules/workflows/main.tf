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
#   skillcorner       — SkillCorner tracking via pining API: public A-League broadcast (10fps, CSV/JSONL)
#                       + private RM full-format (parquet/gzip); serialization auto-detected per match
#   backfill_statsbomb_extra — Backfill _raw_extra_json for GK sub-types (depends on statsbomb)
#   compute_spadl_vaep — SPADL conversion + VAEP scoring (depends on backfill_extra + wyscout)
#   compute_xg_shot_scores — canonical-SPADL pre-shot xG v3 scoring → bronze.xg_shot_predictions (ADR-066)
#   compute_off_ball_xt — Off-Ball xT from tracking + pitch control (depends on tracking tasks)
#   compute_pitch_control — Spearman 2017 pitch control values (depends on tracking tasks)
#   compute_defcon_lite — DEFCON-lite defensive valuation (depends on SPADL/VAEP)
#   compute_elastic_sync — ELASTIC event-tracking alignment (depends on idsse_events)
#   compute_pausa     — PAUSA pass timing pipeline (depends on elastic_sync + OBSO import)
#   resolve_players   — Cross-source entity resolution (depends on statsbomb + wyscout)
#   compute_embeddings_v2 — Transformer (192d) player embeddings with adversarial debiasing (depends on entity resolution)
#   compute_formations_efpi — EFPI template-matching formation detection (depends on pitch control)
#   compute_formations_shape_graph — Shape graph geometric formation detection (depends on EFPI)
#   compute_action_context — Unified action context enrichment (depends on SPADL + all providers)
#   run_model_validation — Model drift detection (depends on compute_pausa)
#   backfill_statsbomb_360 — Catchup 360 freeze frames for already-ingested matches (depends on statsbomb)
#   hf_sync — Combined HF Hub imports + exports (depends on gate + compute tasks)
#
# HF Hub tasks use the "hf" environment (huggingface_hub + wheel).
# Write tasks require HF_TOKEN from Databricks secret scope "hf", key "token".
# SkillCorner ingestion requires pining-for-the-data API token in scope "pining", key "token".
#
# Secret scope setup (one-time per workspace):
#   databricks secrets create-scope pining
#   databricks secrets put-secret --scope pining --key token
#   databricks secrets put-acl --scope pining --principal <sp-application-id> --permission READ
#   databricks secrets create-scope hf
#   databricks secrets put-secret --scope hf --key token
#   databricks secrets put-acl --scope hf --principal <sp-application-id> --permission READ
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

  # ── Job-level parameters ────────────────────────────────────────────────
  # Accessible in task configs as {{job.parameters.<name>}}.
  # Override at run-now time via job_parameters JSON.
  parameter {
    name    = "dbt_full_refresh"
    default = "false"
  }

  # AC-1 ad-hoc scoping for preflight_action_context. Empty defaults => the daily
  # scheduled run discovers ALL providers with NO cap (unchanged). Override at
  # run-now via job_parameters, e.g. {"provider":"wyscout","max_units":"5"} picks
  # the next 5 unprocessed wyscout units only; {"max_units":"1"} picks <=1 unit
  # per provider. A "unit" is a match (most providers) or a (match,period) half (IDSSE).
  parameter {
    name    = "provider"
    default = ""
  }
  parameter {
    name    = "max_units"
    default = ""
  }
  # Run-scoped cap on SkillCorner ingestion (phased private-match rollout, e.g. the RM-5
  # probe). Empty => ingest all missing/modified matches (the scheduled-run default);
  # override per run via job_parameters, e.g. {"max_matches":"5"} => ingest_skillcorner --max-matches 5.
  parameter {
    name    = "max_matches"
    default = ""
  }

  # AC-1 ghost-GK backend selection (ADR-035 amendment). The job-parameter default IS the
  # per-installation knob: serverless cannot take per-task env vars via TF, so the installation default
  # flows var.ghost_gk_backend_default => {{job.parameters.ghost_gk_backend}} => preflight --ghost-gk-backend.
  # Override per run via job_parameters, e.g. {"ghost_gk_backend":"cpu-numba","watchdog_budget_s":"5400"}.
  parameter {
    name    = "ghost_gk_backend"
    default = var.ghost_gk_backend_default
  }
  parameter {
    name    = "watchdog_budget_s"
    default = var.watchdog_budget_s
  }
  # Run-scoped frame-batch-size override for action-context (ADR-047 amendment 2).
  # Empty => per-provider defaults in analytics.action_context.batching
  # (250 dense 25fps providers, 2500 where prod-proven). Set for memory-envelope
  # A/Bs, e.g. {"frame_batch_size":"1000"}.
  parameter {
    name    = "frame_batch_size"
    default = ""
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
    max_retries     = 1 # external API — transient failures benefit from retry

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
    timeout_seconds = 2400
    max_retries     = 1 # external API — transient failures benefit from retry

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

  # ── Task: Compute unified action context features (AC-1 fan-out) ────────
  # All silly-kicks enrichments for SPADL actions in a single pass per
  # match. Event-only providers get game_state + GK resolution (~5 cols);
  # tracking providers get the full enrichment chain (~102 cols).
  # Writes bronze.spadl_action_context.
  #
  # AC-1 fan-out (ADR-037, worker-drain): N persistent drain workers, each draining
  # its slice of observability.action_context_work_queue (filled by preflight). The
  # for-each input is the CONSTANT worker-id list (not per-game), so the task value
  # never approaches the 48 KB cap regardless of game count. concurrency MUST equal
  # _ActionContextGuard._N_DRAIN_WORKERS (pinned by test_terraform_concurrency_matches_n_workers).
  task {
    task_key = "compute_action_context"

    depends_on {
      task_key = "preflight_action_context"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_action_context.values.action_context_worker_ids}}"
      concurrency = 8 # == _N_DRAIN_WORKERS

      task {
        task_key = "compute_action_context_iteration"
        # 8 h: a worker drains its slice to COMPLETION. The 1800 s per-game budget is now
        # a watchdog INSIDE the worker (ADR-037), not the iteration timeout. One-time cold
        # start ~5.5 h on the slowest worker; daily runs are tiny. Documented exception to
        # the "compute task <= 2 hr" budget.
        timeout_seconds = 28800
        # Go omitempty zero-value bug: max_retries=0 is silently dropped by
        # the TF provider. scripts/patch_job_retries.py enforces this
        # post-apply via the REST API.
        max_retries = 0

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "compute_action_context_drain_worker"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--worker-id", "{{input}}",
            "--run-id", "{{tasks.preflight_action_context.values.action_context_run_id}}",
            # Per-game watchdog override (ADR-037 amendment); empty => in-code WATCHDOG_BUDGET_S=2700.
            "--watchdog-budget-s", "{{job.parameters.watchdog_budget_s}}",
            # Frame-batch-size override (ADR-047 amendment 2); empty => per-provider defaults.
            "--frame-batch-size", "{{job.parameters.frame_batch_size}}",
          ]
        }

        environment_key = "analytics"
      }
    }
  }

  # ── Task: Compute action context for StatsBomb (sb360) ─────────────────
  # ADR-058: statsbomb sb360 EXITS the per-match drain and runs as ONE distributed
  # cogroup.applyInPandas job (scan each bronze table once, enrich per match on executors,
  # distributed write). Reads bronze.spadl_actions (compute_spadl_vaep), bronze.statsbomb_360
  # (backfill_statsbomb_360), and bronze.expected_threat_grids (compute_expected_threat). The 8 h
  # timeout matches the drain's documented exception (first full backlog run); daily runs are tiny.
  task {
    task_key        = "compute_action_context_statsbomb"
    timeout_seconds = 28800
    max_retries     = 0

    depends_on {
      task_key = "backfill_statsbomb_360"
    }
    depends_on {
      task_key = "compute_expected_threat"
    }
    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_action_context_statsbomb"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        # Optional cap for scoped runs; empty => all pending sb360 matches (parsed in main_statsbomb).
        "--max-units", "{{job.parameters.max_units}}",
        # D9 (Task 6): sb360 is NEVER enqueued, so its OWN unit events are the only evidence the D8
        # gate has about statsbomb -- and they are only evidence if they carry the run it verifies.
        #
        # {{job.run_id}}, NOT the preflight task value the DRAIN workers use: this task does NOT
        # depend on preflight_action_context, and a Databricks task value resolves only from an
        # UPSTREAM task. It is the identical value -- {{job.run_id}} is exactly what preflight itself
        # is passed (see its task below), and _resolve_run_id returns it verbatim. It is also the
        # more robust of the two: the preflight task value is "" on a nothing-to-do run, which would
        # leave sb360 unable to file its events at all.
        "--run-id", "{{job.run_id}}",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Compute DEFCON-lite defensive valuation ──────────────────────
  # Reads gold fct_action_values + bronze statsbomb_360, assigns defensive
  # credits per-defender per-action, trains XGBoost value estimators.
  task {
    task_key        = "compute_defcon_lite"
    timeout_seconds = 1500
    max_retries     = 0

    # PR-Cycle-B (2026-05-01): defcon_lite_360 reads bronze.statsbomb_360
    # written by backfill_statsbomb_360. Without this edge today's compute
    # silently runs against yesterday's 360 frames (1-day lag class).
    depends_on {
      task_key = "backfill_statsbomb_360"
    }
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
    timeout_seconds = 600
    max_retries     = 0

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
  # Football2vec 360: imports pre-trained 208d 360-enriched embeddings from
  # HF Hub, writes to bronze.player_embeddings_raw with
  # data_source='football2vec_360'. Depends on compute_embeddings_v2 running
  # first (shared HF Hub auth + stat vector cache path).
  task {
    task_key        = "compute_embeddings_360"
    timeout_seconds = 600
    max_retries     = 0

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

  # ── Task: Compute player embeddings v2 (transformer + adversarial) ───
  # Football2vec v2: imports pre-trained 128d transformer embeddings from
  # HF Hub, writes to bronze.player_embeddings_raw with model_version='v2'.
  task {
    task_key        = "compute_embeddings_v2"
    timeout_seconds = 600
    max_retries     = 0

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_action_values
    # (intermediate_mart) — wait on stage 2. The legacy resolve_players
    # edge was a misclassification — v2 inference reads HF-published
    # weights + fct_action_values gold, NOT bronze.player_xref_raw.
    depends_on {
      task_key = "dbt_build_intermediate_marts"
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
    timeout_seconds = 600
    max_retries     = 0

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
    timeout_seconds = 900
    max_retries     = 0

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_tracking_frames
    # (input_mart). The legacy compute_pitch_control edge was a peer
    # serialization remnant — formations_efpi does NOT read pitch_control
    # bronze (would be in test_workflow_dag_bronze_reads if it did).
    depends_on {
      task_key = "dbt_build_input_marts"
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
    timeout_seconds = 900
    max_retries     = 0

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): consumes the EFPI temp table
    # written by compute_formations_efpi (kept) AND reads gold.fct_tracking_frames
    # input_mart (new edge to stage 1). Order: alphabetical.
    depends_on {
      task_key = "compute_formations_efpi"
    }
    depends_on {
      task_key = "dbt_build_input_marts"
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
  # Path C: IDSSE tracking + events for Bundesliga line-breaking.
  # Writes bronze.line_breaking_results.
  task {
    task_key        = "compute_line_breaking"
    timeout_seconds = 600
    max_retries     = 0

    # PR-Cycle-B (2026-05-01): conformance test caught 5 missing edges
    # for line-breaking's 4-path detector (caught beyond the original audit):
    # - Path A reads bronze.statsbomb_360 → backfill_statsbomb_360
    # - Path B reads bronze.metrica_events → ingest_metrica (kept)
    # - Path C reads bronze.idsse_events + bronze.idsse_tracking → ingest_idsse + ingest_idsse_events
    # - Plus bronze.statsbomb_events for guard's source-filter scope → ingest_statsbomb (kept)
    # Without these, today's compute silently runs against yesterday's bronze
    # for the missing source(s) (1-day lag class).
    # PR-Cycle-C PR-β (2026-05-02, ADR-019): added dbt_build_input_marts so
    # gold-side reads of fct_tracking_frames pick up today's input_mart.
    # Bronze-side ingest edges retained per test_workflow_dag_bronze_reads.
    # Order: alphabetical (test_workflows_tf_ordering enforcement).
    depends_on {
      task_key = "backfill_statsbomb_360"
    }
    depends_on {
      task_key = "dbt_build_input_marts"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
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
    timeout_seconds = 600
    max_retries     = 0

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_tracking_frames
    # (input_mart) + bronze xT grids from compute_expected_threat. Drops the
    # legacy ingest_* edges (pre-three-stage gold-reader workarounds).
    # Order: alphabetical (test_workflows_tf_ordering enforcement).
    depends_on {
      task_key = "compute_expected_threat"
    }
    depends_on {
      task_key = "dbt_build_input_marts"
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
  # values (imported from HF Jobs GPU run via the standalone
  # import_obso_results task — split out of hf_sync in PR-Cycle-B).
  task {
    task_key        = "compute_pausa"
    timeout_seconds = 600
    max_retries     = 0

    depends_on {
      task_key = "compute_elastic_sync"
    }
    # PR-Cycle-B (2026-05-01): PAUSA reads bronze.pausa_raw_scores written
    # by import_obso_results. Pre-split, hf_sync wrapped this import as a
    # sub-op and ran in PARALLEL with compute_pausa — so PAUSA silently ran
    # on yesterday's pausa_raw_scores. C-6 split makes the dep explicit.
    # Evidence: src/ingestion/pausa.py:69,180 +
    # src/ingestion/import_obso_results.py:141-142.
    depends_on {
      task_key = "import_obso_results"
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
    timeout_seconds = 1200
    max_retries     = 0

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads gold.fct_tracking_frames
    # (input_mart) — wait on stage 1 to ensure today's tracking frames are
    # built before pitch-control compute. Drops the legacy ingest_* edges
    # which were pre-three-stage gold-reader workarounds.
    depends_on {
      task_key = "dbt_build_input_marts"
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

  # ── Task: Compute pre-shot tracking freeze frames ──────────────────────
  # Canonical-SPADL pre-shot xG (Task 0.5): converts each tracking shot's linked frames to
  # canonical home-LTR AC frames (reusing analytics.action_context._convert_tracking_batch) and
  # writes the per-(shot, player) set to bronze.shot_freeze_frames (replaceWhere per match_key).
  # Reads bronze.spadl_actions (compute_spadl_vaep), the raw tracking bronze (tracking ingests),
  # and gold dim_matches (dbt_build_input_marts — the match_key surrogate; ADR-013). Incremental
  # by default; a --match-ids "provider:id,.." backfill overrides discovery.
  #
  # INTERIM SCOPE — GradientSports + SkillCorner only (--providers below): the current driver-side
  # per-(match,period) conversion risks OOM on IDSSE's ~1.5M-row periods and lacks M13 boundary
  # de-dup. IDSSE/Metrica onboard with a distributed SINK rewrite (per-(match,period) work-queue in
  # action_context_queue + a compute_shot_freeze_frames_drain_worker entry point, mirroring
  # compute_action_context) that moves conversion onto executors + amortizes cold-start. See the
  # module docstring of src/ingestion/shot_freeze_frames.py + wf-shot-freeze-frames.yaml.
  task {
    task_key        = "compute_shot_freeze_frames"
    timeout_seconds = 3600
    max_retries     = 0

    depends_on {
      task_key = "compute_spadl_vaep"
    }
    depends_on {
      task_key = "dbt_build_input_marts"
    }
    depends_on {
      task_key = "ingest_gradientsports"
    }
    depends_on {
      task_key = "ingest_idsse"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_shot_freeze_frames"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        # INTERIM SCOPE (self-documenting; matches the driver default): GS + SkillCorner only.
        # idsse/metrica await the distributed-sink rewrite — see the task comment above.
        "--providers", "gradientsports,skillcorner",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Compute SPADL actions and VAEP scores (for_each_task fan-out) ──
  # Each iteration processes one chunk: converts a single provider's matches
  # to SPADL (scoped to chunk match_ids only), then scores with VAEP.
  # Idempotent via replaceWhere.
  task {
    task_key        = "compute_spadl_vaep"
    timeout_seconds = 0

    depends_on {
      task_key = "preflight_spadl_vaep"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_spadl_vaep.values.spadl_vaep_chunks}}"
      concurrency = 4

      task {
        task_key        = "compute_spadl_vaep_iteration"
        timeout_seconds = 1800
        max_retries     = 0

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "compute_spadl_vaep"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-ids", "{{input}}"
          ]
        }

        environment_key = "analytics"
      }
    }
  }

  # ── Task: Score shots with the pre-shot xG v3 model (canonical SPADL, two-mode gate) ──
  # Canonical-SPADL Pre-Shot xG Unification (Task 1.9, §C1): loads xg_model_v3@Champion
  # (raw xG) + the shipped per-provider OOF calibrators, scores every shot-family row in
  # gold.fct_action_values (freeze frames from bronze.shot_freeze_frames on
  # (match_key, action_id); empty -> zero-context), applies the single per-provider
  # calibrator (pooled fallback -> ood_flag) under the two-mode gate, and writes
  # bronze.xg_shot_predictions (replaceWhere per match_key). Reads fct_action_values
  # (intermediate_mart, built by stage 2) + bronze.shot_freeze_frames.
  task {
    task_key        = "compute_xg_shot_scores"
    timeout_seconds = 900
    max_retries     = 0

    depends_on {
      task_key = "compute_shot_freeze_frames"
    }
    depends_on {
      task_key = "dbt_build_intermediate_marts"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_xg_shot_scores"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: dbt build — stage 1 (input + dimension marts) ──────────────
  # PR-Cycle-C PR-β (2026-05-02, ADR-019): first of three sequential dbt
  # invocations. Builds dimensions + marts whose lineage has NO compute
  # task in it (`fct_tracking_frames`, `fct_shots`, `fct_discipline_events`)
  # plus their staging-view ancestors and seeds. Compute tasks reading
  # these gold marts (pitch_control / off_ball_xt / xg_model[_v2] /
  # formations_efpi / formations_shape_graph / line_breaking) wait on
  # this stage. Selector: --select +tag:input_mart +tag:dimension.
  # See src/ingestion/dbt_runner.py — selector arg differentiates from stages 2/3.
  task {
    task_key        = "dbt_build_input_marts"
    timeout_seconds = 3600
    max_retries     = 0

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
      parameters   = ["--select", "+tag:input_mart", "+tag:dimension", "--dbt-full-refresh", "{{job.parameters.dbt_full_refresh}}"]
    }

    # All ingest tasks + ingest-helper compute tasks (extract_tracking_metadata,
    # backfills, resolve_players) per ADR-019 § "Treatment of ingest-helper
    # compute tasks". Order: alphabetical.
    depends_on { task_key = "backfill_statsbomb_360" }
    depends_on { task_key = "backfill_statsbomb_extra" }
    depends_on { task_key = "extract_tracking_metadata" }
    depends_on { task_key = "ingest_gradientsports" }
    depends_on { task_key = "ingest_idsse" }
    depends_on { task_key = "ingest_idsse_events" }
    depends_on { task_key = "ingest_metrica" }
    depends_on { task_key = "ingest_skillcorner" }
    depends_on { task_key = "ingest_statsbomb" }
    depends_on { task_key = "ingest_wyscout" }
    depends_on { task_key = "resolve_players" }

    environment_key = "dbt"
  }

  # ── Task: dbt build — stage 2 (intermediate marts) ───────────────────
  # PR-Cycle-C PR-β (2026-05-02, ADR-019): second of three sequential dbt
  # invocations. Builds intermediate_marts (marts with compute output in
  # their lineage AND consumed by at least one compute task) — currently
  # `fct_action_values` only. `+tag:intermediate_mart` includes ancestors
  # so any staging views unique to intermediate marts are built here.
  # Selector: --select +tag:intermediate_mart.
  task {
    task_key        = "dbt_build_intermediate_marts"
    timeout_seconds = 3600
    max_retries     = 0

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
      parameters   = ["--select", "+tag:intermediate_mart", "--dbt-full-refresh", "{{job.parameters.dbt_full_refresh}}"]
    }

    # Sequential edge to stage 1 + the phase-1 compute task that writes
    # bronze.{spadl_actions, vaep_action_values} consumed by fct_action_values.
    # Order: alphabetical.
    depends_on { task_key = "compute_spadl_vaep" }
    depends_on { task_key = "dbt_build_input_marts" }

    environment_key = "dbt"
  }

  # ── Task: dbt build — stage 3 (output marts) ─────────────────────────
  # PR-Cycle-C PR-β (2026-05-02, ADR-019): third of three sequential dbt
  # invocations. Builds output_marts — every mart that is NOT consumed
  # by a compute task (consumed only by apps/dashboards/HF/run_model_validation).
  # Selector: --select +tag:output_mart path:models/staging path:models/intermediate
  #           --exclude +tag:input_mart +tag:dimension +tag:intermediate_mart.
  # Stage 3 runs LAST (after all ingest + compute), so every bronze source is
  # available — it builds output marts + ALL staging + ALL intermediate models,
  # MINUS everything stages 1+2 already built (those marts + their ancestors).
  # This makes union(stage1, stage2, stage3) == every model BY CONSTRUCTION.
  # ADR-019's original `tag:output_mart` (no `+`, no staging/intermediate paths)
  # wrongly assumed stages 1+2 built ALL staging. They did not: staging VIEWS
  # that feed ONLY output marts (e.g. stg_action_context__values) or that have
  # NO dbt consumer but are read externally (e.g. stg_pitch_control__values via
  # the HF publisher / Taipy app) were selected by NO stage → never rebuilt →
  # went schema-stale (a view's columns are frozen at creation). That broke
  # fct_action_context full-refresh after the PR-#337 GK-zone columns landed.
  # Full coverage is enforced by test_dbt_stage_selector_coverage. ADR-019 (amended).
  #
  # refresh_synced_tables and run_model_validation are SIBLINGS depending
  # on this stage — both children of `dbt_build_output_marts`. A
  # validation regression cannot transitively block today's synced-table
  # refresh, preserving ADR-017's "signal not gate" intent via topology
  # rather than via stale reads (supplants ADR-017's pre-three-stage
  # yesterday-gold workaround).
  task {
    task_key        = "dbt_build_output_marts"
    timeout_seconds = 3600
    max_retries     = 0

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "dbt_build"
      # --vars xg_v3_enabled=true promotes the gated pre-shot xG v3 mart
      # (fct_shot_xg + stg_xg__shot_predictions) into the daily build — ADR-066's
      # "flip in the job config". Appended after --select so dbt_runner's selector
      # capture still resolves the wf-dbt-build-output-marts card.
      parameters = ["--select", "+tag:output_mart", "path:models/staging", "path:models/intermediate", "--exclude", "+tag:input_mart", "+tag:dimension", "+tag:intermediate_mart", "--dbt-full-refresh", "{{job.parameters.dbt_full_refresh}}", "--vars", "{xg_v3_enabled: true}"]
    }

    # Stage 2 sequential edge + every phase-2 compute task that writes
    # bronze read by an output_mart. (compute_spadl_vaep is in stage 2;
    # extract_tracking_metadata + backfills + resolve_players are in
    # stage 1.) Order: alphabetical.
    depends_on { task_key = "compute_action_context" }
    depends_on { task_key = "compute_action_context_statsbomb" }
    depends_on { task_key = "compute_defcon_lite" }
    depends_on { task_key = "compute_elastic_sync" }
    depends_on { task_key = "compute_embeddings_360" }
    depends_on { task_key = "compute_embeddings_v2" }
    depends_on { task_key = "compute_expected_threat" }
    depends_on { task_key = "compute_formations_efpi" }
    depends_on { task_key = "compute_formations_shape_graph" }
    depends_on { task_key = "compute_line_breaking" }
    depends_on { task_key = "compute_off_ball_xt" }
    depends_on { task_key = "compute_pausa" }
    depends_on { task_key = "compute_pitch_control" }
    # fct_shot_xg reads bronze.xg_shot_predictions from compute_xg_shot_scores
    # (ADR-066; enforced by test_workflow_dag_bronze_reads).
    depends_on { task_key = "compute_xg_shot_scores" }
    depends_on { task_key = "dbt_build_intermediate_marts" }
    depends_on { task_key = "hf_sync" }

    environment_key = "dbt"
  }

  # ── Task: Extract tracking player metadata ─────────────────────────────
  # Reads IDSSE DFL match info XMLs to populate tracking_player_metadata
  # bronze table with player/team names.
  task {
    task_key        = "extract_tracking_metadata"
    timeout_seconds = 900
    max_retries     = 0

    depends_on {
      task_key = "ingest_idsse"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "extract_tracking_metadata"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: HF Hub sync — combined imports + exports ───────────────────
  task {
    task_key        = "hf_sync"
    timeout_seconds = 600
    max_retries     = 1 # HF Hub network calls — transient failures benefit from retry

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
      task_key = "resolve_players"
    }

    environment_key = "hf"
  }

  # ── Task: Import OBSO/PAUSA results from HF Hub ────────────────────────
  # PR-Cycle-B (2026-05-01) — split out of hf_sync to make compute_pausa's
  # dependency on this import explicit. Pre-split, hf_sync invoked
  # import_obso_results as one of 7 sub-operations; compute_pausa ran in
  # parallel with hf_sync, so PAUSA silently ran on yesterday's
  # bronze.pausa_raw_scores when hf_sync took longer than the
  # elastic_sync→pausa chain. Now: standalone task with no upstream deps
  # (pure HF Hub download), and compute_pausa explicitly waits on it.
  task {
    task_key        = "import_obso_results"
    timeout_seconds = 600
    max_retries     = 1 # HF Hub download — transient failures benefit from retry

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "import_obso_results"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "hf"
  }

  # ── Task: Ingest Gradient Sports data (for_each_task fan-out) ───────────
  # One iteration per match. Each iteration receives a JSON-serialized
  # MatchInfo via {{input}} and ingests that single match (events + tracking).
  # Parquet staging bypasses the 256 MB Spark Connect RPC limit.
  #
  # Downstream tasks reference this task as `ingest_gradientsports` (the
  # parent); Databricks resolves dependencies against the for_each_task
  # parent rather than individual iterations.
  task {
    task_key = "ingest_gradientsports"

    depends_on {
      task_key = "preflight_gradientsports"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_gradientsports.values.gradientsports_matches}}"
      concurrency = 8

      task {
        task_key        = "ingest_gradientsports_iteration"
        timeout_seconds = 900
        max_retries     = 1 # API calls — transient failures benefit from retry

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "ingest_gradientsports"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-json", "{{input}}",
          ]
        }

        environment_key = "default"
      }
    }
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
        max_retries     = 1 # UC Volume I/O — transient failures benefit from retry

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
    timeout_seconds = 600
    max_retries     = 1 # UC Volume I/O — transient failures benefit from retry

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
    timeout_seconds = 600
    max_retries     = 1 # external download — transient failures benefit from retry

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
  # Requires secret scope "pining", key "token" (pining-for-the-data API bearer token).
  # Token source: pining-for-the-data repo terraform/environments/dev/terraform.tfvars
  task {
    task_key        = "ingest_skillcorner"
    timeout_seconds = 1200
    max_retries     = 1 # external API — transient failures benefit from retry

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_skillcorner"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        # Phased-rollout cap (RM-5 probe); empty => all missing/modified. See job parameter `max_matches`.
        "--max-matches", "{{job.parameters.max_matches}}"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Ingest StatsBomb data ──────────────────────────────────────────
  task {
    task_key        = "ingest_statsbomb"
    timeout_seconds = 1200
    max_retries     = 1 # external API — transient failures benefit from retry

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
    timeout_seconds = 1200
    max_retries     = 1 # UC Volume I/O — transient failures benefit from retry

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

  # ── Task: Action-context preflight — discover units + fill work-queue ──
  # AC-1 (ADR-037): discovers unprocessed units across all 6 providers via skip_guard,
  # LPT-bin-packs them into observability.action_context_work_queue, and writes the
  # constant worker-id list + run_id as task values. xT grid is NOT loaded here — each
  # drain worker reads it from bronze.expected_threat_grids (~192 rows) at startup.
  #
  # environment_key = "default": main_preflight only imports numpy + the
  # wheel; no silly-kicks / xgboost / mlflow / scipy needed. The task timeout was
  # raised 300 -> 600 s (2026-06-03): cold-start env setup alone (the analytics env's
  # 11-dep pip resolution on a cache-cold build) was busting the 300 s budget before
  # the preflight finished enqueuing (observed TIMEDOUT on warm-vs-cold runs).
  #
  # The downstream `compute_action_context` for_each_task consumes the constant
  # worker-id list + run_id task values this preflight writes (ADR-037 worker-drain).
  task {
    task_key        = "preflight_action_context"
    timeout_seconds = 600
    max_retries     = 0

    # Needs SPADL + all provider data ingested before checking freshness.
    # Order: alphabetical (test_workflows_tf_ordering enforcement).
    depends_on { task_key = "backfill_statsbomb_360" }
    depends_on { task_key = "compute_spadl_vaep" }
    depends_on { task_key = "ingest_gradientsports" }
    depends_on { task_key = "ingest_idsse" }
    depends_on { task_key = "ingest_idsse_events" }
    depends_on { task_key = "ingest_metrica" }
    depends_on { task_key = "ingest_skillcorner" }
    depends_on { task_key = "ingest_statsbomb" }
    depends_on { task_key = "ingest_wyscout" }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_action_context"

      # provider/max_units pass through from job parameters (empty => all/no-cap;
      # main_preflight coerces "" -> None). Lets a run-now scope the fan-out to
      # e.g. the next 5 wyscout units without touching the daily schedule.
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        "--provider", "{{job.parameters.provider}}",
        "--max-units", "{{job.parameters.max_units}}",
        # Ghost-GK backend (ADR-035 amendment): default = var.ghost_gk_backend_default (installation knob);
        # the preflight resolves + stamps it onto every WorkUnit, so the drain workers read it off the queue.
        "--ghost-gk-backend", "{{job.parameters.ghost_gk_backend}}",
        # Job-level run id (identical across all tasks in the run) -> written to the
        # action_context_run_id task value -> read by each drain worker (ADR-037 B1).
        "--run-id", "{{job.run_id}}",
      ]
    }

    environment_key = "default"
  }

  # ── Task: Gradient Sports preflight (guard + match discovery) ───────────
  # Runs the skip guard, discovers matches via the pining-for-the-data API,
  # and emits each match as a JSON-serialized MatchInfo string in the task
  # value array. Downstream for_each_task consumes via {{input}}.
  #
  # Behavior:
  #   - 64 matches → 64-element JSON array → 64 iterations (concurrency=8)
  #   - 0 matches  → [] → 0 iterations spawned
  task {
    task_key        = "preflight_gradientsports"
    timeout_seconds = 600
    max_retries     = 0

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_gradientsports"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
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
    timeout_seconds = 600
    max_retries     = 0

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

  # ── Task: Preflight SPADL/VAEP (guard + chunk emission + model cache) ────
  # Discovers new matches per provider, caches VAEP models to UC Volume,
  # emits chunk strings as task values for downstream for_each_task.
  task {
    task_key        = "preflight_spadl_vaep"
    timeout_seconds = 600
    max_retries     = 0

    # Same dependencies as the old monolithic compute_spadl_vaep.
    # Order: alphabetical.
    depends_on {
      task_key = "backfill_statsbomb_extra"
    }
    depends_on {
      task_key = "ingest_gradientsports"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }
    depends_on {
      task_key = "ingest_wyscout"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_spadl_vaep"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "analytics"
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
    max_retries     = 0

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "refresh_synced_tables"
      parameters   = ["--wait"]
    }

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): waits on stage 3 (the final dbt
    # invocation). Pre-PR-β this depended on the single `dbt_build` task;
    # post-PR-β stage 3 is the equivalent terminal mart-build step.
    depends_on {
      task_key = "dbt_build_output_marts"
    }

    # Control-plane task: calls ws.postgres.* (databricks-sdk PostgresAPI) to resolve + poll
    # synced-table pipelines. Needs the "lakebase" env (wheel + databricks-sdk) — the bare
    # "default" env's runtime-bundled SDK lacks .postgres. Guarded by
    # test_refresh_synced_tables_env_ships_databricks_sdk.
    environment_key = "lakebase"
  }

  # ── Task: Resolve cross-source player identity ───────────────────────────
  # Matches StatsBomb and Wyscout players via TF-IDF + rapidfuzz.
  # Writes player_xref_raw bronze table for dbt int_player_xref → dim_players.
  task {
    task_key        = "resolve_players"
    timeout_seconds = 1200
    max_retries     = 0

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
    timeout_seconds = 600
    max_retries     = 0

    # PR-Cycle-C PR-β (2026-05-02, ADR-019): reads today's output_marts
    # (fct_xg_predictions_v2, fct_pausa_values, etc.). Sibling of
    # refresh_synced_tables (both children of dbt_build_output_marts) —
    # validation regression CANNOT block today's mart refresh, preserving
    # ADR-017's "signal not gate" intent via topology rather than via
    # stale reads. Drops the legacy compute_pausa edge (subsumed by
    # transitive path through dbt_build_output_marts).
    depends_on {
      task_key = "dbt_build_output_marts"
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

  # ── Task: Derived-artifact staleness monitor (ADR-063 H4) ──────────────
  # Detect-and-alert backstop: ERROR-logs any workflow whose recorded watermark
  # lags its upstream's current data version (the stale-grid class that produced
  # the negative xt_gk_dzv). Read-only; runs late (post output-marts) so today's
  # watermarks are current. Sibling of run_model_validation — a staleness alert
  # must never block the daily job (signal, not gate).
  task {
    task_key        = "run_staleness_monitor"
    timeout_seconds = 600
    max_retries     = 0

    depends_on {
      task_key = "dbt_build_output_marts"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "run_staleness_monitor"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: D8 — action-context drain completeness gate ──────────────────
  # The FAN-IN over BOTH action-context producers: the 8-way `compute_action_context`
  # drain for_each AND `compute_action_context_statsbomb` (sb360 exits the per-match
  # drain — ADR-058 — so a queue-only gate would leave statsbomb completely unchecked).
  # Reads the work queue + the D9 unit-event log + the results mart and asserts that
  # every expected unit actually ran and its rows actually landed
  # (`skillcorner:1552423:2` wrote 0 of its 550 actions while the job reported SUCCESS).
  task {
    task_key        = "verify_action_context_drain"
    timeout_seconds = 3600
    max_retries     = 0

    # run_if = ALL_DONE: the gate must run even when the drain FAILED. Otherwise a dead
    # worker SKIPS the gate and D9's whole OOM-visibility payoff (`running` written BEFORE
    # processing) is delivered to nobody. Only INCOMPLETE (and a planner alarm) fails the
    # task; DRAIN_FAILED / UNVERIFIABLE are REPORTS — the job already failed, and the gate's
    # job there is to say WHAT DIED, not to mask the drain's real exception with its own.
    run_if = "ALL_DONE"

    depends_on { task_key = "compute_action_context" }
    depends_on { task_key = "compute_action_context_statsbomb" }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "verify_action_context_drain"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
        # {{job.run_id}} — NOT {{tasks.preflight_action_context.values.action_context_run_id}}.
        # The preflight task value is "" on a nothing-to-do run, while sb360 (which does not
        # depend on preflight, so the task value is not even resolvable there) files its events
        # under the JOB run id. A gate wired to the task value would audit run "", find no sb360
        # slice_completed, and report DRAIN_FAILED EVERY QUIET DAY. It is the identical value:
        # preflight is itself passed "{{job.run_id}}" and returns it verbatim.
        "--run-id", "{{job.run_id}}",
      ]
    }

    environment_key = "analytics"
  }

  # ── Environment for SPADL/VAEP task (includes analytics extras) ─────────
  # No statsbombpy needed — pipeline reads from bronze, not the API.
  #
  # EXACT PINS (==), not floors: serverless rebuilds the env on every wheel bump and
  # re-resolves floors against PyPI — prod silently ran THREE different silly-kicks
  # versions in one day (4.21.0 → 4.21.1 → 4.21.2, none of them the lock-tested 4.20.1).
  # Pins mirror uv.lock (enforced by test_tf_exact_pins_match_uv_lock); bumping a
  # version here REQUIRES bumping pyproject + uv.lock first, so prod only ever runs
  # what local tests + goldens validated. Do NOT "clean up" pins that look redundant
  # with the serverless base image: env-version-1 ships numpy 1.23.5 / scipy 1.10.0 /
  # matplotlib 3.7.0 — dropping a pin DOWNGRADES prod to those.
  environment {
    environment_key = "analytics"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "silly-kicks[das,ghost-gk,parse-dfl]==4.43.0",
        "accessible-space==2.0.15",
        # numba: silly-kicks ships @njit kernels for pitch control + ball-carrier
        # (tracking/pitch_control/_{spearman,fernandez_bornn}.py, tracking/_ball_carrier.py)
        # behind a `try: import numba / except: _HAS_NUMBA=False` fallback. numba is
        # only an OPTIONAL silly-kicks extra ([numba]), so without it pinned here the
        # serverless tracking enrichment silently runs the slow numpy fallback — a
        # latent footgun (correct output, no error, ~2x slower hot loops). Pinning it
        # makes the deployed env match the code's design contract. (DAS/accessible-space
        # is pure numpy — numba does not accelerate it; see project memory
        # ac1-numba-das-cost.)
        "numba==0.66.0",
        "numpy==1.26.4",
        # xgboost-cpu: same version + API as xgboost, but without the
        # nvidia-nccl-cu12 core dep (300 MB GPU lib, useless on CPU-only
        # serverless). Cuts cold-start env build by ~60-90 s.
        "xgboost-cpu==3.2.0",
        "rapidfuzz==3.14.5",
        "unidecode==1.4.0",
        "sparse-dot-topn==1.2.0",
        # mlflow-skinny: client-only mlflow (pyfunc + tracking). The analytics
        # path only loads models + uses MlflowClient; the server-side bundle
        # (flask/gunicorn/sqlalchemy/alembic/docker/...) is unused. ~30-50 MB
        # + ~11 fewer transitives off cold-start.
        "mlflow-skinny==3.13.0",
        "mplsoccer==1.6.1",
        "matplotlib==3.10.9",
        "scipy==1.15.3"
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
        "dbt-core==1.11.6",
        "dbt-databricks==1.11.6",
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
          "gensim==4.4.0",
          "huggingface_hub==1.6.0",
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
        "huggingface_hub==1.6.0"
      ]
    }
  }

  # ── Environment for Lakebase control-plane tasks (databricks-sdk PostgresAPI) ──
  # refresh_synced_tables resolves + polls synced-table pipelines via
  # ws.postgres.get_synced_table (databricks-sdk >= the line that added PostgresAPI).
  # The serverless runtime bundles an older SDK without .postgres, so the SDK must be
  # pinned here explicitly. Kept separate from "default" so the ~15 default tasks (preflight
  # / ingestion) stay minimal. Pin matches pyproject's [sdk] / [taipy-app] extras (==0.114.0);
  # version overlap enforced by test_terraform_env_specs_align_with_pyproject.
  environment {
    environment_key = "lakebase"

    spec {
      client = "1"

      dependencies = [
        var.wheel_path,
        "databricks-sdk==0.121.0",
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
        # TF-only dep (not in pyproject/uv.lock — ingestion API client only used on
        # serverless). Pinned to what the old >=1.13.0 floor resolved to on 2026-06-10.
        "statsbombpy==1.19.0"
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
