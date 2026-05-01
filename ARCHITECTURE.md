# Databricks Lakebase Architecture — Soccer Analytics Platform

> **Status**: SEC1 cycle (EU AI Act gap analysis) — 16 Taipy pages, 40 synced tables, 71 PG indexes (65 btree + 6 HNSW at 192d/144d/13d). Hugging Face Hub: 17 models + 19 datasets published, GPU training on HF Jobs L40S. Regulation (EU) 2024/1689 gap analysis in [`AI_GOVERNANCE.md`](AI_GOVERNANCE.md) covering 13 per-player evaluative ML systems; every model card carries an "EU AI Act — Intended Use and Non-Use" stanza, enforced by `src/tests/test_ai_governance_md.py`. Daily Job Hardening (D59/D56/SEC2): self-healing daily job with dbt_build python_wheel_task + SHA-256 artifact integrity verification on model loads. PSxG model (Brier 0.129). ScoutGPT decoder + training pipeline (D32). Guard-as-wrapper: 33 skip guards with mandatory `FilterResult` injection. `fct_workflow_costs` enriched with warm-tier lifecycle data (D51). HF-app SP codified in Terraform with UC grants (TF-SP). M2 OAuth infrastructure complete. Mart classification taxonomy (PR-Cycle-C, ADR-019): every gold mart tagged `dimension`/`input_mart`/`intermediate_mart`/`output_mart` for the upcoming three-stage `dbt_build` (PR-α metadata; PR-β TF restructure).
> **Last Updated**: 2026-04-18
> **Repository**: [`karsten-s-nielsen/luxury-lakehouse`](https://github.com/karsten-s-nielsen/luxury-lakehouse)
> **Approach**: Professional-grade IaC, best practices, production-ready

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Target Architecture](#2-target-architecture)
3. [C4 Architecture Model](#3-c4-architecture-model)
4. [Technology Decisions](#4-technology-decisions)
5. [Repository Structure](#5-repository-structure)
6. [Cross-Cutting Concerns](#6-cross-cutting-concerns)
7. [Risk Register](#7-risk-register)
8. [Appendices](#8-appendices)

---

## 1. Executive Summary

A serverless soccer analytics platform built on the Databricks Lakebase architecture. The pipeline ingests open-source match data (StatsBomb, Metrica Sports, Wyscout, IDSSE, SkillCorner), transforms it through a medallion architecture (Bronze → Silver → Gold), synchronizes curated tables into Lakebase (PostgreSQL 17), and serves a Taipy dashboard for coaches and analysts.

**Why Lakebase over Traditional AWS?**

| Concern | Traditional AWS | Lakebase |
|---------|----------------|----------|
| Services to manage | 6 (EC2, S3, Glue, Redshift, RDS, MWAA) | 2 (Databricks Workspace, Lakebase) |
| Data movement hops | 5 (ingest → S3 → Glue → Redshift → S3 → RDS) | 2 (ingest → Delta Lake → Synced Table) |
| Reverse ETL complexity | High (UNLOAD → S3 → s3_import) | Zero (Synced Tables auto-sync) |
| Idle cost | High (Redshift cluster, RDS always-on) | Near-zero (scale-to-zero compute) |
| Credential management | Manual (connection strings, secrets) | Automatic (OAuth M2M, service principals) |
| Database branching | Not available | Native copy-on-write clones |
| Vector/AI readiness | Bolt-on (SageMaker) | Native pgvector |

---

## 2. Target Architecture

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES (Open Source)                               │
│  ┌───────────┐  ┌────────────────┐   ┌─────────┐  ┌───────────┐  ┌──────────────┐     │
│  │ StatsBomb │  │ Metrica Sports │   │ Wyscout │  │   IDSSE   │  │ SkillCorner  │     │
│  │ (JSON API)│  │ (CSV tracking) │   │ (JSON)  │  │   (XML)   │  │   (JSONL)    │     │
│  └─────┬─────┘  └───────┬────────┘   └────┬────┘  └─────┬─────┘  └──────┬───────┘     │
└────────┼────────────────┼─────────────────┼─────────────┼───────────────┼─────────────┘
         │                │                 │             │               │
         ▼                ▼                 ▼             ▼               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              DATABRICKS SERVERLESS WORKFLOWS                             │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Python ingestion tasks on Serverless Compute (scheduled)        │    │
│  │  • statsbombpy → competitions, matches, events, lineups, 360     │    │
│  │  • requests → Metrica sample-data CSV + EPTS                     │    │
│  │  • requests → Wyscout public JSON datasets                       │    │
│  │  • xml.etree → IDSSE DFL position XML from UC Volume             │    │
│  │  • kloppy → SkillCorner broadcast tracking from open data        │    │
│  │  • spadl_vaep → SPADL conversion + VAEP scoring from bronze      │    │
│  │  • defcon_lite → DEFCON-lite defensive credit assignment         │    │
│  │  • resolve_players → cross-source entity resolution              │    │
│  │  • compute_embeddings_v2 → Import 128-d transformer embeddings    │    │
│  │  • compute_embeddings_v1 → Doc2Vec + z-score (deprecated)        │    │
│  │  • export_embeddings_training_data → Transformer training data   │    │
│  │  • compute_formations_efpi → EFPI template-matching detection    │    │
│  │  • compute_formations_shape_graph → Shape graph detection        │    │
│  │  • compute_xg_model → xG v1 scoring (logistic + XGBoost)         │    │
│  │  • compute_xg_model_v2 → xG v2 scoring (Deep Sets + MC dropout) │    │
│  │  • compute_expected_threat → Data-driven xT grid from SPADL      │    │
│  │  • elastic_sync → ELASTIC event-tracking alignment (Kim 2025)    │    │
│  │  • compute_pausa → PAUSA pass timing (Lee et al. 2026)          │    │
│  │  • model_validation → Drift detection (PSI/Wasserstein/CUSUM)   │    │
│  │  • compute_pitch_control → Batch pitch control (applyInPandas)  │    │
│  │  • compute_off_ball_xt → Off-ball xT (pitch control × xT grid)  │    │
│  │  • compute_line_breaking → Line-breaking pass detection (batch)  │    │
│  │  • import_obso_results → OBSO values from HF Hub to bronze      │    │
│  │  • import_psxg_predictions → PSxG predictions from HF Hub       │    │
│  │  • import_space_creation → Space creation values from HF Hub    │    │
│  │  • extract_tracking_metadata → Tracking data metadata extraction│    │
│  │  • guards → 33 skip guards (each pipeline runs its own at startup)│    │
│  │  • evolve → Level 2 code evolution (ScoutGPT decoder, D32)      │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG — BRONZE LAYER                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Delta Lake tables (raw, append-only, schema-on-read)            │    │
│  │  • statsbomb: competitions, matches, events, lineups, 360        │    │
│  │  • metrica: tracking, events                                     │    │
│  │  • wyscout: events, matches, players                             │    │
│  │  • idsse: tracking (7 Bundesliga matches, 25fps)                 │    │
│  │  • skillcorner: tracking (10 A-League matches, 10fps)            │    │
│  │  • spadl: actions, action_values                                 │    │
│  │  • entity_resolution: player_xref_raw                            │    │
│  │  • pitch_control_batch: pitch_control_values                     │    │
│  │  • idsse: events (DFL event XML)                                │    │
│  │  • elastic_sync: elastic_sync_results                           │    │
│  │  • obso: obso_surfaces, pausa_raw_scores                       │    │
│  │  • model_validation: model_validation_runs                      │    │
│  │  • xg_predictions (v1), xg_predictions_v2, expected_threat_grids  │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          DATABRICKS SERVERLESS SQL + dbt                                 │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  dbt-databricks models on Serverless SQL Warehouse               │    │
│  │                                                                  │    │
│  │  SILVER (cleaned, typed, deduplicated):                          │    │
│  │  • stg_statsbomb__events, shots, matches, lineups, 360           │    │
│  │  • stg_metrica__tracking, events                                 │    │
│  │  • stg_wyscout__events, stg_wyscout__players                     │    │
│  │  • stg_idsse__tracking, stg_skillcorner__tracking                │    │
│  │  • stg_spadl__action_values, stg_pitch_control__values           │    │
│  │  • stg_idsse__events, stg_idsse__elastic_sync                  │    │
│  │  • stg_pausa__values                                           │    │
│  │                                                                  │    │
│  │  GOLD (business logic, analytics-ready — 36 fact + 4 dim):       │    │
│  │  Tagged per ADR-019 (PR-Cycle-C). Authoritative list:            │    │
│  │  `dbt_project/models/marts/{dim_*,fct_*}.sql` + `tags=[...]`.    │    │
│  │  • dimension (4): dim_competitions, dim_matches, dim_players,    │    │
│  │    dim_teams                                                     │    │
│  │  • input_mart (3): fct_tracking_frames, fct_shots,               │    │
│  │    fct_discipline_events                                         │    │
│  │  • intermediate_mart (1): fct_action_values                      │    │
│  │  • output_mart (32): fct_passes, fct_match_summary,              │    │
│  │    fct_physical_stats, fct_player_stats, fct_player_percentiles, │    │
│  │    fct_xg_predictions, fct_xg_predictions_v2, fct_off_ball_xt,   │    │
│  │    fct_formation_labels, fct_player_positions, fct_position_maps,│    │
│  │    fct_player_embeddings(_career/_season/_career_360/_season_360)│    │
│  │    fct_line_breaking_results, fct_pausa_values, fct_pausa_rankings│   │
│  │    fct_pass_timing, fct_defcon_actions, fct_defcon_pressure,     │    │
│  │    fct_defensive_values, fct_goalkeeper_stats,                   │    │
│  │    fct_funnel_stages_agg, fct_heatmap_agg, fct_vaep_breakdown_agg,│   │
│  │    fct_gk_actions_detail, fct_space_creation,                    │    │
│  │    fct_tracking_avg_positions, fct_tracking_shape_timeline,      │    │
│  │    fct_workflow_costs                                            │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              SYNCED TABLES — ZERO-ETL                                    │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Lakeflow Declarative Pipelines                                  │    │
│  │  Gold Delta tables → continuous async sync → Lakebase            │    │
│  │  (read-only PostgreSQL-queryable mirrors, sub-10ms latency)      │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          LAKEBASE AUTOSCALING (PostgreSQL 17)                            │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Serverless OLTP • Scale-to-zero • OAuth M2M auth                │    │
│  │  • Standard PostgreSQL wire protocol (JDBC/psycopg2)             │    │
│  │  • Native pgvector with HNSW indexes for player similarity       │    │
│  │  • Copy-on-write database branching for dev/test                 │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌───────────────────────────────────────────────────────────────────────────┐
│          TAIPY APPLICATION                                                │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  Deployed on Hugging Face Spaces (Docker SDK)                      │    │
│  │  • PAT auth (OAuth M2M ready — PG role created, secret swap pending)│    │
│  │  • Connects to Lakebase via psycopg2 (ThreadedConnectionPool)     │    │
│  │  • 16 pages: Shot Map, Pass Map, Heat Map, Pass Network,          │    │
│  │    Match Summary, Player Comparison, Player Impact,               │    │
│  │    Player Similarity, Goalkeeper Analytics, Movement & Pressing,  │    │
│  │    Pass Timing, Pitch Control, Tactical Positions, Team Shape,    │    │
│  │    Defensive Impact, AI/ML Workflows                              │    │
│  └───────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────────┘
```

### Synced Tables (Gold → Lakebase)

| Source Table | Synced Table | Primary Key | Rows |
|-------------|-------------|-------------|------|
| `dev_gold.fct_shots` | `fct_shots_synced` | `shot_id` | 131,077 |
| `dev_gold.fct_xg_predictions` | `fct_xg_predictions_synced` | `shot_id` | 87,999 |
| `dev_gold.fct_passes` | `fct_passes_synced` | `pass_id` | 5,052,415 |
| `dev_gold.fct_player_stats` | `fct_player_stats_synced` | `player_stats_id` | 19,154 |
| `dev_gold.fct_match_summary` | `fct_match_summary_synced` | `match_id` | 3,464 |
| `dev_gold.fct_tracking_frames` | `fct_tracking_frames_synced` | `tracking_id` | 38,118,607 |
| `dev_gold.fct_action_values` | `fct_action_values_synced` | `action_value_id` | ~9,500,000 |
| `dev_gold.fct_discipline_events` | `fct_discipline_events_synced` | `event_id` | ~14,000 |
| `dev_gold.fct_physical_stats` | `fct_physical_stats_synced` | `physical_stats_id` | 616 |
| `dev_gold.fct_defensive_values` | `fct_defensive_values_synced` | `defensive_value_id` | 829,377 |
| `dev_gold.fct_defcon_actions` | `fct_defcon_actions_synced` | `defcon_action_id` | 829,377 |
| `dev_gold.fct_defcon_pressure` | `fct_defcon_pressure_synced` | `pressure_id` | ~28,000 |
| `dev_gold.fct_player_embeddings` | `fct_player_embeddings_synced` | `embedding_id` | ~87,035 |
| `dev_gold.fct_player_embeddings_season` | `fct_player_embeddings_season_synced` | `embedding_season_id` | ~8,950 |
| `dev_gold.fct_player_embeddings_career` | `fct_player_embeddings_career_synced` | `embedding_career_id` | ~8,950 |
| `dev_gold.dim_players` | `dim_players_synced` | `canonical_player_id` | 11,918 |
| `dev_gold.fct_pausa_values` | `fct_pausa_values_synced` | `pass_id` | ~3,500 |
| `dev_gold.fct_pass_timing` | `fct_pass_timing_synced` | `(player_id, match_id)` | ~600 |
| `dev_gold.dim_teams` | `dim_teams_synced` | `team_id` | 453 |
| `dev_gold.dim_competitions` | `dim_competitions_synced` | `competition_id` | 21 |
| `dev_gold.fct_player_percentiles` | `fct_player_percentiles_synced` | `percentile_id` | ~19,000 |
| `dev_gold.fct_pausa_rankings` | `fct_pausa_rankings_synced` | `ranking_id` | ~600 |
| `dev_gold.fct_workflow_costs` | `fct_workflow_costs_synced` | `cost_id` | ~100 |
| `dev_gold.fct_formation_labels` | `fct_formation_labels_synced` | `formation_label_id` | ~2,000 |
| `dev_gold.fct_tracking_avg_positions` | `fct_tracking_avg_positions_synced` | `avg_position_id` | ~4,000 |
| `dev_gold.fct_tracking_shape_timeline` | `fct_tracking_shape_timeline_synced` | `shape_timeline_id` | ~50,000 |
| `dev_gold.fct_player_positions` | `fct_player_positions_synced` | `position_id` | ~8,500 |
| `dev_gold.fct_position_maps` | `fct_position_maps_synced` | `position_map_id` | ~600 |
| `dev_gold.fct_line_breaking_results` | `fct_line_breaking_results_synced` | `line_breaking_id` | ~5,000,000 |
| `dev_gold.fct_goalkeeper_stats` | `fct_goalkeeper_stats_synced` | `goalkeeper_stats_id` | ~600 |
| `dev_gold.fct_off_ball_xt` | `fct_off_ball_xt_synced` | `off_ball_xt_id` | ~100,000 |
| `dev_gold.fct_space_creation` | `fct_space_creation_synced` | `space_creation_id` | ~3,500 |
| `dev_gold.fct_player_embeddings_career_360` | `fct_player_embeddings_career_360_synced` | `embedding_career_360_id` | ~8,950 |
| `dev_gold.fct_player_embeddings_season_360` | `fct_player_embeddings_season_360_synced` | `embedding_season_360_id` | ~8,950 |
| `observability.workflow_cost_live` | `workflow_cost_live_synced` | `cost_live_id` | ~500 |
| `observability.workflow_import_checksums` | — (not synced) | `workflow_id` | ~5 |

**Implementation notes:**

- `scheduling_policy = "SNAPSHOT"` — initial sync with on-demand refresh
- `logical_database_name = "databricks_postgres"` — standard Lakebase database
- **Autoscaling workaround (provider v1.110.0):** `databricks_database_synced_database_table` only supports `database_instance_name` (Provisioned). Synced tables targeting Autoscaling projects must be created via Databricks UI, then imported into Terraform. `lifecycle { ignore_changes = all }` prevents drift. This applies to any new synced table.
- **Schema changes:** Must delete synced table, drop ghost PG table, recreate via API, re-import into Terraform.
- **PG indexes:** 61 btree indexes across 27 tables + 6 HNSW vector indexes on embedding tables (128-dim/144-dim) = 67 total. Dropped on synced table recreation — re-run `scripts/create_indexes.py` alongside `scripts/lakebase_grants.sql`. Script now runs `ANALYZE` on all indexed tables to ensure the query planner uses indexes.
- **SP refresh permissions:** The Lakebase database project + each backing pipeline must grant `CAN_USE` (project) + `CAN_RUN` (pipeline) to both the `hf_app_v2` SP (Taipy admin endpoint) and the `ingestion` SP (daily Databricks job's refresh task). Without these grants, calls to `GET /api/2.0/database/synced_tables/{name}` return 403. Apply via `scripts/grant_synced_table_permissions.py` (idempotent, integrated into `scripts/maintain_synced_tables.py` as Step 0). Re-run after any synced table recreation since pipeline_ids may change. Hard-verified empirically in dev: 70 grants total (2 project + 68 pipeline) → 34/34 staging refresh subprocess success.
- **SNAPSHOT refresh:** Synced tables with `scheduling_policy = "SNAPSHOT"` do not auto-refresh. Run `python -m ingestion.refresh_synced_tables` (or the `refresh_synced_tables` console-script entry point) after upstream dbt rebuilds. Supports `--wait` (poll until IDLE) and `--tables` (comma-separated subset). Use `scripts/dbt_build_and_refresh.py` to chain `dbt build` + refresh atomically. The daily Databricks job auto-runs `refresh_synced_tables` as a final task. The Terraform provider has no schedule/cron field — this is the operational workaround.
- **Credential API:** REST endpoint is `/api/2.0/postgres/credentials` (NOT `/api/2.0/database/credentials`).

### Taipy Application Pages

| Page | Visualization | Data Source |
|------|--------------|-------------|
| Heat Map | Action density per player/team/match | `fct_passes_synced`, `fct_shots_synced` |
| Match Summary | Dashboard: Final/xG/Verdict tiles + Big Story VAEP cards + Plotly xG race + ranked delta table | `fct_match_summary_synced`, `fct_action_values_synced`, `fct_shots_synced`, `fct_discipline_events_synced` |
| Pass Map | Full pitch arrows, progressive pass highlighting | `fct_passes_synced` |
| Pass Network | Interactive Plotly graph with hover tooltips | `fct_passes_synced` |
| Shot Map | Half-pitch shots sized by xG, colored by outcome, custom xG overlay | `fct_shots_synced`, `fct_xg_predictions_synced` |
| Player Comparison | Per-90 metrics radar (1-3 players), incl. VAEP/90, percentile ranks | `fct_player_stats_synced`, `fct_player_percentiles_synced` |
| Player Impact (VAEP) | VAEP rankings, action type breakdown, timeline | `fct_action_values_synced`, `fct_player_stats_synced` |
| Player Similarity | pgvector nearest-neighbor search ("Find players like X"), radar overlay | `fct_player_embeddings_career_synced`, `fct_player_embeddings_season_synced`, `fct_player_stats_synced` |
| Movement & Pressing | Physical performance, PPDA pressing, off-ball xT | `fct_physical_stats_synced`, `fct_match_summary_synced` |
| Pass Timing | PAUSA scores, OBSO heatmap, temporal/spatial scatter | `fct_pausa_values_synced`, `fct_pass_timing_synced` |
| Pitch Control | Physics (Spearman 2017) + Voronoi toggle from tracking data | `fct_tracking_frames_synced` |
| Team Shape | Convex hull, centroid, formation lines, 6 spatial metrics, snapshot + timeline | `fct_tracking_avg_positions_synced`, `fct_tracking_shape_timeline_synced`, `fct_tracking_frames_synced` |
| Goalkeeper Analytics | PSxG, distribution xT, collection, sweeper metrics, shot-stopping | `fct_goalkeeper_stats_synced`, `fct_shots_synced` |
| Tactical Positions | Average positions, position maps, role assignments from tracking | `fct_player_positions_synced`, `fct_position_maps_synced`, `fct_tracking_avg_positions_synced` |
| Defensive Impact | DEFCON-lite attacker pressure rankings, breakdown, match timeline | `fct_defcon_pressure_synced`, `fct_defcon_actions_synced` |
| AI/ML Workflows | DAG visualization, cost tracking, workflow cards, run status | `fct_workflow_costs_synced` |

**Admin endpoints** (authenticated, not user-facing): `POST /api/cache/clear` (with optional `?refresh_synced=1`) is mounted on the Taipy app's Flask layer (`hf_taipy_app/src/admin_api.py`, injected via `Gui(flask=...)`). Caller must present a HuggingFace user access token in `Authorization: Bearer hf_xxx`; the endpoint validates against `https://huggingface.co/api/whoami-v2` and requires membership in the `luxury-lakehouse` org with role `admin` or `write`. The optional `?refresh_synced=1` query param spawns a background subprocess that runs `python -m ingestion.refresh_synced_tables --wait` (isolated process — no in-process state mutation). Used for forced cache invalidation during incident response and manual synced-table refresh from outside the daily Databricks job.

### Skip Guards (Guard-as-Wrapper)

Each pipeline runs its own skip guard at startup via `skip_guard.check()`, raising `WorkflowSkippedError` when there is no new data. The 33 registered `SkipGuard` adapters live in `src/ingestion/guards.py`. This replaced the centralized freshness gate (D52) — each pipeline is self-contained, removing the 170s serial bottleneck that existed when all guards ran sequentially in a single task.

**Architecture:**
- **Guard registry** (`_GUARD_MODULES`): Maps workflow IDs to guard modules. Each guard's `check()` returns a `FilterResult(workflow_id, count, chunks, metadata)`.
- **`find_new_ids()`**: Spark LEFT ANTI JOIN with `cast("string")` normalization — pushes set difference to executors instead of collecting all IDs to the driver.
- **`check_hf_dataset_freshness()`**: SHA-based skip guard for HF Hub import pipelines. Fetches the current HF Hub commit SHA via `HfApi.repo_info()`, compares against the stored SHA in `observability.workflow_import_checksums` (Delta, MERGE upsert). Skips import when SHAs match; fails open on network errors. Used by 5 guards: `wf-import-obso`, `wf-import-psxg`, `wf-import-space-creation`, `wf-football2vec-v2`, `wf-football2vec-360`.
- **Mandatory `FilterResult` injection**: `run_pipeline()` receives `FilterResult` as a **required** parameter (no default). Each pipeline's `main()` calls its guard via `timed_check(skip_guard, spark, catalog, schema)` — a wrapper in `ingestion/guards.py` that records guard wall-clock duration via `time.monotonic()` and returns a `FilterResult` with `guard_duration_seconds` populated. Inline `find_new_ids()` calls are prohibited outside guard classes, enforced by `TestNoInlineGuardInPipeline`.
- **Three-way cost decomposition**: `CostEstimateHook` writes `entity_count` (input entities from guard), `row_count` (output rows), and `guard_duration_seconds` (guard wall-clock from `timed_check`) to `workflow_cost_live` in the observability schema. `fct_workflow_costs` uses `tasks` (lakeflow) as the driving table with LEFT JOIN on billing — timing data is available immediately, billing cost arrives with ~1 day lag. `effective_cost_usd = COALESCE(attributed_cost_usd, estimated_cost_usd)` ensures the UI always has a cost value. The cold tier exposes `cold_start_seconds` (total pre-pipeline time = env init + guard) and `guard_duration_seconds` (guard only); UI derives `environment_setup = cold_start - guard`. Warm-tier join uses `workflow_id` + temporal window — `job_run_id` and `task_key` are not in the warm tier (serverless exposes neither via Spark conf, and the columns were dropped after the seed mapping fix made them obsolete).
- **Conformance tests**: `test_guard_conformance.py` auto-discovers all guards from the registry and validates architectural invariants: import isolation, mandatory parameters, no inline guards, direct guard call (no gate indirection), early exit structure, and early exit behavior. `test_pipeline_row_count.py` enforces all `run_pipeline()` functions return `int` (row count) and verifies all Terraform task_keys have entries in `task_workflow_mapping.csv` (seed coverage).
- **Production behavior**: Each pipeline starts, runs its guard (~5s), and either proceeds (count > 0) or raises `WorkflowSkippedError` (count = 0, triggers `on_skip` hook). No centralized gate — the 5 ingest tasks are DAG roots, running in parallel with infrastructure-level concurrency.

### Evolve Engine (Level 2 Code Evolution)

The evolve engine (`src/evolve/`, entry point `evolve`) implements automated code evolution for neural network conditioning architectures. Governed by [ADR-001](docs/superpowers/adrs/ADR-001-evolve-code-execution.md).

**Architecture:**
- **Runner** (`runner.py`): Evolution loop — generates candidate programs via LLM, validates, evaluates, selects.
- **Code validator** (`code_validator.py`): AST allowlist (parse-time) rejects dangerous constructs before execution.
- **Evaluator bridge** (`evaluator.py`): Loads candidate programs, validates search space, delegates to backends, returns `EvaluationResult` with error artifacts (tracebacks) on failure so the LLM learns from crashes.
- **Target evaluator** (`targets/scoutgpt/evaluator.py`): `exec()` under defense-in-depth: AST allowlist + restricted globals (`__builtins__: {}`) + subprocess isolation.
- **5 execution backends**: Docker, HF Jobs, local CUDA, remote SSH, job pool. Selected by config.
- **Target: ScoutGPT decoder** (`targets/scoutgpt/`): Evolves player-conditioned action prediction architectures. 8 seed programs (additive, cross-attention, FiLM, gated, hybrid gated attention, SwiGLU, orthogonal cross-attention, Fourier spatial). 9 pre-validated building blocks (`building_blocks.py`) exposed in restricted globals for LLM use.
- **Gated by `code_evolution=True`**: Disabled by default. All other code continues to avoid `exec()`/`eval()`.

---

## 3. C4 Architecture Model

C4 diagrams (Context, Container, Component, Dynamic) are the standard deliverable for documenting this system's architecture. The Structurizr DSL source and generated HTML live in `docs/c4/`.

### 3.1 — Diagram Inventory

| Diagram Level | Name | Purpose |
|---------------|------|---------|
| **L1 — System Context** | Soccer Analytics Platform | Platform in its environment: users, data providers, Databricks boundary |
| **L2 — Container** | Platform Containers | Ingestion Workflows, Unity Catalog, SQL Warehouse, dbt, Lakebase, Synced Tables, Taipy |
| **L3 — Component** | Ingestion Service | StatsBomb/Metrica/Wyscout fetchers, SPADL adapter, shared utilities, Delta writer |
| **L3 — Component** | dbt Transformation | Staging, intermediate, mart models, macros, test suite |
| **L3 — Component** | Taipy Application | Page modules, filter components, chart components, Lakebase connection pool |
| **L4 — Dynamic** | Data Flow | End-to-end: API fetch → Bronze → dbt → Gold → Synced Table → Lakebase → Taipy |
| **L4 — Dynamic** | Zero-ETL Sync | Gold Delta change → Lakeflow pipeline → Lakebase mirror update |
| **Deployment** | Infrastructure | Databricks/AWS resource mapping |
| **L3 — Component** | Guard Registry & Skip Guards | 33 SkipGuard adapters, mandatory FilterResult injection, guard-as-wrapper (each pipeline self-contained), entity_count observability |

### 3.2 — C4 Model: Persons & External Systems

```
Persons:
  - Coach / Match Analyst     : Views match dashboards, shot maps, pass networks
  - Scouting Director         : Compares player stats via radar charts
  - Data Scientist            : Runs notebooks against Delta Lake, builds xG models
  - Platform Engineer (you)   : Provisions infrastructure, maintains pipeline

External Systems:
  - StatsBomb Open Data       : REST API + GitHub JSON (events, lineups, 360)
  - Metrica Sports Sample     : GitHub CSV (25fps tracking data)
  - Wyscout Public Dataset    : JSON (event streams, top 5 leagues)
  - IDSSE (Bundesliga)        : DFL position + event XML from UC Volume (7 matches, 25fps)
  - SkillCorner Open Data     : JSONL broadcast tracking via kloppy (10 A-League matches, 10fps)
  - Hugging Face Hub          : Model/dataset hosting, HF Jobs GPU compute, wheel distribution
  - GitHub                    : Source control, CI/CD via Actions
  - AWS                       : Underlying cloud (S3 storage, IAM, networking)
```

### 3.3 — C4 Model: Containers

```
System Boundary: Soccer Analytics Platform (Databricks on AWS)
  │
  ├── Ingestion Workflows          [Databricks Serverless Compute]
  │   Technology: Python + statsbombpy + requests + silly-kicks
  │   Responsibility: Fetch raw data from providers → write to Bronze
  │
  ├── Unity Catalog                [Databricks Managed]
  │   Technology: Delta Lake (Parquet + transaction log)
  │   Schemas: bronze, silver, gold
  │   Responsibility: Governed data storage across medallion layers
  │
  ├── Serverless SQL Warehouse     [Databricks Serverless]
  │   Technology: Photon engine
  │   Responsibility: Execute dbt transformations and ad-hoc queries
  │
  ├── dbt Project                  [Runs on SQL Warehouse]
  │   Technology: dbt-core + dbt-databricks
  │   Responsibility: Bronze→Silver→Gold transformations, data quality tests
  │
  ├── Synced Tables Pipeline       [Lakeflow Declarative Pipelines]
  │   Technology: Managed Spark streaming
  │   Responsibility: Continuous Gold Delta → Lakebase synchronization
  │
  ├── Lakebase PostgreSQL 17       [Databricks Serverless OLTP]
  │   Technology: PostgreSQL 17, Autoscaling (0.5–4 CU), pgvector
  │   Responsibility: Low-latency OLTP queries for the Taipy app
  │
  ├── Taipy Dashboard              [Hugging Face Spaces (Docker SDK)]
  │   Technology: Python + Taipy + mplsoccer + Plotly + psycopg2
  │   Responsibility: Interactive analytics UI for coaches/analysts
  │
  ├── Evolve Engine                [Local CUDA / HF Jobs / Docker / Remote SSH]
  │   Technology: Python + AST validation + restricted exec() (ADR-001)
  │   Responsibility: Automated code evolution for ScoutGPT conditioning architectures
  │
  └── Hugging Face Hub             [External SaaS]
      Technology: huggingface_hub + HF Jobs + HF Spaces
      Responsibility: Model/dataset hosting, GPU training, wheel distribution, app deployment
```

### 3.4 — C4 Diagram Lifecycle

The `/c4` skill generates a self-contained HTML file with embedded SVGs and tabbed navigation. The `/final-review` skill includes C4 regeneration as part of its pre-commit quality gate.

**Workflow:**
1. Architecture changes are made (new module, new service, changed data flow)
2. Update the Structurizr DSL source in `docs/c4/` (or let `/c4` regenerate from codebase analysis)
3. Run `/final-review` before commit → C4 diagrams regenerated automatically
4. Generated HTML committed alongside code changes

**File locations:**
```
docs/c4/
├── architecture.dsl           # Structurizr DSL source (the model)
└── architecture.html          # Generated: self-contained HTML with all diagrams
```

---

## 4. Technology Decisions

### IaC: Terraform

| Decision | Rationale |
|----------|-----------|
| **Terraform** with Databricks provider | Official `databricks/databricks` provider; multi-cloud; S3+native-locking state backend |
| **Terraform modules** | Separate modules per concern: workspace, catalog, lakebase, workflows, sql_warehouse, synced_tables, service_principals, github_oidc, state_kms |
| **Remote state** | S3 backend with native locking, KMS CMK encryption |

### Python Tooling

| Tool | Purpose | Why |
|------|---------|-----|
| **uv** | Package management, virtual envs | Fast, deterministic, replaces pip+venv+pip-tools |
| **ruff** | Linting + formatting | Single tool replaces flake8+black+isort, 10-100x faster |
| **pyright** | Type checking | Best-in-class for Python, basic mode |
| **pytest** | Testing | Standard, with `pytest-cov` for coverage |
| **pre-commit** | Git hooks | Enforce quality gates locally before push |

### dbt

| Tool | Purpose |
|------|---------|
| **dbt-core** + **dbt-databricks** | Transformation layer |
| **dbt-expectations** | Data quality tests (Great Expectations in dbt) |
| **sqlfluff** | SQL linting for dbt models |

### CI/CD

| Tool | Purpose |
|------|---------|
| **GitHub Actions** | CI/CD pipeline (3 workflows: Python CI, Terraform Plan, dbt CI) |
| **GitHub OIDC** | Secretless CI — AWS IAM + Databricks federation |
| **Environments** | Dev only (single environment; add prod later) |

### Key Design Decisions

| # | Decision | Impact |
|---|----------|--------|
| 1 | Databricks Premium tier | Includes Lakebase, Unity Catalog, Serverless SQL |
| 2 | AWS us-east-1 | Consistent with existing infrastructure |
| 3 | Dev only (single environment) | Simplifies structure; add prod later |
| 4 | Under $100/month budget | Scale-to-zero mandatory everywhere |
| 5 | All 5 data sources | StatsBomb, Metrica, Wyscout (Phase 2), IDSSE + SkillCorner (Phase 11/13) |
| 6 | Kloppy for multi-provider tracking | SkillCorner ingestion uses Kloppy for standardized tracking data parsing. Event data adapters remain direct (simpler for provider-specific JSON/XML). |
| 7 | "Fetch Once, Fork Twice" | SPADL reads from bronze — no redundant API calls |
| 8 | Synced tables via UI + import | Provider lacks project/branch fields; `lifecycle { ignore_changes = all }` |
| 9 | OAuth M2M everywhere | Zero secrets in CI; short-lived JWT (60 min) for app |

---

## 5. Repository Structure

```
luxury-lakehouse/
│
├── ARCHITECTURE.md                    # This document
├── CLAUDE.md                         # AI assistant instructions
├── README.md                         # Project overview
├── ROADMAP.md                       # Research directions and future ideas
├── SECURITY.md                       # Security audit report
├── TODO.md                           # Forward-looking action items
├── .pre-commit-config.yaml           # Pre-commit hooks (ruff, terraform_fmt, detect-secrets)
├── pyproject.toml                    # Python project metadata (uv)
├── uv.lock                           # Deterministic dependency lock
│
├── assets/                           # Images and branding
│   ├── luxury-lakehouse.jpg
│   └── hf-logo.png                  # Hugging Face logo (ROADMAP § HF Hub Integration)
│
├── terraform/
│   ├── environments/dev/             # Dev environment composition
│   ├── modules/
│   │   ├── workspace/                # Unity Catalog
│   │   ├── catalog/                  # Bronze, Silver, Gold schemas + grants
│   │   ├── lakebase/                 # Lakebase Autoscaling (PG 17)
│   │   ├── sql_warehouse/            # Serverless SQL Warehouse
│   │   ├── workflows/                # Ingestion job definitions
│   │   ├── synced_tables/            # Gold → Lakebase sync (40 synced tables)
│   │   ├── app/                      # (removed — Streamlit migrated to HF Spaces)
│   │   ├── service_principals/       # Ingestion SP, App SP, CI SP + federation
│   │   ├── github_oidc/              # AWS IAM OIDC provider + scoped role
│   │   └── state_kms/                # KMS CMK for Terraform state encryption
│   └── shared/
│       ├── versions.tf               # Provider version constraints
│       └── tags.tf                   # Standard resource tagging
│
├── src/
│   ├── analytics/                    # Pure-Python domain models (zero I/O, 30 modules)
│   │   ├── array_utils.py            # NumPy array helpers (shared by multiple modules)
│   │   ├── augmentation.py           # Physics-based position jitter (TacticAI-inspired, pure NumPy)
│   │   ├── coordinates.py            # Coordinate normalization utilities (provider-specific → unified 105×68m)
│   │   ├── defcon_lite.py            # DEFCON-lite: heuristic defensive credit assignment + XGBoost
│   │   ├── elastic_sync.py           # ELASTIC event-tracking sync (Kim et al. 2025) — pure compute
│   │   ├── entity_resolution.py      # Three-layer progressive player matching (TF-IDF + rapidfuzz)
│   │   ├── expected_threat.py        # Data-driven Expected Threat via Markov chain value iteration
│   │   ├── football2vec.py           # Doc2Vec behavioral embeddings (tokenizer, training, inference)
│   │   ├── football2vec_transformer.py # Transformer encoder for 128-dim embeddings (adversarial team debiasing, Ganin GRL)
│   │   ├── football2vec_360.py       # 360-enriched encoder: base transformer + Deep Sets context (144-dim, 128+16)
│   │   ├── formation_detection.py    # Formation label assignment from shape graph + EFPI
│   │   ├── goalkeeper.py             # GK analytics: PSxG, distribution xT, collection, sweeper metrics
│   │   ├── line_breaking.py          # Ward clustering + straddle test for line-breaking passes
│   │   ├── model_validation.py       # Model drift detection: PSI, Wasserstein, CUSUM, KS (pure scipy)
│   │   ├── obso.py                   # OBSO value surface: PPCF × Transition × EPV (Spearman 2018)
│   │   ├── off_ball_xt.py            # Off-ball xT: pitch control × expected threat zones
│   │   ├── pausa.py                  # PAUSA pass timing: temporal/spatial decomposition (Lee et al. 2026)
│   │   ├── pitch_control.py          # Spearman (2017) physics-based pitch control model + ghost trajectories
│   │   ├── pitch_control_numba.py    # Numba-accelerated pitch control for batch computation
│   │   ├── scoutgpt_decoder.py       # GPT-style causal decoder for player-conditioned action prediction (Hong et al. 2025)
│   │   ├── scoutgpt_training.py      # ScoutGPT training: dataset, training loop, evaluation, scheduling
│   │   ├── set_encoder.py            # Deep Sets encoder for xG v2 (freeze-frame player features)
│   │   ├── shape_graph.py            # Shape graph formation detection (Sotudeh 2026, Delaunay triangulation)
│   │   ├── shape_graph_construction.py # Shape graph construction: Delaunay → role assignment
│   │   ├── shape_graph_inference.py  # Shape graph inference: position assignment from graph structure
│   │   ├── smoothing.py              # Savitzky-Golay position smoothing for tracking data
│   │   ├── space_creation.py         # Space creation/destruction via differential OBSO (Fernandez & Bornn 2018)
│   │   ├── symmetry.py               # TacticAI symmetry augmentation (H-flip, V-flip, team swap → 8× data)
│   │   ├── team_shape.py             # Convex hull, centroid, formation lines, spatial metrics from tracking
│   │   └── xg_model.py               # Custom xG: logistic baseline + calibrated XGBoost (JSON serialization)
│   │
│   ├── ingestion/                    # @workflow-decorated Databricks pipelines (51 modules)
│   │   ├── bootstrap.py              # Centralized hook registration for all pipelines
│   │   ├── cost_hook.py              # CostEstimateHook: lifecycle hook writing cost to Delta
│   │   ├── defcon_lite.py            # DEFCON-lite batch computation (gold+bronze → bronze)
│   │   ├── defcon_lite_360.py        # DEFCON-lite 360 variant (freeze-frame context)
│   │   ├── defcon_lite_common.py     # Shared DEFCON-lite constants and helpers
│   │   ├── defcon_lite_tracking.py   # DEFCON-lite tracking variant (player coordinates)
│   │   ├── elastic_sync.py           # ELASTIC event-tracking alignment pipeline (applyInPandas)
│   │   ├── entity_resolution.py      # Cross-source player entity resolution (StatsBomb × Wyscout → bronze)
│   │   ├── expected_threat.py        # Expected Threat pipeline (Databricks → HF Hub)
│   │   ├── export_embeddings_training_data.py # Export training data for football2vec v2 transformer
│   │   ├── export_scoutgpt_training_data.py   # Export SPADL possession episodes for ScoutGPT training
│   │   ├── export_shots_on_target.py # On-target shots export to HF Hub (D39 prerequisite)
│   │   ├── football2vec_v2_training.py # Football2vec v2 training helpers (dataset, MLM masking, splits, LR schedule)
│   │   ├── formations_common.py      # Shared formation detection constants
│   │   ├── formations_efpi.py        # EFPI template-matching formation detection
│   │   ├── formations_shape_graph.py # Shape graph geometric formation detection
│   │   ├── hf_jobs_cost.py           # HFJobsCostRecorder for HF Jobs scripts
│   │   ├── idsse.py                  # IDSSE Bundesliga DFL tracking + events (7 matches, stdlib XML)
│   │   ├── import_obso_results.py    # Import OBSO values from HF Hub to bronze
│   │   ├── import_psxg_predictions.py # Import PSxG predictions from HF Hub to bronze
│   │   ├── import_space_creation.py  # Import space creation values from HF Hub to bronze
│   │   ├── line_breaking.py          # Line-breaking pass batch computation (dispatcher)
│   │   ├── line_breaking_360.py      # Line-breaking via 360 freeze frames
│   │   ├── line_breaking_common.py   # Shared line-breaking constants
│   │   ├── line_breaking_tracking.py # Line-breaking via tracking data
│   │   ├── metrica.py                # Metrica CSV + EPTS ingestion (Games 1-3)
│   │   ├── metrica_common.py         # Shared Metrica ingestion helpers
│   │   ├── metrica_events.py         # Metrica event data ingestion
│   │   ├── metrica_tracking.py       # Metrica tracking data ingestion
│   │   ├── model_validation.py       # Model validation & drift detection pipeline
│   │   ├── off_ball_xt.py            # Off-ball xT batch computation (gold → bronze)
│   │   ├── pausa.py                  # PAUSA pass timing pipeline (temporal/spatial decomposition)
│   │   ├── pitch_control_batch.py    # Pitch control batch pipeline (applyInPandas + frame_batch_id)
│   │   ├── player_embeddings_common.py # Shared embedding constants and stat features
│   │   ├── player_embeddings_v1.py   # Doc2Vec (gensim) player embeddings (v1 baseline)
│   │   ├── player_embeddings_v2.py   # Transformer (128d) player embeddings with adversarial debiasing
│   │   ├── prepare_360_training_data.py # SPADL + 360 freeze frame export to HF Hub
│   │   ├── skillcorner.py            # SkillCorner A-League broadcast tracking (10 matches, kloppy)
│   │   ├── spadl_adapter.py          # Bronze-to-SPADL-converter format adapters
│   │   ├── spadl_conversion.py       # SPADL conversion helpers (action type mapping)
│   │   ├── spadl_vaep.py             # SPADL conversion + VAEP scoring pipeline
│   │   ├── statsbomb.py              # StatsBomb API ingestion (5 bronze tables + 360 backfill)
│   │   ├── sync_hf_costs.py          # Sync HF Jobs cost artifacts → Lakebase
│   │   ├── utils.py                  # Shared CLI, logging, HTTP, Delta helpers
│   │   ├── vaep_training.py          # VAEP model training pipeline
│   │   ├── wyscout.py                # Wyscout JSON ingestion
│   │   ├── xg_model.py               # xG v1 scoring pipeline (logistic + XGBoost)
│   │   ├── xg_model_v2.py            # xG v2 scoring pipeline (Deep Sets + MC dropout)
│   │   ├── guards.py                 # SkipGuard registry + find_new_ids()
│   │   ├── hf_sync.py                # HF Hub dataset sync utilities
│   │   └── tracking_metadata.py      # Tracking data metadata extraction
│   │
│   ├── workflows/                    # Workflow framework (7 modules, zero Spark/Taipy imports)
│   │   ├── card.py                   # WorkflowCard Pydantic model (YAML manifest schema)
│   │   ├── context.py                # WorkflowContext: runtime metadata for lifecycle hooks
│   │   ├── exceptions.py             # WorkflowSkippedError and custom exceptions
│   │   ├── hooks.py                  # Hook protocol and base hook implementations
│   │   ├── loader.py                 # YAML card loader + validate_cli entry point
│   │   ├── registry.py               # WorkflowRegistry singleton: @workflow decorator registration
│   │   └── runner.py                 # Lifecycle runner: on_start/on_complete/on_skip/on_error dispatch
│   │
│   ├── shared/                       # Cross-package constants (zero external deps)
│   │   ├── constants.py              # IDENTIFIER_RE, DEFAULT_GOLD_SCHEMA, mlflow_model_uri()
│   │   └── wheel.py                  # WHEEL_VERSION, WHEEL_FILENAME, WHEEL_BASE_URL, rewrite utilities
│   │
│   ├── evolve/                       # Level 2 code evolution engine (ADR-001)
│   │   ├── config.py                 # Evolution configuration (Pydantic)
│   │   ├── code_validator.py         # AST allowlist validation (defense-in-depth)
│   │   ├── evaluator.py              # Candidate evaluation (exec() under ADR-001 policy)
│   │   ├── remote_worker.py          # Distributed execution handler
│   │   ├── runner.py                 # Evolution loop entry point
│   │   ├── backends/                 # 5 execution backends
│   │   │   ├── docker.py             # Docker container isolation
│   │   │   ├── hf_jobs.py            # Hugging Face Jobs execution
│   │   │   ├── local_cuda.py         # Local GPU (RTX 5070 Ti)
│   │   │   ├── pool.py               # Job pooling / scheduling
│   │   │   └── remote_ssh.py         # Remote SSH execution (DGX Spark)
│   │   └── targets/scoutgpt/         # ScoutGPT decoder evolution target
│   │       ├── evaluator.py          # Fitness function (cross-entropy loss)
│   │       ├── validation.py         # Solution validation
│   │       ├── prompts/              # L1 + L2 system messages
│   │       └── seed_programs/        # 8 conditioning architectures (additive, cross_attention, film, fourier_cross_attention, gated, hybrid_gated_attention, orthogonal_cross_attention, swiglu_conditioning)
│   │
│   └── tests/                        # 91 test modules
│       ├── conftest.py               # Shared fixtures
│       ├── test_augmentation.py
│       ├── test_benchmarks.py        # Performance benchmarks (pytest-benchmark)
│       ├── test_card.py              # WorkflowCard validation tests
│       ├── test_context.py           # WorkflowContext tests
│       ├── test_coordinates.py
│       ├── test_cost_history.py      # HF Jobs cost history tests
│       ├── test_cost_hook.py         # CostEstimateHook tests
│       ├── test_cost_recorder.py     # HFJobsCostRecorder tests
│       ├── test_defcon_lite.py
│       ├── test_elastic_sync.py
│       ├── test_entity_resolution.py
│       ├── test_exceptions.py        # Workflow exception tests
│       ├── test_expected_threat.py
│       ├── test_football2vec.py
│       ├── test_football2vec_360.py
│       ├── test_football2vec_transformer.py
│       ├── test_formations.py
│       ├── test_goalkeeper.py        # GK analytics tests
│       ├── test_hooks.py             # Workflow hook tests
│       ├── test_idsse.py
│       ├── test_ingestion_utils.py
│       ├── test_line_breaking.py
│       ├── test_loader.py            # YAML card loader tests
│       ├── test_merge_delta.py
│       ├── test_metrica.py
│       ├── test_model_validation.py
│       ├── test_obso.py
│       ├── test_off_ball_xt.py
│       ├── test_pausa.py
│       ├── test_pitch_control_batch.py
│       ├── test_pitch_control_model.py
│       ├── test_player_embeddings.py
│       ├── test_registry.py          # WorkflowRegistry tests
│       ├── test_runner.py            # Lifecycle runner tests
│       ├── test_scoutgpt_decoder.py  # ScoutGPT decoder architecture tests
│       ├── test_scoutgpt_training.py # ScoutGPT E2E smoke tests (dataset, training, eval)
│       ├── test_set_encoder.py       # Deep Sets encoder tests
│       ├── test_setup_hf_buckets.py
│       ├── test_shape_graph.py
│       ├── test_shared_constants.py
│       ├── test_skillcorner.py
│       ├── test_smoothing.py
│       ├── test_space_creation.py
│       ├── test_spadl_adapter.py
│       ├── test_spadl_vaep.py
│       ├── test_statsbomb.py
│       ├── test_symmetry.py
│       ├── test_sync_hf_costs.py
│       ├── test_taipy_workflows_perf.py
│       ├── test_taipy_workflows_styling.py
│       ├── test_team_shape.py
│       ├── test_workflows_auto_refresh.py
│       ├── test_wyscout.py
│       ├── test_xg_model.py
│       ├── test_xg_model_v2.py
│       ├── test_scoutgpt_conditioning.py
│       ├── test_backend_pool.py      # Evolve backend pool tests
│       ├── test_hf_jobs_backend.py   # Evolve HF Jobs backend tests
│       ├── test_code_validator.py    # Evolve AST validator tests
│       ├── test_evolve_config.py     # Evolve configuration tests
│       ├── test_evolve_evaluator.py  # Evolve evaluator tests
│       ├── test_evolve_level2.py     # Evolve L2 prompt tests
│       ├── test_hf_sync.py           # HF sync tests
│       ├── test_guards.py            # Guard registry + individual guard tests
│       └── test_guard_conformance.py # Guard conformance suite (auto-discovers all guards)
│
├── hf_taipy_app/                     # Production Taipy dashboard (deployed to HF Spaces)
│   ├── src/
│   │   ├── main.py                   # Entrypoint: PAGE_REGISTRY, Taipy GUI init
│   │   ├── page_template.py          # Template engine: build_page(PageConfig)
│   │   ├── template.py              # GLOSSARY, PAGE_TERMS, shared constants
│   │   ├── pages/                    # 16 pages (PageConfig + build_page per page)
│   │   └── state/                    # Per-page state modules (callbacks, queries, charts)
│   └── README.md                     # HF Spaces metadata (Docker SDK)
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # SILVER: statsbomb/, metrica/, wyscout/, spadl/, idsse/, skillcorner/, line_breaking/, off_ball_xt/, defcon/, entity_resolution/, pitch_control/, pausa/
│   │   ├── intermediate/             # Cross-source joins (ephemeral)
│   │   └── marts/                    # GOLD: 35 fact + 4 dimension tables (39 total) — `dim_matches` / `dim_competitions` / `dim_players` / `dim_teams` are Kimball-conformed per [ADR-011](docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md)
│   ├── tests/                        # Custom data tests
│   ├── macros/                       # distance_to_goal, shot_angle, normalize_coordinates, generate_match_key (ADR-011)
│   └── seeds/                        # 6 seeds: competition_metadata, position_mapping, player_xref_overrides, competition_id_mapping, model_baseline_scalars, task_workflow_mapping
│
├── workflow-cards/                    # 39 YAML workflow manifests (inputs, outputs, deps, cost, monitoring)
│
├── notebooks/                        # Databricks notebooks (8 scripts)
│   ├── train_football2vec.py         # Doc2Vec training + HF Hub publishing
│   ├── train_xg_model.py            # xG model training (logistic + XGBoost) + HF Hub publishing
│   ├── sync_hf_weights.py           # Download model weights from HF Hub to UC Volume
│   ├── publish_datasets.py           # Export Gold tables as Parquet to HF Hub
│   ├── export_demo_data.py           # Export demo data for Gradio Space
│   ├── import_obso_results.py        # Import OBSO results to bronze
│   ├── publish_obso_data.py          # Publish OBSO data to HF Hub
│   └── diag_defcon2.py               # DEFCON diagnostic notebook
│
├── scripts/                          # Infrastructure, HF Jobs, and deployment scripts (32 Python + 6 shell/SQL)
│   ├── manage_space.py               # HF Space lifecycle: create/deploy/status/rebuild/teardown
│   ├── bump_wheel.py                 # Sync wheel version from pyproject.toml to all static consumers (PEP 723, deploy.sh, Terraform)
│   ├── deploy_wheel.py               # Downloads wheel from HF Hub build-artifacts → UC Volume for inference
│   ├── setup_hf_buckets.py           # Initialize HF Buckets (demo-data) with versioned Parquet uploads
│   ├── setup_lakebase_roles.py       # Manage Lakebase PG roles for service principals (databricks-sdk 0.102+)
│   ├── create_indexes.py             # PG indexes on Lakebase synced tables (67 indexes, --verify + ANALYZE)
│   ├── ensure_warehouse.py           # Verify SQL warehouse is RUNNING before dbt builds
│   ├── maintain_synced_tables.py     # Synced table maintenance (refresh, health check)
│   ├── refresh_synced_tables.py      # Trigger SNAPSHOT refresh on synced tables (--wait, --tables)
│   ├── delete_synced_table.py        # Delete synced table + drop PG ghost table
│   ├── compute_xt_grid_hf.py         # HF Jobs UV script: compute data-driven xT grid from SPADL actions
│   ├── compute_obso_hf.py            # HF Jobs GPU script: OBSO value surfaces via JAX on A10G
│   ├── compute_epv_transition_hf.py  # HF Jobs script: EPV + transition grids for OBSO
│   ├── compute_epv_transition_hf_helpers.py # EPV computation helpers
│   ├── compute_space_creation_hf.py  # HF Jobs GPU script: space creation via JAX double-vmap on A10G
│   ├── compute_space_creation_hf_helpers.py # Space creation computation helpers
│   ├── train_xg_model_hf.py          # HF Jobs CPU script: xG model training with MLflow logging
│   ├── train_xg_v2_hf.py             # HF Jobs GPU script: xG v2 Deep Sets + MC dropout training
│   ├── train_xg_v2_hf_helpers.py     # xG v2 training helpers (dataset, evaluation)
│   ├── train_vaep_model_hf.py        # HF Jobs CPU script: VAEP model training
│   ├── train_football2vec_v2.py      # HF Jobs GPU script: football2vec v2 transformer + adversarial debiasing
│   ├── train_football2vec_360.py     # HF Jobs GPU script: football2vec 360-enriched encoder training
│   ├── train_football2vec_360_helpers.py # Football2vec 360 training helpers
│   ├── train_psxg_hf.py              # HF Jobs CPU script: PSxG logistic model training
│   ├── train_scoutgpt_hf.py          # HF Jobs GPU script: ScoutGPT decoder training (Hong et al. 2025)
│   ├── publish_freeze_frame_hf.py    # Publish StatsBomb 360 freeze-frame dataset to HF Hub
│   ├── publish_xg_shots_hf.py        # Publish xG shot dataset to HF Hub
│   ├── publish_spadl_vaep_hf.py      # Publish SPADL + VAEP dataset to HF Hub
│   ├── export_embedding_atlas_data.py # Export embedding atlas data for visualization
│   ├── benchmark_hf_jobs.py          # HF Jobs performance benchmarking
│   ├── import_synced_tables.sh       # Terraform import workflow (19 tables)
│   ├── create_cost_table.sql         # Create observability cost table DDL
│   ├── lakebase_grants.sql           # PG GRANT SELECT for Taipy app SP
│   ├── deploy.sh                     # Wheel build + Terraform apply + ingestion trigger
│   ├── run_evolve_local.sh           # Run evolve engine locally (CUDA)
│   └── run_evolve_overnight.sh       # Run evolve engine overnight (multi-backend)
│
├── .github/workflows/
│   ├── python-ci.yml                 # ruff + pyright + pytest
│   ├── semgrep.yml                   # Semgrep SAST (p/python + p/security-audit)
│   ├── terraform-plan.yml            # Plan on PR (OIDC auth)
│   └── dbt-ci.yml                    # dbt slim CI (state:modified+, --empty, --defer)
│
├── demo_space/                      # Hugging Face Gradio demo Space (6 tabs: pass quality, pitch control, player similarity, shot map, DEFCON pressure, pass timing)
│   ├── app.py                       # Gradio app with luxury flagship theme (dark surfaces, gold accents)
│   └── pitch_control.py             # Pure NumPy pitch control (Spearman 2017) — no Spark dependency
│
└── docs/
    ├── c4/
    │   ├── architecture.dsl          # Structurizr DSL source
    │   └── architecture.html         # Generated: self-contained HTML
    ├── huggingface/
    │   ├── model-card.md             # HF Hub model card: football2vec (source of truth)
    │   ├── xg-model-card.md          # HF Hub model card: xG model (source of truth)
    │   ├── xg-v2-model-card.md       # HF Hub model card: xG v2 set encoder (source of truth)
    │   ├── model-cards/
    │   │   └── vaep-model.md         # HF Hub model card: VAEP model (source of truth)
    │   ├── org-card.md               # HF Hub org card (source of truth)
    │   ├── org-interests.md          # HF Hub org "AI & ML interests" (paste via web UI)
    │   └── dataset-cards/            # HF Hub dataset cards (16 datasets)
    ├── huggingface-setup.md          # Hugging Face Hub integration guide (forks)
    └── plans/                        # Implementation design documents
```

---

## 6. Cross-Cutting Concerns

### 6.1 — Security

| Concern | Implementation |
|---------|---------------|
| Secrets management | No hardcoded credentials; OAuth M2M for Terraform + CI (OIDC federation, zero secrets); PAT for app (OAuth M2M ready, pending secret rotation) |
| Admin API auth | `POST /api/cache/clear` on the Taipy app validates an HF user access token against `whoami-v2` and requires `luxury-lakehouse` org membership with `admin`/`write` role. No shared secret stored — each call validates independently against HF, so revocation is immediate. See `hf_taipy_app/src/admin_api.py`. |
| Network | TLS everywhere; HTTPS-only for all data fetches |
| IAM | Least-privilege; separate service principals per workload. Terraform CI SP runs with three co-floor privileges — documented: workspace-admins-group membership per [ADR-007](docs/superpowers/adrs/ADR-007-workspace-admin-floor.md) (TF-planner cascade makes reduction destructive), `account_admin` per [ADR-006](docs/superpowers/adrs/ADR-006-account-admin-floor.md) (no narrower account-scope named role in provider v1.112/v1.113), and Unity-Catalog `ALL_PRIVILEGES` + group ownership. SEC4 (SEC-AUDIT-v1.12.0 INF-01 partially closed 2026-04-17) added explicit ACLs for the Lakebase project + 37 synced-table pipelines, reducing the transitive-admin surface. |
| Data classification | Open-source data only (no PII); Unity Catalog ACLs applied |
| Audit | Unity Catalog audit logs; Terraform state versioning |
| Input validation | Regex on all user-supplied identifiers (`^[a-zA-Z_][a-zA-Z0-9_]*$`) |
| SSL verification | Explicit `verify=True` on all HTTP requests |
| Timeouts | `(10, 30)` connect/read on every HTTP call |
| Retry safety | Exponential backoff on transient errors (429/5xx); max 3 retries |
| Bandit compliance | Ruff S rules enforced on `src/` and `scripts/`; no eval/exec/pickle/shell=True. Scoped exception: `exec()` permitted in `src/evolve/targets/*/evaluator.py` and `src/evolve/remote_worker.py` only, under [ADR-001](docs/superpowers/adrs/ADR-001-evolve-code-execution.md) defense-in-depth (AST allowlist + restricted globals + subprocess isolation) |
| SAST | Semgrep in CI (`p/python` + `p/security-audit` rulesets) |
| Content validation | Schema checks and non-empty assertions before every Delta write |
| Model serialization | MLflow cloudpickle bounded by UC ACLs; executors receive JSON only (see [SECURITY.md](SECURITY.md)) |
| Full audit | See [SECURITY.md](SECURITY.md) — 31 findings, 28 resolved, 3 accepted |

### 6.2 — Quality Standards

All code must pass these gates before merge:

| Check | Command | Threshold |
|-------|---------|-----------|
| Lint | `uv run ruff check src/ scripts/` | Zero violations |
| Type check | `uv run pyright src/` | Zero errors (basic mode) |
| Unit tests | `uv run pytest src/tests/ -v` | All pass |
| Security scan | Ruff S (bandit) + Semgrep SAST | Zero violations |
| Wheel build | `uv build` | Produces installable wheel |

**Enforced Ruff rule sets:** E, W, F, I, N, UP, B, S (bandit), RUF

### 6.3 — Cost Management

| Strategy | Implementation |
|----------|---------------|
| Scale-to-zero | Lakebase Autoscaling min=0.5 CU; SQL Warehouse auto-stop=10min |
| Serverless compute | No always-on clusters; pay per DBU consumed |
| Budget | Dev environment: under $100/month with scale-to-zero |

### 6.4 — Observability

| Layer | Tool |
|-------|------|
| Infrastructure | Terraform plan output, Databricks audit logs |
| Data pipeline | dbt test results, source freshness checks |
| Ingestion | Databricks Workflow run history, row count assertions |
| Cost tracking | `CostEstimateHook` → `observability.workflow_cost_live` (Delta MERGE per run: state, duration, cost, entity count, row count) |
| Application | Taipy built-in metrics, Lakebase query logs |

### 6.5 — Testing Strategy

| Level | What | How |
|-------|------|-----|
| Unit | Ingestion logic, utility functions, analytics models | pytest (91 test modules, incl. pytest-benchmark baselines) |
| Integration | dbt models compile and run | dbt slim CI (`state:modified+`, `--empty`, `--defer`) |
| Data quality | Row counts, value ranges, referential integrity | dbt tests (~523) + dbt-expectations |
| E2E | Taipy pages render with real data | Manual smoke test |
| Infrastructure | Terraform validates | `terraform validate` + `terraform plan` |

### 6.6 — Database Performance

Lakebase and Databricks performance standards are codified in [CLAUDE.md § Database Performance](CLAUDE.md#database-performance). Key rules:

- **Lakebase (PG):** Index every filtered column on fact tables >100K rows. No `ON ONLY` indexes (partitioned tables). Avoid `SELECT DISTINCT` on large tables — use recursive CTE. Re-run `scripts/create_indexes.py` after every synced table recreation.
- **Databricks (Spark/dbt):** `validate_dataframe()` returns row count to `write_delta_table()` (no double `df.count()`), all writes use `replaceWhere` for idempotency, don't `.toPandas()` unbounded tables, extract repeated window functions into CTEs. 24 mart models use `liquid_clustered_by` for automatic data layout (replaced static Z-ordering). Predictive Optimization enabled at catalog level. Auto-compaction and `optimizeWrite` enabled via `+tblproperties` on all mart tables. 34 of 37 mart models enforce dbt model contracts (`contract: {enforced: true}`, `on_schema_change: fail`).

The platform has 61 btree indexes across 27 tables + 6 HNSW vector indexes on embedding tables at 128-dim/144-dim (67 total) covering all Taipy query patterns. Managed by `scripts/create_indexes.py` with `ANALYZE` for planner statistics and `--verify` for EXPLAIN ANALYZE validation.

### 6.7 — Architecture Documentation

C4 diagrams are the single source of truth for architecture documentation, maintained as Structurizr DSL and regenerated automatically via `/final-review` before significant commits. See [§3.4](#34--c4-diagram-lifecycle).

---

## 7. Risk Register

Some Terraform provider gaps and data constraints require manual workarounds that add operational friction. The table below tracks these risks and their mitigations.

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| R1 | Lakebase Terraform provider support incomplete | Medium | High | **Realized**: Provider v1.110.0 lacks project/branch fields for synced tables. Mitigated via UI creation + `terraform import` + `lifecycle { ignore_changes = all }`. |
| R2 | Synced Tables feature not in Databricks tier | Low | Critical | Confirmed: Premium tier supports Synced Tables. |
| R3 | StatsBomb API rate limiting during bulk ingestion | Medium | Low | Exponential backoff; local caching; off-peak scheduling. |
| R4 | Databricks cost exceeds expectations | Medium | Medium | Scale-to-zero everywhere; billing alerts; monthly review. |
| R5 | dbt-databricks adapter incompatibility | Low | Medium | Tested in Phase 3; no issues encountered. |
| R6 | Taipy performance with mplsoccer | Medium | Low | Figure caching; pre-computed static images for common views. |
| R7 | Unity Catalog ACL complexity | Low | Low | Single admin user for dev; RBAC deferred to prod. |
| R8 | DEFCON repo has no license | ~~High~~ Resolved | ~~Medium~~ | Apache-2.0 license added to `hyunsungkim-ds/defcon`. Implementation uses paper equations and open-source libraries. |
| R9 | `players-matcher` has no license | ~~High~~ Resolved | ~~Medium~~ | Apache-2.0 license added by maintainer (Matteo Matteotti) on 2026-03-06, merging PR #2. `rapidfuzz` remains primary approach; `players-matcher` available as reference. |
| R10 | Public tracking data insufficient for GNN training | High | High | Tiers 1–3 feasible with public data. Full GNN deferred. |

---

## 8. Appendices

### A. Data Volume Estimates

| Source | Matches | Events/Match | Rows (Bronze) | Size Estimate |
|--------|---------|-------------|----------------|---------------|
| StatsBomb (open) | ~3,000 | ~3,400 | ~10.2M events | ~500 MB Parquet |
| Metrica (sample) | 3 | 135,000 frames | ~405K frames | ~15 MB Parquet |
| Wyscout (public) | ~1,900 | ~1,800 | ~3.4M events | ~400 MB Parquet |
| Bundesliga IDSSE | 7 | ~460,000 frames + events | ~21.9M tracking + events | ~2.5 GB XML |
| SkillCorner | 10 | ~97,000 frames | ~6.8M rows | ~100 MB JSONL |
| **Total Bronze** | | | **~42.3M rows** | **~4 GB** |

### B. Reference: Soccermatics Chapter → Analytics Mapping

| Chapter | Analytics Concept | Implementation |
|---------|-------------------|----------------|
| 00 | Data loading patterns | `stg_*` sources |
| 01 | Shot plotting, pass networks, heat maps | `fct_shots`, `fct_passes` |
| 02 | xG model, shot geometry | `fct_shots` (distance, angle features) |
| 03 | Radar plots, per-90 stats | `fct_player_stats` |
| 04 | Expected Threat (xT), Markov chains | `fct_passes` (xT values) |
| 05 | Match simulation, randomness | `fct_match_summary` |
| 06 | Voronoi diagrams, pitch control | `fct_tracking_frames` |
| 07 | xG with tracking data | Future: join events + tracking |
| 08 | Physical data, acceleration | `fct_tracking_frames` |
| 09 | Clustering, progressive passes | `fct_passes`, `fct_player_stats` |
| 10 | Taipy web app | `hf_taipy_app/` |
| — | SPADL action valuation (VAEP) | `fct_action_values` (Phase 9) |
| — | Movement analysis | Phase 12 — complete (PPDA, physical metrics, off-ball xT) |
| — | Line-breaking pass detection | Phase 13 — clustering + segment intersection |
| — | Defensive contribution (DEFCON) | Phase 17 — EPV decomposition + credit assignment |
| — | PAUSA pass timing (Lee et al. 2026) | D9/D10/D16 — ELASTIC sync + OBSO + temporal/spatial decomposition |
| — | Model validation & drift detection | D12 — PSI, Wasserstein, CUSUM, KS across all models |
| — | Goalkeeper analytics (PSxG, dist. xT) | Cycle 4 Phase 2 — `fct_goalkeeper_stats` |
| — | Tactical positions & formations | Cycle 4 Phase 2 — `fct_player_positions`, `fct_position_maps`, shape graphs |
| — | Player embeddings (transformer) | D25/D45 — football2vec v2 128d + 360-enriched 144d |
| — | ScoutGPT decoder | D32 — player-conditioned action prediction (Hong et al. 2025) |
| — | Space creation & destruction | D20 — differential OBSO (Fernandez & Bornn 2018) |
| — | Skip guards (guard-as-wrapper) | D40/D52 — 33 guards with mandatory injection, guard-as-wrapper (each pipeline self-contained), entity_count observability |

### C. Dependencies on MCP CodeDeploy Project

| Dependency | Status |
|------------|--------|
| DevOpsAgent IAM role | Active — `AWS_PROFILE=devops-agent` (account 454762693631) |
| S3 state bucket | Active — `karstenskyt-terraform-state` with native S3 locking |

### D. Academic References

Consolidated list of academic citations referenced across UI pages and analytics modules. Each entry mirrors the canonical citation in the corresponding workflow card, NOTICE file, or implementation source-code docstring. When updating an entry here, update all three sources together to prevent drift (the D56 audit closed 7 such drifts on 2026-04-13 — see `docs/superpowers/specs/2026-04-13-daily-job-hardening-design.md` § Item 3).

| Author / Year | Title | Used by (UI / module) |
|---|---|---|
| Anzer & Bauer (2022) | "A Goal Scoring Probability Model for Shots Based on Synchronized Positional and Event Data in Football and Floorball." *Machine Learning 111(6)*. DOI: 10.1007/s10994-021-06011-5 | Heat Map page |
| Bekkers & Dabadghao (2025) | "Flow Motifs in Soccer: How Teams Play." *arXiv:2506.23843* | Team Shape page |
| Bourbousson, Sève & McGarry (2010) | "Space-time coordination dynamics in basketball." *Journal of Sports Sciences 28(3)* | Team Shape page |
| Butcher et al. (2025) | "An Expected Goals On Target (xGOT) Model." *MDPI* (DOI: 10.1515/jqas-2024-0091) | Goalkeeper Analytics, `wf-goalkeeper`, `wf-import-psxg`, `wf-export-shots` |
| Danesi, P. (2025) | "Football2Vec: Transformer-Based Player Embeddings." | Player Similarity, `src/analytics/football2vec_transformer.py`, `wf-football2vec-v2` |
| Decroos, Bransen, Van Haaren & Davis (2019) | "Actions Speak Louder than Goals: Valuing Player Actions in Soccer." *KDD* | Action Values, Player Radar, Match Summary (Big Story VAEP ranking), `wf-vaep` |
| Donnelly (2024) | "Systematic Approach to Performance Analysis." (course materials) | Conversion Funnel page |
| Frencken, Lemmink, Delleman & Visscher (2011) | "Oscillations of centroid position and surface area of soccer teams in small-sided games." *Journal of Sports Sciences 29(14)* | Team Shape page |
| Ganin et al. (2016) | "Domain-Adversarial Training of Neural Networks." *JMLR 17* | Player Similarity (gradient reversal for adversarial debiasing) |
| Kim, H.S. et al. (2025) — ELASTIC | "ELASTIC: Event-Tracking Data Synchronization in Soccer Without Annotated Event Locations." *ECML-PKDD MLSA* (arXiv:2508.09238) | Pass Timing page, `wf-elastic-sync` |
| Kim, H.S. et al. (2025) — DEFCON | "Better Prevent than Tackle: Valuing Defense in Soccer Based on Graph Neural Networks." *arXiv:2512.10355* | Defensive Valuation page, `wf-defcon`, `src/analytics/defcon_lite.py` |
| Lamberts (2025) | Goalkeeper Distribution Value Model. DOI: 10.1007/978-3-031-31772-9_19 | Goalkeeper Analytics |
| Lee, Jo, Hong, Bauer & Ko (2026) | "Valuing La Pausa" (PAUSA). *MIT Sloan Sports Analytics Conference 2026* | Pass Timing page, `wf-obso-pausa` |
| Pena & Touchette (2012) | "A network theory analysis of football strategies." *arXiv:1206.6904* | Pass Network page |
| Robberechts & Davis (2020) | "How Data Availability Affects the Ability to Learn Good xG Models." | Match Summary, Shot Map, `wf-xg-v1` (replaced Rathke per D56 Option A, 2026-04-13) |
| Shazeer, N. (2020) | "GLU Variants Improve Transformer." *arXiv:2002.05202* | `src/analytics/scoutgpt_decoder.py` (swiglu branch), `wf-scoutgpt` |
| Singh, Karun (2018) | "Introducing Expected Threat (xT)." (blog: karun.in/blog/expected-threat.html) | Movement & Pressing, `wf-xt-grids`, `wf-off-ball-xt` |
| Sotudeh, H. (2026) | "Identification of Team Tactical Formations and Player Positions in Association Football." *PhD thesis, ETH Zurich (DISS. ETH NO. 31732)*. Published: *npj Complexity*, DOI: 10.1038/s44260-025-00047-x | Tactical Positions, `src/analytics/shape_graph_construction.py`, `wf-shape-graphs` |
| Spearman, W. (2017) | "Physics-Based Modeling of Pass Probabilities in Soccer." *MIT Sloan Sports Analytics Conference 2017* | Pitch Control, Movement & Pressing, `src/analytics/pitch_control.py`, `wf-pitch-control`, `wf-off-ball-xt` |
| Spearman, W. (2018) | "Beyond Expected Goals." *MIT Sloan Sports Analytics Conference 2018* (builds on the 2017 framework; DO NOT CONFLATE with 2017) | Pass Timing, `src/analytics/obso.py`, `wf-obso-pausa`, `wf-import-obso`, `wf-space-creation`, `wf-epv-reachability` |
| Suzuki et al. (2019) | "Team Tactics Estimation in Soccer Videos Based on a Deep Extreme Learning Machine and Characterized by Distance Matrices." DOI: 10.1515/jqas-2019-0060 | Pass Map page |
| Tancik, M. et al. (2020) | "Fourier Features Let Networks Learn High Frequency Functions in Low Dimensional Domains." *arXiv:2006.10739* | `src/analytics/scoutgpt_decoder.py` (fourier_cross_attention branch), `wf-scoutgpt` |
| Trainor & Chassy (2021) | "Psychological and Physiological Impact of Soccer's Transition Periods." *Frontiers in Psychology 11*. DOI: 10.3389/fpsyg.2020.531688 | Match Summary page |

**Notes on the D56 audit (2026-04-13):**

- The 2017 and 2018 Spearman papers are distinct works, both cited in this codebase. 2017 = pitch control (time-to-intercept); 2018 = OBSO/EPV (scoring probability surface). Do not conflate. See `docs/superpowers/specs/2026-04-13-daily-job-hardening-design.md` Appendix C for the full disambiguation.
- Rathke's xG paper is no longer cited (the implementation was never anchored to any specific Rathke paper; replaced with the project-canonical Robberechts & Davis 2020).
- Sotudeh's PhD thesis is at ETH Zurich (DISS. ETH NO. 31732), not the University of Twente MSc thesis. The implementation references the PhD work.

---

*This is a living document. Completed phase details are preserved in git history. See [ROADMAP.md](ROADMAP.md) for future research directions.*
