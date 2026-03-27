# Databricks Lakebase Architecture — Soccer Analytics Platform

> **Status**: Phase 20 complete (Taipy migration) + Team Shape + pre-aggregated tracking tables — 14 Taipy pages, 26 synced tables, 45 PG indexes (41 btree + 4 HNSW). HuggingFace Hub: 4 models + 11 datasets published, GPU training on HF Jobs A10G.
> **Last Updated**: 2026-03-26
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
│  │  • compute_embeddings → Doc2Vec + z-score player embeddings      │    │
│  │  • compute_xg_model → Custom xG scoring (logistic + XGBoost)     │    │
│  │  • compute_expected_threat → Data-driven xT grid from SPADL      │    │
│  │  • elastic_sync → ELASTIC event-tracking alignment (Kim 2025)    │    │
│  │  • compute_pausa → PAUSA pass timing (Lee et al. 2026)          │    │
│  │  • model_validation → Drift detection (PSI/Wasserstein/CUSUM)   │    │
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
│  │  • xg_predictions, expected_threat_grids                         │    │
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
│  │  GOLD (business logic, analytics-ready):                         │    │
│  │  • fct_shots, fct_passes, fct_player_stats, fct_match_summary    │    │
│  │  • fct_xg_predictions, fct_tracking_frames, fct_action_values    │    │
│  │  • fct_player_embeddings, fct_physical_stats                     │    │
│  │  • fct_defensive_values, fct_defcon_actions, fct_defcon_pressure │    │
│  │  • fct_player_embeddings_season/career                           │    │
│  │  • fct_pausa_values, fct_pass_timing, fct_pausa_rankings         │    │
│  │  • fct_player_percentiles, fct_workflow_costs                    │    │
│  │  • fct_formation_labels, fct_tracking_avg_positions              │    │
│  │  • fct_tracking_shape_timeline                                   │    │
│  │  • dim_players, dim_teams, dim_competitions                      │    │
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
│  │  Deployed on HuggingFace Spaces (Docker SDK)                      │    │
│  │  • PAT auth (OAuth M2M blocked — see TODO M2)                     │    │
│  │  • Connects to Lakebase via psycopg2 (ThreadedConnectionPool)     │    │
│  │  • 14 pages: Shot Map, Pass Map, Heat Map, Pass Network,          │    │
│  │    Match Summary, Player Comparison, Player Impact,               │    │
│  │    Player Similarity, Movement & Pressing, Pass Timing,           │    │
│  │    Pitch Control, Team Shape, Defensive Impact,                   │    │
│  │    AI/ML Workflows                                                │    │
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

**Implementation notes:**

- `scheduling_policy = "SNAPSHOT"` — initial sync with on-demand refresh
- `logical_database_name = "databricks_postgres"` — standard Lakebase database
- **Autoscaling workaround (provider v1.110.0):** `databricks_database_synced_database_table` only supports `database_instance_name` (Provisioned). Synced tables targeting Autoscaling projects must be created via Databricks UI, then imported into Terraform. `lifecycle { ignore_changes = all }` prevents drift. This applies to any new synced table.
- **Schema changes:** Must delete synced table, drop ghost PG table, recreate via API, re-import into Terraform.
- **PG indexes:** 41 btree indexes across 17 tables + 4 HNSW vector indexes on embedding tables = 45 total. Dropped on synced table recreation — re-run `scripts/create_indexes.py` alongside `scripts/lakebase_grants.sql`. Script now runs `ANALYZE` on all indexed tables to ensure the query planner uses indexes.
- **SNAPSHOT refresh:** Synced tables with `scheduling_policy = "SNAPSHOT"` do not auto-refresh. Run `scripts/refresh_synced_tables.py` after upstream dbt rebuilds. Supports `--wait` (poll until IDLE) and `--tables` (comma-separated subset). The Terraform provider has no schedule/cron field — this is the operational workaround.
- **Credential API:** REST endpoint is `/api/2.0/postgres/credentials` (NOT `/api/2.0/database/credentials`).

### Taipy Application Pages

| Page | Visualization | Data Source |
|------|--------------|-------------|
| Heat Map | Action density per player/team/match | `fct_passes_synced`, `fct_shots_synced` |
| Match Summary | Scorecard + xG metrics + horizontal bar chart | `fct_match_summary_synced` |
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
| Defensive Impact | DEFCON-lite attacker pressure rankings, breakdown, match timeline | `fct_defcon_pressure_synced`, `fct_defcon_actions_synced` |
| AI/ML Workflows | DAG visualization, cost tracking, workflow cards, run status | `fct_workflow_costs_synced` |

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
  - GitHub                    : Source control, CI/CD via Actions
  - AWS                       : Underlying cloud (S3 storage, IAM, networking)
```

### 3.3 — C4 Model: Containers

```
System Boundary: Soccer Analytics Platform (Databricks on AWS)
  │
  ├── Ingestion Workflows          [Databricks Serverless Compute]
  │   Technology: Python + statsbombpy + requests + socceraction
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
  └── Taipy Dashboard              [HuggingFace Spaces (Docker SDK)]
      Technology: Python + Taipy + mplsoccer + Plotly + psycopg2
      Responsibility: Interactive analytics UI for coaches/analysts
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
| 5 | All 3 data sources from start | StatsBomb, Metrica, Wyscout all in Phase 2 |
| 6 | No Kloppy in Phase 9 | Direct bronze adapters simpler; Kloppy deferred to Phase 10 |
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
│   └── hf-logo.png                  # HuggingFace logo (ROADMAP § HF Hub Integration)
│
├── terraform/
│   ├── environments/dev/             # Dev environment composition
│   ├── modules/
│   │   ├── workspace/                # Unity Catalog
│   │   ├── catalog/                  # Bronze, Silver, Gold schemas + grants
│   │   ├── lakebase/                 # Lakebase Autoscaling (PG 17)
│   │   ├── sql_warehouse/            # Serverless SQL Warehouse
│   │   ├── workflows/                # Ingestion job definitions
│   │   ├── synced_tables/            # Gold → Lakebase sync (19 synced tables)
│   │   ├── app/                      # (removed — Streamlit migrated to HF Spaces)
│   │   ├── service_principals/       # Ingestion SP, App SP, CI SP + federation
│   │   ├── github_oidc/              # AWS IAM OIDC provider + scoped role
│   │   └── state_kms/                # KMS CMK for Terraform state encryption
│   └── shared/
│       ├── versions.tf               # Provider version constraints
│       └── tags.tf                   # Standard resource tagging
│
├── src/
│   ├── analytics/
│   │   ├── pitch_control.py          # Spearman (2017) physics-based pitch control model + ghost trajectories
│   │   ├── line_breaking.py          # Ward clustering + straddle test for line-breaking passes
│   │   ├── off_ball_xt.py            # Off-ball xT: pitch control × expected threat zones
│   │   ├── defcon_lite.py            # DEFCON-lite: heuristic defensive credit assignment + XGBoost
│   │   ├── elastic_sync.py          # ELASTIC event-tracking sync (Kim et al. 2025) — pure compute
│   │   ├── obso.py                  # OBSO value surface: PPCF × Transition × EPV (Spearman 2018)
│   │   ├── augmentation.py          # Physics-based position jitter (TacticAI-inspired, pure NumPy)
│   │   ├── model_validation.py      # Model drift detection: PSI, Wasserstein, CUSUM, KS (pure scipy)
│   │   ├── entity_resolution.py     # Three-layer progressive player matching (TF-IDF + rapidfuzz)
│   │   ├── football2vec.py          # Doc2Vec behavioral embeddings (tokenizer, training, inference)
│   │   ├── xg_model.py             # Custom xG: logistic baseline + calibrated XGBoost (JSON serialization, no pickle)
│   │   ├── symmetry.py             # TacticAI symmetry augmentation (H-flip, V-flip, team swap → 8× data)
│   │   └── smoothing.py             # Savitzky-Golay position smoothing for tracking data
│   │
│   ├── ingestion/
│   │   ├── statsbomb.py              # StatsBomb API ingestion (5 bronze tables + 360 backfill)
│   │   ├── metrica.py                # Metrica CSV + EPTS ingestion (Games 1-3)
│   │   ├── wyscout.py                # Wyscout JSON ingestion
│   │   ├── idsse.py                  # IDSSE Bundesliga DFL tracking + events (7 matches, stdlib XML)
│   │   ├── skillcorner.py            # SkillCorner A-League broadcast tracking (10 matches, kloppy)
│   │   ├── elastic_sync.py          # ELASTIC event-tracking alignment pipeline (applyInPandas)
│   │   ├── pausa.py                 # PAUSA pass timing pipeline (temporal/spatial decomposition)
│   │   ├── model_validation.py      # Model validation & drift detection pipeline (reads gold, writes results)
│   │   ├── line_breaking.py          # Line-breaking pass batch computation (360 + tracking)
│   │   ├── off_ball_xt.py            # Off-ball xT batch computation (gold → bronze)
│   │   ├── defcon_lite.py            # DEFCON-lite batch computation (gold+bronze → bronze)
│   │   ├── entity_resolution.py     # Cross-source player entity resolution (StatsBomb × Wyscout → bronze)
│   │   ├── player_embeddings.py     # Player embedding inference + stat vector computation
│   │   ├── xg_model.py             # xG model scoring pipeline (load weights from UC Volume, score shots)
│   │   ├── pitch_control_batch.py  # Pitch control batch pipeline (applyInPandas + frame_batch_id)
│   │   ├── spadl_adapter.py          # Bronze-to-socceraction format adapters
│   │   ├── spadl_vaep.py             # SPADL conversion + VAEP scoring pipeline
│   │   └── utils.py                  # Shared CLI, logging, HTTP, Delta helpers
│   │
│   ├── streamlit_app/
│   │   ├── app.py                    # Entrypoint: st.navigation, page routing
│   │   ├── config.py                 # Pydantic BaseSettings
│   │   ├── db.py                     # OAuth M2M, ThreadedConnectionPool, parameterized queries
│   │   ├── pages/                    # 12 pages (incl. player_similarity.py, pass_timing.py)
│   │   └── components/               # filters.py, pitch.py, charts.py, feedback.py, glossary.py
│   │
│   └── tests/                        # 31 test modules
│       ├── test_statsbomb.py
│       ├── test_metrica.py
│       ├── test_wyscout.py
│       ├── test_idsse.py
│       ├── test_skillcorner.py
│       ├── test_spadl_adapter.py
│       ├── test_spadl_vaep.py
│       ├── test_ingestion_utils.py
│       ├── test_pitch_control_model.py
│       ├── test_line_breaking.py
│       ├── test_off_ball_xt.py
│       ├── test_defcon_lite.py
│       ├── test_entity_resolution.py
│       ├── test_football2vec.py
│       ├── test_player_embeddings.py
│       ├── test_xg_model.py
│       ├── test_expected_threat.py
│       ├── test_player_similarity.py
│       ├── test_smoothing.py
│       ├── test_pitch_control_batch.py
│       ├── test_symmetry.py
│       ├── test_merge_delta.py
│       ├── test_benchmarks.py
│       ├── test_elastic_sync.py
│       ├── test_obso.py
│       ├── test_pausa.py
│       ├── test_model_validation.py
│       ├── test_augmentation.py
│       ├── test_streamlit_components.py
│       ├── test_streamlit_config.py
│       └── test_streamlit_db.py
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # SILVER: statsbomb/, metrica/, wyscout/, spadl/, idsse/, skillcorner/, line_breaking/, off_ball_xt/, defcon/, entity_resolution/, pitch_control/, pausa/
│   │   ├── intermediate/             # Cross-source joins (ephemeral)
│   │   └── marts/                    # GOLD: 16 fact + 3 dimension tables
│   ├── tests/                        # Custom data tests
│   ├── macros/                       # distance_to_goal, shot_angle
│   └── seeds/                        # competition_metadata.csv, position_mapping.csv, player_xref_overrides.csv
│
├── notebooks/
│   ├── train_football2vec.py         # Databricks notebook: Doc2Vec training + HuggingFace Hub publishing
│   ├── train_xg_model.py            # Databricks notebook: xG model training (logistic + XGBoost) + HF Hub publishing
│   ├── sync_hf_weights.py           # Databricks notebook: Download model weights from HF Hub to UC Volume
│   └── publish_datasets.py           # Databricks notebook: Export Gold tables as Parquet to HF Hub (5 datasets + model cards)
│
├── scripts/
│   ├── create_indexes.py             # PG indexes on Lakebase synced tables (38 indexes, 14 tables, --verify + ANALYZE)
│   ├── compute_xt_grid_hf.py        # HF Jobs UV script: compute data-driven xT grid from SPADL actions
│   ├── compute_obso_hf.py          # HF Jobs GPU script: OBSO value surfaces via JAX on A10G
│   ├── train_xg_model_hf.py        # HF Jobs CPU script: xG model training with MLflow logging
│   ├── refresh_synced_tables.py      # Trigger SNAPSHOT refresh on synced tables (--wait, --tables)
│   ├── delete_synced_table.py        # Delete synced table + drop PG ghost table
│   ├── import_obso_results.py        # Download OBSO Parquet from HF Hub → bronze Delta tables
│   ├── import_synced_tables.sh       # Terraform import workflow (19 tables)
│   ├── lakebase_grants.sql           # PG GRANT SELECT for Taipy app SP
│   └── deploy.sh                     # Wheel build + Terraform apply + ingestion trigger
│
├── .github/workflows/
│   ├── python-ci.yml                 # ruff + pyright + pytest
│   ├── terraform-plan.yml            # Plan on PR (OIDC auth)
│   └── dbt-ci.yml                    # dbt slim CI (state:modified+, --empty, --defer)
│
├── demo_space/                      # HuggingFace Gradio demo Space (6 tabs: pass quality, pitch control, player similarity, shot map, DEFCON pressure, pass timing)
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
    │   ├── org-card.md               # HF Hub org card (source of truth)
    │   ├── org-interests.md          # HF Hub org "AI & ML interests" (paste via web UI)
    │   └── dataset-cards/            # HF Hub dataset cards (5 datasets)
    ├── huggingface-setup.md          # HuggingFace Hub integration guide (forks)
    └── plans/                        # Implementation design documents
```

---

## 6. Cross-Cutting Concerns

### 6.1 — Security

| Concern | Implementation |
|---------|---------------|
| Secrets management | No hardcoded credentials; OAuth M2M for app + Terraform; GitHub OIDC federation for CI (zero secrets) |
| Network | TLS everywhere; HTTPS-only for all data fetches |
| IAM | Least-privilege; separate service principals per workload |
| Data classification | Open-source data only (no PII); Unity Catalog ACLs applied |
| Audit | Unity Catalog audit logs; Terraform state versioning |
| Input validation | Regex on all user-supplied identifiers (`^[a-zA-Z_][a-zA-Z0-9_]*$`) |
| SSL verification | Explicit `verify=True` on all HTTP requests |
| Timeouts | `(10, 30)` connect/read on every HTTP call |
| Retry safety | Exponential backoff on transient errors (429/5xx); max 3 retries |
| Bandit compliance | Ruff S rules enforced; no eval/exec/pickle/shell=True |
| Content validation | Schema checks and non-empty assertions before every Delta write |
| Full audit | See [SECURITY.md](SECURITY.md) — 31 findings, 28 resolved, 3 accepted |

### 6.2 — Quality Standards

All code must pass these gates before merge:

| Check | Command | Threshold |
|-------|---------|-----------|
| Lint | `uv run ruff check src/` | Zero violations |
| Type check | `uv run pyright src/` | Zero errors (basic mode) |
| Unit tests | `uv run pytest src/tests/ -v` | All pass |
| Security scan | Ruff S (bandit) rules | Zero violations |
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
| Application | Taipy built-in metrics, Lakebase query logs |

### 6.5 — Testing Strategy

| Level | What | How |
|-------|------|-----|
| Unit | Ingestion logic, utility functions, analytics models | pytest (704 passed, incl. pytest-benchmark baselines) |
| Integration | dbt models compile and run | dbt slim CI (`state:modified+`, `--empty`, `--defer`) |
| Data quality | Row counts, value ranges, referential integrity | dbt tests (381) + dbt-expectations |
| E2E | Taipy pages render with real data | Manual smoke test |
| Infrastructure | Terraform validates | `terraform validate` + `terraform plan` |

### 6.6 — Database Performance

Lakebase and Databricks performance standards are codified in [CLAUDE.md § Database Performance](CLAUDE.md#database-performance). Key rules:

- **Lakebase (PG):** Index every filtered column on fact tables >100K rows. No `ON ONLY` indexes (partitioned tables). Avoid `SELECT DISTINCT` on large tables — use recursive CTE. Re-run `scripts/create_indexes.py` after every synced table recreation.
- **Databricks (Spark/dbt):** `validate_dataframe()` returns row count to `write_delta_table()` (no double `df.count()`), all writes use `replaceWhere` for idempotency, don't `.toPandas()` unbounded tables, extract repeated window functions into CTEs. All 14 mart fact tables use `liquid_clustered_by` for automatic data layout (replaced static Z-ordering). Predictive Optimization enabled at catalog level. Auto-compaction and `optimizeWrite` enabled via `+tblproperties` on all mart tables. All 17 mart models enforce dbt model contracts (`contract: {enforced: true}`, `on_schema_change: fail`).

Currently 41 btree indexes across 17 tables + 4 HNSW vector indexes on embedding tables (45 total) covering all Taipy query patterns. Managed by `scripts/create_indexes.py` with `ANALYZE` for planner statistics and `--verify` for EXPLAIN ANALYZE validation.

### 6.7 — Architecture Documentation

C4 diagrams are the single source of truth for architecture documentation, maintained as Structurizr DSL and regenerated automatically via `/final-review` before significant commits. See [§3.4](#34--c4-diagram-lifecycle).

---

## 7. Risk Register

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

### C. Dependencies on MCP CodeDeploy Project

| Dependency | Status |
|------------|--------|
| DevOpsAgent IAM role | Active — `AWS_PROFILE=devops-agent` (account 454762693631) |
| S3 state bucket | Active — `karstenskyt-terraform-state` with native S3 locking |

---

*This is a living document. Completed phase details are preserved in git history. See [ROADMAP.md](ROADMAP.md) for future research directions.*
