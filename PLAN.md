# Databricks Lakebase Implementation Plan — Soccer Analytics Platform

> **Status**: Phase 9 complete — 8 Streamlit pages, 155 unit tests, 225 dbt data tests, 10 synced tables.
> **Last Updated**: 2026-03-02
> **Repository**: [`karstenskyt/luxury-lakehouse`](https://github.com/karstenskyt/luxury-lakehouse)
> **Approach**: Professional-grade IaC, best practices, production-ready from day one

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Target Architecture](#2-target-architecture)
3. [C4 Architecture Model](#3-c4-architecture-model)
4. [Technology Decisions](#4-technology-decisions)
5. [Repository Structure](#5-repository-structure)
6. [Cross-Cutting Concerns](#6-cross-cutting-concerns)
7. [Completed Phases](#7-completed-phases)
8. [Future Work](#8-future-work)
9. [Risk Register](#9-risk-register)
10. [Appendices](#10-appendices)

---

## 1. Executive Summary

This plan implements the Databricks Lakebase architecture to build a serverless soccer analytics platform. The pipeline ingests open-source match data (StatsBomb, Metrica Sports, Wyscout), transforms it through a medallion architecture (Bronze → Silver → Gold), synchronizes curated tables into Lakebase (PostgreSQL 17), and serves a Streamlit dashboard for coaches and analysts.

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
┌──────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Open Source)                        │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────────────────┐  │
│  │  StatsBomb   │  │  Metrica Sports  │  │         Wyscout            │  │
│  │  (JSON API)  │  │  (CSV tracking)  │  │    (JSON events)           │  │
│  └──────┬───────┘  └───────┬──────────┘  └────────────┬───────────────┘  │
└─────────┼──────────────────┼──────────────────────────┼──────────────────┘
          │                  │                          │
          ▼                  ▼                          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              DATABRICKS SERVERLESS WORKFLOWS                             │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Python ingestion tasks on Serverless Compute (scheduled)       │    │
│  │  • statsbombpy → competitions, matches, events, lineups, 360   │    │
│  │  • requests → Metrica sample-data CSV + EPTS                    │    │
│  │  • requests → Wyscout public JSON datasets                      │    │
│  │  • spadl_vaep → SPADL conversion + VAEP scoring from bronze     │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG — BRONZE LAYER                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Delta Lake tables (raw, append-only, schema-on-read)           │    │
│  │  • statsbomb: competitions, matches, events, lineups, 360      │    │
│  │  • metrica: tracking, events                                    │    │
│  │  • wyscout: events, matches                                     │    │
│  │  • spadl: actions, action_values                                 │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          DATABRICKS SERVERLESS SQL + dbt                                 │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  dbt-databricks models on Serverless SQL Warehouse              │    │
│  │                                                                  │    │
│  │  SILVER (cleaned, typed, deduplicated):                         │    │
│  │  • stg_statsbomb__events, shots, matches, lineups, 360         │    │
│  │  • stg_metrica__tracking, events                                │    │
│  │  • stg_wyscout__events                                          │    │
│  │  • stg_spadl__action_values                                     │    │
│  │                                                                  │    │
│  │  GOLD (business logic, analytics-ready):                        │    │
│  │  • fct_shots, fct_passes, fct_player_stats, fct_match_summary  │    │
│  │  • fct_tracking_frames, fct_action_values, fct_player_embeddings│    │
│  │  • dim_players, dim_teams, dim_competitions                     │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              SYNCED TABLES — ZERO-ETL                                    │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Lakeflow Declarative Pipelines                                 │    │
│  │  Gold Delta tables → continuous async sync → Lakebase           │    │
│  │  (read-only PostgreSQL-queryable mirrors, sub-10ms latency)     │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          LAKEBASE AUTOSCALING (PostgreSQL 17)                            │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Serverless OLTP • Scale-to-zero • OAuth M2M auth               │    │
│  │  • Standard PostgreSQL wire protocol (JDBC/psycopg2)            │    │
│  │  • Native pgvector for future embedding search                  │    │
│  │  • Copy-on-write database branching for dev/test                │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          STREAMLIT APPLICATION                                           │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Deployed as Databricks App (serverless runtime)                │    │
│  │  • OAuth M2M auth (automatic token rotation, no passwords)      │    │
│  │  • Connects to Lakebase via psycopg2 (ThreadedConnectionPool)   │    │
│  │  • 8 pages: Shot Map, Pass Map, Heat Map, Pass Network,        │    │
│  │    Action Values, Player Radar, Match Summary, Pitch Control    │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. C4 Architecture Model

C4 diagrams (Context, Container, Component, Dynamic) are the standard deliverable for documenting this system's architecture. The Structurizr DSL source and generated HTML live in `docs/c4/`.

### 3.1 — Diagram Inventory

| Diagram Level | Name | Purpose |
|---------------|------|---------|
| **L1 — System Context** | Soccer Analytics Platform | Platform in its environment: users, data providers, Databricks boundary |
| **L2 — Container** | Platform Containers | Ingestion Workflows, Unity Catalog, SQL Warehouse, dbt, Lakebase, Synced Tables, Streamlit |
| **L3 — Component** | Ingestion Service | StatsBomb/Metrica/Wyscout fetchers, SPADL adapter, shared utilities, Delta writer |
| **L3 — Component** | dbt Transformation | Staging, intermediate, mart models, macros, test suite |
| **L3 — Component** | Streamlit Application | Page modules, filter components, chart components, Lakebase connection pool |
| **L4 — Dynamic** | Data Flow | End-to-end: API fetch → Bronze → dbt → Gold → Synced Table → Lakebase → Streamlit |
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
  │   Responsibility: Low-latency OLTP queries for the Streamlit app
  │
  └── Streamlit Dashboard          [Databricks App]
      Technology: Python + Streamlit + mplsoccer + psycopg2
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
| **Terraform modules** | Separate modules per concern: workspace, catalog, lakebase, workflows, sql_warehouse, synced_tables, app, service_principals, github_oidc, state_kms |
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

---

## 5. Repository Structure

```
luxury-lakehouse/
│
├── PLAN.md                           # This document
├── CLAUDE.md                         # AI assistant instructions
├── README.md                         # Project overview
├── SECURITY.md                       # Security audit report
├── TODO.md                           # Forward-looking action items
├── .pre-commit-config.yaml           # Pre-commit hooks (ruff, terraform_fmt, detect-secrets)
├── pyproject.toml                    # Python project metadata (uv)
├── uv.lock                           # Deterministic dependency lock
│
├── assets/                           # Images and branding
│   └── luxury-lakehouse.jpg
│
├── terraform/
│   ├── environments/dev/             # Dev environment composition
│   ├── modules/
│   │   ├── workspace/                # Unity Catalog
│   │   ├── catalog/                  # Bronze, Silver, Gold schemas + grants
│   │   ├── lakebase/                 # Lakebase Autoscaling (PG 17)
│   │   ├── sql_warehouse/            # Serverless SQL Warehouse
│   │   ├── workflows/                # Ingestion job definitions
│   │   ├── synced_tables/            # Gold → Lakebase sync (10 tables)
│   │   ├── app/                      # Databricks App (Streamlit)
│   │   ├── service_principals/       # Ingestion SP, App SP, CI SP + federation
│   │   ├── github_oidc/              # AWS IAM OIDC provider + scoped role
│   │   └── state_kms/                # KMS CMK for Terraform state encryption
│   └── shared/
│       ├── versions.tf               # Provider version constraints
│       └── tags.tf                   # Standard resource tagging
│
├── src/
│   ├── ingestion/
│   │   ├── statsbomb.py              # StatsBomb API ingestion (5 bronze tables + 360 backfill)
│   │   ├── metrica.py                # Metrica CSV + EPTS ingestion (Games 1-3)
│   │   ├── wyscout.py                # Wyscout JSON ingestion
│   │   ├── spadl_adapter.py          # Bronze-to-socceraction format adapters
│   │   ├── spadl_vaep.py             # SPADL conversion + VAEP scoring pipeline
│   │   └── utils.py                  # Shared CLI, logging, HTTP, Delta helpers
│   │
│   ├── streamlit_app/
│   │   ├── app.py                    # Entrypoint: st.navigation, page routing
│   │   ├── config.py                 # Pydantic BaseSettings
│   │   ├── db.py                     # OAuth M2M, ThreadedConnectionPool, parameterized queries
│   │   ├── pages/                    # 8 pages (+ player_search.py planned)
│   │   └── components/               # filters.py, pitch.py, charts.py
│   │
│   └── tests/
│       ├── test_ingestion.py
│       └── test_streamlit.py
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # SILVER: statsbomb/, metrica/, wyscout/, spadl/
│   │   ├── intermediate/             # Cross-source joins (ephemeral)
│   │   └── marts/                    # GOLD: 7 fact + 3 dimension tables
│   ├── tests/                        # Custom data tests
│   ├── macros/                       # distance_to_goal, shot_angle
│   └── seeds/                        # competition_metadata.csv, position_mapping.csv
│
├── .github/workflows/
│   ├── python-ci.yml                 # ruff + pyright + pytest
│   ├── terraform-plan.yml            # Plan on PR (OIDC auth)
│   └── dbt-ci.yml                    # dbt build --target ci
│
└── docs/c4/
    ├── architecture.dsl              # Structurizr DSL source
    └── architecture.html             # Generated: self-contained HTML
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
| Application | Streamlit built-in metrics, Lakebase query logs |

### 6.5 — Testing Strategy

| Level | What | How |
|-------|------|-----|
| Unit | Ingestion logic, utility functions | pytest (155 tests) |
| Integration | dbt models compile and run | `dbt build --target ci` |
| Data quality | Row counts, value ranges, referential integrity | dbt tests (225) + dbt-expectations |
| E2E | Streamlit pages render with real data | Manual smoke test |
| Infrastructure | Terraform validates | `terraform validate` + `terraform plan` |

### 6.6 — Architecture Documentation

C4 diagrams are the single source of truth for architecture documentation, maintained as Structurizr DSL and regenerated automatically via `/final-review` before significant commits. See [§3.4](#34--c4-diagram-lifecycle).

---

## 7. Completed Phases

| Phase | Description | Key Deliverables |
|-------|-------------|------------------|
| **0** | Foundation & Prerequisites | Workspace provisioned (Premium, us-east-1), Terraform state backend (S3 + native locking), repo initialized with uv + ruff + pyright + pre-commit, initial C4 diagrams |
| **1** | Serverless Infrastructure | 7 Terraform modules (workspace, catalog, sql_warehouse, lakebase, workflows, synced_tables, app), 27+ resource files |
| **2** | Data Ingestion | 3 ingestion modules → 9 bronze tables, 31.4M+ rows; shared utils with CLI, logging, HTTP, Delta helpers; 55 unit tests |
| **3** | Transformation (dbt) | 20 dbt models (staging → intermediate → marts), 225 data tests, position_mapping seed, dbt-expectations range tests |
| **4** | Zero-ETL Synchronization | 10 synced tables (Gold Delta → Lakebase PG), snapshot scheduling, sub-10ms OLTP queries |
| **5** | Streamlit Application | 4 initial pages (Shot Map, Pass Map, Player Radar, Match Summary), OAuth M2M auth, ThreadedConnectionPool, deployed as Databricks App |
| **5.5** | Lakebase Autoscaling Migration | PG 16 → PG 17, `databricks_postgres_project` + endpoint, scale-to-zero (0.5–4 CU), `sslmode=require` |
| **5.6** | IAM OIDC + OAuth M2M + KMS | PAT → OAuth M2M for local dev, GitHub OIDC federation for secretless CI, KMS CMK for state encryption |
| **6** | StatsBomb 360 Freeze Frames | `backfill_360()` entry point, `stg_statsbomb__360` staging model, 15.58M rows across 323 matches |
| **7** | Metrica Game 3 + Pitch Control | EPTS format parsers (XML metadata + colon-delimited tracking + JSON events), ball coordinate fix, Voronoi pitch control page, 107 tests |
| **8** | Heat Map + Pass Network | `pass_recipient_id` through dbt pipeline, Heat Map page (action density), Pass Network page (graph viz), synced table schema migration, 118 tests |
| **9** | SPADL/VAEP Action Valuation | "Fetch Once, Fork Twice" — `spadl_adapter.py` + `spadl_vaep.py`, socceraction + XGBoost, 9.5M VAEP-scored actions, Action Values page (3 views), Player Radar VAEP/90, 155 tests |

### Key Design Decisions (from completed phases)

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

### Synced Tables (Gold → Lakebase)

| Source Table | Synced Table | Primary Key | Rows |
|-------------|-------------|-------------|------|
| `dev_gold.fct_shots` | `fct_shots_synced` | `shot_id` | 131,077 |
| `dev_gold.fct_passes` | `fct_passes_synced` | `pass_id` | 5,052,415 |
| `dev_gold.fct_player_stats` | `fct_player_stats_synced` | `player_stats_id` | 19,664 |
| `dev_gold.fct_match_summary` | `fct_match_summary_synced` | `match_id` | 3,464 |
| `dev_gold.fct_tracking_frames` | `fct_tracking_frames_synced` | `tracking_frame_id` | 9,464,895 |
| `dev_gold.fct_action_values` | `fct_action_values_synced` | `action_value_id` | ~9,500,000 |
| `dev_gold.fct_player_embeddings` | `fct_player_embeddings_synced` | `embedding_id` | 0 |
| `dev_gold.dim_players` | `dim_players_synced` | `player_id` | 10,803 |
| `dev_gold.dim_teams` | `dim_teams_synced` | `team_id` | 453 |
| `dev_gold.dim_competitions` | `dim_competitions_synced` | `competition_id` | 21 |

### Synced Tables: Implementation Notes

- `scheduling_policy = "SNAPSHOT"` — initial sync with on-demand refresh
- `logical_database_name = "databricks_postgres"` — standard Lakebase database
- **Autoscaling workaround (provider v1.110.0):** `databricks_database_synced_database_table` only supports `database_instance_name` (Provisioned). Synced tables targeting Autoscaling projects must be created via Databricks UI, then imported into Terraform. `lifecycle { ignore_changes = all }` prevents drift. This applies to any new synced table.
- **Schema changes:** Must delete synced table, drop ghost PG table, recreate via API, re-import into Terraform.
- **Credential API:** REST endpoint is `/api/2.0/postgres/credentials` (NOT `/api/2.0/database/credentials`).

### Streamlit App

| Page | Visualization | Data Source |
|------|--------------|-------------|
| Shot Map | Half-pitch shots sized by xG, colored by outcome | `fct_shots_synced` |
| Pass Map | Full pitch arrows, progressive pass highlighting | `fct_passes_synced` |
| Player Radar | Per-90 metrics comparison (1-3 players), incl. VAEP/90 | `fct_player_stats_synced` |
| Match Summary | Scorecard + xG metrics + horizontal bar chart | `fct_match_summary_synced` |
| Pitch Control | Voronoi tessellation from tracking data | `fct_tracking_frames_synced` |
| Heat Map | Action density per player/team/match | `fct_passes_synced`, `fct_shots_synced` |
| Pass Network | Player-to-player graph with scaled nodes/edges | `fct_passes_synced` |
| Action Values | VAEP rankings, action type breakdown, timeline | `fct_action_values_synced`, `fct_player_stats_synced` |

---

## 8. Future Work

### 8.1 — Phase 10: Additional Public Tracking Datasets

**Status:** Planned. Expands beyond Metrica's 3 sample matches.

| Dataset | Matches | Tracking Format | Frame Rate | License |
|---------|---------|----------------|------------|---------|
| **Bundesliga IDSSE** | 7 | TRACAB (x,y,z,speed,accel) | 25fps | CC-BY 4.0 (Nature Scientific Data, Feb 2025) |
| **SkillCorner Open Data** | 10 | JSONL (broadcast-derived) | 10fps | Public repo |
| **Metrica Sports** | 3 | CSV + EPTS | 25fps | Public repo (already ingested) |

**Kloppy integration:** Use Kloppy (v3.18+) as a universal loader rather than per-provider parsers. Kloppy handles coordinate normalization, player identity mapping, and format-specific quirks. A single `src/ingestion/kloppy_loader.py` module could replace provider-specific tracking parsers.

**Architecture decision:** Normalize all providers to the Metrica bronze schema in ingestion (Kloppy handles this). Maintains backward compatibility with existing `stg_metrica__tracking` → `fct_tracking_frames` pipeline.

### 8.2 — Phase 11: Physics-Based Pitch Control Model

**Status:** Planned. Replaces the current Voronoi approximation.

The Phase 7 Pitch Control page uses Voronoi tessellation — a geometric approximation. A physics-based model (Spearman et al., 2017; Fernandez & Bornn, 2018) accounts for player velocity, acceleration, and time-to-intercept to produce continuous pitch control probabilities.

**Implementation:**
1. Per-cell time-to-intercept based on position, velocity, max acceleration
2. Convert to control probability using logistic function
3. Sum team probabilities → continuous pitch control surface [0, 1]
4. Populate `pitch_control_value` in `fct_tracking_frames` (currently NULL)
5. Update Streamlit with continuous heatmap overlay alongside Voronoi

**Data requirements:** Tracking data with positions and velocities — available from Metrica (Phase 7) and IDSSE/SkillCorner (Phase 10).

### 8.3 — Future Data Sources

| Source | Data Type | Status | Notes |
|--------|-----------|--------|-------|
| **Respo.Vision** | 3D pose tracking from broadcast video | Planned | User pursuing via network; skeletal keypoints at 25fps |
| **Wyscout match metadata** | Match details (formations, coaches, venue) | Deferred | Not in public Figshare dataset |

Each new source follows the established pattern: `src/ingestion/<source>.py` → Bronze → dbt staging/marts → Synced Tables → Lakebase.

### 8.4 — Phase 12: Movement Analysis

**Status:** Planned. Physical and tactical movement analysis from tracking data.

| Tier | Scope | Data Source | Dependencies |
|------|-------|-------------|-------------|
| **Tier 1: Event-data proxies** | PPDA, pressure event analysis | StatsBomb + Wyscout events (all matches) | None — feasible now |
| **Tier 2: Physical performance** | Distance, HSR, sprints, accelerations, metabolic power | Tracking data (Metrica + IDSSE + SkillCorner) | Phase 10 |
| **Tier 3: Off-ball spatial** | Off-ball xT, space creation, OBSO | Tracking + pitch control surface | Phase 10 + Phase 11 |

**Key libraries:**
- [`floodlight`](https://github.com/floodlight-sports/floodlight) (v1.1+, MIT) — Full kinematics pipeline
- [`databallpy`](https://github.com/Alek050/databallpy) (MIT) — Gaussian space model, pressure calculation
- [`kloppy`](https://github.com/PySport/kloppy) (BSD-3) — Data loading/normalization (Phase 10)

**Key references:** Spearman 2018 (OBSO), Fernandez & Bornn 2018 ("Wide Open Spaces"), Soccermatics Lesson 7 (off-ball xT), Kempe et al. 2024 (movement analysis framework).

### 8.5 — Phase 13: Cross-Source Player Entity Resolution

`dim_players` currently deduplicates within each source, but StatsBomb, Metrica, and Wyscout use independent player IDs. The same player exists as separate rows per source.

**Planned approach:** [`parmacalcio1913/players-matcher`](https://github.com/parmacalcio1913/players-matcher) for fuzzy matching. **License blocker:** No license as of 2026-02-28 (all rights reserved). Fallback: implement with `rapidfuzz`.

**Integration path:** `int_player_xref` mapping → refactor `dim_players` → downstream tables automatically benefit.

### 8.6 — Phase 14: pgvector Player Embeddings

`fct_player_embeddings` and its synced table are provisioned (0 rows). Design feature vector from `fct_player_stats` per-90 metrics, generate embeddings, enable pgvector `<=>` cosine distance queries. Depends on Phase 13 for cross-source identity (within-source feasible without it).

### 8.7 — Phase 15: Player Similarity Streamlit Page

pgvector nearest-neighbor search (`player_search.py`). "Find players like X." Depends on Phase 14.

### 8.8 — Phase 16: DEFCON-Inspired Defensive Valuation

**Paper:** Kim, H.S. et al. (2025). "Better Prevent than Tackle: Valuing Defense in Soccer Based on Graph Neural Networks." *arXiv:2512.10355*.

**License warning:** [`hyunsungkim-ds/defcon`](https://github.com/hyunsungkim-ds/defcon) has no LICENSE file. Implementation must use paper equations and standard open-source libraries only.

**Tiered approach:**

| Tier | Approach | Data Needed | Feasibility |
|------|----------|-------------|-------------|
| **Tier 1: VAEP** | SPADL + VAEP scoring | Events only (3,000+ matches) | **Complete** (Phase 9) |
| **Tier 2: Pitch Control** | Physics-based model | Tracking (3–20 matches) | Phase 11 |
| **Tier 3: DEFCON-lite** | Tabular model (VAEP + spatial features, no GNN) | ~20 matches with tracking | Feasible after Phase 10 |
| **Tier 4: Full GNN DEFCON** | Graph Attention Networks on player graphs | 500+ matches with tracking | Requires commercial data |

### 8.9 — Additional Streamlit Pages

| Page | Description | Dependencies |
|------|-------------|-------------|
| **Movement Analysis** | Speed heatmaps, distance profiles, sprint timelines, pressing intensity | Phase 12 |
| **Player Similarity** | pgvector nearest-neighbor search | Phase 14 + 15 |

---

## 9. Risk Register

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| R1 | Lakebase Terraform provider support incomplete | Medium | High | **Realized**: Provider v1.110.0 lacks project/branch fields for synced tables. Mitigated via UI creation + `terraform import` + `lifecycle { ignore_changes = all }`. |
| R2 | Synced Tables feature not in Databricks tier | Low | Critical | Confirmed: Premium tier supports Synced Tables. |
| R3 | StatsBomb API rate limiting during bulk ingestion | Medium | Low | Exponential backoff; local caching; off-peak scheduling. |
| R4 | Databricks cost exceeds expectations | Medium | Medium | Scale-to-zero everywhere; billing alerts; monthly review. |
| R5 | dbt-databricks adapter incompatibility | Low | Medium | Tested in Phase 3; no issues encountered. |
| R6 | Streamlit performance with mplsoccer | Medium | Low | Figure caching; pre-computed static images for common views. |
| R7 | Unity Catalog ACL complexity | Low | Low | Single admin user for dev; RBAC deferred to prod. |
| R8 | DEFCON repo has no license | High | Medium | Reimplement from paper equations; use open-source libraries only. |
| R9 | `players-matcher` has no license | High | Medium | Request from maintainer. Fallback: `rapidfuzz`. |
| R10 | Public tracking data insufficient for GNN training | High | High | Tiers 1–3 feasible with public data. Full GNN deferred. |

---

## 10. Appendices

### A. Data Volume Estimates

| Source | Matches | Events/Match | Rows (Bronze) | Size Estimate |
|--------|---------|-------------|----------------|---------------|
| StatsBomb (open) | ~3,000 | ~3,400 | ~10.2M events | ~500 MB Parquet |
| Metrica (sample) | 3 | 135,000 frames | ~405K frames | ~15 MB Parquet |
| Wyscout (public) | ~1,900 | ~1,800 | ~3.4M events | ~400 MB Parquet |
| Bundesliga IDSSE (planned) | 7 | ~135,000 frames | ~1M frames | ~200 MB TRACAB |
| SkillCorner (planned) | 10 | ~54,000 frames | ~540K frames | ~100 MB JSONL |
| **Total Bronze** | | | **~31.4M rows** | **~1 GB Parquet** |
| **Projected (with new sources)** | | | **~33M rows** | **~1.3 GB Parquet** |

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
| 10 | Streamlit web app | `src/streamlit_app/` |
| — | SPADL action valuation (VAEP) | `fct_action_values` (Phase 9) |
| — | Movement analysis | Phase 12 — PPDA, physical metrics, off-ball xT |
| — | Defensive contribution (DEFCON) | Phase 16 — EPV decomposition + credit assignment |

### C. Dependencies on MCP CodeDeploy Project

| Dependency | Status |
|------------|--------|
| DevOpsAgent IAM role | Active — `AWS_PROFILE=devops-agent` (account 454762693631) |
| S3 state bucket | Active — `karstenskyt-terraform-state` with native S3 locking |

---

*This plan is a living document. Completed phase details are preserved in git history. Future phases are designed to be self-contained with clear inputs, outputs, and verification criteria.*
