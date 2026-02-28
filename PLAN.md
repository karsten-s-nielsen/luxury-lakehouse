# Databricks Lakebase Implementation Plan — Soccer Analytics Platform

> **Status**: Phase 5.5 complete — Lakebase migrated to Autoscaling (PG 17, scale-to-zero). Streamlit dashboard deployed as Databricks App with 4 pages, backed by Lakebase PostgreSQL via OAuth M2M
> **Last Updated**: 2026-02-28
> **Repository**: [`karstenskyt/luxury-lakehouse`](https://github.com/karstenskyt/luxury-lakehouse)
> **Scope**: Document 3 ("3_AWS Lake House.pdf") — Databricks Lakebase serverless architecture
> **Approach**: Professional-grade IaC, best practices, production-ready from day one

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Current State Assessment](#2-current-state-assessment)
3. [Target Architecture](#3-target-architecture)
4. [C4 Architecture Model](#4-c4-architecture-model)
5. [Technology Decisions](#5-technology-decisions)
6. [Repository Structure](#6-repository-structure)
7. [Phase 0 — Foundation & Prerequisites](#7-phase-0--foundation--prerequisites)
8. [Phase 1 — Serverless Infrastructure (IaC)](#8-phase-1--serverless-infrastructure-iac)
9. [Phase 2 — Data Ingestion](#9-phase-2--data-ingestion)
10. [Phase 3 — Transformation (dbt)](#10-phase-3--transformation-dbt)
11. [Phase 4 — Zero-ETL Synchronization](#11-phase-4--zero-etl-synchronization)
12. [Phase 5 — Application Deployment (Streamlit)](#12-phase-5--application-deployment-streamlit)
13. [Cross-Cutting Concerns](#13-cross-cutting-concerns)
14. [Future Data Sources](#14-future-data-sources)
15. [Risk Register](#15-risk-register)
16. [Appendices](#16-appendices)

---

## 1. Executive Summary

This plan implements the Databricks Lakebase architecture described in Document 3 to build a serverless soccer analytics platform. The pipeline ingests open-source match data (StatsBomb, Metrica Sports, Wyscout), transforms it through a medallion architecture (Bronze → Silver → Gold), synchronizes curated tables into Lakebase (PostgreSQL 17), and serves a Streamlit dashboard for coaches and analysts.

**Why Lakebase over Traditional AWS (Document 2)?**

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

## 2. Current State Assessment

### What Exists

| Asset | Location | Status |
|-------|----------|--------|
| Architecture research (Document 1) | `documents/1_AWS Lake House.pdf` | Complete — 8-page comparative analysis |
| Traditional AWS plan (Document 2) | `documents/2_AWS Lake House.pdf` | Complete — 5-phase implementation guide |
| Lakebase plan (Document 3) | `documents/3_AWS Lake House.pdf` | Complete — 5-phase implementation guide (THIS plan implements it) |
| Architecture diagram | `documents/Screenshot_20251212_222944_Chrome.jpg` | Reference — original pipeline by "Matteo" |
| Soccermatics local workspace | `D:/Development/soccermatics/` | Working — 25/25 scripts pass (Python 3.12, conda) |
| MCP AWS CodeDeploy server | `D:/Development/karstenskyt__mcp-aws-codedeploy/` | Working — 8 tools, FastMCP, Stdio transport |
| AWS IAM DevOpsAgent role spec | `karstenskyt__mcp-aws-codedeploy/TODO.md` | Documented — policy template ready |
| Implementation code | This repository | **Phase 5 complete** — 4 ingestion modules, 83 unit tests, 9 bronze tables (31.4M rows); 19 dbt models, 165 data tests; Streamlit dashboard (4 pages); security audit complete |

### Soccermatics Workspace Details

The local workspace at `D:/Development/soccermatics/` was set up in a previous Claude session (2026-02-07). Key facts:

- **Not a git clone** — reorganized from `github.com/soccermatics/Soccermatics` into chapter-based layout
- **25/25 scripts verified passing** on Windows 11 + Python 3.12 + conda
- **Data bundled locally**: StatsBomb (auto-fetch via API), Wyscout (bundled JSON), Metrica (bundled CSV)
- **Chapter 10 (Web App)**: Requires paid Twelve API key — not runnable, but Streamlit pattern is the target
- **TensorFlow disabled** (Ch 07): CUDA 12.8 + Win + Py 3.12 incompatibility — gated behind flag
- **Dependencies**: mplsoccer, kloppy, statsbombpy, xgboost, streamlit, umap-learn, plus standard scientific stack

This workspace serves as the **reference implementation** — the analytics logic from these scripts will be ported into dbt models and the Streamlit app.

---

## 3. Target Architecture

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
│              DATABRICKS SERVERLESS WORKFLOWS (Phase 2)                   │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Python ingestion tasks on Serverless Compute (scheduled)       │    │
│  │  • statsbombpy → competitions, matches, events, lineups, 360   │    │
│  │  • requests → Metrica sample-data CSV                           │    │
│  │  • requests → Wyscout public JSON datasets                      │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG — BRONZE LAYER                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Delta Lake tables (raw, append-only, schema-on-read)           │    │
│  │  • bronze.statsbomb_competitions                                │    │
│  │  • bronze.statsbomb_events                                      │    │
│  │  • bronze.statsbomb_lineups                                     │    │
│  │  • bronze.statsbomb_matches                                     │    │
│  │  • bronze.statsbomb_three_sixty                                 │    │
│  │  • bronze.metrica_tracking                                      │    │
│  │  • bronze.metrica_events                                        │    │
│  │  • bronze.wyscout_events                                        │    │
│  │  • bronze.wyscout_matches                                       │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          DATABRICKS SERVERLESS SQL + dbt (Phase 3)                       │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  dbt-databricks models on Serverless SQL Warehouse              │    │
│  │                                                                  │    │
│  │  SILVER (cleaned, typed, deduplicated):                         │    │
│  │  • silver.stg_statsbomb__events (flattened JSON)                │    │
│  │  • silver.stg_statsbomb__shots (shot-specific columns)          │    │
│  │  • silver.stg_statsbomb__matches (competition/season metadata)  │    │
│  │  • silver.stg_statsbomb__lineups (player positions per match)   │    │
│  │  • silver.stg_metrica__events (scaled coordinates)              │    │
│  │  • silver.stg_metrica__tracking (parsed coordinates)            │    │
│  │  • silver.stg_wyscout__events (normalized schema)               │    │
│  │                                                                  │    │
│  │  GOLD (business logic, analytics-ready):                        │    │
│  │  • gold.fct_shots (xG features: distance, angle, body_part)    │    │
│  │  • gold.fct_passes (pass networks, progressive passes)         │    │
│  │  • gold.fct_player_stats (per-90 metrics, radar chart data)    │    │
│  │  • gold.fct_match_summary (possession, xG, xT totals)         │    │
│  │  • gold.fct_tracking_frames (pitch control, velocities)        │    │
│  │  • gold.dim_players / dim_teams / dim_competitions             │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│              SYNCED TABLES — ZERO-ETL (Phase 4)                          │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Lakeflow Spark Declarative Pipelines                           │    │
│  │  Gold Delta tables → continuous async sync → Lakebase           │    │
│  │  (read-only PostgreSQL-queryable mirrors, sub-10ms latency)     │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          LAKEBASE AUTOSCALING (PostgreSQL 17) — Phase 4                  │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Serverless OLTP • Scale-to-zero • OAuth M2M auth               │    │
│  │  • Standard PostgreSQL wire protocol (JDBC/psycopg2)            │    │
│  │  • Native pgvector for future embedding search                  │    │
│  │  • Copy-on-write database branching for dev/test                │    │
│  └──────────────────────────┬───────────────────────────────────────┘    │
└─────────────────────────────┼────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│          STREAMLIT APPLICATION — Phase 5                                 │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  Deployed as Databricks App (databricks_app resource)           │    │
│  │  • OAuth M2M auth (automatic token rotation, no passwords)      │    │
│  │  • Connects to Lakebase via psycopg2 / SQLAlchemy               │    │
│  │  • mplsoccer visualizations (shots, passes, heat maps, radars) │    │
│  │  • Interactive filters: competition, season, team, player       │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. C4 Architecture Model

C4 diagrams (Context, Container, Component, Dynamic) are the standard deliverable for documenting this system's architecture. We use the `/c4` skill (Structurizr DSL → rendered SVGs in a self-contained HTML file) to produce and maintain these diagrams as living documentation.

### 4.1 — Diagram Inventory

The following C4 diagrams will be generated and maintained throughout implementation. Each diagram is regenerated via `/final-review` before any commit to ensure architecture docs stay synchronized with code.

| Diagram Level | Name | Purpose | Generated At |
|---------------|------|---------|-------------|
| **L1 — System Context** | Soccer Analytics Platform | Shows the platform in its environment: users (coaches, analysts, data scientists), external data providers (StatsBomb, Metrica, Wyscout), and the Databricks platform boundary | Phase 0 (initial), updated every phase |
| **L2 — Container** | Platform Containers | Shows the major runtime containers: Ingestion Workflows, Unity Catalog (Bronze/Silver/Gold), Serverless SQL Warehouse, dbt Project, Lakebase PostgreSQL, Synced Tables pipeline, Streamlit App | Phase 1 (after IaC), updated as containers are provisioned |
| **L3 — Component** | Ingestion Service | Zooms into the ingestion container: StatsBomb fetcher, Metrica fetcher, Wyscout fetcher, shared utilities, Delta writer | Phase 2 |
| **L3 — Component** | dbt Transformation | Zooms into dbt: staging models, intermediate models, mart models, custom macros, test suite | Phase 3 |
| **L3 — Component** | Streamlit Application | Zooms into the app: page modules, filter components, chart components, Lakebase connection pool | Phase 5 |
| **L4 — Dynamic** | Data Flow: Ingestion to Dashboard | Shows the end-to-end runtime sequence: API fetch → Bronze write → dbt transform → Gold table → Synced Table sync → Lakebase query → Streamlit render | Phase 4 (once pipeline is end-to-end) |
| **L4 — Dynamic** | Zero-ETL Sync | Shows the Synced Tables mechanism: Gold Delta table change → Lakeflow pipeline trigger → Lakebase mirror update | Phase 4 |
| **Deployment** | AWS + Databricks Infrastructure | Maps containers to actual Databricks/AWS resources: workspace, serverless compute, S3 storage, Lakebase instance, Databricks App runtime | Phase 1 |

### 4.2 — C4 Model: Persons & External Systems

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

### 4.3 — C4 Model: Containers

```
System Boundary: Soccer Analytics Platform (Databricks on AWS)
  │
  ├── Ingestion Workflows          [Databricks Serverless Compute]
  │   Technology: Python + statsbombpy + requests
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
  │   Technology: PostgreSQL 17, Autoscaling, pgvector
  │   Responsibility: Low-latency OLTP queries for the Streamlit app
  │
  └── Streamlit Dashboard          [Databricks App]
      Technology: Python + Streamlit + mplsoccer + psycopg2
      Responsibility: Interactive analytics UI for coaches/analysts
```

### 4.4 — C4 Diagram Lifecycle

The `/c4` skill (already installed locally with Java 21+) generates a self-contained HTML file with embedded SVGs and tabbed navigation across all diagram levels. It uses Structurizr DSL as the source notation — the industry standard C4 toolchain by Simon Brown. The `/final-review` skill includes C4 regeneration as part of its pre-commit quality gate.

**Toolchain status:** `/c4` and `/final-review` skills installed and operational. Java 21+ present. No additional setup required.

**Workflow:**
1. Architecture changes are made (new module, new service, changed data flow)
2. Update the Structurizr DSL source in `docs/c4/` (or let `/c4` regenerate from codebase analysis)
3. Run `/final-review` before commit → C4 diagrams regenerated automatically
4. Generated HTML committed alongside code changes

**File locations:**
```
docs/
├── c4/
│   ├── architecture.dsl           # Structurizr DSL source (the model)
│   └── architecture.html          # Generated: self-contained HTML with all diagrams
└── architecture-decision-records/
    └── ...
```

### 4.5 — Relationship to Original Architecture Diagram

The original `documents/Screenshot_20251212_222944_Chrome.jpg` (Matteo's "Our Data Architecture" diagram) represents the **traditional AWS pipeline** from Document 2. Our C4 diagrams replace this informal screenshot with:

- A formal **System Context** showing the same actors and boundaries
- A **Container** diagram that maps each node in Matteo's diagram to its Lakebase equivalent
- **Component** and **Dynamic** diagrams that go deeper than the original ever could

This is the architectural documentation upgrade from ad-hoc screenshots to industry-standard C4 notation.

---

## 5. Technology Decisions

### IaC: Terraform (not CloudFormation/CDK)

| Decision | Rationale |
|----------|-----------|
| **Terraform** with Databricks provider | Official `databricks/databricks` provider; multi-cloud; state management via S3+DynamoDB already spec'd in MCP CodeDeploy project |
| **Terraform modules** | Separate modules per concern (workspace, catalog, lakebase, workflows, app) |
| **Remote state** | S3 backend with DynamoDB locking (consistent with existing IAM role design) |
| **Terragrunt** (evaluate) | DRY configuration across dev/staging/prod environments — decide during Phase 0 |

### Python Tooling

| Tool | Purpose | Why |
|------|---------|-----|
| **uv** | Package management, virtual envs | Fast, deterministic, replaces pip+venv+pip-tools |
| **ruff** | Linting + formatting | Single tool replaces flake8+black+isort, 10-100x faster |
| **pyright** | Type checking | Best-in-class for Python, integrates with LSP |
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
| **GitHub Actions** | CI/CD pipeline |
| **terraform plan** on PR | Infrastructure change review |
| **dbt build --target ci** on PR | Model validation |
| **Environments** | dev → staging → prod promotion |

---

## 5. Repository Structure

```
luxury-lakehouse/
│
├── PLAN.md                           # This document
├── CLAUDE.md                         # AI assistant instructions
├── README.md                         # Project overview and quickstart
├── .gitignore
├── .pre-commit-config.yaml           # Pre-commit hooks configuration
├── pyproject.toml                    # Python project metadata (uv)
├── uv.lock                           # Deterministic dependency lock
│
├── documents/                        # Reference PDFs and architecture diagram
│   ├── 1_AWS Lake House.pdf
│   ├── 2_AWS Lake House.pdf
│   ├── 3_AWS Lake House.pdf
│   └── Screenshot_20251212_222944_Chrome.jpg
│
├── terraform/                        # Infrastructure as Code
│   ├── environments/
│   │   └── dev/                      # Dev-only for now (add prod later)
│   │       ├── main.tf              # Dev environment composition
│   │       ├── variables.tf
│   │       ├── terraform.tfvars      # Dev-specific values (gitignored)
│   │       └── backend.tf            # S3 state backend (dev key)
│   │
│   ├── modules/
│   │   ├── workspace/                # Databricks workspace + Unity Catalog
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   │
│   │   ├── lakebase/                 # Lakebase Autoscaling Project
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   │
│   │   ├── catalog/                  # Unity Catalog schemas (bronze, silver, gold)
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   │
│   │   ├── workflows/                # Databricks Workflows (ingestion jobs)
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   │
│   │   ├── sql_warehouse/            # Serverless SQL Warehouse for dbt
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   │
│   │   ├── synced_tables/            # Lakeflow Synced Tables (Gold → Lakebase)
│   │   │   ├── main.tf
│   │   │   ├── variables.tf
│   │   │   └── outputs.tf
│   │   │
│   │   └── app/                      # Databricks App (Streamlit hosting)
│   │       ├── main.tf
│   │       ├── variables.tf
│   │       └── outputs.tf
│   │
│   └── shared/
│       ├── versions.tf               # Provider version constraints
│       └── tags.tf                    # Standard resource tagging
│
├── src/                              # Python source code
│   ├── ingestion/                    # Phase 2: Data ingestion scripts
│   │   ├── __init__.py
│   │   ├── statsbomb.py             # StatsBomb API ingestion
│   │   ├── metrica.py               # Metrica Sports CSV ingestion
│   │   ├── wyscout.py               # Wyscout JSON ingestion
│   │   └── utils.py                  # Shared ingestion utilities
│   │
│   ├── streamlit_app/               # Phase 5: Streamlit dashboard
│   │   ├── __init__.py
│   │   ├── app.py                   # Main Streamlit entrypoint
│   │   ├── pages/
│   │   │   ├── shots.py             # Shot maps and xG analysis
│   │   │   ├── passes.py            # Pass networks and progressive passes
│   │   │   ├── player_radar.py      # Player comparison radar charts
│   │   │   ├── match_summary.py     # Match overview dashboard
│   │   │   ├── pitch_control.py     # Tracking data visualizations
│   │   │   └── player_search.py    # pgvector similarity search
│   │   ├── components/
│   │   │   ├── filters.py           # Reusable filter sidebar
│   │   │   └── charts.py            # mplsoccer chart wrappers
│   │   └── db.py                    # Lakebase connection (OAuth M2M)
│   │
│   └── tests/
│       ├── test_ingestion.py
│       └── test_streamlit.py
│
├── dbt_project/                      # Phase 3: dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml                  # Connection profiles (gitignored)
│   ├── packages.yml                  # dbt packages (dbt-expectations, etc.)
│   │
│   ├── models/
│   │   ├── staging/                  # SILVER layer (1:1 source cleaning)
│   │   │   ├── statsbomb/
│   │   │   │   ├── _statsbomb__sources.yml
│   │   │   │   ├── _statsbomb__models.yml
│   │   │   │   ├── stg_statsbomb__events.sql
│   │   │   │   ├── stg_statsbomb__shots.sql
│   │   │   │   ├── stg_statsbomb__lineups.sql
│   │   │   │   └── stg_statsbomb__matches.sql
│   │   │   ├── metrica/
│   │   │   │   ├── _metrica__sources.yml
│   │   │   │   ├── _metrica__models.yml
│   │   │   │   ├── stg_metrica__tracking.sql
│   │   │   │   └── stg_metrica__events.sql
│   │   │   └── wyscout/
│   │   │       ├── _wyscout__sources.yml
│   │   │       ├── _wyscout__models.yml
│   │   │       └── stg_wyscout__events.sql
│   │   │
│   │   ├── intermediate/             # Cross-source joins, deduplication
│   │   │   ├── _intermediate__models.yml
│   │   │   ├── int_unified_shots.sql
│   │   │   ├── int_unified_passes.sql
│   │   │   └── int_minutes_played.sql
│   │   │
│   │   └── marts/                    # GOLD layer (business logic)
│   │       ├── _marts__models.yml
│   │       ├── fct_shots.sql         # xG features (distance, angle, body_part)
│   │       ├── fct_passes.sql        # Pass metrics, progressive passes
│   │       ├── fct_player_stats.sql  # Per-90 aggregations, radar data
│   │       ├── fct_match_summary.sql # Match-level aggregations
│   │       ├── fct_tracking_frames.sql # Pitch control metrics
│   │       ├── fct_player_embeddings.sql # pgvector: movement pattern embeddings
│   │       ├── dim_players.sql
│   │       ├── dim_teams.sql
│   │       └── dim_competitions.sql
│   │
│   ├── tests/                        # Custom data tests
│   │   ├── assert_xg_between_0_and_1.sql
│   │   └── assert_coordinates_in_bounds.sql
│   │
│   ├── macros/                       # Reusable SQL macros
│   │   ├── distance_to_goal.sql
│   │   ├── shot_angle.sql
│   │   └── flatten_json.sql
│   │
│   └── seeds/                        # Static reference data
│       ├── competition_metadata.csv
│       └── position_mapping.csv
│
├── .github/
│   └── workflows/
│       ├── terraform-plan.yml        # PR: terraform plan + comment
│       ├── terraform-apply.yml       # Merge to main: terraform apply
│       ├── dbt-ci.yml                # PR: dbt build --target ci
│       └── python-ci.yml             # PR: ruff + pyright + pytest
│
└── docs/                             # Additional documentation
    ├── c4/
    │   ├── architecture.dsl          # Structurizr DSL source (C4 model)
    │   └── architecture.html         # Generated: self-contained HTML (all diagram levels)
    ├── architecture-decision-records/
    │   ├── 001-lakebase-over-redshift.md
    │   ├── 002-terraform-over-cdk.md
    │   ├── 003-uv-over-pip.md
    │   └── 004-c4-architecture-docs.md
    └── runbooks/
        ├── initial-setup.md
        └── disaster-recovery.md
```

---

## 6. Phase 0 — Foundation & Prerequisites

### 0.1 — Provision Databricks Workspace on AWS — COMPLETE

**Completed**: Signed up via databricks.com → AWS Marketplace subscription → workspace provisioned.

| Detail | Value |
|--------|-------|
| Workspace URL | `https://dbc-48322be9-16be.cloud.databricks.com` |
| Tier | Premium (14-day free trial, then pay-as-you-go) |
| Region | us-east-1 |
| Unity Catalog metastore | `metastore_aws_us_east_1` (auto-created) |
| Personal access token | Generated (scope: Other APIs, all API scopes) |
| terraform.tfvars | Populated with host + token |

**Action needed**: Rotate the token (exposed in chat) — generate a new one and update `terraform.tfvars`.

**Cost note**: Premium tier at ~$0.55/DBU. With $100/month budget and aggressive scale-to-zero, this allows ~180 DBUs of active compute.

### 0.2 — Verify Terraform State Backend

**Decision**: S3 bucket + DynamoDB lock table already exist (provisioned via MCP CodeDeploy project) but have never been used.

```hcl
terraform {
  backend "s3" {
    bucket         = "<your-terraform-state-bucket>"  # Verify actual name
    key            = "luxury-lakehouse/terraform.tfstate"  # Separate key from other projects
    region         = "us-east-1"
    dynamodb_table = "terraform-state-lock"
    encrypt        = true
  }
}
```

**Phase 0 task**: Run `terraform init` to verify connectivity to the existing backend. If it fails, troubleshoot S3/DynamoDB access before proceeding.

### 0.3 — IAM Role Extension

The existing DevOpsAgent IAM role in `karstenskyt__mcp-aws-codedeploy` needs additional permissions for Databricks:

| Additional Permission | Purpose |
|----------------------|---------|
| `databricks:*` (via cross-account role) | Databricks workspace management |
| `iam:PassRole` (scoped) | Allow Databricks to assume instance profiles |
| `s3:*` on Unity Catalog bucket | Delta Lake storage |

### 0.4 — Repository Initialization — COMPLETE

**Completed**:
- `git init` + remote set to `git@github.com:karstenskyt/luxury-lakehouse.git`
- `uv init --name luxury-lakehouse --python 3.12` + `uv sync --extra dev`
- `.gitignore` created (Python, Terraform, dbt, IDE, secrets)
- `pyproject.toml` with all dependencies (statsbombpy, mplsoccer, streamlit, dbt-databricks, databricks-sdk, etc.)
- Dev tools: ruff 0.15.2, pytest 9.0.2, pyright 1.1.408, pre-commit 4.5.1, sqlfluff 4.0.4
- `pre-commit install` — hooks active
- `.pre-commit-config.yaml` — ruff, terraform_fmt, yaml checks, sqlfluff
- GitHub Actions workflows: `python-ci.yml`, `terraform-plan.yml`, `dbt-ci.yml`
- Full directory structure created (terraform modules, src, dbt_project, docs, .github)
- All Terraform module skeletons (27 files across 7 modules + shared + dev environment)
- All dbt project skeletons (35 files: models, macros, tests, seeds, sources)
- Python `__init__.py` files for all packages

**Completed** (previously pending cloud access):
- `terraform init` — configured with S3 backend, native locking
- Databricks CLI — installed and configured with workspace token
- `dbt deps` — still pending (Phase 3)

### 0.5 — Local Development Environment — PARTIALLY COMPLETE

| Tool | Version | Status |
|------|---------|--------|
| Python | 3.12.12 (via uv) | Installed |
| uv | 0.9.28 | Installed |
| git | 2.51.2 | Installed |
| Java | 21.0.10 (OpenJDK) | Installed |
| ruff | 0.15.2 | Installed (via uv dev extra) |
| pyright | 1.1.408 | Installed (via uv dev extra) |
| pytest | 9.0.2 | Installed (via uv dev extra) |
| pre-commit | 4.5.1 | Installed (via uv dev extra) |
| sqlfluff | 4.0.4 | Installed (via uv dev extra) |
| dbt-core + dbt-databricks | 1.9.x | Installed (via uv) |
| AWS CLI | v2 | Installed (profile: `devops-agent`) |
| Terraform | 1.14.6 | Installed (via winget) |
| Databricks CLI | 0.x | Installed and configured |

**AWS access**: Configured with profile `devops-agent`. Start Claude Code with `AWS_PROFILE=devops-agent claude` for inherited credentials.

### 0.6 — Initial C4 Diagram (System Context) — COMPLETE

**Deliverable**: Generate the L1 System Context and L2 Container diagrams using `/c4` before writing any implementation code.

**Completed**: `docs/c4/architecture.html` (215 KB, self-contained) and `docs/c4/architecture.dsl` (Structurizr DSL source) generated with:
- **System Context**: Platform + 4 persons (Coach/Analyst, Scout, Data Scientist, Platform Engineer) + 5 external systems (StatsBomb, Metrica, Wyscout, GitHub, AWS)
- **Container**: All 7 containers (Ingestion Workflows, Unity Catalog, Serverless SQL Warehouse, dbt Project, Synced Tables Pipeline, Lakebase PostgreSQL 17, Streamlit Dashboard)
- **DSL tab**: Full Structurizr DSL source with copy button

This is the "north star" diagram that all subsequent phases implement towards. Updated via `/final-review` at each phase boundary.

### 0.7 — README — COMPLETE

**Completed**: `README.md` written with:
- NanoBanano comic strip as hero image (`documents/luxury-lakehouse.png`)
- Four Yorkshiremen framing (old way vs new way)
- Architecture table, data sources, analytics, project structure, tech stack
- Link to interactive C4 diagrams

---

## 7. Phase 1 — Serverless Infrastructure (IaC)

### 1.1 — Databricks Workspace & Unity Catalog

**Terraform module: `terraform/modules/workspace/`**

```hcl
# Key resources:
resource "databricks_catalog" "soccer_analytics" {
  name    = "soccer_analytics"
  comment = "Unity Catalog for soccer analytics lakehouse"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.soccer_analytics.name
  name         = "bronze"
  comment      = "Raw ingested data (append-only, schema-on-read)"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.soccer_analytics.name
  name         = "silver"
  comment      = "Cleaned, typed, deduplicated staging tables"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.soccer_analytics.name
  name         = "gold"
  comment      = "Business logic, analytics-ready fact and dimension tables"
}
```

### 1.2 — Lakebase Database Instance

**Terraform module: `terraform/modules/lakebase/`**

**Planned (aspirational):**
```hcl
resource "databricks_lakebase_project" "soccer_analytics" {
  name         = "soccer-analytics-lakebase"
  catalog_name = databricks_catalog.soccer_analytics.name
  autoscaling { min_capacity = 0; max_capacity = 4 }
  engine_version = "17"
}
```

**Actual implementation** (provider v1.110.0+, Autoscaling):
```hcl
resource "databricks_postgres_project" "soccer_analytics" {
  project_id = "soccer-analytics-${var.environment}"
  spec = {
    pg_version   = 17
    display_name = "Soccer Analytics Lakebase (${var.environment})"
    default_endpoint_settings = {
      autoscaling_limit_min_cu = var.autoscaling_min_cu
      autoscaling_limit_max_cu = var.autoscaling_max_cu
      suspend_timeout_duration = var.suspend_timeout_duration
    }
  }
}
```

**Phase 5.5 migration**: Replaced `databricks_database_instance` (Provisioned, PG 16, fixed CU) with `databricks_postgres_project` + branch + endpoint (Autoscaling, PG 17, scale-to-zero). True usage-based billing, configurable PG version, and automatic suspend/resume.

### 1.3 — Serverless SQL Warehouse

**Terraform module: `terraform/modules/sql_warehouse/`**

```hcl
resource "databricks_sql_endpoint" "serverless" {
  name             = "soccer-analytics-warehouse"
  cluster_size     = "2X-Small"
  enable_serverless_compute = true
  auto_stop_mins   = 10       # Auto-stop after 10 min idle

  tags {
    custom_tags {
      key   = "project"
      value = "soccer-analytics"
    }
  }
}
```

### 1.4 — Databricks App (Streamlit Host)

**Terraform module: `terraform/modules/app/`**

```hcl
resource "databricks_app" "streamlit" {
  name        = "soccer-analytics-dashboard"
  description = "Soccermatics interactive analytics dashboard"

  # Serverless runtime
  resources {
    name = "lakebase-connection"
    sql_warehouse {
      id         = databricks_sql_endpoint.serverless.id
      permission = "CAN_USE"
    }
  }
}
```

### 1.5 — Terraform Execution Order

```
1. workspace (catalog + schemas)
    ├── 2a. lakebase (depends on catalog)
    ├── 2b. sql_warehouse (independent)
    └── 2c. workflows (depends on catalog)
3. synced_tables (depends on lakebase + catalog)
4. app (depends on sql_warehouse + lakebase)
```

---

## 8. Phase 2 — Data Ingestion

### 2.1 — Ingestion Scripts

Three Python modules, each responsible for one data source:

**`src/ingestion/statsbomb.py`**

```python
# Key responsibilities:
# 1. Fetch competitions list → write to bronze.statsbomb_competitions
# 2. For each competition/season: fetch matches → bronze.statsbomb_matches
# 3. For each match: fetch events → bronze.statsbomb_events
# 4. For each match: fetch lineups → bronze.statsbomb_lineups
# 5. For each match (where available): fetch 360 data → bronze.statsbomb_360

# Uses: statsbombpy library (same as local soccermatics workspace)
# Writes: Delta Lake tables via Databricks SDK / spark.write.format("delta")
# Idempotency: partition by competition_id/season_id, overwrite partition
```

**`src/ingestion/metrica.py`**

```python
# Key responsibilities:
# 1. Fetch sample tracking CSV from Metrica GitHub
# 2. Parse 25fps coordinate data (all 22 players + ball)
# 3. Write to bronze.metrica_tracking (partitioned by match_id, half)
# 4. Fetch sample event data → bronze.metrica_events

# Challenge: Tracking data is HIGH VOLUME (25 frames/sec × ~90 min = ~135,000 rows/match)
# Strategy: Batch write in chunks, partition by match_id
```

**`src/ingestion/wyscout.py`**

```python
# Key responsibilities:
# 1. Fetch Wyscout public event dataset JSON
# 2. Parse match events for top 5 European leagues
# 3. Write to bronze.wyscout_events (partitioned by competition_id)
# 4. Write to bronze.wyscout_matches

# Note: Wyscout public dataset is the 2017-18 season release
# Local copy already exists at D:/Development/soccermatics/data/Wyscout/
```

### 2.2 — Databricks Workflows (Scheduling)

**Terraform module: `terraform/modules/workflows/`**

```hcl
resource "databricks_job" "data_ingestion" {
  name = "soccer-analytics-ingestion"

  # Run on serverless compute (no cluster to manage)
  task {
    task_key = "ingest_statsbomb"
    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_statsbomb"
    }
  }

  task {
    task_key = "ingest_metrica"
    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_metrica"
    }
  }

  task {
    task_key = "ingest_wyscout"
    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "ingest_wyscout"
    }
  }

  # All three tasks can run in parallel
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"  # Daily at 6am UTC
    timezone_id            = "UTC"
  }
}
```

### 2.3 — Ingestion Best Practices

| Practice | Implementation |
|----------|---------------|
| Idempotency | Partition-level overwrites; same run twice = same result |
| Schema evolution | `mergeSchema = true` on Delta writes |
| Data quality | Row counts logged; null checks on critical fields |
| Incremental loads | StatsBomb: track last-seen match_id; only fetch new |
| Error handling | Per-source try/catch; partial failure doesn't block others |
| Audit trail | `_ingested_at` timestamp column on all bronze tables |

---

## 9. Phase 3 — Transformation (dbt)

### 3.1 — dbt Project Configuration

**`dbt_project/dbt_project.yml`**

```yaml
name: soccer_analytics
version: 1.0.0
profile: databricks

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
seed-paths: ["seeds"]

models:
  soccer_analytics:
    staging:
      +materialized: view           # Views for staging (cheap, always fresh)
      +schema: silver
    intermediate:
      +materialized: ephemeral      # CTEs, not persisted
    marts:
      +materialized: table          # Tables for gold (synced to Lakebase)
      +schema: gold
```

### 3.2 — Key dbt Models

**Silver: `stg_statsbomb__events.sql`** (flattens nested JSON)
```sql
-- Flattens the deeply nested StatsBomb event structure:
-- Raw: { "type": {"id": 16, "name": "Shot"}, "location": [100.5, 33.2], "shot": {"statsbomb_xg": 0.12, ...} }
-- Output: event_id, match_id, type_name, location_x, location_y, shot_statsbomb_xg, ...
```

**Silver: `stg_statsbomb__shots.sql`** (shot-specific extraction)
```sql
-- Extracts shot events with freeze-frame data exploded
-- Each row = one player position in the shot freeze frame
-- Enables defensive positioning analysis at moment of shot
```

**Gold: `fct_shots.sql`** (analytics-ready shot model)
```sql
-- Business logic from soccermatics Chapter 02 (xG model):
-- • Distance to goal center (Pythagorean from location_x, location_y)
-- • Shot angle (arctan geometry from Chapter 02 plot_xGModelFit.py)
-- • Body part (left/right/head)
-- • Situation (open play, set piece, counter)
-- • StatsBomb xG (pre-computed by provider)
-- • Custom xG (logistic regression features ready for ML)
```

**Gold: `fct_player_stats.sql`** (per-90 aggregations)
```sql
-- Business logic from soccermatics Chapter 03 (radar plots):
-- • Goals per 90, assists per 90, xG per 90
-- • Passes attempted/completed per 90
-- • Progressive passes per 90 (from Chapter 09 clustering)
-- • Defensive actions per 90
-- • Minutes played normalization
```

**Gold: `fct_match_summary.sql`** (match-level dashboard)
```sql
-- Aggregated match stats for the main dashboard view:
-- • Total xG per team
-- • Possession percentage
-- • Pass completion rate
-- • Shot count and conversion rate
-- • Expected Threat (xT) buildup
```

### 3.3 — dbt Testing Strategy

| Test Type | Coverage |
|-----------|----------|
| **Schema tests** | Not null on PKs, unique on IDs, accepted values on enums |
| **dbt-expectations** | `expect_column_values_to_be_between` for xG (0–1), coordinates (pitch bounds) |
| **Custom tests** | `assert_xg_between_0_and_1.sql`, `assert_coordinates_in_bounds.sql` |
| **Source freshness** | Bronze tables must have data < 24 hours old |

### 3.4 — Security

| Control | Implementation |
|---------|---------------|
| Data integrity | dbt tests: `unique` on PKs, `not_null` on required columns, `accepted_values` on enums |
| Access control | Unity Catalog `grants` defined in dbt post-hooks for schema-level permissions |
| Audit lineage | dbt artifacts (`manifest.json`, `run_results.json`) logged; Unity Catalog lineage enabled |

### 3.5 — dbt Execution

dbt models run on the **Serverless SQL Warehouse** provisioned in Phase 1. No cluster management needed.

```bash
# Development (local)
dbt run --target dev --select staging
dbt run --target dev --select marts
dbt test --target dev

# CI (GitHub Actions)
dbt build --target ci  # run + test in one command

# Production (Databricks Workflow)
dbt build --target prod --full-refresh  # initial load
dbt build --target prod                 # incremental after
```

---

## 10. Phase 4 — Zero-ETL Synchronization (COMPLETE)

> **Status**: Complete — 8 synced tables online, all data verified in Lakebase PostgreSQL

### 4.1 — Synced Tables Configuration

This is the core differentiator vs. the traditional AWS architecture. Instead of Reverse ETL (Redshift UNLOAD → S3 → RDS import), Lakebase uses **Synced Tables**.

**Terraform module: `terraform/modules/synced_tables/`**

Each Gold-layer table that needs to be queryable by Streamlit gets a `databricks_database_synced_database_table` resource:

```hcl
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
```

**Key implementation details:**
- `gold_schema` variable handles dbt's environment prefix (`dev_gold` in dev, `gold` in prod)
- `scheduling_policy = "SNAPSHOT"` — initial sync with on-demand refresh
- `logical_database_name = "databricks_postgres"` — standard Lakebase database

> **Autoscaling workaround (provider v1.110.0):** The `databricks_database_synced_database_table` resource only supports `database_instance_name` (Provisioned). The REST API server accepts project+branch fields but the Terraform provider, Go SDK, Python SDK, and CLI do not expose them. Synced tables targeting **Autoscaling projects** must be created via the **Databricks UI** (Catalog Explorer → source table → Create → Synced table → select project/branch), then imported into Terraform state using `scripts/import_synced_tables.sh`. The `lifecycle { ignore_changes = all }` block prevents drift — the provider also does not support updates ("Update Synced Database Table is not yet implemented"). This applies to any new synced table added in future phases.

**Tables synced (Gold → Lakebase) with verified row counts:**

| Source Table | Synced Table | Primary Key | Rows |
|-------------|-------------|-------------|------|
| `dev_gold.fct_shots` | `fct_shots_synced` | `shot_id` | 131,077 |
| `dev_gold.fct_passes` | `fct_passes_synced` | `pass_id` | 5,052,415 |
| `dev_gold.fct_player_stats` | `fct_player_stats_synced` | `player_stats_id` | 19,664 |
| `dev_gold.fct_match_summary` | `fct_match_summary_synced` | `match_id` | 3,464 |
| `dev_gold.fct_player_embeddings` | `fct_player_embeddings_synced` | `embedding_id` | 0 |
| `dev_gold.dim_players` | `dim_players_synced` | `player_id` | 10,803 |
| `dev_gold.dim_teams` | `dim_teams_synced` | `team_id` | 453 |
| `dev_gold.dim_competitions` | `dim_competitions_synced` | `competition_id` | 21 |

**NOT synced** (too large for OLTP, query via Databricks SQL instead):
- `dev_gold.fct_tracking_frames` — 135K rows/match at 25fps; keep in lakehouse only

**PK fixes applied during implementation:**
- `fct_player_stats`: changed from `["player_id", "match_id"]` to `["player_stats_id"]` (dbt surrogate key)
- `fct_player_embeddings`: changed from `["player_id"]` to `["embedding_id"]` (dbt surrogate key)

### 4.2 — How Synced Tables Work

```
Gold Delta Table (lakehouse)
         │
         ▼
  Lakeflow Declarative Pipeline (managed, serverless)
         │
         ▼  snapshot sync (initial + on-demand refresh)
  Lakebase PostgreSQL table (read-only mirror)
         │
         ▼  standard PostgreSQL wire protocol (port 5432, sslmode=require)
  Streamlit app queries via psycopg2 (OAuth M2M auth, retry on scale-to-zero)
```

- **Latency**: Sub-10ms for point queries from Streamlit
- **Consistency**: Snapshot-based (SNAPSHOT scheduling policy)
- **Cost**: Included in Lakebase compute; no separate ETL job cost
- **Management**: Zero — Databricks handles pipeline lifecycle
- **Initial sync**: All 8 tables reached `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE` within 30 seconds

### 4.3 — Security

| Control | Implementation | Status |
|---------|---------------|--------|
| Connection restriction | Lakebase access restricted to Streamlit app service principal only | Phase 5 |
| Connection pooling | `psycopg2.pool` with 55min recycle (under 1hr OAuth token expiry) | Phase 5 |
| Encryption in transit | `sslmode=require` enforced on all PostgreSQL connections (Autoscaling requirement) | Active |
| Read-only mirrors | Synced tables are read-only replicas — no write path from Streamlit to Gold | Active |
| Authentication | OAuth M2M only (`effective_enable_pg_native_login = false`) | Active |
| SSL enforcement | All Lakebase connections require SSL by default | Active |

### 4.4 — Implementation Notes

- **dbt schema naming**: dbt prepends target name to custom schemas, so `gold` becomes `dev_gold` in the dev target. The `gold_schema` variable in the synced_tables module handles this.
- **Git Bash MSYS path mangling**: On Windows, Git Bash converts `/sql/1.0/...` to `C:/Program Files/Git/sql/1.0/...`. Fix: set `MSYS_NO_PATHCONV=1` before running dbt commands.
- **Autoscaling scale-to-zero wake-up**: Lakebase Autoscaling endpoints suspend after the configured timeout (default 300s). First connection after suspension takes 2–5 seconds. The Streamlit app retries up to 3 times with 3s delay to handle this transparently.
- **Synced table creation (manual step)**: Until the Databricks provider adds project/branch fields to `databricks_database_synced_database_table`, new synced tables must be created via the UI then imported into Terraform. See `scripts/import_synced_tables.sh` for the import workflow. This affects any future gold table additions (Phase 6+).
- **Credential API for Autoscaling**: The REST endpoint is `/api/2.0/postgres/credentials` (NOT `/api/2.0/database/credentials` which only supports Provisioned instances). Requires OAuth authentication — PATs return 401 on `/api/2.0/database/` endpoints.

---

## 11. Phase 5 — Application Deployment (Streamlit) — COMPLETE

### 5.1 — Streamlit App Architecture (Implemented)

```
src/streamlit_app/
├── app.py              # Entrypoint: st.navigation, page routing, sidebar branding
├── config.py           # Pydantic BaseSettings: env vars, identifier validation
├── db.py               # OAuth M2M token management, parameterized query execution
├── pages/
│   ├── shot_map.py       # Shot map + xG scatter (mplsoccer half-pitch)
│   ├── pass_map.py       # Pass arrows on full pitch with progressive highlighting
│   ├── player_radar.py   # Radar chart comparing 1-3 players on per-90 metrics
│   └── match_summary.py  # Scorecard, xG comparison, horizontal bar chart
└── components/
    ├── filters.py      # 5 cascading filter widgets backed by Lakebase dimension tables
    ├── pitch.py        # mplsoccer wrappers (shot scatter, pass arrows)
    └── charts.py       # matplotlib wrappers (radar, bar comparison)
```

Supporting files:
- `app.yaml` — Databricks Apps manifest (port 8000, PYTHONPATH=src, env vars)
- `requirements.txt` — Python dependencies for Databricks Apps deployment
- `.streamlit/config.toml` — Dark theme (#1a1a2e), XSRF protection

### 5.2 — Authentication: OAuth M2M (Implemented)

The app runs as a Databricks App with an auto-assigned **service principal** (`be66af99-5296-4fd9-887a-c081bce38bfa`). Token generation uses the SDK with a REST API fallback for older runtimes:

```python
# db.py — actual implementation pattern (Autoscaling, PG 17)
ws = WorkspaceClient()  # Inherits SP identity from Databricks App runtime
try:
    credential = ws.postgres.generate_database_credential(
        endpoint=settings.lakebase_endpoint_name,
    )
    token = credential.token
except AttributeError:
    # REST fallback for older SDK versions
    token = _generate_credential_via_rest(ws, endpoint_name)

pg_user = _extract_jwt_subject(token)  # JWT 'sub' claim = PG role name
conn = psycopg2.connect(
    host=settings.lakebase_host, port=5432,
    database="databricks_postgres", user=pg_user, password=token,
    sslmode="require",  # Autoscaling requirement
)
```

**Key learnings from deployment:**
- PG username is the JWT `sub` claim (SP UUID), not `"token"` or `"databricks"`
- Lakebase PG database is `databricks_postgres` (Unity Catalog catalog does NOT map to a PG database)
- Unity Catalog schema maps directly to a PG schema (e.g. `dev_gold`)
- PG-level `GRANT USAGE ON SCHEMA` and `GRANT SELECT ON ALL TABLES` required in addition to UC grants

### 5.3 — Implemented Pages

| Streamlit Page | Key Visualization | Data Source (Lakebase table) |
|----------------|-------------------|-----------------------------|
| Shot Map | mplsoccer half-pitch, shots sized by xG, colored by outcome | `fct_shots_synced` JOIN `dim_players_synced` |
| Pass Map | mplsoccer full pitch, arrows colored by progressive/complete/incomplete | `fct_passes_synced` |
| Player Radar | mplsoccer Radar, 1-3 players, 6 configurable per-90 metrics | `fct_player_stats_synced` JOIN `dim_players_synced` |
| Match Summary | Scorecard + xG metrics + horizontal bar chart (8 stat categories) | `fct_match_summary_synced` |

**Planned pages** (see [Section 14.4](#144--additional-streamlit-pages)): Pitch Control (Voronoi), Player Similarity (pgvector), Heat Map, Pass Network.

### 5.4 — Security (Implemented)

| Control | Implementation |
|---------|---------------|
| Authentication | Databricks App OAuth M2M — SP identity with 55-min token refresh cycle |
| SQL injection prevention | All queries use `%s` parameterized placeholders; table names validated via `_IDENTIFIER_RE` |
| Input validation | Filter inputs sourced from dimension table queries; identifiers regex-validated; `int()` type assertions on all IDs (L-3) |
| Connection pooling | `ThreadedConnectionPool` (min=1, max=5) with 55-min recycle aligned to token expiry; thread-safe via `_pool_lock` (L-4, L-6) |
| SSL | `sslmode="require"` on all Lakebase connections (Autoscaling requirement) |
| Query limits | `statement_timeout=30000` (30s) prevents runaway queries (M-3) |
| Error handling | `psycopg2.Error` caught and sanitized — no tracebacks leaked to browser (M-2) |
| Session security | No credentials or PII in `st.session_state`; tokens cached in module-level dict with TTL |
| Full security audit | See [SECURITY.md](SECURITY.md) — 30 findings, 25 resolved (83% coverage) |

### 5.5 — Deployment (Implemented)

Deployed via `databricks apps deploy` with workspace source code sync. No Docker, no ECS — fully serverless on Databricks Apps runtime (Python 3.11).

```yaml
# app.yaml
command: ['streamlit', 'run', 'src/streamlit_app/app.py', '--server.port', '8000', '--server.address', '0.0.0.0']
```

---

## 12. Cross-Cutting Concerns

### 12.1 — Security

| Concern | Implementation |
|---------|---------------|
| Secrets management | No hardcoded credentials; OAuth M2M for app, Terraform vars for IaC |
| Network | Private endpoints where available; TLS everywhere; HTTPS-only for all data fetches |
| IAM | Least-privilege; separate service principals per workload |
| Data classification | Open-source data only (no PII); still apply Unity Catalog ACLs |
| Audit | Unity Catalog audit logs; Terraform state versioning |
| Input validation | Regex on all user-supplied identifiers (`^[a-zA-Z_][a-zA-Z0-9_]*$`) to prevent injection |
| SSL verification | Explicit `verify=True` on all HTTP requests; never disable cert checks |
| Timeouts | `(10, 30)` connect/read on every HTTP call; no unbounded requests |
| Retry safety | Exponential backoff on transient errors (429/5xx); max 3 retries |
| Bandit compliance | Ruff S rules enforced; no eval/exec/pickle/shell=True |
| Content validation | Schema checks and non-empty assertions before every Delta write |
| Job notifications | Email alerts on ingestion job start, success, and failure |

#### Phase 3 Security Requirements (dbt)

| Requirement | Implementation |
|-------------|---------------|
| Data integrity tests | `unique`, `not_null`, `accepted_values` on all silver/gold model PKs and enums |
| Access control | Define `grants` in dbt models for schema-level Unity Catalog permissions |
| Audit lineage | Enable dbt artifacts logging; leverage Unity Catalog lineage for traceability |

#### Phase 4 Security Requirements (Synced Tables / Lakebase)

| Requirement | Implementation |
|-------------|---------------|
| Connection restriction | Restrict Lakebase access to Streamlit app service principal only |
| Query limits | Configure query timeouts and connection pooling limits to prevent resource exhaustion |
| Encryption in transit | Enforce `sslmode=require` on all PostgreSQL connections (Autoscaling) |

#### Phase 5 Security Requirements (Streamlit)

| Requirement | Implementation |
|-------------|---------------|
| Authentication | Use Databricks App auth (OAuth M2M) — never deploy without authentication |
| SQL injection prevention | Parameterized queries only — never concatenate user input into SQL |
| Input validation | Validate and sanitize all filter inputs (competition, team, player selectors) |
| Session security | No PII or credentials cached in Streamlit session state |

#### Production Hardening (All Phases)

| Requirement | Implementation |
|-------------|---------------|
| Terraform state encryption | Add explicit `kms_key_id` to S3 backend for production |
| CI/CD supply chain | Pin GitHub Actions to SHA digests instead of version tags |
| Secrets rotation | Document 90-day Databricks PAT rotation; migrate to service principals for prod |

### 12.6 — Quality Standards

All code must pass these gates before merge:

| Check | Command | Threshold |
|-------|---------|-----------|
| Lint | `uv run ruff check src/` | Zero violations |
| Type check | `uv run pyright src/` | Zero errors (basic mode) |
| Unit tests | `uv run pytest src/tests/ -v` | All pass |
| Security scan | Ruff S (bandit) rules | Zero violations |
| Wheel build | `uv build` | Produces installable wheel |

**Enforced Ruff rule sets:** E, W, F, I, N, UP, B, S (bandit), RUF

### 12.2 — Cost Management

| Strategy | Implementation |
|----------|---------------|
| Scale-to-zero | Lakebase Autoscaling min=0; SQL Warehouse auto-stop=10min |
| Serverless compute | No always-on clusters; pay per DBU consumed |
| Right-sizing | Start with 2X-Small SQL Warehouse; scale based on usage |
| Monitoring | Databricks billing alerts; tag all resources with `project=soccer-analytics` |
| Estimate | Dev environment: ~$50-100/month with scale-to-zero (mostly idle) |

### 12.3 — Observability

| Layer | Tool |
|-------|------|
| Infrastructure | Terraform plan output, Databricks audit logs |
| Data pipeline | dbt test results, source freshness checks |
| Ingestion | Databricks Workflow run history, row count assertions |
| Application | Streamlit built-in metrics, Lakebase query logs |

### 12.4 — Testing Strategy

| Level | What | How |
|-------|------|-----|
| Unit | Ingestion logic, utility functions | pytest |
| Integration | dbt models compile and run | `dbt build --target ci` |
| Data quality | Row counts, value ranges, referential integrity | dbt tests + dbt-expectations |
| E2E | Streamlit pages render with real data | Manual smoke test + screenshots |
| Infrastructure | Terraform validates | `terraform validate` + `terraform plan` |

### 12.5 — Architecture Documentation (C4 + /final-review)

C4 diagrams are the single source of truth for architecture documentation. They are maintained as code (Structurizr DSL) and regenerated automatically.

**Pre-commit quality gate via `/final-review`:**

The `/final-review` skill is invoked before any significant commit. It performs:
1. Code and documentation consistency check
2. Best practices review
3. **C4 diagram regeneration** — ensures architecture docs reflect actual code

**C4 update triggers:**
| Change Type | C4 Action |
|-------------|-----------|
| New Terraform module added | Update L2 Container + Deployment diagrams |
| New ingestion source | Update L3 Ingestion Component diagram |
| New dbt model in marts/ | Update L3 dbt Component + L4 Data Flow diagrams |
| New Streamlit page | Update L3 App Component diagram |
| Infrastructure topology change | Update Deployment diagram |
| Any of the above | Run `/final-review` → auto-regenerates all C4 diagrams |

**ADR**: `docs/architecture-decision-records/004-c4-architecture-docs.md` documents the decision to use C4 as the standard.

---

## 13. Decisions Log (Resolved Questions)

All planning questions have been answered. This section records the decisions for future reference.

| # | Question | Decision | Impact |
|---|----------|----------|--------|
| 1 | Databricks workspace | **Provision new** via AWS Marketplace | Phase 0 includes workspace provisioning |
| 2 | Databricks tier | **Premium** | Includes Lakebase, Unity Catalog, Serverless SQL |
| 3 | AWS region | **us-east-1** | Consistent with MCP CodeDeploy setup |
| 4 | Terraform state backend | **Exists but unused** — S3 bucket + DynamoDB table provisioned, never used | Verify connectivity in Phase 0, no provisioning needed |
| 5 | GitHub repository | **`karstenskyt/luxury-lakehouse`** — Monty Python Four Yorkshiremen theme | Repo created on GitHub, ready for initial push |
| 6 | Budget | **Under $100/month** | Scale-to-zero mandatory everywhere; 2X-Small SQL Warehouse; aggressive auto-stop |
| 7 | Environments | **Dev only** | Single Terraform environment; simplifies structure; add prod later |
| 8 | Metrica tracking data | **Include from start** | All three data sources (StatsBomb, Metrica, Wyscout) in Phase 2 |
| 9 | Additional data sources | **Future consideration** | Design pipeline to be extensible (modular ingestion); don't implement Signality/SkillCorner now |
| 10 | pgvector | **In scope** | Include vector embedding work in initial implementation; leverage Lakebase native pgvector |

### Design Implications of These Decisions

**Budget ($100/month) + Dev-only:**
- All compute must scale to zero: Lakebase Autoscaling min=0, SQL Warehouse auto-stop=10min
- No always-on resources whatsoever
- Single Terraform environment (removes `terraform/environments/prod/`)
- Databricks Premium is ~$0.55/DBU — budget allows ~180 DBUs/month of active compute

**Metrica from start + pgvector in scope:**
- Phase 2 includes all three ingestion modules (no deferral)
- pgvector-powered similarity search deferred to Phase 8 (embeddings) and Phase 9 (Streamlit page)
- Gold layer has a `gold.fct_player_embeddings` table provisioned (0 rows; populated in Phase 8)
- Lakebase Synced Tables include the embeddings table (ready for Phase 9 Player Similarity page)

**Repo finalized:**
- GitHub repo: `karstenskyt/luxury-lakehouse` (created, empty)
- Ready for `git remote add origin` and initial push
- GitHub Actions CI can be configured from Phase 0

### Remaining Open Items

| Item | Status | Blocker? |
|------|--------|----------|
| GitHub repo name | Decided: `karstenskyt/luxury-lakehouse` | No — resolved |
| Terraform state backend validation | Exists, needs connectivity test | Phase 0 task |
| Databricks Terraform provider Lakebase support | Must verify at implementation time | R1 in Risk Register |

---

## 14. Future Work

### 14.1 — Future Data Sources

The following data sources are planned for integration after Phase 5:

| Source | Data Type | Status | Notes |
|--------|-----------|--------|-------|
| **Respo.Vision** | 3D pose tracking from broadcast video | Planned | User pursuing via professional network; skeletal keypoints at 25fps |
| **Wyscout match metadata** | Match details (formations, coaches, venue) | Deferred | Event data ingested; full match metadata not in public Figshare dataset |
| **StatsBomb 360 freeze frames** | Visible player positions per event | Planned | Ingestion scaffolded in `statsbomb.py`; 11 competition-seasons have 360 data (World Cup 2022, Euro 2024, Euro 2020, La Liga 2020/21, Ligue 1 2021/22 + 2022/23, Bundesliga 2023/24, MLS 2023, Women's Euro 2022 + 2025, Women's World Cup 2023) |

Each new source follows the established pattern: `src/ingestion/<source>.py` → Bronze Delta tables → dbt staging/marts → Synced Tables → Lakebase.

### 14.2 — Cross-Source Player Entity Resolution

`dim_players` currently deduplicates within each source, but StatsBomb, Metrica, and Wyscout use independent player IDs with no shared key. The same player (e.g., Messi) exists as three separate rows.

**Planned approach:** [`parmacalcio1913/players-matcher`](https://github.com/parmacalcio1913/players-matcher) — a fuzzy-matching library purpose-built for football player entity resolution across data providers. It uses name similarity, birth date, and team context to produce a canonical mapping table.

**Integration path:**
1. Add `players-matcher` as a dependency
2. Build a mapping seed or intermediate model (`int_player_xref`) that links StatsBomb, Metrica, and Wyscout player IDs to a canonical `player_id`
3. Refactor `dim_players` to merge cross-source records using the mapping
4. Downstream fact tables and Streamlit pages automatically benefit from unified player identity

This is a prerequisite for meaningful cross-source analytics (e.g., comparing a player's StatsBomb xG with their Wyscout event data).

### 14.3 — pgvector Player Embeddings

The `fct_player_embeddings` gold table and its synced table are provisioned but contain 0 rows. The embedding logic has not been implemented yet.

**Planned work:**
- Design a feature vector from `fct_player_stats` per-90 metrics (goals, assists, xG, progressive passes, etc.)
- Generate embeddings in a dbt model or Python post-processing step
- Populate `fct_player_embeddings` with vectors suitable for pgvector similarity search
- Implement the **Player Similarity** Streamlit page (`player_search.py`) using pgvector `<=>` cosine distance queries
- Depends on cross-source player entity resolution (14.2) for unified player identity

### 14.5 — Metrica Tracking Data: Game 3 + Pitch Control

Games 1–2 are already ingested, transformed (`fct_tracking_frames`), and synced to Lakebase. This phase adds Game 3 and builds the Pitch Control visualization.

| Task | Description | Status |
|------|-------------|--------|
| **Game 3 ingestion** | EPTS FIFA format (JSON events + tracking); new parser in `metrica.py` | Planned |
| **dbt tests** | Verify Game 3 compatibility with existing `stg_metrica__tracking` schema | Planned |
| **Pitch Control page** | Voronoi diagrams showing space ownership from `fct_tracking_frames_synced` | Planned |
| **Velocity/acceleration viz** | Visualize speed data from `fct_tracking_frames` `final` CTE | Planned |

### 14.6 — Additional Streamlit Pages

| Page | Description | Data Source | Status |
|------|-------------|-------------|--------|
| **Pitch Control** | Voronoi diagrams showing space ownership | `fct_tracking_frames_synced` | Planned — requires Metrica tracking data |
| **Player Similarity** | pgvector-powered nearest-neighbor search | `fct_player_embeddings_synced` | Planned — depends on 14.3 |
| **Heat Map** | Touch/action density maps per player or team | `fct_passes_synced`, `fct_shots_synced` | Planned |
| **Pass Network** | Graph visualization of passing connections between teammates | `fct_passes_synced` | Planned |

---

## 15. Risk Register

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| R1 | Lakebase Terraform provider support incomplete (GA Feb 2026) | Medium | High | **Realized**: Provider v1.110.0 lacks project/branch fields for synced tables. Mitigated via UI creation + `terraform import` + `lifecycle { ignore_changes = all }`. See §4.1 and §4.4. |
| R2 | Synced Tables feature not available in chosen Databricks tier | Low | Critical | Confirm tier supports Synced Tables before provisioning |
| R3 | StatsBomb API rate limiting during bulk ingestion | Medium | Low | Implement exponential backoff; cache locally; run during off-peak |
| R4 | Databricks cost exceeds expectations | Medium | Medium | Start with scale-to-zero everywhere; set billing alerts; review after 1 month |
| R5 | dbt-databricks adapter incompatibility with Lakebase features | Low | Medium | Test early in Phase 3; fall back to raw SQL if needed |
| R6 | Streamlit performance with mplsoccer (matplotlib is slow for interactive) | Medium | Low | Cache rendered figures; pre-compute static images for common views |
| R7 | Unity Catalog ACL complexity for multi-user access | Low | Low | Start with single admin user; add RBAC later |

---

## 16. Appendices

### A. Data Volume Estimates

| Source | Matches | Events/Match | Rows (Bronze) | Size Estimate |
|--------|---------|-------------|----------------|---------------|
| StatsBomb (open) | ~3,000 | ~3,400 | ~10.2M events | ~2 GB JSON → ~500 MB Parquet |
| Metrica (sample) | 3 | 135,000 frames | ~405K frames | ~50 MB CSV → ~15 MB Parquet |
| Wyscout (public) | ~1,900 | ~1,800 | ~3.4M events | ~1.5 GB JSON → ~400 MB Parquet |
| **Total Bronze** | | | **~31.4M rows** | **~1 GB Parquet** |

This is a small-to-medium dataset — well within free/dev tier limits for most services.

### B. Reference: Soccermatics Chapter → Analytics Mapping

| Chapter | Analytics Concept | dbt Model Target |
|---------|-------------------|------------------|
| 00 | Data loading patterns | `stg_*` sources |
| 01 | Shot plotting, pass networks, heat maps | `fct_shots`, `fct_passes` |
| 02 | xG model (logistic regression), shot geometry | `fct_shots` (distance, angle features) |
| 03 | Radar plots, per-90 stats | `fct_player_stats` |
| 04 | Expected Threat (xT), Markov chains | `fct_passes` (xT values) |
| 05 | Match simulation, randomness | `fct_match_summary` |
| 06 | Voronoi diagrams, pitch control | `fct_tracking_frames` |
| 07 | xG with tracking data | Future: join events + tracking |
| 08 | Physical data, acceleration | `fct_tracking_frames` |
| 09 | Clustering, progressive passes | `fct_passes`, `fct_player_stats` |
| 10 | Streamlit web app | `src/streamlit_app/` |

### C. Dependencies on MCP CodeDeploy Project

| Dependency | Status | Action |
|------------|--------|--------|
| DevOpsAgent IAM role | Active | `AWS_PROFILE=devops-agent` (account 454762693631) |
| S3 state bucket | Active | `karstenskyt-terraform-state` with native S3 locking |
| S3 native state locking | Active | S3 bucket `karstenskyt-terraform-state` uses native locking (no DynamoDB) |
| MCP server for Claude Code | Working | Can use for AWS operations during setup |

### D. Implementation Timeline (Suggested)

| Phase | Effort | Dependencies |
|-------|--------|-------------|
| Phase 0: Foundation | 1-2 sessions | AWS account, Databricks account |
| Phase 1: Infrastructure (Terraform) | 2-3 sessions | Phase 0 complete |
| Phase 2: Data Ingestion | 2-3 sessions | Phase 1 (catalog + workflows) |
| Phase 3: dbt Transformations | 3-4 sessions | Phase 2 (bronze tables populated) |
| Phase 4: Synced Tables | 1 session | Phase 1 (lakebase) + Phase 3 (gold tables) |
| Phase 5: Streamlit App | 3-4 sessions | Phase 4 (lakebase queryable) |

Phases 2 and 3 can partially overlap once the catalog is provisioned.

---

*This plan is designed to survive session interruptions. Each phase is self-contained with clear inputs, outputs, and verification criteria. Resume from where you left off by checking task completion status.*
