# (Right! Luxury!) Lakehouse

> *"Luxury! We used to dream of serverless! We had to get up at three o'clock in the morning, restart the Airflow EC2 instance, write raw JSON to an S3 bucket with our bare hands, wait six hours for a Glue job to flatten the arrays, and force-feed it into Redshift just so we could reverse-ETL it all back into Postgres!"*

![The Modern Engineer vs The Veteran Engineer](assets/luxury-lakehouse.jpg)

<sup>Comic by NanoBanana &mdash; inspired by Monty Python's *Four Yorkshiremen*</sup>

---

## What Is This?

A serverless soccer analytics platform built on **Databricks Lakebase** — replacing a 6-service traditional AWS pipeline with a unified lakehouse architecture that scales to zero.

The platform ingests open-source match data from professional football, transforms it through a medallion architecture, and serves interactive dashboards for coaches, scouts, and analysts.

### The Old Way (The Veteran Engineer)

```
Data Providers → EC2 Airflow → S3 Raw → AWS Glue → S3 Processed → Redshift → dbt → Reverse ETL → S3 → RDS PostgreSQL → Streamlit
```

Six AWS services. Five data movement hops. Always-on compute. Manual credential management. The full Victorian workhouse experience.

### The New Way (The Modern Engineer)

```
Data Providers → Databricks Workflows → Delta Lake (Bronze/Silver/Gold) → Synced Tables → Lakebase PostgreSQL 17 → Streamlit
```

Two services. Zero-ETL. Scale-to-zero. Automatic OAuth. Right luxury.

## Architecture

> **[View interactive C4 diagrams](docs/c4/architecture.html)** &mdash; System Context, Container, Ingestion Component, dbt Component, Streamlit Component, and Data Flow levels, generated from [Structurizr DSL](docs/c4/architecture.dsl)

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | Databricks Serverless Workflows | Fetch data from StatsBomb, Metrica, Wyscout, IDSSE, SkillCorner |
| **Storage** | Delta Lake on Unity Catalog | Medallion architecture (Bronze → Silver → Gold) |
| **Transformation** | dbt-databricks on Serverless SQL | Flatten nested JSON, compute xG/xT metrics |
| **Synchronization** | Lakeflow Synced Tables | Zero-ETL continuous sync from Gold → Lakebase |
| **Serving** | Lakebase PostgreSQL 17 (Autoscaling) | Sub-10ms OLTP queries, native pgvector, scale-to-zero |
| **Application** | Streamlit on Databricks Apps | Interactive dashboards with mplsoccer visualizations |
| **ML Artifacts** | HuggingFace Hub | Publish football2vec model weights for community access |
| **Security** | OAuth M2M + OIDC Federation + KMS | Zero-secret CI, least-privilege SPs, encrypted state |
| **Infrastructure** | Terraform + Databricks Provider | Everything as code |

## Data Sources

| Provider | Data Type | Format | License | Coverage |
|----------|-----------|--------|---------|----------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | Match events + 360 context | Nested JSON | CC-BY 4.0 | ~3,000 matches |
| [Metrica Sports](https://github.com/metrica-sports/sample-data) | Optical tracking (25 fps) | CSV/EPTS | Unlicensed | 3 sample matches |
| [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | Event streams | JSON | CC-BY 4.0 | Top 5 leagues |
| [IDSSE (Bundesliga)](https://figshare.com/collections/DFL_-_Bundesliga_Data_Shootout/5830772) | DFL tracking (25 fps) | XML | CC-BY 4.0 | 7 matches |
| [SkillCorner](https://github.com/SkillCorner/opendata) | Broadcast tracking (10 fps) | JSONL | MIT | 10 A-League matches |
| *Respo.Vision* (planned) | 3D pose tracking | JSON | TBD | TBD |

## Analytics

Built on the [Soccermatics](https://soccermatics.readthedocs.io/) curriculum by David Sumpter:

- **Expected Goals (xG)** — Shot quality model using distance, angle, body part
- **Expected Threat (xT)** — Pitch zone valuation via Markov chains
- **Pass Networks** — Interactive team passing structure with hover tooltips (Plotly)
- **Heat Maps** — Action density visualization for players and teams
- **VAEP Action Valuation** — Player contribution scoring beyond goals/assists (SPADL + VAEP)
- **Pitch Control** — Physics-based (Spearman 2017) and Voronoi models from tracking data
- **Line-Breaking Passes** — Ward clustering + cross-product straddle test for defensive line penetration (StatsBomb 360)
- **Movement Analysis** — PPDA pressing intensity, physical performance metrics (distance, HSR, sprints), and off-ball xT from tracking data
- **Defensive Pressure (DEFCON-lite)** — Attacker-perspective defensive credit assignment (intercept/concede/disturb/deter) based on Kim et al. (2025)
- **Cross-Source Entity Resolution** — Three-layer progressive player matching (TF-IDF + rapidfuzz + bidirectional validation) inspired by US Soccer's glass_onion
- **Player Embeddings** — Dual-vector player representation: 32-dim Doc2Vec behavioral + 13-dim statistical z-score, published to HuggingFace Hub
- **Player Similarity** — pgvector HNSW cosine-distance search ("Find players like X") with interactive Streamlit page
- **Player Radar Charts** — Per-90 stat comparison across multiple metrics (incl. DEFCON pressure/90)

## Project Structure

```
luxury-lakehouse/
├── terraform/          # Infrastructure as Code (Databricks on AWS)
├── src/
│   ├── analytics/      # Pure-Python analytics models (pitch control, line-breaking, entity resolution, DEFCON, football2vec)
│   ├── ingestion/      # Data ingestion (StatsBomb, Metrica, Wyscout, IDSSE, SkillCorner)
│   └── streamlit_app/  # Interactive analytics dashboard
├── notebooks/          # Databricks notebooks (football2vec training + HF Hub publishing)
├── dbt_project/        # Bronze → Silver → Gold transformations
├── scripts/            # Operational scripts (PG indexes, grants, synced table management)
├── docs/
│   ├── c4/             # C4 architecture diagrams (Structurizr DSL)
│   └── huggingface-setup.md  # HuggingFace Hub integration guide
├── assets/             # Images and branding
├── PLAN.md             # Detailed implementation plan
└── ROADMAP.md          # Research directions and future ideas
```

## Status

**Phase 17 complete** — 11 Streamlit pages, 16 synced tables, 31 PG indexes, 470 unit tests. See [PLAN.md](PLAN.md) for the implementation plan and [ROADMAP.md](ROADMAP.md) for research directions.

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Foundation & Prerequisites | Complete |
| 1 | Serverless Infrastructure (Terraform) | Complete |
| 2 | Data Ingestion (StatsBomb, Metrica, Wyscout) | Complete |
| 3 | Transformation (dbt on Databricks) | Complete |
| 4 | Zero-ETL Synchronization (Synced Tables → Lakebase) | Complete |
| 5 | Application Deployment (Streamlit) | Complete |
| 5.5 | Lakebase Autoscaling + PG 17 Migration | Complete |
| 5.6 | IAM OIDC + OAuth M2M + KMS Hardening | Complete |
| 6 | StatsBomb 360 Freeze Frames | Complete |
| 7 | Metrica Game 3 (EPTS) + Pitch Control | Complete |
| 8 | Heat Map + Pass Network Pages | Complete |
| 9 | SPADL/VAEP Action Valuation | Complete |
| 10 | Additional Tracking Data (IDSSE, SkillCorner) | Complete |
| 11 | Physics-Based Pitch Control (Spearman 2017) | Complete |
| 12 | Movement Analysis | Complete |
| 13 | Line-Breaking Pass Detection | Complete |
| 14 | Cross-Source Player Entity Resolution | Complete |
| 15 | Player Embeddings (Doc2Vec + z-score) | Complete |
| 16 | Player Similarity Page (pgvector HNSW) | Complete |
| 17 | DEFCON-lite Defensive Pressure | Complete |

## Tech Stack

| Category | Tool |
|----------|------|
| Cloud | AWS (us-east-1) + Databricks Premium |
| IaC | Terraform with `databricks/databricks` provider |
| Data Lake | Delta Lake on Unity Catalog |
| OLTP Database | Databricks Lakebase (PostgreSQL 17, autoscaling) |
| Transformations | dbt-core + dbt-databricks |
| Orchestration | Databricks Serverless Workflows |
| Application | Streamlit + mplsoccer + Plotly |
| Vector Search | pgvector HNSW (native in Lakebase) |
| Embeddings | gensim (Doc2Vec) + huggingface_hub (model publishing) |
| Python | 3.10+ (Databricks serverless), managed with uv |
| Linting | ruff + pyright + sqlfluff |
| CI/CD | GitHub Actions |
| Architecture Docs | C4 diagrams (Structurizr DSL) |

## License

[Apache License 2.0](LICENSE) &mdash; see [NOTICE](NOTICE) for third-party data attribution.

---

<sub>Named after Monty Python's *Four Yorkshiremen* sketch, where each comedian one-ups the others about how deprived their childhood was. In data engineering, moving from hand-managed EC2 instances and 5-hop Reverse ETL pipelines to serverless Lakebase truly is... right luxury.</sub>
