# (Right! Luxury!) Lakehouse

> *"Luxury! We used to dream of serverless! We had to get up at three o'clock in the morning, restart the Airflow EC2 instance, write raw JSON to an S3 bucket with our bare hands, wait six hours for a Glue job to flatten the arrays, and force-feed it into Redshift just so we could reverse-ETL it all back into Postgres!"*

![The Modern Engineer vs The Veteran Engineer](documents/luxury-lakehouse.png)

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
| **Ingestion** | Databricks Serverless Workflows | Fetch data from StatsBomb, Metrica Sports, Wyscout |
| **Storage** | Delta Lake on Unity Catalog | Medallion architecture (Bronze → Silver → Gold) |
| **Transformation** | dbt-databricks on Serverless SQL | Flatten nested JSON, compute xG/xT metrics |
| **Synchronization** | Lakeflow Synced Tables | Zero-ETL continuous sync from Gold → Lakebase |
| **Serving** | Lakebase PostgreSQL 17 (Autoscaling) | Sub-10ms OLTP queries, native pgvector, scale-to-zero |
| **Application** | Streamlit on Databricks Apps | Interactive dashboards with mplsoccer visualizations |
| **Security** | OAuth M2M + OIDC Federation + KMS | Zero-secret CI, least-privilege SPs, encrypted state |
| **Infrastructure** | Terraform + Databricks Provider | Everything as code |

## Data Sources

| Provider | Data Type | Format | Coverage |
|----------|-----------|--------|----------|
| [StatsBomb Open Data](https://github.com/statsbomb/open-data) | Match events + 360 context | Nested JSON | ~3,000 matches |
| [Metrica Sports](https://github.com/metrica-sports/sample-data) | Optical tracking (25 fps) | CSV | Sample matches |
| [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) | Event streams | JSON | Top 5 European leagues |
| *Respo.Vision* (planned) | 3D pose tracking | JSON | TBD |

## Analytics

Built on the [Soccermatics](https://soccermatics.readthedocs.io/) curriculum by David Sumpter:

- **Expected Goals (xG)** — Shot quality model using distance, angle, body part
- **Expected Threat (xT)** — Pitch zone valuation via Markov chains
- **Pass Networks** — Team passing structure and progressive passes
- **Pitch Control** — Voronoi diagrams from tracking data
- **Player Similarity** — pgvector cosine-distance search ("Find players like X")
- **Player Radar Charts** — Per-90 stat comparison across multiple metrics

## Project Structure

```
luxury-lakehouse/
├── terraform/          # Infrastructure as Code (Databricks on AWS)
├── src/
│   ├── ingestion/      # Data ingestion from StatsBomb, Metrica, Wyscout
│   └── streamlit_app/  # Interactive analytics dashboard
├── dbt_project/        # Bronze → Silver → Gold transformations
├── docs/
│   └── c4/             # C4 architecture diagrams (Structurizr DSL)
├── documents/          # Reference PDFs and architecture diagrams
└── PLAN.md             # Detailed implementation plan
```

## Status

**Phase 6: StatsBomb 360 Freeze Frames** — Bronze backfill function + dbt staging model for 360 freeze-frame data (player positions at moment of each event). See [PLAN.md](PLAN.md) for the full implementation plan.

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

## Tech Stack

| Category | Tool |
|----------|------|
| Cloud | AWS (us-east-1) + Databricks Premium |
| IaC | Terraform with `databricks/databricks` provider |
| Data Lake | Delta Lake on Unity Catalog |
| OLTP Database | Databricks Lakebase (PostgreSQL 17, autoscaling) |
| Transformations | dbt-core + dbt-databricks |
| Orchestration | Databricks Serverless Workflows |
| Application | Streamlit + mplsoccer |
| Vector Search | pgvector (native in Lakebase) |
| Python | 3.10+ (Databricks serverless), managed with uv |
| Linting | ruff + sqlfluff |
| CI/CD | GitHub Actions |
| Architecture Docs | C4 diagrams (Structurizr DSL) |

## License

[Apache License 2.0](LICENSE) &mdash; see [NOTICE](NOTICE) for third-party data attribution.

---

<sub>Named after Monty Python's *Four Yorkshiremen* sketch, where each comedian one-ups the others about how deprived their childhood was. In data engineering, moving from hand-managed EC2 instances and 5-hop Reverse ETL pipelines to serverless Lakebase truly is... right luxury.</sub>
