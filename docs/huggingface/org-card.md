---
title: README
emoji: ⚽
colorFrom: yellow
colorTo: gray
sdk: static
pinned: false
---

<p align="center">
  <img src="luxury-lakehouse.jpg" alt="Luxury Lakehouse" width="400">
</p>

# (Right! Luxury!) Lakehouse

> *"Luxury! We used to dream of serverless!"*

Open-source soccer analytics platform built on **Databricks Lakebase** &mdash; replacing a 6-service traditional AWS pipeline with a unified lakehouse architecture that scales to zero. The Hugging Face Hub serves as the public distribution layer for models, datasets, and interactive demos.

> **Try it now:** [Interactive Gradio Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) &mdash; pass quality, pitch control, player similarity, shot maps, and defensive pressure from open-source soccer data.

---

## Platform Scale &amp; Data Engineering

The infrastructure uses a **Medallion architecture** (Bronze &rarr; Silver &rarr; Gold) provisioned entirely via Terraform IaC, unifying multi-vendor event and tracking data into a single analytical layer.

- **38M+ tracking frames** ingested from three optical tracking providers (25fps and 10fps)
- **5 distinct data sources** unified: StatsBomb, Wyscout, Metrica Sports, IDSSE (Bundesliga), and SkillCorner (A-League)
- **12 Streamlit dashboard pages** deployed natively on Databricks Apps, serving coaches, scouts, and analysts
- **19 synced tables** with Zero-ETL continuous sync from Gold Delta Lake to Lakebase PostgreSQL 17
- **38 PostgreSQL indexes** (34 btree + 4 HNSW vector indexes) for sub-10ms OLTP queries
- Pipeline reliability enforced through **704 unit tests** (714+ with gensim) and **381 dbt data tests**

## The Hugging Face Footprint

All public artifacts are hosted entirely within the HF ecosystem.

### Models

| Model | Architecture | Scale |
|-------|-------------|-------|
| [football2vec-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | Doc2Vec (PV-DM) 32-dim behavioral embeddings | 87K per-match vectors across 8,950 players from ~3,000 matches |
| [xg-model-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Calibrated XGBoost + logistic baseline (13 features) | Trained on ~131K shots, ROC-AUC 0.979 on held-out test set |

All model serialization uses **JSON envelopes** &mdash; zero pickle files (banned by project security policy).

### Datasets

| Dataset | Scale | Description |
|---------|-------|-------------|
| [spadl-vaep-action-values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | ~9.5M actions | Per-action offensive/defensive VAEP valuations |
| [line-breaking-passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) | ~5M passes | All passes with defensive line-breaking labels via Ward clustering on 360 freeze frames |
| [football2vec-player-embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | 87K vectors | Pre-computed behavioral (32-d) + statistical (13-d) player vectors |
| [pitch-control-tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | 38M frames | Per-player per-frame Spearman (2017) physics-based pitch control |
| [expected-threat-grids](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids) | 12x8 grid | Data-driven Expected Threat values computed from 2.2M SPADL actions |
| [obso-pausa-inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) | 7 matches | ELASTIC-synced event-tracking inputs for OBSO/PAUSA computation |
| [obso-pausa-values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | ~3,500 passes | PAUSA pass timing scores with OBSO temporal/spatial decomposition |

### Interactive Demo

The [Soccer Analytics Explorer](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) is a 6-tab Gradio Space demonstrating pass quality analysis, physics-based pitch control surfaces, player similarity search, shot maps, DEFCON defensive pressure breakdowns, and PAUSA pass timing analysis &mdash; all running on pre-cached Parquet data with no database dependency.

## Compute &amp; Bidirectional Sync

While Databricks handles core data engineering, we use **HF Jobs** for workloads where a serverless Python environment is the right tool.

**Example: Expected Threat grid computation** runs as an automated HF Jobs pipeline. It downloads SPADL data directly from an HF Dataset, computes the Markov chain value iteration, and publishes the resulting xT grids back to the Hub &mdash; using PEP 723 inline script metadata for zero-setup reproducibility.

Model weights published to HF Hub are synced back to **Databricks UC Volumes** for inference in the production Streamlit app. This creates a bidirectional flow: Databricks produces training data &rarr; HF Hub hosts artifacts &rarr; Databricks consumes model weights for scoring.

## Academic Foundations

Every analytics module is grounded in peer-reviewed research, cited directly in the platform UI:

| Module | Foundation |
|--------|-----------|
| **Pitch Control** | Spearman, "Beyond Expected Goals" (2017) |
| **Expected Threat** | Karun Singh (2018), Markov chain value iteration |
| **VAEP** | Decroos et al., "Actions Speak Louder than Goals" (2019) |
| **DEFCON** | Kim et al., defensive contribution framework (2025) |
| **Player Embeddings** | Le &amp; Mikolov, Doc2Vec (2014); Theiner et al., football2vec (2022) |
| **Line-Breaking** | Ward clustering on StatsBomb 360 freeze frames; adapted from Parma Calcio 1913 |
| **xG Model** | Rathke, "An examination of expected goals" (2017); XGBoost with isotonic calibration |
| **PAUSA** | Lee et al., "Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed" (2026) |
| **Pass Networks** | Pena &amp; Touchette, "A network theory analysis of football strategies" (2012) |

## Engineering Quality

The platform maintains professional-grade engineering standards:

- **Security**: OAuth M2M everywhere, HTTPS-only, zero secrets in code, input validation on all identifiers, SSL verification enforced, JSON-only model serialization
- **Type safety**: Pyright basic mode, Pydantic models for configuration
- **Testing**: 704 pytest unit tests (714+ with gensim, including performance benchmarks), 381 dbt data quality tests
- **CI/CD**: GitHub Actions with OIDC federation (zero-secret CI), ruff linting, pre-commit hooks
- **UX discipline**: 50 of 53 findings resolved from a cognitive interface audit grounded in 15 HCI frameworks (Norman, Sweller, Gergle, Kahneman, Cleveland &amp; McGill, and others) &mdash; every metric has a help tooltip, every page has academic citations, every analytics term is defined in a context-sensitive glossary

## Links

- **License**: [Apache 2.0](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/LICENSE)

<sub>Named after Monty Python's <em>Four Yorkshiremen</em> sketch, where each comedian one-ups the others about how deprived their childhood was. In data engineering, moving from hand-managed EC2 instances and 5-hop Reverse ETL pipelines to serverless Lakebase truly is... right luxury.</sub>
