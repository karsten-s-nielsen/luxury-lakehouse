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

> **Try it now:** [Full Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; 14-page Taipy app with live data from 380+ matches across 5 providers. Or explore the [Gradio Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) for a quick look.

---

## Platform Scale &amp; Data Engineering

The infrastructure uses a **Medallion architecture** (Bronze &rarr; Silver &rarr; Gold) provisioned entirely via Terraform IaC, unifying multi-vendor event and tracking data into a single analytical layer.

- **38M+ tracking frames** ingested from three optical tracking providers (25fps and 10fps)
- **5 distinct data sources** unified: StatsBomb, Wyscout, Metrica Sports, IDSSE (Bundesliga), and SkillCorner (A-League)
- **[14 Taipy dashboard pages](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)** deployed on Hugging Face Spaces (Docker SDK), querying Lakebase PostgreSQL via OAuth
- **34 synced tables** with Zero-ETL continuous sync from Gold Delta Lake to Lakebase PostgreSQL 17
- **56 PostgreSQL indexes** (50 btree + 6 HNSW vector indexes: 4x128d + 2x144d) for sub-10ms OLTP queries
- Pipeline reliability enforced through **1,135 unit tests** and **381+ dbt data tests**

## The Hugging Face Footprint

All public artifacts are hosted entirely within the HF ecosystem.

### Models

| Model | Architecture | Scale |
|-------|-------------|-------|
| [football2vec-v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) | Transformer encoder (128-dim) + adversarial team debiasing (Ganin GRL) | 87K per-match vectors across 8,950 players, debiased for team identity |
| [football2vec-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | Doc2Vec (PV-DM) 32-dim behavioral embeddings (v1 baseline) | 87K per-match vectors across 8,950 players from ~3,000 matches |
| [xg-model-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) | Calibrated XGBoost + logistic baseline (13 features) | Trained on ~131K shots, ROC-AUC 0.979 on held-out test set |
| [vaep-model-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/vaep-model-statsbomb-wyscout) | 2&times; XGBClassifier (P(scores) + P(concedes)) | Trained on ~2,388 matches from StatsBomb + Wyscout |
| [xg-v2-model-set-encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Deep Sets (Zaheer et al. 2017) + MC dropout (Gal &amp; Ghahramani 2016) | ROC-AUC 0.915, trained on ~131K shots with 360 freeze frames |
| [psxg-model](https://huggingface.co/luxury-lakehouse/psxg-model) | Logistic regression on goalmouth coordinates (Butcher et al. 2025) | Trained on ~15K on-target shots, JSON-serialised weights |
| [football2vec-360](https://huggingface.co/luxury-lakehouse/football2vec-360) | Transformer encoder (128-dim) + Deep Sets 360 context (16-dim) = 144-dim | 323 StatsBomb 360 matches, adversarial team debiasing |

All model serialization uses **JSON envelopes** &mdash; zero pickle files (banned by project security policy).

### Datasets

| Dataset | Scale | Description |
|---------|-------|-------------|
| [spadl-vaep-action-values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | ~9.5M actions | Per-action offensive/defensive VAEP valuations |
| [line-breaking-passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) | ~5M passes | All passes with defensive line-breaking labels via Ward clustering on 360 freeze frames |
| [football2vec-player-embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | 87K vectors | Pre-computed behavioral (128-d transformer) + statistical (13-d) player vectors |
| [football2vec-training-data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) | ~87K sequences | Tokenized SPADL action sequences for transformer training |
| [pitch-control-tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | 38M frames | Per-player per-frame Spearman (2017) physics-based pitch control |
| [expected-threat-grids](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids) | 12x8 grid | Data-driven Expected Threat values computed from 2.2M SPADL actions |
| [obso-pausa-inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) | 7 matches | ELASTIC-synced event-tracking inputs for OBSO/PAUSA computation |
| [obso-pausa-values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | ~3,500 passes | PAUSA pass timing scores with OBSO temporal/spatial decomposition |
| [obso-trained-grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | 8 competitions + global | Data-driven ball reachability (100&times;64) + EPV (50&times;32) grids for OBSO |
| [xg-freeze-frame-data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data) | 137K player rows | StatsBomb 360 freeze-frame player positions for xG v2 set encoder |
| [xg-shot-data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | 131K shots | Tabular shot features from StatsBomb + Wyscout for xG model training |
| [space-creation-values](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values) | 875K player-frames | Per-player space creation/destruction via differential OBSO (Fernandez &amp; Bornn 2018) |
| [statsbomb-shots-on-target](https://huggingface.co/datasets/luxury-lakehouse/statsbomb-shots-on-target) | ~15K shots | On-target shots with goalmouth coordinates for PSxG training |
| [psxg-predictions](https://huggingface.co/datasets/luxury-lakehouse/psxg-predictions) | ~15K shots | Per-shot PSxG probabilities from logistic model |
| [football2vec-360-training-data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-training-data) | ~2M actions | SPADL action sequences with 360 freeze frame context |
| [football2vec-360-embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-embeddings) | ~4K players | 144-dim player embeddings from 360-enriched model |

### Interactive Spaces

| Space | What it is |
|-------|-----------|
| [**Soccer Analytics App**](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) | Full 14-page Taipy dashboard (Docker SDK) querying Lakebase PostgreSQL via OAuth. Live data from 380+ matches. Shot maps, pass networks, player comparison, pitch control, PAUSA pass timing, DEFCON defensive pressure, and more. |
| [**Soccer Analytics Demo**](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) | Lightweight 6-tab Gradio explorer with pre-cached Parquet data. No database dependency &mdash; instant load for quick exploration. |

## Compute &amp; Bidirectional Sync

While Databricks handles core data engineering, we use **HF Jobs** for workloads where a serverless Python environment is the right tool.

**Examples:**

- **Expected Threat grids** run as a CPU-based HF Jobs pipeline &mdash; downloads SPADL data from an HF Dataset, computes Markov chain value iteration, and publishes xT grids back to the Hub.
- **xG v2 neural model** trains on an A10G GPU via HF Jobs &mdash; a Deep Sets architecture with MC dropout, processing 131K shots with 360 freeze-frame context, exporting pure-NumPy weights for serverless inference.
- **Space Creation** computes per-player counterfactual pitch control surfaces on A10G via JAX double-`vmap` &mdash; 875K player-frame values across 40K frames in under 6 minutes.

All HF Jobs scripts use PEP 723 inline script metadata for zero-setup reproducibility.

Model weights published to HF Hub are synced back to **Databricks UC Volumes** for inference in the production Taipy app. This creates a bidirectional flow: Databricks produces training data &rarr; HF Hub hosts artifacts &rarr; Databricks consumes model weights for scoring.

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
| **Space Creation** | Fernandez &amp; Bornn, "Wide Open Spaces" (2018), differential OBSO integration |
| **xG v2 Set Encoder** | Zaheer et al., "Deep Sets" (NeurIPS 2017); Gal &amp; Ghahramani, "Dropout as Bayesian Approximation" (ICML 2016) |
| **Pass Networks** | Pena &amp; Touchette, "A network theory analysis of football strategies" (2012) |

## Engineering Quality

The platform maintains professional-grade engineering standards:

- **Security**: OAuth M2M everywhere, HTTPS-only, zero secrets in code, input validation on all identifiers, SSL verification enforced, JSON-only model serialization
- **Type safety**: Pyright basic mode, Pydantic models for configuration
- **Testing**: 807 pytest unit tests (819+ with gensim, including performance benchmarks), 381 dbt data quality tests
- **CI/CD**: GitHub Actions with OIDC federation (zero-secret CI), ruff linting, pre-commit hooks
- **UX discipline**: 71 of 78 findings resolved across two cognitive interface audits (CHI-AUDIT-180, CHI-AUDIT-190), grounded in 15 HCI frameworks including Norman, Sweller, Gergle, Kahneman, and Cleveland &amp; McGill. Every metric has a help tooltip, every page has academic citations, and every analytics term is defined in a context-sensitive glossary.

## Links

- **License**: [Apache 2.0](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/LICENSE)

<sub>Named after Monty Python's <em>Four Yorkshiremen</em> sketch, where each comedian one-ups the others about how deprived their childhood was. In data engineering, moving from hand-managed EC2 instances and 5-hop Reverse ETL pipelines to serverless Lakebase truly is... right luxury.</sub>
