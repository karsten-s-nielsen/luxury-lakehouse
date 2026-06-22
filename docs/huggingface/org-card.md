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

> **Try it now:** [Full Dashboard](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) &mdash; 17-page Taipy app with live data from ~444+ matches across 6 providers.

---

## Platform Scale &amp; Data Engineering

The infrastructure uses a **Medallion architecture** (Bronze &rarr; Silver &rarr; Gold) provisioned entirely via Terraform IaC, unifying multi-vendor event and tracking data into a single analytical layer.

- **38M+ tracking frames** ingested from three optical tracking providers (25fps and 10fps)
- **6 distinct data sources** unified: StatsBomb, Wyscout, Metrica Sports, IDSSE (Bundesliga), SkillCorner (A-League), and Gradient Sports (WC 2022)
- **[17 Taipy dashboard pages](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app)** deployed on Hugging Face Spaces (Docker SDK), querying Lakebase PostgreSQL via Databricks OAuth
- **41 synced tables** with Zero-ETL continuous sync from Gold Delta Lake to Lakebase PostgreSQL 17
- **72 PostgreSQL indexes** (66 btree + 6 HNSW vector indexes: 4x192d + 2x208d) for sub-10ms OLTP queries
- Pipeline reliability enforced through **2,712+ unit tests** and **862+ dbt data tests**

## The Hugging Face Footprint

All public artifacts are hosted entirely within the HF ecosystem.

### Models

| Model | Architecture | Scale |
|-------|-------------|-------|
| [football2vec-v2](https://huggingface.co/luxury-lakehouse/football2vec-v2) | Transformer encoder (192-dim) + adversarial competition debiasing (Ganin GRL) | 114K per-match vectors across 22 competitions, debiased for competition identity |
| [football2vec-statsbomb-wyscout](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) | Doc2Vec (PV-DM) 32-dim behavioral embeddings (v1 baseline) | 87K per-match vectors across 8,950 players from ~3,000 matches |
| [vaep-model](https://huggingface.co/luxury-lakehouse/vaep-model) | 2&times; XGBClassifier (P(scores) + P(concedes)) | Trained on ~5,414 matches across all providers (StatsBomb, Wyscout, IDSSE, Metrica, SkillCorner) |
| [xg-v2-model-set-encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder) | Deep Sets (Zaheer et al. 2017) + MC dropout (Gal &amp; Ghahramani 2016) | ROC-AUC 0.915, trained on ~131K shots with 360 freeze frames |
| [psxg-model](https://huggingface.co/luxury-lakehouse/psxg-model) | Logistic regression on goalmouth coordinates (Butcher et al. 2025) | Trained on ~15K on-target shots, JSON-serialised weights |
| [football2vec-360](https://huggingface.co/luxury-lakehouse/football2vec-360) | Transformer encoder (192-dim) + Deep Sets 360 context (16-dim) = 208-dim | 323 StatsBomb 360 matches, adversarial team debiasing |
| [pitch-control](https://huggingface.co/luxury-lakehouse/pitch-control) | Physics-based team-control probability surface (Spearman 2017) | Heuristic method card &mdash; no trained weights; substrate for OBSO / Off-Ball xT / Space Creation |
| [defcon](https://huggingface.co/luxury-lakehouse/defcon) | XGBoost counterfactual value estimator (Kim et al. 2025 DEFCON-lite) | Inline-trained per run; per-defender credit assignment on open 360/tracking data |
| [off-ball-xt](https://huggingface.co/luxury-lakehouse/off-ball-xt) | Heuristic xT &times; pitch-control attribution (Singh 2018, Spearman 2017) | Method card &mdash; attributes attacking threat to off-ball players |
| [obso-pausa-method](https://huggingface.co/luxury-lakehouse/obso-pausa-method) | OBSO surface + PAUSA pass timing (Spearman 2018; Fern&aacute;ndez &amp; Bornn 2018; Lee et al. 2026) | Method card &mdash; pass-timing counterfactuals on GPU-accelerated OBSO |
| [space-creation-method](https://huggingface.co/luxury-lakehouse/space-creation-method) | Counterfactual pitch control (Fern&aacute;ndez &amp; Bornn 2018) | Method card &mdash; per-player EPV-weighted space-creation value |
| [scoutgpt](https://huggingface.co/luxury-lakehouse/scoutgpt) | Transformer decoder with per-action player conditioning (Hong et al. 2025) | Sequence-aware player embedding model &mdash; captures tempo + build-up patterns |
| [scoutgpt-variant-rope](https://huggingface.co/luxury-lakehouse/scoutgpt-variant-rope) | ScoutGPT ablation: RoPE position encoding (Su et al. 2021) | Research artefact &mdash; rope-scoutgpt A/B cycle 2026-04-22 |
| [scoutgpt-variant-learnable](https://huggingface.co/luxury-lakehouse/scoutgpt-variant-learnable) | ScoutGPT ablation: learnable absolute positions (Vaswani et al. 2017 baseline) | Research artefact &mdash; A/B comparison baseline |
| [scoutgpt-l2-harvest](https://huggingface.co/luxury-lakehouse/scoutgpt-l2-harvest) | OpenEvolve L2 seed-program evaluations (metrics.json per seed) | Research artefact &mdash; evolve-engine audit trail |
| [football2vec-l2-harvest](https://huggingface.co/luxury-lakehouse/football2vec-l2-harvest) | OpenEvolve L2 seed-program evaluations for Football2Vec v2 | Research artefact &mdash; evolve-engine audit trail |

All model serialization uses **JSON envelopes** &mdash; zero pickle files (banned by project security policy). Every model card above carries an **EU AI Act &mdash; Intended Use and Non-Use** stanza per the project's [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md) gap analysis (SEC1, April 2026).

### Datasets

| Dataset | Scale | Description |
|---------|-------|-------------|
| [spadl-vaep-action-values](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) | ~9.5M actions | Per-action offensive/defensive VAEP valuations. **Dual-column schema through 2026-07-22** &mdash; `match_id` / `competition_id` sunset then; migrate consumers to `match_key` / `competition_key` per [ADR-011](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-011-unified-kimball-match-dimension.md). |
| [line-breaking-passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) | ~5M passes | All passes with defensive line-breaking labels via Ward clustering on 360 freeze frames. **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [football2vec-player-embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) | 114K vectors | Pre-computed behavioral (192-d transformer) + statistical (13-d) player vectors. **Dual-column schema through 2026-07-22** &mdash; legacy `canonical_player_id` sunset then; migrate to `player_key`. |
| [football2vec-training-data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-training-data) | ~114K sequences | Tokenized SPADL action sequences for transformer training. **Dual-column schema through 2026-07-22** &mdash; legacy `canonical_player_id` sunset then; migrate to `player_key`. |
| [pitch-control-tracking](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) | 38M frames | Per-player per-frame Spearman (2017) physics-based pitch control. **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [expected-threat-grids](https://huggingface.co/datasets/luxury-lakehouse/expected-threat-grids) | 12x16 grid | Data-driven Expected Threat values computed from 2.2M SPADL actions |
| [obso-pausa-inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) | 7 matches | ELASTIC-synced event-tracking inputs for OBSO/PAUSA computation |
| [obso-pausa-values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values) | 1,627 passes (7 IDSSE matches) | PAUSA pass timing scores with OBSO temporal/spatial decomposition. Provider scope: continuous-tracking + frame-aligned events &mdash; today IDSSE only; Metrica is the next candidate. **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [obso-trained-grids](https://huggingface.co/datasets/luxury-lakehouse/obso-trained-grids) | 8 competitions + global | Data-driven ball reachability (100&times;64) + EPV (50&times;32) grids for OBSO |
| [xg-freeze-frame-data](https://huggingface.co/datasets/luxury-lakehouse/xg-freeze-frame-data) | 137K player rows | StatsBomb 360 freeze-frame player positions for xG v2 set encoder. **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [xg-shot-data](https://huggingface.co/datasets/luxury-lakehouse/xg-shot-data) | 131K shots | Tabular shot features from StatsBomb + Wyscout for xG model training. **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [space-creation-values](https://huggingface.co/datasets/luxury-lakehouse/space-creation-values) | 875K player-frames | Per-player space creation/destruction via differential OBSO (Fernandez &amp; Bornn 2018). **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [statsbomb-shots-on-target](https://huggingface.co/datasets/luxury-lakehouse/statsbomb-shots-on-target) | ~15K shots | On-target shots with goalmouth coordinates for PSxG training. **Dual-column schema through 2026-07-22** &mdash; legacy `match_id` sunset then; migrate to `match_key`. |
| [psxg-predictions](https://huggingface.co/datasets/luxury-lakehouse/psxg-predictions) | ~15K shots | Per-shot PSxG probabilities from logistic model |
| [psxg-shots](https://huggingface.co/datasets/luxury-lakehouse/psxg-shots) | ~33K shots | Shot-grain post-shot xG (PSxG/xGOT), all providers; projected (StatsBomb) / measured (tracking) goalmouth geometry + shooter &amp; defending-GK keys. 4-feature logistic, OOS AUC 0.818 ([ADR-060](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/docs/superpowers/adrs/ADR-060-psxg-projected-goalmouth-four-feature-model.md)). GradientSports partitions live in a private org-members-only companion repo (ADR-049). |
| [football2vec-360-training-data](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-training-data) | ~2M actions | SPADL action sequences with 360 freeze frame context. **Dual-column schema through 2026-07-22** &mdash; legacy `canonical_player_id` sunset then; migrate to `player_key`. |
| [football2vec-statsbomb-wyscout](https://huggingface.co/datasets/luxury-lakehouse/football2vec-statsbomb-wyscout) | 114K vectors | Per-match v2 transformer (192-dim) raw embeddings with adversarial competition debiasing. **Dual-column schema through 2026-07-22** &mdash; legacy `canonical_player_id` sunset then; migrate to `player_key`. |
| [football2vec-360-embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-360-embeddings) | ~4K players | 208-dim player embeddings from 360-enriched model. **Dual-column schema through 2026-07-22** &mdash; legacy `canonical_player_id` sunset then; migrate to `player_key`. |
| [scoutgpt-training-data](https://huggingface.co/datasets/luxury-lakehouse/scoutgpt-training-data) | 894K episodes | SPADL possession episodes with per-action player attribution (Hong et al. 2025) |
| [spadl-tracking-context](https://huggingface.co/datasets/luxury-lakehouse/spadl-tracking-context) | 25,322 actions | Per-action tracking-derived features (66 columns) from 20 matches across IDSSE, Metrica, and SkillCorner |
| [pining-for-the-data](https://huggingface.co/datasets/luxury-lakehouse/pining-for-the-data) | 10 matches | SkillCorner open tracking data (V3 format) redistributed under MIT |

### Interactive Spaces

| Space | What it is |
|-------|-----------|
| [**Soccer Analytics App**](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-app) | Full 17-page Taipy dashboard (Docker SDK) querying Lakebase PostgreSQL via Databricks OAuth. Live data from ~444+ matches. Shot maps, pass networks, player comparison, GK analytics, tactical positions, pitch control, PAUSA pass timing, DEFCON defensive pressure, and more. |

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
| **Pitch Control** | Spearman, "Physics-Based Modeling of Pass Probabilities in Soccer" (2017) |
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
| **ScoutGPT Decoder** | Hong et al., "ScoutGPT: Player-conditioned Football Language Model for Counterfactual Evaluation" (2025, arXiv:2512.17266) |

## Engineering Quality

The platform maintains professional-grade engineering standards:

- **Security**: OAuth M2M everywhere, HTTPS-only, zero secrets in code, input validation on all identifiers, SSL verification enforced, JSON-only model serialization
- **Type safety**: Pyright basic mode, Pydantic models for configuration
- **Testing**: 2,712+ pytest unit tests (including performance benchmarks), 862+ dbt data quality tests
- **CI/CD**: GitHub Actions with OIDC federation (zero-secret CI), ruff linting, import-linter boundary enforcement, pre-commit hooks
- **UX discipline**: 71 of 78 findings resolved across two cognitive interface audits (CHI-AUDIT-180, CHI-AUDIT-190), grounded in 15 HCI frameworks including Norman, Sweller, Gergle, Kahneman, and Cleveland &amp; McGill. Every metric has a help tooltip, every page has academic citations, and every analytics term is defined in a context-sensitive glossary.
- **AI governance**: The project is assessed against Regulation (EU) 2024/1689 (the EU AI Act). Under the current operating posture &mdash; a solo research project on public data, not sold or licensed to clubs, not used for employment decisions &mdash; none of the thirteen per-player evaluative ML systems is classified as high-risk. Every model card carries an explicit intended-use / non-use stanza; the full gap analysis, conformity-assessment mapping, and re-classification triggers live in [`AI_GOVERNANCE.md`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/AI_GOVERNANCE.md). Enforcement is via [`src/tests/test_ai_governance_md.py`](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/src/tests/test_ai_governance_md.py), which fails CI if the document drifts from the workflow-card inventory or if the annual review date goes more than 30 days stale.

## Links

- **License**: [Apache 2.0](https://github.com/karsten-s-nielsen/luxury-lakehouse/blob/main/LICENSE)

<sub>Named after Monty Python's <em>Four Yorkshiremen</em> sketch, where each comedian one-ups the others about how deprived their childhood was. In data engineering, moving from hand-managed EC2 instances and 5-hop Reverse ETL pipelines to serverless Lakebase truly is... right luxury.</sub>
