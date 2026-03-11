# (Right! Luxury!) Lakehouse — Roadmap

Research directions, long-horizon features, and exploratory ideas beyond the [phased plan](PLAN.md). Items here are **unscheduled** — they represent valuable directions that may graduate into numbered phases as prerequisites are met and priorities clarify.

**Last updated**: 2026-03-11

---

## Observability Layer (OpenTelemetry)

**Status:** Research complete, ready for implementation
**Budget:** ~$1-2/month (personal) or enterprise-swappable via config

The platform currently has minimal observability (PLAN.md &sect;6.4): Databricks audit logs, dbt test results, and Streamlit built-in metrics. No structured telemetry, no model validation, no pipeline performance tracking. This section defines a proper observability layer using OpenTelemetry as the instrumentation standard.

### Core principle: instrument once, observe anywhere

OpenTelemetry's dual-export pattern decouples instrumentation from backend. Application code emits telemetry via the OTel SDK. An OTel Collector routes signals to cheap local storage or enterprise backends &mdash; controlled by a single environment variable, zero code changes.

```
Application Code (OTel SDK)
         |
    OTLP/HTTP
         |
    OTel Collector
         |
    +----+----+
    |         |
 personal  enterprise
    |         |
  S3 bucket  Grafana Cloud / Datadog / etc.
```

**Mode switching:** Layered Collector config files selected by `OTEL_MODE` env var. The `awss3exporter` (contrib) writes time-partitioned OTLP JSON directly to S3. The `otlp/http` exporter sends to any OTLP-compatible backend. Both can run simultaneously (fan-out).

### Cost tiers

| Tier | Stack | Monthly Cost |
|------|-------|-------------|
| **Personal** | OTel Collector &rarr; S3 + DuckDB/Athena queries | ~$1-2 |
| **Mid-range** | Grafana LGTM (Loki + Tempo + Mimir) on t3.medium | ~$35 |
| **Enterprise** | Swap Collector config to Grafana Cloud / Datadog / Splunk | Varies |

The personal tier has no always-on infrastructure. S3 storage is $0.023/GB/month. Query with DuckDB locally (`duckdb-otlp` community extension) or Athena serverlessly ($5/TB scanned). The `otlp2parquet` tool can convert OTLP to Parquet/Iceberg for columnar analytics.

### Pipeline instrumentation layers

Each layer of the platform gets structured telemetry with configurable granularity:

| Layer | Instrumentation | Signal Type | Auto/Manual |
|-------|----------------|-------------|-------------|
| **Ingestion** (StatsBomb, Metrica, etc.) | HTTP calls, Delta writes, row counts, duration | Traces + Metrics | Auto (`requests`) + manual spans |
| **dbt transformations** | Per-model execution time, test pass/fail, row counts | Metrics | Parse `run_results.json` post-build |
| **Analytics models** (xG, xT, VAEP, pitch control) | Input/output stats, drift metrics, validation status | Traces + Metrics | Manual spans with `analytics.*` attributes |
| **Streamlit app** | Lakebase query latency, page render time | Traces | Auto (`psycopg2`) + manual spans |

**Python OTel SDK** (v1.39.1, stable): Auto-instrumentation available for `requests` and `psycopg2` via `opentelemetry-instrumentation-*` packages. No Streamlit or PySpark auto-instrumentation &mdash; use manual spans around key operations.

**Custom attribute namespace** for analytics models (no official OTel semantic conventions exist for traditional ML):

| Attribute | Example |
|-----------|---------|
| `analytics.model.name` | `"xg_logistic"`, `"vaep_spadl"`, `"pitch_control_spearman"` |
| `analytics.model.version` | `"v2"` |
| `analytics.model.input_count` | `3400` |
| `analytics.model.output_mean` | `0.098` |
| `analytics.model.output_p90` | `0.312` |
| `analytics.model.drift_psi` | `0.15` |
| `analytics.pipeline.source` | `"statsbomb"`, `"skillcorner"` |

### Model validation: catching offside runners

Detect when analytics models produce bad outputs using statistical process control and drift detection. All methods use `scipy` + `numpy` only &mdash; no new dependencies.

| Model | Monitor | Detection | Threshold |
|-------|---------|-----------|-----------|
| **xG** | Mean prediction per match | PSI (Population Stability Index) | PSI > 0.2 = significant shift |
| **xT** | Zone coverage distribution | Wasserstein distance vs reference grid | Quantifies magnitude of shift |
| **VAEP** | Fraction of negative actions, distribution shape | KS test + Wasserstein | Two-sample distribution comparison |
| **Pitch Control** | Field sum &asymp; 1.0 | Hard constraint check | > 5% error = calculation bug |
| **Line-breaking** | Detection rate per match | CUSUM (cumulative sum) | Sustained drift beyond 3&sigma; |
| **Physical stats** | Max speed, acceleration | Range bound | Max speed > 15 m/s = unit conversion error |

**CUSUM** is particularly valuable: O(n), ~10 lines of pure Python, detects sustained small shifts that single-match thresholds miss. Ideal for catching a slowly miscalibrating model over a season.

**Reference baselines** stored as dbt seeds (following the `expected_threat_grid.csv` pattern) or a small Delta table `dev_gold.model_baselines`.

### Open-source monitoring tools (all Apache 2.0)

| Tool | Key Capability | Integration Path |
|------|---------------|-----------------|
| **Evidently AI** | 100+ pre-built drift metrics, HTML reports | Prometheus bridge &rarr; OTel Collector |
| **NannyML** | CBPE: estimate performance *without ground truth* | DataFrame output &rarr; OTel metric emission |
| **WhyLogs** | Lightweight statistical profiles, Spark-compatible | Profile diffs &rarr; OTel attributes |

NannyML's CBPE (Confidence-Based Performance Estimation) is especially relevant for models like xT and pitch control where "correct answers" are ambiguous &mdash; it estimates performance degradation from output distribution alone.

### MLflow integration (included in Databricks workspace)

MLflow provides model registry and batch evaluation at no additional cost:

- `mlflow.evaluate()` for post-match batch validation (Brier score, calibration error)
- Unity Catalog Model Registry for version tracking and aliases (`@champion`, `@challenger`)
- Native OTel dual export (`MLFLOW_TRACE_ENABLE_OTLP_DUAL_EXPORT=true`) writes spans to both MLflow Tracking and any external OTLP endpoint
- Databricks managed OTLP endpoint (Zerobus) writes telemetry to Delta tables in Unity Catalog

### dbt pipeline observability

Three approaches evaluated; **artifact parsing** is recommended for dbt Core:

| Approach | Maturity | Fit |
|----------|----------|-----|
| **dbt Fusion native OTLP** | Preview (2026) | Future &mdash; requires dbt Core migration |
| **Elementary** | Production | Rich features, but no native OTel export |
| **Artifact parsing** (recommended) | Stable | Parse `target/run_results.json`, emit OTel counters. Zero dependency. |

Post-run parsing captures per-model execution time, test pass/fail counts, failure details, and source freshness &mdash; all emitted as OTel metrics.

### Databricks constraints

- OTel Collector **cannot run as sidecar** in serverless compute (Databricks-managed infrastructure)
- SDK-level OTLP/HTTP export works from within jobs/notebooks to an external endpoint
- Databricks system tables (`system.compute`, `system.workflow`, `system.audit`) cover infrastructure; OTel covers application logic &mdash; they are complementary and coexist in Unity Catalog
- MLflow dual export is the cleanest integration path for Databricks workloads

### Key tools

| Tool | Purpose |
|------|---------|
| `opentelemetry-sdk` (v1.39.1) | Python instrumentation API |
| `opentelemetry-instrumentation-requests` | Auto-instrument HTTP calls |
| `opentelemetry-instrumentation-psycopg2` | Auto-instrument Lakebase queries |
| `awss3exporter` (Collector contrib) | Write OTLP to S3, time-partitioned |
| `otlp2parquet` | Convert OTLP to Parquet/Iceberg |
| `duckdb-otlp` | Query OTLP data from S3 via DuckDB |

### Open questions

1. **Collector location**: Sidecar in Streamlit App container? Separate ECS task? Lambda?
2. **S3 bucket**: Dedicated telemetry bucket or partition within existing infrastructure?
3. **Query interface**: DuckDB (free, local) vs Athena (serverless, pay-per-query)?
4. **Ingestion granularity**: Per-match spans or per-batch spans?
5. **Real-time UI**: Is the $35/month Grafana LGTM tier worth it, or is S3 + DuckDB sufficient?

### Dependencies

- No blocking dependencies &mdash; can be implemented at any time
- Synergistic with Staging Environment (observability validates staging deployments)
- Foundation for DEFCON (Phase 17) model monitoring

---

## Pipeline Optimization & Scaling (Enterprise Integration Patterns)

**Status:** Initial optimization complete (2026-03-11); EIP patterns ready for next-level scaling
**Budget:** $0 incremental (uses existing serverless compute + Delta Lake)
**References:** Hohpe & Woolf (2003) *Enterprise Integration Patterns*; Sp&auml;ti, *DEDP/PoDE* (dedp.online, open access)

The platform's compute pipelines have been migrated from driver-bound loops to `applyInPandas` (Spark-distributed), with incremental skip guards and per-partition memory management on all ingestion modules. The pure-Python analytics modules (`src/analytics/`) work standalone without Spark for community/personal use. As data volume grows &mdash; especially with Respo.Vision 3D pose tracking (est. ~7M rows/match vs ~1.9M current) &mdash; horizontal scaling via Enterprise Integration Patterns enables the next level of throughput.

### Core principle: split, scatter, cache

Classic EIP patterns map directly to Databricks primitives. The medallion architecture is already Pipes and Filters; extending with Splitter, Aggregator, and Scatter-Gather enables horizontal scaling without infrastructure upgrades.

| EIP Pattern | Data Engineering Equivalent | Application |
|-------------|---------------------------|-------------|
| **Splitter** | Partition into independent chunks | `for_each_task` inputs, Spark `repartition` |
| **Aggregator** | Delta table as accumulator | `replaceWhere` idempotent writes per partition |
| **Scatter-Gather** | Fan-out with coordinator/finalizer | `for_each_task` workers + validation step |
| **Content-Based Router** | Provider manifest &rarr; dynamic dispatch | Route ingestion by `data_source` type |
| **Claim Check** | Store payload in Delta, pass reference | `match_id` references instead of full DataFrames |
| **Dead Letter Channel** | Failed item quarantine | `bronze.dead_letters` Delta table |
| **Pipes and Filters** | Medallion architecture | Bronze &rarr; Silver &rarr; Gold (already implemented) |

### Horizontal scaling: `for_each_task`

Databricks `for_each_task` (GA since 2023, enhanced July 2025) is the core fan-out primitive: max 100 concurrent tasks, 48KB task value limit, serverless compute, independent retry per iteration.

**Coordinator-Worker-Finalizer pattern:**

```
Coordinator (discover work units)
    &darr; outputs [{match_id, source, params}, ...]
for_each_task: Worker (process one unit)
    &darr; each writes to Delta partition via replaceWhere
Finalizer (validate completeness, emit OTel metrics)
```

### Existing pain point fixes

| Pain Point | Current Issue | EIP Fix |
|-----------|---------------|---------|
| ~~**StatsBomb N+1** (TODO #3)~~ | ~~~3,500 sequential per-match queries~~ | ~~Resolved: Each per-match `SELECT *` is bounded by `WHERE match_id = {match_id}`. Backfill uses Delta MERGE instead of read-modify-write.~~ |
| ~~**SPADL/VAEP OOM** (TODO #4)~~ | ~~Full bronze tables collected to driver~~ | ~~Resolved: Per-partition Spark pulls replace full-table `.toPandas()`. XGBoost models serialized to bytes via closure (UC Volume FUSE broken on serverless).~~ |
| ~~**Off-Ball xT loop** (TODO #12)~~ | ~~Sequential per-match at 1fps~~ | ~~Resolved (2026-03-10): Migrated to `applyInPandas` grouped by `match_id`. Spark distributes across executors. 1fps sampling rate retained as correct accuracy/compute trade-off.~~ |

### Respo.Vision scale planning

50+ keypoints per player at up to 60fps produces ~1.07B float values per match. A wide-per-player schema (`head_x, head_y, head_z, left_shoulder_x, ...` ~150 columns) keeps rows at ~7M/match &mdash; only 2.4&times; current tracking volume. Strategy: Splitter at match level, liquid clustering on `(match_id, frame_id)`, `for_each_task` for parallel ingestion.

### Caching &amp; storage layers

Five complementary caching layers prevent redundant work across the full stack:

**Layer 1 &mdash; HTTP response caching.** `requests-cache` (Apache 2.0) with persistent SQLite backend as drop-in for `fetch_url()`. StatsBomb open data: `expire_after=None` (static). SkillCorner/Wyscout: 24h/7d TTL. Bronze Delta tables remain the durable cache; HTTP cache avoids redundant network round-trips during development and retry.

**Layer 2 &mdash; Training data versioning.** Delta Lake time travel + MLflow `log_input()` with `delta://table@version` URIs. Zero data duplication. Requires explicit `delta.deletedFileRetentionDuration` table properties (30d gold, 7d bronze) ahead of DBR 18.0 changes where `RETAIN X HOURS` in manual VACUUM is ignored.

**Layer 3 &mdash; Intermediate result caching.** Match-level existence check before expensive computation (skip 100% of pitch control loop on re-runs). `joblib.Memory` for disk-backed memoization of static lookups (xT grid). Future: `np.memmap` for memory-mapped NumPy arrays when Respo.Vision 3D pose feature matrices exceed RAM.

**Layer 4 &mdash; Query result caching.** Databricks remote result cache (24h, survives warehouse restarts &mdash; automatic). Lakebase materialized views for pre-aggregated dashboard data. Streamlit `@st.cache_resource` connection pool (swap per-query connections for `SimpleConnectionPool`).

**Layer 5 &mdash; Cost controls.** Predictive Optimization auto-VACUUMs Unity Catalog managed tables. S3 Intelligent Tiering deferred until Respo.Vision volumes justify monitoring fees.

### Delta Lake optimization

- **Liquid clustering** preferred over Z-ordering for new tables (incremental, automatic layout)
- **Change Data Feed** for incremental downstream consumption (`table_changes()` queries)
- **Deletion vectors** + auto-compaction for write performance

### dbt optimization

| Pattern | Benefit | When |
|---------|---------|------|
| **Incremental models** | Process only new/changed rows | Fact tables with `_ingested_at` partitioning |
| **Slim CI** (`state:modified+`) | Only build/test changed models | PR validation in GitHub Actions |
| **`dbt clone`** | Zero-copy table references | Staging environment testing |
| **Model contracts** | Schema enforcement at build time | Gold layer |
| **`--empty` flag** | Zero-cost CI validation (DDL only) | Schema change validation |

### Open questions

1. **Coordinator implementation**: Python notebook or lightweight Databricks SQL task?
2. **Dead Letter Channel**: Separate Delta table or partition within existing bronze?
3. **`for_each_task` vs `mapInPandas`**: When to fan out at job level vs within a single Spark job?
4. **Liquid clustering migration**: Convert existing Z-ordered tables or only apply to new tables?
5. **Respo.Vision schema**: Wide-per-player (150 float cols) vs normalized keypoints table?
6. **Incremental dbt**: Which fact tables benefit most from incremental strategy?
7. **Connection pool size**: `SimpleConnectionPool(2)` or `(5)` for single-instance Streamlit?

### Dependencies

- No blocking dependencies &mdash; caching layers can be implemented incrementally
- Synergistic with Observability (OTel traces measure optimization impact)
- `for_each_task` patterns require Databricks workflow refactoring (currently single-task jobs)
- Delta retention policy changes should precede any MLflow training data versioning

---

## Deep Learning Infrastructure &amp; Pre-trained Models

**Status:** Research complete, ready for incremental implementation
**Budget:** ~$6-14/month incremental (external GPU training + existing Databricks governance)
**References:** DeepMind AlphaEvolve/FunSearch (Apache 2.0); TacticAI (Nature Communications, 2024); SoccerNet benchmarks

The platform's analytics models are currently traditional ML (logistic regression xG, grid-based xT, SPADL/VAEP). Multiple planned phases &mdash; DEFCON Tier 4 GNN, pgvector embeddings (Phase 15), and Space Creation (ROADMAP) &mdash; assume deep learning capability but no infrastructure exists to train, version, serve, or iteratively improve neural models. This section defines the end-to-end DL stack and catalogs pre-trained models that provide a head start.

### Core principle: train cheap, govern centrally

Databricks GPU training costs 3-5&times; more than external providers. The hybrid pattern uses Databricks for data preparation, experiment tracking (MLflow), and model registry governance, while offloading actual GPU training to budget-friendly providers.

```
Delta Lake (training data)
    &darr; MosaicML StreamingDataset (stream to external GPU)
External GPU (RunPod spot ~$0.35/hr, Lambda Labs ~$0.75/hr)
    &darr; PyTorch/JAX training, MLflow remote logging
Unity Catalog Model Registry (@Champion / @Challenger aliases)
    &darr; Batch inference (Databricks serverless CPU job)
Delta Lake &rarr; Synced tables &rarr; Lakebase &rarr; Streamlit
```

**MLflow 3** (current): `LoggedModel` as first-class citizen, Unity Catalog default registry (`catalog.schema.model_name`), Champion/Challenger aliases decouple inference code from version numbers. Pre-trained weights stored in UC Volumes (`/Volumes/soccer_analytics/dev_gold/model_weights/`). `HF_HOME` pointed at UC Volume to cache HuggingFace downloads across sessions.

### Budget architecture

| Component | Provider | Est. Monthly Cost |
|-----------|----------|-------------------|
| GNN training (2 hr/week on RTX 4090) | RunPod spot | ~$3-5 |
| Embedding batch inference | Databricks Serverless (CPU) | ~$2-5 |
| Model serving (CPU, scale-to-zero) | Databricks Model Serving | ~$0-2 |
| MLflow tracking + model registry | Included in workspace | $0 |
| Pre-trained weight storage (UC Volume, ~10GB) | Delta storage | ~$1-2 |

### DeepMind-inspired optimization patterns

Three approaches from DeepMind's recent work apply directly to soccer analytics at individual-developer scale:

**FunSearch / AlphaEvolve pattern.** LLM-driven algorithm evolution: define an `evaluate(candidate) &rarr; score` function, let an LLM generate and mutate candidates, keep the best. [OpenEvolve](https://huggingface.co/blog/codelion/openevolve) (MIT) is a community implementation that works with any LLM API. Targets: evolve `expected_threat_grid.csv` values against StatsBomb event data; optimize pitch control kernel vectorization strategies. Cost: ~$5-20 for a weekend search run, CPU only.

**JAX `vmap` vectorization.** Single highest-leverage tool for existing code. `jax.vmap` vectorizes `compute_pitch_control_at_point()` from ~2,700 serial Python calls per second to one array operation &mdash; unlocking full Space Creation (Fernandez &amp; Bornn 2018 OBSO) on the existing budget without GPU. JAX compiles to vectorized CPU operations via XLA; no infrastructure change required.

**Continual learning (EWC / Knowledge Distillation).** DeepMind's Elastic Weight Consolidation (Kirkpatrick et al. 2017) prevents catastrophic forgetting when adapting models to new seasons or competitions. The practical variant &mdash; Knowledge Distillation (Learning without Forgetting) &mdash; maps directly to the MLflow Champion/Challenger pattern: the `@Champion` model provides soft labels for `@Challenger` training on new data, preserving historical calibration.

### Data augmentation for limited tracking data

With only 20 tracking matches, synthetic data multiplication is critical:

| Technique | Multiplier | Compute | Basis |
|-----------|-----------|---------|-------|
| **Symmetry augmentation** (H-flip, V-flip, team swap) | 8&times; | Zero (NumPy) | TacticAI (DeepMind, 2024) |
| **Physics-based perturbation** (position/velocity jitter within constraints) | 10&times; per frame | Minimal (NumPy) | Counterfactual simulation |
| **dm_control MuJoCo Soccer** (synthetic match generation) | Unlimited | CPU | Pretrain-then-fine-tune pattern |

### Pre-trained models: immediately usable

Models with available weights compatible with current data sources:

| Model | Domain | Data Compatibility | License | Compute |
|-------|--------|-------------------|---------|---------|
| [**football2vec**](https://github.com/ofirmg/football2vec) | Player/action embeddings | StatsBomb (exact match) | MIT | Hours / CPU |
| [**OpenSTARLab**](https://github.com/open-starlab) (LEM, FMS, Seq2Event) | Event prediction, match simulation | StatsBomb + Wyscout | Apache 2.0 | Minimal |
| [**Foundation Model for Soccer**](https://arxiv.org/abs/2407.14558) | Action prediction transformer | FAWSL (fine-tune on SB) | Research | Days / 1 GPU |
| [**RTMPose**](https://github.com/open-mmlab/mmpose) (MMPose) | Pose estimation from video | Broadcast footage | Apache 2.0 | 1-2 days / 4 GPU |

### Pre-trained models: available with fine-tuning

| Model | Domain | License | Fine-tune Compute |
|-------|--------|---------|-------------------|
| [**T-DEED**](https://github.com/arturxe2/T-DEED) | Video event spotting (SoccerNet 2024 winner) | Research | 1-2 GPU-days |
| [**PRTReID**](https://github.com/SoccerNet/sn-gamestate) (SoccerNet GSR) | Player re-identification | Research | 1 GPU-day |
| [**TranSPORTmer**](https://arxiv.org/abs/2410.17785) | Multi-task trajectory prediction | Academic | 1-2 GPU-days |

### Watch list (pending weight release)

| Model | Domain | Status | Why It Matters |
|-------|--------|--------|----------------|
| [**SoccerMaster**](https://arxiv.org/abs/2512.11016) | Vision foundation (multi-task) | Dec 2024, weights pending | First soccer-specific foundation model; if released, becomes dominant backbone |
| [**SportMamba**](https://arxiv.org/abs/2506.03335) | Video tracking (Mamba SSM) | CVPR 2025 | State-of-the-art multi-object tracking for team sports |

### Relationship to existing phases

| Phase | DL Infrastructure Enables |
|-------|--------------------------|
| **Phase 15** (pgvector embeddings) | **Complete** — retrained football2vec (32-dim Doc2Vec) + 13-dim z-score stat vectors. Model published to HF Hub. |
| **DEFCON Tier 4** (GNN) | GNN pre-trained on StatsBomb 360 freeze frames (15.58M rows), fine-tuned for defensive valuation. Tier 3 tabular model **complete** (Phase 17). |
| **Space Creation** (ROADMAP) | JAX `vmap` pitch control vectorization makes full OBSO feasible on CPU |
| **Graph Tactical Patterns** (ROADMAP) | PyTorch Geometric GNN on tracking data with symmetry augmentation |
| **Visual Exploratory Behavior** (ROADMAP) | RTMPose for pose estimation if Respo.Vision data requires broadcast video processing |

### Key tools

| Tool | Purpose | License |
|------|---------|---------|
| **JAX** + `vmap` | Pitch control vectorization, array computation | Apache 2.0 |
| **PyTorch Geometric** | GNN training (player interaction graphs) | MIT |
| **Flax NNX** | JAX neural networks (2024 rewrite, clean API) | Apache 2.0 |
| **Optax** | EWC-compatible optimizers, LR schedules | Apache 2.0 |
| **OpenEvolve** | AlphaEvolve-style algorithm evolution | MIT |
| **MosaicML StreamingDataset** | Stream Delta to external GPU DataLoaders | Apache 2.0 |
| **MLflow 3** | Experiment tracking, model registry, deployment | Apache 2.0 |

### Open questions

1. **JAX vs PyTorch**: JAX `vmap` for pitch control is compelling, but GNN ecosystem is PyTorch-centric. Maintain both or pick one?
2. **External GPU provider**: RunPod (cheapest) vs Lambda Labs (more reliable, SSD-backed)?
3. ~~**football2vec**~~ Resolved &mdash; retrained on full ~3,000-match StatsBomb corpus (Phase 15 complete).
4. **Feature store scope**: Which player features justify formal Databricks Feature Engineering tables?
5. **Serving strategy**: CPU batch inference (simple, scheduled) vs scale-to-zero endpoint (real-time)?
6. **SoccerMaster timeline**: Monitor GitHub for weight release &mdash; could consolidate multiple point solutions

### Dependencies

- No blocking dependencies for JAX `vmap` or symmetry augmentation (Tier 1)
- GNN pre-training depends on PyTorch Geometric + external GPU access
- football2vec / OpenSTARLab usable immediately with existing StatsBomb/Wyscout data
- Full model serving pipeline depends on MLflow 3 + Unity Catalog (already provisioned)
- Synergistic with Observability (OTel traces measure model performance and drift)
- Synergistic with Pipeline Optimization (`for_each_task` for distributed inference jobs)

---

## Provider Abstraction &amp; Multi-Tier Ingestion

**Status:** Research complete, ready for implementation
**Budget:** $0 incremental (refactoring existing code)
**EIP Pattern:** Content-Based Router + Provider Manifest

The platform ingests from five data sources, each with a dedicated Python module. The shared `utils.py` abstracts infrastructure plumbing (CLI, logging, Spark, Delta writes, HTTP), but provider identity &mdash; URLs, match lists, table names, partition keys, schemas &mdash; is hardcoded in each module. Adding a new source means writing a module from scratch; switching a provider's data tier (e.g., Wyscout Figshare &rarr; Wyscout Commercial API) means editing code.

### Core principle: configure, don't code

A provider registry pattern where each data source has a configuration manifest defining its available tiers, endpoints, auth mechanism, format, and partition strategy. Users select a tier via config &mdash; commercial tiers activate only when credentials are provided; **free/open tiers are the default**.

```
Provider Manifest (YAML / dataclass)
        |
   Content-Based Router (dispatch by source + tier)
        |
   +----+----+----+----+----+
   |    |    |    |    |    |
  SB  Wyscout Meta IDSSE SK  ...
   |    |    |    |    |    |
   Provider Adapters (fetch + normalize)
        |
   Shared Pipeline (validate &rarr; audit &rarr; Delta write)
```

Each adapter implements a common interface: `discover_matches()`, `fetch_events()`, `fetch_tracking()`. The orchestrator calls the adapter, receives normalized DataFrames, and handles Delta writes via existing `utils.py`.

### Provider catalog

| Provider | Default (Free/Open) Tier | Commercial Tier | Config Switch |
|----------|-------------------------|-----------------|---------------|
| **StatsBomb** | GitHub JSON via `statsbombpy` | Same library, authenticated endpoints | Set `SB_USERNAME`/`SB_PASSWORD` env vars |
| **Wyscout** | Figshare ZIP download | REST API (`/v3/matches/{id}/events`) + API key | API key env var + new fetch logic |
| **SkillCorner** | `kloppy` open data loader | `skillcorner-py` client + bearer token | Token env var + client swap |
| **Metrica** | GitHub sample CSV/JSON | GameCloud portal download (same CSV format) | File source change only |
| **IDSSE / Sportec** | Pre-downloaded DFL XML on UC Volume | S3/FTP push from DFL Data Hub (same XML parser) | File source change only |

### Future provider templates

Providers not yet implemented but with known delivery patterns ready for adapter development:

| Provider | Delivery | Auth | Format | Notes |
|----------|----------|------|--------|-------|
| **Opta / Stats Perform** | REST, FTP push, S3 drop, or WebSocket (customer chooses) | API key | JSON (SDAPI) or XML (F-series legacy) | Most complex &mdash; multiple delivery modes |
| **Second Spectrum** | REST JSON via Stats Perform "Insight Feed" | Bearer token | JSON/JSONL per half | Combined event + tracking in one feed |
| **Respo.Vision** | REST API, &lt;12hrs post-match | Bearer token | JSON (3D pose, 40+ keypoints) | Own-footage recording in broadcast mode; wide-per-player schema needed |
| **Catapult** | REST API (`/parameters`, `/activities`) | Bearer token | JSON | Session-based, not match-based &mdash; different data model |
| **STATSports** | SONRA REST API | API key | JSON | Same session-based model as Catapult |
| **Kinexon** | REST + real-time UWB streaming | API key | JSON/CSV | LPS (sub-10cm accuracy), Bundesliga EPTS certified |

### Delivery patterns

The framework needs to support three ingestion triggers (batch first, streaming deferred):

| Pattern | Providers | Implementation |
|---------|-----------|----------------|
| **Scheduled REST poll** (primary) | StatsBomb, Wyscout, SkillCorner, Catapult, Kinexon, Respo.Vision | Databricks workflow on schedule or post-match trigger |
| **S3 event-driven** | Stats Perform, Sportec/DFL, Hawk-Eye | S3 event notification &rarr; EventBridge &rarr; Databricks workflow |
| **WebSocket subscription** (deferred) | StatsBomb Live, Stats Perform, Genius Sports | Persistent connection &mdash; fundamentally different architecture |

### What already works (no refactoring needed)

StatsBomb's open-to-commercial switch is already zero-code: `statsbombpy` checks for `SB_USERNAME`/`SB_PASSWORD` env vars and switches endpoints automatically. This is the gold standard the other providers should match.

### Industry context

Post-match batch via REST API polling is the dominant pattern across the industry. Clubs like K.V. Mechelen describe their pipeline as "script polls Wyscout API on a schedule." Push-based delivery (webhooks, S3 drops, WebSocket) exists primarily for real-time use cases (betting, broadcast, live coaching). No universal cross-vendor data exchange standard exists &mdash; the closest are Opta's F-series XML schema (de facto event standard), FIFA EPTS certification (tracking accuracy), and `kloppy`'s vendor-neutral Python data model.

### Open questions

1. **Config format**: YAML manifest (12-factor, user-friendly) vs Python dataclass registry (type-safe, IDE support)?
2. **Adapter granularity**: One adapter per provider with tier as config, or separate adapters per tier?
3. **kloppy integration**: Delegate to kloppy where it has parsers (SkillCorner, Metrica, Second Spectrum, TRACAB, Opta), or maintain independent parsers for control?
4. **Wearable data model**: GPS/LPS data is session-centric (training), not match-centric. Separate pipeline or unified with match ingestion?
5. **Credential management**: Env vars (current, simple) vs Databricks Secrets (more secure, workspace-bound) vs AWS Secrets Manager (centralized)?

### Dependencies

- No blocking dependencies &mdash; can refactor existing modules incrementally
- Synergistic with Pipeline Optimization (adapters become `for_each_task` workers in Scatter-Gather)
- Synergistic with Observability (OTel spans per adapter, per provider, per tier)
- StatsBomb adapter is effectively already done (zero-code tier switch via env vars)
- `kloppy` (BSD-3) provides vendor-neutral parsing for SkillCorner, Metrica, Second Spectrum, TRACAB, Opta &mdash; reduces adapter implementation effort

---

## Visual Exploratory Behavior (Pose-Enhanced Tracking)

**Status:** Blocked by pose data &mdash; Respo.Vision on own footage planned
**License:** BSD 3-Clause ([USSoccerFederation/ssac26_visual_exploratory_behavior](https://github.com/USSoccerFederation/ssac26_visual_exploratory_behavior))
**Paper:** Bekkers (2026), "Wide Open Gazes: Quantifying Visual Exploratory Behavior in Soccer with Pose Enhanced Positional Data" (SSAC26)

Probabilistic 2D vision model: for each player at each frame, computes a pitch-surface probability grid of what they can see, accounting for head rotation (120-degree FoV), speed-dependent perception decay, and occlusion by other players' torsos.

### Why it matters

The paper proves that aggregated vision features improve prediction of pitch value gained (AUC 0.744 to 0.788 with vision, +0.0 without), while traditional VEA counting (head movements > 125 deg/s) adds zero predictive power. This is the frontier of off-ball analysis.

### Hard blocker: pose data

The model requires **`head_angle`** and **`shoulders_angle`** per player per frame — data from pose estimation applied to broadcast video. None of luxury-lakehouse's current tracking sources provide these angles.

| Data Source | Has pose angles? | Viable? |
|-------------|-----------------|---------|
| Metrica / IDSSE / SkillCorner tracking | No | No |
| StatsBomb 360 freeze frames | No | No |
| **Respo.Vision** (on own footage) | Yes | Yes — planned |

**Acquisition path:** Record own footage in Respo.Vision-compatible "broadcast" mode. As data originator, this footage has no league or broadcast copyrights attached — a completely clean slate. Vendor EULAs for existing league data would block any third-party data sharing, so own-footage is the only viable path.

### What's ready now

The `Vision` class is a clean NumPy/scipy implementation. Once pose data arrives, integration is straightforward:
- Narrow format matches `fct_tracking_frames` (no format adaptation)
- Pitch dimensions are configurable
- Speed is already computed in our tracking pipeline
- Combines naturally with Phase 11 pitch control via element-wise matrix multiplication

### Potential artifacts (once data available)

| Artifact | Layer | Description |
|----------|-------|-------------|
| `src/ingestion/pose_tracking.py` | Ingestion | Ingest pose-enhanced tracking (Respo.Vision format) |
| `src/analytics/vision.py` | Analytics | Adapted `Vision` class for 120x80 coordinate system |
| `int_vision_maps.sql` | dbt intermediate | Per-player per-frame vision metrics |
| `fct_player_stats.sql` (update) | dbt marts | Vision-derived per-90 stats |
| Heat Map page (update) | Streamlit | Vision map overlay on tracking viz |

### Dependencies

- Own-footage recording + Respo.Vision processing (blocker)
- Phase 11 (pitch control) — for vision x pitch control x pitch value framework
- Phase 10 (tracking) — **complete** (velocity computation ready)

---

## Staging Environment (Lakebase Branching)

**Status:** Design phase
**Budget impact:** Moderate — second Lakebase project with scale-to-zero minimizes idle cost

Currently the platform has a single `dev` environment. Adding a `staging` environment leverages Lakebase's unique serverless PostgreSQL capabilities — particularly **copy-on-write database branching** — for pre-production validation without duplicating the full data pipeline.

### Why it matters

- **Lakebase branching**: Create lightweight branches of the production database for testing schema changes, index strategies, and synced table migrations — without affecting dev
- **dbt environment isolation**: Run `dbt build --target staging` against a separate Gold schema, validating transformations before promoting to dev
- **Synced table dry-run**: Test synced table schema changes (the current delete-drop-recreate workflow) in staging before applying to dev
- **Learning objective**: Hands-on experience with Lakebase's PostgreSQL branching, which is a differentiating capability vs. traditional RDS

### Implementation sketch

| Component | Dev (current) | Staging (new) |
|-----------|--------------|---------------|
| Unity Catalog schema | `dev_bronze`, `dev_silver`, `dev_gold` | `staging_bronze`, `staging_silver`, `staging_gold` |
| Lakebase project | `soccer-analytics-dev` | `soccer-analytics-staging` |
| Lakebase branch | `production` | `staging` (branched from dev production) |
| dbt target | `dev` | `staging` |
| Synced tables | 16 tables | Subset (fact tables only for validation) |
| Terraform | `terraform/environments/dev/` | `terraform/environments/staging/` |
| Budget | Under $100/month | Minimal incremental (scale-to-zero) |

### Key decisions to make

1. **Branch source**: Branch staging from dev's production, or maintain independently?
2. **Data scope**: Full data replication or subset (e.g., 1 competition per source)?
3. **CI integration**: Should GitHub Actions run `dbt build --target staging` on PRs?
4. **Synced table subset**: Which tables justify staging replication?

### Dependencies

- No blocking dependencies — can be implemented at any time
- Terraform module refactoring to support multi-environment

---

## Graph-Based Tactical Pattern Recognition

**Status:** Research direction
**Paper:** Raabe, Nabben & Memmert (2022), "Graph representations for the analysis of multi-agent spatiotemporal sports data" (*Applied Intelligence*, CC BY open access)

Proposes **Tactical Graphs** — representing players as graph nodes and spatial interactions as edges — processed by lightweight Tactical Graph Networks (TGNets) for classifying defensive outcomes from tracking data. Key finding: graph representations match or outperform CNN/LSTM approaches at a fraction of computational complexity.

### Relevance

- Directly applicable to luxury-lakehouse's 20 tracking matches (38M frames)
- Player-to-player distance edges naturally model defensive structure
- Graph representation is permutation-invariant (player ordering doesn't matter) and rotation-invariant
- Could power tactical pattern classification: pressing triggers, defensive shape transitions, counter-attack detection
- Lightweight architecture means feasible without GPU infrastructure

### Relationship to existing phases

- **Phase 11** (pitch control): TGNets could classify game states by pitch control regime
- **Phase 12** (movement analysis): Graph features complement physical metrics
- **DEFCON Tier 4**: The DEFCON paper also uses Graph Attention Networks — shared infrastructure. Tier 3 tabular model complete (Phase 17).
- Would require a new `src/analytics/` module for graph construction and model training

### Not immediately actionable

Requires labeled training data (defensive outcomes per tracking sequence). The paper used proprietary German football data with expert labels. Luxury-lakehouse would need to derive labels from events (e.g., possession outcome after defensive sequence) or use manual annotation.

---

## Decision Optimization (Beyond VAEP)

**Status:** Research direction
**Paper:** Rahimian, Van Haaren & Toka, "Beyond action valuation: A deep reinforcement learning framework for optimizing player decisions in soccer"

Extends VAEP (Phase 9) from *valuing what happened* to *optimizing what should happen*. Uses RL to learn team-specific optimal pass selection and success probability surfaces, then compares actual decisions against optimal ones.

### Relevance

- Natural evolution of the Phase 9 VAEP pipeline
- Answers "where *should* the player have passed?" not just "how valuable was the pass?"
- Requires synchronized 25fps tracking + event data (Stats Perform level — commercial)
- Implementation would need CNN policy networks trained on 11-channel game state representations

### Not immediately actionable

Requires commercial-grade tracking data (Belgian Pro League / Stats Perform) — significantly beyond current public datasets. Filed as a long-horizon research direction.

---

## Space Creation Quantification (Fernandez & Bornn 2018)

**Status:** Research direction — deferred from Phase 12
**Paper:** Fernandez & Bornn (2018), "Wide Open Spaces: A statistical technique for measuring space creation in professional soccer"

Full OBSO (Off-Ball Scoring Opportunity) requires computing N+1 pitch control surfaces per frame (one counterfactual surface with each player removed) to measure each player's space creation contribution. At 25fps with 22 players, this is ~2,700 pitch control evaluations per second of play — prohibitively expensive for the current compute budget.

### What was implemented instead

Phase 12 implemented a simpler Off-Ball xT metric: `pitch_control(player_location) x xT(player_zone)`, computed at 1fps sampling. This captures positional value without the counterfactual computation.

### What would be needed

- GPU-accelerated pitch control (vectorized TTI computation across grid)
- ~25x compute budget increase (from 1fps to 25fps full OBSO)
- Differential pitch control: `PC_with_player - PC_without_player` per player per frame

### Dependencies

- Phase 11 (pitch control) — complete
- Phase 12 (off-ball xT) — complete (provides foundation)
- GPU compute infrastructure (not currently available)

---

## <img src="assets/hf-logo.png" height="28" align="top"> HuggingFace Hub Integration (Open Model & Dataset Ecosystem)

**Status:** Research complete, ready for incremental implementation
**Budget:** $0 (free tier) or $9/month (PRO for priority GPU access)
**References:** [Databricks &hearts; HuggingFace](https://www.databricks.com/blog/contributing-spark-loader-for-hugging-face-datasets); [PyG Hub Integration](https://github.com/pyg-team/pytorch_geometric/issues/7170); [SoccerNet on HF](https://huggingface.co/SoccerNet)

HuggingFace is the open-source AI community's central hub &mdash; model weights, datasets, and interactive demos, all freely accessible without gatekeeping. Their commitment to open science aligns with this project's values: luxury-lakehouse is built on open data (StatsBomb, Wyscout Figshare, Metrica, SoccerNet) and open tools (dbt, Streamlit, socceraction, kloppy). Integrating with HuggingFace is a deliberate choice to participate in and contribute back to that ecosystem, not just consume from it.

Phase 14 (entity resolution) is complete using TF-IDF + rapidfuzz. As Phases 15&ndash;16 and DEFCON Tier 4 introduce deep learning (learned embeddings, full GNN), the project needs an artifact ecosystem for model weights, training datasets, and community sharing. HuggingFace Hub provides this at zero cost for public artifacts, with native Databricks integration via MLflow's `transformers` flavor and Unity Catalog model registry.

### Core principle: publish openly, consume freely

HuggingFace's model is consumption-free: anyone can download public models and datasets without an account. Publishing is free for public repos (10 GB/repo Git LFS). A HuggingFace Organization (e.g., `luxury-lakehouse`) provides a namespace for all artifacts, with collaborators using their own free accounts. Compute costs (GPU Spaces, HF Jobs) are per-user &mdash; the org pays nothing when others train on published data.

```
Delta Lake (source of truth)
    ↓ export / stream
HuggingFace Hub (public artifacts)
    ├── Models: safetensors weights + model cards
    ├── Datasets: Parquet + dataset cards + streaming
    └── Spaces: public demo (Streamlit/Gradio)
    ↓ consume
Community (zero cost to download/use)
    ↓ train / fine-tune
Their own compute (HF Jobs, RunPod, Databricks)
```

### Integration tiers

| Tier | Action | Phase Alignment | Cost |
|------|--------|----------------|------|
| **1 &mdash; Consume** | Pull pre-trained models (football2vec, sentence-transformers) for embeddings | Phase 15 | $0 |
| **2 &mdash; Publish** | Push trained model weights (safetensors) and processed datasets (Parquet) to HF Hub | Phase 15, 17 | $0 |
| **3 &mdash; Train** | Use HF Jobs or ZeroGPU Spaces for GNN training; compare pricing vs RunPod | DEFCON Tier 4 | $9/mo PRO + per-job |
| **4 &mdash; Demo** | Host a public Streamlit/Gradio Space with cached data subsets as a portfolio showcase | Post-Phase 16 | $0 (CPU) |

### Tier 1 &mdash; Consume pre-trained models

Models with immediate applicability to planned phases:

| Model | Source | License | Relevance |
|-------|--------|---------|-----------|
| [**sentence-transformers**](https://sbert.net/) (`all-mpnet-base-v2`) | HF Hub | Apache 2.0 | Phase 14 entity resolution &mdash; embed player names + metadata for cosine similarity matching via pgvector. Handles transliteration, accented characters, name variants better than `rapidfuzz`. |
| [**football2vec**](https://github.com/ofirmg/football2vec) | GitHub (MIT) | MIT | Phase 15 embeddings &mdash; pre-trained player/action embeddings on StatsBomb data. Could replace or complement simple per-90 stat vectors. |
| [**OpenSTARLab**](https://arxiv.org/html/2502.02785v2) (LEM, FMS, Seq2Event) | GitHub (Apache 2.0) | Apache 2.0 | Event prediction and match simulation on StatsBomb + Wyscout data. Foundation for Phase 17 and Decision Optimization. |
| [**microsoft/SportsBERT**](https://huggingface.co/microsoft/SportsBERT) | HF Hub | MIT | Sports-domain BERT for NLP-based player search or commentary enrichment. Lower priority. |

**Databricks integration path:** `HF_HOME` &rarr; UC Volume caches downloads across sessions. Models logged via `mlflow.transformers.log_model()`, registered in Unity Catalog with `@Champion`/`@Challenger` aliases.

### Tier 2 &mdash; Publish models and datasets

Artifacts the project could publish to the community:

| Artifact | Format | Est. Size | Publication Trigger |
|----------|--------|-----------|-------------------|
| Player embedding model + vectors | safetensors + Parquet | ~50&ndash;200 MB | Phase 15 completion |
| DEFCON GNN weights | safetensors via `PyGModelHubMixin` | ~50&ndash;200 MB | DEFCON Tier 4 completion |
| SPADL/VAEP action value dataset | Parquet (auto-streaming) | ~500 MB&ndash;2 GB | Available now (optional) |
| Evolved xT grid (if OpenEvolve used) | CSV + model card | <1 MB | Post-DL Infrastructure |
| Line-breaking pass detection results | Parquet | ~50 MB | Available now (optional) |

All artifacts fit within HF's free 10 GB/repo Git LFS limit. Dataset repos get automatic Parquet conversion, DuckDB-queryable dataset viewer, and streaming support.

**Model cards** document methodology, training data provenance (StatsBomb open data, Wyscout Figshare), coordinate systems, and reproduction steps &mdash; the same rigor as the project's existing documentation standards. Source files are maintained in [`docs/huggingface/`](docs/huggingface/) and pushed to HF Hub as the canonical README.

### Tier 3 &mdash; GPU training on HuggingFace

| | RunPod Spot | HF Jobs | HF ZeroGPU (PRO) |
|---|---|---|---|
| **RTX 4090** | ~$0.35/hr | &mdash; | &mdash; |
| **A100 80 GB** | ~$1.59/hr | Available | Queue-based |
| **H200** | &mdash; | Available (PRO) | Priority (PRO) |
| **Ecosystem** | Raw GPU | `hf jobs run`, auto-push to Hub | Spaces integration |
| **Min cost** | Per-hour only | Per-hour only | $9/month base |

**Decision point:** Compare HF Jobs vs RunPod pricing when DEFCON Tier 4 GNN training begins. HF's advantage is ecosystem integration (train &rarr; push &rarr; serve in one flow). RunPod is cheaper for raw GPU-hours. Both are compatible with MLflow remote logging back to Databricks.

### Tier 4 &mdash; Public demo Space

A HuggingFace Space (Streamlit or Gradio) hosting a read-only demo with pre-cached data subsets. Not a replacement for the Databricks Apps deployment (which has live Lakebase connectivity), but a public portfolio piece for:

- Interactive pitch control visualization
- Player embedding similarity explorer
- xG model playground
- Line-breaking pass detection examples

**Constraint:** No Lakebase connectivity from HF Spaces. All data must be pre-exported as static Parquet/CSV files bundled with the Space. This limits the demo to curated subsets rather than full interactive queries.

### Account and organization model

| Role | Account Type | Cost | Access |
|------|-------------|------|--------|
| **Project owner** | HF Org admin | $0 (free) or $9/mo (PRO) | Full control |
| **Collaborators** | Their own free HF account | $0 | Push to org repos |
| **Consumers** | No account needed | $0 | Download models/datasets |
| **GPU users** | Their own account + billing | Their cost | HF Jobs / Spaces |

### Open questions

1. ~~**Org name**~~ Resolved &mdash; `luxury-lakehouse` (created, model published).
2. ~~**football2vec evaluation**~~ Resolved &mdash; retrained on full ~3,000-match StatsBomb corpus (not pre-trained weights). 32-dim Doc2Vec model saved to UC Volume + HF Hub.
3. ~~**sentence-transformers for entity resolution**~~ Resolved &mdash; Phase 14 complete using TF-IDF + rapidfuzz (2,388 matches). Sentence-transformers remains an option for future embedding-based matching if needed.
4. ~~**Publishing priority**~~ Resolved &mdash; Phase 15 model weights published to `luxury-lakehouse/football2vec-statsbomb-wyscout`.
5. **Space framework**: Streamlit (reuse existing code) or Gradio (better for model demos)?
6. **HF Jobs vs RunPod**: Defer comparison until DEFCON Tier 4, or benchmark early with a small training run?

### Dependencies

- Tier 1 (consume) &mdash; **complete** (football2vec retrained on StatsBomb corpus)
- Tier 2 (publish) &mdash; **complete** (model published to [`luxury-lakehouse/football2vec-statsbomb-wyscout`](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout); [model card](docs/huggingface/model-card.md) and [org card](docs/huggingface/org-card.md) pushed to HF Hub)
- Tier 3 (train) depends on DL Infrastructure (ROADMAP) for GNN training pipeline
- Tier 4 (demo) depends on sufficient published artifacts to make a compelling showcase
- Synergistic with DL Infrastructure (HF models flow into MLflow + UC model registry)
- Synergistic with Provider Abstraction (football2vec/OpenSTARLab consume same StatsBomb/Wyscout data)

---

## PAUSA: Optimal Pass Timing &amp; OBSO Value Surface

**Status:** Research complete, license pending (contacting author)
**Paper:** Lee, Jo, Hong, Bauer &amp; Ko (2026), "Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed" (MIT Sloan 2026 finalist, top 7 of 200+)
**Repo:** [`leemingo/mitssac-pausa`](https://github.com/leemingo/mitssac-pausa) (public, **no license yet** &mdash; Apache-2.0 PR planned)
**License status:** Author (Minho Lee) contacted via LinkedIn at SSAC26. Awaiting response to contribute Apache-2.0 license.

The PAUSA metric (Passing Ability Under Spatiotemporal Awareness) decomposes pass quality into two axes: **Temporal Judgment** (was the pass released at the optimal moment?) and **Spatial Selection** (was the target location the best available?). Both are quantified using OBSO (Off-Ball Scoring Opportunity), Spearman's 2018 continuous value surface that evaluates all 22 players' positions to estimate scoring probability at every pitch location.

### Why it matters

Traditional speed-of-play metrics penalize players who hold the ball. PAUSA distinguishes between slow decision-making and elite playmaking &mdash; the deliberate, strategic delay ("la pausa") that draws defenders out of position and manipulates defensive structure. The paper shows PAUSA correlates more strongly with team performance (Bundesliga points) than traditional speed-based metrics.

### Technical components

The repo implements four layers, each with clear Luxury Lakehouse integration potential:

| Component | What it does | Lakehouse overlap |
|-----------|-------------|-------------------|
| **ELASTIC** (`elastic/`) | Synchronizes discrete event data with 25fps tracking using ball acceleration and player-ball distance features. 95.5% exact alignment, 0.023s mean error. Kim, H.S. et al. (2025). "ELASTIC: Event-Tracking Data Synchronization in Soccer Without Annotated Event Locations." ECML-PKDD MLSA 2025. [arXiv:2508.09238](https://arxiv.org/abs/2508.09238). | **None** &mdash; fills a real gap. We have events and tracking as separate streams with no alignment engine. |
| **Pitch Control** (`pitch_control.py`) | Spearman 2018 PPCF, 50x32 grid, **Numba JIT** accelerated. Includes `for_virtual` variant for counterfactual ghost trajectories. | **High** &mdash; same math as `src/analytics/pitch_control.py`, but faster (Numba) and with counterfactual support we lack. |
| **OBSO** (`obso.py`) | `PPCF &times; Transition &times; EPV` scoring surface. Requires pre-computed transition probability matrix (64&times;100 Gaussian) and EPV grid (32&times;50). | **None** &mdash; novel. Our Off-Ball xT (Phase 12) is a simpler `pitch_control &times; xT` without the transition model. OBSO is the full version of what Space Creation (Fernandez &amp; Bornn 2018) requires. |
| **PAUSA** (`calculate_obso.py --unit virtual`) | For each pass: generates ghost trajectories (constant-velocity extrapolation, 3s before to 1s after), computes PPCF+OBSO at each counterfactual frame, decomposes into spatial selection and temporal judgment. | **None** &mdash; novel metric. |

### Data situation

The repo runs on the **same 7 IDSSE Bundesliga matches** we already ingest in Phase 10. Same DFL XML files, same `kloppy` parsing, same 25fps TRACAB tracking. Zero data procurement needed for prototyping.

Static data assets included in the repo (not currently in our stack):
- `EPV_grid.csv` (32&times;50 Expected Possession Value surface)
- `Transition_gauss.csv` (64&times;100 Gaussian ball transition probability matrix)
- `xT_grid.json` (Karun Singh 12&times;8, equivalent to our dbt seed)

### Compute profile

Virtual mode is the heavy-lifter: each pass generates ~100 ghost frames &times; 1,600 grid cells of pitch control. The repo parallelizes via `joblib` with `n_jobs=-1`. For 7 matches this is feasible locally; at scale it needs distributed compute (Databricks serverless or GPU).

### Integration path (if license secured)

| Step | Module | Description |
|------|--------|-------------|
| 1 | `src/analytics/elastic_sync.py` | Adapt ELASTIC sync engine for our tracking+event schema. Align IDSSE events with tracking frames. |
| 2 | `src/analytics/obso.py` | OBSO value surface: combine existing pitch control with transition and EPV grids. |
| 3 | `src/analytics/pausa.py` | PAUSA metric: ghost trajectory generation + temporal/spatial decomposition. |
| 4 | `src/ingestion/pausa.py` | Batch pipeline writing `fct_pausa_values` to Delta. |
| 5 | dbt model | `fct_pass_timing` mart aggregating PAUSA per player per match. |
| 6 | Streamlit page | Pass Timing page: actual vs optimal timing snapshots with OBSO heatmap overlay. |

### Relationship to existing work

- **Phase 11** (pitch control): OBSO extends PPCF with transition and EPV layers. Numba JIT from this repo could accelerate our existing pitch control.
- **Phase 12** (off-ball xT): Our `pitch_control &times; xT` is a simplified OBSO. Full OBSO subsumes it.
- **Space Creation** (roadmap): Full OBSO is a prerequisite for Fernandez &amp; Bornn counterfactual space creation. PAUSA's ghost trajectory infrastructure directly enables it.
- **Decision Optimization** (roadmap): PAUSA answers "when should the player have passed?" &mdash; complementary to the RL-based "where should the player have passed?"

### Open questions

1. **License**: Awaiting Apache-2.0 from Minho Lee. No code adaptation until license is secured.
2. **Numba adoption**: Should we add Numba to our pitch control module? Adds a compiled dependency but significant speedup.
3. **Coordinate system**: Their code uses centered coordinates (&minus;52.5 to +52.5). Our stack uses StatsBomb 120&times;80. Adapter or full migration?
4. **Static grids**: The EPV and Transition grids are pre-computed (provenance unclear). Train our own from StatsBomb data, or use theirs as-is?
5. **Scope**: Full PAUSA pipeline (heavy) or start with ELASTIC sync + OBSO surface only (lighter, more broadly useful)?

### Dependencies

- License from `leemingo/mitssac-pausa` (blocker)
- Phase 10 (IDSSE tracking) &mdash; **complete** (same 7 matches)
- Phase 11 (pitch control) &mdash; **complete** (foundation for OBSO)
- Phase 12 (off-ball xT) &mdash; **complete** (OBSO is the full version)
- Synergistic with DL Infrastructure (Numba JIT, joblib parallelization)
- Synergistic with Space Creation (OBSO + ghost trajectories enable counterfactual analysis)

---

## Other Ideas (Unscheduled)

- [ ] Voronoi area persistence — pre-compute in dbt (lower priority if Phase 11 replaces Voronoi)
- [ ] Pitch Control animation — frame-by-frame playback in Streamlit
- [ ] Event overlay on Pitch Control — render events on pitch control view
- [ ] Wyscout match metadata — formations, coaches, venue (not in public Figshare dataset)


---

*Items graduate from this roadmap into numbered phases in [PLAN.md](PLAN.md) when prerequisites are met and the scope is well-defined.*
