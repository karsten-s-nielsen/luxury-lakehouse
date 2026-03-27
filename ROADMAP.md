# (Right! Luxury!) Lakehouse — Roadmap

Research directions, long-horizon features, and exploratory ideas beyond the current [architecture](ARCHITECTURE.md). Items here are **unscheduled** — they represent valuable directions that may graduate into numbered phases as prerequisites are met and priorities clarify.

**Last updated**: 2026-03-26 (HF Buckets D27, SoccerMaster/RTMO research, adversarial training D28-D33)

---

## Observability Layer (OpenTelemetry)

**Status:** Research complete, ready for implementation
**Budget:** ~$1-2/month (personal) or enterprise-swappable via config

The platform currently has minimal observability (ARCHITECTURE.md &sect;6.4): Databricks audit logs, dbt test results, and Taipy/HF Spaces built-in metrics. No structured telemetry, no model validation, no pipeline performance tracking. This section defines a proper observability layer using OpenTelemetry as the instrumentation standard.

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
| **Taipy app** | Lakebase query latency, page render time | Traces | Auto (`psycopg2`) + manual spans |

**Python OTel SDK** (v1.39.1, stable): Auto-instrumentation available for `requests` and `psycopg2` via `opentelemetry-instrumentation-*` packages. No Taipy or PySpark auto-instrumentation &mdash; use manual spans around key operations.

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

**Reference baselines** stored as dbt seeds or a small Delta table `dev_gold.model_baselines`.

### Open-source monitoring tools (all Apache 2.0)

| Tool | Key Capability | Integration Path |
|------|---------------|-----------------|
| **Evidently AI** | 100+ pre-built drift metrics, HTML reports | Prometheus bridge &rarr; OTel Collector |
| **NannyML** | CBPE: estimate performance *without ground truth* | DataFrame output &rarr; OTel metric emission |
| **WhyLogs** | Lightweight statistical profiles, Spark-compatible | Profile diffs &rarr; OTel attributes |

NannyML's CBPE (Confidence-Based Performance Estimation) estimates performance degradation from output distribution alone &mdash; valuable when ground truth is delayed. **Note:** Evaluated and deferred as D22 in TODO.md (all current models have immediate ground truth; CBPE does not apply today). Revisit if a real-time inference use case with delayed ground truth is added.

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

1. **Collector location**: Sidecar in Taipy App container? Separate ECS task? Lambda?
2. **S3 bucket**: Dedicated telemetry bucket or partition within existing infrastructure?
3. **Query interface**: DuckDB (free, local) vs Athena (serverless, pay-per-query)?
4. **Ingestion granularity**: Per-match spans or per-batch spans?
5. **Real-time UI**: Is the $35/month Grafana LGTM tier worth it, or is S3 + DuckDB sufficient?

### Dependencies

- No blocking dependencies &mdash; can be implemented at any time
- Synergistic with Staging Environment (observability validates staging deployments)
- Foundation for DEFCON (Phase 17) model monitoring

---

## Deep Learning Infrastructure &amp; Pre-trained Models

**Status:** GNN training infrastructure (DEFCON Tier 4), continual learning, FunSearch/AlphaEvolve exploration
**Budget:** ~$6-14/month incremental (external GPU training + existing Databricks governance)
**References:** DeepMind AlphaEvolve/FunSearch (Apache 2.0); TacticAI (Nature Communications, 2024); SoccerNet benchmarks

Foundation in place (MLflow UC Model Registry with Champion/Challenger aliases, scipy-based drift detection, HF Jobs A10G training). This section defines the remaining DL stack needed for GNN training, continual learning, and pre-trained model integration.

### Core principle: train cheap, govern centrally

Databricks GPU training costs 3-5&times; more than external providers. The hybrid pattern uses Databricks for data preparation, experiment tracking (MLflow), and model registry governance, while offloading actual GPU training to budget-friendly providers.

```
Delta Lake (training data)
    &darr; MosaicML StreamingDataset (stream to external GPU)
External GPU (RunPod spot ~$0.35/hr, Lambda Labs ~$0.75/hr)
    &darr; PyTorch/JAX training, MLflow remote logging
Unity Catalog Model Registry (@Champion / @Challenger aliases)
    &darr; Batch inference (Databricks serverless CPU job)
Delta Lake &rarr; Synced tables &rarr; Lakebase &rarr; Taipy
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

**FunSearch / AlphaEvolve pattern.** LLM-driven algorithm evolution: define an `evaluate(candidate) &rarr; score` function, let an LLM generate and mutate candidates, keep the best. [OpenEvolve](https://huggingface.co/blog/codelion/openevolve) (MIT) is a community implementation that works with any LLM API. Targets: evolve xT grid values against StatsBomb event data; optimize pitch control kernel vectorization strategies. Cost: ~$5-20 for a weekend search run, CPU only.

**JAX `vmap` vectorization.** Already deployed: `compute_pitch_control_grid_fast()` with `@jax.jit` backend in `src/analytics/pitch_control.py` (dual NumPy/JAX auto-dispatch). Unlocked full Space Creation (Fernandez &amp; Bornn 2018 OBSO) on CPU without GPU infrastructure. Same pattern applies to future array-intensive analytics.

**Continual learning (EWC / Knowledge Distillation).** DeepMind's Elastic Weight Consolidation (Kirkpatrick et al. 2017) prevents catastrophic forgetting when adapting models to new seasons or competitions. The practical variant &mdash; Knowledge Distillation (Learning without Forgetting) &mdash; maps directly to the MLflow Champion/Challenger pattern: the `@Champion` model provides soft labels for `@Challenger` training on new data, preserving historical calibration.

### Data augmentation for limited tracking data

With only 20 tracking matches, synthetic data multiplication is critical:

| Technique | Multiplier | Compute | Basis |
|-----------|-----------|---------|-------|
| **Symmetry augmentation** (H-flip, V-flip, team swap) | 8&times; | Zero (NumPy) | TacticAI (DeepMind, 2024) &mdash; deployed in `src/analytics/symmetry.py` |
| **Physics-based perturbation** (position/velocity jitter within constraints) | 10&times; per frame | Minimal (NumPy) | Counterfactual simulation |
| **dm_control MuJoCo Soccer** (synthetic match generation) | Unlimited | CPU | Pretrain-then-fine-tune pattern |

### Pre-trained models: immediately usable

Models with available weights compatible with current data sources:

| Model | Domain | Data Compatibility | License | Compute |
|-------|--------|-------------------|---------|---------|
| [**football2vec**](https://github.com/ofirmg/football2vec) | Player/action embeddings | StatsBomb (exact match) | MIT | Hours / CPU |
| [**Foundation Model for Soccer**](https://arxiv.org/abs/2407.14558) | Action prediction transformer | FAWSL (fine-tune on SB) | Research | Days / 1 GPU |
| [**RTMO / RTMPose**](https://github.com/open-mmlab/mmpose) (MMPose) | Pose estimation from video | Broadcast footage (6 Veo3 recordings available) | Apache 2.0 | Real-time inference (see notes below) |

### Pre-trained models: available with fine-tuning

| Model | Domain | License | Fine-tune Compute |
|-------|--------|---------|-------------------|
| [**T-DEED**](https://github.com/arturxe2/T-DEED) | Video event spotting (SoccerNet 2024 winner) | Research | 1-2 GPU-days |
| [**PRTReID**](https://github.com/SoccerNet/sn-gamestate) (SoccerNet GSR) | Player re-identification | Research | 1 GPU-day |
| [**TranSPORTmer**](https://arxiv.org/abs/2410.17785) | Multi-task trajectory prediction | Academic | 1-2 GPU-days |

### Research tier (weights available, not yet integrated)

| Model | Domain | Status | Why It Matters |
|-------|--------|--------|----------------|
| [**SoccerMaster**](https://arxiv.org/abs/2512.11016) | Vision foundation (multi-task) | CVPR 2026, weights released 2026-03-05 | First soccer-specific foundation model — unified backbone for detection, calibration, event classification. See details below |

### Watch list (pending weight release)

| Model | Domain | Status | Why It Matters |
|-------|--------|--------|----------------|
| [**SportMamba**](https://arxiv.org/abs/2506.03335) | Video tracking (Mamba SSM) | CVPR 2025 | State-of-the-art multi-object tracking for team sports |

### SoccerMaster — Investigation Notes (2026-03-26)

**Paper**: arXiv 2512.11016 (Yang, Rao, Wu, Xie). CVPR 2026.
**Weights**: [huggingface.co/xleprime/SoccerMaster](https://huggingface.co/xleprime/SoccerMaster) — Apache 2.0. ~1.61 GB total (backbone 1.44 GB + task heads). PyTorch state dicts. Also requires base [SigLIP2-L/16-512](https://huggingface.co/google/siglip2-large-patch16-512) (~1.9 GB).
**Code**: [github.com/haolinyang-hlyang/SoccerMaster](https://github.com/haolinyang-hlyang/SoccerMaster) — 2 commits, self-described "early version". No license file on repo (weights are Apache 2.0 on HF).

**Architecture**: SigLIP2 ViT-L backbone (24 layers, 1024 hidden dim, 512px input) with temporal attention (layers 16-23). Processes 30-frame video clips as `[B, T, 3, 512, 512]`. Task heads:
- **Detection** (Deformable DETR): player/GK/referee/ball classification + bounding boxes + jersey number. Custom CUDA ops required (multi-scale deformable attention).
- **Pitch keypoints** (58 keypoints, 256x256 heatmap output)
- **Pitch lines** (24 line segments with semantic labels)
- **Camera calibration** (pan/tilt/roll + position regression)
- **Action classification** (23 event categories via 2-layer transformer)
- **Video-to-commentary** (SigLIP contrastive loss)

**Results** (from paper): Detection +4.3 AP@50 over baseline, camera calibration +8.2 on SN22, tracking HOTA 59.1 / MOTA 81.6, commentary BLEU@1 31.3 / CIDEr 38.6.

**GPU requirements**: ~16-24 GB VRAM for inference (batch=1, 30 frames). A100 40GB+ for training. Gradient checkpointing supported.

**Not yet available**: SoccerFactory pretraining dataset (7.45M frames), requirements.txt, end-to-end inference pipeline, quick-start guide.

**Relevance to this project**:
- Detection head (GK/player/referee/ball) could solve D26 (GK exclusion) from video rather than provider metadata — but D26's metadata approach is cleaner and more maintainable for our current data.
- Camera calibration + detection become high-value when own-footage pipeline (Respo.Vision + broadcast video) is active.
- Action classification could supplement/validate StatsBomb event data.
- **Revisit when**: (1) SoccerFactory dataset is released, (2) inference pipeline matures beyond dummy tensors, (3) own-footage pipeline is active and needs detection + calibration.

### Remaining DL use cases

| Use Case | DL Infrastructure Needed |
|----------|--------------------------|
| **DEFCON Tier 4** (GNN) | GNN pre-trained on StatsBomb 360 freeze frames (15.58M rows), fine-tuned for defensive valuation |
| **Graph Tactical Patterns** (ROADMAP) | PyTorch Geometric GNN on tracking data with symmetry augmentation |
| **Visual Exploratory Behavior** (ROADMAP) | RTMO-l for pose estimation on own Veo3 broadcast footage (6 recordings available). Local inference on RTX 4070 Ti. See MMPose notes below |

### Open research (undefined concepts, not yet actionable)

| Use Case | Blockers | Why Not Actionable |
|----------|----------|--------------------|
| **Counterfactual Pitch Control Substitution** | Trajectory normalization (how to map Player A's movement patterns into Player B's positional role — no published method), evaluation metric (total control area? xT-weighted? no consensus), match volume (20 tracking matches too few, need 50+), player ID bridge (tracking IDs not linked to `dim_players.canonical_player_id`) | Two open research questions (trajectory normalization, evaluation metric) plus data volume blocker. The pitch control model (`pitch_control.py`, Spearman 2017) already supports arbitrary player inputs — the math is ready, the methodology isn't. Becomes feasible when own-footage pipeline delivers 50+ player-identified tracking matches. Source: [adversarial-training.md](../adversarial-training.md) |

### MMPose / RTMO — Investigation Notes (2026-03-26)

**Repo**: [github.com/open-mmlab/mmpose](https://github.com/open-mmlab/mmpose) — 7.5K stars, 114 contributors, Apache 2.0. Last release v1.3.2 (2024-07). Active commits through 2025-08.

**RTMO is the preferred model for soccer, not RTMPose.** RTMO is a one-stage multi-person pose estimator (no separate detector). Explicitly faster than RTMPose when >4 people are in frame — always true in soccer. CrowdPose AP **83.8** (RTMO-l body7) on dense multi-person benchmark.

**Inference performance** (no fine-tuning needed — COCO 17-keypoint schema covers shoulders + head):
| Model | Latency (V100 ONNX) | CrowdPose AP |
|-------|---------------------|--------------|
| RTMO-s | 8.9 ms/frame | — |
| RTMO-m | 12.4 ms/frame | — |
| RTMO-l (body7) | 19.1 ms/frame | 83.8 |

**RTX 4070 Ti (12 GB VRAM, local)**: RTMO was benchmarked on GTX 1660 Ti (6 GB). The 4070 Ti is significantly faster — expect 50+ FPS with RTMO-l via TensorRT FP16. Real-time inference on local hardware is confirmed feasible.

**rtmlib** (`pip install rtmlib`): Lightweight ONNX-only inference wrapper. No mmcv/mmengine/mmdet dependency chain. Three lines of code, auto-downloads weights. Supports CPU and GPU.
```python
from rtmlib import Body, draw_skeleton
body = Body(mode='performance', backend='onnxruntime', device='cuda')
keypoints, scores = body(cv_image)  # per-person 17-keypoint arrays
```

**What RTMO provides**: Per-detected-person 17-keypoint coordinates (nose, eyes, ears, shoulders, elbows, wrists, hips, knees, ankles). From shoulders + nose/ears, `head_angle` and `shoulders_angle` can be derived geometrically — exactly what Bekkers (2026) Visual Exploratory Behavior model requires.

**What RTMO does NOT provide**: Player identity across frames (need ByteTrack/StrongSORT), pitch coordinate mapping (need camera calibration / homography). These are separate pipeline stages.

**Own-footage pipeline (feasible locally)**:
1. Veo3 broadcast recording → frame extraction
2. RTMO-l (body7) → 17 keypoints per detected person per frame (local GPU, real-time)
3. ByteTrack or similar → track identities across frames
4. Camera calibration → homography (SoccerMaster or manual 4-point)
5. Map keypoints to pitch coordinates → `head_angle`, `shoulders_angle`
6. Feed into Bekkers Vision model (already implemented in `src/analytics/`)

Steps 1-2 are immediately feasible on local hardware. Steps 3-5 are the integration work. Step 6 is ready.

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

1. **JAX vs PyTorch**: JAX for array computation (pitch control, OBSO), PyTorch Geometric for GNN training. Maintaining both.
2. **External GPU provider**: RunPod (cheapest) vs Lambda Labs (more reliable, SSD-backed)?
3. **Feature store scope**: Which player features justify formal Databricks Feature Engineering tables?
4. **Serving strategy**: CPU batch inference (simple, scheduled) vs scale-to-zero endpoint (real-time)?
5. **SoccerMaster integration**: Weights released 2026-03-05 but codebase is "early version" (no requirements, no end-to-end inference). Camera calibration head could complement RTMO pipeline for own-footage homography. Revisit when inference pipeline matures
6. **RTMO vs Respo.Vision**: RTMO-l local pipeline is free and immediate for 2D pose (head/shoulder angles). Respo.Vision provides 3D (50+ keypoints). Start with RTMO to validate the Visual Exploratory Behavior pipeline end-to-end, upgrade to Respo.Vision for ground truth if results are promising

### Dependencies

- GNN pre-training depends on PyTorch Geometric + external GPU access
- Full model serving pipeline depends on MLflow 3 + Unity Catalog (already provisioned)
- Synergistic with Observability (OTel traces measure model performance and drift)

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

**Status:** Partially unblocked &mdash; 6 Veo3 broadcast recordings available, RTMO pose estimation feasible on local GPU
**License:** BSD 3-Clause ([USSoccerFederation/ssac26_visual_exploratory_behavior](https://github.com/USSoccerFederation/ssac26_visual_exploratory_behavior))
**Paper:** Bekkers (2026), "Wide Open Gazes: Quantifying Visual Exploratory Behavior in Soccer with Pose Enhanced Positional Data" (SSAC26)

Probabilistic 2D vision model: for each player at each frame, computes a pitch-surface probability grid of what they can see, accounting for head rotation (120-degree FoV), speed-dependent perception decay, and occlusion by other players' torsos.

### Why it matters

The paper proves that aggregated vision features improve prediction of pitch value gained (AUC 0.744 to 0.788 with vision, +0.0 without), while traditional VEA counting (head movements > 125 deg/s) adds zero predictive power. This is the frontier of off-ball analysis.

### Pose data path (updated 2026-03-26)

The model requires **`head_angle`** and **`shoulders_angle`** per player per frame — data from pose estimation applied to broadcast video. None of luxury-lakehouse's existing tracking sources provide these angles, but **own footage + local pose estimation is now feasible**.

| Data Source | Has pose angles? | Viable? |
|-------------|-----------------|---------|
| Metrica / IDSSE / SkillCorner tracking | No | No |
| StatsBomb 360 freeze frames | No | No |
| **Respo.Vision** (on own footage) | Yes | Yes — planned, high cost per match |
| **RTMO-l + own Veo3 footage** | Derived | **Yes — local GPU, real-time, zero cost** |

**Two viable paths** (not mutually exclusive):

1. **Respo.Vision** (commercial): Upload Veo3 footage → get full 3D pose tracking (50+ keypoints × 22 players × 60fps). High fidelity, high cost. Best for ground truth validation.

2. **RTMO-l local pipeline** (open-source, new): Veo3 broadcast footage → RTMO-l body7 (rtmlib, ONNX, local RTX 4070 Ti, 50+ FPS) → 17 COCO keypoints per detected person → derive `head_angle` from nose/ears, `shoulders_angle` from shoulder keypoints → ByteTrack for identity tracking → camera homography for pitch mapping. Lower fidelity than Respo.Vision 3D, but free, fast, and immediately available for 6 recorded matches.

**Hardware** (two local options):
- **Primary**: Windows 11, 96 GB RAM, RTX 4070 Ti (12 GB VRAM). RTMO-l benchmarked at real-time on GTX 1660 Ti (6 GB) — the 4070 Ti is comfortably overprovisioned. Best for real-time inference throughput.
- **DGX Spark**: NVIDIA DGX Spark, 128 GB unified RAM (Grace Blackwell). Slower inference than discrete 4070 Ti, but unified memory eliminates GPU VRAM limits — relevant for larger models (SoccerMaster backbone 1.44 GB + SigLIP2 1.9 GB fit entirely without CPU-GPU transfer overhead) or batch processing entire matches without memory pressure.
- No cloud GPU needed for either path.

**Own-footage licensing**: As data originator, this footage has no league or broadcast copyrights attached — a completely clean slate. Vendor EULAs for existing league data would block any third-party data sharing, so own-footage is the only viable path.

### What's ready now

The `Vision` class is a clean NumPy/scipy implementation. Once pose data arrives, integration is straightforward:
- Narrow format matches `fct_tracking_frames` (no format adaptation)
- Pitch dimensions are configurable
- Speed is already computed in our tracking pipeline
- Combines naturally with Phase 11 pitch control via element-wise matrix multiplication

### Potential artifacts (once data available)

| Artifact | Layer | Description |
|----------|-------|-------------|
| `src/ingestion/pose_tracking.py` | Ingestion | Ingest pose-enhanced tracking (Respo.Vision format OR RTMO keypoint output) |
| `scripts/run_pose_estimation.py` | Local pipeline | RTMO-l inference on Veo3 footage → keypoints + ByteTrack → Delta table |
| `src/analytics/vision.py` | Analytics | Adapted `Vision` class for 120x80 coordinate system |
| `int_vision_maps.sql` | dbt intermediate | Per-player per-frame vision metrics |
| `fct_player_stats.sql` (update) | dbt marts | Vision-derived per-90 stats |
| Heat Map page (update) | Taipy | Vision map overlay on tracking viz |

### Dependencies

- Own-footage recording + Respo.Vision processing (blocker)
- Phase 11 (pitch control) — for vision x pitch control x pitch value framework
- Phase 10 (tracking) — velocity computation ready

---

## Team Shape Analysis — Stage 2 (Own-Footage Pipeline)

**Status:** Blocked by SkillCorner DoD commercial access
**Prerequisite:** Stage 1 complete (D19 spatial metrics, D20 EFPI formation detection, D21 Taipy page — all shipped)
**Note:** Formation detection is partially blocked by D26 (GK Metadata Pipeline) for full-coverage results. See TODO.md.

### Stage 2 &mdash; Own-Footage Pipeline (Veo3 &rarr; SkillCorner DoD &rarr; Platform)

Enables team shape analysis on games recorded with Veo3 in broadcast format. The user is the data originator &mdash; no league or broadcast copyrights attached.

#### Pipeline flow

1. **Record** on Veo3 in broadcast format (already happening)
2. **Upload** to SkillCorner Data on Demand via API (bearer token auth)
3. **Poll** for processing completion
4. **Download** tracking CSV (same coordinate system as SkillCorner open data)
5. **Ingest** via adapted SkillCorner pipeline &rarr; `fct_tracking_frames`
6. **Analyze** on Team Shape page &mdash; formation detection, spatial metrics, time-series

#### Integration with Provider Framework

SkillCorner DoD is a known tier in the [Provider Abstraction &amp; Multi-Tier Ingestion](#provider-abstraction--multi-tier-ingestion) roadmap item. The adapter would implement:

```
SkillCorner Open Data (current):  kloppy.skillcorner.load_open_data() → JSON
SkillCorner DoD (new):            upload video → poll → CSV download → same parser
```

The CSV format matches the open data schema &mdash; the parser in `src/ingestion/skillcorner.py` and `stg_skillcorner__tracking.sql` would need minimal adaptation. Frame rate may differ (up to 25fps commercial vs 10fps open).

#### What this unlocks

Team shape analysis on actual youth/amateur games &mdash; metrics that resonate with parents and coaches without data science background:
- "Your team played a 4-3-3 in the first half, shifted to 4-4-2 after the sub at 55'"
- "Defensive line sat at 42% &mdash; you pressed high"
- "Team length expanded from 28m to 38m in the last 15 minutes &mdash; shape got stretched when tired"

#### Blockers

- SkillCorner DoD commercial relationship (pricing, API access)
- `skillcorner-py` client integration (replaces `kloppy` for DoD tier)
- Upload/poll workflow &mdash; may need lightweight automation (Databricks workflow or script)

### Academic citations

| Paper | Contribution |
|-------|-------------|
| Bekkers &amp; Dabadghao (2025), arXiv:2506.23843 | EFPI &mdash; elastic formation detection via template matching + Hungarian algorithm |
| Bialkowski et al. (2014), IEEE ICDM Workshops | Role assignment via Hungarian algorithm &mdash; seminal formation detection paper |
| Shaw &amp; Glickman (2019), Barca Sports Analytics Summit | In/out-of-possession formation splits &mdash; dynamic formation analysis |
| Frencken et al. (2011), J. Sports Sciences | Team centroid + surface area as collective tactical variables |
| Bourbousson et al. (2010), J. Sports Sciences | Stretch index definition &mdash; mean distance from team centroid |
| Fradua et al. (2013), Int. J. Performance Analysis in Sport | Reference intervals for team length/width by tactical style |
| Narizuka &amp; Yamazaki (2019), Scientific Reports 9:13172 | Delaunay triangulation formation fingerprinting |
| Kim et al. (2022), ACM KDD | SoccerCPD &mdash; formation change-point detection |

### Dependencies

- SkillCorner DoD commercial access + [Provider Framework](#provider-abstraction--multi-tier-ingestion) adapter
- Synergistic with Visual Exploratory Behavior (same own-footage pipeline)
- Synergistic with Space Creation Quantification (convex hull and spatial control are shared concepts)

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
| Synced tables | 26 tables | Subset (fact tables only for validation) |
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
- **DEFCON Tier 4**: The DEFCON paper also uses Graph Attention Networks — shared infrastructure
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

## HuggingFace Hub — Streaming Dataset Publishing (Tier 5)

**Status:** Research complete, blocked on upstream (Polars branch not yet merged)
**Discovered:** 2026-03-12 via [Daniel van Strien](https://www.linkedin.com/in/danielvanstrien/) (HuggingFace)
**Branch:** [`davanstrien/polars:feature/hf-bucket-sink`](https://github.com/davanstrien/polars/tree/feature/hf-bucket-sink)
**Proof of concept:** 74 GB of Dutch PDFs filtered to 650 MB in 18 minutes on 2 vCPUs, constant memory

Three HF Hub primitives converge to enable a fundamentally better dataset publishing workflow:

#### 1. XET protocol (live, March 2025)

Content-addressed storage replacing Git LFS on HF Hub. Uses Content-Defined Chunking (~64 KB chunks) with chunk-level deduplication. Key implication: **re-publishing a dataset after adding new competitions only uploads changed chunks**, not the entire file. Our published datasets (SPADL 500 MB+, pitch control tracking, embeddings) would benefit immediately.

- [XET blog post](https://huggingface.co/blog/xet-on-the-hub)
- Chunk-level dedup: editing one row in a 5 GB Parquet file uploads ~64 KB, not 5 GB
- Already active on all new HF Hub repos

#### 2. Storage Buckets (live)

Non-versioned, mutable S3-like storage on HF Hub. No git overhead, no version history accumulation. Managed via `hf buckets sync` CLI or `huggingface_hub` Python API.

- [Docs](https://huggingface.co/docs/hub/storage-buckets)
- Example output: [`davanstrien/finepdfs-edu-gold`](https://huggingface.co/buckets/davanstrien/finepdfs-edu-gold) (702 MB filtered Dutch educational PDFs)
- CDN pre-warming available per-region
- `hf://buckets/` path protocol for programmatic access

**Applicability to us:** Demo Space data files (`sample_tracking.parquet` 7 MB, `defcon_pressure.parquet` 1.6 MB, `career_embeddings.parquet` 330 KB) are static exports that don&rsquo;t need git versioning. Storage Buckets would be a cleaner fit than the current pattern of committing Parquet files into the Space repo.

#### 3. Polars `sink_parquet` to HF Buckets (branch, not merged)

Rust-level implementation in Polars that adds streaming writes from lazy frames directly to HF Storage Buckets via XET. The architecture uses O(row_group_size) memory instead of O(total_dataset) &mdash; row groups are encoded and streamed as produced, never buffered in full.

Key implementation files in the branch:

| File | Purpose |
|------|---------|
| `hf_bucket_sink.rs` | ComputeNode routing `sink_parquet("hf://buckets/...")` |
| `streaming_upload.rs` | Incremental Parquet encoding + XET streaming via bounded channel |
| `xet_upload.rs` | Session management using `xet-session` from `huggingface/xet-core` |
| `lower_ir.rs` | URL routing for `hf://buckets/` path detection |

Token refresh for long-running uploads (XET tokens expire ~1 hour) is handled. Uses `spawn_blocking` for XetSession creation to avoid async runtime conflicts.

```python
# Current publishing workflow (5 steps, local staging required):
# 1. Spark SQL → UC Volume (Parquet directory)
# 2. databricks fs cp → local download
# 3. pd.read_parquet() → consolidate part files
# 4. df.to_parquet() → single file
# 5. HfApi().upload_folder() → HF Hub

# Future workflow (1 step, zero local staging):
pl.scan_parquet("hf://datasets/luxury-lakehouse/spadl-vaep-action-values/**/*.parquet")
  .vstack(new_season_data)
  .sink_parquet("hf://buckets/luxury-lakehouse/staging/spadl-vaep.parquet")
```

#### When to act

| Signal | Action |
|--------|--------|
| Polars merges `hf://buckets/` sink support | Evaluate for dataset refresh automation |
| Published datasets exceed 1 GB per repo | Migrate to XET-aware incremental updates |
| Respo.Vision 3D pose data arrives | Streaming writes essential (large video-derived datasets) |
| CI/CD dataset publishing needed | Storage Buckets as staging + `hf buckets sync` in GitHub Actions |

#### Immediate low-effort wins (no Polars dependency)

Even before the Polars branch merges, two things are actionable today:

1. **XET is already active** &mdash; our existing `HfApi().upload_folder()` calls already benefit from chunk-level dedup when updating published datasets. No code change needed.
2. **Storage Buckets for demo data** &mdash; could migrate `demo_space/data/` from git-tracked Parquet to a bucket, reducing Space repo size and avoiding git history bloat when demo data is refreshed. Requires updating `app.py` to read from `hf://buckets/luxury-lakehouse/demo-data/` instead of local paths.

---

## Other Ideas (Unscheduled)

- [ ] Voronoi area persistence &mdash; pre-compute in dbt (lower priority if Phase 11 replaces Voronoi)
- [ ] Pitch Control animation &mdash; frame-by-frame playback in Taipy
- [ ] Event overlay on Pitch Control &mdash; render events on pitch control view
- [ ] Wyscout match metadata &mdash; formations, coaches, venue (not in public Figshare dataset)
- [ ] **Local GPU Compute Sidecar** &mdash; optional local GPU acceleration for ML workloads (training, inference, CV) using Docker + NVIDIA Container Toolkit. Cloud by default, local if GPU available. Pattern: `delta-rs` reads input from Unity Catalog, GPU container runs computation, writes results back to Delta. Triton Inference Server or MLflow model serving for persistent endpoints. Relevant when neural xG training, DEFCON Tier 4 GNN, or computer vision workloads begin. Not needed for current CPU-based pipelines


---

*Items graduate from this roadmap into numbered phases in [ARCHITECTURE.md](ARCHITECTURE.md) when prerequisites are met and the scope is well-defined.*
