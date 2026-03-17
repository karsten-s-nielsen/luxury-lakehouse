# (Right! Luxury!) Lakehouse — Roadmap

Research directions, long-horizon features, and exploratory ideas beyond the current [architecture](ARCHITECTURE.md). Items here are **unscheduled** — they represent valuable directions that may graduate into numbered phases as prerequisites are met and priorities clarify.

**Last updated**: 2026-03-15

---

## Observability Layer (OpenTelemetry)

**Status:** Research complete, ready for implementation
**Budget:** ~$1-2/month (personal) or enterprise-swappable via config

The platform currently has minimal observability (ARCHITECTURE.md &sect;6.4): Databricks audit logs, dbt test results, and Streamlit built-in metrics. No structured telemetry, no model validation, no pipeline performance tracking. This section defines a proper observability layer using OpenTelemetry as the instrumentation standard.

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

**Reference baselines** stored as dbt seeds or a small Delta table `dev_gold.model_baselines`.

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

## UI/HCI Audit Skill (Mental Model &amp; Error Tolerance)

**Status:** Beta complete &mdash; `cognitive-interface-audit` skill authored in [mad-scientist-skills](https://github.com/karsten-s-nielsen/mad-scientist-skills) v1.7.0 (SKILL.md + 5 templates)
**Budget:** Zero &mdash; Claude Code skill (no infrastructure)
**References:** Wood &amp; Byrne 2002 (error-tolerant interfaces); Gergle et al. 2004/2013 (visual grounding &amp; common ground); Brinck, Gergle &amp; Wood 2001 (*Usability for the Web*, Morgan Kaufmann); Rasmussen 1983 (Skills-Rules-Knowledge); Card, Moran &amp; Newell 1983 (GOMS)

A new [mad-scientist-skills](https://github.com/karsten-s-nielsen/mad-scientist-skills) skill that audits user interfaces and human workflows for **mental model alignment** &mdash; ensuring that the way an interface structures tasks matches how users actually think about those tasks. Visual polish (colors, spacing, typography) matters, but the real value &mdash; like rock-solid infrastructure &mdash; is in the underlying task models that users never consciously see but always feel when they&rsquo;re wrong.

### Core principle: the interface should think the way the user thinks

The best interfaces are invisible. Users complete tasks without friction because the system&rsquo;s workflow mirrors their existing mental model &mdash; same vocabulary, same sequence, same chunking of operations. When the interface&rsquo;s task model diverges from the user&rsquo;s mental model, errors aren&rsquo;t &ldquo;user mistakes&rdquo; &mdash; they&rsquo;re design failures.

### Academic foundations

Three research threads converge into a single audit methodology:

#### 1. Task Model &amp; Error Tolerance (Wood, Byrne, Rasmussen)

Scott D. Wood&rsquo;s dissertation (*Extending GOMS to Human Error and Applying it to Error-Tolerant Design*, University of Michigan, 2000) extended **GOMS (Goals, Operators, Methods, Selection rules)** to predict where human errors will occur in an interface. His 2002 paper with Mike Byrne (&ldquo;A Cognitive Approach to Designing Human Error Tolerant Interfaces&rdquo;) provides a **7-layer defense framework** mapped to stages of erroneous performance:

| Layer | Stage | Audit Question |
|-------|-------|----------------|
| Prevention | Before error | Can this error class be eliminated by design constraints? |
| Reduction | Before error | Does the task model minimize opportunities for this error? |
| Detection | After commission | Will the user notice something went wrong? |
| Identification | After detection | Can the user understand *what* went wrong? |
| Correction | After identification | Is fixing it straightforward and discoverable? |
| Resumption | After correction | Can the user return to their task without losing context? |
| Mitigation | Unrecoverable | Is damage minimized when all else fails? |

Built on **Rasmussen&rsquo;s Skills-Rules-Knowledge (SRK) framework**: skill-based errors (slips) need different defenses than rule-based errors (misapplication) and knowledge-based errors (wrong mental model). The audit must classify error risks by SRK level. Wood&rsquo;s extension also introduced two key error mechanisms from Reason&rsquo;s taxonomy: **similarity matching** (wrong-but-similar rule fires) and **frequency gambling** (most-used routine executes even when context demands otherwise).

**Key references (open access):** [eScholarship](https://escholarship.org/uc/item/4nr8x5b1) &mdash; Wood &amp; Byrne, CogSci 2002; [IITSEC 2002](https://web.eecs.umich.edu/~kieras/docs/GOMS/Wood_IITSEC2002.pdf) &mdash; Wood, &ldquo;Modeling Human Error for Experimentation, Training, and Error-Tolerant Design&rdquo;. Dissertation: [ProQuest](https://www.proquest.com/openview/7a8b78bcf5d8ab261d06fed9d096bdba/) (abstract free, full text paywalled; author copy available on request).

#### 2. Visual Grounding &amp; Common Ground (Gergle, Kraut, Fussell)

Darren Gergle&rsquo;s research at Northwestern (CollabLab, CHI Academy 2026) demonstrates that **shared visual information** directly affects task performance through two distinct mechanisms:

- **Situation awareness** &mdash; does the user understand the current state of the system?
- **Conversational grounding** &mdash; does the interface provide enough shared context for efficient communication (between user and system, or between collaborating users)?

His work shows that not just the *availability* but the *form* of visual information differentially affects coordination. Key findings for audit criteria:

- Delayed visual feedback degrades collaborative performance (latency thresholds)
- Display characteristics affect spatial task performance
- Age and demographic bias can be embedded in computational systems (sentiment analysis, data contribution)
- Accessibility barriers in collaborative tools are systematic, not incidental

Gergle also co-developed the **Joint Action Storyboard** framework (&ldquo;Joint Action Storyboards: A Framework for Visualizing Communication Grounding Costs&rdquo;, CSCW 2021) &mdash; a structured method that maps each UI interaction to its grounding cost, identifying exactly where the design forces users to do extra cognitive work. This is essentially a ready-made audit tool for Phase 4.

**Key references:** Gergle, Kraut &amp; Fussell, &ldquo;Using Visual Information for Grounding and Awareness&rdquo; (*Human-Computer Interaction*, 2013); &ldquo;Language Efficiency and Visual Technology&rdquo; (*JLSP*, 2004); &ldquo;Joint Action Storyboards&rdquo; (CSCW 2021); &ldquo;Addressing Age-Related Bias in Sentiment Analysis&rdquo; (CHI 2018, Best Paper); &ldquo;Model Positionality and Computational Reflexivity&rdquo; (CHI 2022, Best Paper HM). Also: Brinck, Gergle &amp; Wood, *Usability for the Web: Designing Web Sites that Work* (Morgan Kaufmann, 2001) &mdash; a &ldquo;pervasive usability&rdquo; framework with stage-by-stage checklists. Note: Wood and Gergle co-authored this book &mdash; the two primary academic foundations for this skill converge in a single prior collaboration.

#### 3. Cognitive Load (Sweller, Madsen, NASA-TLX)

Cognitive load theory provides the quantitative backbone: every interface decision either consumes or conserves working memory. The **NASA-TLX** framework (6 dimensions: mental demand, physical demand, temporal demand, effort, performance, frustration) offers structured evaluation of interface complexity. Jes Buster Madsen&rsquo;s work on cognitive load in team sports (&ldquo;Evaluation of Cognitive Load in Team Sports&rdquo;, *PeerJ*, 2021) confirms that as cognitive load increases, decision-making accuracy decreases &mdash; directly applicable to information-dense dashboards and multi-step workflows.

### Proposed audit phases

| Phase | Focus | Primary Framework |
|-------|-------|-------------------|
| 0. Task Model Mapping | Map user goals &rarr; tasks &rarr; operations. Identify where the interface&rsquo;s task decomposition diverges from users&rsquo; mental models. Evaluate across the **user expertise spectrum** (kiosk/first-time &rarr; regular &rarr; power user) &mdash; each has a different task decomposition and the interface must degrade gracefully across all three | GOMS (Card, Moran &amp; Newell) |
| 1. Consistency &amp; Convention | Same patterns for same operations across the entire interface. Leverage existing knowledge (platform conventions, domain standards) | Nielsen&rsquo;s heuristics + GOMS |
| 2. Error Tolerance | For each critical task path, evaluate all 7 defense layers. Classify error risks by Rasmussen SRK level | Wood &amp; Byrne 7-layer |
| 3. Cognitive Load | Information density per screen, decision points per task, working memory demands, progressive disclosure | NASA-TLX + Sweller CLT |
| 4. Visual Grounding | Feedback sufficiency, state visibility, shared context for collaborative workflows, latency tolerance | Gergle grounding theory |
| 5. Accessibility &amp; Inclusion | Demographic bias in data presentation, age/ability inclusivity, assistive technology compatibility | Gergle bias research + WCAG |
| 6. Information Architecture | Navigation coherence, error recovery paths, undo/resume affordances, breadcrumb trails | Combined frameworks |

### Coded rules (complementary to skill)

In addition to the skill&rsquo;s manual audit phases, codify machine-checkable rules for common task model violations:

- **Inconsistent action vocabulary** &mdash; same operation uses different labels/icons across pages (grep-detectable in Streamlit/React codebases)
- **Missing confirmation on destructive actions** &mdash; delete/overwrite without undo or confirmation dialog
- **Dead-end states** &mdash; error pages or empty states with no clear recovery path
- **Orphaned navigation** &mdash; pages reachable only by direct URL, not discoverable from the UI
- **Overloaded screens** &mdash; more than N interactive elements per viewport (configurable threshold)
- **Inconsistent response patterns** &mdash; success feedback uses different mechanisms across features (toast vs inline vs redirect)

### Relationship to existing skills

| Skill | Overlap | Distinction |
|-------|---------|-------------|
| `security-audit` | Both scan code for anti-patterns | Security focuses on attack surface; UI/HCI focuses on task model alignment |
| `optimization-audit` | Both evaluate response latency | Optimization focuses on server-side; UI/HCI focuses on perceived responsiveness and cognitive cost |
| `observability-audit` | Both care about feedback loops | Observability instruments for engineers; UI/HCI audits feedback *for end users* |
| `final-review` | Both run before commit | Final review checks code quality and docs; UI/HCI audits the human experience |

### Implementation approach

1. **Skill authoring** &mdash; new `cognitive-interface-audit` skill in [mad-scientist-skills](https://github.com/karsten-s-nielsen/mad-scientist-skills) following the existing security/optimization/observability pattern (planning + audit modes, severity classification, phase-based execution). Beta version complete &mdash; SKILL.md + 5 templates authored
2. **Coded rules** &mdash; Ruff-style grep patterns for common UI anti-patterns, integrated into Phase 0 (fast scan before deeper analysis)
3. **Framework templates** &mdash; `templates/task-model-analysis.md`, `templates/error-tolerance-checklist.md`, `templates/cognitive-load-assessment.md` following the template pattern established in the optimization and security audit skills

### Dependencies

- No blocking dependencies &mdash; skill can be authored at any time
- First test target: this project&rsquo;s Streamlit app (11 pages, data-dense dashboards &mdash; ideal candidate for mental model audit)
- Synergistic with HF Space expansion (D1/D2) &mdash; Gradio UI would benefit from audit before shipping

---

## Deep Learning Infrastructure &amp; Pre-trained Models

**Status:** Partially implemented &mdash; MLflow UC Model Registry active (D11), model validation &amp; drift detection deployed (D12). Tier 3 GPU training proven &mdash; xG v2 trained on HF Jobs A10G (D17), VAEP training migrated to HF Jobs (O2)
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

**FunSearch / AlphaEvolve pattern.** LLM-driven algorithm evolution: define an `evaluate(candidate) &rarr; score` function, let an LLM generate and mutate candidates, keep the best. [OpenEvolve](https://huggingface.co/blog/codelion/openevolve) (MIT) is a community implementation that works with any LLM API. Targets: evolve xT grid values against StatsBomb event data; optimize pitch control kernel vectorization strategies. Cost: ~$5-20 for a weekend search run, CPU only.

**JAX `vmap` vectorization.** Single highest-leverage tool for existing code. `jax.vmap` vectorizes `compute_pitch_control_at_point()` from ~2,700 serial Python calls per second to one array operation &mdash; unlocking full Space Creation (Fernandez &amp; Bornn 2018 OBSO) on the existing budget without GPU. JAX compiles to vectorized CPU operations via XLA; no infrastructure change required. **Implemented in Phase 18:** `compute_pitch_control_grid_fast()` with `@jax.jit` backend in `src/analytics/pitch_control.py`. Dual NumPy/JAX backend auto-dispatches based on JAX availability.

**Continual learning (EWC / Knowledge Distillation).** DeepMind's Elastic Weight Consolidation (Kirkpatrick et al. 2017) prevents catastrophic forgetting when adapting models to new seasons or competitions. The practical variant &mdash; Knowledge Distillation (Learning without Forgetting) &mdash; maps directly to the MLflow Champion/Challenger pattern: the `@Champion` model provides soft labels for `@Challenger` training on new data, preserving historical calibration.

### Data augmentation for limited tracking data

With only 20 tracking matches, synthetic data multiplication is critical:

| Technique | Multiplier | Compute | Basis |
|-----------|-----------|---------|-------|
| **Symmetry augmentation** (H-flip, V-flip, team swap) | 8&times; | Zero (NumPy) | TacticAI (DeepMind, 2024) — **Implemented (Phase 18):** `src/analytics/symmetry.py` |
| **Physics-based perturbation** (position/velocity jitter within constraints) | 10&times; per frame | Minimal (NumPy) | Counterfactual simulation |
| **dm_control MuJoCo Soccer** (synthetic match generation) | Unlimited | CPU | Pretrain-then-fine-tune pattern |

### Pre-trained models: immediately usable

Models with available weights compatible with current data sources:

| Model | Domain | Data Compatibility | License | Compute |
|-------|--------|-------------------|---------|---------|
| [**football2vec**](https://github.com/ofirmg/football2vec) | Player/action embeddings | StatsBomb (exact match) | MIT | Hours / CPU |
| ~~[**OpenSTARLab**](https://github.com/open-starlab) (LEM, FMS, Seq2Event)~~ | ~~Event prediction, match simulation~~ | ~~StatsBomb + Wyscout~~ | ~~Apache 2.0~~ | ~~Dropped — UEID format incompatible with multi-league data ([decision](docs/decisions/openstarlab-dropped.md))~~ |
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
| **Space Creation** (ROADMAP) | JAX `vmap` pitch control vectorization makes full OBSO feasible on CPU — **JAX kernel implemented (Phase 18)** |
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

1. **JAX vs PyTorch**: JAX `vmap` for pitch control implemented (Phase 18). GNN ecosystem is PyTorch-centric. Currently maintaining both: JAX for array computation (pitch control, OBSO), PyTorch Geometric for GNN training.
2. **External GPU provider**: RunPod (cheapest) vs Lambda Labs (more reliable, SSD-backed)?
3. ~~**football2vec**~~ Resolved &mdash; retrained on full ~3,000-match StatsBomb corpus (Phase 15 complete).
4. **Feature store scope**: Which player features justify formal Databricks Feature Engineering tables?
5. **Serving strategy**: CPU batch inference (simple, scheduled) vs scale-to-zero endpoint (real-time)?
6. **SoccerMaster timeline**: Monitor GitHub for weight release &mdash; could consolidate multiple point solutions

### Dependencies

- JAX `vmap` and symmetry augmentation **complete** (Phase 18) — no longer blocked
- GNN pre-training depends on PyTorch Geometric + external GPU access
- football2vec usable immediately with existing StatsBomb/Wyscout data (OpenSTARLab dropped — [decision](docs/decisions/openstarlab-dropped.md))
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

## Team Shape Analysis (Formation Detection &amp; Spatial Metrics)

**Status:** Stage 1 ready for implementation (existing tracking data); Stage 2 blocked by SkillCorner DoD commercial relationship
**License:** `unravelsports` &mdash; MPL 2.0 ([UnravelSports/unravelsports](https://github.com/UnravelSports/unravelsports))

Formation detection, team spatial metrics, and connected-formation visualizations derived from tracking data. Designed to be immediately useful to non-data-science audiences (parents, youth coaches) &mdash; metrics like team length, defensive line height, and GK-to-backline distance are intuitive without statistical background.

### Why it matters

Team shape metrics are the most relatable tracking-derived analytics for non-expert audiences. "We got stretched in the last 15 minutes" or "defensive line sat at 42% &mdash; we pressed high" communicate tactical reality without requiring explanation of xG, VAEP, or pitch control surfaces. FIFA match reports have normalized these metrics since the 2018 World Cup (EPTS tracking at 25Hz across all 64 matches).

### Stage 1 &mdash; Team Shape from Existing Tracking Data

Works immediately on the 20 matches already in `fct_tracking_frames` (Metrica, IDSSE, SkillCorner open data). No external dependencies.

#### Metrics catalog

**Tier 1 &mdash; Intuitive (no explanation needed)**

| Metric | Definition | Reference Values |
|--------|-----------|-----------------|
| **Average position map** | Mean (x, y) per player per phase &mdash; the "where does each player stand" formation diagram | Visual &mdash; dots on pitch with jersey numbers |
| **Team length** | `max(y) - min(y)` of outfield players along the goal-to-goal axis | &lt;30m defending = compact; &gt;40m = stretched (Fradua et al. 2013) |
| **Team width** | `max(x) - min(x)` of outfield players | &gt;38m in possession = good width creation |
| **Defensive line height** | Mean y of back line cluster, normalized to pitch % (0% = own goal, 100% = opponent goal) | &gt;50% = high press; &lt;35% = deep block |
| **GK-to-backline distance** | GK y minus mean(back line y) | FIFA reports this explicitly in TSG match reports |

**Tier 2 &mdash; One sentence of explanation**

| Metric | Definition |
|--------|-----------|
| **Team area (convex hull)** | Area of smallest polygon containing 10 outfield players (`scipy.spatial.ConvexHull`). ~1,000 m&sup2; defending, ~1,500 m&sup2; attacking (Frencken et al. 2011) |
| **Inter-line gaps** | Distance between defensive &harr; midfield and midfield &harr; attacking line centroids. &lt;12m = compact, &gt;18m = exposed |
| **Compactness time series** | Team length over match time, annotated with goals/subs |
| **Stretch index** | Mean distance of all players from team centroid (Bourbousson et al. 2010). More robust than length/width &mdash; not distorted by a single outlier |

**Tier 3 &mdash; Coaching conversation starters**

| Metric | Definition |
|--------|-----------|
| **Formation detection** | Automatic classification via EFPI template matching (e.g., "4-3-3 in possession, 4-4-2 out of possession") |
| **Phase-split shapes** | In-possession vs out-of-possession averages (Shaw &amp; Glickman 2019) |
| **Shape comparison** | Side-by-side convex hulls: your team vs opponent |

#### Formation detection: EFPI (unravelsports)

The `unravelsports` package implements EFPI &mdash; Elastic Formation and Position Identification (Bekkers &amp; Dabadghao 2025). It uses the Hungarian algorithm (linear sum assignment) with scale-normalized template matching against 65 formation templates from mplsoccer. Key properties:

- Integrates with **kloppy** (already used for SkillCorner/Metrica ingestion)
- Configurable time windows: per-frame, per-possession, per-5-minutes, per-period
- Stability filtering (`change_threshold`) prevents spurious formation flips
- Handles missing players (red cards, substitutions) gracefully
- Maintained by Joris Bekkers (PySport co-founder) &mdash; connected on LinkedIn, met at MIT Sloan

**Alternative approaches evaluated but not selected:**
- SoccerCPD (Kim et al., KDD 2022) &mdash; state-of-the-art change-point detection but requires R runtime via `rpy2`, incompatible with Databricks serverless
- Naive y-sort grouping &mdash; works for static averages but fails during transitions
- Delaunay triangulation fingerprinting (Narizuka &amp; Yamazaki 2019) &mdash; topology-invariant but outputs abstract distance matrices, not human-readable formation labels

#### Streamlit page design

A "Team Shape" page following the Pitch Control page pattern, with two views:

**Snapshot view** (single frame or phase average):
- Pitch diagram with player positions (jersey numbers) and connected formation lines (GK &rarr; back line &rarr; midfield &rarr; attack)
- Convex hull overlay (shaded polygon per team)
- Sidebar `st.metric` widgets: team length, width, defensive line height, GK-backline distance, convex hull area &mdash; with `delta=` showing difference from match average

**Timeline view** (full match):
- Time-series chart of team length, width, and defensive line height
- Annotated with goals, substitutions, formation changes
- Formation label per 5-minute window (via EFPI)
- Phase comparison: in-possession vs out-of-possession metric averages

#### Compute approach

All Tier 1 and Tier 2 metrics are lightweight NumPy/scipy operations on per-frame player positions &mdash; no heavy compute pipeline needed:
- `scipy.spatial.ConvexHull` for team area
- Basic y-sorting + k-means (k=3 or k=4) for line detection
- EFPI for formation labels (runs in Streamlit or pre-computed via `applyInPandas`)

Option to pre-compute and persist as a dbt mart (e.g., `fct_team_shape`) for larger datasets, but Streamlit-side computation is sufficient for the current 20-match corpus.

#### Potential artifacts

| Artifact | Layer | Description |
|----------|-------|-------------|
| `src/analytics/team_shape.py` | Analytics | Team centroid, convex hull, line detection, shape metrics |
| `fct_team_shape` (optional) | dbt marts | Pre-computed per-frame or per-window team shape metrics |
| `src/streamlit_app/pages/team_shape.py` | Streamlit | Team Shape page (snapshot + timeline views) |
| `src/streamlit_app/components/pitch.py` (update) | Streamlit | Connected-formation diagram renderer, convex hull overlay |

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

- Phase 10 (tracking data ingestion) &mdash; **complete** (20 matches available)
- Phase 11 (pitch control) &mdash; **complete** (Streamlit page pattern to follow)
- `unravelsports` package (MPL 2.0) &mdash; new dependency for EFPI formation detection
- Stage 2 only: SkillCorner DoD commercial access + [Provider Framework](#provider-abstraction--multi-tier-ingestion) adapter
- Synergistic with Visual Exploratory Behavior (same own-footage pipeline for Stage 2)
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
| Synced tables | 17 tables | Subset (fact tables only for validation) |
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

**Status:** Implemented — D14 batch on HF Jobs A10G, `vmap`-batched PC, differential OBSO per player
**Paper:** Fernandez & Bornn (2018), "Wide Open Spaces: A statistical technique for measuring space creation in professional soccer"

Full OBSO (Off-Ball Scoring Opportunity) requires computing N+1 pitch control surfaces per frame (one counterfactual surface with each player removed) to measure each player's space creation contribution. At 25fps with 22 players, this is ~2,700 pitch control evaluations per second of play — prohibitively expensive for the current compute budget.

### What was implemented instead

Phase 12 implemented a simpler Off-Ball xT metric: `pitch_control(player_location) x xT(player_zone)`, computed at 1fps sampling. This captures positional value without the counterfactual computation.

### What would be needed

- ~~GPU-accelerated pitch control (vectorized TTI computation across grid)~~ **Partially addressed (Phase 18):** `compute_pitch_control_grid_fast()` with `@jax.jit` backend enables dense 50&times;32 grid computation on CPU via XLA vectorization
- ~25x compute budget increase (from 1fps to 25fps full OBSO) — JAX kernel reduces this requirement significantly
- Differential pitch control: `PC_with_player - PC_without_player` per player per frame
- Counterfactual ghost trajectories (available in PAUSA repo, see PAUSA section)

### Dependencies

- Phase 11 (pitch control) — **complete**
- Phase 12 (off-ball xT) — **complete** (provides foundation)
- Phase 18 (JAX kernel) — **complete** (enables dense grid computation for OBSO)
- ~~GPU compute infrastructure~~ JAX CPU vectorization may be sufficient for batch processing

---

## <img src="assets/hf-logo.png" height="28" align="top"> HuggingFace Hub Integration (Open Model & Dataset Ecosystem)

**Status:** Tiers 1&ndash;3 complete (4 models, 11 datasets published, GPU training proven on HF Jobs A10G), Tier 4 complete (Gradio demo Space with luxury flagship theme).
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
| ~~[**OpenSTARLab**](https://arxiv.org/html/2502.02785v2) (LEM, FMS, Seq2Event)~~ | ~~GitHub (Apache 2.0)~~ | ~~Apache 2.0~~ | ~~Dropped — `openstarlab-event` requires hardcoded La Liga UEID format incompatible with multi-league data. See [decision](docs/decisions/openstarlab-dropped.md).~~ |
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

**Status:** Complete

A Gradio Space hosting a read-only demo with pre-cached Parquet subsets. Complements the primary Streamlit deployment on HuggingFace Spaces (which has live Lakebase connectivity) as a lightweight public portfolio piece.

**Live at:** [`luxury-lakehouse/soccer-analytics-demo`](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo)

**Tabs** (in order): Pass Quality, Pass Timing, Pitch Control, Player Similarity, Shot Map, DEFCON Pressure

**Theme:** Luxury flagship &mdash; `gr.themes.Monochrome` with dark surfaces (`#0f0f14`), amber/gold accents (`#f59e0b`), sharp corners, Inter font, and CSS-injected tab navigation with gold bottom-border active state.

**Constraint:** No Lakebase connectivity from HF Spaces. All data must be pre-exported as static Parquet/CSV files bundled with the Space. This limits the demo to curated subsets rather than full interactive queries. Export notebook: `notebooks/export_demo_data.py`.

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
5. ~~**Space framework**~~ Resolved &mdash; Gradio chosen for model/dataset demos. Deployed at `luxury-lakehouse/soccer-analytics-demo`. Streamlit primary deployment migrated to HuggingFace Spaces (Docker SDK) at `luxury-lakehouse/soccer-analytics-app` with live Lakebase connectivity.
6. **HF Jobs vs RunPod**: Defer comparison until DEFCON Tier 4, or benchmark early with a small training run?

### Dependencies

- Tier 1 (consume) &mdash; **complete** (football2vec retrained on StatsBomb corpus)
- Tier 2 (publish) &mdash; **complete** (4 models + 11 datasets published: [football2vec](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout), [xG v1](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout), [xG v2 set encoder](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder), [VAEP](https://huggingface.co/luxury-lakehouse/vaep-model-statsbomb-wyscout), plus datasets for SPADL/VAEP, line-breaking, embeddings, pitch control, xT grids, OBSO inputs/values/grids, freeze frames, shots, space creation)
- Tier 3 (train) &mdash; **complete** (xG v2 trained on HF Jobs A10G (D17); VAEP training migrated to HF Jobs (O2); HF Jobs proven as primary external GPU provider)
- Tier 4 (demo) &mdash; **complete** (Gradio Space at [`luxury-lakehouse/soccer-analytics-demo`](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) with luxury flagship theme and 6 tabs: pass quality, pass timing, pitch control, player similarity, shot map, DEFCON pressure)
- Tier 5 (streaming) &mdash; blocked on Polars `hf://buckets/` merge (see below)
- Synergistic with DL Infrastructure (HF models flow into MLflow + UC model registry)
- Synergistic with Provider Abstraction (football2vec consumes same StatsBomb/Wyscout data)

### Tier 5 &mdash; Streaming dataset publishing via XET + Polars

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

## PAUSA: Optimal Pass Timing &amp; OBSO Value Surface

**Status:** Implemented (D9+D10+D16) &mdash; ELASTIC sync, OBSO surfaces, PAUSA pipeline, Streamlit page, HF Space tab all deployed
**Paper:** Lee, Jo, Hong, Bauer &amp; Ko (2026), "Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed" (MIT Sloan 2026 finalist, top 7 of 200+)
**Repo:** [`leemingo/mitssac-pausa`](https://github.com/leemingo/mitssac-pausa) (public, Apache-2.0)
**License status:** Apache-2.0 merged by Minho Lee (2026-03-13). No license blocker remaining.

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
- `xT_grid.json` (Karun Singh 12&times;8, equivalent to our `expected_threat_grids` Delta table)

### Compute profile

Virtual mode is the heavy-lifter: each pass generates ~100 ghost frames &times; 1,600 grid cells of pitch control. The repo parallelizes via `joblib` with `n_jobs=-1`. For 7 matches this is feasible locally; at scale it needs distributed compute (Databricks serverless or GPU).

### Integration path

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

1. ~~**License**~~: Resolved &mdash; Apache-2.0 merged by Minho Lee (2026-03-13).
2. ~~**Numba adoption**: Should we add Numba to our pitch control module? Adds a compiled dependency but significant speedup.~~ **Resolved** &mdash; No Numba. JAX kernel extended with ghost trajectory support (Phase D16).
3. ~~**Coordinate system**: Their code uses centered coordinates (&minus;52.5 to +52.5). Our stack uses StatsBomb 120&times;80. Adapter or full migration?~~ **Resolved** &mdash; StatsBomb 120&times;80 at API boundary, internal meter conversion where physics requires.
4. ~~**Static grids**: The EPV and Transition grids are pre-computed (provenance unclear). Train our own from StatsBomb data, or use theirs as-is?~~ **Resolved** &mdash; Using PAUSA repo grids as-is. Custom training deferred (tracked in TODO.md).
5. ~~**Scope**: Full PAUSA pipeline (heavy) or start with ELASTIC sync + OBSO surface only (lighter, more broadly useful)?~~ **Resolved** &mdash; Full PAUSA pipeline implemented (D9+D10+D16).

### Dependencies

- ~~License from `leemingo/mitssac-pausa`~~ Resolved (Apache-2.0 merged 2026-03-13)
- Phase 10 (IDSSE tracking) &mdash; **complete** (same 7 matches)
- Phase 11 (pitch control) &mdash; **complete** (foundation for OBSO)
- Phase 12 (off-ball xT) &mdash; **complete** (OBSO is the full version)
- Synergistic with DL Infrastructure (Numba JIT, joblib parallelization)
- Synergistic with Space Creation (OBSO + ghost trajectories enable counterfactual analysis)

---

## Other Ideas (Unscheduled)

- [ ] Voronoi area persistence &mdash; pre-compute in dbt (lower priority if Phase 11 replaces Voronoi)
- [ ] Pitch Control animation &mdash; frame-by-frame playback in Streamlit
- [ ] Event overlay on Pitch Control &mdash; render events on pitch control view
- [ ] Wyscout match metadata &mdash; formations, coaches, venue (not in public Figshare dataset)
- [ ] **Local GPU Compute Sidecar** &mdash; optional local GPU acceleration for ML workloads (training, inference, CV) using Docker + NVIDIA Container Toolkit. Cloud by default, local if GPU available. Pattern: `delta-rs` reads input from Unity Catalog, GPU container runs computation, writes results back to Delta. Triton Inference Server or MLflow model serving for persistent endpoints. Relevant when neural xG training, DEFCON Tier 4 GNN, or computer vision workloads begin. Not needed for current CPU-based pipelines


---

*Items graduate from this roadmap into numbered phases in [ARCHITECTURE.md](ARCHITECTURE.md) when prerequisites are met and the scope is well-defined.*
