# Model Ops & Event Sync — Design Spec

**Date:** 2026-03-15
**Branch:** `feature/model-ops-and-event-sync`
**Scope:** D9, D10, D11, D12, D13, D16 + Pass Timing Streamlit page

---

## 1. Overview

Seven work items on a single feature branch, building the MLOps foundation and the PAUSA optimal pass timing pipeline:

| # | Item | Size | Summary |
|---|------|------|---------|
| D11 | MLflow Registry + HF Jobs Template | Dunkin' | Register models in UC Model Registry, Champion/Challenger aliases, HF Jobs training template |
| D13 | Physics-Based Tracking Augmentation | Dunkin' | Position jitter within physical constraints, 10× in-memory multiplier, pure NumPy |
| D9 | ELASTIC Event-Tracking Sync | Wicked | IDSSE event XML ingestion + ELASTIC frame alignment (Kim et al. 2025) |
| D16 | OBSO Batch on HF Jobs GPU | Wicked | Full OBSO value surfaces via JAX on A10G GPU |
| D10 | Full PAUSA Pipeline | Wicked | Ghost trajectories, temporal/spatial decomposition, Delta tables, dbt marts |
| D12 | Model Validation & Drift Detection | Dunkin' | PSI, Wasserstein, CUSUM, hard bounds — pure scipy/numpy |
| — | Pass Timing Streamlit Page | Part of D10 | OBSO heatmap, PAUSA scores, player rankings |
| — | HF Space Pass Timing Tab | Part of D10 | Gradio tab for live PAUSA demo on HuggingFace |

**Implementation order:** D11 → D13 → D9 → D16 → D10 + Streamlit → D12

**Commit strategy:** Single commit of fully E2E-tested code. Additional commits only with explicit approval.

## 2. Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| PAUSA scope | Full pipeline (ghost trajectories + temporal/spatial decomposition) | Complete metric, not just OBSO surface |
| Training compute | HF Jobs (not RunPod/Lambda Labs) | Existing HF Hub integration, PEP 723 template proven |
| MLflow scope | Registry + experiment tracking + HF Jobs template on existing CPU workloads | Prove pattern before GPU workloads (D17/D18) |
| Drift detection | Pure scipy/numpy | Zero new deps, Databricks-serverless-friendly, "Dunkin'" size |
| Static grids | PAUSA repo EPV/Transition as-is | Defer custom training (tracked in TODO.md) |
| JIT compiler | JAX only, extend kernel for ghost trajectories | No Numba — JAX has native GPU, already in codebase |
| Coordinate system | StatsBomb 120×80 at API boundary, meter-space internally | No coordinate island, clean joins with all gold tables |
| Augmentation storage | In-memory only | 88× multiplier → ~3.3B rows is impractical to materialize |
| Monitoring library | Deferred NannyML CBPE | Tracked in TODO.md for future evaluation |

## 3. D11 — MLflow Registry + HF Jobs Training Template

### Architecture

```
Training Path (notebook or HF Jobs script):
  mlflow.set_experiment("soccer_analytics/<model_name>")
  mlflow.start_run()
    → log_params(), log_metrics(), log_model()
    → mlflow.register_model("models:/soccer_analytics.dev_gold.<model>")
    → client.set_registered_model_alias(..., "Champion", version)

Inference Path (Databricks serverless job):
  model = mlflow.pyfunc.load_model("models:/soccer_analytics.dev_gold.<model>@Champion")
  bytes = serialize(model)  # driver-side only
  spark.groupBy(...).applyInPandas(udf_with_bytes_closure)
```

### Models to Register

| Model | UC Registry Path | Current State | D11 Change |
|-------|-----------------|---------------|------------|
| xG XGBoost | `soccer_analytics.dev_gold.xg_model` | Custom JSON in UC Volume + HF Hub | Add `mlflow.log_model()` to training notebook, register with `@Champion`/`@Baseline` aliases |
| Football2Vec | `soccer_analytics.dev_gold.football2vec` | Gensim files in UC Volume + HF Hub | Wire existing `Football2VecModel` pyfunc stub, register |
| VAEP scoring/conceding | `soccer_analytics.dev_gold.vaep_model` | Ephemeral (retrained every run) | Train once, persist via MLflow, load `@Champion` in pipeline |
| DEFCON value estimators | `soccer_analytics.dev_gold.defcon_model` | Ephemeral (retrained every run) | Same as VAEP |
| xT grid | No registry (not a model) | Delta table `expected_threat_grids` | Log as MLflow artifact for provenance |

### HF Jobs Template

Extend existing `scripts/compute_xt_grid_hf.py` pattern:

```
scripts/train_<model>_hf.py (PEP 723 inline metadata)
  → hf jobs uv run scripts/train_<model>_hf.py \
       --flavor <cpu-basic|a10g> --timeout 30m \
       --secrets HF_TOKEN=$HF_TOKEN
  → Downloads training data from HF Hub dataset
  → Trains model (CPU or GPU)
  → Logs to MLflow (remote tracking URI → Databricks workspace)
  → Pushes weights to HF Hub model repo
  → (separately) sync_hf_weights.py → UC Volume → register @Champion
```

First exercise: re-run xG model training as HF Jobs CPU script with MLflow logging.

### New/Modified Files

- `notebooks/train_xg_model.py` — add MLflow logging + model registration
- `notebooks/train_football2vec.py` — add MLflow logging + model registration
- `scripts/train_xg_model_hf.py` — new HF Jobs CPU template proof-of-concept
- `src/ingestion/spadl_vaep.py` — load `@Champion` instead of retraining every run
- `src/ingestion/defcon_lite.py` — load `@Champion` instead of retraining every run
- `src/ingestion/xg_model.py` — load via MLflow URI on driver
- `pyproject.toml` — add `mlflow` optional dependency group

### Constraints

- `mlflow` in notebooks and `scripts/` only — `src/` loads models via MLflow URI on driver, never on executors (no internet in UDFs)
- VAEP/DEFCON persistence requires one-time training notebook run to register initial `@Champion`
- `Football2VecModel` pyfunc wrapper handles gensim multi-file format (already designed)

## 4. D13 — Physics-Based Tracking Augmentation

### Module Design

New file: `src/analytics/augmentation.py` — pure NumPy.

```python
@dataclass(frozen=True)
class PerturbationConfig:
    """Physical constraints for position jitter."""
    max_speed_ms: float = 12.0          # Elite sprint ceiling (m/s)
    max_acceleration_ms2: float = 7.0   # From PitchControlParams
    jitter_sigma_m: float = 0.10        # Position noise std dev (meters)
    n_perturbations: int = 10           # Draws per frame
    pitch_length_m: float = 105.0       # Meter-space bounds
    pitch_width_m: float = 68.0
```

### Algorithm

1. For each frame, draw `n_perturbations` independent Gaussian samples: `dx, dy ~ N(0, jitter_sigma_m)`
2. Add noise to each player's `(x, y)` position in meter-space
3. Clamp positions to pitch bounds
4. **Re-derive velocities** from perturbed position deltas — guarantees kinematic consistency (matches how `fct_tracking_frames` derives velocity from LAG)
5. Clamp resulting speed to `max_speed_ms` — if perturbation creates impossible speed, scale velocity vector down
6. Return `list[pd.DataFrame]` with `augmentation` column (`"jitter_0"` through `"jitter_9"`) and `jitter_seed` for reproducibility

### Composition with Symmetry

Symmetry operates in StatsBomb 120×80 coordinates; jitter operates in meter-space. The `augment_full` wrapper handles coordinate conversion internally (using the same `_sb_to_meters_x/y` pattern from `pitch_control.py`): convert to meters → jitter → convert back.

```python
def augment_full(df, sym_config, pert_config, rng) -> list[pd.DataFrame]:
    """8× symmetry × (1 + 10) jitter = 88× in-memory multiplier."""
    symmetry_variants = augment_tracking_frame(df, sym_config, include_original=True)  # existing function
    results = []
    for variant in symmetry_variants:
        results.append(variant)                                    # 8 un-jittered
        results.extend(perturb_positions(variant, pert_config, rng))  # 8 × 10 jittered
    return results  # 8 + 80 = 88 total DataFrames
```

**Note:** The multiplier is 88× (8 symmetry originals + 80 jittered variants), not 80×. Both the originals and the jittered versions are useful training data.

### Storage

In-memory only — never materialized to Delta. Called inside `applyInPandas` UDF bodies by future consumers (GNN training, space creation).

### Testing

- Unit tests with synthetic 22-player single-frame data
- Property tests: perturbed speed ≤ `max_speed_ms`, positions within bounds, velocity consistency
- Reproducibility test: same RNG seed → identical output
- Benchmark test: ≤ 1ms per frame for 10 perturbations

### Citation

TacticAI (Wang et al., Nature Communications 2024) — symmetry augmentation foundation.

## 5. D9 — ELASTIC Event-Tracking Sync

### Sub-task A: IDSSE Event Ingestion

Extend `src/ingestion/idsse.py` to parse DFL event XML from the figshare collection.

**New bronze table:** `idsse_events`

| Column | Type | Notes |
|--------|------|-------|
| match_id | string | `idsse_J03...` prefix |
| event_id | string | Unique within match |
| event_type | string | DFL event taxonomy |
| timestamp_seconds | float | From period start |
| period | int | 1 or 2 |
| player_id | string | DFL PersonId |
| team | string | home/away |
| x | float | DFL center-origin meters (raw bronze) |
| y | float | DFL center-origin meters (raw bronze) |
| ball_x | float | |
| ball_y | float | |
| _ingested_at | timestamp | UTC audit column |

**New dbt staging model:** `stg_idsse__events` — coordinate transform to StatsBomb 120×80, matching `stg_idsse__tracking` pattern.

**Entry point:** `ingest_idsse_events = "ingestion.idsse:main_events"` — separate from existing tracking ingestion.

**Upload:** DFL event XML files to UC Volume alongside existing position XMLs.

### Sub-task B: ELASTIC Sync Engine

Two files:
- `src/analytics/elastic_sync.py` — pure compute: feature extraction, frame matching algorithm (no Spark dependency)
- `src/ingestion/elastic_sync.py` — Spark pipeline: reads from Delta, calls analytics module, writes results to Delta via `applyInPandas`

Adapts `elastic/` from `leemingo/mitssac-pausa` (Apache-2.0).

**Input:** Event stream from `stg_idsse__events` + tracking stream from `stg_idsse__tracking` (both StatsBomb 120×80).

**Algorithm (Kim et al. 2025):**
- Compute ball acceleration and player-ball distance features from tracking
- For each event, find the tracking frame with best-matching feature signature
- Output: `(event_id, frame_id, alignment_confidence)` mapping

**Output table:** `elastic_sync_results` (bronze Delta)

| Column | Type |
|--------|------|
| match_id | string |
| event_id | string |
| frame_id | int |
| alignment_confidence | float |
| alignment_error_seconds | float |
| _ingested_at | timestamp |

**New dbt staging model:** `stg_idsse__elastic_sync` — joins events with aligned frames. Named under `idsse/` to clarify ELASTIC is applied to IDSSE data, not a separate source.

**Entry point:** `compute_elastic_sync = "ingestion.elastic_sync:main"`

**Pipeline:** `applyInPandas` grouped by `match_id` (7 groups).

### Testing

- Unit tests with synthetic event+tracking data (known alignment)
- Accuracy validation against paper's 95.5% claim
- Integration test: `elastic_sync_results` joins cleanly with events and tracking

### Citation

Kim, H.S. et al. (2025). "ELASTIC: Event-Tracking Data Synchronization in Soccer Without Annotated Event Locations." ECML-PKDD MLSA 2025. arXiv:2508.09238.

## 6. D16 — OBSO Batch on HF Jobs GPU

### HF Jobs Script

New file: `scripts/compute_obso_hf.py` (PEP 723 inline metadata)

```bash
hf jobs uv run scripts/compute_obso_hf.py \
    --flavor a10g --timeout 60m \
    --secrets HF_TOKEN=$HF_TOKEN
```

**Flow:**
1. Download from HF Hub: tracking data, ELASTIC sync results, event data, static grids (EPV 32×50, Transition 64×100)
2. For each pass in each match:
   - Generate ghost trajectories (constant-velocity, 3s before to 1s after)
   - Compute PPCF via JAX `compute_pitch_control_grid_fast()` on GPU
   - `OBSO = PPCF × Transition × EPV` per grid cell
   - Record actual OBSO at release, peak OBSO across window, optimal OBSO across receivers
3. Log metrics to MLflow (remote tracking URI)
4. Push results to HF Hub as Parquet + UC Volume

### JAX GPU Extension

Extend `src/analytics/pitch_control.py`:

```python
def generate_ghost_trajectories(
    players_df: pd.DataFrame,
    event_frame: int,
    frame_rate: int = 25,
    window_before_s: float = 3.0,
    window_after_s: float = 1.0,
) -> list[pd.DataFrame]:
    """Constant-velocity extrapolation for counterfactual frames."""
```

JAX kernel auto-detects GPU — no code changes needed. HF Jobs script installs `jax[cuda12]` via PEP 723 metadata.

### New Analytics Module

New file: `src/analytics/obso.py`

```python
def compute_obso_surface(
    ppcf_grid: np.ndarray,       # (nx, ny) pitch control
    transition_grid: np.ndarray,  # (64, 100) ball transition probs
    epv_grid: np.ndarray,         # (32, 50) expected possession value
    ball_position: tuple[float, float],
) -> np.ndarray:
    """OBSO = PPCF × Transition(ball→cell) × EPV(cell)."""
```

### Output Tables

| Table | Content |
|-------|---------|
| `obso_surfaces` (bronze Delta) | Per-pass, per-ghost-frame OBSO grid snapshots (Streamlit heatmap) |
| `pausa_raw_scores` (bronze Delta) | Per-pass scalars: actual_obso, peak_obso, optimal_obso |

### Static Grid Storage

- UC Volume: `/Volumes/soccer_analytics/dev_gold/model_weights/obso/`
- HF Hub: `luxury-lakehouse/obso-pausa-values` repo (bundled with dataset)

### OBSO-to-Delta Import

New file: `scripts/import_obso_results.py` — downloads OBSO Parquet from HF Hub (or reads from UC Volume staging path), writes to `obso_surfaces` and `pausa_raw_scores` bronze Delta tables via PySpark. Follows the existing `sync_hf_weights.py` pattern. Entry point: `import_obso_results = "ingestion.import_obso:main"` (or run as a notebook).

### Scale

~3,500 passes × 100 ghost frames × 7,072 grid cells. JAX `vmap` on A10G. Estimated: 15–30 min, ~$0.50–1.00/run.

## 7. D10 — Full PAUSA Pipeline

### Pipeline

New file: `src/ingestion/pausa.py` — Databricks serverless batch, `applyInPandas` pattern.

**Input:** `pausa_raw_scores` (D16) + `elastic_sync_results` (D9) + `stg_idsse__events` (passes).

**Compute:**
- Temporal judgment = `actual_obso / peak_obso`
- Spatial selection = `actual_obso / optimal_obso`
- PAUSA = `temporal_judgment × spatial_selection`

### Delta Tables

**`fct_pausa_values`** — written directly by the `compute_pausa` Python pipeline (same pattern as `fct_tracking_frames`). A dbt staging model `stg_pausa__values` wraps the bronze table for downstream consumption. The synced table `fct_pausa_values_synced` syncs from the pipeline-written table, not from a dbt model. Contract enforced in `_marts__models.yml` with explicit column `data_type` definitions.

Schema (gold):

| Column | Type |
|--------|------|
| pass_id | string |
| match_id | string |
| player_id | string |
| team | string |
| period | int |
| timestamp_seconds | float |
| frame_id | int |
| temporal_judgment | float (0–1) |
| spatial_selection | float (0–1) |
| pausa_score | float (0–1) |
| actual_obso | float |
| peak_obso | float |
| optimal_obso | float |
| receiver_x | float |
| receiver_y | float |

**`fct_pass_timing`** (dbt mart):

| Column | Type |
|--------|------|
| player_id | string |
| match_id | string |
| competition_id | string |
| team_id | string |
| pass_count | int |
| avg_temporal_judgment | float |
| avg_spatial_selection | float |
| avg_pausa | float |
| median_pausa | float |
| passes_above_median_pausa | int |

### dbt Models

- `stg_pausa__values.sql` — staging view
- `int_pausa__pass_quality.sql` — ephemeral CTE joining with player names
- `fct_pass_timing.sql` — mart, contract enforced
- `_marts__models.yml` — contracts + `dbt_expectations` range tests
- New toggle: `pausa_enabled` in `dbt_project.yml`

### Entry Point

`compute_pausa = "ingestion.pausa:main"` — incremental skip guard, `replaceWhere`, structured logging.

### MLflow

Register pipeline runs as experiment `soccer_analytics/pausa` — log row counts and mean scores per run. Not a trained model; experiment tracks pipeline execution.

### Synced Tables (Manual)

- `fct_pausa_values_synced` — create in Databricks UI
- `fct_pass_timing_synced` — create in Databricks UI
- Then: Terraform import → `create_indexes.py` (composite `(match_id, player_id)`) → PG grants

### Citation

Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed." MIT Sloan 2026.

## 8. D12 — Model Validation & Drift Detection

### Module

Two files (same split pattern as ELASTIC):
- `src/analytics/model_validation.py` — pure scipy/numpy validation functions (PSI, Wasserstein, CUSUM, etc.)
- `src/ingestion/model_validation.py` — Spark pipeline: reads gold tables, calls analytics functions, writes results to Delta, emits structured logs

```python
@dataclass(frozen=True)
class ValidationResult:
    model_name: str
    metric_name: str
    value: float
    status: str          # "ok" | "warn" | "alert"
    threshold_warn: float
    threshold_alert: float
    reference_value: float
```

### Validation Functions

| Function | Monitors | Method |
|----------|----------|--------|
| `compute_psi(reference, current, n_bins=10)` | xG output distribution | Population Stability Index |
| `compute_wasserstein_drift(reference, current)` | xT zone coverage, VAEP distribution | `scipy.stats.wasserstein_distance` |
| `compute_cusum(values, target_mean, sigma)` | Line-breaking detection rate | Cumulative sum control chart |
| `check_ks_test(reference, current, alpha=0.05)` | VAEP distribution shape | `scipy.stats.ks_2samp` |
| `check_physical_bounds(df, col, lower, upper)` | Physical stats | Hard range check |
| `check_field_sum_constraint(ppcf_grid, tolerance=0.05)` | Pitch control integrity | Sum ≈ 1.0 |

### Validation Matrix

| Model | Metric | Method | Baseline Source |
|-------|--------|--------|-----------------|
| xG XGBoost | Mean prediction per comp | PSI | StatsBomb La Liga 2015/16 |
| xG XGBoost | ROC-AUC, Brier | Threshold | metrics.json (0.979, 0.059) |
| xT | Zone coverage (96 values) | Wasserstein | `expected_threat_grids` global |
| VAEP | Fraction negative actions | Threshold + CUSUM | 2,388-match corpus |
| VAEP | Full distribution | KS + Wasserstein | Same corpus |
| Line-breaking | Detection rate per match | CUSUM | StatsBomb mean rate |
| Physical stats | max_speed_ms | Hard bound | 15.0 m/s |
| Pitch control | Field sum | Hard constraint | 1.0 ± 5% |
| PAUSA | temporal/spatial ∈ [0,1] | Range bound | Definition |
| PAUSA | Mean per match | CUSUM | 7-match IDSSE baseline |

### Reference Baselines

**dbt seed** `model_baseline_scalars.csv` for scalar thresholds. Distribution references (xT grid, VAEP) in Delta table `dev_gold.model_baselines_distributions`:

| Column | Type | Notes |
|--------|------|-------|
| model_name | string | e.g., `xt_global`, `vaep_statsbomb` |
| metric_name | string | e.g., `zone_distribution`, `value_distribution` |
| reference_values | array&lt;double&gt; | Distribution array (JSON-serialized in seed, native array in Delta) |
| n_samples | int | Number of samples in reference |
| computed_from | string | Source description |
| computed_at | timestamp | When baseline was established |

### Output Table

`dev_gold.model_validation_runs`:

| Column | Type | Notes |
|--------|------|-------|
| run_id | string | Unique per validation run |
| run_date | timestamp | UTC |
| model_name | string | |
| metric_name | string | |
| value | double | Computed metric value |
| status | string | `ok`, `warn`, `alert` |
| threshold_warn | double | |
| threshold_alert | double | |
| reference_value | double | Scalar baseline (NULL for distribution metrics) |
| _ingested_at | timestamp | UTC audit column |

### Entry Point

`run_model_validation = "ingestion.model_validation:main"` — runs post-`dbt build`, reads gold tables, writes to `dev_gold.model_validation_runs`, emits structured JSON logs.

### MLflow

Logs validation metrics to each model's experiment for time-series CUSUM.

## 9. Pass Timing Streamlit Page

### Registration

```python
st.Page("pages/pass_timing.py", title="Pass Timing", url_path="pass-timing", icon="⏱")
```

Placed after "Pitch Control" in navigation.

### Layout

**Filter bar** (top): Competition → Match → Team → Player dropdowns. Session state persistence.

**Metrics row**: Three `st.columns(3)` with `st.metric` — Avg PAUSA, Avg Temporal Judgment, Avg Spatial Selection. All with `help=` tooltips.

**Visualization row**: Two `st.columns([1, 1])`:
- **Left**: OBSO heatmap on pitch overlay. Actual release = filled marker, peak OBSO = open marker.
- **Right**: Scatter plot. x = temporal judgment, y = spatial selection, size = PAUSA score. Quadrant labels.

**Rankings table**: Full-width `st.dataframe`, sortable. `LIMIT 500`.

**Footer**: Academic citation caption + data scope label ("IDSSE Bundesliga · 7 matches · Tracking-dependent").

### UX Standards

- Every `st.metric` has `help=` (PAUSA, temporal judgment, spatial selection in `glossary.py`)
- `data_scope_note()` from `feedback.py`
- `empty_select()` / `empty_result()` for empty states
- Filter persistence via `st.session_state`
- Bounded queries: `LIMIT 500` rankings, `LIMIT 2000` per-pass data
- No raw IDs in UI — join to `dim_players`, `dim_teams`

### Glossary Entries

```python
"PAUSA": "Passing Ability Under Spatiotemporal Awareness. Composite of temporal judgment × spatial selection. Higher = better pass timing and target choice. (Lee et al., MIT Sloan 2026)",
"Temporal Judgment": "Was the pass released at the optimal moment? Ratio of actual OBSO at release to peak OBSO in the ±3s/+1s window. 1.0 = perfect timing.",
"Spatial Selection": "Was the target location the best available? Ratio of actual OBSO at target to maximum OBSO across all receivers. 1.0 = optimal target.",
"OBSO": "Off-Ball Scoring Opportunity. Continuous value surface: Pitch Control × Ball Transition Probability × Expected Possession Value. (Spearman 2018, Fernandez & Bornn 2018)",
```

## 10. HF Space — Pass Timing Tab

### Purpose

Add a "Pass Timing" tab to the Gradio demo Space (`luxury-lakehouse/soccer-analytics-demo`) so Minho Lee and the broader community can interact with PAUSA results live. Follows the established pattern of pre-cached Parquet data + Gradio tab.

### Data File

New: `demo_space/data/sample_pausa.parquet` — pre-cached subset of `fct_pausa_values` joined with player names for all 7 IDSSE matches. Exported via `notebooks/publish_datasets.py` pattern. ~3,500 passes × ~15 columns — tiny file.

### Gradio Tab

```python
with gr.Tab("Pass Timing"):
    gr.Markdown(
        "PAUSA pass quality: temporal judgment (when) × spatial selection (where).\n\n"
        "*[Lee, Jo, Hong, Bauer & Ko (2026)](https://github.com/leemingo/mitssac-pausa) "
        "PAUSA metric from MIT Sloan 2026. OBSO value surface by "
        "[Spearman (2018)](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals). "
        "Event-tracking sync via [Kim et al. (2025)](https://arxiv.org/abs/2508.09238) ELASTIC.*"
    )
```

### Components

- **Dropdowns**: Match → Team → Player (Gradio `gr.Dropdown`)
- **OBSO Heatmap**: Matplotlib pitch overlay (reuse `mplsoccer.Pitch` from existing Pitch Control tab). Actual release marker + peak OBSO marker.
- **Scatter Plot**: Plotly — x = temporal judgment, y = spatial selection, size = PAUSA score. Matches existing Plotly dark theme (`paper_bgcolor="#1a1a2e"`).
- **Rankings Table**: `gr.Dataframe` — player name, avg PAUSA, avg temporal, avg spatial, pass count. Sorted by PAUSA descending. Column headers include brief interpretation (e.g., "Avg PAUSA (higher = better timing + target)") since Gradio lacks native tooltip support. A `gr.Markdown` legend below the table explains each column for first-time users.

### Deployment

Update `demo_space/requirements.txt` if needed (unlikely — matplotlib, plotly, mplsoccer already present). Push updated Space to HF Hub via `huggingface-cli` or manual upload.

## 11. Cross-Cutting Concerns

### Academic Citations (NOTICE.md)

| Algorithm | Citation | License |
|-----------|----------|---------|
| ELASTIC | Kim et al. (2025). ECML-PKDD MLSA. arXiv:2508.09238 | Apache-2.0 |
| PAUSA/OBSO | Lee et al. (2026). MIT Sloan 2026 | Apache-2.0 |
| OBSO | Spearman (2018). MIT Sloan. Fernandez & Bornn (2018). MIT Sloan | Academic |
| Augmentation | Wang et al. (2024). TacticAI. Nature Communications | Academic |

### dbt Toggle

```yaml
pausa_enabled: true  # in dbt_project.yml vars
```

Model-level SQL uses `var('pausa_enabled', false)` as defensive default (matching existing toggle pattern). Set to `true` in `dbt_project.yml` for production.

### New Entry Points (pyproject.toml)

```toml
ingest_idsse_events = "ingestion.idsse:main_events"
compute_elastic_sync = "ingestion.elastic_sync:main"
compute_pausa = "ingestion.pausa:main"
run_model_validation = "ingestion.model_validation:main"
```

### New Dependencies (pyproject.toml)

```toml
[project.optional-dependencies]
mlflow = ["mlflow>=2.17.0"]
```

No other new dependencies. `jax[cuda12]` in HF Jobs PEP 723 metadata only.

### CI Workflow Changes

- Add `--extra mlflow` to CI install commands in `.github/workflows/` for type checking and tests that exercise VAEP/DEFCON `@Champion` loading paths
- MLflow imports in `src/ingestion/spadl_vaep.py` and `src/ingestion/defcon_lite.py` guarded by `TYPE_CHECKING` for pyright, with runtime `try/except ImportError` fallback to UC Volume path loading (Databricks has mlflow pre-installed; CI needs the extra)
- Add `import_obso_results` entry point if implemented as `src/ingestion/` module

### Terraform

New workflow tasks in `terraform/modules/workflows/main.tf`:
- `ingest_idsse_events` — depends on IDSSE data upload to UC Volume
- `compute_elastic_sync` — depends on `ingest_idsse_events`
- `compute_pausa` — depends on `compute_elastic_sync` + OBSO import
- `run_model_validation` — depends on `dbt_build` + `compute_pausa`

Library configuration: tasks that load `@Champion` models (VAEP, DEFCON, xG) need `mlflow` available — Databricks serverless includes it in the runtime, so no wheel addition needed. The `[mlflow]` extra is for local dev and CI only.

### HF Jobs Script Access to `src/analytics`

HF Jobs scripts are standalone (PEP 723). They cannot `pip install` the project wheel from a private repo. Two options:
- **Inline critical functions**: Copy `compute_pitch_control_grid_fast()`, `generate_ghost_trajectories()`, `compute_obso_surface()` into the HF Jobs script. ~200 lines. Matches existing `compute_xt_grid_hf.py` pattern (which inlines xT computation).
- **Publish analytics wheel to HF Hub**: Too complex for this branch.

Recommendation: inline, matching established pattern.

### Benchmark Tests

Per CLAUDE.md, critical-path functions need `pytest-benchmark` tests:
- `compute_obso_surface()` — benchmark with a 104×68 grid (target: ≤ 5ms, similar to pitch control baseline)
- `perturb_positions()` — benchmark with 22-player frame, 10 perturbations (target: ≤ 1ms)
- ELASTIC frame matching — benchmark per-match alignment (target: ≤ 1s for ~500 events)

### IDSSE Event Data Availability

**Clarification:** TODO.md tech debt #6 says "IDSSE ... have tracking but no event data" — this means *not ingested*, not non-existent. The IDSSE acronym stands for "Integrated Dataset of Spatiotemporal and **Event** data" (Bassek et al., Scientific Data, Nature 2025). DFL event XML files (`DFL_03_02_*` series) exist in the figshare collection (CC-BY 4.0). D9/Sub-task A downloads and ingests them. Zero procurement risk. Tech debt #6 will be updated to distinguish IDSSE (events exist, not ingested) from SkillCorner (events genuinely absent).

### ROADMAP.md Updates

Resolve open questions:
- Q2 (Numba): No — JAX kernel extended with ghost trajectories
- Q3 (Coordinates): StatsBomb 120×80 at boundary, meters internally
- Q4 (Static grids): PAUSA repo as-is, custom training deferred
- Q5 (Scope): Full PAUSA pipeline

### TODO.md Updates

- Move D9, D10, D11, D12, D13, D16 to Completed
- Add deferred: NannyML CBPE, custom EPV/Transition grids, Numba evaluation

### Deployment Checklist

1. Push branch, verify CI green
2. Deploy wheel to Databricks
3. Run `ingest_idsse_events` (populate `idsse_events` bronze)
4. Run `compute_elastic_sync` (populate `elastic_sync_results`)
5. Run D16 HF Jobs script (`hf jobs uv run scripts/compute_obso_hf.py --flavor a10g`)
6. Import OBSO results to Delta (`scripts/import_obso_results.py` or notebook — reads Parquet from UC Volume staging path, writes to `obso_surfaces` + `pausa_raw_scores` bronze tables)
7. Run `compute_pausa` (populate `fct_pausa_values`)
8. Run `dbt build` (new staging/intermediate/mart models)
9. Run VAEP/DEFCON training notebooks (one-time `@Champion` registration)
10. Run `run_model_validation` (verify all baselines "ok")
11. Create synced tables in Databricks UI (`fct_pausa_values_synced`, `fct_pass_timing_synced`)
12. Terraform import synced tables
13. Run `create_indexes.py` + PG grants
14. Deploy Streamlit app
15. E2E test Pass Timing page (Streamlit)
16. Export `sample_pausa.parquet` for HF Space
17. Deploy updated Space to HF Hub
18. E2E test Pass Timing tab (Gradio)

## 12. Dependency Graph

```
D11 (MLflow Registry)          D13 (Augmentation)
         │                              │
         │ (template proven)            │ (standalone)
         ▼                              │
D9 (ELASTIC Sync)                       │
         │                              │
         │ (event-frame mapping)        │
         ▼                              │
D16 (OBSO on HF Jobs GPU)              │
         │                              │
         │ (OBSO surfaces)              │
         ▼                              │
D10 (PAUSA Pipeline + Streamlit)        │
         │                              │
         │ (all models + outputs)       │
         ▼                              │
D12 (Model Validation) ◄───────────────┘
```

## 13. Deferred Items (tracked in TODO.md)

| Item | Rationale | When to Revisit |
|------|-----------|-----------------|
| NannyML CBPE | Zero-dep scipy approach sufficient for current 10 models | When adding neural models (D17/D18) or when ground truth is unavailable |
| Custom EPV/Transition grids | PAUSA repo grids validated by paper | When expanding OBSO to >7 matches or drift detected |
| Numba evaluation | JAX covers all current use cases including GPU | If JAX compile times become problematic for small workloads |
