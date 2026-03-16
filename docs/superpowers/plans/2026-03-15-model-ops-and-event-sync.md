# Model Ops & Event Sync Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the MLOps foundation (MLflow registry, model validation, tracking augmentation) and the full PAUSA optimal pass timing pipeline (ELASTIC sync, OBSO computation, ghost trajectories, Streamlit + HF Space pages).

**Architecture:** Infrastructure-first approach — D11 (MLflow) and D13 (augmentation) establish reusable patterns, then D9→D16→D10 builds the analytics pipeline on those rails, D12 validates everything, and deployment exercises the full stack including HF Jobs GPU compute.

**Tech Stack:** MLflow 3 + UC Model Registry, JAX (CPU+GPU), scipy.stats, NumPy, PySpark `applyInPandas`, dbt, Streamlit, Gradio, HF Jobs (PEP 723)

**Spec:** `docs/superpowers/specs/2026-03-15-model-ops-and-event-sync-design.md`

**Branch:** `feature/model-ops-and-event-sync`

**Commit strategy:** Single commit of fully E2E-tested code. Additional commits only with explicit user approval.

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `src/analytics/augmentation.py` | Pure NumPy position jitter within physical constraints |
| `src/analytics/elastic_sync.py` | Pure compute: ball acceleration features, frame matching algorithm |
| `src/analytics/obso.py` | OBSO surface computation: PPCF × Transition × EPV |
| `src/analytics/model_validation.py` | Pure scipy/numpy validation functions (PSI, Wasserstein, CUSUM) |
| `src/ingestion/elastic_sync.py` | Spark pipeline: ELASTIC sync via `applyInPandas`, writes to Delta |
| `src/ingestion/pausa.py` | Spark pipeline: PAUSA scoring, writes `fct_pausa_values` to Delta |
| `src/ingestion/model_validation.py` | Spark pipeline: reads gold tables, runs validation, writes results |
| `src/tests/test_augmentation.py` | Unit + property + benchmark tests for augmentation |
| `src/tests/test_elastic_sync.py` | Unit + integration tests for ELASTIC sync |
| `src/tests/test_obso.py` | Unit + benchmark tests for OBSO surface |
| `src/tests/test_pausa.py` | Unit tests for PAUSA scoring pipeline |
| `src/tests/test_model_validation.py` | Unit + property tests for validation functions |
| `scripts/train_xg_model_hf.py` | HF Jobs CPU template: xG training with MLflow logging |
| `scripts/compute_obso_hf.py` | HF Jobs GPU script: OBSO batch on A10G |
| `scripts/import_obso_results.py` | Import OBSO Parquet from UC Volume to Delta bronze (standalone script, no entry point — run as notebook or `spark-submit`) |
| `src/streamlit_app/pages/pass_timing.py` | Pass Timing Streamlit page |
| `dbt_project/models/staging/idsse/stg_idsse__events.sql` | IDSSE event staging (coord transform) |
| `dbt_project/models/staging/idsse/stg_idsse__elastic_sync.sql` | ELASTIC sync staging |
| `dbt_project/models/staging/pausa/stg_pausa__values.sql` | PAUSA values staging |
| `dbt_project/models/staging/pausa/_pausa__sources.yml` | PAUSA source definitions |
| `dbt_project/models/staging/pausa/_pausa__models.yml` | PAUSA staging model docs |
| `dbt_project/models/staging/idsse/stg_idsse__elastic_sync.sql` | ELASTIC sync staging (joins events with aligned frames) |
| `dbt_project/models/intermediate/int_pausa__pass_quality.sql` | Ephemeral CTE: PAUSA + player names |
| `dbt_project/models/marts/fct_pass_timing.sql` | Mart: per-player per-match PAUSA aggregation |
| `dbt_project/seeds/model_baseline_scalars.csv` | Reference baselines for D12 |
| `demo_space/data/sample_pausa.parquet` | Pre-cached PAUSA data for HF Space |

### Modified Files

| File | Change |
|------|--------|
| `pyproject.toml` | Add `mlflow` extra, new entry points |
| `src/analytics/pitch_control.py` | Add `generate_ghost_trajectories()` |
| `src/ingestion/idsse.py` | Add `main_events()` for DFL event XML parsing |
| `src/ingestion/spadl_vaep.py` | Load VAEP models from `@Champion` instead of retraining |
| `src/ingestion/defcon_lite.py` | Load DEFCON models from `@Champion` instead of retraining |
| `src/ingestion/xg_model.py` | Load xG model via MLflow URI |
| `notebooks/train_xg_model.py` | Add MLflow logging + model registration |
| `notebooks/train_football2vec.py` | Add MLflow logging + model registration |
| `src/streamlit_app/app.py` | Add Pass Timing page to navigation |
| `src/streamlit_app/components/glossary.py` | Add PAUSA/OBSO glossary entries |
| `src/tests/test_benchmarks.py` | Add OBSO + augmentation benchmarks |
| `src/tests/test_idsse.py` | Add event ingestion tests |
| `src/tests/test_pitch_control_model.py` | Add ghost trajectory tests |
| `demo_space/app.py` | Add Pass Timing Gradio tab |
| `dbt_project/dbt_project.yml` | Add `pausa_enabled` toggle |
| `dbt_project/models/staging/idsse/_idsse__sources.yml` | Add `idsse_events` + `elastic_sync_results` sources |
| `dbt_project/models/staging/idsse/_idsse__models.yml` | Add event + elastic sync model docs |
| `dbt_project/models/marts/_marts__models.yml` | Add `fct_pausa_values` + `fct_pass_timing` contracts |
| `dbt_project/seeds/_seeds__schema.yml` | Add `model_baseline_scalars` schema |
| `terraform/modules/workflows/main.tf` | Add pipeline tasks |
| `.github/workflows/python-ci.yml` | Add `--extra mlflow` to install |
| `NOTICE` | Add ELASTIC, PAUSA, OBSO citations |
| `ROADMAP.md` | Resolve open questions Q2–Q5 |
| `TODO.md` | Move D9/D10/D11/D12/D13/D16 to completed, add deferred items |
| `scripts/create_indexes.py` | Add PAUSA table indexes |

---

## Chunk 1: D11 — MLflow Registry + HF Jobs Template

### Task 1.1: Add MLflow dependency and entry points

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `mlflow` optional dependency group**

In `pyproject.toml`, add to `[project.optional-dependencies]`:

```toml
mlflow = [
    "mlflow>=2.17.0",
]
```

- [ ] **Step 2: Add new entry points**

Add to `[project.scripts]`:

```toml
ingest_idsse_events = "ingestion.idsse:main_events"
compute_elastic_sync = "ingestion.elastic_sync:main"
compute_pausa = "ingestion.pausa:main"
run_model_validation = "ingestion.model_validation:main"
```

- [ ] **Step 3: Add `--extra mlflow` to CI**

Modify `.github/workflows/python-ci.yml` — add `mlflow` to the install extras so pyright and tests can resolve MLflow imports. The MLflow import in `src/ingestion/` files uses `TYPE_CHECKING` guard for pyright, with runtime `try/except ImportError` fallback.

- [ ] **Step 4: Sync local environment**

Run: `uv sync --extra analytics --extra dev --extra mlflow --extra jax --extra embeddings`

Expected: Clean install, mlflow available.

### Task 1.2: Add MLflow logging to xG training notebook

**Files:**
- Modify: `notebooks/train_xg_model.py`

- [ ] **Step 1: Read the existing notebook**

Read `notebooks/train_xg_model.py` fully. Identify:
- Where `XGBClassifier` and `LogisticRegression` are trained
- Where `metrics.json` is saved
- Where UC Volume paths are used
- Where HF Hub upload happens

- [ ] **Step 2: Add MLflow experiment tracking**

After the training section, add:
- `import mlflow` + `from mlflow.tracking import MlflowClient`
- `mlflow.set_experiment("/soccer_analytics/xg_model")`
- Wrap training in `with mlflow.start_run(run_name="xg_model_v1"):`
- `mlflow.log_params()` for all training hyperparameters
- `mlflow.log_metrics()` for brier_score, log_loss, roc_auc, calibration_error
- `mlflow.xgboost.log_model()` for the XGBoost model
- `mlflow.sklearn.log_model()` for the logistic model
- Register model: `mlflow.register_model(model_uri, "soccer_analytics.dev_gold.xg_model")`
- Set alias: `client.set_registered_model_alias("soccer_analytics.dev_gold.xg_model", "Champion", version)`
- Set alias: `client.set_registered_model_alias("soccer_analytics.dev_gold.xg_model", "Baseline", logistic_version)`

- [ ] **Step 3: Keep existing UC Volume + HF Hub paths**

MLflow registration is additive — existing `serialize_*` → UC Volume → HF Hub paths remain for backward compatibility. The `@Champion` alias is a new, parallel access path.

### Task 1.3: Add MLflow logging to Football2Vec training notebook

**Files:**
- Modify: `notebooks/train_football2vec.py`

- [ ] **Step 1: Read the existing notebook**

Read `notebooks/train_football2vec.py` fully. Identify the `Football2VecModel` pyfunc stub in `src/analytics/football2vec.py`.

- [ ] **Step 2: Add MLflow experiment tracking**

- `mlflow.set_experiment("/soccer_analytics/football2vec")`
- Wrap training in `mlflow.start_run()`
- `mlflow.log_params()` for Doc2Vec hyperparameters (vector_size, window, min_count, epochs)
- `mlflow.log_metrics()` for corpus size, vocabulary size
- `mlflow.pyfunc.log_model()` using the existing `Football2VecModel` class
- Register: `soccer_analytics.dev_gold.football2vec` with `@Champion` alias

### Task 1.4: Create HF Jobs xG training template

**Files:**
- Create: `scripts/train_xg_model_hf.py`

- [ ] **Step 1: Read the existing HF Jobs template**

Read `scripts/compute_xt_grid_hf.py` for the PEP 723 pattern, HF Hub data download, and result upload.

- [ ] **Step 2: Write the HF Jobs xG training script**

Create `scripts/train_xg_model_hf.py` with PEP 723 inline metadata:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy>=1.26.0",
#     "pandas>=2.0.0",
#     "pyarrow>=14.0.0",
#     "scikit-learn>=1.3.0",
#     "xgboost==3.2.0",
#     "huggingface-hub>=0.25.0",
#     "mlflow>=2.17.0",
# ]
# ///
```

Flow:
1. Download shots data from `luxury-lakehouse/spadl-vaep-action-values` via `hf_hub_download`
2. Train logistic + XGBoost models (same code as notebook, extracted)
3. Log to MLflow via `MLFLOW_TRACKING_URI` env var (points to Databricks workspace)
4. Push weights to HF Hub `luxury-lakehouse/xg-model-statsbomb-wyscout`
5. Print summary metrics

- [ ] **Step 3: Test locally (CPU)**

Run: `uv run scripts/train_xg_model_hf.py` (local test, no HF Jobs)

Verify: Script completes, metrics printed, HF Hub upload succeeds.

### Task 1.5: Modify VAEP pipeline to load @Champion models

**Files:**
- Modify: `src/ingestion/spadl_vaep.py`
- Modify: `src/tests/test_spadl_vaep.py`

- [ ] **Step 1: Read `src/ingestion/spadl_vaep.py`**

Find `_load_or_train_models()` (around line 570). Understand the current retrain-every-run pattern and how model bytes are distributed to executors via closure.

- [ ] **Step 2: Write failing test for Champion model loading**

In `src/tests/test_spadl_vaep.py`, add a test that verifies:
- When `@Champion` model exists (mocked), it's loaded instead of retrained
- The loaded model bytes match the expected format for `applyInPandas` closure

- [ ] **Step 3: Modify `_load_or_train_models()`**

Change to:
1. Try loading from MLflow `@Champion` URI on driver
2. If `@Champion` not found (first run, or MLflow not available), fall back to current retrain behavior
3. Serialize loaded model to bytes for executor distribution (same as current pattern)

Use `TYPE_CHECKING` guard for mlflow import:
```python
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import mlflow
```

Runtime: `try: import mlflow` with fallback.

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_spadl_vaep.py -v`

Expected: All existing tests pass + new Champion loading test passes.

### Task 1.6: Modify DEFCON pipeline to load @Champion models

**Files:**
- Modify: `src/ingestion/defcon_lite.py`
- Modify: `src/tests/test_defcon_lite.py`

- [ ] **Step 1: Apply same pattern as Task 1.5**

Read `src/ingestion/defcon_lite.py`, find the in-pipeline `model.fit()` call. Add Champion loading with fallback, same `TYPE_CHECKING` guard pattern.

- [ ] **Step 2: Write failing test + implement + verify**

Same structure as VAEP: test Champion loading, implement with fallback, verify all tests pass.

Run: `uv run pytest src/tests/test_defcon_lite.py -v`

### Task 1.7: Modify xG inference pipeline to use MLflow URI

**Files:**
- Modify: `src/ingestion/xg_model.py`
- Modify: `src/tests/test_xg_model.py`

- [ ] **Step 1: Read `src/ingestion/xg_model.py`**

Find the UC Volume path loading (around line 127–129). Change to MLflow URI with UC Volume fallback.

- [ ] **Step 2: Write failing test + implement + verify**

Test: verify MLflow URI loading is attempted first, falls back to UC Volume.

Run: `uv run pytest src/tests/test_xg_model.py -v`

### Task 1.8: Verify all D11 tests pass

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest src/tests/ -v --ignore=src/tests/test_football2vec.py`

(Ignore gensim-dependent tests locally per MEMORY.md — CI runs all.)

Expected: All tests pass, zero regressions.

- [ ] **Step 2: Run lint + typecheck**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/`

Expected: Zero violations.

---

## Chunk 2: D13 — Physics-Based Tracking Augmentation

### Task 2.1: Write augmentation tests (TDD)

**Files:**
- Create: `src/tests/test_augmentation.py`

- [ ] **Step 1: Write unit tests**

```python
"""Tests for physics-based tracking augmentation."""
from analytics.augmentation import PerturbationConfig, perturb_positions, augment_full
from analytics.symmetry import AugmentationConfig
import numpy as np
import pandas as pd

class TestPerturbPositions:
    """Test position jitter within physical constraints."""

    def _make_frame(self, n_players=22):
        """Synthetic single-frame tracking data in meter-space."""
        rng = np.random.default_rng(42)
        return pd.DataFrame({
            "player_id": [f"p{i}" for i in range(n_players)],
            "x": rng.uniform(-52.5, 52.5, n_players),
            "y": rng.uniform(-34, 34, n_players),
            "velocity_x": rng.uniform(-5, 5, n_players),
            "velocity_y": rng.uniform(-5, 5, n_players),
            "team": ["home"] * (n_players // 2) + ["away"] * (n_players // 2),
        })

    def test_output_count(self):
        """10 perturbations requested → 10 DataFrames returned."""
        ...

    def test_positions_within_pitch_bounds(self):
        """All perturbed positions within [-52.5, 52.5] x [-34, 34]."""
        ...

    def test_speed_within_physical_limit(self):
        """No perturbed speed exceeds max_speed_ms."""
        ...

    def test_reproducibility(self):
        """Same RNG seed → identical output."""
        ...

    def test_augmentation_column_present(self):
        """Each DataFrame has 'augmentation' and 'jitter_seed' columns."""
        ...

class TestAugmentFull:
    """Test composition of symmetry + jitter."""

    def test_total_count(self):
        """8 symmetry × (1 + 10 jitter) = 88 DataFrames."""
        ...

    def test_coordinate_consistency(self):
        """Output is in StatsBomb 120x80 coordinates (same as input)."""
        ...
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_augmentation.py -v`

Expected: ImportError — `analytics.augmentation` does not exist yet.

### Task 2.2: Implement augmentation module

**Files:**
- Create: `src/analytics/augmentation.py`

- [ ] **Step 1: Implement `PerturbationConfig` and `perturb_positions()`**

See spec Section 4 for the full algorithm:
1. Gaussian noise on positions in meter-space
2. Clamp to pitch bounds
3. Re-derive velocities from position deltas
4. Clamp speed to max_speed_ms
5. Return list of DataFrames with augmentation/jitter_seed columns

- [ ] **Step 2: Implement `augment_full()`**

Composes `augment_tracking_frame()` from `symmetry.py` with `perturb_positions()`. Handles coordinate conversion (StatsBomb ↔ meters) internally.

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_augmentation.py -v`

Expected: All pass.

### Task 2.3: Add benchmark test

**Files:**
- Modify: `src/tests/test_benchmarks.py`

- [ ] **Step 1: Add augmentation benchmark**

```python
def test_perturb_positions_benchmark(benchmark):
    """Position jitter: ≤1ms per frame for 10 perturbations."""
    ...
```

- [ ] **Step 2: Run benchmarks**

Run: `uv run pytest src/tests/test_benchmarks.py -v --benchmark-only`

Expected: ≤1ms per frame.

### Task 2.4: Lint and typecheck

- [ ] **Step 1: Verify**

Run: `uv run ruff check src/analytics/augmentation.py && uv run pyright src/analytics/augmentation.py`

Expected: Zero violations.

---

## Chunk 3: D9 — ELASTIC Event-Tracking Sync

### Task 3.1: Add IDSSE event ingestion

**Files:**
- Modify: `src/ingestion/idsse.py`
- Modify: `src/tests/test_idsse.py`

- [ ] **Step 1: Read existing IDSSE ingestion code**

Read `src/ingestion/idsse.py` fully. Understand:
- How `DFL_04_03_positions_raw_observed_*` XML is parsed
- How `DFL_02_01_matchinformation_*` XML is parsed for player→team mapping
- The UC Volume path structure
- The structured logging pattern

- [ ] **Step 2: Download DFL event XML files from figshare**

The IDSSE figshare collection includes `DFL_03_02_eventdata_*` XML files. Download all 7 matches' event files and upload to UC Volume at the same path as existing XMLs.

Run (Databricks notebook or CLI):
```python
# Download from figshare, upload to /Volumes/soccer_analytics/bronze/libs/idsse_data/
```

- [ ] **Step 3: Write failing tests for event ingestion**

In `src/tests/test_idsse.py`, add tests for `main_events()`:
- Parses DFL event XML correctly
- Extracts event_id, event_type, timestamp_seconds, player_id, team, x, y
- Coordinates are in DFL center-origin meters (raw bronze)
- `_ingested_at` audit column present
- Incremental skip guard works

- [ ] **Step 4: Implement `main_events()` in `idsse.py`**

New function following the existing `main()` pattern:
- Parse `DFL_03_02_eventdata_*` XML files from UC Volume
- Extract event attributes per the DFL schema
- Write to `idsse_events` bronze table with `replaceWhere` on `match_id`
- Structured JSON logging

- [ ] **Step 5: Run tests**

Run: `uv run pytest src/tests/test_idsse.py -v`

Expected: All pass (existing + new).

### Task 3.2: Add IDSSE event dbt staging model

**Files:**
- Modify: `dbt_project/models/staging/idsse/_idsse__sources.yml`
- Modify: `dbt_project/models/staging/idsse/_idsse__models.yml`
- Create: `dbt_project/models/staging/idsse/stg_idsse__events.sql`

- [ ] **Step 1: Read existing `stg_idsse__tracking.sql`**

Understand the coordinate transform pattern (center-origin meters → StatsBomb 120×80).

- [ ] **Step 2: Add `idsse_events` and `elastic_sync_results` source definitions**

In `_idsse__sources.yml`, add both `idsse_events` and `elastic_sync_results` tables under the existing `idsse` source. Both are bronze tables written by Python pipelines.

- [ ] **Step 3: Create `stg_idsse__events.sql`**

Staging view that:
- Selects from `{{ source('idsse', 'idsse_events') }}`
- Transforms coordinates: `(x + 52.5) * (120.0 / 105.0)` for x, `(y + 34.0) * (80.0 / 68.0)` for y
- Same transform as `stg_idsse__tracking.sql`
- Enabled by `pausa_enabled` toggle

- [ ] **Step 4: Add model docs in `_idsse__models.yml`**

Add column descriptions and data tests for both `stg_idsse__events` and `stg_idsse__elastic_sync` models.

### Task 3.3: Write ELASTIC sync analytics module

**Files:**
- Create: `src/analytics/elastic_sync.py`
- Create: `src/tests/test_elastic_sync.py`

- [ ] **Step 1: Read the PAUSA repo's `elastic/` code**

Fetch from `https://github.com/leemingo/mitssac-pausa`. Understand:
- Input format expectations
- Feature extraction (ball acceleration, player-ball distance)
- Frame matching algorithm
- Output format

- [ ] **Step 2: Write failing tests**

Test with synthetic data:
- Known event at frame 100 → algorithm should find frame 100
- Ball acceleration spike at event moment → correct detection
- Output has correct schema (event_id, frame_id, alignment_confidence)
- Edge cases: event at start/end of period, missing ball data

- [ ] **Step 3: Implement `elastic_sync.py`**

Pure compute module — no Spark dependency:
- `extract_ball_features(tracking_df)` → acceleration, player-ball distances
- `align_event_to_frame(event, features, tracking_df)` → (frame_id, confidence)
- `sync_match(events_df, tracking_df)` → DataFrame of aligned events

Adapt from PAUSA repo, converting to StatsBomb 120×80 coordinate assumptions.

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_elastic_sync.py -v`

Expected: All pass.

### Task 3.4: Write ELASTIC sync ingestion pipeline

**Files:**
- Create: `src/ingestion/elastic_sync.py`

- [ ] **Step 1: Implement Spark pipeline**

Following established `applyInPandas` pattern:
- Read `stg_idsse__events` and `stg_idsse__tracking` from Delta
- Group by `match_id`, apply `sync_match()` via `applyInPandas`
- Write to `elastic_sync_results` bronze table with `replaceWhere`
- Incremental skip guard on `match_id`
- Structured JSON logging

- [ ] **Step 2: Add `stg_idsse__elastic_sync.sql` dbt staging model**

Create: `dbt_project/models/staging/idsse/stg_idsse__elastic_sync.sql`

Staging view selecting from `{{ source('idsse', 'elastic_sync_results') }}`, joining with events and tracking frames. Enabled by `pausa_enabled` toggle. Add model definition (column descriptions + data tests) to `_idsse__models.yml` (already prepped in Task 3.2 Step 4).

### Task 3.5: Lint and typecheck D9

- [ ] **Step 1: Verify**

Run: `uv run ruff check src/ && uv run pyright src/`

Expected: Zero violations.

---

## Chunk 4: D16 — OBSO Batch on HF Jobs GPU

### Task 4.1: Add ghost trajectory generation to pitch control

**Files:**
- Modify: `src/analytics/pitch_control.py`
- Modify: `src/tests/test_pitch_control_model.py`

- [ ] **Step 1: Read existing pitch control code**

Read `src/analytics/pitch_control.py` fully. Understand:
- `compute_pitch_control_grid_fast()` — the JAX-accelerated grid function
- `PitchControlParams` frozen dataclass
- The dual NumPy/JAX dispatch pattern
- Meter-space conversion helpers

- [ ] **Step 2: Write failing tests for ghost trajectories**

In `src/tests/test_pitch_control_model.py`, add:
- `test_ghost_trajectory_count()` — 3s before + 1s after at 25fps = 100 frames
- `test_ghost_trajectory_constant_velocity()` — positions extrapolate linearly
- `test_ghost_trajectory_stationary_player()` — zero velocity → same position repeated
- `test_ghost_trajectory_pitch_bounds()` — positions clamped to pitch

- [ ] **Step 3: Implement `generate_ghost_trajectories()`**

Add to `src/analytics/pitch_control.py`:

```python
def generate_ghost_trajectories(
    players_df: pd.DataFrame,
    event_frame: int,
    frame_rate: int = 25,
    window_before_s: float = 3.0,
    window_after_s: float = 1.0,
) -> list[pd.DataFrame]:
```

Constant-velocity extrapolation from each player's position/velocity at `event_frame`. Clamp to pitch bounds. Return one DataFrame per ghost frame.

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_pitch_control_model.py -v`

Expected: All pass.

### Task 4.2: Create OBSO analytics module

**Files:**
- Create: `src/analytics/obso.py`
- Create: `src/tests/test_obso.py`

- [ ] **Step 1: Write failing tests**

```python
class TestComputeObsoSurface:
    def test_obso_shape_matches_ppcf(self):
        """OBSO grid has same shape as PPCF grid."""
        ...

    def test_obso_values_bounded(self):
        """OBSO values in [0, 1] (product of probabilities)."""
        ...

    def test_obso_zero_when_no_control(self):
        """OBSO = 0 where PPCF = 0 (opponent controls)."""
        ...

    def test_obso_with_known_grids(self):
        """Verify against hand-computed values with small grids."""
        ...

class TestComputePassObso:
    def test_actual_obso_at_release(self):
        """Actual OBSO is the surface value at release frame + target location."""
        ...

    def test_peak_obso_across_window(self):
        """Peak OBSO is the max across all ghost frames at target location."""
        ...

    def test_optimal_obso_across_receivers(self):
        """Optimal OBSO is the max across all off-ball teammates at release frame."""
        ...
```

- [ ] **Step 2: Implement `obso.py`**

```python
def compute_obso_surface(ppcf_grid, transition_grid, epv_grid, ball_position) -> np.ndarray:
    """OBSO = PPCF × Transition(ball→cell) × EPV(cell)."""

def compute_pass_obso(
    tracking_frames: list[pd.DataFrame],   # ghost trajectory frames
    event_frame_idx: int,
    target_position: tuple[float, float],
    teammate_positions: np.ndarray,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    params: PitchControlParams,
) -> dict[str, float]:
    """Returns actual_obso, peak_obso, optimal_obso for one pass."""
```

Handles grid interpolation (resize static grids to match PPCF grid dimensions).

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_obso.py -v`

Expected: All pass.

### Task 4.3: Add OBSO benchmark

**Files:**
- Modify: `src/tests/test_benchmarks.py`

- [ ] **Step 1: Add benchmark**

```python
def test_obso_surface_benchmark(benchmark):
    """OBSO surface computation: ≤5ms for 104×68 grid."""
    ...
```

- [ ] **Step 2: Run benchmarks**

Run: `uv run pytest src/tests/test_benchmarks.py::test_obso_surface_benchmark -v --benchmark-only`

### Task 4.4: Create HF Jobs OBSO GPU script

**Files:**
- Create: `scripts/compute_obso_hf.py`

- [ ] **Step 1: Write the HF Jobs script**

PEP 723 inline metadata with `jax[cuda12]`:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "jax[cuda12]>=0.4.35",
#     "numpy>=1.26.0",
#     "pandas>=2.0.0",
#     "pyarrow>=14.0.0",
#     "huggingface-hub>=0.25.0",
#     "mlflow>=2.17.0",
# ]
# ///
```

Flow:
1. Download tracking data + ELASTIC sync results + static grids from HF Hub
2. Inline critical functions from `src/analytics/` (pitch control, ghost trajectories, OBSO) — matches existing `compute_xt_grid_hf.py` pattern
3. For each match → for each pass → generate ghost trajectories → compute OBSO → record scores
4. Log metrics to MLflow remote tracking
5. Save results as Parquet → upload to HF Hub + write to UC Volume staging path

- [ ] **Step 2: Test locally (CPU fallback)**

Run: `uv run scripts/compute_obso_hf.py` (with `jax[cpu]` locally — GPU only on HF Jobs)

Verify: Script runs, produces Parquet output. JAX auto-detects CPU.

### Task 4.5: Create OBSO import script

**Files:**
- Create: `scripts/import_obso_results.py`

- [ ] **Step 1: Write import script**

Reads OBSO Parquet from UC Volume staging path, writes to `obso_surfaces` and `pausa_raw_scores` bronze Delta tables via PySpark.

### Task 4.6: Lint and typecheck D16

- [ ] **Step 1: Verify**

Run: `uv run ruff check src/ && uv run pyright src/`

Expected: Zero violations.

---

## Chunk 5: D10 — PAUSA Pipeline + Streamlit + HF Space

### Task 5.1: Write PAUSA scoring tests

**Files:**
- Create: `src/tests/test_pausa.py`

- [ ] **Step 1: Write unit tests**

```python
class TestPausaScoring:
    def test_temporal_judgment_perfect(self):
        """actual_obso == peak_obso → temporal_judgment = 1.0."""
        ...

    def test_spatial_selection_perfect(self):
        """actual_obso == optimal_obso → spatial_selection = 1.0."""
        ...

    def test_pausa_composite(self):
        """pausa = temporal_judgment × spatial_selection."""
        ...

    def test_values_bounded_zero_one(self):
        """All scores in [0, 1]."""
        ...

    def test_zero_peak_obso_handled(self):
        """peak_obso = 0 → temporal_judgment = 0 (not divide-by-zero)."""
        ...
```

### Task 5.2: Implement PAUSA ingestion pipeline

**Files:**
- Create: `src/ingestion/pausa.py`

- [ ] **Step 1: Implement `main()`**

Following established `applyInPandas` pattern:
- CLI via `build_cli()` from `utils.py`
- Read `pausa_raw_scores` + `elastic_sync_results` + `stg_idsse__events`
- Compute temporal_judgment, spatial_selection, pausa_score
- Write to `fct_pausa_values` with `replaceWhere` on `match_id`
- Incremental skip guard
- Structured JSON logging

- [ ] **Step 2: Run tests**

Run: `uv run pytest src/tests/test_pausa.py -v`

### Task 5.3: Create dbt models for PAUSA

**Files:**
- Create: `dbt_project/models/staging/pausa/_pausa__sources.yml`
- Create: `dbt_project/models/staging/pausa/_pausa__models.yml`
- Create: `dbt_project/models/staging/pausa/stg_pausa__values.sql`
- Create: `dbt_project/models/intermediate/int_pausa__pass_quality.sql`
- Create: `dbt_project/models/marts/fct_pass_timing.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`
- Modify: `dbt_project/dbt_project.yml`

- [ ] **Step 1: Add `pausa_enabled` toggle**

In `dbt_project.yml` vars:
```yaml
pausa_enabled: true
```

- [ ] **Step 2: Create source and staging models**

`_pausa__sources.yml` — define `pausa_raw_scores`, `fct_pausa_values` sources.
`stg_pausa__values.sql` — staging view with `{{ config(enabled=var('pausa_enabled', false)) }}`.

- [ ] **Step 3: Create intermediate model**

`int_pausa__pass_quality.sql` — ephemeral CTE joining PAUSA values with `dim_players` and `dim_teams` for human-readable names.

- [ ] **Step 4: Create mart model**

`fct_pass_timing.sql` — aggregates per player per match: avg/median PAUSA, temporal, spatial, pass count. `contract: {enforced: true}` with all column types.

- [ ] **Step 5: Add contracts to `_marts__models.yml`**

Add `fct_pausa_values` and `fct_pass_timing` column contracts with `data_type` on every column. Add `dbt_expectations` range tests (temporal_judgment ∈ [0,1], etc.).

### Task 5.4: Add glossary entries and NOTICE citations

**Files:**
- Modify: `src/streamlit_app/components/glossary.py`
- Modify: `NOTICE`

- [ ] **Step 1: Add PAUSA/OBSO glossary entries**

Add to `METRIC_HELP` and `GLOSSARY` dicts — see spec Section 9 for exact text.

- [ ] **Step 2: Add citations to NOTICE**

Add ELASTIC (Kim et al. 2025), PAUSA (Lee et al. 2026), OBSO (Spearman 2018, Fernandez & Bornn 2018), TacticAI augmentation (Wang et al. 2024).

### Task 5.5: Create Pass Timing Streamlit page

**Files:**
- Create: `src/streamlit_app/pages/pass_timing.py`
- Modify: `src/streamlit_app/app.py`

- [ ] **Step 1: Read an existing tracking-dependent page for pattern**

Read `src/streamlit_app/pages/pitch_control.py` as the template — it queries tracking data, uses pitch visualization, has metric cards with `help=`.

- [ ] **Step 2: Create `pass_timing.py`**

Following spec Section 9 layout:
- Filter bar: competition → match → team → player (session state persistence)
- 3 × `st.metric` with `help=`: Avg PAUSA, Avg Temporal Judgment, Avg Spatial Selection
- Two-column visualization: OBSO heatmap (left), scatter plot (right)
- Rankings `st.dataframe` with `LIMIT 500`
- Academic citation footer + data scope label
- `empty_select()` / `empty_result()` for empty states
- All queries against `fct_pass_timing_synced` and `fct_pausa_values_synced` via `execute_query()`

- [ ] **Step 3: Register page in `app.py`**

Add `st.Page("pages/pass_timing.py", title="Pass Timing", url_path="pass-timing")` after Pitch Control in navigation.

- [ ] **Step 4: Run Streamlit component tests**

Run: `uv run pytest src/tests/test_streamlit_components.py -v`

### Task 5.6: Add Pass Timing tab to HF Space

**Files:**
- Modify: `demo_space/app.py`

- [ ] **Step 1: Read existing Gradio tabs**

Read `demo_space/app.py` — understand the tab pattern, data loading, Plotly dark theme.

- [ ] **Step 2: Add data loading**

```python
pausa_df = _load_parquet("sample_pausa.parquet")
```

- [ ] **Step 3: Add Pass Timing tab**

New `with gr.Tab("Pass Timing"):` block with:
- `gr.Markdown()` header with academic citations (PAUSA, OBSO, ELASTIC)
- Match/Team/Player `gr.Dropdown`s
- OBSO heatmap via matplotlib + `mplsoccer.Pitch`
- Scatter plot via Plotly (temporal × spatial, size = PAUSA)
- `gr.Dataframe` rankings with column-level interpretation in headers
- `gr.Markdown` legend explaining metrics for first-time users

### Task 5.7: Lint and typecheck D10

- [ ] **Step 1: Verify all new/modified files**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/`

Expected: Zero violations.

---

## Chunk 6: D12 — Model Validation & Drift Detection

### Task 6.1: Write validation function tests (TDD)

**Files:**
- Create: `src/tests/test_model_validation.py`

- [ ] **Step 1: Write unit + property tests**

```python
class TestComputePSI:
    def test_identical_distributions_zero(self):
        """PSI(A, A) = 0."""
        ...
    def test_shifted_distribution_positive(self):
        """Known shifted distribution → PSI > 0."""
        ...
    def test_psi_non_negative(self):
        """PSI is always >= 0."""
        ...

class TestComputeWassersteinDrift:
    def test_identical_zero(self):
        """wasserstein_distance(A, A) = 0."""
        ...
    def test_shifted_positive(self):
        """Shifted distribution → positive distance."""
        ...

class TestComputeCUSUM:
    def test_no_drift_below_threshold(self):
        """Stable process → CUSUM stays below 3σ."""
        ...
    def test_sustained_shift_triggers_alert(self):
        """Mean shift of 2σ → CUSUM crosses threshold."""
        ...

class TestCheckPhysicalBounds:
    def test_within_bounds_ok(self):
        ...
    def test_exceeds_upper_alert(self):
        ...

class TestCheckFieldSumConstraint:
    def test_valid_grid_ok(self):
        """Grid summing to ~1.0 → ok."""
        ...
    def test_invalid_grid_alert(self):
        """Grid summing to 1.3 → alert."""
        ...
```

### Task 6.2: Implement validation analytics module

**Files:**
- Create: `src/analytics/model_validation.py`

- [ ] **Step 1: Implement all validation functions**

See spec Section 8 for function signatures:
- `compute_psi(reference, current, n_bins=10)` → float
- `compute_wasserstein_drift(reference, current)` → float
- `compute_cusum(values, target_mean, sigma)` → tuple[float, str]
- `check_ks_test(reference, current, alpha=0.05)` → tuple[float, float, str]
- `check_physical_bounds(df, col, lower, upper)` → ValidationResult
- `check_field_sum_constraint(ppcf_grid, tolerance=0.05)` → ValidationResult

All pure scipy/numpy. `ValidationResult` frozen dataclass.

- [ ] **Step 2: Run tests**

Run: `uv run pytest src/tests/test_model_validation.py -v`

Expected: All pass.

### Task 6.3: Implement validation pipeline

**Files:**
- Create: `src/ingestion/model_validation.py`

- [ ] **Step 1: Implement `main()`**

Pipeline that:
- Reads gold tables (fct_xg_predictions, fct_action_values, fct_passes, fct_tracking_frames, fct_pausa_values, expected_threat_grids)
- Reads reference baselines from `model_baseline_scalars` dbt seed
- Runs each validation function
- Writes results to `dev_gold.model_validation_runs`
- Logs to MLflow experiments
- Emits structured JSON logs

### Task 6.4: Create baseline dbt seed

**Files:**
- Create: `dbt_project/seeds/model_baseline_scalars.csv`
- Modify: `dbt_project/seeds/_seeds__schema.yml`

- [ ] **Step 1: Create seed CSV**

```csv
model_name,metric_name,reference_value,threshold_warn,threshold_alert,computed_from
xg_xgboost,mean_prediction,0.098,0.04,0.08,statsbomb_la_liga_2015_16
xg_xgboost,roc_auc,0.979,0.95,0.92,held_out_test_set
xg_xgboost,brier_score,0.059,0.08,0.10,held_out_test_set
vaep,negative_action_fraction,0.35,0.05,0.10,statsbomb_2388_matches
line_breaking,detection_rate,0.18,0.05,0.10,statsbomb_2388_matches
physical_stats,max_speed_ms,15.0,,15.0,physics
pitch_control,field_sum,1.0,,0.05,definition
pausa,temporal_judgment_range,1.0,,1.0,definition
pausa,spatial_selection_range,1.0,,1.0,definition
```

- [ ] **Step 2: Add schema definition**

### Task 6.5: Lint, typecheck, and full test suite

- [ ] **Step 1: Run everything**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v --ignore=src/tests/test_football2vec.py`

Expected: All green.

---

## Chunk 7: Cross-Cutting Updates + Terraform + Docs

### Task 7.1: Update Terraform workflow

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Read existing workflow structure**

Read the full file. Understand task dependency chains and `python_wheel_task` format.

- [ ] **Step 2: Add new tasks**

Add `ingest_idsse_events`, `compute_elastic_sync`, `compute_pausa`, `run_model_validation` tasks. Wire dependencies:
- `ingest_idsse_events` depends on IDSSE data availability
- `compute_elastic_sync` depends on `ingest_idsse_events`
- `compute_pausa` depends on `compute_elastic_sync` (+ manual OBSO import step)
- `run_model_validation` depends on `dbt_build` + `compute_pausa`

### Task 7.2: Update indexes script

**Files:**
- Modify: `scripts/create_indexes.py`

- [ ] **Step 1: Add PAUSA indexes**

Add composite `(match_id, player_id)` btree index on `fct_pausa_values_synced` and `fct_pass_timing_synced`.

### Task 7.3: Update ROADMAP.md and TODO.md

**Files:**
- Modify: `ROADMAP.md`
- Modify: `TODO.md`

- [ ] **Step 1: Resolve ROADMAP open questions**

In the PAUSA section:
- Q2: "Resolved — No Numba. JAX kernel extended with ghost trajectory support."
- Q3: "Resolved — StatsBomb 120×80 at API boundary, internal meter conversion."
- Q4: "Resolved — Using PAUSA repo grids as-is. Custom training deferred."
- Q5: "Resolved — Full PAUSA pipeline implemented."

- [ ] **Step 2: Update TODO.md**

Move D9, D10, D11, D12, D13, D16 to Completed section with resolution notes. Add deferred items: NannyML CBPE, custom EPV/Transition grids, Numba evaluation. Update tech debt #6 to distinguish IDSSE (events ingested) from SkillCorner (events absent).

### Task 7.4: Update ARCHITECTURE.md

- [ ] **Step 1: Add new modules to architecture tree**

Add `elastic_sync.py`, `obso.py`, `augmentation.py`, `model_validation.py` to the file tree. Add `fct_pausa_values`, `fct_pass_timing` to the data model section.

---

## Chunk 8: Deploy & E2E Testing

### Task 8.1: Local verification gate

- [ ] **Step 1: Full lint + typecheck + test suite**

Run:
```bash
uv run ruff check src/ && \
uv run ruff format --check src/ && \
uv run pyright src/ && \
uv run pytest src/tests/ -v --ignore=src/tests/test_football2vec.py
```

Expected: All green. Record test count (target: 614+ locally, more with new tests).

### Task 8.2: Deploy to Databricks

- [ ] **Step 1: Sync workspace**

Run:
```bash
databricks sync . /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse --profile OAUTH
```

- [ ] **Step 2: Verify wheel builds**

Confirm the Databricks workspace has the updated wheel with new entry points.

### Task 8.3: Run IDSSE event ingestion (D9)

- [ ] **Step 1: Run pipeline**

Execute `ingest_idsse_events` on Databricks. Verify:
- `idsse_events` bronze table populated with 7 matches of event data
- Row counts logged
- `_ingested_at` column present

### Task 8.4: Run ELASTIC sync (D9)

- [ ] **Step 1: Run pipeline**

Execute `compute_elastic_sync` on Databricks. Verify:
- `elastic_sync_results` populated
- Alignment confidence > 90% for majority of events
- Structured logs show per-match timing

### Task 8.5: Run OBSO batch on HF Jobs GPU (D16)

- [ ] **Step 1: Publish prerequisite data to HF Hub**

Ensure tracking data, ELASTIC sync results, and static grids (EPV, Transition) are available on HF Hub.

- [ ] **Step 2: Run HF Jobs script**

Run:
```bash
hf jobs uv run scripts/compute_obso_hf.py \
    --flavor a10g --timeout 60m \
    --secrets HF_TOKEN=$HF_TOKEN
```

Monitor: Job logs on HF Hub. Expected: completes in 15–30 min.

- [ ] **Step 3: Import results to Delta**

Run `scripts/import_obso_results.py` (or notebook) to write OBSO Parquet to `obso_surfaces` and `pausa_raw_scores` bronze Delta tables.

### Task 8.6: Run PAUSA pipeline (D10)

- [ ] **Step 1: Run pipeline**

Execute `compute_pausa` on Databricks. Verify:
- `fct_pausa_values` populated with ~3,500 passes
- temporal_judgment, spatial_selection all in [0, 1]
- Structured logs show per-match timing

### Task 8.7: Run dbt build

- [ ] **Step 1: Run dbt**

```bash
MSYS_NO_PATHCONV=1 python -c "import dbt.cli.main; dbt.cli.main.dbtRunner().invoke(['build', '--project-dir', 'dbt_project', '--profiles-dir', 'dbt_project'])"
```

Verify:
- All new models build successfully
- `fct_pass_timing` populated
- All dbt tests pass (381+ data tests)

### Task 8.8: Run training notebooks for @Champion registration (D11)

- [ ] **Step 1: Run xG training notebook**

Execute `notebooks/train_xg_model.py` on Databricks. Verify:
- MLflow experiment created at `/soccer_analytics/xg_model`
- Model registered as `soccer_analytics.dev_gold.xg_model` with `@Champion` alias
- Metrics logged (ROC-AUC, Brier, etc.)

- [ ] **Step 2: Run Football2Vec training notebook**

Execute `notebooks/train_football2vec.py`. Verify `@Champion` alias set.

- [ ] **Step 3: Run VAEP/DEFCON training notebooks**

Create one-time training notebooks (or run existing pipelines with training flag) to register initial `@Champion` versions for VAEP and DEFCON models.

### Task 8.9: Run model validation (D12)

- [ ] **Step 1: Run pipeline**

Execute `run_model_validation` on Databricks. Verify:
- All validation results show "ok" status
- `model_validation_runs` table populated
- Structured logs emitted

### Task 8.10: Create synced tables

- [ ] **Step 1: Create in Databricks UI**

Manually create:
- `fct_pausa_values_synced` (project: soccer-analytics-dev, branch: production, scheduling: SNAPSHOT)
- `fct_pass_timing_synced` (same settings)

- [ ] **Step 2: Terraform import**

```bash
cd terraform/environments/dev
AWS_PROFILE=devops-agent terraform import \
    'module.synced_tables.databricks_database_synced_database_table.fct_pausa_values' \
    'soccer_analytics.dev_gold.fct_pausa_values_synced'
# Repeat for fct_pass_timing
```

- [ ] **Step 3: Restore indexes and grants**

Run: `.venv/Scripts/python.exe scripts/create_indexes.py`

Run PG grants via psycopg2 for SP `be66af99-...`.

### Task 8.11: Deploy and test Streamlit app

- [ ] **Step 1: Deploy**

```bash
databricks apps deploy soccer-analytics-dashboard-dev \
    --source-code-path /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse \
    --profile OAUTH
```

- [ ] **Step 2: E2E test Pass Timing page**

Open: `https://soccer-analytics-dashboard-dev-7474660814094441.aws.databricksapps.com`

Verify:
- Pass Timing page loads in ≤3 seconds
- Competition/match/team/player filters work
- Metric cards show values with `help=` tooltips
- OBSO heatmap renders on pitch overlay
- Scatter plot shows temporal × spatial with PAUSA sizing
- Rankings table loads, sortable, LIMIT 500
- Academic citation visible in footer
- Data scope label shows "IDSSE Bundesliga · 7 matches"
- All 11 existing pages still work (no regressions)

### Task 8.12: Deploy and test HF Space

- [ ] **Step 1: Export sample data**

Run notebook to export `sample_pausa.parquet` from `fct_pausa_values` joined with player names. Place in `demo_space/data/`.

- [ ] **Step 2: Deploy to HF Hub**

Push updated Space:
```bash
cd demo_space
huggingface-cli upload luxury-lakehouse/soccer-analytics-demo . . --repo-type space
```

- [ ] **Step 3: E2E test Pass Timing tab**

Open: `https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo`

Verify:
- Pass Timing tab loads
- Academic citations visible (PAUSA, OBSO, ELASTIC)
- Dropdowns filter correctly
- OBSO heatmap renders
- Scatter plot renders with dark theme
- Rankings table has interpretive column headers

### Task 8.13: Run HF Jobs xG template (D11 proof-of-concept)

- [ ] **Step 1: Run on HF Jobs**

```bash
hf jobs uv run scripts/train_xg_model_hf.py \
    --flavor cpu-basic --timeout 30m \
    --secrets HF_TOKEN=$HF_TOKEN
```

Verify: Job completes, metrics match notebook training, HF Hub weights updated.

### Task 8.14: Final CI verification

- [ ] **Step 1: Push branch and verify CI**

Push `feature/model-ops-and-event-sync` to remote. Verify all 3 CI workflows green:
- `python-ci.yml` — lint + typecheck + tests
- `dbt-ci.yml` — dbt slim CI (`state:modified+`)
- `terraform-plan.yml` — Terraform plan succeeds

### Task 8.15: Request commit approval

- [ ] **Step 1: Present results to user**

Show:
- Test count (before and after)
- All CI checks green
- Streamlit screenshots
- HF Space screenshots
- MLflow experiment summary
- Model validation results
- HF Jobs run logs

Ask user for commit approval.
