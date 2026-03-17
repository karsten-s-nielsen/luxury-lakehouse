# GPU Models v2 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate ML training to HF Jobs GPU, train custom EPV/Transition grids, build a neural xG v2 model with uncertainty quantification, and implement full Fernandez & Bornn 2018 Space Creation via `jax.vmap` batched pitch control.

**Architecture:** Foundation-first — E5 (data versioning) and O2 (VAEP HF Jobs) establish reusable patterns, D23 (trained grids) improves OBSO surface quality, D17 (xG v2) introduces neural inference via pure NumPy set encoder, D14 (Space Creation) adds `vmap`-batched pitch control for per-player counterfactual OBSO. All GPU compute runs on HF Jobs A10G. All inference runs on Databricks serverless executors via pure NumPy (no new executor dependencies).

**Tech Stack:** PyTorch (HF Jobs training only), NumPy (inference), JAX + `vmap` (D14 batch compute), XGBoost, MLflow, HF Hub, PySpark `applyInPandas`, ONNX-free design (pure NumPy forward pass)

**Branch:** `feature/gpu-models-v2`

**Commit strategy:** Single commit of fully E2E-tested code. No commits without explicit user approval. Additional commits only after audit tools and with approval.

**E2E Deployment:** User has full AWS, Databricks, and HF access. ALL of the following must complete before work is considered done:
- HF Jobs training scripts run and verified
- HF Hub datasets/models published
- dbt builds if models change
- Full local test suite passing (`ruff check`, `ruff format`, `pyright`, `pytest`)
- Streamlit/Gradio pages verified if UI changes

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `scripts/train_vaep_model_hf.py` | O2: HF Jobs PEP 723 script — VAEP model training on A10G |
| `scripts/compute_epv_transition_hf.py` | D23: HF Jobs PEP 723 script — train EPV + transition grids from SPADL data |
| `scripts/train_xg_v2_hf.py` | D17: HF Jobs PEP 723 script — set encoder + MLP xG model with MC dropout |
| `scripts/compute_space_creation_hf.py` | D14: HF Jobs PEP 723 script — `vmap`-batched space creation on A10G |
| `src/analytics/set_encoder.py` | D17: Pure NumPy set encoder forward pass + MC dropout inference + JSON serialization |
| `src/analytics/space_creation.py` | D14: Space creation analytics — differential OBSO per player |
| `src/tests/test_set_encoder.py` | D17: Unit tests for set encoder (forward pass, serialization, uncertainty) |
| `src/tests/test_space_creation.py` | D14: Unit tests for batched pitch control + space creation |
| `docs/huggingface/xg-v2-model-card.md` | D17: HF Hub model card for xG v2 |

### Modified Files

| File | Change |
|------|--------|
| `notebooks/train_xg_model.py` | E5: Add `mlflow.log_input()` with Delta version |
| `notebooks/train_football2vec.py` | E5: Add `mlflow.log_input()` with Delta version |
| `scripts/train_xg_model_hf.py` | E5: Add HF dataset commit hash logging |
| `scripts/compute_xt_grid_hf.py` | E5: Add HF dataset commit hash logging |
| `scripts/compute_obso_hf.py` | E5: Add HF dataset commit hash logging |
| `src/ingestion/spadl_vaep.py` | O2: Remove driver-side training fallback; load-only from MLflow `@Champion` |
| `src/analytics/obso.py` | D23: Add `load_trained_grids()` to consume HF Hub trained grids instead of synthetic proxy |
| `src/analytics/xg_model.py` | D17: Add `SetEncoderXGConfig`, `serialize_set_encoder_model()`, `deserialize_set_encoder_model()`, extend `build_features()` for freeze-frame context |
| `src/analytics/pitch_control.py` | D14: Add `compute_pitch_control_at_points_batched()` with `jax.vmap` over player-removal variants |
| `src/ingestion/xg_model.py` | D17: Extend scoring UDF to load v2 model, join freeze-frame data, run set encoder inference |
| `src/tests/test_xg_model.py` | D17: Add tests for set encoder features, v2 serialization, uncertainty bounds |
| `src/tests/test_pitch_control_model.py` | D14: Add `vmap` batched API tests |
| `src/tests/test_obso.py` | D23: Add trained grid loading/interpolation tests |
| `pyproject.toml` | No new runtime deps (pure NumPy inference). Add `torch>=2.0` to `[training]` optional extra for local dev only |
| `TODO.md` | Move E5, O2, D23, D17, D14 to completed |
| `ROADMAP.md` | Update Space Creation, DL Infrastructure, HF Hub Tier 3 status |

---

## Chunk 1: E5 + O2 — Foundation

### Task 1: E5 — Training Data Versioning (Existing Notebooks)

Add `mlflow.log_input()` to all existing training scripts. This is 3-5 lines per script inside existing `mlflow.start_run()` blocks.

**Files:**
- Modify: `notebooks/train_xg_model.py`
- Modify: `notebooks/train_football2vec.py`
- Modify: `scripts/train_xg_model_hf.py`
- Modify: `scripts/compute_xt_grid_hf.py`
- Modify: `scripts/compute_obso_hf.py`

#### Pattern A: Databricks notebooks (Delta tables)

- [ ] **Step 1: Add Delta version capture + `mlflow.log_input()` to `notebooks/train_xg_model.py`**

Inside the existing `with mlflow.start_run(...)` block, before the data read. Note: notebooks use hardcoded catalog/schema (not variables):

```python
# Capture Delta table version for reproducibility (E5)
_shots_version = spark.sql(
    "DESCRIBE HISTORY soccer_analytics.dev_gold.fct_shots LIMIT 1"
).first()["version"]

# ... existing data read ...

# Log training data provenance (E5)
# Note: mlflow.data.from_spark() with delta:// source requires MLflow 2.17+
# which is already pinned. Log version as param for simpler retrieval too.
mlflow.log_param("fct_shots_delta_version", int(_shots_version))
```

**Note on `mlflow.data.from_spark()`:** The `delta://table@version` URI format may not be fully supported in all MLflow configurations. The safe baseline is to log the Delta version as a plain param (always works). If `mlflow.data.from_spark()` is available and stable, add it as a supplementary call — but the param is the guaranteed minimum.

- [ ] **Step 2: Add Delta version capture to `notebooks/train_football2vec.py`**

Same pattern, twice — once for `stg_statsbomb__events` and once for `stg_wyscout__events`. Uses hardcoded catalog/schema matching the notebook:

```python
_sb_version = spark.sql(
    "DESCRIBE HISTORY soccer_analytics.dev_silver.stg_statsbomb__events LIMIT 1"
).first()["version"]
_ws_version = spark.sql(
    "DESCRIBE HISTORY soccer_analytics.dev_silver.stg_wyscout__events LIMIT 1"
).first()["version"]

# Inside mlflow.start_run():
mlflow.log_param("stg_statsbomb__events_delta_version", int(_sb_version))
mlflow.log_param("stg_wyscout__events_delta_version", int(_ws_version))
```

#### Pattern B: HF Jobs scripts (HF Hub datasets)

- [ ] **Step 3: Add HF dataset commit hash logging to all three HF Jobs scripts**

In `scripts/train_xg_model_hf.py`, `scripts/compute_xt_grid_hf.py`, and `scripts/compute_obso_hf.py`, add after data download:

```python
# Log HF dataset commit hash for reproducibility (E5)
_dataset_info = api.repo_info(repo_id=DATASET_REPO, repo_type="dataset")
_dataset_commit = _dataset_info.sha

# Inside mlflow block (where MLflow is conditional):
if mlflow is not None:
    mlflow.log_param(f"{DATASET_REPO.split('/')[-1]}_commit", _dataset_commit)
```

For scripts with multiple datasets (e.g., `compute_obso_hf.py` downloads 2), log each commit hash separately.

---

### Task 2: O2 — VAEP Training HF Jobs Script

Create a standalone PEP 723 script that trains VAEP models on HF Jobs, following the proven `train_xg_model_hf.py` pattern.

**Files:**
- Create: `scripts/train_vaep_model_hf.py`
- Test: Run locally with `uv run scripts/train_vaep_model_hf.py --dry-run` (if we add a dry-run flag) or test feature extraction logic in existing `test_spadl_vaep.py`

- [ ] **Step 1: Create `scripts/train_vaep_model_hf.py`**

PEP 723 header:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "socceraction==1.5.3",
#     "multimethod==1.12",
#     "huggingface-hub>=0.25.0",
#     "mlflow>=2.17.0",
# ]
# ///
```

HF Jobs invocation (in docstring):
```
hf jobs uv run scripts/train_vaep_model_hf.py \
    --flavor cpu-basic --timeout 60m \
    --secrets HF_TOKEN=$HF_TOKEN \
    --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
    --env DATABRICKS_HOST=$DATABRICKS_HOST \
    --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
```

Key implementation details:

**Data download:** Download from `luxury-lakehouse/spadl-vaep-action-values` (same pattern as `compute_xt_grid_hf.py`). The dataset has `type_id`, `result_id`, `bodypart_id` integer columns needed by socceraction.

**Feature extraction:** Inline the logic from `spadl_vaep.py:_extract_features_for_games()`:
- Same `_FEATURE_FNS` list (11 functions) and `_NB_PREV_ACTIONS = 3`
- Pre-build `game_groups = dict(iter(named.groupby("game_id")))` (CLAUDE.md anti-pattern guard)
- No `_MAX_TRAINING_GAMES` cap — HF Jobs has 46 GB RAM (A10G) or 16 GB (cpu-basic), far more than needed for ~2,388 matches
- Use `socceraction.spadl.add_names(actions)` to convert integer IDs to string names before `fs.gamestates()`

**Training:** Two `XGBClassifier` models (scores, concedes) with same hyperparameters as `spadl_vaep.py`:
```python
XGBClassifier(
    n_estimators=50,
    max_depth=3,
    n_jobs=-1,
    random_state=42,
)
```

**Serialization:** JSON envelope with base64-encoded XGBoost booster (same `save_raw("json")` pattern):
```python
def _serialize_vaep_model(model_scores, model_concedes):
    return json.dumps({
        "model_type": "vaep_xgboost_v1",
        "scores_booster_b64": base64.b64encode(
            model_scores.get_booster().save_raw("json")
        ).decode(),
        "concedes_booster_b64": base64.b64encode(
            model_concedes.get_booster().save_raw("json")
        ).decode(),
        "n_features": model_scores.n_features_in_,
        "nb_prev_actions": 3,
    }).encode()
```

**MLflow logging:**
- Experiment: `/soccer_analytics/vaep_model`
- Log params: hyperparameters, dataset sizes, `training_env="hf_jobs_cpu"`
- Log metrics: classification report metrics for both models
- Register: `soccer_analytics.dev_gold.vaep_model@Champion`
- E5 pattern: log HF dataset commit hash

**HF Hub publish:** Push serialized model to `luxury-lakehouse/vaep-model-statsbomb-wyscout` (new repo). Two files: `vaep_model.json` (combined scores+concedes) and `metrics.json`.

- [ ] **Step 2: Verify feature extraction produces identical features to `spadl_vaep.py`**

Add a test in `src/tests/test_spadl_vaep.py` that verifies the inlined feature function list and prev_actions constant match:

```python
class TestHfJobsFeatureCompat:
    """Verify HF Jobs script uses identical feature space."""

    def test_feature_fn_count(self):
        from ingestion.spadl_vaep import _FEATURE_FNS, _NB_PREV_ACTIONS
        assert len(_FEATURE_FNS) == 11
        assert _NB_PREV_ACTIONS == 3

    def test_feature_fn_names(self):
        from ingestion.spadl_vaep import _FEATURE_FNS
        expected_names = [
            "actiontype_onehot", "result_onehot", "bodypart_onehot",
            "time", "startlocation", "endlocation", "startpolar",
            "endpolar", "movement", "team", "time_delta",
        ]
        actual_names = [fn.__name__ for fn in _FEATURE_FNS]
        assert actual_names == expected_names
```

---

### Task 3: O2 — Simplify `spadl_vaep.py` Driver-Side Training

Remove the driver-side training fallback now that HF Jobs is the training path. Keep only the MLflow champion load. Also remove the now-unnecessary training data preparation at the call site.

**Files:**
- Modify: `src/ingestion/spadl_vaep.py` (lines 635-674 and 887-917)

- [ ] **Step 1: Simplify `_load_or_train_models()` (lines 635-674)**

Keep the existing function signature to avoid breaking the call site, but remove the training fallback:

```python
def _load_or_train_models(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    training_game_ids: list[int],
    training_pdf: pd.DataFrame,
) -> tuple[XGBClassifier, XGBClassifier] | None:
    """Load VAEP models from MLflow @Champion registry.

    Training is handled externally by HF Jobs (scripts/train_vaep_model_hf.py).
    The training_game_ids and training_pdf parameters are retained for signature
    compatibility but are no longer used for fallback training.
    """
    champion_models = _try_load_champion_vaep(logger)
    if champion_models is not None:
        return champion_models

    logger.warning(
        "No Champion VAEP model found in MLflow registry. "
        "Run scripts/train_vaep_model_hf.py on HF Jobs to train and register a model."
    )
    return None
```

- [ ] **Step 2: Simplify the call site (lines 887-917)**

The Phase C training data preparation (lines 887-900) is now unnecessary since we never train on the driver. Simplify:

```python
    # Phase C: Load pre-trained models from MLflow @Champion
    # Training is handled by HF Jobs (scripts/train_vaep_model_hf.py)
    spadl_sdf = spark.table(spadl_table)

    models = _load_or_train_models(
        spark, catalog, schema, logger,
        training_game_ids=[],   # unused — training on HF Jobs
        training_pdf=pd.DataFrame(),  # unused — training on HF Jobs
    )

    if models is None:
        return

    model_scores, model_concedes = models
```

This removes the `.toPandas()` call (line 900) that pulls training data to the driver — the core OOM risk that O2 is solving.

- [ ] **Step 3: Keep `_extract_features_for_games()` and `train_vaep_models()` as-is**

These functions remain in the module for reference. The HF Jobs script inlines its own copy (standalone pattern). Add a docstring note:

```python
# NOTE: _extract_features_for_games() and train_vaep_models() are retained
# for reference and local testing. Production training runs on HF Jobs
# via scripts/train_vaep_model_hf.py (PEP 723 standalone script).
```

- [ ] **Step 4: Run existing VAEP tests**

```bash
uv run pytest src/tests/test_spadl_vaep.py -v
```

Expected: All existing tests pass. The `TestTryLoadChampionVaep` tests mock MLflow and should be unaffected.

---

## Chunk 2: D23 — Custom EPV/Transition Grids

### Task 4: D23 — EPV/Transition Grid Training Script

Train proper ball-reachability (transition) and EPV grids from SPADL action data, replacing the synthetic Gaussian proxy used in OBSO.

**Files:**
- Create: `scripts/compute_epv_transition_hf.py`

**Key design decisions:**

The existing `compute_xt_grid_hf.py` already computes xT grids at 12×8 resolution from SPADL data using Markov chain value iteration. D23 extends this approach to produce the two grids that OBSO consumes:

**Important distinction:** OBSO's `compute_obso_surface()` expects two **2D spatial grids** — not zone-to-zone transition matrices. The existing synthetic proxy uses Gaussian distance decay for the transition grid. D23 replaces this with data-driven grids:

1. **Ball reachability grid** (replaces synthetic transition): A 2D spatial grid `(ny, nx)` representing P(ball reaches cell | ball at origin). Computed by marginalizing the zone-to-zone pass completion matrix: for each target zone, the reachability score is the empirical pass completion rate from the origin zone to that target, averaged across all origin zones weighted by pass frequency. The output is a `(64, 100)` spatial grid matching OBSO's expected shape — NOT the raw `(n_zones, n_zones)` transition matrix.

2. **EPV grid** (expected possession value): Same value iteration as xT but at `(32, 50)` resolution. `EPV(z) = P(shot|z) * P(goal|shot,z) + P(move|z) * sum_j(T(z,j) * EPV(j))`. The output is a `(32, 50)` spatial grid.

Both grids are parameterized by competition (per-competition + global), matching `compute_xt_grid_hf.py`. Both are 2D spatial grids compatible with `obso.py:interpolate_grid()` and `compute_obso_surface()`.

- [ ] **Step 1: Create `scripts/compute_epv_transition_hf.py`**

PEP 723 header:
```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "huggingface-hub>=0.25.0",
#     "mlflow>=2.17.0",
# ]
# ///
```

HF Jobs invocation:
```
hf jobs uv run scripts/compute_epv_transition_hf.py \
    --flavor cpu-basic --timeout 30m \
    --secrets HF_TOKEN=$HF_TOKEN
```

Key functions (inline from `expected_threat.py` patterns, extended for OBSO resolution):

```python
@dataclasses.dataclass(frozen=True)
class OBSOGridParams:
    """Grid parameters matching OBSO computation resolution."""
    transition_zones_x: int = 64
    transition_zones_y: int = 100
    epv_zones_x: int = 32
    epv_zones_y: int = 50
    pitch_length: float = 105.0  # SPADL coordinates
    pitch_width: float = 68.0
    max_iterations: int = 100
    tolerance: float = 1e-6
```

**Ball reachability grid computation:**

The key insight: OBSO needs `P(ball reaches cell C | ball at position B)` as a 2D spatial surface. We compute this by:
1. Build the full zone-to-zone pass completion matrix at intermediate resolution (16×25 = 400 zones — balances data density vs resolution)
2. For the "global" reachability grid: marginalize over all origin zones weighted by pass frequency → produces a single 2D grid showing how reachable each target zone is on average
3. At OBSO runtime: given ball position, look up the origin zone row from the completion matrix to get a ball-position-specific reachability surface

```python
def compute_ball_reachability_grid(actions_df, params):
    """Compute 2D spatial ball reachability grid from SPADL pass data.

    This produces two outputs:
    1. A global reachability grid (64, 100) — frequency-weighted average
       reachability across all origin zones. Replaces the Gaussian proxy.
    2. The intermediate zone-to-zone completion matrix for origin-specific
       lookup at OBSO runtime.

    Returns:
        global_grid: ndarray of shape (transition_zones_y, transition_zones_x)
            — 2D spatial grid compatible with obso.interpolate_grid()
        completion_matrix: ndarray of shape (n_intermediate, n_intermediate)
            — zone-to-zone pass completion for origin-specific lookup
    """
    intermediate_x, intermediate_y = 16, 25  # 400 zones — good data density
    n_intermediate = intermediate_x * intermediate_y

    moves = actions_df[actions_df["type_name"].isin(_MOVE_TYPES)]
    successful = moves[moves["result_name"] == "success"]

    attempt_matrix = np.zeros((n_intermediate, n_intermediate), dtype=np.float64)
    success_matrix = np.zeros((n_intermediate, n_intermediate), dtype=np.float64)

    origin_zones = _assign_zones(moves, "start_x", "start_y", intermediate_x, intermediate_y, params)
    target_zones = _assign_zones(moves, "end_x", "end_y", intermediate_x, intermediate_y, params)
    np.add.at(attempt_matrix, (origin_zones, target_zones), 1)

    origin_s = _assign_zones(successful, "start_x", "start_y", intermediate_x, intermediate_y, params)
    target_s = _assign_zones(successful, "end_x", "end_y", intermediate_x, intermediate_y, params)
    np.add.at(success_matrix, (origin_s, target_s), 1)

    # Per-zone pass completion with Laplace smoothing
    completion_matrix = (success_matrix + 1) / (attempt_matrix + 2)

    # Global reachability: weight each origin row by pass frequency
    origin_freq = attempt_matrix.sum(axis=1)
    origin_weights = origin_freq / origin_freq.sum()
    global_reachability = (origin_weights[:, None] * completion_matrix).sum(axis=0)

    # Reshape to 2D spatial grid (y, x) — OBSO convention
    global_grid = global_reachability.reshape(intermediate_y, intermediate_x)

    # Upscale to target resolution via interpolation
    from analytics.obso import interpolate_grid
    global_grid_hires = interpolate_grid(
        global_grid,
        (params.transition_zones_y, params.transition_zones_x),
    )

    return global_grid_hires, completion_matrix
```

**EPV computation:** Same `_value_iteration()` as `expected_threat.py` but at `(32, 50)` resolution. Output shape `(50, 32)` following the `(ny, nx)` convention.

**Output:** Publish to `luxury-lakehouse/obso-trained-grids` (new HF dataset):
- `data/reachability_grid_global.parquet` — long format `(zone_y, zone_x, reachability)` for the 2D spatial grid
- `data/epv_grid_global.parquet` — long format `(zone_y, zone_x, epv_value)`
- `data/completion_matrix_global.parquet` — sparse long format `(origin_zone, target_zone, probability)` for origin-specific lookup
- Per-competition grids as separate partitions
- `metadata.json` with grid dimensions, competition list, data provenance

**MLflow:** Log grid statistics (max/min/mean EPV, reachability range) + dataset commit hash (E5).

- [ ] **Step 2: Validate grid quality**

```python
def validate_reachability_grid(grid, params):
    """Validate trained ball reachability grid."""
    assert grid.shape == (params.transition_zones_y, params.transition_zones_x)
    assert np.all(grid >= 0) and np.all(grid <= 1)

def validate_epv_grid(grid, params):
    """Validate trained EPV grid."""
    assert grid.shape == (params.epv_zones_y, params.epv_zones_x)  # (ny, nx)
    assert np.all(grid >= 0) and np.all(grid <= 0.5)
    # EPV should increase toward goal (last column > first column)
    col_means = grid.mean(axis=0)
    assert col_means[-1] > col_means[0], "EPV must increase toward goal"
```

---

### Task 5: D23 — Wire Trained Grids into OBSO

Modify `obso.py` to load trained grids from HF Hub instead of using synthetic Gaussian proxy.

**Files:**
- Modify: `src/analytics/obso.py`
- Modify: `scripts/compute_obso_hf.py`
- Test: `src/tests/test_obso.py`

- [ ] **Step 1: Add `load_trained_grids()` and synthetic fallbacks to `src/analytics/obso.py`**

```python
def _make_synthetic_reachability_grid(ny: int = 100, nx: int = 64) -> np.ndarray:
    """Gaussian distance decay proxy for ball reachability.

    Used as fallback when trained grids are not available.
    Shape: (ny, nx) — OBSO convention.
    """
    y = np.linspace(0, 1, ny)
    x = np.linspace(0, 1, nx)
    yy, xx = np.meshgrid(y, x, indexing="ij")
    # Decay from center — rough proxy
    center_y, center_x = 0.5, 0.5
    dist = np.sqrt((xx - center_x) ** 2 + (yy - center_y) ** 2)
    return np.exp(-dist ** 2 / (2 * 0.3 ** 2))


def _make_synthetic_epv_grid(ny: int = 50, nx: int = 32) -> np.ndarray:
    """Linear ramp proxy for EPV. Shape: (ny, nx)."""
    x = np.linspace(0.01, 0.3, nx)
    return np.tile(x, (ny, 1))


def load_trained_grids(
    reachability_path: str | None = None,
    epv_path: str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Load trained 2D spatial grids from Parquet files.

    Both grids use (ny, nx) shape convention matching compute_obso_surface().
    Falls back to synthetic grids if paths are None (backward compatible).

    Returns:
        (reachability_grid, epv_grid) — both (ny, nx) shaped.
    """
    if reachability_path is not None:
        df = pd.read_parquet(reachability_path)
        ny = df["zone_y"].nunique()
        nx = df["zone_x"].nunique()
        reachability = df.pivot(
            index="zone_y", columns="zone_x", values="reachability",
        ).values.astype(np.float64)
    else:
        reachability = _make_synthetic_reachability_grid()

    if epv_path is not None:
        df = pd.read_parquet(epv_path)
        ny = df["zone_y"].nunique()
        nx = df["zone_x"].nunique()
        epv = df.pivot(
            index="zone_y", columns="zone_x", values="epv_value",
        ).values.astype(np.float64)
    else:
        epv = _make_synthetic_epv_grid()

    return reachability, epv
```

- [ ] **Step 2: Update `compute_obso_hf.py` to download trained grids from HF Hub**

Replace `_make_synthetic_grids()` call with:

```python
from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError, HfHubHTTPError

# Download trained grids (D23) — fall back to synthetic if not yet published
try:
    reachability_path = hf_hub_download(
        repo_id="luxury-lakehouse/obso-trained-grids",
        filename="data/reachability_grid_global.parquet",
        repo_type="dataset",
    )
    epv_path = hf_hub_download(
        repo_id="luxury-lakehouse/obso-trained-grids",
        filename="data/epv_grid_global.parquet",
        repo_type="dataset",
    )
    reachability_grid, epv_grid = load_trained_grids(reachability_path, epv_path)
    logger.info("loaded_trained_grids", source="luxury-lakehouse/obso-trained-grids")
except (HfHubHTTPError, EntryNotFoundError, RepositoryNotFoundError):
    logger.warning("trained_grids_unavailable — falling back to synthetic grids")
    reachability_grid, epv_grid = _make_synthetic_reachability_grid(), _make_synthetic_epv_grid()
```

- [ ] **Step 3: Add tests for trained grid loading**

In `src/tests/test_obso.py`:

```python
from analytics.obso import load_trained_grids


class TestLoadTrainedGrids:
    def test_load_from_parquet(self, tmp_path):
        """Round-trip: create parquet, load, verify shape."""
        ny_r, nx_r = 8, 6  # small reachability grid
        rng = np.random.default_rng(42)
        reach_df = pd.DataFrame({
            "zone_y": np.repeat(np.arange(ny_r), nx_r),
            "zone_x": np.tile(np.arange(nx_r), ny_r),
            "reachability": rng.random(ny_r * nx_r),
        })
        r_path = tmp_path / "reachability.parquet"
        reach_df.to_parquet(r_path)

        ny_e, nx_e = 5, 4  # small EPV grid
        epv_df = pd.DataFrame({
            "zone_y": np.repeat(np.arange(ny_e), nx_e),
            "zone_x": np.tile(np.arange(nx_e), ny_e),
            "epv_value": rng.random(ny_e * nx_e),
        })
        e_path = tmp_path / "epv.parquet"
        epv_df.to_parquet(e_path)

        reachability, epv = load_trained_grids(str(r_path), str(e_path))
        assert reachability.shape == (ny_r, nx_r)  # (ny, nx) convention
        assert epv.shape == (ny_e, nx_e)

    def test_fallback_to_synthetic(self):
        """None paths produce synthetic grids."""
        reachability, epv = load_trained_grids(None, None)
        assert reachability.ndim == 2
        assert epv.ndim == 2
        assert np.all(reachability >= 0)
        assert np.all(epv >= 0)
```

- [ ] **Step 4: Run OBSO tests**

```bash
uv run pytest src/tests/test_obso.py -v
```

---

## Chunk 3: D17 + U4p — xG v2 Neural Context Model

### Task 6: D17 — Set Encoder Analytics Module (Pure NumPy Inference)

Create a pure NumPy set encoder that can run on Databricks serverless executors with zero new dependencies. The architecture follows Deep Sets (Zaheer et al. 2017): per-element MLP → sum aggregation → context vector.

**Files:**
- Create: `src/analytics/set_encoder.py`
- Create: `src/tests/test_set_encoder.py`

**Architecture:**

```
Input: (N_players, 4) — [x, y, is_keeper, is_teammate] per visible player
    ↓
Per-player MLP: Linear(4→32) → ReLU → Linear(32→16) → ReLU
    ↓
Sum aggregation (permutation invariant)
    ↓
Context vector: (16,)
    ↓
Concat with tabular features: (13 + 16 = 29 after one-hot expansion: ~50-60)
    ↓
Prediction MLP: Linear(input→64) → ReLU → Dropout(p) → Linear(64→32) → ReLU → Dropout(p) → Linear(32→1) → Sigmoid
    ↓
Output: xG probability (scalar)
```

For shots without freeze-frame data: zero vector `(16,)` context. The model learns during training that zeros = "no spatial context."

MC dropout uncertainty (U4p): At inference, run N forward passes with random dropout masks, compute `mean ± 1.96*std` for 95% CI.

- [ ] **Step 1: Create `src/analytics/set_encoder.py`**

```python
"""Pure NumPy set encoder for freeze-frame context in xG v2.

Training uses PyTorch (scripts/train_xg_v2_hf.py on HF Jobs).
Inference uses pure NumPy — no PyTorch, no ONNX, zero new dependencies.

Architecture: Deep Sets (Zaheer et al. 2017)
    per-player MLP → sum aggregation → prediction MLP with MC dropout

References:
    Zaheer, M. et al. (2017). "Deep Sets." NeurIPS.
"""
from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SetEncoderConfig:
    """Hyperparameters for the set encoder architecture."""

    player_feature_dim: int = 4      # x, y, is_keeper, is_teammate
    encoder_hidden: int = 32
    context_dim: int = 16
    pred_hidden_1: int = 64
    pred_hidden_2: int = 32
    dropout_p: float = 0.1
    n_mc_samples: int = 50           # MC dropout forward passes


def _relu(x: np.ndarray) -> np.ndarray:
    return np.maximum(x, 0)


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -50, 50)))


def _linear(x: np.ndarray, weight: np.ndarray, bias: np.ndarray) -> np.ndarray:
    """Linear layer: x @ weight.T + bias."""
    return x @ weight.T + bias


def encode_player_set(
    player_features: np.ndarray,
    weights: dict[str, np.ndarray],
) -> np.ndarray:
    """Encode a variable-size player set into a fixed-size context vector.

    Args:
        player_features: (N_players, 4) array of [x, y, is_keeper, is_teammate].
            Coordinates should be normalized to [0, 1] range.
        weights: Dict of weight matrices from training.

    Returns:
        (context_dim,) context vector (sum-pooled).
    """
    if len(player_features) == 0:
        return np.zeros(weights["encoder_fc2_bias"].shape[0], dtype=np.float64)

    # Per-player MLP
    h = _relu(_linear(player_features, weights["encoder_fc1_weight"], weights["encoder_fc1_bias"]))
    h = _relu(_linear(h, weights["encoder_fc2_weight"], weights["encoder_fc2_bias"]))

    # Sum aggregation (permutation invariant)
    return h.sum(axis=0)


def predict_xg(
    tabular_features: np.ndarray,
    context_vector: np.ndarray,
    weights: dict[str, np.ndarray],
    *,
    dropout_mask: np.ndarray | None = None,
    config: SetEncoderConfig = SetEncoderConfig(),
) -> float:
    """Predict xG from tabular features + set encoder context.

    Args:
        tabular_features: (D,) one-hot encoded shot features.
        context_vector: (context_dim,) from encode_player_set().
        weights: Dict of weight matrices.
        dropout_mask: Optional pre-computed mask for MC dropout.
        config: Model configuration.

    Returns:
        xG probability in [0, 1].
    """
    x = np.concatenate([tabular_features, context_vector])

    # Prediction MLP with optional dropout
    h = _relu(_linear(x, weights["pred_fc1_weight"], weights["pred_fc1_bias"]))
    if dropout_mask is not None:
        h = h * dropout_mask[:config.pred_hidden_1] / (1 - config.dropout_p)

    h = _relu(_linear(h, weights["pred_fc2_weight"], weights["pred_fc2_bias"]))
    if dropout_mask is not None:
        h = h * dropout_mask[config.pred_hidden_1:config.pred_hidden_1 + config.pred_hidden_2] / (1 - config.dropout_p)

    logit = _linear(h, weights["pred_fc3_weight"], weights["pred_fc3_bias"])
    return float(_sigmoid(logit).item())


def predict_xg_with_uncertainty(
    tabular_features: np.ndarray,
    context_vector: np.ndarray,
    weights: dict[str, np.ndarray],
    *,
    config: SetEncoderConfig = SetEncoderConfig(),
    random_state: int = 42,
) -> tuple[float, float, float, float]:
    """Predict xG with MC dropout uncertainty quantification.

    Returns:
        (mean, std, ci_lower, ci_upper) — 95% confidence interval.
    """
    rng = np.random.default_rng(random_state)
    mask_size = config.pred_hidden_1 + config.pred_hidden_2
    predictions = np.empty(config.n_mc_samples, dtype=np.float64)

    for i in range(config.n_mc_samples):
        mask = (rng.random(mask_size) > config.dropout_p).astype(np.float64)
        predictions[i] = predict_xg(
            tabular_features, context_vector, weights,
            dropout_mask=mask, config=config,
        )

    mean = float(predictions.mean())
    std = float(predictions.std())
    ci_lower = float(np.clip(mean - 1.96 * std, 0, 1))
    ci_upper = float(np.clip(mean + 1.96 * std, 0, 1))
    return mean, std, ci_lower, ci_upper


def serialize_set_encoder_weights(weights: dict[str, np.ndarray]) -> bytes:
    """Serialize set encoder weights to JSON (no pickle).

    Same envelope pattern as xg_model.py serialize_xgboost_model().
    """
    serialized = {}
    for key, arr in weights.items():
        serialized[key] = base64.b64encode(arr.astype(np.float64).tobytes()).decode()
        serialized[f"{key}_shape"] = list(arr.shape)

    return json.dumps({
        "model_type": "set_encoder_xg_v2",
        "weights": serialized,
    }).encode()


def deserialize_set_encoder_weights(data: bytes) -> dict[str, np.ndarray]:
    """Deserialize set encoder weights from JSON."""
    envelope = json.loads(data)
    assert envelope["model_type"] == "set_encoder_xg_v2"

    weights = {}
    serialized = envelope["weights"]
    for key in serialized:
        if key.endswith("_shape"):
            continue
        shape = tuple(serialized[f"{key}_shape"])
        weights[key] = np.frombuffer(
            base64.b64decode(serialized[key]), dtype=np.float64
        ).reshape(shape).copy()  # .copy() to make writeable

    return weights
```

- [ ] **Step 2: Create `src/tests/test_set_encoder.py`**

```python
"""Tests for pure NumPy set encoder (D17)."""
from __future__ import annotations

import numpy as np
import numpy.testing as npt

from analytics.set_encoder import (
    SetEncoderConfig,
    deserialize_set_encoder_weights,
    encode_player_set,
    predict_xg,
    predict_xg_with_uncertainty,
    serialize_set_encoder_weights,
)


def _make_random_weights(
    config: SetEncoderConfig, tabular_dim: int = 13, seed: int = 42,
) -> dict[str, np.ndarray]:
    """Create random weights matching the architecture.

    tabular_dim: number of tabular features BEFORE one-hot encoding.
    In production, this will be larger (~50-60 after one-hot expansion of
    shot_body_part, shot_technique, shot_type, play_pattern). Tests use
    13 (raw feature count) for simplicity.
    """
    rng = np.random.default_rng(seed)
    return {
        "encoder_fc1_weight": rng.standard_normal((config.encoder_hidden, config.player_feature_dim)),
        "encoder_fc1_bias": rng.standard_normal(config.encoder_hidden),
        "encoder_fc2_weight": rng.standard_normal((config.context_dim, config.encoder_hidden)),
        "encoder_fc2_bias": rng.standard_normal(config.context_dim),
        "pred_fc1_weight": rng.standard_normal((config.pred_hidden_1, config.context_dim + tabular_dim)),
        "pred_fc1_bias": rng.standard_normal(config.pred_hidden_1),
        "pred_fc2_weight": rng.standard_normal((config.pred_hidden_2, config.pred_hidden_1)),
        "pred_fc2_bias": rng.standard_normal(config.pred_hidden_2),
        "pred_fc3_weight": rng.standard_normal((1, config.pred_hidden_2)),
        "pred_fc3_bias": rng.standard_normal(1),
    }


class TestEncodePlayerSet:
    def test_empty_set_returns_zeros(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        context = encode_player_set(np.empty((0, 4)), weights)
        assert context.shape == (config.context_dim,)
        npt.assert_array_equal(context, 0)

    def test_single_player(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        player = np.array([[0.5, 0.5, 0.0, 1.0]])  # teammate at center
        context = encode_player_set(player, weights)
        assert context.shape == (config.context_dim,)
        assert not np.all(context == 0)

    def test_permutation_invariance(self):
        """Deep Sets must be permutation invariant."""
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        rng = np.random.default_rng(42)
        players = rng.random((5, 4))

        ctx_original = encode_player_set(players, weights)
        ctx_shuffled = encode_player_set(players[rng.permutation(5)], weights)
        npt.assert_allclose(ctx_original, ctx_shuffled, atol=1e-10)

    def test_variable_size_input(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        rng = np.random.default_rng(42)

        ctx_3 = encode_player_set(rng.random((3, 4)), weights)
        ctx_10 = encode_player_set(rng.random((10, 4)), weights)
        assert ctx_3.shape == ctx_10.shape == (config.context_dim,)


class TestPredictXG:
    def test_output_range(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        tabular = np.random.default_rng(42).random(13)
        context = np.random.default_rng(42).random(config.context_dim)
        pred = predict_xg(tabular, context, weights, config=config)
        assert 0 <= pred <= 1

    def test_deterministic_without_dropout(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        tabular = np.random.default_rng(42).random(13)
        context = np.random.default_rng(42).random(config.context_dim)
        p1 = predict_xg(tabular, context, weights, config=config)
        p2 = predict_xg(tabular, context, weights, config=config)
        assert p1 == p2


class TestMCDropoutUncertainty:
    def test_returns_four_values(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        tabular = np.random.default_rng(42).random(13)
        context = np.random.default_rng(42).random(config.context_dim)
        mean, std, ci_lo, ci_hi = predict_xg_with_uncertainty(
            tabular, context, weights, config=config,
        )
        assert 0 <= mean <= 1
        assert std >= 0
        assert 0 <= ci_lo <= ci_hi <= 1

    def test_ci_contains_mean(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        tabular = np.random.default_rng(42).random(13)
        context = np.random.default_rng(42).random(config.context_dim)
        mean, _, ci_lo, ci_hi = predict_xg_with_uncertainty(
            tabular, context, weights, config=config,
        )
        assert ci_lo <= mean <= ci_hi

    def test_reproducible_with_seed(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        tabular = np.random.default_rng(42).random(13)
        context = np.random.default_rng(42).random(config.context_dim)
        r1 = predict_xg_with_uncertainty(tabular, context, weights, config=config, random_state=99)
        r2 = predict_xg_with_uncertainty(tabular, context, weights, config=config, random_state=99)
        assert r1 == r2


class TestSerialization:
    def test_roundtrip(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        data = serialize_set_encoder_weights(weights)
        recovered = deserialize_set_encoder_weights(data)
        for key in weights:
            npt.assert_array_equal(weights[key], recovered[key])

    def test_no_pickle_in_bytes(self):
        config = SetEncoderConfig()
        weights = _make_random_weights(config)
        data = serialize_set_encoder_weights(weights)
        assert b"\x80\x05" not in data  # pickle protocol 5 magic
```

- [ ] **Step 3: Run set encoder tests**

```bash
uv run pytest src/tests/test_set_encoder.py -v
```

---

### Task 7: D17 — xG v2 HF Jobs Training Script

Train the set encoder + prediction MLP on HF Jobs GPU, export weights as NumPy arrays.

**Files:**
- Create: `scripts/train_xg_v2_hf.py`

- [ ] **Step 1: Create `scripts/train_xg_v2_hf.py`**

PEP 723 header:
```python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "huggingface-hub>=0.25.0",
#     "mlflow>=2.17.0",
# ]
# ///
```

HF Jobs invocation:
```
hf jobs uv run scripts/train_xg_v2_hf.py \
    --flavor a10g --timeout 60m \
    --secrets HF_TOKEN=$HF_TOKEN \
    --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
    --env DATABRICKS_HOST=$DATABRICKS_HOST \
    --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
```

**Data pipeline:**
1. Download `luxury-lakehouse/xg-shot-data` (131K shots with tabular features)
2. Download `luxury-lakehouse/xg-freeze-frame-data` (new dataset, ~250K rows — shot freeze-frame positions)
3. Join on `event_id` — shots with freeze-frame get full position arrays, shots without get empty arrays

**PyTorch training architecture:**

```python
class SetEncoderXG(torch.nn.Module):
    """Deep Sets encoder + prediction MLP for xG."""

    def __init__(self, tabular_dim, config):
        super().__init__()
        self.encoder = torch.nn.Sequential(
            torch.nn.Linear(config.player_feature_dim, config.encoder_hidden),
            torch.nn.ReLU(),
            torch.nn.Linear(config.encoder_hidden, config.context_dim),
            torch.nn.ReLU(),
        )
        self.predictor = torch.nn.Sequential(
            torch.nn.Linear(tabular_dim + config.context_dim, config.pred_hidden_1),
            torch.nn.ReLU(),
            torch.nn.Dropout(config.dropout_p),
            torch.nn.Linear(config.pred_hidden_1, config.pred_hidden_2),
            torch.nn.ReLU(),
            torch.nn.Dropout(config.dropout_p),
            torch.nn.Linear(config.pred_hidden_2, 1),
        )

    def forward(self, tabular, player_sets, set_sizes):
        # Encode variable-length player sets
        encoded = self.encoder(player_sets)  # (total_players, context_dim)
        # Sum-pool per shot (using set_sizes to group)
        contexts = torch.zeros(len(set_sizes), encoded.shape[-1], device=encoded.device)
        offset = 0
        for i, size in enumerate(set_sizes):
            if size > 0:
                contexts[i] = encoded[offset:offset + size].sum(dim=0)
            offset += size
        # Predict
        combined = torch.cat([tabular, contexts], dim=-1)
        return torch.sigmoid(self.predictor(combined)).squeeze(-1)
```

**Training loop:** Standard PyTorch, BCE loss, Adam optimizer, early stopping on validation Brier score. Train/test split: 80/20 stratified by `competition_id` (same as v1).

**Weight export:** Extract PyTorch state_dict → convert to NumPy arrays → `serialize_set_encoder_weights()`.

**Evaluation:** Must compute:
- ROC-AUC, Brier score, log loss (same as v1)
- Brier score improvement over v1 XGBoost baseline
- Uncertainty calibration: empirical coverage of 95% CI
- StatsBomb benchmark comparison

**MLflow:**
- Experiment: `/soccer_analytics/xg_model_v2`
- Log all metrics + hyperparameters + E5 dataset commit hashes
- Register as `soccer_analytics.dev_gold.xg_model_v2@Champion` (separate from v1)

**HF Hub publish:** `luxury-lakehouse/xg-v2-model-set-encoder` with `model_weights.json` + `metrics.json` + model card.

---

### Task 8: D17 — Publish 360 Freeze-Frame Dataset

Create the training dataset that pairs shots with freeze-frame positions.

**Files:**
- Modify: `notebooks/publish_datasets.py` (add new dataset publication)

- [ ] **Step 1: Add freeze-frame dataset publication to `notebooks/publish_datasets.py`**

New SQL query for the dataset:

```sql
SELECT
    s.event_id,
    s.match_id,
    s.competition_id,
    ff.location_x / 120.0 as player_x_norm,
    ff.location_y / 80.0 as player_y_norm,
    ff.is_keeper,
    ff.is_teammate
FROM {catalog}.{schema}.stg_statsbomb__shots s
INNER JOIN {catalog}.{schema}.stg_statsbomb__360 ff
    ON s.event_id = ff.event_uuid
    AND s.match_id = ff.match_id
WHERE s.event_id IS NOT NULL
```

Publish to `luxury-lakehouse/xg-freeze-frame-data` partitioned by `competition_id`.

For shots in matches WITHOUT dedicated 360 data, parse inline `shot_freeze_frame` JSON:

```sql
-- Alternative: inline freeze frame from events (broader coverage)
SELECT
    event_id,
    match_id,
    competition_id,
    ff_element.location[0] / 120.0 as player_x_norm,
    ff_element.location[1] / 80.0 as player_y_norm,
    ff_element.keeper as is_keeper,
    ff_element.teammate as is_teammate
FROM {catalog}.{schema}.stg_statsbomb__events
LATERAL VIEW explode(
    from_json(shot_freeze_frame, 'ARRAY<STRUCT<location:ARRAY<DOUBLE>,teammate:BOOLEAN,actor:BOOLEAN,keeper:BOOLEAN>>')
) AS ff_element
WHERE event_type = 'Shot' AND shot_freeze_frame IS NOT NULL
```

Decision: Use the inline `shot_freeze_frame` (broader coverage — all StatsBomb matches, not just 323 360-matches). This gives ~95K shots with freeze-frame context instead of ~10K.

**Remove the first SQL block** (360 join) — it's dead code since we're using the inline approach. Keep only the `LATERAL VIEW` version.

**Column naming:** Output `ff_element.teammate as is_teammate` (not `NOT ff_element.teammate as is_opponent`) to match the set encoder's expected input format `[x, y, is_keeper, is_teammate]`.

---

### Task 9: D17 — Extend Scoring Pipeline for v2

Modify `src/ingestion/xg_model.py` to support the set encoder model at inference time.

**Files:**
- Modify: `src/ingestion/xg_model.py`
- Modify: `src/analytics/xg_model.py`
- Test: `src/tests/test_xg_model.py`

- [ ] **Step 1: Add freeze-frame parsing to the scoring query**

In `src/ingestion/xg_model.py`, modify the data loading to include freeze-frame context:

```python
def _load_shots_with_context(spark, catalog, schema):
    """Load shots with inline freeze-frame context for v2 scoring."""
    return spark.sql(f"""
        SELECT
            s.*,
            e.shot_freeze_frame
        FROM {catalog}.{schema}.fct_shots s
        LEFT JOIN {catalog}.{schema}.stg_statsbomb__events e
            ON s.event_id = e.event_id
            AND s.data_source = 'statsbomb'
    """)
```

- [ ] **Step 2: Extend the scoring UDF to handle v2 model**

Add a v2 code path in `_make_scoring_udf()` that:
1. Detects model version from the weights envelope (`model_type` field)
2. For v2: parse `shot_freeze_frame` JSON → build player position array → run `encode_player_set()` → `predict_xg()` or `predict_xg_with_uncertainty()`
3. Output columns: `xg_set_encoder` (point estimate), `xg_ci_lower`, `xg_ci_upper`
4. For v1: unchanged behavior (backward compatible)

```python
def _parse_freeze_frame(json_str: str | None) -> np.ndarray:
    """Parse shot_freeze_frame JSON to (N, 4) player feature array."""
    if json_str is None:
        return np.empty((0, 4), dtype=np.float64)
    try:
        players = json.loads(json_str)
        features = []
        for p in players:
            loc = p.get("location", [0, 0])
            features.append([
                loc[0] / 120.0,  # normalize to [0, 1]
                loc[1] / 80.0,
                float(p.get("keeper", False)),
                float(p.get("teammate", False)),
            ])
        return np.array(features, dtype=np.float64) if features else np.empty((0, 4), dtype=np.float64)
    except (json.JSONDecodeError, TypeError):
        return np.empty((0, 4), dtype=np.float64)
```

- [ ] **Step 3: Add v2 tests**

In `src/tests/test_xg_model.py`:

```python
class TestParseFreezeFrame:
    def test_valid_json(self):
        ff_json = json.dumps([
            {"location": [60, 40], "teammate": True, "keeper": False, "actor": False},
            {"location": [90, 20], "teammate": False, "keeper": True, "actor": False},
        ])
        result = _parse_freeze_frame(ff_json)
        assert result.shape == (2, 4)
        npt.assert_allclose(result[0, :2], [60/120, 40/80])

    def test_none_returns_empty(self):
        result = _parse_freeze_frame(None)
        assert result.shape == (0, 4)

    def test_invalid_json_returns_empty(self):
        result = _parse_freeze_frame("{bad json")
        assert result.shape == (0, 4)
```

---

### Task 10: D17 — HF Hub Model Card

**Files:**
- Create: `docs/huggingface/xg-v2-model-card.md`

- [ ] **Step 1: Write model card**

Follow the pattern from the existing xG model card. Include:
- Architecture description (Deep Sets + MLP)
- Training data provenance (StatsBomb CC-BY 4.0, Wyscout CC-BY-NC 4.0)
- Performance metrics (ROC-AUC, Brier, calibration)
- Uncertainty quantification methodology (MC dropout, Gal & Ghahramani 2016)
- Comparison with v1 XGBoost baseline
- Coordinate system (StatsBomb 120×80, normalized to [0,1])
- Academic citations:
  - Zaheer et al. (2017) "Deep Sets" NeurIPS
  - Gal & Ghahramani (2016) "Dropout as a Bayesian Approximation" ICML

---

## Chunk 4: D14 — Space Creation

### Task 11: D14 — `vmap`-Batched Pitch Control

Add a `jax.vmap`-able version of pitch control that computes N+1 player-configuration variants in a single GPU dispatch.

**Files:**
- Modify: `src/analytics/pitch_control.py`
- Test: `src/tests/test_pitch_control_model.py`

**Design:**

The current kernel computes `(n_players, n_targets)` TTI in one call. For space creation, we need to compute pitch control for 23 variants (all players + remove each of 22 players) simultaneously. The key insight: `jax.vmap` can map over a "player mask" dimension.

- [ ] **Step 1: Add batched array-based pitch control API**

```python
def _compute_pc_from_arrays(
    home_xy: np.ndarray,       # (n_home, 2)
    home_vel: np.ndarray,      # (n_home, 2)
    away_xy: np.ndarray,       # (n_away, 2)
    away_vel: np.ndarray,      # (n_away, 2)
    targets: np.ndarray,       # (n_targets, 2)
    params: PitchControlParams,
) -> np.ndarray:
    """Compute pitch control from pure arrays (no DataFrame).

    Returns: (n_targets,) home control values.
    """
    # Same math as compute_pitch_control_at_points but with array inputs
    # This is the vmappable inner function
    ...
```

- [ ] **Step 2: Add player-removal batch API with `jax.vmap`**

The key challenge: `jax.vmap` requires fixed-size arrays. The existing kernel separates home/away into variable-size arrays. For `vmap` over player-removal variants, we use a **mask-based approach** on fixed-size padded arrays where masked-out players contribute zero influence.

```python
def compute_pitch_control_player_removal(
    players_df: pd.DataFrame,
    targets: np.ndarray,
    params: PitchControlParams | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute pitch control with each player removed (Space Creation).

    Uses jax.vmap to compute all N+1 variants (baseline + N player removals)
    in a single GPU dispatch. Players are represented as a combined array
    with team indicators; removed players are masked via zeroed influence.

    Args:
        players_df: DataFrame with player_id, team, x, y, velocity_x, velocity_y.
        targets: (n_targets, 2) target points in StatsBomb 120x80 coordinates.
        params: Pitch control parameters.

    Returns:
        baseline: (n_targets,) — pitch control with all players.
        removed: (n_players, n_targets) — pitch control with each player removed.
    """
    if not _USE_JAX:
        raise ImportError("Space Creation requires JAX for vmap batching")
    import jax
    import jax.numpy as jnp

    params = params or PitchControlParams()
    n_players = len(players_df)
    n_targets = len(targets)

    # Convert to StatsBomb coordinates and extract arrays
    home_mask = (players_df["team"] == "home").values
    xy = players_df[["x", "y"]].values.astype(np.float64)
    vel = players_df[["velocity_x", "velocity_y"]].values.astype(np.float64)

    # Build N+1 player masks: (n_variants, n_players)
    # Row 0: all players present (baseline)
    # Row i+1: player i removed
    masks = np.ones((n_players + 1, n_players), dtype=np.float64)
    for i in range(n_players):
        masks[i + 1, i] = 0.0

    # JIT-compiled function for a single variant
    @jax.jit
    def _pc_single_variant(mask, xy, vel, home_mask, targets, reaction_time, max_accel, sigma):
        """Compute pitch control for one player-mask configuration.

        Masked players (mask=0) have their TTI set to infinity (large value),
        so they contribute zero influence.
        """
        n_p = xy.shape[0]
        n_t = targets.shape[0]

        # Displacement from each player to each target: (n_players, n_targets, 2)
        disp = targets[None, :, :] - xy[:, None, :]
        dist = jnp.sqrt(jnp.sum(disp ** 2, axis=-1))  # (n_players, n_targets)

        # Velocity projection onto displacement direction
        direction = disp / jnp.maximum(dist[:, :, None], 1e-10)
        v_proj = jnp.sum(vel[:, None, :] * direction, axis=-1)  # (n_players, n_targets)

        # TTI via kinematic equation
        discriminant = v_proj ** 2 + 2 * max_accel * dist
        tti = reaction_time + (-v_proj + jnp.sqrt(jnp.maximum(discriminant, 0))) / max_accel

        # Apply mask: removed players get infinite TTI
        large_tti = 1e6
        tti = jnp.where(mask[:, None] > 0.5, tti, large_tti)

        # Split by team and compute influence
        home_tti = jnp.where(home_mask[:, None], tti, large_tti)  # (n_players, n_targets)
        away_tti = jnp.where(~home_mask[:, None], tti, large_tti)

        home_min_tti = jnp.min(home_tti, axis=0)  # (n_targets,)
        away_min_tti = jnp.min(away_tti, axis=0)

        # Logistic influence (same as _influence_jax)
        k = jnp.pi / jnp.sqrt(3.0) / sigma
        home_influence = 1.0 / (1.0 + jnp.exp(jnp.clip(-k * (away_min_tti - home_min_tti), -50, 50)))

        return home_influence  # (n_targets,) home control in [0, 1]

    # vmap over mask dimension — single GPU dispatch for all N+1 variants
    _pc_batched = jax.vmap(
        _pc_single_variant,
        in_axes=(0, None, None, None, None, None, None, None),
    )

    results = np.array(_pc_batched(
        jnp.array(masks),
        jnp.array(xy),
        jnp.array(vel),
        jnp.array(home_mask),
        jnp.array(targets),
        params.reaction_time,
        params.max_acceleration,
        params.sigma,
    ))

    baseline = results[0]  # (n_targets,)
    removed = results[1:]  # (n_players, n_targets)
    return baseline, removed
```

**Key design notes:**
- Masked players get `TTI = 1e6` (effectively infinite) so they contribute zero influence
- `home_mask` is a boolean array distinguishing teams — no separate home/away arrays needed
- The `jax.vmap` maps over the first axis of `masks` (n_variants), keeping all other inputs fixed
- One GPU dispatch computes all 23 variants simultaneously
- The logistic influence formula matches `_influence_jax` exactly

- [ ] **Step 3: Add tests for batched API**

In `src/tests/test_pitch_control_model.py`:

```python
@pytest.mark.skipif(not _HAS_JAX, reason="JAX not installed")
class TestPlayerRemovalBatch:
    def test_baseline_matches_standard(self):
        """Baseline (all players) must match compute_pitch_control_at_points."""
        players = _make_players(
            home_positions=[(30, 30), (50, 40)],
            away_positions=[(70, 40), (90, 40)],
        )
        targets = np.array([[50, 34], [80, 40]])
        standard = compute_pitch_control_at_points(players, targets)
        baseline, _ = compute_pitch_control_player_removal(players, targets)
        npt.assert_allclose(baseline, standard, atol=1e-6)

    def test_removing_defender_increases_attack_control(self):
        """Removing a defender should increase home control near that defender."""
        players = _make_players(
            home_positions=[(30, 40)],
            away_positions=[(60, 40)],
        )
        target_near_defender = np.array([[60, 40]])
        baseline, removed = compute_pitch_control_player_removal(players, target_near_defender)
        # removed[1] = away player removed → home control should increase
        assert removed[1, 0] > baseline[0]

    def test_output_shapes(self):
        players = _make_players(
            home_positions=[(30, 30), (50, 40)],
            away_positions=[(70, 40), (90, 40)],
        )
        targets = np.array([[50, 34], [80, 40], [60, 20]])
        baseline, removed = compute_pitch_control_player_removal(players, targets)
        assert baseline.shape == (3,)
        assert removed.shape == (4, 3)  # 4 players × 3 targets
```

---

### Task 12: D14 — Space Creation Analytics Module

Pure analytics module for computing per-player space creation from differential OBSO.

**Files:**
- Create: `src/analytics/space_creation.py`
- Create: `src/tests/test_space_creation.py`

- [ ] **Step 1: Create `src/analytics/space_creation.py`**

```python
"""Space Creation quantification (Fernandez & Bornn 2018).

Measures each player's contribution to the team's off-ball scoring
opportunity by computing differential OBSO: how much the team's
OBSO surface changes when that player is removed.

References:
    Fernandez, J. & Bornn, L. (2018). "Wide Open Spaces: A statistical
    technique for measuring space creation in professional soccer."
    MIT Sloan Sports Analytics Conference.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpaceCreationParams:
    """Parameters for space creation computation."""
    grid_cells_x: int = 104
    grid_cells_y: int = 68
    pitch_length: float = 120.0  # StatsBomb
    pitch_width: float = 80.0


def compute_space_created(
    baseline_obso: np.ndarray,
    removed_obso: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
) -> float:
    """Compute space created by a single player (m²).

    Space created = integral of (baseline_OBSO - counterfactual_OBSO)
    over all cells where the difference is positive.

    Args:
        baseline_obso: (nx, ny) OBSO with all players.
        removed_obso: (nx, ny) OBSO with this player removed.
        grid_x, grid_y: cell center coordinates for area computation.

    Returns:
        Space created in square meters (positive = player adds value).
    """
    delta = baseline_obso - removed_obso
    # Cell area from grid spacing
    dx = grid_x[1, 0] - grid_x[0, 0] if grid_x.shape[0] > 1 else 1.0
    dy = grid_y[0, 1] - grid_y[0, 0] if grid_y.shape[1] > 1 else 1.0
    cell_area = dx * dy
    # Sum positive contributions only (space created, not destroyed)
    return float(np.sum(np.maximum(delta, 0)) * cell_area)


def compute_frame_space_creation(
    players_df: pd.DataFrame,
    transition_grid: np.ndarray,
    epv_grid: np.ndarray,
    ball_position: tuple[float, float],
    params: SpaceCreationParams | None = None,
) -> pd.DataFrame:
    """Compute space creation for all players in a single frame.

    Returns DataFrame with columns:
        player_id, team, space_created_m2, space_destroyed_m2, net_space_m2
    """
    from analytics.obso import compute_obso_surface, interpolate_grid
    from analytics.pitch_control import compute_pitch_control_player_removal

    params = params or SpaceCreationParams()

    # Build target grid — (ny, nx) convention matching OBSO
    x_coords = np.linspace(0, params.pitch_length, params.grid_cells_x)
    y_coords = np.linspace(0, params.pitch_width, params.grid_cells_y)
    # meshgrid with indexing="ij" produces (nx, ny) — we need (ny, nx) for OBSO
    grid_y, grid_x = np.meshgrid(y_coords, x_coords, indexing="ij")  # (ny, nx)
    targets = np.column_stack([grid_x.ravel(), grid_y.ravel()])

    # Compute N+1 pitch control surfaces
    baseline_pc, removed_pc = compute_pitch_control_player_removal(
        players_df, targets, None,
    )

    # Resize grids to match OBSO dimensions — (ny, nx) convention
    target_shape = (params.grid_cells_y, params.grid_cells_x)
    reachability_resized = interpolate_grid(transition_grid, target_shape)
    epv_resized = interpolate_grid(epv_grid, target_shape)

    # Baseline OBSO — reshape to (ny, nx)
    baseline_obso = compute_obso_surface(
        baseline_pc.reshape(params.grid_cells_y, params.grid_cells_x),
        reachability_resized, epv_resized,
        ball_position, grid_x, grid_y,
    )

    # Per-player space creation
    dx = x_coords[1] - x_coords[0] if len(x_coords) > 1 else 1.0
    dy = y_coords[1] - y_coords[0] if len(y_coords) > 1 else 1.0
    cell_area = dx * dy

    results = []
    for i, row in enumerate(players_df.itertuples()):
        removed_obso = compute_obso_surface(
            removed_pc[i].reshape(params.grid_cells_y, params.grid_cells_x),
            reachability_resized, epv_resized,
            ball_position, grid_x, grid_y,
        )
        delta = baseline_obso - removed_obso

        created = float(np.sum(np.maximum(delta, 0)) * cell_area)
        destroyed = float(np.sum(np.minimum(delta, 0)) * cell_area)

        results.append({
            "player_id": row.player_id,
            "team": row.team,
            "space_created_m2": created,
            "space_destroyed_m2": abs(destroyed),
            "net_space_m2": created + destroyed,
        })

    return pd.DataFrame(results)
```

- [ ] **Step 2: Create `src/tests/test_space_creation.py`**

```python
"""Tests for space creation (D14)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

try:
    import jax
    _HAS_JAX = True
except ImportError:
    _HAS_JAX = False

from analytics.space_creation import (
    SpaceCreationParams,
    compute_space_created,
)


class TestComputeSpaceCreated:
    def test_identical_surfaces_zero_creation(self):
        """No space created if removing player has no effect."""
        surface = np.random.default_rng(42).random((10, 10))
        grid_x, grid_y = np.meshgrid(np.linspace(0, 120, 10), np.linspace(0, 80, 10), indexing="ij")
        result = compute_space_created(surface, surface, grid_x, grid_y)
        assert result == 0.0

    def test_higher_baseline_means_positive_creation(self):
        """Player creates space when baseline OBSO > counterfactual."""
        baseline = np.full((10, 10), 0.5)
        removed = np.full((10, 10), 0.3)
        grid_x, grid_y = np.meshgrid(np.linspace(0, 120, 10), np.linspace(0, 80, 10), indexing="ij")
        result = compute_space_created(baseline, removed, grid_x, grid_y)
        assert result > 0

    def test_lower_baseline_means_zero_creation(self):
        """Only positive differences count as space creation."""
        baseline = np.full((10, 10), 0.3)
        removed = np.full((10, 10), 0.5)
        grid_x, grid_y = np.meshgrid(np.linspace(0, 120, 10), np.linspace(0, 80, 10), indexing="ij")
        result = compute_space_created(baseline, removed, grid_x, grid_y)
        assert result == 0.0


@pytest.mark.skipif(not _HAS_JAX, reason="JAX not installed")
class TestFrameSpaceCreation:
    """Integration tests requiring JAX for vmap batched PC."""

    def test_all_players_get_values(self):
        from analytics.space_creation import compute_frame_space_creation

        # Minimal synthetic setup
        players = pd.DataFrame({
            "player_id": ["p1", "p2", "p3", "p4"],
            "team": ["home", "home", "away", "away"],
            "x": [30.0, 50.0, 70.0, 90.0],
            "y": [40.0, 40.0, 40.0, 40.0],
            "velocity_x": [1.0, 0.0, -1.0, 0.0],
            "velocity_y": [0.0, 0.0, 0.0, 0.0],
        })
        transition = np.random.default_rng(42).random((64, 100))
        epv = np.random.default_rng(42).random((32, 50))
        params = SpaceCreationParams(grid_cells_x=20, grid_cells_y=14)  # small for test speed

        result = compute_frame_space_creation(
            players, transition, epv, (50.0, 40.0), params,
        )
        assert len(result) == 4
        assert set(result.columns) == {"player_id", "team", "space_created_m2", "space_destroyed_m2", "net_space_m2"}
        assert all(result["space_created_m2"] >= 0)
        assert all(result["space_destroyed_m2"] >= 0)
```

---

### Task 13: D14 — Space Creation HF Jobs Script

Batch computation of per-player space creation on HF Jobs A10G.

**Files:**
- Create: `scripts/compute_space_creation_hf.py`

- [ ] **Step 1: Create `scripts/compute_space_creation_hf.py`**

PEP 723 header:
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
#     "scipy>=1.11.0",
# ]
# ///
```

HF Jobs invocation:
```
hf jobs uv run scripts/compute_space_creation_hf.py \
    --flavor a10g --timeout 120m \
    --secrets HF_TOKEN=$HF_TOKEN
```

**Pipeline:**
1. Download tracking data from `luxury-lakehouse/pitch-control-tracking`
2. Download trained grids from `luxury-lakehouse/obso-trained-grids` (D23)
3. For each match, for each frame (sampled at 5fps for initial run, 25fps later):
   - Build player DataFrame from tracking
   - `compute_pitch_control_player_removal()` via `vmap` — one GPU dispatch per frame
   - Compute baseline + per-player OBSO surfaces
   - Compute `space_created_m2` per player per frame
4. Aggregate: per-player-per-match mean space created
5. Publish to `luxury-lakehouse/space-creation-values`

**Frame sampling:** Start at 5fps (every 5th frame) for the initial run. At 5fps × 90min × 7 matches × 23 variants:
- `5 × 90 × 60 × 7 × 23 = 4.35M` surface evaluations
- With `vmap` batching 23 variants per GPU call: `5 × 90 × 60 × 7 = ~189K` GPU calls
- Estimated: ~30-60 min on A10G

**Output schema:**
```
match_id, frame_id, player_id, team, space_created_m2, space_destroyed_m2, net_space_m2
```

---

## Chunk 5: Finalization

### Task 14: Documentation and TODO Updates

**Files:**
- Modify: `TODO.md` — move E5, O2, D23, D17, D14, U4p to completed
- Modify: `ROADMAP.md` — update Space Creation, DL Infrastructure, HF Hub Tier 3 status
- Modify: `NOTICE` — add citations (Zaheer et al. 2017, Gal & Ghahramani 2016, Fernandez & Bornn 2018)
- Modify: `pyproject.toml` — add `[training]` optional extra with `torch>=2.0` for local dev

- [ ] **Step 1: Update `pyproject.toml`**

Add new entry points if needed (no new Spark entry points — all computation is HF Jobs scripts).

Add optional extra (local dev only — NOT installed in CI or on Databricks. HF Jobs scripts use PEP 723 inline dependencies):
```toml
[project.optional-dependencies]
training = ["torch>=2.0"]
```

Do NOT add `--extra training` to CI install commands in `.github/workflows/python-ci.yml`.

- [ ] **Step 2: Update TODO.md**

Move E5, O2, D23, D17 (+ U4p), D14 from "On Deck" to "Completed On-Deck Items" with resolution notes.

- [ ] **Step 3: Update ROADMAP.md**

- Space Creation: "Research direction" → "Implemented — D14 batch on HF Jobs, `vmap`-batched PC"
- DL Infrastructure: Update Tier 3 GPU training status (proven with xG v2)
- HF Hub: Tier 3 complete (xG v2 + VAEP trained on HF Jobs)

- [ ] **Step 4: Add academic citations to NOTICE**

```
Space Creation Quantification
Fernandez, J. & Bornn, L. (2018). "Wide Open Spaces: A statistical technique
for measuring space creation in professional soccer." MIT Sloan Sports Analytics Conference.

Deep Sets (Set Encoder Architecture)
Zaheer, M., Kottur, S., Ravanbakhsh, S., Poczos, B., Salakhutdinov, R., & Smola, A. (2017).
"Deep Sets." Advances in Neural Information Processing Systems (NeurIPS).

MC Dropout Uncertainty Quantification
Gal, Y. & Ghahramani, Z. (2016). "Dropout as a Bayesian Approximation:
Representing Model Uncertainty in Deep Learning." ICML.
```

- [ ] **Step 5: Run full test suite**

```bash
uv run ruff check src/
uv run ruff format --check src/
uv run pyright src/
uv run pytest src/tests/ -v
```

All must pass with zero violations.

---

## Dependency Graph

```
E5 (versioning) ─────────────────────────────┐
                                               │
O2 (VAEP → HF Jobs) ──── D23 (trained grids) ─┼── D14 (Space Creation)
                                               │
D17 + U4p (xG v2) ───────────────────────────┘
```

- E5 is wired into every new training script from the start
- O2 must complete first (establishes HF Jobs training pattern, validates SPADL dataset)
- D23 depends on O2's dataset being validated
- D17 is independent after O2 pattern is proven (parallel with D23)
- D14 depends on D23 (trained grids) and the `vmap` kernel (Task 11)
- U4p is folded into D17 (MC dropout in set encoder)

## Execution Order

1. **Task 1** (E5 versioning) — quick, retroactive
2. **Tasks 2-3** (O2 VAEP) — establishes HF Jobs pattern
3. **Tasks 4-5** (D23 grids) — improves OBSO quality
4. **Tasks 6-10** (D17 xG v2) — can parallel with D23 after Task 2
5. **Tasks 11-13** (D14 Space Creation) — depends on D23 + vmap kernel
6. **Task 14** (finalization) — docs, TODO, tests
