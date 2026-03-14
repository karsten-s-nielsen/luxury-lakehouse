# Predictive Models — D5, D6, D8 Design Spec

**Branch:** `feature/predictive-models`
**Date:** 2026-03-13
**Items:** D8 (Dynamic xT Grid), D6 (Custom xG Model), D5 (OpenSTARLab LEM_3)

---

## D8: Dynamic xT Grid Seed Replacement

### Goal

Replace the static Karun Singh 12x8 xT seed with a data-driven Delta table computed from ~829K SPADL actions via Markov chain value iteration. The CSV seed is fully deprecated — the Delta table becomes the sole source of truth.

### Changes

1. **Terraform task** — Add `compute_expected_threat` to the workflow in `terraform/modules/workflows/main.tf`. Depends on `compute_spadl_vaep` (reads `fct_action_values`). Uses `environment_key = "default"` (no extra deps). Timeout: 900s.

2. **Refactor write to `replaceWhere`** — The existing `expected_threat.py` uses `mode="overwrite"` (full table overwrite). Refactor to use `replaceWhere` per `competition_id` for consistency with established patterns, even though the table is small (~1K rows).

3. **Remove CSV export side-effect** — Delete the code in `expected_threat.py` that writes to `dbt_project/seeds/expected_threat_grid.csv` when run locally. The Delta table is the sole source now.

4. **Add pipeline-level data quality validation** — After grid computation, assert: values span 0.005–0.35 range, monotonically increasing left-to-right on average (row means), shape is `(n_zones_x, n_zones_y)`. These checks replace the dbt seed tests being removed.

5. **Run pipeline on Databricks** — Writes per-competition + global grids to `{catalog}.bronze.expected_threat_grids`.

6. **Delete CSV seed and dbt seed tests** — Remove `dbt_project/seeds/expected_threat_grid.csv` and its entry in `_seeds__schema.yml` (unique combination, range checks). The dbt seed table `dev_silver.expected_threat_grid` will no longer be populated.

7. **Update `off_ball_xt.py` grid loading** — Rewrite `_load_xt_grid_from_spark()` to read from `{catalog}.bronze.expected_threat_grids WHERE competition_id = 'global'`. Remove the CSV fallback path entirely. If the Delta table doesn't exist, raise a clear error directing the user to run `compute_expected_threat` first.

8. **Re-run `compute_off_ball_xt`** — Off-ball xT values will change with the new grid.

### Output

- Delta table: `{catalog}.bronze.expected_threat_grids` (columns: `zone_x`, `zone_y`, `xt_value`, `competition_id`, `_ingested_at`)
- CSV seed deleted from repo
- Updated off-ball xT values downstream

### Testing

- Unit tests for rewritten `_load_xt_grid_from_spark()` (reads Delta, raises on missing table)
- Pipeline validation: gradient range assertion, monotonicity check
- E2E: run pipeline on Databricks, confirm Delta table populated, confirm off-ball xT pipeline picks up new grid

---

## D6: Custom xG Model

### Goal

Train competition-specific xG from ~131K shots. Logistic regression baseline + XGBoost gradient-boosted model. Publish model + predictions to HF Hub.

### Data

Source: `dev_gold.fct_shots` (131,077 rows). Target: `is_goal` (binary).

**Existing features (in mart):** `distance_to_goal`, `shot_angle`, `shot_body_part`, `shot_technique`, `shot_type`, `is_first_time`, `location_x`, `location_y`, `end_location_x`, `end_location_y`, `period`, `minute`.

**New feature to add:** `play_pattern` (Regular Play, From Corner, From Free Kick, etc.) — available in `stg_statsbomb__events`. **StatsBomb-only**: Wyscout events do not have `play_pattern`; the Wyscout branch of `int_unified_shots` will use `CAST(NULL AS STRING)`. The xG model must treat `play_pattern` as a nullable categorical feature (impute or use a dedicated "unknown" category for the ~25% Wyscout shots). Requires changes to `int_unified_shots` → `fct_shots` → `_marts__models.yml` contract. Recreate `fct_shots_synced` for the new column.

**Benchmark:** `statsbomb_xg` column for calibration comparison (not a training feature).

### Architecture

#### `src/analytics/xg_model.py` — Pure analytics module

```python
@dataclass(frozen=True)
class XGModelConfig:
    features: list[str]
    categorical_features: list[str]
    target: str = "is_goal"
    xgb_params: dict[str, Any]  # n_estimators, max_depth, learning_rate, etc.
    calibration_method: str = "isotonic"
    test_size: float = 0.2
    random_state: int = 42

def build_features(shots_df: pd.DataFrame, config: XGModelConfig) -> tuple[pd.DataFrame, pd.Series]
def train_logistic_baseline(X: pd.DataFrame, y: pd.Series) -> CalibratedClassifierCV
def train_xgboost_model(X: pd.DataFrame, y: pd.Series, config: XGModelConfig) -> CalibratedClassifierCV
def evaluate_model(model: Any, X: pd.DataFrame, y: pd.Series) -> dict[str, float]
    # Returns: brier_score, log_loss, roc_auc, calibration_error
def serialize_xgboost_model(model: CalibratedClassifierCV) -> bytes
    # XGBoost bytes-in-closure serialization (same pattern as spadl_vaep.py)
def deserialize_xgboost_model(model_bytes: bytes) -> CalibratedClassifierCV
```

No Spark imports. No HF imports. Pure pandas/numpy/sklearn/xgboost.

#### `src/ingestion/xg_model.py` — Batch scoring pipeline

Pattern: `applyInPandas` with model bytes captured in closure (executor-side scoring). sklearn + XGBoost are lightweight (~50 MB combined) — well within the 1 GB executor UDF cap. This is the same proven pattern as `spadl_vaep.py`.

```python
def run_pipeline(spark, catalog, schema, log) -> None:
    # 1. Incremental skip guard: check existing competition_ids in xg_predictions
    # 2. Load fct_shots as Spark DF (NOT toPandas — use executors)
    # 3. Load trained model bytes from UC Volume (read on driver, pass via closure)
    # 4. Build applyInPandas UDF with model bytes in closure
    # 5. Group by competition_id, score each partition on executors
    # 6. Write xg_predictions with replaceWhere per competition_id

def main() -> None:  # CLI entry point
```

**Output table:** `{catalog}.{schema}.xg_predictions` — columns: `shot_id` (FK), `match_id`, `competition_id`, `xg_logistic`, `xg_gradient_boosted`, `_ingested_at`.

Separate table (not added to `fct_shots`) to avoid additional synced table recreation beyond the `play_pattern` change.

#### `notebooks/train_xg_model.py` — Databricks training notebook

1. `spark.sql("SELECT * FROM soccer_analytics.dev_gold.fct_shots").toPandas()` — 131K rows, ~15 cols, well within 16 GB driver
2. Train-test split stratified by `competition_id`
3. Train logistic baseline + XGBoost with cross-validation
4. Calibrate both models (isotonic regression)
5. Evaluate: Brier score, log loss, ROC-AUC, calibration curve, comparison vs `statsbomb_xg`
6. Save artifacts to `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model/`
7. Publish to HF Hub `luxury-lakehouse/xg-model-statsbomb-wyscout`

#### dbt changes

1. Add `play_pattern` column to `int_unified_shots.sql` (from staging events)
2. Add `play_pattern` to `fct_shots.sql` + update `_marts__models.yml` contract
3. New staging view: `stg_xg__predictions.sql` (reads `bronze.xg_predictions`)
4. New gold mart: `fct_xg_predictions.sql` with contract (joins to `fct_shots` on `shot_id`)
5. New feature toggle: `xg_model_enabled: false` in `dbt_project.yml`

#### Streamlit — Shot Map enhancement

Add xG comparison column to the existing Shot Map page:
- New "Model" radio selector: StatsBomb xG / Custom xG (logistic) / Custom xG (XGBoost)
- Shot dots sized by selected xG value
- Summary metrics: mean xG, total xG, Brier score vs actual goals

#### Terraform

- New `compute_xg_model` task depending on `compute_spadl_vaep` (reads `fct_shots` which depends on dbt build after SPADL)
- Uses `environment_key = "analytics"` (includes `xgboost==3.2.0` and `scikit-learn` — same as `compute_spadl_vaep` and `compute_defcon_lite`)

#### Synced tables

- New synced table: `fct_xg_predictions_synced` (created via UI, imported into Terraform)
- PG indexes: composite `(match_id, competition_id)` on `fct_xg_predictions_synced`
- Recreate `fct_shots_synced` for the new `play_pattern` column (6-step procedure: Terraform destroy → PG drop → UI recreate → Terraform import → indexes → grants)

### Testing

- Unit tests: `build_features()`, `train_logistic_baseline()`, `train_xgboost_model()`, `evaluate_model()`, `serialize/deserialize`
- Integration: model trains on synthetic shot data, predictions are 0-1, calibration passes
- Benchmark: custom xG Brier score within 10% of StatsBomb xG on held-out set
- E2E: pipeline scores all shots on Databricks, dbt builds successfully, Shot Map shows custom xG

---

## D5: OpenSTARLab LEM_3 Event Prediction

### Goal

Train LEM_3 (Large Events Model, 3-timestep context) on StatsBomb + Wyscout event data. Run batch inference to generate next-event predictions for all matches. Publish model weights to HF Hub.

### Scope limitation

**LEM_3 only.** Seq2Event (transformer, heavier) deferred to D15. Reasons:
- Pre-trained weights are not available for any OpenSTARLab model — training from scratch is required
- LEM_3 (3 independent MLPs) is the lightest architecture and the best performer per the paper
- LEM_3 validates the full integration pipeline (preprocessing → training → inference → Delta → dbt → Streamlit)
- Seq2Event reuses the same preprocessing and infrastructure once LEM_3 is proven

### Dependencies

New `openstarlab` optional extra in `pyproject.toml`:
```toml
openstarlab = [
    "openstarlab-event>=0.1.33",
    "openstarlab-preprocessing>=0.1.0",
    "torch==2.4.1",
]
```

Pin `torch==2.4.1` for reproducibility (large, frequently-breaking dependency).

New Terraform `environment_key = "openstarlab"` with these deps + the wheel.

**Import-time network validation:** Before deploying, verify that `import openstarlab_event` and `import openstarlab_preprocessing` do not make network calls at import time. If they do, vendor required artifacts into UC Volume and configure library to read from local paths. Serverless executors have no internet access.

### Architecture

#### `src/analytics/openstarlab.py` — Pure analytics module

```python
@dataclass(frozen=True)
class LEM3Config:
    model_name: str = "LEM"
    context_length: int = 3
    config_path: str | None = None  # YAML config override
    batch_size: int = 64

def prepare_events_for_openstarlab(
    events_df: pd.DataFrame,
    min_max_dict: dict[str, tuple[float, float]] | None = None,
) -> tuple[pd.DataFrame, dict[str, tuple[float, float]]]
    # Convert from our event schema to OpenSTARLab CSV format
    # Returns (preprocessed_df, min_max_dict for normalization)

def run_lem3_inference(
    model_path: str,
    model_config_path: str,
    events_csv_path: str,
    min_max_dict_path: str,
) -> pd.DataFrame
    # Returns predictions: next_action_type, next_location_x, next_location_y, next_time_delta
```

#### `src/ingestion/openstarlab.py` — Batch inference pipeline

Pattern: **Driver-side per-match inference** with `del` + `gc.collect()`. This is a legitimate exception to the "prefer executors" rule because PyTorch's base import alone consumes ~600-800 MB, which exceeds the 1 GB UDF executor memory cap on serverless. Event data per match is small (a few thousand rows), so driver-side processing is safe. This follows CLAUDE.md's decision hierarchy point (3): per-partition `.toPandas()` as last resort when Spark executors cannot accommodate the workload.

```python
def run_pipeline(spark, catalog, schema, log) -> None:
    # 1. Load match_ids from stg_statsbomb__events + stg_wyscout__events
    # 2. Incremental skip guard: existing = {str(row["match_id"]) for row in ...}
    # 3. Load LEM_3 model + config from UC Volume (once, driver-side)
    # 4. For each new match_id:
    #    a. spark.sql("SELECT ... WHERE match_id = %s").toPandas()  (bounded, ~1K-5K rows)
    #    b. prepare_events_for_openstarlab(events_df)
    #    c. run_lem3_inference(model, preprocessed)
    #    d. Accumulate predictions DataFrame
    #    e. del events_df; gc.collect() every N matches
    # 5. spark.createDataFrame(all_predictions)
    # 6. Write openstarlab_predictions with replaceWhere per match_id IN (...)

def main() -> None:  # CLI entry point
```

**Why not `applyInPandas`:** PyTorch ~800 MB base import + model + tensors + pandas overhead exceeds the 1 GB hard cap per UDF executor on Databricks serverless. LEM_3 is 3 small MLPs but the PyTorch runtime itself is the bottleneck. sklearn/xgboost (D6) do not have this problem (~50 MB combined).

**Output table:** `{catalog}.{schema}.openstarlab_predictions` — columns: `event_id`, `match_id`, `competition_id`, `data_source`, `sequence_index`, `predicted_action_type`, `predicted_x`, `predicted_y`, `predicted_time_delta`, `actual_action_type`, `actual_x`, `actual_y`, `prediction_correct` (bool), `_ingested_at`.

#### `notebooks/train_openstarlab.py` — Databricks training notebook

1. Install `openstarlab-preprocessing` + `openstarlab-event` + PyTorch
2. Load StatsBomb + Wyscout events via `spark.sql().toPandas()` (driver-side, training is global)
3. Preprocess through OpenSTARLab's pipeline → CSV + min-max dict
4. Configure LEM via YAML config file
5. `Event_Model("LEM", config).train()` → saves model checkpoint
6. Evaluate on held-out matches
7. Save model + config + min-max dict to `/Volumes/soccer_analytics/dev_gold/model_weights/openstarlab/lem3/`
8. Publish to HF Hub `luxury-lakehouse/openstarlab-lem3-statsbomb-wyscout`

#### dbt changes

1. New staging view: `stg_openstarlab__predictions.sql` (reads `bronze.openstarlab_predictions`)
2. New gold mart: `fct_event_predictions.sql` with contract — aggregates prediction accuracy per match, per action type
3. Optionally: HPUS (possession utility) scores into `fct_match_summary` (if OpenSTARLab's `cal_HPUS()` proves useful)
4. New feature toggle: `openstarlab_enabled: false` in `dbt_project.yml`

#### Streamlit

New "Event Prediction" page or enhancement to existing Match Summary:
- Per-match prediction accuracy breakdown by action type
- "Most surprising events" — actions where the model assigned lowest probability to what actually happened (tactical novelty detection)

#### Terraform

- New `compute_openstarlab` task depending on `ingest_statsbomb` and `ingest_wyscout`
- Uses `environment_key = "openstarlab"`
- Timeout: 7200s (2 hr, conservative for first run with PyTorch on serverless)

#### Synced tables

- New synced table: `fct_event_predictions_synced`
- PG indexes: composite `(match_id, competition_id)` + `(data_source, predicted_action_type)`

### Testing

- Unit tests: `prepare_events_for_openstarlab()` format conversion, config validation
- Mock inference test with synthetic event sequences (no PyTorch dependency in CI — guard with `pytest.importorskip("torch")`)
- E2E: train on Databricks, run inference pipeline, confirm predictions written, dbt builds, Streamlit shows results

---

## Cross-Cutting Concerns

### Execution order

D8 → D6 → D5 (increasing complexity, each validates infrastructure the next needs)

### CI

- `openstarlab` extra added to CI install only if torch is manageable in CI runners (likely skip — use `pytest.importorskip`)
- `play_pattern` dbt change needs `dbt parse` validation in CI
- All new Python code must pass `ruff check`, `ruff format`, `pyright basic`, `pytest`

### HF Hub model cards

Both D6 and D5 publish to HF Hub. Each model repo must include a README.md model card following the template established in Phase 15 (`docs/huggingface/model-card.md`): methodology, training data provenance (StatsBomb open data, Wyscout Figshare), coordinate systems, evaluation metrics, and reproduction steps.

### Deployment sequence (per item)

1. Code changes (analytics module + ingestion pipeline + tests)
2. `uv build` → upload wheel to UC Volume
3. `terraform apply` (new tasks + environments)
4. Run training notebook on Databricks (D6, D5)
5. Run compute pipeline on Databricks
6. `dbt build` (new/modified models)
7. Recreate affected synced tables (UI → Terraform import)
8. `scripts/create_indexes.py` → PG grants
9. Deploy Streamlit app
10. Verify UI end-to-end
