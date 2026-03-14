# Predictive Models (D8, D6, D5) Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace static xT grid with data-driven Delta table, train and deploy custom xG models, and integrate OpenSTARLab LEM_3 event prediction — all with full E2E deployment to Databricks, dbt, Lakebase, and Streamlit.

**Architecture:** Three independent ML features sharing common infrastructure patterns: frozen dataclass configs in `src/analytics/`, batch pipelines in `src/ingestion/`, training notebooks in `notebooks/`, dbt staging→gold with feature toggles, and Streamlit pages reading from Lakebase synced tables. D6 uses `applyInPandas` executor-side scoring (sklearn+xgboost ~50 MB). D5 uses driver-side per-match inference (PyTorch ~800 MB exceeds 1 GB UDF cap).

**Tech Stack:** Python 3.10, NumPy, pandas, scikit-learn, XGBoost 3.2.0, PyTorch 2.4.1, OpenSTARLab (openstarlab-event, openstarlab-preprocessing), PySpark, dbt-databricks, Terraform, Streamlit, psycopg2, HuggingFace Hub

**Spec:** `docs/superpowers/specs/2026-03-13-predictive-models-design.md`

**Commit policy:** NO intermediate commits. Single commit after all code is complete and all local tests pass. Deployment and E2E testing happen before commit. User must approve before commit.

---

## Chunk 1: D8 — Dynamic xT Grid

### Task 1.1: Refactor `expected_threat.py` — Remove CSV side-effect, add `replaceWhere`, add validation

**Files:**
- Modify: `src/ingestion/expected_threat.py`
- Modify: `src/tests/test_expected_threat.py`

- [ ] **Step 1: Add pipeline validation tests**

Add to `src/tests/test_expected_threat.py`:

```python
from analytics.expected_threat import validate_xt_grid

class TestGridValidation:
    """Tests for pipeline-level data quality checks."""

    def test_validate_grid_passes_valid_grid(self) -> None:
        grid = np.array([[0.01 * (x + 1) for y in range(8)] for x in range(12)])
        validate_xt_grid(grid)  # Should not raise

    def test_validate_grid_rejects_out_of_range(self) -> None:
        grid = np.full((12, 8), 0.1)
        grid[0, 0] = 0.0001  # Below 0.001 lower bound
        with pytest.raises(ValueError, match="out of expected range"):
            validate_xt_grid(grid)

    def test_validate_grid_rejects_wrong_shape(self) -> None:
        grid = np.zeros((10, 6))
        with pytest.raises(ValueError, match="shape"):
            validate_xt_grid(grid)

    def test_validate_grid_checks_monotonicity(self) -> None:
        # Reversed gradient: high values at x=0, low at x=11 — fails monotonicity
        grid = np.array([[0.3 - 0.02 * x for y in range(8)] for x in range(12)])
        with pytest.raises(ValueError, match="monoton"):
            validate_xt_grid(grid)
```

Run: `uv run pytest src/tests/test_expected_threat.py::TestGridValidation -v`
Expected: FAIL — `validate_xt_grid` not yet defined in analytics module

- [ ] **Step 2: Implement `validate_xt_grid()` in analytics module**

Add to `src/analytics/expected_threat.py`:

```python
def validate_xt_grid(grid: np.ndarray, params: ExpectedThreatParams | None = None) -> None:
    """Validate computed xT grid meets data quality requirements."""
    if params is None:
        params = ExpectedThreatParams()
    expected_shape = (params.n_zones_x, params.n_zones_y)
    if grid.shape != expected_shape:
        msg = f"Grid shape {grid.shape} != expected {expected_shape}"
        raise ValueError(msg)
    if grid.min() < 0.001 or grid.max() > 0.50:
        msg = f"Grid values out of expected range [0.001, 0.50]: min={grid.min():.4f}, max={grid.max():.4f}"
        raise ValueError(msg)
    row_means = grid.mean(axis=1)
    if not np.all(np.diff(row_means) >= -0.01):
        msg = "Grid row means not approximately monotonically increasing left-to-right"
        raise ValueError(msg)
```

Run: `uv run pytest src/tests/test_expected_threat.py::TestGridValidation -v`
Expected: PASS

- [ ] **Step 3: Refactor `run_pipeline()` in ingestion module**

Modify `src/ingestion/expected_threat.py`:
1. Import `validate_xt_grid` from `analytics.expected_threat`
2. Call `validate_xt_grid(grid)` after each `compute_expected_threat_grid()` call
3. Change `write_delta_table()` to use `replace_where=f"competition_id = '{comp_id}'"` instead of `mode="overwrite"`
4. Delete the CSV export block (lines ~138–143 that write to `dbt_project/seeds/`)
5. For the first write (table may not exist), catch `AnalysisException` and fall back to `mode="overwrite"` with `overwriteSchema=True`

- [ ] **Step 4: Run existing tests to verify no regressions**

Run: `uv run pytest src/tests/test_expected_threat.py -v`
Expected: All existing + new tests PASS

### Task 1.2: Delete CSV seed and dbt seed tests

**Files:**
- Delete: `dbt_project/seeds/expected_threat_grid.csv`
- Modify: `dbt_project/seeds/_seeds__schema.yml` (remove `expected_threat_grid` section, lines ~58–93)

- [ ] **Step 1: Delete the CSV seed file**

```bash
rm dbt_project/seeds/expected_threat_grid.csv
```

- [ ] **Step 2: Remove seed schema entry from `_seeds__schema.yml`**

Remove the entire `expected_threat_grid` block (name, description, data_tests, columns) from `dbt_project/seeds/_seeds__schema.yml`.

- [ ] **Step 3: Verify dbt parse still passes**

Run: `MSYS_NO_PATHCONV=1 uv run python -c "import subprocess; subprocess.run(['dbt', 'parse', '--profiles-dir', '.'], cwd='dbt_project', check=True)"`
Expected: PASS (no references to the deleted seed remain)

### Task 1.3: Rewrite `off_ball_xt.py` grid loading

**Files:**
- Modify: `src/ingestion/off_ball_xt.py` (lines 49–85)
- Modify: `src/tests/test_off_ball_xt.py`

- [ ] **Step 1: Add test for new grid loading**

Add to the existing test file:

```python
from unittest.mock import MagicMock, patch
from collections import namedtuple

class TestLoadXtGridFromSpark:
    def test_loads_from_delta_table(self) -> None:
        Row = namedtuple("Row", ["zone_x", "zone_y", "xt_value"])
        mock_rows = [Row(x, y, 0.01 * (x + 1)) for x in range(12) for y in range(8)]
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = mock_rows
        grid = _load_xt_grid_from_spark(mock_spark, "soccer_analytics")
        assert grid.shape == (12, 8)
        assert grid[0, 0] == pytest.approx(0.01)
        assert grid[11, 7] == pytest.approx(0.12)

    def test_raises_on_missing_table(self) -> None:
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Table not found")
        with pytest.raises(RuntimeError, match="Run compute_expected_threat"):
            _load_xt_grid_from_spark(mock_spark, "soccer_analytics")
```

- [ ] **Step 2: Rewrite `_load_xt_grid_from_spark()`**

Replace both `_load_xt_grid()` and `_load_xt_grid_from_spark()` with a single function:

```python
def _load_xt_grid_from_spark(
    spark: SparkSession, catalog: str, schema: str = "bronze"
) -> np.ndarray:
    """Load xT grid from the expected_threat_grids Delta table.

    Reads the global grid from {catalog}.{schema}.expected_threat_grids.
    Raises RuntimeError if the table does not exist — run compute_expected_threat first.
    """
    table = f"{catalog}.{schema}.expected_threat_grids"
    try:
        rows = (
            spark.sql(
                f"SELECT zone_x, zone_y, xt_value FROM {table} "
                "WHERE competition_id = 'global'"
            )
            .collect()
        )
    except Exception as exc:
        msg = (
            f"xT grid table {table} not found. "
            "Run compute_expected_threat pipeline first."
        )
        raise RuntimeError(msg) from exc

    grid = np.zeros((12, 8))
    for row in rows:
        grid[int(row.zone_x), int(row.zone_y)] = float(row.xt_value)
    return grid
```

Delete the old `_load_xt_grid()` function entirely (CSV/Volume fallback paths).

- [ ] **Step 3: Update callers**

The call site in `run_pipeline()` already calls `_load_xt_grid_from_spark(spark, catalog)` — the signature is unchanged. The only changes are: (1) delete `_load_xt_grid()` entirely, and (2) remove the CSV fallback call inside `_load_xt_grid_from_spark()`. The new function reads from `{catalog}.bronze.expected_threat_grids` (hardcoded `bronze` schema, matching where `compute_expected_threat` writes).

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_off_ball_xt.py -v`
Expected: PASS

### Task 1.4: Add Terraform task

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Add `compute_expected_threat` task block**

Insert after `compute_spadl_vaep` task (after line ~167):

```hcl
  task {
    task_key        = "compute_expected_threat"
    timeout_seconds = 900
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_expected_threat"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "default"
  }
```

- [ ] **Step 2: Update `compute_off_ball_xt` to depend on `compute_expected_threat`**

**Note:** This adds `compute_expected_threat` → `compute_off_ball_xt` to the DAG. Since `compute_expected_threat` depends on `compute_spadl_vaep`, tracking tasks that finish early will wait for the xT grid computation before off-ball xT starts. This is an acceptable trade-off — the xT grid is a prerequisite for correct off-ball xT values.

Add `depends_on { task_key = "compute_expected_threat" }` to the existing `compute_off_ball_xt` task block, so the xT grid is computed before off-ball xT runs.

- [ ] **Step 3: Validate Terraform**

Run: `terraform -chdir=terraform/environments/dev validate`
Expected: Success

### Task 1.5: Run lint and type checks

- [ ] **Step 1: Ruff + Pyright**

Run: `uv run ruff check src/ingestion/expected_threat.py src/analytics/expected_threat.py src/ingestion/off_ball_xt.py && uv run ruff format --check src/ && uv run pyright src/ingestion/expected_threat.py src/analytics/expected_threat.py src/ingestion/off_ball_xt.py`
Expected: Zero violations

---

## Chunk 2: D6 — Custom xG Model

### Task 2.1: Add `play_pattern` to dbt mart

**Files:**
- Modify: `dbt_project/models/staging/statsbomb/stg_statsbomb__shots.sql`
- Modify: `dbt_project/models/intermediate/int_unified_shots.sql`
- Modify: `dbt_project/models/marts/fct_shots.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`

- [ ] **Step 0: Add `play_pattern` to `stg_statsbomb__shots.sql`**

`stg_statsbomb__shots.sql` has an explicit SELECT list (not `SELECT *`). Add `play_pattern` to its column list — this column exists in the upstream `stg_statsbomb__events` source that `stg_statsbomb__shots` reads from.

- [ ] **Step 1: Add `play_pattern` to StatsBomb branch of `int_unified_shots.sql`**

In the StatsBomb CTE (`statsbomb_shots AS (SELECT * FROM stg_statsbomb__shots)`), the `play_pattern` column is now available after Step 0. Add it to the final StatsBomb SELECT:
```sql
play_pattern,
```

- [ ] **Step 2: Add `CAST(NULL AS STRING) AS play_pattern` to Wyscout branch**

In the Wyscout SELECT (around line 39–76), add:
```sql
cast(null as string) as play_pattern,
```

- [ ] **Step 3: Add `play_pattern` to `fct_shots.sql`**

Pass through `play_pattern` from `int_unified_shots` in the final SELECT.

- [ ] **Step 4: Update `_marts__models.yml` contract**

Add to the `fct_shots` columns section:
```yaml
      - name: play_pattern
        data_type: string
        description: "Play pattern (e.g., Regular Play, From Corner). StatsBomb only; NULL for Wyscout."
```

- [ ] **Step 5: Validate dbt parse**

Run: `MSYS_NO_PATHCONV=1 uv run python -c "import subprocess; subprocess.run(['dbt', 'parse', '--profiles-dir', '.'], cwd='dbt_project', check=True)"`
Expected: PASS

### Task 2.2: Build `src/analytics/xg_model.py`

**Files:**
- Create: `src/analytics/xg_model.py`
- Create: `src/tests/test_xg_model.py`

- [ ] **Step 1: Write unit tests**

Create `src/tests/test_xg_model.py` with tests for:
- `TestXGModelConfig`: frozen dataclass, default values
- `TestBuildFeatures`: correct column output, handles NULL `play_pattern`, one-hot encoding of categoricals
- `TestTrainLogistic`: returns `CalibratedClassifierCV`, predictions in [0,1], handles small dataset
- `TestTrainXGBoost`: returns calibrated model, predictions in [0,1], serialization roundtrip
- `TestEvaluateModel`: returns dict with `brier_score`, `log_loss`, `roc_auc`, `calibration_error`
- `TestSerializeDeserializeXGBoost`: bytes roundtrip produces identical predictions
- `TestBenchmarkVsStatsBomb`: on synthetic data with a `statsbomb_xg` column, assert custom xG Brier score is within 10% of StatsBomb xG Brier score (spec requirement: "custom xG Brier score within 10% of StatsBomb xG on held-out set")

Use synthetic shot data (helper `_make_synthetic_shots(n=500)`) with realistic feature distributions. Include a `statsbomb_xg` column for benchmark tests.

Run: `uv run pytest src/tests/test_xg_model.py -v`
Expected: FAIL — module not found

- [ ] **Step 2: Implement `src/analytics/xg_model.py`**

Structure:
```python
"""Custom xG model — logistic regression baseline + gradient-boosted XGBoost."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import cross_val_predict
from xgboost import XGBClassifier

_CATEGORICAL_FEATURES = ["shot_body_part", "shot_technique", "shot_type", "play_pattern"]
_NUMERIC_FEATURES = [
    "distance_to_goal", "shot_angle", "location_x", "location_y",
    "end_location_x", "end_location_y", "period", "minute",
]
_BOOLEAN_FEATURES = ["is_first_time"]
_BASELINE_FEATURES = ["distance_to_goal", "shot_angle"]


@dataclass(frozen=True)
class XGModelConfig:
    features: tuple[str, ...] = tuple(_NUMERIC_FEATURES + _BOOLEAN_FEATURES + _CATEGORICAL_FEATURES)
    categorical_features: tuple[str, ...] = tuple(_CATEGORICAL_FEATURES)
    target: str = "is_goal"
    n_estimators: int = 100
    max_depth: int = 3
    learning_rate: float = 0.1
    calibration_method: str = "isotonic"
    test_size: float = 0.2
    random_state: int = 42


def build_features(
    shots_df: pd.DataFrame, config: XGModelConfig
) -> tuple[pd.DataFrame, pd.Series]:
    ...  # One-hot encode categoricals, fill NULL play_pattern with "Unknown"
    ...  # Return (X, y)


def train_logistic_baseline(
    X: pd.DataFrame, y: pd.Series, *, random_state: int = 42
) -> CalibratedClassifierCV:
    ...  # LogisticRegression on distance_to_goal + shot_angle only
    ...  # Fit base model first, then wrap with CalibratedClassifierCV(cv="prefit")
    ...  # cv="prefit" keeps single fitted estimator for clean serialization


def train_xgboost_model(
    X: pd.DataFrame, y: pd.Series, config: XGModelConfig | None = None
) -> CalibratedClassifierCV:
    ...  # XGBClassifier with config params
    ...  # Fit base model first, then wrap with CalibratedClassifierCV(cv="prefit")
    ...  # cv="prefit" keeps single fitted estimator for clean serialization


def evaluate_model(
    model: Any, X: pd.DataFrame, y: pd.Series
) -> dict[str, float]:
    ...  # brier_score, log_loss, roc_auc, calibration_error (ECE)


def serialize_xgboost_model(model: CalibratedClassifierCV) -> bytes:
    """Serialize calibrated XGBoost model to bytes for UDF closure transport.

    Avoids pickle (banned by CLAUDE.md). Serializes:
    - XGBoost booster via save_raw("json") → bytes
    - Isotonic calibrator state (f_, y_) as JSON arrays
    Combined into a single JSON envelope as UTF-8 bytes.
    """
    ...  # Use cv="prefit" in CalibratedClassifierCV to keep single fitted estimator
    ...  # Extract fitted XGBClassifier via model.calibrated_classifiers_[0].estimator
    ...  # Call .get_booster().save_raw("json") for the booster bytes
    ...  # Extract calibrator's f_ and y_ arrays from model.calibrated_classifiers_[0].calibrator
    ...  # Combine into {"booster": base64(raw), "calibrator": {"f": [...], "y": [...]}}
    ...  # Return json.dumps(envelope).encode("utf-8")


def deserialize_xgboost_model(model_bytes: bytes) -> CalibratedClassifierCV:
    """Deserialize bytes back to calibrated model on executor.

    Reconstructs XGBClassifier from booster JSON + isotonic calibrator
    from stored (f_, y_) arrays. No pickle involved.
    """
    ...
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_xg_model.py -v`
Expected: PASS

- [ ] **Step 4: Lint and type check**

Run: `uv run ruff check src/analytics/xg_model.py && uv run pyright src/analytics/xg_model.py`
Expected: Zero violations

### Task 2.3: Build `src/ingestion/xg_model.py`

**Files:**
- Create: `src/ingestion/xg_model.py`
- Add to `src/tests/test_xg_model.py`

- [ ] **Step 1: Write pipeline structure tests**

Add tests for:
- `TestXGPipeline`: skip guard returns early when all competitions scored, `_make_scoring_udf` creates callable, output schema matches expected columns

- [ ] **Step 2: Implement `src/ingestion/xg_model.py`**

Follow the `spadl_vaep.py` pattern exactly:

```python
"""Batch xG scoring pipeline — executor-side inference via applyInPandas."""

from __future__ import annotations

_TABLE_NAME = "xg_predictions"


def _make_scoring_udf(
    logistic_bytes: bytes, xgboost_bytes: bytes
) -> object:
    """Build applyInPandas UDF with model bytes in closure."""
    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import pandas as _pd
        from analytics.xg_model import build_features, deserialize_xgboost_model, XGModelConfig

        if not hasattr(_udf, "_model_cache"):  # type: ignore[attr-defined]
            _udf._model_cache = {}  # type: ignore[attr-defined]
        cache = _udf._model_cache  # type: ignore[attr-defined]
        if "logistic" not in cache:
            cache["logistic"] = deserialize_xgboost_model(logistic_bytes)
            cache["xgboost"] = deserialize_xgboost_model(xgboost_bytes)

        config = XGModelConfig()
        X, _ = build_features(pdf, config)
        return _pd.DataFrame({
            "shot_id": pdf["shot_id"],
            "match_id": pdf["match_id"],
            "competition_id": pdf["competition_id"],
            "xg_logistic": cache["logistic"].predict_proba(X)[:, 1],
            "xg_gradient_boosted": cache["xgboost"].predict_proba(X)[:, 1],
        })
    return _udf


def run_pipeline(spark, catalog, schema, log) -> None:
    # 1. Incremental skip guard on competition_id
    # 2. Load model bytes from UC Volume
    # 3. Load fct_shots as Spark DF
    # 4. groupBy("competition_id").applyInPandas(udf, schema)
    # 5. write_delta_table with replaceWhere per competition_id


def main() -> None:
    args = parse_ingestion_args("Score shots with custom xG models")
    log = configure_logging("xg_model")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, log)
```

- [ ] **Step 3: Register entry point in `pyproject.toml`**

Add to `[project.scripts]`:
```toml
compute_xg_model = "ingestion.xg_model:main"
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest src/tests/test_xg_model.py -v && uv run ruff check src/ingestion/xg_model.py && uv run pyright src/ingestion/xg_model.py`
Expected: PASS, zero violations

### Task 2.4: Create training notebook and HF model card

**Files:**
- Create: `notebooks/train_xg_model.py`
- Create: `docs/huggingface/xg-model-card.md`

- [ ] **Step 1: Write HF Hub model card**

Create `docs/huggingface/xg-model-card.md` following the template established in Phase 15 (`docs/huggingface/model-card.md`). Include:
- Model description: logistic regression baseline + calibrated XGBoost for xG
- Training data provenance: StatsBomb open data + Wyscout Figshare (~131K shots)
- Features: all 13 features (8 numeric + 1 boolean + 4 categorical)
- Coordinate system: StatsBomb 120x80
- Evaluation metrics: Brier score, log loss, ROC-AUC, calibration error
- Comparison vs StatsBomb xG
- Reproduction steps (Databricks notebook path, UC Volume model weights path)
- License: MIT

- [ ] **Step 2: Write training notebook**

Follow the `notebooks/train_football2vec.py` pattern (Databricks notebook with `# MAGIC` markers):

1. Load `fct_shots` via `spark.sql().toPandas()` (131K rows)
2. `build_features()` with full config
3. Train-test split stratified by `competition_id` (80/20)
4. Train logistic baseline + XGBoost
5. Evaluate both: Brier score, log loss, ROC-AUC
6. Print comparison vs `statsbomb_xg` on StatsBomb shots
7. Assert custom xG Brier score within 10% of StatsBomb xG (fail loudly if not)
8. Save model bytes + config JSON to `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model/`
9. HF Hub publish block: upload model weights + `docs/huggingface/xg-model-card.md` as `README.md` (try/except with `dbutils.secrets.get(scope="hf", key="token")`)

### Task 2.5: dbt models for xG predictions

**Files:**
- Create: `dbt_project/models/staging/xg/stg_xg__predictions.sql`
- Create: `dbt_project/models/staging/xg/_stg_xg__models.yml`
- Create: `dbt_project/models/marts/fct_xg_predictions.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`
- Modify: `dbt_project/dbt_project.yml`

- [ ] **Step 1: Add feature toggle**

Add to `dbt_project.yml` vars:
```yaml
  xg_model_enabled: false
```

- [ ] **Step 2: Create staging view**

`stg_xg__predictions.sql`:
```sql
{{ config(materialized='view') }}

select
    shot_id,
    match_id,
    competition_id,
    xg_logistic,
    xg_gradient_boosted,
    _ingested_at
from {{ source('bronze', 'xg_predictions') }}
```

Plus `_stg_xg__models.yml` with source definition.

- [ ] **Step 3: Create gold mart**

`fct_xg_predictions.sql`:
```sql
{{ config(
    materialized='table',
    enabled=var('xg_model_enabled', false),
    liquid_clustered_by=['match_id'],
    on_schema_change='fail',
    contract={'enforced': true}
) }}

select
    p.shot_id,
    p.match_id,
    p.competition_id,
    p.xg_logistic,
    p.xg_gradient_boosted
from {{ ref('stg_xg__predictions') }} p
inner join {{ ref('fct_shots') }} s on p.shot_id = s.shot_id
```

**Note:** The `INNER JOIN` to `fct_shots` validates referential integrity (spec requires this join). No additional columns are selected from `fct_shots` — the join is purely a guard against orphaned predictions.

- [ ] **Step 4: Add contract to `_marts__models.yml`**

Add `fct_xg_predictions` model with enforced contract: `shot_id` (string), `match_id` (bigint), `competition_id` (int), `xg_logistic` (double), `xg_gradient_boosted` (double).

- [ ] **Step 5: Validate dbt parse**

Run dbt parse to verify all models are valid.

### Task 2.6: Terraform task for xG scoring

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Add `compute_xg_model` task**

```hcl
  task {
    task_key        = "compute_xg_model"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_spadl_vaep"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_xg_model"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }
```

- [ ] **Step 2: Validate Terraform**

Run: `terraform -chdir=terraform/environments/dev validate`

### Task 2.7: Streamlit Shot Map enhancement

**Files:**
- Modify: `src/streamlit_app/pages/shot_map.py`

- [ ] **Step 1: Add xG model selector and comparison metrics**

Enhancements to `shot_map.py`:
1. Add query join to `fct_xg_predictions_synced` (LEFT JOIN on `shot_id`) when the table exists
2. Add `st.radio("xG Model", ["StatsBomb", "Custom (Logistic)", "Custom (XGBoost)"])` in sidebar
3. Use selected model's xG for dot sizing
4. Add summary metrics row: mean xG, total xG, Brier score vs actual `is_goal` — all three update when switching models
5. Wrap xG prediction columns in a try/except fallback (graceful degradation when synced table doesn't exist yet)

### Task 2.8: Lint and type check all D6 code

- [ ] **Step 1: Full check**

Run: `uv run ruff check src/analytics/xg_model.py src/ingestion/xg_model.py src/streamlit_app/pages/shot_map.py && uv run ruff format --check src/ && uv run pyright src/analytics/xg_model.py src/ingestion/xg_model.py`
Expected: Zero violations

---

## Chunk 3: D5 — OpenSTARLab LEM_3

### Task 3.1: Add `openstarlab` dependency group

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add optional dependency group**

Add to `[project.optional-dependencies]`:
```toml
openstarlab = [
    "openstarlab-event>=0.1.33",
    "openstarlab-preprocessing>=0.1.0",
    "torch==2.4.1",
]
```

- [ ] **Step 2: Register entry point**

Add to `[project.scripts]`:
```toml
compute_openstarlab = "ingestion.openstarlab:main"
```

- [ ] **Step 3: Sync dependencies**

Run: `uv sync --extra dev --extra analytics --extra app --extra embeddings`
(Do NOT install openstarlab locally — torch is too large. Tests will use `pytest.importorskip`.)

### Task 3.2: Build `src/analytics/openstarlab.py`

**Files:**
- Create: `src/analytics/openstarlab.py`
- Create: `src/tests/test_openstarlab.py`

- [ ] **Step 1: Write unit tests**

Tests for:
- `TestLEM3Config`: frozen dataclass, defaults
- `TestPrepareEvents`: converts our event schema columns to OpenSTARLab format, handles StatsBomb and Wyscout coordinate systems (both already 120x80), produces required columns. **Note:** Use the plan's 3-parameter signature `prepare_events_for_openstarlab(events_df, data_source, min_max_dict)` — the spec's 2-parameter version is stale (missing `data_source`).
- `TestPrepareEventsEdgeCases`: empty DataFrame, single event, missing optional columns
- `TestRunLEM3InferenceBatch`: tests `run_lem3_inference_batch(model, events_df, config)` — **Note:** use the plan's object-based API, not the spec's file-path API `run_lem3_inference()` which is stale.

Guard all tests that import torch/openstarlab with `pytest.importorskip("torch")` or use mock-based tests that don't need the library.

- [ ] **Step 2: Implement `src/analytics/openstarlab.py`**

```python
"""OpenSTARLab LEM_3 event prediction — data preparation and inference wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class LEM3Config:
    model_name: str = "LEM"
    context_length: int = 3
    config_path: str | None = None
    batch_size: int = 64
    pitch_length: float = 120.0
    pitch_width: float = 80.0


# Column mapping from our schema to OpenSTARLab expected format
_SB_EVENT_TYPE_MAP: dict[str, str] = {
    "Pass": "pass", "Shot": "shot", "Dribble": "dribble",
    # ... full mapping
}

_WYSCOUT_EVENT_TYPE_MAP: dict[str, str] = {
    "Pass": "pass", "Shot": "shot",
    # ... full mapping
}


def prepare_events_for_openstarlab(
    events_df: pd.DataFrame,
    data_source: str,
    min_max_dict: dict[str, tuple[float, float]] | None = None,
) -> tuple[pd.DataFrame, dict[str, tuple[float, float]]]:
    """Convert events from our schema to OpenSTARLab CSV format."""
    ...


def run_lem3_inference_batch(
    model: Any,
    events_df: pd.DataFrame,
    config: LEM3Config,
) -> pd.DataFrame:
    """Run LEM_3 inference on a preprocessed event sequence for one match."""
    ...
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_openstarlab.py -v`
Expected: PASS (mock-based tests pass without torch)

### Task 3.3: Build `src/ingestion/openstarlab.py`

**Files:**
- Create: `src/ingestion/openstarlab.py`

- [ ] **Step 1: Write pipeline tests**

Tests for:
- Skip guard returns early when all matches processed
- Output DataFrame has correct schema
- `main()` function exists and is callable

- [ ] **Step 2: Implement driver-side per-match pipeline**

```python
"""OpenSTARLab LEM_3 batch inference — driver-side per-match processing.

PyTorch (~800 MB base import) exceeds the 1 GB UDF executor memory cap
on Databricks serverless. This pipeline processes matches on the driver
using per-match toPandas() with gc.collect() — a legitimate exception
to the "prefer executors" rule per CLAUDE.md decision hierarchy point (3).
"""

from __future__ import annotations

import gc

_TABLE_NAME = "openstarlab_predictions"


def run_pipeline(spark, catalog, schema, log) -> None:
    from analytics.openstarlab import LEM3Config, prepare_events_for_openstarlab

    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # Incremental skip guard
    existing: set[str] = set()
    try:
        existing = {
            str(row["match_id"])
            for row in spark.table(results_table).select("match_id").distinct().collect()
        }
    except Exception:
        log.info("No existing predictions table — processing all matches")

    # Get all match_ids with their competition_ids from events
    # Note: competition_id is NOT in staging events views — join to fct_match_summary
    all_matches_query = f"""
        SELECT DISTINCT CAST(e.match_id AS STRING) AS match_id,
               CAST(m.competition_id AS STRING) AS competition_id,
               'statsbomb' AS data_source
        FROM {catalog}.dev_silver.stg_statsbomb__events e
        JOIN {catalog}.dev_gold.fct_match_summary m ON e.match_id = m.match_id
        UNION ALL
        SELECT DISTINCT CAST(e.match_id AS STRING) AS match_id,
               CAST(m.competition_id AS STRING) AS competition_id,
               'wyscout' AS data_source
        FROM {catalog}.dev_silver.stg_wyscout__events e
        JOIN {catalog}.dev_gold.fct_match_summary m ON e.match_id = m.match_id
    """
    all_matches = spark.sql(all_matches_query).collect()
    new_matches = [(row["match_id"], row["competition_id"], row["data_source"])
                   for row in all_matches if str(row["match_id"]) not in existing]

    if not new_matches:
        log.info("All matches already have predictions — skipping")
        return

    log.info("Processing %d new matches", len(new_matches))

    # Load model once on driver
    try:
        from openstarlab_event import Event_Model
    except ImportError as exc:
        msg = "openstarlab-event not installed. Install with: pip install openstarlab-event"
        raise ImportError(msg) from exc

    model_dir = f"/Volumes/{catalog}/dev_gold/model_weights/openstarlab/lem3"
    # ... load model from model_dir

    config = LEM3Config()
    all_predictions: list[pd.DataFrame] = []

    for i, (match_id, competition_id, data_source) in enumerate(new_matches):
        # Bounded toPandas per match (~1K-5K rows)
        if data_source == "statsbomb":
            events_df = spark.sql(f"""
                SELECT event_id, match_id, '{competition_id}' AS competition_id,
                       event_type, location_x, location_y,
                       period, minute, second, player_id, team_id, index AS event_index,
                       '{data_source}' AS data_source
                FROM {catalog}.dev_silver.stg_statsbomb__events
                WHERE match_id = {match_id}
                ORDER BY period, index
            """).toPandas()
        else:
            events_df = spark.sql(f"""
                SELECT event_sk AS event_id, match_id, '{competition_id}' AS competition_id,
                       event_type, start_x AS location_x,
                       start_y AS location_y, period, event_sec, player_id, team_id,
                       event_sec AS event_index, '{data_source}' AS data_source
                FROM {catalog}.dev_silver.stg_wyscout__events
                WHERE match_id = {match_id}
                ORDER BY period, event_sec
            """).toPandas()

        if events_df.empty:
            continue

        # Preprocess and infer
        preprocessed, _ = prepare_events_for_openstarlab(events_df, data_source)
        predictions = run_lem3_inference_batch(model, preprocessed, config)
        all_predictions.append(predictions)

        # Memory management
        del events_df, preprocessed, predictions
        if (i + 1) % 50 == 0:
            gc.collect()
            log.info("Processed %d/%d matches", i + 1, len(new_matches))

    if not all_predictions:
        log.warning("No predictions generated")
        return

    combined = pd.concat(all_predictions, ignore_index=True)
    # Write to Delta with replaceWhere per competition_id (not per match_id — IN clause
    # would be impractically large). Group by competition_id and write each partition.
    result_df = spark.createDataFrame(combined)
    for comp_id in combined["competition_id"].unique():
        partition = result_df.filter(f"competition_id = '{comp_id}'")
        write_delta_table(partition, results_table,
                         replace_where=f"competition_id = '{comp_id}'")


def main() -> None:
    from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
    args = parse_ingestion_args("OpenSTARLab LEM_3 event prediction")
    log = configure_logging("openstarlab")
    spark = get_spark_session()
    run_pipeline(spark, args.catalog, args.schema, log)
```

- [ ] **Step 3: Run tests and lint**

Run: `uv run pytest src/tests/test_openstarlab.py -v && uv run ruff check src/ingestion/openstarlab.py src/analytics/openstarlab.py && uv run pyright src/ingestion/openstarlab.py src/analytics/openstarlab.py`

### Task 3.4: Create training notebook and HF model card

**Files:**
- Create: `notebooks/train_openstarlab.py`
- Create: `docs/huggingface/openstarlab-model-card.md`

- [ ] **Step 1: Write HF Hub model card**

Create `docs/huggingface/openstarlab-model-card.md` following the Phase 15 template. Include:
- Model description: LEM_3 (Large Events Model, 3-timestep context) — 3 independent MLPs for next-event prediction
- Training data provenance: StatsBomb open data + Wyscout Figshare (all events from both sources)
- Coordinate system: 120x80 (both sources normalized at staging)
- Output: predicted action type, location (x, y), time delta
- Evaluation metrics: prediction accuracy by action type, overall accuracy
- Limitations: no pre-trained weights available, trained from scratch
- Reproduction steps (Databricks notebook path, UC Volume model weights path)
- License: MIT

- [ ] **Step 2: Write training notebook**

Databricks notebook with `# MAGIC` markers:
1. `%pip install openstarlab-event openstarlab-preprocessing torch==2.4.1`
2. Load all events via `spark.sql().toPandas()` (both sources)
3. Preprocess through `prepare_events_for_openstarlab()`
4. Write preprocessed CSV to `/Volumes/soccer_analytics/dev_gold/model_weights/openstarlab/lem3/train_data.csv`
5. Configure LEM via YAML
6. `Event_Model("LEM", config_path).train()`
7. Evaluate on held-out matches
8. Save model + config + min_max_dict to UC Volume
9. HF Hub publish block: upload model weights + `docs/huggingface/openstarlab-model-card.md` as `README.md` (same try/except pattern as football2vec)

### Task 3.5: dbt models for event predictions

**Files:**
- Create: `dbt_project/models/staging/openstarlab/stg_openstarlab__predictions.sql`
- Create: `dbt_project/models/staging/openstarlab/_stg_openstarlab__models.yml`
- Create: `dbt_project/models/marts/fct_event_predictions.sql`
- Modify: `dbt_project/models/marts/_marts__models.yml`
- Modify: `dbt_project/dbt_project.yml`

- [ ] **Step 1: Add feature toggle**

Add to `dbt_project.yml` vars:
```yaml
  openstarlab_enabled: false
```

- [ ] **Step 2: Create staging view + source definition**

- [ ] **Step 3: Create gold mart with contract**

`fct_event_predictions.sql` — **row-level** mart (one row per predicted event), not aggregated. Columns: `event_id`, `match_id`, `competition_id`, `data_source`, `sequence_index`, `predicted_action_type`, `predicted_x`, `predicted_y`, `predicted_time_delta`, `actual_action_type`, `actual_x`, `actual_y`, `prediction_correct`. Streamlit aggregates on the fly for accuracy breakdowns and serves raw rows for "most surprising events" (lowest-probability outcomes). Row-level granularity serves both use cases without needing a second mart.

- [ ] **Step 4: Add contract to `_marts__models.yml`**

- [ ] **Step 5: Validate dbt parse**

### Task 3.6: Terraform task and environment

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Add `openstarlab` environment block**

```hcl
  environment {
    environment_key = "openstarlab"
    spec {
      client = "1"
      dependencies = concat(
        [var.wheel_path],
        [
          "openstarlab-event>=0.1.33",
          "openstarlab-preprocessing>=0.1.0",
          "torch==2.4.1",
        ]
      )
    }
  }
```

- [ ] **Step 2: Add `compute_openstarlab` task**

```hcl
  task {
    task_key        = "compute_openstarlab"
    timeout_seconds = 7200
    max_retries     = 1

    depends_on {
      task_key = "ingest_statsbomb"
    }
    depends_on {
      task_key = "ingest_wyscout"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_openstarlab"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "openstarlab"
  }
```

- [ ] **Step 3: Validate Terraform**

### Task 3.7: Streamlit Event Prediction page

**Files:**
- Create: `src/streamlit_app/pages/event_prediction.py`
- Modify: `src/streamlit_app/app.py`

- [ ] **Step 1: Create page**

New page showing:
- Per-match prediction accuracy breakdown by action type (bar chart) — aggregated from row-level `fct_event_predictions_synced` via SQL `GROUP BY`
- "Most surprising events" table — rows where `prediction_correct = false`, sorted by rarest actual action types (tactical novelty detection)
- Competition filter, match filter in sidebar
- Reads from `fct_event_predictions_synced` via `t()` helper (row-level mart — aggregate in Streamlit SQL queries, not in dbt)

- [ ] **Step 2: Register in `app.py`**

Add import at the top with other page imports:
```python
from streamlit_app.pages.event_prediction import page as event_prediction_page
```

Add to the pages list:
```python
st.Page(event_prediction_page, title="Event Prediction", icon=":material/psychology:", url_path="event-prediction")
```

### Task 3.8: Lint and type check all D5 code

- [ ] **Step 1: Full check**

Run: `uv run ruff check src/analytics/openstarlab.py src/ingestion/openstarlab.py src/streamlit_app/pages/event_prediction.py && uv run ruff format --check src/ && uv run pyright src/analytics/openstarlab.py src/ingestion/openstarlab.py src/streamlit_app/pages/event_prediction.py`

---

## Chunk 4: Integration, Deployment & E2E Testing

### Task 4.1: Full local test suite

- [ ] **Step 1: Run all tests**

Run: `uv run pytest src/tests/ -v --tb=short`
Expected: All tests PASS (existing + new)

- [ ] **Step 2: Run full lint + type check**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/`
Expected: Zero violations

### Task 4.2: Build and upload wheel

- [ ] **Step 1: Build wheel**

Run: `uv build`

- [ ] **Step 2: Upload to UC Volume**

```bash
databricks fs cp dist/luxury_lakehouse-0.1.0-py3-none-any.whl \
  dbfs:/Volumes/soccer_analytics/bronze/libs/luxury_lakehouse-0.1.0-py3-none-any.whl \
  --overwrite --profile OAUTH
```

### Task 4.3: Terraform apply

- [ ] **Step 1: Apply infrastructure changes**

Run: `AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev apply`

Verify: 3 new tasks created (`compute_expected_threat`, `compute_xg_model`, `compute_openstarlab`), 1 new environment (`openstarlab`), 1 modified task (`compute_off_ball_xt` depends on `compute_expected_threat`).

**WARNING:** Do NOT manually trigger `compute_xg_model` until Task 4.5 Step 1 (`fct_shots` rebuild with `--full-refresh`) is complete — `play_pattern` column must exist first.

### Task 4.4: D8 deployment — Run xT grid pipeline

- [ ] **Step 1: Run `compute_expected_threat` on Databricks**

Trigger the task manually or via the full workflow.

- [ ] **Step 2: Verify Delta table**

Query `soccer_analytics.bronze.expected_threat_grids`:
- Confirm multiple competition_ids + "global"
- Confirm global grid shows gradient (min ~0.005, max ~0.30)
- Confirm 96 rows per competition_id

- [ ] **Step 3: Re-run `compute_off_ball_xt`**

Trigger to pick up new grid values.

### Task 4.5: D6 deployment — dbt build + train + score

- [ ] **Step 1: Run dbt build for `fct_shots` changes**

Rebuild `fct_shots` to add `play_pattern` column. Full refresh required since the column is new and `on_schema_change: fail` is set:

```bash
MSYS_NO_PATHCONV=1 uv run python -c "import subprocess; subprocess.run(['dbt', 'build', '--select', 'stg_statsbomb__shots+', '--full-refresh', '--profiles-dir', '.'], cwd='dbt_project', check=True)"
```

- [ ] **Step 2: Recreate `fct_shots_synced`**

Follow 6-step procedure:
1. `AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev destroy -target='module.synced_tables.databricks_database_synced_database_table.fct_shots' -auto-approve`
2. Drop PG ghost via psycopg2: `DROP TABLE IF EXISTS dev_gold.fct_shots_synced CASCADE`
3. Recreate in Databricks UI (Catalog > `dev_gold.fct_shots` > Create synced table, project: `soccer-analytics-dev`, branch: `production`)
4. `AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev import 'module.synced_tables.databricks_database_synced_database_table.fct_shots' 'soccer_analytics.dev_gold.fct_shots_synced'`
5. `.venv/Scripts/python.exe scripts/create_indexes.py`
6. PG grants for SP `be66af99-...`

- [ ] **Step 3: Run training notebook**

Execute `notebooks/train_xg_model.py` on Databricks. Verify:
- Brier score, log loss, ROC-AUC printed
- Model artifacts saved to `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model/`
- HF Hub publish succeeds

- [ ] **Step 4: Run `compute_xg_model` pipeline**

Trigger the task. Verify `soccer_analytics.bronze.xg_predictions` table populated.

- [ ] **Step 5: dbt build for xG predictions**

Set `xg_model_enabled: true` in `dbt_project.yml`. Run dbt build for new models.

- [ ] **Step 6: Create `fct_xg_predictions_synced`**

1. Add `databricks_database_synced_database_table.fct_xg_predictions` resource block to `terraform/modules/synced_tables/main.tf` (with `lifecycle { ignore_changes = all }`)
2. Create synced table via Databricks UI (Catalog > `dev_gold.fct_xg_predictions` > Create synced table)
3. `AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev import 'module.synced_tables.databricks_database_synced_database_table.fct_xg_predictions' 'soccer_analytics.dev_gold.fct_xg_predictions_synced'`
4. Add `fct_xg_predictions_synced` index definitions to `scripts/create_indexes.py`: composite `(match_id, competition_id)`
5. `.venv/Scripts/python.exe scripts/create_indexes.py`
6. PG grants for SP `be66af99-...`

### Task 4.6: D5 deployment — train + infer

- [ ] **Step 1: Validate OpenSTARLab imports on serverless**

Run a quick notebook cell: `import openstarlab_event; import openstarlab_preprocessing` — verify no network calls at import time.

- [ ] **Step 2: Run training notebook**

Execute `notebooks/train_openstarlab.py` on Databricks. Verify:
- Preprocessing completes for both sources
- LEM_3 trains successfully
- Evaluation metrics printed
- Model saved to UC Volume
- HF Hub publish succeeds

- [ ] **Step 3: Run `compute_openstarlab` pipeline**

Trigger the task. Verify `soccer_analytics.bronze.openstarlab_predictions` populated.

- [ ] **Step 4: dbt build for event predictions**

Set `openstarlab_enabled: true`. Run dbt build.

- [ ] **Step 5: Create `fct_event_predictions_synced`**

1. Add `databricks_database_synced_database_table.fct_event_predictions` resource block to `terraform/modules/synced_tables/main.tf` (with `lifecycle { ignore_changes = all }`)
2. Create synced table via Databricks UI (Catalog > `dev_gold.fct_event_predictions` > Create synced table)
3. `AWS_PROFILE=devops-agent terraform -chdir=terraform/environments/dev import 'module.synced_tables.databricks_database_synced_database_table.fct_event_predictions' 'soccer_analytics.dev_gold.fct_event_predictions_synced'`
4. Add `fct_event_predictions_synced` index definitions to `scripts/create_indexes.py`: composite `(match_id, competition_id)` + `(data_source, predicted_action_type)`
5. `.venv/Scripts/python.exe scripts/create_indexes.py`
6. PG grants for SP `be66af99-...`

### Task 4.7: Deploy Streamlit app

- [ ] **Step 1: Sync and deploy**

```bash
databricks sync . /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse --profile OAUTH
databricks apps deploy soccer-analytics-dashboard-dev \
  --source-code-path /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse \
  --profile OAUTH
```

### Task 4.8: E2E UI verification

- [ ] **Step 1: Verify Shot Map page**

Open app. Navigate to Shot Map. Verify:
- xG model selector appears (StatsBomb / Custom Logistic / Custom XGBoost)
- Dots resize when switching models
- Brier score metric shows for each model

- [ ] **Step 2: Verify Event Prediction page**

Navigate to Event Prediction. Verify:
- Prediction accuracy breakdown by action type renders
- "Most surprising events" table populated
- Competition and match filters work

- [ ] **Step 3: Verify existing pages unaffected**

Quick smoke test of all 11 existing pages — ensure no regressions from dbt changes.

### Task 4.9: User approval gate

- [ ] **Present results to user for approval before commit**

Show:
- Test counts (existing + new)
- Lint/type check results
- Deployed URLs and screenshots
- Model evaluation metrics (xG Brier score, LEM_3 accuracy)
- HF Hub model links
