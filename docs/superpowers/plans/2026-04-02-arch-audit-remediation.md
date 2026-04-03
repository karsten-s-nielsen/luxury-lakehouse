# Architecture Audit Remediation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remediate all 56 findings from the consolidated architecture audit (LAKEHOUSE-ARCH-AUDIT-CONSOLIDATED-V2.md), deprecate Streamlit, and enforce architectural boundaries with CI tooling.

**Architecture:** Four phases, each independently committable. Phase A clears the deck and builds shared foundations. Phase B restructures the ingestion layer (module splits, workflow coverage). Phase C integrates the wheel into the Taipy Docker image and restructures the app's data access layer. Phase D adds CI enforcement, ADRs, and remaining housekeeping.

**Tech Stack:** Python 3.10, uv, hatchling, Ruff, Pyright, pytest, import-linter, Databricks, dbt, Taipy 4.1, HF Spaces Docker

**Branch:** `refactor/arch-audit` (single feature branch, minimal commits with explicit user approval)

**Audit reference:** `D:\Development\LAKEHOUSE-ARCH-AUDIT-CONSOLIDATED-V2.md`

---

## Phase A — Foundation (independently committable)

Clears the Streamlit code, creates the shared constants package, and restores analytics layer purity. After this phase, the wheel is clean and ready for Taipy Docker integration.

### Task 1: Remove Streamlit Code

**Findings addressed:** A12, A31, CI3 (moot after removal)

**Files:**
- Delete: `src/streamlit_app/` (entire directory, 23 files, ~5,492 lines)
- Delete: `hf_streamlit_app_deprecated/` (entire directory, ~6,341 lines)
- Delete: `src/tests/test_streamlit_config.py`
- Delete: `src/tests/test_streamlit_components.py`
- Delete: `src/tests/test_streamlit_db.py`
- Modify: `src/tests/test_player_similarity.py` — remove `streamlit_app` import, relocate tested functions
- Modify: `src/tests/test_spadl_vaep.py` — remove `streamlit_app` chart imports
- Modify: `pyproject.toml` — remove `app` optional dependency group, remove `streamlit_app` from `known-first-party`
- Modify: `CLAUDE.md` — remove Streamlit retention notes and Streamlit UX Standards section

- [ ] **Step 1: Run existing tests to establish green baseline**

Run: `uv run pytest src/tests/ -x -q`
Expected: All tests pass (note any pre-existing failures).

- [ ] **Step 2: Delete Streamlit directories**

Delete the two directory trees:
- `src/streamlit_app/`
- `hf_streamlit_app_deprecated/`

- [ ] **Step 3: Delete pure Streamlit test files**

Delete:
- `src/tests/test_streamlit_config.py`
- `src/tests/test_streamlit_components.py`
- `src/tests/test_streamlit_db.py`

- [ ] **Step 4: Fix test_player_similarity.py**

This test imports `_format_vector_literal`, `_get_table_and_columns`, `_get_vector_column`, `_get_vector_dimension` from `streamlit_app.pages.player_similarity`. These are pure logic functions (vector formatting, column selection) — they don't depend on Streamlit.

The Taipy equivalent lives in `hf_taipy_app/src/state/player_similarity.py`. Check if the same functions exist there. If so, the test should be rewritten to test the Taipy state module's equivalents. If the functions are not present in the Taipy app, the logic is covered by the Taipy app's runtime behavior — delete the test functions that imported from Streamlit.

- [ ] **Step 5: Fix test_spadl_vaep.py**

This test imports `plot_action_type_breakdown` and `plot_action_value_timeline` from `streamlit_app.components.charts`. These are Streamlit-specific chart functions. Remove these imports and any test functions that call them. The core SPADL/VAEP pipeline tests in the same file do not depend on Streamlit — verify they still pass.

- [ ] **Step 6: Update pyproject.toml**

Remove the `app` optional dependency group:
```toml
# DELETE this entire block:
app = [
    "streamlit>=1.36.0",
    "psycopg2-binary>=2.9.9",
    "databricks-sdk>=0.20.0",
    "plotly>=5.18.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
]
```

Update `known-first-party` in `[tool.ruff.lint.isort]`:
```toml
# BEFORE:
known-first-party = ["analytics", "ingestion", "streamlit_app", "workflows"]
# AFTER:
known-first-party = ["analytics", "ingestion", "shared", "workflows"]
```

Note: `shared` is added here in anticipation of Task 2.

- [ ] **Step 7: Update CLAUDE.md**

Remove or update these sections:
- Line 16: Delete "Streamlit retained for reference" paragraph
- Lines 148-205: Delete or rename `## Streamlit UX Standards` section — the Taipy-relevant rules are already in `## UI Architecture`. Remove Streamlit-specific references (`st.metric`, `st.cache_data`, `show_spinner`). Keep framework-agnostic rules that apply to Taipy (already duplicated in the Taipy equivalents noted in the section).
- Line 13: Update to remove the Streamlit clause: "`src/workflows/` has zero Spark/Taipy imports"

- [ ] **Step 8: Update docs references**

In `docs/` files, Streamlit references are historical (plans, specs, decisions from the migration period). These are record-keeping — do NOT delete them. Only update forward-looking references:
- If any doc says "Streamlit retained for reference" or "Streamlit is the production app", update to reflect that Taipy is the sole production surface and Streamlit has been removed.
- `README.md`: Remove any Streamlit app links or setup instructions.

- [ ] **Step 9: Run full test + lint suite**

Run:
```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -x -q
```
Expected: All pass with zero violations. The 3 deleted test files and 2 fixed test files should result in a clean run.

---

### Task 2: Create Shared Constants Package

**Findings addressed:** A23, A26, A27, A36, A37

**Files:**
- Create: `src/shared/__init__.py`
- Create: `src/shared/constants.py`
- Create: `src/tests/test_shared_constants.py`
- Modify: `pyproject.toml` — add `shared` to wheel packages
- Modify: 14 files with `_IDENTIFIER_RE` duplication
- Modify: 8 files with `_GOLD_SCHEMA` duplication
- Modify: 4 files with hardcoded MLflow model FQNs
- Modify: Files with `"workflow_cost_live"` and `"observability"` literals

- [ ] **Step 1: Write tests for shared constants**

Create `src/tests/test_shared_constants.py`:
```python
"""Tests for shared constants and utility functions."""

import re

import pytest

from shared.constants import (
    COST_TABLE_NAME,
    DEFAULT_CATALOG,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_OBSERVABILITY_SCHEMA,
    IDENTIFIER_RE,
    mlflow_model_uri,
)


class TestIdentifierRe:
    """Verify the SQL identifier regex matches the existing pattern."""

    def test_valid_identifiers(self) -> None:
        for name in ("soccer_analytics", "dev_gold", "_private", "a1b2c3"):
            assert IDENTIFIER_RE.match(name), f"{name} should be valid"

    def test_invalid_identifiers(self) -> None:
        for name in ("1leading", "has space", "semi;colon", "", "has-dash"):
            assert not IDENTIFIER_RE.match(name), f"{name!r} should be invalid"

    def test_pattern_is_compiled(self) -> None:
        assert isinstance(IDENTIFIER_RE, re.Pattern)


class TestMlflowModelUri:
    """Verify MLflow model URI builder."""

    def test_builds_fqn(self) -> None:
        result = mlflow_model_uri("soccer_analytics", "dev_gold", "xg_model")
        assert result == "soccer_analytics.dev_gold.xg_model"

    def test_custom_catalog(self) -> None:
        result = mlflow_model_uri("prod_catalog", "gold", "vaep_model")
        assert result == "prod_catalog.gold.vaep_model"


class TestDefaults:
    """Verify default constant values match existing codebase conventions."""

    def test_default_catalog(self) -> None:
        assert DEFAULT_CATALOG == "soccer_analytics"

    def test_default_gold_schema(self) -> None:
        assert DEFAULT_GOLD_SCHEMA == "dev_gold"

    def test_observability_schema(self) -> None:
        assert DEFAULT_OBSERVABILITY_SCHEMA == "observability"

    def test_cost_table_name(self) -> None:
        assert COST_TABLE_NAME == "workflow_cost_live"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_shared_constants.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'shared'`

- [ ] **Step 3: Create shared constants module**

Create `src/shared/__init__.py`:
```python
"""Shared constants and utilities used across all packages."""
```

Create `src/shared/constants.py`:
```python
"""Cross-package constants and identifier validation.

This module has zero external dependencies — stdlib only.
It is safe to import from any package (analytics, ingestion, workflows)
and from the Taipy Docker image (via wheel install).
"""

import re

IDENTIFIER_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
"""SQL-safe identifier pattern. Use for catalog, schema, and table name validation."""

DEFAULT_CATALOG = "soccer_analytics"
DEFAULT_GOLD_SCHEMA = "dev_gold"
DEFAULT_OBSERVABILITY_SCHEMA = "observability"
COST_TABLE_NAME = "workflow_cost_live"


def mlflow_model_uri(catalog: str, schema: str, model_name: str) -> str:
    """Build a fully qualified MLflow Unity Catalog model URI."""
    return f"{catalog}.{schema}.{model_name}"
```

Update `pyproject.toml` — add to wheel packages:
```toml
[tool.hatch.build.targets.wheel]
packages = ["src/ingestion", "src/analytics", "src/shared", "src/workflows"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_shared_constants.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 5: Replace all _IDENTIFIER_RE duplications**

In each of these 14 files, replace the local `_IDENTIFIER_RE = re.compile(...)` with an import from `shared.constants`:

```python
from shared.constants import IDENTIFIER_RE
```

Files to update (replace `_IDENTIFIER_RE` with `IDENTIFIER_RE` throughout each file):
1. `src/ingestion/utils.py:26`
2. `src/ingestion/cost_hook.py:33`
3. `src/ingestion/export_shots_on_target.py:49`
4. `src/ingestion/import_obso_results.py:27`
5. `src/ingestion/import_psxg_predictions.py:19`
6. `src/ingestion/import_space_creation.py:23`
7. `src/ingestion/prepare_360_training_data.py:53`
8. `scripts/delete_synced_table.py:24`
9. `scripts/deploy_wheel.py:30`
10. `hf_taipy_app/src/config.py:11`
11. `hf_taipy_app/src/db.py:24`
12. `hf_taipy_app/src/filters.py:26`

Note: `src/streamlit_app/config.py:11` and `src/streamlit_app/db.py:25` are already deleted in Task 1.

In each file: remove the `import re` if it was only used for the regex compile, remove the `_IDENTIFIER_RE = re.compile(...)` line, add `from shared.constants import IDENTIFIER_RE`, and update all references from `_IDENTIFIER_RE` to `IDENTIFIER_RE`.

- [ ] **Step 6: Replace _GOLD_SCHEMA duplications**

In each of these 8 ingestion files, replace `_GOLD_SCHEMA = "dev_gold"` with:
```python
from shared.constants import DEFAULT_GOLD_SCHEMA
```

Files: `defcon_lite.py`, `export_embeddings_training_data.py`, `formations.py`, `model_validation.py`, `off_ball_xt.py`, `pausa.py`, `pitch_control_batch.py`, `player_embeddings.py`.

Update all usages from `_GOLD_SCHEMA` to `DEFAULT_GOLD_SCHEMA`.

- [ ] **Step 7: Replace hardcoded MLflow model FQNs**

In each of these 4 files, replace the hardcoded string with `mlflow_model_uri(catalog, schema, model_name)`:

1. `src/ingestion/defcon_lite.py:69` — `"soccer_analytics.dev_gold.defcon_model"` → `mlflow_model_uri(catalog, schema, "defcon_model")`
2. `src/ingestion/spadl_vaep.py:640` — `"soccer_analytics.dev_gold.vaep_model"` → `mlflow_model_uri(catalog, schema, "vaep_model")`
3. `src/ingestion/xg_model.py:94` — `"soccer_analytics.dev_gold.xg_model"` → `mlflow_model_uri(catalog, schema, "xg_model")`
4. `src/ingestion/xg_model_v2.py:79` — `"soccer_analytics.dev_gold.xg_model"` → `mlflow_model_uri(catalog, schema, "xg_model")`

Each function already has `catalog` and `schema` as parameters — use them.

- [ ] **Step 8: Replace workflow_cost_live and observability literals**

Replace bare string literals with imports from `shared.constants`:
- `"workflow_cost_live"` → `COST_TABLE_NAME` (2 locations in `cost_hook.py`)
- `"observability"` → `DEFAULT_OBSERVABILITY_SCHEMA` (4 locations)

- [ ] **Step 9: Run full test + lint suite**

Run: `uv run ruff check src/ scripts/ && uv run pyright src/ && uv run pytest src/tests/ -x -q`
Expected: All pass.

---

### Task 3: Restore Analytics Layer Purity — Move cost.py

**Findings addressed:** A9

**Files:**
- Move: `src/analytics/cost.py` → `src/ingestion/hf_jobs_cost.py`
- Modify: All import sites (grep for `from analytics.cost import`)
- Modify: `src/tests/test_cost_recorder.py` — update imports

- [ ] **Step 1: Identify all import sites**

Grep for `from analytics.cost import` and `from analytics import cost` across the entire codebase. Expected callers: `scripts/compute_epv_transition_hf.py`, `scripts/compute_space_creation_hf.py`, `src/ingestion/sync_hf_costs.py`, `src/tests/test_cost_recorder.py`, and any HF Jobs training scripts.

- [ ] **Step 2: Move the file**

Move `src/analytics/cost.py` to `src/ingestion/hf_jobs_cost.py`. Update the module docstring to reflect its new home.

- [ ] **Step 3: Update all import sites**

Change all `from analytics.cost import ...` to `from ingestion.hf_jobs_cost import ...`.

For HF Jobs PEP 723 scripts that install the wheel: the import path changes from `analytics.cost` to `ingestion.hf_jobs_cost`. Since these scripts install the wheel, both `analytics` and `ingestion` packages are available.

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_cost_recorder.py src/tests/test_sync_hf_costs.py -v`
Expected: PASS (imports updated).

Run: `uv run pytest src/tests/ -x -q` for full suite.

---

### Task 4: Restore Analytics Layer Purity — football2vec.py

**Findings addressed:** A10

**Files:**
- Modify: `src/analytics/football2vec.py` — remove `Doc2Vec.load()` and `open()` from the class
- Modify: `src/ingestion/player_embeddings.py` — caller handles I/O
- Modify: `src/tests/test_football2vec.py` — update to test without filesystem

- [ ] **Step 1: Refactor the MLflow PythonModel wrapper**

The `load_context` method (around line 268) currently calls `Doc2Vec.load()` and `open()`. Refactor: add a `from_loaded(model, tokenizer_config)` classmethod or `__init__` parameter that accepts pre-loaded objects. The `load_context` method becomes a thin wrapper that reads from disk and delegates to the constructor.

For the analytics module's testability, the class should be constructable with in-memory data:
```python
class Football2VecModel:
    def __init__(self, model: Doc2Vec, tokenizer_config: TokenizerConfig) -> None:
        self.model = model
        self.tokenizer_config = tokenizer_config

    def load_context(self, context: Any) -> None:
        """MLflow PythonModel hook — loads from disk."""
        model_dir: str = context.artifacts["model_dir"]
        self.model = cast(Doc2Vec, Doc2Vec.load(os.path.join(model_dir, "player2vec.model")))
        config_path = os.path.join(model_dir, "tokenizer_config.json")
        with open(config_path) as f:
            config_data = json.load(f)
        self.tokenizer_config = TokenizerConfig(**config_data) if config_data else TokenizerConfig()
```

The `load_context` method remains for MLflow runtime compatibility but is no longer required for construction. Tests can create instances with mock model objects.

- [ ] **Step 2: Run tests**

Run: `uv run pytest src/tests/test_football2vec.py -v`
Expected: PASS.

---

### Task 5: Restore Analytics Layer Purity — obso.py

**Findings addressed:** A11

**Files:**
- Modify: `src/analytics/obso.py` — change `load_trained_grids()` and `load_static_grid()` to accept arrays
- Modify: Callers in `src/ingestion/` and `scripts/` — they now pass loaded data
- Modify: `src/tests/test_obso.py` — update test fixtures

- [ ] **Step 1: Refactor load_trained_grids()**

Current signature: `load_trained_grids(reachability_path=None, epv_path=None) -> tuple[ndarray, ndarray]`

New approach: rename to `get_grids()` that accepts optional pre-loaded arrays. If arrays are provided, use them. If not, generate synthetic grids (the fallback path that's already pure computation). Remove `pd.read_parquet` and `np.loadtxt` from the analytics module entirely.

Create a new function in `src/ingestion/` (or the calling script) that handles the I/O:
```python
def load_grids_from_parquet(reachability_path: str, epv_path: str) -> tuple[np.ndarray, np.ndarray]:
    """Load trained OBSO grids from Parquet files."""
    reach_df = pd.read_parquet(reachability_path)
    reach_grid = reach_df.pivot(index="zone_y", columns="zone_x", values="reachability").values
    epv_df = pd.read_parquet(epv_path)
    epv_grid = epv_df.pivot(index="zone_y", columns="zone_x", values="epv_value").values
    return reach_grid, epv_grid
```

- [ ] **Step 2: Refactor load_static_grid()**

Current: `load_static_grid(path: str) -> ndarray` (calls `np.loadtxt`).

Move `np.loadtxt` to the caller. The analytics function becomes `parse_static_grid(data: ndarray) -> ndarray` or is removed entirely if the loadtxt result is used directly. Callers handle `np.loadtxt(path, delimiter=",", dtype=np.float64)`.

- [ ] **Step 3: Update all callers**

Update `scripts/compute_epv_transition_hf.py` and any ingestion module that calls these functions — they now do the I/O themselves and pass arrays to the analytics functions.

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_obso.py -v`
Expected: PASS. Tests should now create synthetic/mock arrays directly instead of relying on file paths.

---

### Task 6: Extract _col_f64 Helper

**Findings addressed:** A35

**Files:**
- Create: `src/analytics/spark_types.py` (or add to an existing utils module)
- Modify: 5 analytics modules that define `_col_f64`

- [ ] **Step 1: Create shared helper**

Check the exact definition of `_col_f64` in `src/analytics/pitch_control.py` — it's likely a small helper that creates a `StructField` with `DoubleType()`. Create `src/analytics/spark_types.py` with this function (or add to an existing analytics utility module).

- [ ] **Step 2: Replace duplicates**

In each of these 5 files, replace the local `_col_f64` with an import:
- `src/analytics/pitch_control.py`
- `src/analytics/line_breaking.py`
- `src/analytics/off_ball_xt.py`
- `src/analytics/elastic_sync.py`
- `src/analytics/defcon_lite.py`

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/ -x -q`
Expected: All pass.

---

## Phase B — Ingestion Layer Restructuring (independently committable)

Splits god modules, fixes the Critical duplicate registry key, adds @workflow coverage, and centralizes the composition root.

### Task 7: Fix Critical Duplicate Workflow Registry Key

**Findings addressed:** A1 (Critical)

**Files:**
- Modify: `src/ingestion/export_embeddings_training_data.py:316` — change workflow ID
- Create: `workflow-cards/wf-football2vec-v2-export.yaml`

- [ ] **Step 1: Rename the workflow ID**

In `src/ingestion/export_embeddings_training_data.py`, change:
```python
# BEFORE:
@workflow("wf-football2vec-v2", phase="training")
# AFTER:
@workflow("wf-football2vec-v2-export", phase="export")
```

- [ ] **Step 2: Create workflow card**

Create `workflow-cards/wf-football2vec-v2-export.yaml` modeled on the existing `wf-football2vec-v2.yaml`. Update the `id`, `name`, `module`, and `entry_point` fields to reflect the export operation.

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/ -x -q && uv run validate_workflow_cards`
Expected: All pass, card validates.

---

### Task 8: Split player_embeddings.py

**Findings addressed:** A4

**Files:**
- Create: `src/ingestion/player_embeddings_v1.py` — Doc2Vec path
- Create: `src/ingestion/player_embeddings_v2.py` — transformer/HF path
- Create: `src/ingestion/player_embeddings_common.py` — shared helpers
- Delete: `src/ingestion/player_embeddings.py` (after migration)
- Modify: `pyproject.toml` — update entry points
- Modify: `workflow-cards/wf-football2vec.yaml` — update `module` field
- Modify: `workflow-cards/wf-football2vec-v2.yaml` — update `module` field
- Modify: `workflow-cards/wf-football2vec-360.yaml` — update `module` field
- Modify: `src/tests/test_player_embeddings.py` — update imports

- [ ] **Step 1: Create common module**

Extract shared helpers into `src/ingestion/player_embeddings_common.py`:
- `_zscore_normalize()` and `_save_norm_params()` — normalization utilities
- Stat-vector loading functions (`_load_outfield_stats`, `_load_goalkeeper_stats`, etc.) used by both v1 and v2
- Common constants and spark.sql query strings for stat loading

- [ ] **Step 2: Create v1 module**

Move v1-specific code into `src/ingestion/player_embeddings_v1.py`:
- `run_pipeline_v1()` (the `@workflow("wf-football2vec", phase="training")` decorated function)
- `main_v1()` entry point
- Any v1-specific helpers (Doc2Vec behavioral UDF, `_merge_vectors`)

Import shared helpers from `player_embeddings_common`.

- [ ] **Step 3: Create v2 module**

Move v2-specific code into `src/ingestion/player_embeddings_v2.py`:
- `run_pipeline_v2()` (the `@workflow("wf-football2vec-v2", phase="inference")` decorated function)
- `main_v2()` entry point
- HF Hub download and import logic
- The combined `run_pipeline()` that calls both v1 and v2, plus `main()` — this becomes the orchestrator

- [ ] **Step 4: Update entry points in pyproject.toml**

```toml
compute_embeddings = "ingestion.player_embeddings_v2:main"
compute_embeddings_v2 = "ingestion.player_embeddings_v2:main_v2"
compute_embeddings_v1 = "ingestion.player_embeddings_v1:main_v1"
```

- [ ] **Step 5: Update workflow cards**

Update `module:` field in `wf-football2vec.yaml`, `wf-football2vec-v2.yaml`, and `wf-football2vec-360.yaml` to point to the new module paths.

- [ ] **Step 6: Delete original file and run tests**

Remove `src/ingestion/player_embeddings.py`. Run:
```bash
uv run pytest src/tests/test_player_embeddings.py -v
uv run validate_workflow_cards
uv run ruff check src/ingestion/player_embeddings_*.py
```
Expected: All pass.

---

### Task 9: Split formations.py

**Findings addressed:** A5

**Files:**
- Create: `src/ingestion/formations_efpi.py`
- Create: `src/ingestion/formations_shape_graph.py`
- Create: `src/ingestion/formations_common.py`
- Delete: `src/ingestion/formations.py`
- Modify: `pyproject.toml` entry points
- Modify: `workflow-cards/wf-formations.yaml`
- Modify: `terraform/modules/workflows/main.tf` — update `entry_point` if names change

- [ ] **Step 1: Create common module**

Extract shared helpers: `_attacking_direction()`, `_derive_formation_label()`, `_prepare_tracking_data()`, shared constants.

- [ ] **Step 2: Create EFPI module**

Move `run_pipeline_efpi()`, `main_efpi()`, and EFPI-specific UDF into `formations_efpi.py`. Decorator: `@workflow("wf-formations", phase="heuristic")`.

- [ ] **Step 3: Create shape graph module**

Move `run_pipeline_shape_graph()`, `main_shape_graph()`, and shape-graph-specific UDF into `formations_shape_graph.py`. Decorator: `@workflow("wf-shape-graphs", phase="heuristic")`.

- [ ] **Step 4: Update entry points and Terraform**

```toml
compute_formations_efpi = "ingestion.formations_efpi:main_efpi"
compute_formations_shape_graph = "ingestion.formations_shape_graph:main_shape_graph"
compute_formations = "ingestion.formations_efpi:main"  # or a thin orchestrator
```

Check `terraform/modules/workflows/main.tf` — if `entry_point` references change, update them. The Terraform tasks reference entry point names (`compute_formations_efpi`, `compute_formations_shape_graph`), which are pyproject.toml script names, not module paths — so they should remain stable.

- [ ] **Step 5: Delete original and run tests**

```bash
uv run pytest src/tests/test_formations.py -v
uv run validate_workflow_cards
```

---

### Task 10: Split defcon_lite.py

**Findings addressed:** A6 (partial — defcon_lite.py, 886 lines)

**Files:**
- Create: `src/ingestion/defcon_lite_360.py`
- Create: `src/ingestion/defcon_lite_tracking.py`
- Create: `src/ingestion/defcon_lite_common.py`
- Modify: `src/ingestion/defcon_lite.py` → becomes thin orchestrator (<100 lines)

- [ ] **Step 1: Create common module**

Extract: `_try_load_champion_defcon()`, `_make_values_udf()`, constants (`_TABLE_NAME`, `_ACTION_PREFIX`, `_FF_PREFIX`), and the `DefconLiteParams` import.

- [ ] **Step 2: Create 360 module**

Move `_make_credits_udf_360()` and `_process_360_matches()` into `defcon_lite_360.py`.

- [ ] **Step 3: Create tracking module**

Move `_make_credits_udf_tracking()` and `_process_tracking_matches()` into `defcon_lite_tracking.py`.

- [ ] **Step 4: Slim down the orchestrator**

`defcon_lite.py` retains `run_pipeline()` and `main()` — it imports from the three sub-modules and calls `_process_360_matches()` and `_process_tracking_matches()`. Should be <100 lines.

- [ ] **Step 5: Run tests**

```bash
uv run pytest src/tests/test_defcon_lite.py -v
uv run validate_workflow_cards
```

---

### Task 11: Split metrica.py

**Findings addressed:** A6 (partial — metrica.py, 843 lines)

**Files:**
- Create: `src/ingestion/metrica_tracking.py`
- Create: `src/ingestion/metrica_events.py`
- Create: `src/ingestion/metrica_common.py` (shared: `_COLUMN_CLEAN_RE`, EPTS parsers, `_safe_float`)
- Modify: `src/ingestion/metrica.py` → thin orchestrator

- [ ] **Step 1-4: Same pattern as defcon_lite split**

`ingest_tracking()` + all tracking-specific helpers → `metrica_tracking.py`
`ingest_events()` + all events-specific helpers → `metrica_events.py`
EPTS parsers + shared constants → `metrica_common.py` (both tracking and events can use EPTS format for Game 3)
`metrica.py` retains `main()` which calls both.

- [ ] **Step 5: Run tests**

```bash
uv run pytest src/tests/test_metrica.py -v
```

---

### Task 12: Split line_breaking.py

**Findings addressed:** A6 (partial — line_breaking.py, 834 lines)

**Files:**
- Create: `src/ingestion/line_breaking_360.py` — StatsBomb 360 path
- Create: `src/ingestion/line_breaking_tracking.py` — Metrica + IDSSE tracking paths
- Create: `src/ingestion/line_breaking_common.py` — shared constants, merge pattern
- Modify: `src/ingestion/line_breaking.py` → thin orchestrator

- [ ] **Step 1-4: Same split pattern**

`_make_statsbomb_udf()` + `_process_statsbomb_360()` → `line_breaking_360.py`
`_make_metrica_udf()` + `_make_idsse_udf()` + `_process_metrica_tracking()` + `_process_idsse_tracking()` → `line_breaking_tracking.py`
Shared constants (`_TABLE_NAME`, `_XY_COLS`) → `line_breaking_common.py`
`line_breaking.py` retains `run_pipeline()` and `main()`.

- [ ] **Step 5: Run tests**

```bash
uv run pytest src/tests/test_line_breaking.py -v
```

---

### Task 13: Split shape_graph.py

**Findings addressed:** A6 (partial — shape_graph.py, 949 lines)

**Files:**
- Create: `src/analytics/shape_graph_construction.py` — Algorithm 1: Delaunay, edge stability, face merging
- Create: `src/analytics/shape_graph_inference.py` — Position inference: vertical/horizontal decomposition
- Modify: `src/analytics/shape_graph.py` → public API re-exports (or delete and update imports)

- [ ] **Step 1: Identify the split boundary**

`compute_shape_graph()` (lines 246-367) and all its helpers (`_angle_at_vertex`, `_compute_edge_stability`, `_extract_edges`, `_empty_shape_graph`, `_merge_faces_for_edge`, `_update_stabilities_for_merged_face`) → `shape_graph_construction.py`

`infer_positions()` (lines 893-949) and all its helpers (`_face_centroids`, `_bridging_edge_midpoints`, `_is_tree`, `_decompose_middle`, `_assign_levels_vertical`, `_assign_levels_horizontal`, `_equal_frequency_assign`) → `shape_graph_inference.py`

Dataclasses (`PositionLabel`, `ShapeGraph`) → `shape_graph_construction.py` (since `ShapeGraph` is the output of construction).

- [ ] **Step 2: Create construction module**

Move `compute_shape_graph()`, `ShapeGraph`, `PositionLabel`, and all construction helpers.

- [ ] **Step 3: Create inference module**

Move `infer_positions()` and all inference helpers. Import `ShapeGraph` and `PositionLabel` from construction.

- [ ] **Step 4: Update shape_graph.py as public API**

`shape_graph.py` becomes a thin re-export:
```python
from analytics.shape_graph_construction import PositionLabel, ShapeGraph, compute_shape_graph
from analytics.shape_graph_inference import infer_positions

__all__ = ["PositionLabel", "ShapeGraph", "compute_shape_graph", "infer_positions"]
```

This preserves all existing import paths (`from analytics.shape_graph import ...`).

- [ ] **Step 5: Run tests**

```bash
uv run pytest src/tests/test_shape_graph.py -v
```

---

### Task 14: Split spadl_vaep.py

**Findings addressed:** A3

**Files:**
- Create: `src/ingestion/vaep_training.py` — `train_vaep_models()` + MLflow model management
- Modify: `src/ingestion/spadl_vaep.py` — remove training code, keep SPADL conversion + VAEP inference

- [ ] **Step 1: Extract training code**

Move `train_vaep_models()` and any training-specific helpers (model building, MLflow logging) into `vaep_training.py`. The function is imported by `scripts/train_vaep_model_hf.py` — update that import.

- [ ] **Step 2: Verify spadl_vaep.py is under 800 lines**

After extraction, `spadl_vaep.py` should be well under 800 lines (training code is ~200-300 lines).

- [ ] **Step 3: Run tests**

```bash
uv run pytest src/tests/test_spadl_vaep.py -v
```

---

### Task 15: Centralize Composition Root + Add @workflow Coverage

**Findings addressed:** A14, A15

**Files:**
- Create: `src/ingestion/bootstrap.py`
- Modify: All 17+ `main()` functions with inline `register_hook(CostEstimateHook(...))`
- Modify: 11 un-decorated ingestion modules — add `@workflow` decorators
- Create: workflow cards for newly decorated modules

- [ ] **Step 1: Create bootstrap module**

Create `src/ingestion/bootstrap.py`:
```python
"""Centralized lifecycle hook registration for all ingestion pipelines."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def bootstrap_hooks(spark: "SparkSession", catalog: str, schema: str) -> None:
    """Register all lifecycle hooks for a pipeline execution.

    Call this once in every main() before run_pipeline().
    Adding a new hook type (e.g., OpenTelemetry) requires changing only this function.
    """
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook

    register_hook(CostEstimateHook(spark, catalog, schema))
```

- [ ] **Step 2: Replace inline hook registration in all main() functions**

In every `main()` that currently has:
```python
from ingestion.cost_hook import CostEstimateHook
from workflows import register_hook
register_hook(CostEstimateHook(spark, args.catalog, args.schema))
```

Replace with:
```python
from ingestion.bootstrap import bootstrap_hooks
bootstrap_hooks(spark, args.catalog, args.schema)
```

Apply to all 17+ main() functions across the split modules.

- [ ] **Step 3: Add @workflow decorators to un-instrumented modules**

For each of these 11 modules, add a `@workflow` decorator to their `run_pipeline` or primary function, and call `bootstrap_hooks` in their `main()`:

1. `statsbomb.py` — `@workflow("wf-statsbomb", phase="ingestion")`
2. `wyscout.py` — `@workflow("wf-wyscout", phase="ingestion")`
3. `metrica.py` (orchestrator) — `@workflow("wf-metrica", phase="ingestion")`
4. `idsse.py` — `@workflow("wf-idsse", phase="ingestion")`
5. `skillcorner.py` — `@workflow("wf-skillcorner", phase="ingestion")`
6. `export_shots_on_target.py` — `@workflow("wf-export-shots", phase="export")`
7. `import_obso_results.py` — `@workflow("wf-import-obso", phase="import")`
8. `import_psxg_predictions.py` — `@workflow("wf-import-psxg", phase="import")`
9. `import_space_creation.py` — `@workflow("wf-import-space-creation", phase="import")`
10. `prepare_360_training_data.py` — `@workflow("wf-prepare-360-data", phase="export")`
11. `sync_hf_costs.py` — `@workflow("wf-sync-hf-costs", phase="sync")`

- [ ] **Step 4: Create workflow cards for newly instrumented modules**

Create 11 new YAML files in `workflow-cards/` following the existing card template. Each needs: `id`, `name`, `version`, `status`, `type`, `domain`, `owners`, `inputs`, `outputs`, `execution`, `cost`.

- [ ] **Step 5: Run full test + validation suite**

```bash
uv run pytest src/tests/ -x -q
uv run validate_workflow_cards
uv run ruff check src/ingestion/
```

---

## Phase C — Taipy App Restructuring (independently committable)

Installs the wheel in the Docker image, extracts query modules, and separates concerns in state modules.

### Task 16: Install Wheel in Taipy Docker Image

**Findings addressed:** A13, A39

**Files:**
- Modify: `hf_taipy_app/Dockerfile` — add wheel COPY + install
- Delete: `hf_taipy_app/src/analytics/` (all 3 files: `pitch_control.py`, `team_shape.py`, `formation_detection.py`)
- Modify: `hf_taipy_app/requirements.txt` — remove dead deps
- Modify: `scripts/manage_space.py` — ensure wheel build before deploy

- [ ] **Step 1: Update Dockerfile**

Add wheel copy and install before the requirements install:
```dockerfile
# After COPY requirements.txt:
COPY dist/luxury_lakehouse-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/luxury_lakehouse-*.whl && rm /tmp/*.whl
RUN pip install --no-cache-dir -r requirements.txt
```

The wheel provides `analytics`, `ingestion`, `shared`, and `workflows` packages. The existing `from analytics.pitch_control import ...` imports in state modules continue to work transparently.

- [ ] **Step 2: Delete vendored analytics**

Remove the entire `hf_taipy_app/src/analytics/` directory (3 files, 1,435 lines):
- `pitch_control.py`
- `formation_detection.py` (dead code — never imported)
- `team_shape.py`
- `__init__.py`

- [ ] **Step 3: Clean up requirements.txt**

Verify whether `gunicorn`, `gevent`, and `gevent-websocket` are used. The Dockerfile CMD is `python src/main.py` — Taipy runs its own server. If these are dead dependencies, remove them. If they are needed for Taipy's WebSocket internals, keep them.

Also remove any deps now provided by the wheel (e.g., `scipy` if it's a transitive dep of the wheel).

- [ ] **Step 4: Update shared constants imports**

With the wheel installed, the Taipy app can now import from `shared.constants`. Update:
- `hf_taipy_app/src/config.py` — replace local `_IDENTIFIER_RE` with `from shared.constants import IDENTIFIER_RE`
- `hf_taipy_app/src/db.py` — same
- `hf_taipy_app/src/filters.py` — same

(These may already have been updated in Task 2 Step 5 if the wheel was available. If not, do it now.)

- [ ] **Step 5: Import WorkflowCard from the wheel**

In `hf_taipy_app/src/state/workflows.py`, replace the custom YAML frontmatter parsing with:
```python
from workflows.card import WorkflowCard
```

Remove the `_FRONTMATTER_RE` regex and the `yaml.safe_load(match.group(1))` code block. Use `WorkflowCard.from_yaml_file(path)` to load cards with full Pydantic validation. This addresses finding A22.

- [ ] **Step 6: Ensure deploy script builds wheel first**

Check `scripts/manage_space.py` — verify it calls `uv build` (or equivalent) before copying files to the Space. The `dist/` directory must contain a fresh wheel matching the current source.

- [ ] **Step 7: Test locally**

Run the Taipy app locally to verify imports work:
```bash
cd hf_taipy_app && python src/main.py
```

Verify: app starts, pages load, no import errors.

---

### Task 17: Extract Taipy Query Modules

**Findings addressed:** A7, A8, A19 (partial)

**Files:**
- Create: `hf_taipy_app/src/queries/__init__.py`
- Create: `hf_taipy_app/src/queries/shots.py`
- Create: `hf_taipy_app/src/queries/passes.py`
- Create: `hf_taipy_app/src/queries/defensive.py`
- Create: `hf_taipy_app/src/queries/players.py`
- Create: `hf_taipy_app/src/queries/tracking.py`
- Create: `hf_taipy_app/src/queries/team_shape.py`
- Create: `hf_taipy_app/src/queries/workflows.py`
- Create: `hf_taipy_app/src/queries/common.py`
- Modify: All 14 state modules — replace inline SQL with query function calls
- Modify: `hf_taipy_app/src/filters.py` — move filter queries to `queries/common.py` or keep in place (filters are already centralized)

- [ ] **Step 1: Create queries/common.py**

```python
"""Shared query infrastructure and column name constants."""

from db import execute_query, t
from cache import ttl_cache

# Column name constants — single source of truth for read-side contracts.
# These must match dbt _marts__models.yml column definitions.
SHOT_COLUMNS = ("shot_id", "location_x", "location_y", "statsbomb_xg", "is_goal",
                "shot_outcome", "shot_body_part", "distance_to_goal", "shot_angle", "minute")
PASS_COLUMNS = ("pass_id", "location_x", "location_y", "end_location_x", "end_location_y",
                "pass_outcome", "pass_type", "pass_height", "minute")
# ... etc for each domain area
```

- [ ] **Step 2: Extract shot queries**

Create `queries/shots.py` — move all SQL from `state/shot_map.py`'s `_fetch_shots()` and `_join_xg_predictions()` into dedicated query functions:
```python
@ttl_cache()
def fetch_shots(comp_id: int | None, team_id: int | None, player_id: int | None) -> pd.DataFrame:
    """Fetch shot data for the given filters."""
    query = f"""
        SELECT s.shot_id, s.location_x, ...
        FROM {t('fct_shots_synced')} s
        JOIN {t('dim_players_synced')} p ON ...
        WHERE ...
    """
    return execute_query(query, params)
```

- [ ] **Step 3: Extract remaining domain queries**

Repeat for each domain area — move SQL strings from state modules into the corresponding `queries/` module. Each query function:
- Uses `@ttl_cache()` (moved from the state module)
- Accepts primitive filter arguments (comp_id, team_id, etc.)
- Returns `pd.DataFrame`
- Documents expected columns

- [ ] **Step 4: Update state modules**

In each state module, replace inline SQL calls with imports from `queries/`:
```python
# BEFORE (in state/shot_map.py):
query = f"SELECT ... FROM {t('fct_shots_synced')} ..."
df = execute_query(query, (comp_id,))

# AFTER:
from queries.shots import fetch_shots
df = fetch_shots(comp_id, team_id, player_id)
```

- [ ] **Step 5: Fix SELECT DISTINCT in filters.py (O2)**

Replace the 4 `SELECT DISTINCT` queries in `filters.py` (lines 47, 65, 133, 227) with recursive CTE pattern matching the 8 existing CTE instances in the same file.

- [ ] **Step 6: Run app locally and verify**

Start the Taipy app, navigate through all 14 pages, verify data loads correctly.

---

### Task 18: Split state/workflows.py

**Findings addressed:** A2

**Files:**
- Create: `hf_taipy_app/src/state/workflows_dag.py` — DAG rendering + content providers
- Create: `hf_taipy_app/src/state/workflows_stats.py` — stat computation + cards
- Modify: `hf_taipy_app/src/state/workflows.py` → thin orchestrator (<400 lines)
- Modify: `hf_taipy_app/src/queries/workflows.py` — SQL queries (from Task 17)
- Modify: `hf_taipy_app/src/main.py` — update star imports

- [ ] **Step 1: Identify the split boundaries**

The 1,990-line module contains: DAG SVG rendering + click handlers (~600 lines), stat computation + card parsing (~500 lines), content provider infrastructure (~300 lines), refresh logic + state variables (~400 lines), SQL queries (~100 lines, moved in Task 17).

- [ ] **Step 2: Create workflows_dag.py**

Move DAG rendering, SVG generation, content provider callbacks, and `RawHtml` class. Export public names via `__all__` with `wf_` prefix.

- [ ] **Step 3: Create workflows_stats.py**

Move stat computation, card loading (now using `WorkflowCard.from_yaml_file` from the wheel), metric formatting. Export via `__all__` with `wf_` prefix.

- [ ] **Step 4: Slim down workflows.py**

Keep: `wf_refresh(state)`, state variable declarations, `register_page_refresher` call. Import from `workflows_dag`, `workflows_stats`, and `queries.workflows`. Target: <400 lines.

- [ ] **Step 5: Update main.py star imports**

Add star imports for the new sub-modules if they export state variables that Taipy needs to bind.

- [ ] **Step 6: Test locally**

Navigate to the AI/ML Workflows page, verify DAG renders, stats display, click handlers work.

---

### Task 19: Taipy App Cleanup

**Findings addressed:** A28, A29, OB1, OB2, OB3

**Files:**
- Modify: `hf_taipy_app/src/main.py` — wire JSON logging
- Modify: `hf_taipy_app/src/render.py` — add temp file cleanup
- Modify: `hf_taipy_app/Dockerfile` — add STOPSIGNAL (A38)
- Create or modify: health check endpoint (OB3)

- [ ] **Step 1: Wire JSON logging (A29/OB1)**

In `main.py`, replace `logging.basicConfig(format="%(asctime)s ...")` with the `_JsonFormatter` from the wheel:
```python
from ingestion.utils import configure_logging
logger = configure_logging("taipy_app")
```

Or if importing from `ingestion.utils` pulls too many deps, extract `_JsonFormatter` into `shared/logging.py` and import from there.

- [ ] **Step 2: Add temp file cleanup (A28/OB2)**

In `render.py`, track created temp files and register cleanup:
```python
import atexit
import os

_temp_files: list[str] = []

def _cleanup_temp_files() -> None:
    for path in _temp_files:
        try:
            os.unlink(path)
        except OSError:
            pass

atexit.register(_cleanup_temp_files)
```

In `_unique_path()`, append each created path to `_temp_files`.

- [ ] **Step 3: Add STOPSIGNAL to Dockerfile (A38)**

Add before CMD:
```dockerfile
STOPSIGNAL SIGTERM
```

- [ ] **Step 4: Add health endpoint (OB3)**

Add a simple health check that verifies DB connectivity. This could be a Taipy page at `/health` or a separate lightweight endpoint. The Dockerfile HEALTHCHECK can then hit this endpoint instead of the root URL.

- [ ] **Step 5: Test locally**

Verify JSON log output, app startup, and health endpoint responds.

---

## Phase D — CI Enforcement + Housekeeping (independently committable)

### Task 20: Add import-linter for Dependency Direction Enforcement

**Findings addressed:** A20

**Files:**
- Modify: `pyproject.toml` — add `import-linter` to dev dependencies
- Create: `.importlinter` configuration file
- Modify: CI workflow (`.github/workflows/python-ci.yml`) — add `lint-imports` step

- [ ] **Step 1: Add import-linter dependency**

Add to `[dependency-groups]` dev:
```toml
"import-linter>=2.0",
```

Run: `uv sync`

- [ ] **Step 2: Create .importlinter config**

Create `.importlinter` in project root:
```ini
[importlinter]
root_packages =
    analytics
    ingestion
    shared
    workflows

[importlinter:contract:analytics-isolation]
name = analytics must not import ingestion, workflows, or shared (except shared.constants)
type = forbidden
source_modules =
    analytics
forbidden_modules =
    ingestion
    workflows

[importlinter:contract:workflows-isolation]
name = workflows must not import ingestion or analytics
type = forbidden
source_modules =
    workflows
forbidden_modules =
    ingestion
    analytics
    shared

[importlinter:contract:shared-isolation]
name = shared must not import ingestion, analytics, or workflows
type = forbidden
source_modules =
    shared
forbidden_modules =
    ingestion
    analytics
    workflows
```

Note: `analytics` importing from `shared.constants` is allowed since `shared` is not in analytics' forbidden list. Adjust if the analytics layer should remain fully independent of `shared`.

- [ ] **Step 3: Run import-linter locally**

Run: `uv run lint-imports`
Expected: All contracts satisfied. If violations exist, fix them before proceeding.

- [ ] **Step 4: Add to CI**

Add a step in `.github/workflows/python-ci.yml`:
```yaml
- name: Import boundary check
  run: uv run lint-imports
```

- [ ] **Step 5: Run full CI suite locally**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -x -q
uv run lint-imports
uv run validate_workflow_cards
```
Expected: All pass.

---

### Task 21: Write ADRs

**Findings addressed:** A30

**Files:**
- Create: `docs/decisions/taipy-selection.md`
- Create: `docs/decisions/lakebase-read-layer.md`
- Create: `docs/decisions/liquid-clustering.md`
- Create: `docs/decisions/template-first-ui.md`
- Create: `docs/decisions/pep723-hf-jobs.md`
- Create: `docs/decisions/wyscout-acl-deferred.md` (A21 documentation)

Each ADR follows the Nygard format: Title, Status, Context, Decision, Consequences.

- [ ] **Step 1: Write all 6 ADRs**

Write each ADR with:
- **Context**: What problem or choice prompted the decision
- **Decision**: What was decided
- **Alternatives considered**: What was evaluated and rejected
- **Consequences**: Trade-offs accepted

- [ ] **Step 2: Verify against code**

For each ADR, verify the stated decision matches current code behavior.

---

### Task 22: Split HF Jobs Scripts

**Findings addressed:** A6 (5 scripts >800 lines)

**Files:**
- Split: `scripts/train_football2vec_360.py` (1,776 lines)
- Split: `scripts/train_football2vec_v2.py` (1,548 lines)
- Split: `scripts/train_xg_v2_hf.py` (1,168 lines)
- Split: `scripts/compute_epv_transition_hf.py` (1,111 lines)
- Split: `scripts/compute_space_creation_hf.py` (918 lines)

These are PEP 723 self-contained scripts. Each needs a companion module (e.g., `scripts/train_football2vec_360_helpers.py`) that the main script imports for data loading, model setup, or evaluation functions. The PEP 723 header stays in the main script.

- [ ] **Step 1-5: Split each script along its natural seam**

For each script:
1. Identify the natural sections (data loading, model definition, training loop, evaluation, upload)
2. Extract the longest independent section into a helper module
3. Import from the helper in the main script
4. Verify both files are under 800 lines
5. Test: `uv run python scripts/<script>.py --help` (or equivalent dry-run if available)

Note: These scripts run on HF Jobs, not locally. The helper modules must be co-located (same directory) since PEP 723 scripts resolve imports from the script's directory.

---

### Task 23: Remaining Housekeeping

**Findings addressed:** A16, A17, A31, A32, A33, S1-S5, CI1, CI2, OB3

- [ ] **Step 1: Remove hardcoded workspace URLs (A16/S4)**

In `scripts/create_indexes.py` and `scripts/refresh_synced_tables.py`, remove the fallback default URLs. Replace with:
```python
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]  # fail fast if missing
```

- [ ] **Step 2: Fix mixed abstraction levels (A17)**

For ingestion modules with main() + UDF factories + parsing helpers: this has been largely addressed by the splits in Tasks 8-14. Verify no remaining module has main() alongside low-level helpers in the same file. If any remain, extract helpers to `_common.py` modules.

- [ ] **Step 3: Document secrets inventory (S1)**

Add a secrets inventory table to CLAUDE.md or a dedicated `docs/decisions/secrets-inventory.md`:

| Store | Secrets | Rotation | Owner |
|-------|---------|----------|-------|
| Databricks ambient OAuth | Workspace access | Automatic | Platform |
| HF Space secrets | DATABRICKS_TOKEN, LAKEBASE_HOST, etc. | Manual (M1: ~2026-06-14) | @karsten |
| HF Jobs secrets | HF_TOKEN | Via HF settings | @karsten |
| GitHub Actions OIDC | AWS role assumption | Automatic | CI |
| Developer env vars | Local .env files | Per-developer | Individual |

- [ ] **Step 4: Document PAT rotation runbook (S2/S3)**

Create `docs/decisions/pat-rotation.md` ADR documenting the current PAT dependency, the expiry timeline, and the manual rotation procedure.

- [ ] **Step 5: Validate Databricks credentials at boot (S5)**

In `hf_taipy_app/src/config.py`, add `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as pydantic-settings fields with validation, so misconfiguration fails at startup rather than at first user query.

- [ ] **Step 6: Create cross-layer metric concordance table (CI1/CI2)**

Create `docs/metric-concordance.md`:

| Metric | Bronze Column | dbt Gold Column | Code Variable | UI Label | Glossary Key |
|--------|---------------|-----------------|---------------|----------|-------------|
| Expected Goals | `statsbomb_xg` | `statsbomb_xg` | `xg` | "xG" | `expected_goals_xg` |
| Expected Threat | `xt_value` | `xt_value` | `xt` | "xT" | `expected_threat_xt` |
| ... | ... | ... | ... | ... | ... |

- [ ] **Step 7: Fix lazy circular import (A34)**

In `src/workflows/registry.py`, break the registry↔runner circular dependency. Instead of the lazy `from workflows.runner import run_workflow` inside the decorator wrapper, have `__init__.py` inject the runner function into the registry at package init time:

```python
# workflows/__init__.py
from workflows.registry import WorkflowRegistry as _registry
from workflows.runner import run_workflow as _run
_registry._runner_fn = _run
```

The registry wrapper calls `self.__class__._runner_fn(...)` instead of importing runner directly.

- [ ] **Step 8: Run final full verification**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
uv run pyright src/
uv run pytest src/tests/ -v
uv run lint-imports
uv run validate_workflow_cards
```
Expected: All pass. Zero violations. All audit findings addressed.

---

## Phase Boundaries and Commit Strategy

Each phase produces an independently working, testable codebase:

| Phase | Scope | Natural break? |
|-------|-------|---------------|
| **A** | Streamlit removal + shared constants + analytics purity | Yes — clean foundation |
| **B** | Ingestion module splits + workflow coverage | Yes — ingestion layer complete |
| **C** | Wheel in Docker + query extraction + state separation | Yes — Taipy app restructured |
| **D** | CI enforcement + ADRs + housekeeping | Yes — everything done |

If the cycle needs to split, the recommended break is **after Phase B**. Phases A+B address 30 of 56 findings and leave the codebase in a strictly better state. Phases C+D can follow in a subsequent cycle.

Commits require explicit user approval per project ground rules.

---

## Findings Coverage Checklist

| Finding | Task | Phase |
|---------|------|-------|
| A1 (Critical) | Task 7 | B |
| A2 | Task 18 | C |
| A3 | Task 14 | B |
| A4 | Task 8 | B |
| A5 | Task 9 | B |
| A6 | Tasks 10-14, 18, 22 | B, C, D |
| A7 | Task 17 | C |
| A8 | Task 17 | C |
| A9 | Task 3 | A |
| A10 | Task 4 | A |
| A11 | Task 5 | A |
| A12 | Task 1 (moot) | A |
| A13 | Task 16 | C |
| A14 | Task 15 | B |
| A15 | Task 15 | B |
| A16 | Task 23 | D |
| A17 | Task 23 | D |
| A18 | Partially addressed (bootstrap adds structure) | B |
| A19 | Task 17 | C |
| A20 | Task 20 | D |
| A21 | Task 21 | D |
| A22 | Task 16 | C |
| A23 | Task 2 | A |
| A24 | Task 17 (column constants) | C |
| A25 | Task 2 | A |
| A26 | Task 2 | A |
| A27 | Task 2 | A |
| A28 | Task 19 | C |
| A29 | Task 19 | C |
| A30 | Task 21 | D |
| A31 | Task 1 (moot) | A |
| A32 | Task 2 (via constants) | A |
| A33 | Task 2 (via constants) | A |
| A34 | Task 23 | D |
| A35 | Task 6 | A |
| A36 | Task 2 | A |
| A37 | Task 2 | A |
| A38 | Task 19 | C |
| A39 | Task 16 | C |
| S1-S5 | Task 23 | D |
| S6 | Task 16 (hash pins) | C |
| S7 | DF4 (Don't Fix) | — |
| O1 | Task 21 (ADR) | D |
| O2 | Task 17 | C |
| O3 | Task 17 | C |
| O4 | Task 2 | A |
| OB1 | Task 19 | C |
| OB2 | Task 19 | C |
| OB3 | Task 19 | C |
| CI1 | Task 23 | D |
| CI2 | Task 23 | D |
| CI3 | Task 1 (moot) | A |
