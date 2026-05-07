# F2V Dimension Fix + V1 Deprecation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix three bugs in the f2v embedding pipeline (stale dimension constants, stage1 production overwrites, missing two-stage orchestrator dispatch), add a v2 dimension guard, and deprecate the v1 Doc2Vec daily task.

**Architecture:** Update dimension constants to match the evolve engine's `hidden_dim=192` promotion (v2: 128→192, 360: 144→208). Remove stage1 publish calls from both trainers. Add two-stage sequential dispatch to the SK3-MIG-B orchestrator. Remove `compute_embeddings_v1` from the Databricks job and delete the combined v2→v1 fallback orchestrator.

**Tech Stack:** Python 3.10, PySpark, Terraform, pytest, huggingface_hub

**Spec:** `docs/superpowers/specs/2026-05-07-f2v-dim-fix-v1-deprecation-design.md`

---

### Task 1: Create feature branch

**Files:** None

- [ ] **Step 1: Create branch from main**

```bash
git checkout main
git checkout -b fix/f2v-dim-v1-deprecation
```

- [ ] **Step 2: Verify clean state**

```bash
git log --oneline -3
```

Expected: HEAD at `04e2c87` (or later main HEAD).

---

### Task 2: Update dimension constants (wheel + helpers)

**Files:**
- Modify: `src/ingestion/player_embeddings_v2.py` (lines 68-69)
- Modify: `scripts/train_football2vec_360_helpers.py` (line 28)
- Test: `src/tests/test_hnsw_dim_parity.py` (auto-passes — reads from source-of-truth)

- [ ] **Step 1: Update `_V2_BEHAVIORAL_DIM` and `_V360_BEHAVIORAL_DIM`**

In `src/ingestion/player_embeddings_v2.py`, change:

```python
_V2_BEHAVIORAL_DIM = 128
_V360_BEHAVIORAL_DIM = 144
```

to:

```python
_V2_BEHAVIORAL_DIM = 192
_V360_BEHAVIORAL_DIM = 208
```

- [ ] **Step 2: Update `OUTPUT_DIM` in helpers**

In `scripts/train_football2vec_360_helpers.py`, change:

```python
OUTPUT_DIM = 144
```

to:

```python
OUTPUT_DIM = 208
```

- [ ] **Step 3: Update HNSW 360 index dimensions**

In `scripts/create_indexes.py`, change both 360 HNSW entries from `vector(144)` to `vector(208)`:

```python
    (
        "idx_fct_emb_career_360_behavioral_hnsw",
        "fct_player_embeddings_career_360_synced",
        "USING hnsw ((behavioral_vector::text::vector(208)) vector_cosine_ops)",
    ),
    (
        "idx_fct_emb_season_360_behavioral_hnsw",
        "fct_player_embeddings_season_360_synced",
        "USING hnsw ((behavioral_vector::text::vector(208)) vector_cosine_ops)",
    ),
```

Also update the comment above these entries from `144d` to `208d`:

```python
    # 360 embeddings — 208d (hidden_dim=192 + context_dim=16)
```

- [ ] **Step 4: Run HNSW parity tests**

Run: `uv run pytest src/tests/test_hnsw_dim_parity.py -v`

Expected: All 4 tests PASS. The tests read `_V360_BEHAVIORAL_DIM` and `Football2VecConfig.hidden_dim` from Python source-of-truth and compare against the `create_indexes.py` literals.

---

### Task 3: Update docstrings (144d → 208d)

**Files:**
- Modify: `src/analytics/football2vec_360.py` (lines 106-110)
- Modify: `scripts/train_football2vec_360.py` (lines 16, 446, 578)

- [ ] **Step 1: Update `Football2Vec360Encoder` class docstring**

In `src/analytics/football2vec_360.py`, change:

```python
    """Football2Vec encoder enriched with 360 freeze frame context.

    Combines the transformer encoder (SPADL action sequences -> 128d)
    with a Deep Sets branch (360 freeze frames -> 16d) via concatenation
    to produce 144d embeddings.
```

to:

```python
    """Football2Vec encoder enriched with 360 freeze frame context.

    Combines the transformer encoder (SPADL action sequences -> 192d)
    with a Deep Sets branch (360 freeze frames -> 16d) via concatenation
    to produce 208d embeddings.
```

- [ ] **Step 2: Update trainer module docstring**

In `scripts/train_football2vec_360.py`, change line 16:

```python
128d transformer + 16d Deep Sets context = 144d output embeddings.
```

to:

```python
192d transformer + 16d Deep Sets context = 208d output embeddings.
```

- [ ] **Step 3: Update `_generate_embeddings` docstring**

In `scripts/train_football2vec_360.py`, change line 446:

```python
    """Run inference on all data to produce 144d embeddings."""
```

to:

```python
    """Run inference on all data to produce 208d embeddings."""
```

- [ ] **Step 4: Update `_publish_embeddings` docstring**

In `scripts/train_football2vec_360.py`, change line 578:

```python
    """Publish 144d embeddings DataFrame to HF Hub as Parquet."""
```

to:

```python
    """Publish 208d embeddings DataFrame to HF Hub as Parquet."""
```

- [ ] **Step 5: Update `player_embeddings_v2.py` docstrings (5 stale dimension references)**

In `src/ingestion/player_embeddings_v2.py`, update these docstrings:

In `_import_v2_embeddings` docstring (line 79), change:

```python
    """Import pre-computed 128-dim transformer embeddings from HF Hub.
```

to:

```python
    """Import pre-computed 192-dim transformer embeddings from HF Hub.
```

In `_import_embeddings_360` docstring (line 282), change:

```python
    """Import pre-computed 144-dim 360-enriched embeddings from HF Hub.
```

to:

```python
    """Import pre-computed 208-dim 360-enriched embeddings from HF Hub.
```

In `_import_embeddings_360` docstring (lines 286-287), change:

```python
    labels every row with ``data_source='football2vec_360'`` (overriding any
    provider info in the parquet), validates the 144-dim vector contract, and
```

to:

```python
    labels every row with ``data_source='football2vec_360'`` (overriding any
    provider info in the parquet), validates the 208-dim vector contract, and
```

In the same docstring (line 288), change:

```python
    The 360 model has its own embedding space (144-dim = 128-dim transformer +
    16-dim Deep Sets context) and is NOT directly comparable to the v2 128-dim
```

to:

```python
    The 360 model has its own embedding space (208-dim = 192-dim transformer +
    16-dim Deep Sets context) and is NOT directly comparable to the v2 192-dim
```

In the Raises section (line 304), change:

```python
        RuntimeError: If the downloaded parquet has vectors of the wrong
            dimension (expected 144 per row). Silent dimension drift is
```

to:

```python
        RuntimeError: If the downloaded parquet has vectors of the wrong
            dimension (expected 208 per row). Silent dimension drift is
```

---

### Task 4: Update `test_player_embeddings_360.py` (7 hardcoded 144 locations)

**Files:**
- Modify: `src/tests/test_player_embeddings_360.py`

- [ ] **Step 1: Update module docstring**

Change line 5:

```python
- Produces 144-dim behavioral vectors (v2 is 128-dim)
```

to:

```python
- Produces 208-dim behavioral vectors (v2 is 192-dim)
```

- [ ] **Step 2: Update test vectors in `test_run_pipeline_360_writes_football2vec_360_data_source`**

Change lines 45-47:

```python
            "behavioral_vector": [
                [0.1] * 144,
                [0.2] * 144,
                [0.3] * 144,
            ],
```

to:

```python
            "behavioral_vector": [
                [0.1] * 208,
                [0.2] * 208,
                [0.3] * 208,
            ],
```

- [ ] **Step 3: Update wrong-dim test in `test_run_pipeline_360_rejects_wrong_dimension`**

Change line 86 docstring:

```python
    """If the HF parquet has vectors with length != 144, the import must raise
    (not silently pass through the wrong dimension)."""
```

to:

```python
    """If the HF parquet has vectors with length != 208, the import must raise
    (not silently pass through the wrong dimension)."""
```

Change line 94 wrong-dim test vector:

```python
            "behavioral_vector": [[0.1] * 128],  # WRONG: 128 instead of 144
```

to:

```python
            "behavioral_vector": [[0.1] * 192],  # WRONG: 192 instead of 208
```

Change line 106 pytest.raises match string:

```python
        with pytest.raises((ValueError, RuntimeError), match="144"):
```

to:

```python
        with pytest.raises((ValueError, RuntimeError), match="208"):
```

- [ ] **Step 4: Run the 360 tests**

Run: `uv run pytest src/tests/test_player_embeddings_360.py -v`

Expected: All 4 tests PASS.

---

### Task 5: Add v2 dimension validation guard

**Files:**
- Modify: `src/ingestion/player_embeddings_v2.py` (`_import_v2_embeddings` function)

- [ ] **Step 1: Write a failing test**

Add to `src/tests/test_player_embeddings_360.py` (this file already tests the 360 dim guard, adding the v2 equivalent here keeps dim-guard tests together):

```python
def test_import_v2_rejects_wrong_dimension() -> None:
    """If the v2 HF parquet has vectors with length != 192, the import must raise."""
    from ingestion import player_embeddings_v2 as mod

    fake_parquet = pd.DataFrame(
        {
            "canonical_player_id": ["p1"],
            "match_id": ["m1"],
            "behavioral_vector": [[0.1] * 128],  # WRONG: 128 instead of 192
        }
    )

    spark = MagicMock()
    logger = MagicMock()

    with (
        patch("ingestion.player_embeddings_v2.hf_hub_download", return_value="/fake.parquet"),
        patch("ingestion.player_embeddings_v2.repo_exists", return_value=True),
        patch("ingestion.player_embeddings_v2.pd.read_parquet", return_value=fake_parquet),
    ):
        with pytest.raises(RuntimeError, match="192"):
            mod._import_v2_embeddings(spark, "soccer_analytics", "bronze", logger)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest src/tests/test_player_embeddings_360.py::test_import_v2_rejects_wrong_dimension -v`

Expected: FAIL — no dimension check exists in `_import_v2_embeddings` yet.

- [ ] **Step 3: Add the dimension guard to `_import_v2_embeddings`**

In `src/ingestion/player_embeddings_v2.py`, after the column validation block (after line 125 `return False`), add:

```python
    # Validate the dimension contract. Dimension drift would silently corrupt
    # downstream cosine similarity — fail loudly. Check the first 10 rows.
    for i, vec in enumerate(v2_pdf["behavioral_vector"].iloc[:10]):
        vec_list = list(vec) if not isinstance(vec, list) else vec
        if len(vec_list) != _V2_BEHAVIORAL_DIM:
            msg = (
                f"v2 Parquet vector at row {i} has length {len(vec_list)}, "
                f"expected {_V2_BEHAVIORAL_DIM} ({_V2_BEHAVIORAL_DIM}-dim transformer). "
                f"This means the HF dataset schema drifted — do NOT import "
                f"until the training run is re-verified."
            )
            raise RuntimeError(msg)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest src/tests/test_player_embeddings_360.py -v`

Expected: All 5 tests PASS (4 existing + 1 new).

---

### Task 6: Remove stage1 publish calls

**Files:**
- Modify: `scripts/train_football2vec_360.py` (line 827)
- Modify: `scripts/train_football2vec_v2.py` (line 744)

- [ ] **Step 1: Remove stage1 publish from 360 trainer**

In `scripts/train_football2vec_360.py`, delete line 827:

```python
    _publish_embeddings(embeddings_df, hf_token, "stage1")
```

This is the line in `_run_stage1` that overwrites the production HF dataset. The `_save_checkpoint` call above it is kept (stage2 needs it). The `_publish_embeddings` call in `_run_stage2` (line 923) is kept.

- [ ] **Step 2: Remove stage1 publish from v2 trainer**

In `scripts/train_football2vec_v2.py`, delete line 744:

```python
    _publish_emb(emb_df, hf_token, "stage1")
```

Same pattern — stage1 keeps its checkpoint, only stage2 publishes. The `_publish_emb` call in `_run_stage2` (line 822) is kept.

- [ ] **Step 3: Verify syntax**

Run: `uv run python -c "import ast; ast.parse(open('scripts/train_football2vec_360.py').read()); print('360 OK')"`
Run: `uv run python -c "import ast; ast.parse(open('scripts/train_football2vec_v2.py').read()); print('v2 OK')"`

Expected: Both print OK.

---

### Task 7: Two-stage orchestrator dispatch

**Files:**
- Modify: `scripts/sk3_mig_b_retrain.py` (`_dispatch_trained_model` function, `_ITEM_COST_USD`)

**Important:** The committed version of `_dispatch_trained_model` (lines 570-627) has no `_SCRIPT_ARGS_MAP`, no `_TIMEOUT_MAP`, and does not pass `script_args` or `timeout` to `api.run_uv_job()`. The changes below add only the minimum needed for two-stage dispatch.

- [ ] **Step 1: Add `_TWO_STAGE_ITEMS` constant**

In `scripts/sk3_mig_b_retrain.py`, after the `_ITEM_COST_USD` dict (the dict ends with `}` followed by a blank line before the `@dataclass` `CycleState`), add:

```python
# Items requiring two-stage dispatch (stage1 -> poll -> stage2). Both stages are
# separate HF Jobs. Stage1 saves a checkpoint; stage2 loads it via _load_stage1
# and publishes final debiased embeddings to the production HF dataset.
_TWO_STAGE_ITEMS: set[str] = {"f2v_v2", "f2v_360"}
```

- [ ] **Step 2: Update `_ITEM_COST_USD` for two-stage items**

Change:

```python
    "f2v_v2": 4.00,
    "f2v_360": 5.00,
```

to:

```python
    "f2v_v2": 8.00,   # doubled — stage1 + stage2
    "f2v_360": 10.00,  # doubled — stage1 + stage2
```

- [ ] **Step 3: Modify `_dispatch_trained_model` for two-stage dispatch**

In the function body of `_dispatch_trained_model` (lines 570-627), insert a two-stage branch after the `api = HfApi()` line and before the existing `job = api.run_uv_job(...)` call. The two-stage path uses early return, so the existing single-stage code remains at the same indentation level — no `else` block needed. The `if script is None:` local-only branch at the top is unchanged.

The full replacement of the function body after the local-only branch:

```python
    flavor = _FLAVOR_MAP[cycle_item]

    from huggingface_hub import HfApi

    api = HfApi()
    # JobInfo dataclass exposes `.id` (not `.job_id`); run_uv_job takes `flavor=`
    # (not `hardware=`). Sentinel test: src/tests/test_sk3_mig_b_orchestrator_apis.py.

    if cycle_item in _TWO_STAGE_ITEMS:
        # Two-stage dispatch: stage1 (checkpoint only) -> stage2 (publish)
        for stage_num in (1, 2):
            job = api.run_uv_job(
                script=script,
                script_args=["--stage", str(stage_num)],
                flavor=flavor,
                secrets={
                    "HF_TOKEN": os.environ["HF_TOKEN"],
                    "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
                    "DATABRICKS_HOST": os.environ["DATABRICKS_HOST"],
                    "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI"],
                    "DATABRICKS_WAREHOUSE_ID": state.warehouse_id,
                    "DATABRICKS_SQL_WAREHOUSE_ID": state.warehouse_id,
                },
            )
            job_id = job.id
            state.current_hf_job_id = job_id
            _emit_status(
                state,
                step="dispatch",
                item=cycle_item,
                phase="running",
                hf_job_id=job_id,
                msg=f"HF Job dispatched: script={script} flavor={flavor} stage={stage_num}",
            )
            _poll_hf_job_until_terminal(state, cycle_item, job_id)
        return job_id  # return stage2 job_id

    job = api.run_uv_job(
        script=script,
        flavor=flavor,
        secrets={
            "HF_TOKEN": os.environ["HF_TOKEN"],
            "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
            "DATABRICKS_HOST": os.environ["DATABRICKS_HOST"],
            "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI"],
            "DATABRICKS_WAREHOUSE_ID": state.warehouse_id,
            # f2v_v1 (scripts/train_football2vec.py) reads
            # DATABRICKS_SQL_WAREHOUSE_ID; the v2/360 gamma rewrites in PR-2
            # will need it too. Pass on every dispatch — non-SQL trainers ignore.
            "DATABRICKS_SQL_WAREHOUSE_ID": state.warehouse_id,
        },
    )
    job_id = job.id
    state.current_hf_job_id = job_id
    _emit_status(
        state,
        step="dispatch",
        item=cycle_item,
        phase="running",
        hf_job_id=job_id,
        msg=f"HF Job dispatched: script={script} flavor={flavor}",
    )
    _poll_hf_job_until_terminal(state, cycle_item, job_id)
    return job_id
```

The single-stage path is identical to the current committed code — no `script_args`, no `timeout`, same secrets dict with existing comments preserved.

- [ ] **Step 4: Verify syntax**

Run: `uv run python -c "import ast; ast.parse(open('scripts/sk3_mig_b_retrain.py').read()); print('OK')"`

Expected: Prints OK.

---

### Task 8: Deprecate v1 — remove entry point and Terraform task

**Files:**
- Modify: `pyproject.toml` (line 118)
- Modify: `terraform/modules/workflows/main.tf` (lines 24, 198-221, 650)

- [ ] **Step 1: Remove entry point from pyproject.toml**

Delete this line from the `[project.scripts]` section:

```toml
compute_embeddings_v1 = "ingestion.player_embeddings_v1:main_v1"
```

- [ ] **Step 2: Remove v1 task block from Terraform**

In `terraform/modules/workflows/main.tf`, delete the entire v1 task block (lines 198-221):

```terraform
  # ── Task: Compute player embeddings v1 (Doc2Vec, deprecated) ────────
  # Football2vec v1: Doc2Vec action sequences + statistical z-score vectors.
  # Retained for comparison; superseded by v2 transformer embeddings.
  task {
    task_key        = "compute_embeddings_v1"
    timeout_seconds = 600
    max_retries     = 1

    depends_on {
      task_key = "compute_embeddings_v2"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings_v1"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
  }
```

- [ ] **Step 3: Remove v1 `depends_on` from dbt_build task**

In `terraform/modules/workflows/main.tf`, delete line 650:

```terraform
    depends_on { task_key = "compute_embeddings_v1" }
```

- [ ] **Step 4: Update task-key comment list**

In `terraform/modules/workflows/main.tf`, delete line 24:

```terraform
#   compute_embeddings_v1 — Doc2Vec (gensim) player embeddings, deprecated (depends on compute_embeddings_v2)
```

- [ ] **Step 5: Update v2 task comment (128d → 192d)**

In `terraform/modules/workflows/main.tf`, change line 23:

```terraform
#   compute_embeddings_v2 — Transformer (128d) player embeddings with adversarial debiasing (depends on entity resolution)
```

to:

```terraform
#   compute_embeddings_v2 — Transformer (192d) player embeddings with adversarial debiasing (depends on entity resolution)
```

---

### Task 9: Deprecate v1 — remove combined orchestrator and fix tests

**Files:**
- Modify: `src/ingestion/player_embeddings_v2.py` (remove `run_pipeline` + `main`)
- Modify: `src/tests/test_player_embeddings.py` (remove `TestMainFunction` class + import)

- [ ] **Step 1: Remove `run_pipeline()` from `player_embeddings_v2.py`**

Delete the entire `run_pipeline` function (lines 470-511):

```python
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    ...
```

This includes the `from ingestion.player_embeddings_v1 import run_pipeline_v1` lazy import inside it.

- [ ] **Step 2: Remove `main()` from `player_embeddings_v2.py`**

Delete the entire `main` function (lines 519-531):

```python
def main() -> None:
    """CLI entry point for player embedding computation."""
    args = parse_ingestion_args("Compute player embeddings from event data")
    logger = configure_logging("player_embeddings")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
```

- [ ] **Step 3: Remove `_main_combined` import from test file**

In `src/tests/test_player_embeddings.py`, delete line 25:

```python
from ingestion.player_embeddings_v2 import main as _main_combined  # ensure module in sys.modules
```

- [ ] **Step 4: Remove entire `TestMainFunction` class**

In `src/tests/test_player_embeddings.py`, delete the entire `TestMainFunction` class (line 896 through end of file). This class contains 4 test methods that all call `_main_combined()` / `main()` which no longer exist:

- `test_writes_with_replace_where` (line 917)
- `test_skips_when_all_matches_have_embeddings` (line 1009)
- `test_defensive_fallback_no_source_matches_but_existing_embeddings` (line 1045)
- `test_empty_events_exits_early` (line 1079)

- [ ] **Step 5: Run the remaining tests**

Run: `uv run pytest src/tests/test_player_embeddings.py -v`

Expected: All remaining tests PASS. The `TestMainFunction` class is gone; all other test classes are unaffected.

---

### Task 10: Run full test suite and lint

**Files:** None (verification only)

- [ ] **Step 1: Run ruff lint**

Run: `uv run ruff check src/ingestion/player_embeddings_v2.py src/analytics/football2vec_360.py scripts/train_football2vec_360.py scripts/train_football2vec_v2.py scripts/sk3_mig_b_retrain.py scripts/train_football2vec_360_helpers.py scripts/create_indexes.py`

Expected: No violations.

- [ ] **Step 2: Run ruff format check**

Run: `uv run ruff format --check src/ingestion/player_embeddings_v2.py src/analytics/football2vec_360.py`

Expected: No reformatting needed.

- [ ] **Step 3: Run pyright on wheel code**

Run: `uv run pyright src/ingestion/player_embeddings_v2.py src/analytics/football2vec_360.py`

Expected: 0 errors.

- [ ] **Step 4: Run all affected tests**

Run: `uv run pytest src/tests/test_hnsw_dim_parity.py src/tests/test_player_embeddings_360.py src/tests/test_player_embeddings.py -v`

Expected: All tests PASS.

- [ ] **Step 5: Run broader test suite for regressions**

Run: `uv run pytest src/tests/ -v --ignore=src/tests/sk3_mig_b -x`

Expected: PASS (ignoring sk3_mig_b subdir which has Databricks-dependent smoke tests).

---

### Task 11: Version bump and commit

**Files:**
- Many (via `scripts/bump_wheel.py`)

- [ ] **Step 1: Bump wheel version**

Run: `uv run python scripts/bump_wheel.py`

This updates the version in pyproject.toml, `src/shared/wheel.py`, all 17 PEP 723 trainer scripts, deploy.sh, and TF files. Never edit `version =` manually.

- [ ] **Step 2: Commit all changes**

```bash
git add \
  src/ingestion/player_embeddings_v2.py \
  src/analytics/football2vec_360.py \
  scripts/train_football2vec_360_helpers.py \
  scripts/train_football2vec_360.py \
  scripts/train_football2vec_v2.py \
  scripts/sk3_mig_b_retrain.py \
  scripts/create_indexes.py \
  pyproject.toml \
  terraform/modules/workflows/main.tf \
  src/tests/test_player_embeddings_360.py \
  src/tests/test_player_embeddings.py \
  docs/superpowers/specs/2026-05-07-f2v-dim-fix-v1-deprecation-design.md \
  docs/superpowers/plans/2026-05-07-f2v-dim-fix-v1-deprecation.md
```

Also add any files touched by `bump_wheel.py` (check `git status`).

**Do NOT commit without explicit user approval.**

Proposed commit message:

```
fix(embeddings): update dims to match hidden_dim=192, fix stage1 publish, deprecate v1

- _V2_BEHAVIORAL_DIM 128→192, _V360_BEHAVIORAL_DIM 144→208, OUTPUT_DIM 144→208
- Remove _publish_embeddings from _run_stage1 in both f2v trainers (only stage2 publishes)
- Add _TWO_STAGE_ITEMS two-stage dispatch to SK3-MIG-B orchestrator
- Add v2 dimension validation guard (mirrors existing 360 guard)
- Remove compute_embeddings_v1 Terraform task + entry point + combined orchestrator
- Update HNSW 360 indexes vector(144)→vector(208)
- Update test_player_embeddings_360.py + remove TestMainFunction class
```
