# Discrete Workflow Tracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split two shared Databricks tasks into discrete task-per-workflow-card mappings so each algorithm gets its own cost, runtime, status, and freshness tracking on the AI/ML Workflows dashboard.

**Architecture:** Two collisions exist where multiple workflow cards claim the same Databricks task key. The xG v1/v2 pattern (separate entry points, separate tasks, separate cards) is the target. Formations split uses Option B — EFPI writes a temp Delta table, shape graph reads from it, creating a DAG dependency. Football2vec split separates v2 import from v1 Doc2Vec inference.

**Tech Stack:** Python (PySpark), Terraform (HCL), YAML workflow cards, dbt seed CSV

---

### Task 1: Split formations.py into two entry points

**Files:**
- Modify: `src/ingestion/formations.py`
- Modify: `pyproject.toml`

The current `_process_matches()` runs both detectors and has a single `@workflow("wf-formations")`. Split into:
- `run_pipeline_efpi()` — materializes tracking data to temp table, runs EFPI, writes formation_labels
- `run_pipeline_shape_graph()` — reads from temp table, runs shape graph, writes formation_labels + player_positions, drops temp table
- `main_efpi()` / `main_shape_graph()` — CLI entry points

- [ ] **Step 1: Refactor `_process_matches` into `_run_efpi` and `_run_shape_graph`**

Extract the shared preamble (skip guard, match ID collection, temp table materialization) into `_prepare_tracking_data()` that returns `(spark_df, new_ids_str, temp_table_name)`. Then:

`_run_efpi(spark, catalog, schema, logger)`:
1. Call `_prepare_tracking_data()` — materializes temp table, returns tracking_df + new_ids
2. Build EFPI templates on driver
3. Run EFPI `applyInPandas`
4. Write EFPI formation_labels with `replaceWhere`
5. Return rows written (temp table stays for shape graph)

`_run_shape_graph(spark, catalog, schema, logger)`:
1. Read from the temp table (written by EFPI task)
2. Get new match IDs from the temp table (no need to re-query gold)
3. Run shape graph `applyInPandas`
4. Write shape graph formation_labels + player_positions with `replaceWhere`
5. Drop temp table
6. Return rows written

- [ ] **Step 2: Add two `@workflow`-decorated pipeline functions**

```python
@workflow("wf-formations", phase="heuristic")
def run_pipeline_efpi(spark, catalog, schema, logger, *, ctx=None):
    """Execute EFPI formation detection (Pass 1). Materializes temp table."""
    total = _run_efpi(spark, catalog, schema, logger)
    logger.info("EFPI formation detection complete -- %d rows written", total)


@workflow("wf-shape-graphs", phase="heuristic")
def run_pipeline_shape_graph(spark, catalog, schema, logger, *, ctx=None):
    """Execute shape graph formation detection (Pass 2). Reads temp table from EFPI."""
    total = _run_shape_graph(spark, catalog, schema, logger)
    logger.info("Shape graph formation detection complete -- %d rows written", total)
```

- [ ] **Step 3: Add two `main` entry points**

```python
def main_efpi() -> None:
    args = parse_ingestion_args("Detect team formations (EFPI template matching)")
    logger = configure_logging("formations_efpi")
    spark = get_spark_session()
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook
    register_hook(CostEstimateHook(spark, args.catalog, args.schema))
    run_pipeline_efpi(spark, args.catalog, args.schema, logger)


def main_shape_graph() -> None:
    args = parse_ingestion_args("Detect team formations (shape graph)")
    logger = configure_logging("formations_shape_graph")
    spark = get_spark_session()
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook
    register_hook(CostEstimateHook(spark, args.catalog, args.schema))
    run_pipeline_shape_graph(spark, args.catalog, args.schema, logger)
```

Keep the existing `main()` as a convenience that runs both sequentially (for local dev).

- [ ] **Step 4: Register entry points in pyproject.toml**

Replace:
```
compute_formations = "ingestion.formations:main"
```
With:
```
compute_formations = "ingestion.formations:main"
compute_formations_efpi = "ingestion.formations:main_efpi"
compute_formations_shape_graph = "ingestion.formations:main_shape_graph"
```

- [ ] **Step 5: Run lint + tests**

```bash
uv run ruff check src/ingestion/formations.py
uv run ruff format --check src/ingestion/formations.py
uv run pytest src/tests/test_shape_graph.py -v --benchmark-disable
```

---

### Task 2: Split player_embeddings.py into two entry points

**Files:**
- Modify: `src/ingestion/player_embeddings.py`
- Modify: `pyproject.toml`

The current `run_pipeline()` tries v2 import, falls back to v1. Split into:
- `run_pipeline_v2()` — imports pre-computed 128-d transformer embeddings from HF Hub
- `run_pipeline_v1()` — runs Doc2Vec inference via applyInPandas (fallback/baseline)

- [ ] **Step 1: Extract v2 import into `run_pipeline_v2`**

```python
@workflow("wf-football2vec-v2", phase="training")
def run_pipeline_v2(spark, catalog, schema, logger, *, ctx=None):
    """Import pre-computed v2 transformer embeddings from HF Hub."""
    logger.info("Starting v2 embedding import for %s.%s", catalog, schema)
    if _import_v2_embeddings(spark, catalog, schema, logger):
        logger.info("v2 embedding import complete")
    else:
        logger.warning("v2 embeddings not available on HF Hub — no data written")
```

- [ ] **Step 2: Extract v1 Doc2Vec into `run_pipeline_v1`**

Move the v1 Doc2Vec inference code (lines 686-872 of current `run_pipeline`) into `run_pipeline_v1()` with `@workflow("wf-football2vec", phase="training")`.

- [ ] **Step 3: Keep `run_pipeline` as combined convenience for local dev**

The existing `run_pipeline()` keeps its try-v2-then-v1 logic but loses the `@workflow` decorator (it's no longer called by a Databricks task directly).

- [ ] **Step 4: Add two main entry points**

```python
def main_v2() -> None:
    args = parse_ingestion_args("Import v2 transformer embeddings from HF Hub")
    logger = configure_logging("player_embeddings_v2")
    spark = get_spark_session()
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook
    register_hook(CostEstimateHook(spark, args.catalog, args.schema))
    run_pipeline_v2(spark, args.catalog, args.schema, logger)


def main_v1() -> None:
    args = parse_ingestion_args("Compute v1 Doc2Vec player embeddings")
    logger = configure_logging("player_embeddings_v1")
    spark = get_spark_session()
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook
    register_hook(CostEstimateHook(spark, args.catalog, args.schema))
    run_pipeline_v1(spark, args.catalog, args.schema, logger)
```

- [ ] **Step 5: Register entry points in pyproject.toml**

Replace:
```
compute_embeddings = "ingestion.player_embeddings:main"
```
With:
```
compute_embeddings = "ingestion.player_embeddings:main"
compute_embeddings_v2 = "ingestion.player_embeddings:main_v2"
compute_embeddings_v1 = "ingestion.player_embeddings:main_v1"
```

- [ ] **Step 6: Run lint + tests**

```bash
uv run ruff check src/ingestion/player_embeddings.py
uv run ruff format --check src/ingestion/player_embeddings.py
uv run pytest src/tests/test_player_embeddings.py -v --benchmark-disable
uv run pytest src/tests/test_football2vec.py -v --benchmark-disable
```

---

### Task 3: Update Terraform workflow DAG

**Files:**
- Modify: `terraform/modules/workflows/main.tf`

- [ ] **Step 1: Replace `compute_formations` task with two tasks**

Replace the single `compute_formations` task with:

```hcl
  task {
    task_key        = "compute_formations_efpi"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_pitch_control"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_formations_efpi"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }

  task {
    task_key        = "compute_formations_shape_graph"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "compute_formations_efpi"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_formations_shape_graph"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "analytics"
  }
```

- [ ] **Step 2: Replace `compute_embeddings` task with two tasks**

Replace the single `compute_embeddings` task with:

```hcl
  task {
    task_key        = "compute_embeddings_v2"
    timeout_seconds = 3600
    max_retries     = 1

    depends_on {
      task_key = "resolve_players"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "compute_embeddings_v2"
      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "embeddings"
  }

  task {
    task_key        = "compute_embeddings_v1"
    timeout_seconds = 3600
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

- [ ] **Step 3: Update header comment**

Update task count: 22 → 24 (net +2 from splitting 2 tasks into 4). Update task list in header to include new names.

- [ ] **Step 4: Run `terraform fmt`**

```bash
terraform fmt terraform/modules/workflows/main.tf
```

---

### Task 4: Update workflow cards

**Files:**
- Modify: `workflow-cards/wf-formations.yaml`
- Modify: `workflow-cards/wf-shape-graphs.yaml`
- Modify: `workflow-cards/wf-football2vec.yaml`
- Modify: `workflow-cards/wf-football2vec-v2.yaml`

- [ ] **Step 1: Update wf-formations.yaml**

Change `entry_point: compute_formations` to `entry_point: compute_formations_efpi`.

Remove shape graph outputs (`fct_player_positions`, `fct_position_maps`) — those belong to `wf-shape-graphs`.

Remove Sotudeh references — those belong to `wf-shape-graphs`.

- [ ] **Step 2: Update wf-shape-graphs.yaml**

Revert the `parent_workflow` change. Set `entry_point: compute_formations_shape_graph`.

Change `depends_on` from `wf-formations` to `wf-pitch-control` (or `wf-formations` since it depends on the temp table from EFPI). Since the shape graph task depends on the EFPI task in the Databricks DAG, it should `depends_on: wf-formations` in the card.

- [ ] **Step 3: Update wf-football2vec-v2.yaml**

Change `inference.entry_point: compute_embeddings` to `inference.entry_point: compute_embeddings_v2`.

- [ ] **Step 4: Update wf-football2vec.yaml**

Change `inference.entry_point: compute_embeddings` to `inference.entry_point: compute_embeddings_v1`.

---

### Task 5: Update task_workflow_mapping seed

**Files:**
- Modify: `dbt_project/seeds/task_workflow_mapping.csv`

- [ ] **Step 1: Update mappings**

Replace:
```
compute_formations,wf-formations
compute_embeddings,wf-football2vec
```
With:
```
compute_formations_efpi,wf-formations
compute_formations_shape_graph,wf-shape-graphs
compute_embeddings_v2,wf-football2vec-v2
compute_embeddings_v1,wf-football2vec
```

Keep `compute_formations` as a legacy entry mapping to `wf-formations` (for historical job runs).

---

### Task 6: Update C4 diagram and documentation counts

**Files:**
- Modify: `docs/c4/architecture.dsl`
- Modify: `docs/c4/architecture.html`
- Modify: `ARCHITECTURE.md`

- [ ] **Step 1: Update task count in C4 DSL**

Change `22 tasks` to `24 tasks` and `22 pipeline tasks` to `24 pipeline tasks`.

- [ ] **Step 2: Update task count in C4 HTML**

Same changes as DSL (the HTML embeds the DSL as a code block).

- [ ] **Step 3: Update ARCHITECTURE.md**

No count changes needed (fact tables and synced tables unchanged). Verify the header status line says 24 tasks if it references the task count.

---

### Task 7: Full verification

- [ ] **Step 1: Lint + format**

```bash
uv run ruff check src/ scripts/
uv run ruff format --check src/ scripts/
```

- [ ] **Step 2: Tests**

```bash
uv run pytest src/tests/ --benchmark-disable --no-header
```

- [ ] **Step 3: Terraform validate**

```bash
terraform fmt -check terraform/modules/workflows/main.tf
```

- [ ] **Step 4: Verify no entry_point collisions**

```python
# Run the collision check from the brainstorming session
# Expected: 0 collisions, each entry_point maps to exactly one card
```
