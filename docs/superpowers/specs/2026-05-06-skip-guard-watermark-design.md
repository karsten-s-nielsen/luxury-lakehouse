# Skip Guard Watermark & Freshness Hardening

**Date:** 2026-05-06
**Status:** Draft
**Author:** Karsten Skyt Nielsen + Claude

## Problem

The Databricks daily pipeline runs 28 tasks. Compute tasks have effective anti-join skip guards (`find_new_ids`), but 10 downstream tasks either have no guard or are always-run. On a steady-state day with no new data, these tasks still spin up environments, run pipelines, and exit — wasting DBUs.

Additionally, `ingest_statsbomb` has a pass-through guard (`count=1` unconditionally) that defers all freshness logic to the pipeline internals. The guard should answer "is there new data?" cheaply before the heavy pipeline starts.

## Goals

1. Add a **watermark-based skip guard** pattern for tasks that don't need specific new-ID lists — just "has any upstream table changed since my last run?"
2. **Fix the StatsBomb guard** to check for new data at the guard level via anti-join.
3. Establish the **guard-checks-first** convention for future live-API ingestion providers.
4. **No new dependency lists** — upstream tables are derived from existing sources (workflow card `inputs`, `SYNCED_TABLES` list).

## Non-Goals

- Replacing anti-join guards on compute tasks (they need specific ID lists).
- Replacing HF SHA guards on import tasks (SHA comparison is correct for external repos).
- Changing the Terraform DAG topology.
- Implementing preflight-task optimization (Approach B) — deferred as a future upgrade.

## Design

### 1. Watermark Table

New Delta table `observability.workflow_watermarks`:

```sql
CREATE TABLE IF NOT EXISTS {catalog}.observability.workflow_watermarks (
    workflow_id       STRING NOT NULL,
    upstream_table    STRING NOT NULL,
    last_seen_version BIGINT NOT NULL,
    checked_at        TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
```

Composite key: `(workflow_id, upstream_table)`. One row per upstream dependency per workflow. Created lazily via `ensure_table()` on first use.

### 2. New Functions in `guards.py`

#### `check_upstream_freshness`

```python
def check_upstream_freshness(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    upstream_tables: list[str],
) -> FilterResult:
```

Behavior:

1. `ensure_table(spark, watermarks_table, _WATERMARKS_DDL)`.
2. For each table in `upstream_tables`:
   - Run `DESCRIBE HISTORY {table} LIMIT 20`.
   - Filter to data-changing operations: `WRITE`, `MERGE`, `DELETE`, `UPDATE`, `CREATE TABLE AS SELECT`, `CREATE OR REPLACE TABLE AS SELECT`, `RESTORE`.
   - Take `max(version)` from those rows.
3. Load stored watermarks for this `workflow_id` from `observability.workflow_watermarks`.
4. If all current versions match stored versions → `FilterResult(count=0)`.
5. If any differ, any stored watermark is missing (first run), or any table has no data-changing history → `FilterResult(count=1)`.
6. Fail open: if `DESCRIBE HISTORY` throws (table doesn't exist yet) → `FilterResult(count=1)`.

**Why Delta version, not `max(_ingested_at)`:** Delta history is pure metadata (no table scan). Works on all tables including dbt marts that lack `_ingested_at`. Predictive Optimization's auto-OPTIMIZE/VACUUM bump the version number, but the operation-type filter excludes those — only data-changing operations count.

**Implementation prerequisite:** `DESCRIBE HISTORY` is novel to this codebase (zero existing usage). Before implementing, run `DESCRIBE HISTORY <table> LIMIT 50` on at least one bronze table, one gold mart, and one table with Predictive Optimization enabled. Verify: (a) the column name is `operation` (not `operation_type`), (b) the exact string values for data-changing operations, (c) that auto-OPTIMIZE and auto-VACUUM from PO produce reliably distinguishable operation values. Also check for `RESTORE` operation strings (time-travel restore is a data-changing operation). Lock in the allowlist from empirical evidence, not documentation.

#### `record_watermarks`

```python
def record_watermarks(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    upstream_tables: list[str],
) -> None:
```

Called after successful pipeline completion. Re-queries `DESCRIBE HISTORY` for each table, MERGEs current versions into `workflow_watermarks`. Separate from the check so that a failed run does not update the watermark.

**Error semantics in hf_sync:** `hf_sync.py` wraps sub-ops in a catch-all `except Exception` block. If `run_pipeline` succeeds but `record_watermarks` fails (e.g., MERGE conflict), the sub-op reports failure (exception propagates to the catch-all), and the watermark is not recorded — so the next run re-runs the sub-op. This is the correct behavior: a successful data write with a failed watermark record is a minor redundancy (one extra re-run), not data loss. The alternative (catching watermark errors separately) risks silently losing the write-back.

#### `resolve_upstream_tables_from_card`

```python
def resolve_upstream_tables_from_card(
    workflow_id: str,
    catalog: str,
    schema: str,
    cards_dir: Path | None = None,
) -> list[str]:
```

Loads `workflow-cards/{workflow_id}.yaml`, extracts entries from `inputs.tables` and `inputs.datasets` where `source == "delta-table"`, substitutes `{catalog}` and `{schema}` placeholders, returns the list of fully-qualified table names.

Default `cards_dir` resolves from the wheel's bundled `workflow-cards/` directory (already force-included via hatchling).

**Workflow card input schema** (canonical example from `wf-publish-spadl-vaep.yaml`):

```yaml
inputs:
  tables:
    - id: "{catalog}.{schema}.fct_action_values"
      source: delta-table
      description: "Gold-layer per-action SPADL records with VAEP value labels"
```

Each `inputs.tables` entry is an object with `id` (FQN with `{catalog}`/`{schema}` placeholders), `source` (literal `"delta-table"`), and `description`. The function filters on `source == "delta-table"` and extracts `id`.

### 3. Upstream Table Resolution Per Task

Each task derives its upstream table list from an existing source — no separately maintained dependency list.

| Task | Source of Upstream Tables |
|------|--------------------------|
| `publish_spadl_vaep_hf` | Workflow card `inputs.tables` (already declared) |
| `publish_xg_shots_hf` | Workflow card `inputs.tables` (to be added) |
| `publish_freeze_frame_hf` | Workflow card `inputs.tables` (to be added) |
| `export_shots_on_target` | Workflow card `inputs.tables` (to be added) |
| `export_scoutgpt_training_data` | Workflow card `inputs.tables` (to be added) |
| `run_model_validation` | Workflow card `inputs.tables` (to be added) |
| `dbt_build_input_marts` | Workflow card `inputs.tables` (existing card, expand inputs) |
| `dbt_build_intermediate_marts` | Workflow card `inputs.tables` (existing card, expand inputs) |
| `dbt_build_output_marts` | Workflow card `inputs.tables` (existing card, expand inputs) |
| `refresh_synced_tables` | Derived from `SYNCED_TABLES` list in code — `list[tuple[str, str | None]]` where second element is per-table schema override (strip `_synced` suffix, apply override or default schema) |

### 4. Workflow Card Changes

#### 3 Existing dbt Cards to Expand

These cards already exist (created in PR-Cycle-C PR-β) but currently have sparse `inputs.datasets` sections (typically a single representative table). Each needs its `inputs` section expanded to list all upstream Delta tables consumed by the corresponding dbt selector, so `resolve_upstream_tables_from_card` returns the complete dependency set.

- **`wf-dbt-build-input-marts.yaml`** — expand `inputs` to list all bronze source tables that feed `+tag:input_mart +tag:dimension`: `statsbomb_events`, `statsbomb_360`, `statsbomb_lineups`, `statsbomb_competitions`, `statsbomb_matches`, `metrica_tracking`, `metrica_events`, `idsse_tracking`, `idsse_events`, `skillcorner_tracking`, `wyscout_events`, `wyscout_matches`, `wyscout_players`, `wyscout_teams`, `player_xref_raw`, `tracking_player_metadata`.
- **`wf-dbt-build-intermediate-marts.yaml`** — expand `inputs` to list stage 1 gold outputs consumed by `+tag:intermediate_mart` plus compute bronze tables: `spadl_actions`, `vaep_action_values`, and the relevant stage 1 gold tables.
- **`wf-dbt-build-output-marts.yaml`** — expand `inputs` to list stage 2 gold outputs plus all compute bronze tables consumed by `tag:output_mart`: `line_breaking_results`, `pitch_control_values`, `off_ball_xt_results`, `defcon_results`, `expected_threat_grids`, `formations_efpi_results`, `formations_shape_graph_results`, `elastic_sync_results`, `pausa_values`, `player_embeddings_raw`, `xg_predictions_v2`.

**Mechanical derivation:** during implementation, derive each card's input list via `dbt ls --resource-type source --select <selector> --output json` rather than manual inspection. This eliminates guesswork and produces an auditable, reproducible list. The lists above are best-effort from manual lineage review; the `dbt ls` output is authoritative.

#### ~5 Existing Cards to Update

Add or correct `inputs.tables` entries with `source: delta-table` for:
- `wf-publish-xg-shots.yaml`
- `wf-publish-freeze-frames.yaml`
- `wf-export-shots.yaml`
- `wf-scoutgpt-export.yaml`
- `wf-model-validation.yaml`

Exact source tables will be verified from each module's `run_pipeline` code during implementation.

### 5. Guard Wiring

#### Watermark Guards (10 tasks)

Guard wiring differs by call site. Standalone tasks (`dbt_runner`, `refresh_synced_tables`, `run_model_validation`) expose a module-level `skip_guard` object following the existing `SkipGuard` protocol, registered in `_GUARD_MODULES`. The `hf_sync` sub-operations do NOT expose module-level guards — they are internal to `hf_sync.py` and use a factory wrapper instead (see below).

**hf_sync sub-operations:** the 5 guardless sub-ops move to watermark-guarded factories in `hf_sync.py`. There are two factory variants because `export_shots_on_target` has a non-standard `run_pipeline` signature:

- **`_make_watermark_op(module_path)`** — for the 4 sub-ops currently on `_make_plain_op` (`publish_spadl_vaep_hf`, `publish_xg_shots_hf`, `publish_freeze_frame_hf`, `export_scoutgpt_training_data`). Their `run_pipeline` signature is `(spark, catalog, schema, logger)`.
- **`_make_watermark_volume_op(module_path)`** — for `export_shots_on_target`, currently on `_make_export_shots_op()`. Its `run_pipeline` signature is `(spark, catalog, schema, volume_path)` — it takes a UC Volume path instead of a logger, and has no `filter_result` kwarg.

Both factories return `Callable[[SparkSession, str, str, logging.Logger], None]` — the same concrete signature as the existing `_make_plain_op` / `_make_export_shots_op` factories (matching the sub-op dispatch in `_run_sub_operations`). Both follow the same logic:
1. Loads upstream tables via `resolve_upstream_tables_from_card`.
2. Calls `check_upstream_freshness`.
3. If `count=0` → logs skip, returns.
4. If `count=1` → calls the module's `run_pipeline(...)` with the appropriate positional args, then `record_watermarks(...)`.

These sub-ops are not registered in `_GUARD_MODULES` (they have no module-level `skip_guard`). The conformance test `TestWatermarkRecordAfterSuccess` verifies the factory pattern via AST scan of `hf_sync.py` (see §7 test 3).

`sync_hf_costs` remains on `_make_plain_op` (legitimately always-run polling task).

**dbt stages — new guard infrastructure in `dbt_runner.py`:** Today `dbt_runner.py` has zero guard infrastructure — no `FilterResult` import, no `skip_guard`, no `timed_check`, no Spark session. The module invokes dbt via `dbtRunner().invoke(args)` (dbt-core CLI runner, not subprocess). Adding the watermark guard requires:

1. **New imports:** `FilterResult`, `timed_check`, `check_upstream_freshness`, `record_watermarks`, `resolve_upstream_tables_from_card` from `guards.py`. Also `get_spark_session` from `utils` (needed for `DESCRIBE HISTORY`).
2. **Module-level `_SELECTOR_TO_CARD` mapping:**

```python
_SELECTOR_TO_CARD: dict[str, str] = {
    "+tag:input_mart +tag:dimension": "wf-dbt-build-input-marts",
    "+tag:intermediate_mart": "wf-dbt-build-intermediate-marts",
    "tag:output_mart": "wf-dbt-build-output-marts",
}
```

3. **Module-level `skip_guard` class** parameterized by workflow card ID, with a `check()` that calls `check_upstream_freshness`.
4. **Guard check placement:** In `main()`, AFTER arg parsing (to know the `--select` value) but BEFORE `run_pipeline()`. Join `--select` args into a string, look up in `_SELECTOR_TO_CARD`. If match found → instantiate guard with that card ID, call `timed_check`. If no match (ad-hoc selector) → fail open (`count=1`).
5. **Watermark recording:** After `run_pipeline()` returns successfully (no `RuntimeError`), call `record_watermarks(...)`. On dbt failure, `run_pipeline` raises `RuntimeError` and watermark is not recorded — correct behavior.
6. **Spark session lifecycle:** Create via `get_spark_session()` only when guard check needs it. The Spark session is used solely for `DESCRIBE HISTORY` and watermark MERGE — dbt itself uses `dbtRunner` (which manages its own Databricks SQL warehouse connection).

**refresh_synced_tables — new guard infrastructure:** Today this module is a CLI utility with zero guard infrastructure. Adding the watermark guard requires:

1. **New imports:** Same set as dbt_runner.
2. **Module-level `skip_guard` class** with a `check()` that derives upstream tables from `SYNCED_TABLES` — for each `(table_name, schema_override)` tuple, strip the `_synced` suffix and qualify as `{catalog}.{schema_override or default_schema}.{base_name}`. The per-table schema override (e.g., `("workflow_cost_live_synced", "observability")` → `{catalog}.observability.workflow_cost_live`) must be respected, not blindly using the default schema.
3. **Guard check placement:** In `main()`, AFTER arg parsing (to know `--catalog`/`--schema`) but BEFORE the refresh-trigger loop. If `count=0` → exit early.
4. **Watermark recording:** After all refreshes complete successfully (zero errors). If any refresh fails, the watermark is not recorded — next run retries all.
5. Registered in `_GUARD_MODULES`.

**model_validation:** Already has guard infrastructure (`skip_guard = _ModelValidationGuard()` at line 54, imports `FilterResult` and `timed_check`). The existing always-run guard (`return FilterResult(count=1)`) is replaced with a watermark guard reading upstream tables from its workflow card. Minimal integration work — the guard class body changes, no structural additions needed.

#### StatsBomb Guard Fix (1 task)

`ingest_statsbomb` guard changes from unconditional `count=1` to:
1. Call `sb.competitions()` — this fetches the competitions JSON from the StatsBomb open-data GitHub repo (raw.githubusercontent.com), not a StatsBomb API endpoint. Unauthenticated GitHub rate limit is 60 requests/hour; one guard check per daily scheduled run is well within this. If the lakehouse transitions to a paid StatsBomb API, the guard implementation changes to call the API endpoint instead.
2. Anti-join against `bronze.statsbomb_competitions` (existing table) to find new competition/season pairs.
3. Anti-join against `bronze.statsbomb_matches` to find new matches in existing competitions.
4. If both empty → `FilterResult(count=0)`.
5. If either has new entries → `FilterResult(count=1)` with metadata containing the new IDs.

This follows the existing `find_new_ids` pattern. The pipeline's internal per-competition/per-match logic still runs as defense-in-depth but is no longer the primary freshness gate.

**Note on StatsBomb open data:** The current lakehouse consumes StatsBomb's open dataset, which updates infrequently (new competitions/seasons added a few times per year). The anti-join guard will correctly return `count=0` on the vast majority of days. When the lakehouse transitions to a paid StatsBomb API with more frequent updates, this same guard pattern still applies — the anti-join check is the same regardless of update frequency.

**Convention for future live-API providers:** any ingestion task that calls an external API must do the freshness check in the guard, not in the pipeline internals. The guard answers "is there new data?" cheaply; the pipeline only starts if there is.

### 6. Guard Pattern Summary

After this work, the four skip guard patterns are:

| Pattern | Purpose | Used By |
|---------|---------|---------|
| Anti-join (`find_new_ids`) | "Which specific IDs are new?" + skip if none | Compute tasks (20+) |
| HF SHA (`check_hf_dataset_freshness`) | "Has the HF repo commit changed?" | Import tasks (3) |
| Count-based (hardcoded expected count) | "Have all known static items been ingested?" | Static dataset ingestion (5) |
| **Watermark** (`check_upstream_freshness`) | "Has any upstream Delta table changed?" | Publishers, dbt, refresh, validation (10) |

### 7. Testing

#### New Tests

1. **`test_watermark_freshness.py`** — unit tests for `check_upstream_freshness` and `record_watermarks` with mocked Spark:
   - First run (no stored watermarks) → `count=1`.
   - All versions match → `count=0`.
   - One upstream changed → `count=1`.
   - Table doesn't exist (bootstrap) → `count=1` (fail open).
   - `DESCRIBE HISTORY` returns only OPTIMIZE/VACUUM ops → no change detected.
   - `record_watermarks` writes correct versions via MERGE.

2. **`TestWatermarkGuardHasCardInputs`** in `test_guard_conformance.py` — for every guard using `check_upstream_freshness`, verify its workflow card has non-empty `inputs.tables` or `inputs.datasets` with `source: delta-table`.

3. **`TestWatermarkRecordAfterSuccess`** in `test_guard_conformance.py` — AST scan with two targets: (a) standalone modules with module-level watermark guards (`model_validation`, `dbt_runner`, `refresh_synced_tables`) must call `record_watermarks(...)` after `run_pipeline(...)`, and (b) `hf_sync.py`'s `_make_watermark_op` and `_make_watermark_volume_op` factory functions must call `record_watermarks(...)` after the downstream `run_pipeline(...)` call.

#### Extended Tests

4. **`_GUARD_MODULES` registration** — add `refresh_synced_tables` and the dbt_runner entries. Existing auto-discovery tests verify `skip_guard` exposure.

5. **`TestSelectorToCardParity`** in `test_guard_conformance.py` — verify every key in `_SELECTOR_TO_CARD` corresponds to a dbt task in the Terraform workflow definition, and every dbt task in TF has a matching entry in `_SELECTOR_TO_CARD`. Prevents drift when dbt stages are added or renamed.

6. **StatsBomb guard unit test** — mock `sb.competitions()` response, verify anti-join logic returns `count=0` when no new data and `count=1` with metadata when new competitions/matches exist.

### 8. Cost Quantification

**Phase 0 task:** Before implementing guards, measure the current cost of no-op runs. Query `observability.workflow_cost_live` for the 10 target tasks over the last 30 days, filtering to runs where the task completed but wrote zero rows (or the equivalent "no work done" signal). Sum the DBU cost. This gives the baseline savings number and prioritizes which tasks to guard first if the implementation is staged.

Existing workflow card cost estimates for the 3 dbt stages total ~$0.42/day ($0.14 + $0.07 + $0.21). The 5 HF publisher sub-ops, refresh_synced_tables, and model_validation add further. Exact per-task steady-state cost will be established by the Phase 0 query.

### 9. Future: Preflight Task Upgrade (Approach B)

Not in scope, but the design supports a clean upgrade path:

1. Create a `preflight_watermark` task in the "default" environment.
2. It calls `check_upstream_freshness` for all watermark-guarded workflows in one pass.
3. Emits skip/run decisions as Databricks task values.
4. Downstream tasks read those values and skip immediately without booting their environments.

The `check_upstream_freshness` function, `workflow_watermarks` table, and card-reading utility are identical in both approaches — only the call site moves from inside the task to the preflight.

### 10. Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Workflow card `inputs.tables` drifts from actual code reads | Conformance test `TestWatermarkGuardHasCardInputs` + implementation-time verification against source code |
| `DESCRIBE HISTORY` filtered to data ops misses an operation type | Conservative operation allowlist; fail open on unrecognized ops |
| Watermark not recorded on partial success | `record_watermarks` called only after `run_pipeline` returns successfully; failed runs leave watermark unchanged → next run retries |
| dbt model SQL changes need a run but watermark says "no upstream change" | Acceptable: SQL changes go through PRs → CI runs dbt. Daily scheduled run skipping is correct — the data hasn't changed. |
| `SYNCED_TABLES` list in code changes but watermark guard not updated | Guard derives from the list dynamically — no separate list to maintain |
| Stale watermark blocks a task from running after a manual data fix | Operator runbook: `DELETE FROM observability.workflow_watermarks WHERE workflow_id = '<id>'` forces the next run to treat all upstreams as changed. Safe because deletion only causes one redundant re-run. |
