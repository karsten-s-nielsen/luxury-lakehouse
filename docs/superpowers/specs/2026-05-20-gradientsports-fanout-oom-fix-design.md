# Gradient Sports Fan-Out + OOM Fix

**Origin**: Production failure 2026-05-19 — `ingest_gradientsports` task crashed on match 10508 (Morocco vs Spain, extra time, 5.3M tracking rows).
**TODO ref**: N/A (incident-driven, not from optimization audit).

## 1. Problem Statement

Two coupled problems in the Gradient Sports ingestion pipeline:

### 1.1 Spark Connect serialization limit (CRITICAL)

`write_tracking()` calls `spark.createDataFrame(df)` where `df` is a 4-5M row pandas DataFrame. Spark Connect serializes the entire pandas DF as an Arrow batch in the RPC payload. Match 10508 (extra time) produced 5,347,807 rows x 26 columns = **305 MB serialized**, exceeding the **256 MB `spark.rpc.message.maxSize`** hard limit. On Databricks Serverless, this config is not tunable.

The first 6 matches succeeded (4.0-4.1M rows ~= 230 MB each). Extra-time matches (5.3M+ rows) cross the threshold. This is a hard ceiling — it will fail on every extra-time match.

**Empirical data from the failed run:**

| Match | Teams | Tracking rows | Status |
|-------|-------|--------------|--------|
| 10502 | Netherlands vs United States | 4,020,484 | OK |
| 10503 | Argentina vs Australia | 4,082,499 | OK |
| 10504 | France vs Poland | 4,145,024 | OK |
| 10505 | England vs Senegal | 3,996,542 | OK |
| 10506 | Japan vs Croatia (ET) | 5,325,630 | OK |
| 10507 | Brazil vs South Korea | 4,048,756 | OK |
| 10508 | Morocco vs Spain (ET) | 5,347,807 | **FAILED** (305 MB > 256 MB) |

Match 10506 (Japan vs Croatia) succeeded at 5.3M rows despite also having extra time — it was likely right at the threshold. The failure is non-deterministic near the boundary because serialized size depends on data distribution, not just row count.

### 1.2 Sequential processing of 64 matches (PERFORMANCE)

The current monolithic `ingest_gradientsports` task processes all matches sequentially in a single Databricks task. At ~2.5 min/match, 64 matches = ~160 min wall clock. This exceeds the 1800s (30 min) task timeout and wastes DBU on serial API calls that have no data dependency between matches.

## 2. Design

### 2.1 Fix serialization limit: Parquet staging via UC Volume

**Root cause**: `spark.createDataFrame(pandas_df)` sends the entire pandas DataFrame through the Spark Connect wire protocol as a single Arrow batch. Large DataFrames exceed the 256 MB `spark.rpc.message.maxSize` hard limit.

**Fix**: Write the pandas DataFrame to Parquet on a UC Volume staging path, then `spark.read.parquet()` to create the Spark DataFrame. This bypasses the RPC limit entirely — data flows through cloud storage, not the wire protocol.

**Changes to `src/ingestion/gradientsports_tracking.py`**:

1. **`write_tracking()`**: Replace `spark.createDataFrame(df)` with:
   - Create staging directory: `os.makedirs(parent, exist_ok=True)` — pandas `to_parquet()` does not create parent directories. FUSE handles this transparently on Databricks; `exist_ok=True` makes it safe for concurrent iterations writing to different `match_id` subdirectories.
   - Write pandas DF to a temp Parquet file on the UC Volume staging path: `/Volumes/{catalog}/{schema}/_staging/gradientsports_tracking/{match_id}.parquet`
   - Read it back: `spark.read.parquet(staging_path)`
   - After successful Delta write, delete the staging file

2. **Staging path convention**: `/Volumes/{catalog}/{schema}/_staging/gradientsports_tracking/{match_id}.parquet`. The `{schema}` parameter (currently `bronze`) flows from the CLI args — not hardcoded. The `_staging` prefix signals "transient, not a data table". Cleanup after write makes this self-maintaining. If the task crashes between staging write and cleanup, the next run overwrites the same path (idempotent).

**Why not apply to events?**: Events are ~2K rows/match (max 3K). Never close to the 256 MB limit. No change needed — `spark.createDataFrame()` is fine for small DataFrames.

**Alternative considered — chunked `createDataFrame` with `union()`**: Splits the pandas DF into N chunks, creates N Spark DFs, unions them. Rejected because: (a) each chunk still goes through the RPC wire, (b) union creates N partitions in the DAG, (c) more complex, fragile to threshold changes, and (d) UC Volume Parquet is the established pattern in `databricks-serverless.md`.

### 2.2 Fan-out: preflight + for_each_task

**Pattern**: Same as IDSSE (PR Cycle-A), SPADL/VAEP, and Tracking Context. Three components:

#### 2.2.1 Preflight task (`preflight_gradientsports`)

New entry point `main_preflight()` in `gradientsports.py`:
1. Runs the existing `_GradientSportsGuard.check()` — discovers matches via API, returns `FilterResult` with match list in metadata.
2. Serializes each `MatchInfo` as a JSON string via `m.model_dump_json()` and emits the list as the task value array. Each element is a complete `MatchInfo` that the iteration can deserialize directly via `MatchInfo.model_validate_json()`. One match per iteration — no chunking needed because each match is independent and takes ~2-3 min.
3. Writes the array via `dbutils.jobs.taskValues.set(key="gradientsports_matches", value=match_jsons)`.
4. If no matches found (guard returns count=0), emits empty list `[]` — `for_each_task` spawns 0 iterations.

**Guard change**: The guard currently stores full `MatchInfo` objects in `FilterResult.metadata`. The preflight needs the match list to emit IDs, but each iteration also needs the match's artifact keys to download. Two options:

- **Option A**: Preflight emits just match IDs, each iteration re-calls the discovery API to get its own `MatchInfo`. Downside: 64 API calls instead of 1.
- **Option B (recommended)**: Preflight serializes the full match list as a JSON task value. Each iteration receives its match ID via `{{input}}`, but also needs `MatchInfo` to know artifact keys. Since `for_each_task` can only pass `{{input}}` (a single string per iteration), the preflight encodes each element as a JSON string: `'{"id": "10502", "artifacts": {...}, "home": "...", "away": "..."}'`. The iteration deserializes this from `{{input}}`.

**Recommendation: Option B.** One API call in preflight, zero API calls per iteration. The `{{input}}` string is a JSON-serialized `MatchInfo` dict. The iteration entry point deserializes it: `match = MatchInfo.model_validate_json(match_json)`.

#### 2.2.2 Iteration entry point

Modify `main()` to accept `--match-json` argument:
- When `--match-json` is provided: single-match mode (for_each_task iteration). Deserialize the JSON, ingest that one match.
- When `--match-json` is absent: legacy standalone mode (runs guard + full sequential ingestion). Kept for manual CLI usage.

The iteration entry point is the same `ingest_gradientsports` wheel entry point — no new entry point needed. The preflight gets a new `preflight_gradientsports` entry point.

**Convention note — `--match-json` vs `--match-ids`**: IDSSE uses `--match-ids` (comma-separated IDs) because its preflight chunks multiple matches per iteration. Gradient Sports uses `--match-json` (JSON-serialized `MatchInfo`) because each iteration handles exactly one match and needs the full `MatchInfo` including artifact keys — re-fetching artifact metadata per iteration would add 64 unnecessary API calls. This is a deliberate convention deviation, not a bug. If future fan-out pipelines need artifact metadata, `--match-json` is the better pattern. Consider standardizing across all fan-out pipelines in a future cycle.

#### 2.2.3 Terraform changes

Replace the monolithic `ingest_gradientsports` task block with:

```hcl
# Preflight: discover matches, emit task value
task {
  task_key        = "preflight_gradientsports"
  timeout_seconds = 300
  max_retries     = 0

  python_wheel_task {
    package_name = "luxury_lakehouse"
    entry_point  = "preflight_gradientsports"
    parameters   = ["--catalog", var.catalog_name, "--schema", "bronze"]
  }
  environment_key = "default"
}

# Fan-out: one iteration per match
task {
  task_key = "ingest_gradientsports"

  depends_on {
    task_key = "preflight_gradientsports"
  }

  for_each_task {
    inputs      = "{{tasks.preflight_gradientsports.values.gradientsports_matches}}"
    concurrency = 8

    task {
      task_key        = "ingest_gradientsports_iteration"
      timeout_seconds = 900
      max_retries     = 1  # API calls — transient failures benefit from retry

      python_wheel_task {
        package_name = "luxury_lakehouse"
        entry_point  = "ingest_gradientsports"
        parameters   = [
          "--catalog", var.catalog_name,
          "--schema", "bronze",
          "--match-json", "{{input}}",
        ]
      }
      environment_key = "default"
    }
  }
}
```

**Concurrency = 8**: At ~2.5 min/match, 64 matches with 8 parallel runners = ~20 min wall clock. `for_each_task` never spawns more runners than inputs, so 3 matches = 3 runners. No wasted resources.

**Timeout = 900s per iteration**: Single match takes ~2.5 min (download + parse + write). 900s (15 min) gives 6x headroom for slow API responses or large extra-time matches.

**max_retries = 1 for iterations**: Each iteration makes API calls (fetch_artifact) which can fail transiently. Unlike the monolithic task, guard-based retry masking does NOT apply here because the iteration receives its match directly — there is no guard in the iteration path.

**max_retries = 0 for preflight**: Preflight runs the guard. If it fails, the downstream for_each_task gets no inputs and spawns 0 iterations (clean no-op). Retrying the preflight would re-run the API discovery, which is safe but unnecessary.

### 2.3 Watermark safety under fan-out

The skip guard reads `MAX(_ingested_at)` from `bronze.gradientsports_events`. With parallel iterations writing events for different matches, each `replaceWhere(match_id = 'X')` is independent — no cross-match interference. The guard's `updatedSince` cutoff is set once in the preflight and is not affected by parallel writes.

**Partial failure handling**: If iteration for match 10508 fails but 10507 succeeds, the guard's `MAX(_ingested_at)` advances to 10507's timestamp. On the next daily run, the preflight re-discovers 10508 (and any newer matches) via the API's `updatedSince` filter. This is correct because the API returns matches updated since the cutoff, and the failed match was never committed to the events table.

### 2.4 Downstream dependency changes

The `dbt_build_input_marts` task currently `depends_on { task_key = "ingest_gradientsports" }`. This continues to work because `ingest_gradientsports` remains the parent task key of the for_each_task — Databricks resolves dependencies against the parent, not individual iterations.

## 3. Scope

### In scope

| File | Change |
|------|--------|
| `src/ingestion/gradientsports_tracking.py` | Parquet staging in `write_tracking()` |
| `src/ingestion/gradientsports.py` | Add `main_preflight()`, add `--match-json` to `main()` |
| `terraform/modules/workflows/main.tf` | Replace monolithic task with preflight + for_each_task |
| `pyproject.toml` | Add `preflight_gradientsports` entry point |
| `scripts/patch_job_retries.py` | Add `ingest_gradientsports_iteration` to `_INGESTION_TASK_KEYS`; update `ingest_gradientsports` exclusion comment (see §3.1) |
| `dbt_project/seeds/task_workflow_mapping.csv` | Add `preflight_gradientsports` row |
| `workflow-cards/wf-gradientsports.yaml` | Update execution section (entry_point, distribution) |
| `src/tests/test_gradientsports_ingestion.py` | Update/extend for preflight + iteration modes |

### 3.1 `patch_job_retries.py` classification (CRITICAL)

Three task keys affected by this change. Each must be classified correctly in `_INGESTION_TASK_KEYS`:

| Task key | Classification | `_INGESTION_TASK_KEYS`? | Reason |
|----------|---------------|------------------------|--------|
| `preflight_gradientsports` | Compute (guard + API discovery) | **No** | Guard-only task. If it fails, for_each_task gets no inputs → 0 iterations (clean no-op). max_retries=0 in TF. |
| `ingest_gradientsports` | for_each_task parent | **No** | Parent wrapper — no direct execution, no max_retries of its own. Update existing exclusion comment from "guard-based retry masks failures" to "for_each_task parent — no direct execution". |
| `ingest_gradientsports_iteration` | Ingestion (API calls) | **Yes — add** | Each iteration makes API calls (fetch_artifact) which can fail transiently. No guard in iteration path, so retry masking does not apply. max_retries=1 in TF. |

The existing `ingest_gradientsports` exclusion comment (line 62-63 of `patch_job_retries.py`) must be updated:
```python
# ingest_gradientsports is a for_each_task parent (no max_retries of its own)
```

### Not in scope

- No `gradientsports_events.py` changes (events are ~2K rows, well under 256 MB limit)
- No `gradientsports_common.py` changes
- No dbt changes
- No new workflow cards (same `wf-gradientsports`)
- Wheel version bump via `uv run python scripts/bump_wheel.py` is needed before deploy (standard procedure, not a code change)

## 4. Verification Strategy

### 4.1 Parquet staging tests

1. **Source-code guard test**: AST scan of `gradientsports_tracking.py` — assert `spark.createDataFrame` does NOT appear anywhere in the module. This is a regression guard: if someone reverts the Parquet staging fix, the test fails before CI. Pattern: `ast.parse(source)` → walk for `ast.Call` nodes matching `createDataFrame` → fail on match.

2. **Parquet staging flow test (mock spark)**: Assert `write_tracking()` calls `df.to_parquet(staging_path)` then `spark.read.parquet(staging_path)` then deletes the staging file after the Delta write. Verifiable with mocked spark — tests the control flow, not the data.

3. **Parquet schema round-trip test (no PySpark)**: Write a synthetic 26-column pandas DF (matching `gradientsports_tracking` schema, including edge cases: NaN floats, empty strings, max-length player names) to Parquet, read back with `pd.read_parquet()`, assert column names and dtypes survive (especially `float64` preservation, `object` → `string`). Spark's Parquet reader is Spark's responsibility — this test validates the pandas → Parquet → pandas layer where int64/float64 widening matters.

### 4.2 Preflight tests

4. **Preflight task value format**: Mock the guard to return a known match list with 3 matches. Assert `main_preflight()` produces a JSON array of 3 elements, each parseable via `MatchInfo.model_validate_json()`.

5. **MatchInfo JSON round-trip**: Assert `MatchInfo.model_validate_json(match.model_dump_json()) == match` for a `MatchInfo` with all field types populated (string, list, datetime). Uses Pydantic's native `model_dump_json()` — NOT `json.dumps(m.model_dump())` which raises `TypeError` on `datetime` fields. The preflight production code must also use `model_dump_json()` for the same reason.

6. **Empty guard result**: Mock the guard to return `count=0`. Assert `main_preflight()` emits `[]` via task values.

### 4.3 Iteration tests

7. **`--match-json` deserialization**: Call `main()` with `--match-json '<valid JSON>'`. Assert `ingest_gradientsports()` is called with a single-element match list containing the correct `MatchInfo`.

8. **Write-ordering invariant (TestIngestAtomicity)**: The existing `TestIngestAtomicity` tests assert that tracking is written BEFORE events (events._ingested_at is the watermark). These tests must survive the refactor — verify they still pass in single-match mode (iteration path). If the test class is renamed or restructured, the write-ordering assertion must be preserved.

### 4.4 Guard conformance

9. **`test_guard_conformance.py` passes**: The guard itself is unchanged, but guard invocation moves from `main()` to `main_preflight()`. The conformance test patches `ingestion.gradientsports.fetch_match_list` — verify the patch target is still valid. `wf-gradientsports` is in the conformance exemption list (line 32) and should remain there.

### 4.5 TF parity tests

10. **`test_job_retry_policy.py`**: Must pass with the updated `_INGESTION_TASK_KEYS` (adding `ingest_gradientsports_iteration`, updating `ingest_gradientsports` exclusion comment).
11. **`test_workflows_tf_ordering.py`**: Must pass with the new preflight + for_each_task structure.

### 4.6 Production validation

12. **Post-deploy**: Trigger `preflight_gradientsports` manually, verify it emits the match list, then trigger the full fan-out. Verify all 64 matches ingest (including extra-time matches 10506/10508 that previously failed/were borderline).

## 5. Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Parquet staging path collision | Path includes `match_id` — unique per match. Parallel iterations write to different paths. |
| Staging file left behind on crash | Next run overwrites the same path. Self-cleaning by design. |
| `{{input}}` JSON string too long for task value | `MatchInfo` is ~200 bytes JSON per match. 64 matches = ~13 KB array. Databricks task values support up to 48 KB. Safe. |
| API rate limit under concurrency=8 | pining-for-the-data has no documented rate limit. Each iteration makes 2 API calls (events + tracking artifact). 8 concurrent = 16 concurrent requests. If rate-limited, the built-in `fetch_url` retry-with-backoff handles it. |
| Downstream `depends_on` broken | `ingest_gradientsports` remains the parent task_key. Databricks resolves against parent. Verified pattern from IDSSE. |
| `test_guard_conformance.py` patch target stale | Guard is unchanged. Patch target (`ingestion.gradientsports.fetch_match_list`) remains valid — `main_preflight()` calls the same guard. Verify in §4.4. |
| `patch_job_retries.py` misclassification | Explicit 3-task classification table in §3.1. `test_job_retry_policy.py` enforces parity. |
| MatchInfo JSON round-trip lossy | Explicit round-trip test (§4.2 item 4) for all field types including datetime. |
| `createDataFrame` silently reintroduced | AST source-code guard test (§4.1 item 1) — regression-proof. |
| Write-ordering invariant broken by refactor | Existing `TestIngestAtomicity` must survive; verified in §4.3 item 7. |
