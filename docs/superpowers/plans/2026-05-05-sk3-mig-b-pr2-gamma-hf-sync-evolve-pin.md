# SK3-MIG-B PR-2: gamma Trainer Rewrite + wf-hf-sync Amendment + Evolve Pin-Drift

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite 3 trainers (f2v_v2, f2v_360, scoutgpt) to read from gold marts via Databricks SQL instead of stale HF datasets; wire scoutgpt export + 3 Group 0 publishers into wf-hf-sync for daily refresh; add evolve SHA-pinning discipline to prevent silent data drift mid-experiment.

**Architecture:** gamma trainers replicate the f2v_v1 pattern: `query_databricks_sql()` over HTTP Statement Execution API -> pandas DataFrame -> existing training pipeline. wf-hf-sync amendment adds 4 new sub-operations to `_SUB_OPERATIONS` in `src/ingestion/hf_sync.py`. Evolve pin-drift uses module-level `PINNED_DATASET_SHA` constants + importlib CI sentinel + `bump_evolve_pin.py` operator helper.

**Tech Stack:** Python 3.10, pandas, pyarrow, requests (SQL fetch), huggingface-hub, PySpark (Databricks-runtime publishers only), pytest + importlib (sentinels)

**Spec:** `docs/superpowers/specs/2026-05-04-sk3-mig-b-orchestrator-data-source-and-flavor-alignment-design.md` sections §2.1, §2.4a, §2.10.6, §2.12. TDD steps 8-10, 14a, 14b, 15-17, 19.

**Predecessor:** PR-1 (PR #254, main `98e0b67`), PR-1.5 (PR #255, `6dfef1b`), PR #257-258 (main `02c0455`). Wheel 0.3.32.

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/ingestion/databricks_sql_fetch.py` | Shared `query_databricks_sql()` helper extracted from f2v_v1 pattern — avoids 3x copy in gamma trainers |
| `src/ingestion/publish_spadl_vaep_hf.py` | Databricks-runtime counterpart of `scripts/publish_spadl_vaep_hf.py` — reads via `spark.sql()`, uploads to HF Hub |
| `src/ingestion/publish_xg_shots_hf.py` | Databricks-runtime counterpart of `scripts/publish_xg_shots_hf.py` |
| `src/ingestion/publish_freeze_frame_hf.py` | Databricks-runtime counterpart of `scripts/publish_freeze_frame_hf.py` |
| `workflow-cards/wf-publish-spadl-vaep.yaml` | Workflow card for Databricks-runtime SPADL/VAEP publisher |
| `workflow-cards/wf-publish-xg-shots.yaml` | Workflow card for Databricks-runtime xG shots publisher |
| `workflow-cards/wf-publish-freeze-frames.yaml` | Workflow card for Databricks-runtime freeze-frame publisher |
| `src/tests/test_evolve_pin_drift.py` | Evolve pin-drift CI sentinel (env-gated) |
| `src/tests/test_orchestrator_input_dataset_upstream.py` | §2.10.6 input-dataset upstream sentinel |
| `scripts/bump_evolve_pin.py` | Operator helper to bump `PINNED_DATASET_SHA` constants |

### Modified files

| File | Change |
|------|--------|
| `scripts/train_football2vec_v2.py` | gamma: replace HF dataset load with SQL fetch via `query_databricks_sql()` |
| `scripts/train_football2vec_360.py` | gamma: replace HF dataset load with SQL fetch |
| `scripts/train_football2vec_360_helpers.py` | gamma: add `load_training_data_sql()` alongside existing `load_training_data()` |
| `scripts/train_scoutgpt_hf.py` | gamma: replace HF dataset load with SQL fetch |
| `src/ingestion/football2vec_v2_training.py` | gamma: add `load_training_data_sql()` alongside existing `load_training_data()` |
| `src/analytics/scoutgpt_training.py` | gamma: add `load_training_data_sql()` alongside existing `load_training_data()` |
| `src/ingestion/hf_sync.py` | Add 4 new sub-operations to `_SUB_OPERATIONS` |
| `workflow-cards/wf-hf-sync.yaml` | Add 4 new sub-operation entries |
| `workflow-cards/wf-scoutgpt-export.yaml` | Change `trigger: manual` -> `trigger: orchestrated` |
| `scripts/evaluate_football2vec_l2_adversary_seeds.py` | Add `PINNED_DATASET_*` constants + `revision=` kwarg |
| `scripts/evaluate_scoutgpt_l2_seeds.py` | Add `PINNED_DATASET_*` constants + `revision=` kwarg |
| `src/evolve/targets/football2vec/evaluator.py` | Add `PINNED_DATASET_*` constants + `revision=` kwarg |
| ~~`src/evolve/backends/remote_ssh.py`~~ | ~~Removed: dispatcher, not a dataset consumer — see C1 review~~ |
| `pyproject.toml` | Add 3 new `[project.scripts]` entry points for Databricks-runtime publishers |
| `src/shared/wheel.py` | Version bump (if needed for wheel rebuild) |

---

## Task 1: Extract `query_databricks_sql` to shared wheel module

**Spec:** §2.1 preparation — DRY the SQL fetch helper so all 3 gamma trainers can import it from the wheel instead of copy-pasting 40 lines each.

**Files:**
- Create: `src/ingestion/databricks_sql_fetch.py`
- Test: `src/tests/test_databricks_sql_fetch.py`

- [ ] **Step 1: Write the failing test**

```python
# src/tests/test_databricks_sql_fetch.py
"""Smoke test for the query_databricks_sql helper — module-level import only."""
from ingestion.databricks_sql_fetch import query_databricks_sql

def test_query_databricks_sql_is_callable():
    assert callable(query_databricks_sql)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_databricks_sql_fetch.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ingestion.databricks_sql_fetch'`

- [ ] **Step 3: Create the module**

Extract `query_databricks_sql` from `scripts/train_football2vec.py:139-182` into `src/ingestion/databricks_sql_fetch.py`. Keep the exact same signature and implementation. Add the timeout constants as module-level. The function reads from Databricks Statement Execution API with EXTERNAL_LINKS + ARROW_STREAM, polls for completion, downloads Arrow chunks, and returns a pandas DataFrame.

```python
# src/ingestion/databricks_sql_fetch.py
"""Databricks SQL Statement Execution API helper for HF Jobs trainers.

Extracted from scripts/train_football2vec.py (f2v_v1) so gamma trainers
(f2v_v2, f2v_360, scoutgpt) can import from the wheel without copy-paste.

NOT for Databricks-runtime code — use spark.sql() there. This helper exists
for PEP 723 scripts running on HF Jobs (no Spark, no databricks-sdk SQL
connector) that need to query gold marts via HTTP.
"""
from __future__ import annotations

import logging
import time

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_POLL_INTERVAL_S = 2.0
_TIMEOUT_SUBMIT = (10, 120)
_TIMEOUT_POLL = (10, 30)
_TIMEOUT_CHUNK = (10, 300)


def query_databricks_sql(host: str, token: str, sql: str, warehouse_id: str) -> pd.DataFrame:
    """Execute SQL via Databricks Statement Execution API + Arrow chunks.

    Args:
        host: Databricks workspace hostname (no scheme, no trailing slash).
        token: Databricks PAT or OAuth token.
        sql: SQL statement to execute.
        warehouse_id: SQL warehouse ID.

    Returns:
        pandas DataFrame with query results.

    Raises:
        RuntimeError: If SQL execution fails or returns no data chunks.
    """
    url = f"https://{host}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=_TIMEOUT_SUBMIT, verify=True)
    resp.raise_for_status()
    result = resp.json()
    statement_id = result.get("statement_id")
    status = result.get("status", {}).get("state")
    while status in ("PENDING", "RUNNING"):
        time.sleep(_POLL_INTERVAL_S)
        poll_resp = requests.get(f"{url}/{statement_id}", headers=headers, timeout=_TIMEOUT_POLL, verify=True)
        poll_resp.raise_for_status()
        result = poll_resp.json()
        status = result.get("status", {}).get("state")
    if status != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL {status}: {err.get('message', '?')}")

    manifest = result.get("manifest", {})
    total_chunks = int(manifest.get("total_chunk_count", 0) or 0)

    import pyarrow as pa

    arrow_tables: list[pa.Table] = []
    for chunk_idx in range(total_chunks):
        chunk_url = f"{url}/{statement_id}/result/chunks/{chunk_idx}"
        chunk_resp = requests.get(chunk_url, headers=headers, timeout=_TIMEOUT_CHUNK, verify=True)
        chunk_resp.raise_for_status()
        for link_info in chunk_resp.json().get("external_links", []):
            dl_resp = requests.get(link_info["external_link"], timeout=_TIMEOUT_CHUNK, verify=True)
            dl_resp.raise_for_status()
            reader = pa.ipc.open_stream(dl_resp.content)
            arrow_tables.append(reader.read_all())
    if not arrow_tables:
        raise RuntimeError("No data chunks returned from Databricks SQL")
    combined = pa.concat_tables(arrow_tables).to_pandas()
    logger.info("SQL fetch returned %d rows", len(combined))
    return combined
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_databricks_sql_fetch.py -v`
Expected: PASS

- [ ] **Step 5: Verify ruff + pyright**

Run: `uv run ruff check src/ingestion/databricks_sql_fetch.py && uv run pyright src/ingestion/databricks_sql_fetch.py`

---

## Task 2: gamma trainer rewrite — f2v_v2

**Spec:** §2.1, §4 Step 8. Replace `load_training_data()` (HF Hub download) with SQL fetch from `fct_action_values JOIN dim_players`.

**Files:**
- Modify: `src/ingestion/football2vec_v2_training.py` (add `load_training_data_sql`)
- Modify: `scripts/train_football2vec_v2.py` (switch data source)

The f2v_v2 export module (`src/ingestion/export_embeddings_training_data.py`) produces per-player-match rows with columns: `canonical_player_id, match_id, competition_id, season_id, position_group, actions` (array of struct: `action_type int, x float, y float, result int`).

The gamma rewrite fetches the same source data via SQL, then does the action-type mapping + coordinate normalization + grouping in pandas.

- [ ] **Step 1: Add `load_training_data_sql` to `football2vec_v2_training.py`**

Add a new function alongside existing `load_training_data()` (which stays for backward compat / evolve consumers). The new function:
1. Calls `query_databricks_sql()` with SQL that matches the export module's query (line 146-167 of `export_embeddings_training_data.py`)
2. Maps action_type string -> int using `_ACTION_TYPE_IDS` (same 23-type vocab)
3. Normalizes coordinates (start_x / 105.0, start_y / 68.0)
4. Binarizes result (success -> 1, else 0)
5. Groups by (canonical_player_id, match_id) to produce one row per player-match with actions as list-of-dicts
6. Returns `(pd.DataFrame, str)` — same signature as `load_training_data`; commit hash replaced with a SQL-fetch timestamp

Key SQL (mirrors `export_embeddings_training_data.py:_export_training_sequences` lines 146-167):

```sql
SELECT
    CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
    CAST(av.match_id AS STRING) AS match_id,
    CAST(av.competition_id AS INT) AS competition_id,
    CAST(av.season_id AS INT) AS season_id,
    dp.position_group,
    av.action_type,
    av.start_x,
    av.start_y,
    av.action_result,
    av.period,
    av.time_seconds
FROM soccer_analytics.dev_gold.fct_action_values av
INNER JOIN soccer_analytics.dev_gold.dim_players dp
    ON av.player_key = dp.player_key
WHERE av.player_id IS NOT NULL
  AND dp.canonical_player_id IS NOT NULL
  AND av.action_type IS NOT NULL
  AND av.start_x IS NOT NULL
  AND av.start_y IS NOT NULL
```

NOTE: The export module joins on `player_id` but the f2v_v1 reference joins on `player_key`. Check which is correct for this context — `player_key` is the Kimball surrogate (ADR-011). Use `player_key` for consistency with f2v_v1.

Pandas transform after SQL fetch:

```python
_PITCH_LENGTH = 105.0
_PITCH_WIDTH = 68.0
_ACTION_TYPE_IDS = { ... }  # same 23-entry dict

def _transform_to_training_data(raw: pd.DataFrame) -> pd.DataFrame:
    raw["action_type_id"] = raw["action_type"].map(_ACTION_TYPE_IDS).fillna(20).astype(int)
    raw["x_norm"] = (raw["start_x"] / _PITCH_LENGTH).astype(float)
    raw["y_norm"] = (raw["start_y"] / _PITCH_WIDTH).astype(float)
    raw["result_binary"] = (raw["action_result"] == "success").astype(int)
    raw = raw.sort_values(["canonical_player_id", "match_id", "period", "time_seconds"])

    # Vectorized groupby — H2 review: .iterrows() is O(n×m) on 9.53M rows.
    # Build actions column via .to_dict("records") on pre-sliced columns.
    action_cols = ["action_type_id", "x_norm", "y_norm", "result_binary"]
    rename_map = {"action_type_id": "action_type", "x_norm": "x", "y_norm": "y", "result_binary": "result"}

    grouped = raw.groupby(["canonical_player_id", "match_id"], sort=False)
    actions_series = grouped.apply(
        lambda grp: grp[action_cols].rename(columns=rename_map).to_dict("records"),
        include_groups=False,
    )
    meta = grouped.first()[["competition_id", "season_id", "position_group"]].copy()
    meta["competition_id"] = meta["competition_id"].fillna(0).astype(int)
    meta["season_id"] = meta["season_id"].fillna(0).astype(int)
    meta["actions"] = actions_series
    return meta.reset_index()
```

- [ ] **Step 2: Update `scripts/train_football2vec_v2.py` to use SQL fetch**

In `_run_stage1` and `_run_stage2`, replace:
```python
data, dc = load_training_data(hf_token, TRAINING_DATASET)
```
with:
```python
host = os.environ["DATABRICKS_HOST"].replace("https://", "").replace("http://", "").rstrip("/")
db_token = os.environ["DATABRICKS_TOKEN"]
warehouse_id = os.environ["DATABRICKS_SQL_WAREHOUSE_ID"]
data, dc = load_training_data_sql(host, db_token, warehouse_id)
```

Add `DATABRICKS_TOKEN`, `DATABRICKS_HOST`, `DATABRICKS_SQL_WAREHOUSE_ID` to the env-var reads at the top of `main()`. Add `databricks-sdk` or `requests` to PEP 723 deps if not present (requests is already in the wheel). Remove `datasets>=3.0` from PEP 723 deps since HF dataset loading is no longer used.

Update the Usage docstring to include `--env DATABRICKS_SQL_WAREHOUSE_ID=...` and `--secrets DATABRICKS_TOKEN=...`.

- [ ] **Step 3: Run ruff + pyright on modified files**

Run: `uv run ruff check src/ingestion/football2vec_v2_training.py scripts/train_football2vec_v2.py && uv run pyright src/ingestion/football2vec_v2_training.py`

- [ ] **Step 4: Run existing f2v_v2 tests**

Run: `uv run pytest src/tests/test_football2vec_v2_training.py -v`
Expected: PASS (existing tests should not break — they don't call `load_training_data` directly)

---

## Task 3: gamma trainer rewrite — f2v_360

**Spec:** §2.1, §4 Step 9. Same pattern as Task 2 but additionally requires freeze-frame data from `stg_statsbomb__360`.

**Files:**
- Modify: `scripts/train_football2vec_360_helpers.py` (add `load_training_data_sql`)
- Modify: `scripts/train_football2vec_360.py` (switch data source)

The 360 trainer requires TWO data streams:
1. **Actions**: same as f2v_v2 (from `fct_action_values JOIN dim_players`)
2. **Freeze frames**: from `stg_statsbomb__360` joined on action-level keys

The export module (`prepare_360_training_data.py`) uses Spark window functions + `applyInPandas` for the freeze-frame join. For the gamma trainer, we fetch both via separate SQL queries and join in pandas.

- [ ] **Step 1: Add `load_training_data_sql` to `train_football2vec_360_helpers.py`**

Two SQL queries:
1. Actions query (same as Task 2's SQL, limited to StatsBomb since only StatsBomb has 360 data)
2. Freeze-frame query (from stg_statsbomb__360 or the equivalent staging view)

Then: join freeze frames to actions by match + action timestamp, group by (canonical_player_id, match_id), produce the combined training DataFrame.

**Key difference from f2v_v2**: output must include a `freeze_frames` column containing per-action player position arrays (array of struct: `action_idx int, players array of [x float, y float, is_keeper bool, is_teammate bool]`). This mirrors the export module's output schema.

- [ ] **Step 2: Update `scripts/train_football2vec_360.py` to use SQL fetch**

Same pattern as Task 2: replace `load_training_data(hf_token, INPUT_DATASET)` with SQL-based fetch. Add env-var reads for Databricks credentials.

- [ ] **Step 3: Run ruff + pyright + existing tests**

---

## Task 4: gamma trainer rewrite — scoutgpt

**Spec:** §2.1, §4 Step 10. Most complex gamma rewrite — requires possession segmentation in pandas (Spark window functions reimplemented).

**Files:**
- Modify: `src/analytics/scoutgpt_training.py` (add `load_training_data_sql`)
- Modify: `scripts/train_scoutgpt_hf.py` (switch data source)

The ScoutGPT export module (`export_scoutgpt_training_data.py`) produces possession episode rows with columns: `episode_id, match_id, competition_id, season_id, team_id, data_source, actions` (array of struct with 9 fields including `time_delta` and `player_idx`).

The gamma rewrite:
1. SQL fetch from `fct_action_values JOIN dim_players` (same base query as f2v_v2 but with additional columns: `team_id, end_x, end_y, vaep_value, data_source`)
2. Possession segmentation in pandas:
   - Sort by (match_id, period, time_seconds)
   - Mark boundaries: team_id change, period change, set-piece restart, time_gap > 10s
   - Assign episode_id via cumulative sum
   - Filter episodes with < 3 actions
3. Build player_id_map (canonical_player_id -> contiguous int)
4. Normalize coordinates, map action types, compute time_delta
5. Group by episode, collect sorted action struct arrays

- [ ] **Step 1: Add `load_training_data_sql` to `scoutgpt_training.py`**

Function signature: `load_training_data_sql(host, token, warehouse_id) -> tuple[pd.DataFrame, dict[str, int], str]` — matches `load_training_data()` return type.

SQL query (mirrors `export_scoutgpt_training_data.py` lines 213-238):

```sql
SELECT
    CAST(dp.canonical_player_id AS STRING) AS canonical_player_id,
    CAST(av.match_id AS STRING) AS match_id,
    CAST(av.team_id AS INT) AS team_id,
    CAST(av.competition_id AS INT) AS competition_id,
    CAST(av.season_id AS INT) AS season_id,
    av.data_source,
    av.action_type,
    av.action_result,
    av.period,
    av.time_seconds,
    av.start_x,
    av.start_y,
    av.end_x,
    av.end_y,
    av.vaep_value
FROM soccer_analytics.dev_gold.fct_action_values av
INNER JOIN soccer_analytics.dev_gold.dim_players dp
    ON av.player_key = dp.player_key
WHERE av.player_id IS NOT NULL
  AND dp.canonical_player_id IS NOT NULL
  AND av.action_type IS NOT NULL
  AND av.start_x IS NOT NULL
  AND av.start_y IS NOT NULL
```

Possession segmentation in pandas (reimplements Spark window functions from `export_scoutgpt_training_data.py` lines 267-329):

```python
_SET_PIECE_TYPES = frozenset({"goalkick", "throw_in", "freekick_short", ...})
_TIME_GAP_THRESHOLD = 10.0
_MIN_EPISODE_LENGTH = 3

def _segment_possessions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["match_id", "period", "time_seconds"]).reset_index(drop=True)
    # Compute lagged values
    df["prev_team_id"] = df.groupby("match_id")["team_id"].shift(1)
    df["prev_period"] = df.groupby("match_id")["period"].shift(1)
    df["prev_time"] = df.groupby("match_id")["time_seconds"].shift(1)
    # Mark boundaries
    df["is_boundary"] = (
        df["prev_team_id"].isna()
        | (df["team_id"] != df["prev_team_id"])
        | (df["period"] != df["prev_period"])
        | df["action_type"].isin(_SET_PIECE_TYPES)
        | ((df["period"] == df["prev_period"]) & ((df["time_seconds"] - df["prev_time"]) > _TIME_GAP_THRESHOLD))
    ).astype(int)
    df["episode_seq"] = df.groupby("match_id")["is_boundary"].cumsum()
    df["episode_id"] = df["match_id"] + "_" + df["period"].astype(str) + "_" + df["episode_seq"].astype(str)
    # Filter short episodes
    ep_counts = df.groupby("episode_id").size()
    valid = ep_counts[ep_counts >= _MIN_EPISODE_LENGTH].index
    df = df[df["episode_id"].isin(valid)]
    # Compute time_delta
    df["time_delta"] = df["time_seconds"] - df["prev_time"]
    df.loc[df["is_boundary"] == 1, "time_delta"] = 0.0
    df["time_delta"] = df["time_delta"].fillna(0.0).astype(float)
    return df
```

- [ ] **Step 2: Update `scripts/train_scoutgpt_hf.py` to use SQL fetch**

In `_run_training_core()`, replace:
```python
data, _player_id_map, dataset_commit = load_training_data(hf_token, TRAINING_DATASET, revision=dataset_revision)
```
with conditional logic:
```python
db_host = os.environ.get("DATABRICKS_HOST", "")
if db_host:
    data, _player_id_map, dataset_commit = load_training_data_sql(
        db_host.replace("https://", "").replace("http://", "").rstrip("/"),
        os.environ["DATABRICKS_TOKEN"],
        os.environ["DATABRICKS_SQL_WAREHOUSE_ID"],
    )
else:
    data, _player_id_map, dataset_commit = load_training_data(hf_token, TRAINING_DATASET, revision=dataset_revision)
```

This preserves the HF dataset fallback for local-mode / evolve runs while defaulting to SQL when Databricks creds are available.

- [ ] **Step 3: Run ruff + pyright + existing tests**

---

## Task 5: Wire wf-scoutgpt-export into hf_sync sub-operations

**Spec:** §2.4a.1, §4 Step 14a.

**Files:**
- Modify: `src/ingestion/hf_sync.py` (add scoutgpt export to `_SUB_OPERATIONS`)
- Modify: `workflow-cards/wf-hf-sync.yaml` (add `wf-scoutgpt-export`)
- Modify: `workflow-cards/wf-scoutgpt-export.yaml` (change trigger)

- [ ] **Step 1: Add scoutgpt export to `_SUB_OPERATIONS` in `hf_sync.py`**

`export_scoutgpt_training_data.run_pipeline` takes 4 positional args: `(spark, catalog, schema, logger)` — no `skip_guard`, no `filter_result` (M1 review: confirmed from source at line 590). Create a `_make_scoutgpt_export_op()` factory similar to `_make_export_shots_op()`:

```python
def _make_scoutgpt_export_op() -> Callable[..., None]:
    """Create the export_scoutgpt_training_data sub-operation."""
    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        mod = importlib.import_module("ingestion.export_scoutgpt_training_data")
        mod.run_pipeline(spark, catalog, schema, logger_arg)
    return _call
```

Append to `_SUB_OPERATIONS`:
```python
("ingestion.export_scoutgpt_training_data", _make_scoutgpt_export_op()),
```

- [ ] **Step 2: Update wf-hf-sync.yaml**

Add `- wf-scoutgpt-export` to `sub_operations` list.

- [ ] **Step 3: Update wf-scoutgpt-export.yaml**

Change `trigger: manual` to `trigger: orchestrated` and add `orchestrated_by: wf-hf-sync`.

- [ ] **Step 4: Run ruff + pyright on hf_sync.py**

---

## Task 6: Port 3 Group 0 publishers to Databricks-runtime modules

**Spec:** §2.4a.2, §4 Step 14b. Create `src/ingestion/publish_{spadl_vaep,xg_shots,freeze_frame}_hf.py` modules that run inside the Databricks workflow (using `spark.sql()`) and upload to HF Hub. Wire into hf_sync sub_operations.

**Files:**
- Create: `src/ingestion/publish_spadl_vaep_hf.py`
- Create: `src/ingestion/publish_xg_shots_hf.py`
- Create: `src/ingestion/publish_freeze_frame_hf.py`
- Create: `workflow-cards/wf-publish-spadl-vaep.yaml`
- Create: `workflow-cards/wf-publish-xg-shots.yaml`
- Create: `workflow-cards/wf-publish-freeze-frames.yaml`
- Modify: `src/ingestion/hf_sync.py` (add 3 new sub-operations)
- Modify: `workflow-cards/wf-hf-sync.yaml` (add 3 entries)
- Modify: `pyproject.toml` (add 3 entry points)

Each Databricks-runtime publisher:
1. Reads from gold mart via `spark.sql(...)` or `spark.table(...)`
2. Converts to pandas (bounded — these tables are manageable size)
3. Writes to HF Hub via `huggingface_hub.HfApi`
4. Uploads README via `upload_hf_readme()`

The implementation mirrors the PEP 723 publishers' SQL and HF upload logic, replacing `query_databricks_sql()` with `spark.sql()`.

- [ ] **Step 1: Create `src/ingestion/publish_spadl_vaep_hf.py`**

Follow the `ingestion.export_embeddings_training_data` pattern for module structure: `skip_guard`, `run_pipeline(spark, catalog, schema, logger, *, filter_result)`, `main()`. The SQL matches `scripts/publish_spadl_vaep_hf.py`'s query. Upload to `luxury-lakehouse/spadl-vaep-action-values`.

- [ ] **Step 2: Create `src/ingestion/publish_xg_shots_hf.py`**

Same pattern. SQL matches `scripts/publish_xg_shots_hf.py`. Upload to `luxury-lakehouse/xg-shots`.

- [ ] **Step 3: Create `src/ingestion/publish_freeze_frame_hf.py`**

Same pattern. SQL matches `scripts/publish_freeze_frame_hf.py`. Upload to `luxury-lakehouse/xg-freeze-frame-data`.

- [ ] **Step 4: Create 3 workflow cards**

Each: `runtime: databricks-workflow`, `trigger: orchestrated`, `orchestrated_by: wf-hf-sync`. Follow existing pattern from `wf-football2vec-v2-export.yaml`.

- [ ] **Step 5: Add 3 entry points to `pyproject.toml`**

```toml
publish_spadl_vaep_hf = "ingestion.publish_spadl_vaep_hf:main"
publish_xg_shots_hf = "ingestion.publish_xg_shots_hf:main"
publish_freeze_frame_hf = "ingestion.publish_freeze_frame_hf:main"
```

- [ ] **Step 6: Wire 3 new sub-operations into `hf_sync.py`**

Append to `_SUB_OPERATIONS`:
```python
("ingestion.publish_spadl_vaep_hf", _make_logger_op("ingestion.publish_spadl_vaep_hf")),
("ingestion.publish_xg_shots_hf", _make_logger_op("ingestion.publish_xg_shots_hf")),
("ingestion.publish_freeze_frame_hf", _make_logger_op("ingestion.publish_freeze_frame_hf")),
```

(Or appropriate factory depending on each module's `run_pipeline` signature.)

- [ ] **Step 7: Update wf-hf-sync.yaml**

Add 3 entries to `sub_operations`.

- [ ] **Step 8: Run ruff + pyright on all new/modified files**

---

## Task 7: Evolve pin-drift constants

**Spec:** §2.12.1, §4 Step 15. Add `PINNED_DATASET_REPO`, `PINNED_DATASET_SHA`, `PINNED_REASON` constants to 3 evolve consumer scripts + pass `revision=PINNED_DATASET_SHA` to dataset load calls.

**Scope note (C1 review):** `src/evolve/backends/remote_ssh.py` is a **dispatcher**, not a dataset consumer — it passes `dataset_repo` through config but never downloads data itself. Removed from scope. Only actual dataset consumers get pins.

**Files:**
- Modify: `scripts/evaluate_football2vec_l2_adversary_seeds.py`
- Modify: `scripts/evaluate_scoutgpt_l2_seeds.py`
- Modify: `src/evolve/targets/football2vec/evaluator.py`

- [ ] **Step 1: Add constants to each file**

For each of the 3 files, add module-level constants:

```python
PINNED_DATASET_REPO: str = "luxury-lakehouse/<dataset-name>"
PINNED_DATASET_SHA: str = "PLACEHOLDER_UNTIL_PHASE_9"
PINNED_REASON: str = "Post-SK3-MIG-B Phase 9; bump via scripts/bump_evolve_pin.py"
```

The SHA is a placeholder until Phase 9 retrains complete and the operator bumps it.

- [ ] **Step 2: Add `revision=` kwarg to dataset load calls**

**`scripts/evaluate_football2vec_l2_adversary_seeds.py`** (line 76 `TRAINING_DATASET`): This script already has a `--dataset-sha` CLI arg (line 459). **Resolution (H1 review):** default the CLI arg to `PINNED_DATASET_SHA` so operator-driven bumps propagate automatically, but `--dataset-sha` can still override for one-off experiments:
```python
parser.add_argument("--dataset-sha", default=PINNED_DATASET_SHA,
                    help="HF dataset revision (default: PINNED_DATASET_SHA constant)")
```
Pass the resolved value through to `load_training_data(..., revision=args.dataset_sha)`. This requires adding `revision` to `football2vec_v2_training.load_training_data()` — add it as an optional `revision: str | None = None` parameter and forward to `datasets.load_dataset(..., revision=revision)`.

**`src/evolve/targets/football2vec/evaluator.py`** (C2 review — lines 59 and 540, NOT line 373): line 373 is `hf_hub_download` for **model weights**, not dataset. The actual dataset consumption is via `load_training_data()` calls at lines ~59 and ~540 (where `dataset_repo` / `self._dataset_repo` is used). Wire `revision=PINNED_DATASET_SHA` into those `load_training_data(...)` calls (same `revision` param added to `football2vec_v2_training.load_training_data` in the step above).

**`scripts/evaluate_scoutgpt_l2_seeds.py`** (C3 review — no direct load site): This script dispatches eval configs but does not itself call `load_training_data`. **Resolution:** sentinel-only — add `PINNED_DATASET_*` constants + propagate `PINNED_DATASET_SHA` as `"dataset_revision"` in the config dict passed to `_SHARED_CONFIG`, so downstream evaluator workers receive it. If the evaluator ignores it today, the constant still prevents untracked drift.

- [ ] **Step 3: Add `revision` parameter to `football2vec_v2_training.load_training_data`**

Modify `src/ingestion/football2vec_v2_training.py:load_training_data` to accept `revision: str | None = None` and forward to `datasets.load_dataset(training_dataset, split="train", revision=revision)`. No other signature changes. Existing callers that don't pass `revision` get `None` (HEAD) — backward-compatible.

- [ ] **Step 4: Run ruff + pyright**

---

## Task 8: Evolve pin-drift sentinel test

**Spec:** §2.12.2, §4 Step 16.

**Files:**
- Create: `src/tests/test_evolve_pin_drift.py`

- [ ] **Step 1: Write the sentinel test**

```python
# src/tests/test_evolve_pin_drift.py
"""Evolve dataset SHA pin-drift sentinel (env-gated).

Asserts pinned SHAs in evolve consumer scripts are within _MAX_AGE_DAYS
of HF Hub HEAD. Env-gated: skips without HF_TOKEN.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"

EVOLVE_SCRIPTS: list[tuple[str, Path]] = [
    ("eval_f2v_l2", _REPO_ROOT / "scripts" / "evaluate_football2vec_l2_adversary_seeds.py"),
    ("eval_scoutgpt_l2", _REPO_ROOT / "scripts" / "evaluate_scoutgpt_l2_seeds.py"),
    ("f2v_evaluator", _REPO_ROOT / "src" / "evolve" / "targets" / "football2vec" / "evaluator.py"),
    # remote_ssh.py EXCLUDED — dispatcher, not a dataset consumer (C1 review)
]

_MAX_AGE_DAYS = 90


def _load_module(name: str, path: Path) -> ModuleType:
    cached = sys.modules.get(name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    scripts_str = str(_SCRIPTS_DIR)
    added = scripts_str not in sys.path
    if added:
        sys.path.insert(0, scripts_str)
    try:
        spec.loader.exec_module(mod)
    finally:
        if added:
            sys.path.remove(scripts_str)
    return mod


@pytest.mark.skipif(not os.environ.get("HF_TOKEN"), reason="HF Hub access required")
@pytest.mark.parametrize("name,path", EVOLVE_SCRIPTS, ids=[s[0] for s in EVOLVE_SCRIPTS])
def test_pinned_sha_within_max_age(name: str, path: Path) -> None:
    mod = _load_module(f"_evolve_pin_{name}", path)
    repo = getattr(mod, "PINNED_DATASET_REPO", None)
    sha = getattr(mod, "PINNED_DATASET_SHA", None)
    assert repo is not None, f"{path.name} missing PINNED_DATASET_REPO"
    assert sha is not None, f"{path.name} missing PINNED_DATASET_SHA"
    if sha == "PLACEHOLDER_UNTIL_PHASE_9":
        pytest.skip("SHA not yet set — awaiting Phase 9 operator bump")
    from huggingface_hub import HfApi
    api = HfApi()
    info = api.dataset_info(repo_id=repo)
    if sha != info.sha:
        age = (datetime.now(timezone.utc) - info.last_modified).days
        assert age < _MAX_AGE_DAYS, (
            f"{path.name} pinned SHA is {age}d behind HEAD ({repo}). "
            f"Bump via: uv run python scripts/bump_evolve_pin.py {path}"
        )
```

- [ ] **Step 2: Run test**

Run: `uv run pytest src/tests/test_evolve_pin_drift.py -v`
Expected: PASS (skips without HF_TOKEN; or skips on PLACEHOLDER SHA)

---

## Task 9: `bump_evolve_pin.py` operator helper

**Spec:** §2.12.3, §4 Step 17.

**Files:**
- Create: `scripts/bump_evolve_pin.py`

- [ ] **Step 1: Write the helper script**

~50 LOC CLI that:
1. Accepts a script path as positional arg
2. Requires `--confirm-not-mid-experiment` flag
3. Requires `--reason` string
4. Fetches current HF Hub HEAD SHA for the script's `PINNED_DATASET_REPO`
5. Rewrites the script file: updates `PINNED_DATASET_SHA` and `PINNED_REASON` in-place via regex
6. Prints the change for operator review

```python
#!/usr/bin/env python3
"""Bump PINNED_DATASET_SHA in an evolve consumer script to HF Hub HEAD.

Operator-driven: requires --confirm-not-mid-experiment to prevent silent
data-shift during active architecture comparisons.

Usage:
    uv run python scripts/bump_evolve_pin.py scripts/evaluate_scoutgpt_l2_seeds.py \
        --confirm-not-mid-experiment \
        --reason "starting new architecture cycle XYZ"
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType


def _load(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("_bump_target", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    scripts_dir = str(path.parent)
    added = scripts_dir not in sys.path
    if added:
        sys.path.insert(0, scripts_dir)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    finally:
        if added:
            sys.path.remove(scripts_dir)
    return mod


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("script", type=Path, help="Path to evolve consumer script")
    parser.add_argument("--confirm-not-mid-experiment", action="store_true", required=True)
    parser.add_argument("--reason", type=str, required=True, help="Why the pin is being bumped")
    args = parser.parse_args()

    if not args.script.exists():
        raise FileNotFoundError(args.script)

    mod = _load(args.script)
    repo = getattr(mod, "PINNED_DATASET_REPO", None)
    if repo is None:
        raise ValueError(f"{args.script} has no PINNED_DATASET_REPO constant")

    from huggingface_hub import HfApi
    api = HfApi()
    info = api.dataset_info(repo_id=repo)
    new_sha = info.sha
    print(f"Repo: {repo}")
    print(f"HEAD SHA: {new_sha}")

    content = args.script.read_text(encoding="utf-8")
    content = re.sub(
        r'PINNED_DATASET_SHA:\s*str\s*=\s*"[^"]*"',
        f'PINNED_DATASET_SHA: str = "{new_sha}"',
        content,
    )
    content = re.sub(
        r'PINNED_REASON:\s*str\s*=\s*"[^"]*"',
        f'PINNED_REASON: str = "{args.reason}"',
        content,
    )
    args.script.write_text(content, encoding="utf-8")
    print(f"Updated {args.script}")


if __name__ == "__main__":
    main()
```

---

## Task 10: Input-dataset upstream sentinel

**Spec:** §2.10.6, §4 Step 19 (partial).

**Files:**
- Create: `src/tests/test_orchestrator_input_dataset_upstream.py`

- [ ] **Step 1: Write the sentinel test**

Static map `TRAINER_INPUT_DATASETS` curated from the spec (§2.10.6). For each trainer:
- If producer is `group_0`, asserts the trainer references the dataset string
- If producer is `gold_sql`, asserts the trainer source references `fct_action_values` and uses `query_databricks_sql` or `load_training_data_sql`

This test catches a future trainer silently regressing back to HF dataset consumption.

- [ ] **Step 2: Run test**

Run: `uv run pytest src/tests/test_orchestrator_input_dataset_upstream.py -v`

---

## Task 11: Final sweep — ruff, pyright, pytest, wheel bump

**Spec:** §4 Step 19.

- [ ] **Step 1: Bump wheel version**

Run: `uv run python scripts/bump_wheel.py`
(NEVER edit pyproject version manually — per CLAUDE.md rule)

- [ ] **Step 2: Run full lint + type check**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest src/tests/ -v --timeout=120`
Expected: all existing + new tests PASS

- [ ] **Step 4: Verify no pyproject.toml dep changes require `uv lock`**

PEP 723 dep changes (e.g. removing `datasets` from trainer scripts) do NOT affect `uv.lock` — that file tracks `pyproject.toml` deps only (M2 review). Only run `uv lock` if `pyproject.toml` `[project.dependencies]` or `[project.optional-dependencies]` changed. In this PR the only `pyproject.toml` change is `[project.scripts]` entry points, which do not affect the lock file.

- [ ] **Step 5: Commit (requires explicit user approval)**

Single squash commit per CLAUDE.md convention. Commit message:

```
feat(gamma): rewrite f2v_v2/f2v_360/scoutgpt trainers to read from gold marts

- gamma trainer rewrite: 3 trainers now fetch training data via
  Databricks SQL Statement Execution API instead of stale HF datasets
- wf-hf-sync amendment: scoutgpt export + 3 Group 0 publishers wired
  as daily sub-operations for external consumer freshness
- evolve pin-drift discipline: PINNED_DATASET_SHA constants + sentinel
  test + bump_evolve_pin.py operator helper
- §2.10.6 input-dataset upstream sentinel

Spec: docs/superpowers/specs/2026-05-04-sk3-mig-b-orchestrator-data-source-and-flavor-alignment-design.md §2.1, §2.4a, §2.10.6, §2.12
```

---

## Execution Notes

**Critical path:** Tasks 2-4 (gamma rewrites) are the long pole (~750 LOC). Tasks 5-6 (hf_sync) are mechanical. Tasks 7-9 (evolve) are independent. Task 10 (sentinel) depends on Tasks 2-4.

**Parallelizable:** Tasks 7-9 (evolve pin-drift) are fully independent of Tasks 2-6 (gamma + hf_sync) and can be done in parallel.

**Wheel rebuild:** Required for PR-2 because `_SUB_OPERATIONS` lives in `src/ingestion/hf_sync.py` which IS in the wheel. Also `src/ingestion/databricks_sql_fetch.py` (new), `src/ingestion/publish_*_hf.py` (new), and `src/analytics/scoutgpt_training.py` changes.

**Phase 9 second retry:** After PR-2 merges + post-merge CI clears, operator triggers Phase 9 retry #2. This validates gamma trainer parity end-to-end. See spec §2.0.
