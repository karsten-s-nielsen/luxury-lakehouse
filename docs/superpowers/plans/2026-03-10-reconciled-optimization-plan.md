# Reconciled Optimization Plan — Implementation

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 42 optimization audit findings, migrate 6 compute pipelines to `applyInPandas`, and deploy everything end-to-end with verified databases, indexes, and Streamlit app.

**Architecture:** Layered execution — Critical bugfixes → Quick wins → Pipeline migrations (batched pitch control → off-ball xT → DEFCON → line-breaking → SPADL/VAEP → embeddings) → Streamlit/dbt/Terraform sweep → Profiling posture. Each pipeline migration replaces driver-bound `toPandas()` loops with executor-distributed `applyInPandas` on Databricks Serverless (16 GB driver, 1 GB per-UDF limit).

**Tech Stack:** PySpark `applyInPandas`, NumPy, XGBoost, gensim Doc2Vec, scipy Ward clustering, Streamlit `@st.cache_data`, dbt incremental models, Terraform `aws_budgets_budget`, pytest-benchmark.

**Spec:** `docs/superpowers/specs/2026-03-10-reconciled-optimization-plan-design.md`

**Key constraints (read CLAUDE.md):**
- Databricks Serverless: 16 GB driver (fixed), 1 GB per-UDF executor, no broadcast vars, no cache/persist, no internet in UDFs
- All code must pass: `uv run ruff check src/` + `uv run ruff format --check src/` + `uv run pyright src/` + `uv run pytest src/tests/ -v`
- Python 3.10 locked, 120-char line length

---

## Chunk 1: Phase 0 — Critical Bugfixes

### Task 1: Fix embeddings incremental skip bug (P9-01)

**Files:**
- Modify: `src/ingestion/player_embeddings.py:356-392`
- Test: `src/tests/test_player_embeddings.py`

The skip guard at line 377-384 queries `fct_match_summary` with `WHERE data_source IN ('statsbomb', 'wyscout')`. This returns 0 rows (data_source values don't match), so `source_matches` is empty, the guard at line 387 (`if source_matches and not new_matches: return`) never fires, and the pipeline always runs fully — causing OOM.

- [ ] **Step 1: Write failing test for skip guard**

Add test to `TestMainFunction` class in `test_player_embeddings.py`. The test should verify that when existing embeddings cover all source matches, the pipeline returns early without calling `_load_events`.

```python
def test_skip_guard_when_all_matches_processed(self, mock_spark):
    """P9-01: Skip guard must fire when all matches already have embeddings."""
    # Setup: results table has match_ids {1, 2, 3}
    # Setup: source events table also has match_ids {1, 2, 3}
    # Expect: _load_events is NOT called
    # Assert: logger.info contains "skip" or "already processed"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_player_embeddings.py::TestMainFunction::test_skip_guard_when_all_matches_processed -v`
Expected: FAIL

- [ ] **Step 3: Fix the skip guard**

In `src/ingestion/player_embeddings.py`, replace the buggy `source_matches` query (lines 377-384) with a query that reliably gets all match_ids that should have embeddings. The fix has two parts:

1. Query source match_ids from the actual events tables (bronze) instead of `fct_match_summary`, OR fix the `data_source` filter to match actual values in `fct_match_summary`
2. Add a defensive fallback: if the results table has data but `source_matches` is empty, log a warning and return early (prevents silent OOM)

```python
# Part 1: Get all match_ids that have events (the actual source of truth)
source_query = f"""
    SELECT DISTINCT match_id FROM {catalog}.{schema}.fct_action_values
"""
source_rows = spark.sql(source_query).collect()
source_matches = {str(row["match_id"]) for row in source_rows}

# Part 2: Defensive fallback
if not source_matches and existing_matches:
    logger.info("No source matches found but embeddings exist — skipping recompute")
    return

new_matches = source_matches - existing_matches
if not new_matches:
    logger.info("All %d matches already have embeddings — skipping", len(existing_matches))
    return
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_player_embeddings.py::TestMainFunction -v`
Expected: ALL PASS

- [ ] **Step 5: Run full test suite + linting**

Run: `uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py && uv run pytest src/tests/test_player_embeddings.py -v`
Expected: 0 errors

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/player_embeddings.py src/tests/test_player_embeddings.py
git commit -m "fix(embeddings): fix incremental skip guard — query source matches from fct_action_values (P9-01)"
```

---

### Task 2: Add bounded toPandas guard to embeddings (P0-01)

**Files:**
- Modify: `src/ingestion/player_embeddings.py:107-195` (`_load_events`)
- Test: `src/tests/test_player_embeddings.py`

`_load_events()` at line 188 calls `.toPandas()` on a full StatsBomb+Wyscout event join (~10M rows). Even with the skip bug fixed (Task 1), if new matches exist the query pulls everything. Add a filter for only the new match_ids.

- [ ] **Step 1: Write failing test**

Test that `_load_events` accepts an optional `match_ids` parameter and only returns events for those matches.

```python
def test_load_events_filters_by_match_ids(self, mock_spark):
    """P0-01: _load_events must filter to specific match_ids when provided."""
    # Call _load_events(spark, catalog, schema, match_ids={"1", "2"})
    # Verify the SQL includes a WHERE match_id IN clause
```

- [ ] **Step 2: Run test — expect FAIL**

- [ ] **Step 3: Add `match_ids` parameter to `_load_events`**

Modify `_load_events` signature at line 107:
```python
def _load_events(spark: SparkSession, catalog: str, schema: str, match_ids: set[str] | None = None) -> pd.DataFrame:
```

Before the `.toPandas()` at line 188, add a filter:
```python
events_sdf = spark.sql(query)
if match_ids:
    events_sdf = events_sdf.filter(F.col("match_id").isin(list(match_ids)))
events_pdf = events_sdf.toPandas()
```

Update the call site in `main()` (around line 402) to pass `new_matches` from Task 1's skip guard.

- [ ] **Step 4: Run test — expect PASS**

- [ ] **Step 5: Quality checks**

Run: `uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py && uv run pytest src/tests/test_player_embeddings.py -v`

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/player_embeddings.py src/tests/test_player_embeddings.py
git commit -m "fix(embeddings): filter _load_events by match_ids to prevent unbounded toPandas (P0-01)"
```

---

### Task 3: Fix Streamlit cache bypass (CACHE-01)

**Files:**
- Modify: `src/streamlit_app/components/filters.py:14-21`
- Test: `src/tests/test_streamlit_components.py`

`_cached_query` defines an inner function `_run` decorated with `@st.cache_data` on every call. The decorator re-applies each time, creating a new cache key — so the cache never hits.

- [ ] **Step 1: Write failing test**

```python
def test_cached_query_cache_key_stability(self):
    """CACHE-01: _cached_query must return the same cached function object on repeated calls."""
    # Call _cached_query("SELECT 1") twice
    # The second call should hit cache (not re-execute)
```

- [ ] **Step 2: Run test — expect FAIL**

- [ ] **Step 3: Move inner function to module level**

In `filters.py`, replace lines 14-21:

```python
# Before (broken):
def _cached_query(query: str, params: tuple | None = None) -> pd.DataFrame:
    @st.cache_data(ttl=600)
    def _run(q: str, p: tuple | None = None) -> pd.DataFrame:
        return execute_query(q, p)
    return _run(query, params)

# After (fixed):
@st.cache_data(ttl=600)
def _cached_query(query: str, params: tuple | None = None) -> pd.DataFrame:
    return execute_query(query, params)
```

- [ ] **Step 4: Run test — expect PASS**

- [ ] **Step 5: Quality checks**

Run: `uv run ruff check src/streamlit_app/components/filters.py && uv run pyright src/streamlit_app/ && uv run pytest src/tests/test_streamlit_components.py -v`

- [ ] **Step 6: Commit**

```bash
git add src/streamlit_app/components/filters.py src/tests/test_streamlit_components.py
git commit -m "fix(streamlit): move _cached_query to module level — fix cache key instability (CACHE-01)"
```

---

## Chunk 2: Phase 1 — Quick Wins

### Task 4: Pass row_count to 5 write_delta_table call sites (DB-01)

**Files:**
- Modify: `src/ingestion/wyscout.py:213,256,417`
- Modify: `src/ingestion/entity_resolution.py:116`
- Modify: `src/ingestion/player_embeddings.py:466`

Each of these calls `validate_dataframe()` (which returns a row count) then calls `write_delta_table()` without passing the count — triggering a redundant `df.count()` DAG recomputation.

- [ ] **Step 1: Fix all 5 call sites**

Pattern at each site — find the `validate_dataframe` call, capture its return value, pass to `write_delta_table`:

```python
# Before:
validate_dataframe(df, required_columns, source_name, logger)
write_delta_table(df, catalog, schema, table_name, ...)

# After:
row_count = validate_dataframe(df, required_columns, source_name, logger)
write_delta_table(df, catalog, schema, table_name, ..., row_count=row_count)
```

Apply to all 5 sites in `wyscout.py` (3 sites), `entity_resolution.py` (1 site), `player_embeddings.py` (1 site).

- [ ] **Step 2: Quality checks**

Run: `uv run ruff check src/ingestion/wyscout.py src/ingestion/entity_resolution.py src/ingestion/player_embeddings.py && uv run pyright src/ingestion/`

- [ ] **Step 3: Commit**

```bash
git add src/ingestion/wyscout.py src/ingestion/entity_resolution.py src/ingestion/player_embeddings.py
git commit -m "perf: pass row_count to write_delta_table at 5 call sites — eliminate redundant df.count() (DB-01)"
```

---

### Task 5: Project columns in backfill SELECT (P0-07)

**Files:**
- Modify: `src/ingestion/statsbomb.py:454`

- [ ] **Step 1: Change SELECT * to SELECT specific columns**

At line 454, change:
```python
# Before:
f"SELECT * FROM {catalog}.{schema}.statsbomb_events WHERE match_id = {match_id}"

# After:
f"SELECT id, _raw_extra_json FROM {catalog}.{schema}.statsbomb_events WHERE match_id = {match_id}"
```

- [ ] **Step 2: Quality checks + commit**

```bash
uv run ruff check src/ingestion/statsbomb.py && uv run pyright src/ingestion/statsbomb.py
git add src/ingestion/statsbomb.py
git commit -m "perf(statsbomb): SELECT specific columns in backfill_extra_json instead of SELECT * (P0-07)"
```

---

### Task 6: Remove standalone df.count() in SPADL training (P0-06)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:636`

- [ ] **Step 1: Remove or replace the standalone count**

At line 636, `spark.table(spadl_table).count()` is called as a sanity check before the training pull. This triggers a full DAG recomputation. Either remove it or replace with a cached count from the prior write.

- [ ] **Step 2: Quality checks + commit**

```bash
uv run ruff check src/ingestion/spadl_vaep.py && uv run pyright src/ingestion/spadl_vaep.py
git add src/ingestion/spadl_vaep.py
git commit -m "perf(spadl): remove standalone df.count() before training pull — avoid DAG recomputation (P0-06)"
```

---

### Task 7: Pre-compile regex in Metrica ingestion (P0-05)

**Files:**
- Modify: `src/ingestion/metrica.py:569`

- [ ] **Step 1: Move re.compile to module level**

Find the `re.sub()` or `re.compile()` call at/near line 569. Move the pattern to a module-level constant:

```python
# At module level (near top of file):
_COLUMN_CLEAN_RE = re.compile(r"<pattern>")

# In function body, replace:
#   re.sub(r"<pattern>", replacement, text)
# With:
#   _COLUMN_CLEAN_RE.sub(replacement, text)
```

- [ ] **Step 2: Quality checks + commit**

```bash
uv run ruff check src/ingestion/metrica.py && uv run pyright src/ingestion/metrica.py
git add src/ingestion/metrica.py
git commit -m "perf(metrica): pre-compile regex at module level (P0-05)"
```

---

### Task 8: Use requests.Session in fetch_url (NET-01)

**Files:**
- Modify: `src/ingestion/utils.py:295-340`
- Test: `src/tests/test_ingestion_utils.py`

- [ ] **Step 1: Write failing test**

Test that `fetch_url` reuses a session when called multiple times to the same host.

- [ ] **Step 2: Implement session reuse**

Add a module-level `_session: requests.Session | None = None` and a `_get_session()` helper that creates it lazily. Use `session.get()` instead of `requests.get()` inside `fetch_url`:

```python
_session: requests.Session | None = None

def _get_session() -> requests.Session:
    global _session  # noqa: PLW0603
    if _session is None:
        _session = requests.Session()
        _session.verify = True
    return _session

def fetch_url(url: str, timeout: tuple[int, int] = (10, 30), max_retries: int = 3) -> requests.Response:
    # ... existing HTTPS enforcement ...
    session = _get_session()
    # Replace requests.get() with session.get()
```

- [ ] **Step 3: Run tests + quality checks**

Run: `uv run pytest src/tests/test_ingestion_utils.py -v && uv run ruff check src/ingestion/utils.py && uv run pyright src/ingestion/utils.py`

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/utils.py src/tests/test_ingestion_utils.py
git commit -m "perf(utils): use requests.Session for TCP keep-alive in fetch_url (NET-01)"
```

---

### Task 9: Fix action_values.py cache bypass (CACHE-02)

**Files:**
- Modify: `src/streamlit_app/pages/action_values.py:49-91`

Same pattern as CACHE-01. Functions `_load_action_type_breakdown` and `_load_match_timeline` define inner functions with `@st.cache_data` inside their bodies.

- [ ] **Step 1: Refactor to module-level cached functions**

Move the inner `_query` functions from inside `_load_action_type_breakdown` (line 72) and `_load_match_timeline` (line 91) to module-level standalone functions with `@st.cache_data`.

- [ ] **Step 2: Quality checks + commit**

```bash
uv run ruff check src/streamlit_app/pages/action_values.py && uv run pyright src/streamlit_app/
git add src/streamlit_app/pages/action_values.py
git commit -m "fix(streamlit): move cached functions to module level in action_values.py (CACHE-02)"
```

---

### Task 10: Filter player stats toPandas (P0-09)

**Files:**
- Modify: `src/ingestion/player_embeddings.py:196-250` (`_compute_stat_vectors`)

- [ ] **Step 1: Add filter to stat vectors query**

At line 212-223, the SQL query pulls all player stats without a filter. Add a WHERE clause to only pull stats for players that have events (bounded subset). This is a temporary fix — Phase 2f will replace toPandas entirely.

```python
# Add parameter: match_ids or player_ids to filter
def _compute_stat_vectors(
    spark: SparkSession, catalog: str, gold_schema: str, player_ids: set[int] | None = None
) -> tuple[pd.DataFrame, dict[str, dict[str, float]]]:
```

- [ ] **Step 2: Quality checks + commit**

```bash
uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py
git add src/ingestion/player_embeddings.py
git commit -m "perf(embeddings): filter _compute_stat_vectors to relevant players (P0-09)"
```

---

### Task 11: Add incremental skip guard to line-breaking (missing)

**Files:**
- Modify: `src/ingestion/line_breaking.py:108-145` (`_process_statsbomb_360`)
- Modify: `src/ingestion/line_breaking.py:234-275` (`_process_metrica_tracking`)
- Test: `src/tests/test_line_breaking.py`

Line-breaking has NO incremental skip check. It uses `merge_delta_table` which is idempotent, but still runs all 323 matches every time (646 sequential Spark DAGs).

- [ ] **Step 1: Write failing test**

```python
def test_incremental_skip_guard(self):
    """Line-breaking must skip already-processed matches."""
    # Mock spark.table(results) to return match_ids {1, 2}
    # Mock source to return match_ids {1, 2, 3}
    # Assert only match_id 3 is processed
```

- [ ] **Step 2: Add skip guard to _process_statsbomb_360**

Follow the pattern from `off_ball_xt.py:108-126`:

```python
# After line ~128 (where match_ids are collected), before the per-match loop:
results_table = f"{catalog}.{schema}.line_breaking_results"
try:
    existing_rows = spark.table(results_table).select("match_id").distinct().collect()
    existing_ids = {str(row["match_id"]) for row in existing_rows}
except Exception:
    existing_ids = set()

new_match_ids = [mid for mid in all_match_ids if str(mid) not in existing_ids]
if not new_match_ids:
    logger.info("All %d matches already processed — skipping", len(all_match_ids))
    return 0
logger.info("Processing %d new matches (skipping %d existing)", len(new_match_ids), len(existing_ids))
```

- [ ] **Step 3: Add same guard to _process_metrica_tracking**

Same pattern, applied to the tracking path.

- [ ] **Step 4: Run tests + quality checks**

Run: `uv run pytest src/tests/test_line_breaking.py -v && uv run ruff check src/ingestion/line_breaking.py && uv run pyright src/ingestion/line_breaking.py`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/line_breaking.py src/tests/test_line_breaking.py
git commit -m "perf(line-breaking): add incremental skip guard — skip already-processed matches"
```

---

## Chunk 3: Phase 2a-2b — Batched Pitch Control + Off-Ball xT Migration

### Task 12: Create batched pitch control function (Phase 2a)

**Files:**
- Modify: `src/analytics/pitch_control.py:244+` (add new function after `compute_pitch_control_at_point`)
- Test: `src/tests/test_pitch_control_model.py`

`_compute_time_to_intercept` at line 69 already returns `(n_players, n_targets)` — it handles multiple targets. The batch function reuses this to process N target points with a single matrix setup.

- [ ] **Step 1: Write failing tests for batch function**

Add to `test_pitch_control_model.py`:

```python
class TestBatchPitchControl:
    """Tests for compute_pitch_control_at_points (batch version)."""

    def test_single_point_matches_scalar(self):
        """Batch with 1 target must match compute_pitch_control_at_point."""
        players_df = _make_test_frame()  # reuse existing fixture
        scalar = compute_pitch_control_at_point(players_df, 60.0, 40.0)
        batch = compute_pitch_control_at_points(players_df, np.array([[60.0, 40.0]]))
        np.testing.assert_allclose(batch, [scalar], atol=1e-10)

    def test_multiple_points_shape(self):
        """Batch with N targets returns (N,) array."""
        players_df = _make_test_frame()
        targets = np.array([[30.0, 20.0], [60.0, 40.0], [90.0, 60.0]])
        result = compute_pitch_control_at_points(players_df, targets)
        assert result.shape == (3,)

    def test_values_bounded_zero_one(self):
        """All pitch control values must be in [0, 1]."""
        players_df = _make_test_frame()
        targets = np.array([[x, y] for x in range(10, 111, 20) for y in range(10, 71, 20)])
        result = compute_pitch_control_at_points(players_df, targets)
        assert np.all(result >= 0.0)
        assert np.all(result <= 1.0)

    def test_empty_targets_returns_empty(self):
        """Empty target array returns empty result."""
        players_df = _make_test_frame()
        result = compute_pitch_control_at_points(players_df, np.empty((0, 2)))
        assert result.shape == (0,)
```

- [ ] **Step 2: Run tests — expect FAIL (function not defined)**

Run: `uv run pytest src/tests/test_pitch_control_model.py::TestBatchPitchControl -v`

- [ ] **Step 3: Implement `compute_pitch_control_at_points`**

Add after line 315 in `pitch_control.py`:

```python
def compute_pitch_control_at_points(
    players_df: pd.DataFrame,
    target_points: np.ndarray,
    params: PitchControlParams | None = None,
) -> np.ndarray:
    """Compute pitch control at multiple target points in a single pass.

    Args:
        players_df: Frame data with columns: player_id, team, x, y, vx, vy.
        target_points: (N, 2) array of (x, y) target positions in StatsBomb coordinates.
        params: Pitch control parameters.

    Returns:
        (N,) array of home-team pitch control values in [0, 1].
    """
    if params is None:
        params = PitchControlParams()
    if len(target_points) == 0:
        return np.empty(0)

    # Single home/away split (done ONCE, not per-target)
    home = players_df[players_df["team"] == "home"]
    away = players_df[players_df["team"] == "away"]

    if home.empty or away.empty:
        return np.full(len(target_points), 0.5)

    # Single coordinate conversion to meters (done ONCE)
    home_pos = np.column_stack([
        _sb_to_meters_x(_col_f64(home, "x"), params),
        _sb_to_meters_y(_col_f64(home, "y"), params),
    ])
    home_vel = np.column_stack([
        _col_f64(home, "vx") * params.pitch_length_m / params.sb_length,
        _col_f64(home, "vy") * params.pitch_width_m / params.sb_width,
    ])
    away_pos = np.column_stack([
        _sb_to_meters_x(_col_f64(away, "x"), params),
        _sb_to_meters_y(_col_f64(away, "y"), params),
    ])
    away_vel = np.column_stack([
        _col_f64(away, "vx") * params.pitch_length_m / params.sb_length,
        _col_f64(away, "vy") * params.pitch_width_m / params.sb_width,
    ])

    # Convert targets to meters (done ONCE)
    targets_m = np.column_stack([
        _sb_to_meters_x(target_points[:, 0], params),
        _sb_to_meters_y(target_points[:, 1], params),
    ])

    # Single TTI computation for all targets (vectorized)
    home_tti = _compute_time_to_intercept(home_pos, home_vel, targets_m, params)
    away_tti = _compute_time_to_intercept(away_pos, away_vel, targets_m, params)

    # Compute influence per target
    home_min_tti = np.min(home_tti, axis=0)  # (n_targets,)
    away_min_tti = np.min(away_tti, axis=0)  # (n_targets,)

    home_influence = _compute_team_influence(home_min_tti, away_min_tti, params)
    away_influence = _compute_team_influence(away_min_tti, home_min_tti, params)

    total = home_influence + away_influence
    safe_total = np.where(total > 0, total, 1.0)
    return home_influence / safe_total
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `uv run pytest src/tests/test_pitch_control_model.py -v`

- [ ] **Step 5: Quality checks**

Run: `uv run ruff check src/analytics/pitch_control.py && uv run pyright src/analytics/pitch_control.py`

- [ ] **Step 6: Commit**

```bash
git add src/analytics/pitch_control.py src/tests/test_pitch_control_model.py
git commit -m "feat(pitch-control): add compute_pitch_control_at_points batch function (Phase 2a)"
```

---

### Task 13: Rewrite off-ball xT frame computation to use batched PC (Phase 2b — algorithmic)

**Files:**
- Modify: `src/analytics/off_ball_xt.py:63-117` (`compute_off_ball_xt_frame`)
- Test: `src/tests/test_off_ball_xt.py`

Currently calls `compute_pitch_control_at_point()` 22 times per frame (once per player). Replace with single `compute_pitch_control_at_points()` call.

- [ ] **Step 1: Run existing tests to establish baseline**

Run: `uv run pytest src/tests/test_off_ball_xt.py -v`
Expected: ALL PASS (21 tests)

- [ ] **Step 2: Rewrite `compute_off_ball_xt_frame` to use batch**

Replace the per-player loop at lines 92-114 with:

```python
def compute_off_ball_xt_frame(
    players_df: pd.DataFrame,
    xt_grid: np.ndarray,
    pitch_control_params: PitchControlParams | None = None,
) -> pd.DataFrame:
    if players_df.empty:
        return pd.DataFrame(columns=["player_id", "team", "x", "y", "xt_value", "pitch_control", "off_ball_xt"])

    xs = _col_f64(players_df, "x")
    ys = _col_f64(players_df, "y")
    target_points = np.column_stack([xs, ys])  # (N, 2)

    # Single batched call — one matrix setup for all players
    pc_values = compute_pitch_control_at_points(players_df, target_points, pitch_control_params)

    # xT lookup per player (vectorized)
    xt_values = np.array([
        _lookup_xt(x, y, xt_grid) for x, y in zip(xs, ys)
    ])

    # Adjust PC for away team (pitch control is from home perspective)
    teams = players_df["team"].values
    adjusted_pc = np.where(teams == "home", pc_values, 1.0 - pc_values)

    return pd.DataFrame({
        "player_id": players_df["player_id"].values,
        "team": teams,
        "x": xs,
        "y": ys,
        "xt_value": xt_values,
        "pitch_control": adjusted_pc,
        "off_ball_xt": xt_values * adjusted_pc,
    })
```

- [ ] **Step 3: Run tests — all 21 must still pass**

Run: `uv run pytest src/tests/test_off_ball_xt.py -v`
Expected: ALL PASS

- [ ] **Step 4: Quality checks**

Run: `uv run ruff check src/analytics/off_ball_xt.py && uv run pyright src/analytics/off_ball_xt.py`

- [ ] **Step 5: Commit**

```bash
git add src/analytics/off_ball_xt.py
git commit -m "perf(off-ball-xt): use batched pitch control — one matrix setup per frame instead of 22 (Phase 2b)"
```

---

### Task 14: Migrate off-ball xT ingestion to applyInPandas (Phase 2b — structural)

**Files:**
- Modify: `src/ingestion/off_ball_xt.py:80-195` (`_process_matches`)
- Test: `src/tests/test_off_ball_xt.py`

Replace the per-match `toPandas()` loop with a two-pass `applyInPandas` pipeline. Resolves P0-08, P9-03, P0-14.

- [ ] **Step 1: Write the UDF function**

Add a new function `_compute_off_ball_xt_batch` that accepts a pandas DataFrame (one frame batch for one match) and returns per-player xT results:

```python
def _compute_off_ball_xt_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    """UDF for applyInPandas — process one frame batch."""
    from analytics.off_ball_xt import compute_off_ball_xt_frame, OffBallXtParams
    from analytics.pitch_control import PitchControlParams

    match_id = pdf["match_id"].iloc[0]
    xt_grid = _load_xt_grid()  # loads from UC Volume or fallback
    params = OffBallXtParams()
    pc_params = PitchControlParams()

    # ... sample frames, compute per-frame, aggregate per-player ...
    # Return: match_id, player_id, total_off_ball_xt, frame_count
```

- [ ] **Step 2: Replace `_process_matches` with distributed pipeline**

```python
def _process_matches(spark, catalog, schema, logger, xt_grid, params, pc_params) -> int:
    tracking_table = f"{catalog}.{schema}.fct_tracking_frames"
    results_table = f"{catalog}.{_GOLD_SCHEMA}.{_TABLE_NAME}"

    # Incremental skip (existing logic, keep as-is)
    # ...

    # Add synthetic partition key for frame batching
    batch_size = 270  # ~5 min at 1fps
    tracking_sdf = (
        spark.table(tracking_table)
        .filter(F.col("match_id").isin(new_match_ids))
        .withColumn(
            "frame_batch_id",
            (F.col("frame") / (F.lit(params.sample_fps) * batch_size)).cast("int"),
        )
    )

    # Pass 1: compute per-player xT per batch (distributed)
    batch_schema = "match_id string, player_id string, total_off_ball_xt double, frame_count long"
    batch_results = (
        tracking_sdf
        .groupBy("match_id", "frame_batch_id")
        .applyInPandas(_compute_off_ball_xt_batch, schema=batch_schema)
    )

    # Pass 2: aggregate across batches (Spark-native)
    final = (
        batch_results
        .groupBy("match_id", "player_id")
        .agg(
            F.sum("total_off_ball_xt").alias("total_off_ball_xt"),
            F.sum("frame_count").alias("frames_sampled"),
        )
        .withColumn("avg_off_ball_xt", F.col("total_off_ball_xt") / F.col("frames_sampled"))
    )

    # Write results
    row_count = final.count()
    write_delta_table(final, catalog, _GOLD_SCHEMA, _TABLE_NAME, row_count=row_count, ...)
    return row_count
```

- [ ] **Step 3: Run tests — expect PASS**

Run: `uv run pytest src/tests/test_off_ball_xt.py -v`

- [ ] **Step 4: Quality checks**

Run: `uv run ruff check src/ingestion/off_ball_xt.py && uv run pyright src/ingestion/off_ball_xt.py`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/off_ball_xt.py src/analytics/off_ball_xt.py src/tests/test_off_ball_xt.py
git commit -m "feat(off-ball-xt): migrate to applyInPandas with frame-batch grouping — ~100-200x speedup (Phase 2b)"
```

---

## Chunk 4: Phase 2c-2d — DEFCON Two-Pass + Line-Breaking Migration

### Task 15: Split DEFCON into credit assignment + value estimation (Phase 2c — analytics)

**Files:**
- Modify: `src/analytics/defcon_lite.py:317-422` (split `compute_defcon_match`)
- Test: `src/tests/test_defcon_lite.py`

Split `compute_defcon_match` into two functions: `assign_credits_for_period` (parallelizable by period) and `estimate_values_for_match` (needs full match credits).

- [ ] **Step 1: Write tests for split functions**

```python
class TestTwoPassDefcon:
    def test_credit_assignment_independent_per_period(self):
        """Credits from period 1 and period 2 must be identical whether computed together or separately."""
        # Full match credits
        full = compute_defcon_match(actions, ff, params)
        # Per-period credits
        p1_actions = actions[actions["period"] == 1]
        p1_ff = ff[ff["event_id"].isin(p1_actions["event_id"])]
        p1 = assign_credits_for_period(p1_actions, p1_ff, params)
        # Assert period-1 credits match
        assert_frame_equal(full[full["period"] == 1][credit_cols], p1[credit_cols])

    def test_value_estimation_uses_full_match(self):
        """XGBoost estimation uses credits from all periods."""
        credits = assign_credits_for_period(actions, ff, params)
        valued = estimate_values_for_match(credits, params)
        assert "defcon_value" in valued.columns
```

- [ ] **Step 2: Implement the split**

Extract the credit-assignment loop (lines 370-406) into `assign_credits_for_period()`. Extract the XGBoost value estimation (line 417: `estimate_defcon_values`) into `estimate_values_for_match()`. Keep `compute_defcon_match()` as a convenience wrapper that calls both.

- [ ] **Step 3: Run all DEFCON tests — must pass**

Run: `uv run pytest src/tests/test_defcon_lite.py -v`
Expected: ALL PASS (26 tests + new tests)

- [ ] **Step 4: Quality checks + commit**

```bash
uv run ruff check src/analytics/defcon_lite.py && uv run pyright src/analytics/defcon_lite.py
git add src/analytics/defcon_lite.py src/tests/test_defcon_lite.py
git commit -m "refactor(defcon): split compute_defcon_match into credit assignment + value estimation (Phase 2c)"
```

---

### Task 16: Migrate DEFCON ingestion to two-pass applyInPandas (Phase 2c — structural)

**Files:**
- Modify: `src/ingestion/defcon_lite.py:36-180` (`_process_360_matches`)
- Modify: `src/ingestion/defcon_lite.py:183-365` (`_process_tracking_matches`)

Replace per-match `toPandas()` loops with two-pass distributed execution:
- Pass 1: `groupBy("match_id", "period").applyInPandas(assign_credits_udf)` — credit assignment
- Pass 2: `groupBy("match_id").applyInPandas(estimate_values_udf)` — XGBoost value estimation

- [ ] **Step 1: Implement UDF wrappers for both passes**

```python
def _assign_credits_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """Pass 1 UDF: assign defensive credits for one (match_id, period) group."""
    from analytics.defcon_lite import assign_credits_for_period, DefconLiteParams
    params = DefconLiteParams()
    # Split pdf into actions and freeze_frames based on column presence
    # Call assign_credits_for_period
    # Return credits DataFrame

def _estimate_values_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """Pass 2 UDF: estimate DEFCON values for one match's credits."""
    from analytics.defcon_lite import estimate_values_for_match, DefconLiteParams
    params = DefconLiteParams()
    return estimate_values_for_match(pdf, params)
```

- [ ] **Step 2: Rewrite `_process_360_matches` to use two-pass pipeline**

Keep the incremental skip guard (lines 66-74). Replace the per-match loop (line 87+) with:

```python
# Join actions + freeze frames in Spark
joined_sdf = actions_sdf.join(ff_sdf, on="event_id", how="inner")

# Pass 1: credits by (match_id, period) — distributed
credits_schema = "..."  # full credit column list
credits_sdf = joined_sdf.groupBy("match_id", "period").applyInPandas(_assign_credits_udf, schema=credits_schema)

# Pass 2: values by match_id — XGBoost training per match
valued_schema = "..."  # credits + defcon_value
valued_sdf = credits_sdf.groupBy("match_id").applyInPandas(_estimate_values_udf, schema=valued_schema)

# Write
write_delta_table(valued_sdf, catalog, schema, _TABLE_NAME, ...)
```

- [ ] **Step 3: Apply same pattern to `_process_tracking_matches`**

- [ ] **Step 4: Run tests + quality checks**

Run: `uv run pytest src/tests/test_defcon_lite.py -v && uv run ruff check src/ingestion/defcon_lite.py && uv run pyright src/ingestion/defcon_lite.py`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/defcon_lite.py src/analytics/defcon_lite.py
git commit -m "feat(defcon): migrate to two-pass applyInPandas — credits by period, values by match (Phase 2c)"
```

---

### Task 17: Add Ward cluster caching to line-breaking (Phase 2d — algorithmic)

**Files:**
- Modify: `src/analytics/line_breaking.py:316-367` (`detect_line_breaking_batch`)
- Test: `src/tests/test_line_breaking.py`

Multiple passes in the same frame share identical opponent positions. Cache Ward clusters per unique opponent position hash.

- [ ] **Step 1: Write failing test for cluster caching**

```python
def test_cluster_cache_reuse(self):
    """Passes sharing same opponents must reuse clusters (not re-cluster)."""
    # Create 3 passes with identical opponent positions
    # Mock _cluster_opponents to track call count
    # Assert _cluster_opponents called once, not three times
```

- [ ] **Step 2: Implement caching in `detect_line_breaking_batch`**

```python
def detect_line_breaking_batch(
    passes_df: pd.DataFrame,
    opponents_by_event: dict[str, pd.DataFrame],
    params: LineBreakingParams | None = None,
) -> pd.DataFrame:
    if params is None:
        params = LineBreakingParams()

    cluster_cache: dict[bytes, tuple[list[np.ndarray], np.ndarray]] = {}
    results = []

    for _, row in passes_df.iterrows():
        event_id = str(row["event_id"])
        opponents = opponents_by_event.get(event_id)
        if opponents is None or len(opponents) < params.min_opponents:
            results.append({"event_id": event_id, **LineBreakingResult(False, 0, "none")._asdict()})
            continue

        # Hash opponent positions for cache key
        positions = np.column_stack([_col_f64(opponents, "x"), _col_f64(opponents, "y")])
        cache_key = positions.tobytes()

        if cache_key not in cluster_cache:
            clusters = _cluster_opponents(positions, params)
            segments = _build_line_segments(clusters, params) if clusters else np.empty((0, 2, 2))
            cluster_cache[cache_key] = (clusters, segments)

        clusters, segments = cluster_cache[cache_key]
        # ... test pass against cached clusters/segments ...
```

- [ ] **Step 3: Run tests — all 28 must pass**

Run: `uv run pytest src/tests/test_line_breaking.py -v`

- [ ] **Step 4: Quality checks + commit**

```bash
uv run ruff check src/analytics/line_breaking.py && uv run pyright src/analytics/line_breaking.py
git add src/analytics/line_breaking.py src/tests/test_line_breaking.py
git commit -m "perf(line-breaking): cache Ward clusters per unique opponent snapshot — ~3-5x speedup (Phase 2d)"
```

---

### Task 18: Migrate line-breaking ingestion to applyInPandas (Phase 2d — structural)

**Files:**
- Modify: `src/ingestion/line_breaking.py:108-232` (`_process_statsbomb_360`)
- Modify: `src/ingestion/line_breaking.py:234-362` (`_process_metrica_tracking`)

Replace per-match loops with `applyInPandas` grouped by `(match_id, period)`.

- [ ] **Step 1: Implement UDF for line-breaking detection**

```python
def _detect_line_breaking_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """UDF for applyInPandas — detect line-breaking passes for one (match_id, period) group."""
    from analytics.line_breaking import detect_line_breaking_batch, LineBreakingParams
    params = LineBreakingParams()
    # Split pdf into passes and opponents
    # Build opponents_by_event dict
    # Call detect_line_breaking_batch
    # Return results with match_id
```

- [ ] **Step 2: Rewrite Path A to use applyInPandas**

Join passes + opponents in Spark, group by `(match_id, period)`, apply UDF, write via `merge_delta_table`.

- [ ] **Step 3: Apply same pattern to Path B (Metrica tracking)**

- [ ] **Step 4: Run tests + quality checks**

Run: `uv run pytest src/tests/test_line_breaking.py -v && uv run ruff check src/ingestion/line_breaking.py && uv run pyright src/ingestion/line_breaking.py`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/line_breaking.py
git commit -m "feat(line-breaking): migrate to applyInPandas grouped by (match_id, period) (Phase 2d)"
```

---

## Chunk 5: Phase 2e-2f — SPADL/VAEP + Embeddings Migration

### Task 19: Migrate SPADL conversion to applyInPandas (Phase 2e)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:155-280` (`_convert_statsbomb_from_bronze`, `_convert_wyscout_from_bronze`)

Replace per-competition toPandas loops with `applyInPandas` grouped by `game_id`.

- [ ] **Step 1: Implement SPADL conversion UDF**

```python
def _convert_game_to_spadl_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """UDF: convert one game's events to SPADL actions."""
    import socceraction.spadl.statsbomb as sbspadl
    actions = sbspadl.convert_to_actions(pdf, home_team_id=pdf["home_team_id"].iloc[0])
    return _clean_spadl_for_spark(actions)
```

- [ ] **Step 2: Rewrite conversion functions**

Replace the per-competition, per-batch loops with:
```python
events_sdf = spark.table(events_table).filter(F.col("game_id").isin(new_game_ids))
spadl_sdf = events_sdf.groupBy("game_id").applyInPandas(_convert_game_to_spadl_udf, schema=spadl_schema)
write_delta_table(spadl_sdf, ...)
```

- [ ] **Step 3: Run tests + quality checks**

Run: `uv run pytest src/tests/test_spadl_vaep.py -v && uv run ruff check src/ingestion/spadl_vaep.py`

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/spadl_vaep.py
git commit -m "feat(spadl): migrate SPADL conversion to applyInPandas per game_id (Phase 2e)"
```

---

### Task 20: Migrate VAEP scoring to applyInPandas with UC Volume model loading (Phase 2e)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:605-737` (`run_pipeline` scoring section)

Training stays on driver (bounded). Scoring moves to executors with `_model_cache` pattern.

- [ ] **Step 1: Save trained models to UC Volume**

After training at line ~671, save models:
```python
scores_path = f"/Volumes/{catalog}/{schema}/vaep_models/vaep_scores.json"
concedes_path = f"/Volumes/{catalog}/{schema}/vaep_models/vaep_concedes.json"
model_scores.save_model(scores_path)
model_concedes.save_model(concedes_path)
```

- [ ] **Step 2: Implement scoring UDF with model cache**

```python
_model_cache: dict[str, XGBClassifier] = {}

def _score_competition_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """UDF: score one competition's SPADL actions with VAEP."""
    if "scores" not in _model_cache:
        from xgboost import XGBClassifier
        m = XGBClassifier()
        m.load_model("/Volumes/.../vaep_scores.json")
        _model_cache["scores"] = m
    if "concedes" not in _model_cache:
        m = XGBClassifier()
        m.load_model("/Volumes/.../vaep_concedes.json")
        _model_cache["concedes"] = m
    # Score using cached models
    return _score_competition(pdf, _model_cache["scores"], _model_cache["concedes"], logger=None)
```

- [ ] **Step 3: Replace scoring loop with applyInPandas**

```python
spadl_sdf = spark.table(spadl_table).filter(F.col("game_id").isin(unscored_games))
scored_sdf = spadl_sdf.groupBy("competition_id", "data_source").applyInPandas(
    _score_competition_udf, schema=vaep_schema
)
write_delta_table(scored_sdf, ...)
```

- [ ] **Step 4: Run tests + quality checks**

Run: `uv run pytest src/tests/test_spadl_vaep.py -v && uv run ruff check src/ingestion/spadl_vaep.py`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/spadl_vaep.py
git commit -m "feat(vaep): migrate scoring to applyInPandas with UC Volume model loading (Phase 2e)"
```

---

### Task 21: Migrate player embeddings to flat-partitioned applyInPandas (Phase 2f)

**Files:**
- Modify: `src/ingestion/player_embeddings.py:356-478` (`main`)

Replace driver-bound event loading and sequential inference with Spark-native joins and flat-partitioned `applyInPandas`.

- [ ] **Step 1: Move event join to Spark (eliminate driver-bound _load_events toPandas)**

Replace the `_load_events()` call (which does `.toPandas()` on ~10M rows) with a Spark-native join that stays distributed. Only pull metadata to the driver.

- [ ] **Step 2: Implement inference UDF with Doc2Vec model cache**

```python
_model_cache: dict[str, object] = {}

def _infer_embeddings_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    """UDF: infer Doc2Vec embeddings for a batch of players."""
    if "doc2vec" not in _model_cache:
        from gensim.models.doc2vec import Doc2Vec
        _model_cache["doc2vec"] = Doc2Vec.load("/Volumes/.../football2vec/player2vec.model")
    model = _model_cache["doc2vec"]
    # Tokenize and infer for each player in batch
    # Return: canonical_player_id, match_id, data_source, behavioral_vector, stat_vector
```

- [ ] **Step 3: Replace main flow with flat-partitioned pipeline**

```python
# Flat partitioning for balanced distribution
num_batches = max(1, total_players // 100)
player_sdf = player_sdf.withColumn(
    "batch_id", (F.monotonically_increasing_id() % num_batches).cast("int")
)
results_sdf = player_sdf.groupBy("batch_id").applyInPandas(
    _infer_embeddings_batch, schema=embedding_schema
)
```

- [ ] **Step 4: Run tests + quality checks**

Run: `uv run pytest src/tests/test_player_embeddings.py -v && uv run ruff check src/ingestion/player_embeddings.py && uv run pyright src/ingestion/player_embeddings.py`

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/player_embeddings.py
git commit -m "feat(embeddings): migrate to flat-partitioned applyInPandas with UC Volume model loading (Phase 2f)"
```

---

### Task 22: Phase 2 Epilogue — Rebuild wheel + end-to-end validation

**Files:**
- Modify: `CLAUDE.md` (add applyInPandas patterns — already done in design phase, verify)
- Modify: `ROADMAP.md` (mark TODO #12 resolved)
- Modify: `TODO.md` (close relevant items)

- [ ] **Step 1: Run full local test suite**

```bash
uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/ && uv run pytest src/tests/ -v
```
Expected: ALL PASS, 0 errors

- [ ] **Step 2: Build wheel**

```bash
uv build
```

- [ ] **Step 3: Deploy to Databricks**

Upload wheel to DBFS/UC Volume, update Terraform workflow library references if needed.

- [ ] **Step 4: Run end-to-end Databricks workflow**

Trigger the full ingestion workflow. Verify all 10 tasks pass:
1. ingest_statsbomb
2. ingest_metrica
3. ingest_wyscout
4. ingest_idsse
5. ingest_skillcorner
6. compute_spadl_vaep
7. compute_off_ball_xt
8. compute_defcon_lite
9. resolve_players
10. compute_embeddings

- [ ] **Step 5: Measure wall-clock timing**

Record per-task duration. Compare to baseline (pre-optimization).

- [ ] **Step 6: Update TODO.md**

Close items: #3 (partially — SELECT * fixed), #12 (off-ball xT scaling), #21 (SELECT * fixed), #24 (Metrica iterrows — Phase 6).

- [ ] **Step 7: Commit**

```bash
git add CLAUDE.md ROADMAP.md TODO.md
git commit -m "docs: update project docs after Phase 2 pipeline migrations"
```

---

## Chunk 6: Phase 3 — Streamlit & Frontend

### Task 23: Add LIMIT to VAEP rankings query (DB-02)

**Files:**
- Modify: `src/streamlit_app/pages/action_values.py:24-48`

- [ ] **Step 1: Add LIMIT 500 to the rankings query**

- [ ] **Step 2: Commit**

```bash
git add src/streamlit_app/pages/action_values.py
git commit -m "perf(streamlit): add LIMIT 500 to VAEP rankings query (DB-02)"
```

---

### Task 24: Replace SELECT DISTINCT with recursive CTE (DB-03)

**Files:**
- Modify: `src/streamlit_app/pages/action_values.py:193-201`
- Modify: `src/streamlit_app/pages/defensive_valuation.py:211-256`

Per CLAUDE.md: `SELECT DISTINCT` on fact tables forces sequential scans. Use recursive CTE loose index scan.

- [ ] **Step 1: Replace DISTINCT queries with recursive CTE pattern**

Follow the pattern already used in `pitch_control.py:27-44`:
```sql
WITH RECURSIVE distinct_vals AS (
    (SELECT player_id FROM fct_table WHERE ... ORDER BY player_id LIMIT 1)
    UNION ALL
    SELECT (SELECT player_id FROM fct_table WHERE player_id > d.player_id AND ... ORDER BY player_id LIMIT 1)
    FROM distinct_vals d WHERE d.player_id IS NOT NULL
)
SELECT player_id FROM distinct_vals WHERE player_id IS NOT NULL
```

- [ ] **Step 2: Quality checks + commit**

```bash
git add src/streamlit_app/pages/action_values.py src/streamlit_app/pages/defensive_valuation.py
git commit -m "perf(streamlit): replace SELECT DISTINCT with recursive CTE on fact tables (DB-03)"
```

---

### Task 25: Bound match timeline + merge duplicate round-trips + cache PC grid (FE-01, FE-02, FE-03)

**Files:**
- Modify: `src/streamlit_app/pages/action_values.py:91-123` (FE-01: add LIMIT 2000)
- Modify: `src/streamlit_app/pages/defensive_valuation.py:211-265` (FE-02: merge queries)
- Modify: `src/streamlit_app/pages/pitch_control.py:105-191` (FE-03: cache grid)

- [ ] **Step 1: FE-01 — Add LIMIT 2000 to match timeline query**

- [ ] **Step 2: FE-02 — Merge two player ID set queries into single query**

Combine `_load_breakdown_player_ids` and `_load_timeline_player_ids` into a single query that returns both sets.

- [ ] **Step 3: FE-03 — Cache pitch control grid computation**

Wrap the `compute_pitch_control_frame` call at line 159 with `@st.cache_data`:
```python
@st.cache_data(ttl=300)
def _compute_cached_pc_grid(frame_data_json: str) -> tuple:
    """Cache the physics computation (pure numpy, deterministic)."""
    frame_data = pd.read_json(frame_data_json)
    return compute_pitch_control_frame(frame_data)
```

- [ ] **Step 4: Quality checks + commit**

```bash
uv run ruff check src/streamlit_app/ && uv run pyright src/streamlit_app/
git add src/streamlit_app/pages/action_values.py src/streamlit_app/pages/defensive_valuation.py src/streamlit_app/pages/pitch_control.py
git commit -m "perf(streamlit): bound timeline, merge queries, cache PC grid (FE-01, FE-02, FE-03)"
```

---

## Chunk 7: Phase 4-6 — dbt + StatsBomb + Metrica

### Task 26: Convert fct_tracking_frames to incremental (DBT-01)

**Files:**
- Modify: `dbt_project/models/marts/fct_tracking_frames.sql`

- [ ] **Step 1: Add incremental config**

Replace the implicit `table` materialization with:
```sql
{{ config(
    materialized='incremental',
    unique_key='tracking_id',
    cluster_by=['match_id'],
    incremental_strategy='merge'
) }}
```

- [ ] **Step 2: Add is_incremental() guard to source CTE**

```sql
tracking as (
    select * from {{ ref('stg_metrica__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
    union all
    select * from {{ ref('stg_idsse__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
    union all
    select * from {{ ref('stg_skillcorner__tracking') }}
    {% if is_incremental() %}
    where match_id not in (select distinct match_id from {{ this }})
    {% endif %}
),
```

- [ ] **Step 3: Test dbt build**

```bash
cd dbt_project && dbt build --select fct_tracking_frames --vars '{off_ball_xt_enabled: true, defcon_enabled: true, entity_resolution_enabled: true, embeddings_enabled: true}'
```

- [ ] **Step 4: Commit**

```bash
git add dbt_project/models/marts/fct_tracking_frames.sql
git commit -m "perf(dbt): convert fct_tracking_frames to incremental materialization (DBT-01)"
```

---

### Task 27: Convert fct_player_embeddings to incremental + remove redundant LAG (DBT-02, DBT-03)

**Files:**
- Modify: `dbt_project/models/marts/fct_player_embeddings.sql`
- Modify: `dbt_project/models/marts/fct_physical_stats.sql:33-34`

- [ ] **Step 1: DBT-02 — Add incremental config to fct_player_embeddings**

```sql
{{ config(
    materialized='incremental',
    unique_key='embedding_id',
    enabled=var('embeddings_enabled', false),
    incremental_strategy='merge'
) }}
```

Add `is_incremental()` guard to filter for new canonical_player_id + match_id combinations.

- [ ] **Step 2: DBT-03 — Remove redundant LAG in fct_physical_stats**

Lines 33-34 re-derive `prev_x`/`prev_y` via LAG when `fct_tracking_frames` already computed velocity. Either:
- (a) Expose `prev_x`/`prev_y` from `fct_tracking_frames` — requires adding columns to its final SELECT
- (b) Derive displacement from `speed_ms / frame_rate` — avoids the extra window function entirely

Option (b) is simpler:
```sql
-- Replace:
--   lag(x) over (...) as prev_x,
--   lag(y) over (...) as prev_y,
-- With:
--   speed_ms / frame_rate as displacement_m,  -- already available in fct_tracking_frames
```

**Note:** If upstream columns change, synced table recreation required (follow MEMORY.md procedure).

- [ ] **Step 3: Test dbt build**

```bash
cd dbt_project && dbt build --select fct_player_embeddings fct_physical_stats
```

- [ ] **Step 4: Commit**

```bash
git add dbt_project/models/marts/fct_player_embeddings.sql dbt_project/models/marts/fct_physical_stats.sql
git commit -m "perf(dbt): convert embeddings to incremental, remove redundant LAG in physical_stats (DBT-02, DBT-03)"
```

---

### Task 28: StatsBomb HTTP concurrency (P0-10, P0-11)

**Files:**
- Modify: `src/ingestion/statsbomb.py:417-480` (`backfill_extra_json`)
- Modify: `src/ingestion/statsbomb.py:147-283` (`ingest_matches_and_details`)
- Test: `src/tests/test_statsbomb.py`

- [ ] **Step 1: Add ThreadPoolExecutor for backfill_extra_json**

Replace the sequential per-match loop with concurrent execution:
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {
        pool.submit(_build_raw_extra_json, match_id, logger): match_id
        for match_id in match_ids_to_backfill
    }
    for future in as_completed(futures):
        match_id = futures[future]
        extra_map = future.result()
        # ... write to Delta ...
```

- [ ] **Step 2: Add concurrency to ingest_matches_and_details**

Fetch events, lineups, and 360 for each match concurrently (3 parallel fetches per match, 2-4 matches at a time):

```python
def _fetch_match_details(match_id, comp_id, season_id):
    events = _safe_fetch(sb.events, match_id=match_id, ...)
    lineups = _safe_fetch(sb.lineups, match_id=match_id, ...)
    frames = _safe_fetch(sb.frames, match_id=match_id, ...)
    return match_id, events, lineups, frames

with ThreadPoolExecutor(max_workers=4) as pool:
    futures = [pool.submit(_fetch_match_details, mid, cid, sid) for mid in new_match_ids]
```

- [ ] **Step 3: Run tests + quality checks**

Run: `uv run pytest src/tests/test_statsbomb.py -v && uv run ruff check src/ingestion/statsbomb.py`

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/statsbomb.py
git commit -m "perf(statsbomb): add HTTP concurrency to backfill and ingestion (P0-10, P0-11)"
```

---

### Task 29: Replace Metrica iterrows with pd.melt (P9-08)

**Files:**
- Modify: `src/ingestion/metrica.py:438-488` (`_reshape_tracking_to_narrow`)
- Test: `src/tests/test_metrica.py`

- [ ] **Step 1: Run existing tests to establish baseline**

Run: `uv run pytest src/tests/test_metrica.py::TestReshapeTrackingToNarrow -v`

- [ ] **Step 2: Replace iterrows with pd.melt + regex extraction**

The current code at line 451 iterates row-by-row to build player JSON from wide columns (`Home_1_x`, `Home_1_y`, etc.). Replace with:

```python
def _reshape_tracking_to_narrow(df: pd.DataFrame, match_id: str) -> pd.DataFrame:
    # Extract player columns using regex
    home_cols = [c for c in df.columns if re.match(r"Home_\d+_[xy]", c)]
    away_cols = [c for c in df.columns if re.match(r"Away_\d+_[xy]", c)]

    # Melt home players
    home_x = [c for c in home_cols if c.endswith("_x")]
    home_y = [c for c in home_cols if c.endswith("_y")]
    # ... vectorized reshape using melt + pivot ...
```

- [ ] **Step 3: Run tests — must still pass**

Run: `uv run pytest src/tests/test_metrica.py -v`

- [ ] **Step 4: Quality checks + commit**

```bash
uv run ruff check src/ingestion/metrica.py && uv run pyright src/ingestion/metrica.py
git add src/ingestion/metrica.py
git commit -m "perf(metrica): replace iterrows with pd.melt in tracking reshape (P9-08)"
```

---

## Chunk 8: Phase 7-9 — Terraform, Profiling, Backlog + Deployment

### Task 30: Add AWS budget alarm (COST-01)

**Files:**
- Modify: `terraform/environments/dev/main.tf`

- [ ] **Step 1: Add aws_budgets_budget resource**

```hcl
resource "aws_budgets_budget" "monthly" {
  name         = "luxury-lakehouse-monthly-${var.environment}"
  budget_type  = "COST"
  limit_amount = "100"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
```

- [ ] **Step 2: Tighten workflow timeouts (COST-03)**

In `terraform/modules/workflows/main.tf`, change:
- `ingest_metrica`: 1800 → 900 (15 min)
- `ingest_wyscout`: 1800 → 900 (15 min)
- `resolve_players`: 1800 → 900 (15 min)

- [ ] **Step 3: Terraform plan + apply**

```bash
cd terraform/environments/dev && AWS_PROFILE=devops-agent terraform plan
```

- [ ] **Step 4: Commit**

```bash
git add terraform/environments/dev/main.tf terraform/modules/workflows/main.tf
git commit -m "infra: add AWS budget alarm at $100/month, tighten workflow timeouts (COST-01, COST-03)"
```

---

### Task 31: Add pytest-benchmark for critical paths (PROF-01)

**Files:**
- Modify: `pyproject.toml` (add `pytest-benchmark` to dev deps)
- Create: `src/tests/test_benchmarks.py`

- [ ] **Step 1: Add pytest-benchmark dependency**

```bash
uv add --dev pytest-benchmark
```

- [ ] **Step 2: Create benchmark tests**

```python
"""Performance benchmarks for critical-path functions."""
import numpy as np
import pandas as pd
import pytest

from analytics.pitch_control import compute_pitch_control_at_points, PitchControlParams
from analytics.off_ball_xt import compute_off_ball_xt_frame
from analytics.defcon_lite import assign_defensive_credits, DefconLiteParams
from analytics.line_breaking import detect_line_breaking, LineBreakingParams


def _make_players_df(n_home=11, n_away=11):
    """Create a realistic players DataFrame for benchmarking."""
    # ... fixture with realistic positions and velocities ...


class TestBenchmarks:
    def test_bench_batched_pitch_control(self, benchmark):
        players = _make_players_df()
        targets = np.array([[x, y] for x in range(10, 111, 10) for y in range(10, 71, 10)])
        benchmark(compute_pitch_control_at_points, players, targets)

    def test_bench_off_ball_xt_frame(self, benchmark):
        players = _make_players_df()
        xt_grid = np.random.rand(8, 12)
        benchmark(compute_off_ball_xt_frame, players, xt_grid)

    def test_bench_defcon_credit_assignment(self, benchmark):
        action = {...}  # realistic action dict
        defenders = _make_defenders_df()
        benchmark(assign_defensive_credits, action, defenders, DefconLiteParams())

    def test_bench_line_breaking_detection(self, benchmark):
        opponents = _make_opponents_df()
        benchmark(detect_line_breaking, 40.0, 40.0, 80.0, 40.0, opponents)
```

- [ ] **Step 3: Run benchmarks**

```bash
uv run pytest src/tests/test_benchmarks.py -v --benchmark-only
```

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock src/tests/test_benchmarks.py
git commit -m "test: add pytest-benchmark tests for critical-path functions (PROF-01)"
```

---

### Task 32: Document performance baselines + budgets (PROF-02, PROF-03)

**Files:**
- Modify: `CLAUDE.md` (add performance budgets section)

- [ ] **Step 1: Add performance budgets to CLAUDE.md**

Add under the existing "Databricks Serverless Constraints" section:

```markdown
### Performance Budgets

- **Pipeline task timeout**: ingest tasks ≤15 min, compute tasks ≤2 hr
- **Streamlit page load**: ≤3 seconds (first load), ≤500ms (cached interaction)
- **UDF group memory**: ≤800 MB peak (1 GB limit minus overhead)
- **Batched pitch control**: ≤5ms per frame for 22 targets (benchmark baseline)
- **Line-breaking detection**: ≤2ms per pass (benchmark baseline)
```

- [ ] **Step 2: Record actual timing baselines from Phase 2 epilogue measurements**

Document in `docs/performance-baselines.md`:
- Per-pipeline wall-clock (before/after optimization)
- Per-function benchmark results from pytest-benchmark
- Streamlit page load times (first load + cached)

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md docs/performance-baselines.md
git commit -m "docs: add performance budgets and baselines (PROF-02, PROF-03)"
```

---

### Task 33: Low-priority backlog (Phase 9)

**Files:**
- Modify: `src/ingestion/off_ball_xt.py:59,73` (P9-09: xT grid iterrows)
- Modify: `src/ingestion/spadl_adapter.py:158` (P9-10: team extraction iterrows)

- [ ] **Step 1: P9-09 — Replace xT grid iterrows with NumPy indexing**

```python
# Before:
for _, row in xt_df.iterrows():
    grid[int(row["y_zone"])][int(row["x_zone"])] = row["xt_value"]

# After:
grid[xt_df["y_zone"].astype(int).values, xt_df["x_zone"].astype(int).values] = xt_df["xt_value"].values
```

- [ ] **Step 2: P9-10 — Replace spadl_adapter iterrows**

Replace with `.apply().to_dict()` or zip pattern.

- [ ] **Step 3: Quality checks + commit**

```bash
uv run ruff check src/ && uv run pytest src/tests/ -v
git add src/ingestion/off_ball_xt.py src/ingestion/spadl_adapter.py
git commit -m "perf: replace trivial iterrows with vectorized ops (P9-09, P9-10)"
```

---

### Task 34: Full deployment — dbt build + synced tables + indexes + grants + Streamlit

This is the final deployment task. Everything must be tested end-to-end.

- [ ] **Step 1: Run full dbt build**

```bash
cd dbt_project && dbt build --vars '{off_ball_xt_enabled: true, defcon_enabled: true, entity_resolution_enabled: true, embeddings_enabled: true}'
```
Expected: All models pass, all tests pass.

- [ ] **Step 2: Recreate synced tables if schema changed**

Follow MEMORY.md synced table recreation procedure for any tables with schema changes:
1. Delete via Terraform
2. Drop PG ghost
3. Recreate in Databricks UI
4. Import into Terraform
5. Restore PG indexes
6. Restore PG grants

- [ ] **Step 3: Restore PG indexes**

```bash
.venv/Scripts/python.exe scripts/create_indexes.py
.venv/Scripts/python.exe scripts/create_indexes.py --verify
```
Expected: All indexes created, `--verify` confirms Index Scan on all fact tables.

- [ ] **Step 4: Restore PG grants**

Run grant SQL via psycopg2 for SP `be66af99-5296-4fd9-887a-c081bce38bfa`.

- [ ] **Step 5: Deploy Streamlit app**

```bash
databricks sync . /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse --profile OAUTH
databricks apps deploy soccer-analytics-dashboard-dev --source-code-path /Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse --profile OAUTH
```

- [ ] **Step 6: Verify all 11 Streamlit pages load**

Open app URL and test each page:
1. Shot Map
2. Pass Map
3. Heat Map
4. Pass Network
5. Action Values
6. Player Radar
7. Match Summary
8. Movement Analysis
9. Pitch Control
10. Defensive Valuation
11. Player Similarity

Verify: pages load in <3s, cached interactions in <500ms, no "querying..." spinners on cached data.

- [ ] **Step 7: Run end-to-end Databricks workflow (final)**

Trigger full workflow. All 10 tasks must pass. No OOM. No UDF memory errors.

- [ ] **Step 8: Final commit + update TODO.md**

Close all resolved tech debt items in TODO.md. Update phase status.

```bash
git add -A
git commit -m "deploy: full end-to-end deployment — 42 findings addressed, 6 pipelines migrated"
```

---

## Definition of Done

- [ ] All 42 audit findings addressed (fixed or tracked with justification)
- [ ] All 6 NEXTSTEP pipelines migrated to `applyInPandas`
- [ ] All existing unit tests pass + new tests for batched PC, cluster caching, two-pass DEFCON, model loading, flat partitioning
- [ ] Zero ruff violations, zero pyright errors
- [ ] `pytest-benchmark` tests for critical paths
- [ ] Performance baselines documented
- [ ] dbt build passes (incremental models, updated marts)
- [ ] Synced tables recreated where schema changed
- [ ] PG indexes restored (`scripts/create_indexes.py --verify` confirms Index Scan on all fact tables)
- [ ] PG grants restored for SP `be66af99-...`
- [ ] Wheel rebuilt and deployed to Databricks workflow
- [ ] End-to-end Databricks workflow: 10/10 tasks pass, no OOM, no UDF memory errors
- [ ] Wall-clock timing measured and compared to sequential baseline
- [ ] Streamlit app deployed and verified (all 11 pages load, cache hits confirmed)
- [ ] CLAUDE.md, ROADMAP.md, TODO.md updated
- [ ] Branch merged to main
