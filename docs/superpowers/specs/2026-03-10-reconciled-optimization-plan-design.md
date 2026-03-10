# Reconciled Optimization Plan — Luxury Lakehouse

> **Date**: 2026-03-10
> **Branch**: `perf/initial-optimization-audit`
> **Sources**: `NEXTSTEP.md` (applyInPandas migration plan) + `OPTIMIZATIONS-140.md` (v1.4.0 audit, 42 findings)
> **Approach**: Layered — Bugs > Quick Wins > Pipeline Migrations > Sweep > Posture

---

## Design Principle

Do not scale anything down regardless of current data volume. Fix every finding so the system runs correctly for a very long time — at Respo.Vision scale (~7M rows/match, ~500 matches) and beyond.

---

## Approach Selection

Three approaches were evaluated:

| Approach | Strategy | Verdict |
|----------|----------|---------|
| **A: Severity-Stratified** | Fix all Criticals > Highs > Mediums > Lows across all categories | Rejected — context-switching between pipelines, Streamlit, dbt, Terraform; ignores pipeline dependencies |
| **B: NEXTSTEP-Led** | Follow NEXTSTEP.md pipeline order, sweep remaining after | Rejected — defers quick wins (Streamlit cache, double count) until after all 6 migrations |
| **C: Layered** | Bugs > Quick Wins > Pipelines > Sweep > Posture | **Selected** — each layer is reviewable/mergeable; quick wins deliver immediate value; pipelines get full focus; sweep catches everything; posture prevents regression |

---

## Phase 0: Critical Bugfixes (3 items)

Unblock production. All three are blockers in the audit report.

| # | Finding | File | Fix | Impact |
|---|---------|------|-----|--------|
| 0.1 | **P9-01**: Embeddings incremental skip bug — `source_matches` query returns 0 Wyscout rows, guard bypassed, full recompute > OOM | `ingestion/player_embeddings.py:382-386` | Fix query to include Wyscout match IDs; add fallback guard (if `source_matches` empty but embeddings table exists, skip) | Prevents OOM on every workflow run |
| 0.2 | **P0-01**: `_load_events()` pulls full SB+Wyscout event join (~10M rows) to 16 GB driver | `ingestion/player_embeddings.py:188` | Add `.filter()` to bound the toPandas call; long-term fix in Phase 2f replaces toPandas entirely | OOM guard even when skip bug is fixed |
| 0.3 | **CACHE-01**: `_cached_query` re-defines `@st.cache_data` inner function per call — cache key instability, every widget interaction hits Lakebase | `streamlit_app/components/filters.py:14-21` | Move `_run` to module level so the decorator is applied once | Eliminates redundant PG queries on all 11 pages |

**Dependencies:** None. All independent.
**Files changed:** `ingestion/player_embeddings.py`, `streamlit_app/components/filters.py`

---

## Phase 1: Quick Wins (8 items, <30 min each)

High-impact, low-effort fixes. Each is a 1-5 line change.

| # | Finding | File | Fix | Impact |
|---|---------|------|-----|--------|
| 1.1 | **DB-01**: 5 `write_delta_table()` calls missing `row_count` — each triggers extra `df.count()` DAG recomputation | `wyscout.py:213,256,417`, `entity_resolution.py:116`, `player_embeddings.py:466` | Pass `row_count=row_count` from prior `validate_dataframe()` call | 5 eliminated Spark DAG recomputations per pipeline run |
| 1.2 | **P0-07**: `SELECT *` in `backfill_extra_json` — only `id` and `_raw_extra_json` needed | `ingestion/statsbomb.py:454` | Change to `SELECT id, _raw_extra_json FROM ...` | ~30-50% memory reduction per match in backfill loop |
| 1.3 | **P0-06**: Standalone `df.count()` before VAEP training pull triggers extra DAG recomputation | `ingestion/spadl_vaep.py:636` | Remove standalone count; use count from `validate_dataframe()` or `write_delta_table()` flow | 1 eliminated DAG recomputation |
| 1.4 | **P0-05**: `re.sub()` not pre-compiled in Metrica ingestion | `ingestion/metrica.py:569` | Move `re.compile()` to module level | Trivial — eliminates per-column regex compilation |
| 1.5 | **NET-01**: `fetch_url` creates new TCP connection per call | `ingestion/utils.py:321` | Use `requests.Session` with keep-alive for repeated calls to same host | Reuses TCP connections; ~4 saved handshakes per Metrica match |
| 1.6 | **CACHE-02**: Same inner-function cache bypass pattern in action_values.py | `streamlit_app/pages/action_values.py:192` | Same fix as CACHE-01 — move function to module level | Cache hits on player option queries |
| 1.7 | **P0-09**: `fct_player_stats` join toPandas with no filter (19K rows) | `ingestion/player_embeddings.py:224` | Add `.filter()` for relevant data_source+competition | Reduces driver memory; full fix in Phase 2f |
| 1.8 | **Missing skip guard**: Line-breaking has NO incremental skip check — processes all 323 matches every run | `ingestion/line_breaking.py` | Add incremental skip pattern matching `off_ball_xt.py:108-126` with `str()` normalization | Prevents redundant recomputation of all matches |

**Dependencies:** None. All independent; parallelizable.
**Files changed:** 8 files (one edit each)

---

## Phase 2: Pipeline Migrations (6 pipelines, NEXTSTEP order)

The structural unlock. Moves all analytics computation from driver-bound sequential loops to executor-distributed `applyInPandas`. Ordered by ROI and dependency chain.

### Phase 2a: Batched Pitch Control (foundation)

**New function**: `compute_pitch_control_at_points(players_df, target_points: np.ndarray, params) -> np.ndarray`

| What | Detail |
|------|--------|
| Problem | `compute_pitch_control_at_point()` called 22x/frame, each re-splitting home/away, re-converting coords, re-building TTI matrices |
| Fix | Accept `(n, 2)` target array. One matrix setup per frame, NumPy broadcasting across all targets |
| Speedup | ~15-20x per frame (eliminates 21 redundant setups) |
| Foundation for | Phase 2b (off-ball xT), future OBSO/Space Creation |

**Files changed:** `src/analytics/pitch_control.py`
**Tests:** New unit tests for batch function; all existing pitch control tests must still pass.

### Phase 2b: Off-Ball xT Migration

**Resolves:** P0-03, P0-04, P0-08, P0-14, P9-02, P9-03, P9-09

| What | Detail |
|------|--------|
| Algorithmic | Rewrite `compute_off_ball_xt_frame()` to use batched pitch control from 2a |
| Structural | Replace per-match `toPandas()` loop with two-pass `applyInPandas` |
| Group key | `(match_id, frame_batch_id)` — synthetic key: `(frame / (frame_rate * batch_size)).cast("int")`, batch_size=270 (~5 min at 1fps) |
| Pass 1 | `applyInPandas(compute_off_ball_xt_batch, schema)` grouped by `(match_id, frame_batch_id)` — per-player xT per batch |
| Pass 2 | Spark-native `groupBy("match_id", "player_id").agg(sum, count)` — aggregate across batches |
| Per-group size | ~270 frames x 22 players = ~6K rows = ~50-100 KB (well under 1 GB limit) |
| Groups | ~20 batches x 20 matches = ~400 groups |
| Combined speedup | ~100-200x (batched PC ~15x x parallelism ~10-20x) |
| P9-02 absorbed | Frame groupby dict is built inside the UDF, not in driver code |
| P9-09 absorbed | xT grid lookup inside UDF uses NumPy fancy indexing |

**Files changed:** `src/analytics/off_ball_xt.py`, `src/ingestion/off_ball_xt.py`
**Tests:** Existing off-ball xT tests + new tests for batch UDF output schema.

### Phase 2c: DEFCON Two-Pass Migration

**Resolves:** P0-02, P0-13, P9-04, P9-07

| What | Detail |
|------|--------|
| Architecture | Two-pass distributed execution (NEXTSTEP Option B) |
| Pass 1 | Credit assignment by `(match_id, period)` — geometric computations distributed across executors |
| Pass 2 | XGBoost value estimation by `match_id` — trains on combined credits from both halves (preserves model quality) |
| Groups (Pass 1) | ~328 matches x 2 periods = ~656 groups |
| Groups (Pass 2) | ~328 matches |
| P9-07 note | XGBoost stays per-match (not global) in Pass 2 — training on ~1K credits is fast; model quality matters more than training speedup |
| Speedup | ~4-8x |

**Files changed:** `src/analytics/defcon_lite.py` (split `compute_defcon_match()` into credit + value functions), `src/ingestion/defcon_lite.py`
**Tests:** Existing DEFCON tests + new tests for two-pass output equivalence.

### Phase 2d: Line-Breaking Migration

**Resolves:** P0-12, P9-05

| What | Detail |
|------|--------|
| Algorithmic | Ward cluster caching per unique opponent snapshot — hash opponent positions, cache clusters+segments, test all passes against cached result |
| Structural | Replace per-match loop with `applyInPandas` grouped by `(match_id, period)` |
| Groups | ~3,500 matches x 2 periods = ~7,000 groups |
| Cache hit rate | StatsBomb 360: low (per-event freeze frames). Tracking data: high (per-frame positions, multiple passes per frame) |
| Speedup | ~12-40x (caching ~3-5x x parallelism ~4-8x) |
| Note | Phase 1.8 already added the incremental skip guard |

**Files changed:** `src/analytics/line_breaking.py`, `src/ingestion/line_breaking.py`
**Tests:** Existing line-breaking tests + new tests for cluster cache correctness.

### Phase 2e: SPADL/VAEP Migration

**Resolves:** P9-06

| What | Detail |
|------|--------|
| SPADL conversion | `applyInPandas` grouped by `game_id` — each game's `convert_to_actions()` is independent |
| VAEP scoring | `applyInPandas` grouped by `(competition_id, data_source)` — UC Volume model loading with `_model_cache` pattern |
| Training | Stays on driver (bounded: 200 representative games, one-time operation). Models saved to UC Volume for executor access |
| SPADL groups | ~3,500 games, ~2K events each = ~20 MB per group |
| Scoring groups | ~50 (competition x source), ~200 MB per group |
| Model loading | `_model_cache: dict[str, XGBClassifier]` at module level; lazy-load from `/Volumes/soccer_analytics/{schema}/model_weights/` |
| Speedup | SPADL ~4-8x, Scoring ~3-5x |

**Files changed:** `src/ingestion/spadl_vaep.py`
**Tests:** Existing SPADL/VAEP tests + model loading/caching test.

### Phase 2f: Player Embeddings Migration

**Resolves:** P0-01 (full fix), P0-09 (full fix)

| What | Detail |
|------|--------|
| Event loading | Replace driver-bound `_load_events()` toPandas with Spark-native join — events never hit driver |
| Stat vectors | Filter to relevant players only before toPandas (bounded subset) |
| Inference | Flat batch partitioning: `batch_id = (monotonically_increasing_id() % num_batches).cast("int")` |
| Doc2Vec model | `_model_cache` pattern, loaded from UC Volume (`/Volumes/soccer_analytics/{schema}/model_weights/`) |
| Groups | ~8,950 players / 100 per batch = ~90 groups |
| Speedup | ~3-4x |

**Files changed:** `src/ingestion/player_embeddings.py`
**Tests:** Existing embeddings tests + flat partitioning test.

### Phase 2 Epilogue: Validation & Documentation

| Task | Detail |
|------|--------|
| End-to-end workflow | Run full Databricks workflow (all 10 tasks) on serverless — verify 10/10 pass, no OOM, no UDF memory errors |
| Timing comparison | Measure wall-clock per pipeline vs sequential baseline |
| CLAUDE.md update | Add `applyInPandas` patterns, serverless constraints, model loading pattern, group sizing guidance |
| ROADMAP.md update | Mark TODO #12 resolved (off-ball xT scaling), update EIP section |
| TODO.md update | Close relevant tech debt items |

---

## Phase 3: Streamlit & Frontend (5 items)

Remaining Streamlit optimizations not covered by Phase 0/1.

| # | Finding | File | Fix |
|---|---------|------|-----|
| 3.1 | **DB-02**: VAEP rankings query has no LIMIT | `action_values.py:32` | Add `LIMIT 500` (or reasonable cap) |
| 3.2 | **DB-03**: `SELECT DISTINCT` on 800K-2M row fact tables | `action_values.py:195`, `defensive_valuation.py:220-256` | Replace with recursive CTE loose index scan (per CLAUDE.md) |
| 3.3 | **FE-01**: Unbounded match timeline | `action_values.py:107` | Add `LIMIT 2000` |
| 3.4 | **FE-02**: Two separate DB round-trips for player ID sets | `defensive_valuation.py:212-264` | Merge into single query |
| 3.5 | **FE-03**: Pitch control physics grid recomputed per toggle | `pitch_control.py:159` | Cache grid computation with `@st.cache_data` |

**Files changed:** 3 Streamlit page files

---

## Phase 4: dbt Incremental Models (3 items)

| # | Finding | Fix | Impact |
|---|---------|-----|--------|
| 4.1 | **DBT-01**: `fct_tracking_frames` full rebuild (38.1M rows) | Convert to `materialized='incremental'` with `unique_key='tracking_id'`, `is_incremental()` guard | Largest dbt model; incremental saves minutes per run |
| 4.2 | **DBT-02**: `fct_player_embeddings` full rebuild (87K rows) | Convert to incremental | Moderate savings |
| 4.3 | **DBT-03**: Redundant `LAG()` on 38.1M rows in `fct_physical_stats` | Derive displacement from upstream `fct_tracking_frames` columns; remove redundant window | Eliminates unnecessary window on largest table |

**Note:** DBT-03 requires synced table recreation for `fct_tracking_frames` if upstream columns change. Follow MEMORY.md recreation procedure.

---

## Phase 5: StatsBomb HTTP Optimization (2 items)

| # | Finding | Fix | Impact |
|---|---------|-----|--------|
| 5.1 | **P0-10**: `backfill_extra_json` — N sequential SELECT+HTTP per match | Batch: SELECT specific columns in bulk, batch HTTP requests with `requests.Session` + `concurrent.futures.ThreadPoolExecutor` | ~3,500 sequential calls > batched parallel |
| 5.2 | **P0-11**: `ingest_matches_and_details` — 3N sequential HTTP calls | `ThreadPoolExecutor` for concurrent event+lineup+360 fetches per match (respects rate limits) | 3x concurrency per match batch |

**Note:** NEXTSTEP.md classified StatsBomb as "not migrating" (I/O-bound), which is correct for `applyInPandas`. HTTP concurrency is a different optimization axis — we're parallelizing I/O, not distributing compute.

---

## Phase 6: Metrica Reshape (1 item)

| # | Finding | Fix | Impact |
|---|---------|-----|--------|
| 6.1 | **P9-08**: `.iterrows()` for wide-to-narrow tracking reshape (~270K frames per match) | Replace with `pd.melt()` + regex column extraction | C-speed vs Python-speed on 270K rows |

**Files changed:** `src/ingestion/metrica.py:451`

---

## Phase 7: Terraform & Cost (3 items)

| # | Finding | Fix | Impact |
|---|---------|-----|--------|
| 7.1 | **COST-01**: No `aws_budgets_budget` — $100/month target unenforced | Add `aws_budgets_budget` resource with SNS notification | Silent overspend prevention |
| 7.2 | **COST-02**: Databricks App has no auto-suspend | Investigate App auto-suspend option (may be blocked by provider) | Minimal idle cost reduction |
| 7.3 | **COST-03**: Over-generous workflow timeouts (30 min for 5-10 min tasks) | Reduce `ingest_metrica`, `ingest_wyscout`, `resolve_players` to 15 min | Faster failure detection |

---

## Phase 8: Profiling Posture (3 items)

Establish performance regression detection so future changes don't silently degrade.

| # | Finding | Fix | Impact |
|---|---------|-----|--------|
| 8.1 | **PROF-01**: No performance test suite | Add `pytest-benchmark` tests for critical paths: batched pitch control, off-ball xT frame, DEFCON credit assignment, line-breaking detection | Regression detection in CI |
| 8.2 | **PROF-02**: No latency baselines documented | Document p50/p95/p99 for pipeline tasks (from Phase 2 timing) and Streamlit page loads | Baseline for comparison |
| 8.3 | **PROF-03**: No performance budgets | Define max pipeline duration, max Streamlit page load time, max memory per UDF group in CLAUDE.md | Enforced standards |

---

## Phase 9: Low-Priority Backlog (2 items)

Negligible impact. Track but deprioritize.

| # | Finding | Fix | Impact |
|---|---------|-----|--------|
| 9.1 | **P9-09**: `.iterrows()` on 96-row xT grid | NumPy fancy indexing | ~96 rows — no measurable gain |
| 9.2 | **P9-10**: `.iterrows()` for JSON team extraction (~500 rows) | `.apply().to_dict()` | ~500 rows — no measurable gain |

---

## Execution Dependencies

```
Phase 0  (Critical bugs — independent, parallel)
    |
Phase 1  (Quick wins — independent, parallel)
    |
Phase 2a (Batched pitch control — foundation)
    |
Phase 2b (Off-ball xT — depends on 2a)
    |          (parallel from here)
Phase 2c (DEFCON)  <->  Phase 2d (Line-breaking)
    |
Phase 2e (SPADL/VAEP — validates model loading)
    |
Phase 2f (Embeddings — validates flat partitioning)
    |
Phase 2 Epilogue (E2E validation)
    |
Phases 3-8  (all independent, parallel)
    |
Phase 9  (backlog — whenever)
```

---

## Finding Reconciliation Matrix

### OPTIMIZATIONS-140.md: 42/42 Mapped

| Finding | Phase | Status |
|---------|-------|--------|
| P0-01 | 0.2 + 2f | Critical bugfix then full fix in migration |
| P0-02 | 2c | DEFCON migration |
| P0-03 | 2b | Off-ball xT migration |
| P0-04 | 2b | Off-ball xT migration |
| P0-05 | 1.4 | Quick win |
| P0-06 | 1.3 | Quick win |
| P0-07 | 1.2 | Quick win |
| P0-08 | 2b | Off-ball xT migration |
| P0-09 | 1.7 + 2f | Quick fix then full fix in migration |
| P0-10 | 5.1 | StatsBomb HTTP |
| P0-11 | 5.2 | StatsBomb HTTP |
| P0-12 | 2d | Line-breaking migration |
| P0-13 | 2c | DEFCON migration |
| P0-14 | 2b | Off-ball xT migration |
| DB-01 | 1.1 | Quick win |
| DB-02 | 3.1 | Streamlit |
| DB-03 | 3.2 | Streamlit |
| CACHE-01 | 0.3 | Critical bugfix |
| CACHE-02 | 1.6 | Quick win |
| NET-01 | 1.5 | Quick win |
| FE-01 | 3.3 | Streamlit |
| FE-02 | 3.4 | Streamlit |
| FE-03 | 3.5 | Streamlit |
| P9-01 | 0.1 | Critical bugfix |
| P9-02 | 2b | Absorbed into migration |
| P9-03 | 2b | Off-ball xT migration |
| P9-04 | 2c | DEFCON migration |
| P9-05 | 2d | Line-breaking migration |
| P9-06 | 2e | SPADL/VAEP migration |
| P9-07 | 2c | DEFCON migration (stays per-match) |
| P9-08 | 6.1 | Metrica reshape |
| P9-09 | 9.1 | Low-priority backlog |
| P9-10 | 9.2 | Low-priority backlog |
| DBT-01 | 4.1 | dbt incremental |
| DBT-02 | 4.2 | dbt incremental |
| DBT-03 | 4.3 | dbt incremental |
| COST-01 | 7.1 | Terraform |
| COST-02 | 7.2 | Terraform |
| COST-03 | 7.3 | Terraform |
| PROF-01 | 8.1 | Profiling posture |
| PROF-02 | 8.2 | Profiling posture |
| PROF-03 | 8.3 | Profiling posture |

### NEXTSTEP.md: All Items Mapped

| NEXTSTEP Item | Phase |
|---------------|-------|
| Batched `compute_pitch_control_at_points()` | 2a |
| Off-ball xT frame-batch `applyInPandas` | 2b |
| DEFCON two-pass (credits by period, values by match) | 2c |
| Line-breaking cluster caching + period split | 2d |
| SPADL/VAEP per-game `applyInPandas` | 2e |
| VAEP scoring UC Volume model loading | 2e |
| Player embeddings flat partitioning | 2f |
| `_model_cache` executor pattern | 2e, 2f |
| Synthetic partition keys | 2b |
| Serverless constraints documentation | 2 epilogue |
| End-to-end validation | 2 epilogue |
| Respo.Vision preparation | 2b (frame-batch pattern reusable) |
| CLAUDE.md / ROADMAP.md updates | 2 epilogue |

### Items Discovered During Reconciliation

| Item | Phase |
|------|-------|
| Line-breaking missing incremental skip guard | 1.8 |

---

## Databricks Serverless Constraints

### Hard Constraints

| Constraint | Impact | Mitigation |
|---|---|---|
| **1 GB per-UDF memory limit** | `applyInPandas` materializes entire group as one pandas DataFrame | All pipelines fit after subdivision (largest: ~200 MB per VAEP scoring group) |
| **No broadcast variables** | Cannot use `spark.sparkContext.broadcast()` | Close over small objects in function closure (xT grid = 768 bytes, pitch control params = frozen dataclass). Load larger objects (XGBoost, Doc2Vec) from UC Volume inside function body |
| **No internet access in UDFs** | No HTTP calls inside function body | Not an issue — all pipelines read from Delta/UC Volume |
| **No `df.cache()` / `df.persist()`** | Cannot cache intermediate Spark DataFrames | Write intermediate results to Delta if needed, or structure DAG to avoid re-reads |
| **Lazy closure capture** | Variables captured at action time, not definition time | Use frozen dataclasses for config. Don't mutate between definition and `.applyInPandas()` call |

### Model Loading Pattern

```python
# Module-level cache — Spark reuses Python worker processes across groups,
# so the model is loaded once per executor, not once per group.
_model_cache: dict[str, object] = {}

def process_group(pdf: pd.DataFrame) -> pd.DataFrame:
    if "model" not in _model_cache:
        _model_cache["model"] = load_model_from_uc_volume(...)
    model = _model_cache["model"]
    # ... use model ...
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
- [ ] Synced tables recreated where schema changed (following MEMORY.md procedure)
- [ ] PG indexes restored (`scripts/create_indexes.py --verify` confirms Index Scan on all fact tables)
- [ ] PG grants restored for SP `be66af99-...`
- [ ] Wheel rebuilt and deployed to Databricks workflow
- [ ] End-to-end Databricks workflow: 10/10 tasks pass, no OOM, no UDF memory errors
- [ ] Wall-clock timing measured and compared to sequential baseline
- [ ] Streamlit app deployed and verified (all 11 pages load, cache hits confirmed)
- [ ] CLAUDE.md, ROADMAP.md, TODO.md updated
- [ ] Branch merged to main
