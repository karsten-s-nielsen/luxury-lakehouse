# SB360 Action-Context: Snapshot Vectorization + Distributed Rewrite — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make StatsBomb 360 (sb360) action-context processing fast and scalable by (1) vectorizing the dominant `build_snapshots` loop, (2) excluding the velocity-degenerate ghost-GK enricher + emitting `pitch_control_at_target__voronoi`, and (3) moving the sb360 path from per-match driver-side `toPandas` to a distributed `cogroup.applyInPandas` job.

**Architecture:** sb360 enrichment currently runs **driver-side, per match** (`_process_statsbomb_match` → `_run_sb360_enrichment`): `toPandas` the actions + raw freeze-frames, build snapshots in a Python `iterrows` loop, enrich in pandas, write. We keep the pure enrichment (`_enrich_sb360_match`) but (a) replace the `iterrows`+`json.loads` snapshot loop with vectorized pandas, (b) lift snapshot-building into the analytics core as a pure, tested function shared by production + the local hexagon, and (c) process **all pending statsbomb matches in one distributed `cogroup.applyInPandas` job** (scan each bronze table once, enrich per match on executors, distributed write) so statsbomb exits the per-match drain.

**Tech Stack:** PySpark (serverless, `cogroup.applyInPandas`/`mapInPandas`, ADR-045 closure rules), pandas/numpy (vectorized), silly-kicks 4.32.0 enrichers, the action-context hexagon (`analytics.action_context.*` ports + `run_work_unit`/`enrich_batch`), Delta `replaceWhere`, dbt mart `fct_action_context`.

---

## Measured findings that drive this plan (one-match serverless timing probe, match 3788746, deployed wheel 0.5.43)

Authoritative per-step timing on **real serverless compute, full data** (notebook probe returning exit-JSON; run `817166962154129`):

```
read_spadl_actions  toPandas : 21.4s
has_360 count                :  2.3s
read_statsbomb_360  toPandas :  1.9s     ← scans are NOT the bottleneck
build_snapshots (iterrows)   : 147.7s    ← DOMINANT (52%); 56,207 raw rows -> 32,633 snapshots
enrich_sb360_match           : 109.1s    ← 2nd (38%); includes ghost-GK in the deployed wheel
build_output / createDF      : ~1.3s
measured total               : ~284s
```

**Corrected hypotheses (do not re-litigate these — they were measured):**
- ❌ **Per-match table SCAN is NOT the cost** (`statsbomb_360` read = 1.9s). **Clustering bronze by `match_id` is NOT needed** and is out of scope.
- ❌ **Ghost-GK is NOT 83% of the cost** — that figure came from a *contaminated local fixture* (every player mis-flagged GK because the SQL Statement Execution API serializes booleans as the strings `'true'`/`'false'`, and `bool('false') is True`). On clean data ghost-GK is ~8s locally / part of the 109s enrich. It is dropped for **output-quality** reasons (velocity-degenerate → ~7-14% coverage, ~85% clamped off-pitch), not speed.
- ✅ **`build_snapshots` (147s `iterrows`+`json.loads`) is the real hot spot** — vectorizing it is the single biggest, cheapest win and was not suspected before the probe.

**Local-fixture gotcha (must be respected by anyone re-measuring):** the Databricks **Statement Execution API returns only the FIRST result chunk** (`statement_execution.execute_statement(...).result.data_array`). A `SELECT * FROM bronze.statsbomb_360 WHERE match_id=...` pulled only **19,292 of 56,207** rows (~34%). Always pull full data via `toPandas()` on a Spark session (a one-off serverless notebook), or page the Statement API via `result.next_chunk_*`. Every local sb360 number measured via the Statement API is on partial data.

**Reference probe machinery (reusable):** `tmp/submit_sb360_timing_nb.py` (submits a one-off serverless notebook task reusing the live job's `analytics` environment; returns timings via `dbutils.notebook.exit(json)`), `tmp/poll_timing_nb.py` (polls + reads `notebook_output.result`). Re-run post-change to confirm the win.

---

## File structure

- **Create:** `src/analytics/action_context/sb360_snapshots.py` — pure (no pyspark dep) `build_sb360_snapshots(actions_df, sb360_raw_df)` **and** `resolve_home_team_id(actions_df)`. Both live in the core so the Spark path AND the hexagon import one impl (dependency direction: the hexagon must never import from `ingestion/`).
- **Modify:** `src/ingestion/action_context.py` — replace the `_run_sb360_enrichment` loop with the helper; add `_make_sb360_cogroup_udf` + `_process_statsbomb_matches`; route statsbomb out of the per-match drain.
- **Modify:** `src/analytics/action_context/enrich.py` — ghost-GK drop + voronoi add (**already done** on the working branch; this plan documents + tests it).
- **Modify:** `src/analytics/action_context/pipeline.py` (`enrich_batch` sb360 branch) + `src/analytics/action_context/local/parquet_sources.py` — make the hexagon take RAW freeze-frames (`sb360.parquet` = raw `statsbomb_360` rows) and build snapshots via the shared helper, so local mirrors production exactly.
- **Modify:** `src/ingestion/action_context_queue.py` (`DrainProcessor`) — stop enqueuing statsbomb as per-match drain units.
- **Create:** `docs/superpowers/adrs/ADR-058-sb360-distributed-and-enricher-tiering.md`.
- **Tests:** `src/tests/action_context/test_sb360_snapshots.py` (new), `src/tests/action_context/test_sb360_enricher_tiering.py` (new), updates to dispatch/discovery/port tests + the local fixture builder.

---

## Execution order (dependency-corrected)

Task numbers are topic groupings, NOT execution order. Execute in this order:
**Task 7 (fixture full-data fix) FIRST** → then Task 1-2 (helper) → Task 3 (tiering test, needs a full-data fixture) → Task 4 (cogroup) → Task 6 (hexagon lockstep) → Task 5 (drain-exit dispatch) → Task 8 (ADR + validation). **Rationale:** any sb360 fixture built before Task 7 is silently corrupted — the Statement-API boolean-string trap (`bool('false') is True`) flags every player GK/teammate, and the first-chunk truncation drops ~⅔ of freeze-frames. Tasks 3/4/6 all consume an sb360 fixture, so Task 7 must land first.

### Task 1: Vectorized snapshot helper (pure, tested)

**Files:**
- Create: `src/analytics/action_context/sb360_snapshots.py`
- Test: `src/tests/action_context/test_sb360_snapshots.py`

- [ ] **Step 1: Extract the REAL loop as the oracle (do not hand-rewrite).** Before writing the helper, lift the *current* `_run_sb360_enrichment` snapshot loop verbatim into a temporary private `_legacy_build_snapshots(actions_pdf, sb360_pdf)` in `action_context.py` (cut-paste, no edits — preserve `dict(zip(..., strict=True))`, the `all_teams` opp logic, the `json.loads` skips). The test imports THIS, so the helper is validated against the spec, not a re-spec. Delete `_legacy_build_snapshots` in Task 2 once the helper replaces the loop.

- [ ] **Step 2: Write the failing test** — equality vs the extracted real loop, malformed-row drops, AND a duplicate-`original_event_id` tie-break case (the MEDIUM gap the unique-id fixture would miss).

```python
# test_sb360_snapshots.py
import pandas as pd
from analytics.action_context.sb360_snapshots import build_sb360_snapshots
from ingestion.action_context import _legacy_build_snapshots  # the verbatim oracle (deleted after Task 2)

def _fixture():
    actions = pd.DataFrame({  # uuidA maps to TWO actions (10 then 12): loop's dict keeps LAST (12)
        "action_id":         [10, 12, 11],
        "original_event_id": ["uuidA", "uuidA", "uuidB"],
        "team_id":           [941, 941, 911],
    })
    sb360 = pd.DataFrame({  # two teams, a GK, a malformed + a single-value location, an unmapped event
        "id":        ["uuidA", "uuidA", "uuidB", "uuidB", "uuidZ"],
        "teammate":  [True,    False,   True,    False,   True],
        "keeper":    [False,   True,    False,   False,   False],
        "location":  ["[40.5, 30.2]", "[100.0, 34.0]", "[12.0, 8.0]", "bad", "[1,1]"],
    })
    return actions, sb360

def test_vectorized_matches_legacy_loop():
    actions, sb360 = _fixture()
    keys = ["action_id", "x", "y"]
    got = build_sb360_snapshots(actions, sb360).sort_values(keys).reset_index(drop=True)
    exp = _legacy_build_snapshots(actions, sb360).sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(got[exp.columns], exp, check_dtype=False)

def test_duplicate_event_keeps_last_action():   # tie-break pinned to the loop's dict-keeps-last
    actions, sb360 = _fixture()
    got = build_sb360_snapshots(actions, sb360)
    assert set(got["action_id"]) == {12, 11}     # uuidA -> 12 (last), NOT 10 (first)

def test_drops_unmapped_and_malformed():
    actions, sb360 = _fixture()
    got = build_sb360_snapshots(actions, sb360)
    assert len(got) == 3                           # uuidZ unmapped + the "bad" location dropped
```

- [ ] **Step 2b: Run it, verify it fails** — `uv run pytest src/tests/action_context/test_sb360_snapshots.py -v` → FAIL (helper module missing).

- [ ] **Step 3: Implement the vectorized helper** (no pyspark import).

```python
# src/analytics/action_context/sb360_snapshots.py
"""Vectorized SB360 freeze-frame -> snapshot conversion (pure pandas/numpy).

Replaces the per-row iterrows+json.loads loop that dominated sb360 wall-time (~147s/match
on serverless; ADR-058). Output schema is identical: action_id(int), team_id(str),
is_goalkeeper(bool), x(float), y(float). One impl shared by the Spark path and the hexagon.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def build_sb360_snapshots(actions_df: pd.DataFrame, sb360_raw_df: pd.DataFrame) -> pd.DataFrame:
    if sb360_raw_df.empty or actions_df.empty:
        return pd.DataFrame(columns=["action_id", "team_id", "is_goalkeeper", "x", "y"])

    ev = actions_df["original_event_id"].dropna()
    ev_to_action = pd.DataFrame({
        "id": ev.astype(str).to_numpy(),
        "action_id": actions_df.loc[ev.index, "action_id"].to_numpy(),
        "acting_team": actions_df.loc[ev.index, "team_id"].astype(str).to_numpy(),
    }).drop_duplicates("id", keep="last")   # MATCH the loop: dict(zip(...)) keeps the LAST action for a
    #                                          duplicated original_event_id (carry+pass decomposition etc.)

    teams = [str(t) for t in actions_df["team_id"].dropna().unique()]
    # opponent of the acting team (2-team match; first "other" id, mirroring the loop).
    opp_of = {t: next((o for o in teams if o != t), t) for t in teams}

    df = sb360_raw_df.copy()
    df["id"] = df["id"].astype(str)
    df = df.merge(ev_to_action, on="id", how="inner")          # drops unmapped events
    if df.empty:
        return pd.DataFrame(columns=["action_id", "team_id", "is_goalkeeper", "x", "y"])

    teammate = df["teammate"].astype(bool).to_numpy()
    acting = df["acting_team"].to_numpy()
    opponent = np.array([opp_of.get(t, t) for t in acting], dtype=object)
    team_id = np.where(teammate, acting, opponent)

    # location is a JSON-ish "[x, y]" STRING in bronze (DESCRIBE: location string). Vectorized parse;
    # malformed rows -> NaN -> dropped. (If a Spark ArrayType ever reaches here, coerce via .tolist().)
    loc = df["location"].astype(str).str.strip().str.strip("[]")
    xy = loc.str.split(",", n=1, expand=True)
    x = pd.to_numeric(xy[0], errors="coerce")
    y = pd.to_numeric(xy[1], errors="coerce") if xy.shape[1] > 1 else pd.Series(np.nan, index=df.index)

    out = pd.DataFrame({
        "action_id": df["action_id"].astype("int64").to_numpy(),
        "team_id": team_id,
        "is_goalkeeper": df["keeper"].astype(bool).to_numpy(),
        "x": x.to_numpy(dtype="float64"),
        "y": y.to_numpy(dtype="float64"),
    })
    return out[out["x"].notna() & out["y"].notna()].reset_index(drop=True)
```

- [ ] **Step 4: Run tests** → PASS. Run `uv run ruff check` + `uv run pyright src/analytics/action_context/sb360_snapshots.py`.

- [ ] **Step 5: Benchmark** (optional, recommended) — add a `pytest-benchmark` over a 56k-row synthetic frame asserting <5s (was ~147s). Commit.

---

### Task 2: Wire the helper into `_run_sb360_enrichment` (behavior-preserving)

**Files:** Modify: `src/ingestion/action_context.py` (the snapshot loop in `_run_sb360_enrichment`, ~lines 2155-2218)

- [ ] **Step 1:** Replace the entire `snapshots = []` … `freeze_frames = pd.DataFrame(snapshots)` block with:

```python
from analytics.action_context.sb360_snapshots import build_sb360_snapshots
freeze_frames = build_sb360_snapshots(actions_pdf, sb360_pdf)
if freeze_frames.empty:
    task_logger.warning("No valid snapshots for match %s — 0 AC rows", match_id)
    return actions_pdf.iloc[0:0]
```

- [ ] **Step 2:** Verify the existing sb360 tests still pass (`test_action_context_enrichment.py`, `test_parquet_sources.py`). The helper output is column-identical, so enrichment output is unchanged.
- [ ] **Step 3:** Commit.

---

### Task 3: Enricher tiering — ghost-GK out, voronoi in (DONE; add tests)

**Files:** `src/analytics/action_context/enrich.py` (**already edited** on branch), Test: `src/tests/action_context/test_sb360_enricher_tiering.py`

The `_enrich_sb360_match` edits are committed: `add_ghost_gk` removed; `pitch_control_at_target(..., method="voronoi")` added; `ghost_gk_method` no longer set. Validated locally: `pitch_control_at_target__voronoi` populates (30.4% = frame-linkage ceiling on the truncated fixture), ghost-GK columns NULL.

- [ ] **Step 1: Write a test** recomputing `_enrich_sb360_match` on a small real sb360 fixture (built from FULL data — see Task 7) asserting: `ghost_gk_x/ghost_gk_y/ghost_gk_density_spread/ghost_gk_method` are all-NaN; `pitch_control_at_target__voronoi` has ≥1 non-null; `pitch_control_at_target__spearman/fernandez_bornn` all-NaN.
- [ ] **Step 2:** Run → PASS. Commit.

---

### Task 4: Distributed `cogroup.applyInPandas` sb360 processor

**Files:** Modify: `src/ingestion/action_context.py`. Test: `src/tests/action_context/test_sb360_cogroup.py`

- [ ] **Step 1:** Add the UDF factory + driver (closure captures only picklable scalars — ADR-045):

```python
def _make_sb360_cogroup_udf(xt_grid_data, xt_l, xt_w):
    def _udf(actions_pdf: pd.DataFrame, sb360_pdf: pd.DataFrame) -> pd.DataFrame:
        from analytics.action_context.enrich import _enrich_sb360_match
        from analytics.action_context.pipeline import _reconstruct_xt
        from analytics.action_context.sb360_snapshots import build_sb360_snapshots, resolve_home_team_id
        if actions_pdf.empty:
            return _empty_result_pdf()                         # 0-row, _get_result_schema() columns
        # DETERMINISM: cogroup gives NO row-order guarantee. Sort by action_id so the dup-event
        # keep="last" tie-break (and any .iloc[0]) is reproducible run-to-run. The legacy path
        # must apply the SAME sort before build_sb360_snapshots.
        actions_pdf = actions_pdf.sort_values("action_id").reset_index(drop=True)
        match_id = str(actions_pdf["match_id_native"].iloc[0])
        frames = build_sb360_snapshots(actions_pdf, sb360_pdf)
        if frames.empty:
            return _empty_result_pdf()
        home = resolve_home_team_id(actions_pdf)   # shared CORE resolver (Step 1b) — NOT unique()[0]
        result = _enrich_sb360_match(actions_pdf, frames, home, _reconstruct_xt(xt_grid_data, xt_l, xt_w))
        return _build_output(result, match_id_native=match_id, data_source="statsbomb")
    return _udf
```

- [ ] **Step 1b: `home_team_id` — a CORRECTNESS fix, not just determinism (HIGH/ELEVATE).** `str(unique()[0])` returns an **arbitrary** team (often the AWAY team) → production sb360 orientation is **systematically wrong for ~half of matches**: `compute_team_shape`'s "deepest line nearest the defended goal", `add_defensive_line`, `add_line_break(ward)`, `add_shape_graph`, `add_gk_influence` (enrich.py:530-576). The real home id (`home_team_id_native`) is the proven production pattern — `identifiers.py:78` documents it, `spadl_conversion.py:277` populates it `str(home_team_id)` for all providers, and the tracking driver already uses the exact form `str(events_pdf["home_team_id_native"].dropna().iloc[0])` (action_context.py:1462). So make it the **confident primary path** (no tentative hedge). Place the resolver in the **analytics core** (`sb360_snapshots.py`) so BOTH the prod UDF and the hexagon import one impl — putting it in `ingestion/` would force the hexagon to import from ingestion, violating the dependency-direction rule (the same reason `build_sb360_snapshots` lives in the core).

```python
# in src/analytics/action_context/sb360_snapshots.py (core — no pyspark)
def resolve_home_team_id(actions_pdf) -> str:
    """Home team for sb360 orientation. home_team_id_native is the real home id (LL2 Path B,
    identifiers.py:78; populated for all providers, spadl_conversion.py:277) — same form the tracking
    driver uses (action_context.py:1462). Sorted-unique fallback is deterministic, orientation-only."""
    if "home_team_id_native" in actions_pdf.columns:
        h = actions_pdf["home_team_id_native"].dropna()
        if not h.empty:
            return str(h.iloc[0])
    teams = sorted(str(t) for t in actions_pdf["team_id"].dropna().unique())
    return teams[0] if teams else "unknown"
```

  - **Genuine open item:** verify `home_team_id_native` is populated for **statsbomb** on `bronze.spadl_actions` (live `SELECT`). If so, the fallback is dead code on the statsbomb path.
  - **Consequence (must be captured — Task 8 + ADR-058):** switching from arbitrary-team to real-home **re-materializes orientation-aware columns for every previously-processed sb360 match**. This is a **SECOND, independent source of golden/data drift** beyond the ghost-GK/voronoi tiering change. Both shift values; the golden regen must account for both.
  - Update the legacy `_run_sb360_enrichment` to use the same `resolve_home_team_id` + the same `sort_values("action_id")`, so the parity test (Step 3) compares apples to apples.

```python
def _canon_key(col):
    # ADR-019: canonicalize ids on EVERY join side exactly as _find_sb360_new_ids (action_context.py:580)
    # does — cast("long").cast("string") normalizes the "3788746.0" vs "3788746" float-format class.
    # A bare cast("string") on a double-typed id yields "3788746.0" → silently drops from .isin and
    # mis-aligns the cogroup. The IN-list `match_ids` are the "3788746"-style native _join_id strings.
    from pyspark.sql import functions as F
    return F.col(col).cast("long").cast("string")

def _process_statsbomb_matches(spark, catalog, schema, match_ids, xt_grid_data, xt_l, xt_w, logger):
    from pyspark.sql import functions as F
    if not match_ids:   # belt-and-suspenders: empty -> "match_id IN ()" is a SQL syntax error (the
        logger.info("No statsbomb sb360 matches to process — skipping")  # Task-5 skip-guard should pre-empt this
        return 0
    # CRITICAL: discovery (_find_sb360_new_ids + self._cap) is INCREMENTAL and CAPPED — match_ids is a
    # subset of unprocessed matches, NEVER the full statsbomb corpus. A full-partition
    # replace_where="data_source='statsbomb'" would DELETE every previously-processed match and rewrite
    # only this batch → silent data loss. Use the incremental match_id IN (...) list (the codebase
    # convention: formations_efpi.py:255, defcon_lite_360.py:434, import_obso_results.py:155).
    actions = (spark.table(f"{catalog}.bronze.spadl_actions")
               .filter((F.col("data_source") == "statsbomb") & _canon_key("match_id_native").isin(match_ids))
               .withColumn("_ck", _canon_key("match_id_native")))
    sb360 = (spark.table(f"{catalog}.bronze.statsbomb_360")
             .filter(_canon_key("match_id").isin(match_ids))
             .withColumn("_ck", _canon_key("match_id")))
    result_sdf = (actions.groupBy("_ck").cogroup(sb360.groupBy("_ck"))
                  .applyInPandas(_make_sb360_cogroup_udf(xt_grid_data, xt_l, xt_w), schema=_get_result_schema()))
    # build_output writes match_id = match_id_native (schema.py:356), identical to the _join_id strings in
    # match_ids → the IN-list is those exact native strings (no hashing; resolved open item #1).
    ids_sql = ", ".join(f"'{m}'" for m in match_ids)
    return write_delta_table(result_sdf, catalog, schema, _TABLE_NAME,
                             replace_where=f"data_source = 'statsbomb' AND match_id IN ({ids_sql})", logger=logger)
```

- [ ] **Step 2: VERIFY before the write** (resolved items first, then the genuinely-open ones):
  - ✅ RESOLVED from source: `match_id` written = **native** (`build_output` `out["match_id"] = match_id_native`, schema.py:356; per-match path `replace_where="match_id = '<native>'"`, action_context.py:2120). The IN-list uses those native strings. **No hashing.**
  - ✅ RESOLVED from source: canonicalize via `cast("long").cast("string")` on **all three** sides (both `_ck` + the `.isin` filter), matching `_find_sb360_new_ids` (action_context.py:580). The `_canon_key` helper above does this.
  - OPEN — `_empty_result_pdf()` must produce a 0-row frame whose columns match `_get_result_schema()`. Note `pipeline._empty_result()` (pipeline.py:212) returns a **dtype-less** (object-column) 0-row frame; a 0-row return to `applyInPandas` is usually tolerated (no rows to Arrow-convert), but the safest form builds the empty frame **with the schema's dtypes** to avoid the same float64↔BIGINT strictness. Add an offline column-name equality test; the dtype safety is only truly confirmed by the live probe (Step 2a.2).
  - OPEN — confirm the largest statsbomb sb360 match (actions + ~56k freeze rows) fits the **1 GB `applyInPandas` group cap** (action_context.py:641) under cogroup co-partitioning, and that AQE produces a sane partition count for a **cogroup** (it can't bytes-coalesce a cogroup the way it does `groupBy().applyInPandas`; may need an explicit `spark.sql.shuffle.partitions` or a repartition).

- [ ] **Step 2a: the float64↔BIGINT Arrow seam — split into an offline guard + a LIVE check (HIGH).** `build_output` leaves the ~80 tracking columns **all-NaN float64** and relies on the per-match path's `spark.createDataFrame(out_pdf, schema=_get_result_schema())` to coerce (action_context.py:2106-2114). `cogroup.applyInPandas(schema=...)` Arrow-converts the returned frame **directly, with NO createDataFrame coercion** — so an all-NaN-float64 column targeting a BIGINT/Long field hits the `convertToArrowArraySafely` seam (raises, or silently nulls). **This CANNOT be a local unit test** — pyspark is not installed locally; `conftest.py:23-46` injects `MagicMock()` for `pyspark.sql` and the suite has no SparkSession (`test_action_context_createdataframe_schema.py` is explicitly AST-based "because pyspark is not installed locally"). A "local Arrow round-trip test" would silently mock out and test nothing. Do BOTH of these instead:
  1. **Offline structural guard (AST, mirrors `test_action_context_createdataframe_schema.py`):** assert the cogroup `applyInPandas(...)` call passes `schema=_get_result_schema()`. That existing test already certifies the tracking `applyInPandas` path is safe *because* it passes that schema; the cogroup path using the identical `schema=` is the same proven mechanism, so the residual risk is bounded — not eliminated.
  2. **Live serverless check (fold into Task 8 Step 2's probe):** after the distributed write completes, `SELECT frame_id FROM ... WHERE data_source='statsbomb' AND match_id='<probe match>'` and assert a representative BIGINT column (e.g. `frame_id`) is **NULL, not 0/garbage**. This is the ONLY faithful test of the float64→BIGINT cast — the at-risk columns are exactly the ones the tracking path populates but sb360 leaves all-NaN-float64, which no offline test can exercise.

- [ ] **Step 3: Parity test (deterministic).** Compare `_process_statsbomb_matches([m])` vs the legacy driver path **both calling the edited `_enrich_sb360_match`** (so the ghost-GK/voronoi tiering is identical on both sides) and **both using the deterministic `home_team_id`** (see Task 4 Step 1b) — sort actions by `action_id` on both sides before `assert_frame_equal`. Without these, the shuffle makes the comparison flaky.
- [ ] **Step 4:** Commit.

---

### Task 5: Route statsbomb out of the per-match drain → single distributed job

**Files:** Modify: `src/ingestion/action_context.py` (`main`), `src/ingestion/action_context_queue.py` (`DrainProcessor.discover_units`/`process`), Terraform task wiring if a new entry point is added. Test: `src/tests/test_drain.py`, dispatch tests.

> This is the largest-surface task and aligns with the pre-existing investigation note `project_statsbomb_ac_commit_contention_plan` ("event-only providers exit the drain → single applyInPandas write"). It also removes the 8-worker per-match `replaceWhere` commit contention for statsbomb.

- [ ] **Step 1:** Decide the entry shape (recommend: a dedicated `compute_action_context_statsbomb` entry point / task that calls `_process_statsbomb_matches(all_pending_sb360_ids)`), and stop `DrainProcessor.discover_units` from enqueuing statsbomb units (the `_find_sb360_new_ids` branch, ~line 737). The 4 tracking providers keep the drain.
- [ ] **Step 2:** Update discovery so statsbomb pending-match resolution (`_find_sb360_new_ids`) feeds the new batch job instead of the queue.
- [ ] **Step 3:** Update `main()` dispatch (the `elif provider == "statsbomb"` branch, ~line 1114) to call `_process_statsbomb_matches` for the batch (or keep a per-match fallback path behind a flag for debugging).
- [ ] **Step 4: Skip-guard for the new batch entry point (MEDIUM).** Removing statsbomb from `discover_units` (action_context_queue.py:736-738) also drops it from the shared drain skip-guard `check()` count (line 744-752). The new batch entry point MUST add its own guard: resolve `_find_sb360_new_ids(...)` up front and **no-op (skip, log) when it is empty** — otherwise an empty run launches a cogroup over zero matches. Mirror the existing skip-guard's empty-handling.
- [ ] **Step 5:** Update tests (`test_drain.py`, `test_action_context_*` dispatch tests, the frames-required sentinel). Run the full action-context test subset.
- [ ] **Step 6:** Terraform: if a new task/entry point is added, wire it (env `analytics`, depends_on, the mega-job task list) and update `test_terraform_env_dep_parity` if needed. Commit.

---

### Task 6: Local hexagon lockstep — raw freeze-frames, snapshots built in-core

**Files:** Modify: `src/analytics/action_context/pipeline.py` (`enrich_batch` sb360 branch, ~line 430), `src/analytics/action_context/local/parquet_sources.py`, fixtures + `src/tests/action_context/test_ports.py`.

- [ ] **Step 1:** Change the sb360 contract so `sb360.parquet` holds **raw `statsbomb_360` rows** (id/teammate/keeper/location/...), and `enrich_batch` (or `run_work_unit`) calls `build_sb360_snapshots(actions, raw)` before `_enrich_sb360_match`. This makes the local path mirror production (which builds snapshots from raw), instead of expecting pre-built snapshots.
- [ ] **Step 1b: Resolve home through ONE shared path (MEDIUM — lockstep).** The hexagon sb360 branch currently passes `meta.home_team_id` (pipeline.py:437) while the prod UDF now uses `resolve_home_team_id(actions["home_team_id_native"])`. If `meta.home_team_id` and `actions["home_team_id_native"]` disagree for statsbomb, local ≠ prod on **every orientation-aware column** — defeating Task 6's goal. Fix: have the hexagon sb360 branch ALSO call `resolve_home_team_id(actions)` (the core function), so both paths derive home identically. (Alternative if `meta` must stay authoritative: add a test asserting `meta.home_team_id == resolve_home_team_id(actions)` for sb360 fixtures — but routing both through the one resolver is the cleaner lockstep.) Also apply the `sort_values("action_id")` in the hexagon path so the dup-event tie-break matches prod.
- [ ] **Step 2:** Update `ParquetFrameSource.frames` sb360 branch + the fixture layout doc.
- [ ] **Step 3:** Update `test_ports.py` and any sb360 fixtures. Run → PASS. Commit.

---

### Task 7: Fix the local fixture builder (full data, not Statement-API first chunk)

**Files:** the fixture/golden builders that pull sb360 (e.g. a `scripts/build_*` or the `tmp/pull_sb360_fixture.py` pattern).

- [ ] **Step 1:** Pull sb360 + actions via a **one-off serverless notebook `toPandas`** (reuse `tmp/submit_sb360_timing_nb.py` machinery) or page the Statement API (`result.next_chunk_*`) — NEVER trust `result.data_array` alone (first chunk only; truncated 56,207→19,292 on match 3788746). Parse `keeper`/`teammate` as real bools.
- [ ] **Step 2:** Regenerate any sb360 fixture/golden from full data. Commit.

---

### Task 8: ADR-058 + docs + validation

- [ ] **Step 1:** Write `docs/superpowers/adrs/ADR-058-sb360-distributed-and-enricher-tiering.md` (Nygard format): context = the measured timing breakdown + the two refuted hypotheses (scan, ghost-GK-83%) + the Statement-API truncation gotcha; decision = vectorize snapshots + ghost-GK exclusion + voronoi emission + cogroup distribution + statsbomb-exits-drain + **home_team_id via `home_team_id_native`** (was arbitrary `unique()[0]`). **Consequences (THREE value-drift sources for sb360, all re-materialize prior data):** (a) ghost_gk_* now NULL (Hyrum check: `fct_action_context` is a leaf mart); (b) `pitch_control_at_target__voronoi` now populated; (c) **orientation-aware columns shift** because home flips from arbitrary-team to real-home for ~half of matches (`team_shape`/`defensive_line`/`line_break(ward)`/`shape_graph`/`gk_influence`). Also note: the dup-`original_event_id` `keep="last"` is an **inherited arbitrary artifact** preserved for parity (Chesterton's fence — not a designed choice; a future pass might attach the freeze-frame to the event's primary action). No bronze clustering (scans measured fast). Cross-reference ADR-056/ADR-057/ADR-037/ADR-019/ADR-045. Update `enrich.py`'s "ADR-058" comment anchors.
- [ ] **Step 2:** Re-run the serverless timing probe (`tmp/submit_sb360_timing_nb.py`) post-change; assert `build_snapshots` < ~5s and total per-match wall is dominated by `enrich` (now ghost-GK-free). **Also fold in the live Arrow-seam check (Step 2a.2):** after a distributed write, `SELECT frame_id FROM fct/bronze sb360 row` and assert a BIGINT column is **NULL, not 0/garbage**. Record before/after in the ADR.
- [ ] **Step 3:** Run the full gates: `uv run ruff check src/ scripts/`, `uv run ruff format --check`, `uv run pyright src/`, `uv run pytest src/tests/ -v` (or the AC subset for iteration; FULL suite before merge — silly-kicks sentinels etc.).
- [ ] **Step 4:** Operational (user-checkpointed, NOT in this PR): once merged + deployed, re-run statsbomb sb360 AC via the new batch path; **regen the AC golden — BOTH the tiering (ghost-GK/voronoi) AND the home-orientation change shift values, plus the full-data fixture (Task 7) changes coverage**; rebuild `fct_action_context`.

---

## Self-review checklist (run before handing off)

1. **Spec coverage:** snapshot vectorization (T1-2), enricher tiering (T3), distributed cogroup (T4), drain-exit dispatch (T5), hexagon lockstep (T6), fixture-truncation fix (T7), ADR + validation (T8). ✔
2. **Placeholders:** none — vectorized helper + cogroup UDF + tests are full code.
3. **Type consistency:** `build_sb360_snapshots(actions_df, sb360_raw_df)` signature identical everywhere; cogroup UDF returns `_get_result_schema()`-shaped frames; `_empty_result_pdf()` flagged as a must-verify schema match.

## Verification items

**RESOLVED from source (2026-06-17 review — do not re-probe):**
- ✅ Written `match_id` = **native** (schema.py:356 `out["match_id"]=match_id_native`; per-match `replace_where="match_id='<native>'"`, action_context.py:2120). `replace_where` uses `match_id IN (<native ids>)`, scoped `data_source='statsbomb'` — **never** a full-partition replace (discovery is incremental+capped → full replace = data loss).
- ✅ Canonicalize ids via `cast("long").cast("string")` on all three join sides (`_canon_key`), matching `_find_sb360_new_ids` (action_context.py:580, ADR-019).

**STILL OPEN — resolve against live data / runtime (do not assume):**
- The float64↔BIGINT Arrow seam (Task 4 Step 2a): an offline AST guard (`schema=_get_result_schema()` is passed) bounds the risk but does NOT eliminate it — only the **live serverless BIGINT-NULL check** (Step 2a.2 / Task 8 Step 2) confirms cogroup's direct Arrow conversion (no `createDataFrame` coercion) doesn't raise/garbage the all-NaN-float64 columns. **This cannot be a local Spark unit test** (pyspark mocked, no SparkSession — conftest.py:23-46).
- `_empty_result_pdf()` column-name parity with `_get_result_schema()` offline; dtype safety only confirmed live (Task 4 Step 2).
- Largest statsbomb sb360 match fits the 1 GB `applyInPandas` group cap under cogroup; AQE partition count is sane for a cogroup (no bytes-coalesce — may need explicit repartition).
- `home_team_id_native` is populated for statsbomb on `bronze.spadl_actions` (else the sorted-unique fallback applies) — Task 4 Step 1b.
- Whether statsbomb-exits-drain needs a new Terraform task or can reuse `compute_action_context` with a provider branch (Task 5) — plus the new entry point's own skip-guard.
- Re-measure on FULL data (the local 30.4%/6.6%/715-ceiling numbers were on the truncated 34% fixture; Task 8 Step 2 re-probe is authoritative).
