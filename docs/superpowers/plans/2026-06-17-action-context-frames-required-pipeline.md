# Action-Context Frames-Required Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make action-context a **frames-required** pipeline. Event-only matches do not exist as far as
action-context is concerned — they are out of scope, not "in scope but empty". Delete the `event_only` tier
end-to-end; the tier model collapses to `{tracking, sb360}`; discovery enqueues only frame-bearing units.

**Architecture:** Delete-and-depend (Chesterton's fence: the `event_only` tier existed ONLY to carry the
action-derived columns — `game_state`, the GK quartet, `defending_gk_player_id` — that the Kimball slimming
(ADR-056) moved to `fct_action_values`; `defending_gk_player_id` is confirmed present in `fct_action_values.sql`,
so SB/WS lose nothing). The change spans (a) the hexagonal core (`work_unit` tier enum + `pipeline.enrich_batch`
dispatch + `enrich`), (b) the production drain (`action_context._process_*` + discovery + `drain` cost map),
(c) the local ports (`parquet_sources`), and (d) tests + an ADR. "No tracking for this match" is expressed by
**row absence** (the LEFT JOIN from `fct_action_values`), never an empty row. Production `_process_*_match`
and hexagonal `run_work_unit`/`enrich_batch` are kept in lockstep by existing tests — change BOTH.

**Tech stack:** Python 3.10, PySpark (production drain), pandas (enrich core), pytest, ruff, pyright; dbt (marts).

**Decision:** ADR-057 (new) — extends ADR-039 (SB360 coverage) and ADR-056 (AC Kimball slim).

**Not in scope / separate work:** The 10 currently-failing suite tests (SPADL/AC schema-parity, live-DDL,
vaep) are PRE-EXISTING cycle drift from the gk_dist + shot_goalmouth + pitch_control-rename schema additions —
fixed by the already-listed bronze-migration + fixture/golden regen, NOT by this plan. Do not conflate.

**Owner policy:** feature work on the current branch `feat/silly-kicks-4-31-0-pitch-control-at-target`, NO
worktree; each task ends in a verify checkpoint + `git add` (staging). Commits are owner-approved separately.
RED-first = run + observe, not a commit.

**Commit granularity (review L2a) — OWNER DECISION: single commit.** This branch carries the 4.31.0-cycle
adoption + the silly-kicks **4.32.0** bump/rename + this frames-required pipeline change, committed together as
one cycle commit (owner chose this 2026-06-17, accepting the review/rollback trade-off — the recompute is one
window). The reviewer's split recommendation is noted and declined.

**Pin confirmation (review L2b):** the branch is *named* 4.31.0, but the pins are **silly-kicks 4.32.0**
(pyproject `>=4.32.0`, terraform `==4.32.0`, uv.lock `4.32.0` installed + verified, all 7 `_REQUIRED_SK_MIN`).
The `pitch_control_at_action→pitch_control_at_target` FUNCTION rename and the `add_gk_distribution_metrics`
mutation fix are 4.32.0 changes (4.31.0 only renamed the column `at_ball→at_target`). Branch name is stale but
harmless; not worth a mid-flight rename.

---

## File structure

| Path | Responsibility | Action |
|------|----------------|--------|
| `src/analytics/action_context/work_unit.py` | tier model: drop `_EVENT_ONLY_PROVIDERS`; `provider_tier` → `{tracking, statsbomb}`; `FrameBundle.tier` doc `{tracking, sb360}` | Modify |
| `src/analytics/action_context/pipeline.py` | `enrich_batch`: remove `tier=="event_only"` arm (431-436); unknown tier raises | Modify |
| `src/analytics/action_context/enrich.py` | delete `_enrich_event_only_match` (572-579); `_enrich_sb360_match` 0-frame path (513-514) → empty result (no row), not actions-only `out` | Modify |
| `src/ingestion/action_context.py` | discovery: drop wyscout, statsbomb discovery → frame-bearing semi-join `bronze.statsbomb_360`; drop `_find_event_only_new_ids`/`_is_event_only_provider`/`_EVENT_ONLY_PROVIDERS`(96)/`_ALL_PROVIDERS`(97) if unused; dispatch: drop wyscout branch (1114-1115); `_process_statsbomb_match` no-360 → 0 rows (no write); delete `_process_event_only_match` | Modify |
| `src/ingestion/action_context_queue.py` | **(Phase-0 gap)** `DrainProcessor.process` (300-302) — the LIVE worker-drain dispatch (ADR-037): drop the `wyscout → _process_event_only_match` branch (statsbomb stays); drop the `_process_event_only_match` import (266-271) | Modify |
| `scripts/extract_action_context_fixture.py` | **(Phase-0 gap, LOW)** drop wyscout from its `_EVENT_ONLY_PROVIDERS` (60) — dev fixture tool, no runtime path; wyscout AC fixtures are now meaningless | Modify |
| `src/analytics/action_context/drain.py` | `_TIER_COST_S`: drop `"event_only"` (23) | Modify |
| `src/analytics/action_context/local/parquet_sources.py` | drop event_only resolution (statsbomb w/o `sb360.parquet` → out of scope, not event_only) | Modify |
| `src/ingestion/action_context.py` (import line 29) | drop `_enrich_event_only_match` import | Modify |
| `src/tests/action_context/test_work_unit.py` + `test_pipeline_dispatch.py` + `test_parquet_sources.py` + `test_ports.py` + `test_action_context_enrichment.py` + `test_action_context_createdataframe_schema.py` + `test_workflow_dag_bronze_reads.py` | update/remove event_only assertions; add frames-required contract tests | Modify |
| `docs/superpowers/adrs/ADR-057-action-context-frames-required.md` | NEW ADR | Create |
| `CLAUDE.md` | note AC is frames-required (if AC scope is documented there) | Modify |

---

## Phase 0 — Chesterton's-fence usage sweep (before any deletion)

### Task 0.1: Confirm the deletion surface has no other consumers

- [ ] **Step 1: Grep every symbol slated for deletion.** Run and record the hit set:
```bash
uv run python - <<'PY'
import subprocess
for sym in ("_enrich_event_only_match","_process_event_only_match","_find_event_only_new_ids",
            "_is_event_only_provider","_EVENT_ONLY_PROVIDERS","event_only"):
    print("==",sym,"=="); subprocess.run(["git","grep","-n",sym,"--","src","scripts","dbt_project","docs"])
PY
```
Expected: hits ONLY in the files this plan lists. If a symbol is referenced somewhere unlisted (a script, a
mart, an app query), STOP and add it to the surface — do not delete a referenced symbol.
- [ ] **Step 2: Confirm ALL 7 columns the `event_only` tier carried are served by `fct_action_values`** (review
  M1 — enumerate the full set, not just one; a future ADR-056 edit dropping any of these would silently re-break
  the fence). Run:
```bash
for c in game_state gk_role gk_pass_length_m gk_pass_length_class is_launch gk_xt_delta defending_gk_player_id; do
  echo -n "$c: "; git grep -c "$c" dbt_project/models/marts/fct_action_values.sql || echo 0
done
```
Expected: every one ≥1 hit (the actions fact carries the full set). This is the fence proof that dropping
event-only AC rows loses nothing recoverable. If ANY is ZERO, STOP — the fence is not safe; raise with the owner.
- [ ] **Step 3: Hyrum's-law consumer check — confirm `fct_action_context` is LEFT-joined, never INNER, and no
  consumer reads the carried columns from AC for event-only providers** (review L3). The row-absence contract
  ("no tracking → no row") holds ONLY if every consumer (i) reads `game_state`/GK columns from `fct_action_values`,
  not `fct_action_context`, and (ii) LEFT-joins AC (an INNER join would now DROP the action rows entirely). Run:
```bash
git grep -nE "join.*fct_action_context|fct_action_context.*join" dbt_project hf_taipy_app src scripts
git grep -nl "fct_action_context" dbt_project hf_taipy_app src scripts
```
For each consumer: confirm the join is LEFT (or the consumer is tracking-only). Record the consumer list + join
types. If any INNER-joins AC or reads a carried column from AC expecting event-only rows, STOP and route that
consumer through this plan (it must switch to `fct_action_values`). Live dashboards/synced tables that read AC
directly count as consumers — check the synced-table set too.

---

## Phase 1 — Core hexagon: tier model `{tracking, sb360}`

### Task 1.1: `work_unit` — remove the event_only tier classification

**Files:** `src/analytics/action_context/work_unit.py`; Test: `src/tests/action_context/test_work_unit.py`

- [ ] **Step 1: Write/adjust the contract test (RED).** In `test_work_unit.py`, assert the new model:
```python
def test_provider_tier_has_no_event_only():
    # tracking providers classify as "tracking"; statsbomb defers to runtime sb360 resolution.
    assert provider_tier(WorkUnit(provider="idsse", match_id="m", period=1)) == "tracking"
    assert provider_tier(WorkUnit(provider="statsbomb", match_id="m")) == "statsbomb"

def test_provider_tier_rejects_non_frame_providers():
    # wyscout (and any event-only provider) no longer exists for action-context.
    with pytest.raises(ValueError, match="not an action-context provider"):
        provider_tier(WorkUnit(provider="wyscout", match_id="m"))
```
Remove any existing test asserting `provider_tier(... wyscout ...) == "event_only"`.
- [ ] **Step 2: Run → RED.** `uv run pytest src/tests/action_context/test_work_unit.py -q`. Expected: the
  new tests fail (wyscout still returns `"event_only"`; no raise).
- [ ] **Step 3: Implement with distinct Literal types** (review M2 — two separate vocabularies threaded as bare
  `str` is a footgun; `pyright` should enforce the static/runtime boundary). In `work_unit.py`: delete
  `_EVENT_ONLY_PROVIDERS` (line 20). Add the two Literal aliases + an explicit resolution function so a crossed
  static/runtime value is a type error, not a silent string:
```python
from typing import Literal

ProviderTier = Literal["tracking", "statsbomb"]   # static (provider_tier)
FrameTier = Literal["tracking", "sb360"]           # runtime (FrameBundle / enrich_batch)

def provider_tier(wu: WorkUnit) -> ProviderTier:
    """Static provider classification for the FRAMES-REQUIRED action-context pipeline.

    ``tracking`` (idsse/metrica/skillcorner/gradientsports) or ``statsbomb`` (resolved to the
    ``sb360`` FrameTier at runtime). Event-only providers do NOT exist for action-context
    (ADR-057) — they raise.
    """
    if wu.provider in _TRACKING_PROVIDERS:
        return "tracking"
    if wu.provider == "statsbomb":
        return "statsbomb"
    raise ValueError(f"{wu.provider!r} is not an action-context provider (frames-required; ADR-057)")

def resolve_frame_tier(pt: ProviderTier) -> FrameTier:
    """Map the static ProviderTier to the runtime FrameTier — THE single mapping site (review
    M-new-2: not dead documentation). Frames-required: an ENQUEUED statsbomb unit always has
    freeze-frames (discovery semi-joins statsbomb_360), so it is always ``sb360`` — there is no
    event-only runtime outcome."""
    return "tracking" if pt == "tracking" else "sb360"
```
  Retype `FrameBundle.tier: FrameTier` (and update its docstring, line 75, to ``tracking`` | ``sb360``).
- [ ] **Step 4: WIRE `resolve_frame_tier` at the real static→runtime boundary** (review M-new-2 — an unused
  helper drifts; pyright must catch a crossed value at the CALL SITE, not just at the runtime dispatch). The
  boundary is wherever a `FrameBundle` is constructed from a `WorkUnit` (the `FrameSource` implementations).
  Replace any hardcoded `tier="sb360"`/`tier="tracking"` literal in a `FrameSource` with
  `tier=resolve_frame_tier(provider_tier(wu))` so the helper IS the mapping (covered concretely in Task 3.1 for
  the local parquet source; apply the same to any production Spark `FrameSource`). This makes `resolve_frame_tier`
  live and single-source.
- [ ] **Step 5: Run → GREEN + pyright.** `uv run pytest src/tests/action_context/test_work_unit.py -q && uv run pyright src/analytics/action_context/work_unit.py`. Stage.

### Task 1.2: `pipeline.enrich_batch` — remove the event_only dispatch arm

**Files:** `src/analytics/action_context/pipeline.py:431-436`; Test: `src/tests/action_context/test_pipeline_dispatch.py`

- [ ] **Step 1: Adjust the dispatch test (RED).** In `test_pipeline_dispatch.py`, remove the event_only
  dispatch case; add:
```python
def test_enrich_batch_rejects_unknown_tier():
    with pytest.raises(ValueError, match="unknown action-context tier"):
        enrich_batch(tier="event_only", ...)  # use the module's existing minimal-args helper
```
- [ ] **Step 2: Run → RED.** `uv run pytest src/tests/action_context/test_pipeline_dispatch.py -q`.
- [ ] **Step 3: Implement as a TOTAL dispatch + retype the param `tier: FrameTier`** (review M3 + M-new-2 —
  typing the param makes passing the static `"statsbomb"` a pyright error at the call site, not just a runtime
  raise). Import `FrameTier` from `work_unit`; signature becomes `def enrich_batch(..., tier: FrameTier, ...)`.
  Delete the `if tier == "event_only":` block (431-436), and restructure the tail so every tier is explicit and
  an unknown one raises:
```python
    if tier == "sb360":
        ...  # existing sb360 body
        return build_output(result, native_match_id, provider)

    if tier != "tracking":
        raise ValueError(f"unknown action-context tier {tier!r} (frames-required; ADR-057)")

    # ── tracking tier (per-frame-batch) ──
    ...
```
  Update the docstring at 426-427 (drop the `event_only: frames_pdf is ignored` sentence).
- [ ] **Step 4: Run → GREEN.** Stage.

### Task 1.3: `enrich` — delete `_enrich_event_only_match`; sb360 0-frame → no rows

**Files:** `src/analytics/action_context/enrich.py:472-579`; Test: `src/tests/test_action_context_enrichment.py`

- [ ] **Step 1: Adjust tests (RED).** In `test_action_context_enrichment.py`, remove any test exercising
  `_enrich_event_only_match`. Add a guard test that a sb360 match whose freeze-frames convert to zero synthetic
  frames yields **zero rows**. The fixture MUST land on the `len(frames)==0` branch specifically (review
  L-new-2): use **non-empty** freeze-frames that `snapshot_to_tracking_frames` converts to zero frames (e.g.
  positionless/corrupt frames) — NOT trivially-empty freeze-frames, which would instead hit the
  `sb360_pdf.empty` branch in `_run_sb360_enrichment` (line 2134) and pass for the wrong reason:
```python
def test_sb360_zero_frames_yields_no_rows():
    actions = _minimal_sb360_actions()
    ff = _nonempty_freeze_frames_that_convert_to_zero_frames()  # corrupt/positionless, NOT empty
    frames, _ = snapshot_to_tracking_frames(ff, actions)
    assert len(ff) > 0 and len(frames) == 0, "fixture must hit the len(frames)==0 branch, not the empty-ff branch"
    out = _enrich_sb360_match(actions, ff, home_team_id="H", xt=_mock_xt())
    assert len(out) == 0
```
- [ ] **Step 2: Run → RED.** `uv run pytest src/tests/test_action_context_enrichment.py -q`.
- [ ] **Step 3: Implement.** Delete `_enrich_event_only_match` (572-579). In `_enrich_sb360_match`, replace
  the 0-frame fallback (513-514). The **pure core returns empty and does NOT log** (logging is a production-edge
  concern — the WARN lives in `_process_statsbomb_match`, Task 2.2, review H1). Note for the rationale: the
  current code returns `out` = actions + `add_game_state` + `add_pre_shot_gk_context` (review L1 — it is NOT
  "actions-only"; those columns live in `fct_action_values`, so the conclusion is unchanged, but the wording is
  corrected here and in the plan header):
```python
    # Frames-required (ADR-057): a sb360 match whose freeze-frames convert to ZERO synthetic
    # frames produces NO rows. The "0 frames despite having 360 data" anomaly is WARN-logged at
    # the production edge (_process_statsbomb_match); the pure core just returns empty.
    if len(frames) == 0:
        return out.iloc[0:0]
```
- [ ] **Step 4: Run → GREEN.** Stage.

---

## Phase 2 — Production drain + discovery

### Task 2.1: Discovery — drop wyscout; statsbomb → frame-bearing units only

**Files:** `src/ingestion/action_context.py` (discovery 690-738; helper 561-587; import 29; `_is_event_only_provider` 104); Test: a discovery unit test (extend the nearest existing discovery test module)

- [ ] **Step 1a: Write the discovery contract test (RED, fakes).** Assert `discover_units` emits NO wyscout
  units and that statsbomb units are exactly the unprocessed matches present in `bronze.statsbomb_360`:
```python
def test_discovery_excludes_wyscout_and_requires_sb360():
    units = guard.discover_units(fake_spark, "cat", "sch")
    assert all(u.provider != "wyscout" for u in units)
    sb = {u.match_id for u in units if u.provider == "statsbomb"}
    assert sb == {"sb_match_with_360"}  # not the statsbomb match lacking 360
```
- [ ] **Step 1b: Write a REAL-DTYPE SET-EQUALITY id-join probe (RED→GREEN guard, review H2 + M-new-1).** A
  fakes-only test cannot catch an id-space/dtype mismatch — this is the ADR-019 `"366.0"` vs `"366"` class that
  all-NaN'd ~83% of GK features. A *membership* probe (`"x" in ids`) passes even if canonicalization drops 9 of
  10 eligible matches (mixed int/float/zero-padded storage). Use **set-equality against the known-eligible set**
  with **real SB match ids pulled from the actual bronze**, not a placeholder, on a fixture carrying the real
  dtypes of `spadl_actions.match_id_native` and `statsbomb_360.match_id`:
```python
def test_sb360_discovery_id_join_is_dtype_safe():
    # real dtypes (not fakes): a naive cast("string") yields "366.0" vs "366" and silently
    # drops matches; set-equality catches a PARTIAL drop, which membership would miss.
    eligible = {<real eligible sb360 ids from bronze, unprocessed>}
    assert set(_find_sb360_new_ids(spark_real_dtype_fixture, spadl_t, results_t, sb360_t)) == eligible, (
        "id-join dropped/added matches (ADR-019 dtype/format class)")
```
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement with CANONICAL id-equality** (review H2 — do not trust `cast("string")` on numeric
  ids; canonicalize both sides identically to match the process-path native contract). Prefer the shared
  identifier canonicalizer if one applies; otherwise normalize float-formatting by casting to `long` (`bigint`)
  THEN `string` on BOTH sides so `366.0 → 366 → "366"`:
```python
def _find_sb360_new_ids(spark, spadl_table, results_table, sb360_table) -> list[str]:
    """Unprocessed StatsBomb matches that HAVE freeze-frames (frames-required; ADR-057).

    statsbomb spadl matches ∩ statsbomb_360 match_ids \\ results. Event-only statsbomb matches
    (no 360) are out of action-context scope and never enqueued. The join key is CANONICALIZED
    identically on all three sides (cast long->string normalizes the "366.0" vs "366" float-format
    mismatch class — ADR-019); if statsbomb match ids are non-numeric, drop the long cast but keep
    one shared normalization. A real-dtype probe (Step 1b) guards this.
    """
    from pyspark.sql import functions as F  # noqa: N812
    def _key(col):  # one canonicalization, applied to every side
        return F.col(col).cast("long").cast("string").alias("_join_id")
    source = (spark.table(spadl_table).filter(F.col("data_source") == "statsbomb")
              .select(_key("match_id_native")).distinct())
    have_360 = spark.table(sb360_table).select(_key("match_id")).distinct()
    results = (spark.table(results_table).filter(F.col("data_source") == "statsbomb")
               .select(_key("match_id")).distinct())
    new = source.join(have_360, "_join_id", "inner").join(results, "_join_id", "left_anti")
    return [str(r["_join_id"]) for r in new.collect()]
```
  **MANDATORY precondition (review L-new-1 — this is what makes the long-cast safe, not a "before finalizing"
  afterthought):** FIRST run live `DESCRIBE bronze.statsbomb_360` + `DESCRIBE bronze.spadl_actions` (or read the
  source DDL) to confirm the `match_id`/`match_id_native` dtypes. **`cast("long")` NULLs a non-numeric id** →
  joins-on-NULL → silent empty (the same H2 class, relocated — L-new-1). If either id is a non-numeric/hash/
  zero-padded string, DROP the long cast and use the shared `src/shared/identifiers.py` canonicalizer (or a single
  shared `.cast("string")` if both are already canonical strings) instead. **Record the actual dtypes in this plan
  and in ADR-057** so a future reader knows why the cast is shaped this way. The Step-1b set-equality probe is the
  backstop.
  Replace the `for prov in ("statsbomb", "wyscout"):` loop (731-734) with statsbomb-only frame-bearing discovery:
```python
        if self._selected("statsbomb"):
            ids = self._cap(_find_sb360_new_ids(
                spark, spadl_table, results_table, f"{catalog}.bronze.statsbomb_360"))
            units += [WorkUnit(provider="statsbomb", match_id=mid) for mid in ids]
```
  Delete `_find_event_only_new_ids` (561-587) and `_is_event_only_provider` (104) **iff** Phase-0 Step 1 showed
  no other consumers (if a consumer exists, route it through this plan instead). Drop the
  `_enrich_event_only_match` import (line 29).
- [ ] **Step 4: Run → GREEN.** Stage.

### Task 2.2: Dispatch + statsbomb processor — no event-only write path

**Files:** `src/ingestion/action_context.py` (dispatch 1110-1117; `_process_statsbomb_match` 2037-2110; `_run_sb360_enrichment` 2135-2137; `_process_event_only_match` 2210-2240)

- [ ] **Step 1: Adjust the processor test (RED).** Where `_process_statsbomb_match` is tested, assert BOTH
  H1 cases: (a) a **no-360** statsbomb match returns 0 with no Delta write and an INFO "out of scope" log;
  (b) a **has-360-but-0-frames** match returns 0 AND emits a WARN containing "conversion failure" (use `caplog`).
  Remove any `_process_event_only_match` test.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement.**
  - Dispatch (1110-1117): delete the `elif provider == "wyscout":` branch (1114-1115). Keep statsbomb. Keep
    the `else: raise SystemExit(...)` (now also covers a stray wyscout unit — defensive).
  - `_process_statsbomb_match` no-360 branch (2086-2088): replace the event_only fallback with a 0-row return
    (out-of-scope is silent/INFO — discovery shouldn't enqueue it; this is defensive):
```python
    if not has_360:
        # Frames-required (ADR-057): discovery only enqueues sb360 matches, so this is a
        # defensive no-op (e.g. 360 deleted between discovery and processing). Out of scope → no rows.
        task_logger.info("StatsBomb match %s has no 360 data — out of action-context scope, skipping", match_id)
        return 0
```
  - After the sb360 enrichment, **WARN on the conversion-failure anomaly** (review H1 — `has_360` was true but
    the freeze-frames produced ZERO rows: corrupt/empty freeze-frames or a `snapshot_to_tracking_frames` bug — a
    real data-quality signal, NOT an out-of-scope match; per ADR-002 this must be ERROR/WARN-visible, not silent):
```python
    if has_360 and len(result_pdf) == 0:
        task_logger.warning(
            "StatsBomb match %s had 360 data but produced 0 frames — conversion failure, 0 AC rows written", match_id
        )
        return 0
```
  - `_run_sb360_enrichment` empty fallback (2135-2137): `return _enrich_event_only_match(...)` →
    `return actions_pdf.iloc[0:0]` (0 rows; the edge WARN above fires when this path is hit with `has_360`).
  - Delete `_process_event_only_match` (2210-2240).
  - **(Phase-0 gap) `action_context_queue.py::DrainProcessor.process` (the LIVE drain dispatch):** delete the
    `if unit.provider == "wyscout": return _process_event_only_match(...)` branch (300-302) and drop
    `_process_event_only_match` from the import (266-271). The trailing `raise ValueError(f"unknown provider...")`
    now also covers a stray wyscout unit (defensive — discovery no longer enqueues it).
  - **(Phase-0 gap) Provider-set helpers in `action_context.py` (96-105):** grep `_ALL_PROVIDERS` /
    `_is_event_only_provider` usages; `_EVENT_ONLY_PROVIDERS`(96)/`_ALL_PROVIDERS`(97) here are `{statsbomb,
    wyscout}` (distinct from `work_unit.py`'s `{wyscout}`). Remove `_is_event_only_provider` +
    `_EVENT_ONLY_PROVIDERS` if used only by it/tests; if `_ALL_PROVIDERS` is load-bearing (e.g. an arg validator),
    redefine it as `_TRACKING_PROVIDERS | {"statsbomb"}` (wyscout is no longer an AC provider). Update the
    `test_action_context_enrichment.py` `_is_event_only_provider` assertions (375-377) accordingly.
- [ ] **Step 3b: Verify the 0-row empty frame is never unioned before the len-check** (review L-new-4 — the
  empty `out.iloc[0:0]` has a PARTIAL column set: actions + game_state + gk_context, missing the frame-derived AC
  columns; it is harmless ONLY because `_process_statsbomb_match` short-circuits on `len(result_pdf)==0` and
  writes nothing). Trace `_process_statsbomb_match`: confirm there is NO intermediate `pd.concat`/union of the
  enrichment result before the `len(...)==0` check + `_build_output`. If a future assembler unions strands first,
  return an empty frame carrying the **canonical full AC schema** instead of the partial-column `iloc[0:0]`.
  Record the finding (expected: no pre-check union — the result flows straight to the len-check).
- [ ] **Step 4: Run → GREEN.** Stage.

### Task 2.3: `drain` — drop the event_only cost tier

**Files:** `src/analytics/action_context/drain.py:23,53-60`; Test: the drain cost test (if present)

- [ ] **Step 1: Adjust test (RED) if a `tier_cost_fn`/`_TIER_COST_S` test exists** — assert keys are
  `{"tracking","statsbomb"}`.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement.** `_TIER_COST_S = {"tracking": 1800.0, "statsbomb": 120.0}` (drop `"event_only"`).
  Update the docstring at 57 ("statsbomb between (sb360 subset)" → "statsbomb the sb360 tier; event-only
  providers are out of scope").
- [ ] **Step 4: Run → GREEN.** Stage.

---

## Phase 3 — Local ports + remaining test sweep

### Task 3.1: `parquet_sources` — drop the event_only resolution

**Files:** `src/analytics/action_context/local/parquet_sources.py:10-50`; Test: `src/tests/action_context/test_parquet_sources.py`

- [ ] **Step 1: Adjust test (RED).** Assert a statsbomb dir WITHOUT `sb360.parquet` is **out of scope**
  (the source raises / yields no work unit), not resolved to `event_only`; a wyscout dir is likewise out of scope.
- [ ] **Step 2: Run → RED.**
- [ ] **Step 3: Implement — and WIRE `resolve_frame_tier` here** (review M-new-2 — this is the local
  `FrameSource`'s static→runtime boundary, so it's the concrete call site that makes the helper live). Remove the
  `event_only` `FrameBundle` branch (47-50 fallback). statsbomb dir with `sb360.parquet` present →
  `FrameBundle(tier=resolve_frame_tier(provider_tier(wu)), frames=...)` (resolves to `"sb360"`); tracking dirs →
  same call (resolves to `"tracking"`); statsbomb dir WITHOUT `sb360.parquet` → raise a clear "out of
  action-context scope (no freeze-frames)" error (the local harness mirrors discovery, which would never enqueue
  it). Do NOT hardcode the tier string. Update the module docstring (10-13).
- [ ] **Step 4: Run → GREEN.** Stage.

### Task 3.2: Sweep the remaining event_only test references

**Files:** `src/tests/action_context/test_ports.py`, `src/tests/test_action_context_createdataframe_schema.py`,
`src/tests/test_workflow_dag_bronze_reads.py`

- [ ] **Step 1: Grep + triage.** `git grep -n "event_only\|_enrich_event_only\|wyscout" src/tests/action_context/test_ports.py src/tests/test_action_context_createdataframe_schema.py src/tests/test_workflow_dag_bronze_reads.py`
  For each hit: if it asserts the (now-deleted) event_only behavior, remove it; if it asserts wyscout is a
  bronze read for AC, update to reflect AC no longer reads wyscout for enrichment.
  NOTE: `test_workflow_dag_bronze_reads.py` may assert which bronze tables AC reads — wyscout's `spadl_actions`
  read for AC discovery is gone, but statsbomb still reads `spadl_actions` + `statsbomb_360`. Adjust precisely.
- [ ] **Step 2: Run the three files → GREEN.** Stage.

---

## Phase 4 — dbt marts + coverage

### Task 4.1: Verify no mart/coverage test assumes event-only provider rows

**Files:** `dbt_project/models/staging/action_context/stg_action_context__values.sql`,
`dbt_project/models/marts/fct_action_context.sql`, and any `test_*coverage*`/`test_*completeness*`

- [ ] **Step 1:** Grep the AC staging/mart for `wyscout`/`statsbomb`/`data_source` provider filters. Expected:
  they read whatever is in `bronze.spadl_action_context` with no provider allow/deny list → **no SQL change**
  (the bronze simply won't contain event-only rows after the recompute). Confirm and record.
- [ ] **Step 2: Add an EXPLICIT, REQUIRED frames-required coverage assertion** (review M4 — not "if present";
  this is the contract a consumer depends on). Add/repurpose a completeness test asserting the recomputed
  `fct_action_context` provider coverage is **exactly** `{idsse, metrica, skillcorner, gradientsports,
  statsbomb-with-360}` with **zero** wyscout / event-only rows:
```python
def test_fct_action_context_coverage_is_frames_required():
    ac = spark.table("...fct_action_context")
    providers = {r.data_source for r in ac.select("data_source").distinct().collect()}
    # forbidden-provider guard
    assert "wyscout" not in providers
    assert providers <= {"idsse", "metrica", "skillcorner", "gradientsports", "statsbomb"}
    # frames-required CONTRACT (review L-new-3 — implement, not comment): every statsbomb AC row
    # maps to a match present in bronze.statsbomb_360. An SB AC row with no 360 is the exact bug
    # this change prevents.
    sb_ac = {r.match_id for r in ac.filter("data_source = 'statsbomb'").select("match_id").distinct().collect()}
    sb_360 = {str(r.match_id) for r in spark.table("...bronze.statsbomb_360").select("match_id").distinct().collect()}
    assert sb_ac <= sb_360, f"statsbomb AC rows without freeze-frames (frames-required violation): {sb_ac - sb_360}"
```
  Name the test for what it asserts. (Run as an integration/live test in CI where the warehouse is available —
  mark accordingly; locally it may skip.) Also update any existing test that asserts all-6-provider AC coverage.
  Run → GREEN. Stage.

---

## Phase 5 — ADR + docs

### Task 5.1: ADR-057

**Files:** Create `docs/superpowers/adrs/ADR-057-action-context-frames-required.md`

- [ ] **Step 1:** Nygard format. Context: post-ADR-056 slimming, `fct_action_context` is a pure
  tracking-derived fact; the `event_only` tier carried only action-derived columns now owned by
  `fct_action_values` (`defending_gk_player_id` confirmed there). Decision: action-context is a frames-required
  pipeline; event-only matches are out of scope; tier model `{tracking, sb360}`; discovery enqueues only
  frame-bearing units (tracking per-period + statsbomb-with-360). Consequences: row-absence = "no tracking
  context" via the LEFT JOIN; SB/WS exit the AC drain entirely (also relieves the event-only commit-contention
  path); existing event-only rows in `bronze.spadl_action_context` are deleted on recompute (operational, no
  schema change). Extends ADR-039, ADR-056. Status → Accepted.
- [ ] **Step 2:** If AC scope is documented in `CLAUDE.md`, add the frames-required note. Stage.

---

## Phase 6 — Verification

### Task 6.1: Full local gate

- [ ] **Step 1:** `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/` → clean.
- [ ] **Step 2:** `uv run pyright src/` → 0 new errors.
- [ ] **Step 3:** AC slice: `uv run pytest src/tests/action_context/ src/tests/test_action_context_enrichment.py src/tests/test_action_context_createdataframe_schema.py src/tests/test_workflow_dag_bronze_reads.py -q` → GREEN.
- [ ] **Step 4:** Full suite (background, per the >30s rule): `uv run pytest src/tests/ -q --ignore=src/tests/test_migrate_synced_tables.py`.
  Expected residual failures: ONLY the 10 pre-existing schema-parity/live-DDL/vaep items (gk_dist + shot_goalmouth
  + pitch_control-rename schema additions awaiting the bronze migration + fixture/golden regen — separate work).
  Confirm NO NEW failures attributable to this plan. Record the delta.

---

## Phase 7 — Operational (USER-CHECKPOINTED — do not fire without explicit go-ahead)

- [ ] Existing event-only rows in `bronze.spadl_action_context` (wyscout + statsbomb-without-360) are deleted as
  part of the AC recompute wipe — they are now out of scope. (Current AC-1 is sparse/GS+idsse per memory, so this
  is near-empty today, but make the wipe explicit.)
- [ ] The full AC recompute includes `provider=statsbomb` (sb360 matches now in scope) alongside the 4 tracking
  providers; wyscout is NOT in the recompute. Re-derive `fct_action_context` strand-safe (ADR-043).

---

## Self-review

- **Spec coverage:** event_only deleted from work_unit (1.1), enrich_batch (1.2), enrich (1.3), discovery +
  dispatch + processors (2.1/2.2), drain cost (2.3), local sources (3.1), tests (3.2/4.1), docs (5.1). SB360-in
  needs no new wiring (verified live) — just statsbomb in recompute scope (Phase 7). Fence proof (0.1).
- **Placeholder scan:** every deletion has an exact file:line target; every new helper/guard has full code; the
  one "find the nearest test module" instruction (2.1/2.2) is a real triage, not a TODO — the test files are
  enumerated in Phase 0/file-structure.
- **Type consistency:** `provider_tier` → `{"tracking","statsbomb"}`; `FrameBundle.tier` / `enrich_batch` tier
  ∈ `{"tracking","sb360"}`; `_find_sb360_new_ids` returns `list[str]` like `_find_event_only_new_ids` did;
  `_TIER_COST_S` keys `{"tracking","statsbomb"}` match `provider_tier`'s range.
- **Hexagonal/e2e:** the tier enum lives in the core; production (`_process_*`) and the hexagonal mirror
  (`enrich_batch`/`run_work_unit`) are changed together (lockstep tests enforce parity); e2e is covered by the
  discovery contract test (no wyscout, statsbomb-360-only) + the sb360-zero-frames no-rows test.
