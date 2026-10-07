# Tracking-Marts Executor Refactor + silly-kicks 4.128 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (INLINE execution — this repo forbids dispatched implementation subagents; read-only research subagents only). Steps use checkbox (`- [ ]`) syntax.

**Goal:** Move `compute_tracking_marts` + `compute_shot_freeze_frames` off the driver to the AC executor transport, adopt silly-kicks 4.128.0 (BREAKING), re-enable gkdv, and deliver the operator runbook to re-materialize the DAS/frame-geometry marts + retrain `xt_gk_v2`.

**Architecture:** Mirror `compute_action_context`'s ADR-045 transport — `repartition(_UDF_SHUFFLE_PARTITIONS, *keys).sortWithinPartitions(*keys).mapInPandas(mapper, schema)` via `_make_streaming_group_mapper` — grouping the unit-global tracking scorers at `(match_id, period)` grain (sub-unit `frame_batch_id` is infeasible for the global action→frame link). Fit the whole-unit group under the 1 GB UDF cap with sk 4.128 float32 frames + column-prune + sk `batch_size=` internal streaming + sequential scorers sharing one `PitchControlCache`; UC-Volume-Parquet spill fallback only where profiling busts budget.

**Tech Stack:** Python 3.10; PySpark (Databricks serverless); Delta; dbt; silly-kicks 4.128.0; pytest/pytest-benchmark; ruff/pyright; Terraform (env pins via `sync_tf_env_pins.py`).

**Spec:** `docs/superpowers/specs/2026-10-06-tracking-marts-executor-refactor-sk4128-design.md` (sha256 `72adeb83…`, **rev3 — re-review pending**; §4.2 transport re-architected to two-stage build-spill, rest unchanged from the approved rev2). The plan argues from the spec; executors read both. Every `R-*` ID below is the spec's.

## Global Constraints (every task's requirements implicitly include this)

- **Python 3.10** (`>=3.10,<3.11`); line length 120; Ruff E,W,F,I,N,UP,B,S,BLE,RUF; pyright basic over the CI target (**not** `pyright src/` — includes `hf_taipy_app/src/` + `sync_tf_env_pins` modules).
- **Serverless:** 16 GB driver; **1 GB UDF-group cap, ≤800 MB pandas-group target** (`pdf.memory_usage(deep=True).sum()`); no broadcast/cache/persist of large data; no internet / local-FS writes in UDFs; frozen dataclasses for UDF config; lazy closure capture (only small serializable scalars/structures cross into the UDF).
- **Canonical SPADL 105×68.** No secrets in code; HTTPS+verify=True+timeouts; no `eval`/`exec`/`pickle.loads`/`shell=True`.
- **BLE001:** no silent swallow; drain may swallow per-unit but `main_*_drain_worker` MUST call `raise_on_failed_units`. Structured JSON-line logs (source, row_count, timing).
- **Idempotent writes:** per-unit `replaceWhere` (`data_source='{p}' AND match_id='{m}' AND period_id={pd}`); pass `row_count` to `write_delta_table`. ADR-068 lifecycle events + ADR-087 circuit-breaker + in-preflight canary + 2700s watchdog preserved.
- **Commit discipline (HARD):** ALL tasks land as **one coherent, fully-tested commit**. **No per-task commits, no micro-commits.** The session STOPS before `git commit` and shows the diff; `git commit` / `git push` / `gh pr create` / `gh pr merge` each need separate explicit owner approval. Every job `run_now` / one-off `w.jobs.submit` is a separate approval. No wheel-consuming **production** job before post-merge `main` CI is green ([no-jobs-in-ci]).
- **Branch:** one feature branch off `origin/main`; `git fetch` + `git pull --ff-only` first; verify vs `origin/main`. **No worktrees.**

## Sequencing note (transport-neutrality vs the bump) — EXECUTION ORDER IS NOT TASK-NUMBER ORDER

Transport-neutrality (R-9 oracle) must isolate the dispatch change from the sk DAS value shift. Everything that OBSERVES 4.128 behaviour (float32 frames, native-DAS values/raises, the fit profile, goldens) MUST run after the bump. Explicit execution order:

```
T1 → T2 → T3 → T3b(exc. Step7) → T4 → T5   (build + prove transport on pinned sk 4.123: old-driver == new-executor, byte-identical)
   → T6.Step1 (LOCATE only, pre-bump)
   → T7                        (bump to 4.128 — DAS values + float32 now in effect)
   → T3b.Step7 → T6.Steps2-6 → T8 → T9   (4.128-DEPENDENT: batch_size default, float32 dtype asserts, native-DAS audit, two-stage fit profile)
   → T10 → T11 → T12 → T13        (Task 9b is SUBSUMED into T3/T3b — spill is now primary)
```

**Why:** float32 coords, the native-DAS value shift + new `ValueError`s, and the fit win are all sk-4.128 features. On pinned 4.123 the sk builder emits float64, so T6's float32 assertions (and T8/T9/T10's 4.128 observations) would be false-negative before the bump. T1-5 are version-independent transport logic (same sk both sides), so they run pre-bump against the 4.123 baseline. All within one branch / one final commit; intermediate validations do not commit.

## File Structure (created / modified)

- Create: `src/analytics/action_context/tracking_transport.py` (shared mapInPandas seam, if extraction needed — Task 2).
- Create: `src/analytics/action_context/tracking_frame_spill.py` (**rev3: PRIMARY** — build-once → UC-Volume-Parquet spill + stream-back; Task 3/3b. Was rev2's conditional Task-9b fallback; now the main transport because the six marts are different-cardinality.)
- Modify: `src/ingestion/tracking_marts_driver.py`, `tracking_marts_processor.py`, `tracking_marts_drain.py`, `shot_freeze_frames.py`, `src/analytics/action_context/action_context.py` (seam export only).
- Modify (dtype): the located per-provider frame-build site(s) (Task 6).
- Modify (bump): `pyproject.toml`, `uv.lock`, `terraform/modules/workflows/main.tf`, `scripts/submit_ac1_oneshot.py`, 21 `_REQUIRED_SK_MIN` files, `exec_visibility.py`, `action_context.py` (DAS string), gkdv docstrings, `src/tests/test_sk3_mig_b_orchestrator_invariants.py`, `src/tests/test_train_xt_gk_v2.py`, `src/tests/_topandas_exemptions.yml`.
- Docs: new ADR, ADR-082 amend, `AI_GOVERNANCE.md` + `docs/huggingface/model-cards/xt-gk-v2.md` ("retrain pending"), the operator runbook doc, `TODO.md`.
- Tests: new oracle + boundary fixtures under `src/tests/` (+ `src/tests/action_context/` style).

---

## Task 1: Branch + pinned-4.123 transport-neutrality baseline

**Files:**
- Create: `src/tests/tracking_marts/test_transport_oracle.py`
- Create (artifact): `src/tests/tracking_marts/_fixtures/transport_baseline/<provider>_<unit>.parquet` (captured old-scorer outputs)

**Interfaces — Produces:** a recorded per-provider small-unit baseline of the CURRENT scorer outputs on sk 4.123, consumed by Tasks 3 & 5 to assert transport-neutrality.

- [ ] **Step 1:** `git fetch origin && git pull --ff-only origin main`; confirm `git rev-parse HEAD == origin/main`; `git switch -c feat/tracking-marts-executor-sk4128`.
- [ ] **Step 2:** Pick one SMALL representative unit per provider (metrica tiny half; a short skillcorner half; a truncated/synthetic gradientsports + idsse unit that fits the current driver path). Record the exact `(provider, match_id, period)` tuples in the test module as constants.
- [ ] **Step 3 (capture baseline):** run the CURRENT (pre-refactor) `TrackingMartsProcessor.process` on each unit against sk 4.123 (the installed pin) and write each scorer's output DataFrame to the `transport_baseline/` fixture (parquet). This is the frozen oracle. Record how it was produced in a header comment (sk version, HEAD).
- [ ] **Step 4:** Write `test_transport_oracle.py::test_baseline_fixtures_present` asserting every expected baseline parquet exists + non-empty (guards against a silently missing capture).
- [ ] **Step 5:** Run `uv run pytest src/tests/tracking_marts/test_transport_oracle.py -q` — expect PASS (fixtures present). The equivalence assertions are added in Task 3/5.

---

## Task 2: Shared transport seam

**Files:**
- Modify: `src/ingestion/action_context.py` (L343-391 `_make_streaming_group_mapper`, L87 `_UDF_SHUFFLE_PARTITIONS`)
- Create (if needed): `src/analytics/action_context/tracking_transport.py`
- Test: `src/tests/action_context/test_tracking_transport.py`

**Interfaces — Produces:** `make_streaming_group_mapper(udf_fn, key_cols)` and `UDF_SHUFFLE_PARTITIONS` importable by both `action_context.py` and the tracking-marts/shot_freeze drains, with identical carry/flush semantics.

- [ ] **Step 1 (investigate — record result):** read `action_context.py:343-391` + L87; determine whether `_make_streaming_group_mapper` closes over any AC-only state. Run: `grep -nE "_make_streaming_group_mapper|_UDF_SHUFFLE_PARTITIONS" src/ingestion/action_context.py`. Record: is it already import-safe for a second consumer, or must it be promoted?
- [ ] **Step 2 (test-first):** write `test_tracking_transport.py::test_streaming_group_mapper_carry_flush` — feed an iterator of Arrow-sized chunks that split a group across chunks; assert each `udf_fn` call receives exactly one whole group's rows (compare to a `groupby` oracle over the concatenated input).
- [ ] **Step 3:** run it — expect FAIL (import error / not yet shared).
- [ ] **Step 4:** if Step 1 found AC-only coupling, extract the mapper + partition constant into `tracking_transport.py` and re-export from `action_context.py` (no behavior change to AC — AC still imports the same symbol). Otherwise make the existing symbols importable (promote to module scope / `__all__`).
- [ ] **Step 5:** run `uv run pytest src/tests/action_context/test_tracking_transport.py -q` — expect PASS. Run the existing AC transport tests to prove no AC regression: `uv run pytest src/tests/action_context -q`.

---

## Task 3: compute_tracking_marts — Stage 1: build-once + spill to UC-Volume Parquet (rev3)

**Why two stages (rev3):** six marts of different cardinality can't share one `mapInPandas` schema, and per-mart rebuild = 6× the BUILD cost. So Stage 1 builds the oriented frame ONCE per unit and spills it; Stage 2 (Task 3b) scores each mart from the spill. See spec §4.2.

**Files:**
- Modify: `src/ingestion/tracking_marts_driver.py` (`_read_unit` L66-132 raw read stays Spark-DF; builder moves into the Stage-1 UDF), `tracking_marts_drain.py` (`_run_worker` L254-315 → two-stage dispatch)
- Create: `src/analytics/action_context/tracking_frame_spill.py` (`spill_built_frames(raw_sdf, xt, …) -> volume_path`; `stream_built_frames(volume_path, batch_size)`)
- Test: `src/tests/tracking_marts/test_build_spill.py`

**Interfaces — Consumes:** `make_streaming_group_mapper` + `UDF_SHUFFLE_PARTITIONS` (Task 2). **Produces:** per-unit built-frame Parquet under a UC Volume path keyed `(provider, match_id, period)`; `stream_built_frames` for Task 3b.

- [ ] **Step 1 (Stage-1 build UDF):** read a unit's RAW tracking rows as a Spark DF (reuse `_read_unit`'s `_*_TRACKING_SELECT_COLS` projections — but NO driver `toPandas`). Dispatch `raw_sdf.select(<raw-select>).repartition(UDF_SHUFFLE_PARTITIONS,"match_id","period").sortWithinPartitions("match_id","period",<frame_col>).mapInPandas(build_udf, built_schema)`. The UDF runs `build_unit_inputs` on the group's raw frames → emits the BUILT oriented frames (single homogeneous `built_schema`; record it as a constant + `StructType` factory). xt grid rides the closure.
- [ ] **Step 2 (raw-select — TMP-01a):** `select` exactly the columns `build_unit_inputs` needs as INPUT (the `_*_TRACKING_SELECT_COLS` set). Test `test_build_spill.py::test_raw_select_is_builder_input`.
- [ ] **Step 3 (built-frame prune — TMP-01b):** inside the UDF after the build, reduce the built frame to the UNION of columns the SIX scorers read, before spilling. **This is the ~1.27→0.64GB/800MB fit lever** (float32 + prune; `team_id` category). Test `test_build_spill.py::test_built_frame_prune_is_scorer_union` (union computed from the six scorer signatures).
- [ ] **Step 4 (frame ordering — TMP-03):** add the provider frame col (`frame`/`frame_num`) to `sortWithinPartitions`. Builders sort internally (verified: sk `silly_kicks/tracking/skillcorner.py:311` + `metrica.py:192` sort `["player_id","frame_id"]`; lakehouse `src/analytics/action_context/convert.py:72` sorts `["frame_id","is_ball"]` — NOT `src/ingestion/skillcorner.py:311`, an unrelated CLI); the in-group sort is the belt-and-suspenders guarantee for gradientsports/idsse.
- [ ] **Step 5 (test-first, build-spill round-trip — TM-PLAN-12):** `test_build_spill.py::test_spill_roundtrip[provider]` — on each Task-1 unit (local Spark, sk 4.123), the spilled Parquet read back == the in-UDF built frames, asserting BOTH: (a) **dtypes restored** — a Parquet/pyarrow→pandas round-trip can decode `category`→`object` and change int/float widths, so `stream_built_frames` must restore the built-frame dtype schema (float32 coords, `team_id` category, `player_id` Int64) and the test asserts `.dtypes` equality, not just values; (b) **intra-unit frame order** preserved (order-sensitive compare). Run — expect FAIL (spill module absent).
- [ ] **Step 6 (implement):** write `tracking_frame_spill.py`; wire the Stage-1 dispatch in `_run_worker`; the driver writes the `mapInPandas` output to the UC Volume Parquet (normal Spark write, partitioned by unit — NOT an in-UDF FS write). Cleanup the intermediate after Stage 2 (Task 3b) / overwrite per-unit partitions on re-run.
- [ ] **Step 7:** `uv run pytest src/tests/tracking_marts/test_build_spill.py -q` — expect PASS.

---

## Task 3b: compute_tracking_marts — Stage 2: per-mart scoring passes from the spill (rev3)

**Files:**
- Modify: `tracking_marts_processor.py` (`process` L225-327 → six per-mart Stage-2 UDFs; the driver-Spark-reads move out), `tracking_marts_drain.py` (Stage-2 dispatch)
- Test: `src/tests/tracking_marts/test_transport_oracle.py`, `src/tests/tracking_marts/test_executor_grain.py`

**Interfaces — Consumes:** `stream_built_frames` (Task 3), the Task 1 baseline. **Produces:** each mart's bronze table, per-unit `replaceWhere`, output == baseline on sk 4.123.

**Lifecycle mapping (TM-PLAN-10) — the unit-of-work boundary does NOT move.** `process(unit)` stays the per-unit boundary that `drain_worker` (`drain.py:254-315`) wraps, so ALL the drain's hardest-constraint machinery is UNCHANGED from today, per unit:
- The 7 passes (1 Stage-1 build-spill + 6 Stage-2 scores) run INSIDE `process(unit)` for that one unit. Per-unit mapInPandas = one `(match,period)` group = one executor task; this is what moves the unit's frames off the 16GB driver onto an executor (the OOM fix). Cross-unit parallelism stays the existing 8-worker fan-out (`assign_workers`), unchanged from today's per-worker sequential loop.
- **Unit "complete"** = all 7 passes succeeded (same as today: `process` returns the summed rows; `slice_completed`/ADR-068 events emitted per unit by `drain_worker` after `process(unit)` returns).
- **Mid-run mart failure** (marts 1-2 written, 3-6 not) = the whole unit fails: each mart pass runs in its own try/except that attributes the error, and `process` raises a COMBINED `RuntimeError` (the existing pattern, `processor.py:323-326`) → `drain_worker` rolls the unit forward via `raise_on_failed_units`. Partial per-mart writes are harmless — the next run's per-unit `replaceWhere` overwrites them.
- **Watchdog** (`SparkInterruptWatchdog`, `drain.py:292`) wraps the whole `process(unit)` (all 7 passes), exactly as it wraps `process` today. **Circuit-breaker** (ADR-087) trips on per-unit consecutive/rate failures — per unit, unchanged.
- **Spill cleanup** = a `finally` in `process(unit)` deletes that unit's UC-Volume Parquet after the 6 score passes (success OR failure); the Stage-1 write also overwrites the unit's partition on a re-run, so a crashed-before-cleanup unit self-heals.
Record this mapping in the ADR (§11).

- [ ] **Step 1 (driver-reads → closure — TMP-04 + TM-PLAN-14):** `process` currently does `_read_xg_preds` (`processor.py:265`) + `resolve_unit_meta` (`:282`) as driver Spark reads INSIDE scoring — illegal on an executor. Pre-load both on the driver into **per-unit** dicts; pass via the UDF closure alongside `actions`, `xt`, `access_tier`, the bundled `completion_model`, `comp_season` (all small, lazy-captured, keyed by `(match_id,period)` / `(provider,match_id)`). Add `test_executor_grain.py::test_closure_is_per_unit` asserting the captured xg_preds/meta are the single unit's, NOT a whole-corpus dict (guards a memory blowup in the closure). **TM-DELTA-03:** also confirm the bundled `completion_model` (loop-invariant, built once in `__init__`) is small enough to ride the closure — measure its serialized size; if large, broadcast or load-in-UDF instead of closure-capture.
- [ ] **Step 2 (UC-Volume read-path FIRST, via a spike — TM-PLAN-11 / TM-DELTA-01/02):** before writing the Stage-2 UDF, run a cheap serverless SPIKE (a one-off job reading a small `/Volumes/…` Parquet INSIDE a `mapInPandas` UDF — tie it to the TMP-05 pre-T1 job approval, already granted). ADR-012 documents only DRIVER-side volume I/O, so this is unconfirmed.
  - **If the spike PASSES:** the PREFERRED direct-read path — the UDF reads the unit's spill Parquet + streams via sk `batch_size` (bounded peak).
  - **If it FAILS:** fallback = built-frames as a Spark DF re-grouped by `(match,period)` — this materializes the WHOLE ~0.64 GB built-frame group in the UDF (no streaming), so per-mart peak = 0.64 GB + working set, measured against the **800 MB gate** (NOT the old 3.4 GB — only ~160 MB headroom; gkdv is the risk). R-4.2-b's fallback remedy (frame-window sub-batch for gkdv / sk `batch_size` for the unit-global scorers / else STOP) applies.
  - Record which path ships; it determines the UDF in Step 3 and what T9 Step 3 profiles.
- [ ] **Step 3 (per-mart Stage-2 UDF):** for each of the six marts, a `mapInPandas` keyed `(match_id,period)` whose GROUP is just the unit key; the UDF obtains the built frames by the Step-2 read-path (`stream_built_frames(<unit volume path>, batch_size)` or the Spark-DF re-group) + the closure inputs + scores that ONE mart (its existing single-output scorer); returns that mart's `StructType`; driver writes it with the per-unit `replaceWhere`. One `PitchControlCache()` per mart-pass. (gkdv is wired in Task 4.)
- [ ] **Step 4 (test-first, transport-neutrality per mart):** `test_transport_oracle.py::test_tracking_marts_transport_neutral[provider,mart]` — the Stage-1→Stage-2 path on each Task-1 unit (local Spark, sk 4.123) == the baseline parquet per mart (exact dtypes+values; `assert_frame_equal` after a KEY-column sort only — order-sensitive, do not mask a frame-order regression). Run — expect FAIL.
- [ ] **Step 5 (implement):** rewrite `process`/`_run_worker` to the Stage-2 per-mart dispatch; keep ADR-068 events, watchdog, circuit-breaker.
- [ ] **Step 6 (boundary fixtures):** `test_executor_grain.py::test_arrow_chunk_boundary` (Stage-1 group spanning the Arrow batch size, built output == single-group oracle, frame-order preserved) + `::test_empty_unit_short_circuit` (empty unit → both stages no-op, empty results).
- [ ] **Step 7 (batch_size default — TMP-02; RUN POST-T7 — TMP-07):** 4.128-dependent — defer to after Task 7. Confirm the scorers inherit sk's bounded DEFAULT (`value_off_ball_runs` `batch_size=_DEFAULT_OFFBALL_BATCH`, `_run_values.py:449`; `=None` `_run_values.py:437` → unbounded). Never pass `None`; confirm `compute_rest_defense`'s default is bounded.
- [ ] **Step 8:** `uv run pytest src/tests/tracking_marts -q` — expect PASS (neutrality per mart + ordering + boundary).

---

## Task 4: gkdv re-enable + pooling stage

**Files:**
- Modify: `tracking_marts_processor.py` (`GKDV_ENABLED` L119 `False→True`; wire `score_gkdv_unit` into the UDF body), `tracking_marts_drain.py` (`main_gkdv_pool`/`_pool_gkdv` L400-463)
- Modify: `docs/superpowers/adrs/ADR-082-*.md` (amend)
- Test: `src/tests/tracking_marts/test_gkdv_pool.py`

**Interfaces — Consumes:** the Task 3b Stage-2 per-mart dispatch. **Produces:** gkdv `gkdv_observations` written per unit + pooled by `_pool_gkdv`.

- [ ] **Step 1 (test-first, pool bound re-validation):** `test_gkdv_pool.py::test_pool_keepers_bound_on_live_shape` — build a realistic gkdv-ON `gkdv_observations` (a few-thousand narrow rows across ~84 games per the exemption claim); assert `_pool_gkdv`'s driver read stays within the documented bound (row count + `memory_usage(deep=True)` under a stated ceiling). This replaces the gkdv-OFF-era assumption with a live-shape check.
- [ ] **Step 2:** run — expect FAIL or trivially pass; if the realistic shape exceeds the old claim, STOP and surface (do not silently widen the bound).
- [ ] **Step 3 (implement):** set `GKDV_ENABLED = True`; add gkdv as a SIXTH Stage-2 per-mart pass (reads the spilled built frames via `stream_built_frames`, scores per-frame DAS actual+ghost, own `PitchControlCache`); confirm `tracking_marts_drain.py:453` activation derives from the single lever.
- [ ] **Step 4:** add `test_executor_grain.py::test_gkdv_keeper_removed_counterfactual` — a gkdv unit with a keeper-removed counterfactual frame scores DAS on explicit frame slices (not the frame-keyed cache; sk ADR-043).
- [ ] **Step 5:** amend `ADR-082` (gkdv un-gated; the pooling stage; the per-unit budget re-measured in Task 9 covers BOTH the per-mart memory AND the aggregate 7-pass dispatch/wall-clock, TM-PLAN-16).
- [ ] **Step 6:** `uv run pytest src/tests/tracking_marts/test_gkdv_pool.py src/tests/tracking_marts/test_executor_grain.py -q` — expect PASS.

---

## Task 5: compute_shot_freeze_frames transport refactor (+IDSSE, −Metrica)

**Files:**
- Modify: `src/ingestion/shot_freeze_frames.py` (`_process_match` def L698; per-period loop ~L789-807 with `toPandas` L792 → mapInPandas; `_DEFAULT_PROVIDERS` L106)
- Test: `src/tests/tracking_marts/test_shot_freeze_transport.py`

**Interfaces — Consumes:** `make_streaming_group_mapper`. **Produces:** executor-distributed shot-freeze covering GradientSports + SkillCorner + IDSSE.

- [ ] **Step 1 (test-first):** `test_shot_freeze_transport.py::test_shot_freeze_transport_neutral` — capture a current-path baseline for a small GS + SkillCorner unit (as Task 1), assert the new executor path matches on sk 4.123. Add `::test_default_providers` asserting `_DEFAULT_PROVIDERS == frozenset({"gradientsports","skillcorner","idsse"})` (no metrica).
- [ ] **Step 2:** run — expect FAIL.
- [ ] **Step 3 (implement):** replace the per-period `trk_period_pdf...toPandas()` (L792) with the mapInPandas transport over `(match_id, period)`; set `_DEFAULT_PROVIDERS` to add `idsse`, keep metrica OUT; record the Metrica exclusion rationale ("3 legacy matches, not quality data — owner decision 2026-10-06") in the module docstring (L17-42).
- [ ] **Step 4:** add an IDSSE small-unit neutrality case to the test (IDSSE now in scope).
- [ ] **Step 5:** `uv run pytest src/tests/tracking_marts/test_shot_freeze_transport.py -q` — expect PASS.

---

## Task 6: Frame dtypes — locate IDSSE build, assert float32 (R-6-a/b/c; HARD, not verify-only)

**Files:**
- Modify: the located per-provider float64 frame-build site(s) (unknown until Step 1)
- Test: `src/tests/tracking_marts/test_frame_dtypes.py`

**Interfaces — Produces:** every in-scope provider's oriented frame carries sk 4.128 dtypes (coords float32, `team_id` category, `player_id` Int64).

**Precondition (TM-PLAN-01): Steps 2-6 run AFTER Task 7 (the bump).** float32 is a 4.128 feature; on pinned 4.123 the sk builder emits float64, so the float32 assertions would fail for EVERY provider, not the PASS-inheriting / FAIL-IDSSE split. Only Step 1 (LOCATE) runs pre-bump.

- [ ] **Step 1 (LOCATE — reviewer-flagged, do not skip; pre-bump OK):** find where each provider's frames get their coord/id dtypes. Run: `grep -rnE "convert_to_frames|_AC_FRAME_COLUMNS|astype|float64|float32|\"x\"|x_centered" src/analytics/action_context/convert.py src/analytics/action_context/sk_frame_adapters.py src/analytics/action_context/pipeline.py src/ingestion/tracking_marts_driver.py`. Record, per provider (idsse/metrica/skillcorner/gradientsports), the exact build site and whether coords come out float32 (inherited from the sk builder) or float64 (hand-built). `convert.py:26` is GradientSports-only — the IDSSE build is elsewhere; find it.
- [ ] **Step 2 (test-first):** `test_frame_dtypes.py::test_oriented_frame_dtypes[provider]` — build one unit's oriented frames per in-scope provider and assert `x/y/z` dtype == `float32`, `team_id` == `category`, `player_id` == `Int64`.
- [ ] **Step 3:** run — expect PASS for providers inheriting sk dtypes, FAIL for any hand-built float64 path (likely IDSSE).
- [ ] **Step 4 (implement only where Step 3 failed):** at the located float64 site, cast coords `.astype("float32")`, `team_id` `.astype("category")` (static), leave `player_id` `Int64`. Any masked coord write-back uses `.astype(col.dtype)` (pandas-3 `LossySetitemError`). Do NOT category-cast `player_id` (post-build mutation; sk `schema.py:17-27`).
- [ ] **Step 5 (round-trip fixture):** `test_frame_dtypes.py::test_float32_frame_round_trips_scorers` — a post-cast frame runs through every scorer without `LossySetitemError` / new-category setitem and produces finite output.
- [ ] **Step 6:** `uv run pytest src/tests/tracking_marts/test_frame_dtypes.py -q` — expect PASS.

---

## Task 7: silly-kicks 4.128 bump (drop [das]) + sentinels + wheel

**Files:** `pyproject.toml` (L71, L150 comment), `uv.lock`, `terraform/modules/workflows/main.tf:1927`, `scripts/submit_ac1_oneshot.py:55`, 21 `_REQUIRED_SK_MIN` files (spec §5.2 list), `src/tests/test_sk3_mig_b_orchestrator_invariants.py` (L258/L367/L385/L390/L392), `src/tests/test_train_xt_gk_v2.py:95`, `exec_visibility.py` (L365 docstring, L402 probe, L450 sentinel), `src/ingestion/action_context.py:2152`, gkdv docstrings.

- [ ] **Step 1 (test-first, sentinels):** update `test_sk3_mig_b_orchestrator_invariants.py` `expected`/strings to `(4,128,0)` AND fix the stale docstring L367 `(4,121,0)→(4,128,0)`; update `test_train_xt_gk_v2.py:95`. Run `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py src/tests/test_train_xt_gk_v2.py -q` — expect FAIL (sentinels still 4,123,0).
- [ ] **Step 2 (pins):** edit `pyproject.toml:71` + comment L150 → `silly-kicks[ghost-gk,parse-dfl]>=4.128.0,<5` (drop `das`). Run `uv lock`. Run `uv run python scripts/sync_tf_env_pins.py` then `... --check` (regenerates `main.tf:1927`). Edit `scripts/submit_ac1_oneshot.py:55` by hand (not sync-managed) → drop `das`, 4.128.0. Confirm `uv.lock` L1696/L1722/L1817 extras no longer list `das` and sdist/wheel are 4.128.0.
- [ ] **Step 3 (sentinels):** bump all 21 `_REQUIRED_SK_MIN` to `(4, 128, 0)`. Verify: `grep -rnE "_REQUIRED_SK_MIN.*=.*\(4, *12[0-9]" src scripts --include=*.py | grep -v "(4, 128, 0)"` returns nothing.
- [ ] **Step 4 (das-drop audit, R-7-d):** remove `"accessible_space"` from the `exec_visibility.py:402` import-probe tuple (+ L365 docstring); drop/adjust the `action_context.py:2152` `"get_dangerous_accessible_space"` string; fix gkdv docstrings referencing the old extra. Grep `grep -rnE "accessible_space|\[das\]" src/ scripts/` — only intentional references (e.g. comments explaining the drop) remain.
- [ ] **Step 5 (wheel):** bump `pyproject.toml` wheel version; `uv run python scripts/bump_wheel.py`; `uv run python scripts/bump_wheel.py --check`.
- [ ] **Step 6:** `uv run pytest src/tests/test_sk3_mig_b_orchestrator_invariants.py src/tests/test_train_xt_gk_v2.py -q` — expect PASS. Then `uv run python scripts/sync_tf_env_pins.py --check` — expect clean.

---

## Task 8: Native DAS breaking audit (R-7-a/b)

**Files:** `src/ingestion/gkdv_writer.py`, any `restdefense`/`territory` DAS caller; Test: `src/tests/tracking_marts/test_native_das_raises.py`

**Precondition (TM-PLAN-01): run AFTER Task 7.** The native engine + its new `ValueError`s only exist at 4.128; Step 1 LOCATE may run pre-bump.

- [ ] **Step 1 (locate):** `grep -rnE "use_progress_bar|accessible_space|get_dangerous|\*\*kwargs" src/ingestion/gkdv_writer.py src/ingestion/restdefense_writer.py src/ingestion/territory_writer.py`. Record every DAS call site + whether it passes `use_progress_bar`/`**kwargs`.
- [ ] **Step 2 (strip):** remove `use_progress_bar=`/`**kwargs` passthrough at each site found.
- [ ] **Step 3 (test-first, new raises vs LIVE bronze):** `test_native_das_raises.py` — exercise each DAS caller on representative LIVE-shape bronze frames per provider; assert no unexpected `ValueError` escapes, and that a deliberately malformed frame DOES raise the native engine's new `ValueError` (red-green: the new raise path is reachable, not dead). Record any live frame that trips a new ValueError as a finding to surface (do not catch-and-continue).
- [ ] **Step 4:** `uv run pytest src/tests/tracking_marts/test_native_das_raises.py -q` — expect PASS.

---

## Task 9: R-4.2-a memory-profile gate (dev one-off; SEPARATE approval)

**Files:** `scripts/profile_tracking_marts_executor.py` (dev diagnostic, one-off `w.jobs.submit`); artifact `D:\Development\_reviews\2026-10-06-tracking-marts-fit-profile.md`

**Precondition (TM-PLAN-01): run AFTER Task 7** (float32 fit is a 4.128 effect) and requires a **built, published branch wheel** (the one-off job installs it from the branch).

> **Process flag (RESOLVED 2026-10-06, TMP-05):** owner allows the pre-merge fit profile as a sanctioned dev one-off `w.jobs.submit` on the branch wheel (its own separate approval), distinct from §8's post-merge production re-materialize. Proceed on that basis.

**rev3: profile BOTH stages** (the transport is now two-stage build-spill, spec §4.2).

- [ ] **Step 1 (non-vacuity — watch the NEW instrument bust):** run the Stage-1 build UDF once in a deliberately un-optimized config — **float64 + no column-prune + largest half** — with the in-UDF sampler (`memory_usage(deep=True).sum()` for built-frame pandas bytes + peak RSS). CONFIRM the sampler reports a bust (>800 MB). If it does NOT bust, the gate is vacuous — STOP and fix the instrument.
- [ ] **Step 2 (Stage-1 fit):** run the optimized Stage-1 build (float32 + built-frame prune + team_id category), per provider at its largest half. Record built-frame bytes + peak RSS. Acceptance ≤800 MB.
- [ ] **Step 3 (Stage-2 fit — the path ACTUALLY TAKEN, TM-DELTA-01):** run the per-mart scoring under whichever §12.1a path shipped (T3b Step 2): direct-read streaming OR the fallback whole-0.64GB-group. Profile **per mart incl. gkdv**, per provider at its largest half, gkdv on. Record the scoring-UDF peak RSS. Acceptance **≤800 MB** — gkdv under the fallback is the risk (0.64 GB + DAS working set).
- [ ] **Step 4 (route / fallback remedies — R-4.2-b):**
  - Stage-1 build busts ≤800 MB → sub-batch the build in frame-windows **WITH A HALO** (TM-PLAN-13/TM-DELTA-04: `build_unit_inputs`' Savitzky-Golay velocity is sequence-continuous — a naive split perturbs boundary velocity; the halo + a neutrality proof vs the whole-unit build are mandatory if this branch triggers). Risky branch — unlikely (~0.64 GB << 800 MB).
  - Stage-2 fallback busts a mart → gkdv: frame-window sub-batch (per-frame, no halo needed); off_ball/rest_defense: rely on sk `batch_size`. If a mart still busts → STOP, surface (do not scope-cut).
  - Record the per-provider Stage-1/Stage-2 peaks + §12.1a path + any sub-batching + non-vacuity evidence in the fit-profile artifact.
- [ ] **Step 5 (dispatch/wall-clock — TM-PLAN-16):** the per-unit topology is 7 Spark jobs/unit (1 build + 6 score) across the unit set — deliberate (keeps the `process(unit)` lifecycle boundary), a throughput (not correctness) trade vs today's 1 driver-pass/unit. Measure the AGGREGATE dispatch overhead + wall-clock of the 7 passes over a representative worker's unit set (not just per-mart memory); record it so the ADR-082 per-unit budget re-measure (Task 4) reflects the real 7-pass cost. If wall-clock is prohibitive, surface it (do not silently accept or re-architect) — the owner decides whether to revisit the per-unit vs per-worker-batch topology.

---

## Task 9b: (rev3) SUBSUMED into Task 3/3b

The UC-Volume-Parquet spill is no longer a conditional fallback — it is the PRIMARY transport (`tracking_frame_spill.py`, built + tested in Task 3/3b), because the six different-cardinality marts cannot share one `mapInPandas` schema. There is no separate conditional spill task. (The only remaining conditional is the Stage-1 build sub-batching in Task 9 Step 4, if a provider's build busts.)

---

## Task 10: Goldens regen + boundedness + full gates

**Files:** `src/tests/_topandas_exemptions.yml`, the mini-golden fixtures, workflow cards (gkdv/xt-gk-v2 if changed)

**Precondition (TM-PLAN-01): run AFTER Task 7** — the goldens regenerate against the 4.128 DAS value shift.

- [ ] **Step 1 (exemptions, R-4.1-a):** remove the `tracking_marts_driver.py / _read_unit` entry (yml L92-93). Confirm `tracking_marts_drain.py / _pool_gkdv` (L88-90) REMAINS. Confirm shot_freeze needs no entry. Run `uv run pytest src/tests/test_topandas_boundedness.py -q` — expect PASS with exactly that delta.
- [ ] **Step 2 (goldens, R-7-c):** regenerate BOTH mini-goldens (IDSSE 3-action + SB360 variant) against sk 4.128 (DAS value shift is intended). Record the regeneration (what moved, tied to the sk 4.128 parity numbers). `uv run pytest <mini-golden tests> -q` — expect PASS.
- [ ] **Step 3 (workflow cards):** if any workflow card changed (gkdv re-enable / xt-gk-v2), run the hidden 8th gate `uv run python <validate_workflow_cards>` — expect clean.
- [ ] **Step 4 (benchmark — TMP-06; distinct from T9):** this is the **CI in-process pytest-benchmark on hot-path FUNCTIONS** (the scorer/kernel functions, per the AGENTS critical-path rule), NOT T9's serverless executor-scale memory profile. Run the benchmark on representative-scale data; confirm no CI-caught time regression. (Invoke `mad-scientist-skills:measure-before-optimize` before touching any benchmarked function.)
- [ ] **Step 5 (all gates):** ruff check; ruff format --check; `lint-imports`; `bump_wheel.py --check`; `pip_audit_ignores --check`; pyright (CI target, not `src/`); **full** `uv run pytest` (no `--collect-only`, no subset) — capture exit code, never `| tail`.

---

## Task 11: Governance "retrain pending" doc edits (in-commit part of §4.6)

**Files:** `AI_GOVERNANCE.md` (§5 row 19), `docs/huggingface/model-cards/xt-gk-v2.md`, `wf-xt-gk-v2.yaml`; Test: the governance test.

- [ ] **Step 1:** edit the xt-gk-v2 model card + AI_GOVERNANCE.md §5 row 19 to note a **4.128 retrain is REQUIRED (pending)** — driven by the F1b `pressure_on_actor__andrienko_oval` vintage refresh. Add NO produced-model metrics and do NOT flip status to produced (those land post-merge, §8 / Task-12 runbook).
- [ ] **Step 2 (tolerance — A-consider):** run the governance test (`src/tests/test_ai_governance_md.py`) — expect PASS. If it enforces a fixed status vocabulary that rejects a "retrain pending" marker, express the pending state with an ALLOWED status value (or add the marker in a field the test does not pin) — do NOT weaken the test to accept arbitrary text. Record how "pending" is encoded.

---

## Task 12: ADR + operator runbook + TODO

**Files:** new ADR under `docs/superpowers/adrs/`, `ADR-082` amend (Task 4), the operator runbook doc, `TODO.md`

- [ ] **Step 1:** write the new ADR (Nygard): executor refactor (driver→mapInPandas, ADR-045 lineage), whole-unit grain + why sub-unit infeasible, Approach-1/profile-gate/Parquet-fallback + per-provider routes, `batch_size=` byte-identity reliance, `[das]` drop (native engine).
- [ ] **Step 2:** write the operator runbook (spec §8) as a doc: the ordered re-materialize (wipe → tracking_marts+AC drains → **retrain xt_gk_v2 on 4.128 corpus → promote champion → produced-model governance edits + governance-test re-run → re-run xt_gk_v2_writer** → dbt staging → `rederive_synced_marts.py` TRIGGERED / `refresh_synced_tables` SNAPSHOT → `fct_action_defensive --vars xg_v3_enabled:true` → parity vs regenerated goldens). Pin each of the 11 marts' synced mode (confirm at this step: `grep`/read the synced-table manifest).
- [ ] **Step 3:** update `TODO.md` in this branch (the shipping cycle). Verify `git status TODO.md` shows it staged + `bump_wheel.py --check` clean.

---

## Task 13: Final gate + STOP for approval

- [ ] **Step 1:** re-run the full gate set (Task 10 Step 5) clean; confirm the Task 9 fit-profile artifact exists + every provider routed within budget.
- [ ] **Step 2:** `git status` + `git diff --stat`; assemble the one coherent commit's file list.
- [ ] **Step 3:** **STOP.** Present the diff / file list to the owner. Do NOT `git commit`. Wait for explicit commit approval. (Then, separately: push approval, PR approval, merge approval; then post-merge CI; then the §8 operator runbook runs under their own approvals.)

---

## Self-Review (against the spec)

- **Spec coverage (rev3):** R-4.1-a→T3b/T10; R-4.2-a→T9(two-stage); R-4.2-b→T9 Step 4 (build sub-batch; 9b subsumed); R-4.3→T2; §4.2 two-stage build-spill→T3(Stage 1)+T3b(Stage 2); §4.4 gkdv+pool→T4; §4.5 shot_freeze→T5; R-6-a/b/c→T6; §5.1 pins + §5.2 sentinels + §5.3 wheel→T7; R-7-a/b→T8; R-7-c goldens→T10; R-7-d das-audit→T7; §4.6 governance split (in-commit)→T11, (post-merge)→T12 runbook; §8 runbook→T12; §9 oracle+build-spill+boundary+benchmark+baseline→T1/T3/T3b/T5/T10; §10 gates→T10/T13; §11 ADRs→T4/T12; §12 open items→investigation steps T2.1/T6.1/T8.1/T12.2 + T3b.3 (UC-Volume read-in-UDF); commit discipline→Global + T13.
- **No placeholders:** the "unlocated" items (IDSSE build T6.1; seam coupling T2.1; synced modes T12.2; DAS sites T8.1) are concrete investigation steps with exact greps + what to record, not deferred work.
- **Type/name consistency:** `make_streaming_group_mapper` / `UDF_SHUFFLE_PARTITIONS` used consistently T2→T3/T5; result-schema constant defined T3.1 and reused; `spill_unit_frames`/`stream_spilled_frames` (T9b) referenced only by the conditional route.
- **Single commit:** no per-task commit steps; the only commit is T13 after owner approval.

### Plan-review reconciliation (rev applied 2026-10-06, both sessions)
- **A / TM-PLAN-01** (4.128-dependent tasks ordered before the bump) → Sequencing note rewritten with explicit execution order; preconditions added to T6/T8/T9/T10.
- **A / TM-PLAN-02** (Approach-2 had no build task) → new conditional **Task 9b** (TDD) + File-Structure entry; T9 Step 3 routes to it; only ships if T9 busts.
- **B / TMP-01** (prune confusion) → T3 Step 2 split into raw-select (builder inputs) + in-UDF built-frame prune (scorer union = the 800MB term); tests split.
- **B / TMP-02** (`batch_size=` unwired) → T3 Step 7: it is the sk bounded DEFAULT (`_run_values.py:449`); never pass `None`; verify rest_defense's default.
- **B / TMP-03** (frame ordering lost) → T3 Step 3: add provider frame col to `sortWithinPartitions` + confirm builder internal sort (sk `silly_kicks/tracking/skillcorner.py:311` + `metrica.py:192`; lakehouse `convert.py:72`) + order-sensitive oracle.
- **A-r2 / TM-PLAN-09** (ambiguous cite) → T3 Step 4 cites now prefixed `silly_kicks/tracking/…` vs `src/…` to avoid the coincidental `skillcorner.py:311` collision.
- **rev3 architecture amendment (owner decision 2026-10-06, re-review pending):** implementation exposed that the six tracking-marts outputs are different-cardinality → cannot share one AC-style single-schema `mapInPandas`. Task 3 split into **T3 (Stage-1 build-once + spill to UC-Volume Parquet)** + **T3b (Stage-2 six per-mart scoring passes from the spill)**; `tracking_frame_spill.py` promoted from conditional Task-9b to primary; T9 profiles both stages; Task 9b subsumed; driver-Spark-reads (`_read_xg_preds`/`resolve_unit_meta`) moved to the UDF closure (T3b Step 1). Spec amended to rev3 §4.2 (sha `72adeb83`).
- **B / TMP-04** → T3 Step 1 names the per-unit actions/meta closure mechanism (AC `actions_records`).
- **B / TMP-05** → T9 process-flag: resolve the [no-jobs-in-ci] pre-merge-profile question BEFORE Task 1.
- **B / TMP-06** → T10 Step 4: CI function benchmark clarified as distinct from T9's executor-scale profile.
- Cite fixes: shot_freeze `_process_match` def L698 (T5).
