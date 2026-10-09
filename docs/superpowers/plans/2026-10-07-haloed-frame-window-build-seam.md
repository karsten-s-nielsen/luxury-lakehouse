# Haloed Frame-Window Build Seam — Implementation Plan

> **For agentic workers (THIS repo overrides the skill defaults):** Execute **INLINE** (not subagent-driven) — [no-subagents-for-implementation]. **ONE coherent, fully-tested commit for the whole seam feature** at the T10 gate (NOT per-task commits) — [no-micro-commits]. Per-task "Step: checkpoint" means **green + staged, NO commit**. `git commit`/`push`/`gh pr create`/`merge` each need separate explicit approval. Steps use `- [ ]` for tracking.

## Execution order & commit model (rev2 — folds plan reviews A+B: HFW-PLAN-01/02, TMP-H-01/02/03)

**ONE commit (TMP-H-01 / HFW-PLAN-01).** The seam + its three adoptions + AC goldens + ADR are ONE complete, interdependent change (T1 `halo_frames` / T2 `build_windowed` / T3 Spark transform land unwired until T4+ — fragments, not rollback targets). Build across all tasks → **one coherent fully-tested commit, gated once at T10 STOP**. Every per-task "Step N: checkpoint" below is **green-and-staged, NOT a commit**. (Matches sibling cycles tracking-marts/TF-58 = one commit. A small reasoned split is permitted ONLY if the owner asks; default = one.)

**Execution order ≠ task-number order — GATES RUN FIRST (TMP-H-02/03 / HFW-PLAN-02):**
1. **T1, T2, T3** — pure seam + Spark transform (no site wired yet).
2. **T6 convert-feasibility gate** — run BEFORE any adoption (spec §4.1a "gate BEFORE adoption"). A provider whose convert is whole-unit is EXCLUDED → changes what T4/T5/T7 wire. Covers ALL of idsse/metrica/skillcorner/GS (AC runs all — TMP-H-04/HFW-PLAN-04; idsse established in spec §4.1a).
3. **T8 residual measurement — PRE-MERGE branch-wheel diagnostic** (the T9 fit-profile precedent: an owner-allowed dev `jobs.submit` on the branch wheel; separate approval). Produces the per-provider `frac_affected` disposition that DECIDES which goldens the single commit carries. Must precede the goldens (T9) — a post-merge measurement cannot gate a pre-merge commit (TMP-H-03 / HFW-PLAN-02).
4. **T4, T5, T7** — adopt the seam (tracking-marts, shot_freeze, AC) now that the gate + disposition are known.
5. **T9 goldens** — AC goldens regen (value-change, known); tracking-marts/shot_freeze goldens regen ONLY for providers T8 flagged `frac_affected>0` (conservative rule). All in the one commit.
6. **T10 STOP** — the single commit gate.

Build-wheel for the T8 branch-wheel diagnostic + the T9 canary = `scripts/bump_wheel.py` on a dev version, deploy to the libs UC Volume (owner-allowed pre-merge dev one-off, fit-profile precedent); NOT the production bump (that lands in the commit).

**Goal:** A reusable, memory-bounded, correctness-preserving distributed frame-build seam (haloed frame-window partitioning) that bounds the serverless UDF-worker peak by WINDOW size (not unit size), adopted at tracking-marts Stage-1 + shot_freeze + the AC drain.

**Architecture:** Per unit, partition frames into contiguous windows; replicate a per-provider halo `H = (window_frames−1) + max_gap_frames` into adjacent windows; build each `(core+halo)` window via the sk build (`build_unit_inputs`); trim halo rows; union of cores = the whole built frame. Pure core in `analytics.action_context`; Spark dispatch in `ingestion`.

**Tech Stack:** Python 3.10, PySpark (`mapInPandas`, ADR-045), pandas, silly-kicks 4.128.0 (`tracking.preprocess`), Databricks serverless.

**Spec:** `docs/superpowers/specs/2026-10-07-haloed-frame-window-build-seam-design.md` (APPROVED rev3; read it + both R3 reports first).

## Global Constraints (verbatim from the spec)

- Halo `H = (window_frames − 1) + max_gap_frames` per provider, sourced at runtime from sk `PreprocessConfig` (`window_frames = max(round(sg_window_seconds × hz)|1, sg_poly_order+2)`; `max_gap_frames = ceil(max_gap_seconds × fps)`) + a module-level sentinel that fails loud on sk `sg_window_seconds`/`max_gap_seconds`/pipeline-order drift (ADR-067). NEVER `max(...)`.
- Neutrality is a MEASURED contract: byte-identical except frames within `(window_frames−1)` of BOTH a window boundary AND a `>max_gap` detection gap (uncapped `np.interp`, `_velocity.py:76-94`).
- **Owner ruling — residual rule: CONSERVATIVE.** Per provider: `frac_affected == 0` → neutral, no golden change; any non-zero residual → value-change + regen that provider's goldens. NO compare-atol relaxation.
- **Owner ruling — AC: TAKE THE CORRECTION NOW.** Adopt the seam at AC with the boundary-velocity correction live; regen AC goldens; ride the already-scheduled sk-4.128 §8 re-materialize (no second wipe).
- Convert-stage window-safety is a per-provider feasibility GATE before adoption (a whole-unit convert op = seam infeasible for that site → exclude + surface).
- `window_id` on the dense DISTINCT-frame ordinal, NOT raw row index (a frame = ~22 player rows).
- Layering: pure core in `analytics` (no pyspark/ingestion import); Spark dispatch in `ingestion` (import-linter: ingestion→analytics OK).
- Serverless: 16 GB driver, ~1 GB UDF-group cap; lazy closure capture; no broadcast/cache of large data.
- One feature branch; INLINE; coherent commits; each commit/push/PR/merge a separate approval. No **production** wheel-consuming job (the daily drains) until post-merge main CI green ([no-jobs-in-ci]) — EXCEPT the owner-approved **pre-merge dev-wheel diagnostic one-offs** (T8 residual measurement, T9 canary), each its own separate approval, on a dev wheel never promoted to the daily path (the T9-fit-profile precedent). HFW-PLAN-06: this distinction (production-job vs dev-one-off) is the intent — the post-merge gate binds the production drains, not the dev diagnostics.

## File Structure

- **Create `src/analytics/action_context/frame_windows.py`** — pure seam core (no pyspark): `halo_frames(provider) -> int` (+ the `_HALO_SENTINEL` assertion), `assign_window_ids(frames_pdf, provider, target_window_frames) -> frames_pdf'` (adds `window_id` + `is_halo` on a driver/local pandas frame — the pure algorithm), `build_windowed(core_plus_halo, build_fn) -> core_built` (run `build_fn`, drop `is_halo`), `assert_orientation_backstop_noop(...)` guard.
- **Create `src/ingestion/frame_window_dispatch.py`** — Spark transform `assign_frame_windows(raw_sdf, provider, *, target_window_frames, halo) -> raw_sdf'` (window_id + halo replication at the Spark level, reusing the pure `assign_window_ids` logic per partition) + `windowed_build_sdf(spark, raw_sdf, provider, build_fn, built_schema, *, target_window_frames)`.
- **Modify `src/ingestion/tracking_marts_dispatch.py`** — `run_stage1_build_and_spill` uses `windowed_build_sdf` (grain `(match, period, window_id)` instead of whole-unit).
- **Modify `src/ingestion/shot_freeze_frames.py`** — the snapshot build uses the seam.
- **Modify `src/ingestion/action_context.py`** — replace `frame_batch_id = floor(...)` (`:1909`) grouping with the haloed window seam; AC correction live.
- **Modify `src/analytics/action_context/unit_inputs.py` / `pipeline.py`** — only if the build needs to accept a pre-assigned window subset (likely none — `build_unit_inputs` already takes a frame subset).
- **Tests:** `src/tests/action_context/test_frame_windows.py` (pure: halo, assign, build_windowed, neutrality bounded + >max_gap, sufficiency H/H−1), `src/tests/tracking_marts/test_windowed_stage1.py`, `src/tests/test_shot_freeze_windowed.py`, AC neutrality-interior + boundary-characterization tests.
- **Diagnostic (scratch, not committed):** per-provider `frac_affected` / `max|Δspeed|` on real bronze (Databricks notebook_task, like the T9 fit-profile) → the neutral-vs-regen decision record → `D:\Development\_reviews\2026-10-07-haloed-residual-measurement.md`.
- **Docs:** new ADR (seam); AC goldens regen; sk-4.128 §8 runbook amend (AC rides it).

---

## Task 1: `halo_frames(provider)` + drift sentinel

**Files:** Create `src/analytics/action_context/frame_windows.py`; Test `src/tests/action_context/test_frame_windows.py`

**Interfaces — Produces:**
- `halo_frames(provider: str) -> int` — `(window_frames − 1) + max_gap_frames` from sk `PreprocessConfig` for the provider.
- `_PINNED_HALO: dict[str, int]` + an assertion that the sk-derived value matches (fails loud on sk drift).

- [ ] **Step 1 — failing test (derive from sk config, NOT the 9/39 estimate — TMP-H-04/HFW-PLAN-03):** the test recomputes the expected H from the SAME documented formula + the live sk `PreprocessConfig`, and asserts `halo_frames` equals it — so the test is not pinned to the spec's estimate:
```python
def test_halo_frames_equals_composed_reach_formula():
    from analytics.action_context.frame_windows import halo_frames, _resolve_sk_cfg

    for p in ("gradientsports", "idsse", "skillcorner", "metrica"):
        cfg, fps = _resolve_sk_cfg(p)  # live sk PreprocessConfig + provider fps
        window_frames = max(round(cfg.sg_window_seconds * fps) | 1, cfg.sg_poly_order + 2)
        if window_frames % 2 == 0:
            window_frames += 1
        max_gap_frames = math.ceil(cfg.max_gap_seconds * fps)
        expected = (window_frames - 1) + max_gap_frames  # composed reach (§4.2)
        assert halo_frames(p) == expected and expected >= 1
```
- [ ] **Step 2 — verify fail** (ImportError).
- [ ] **Step 3 — implement** `halo_frames` + `_resolve_sk_cfg`: read sk `PreprocessConfig` per provider; return `(window_frames-1)+max_gap_frames`. The **drift sentinel is NOT tautological** (TMP-H-04): `_PINNED_SK_CFG` pins the sk-4.128 INPUT values per provider (`sg_window_seconds`, `sg_poly_order`, `max_gap_seconds`, fps) **with a comment showing the sk source file:line + the hand-derivation to H (GS≈9, metrica≈39 are the EXPECTED of that derivation, not the pin)**; at import, `assert _resolve_sk_cfg(p) == _PINNED_SK_CFG[p]` — fires when sk changes a config value (the pin is the sk INPUT, the formula derives H, so a sk change breaks loudly instead of silently resizing the halo).
- [ ] **Step 4 — verify pass.**
- [ ] **Step 5 — sentinel test (non-vacuous):** `test_halo_sentinel_fires_on_sk_drift` — monkeypatch `_resolve_sk_cfg` to return a changed `sg_window_seconds` → assert the import-time `_PINNED_SK_CFG` assert raises (proves the pin guards the INPUT, not halo_frames' own output).
- [ ] **Step 6 — checkpoint** (green + staged; NO commit — folds into the single T10 commit).

## Task 2: `build_windowed` + neutrality / sufficiency / residual tests (pure)

**Files:** Modify `frame_windows.py`; Test `test_frame_windows.py`
**Interfaces — Consumes:** `halo_frames`. **Produces:** `assign_window_ids(frames, provider, target_window_frames) -> frames` (adds `window_id`, `is_halo`); `build_windowed(core_plus_halo, build_fn) -> core_built`.

- [ ] **Step 1 — neutrality (bounded gap) failing test:** on a committed fixture (small, ≥2 windows, FULL pipeline — `PreprocessConfig(derive_velocity=True)`), plant a ≤`max_gap` detection-NaN gap straddling a window boundary; assert `pd.concat(build_windowed(w) for each window) == build_unit_inputs(whole).frames` value-identical (sorted, `check_dtype=False`). Force `target_window_frames` low so the fixture spans windows.
```python
def test_windowed_build_byte_identical_bounded_gap(gs_fixture_with_small_gap):
    whole = build_unit_inputs(wu, frame_bundle=FrameBundle("tracking", frames_all), ...).frames
    windowed = concat_core_builds(frames_all, provider="gradientsports", target_window_frames=SMALL, build_fn=...)
    assert_frame_equal(sort(windowed), sort(whole), check_dtype=False)
```
- [ ] **Step 2 — verify fail.**
- [ ] **Step 3 — implement** `assign_window_ids` (dense DISTINCT-frame ordinal `//` target; `is_halo` for frames within `H` of a boundary, duplicated into the neighbour window) + `build_windowed` (run `build_fn` on core+halo, drop `is_halo` rows, return core).
- [ ] **Step 4 — verify pass.**
- [ ] **Step 5 — sufficiency (non-vacuous):** `test_halo_exactly_sufficient` — a frame exactly `H` from a boundary is byte-identical; with `H−1` (monkeypatched) it DIFFERS. Proves the composed reach, not an over-sized halo.
- [ ] **Step 6 — residual (>max_gap) characterization:** plant a `>max_gap` gap within `(window_frames−1)` of a boundary; assert the divergence is CONFINED to the predicted band (interior rows still identical); record the band predicate. (Per owner ruling, a real-corpus `frac_affected>0` → regen; this test only bounds WHERE it can occur.)
- [ ] **Step 7 — checkpoint** (green + staged; NO commit).

## Task 3: `assign_frame_windows` Spark transform + halo replication

**Files:** Create `src/ingestion/frame_window_dispatch.py`; Test `src/tests/tracking_marts/test_windowed_stage1.py` (pyspark-skip marker; Docker-validated)
**Interfaces — Consumes:** `halo_frames`, `assign_window_ids`. **Produces:** `assign_frame_windows(raw_sdf, provider, *, target_window_frames, halo) -> raw_sdf'`; `windowed_build_sdf(spark, raw_sdf, provider, build_fn, built_schema, *, target_window_frames) -> built_sdf`.

- [ ] **Step 1 — pure-logic test** (no Spark): the window-ordinal + halo-replication logic on a pandas frame reproduces `assign_window_ids`; a frame within `H` of a boundary appears in BOTH windows with `is_halo` set in the neighbour. (The Spark transform delegates to the pure core per partition; validate the core here.)
- [ ] **Step 2 — verify fail.**
- [ ] **Step 3 — implement** `assign_frame_windows`: compute the dense frame ordinal per `(match, period)` (a Spark window-function `dense_rank` over distinct `frame`), `window_id = floor((ordinal-1)/target)`, then a halo explode (boundary frames emitted into the neighbour `window_id` with `is_halo=true`). `windowed_build_sdf`: `assign_frame_windows` → `repartition(_UDF_SHUFFLE_PARTITIONS, match_id, period, window_id).sortWithinPartitions(..., frame).mapInPandas(_make_streaming_group_mapper(build_windowed∘build_fn, keys), built_schema)`. Reuse `_UDF_SHUFFLE_PARTITIONS` + `_make_streaming_group_mapper`.
- [ ] **Step 4 — verify pass** (pure parts).
- [ ] **Step 5 — Docker two-stage test** (`test_two_stage_dispatch`-style, pyspark-skip locally): REAL Spark windowed build on a fixture spanning ≥2 windows == the whole-unit build. Run in the `ll-pyspark-test` container.
- [ ] **Step 6 — checkpoint** (green + staged; NO commit).

## Task 4: wire tracking-marts Stage-1 to the seam

**Files:** Modify `src/ingestion/tracking_marts_dispatch.py:run_stage1_build_and_spill`; Test `test_windowed_stage1.py`
**Interfaces — Consumes:** `windowed_build_sdf`.

- [ ] **Step 1 — test:** `run_stage1_build_and_spill` spills a built frame whose union == the current whole-unit build (neutrality via the Task-2 oracle on a fixture); the spill is still keyed `(game_id, period_id)` for Stage-2 (unchanged).
- [ ] **Step 2 — verify fail.**
- [ ] **Step 3 — implement:** replace the whole-unit `repartition(…match_id,period).mapInPandas(build_udf)` with `windowed_build_sdf(spark, raw_trk_sdf, provider, build_udf, built_schema, target_window_frames=_TM_WINDOW)`. Keep the spill write + Stage-2 unchanged. Add `_TM_WINDOW` constant (per-provider or global; default ~21 000, tunable).
- [ ] **Step 4 — verify pass** (pure + Docker).
- [ ] **Step 5 — checkpoint** (green + staged; NO commit).

## Task 5: wire shot_freeze to the seam

**Files:** Modify `src/ingestion/shot_freeze_frames.py`; Test `src/tests/test_shot_freeze_windowed.py`
- [ ] **Step 1 — test:** the shot_freeze snapshot build over windows == whole-unit snapshot build (neutrality) on a fixture; dense IDSSE no longer whole-unit.
- [ ] **Step 2 — verify fail.**
- [ ] **Step 3 — implement:** route shot_freeze's `mapInPandas` build through `windowed_build_sdf` (shot_freeze needs no spill — it yields the snapshot; §9 Q4 → window-build-in-place, union the per-window snapshots).
- [ ] **Step 4 — verify pass.**
- [ ] **Step 5 — checkpoint** (green + staged; NO commit).

## Task 6: convert-feasibility per-provider gate (metrica/skillcorner/GS)

**Files:** `src/tests/action_context/test_convert_window_safety.py` (analysis-backed asserts) + a short note appended to the spec §4.1a / the ADR.
- [ ] **Step 1 — for each of metrica, skillcorner, gradientsports:** read its sk converter (`sk_frame_adapters` → the sk metrica/kloppy/GS converter); confirm orientation is a per-unit constant applied per-frame (no whole-sequence re-derivation), groupby is per-period, no sequence-length resample. Record the file:line evidence.
- [ ] **Step 2 — test:** a per-provider assertion/fixture that the `assert_orientation_backstop_noop` guard holds (the backstop did not fire) on a representative built unit. A provider that FAILS the gate is EXCLUDED from the seam (surfaced, kept on a bounded alternative) — do NOT silently ship.
- [ ] **Step 3 — checkpoint** (gate + evidence; NO commit).

## Task 7: AC drain adoption (value-change; correction live)

**Files:** Modify `src/ingestion/action_context.py` (`:1909` grouping); Test `src/tests/action_context/` (interior-neutrality + boundary-characterization)
**Interfaces — Consumes:** `assign_frame_windows`.

- [ ] **Step 1 — interior-neutrality test:** AC enrich over the haloed window seam == current output on all frames EXCEPT within `(window_frames−1)` of a former `frame_batch_id` boundary (interior byte-identical). Non-boundary rows must not move.
- [ ] **Step 2 — boundary-characterization test:** at former batch boundaries, the haloed velocity differs from the halo-less result (the correction) — assert it matches the WHOLE-UNIT build there (i.e. the halo fixes it to the correct value).
- [ ] **Step 3 — verify fail.**
- [ ] **Step 4 — implement:** replace `frame_batch_id = floor(frame_col / frame_batch_size)` + `_group_keys=[match_id,period,frame_batch_id]` with the haloed `window_id` assignment (`assign_frame_windows`, `target_window_frames = frame_batch_size`) + the halo explode; `enrich_batch` runs per `(match, period, window_id)` on core+halo, output trimmed to core. Preserve the ADR-068 events/watchdog/circuit-breaker + the executor rendezvous markers.
- [ ] **Step 5 — ACTIONS single-owner (TMP-H-06):** the halo replicates **FRAME rows only** — `is_halo` frame rows feed the per-window velocity/preprocess but are trimmed from the output; each ACTION is enriched in EXACTLY ONE window (its owning `window_id`), never duplicated by the halo. Add a test: an action whose frame is in the halo band is enriched once (asserted row count == the whole-unit action count; no AC single-owner double-count). This is the AC-specific risk the frame-only halo must not break.
- [ ] **Step 6 — verify pass.**
- [ ] **Step 7 — checkpoint** (green + staged; NO commit).

## Task 8: per-provider residual measurement (PRE-MERGE diagnostic → decision record) — runs BEFORE T9 goldens

**Files:** scratch Databricks notebook_task (the T9 fit-profile precedent); artifact `D:\Development\_reviews\2026-10-07-haloed-residual-measurement.md`
**Sequencing (TMP-H-03 / HFW-PLAN-02):** this is a **PRE-MERGE branch-wheel diagnostic** — it runs on a dev build of the seam (bump to a dev wheel + deploy to the libs UC Volume, owner-allowed dev one-off per the fit-profile precedent; separate approval), BEFORE the single commit, because its per-provider disposition DECIDES which goldens the commit carries. It is NOT post-merge.
- [ ] **Step 1 — measure** `frac_affected` (fraction of built rows within `(window_frames−1)` of BOTH a window boundary and a `>max_gap` gap) + `max|Δspeed|` vs the whole-unit build, per provider, on the largest real dense half (gradientsports:10502:1 + idsse/skillcorner/metrica reps).
- [ ] **Step 2 — apply the owner rule (conservative):** per provider, `frac_affected==0` → neutral (no golden change); `>0` → value-change → that provider's tracking-marts/shot_freeze goldens regen (+ ride the re-mat). Record the per-provider disposition in the artifact — this is the input to T9.
- [ ] **Step 3 — no commit** (diagnostic; gates T9's golden set).

## Task 9: goldens + gates + canary

**Files:** AC goldens (mini `idsse/J03WMXmini_p1/golden.parquet` + full `J03WMX_p1`); tracking-marts/shot_freeze goldens IF Task-8 says value-change for a provider; `_topandas_exemptions.yml` (unchanged — the seam adds no toPandas); ADR.
- [ ] **Step 1 — regen AC goldens** (the correction is a value change) via `scripts/build_ac1_{mini,full}_golden.py`, capturing the boundary delta (capture-before-cleanup); + any provider Task-8 flagged. Record what moved.
- [ ] **Step 2 — all seven local gates + `validate_workflow_cards` + full `uv run pytest`** (CI-exact pyright via `uv run`).
- [ ] **Step 3 — re-run the ADR-087 preflight canary on gradientsports:10502:1** (operator-gated; the reliable OOM repro) → expect `drain_canary_ok`; fit-profile the per-window peak ≤ 800 MB per provider.
- [ ] **Step 4 — ADR** (`docs/superpowers/adrs/ADR-089-haloed-frame-window-build-seam.md`): the seam, composed halo, measured-neutral contract, 3-site adoption, AC value-change.
- [ ] **Step 5 — checkpoint** (goldens + ADR staged; NO commit — the single commit is T10).

## Task 10: runbook + docs + STOP

- [ ] **Step 1 — amend the sk-4.128 §8 re-materialize runbook:** AC's boundary-correction re-mat RIDES the existing §8 sequence (no second wipe); note the seam + the Task-8 per-provider dispositions. Update `AI_GOVERNANCE.md` only if a model card is affected (velocity feeds xt_gk_v2 `pressure` — already retrain-pending; confirm no new governance row).
- [ ] **Step 2 — TODO.md** cycle update (IN the commit).
- [ ] **Step 3 — C4** regen if a container description changes (the AC/tracking containers' build mechanism).
- [ ] **Step 4 — T-STOP:** present the full diff/file list; await explicit commit approval. NO commit/push/PR without it.

---

## Self-Review

**Spec coverage:** seam (§4.1/4.4)→T1-3; composed halo+sentinel (§4.2)→T1; measured contract+conservative rule (§4.2a, owner)→T2/T8/T9; convert feasibility gate (§4.1a)→T6; tracking-marts (§4.5a)→T4; shot_freeze (§4.5b)→T5; AC value-change take-now (§4.5c, owner)→T7/T9/T10; residual measurement→T8; tests (§6)→T2/T3/T7; risks R1-R6→T6(convert)/T1(sentinel)/T2(ordinal)/T8(residual); rollout (§8)→Global+T10. No gap.

**Placeholder scan:** none. The `halo_frames` value is DERIVED (T1 reads the sk `PreprocessConfig` + applies the §4.2 formula; the test recomputes the expected from the live sk config, not the 9/39 estimate; the sentinel pins the sk INPUT values with the derivation shown — rev2 TMP-H-04 fix). "Read the sk config at implement-time" is an instruction, not a deferred value.

**Type consistency:** `halo_frames(provider)->int`, `assign_window_ids(frames,provider,target)->frames`, `build_windowed(core_plus_halo,build_fn)->core`, `assign_frame_windows(raw_sdf,provider,*,target_window_frames,halo)->raw_sdf`, `windowed_build_sdf(spark,raw_sdf,provider,build_fn,built_schema,*,target_window_frames)->built_sdf` — consistent across T1-7.
