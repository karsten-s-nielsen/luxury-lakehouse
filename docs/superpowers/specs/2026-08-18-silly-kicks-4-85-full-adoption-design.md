# silly-kicks Full Adoption + Live Recompute — Design Spec

- **Date:** 2026-08-18 (Rev 7, 2026-08-20 — target re-pointed 4.86.1 → **4.87.0**; delta NIL, see note; Rev 6 design unchanged)
- **Status:** DRAFT — execution-ready (all review rounds + review-4 writer fixes folded in)
- **Rev 6 scope change (user directive):** the round-1/round-2 reviews *recommended* deferring/excluding three
  families; those are advisory, not scope. Per the user, **nothing is deferred** — every new 4.87.0 metric is
  adopted, organized **wide-by-grain** (§7): `gk_geometry_source` via **xtgk v2 replacing v1**; defensive-credit,
  bravery, gkdv, off-ball-runs, visibility all IN, each landing in the existing mart at its grain or a new
  grain-named mart. Completeness verified against silly-kicks' `feature_glossary.py` (audit found + folded in 6
  uncaptured `team_shape` cols, `obso_epv_source` under-count, two families the spec never surfaced —
  `value_off_ball_runs`, `gkdv`).
- **Target:** `silly-kicks[das,ghost-gk,parse-dfl]` **4.43.0 → 4.87.0** — the release the silly-kicks *Part Deux*
  cycle cut (2026-08-19), carrying the off-ball-context fix. Version assigned by the silly-kicks release process
  (confirmed: PyPI latest + tag `v4.87.0`), **not** this spec.
- **Delivery shape:** consume `4.87.0` (the fix is already released) + one big-bang lakehouse PR + a driven live
  recompute

> **Target = 4.87.0; delta re-validated 4.43→4.87.0 (Rev 7).** Version cut by *Part Deux*, not lakehouse-assigned
> (verified PyPI latest + tag `v4.87.0`). Three releases past the originally-mined 4.85, all folded in:
> - **4.86.0** — StatsBomb `cross_blocked` un-deferred (all-`pd.NA` → real open-play-cross mask, §7.2). Additive;
>   no VAEP/atomic feature reads it → no silly-kicks or lakehouse retrain; it IS a StatsBomb-SPADL value change
>   the recompute materializes (§11.1b oracle). Now **consumed by bravery** (§7.5 — no longer deferred, Rev 6).
> - **4.86.1** — the off-ball-context **crash-fix** only (guard inside `_line_break_kernel`, verified at
>   `_off_ball_runs.py:313`; ADR-055 edge policy, all three callers). No value change on resolvable inputs, no
>   retrain. Rides inside the 4.87.0 target.
> - **4.87.0** — a **reported-not-gated** cover-shadow / pass-risk research-validation cycle (ADR-064, PR-S157):
>   three `scripts/` research drivers + `docs/research/` artifacts. **NO library API / column / aggregator /
>   behaviour change, no retrain** — NIL lakehouse impact beyond the pin bump.
>
> Net: the mined 4.43→4.85 tables **and the entire Rev-6 design hold unchanged**. Delta re-validation **closed**.

## Rev 3 — what changed after review round 2

| Review finding | Resolution in this rev |
|----------------|------------------------|
| **R1** (high) mart-rebuild list incomplete → half-refreshed gold layer | §11.3 replaced the hand-picked 3-mart list with a **`dbt ls --select <root>+` DAG-completeness step** (≥8 stale consumers confirmed by grep: `fct_player_stats`, `fct_vaep_breakdown_agg`, `fct_goalkeeper_stats`, `fct_funnel_stages_agg`, `fct_gk_tracking_actions`, `fct_shot_psxg`, `fct_xg_predictions_v2`, `int_running_score`), partitioned TRIGGERED-vs-not |
| **R2** (med) shadow validation scoped only to AC | §11.1b widened to **all four destructively-re-run surfaces** (AC, `spadl_actions`, `shot_freeze_frames`, `vaep_action_values`) |
| **R3** (med) "expected shift" is the correctness question, left hand-wavy | The **expected-shift oracle is now a first-class deliverable** (§8) with a defined methodology (cohort + rate-band + direction per column) |
| **R4** (med) cross-repo release cut under-specified | Phase-1 now verifies against the silly-kicks **remote** (not local), and assigns explicit ownership of the cut |
| **R5** (low) dangling §-refs; §10 empty | Phase headings **numbered 5–11**; Phase-6 steps renumbered `11.x`; **wheel-build ordering** content written into §10 |

## Rev 2 — what changed after review round 1

| Review finding | Resolution in this rev |
|----------------|------------------------|
| **C1** "0-NULL" is the wrong live gate (contradicts nullable columns) | Replaced with **per-column expected-null-rate bounds** (§11.1, §13) |
| **C2** rebaselined goldens validate stability, not correctness | Added **invariant/range checks + a pre-wipe shadow-recompute distributional diff** (§8, §11.1); risk §12 corrected |
| **C3** retrain conflates input-shift with output-shift | Retrain set = **VAEP + ScoutGPT + xG v3** (corpus-wide); football2vec reasoning corrected — it reads action attributes, **not** `vaep_value`, so it is insulated (§11.2) |
| **C4** defensive-credit threads gold xG back into bronze AC (circular) | **Defer the entire defensive-credit family** (per-action + long-form) as a separate architecture decision (§7.4) |
| **C5** `add_off_ball_context` — fix upstream not consumer | **Upstream per-frame guard, in `4.87.0`**; no consumer workaround (Phase-1, §6.1) |
| **C6** governance conditional; private-module debt | Governance made **unconditional** with per-family card mapping; **new-private-import lint** added (§9) |
| Minors | Wheel built post-Phase-2; regression test for the two KEEP-`home_team_id` silent-failure cases; golden-vs-contract type assertion |

---

## 1. Context & Motivation

The lakehouse pins silly-kicks **4.43.0** (2026-07-10). Since then 42 versions shipped (4.85.0 tagged/on-PyPI
2026-08-18): a direction-of-play correctness cycle (ADR-028/051), SB360 enablement + velocity-less
pitch-control lift (ADR-062/063), and ~11 new column families. This spec adopts the library in full, adds a
single upstream fix (carried in `4.87.0`), materializes the in-scope new columns with contracts, and recalculates the
entire tracking + downstream-model surface on live data.

## 2. Goals / Non-Goals

**Goals**
- Land the one upstream silly-kicks fix and cut 4.87.0; move every pinned reference 4.43.0 → 4.87.0 in one
  lockstepped change, CI-green.
- Adapt every broken call site (direction re-key, `id_compat` import move, velocity fail-fast, nullable
  dtypes) so the existing pipeline produces **correct** values on `4.87.0`.
- Materialize the **in-scope** new column families (§7) into `fct_action_context` / the SPADL surface, with
  enforced contracts and validated per-column null-rate + distributional expectations.
- Rebaseline goldens (regression guard) **and** add independent correctness checks (invariant/range +
  shadow-diff) for the value-shifting recompute.
- Drive the full live recalculation: AC recompute → VAEP/ScoutGPT/xG-v3 retrains → mart rebuilds → HF
  republish → synced refresh → verification.

**Non-Goals** (Rev 6 — the only exclusions; everything silly-kicks-new is IN, §7)
- No re-fit of the persisted global xT grid (reuse-as-is — §14).
- No PSxG retrain (StatsBomb-native, unaffected — §11.2).
- No new lakehouse analytical capability beyond surfacing silly-kicks' 4.87.0 metrics.
  *(xT-GK v1→v2, visibility coverage, and the defensive-credit/bravery/gkdv families are all IN — §7; they were
  Non-Goals in Rev ≤5 and are no longer.)*

## 3. Scope decisions (locked with the requester)

1. **Big-bang single lakehouse PR** for all code (structure the commits phase-isolated for reviewability).
2. **ALL new metrics adopted — nothing deferred** (§7, Rev 6). The round-1/2 reviews *recommended* deferring
   three families; that is advisory, not scope. Everything is IN, wide-by-grain (incl. xtgk v2 replacing v1).
3. **End-to-end execution**: this session drives through the live recompute + retrains, with **separate,
   explicit requester approval at each commit / PR / merge / job step**.

## 4. Current state (verified facts)

- `main` == `origin/main` (`dbab517e`); nothing to pull.
- silly-kicks local checkout is exactly on tag `v4.85.0` (`git describe --tags` clean).
- Wheel version: `0.5.94` (`src/shared/wheel.py:18`); bump via `scripts/bump_wheel.py` (~30 consumers).
- AC applyInPandas StructType is **derived** from `ACTION_CONTEXT_DDL` via `_parse_ddl_to_struct_type`
  (`src/ingestion/action_context.py:111`) — new AC columns need `schema.py` + a bronze migration + dbt
  wiring, **no hand-edited StructField list**.
- Registry baseline: `RESULT_COLUMNS` = **152** (151 output + `_ingested_at`), exact name+order parity with
  `ACTION_CONTEXT_DDL` (`test_action_context_schema_parity.py`). The mini-golden asserts full list-equality
  against all **151** pipeline columns; its "103" docstring is **stale** (correct it in-flight).
- Two latent issues to fix in-flight: `scripts/sk3_mig_b_retrain.py` is dead (floor 4.26, wheel gate 0.3.34
  — already broken; **exclude** from lockstep, mark dead); `scripts/submit_ac1_oneshot.py`'s `numba` pin
  (0.64.0) has drifted from Terraform (`main.tf` 0.66.0) — realign in the same PR.

## Phase -1 — silly-kicks 4.87.0 (DONE — prerequisite satisfied)

The fix is released by *Part Deux*; the lakehouse consumes it. Recorded here for provenance and the
verification facts the implementation relies on.

- **Fix (shipped in 4.87.0):** `_line_break_kernel` now catches `GoalEndUnresolvedError` at a single point and
  returns its all-NaN frame (**verified** `_off_ball_runs.py:313` at tag `v4.87.0`), covering all three
  previously-unguarded callers — `add_line_break(method="threshold")`, `add_off_ball_context`, and the
  `off_ball_context_xfns` VAEP factory — per the ADR-055 edge policy. `add_line_break(method="ward")` unchanged.
  Crash-fix only: **no value change on resolvable inputs, no retrain**. Brief:
  `scratchpad/sk-off-ball-context-fix-handoff.md`.
- **Verified released:** PyPI latest = `4.87.0`; tags `v4.86.0` + `v4.87.0` present; the guard is in the tagged
  source. No lakehouse consumer workaround — the call sites (§6.1) simply drop `home_team_id` like their
  siblings; the fix lives in `4.87.0`.
- **Delta re-validation (CLOSED):** the 4.43→4.87.0 delta was re-run past the mined 4.85 (see the Rev-5 note at
  the top). Only additions: **4.86.0** StatsBomb `cross_blocked` NA→real (§7.2 — a value change, no retrain) and
  the **4.87.0** crash-fix. The §6/§7/§11.2 tables otherwise hold.

## 5. Phase 0 — Version lockstep (mechanical)

Move all to **4.87.0** in one commit. Order: edit `pyproject.toml` → `uv lock` → pin-sync tool → sentinels
→ wheel version bump (the wheel is *built/published* post-Phase-2 — see the wheel-build note in Phase 5).

| # | Location | 4.43.0 → 4.87.0 | Mechanism |
|---|----------|-----------------|-----------|
| 1 | `pyproject.toml:71` | floor `>=4.87.0,<5` | edit |
| 2 | `uv.lock` | resolved `4.87.0` | `uv lock --refresh-package silly-kicks && uv sync --inexact` |
| 3 | `terraform/modules/workflows/main.tf:1567` | `==4.87.0` | `scripts/sync_tf_env_pins.py` (never by hand) |
| 4 | `src/ingestion/exec_visibility.py:450` | `_REQUIRED_SK_MIN=(4,87,0)` | edit |
| 5–9 | `train_vaep_model_hf.py:73`, `train_football2vec.py:82`, `train_football2vec_360.py:76`, `train_football2vec_v2.py:78`, `train_scoutgpt_hf.py:82` | `(4,87,0)` | edit (sentinel-enforced) |
| 10 | `scripts/train_xg_v3_hf.py:145` | `(4,87,0)` | edit (**NOT** sentinel-enforced — must not be missed) |
| 11 | `scripts/submit_ac1_oneshot.py:55` | `==4.87.0` (+ realign `numba` to TF) | edit |
| 12 | `src/tests/test_sk3_mig_b_orchestrator_invariants.py:363` | expected `(4,87,0)` | edit |
| 13 | `src/shared/wheel.py` + ~30 consumers | wheel bump | `scripts/bump_wheel.py` |
| 14 | `src/ingestion/exec_visibility.py:466` `_SK_GUARD_SUBMODULES` | verify the 4 private paths still exist at `4.87.0` | verify + adjust |

Guardrails after Phase 0: `test_terraform_env_dep_parity.py`, `test_executor_env_guard.py`,
`test_sk3_mig_b_orchestrator_invariants.py`, `bump_wheel.py --check`. Mark `sk3_mig_b_retrain.py` dead +
exclude from lockstep (confirm it is not in the §2.10.4 trainer set).

## 6. Phase 1 — Breaking call-site adaptation

### 6.1 Direction re-key — drop `home_team_id`, keep it on two, guard none

**Verified empirically:** 14 of 16 aggregators self-resolve direction on our home-LTR frames, so migration =
**delete `home_team_id=`**. Build one shared map for the 5 that *accept* `goal_map=` (perf only):
```python
from silly_kicks.tracking import resolve_defended_goals
goal_map = resolve_defended_goals(tracking_df)   # once per work-unit, full frames
```

| Aggregator | `enrich.py` | `4.87.0` handling | **Migration action** |
|-----------|-------------|---------------|----------------------|
| `add_defensive_line` | 328 | `goal_map=None`, self-resolves | drop `home_team_id`; pass `goal_map=` (perf) |
| `add_off_ball_context` | 331 | `goal_map=None`; **`4.87.0` guards its own raise (Part Deux fix)** | drop `home_team_id`; pass `goal_map=` (no consumer catch — Phase-1 fixed it) |
| `add_line_break` (`method="ward"`) | 334 | ward path never touches `GoalMap` | drop `home_team_id` |
| `add_team_shape` | 337 | direction-free | drop `home_team_id` |
| `add_ghost_gk` | 374-382 | **`home_team_id` REQUIRED — genuinely read** | **KEEP `home_team_id=home_team_id`** |
| `add_gk_influence` | 387-396 | `goal_map=None`, self-resolves | drop `home_team_id`; pass `goal_map=` (perf) |
| `add_cover_shadows` | 402-404 | `goal_map=None`, self-resolves | drop `home_team_id`; pass `goal_map=` (perf) |
| `add_shape_graph` | 408 | direction-free | drop `home_team_id` |
| `add_obso` | 411-418 | direction-free | drop `home_team_id` (+ add `xt=` per §7.3) |
| `add_pausa` | 421 | direction-free | drop `home_team_id` (+ add `xt=` per §7.3) |
| `add_space_creation` | 424 | `home_team_id=0` DEAD | drop `home_team_id` (+ add `xt=` per §7.3) |
| `add_structural_pass` | 447 | direction-free | drop `home_team_id` |
| `add_player_influence` | 450-458 | direction-free | drop `home_team_id` |
| `add_xcross_attempt` | 462-470 | `home_team_id` gates `score_differential` **feature** | **KEEP `home_team_id=home_team_id`** (else feature silently NaN) |
| `add_xt_gk` | 478 | direction-free | drop `home_team_id` |
| `add_xshot_occurrence` | 435-437 | `home_team_id` dead | drop `home_team_id` |

Apply the same table to the SB360 chain (`enrich.py` ~556-647; grep `home_team_id=`). **Regression test
(round-1 minor):** the two KEEP cases are silent-failure — add an explicit test asserting `score_differential`
and the ghost-GK columns stay populated post-migration (a wrongly-dropped kwarg NaNs a feature with no error).

### 6.2 `id_compat` import move (4.53.0)

`silly_kicks.tracking._id_compat` → `silly_kicks.id_compat` (no shim). Grep `_id_compat` across `src/` +
`scripts/` + `src/tests/` and repoint (known: `test_frame_orientation_golden.py`). Old path raises
`ImportError` at collection.

### 6.3 Velocity fail-fast (ADR-063)

Declaration is a per-row string column `speed_source = SPEED_SOURCE_UNAVAILABLE` (`== "unavailable"`,
importable from `silly_kicks.tracking`) on **every** row; `velocity_unavailable_by_design` is `True` iff all
rows carry it.

| Frame state | 6 zero-fill consumers (`add_pitch_control`, `add_gk_influence`, `add_cover_shadows`, `add_player_influence`, `add_obso`, `add_space_creation`) | refuse-consumers (`add_das`, `add_ghost_gk`, `add_press_commitment`, `add_xcross_attempt`) |
|-------------|--------|--------|
| has `vx`/`vy` | run normally | run normally |
| declared velocity-less | zero-velocity positional model | honest-NaN with provenance |
| neither (forgot `derive_velocities()`) | raise `ValueError` (except `voronoi`) | raise |

**Impact is small:** full-tracking carries velocity (unaffected); the SB360 chain's `snapshot_to_tracking_frames`
already stamps the marker — **verify** every SB360 frame reaching the aggregators carries it (stamp in
`sb360_snapshots.py` if any hand-rolled path bypasses the converter). Moot for live data (SB360 AC held/empty,
ADR-058), but must be correct for tests + future enable.

### 6.4 Nullable-dtype changes (4.79/4.80)

Tracking `player_id`/`team_id` `int64`→nullable `Int64`; `acting_team_attacks_rtl` `bool`→nullable `boolean`.
Audit joins + `.astype(bool)` for the string-qualifier trap.

### 6.5 Ghost-GK column removal + artifact bump

`ghost_gk_density_spread` removed (4.54; ghost_gk_xfns 9→6) — remove any lakehouse schema/mart reference.
`GhostGkModel` artifact `1.2.0`→`1.3.0` (bundled variant ships in wheel) — verify no cached 1.2.0 artifact in
`/Volumes/soccer_analytics/dev_gold/model_weights/`.

### 6.6 Frame-builder + ghost-GK API changes (found in execution — spec §6 gap, re-audited 4.43→4.87)

The changelog mining covered aggregators/columns but MISSED the frame-builder surface. Verified against v4.87.0
source + the live test suite (13 failures). Adapted in execution chunks P1/P1b:
- **SkillCorner `convert_to_frames` now REQUIRES `pitch_length`/`pitch_width`** (unless `assume_standard_pitch=True`,
  which reintroduces the ADR-038 clamp/scale defect). Fix: thread real per-match dims from `bronze.skillcorner_matches`
  (`action_context.py` join + `sk_frame_adapters.py`), loud-log 105×68 fallback only when genuinely absent. Metrica/
  Sportec/GS builders unchanged (their required kwargs were already threaded).
- **Ghost-GK: `kde_backend` (+ `pitch_control_cache`) removed from `add_ghost_gk`/`compute_ghost_gk`.** The KDE
  backend moved to `GhostGkModel.predict_density(kde_backend=…)` (default `vectorized`); `compute_ghost_gk` now leads
  with `predict_mean` (params-only), not the KDE argmax. **Decision (user): accept the 4.87.0 default path** — drop
  the removed args (done), **RETIRE the `ghost_gk_method` provenance column** (schema chunk), rebaseline the goldens
  for the ~0.20 m value shift, and **verify the drain finishes inside the watchdog in Part B** (predict_mean may be
  cheaper than the old KDE; the perf-tuned `fft-cic` is no longer on the default path).
- **Velocity single-frame** (2 tests) — same SkillCorner pitch-dim cause. **SB360 coverage** — a genuine 4.87.0
  velocity-availability-contract change (velocity-derived GK closing-time is honest-NaN on freeze-frames); coverage
  expectation rebaselined.
- **IDSSE ET-direction deriver now RAISES on a period-less events frame** (found in Chunk STAB; spec gap).
  `silly_kicks.providers.sportec.derive_idsse_home_team_start_left_extratime` → `_resolve_period_column` now raises
  `RuntimeError` ("events carry no period column … refusing to report a silent pass") when neither `period_id` nor
  `period` is present — earlier ports returned `None`. Fix: the lakehouse re-owns a THIN defensive wrapper in
  `ingestion.spadl_adapter.derive_idsse_home_team_start_left_extratime` that preserves the historical "no period
  column → no ET → `None`" contract and delegates to the port otherwise; the 3 production callers (action_context /
  shot_freeze_frames / spadl_conversion) + `test_spadl_adapter_et_direction.py` route through it. Behaviour is
  identical on real IDSSE data (`shape_events_to_native` always emits `period`) — this is purely defensive.

### 6.7 UPSTREAM silly-kicks bug — non-chronological `action_id` (2nd upstream fix; **RESOLVED in 4.89.0**)

> **RESOLVED — landed in silly-kicks 4.89.0 (sk ADR-065 / PR-S159), 2026-08-21.** All six order-dependent
> converters (`sportec`, `gradientsports`, `metrica`, `wyscout`, `skillcorner`, `opta`) now sort chronologically
> at the top of the frame before any positional/`.shift()` derivation, enforced by a raise-by-default
> `_assert_chronological_action_id` guard at `_finalize_output`. The Part-A pin advanced 4.87.0 → 4.89.0 to fold
> it in (a mechanical lockstep bump). **Breaking input-contract change:** GradientSports gains a **required
> `start_time`** column (raw absolute event clock) — the lakehouse GS shaper (`adapt_gradientsports_events`)
> now maps bronze `startTime`/`eventTime` → `start_time`/`event_time`. **Measured per-provider retrain scope
> (from the 4.89.0 changelog):** IDSSE/sportec + wyscout genuinely change (Part-B regen goldens + retrain);
> **GS + skillcorner byte-identical → NOT retrain triggers**; metrica unmeasured → real-data M-C recommended
> before re-materialization. The original pending-fix analysis is retained below for the historical record.

Found running `add_packing` (Chunk P2). **Verdict (investigated): a genuine silly-kicks conversion bug, fixed
UPSTREAM per the user's "best-practice, fix the library not the lakehouse" directive — NOT a lakehouse
workaround.** `spadl/sportec.py:656` assigns `action_id = range(len(actions))` over **raw DFL document order**,
never sorting by time — so `action_id` order ≠ chronological, violating the canonical-SPADL invariant that
silly-kicks documents + relies on (`_gk_geometry.py:412`, `add_restart_coordinates`, `vaep/labels.py:415`,
`_retention_labels.py:70`, `secured_reception` `_packing.py:264`). **Not IDSSE-only — GradientSports too**
(`retains()` already worked around it for the live GS cohort). Three symptoms: (1) `add_packing` hard-RAISES on
IDSSE (34–76 intra-period time-inversions/match); (2) SILENT ~1-yr corruption — `_derive_end_coordinates` +
`_add_dribbles` use positional `.shift(-1)` → wrong IDSSE/GS end-coords + dribbles; (3) the invariant is
load-bearing across the library. **Fix (silly-kicks):** stable-sort by `(period_id, time_seconds)` before
`action_id = range(len)` in the sportec (+ audit gradientsports) converter. Brief:
`scratchpad/sk-action-id-chronological-order-fix-handoff.md`.
- **Consequence:** a value-shifting fix — renumbers IDSSE/GS `action_id` + corrects their end-coords/dribbles →
  regenerate the IDSSE/GS goldens + retrain VAEP/ScoutGPT/xG-v3 on the corrected SPADL (folds into §11 recompute).
- **Execution impact:** the DEFAULT 7-gate is NOT blocked (the IDSSE mini-golden 3-action slice has no inversion),
  so Part-A code completes on 4.87.0 with `add_packing` wired. But **the live IDSSE recompute (Part-B Task 21)
  would crash** → the fix is a **Part-B prerequisite that bumps the target again** (like the 4.86.1 off-ball-context
  fix). Adopt the fix release, re-convert IDSSE+GS bronze, re-regenerate the IDSSE/GS goldens, before Part-B IDSSE.

## 7. Phase 2 — New-metric materialization (ALL families, WIDE-BY-GRAIN)

**Scope (Rev 6):** nothing is deferred or excluded. Every new metric silly-kicks 4.87.0 emits is adopted.
Metrics are grouped **by grain into wide marts** (like `fct_action_context` / `fct_action_values`), never one
mart per feature; an existing mart at the grain is **extended** rather than a new mart created.

| Grain | Mart | Action | Families |
|-------|------|--------|----------|
| per-action, tracking | `fct_action_context` | **extend** | obso-xt, run-values, press-commitment, packing, provenance, team_shape gaps, xtgk-v2, visibility |
| per-action, post-xG | `fct_action_defensive` (**NEW**, per-action) | **new** | per-action defensive-credit — downstream of `fct_shot_xg` (see note below; cannot extend `fct_action_values`, which xG is *upstream*-joined into) |
| SPADL action | `fct_action_values` / bronze SPADL | **extend** | `shot_blocked`, `cross_blocked` |
| per-`(match, team)` | `fct_match_summary` (`group by match_key, team_id`) | **extend** | bravery |
| per-keeper-pooled `(player, comp, season)` | `fct_gk_shot_stopping_pooled` (`group by player_key, competition_key, season_id`) | **extend** | gkdv `aggregate_by_keeper` |
| per-run `(action, runner)` | `fct_off_ball_runs` | **NEW** (no existing grain-mart) | `detect_off_ball_runs` + `value_off_ball_runs` |
| per-`(action, player, rule)` | long-form defensive-credit mart (grain-named) | **NEW** (no existing grain-mart) | `compute_defensive_credits` |

Per-AC-column wiring path (for the `fct_action_context` set): `enrich.py` → `schema.py` (`RESULT_COLUMNS` +
`ACTION_CONTEXT_DDL`) → idempotent bronze `ALTER` migration → `stg_action_context__values.sql` `cast(...)` →
`fct_action_context.sql` + `_marts__models.yml` contract. The new marts follow the ADR-013 writer→bronze→
staging→gold pattern.

### 7.1 `fct_action_context` — per-action tracking (extend)

Columns emitted by the AC enrichment chain (pre-xG), added to `RESULT_COLUMNS` + `ACTION_CONTEXT_DDL`:

| Family | Columns (dtype) | Emitting fn | Mechanism |
|--------|-----------------|-------------|-----------|
| Real-xT OBSO (4.52) | `obso_epv_source` (STRING: `xt`/`synthetic`/`injected`, NA off-domain) | `xt=` on obso/pausa/space | kwarg on 4 calls — MANDATORY §7.3 |
| Off-ball run values (4.52) | `run_value_target`,`run_value_disruptive_sum`,`run_value_enabled_pass` (DOUBLE); `n_disruptive_runs`,`n_valued_disruptive_runs` (BIGINT) | `add_off_ball_run_values(actions,frames,xt,...)` | NEW call |
| Press commitment (4.61) | `press_commitment`,`press_commitment_closing_speed` (DOUBLE); `press_commitment_source` (STRING) | `add_press_commitment` | NEW call |
| Packing (4.50) | `packing_made`,`packing_goal_threat` (BIGINT); `packing_net` (DOUBLE); `packing_receiver_player_id` (ID); `packing_secured` (BOOLEAN) | `add_packing` | NEW call |
| Provenance | `das_source`,`ghost_gk_source` (STRING) | `add_das`,`add_ghost_gk` (called) | free ride |
| Cover-shadow id | `max_single_defender_player_id` (ID) | `add_cover_shadows(detailed=True)` (called) | free ride |
| **team_shape gaps** | `team_shape_defensive_line_height_{attacking,defending}`, `team_shape_inter_line_gap_1_{attacking,defending}`, `team_shape_inter_line_gap_2_{attacking,defending}` (6, DOUBLE) | `add_team_shape` (called; emits 20, we carry 14) | free ride |
| **Visibility (8)** | `visible_area_fraction` (DOUBLE), `visible_area_source` (STRING); + 6 companions `{nearest_defender_distance,receiver_zone_density,defenders_in_triangle_to_goal}_observed_{fraction,source}` | `add_visible_area_coverage(visible_area=)` + `add_action_context(visible_area=)` | NEW call + kwarg; needs the §7.5 `visible_area` parser (SB360 rows; empty until SB360 AC enabled) |
| **xtgk-v2 (6)** | see §7.4 | `xtgk.compute_xt_gk_v2` + `apply_resolved_gk_geometry` | **replaces v1 §7.4** |

### 7.2 SPADL columns — `shot_blocked`, `cross_blocked` (4.56)

`BOOLEAN` nullable, baked into `SPADL_COLUMNS` (every `convert_to_actions` emits them). Add to `_SPADL_SCHEMA`
(`spadl_vaep.py:55`) + `_VAEP_SCHEMA` (`:126`) + per-provider StructTypes; `test_spadl_vaep_writer_parity.py`
gates. **They now feed the per-action defensive-credit (§7.5).** 4.86.0 delta: StatsBomb `cross_blocked` flips
all-`pd.NA` → real open-play-cross mask — a value change on the StatsBomb SPADL surface, folded into the §11.1b
oracle (non-null 0% → ~base-rate).

### 7.3 MANDATORY on bump — real-xT OBSO (`xt=`)

`enrich.py` calls obso/pausa/space **without `xt=`** (411, 421, 424, 640-641). From 4.52 on, omitting `xt=`
emits a non-fatal `SyntheticEPVWarning` and **falls back to synthetic EPV** → a bare bump silently degrades
every AC run to synthetic values (the warning is not escalated to an error). Adding `xt=xt` (grid already in
scope) is **required**; it switches OBSO/PAUSA/space to real fitted xT (corpus-wide value change → §8) and
stamps `obso_epv_source="xt"`; regression is caught by the mini-golden `obso_epv_source` value test.

### 7.4 xtgk v2 **replaces** v1 (`gk_geometry_source` + v2 metric)

Retire the in-repo v1 `add_xt_gk` chain (enrich.py steps 25/25b) and adopt the silly-kicks `xtgk` v2 pipeline.
- **Call sequence (verified against `tests/xtgk/test_resolved_origin_changes_score_e2e.py`):**
  `resolved = apply_resolved_gk_geometry(actions)` (overrides start/end coords in-domain, adds
  `gk_geometry_source` STRING 7-vocab) → `rf = extract_retention_features(resolved)` → `compute_xt_gk_v2(resolved,
  possession_value=…, retention=…, turnover_cost=…, pressure_levels=…, retention_features=rf)`. Ordering is
  enforced by v2's internal `_check_coordinate_coherence` (raises if scored off a non-resolved frame).
- **v2 output (per-action, DOUBLE):** `xt_gk_v2_position`, `xt_gk_v2_pev`, `xt_gk_v2_retention_loss`,
  `xt_gk_v2_dzv`, `xt_gk_v2` (+ `gk_geometry_source`). NaN (never grid-fabricated) on non-finite coords.
- **Model provisioning (CORRECTED — review-2 blocking): only `retention` is bundled; `possession_value` +
  `turnover_cost` must be FIT.** Verified at v4.87.0: the only bundled xtgk data is
  `_retention_weights/{default,skillcorner}/` (loaded by `GkRetentionModel.from_variant`). `MarkovPossessionValue`
  exposes only `.fit()`/`.load()` (nothing bundled to `.load()`) and `_turnover` only `.fit()`. Both must be fit
  on the **gold action marts** (the docstring requires the terciles/features match the fit corpus). So xtgk-v2 is
  a **fit-on-corpus training sub-project** — an ADR-012 trainer (`scripts/train_xt_gk_v2_hf.py`) + a §11.2 retrain
  row — NOT a wiring adoption. It is the largest single workstream in this migration. *(I previously mis-stated
  this as bundled; corrected.)*
- **Capability loss (review-2 M-A): the 5 v1 philosophy presets have NO v2 successor.** v2 emits one `xt_gk_v2`
  (+ 4 terms), no presets — so `xt_gk_{possession,counter,direct,high_press,low_block}` are DROPPED with no
  re-home; any Taipy/HF/mart view on the tactical-philosophy presets loses it. A **known regression** of the
  v2-replaces-v1 decision, flagged for the user.
- **v1→v2 reconciliation:** retire the 16 v1 `xt_gk_*` + the 5 presets; **keep `gk_completion`** (a distinct
  `add_gk_completion` call, unaffected); re-home the remaining v1 consumers/marts onto `xt_gk_v2`.

### 7.5 Other-grain marts (extend existing where the grain exists)

- **Per-action defensive-credit → NEW `fct_action_defensive` (per-action, post-xG).** `add_defensive_credit(
  actions, frames, xg_column=…, xt=…, blocked_column="shot_blocked")` emits `defensive_credit_net`/`_plus`/`_minus`
  (DOUBLE, 0.0 not NaN when no credit), `n_defensive_credits` (BIGINT). It needs per-shot xG. It **cannot** extend
  `fct_action_values` — `fct_shot_xg` already `ref()`s `fct_action_values` (`fct_shot_xg.sql:43`), so putting an
  xG-dependent column into `fct_action_values` is a **dbt cycle**. Instead a new per-action mart *downstream* of
  `fct_shot_xg` (writer reads bronze AC frames + `fct_shot_xg` predictions → bronze → staging → `fct_action_defensive`,
  ADR-013). Same grain as `fct_action_context`/`fct_action_values` but a different DAG position (post-xG); it holds
  all future post-xG per-action metrics, so it is grain-consistent, not a per-feature mart.
- **Long-form defensive-credit → NEW grain-named mart, per `(action, player, rule)`.** `compute_defensive_credits`
  11 cols: `game_id, period_id, action_id, player_id, team_id` (keys), `rule` (STRING 10-vocab), `signed_value`
  (DOUBLE), `anchor_type` (STRING 5-vocab), `frame_id` (BIGINT), `sizing` (STRING 3-vocab), `resolution`
  (STRING 6-vocab).
- **Bravery → extend `fct_match_summary`** (per `(match, team)`). `compute_bravery` 10 cols (grain = defending
  team): `bravery_shots`,`bravery_open_play_crosses`,`bravery_set_piece_crosses` (DOUBLE; set-piece always NaN
  v1),`bravery_pct_known_domain` (DOUBLE),`n_shots_faced`,`n_open_play_crosses_faced`,`n_set_piece_crosses_faced`,
  `n_blocks_known` (BIGINT). Key on `(match_key, defending team_id)`.
- **Off-ball runs → NEW `fct_off_ball_runs`, per `(action, runner)`.** `detect_off_ball_runs` 14 cols (keys +
  `run_start_x/y`,`run_end_x/y`,`displacement_m`,`duration_s`,`mean_speed_ms`,`peak_speed_ms`,`peak_speed_source`
  STRING,`toward_goal` BOOLEAN) + `value_off_ball_runs` 4 cols (`role` STRING,`is_receiver` BOOLEAN,`run_value`
  DOUBLE,`enabled_pass_credit` DOUBLE). One wide run mart holds both. `value_off_ball_runs` needs a fitted `xt`.
- **gkdv → extend `fct_gk_shot_stopping_pooled`** (per `(player, comp, season)`). Mini-pipeline:
  `build_ghost_frames` (per-frame, home_team_id) → score `delta_das`/`delta_threat_suppression` per frame →
  `aggregate_by_keeper(observations, value_col=…, min_nonzero=20, min_games=2)` → per-keeper cols `mean`,
  `median`, `n`, `n_nonzero`, `n_games`, `gate_eligible` **per value_col** (so `gkdv_delta_das_*`,
  `gkdv_delta_threat_*`). Run `aggregate_by_keeper` partitioned by `(comp, season)` to match the mart grain.
  `[das]` extra required. Caveat: gkdv API is evolving upstream — pin to 4.87.0's surface.
- **Visibility parser (§7.1 dependency).** Build an `action_id`→`polygon` frame from `bronze.statsbomb_360.
  visible_area` (JSON vertices) mirroring `providers.statsbomb.shape_snapshots`; SB360-only; the 8 visibility
  columns populate for StatsBomb rows and are empty until SB360 AC is enabled (they still ship in the schema).

## 8. Phase 3 — Golden rebaselines + independent correctness checks + local gate

The rebaselined goldens are a **regression guard** (they pin whatever `4.87.0` produces), **not** a correctness
validator of this migration. Correctness is established by the independent checks below.

- **Rebaseline (regression guard):** mini-golden (mandatory, `build_ac1_mini_golden.py`, full 151-col equality;
  fix the stale "103" docstring; assert its column set + types match the `_marts__models.yml` contract so a
  mis-typed new column can't pass equality while violating its contract); full/differential goldens
  (`build_ac1_full_golden.py`).
- **Independent invariant checks (keep — these do NOT move on rebaseline):** the differential oracle's
  away-vs-home asymmetry; the cross-provider orientation golden's home-GK-low (`test_frame_orientation_golden.py`,
  re-run after the `id_compat` repoint).
- **New-column range/invariant checks (add):** `obso_epv_source == "xt"` on the `xt=` path (never `synthetic`);
  `press_commitment_closing_speed` within a physical m/s range; `packing_net` sign/range sanity;
  `n_disruptive_runs >= 0`; provenance columns in their closed vocabularies.
- **Expected-shift oracle (deliverable — round-2 R3).** The §11.1b shadow diff is only as strong as the model
  of what *should* change. Build an explicit oracle (a committed module + fixture, e.g.
  `src/tests/action_context/expected_shift_oracle.py`) that, per value-shifting column, declares: the row
  cohort expected to move (away-team; home only under a named y-mirror bug), the expected change *rate* band
  (from the changelog's measured per-provider deltas), and the expected *direction* where one exists (e.g.
  space-creation `created`/`denied` were exchanged for away actions pre-fix). §11.1b asserts the live shadow
  diff falls inside these bands; a shift outside them fails the gate. This is the correctness check that
  0-NULL and rebaselined goldens cannot provide.
- **The 7 CI checks, all green locally:** `ruff check`, `ruff format --check`, `lint-imports`,
  `bump_wheel.py --check`, `pip_audit_ignores.py --check`, **full** `pytest src/tests/`, and `pyright` over all
  four targets.

## 9. Phase 4 — Governance & documentation

- **ADR:** new ADR (`ADR-0XX-silly-kicks-4-86-full-adoption.md`) — cross-cutting dependency bump + new column
  contracts.
- **AI governance (COMMITTED, not conditional — round-1 C6):** the new columns *are* per-player evaluative
  (packing credits passers/names receivers, run-values value runners, press-commitment evaluates defenders),
  and `PER_PLAYER_EVALUATIVE_CARDS` already covers deterministic methods. **Per new family, determine** whether
  it extends an existing card (candidates: run-values → `wf-off-ball-xt`; obso_epv_source → `wf-obso-pausa`) or
  needs a new card (candidates: `wf-packing`, `wf-press-commitment`), then run the full chain: `AI_GOVERNANCE.md`
  §5 + matching HF model card + `governance:` YAML block + re-run `test_ai_governance_md.py`.
- **Academic references:** add authors to `ARCHITECTURE.md` §8 Appendix D + extend
  `test_architecture_md_appendix.py::expected_authors` (packing = Impect; press-commitment; run-values = TF-35).
- **Private-module lint (round-1 C6):** we *intentionally* import 4 private sk modules (the `exec_visibility`
  guard needs them). Add a lint/test that flags any **new** `silly_kicks._`-prefixed import not on the
  documented-intentional allowlist, so the next `_id_compat`-class accidental private dependency is caught at
  adoption, not at the next upstream move.
- **Workflow/HF cards:** update the `compute_action_context` card inventory; refresh HF cards via
  `ingestion.hf_publish.upload_hf_readme`.

## 10. Phase 5 — Merge + post-merge CI (requester-gated)

Squash-merge (`--admin`), one commit. No wheel-consuming job until post-merge `python-ci.yml` green. Apply the
Phase-2 bronze migration with the merge via `scripts/migrations/_runner.py`.

**Wheel-build ordering (round-2 R5).** The wheel force-includes `dbt_project/`, and a **same-version** wheel
will not overwrite the copy already cached in the UC Volume. Phase 0 bumps only the wheel *version string*;
the actual `bump_wheel.py` build + publish must run against the **final post-Phase-2 tree** (after all
`dbt_project/` YAML/contract edits), so the shipped wheel carries the updated dbt project — a Phase-0-time
build would ship stale dbt. One build, at the end, from the merged tree.

## 11. Phase 6 — Live recalculation runbook (driven end-to-end, requester-gated per job)

Ordering: bundled models ship in the wheel → AC recompute consumes them → lakehouse-owned models retrain on the
recomputed surface.

**11.1 — Recompute action-context + SPADL/VAEP + freeze-frames.** Capture per-provider pre-counts → wipe
(`DELETE FROM soccer_analytics.bronze.spadl_action_context`, 4 tracking providers; SB360 held/empty) →
`w.jobs.run_now(job_id=302697362345215, only=["preflight_action_context","compute_action_context"])` (~5.5h)
→ re-run SPADL/VAEP bronze + `bronze.shot_freeze_frames` (SPADL surface changed: `shot_blocked`/`cross_blocked`,
GS carry/dribble fixes; freeze-frames rebuilt via the changed orientation pipeline). **Verification — per-column
null-rate bounds, NOT blanket 0-NULL (round-1 C1):** always-emitted columns (e.g. `das_source`,
`ghost_gk_source`, `is_gk_distribution`) at their documented 0%/off-domain rate; event-conditional columns
(`packing_*`, `press_commitment*`, run-values) within documented bounded null rates (an action that isn't a
line-breaking pass legitimately has null `packing_receiver_player_id`). Row-count == pre-count (additive).

**11.1b — Pre-wipe shadow validation (round-1 C2; widened round-2 R2/R3).** BEFORE any destructive wipe,
recompute a **sample of matches** into a shadow location and diff old-vs-new distributions across **all four
destructively-re-run surfaces**, not just AC: (i) AC columns (OBSO synthetic→xT + the orientation-cycle
columns), (ii) `bronze.spadl_actions` (GS carry/dribble values + the additive `shot_blocked`/`cross_blocked`),
(iii) `bronze.shot_freeze_frames` (the exact input driving the xG-v3 retrain skew), (iv) `vaep_action_values`.
The check is only as strong as its **expected-shift oracle** — a first-class deliverable (Phase 3), not a
runbook aside: per affected column it must encode *where* the shift should land (away-team rows; home rows only
where a y-mirror bug applied) and *how much* (magnitudes consistent with the changelog's measured per-provider
deltas — e.g. `space_creation` ~47% GS / ~60% IDSSE away-row change). A diff that merely says "values moved"
has degraded back to the weak 0-NULL it replaced.

**11.2 — Retrain VAEP + ScoutGPT + xG v3** (corpus-wide, coherent — round-1 C3).

| Model | Verdict | Reason | ADR-013 mart | ADR-012 helpers |
|-------|---------|--------|--------------|-----------------|
| **VAEP** | **RETRAIN** | GS SPADL coord fixes shift its features on GS training rows; retraining yields new weights applied corpus-wide → `vaep_value` moves for **all** providers | `fct_action_values` | `require_mlflow_env`✓ `champion`✓ |
| **ScoutGPT** | **RETRAIN** | Consumes `vaep_value` (verified `scoutgpt_training.py:121`) → moves corpus-wide with the VAEP retrain | none (HF-consumed) | `require_mlflow_env`✓ `champion`✓ |
| **xG v3** | **RETRAIN (required)** | Deep-Sets branch trains on `bronze.shot_freeze_frames` rebuilt via the changed orientation pipeline → train/serve skew on GS+SkillCorner | `fct_shot_xg` | all three ✓ |
| **football2vec v1/v2/360** | **NOT-NEEDED** | Reads action-attribute columns (`action_type`,`start_x/y`,`result`,`period`,`time`), **not** `vaep_value` (verified `train_football2vec.py:118-133`, `football2vec_v2_training.py:88-107`); those attributes are unchanged for public providers → insulated from the VAEP retrain | `fct_player_embeddings*` | n/a |
| **PSxG** | **NOT-NEEDED** | StatsBomb-native goal-line geometry; zero silly-kicks dependency | `fct_shot_psxg` | n/a |
| **xtgk-v2 (NEW, review-2 blocking)** | **FIT (train)** | `possession_value` + `turnover_cost` are NOT bundled (only `retention` is) — must be `.fit()` on the gold action marts. New trainer `scripts/train_xt_gk_v2_hf.py`; the fitted models feed an ADR-013 writer that scores `xt_gk_v2`/`gk_geometry_source` (NOT inline in enrich.py — enrich is bundled-models-only) | `fct_action_context` (via the writer) | all three (new trainer) |

**Order:** §11.1 recompute → retrain VAEP → rebuild `fct_action_values` → retrain ScoutGPT (consumes the rebuilt
`vaep_value`); retrain xG v3 in parallel (consumes recomputed freeze-frames) → rebuild `fct_shot_xg`; **fit
xtgk-v2 (possession+turnover) on the recomputed gold action marts → run the xt_gk_v2 writer** (bootstraps
cleanly: the fit corpus is the SPADL/action gold, which does NOT depend on v2). Each via `hf jobs uv run` per
ADR-012 (`--secrets`, not `--env`); verify each trainer's `_REQUIRED_SK_MIN`==`(4,87,0)` from the shipped wheel
before dispatch.

**11.3 — Rebuild marts (DAG-complete — round-2 R1).** A hand-picked mart list ships a half-refreshed gold layer.
The corpus-wide VAEP retrain moves `vaep_value` for **every** `ref('fct_action_values')` consumer, and the
SPADL re-run ripples to every SPADL-derived mart. Grep already confirms ≥8 stale consumers beyond the original
three: `fct_player_stats`, `fct_vaep_breakdown_agg`, `fct_goalkeeper_stats`, `fct_funnel_stages_agg`,
`fct_gk_tracking_actions`, `fct_shot_psxg`, `fct_xg_predictions_v2`, `int_running_score`. So **enumerate the
downstream set from the DAG, do not hand-pick**:
1. `dbt ls --select <root>+ --resource-type model` for every changed root — `stg_spadl__actions`,
   `stg_action_context__values`, `stg_spadl__action_values`, the freeze-frame staging model, and
   `fct_action_values` (post-retrain) — and union the results. That union is the complete rebuild set.
2. Partition it by TRIGGERED-synced membership (`triggered_synced_marts` var / `SYNCED_TABLES`): TRIGGERED
   marts go through `rederive_synced_marts.py --select <them>` (`--rebuild` for schema changes; NEVER
   `dbt --full-refresh` — ADR-043); everything else via `dbt build --select <them>` (staging views first).
3. Refresh any SNAPSHOT-synced mart in the rebuilt set via `refresh_synced_tables` (§11.5).
A partial rebuild is worse than none — it hides the inconsistency behind a green run.
4. **The Rev-6 marts are in the rebuild set** and mostly ride the DAG selector automatically once they exist in
   the project: extended `fct_action_context` / `fct_match_summary` (bravery) / `fct_gk_shot_stopping_pooled`
   (gkdv), and NEW `fct_action_defensive` / `fct_off_ball_runs` / the long-form defensive-credit mart. **Build
   order:** `fct_action_defensive` reads `fct_shot_xg`, so it (and any dbt selection of it) must build **after**
   the xG-v3 retrain + `fct_shot_xg` rebuild (§11.2). gkdv's build-ghost-frames→score→aggregate pipeline runs
   after the AC recompute (needs the `[das]` extra).

**11.4 — HF republish.** Every changed dataset through the ADR-072 guarded seam (`prepare_public_upload` /
`upload_guarded`), never direct `HfApi`. Update HF cards (`build_provider_configs`).

**11.5 — Refresh synced + verify.** SNAPSHOT tables via `refresh_synced_tables`; verify Taipy pages render the
new columns with scale/direction labels; final null-rate-bounds + row-count parity + shadow-diff-shape sweep.

## 12. Risks, blast radius, rollback

- **Correctness of a value-shifting recompute (biggest risk).** The direction re-key itself is low-risk
  (14/16 self-resolve → mostly deletions). The exposure is that live values change corpus-wide (orientation
  cycle + mandatory synthetic→real-xT OBSO), and **the rebaselined goldens validate stability, not
  correctness** — a wrong-but-stable `4.87.0` value is invisible to a golden regenerated from `4.87.0`. Mitigation
  (round-1 C2): the independent invariant checks (orientation, away-vs-home asymmetry), the new-column
  range/vocab checks, and the pre-wipe shadow distributional-shift check are the real gates; the goldens are
  regression guards only.
- **All-or-nothing `xt=` (§7.3):** the bump and the `xt=` addition must land together (warning-as-error).
- **Big-bang review burden:** phase-isolated commits + the tables here.
- **Destructive recompute:** capture-before-cleanup; the drain is idempotent/additive; §11.1b shadow diff runs
  **before** the wipe; per-column null-rate gate before mart rebuild.
- **Two-repo coupling:** `4.87.0` must be released (tagged + on PyPI) before the lakehouse pin moves (Phase-1).
- **Rollback:** code PR revertible; wheel monotonic; live data recomputed forward (a rollback re-runs the
  recompute on the prior wheel — expensive but deterministic).

## 13. Definition of done

1. `4.87.0` released with the Phase-1 fix; all Phase-0 pins at `4.87.0`; the four guardrail
   tests green.
2. Every broken call site adapted; the two KEEP-`home_team_id` regression tests green; the 7 CI gates green
   locally **and** post-merge CI green.
3. In-scope new columns present with enforced contracts; each within its documented per-column null-rate bound
   (not blanket 0-NULL); the shadow distributional-shift check matches expectation on live data.
4. VAEP + ScoutGPT + xG v3 retrained; **xtgk-v2 fit (possession+turnover) + scored by its writer**;
   football2vec/PSxG justified NOT-NEEDED. All Rev-6 marts built/populated: extended `fct_action_context`
   (incl. the xt_gk_v2 mart-join) / `fct_match_summary` (bravery) / `fct_gk_shot_stopping_pooled` (gkdv), and
   NEW `fct_action_defensive` / `fct_off_ball_runs` / the long-form defensive-credit mart.
5. HF republished via the guarded seam; synced tables refreshed; Taipy verified.
6. ADR + committed governance (per-family card mapping done) + Appendix-D + the new-private-import lint, all
   green.

## 14. Decisions — resolved (round 1) + remaining verify-items

**Scope (Rev 6 — user directive "everything new, nothing deferred"; reversing the round-1 review's defer/exclude recommendations, which are advisory, not scope):**
- `gk_geometry_source` → **IN via xtgk v2 REPLACING v1** (§7.4). The not-construct-validated caveat was stated once and the user's call is v2-replaces-v1.
- Visibility coverage → **IN** (§7.1 + the §7.5 parser); columns ship, empty until SB360 AC enabled.
- Defensive credit + bravery + long-form + gkdv + off-ball-runs → **IN**, materialized **wide-by-grain** (§7.5): per-action defensive-credit in a **NEW `fct_action_defensive`** mart downstream of `fct_shot_xg` (it CANNOT extend `fct_action_values` — `fct_shot_xg` `ref()`s that mart, so an xG-dependent column there is a **dbt cycle**; build order does NOT resolve graph topology — H-B fix); long-form + off-ball-runs as grain-named NEW marts; bravery in `fct_match_summary`; gkdv in `fct_gk_shot_stopping_pooled`.
- Retrain set → **VAEP + ScoutGPT + xG v3** (corpus-wide); football2vec/PSxG NOT-NEEDED.
- `add_off_ball_context` gap → **upstream fix, carried in `4.87.0`** (version assigned by Part Deux).
- Global xT grid → **reuse as-is** (SPADL input change is GS-scoped). *Caveat:* real-xT OBSO now compounds
  corrected GS coords against a GS-stale grid — acceptable, noted.
- Mandatory OBSO value change → acknowledged expected; gated by §8/§11.1b, not only the golden.

**Closed (Rev 5):**
- Target version — resolved to **4.87.0** (Part Deux cut it; no lakehouse cut, no parallel-session conflict).
- Delta re-validation 4.43→4.87.0 — done (4.86.0 `cross_blocked` + 4.86.1 off-ball crash-fix + 4.87.0 research-tooling/NIL; see the Rev-7 note).

**Remaining verify-items (for implementation):**
- Per-column null-rate bounds must be **measured** on a recompute sample to set the documented thresholds
  (§11.1) — placeholder thresholds are not acceptable as the live gate.
- Confirm whether live tracking frames can carry ≥3 `NA`-`team_id` rows (sizes how often the 4.87.0 guard
  actually fires; the fix is correct regardless).
