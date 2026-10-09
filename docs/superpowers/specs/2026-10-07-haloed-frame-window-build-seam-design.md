# Haloed Frame-Window Build Seam — Design

**Status:** APPROVED — rev3 (both reviewers APPROVE: A R3 + B R3; reports `…-spec-r3{,-B}.md`). Owner rulings recorded below. Ready for writing-plans.
**Date:** 2026-10-07

### Owner rulings (2026-10-07)
- **metrica EXCLUDED from the seam.** Owner: metrica's 3 matches are ancient / no value; "not doing metrica at all." The T6 gate also found metrica's convert window-UNSAFE (`unit_inputs.py:19` rebases `time_seconds` off the passed set's `period_min`), and metrica does NOT OOM at whole-unit grain (fit-profile: raw 77 MB + built 452 MB < 1 GB). So metrica is never a seam target: it keeps its existing whole-unit / current path at every site (tracking-marts, shot_freeze already excluded it, AC keeps its current metrica path — no regression). The seam serves **idsse + skillcorner + gradientsports**.
- **AC correction (§4.5c / Q1): TAKE NOW, ride the re-mat.** Adopt the seam at AC with the boundary-velocity correction live; regen AC goldens; ride the already-scheduled sk-4.128 §8 re-materialize (no second wipe; confirmed not yet run).
- **§4.2a residual rule: CONSERVATIVE DEFAULT.** `frac_affected == 0` → byte-identical neutral, no golden change; any non-zero residual → value-change + regen that provider's goldens. NO compare-atol relaxation (the pre-authorize option is DECLINED).

### rev3 changelog (R2 should-fix / consider)
- **TM-HFW-R2-02 / HFW-SPEC-06 (both):** the §4.2a tolerance is an **OWNER quality-bar decision** — session PROPOSES `frac_affected ≤ 1e-4` & `max|Δspeed| ≤ 0.01 m/s`, owner RATIFIES (same class as AC Q1). AND the golden/oracle compares are ~exact (mini-golden `allclose(atol=1e-6, rtol=1e-4)`; tracking-marts oracle `assert_frame_equal` ≈1e-8) — so a sub-0.01 m/s residual STILL fails them. Corrected decision rule: "neutral, no golden change" holds only if **`frac_affected == 0`** (band empty) under the existing compares; any non-zero residual is a value-change+regen UNLESS the owner ratifies relaxing the compare atol to the bound. §4.2a.
- **TM-HFW-R2-01 (B):** convert_to_frames window-safety ESTABLISHED analytically (not just tested): §4.1a. idsse/`sportec.convert_to_frames` — orientation = per-unit flag applied per-frame, geometric backstop a no-op on correct meta, per-`period_id` groupby, no resample. Per-provider convert feasibility is a plan gate BEFORE adoption (a whole-unit convert op = seam INFEASIBLE for that site).
- **HFW-SPEC-07 (A):** convert-file cite corrected/generalised (§2) — the chain lives in each provider's sk converter (idsse → `silly_kicks.tracking.sportec`; metrica/skillcorner via `sk_frame_adapters`), not a single `sportec:206`.

### rev2 changelog (both findings verified in sk 4.128 source before revising)
- **HFW-SPEC-01 (BLOCKING, both):** halo formula corrected from `max(savgol_radius, max_gap)` to the **composed reach** `(window_frames−1) + max_gap_frames` (interp → smooth-savgol → velocity-savgol-on-`x_smoothed`; two chained savgol passes + the preceding interpolation sum, they do not `max`). Confirmed: `_velocity.py:42` reads `x_smoothed`; `_smoothing.py` + `_velocity.py` share `window_frames`; the interp→smooth→velocity chain runs inside each provider's sk `convert_to_frames` (idsse → `silly_kicks.tracking.sportec` at `sportec.py:200-206`; metrica/skillcorner via `sk_frame_adapters` → the sk metrica/kloppy converters) — not a single file. H is order-independent (additive reach). §4.2.
- **HFW-SPEC-01/02 (unbounded interp):** `_velocity.py:76-94` fills savgol-input NaNs with an **uncapped `np.interp`** (nearest non-NaN at any distance). A fixed halo therefore does NOT guarantee byte-identity when a `>max_gap` detection gap sits within `(window_frames−1)` of a window boundary. The rev1 "byte-identical NEUTRAL" claim is dropped; replaced with a **measured neutral-in-practice** contract + a characterized residual band (§4.2a). Applies to tracking-marts + shot_freeze, not only AC.
- **HFW-SPEC-02 (B):** neutrality now covers `convert_to_frames` (orientation/identity/resample), not only preprocess — the end-to-end `build_unit_inputs` neutrality test covers convert+preprocess; §6.
- **tests (both):** neutrality/halo-sufficiency tests must run the FULL pipeline (smoothing ON) + plant a boundary-straddling bounded gap AND a `>max_gap` gap (§6) — else vacuous.
- **AC scope Q1 DEFERRED** until H is ratified (downstream of HFW-SPEC-01). `:1908`→`:1909` fixed. Per-window peak re-measured with the larger halo (§4.3a).
**Author:** session (karsten@skyt.com)
**Related:** ADR-088 (tracking-marts two-stage), ADR-067 (velocity via sk preprocess seam), ADR-087 (preflight canary), ADR-045 (repartition+mapInPandas transport), ADR-047 (frame batching)
**Fit-profile evidence:** `D:\Development\_reviews\2026-10-06-tracking-marts-fit-profile.md`

## 1. Problem

A per-unit pandas frame build (`analytics.action_context.unit_inputs.build_unit_inputs` → sk `convert_to_frames` + `PreprocessConfig` preprocess) runs inside a serverless `mapInPandas` UDF (~1 GB Python-worker cap). On a **dense tracking half** the whole-unit build's peak exceeds the cap.

**Confirmed (ADR-087 canary + T9 fit-profile, 2026-10-07):** `gradientsports:10502:1` — raw group **436 MB** + built frame **583 MB** = **1019 MB coresident before build transients** (savgol copies). The preflight canary failed `[UDF_PYSPARK_ERROR.OOM]` at `run_stage1_build_and_spill`. Metrica builds 452 MB, IDSSE 477 MB — every dense half is at risk at whole-unit grain. The peak is bounded by **unit size**, which is unbounded w.r.t. the worker cap.

### 1.1 The three affected call-sites (differ materially)

| Site | Current grain | Memory today | Velocity continuity |
|---|---|---|---|
| **tracking-marts Stage-1** (`tracking_marts_dispatch.run_stage1_build_and_spill`) | **whole-unit** (`(match, period)`) | OOM on dense (583 MB built + 436 MB raw) | correct (whole sequence) |
| **shot_freeze** (`shot_freeze_frames` mapInPandas build) | **whole-unit** (`period`) | OOM-risk on dense IDSSE (onboarded in #599) | correct (whole sequence) |
| **AC drain** (`ingestion.action_context._make_action_context_udf`) | **frame-batched** `frame_batch_id = floor(frame/frame_batch_size)` (ADR-047) | bounded (per-batch) — NOT OOM-prone | **DEGRADED** — halo-less floor-division batching: the first/last ~savgol-radius frames of every batch derive velocity from within-batch neighbours only |

**AC's existing batching has no halo.** `action_context.py:1909` assigns `frame_batch_id = floor(frame_col / frame_batch_size)` and groups `[match_id, period, frame_batch_id]`; each group runs `enrich_batch` independently, so sk's composed savgol velocity/smoothing (reach `window_frames−1`) and bounded-gap interpolation see only the batch's own frames. At each batch boundary ~`(window_frames−1)` frames per side carry degraded velocity (the composed reach, §2). This is a **latent correctness gap**, not just a memory concern.

## 2. Root cause

`build_unit_inputs` peak = raw group + built frame + preprocess transients, all coresident, bounded by unit size. sk preprocess (`silly_kicks.tracking.preprocess`: `_interpolation`, `_smoothing`, `_velocity`) runs per-player-per-period as a **COMPOSED chain of stencils** (verified in sk 4.128, 2026-10-07):

1. `interpolate_frames` — linear gap-fill ≤ `max_gap_seconds` (bounded). Reach `max_gap_frames`.
2. `smooth_frames` — savgol of `window_frames` → `x_smoothed`/`y_smoothed`. Reach `savgol_radius = (window_frames−1)/2`.
3. `derive_velocities` — savgol-deriv of `window_frames` run **on `x_smoothed`** (`_velocity.py:42`). Another `savgol_radius`.

So velocity at frame *i* depends on `x_smoothed[i ± savgol_radius]`, each of which depends on `interp(x)[i ± 2·savgol_radius]`, each of which depends on `x[i ± (2·savgol_radius + max_gap_frames)]`. The composed reach is **additive**, ≈ `(window_frames−1) + max_gap_frames` — NOT `max(...)` (rev1 error, HFW-SPEC-01).

**One stencil is UNBOUNDED.** `derive_velocities` (`_velocity.py:76-94`) fills remaining savgol-input NaNs with an **uncapped `np.interp`** (nearest non-NaN at ANY distance; the originally-NaN frames are re-NaN'd at `:127-129`, but observed frames within `savgol_radius` of a `>max_gap` gap get a velocity computed from the unboundedly-interpolated values). No fixed halo bounds this when a `>max_gap` gap sits near a window boundary bracketed by non-NaN beyond the halo — common on fragmented broadcast tracking (skillcorner/GS). So the windowed build is byte-identical to the whole-unit build only for sequences whose detection gaps near a window boundary are **≤ max_gap**; the `>max_gap` case needs the §4.2a treatment.

Therefore the build is window-safe for the **bounded** case with a halo `H = (window_frames−1) + max_gap_frames`, and the `>max_gap` residual is characterized + measured (§4.2a), not assumed away.

## 3. Goal / Non-goals

**Goal.** One reusable, memory-bounded, correctness-preserving distributed build seam — **haloed frame-window partitioning** — adopted at all three sites. Worker peak bounded by **window size** (tunable), independent of unit density; velocity/interpolation correct across window boundaries via a halo.

**Non-goals.** (a) Re-implementing sk's build in Spark SQL — the build stays the sk pandas build (ADR-053/067). (b) Changing the tracking-marts Stage-2 scorers or AC's enrich logic. (c) Changing `frame_batch_size` tuning policy beyond adding the halo.

## 4. Design — the haloed frame-window seam

### 4.1 Canonical pattern
Overlapping windowed partitioning with a halo (the standard distributed stencil solution): partition each unit's frames into contiguous windows; **replicate** a halo of `H` frames from each side into the adjacent window; build each `(core + halo)` window via the sk build; **trim the halo**, keep only core rows; union the cores = the whole built frame, byte/value-identical to the whole-unit build.

### 4.1a Convert-stage window-safety (feasibility premise, TM-HFW-R2-01)
`build_unit_inputs = convert_to_frames + preprocess`. §2 establishes preprocess as a bounded stencil (+ the §4.2a unbounded-interp residual); the convert stage must ALSO be window-safe or the seam is **infeasible** for that site (not merely non-neutral — a whole-unit convert op cannot be reconstructed per window). Established for **idsse** (`silly_kicks.tracking.sportec.convert_to_frames`, read 2026-10-07):
- **Orientation** is a per-UNIT constant — `home_team_start_left(_extratime)` resolved from `meta` on the driver and passed in (`sportec.py:62`), applied per-frame; the `finalize_orientation` geometric backstop (`sportec.py:137`) is a **byte-identical no-op when the flag is correct** (production, resolved meta) and only self-corrects a WRONG flag from GK geometry (which would be whole-sequence-dependent → window-unsafe). The seam passes the resolved per-unit flag to every window → the backstop is a no-op → window-safe. A per-window assertion that the backstop did not fire (flag unchanged) guards the residual risk.
- **Groupby is per-`period_id`** (`sportec.py:170`); windows are within a period → safe.
- **No sequence-length resample** in the convert path.
**Plan gate:** verify the SAME per provider (metrica/skillcorner → `sk_frame_adapters` → sk metrica/kloppy converters; gradientsports) BEFORE adopting the seam there. A provider whose convert has a genuine whole-unit op is excluded from the seam (kept on a bounded alternative), surfaced, not silently shipped.

### 4.2 Halo radius `H` (per provider) — composed reach
`H = (window_frames − 1) + max_gap_frames` (the composed stencil reach from §2: smooth-savgol ∘ velocity-savgol = `2·savgol_radius = window_frames−1`, plus the preceding `interpolate_frames` `max_gap_frames`). `window_frames` and `max_gap_seconds` are sourced at runtime from sk `PreprocessConfig` per-provider defaults (`silly_kicks.tracking.preprocess._provider_defaults_generated` / `_config`; `window_frames = max(round(sg_window_seconds × hz)|1, sg_poly_order+2)`), `max_gap_frames = ceil(max_gap_seconds × fps)`. A module-level assertion pins the DERIVED `H` per provider and fails loud if sk changes `sg_window_seconds` / `max_gap_seconds` / the pipeline order (ADR-067 lockstep). Provider fps differs (idsse 25, gs/sc 10, metrica derived) → `H` is per-provider. (rev1's `max(savgol_radius, max_gap_frames)` was 2–3× too small — GS ~5 vs true ~9; metrica ~15 vs true ~39.)

### 4.2a The unbounded-`np.interp` residual + the neutrality contract
`H` makes the windowed build byte-identical to the whole-unit build for every sequence whose detection gaps near a window boundary are `≤ max_gap`. It does NOT cover a `>max_gap` gap sitting within `(window_frames−1)` of a boundary (the uncapped `np.interp` reaches across it to the nearest non-NaN, which may lie beyond `H`). That residual is **per-player** (gaps differ by player, so window boundaries cannot be aligned to "the" gap — a per-frame boundary serves all players), so it cannot be eliminated by boundary placement; it must be measured.

**Neutrality is therefore a MEASURED contract, not an assumed byte-identity:**
- **Fully neutral (byte-identical):** all frames except those within `(window_frames−1)` of a window boundary AND within `(window_frames−1)` of a `>max_gap` gap. Proven by the §6 bounded-gap test.
- **Residual band:** frames meeting both conditions. The plan **MEASURES** per provider, on real dense bronze, `frac_affected` (fraction of built rows in the band) and `max |Δvelocity|` vs the whole-unit build.
- **Decision rule (rev3 — the compares are ~exact, so this is sharper than rev2):** the committed goldens/oracles compare ~exactly (mini-golden `allclose(atol=1e-6, rtol=1e-4)`; tracking-marts oracle `assert_frame_equal` ≈ 1e-8). A residual of ANY magnitude above those atols fails them. Therefore, **per provider:**
  - `frac_affected == 0` (no `>max_gap` gap within `(window_frames−1)` of any boundary in the corpus) → **byte-identical, no golden change**. This is the only path to "neutral, no regen" under the existing compares.
  - `frac_affected > 0` → the windowed build is a **value change** for that provider → regen its goldens + ride the re-materialize (same disposition as AC, §4.5c) — EVEN IF `max|Δspeed|` is tiny, because the compares will not tolerate it.
- **OWNER-RATIFIED quality bar (TM-HFW-R2-02 / HFW-SPEC-06):** the session PROPOSES a tolerance `frac_affected ≤ 1e-4` AND `max|Δspeed| ≤ 0.01 m/s` under which the residual could be ACCEPTED (by relaxing the golden/oracle compare atol to that bound) rather than regenerating — but accepting sub-golden non-neutrality in shipped marts is a **quality-bar decision for the owner**, not the session's (same class as AC Q1, §9). Absent owner ratification, the default is the sharp rule above (`frac_affected==0` or regen).

This replaces rev1's unconditional "byte-identical NEUTRAL": tracking-marts + shot_freeze are neutral-no-regen **iff `frac_affected==0`** under the existing compares; otherwise value-change+regen (or an owner-ratified atol relaxation). The spec commits to measuring + to the sharp rule; the tolerance path is an explicit owner item.

### 4.3 Window assignment + halo replication (Spark level)
1. Order frames per `(match, period)` by the provider frame column (`frame`/`frame_num`); assign `window_id = floor(frame_ordinal / target_window_frames)` (dense ordinal over DISTINCT frames, not raw row index — a frame has ~22 player rows).
2. Emit each frame row into its own `window_id` as **core**, and into the neighbouring `window_id`(s) as **halo** when within `H` frames of a boundary (a Spark transform duplicating boundary frames with the neighbour `window_id` + `is_halo=true`).
3. `repartition(N, match_id, period, window_id).sortWithinPartitions(match_id, period, window_id, frame)`; each group = one `(core+halo)` window.
4. The per-group build runs the sk build on `(core+halo)`, then **drops `is_halo` rows**, yields the core built slice.
5. Union of core slices per unit = the whole built frame.

`target_window_frames` is a tunable constant (fit-profile: ~21 k for GS → ~430 MB peak). Tuned per provider; canary-validated.

### 4.3a Memory ceiling
Per-window peak ≈ `(core+halo)` raw + `(core+halo)` built + transients — bounded by `target_window_frames`, NOT unit size. The composed halo (`H ≈ 9` GS, `≈ 39` metrica frames) is **≪ window** (~21 k frames), so halo replication adds <0.2% rows — the per-window estimate is essentially unchanged by the rev2 halo correction: GS at 21 k frames ≈ 109 MB raw + 146 MB built + ~150 MB transients ≈ **~430 MB** (vs the 1 GB cap). Confirmed in the fit-profile/canary (HFW-SPEC-05). Denser future data → lower `target_window_frames` → still fits.

### 4.4 Seam API (pure core in `analytics.action_context`; `ingestion` dispatches)
- `halo_frames(provider) -> int` — from sk `PreprocessConfig` (asserted).
- `assign_frame_windows(raw_sdf, provider, *, target_window_frames) -> raw_sdf'` — adds `window_id` + replicates halo rows (`is_halo`). Pure Spark transform (`ingestion`, pyspark).
- `build_windowed(group_core_plus_halo, build_fn) -> core_built` — pure pandas: run `build_fn`, drop halo rows. Unit-testable on fixtures (no Spark).
- The dispatch helper wraps `assign_frame_windows` + `repartition…mapInPandas(build_windowed via _make_streaming_group_mapper, keys=[match_id, period, window_id])`.

### 4.5 Per-site adoption

**(a) tracking-marts Stage-1** — replace the whole-unit `run_stage1_build_and_spill` group with the windowed seam. Stage-2 scorers are unchanged (they still read the whole reassembled spill; sk `batch_size` bounds them). **Requirement: neutral-in-practice per §4.2a** — byte-identical for ≤max_gap boundaries; the `>max_gap` residual MEASURED per provider → neutral if within the §4.2a bound, else value-change+regen for that provider. Today's whole-unit build is the reference.

**(b) shot_freeze** — same substitution for its single-output build. **Requirement: neutral-in-practice per §4.2a** vs the whole-unit build. Enables dense IDSSE without OOM. (shot_freeze needs no spill — §9 Q4.)

**(c) AC drain** — replace the halo-less `frame_batch_id = floor(...)` grouping with the haloed `window_id` seam (same bounded memory, now with a halo). **This is a VALUE CHANGE, not neutral:** the halo CORRECTS the batch-boundary velocity/interpolation that floor-division batching degraded. Consequences:
  - AC enriched outputs shift at former batch boundaries (a correctness improvement).
  - Requires AC **golden regen** (mini + full) + a **re-materialize of `spadl_action_context`** and everything downstream of AC velocity (das_*, ghost-GK, xshot/xcross, team-shape, etc.) — i.e. the SAME re-materialize the sk-4.128 cycle already schedules (spec §8 of `2026-10-06-tracking-marts-executor-refactor-sk4128`). This change should RIDE that re-materialize, not trigger a second one.
  - AC is the highest-volume, most-tested path → strongest parity/validation bar (see §6).
  - **Open scope question for review:** is the boundary-velocity correction in-scope for this cycle, or should AC adopt the seam in **memory-neutral mode first** (halo added but boundary values pinned to the current degraded result via a compatibility flag) and take the correctness improvement as a separate, explicitly-approved change? Default recommendation: take the correction now and ride the already-scheduled re-materialize, since a halo-less batch boundary is a latent defect and a mixed-vintage AC corpus is worse. **Flagged for the owner.**

## 5. Why not the alternatives
- **In-UDF generator windowing (yield slices, whole raw group coresident)** — floor is the whole raw group (436 MB) held by the mapper; ~730 MB peak, ~270 MB margin; bounded by unit size, not window → busts on denser future data. Band-aid, not reusable/scalable.
- **Keep AC's halo-less floor-division for tracking-marts/shot_freeze** — reproduces AC's latent boundary degradation in the new marts. Rejected.
- **Spark-native velocity (no pandas build)** — violates the sk-owned-build contract (ADR-053/067). Rejected.

## 6. Testing & correctness
- **Neutrality — bounded case (tracking-marts, shot_freeze):** `build_windowed` union across windows == whole-unit `build_unit_inputs().frames`, value-identical, per provider, on committed fixtures forced to span ≥2 windows + a boundary (`target_window_frames` low). **The fixture MUST run the FULL pipeline — smoothing ON (`PreprocessConfig(derive_velocity=True)`), the production config — and plant a detection-NaN gap ≤ `max_gap` straddling a window boundary**, else the test passes vacuously on gap-free, smoothing-off data (HFW-SPEC-03, both reviewers). This is end-to-end `build_unit_inputs` so it covers `convert_to_frames` (orientation/identity/resample) + preprocess, not preprocess alone (HFW-SPEC-02/B).
- **Halo sufficiency (non-vacuous):** a frame exactly `H = (window_frames−1)+max_gap_frames` from a boundary has identical velocity windowed vs whole; `H−1` DIFFERS (proves the composed reach, not an over-sized halo).
- **Residual characterization (the `>max_gap` case, §4.2a):** a fixture planting a `>max_gap` gap within `(window_frames−1)` of a boundary — assert the divergence is confined to the predicted band; and the **plan measures `frac_affected` + `max |Δspeed|` per provider on real dense bronze** and applies the §4.2a decision rule (neutral-in-practice vs value-change+regen).
- **AC value-change characterization:** quantify the boundary-velocity delta (how many rows move, by how much) vs the current halo-less output; regen AC goldens; record in the re-materialize artifact (capture-before-cleanup).
- **Memory:** re-run the ADR-087 preflight canary on `gradientsports:10502:1` (reliable OOM repro) → `drain_canary_ok`; fit-profile the per-window peak per provider ≤ 800 MB.
- **Live parity (AC):** a before/after on a representative dense unit, confirming only the boundary frames move.
- All seven local gates + `validate_workflow_cards`; full `uv run pytest`.

## 7. Risks
- **R1 — AC regression (highest).** AC is the core daily drain. The grouping change + halo must not alter non-boundary rows. Mitigation: neutrality test on interior rows; the value change is isolated to boundary frames + characterized.
- **R2 — halo radius drift.** If sk changes `window_frames`/`max_gap_seconds`, `H` must track it. Mitigation: source from `PreprocessConfig` at runtime + an assertion sentinel.
- **R3 — window ordinal vs raw row index.** `window_id` MUST be on the dense DISTINCT-frame ordinal, not raw row index (a frame = ~22 rows). A bug here mis-sizes windows. Mitigation: explicit test.
- **R4 — halo replication cost.** Duplicating boundary frames inflates shuffle by ~`2H/window` fraction (small for window≫H). Measured in the fit-profile.
- **R5 — provider frame-column + fps variance.** `frame`/`frame_num`, fps per provider. Mitigation: per-provider halo + the existing `_frame_col`.
- **R6 — `>max_gap` residual exceeds the §4.2a bound (unbounded-interp).** If a fragmented provider (skillcorner/GS broadcast) measures above the tolerance, tracking-marts + shot_freeze become value-changing for that provider → golden regen + re-materialize there too (larger blast radius than "neutral refactor"). Mitigation: the §4.2a measurement is a plan gate BEFORE implementation commits to "neutral"; the decision rule is pre-agreed so the outcome is deterministic, not negotiated late.

## 8. Rollout / approval gates
- One feature branch; spec → 2 independent reviews → plan → 2 reviews → implement INLINE (per standing rules).
- Commit only on explicit approval; each of commit / push / PR / merge a separate approval.
- AC re-materialize RIDES the sk-4.128 cycle's already-scheduled §8 re-materialize (no second wipe); its execution is a post-merge operator step under separate approval.
- No wheel-consuming job until post-merge main CI green.

## 9. Open questions for the reviewers
1. §4.5(c): take the AC boundary-velocity correction now (ride the scheduled re-materialize) vs memory-neutral-first with a compatibility pin? (Owner-flagged; recommendation = take it now.) **Now downstream of the rev2 H fix** (HFW-SPEC-04): the correction magnitude = the composed-reach band + the §4.2a residual, so size it with the §4.2a measurement before settling. Confirm the sk-4.128 §8 re-materialize has NOT run yet (operator steps not started as of 2026-10-07 → AC can still ride it).
2. Seam home: `analytics.action_context` pure core + `ingestion` Spark dispatch — correct layering (import-linter: ingestion→analytics OK)?
3. `target_window_frames` as a per-provider constant vs a single global default tuned to the densest provider?
4. Should shot_freeze (currently single-output, no spill) reuse the exact Stage-1 spill seam, or window-build-in-place (it doesn't need a spill — its output is the snapshot, not the frames)?
