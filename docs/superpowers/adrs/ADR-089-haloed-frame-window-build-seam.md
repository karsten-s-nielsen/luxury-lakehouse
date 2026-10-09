# ADR-089: Haloed frame-window build seam (window-bounded distributed frame build)

| Field | Value |
|---|---|
| **Date** | 2026-10-09 |
| **Status** | Proposed |
| **Deciders** | Karsten |

## Context

A per-unit pandas frame build (`analytics.action_context.unit_inputs.build_unit_inputs` → silly-kicks `convert_to_frames` + `PreprocessConfig` preprocess) runs inside a serverless `mapInPandas` UDF (~1 GB Python-worker cap). On a dense tracking half the raw group and the built frame are coresident: `gradientsports:10502:1` is raw 436 MB + built 583 MB = **1019 MB** before build transients → `[UDF_PYSPARK_ERROR.OOM]` at `run_stage1_build_and_spill` (ADR-088). The peak is bounded by **unit size**, which is unbounded w.r.t. the worker cap; idsse (477 MB) and metrica (452 MB) dense halves are at risk too.

Separately, the AC drain already sub-batched by `frame_batch_id = floor(frame / frame_batch_size)` with **no halo**, so silly-kicks' composed savgol velocity (reach `window_frames−1`) and bounded-gap interpolation saw only each batch's own frames → degraded velocity within the composed reach of every batch boundary (a latent correctness gap, not just memory).

## Decision

A reusable **haloed frame-window partitioning** seam bounds the UDF-worker peak by **window size**, not unit size, and makes velocity/interpolation continuous across window boundaries.

- **Pure core** `src/analytics/action_context/frame_windows.py` (no pyspark): `halo_frames(provider, hz)`, `assign_window_ids(frames, …)`, `build_windowed(window_rows, build_fn, …)`, `SEAM_PROVIDERS`, `provider_hz`. **Spark dispatch** `src/ingestion/frame_window_dispatch.py`: `assign_frame_windows` (dense `dense_rank` frame ordinal → `_window_id`, halo replication) + `windowed_build_sdf`. Layering enforced by import-linter (ingestion→analytics).
- **Window on the dense DISTINCT-frame ordinal** (`floor(ordinal / target_window_frames)`), NOT raw row index (a frame ≈ 22 player rows).
- **Composed halo** `H = (window_frames − 1) + max_gap_frames` per provider (interpolate → smooth-savgol → velocity-savgol-on-`x_smoothed`; the reaches are ADDITIVE, never `max(...)`), sourced from the live sk `PreprocessConfig`. A module-level drift sentinel pins the sk INPUT config (`sg_window_seconds`/`sg_poly_order`/`max_gap_seconds`) and derives H, failing loud on sk drift (ADR-067 lockstep).
- **Three-site adoption:** tracking-marts Stage-1 (ADR-088), shot_freeze, the AC drain. Seam providers = **idsse + skillcorner + gradientsports**. **metrica EXCLUDED** — its convert is window-unsafe (`unit_inputs.py` rebases `time_seconds` off the passed set's `period_min`), it does not OOM (sparse), and it is legacy; it keeps the whole-unit / floor-batched path everywhere.
- **AC drain (value-change, A2 "build-core+halo → trim-to-core"):** replace the halo-less `frame_batch_id` grouping with the dense-ordinal `_window_id` + halo. `enrich_batch` builds velocity on `core+halo` (continuous) then **trims the built frames to CORE before the enrich chain**, so the value-change is isolated to boundary VELOCITY (+ its correct propagation); the window-dependent enrich features (obso_peak, pausa_*, elastic_sync) keep the exact core batch window — unchanged from pre-seam, honouring their ADR-047 batch-scope. Single-owner de-dup moves to ordinal space: `floor(searchsorted(sorted_frames, est_frame) / target) == _window_id`, with the dispatcher threading a per-period sorted-distinct-frame array (mirrors the M13 global anchor). Both dispatch paths (`run_work_unit`, `_process_tracking_match`) share `enrich_batch`, kept in lockstep by `test_ac_seam_in_both_dispatchers`.

## Neutrality is a MEASURED contract (§4.2a), not an assumed byte-identity

`derive_velocities` fills NaN savgol inputs with an UNCAPPED `np.interp`, so the windowed build is byte-identical to the whole-unit build only where detection gaps near a boundary are `≤ max_gap`; a `>max_gap` residual within `(window_frames−1)` of a boundary is characterized + measured. **Conservative rule (owner):** per provider, `frac_affected == 0` → neutral, no golden change; `> 0` → value-change + regen (no compare-atol relaxation).

**T8 pre-merge measurement (real dense halves, driver-side whole vs windowed):** idsse `frac_affected=0` and skillcorner `frac_affected=0` at both the AC (250) and tracking-marts (20000) targets → **NEUTRAL**. gradientsports `frac_affected>0` → value-change. The GS residual is **dominated by a pre-existing silly-kicks ball-velocity pathology** (up to 817 m/s), present in the whole-unit build and unrelated to the seam; GS outfield-player velocity shifts ≤ 0.36 m/s (the genuine small boundary residual). Root cause + proposed sk fix: `D:\Development\_reviews\2026-10-09-silly-kicks-velocity-gap-contamination-handoff.md` (sk `smooth_frames`/`derive_velocities` savgol on row order ignore `frame_id` gaps; the ball's long non-detections contaminate across the gap).

**Owner decision (2026-10-09):** ship the seam (it fixes the GS OOM — the cycle's purpose); **DEFER the GS data re-materialize** to ride the silly-kicks velocity-fix cycle (that fix is model-affecting — ghost-GK / elastic retrain — so GS is recomputed ONCE with seam + corrected velocity; GS AC is gated/not-live, so deferring ships nothing stale). idsse + skillcorner ride the already-scheduled sk-4.128 §8 re-materialize.

## Alternatives considered

| Option | Why rejected |
|---|---|
| In-UDF generator windowing (hold the whole raw group, yield slices) | peak bounded by unit size, not window → busts on denser data; band-aid |
| Keep AC's halo-less floor-division for tracking-marts/shot_freeze | reproduces AC's latent boundary degradation in the new marts |
| Spark-native velocity (no pandas build) | violates the sk-owned-build contract (ADR-053/067) |
| AC "no trim" (A1 — enrich on core+halo directly) | silently widens the batch window by 2H → redefines the ADR-047 batch-scoped features (obso/pausa) beyond the intended velocity correction |

## Consequences

**Positive.** Worker peak bounded by `target_window_frames` (fit-profile: GS ~21 k frames → ~430 MB, vs 1 GB cap); the GS Stage-1 OOM is fixed. AC batch-boundary velocity is corrected (the halo). One reusable seam across three sites; neutrality is machine-measured, not assumed.

**Costs / negatives.** Halo replication adds `<0.2%` rows (H ≪ window). A per-window build + the AC core-trim (a `frame_id`-set filter on the built frame). Neutrality for fragmented-broadcast providers is provider-measured, not guaranteed. metrica stays whole-unit (excluded). The GS data recompute is deferred to the sk velocity-fix cycle; GS AC/marts carry the pre-existing ball-velocity artifact until then (no regression — GS already has it, and GS AC is gated).

**Validation.** Pure neutrality + halo-sufficiency tests (`tests/action_context/test_frame_windows.py`); Docker real-Spark dispatch (`tests/tracking_marts/test_windowed_stage1.py`, `tests/action_context/test_ac_windowed_dispatch_spark.py`); AC interior/boundary/single-owner (`test_ac_windowed_seam.py`); the ADR-087 preflight canary on `gradientsports:10502:1` (memory ≤ 800 MB/window — the OOM-repro oracle). The T8 measurement + GS root-cause artifacts live under `D:\Development\_reviews\`.
