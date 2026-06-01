# AC-1 profiling results + optimization decision gate (Phase D)

**Method:** `analytics.action_context.profiling.profile_callable` (cProfile, cumulative time) over
the real `run_work_unit` on the committed IDSSE J03WMX p1 anchor fixture — **30 batches, run
fully locally** (no Spark/Databricks). silly-kicks 3.23.0.

## Measured (per-function, cumulative)

| Function | cum_s | % of total | calls |
|---|---|---|---|
| **TOTAL (profiled wall)** | **348.0** | 100% | — |
| `enrich_batch` | 347.8 | ~100% | 30 |
| `_enrich_tracking_match` | 341.9 | 98% | 29 |
| **`add_das` → `simulate_passes_chunked`** | **213.5** | **61%** | 29 |
| `gc.collect` (built-in) | 103.8 | 30% | 2174 |
| `add_shape_graph` | 38.6 | 11% | 29 |
| `add_elastic_sync` | 24.1 | 7% | 29 |
| `add_cover_shadows` | 23.7 | 7% | 29 |

(Unprofiled wall is ~305 s for the same 30 batches; cProfile adds ~14% overhead. The 30%
attributed to `gc.collect` is partly cProfile-inflated but is real — see lever 2.)

## Extrapolation to the timeout

A full IDSSE half is ~283 batches (1.5 M tracking rows / 250 / ~22 objects). At ~10 s/batch
unprofiled that is **~50–55 min/half**, which is exactly why `compute_action_context_iteration`
(1800 s = 30 min) timed out writing zero rows. The timeout is **compute-bound**, not infra.

## §3 cause attribution

- **(a) per-group compute — DOMINANT.** DAS (`get_dangerous_accessible_space` →
  `simulate_passes_chunked`, 429 `simulate_passes` calls) is 61% of wall. shape_graph + elastic +
  cover_shadows add another ~25%.
- **(b) executor starvation — RULED OUT** as the primary cause: this is a single-process local
  run, yet it already takes ~55 min/half. Adding executors cannot fix a per-group cost this large.
- **(c) per-group overhead — minor.** The per-group `pd.DataFrame(actions_records)` rebuild (L1)
  did not surface in the top callees; it is negligible next to DAS.

## §8 decision gate

The binding constraint is **per-group compute, dominated by DAS pass-simulation**. Therefore the
recommended lever is to **reduce DAS cost**, NOT to change cluster size / `concurrency` / chunk
sizing (those address starvation/overhead, which are not the bottleneck).

Concrete levers, in priority order (each warrants its own spec/PR — out of scope for this
foundation PR, which only makes AC-1 runnable + verifiable):

1. **DAS (61%) — the headline.** Options: (a) coarsen the accessible-space simulation grid /
   `simulate_passes` resolution; (b) share the pitch-control/accessible-space surface across the
   DAS + pitch_control + gk_influence + cover_shadows steps (they each re-simulate); (c) compute
   DAS on a downsampled frame subset. This is the silly-kicks "surface-sharing" optimization the
   spec flagged — it lives in silly-kicks, so it is a silly-kicks change.
2. **`gc.collect` (30%).** `_convert_tracking_batch` calls `gc.collect()` per batch (×30) and the
   chain triggers many more (2174 total). These explicit collects were added for the 1 GB UDF cap
   but are costly; profile whether they are still needed at `_FRAME_BATCH_SIZE=250` and drop the
   redundant ones.
3. **elastic (7%) computes 24 s/half and returns all-NaN** (window-dependent; can't align on a
   250-frame batch). Either skip `add_elastic_sync` in the batched path (reclaim the 24 s) or
   source elastic from the dedicated whole-half `elastic_sync_results` pipeline via a join.

## Optimizations applied (quality-preserving — golden byte-identical at every step)

Root cause of the dominant costs: several per-frame **snapshot** enrichments compute their metric
on EVERY frame of the 250-frame batch, then map only the ~3 action-linked frames to actions —
discarding ~98% of the per-frame work. Restricting their input to the action-linked frames (via
`enrich._restrict_to_linked_frames`, mirroring the legacy `tracking_context` DAS bypass) yields
**identical per-action values** (verified: the frozen `golden.parquet` is unchanged) for a large
reduction in work. This is valid only for true per-frame snapshots, NOT window features
(actor-pre-window, off-ball runs, elastic), which still receive full frames.

| Stage | Wall (30 batches) | Extrapolated IDSSE half (~283 batches) | vs 1800 s timeout |
|---|---|---|---|
| Baseline | 366 s | ~58 min | OVER |
| + DAS restricted to linked frames | 92 s | ~14.5 min | under |
| + shape_graph restricted to linked frames | **70 s** | **~11 min** | comfortably under |

Net **~5.2x speedup, zero quality loss.** The 30-min timeout is resolved by these two changes
alone. The `gc.collect` lever (30% in the baseline) was largely **subsumed**: those 2174 collects
were mostly inside the DAS accessible-space simulation, which now runs ~80x less.

## Remaining levers (after the two applied — measured post-optimization breakdown)

`_enrich_tracking_match` is now ~70 s wall (118.7 s profiled). Top residual costs:

| Step | ~cum_s (profiled) | Same all-frames-then-map pattern? |
|---|---|---|
| `add_cover_shadows` | 28.5 | **No** — `_compute_cover_shadow_dict` is ~3.5 calls/batch (already linked-scoped); the cost is `lane_control` (5299 calls), genuinely per-action per passer-receiver pair. Linked-frame filter would NOT help. |
| `add_elastic_sync` | 26.4 | N/A — **100% wasted** (the frame-origin bug below) |
| `add_obso` | 19.3 | **No** (verified) — `_precompute_obso_lookup` iterates per-pass-action and builds a ±window (pre 3 s / post 1 s) around each pass, so it needs contiguous frames. The linked-frame filter would BREAK it. Genuinely per-pass with windowing. |

The pipeline is already comfortably under the 30-min budget, so these are optional. None of the
three residuals is an all-frames-then-map snapshot like DAS/shape_graph were, so the linked-frame
trick does NOT apply to them. Further gains would need different work (e.g. silly-kicks
`lane_control` vectorization for cover_shadows), and the elastic 26 s is the frame-origin bug
(reclaimed once silly-kicks is fixed). DAS + shape_graph were the only all-frames-waste cases.

### elastic — a silly-kicks bug, not a cost to optimize

`add_elastic_sync` spends ~26 s/half and returns **all-NaN** for IDSSE. Root cause (verified on
real data): `align_events_to_frames` computes `nominal_frame = round(time_seconds * frame_rate)`,
assuming 0-based frame numbering, but IDSSE `frame_id` starts at 10000 (period 1) / 100000
(period 2) while `time_seconds` is period-elapsed. The candidate-frame window therefore never
overlaps the real frames -> zero alignments -> all-NaN. Fix belongs in silly-kicks
`_elastic_sync.py` (derive the frame from the frames' own time<->frame_id line). Handoff:
`docs/superpowers/plans/notes/silly-kicks-handoff-elastic-frame-origin-bug.md`.

## Status

AC-1 is **runnable, verifiable, profiled, AND optimized under the timeout** — all quality-preserving.
Deeper levers (silly-kicks surface-sharing / accessible-space re-implementation) are no longer
required to meet the 30-min budget, but remain available if further headroom is wanted.

## Update (2026-05-29 — post silly-kicks 3.27.0 adoption)

The Phase-D profiling above is on silly-kicks 3.23.0 with the lakehouse-side `_restrict_to_linked_frames`
workaround. Since then the optimizations moved **into silly-kicks** and the workaround was removed:

- **DAS + shape_graph linked-frame restriction** → native in 3.25.0 (`add_das`/`add_shape_graph`
  restrict the per-frame precompute when `links` is supplied; bit-identical). Lakehouse workaround deleted.
- **Shared `PitchControlCache`** (3.25.0, TF-7) wired across obso/cover_shadows/gk_influence/
  space_creation/pitch_control_at_action.
- **ghost_gk** added to the chain (Step 12b). 3.24.0 introduced it; 3.26.0's `link_frame_ids`
  restriction made it viable (~47.5 min → ~27 s per 250-batch, ~100×). Without 3.26.0 it was
  ~225 hr/half — the prior "ghost_gk BLOCKER".
- **cover_shadows `detailed=True`** is now the wired default (all 4 call sites). Measured A/B: only
  `max_single_defender_blocking_score` changes (0→1.32 vs the 0→0.19 fixed-cast approximation); the
  other 4 cover columns are byte-identical; ~1.5× cover_shadows cost (uncached per-defender
  counterfactual; not on the critical path).
- **elastic is FIXED, not "all-NaN / 100% wasted"** as the §"Remaining levers" / "elastic — a
  silly-kicks bug" sections above state. silly-kicks 3.25.0 corrected the frame-origin; AC-1 elastic
  now populates (78/97 on the anchor) with correct 10000-based frames. The frame-origin bug that
  remains is in the **legacy** `analytics.elastic_sync` (the `elastic_sync_results` oracle), NOT
  silly-kicks — so elastic is range-checked, not oracle-validated. See
  `memory/project_legacy_elastic_sync_frame_origin_bug.md`.

Golden regenerated on 3.27.0: rows=97, cols=103 (+3 ghost_gk), 0 boundary dups, differential 2/2 green.

## Update (2026-06-01 — real-serverless full-chain profile via `profile_action_context`)

The Phase-D numbers above are **superseded**. They were measured on silly-kicks 3.23.0 *before*
the DAS/shape_graph linked-frame restrictions landed natively (3.25.0) and *before* ghost_gk
was added to the chain (3.24.0). A fresh full-chain profile on the REAL serverless env (new
`profile_action_context` wheel entry point — single-process cProfile of `enrich_batch` on the
driver, no bronze write; silly-kicks 4.1.1, wheel 0.5.6) inverts the picture:

**skillcorner 2011166, 60/210-batch sample, wall 1405 s** (run `106326284274473`):

| Stage | % of chain wall | Hotspot |
|---|---|---|
| **`add_ghost_gk`** | **74.4 %** (1045 s) | `_ghost_gk.predict_density` → `scipy.stats.gaussian_kde.evaluate` (534 calls, 931 s self-time, ~1.74 s/call) |
| `add_elastic_sync` | 6.1 % | `_build_player_ball_distance_lookup` (82 s — looks O(n×m)) |
| `add_cover_shadows` | 4.7 % | |
| `add_obso` | 3.8 % | |
| **DAS** (`add_das`+`get_dangerous_accessible_space`+`simulate_passes`) | **~1.2 %** | — |
| pitch_control | 0.7 % | |

Plus ~14 % in pandas scalar access (`frame.__getitem__` 108 s + `_ixs` 93 s, ~2.1 M calls —
per-row `.iloc`/iterrows, likely inside ghost-GK / elastic-sync).

**The bottleneck is now ghost-GK's `scipy.gaussian_kde`, not DAS.** The 3.25.0 linked-frame
restriction (recorded above) cut DAS from 61 % to ~1 %; ghost_gk's KDE is the new dominant
cost. Implication: a DAS GPU/native rewrite (silly-kicks `2026-06-01-das-native-multibackend`
spec) caps at ~1 % AC-1 whole-chain speedup (Amdahl) — the real lever is ghost-GK's KDE
(vectorize/numba/FFT-KDE/GPU). NOTE the silly-kicks DAS Step-0 ("DAS = 70 % of `get_das`") is
*also* correct — different denominator (the DAS function vs the full 20-stage chain). Sample
over-weights one-time costs, but ghost-GK's cost is per-call (recurring) so the 74 % headline
is robust. Re-run any provider via `scripts/submit_ac1_oneshot.py --profile --match-ids
<provider>:<id>[:period]`; the summary is logged to the driver task log (retrievable via
`jobs.get_run_output`, no UC Volume READ needed) and also dropped as a `.pstats` in the
rendezvous dir for offline deep-dive.

**Generalization — IDSSE J03WMX p1 (25 fps, 60/283-batch sample, wall 1206 s, run 875061375644050):**
ghost-GK dominance holds across providers:

| Stage | skillcorner (10 fps) | IDSSE (25 fps) | GradientSports (30 fps) |
|---|---|---|---|
| `add_ghost_gk` | 74.4% | **68.5%** | **62.2%** |
| `add_elastic_sync` | 6.1% | 8.4% | 10.8% |
| `add_obso` | 3.8% | 6.6% | 8.0% |
| `add_cover_shadows` | 4.7% | 3.9% | 3.0% |
| DAS (`add_das`+sim) | ~1.2% | ~0.9% | ~0.9% |

ghost-GK is the clear #1 on all three; the denser providers shift a little weight into
elastic/obso (more window work) but the headline holds. IDSSE NaN-degrades DAS on ~8% of
batches — benign: IDSSE honestly marks ~33% of p1 frames `ball_status=0` (dead-ball) →
`infer_ball_carrier` correctly yields no carrier → DAS undefined (ADR-003). skillcorner has no
per-frame ball_state so never gates. NOT a bug; see `memory/project_ac1_numba_das_cost.md`.

**GradientSports — match 3840 p1 (40/377-batch sample, wall 467 s, run `1039554735504537`).**
This was the conclusive end-to-end GS validation: AC-1 had **never run** for GS before (four
latent adapter bugs — see ADR-034). With the fixes it SUCCEEDS, and result-health proves
RESOLUTION works (not just no-crash): `das_team`/`das_opponent`/`das_diff` ≈ **48% non-null**
(carriers/possession resolve — pre-fix this was 0%: empty roster dicts + Int64-vs-string id
mismatch), `ghost_gk_x`/`ghost_gk_spread` = **100%**. The ~52% NaN DAS is the normal
possession-undefined-at-action-frame degradation. The full `add_gradientsports_player_ids`
adoption (queued robustness) may raise the carrier-match rate further.
