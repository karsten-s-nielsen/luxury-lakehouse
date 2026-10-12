# ADR-090: Window-bounded tracking-marts Stage-2 scoring (haloed scoring windows + injected whole-unit invariants)

| Field | Value |
|---|---|
| **Date** | 2026-10-12 |
| **Status** | Proposed |
| **Deciders** | Karsten |

## Context

ADR-088 split the per-unit tracking-marts work into two Spark stages so the frames live on executors, not the 16 GB driver: Stage 1 builds the oriented frames once and spills them to a UC-Volume Parquet (window-bounded by ADR-089), and Stage 2 scores six per-mart passes that read the spill back. Stage 2, however, still re-groups the spill by `(game_id, period_id)` — **the whole unit in one `mapInPandas` group**. On a dense skillcorner half the whole-unit built frame (~0.64 GB float32 at sk 4.133) is resident in that one scoring UDF group *on top of* the scorer's working set (off_ball / defensive-credit per-action grids; the gkdv ghost-GK DAS, which is doubled — actual + ghost pitch-control grids per scored frame). The sum exceeds the serverless ~1 GB UDF-worker cap on **8 dense skillcorner units** (`1204769p2, 1374493p2, 1463749p1, 1562415p1, 1562415p2, 1654689p2, 2015213p1, 2015213p2`) → `[UDF_PYSPARK_ERROR.OOM]` in `build_stage2_mart_sdf`. The peak is bounded by unit size, which is unbounded w.r.t. the cap.

The difficulty specific to **scoring** (vs the ADR-089 build) is that three of the six marts depend on **whole-unit estimators** that change if re-computed on a sub-window:

- `resolve_defended_goals` (the GoalMap — mean GK x per team) feeds `rest_defense`, `gk_decision`, `gkdv`. Re-estimated per window it is a smaller-sample estimate (skillcorner ~7.1% of frames flip the defended end under a per-frame estimate).
- `infer_ball_carrier` (stateful hysteresis, γ=0.25) feeds the gkdv DAS arms; re-estimated per window the boundary state resets.
- the gkdv `possession_stride` survivor set (which keeper-frames are scored) is a whole-unit rank; re-ranked per window it keeps different frames.

So a naïve "just window Stage-2 like Stage-1" would silently change the metric.

## Decision

Extend the ADR-089 haloed frame-window seam from Stage-1 **build** to Stage-2 **scoring**, and inject the whole-unit estimators so every window reproduces the whole-unit metric byte-for-byte.

- **Per-mart scoring halo** `score_halo_frames(mart, provider)` (pure, `frame_windows.py`): off_ball = `ceil(pre_seconds·hz)` (the 1.5 s run look-back), the action-linked marts = `ceil(link_tolerance_s·hz)` (0.2 s), gkdv = 0 (single-frame arms). Sourced from the live sk defaults with a module-level drift sentinel (`_assert_score_params_pinned`, ADR-067 lockstep), distinct from the ADR-089 **build** halo.
- **Re-window the spilled built frames** `assign_score_windows` (Spark) / `assign_score_window_ids` (pure) on the dense `frame_id` ordinal with the per-mart scoring halo — the exact ADR-089 mechanism, keyed on the built-frame columns.
- **Route, don't re-link.** Each action is routed to the window whose **core** frames contain its nearest-in-time frame (`route_actions_to_windows`, a `merge_asof(direction='nearest')` on `time_seconds`). The per-window scorer runs on the window's `(core+halo)` frames with the **whole unit's** actions (the action-grain scorers chain the full possession sequence via `with_possessions`; filtering actions per window would break possession context — the 43/44 and `n_defensive_credits` regressions), and the output is filtered to the rows whose anchor was routed to this window (`_keep_core_outputs`). gkdv (frame-grain, no actions) trims halo-origin frame rows instead. Each scored entity survives in exactly one window → the union is the whole-unit output.
- **Inject whole-unit invariants (Stage-2a).** `precompute_stage2a` computes the GoalMap (+ carrier for gkdv) + the gkdv stride-survivor set + the coverage-restricted actions + the action→window routing **once** on the driver from a bounded spill read (positions domain, no grids), and `make_mart_scorers` captures them into the scorer closures. The three whole-unit dependencies are closed through silly-kicks seams: `goal_map=` (sk 4.132.0), `infer_ball_carrier` injection (already available), and `select_gkdv_scored_frames` + `build_ghost_frames(scored_frame_keys=)` (sk 4.133.0, sk-side ADR-119). The lakehouse pins the released PyPI floor `>=4.133.0,<5`.
- **Provider routing** mirrors Stage 1: seam providers (idsse + skillcorner + gradientsports) are scored windowed; **metrica is EXCLUDED** (does not OOM, converter window-unsafe) and keeps the whole-unit `(game_id, period_id)` regroup. Both paths carry the same Stage-2a restricted actions + injected invariants, so they and the neutrality baseline agree.
- **Scoring-window size** `_TM_SCORE_WINDOW_FRAMES` is a knob DISTINCT from the Stage-1 build knob (the scorer working set per resident frame is heavier). Neutrality holds at any size, so the value is tuned purely for memory.

## Beyond-coverage actions are out of contract (§4.6a)

An action beyond the unit's frame coverage (an event logged after tracking stops) links to no frame; the whole-unit scorer still emits a degenerate, frame-independent fallback row for it (zero-value credit, `n=1`) that a single window cannot reproduce. **Owner decision (2026-10-12, option 1):** exclude such actions via `restrict_actions_to_frame_coverage`, applied **once at the unit level in BOTH the windowed path AND the whole-unit (metrica) path** so production cannot silently differ and the two agree with the neutrality baseline. This is a genuine output-contract change (the degenerate rows are dropped for all providers, not just the 8 units).

**Hyrum / row-count assessment (S2W-PART-01, DONE 2026-10-12 — `D:\Development\_reviews\2026-10-12-adr090-beyond-coverage-hyrum-assessment.md`).** Measured on the canary (1 beyond-coverage action dropped): only `action_defensive` (−1) and `defensive_credit_attributions` (−1) actually lose the degenerate zero-value per-action row; `gk_decision`, `rest_defense`, `off_ball_runs` are unchanged (Δ=0 — their own scoring is frame-gated, so no such row existed). The 5 bronze tables' gold marts are dbt DAG leaves with no `relationships`/`equal_rowcount`/custom row-count test; no Taipy/HF/`src` consumer depends on these rows or on per-action counts (the one plausible count-dependent consumer, the `gk_decision` Taipy cohort `COUNT(*)`/`AVG`/`HAVING n≥10`, is moot — `gk_decision` Δ=0). **No downstream consumer is affected → does not block F.4.**

**Release note (consumers):** ADR-090 excludes beyond-frame-coverage actions in both scoring paths; only `action_defensive` + `defensive_credit_attributions` drop a degenerate zero-value per-action row, and no known consumer depends on their presence or on per-action counts for these marts.

## Load-bearing is proven by PERTURBATION, not per-window divergence (S2W-SPEC-01)

`windowed == whole-unit` cannot by itself prove an injection is wired, because **no committed fixture diverges per window**: the sk 7.1% GoalMap figure is per-FRAME, and a ~150-frame scoring window's mean GK-x does not flip the defended end, so `goal_map=None` (per-window recompute) already equals the injected whole-unit map on every fixture — a dropped injection would ship green. The injections are therefore proven load-bearing by **perturbation**: invert the injected GoalMap / scramble the carrier / drop a stride survivor and assert the windowed output CHANGES (red-green verified — nulling the injection fails all three). Composed with the silly-kicks seam's own load-bearing tests (the seam consumes the value), the full chain is non-vacuous. This is the HFW-SPEC-03 "the detector must be able to fail" rule applied to the injection guard itself.

## Alternatives considered

| Option | Why rejected |
|---|---|
| Keep the whole-unit Stage-2 regroup | the OOM itself — the whole-unit built frame + scorer working set busts the ~1 GB cap on 8 dense units |
| Re-estimate the invariants per window (no injection) | changes the metric: per-window GoalMap (skillcorner 7.1% per-frame), carrier hysteresis reset, stride re-rank → not neutral |
| Stateful carried-forward carrier across windows (the §9 Q2 fallback) | a re-implementation of sk `infer_ball_carrier`'s hysteresis; would need its OWN neutrality proof (carried-forward boundary state == whole-unit sequential), not assumed. Not needed — the driver-side whole-unit carrier fits the fit-profile budget |
| Window only the frame-local marts, leave the invariant marts whole-unit | the invariant marts (incl. gkdv's doubled DAS) are the heaviest — leaving them whole-unit does not fix the OOM |

## Consequences

**Positive.** The resident built slice in a Stage-2 scoring UDF is bounded by `_TM_SCORE_WINDOW_FRAMES` (+ the mart's scoring halo), not unit size — the 8-unit Stage-2 OOM is fixed. Windowed scoring is byte-identical to whole-unit for every mart (neutrality). The seam reuses the ADR-089 ordinal + halo machinery (no new windowing logic); the routing + invariant injection are the only new concepts.

**Costs / negatives.** A driver-side Stage-2a whole-unit pass remains (bounded, positions domain) — the GoalMap/carrier/stride reads re-touch the spill on the driver, which the §4.6 fit-profile budgets. The beyond-coverage exclusion is a real output-contract change pending the Hyrum assessment. `_TM_SCORE_WINDOW_FRAMES` is provisional pending the Task 0.2/D.1 fit-profile (a scoped dense-unit canary measurement, separate approval) before the 8-unit re-run. metrica stays whole-unit (excluded).

**Validation.** Pure per-mart neutrality (`windowed union == whole-unit`, all six marts), the perturbation load-bearing tests (S2W-SPEC-01), the Stage-2a driver-precompute equivalence through a real parquet round-trip, and the coverage-guard non-vacuity test — all in `tests/tracking_marts/test_stage2_window_scoring.py`. The real-Spark windowed dispatch (`assign_score_windows` + per-window `mapInPandas` + core-output trim + union) is Docker/live-validated (`tests/tracking_marts/test_two_stage_dispatch.py`, pyspark). The 8-unit re-run is a separate post-merge operator step under its own approval, only after post-merge main CI is green.
