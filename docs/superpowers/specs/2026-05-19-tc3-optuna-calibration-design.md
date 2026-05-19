# TC-3: Optuna Calibration Sweep — Design Spec

**Date**: 2026-05-19
**Status**: Approved (v3 — incorporates silly-kicks source review rounds 1+2)
**Author**: Karsten Skyt Nielsen + Claude
**References**: silly-kicks TF-24, `reference_evolve_vs_optuna_tool_choice.md`
**Review**: silly-kicks session review 2026-05-19 (H1-H3, M1-M6, L1-L4 addressed)

## 1. Goal

Replace three sets of engineering-choice defaults in silly-kicks with Optuna-calibrated
values, validated against lakehouse production data. Deliverable: PR to silly-kicks
updating `LinkParams.k3`, `infer_ball_carrier` defaults, and `add_off_ball_runs` defaults,
with provenance ("Optuna-calibrated against `<fold>` on `<date>`").

## 2. Providers

**Calibration sweep**: IDSSE (Sportec) + SkillCorner + Gradient Sports WC 2022.

- **Metrica excluded** — different reasons per stage:
  - Stage 1 (carrier accuracy): degraded signal — NaN ball positions break
    ball-to-player distance, 10 fps yields noisy velocity estimates for `beta`.
  - Stage 2 (VAEP Brier): insufficient sample size — only 3 anonymized matches.
    Player-position-dependent features (action context, defensive line, team shape)
    would work at 25 fps, but 3 matches contribute more noise than signal.

- **Gradient Sports WC 2022** (64 matches, events + tracking): ingested into
  bronze/staging as part of this cycle and **used in the calibration sweep**.
  License status: pending written confirmation from Gradient Sports.
  Approved for internal training/calibration use.
  **NOT published** to HF datasets, gold marts, synced tables, or Taipy UI until
  license is confirmed in writing. The data stays in bronze/staging — the sweep
  reads directly from cached Parquet, not from gold-layer tables.

## 3. Two-Stage Optimization

### Stage 1: `infer_ball_carrier` (carrier accuracy)

- **Parameters**: `tolerance_m`, `beta`, `gamma` (3 dimensions)
- **Objective**: `(inferred_carrier == action.player_id).mean()` at linked-event timestamps
- **Direction**: maximize
- **Warm-start**: first trial seeded with current defaults via `study.enqueue_trial`
- **Trials**: 100 TPE
- **Per trial**: `infer_ball_carrier(frames, ...)` only — no full enrichment pipeline.
  Carrier accuracy is a direct comparison against SPADL ground truth.
- **Diagnostic** (H2): carrier-switch-rate per minute reported alongside accuracy.
  Literature baseline: ~15-25 possession changes per match. If optimal `gamma`
  produces a rate below this floor, flag it. Not a hard constraint — diagnostic only.
- **Parallelization** (L3): 8-core `ProcessPoolExecutor` over matches within each trial.
- **Estimated wall-clock**: ~20-40s/trial (74 matches, 8-core) -> ~30-67 min total

### Stage 2: Joint `k3` + off-ball-runs (VAEP Brier)

- **Parameters**: `k3`, `pre_seconds`, `min_displacement_m` (3 dimensions)
- **Objective**: mean of per-provider 5-fold CV augmented VAEP Brier scores —
  IDSSE, SkillCorner, and Gradient Sports Brier averaged (M2: equal provider weight
  regardless of match count imbalance)
- **Direction**: minimize
- **Warm-start**: first trial seeded with current defaults via `study.enqueue_trial`
- **Sanity gate** (H1): per trial, if any optimized feature's variance drops below
  10% of its default-param variance (degenerate feature), return penalty Brier
  score of 0.25 (random-guess baseline) instead of `TrialPruned` — gives TPE
  an informative signal that this region of the search space is bad (L4).
  Also report feature importance rankings at optimum vs default to verify the
  optimized features are actually used by the model.
- **Trials**: 100 TPE
- **Per trial**: Full `_enrich_match` pipeline (all 15 steps) with param injection.
  Ball-carrier params fixed at Stage 1 optimum (affects DAS via the
  carrier -> team-in-possession -> DAS chain).
- **Estimated wall-clock**: ~60-90 min/trial (74 matches × full enrichment) -> multi-day
  (see §13 for parallelization strategy)

### Why two stages

- Sweeps (a) and (c) share the same objective (VAEP Brier) and their features both
  feed into the same augmented VAEP model — joint optimization captures cross-feature
  interactions.
- Sweep (b) has a fundamentally different objective (carrier accuracy) and must run
  first because its output (optimal ball-carrier params) feeds into Stage 2's DAS
  computation.

### Why full enrichment in Stage 2

1. **Verified coupling**: `infer_ball_carrier` -> `derive_team_in_possession` -> DAS.
   Changing ball-carrier params changes `das_team`, `das_opponent`, `das_diff`.
2. **VAEP feature-vector consistency**: The augmented VAEP model sees all ~55 tracking
   features. Changing one feature in isolation creates a distribution-shifted input
   the model was never trained on. Full recompute keeps every trial's feature vector
   internally consistent.
3. **Forward-compatibility**: silly-kicks evolves; today's independent enrichment steps
   may develop cross-dependencies. Treating `_enrich_match` as the atomic unit of
   correctness is correct by construction.

## 4. Data Flow

```
Databricks (one-time pull via SQL connector)
  |-- bronze.idsse_tracking           -> raw tracking frames (IDSSE, 7 matches)
  |-- bronze.skillcorner_tracking     -> raw tracking frames (SkillCorner)
  |-- bronze.gradientsports_tracking  -> raw tracking frames (WC 2022, 64 matches)
  |-- bronze.spadl_actions            -> SPADL actions for tracking matches
  `-- dev_gold.fct_action_values      -> match list (identify tracking matches)
                    |
        Local Parquet cache (~20-30 GB with WC 2022 tracking)
  + xT grid fitted locally from cached SPADL actions (Phase 0)
                    |
        Optuna study (local, single CPU)
                    |
        docs/evolve/tc3-calibration/
```

Data pulled once and cached as Parquet. No network I/O in the trial hot loop.

**xT grid** (L1): fitted locally in Phase 0 via `ExpectedThreat().fit(actions)` on
the cached SPADL actions. Fast (~seconds on 30K+ actions). The production pipeline
receives `xt` as an argument; the calibration script produces its own.

## 5. Augmented VAEP Model (Stage 2 objective)

The model exists only as Optuna's objective function — not a deliverable.

- **Features**: standard SPADL (type_id, body_part, result, start_x/y, end_x/y,
  time_delta, dx, dy) + all ~55 tracking context columns from enrichment
- **Labels**: binary scoring/conceding targets via
  `silly_kicks.vaep.labels.compute_scores_and_concedes(actions, nr_actions=10)` —
  ground truth derived from action sequences, not from existing VAEP predictions
- **Model**: XGBoost classifier, match-stratified CV, fixed hyperparameters
  (n_estimators=100, max_depth=4). Optimizing feature quality, not model architecture.
- **CV fold strategy** (M5): match-stratified to prevent within-match context leakage
  (same players, same pitch, same tracking noise). Per-provider structure:
  - **Gradient Sports** (64 matches): `GroupKFold(n_splits=5, groups=match_id)` —
    ~13 matches/fold, standard sklearn approach.
  - **IDSSE** (7 matches): leave-one-match-out (7 folds).
  - **SkillCorner** (~3 matches): leave-one-match-out (3 folds).
  Per-provider Brier scores already average independently, so CV structure
  can differ per provider. Random-action splits are forbidden — they leak
  match-level structure and inflate apparent performance.
- **Metric**: mean Brier score across folds, averaged across providers.
  Brier = MSE between predicted probability and binary label — proper scoring rule,
  rewards calibration.

## 6. Search Spaces

### Stage 1 — `infer_ball_carrier`

| Parameter | Range | Distribution | Default | Rationale |
|-----------|-------|-------------|---------|-----------|
| `tolerance_m` | [1.0, 8.0] | uniform | 3.0 | Below 1m misses carriers at a jog; above 8m attributes to distant players |
| `beta` | [0.0, 2.0] | uniform | 0.5 | 0 = pure distance; higher = velocity dominates. Cap at 2.0 keeps distance relevant |
| `gamma` | [0.0, 3.0] | uniform | 1.0 | 0 = stateless; 3.0 = very sticky incumbent. Above 3m carrier can't change in tight play |

### Stage 2 — Joint `k3` + off-ball-runs

| Parameter | Range | Distribution | Default | Rationale |
|-----------|-------|-------------|---------|-----------|
| `k3` | [0.1, 5.0] | log-uniform | 1.0 | Multiplicative scaling factor; 0.1 = pressure barely registers, 5.0 = very sensitive. Paper says "calibrated with experts" — no published value. Only the `link_zones` pressure method uses k3 (L2); andrienko_oval and bekkers_pi are unaffected. |
| `pre_seconds` | [0.5, 5.0] | uniform | 1.5 | Time window for off-ball-run detection. Below 0.5s misses build-up; above 5s captures irrelevant movement |
| `min_displacement_m` | [1.0, 8.0] | uniform | 3.0 | Minimum movement for off-ball run. Below 1m captures jitter; above 8m only catches sprints |

### LinkParams scope

k3 only. The zone geometry (`r_hoz=4, r_lz=3, r_hz=2, angles 45/90`) comes from
Figure 2 of Link 2016 — measured, not arbitrary. k3 is the one parameter the authors
explicitly flagged as needing calibration. A post-hoc geometry sensitivity scan confirms
(or refutes) this assumption.

## 7. Enrichment Parameter Injection

The calibration script does NOT modify the production `_enrich_match` pipeline.
It implements a calibration-specific enrichment function that mirrors the production
steps but accepts tunable parameters.

**API paths confirmed** (silly-kicks source review):
- `add_pressure_on_actor` accepts `params_per_method={"link_zones": LinkParams(k3=...)}`
- `add_off_ball_context` accepts `pre_seconds` and `min_displacement_m` kwargs directly
  (signature at `features.py:1261`)
- `add_off_ball_runs` also accepts these directly (lower-level alternative)

**Deployment gap** (H3): The lakehouse production `_enrich_match` hardcodes defaults —
it does not accept these as arguments. Calibrated values take effect via a two-step
deployment chain:
1. PR to silly-kicks updates the library defaults
2. Lakehouse bumps silly-kicks version
After step 2, production `_enrich_match` automatically uses the new defaults.
No lakehouse code changes required beyond the version bump.

## 8. Script Architecture

```
scripts/run_tc3_calibration.py
|
|-- Phase 0: Data loading + validation
|   |-- Pull tracking frames + SPADL actions from Databricks SQL
|   |-- Cache as local Parquet per match
|   |-- Gradient Sports validation gate (M6):
|   |   |-- Per-match frame count sanity (min/max, flag outliers)
|   |   |-- GK identification via derive_goalkeepers per match
|   |   |-- NaN prevalence in x/y/ball_x/ball_y per match
|   |   |-- At least one action from each team per match
|   |   `-- Log report, exclude anomalous matches from sweep
|   |-- Compute VAEP labels (scores/concedes) per match
|   `-- Fit xT grid from SPADL actions (for cover shadows + GK influence)
|
|-- Phase 1: Stage 1 -- carrier accuracy
|   |-- optuna.create_study(direction="maximize", sampler=TPESampler)
|   |-- Enqueue default params as first trial (warm-start)
|   |-- Objective (8-core parallel over matches):
|   |   |-- infer_ball_carrier with trial params per match
|   |   |-- Compare to action.player_id at linked timestamps
|   |   `-- Record carrier-switch-rate diagnostic
|   |-- study.optimize(objective, n_trials=100)
|   `-- Save study + best params to stage1_results.json
|
|-- Phase 2: Stage 2 -- VAEP Brier
|   |-- optuna.create_study(direction="minimize", sampler=TPESampler)
|   |-- Enqueue default params as first trial (warm-start)
|   |-- Objective: for each match, full enrichment with trial params
|   |   |-- k3 -> pressure_on_actor__link_zones (via params_per_method)
|   |   |-- pre_seconds, min_displacement_m -> off-ball-run columns
|   |   |-- Ball-carrier fixed at Stage 1 optimum -> DAS
|   |   `-- All other steps: default params
|   |-- Sanity gate: if feature variance < 10% of default -> return 0.25 penalty
|   |-- Per provider: match-stratified CV XGBoost -> Brier score
|   |-- Objective value = mean(IDSSE_brier, SkillCorner_brier, GradientSports_brier)
|   |-- study.optimize(objective, n_trials=100)
|   `-- Save study + best params + feature importances to stage2_results.json
|
|-- Phase 3: Post-hoc diagnostics
|   |-- Per-provider re-evaluation at global optimum
|   |-- k3 1D sensitivity curve per provider (TF-25 gate input)
|   |-- Geometry sensitivity scan (r_hoz, r_lz, r_hz -- 1D sweeps)
|   |-- Feature importance comparison: optimum vs default params
|   `-- Convergence visualization (Optuna plot_optimization_history)
|
`-- Phase 4: Output generation
    |-- docs/evolve/tc3-calibration/SUMMARY.md
    |-- docs/evolve/tc3-calibration/stage1_results.json
    |-- docs/evolve/tc3-calibration/stage2_results.json
    `-- docs/evolve/tc3-calibration/per_provider_diagnostics.json
```

**Persistence**: Optuna uses SQLite storage (`tc3_stage1.db`, `tc3_stage2.db`)
so trials survive interruptions. Script resumes with `study.optimize(n_trials=remaining)`.

**Execution**: `uv run python scripts/run_tc3_calibration.py --stage 1` /
`--stage 2` / `--stage diagnostics`. Stages are independently runnable.

## 9. Post-Hoc Diagnostics & TF-25 Gate

1. **Per-provider evaluation**: re-run best-param enrichment per provider separately,
   report carrier accuracy and Brier score per provider.

2. **k3 per-provider sensitivity**: sweep k3 from 0.1-5.0 in 20 steps with other params
   fixed at global optimum. Plot Brier vs k3 per provider.

3. **TF-25 gate decision** (M3): principled criterion replaces the arbitrary 30%
   threshold. Compute per-provider Brier at global optimum vs per-provider optimum.
   If the gap exceeds the 5-fold CV standard error for that provider, the provider
   needs its own k3 -> recommend TF-25 (evolve the aggregation form). If the gap
   is within the CV standard error for all providers, the global optimum generalizes
   and TF-25 is unnecessary.

4. **Geometry sensitivity scan**: 1D sweeps on `r_hoz`, `r_lz`, `r_hz` to confirm
   Figure 2 values are locally optimal. If any shows >5% Brier improvement, flag
   for follow-up. Prior: no movement.

5. **Feature importance comparison**: XGBoost feature importances at optimal params
   vs default params. Verifies optimized features (pressure, off-ball-runs) are
   actively used by the model, not ignored (H1 defense-in-depth).

## 10. Outputs

| Artifact | Purpose |
|----------|---------|
| `docs/evolve/tc3-calibration/SUMMARY.md` | Optimal values, improvement over defaults, per-provider breakdown, TF-25 recommendation, feature importance comparison |
| `docs/evolve/tc3-calibration/stage1_results.json` | Stage 1 study: best params, all trials, carrier-switch-rate diagnostics |
| `docs/evolve/tc3-calibration/stage2_results.json` | Stage 2 study: best params, all trials, feature importances, sanity gate log |
| `docs/evolve/tc3-calibration/per_provider_diagnostics.json` | Per-provider metrics + k3 sensitivity curves + TF-25 gate evaluation |
| PR to silly-kicks | Update default constants with calibrated values + provenance comments |

## 11. Prerequisites & Risks

| Risk | Mitigation |
|------|------------|
| Degenerate features pass optimization (H1) | Variance sanity gate (10% of default) -> 0.25 penalty Brier (L4) + feature importance reporting |
| High-gamma carrier locks to incumbent (H2) | Carrier-switch-rate diagnostic vs literature baseline |
| Within-match context leakage in CV (M5) | Match-stratified folds: LOMO for IDSSE/SkillCorner, GroupKFold(5) for Gradient Sports |
| Gradient Sports data quality (M6) | Per-match validation gate in Phase 0: frame count, GK, NaN, team coverage |
| Production deployment gap (H3) | Two-step chain documented: silly-kicks PR -> lakehouse version bump |
| Dataset balance across providers | Objective = mean of per-provider Brier scores (equal provider weight); ~74 matches across 3 providers |
| IDSSE-dominated dataset biases optimizer (M2) | Per-provider averaging + Gradient Sports 64 matches rebalance away from IDSSE dominance |
| Stage 2 overnight run interrupted | SQLite storage persists all trials; script resumes |
| xT grid dependency | Fitted locally in Phase 0 from cached SPADL actions |

## 12. Gradient Sports WC 2022 Ingestion

Ingested into bronze/staging as part of this cycle. Used in calibration sweep.

- **Data**: 64 WC 2022 matches, events + tracking (Gradient Sports)
- **silly-kicks support**: `GRADIENTSPORTS_TRACKING_FRAMES_COLUMNS` exists;
  tracking module has `gradientsports` submodule
- **License status**: pending written confirmation from Gradient Sports.
- **Approved for**: internal calibration/training (this sweep).
- **NOT approved for**: HF dataset publication, gold marts, synced tables, Taipy UI.
  Data stays in bronze/staging until license confirmed in writing.
- **Value**: transforms calibration dataset from ~10 to ~74 matches across
  3 competitions (Bundesliga, SkillCorner coverage, WC 2022). Eliminates the
  small-dataset risk and enables genuine cross-competition generalization.

## 13. Estimated Wall-Clock & Parallelization

With 74 matches (vs the original ~10), per-trial cost scales linearly. The enrichment
pipeline is embarrassingly parallel across matches — each match is independent.

**Per-match enrichment**: ~30-60s on single CPU (pandas, no Spark overhead).
**Per-trial (74 matches, serial)**: ~37-74 min.
**Per-trial (74 matches, 8-core parallel)**: ~5-10 min.

The script uses `concurrent.futures.ProcessPoolExecutor` (8 workers on the 96 GB
machine) to parallelize per-match enrichment within each trial. This brings Stage 2
back to the overnight range.

| Phase | Time |
|-------|------|
| Gradient Sports ingestion | ~half-day (separate from sweep) |
| Data pull + cache + validation | ~30 min (64 WC matches + existing providers) |
| Stage 1 (100 trials, 8-core) | ~30-67 min |
| Stage 2 (100 trials, 8-core) | ~8-17h |
| Post-hoc diagnostics | ~3h |
| **Total** | **~12-21h** sweep (overnight) + ~half-day ingestion |

## 14. Out of Scope

- Full 6-parameter LinkParams geometry sweep (deferred unless geometry sensitivity
  scan flags it)
- Metrica data inclusion
- TF-25 evolve follow-up (gated on TC-3 diagnostics)
- Augmented VAEP as a production model (the model is disposable)
- Gradient Sports data in gold marts, synced tables, HF datasets, or Taipy UI (gated on license)
