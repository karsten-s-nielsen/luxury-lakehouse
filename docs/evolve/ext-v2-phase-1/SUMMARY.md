# ExT v2 Phase 1 — KDE-Smoothed Singh under Optuna

**Date:** 2026-04-26
**Branch:** `feat/ext-v2-phase-1-kde-smoothed`
**Execution venue:** Local (Win11 96GB) → Databricks `soccer-analytics-warehouse-dev` (2X-Small serverless)
**Source:** `soccer_analytics.dev_gold.fct_action_values` filtered to xT-relevant SPADL types
**Spec:** [`docs/superpowers/specs/2026-04-25-ext-v2-reproduction-design.md`](../../superpowers/specs/2026-04-25-ext-v2-reproduction-design.md) §10.3 (locked decisions) + §10.3 build outcomes
**Plan:** [`docs/superpowers/plans/2026-04-26-ext-v2-phase-1.md`](../../superpowers/plans/2026-04-26-ext-v2-phase-1.md)
**Phase 0 baseline:** [`docs/evolve/ext-v2-phase-0/SUMMARY.md`](../ext-v2-phase-0/SUMMARY.md) (NLL 3.78924)

## Phase 1 stop condition (pre-registered)

> Continue if Phase 1 NLL < Phase 0 NLL by ≥1% relative; otherwise KDE smoothing isn't doing useful work and we file the finding.

Concrete threshold: `nll_primary < 3.7513`.

**Disposition: PASS.** `nll_primary = 3.74823` → 1.082% relative improvement over Phase 0's 3.78924. KDE smoothing produces a real (though modest) improvement on this data.

## Phase 1 baseline metrics

Holdout: identical to Phase 0 (15% match-stratified hash on `(competition_id, match_key)`); same 16-of-22 competitions with non-empty holdout. NLL evaluated on `action_type='pass'` only.

| Metric | Value |
|---|---:|
| Total xT-relevant actions | 8,809,385 |
| Train fold | 7,516,275 actions |
| Holdout fold (passes only) | 677,436 passes |
| Total matches | 5,404 |
| Total competitions | 22 |
| Competitions with non-empty holdout | 16 |
| **Best `nll_primary` (eps=1e-10)** | **3.74823** |
| Best `nll_floorless` (eps=1e-300, diagnostic) | 3.74823 |
| Phase 0 baseline | 3.78924 |
| **Relative improvement** | **+1.082%** |
| Stop threshold | 3.7513 |
| Stop disposition | **PASS** |
| Optuna trials | 500 |
| Best trial | #276 of 500 |
| Plateau warning | False (best #276 outside last-50 range) |
| Wall-clock | 8,130.2s (~135.5 min) |
| Per-trial wall-clock | ~16s/trial (slower than smoke-extrapolated 10s) |

## Best Optuna trial parameters

| Axis | Value | Notes |
|---|---|---|
| `kde_kernel` | `gaussian` | TPE explored all three named kernels; gaussian dominated |
| `kde_bandwidth` | **1.9999822** | **At saturated upper edge of `[0.01, 2.0]` log-uniform prior** — Phase 2 should widen |
| `kde_adaptive` | `True` | Per-row Silverman with global multiplier; widens sparse rows automatically |

**Diagnostic equality `nll_primary == nll_floorless`** confirms gaussian kernel never triggered the 1e-10 eps floor — every transition probability is strictly positive within float64 representable range. Validates the Q4 design assumption that gaussian + reasonable bandwidth makes the eps clip dormant.

## Per-competition NLL

6 competitions ({35, 44, 81, 87, 116, 1470}) have ≤6 matches each and fell entirely into the train fold under the 15% hash threshold; per-competition NLL skips them (locked design decision iii). Their actions still contribute to producer training. Sorted by NLL ascending:

| competition_id | NLL |
|---:|---:|
| 53 | 3.50819 |
| 55 | 3.58451 |
| 7 | 3.60037 |
| 11 | 3.60404 |
| 12 | 3.61660 |
| 43 | 3.62583 |
| 72 | 3.62774 |
| 2 | 3.66779 |
| 1267 | 3.68510 |
| 37 | 3.69522 |
| 9 | 3.69567 |
| 49 | 3.72052 |
| 16 | 3.73326 |
| 223 | 3.74417 |
| 1238 | 3.82321 |
| 0 | 3.93147 |

Range: 3.508 (comp 53) — 3.931 (comp 0). Same shape as Phase 0 (3.545 — 3.976) — KDE smoothing improved every competition's NLL relative to Phase 0 modestly and consistently, not catastrophically on any one comp.

## Architecture (Phase 1 deliverable)

`src/analytics/ext_v2/` — extends Phase 0 with one new module + one extended class:

| Module | Role | Phase 1 changes |
|---|---|---|
| `kde.py` (new) | `KDESmoothedTransition(TransitionModel)` + `silverman_2d` helper | New file; per-source-zone 2D `sklearn.KernelDensity`; per-row Silverman with global multiplier when `adaptive=True`; row-mean fallback for zero-event source zones |
| `producer.py` (extended) | `KDESmoothedProducer(Producer)` | New class composing `KDESmoothedTransition` with value iteration + `XTGrid` wrap (mirrors `SinghProducer` end-to-end except the transition step) |
| `harness.py` (extended) | `objective_phase1`, `Phase1Result`, `run_phase1_harness` | Activates 3 KDE Optuna axes; logs eps-free `nll_floorless` user_attr per trial; accepts `callbacks` passthrough so MLflow wiring lives in the run script (library stays MLflow-dep-free) |

Test coverage: 47 new tests in `src/tests/test_ext_v2/`:
- `test_kde.py` (23 tests): contract surface, kernel correctness vs sklearn reference, Singh-limit recovery, Silverman monotonicity, zero-event fallback, all-zero edge case
- `test_producer.py` (8 added): KDESmoothedProducer composition + ABC + API
- `test_harness.py` (16 added): TestPhase1Objective (6), TestPhase1Result (2), TestRunPhase1Harness (8) — including SQLite study persistence + callback passthrough

Total ext_v2 test count: 174 (Phase 0's 127 + Phase 1's 47).

## Locked design decisions

See [spec §10.3](../../superpowers/specs/2026-04-25-ext-v2-reproduction-design.md) for the seven locked decisions and rationale (KDE library = sklearn; per-source-zone smoothing with point evaluation at zone centers; per-row Silverman with global multiplier; eps=1e-10 primary + eps-free diagnostic; n_trials=500; outer selection deferred to Phase 2; local Win11 venue).

## Lessons / decisions worth carrying

1. **`(m > 0).all()` for gaussian KDE is float64-wrong.** Mathematical unbounded support ≠ float64 strictly-positive support — gaussian density underflows to 0.0 at destinations many bandwidths away. Test rewritten to assert positive density at *observed* destinations only.
2. **Adaptive widening test must isolate from data-spread effects.** Original entropy-of-row comparison conflated bandwidth-widening with how spread the underlying data was. Replaced with direct property test of `silverman_2d`.
3. **`cosine` is a valid sklearn kernel.** `KDESmoothedTransition` doesn't validate kernel name at `__init__` — defers to sklearn at fit() time. The Optuna axis is the constraint, not the class.
4. **Optuna 4.x removed `MLflowCallback` from core.** Need `optuna-integration[mlflow]>=4.0`. Added to `[mlflow]` extra.
5. **Bandwidth optimum saturated the upper edge.** TPE found best at bandwidth 1.99998 (upper bound 2.0). Phase 2 should widen the prior to `[0.01, 5.0]` or higher.
6. **Per-trial wall-clock 16s > smoke-extrapolated 10s.** Smoke used 5K-row uniform synthetic data; real 8.8M-row clustered data is heavier per BallTree query. Update Phase 4 sizing estimates accordingly.
7. **Per-comp NLL improvement was modest and uniform.** No outlier comp where smoothing failed; no outlier comp where smoothing helped enormously. Confirms KDE smoothing is a flat-improvement methodology — not the place to look for big gains. Phase 2's KNN substitution is where structural variance should appear.

## Reproducibility

```bash
uv run --with databricks-sql-connector python scripts/run_ext_v2_phase1.py \
    --output docs/evolve/ext-v2-phase-1/phase1_baseline.json \
    --n-trials 500 \
    --study-db docs/evolve/ext-v2-phase-1/optuna.db \
    --mlflow-uri file:./mlruns \
    --best-producer docs/evolve/ext-v2-phase-1/best_producer.joblib \
    --mlflow-experiment ext-v2-phase-1
```

Auto-resumes the Optuna study from `optuna.db` if the run is interrupted (load_if_exists=True). The 8.8M-row Arrow pull takes ~30s on a warm warehouse, ~75s cold. ~135 min total wall-clock for 500 trials on Win11 96GB. Re-running on the same data + same study-name continues from the existing trial set.

## Forward — Phase 2

**Branch (planned):** `feat/ext-v2-phase-2-knn-no-context` (cut after Phase 1 merges to main).

**Methodology:** KNN replaces transition matrix entirely. Per spec §6 row 2:

> Continue if Phase 2 NLL ≈ Phase 1 NLL within 1% relative; large regression means the KNN implementation has a bug.

**Stop condition (pre-registered):** Phase 2 NLL within 1% of Phase 1's 3.74823 — i.e., `3.71 ≤ nll_primary ≤ 3.79`. Below 3.71 → KNN beats KDE (good, proceed with confidence to Phase 3); above 3.79 → halt and debug.

**Activate Optuna axes:** add `knn_k ∈ [5, 200]` (log-uniform), `knn_distance ∈ {euclidean, mahalanobis, weighted_euclidean}`, `feature_norm ∈ {standardize, minmax, raw}` (in addition to the 3 KDE axes from Phase 1).

**Phase 1 follow-up to fold into Phase 2:**
- Widen `kde_bandwidth` prior from `[0.01, 2.0]` to `[0.01, 5.0]` — Phase 1's optimum saturated at the upper edge.

**Schema dependency:** Phase 2 is schema-agnostic (event-data only); does NOT need Kimball PR 7. Can begin immediately after Phase 1 merges.

## Cross-reference

- [Spec §10.3](../../superpowers/specs/2026-04-25-ext-v2-reproduction-design.md) — locked design decisions + Phase 1 build outcomes (after F5)
- [Phase 1 plan](../../superpowers/plans/2026-04-26-ext-v2-phase-1.md) — 24-task implementation plan
- [Phase 0 SUMMARY](../ext-v2-phase-0/SUMMARY.md) — baseline this Phase 1 improves over
- [ADR-015](../../superpowers/adrs/ADR-015-xt-pipeline-hardening.md) — v1 architectural hygiene (XTGrid wrapper, differential validation, workflow card SSOT) shipped in PR #205, the prerequisite that pre-positioned Phase 0 + Phase 1
- [T1 tracker](../../research/external-research-tracking.md) — channel-attributed Salimi/Salmankhah methodology source
