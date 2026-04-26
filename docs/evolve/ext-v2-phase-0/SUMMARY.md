# ExT v2 Phase 0 — Singh-2018 Baseline under Optuna Harness

**Date:** 2026-04-26
**Branch:** `feat/ext-v2-phase-0-singh-baseline`
**Execution venue:** Local (Win11 96GB) → Databricks `soccer-analytics-warehouse-dev` (2X-Small serverless)
**Source:** `soccer_analytics.dev_gold.fct_action_values` filtered to xT-relevant SPADL types
**Spec:** [`docs/superpowers/specs/2026-04-25-ext-v2-reproduction-design.md`](../../superpowers/specs/2026-04-25-ext-v2-reproduction-design.md)
**ADR ref:** [ADR-015](../../superpowers/adrs/ADR-015-xt-pipeline-hardening.md) (v1 hygiene prerequisite)

## Phase 0 stop condition (pre-registered)

> Continue if the v2 implementation matches `analytics.expected_threat.compute_expected_threat_grid` outputs to within numerical tolerance.

**Disposition: PASS.**

- Production-default 12x8 grid: `np.testing.assert_array_equal(v2.values, v1.values)` passes byte-for-byte across all tested seeds + grid resolutions ≤200 zones (12x8, 16x12, 6x4).
- 24x16 grid (n_zones=384): v2 always uses numpy; v1 routes to JAX when `n_zones > 200`. JAX vs numpy reduction-ordering produces ~3e-7 max relative diff (float-arithmetic, not implementation). Match asserted at `allclose(rtol=1e-5, atol=1e-7)` — orders of magnitude tighter than any plausible implementation bug.
- Independent reimplementation: v2's `SinghTransitionMatrix`, `_assign_zones`, `SINGH_MOVE_TYPES`, and `value_iteration.iterate` are all written from scratch in `src/analytics/ext_v2/`; the v1↔v2 parity tests in `test_transition.py::TestParityWithV1` enforce equality of the SPADL-domain constants and binning helpers separately from the producer-level numerical match.

## Phase 0 baseline metrics

Holdout: 15% of matches per `(competition_id, match_key)` hash bucket (sha256 % 100 < 15). NLL evaluated on `action_type='pass'` rows only.

| Metric | Value |
|---|---:|
| Total xT-relevant actions | 8,809,385 |
| Train fold | 7,516,275 actions |
| Holdout fold (passes only) | 677,436 passes |
| Total matches | 5,404 |
| Total competitions | 22 |
| Competitions with non-empty holdout | 16 |
| **Global held-out NLL** | **3.78924** |
| log(96) — uniform 12x8 baseline | 4.564 |
| Improvement vs uniform | **−17.0 %** |
| Wall-clock fit | 6.9s (warehouse cold-start ~75s + 8.8M-row load ~30-265s depending on warmth, fit + NLL 6.9s) |

## Per-competition NLL

6 competitions ({35, 44, 81, 87, 116, 1470}) have 1-6 matches each and fell entirely into the train fold under the 15% hash threshold (binomial floor); per-competition NLL skips them gracefully (locked design decision iii). Their actions still contribute to producer training.

| competition_id | NLL | matches in source |
|---:|---:|---:|
| 0 | 3.9760 | 1,941 |
| 11 | 3.6445 | 867 |
| 7 | 3.6381 | 435 |
| 2 | 3.7077 | 418 |
| 12 | 3.6536 | 381 |
| 9 | 3.7356 | 340 |
| 37 | 3.7336 | 326 |
| 43 | 3.6614 | 147 |
| 72 | 3.6611 | 116 |
| 1238 | 3.8665 | 115 |
| 55 | 3.6201 | 102 |
| 53 | 3.5451 | 62 |
| 1267 | 3.7215 | 52 |
| 49 | 3.7643 | 36 |
| 223 | 3.7797 | 32 |
| 16 | 3.7796 | 18 |

Range: 3.545 (comp 53) — 3.976 (comp 0). Mean: 3.706. Largest competitions (0, 11, 7) span a 0.34-NLL range, so cross-comp variance reflects league-specific transition structure rather than statistical noise.

## Architecture (Phase 0 deliverable)

`src/analytics/ext_v2/` — parallel package, independent of v1's
`compute_expected_threat_grid`:

| Module | Role | LOC |
|---|---|---:|
| `holdout.py` | Hash-based deterministic 15% match split | 95 |
| `value_iteration.py` | Pure-numpy Bellman fixed-point (mirrors v1) | 56 |
| `transition.py` | `TransitionModel` ABC + `SinghTransitionMatrix` + `GridSpec` + `_assign_zones` | 158 |
| `producer.py` | `Producer` ABC + `SinghProducer` (composes transition + value iteration → `XTGrid`) | 163 |
| `fitness.py` | `compute_holdout_nll`, `compute_holdout_nll_per_competition` | 117 |
| `harness.py` | Optuna single-trial runner; `Phase0Result` dataclass | 147 |

Test coverage: 127 tests in `src/tests/test_ext_v2/`, including the parity + numerical-match suites that enforce the stop condition.

## Locked design decisions (per spec §10.2)

1. **Source: A** — single-source `fct_action_values` for both training and NLL evaluation. Rationale: train↔eval distributional consistency; version stability across phases; numerical-tolerance match purity. Rejected: B (eval on `fct_passes`).
2. **Hash key: `match_key`** (BIGINT, present on both `fct_action_values` and `fct_passes`).
3. **Small-comp handling: iii** — hash-split as-is; per-comp NLL skips empty-holdout comps; global NLL uses all 22 comps' contributions.
4. **MLflow integration: deferred to Phase 1** (YAGNI — Phase 0 has no trials worth tracking).
5. **No workflow card for Phase 0** — local benchmark, not recurring.

## Reproducibility

```bash
uv run --with databricks-sql-connector python scripts/run_ext_v2_phase0.py \
    --output docs/evolve/ext-v2-phase-0/phase0_baseline.json
```

Auto-starts the warehouse if stopped (~75s cold start), pulls 8.8M rows via Arrow chunks (~4 min on a typical home connection), fits + evaluates in ~7s. Re-running on the same data is bit-deterministic (hash split + numpy operations).

The full output is checked in at `phase0_baseline.json` for cross-phase comparison.

## Forward — Phase 1

Branch: `feat/ext-v2-phase-1-kde-smoothed` (cut after Phase 0 lands).

Activate Optuna axes:

- `kde_kernel ∈ {gaussian, epanechnikov, tophat}`
- `kde_bandwidth ∈ [0.01, 2.0]` (log-uniform)
- `kde_adaptive ∈ {True, False}`

Stop condition: Phase 1 NLL < Phase 0 NLL by ≥1% relative (i.e., **< 3.7513**). Otherwise KDE smoothing isn't doing useful work on this data — file finding and skip to Phase 2.

Add MLflow tracking via `optuna.integration.MLflowCallback` (Phase 0 deferral activates here).

## Cross-reference

- [ADR-015](../../superpowers/adrs/ADR-015-xt-pipeline-hardening.md) — v1 architectural hygiene (XTGrid wrapper, differential validation, workflow card SSOT) shipped in PR #205, the prerequisite that pre-positioned this Phase 0 build.
- [Spec §6 phasing plan](../../superpowers/specs/2026-04-25-ext-v2-reproduction-design.md) — Phase 1-4 axes.
- T1 tracker entry in [external research tracking](../../research/external-research-tracking.md) — channel-attributed Salimi/Salmankhah methodology source.
