# ADR-035: silly-kicks 4.2.0 adoption — vectorized ghost-GK KDE backend + local-fixture profiling methodology

| Field | Value |
|---|---|
| **Date** | 2026-06-01 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

silly-kicks 4.2.0 (published 2026-06-01) reworks the ghost-GK density estimator — the dominant cost in the AC-1 enrichment chain. The previous implementation evaluated a per-sample 2D KDE via `scipy.stats.gaussian_kde` (`_kde.py:evaluate`); 4.2.0 replaces it with an in-house vectorized estimator (`_ghost_gk.py:_kde_density_vectorized`) selected by a new `predict_density(..., *, kde_backend: str = "vectorized")` parameter, with the scipy path retained as `kde_backend="scipy"` (the `_reference`). 4.2.0 also (a) forwards the ball carrier into `add_das` so `accessible-space`'s offside check stops warning per call, and (b) de-`iloc`'s the elastic-sync alignment.

This adoption gated a separate decision in the silly-kicks repo: whether to invest in a GPU ghost-GK backend (GPU Phase 1). The gate datum is "is ghost-GK still the dominant per-match cost after the CPU vectorization, and is the vectorized path correct + at least as fast as scipy?"

The serverless profiling path that was supposed to answer this proved **untenable**: each measurement was a 25–40 min black-box Databricks run whose only feedback channel — the driver task log via `jobs.get_run_output` — is delayed, executor-blind, and (verified) **contaminated by cross-run log-bleed on reused serverless compute**. Three runs produced zero trustworthy timing: one compared total-elapsed against compute-wall (apples-to-oranges), one silently resolved a pre-4.2.0 silly-kicks despite a `>=4.2.0` pin (index-lag / env-cache footgun), and one fired its summary poll on a stale prior-run `wall_s` bleeding into the log. The bottleneck question does not actually need Spark — the ghost-GK KDE is pure-Python CPU work on a single match pulled to the driver.

## Decision

Adopt silly-kicks 4.2.0 with the floor pin `silly-kicks[das,ghost-gk]>=4.2.0,<5`, accepting the **vectorized** KDE as the default backend (do NOT pin `scipy`). Advance every floor consumer: pyproject `[spadl]`, the Terraform analytics env spec, the 6 trainer `_REQUIRED_SK_MIN` constants → `(4, 2, 0)`, and the enforcing sentinel in `test_sk3_mig_b_orchestrator_invariants.py`. Bump the wheel 0.5.6 → 0.5.7 via `bump_wheel.py`.

Establish **local-fixture profiling** as the methodology for stage-share / hot-path questions: `scripts/profile_ac1_local.py` runs the real `run_work_unit` → `enrich_batch` chain in-process under cProfile against the committed IDSSE J03WMX p1 fixture (165k frames, 1364 actions → 97 enriched rows) — no Spark, no Databricks, ~15 min, full visibility, exact version pinning via `uv run --with silly-kicks==X`. Add an `AC1_PROFILE env_versions ...` self-certification log to the driver profile path (`_run_profile_on_driver`) so any future serverless profile states the silly-kicks/accessible-space/numba/numpy/scipy it actually resolved.

The adoption is justified on three validated facts (local, version-certified, reproducible):

1. **Correctness** — vectorized vs scipy density: max rel-err 4.8e-14, spread 3.6e-15 (target rtol ≤ 1e-7); `ghost_gk_x/y` modes 60/60 exact. Both versions produce identical 97-row output on the fixture.
2. **Performance (A/B, same machine/fixture, only silly-kicks differs)** — whole-chain wall 1135 s (4.1.1/scipy) → 915 s (4.2.0/vectorized) = **1.24× faster**; `predict_density` 35.5 → 28.9 s/call (1.23×); `add_elastic_sync` 25.2 → 1.1 s (~23×). Identical row count → behaviour preserved.
3. **GPU Phase 1 gate** — ghost-GK remains **91 %** of the enrichment chain on 4.2.0; its residual cost is `cho_solve` + numpy reductions / `einsum` (batched linear algebra). The CPU vectorization lever is now spent (~1.24×) without collapsing the bottleneck → GPU is justified.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Adopt 4.2.0 but pin `kde_backend="scipy"` | Zero behavioural risk from the new estimator | Forgoes the 1.24× win; the vectorized path is already value-equivalent | Rejected — vectorized is both faster AND bit-equivalent; pinning scipy would freeze a known-slower path |
| B. Stay on 4.1.1; defer 4.2.0 | No cross-repo work | Keeps the offside warning flood; forgoes the speedup; blocks the GPU gate decision | Rejected — 4.2.0 is a strict improvement on every measured axis |
| C. Keep profiling on serverless, "just instrument it more" | Measures the real production env's absolute wall | The feedback loop is fundamentally broken (log-bleed, 25–40 min/iter, version-resolution surprises); 3 runs → 0 facts | Rejected — wrong tool for a CPU-bound, platform-portable question |
| D. Adopt 4.2.0 vectorized default + local-fixture profiling + self-cert (chosen) | Correct, faster, hygiene-fixed; reproducible facts in ~15 min; durable tooling | Local absolute seconds are machine-specific (not serverless wall) | — |

## Consequences

### Positive

- AC-1 enrichment is ~1.24× faster per match with bit-equivalent ghost-GK output; the per-DAS-call offside warning flood is gone; elastic-sync is ~23× faster.
- Stage-share / hot-path questions now have a fast, observable, version-pinnable local harness (`scripts/profile_ac1_local.py`) — no more black-box serverless iterations. The committed fixture makes the profile reproducible by any contributor.
- The self-cert log closes the class of bug where a profile is silently attributed to the wrong silly-kicks version (the index-lag / env-cache footgun that wasted a serverless run).
- The GPU Phase 1 decision in the silly-kicks repo is unblocked with a defensible, reproducible gate datum (91 % ghost-GK share, CPU lever spent).

### Negative

- Mechanical cost of a minor-bump floor advance: pyproject + 17 PEP 723 scripts + 6 trainer `_REQUIRED_SK_MIN` + enforcing test + TF env spec + wheel.py + deploy.sh (28 files synced via `bump_wheel.py`).
- Local absolute timings (this machine) are NOT the serverless wall-clock — the absolute serverless number remains unmeasured. Accepted: the A/B **ratios** (1.24×, 91 % share) are the portable, decision-relevant facts; an absolute serverless figure, if ever needed, is one final single-run check (never an iterative loop), trusting Jobs-API timing over the driver-log tail.
- The 1 GB serverless `applyInPandas` UDF-memory behaviour of the vectorized estimator's train-blocking is not exercised locally (96 GB driver). The bundled "default" ghost-GK model ran without issue; the "full" model's serverless memory envelope is out of scope here.

### Neutral

- Extends ADR-029 (silly-kicks 4.0) and ADR-022 (3.0.1): each records a silly-kicks adoption; this one is the first within-4.x minor bump and the first driven by a performance (not correctness-contract) change. Those ADRs stay Accepted.
- Wheel 0.5.6 → 0.5.7 (patch) signals a dependency-floor advance with no lakehouse API change. Stays on 0.x per the "private artifact, not a public PyPI API" posture.

## Related

- **Commits:** TBD (single commit per branch, this PR)
- **Specs:** silly-kicks 4.2.0 release (silly-kicks repo)
- **ADRs:** extends `ADR-029-silly-kicks-4-et-direction-adoption`, `ADR-022-direction-of-play-migration`; references `ADR-028-hexagonal-architecture-for-compute-pipelines` (the `run_work_unit`/`enrich_batch` hexagon the local profiler exercises) and `ADR-012` §2 (the `_REQUIRED_SK_MIN` runtime-assertion contract)
- **External references:** silly-kicks 4.2.0 PyPI release notes; `scripts/profile_ac1_local.py` (new local harness)

## Notes

A/B measured on the Win11 dev box (RTX 5070 Ti / 96 GB), silly-kicks 4.1.1 vs 4.2.0, accessible-space 2.0.15, numba 0.64.0, numpy 1.26.4, scipy 1.15.3, IDSSE J03WMX p1 fixture. The KDE leaf confirms the backend swap is real: 4.1.1 → `evaluate (_kde.py)` (scipy); 4.2.0 → `_kde_density_vectorized (_ghost_gk.py)`. The earlier serverless "appears slower" reading was retracted as contaminated (stale cross-run `wall_s` log-bleed + a run that loaded a pre-4.2.0 silly-kicks); no serverless 4.2.0 timing was ever validly captured.
