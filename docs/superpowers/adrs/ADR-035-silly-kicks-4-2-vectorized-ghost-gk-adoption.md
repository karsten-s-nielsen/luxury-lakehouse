# ADR-035: silly-kicks 4.2.0 adoption — vectorized ghost-GK KDE backend + local-fixture profiling methodology

| Field | Value |
|---|---|
| **Date** | 2026-06-01 |
| **Status** | Accepted (amended 2026-06-02 — cpu-numba default; amended 2026-06-03 — fft-cic default; see Amendments) |
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

## Amendment (2026-06-02): adopt `cpu-numba` as the AC-1 default ghost-GK backend

### Context

silly-kicks 4.3.0 added a third KDE backend, `kde_backend="cpu-numba"` (an `@njit` fused loop over the closed-form 2×2 whitened Gaussian, sharing `_kde_setup` with `vectorized`), alongside `scipy` and `vectorized`. The original decision above adopted **vectorized** as the default and concluded (Decision §3) that "the CPU vectorization lever is now spent (~1.24×) … → GPU is justified." A clean local bench of all backends on the real 4.4.0 internals (real bundled "default" model, k = 35 816 = the production regime, accuracy vs the scipy oracle, numba JIT warmed before timing — `tmp/numba_kde_bench.py`) shows that conclusion was **premature**: there is a large *exact* CPU lever (numba) and a larger *approximate* one (FFT) still on the table.

| backend | warm ms/sample | vs vectorized | grid rel-err vs scipy | mode / mean / spread |
|---|---|---|---|---|
| scipy (oracle) | 4949 | 0.88× | — | reference |
| vectorized (prior default) | 4369 | 1.00× | 4e-14 | exact |
| **cpu-numba** | **456** | **9.58×** | **3e-14** | **exact (10/10)** |
| fft-ngp (not yet in silly-kicks) | 2.2 | 1988× | 1.4e-2 | mode 9/10, mean ≤2.5 mm, spread ≤1.1e-3 |

JIT compile is 1.7 s one-time (cached). cpu-numba is **machine-precision identical** to the scipy oracle (mode/mean/spread exact) — strictly more accurate than the approximate FFT and tied with vectorized.

### Decision

Flip the AC-1 production path to `add_ghost_gk(..., kde_backend="cpu-numba")` in `src/analytics/action_context/enrich.py`. This is **value-equivalent within golden tolerance** (the `test_mini_golden` CI gate, ADR-036, stays green — **no golden re-baseline**). No new dependency or floor change: numba is already pinned in `pyproject` and the Terraform serverless analytics env, and 4.4.0 already ships the backend. Wheel 0.5.9 → 0.5.10.

### Correction to the original Decision §3 / GPU gate

The "CPU lever spent → GPU justified" framing is **superseded**: on the dominant ghost-GK stage, cpu-numba delivers **9.6× exact** in-venue (serverless CPU), and FFT/binned-convolution KDE — the eventual silly-kicks lever — is ~2000×. Both are CPU/algorithmic. So **GPU is NOT justified at current scale** (84 tracking matches); the levers are numba now and FFT next. See memory `project_ac1_ghost_gk_gpu_venue_roi` and `project_ac1_numba_das_cost`.

### Reconciliation of the prior "~nil serverless numba" reading

A prior serverless run measured numba at ~nil (1656 s vs 1671 s, PR #325/#326 era). That was **silly-kicks 4.1.1**, where ghost-GK had *only* the scipy backend (vectorized shipped 4.2.0; cpu-numba KDE shipped 4.3.0) — the serverless profile shows ghost-GK = **74 %** running `scipy.gaussian_kde`, and the numba in that run only touched the ~1–2 % pitch-control / ball-carrier kernels → Amdahl ~nil, exactly as expected. It is **not** evidence against the ghost-GK cpu-numba backend, which had never been wired into `enrich.py` (it used the `vectorized` default). cpu-numba attacks the 74 %-dominant stage directly.

### Status of this amendment — serverless speedup PENDING verification

The 9.58× is local CPU. It is **projected** (not yet measured) on serverless: 74 % stage × 9.6× ⇒ skillcorner ~1405 s → ~470 s, a metrica game from timeout (1800 s+) to under budget. Risk is low — value-equivalent + the serverless numba `@njit` infra is already validated working (numba 0.65.1 imports clean, kernels compile, no locator error; ADR/PR #326). A scoped serverless A/B (one metrica game via `preflight_action_context --provider metrica --max-units 1`, or a `submit_ac1_oneshot --wheel-path` dev-wheel run) is the confirming step before this is relied upon as the production speedup. The correctness/value-equivalence half is already proven (mini-golden green); only the magnitude of the serverless speedup is open.

Wheel 0.5.10. Related: ADR-036 (the CI golden gate that makes this backend swap safe to ship), memory `project_ac1_numba_das_cost` / `project_ac1_ghost_gk_gpu_venue_roi`.

## Amendment (2026-06-03): adopt `fft-cic` (CIC bilinear binning) as the AC-1 default ghost-GK backend

### Context

silly-kicks 4.8.0/4.9.0 shipped the FFT/binned-convolution ghost-GK KDE backend in two binning flavours: `fft` (NGP — nearest-grid-point) and `fft-cic` (CIC — cloud-in-cell / bilinear binning). The first amendment's table already anticipated this lever ("fft-ngp (not yet in silly-kicks) … 1988×"). cpu-numba is exact but **cannot finish a full metrica tracking game (141k frames) inside the per-game watchdog** — this is the blocker for the 84 tracking-match backfill on the worker-drain (ADR-037). The FFT backend is the only lever that makes a large tracking game finishable.

### Decision

Flip the AC-1 production path to `add_ghost_gk(..., kde_backend="fft-cic")` in `src/analytics/action_context/enrich.py`. **CIC over NGP** on the data: a local A/B of all three backends on the real fixture (`tmp/ghost_gk_backend_ab.py`, full `run_work_unit` → `enrich_batch` on IDSSE J03WMX p1, all 97 ghost-bearing actions, cpu-numba = scipy-oracle reference):

| backend | mode-exact | flips > 0.5 m | mean Δ | p95 Δ | spread err (mean/max) | ghost-GK stage wall |
|---|---|---|---|---|---|---|
| cpu-numba (ref) | 97/97 | 0 | — | — | — | 106.99 s (1.0×) |
| fft (NGP) | 76/97 (78%) | 12 | 343 mm | 2648 mm | 0.10% / 0.24% | 17.84 s (6.0×) |
| **fft-cic (CIC)** | **92/97 (95%)** | **4** | **97 mm** | **100 mm** | 0.24% / 0.27% | 18.04 s (5.9×) |

CIC is **95% mode-exact vs NGP's 78%** at the **same cost** (the earlier "CIC ~2× slower" concern did NOT reproduce). The two multi-metre fft-cic flips (actions 29, 36: `x≈11.25 → ≈15` at fixed `y`) are genuinely bimodal near-tie grids where the argmax is inherently unstable; entropy/spread is solid (<0.3%) for both.

Unlike the cpu-numba amendment, `fft-cic` is **NOT value-equivalent within bit tolerance** to the scipy oracle — so **BOTH AC-1 goldens were re-baselined to fft-cic** (full `J03WMX_p1/golden.parquet` via the real pipeline; mini `J03WMXmini_p1/golden.parquet` via `scripts/build_ac1_mini_golden.py`). The `test_differential.py` range-checks (`ghost_gk_*` is INVARIANT_ONLY, not oracle-compared) stay green by construction — fft-cic values stay within `x∈[0,105]`, `y∈[0,68]`, `spread≥0`. A nice live signal that fft is active: the always-on `test_mini_golden` recompute dropped from ~30 s to ~9 s.

Floor advanced `silly-kicks[das,ghost-gk]>=4.6.0,<5` → `>=4.9.0,<5` (fft-cic exists only on 4.8.0+; on 4.6.0 `kde_backend="fft-cic"` would error — NGP-only). All floor consumers advanced together per this ADR's original Decision pattern: pyproject `[spadl]`, the Terraform analytics env spec (`terraform/modules/workflows/main.tf`), the 6 trainer `_REQUIRED_SK_MIN` constants → `(4, 9, 0)`, the enforcing sentinel in `test_sk3_mig_b_orchestrator_invariants.py`, and `scripts/submit_ac1_oneshot.py`'s analytics-env mirror. Wheel 0.5.13 → 0.5.14 via `bump_wheel.py`.

### Correction to the first amendment

The first amendment adopted cpu-numba as "machine-precision identical … strictly more accurate than the approximate FFT." That accuracy ranking still holds, but it is **superseded as the production choice**: exact-but-cannot-finish loses to 95%-mode-exact-and-finishes when the alternative is a metrica game that never completes. cpu-numba remains available (`kde_backend="cpu-numba"`) for any exact-required offline use. GPU remains unjustified (the FFT CPU lever closes the gap).

### Consequences

- **Positive:** large tracking games (metrica) finish inside the per-game watchdog; the 84-match tracking backfill is unblocked; no watchdog band-aid extension needed. ~6× ghost-GK speedup on the small fixture (larger on production clouds).
- **Negative:** ghost-GK `x/y` is now a ~95%-exact approximation, not exact — 2/97 fixture actions flip the mode multiple metres on bimodal grids (within the model's own argmax instability). Both goldens re-baselined, so the frozen reference now encodes fft-cic, not the scipy oracle.
- **Neutral:** `tmp/ghost_gk_backend_ab.py` is a throwaway harness (gitignored); the reproducible A/B method is the same local-fixture `run_work_unit` path this ADR established.

Wheel 0.5.14. Related: ADR-036 (the CI golden gate), ADR-037 (the worker-drain this unblocks), memory `project_next_session_cic_ghost_gk_testing` / `project_ac1_ghost_gk_gpu_venue_roi`.

## Third amendment (2026-06-03): selectable `kde_backend` + `ghost_gk_method` provenance + silly-kicks 4.11.0

The second amendment hard-coded `kde_backend="fft-cic"`. This amendment makes the backend **selectable** so batch jobs (not only local one-offs) can opt into the exact backends for higher-accuracy ghost-GK, while `fft-cic` stays the default.

**Decision.**
- **`kde_backend` is domain policy carried per-unit on the `WorkUnit`** (queue = single source of truth across the preflight→drain task boundary), resolved ONCE at the adapter boundary by a pure resolver `analytics.action_context.ghost_gk_backend.resolve_ghost_gk_backend(explicit, installation_default)` with precedence **explicit per-run flag > per-installation default (`AC1_GHOST_GK_BACKEND` env ← Terraform `var.ghost_gk_backend_default`) > `fft-cic`**. The resolver raises `ValueError` (pure domain); the CLI boundary (`_resolve_backend_or_exit`) translates it to `SystemExit`. `WorkUnit.__post_init__` validates the value before it can enter the queue.
- **New `ghost_gk_method` STRING column** on `fct_action_context` records the resolved backend per row. It scopes **only** to the `ghost_gk_*` columns and is **orthogonal** to `pitch_control_method` (PR #337). Justification (Hyrum): the backend is a run-time choice, not inferable from any persisted field. Mart consumers segment on it. RESULT_COLUMNS/DDL 110 → 111.
- **Entry points:** mega-job (preflight `--ghost-gk-backend "{{job.parameters.ghost_gk_backend}}"` stamps every unit; drain reads `unit.kde_backend` off the queue — no drain arg needed) AND `submit_ac1_oneshot.py` (`--ghost-gk-backend` → for-each `compute_action_context`). The per-installation default is the **job-parameter default** `var.ghost_gk_backend_default` (serverless cannot take per-task env vars via TF; the env-var leg is for non-serverless contexts).

**Consequence — re-running with an exact backend OVERWRITES `fft-cic` values.** The exact backends (`scipy`/`vectorized`/`cpu-numba`) produce different `ghost_gk_x/y/spread` than `fft-cic` (95% mode-exact; multi-metre flips on bimodal grids). So the goldens stay on the **default `fft-cic`**, regression tests must not assume a single backend across the live table, and consumers tell backends apart via `ghost_gk_method`. Exact backends are slow on full tracking (the reason fft-cic was adopted) — pair them with the drain `--watchdog-budget-s` override (ADR-037 amendment) or the oneshot `--timeout-seconds`.

**silly-kicks floor 4.9.1 → 4.11.0 (bundled).** Enumerated from the changelog, the only AC-1-relevant numeric change across the span is `ghost_gk_x/y/spread` (4.10.0 serve-carrier consistency fix on ~0.4% of frames + a quality-equivalent default-weights re-fit); xShotOccurrence is bit-identical; DAS/pitch-control/OBSO/PAUSA/shape-graph/line-breaking/team-shape are unchanged. **Both goldens re-baselined** (the change is the library bump, not the backend selection — default stays `fft-cic`); a column-by-column diff confirmed zero churn outside `ghost_gk_*`. 4.11.0's `xCrossAttempt` (TF-17) ships untrained and is **not** consumed (no column). All floor consumers advanced together: pyproject `[spadl]`, the TF analytics env spec, the 6 trainer `_REQUIRED_SK_MIN` → `(4, 11, 0)`, the sentinel in `test_sk3_mig_b_orchestrator_invariants.py`, and `scripts/submit_ac1_oneshot.py`.

Related: ADR-037 amendment (period work-units + watchdog 1800→2700 + override). Code: `src/analytics/action_context/ghost_gk_backend.py`, `enrich.py`, `work_unit.py`, `pipeline.py`, `schema.py`; `src/ingestion/action_context.py`, `action_context_queue.py`; `scripts/submit_ac1_oneshot.py`; `terraform/modules/workflows/main.tf`.
