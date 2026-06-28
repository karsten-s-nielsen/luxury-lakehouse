# ADR-044: Executor-side silly-kicks env-drift guard for the action-context UDF

| Field | Value |
|---|---|
| **Date** | 2026-06-09 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

On 2026-06-09 the `compute_action_context` mega-job task crashed inside silly-kicks on GradientSports match 10504 (`_ghost_gk.py:1827`, `ValueError: Must have equal len keys and value`) — the dual-goalkeeper ghost-GK case that silly-kicks **4.12.1** fixed with a `(frame, gk_team)` dedup and that **4.20.1** (our pinned floor) still carries.

Root cause was **not** a code or library bug. It was **environment version drift inside the serverless `applyInPandas` executor sandbox.** Every governance surface we own was correct and current — `pyproject.toml` floor `silly-kicks[das,ghost-gk]>=4.20.1`, `uv.lock` 4.20.1, `bump_wheel.py`/`WHEEL_VERSION` 0.5.25, and the live job's `analytics` environment dependency (`luxury_lakehouse-0.5.25` + `silly-kicks>=4.20.1`, verified via the SDK). But the UDF executor ran silly-kicks **4.12.0** code: the crash line (1827) and a no-dedup file matched 4.10–4.12.0 by version bisect, and the same executor ran a #339-era wheel (`pipeline.py:255`, vs 277 on the deployed commit). Per-iteration logs showed the executor `AC1_ENVFP` fingerprint reporting `silly_kicks_version=4.20.1` **while the crash executed 4.12.0 submodule code**, across multiple `pythonEnv-*` ids in a single iteration — i.e. the sandbox has **two silly-kicks installs on `sys.path`**: `silly_kicks.__init__` resolves from the 4.20.1 layer (so `__version__` reads 4.20.1) while `silly_kicks.tracking._ghost_gk` resolves from a stale 4.12.0 layer. The drift is **intermittent** (non-uniform across the executor pool) and **invisible** to our existing telemetry, because the only version check we had (`action_context._iteration_fingerprint`, and `executor_env_fingerprint`) reads `silly_kicks.__version__` — which the split install fools.

None of our pinning controls `sys.path` layering or warm-process module caching inside the serverless UDF sandbox. So we cannot *prevent* the drift from the lakehouse side; we can only make it **fail loud and immediately** instead of crashing randomly several enrich-steps later (or, worse, silently computing wrong tracking features on a provider whose data shape doesn't trip a hard error).

## Decision

Add an **executor-side guard** (`ingestion.exec_visibility.assert_executor_silly_kicks_sane`) called at the top of the action-context UDF, immediately after the env fingerprint. It (1) asserts `silly_kicks.__version__ >= _REQUIRED_SK_MIN` (kept in lockstep with the pyproject floor by a sentinel test), and (2) — because `__version__` is fooled by a split install — asserts every load-bearing silly-kicks submodule (`tracking._ghost_gk`, `tracking.features`, `tracking.pitch_control`, `tracking._xt_gk`, `tracking._gk_completion`, `tracking._gk_geometry`, `xthreat`, `spadl`) loads from the **same install root** as `silly_kicks` itself. Any mismatch raises `RuntimeError` on the first batch the contaminated executor handles.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Assert `silly_kicks.__version__ >= floor` only | one-line, cheap | **fooled by the split install** — `__version__` reads 4.20.1 while submodules run 4.12.0; would not have caught this crash | insufficient |
| B. Trust the existing `executor_env_fingerprint` log + alert | no new code | passive (nobody reads envfp markers absent an investigation); reports the same fooled `__version__` | does not fail loud |
| C. Pin silly-kicks to an exact `==` in the env | tighter | still installed on the *task* env, not the UDF sandbox; does not touch the warm-pool layering | wrong layer |
| D. Version floor **+ submodule install-root provenance check** (chosen) | catches both the uniformly-stale and the `__version__`-lying split; fail-loud on batch 1; cheap (process-local one-shot) | does not *fix* the drift — needs a clean executor-env rebuild as remediation | — |

## Consequences

### Positive

- Silent, intermittent executor env-drift becomes an immediate, self-describing failure ("rebuild the serverless executor environment clean") on the first contaminated batch — no more multi-hour random-crash investigations.
- Generalises beyond this incident: any future silly-kicks executor drift (stale OR split) is caught at the boundary, on every provider, not just when a version-specific edge case happens to crash.
- The floor sentinel (`test_required_sk_min_matches_pyproject_floor`) keeps the guard's required version honest against the pyproject pin.

### Negative

- The guard makes drift **fail** rather than **fix** it: until the serverless executor environment is rebuilt clean (warm-pool re-provisioned), AC runs on a contaminated executor will hard-fail by design. That operational rebuild is the actual remediation; the guard only enforces it.
- One extra forced import of the guarded submodules per executor process (negligible — the enrich chain imports them moments later anyway).

### Neutral

- `_REQUIRED_SK_MIN` is a second place the silly-kicks floor lives; the sentinel test makes the duplication safe.

## Related

- **Issues / PRs:** #358 (the 4.20.1 adoption whose executor never reached the UDF sandbox)
- **ADRs:** complements ADR-031 (executor visibility on Spark Connect), ADR-030/ADR-029 (GS ghost-GK / dual-GK lineage)
- **Code:** `ingestion/exec_visibility.py::assert_executor_silly_kicks_sane`, `ingestion/action_context.py` UDF, `src/tests/test_executor_env_guard.py`

## Amendment (2026-06-28) — xT-GK surface added to the guarded submodules

The xT-GK DZV investigation (silly-kicks 4.35.0 adoption) flagged a **coverage gap**: the
guarded-submodule list omitted the xT-GK surface (`tracking._xt_gk`, `tracking._gk_completion`,
`tracking._gk_geometry`). A split shadowing only `_xt_gk` (the PEV/DZV/base/RAV math) would have
passed the guard while silently producing wrong `xt_gk_*` columns. The three xt-gk submodules are
now in `_SK_GUARD_SUBMODULES` (regression tests `test_split_install_xt_gk_raises` +
`test_xt_gk_surface_guarded`). Note: that investigation determined the actual 4.35.0 recompute was
**not** affected by a split — the negative `xt_gk_dzv` values were the genuine output of released
4.35.0 on the lakehouse's flat/coarse global xT grid (the global `V_GK` max falls inside the
defensive third, inverting `(M−1)·V_GK`). This amendment closes the guard gap as defense-in-depth;
it was not the cause of that finding.

## Notes

Version-bisect evidence (downloaded PyPI wheels): silly-kicks 4.10.0/4.11.0/4.12.0 all place the crashing `out.loc[gk_mask, "ghost_gk_x"] = ….values` at line **1827** with no dedup; 4.20.1 places it at **2011** with the `(frame, gk_team)` dedup at 1968–1983. Production crashed at 1827 → pre-4.12.1.
