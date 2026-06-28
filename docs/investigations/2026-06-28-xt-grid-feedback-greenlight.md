# Feedback + green-light on the stale-grid root cause

**Date:** 2026-06-28 · **Author:** xT-GK analysis side · **Re:** `2026-06-28-xt-grid-stale-not-directionality-root-cause.md`
**Status:** Your diagnosis is accepted. It's better than mine. Proceed with the rebuild — four refinements below.

## Accepted

Staleness is the correct root cause. You're right that `compute_xt_grid_hf.py` is not the live writer, that the
source SPADL is already LTR, and that the live core is correct (att/def 9.55 on current data). The frozen-2026-05-02
grid + the build-if-absent guard explains everything — including the per-competition scatter and the base/rav
stability. My `2026-06-28-xt-grid-directionality-root-cause.md` is marked SUPERSEDED, pointing here.

## Four refinements before you run it

1. **Lower the acceptance bar from `≫10` to ≥5–7.** Your own corrected grid measured **att/def = 9.55**, so the
   `≫10` target I wrote (and you carried into Acceptance) would marginally fail the *correct* grid. Make the
   acceptance threshold match your `validate_structural` assertion (you proposed ≥5). Set both at ≥5 (global landing
   ~9–10 with margin).

2. **The guard fix (your #3) is the systemic priority — generalize it.** "Build-if-absent, never recompute" silently
   froze this artifact for ~2 months. Please check whether the **same guard pattern** freezes other derived
   artifacts (xG grids, EPV reachability, any model with a `find_new_ids` / `not in existing` gate). The recurrence
   risk is broader than this one table; a data-version/orientation fingerprint in the guard would cover all of them.

3. **Make the `validate_differential(0.30)` bypass explicit, logged, one-time** — not a lingering disable. And
   confirm the **inverted** per-competition grids (ratio < 1) are wiped too, not just the symmetric ones, if any
   consumer reads per-competition rather than `global`.

4. **Heads-up on downstream scale (for the re-materialize + our acceptance):** the corrected grid is both directional
   *and* ~3× higher amplitude (max 0.054 → 0.17). So every xT-GK term **rescales**, not just DZV's sign: keeper
   origins drop to low xT (base less negative), forward destinations rise (rav up), and PEV's forward gain on a
   directional surface should become **non-trivially positive** for the first time. Expect the xT-GK numbers to move
   a lot. The acceptance targets `dzv ≥ 0` and `dzv_avg ≈ +0.01` are directionally right, but the `+0.01` magnitude
   was calibrated on Eyestone's grid — treat it as a sanity band, not a hard gate, until the analysis side
   re-baselines against the new surface. We'll do a full xT-GK re-analysis (not just a `dzv ≥ 0` check) once you ping
   us that the grid + `fct_action_context` are rebuilt.

Targets otherwise unchanged: `att_to_def_ratio ≥ ~5` (global), `COUNT(*) WHERE xt_gk_dzv < 0 → 0`, `xt_gk_pev` means
up vs the prior baseline. Good to proceed.
