# xT-grid fix — open-decision sign-off (M6, R5) + the coupling that makes R5 safe

**Date:** 2026-06-28 · **Author:** xT-GK analysis side (decisions confirmed by Karsten) · **Re:**
`2026-06-28-xt-grid-fix-plan-review.md`, plan + ADR-063.

## M6 — per-competition grids: KEEP-AND-GUARD

ExT v2 (per-competition xT) is **active/committed**, so the per-comp grids are roadmapped, not dead. Decision:

- **Keep** computing the per-competition grids.
- **Guard them:** apply the directionality assertion to per-comp grids **above a min-action threshold** (small/noisy
  competitions are exempt to avoid false-fails). This is the non-negotiable part — the per-comp grids were the ones
  found *inverted* (ratio < 1); keeping them **unguarded** is the worst option.
- This **resolves the M5 ADR↔plan mismatch in the ADR's favour:** the plan must actually implement per-comp
  `require_directional` gating, not only pass it for `global`. Define and document the min-action threshold (and a
  test for a below-threshold comp that must NOT false-fail).

## R5 — interim full re-materialize; DEFER the projection split

Do **not** put the `compute_xt_gk_projection` AC-pipeline refactor in this initiative. Instead:

- On a **material** grid change, **full-AC-all-matches re-materialize** (correct — the grid is global, so every
  match's xt_gk is invalidated; a match subset would be wrong).
- The waste (recomputing grid-independent features) is **acceptable because it is bounded by R4** — material grid
  changes are rare post-convergence, so the full pass runs seldom.
- Revisit the projection split **later, as a measured optimization**, only if the re-materialization cost actually
  proves painful. Don't pre-emptively refactor the critical path that's blocking the xT-GK re-run.

## ⚠ The coupling: R5-interim is only safe if R4 is done right

Because R5 leans entirely on R4 to keep the expensive full re-materialize rare, **R4 is now load-bearing, not
optional**, and two of its properties are mandatory (from the review):

1. **Material-only propagation.** Only a grid change exceeding the materiality threshold writes the grid + triggers
   the full-AC re-materialize. (Pick ε by *measuring* real append-to-append drift first; prefer a relative/zone-aware
   measure since xt_gk lives in the low-value 0.007–0.02 defensive zones where a flat absolute ε is least sensitive.)
2. **Drift-bounded against the last *propagated* grid.** Gate the change vs. the grid consumers were last
   materialized on — NOT vs. the previous compute. Otherwise slow sub-ε drift (e.g. 0.5·ε/day) never trips the gate
   but accumulates unbounded, and `fct_action_context` silently drifts from the grid — re-introducing the exact
   staleness class one hop down (review H1). This is the property that makes "interim full re-materialize" correct
   rather than a slow-drift generator.

If R4 ships without (1)+(2), revert R5 to the projection split — full-AC re-materialize on *every* grid change is not
viable.

## Still open from the review (unchanged)
- **H1** (consumers must watermark on the grid) and **H3** (`validate_differential` × watermark deadlock / record-
  on-failure) remain blockers before the one-time rebuild.
- **H4** (cross-cutting staleness monitor) recommended as the interim backstop for Tiers B/C.
- **M7** (`fct_tracking_context` re-materialize?), **M8** (real rollback via Delta time-travel, not row-count
  snapshot), and the L-items stand.
