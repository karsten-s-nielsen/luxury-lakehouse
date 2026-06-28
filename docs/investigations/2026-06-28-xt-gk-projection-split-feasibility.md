# Reference: feasibility of a cheap `xt_gk` projection split (DEFERRED — not for now)

**Date:** 2026-06-28 · **Author:** xT-GK analysis side · **Status:** Reference note for the deferred R5 split
(see `2026-06-28-xt-grid-fix-decisions.md` — R5 chose the interim full re-materialize; this records what a future
cheap split would need). **No action now.**

## Question
If/when we extract `compute_xt_gk_projection` as a standalone grid-derived stage (to recompute `xt_gk` cheaply on a
grid change instead of re-running the whole AC pipeline), can it run **frame-free** — i.e. reuse already-persisted
intermediates and do only grid arithmetic — or must it re-run the expensive frame-dependent steps?

## What a frame-free projection needs vs. what's persisted
`xt_gk` recompute on a new grid needs these **grid-independent** inputs (everything else is grid arithmetic):

| Input | Grid-independent? | Persisted in `fct_action_context`? |
|---|---|---|
| Pressure ρ | yes | ✅ `xt_gk_pressure` |
| Completion p (RAV `P(success)`) | yes | ✅ `gk_completion` *(confirm it == the served RAV p, not a separate metric)* |
| Possession depth k (for temporal T) | yes | ➖ not stored, but frame-free — cheap to recompute from the action sequence |
| **Resolved origin/dest coords** | yes | ❌ **NOT persisted** — only raw `start_x/y`,`end_x/y` (NaN for ~67% of goal-kicks) + provenance *source* tags |
| xT grid / V_GK lookups, base/pev/rav/dzv/counter/composite | NO (grid-derived) | stored but STALE after a grid change — recomputed, not reused |

## Verdict
- **Not fully cheap with today's schema.** The one missing piece is the **resolved coordinates**. Without them, a
  projection must re-run `resolve_gk_geometry` (frame-dependent — fills the NaN goal-kick origins from tracking),
  which is the expensive part the split is meant to avoid.
- **Small enabling change:** persist the resolved coords as 4 columns (e.g. `xt_gk_origin_x/_y`, `xt_gk_dest_x/_y`).
  silly-kicks already computes them inside `resolve_gk_geometry`; this only surfaces them. With those + ρ + p stored,
  the projection is pure arithmetic (grid + V_GK lookups at stored coords → base/pev/rav/dzv → composite), **no frame
  access** → genuinely cheap, and it preserves the single-pass full-build model (the monolithic build still computes
  everything; the projection is an *additional* cheap refresh path that reuses the wide table).

## Cheap forward-compatible move (optional, no commitment to the split)
Add the 4 resolved-coord columns at the **next** re-materialize. That preserves the cheap-split *option* for ~nothing
and is independently useful (resolved geometry becomes inspectable/joinable). It does not require doing the split.

## Caveats / to-confirm if revived
- Confirm `gk_completion` is exactly the RAV `p` served by silly-kicks (vs. a separate GK-completion metric); the
  provenance cols (`xt_gk_completion_variant`/`_source`) suggest it's the served value, but verify before relying on it.
- A split introduces partial-column writes to the wide Delta table (two writers → MERGE/upsert semantics + a row
  with new `xt_gk` over older neighbours). Acceptable since the columns are independent, but it's added write-
  coordination the monolithic build doesn't have — another reason the interim was the right near-term call.
