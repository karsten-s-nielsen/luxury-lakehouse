# Go-ahead: persist xT-GK resolved coordinates in `fct_action_context`

**Date:** 2026-06-29 · **From:** xT-GK analysis side · **Status:** Unblocked — proceed. Runs in parallel with the
silly-kicks audit items (2/4/3-guard); no dependency on them.

## Why
silly-kicks **4.36.0** now emits four resolved-coordinate audit columns from `compute_xt_gk`:
`xt_gk_origin_x`, `xt_gk_origin_y`, `xt_gk_dest_x`, `xt_gk_dest_y` (the **exact** origin/destination the grid lookups
used — including the imputed ~67% of goal-kick origins). Carrying these into `fct_action_context` gives us (a)
per-row external auditability of every `xt_gk`, and (b) the inputs the analysis side needs to verify
**action/frame orientation end-to-end** against the grid (the last substantive our-side check before we'd involve
Eyestone). This was the migration held pending silly-kicks item 1 — it's now ready.

## Key property: additive, zero value change
4.36.0 is **additive** — its CHANGELOG is explicit: *"no behaviour change to any existing `xt_gk_*` value; no retrain
trigger."* The four coords are a parallel `_COORD_COLS` set, deliberately **not** in `_OUTPUT_COLS` (not VAEP
features). So this migration **must not change any existing `xt_gk_*` value** — the WC2022 cohort numbers stay
byte-identical; we're only adding four columns. That byte-identity is the acceptance check (below).

## Tasks
1. **Bump silly-kicks to 4.36.0** (pin already `>=4.35.0,<5`; refresh `uv.lock` to resolve 4.36.0, redeploy the wheel/env).
2. **Schema migration — add four `DOUBLE` columns** through the contract:
   - bronze landing of the silly-kicks AC output (wherever `compute_xt_gk`'s frame is persisted),
   - the staging passthrough `stg_action_context__values.sql` — add the four `cast(... as double)` lines alongside
     the existing `xt_gk_*` casts,
   - the mart `fct_action_context` + its schema/`_marts__models.yml`,
   - a `scripts/migrations/2026-06-29-*.sql` `ALTER TABLE ... ADD COLUMNS` for the existing Delta tables.
3. **Re-materialize the xt_gk path** (the coords only exist if `compute_xt_gk` runs again — they can't be
   backfilled without re-running geometry resolution). Same AC-recompute procedure as the corrected-grid run; since
   4.36.0 is additive, it reproduces the identical `xt_gk_*` values + the four new coords.

## Acceptance
- The four columns exist and are **populated for in-scope GK distributions** (NaN off-scope) in
  `dev_gold.fct_action_context`.
- **Byte-identity guard:** `xt_gk`, `xt_gk_base/_pev/_rav/_dzv/_pressure` are unchanged vs the current
  (corrected-grid) materialization for `data_source='gradientsports'` — confirm a couple of keepers' means match.
  If any value moved, stop — something other than the additive coord emit changed.
- Spot-check the coords are sane: GK-distribution origins land in the **own defensive third** (low LTR x), goal-kick
  origins at the rule point (~5.5, 34); ping the analysis side.

## Sequencing / notes
- Independent of silly-kicks items 2 (parity), 4 (golden composite test), 3-guard (orientation guard) — those are
  tests/docs, no release needed; this migration only needs 4.36.0.
- This does **not** touch the xT grid or the ADR-063 watermark work — it's a one-time additive column add.
- After it lands, the analysis side runs the end-to-end orientation verification using the persisted coords; that
  closes the last open our-side check (item 3 live half).
