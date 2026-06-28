> **⚑ SUPERSEDED (2026-06-28) by `2026-06-28-xt-grid-stale-not-directionality-root-cause.md`.** The *symptom*
> here is correct (the `global` grid is non-directional, which drives the negative DZV). The *cause* I named is
> wrong: `compute_xt_grid_hf.py` is NOT the live writer (it publishes an HF dataset + dbt-seed CSV), and the source
> SPADL is already LTR so no orientation step is needed. The live grid is **STALE** — frozen 2026-05-02 mid-LTR-
> migration, never refreshed because the rebuild guard only builds-if-absent. See the lakehouse follow-up for the
> correct root cause + fix. Retained for history; **act on the staleness doc, not this one.**

---

# Root cause confirmed: the `global` xT grid is non-directional (orientation bug in the grid build)

**Date:** 2026-06-28 · **Author:** xT-GK analysis side · **Re:** `2026-06-28-xt-gk-dzv-negative-root-cause.md`
**Status:** Your refutation is correct and accepted. Root cause located one level deeper — in the xT grid, not `_dzv`.

---

## TL;DR

- **You were right:** the negative `xt_gk_dzv` is genuine released-v4.35.0 output, reproduced exactly; the
  `dzv_min = −vgk_max` identity proves it. Not a materialization defect, not a pre-release/shadow install. My
  earlier `BUG_xt_gk_dzv_materialization.md` had the right symptom and the wrong cause — it's withdrawn.
- **The deeper cause:** the `global` xT grid `_dzv` normalizes against is **non-directional** — attacking-goal xT ≈
  own-goal xT. Confirmed by querying every grid in `bronze.expected_threat_grids`.
- **Why it produces negative DZV:** a symmetric grid makes the keeper's own-goal zone spuriously high-xT; φ
  (2.1–2.85×) amplifies it so `max V_GK` lands in the defensive third → `M = φ·(1 − V_GK/max V_GK) < 1` →
  `(M−1)·V_GK < 0`. `_dzv` is simply the first component sensitive enough to expose a grid that's been wrong all along.
- **Fix is upstream and singular:** fix orientation in `scripts/compute_xt_grid_hf.py`, add a directionality
  assertion, rebuild grids → re-materialize `action_context`. DZV ≥ 0 then falls out for free, and base/RAV/PEV get
  a correct surface for the first time. The silly-kicks `_dzv` normalizer hardening becomes optional, not the fix.

## Evidence (reproducible)

```sql
SELECT competition_id,
  ROUND(AVG(CASE WHEN zone_x=0  THEN xt_value END),5) x0_own_goal,
  ROUND(AVG(CASE WHEN zone_x=11 THEN xt_value END),5) x11_att_goal,
  ROUND(AVG(CASE WHEN zone_x=11 THEN xt_value END) / NULLIF(AVG(CASE WHEN zone_x=0 THEN xt_value END),0),2) att_to_def_ratio
FROM soccer_analytics.bronze.expected_threat_grids
GROUP BY competition_id ORDER BY att_to_def_ratio;
```

A correct xT surface has `att_to_def_ratio ≫ 10` (own-goal xT ~0.002, attacking ~0.1–0.3). Actual:

| grid | att/def ratio | reading |
|---|---|---|
| **global** (the one xT-GK consumes) | **0.98** | symmetric — own goal ≈ attacking goal |
| comps 0,102,1238,1470,2,426,37,9,524,16,12 … | 0.97–1.17 | symmetric |
| comps 28, 81, 364, 412, 55 | **0.55–0.89** | **inverted** (defensive end higher) |
| comps 35, 116, 87 (best) | 2.8–4.9 | weakly directional, still far short of ≫10 |

The grid's per-`zone_x` mean is a **U** (high at both ends): `0.0176 (x0) → 0.0053 (x5–6) → 0.0172 (x11)`. The
build's own convention (`compute_xt_grid_hf.py:276-277`) labels `x=0` defensive / `x=11` attacking — so the grid is
the inverse of what the builder intends.

## Where it goes wrong (`scripts/compute_xt_grid_hf.py`)

An orientation step **exists** (`_normalize_attack_direction`, called L243) but is not producing directional grids.
Two suspect spots:

1. **No-shot groups are left unflipped.** The flip is decided per `(match_id, team_id, period)` from shot
   clustering; groups without shots (and whose other period also lacks shots) are *assumed correct* (L131) — so any
   mis-oriented no-shot team-period passes through.
2. **The "teams swap sides each half" inference (L129) assumes RAW physical coordinates.** If the source SPADL in
   `spadl-vaep-action-values` is already attack-normalized (standard socceraction LTR — every action already attacks
   +x), then a per-period swap-flip *corrupts* already-correct data toward ~50/50. Worth checking the source SPADL's
   orientation contract first — if it's already LTR, this normalizer may need to become a no-op / be removed rather
   than fixed.

**Validation gap:** `global_grid.validate_structural(max_value=0.50)` (L269) only bounds the max; it does **not**
assert directionality. The build even prints `Zone x=0` vs `Zone x=11` (L276-277) — they came out ≈ equal — but
nothing fails on it, so the broken grid shipped.

## Blast radius (please scope)

`xt_gk_base` and `xt_gk_rav` being "byte-identical to 4.34.0" means they've been running on this non-directional
surface all along — stable, but a forward pass and a backward pass get conflated xT. Any metric that consumes the
`global` grid inherits this. Does VAEP / EPV-transition (or anything else) read the same grid? If so, this fix has
reach beyond xT-GK.

## Fix & acceptance

1. Fix orientation in `compute_xt_grid_hf.py` (repair or remove `_normalize_attack_direction` per the SPADL
   orientation contract).
2. Add a **directionality assertion** to `validate_structural` (or the build): require, on the global grid,
   `mean(xt[x=11]) / mean(xt[x=0]) ≥ ~10` (and monotone-ish rise across `zone_x`). Fail the build otherwise.
3. Rebuild grids → re-seed `bronze.expected_threat_grids` → re-materialize `fct_action_context`.
4. Acceptance:
   - the query above returns `att_to_def_ratio ≫ 10` for `global` (and sane values per competition),
   - `SELECT COUNT(*) FROM fct_action_context WHERE data_source='gradientsports' AND xt_gk_dzv < 0` → **0**,
   - `xt_gk_dzv` per-keeper means land ~**+0.01** (Eyestone's ~0.009 anchor),
   - `xt_gk_pev` per-keeper means rise non-trivially above the prior raw-surface baseline.
5. Ping the xT-GK analysis side to re-run the WC2022 cohort + report.

> The silly-kicks `_dzv` global-max-`V_GK` normalization is still somewhat grid-shape sensitive; hardening it
> (attacking-region max, or `max(0, M−1)·V_GK`) is reasonable defense-in-depth, but it is **not** required once the
> grid is directional. That call sits with the silly-kicks / Eyestone side.
