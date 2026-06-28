# Root cause (corrected): the `global` xT grid is STALE (frozen pre-LTR-migration), not a live orientation-build bug

**Date:** 2026-06-28 · **Author:** lakehouse session · **Re:** `2026-06-28-xt-grid-directionality-root-cause.md`
**Status:** Symptom confirmed (grid is non-directional). Root cause is one level deeper than the orientation
normalizer — and the file your report names (`compute_xt_grid_hf.py`) is **not** the live producer. Fixing only
that script would not fix the live grid.

---

## TL;DR

- **You're right about the symptom:** the `global` xT grid is non-directional (U-shaped, att/def ratio **0.98**),
  reproduced exactly. And yes, that symmetric grid + φ-amplification is what drives the negative `xt_gk_dzv`.
- **The cause is NOT an orientation normalizer.** The live `bronze.expected_threat_grids` is written by the daily
  Spark pipeline **`src/ingestion/expected_threat.py`** (`compute_expected_threat`), which does **no** orientation
  normalization at all. `scripts/compute_xt_grid_hf.py` (the file your report blames, with `_normalize_attack_direction`)
  publishes to an **HF dataset + a dbt-seed CSV — it does not write the live bronze table.**
- **The source SPADL is already LTR** (99.8–100% of shots in the attacking half, all providers, all periods), and the
  **live core compute is correct**: run on a current LTR sample it produces a properly directional grid (monotonic
  x0→x11, att/def ratio **9.55**, max xT 0.17).
- **The live grid is simply STALE.** `bronze.expected_threat_grids` was last written **2026-05-02** and never since —
  a frozen snapshot from when the SPADL was still mid-/pre-LTR-migration (heterogeneous orientation across
  competitions). The rebuild guard **never recomputes an existing grid**, so ~2 months of LTR data never refreshed it.
- **Two compounding issues** make this sticky: (a) `validate_differential(max_relative_change=0.30)` would actively
  **reject** the correct rebuild (0.054 → 0.17 max is a >200% jump), and (b) `validate_structural`'s directionality
  check is too lax (a gentle U passes), so the stale grid was never flagged.
- **Fix:** force a clean rebuild of all xT grids from current LTR data (bypassing the differential guard for the
  one-time correction), tighten the directionality assertion, and fix the rebuild trigger so grids refresh when the
  data changes — not only when a grid is absent. The `compute_xt_grid_hf.py` normalizer is a side issue (see §5).

---

## Evidence

### 1. Symptom reproduced (grid is non-directional)
`bronze.expected_threat_grids`, `competition_id='global'`, per-`zone_x` mean is a **U**:
`0.0176 (x0) → 0.0053 (x5–6) → 0.0172 (x11)`; att/def ratio **0.98**. Per-competition: most 0.97–1.3 (symmetric),
28/81/364/412/55 inverted (0.55–0.89), 35/116/87 weakly directional (2.8–4.9). Matches your report.

### 2. Source SPADL is already LTR (so no normalization is needed)
`bronze.vaep_action_values` (the publish source) shots, by period:

| period | n_shots | avg start_x | % attacking half |
|---|---|---|---|
| 1 | 62,017 | 89.8 | **99.9%** |
| 2 | 72,779 | 90.0 | **99.8%** |
| 3/4/5 | small | ~89 | 91–100% |

By provider: gradientsports 97.9%, idsse 100%, metrica 100%, skillcorner 99.6%, statsbomb 99.9%, wyscout 99.7%.
**Every team attacks +x, in both halves — canonical LTR (ADR-022).**

### 3. The live core compute is correct on LTR data
Ran `analytics.expected_threat.compute_expected_threat_grid` (the exact function the live pipeline calls) on a
current LTR sample (179,736 actions, shots 99.8% attacking):

```
per-zone_x means: x0=0.0071  x1=0.0070 ... x6=0.0113 ... x10=0.0481  x11=0.0674   (monotonic rise)
att/def ratio = 9.55 ;  max xT = 0.170 at zone_x=11
```
So the core produces a **correct directional grid** from LTR data. The bug is not in the math.

### 4. The live producer + writer
- **Live writer:** `src/ingestion/expected_threat.py::run_pipeline` (daily `compute_expected_threat` task) reads the
  **LTR** gold mart `dev_gold.fct_action_values`, buckets into `ZoneCounters`, runs value iteration,
  `write_delta_table` → `bronze.expected_threat_grids`. **No `_normalize_attack_direction` anywhere in this path.**
- **Not the writer:** `scripts/compute_xt_grid_hf.py` reads the `spadl-vaep-action-values` HF dataset, runs its own
  copy of `_normalize_attack_direction`, and publishes to the **HF dataset** `expected-threat-grids` + a dbt-seed CSV.
  It does not write `bronze.expected_threat_grids`.

### 5. The grid is frozen (stale)
`DESCRIBE HISTORY soccer_analytics.bronze.expected_threat_grids` — latest version **101, 2026-05-02 16:42**; all
recent writes are 2026-05-02. `fct_action_values` is active through 2026-06-24. The grid has not been recomputed in
~2 months. The per-competition directionality scatter (some directional, most symmetric, a few inverted) reflects the
**heterogeneous SPADL orientation state as of 2026-05-02** (a partial-migration snapshot), frozen ever since.

### 6. Why it never refreshed (the enabling bug)
`expected_threat.py::_ExpectedThreatGuard.check`:
- `new_comps = find_new_ids(...)` → returns only competitions **absent** from the results table; an existing
  competition grid is **never recomputed**.
- `need_global = "global" not in existing` → the global grid is rebuilt **only if missing**; once present, never again.

So after the LTR migration landed, no mechanism recomputed the already-present grids. Worse,
`XTGrid.validate_differential(max_relative_change=0.30)` would **reject** the corrected grid (0.054 → 0.17 peak), and
`validate_structural`'s monotonicity check (`np.all(np.diff(row_means) >= -0.01)`) is too lax — the gentle U
(per-step drop < 0.01) passes, so nothing failed.

---

## Blast radius

Consumers of the `global` grid (all running on the stale symmetric surface):
- **xT-GK** (`action_context`): `base`/`rav`/`pev`/`dzv` — the original symptom; DZV negatives are downstream of this.
  ("base/rav byte-identical to 4.34.0" is because the grid hasn't changed since 2026-05-02 — stable but wrong.)
- **Off-ball xT** (`ingestion/off_ball_xt.py::_load_xt_grid_from_spark`, `competition_id='global'`) — affected.
- **Legacy `fct_tracking_context`** (`ingestion/tracking_context.py`) — affected (legacy path).
- **VAEP** (`fct_action_values`): independent — silly-kicks' own value model, does **not** read this grid.
- **EPV-transition** (`scripts/compute_epv_transition_hf.py`): builds its **own** reachability/EPV grids from the same
  HF SPADL dataset; not this table, but worth a separate orientation/freshness check (likely the same class).

---

## Recommended fix

1. **Rebuild all xT grids from current LTR data.** The current core is correct; it just needs to run on today's data.
   Operationally: wipe `bronze.expected_threat_grids` (or the `global` + affected per-comp rows) so the guard treats
   them as absent, then run `compute_expected_threat`. The first corrected build will trip
   `validate_differential` (legit — it's a deliberate correction); bypass/seed it for this one run.
2. **Tighten the directionality assertion** in `XTGrid.validate_structural`: require the global grid to be materially
   directional — e.g. `mean(xt[x=-1]) / mean(xt[x=0]) >= ~5` and a steeper monotonic rise than the current `-0.01`
   per-step tolerance. Fail the build otherwise (this is what should have caught the stale U).
3. **Fix the rebuild trigger** so grids recompute when the underlying data/orientation changes, not only when a grid
   is absent (e.g. periodic full rebuild, or a data-version/orientation fingerprint in the guard). Otherwise the grid
   silently re-staled the next time the SPADL contract shifts.
4. **Re-materialize `fct_action_context`** after the grid is correct (xT-GK base/rav/pev/dzv all change; DZV ≥ 0 then
   falls out for free, as your report notes). Also re-check off-ball xT.
5. **Side issue — `compute_xt_grid_hf.py::_normalize_attack_direction`:** not the live writer, but it **is** wrong for
   LTR input (its no-shot "teams swap sides" inference flips already-correct no-shot team-periods). If that script is
   still used to seed the HF dataset / dbt seed, make the normalizer a no-op (the source is LTR) or delete it, and add
   the same directionality assertion there.

## Acceptance (unchanged from your report — the targets are right; only the path differs)
- `att_to_def_ratio ≫ 10` for `global`;
- `COUNT(*) WHERE data_source='gradientsports' AND xt_gk_dzv < 0` → **0**;
- `xt_gk_dzv` per-keeper means ~**+0.01**; `xt_gk_pev` means rise above the prior baseline.

> Note: once the grid is directional, the silly-kicks `_dzv` global-max normalization is no longer pathological here
> (keeper-zone V_GK ≪ attacking-zone V_GK again), so DZV ≥ 0 without upstream changes. Hardening `_dzv`
> (attacking-region max, or `max(0, M−1)·V_GK`) remains optional defense-in-depth on the silly-kicks side.
