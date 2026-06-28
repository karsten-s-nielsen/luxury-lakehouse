# Investigation: negative `xt_gk_dzv` in `fct_action_context` is genuine silly-kicks 4.35.0 output

**Date:** 2026-06-28 · **Author:** lakehouse session · **Re:** `BUG_xt_gk_dzv_materialization.md` (xT-GK analysis side)
**Status:** Root cause established. The bug report's central proof is incomplete; the rejection is based on a
wrong root cause. There IS a real underlying concern, but it is upstream/modeling, not a materialization defect.

---

## TL;DR

- The materialized `xt_gk_dzv` values **are the genuine output of released silly-kicks v4.35.0** running on the
  lakehouse's actual global xT grid. They were **reproduced exactly** by running the installed-from-PyPI 4.35.0
  `_dzv` against that grid.
- The bug report's proof ("DZV ≥ 0 by construction under v4.35.0") **assumes the global `V_GK` maximum lies in the
  attacking third**. On the lakehouse's **flat, coarse global xT grid**, φ-amplification of the defensive third
  pushes the **global `V_GK` max INTO the defensive third**, so `(M−1)·V_GK` goes negative for deep keeper origins.
  This is a property of the *input grid*, not evidence of a wrong build.
- **It was NOT a pre-release / split / shadowed silly-kicks install.** The run used correct 4.35.0 (env pinned
  `==4.35.0`, deployed wheel carries the `_REQUIRED_SK_MIN=(4,35,0)` guard, guard passed). The exact numeric match
  rules out the legacy `φ·(v_def−xT)` form the report hypothesized.
- **Re-materializing on a "clean env" will reproduce the identical negatives.** It is not a fix.
- The real issue is a **modeling fragility**: `_dzv` normalizes by the **global** `max V_GK`, which is unstable when
  φ-amplification dominates a flat/low-resolution xT surface. That is an Eyestone/silly-kicks decision (or a
  lakehouse xT-grid-resolution choice) — see *Recommended fixes*.

---

## What the bug report claimed

> "The deployed `xt_gk_dzv` cannot have been produced by v4.35.0: it contains thousands of negative values, while
> v4.35.0's `_dzv` is non-negative by construction … consistent with an intermediate/pre-release silly-kicks build
> (φ multiplied onto the legacy `v_def − xT` term)."

Reported evidence (`data_source='gradientsports'`, WC2022): `n_scored=3458`, `dzv_neg=2274`, `dzv_min=−0.0726`,
`dzv_max=+0.02238`, `dzv_avg=−0.02338`; per-keeper means negative for all 34 cohort keepers.

The report's proof: `M = φ·(1 − V_GK/max V_GK)` with `φ≥1`, `V_GK≥0`, and **`V_GK/max V_GK ≈ 0.01–0.04`** →
`M ≈ 2+ > 1` → `(M−1)·V_GK ≥ 0`. The bolded assumption is the flaw.

---

## Investigation

### 1. The released 4.35.0 `_dzv` source is correct (not legacy)

Inspected the **pip-installed-from-PyPI** `silly_kicks==4.35.0` (`.venv/.../silly_kicks/tracking/_xt_gk.py`).
`_dzv` is exactly the released form, and the normalizer is the **global** surface max:

```python
# _dzv:
m = phi * (1.0 - vgk / vgk_star_max)
in_def_third = start_x < boundary
return np.where(in_def_third, (m - 1.0) * vgk, 0.0)

# compute_xt_gk (caller):
phi_grid = _phi_grid(xt.xT.shape, p.dzv_alpha, p.dzv_beta, p.dzv_d_max, p.defensive_third_boundary)
vgk_star = _convolve_grid(np.asarray(xt.xT, float) * phi_grid, p.convolution_sigma)
vgk_max  = float(np.nanmax(vgk_star))     # <-- GLOBAL max over the whole pitch
```

`PyPI does not allow re-uploading a version`, so the bytes serverless installed = the bytes inspected here = the
released 4.35.0. There is no "intermediate published as 4.35.0" possibility.

### 2. Exact numerical reproduction on the real lakehouse xT grid

Loaded the lakehouse global xT grid (`soccer_analytics.bronze.expected_threat_grids`, `competition_id='global'`)
and ran the **installed 4.35.0** `_phi_grid` / `_convolve_grid` / `_dzv` over defensive-third origins:

| quantity | value |
|---|---|
| xT grid shape (n_y × n_x) | **8 × 12** (coarse) |
| xT value range | **[0.00488, 0.05395]** (flat; peak only 0.054) |
| `vgk_star` global max | **0.07260** |
| **location of `vgk_star` global max** | **inside the defensive third** (`max def-third V_GK / vgk_max = 1.0000`) |
| `_dzv` over def-third origins | **negatives present**; `min = −0.07260`, `max = +0.02216` |

Compare to the report's actual-data numbers: `dzv_min = −0.0726`, `dzv_max ≈ +0.0224`. **Match.**

The smoking gun: `dzv_min = −0.0726` equals **exactly `−vgk_max`**. That value can arise *only* from `(M−1)·V_GK`
evaluated at the global-max cell, where `M = φ·(1 − V_GK/max V_GK) = φ·(1 − 1) = 0` → `(0−1)·vgk = −vgk_max`. The
legacy `φ·(v_def − xT)` form the report hypothesized **cannot** produce `−vgk_max` exactly. So the data is
unambiguously the **4.35.0 `(M−1)·V_GK` form**, on this grid.

### 3. Why the negatives happen (the mechanism)

`φ(z,d) = α·(1 − d/D_max)^(−β)` rises to **2.1–2.85×** across the defensive third (α=2.1, β=0.8). The lakehouse
global xT grid is **flat** (max 0.054, min 0.0049, ~11× range). So φ-weighted defensive-third `V_GK`
(`xT·φ` ≈ up to 0.073) **exceeds** the un-amplified attacking-third `V_GK` (`xT·1` ≈ up to 0.054). Therefore
`max V_GK` lands in the **defensive third**. For any keeper origin with `V_GK / max V_GK > 1 − 1/φ ≈ 0.52–0.65`,
`M < 1` → `DZV = (M−1)·V_GK < 0`. Real GK distributions originate in the **deep** defensive third (near own goal),
where φ is highest and `V_GK/max V_GK → 1`, so the negative regime dominates (report: 66% negative; deep origins
give the strongly-negative `dzv_avg = −0.023`).

The report's `V_GK/max V_GK ≈ 0.01–0.04` assumption holds only for a **sharply-peaked** xT grid (peak ~0.3 at the
attacking goal, near-zero at own goal). The lakehouse global xT grid is not that shape. (Note: `xt_gk_base` and
`xt_gk_rav` read the same grid and the report confirms they are correct / byte-identical to 4.34.0 — so the grid is
the established one, not corrupted. It is simply too flat for `_dzv`'s global-max normalization to behave as
intended.)

### 4. Runtime provenance (no split install)

- The recompute env was verified to pin `silly-kicks[das,ghost-gk,parse-dfl]==4.35.0` (read back from the live job's
  `analytics` environment via the SDK).
- The deployed lakehouse wheel (0.5.53) carries the ADR-044 executor guard at floor `(4,35,0)`; the run completed,
  so the guard did not trip.
- The exact numeric reproduction (§2) fully explains the data with correct 4.35.0 — no split/shadow is needed, and
  the `−vgk_max` identity disproves the legacy-form hypothesis. (Separately, we found and **closed** an ADR-044
  guard *coverage* gap — see below — but it was not exercised here.)

---

## What this means

1. **Not a lakehouse materialization bug.** The pipeline passed the values through faithfully; the values are what
   released 4.35.0 computes on this grid.
2. **Re-materialization will not change anything.** A clean re-run of correct 4.35.0 produces the same negatives.
3. **The rejection is justified by a real symptom but the wrong cause.** Negative DZV does contradict Eyestone's
   intent (DZV as a *positive* revaluation increment, ~+0.009/action). The cause is the interaction of `_dzv`'s
   global-`max V_GK` normalization with a flat/coarse xT surface — a **modeling** decision, not an ops re-run.

---

## Recommended fixes (for the silly-kicks / xT-GK side to choose)

The decision is upstream; options, roughly in order of locality:

1. **Normalize `M` over a region where the assumption holds.** Compute `max V_GK` over the attacking portion (or
   exclude the φ-active defensive third) so the normalizer reflects the "threat ceiling," not a φ-inflated
   defensive cell. This restores `V_GK/max V_GK ≪ 1` for keeper origins → `M > 1` → DZV ≥ 0.
2. **Clamp `M ≥ 1`** (i.e., `DZV = max(0, M−1)·V_GK`). DZV is defined as a *revaluation gain*; a sub-1 multiplier
   (a *penalty*) for being near the φ-amplified max is arguably out of the term's intent. Simple and bounded.
3. **Feed xT-GK a higher-resolution / properly-peaked xT surface.** The lakehouse global grid is 12×8 with peak
   0.054; a finer or differently-scaled xT grid would keep `max V_GK` in the attacking third. (Affects only xT-GK's
   `_dzv` sensitivity; base/RAV are already fine on the current grid.)
4. **Confirm intent.** If Eyestone considers negative DZV acceptable (a keeper distributing from the single most
   φ-amplified cell is penalized), then document it and the lakehouse will accept the values as-is.

Whatever is chosen, please re-confirm the acceptance target (`dzv ≥ 0` and `dzv_avg ≈ +0.01`) is achievable under a
**peaked** grid, or revise the target. The current target is unreachable with global-max normalization on the
lakehouse's flat grid.

---

## Secondary finding (closed on the lakehouse side)

The ADR-044 executor split-install guard (`exec_visibility._SK_GUARD_SUBMODULES`) did **not** cover the xT-GK
submodules. A hypothetical split shadowing only `tracking._xt_gk` would have passed undetected. We have **extended
the guard** to cover `tracking._xt_gk`, `tracking._gk_completion`, `tracking._gk_geometry` (with regression tests +
an ADR-044 amendment). This is preventive defense-in-depth; it was **not** the cause of the DZV negatives.

---

## Reproduce

```python
import os, numpy as np
from analytics.databricks_sql_fetch import query_databricks_sql
from silly_kicks.spadl import config as spadlconfig
from silly_kicks.tracking._xt_gk import XtGkParams, _convolve_grid, _dzv, _grid_value, _phi_grid

df = query_databricks_sql(host, token,
    "SELECT zone_x, zone_y, xt_value FROM soccer_analytics.bronze.expected_threat_grids "
    "WHERE competition_id='global'", warehouse_id)
n_x, n_y = int(df.zone_x.max())+1, int(df.zone_y.max())+1
grid = np.zeros((n_y, n_x))
for _, r in df.iterrows(): grid[int(r.zone_y), int(r.zone_x)] = float(r.xt_value)

p = XtGkParams()
phi = _phi_grid(grid.shape, p.dzv_alpha, p.dzv_beta, p.dzv_d_max, p.defensive_third_boundary)
vgk = _convolve_grid(grid*phi, p.convolution_sigma); vgk_max = float(np.nanmax(vgk))
xs = np.linspace(0.5,34.5,70); ys = np.linspace(0.5,67.5,68)
gx, gy = np.meshgrid(xs, ys)
ov = _grid_value(vgk, gx.ravel(), gy.ravel())
dzv = _dzv(gx.ravel(), ov, vgk_max, p.dzv_alpha, p.dzv_beta, p.dzv_d_max, p.defensive_third_boundary)
print(dzv.min(), dzv.max(), (dzv<0).mean())   # -> ~ -0.0726, +0.0222, negatives present
```
