# ADR-042: silly-kicks 4.19.2 adoption — ghost-GK serve-mean rename, dtype-contract guard, and 11 new action-context fields

| Field | Value |
|---|---|
| **Date** | 2026-06-08 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The lakehouse ran silly-kicks 4.13.0 (`pyproject.toml`: `silly-kicks[das,ghost-gk]>=4.13.0,<5`). The
4.14.0→4.19.2 range adds three new tracking action-context aggregators, one breaking ghost-GK column
rename plus a value-semantics change, a dtype-contract correctness sweep that shifts several existing
feature values, and CI-only changes. We bump the floor to **4.19.2** (latest; 4.19.2 is a CI/test-infra
release — byte-identical library runtime to 4.19.1 — chosen to honor "most recent version").

This ADR is PR-1 of a three-part program. PR-1 lands code + schema + goldens only: there is **no live
full action-context recompute** (deferred to a later cycle; `fct_action_context` stays at its current
~1,414 sparse rows). PR-2 covers the IDSSE SPADL re-conversion (4.16.1 Sportec/DFL cross-label fix) +
VAEP champion retrain.

Forcing function: "force silly-kicks 4.19.2 everywhere, and add any new fields to action-context."

## Decision

Bump silly-kicks to `>=4.19.2,<5` across every consumer (pyproject, uv.lock, wheel 0.5.23, all trainer
`_REQUIRED_SK_MIN` + PEP-723 URLs, orchestrator scripts, terraform, pin-parity tests); add the 11 new
action-context columns (`structural_lbs`/`sgm`/`sdi`, `xcross_attempt`, and 7 `add_player_influence`
columns) to the AC schema/enrich/dbt/migration layers; rename the ghost-GK spread column
`ghost_gk_spread → ghost_gk_density_spread` (silly-kicks 4.14.0 now serves the boosted-HGBR mean, not
the KDE mode); add a loud `validate_id_dtypes(..., on_mismatch="raise")` pre-flight guard at the
tracking work-unit entry; and regenerate both AC goldens. `add_off_ball_runs` is skipped (its 4 columns
are already produced by the `add_off_ball_context` umbrella).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Pin exactly 4.19.1 (the literally-named version) | Matches the original request verbatim | 4.19.2 published same day, runtime-identical (CI-only); pinning 4.19.1 forces a downgrade of the installed wheel for no runtime gain | Rejected — "most recent" intent + byte-identical runtime |
| B. Drop the GS `_coerce_gradientsports_frame_ids_to_native_str` helper (4.15.0 seam handshake) | Less lakehouse-side code; relies on library seam coercion | Could not be proven safe locally — the only GS enrich fixture (`gradientsports/10517_p3`) trips the ADR-040 absolute-clock time-base guard before reaching the seam, blocking an end-to-end carrier/possession/DAS test | Rejected (KEEP) per Chesterton's fence — default-keep on doubt |
| C. (chosen) Bump to 4.19.2; KEEP the GS coercion; ADD the dtype guard; add 11 fields; rename ghost | Honors "most recent"; strictly safer (additive guard, no behavior removed); full new-field coverage | Carries a transient ghost-GK value semantics flip on existing columns; requires an operator runbook for the synced-table rebuild | — |

## Consequences

### Positive

- 11 new tracking action-context features land in `fct_action_context` (structural-pass primitives,
  per-player influence, cross-attempt propensity), available for downstream consumers.
- Ghost-GK served position is now the boosted-HGBR mean (~1.07 m held-out MAE vs the old KDE mode
  ~4.65 m) — a materially better estimator.
- A loud `validate_id_dtypes` pre-flight now fails fast on any future actions↔frames id-dtype drift
  (the silent-miss class that caused the GS player-id-space bug), exercised on the IDSSE path by the
  always-on mini-golden recompute.
- 4.15.0 dtype-contract correctness fixes (bekkers_pi pressure, cover_shadows, player_influence
  internals) are adopted.

### Negative

- **Hyrum value flip:** every served `ghost_gk_x`/`ghost_gk_y` value changes (KDE mode → boosted mean),
  and the spread column is renamed/redefined. Consumer audit (verified): no runtime / mart / UI reader
  of `ghost_gk_*` exists, so the blast radius is `fct_action_context` + its synced mirror (both rebuilt
  in the §5.2 operator runbook). Both AC goldens were regenerated to capture the shift.
- The ghost-GK rename requires a **one-time operator RENAME migration** that enables Delta
  column-mapping on `bronze.spadl_action_context` — a **one-way protocol bump**
  (minReader=2/minWriter=5), irreversible on that table.
- The rename + gold full-refresh break the `fct_action_context_synced` Lakebase table; recovery is the
  documented delete → full-refresh → recreate (`scripts/create_synced_table.py`) → reapply
  grants/indexes runbook. Skipping it silently degrades the Lakebase-backed app.
- The GS `_coerce_gradientsports_frame_ids_to_native_str` helper is retained as the permanent solution
  (deletion unprovable locally); the lakehouse keeps this small defensive normalization.
- **Next-cycle note:** adding `add_player_influence` + `add_xcross_attempt` (both pitch-control /
  velocity dependent) to the tracking chain raises per-frame cost; the next-cycle full recompute must
  re-check the ADR-037 per-half worker-drain watchdog (2700 s) against the heavier chain before kicking
  off.

### Neutral

- `xcross_attempt` is velocity-dependent (its feature extraction hard-requires ball `vx`), so it runs
  only on the full-tracking path and stays NULL on SB360 freeze-frames — joining DAS / cover_shadows /
  pre_shot_gk in the SB360 velocity-dependent exclusion set. `structural_pass` + `player_influence`
  (voronoi) do run on SB360.
- No live recompute in PR-1: the new columns are NULL in `fct_action_context` until the deferred
  full recompute.

## Related

- **Specs:** `docs/superpowers/specs/2026-06-08-silly-kicks-4-19-1-and-action-context-fields-design.md`
- **Plans:** `docs/superpowers/plans/2026-06-08-silly-kicks-4-19-1-and-action-context-fields.md`
- **ADRs:** builds on ADR-039 (AC GK metrics + SB360 coverage), ADR-019 (dtype-safe id contract),
  ADR-035 (ghost-GK backend), ADR-040 (time-base guard), ADR-037 (worker-drain watchdog).
- **External references:** Karakus & Arkadas (2026) arXiv:2603.28916 (structural pass); Cao et al.
  (2025) arXiv:2505.11841 (xCross, inspired-by); silly-kicks CHANGELOG 4.14.0–4.19.2.

## Notes

The dtype-drop empirical probe (`run_work_unit` on `gradientsports/10517_p3` with the coercion
monkeypatched off) failed at `assert_work_unit_time_base` (period 3 first action t=5401s, absolute
clock) — i.e. the obstacle is the GS fixture's time base, not the seam. A future deletion attempt needs
a period-relative GS enrich fixture; until then the helper stays.
