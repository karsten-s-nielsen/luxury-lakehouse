# ADR-048: silly-kicks 4.22.0 adoption — xT-GK column family, restart coordinates, SkillCorner result_source

| Field | Value |
|---|---|
| **Date** | 2026-06-10 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen, Claude |

## Context

silly-kicks 4.20.1 → 4.22.0 ships three lakehouse-relevant changes (upstream ADR-024/ADR-025):
**xT-GK** (Eyestone, Pitch to the Pros winner; tracking-required GK-distribution valuation with a
provider-aware logistic completion model and a 4.21.4 per-type base-rate honesty gate), the
**SkillCorner `result_id` native-completion fix** (the old possession proxy overstated goal-kick
success ~16 pp; new per-row `result_source` label tier; upstream flags it a **VAEP-retrain
trigger**), and **`add_restart_coordinates`** (additive Law-fixed-spot coordinate imputation for
restarts — built for GS set-pieces, whose origins are ~67% NaN; canonical coords never mutated).
This adoption is the final schema gate before the full all-provider AC recompute (the AC table is
wiped as part of that operational sequence).

## Decision

Adopt 4.22.0 everywhere (pyproject floor + uv.lock + terraform `==` pins + `_REQUIRED_SK_MIN`,
per ADR-046 lockstep). Add **17 columns to `fct_action_context`/bronze AC**: the xT-GK composite
under **all five upstream philosophy presets as named columns** (`xt_gk` = library default +
`xt_gk_{possession,counter,direct,high_press,low_block}`), the 5 raw components + 5 provenance
columns (stored once, default params), and `gk_completion`. Add **9 columns to
`bronze.spadl_actions`/`vaep_action_values`**: `result_source` + the 8 restart-coordinate
enrichment columns (events-only tiers at the bronze writer).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Default-preset composite only | 13 AC cols | δ enters the stored rav term and η the unstored temporal factor — other presets are NOT client-side derivable; later additions = schema migration + full AC re-materialization | re-valuation per preset is cheap (GK actions only) |
| B. Defer restart coords to a later PR | smaller PR | SPADL re-conversions are happening NOW (SkillCorner + IDSSE); deferring forces a SECOND re-conversion later | one re-conversion pass populates everything |
| C. Hand-roll completion-variant resolution (`variant_key_for_provider` + `from_variant`) | public-ish API | the mapper returns key `"gs"` but the bundled weights dir is `"default"` → `from_variant("gs")` raises FileNotFoundError | reuse `_resolve_completion_for_frames` (the resolver `add_xt_gk` itself uses); wrinkle reported upstream |
| D. All five presets + both new SPADL families now (chosen) | one schema pass, one recompute, full philosophy queryability | 26 new columns across two tables; preset values are upstream-provisional | — |

## Consequences

### Positive
- xT-GK queryable per philosophy without re-materialization; `gk_completion` doubles as a
  standalone GK metric (it IS the P(success) RAV consumes — train==serve parity upstream).
- GS set-piece actions gain usable coordinates (provenance-tagged, tripwire-guarded) without
  touching canonical coords — no retrain trigger from the coordinate side.
- `result_source` records the SkillCorner label tier the VAEP v8 training set is built on.

### Negative
- The five preset composites freeze upstream-provisional parameter values into the mart; a future
  upstream re-tuning of the presets is a value change requiring AC re-materialization.
- SkillCorner SPADL re-conversion + **VAEP retrain (v8) + full re-score** are now mandatory
  follow-ups (with IDSSE's pending 4.16.1 cross-fix re-conversion) before the AC recompute.

### Neutral
- xT-GK columns are NULL for event-only providers and non-GK-distribution actions by design.
- The enrichment runs `compute_xt_gk` 5× per batch (1 wrapper + 4 presets… plus default = 6
  valuations); scope is GK-distribution actions only — immaterial vs ghost-GK/DAS.

## Related
- **ADRs:** ADR-046 (pin lockstep), ADR-047 (batch 2500 — the recompute this gates), ADR-016
  (SPADL enrichment home), ADR-013 (global xT grid injected as the xT-GK baseline)
- **Upstream:** silly-kicks ADR-024 (xT-GK), ADR-025 (restart coords); CHANGELOG 4.21.0–4.22.0
- **Migrations:** `2026-06-10-add-ac-xt-gk-columns.sql`, `2026-06-10-add-spadl-result-source-restart-coords.sql`
