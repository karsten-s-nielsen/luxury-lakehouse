# ADR-065: silly-kicks 4.37.0 — SkillCorner keeper-origin adoption

| Field | Value |
|---|---|
| **Date** | 2026-06-30 |
| **Status** | Accepted |
| **Deciders** | Karsten (operator), lakehouse session, xT-GK analysis side |

## Context

SkillCorner is the only provider whose goalkeeper-distribution origins are not localized to the goal area — they are
**scattered across the whole pitch** (goal-kick own-box rate 51% vs ~100% for every other provider; `start_x` min 0.8
/ max 98.4 / SD 23.2). Root cause (analysis-side investigation,
`docs/investigations/2026-06-29-skillcorner-keeper-origin-coordinate-scatter.md` + the 4.37.0 adoption-context note):
the SkillCorner→SPADL conversion took the keeper-action origin from the **broadcast ball-detection event location, not
the keeper's position**, and `resolve_gk_geometry` trusted that non-NaN native coordinate. This corrupts `xt_gk`,
keeper pressure, and PEV. The fix is general broadcast-tracking domain logic — `resolve_gk_geometry` and the
coordinate transform live in **silly-kicks** — so it belongs upstream, not in a lakehouse-side net (the TF-23 lesson:
the prior lakehouse orientation net was deleted precisely to stop duplicating domain logic downstream).

silly-kicks **4.37.0** (PR-S104, upstream ADR-024 amendment) ships the fix. Real-bronze validation narrowed it vs the
original spec: **distrust is goal-kicks-only** (an open-play keeper pass's native origin IS the keeper — ball at the
feet at release — 0.4 m from the detected keeper, so it keeps its native origin); only goal-kicks carry a displaced
native (broadcast ball ~14–20 m downfield) and are resolved via a ladder (detected keeper → `tracking_gk`; else
rule-point → `goalkick_prior`); the rest flag `unresolved` (no honest prior, `xt_gk` NULL, never imputed). 4.37.0
also adds a within-pitch transform invariant (S1, kills off-pitch x→123) and `is_detected` preservation through
`convert_to_frames`. It is **default-off** for GS/idsse/metrica/sportec (byte-identical regression-gated).

## Decision

Adopt silly-kicks **==4.37.0** (pinned everywhere per ADR-046) with a **thin lakehouse adoption**: version bump +
sentinel lockstep + wheel; surface the new `xt_gk_origin_source` provenance values (`native`/`tracking_gk`/
`goalkick_prior`/`unresolved`) on `fct_action_context` and the `XtGkReport` + `TrackingConversionReport` observability
(M2) in the AC log; a C1 single-provider sentinel. The keeper-origin geometry/resolver fix is owned upstream; the
lakehouse writes **no** geometry code. The per-match privacy hardening (the fail-safe `access_tier` allowlist + the
publish-path divergence guard) rides in the **same cycle/PR** but is recorded in the **ADR-064 amendment**, not here.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Lakehouse-side keeper-origin fix | No silly-kicks dependency | Re-creates a lakehouse geometry path (TF-23 anti-pattern); not reusable | Domain logic belongs upstream; `resolve_gk_geometry` already lives in silly-kicks |
| B. Clamp the bad coordinate to a default | Trivial | Masks the issue; leaves passes + pressure/PEV silently wrong | "Don't mask" — flag `unresolved`, never impute a guess |
| C. **Adopt 4.37.0 (tiered, use-the-right-source), thin lakehouse** (chosen) | Correct + reusable; goal-kick-concentrated blast radius | Needs a SkillCorner AC recompute; a new provenance enum value | — |

## Consequences

### Positive

- SkillCorner goal-kick origins resolve to the goal area; open-play passes keep their (correct) native origin;
  `xt_gk`/pressure/PEV are no longer measured at scattered positions.
- `unresolved` is rare (goal-kick/NaN edge) and surfaced as a count + NULL `xt_gk`, never imputed.
- The provenance enum + the S1/S4 observability counts make the fix auditable.

### Negative

- A SkillCorner **AC-layer recompute** (re-run `compute_action_context` against the existing raw-faithful bronze — no
  re-ingest) is required to re-materialize the corrected `xt_gk`; operational, post-merge, gated.
- `xt_gk_origin_source` gains values (`goalkick_prior`/`unresolved`) — a `fct_action_context` contract surface;
  `unresolved → xt_gk NULL` is a Hyrum-relevant change for consumers (cohort filters `xt_gk IS NOT NULL`, safe).

### Neutral

- `fct_tracking_frames`' separate off-pitch coordinate issue is **decoupled** (it does not feed `xt_gk`; lakehouse
  transform, not `convert_to_frames`) — a separate scoping item (spec L4), not in this adoption.
- The private Real Madrid SkillCorner games have the same broadcast profile but `is_detected` present; the tiered fix
  applies — gated behind this adoption + the ADR-064 privacy hardening being live.

## Amendment (2026-07-01): silly-kicks 4.38.0 — SkillCorner GK-identification

The 4.37.0 recompute fixed goal-kick *origins* but the public SkillCorner gold exposed a **separate, pre-existing**
defect: `xt_gk` (a goalkeeper metric) was computed for **19–24 players/match** — both full squads — not the ~1–2
keepers. Root cause (real-data proven): the AC dispatch builds frames in **250-frame batches**, and silly-kicks
≤4.37.0 **re-derived `is_goalkeeper` positionally per batch**, so on a 25 s window it flagged whichever outfielders
were transiently parked near a goal (~15/team across batches). silly-kicks **4.38.0** (PR-S105, upstream ADR-007) fixes
it by **trusting the native roster `is_goalkeeper`** (skips `derive_goalkeepers` when the roster flag is valid) —
mirroring what the GS/sportec converters already did (they were immune). On real bronze the per-batch union drops
**15/13 → 1/1 per team**.

**Decision:** re-pin **==4.38.0** (supersedes 4.37.0 — it carries both the keeper-*origin* and this GK-*identification*
fix) with the same thin-adoption shape (version bump + sentinel lockstep + wheel 0.5.59). One lakehouse code delta:
surface the new **`TrackingConversionReport.n_implausible_gk_teams`** at ERROR (`pipeline.py`, mirroring
`n_gross_off_pitch`) — a resolved per-`(game,team)` GK count `>2` or `0`; expected **0** on SkillCorner (clean roster),
non-zero flags whole-squad contamination. No lakehouse geometry/identification code — the fix is owned upstream.

**Consequences:** a second SkillCorner **AC-layer recompute** re-materializes `xt_gk` for keepers only (the row count
scored drops sharply); goal-kick origins stay ≈100% own-box (4.37.0, unchanged); GS/idsse/metrica/sportec remain
byte-identical (4.38.0 is SkillCorner-only, regression-gated). **Out of scope (tracked follow-up):** Metrica
(anonymized, no roster) is contaminated the same per-batch way and 4.38.0 does **not** fix it — it needs GK derivation
run **once per full match**, a separable silly-kicks + lakehouse change; filed, does not block SkillCorner/RM.
