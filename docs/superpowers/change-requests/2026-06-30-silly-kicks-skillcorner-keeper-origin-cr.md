# silly-kicks Change Request — SkillCorner keeper-origin (S1–S4)

> **STATUS: RESOLVED — released as silly-kicks 4.37.0 (PR-S104, ADR-024 amendment).** Real-bronze validation
> narrowed the fix vs this CR — **distrust is goal-kicks-only** (open-play passes keep their native origin), so
> `unresolved` is rare and `native` is the common provenance. Adopt against
> `docs/investigations/2026-06-30-silly-kicks-437-adoption-context.md` and the updated lakehouse plan, NOT this CR's
> original assumptions. This doc is retained as the original request record.

**Date:** 2026-06-30 · **For:** silly-kicks maintainer / analysis side · **From:** lakehouse session
**Source finding:** `docs/investigations/2026-06-29-skillcorner-keeper-origin-coordinate-scatter.md`
**Companion lakehouse plan:** `docs/superpowers/plans/2026-06-30-skillcorner-keeper-origin-lakehouse-plan.md`

## Why this is a silly-kicks change (boundary, approved by the analysis side)
The lakehouse bronze is **raw-faithful** — `bronze.skillcorner_tracking` carries `is_visible` + `ball_is_detected` +
native ±52.5 center-origin coords. The corruption is in the **converter/resolver**: the SkillCorner→SPADL coordinate
transform and `resolve_gk_geometry` (`silly_kicks/tracking/_gk_geometry.py`) — both silly-kicks. The lakehouse already
feeds the right signal; this CR is converter/resolver work, reusable by every silly-kicks consumer.

The lakehouse AC path that consumes this: `convert_skillcorner_bronze_to_frames` → `sk_frame_adapters` → silly-kicks
`tracking.skillcorner.convert_to_frames` (TF-23) → `resolve_gk_geometry`. Fixing these fixes `fct_action_context.xt_gk`.

## The four changes

### S1 — Coordinate transform correctness + within-pitch invariant
SkillCorner native is **center-origin** (x∈[−52.5,+52.5]), physical (ends switch each half). The transform to SPADL
[0,105] + home-LTR currently leaks **off-pitch (keeper x→123m, 18m past goal)** — impossible under a correct transform.
- Fix the native→SPADL [0,105]+LTR mapping so a correct transform makes gross off-pitch impossible by construction.
- Add a **within-pitch invariant with tolerance** (review M4): legitimate behind-goal keepers reach ±60 native →
  some x slightly outside [0,105] is allowed (a few-metre tolerance). **Gross** off-pitch (x→123) is a **loud
  assertion pointing upstream — never a silent clamp.**

### S2 — Carry `is_detected` through the frame model
`convert_to_frames` must **preserve** `is_detected` per player-frame (not interpolate/hold the keeper into ~100% of
frames and lose the bit). The resolver (S3) needs it. The lakehouse provides `is_visible`/`ball_is_detected` in bronze;
the converter must thread it through, and for the RM **course-raw** bundle preserve its richer per-frame signal.

### S3 — Tiered keeper-origin resolution in `resolve_gk_geometry`
Today the resolver **trusts the non-NaN native origin** (`xt_gk_origin_source=native`, conf 1.0) — but for SkillCorner
that native coordinate is the **broadcast ball-detection location, not the keeper**. Stop trusting it for keeper
actions; resolve by tier, **by action type**:
1. keeper **detected at / within ±1s** of the action → **tracked keeper position** (transformed). [best]
2. else **goal-kick** → **`goalkick_prior` ≈(5.5, 34)** — reliable (goal kicks are always taken from the goal area;
   exactly the GS path already in the resolver).
3. else **open-play pass / throw, no detection** → **flag `unresolved`** — no honest prior; **never impute a guess.**
- Emit provenance `xt_gk_origin_source` ∈ {`tracking_gk`, `goalkick_prior`, `unresolved`, `native`}. The lakehouse
  surfaces this as a `fct_action_context` contract (NULL `xt_gk` + count for `unresolved`).

### S4 — Loud-validation companion (defense-in-depth, NON-gating)
A loud validation: a **native** goal-kick origin sitting implausibly far from goal **fails loud**, so a future provider
feeding ball-location-as-origin can't silently corrupt. Tracked separately; **does not gate** the recompute.

## Acceptance (the lakehouse will validate post-adoption)
- SkillCorner goal-kick origins ≈100% own-box; pass origins localize; full-pitch scatter SD collapses.
- All tracking outputs within-pitch to tolerance; no gross off-pitch (the x→123 class is gone).
- `unresolved` is surfaced as a count, never imputed; provenance enum populated.
- **GS / IDSSE / Metrica unchanged** (SkillCorner-only) — regression-gate.
- Spot-check goal-kicks: tracking/prior origin ≈(5.5,34) where the native event coord was downfield.
- Works on the RM **course-raw** bundle (partial keeper detection): goal-kicks clean, open-play ~58–70% covered +
  flagged.

## Decided design points (review #1/#2)
- Tier-3 = **flag-`unresolved`, no impute** (firmly agreed — don't mask).
- Within-pitch = **tolerance + loud upstream assertion**, not clamp (behind-goal keepers are legitimate).
- S4 belongs here but is **separate + non-gating**.

## Lakehouse side (for reference — not your work)
Thin adoption only: version bump + `==` env pins + wheel; plumb `xt_gk_origin_source` to the mart; privacy-default
hardening (ADR-064); the SkillCorner recompute. No geometry code lakehouse-side. `fct_tracking_frames` (a separate
lakehouse-transform mart, not on the xt_gk path) is a decoupled follow-up.
