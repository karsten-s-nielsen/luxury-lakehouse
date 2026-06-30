# Adoption context: silly-kicks 4.37.0 (SkillCorner keeper-origin) — deltas vs the combined-recompute spec

**Date:** 2026-06-30 · **From:** xT-GK analysis side · **Status:** silly-kicks **4.37.0** released + committed (PR-S104,
ADR-024 amendment) — the prerequisite for your `2026-06-30-skillcorner-keeper-origin-rebuild-and-access-tier-completion.md`
is satisfied. This note records what **changed during silly-kicks implementation** (real-bronze validation), so adopt
against *this*, not the original spec's assumptions.

## The one big delta: distrust is GOAL-KICKS ONLY now
Real data narrowed it. An **open-play GK pass/throw's native origin IS the keeper** (measured 0.4 m vs the detected
keeper — the ball is at the keeper's feet at release), so those **keep their native origin, unchanged**. Only
**goal-kicks** carry a displaced native (broadcast ball logged ~14–20 m downfield) and are resolved via the ladder
(detected keeper, ADR-028-reprojected + in-box clamp → `tracking_gk`; else rule-point → `goalkick_prior`).

**Consequences for your plan:**
- **`unresolved` is now RARE**, not the common case (it was scoped to "all open-play undetected passes"; now it's
  only a goal-kick / NaN-native edge). Your `unresolved → NULL xt_gk` rendering still applies — just to **far fewer
  rows**. Expect a tiny `unresolved` count, and SkillCorner *open-play pass* origins to localize **via native**, not
  via the ladder.
- The blast radius shrank: SkillCorner open-play passes are effectively unchanged; the fix concentrates on goal-kicks.

## What to pin + plumb
- **Pin silly-kicks `==4.37.0`** (version sentinels, terraform env, wheel).
- **Provenance enum** `xt_gk_origin_source` now spans **{`native`, `tracking_gk`, `goalkick_prior`, `unresolved`}** —
  `native` is the **common** value (open-play + full-tracking), not just legacy. Plumb that to `fct_action_context`.
- **New observability columns/fields to surface (don't emit-and-ignore — review M2):**
  `xt_gk_native_goalkick_out_of_region` (per-row S4 flag), `XtGkReport.n_native_goalkick_out_of_region`,
  `TrackingConversionReport.n_gross_off_pitch`. Wire these into whatever you monitor; the **CI/batch rate-gates remain
  a tracked follow-up** (set thresholds from the recomputed corpus rate — they're the systematic backstop).
- **S2/L1 already satisfied:** silly-kicks `convert_to_frames` preserves bronze `is_visible` as `visibility` — no extra
  lakehouse detection plumbing needed.

## Two things to keep straight
- **L4 (`fct_tracking_frames` re-point) stays DECOUPLED.** The `xt_gk` fix runs on the **AC frame path** (silly-kicks
  `convert_to_frames`, fixed in 4.37.0) — it does **not** depend on `fct_tracking_frames`. Do **not** gate the xt_gk
  recompute on L4; `fct_tracking_frames`' off-pitch issue is a separate mart with its own consumer-migration, on its
  own schedule.
- **C1 (one-call-one-match now enforced):** `compute_xt_gk` raises on a >1-provider frame set **uniformly** (the
  mixed-provider `completion=` escape hatch was removed). Harmless for per-match adoption — just confirm the lakehouse
  always calls it one-provider-per-match (it does). If anything batches multiple providers' frames into one call, it
  now raises (correctly).

## Privacy hardening (H1) — unchanged, still BLOCKING before RM
Your §3a still stands: SkillCorner no-signal default → **restricted**; existing public A-League reconciled to explicit
`visibility='public'`; publish guard requires explicit public. Must land before the RM-5 ingest gate.

## Acceptance (public SkillCorner recompute) — then ping the analysis side
- **Goal-kick** origins ≈ 100% own-box (the headline fix); **open-play pass** origins localize (via native);
  `unresolved` count small.
- **GS / idsse / metrica / sportec byte-identical** (regression gate — 4.37.0 is default-off for them).
- `access_tier` correct (SkillCorner public / GS restricted), public datasets intact.
- Then the analysis side runs the end-to-end check on the recomputed gold before the RM games flow.
