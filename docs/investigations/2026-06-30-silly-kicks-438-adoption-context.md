# Adoption context: silly-kicks 4.38.0 (SkillCorner GK identification) — re-pin + re-recompute

**Date:** 2026-06-30 · **From:** xT-GK analysis side · **Status:** silly-kicks **4.38.0** released + committed
(PR-S105, ADR-007). **Supersedes 4.37.0** — pin straight to **4.38.0** (it contains both the keeper-*origin* fix
(4.37.0) and this GK-*identification* fix). Companion to `2026-06-30-silly-kicks-437-adoption-context.md`.

## Why a second recompute
The 4.37.0 recompute fixed goal-kick *origins* (verified: 100% own-box) but the public SkillCorner gold exposed a
**separate, pre-existing** defect: `xt_gk` (a goalkeeper metric) was being computed for **19–24 players/match** — both
full squads — not the ~1–2 keepers. Root cause (real-data proven): the AC dispatch builds frames in **250-frame
batches**, and silly-kicks ≤4.37.0 **re-derived `is_goalkeeper` positionally per batch**, which on a 25 s window
flags whichever outfielders are transiently parked near a goal (~15/team across batches). 4.38.0 fixes it by
**trusting the native roster `is_goalkeeper`** (skip `derive_goalkeepers` when the roster flag is valid) — mirroring
what the GS/sportec converters already did (they were immune). Result on real bronze: per-batch union **15/13 → 1/1
per team**.

## What to do (thin adoption — no lakehouse SkillCorner code change)
1. **Re-pin `silly-kicks==4.38.0`** (version sentinels, terraform env, wheel — same lockstep as the 4.37.0 bump).
   Run the **FULL** suite (sentinels).
2. **Re-recompute the SkillCorner AC layer** (`compute_action_context` against existing bronze — still no re-ingest;
   the fix is in the converter). Expect the `xt_gk`-scored row count to **drop sharply** (whole-squad → keepers only).
3. **Surface the new S2 observability** — `TrackingConversionReport.n_implausible_gk_teams` (warn + count when a
   resolved per-`(game,team)` GK count is `>2` or `0`). Log it the same way you log `n_gross_off_pitch` (ERROR per the
   telemetry rule). It should be **0** on the SkillCorner recompute (roster is clean 1/team); non-zero = a data-quality
   signal to investigate.
4. **Ping the analysis side** to re-run acceptance on the re-recomputed public gold before anything else proceeds.

## Expected acceptance (what I'll check)
- **~1–2 acting players/match** get `xt_gk` on SkillCorner (down from 19–24), i.e. keepers only — the headline of this
  fix. (GS is ~1/match; SkillCorner should now match that shape.)
- **Goal-kick origins still ≈100% own-box** and open-play keeper passes still sensible (unchanged from 4.37.0).
- `n_implausible_gk_teams` = 0.
- **GS / sportec / idsse / metrica byte-identical** (4.38.0 is SkillCorner-only; regression gate).

## Sequencing (unchanged)
This re-recompute is **public SkillCorner only** — no RM. The gates before **RM ingestion** are unchanged:
**H1 privacy hardening must be LIVE**, and the re-acceptance above must pass. Then RM-5 (spec §3), then RM-94, then the
metric test.

## Out of scope (tracked follow-up — NOT this adoption)
**Metrica** (anonymized, no roster → *must* derive positionally) is contaminated the same per-batch way and 4.38.0
does **not** fix it (roster-trust can't — there's no roster). It needs GK derivation run **once per full match**, not
per 250-frame batch — a separable silly-kicks (derive-once API / accept pre-derived picks) + lakehouse (derive once,
feed `is_goalkeeper` into the per-batch builds) change. File it; it doesn't block SkillCorner/RM. (If Metrica `xt_gk`
is consumed anywhere today, treat it as contaminated until that lands.)
