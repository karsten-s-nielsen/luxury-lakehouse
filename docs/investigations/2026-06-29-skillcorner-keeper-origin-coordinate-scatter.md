# SkillCorner keeper-action origins are unreliable — root cause + the fix the rebuild needs

**Date:** 2026-06-29 (updated) · **From:** xT-GK analysis side · **Status:** Finding for the in-progress SkillCorner
ingestion rebuild — pick up after the current PR merges. **Not urgent** (the private RM data isn't ingested yet), but
it should shape how the rebuilt ingestion derives keeper-action coordinates and how it treats detection.

## TL;DR
SkillCorner is the only provider whose GK-distribution origins aren't localized to the goal area — they're
**scattered across the pitch** because the SPADL coordinate is taken from the **broadcast ball-detection event
location, not the keeper's position**. The keeper **is** reliably identifiable (lineup role / `is_goalkeeper`) and,
**when actually detected**, correctly localized near the goal. The problem is detection reliability:
- **Course raw data (the private RM target)** preserves `is_detected` — keeper detected 24.7% overall, ~58% at the
  keeper's own action frames, ~70% within ±1s. Reliable when detected.
- **Current public lakehouse ingestion** threw that away — keeper interpolated into ~100% of frames (no `is_detected`)
  and coordinates run **off-pitch (x to 123m)**. So today's SkillCorner positions are not trustworthy.

**Fix = use the right source + preserve detection + tiered fallback** (detailed below). **Not** a clamp.

---

## Part A — the origin scatter (what's wrong with `xt_gk` today)

SkillCorner keeper-distribution origins, by type (`fct_action_context`, `xt_gk IS NOT NULL`):
| type | n | own-box (<16.5m) | own-mid | opp-half | mean x | sd |
|---|---|---|---|---|---|---|
| pass | 840 | 500 (60%) | 216 | 124 | 24.5 | 27.6 |
| goalkick | 86 | 44 (51%) | 27 | 15 | 24.8 | 23.2 |
| throw_in | 26 | 5 | 6 | 15 | 59.6 | 37.4 |

Goal-kick own-box rate, all providers (`fct_action_values`) — SkillCorner is the lone outlier:
gradientsports 323 own-box / 603 null→imputed, idsse 143/143, metrica 33/33, statsbomb 55,082/55,082,
wyscout 31,603/31,603, **skillcorner 44/86**. SkillCorner goal-kick `start_x`: mean 24.8, **min 0.8, max 98.4, SD
23.2** — a full-pitch scatter (not bimodal-at-0/105 = no clean flip; not a uniform shift = no simple transform bug).

**Root cause:** the SkillCorner→SPADL conversion assigns keeper-action `start_x/y` from the broadcast event (ball)
location — "wherever the ball was first reliably detected" — which on broadcast tracking is near the keeper sometimes,
downfield often. `resolve_gk_geometry` then trusts it (`xt_gk_origin_source=native`, conf 1.0) because it's non-NaN,
unlike GS (whose NaN goal-kick origins get imputed via tracking-GK / rule point).

## Part B — the keeper IS recognizable; the issue is detection reliability

**Identification works (both datasets):**
- Course raw bundle: meta lineups tag keepers via `player_role`="Goalkeeper"; meta `id` joins directly to tracking
  `player_id` (verified: Lunin 12248, Unai Simón 13899). Frames are 100% linked to actions (`link_q`=1.0, 0 missing;
  fewer candidate frames, 4 vs GS 12).
- Lakehouse frames (`dev_gold.fct_tracking_frames`): `is_goalkeeper`=True flags exactly 2 keepers/match, each
  correctly localized near one goal per period with the proper half-switch (e.g. team A x̄ 104.9 in P1 → 16.5 in P2).
  So identity + orientation are fundamentally sound.

**Detection is partial (broadcast tracking) — course raw data, with `is_detected`:**
- Keeper detected 24.7% of all frames (dominated by open play, keeper off-camera), but **58% at the keeper's own
  action frames** and **70% within ±1s** of the action. When detected, the position is correct (P1 x̄≈+47, P2 x̄≈−49 in
  native center-origin coords).

**The current public ingestion lost this (why its positions are bad):**
- Keeper present in ~100% of frames (vs outfielders ~67%) with **no `is_detected`** → positions are interpolated/held
  when the keeper was actually off-camera; you can't tell real from filled.
- Coordinates extend **off-pitch** (keeper x to 123m, i.e. 18m past goal) → a scaling/mapping problem on top of the
  scattered event origins.

**Coordinate convention:** SkillCorner native tracking is **center-origin** (x∈[-52.5,+52.5]) and **physical**
(teams switch ends each half) — needs the SkillCorner→SPADL [0,105] + LTR transform, and outputs must stay within the
pitch.

---

## The fix (use-the-right-source + preserve detection + tiered — NOT a clamp)

In the rebuilt SkillCorner ingestion:

1. **Preserve `is_detected`** and resolve keeper-action origins from **real keeper detections only** — never the
   interpolated/held positions, and never the ball-event coordinate.
2. **Fix the coordinate transform** (native center-origin ±52.5 → SPADL [0,105] + LTR); validate outputs are within
   the pitch (no x>105 / x<0 beyond a small tolerance).
3. **Tiered keeper-origin resolution**, by action type (this is the part that matters — detection is only ~58–70%):
   - keeper **detected at / within ±1s** of the action → use the tracked keeper position (transformed). [best]
   - else **goal-kick** → **rule-point prior (≈5.5, 34)** — reliable, because a goal kick is always taken from the
     goal area (exactly GS's `goalkick_prior` path). Goal-kicks come out clean either way.
   - else **open-play pass / throw, no detection** → **flag / exclude** — a pass can originate anywhere in the
     defensive third, so there's no honest prior; do not impute a guess.
   - Tag each with provenance (`tracking_gk` / `goalkick_prior` / `unresolved`).

Explicitly **not** a sanity-floor/clamp of the bad event coordinate (would mask the issue and leave passes +
pressure/PEV silently wrong). We're sourcing the origin from the keeper's actual tracked position where it exists,
falling back only to physically-reliable priors, and flagging the honestly-unknowable rest.

## Validation / acceptance
- Goal-kick origins land ~100% own-box (like every other provider); pass origins localize sensibly; scatter SD
  collapses; the flagged-unresolved subset is surfaced (count reported, not silently imputed).
- Spot-check goal-kicks: tracking-derived origin ≈ (5.5, 34) where the native event coordinate was downfield.
- All tracking outputs within the pitch (fix the off-pitch x>105).
- GS / idsse / metrica unchanged (SkillCorner-only).

## Implications (context for the rebuild)
- **Pressure / PEV are computed at the resolved origin**, so today they're measured at the scattered/interpolated
  positions — SkillCorner's elevated keeper pressure (0.120 vs GS 0.026) is an artifact and will move toward the
  keeper's real location after the fix. (The analysis side retracted an earlier "SkillCorner PEV looks alive" read on
  this basis.)
- **The private Real Madrid 2023–25 bundle (99 games, pending) has the same broadcast profile** — partial keeper
  detection — but `is_detected` is present in its raw tracking, so the tiered fix works. Expect goal-kicks clean and
  open-play ~58–70% covered + flagged. First check when it lands: goal-kick origins ≈ own box.
- This is a known broadcast-tracking limitation (full optical wouldn't have it) — relevant to whether downstream
  xT-GK / external-validation numbers on SkillCorner are comparable to optical-tracking work.

## Companion (silly-kicks side, separate — defense-in-depth)
A **loud validation** in `resolve_gk_geometry` — flag (don't silently clamp) a *native* goal-kick origin that sits
implausibly far from goal — so a future provider feeding ball-location-as-origin fails loudly. Tracked separately
with the analysis side; not required for this ingestion fix.
