# Handoff: goal-kick actor override — credit goal-kicks to the keeper (consumes silly-kicks 4.39.0)

**Date:** 2026-07-01 · **From:** xT-GK analysis side · **For:** lakehouse session · **Type:** requirement + rationale
**Consumes:** silly-kicks **4.39.0** `acting_gk_from_frames` (PR-S106). **This is the lakehouse half** of the goal-kick
taker fix; the silly-kicks resolver half is released. Related: my triage above + the set-piece possession precedent
`_fill_possession_from_set_piece_actions` (enrich.py, PR-S67).

## The problem (recap, data-verified on the post-4.38.0 recompute)
A goal-kick's SPADL taker (`player_id`) is **NULL for all four tracking providers**, so the AC layer fills the actor
from the **ball-carrier at the linked frame** (`ball_carrier_at_action`). For a goal-kick the linked frame has the ball
at the **downfield event location** (the 4.37.0 origin scatter), so the "carrier" is whatever outfielder is near the
ball 14–20 m downfield — spread across **29–35 players/match, ~1 each, ~0% the actual keeper**. The `xt_gk` value +
origin are correct; only the **credit** is wrong. It's the **actor-analog of the origin scatter** (4.37.0 fixed the
origin, 4.38.0 fixed identity, this fixes the taker). Affects every tracking provider — SkillCorner worst (5.3
takers/match), idsse 2.4, GS ~1.0 (its goal-kick event happens to log near the keeper).

**Metric-material:** goal-kicks are ≈ half a keeper's distribution volume, so today the keeper cohort **loses its
goal-kick credit** and outfielders get spurious `xt_gk`. Must land before the RM metric run.

## The fix
A goal-kick's taker is **unambiguously the acting team's keeper**. Override the carrier-derived `player_id` for
goal-kicks with `silly_kicks.tracking.acting_gk_from_frames(actions, frames)`. This is the exact analog of what you
already do for **possession** on set-piece restarts (`_fill_possession_from_set_piece_actions` overrides the unreliable
carrier possession from `action.team_id`) — same place, same reasoning, now for the actor.

1. **Pin silly-kicks `==4.39.0`** (supersedes 4.38.0; contains the resolver + all prior fixes). Sentinel lockstep +
   full suite as usual.
2. **In the set-piece-restart synthesis path**, for **goal-kicks only**, set `player_id = acting_gk_from_frames(...)`,
   overriding the carrier-derived value. Keep it in the same synthesis step that already special-cases restarts.

## Scope — goal-kicks ONLY (deliberately narrow)
- **Only goal-kicks.** Other set-pieces (throw-in, corner, free-kick) have **outfielder** takers, and their event ball
  sits *at* the taker (no downfield scatter), so the carrier fill is fine — do **not** apply the GK override to them.
- **Only where the resolver resolves.** Event-only providers (statsbomb/wyscout) carry a real goal-kick taker in SPADL
  and have **no frames** → `acting_gk_from_frames` returns NaN → **no override** (keep the real taker). Tracking
  providers → NULL/carrier taker → override. So gate the override on a non-NaN resolver result; never blank a real
  taker.
- **Robust to undetected keepers:** the 4.39.0 resolver has an identity fallback (resolves the acting GK from the
  roster-stable `is_goalkeeper` even on the ~40% of goal-kicks where the keeper isn't detected at the event frame), so
  the override fires on nearly all goal-kicks, not just detected ones.

## Acceptance (what I'll re-check on the rebuilt mart)
- **Goal-kick takers per (match, team) → ~1 (the keeper)**: SkillCorner 5.3 → ~1–2, idsse 2.4 → ~1–2, GS stays ~1.0.
- **Distinct players carrying `xt_gk` per SkillCorner match → ~1–2** (down from 7.3) — i.e. the mart-level guard from
  `2026-07-01-mart-level-gk-scored-players-guard.md` should now **pass** (this fix is what makes it pass; land them
  together).
- **The keeper cohort now receives its goal-kick credit** (the point of the whole exercise).
- **Open-play passes unchanged** (already keeper-concentrated ~1.3/match — the override must not touch them).
- **Value/origin regression:** goal-kick `xt_gk` values + origins unchanged (still 100% own-box) — only `player_id`
  changes.

## Sequencing
This override + the mart-level guard → re-recompute the affected tracking providers → **rebuild the held
`fct_action_context` mart** → ping the analysis side for re-acceptance. Then the RM gates (H1 live + clean acceptance)
are the only thing left before RM-5.
