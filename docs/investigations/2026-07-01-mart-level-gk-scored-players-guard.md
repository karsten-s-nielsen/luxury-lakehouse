# Handoff: mart-level guard — distinct `xt_gk`-scored players per match (the durable catch for GK-contamination)

**Date:** 2026-07-01 · **From:** xT-GK analysis side · **For:** lakehouse session · **Type:** requirement + rationale
**Related:** silly-kicks 4.38.0 SkillCorner GK-identification fix + `docs/investigations/2026-06-30-silly-kicks-438-adoption-context.md`.

## Why this is needed (the gap the 4.38.0 guard doesn't close)
The whole-squad `xt_gk` contamination (SkillCorner scored 19–24 players/match; Metrica scores 17) came from
`derive_goalkeepers` re-running **per 250-frame batch** and flagging ~15 different transient near-goal players/team
across the match. silly-kicks 4.38.0 fixes SkillCorner (trusts the roster) and adds a guard —
`TrackingConversionReport.n_implausible_gk_teams` — **but that guard runs inside `convert_to_frames`, i.e. per batch.**
Per batch the pick is ~1/team (plausible), so the guard stays silent; the contamination is the **union across ~164
batches**, which **no single call ever sees**. The per-call guard is therefore **structurally blind** to this exact
failure mode — for Metrica today, and for any future regression of it.

The only place the contamination is *visible* is the **mart**, cross-batch: distinct scored players per match. That's
where a hand-run query caught it. This handoff asks to make that check **automatic**.

## The requirement
On `fct_action_context` (post-recompute), assert/observe: **distinct `xt_gk`-scored acting players per
`(match, team)` is small** (a team has one keeper, occasionally a sub → ≤ ~2, allow a little margin). A match/team
exceeding the bound is contaminated (non-keepers scored) and must **not** be served as if trustworthy.

- **Scope of the count:** `COUNT(DISTINCT player_key)` over rows with `xt_gk IS NOT NULL`, grouped by
  `(match_key, team_key)` (or per match — either works; per-team is the cleanest signal).
- **Threshold:** calibrate on the clean 4.38.0 recompute rather than hard-coding a guess (validate-on-real-data).
  Reference values already measured: GS ≈ 1/match, idsse ≈ 2.4/match, SkillCorner **post-4.38.0 should be ≈ 1–2**;
  contaminated is ≈ 8–12/team. So the clean vs contaminated gap is wide — set the threshold at `clean_max + margin`
  (something like `> 3–4 distinct/team` will separate cleanly). Measure, then pin.
- **Action on trip (fail-safe):** flag it **loudly + observably** (ERROR log + a countable field/quality flag), and
  **exclude the offending match's `xt_gk`** (NULL it). At the mart level you can't tell which of the flagged players is
  the real keeper (that's the whole defect), so whole-match exclusion is the only safe option — better to lose a
  contaminated match than serve silently-wrong keeper metrics. **Do not hard-crash the recompute** — one bad match
  must not kill the batch (same warn-and-flag philosophy as S1/S4; here the unit is the match).

## What it buys
- **Catches the contamination class for every provider + every future regression**, at the level where it's visible —
  what the per-call guard cannot do.
- **Neutralizes stale Metrica automatically:** its 17/match trips the guard → excluded, not silently wrong. (Metrica =
  3 stale, hand-curated games; we are **not** fixing its converter — see below.)
- **Doubles as automated acceptance** for the 4.38.0 recompute: SkillCorner should now sit at ~1–2/team and pass; if
  it doesn't, the adoption isn't clean.

## Explicitly out of scope (deferred, by decision)
- **The Metrica converter fix.** Metrica *does* have a GK identity (`metrica_tracking.gk_jersey_numbers`), so in
  principle it's the same "trust the identity, skip derivation" fix as SkillCorner. But it's 3 **stale, hand-curated**
  games that may not fit the pattern cleanly, almost certainly out of the metric scope. **Not worth a converter change
  now.** The mart guard above makes stale Metrica safe (excluded, not wrong). Revisit the converter fix only if Metrica
  becomes real/used — and verify `gk_jersey_numbers` per game first.
- The general **derive-once-per-match** architecture (for a hypothetical future provider with *no* GK identity) — not
  needed; all five current providers either trust a real GK identity or are handled by this guard.

## Placement
Lakehouse-owned (it owns the mart). A dbt data test, a post-recompute validation step, or an observability-table
metric — your call, provided the check is **on the path that gates/serves the recompute** (not an out-of-band job that
can lag a publish). Pin the threshold from the measured clean recompute.
