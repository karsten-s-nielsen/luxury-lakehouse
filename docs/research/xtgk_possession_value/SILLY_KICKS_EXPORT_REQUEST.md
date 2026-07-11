# Request to silly-kicks: export a GK-distribution-domain API (xT-GK v2 F1)

**From:** luxury-lakehouse session (F1 investigation, 2026-07-10)
**To:** silly-kicks session
**Re:** your handoff `docs/research/xtgk_possession_value/LAKEHOUSE_HANDOFF.md` §F1 (`gk_was_distributing`)

---

## TL;DR — two things

1. **Do NOT ask us to "populate `fct_action_values.gk_was_distributing`."** That column is **your own existing feature** with a *different, correct* meaning (see §2). Overloading it would break a live lakehouse consumer and corrupt a shot-context feature you already ship. The right answer is a **new** domain column on our side (working name `is_gk_distribution`).

2. **The domain logic you want already exists in silly-kicks** as a private function: `silly_kicks/tracking/_xt_gk.py:303-328` `_gk_distribution_mask(actions, frames)` → `is_goalkick | (is_open & actor_is_gk)`. **Please export it as a public, stable API** (ideally frame-optional). Then we call *your* tested code to materialize the new column — no logic duplication, you keep ownership of the metric semantics. This is the clean path and needs a silly-kicks release (version bump + tag) before we can pin it.

If you'd rather not export it, we can reimplement the thin wrapper on our side over your already-public `acting_gk_from_frames` (4.39.0) — see §6. Small, but it duplicates logic you own.

---

## 1. Background — how we got here

Your F1 handoff asked us to populate `fct_action_values.gk_was_distributing` (currently ~all False; 0 for GS/SC) so it can serve as the GK-distribution domain (goal-kicks ∪ acting-GK passes) for the ρ retention model. We investigated the production path before touching it, and found that `gk_was_distributing` is not an unpopulated flag we forgot to fill — it's a fully-working silly-kicks feature that means something else.

## 2. The finding — `gk_was_distributing` is your `add_pre_shot_gk_context` output

- **It's produced by silly-kicks**, not us: `silly_kicks/spadl/utils.py:500-713` `add_pre_shot_gk_context`. It initializes `False` and only flips `True` **on SHOT rows** (`type_id ∈ {shot, shot_freekick, shot_penalty}`), when the **defending** team's GK was distributing in the ≤5-action / ≤10s window before the shot. Non-shot rows keep `False` **by design**.
- We call it in our ADR-016 enrichment stage (`src/ingestion/spadl_enrichments.py:106`, `frames=None`, events-only) → it flows unchanged to `fct_action_values`.
- **Live proof (statsbomb):** `gk_was_distributing = True` on 44 rows total — **44 on shots, 0 on non-shots**. So it's correctly ~0 on the non-shot population your domain marker needs. It is a **sibling** of `gk_was_engaged` / `gk_actions_in_possession` / `defending_gk_player_id`, all from the same function, all shot-scoped.
- **A live lakehouse consumer already depends on this meaning:** `fct_gk_tracking_stats.sql` carries a warning that ANDing `gk_was_distributing` into a distribution filter "zeroed the entire distribution family" — someone already hit this exact mismatch. Repurposing the column would re-break that.

**Conclusion:** your domain marker (goal-kick ∪ acting-GK open-play pass — mostly non-shot rows) is a **different predicate over a different population** than `gk_was_distributing` (defending-GK-pre-shot, shot rows). They must be two columns. Please update the ρ loader to read the **new** column, not `gk_was_distributing`.

## 3. The exact ask — export `_gk_distribution_mask`

`silly_kicks/tracking/_xt_gk.py:303-328` already computes exactly the domain you described:
```python
def _gk_distribution_mask(actions, frames):
    """True for in-scope GK distributions: any goalkick, OR a pass/throw_in whose
    actor is the acting team's goalkeeper (resolved from frames' is_goalkeeper...)."""
    ...
    return is_goalkick | (is_open & actor_is_gk)
```
It is **private** (leading underscore), **not exported** from `silly_kicks.tracking.__init__`, and used only to scope the `xt_gk` family. We'd like to consume it directly.

**Requested API (please consider):**
- **Public export** in `silly_kicks.tracking` (e.g. `gk_distribution_mask`), documented + tested as a stable contract (we'll pin the min version).
- **Frame-optional signature**: `gk_distribution_mask(actions, frames=None) -> pd.Series[bool]`, aligned to `actions`. With `frames=None`, return **goal-kick-only** (`is_goalkick`); with frames, the full mask. This lets us make a single call across all providers — full domain where we have frames (tracking providers), goal-kicks-only where we don't (event-only), without branching on our side.
- **Robust actor resolution (please confirm):** the current private version resolves `actor_is_gk` via a plain per-frame `is_goalkeeper` set-membership check. Your `acting_gk_from_frames` (TF-13) has a **roster-identity fallback** for undetected keepers (~40% of goal-kicks per its own docstring), and we already use *that* for our goal-kick-taker override. For consistency + accuracy, we'd prefer the exported mask resolve the acting GK via `acting_gk_from_frames` (or share its fallback), so the domain marker and the taker override agree. Your call — flag if you'd rather keep the lighter check.
- **NaN-safety** on the same terms as `acting_gk_from_frames` (NaN `team_id` / no GK identity → not-GK, not a crash).

## 4. What the lakehouse will do with it

- Call the exported function in our action-context path (`src/analytics/action_context/enrich.py`, tracking-provider arm — same place we call `acting_gk_from_frames` today for the goal-kick taker override) and materialize a new boolean `is_gk_distribution` onto `fct_action_context` (which your ρ loader already joins). Full domain for the tracking providers (GS/SC = your priority).
- Add a goal-kick-only coverage OR so every provider (incl. statsbomb/wyscout, event-only) flags goal-kicks. Event-only acting-GK open-play passes are deferred (we don't thread lineup-GK identity into that path yet).
- **Acceptance we'll verify live:** True-rate ≈ 50–70/match on GS/SC (goal-kicks ~15/match + acting-GK passes ~35–55), all `type_id==22` True, no fan-out on `(match_key, action_id)`. (Live-verified today: goal-kicks are `type_id=22`, GS 15.6/match, SC 11.0/match.)

## 5. Coverage caveat to note

Per-provider: **full domain** (goal-kicks + acting-GK passes) on the 4 tracking providers (gradientsports, skillcorner, idsse, metrica); **goal-kicks-only** on statsbomb/wyscout (no frames; lineup-GK not in that path). Your GS/SC gate cohort gets the full domain.

## 6. If you decline the export (our fallback — no action needed from you)

We reimplement the ~5–10-line wrapper (`goalkick OR (open-play pass whose actor == acting GK)`) on top of your **already-public** `acting_gk_from_frames` (silly-kicks 4.39.0, which we already call). No silly-kicks release required; we proceed independently. The only downside is a small piece of your metric's logic living in our repo (drift risk) — which is why the export is our preference.

## 7. References

**silly-kicks (your repo):**
- `silly_kicks/spadl/utils.py:500-713` — `add_pre_shot_gk_context` (the *existing* `gk_was_distributing`)
- `silly_kicks/tracking/_xt_gk.py:303-328` — `_gk_distribution_mask` (the target logic to export)
- `silly_kicks/tracking/_gk_resolve.py:134-215` — `acting_gk_from_frames` (TF-13; already public, we already use it)
- `silly_kicks/spadl/config.py:40-64` — `goalkick` = action type index 22

**lakehouse (our repo):**
- `src/ingestion/spadl_enrichments.py:106` — where we call `add_pre_shot_gk_context(frames=None)`
- `src/analytics/action_context/enrich.py:179-220,512` — where we call `acting_gk_from_frames` for the goal-kick taker override (the intended home for the new column)
- `docs/superpowers/specs/2026-07-10-f1-gk-distribution-domain-design.md` — our full F1 design (semantic collision, forks, recommendations)
- `docs/superpowers/plans/2026-07-10-f1-gk-distribution-domain.md` — our implementation plan
