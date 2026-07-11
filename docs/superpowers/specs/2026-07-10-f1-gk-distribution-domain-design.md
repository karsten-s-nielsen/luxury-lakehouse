# Design: F1 — a GK-distribution-domain marker for xT-GK v2 (`gk_was_distributing` gap)

**Status:** proposed (2026-07-10) · **For review by:** a follow-up lakehouse session
**Origin:** silly-kicks handoff `docs/research/xtgk_possession_value/LAKEHOUSE_HANDOFF.md` §F1 (in the silly-kicks repo).
**Consumer:** silly-kicks xT-GK v2 ρ-retention model reads this to scope the "GK is distributing" domain.

> **D2 RESOLVED (2026-07-10):** silly-kicks **4.43.0** shipped the export as the public, frame-optional `gk_distribution_mask` (no underscore) — NOT the private `_gk_distribution_mask` referenced in the exploratory sections below. The reimplement fallback (§6) is dead. The lakehouse calls `gk_distribution_mask(actions, frames, resolve_gk="robust")` on the tracking arm and `gk_distribution_mask(actions, frames=None)` (goal-kicks-only) where there are no frames — the mask owns the goal-kick term, so there is **no** hand-rolled `type_id==22` OR. See the plan for the folded-in cross-session review.

---

## 0. The pivotal finding (read this first)

`fct_action_values.gk_was_distributing` is **NOT an unpopulated flag** — it is a silly-kicks column with a **different, correct-by-design meaning**, and repurposing it would break a live consumer.

- **Existing meaning:** "the **defending** GK was distributing in the action **immediately preceding this SHOT**" — a shot-scoped pre-shot GK-context feature produced by silly-kicks `add_pre_shot_gk_context` (`silly_kicks/spadl/utils.py:500-713`), which initializes `False` and only flips `True` **on shot rows**. Live proof (statsbomb): 44 True total, **44 on shots, 0 on non-shots**. It is a sibling of `gk_was_engaged` / `gk_actions_in_possession` / `defending_gk_player_id` from the same function.
- **Live consumer already depends on the current semantics:** `dbt_project/models/marts/fct_gk_tracking_stats.sql:58-61` carries a warning that ANDing `gk_was_distributing` into a distribution filter "zeroed the entire distribution family (ADR-051 follow-up)" — i.e. someone already hit this exact mismatch.
- **What silly-kicks actually needs** is a *domain marker*: **True for every goal-kick, OR any open-play pass/throw-in whose actor is the acting team's GK.** That is a different predicate over a different row population (mostly non-shots).

**⇒ Recommendation R0 (the load-bearing decision): DO NOT overload `gk_was_distributing`. Add a NEW column** (proposed name `is_gk_distribution`, boolean) carrying the domain semantics, and have silly-kicks consume the new column. The existing pre-shot feature stays untouched. (This must be relayed to silly-kicks — the handoff asked for `gk_was_distributing`, but the correct answer is a new column.)

---

## 1. The target predicate (what the new column means)

`is_gk_distribution = True` iff the action is **either**:
- **(a) a goal-kick** — SPADL `type_id == 22` / `type_name == "goalkick"` (a first-class SPADL type; `silly_kicks/spadl/config.py:40-64`), **or**
- **(b) an open-play `pass`/`throw_in` whose actor is the acting team's goalkeeper.**

silly-kicks **already implements exactly this**: `silly_kicks/tracking/_xt_gk.py:303-328` `_gk_distribution_mask(actions, frames)` → `is_goalkick | (is_open & actor_is_gk)`, where `actor_is_gk` is resolved from the tracking frames' roster-derived `is_goalkeeper`. It is **private** (underscore, unexported) and currently used only to scope the `xt_gk` family (tracking-provider AC path).

---

## 2. Live-grounded facts (verified 2026-07-10)

| Fact | Value |
|---|---|
| `gk_was_distributing` True-rate | ~0 everywhere; shot-only (GS 0/88,958, SC 0/134,760, statsbomb 44/44-on-shots) |
| Goal-kicks (`type_id=22`) per match | GS 15.6, SC 11.0, statsbomb 15.9, wyscout 16.4, idsse 20.6, metrica 11.0 — **all providers**, currently unflagged |
| `gk_role` | defensive-only (`sweeping`/`shot_stopping`), ~37/match GS — cannot substitute (F6) |
| Handoff acceptance target | ~50–70 distributions/match GS/SC = goal-kicks (~15) + acting-GK open-play passes (~35–55) |

---

## 3. Data-flow map (where the value would be produced)

Two structurally separate pipelines, and this is the crux of the design:

- **Bronze SPADL path** (`spadl_conversion.py` → `apply_spadl_enrichments` → `bronze.spadl_actions` → `spadl.vaep_action_values` → `fct_action_values`). Runs per-provider but **frame-free** (`add_pre_shot_gk_context(enriched, frames=None)`, `spadl_enrichments.py:106`). This is the path that produces the `gk_was_distributing` that reaches `fct_action_values`. It has **no tracking frames** → cannot do the "actor is acting-team GK" half.
- **Action-context path** (`analytics/action_context/enrich.py` → `bronze.spadl_action_context` → `fct_action_context`). **Frame-based, tracking-providers-only** (gradientsports/idsse/skillcorner/metrica). Has `acting_gk_from_frames` (already used for the goal-kick taker override, `enrich.py:179-220`) and `_gk_distribution_mask` available. But ADR-056 removed the GK flags from `fct_action_context`'s schema.

silly-kicks' **retention cohort joins BOTH** (`fct_action_values ⋈ fct_action_context`), so the new column can live on **either** mart and still be reachable.

**GK-identity signal per provider** (for half (b)): tracking providers have roster-derived `is_goalkeeper` on frames (accurate, handles subs). Event-only providers (statsbomb/wyscout) have `dim_players` roster GK position but it is **not threaded into the SPADL-conversion pandas path** today; idsse `dim_players.primary_position` is NULL.

---

## 4. Design forks (each with a recommendation)

**D1 — New column name + home mart.** Proposed `is_gk_distribution` (boolean). Home:
- **Option A (recommended): compute in the AC path, materialize on `fct_action_context`** (bronze `spadl_action_context` → mart). The AC path has frames + `_gk_distribution_mask`; silly-kicks already joins `fct_action_context`. Covers the priority (GS/SC) accurately incl. keeper subs.
- Option B: compute in dbt over `fct_action_values` using a `dim_players` roster-GK join (frame-free, all-provider, but imprecise on subs + NULL for idsse).
- **Recommendation: A for the tracking providers** (accuracy + reuse the owner's exact logic), **+ a goal-kick-only fallback for every provider** (half (a) needs no frames — a trivial `type_id==22` OR that also covers statsbomb/wyscout). Net: full domain on tracking, goal-kicks-only on event-only (which is the honest limit without a lineup-GK join).

**D2 — Reuse silly-kicks `_gk_distribution_mask` vs reimplement.** It is the exact target logic but **private/unexported** and frame-required.
- **Recommendation: coordinate with silly-kicks to EXPORT `_gk_distribution_mask`** (make it public in `silly_kicks.tracking.__init__`, ideally frame-optional so goal-kick-only works without frames) — since silly-kicks OWNS the metric + asked for the fix, the logic should live with them, not be re-derived in the lakehouse (avoids drift; consistent with "silly-kicks is the SPADL/xT toolkit" ownership). Fallback if they decline: reimplement the OR-logic in `analytics/action_context` using the already-adopted `acting_gk_from_frames`. **This is a cross-repo coordination point — silly-kicks is a parallel session; do not edit it.**

**D3 — Event-only providers (statsbomb/wyscout) half (b).** No frames; lineup GK not currently in the SPADL path.
- **Recommendation: OUT OF SCOPE for v1** — event-only providers get goal-kicks-only (half (a)). Adding a `stg_*__lineups` GK join into the SPADL/AC path is a separate, larger change. Document the per-provider coverage explicitly ("never silently substitute"). silly-kicks' priority is GS/SC (tracking), which get the full domain.

**D4 — Sibling columns.** `gk_was_engaged` / `gk_actions_in_possession` / `defending_gk_player_id` keep their current pre-shot semantics — **explicitly out of scope**; only the new `is_gk_distribution` is added.

**D5 — Stale-comment / consumer audit.** The `fct_gk_tracking_stats.sql:58-61` "DISJOINT" warning + any BI/dashboard filtering `gk_was_distributing = false` as a today-no-op must be audited (a new column doesn't change `gk_was_distributing`, so this risk is *mitigated by R0* — another reason not to overload).

---

## 5. Invariants / acceptance

- `gk_was_distributing` and its siblings are **unchanged** (R0) — regression-proven by the existing shot-scoped tests.
- New `is_gk_distribution` on tracking providers: True-rate ≈ goal-kicks + acting-GK open-play passes ≈ **50–70/match** GS/SC (handoff acceptance). Goal-kicks alone (~15/match) must all be True on every provider.
- No fan-out / no new NULLs on `(match_key, action_id)`; column threads through the ADR-016 8-layer parity (if on the SPADL path) or the AC bronze→mart contract (if on the AC path) with the matching parity test updated.
- silly-kicks re-runs its gate + retrains ρ on the expanded domain and reports back.

## 6. Coordination + out of scope

- **silly-kicks coordination (D2):** export `_gk_distribution_mask`. This spec assumes that; if silly-kicks declines, fall to the reimplement fallback. Relay R0 (new column, not `gk_was_distributing`) regardless.
- **Out of scope:** F2 (relayed — `bekkers_pi`), F3 (SC OOD, by-design), F4/F5/F7 (separate contract-doc PR), the event-only acting-GK-pass half (D3).

## 7. Open question for the reviewer

Is the AC-path/`fct_action_context` home (D1-A) acceptable given silly-kicks reads the domain from the join, or is a `fct_action_values`-native column (D1-B, dbt roster join) preferred for a simpler single-mart contract? This trades accuracy (frames, sub-aware) against self-containment (no silly-kicks export dependency).
