# ADR-077: silly-kicks 4.89.0 full adoption — corpus-wide recompute, wide-by-grain marts, xtgk v2 replaces v1

| Field | Value |
|---|---|
| **Date** | 2026-08-20 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen |

## Context

The lakehouse pinned silly-kicks **4.43.0** (2026-07-10). Since then 42 versions shipped: a
direction-of-play correctness cycle (ADR-028/051), SB360 enablement plus a velocity-less
pitch-control lift (ADR-062/063), and roughly eleven new column families (packing, off-ball
run values, press-commitment, obso_epv provenance, team-shape gaps, visibility coverage,
defensive-credit, bravery, gkdv, xt-gk v2, `shot_blocked`/`cross_blocked`). Adopting these is
not a mechanical pin bump: several are value-shifting on the existing surface (the mandatory
`xt=` real-xT OBSO switch — omitting `xt=` emits a non-fatal `SyntheticEPVWarning` and falls back
to synthetic EPV, so a bare bump silently degrades every action-context run to synthetic values),
and the direction/orientation cycle moves live values corpus-wide.

The forcing function is that this can no longer be taken piecemeal without stranding the gold
layer half-refreshed. The design spec (Rev 7) re-points the target to **4.87.0** — the release
the silly-kicks *Part Deux* cycle cut (2026-08-19); the 4.85→4.87.0 delta is the additive
StatsBomb `cross_blocked` un-defer (4.86.0), the off-ball-context crash-fix (4.86.1), and a
reported-not-gated research-tooling cycle (4.87.0, ADR-064/PR-S157) that carries **no** library
API / column / aggregator / behaviour change. The Rev-6 design therefore holds unchanged under
a mechanical pin bump.

**Part-A pin advanced 4.87.0 → 4.89.0 (2026-08-21).** 4.88.0 is NIL lakehouse impact (SB360
boundary-audit test registry + a gkdv docstring). 4.89.0 (sk ADR-065 / PR-S159) **lands the
`action_id` chronological-order fix** that §6.7 tracked as pending — all six order-dependent
converters now sort chronologically before positional derivation, guarded by a raise-by-default
`_assert_chronological_action_id` at `_finalize_output`. The only breaking input-contract change is
Gradient Sports gaining a required `start_time` column, supplied by the lakehouse shaper from bronze
`startTime`/`eventTime`. Additive to the Rev-6 design; the pin bump is mechanical.

Two review rounds *recommended* deferring or excluding three families (defensive-credit,
bravery, gkdv). That recommendation is advisory, not scope: the user directed that **nothing is
deferred**. Every new 4.87.0 metric is adopted, organized **wide-by-grain**, and the full
tracking-plus-downstream-model surface is recomputed on live data.

## Decision

Adopt silly-kicks **4.87.0** in one lockstepped, CI-green change and recompute the entire
tracking + downstream-model surface on live data — **full scope, nothing deferred**. Every new
metric family lands in a mart keyed by its **grain** (never one mart per feature), extending an
existing mart where the grain already exists and creating a new grain-named mart only where it
does not. The in-repo **xt-gk v1 chain is retired and replaced by the silly-kicks `xtgk` v2
pipeline** (`gk_geometry_source` + the v2 metric).

### Wide-by-grain mart map (spec §7)

| Grain | Mart | Action | Families |
|---|---|---|---|
| per-action, tracking | `fct_action_context` | extend | obso-xt, run-values, press-commitment, packing, provenance, team-shape gaps, xtgk-v2, visibility |
| per-action, post-xG | `fct_action_defensive` | **new** | per-action defensive-credit — downstream of `fct_shot_xg` (cannot extend `fct_action_values`: xG is upstream-joined into it, so an xG-dependent column there is a dbt cycle) |
| SPADL action | `fct_action_values` / bronze SPADL | extend | `shot_blocked`, `cross_blocked` |
| per-`(match, team)` | `fct_match_summary` | extend | bravery (`compute_bravery`, defending-team grain) |
| per-keeper-pooled `(player, comp, season)` | `fct_gk_shot_stopping_pooled` | extend | gkdv `aggregate_by_keeper` |
| per-run `(action, runner)` | `fct_off_ball_runs` | **new** | `detect_off_ball_runs` + `value_off_ball_runs` |
| per-`(action, player, rule)` | `fct_defensive_credit_attributions` | **new** | `compute_defensive_credits` (long-form) |

New marts follow the ADR-013 writer → bronze → dbt staging → contract-enforced gold pattern.

### xtgk v2 replaces v1 — a FIT training sub-project, not wiring

Retire the v1 `add_xt_gk` chain and adopt `apply_resolved_gk_geometry` → `extract_retention_features`
→ `compute_xt_gk_v2`. At 4.87.0 only the `retention` model is bundled; `possession_value` and
`turnover_cost` are **not** bundled and must be `.fit()` on the recomputed gold action marts
(the docstring requires the terciles/features match the fit corpus). xtgk-v2 is therefore an
ADR-012 trainer (`scripts/train_xt_gk_v2_hf.py`) whose fitted models feed an ADR-013 writer that
scores `xt_gk_v2` / `gk_geometry_source` — it is the largest single workstream in this migration,
not a call-site adoption. The v1 tactical-philosophy presets (`xt_gk_{possession,counter,direct,
high_press,low_block}`) have **no** v2 successor and are dropped (a known regression); `gk_completion`
is a distinct call and is kept.

### `cross_blocked` consumers and the "no retrain" boundary

The additive StatsBomb `cross_blocked` value change (all-`pd.NA` → a real open-play-cross mask)
is now consumed by `compute_bravery` (→ `fct_match_summary`) and by the per-action defensive-credit
family — and **no VAEP or atomic-VAEP feature reads it**, so "no retrain of VAEP is triggered by
`cross_blocked`" holds; the recompute simply materializes the value change on the StatsBomb SPADL
surface (verified by the §11.1b expected-shift oracle: non-null 0% → base-rate).

### Recompute and retrain

A DAG-complete rebuild (spec §11.3): enumerate the downstream set with `dbt ls --select <root>+`
for every changed root and union it (a hand-picked list ships a half-refreshed gold layer),
partition by TRIGGERED-synced membership (TRIGGERED marts through `rederive_synced_marts.py`,
**never** `dbt --full-refresh` per ADR-043), refresh SNAPSHOT-synced marts. Retrain set is
**VAEP + ScoutGPT + xG v3** (corpus-wide; football2vec and PSxG are NOT-NEEDED — verified they
read unchanged action attributes / StatsBomb-native geometry), plus the **xtgk-v2 FIT**. The
correctness gate is an **expected-shift oracle** (spec §8: cohort + rate-band + direction per
column) plus per-column null-rate bounds and new-column range/vocab checks — the rebaselined
goldens are regression guards only, because a wrong-but-stable 4.87.0 value is invisible to a
golden regenerated from 4.87.0.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Defer defensive-credit / bravery / gkdv (the review recommendation) | Smaller PR; fewer new marts; avoids the xG-cycle and `[das]` extra now | Leaves the library half-adopted; a second migration to finish it; governance surface incomplete | User directive: nothing is deferred. Reviewers advise; they do not override user scope. |
| B. One mart per new feature | Simplest per-feature dbt model | Mart sprawl; breaks the established `fct_action_context` / `fct_action_values` wide-by-grain pattern; N contracts to maintain | Grain-bucketing is the project's standing architecture (wide marts keyed by grain), not a per-feature convention. |
| C. Keep xt-gk v1, add v2 alongside | No preset capability loss; smaller training scope | Two divergent GK-value surfaces; v1 is superseded upstream; ongoing drift | v2 is the maintained surface; running both is debt. Preset loss is accepted as a known regression. |
| D. Bare pin bump, no `xt=` / no recompute | Trivial diff | Omitting `xt=` silently falls back to synthetic EPV on 4.52+ (emits `SyntheticEPVWarning`, not an error); wrong values across the corpus | Not viable — the bump and the mandatory `xt=` real-xT switch must land together. |
| E. **Full adoption, wide-by-grain, corpus recompute (chosen)** | Library fully adopted; gold layer coherent; governance complete | Large PR; corpus-wide recompute (~5.5 h AC cold-start) + four retrains/fits; two upstream fixes gate Part B | — |

## Consequences

### Positive

- silly-kicks is fully adopted; every 4.87.0 metric family is materialized with an enforced dbt
  contract, and the gold layer is rebuilt DAG-complete rather than hand-picked (no half-refresh).
- Governance is complete and unconditional: each new evaluative family has a workflow card + HF
  model card + `AI_GOVERNANCE.md` scope row + Appendix-D academic reference (this ADR's companion
  governance chunk), and a new-private-import lint guards the adoption seam.
- The wide-by-grain marts hold all future metrics at their grain (`fct_action_defensive` is the
  home for post-xG per-action metrics; `fct_off_ball_runs` for per-run), so the next family
  extends a mart instead of adding one.

### Negative

- **xtgk-v2 is not construct-validated.** The v2 possession-value / turnover-cost surface is fit
  on this corpus without an independent construct-validation study; adopting it (and dropping the
  five v1 philosophy presets with no successor) is an **explicitly accepted consequence** of the
  user's v2-replaces-v1 decision, not a validated equivalence. It is flagged here so a future
  maintainer does not read `xt_gk_v2` as a validated metric.
- Corpus-wide recompute + VAEP/ScoutGPT/xG-v3 retrain + xtgk-v2 fit is the largest single
  migration in the project's history; the biggest risk is a value-shifting recompute whose
  goldens validate stability, not correctness (mitigated by the expected-shift oracle, not the
  goldens).
- New `[das]` extra dependency for gkdv; the xG-cycle constraint forces `fct_action_defensive`
  to build after the xG-v3 retrain and `fct_shot_xg` rebuild.

### Neutral

- Two **upstream silly-kicks fixes were surfaced** by this adoption and gate different phases:
  (1) the **4.86.1 off-ball-context guard** inside `_line_break_kernel` (a crash-fix, no value
  change on resolvable inputs, rides inside the target); and (2) the **non-chronological `action_id`
  conversion fix — now LANDED in silly-kicks 4.89.0 (sk ADR-065 / PR-S159)**, which advances the
  Part-A pin 4.87.0 → 4.89.0. `spadl/sportec.py` numbered `action_id` over raw DFL document order
  rather than `(period_id, time_seconds)`; the fix makes all six order-dependent converters sort
  chronologically before positional derivation, enforced by a raise-by-default
  `_assert_chronological_action_id` at `_finalize_output`. **Measured per-provider retrain scope**
  (per the 4.89.0 changelog, measured — not inferred from the gate): **IDSSE/sportec + wyscout
  genuinely change** (their real feed is non-chronological; wyscout was additionally chronology-broken
  and its fix reorders output) → regenerate their goldens + retrain VAEP/ScoutGPT/xG-v3 on the
  corrected SPADL in Part-B; **GS + skillcorner are byte-identical on real data → NOT retrain
  triggers**; **metrica is unmeasured (not in the pining corpus) → a real-data M-C check is
  recommended before re-materialization**. It does not block Part-A code on 4.89.0 (the IDSSE
  mini-golden 3-action slice has no time-inversion, so the guard is a no-op there).
- The scheduling/wiring of the five new ADR-013 writers (entry points registered, but no scheduled
  TF task) is an open operator decision, resolved in a follow-up wiring PR, not here.

## Related

- **Specs:** `docs/superpowers/specs/2026-08-18-silly-kicks-4-85-full-adoption-design.md` (Rev 7)
- **Plans:** `docs/superpowers/plans/2026-08-19-silly-kicks-4-86-1-full-adoption.md`
- **ADRs:** builds on ADR-013 (ML-inference output pattern), ADR-012 (training-to-production
  delivery), ADR-016 (SPADL enrichment stage), ADR-036 (xt-gk v2 formulation), ADR-043
  (strand-safe synced re-derive), ADR-046 (serverless env exact pins), ADR-064 (per-match
  access-tier / the 4.87.0 research-tooling cycle); retires the in-repo xt-gk v1 chain.
- **External references:** silly-kicks `NOTICE` (canonical bibliography for the new-methodology
  academic references recorded in `ARCHITECTURE.md` §8 Appendix D); silly-kicks tag `v4.89.0`
  (Part-A pin; advanced from `v4.87.0` to fold in the ADR-065 `action_id` chronological-order fix).

## Notes

The academic references for the new published methodologies (packing → Goes et al. 2019;
off-ball runs → Power et al. 2017 / Vidal-Codina et al. 2022 / Esposito et al. 2026;
defensive-credit sizing → Bischofberger, Bauer & Baca 2026; gkdv → Le et al. 2017 /
Bischofberger & Baca 2026 / Shaw & Sudarshan 2020) are recorded in `ARCHITECTURE.md` §8 Appendix D
and mirrored in each family's HF model card, per the three-way-sync discipline. press-commitment
and bravery are silly-kicks-native original metrics with no published paper and deliberately get
no Appendix-D entry.
