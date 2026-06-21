# PSxG subsystem — unified shot-grain fact + GK shot-stopping aggregate (all providers) — Design Spec

- **Date:** 2026-06-20 · **Revised:** 2026-06-21 (post parallel-critic reviews ×2 — d32 session)
- **Status:** DRAFT v3.3 — build-ready pending the gated items in §6/§8
- **Related:** ADR-013 (ML inference → writer→bronze→staging→mart, surrogate keys resolve in the mart), ADR-012 (training→production delivery), ADR-049 (restricted HF companion repos), ADR-054 (per-provider HF dataset configs), TF-48 (shot-goalmouth), [[reference-shot-goalmouth-z-gap-metrica-skillcorner]]
- **Design principle:** gold-standard, long-term. A **new fact at the appropriate grain** (mirroring action-context), **not** patching existing GK marts.
- **Downstream-consumer status (GK-page session, 2026-06-21):** the GK page **decoupled** from this subsystem and ships first — its defensive *hero* is the dense per-action **sweeper family** (`gk_pitch_control_share_weighted` / `gk_reachable_area_m2` / `gk_closing_time_*`, ~100% non-null on ~150 defended actions/GK); `goals_prevented` is an **additive secondary KPI** read from the **pooled layer** as value ± CI + `coverage_pct` + `low_sample`. **This changes urgency, not scope** — single cycle, full subsystem still built as specified. The consumer explicitly endorses keeping the on-target gate **strict** (trustworthy-thin over padded) and the ranking path **off/unbuilt** (NULL column/flag retained; no `percent_rank` computation). Do not loosen the gate, suppress `low_sample`, or descope on account of the de-prioritization.

### Changelog v1→v2 (review dispositions)
Accepted in full: **B1** (pin one on-target definition — now verified *severe*: 46% of the StatsBomb training set is off-target), **B2** (out-of-sample calibration), **C1/C3** (small-n contract + pooled comparison grain), **C4** (2-hop bridge), **C5** (per-provider defending-GK attribution), **D** (single normalization port; test-first plan; handedness fixture; e2e golden), **E** (pinned downstream contract §10), **F** (Platt+norm version stored; explicit `yellow_card` drop). Modified: **C2** (report per-provider reliability + bands; *defer* the provider covariate at n=37/13), **C4 fix** (add native `event_id` to `fct_shots` rather than recompute dbt's surrogate MD5 elsewhere). **Q3** → phased (i). Q1/Q2 already resolved in v1 (§4). Added: **§5.5 Kimball conformance** (fact-type classification, conformed-dimension reuse, role-playing `dim_players`, degenerate `action_id`, measure-additivity discipline) + additivity column on the §10 contract.

**v3.2→v3.3 (user scope decisions 2026-06-21):** **StatsBomb consolidation pulled IN** — single all-provider delivery, not phased-deferred (D-D/§8 Q3); parity guards retained as risk controls. **Q5 deferral explicitly approved** after a no-clean-path investigation (different key universes + 5 xG consumers; `fct_shot_psxg` stays unification-ready on the universal key). **Richer model + per-provider covariate deferrals explicitly approved.**

**v3.1→v3.2 (plan-review round 2 — contract-touching items):** **A1** — pooled band is the **closed-form Poisson-binomial** interval `±z·√(Σ psxgᵢ(1−psxgᵢ))`, **not** bootstrap (degenerate at n≤14); §10 updated. **C2** — `low_sample` documented as the norm (~69% of GKs <5 shots). (A2 attribution-parity, B1 §10-sync confirmation, B2 P-1 gating, C1/C3 are plan-side — see the plan changelog.)

**v3→v3.1 (plan-review feedback that touches the contract — all accepted):** **B1** (verified live: max GK faced 14 on-target shots, 0 ≥30 → percentile leaderboard **deferred**; pooled surface = raw `goals_prevented` ± CI; `ranking_enabled` gate + non-emptiness acceptance — §3/§10). **B2** (per-`(GK,match)` is drill-down, **not** evaluative — pinned §10). **B3** (`goals_conceded_on_shots` pinned to the gate-passed set, golden-asserted). **D1** (column is `defending_gk_player_key`/`defending_gk_player_id_native` — there is **no** `defending_gk_player_id`; fixed D-A). Plan-side items (GroupKFold by match, JOIN-based bridge verification, model-card eval-metric refresh, governance card-parity check, `yellow_card` log, rollback re-derive, breaking-change UX caption) live in the executable plan.

**v2→v3 (re-review 2 dispositions — all accepted):** **B1** D-0 is a *governed, breaking* change — added the AI-governance chain (`wf-goalkeeper` IS in `PER_PLAYER_EVALUATIVE_CARDS`, verified `test_ai_governance_md.py:31`) + the model-card "2.44 m" fix + `model_version` bump + published-artifact refresh. **B2** sequenced the upstream dependency: fix export/publish on-target filter → republish `statsbomb-shots-on-target` → retrain (ADR-012 delivery contract). **B3** gating-induced selection bias on `goals_prevented` → coverage measures + per-GK drop logging. **C1** decided — **exclude `Saved Off Target`**; on-target = `Goal, Saved, Post, Saved to Post`. **C2** `psxg_recalibrated` defined per modality. **C3** pinned the pooled/percentile layer contract (§10). **C4** 1:1 bridge-cardinality test. **D** e2e golden now multi-provider + folds the coverage assertion. **E** `fct_shots` schema-evolution handling + `shot_speed` known-limitation note.

---

## 1. Problem

PSxG is the shot-stopping pillar of GK evaluation. Today it is **StatsBomb-only** (historical: predates TF-48). TF-48 now derives goalmouth geometry from tracking (`shot_crossing_y/z`), verified populated for GradientSports + SkillCorner + IDSSE (§3). Three structural problems a naive "add tracking" patch would entrench:

1. **Shots fragmented across grains** — event shots in `fct_shots`, tracking shots as a *row subset* of `fct_action_context`. No single shot-grain home.
2. **PSxG logic inlined in a consumer** — StatsBomb PSxG + `goals_conceded` live in CTEs inside `fct_goalkeeper_stats.sql:266-298` (`goals_prevented` at `:378`). The only "API" for PSxG is a CTE.
3. **The existing PSxG population is mislabeled (verified, §3)** — trained on `end_location_z IS NOT NULL`, which is **46% off-target**. Any unified fact must not launder this into a published source of truth.

## 2. Goal / Non-goals

**Goal:** a gold-standard PSxG subsystem — one **shot-grain fact** (all providers, both input modalities) + a **GK-grain aggregate** for goals-prevented, on **one pinned on-target definition**. New providers slot in via a writer, not a mart edit.

**Non-goals:** Metrica (no bronze ball-z); StatsBomb action-context (held); a new model architecture (reuse the 2-feature logistic, retrained on the corrected population); off-target shots; subsuming `fct_shots` into a fuller unified shot fact (Q5 — deferred).

## 3. Verified current state (live, 2026-06-20/21)

**Tracking PSxG-input coverage** (`fct_action_context`, on-target ∧ z-present, joined to `fct_action_values` for the goal label `action_result='success'`):

| provider | on-target shots | goals | goal rate | avg `shot_crossing_confidence` | median `shot_fit_rmse` | `shot_crossing_y` (m) | `shot_crossing_z` (m) |
|---|--:|--:|--:|--:|--:|--|--|
| gradientsports | 235 | 77 | **32.8%** | 0.795 | 0.199 | 30.26–37.73 | 0.0–2.52 |
| skillcorner | 37 | 16 | 43.2% | 0.669 | 0.471 | 30.33–37.56 | 0.0–2.33 |
| idsse | 13 | 2 | 15.4% | 0.711 | 0.119 | 30.68–37.24 | 0.0–0.49 |
| metrica | — | — | — | — | — | — | z=0 (structural) |

Pooled **95/285 = 33%**. y ≈ SPADL posts [30.34, 37.66]; z max ≈ crossbar 2.44 m → inputs are SPADL metres, geometry validated.

**Per-defending-GK volume (decisive for the evaluative surface — verified live):** 285 on-target shots spread over **80 GKs** → **0 GKs faced ≥30, 4 ≥10, 25 ≥5, max any GK = 14, avg 3.56** (0 unresolved defending-GK). **Consequence:** a `percent_rank` GK leaderboard is **not viable** at this volume (floor-30 empty, floor-10 = n=4); per-`(GK,match)` goals-prevented is ~1 shot/match = noise. The only defensible evaluative surface is **pooled goals-prevented as a raw value + wide CI**, percentile **deferred** until volume grows (see §10).

**StatsBomb `shot_outcome` × z-present (the B1 finding):**

| shot_outcome | shots | z present | on-target? |
|---|--:|--:|---|
| Off T | 28,460 | 28,460 | **NO (off-target — 46% of training set)** |
| Saved | 20,780 | 20,780 | yes |
| Goal | 9,788 | 9,788 | yes |
| Post | 1,842 | 1,842 | yes |
| Saved Off Target / Saved to Post | 349 / 288 | both | edge |
| Blocked / Wayward | 21,670 / 4,822 | 0 | n/a (no z) |

Current training filter `z IS NOT NULL` = 61,507 shots, **15.9% goal rate** (contaminated — 46% `Off T`). **True on-target = `Goal+Saved+Post+Saved to Post`** (C1: `Saved Off Target` excluded — geometrically off-target, 349 rows, 0 goals) = **32,698 shots, 29.9% goal rate** — which *matches* the tracking 33%. `original_event_id` is 100% populated on StatsBomb shot actions (87,999/87,999) → C4 bridge precondition holds.

## 4. Inputs & the model (verified — unchanged from v1)

`src/analytics/goalkeeper.py:_normalise_goalmouth` normalizes in **StatsBomb units** (≈0.915 m/unit): `y_norm=(y−36)/8`, `z_norm=z/8` (`_GOAL_Z_MAX=8.0`), then `StandardScaler`, then logistic. Model-card "2.44 m" is wrong. **Correct tracking mapping (both ÷7.32 m):** `y_norm=(shot_crossing_y−30.34)/7.32`, `z_norm=shot_crossing_z/7.32` (crossbar 2.44/7.32=0.333 ≈ SB 2.67/8=0.334 ✓). Per D-E this lives in **one** normalization port.

## 5. Decisions

### D-0 (BLOCKING) — pin ONE on-target definition; retrain StatsBomb on it (a governed, breaking, sequenced change)
**On-target definition (C1, decided):** `shot_outcome IN ('Goal','Saved','Post','Saved to Post')` (StatsBomb) / `shot_on_target_derived = true` (tracking). `Saved Off Target` is **excluded** (off-target geometry, 0 goals; keeping it re-introduces the contamination D-0 removes and mismatches tracking's *geometric* on-target). *Open precision item:* confirm tracking treats post-hits the same as StatsBomb `Post`/`Saved to Post` so the definitions are truly parallel. Corrected StatsBomb on-target set ≈ **32,698 shots @ 29.9%**, matching tracking's 33%.

**Retrain the 2-feature logistic on this corrected set** — cheap, and the dominant fix: removes the 46% off-target contamination *and* aligns the model population with tracking's, so cross-modality transfer beats any Platt offset on a contaminated model. Reuse-as-is (v1 D-C) **rejected**. But "reuse the model" is now **"replace the model"**, which carries three obligations:

- **(a) AI governance — non-negotiable (CLAUDE.md).** `wf-goalkeeper` IS a per-player evaluative card (`test_ai_governance_md.py:31` → `psxg-model.md`). Retraining REQUIRES: update `AI_GOVERNANCE.md` §5, update `docs/huggingface/model-cards/psxg-model.md` (**and fix its wrong "2.44 m" in the same pass** — §4), refresh the `governance:` YAML on the workflow card, re-run `uv run pytest src/tests/test_ai_governance_md.py`. Explicit plan steps (§6), or the merge gate fails.
- **(b) Intentional Hyrum break.** The retrained model yields *different* psxg for the same StatsBomb shots → live `goals_prevented`, percentiles, and the published psxg HF dataset all shift. This is a deliberate value change to a published metric: **bump `model_version`**, refresh published artifacts, and the D-D parity guard re-baselines against the *retrained* legacy (not the contaminated one).
- **(c) Sequenced upstream dependency (B2).** The training set is the HF dataset built by `export_shots_on_target.py:123` + `publish_shots_on_target_hf.py:78`, whose filter is *today* `end_location_z IS NOT NULL`. Order is mandatory: **fix the export/publish filter to the on-target `shot_outcome` set → republish `statsbomb-shots-on-target`** (which finally makes the name accurate) **→ retrain** honoring the ADR-012 delivery contract (`require_mlflow_env` / `set_and_verify_mlflow_champion` / `upload_weights_to_uc_volume`).

### D-A — `fct_shot_psxg`: shot-grain fact (the action-context analog)
**Grain:** one row per **true-on-target** shot, all providers. Direct analog of `fct_action_context` being a dedicated mart.
- **Key:** `(match_key, action_id)` (every shot is a SPADL shot action across all providers; StatsBomb bridge per D-D).
- **Population:** all **on-target** shots (D-0 definition), incl. gate-failed ones (D-F: kept with `psxg` NULL + `psxg_gated=true`, not dropped) — so `shots_faced_total` and coverage are computable downstream.
- **Columns** (pinned types in §10's sibling discipline): identity (`match_key`, shooting `player_key`, **defending-GK `player_key`** per C5, `action_id`, `data_source`); inputs raw + normalized (`crossing_y/z`, `y_norm`, `z_norm`); `psxg` (model raw) + `psxg_recalibrated` (both NULL when `psxg_gated`); `psxg_gated` (bool), `psxg_calibration` (`none`\|`platt`); provenance (`psxg_input_source` ∈ {`statsbomb_freeze_frame`,`tracking_trajectory`}, `shot_crossing_confidence`, `shot_fit_rmse`, `model_version`, `platt_version`, `normalization_version`); outcome `is_goal`.
- **Goal-outcome source (verified):** tracking → `fct_action_values.action_result='success'` (`result_id` is *not* emitted in gold); StatsBomb → `fct_shots.is_goal`.
- **Defending-GK attribution (C5 — real logic, per provider):** tracking → `fct_action_context.defending_gk_player_key` (conformed FK — join on this; `defending_gk_player_id_native` also available; **note: there is no `defending_gk_player_id` column — D1 fix**). StatsBomb → the defending team's GK from the match lineup (currently implicit in `fct_goalkeeper_stats`'s GK-match join); port it explicitly to shot grain and test both. Handle in-match GK substitution as a known edge case.

### D-B — `fct_gk_shot_stopping`: GK-grain aggregate (replaces the inlined CTE)
**Grain:** one row per `(player_key, match_key)`. Aggregates `fct_shot_psxg` over shots *faced* (GK team ≠ shooter team). **Replaces** the `psxg_agg` CTE in `fct_goalkeeper_stats` and the v1 patch idea. Exact column contract pinned in **§10** (downstream depends on it — Hyrum). Note: `goals_conceded_on_shots` here ≠ event-side `fct_goalkeeper_stats.goals_conceded` (own goals/all goals) — named distinctly to prevent conflation.

**Gating-bias guard (B3 — important).** D-F drops shots whose trajectory fails the confidence gate; a *conceded* goal that's gated out vanishes from **both** `psxg_faced` and `goals_conceded_on_shots`, so `goals_prevented` is computed over a *gated subset* — and the drops are **non-random** (high `shot_fit_rmse` correlates with chaotic/deflected/spectacular chances, i.e. exactly the hard goals). A GK can look better merely because their messiest conceded shots were gated out. Mitigation: store `shots_faced_total` (all on-target faced, pre-gate) alongside `shots_faced` (scored, post-gate) + a `coverage_pct`, so the page states "goals-prevented over N of M shots faced"; **log gate drops per-GK**, not just a global count (D-F).

### D-C — model: retrain-on-corrected + **out-of-sample** calibration (B1+B2+C2)
1. Retrain StatsBomb logistic on the D-0 on-target set.
2. Apply to rescaled tracking inputs (D-E port).
3. **Calibration (B2 — out-of-sample):** measure reliability + Brier via **k-fold CV** over the 285 tracking shots (never in-sample — the xT-3 leak class); fit the final Platt on all 285; **report the CV Brier**, not in-sample. Store Platt params (F).
4. **Per-provider (C2):** report reliability per provider (GS/SC/IDSSE) even though only a pooled Platt is fittable; **defer** provider-as-covariate (n=37/13 too small to fit). SC/IDSSE per-GK numbers publish only with a band (§10 `low_sample`).
5. **`psxg_recalibrated` is defined per modality (C2):** StatsBomb rows = the **raw** retrained-model output (already calibrated on its own population — no Platt); tracking rows = **Platt-corrected**. So `psxg_faced = Σ psxg_recalibrated` is well-defined and a reader never assumes Platt was applied to StatsBomb. A `psxg_calibration` provenance flag (`none` | `platt`) records which.
- *Rejected:* combined/tracking-only retrain (n too small); reuse-as-is (D-0).

### D-D — StatsBomb consolidation: 2-hop bridge, phased (C4, Q3)
The bridge is **2 hops** (C4, verified): `psxg.event_id (= shot_id MD5 surrogate) → fct_shots → native event_id → fct_action_values.original_event_id → action_id`. `fct_shots` does **not** currently emit the native `event_id` (`fct_shots.sql:49` surrogates it away). **Fix: add native `event_id` to `fct_shots`** (cleaner than recomputing dbt's `generate_surrogate_key` MD5 in another model). `fct_shots` is contract-enforced + `on_schema_change='append_new_columns'` (`fct_shots.sql:6`) and likely synced — the column add is additive, but the plan must confirm the contract test and any synced consumer absorb it cleanly (full-refresh vs append) (E). **1:1 cardinality (C4):** assert one StatsBomb shot resolves to exactly one `action_id` (no SPADL-conversion fan-out), else `fct_shot_psxg`'s grain silently doubles for StatsBomb — a contract test, not just resolution coverage. **Single effort (user decision 2026-06-21, supersedes the earlier "phased Q3=i"):** StatsBomb consolidation is **in-scope for this delivery**, not deferred — the goal is one all-provider source of truth at first ship. The ordered stages (tracking subsystem → StatsBomb consolidation) remain as **internal review gates**, and the risk controls are retained: the **attribution-parity-then-value-parity** guard (reconcile the new shot-grain lineup attribution to legacy's *before* comparing aggregates, so the value-parity guard isolates the psxg change) + comparison to the **re-baselined** (post-D-0-retrain) legacy `psxg_agg`.

### D-E — normalization: ONE pure domain port (D-hex)
Add a pure function beside `_normalise_goalmouth` that funnels **both** StatsBomb and tracking inputs through one normalization, deriving 7.32 from the existing `_GOAL_Z_MAX=8.0` × yard-constant — **no magic numbers (30.34, 7.32) duplicated into a writer/adapter.** Math per §4. Residual: assert post-handedness (see test plan).

### D-F — confidence gating is a FLAG, not a row-drop
Gate on `shot_crossing_confidence ≥ τ_c` ∧ `shot_fit_rmse ≤ τ_r` (τ from the 285-shot distribution; SC's median RMSE 0.471 ≫ GS 0.199). A sub-threshold shot is **kept in `fct_shot_psxg` with `psxg`/`psxg_recalibrated` NULL and `psxg_gated = true`** — *not* dropped. This is what makes B3's coverage computable: `fct_gk_shot_stopping` counts all on-target rows as `shots_faced_total` and the gate-passed (`psxg_gated = false`) subset as `shots_faced`. Only genuinely non-applicable rows are excluded from the fact (e.g. the 2 `yellow_card` GS rows) — **dropped explicitly with a logged count** (no silent caps). Per-GK gate-drop counts logged.

### D-G — restricted publishing (GS)
GS predictions split via `split_restricted` → private companion repo (ADR-049); SC/IDSSE public. Multi-provider published dataset uses ADR-054 per-provider configs; HF card parity per ADR-014.

## 5.5 Kimball conformance (best-practice, long-term)

Verified the warehouse's conformed dimensions: **`dim_matches`, `dim_players`, `dim_teams`, `dim_competitions`** (no `dim_date`/`dim_season` — time is degenerate, competition/season resolved through `dim_matches`, per ADR-011). Both new marts conform to this, matching `fct_shots` / `fct_action_context` — they do **not** fork parallel keys.

**Fact-type classification (declare the grain + type explicitly — Kimball rule 1):**
- **`fct_shot_psxg` = atomic transaction-grain fact** (one row per on-target shot). The source of truth.
- **`fct_gk_shot_stopping` = derived aggregate fact** (one row per `(player_key, match_key)`) — a rollup of the atomic fact, not an independent source.
- The **season/tournament comparison layer = a further additive aggregate** of the GK-match fact (Kimball aggregate navigation). Because all its measures are additive (below), it should be a dbt **aggregate over `fct_gk_shot_stopping`**, not a third hand-maintained mart — derive, don't duplicate.

**Dimension roles:**
- **Conformed FKs** resolved in the mart per ADR-013 (writers emit native ids; surrogates resolve here): `match_key`→`dim_matches`, shooter `player_key`→`dim_players`, `team_key`→`dim_teams`. `competition_key` (→`dim_competitions`) + `season_id` denormalized via the `dim_matches` join (same as `fct_shots`/`fct_player_percentiles`, for the pooled-percentile partition).
- **Role-playing dimension:** `dim_players` is referenced twice — shooter `player_key` **and** `defending_gk_player_key`. Name it as a role-play (two FKs, one conformed dim), not two dims.
- **Degenerate dimension:** the shot identity `action_id` lives in the fact (no attributes of its own).
- **Unknown-member handling:** unresolved `defending_gk_player_key` (GK substitution / no lineup match) → follow the repo's existing convention (left-join NULL, or the `UNKNOWN_*_SENTINEL` member pattern) — pick one and document; do not leave an undocumented NULL FK.

**Measure additivity (the discipline that makes rollups correct):**
- **Fully additive** (sum across GK, match, competition, season): `shots_faced`, `psxg_faced` (Σ expected goals), `goals_conceded_on_shots`, `goals_prevented` (= difference of two additives → additive), and at shot grain `is_goal`, `psxg`/`psxg_recalibrated` (expected-goal contributions).
- **Non-additive — NEVER stored pre-aggregated; derived at query/presentation from additive components:** `save_pct`, `goal_rate`, psxg-per-shot. Storing an averaged ratio breaks rollup (Simpson's paradox across matches). This is *why* the season layer can be a pure SUM aggregate.

## 6. Plan (test-first — each step opens with a failing test)

1. ~~Feasibility/label check~~ **DONE** (§3): goal label, goal rates, normalization, on-target contamination all verified.
2. **Normalization port** (D-E) — *test:* SB crossbar `z/8=0.334` == tracking `2.44/7.32=0.333`; **handedness fixture** (a known near-/far-post shot must not mirror). Then implement the shared port.
3. **Correct + republish the training dataset, then retrain (B2 — ordered, ADR-012).**
   a. Fix the on-target filter in `export_shots_on_target.py:123` + `publish_shots_on_target_hf.py:78` to `shot_outcome IN ('Goal','Saved','Post','Saved to Post')`; republish `statsbomb-shots-on-target`. *test:* republished set excludes `Off T`/`Blocked`/`Wayward`/`Saved Off Target`; goal rate ≈ 29.9%.
   b. Retrain the logistic on the corrected set; **bump `model_version`**; deliver via ADR-012 (`require_mlflow_env` / `set_and_verify_mlflow_champion` / `upload_weights_to_uc_volume`).
   c. **AI-governance (B1a, gating):** update `AI_GOVERNANCE.md` §5 + `psxg-model.md` (incl. the **"2.44 m" → "÷8 StatsBomb units ≈7.32 m" fix**) + `governance:` YAML; `uv run pytest src/tests/test_ai_governance_md.py`.
   d. **Refresh published artifacts + re-baseline** (B1b): note the intentional value change to live `goals_prevented`/percentiles/psxg HF dataset.
4. **Tracking PSxG writer** (ADR-013/012) — *test:* gating drops logged; output keyed `(match_key, action_id, data_source)`. Score → `bronze.psxg_tracking_predictions`.
5. **Calibration** (B2) — *test:* CV harness produces out-of-sample Brier; Platt params persisted.
6. **`fct_shot_psxg`** (D-A) — *test:* grain uniqueness (1 row/shot); defending-GK attribution per provider; contract `enforced: true`.
7. **`fct_gk_shot_stopping`** (D-B/§10) — *test:* `(player_key, match_key)` uniqueness; `low_sample` flag fires below the shots-faced floor; **coverage measures present** (`shots_faced_total` ≥ `shots_faced`; per-GK gate-drop logged) (B3).
8. **e2e golden** (D) — one fixture GS match flowed writer→bronze→staging→`fct_shot_psxg`→`fct_gk_shot_stopping`, asserting a known GK's `psxg_faced`/`goals_prevented`/`coverage_pct` within tolerance; **plus assert multi-provider presence** (≥1 SC and ≥1 IDSSE row flow through the provider-as-column path), not GS alone (mirrors `test_frame_orientation_golden`).
9. **`fct_shots` native `event_id`** + Phase-2 StatsBomb consolidation under the parity guard + the 1:1 bridge-cardinality test (D-D/C4).
10. **GK percentiles / pooled comparison layer** — pin + build the pooled (season/tournament) layer per §10 (C3): columns, shots-faced floor, band methodology — *before* the downstream page builds on it, not GK×match.
11. **Publishing** (D-G) + HF cards.
12. **UX** — page reads `fct_gk_shot_stopping`; provider/modality + Metrica/StatsBomb gaps + `low_sample` band surfaced (no silent substitution).

## 7. Test plan

- Unit: normalization port (crossbar parity); **handedness fixture** (correctness risk, not a "low-risk note"); gating thresholds + drop logging.
- Population: StatsBomb retrain excludes off-target; goal rate ≈ 29.6%.
- Calibration: **out-of-sample** CV Brier vs mean-rate baseline; per-provider reliability.
- Contract: staging↔DDL parity (`psxg_tracking_predictions`); `fct_shot_psxg` + `fct_gk_shot_stopping` + pooled-layer contracts enforced; **full 2-hop StatsBomb bridge** resolution **+ 1:1 cardinality** (C4 — one shot → one `action_id`, no fan-out); `fct_shots` `event_id` add absorbed by the contract test + synced consumer (E).
- Grain uniqueness: shot grain; `(player_key, match_key)` grain.
- Coverage/selection-bias (B3): `shots_faced_total ≥ shots_faced`; per-GK gate-drop count emitted; `coverage_pct` present.
- AI-governance (B1a): `test_ai_governance_md.py` green after the model-card + `AI_GOVERNANCE.md` + YAML updates.
- Parity guard (D-D Phase 2): new StatsBomb `goals_prevented` == re-baselined (post-retrain) legacy `psxg_agg`.
- Defending-GK attribution per provider (C5).
- e2e golden chain (step 8) — GS values + SC/IDSSE presence + coverage assertion.
- Restricted-publishing lockstep (ADR-049); coverage sentinel (GS/SC/IDSSE present, metrica absent).

## 8. Risks & open questions

- **B1/B2 — now DECISIONS** (D-0, D-C), not open.
- **R1 (residual calibration)** — even after the D-0 retrain aligns populations, trajectory-fit vs annotated coords differ; the CV Platt handles residual. Watch SC (high RMSE).
- **R2 (volume / small-n)** — GS 235 is the only standalone-robust cohort; SC 37 / IDSSE 13 publish only behind `low_sample` + band. Hard contract in §10.
- **C1 framing** — gold-standard *architecture*, provisional *data* (noisy for years). Stated.
- **Q3 — RESOLVED: single effort** (user 2026-06-21; supersedes phased-i). StatsBomb consolidation is in-scope for this delivery; ordered stages kept as internal review gates with the attribution-then-value parity guard (D-D).
- **Q4 — RESOLVED toward pooled** — atomic facts at shot + `(GK,match)`; the percentile/comparison layer pooled (season/tournament) with a shots-faced floor (C3).
- **Q5 — DEFERRED (explicitly approved, unification-ready)** — user 2026-06-21, after investigation: **no clean in-scope path.** `fct_shots` and `fct_shot_psxg` live in different key universes (`shot_id`=surrogate(event_id,data_source) vs SPADL `(match_key, action_id)`) and populations (all event shots, StatsBomb+Wyscout only vs on-target, tracking+StatsBomb); `fct_shots` carries no `action_id` and has **5 xG-consumer marts + 2 tests**. Unifying now = re-key `fct_shots` to SPADL + bridge all providers (incl. Wyscout) + add tracking shots + migrate consumers = a full xG-subsystem refactor whose transitional state is the one-off/awkward table to avoid. **Mitigation:** `fct_shot_psxg` is built on the universal `(match_key, action_id)` key, so the unified shot fact is a clean *future* spec, not blocked. Not pulled in.
- **Richer PSxG model + per-provider calibration covariate — DEFERRED (explicitly approved)** — user 2026-06-21. Keep the 2-feature placement-only logistic (`shot_speed`/GK-position/pressure remain reserved) and the single pooled Platt fit (provider covariate unfittable at SC n=37 / IDSSE n=13).
- **`Saved Off Target` — RESOLVED: excluded** (C1, D-0). Residual precision item: confirm tracking post-hit handling parallels StatsBomb `Post`/`Saved to Post`.
- **Shots-faced floor + band methodology — RESOLVED, data-grounded (§10):** verified max GK faced 14 on-target shots → percentile **deferred** (`ranking_enabled` gate at ≥20 GKs × ≥20 shots, currently false), pooled surface = raw `goals_prevented` ± bootstrap CI, show-value floor 5. Exact UX values confirmed by the GK-page session; structure/method/gate pinned.
- **Known limitation (E):** PSxG is placement-only (the 2-feature model is a non-goal to change here); `shot_speed` is carried on `fct_shot_psxg` but **reserved for a future model rev**, not consumed.

## 9. Key references (files)

- `dbt_project/models/marts/fct_action_context.sql` — tracking `shot_crossing_y/z`, `shot_on_target_derived`; grain `(match_key, action_id)`. **Architectural template.**
- `dbt_project/models/marts/fct_action_values.sql` — gold goal label `action_result='success'` (`result_id` read-only in CTE `:57`); `original_event_id` (`:45/257/406`, 100% populated). Action-grain GK-distribution context (`gk_role`, `gk_xt_delta`, `gk_was_distributing`, `is_launch`, `gk_pass_length_m`).
- `dbt_project/models/marts/fct_shots.sql:49` — `shot_id = generate_surrogate_key(['event_id','data_source'])`; **native `event_id` not emitted** (D-D adds it). `is_goal` `:89`.
- `dbt_project/models/marts/fct_goalkeeper_stats.sql:266-298,378` — inlined `psxg_agg`/`goals_prevented` (to be extracted, D-D).
- `scripts/publish_shots_on_target_hf.py:72-78`, `src/ingestion/export_shots_on_target.py:123` — the `z IS NOT NULL` (off-target-contaminated) training filter (D-0 fixes).
- `src/analytics/goalkeeper.py:277-293` — `_normalise_goalmouth` (`(y−36)/8`, `z/8`); `PSxGModel`/`train_psxg_model`/`predict_psxg`.

## 10. `fct_gk_shot_stopping` column contract (pinned — downstream depends on this, Hyrum's Law / E)

| column | type | unit / domain | additivity | notes |
|---|---|---|---|---|
| `gk_shot_stopping_id` | string | surrogate | key | `generate_surrogate_key(['player_key','match_key'])` |
| `player_key` | bigint | dim_players FK | — | the goalkeeper (role-playing dim) |
| `match_key` | bigint | dim_matches FK | — | grain-defining |
| `competition_key` | bigint | dim_competitions FK | — | denormalized via dim_matches (pooled-percentile partition) |
| `season_id` | int | degenerate | — | denormalized via dim_matches |
| `data_source` | string | provider | — | |
| `shots_faced` | int | count | **additive** | on-target shots faced, post-gate (scored set; D-0 definition) |
| `shots_faced_total` | int | count | **additive** | all on-target shots faced, **pre-gate** (B3) — `≥ shots_faced` |
| `goals_conceded_on_shots` | int | count | **additive** | goals from **gated** shots faced — **NOT** own goals / all goals (≠ `fct_goalkeeper_stats.goals_conceded`) |
| `psxg_faced` | double | expected goals | **additive** | Σ `psxg_recalibrated` over the gated shots faced |
| `goals_prevented` | double | goals | **additive** | `psxg_faced − goals_conceded_on_shots`; +ve = better than expected |
| `low_sample` | boolean | flag | — | true when `shots_faced_total` < floor (§ pooled-layer) — page shows a band, not a point |
| `psxg_calibration` | string | `none`\|`platt` | — | which calibration produced `psxg_recalibrated` (StatsBomb=`none`, tracking=`platt`) (C2) |
| `model_version` / `platt_version` / `normalization_version` | string | provenance | — | full reproducibility of `psxg_recalibrated` |
| `_loaded_at` | timestamp | UTC | — | audit |

**Deliberately NOT stored** (non-additive — derive at query/presentation from the additive components): `save_pct`, `goal_rate`, `psxg_per_shot`, `coverage_pct` (= `shots_faced / shots_faced_total`). Storing pre-averaged ratios breaks the season rollup.

**B2 honesty constraint (pin in the contract notes):** the per-`(GK,match)` grain is a **drill-down detail, NOT an evaluative number** — at avg 3.56 on-target shots faced per GK-competition (~1/match), match-level goals-prevented is noise. The only defensible evaluative surface is the pooled layer below — and even that is thin.

**B3 invariant (pin):** `goals_conceded_on_shots` counts goals among **gate-passed** shots only (`psxg_gated = false`), the same set `psxg_faced` sums over — so `goals_prevented = psxg_faced − goals_conceded_on_shots` never mixes denominators. A gated-out conceded goal is excluded from *both* terms and surfaces only via `coverage_pct = shots_faced / shots_faced_total`. Golden-asserted (§7).

### Pooled comparison layer (C3 + B1 — the page surface; data-shaped, pin before the GK-page session builds)

A dbt **additive SUM aggregate** of `fct_gk_shot_stopping` over `(player_key, competition_key, season_id)` (+ tournament/all-time variant). **Reframed by the verified volume (§3): no GK has faced >14 on-target tracking shots (0 ≥30, 4 ≥10, 25 ≥5).** So a `percent_rank` leaderboard is **not viable** and is **deferred** — built as architecture-present-but-gated, not shipped empty.

| column | type | additivity | notes |
|---|---|---|---|
| `player_key` / `competition_key` / `season_id` | bigint/int | — | pooled grain (conformed) |
| `shots_faced` / `shots_faced_total` / `goals_conceded_on_shots` / `psxg_faced` / `goals_prevented` | counts/goals | **additive** | pure SUM rollup of the per-match fact |
| `goals_prevented_ci_low` / `goals_prevented_ci_high` | double | non-additive | **primary surface** — raw value + band, always shown together |
| `coverage_pct` | double | non-additive (derive) | `shots_faced / shots_faced_total` |
| `goals_prevented_pctile` | double \| NULL | non-additive | **NULL until `ranking_enabled`** (deferred — see below) |
| `ranking_enabled` | boolean | — | true only when **≥ 20 GKs** in the cohort clear a **≥ 20 shots-faced** floor; **currently false everywhere** (verified) |
| `low_sample` | boolean | — | `shots_faced_total < 5` — below the minimum to show even a raw value |

- **Presentation (honest read of spec C1):** show **`goals_prevented` ± CI**, never a rank, while `ranking_enabled = false`. The page must not render a "GK shot-stopping ranking" until volume grows. **Floors:** show-a-value `≥ 5` (`low_sample` below); enable-ranking `≥ 20 GKs × ≥ 20 shots`. (Exact values are a UX requirement the GK-page session confirms; these are the data-grounded defaults — the prior "30" was empirically empty.)
- **Band methodology (A1 — closed-form, NOT bootstrap):** `goals_prevented = Σpsxgᵢ − ΣYᵢ` with `Yᵢ ~ Bernoulli(psxgᵢ)`, so under "GK saves at expectation" the exact variance is **Poisson-binomial: `Var = Σ psxgᵢ(1−psxgᵢ)`** → band `±z·√Var` (z=1.96). Bootstrap is **rejected** — at n≤14 it's degenerate (resamples nonexistent variation → false precision). The closed form is exact at small n and answers the page's question directly ("is this GK distinguishable from average?"). (Beta-Binomial save-rate posterior is an acceptable equivalent.) At n≤14 the band typically straddles 0 — that *is* the honest signal. One shared method, not per-page.
- **`low_sample` is the norm (C2):** with max 14 / avg 3.6 and the show-value floor at 5, **~69% of GKs (55/80) are flagged `low_sample`** — the page's default presentation is value + wide band + low-sample for most GKs; do not treat it as an edge case.
- **Acceptance gate (non-emptiness):** the build asserts "either `ranking_enabled` is true for ≥1 cohort, or the layer is explicitly flagged not-yet-ranked" — so a silently empty percentile mart can never ship.
