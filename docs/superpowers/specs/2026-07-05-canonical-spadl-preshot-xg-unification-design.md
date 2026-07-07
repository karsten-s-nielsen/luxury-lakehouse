# Canonical-SPADL Pre-Shot xG — Unified All-Provider Model & `fct_shot_xg`

- **Date:** 2026-07-05 · **Status:** DRAFT v3 (brainstorming design, pre-plan) — incorporates two rounds of silly-kicks consumer review
- **v3 review response (2026-07-05, silly-kicks review round 2) — the SYNTHESIS (user-approved):** verified m1 (the zero-context/tabular-only score is a **trained** prediction — `train_xg_v2_hf.py:256` appends an empty `(0,4)` array for freeze-frame-less shots, so Wyscout/non-360-SB are in training; Wyscout's 0-coverage is a *writer* skip, not a training gap). Adopted the **synthesis of R1 + R2**, not either alone:
  - **Tabular-only (zero-context) scoring is the certified baseline floor** (R1) — robust ~v1-quality (AUC ~0.82), no freeze-frame set-distribution risk, and the immediate fallback.
  - **Context-aware xG trained *on tracking* (R2)** is the canonical target — the 1,588 GS/SC full-22 freeze frames go **into the v3 training set** (GroupKFold-by-match holdout), so B2 is fixed *at the source* (the model learns the full-22 sum regime instead of meeting it OOD). This moves `build_tracking_snapshots` (C1) **before** the Phase-0 retrain.
  - **A discrimination gate decides which mode ships per provider** — context-aware ships only if it beats the tabular baseline on OOS discrimination; else tabular ships. One model, two scoring modes (freeze-frame vs zero-context), gate picks.
  - **R3:** prefer **sum + explicit set-cardinality feature** over mean (mean discards defender-density signal, which is xG-predictive); Option A (reconstruct broadcast visibility from full tracking) noted as ill-defined.
  - **R4:** discrimination floor is **relative to StatsBomb OOS AUC** (− margin) + an absolute backstop.
  - m2 (parity scoped to shared components), m3 (per-row CI on the tabular path). **Rationale:** geometry-only leaves the tracking providers' richest asset (full 22-player configuration) unused; the freeze frame is *why xG v2 exists* (+0.09 AUC). The long-term-best model uses the tracking context — done correctly (trained on it), with a robust floor and an honest gate.
- **v2 review response (round 1):** verified B2 (sum aggregation, `set_encoder.py:104`) + B3 (writer `continue`→NaN, `xg_model_v2.py:255-259`). Incorporated B1 (discrimination gates), B2 (freeze-frame set-distribution risk), B3 (zero-context path is a real deliverable), M1–M4, m1–m5. M5 (interim StatsBomb-input adapter) **not adopted** — re-couples the model input to StatsBomb units (§7.1).
- **Author:** Karsten Skyt Nielsen (with Claude)
- **Trigger:** [silly-kicks request 2026-07-05](#appendix-a--originating-request) — xT-GK v2 SP1 needs a calibrated **pre-shot** xG reward on every shot in two tracking cohorts (Gradient Sports WC2022 ≈ 1,363 shots / 64 matches; SkillCorner Real Madrid ≈ 225 shots / 10 matches). It blocks the SP1 go/no-go gate.
- **Scope decision (user, 2026-07-05):** **all three phases in one spec** — retrain + tracking cohorts + StatsBomb/Wyscout consolidation, one pre-shot xG source of truth at first ship.
- **Two firm user directives shaping every decision:**
  1. **Canonical geometry = SPADL 105×68 m. Never bend any provider's data — or a model — to StatsBomb-specific units.** The fix for a StatsBomb-trained model scoring tracking providers is to make the model *coordinate-native to canonical SPADL*, not to rescale providers into StatsBomb yards.
  2. **Keep xG separate from the feature mart** — a governed model prediction is an ADR-013 inference output with its own lifecycle, not a column bolted onto `fct_action_context`. Reuse is *shared code*, not a *shared pipeline run*.

---

## 1. Problem

The v2 xG model (Deep Sets set encoder + MC dropout, `xg_model_v2@Champion`) produces a well-calibrated **pre-shot** xG — but **only for StatsBomb-360 shots** (well-calibrated: mean xG 0.099 vs goal rate 0.111). It is materialized in `fct_xg_predictions_v2`, keyed `shot_id`, joined off `fct_shots` — a mart that structurally contains only `statsbomb` + `wyscout` (hard `data_source IN ('statsbomb','wyscout')` contract).

> **Correction (v2 review B3):** the current writer produces **no** value for a shot without a freeze frame — `xg_model_v2.py:255-259` `continue`s and leaves `xg_set_encoder=NaN` (init `:243`). Live probe: **Wyscout coverage = 0/43,075.** So there is *no* materialized "tabular-only Wyscout" today, even though the model was trained on zero-context Wyscout shots (the *capability* exists; the *writer* never invokes it). The zero-context/tabular-only path is therefore a **real deliverable** (§6.1), not an existing fallback.

The two cohorts silly-kicks needs live **entirely on the tracking side** (`gradientsports`, `skillcorner`), as `action_type='shot'` rows in `fct_action_values` / `fct_action_context`, with pressure present and **no xG**. They are absent from `fct_shots` / `int_unified_shots` / `xg_predictions_v2` — there is no `stg_<provider>__shots` for either.

### 1.1 The §7 technical question (silly-kicks asked us to own it)

*Does the v2 model's input feature set exist for GS/SkillCorner shots?* Answer, from code:

- The model has **two inputs** that behave very differently for tracking providers:
  - **Tabular features** — geometry (`distance_to_goal`, `shot_angle`, `location_x/y`, `end_location_x/y`) + categorical qualifiers (`shot_body_part`, `shot_technique`, `shot_type`, `play_pattern`). `build_features()` reindexes to the trained feature list with **`fill_value=0.0`** (`src/analytics/xg_model.py:124`), so **missing qualifiers degrade gracefully to zero one-hot columns** — they do not break scoring. The geometry that dominates xG *does* exist for tracking providers (SPADL `start_x/y`, `end_x/y` in the action stream).
  - **The freeze frame** (player-set context vector) — a **hard requirement**: no freeze frame → `NaN` (`src/ingestion/xg_model_v2.py:255-268`). StatsBomb supplies it as `shot_freeze_frame` JSON. GS/SkillCorner don't have that JSON — **but they are tracking providers**, so the equivalent player set is *constructible from the tracking frame at shot time*.

- **Coordinate coupling is the *easy* problem.** The current model consumes raw StatsBomb-yard geometry (`location_x∈[0,120]`, `distance_to_goal` in yards) and freeze-frame positions normalized `÷120,÷80`. Feeding SPADL meters into that model is wrong (a 18.3 m shot ≠ an 18.3 yard shot). The **wrong** fix is to rescale tracking → StatsBomb yards (short-term coupling to a legacy convention). The **right** fix is to make the model canonical-SPADL-native. The set-encoder *freeze-frame input* is already coordinate-**invariant** under correct normalization — `÷105,÷68` lands on the identical `[0,1]` fractional position `÷120,÷80` does; only the raw tabular geometry is unit-coupled. This is a deterministic rescale — solved by the retrain.

### 1.2 The *lead* modeling risk: freeze-frame set-distribution shift (v2 review B2)

The dominant input to this model is the **freeze-frame set**, not the tabular geometry — the v2 context vector is the set-encoder's whole contribution over the v1 tabular baseline. The model was trained on **StatsBomb-360 freeze frames**, which are a **partial, broadcast-visible** player set of **variable, typically small cardinality** (~a handful of visible players). A tracking frame gives the **full 22-player set, always**.

The set encoder aggregates by **sum** (`set_encoder.py:104`, `context = np.sum(h, axis=0)`) — *not* mean. So a full-22 tracking set produces a context vector of **systematically larger magnitude** (roughly 2–3×) than the partial sets the prediction MLP was trained on. That is a real distribution shift in the model's dominant input, in a regime the MLP never saw. **A per-provider Platt recalibration (a monotone 1–2-parameter transform) cannot correct this at the per-shot level — it only shifts the aggregate level, leaving ranking degraded.** This is the failure mode most likely to make tracking xG *calibrated but non-discriminating* — which for a per-shot *reward* is worthless (a flat V(z,p) gradient). **This, not coordinates, is the risk to lead with.**

**How v3 resolves it (the synthesis):** the risk exists only if the model is trained on SB-360 partial sets and then scored on full-22 tracking sets. So we remove the OOD condition three ways at once:
1. **Fix it at the source (R2):** include the 1,588 GS/SC full-22 freeze frames in the v3 *training* set (GroupKFold-by-match holdout) — the model *learns* the full-22 sum regime rather than meeting it OOD.
2. **Disentangle magnitude from density (R3):** add an explicit **set-cardinality feature** alongside the sum (keeping the density signal the sum encodes, letting the MLP separate "how many" from "aggregate magnitude") — preferred over a naive sum→mean switch, which would *discard* defender-density signal.
3. **Bound the downside (R1):** the **tabular-only (zero-context) score** is a robust, *trained* (verified m1), ~v1-quality baseline (AUC ~0.82) with none of this risk; a **discrimination gate** (§9, R4) ships context-aware *only if* it beats that baseline OOS, per provider, else tabular ships.

V-6 stays as a **diagnostic** (measure the shift) but no longer gates a fragile set-construction convention — training-on-tracking is the principled fix.

---

## 2. Goals / Non-Goals

### Goals

- A **calibrated pre-shot xG** (`xg_set_encoder` + CI) on **every** shot for **all** providers, computed and stored in **canonical SPADL 105×68**, keyed `(match_key, action_id)` so it joins directly onto `fct_action_values` / `fct_action_context`. Tracking providers get the **better of** context-aware (freeze-frame) and tabular-only (zero-context) scoring, decided per provider by an OOS discrimination gate; the shipped mode is recorded in provenance.
- **Per-provider certification evidence** — both **discrimination** (OOS ROC-AUC + Brier-skill vs base rate, GroupKFold-by-match) *and* n-aware **calibration** (reliability + `Σxg` vs `Σgoals`) for GS + SkillCorner, with an explicit **OOD/uncertified** verdict when a provider degrades on *either*. A calibrated-but-non-discriminating reward is worthless (flat V(z,p) gradient) — worse than no cohort (silly-kicks §4.3).
- **One pre-shot xG source of truth** — retire `fct_xg_predictions_v2`, replace with a back-compat view; all providers flow through the same unification → model → gold path.
- **Provenance** on every value: `model_version`, `calibration_version`, `xg_ci_lower/upper`, `ood_flag` (silly-kicks §5).

### Non-Goals

- No read-time / fit-time cross-provider "attach StatsBomb WC2022 xG to GS shots" hack (silly-kicks §6).
- No PSxG / post-shot values as a stand-in (leakage for this reward).
- No goals-only population.
- No rescaling of any provider's data into StatsBomb coordinates, anywhere.
- No embedding of the xG prediction as a column in `fct_action_context`.
- Not building a new evaluative *system* — this is the same xG workflow (`wf-xg-v2`) retrained; no new governance card is minted.

---

## 3. Decision & Architecture

**Retrain the set encoder in canonical SPADL 105×68 space**, **trained on all providers including the tracking cohorts' full-22 freeze frames** (+ a set-cardinality feature), so every provider scores natively in one coordinate system with no OOD set-distribution gap. Land all pre-shot xG in a new unified mart **`fct_shot_xg`** keyed `(match_key, action_id)`. Keep the xG prediction decoupled from the feature mart (ADR-013). Reuse the action-context **frame-linkage code** (not its pipeline run) to build per-shot freeze frames. Tracking providers are scored in **both modes** (context-aware + tabular-only); a per-provider OOS discrimination gate picks the shipped mode.

```
 CANONICAL SPADL 105×68 (m), shooter-attacks-→ normalized, one convention everywhere
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ build_tracking_snapshots  (NEW; generalizes build_sb360_snapshots) — Phase 0   │
 │   reuses silly-kicks link_actions_to_frames + sk_frame_adapters                │
 │   → per-shot player set  [action_id, x, y, is_keeper, is_teammate]  (SPADL)     │
 │      persisted: bronze.shot_freeze_frames  (feature-layer FACT, not prediction) │
 └──────────────────────────────────────────────────────────────────────────────┘
        │  StatsBomb-360 via build_sb360_snapshots; GS/SC via the new builder
        ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ xg_model_v3  TRAINING SET (SPADL-native, + set-cardinality feature):           │
 │   SB-360 partial sets + GS/SC full-22 sets (GroupKFold holdout, R2)            │
 │   + zero-context (Wyscout / non-360 SB) shots  → B2 fixed at the source        │
 └──────────────────────────────────────────────────────────────────────────────┘
        │  set_encoder.py inference core; two scoring modes per shot:
        │    (a) context-aware  = freeze frame → context vector
        │    (b) tabular-only   = zero context  (baseline floor; trained, m1)
        ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ per-provider: DISCRIMINATION GATE (OOS, GroupKFold) picks (a) vs (b)  [R4]     │
 │   + per-provider calibration (Platt) + ood_flag                                │
 └──────────────────────────────────────────────────────────────────────────────┘
        │  ADR-013 writer → bronze.xg_shot_predictions → stg → mart
        ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ fct_shot_xg   (match_key, action_id)   ALL providers                           │
 │   xg_set_encoder, xg_ci_lower/upper, model_version, calibration_version,       │
 │   scoring_mode (context_aware|tabular_only), ood_flag, data_source             │
 └──────────────────────────────────────────────────────────────────────────────┘
        │                                             ▲
        │ joins on (match_key, action_id)             │ back-compat VIEW (bridged to
        ▼                                             │ fct_shots via original_event_id)
   fct_action_values / fct_action_context      fct_xg_predictions_v2  (retired → view)
   (silly-kicks reads xG next to pressure_on_actor__*)
```

### 3.1 Component inventory

| # | Component | Responsibility | Reuse |
|---|---|---|---|
| C1 | **`build_tracking_snapshots`** (`src/analytics/action_context/` — new, beside `sb360_snapshots.py`) — **Phase 0** (feeds training) | For each `action_type='shot'` on a tracking match: link to its frame, emit per-player `[action_id, x, y, is_keeper, is_teammate]` in **SPADL 105×68, shooter-attacks-→ oriented**. Runs before the retrain because the v3 training set consumes the GS/SC freeze frames (R2). | silly-kicks `link_actions_to_frames`; `sk_frame_adapters` / `_convert_tracking_batch`; the `build_sb360_snapshots` pattern. |
| C2 | **Freeze-frame normalization port** (pure fn) | `(player_set, pitch_dims, shooter_attack_dir) → (N,4)` array `[x/105, y/68, is_keeper, is_teammate]`, attack-normalized (point-reflect `x→105−x, y→68−y` for away-attacking shots). ONE convention, used identically in training-export and scoring. | Mirrors `parse_freeze_frame` semantics; coordinate-invariant by construction. |
| C3 | **`bronze.shot_freeze_frames`** | Persisted per-shot player-set artifact (feature-layer fact). Provider-agnostic; reusable by future models. Written by C1 (tracking) + `build_sb360_snapshots` (StatsBomb-360). | ADR-013 bronze conventions; `_ingested_at`; `replaceWhere` per `match_key`. |
| C4 | **`scripts/train_xg_v3_hf.py`** | Retrain set encoder on the SPADL-native representation, **including GS/SC full-22 freeze frames** (GroupKFold-by-match holdout) + **a set-cardinality feature** (R2/R3); deliver via ADR-012 (`require_mlflow_env` / `set_and_verify_mlflow_champion` / `upload_weights_to_uc_volume`). New MLflow model `xg_model_v3`; envelope records `coordinate_system: "spadl_105x68"`. | `set_encoder.py` core; `train_xg_v2_hf.py` structure (already trains zero-context, m1) + ADR-012 hardening. |
| C5 | **Per-provider scoring-mode gate + calibration** | Score each tracking provider in **both modes** (context-aware + tabular-only); pick the shipped mode by **OOS discrimination relative to StatsBomb** (R4). Fit Platt on `(xg_raw, is_goal)` for the shipped mode; OOS via **GroupKFold by `match_key`**; n-aware calibration test (M1). Emit report + `scoring_mode` + `ood_flag`. | PSxG Platt precedent (`goalkeeper.py`); `GroupKFold` harness. |
| C6 | **`ingestion.xg_shot_scorer`** (ADR-013 writer) | Score every shot in the SPADL action stream in the provider's gated mode: freeze frame from `bronze.shot_freeze_frames` (context-aware) or zero context (tabular-only), run `xg_model_v3` + apply provider calibration; **coordinate guard (M3)**; write `bronze.xg_shot_predictions` keyed native ids with `scoring_mode`. | `xg_model_v2.py` writer discipline; `set_encoder.py`; guards/`timed_check`. |
| C7 | **`stg_xg__shot_predictions` + `fct_shot_xg`** | ADR-013 staging view + contract-enforced gold mart, grain `(match_key, action_id)`. | ADR-013 mart pattern; `fct_shot_psxg` precedent. |
| C8 | **`fct_xg_predictions_v2` → back-compat view** | Bridge `fct_shot_xg` → `fct_shots` (via `original_event_id` → `event_id`) to reproduce the old `shot_id`-keyed shape for existing consumers. | PSxG bridge (`fct_shots.event_id` ↔ `fct_action_values.original_event_id`). |

---

## 4. Phase 0 — Build tracking snapshots + retrain the set encoder in canonical SPADL space (governed)

Self-contained governed change. Builds `build_tracking_snapshots` (C1) → `bronze.shot_freeze_frames` for GS/SC (needed because the retrain trains on them, R2), then retrains. Ends with a new SPADL-native champion, OOS-validated (both scoring modes, per provider), governance green. **No consumer mart touched yet.** (C1 spec detail lives in §5.1 for locality; it *executes* here.)

### 4.1 Training data assembly (everyone speaks SPADL from the source)

Training and scoring share **identical** feature construction (hexagonal ideal):

- **Tabular features (SPADL-native):** from the action-stream shot rows — `start_x/y`, `end_x/y`, and derived `distance_to_goal` / `shot_angle` computed in **SPADL meters** (goal at `(105, 34)`, goal width `7.32 m`), plus `period`, `time_seconds`-derived minute, `is_first_time`, the categorical qualifiers as **optional zero-fill one-hots** (present for SB/WS; absent → 0 for tracking), and — new (R3) — a **set-cardinality feature** (number of players in the freeze frame; 0 for zero-context shots) so the MLP can disentangle set count from the sum-aggregated context magnitude.
- **Freeze frames (SPADL-native):** StatsBomb-360 via `build_sb360_snapshots`; **GS/SC via `build_tracking_snapshots` (C1) — the tracking cohorts' full-22 sets are IN the training set (R2), held out via GroupKFold-by-match** so the model learns the full-22 sum regime (fixes B2 at the source). Wyscout / non-360 SB → **zero context**, included in training (as the v2 champion already is — verified m1: `train_xg_v2_hf.py:256`) so the zero-context path is a *trained* prediction, not a degenerate output.
- **Label:** `is_goal` from the SPADL shot outcome.
- **Leakage discipline:** GroupKFold-by-`match_key` across the *whole* mixed training set (SB-360 + tracking + zero-context); the GS/SC OOS discrimination reported in §5 is measured on held-out tracking matches, never a match the model trained on.

#### 4.1.1 Shot population definition (v2 review m4 — bigger than a baseline nudge)

The SPADL action-stream shot set is **not** the StatsBomb shot-event set: SPADL routes **penalties → `shot_penalty`** and **own-goals → `bad_touch`+`owngoal`** (per ADR-018). So retraining on "SPADL `action_type='shot'`" trains on a **different population** than the current `xg-shot-data` model — in direct tension with §4.3's "StatsBomb calibration not regressed." This forces an explicit decision, not an assumption:

- **Define the scored shot family explicitly** — recommendation: `{shot, shot_penalty, shot_freekick}` (penalties are ~0.76-xG shots and silly-kicks needs them as rewards; excluding them silently drops the highest-xG shots and skews calibration). Own-goals are **not** attacking shots → excluded from the shot fact.
- **Penalties may warrant special handling** (near-constant xG; the freeze frame is non-informative). Options: score them through the model, or assign a fixed penalty-xG constant. Decide in the plan; whichever, they must be *present* for silly-kicks.
- **PRE-FLIGHT V-1 (elevated):** report the SPADL shot-family count + goal-rate vs the current `xg-shot-data` population, per shot subtype. The delta drives the "which shots count" decision *before* training. A regressed StatsBomb calibration that is really a population change must be attributed correctly, not absorbed.

### 4.2 Model & delivery

- New MLflow model **`xg_model_v3`** (do **not** overwrite `xg_model_v2@Champion` — avoids a dual-champion footgun during the transition; the old `fct_xg_predictions_v2` pipeline keeps its old model until Phase 2 retires it).
- Same architecture (Deep Sets + MC dropout) — this is a **re-coordination + retrain**, not a new architecture, so no new academic citation (ARCHITECTURE Appendix D unchanged; Deep Sets / MC dropout already cited).
- ADR-012 delivery contract: `require_mlflow_env()`, `set_and_verify_mlflow_champion()`, `upload_weights_to_uc_volume()`; PEP 723 single-file; secrets via `--secrets`; envelope carries `feature_names` + `tabular_dim` (ADR-012 §2).
- Envelope records the coordinate contract explicitly: `coordinate_system: "spadl_105x68"`.

### 4.3 Evaluation & governance (Phase-0 gate)

- **OOS metrics via GroupKFold by `match_key`** (plain k-fold leaks same-match shots): **ROC-AUC, Brier, Brier-skill vs base rate, ECE.** Acceptance: StatsBomb OOS **discrimination not regressed** (AUC ≥ current) *and* calibration not regressed (mean xG ≈ goal rate; Brier ≤ current + small margin). These same discrimination metrics become the GS/SC acceptance gate in §5.3/§9 (B1) — not just the calibration identity.
- Governance (mandatory — `wf-xg-v2` ∈ `PER_PLAYER_EVALUATIVE_CARDS`): update `docs/huggingface/model-cards/xg-v2-model-card.md` (coordinate system → SPADL; all-provider intended use; per-provider calibration section), refresh the eval-metrics/model-index block, update `AI_GOVERNANCE.md` §5 + the `governance:` YAML on `wf-xg-v2.yaml`, refresh **Next review** date, re-run `uv run pytest src/tests/test_ai_governance_md.py -v`. **Evolve `wf-xg-v2` in place** — no inventory add/remove.

> **CHECKPOINT A (review):** new SPADL-native champion, governed. Pause before building marts.

---

## 5. Phase 1 — Score the tracking cohorts (both modes), gate, calibrate → `fct_shot_xg` (GS/SC)

**This is where silly-kicks is unblocked.** `build_tracking_snapshots` (C1) already ran in **Phase 0** (it feeds the retrain); Phase 1 reuses its `bronze.shot_freeze_frames` for scoring.

### 5.1 `build_tracking_snapshots` (C1) + `bronze.shot_freeze_frames` (C3) — built in Phase 0, reused here

- Standalone builder (independent of the AC pipeline run): for each tracking match's `action_type='shot'` actions, `link_actions_to_frames`, take the linked frame's player set (`player_id, team_id, is_goalkeeper, x, y` from `_AC_FRAME_COLUMNS`), derive `is_teammate = (row.team_id == shooter.team_id)`, apply C2 normalization + attack orientation, emit per-player rows. **Full 22-player set** (no ad-hoc visibility filter — see §5.1.1).
- **Orientation:** frames are home-LTR SPADL (ADR-035 geometric backstop). Derive the **shooter's** attack direction from the frame's `team_attacking_direction` (or shooter-team vs home), and normalize so the shooter always attacks toward high-x. Period-5/PSO never oriented (consistent with AC invariants).
- Persist to `bronze.shot_freeze_frames` (`match_key`, `action_id`, `data_source`, player rows, `_ingested_at`; `replaceWhere` per `match_key`).

#### 5.1.1 Set-distribution handling — resolved by training, not a fragile convention (R2/R3)

The B2 concern (encoder sums over the set, so a full-22 tracking set has ~2–3× the context magnitude of an SB-360 partial set) is **fixed at the source**, not by a scoring-time filter:

- **R2 (primary):** the GS/SC full-22 sets are **in the v3 training set** (GroupKFold-by-match holdout) — the model *learns* the full-22 regime, so scoring is in-distribution, not OOD.
- **R3 (support):** a **set-cardinality feature** accompanies the sum so the MLP separates count from magnitude — this *keeps* the defender-density signal that the sum encodes (a naive sum→mean switch would *discard* it, potentially costing discrimination).
- **Option A rejected:** "reconstruct SB-360's broadcast-visible subset from full tracking" is **ill-defined** — camera visibility isn't recoverable from tracking; it trades a known, learnable shift for an unprincipled filter. Use the full 22 and let training + the cardinality feature handle it.
- **V-6 is now diagnostic, not gating:** measure the SB-360-vs-tracking set-cardinality/coverage distribution to *document* the shift and inform the training mix — but the discrimination gate (§5.3) is the arbiter, and the tabular-only baseline is the floor.

> **PRE-FLIGHT V-6 (diagnostic):** measure the set-cardinality + spatial-coverage distribution of SB-360 training freeze frames vs the C1 tracking sets. Informs the training mix / balance; no longer gates a scoring-time convention.

> **PRE-FLIGHT V-2:** confirm GS/SC shots resolve a linked frame (non-empty player set) at ≥ the coverage we target (~100% of 1,588). Report and justify any gap (silly-kicks §4.1). Known GS tracking-vs-events `player_id` type mismatch (STRING vs INT) can break carrier/GK resolution — verify `is_goalkeeper` + team resolve on GS frames (silly-kicks 4.27.0 `add_gradientsports_player_ids` fix should apply).
> **PRE-FLIGHT V-3:** confirm the goal label — the SPADL shot's `action_result`/`is_goal` for tracking providers — and its value distribution per cohort.

### 5.2 Golden orientation/coordinate test (regression floor)

- A golden fixture (one GS + one SC match slice) asserting: goalkeeper at **low x in the shooter-attacks-→ frame** for a known shot, `[0,1]` normalization, handedness (near/far-post not mirrored). Mirrors `test_frame_orientation_golden.py`. This is the guard that we never silently reintroduce a coordinate/orientation bug.

### 5.3 Two-mode scoring gate (C5) + writer (C6)

For each tracking provider, `xg_model_v3` is evaluated **both ways** and the shipped mode is chosen by evidence:

- **(a) context-aware** — freeze frame from `bronze.shot_freeze_frames` → context vector.
- **(b) tabular-only** — zero context (the trained baseline, m1; ~v1-quality AUC ~0.82, no set-distribution risk).

**Scoring-mode gate (R1+R4, OOS GroupKFold-by-match, per provider):**
- Ship **(a)** only if its OOS discrimination **beats (b)** *and* clears the relative floor: **GS/SC OOS AUC ≥ (StatsBomb OOS AUC − margin)** with an absolute backstop (≥ 0.65). An absolute 0.65 alone would "certify" a badly-degraded model (a good xG is ~0.78–0.82) — the relative floor is the informative gate.
- If (a) fails to beat (b) or clear the floor → ship **(b)** (tabular-only). silly-kicks is unblocked either way; the freeze-frame path never ships *worse* than the geometry baseline.
- Record the shipped mode in `scoring_mode ∈ {context_aware, tabular_only}` (provenance).

**Writer (C6):** `ingestion.xg_shot_scorer` scores every shot in the provider's gated mode, applies the coordinate guard (§5.5) + provider calibration, writes `bronze.xg_shot_predictions` (native ids + `xg_set_encoder` + CI + `model_version` + `calibration_version` + `scoring_mode` + `ood_flag`).

**Per-provider calibration:** **Platt** default; isotonic (GS n≈1,363) only if it beats Platt on GroupKFold-OOS reliability (m1). **Shipped vs report (m3):** the *shipped* calibrator is fit on **all** labeled shots; the *report* metrics are **GroupKFold-OOS**. State both.

**Per-shot uncertainty (m2/m3):** guarantee `xg_ci_lower/upper` (MC-dropout) is populated **per row** for tracking — **including the tabular-only mode**, where epistemic CI is the main OOD signal silly-kicks has. Lets them down-weight uncertain rewards rather than relying only on the cohort-level `ood_flag`.

**Calibration gate — n-aware (M1):** *not* a fixed 10%. At SC n≈225 (`Σgoals≈25`, Poisson ±~20%) a perfectly-calibrated model violates a fixed 10% by construction. Use a **binomial/Hosmer-Lemeshow test** (or a calibration CI on `Σgoals | Σxg`); flag OOD only when miscalibration exceeds sampling noise.

**Certification (silly-kicks §4.3)** requires **discrimination *and* calibration** — a base-rate predictor that nails `Σxg≈Σgoals` is worthless. Distribution sanity: median ≈ 0.05, p99 < ~0.75, max < 1.0.

**Downstream contract (M1, explicit):** `ood_flag=true ⇒ silly-kicks excludes that cohort` (SP1 runs WC2022-only if SkillCorner is uncertified) — a joint go/no-go surfaced at Checkpoint B.

### 5.4 `fct_shot_xg` (tracking source) (C7)

- ADR-013 staging view `stg_xg__shot_predictions` + gold mart `fct_shot_xg`, grain `(match_key, action_id)`, `contract: enforced: true`, Kimball FKs (`match_key`, `team_key`, `player_key`, `competition_key`) resolved via INNER JOIN to `fct_action_values` / `fct_action_context` on `(match_key, action_id)`.
- **Join contract (silly-kicks §4):** `fct_shot_xg` → `fct_action_values` on `(match_key, action_id)` → `action_id` is the injection key for `xg_column`. Documented + cardinality-tested (1 xG row per shot, no fan-out).

### 5.5 Inference-time coordinate guard (M3)

The scorer asserts input geometry is in the model's declared coordinate system before scoring: read `coordinate_system` from the envelope; **range-check `x∈[0,105], y∈[0,68]`** (with tolerance) and **raise** on violation. This is the guard that stops a `v2`(yards)/`v3`(SPADL) mixup during the dual-model window (§4.2) from silently producing garbage — a wrong-scale input otherwise scores with no error, just wrong xG.

### 5.6 Committed cross-provider end-to-end fixture (M4)

Beyond the orientation golden (§5.2), commit a tiny **e2e** fixture (one GS + one SC match slice): raw shot → freeze frame → score → mart, asserting the xG **lands on the expected `(match_key, action_id)` with 1:1 cardinality** and a sane value. This is the pipeline-level regression floor for the C7/C8 join contract — the analog of `fct_shot_psxg`'s e2e golden — where a silent key-fanout or orientation regression would otherwise reach silly-kicks' consumption undetected.

> **CHECKPOINT B (review):** GS + SC calibrated pre-shot xG live in `fct_shot_xg`, joinable to `action_id`, with calibration evidence. **silly-kicks handoff possible here.** Pause for review.

---

## 6. Phase 2 — Consolidate StatsBomb + Wyscout; retire `fct_xg_predictions_v2`

### 6.1 Zero-context / tabular-only path is a real deliverable (v2 review B3)

The current writer has **no** zero-context path — no freeze frame ⇒ NaN (§1 correction). Wyscout / non-360-SB consolidation therefore requires **building and validating** an explicit tabular-only emission, not carrying it as a solved fallback:

- The scorer must, on a missing freeze frame, **score with the zero context vector** (`encode_player_set` returns zeros for an empty set; the prediction MLP runs) — *emit* the value instead of `continue`-ing.
- `xg_model_v3` training **includes** the zero-context (Wyscout / non-360-SB) shots (as the v2 champion already does — **verified m1**, `train_xg_v2_hf.py:256`) so this path is a *trained* prediction (≈ v1-tabular quality, AUC ~0.82), not an untrained degenerate output. Note the tracking cohorts already de-risk this: their shipped mode may itself be tabular-only (§5.3 gate), exercising this exact path in Phase 1.
- The tabular-only population gets its **own discrimination + calibration evidence** (§5.3 gates), separately reported.
- **Scope guard:** if the zero-context path can't be validated in this cycle, Phase 2 is scoped to **freeze-frame-bearing** shots (StatsBomb-360) and Wyscout / non-360-SB stay explicitly out (documented) rather than shipped uncertified. Phase 1 (GS/SC, real freeze frames) is unaffected either way.

- Extend the C6 scorer to **all** providers from the SPADL action stream (StatsBomb-360 freeze frames via `build_sb360_snapshots` into `bronze.shot_freeze_frames`; Wyscout / non-360 SB via the §6.1 zero-context path). Union into `fct_shot_xg` — provider-as-column, single fact.
- **Bridge + parity:** StatsBomb/Wyscout resolution `fct_shot_xg.(match_key, action_id)` ↔ `fct_action_values.original_event_id` ↔ `fct_shots.event_id` — verify by JOIN (no MD5 recompute), 1:1 cardinality, zero unresolved.
- **`fct_xg_predictions_v2` → back-compat view (C8):** reproduce the `shot_id`-keyed shape from `fct_shot_xg` bridged to `fct_shots`, so existing non-SQL consumers (Taipy shot-map, HF `xg-shots` publisher, `refresh_synced_tables` / `create_indexes`) keep working unchanged (Hyrum's Law). No SQL mart consumes it (verified: the `fct_pausa_values` / `fct_shots` references are ADR-013 doc comments, not joins).
  - **Query-cost note (v2 review m5):** the view turns former point lookups on a materialized table into a **2-hop join** (`fct_shot_xg`→`fct_shots` via `original_event_id`↔`event_id`). V-5 must check whether any consumer is latency-sensitive (the synced Lakebase path in particular); if so, keep `fct_xg_predictions_v2` **materialized** (a table derived from `fct_shot_xg`) rather than a view. Default to view; escalate to materialized only on a measured latency need.
- **User-facing value change:** StatsBomb shot-map xG shifts (new SPADL-native model). Per UX standards ("never silently substitute"), add a methodology caption noting the recalibration as of `model_version`, mirroring PSxG Phase 0.6.

> **CHECKPOINT C (review):** one all-provider pre-shot xG source of truth; `fct_xg_predictions_v2` is a view.

---

## 7. Cross-cutting

### 7.1 The governed retrain sits on silly-kicks' unblock critical path (v2 review M5 — acknowledged)

Phase 0 is a full governed retrain (new champion, model-card/governance updates, OOS gates) and it sits **on the critical path to Checkpoint B** (silly-kicks' unblock). This is a deliberate consequence of two firm directives: "make the model SPADL-native (retrain), not rescale into StatsBomb yards" + "one spec, all three phases." An interim alternative exists — score the *existing* `v2` via an inference-only adapter that scales just the ephemeral model-input vector into the trained (StatsBomb) space, keeping stored provider data canonical. **It is not adopted:** it re-couples the model input to StatsBomb units (the exact coupling the canonical-SPADL directive rejects) and does **not** address the lead risk B2 (v2 still trained on SB-360 partial sets). Recorded eyes-open: if unblock *latency* becomes the binding constraint, this is the lever to revisit — user's call.

- **ADR:** write a new ADR — *Canonical-SPADL pre-shot xG model + unified `fct_shot_xg`* — covering (a) the model's coordinate contract moving to SPADL 105×68, (b) `fct_shot_xg` replacing `fct_xg_predictions_v2`, (c) the reusable `bronze.shot_freeze_frames` artifact, (d) the decoupling rationale (feature mart vs governed prediction). References ADR-013, ADR-012, ADR-035 (frame orientation), ADR-018 (join contracts), ADR-064 (access tier).
- **Restricted publishing (ADR-064/049):** both cohorts are **restricted** providers (GS + Real Madrid SkillCorner private). The internal `fct_shot_xg` mart is the reward delivery — silly-kicks reads gold directly, **no HF publish is on the critical path**. If `fct_shot_xg` / an xG dataset is ever published, GS + restricted-SC rows split to the private companion repo via `split_restricted` + `assert_no_private_leak`; `access_tier` rides per-row from the action stream. Include the leak-guard registration if a publisher is added.
- **Synced tables:** if `fct_shot_xg` is synced to Lakebase, add to `SYNCED_TABLES` + `triggered_synced_marts` (parity-tested), index filtered columns, heal grants via `gh workflow run lakebase-grants.yml`.
- **Performance:** snapshot builder + scorer run on the shot subset (~1,588 for the cohorts; full SB later) — trivial; no benchmark gate. The scorer is decoupled from AC, so retraining re-scores without an AC recompute.
- **Terraform env pins (ADR-046):** if the trainer/scorer changes a serverless env dep, mirror `==` pins + `uv.lock` + terraform together.

---

## 8. Testing strategy (test-first per task)

- **C2 normalization port:** unit — SPADL `÷105,÷68` == StatsBomb `÷120,÷80` for the same fractional position; attack-orientation reflects away-team shots; handedness preserved.
- **C1 snapshot builder:** GS + SC fixtures — non-empty player set per shot, `is_keeper`/`is_teammate` correct, SPADL range `x∈[0,105], y∈[0,68]`.
- **5.2 golden:** GK-at-low-x in shooter-attacks-→ frame (cross-provider regression floor).
- **C5 gate + calibration:** two-mode scoring per provider; **scoring-mode gate** ships the mode that wins OOS discrimination **relative to StatsBomb** (AUC ≥ SB − margin, absolute backstop ≥ 0.65) + beats the tabular baseline; GroupKFold leakage guard (fit ∩ measure = ∅); Platt-default; **n-aware** calibration test (binomial/Hosmer-Lemeshow, not fixed 10%); `ood_flag` fires when discrimination *or* n-aware calibration fails; per-row CI populated (both modes).
- **M2 train/serve parity gate (scoped, m2):** `build_features` (+ the C2 port) is the **single** function called by both `train_xg_v3_hf.py` and `xg_shot_scorer`; parity test asserts **identical feature vectors** from both entry points **for the shared components** (tabular + the C2 seam). The provider-specific freeze-frame builders (`build_sb360_snapshots` vs `build_tracking_snapshots`) differ by design and get correctness tests, *not* cross-builder parity.
- **M3 coordinate guard:** scorer range-checks input geometry (`x∈[0,105], y∈[0,68]`) against the envelope's `coordinate_system` and raises on violation (v2/v3 mixup guard).
- **M4 cross-provider e2e:** committed GS + SC fixture — raw shot → freeze frame → score → mart, asserting xG lands on the expected `(match_key, action_id)`, 1:1 cardinality, sane value.
- **C6 writer:** bronze DDL ↔ staging parity; NaN→NULL; provenance columns present; hard-fail-first UDF semantics with the group key in errors (ADR-002 §5).
- **C7 mart:** grain uniqueness `(match_key, action_id)`; contract enforced; Kimball FK resolution.
- **C8 bridge:** 2-hop resolution + 1:1 cardinality; back-compat view reproduces the old columns.
- **Governance:** `test_ai_governance_md.py` stays green.
- **dbt:** PR CI is parse-only (Thrift unreachable from GH runners); dbt build/test runs only in the daily live job. For merge-time protection of a SQL invariant, assert on the model's SQL text in python-ci — a dbt test is a ≤24h daily-live guard, not a PR gate.

---

## 9. Acceptance criteria (maps to silly-kicks §5)

1. GS + SkillCorner shots resolve a non-null `xg_set_encoder` at ≥ StatsBomb coverage (~100% of 1,588); any gap reported + justified.
2. **Certification requires discrimination *and* calibration (B1/R4):** per-cohort OOS (GroupKFold-by-match) **ROC-AUC ≥ (StatsBomb OOS AUC − margin)** with an absolute backstop (≥ 0.65) and Brier-skill > 0 vs base rate, **and** n-aware calibration (binomial/Hosmer-Lemeshow) within sampling noise (M1 — *not* a fixed 10%). A cohort failing *either* is `ood_flag`/uncertified (loudly). A calibrated-but-non-discriminating cohort does **not** pass.
3. **Scoring mode gated (R1):** each tracking provider ships the mode (context-aware vs tabular-only) that wins OOS discrimination; `scoring_mode` recorded per row. Context-aware never ships *worse* than the tabular baseline. V-6 set-distribution diagnostic attached.
4. Documented, cardinality-tested join `fct_shot_xg.(match_key, action_id)` → `fct_action_values.action_id` for both cohorts, with a committed cross-provider e2e (M4).
5. Per-provider distribution sanity: median ≈ 0.05, p99 < ~0.75, max < 1.0. Per-row `xg_ci_lower/upper` populated for tracking (m2).
6. **Downstream contract explicit (M1):** `ood_flag=true ⇒ silly-kicks excludes that cohort` — a joint go/no-go surfaced at Checkpoint B (SP1 runs WC2022-only if SkillCorner is uncertified).
7. Provenance (`model_version`, `calibration_version`, `scoring_mode`, `xg_ci_lower/upper`, `ood_flag`) present on every row.
8. One pre-shot xG source of truth: `fct_xg_predictions_v2` is a back-compat view (or materialized per m5) over `fct_shot_xg`; all providers flow through one SPADL-native model. Zero-context (Wyscout/non-360-SB) path built + validated, or explicitly scoped out (B3).
9. Train/serve feature parity test (M2) + inference coordinate guard (M3) + governance + coordinate-golden + bridge + restricted-publishing (if applicable) tests green.

---

## 10. Pre-flight verifications (live — do before/early in the plan)

| ID | Verify | Why it matters |
|---|---|---|
| V-1 | SPADL shot-**family** population (`{shot, shot_penalty, shot_freekick}`) count + goal-rate **per subtype** vs current `xg-shot-data` population. | Not a nudge (m4): SPADL routes penalties→`shot_penalty`, own-goals→`bad_touch`+`owngoal`. Drives the "which shots count" decision + the penalty-handling choice *before* training; §4.1.1. |
| V-6 | SB-360 training freeze-frame set cardinality/coverage distribution vs C1 tracking sets (mean/median players, spatial coverage). | **Diagnostic (B2):** documents the set-distribution shift + informs the training mix/balance. No longer gates a scoring-time convention (R2 trains on tracking; R3 cardinality feature). |
| m1 ✓ | **VERIFIED** — v2/v3 trainer includes zero-context shots (`train_xg_v2_hf.py:256` appends empty `(0,4)` array; `:321` handles size-0 sets). | The tabular-only baseline (R1) is a *trained* prediction, not degenerate — the premise the whole synthesis rests on. |
| V-2 | GS/SC shots resolve a linked frame with non-empty player set; `is_goalkeeper` + team resolve on GS frames. | Freeze-frame coverage; GS player-id caveat ([[project-gradientsports-player-id-space-bug]]). |
| V-3 | Goal label field + value for tracking shots (`action_result`/`is_goal`), per-cohort goal rate. | Calibration + acceptance need real outcomes. |
| V-4 | Exact live shot counts per cohort (expect ≈1,363 GS / ≈225 SC). | Coverage target §4.1; isotonic-vs-Platt choice. |
| V-5 | Actual runtime consumers of `fct_xg_predictions_v2` (Taipy, HF publisher, synced/index scripts). | Back-compat view must preserve their observable interface (Hyrum). |

---

## 11. Open questions (for user/plan)

- **O-1:** `fct_shot_xg` naming — confirm `fct_shot_xg` (vs `fct_xg_predictions_v3`). Recommendation: `fct_shot_xg` (provider-agnostic, action-keyed, not version-suffixed).
- **O-2:** Should `bronze.shot_freeze_frames` be produced as a **side-output of the AC pipeline** (needs an AC re-run to backfill) or a **standalone builder** (re-does cheap linkage, no AC touch)? Recommendation (this spec's default): **standalone** — decouples from the painful AC recompute.
- **O-3:** Calibration granularity — single pooled Platt per provider vs pressure-stratified. silly-kicks stratifies the reward by pressure downstream; recommendation: deliver a single well-calibrated per-provider xG (pressure stratification is silly-kicks' concern), keep pressure-covariate calibration as a documented future option.
- **O-4 (resolved in v3):** Set-distribution handling — **decided:** full-22 sets + **sum + set-cardinality feature** (R3) + **tracking shots in v3 training** (R2), with the tabular-only baseline + discrimination gate as the floor. Option A (reconstruct SB-360 visibility) rejected as ill-defined; naive sum→mean rejected (discards density signal). The only remaining tuning knob for the plan: the *training mix/balance* between SB-360, tracking, and zero-context shots (informed by V-6).

---

## Appendix A — Originating request

silly-kicks session, 2026-07-05 — verbatim request retained in the conversation that produced this spec. Key asks: all shots not goals-only (§4.1); pre-shot not post-shot (§4.2); calibrated per-provider with report (§4.3); join to `fct_action_values.action_id` (§4.4); provenance + OOD flag (§5). Out of scope: cross-provider attach hack, PSxG stand-in, goals-only (§6).
