# Canonical-SPADL Pre-Shot xG v3 — Complete Delivery Design

**Status:** Draft (for review) · **Date:** 2026-07-07 · **Supersedes/completes:** `2026-07-05-canonical-spadl-preshot-xg-unification-design.md` (which under-specified the corpus, staging, and scorer, and assumed pieces that were never built).

**Goal:** Deliver **consumable, calibrated pre-shot xG values** — `fct_shot_xg`, one row per shot, joinable to `fct_action_values` on `(match_key, action_id)` — produced by a canonical-SPADL-native model (`xg_model_v3`) trained on **all** shot cohorts (StatsBomb-360 + GS/SkillCorner tracking freeze frames + zero-context), replacing `fct_xg_predictions_v2`.

**Why this spec exists (context):** The v3 effort was executed reactively and repeatedly surfaced "this wasn't done/tested either": the SB-360 freeze frames (the largest context cohort) were never computed or wired; both HF training datasets never existed; the freeze-frame table has no `access_tier`; the trainer joins freeze frames on a non-unique key and would silently train on public-only data; the scorer and mart don't exist. This spec inventories **everything** and designs the complete path once, so implementation is execution, not discovery.

---

## 1. Scope & locked decisions

**In scope (definition of done):** the full path A → B → C below, ending in a populated `fct_shot_xg` gold mart with `fct_xg_predictions_v2` consumers migrated. Nothing is deferred as "future work"; where a piece is large it is a stage of this spec, not out of scope.

**Locked decisions (approved 2026-07-07):**
- **D1 — Privacy: public + restricted-split.** Both training datasets follow the ADR-049/064 pattern: a public repo + a private `<repo>-restricted` companion. RM SkillCorner + GS rows go only to the restricted companion. **The trainer must read BOTH** — this is a first-class correctness requirement (§B2), not optional.
- **D2 — Features: uniform provider-agnostic.** Tabular inputs are only features that exist identically for every provider: canonical SPADL geometry (`distance_to_goal`, `shot_angle`, `location_x`, `location_y`) + `set_cardinality`. No StatsBomb-only qualifiers (body part / technique / play pattern). The freeze-frame player set supplies the context via the set encoder.
- **D3 — Coordinates: canonical SPADL 105×68 everywhere.** Every freeze frame stored in `bronze.shot_freeze_frames` is SPADL 105×68, home-LTR. StatsBomb-360 (raw 120×80) is converted at compute time (§A1). No provider is bent to StatsBomb units; no per-provider normalization in the model.
- **D4 — Shot family:** train + score `action_type ∈ {shot, shot_freekick}`; `shot_penalty` is excluded from the model and assigned a constant penalty-xG at scoring (§C1). Goal label: `action_result == 'success'`.

**Cohort sizes (live, 2026-07-07):** context-bearing — StatsBomb-360 **7,701** shots (323 matches, 15.6M `bronze.statsbomb_360` rows), SkillCorner **2,596**, GradientSports **1,363** (GS/SC already in the store). Zero-context — StatsBomb non-360 ~74.7k, Wyscout ~43k, IDSSE/Metrica ~230. SB-360 is ~2/3 of all context data → non-negotiable.

**Relationship to prior work (m4):** this spec **supersedes the `2026-07-05` design** but **carries forward — does not re-derive** — the already-built, already-merged Phase-0 components and their verified tests: `analytics.xg_calibration` (the n-aware bootstrap/DeLong AUC-CI gate + `select_scoring_mode`/`is_mode_certified`/`calibration_ok_n_aware` — Task 1.1), `analytics.xg_freeze_frame` (C2 port), `analytics.xg_model` (SPADL geometry + `set_cardinality`), the GS/SC `bronze.shot_freeze_frames` store, and `train_xg_v3_hf.py`'s skeleton. The plan built from this spec **amends/extends** the prior plan's task scaffolding rather than starting fresh, so the DeLong-gate + `set_cardinality` + e2e tests are preserved.

**Plan sequencing (M3):** the bundled scope deliberately places §A1 (SB-360 conversion, the highest-risk unit) on the critical path to the downstream unblock (calibrated GS/SC xG), even though that unblock is reachable via the tabular-only + tracking path alone (no SB-360). This is accepted. The plan MAY structure a **GS/SC-inclusive first milestone** (trainer + scorer + `fct_shot_xg` over the already-existing corpus, before the SB-360 consolidation) so the downstream consumer can integrate early while SB-360 lands — a sequencing choice for the plan, not a scope reduction. **If that milestone is taken (N6):** the interim model is trained without SB-360 (~4k GS/SC context shots), so its context-aware mode is weak and the gate will likely ship it **tabular-only** — this is an explicitly **interim, superseded** model, NOT a separately-governed champion. The SB-360 retrain replaces it in place (same `xg_model_v3` name/alias); it must not read as a second governed model or cause champion-churn / a second governance card.

---

## 2. Architecture

```
[A] CORPUS + STAGING
  fct_action_values (shots, all providers, access_tier)         ──► publish_xg_shot_data_v3_hf  ─► HF: xg-shot-data-v3  (+ -restricted)
  bronze.shot_freeze_frames  (GS/SC done + SB-360 NEW, access_tier NEW) ─► publish_shot_freeze_frames_hf ─► HF: xg-shot-freeze-frames (+ -restricted)

[B] TRAINER + MODEL
  train_xg_v3_hf (fixed): read public + -restricted for BOTH datasets
    → join freeze↔shots on (match_key, action_id)
    → uniform features + set encoder → GroupKFold-by-match OOS (both modes, per provider)
    → xg_model_v3 Champion  (ADR-012: MLflow alias + UC Volume; envelope coordinate_system=spadl_105x68; OOF calibrator)

[C] SCORER + MART
  ingestion.xg_shot_scorer (ADR-013): score every shot (shared build_features/normalize_freeze_frame; M2 parity)
    → two-mode gate (analytics.xg_calibration): per-provider mode select + calibrate + ood_flag; penalty=constant
    → bronze.xg_shot_predictions → stg view → fct_shot_xg (contract, key (match_key, action_id))
  fct_xg_predictions_v2 → view over fct_shot_xg / retired (ADR-043 strand-safe if synced)
```

Data flows one direction; each stage has a well-defined output another stage consumes. `(match_key, action_id)` is the shot identity throughout (`action_id` is per-match, NOT global — this is the single most important invariant, §5).

---

## 3. Stage A — Complete training corpus + staging

### A1. StatsBomb-360 freeze frames (the missing cohort)

`build_sb360_snapshots` currently runs only inside the AC pipeline and emits `[action_id, team_id, is_goalkeeper, x, y]` in **StatsBomb 120×80** units with a native `teammate` flag. Nothing writes SB-360 into `bronze.shot_freeze_frames`.

> **Implementation revision (2026-07-08, evidence-based — supersedes step 4 below).** Step 4's separate **home-LTR orientation** step was found **unnecessary and was NOT built**: StatsBomb event + 360 data is already **shooter-normalized** (the attacking team always shoots toward high-x). Live evidence: 99.9% of StatsBomb shots have `start_x ≥ 52.5` in *both* periods (home and away), and for a real away-P2 shot the actor's raw 360 location converts **exactly** (0.000001 m) onto its `fct_action_values.start_x/y` via `_convert_locations` with **no** reorientation. So the SB-360 builder does coordinate conversion ONLY and stamps `shooter_attacks_high_x = True` (constant); `normalize_freeze_frame` reconciles SB-360 (stored shooter-LTR) and tracking (stored home-LTR) to a common shooter-attacks-high-x frame at feature time via that flag — consistent by construction. Forcing home-LTR would have *double-oriented* SB-360. The co-location golden (step A1-golden) is therefore `convert(raw_actor) ≈ fct_action_values.start` (convert-only, non-circular against the independent action pipeline), not "convert + orient". See `src/analytics/action_context/sb360_freeze_frames.py` + `src/tests/action_context/test_sb360_freeze_frames.py`. Reviewer (silly-kicks) re-blessed. Steps 3, 5, 6 below stand as written.

**Design:** add a `statsbomb` branch to `compute_shot_freeze_frames` (`ingestion.shot_freeze_frames`) that, per match, runs coordinate *conversion* (step 3) — **and, per the revision above, NO separate orientation step** (step 4 is superseded). Conversion is kept isolated + separately tested so the y-flip cannot be lost:

1. Loads that match's `bronze.statsbomb_360` freeze-frame rows + its SPADL shot actions.
2. Runs `build_sb360_snapshots(actions, sb360_raw)` → per-(shot, player) rows with `teammate`, `is_goalkeeper`, raw StatsBomb `x/y`.
3. **Coordinate conversion (StatsBomb → SPADL): reuse silly-kicks' own transform — do NOT hand-roll a scale.** The correct transform is `silly_kicks/spadl/statsbomb.py::_convert_locations` (`_convert_locations` / lines ~417-418), which is NOT a bare scale — it applies (a) a **Y-FLIP** (`y = 68 − (y_raw − y_offset)/80*68`, because StatsBomb (1,1) is top-left / top-down while SPADL (0,0) is bottom-left / bottom-up), (b) a **cell-center offset** (`x = (x_raw − crc)/120*105`), and (c) the match's **`fidelity_version`** (cell side 1.0 vs 0.1 — per-match metadata that MUST match the `fidelity_version` the shot *action* was converted with, or frame and action disagree sub-yard). A pure `×105/120, ×68/80` scale omits all three and **vertically mirrors** the freeze frame relative to its own shot — corrupting ~2/3 of the context corpus (the classic y-inversion bug class). Call `_convert_locations` (or an in-repo transform byte-identical to it, threading the same per-match `fidelity_version`) on the freeze-frame points. This step is purely coordinate conversion — it does NOT orient.
4. **Orientation (home-LTR) — a separate step.** After conversion, apply the same per-period home-LTR orientation the shot action carries, so the frame lands in the action's home-LTR frame and `shooter_attacks_high_x` is derived from home/away exactly as the tracking path does. Conversion (step 3) and orientation (step 4) are distinct functions with distinct tests.
5. Derives the `_SHOT_FF_COLUMNS` schema: `is_keeper = is_goalkeeper`; `is_teammate = (player_team == shooter_team)`; `shooter_attacks_high_x` from `home_team_id_native`; `player_id` from the 360 row; `set_cardinality`; `team_attacking_direction` derived from the flag.
6. Writes to `bronze.shot_freeze_frames` with `data_source='statsbomb'`, `access_tier='public'` (§A2), `replaceWhere match_key`.

SB-360 snapshot building is vectorized (JSON parse, no tracking-frame conversion) → fast; 323 matches process without the tracking-style per-match timeout, but the incremental/discovery + resume-run mechanics from GS/SC still apply. Provider set for the SB-360-enabled task run includes `statsbomb`.

**A1 golden (TDD, write FIRST) — anchor on the ACTOR; assert co-location, not SPADL range.** A range check (`x ∈ [0,105]`) passes a mirrored frame; useless here. `build_sb360_snapshots` keeps the StatsBomb `teammate` flag and does NOT strip the actor, so **the shooter is in the SB-360 set** with a known position — the single most direct y-flip detector (a mirrored conversion puts the actor at `68−y`). The golden's **primary** assertion (N1): the converted **actor position ≈ the shot action's `(start_x, start_y)`**. Secondary: the defending goalkeeper's converted position sits near the goal the shooter attacks. Writing this first makes A1 self-correcting.

**This golden is a COMMITTED CI unit test, not only a live gate (N5).** SB-360 is **public** StatsBomb data, so a small real SB-360 shot slice is committable (unlike GS/SC-RM fixtures). Commit the co-location golden as a fast CI test on a real public SB-360 shot **in addition to** the live corpus gate — so the y-flip regression is caught on every PR, not only in the live run.

### A2. `access_tier` on `bronze.shot_freeze_frames`

The table has no `access_tier` today; its providers (GS/SC) are exactly the restricted ones. Add `access_tier STRING` to the DDL + the writer schema (`_SHOT_FF_COLUMNS`, StructType, DDL-parity test), stamped per-row from the match's tier (SB-360 → public; GS/SC per-match). A backfill migration stamps the existing GS/SC rows from `dim_matches`. This lets the freeze-frame publisher split.

### A3. Shots publisher — `scripts/publish_xg_shot_data_v3_hf.py` → `xg-shot-data-v3`

Source `fct_action_values` (has all columns + native `access_tier`), no provider filter in SQL. Columns: `match_key, action_id, action_result, action_type, start_x, start_y, data_source, access_tier`. Split pattern (per `publish_spadl_vaep_hf`): `split_restricted(df, column="access_tier")` → `assert_no_private_leak(public_df, publisher=...)` → drop `access_tier` → publish public repo + private `-restricted` companion (`delete_patterns=["**"]`), `upload_hf_readme` for both. Register in `PUBLISHER_REGISTRY` (mode `"split"`) + `_ADR049_SPLIT_PUBLISHER_CARDS`. In-repo cards `xg-shot-data-v3.md` + `xg-shot-data-v3-restricted.md`.

### A4. Freeze-frame publisher — `scripts/publish_shot_freeze_frames_hf.py` → `xg-shot-freeze-frames`

Source `bronze.shot_freeze_frames` (now carrying `access_tier` + SB-360). Columns: `match_key, action_id, data_source, player_id, x, y, is_keeper, is_teammate, set_cardinality, shooter_attacks_high_x, team_attacking_direction, access_tier`. Same split pattern + registry + cards.

---

## 4. Stage B — Trainer + model (`scripts/train_xg_v3_hf.py`)

- **B1 — key fix.** `parse_freeze_frames_spadl` groups + looks up freeze rows by `action_id` alone. Change to `(match_key, action_id)` (group `freeze_df` by both; iterate shots by both). Read `match_key` from `freeze_df`.
- **B2 — read both repos.** For BOTH datasets, download the public repo AND the `<repo>-restricted` companion and concatenate. If a `-restricted` companion is expected (RM/GS present in the cohort) but missing/empty, **fail loudly** (ERROR, non-zero) — never silently train public-only. Record both commit shas in the model provenance.
- **B3 — uniform features.** `build_spadl_tabular` builds only the D2 feature set; pin `feature_names` order deterministically; the envelope's `feature_names`/`tabular_dim` are the serve-time contract.
- **B4 — calibration is FIT here, APPLIED at scoring (ONE calibration, no double, M1).** The trained model emits **RAW (uncalibrated) xG**. The trainer, using GroupKFold out-of-fold predictions, (a) **fits per-provider OOF calibrators** and ships their parameters in the weight envelope, and (b) produces the per-provider two-mode **OOS discrimination report** (context-aware vs tabular-only, per `data_source`) — evidence only. **The trainer does NOT transform served values.** Stage C is the *only* place a calibrator is applied (§C1). The spec is explicit so no stage double-calibrates: model → raw xG; scorer → apply the single per-provider OOF calibrator once. Use `analytics.xg_calibration` (already built, Task 1.1) for the calibrator fitting + the gate primitives — do not re-derive them.
- **B5 — model card.** Create `docs/huggingface/model-cards/xg-v3-model-card.md` (governed under `wf-xg-v2`, coordinate contract, all-provider, two-mode). `upload_hf_readme` currently references a nonexistent path — this fixes it.
- **B6 — tests.** Add real tests for `parse_freeze_frames_spadl` (incl. the two-matches-share-an-action_id case), the read-both-repos assembly, and the uniform feature contract. Extend fixtures to mirror **live** schemas (the minimal fixtures hid every gap).
- **B7 — retrain.** HF-Jobs GPU run → `xg_model_v3` Champion via ADR-012 (`require_mlflow_env`, `set_and_verify_mlflow_champion`, `upload_weights_to_uc_volume`), secrets via `--secrets`.

---

## 5. Keys, coordinates, and the identity invariant

- **Shot identity is `(match_key, action_id)`.** `action_id` is per-match, not globally unique (live-confirmed: one `action_id` under two `match_key`s). EVERY freeze↔shot join, groupby, dedup, and mart key uses `(match_key, action_id)`. A validation query that groups by `action_id` alone is wrong and must never be used to judge correctness.
- **Coordinates:** all stored freeze frames + all model geometry are canonical SPADL 105×68, home-LTR, goal at `(105, 34)`. `normalize_freeze_frame` divides by 105/68 — so every row reaching it must already be SPADL (SB-360 converted at §A1).
- **Actor-inclusion consistency (N2) — both freeze-frame builders must include the shooter.** The set encoder **sum**-aggregates the player set, so a one-player difference between sources (shooter in one, absent in the other) systematically shifts the summed context magnitude and corrupts train/serve consistency. `build_sb360_snapshots` includes the actor (keeps the `teammate` flag, no actor strip); `build_tracking_snapshots` must too (its linked-frame set keeps the full player set, ball row dropped, so the ball-carrier/shooter is present). This is **pinned by an explicit cross-builder assertion test** (both builders' output for a shot includes a row for the shooter), not assumed — it is exactly the kind of convention that silently drifts.

---

## 6. Stage C — Scorer + mart (consumable xG)

- **C1 — `ingestion.xg_shot_scorer`** (ADR-013 writer, new). Loads `xg_model_v3@Champion` weights (**raw xG**) + the shipped per-provider OOF calibrators; scores every shot in `fct_action_values` using the SHARED `build_features` + `normalize_freeze_frame` (same functions as the trainer — cross-entry-point parity test). Per shot: assemble tabular features + the freeze-frame set (from `bronze.shot_freeze_frames` on `(match_key, action_id)`; empty → zero-context). Then the **two-mode gate**, using `analytics.xg_calibration` (Task 1.1 — carried forward, not re-derived), with **selection and certification kept SEPARATE** (M2):
  - **Mode selection** (`select_scoring_mode`): per provider, pick context-aware vs tabular-only by held-out discrimination.
  - **Certification** (`is_mode_certified`): the selected mode is trusted only if its **n-aware discrimination CI *lower bound*** (bootstrap/DeLong AUC CI — NOT a point estimate) clears the **StatsBomb-relative floor**, AND `calibration_ok_n_aware` passes. Selection ≠ certification: a mode can be selected but not certified.
  - **Calibration (the ONLY one — M1):** apply the single per-provider OOF calibrator to the model's raw xG. No other stage calibrates. **Missing-calibrator fallback (N4):** if a shot's `data_source` has no shipped per-provider calibrator (a provider absent from training, or too few shots to fit one), fall back to the **pooled/global OOF calibrator** and force **`ood_flag = true`** (the value is still calibrated + emitted, but flagged not-provider-certified). This is a defined serve-time contract, never undefined behavior. The trainer ships the pooled calibrator alongside the per-provider ones for exactly this fallback.
  - **`ood_flag`** set when the certified check fails (uncertified discrimination or failed calibration); the xG value is still emitted, flagged.
  - `shot_penalty` → **constant penalty-xG** = the empirical `shot_penalty` conversion (goal) rate computed + logged from the training corpus (≈ 0.76; derived, cited — NOT a hard-coded magic number, m2), bypassing the encoder.
  Emit MC-dropout CI bounds. Writes `bronze.xg_shot_predictions` with native ids + `(match_key, action_id)` + `xg`, `xg_ci_low/high`, `scoring_mode`, `ood_flag`.
- **C2 — dbt.** `stg_xg__shot_predictions` view over the bronze; `fct_shot_xg` gold mart, `contract: enforced: true`, keyed `(match_key, action_id)`, joining identity from `fct_action_values`. `fct_xg_predictions_v2` is redefined as a view over `fct_shot_xg` (or retired) so existing consumers keep working; if it is a TRIGGERED synced mart, follow ADR-043 strand-safe re-derive.

---

## 7. Data contracts (authoritative)

- **`xg-shot-data-v3`** (parquet, per-provider flat files, ADR-054): `match_key BIGINT, action_id BIGINT, action_type STRING, action_result STRING, start_x DOUBLE, start_y DOUBLE, data_source STRING`. (`access_tier` used for the split, dropped before upload.)
- **`xg-shot-freeze-frames`**: `match_key BIGINT, action_id BIGINT, data_source STRING, player_id STRING, x DOUBLE, y DOUBLE, is_keeper INT, is_teammate INT, set_cardinality INT, shooter_attacks_high_x BOOLEAN, team_attacking_direction STRING`. (`access_tier` for split, dropped before upload.)
- **`bronze.shot_freeze_frames`** (updated): the above + `access_tier STRING` + `_ingested_at`.
- **`bronze.xg_shot_predictions`**: native ids + `match_key BIGINT, action_id BIGINT, data_source STRING, xg DOUBLE, xg_ci_low DOUBLE, xg_ci_high DOUBLE, scoring_mode STRING, ood_flag BOOLEAN, _ingested_at`.
- **`fct_shot_xg`** (gold, contract): `match_key, action_id, data_source, xg, xg_ci_low, xg_ci_high, scoring_mode, ood_flag` + Kimball surrogates joined from `fct_action_values`.

---

## 8. Testing & live-data validation strategy (non-negotiable)

The root cause of the firefighting was code tested only against minimal synthetic fixtures. Every stage below has a **mandatory live-data gate that must pass before that stage is called done**:

- **Unit tests** for all pure logic, with fixtures that **mirror the real live schemas** (all columns the live table carries), including the adversarial cases that bit us (cross-match `action_id`, duplicated frames, missing orientation column).
- **A — corpus gate:** after SB-360 compute, validate on real SB-360 matches, **grouped by `(match_key, action_id)`**: `is_teammate` has both classes (~11/11 for tracking; SB-360 per its `teammate` flag), `shooter_attacks_high_x` non-NULL, `set_cardinality` in-range, **no multi-frame fan-out** (rows-per-`(match_key,action_id)` is a single ~frame, never a multiple), and the **A1 co-location golden** (converted SB-360 frame is spatially consistent with its shot action — not mirrored). After staging, verify per-provider published row counts AND that no restricted (RM/GS) row appears in any public repo (the leak guard + a direct check), including a **NULL-`access_tier` fail-safe test** (m1): a row with NULL tier is treated as restricted by `split_restricted`/`assert_no_private_leak` and never appears in a public repo.
- **Cross-mart gate (A/C — the seam downstream depends on):** the **robust integrity check is the key anti-join** — for every tracking-provider shot, `(match_key, action_id)` in `fct_action_values` (the xG source) MUST exist in `fct_action_context` (where pressure/RM has always lived): anti-join count = 0. This guarantees `fct_shot_xg ⋈ fct_action_context` pairs xG and pressure on the *same* physical shot for RM (both marts derive `action_id` from the same `bronze.spadl_actions` SPADL conversion; the anti-join pins they never diverge). We own both marts, so this gate lives here. **The coordinate check is only a SOFT cross-check (N3):** `fct_action_values` and `fct_action_context` may store different orientations (e.g. acting-team-LTR per ADR-028 vs home-LTR), so the same physical shot can have *mirror* coordinates — a raw `(start_x, start_y)` equality would false-fail. If included, the coordinate check must first normalize both marts to the same orientation; otherwise rely on the key anti-join alone. An orientation-convention mismatch must NEVER read as a key-integrity failure.
- **B — trainer gate:** a **subset dry-run of the training-set assembly on the real downloaded datasets** (both public+restricted) before the full GPU run: freeze-frame match-rate per provider, resolved `feature_names`, and a check that no shot's player set contains players from another match (the `(match_key, action_id)` fix, verified on live data). Then the GPU run's GroupKFold OOS per provider + mode.
- **C — scorer gate:** score a real slice, verify `fct_shot_xg` xG distributions are sane per provider (range, goal-rate calibration), the two-mode gate picks expected modes, `ood_flag` behaves, and penalties are the constant. Then full materialization + the standing dbt contract/consistency tests.
- **Wheel/CI discipline:** each deployable change is a proper wheel-bumped PR; `uv run lint-imports`, full `uv run pytest src/tests/`, `validate_workflow_cards`, and the mega-job task-registration obligations are all part of "done" for any stage that touches them.

---

## 9. Acceptance criteria

1. `bronze.shot_freeze_frames` contains correct, validated freeze frames for **all** context cohorts (SB-360 + GS/SC), verified by `(match_key, action_id)`, no fan-out, with `access_tier`.
2. Both HF datasets published (public + `-restricted`), RM/GS only in restricted repos (leak-guard green), cards present.
3. `xg_model_v3` is Champion, trained on the **full** corpus incl. RM (both repos read), with documented per-provider two-mode OOS metrics and an OOF calibrator; envelope `coordinate_system=spadl_105x68`.
4. `fct_shot_xg` populated with calibrated per-row pre-shot xG + CI + mode + ood_flag for all shots, joinable to `fct_action_values` on `(match_key, action_id)`; penalties constant.
5. `fct_xg_predictions_v2` consumers migrated (view or retire).
6. Full `pytest` suite + `lint-imports` + card validation + all per-stage live gates green.

---

## 10. Risks, rollback, open items

- **SB-360 coordinate conversion (A1) is the single highest-risk unit** — a mirrored freeze frame (missing the y-flip) would produce plausible-but-wrong xG on ~2/3 of the context corpus and surface downstream as a flat xT-GK gradient that looks like a consumer bug. Mitigated by pinning to silly-kicks `_convert_locations` (no hand-rolled scale) + the **co-location golden** (frame↔action spatial consistency, written first) + the live `(match_key, action_id)` distribution gate. `fidelity_version` must be threaded per-match (m3).
- **Trainer read-both** (B2) — mitigated by fail-loud-if-companion-missing + a corpus-composition assertion (expected RM shot count present).
- **Cross-mart AV↔AC agreement** (§8 gate) — the seam the downstream `fct_shot_xg ⋈ fct_action_context` join depends on; verified by anti-join = 0 + a coordinate spot-check.
- **Rollback:** Stage A is additive (new column/rows/datasets). Stage B does not touch `xg_model_v2` (new model name); revert = re-point `wf-xg-v2`/scorer to v2. Stage C: `fct_shot_xg` is new + gated; `fct_xg_predictions_v2` kept as a view until consumers migrate; ADR-043 strand-safe if synced.
- **Sequencing note (M3):** the bundled "nothing deferred" scope puts A1 (the mirror-risk unit) on the critical path to the downstream unblock (calibrated GS/SC xG), even though that unblock is reachable via the tabular-only path alone (no SB-360, no HF publishing, no SB/WS consolidation). This is a deliberate, eyes-open choice. **Decoupling lever:** if unblock latency ever binds, the plan can ship a GS/SC-inclusive first milestone (trainer + scorer on the already-existing corpus, tabular-only + tracking freeze frames) ahead of the SB-360 consolidation. See §1 note for the plan-sequencing decision.

---

## 11. References

ADR-013 (ML inference outputs), ADR-012 (training→prod delivery), ADR-016 (native-id conventions), ADR-030 (GS frame dedup), ADR-043 (strand-safe re-derive), ADR-049/064 (restricted companion repos / per-match access_tier), ADR-053/035 (frame orientation), ADR-066 (canonical-SPADL pre-shot xG / `fct_shot_xg`). Prior spec: `2026-07-05-canonical-spadl-preshot-xg-unification-design.md`. Distributed-sink follow-up: `project_distributed_sink_architecture_followup` (interim driver-side compute; long-term worker-drain).
