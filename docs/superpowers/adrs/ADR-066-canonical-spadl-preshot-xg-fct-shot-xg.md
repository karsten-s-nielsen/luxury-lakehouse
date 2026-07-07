# ADR-066: Canonical-SPADL pre-shot xG — unified all-provider model + `fct_shot_xg`

| Field | Value |
|---|---|
| **Date** | 2026-07-05 |
| **Status** | Accepted — design approved; implementation in progress on `feat/canonical-spadl-preshot-xg`. Supersedes the `fct_xg_predictions_v2` `shot_id`-keyed path. |
| **Deciders** | Karsten (operator), lakehouse session, silly-kicks consumer (xT-GK v2 SP1) |

## Context

The v2 xG set encoder (Deep Sets + MC dropout, `xg_model_v2@Champion`) produces a well-calibrated **pre-shot** xG but only for **StatsBomb-360** shots: it consumes raw StatsBomb-yard geometry (`location_x∈[0,120]`, distance in yards) and freeze-frame positions normalized `÷120,÷80`, and it is materialized in `fct_xg_predictions_v2` keyed `shot_id`, joined off `fct_shots` — a mart structurally limited to `data_source IN ('statsbomb','wyscout')`. The current writer emits **no** value without a freeze frame (`xg_model_v2.py:255-259` `continue`s → NaN; live Wyscout coverage 0/43,075).

silly-kicks' xT-GK v2 SP1 needs a calibrated pre-shot xG reward on every shot in two tracking cohorts (Gradient Sports WC2022 ≈ 1,363 shots / 64 matches; SkillCorner Real Madrid ≈ 225 shots / 10 matches). Those shots live entirely on the tracking side (`gradientsports`, `skillcorner`) as `action_type='shot'` rows in `fct_action_values` / `fct_action_context`, with no `stg_<provider>__shots`, no `fct_shots` presence, and no xG. Feeding SPADL meters into a StatsBomb-yard model is wrong (an 18.3 m shot ≠ an 18.3 yard shot). The deeper risk is not coordinates but **set-distribution shift**: the set encoder aggregates by **sum**, so a full-22 tracking freeze frame produces a context vector ~2–3× the magnitude of the partial broadcast-visible SB-360 sets the prediction MLP trained on — a Platt recalibration cannot fix per-shot ranking, only the aggregate level, which for a per-shot reward is worthless.

Two firm user directives shape every decision: **(1)** canonical geometry is SPADL 105×68 m — never bend any provider's data *or a model* to StatsBomb units; fix the model by making it coordinate-native to canonical SPADL, not by rescaling providers into yards. **(2)** keep xG separate from the feature mart — a governed model prediction is an ADR-013 inference output with its own lifecycle, not a column bolted onto `fct_action_context`; reuse is *shared code*, not a *shared pipeline run*.

## Decision

Retrain the set encoder in **canonical SPADL 105×68** space (envelope `coordinate_system: "spadl_105x68"`), **including the tracking cohorts' full-22 freeze frames** in the v3 training set (GroupKFold-by-match holdout) plus an explicit **set-cardinality feature**, so every provider scores natively in one coordinate system with no out-of-distribution set gap. Land all pre-shot xG in a new unified mart **`fct_shot_xg`** keyed **`(match_key, action_id)`**, retiring `fct_xg_predictions_v2` to a back-compat view.

Concretely:

- **(a) Coordinate contract → SPADL 105×68.** The v3 model (`xg_model_v3`, a re-coordination + retrain of the same Deep Sets architecture — no new academic citation) is coordinate-native to canonical SPADL. Tabular geometry (`distance_to_goal`, `shot_angle`) is computed in meters (goal at `(105, 34)`, width `7.32 m`); freeze-frame positions normalize `÷105,÷68` (which lands on the identical `[0,1]` fractional position `÷120,÷80` did — the set-encoder input is coordinate-invariant under correct normalization). The envelope records the contract explicitly; an inference-time coordinate guard range-checks `x∈[0,105], y∈[0,68]` and **raises** on violation to stop a v2(yards)/v3(SPADL) mixup during the dual-model window.

- **(b) `fct_shot_xg` keyed `(match_key, action_id)` replaces `fct_xg_predictions_v2`.** ADR-013 staging view `stg_xg__shot_predictions` + contract-enforced gold mart; Kimball FKs resolve via INNER JOIN to `fct_action_values` / `fct_action_context` on `(match_key, action_id)` — the direct injection key silly-kicks reads next to `pressure_on_actor__*`, no publish-time join. `fct_xg_predictions_v2` becomes a back-compat **view** (or materialized only on a measured latency need) that bridges `fct_shot_xg` → `fct_shots` via `original_event_id ↔ event_id` to preserve the old `shot_id`-keyed shape for existing non-SQL consumers (Hyrum's Law).

- **(c) `bronze.shot_freeze_frames` as a reusable feature-layer FACT.** A persisted per-shot player-set artifact (`match_key, action_id, data_source, [x, y, is_keeper, is_teammate] rows, _ingested_at`; `replaceWhere` per `match_key`), written by a **standalone** `build_tracking_snapshots` builder (tracking) and `build_sb360_snapshots` (StatsBomb-360). It is a **feature-layer fact, not a prediction** — provider-agnostic, reusable by future models, and decoupled from the action-context pipeline run (cheap re-linkage, no AC recompute to backfill).

- **(d) Tracking shots trained + a two-mode per-provider discrimination gate.** The 1,588 GS/SC full-22 freeze frames go into v3 training (fixing the set-distribution shift *at the source* rather than meeting it OOD). Each tracking provider is scored in **both modes** — (a) context-aware (freeze frame → context vector) and (b) tabular-only (zero context; the certified ~v1-quality baseline floor, AUC ~0.82) — and a per-provider **OOS discrimination gate** (GroupKFold-by-match) ships context-aware **only if** it beats the tabular baseline *and* clears **StatsBomb OOS AUC − margin** (absolute backstop ≥ 0.65); else tabular ships. The shipped mode is recorded per row in `scoring_mode ∈ {context_aware, tabular_only}`. Certification requires **discrimination *and* n-aware calibration** (binomial / Hosmer-Lemeshow, not a fixed 10%); failing either sets `ood_flag` and `ood_flag=true ⇒ silly-kicks excludes that cohort`.

- **(e) Decoupling: the prediction is a governed ADR-013 output, not a feature-mart column.** The xG prediction is written by an ADR-013 writer (`ingestion.xg_shot_scorer` → `bronze.xg_shot_predictions` → staging → `fct_shot_xg`), never embedded as a column on `fct_action_context`. Reuse of the action-context **frame-linkage code** (`link_actions_to_frames`, `sk_frame_adapters`) is code reuse, not pipeline coupling. Consequence: **retraining xG re-scores without an action-context recompute** — the scorer runs on the shot subset independently of the (expensive) AC drain.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Rescale tracking geometry → StatsBomb yards, keep the v2 model | No retrain; fast unblock | Re-couples stored/ephemeral data to a legacy convention; leaves the lead set-distribution risk (v2 still trained on SB-360 partial sets) unaddressed | Violates the canonical-SPADL directive; doesn't fix B2 |
| B. Inference-only adapter — scale just the ephemeral model-input vector into trained (StatsBomb) space, keep stored data canonical | Stored data stays canonical; no retrain | Re-couples the model *input* to StatsBomb units; still trained on SB-360 partial sets (B2 unfixed) | Recorded eyes-open as the lever if unblock *latency* becomes binding — not adopted (§7.1) |
| C. Embed xG as a column on `fct_action_context` | One mart to read | Bolts a governed prediction onto a feature mart; every xG retrain forces an AC recompute | Violates the decouple directive; ADR-013 exists precisely to avoid this |
| D. Naive sum→mean in the set encoder to normalize full-22 vs partial magnitude | Removes the magnitude gap | Discards defender-density signal (xG-predictive) | Rejected for a **sum + set-cardinality feature** (R3) that keeps density |
| E. Reconstruct SB-360's broadcast-visible subset from full tracking (Option A in spec) | Would match training distribution | Camera visibility is not recoverable from tracking — ill-defined | Trades a known, learnable shift for an unprincipled filter |
| F. **Retrain SPADL-native on all providers incl. tracking, two-mode gate, `fct_shot_xg`** (chosen) | Canonical everywhere; set shift fixed at source; robust tabular floor; decoupled from AC | Full governed retrain sits on silly-kicks' unblock critical path | — |

## Consequences

### Positive

- One SPADL-native pre-shot xG source of truth across **all** providers, keyed `(match_key, action_id)` — joins directly onto the action stream with no coordinate coupling and no publish-time join.
- The tracking cohorts get the **better of** context-aware and tabular-only scoring, with an honest per-provider discrimination + n-aware calibration gate and per-row MC-dropout CI (the main OOD signal for the tabular path).
- `bronze.shot_freeze_frames` is a reusable feature-layer fact — future models consume it without a new linkage pass.
- Retraining xG re-scores independently of the action-context drain (decoupled writer) — no AC recompute forced by a model change.
- New providers slot in via the writer, not a mart edit.

### Negative

- Phase 0 is a **full governed retrain** (new champion, model-card + `AI_GOVERNANCE.md` + `governance:` YAML updates, OOS gates) and sits **on the critical path** to silly-kicks' unblock (§7.1) — a deliberate consequence of the two firm directives.
- A **dual-model window** (`xg_model_v2` yards + `xg_model_v3` SPADL) exists until Phase 2 retires the old path — mitigated by the inference-time coordinate guard (M3).
- StatsBomb shot-map xG **shifts** (new SPADL-native model, and SPADL routes penalties → `shot_penalty`, own-goals → `bad_touch`+`owngoal`, so the trained population differs from the current `xg-shot-data` set). Requires a documented "which shots count" decision (V-1) and a methodology caption ("never silently substitute").
- `fct_xg_predictions_v2` as a view turns former point lookups into a 2-hop join — escalate to materialized only on a measured latency need (V-5).

### Neutral

- Same Deep Sets + MC dropout architecture — a re-coordination + retrain, not a new architecture; ARCHITECTURE Appendix D unchanged, no new governance card (`wf-xg-v2` evolved in place).
- Both cohorts are **restricted** providers (GS + Real Madrid SkillCorner private); silly-kicks reads the internal gold mart directly, so **no HF publish is on the critical path**. If `fct_shot_xg` is ever published, restricted rows split to the private companion repo via `split_restricted` + `assert_no_private_leak` (ADR-064 / ADR-049), with `access_tier` riding per-row from the action stream.

## Related

- **Specs:** `docs/superpowers/specs/2026-07-05-canonical-spadl-preshot-xg-unification-design.md`
- **ADRs:**
  - **ADR-013** (ML inference outputs → writer → bronze → staging → mart) — the pattern `fct_shot_xg` follows.
  - **ADR-012** (training→production delivery) — `train_xg_v3_hf.py` delivery contract (`require_mlflow_env` / `set_and_verify_mlflow_champion` / `upload_weights_to_uc_volume`; envelope `feature_names` + `tabular_dim`).
  - **ADR-018** (cross-table join contracts) — the `(match_key, action_id)` and `original_event_id ↔ event_id` bridge join contracts.
  - **ADR-035** (silly-kicks 4.2 vectorized frame orientation / geometric home-LTR backstop) — the orientation the snapshot builder relies on before attack-normalizing.
  - **ADR-064** (per-match access tier) — restricted-provider publishing posture if a dataset is ever published.
  - **Supersedes** the `fct_xg_predictions_v2` `shot_id`-keyed path (retired to a back-compat view).

## Notes

The set-distribution shift (B2) is the risk to lead with — not coordinates. It is resolved three ways at once: train on the full-22 tracking sets (R2), add a set-cardinality feature to keep the density signal the sum encodes (R3), and bound the downside with the trained tabular-only baseline + discrimination gate (R1). V-6 (measure the SB-360-vs-tracking set-cardinality distribution) is a **diagnostic** informing the training mix, no longer a gate on a fragile scoring-time convention.
