# ADR-023: Retire xG v1 (XG1-RETIRE)

| Field | Value |
|---|---|
| **Date** | 2026-05-03 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The lakehouse has carried two production xG models in parallel since PR 3 (ADR-013):

- **xG v1** — calibrated XGBoost (13 features) + logistic baseline, ROC-AUC 0.979, written by `src/ingestion/xg_model.py` to `bronze.xg_predictions` → `dev_gold.fct_xg_predictions`.
- **xG v2** — Deep Sets set encoder (Zaheer et al. 2017) + MC dropout 95% CI (Gal & Ghahramani 2016), ROC-AUC 0.915, written by `src/ingestion/xg_model_v2.py` to `bronze.xg_predictions_v2` → `dev_gold.fct_xg_predictions_v2`.

Both models are per-shot, post-aggregable to per-player, and listed as "Expected Goals" in `AI_GOVERNANCE.md` §5 Scope (rows 1 + 2). The v1 surface area spans every lakehouse layer: a Databricks workflow task (`compute_xg_model`), a dbt mart + staging model, a Lakebase synced table, an HF Hub model repo, an HF model card, a workflow card YAML, a Taipy Shot Map dropdown selector with two model variants ("Custom (Logistic)", "Custom (XGBoost)"), and a `compute_xg_predictions` entry-point in `pyproject.toml`.

The forcing function is SK3-MIG-B (silly-kicks 3.0.1 retrain cycle). Group A (SK3-MIG-A) full-rebuilt `bronze.spadl_actions` + `dev_gold.fct_action_values` against canonical-LTR coordinates. Group B retrains all action-value-derived models, including v2. To preserve dual-model parity, v1 would also need:

- A retrain dispatch in the SK3-MIG-B Group 1 orchestrator (~$0.50 + cycle time)
- A post-retrain smoke gate (`test_xg_v1_post_retrain_smoke.py`)
- Lakebase synced refresh + index restoration on `fct_xg_predictions_synced`
- Cross-version coordination with v2 (the "which model is the user looking at?" UX problem)

The maintenance cost — for a model whose v2 successor has a tighter calibration band, MC dropout CIs, and 360-aware features — is no longer justified. v2 is the production scorer; v1 is dead weight.

## Decision

Retire xG v1 entirely as part of SK3-MIG-B PR-α. Remove all v1 infrastructure across 7 layers in a single squash commit + Phase 9 operator-runtime steps. v2 becomes the only xG production scorer. The Taipy Shot Map's "xG Model" dropdown collapses from 3 options to 2: `["StatsBomb", "v2 Set Encoder"]`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Retrain v1 alongside v2 | preserves dual-model A/B; no UX migration | recurring cycle cost; smoke-gate maintenance; users confused by 3-way selector | v2 has eclipsed v1 on every metric that matters except ROC-AUC (which is a calibration artifact, not a discrimination win) |
| B. Freeze v1 at SK3-MIG-A coords (no retrain, but keep predictions) | zero cycle cost | predictions reflect a stale coord convention; users see drift between v1 and v2 outputs of the same shot | predictions tied to a deprecated coord convention create silent wrong-answer risk |
| C. Retire v1 entirely (chosen) | removes ~7 layers of dead weight; collapses UX; eliminates retrain-cycle line item | irreversible without v1 reintroduction work; HF Hub repo `xg-model-statsbomb-wyscout` left orphaned (operator follow-up) | — |

## Consequences

### Positive

- **Cycle cost down ~$0.50 per SK3-MIG-B run + recurring.** Removes the v1 retrain dispatch + smoke gate + synced refresh.
- **Surface area down by 11 files** (`src/ingestion/xg_model.py`, `scripts/train_xg_model_hf.py`, `notebooks/train_xg_model.py`, `dbt_project/models/marts/fct_xg_predictions.sql`, `dbt_project/models/staging/xg/stg_xg__predictions.sql`, `workflow-cards/wf-xg-v1.yaml`, `docs/huggingface/model-cards/xg-model-card.md`, `src/tests/test_xg_model.py`, plus pyproject + Terraform task + dbt YAML edits).
- **Shot Map UX simpler.** 2-option dropdown vs 3-option; v2 CI band display becomes the primary differentiator.
- **AI_GOVERNANCE.md narrower.** §5 Scope row count drops from 13 to 12 evaluative ML systems; §6 hypothetical-deployment table aligned.
- **Regression test prevents re-introduction.** `src/tests/test_xg_v1_retired.py` glob-asserts v1 names absent across 7 layers; `import ingestion.xg_model` raises ModuleNotFoundError.

### Negative

- **Irreversible from the Phase 9 operator-runtime forward.** Once `terraform apply` removes the v1 task block + the physical mart is dropped, restoring v1 requires un-doing every layer + re-running the full v1 backfill.
- **HF Hub model repo `luxury-lakehouse/xg-model-statsbomb-wyscout` orphaned.** Listed in `_MODEL_CARD_EXEMPT` with a "retired" reason; an operator follow-up may delete the HF Hub repo, or leave it for historical reproducibility (CC-BY-NC license).
- **Loss of v1-vs-v2 A/B comparison data on the Shot Map.** v2's MC dropout CI band partially compensates by showing per-shot uncertainty natively.

### Neutral

- `src/analytics/xg_model.py` is intentionally retained — it provides freeze-frame parsing + `serialize_xgboost_model` used by v2's training + inference paths. The "v1" name in the module is a historical artifact; the regression test explicitly exempts it via `_FORBIDDEN_NAMES` excluding the bare `xg_model.py` filename in favor of an explicit `src/ingestion/xg_model.py` path check.
- The xG v1 `psxg-model` repo on HF Hub is a separate model (PSxG, post-shot xG, used by goalkeeper analytics) and is unaffected by this ADR.

## Related

- **Commits:** SK3-MIG-B PR-α (this PR; squash hash TBD)
- **Specs:** `docs/superpowers/specs/2026-05-03-sk3-mig-b-retrain-and-republish-design.md` §6 (XG1-RETIRE drop ordering)
- **Plans:** `docs/superpowers/plans/2026-05-03-sk3-mig-b-retrain-and-republish.md` §4 (Phase 4 task list)
- **ADRs:** ADR-013 (ML inference outputs dbt mart — v2 conforms; v1 conformed historically); ADR-022 (direction-of-play migration — Group A predecessor that triggered the v1 retrain question)
- **Regression tests:** `src/tests/test_xg_v1_retired.py`, `src/tests/test_shot_map_v2_columns.py`
- **External references:** Robberechts & Davis (2020) xG methodology (continues to underpin v2); Zaheer et al. (2017) Deep Sets; Gal & Ghahramani (2016) MC Dropout

## Notes

The 11-file deletion + 8-file modification scope is large enough that the regression test (`test_xg_v1_retired.py`) is the single most important deliverable — it glob-asserts every v1 surface across the codebase and would catch an accidental v1 re-introduction (e.g., a copy-paste from a stale tutorial).

Plan-write-time documentation referenced `xg-model-statsbomb-wyscout.md` as the v1 model card filename; the actual file is `xg-model-card.md`. The plan's filename was the F2V v1 card (which is retained, supporting Football2Vec v1 still in production). Both the regression test and the AI_GOVERNANCE inventory test were updated accordingly.
