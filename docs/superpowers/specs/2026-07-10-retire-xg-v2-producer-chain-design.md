# Design: Retire the old v2 xG producer chain

**Status:** proposed (2026-07-10)
**Related:** ADR-066 (canonical-SPADL pre-shot xG → `fct_shot_xg`), ADR-013 (ML inference outputs), ADR-012 (training→prod delivery), SEC2 (artifact-hash integrity), AI Governance (`AI_GOVERNANCE.md`, `wf-xg-v2`).

## Context

`fct_xg_predictions_v2` was retired into a back-compat **table** that *projects* `fct_shot_xg` (the canonical-SPADL `xg_model_v3`) into the legacy schema (PR #434 + the live rebuild). Consequently the **old v2-model producer chain feeds nothing** and is dead code:

```
compute_xg_model_v2 (task)  →  bronze.xg_predictions_v2  →  stg_xg__predictions_v2  →  [nothing]
   (ingestion/xg_model_v2.py,                                    (orphaned — fct_xg_predictions_v2
    loads xg_model_v2@Champion)                                   now selects from fct_shot_xg)
```

Verified live (2026-07-10):
- `soccer_analytics.dev_gold.xg_model_v3@Champion` = **version 1, registered** — the production model backing `fct_shot_xg` (scored `bronze.xg_shot_predictions`, mean xG 0.112). So the v3 path is the live source of truth; the v2 trainer/model are safe to remove.
- `xg_model_v2@Champion` = version 5, still registered — part of the live cleanup.
- Grep confirms **zero** dbt models reference `stg_xg__predictions_v2` / `source('xg','xg_predictions_v2')` except the orphaned staging model itself. `fct_xg_predictions_v2.sql` selects from `fct_shot_xg`/`fct_action_values`/`fct_shots` — never the v2 staging view.

## Goal

Delete the dead v2 producer chain, **migrate the security-integrity coverage from v2 to v3** (not just drop it), and fix the documentation/governance drift that still names the deleted files — all in **one PR**. This closes the ADR-066 "dual-model window."

## This is a PARTIAL retirement — what is KEPT

The `wf-xg-v2` **governance card, HF model card, and `AI_GOVERNANCE.md` System-1 entry were evolved in place to govern the v3 model** (ADR-066 "m4 decoupling"). They are the governed evaluative system for pre-shot xG and **stay**. Therefore:
- `PER_PLAYER_EVALUATIVE_CARDS` is **unchanged** — this is NOT the removal of an evaluative system, so the heavy AI-governance card-removal flow does not apply. The governance work is limited to correcting stale file-path references.
- The back-compat `fct_xg_predictions_v2` mart, its `fct_xg_predictions_v2_synced` Lakebase table, PG indexes, and the Taipy shot-map consumer all **stay** (already consistent with the target state).
- Shared analytics code (`analytics/xg_model.py`, `analytics/set_encoder.py`) is used by both v2 and v3 — **KEPT**.

## Decisions (resolved)

1. **`wf-xg-v2.yaml` `execution:` block** — it still wires to the dead pipeline (`compute_xg_model_v2` / `train_xg_v2_hf.py`), while `wf-shot-xg-scorer.yaml` already owns v3 inference. **Decision: strip `wf-xg-v2.yaml` to a governance/methodology record** — remove the `execution:` block and repoint `links.source_code`/`links.tests` to the v3 surfaces (`ingestion/xg_shot_scorer.py`, `scripts/train_xg_v3_hf.py`, their tests). Rationale: the card's purpose is now governance of the v3 model; execution lives on `wf-shot-xg-scorer` + `wf-xg-v2` should not name deleted files. `test_card.py:145-146` updates in lockstep.
2. **SEC2 hash scope** (`bootstrap_artifact_hashes.py` + `test_bootstrap_artifact_hashes.py`): **replace `xg_model_v2` with `xg_model_v3`** in `_MLFLOW_MODELS` + `_VOLUME_ARTIFACTS`. Dropping v2 without adding v3 would leave the pre-shot xG model uncovered by integrity hashing — a security regression. Same PR.
3. **`test_model_loaders_verify_hash.py`**: **replace the `xg_model_v2.py` loader entry with `xg_shot_scorer.py`** (the v3 loader, 2 `verify_artifact_hash` call sites) — closes a pre-existing coverage gap.
4. **Orphaned HF repo `luxury-lakehouse/xg-v2-model-set-encoder`**: **keep as a frozen historical artifact** (no further pushes once the trainer is gone; the model card's "Model Files" already points production at v3). No HF deletion.
5. **`sk3_mig_b_retrain.py` / `sk3_mig_rebuild.py`** (completed one-off migration orchestrators referencing v2): **leave untouched** as an audit trail — they are historical records, not live pipeline code.

## Inventory

**Delete (v2-only, zero live consumers):**
- `src/ingestion/xg_model_v2.py`; `scripts/train_xg_v2_hf.py`
- `dbt_project/models/staging/xg/stg_xg__predictions_v2.sql`; the `xg_predictions_v2` block in `_xg__sources.yml`
- `terraform/modules/workflows/main.tf`: the `compute_xg_model_v2` task block + the `depends_on { task_key = "compute_xg_model_v2" }` on `dbt_build_output_marts`
- `pyproject.toml` entry point; `dbt_project/seeds/task_workflow_mapping.csv` row; `src/ingestion/guards.py` `_GUARD_MODULES` entry
- Test files (whole): `test_xg_model_v2.py`, `test_xg_v2_regrouping.py`, `test_train_xg_v2_hf.py`, `tests/smoke_gates/sk3_mig_b/test_xg_v2_post_retrain_smoke.py`

**Edit — test-graph surgery (remove entries, keep green):** `test_card_parity_with_terraform.py`, `test_terraform_workflow_dbt_task.py`, `test_dbt_mart_classification.py`, `test_workflow_dag_gold_reads.py`, `test_xg_v2_adr013_static.py` (2 file-exists assertions), `tests/data_quality/test_bronze_live_schema.py` (`test_xg_predictions_v2_live_schema_covers_writer`), `tests/data_quality/test_dbt_xg_v2_mart.py` (stale staging-join test), `test_sync_hf_costs.py`, `test_sk3_mig_b_orchestrator_invariants.py`, `test_card.py`.

**Edit — SEC2 migration:** `scripts/bootstrap_artifact_hashes.py` + `test_bootstrap_artifact_hashes.py`; `test_model_loaders_verify_hash.py`.

**Edit — doc/governance drift (name deleted files):** `workflow-cards/wf-xg-v2.yaml` (strip execution), `docs/huggingface/model-cards/xg-v2-model-card.md` (Model Files), `AI_GOVERNANCE.md` (System-1 source column), `ARCHITECTURE.md`, `CLAUDE.md:120`, `docs/performance-baselines.md` (annotate retired, don't silently drop), stale docstrings in kept files (`analytics/set_encoder.py`, `ingestion/artifact_deploy.py`, `ingestion/sync_hf_costs.py`, `scripts/publish_xg_shots_hf.py`), and `_marts__models.yml` fct_xg_predictions_v2 doc block (pre-existing drift: says "Deep Sets"/"xg_v2_enabled" — correct to the v3 projection). **(review adds)** two stale `xg_model_v2` comments in kept files (`ingestion/shot_freeze_frames.py:151`, `analytics/action_context/tracking_snapshots.py:64`); refresh the `assert_xg_v2_view_shot_id_1to1.sql` "view"→"table" wording; **ADR-066 amendment recording why "v2" (card + HF repo) now governs/points at the v3 model** so the naming reads as intentional (the card-ID rename stays a deferred inventory change).

**Live cleanup (gated operator steps, separate approval):** drop `bronze.xg_predictions_v2`; deregister MLflow `xg_model_v2` (+ `@Champion`); remove UC Volume `model_weights/xg_model_v2/`.

## Invariants (the change must preserve)

- `test_ai_governance_md.py` green with `PER_PLAYER_EVALUATIVE_CARDS` **unchanged** (System-1 still governed).
- SEC2 coverage does **not** regress — `xg_model_v3` present in the hash-bootstrap scope after the change.
- No live consumer breaks: `fct_xg_predictions_v2` (+ synced table + Taipy shot-map) unchanged; `dbt_build_output_marts` still builds all output marts after its `compute_xg_model_v2` edge is removed.
- Test graph stays green: DAG-read, card-parity, task-count-anchor, mart-classification all consistent after the task removal.
- `lint-imports`, `ruff`, `pyright`, dbt parse, `validate_workflow_cards` all pass.

## Out of scope / follow-ups

- Notify silly-kicks that `fct_shot_xg` is live (separate, non-code).
- Any rename of the `wf-xg-v2` card ID (would be a governance-inventory change; explicitly NOT done here).
