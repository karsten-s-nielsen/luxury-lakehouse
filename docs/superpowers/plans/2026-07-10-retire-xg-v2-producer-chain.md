# Plan: Retire the old v2 xG producer chain (one PR)

Design: `docs/superpowers/specs/2026-07-10-retire-xg-v2-producer-chain-design.md`. Single feature = single commit = single PR. Test-first where practical; the bulk is deletion + test-graph surgery + doc fixes, so the discipline is "make each guard test assert the NEW reality, run the full suite, keep it green at every step."

## Prerequisites (verified — no action)
- `xg_model_v3@Champion` = v1 registered (live source of `fct_shot_xg`). ✓
- No dbt model refs `stg_xg__predictions_v2` / the v2 source except itself. ✓

## Phase 1 — Code + config deletions
1. Delete `src/ingestion/xg_model_v2.py`.
2. Delete `scripts/train_xg_v2_hf.py`.
3. Delete `dbt_project/models/staging/xg/stg_xg__predictions_v2.sql`; remove the `xg_predictions_v2` table block from `dbt_project/models/staging/xg/_xg__sources.yml`.
4. `terraform/modules/workflows/main.tf`: remove the `compute_xg_model_v2` task block **and** the `depends_on { task_key = "compute_xg_model_v2" }` line on `dbt_build_output_marts`. `terraform fmt`.
5. Remove the `compute_xg_model_v2` entry point from `pyproject.toml`; the row from `dbt_project/seeds/task_workflow_mapping.csv`; the `"ingestion.xg_model_v2"` entry from `src/ingestion/guards.py` `_GUARD_MODULES`.

## Phase 2 — Test-graph surgery + SEC2 migration
6. Delete the 4 v2-only test files (`test_xg_model_v2.py`, `test_xg_v2_regrouping.py`, `test_train_xg_v2_hf.py`, `tests/smoke_gates/sk3_mig_b/test_xg_v2_post_retrain_smoke.py`).
7. Edit the infra graph tests to drop `compute_xg_model_v2` / `xg_predictions_v2` entries: `test_card_parity_with_terraform.py`, `test_terraform_workflow_dbt_task.py` (+ its task-count anchor if present), `test_dbt_mart_classification.py`, `test_workflow_dag_gold_reads.py`. Confirm each still asserts the remaining graph correctly.
8. `test_xg_v2_adr013_static.py`: remove `test_staging_v2_file_exists` + `test_v2_source_declared`; keep the back-compat-table assertions.
9. `tests/data_quality/test_bronze_live_schema.py`: delete `test_xg_predictions_v2_live_schema_covers_writer` (imports the deleted module). `tests/data_quality/test_dbt_xg_v2_mart.py`: remove/rewrite the stale staging-join test.
10. `test_sync_hf_costs.py`, `test_sk3_mig_b_orchestrator_invariants.py`, `test_card.py`: drop `train_xg_v2_hf.py` / `test_xg_model_v2.py` references (the last in lockstep with the `wf-xg-v2.yaml` `links` edit, Phase 3).
11. **SEC2 migration:** `scripts/bootstrap_artifact_hashes.py` — swap `xg_model_v2` → `xg_model_v3` in `_MLFLOW_MODELS` + `_VOLUME_ARTIFACTS`; update `test_bootstrap_artifact_hashes.py` scope-lock set accordingly. Regenerate the SEC2 baseline hashes for `xg_model_v3` (operator step if the hashes are live-fetched — confirm mechanism during impl).
12. `test_model_loaders_verify_hash.py`: replace the `xg_model_v2.py` loader entry with `xg_shot_scorer.py` (expect its 2 `verify_artifact_hash` sites).

## Phase 3 — Governance + doc drift (edits, not deletions)
13. `workflow-cards/wf-xg-v2.yaml`: strip the `execution:` block; repoint `links.source_code`/`links.tests`/`outputs.tables` to the v3 surfaces (`ingestion/xg_shot_scorer.py`, `scripts/train_xg_v3_hf.py`, `fct_shot_xg`/`bronze.xg_shot_predictions`). Keep `governance:` + methodology.
14. `docs/huggingface/model-cards/xg-v2-model-card.md`: correct "Model Files" (v3 module/registry/volume), remove the false "unchanged code path = ingestion.xg_model_v2" claim.
15. `AI_GOVERNANCE.md`: System-1 source column → `ingestion/xg_shot_scorer.py` + `scripts/train_xg_v3_hf.py`.
16. `ARCHITECTURE.md`, `CLAUDE.md:120`, `docs/performance-baselines.md` (annotate `compute_xg_model_v2` retired, keep baseline history), `_marts__models.yml` fct_xg_predictions_v2 block (correct to v3 projection + `xg_v3_enabled`), and the stale docstrings (`analytics/set_encoder.py`, `ingestion/artifact_deploy.py`, `ingestion/sync_hf_costs.py`, `scripts/publish_xg_shots_hf.py`).
16a. **(review add) Two stale `xg_model_v2` comments in KEPT files** — will name a deleted module post-PR: `src/ingestion/shot_freeze_frames.py:151` ("mirrors xg_model_v2.run_pipeline") and `src/analytics/action_context/tracking_snapshots.py:64` ("mirroring xg_model_v2._XG_V2_BRONZE_COLS"). Reword to reference the v3 surface (or drop the module name). Comments only — no functional break, but closes the drift.
16b. **(review add) Refresh `dbt_project/tests/assert_xg_v2_view_shot_id_1to1.sql`** — its title/comment say "v2_view / view over fct_shot_xg" but `fct_xg_predictions_v2` is now a **table**. Update the in-file comment to say "table"; the filename keeps "v2" (historical, per the deferred card-ID-rename decision) with a one-line note that "view" wording is historical.
16c. **(review add) ADR-066 amendment — record WHY "v2" now means v3.** Add a short amendment noting the deliberate naming split: the `wf-xg-v2` card + `xg-v2-model-set-encoder` HF repo govern/point at the v3 model (ADR-066 m4 decoupling); the card-ID rename is a deferred inventory change. So a future reader reads it as intentional, not stale.

## Phase 4 — Verify (all local, before commit)
17. `uv run ruff check src/ scripts/` + `ruff format --check`; `uv run lint-imports`; `uv run pyright src/` (touched modules).
18. Full `uv run pytest` — green (0 failures); specifically `test_ai_governance_md.py`, `test_bootstrap_artifact_hashes.py`, `test_model_loaders_verify_hash.py`, `test_card*.py`, the DAG/parity/mart-classification tests.
19. `validate_workflow_cards` (CI-only pydantic gate) — run locally if possible; else rely on PR CI.
20. dbt parse with `--vars xg_v3_enabled=true` to confirm the staging/source removal doesn't orphan a ref.
21. Wheel bump (`bump_wheel.py`) + `final-review`.

## Phase 5 — Gated live cleanup (SEPARATE explicit approval, after merge+deploy)
22. `DROP TABLE soccer_analytics.bronze.xg_predictions_v2` (operator; the writer is gone so it's inert).
23. Deregister MLflow `soccer_analytics.dev_gold.xg_model_v2` (delete `@Champion` alias + model, or archive) — confirm nothing else references it first.
24. Remove UC Volume `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/`.
25. Post-cleanup: re-run `test_bootstrap_artifact_hashes.py` semantics against live (v3 present, v2 absent).

## Risks / notes
- The `dbt_build_output_marts` `depends_on` removal must not drop below the documented "phase-2 compute tasks that write bronze read by an output_mart" invariant — v2 no longer writes a consumed bronze, so removal is correct (mirror-image of the edge we ADDED for `compute_xg_shot_scores`).
- SEC2 baseline-hash regeneration for v3 may need a live MLflow/UC fetch — do NOT leave v3 unhashed (the whole point of decision #2).
- Deregistering `xg_model_v2` is destructive + irreversible on the registry — gated, operator-run, after confirming `xg_shot_scorer` never falls back to it.
