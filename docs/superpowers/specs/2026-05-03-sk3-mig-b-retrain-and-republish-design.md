# SK3-MIG-B — silly-kicks 3.0.1 Group B retrain + HF republish + baseline rebase + XG1-RETIRE + HF4

| Field | Value |
|---|---|
| **Date** | 2026-05-03 |
| **Status** | Draft (brainstorm complete; awaiting plan) |
| **Cycle** | SK3-MIG-B (Group B — model retrains; consumes Group A's canonical-LTR `fct_action_values`) |
| **Predecessor** | SK3-MIG-A SHIPPED 2026-05-02 PR #249 squash `485fc10` (data correctness) |
| **Folds in** | XG1-RETIRE (TODO row), HF4 (notebook → PEP 723 publisher migration) |
| **Companion ADR** | ADR-014 amendment (HF publisher discipline — PEP 723 only, notebook ban) |
| **Triggering memory** | `project_sk3_mig_complete.md` §"Group B (SK3-MIG-B) — queued" |

## §0 — Context

SK3-MIG-A shipped on 2026-05-02 — silly-kicks 3.0.1's per-converter `to_spadl_ltr(input_convention=...)` dispatch + lakehouse derivation helpers + `SILLY_KICKS_ASSERT_INVARIANTS=1` in CI/production. `bronze.spadl_actions` was full-rebuilt across all 4 sources, dbt cascade was full-refreshed (32/32 marts PASS), `expected_threat_grids` was wiped + recomputed under the new SPADL coords, and ADR-012 §2 grace-period was closed (v2 → v1 XGBoost feature-list fallback removed). Daily mega-job ran 33/33 task SUCCESS post-merge.

Group B is the model-retrain cycle. Every model whose training data flows from `fct_action_values` carries weights fit against broken-coord SPADL — including VAEP, xG v2, ExT v2 P0+P1, DEFCON-lite, OBSO, PAUSA, Football2Vec v1+v2+360, and ScoutGPT. xT v1 production grid was already wiped + recomputed during SK3-MIG-A's full-refresh; it consumed canonical-LTR `fct_action_values` and is current. Model predictions sit on Lakebase synced tables that need refresh + index restoration to surface new weights to the Taipy app. HF datasets (`spadl-vaep`, `xg-shots`, `freeze-frame`, `shots-on-target`, `obso-pausa-inputs`, `football2vec-player-embeddings`) carry parquet payloads built against the broken-coord SPADL and need republish.

Two follow-ups fold in cleanly: **XG1-RETIRE** (TODO row) — the v2 → v1 fallback removal in SK3-MIG-A made v1 dead-code from the inference path, retraining v1 against canonical-LTR data is wasted spend; v1 is deleted instead. **HF4** (TODO row) — two notebook-style HF publishers under `notebooks/` sit outside ADR-014's `upload_hf_readme` parity-test net; SK3-MIG-B migrates them to PEP 723 scripts so the orchestrator's republish flow is uniform.

## §1 — Scope

### 1.1 PR-α (`sk3-mig-b`, single squash)

The bulk of the cycle. **11 cycle items** — 8 trained-model retrains (xG v1 dropped per XG1-RETIRE; ExT v2 P0 and P1 counted as separate phases each with its own smoke gate) plus 3 compute-only re-runs (DEFCON-lite, OBSO, PAUSA recompute predictions from new `fct_action_values` without weight fitting). 6 HF dataset republishes, HF model card refreshes, MLflow Champion promotion + UC Volume sync per trained model, full XG1-RETIRE execution, full HF4 migration, per-cycle-item E2E test loop including Lakebase synced refresh + index restoration, daily-job manual trigger for end-to-end verification.

#### 1.1.1 Cycle items (11 — 8 trained + 3 compute-only)

| Model | Training script | Venue | Estimated wall-clock |
|---|---|---|---|
| VAEP | `scripts/train_vaep_model_hf.py` | HF Jobs cpu-basic | ~5 min |
| xG v2 | `scripts/train_xg_v2_hf.py` | HF Jobs l40sx1 | ~15 min |
| ExT v2 P0 | `src/analytics/ext_v2/` (Singh baseline) | local Win11 96GB | seconds |
| ExT v2 P1 | `src/analytics/ext_v2/` (KDE Optuna 200 trials) | local Win11 96GB | ~135 min |
| DEFCON-lite | `wf-defcon` Databricks workflow (compute-only, no model fitting) | Databricks workflow | per daily-job |
| OBSO | `wf-obso-pausa` Databricks workflow | Databricks workflow | per daily-job |
| PAUSA | `wf-obso-pausa` Databricks workflow | Databricks workflow | per daily-job |
| Football2Vec v1 | `scripts/train_football2vec.py` (NEW — migrated from `notebooks/train_football2vec.py` as part of HF4 expansion; see §4.2) | HF Jobs gpu-medium | ~30 min |
| Football2Vec v2 | `scripts/train_football2vec_v2.py` | HF Jobs gpu-medium | ~45 min |
| Football2Vec 360 | `scripts/train_football2vec_360.py` | HF Jobs gpu-medium | ~60 min |
| ScoutGPT | `wf-scoutgpt-export` (re-tokenize) → `scripts/train_scoutgpt_hf.py` | Databricks workflow → HF Jobs gpu-large | ~3-4 hr |

xT v1 production grid is **out of scope** — already shipped under SK3-MIG-A's full-refresh.

#### 1.1.2 HF dataset republishes (8)

All overwrite same repo names (established lakehouse pattern; no version tag bump). All go through PEP 723 scripts post-HF4 migration (no notebook publishers remain). All 8 publishers call `ingestion.hf_publish.upload_hf_readme` per ADR-014 — verified at plan-write time and pinned by the §4.2 #2 invariant test.

| # | Dataset | PEP 723 publisher | Depends on |
|---|---|---|---|
| 1 | `spadl-vaep` | `scripts/publish_spadl_vaep_hf.py` (existing) | VAEP retrain |
| 2 | `xg-shots` | `scripts/publish_xg_shots_hf.py` (existing) | xG v2 retrain |
| 3 | `freeze-frame` | `scripts/publish_freeze_frame_hf.py` (existing) | independent (re-export from current `fct_action_values`) |
| 4 | `shots-on-target` | `scripts/publish_shots_on_target_hf.py` (existing) | independent |
| 5 | `obso-trained-grids` | `scripts/compute_epv_transition_hf.py` (existing — the "training" half of `wf-epv-reachability`; `upload_hf_readme` at line 276) | `spadl-vaep` republish (#1 consumes the new SPADL coords) |
| 6 | `obso-pausa-inputs` | `scripts/publish_obso_pausa_inputs_hf.py` (NEW from HF4 §4.2) | new SPADL coords (re-export of IDSSE events + ELASTIC sync) |
| 7 | `obso-pausa-values` | `scripts/compute_obso_hf.py` (existing — the "training" half of `wf-obso-pausa`; `upload_hf_readme` at line 710) | `obso-trained-grids` (#5) + `obso-pausa-inputs` (#6) |
| 8 | `football2vec-player-embeddings` | `scripts/publish_football2vec_embeddings_hf.py` (NEW from HF4 §4.2) | F2V family retrain |

**Architectural note** — `wf-epv-reachability` and `wf-obso-pausa` workflow cards declare both a `training` phase (PEP 723 script on HF Jobs that PRODUCES the HF dataset) and an `inference` phase (Databricks workflow that CONSUMES the HF dataset and writes Delta marts). SK3-MIG-B Group 3 fires the `training` phase scripts directly to refresh the HF datasets; the `inference` phase fires later as part of Step 1's compute-only re-runs (§5.1) consuming the freshly-republished datasets. There is no "workflow-internal publisher" — every HF upload goes through a PEP 723 script in `scripts/`.

**OBSO ecosystem dependency chain** (sequential, enforced by orchestrator step graph in §5.1):
1. `spadl-vaep` republished (consumes new `fct_action_values`).
2. `obso-trained-grids` recomputed via `compute_epv_transition_hf.py` (consumes #1).
3. `obso-pausa-inputs` re-exported via `publish_obso_pausa_inputs_hf.py` (independent; consumes new SPADL coords directly).
4. `obso-pausa-values` recomputed via `compute_obso_hf.py` (consumes #2 + #3).
5. PAUSA inference (`wf-obso-pausa` Databricks-workflow phase) reads #4 and writes `fct_pausa_values` mart.

**Tracking datasets NOT republished in this cycle:** `pitch-control-tracking` and `line-breaking-passes` are tracking-data convenience mirrors. Per SK3-MIG-A §1.3, tracking adapters are pinned to `output_convention="absolute_frame"` and unaffected by SK3-MIG-A's SPADL-LTR migration. HF4 (§4.2) creates the publisher scripts for these datasets so they exist in the canonical PEP 723 inventory, but the orchestrator does NOT fire them as part of SK3-MIG-B Group 3.

Each publisher MUST call `ingestion.hf_publish.upload_hf_readme(...)` per ADR-014. Card content updated: "Last refreshed: 2026-05-XX, post-SK3-MIG-A direction-of-play migration. All coords in canonical SPADL-LTR."

#### 1.1.3 HF model card refreshes

For every retrained model, the HF model card under `docs/huggingface/model-cards/` is updated with: wheel SHA at retrain time, retrain date, new metric values from the smoke gate, governance YAML block (per CLAUDE.md AI Governance rule). Refresh happens via the publisher's `upload_hf_readme` call — no manual `shutil.copy2`.

#### 1.1.4 XG1-RETIRE

Full retire of xG v1 inference path. Detail in §6. **Wheel surface change:** `src/ingestion/xg_model.py` deletion removes a public-via-Hyrum's-Law import target. PR-α bumps the wheel **0.3.30 → 0.3.31** (patch bump per `bump_wheel.py`) — declared in §5.1 Step 0 pre-flight as the one expected wheel change in this cycle.

#### 1.1.5 HF4 migration (expanded scope)

Notebook → PEP 723 migration. **Expanded from the original HF4 TODO row to also include `notebooks/train_football2vec.py` → `scripts/train_football2vec.py`** (the F2V v1 trainer was discovered in `notebooks/` during external review — Group 2 dispatch on HF Jobs requires a PEP 723 trainer). Detail in §4.2.

#### 1.1.6 Lakebase synced refresh + index restoration (per-model E2E)

For each retrained model, after Champion promotion + mart write, the orchestrator triggers `scripts/refresh_synced_tables.py` for each affected synced table, then `scripts/maintain_synced_tables.py --skip-refresh` to restore PG indexes that get dropped on synced-table recreation (per CLAUDE.md Lakebase Ops standard).

Synced tables affected per model (preliminary; verify at plan time):

| Model | Synced tables |
|---|---|
| VAEP | `fct_action_values_synced` |
| xG v2 | `fct_xg_predictions_v2_synced` (`fct_xg_predictions_synced` is **DROPPED** by XG1-RETIRE) |
| DEFCON-lite | `fct_defcon_actions_synced`, `fct_defcon_pressure_synced` |
| OBSO/PAUSA | `fct_pausa_values_synced` (+ any OBSO-surfaced synced tables to verify) |
| F2V v1/v2/360 | `fct_player_embeddings_synced`, `fct_player_embeddings_career_synced`, `fct_player_embeddings_season_synced`, `fct_player_embeddings_career_360_synced`, `fct_player_embeddings_season_360_synced` |
| ScoutGPT | verify at plan time — counterfactual outputs may not be synced; if a similarity mart consumes ScoutGPT embeddings, that mart's synced table is included |

### 1.2 PR-β (`sk3-mig-b-baseline-rebase`, single squash)

Close-out PR. Branches off main at PR-α's merge SHA. Five items:

1. `dbt_project/seeds/model_baseline_scalars.csv` rebased from PR-α's measured retrain metrics. Regen via `scripts/regenerate_model_baseline_scalars.py` (NEW, PEP 723) consuming `bronze.model_validation_runs` post-PR-α rows.
2. `docs/performance-baselines.md` refreshed with PR-α's actual retrain wall-clock + cost numbers. Regen via `scripts/regenerate_perf_baselines_md.py` (NEW, PEP 723) cross-referencing `bronze.workflow_costs`.
3. `AI_GOVERNANCE.md` §5 Scope review (PR-α already updated each model card; PR-β verifies §5 row count + `Next review` dates within 30-day grace per `test_ai_governance_md.py`).
4. `TODO.md` cleanup: **remove** SK3-MIG-B / XG1-RETIRE / HF4 rows entirely (NOT strikethrough — per the standing order on completed-task removal). Update `Last updated:` line.
5. Memory updates: drop `project_sk3_mig_complete.md`, write `project_sk3_mig_b_complete.md` successor, update `MEMORY.md` index. Done post-PR-β-merge per `feedback_no_commits_without_explicit_approval` (memory writes are not part of the code commit).

### 1.3 Out of scope (NOT deferred — these are SK3-MIG-A-or-already-done)

- xT v1 production grid recompute (SK3-MIG-A).
- `bronze.spadl_actions` / `fct_action_values` rebuild (SK3-MIG-A).
- silly-kicks 3.0.0 / 3.0.1 pin or wheel work (SK3-MIG-A).
- ADR-012 §2 grace-period closure (SK3-MIG-A).
- Mart-tag classification + 3-stage `dbt_build` restructure (PR-Cycle-C).

## §2 — Retrain dependency graph

```
GROUP 0 (already done — verify only)
└── xT v1 production grid

GROUP 1 (action-value family — independent of each other; all consume fct_action_values)
├── VAEP                 HF Jobs cpu-basic       ~5 min
├── xG v2                HF Jobs l40sx1          ~15 min
├── DEFCON-lite          Databricks workflow     compute-only
├── OBSO                 Databricks workflow     compute-only
├── PAUSA                Databricks workflow     compute-only (same wf-obso-pausa)
└── ExT v2 P0+P1         local Win11 96GB        ~135 min (P1 Optuna)

GROUP 2 (embedding family — checked: ScoutGPT does NOT depend on F2V weights;
         ScoutGPT consumes raw SPADL action sequences from wf-scoutgpt-export)
├── Football2Vec v1      HF Jobs gpu-medium      ~30 min
├── Football2Vec v2      HF Jobs gpu-medium      ~45 min
├── Football2Vec 360     HF Jobs gpu-medium      ~60 min
└── ScoutGPT             HF Jobs gpu-large       ~3-4 hr
                         (requires wf-scoutgpt-export rerun first)

GROUP 3 (HF dataset republishes — depend on PR-α's retrained models being Champion-promoted; ALL via PEP 723 scripts in scripts/, all calling upload_hf_readme per ADR-014; 8 datasets total)
├── spadl-vaep                       (depends on VAEP retrain)
├── xg-shots                         (depends on xG v2 retrain)
├── freeze-frame                     (independent — re-export from new fct_action_values)
├── shots-on-target                  (independent — same)
├── obso-trained-grids               (depends on spadl-vaep republish — chain step 2)
├── obso-pausa-inputs                (independent — uses NEW HF4 script)
├── obso-pausa-values                (depends on obso-trained-grids + obso-pausa-inputs — chain step 4)
└── football2vec-player-embeddings   (depends on F2V family — uses NEW HF4 script)
```

Total wall-clock: Group 1 ~135 min serial (ExT v2 P1 dominates) but parallelisable to ~15 min via concurrent HF Jobs queue. Group 2 dominated by ScoutGPT at ~3-4 hr. Group 3 republishes minutes each. Net: **~5-6 hr HF Jobs wall-clock + ~135 min local**. Cost estimate: **~$25-40** (ScoutGPT dominant; F2V family ~$10; xG v2 ~$1; everything else negligible).

Group 1 must complete + smoke-pass before Group 2 dispatches. ScoutGPT specifically — the most expensive retrain — only fires after Group 1 full success. This sequencing minimises wasted spend on a hung-upstream scenario.

## §3 — Per-model sanity gates (B-pattern acceptance)

Each retrain has an absolute physical sanity gate written as a pytest-style script under `src/tests/sk3_mig_b/test_<model>_post_retrain_smoke.py`. The orchestrator invokes the script after Champion promotion + mart write, **before** Lakebase synced refresh — so a smoke failure halts the cycle without polluting Lakebase.

| Model | Gate | Threshold source |
|---|---|---|
| **VAEP** | per-action `vaep_value` distribution mean within ±50% of Singh-2018 published per-action ballpark on a 1k-action StatsBomb sample; 0% NaN; 100% within `[-1, 1]` | Singh 2018 + Decroos 2019 VAEP paper |
| **xG v2** | held-out ECE < 0.05 against StatsBomb shots-on-target eval fold; 100% predictions in `[0, 1]`; CI band `xg_ci_upper - xg_ci_lower` median > 0; `feature_names` envelope present | xG calibration literature (ECE < 0.1 standard; 0.05 since v2 has CI bands per ADR-012 §2) |
| **ExT v2 P0** | NLL ≤ 3.7892 + 1% (matches PR #206 production baseline) | Phase 0 stop condition pre-registered |
| **ExT v2 P1** | NLL ≤ 3.7482 + 1% (matches PR #213 production baseline) | Phase 1 stop condition pre-registered |
| **DEFCON-lite** | per-team-match credit-assignment sum within ±10% of expected aggregate; 0% NaN; 100% rows have valid `defending_player_id` | DEFCON paper (Bauer 2024) physical bounds — verify Bauer 2024 is in `ARCHITECTURE.md` Appendix D + `expected_authors` in `test_architecture_md_appendix.py` (academic-reference audit per CLAUDE.md). PR-α's pre-merge sweep includes this check. |
| **OBSO** | per-frame surface integrates to 1.0 within ±0.01; 0% NaN | Spearman 2018 OBSO definition |
| **PAUSA** | per-action `pausa_value` ∈ `[0, 1]` for 100% rows; 0% NaN; mean within Singh-PAUSA ballpark | PAUSA paper |
| **F2V v1** | nearest-neighbor recall@10 > 0.7 on a fixed 100-player eval fold (pre-registered IDs); 0% NaN in 32-d embeddings; cosine norms in `[0.95, 1.05]` | Football2Vec paper + project memory's prior recall numbers |
| **F2V v2** | recall@10 > 0.7 against v2 eval fold; 192-d dim; same norm + NaN checks | same |
| **F2V 360** | recall@10 > 0.7 against 360 eval fold; 192-d dim; same norm + NaN checks | same |
| **ScoutGPT** | held-out test_top1 > 0.80 (PR #176 baseline 0.842 — 2pp tolerance); counterfactual rho > 0.20 (PR #176 baseline 0.247 — 2pp tolerance); 0% NaN in logits; vocab_size=23 unchanged | PR #176 close-out validation set |

**Existing gate code to extract** (no logic change, just a non-evolve / non-training-time wrapper):

- xG v2 ECE check is implicit in `train_xg_v2_hf.py`'s validation block — extract to standalone post-Champion smoke script.
- ExT v2 P0/P1 stop conditions exist as pytest-style in `src/analytics/ext_v2/` — reuse against new `fct_action_values`.
- F2V eval fold exists in `src/evolve/targets/football2vec/evaluator.py` — extract to non-evolve smoke wrapper.
- ScoutGPT eval set + top-k computation exists in `train_scoutgpt_hf.py` — extract.

**New gate scripts to write** (~50-100 LOC each): VAEP, DEFCON-lite, OBSO, PAUSA.

Smoke gate failure → orchestrator halts → emits restore command for the prior Champion's MLflow version + UC Volume version.

## §4 — HF dataset republish + HF4 migration

### 4.1 Republish strategy

All 6 datasets republish to the same HF repo names (no version tag bump; established pattern). Each publisher script:

1. Reads from current `fct_action_values` / mart via `WorkspaceClient.statement_execution` (per `reference_sdk_over_sql_connector.md`).
2. Builds parquet payload.
3. Uploads to HF.
4. Calls `ingestion.hf_publish.upload_hf_readme(...)` per ADR-014 (filename == repo basename invariant).

### 4.2 HF4 fold-in: notebook → PEP 723 migration (expanded)

**Notebook deletions (3 files):**

- `notebooks/publish_datasets.py` — multi-dataset publisher.
- `notebooks/publish_obso_data.py` — OBSO inputs publisher.
- `notebooks/train_football2vec.py` — F2V v1 trainer (discovered during external review; Databricks-notebook with hardcoded workspace path `/Workspace/Users/karstenskyt@gmail.com/luxury-lakehouse/src` — can't run on HF Jobs).
- `notebooks/train_xg_model.py` — xG v1 trainer (deletion folded into XG1-RETIRE per §6.1).

**New PEP 723 scripts (5 files):**

- `scripts/publish_line_breaking_passes_hf.py` (NEW — created so notebooks can be deleted; not fired by SK3-MIG-B orchestrator since tracking data is not coord-dependent per SK3-MIG-A §1.3).
- `scripts/publish_pitch_control_tracking_hf.py` (NEW — same rationale).
- `scripts/publish_football2vec_embeddings_hf.py` (NEW — fired by SK3-MIG-B Group 3).
- `scripts/publish_obso_pausa_inputs_hf.py` (NEW — fired by SK3-MIG-B Group 3).
- `scripts/train_football2vec.py` (NEW — F2V v1 trainer; PEP 723 single-file replacement for `notebooks/train_football2vec.py`; uses `huggingface_hub.get_token()` per ADR-012 delivery contract; calls `set_and_verify_mlflow_champion` + `upload_weights_to_uc_volume`).

Duplicate cells in `notebooks/publish_datasets.py` covering `spadl-vaep-action-values` and `xg-freeze-frame-data` are deleted (use the canonical `scripts/publish_spadl_vaep_hf.py` and `scripts/publish_freeze_frame_hf.py`). The v1 model-card manual push at `notebooks/publish_datasets.py:298+` deletes itself with XG1-RETIRE.

**Three CI invariants enforce the discipline going forward:**

1. **Notebook-publisher ban** — `src/tests/test_no_notebook_hf_publishers.py` — AST-walks `notebooks/publish_*.py` AND `notebooks/train_*.py`, fails on `huggingface_hub.HfApi` / `api.upload_folder` / `api.upload_file` / `mlflow.register_model` / `mlflow.set_registered_model_alias` calls. Scope is intentionally narrow (`publish_*.py` and `train_*.py` only) so legitimate non-publishing notebooks like `notebooks/sync_hf_weights.py`, `notebooks/import_obso_results.py`, `notebooks/diag_*.py` are not falsely flagged. Cleanest enforcement is "no `notebooks/publish_*.py` and no `notebooks/train_*.py` files exist post-HF4" — the AST walk is belt-and-suspenders for any future re-introduction.
2. **`upload_hf_readme` requirement** — extends `src/tests/test_hf_publish_parity.py` to AST-walk `scripts/publish_*_hf.py` and assert every file calls `ingestion.hf_publish.upload_hf_readme`. Closes the parity gap end-to-end.
3. **ADR-014 amendment** — adds: "HF publishers and trainers are PEP 723 scripts in `scripts/`. Notebook publishers and trainers are forbidden; `test_no_notebook_hf_publishers.py` enforces this. Migration: filename == repo basename + `upload_hf_readme` after the data upload, no exceptions."

## §5 — Orchestrator script `scripts/sk3_mig_b_retrain.py`

PEP 723 single-file, idempotent, `--start-at <step>` resumable, `--dry-run` skips actual training. Module-level `_COST_CAP_USD = 80.0` and `_WALLTIME_CAP_HOURS = 8.0` constants. Cycle log written to `bronze.sk3_mig_b_runs` Delta table (durable, queryable; replaces v1 spec's gitignored sidecar — see §5.3).

### 5.0 PR-α code commits — what must be in the working tree before the orchestrator first fires

**Phase boundary**: the orchestrator is a runtime tool. It cannot edit Python files, bump the wheel, write smoke gate scripts, or migrate notebook publishers. Every code change in this cycle must be a PR-α commit landed BEFORE the orchestrator runs. The orchestrator's pre-flight (§5.1 Step 0) verifies these commits landed.

**Required PR-α commits (in working tree before any orchestrator dispatch):**

| Bucket | Items |
|---|---|
| **Wheel** | `pyproject.toml` + `bump_wheel.py` output bumped 0.3.30 → 0.3.31 (XG1-RETIRE wheel surface change per §1.1.4) |
| **HF4 migration** | 5 NEW PEP 723 scripts: `publish_line_breaking_passes_hf.py`, `publish_pitch_control_tracking_hf.py`, `publish_football2vec_embeddings_hf.py`, `publish_obso_pausa_inputs_hf.py`, `train_football2vec.py`. 4 notebook deletions: `notebooks/publish_datasets.py`, `notebooks/publish_obso_data.py`, `notebooks/train_football2vec.py`, `notebooks/train_xg_model.py` (XG1-RETIRE) |
| **Smoke gate scripts** | `src/tests/sk3_mig_b/test_<item>_post_retrain_smoke.py` × 11 (per-cycle-item gates from §3) |
| **Regression tests** | `src/tests/test_no_notebook_hf_publishers.py`, `src/tests/test_hf_publish_parity.py` extension, `src/tests/test_xg_v1_retired.py`, `src/tests/test_shot_map_v2_columns.py` |
| **XG1-RETIRE source-code deletions** | `src/ingestion/xg_model.py`, `scripts/train_xg_model_hf.py`, `pyproject.toml` v1 entry-point line |
| **XG1-RETIRE dbt deletions** | `dbt_project/models/marts/fct_xg_predictions.sql`, `dbt_project/models/staging/xg/stg_xg__predictions.sql`, `_xg__sources.yml` v1 entries, `_marts__models.yml` v1 mart contract entry |
| **XG1-RETIRE workflow + Terraform deletions** | `workflow-cards/wf-xg-v1.yaml` deletion, Terraform job declaration for v1 deletion (TF apply happens at runtime — Step 4 — but the .tf file edit is a PR-α commit) |
| **XG1-RETIRE doc deletions** | `docs/huggingface/model-cards/xg-model-statsbomb-wyscout.md` (v1 card), `docs/huggingface/org-card.md` v1 listing removal, `README.md` HF artifact list update, HF Space header + footer updates, `AI_GOVERNANCE.md` §5 Scope row removal |
| **Shot Map UI migration** | `hf_taipy_app/src/state/shot_map.py` column migration (v1 → v2 columns), `hf_taipy_app/src/queries/shots.py::fetch_xg_predictions()` deletion, glossary entry per CLAUDE.md UX standard |
| **Orchestrator + telemetry table** | `scripts/sk3_mig_b_retrain.py` itself; `scripts/migrations/2026-05-03-create-bronze-sk3-mig-b-runs.sql` (one-time DDL migration creating `bronze.sk3_mig_b_runs` per §5.3 schema; auto-applied by the live-CI bronze-migrations runner). **NOT a dbt model** — operator-written telemetry tables use the DDL-migration path; a dbt model would be wiped on every `dbt run`, destroying orchestrator-written rows. ADR-002 §4 schema-drift guard applies (see below). |
| **ADR-002 §4 telemetry schema discipline** | Module-level schema constant `_SK3_MIG_B_RUNS_COLUMNS` in `src/ingestion/sk3_mig_b_telemetry.py`, lazy factory function converting it to a Spark `StructType`, and pytest `src/tests/test_sk3_mig_b_runs_schema_parity.py` parsing the migration DDL and asserting column-list equality with the constant. Resolves §10 Q7 in-spec. |
| **ADR-014 amendment** | `docs/superpowers/adrs/ADR-014-hf-card-inventory-parity.md` amendment per §4.2 |

**What runs at orchestrator-runtime (NOT in PR-α commit):**

| Bucket | Items |
|---|---|
| **Training** | HF Jobs dispatch for VAEP / xG v2 / F2V family / ScoutGPT; local Win11 dispatch for ExT v2 P0+P1 |
| **Champion + UC Volume** | MLflow Champion alias set + UC Volume weight upload per `artifact_deploy.py` (ADR-012) |
| **Mart writes** | Inference workflow triggers (HF Jobs runtime for trained models; mega-job dispatch for compute-only items) |
| **HF dataset republishes** | 8 PEP 723 publisher invocations (§5.1 Step 3) |
| **Lakebase synced refresh + index restore** | `refresh_synced_tables.py`, `maintain_synced_tables.py --skip-refresh` per affected synced table |
| **XG1-RETIRE runtime** | `terraform apply` for v1 job deletion, MLflow v1 model wipe, UC Volume v1 weights wipe, Lakebase synced table drop, physical mart `DROP TABLE` |
| **Daily mega-job manual trigger** | `databricks.jobs.run_now(job_id=$MEGA_JOB_ID)` post-XG1-RETIRE for end-to-end verification |

PR-α MERGES first. The operator then invokes the orchestrator. The orchestrator never modifies committed code.

### 5.1 Step graph

```
Step 0  : Pre-flight gates — verify §5.0 PR-α commits all landed before any dispatch
            - silly-kicks 3.0.1+ in env (assert)
            - Wheel == 0.3.31 (verifies the PR-α commit per §5.0 actually merged and
              the local working tree matches main HEAD). Mismatch halts with
              "PR-α wheel bump not present — confirm PR-α merged and pull main."
            - PR-α file presence: assert all NEW files in §5.0's table exist on
              disk (5 new HF4 publishers + 11 smoke gate scripts + 4 regression
              tests + orchestrator script + bronze.sk3_mig_b_runs DDL applied).
              Assert all DELETIONS in §5.0 are absent (XG1-RETIRE source/dbt/
              workflow files + 4 notebook files). Halts on mismatch with
              "PR-α commit incomplete — see §5.0 inventory."
            - DATABRICKS_TOKEN, MLFLOW_TRACKING_URI, HF_TOKEN present (not empty)
            - fct_action_values max(updated_at) > SK3-MIG-A merge SHA aa0237f commit time
            - bronze.workflow_costs cost-hook coverage check: query
              `select distinct workflow_id from bronze.workflow_costs where started_at > NOW() - INTERVAL 7 DAYS`
              and assert HF Jobs workflow_ids appear (e.g., wf-football2vec-v2,
              wf-xg-v2, wf-scoutgpt). If only Databricks-workflow workflow_ids are
              present, the $80 cost cap (§9.5) is theatrical for HF-Jobs-dominant
              spend. Halts and emits "extend cost hook to HF Jobs before
              proceeding" on failure.
            - Capture pre-state DESCRIBE HISTORY versions of affected gold marts:
              fct_action_values, fct_xg_predictions_v2, fct_passes, fct_player_embeddings,
              fct_player_embeddings_career, fct_player_embeddings_season,
              fct_player_embeddings_career_360, fct_player_embeddings_season_360,
              fct_pausa_values, fct_defcon_actions, fct_defcon_pressure
              → write to bronze.sk3_mig_b_runs Delta as the cycle's pre-state row
              (cycle_item="pre_state", cycle_item_kind="meta_event").

Step 1  : Group 1 cycle items (mixed dispatch + serial smoke gates)
            Trained models (HF Jobs / local) — dispatch directly:
              VAEP (HF Jobs)
              xG v2 (HF Jobs)
              ExT v2 P0 (local Win11) — instant
              ExT v2 P1 (local Win11) — sequential after P0 Champion + smoke gate
                                        pass (P0 and P1 share the local venue,
                                        cannot run in parallel)
            Compute-only re-runs — mega-job dispatch:
              DEFCON-lite | OBSO | PAUSA
            **Mega-job orchestrator rule (load-bearing):** Lakehouse uses ONE
            mega-job named `soccer-analytics-ingestion-dev` (~33 task_keys
            covering every workflow card). Standalone job dispatch by
            individual task name fails with "job not found" because there are
            no standalone jobs per workflow card. The orchestrator triggers
            the FULL mega-job and relies on per-task skip-guards (every
            workflow card declares an `idempotency:` block; tasks that have
            already-fresh outputs no-op). For Group 1's compute-only re-runs,
            this means: the orchestrator triggers the daily mega-job once per
            cycle item ONLY IF skip-guards would otherwise cause the task to
            no-op against stale state. Concretely: for DEFCON/OBSO/PAUSA, the
            new fct_action_values has invalidated existing predictions, so
            the skip-guard does NOT skip and the task re-runs. The orchestrator
            issues `databricks.jobs.run_now(job_id=$MEGA_JOB_ID)` and waits
            for the specific task_key to complete (polls
            `system.lakeflow.job_task_run_timeline`).

            Per-cycle-item E2E loop (§5.2 below).

Step 2  : Group 2 cycle items (HF Jobs parallel + serial smoke gates)
            F2V v1 | F2V v2 | F2V 360 — all HF Jobs gpu-medium, parallel dispatch.
            wf-scoutgpt-export → ScoutGPT train (sequential):
              - wf-scoutgpt-export is a Databricks workflow task — triggered
                via the mega-job pattern from Step 1.
              - ScoutGPT train is HF Jobs gpu-large — direct dispatch after
                export completes.
            Per-cycle-item E2E loop.

Step 3  : Group 3 HF dataset republishes (8 datasets via 8 PEP 723 scripts)
            Sequential to keep HF Hub rate-limits clean. Order respects the
            OBSO ecosystem dependency chain (§1.1.2):
              1. spadl-vaep                      (publish_spadl_vaep_hf.py)
              2. xg-shots                        (publish_xg_shots_hf.py)
              3. freeze-frame                    (publish_freeze_frame_hf.py)
              4. shots-on-target                 (publish_shots_on_target_hf.py)
              5. obso-pausa-inputs               (publish_obso_pausa_inputs_hf.py — NEW)
              6. obso-trained-grids              (compute_epv_transition_hf.py — depends on #1)
              7. obso-pausa-values               (compute_obso_hf.py — depends on #5+#6)
              8. football2vec-player-embeddings  (publish_football2vec_embeddings_hf.py — NEW)
            Each script calls upload_hf_readme per ADR-014. Each verifies
            dataset card README parity post-upload (the script's own assertion).
            Step 3 captures pre-revision SHA per dataset before each upload
            into bronze.sk3_mig_b_runs.pre_hf_revision_sha for §9.3 rollback.

Step 4  : XG1-RETIRE runtime execution (NOT code commits — those are PR-α per §5.0)
            Source-code deletions, dbt model deletions, workflow YAML deletions,
            doc deletions, Shot Map UI migration are ALL PR-α commits per §5.0.
            Step 4 fires the runtime parts only:
              - terraform apply (drops v1 job declaration / task_keys from mega-job)
              - MLflow v1 registered model wipe (all versions)
              - UC Volume v1 weights folder wipe
              - Lakebase fct_xg_predictions_synced + indexes drop via
                scripts/delete_synced_table.py
              - Physical mart DROP TABLE IF EXISTS dev_gold.fct_xg_predictions
            **Ordering rationale:** Step 6's daily mega-job manual trigger fires
            AFTER Step 4. If terraform apply happened after Step 6, the mega-job
            would attempt to dispatch deleted v1 task_keys and fail with "task
            not found." Step 4 must complete (including terraform apply) before
            Step 6 dispatches the mega-job.
          §6 detail covers the full deletion ordering (8 numbered steps; some
          are PR-α commits, some are Step 4 runtime — explicitly labeled).

Step 5  : HF4 cleanup verification
            - notebooks/publish_datasets.py deleted
            - notebooks/publish_obso_data.py deleted
            - test_no_notebook_hf_publishers.py PASS
            - test_hf_publish_parity.py extended check PASS

Step 6  : Final verification sweep
            - test_ai_governance_md.py PASS
            - test_architecture_md_appendix.py PASS (Bauer 2024 added if absent — see §3)
            - test_topandas_boundedness.py PASS (no expected change)
            - test_xg_v1_retired.py PASS (NEW)
            - test_shot_map_v2_columns.py PASS (NEW)
            - Daily mega-job manual trigger — wait for completion
            - All remaining task_keys SUCCESS (post-XG1-RETIRE delete count is N-1)
          **Irreversibility note:** XG1-RETIRE deletions are committed code +
          Terraform-applied at Step 4. A daily-job failure at Step 6 cannot
          roll back the v1 retire — it must roll forward via incremental
          fixes. State explicitly: "XG1-RETIRE is irreversible by design once
          Step 4 completes; Step 6 failures are roll-forward, not rollback."
```

### 5.2 Per-cycle-item E2E loop (Step 1, 2, 3 inner pattern)

The loop has two flavors — **trained-model** items (8) follow the full 12-step path; **compute-only** items (3 — DEFCON-lite, OBSO, PAUSA) skip steps 4-5 (no MLflow Champion, no UC Volume weights — these models recompute predictions from new fct_action_values without weight fitting) and step 7's smoke-gate failure emits a different rollback command (mart RESTORE rather than MLflow alias revert; see §9.1).

```
1. Cost-cap check : query bronze.workflow_costs cycle aggregate.
                    If > _COST_CAP_USD: HALT. Print breakdown + resume command:
                      python scripts/sk3_mig_b_retrain.py --start-at <item> --override-cost-cap
                    Operator must explicitly re-invoke with --override-cost-cap to proceed.
                    Re-invocation IS the permission grant (no stdin prompt — headless contexts).
2. Walltime-cap   : if single cycle-item exceeds _WALLTIME_CAP_HOURS: HALT.
                    Resume via --override-walltime-cap.
3. Train          : invoke training script (HF Jobs / local).
                    SKIPPED for compute-only items.
4. Promote        : Champion alias set + verified via
                    artifact_deploy.set_and_verify_mlflow_champion (ADR-012).
                    SKIPPED for compute-only items.
5. Sync weights   : artifact_deploy.upload_weights_to_uc_volume (ADR-012).
                    SKIPPED for compute-only items.
6. Inference      : trigger inference workflow → mart write.
                    For trained models: HF Jobs runtime calls inference path
                    against new Champion + writes to gold mart.
                    For compute-only items: mega-job dispatch of the workflow's
                    task_key; per-task skip-guard sees stale predictions vs.
                    new fct_action_values and re-runs.
7. Smoke gate     : run src/tests/sk3_mig_b/test_<item>_post_retrain_smoke.py.
                    HALT on fail.
                    For trained-model items: emit prior-Champion + UC Volume
                      version restore command.
                    For compute-only items: emit
                      `RESTORE TABLE {catalog}.dev_gold.fct_<X> TO VERSION AS OF <pre_state_version>`
                      (pre-state version captured in Step 0).
8. Republish      : trigger HF publisher script for any dataset depending on this item
                    (Group 3 step issues these; Step 1/2 don't republish themselves).
9. Sync refresh   : refresh_synced_tables.py for each affected synced table.
10. Index restore : maintain_synced_tables.py --skip-refresh (CLAUDE.md Lakebase Ops).
11. Lakebase verify: smoke SQL — row count parity gold ↔ synced; sample row's prediction
                     column matches new value (not stale).
12. Record        : append per-item timing + cost + smoke-metric values to
                    bronze.sk3_mig_b_runs Delta table (NOT a gitignored sidecar
                    file — see §5.3 below for the durability rationale).
```

### 5.2.1 Long-running execution — orchestrator runs as background process

Per CLAUDE.md "Failure Investigation Protocol" rule: *"Never disappear into long-running commands: any command that may take >30 seconds MUST use `run_in_background: true` so the user sees responses while it runs."*

SK3-MIG-B's per-cycle-item runtimes far exceed 30s — mega-job triggers run 30-90+ min, ScoutGPT 3-4 hr, F2V variants 30-60 min each. The orchestrator MUST run as a background process; the operator polls progress rather than blocks the terminal.

**Concrete runtime contract:**

- Orchestrator dispatched via `Bash` tool with `run_in_background=true` (or equivalent in the operator's shell — `nohup ... &`, tmux, etc.).
- Orchestrator writes a status line every 60-120s to **two destinations**:
  - **stdout** (line-buffered) — captured by the background process's output file; operator polls via `tail -f` on the captured file.
  - **`bronze.sk3_mig_b_runs`** — every status update appends a row (or updates the in-progress row) so post-hoc queries reconstruct the cycle.
- Status line format: `[YYYY-MM-DDTHH:MM:SSZ] cycle=<cycle_id> step=<step> item=<cycle_item> phase=<dispatched|running|smoke_pending|smoke_pass|complete> elapsed=<HH:MM:SS> hf_job_id=<id_or_null>`.
- Orchestrator never reads stdin — all decisions come from CLI flags + sidecar state. Halt-and-resume flow (cost cap, walltime cap, smoke gate failure) emits a structured halt record + the resume command, then exits with non-zero status.

The operator's polling pattern (CLAUDE.md compliant): start orchestrator → wait notification of completion / halt → check `bronze.sk3_mig_b_runs ORDER BY recorded_at DESC LIMIT 5` for current state → either the cycle finished or the operator decides whether to issue the resume command.

### 5.3 Cycle log destination — `bronze.sk3_mig_b_runs` Delta table

The original v1 spec stashed cycle state in a gitignored `sk3_mig_b_log.json` sidecar file. External review flagged this as fragile: PR-β consumes the log to populate `model_baseline_scalars.csv` + `docs/performance-baselines.md`, and a "operator copies into working dir manually" hand-off can silently produce wrong values if the file is nuked between PR-α merge and PR-β branch creation.

**Replacement:** the orchestrator writes per-cycle-item rows to a Delta table `bronze.sk3_mig_b_runs` at the end of each loop iteration (Step 12). Schema:

```sql
CREATE TABLE IF NOT EXISTS bronze.sk3_mig_b_runs (
  cycle_id STRING,                  -- e.g., "sk3-mig-b-2026-05-03"
  cycle_started_at TIMESTAMP,
  cycle_finished_at TIMESTAMP,
  wheel_at_start STRING,            -- "0.3.30"
  wheel_at_end STRING,              -- "0.3.31" post XG1-RETIRE bump
  silly_kicks_version STRING,
  cost_cap_usd DOUBLE,
  walltime_cap_hours DOUBLE,
  -- Per-cycle-item row (one row per item per cycle invocation)
  cycle_item STRING,                -- "vaep", "xg_v2", "ext_v2_p0", "ext_v2_p1",
                                    --  "defcon_lite", "obso", "pausa",
                                    --  "f2v_v1", "f2v_v2", "f2v_360", "scoutgpt",
                                    --  "spadl_vaep_publish", "xg_shots_publish",
                                    --  "freeze_frame_publish", "shots_on_target_publish",
                                    --  "obso_pausa_inputs_publish", "obso_trained_grids_publish",
                                    --  "obso_pausa_values_publish", "f2v_embeddings_publish",
                                    --  "baseline_rebase" (PR-β meta-event)
  cycle_item_kind STRING,           -- "trained_model" | "compute_only" | "publish" | "meta_event"
  hf_job_id STRING,                 -- nullable for compute-only / publish / meta items
  champion_set_at TIMESTAMP,        -- nullable for non-trained items
  pre_mart_version BIGINT,          -- DESCRIBE HISTORY version pre-write (mart items only)
  post_mart_version BIGINT,         -- DESCRIBE HISTORY version post-write (mart items only)
  pre_hf_revision_sha STRING,       -- HF dataset revision SHA pre-republish (publish items only;
                                    --  consumed by §9.3 rollback). NULL for non-publish items.
  smoke_pass BOOLEAN,
  smoke_metrics MAP<STRING, DOUBLE>, -- numeric metrics: {"per_action_mean": 0.0123, "nan_pct": 0.0}
  smoke_metrics_str MAP<STRING, STRING>, -- string metrics (e.g., human-readable
                                    --  status notes, dataset URLs, regen hints).
                                    --  Separate from smoke_metrics to keep
                                    --  the numeric map type-clean for analytics.
  wall_clock_seconds DOUBLE,
  cost_usd DOUBLE,
  recorded_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

PR-β's `regenerate_model_baseline_scalars.py` and `regenerate_perf_baselines_md.py` read this Delta table directly — no operator-copies-the-file step. The table also serves as the durable cycle audit log, queryable via SQL after-the-fact.

The orchestrator additionally writes a transient `sk3_mig_b_log.json` to the operator's local working dir (gitignored) for live monitoring during cycle execution — but this is debug aid only, not the PR-β handoff contract.

A `DESCRIBE HISTORY bronze.sk3_mig_b_runs` query gives the canonical cycle history. The cycle_id is the natural primary key; idempotent re-runs of the orchestrator with `--start-at <item>` write rows under the same cycle_id (queryable via `WHERE cycle_id = ... ORDER BY recorded_at DESC LIMIT 1` for the latest item state).

## §6 — XG1-RETIRE execution detail

Folded into PR-α Step 4. Decision (per Q1 in brainstorm): migrate Shot Map UI to v2 columns rather than DROP (v2's CI band is the strictly-better signal; cleaner per CLAUDE.md UX standard "every displayed value must be interpretable").

### 6.1 Concrete drop ordering (8 steps; PR-α commit vs runtime explicit)

Order matters for FK/grants — out-of-order steps leave a transient corruption window. Each step is labeled **[PR-α commit]** (lands in the working tree before orchestrator dispatch) or **[Step 4 runtime]** (fires during orchestrator execution per §5.1).

1. **[PR-α commit]** Taipy consumers first — delete `hf_taipy_app/src/queries/shots.py::fetch_xg_predictions()` and migrate `hf_taipy_app/src/state/shot_map.py` to v2 columns (§6.2). Once PR-α merges, no live consumer reads from `fct_xg_predictions_synced`.
2. **[Step 4 runtime]** Lakebase synced table — drop `fct_xg_predictions_synced` + indexes via `scripts/delete_synced_table.py`.
3. **[PR-α commit]** dbt model files — full delete:
   - `dbt_project/models/marts/fct_xg_predictions.sql`
   - `dbt_project/models/staging/xg/stg_xg__predictions.sql`
   - `_xg__sources.yml` v1 entries (and any `_marts__models.yml` v1 mart contract entry)
4. **[Step 4 runtime]** Physical mart — `DROP TABLE IF EXISTS dev_gold.fct_xg_predictions`. PR-α's dbt model deletion (step 3) doesn't auto-drop the physical table; the explicit DROP avoids the orphaned-table state.
5. **[Step 4 runtime]** MLflow + UC Volume — wipe v1 MLflow registered model versions; wipe UC Volume v1 weights folder. No code path can reach these post-step-4 (source code is gone in step 6).
6. **[PR-α commit]** Source code — full delete:
   - `src/ingestion/xg_model.py` (wheel surface — triggers wheel bump 0.3.31 per §1.1.4 + §5.0)
   - `scripts/train_xg_model_hf.py`
   - `notebooks/train_xg_model.py` (Databricks-notebook v1 trainer; discovered during external review)
   - `pyproject.toml` v1 entry-point line
7. **[PR-α commit + Step 4 runtime]** Workflow + Terraform:
   - PR-α commit: `workflow-cards/wf-xg-v1.yaml` deletion + Terraform job declaration deletion in `.tf` source files.
   - Step 4 runtime: `terraform apply` actually drops the v1 task_keys from the mega-job (load-bearing per §5.1 Step 4 ordering rationale — must complete before §5.1 Step 6 daily-job manual trigger).
8. **[PR-α commit]** HF model card — `docs/huggingface/model-cards/xg-model-statsbomb-wyscout.md` (v1 card) deletion; orphan check via `scripts/publish_hf_cards.py --orphans` passes locally pre-merge.

### 6.2 Shot Map UI migration

`hf_taipy_app/src/state/shot_map.py`:

- Replace displayed columns: `xg_logistic`, `xg_gradient_boosted` → `xg_set_encoder`, `xg_ci_lower`, `xg_ci_upper`
- Tooltip: "v2 Set Encoder xG with 95% CI from MC dropout (Gal & Ghahramani 2016). Range: 0.0 (no chance) to 1.0 (certain). Wider CI = more model uncertainty about this shot."
- Glossary entry added per CLAUDE.md UX standard ("Computed metrics must show scale and direction").

### 6.3 Documentation + governance

- `docs/huggingface/org-card.md` — remove v1 listing.
- `README.md` HF artifact list — remove v1.
- HF Space header + footer — remove v1.
- `AI_GOVERNANCE.md` §5 Scope — remove xG v1 model card row.

### 6.4 Regression tests

`src/tests/test_xg_v1_retired.py` (NEW) — three explicit assertions:

1. `import ingestion.xg_model` raises `ModuleNotFoundError` (not just a grep — actually exercise the import path so a future leftover `__init__.py` re-exposing the module fails the test).
2. Glob check returns zero hits across `src/`, `scripts/`, `notebooks/`, `dbt_project/`, `terraform/`, `workflow-cards/`, `hf_taipy_app/` for: `xg_model`, `fct_xg_predictions`, `wf-xg-v1`, `xg-model-statsbomb-wyscout`. Prevents accidental re-introduction in any layer.
3. `pyproject.toml` `[project.scripts]` block does NOT contain a `train_xg_model` entry.

`src/tests/test_shot_map_v2_columns.py` (NEW) — Taipy state smoke: `hf_taipy_app/src/state/shot_map.py` references `xg_set_encoder` / `xg_ci_lower` / `xg_ci_upper`, NOT `xg_logistic` / `xg_gradient_boosted`. AST-walk-based to survive whitespace differences.

### 6.5 Pre-merge cognitive-interface-audit

Invoke `mad-scientist-skills:cognitive-interface-audit` against the migrated Shot Map page to verify the CI band's interpretation is clear (per CLAUDE.md UX standard).

## §7 — PR-β scope detail

### 7.1 `model_baseline_scalars.csv` rebase

The original v1 spec proposed a CSV header comment line for freshness tracking. **External review showed this fails at parse time** — dbt's seed parser uses `agate.from_csv` (Python stdlib `csv` underneath) which has no comment-skip option; a `#`-prefixed first line becomes a malformed single-column header that crashes `dbt seed --full-refresh`. Three viable alternatives were evaluated: (a) sidecar metadata file outside dbt's scan path, (b) parallel Delta table for queryable history, (c) add a metadata column to the seed schema. Long-term best practice favors **(a) + (b) combined** — file for CI freshness checks (no Databricks auth needed), Delta table for cross-cycle audit history. Both written atomically by the regen script.

`scripts/regenerate_model_baseline_scalars.py` (NEW, PEP 723) writes to **three artifacts atomically** (all-or-nothing — partial write halts the script with a clear error so the operator never gets a divergent state):

**(1) The seed CSV** at `dbt_project/seeds/model_baseline_scalars.csv` — pure data, no metadata leakage.

- Reads `bronze.model_validation_runs` rows where `run_date >= PR-α merge date`.
- For each `(model_name, metric_name)` row in the existing seed: compute new `reference_value` (median of post-PR-α runs); compute `threshold_warn` and `threshold_alert` via the existing percentile pattern from `analytics.model_validation`.
- Emits the new CSV.

**(2) A freshness sidecar JSON** at `dbt_project/.metadata/baseline_freshness/model_baseline_scalars.json`.

- Format: JSON, not YAML — YAML files in or near `dbt_project/seeds/` are dbt property-file territory and dbt's parser tries to interpret them. JSON is unambiguously not parsed by dbt.
- Location: `dbt_project/.metadata/` is outside any dbt-scanned path. dbt scans `seeds/`, `models/`, `analyses/`, `tests/`, etc. — the `.metadata/` directory is invisible to dbt's resource discovery.
- Schema:

```json
{
  "last_refreshed": "2026-05-03T14:23:00Z",
  "cycle_id": "sk3-mig-b-2026-05-03",
  "regen_script_version": "0.3.31",
  "regen_pr": "#XYZ",
  "sample_size_per_metric": {
    "xg_v2:brier_score": 5,
    "xg_v2:roc_auc": 5,
    "vaep:per_action_mean": 5
  },
  "notes": "Initial post-SK3-MIG-B rebase. Thin sample (~5 daily runs per metric). 30-day re-rebase scheduled — see TODO row wf-model-validation-rebaseline-30d."
}
```

**(3) A queryable audit row** appended to `bronze.sk3_mig_b_runs` with `cycle_item="baseline_rebase"` and `cycle_item_kind="meta_event"`. Same field content as the JSON sidecar but stored as Delta data. Lets the operator run

```sql
SELECT cycle_id, recorded_at, smoke_metrics
FROM bronze.sk3_mig_b_runs
WHERE cycle_item = 'baseline_rebase'
ORDER BY recorded_at DESC
```

to see every rebase across cycles — answers "when did we last rebase? what was the sample size that cycle?" without grepping git history.

**Why all three and not just one:**

- **Seed CSV alone**: mixing metadata into data rows denormalizes the table; future metadata fields bloat every row uniformly.
- **JSON sidecar alone**: no queryable history; "did the 30-day re-rebase actually happen?" requires reading old git revisions.
- **Delta table alone**: CI freshness test would need Databricks auth for the live-CI path; lint CI lanes don't have this. Sidecar JSON solves CI without auth.

The triple-write atomic pattern gives each consumer the right tool: CI reads the file; the operator queries the table; dbt sees only the data CSV.

**File creation timing:** the JSON sidecar is created by PR-β (the regen script's first run produces it). PR-α does NOT create a placeholder. The freshness test (`src/tests/test_model_baseline_scalars_freshness.py`) is also added in PR-β — both the test and the file it reads land in the same PR. Between PR-α merge and PR-β merge, neither artifact exists; the freshness test does not run because it does not yet exist on main. Once PR-β merges, the test and file coexist on every subsequent PR's CI run.

**Threshold rationale:** test asserts `last_refreshed` is within 30 days of HEAD commit time. 30-day cadence aligns with project-wide governance review patterns (CLAUDE.md AI Governance + ARCHITECTURE.md Appendix D both use 30-day grace windows). If operational experience shows 30 days is too frequent (the test fires noise on unrelated PRs because no migration cycle has happened in 30 days), the threshold can extend to 90 days in a follow-up PR — this design choice is captured in `notes` of the JSON sidecar at refresh time so future operators know the cadence intent.

**Statistical thinness disclaimer:** if PR-β merges within ~1 week of PR-α (per §9.4 to stop drift-detection noise), `bronze.model_validation_runs` will have only ~5-7 daily run rows per metric. Median-of-7 is a noisy estimator; IQR-based thresholds from this sample will be loose. Intentional good-enough-not-perfect:

- Loose thresholds reduce false-alarm rate during early Group-B post-merge daily-job stability period.
- A follow-up `wf-model-validation-rebaseline-30d` cycle re-rebases at the 30-day mark when sample size has grown to ~30 runs per metric. Captured as a TODO row at PR-β merge time. Same regen script; no code change.

`src/tests/test_model_baseline_scalars_freshness.py` (NEW) — pure-Python (no Databricks dep): reads `dbt_project/.metadata/baseline_freshness/model_baseline_scalars.json`, asserts (a) file exists, (b) all required keys present (`last_refreshed`, `cycle_id`, `sample_size_per_metric`), (c) `last_refreshed` is within 30 days of HEAD commit time. Failure message tells the operator exactly which command produces the file: `python scripts/regenerate_model_baseline_scalars.py`.

### 7.2 `docs/performance-baselines.md` refresh

`scripts/regenerate_perf_baselines_md.py` (NEW, PEP 723):

1. Reads `bronze.sk3_mig_b_runs` (the durable cycle log per §5.3) for the latest `cycle_id` AND `bronze.workflow_costs` post-PR-α aggregates. **No file-based handoff** — the operator does not copy any sidecar JSON.
2. Regenerates the per-workflow timing tables in `docs/performance-baselines.md`.
3. Updates the `Last refreshed:` header line.

`src/tests/test_perf_baselines_md_freshness.py` (NEW) — asserts the doc's `Last refreshed:` line is within 30 days of HEAD commit time. Matches CLAUDE.md governance review-cadence patterns.

### 7.3 `AI_GOVERNANCE.md` review

Verify §5 Scope row count (one row per evaluative ML system) + `Next review` dates within 30-day grace per existing `test_ai_governance_md.py`. Touch any rows needing date updates. PR-α already updated each retrained model's HF model card content; PR-β is the AI_GOVERNANCE.md-side close-out.

### 7.4 `TODO.md` cleanup

**Remove** (NOT strikethrough — per the standing order on completed-task removal):

- `SK3-MIG-B` row
- `XG1-RETIRE` row
- `HF4` row

**Add** to On Deck (Dunkin' size):

- `wf-model-validation-rebaseline-30d` row (per §9.4) — at the 30-day mark, re-run `scripts/regenerate_model_baseline_scalars.py` against `bronze.model_validation_runs` rows accumulated since PR-β merge for tighter IQR-based thresholds. Same script, no code change.

Update `Last updated:` line.

### 7.5 Memory updates (post-merge)

- Drop `project_sk3_mig_complete.md` (Group A handoff is no longer relevant once Group B closes).
- Write `project_sk3_mig_b_complete.md` successor.
- Update `MEMORY.md` index.

Memory writes happen post-merge per `feedback_no_commits_without_explicit_approval`.

## §8 — Testing strategy

### 8.1 Tests added in PR-α

- `src/tests/sk3_mig_b/test_<model>_post_retrain_smoke.py` × 11 (one per retrained model + per ExT v2 phase) — invoked by orchestrator step 1.7/2.7/3.7.
- `src/tests/test_no_notebook_hf_publishers.py` (HF4 invariant 1).
- `src/tests/test_hf_publish_parity.py` extension (HF4 invariant 2).
- `src/tests/test_xg_v1_retired.py` (XG1-RETIRE).
- `src/tests/test_shot_map_v2_columns.py` (XG1-RETIRE UI migration).

### 8.2 Tests added in PR-β

- `src/tests/test_model_baseline_scalars_freshness.py`.
- `src/tests/test_perf_baselines_md_freshness.py`.

### 8.3 E2E verification (operator-driven, between PR-α merge and PR-β)

- Daily mega-job runs successfully — all task_keys SUCCESS (modulo XG1-RETIRE-deleted task_keys).
- `wf-model-validation` runs against new mart predictions → results land in `bronze.model_validation_runs`. These results feed PR-β's regenerate script.
- Drift detection signal expected to be high (every retrain shifted distributions); informational only, not a halt — PR-β rebases the thresholds.

### 8.4 Existing CI gates that must still PASS

- `test_terraform_env_dep_parity.py` (no env-spec changes expected).
- `test_silly_kicks_boundary.py` (no API changes expected).
- `test_sk3_coord_correctness.py` (regression gate from SK3-MIG-A).
- `test_ai_governance_md.py` (model card inventory parity).
- `test_architecture_md_appendix.py` (academic reference inventory — no changes expected since methodologies unchanged).
- `test_topandas_boundedness.py` (no `.toPandas()` changes expected).
- `test_hf_publish_parity.py` (existing + extension from §4.2).

## §9 — Risk + rollback

### 9.1 Per-cycle-item rollback

Two rollback patterns by item kind:

**Trained-model items** (8) — SK3-MIG-A pattern: every retrain's prior Champion alias is captured in MLflow's history; UC Volume's prior weights are addressable via Delta versioning. To roll back: `set_and_verify_mlflow_champion(name, version=PRIOR_VERSION)` + UC Volume restore. Smoke gate failure halts orchestrator and emits the prior-version restore command in the failure message.

**Compute-only items** (3 — DEFCON-lite, OBSO, PAUSA) — no MLflow Champion to revert; rollback is at the Delta-mart level. Step 0 captured `pre_mart_version` per affected mart in `bronze.sk3_mig_b_runs`. To roll back: `RESTORE TABLE {catalog}.dev_gold.fct_<X> TO VERSION AS OF <pre_mart_version>` followed by Lakebase synced refresh + index restoration. Compute-only smoke gate failure halts orchestrator and emits the RESTORE command directly with the captured pre-state version number filled in.

Both paths require committing the rollback decision after a smoke failure — the orchestrator does NOT auto-rollback on smoke failure (the failure may be a flake; a transient HF Jobs network blip on the inference step; a one-row corruption in a dim table that doesn't merit reverting the whole retrain). The operator inspects `bronze.sk3_mig_b_runs` + the smoke output, decides whether to roll back or rerun, and either issues the printed RESTORE / Champion-revert command OR re-invokes the orchestrator with `--start-at <item>` to retry.

### 9.2 Whole-cycle rollback

Harder — synced tables and HF datasets have already absorbed new values by the time a late failure surfaces. Mitigation: PR-α's atomic step ordering puts smoke gates BEFORE Lakebase synced refresh and BEFORE HF republish. So a smoke failure only requires per-model rollback, not whole-cycle revert. Group 1 must complete + smoke-pass before Group 2 dispatches; ScoutGPT (most expensive) only fires after Group 1 full success.

### 9.3 HF dataset rollback

Trickiest because HF Hub revision history is the only path back. Mitigation: PR-α's republish step (§5.1 Step 3) captures pre-republish HF dataset `revision` SHA into `bronze.sk3_mig_b_runs.pre_hf_revision_sha` (top-level STRING column per §5.3 schema; not nested in `smoke_metrics` which is `MAP<STRING, DOUBLE>` for numeric metrics only).

**HF Hub does not have a first-class `revert` operation.** The correct mechanism via `huggingface_hub`:

```python
from huggingface_hub import HfApi, CommitOperationCopy

api = HfApi()
# Download files at the prior revision and create a new commit copying them as HEAD.
api.create_commit(
    repo_id="luxury-lakehouse/<dataset>",
    repo_type="dataset",
    operations=[
        CommitOperationCopy(
            src_path_in_repo="<file>",
            path_in_repo="<file>",
            src_revision=PRE_REVISION_SHA,
        )
        for file in files_to_revert
    ],
    commit_message=f"Revert to {PRE_REVISION_SHA[:8]} after SK3-MIG-B smoke gate failure",
)
```

`create_branch` is for branching, NOT reverting — using it would create a divergent ref while `main` still points at the broken revision. The orchestrator's per-dataset republish-failure message emits the exact `create_commit` call with the captured `PRE_REVISION_SHA` filled in.

### 9.4 Daily-job harmless-drift window

Between PR-α merge and PR-β merge, drift detection in `wf-model-validation` will fire warnings on every metric (every retrain shifted distributions). Expected; PR body documents explicitly. PR-β must merge promptly (within the same week) to stop the noise.

**30-day re-rebase follow-up:** PR-β rebases against ~5-7 days of post-PR-α `bronze.model_validation_runs` data (thin sample, per §7.1). A follow-up TODO row `wf-model-validation-rebaseline-30d` is added at PR-β merge time — re-rebases the seed at the 30-day mark when sample size has grown to ~30 daily runs per metric for tighter IQR-based thresholds. Dunkin' size; runs the same `regenerate_model_baseline_scalars.py` script with no code changes.

### 9.5 Cost ceiling

`_COST_CAP_USD = 80.0` module-level constant (raised from initial $50 after retry contingency analysis below). Orchestrator queries `bronze.workflow_costs` at start of each cycle item; halts and waits for explicit operator approval (`--override-cost-cap` resume flag) if cumulative exceeds cap.

**Estimated real-run cost: $25-40** (ScoutGPT ~$15-20; F2V family ~$10; xG v2 ~$1; everything else negligible).

**Retry contingency:** smoke-gate failures lead to retries. Per-item retry costs:
- xG v2 retry: ~$1
- F2V single-variant retry: ~$3-5
- ScoutGPT retry: ~$15-20

A single ScoutGPT retry on top of the base estimate hits ~$45-60. The cap at $50 (initial) made one ScoutGPT retry routinely trigger `--override-cost-cap` — borderline meaningless gate. Cap raised to $80 to give headroom for ONE ScoutGPT retry without override; a second ScoutGPT retry hits the cap and triggers operator pause for explicit re-approval. This positions the cap to catch genuine runaway scenarios (e.g., infinite retry loop, accidental full re-run) rather than firing on routine variance.

**Cost-hook coverage prerequisite:** §5.1 Step 0 includes a pre-flight assertion that `bronze.workflow_costs` actually captures HF Jobs spend (not just Databricks workflow spend). If the cost hook is Databricks-only, the cap is theatrical for this cycle (HF Jobs is the dominant cost line). Pre-flight halts with a clear "extend cost hook to HF Jobs before proceeding" message if HF Jobs workflow_ids are absent from the cost-hook history.

### 9.6 Wall-clock ceiling

`_WALLTIME_CAP_HOURS = 8.0` per single retrain. ScoutGPT historically 3-4hr; 8hr ceiling catches a hung job rather than burning a full GPU-day. Halts orchestrator with `--override-walltime-cap` resume flag.

### 9.7 Dry-run mode

`--dry-run` skips actual HF Jobs invocations; runs steps 5-11 against existing Champions. Lets operator verify wiring on $0 spend before committing real spend.

## §10 — Open implementation questions for the plan

These resolve at plan-writing time:

1. **ScoutGPT synced-table refresh scope**: does any gold mart consume ScoutGPT inference outputs and surface them to Lakebase? If yes, that mart's synced table is added to §1.1.6.
2. **HF4 duplicate detection**: confirm `notebooks/publish_datasets.py` cells covering `spadl-vaep-action-values` and `xg-freeze-frame-data` are byte-equivalent to `scripts/publish_spadl_vaep_hf.py` and `scripts/publish_freeze_frame_hf.py` outputs (so deletion is safe).
3. **Lakebase index inventory per affected synced table**: `scripts/maintain_synced_tables.py --skip-refresh` reapplies indexes; verify the index manifest covers every affected synced table.
4. **F2V eval-fold pre-registered IDs**: confirm the 100-player eval fold IDs are stable across retrains (else per-retrain comparisons aren't meaningful).
5. **`_marts__models.yml` mart contract entries for retrained-model marts**: verify no contract changes are needed (additive-fields contract per ADR-019; retrains shouldn't add fields, but verify each mart's column set matches contract). This is also where ADR-013's "Python writers emit only native identifiers + predictions" gets re-tested — confirm no SK3-MIG-A-introduced surrogate keys leaked back into writer payloads.
6. **HF Jobs → Databricks MLflow network glue**: `set_and_verify_mlflow_champion` runs from HF Jobs containers and hits the lakehouse MLflow registry. Confirm HF Jobs containers receive `MLFLOW_TRACKING_URI` + `DATABRICKS_TOKEN` via `--secrets` (not `--env`, per ADR-012's delivery contract) and the network path from HF Jobs to Databricks MLflow is open. Single most failure-prone glue layer in the cycle.
7. ~~`bronze.sk3_mig_b_runs` schema MERGE drift guard~~ — **RESOLVED in §5.0** (folded into PR-α commits as the "ADR-002 §4 telemetry schema discipline" row: `_SK3_MIG_B_RUNS_COLUMNS` module-level constant in `src/ingestion/sk3_mig_b_telemetry.py` + lazy `StructType` factory + DDL parity test `src/tests/test_sk3_mig_b_runs_schema_parity.py`). No longer plan-time deferred.
8. **F2V v1 trainer migration depth**: `notebooks/train_football2vec.py` uses gensim Doc2Vec with hardcoded Databricks workspace path. Confirm at plan time that the migrated `scripts/train_football2vec.py` correctly: (a) replaces the workspace path with `huggingface_hub.snapshot_download` for input data, (b) uses `huggingface_hub.get_token()` for auth (ADR-012), (c) emits weights to UC Volume via `upload_weights_to_uc_volume` (ADR-012), (d) sets MLflow Champion alias via `set_and_verify_mlflow_champion`. Each is non-trivial — the migration is ~200-400 LOC of new code, not a syntactic transform.
9. **Cross-repo state sync**: spec lives in `D:\Development\karstenskyt__luxury-lakehouse`; reviewer is at sibling `D:\Development\karstenskyt__luxury-lakehouse-d32`. Both must be at same SHA before kicking off the cycle. The mega-job lives in dev workspace; only one operator session should drive PR-α.

## §11 — References

### Authoritative (in-repo, readable from any session)

- silly-kicks 3.0.1 (commit `d7f86de` PR-S23) — `D:\Development\karstenskyt__silly-kicks`
- Lakehouse SK3-MIG-A spec: `docs/superpowers/specs/2026-05-02-sk3-mig-direction-of-play-migration-design.md`
- Lakehouse SK3-MIG-A plan: `docs/superpowers/plans/2026-05-02-sk3-mig-direction-of-play-migration.md`
- Lakehouse SK3-MIG-A merged: PR #249 squash `485fc10` + PR #250 hotfix squash `aa0237f`
- ADR-012 (training-to-production delivery hardening) — `artifact_deploy.py` discipline
- ADR-014 (HF card inventory parity) — `upload_hf_readme` discipline; this PR amends with notebook ban
- ADR-018 (cross-table format-contract testing)
- ADR-019 (mart classification + 3-stage `dbt_build`) — `additive-fields` contract
- ADR-022 (direction-of-play migration)
- ADR-002 §4 — telemetry-writer schema-drift guard (applies to `bronze.sk3_mig_b_runs`)
- CLAUDE.md governance + Lakebase Ops + AI Governance sections
- TODO.md SK3-MIG-B / XG1-RETIRE / HF4 rows — superseded by this spec at PR-merge time.

### Informational (operator memory; non-load-bearing for the spec body)

The following memory cites informed design decisions but the relevant rules are inlined into the spec body so the spec is self-contained. Memory dirs are namespaced per Claude Code project directory (`luxury-lakehouse` and `luxury-lakehouse-d32` siblings have separate memory namespaces) — citing memory paths in a load-bearing way breaks cross-session reviewability.

- `project_sk3_mig_complete.md` (Group A handoff) — content reflected in §0 Context.
- `feedback_no_commits_without_explicit_approval.md` — manifests as the "spec written but not committed" pattern in §1.2 + post-merge memory writes pattern in §7.5.
- `feedback_no_micro_approvals_in_execution.md` — manifests as the "design-locked then execute through" pattern in §5.2's per-cycle-item loop (no per-step user prompts).
- `feedback_pull_origin_main_before_branching.md` — operator-side discipline; not in spec body.
- `reference_sdk_over_sql_connector.md` — manifests as `WorkspaceClient.statement_execution` in publisher scripts.
- `feedback_drop_calendar_effort_estimates.md` — manifests as Monstah/Wicked/Dunkin' size labels rather than calendar estimates throughout.
- `reference_mega_job_orchestrator_design.md` — fully inlined in §5.1 Step 1 ("Mega-job orchestrator rule").
