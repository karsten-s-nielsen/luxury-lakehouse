# Pre-Shot xG v3 — Complete Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.
>
> **Approval gate (HARD):** This project's owner requires explicit approval for EVERY `git commit`/push/PR/merge and every live Databricks/HF-Jobs run. The "Commit" and "run live" steps below are PROPOSALS — pause and get explicit approval before executing each. Never commit or run a wheel-consuming/live job without it.

**Goal:** Deliver consumable, calibrated pre-shot xG (`fct_shot_xg`, one row per shot, joinable to `fct_action_values` on `(match_key, action_id)`) from a canonical-SPADL-native `xg_model_v3` trained on all cohorts (StatsBomb-360 + GS/SkillCorner tracking + zero-context), replacing `fct_xg_predictions_v2`.

**Architecture:** Three stages — [A] complete training corpus + two split HF datasets, [B] fixed trainer + validated `xg_model_v3`, [C] scorer (two-mode gate) + `fct_shot_xg` mart. Sequenced as **Milestone 1 (GS/SC-first, end-to-end deliverable + downstream unblock)** then **Milestone 2 (SB-360 consolidation, extends in place)**.

**Tech Stack:** Python 3.10, PySpark/Delta (Databricks serverless), dbt, PyTorch (HF-Jobs GPU trainer), HuggingFace Hub, MLflow, `silly-kicks` (SPADL/coords), `uv`, Ruff/Pyright/pytest.

**Spec:** `docs/superpowers/specs/2026-07-07-preshot-xg-v3-complete-delivery-design.md` (authoritative — read §5 identity/coordinate invariants and §8 gates before starting).

---

## Conventions used throughout (read once)

- **Shot key is ALWAYS `(match_key, action_id)`** (§5). `action_id` is per-match, not global. Never group/join/dedup on `action_id` alone. Any validation query grouping by `action_id` alone is invalid.
- **Coordinates are canonical SPADL 105×68, home-LTR** everywhere a freeze frame or geometry appears.
- **TDD**: write the failing test first, run it red, implement, run it green. Fixtures MUST mirror live schemas (all columns the live table carries), incl. the adversarial cases (cross-match `action_id`, duplicated frames, missing/extra columns).
- **Wheel/CI discipline per deployable change**: bump via `scripts/bump_wheel.py` (never hand-edit versions), then `uv run ruff check src/ scripts/`, `uv run ruff format --check`, `uv run pyright src`, `uv run lint-imports` (3 kept/0 broken), full `uv run pytest src/tests/`, and `uv run validate_workflow_cards workflow-cards/` if a card changed. A new mega-job task also needs: pyproject entry point, alphabetical TF task block, task-count anchor, card, card↔TF map (`test_card_parity_with_terraform`), observability seed row (`dbt_project/seeds/task_workflow_mapping.csv`) — see `[[reference-mega-job-orchestrator-design]]`.
- **New `publish_*_hf.py`** must be added to `hf_leak_guard.PUBLISHER_REGISTRY` (mode `"split"`) and `_ADR049_SPLIT_PUBLISHER_CARDS` in `test_hf_publish_parity.py`, with in-repo cards under `docs/huggingface/dataset-cards/<basename>.md` (+ `<basename>-restricted.md`), pre-declared in `_DATASET_CARD_ORPHAN_EXEMPT` before first publish.
- **Live validation before "done"**: each stage has an explicit live-gate task (SQL over the warehouse via `WorkspaceClient.statement_execution`, or a job run). These are not optional.

---

## File Structure (what each file owns)

**Milestone 1 files**
- Modify `scripts/migrations/2026-07-08-shot-freeze-frames-access-tier.sql` (new) — add `access_tier` col + backfill from `dim_matches` (§A2).
- Modify `src/analytics/action_context/tracking_snapshots.py` — add `access_tier` to `_SHOT_FF_COLUMNS`/`_SHOT_FF_TYPES`/StructType (schema-parity).
- Modify `src/ingestion/shot_freeze_frames.py` — stamp `access_tier` per-row in the writer path.
- Create `scripts/publish_xg_shot_data_v3_hf.py` — shots publisher (§A3).
- Create `scripts/publish_shot_freeze_frames_hf.py` — freeze-frames publisher (§A4).
- Create `docs/huggingface/dataset-cards/xg-shot-data-v3.md` (+ `-restricted.md`), `xg-shot-freeze-frames.md` (+ `-restricted.md`).
- Modify `src/ingestion/hf_leak_guard.py` (`PUBLISHER_REGISTRY`), `src/tests/test_hf_publish_parity.py` (`_ADR049_SPLIT_PUBLISHER_CARDS`, `_DATASET_CARD_ORPHAN_EXEMPT`).
- Modify `scripts/train_xg_v3_hf.py` — B1 (`(match_key, action_id)` join), B2 (read both repos), B3 (uniform features), B4 (fit+ship OOF per-provider + pooled calibrators; model emits raw), B5 (model card), provenance.
- Create `docs/huggingface/model-cards/xg-v3-model-card.md`.
- Modify `src/tests/test_train_xg_v3.py` — tests for `parse_freeze_frames_spadl` join, read-both assembly, uniform features, actor-inclusion cross-builder (N2).
- Create `src/ingestion/xg_shot_scorer.py` — scorer (§C1) + entry point.
- Create `dbt_project/models/staging/xg/stg_xg__shot_predictions.sql`, `dbt_project/models/marts/fct_shot_xg.sql` (+ `_marts__models.yml` contract), migration `scripts/migrations/2026-07-08-xg-shot-predictions-ddl.sql`.
- Create `workflow-cards/wf-shot-xg-scorer.yaml` + terraform task + registrations for `compute_xg_shot_scores`.
- Create `src/tests/test_xg_shot_scorer.py`, `dbt_project/tests/assert_shot_xg_key_in_action_values.sql`, `assert_av_ac_action_id_consistency.sql`.

**Milestone 2 files**
- Create `src/analytics/action_context/sb360_freeze_frames.py` — StatsBomb→SPADL freeze-frame conversion (reuse silly-kicks `_convert_locations`) + orientation (§A1, two separate functions).
- Modify `src/ingestion/shot_freeze_frames.py` — `statsbomb` provider branch (SB-360 compute path).
- Modify `workflow-cards/wf-shot-freeze-frames.yaml` + the task `--providers` to add `statsbomb`.
- Create `src/tests/action_context/test_sb360_freeze_frames.py` — the committed co-location golden (N1/N5) on a real public SB-360 shot fixture + conversion/orientation unit tests.
- Create `src/tests/fixtures/sb360_golden_shot_left.json` + `sb360_golden_shot_right.json` — **two real public SB-360 shots, markedly off-center on OPPOSITE sides** (actor `y ≈ 12` and `y ≈ 56`), each storing the 360 frame, the shot action `(start_x, start_y)`, and the match `fidelity_version` (committable; SB-360 is public; B1/N5/m2).
- Modify `dbt_project/models/marts/fct_xg_predictions_v2.sql` → view over `fct_shot_xg` via the `shot_id` bridge (§C2, **deferred from M1 to M2 per N1** — retire only once StatsBomb is context-aware); Create `dbt_project/tests/assert_xg_v2_view_shot_id_1to1.sql`.

---

## MILESTONE 1 — GS/SC-first, end-to-end `fct_shot_xg`

Produces working, calibrated `fct_shot_xg` for GS/SC (context) + all zero-context providers, on an interim `xg_model_v3` (§N6: interim, superseded in place by M2). This is the downstream unblock.

### Task 1.1 — `access_tier` on `bronze.shot_freeze_frames` (§A2)

**Files:** Create `scripts/migrations/2026-07-08-shot-freeze-frames-access-tier.sql`; Modify `src/analytics/action_context/tracking_snapshots.py`; Test `src/tests/action_context/test_shot_freeze_frames_writer.py`.

- [ ] **Step 1 — failing test:** in `test_shot_freeze_frames_writer.py`, extend the DDL↔`_SHOT_FF_COLUMNS` parity test to require `access_tier` as the last data column (before `_ingested_at`). Assert `"access_tier" in _SHOT_FF_COLUMNS` and it maps to `string` in `_SHOT_FF_TYPES`.
- [ ] **Step 2 — run red:** `uv run pytest src/tests/action_context/test_shot_freeze_frames_writer.py -v` → FAIL.
- [ ] **Step 3 — implement:** add `access_tier` to `_SHOT_FF_COLUMNS`, `_SHOT_FF_TYPES` (`"access_tier": "string"`), and the StructType factory in `tracking_snapshots.py`. In the DDL migration, `ALTER TABLE soccer_analytics.bronze.shot_freeze_frames ADD COLUMNS (access_tier STRING)` (idempotent via `_runner.py`'s DESCRIBE-skip) then backfill: `MERGE INTO ... USING dim_matches ON match_key WHEN MATCHED AND access_tier IS NULL THEN UPDATE SET access_tier = dm.access_tier`.
- [ ] **Step 4 — stamp in writer:** in `src/ingestion/shot_freeze_frames.py`, the snapshot rows must carry `access_tier` — join the per-match tier from `dim_matches` (already resolving `match_key` there) and add it to the written frame before `write_shot_freeze_frames`. Update the writer's column select to include it.
- [ ] **Step 5 — run green + gates:** the parity test passes; `uv run pytest src/tests/action_context/ -q`.
- [ ] **Step 6 — Commit (APPROVAL GATE):** `feat(xg): access_tier on bronze.shot_freeze_frames + backfill`.
- [ ] **Step 7 — live (APPROVAL GATE):** operator-apply the migration via `_runner.py`; verify `SELECT data_source, access_tier, count(*) FROM bronze.shot_freeze_frames GROUP BY 1,2` — GS/SC-RM `restricted`, A-League-SC `public`, no NULLs.

### Task 1.2 — Shots publisher `xg-shot-data-v3` (§A3)

**Files:** Create `scripts/publish_xg_shot_data_v3_hf.py`; Create cards `docs/huggingface/dataset-cards/xg-shot-data-v3.md` + `xg-shot-data-v3-restricted.md`; Modify `src/ingestion/hf_leak_guard.py`, `src/tests/test_hf_publish_parity.py`; Test `src/tests/test_publish_xg_shot_data_v3.py`.

- [ ] **Step 1 — failing tests:** (a) `test_hf_leak_guard::test_registry_covers_every_publisher_module` will fail once the file exists until registered — add `"publish_xg_shot_data_v3_hf": "split"` to `PUBLISHER_REGISTRY`. (b) New `test_publish_xg_shot_data_v3.py`: assert the module's SQL selects `match_key, action_id, action_result, action_type, start_x, start_y, data_source, access_tier` from `fct_action_values` with NO provider filter (mirror `test_publisher_sql_does_not_filter_providers`), calls `split_restricted(df, column="access_tier")`, `assert_no_private_leak(public_df, ...)`, drops `access_tier`, uploads both repos with `delete_patterns=["**"]`, and calls `upload_hf_readme` for both.
- [ ] **Step 2 — run red.**
- [ ] **Step 3 — implement:** copy `scripts/publish_spadl_vaep_hf.py`'s split structure verbatim (its `"split"` pattern is canonical); change repo to `luxury-lakehouse/xg-shot-data-v3`, source `SELECT match_key, action_id, action_result, action_type, start_x, start_y, data_source, access_tier FROM soccer_analytics.dev_gold.fct_action_values WHERE action_type IN ('shot','shot_freekick','shot_penalty')` (include penalties so the scorer's penalty path has rows; the trainer filters them out), flat per-provider files (ADR-054). Register in `_ADR049_SPLIT_PUBLISHER_CARDS`; add cards + `_DATASET_CARD_ORPHAN_EXEMPT` entries.
- [ ] **Step 4 — run green + gates** (`pytest test_hf_publish_parity.py test_hf_leak_guard.py test_publish_xg_shot_data_v3.py`, `lint-imports`).
- [ ] **Step 5 — Commit (APPROVAL GATE).**
- [ ] **Step 6 — live publish (APPROVAL GATE):** run the publisher; verify the restricted repo has GS + RM-SC rows and the public repo has NONE (leak guard + a direct `list_repo_tree` check).

### Task 1.3 — Freeze-frames publisher `xg-shot-freeze-frames` (§A4)

**Files:** Create `scripts/publish_shot_freeze_frames_hf.py`; cards `xg-shot-freeze-frames.md` (+ `-restricted.md`); Modify `hf_leak_guard.py`, `test_hf_publish_parity.py`; Test `src/tests/test_publish_shot_freeze_frames.py`.

- [ ] Same shape as 1.2. Source `SELECT match_key, action_id, data_source, player_id, x, y, is_keeper, is_teammate, set_cardinality, shooter_attacks_high_x, team_attacking_direction, access_tier FROM soccer_analytics.bronze.shot_freeze_frames`. Split on `access_tier`. Register + cards. **m1 test:** a synthetic row with NULL `access_tier` must land in restricted, never public.
- [ ] Commit (APPROVAL GATE); live publish (APPROVAL GATE) + verify split.

### Task 1.4 — Trainer fix: `(match_key, action_id)` freeze-frame join (§B1)

**Files:** Modify `scripts/train_xg_v3_hf.py`; Test `src/tests/test_train_xg_v3.py`.

- [ ] **Step 1 — failing test:** `test_parse_freeze_frames_joins_on_match_key_action_id`: build `shots_df` with two rows sharing `action_id=100` under `match_key=1` and `match_key=2`, and a `freeze_df` where match 1's players differ from match 2's. Assert each shot's returned player set contains ONLY its own match's players (not the union), and length is that match's count.
- [ ] **Step 2 — run red** (current code groups by `action_id` alone → returns the union → FAIL).
- [ ] **Step 3 — implement:** in `parse_freeze_frames_spadl`, `groups = dict(iter(freeze_df.groupby(["match_key", "action_id"])))` and iterate `for mk, aid in zip(shots_df["match_key"], shots_df["action_id"]): group = groups.get((mk, aid))`. Read `match_key` from `freeze_df`.
- [ ] **Step 4 — run green.** Keep existing helper tests green.
- [ ] **Step 5 — Commit (APPROVAL GATE)** (bundled with 1.5–1.7, one trainer PR).

### Task 1.5 — Trainer: read BOTH public + restricted repos, fail-loud (§B2)

**Files:** Modify `scripts/train_xg_v3_hf.py`; Test `src/tests/test_train_xg_v3.py`.

- [ ] **Step 1 — failing test:** extract a pure helper `load_dataset_both_repos(api, repo_id, hf_token) -> pd.DataFrame` that lists+reads parquet from `repo_id` AND `restricted_repo_id(repo_id)` (`<repo>-restricted`) and concatenates. Test: mock the api to return public rows for `repo` and restricted rows for `repo-restricted`; assert the result contains BOTH. Second test: if the restricted companion is expected (a manifest/flag says RM present) but the read yields zero restricted rows, the helper raises `RuntimeError` (fail-loud, ERROR-level) — never silently public-only.
- [ ] **Step 2 — run red; Step 3 — implement** the helper; use it for BOTH `SHOTS_DATASET` and `FREEZE_FRAME_DATASET` in `main()`. Record both public+restricted commit shas in provenance. Import `restricted_repo_id` from `ingestion.hf_publish`.
- [ ] **Step 4 — corpus-composition assertion:** after load, assert the shots include the expected restricted providers (e.g. `skillcorner` restricted-match shot count > 0) — a loud check that RM actually made it in.
- [ ] **Step 5 — run green.**

### Task 1.6 — Trainer: uniform provider-agnostic features + single-calibration ownership (§B3/§B4/§D2/M1)

**Files:** Modify `scripts/train_xg_v3_hf.py`; Test `src/tests/test_train_xg_v3.py`.

- [ ] **Step 1 — failing test:** `test_uniform_feature_names_are_geometry_only`: with a shots df carrying only `start_x/start_y` + `set_cardinality` (no StatsBomb categoricals), assert `build_spadl_tabular` returns exactly the D2 feature columns (`distance_to_goal, shot_angle, location_x, location_y, set_cardinality`) in a stable, pinned order — no `shot_body_part`/etc.
- [ ] **Step 2 — run red; Step 3 — implement:** ensure `XGModelConfig`/`build_features` is configured for the uniform set; do NOT pass StatsBomb categoricals; pin `feature_names` order. The envelope's `feature_names`/`tabular_dim` are the serve contract.
- [ ] **Step 4 — calibration ownership (M1):** replace the in-sample isotonic with **fitting** per-provider OOF calibrators + a pooled OOF calibrator on GroupKFold-held-out predictions via `analytics.xg_calibration`; the model's forward output stays **raw**; ship the calibrators + the per-provider two-mode OOS report in the envelope (evidence only — the trainer applies NOTHING to a served value). Add `test_trainer_ships_calibrators_but_emits_raw` asserting the envelope has `_calibrators` (per-provider + pooled) and no calibration is applied to `_predict` output.
- [ ] **Step 5 — EXPLICIT `shot_penalty` exclusion from training (M2):** the shots dataset deliberately *includes* penalties (Task 1.2, so the scorer has rows), but D4 trains on `{shot, shot_freekick}` only — penalties are constant/degenerate and would skew the encoder + calibration baseline. Confirm `select_training_shots` filters `action_type IN ('shot','shot_freekick')` (exclude `shot_penalty`); add `test_penalties_excluded_from_training` asserting a df with penalty rows returns them removed. This publish-includes / train-excludes asymmetry is stated + tested, not implicit.
- [ ] **Step 6 — penalty constant is trainer-owned (m4), computed from the PENALTY rows (m2):** the trainer computes the empirical `shot_penalty` goal-rate over the **`shot_penalty` rows in the loaded dataset** (≈0.76) — computed BEFORE / independently of the Step-5 training-population filter that excludes penalties (computing it over the penalty-excluded training set would be 0/0 → NaN). It **ships it in the envelope** as `_penalty_xg` — the single provenanced source of truth; the scorer (Task 1.9) reads it, never recomputes live. Add `test_penalty_constant_computed_from_penalty_rows_not_training_set` (a dataset with penalties → non-NaN rate; the training-filtered set is empty of penalties) and `test_envelope_carries_penalty_constant`.
- [ ] **Step 7 — run green.**

### Task 1.7 — Trainer: model card + N2 actor-inclusion cross-builder test (§B5/N2)

**Files:** Create `docs/huggingface/model-cards/xg-v3-model-card.md`; Test `src/tests/test_train_xg_v3.py` (or `test_tracking_snapshots.py`).
- [ ] Create the card (governed under `wf-xg-v2`; coordinate contract SPADL; all-provider; two-mode; metrics PENDING until the live retrain). Fixes the `upload_hf_readme` path that referenced a nonexistent file.
- [ ] **N2 cross-builder test** `test_both_builders_include_actor`: build a synthetic shot through `build_tracking_snapshots` and (M2 will add) `build_sb360_snapshots`→conversion, assert BOTH include a row for the shooter (`player_id == shot actor`). For M1, assert `build_tracking_snapshots` includes the actor; add the SB-360 half in M2's Task 2.2.
- [ ] Run gates; **Commit (APPROVAL GATE)** the whole trainer change (1.4–1.7) as one PR; bump wheel.

### Task 1.8 — Trainer live gate + interim retrain (§8-B, N6)

- [ ] **Step 1 — subset dry-run (APPROVAL GATE, live):** download both repos, run the training-set assembly ONLY (no GPU fit) on the real data; assert: freeze-frame match-rate per provider is sane (GS/SC ~ their shot counts, SB/WS = 0 context in M1), `feature_names` == the D2 set, and a `(match_key, action_id)` contamination check (no shot's player set spans two matches). Abort if any fail.
- [ ] **Step 2 — GPU retrain (APPROVAL GATE, live):** `hf jobs uv run` the trainer → interim `xg_model_v3` Champion (ADR-012 delivery). Capture per-provider two-mode OOS metrics. **N6:** document in the model card that this is the interim (tabular-only-likely) model, superseded in place by M2.

### Task 1.9 — Scorer `ingestion.xg_shot_scorer` (§C1)

**Files:** Create `src/ingestion/xg_shot_scorer.py` + `compute_xg_shot_scores` entry point; Create migration `scripts/migrations/2026-07-08-xg-shot-predictions-ddl.sql`; Test `src/tests/test_xg_shot_scorer.py`.

- [ ] **Step 1 — failing tests (pure helpers):** `test_scorer_reads_penalty_constant_from_envelope` (the scorer takes `_penalty_xg` from the loaded model envelope — trainer-owned, Task 1.6 Step 6 — and applies it to `shot_penalty` rows; it does NOT recompute it live — m4); `test_mode_selection_vs_certification_separate` (a provider whose selected mode has AUC-CI lower bound below the StatsBomb floor → `ood_flag=True` even though a mode was selected — M2-gate via `select_scoring_mode`/`is_mode_certified`); `test_missing_calibrator_falls_back_pooled_and_flags` (a provider with no per-provider calibrator → pooled calibrator applied + `ood_flag=True` — N4); `test_scorer_and_trainer_share_serve_functions` (identity asserts `scorer.build_features is analytics.xg_model.build_features` AND `scorer.normalize_freeze_frame is analytics.xg_freeze_frame.normalize_freeze_frame` — full M2 parity, both serve-critical shared functions — m1); `test_scorer_e2e_synthetic_keyed` (**committed synthetic end-to-end, m5** — a synthetic shot + its freeze frame, joined on `(match_key, action_id)`, run through the scorer's pure scoring path, yields exactly ONE prediction row per `(match_key, action_id)`, 1:1, with the full `bronze.xg_shot_predictions` output schema — catches a join-contract / grain regression in CI, not only in the live gate).
- [ ] **Step 2 — run red; Step 3 — implement:** `main()` loads `xg_model_v3@Champion` (raw) + shipped calibrators; reads shots from `fct_action_values` + freeze frames from `bronze.shot_freeze_frames` on `(match_key, action_id)`; assembles features via SHARED `build_features`/`normalize_freeze_frame`; applies the two-mode gate + single per-provider (or pooled fallback) calibrator; penalties → constant; MC-dropout CI; writes `bronze.xg_shot_predictions` (`replaceWhere` per `match_key`) per the DDL. Register as a mega-job task (`compute_xg_shot_scores`) — see conventions.
- [ ] **Step 4 — run green + gates** (incl. `validate_workflow_cards`, TF/card/seed registration).
- [ ] **Step 5 — Commit (APPROVAL GATE); Step 6 — live (APPROVAL GATE):** apply DDL; run scorer on a small slice; verify `bronze.xg_shot_predictions` distributions.

### Task 1.10 — `fct_shot_xg` mart + retire `fct_xg_predictions_v2` (§C2)

**Files:** Create `dbt_project/models/staging/xg/stg_xg__shot_predictions.sql`, `dbt_project/models/marts/fct_shot_xg.sql`; Modify `_marts__models.yml` (contract), `_xg__sources.yml` (add `xg_shot_predictions`), `fct_xg_predictions_v2.sql` (→ view); Create `dbt_project/tests/assert_shot_xg_key_in_action_values.sql`, `assert_av_ac_action_id_consistency.sql`.

- [ ] **Step 1 — dbt tests first:** `assert_shot_xg_key_in_action_values.sql` (every `fct_shot_xg (match_key, action_id)` exists in `fct_action_values`); `assert_av_ac_action_id_consistency.sql` (the §8/B2/N3 cross-mart gate — anti-join `fct_action_values` tracking shots vs `fct_action_context` = 0; key anti-join only, no raw-coordinate equality per N3). Assert the SQL text of these in `python-ci` (`reference_dbt_ci_parse_only_tests_daily`).
- [ ] **Step 2 — implement `fct_shot_xg`:** `stg_xg__shot_predictions` view over `bronze.xg_shot_predictions`; `fct_shot_xg` (`contract: enforced: true`, keyed `(match_key, action_id)`, cols per §7, surrogates from `fct_action_values`).
- [ ] **Step 3 — leave `fct_xg_predictions_v2` UNTOUCHED on v2 (N1 — M1 is strictly additive).** In M1, `fct_shot_xg` is a **new additive mart**; do NOT repoint `fct_xg_predictions_v2`. M1 has no SB-360 context, so StatsBomb scores **tabular-only** — repointing the legacy view now would swap existing UI consumers (Taipy shot-map, HF `xg-shots`) from v2's **context-aware** StatsBomb xG **down to a tabular-only interim** (a visible regression, reverted a milestone later). The `fct_xg_predictions_v2` → view retirement (`shot_id` bridge + 1:1 test + P-5 consumer migration + latency call) is **deferred to M2 Task 2.4**, where StatsBomb is context-aware again so there is no interim downgrade. `fct_shot_xg` in M1 may still contain SB/WS rows (provisional, unconsumed — the legacy view stays on v2), but nothing existing is touched.
- [ ] **Step 4 — run green** (`dbt build --select fct_shot_xg` in the dev flow; the contract + the two assertions from Step 1).
- [ ] **Step 5 — Commit (APPROVAL GATE).**

### Task 1.11 — Milestone-1 live acceptance gate (§8-C, §9)

- [ ] **(APPROVAL GATE, live)** Materialize `fct_shot_xg` for the current corpus. Verify: rows for all GS/SC + zero-context shots; per-provider `xg` ranges sane + goal-rate roughly calibrated; `scoring_mode`/`ood_flag` behave (GS/SC likely tabular-only+flagged in interim); penalties == the constant; `fct_shot_xg ⋈ fct_action_context` anti-join = 0 for tracking (B2). Full `pytest` + dbt tests green.
- [ ] **Provisional-value contract (m3):** document — in the model card and a NOTE column-comment on `fct_shot_xg` — that **M1 `fct_shot_xg` values are PROVISIONAL** (produced by the interim model; M2 replaces `xg_model_v3` in place and re-scores, so `xg` values change between milestones). Downstream may **develop** against M1 but must **not lock final numbers** on it; anything fitting on `fct_shot_xg.xg` (e.g. the consumer's V(z,p)) re-runs after M2. **Milestone 1 done: downstream can consume provisional `fct_shot_xg` for GS/SC.**

---

## MILESTONE 2 — SB-360 consolidation (extends in place)

Adds the largest context cohort. Replaces the interim model in place (same `xg_model_v3` name — N6).

### Task 2.1 — SB-360 → SPADL conversion module (§A1, the highest-risk unit)

**Files:** Create `src/analytics/action_context/sb360_freeze_frames.py`; Create fixtures `src/tests/fixtures/sb360_golden_shot_left.json` (actor `y ≈ 12`) + `src/tests/fixtures/sb360_golden_shot_right.json` (actor `y ≈ 56`) — two real PUBLIC SB-360 shots, off-center on OPPOSITE sides, each storing its 360 frame + shot `(start_x, start_y)` + match `fidelity_version` (B1/N5/m2); Test `src/tests/action_context/test_sb360_freeze_frames.py`.

- [ ] **Step 1 — CO-LOCATION GOLDEN FIRST (N1/N5, committed CI) — FIXTURES MUST BE OFF-CENTER (B1):** the y-flip is `y → 68 − y`, which is **invisible at `y = 34`** (`68 − 34 = 34`). A central shot (penalty-spot / central-box) passes the golden *whether or not the frame is mirrored* — worse than no test. **Fixture-selection requirement:** capture TWO real PUBLIC SB-360 shots into `src/tests/fixtures/`, both **markedly off-center on OPPOSITE sides** — one with actor `y ≈ 12` and one with actor `y ≈ 56` (`|y − 34|` large, so a y-flip moves the actor ~40 m and the ±2 m tolerance catches it instantly, and the two sides pin the flip *direction*, not just magnitude). Each fixture stores the shot's 360 frame (incl. the `actor`-flagged shooter row), the shot action's `(start_x, start_y)`, AND the match's **`shot_fidelity_version`** (from `bronze.statsbomb_matches` — m2; the conversion uses it, else the wrong cell size is masked by tolerance). Write `test_sb360_conversion_actor_colocates_with_shot` (parametrized over both fixtures): convert the frame using the fixture's `fidelity_version`; assert the **converted actor position ≈ the shot's `(start_x, start_y)`** (primary, ±~2 m) and the defending keeper is near the attacked goal (secondary). RED until conversion is correct; a y-mirror fails it on both fixtures.
- [ ] **Step 2 — separate conversion from orientation (two functions):** `convert_statsbomb_locations_to_spadl(xy, shot_fidelity_version)` — reuse silly-kicks `silly_kicks.spadl.statsbomb._convert_locations` (y-flip + cell-center offset + fidelity; DO NOT hand-roll a scale). **`fidelity_version` source (fact-checked 2026-07-07):** it is NOT in `bronze.statsbomb_360`; read **`shot_fidelity_version` from `bronze.statsbomb_matches`** per `match_id` (the SAME value the shot *action* conversion used — matching m3). Use `shot_fidelity_version` (not `xy_fidelity_version`) because the 360 frame is captured at the shot instant. `orient_frame_home_ltr(frame, action_orientation)` — apply the shot action's home-LTR orientation. Unit-test each separately: conversion test asserts a known StatsBomb point maps to the exact SPADL point `_convert_locations` produces (byte-identical, given the fidelity); orientation test asserts an away-team frame is point-reflected.
- [ ] **Step 3 — run green** (golden + unit tests). `fidelity_version` (m3) threaded from the match's 360 metadata (same value the action used).
- [ ] **Step 4 — builder wrapper:** `build_sb360_freeze_frames(actions, sb360_raw, fidelity_version) -> DataFrame[_SHOT_FF_COLUMNS]` composing `build_sb360_snapshots` → conversion → orientation → derive `is_teammate`/`is_keeper`/`shooter_attacks_high_x`/`set_cardinality`/`access_tier='public'`. N2: assert the actor is present in the output.
- [ ] **Step 5 — Commit (APPROVAL GATE).**

### Task 2.2 — Wire SB-360 into `compute_shot_freeze_frames` (§A1)

**Files:** Modify `src/ingestion/shot_freeze_frames.py`; Modify `workflow-cards/wf-shot-freeze-frames.yaml` + the TF task `--providers`; Test `src/tests/test_compute_shot_freeze_frames.py`.

- [ ] **Step 1 — failing test:** the driver's `_process_match` for `provider='statsbomb'` reads `bronze.statsbomb_360` + spadl shots, calls `build_sb360_freeze_frames`, writes `bronze.shot_freeze_frames` with `data_source='statsbomb'`. Test the dispatch + the discovery SQL includes `statsbomb` only when `--providers` selects it. Extend the live-schema guard to the `statsbomb_360` input columns.
- [ ] **Step 2 — implement** the `statsbomb` branch: per match, read `bronze.statsbomb_360` (freeze rows — has `id, teammate, actor, keeper, location, match_id`) + the shot actions + `shot_fidelity_version` from `bronze.statsbomb_matches` for that `match_id`; pass fidelity into `build_sb360_freeze_frames`. The `actor` column identifies the shooter row directly (used by the N1 co-location golden — the actor's converted position must equal the shot's `(start_x, start_y)`). Add `statsbomb` to the task's `--providers` (still default GS+SC; SB-360 enabled via `--providers gradientsports,skillcorner,statsbomb` for the backfill run). N2 cross-builder actor test now covers both builders.
- [ ] **Step 3 — run green + gates; Step 4 — Commit (APPROVAL GATE).**
- [ ] **Step 5 — live SB-360 backfill (APPROVAL GATE):** run `compute_shot_freeze_frames --providers ...,statsbomb --match-ids <SB-360 matches>` (resume-runs as needed). **Live gate:** validate by `(match_key, action_id)` — is_teammate both classes, orientation non-NULL, cardinality in-range, no fan-out, AND re-run the co-location check on several live SB-360 shots (converted actor ≈ action start). Wipe+recompute if any check fails.

### Task 2.3 — Re-stage, re-train, re-score on the full corpus

- [ ] **Step 1 — re-publish** the freeze-frames dataset (now incl. SB-360 public rows) via Task 1.3's publisher (APPROVAL GATE, live).
- [ ] **Step 2 — full retrain (APPROVAL GATE, live):** re-run the trainer (unchanged code from M1) — now the corpus includes SB-360 context. Same `xg_model_v3` name/alias (N6, in-place replace). The subset dry-run gate (Task 1.8 Step 1) runs first; SB-360 context match-rate now > 0. Capture the improved per-provider two-mode OOS; the gate should now certify context-aware for SB-360 (and possibly GS/SC).
- [ ] **Step 3 — re-score (APPROVAL GATE, live):** re-run `compute_xg_shot_scores` over all shots; SB-360 shots now context-aware. Re-materialize `fct_shot_xg`.

### Task 2.4 — Retire `fct_xg_predictions_v2` (deferred from M1 per N1) + final acceptance (§9)

**Files:** Modify `dbt_project/models/marts/fct_xg_predictions_v2.sql` (→ view); Create `dbt_project/tests/assert_xg_v2_view_shot_id_1to1.sql`.

- [ ] **Step 1 — retire v2 into a view over `fct_shot_xg` (now StatsBomb is context-aware — NOT a rename).** Legacy table is keyed by **`shot_id`** (StatsBomb-shot id) with `xg_set_encoder`, `xg_ci_lower/upper`, `competition_id` — different grain than `fct_shot_xg`'s `(match_key, action_id)`. (a) Write `assert_xg_v2_view_shot_id_1to1.sql` FIRST — the view's `shot_id` is 1:1 with `fct_shots.shot_id`. (b) Implement the view producing the **exact verified legacy schema** (fact-checked 2026-07-07 against `dev_gold.fct_xg_predictions_v2`): `shot_id STRING, match_key LONG, competition_key LONG, competition_id INT, team_key LONG, player_key LONG, xg_set_encoder DOUBLE, xg_ci_lower DOUBLE, xg_ci_upper DOUBLE`. **Reconstruct `shot_id`** via the bridge `fct_shot_xg.(match_key, action_id)` → `fct_action_values.original_event_id` → `fct_shots.event_id`/`shot_id` (all three columns fact-checked present); map `xg → xg_set_encoder`, `xg_ci_low → xg_ci_lower`, `xg_ci_high → xg_ci_upper`; carry `match_key`/`competition_key`/`competition_id`/`team_key`/`player_key` from `fct_action_values`/`fct_shots`/`dim_matches`. **Restrict to `data_source IN ('statsbomb','wyscout')`** (legacy coverage). Because M2 StatsBomb is context-aware, the view's values are equal-or-better than legacy v2 — no downgrade.
- [ ] **Step 2 — P-5 consumer verification (each, not "asserted safe"):** verify every `fct_xg_predictions_v2` consumer resolves against the view — Taipy shot-map page, HF `xg-shots` publisher, `refresh_synced_tables`/`create_indexes` (if synced). If a TRIGGERED synced mart, follow ADR-043 strand-safe (`rederive_synced_marts.py`); state the view-vs-materialized choice per latency-sensitivity. Commit (APPROVAL GATE).
- [ ] **Step 3 — final acceptance gate (APPROVAL GATE, live):** `fct_shot_xg` populated for ALL shot cohorts (SB-360 + GS/SC context-aware, others zero-context) with calibrated xG + CI + mode + ood_flag; RM never public (leak guard); `fct_xg_predictions_v2` consumers on the view (no downgrade vs legacy); cross-mart anti-join = 0; full `pytest` + `lint-imports` + `validate_workflow_cards` + all dbt/live gates green; model card metrics updated with the full-corpus OOS. **Project done: acceptance criteria §9 all satisfied.**

---

## Self-Review (against the spec)

- **§A1 SB-360 conversion** → Tasks 2.1 (module, `_convert_locations`, co-location golden N1/N5, conversion/orientation split), 2.2 (wire + live gate incl. co-location on live shots). ✅
- **§A2 access_tier** → 1.1. ✅ · **§A3/A4 publishers** → 1.2/1.3 (split, registry, cards, m1). ✅
- **§B1 join** → 1.4 · **§B2 read-both/fail-loud** → 1.5 · **§B3 uniform features** → 1.6 · **§B4 single calibration (fit here, apply in C)** → 1.6/1.9 (M1) · **§B5 card** → 1.7 · **§B6 tests** → 1.4–1.7, 2.1 · **§B7 retrain** → 1.8/2.3. ✅
- **§5 identity/coords/N2 actor** → conventions + 1.4 + 2.1 + N2 tests (1.7/2.2). ✅
- **§C1 scorer + two-mode gate (M2 selection≠certification, N4 fallback, m2 penalty)** → 1.9 · **§C2 mart + retire v2** → 1.10. ✅
- **§8 gates** → 1.1/1.2/1.3/1.8/1.11/2.2/2.4 live gates + cross-mart 1.10; committed golden 2.1. ✅
- **§9 acceptance** → 1.11 (partial) + 2.4 (full). ✅ · **M3/N6 sequencing** → Milestone split + interim-model note in 1.8. ✅

No placeholders; every task names exact files, a concrete failing test, and an approval-gated commit/live step. `(match_key, action_id)` and the calibration-ownership contract are consistent across tasks.
