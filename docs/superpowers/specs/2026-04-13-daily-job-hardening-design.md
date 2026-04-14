# Daily Job Hardening + Workflows Polish — Design

| Field | Value |
|---|---|
| **Date** | 2026-04-13 |
| **Status** | Draft — pending user approval |
| **Branch** | `feat/daily-job-hardening` |
| **TODO items** | D59, SEC2, D56, plus a workflows-page UI tweak |
| **Out of scope (originally proposed, removed during scoping)** | D61 (plotly 6 upgrade), D60 (pyright tightening), SEC1 (EU AI Act), SEC4 (CI SP least-privilege — investigation findings folded back into the SEC4 TODO entry instead) |

## Why this cycle

Four threads share an operational-resilience theme and compose without conflict:

1. **D59** closes the synced-table refresh automation loop opened in PR #116 (session 37). The daily Databricks job currently produces bronze + warm-tier observability data but never builds gold — `dbt build` only runs from developer machines via `scripts/dbt_build_and_refresh.py`. After this cycle the daily job is self-sufficient: bronze → gold (dbt) → Lakebase refresh, all unattended.
2. **SEC2** adds defense-in-depth integrity verification when loading model artifacts from MLflow and UC Volume. Closes SEC-AUDIT-v1.12.0 ML-02 (CWE-345).
3. **D56** corrects 7 academic-citation issues across the UI, workflow cards, and `NOTICE`. Two are flagged as Critical in the original audit (Spearman 2017 URL pointing at the 2018 paper; Rathke 2017 cited with a 2019 DOI).
4. **Workflows UI tweak** replaces the conflated "Last Duration" column with the verifiable three-way decomposition `Cold Start | Guard Duration | Workflow Duration`. The data already exists in `fct_workflow_costs.sql:137,141` — this is purely a query-layer + renderer change.

## Goals

- The daily Databricks job runs the full bronze → gold → Lakebase refresh pipeline unattended on a 06:00 UTC schedule, with no developer machine intervention.
- Every model load from MLflow or UC Volume optionally verifies a SHA-256 hash, logging a warning on first observation (no hash recorded yet) and failing closed when an expected hash exists and does not match.
- The 7 D56 citation issues are corrected, and the 8 workflow cards with empty `references:` lists either have populated references (6 cards) or carry a documented "no academic methodology — operational plumbing" comment (2 cards).
- The Workflows page table shows three independent timing columns (Cold Start, Guard Duration, Workflow Duration) instead of one conflated "Last Duration", and the three values add up to approximately the Databricks task wall-clock for verification.

## Non-goals

- Bootstrap of historical artifact hashes for SEC2 — ships as part of this cycle as a one-off script, but is **not** required to be run before merge. The helper fails open (warning + log) when no hash is recorded, so wiring is non-breaking.
- Replacing workspace admin / account admin grants on the CI SP — folded into the SEC4 TODO entry and deferred to a future cycle.
- Adding new academic citations beyond what D56 + the 2 newly-found cards already require — no methodology research, all citations reused from the project's existing canonical list.
- Adding new functionality to the Workflows page beyond the column rename — the existing filter, drilldown, and stats logic is unchanged.

---

## Item 1 — D59: dbt build inside the daily Databricks job

### Current state (verified)

- The daily job is defined in `terraform/modules/workflows/main.tf:41-891`. It contains 27 tasks: 26 ingestion/compute tasks + 1 `refresh_synced_tables` task added in PR #116.
- Schedule: `quartz_cron_expression = "0 0 6 * * ?"` UTC, paused in dev (`terraform/modules/workflows/main.tf:55-59`).
- Run-as identity: `var.run_as_sp_application_id`, wired from `module.service_principals.ingestion_sp_application_id` (`terraform/environments/dev/main.tf:140`).
- The `refresh_synced_tables` task (`terraform/modules/workflows/main.tf:727-768`) `depends_on` 9 leaf compute tasks (`run_model_validation`, `hf_sync`, `compute_formations_shape_graph`, `compute_embeddings_v1`, `compute_off_ball_xt`, `compute_line_breaking`, `compute_defcon_lite`, `compute_xg_model_v2`, `extract_tracking_metadata`).
- **No dbt task exists in the job.** `dbt build` only runs locally via `scripts/dbt_build_and_refresh.py:24-57` (verified read of file).
- `scripts/dbt_build_and_refresh.py` shells out to `dbt build` then `python -m ingestion.refresh_synced_tables --wait`. Refresh is skipped on dbt failure (`scripts/dbt_build_and_refresh.py:35-40`).
- `dbt_project/profiles.yml:8-10` reads connection from `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN` env vars. Three targets (`dev`, `prod`, `ci`).
- Ingestion SP grants on the gold schema: `terraform/modules/catalog/main.tf:118-125` — only `USE_SCHEMA, SELECT`. **Not enough for dbt to materialize tables.** dbt needs `CREATE_TABLE, MODIFY` on the gold schema to drop+recreate tables under the `materialized='table'` strategy used by 33 of 33 gold models (verified via `dbt_project/models/marts/fct_workflow_costs.sql:2`).
- Ingestion SP `CAN_USE` on the SQL warehouse already exists at `terraform/environments/dev/main.tf:230-232`.
- The CI dbt workflow (`.github/workflows/dbt-ci.yml:48-50`) only runs `dbt parse` (no warehouse connection). The comment at lines 66-75 explicitly documents that `dbt build --empty` is not run in CI because "the databricks-sql-connector hangs on OpenSession for 900s from GitHub runners". This is fact relevant to the next item: there is no existing CI test that exercises dbt against a real warehouse, so D59's E2E verification will be the first time dbt runs from a non-developer-machine context.

### Approach decision

There are three viable paths to run dbt inside a Databricks job. I evaluated all three before recommending one.

| Approach | Pros | Cons |
|---|---|---|
| **A. `dbt_task` block + `git_source` job-level setting** | Idiomatic Databricks pattern. Native auth via run-as SP. No wheel changes. | `git_source` is a job-level setting in the Terraform provider — applying it to the daily job would force every task to be Git-based, breaking the existing wheel-task model. Verified in `databricks-terraform-provider` docs (job-level `git_source` block). Mixing wheel-task + dbt_task in the same job is not officially supported. |
| **B. `notebook_task` calling dbt CLI via `%sh`** | No wheel changes. Notebook lives in workspace. | Requires syncing a notebook file into the workspace (new infra: `databricks_notebook` resource + workspace path management). Splits dbt config across the repo and the workspace. Notebook execution context is heavier than a wheel task. |
| **C. `python_wheel_task` + dbt bundled into the wheel + new `dbt_build` entry point** ⭐ | Single deployment artifact (matches the existing 38-entry-point wheel pattern). Version-locks the dbt project to the wheel SHA. Auth via runtime-injected SP identity (same path every other wheel task uses). No new infrastructure. | Bundling `dbt_project/` into a Python wheel is unusual. Wheel size grows by ~few hundred KB. Requires a `dbt_runner.py` shim that invokes `dbt-core`'s programmatic API. |

**Recommendation: C (wheel-bundled).** It matches the project's "everything ships via the wheel" deployment model (`pyproject.toml` already declares 5 packages and 38 entry points). It also avoids introducing two new things (Workspace Repos sync OR job-level `git_source` switchover) for a single new task.

### Components changed

1. **`pyproject.toml`** — bundle `dbt_project/` into the wheel via Hatch `[tool.hatch.build.targets.wheel] force-include` block. Add `dbt-databricks>=1.8.0` and `dbt-core>=1.8.0` to a new `[project.optional-dependencies] dbt` extra. Add a new entry point: `dbt_build = "ingestion.dbt_runner:main"`. Bump wheel version `0.3.1 → 0.3.2` (`src/shared/wheel.py`).
2. **New file `src/ingestion/dbt_runner.py`** — programmatic dbt invocation via `dbt.cli.main.dbtRunner`. Resolves the bundled `dbt_project` location via `importlib.resources` or `pathlib.Path(__file__).parent.parent.parent / "dbt_project"`. Calls `dbtRunner().invoke(["build", "--profiles-dir", "<bundled>", "--target", "dev"])`. Wraps with the `@workflow("wf-dbt-build", phase="ingestion")` decorator and `timed_check` guard pattern (the guard is a simple "always-run" stub since dbt has its own incremental logic; included for conformance with `test_guard_conformance.py`). Returns the count of models built.
3. **New file `workflow-cards/wf-dbt-build.yaml`** — workflow card for the new pipeline. References none (operational plumbing — same disposition as `wf-sync-hf-costs.yaml`). Documents inputs (bronze schema), outputs (gold schema models), execution config, dependencies (the 9 leaf compute tasks), idempotency (dbt's own incremental logic).
4. **`terraform/modules/workflows/main.tf`** — add a new `task` block with `task_key = "dbt_build"`, `python_wheel_task { entry_point = "dbt_build" }`, `depends_on` the same 9 leaf tasks that `refresh_synced_tables` currently depends on, `environment_key = "dbt"`. Add a new `environment` block for `dbt` with the wheel + `dbt-databricks>=1.8.0` deps. Update `refresh_synced_tables` task's `depends_on` to depend on `dbt_build` (single dependency replacing the 9 — chain becomes `compute → dbt_build → refresh_synced_tables`).
5. **`terraform/modules/catalog/main.tf:118-125`** — expand the ingestion SP gold schema grant from `USE_SCHEMA, SELECT` to `USE_SCHEMA, CREATE_TABLE, MODIFY, SELECT`. **This is the only SP-grant change in the cycle.** Comment line referencing D59.
6. **`dbt_project/profiles.yml`** — add a `databricks` target that uses `auth_type: oauth-m2m` with no client_id/secret (relies on runtime-injected SP identity). Keep the existing `dev`/`prod`/`ci` targets. The new target name is `serverless` and is selected via `--target serverless` in the entry point. Verified: dbt-databricks 1.8+ supports OAuth M2M with runtime SP identity discovery via the `databricks-sdk` (assumption to verify against the exact pinned version during impl).
7. **`src/shared/wheel.py`** — bump `WHEEL_VERSION` to `0.3.2`. Then run `uv run python scripts/bump_wheel.py` to propagate to all consumers (`scripts/*_hf.py` PEP 723 headers, `terraform/environments/dev/main.tf:137`, `deploy.sh`).

### Data flow

```
06:00 UTC daily
  ↓
[26 existing ingestion/compute tasks complete in their normal DAG]
  ↓ (9 leaf tasks)
dbt_build (NEW)
  ↓
refresh_synced_tables (existing, now depends on dbt_build instead of the 9 leaves)
  ↓
soccer_analytics.observability.workflow_cost_live (warm tier)
soccer_analytics.dev_gold.* (33 mart tables, freshly built by dbt)
Lakebase synced tables (refreshed)
```

### Error handling

- dbt build failure → task fails → `refresh_synced_tables` skipped due to `depends_on` (Databricks Jobs default). No data corruption: dbt's transactional `CREATE OR REPLACE TABLE` semantics leave gold tables in the previous-good state.
- Warehouse auto-stopped → dbt-databricks 1.8+ adapter sends SQL which auto-resumes the warehouse (verified in adapter source: `dbt-databricks/src/dbt/adapters/databricks/connections.py` calls `start_warehouse_if_stopped`). The CLAUDE.md note about MSYS path mangling does not apply here because the daily job runs on Databricks serverless Linux, not Git Bash on Windows.
- SP missing `CREATE_TABLE`/`MODIFY` on gold → dbt fails fast with `INSUFFICIENT_PERMISSIONS`. The Terraform grant change in component 5 is non-optional and must land in the same commit.
- Wheel version mismatch (job runs old wheel before new task wires up) → handled by the wheel-version-bump-then-deploy sequence in the impl plan; new `dbt_build` task references wheel `0.3.2`, deployed before Terraform apply.

### Test plan (TDD)

#### Failing tests written first

1. **`src/tests/test_dbt_runner.py::test_dbt_runner_main_invokes_dbt_build`** — mocks `dbt.cli.main.dbtRunner` and asserts `dbt_runner.main()` calls `invoke(["build", "--profiles-dir", <path>, "--target", "serverless"])`. Asserts the bundled `dbt_project` path is resolved correctly via `importlib.resources`.
2. **`src/tests/test_dbt_runner.py::test_dbt_runner_returns_model_count`** — mocks `dbtRunner.invoke` to return a `dbtRunnerResult` with N successful nodes, asserts `main()` returns N.
3. **`src/tests/test_dbt_runner.py::test_dbt_runner_raises_on_failure`** — mocks `dbtRunner.invoke` to return `success=False`, asserts `main()` raises `RuntimeError` with the failure summary.
4. **`src/tests/test_terraform_workflow_dbt_task.py::test_dbt_build_task_exists_in_rendered_terraform`** — calls `terraform show -json` (or parses the HCL with `python-hcl2`) on the rendered workflows module, asserts a task with `task_key == "dbt_build"` exists.
5. **`test_terraform_workflow_dbt_task.py::test_dbt_build_task_depends_on_nine_leaf_compute_tasks`** — same parser, asserts the `dbt_build` task's `depends_on` set equals exactly the 9 leaf task keys (`run_model_validation`, `hf_sync`, `compute_formations_shape_graph`, `compute_embeddings_v1`, `compute_off_ball_xt`, `compute_line_breaking`, `compute_defcon_lite`, `compute_xg_model_v2`, `extract_tracking_metadata`).
6. **`test_terraform_workflow_dbt_task.py::test_refresh_synced_tables_depends_on_dbt_build`** — asserts the `refresh_synced_tables` task's `depends_on` set is exactly `{"dbt_build"}` (single dependency replacing the previous 9).
7. **`test_terraform_workflow_dbt_task.py::test_dbt_environment_has_dbt_databricks_dependency`** — asserts an environment block with `environment_key == "dbt"` exists and contains `dbt-databricks>=1.8.0` in its dependencies.
8. **`test_terraform_workflow_dbt_task.py::test_ingestion_sp_has_create_modify_on_gold`** — parses the rendered catalog module, asserts the ingestion SP gold-schema grant includes `CREATE_TABLE` and `MODIFY` privileges.
9. **`src/tests/test_workflow_card_dbt_build.py::test_wf_dbt_build_card_exists`** — loads `workflow-cards/wf-dbt-build.yaml` via `WorkflowCard.from_yaml_file()`, asserts no validation errors, asserts `id == "wf-dbt-build"`, asserts the card has the right `depends_on` list.
10. **`src/tests/test_pyproject_dbt_build_entry_point.py::test_dbt_build_entry_point_registered`** — parses `pyproject.toml`, asserts `[project.scripts] dbt_build = "ingestion.dbt_runner:main"` exists.
11. **`src/tests/test_pyproject_dbt_project_bundled.py::test_dbt_project_in_wheel_force_include`** — parses `pyproject.toml`, asserts `[tool.hatch.build.targets.wheel] force-include` includes a mapping for `dbt_project`.

All 11 tests written and run failing first. Implementation makes them green one by one.

#### E2E verification (must complete before commit)

12. **Local dbt smoke test**: from a fresh venv, install the new wheel, run `python -m ingestion.dbt_runner --target dev` against the dev warehouse. Verify it builds the same models as `dbt build` would.
13. **Manual Databricks job run (dev)**: deploy wheel `0.3.2` to UC Volume, apply Terraform, manually trigger the daily job in the Databricks UI, observe the `dbt_build` task in the run timeline. Verify exit code 0 and that downstream `refresh_synced_tables` runs after.
14. **Verify gold tables refreshed**: query `soccer_analytics.dev_gold.fct_workflow_costs` (or another mart with a known recent change) and confirm the row count / `updated_at` reflects the job run.
15. **Verify Lakebase synced tables refreshed**: same query through `lakebase_host:5432/...workflow_cost_live_synced` and confirm row count matches.

Each E2E step's command and expected output goes into the commit message body so the user can replay them.

---

## Item 2 — SEC2: Model artifact integrity verification

### Current state (verified)

Four model loaders load from MLflow `@Champion` aliases and/or UC Volume paths. None verify the loaded artifact's integrity.

| Loader | MLflow load points | UC Volume load points |
|---|---|---|
| `src/ingestion/xg_model.py` | `:138` (`mlflow_sklearn.load_model(model_uri)` for xG @Champion), `:145` (logistic baseline from same run's `runs:/{run_id}/logistic_model`) | `:209-211` (`spark.read.format("binaryFile").load(...)` for `logistic_model.json` and `xgboost_model.json` from `/Volumes/{catalog}/dev_gold/model_weights/xg_model/`) |
| `src/ingestion/xg_model_v2.py` | `:90-94` (`mlflow.artifacts.download_artifacts(run_id, artifact_path="model_weights.json")`), `:124` (`mlflow_sklearn.load_model(model_uri)` for the v1 XGBoost dependency) | `:297` (`/Volumes/.../xg_model_v2/model_weights.json`), `:310` (`/Volumes/.../xg_model/`) |
| `src/ingestion/spadl_vaep.py` | `:177` (`mlflow_pyfunc.load_model(model_uri)` for VAEP @Champion, returns scores+concedes XGBClassifiers) | None — VAEP uses raw bytes injection into UDF closures (`:295,299`) |
| `src/ingestion/defcon_lite_common.py` | `:59` (`mlflow_pyfunc.load_model(model_uri)` for DEFCON @Champion regressor) | None — also uses raw bytes injection (`:151`) |

The shared utility module is `src/ingestion/utils.py` (organized into 8 numbered sections — verified at end of file). A new section "9. Artifact Hash Verification" is the natural home.

### Approach decision

- **Hash storage location**: MLflow run **tags** for MLflow loads (key: `artifact_sha256`), sidecar `<file>.sha256` text files for UC Volume loads. Both are standard idioms with zero schema migration.
- **Failure mode**: **Fail open with WARNING** on missing hash, **fail closed with raise** on hash mismatch. Rationale: the cycle does not require historical bootstrap to be complete before merge — verification activates lazily as hashes get recorded.
- **Hash recording**: a separate one-off bootstrap script `scripts/bootstrap_artifact_hashes.py` walks each `@Champion` MLflow run and each UC Volume model directory, computes SHA-256, writes the tag/sidecar. Run once after merge to populate. Subsequent training runs (on HF Jobs / Databricks) record hashes automatically — that's a follow-up TODO not in this cycle.
- **Helper signature**:
  ```python
  def verify_artifact_hash(
      data: bytes,
      expected_sha256: str | None,
      artifact_label: str,
      logger: logging.Logger,
  ) -> None:
      """Verify SHA-256 of an in-memory artifact.

      Args:
          data: The artifact bytes (already loaded into memory).
          expected_sha256: Hex-encoded expected SHA-256, or None if no hash recorded.
          artifact_label: Human label for log/error messages.
          logger: For warning-on-missing-hash messages.

      Raises:
          ArtifactHashMismatch: When expected_sha256 is non-None and does not match.
          ValueError: When expected_sha256 is non-None but malformed (not 64 hex chars).
      """
  ```
- **Error class**: new `ArtifactHashMismatch(RuntimeError)` exported from `src/ingestion/utils.py`.

### Components changed

1. **`src/ingestion/utils.py`** — new section "9. Artifact Hash Verification" containing `ArtifactHashMismatch`, `verify_artifact_hash()`, plus two source-specific helpers: `_load_mlflow_artifact_hash(client, model_name, alias) -> str | None` (reads the `artifact_sha256` tag from the @Champion run) and `_load_volume_sidecar_hash(volume_path: str) -> str | None` (reads `<path>.sha256` if present).
2. **`src/ingestion/xg_model.py`** — wrap MLflow loads at `:138,145` and UC Volume reads at `:209-211` with hash verification. The MLflow path uses `_load_mlflow_artifact_hash`; the Volume path uses `_load_volume_sidecar_hash`.
3. **`src/ingestion/xg_model_v2.py`** — same wrapping at `:90-94, 124, 297, 310`.
4. **`src/ingestion/spadl_vaep.py`** — wrap MLflow load at `:177` (verify the pyfunc artifact's downloaded directory hash via a tarball-of-files SHA-256 since pyfunc artifacts are directories).
5. **`src/ingestion/defcon_lite_common.py`** — same wrapping at `:59`.
6. **New `scripts/bootstrap_artifact_hashes.py`** — one-off CLI: `python scripts/bootstrap_artifact_hashes.py --catalog soccer_analytics --schema dev_gold [--dry-run]`. Walks all 4 model paths, computes SHA-256, writes the tag/sidecar. Idempotent. Uses `WorkspaceClient` for MLflow tag updates and the Files API for UC Volume sidecar writes.

### Test plan (TDD)

#### Failing tests written first

1. **`src/tests/test_verify_artifact_hash.py::test_verify_passes_with_correct_sha256`** — pass `b"hello"` and the precomputed SHA-256 (`2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824`), assert returns None.
2. **`test_verify_artifact_hash.py::test_verify_raises_on_mismatch`** — pass `b"hello"` and a wrong hash, assert `ArtifactHashMismatch` raised. Assert error message contains both expected and actual hashes (so the user can diagnose without re-running).
3. **`test_verify_artifact_hash.py::test_verify_warns_on_missing_hash`** — pass `b"hello"` and `expected_sha256=None`, assert returns None **and** the logger received a WARNING with the artifact label.
4. **`test_verify_artifact_hash.py::test_verify_rejects_malformed_hash`** — pass invalid hex (`"xyz"`, 63 chars, 65 chars), assert `ValueError`.
5. **`test_verify_artifact_hash.py::test_verify_handles_empty_bytes`** — edge case: `data=b""` with the SHA-256 of empty (`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`) passes.  <!-- pragma: allowlist secret -->
6. **`src/tests/test_xg_model_loader_verifies_hash.py::test_mlflow_load_calls_verify`** — patch `mlflow_sklearn.load_model` and `verify_artifact_hash`, run `_try_load_champion_xg`, assert `verify_artifact_hash` was called with the model bytes and the tag-sourced hash.
7. **`test_xg_model_loader_verifies_hash.py::test_volume_load_calls_verify`** — patch `spark.read.format("binaryFile")` and `verify_artifact_hash`, run the UC Volume fallback path in `xg_model.run_pipeline`, assert verification ran on each of the two files.
8. Repeat tests 6-7 for `xg_model_v2.py`, `spadl_vaep.py`, `defcon_lite_common.py` (4 loaders × 2 paths = up to 8 wiring tests, some N/A if a loader has no Volume path).
9. **`src/tests/test_bootstrap_artifact_hashes.py::test_dry_run_lists_artifacts_without_writing`** — mock `WorkspaceClient.experiments.search_runs` and the Files API, assert dry-run mode prints planned operations and writes nothing.
10. **`test_bootstrap_artifact_hashes.py::test_apply_writes_mlflow_tag`** — assert the bootstrap script calls `client.set_tag(run_id, "artifact_sha256", <hex>)` for each MLflow @Champion model.
11. **`test_bootstrap_artifact_hashes.py::test_apply_writes_volume_sidecar`** — assert the script writes `<path>.sha256` files via the Databricks Files API.

#### E2E verification (must complete before commit)

12. **Run the bootstrap script in dry-run mode against the dev workspace** — assert it discovers all 4 models without errors. Capture output for the commit message.
13. **Run the bootstrap script in apply mode** — assert tags and sidecars are written. Re-run to verify idempotency (no-op on second invocation).
14. **Manually trigger one of the 4 daily-job tasks** that loads a verified model (e.g., `compute_xg_model`) — assert task succeeds and the log shows `verify_artifact_hash: hash matches for xg_model_logistic` (or equivalent).

---

## Item 3 — D56: Academic reference audit & remediation

### Scope (verified per-issue)

| # | Severity | Issue | Affected file(s) |
|---|---|---|---|
| 1 | Critical | Spearman (2017) cited with the 2018 "Beyond Expected Goals" ResearchGate URL | `hf_taipy_app/src/pages/pitch_control.py:18`, `hf_taipy_app/src/pages/movement_analysis.py:17` |
| 1b | Critical | NOTICE file also has the same Spearman 2017 + "Beyond Expected Goals" mismatch (TODO incorrectly claimed NOTICE was correct — verified at `NOTICE:54-58`) | `NOTICE:54-58` |
| 1c | Critical | **The implementation source code docstring also has the mismatch** — `src/analytics/pitch_control.py:1` says "(Spearman 2017)" then `:7` says `Reference: Spearman (2017) "Beyond Expected Goals"`. Same wrong title as UI and NOTICE. | `src/analytics/pitch_control.py:1, 7` |
| 2 | Critical | "Rathke (2017)" cited with DOI `10.1515/jqas-2019-0044` (a 2019 JQAS publication) | `hf_taipy_app/src/pages/match_summary.py:16`, `hf_taipy_app/src/pages/shot_map.py:17` |
| 3 | High | `wf-defcon.yaml` has `references: []` despite implementing Kim et al. (2025) DEFCON | `workflow-cards/wf-defcon.yaml:16` |
| 4 | High | Danesi (2025) DOI inconsistency: UI Springer DOI vs workflow-card "informal HPI/Hudl" | `hf_taipy_app/src/pages/*.py` (need grep for Danesi), `workflow-cards/wf-football2vec-v2.yaml:19` |
| 5 | Medium | Sotudeh institution mismatch: UI links `essay.utwente.nl`, `wf-shape-graphs.yaml:18` says "ETH Zurich (DISS. ETH NO. 31732)" | UI page (need grep), `workflow-cards/wf-shape-graphs.yaml:18` |
| 6 | Medium | `ARCHITECTURE.md` has no consolidated reference list. 11 UI citations are absent. | `ARCHITECTURE.md` (new Appendix D) |
| 7 | Low | 5 workflow cards with `references: []` (`wf-line-breaking`, `wf-entity-resolution`, `wf-model-validation`, `wf-import-psxg`, `wf-prepare-360-data`) plus 2 newly-found cards | `workflow-cards/wf-{...}.yaml` |

**Verification commands** the user can re-run:
- `Grep -n "Spearman" NOTICE` → confirms `NOTICE:54-58` mismatch
- `Grep -n "Spearman\|Rathke" hf_taipy_app/src/pages/` → confirms the 4 UI mismatches
- `Grep -l "^references: \\[\\]" workflow-cards/*.yaml` → 8 cards (the 6 in TODO + 2 found during scoping)

### Per-issue resolution plan

**Issue 1 + 1b + 1c — Spearman 2017 fix. Direction confirmed during scoping.** The project already has the canonical 2017 citation at `workflow-cards/wf-pitch-control.yaml:16`: `"Spearman (2017). Physics-Based Modeling of Pass Probabilities in Soccer. MIT Sloan Sports Analytics Conference."` This matches the implementation: `src/analytics/pitch_control.py:5-7` describes "time-to-intercept kinematic equations" — the framework introduced in the 2017 Physics-Based paper. The 2018 "Beyond Expected Goals" paper builds *on* the 2017 pitch control framework to compute EPV, but the pitch control model itself is the 2017 contribution. **Five fix sites, all in the same direction:** (1) `pitch_control.py:18` UI Citation: title becomes "Physics-Based Modeling of Pass Probabilities in Soccer", URL becomes `https://www.sloansportsconference.com/research-papers/physics-based-modeling-of-pass-probabilities-in-soccer` (or removed if URL is unreachable during impl). (2) `movement_analysis.py:17` same. (3) `NOTICE:56-58` same. (4) `src/analytics/pitch_control.py:1` docstring summary line. (5) `src/analytics/pitch_control.py:7` `Reference:` line.

**Issue 2 — Rathke fix. Direction confirmed (Option A approved 2026-04-13).** Verified during scoping: `Grep "Rathke" src/analytics/` returns **zero matches**. The Rathke citation is not anchored in any implementation source file — it is decorative-only in the UI. The actual xG implementation (`src/analytics/xg_model.py:1-12`) is described as "Custom xG model — logistic regression baseline + gradient-boosted XGBoost" with no specific paper attribution.

**Approved fix direction:** **Replace Rathke with Robberechts & Davis (2020)** — the project-canonical xG citation already exists at `workflow-cards/wf-xg-v1.yaml:16`: `"Robberechts & Davis (2020). How Data Availability Affects the Ability to Learn Good xG Models."` Reusing it unifies the UI with the workflow card and matches what the implementation actually does (data-driven feature selection). Both `match_summary.py:16` and `shot_map.py:17` get the same fix:

```python
# Before:
Citation("Rathke (2017)", "https://doi.org/10.1515/jqas-2019-0044"),
# After:
Citation("Robberechts & Davis (2020)", "<canonical-URL-to-determine-during-impl>"),
```

The Robberechts & Davis (2020) paper is "How Data Availability Affects the Ability to Learn Good xG Models" — the canonical URL (arXiv, conference proceedings, or DOI) is determined during impl by reading `wf-xg-v1.yaml` for any URL it carries, or by a quick external lookup. **Citation text is locked; URL is the only remaining sub-decision.**

**Issue 3 — wf-defcon citations.** Kim et al. 2025 DEFCON is the methodology. **Verification flag during impl**: locate the canonical DEFCON citation in the project (`Grep "Kim.*DEFCON\|DEFCON.*Kim" --type yaml --type py --type md`). If the citation exists in CLAUDE.md or another card, reuse it verbatim. If not, derive from `src/analytics/defcon_lite.py` module docstring.

**Issue 4 — Danesi reconciliation. Direction confirmed during scoping.** All three sources have different Danesi citations:

- **Implementation source code** (`src/analytics/football2vec_transformer.py:20`, `src/analytics/football2vec_360.py:11`): `Danesi, P. (2025). "Football2Vec: Transformer-Based Player Embeddings."` ← This is the canonical title because the implementation references it directly.
- **UI** (`hf_taipy_app/src/pages/player_similarity.py:21`): `Citation("Danesi (2025) — Football2Vec", "https://doi.org/10.1007/978-3-031-02044-5_2")` — title abbreviated, Springer DOI for what appears to be a 2022 Springer book chapter (DOI prefix `978-3-031-02044-5` is a 2022 publication, year doesn't match Danesi 2025).
- **Workflow card** (`workflow-cards/wf-football2vec-v2.yaml:19`): `"Danesi (2025). The Imposter on the Pitch. HPI/Hudl."` — entirely different title, suggests a different work or a mistaken citation.

**Fix direction:** standardize on the implementation-source canonical: `"Danesi, P. (2025). Football2Vec: Transformer-Based Player Embeddings."` Update wf-football2vec-v2.yaml:19 to use this title. Update player_similarity.py:21 UI Citation to expand the abbreviated title. **Decision flag for the URL only**: the Springer DOI in the UI is suspect. During impl, verify whether `10.1007/978-3-031-02044-5_2` actually resolves to a Danesi paper. If yes, keep it. If no, replace with the actual source URL (or remove the URL parameter, which is supported per `Citation` dataclass conventions in the project).

**Issue 5 — Sotudeh institution. Direction confirmed during scoping.** Verified two facts:

- **Implementation source** (`src/analytics/shape_graph_construction.py:4-6`): `"Sotudeh, H. (2026). Identification of Team Tactical Formations and Player Positions in Association Football. PhD thesis, ETH Zurich (DISS. ETH NO. 31732). Published: npj Complexity, DOI: 10.1038/s44260-025-00047-x."` — matches `wf-shape-graphs.yaml:18` exactly.
- **UI** (`hf_taipy_app/src/pages/tactical_positions.py:29-30`): `Citation("Sotudeh (2026) — Shape Graph Formation Detection", "https://essay.utwente.nl/104491/")` — links the University of Twente thesis (`essay.utwente.nl/104491/`), which is Sotudeh's MSc thesis, NOT the PhD work the implementation references.

**Fix:** update `tactical_positions.py:29-30` to:
```python
Citation(
    "Sotudeh (2026) — Identification of Team Tactical Formations and Player Positions in Association Football, ETH Zurich (DISS. ETH NO. 31732)",
    "https://doi.org/10.1038/s44260-025-00047-x",  # npj Complexity DOI
),
```

**Issue 6 — ARCHITECTURE.md Appendix D.** Append a new section "Appendix D — Academic References" listing all 11 cited papers (Anzer & Bauer, Suzuki, Rathke, Trainor & Chassy, Pena & Touchette, Frencken, Bourbousson, Karun Singh, Donnelly, Danesi, Sotudeh) with their canonical citation strings (matching the workflow cards) and a short note on which UI page / module uses each.

**Issue 7 — 8 workflow cards with empty references.** Per the analysis approved earlier:

| Card | Disposition |
|---|---|
| `wf-defcon.yaml` | Add Kim et al. (2025) DEFCON citation — issue 3 |
| `wf-line-breaking.yaml` | Add references: parmacalcio1913 line-breaking-passes (Apache 2.0) + StatsBomb 360 dataset attribution. Both exist in NOTICE:66-71. |
| `wf-entity-resolution.yaml` | Leave `references: []` with a YAML comment block (same pattern as `wf-sync-hf-costs.yaml`): "operational data plumbing — fuzzy string matching via rapidfuzz + sparse-dot-topn TF-IDF using established libraries, no novel methodology". |
| `wf-model-validation.yaml` | Leave `references: []` with a YAML comment block listing the three techniques (PSI, Wasserstein distance, CUSUM Page 1954) and noting "mixed textbook statistical-process-control methodology — no single canonical citation". This is consistent with the operational-plumbing pattern; adding only Page (1954) for CUSUM would understate the other two and overstate CUSUM's relative weight. |
| `wf-import-psxg.yaml` | Add reference: Butcher et al. (2025) (canonical project citation per `wf-goalkeeper.yaml:17`) |
| `wf-prepare-360-data.yaml` | Add reference: StatsBomb 360 dataset attribution. |
| `wf-export-shots.yaml` (newly found) | Add references: Butcher et al. (2025) (methodology) + StatsBomb Open Data (dataset) — both project-canonical |
| `wf-sync-hf-costs.yaml` (newly found) | Leave `references: []` with a YAML comment block above it explaining "operational telemetry plumbing — no academic methodology". Comment serves as the audit trail. |

### Test plan (TDD)

#### Failing tests written first

1. **`src/tests/test_citation_consistency.py::test_no_spearman_2017_with_beyond_expected_goals_url`** — scans all `.py` files under `hf_taipy_app/src/pages/` for `Citation("Spearman (2017)"...)` patterns, asserts none link to `Beyond_Expected_Goals` URLs.
2. **`test_citation_consistency.py::test_notice_spearman_2017_title_correct`** — reads `NOTICE`, asserts the line containing "Spearman, W. (2017)" mentions "Physics-Based Modeling of Pass Probabilities" (or asserts the year is 2018 if that's the implementation source — set during impl after verifying which paper the code uses).
3. **`test_citation_consistency.py::test_no_rathke_2017_with_2019_doi`** — scans pages for `Citation("Rathke (2017)"...)` linking `10.1515/jqas-2019-0044`, asserts none.
4. **`test_citation_consistency.py::test_sotudeh_citation_consistent_with_workflow_card`** — verifies any UI Citation referencing Sotudeh matches the institution claimed in `wf-shape-graphs.yaml:18`.
5. **`src/tests/test_workflow_card_references.py::test_no_workflow_card_has_undocumented_empty_references`** — for each `workflow-cards/wf-*.yaml` with `references: []`, opens the raw YAML file text and asserts a `# No academic methodology` (or `# Operational`) comment exists in the 5 lines immediately preceding the `references: []` line. Catches future drift.
6. **`test_workflow_card_references.py::test_psxg_pipeline_cards_share_butcher_citation`** — loads `wf-export-shots.yaml`, `wf-import-psxg.yaml`, `wf-goalkeeper.yaml`, asserts all three have a Reference with `citation` containing "Butcher" and `role == "methodology"`.
7. **`test_workflow_card_references.py::test_wf_defcon_has_kim_2025_citation`** — loads `wf-defcon.yaml`, asserts at least one Reference with `citation` containing "Kim" and "DEFCON" and `role == "methodology"`.
8. **`src/tests/test_architecture_md_appendix.py::test_appendix_d_lists_all_eleven_citations`** — reads `ARCHITECTURE.md`, asserts a heading like `## Appendix D — Academic References` exists and contains all 11 author surnames listed in the D56 audit.

#### E2E verification (must complete before commit)

9. **Render each affected UI page locally**: start the Taipy app via `python -m hf_taipy_app.src.main`, navigate Puppeteer to Pitch Control, Movement Analysis, Shot Map, Match Summary, Player Similarity (Sotudeh), and Goalkeeper (PSxG). Visually confirm each Citation block renders with the corrected text and links.
10. **Workflow card validation**: run `uv run validate_workflow_cards` and assert exit 0 (catches Pydantic validation errors from the new Reference entries).

---

## Item 4 — Workflows page UI: Guard Duration + Workflow Duration columns

### Current state (verified)

- `WF_TABLE_COLS` at `hf_taipy_app/src/state/workflows_stats.py:230-243` lists 12 columns including `"Last Duration"` at line 237 and `"Cold Start"` at line 238.
- `build_table_data()` at `workflows_stats.py:246-401` populates the table. The `Last Duration` cell at line 393 is filled from `duration_str`, which comes from `_pick_latest_run(jobs_last_run_ts, jobs_duration_secs, hf_last_run_ts, hf_duration_secs)` at line 339-341.
- `jobs_duration_secs` (line 323) is `job_run.get("duration_seconds", 0)` — from the Databricks Jobs API run metadata, not from the cost table. This is **total task wall-clock**, including cold start + guard + workflow body + shutdown.
- `cold_start_seconds` (line 285) is loaded from `latest_run_metrics["cold_start_seconds"]` and rendered at line 394 as the `Cold Start` cell.
- **The cost-table query already pulls `duration_seconds` but the UI ignores it**: `hf_taipy_app/src/queries/workflows.py:31` includes `"duration_seconds"` in `_LATEST_RUN_COLS`, and the SQL at `:77` selects it from `fct_workflow_costs_synced`. The dbt model at `dbt_project/models/marts/fct_workflow_costs.sql:137` selects `wt.duration_seconds` from the warm tier. So `latest_run_metrics["duration_seconds"]` is available and unused.
- **The dbt model also exposes `guard_duration_seconds`** at `fct_workflow_costs.sql:91, 141`. The query just doesn't pull it yet — `_LATEST_RUN_COLS` does not include it.

So the UI change requires no schema changes, no dbt model changes — only:
1. Add `guard_duration_seconds` to `_LATEST_RUN_COLS` and the SELECT in `queries/workflows.py:30-35, 77-78, 81-82`.
2. Update `WF_TABLE_COLS` to replace `"Last Duration"` with `"Guard Duration"` + `"Workflow Duration"` (in the natural temporal order: `Cold Start | Guard Duration | Workflow Duration`).
3. In `build_table_data()`, build two new lookups (`guard_duration_lookup`, `workflow_duration_lookup`) from `latest_run_metrics`, render two new cells, drop the old `Last Duration` cell.
4. The Jobs API call still happens for the `Last Run` timestamp pick. Only the duration field stops being displayed.

### Components changed

| File | Change |
|---|---|
| `hf_taipy_app/src/queries/workflows.py:28-35` | Add `"guard_duration_seconds"` to `_LATEST_RUN_COLS` |
| `hf_taipy_app/src/queries/workflows.py:77-89` | Add `guard_duration_seconds` to both SELECT clauses |
| `hf_taipy_app/src/state/workflows_stats.py:230-243` | Replace `"Last Duration"` with `"Guard Duration"` and `"Workflow Duration"`. Reorder so `Cold Start | Guard Duration | Workflow Duration` are contiguous and in temporal order. |
| `hf_taipy_app/src/state/workflows_stats.py:280-287` | Build `guard_duration_lookup` and `workflow_duration_lookup` (sibling to existing `cold_start_lookup`, `entity_count_lookup`) |
| `hf_taipy_app/src/state/workflows_stats.py:289-401` | Drop the `_pick_latest_run` duration logic for display. Render two new cells from the lookups using the existing `m{N}s` format. Keep `_pick_latest_run` for the timestamp (used by `Last Run` and `Status`). |
| `src/tests/test_workflows_cost_wiring.py` | Extend tests (see test plan) |

### Test plan (TDD)

#### Failing tests written first

1. **`test_workflows_cost_wiring.py::TestLatestRunMetrics::test_latest_run_metrics_includes_guard_duration`** — extends the existing test at line 142, adds `"guard_duration_seconds"` to the assertion list. Will fail because the SELECT does not yet include the column.
2. **`test_workflows_cost_wiring.py::TestTableColumns::test_table_does_not_have_last_duration_column`** — asserts `"Last Duration" not in WF_TABLE_COLS`. Regression guard.
3. **`TestTableColumns::test_table_has_guard_duration_column`** — asserts `"Guard Duration" in WF_TABLE_COLS`.
4. **`TestTableColumns::test_table_has_workflow_duration_column`** — asserts `"Workflow Duration" in WF_TABLE_COLS`.
5. **`TestTableColumns::test_temporal_columns_are_contiguous`** — asserts `WF_TABLE_COLS` has `"Cold Start"`, `"Guard Duration"`, `"Workflow Duration"` in that order with no other columns between them.
6. **`TestTableColumns::test_guard_duration_populated_from_latest_run`** — extends `_make_latest_run_metrics()` fixture with `guard_duration_seconds=[5, 3, 8]`, asserts `df.loc["wf-vaep", "Guard Duration"]` == `"5s"`.
7. **`TestTableColumns::test_workflow_duration_populated_from_latest_run`** — same pattern, asserts the workflow_duration cell shows the cost-table value (e.g., `"2m 0s"` for `duration_seconds=120`), NOT the Jobs API value.
8. **`TestTableColumns::test_workflow_duration_does_not_use_jobs_api`** — passes a `job_runs` dict with `duration_seconds=999` and a `latest_run_metrics` with `duration_seconds=120`, asserts the rendered cell shows `"2m 0s"` (from cost table) NOT `"16m 39s"` (from Jobs API). This is the regression guard for the source-of-truth swap.
9. **`TestTableColumns::test_temporal_decomposition_addition_holds`** — passes `cold_start=45, guard=5, workflow=120` and a `job_runs` dict with the corresponding `duration_seconds=180` (cold + guard + workflow ≈ wall-clock). Asserts the three displayed values sum to within 5 seconds of the Jobs API wall-clock. This is the user-visible verification path the cycle is built around.

#### E2E verification (must complete before commit)

10. **Local Taipy run + Puppeteer**: start `python -m hf_taipy_app.src.main`, navigate to AI/ML Workflows page, capture screenshot of the table. Visual check: the three columns appear in order, values are non-empty, the math adds up.
11. **Staging deploy + Puppeteer**: deploy to `luxury-lakehouse/staging`, repeat the Puppeteer check against the staging URL with real cost data.

---

## Cross-cutting concerns

### Order of work (suggested)

The 4 items are independent enough that they can be implemented in any order. Recommended order:

1. **Workflows UI tweak** first — smallest item, clean test scaffold to extend, no infra changes. Provides early momentum and gives me a cleanly testable PR-style chunk before tackling D59.
2. **D56 academic refs** — pure text edits, can be done while waiting for any D59 manual job triggers.
3. **SEC2** — independent of D59, no infra entanglement.
4. **D59 last** — biggest, requires manual Databricks job runs, depends on the SP grant change which itself requires Terraform apply.

### Single-commit policy + E2E gate

Per the user's "minimal commits with E2E testing before commits when possible" rule:

- All 4 items land in **one commit** at the end of the cycle.
- E2E verification for each item completes BEFORE staging (and thus before the commit).
- The commit message body lists every E2E command run + expected output, so the user can replay and audit.
- If any item proves harder than expected and would delay the others, propose splitting that item into its own commit (with explicit user approval first per the "no commits without explicit approval" rule).

### Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| dbt-databricks 1.8+ OAuth M2M auth doesn't resolve runtime SP identity on serverless | Medium | Verify via dbt-databricks source code during impl; fallback path is to inject SP credentials via dbutils.secrets |
| Bundling `dbt_project/` into the wheel breaks Hatch build | Low | Verify with a local `uv build` before wiring it into the Terraform apply |
| Ingestion SP `CREATE_TABLE`/`MODIFY` grant on gold conflicts with existing dbt-from-developer-machine flow | Low | Developer machines authenticate as users (account users group), not as the SP — different grant path |
| SHA-256 verification breaks an existing model load that has no recorded hash | Low | Helper fails open with WARNING when hash is None — non-breaking until bootstrap script runs |
| Spearman/Rathke citation fixes pick the wrong direction (year vs title) | Medium | Implementation phase reads `src/analytics/pitch_control.py` and shot xG model code to determine which paper is actually implemented; only then commits to a fix direction |
| Workflow cards `references: []` test rejects legitimate empty-with-comment cases | Low | Test explicitly checks for the comment block above the `references: []` line; ships with correct comments for both newly-found cards |

### Observability impact

- The cycle adds a new `dbt_build` workflow row to `workflow_cost_live` (via the standard `CostEstimateHook` path the wheel-task pattern triggers).
- The new Workflows UI columns make `guard_duration_seconds` user-visible for the first time. After merge, the user can verify the three-way decomposition (`cold_start + guard + workflow ≈ wall-clock`) on any row in the table and use it to spot pipelines where the guard takes a disproportionate share of total time.

---

## Appendix A — File citation index

Every factual claim in this design doc is sourced from one of these files. Use this index to verify any individual claim.

- `terraform/modules/workflows/main.tf:41-891` — daily job, 27 tasks, refresh_synced_tables wiring
- `terraform/modules/catalog/main.tf:118-125` — ingestion SP gold schema grants (the change site for D59)
- `terraform/environments/dev/main.tf:140` — daily job run-as binding
- `terraform/environments/dev/main.tf:226-238` — SQL warehouse permissions
- `dbt_project/models/marts/fct_workflow_costs.sql:91, 137, 141` — guard_duration + duration_seconds in the dbt model
- `dbt_project/profiles.yml:1-29` — current dbt connection profiles
- `scripts/dbt_build_and_refresh.py:24-57` — current local-only dbt invocation
- `scripts/create_cost_table.sql:18, 21` — cost table column definitions
- `src/ingestion/cost_hook.py:160-221` — _build_row schema and MERGE
- `src/ingestion/guards.py:21-62` — FilterResult + timed_check
- `src/ingestion/utils.py:1-650` — utilities module structure (8 numbered sections)
- `src/ingestion/xg_model.py:117-211` — MLflow + UC Volume load points
- `src/ingestion/xg_model_v2.py:70-310` — same
- `src/ingestion/spadl_vaep.py:155-300` — VAEP MLflow load + raw bytes
- `src/ingestion/defcon_lite_common.py:40-167` — DEFCON MLflow load + raw bytes
- `src/ingestion/export_shots_on_target.py:1-227` — wf-export-shots full implementation
- `src/ingestion/sync_hf_costs.py:1-287` — wf-sync-hf-costs full implementation
- `src/workflows/runner.py:39-100` — run_workflow + ctx injection
- `src/workflows/card.py:59-262` — WorkflowCard + Reference Pydantic model
- `src/tests/test_workflows_cost_wiring.py:1-316` — existing test file to extend
- `hf_taipy_app/src/queries/workflows.py:28-94` — fetch_latest_run_metrics + _LATEST_RUN_COLS
- `hf_taipy_app/src/state/workflows_stats.py:230-401` — WF_TABLE_COLS + build_table_data
- `hf_taipy_app/src/pages/pitch_control.py:13-18` — Spearman 2017 mismatch site
- `hf_taipy_app/src/pages/movement_analysis.py:12-17` — same
- `hf_taipy_app/src/pages/match_summary.py:12-16` — Rathke 2017/2019 mismatch site
- `hf_taipy_app/src/pages/shot_map.py:12-17` — same
- `workflow-cards/wf-goalkeeper.yaml:17` — canonical Butcher (2025) PSxG citation reused by D56 fixes
- `workflow-cards/wf-pitch-control.yaml:16` — canonical Spearman (2017) "Physics-Based Modeling of Pass Probabilities" citation
- `workflow-cards/wf-shape-graphs.yaml:18` — canonical Sotudeh ETH Zurich citation
- `workflow-cards/wf-football2vec-v2.yaml:19` — Danesi (2025) workflow card citation
- `workflow-cards/wf-defcon.yaml:16`, `wf-line-breaking.yaml:15`, `wf-entity-resolution.yaml:15`, `wf-model-validation.yaml:16`, `wf-import-psxg.yaml:16`, `wf-prepare-360-data.yaml:17`, `wf-export-shots.yaml:16`, `wf-sync-hf-costs.yaml:16` — 8 cards with `references: []`
- `NOTICE:54-58` — Spearman (2017) + "Beyond Expected Goals" mismatch (TODO claim that NOTICE was correct is wrong)
- `.github/workflows/dbt-ci.yml:48-50, 66-75` — dbt CI runs only `dbt parse`, no warehouse connection
- `.github/workflows/terraform-plan.yml:23` — terraform plan auth via `DATABRICKS_AUTH_TYPE: github-oidc`

## Appendix B — Cycle deliverables (explicit list for the implementation plan)

When the writing-plans skill takes over, it must produce a plan that delivers all of:

1. New file: `src/ingestion/dbt_runner.py`
2. New file: `workflow-cards/wf-dbt-build.yaml`
3. New file: `scripts/bootstrap_artifact_hashes.py`
4. New file: `src/tests/test_dbt_runner.py`
5. New file: `src/tests/test_terraform_workflow_dbt_task.py`
6. New file: `src/tests/test_workflow_card_dbt_build.py`
7. New file: `src/tests/test_pyproject_dbt_build_entry_point.py`
8. New file: `src/tests/test_pyproject_dbt_project_bundled.py`
9. New file: `src/tests/test_verify_artifact_hash.py`
10. New file: `src/tests/test_xg_model_loader_verifies_hash.py`
11. New file: `src/tests/test_xg_model_v2_loader_verifies_hash.py`
12. New file: `src/tests/test_spadl_vaep_loader_verifies_hash.py`
13. New file: `src/tests/test_defcon_lite_loader_verifies_hash.py`
14. New file: `src/tests/test_bootstrap_artifact_hashes.py`
15. New file: `src/tests/test_citation_consistency.py`
16. New file: `src/tests/test_workflow_card_references.py`
17. New file: `src/tests/test_architecture_md_appendix.py`
18. Modified file: `pyproject.toml` (entry point + dbt extra + force-include)
19. Modified file: `src/shared/wheel.py` (version bump 0.3.1 → 0.3.2)
20. Modified file: `dbt_project/profiles.yml` (new `serverless` target)
21. Modified file: `src/ingestion/utils.py` (section 9: hash verification)
22. Modified file: `src/ingestion/xg_model.py` (wire verification)
23. Modified file: `src/ingestion/xg_model_v2.py` (wire verification)
24. Modified file: `src/ingestion/spadl_vaep.py` (wire verification)
25. Modified file: `src/ingestion/defcon_lite_common.py` (wire verification)
26. Modified file: `terraform/modules/workflows/main.tf` (new `dbt_build` task + env, refresh_synced_tables depends_on simplification)
27. Modified file: `terraform/modules/catalog/main.tf:118-125` (gold schema grant expansion)
28. Modified files for D56 UI citations: `hf_taipy_app/src/pages/pitch_control.py:18` (Spearman), `movement_analysis.py:17` (Spearman), `shot_map.py:17` (Rathke — see Issue 2 decision options), `match_summary.py:16` (Rathke — same), `tactical_positions.py:29-30` (Sotudeh ETH Zurich), `player_similarity.py:21` (Danesi title standardization; URL action depends on verification flag 4a)
28a. Modified file: `src/analytics/pitch_control.py:1, 7` — source-code docstring Spearman title fix (Issue 1c)
29. Modified file: `NOTICE:54-58` — Spearman 2017 title fix
30. Modified file: `ARCHITECTURE.md` — new Appendix D academic references list
31. Modified files: `workflow-cards/wf-defcon.yaml`, `wf-line-breaking.yaml`, `wf-entity-resolution.yaml`, `wf-model-validation.yaml`, `wf-import-psxg.yaml`, `wf-prepare-360-data.yaml`, `wf-export-shots.yaml`, `wf-sync-hf-costs.yaml` (D56)
32. Modified file: `hf_taipy_app/src/queries/workflows.py` (column additions)
33. Modified file: `hf_taipy_app/src/state/workflows_stats.py` (table column rename + cell logic rewrite)
34. Modified file: `src/tests/test_workflows_cost_wiring.py` (extended assertions)
35. Modified file: every consumer touched by `bump_wheel.py` after the version bump (PEP 723 scripts, Terraform, deploy.sh)

Total: 17 new files + 18 modified files (approximate — D56 file count depends on Sotudeh/Danesi grep results during impl).

## Appendix C — Decisions deferred to implementation phase

Six decisions were initially deferred. Three were resolved during scoping (citations 1, 5, 6 below). Two remain — one needs the user's explicit input before impl starts, one is a runtime library compatibility check.

### Resolved during scoping (no action required)

1. ✅ **Spearman citation fix direction**: CONFIRMED to "Physics-Based Modeling of Pass Probabilities in Soccer" everywhere. Verified `src/analytics/pitch_control.py:5-7` describes the time-to-intercept framework from the 2017 paper (not the 2018 EPV paper). Five fix sites identified.
2. ✅ **Rathke citation fix direction**: APPROVED Option A (2026-04-13) — replace with Robberechts & Davis (2020), the project-canonical xG citation from `wf-xg-v1.yaml:16`. Two fix sites (`match_summary.py:16`, `shot_map.py:17`). Citation text locked; canonical URL is a 1-line lookup during impl.
4. ✅ **Danesi citation reconciliation**: CONFIRMED canonical title from implementation source `src/analytics/football2vec_transformer.py:20`: `"Danesi, P. (2025). Football2Vec: Transformer-Based Player Embeddings."` All three sources (UI, workflow card, source code) standardize on this title.
5. ✅ **wf-entity-resolution + wf-model-validation citations**: both leave `references: []` with operational-plumbing YAML comment blocks. Resolved.
6. ✅ **Sotudeh institution**: CONFIRMED to ETH Zurich. Verified `src/analytics/shape_graph_construction.py:4-6` matches `wf-shape-graphs.yaml:18` exactly. UI fix at `hf_taipy_app/src/pages/tactical_positions.py:29-30`.

### Verification flags (low-risk, resolved during impl)

3. **dbt-databricks OAuth M2M version (verification flag, not a decision)**: verify dbt-databricks 1.8+ resolves runtime SP identity on serverless before locking the version pin. If 1.8 doesn't, fall back to a `dbutils.secrets` injection pattern. To verify during early impl phase by reading the dbt-databricks adapter source for the pinned version.
4a. **Danesi UI URL only (verification flag)**: `https://doi.org/10.1007/978-3-031-02044-5_2` is a 2022 Springer chapter DOI. Verify during impl whether it actually resolves to a Danesi paper. If not, replace or drop.
2a. **Robberechts & Davis (2020) canonical URL (verification flag)**: locate the canonical URL (arXiv ID, DOI, or conference proceedings link) for the citation during impl. Likely sourced from `wf-xg-v1.yaml` or a quick external lookup.
