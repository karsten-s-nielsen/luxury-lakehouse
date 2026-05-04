# ADR-012: Training-to-Production Delivery Hardening

| Field | Value |
|---|---|
| **Date** | 2026-04-22 |
| **Status** | Accepted |
| **Deciders** | Karsten S. Nielsen (human), Claude Opus 4.7 (AI) |

## Context

[ADR-002](ADR-002-silent-exception-swallow-elimination.md) eliminated silent exception swallows on the **consumer** side. It converted the xG v2 inference pipeline's `except Exception: logger.warning(...); return 0` to `raise RuntimeError(...)`, which caused the daily `compute_xg_model_v2` task to fail loudly starting 2026-04-15.

The loud failure surfaced a matching silent-success bug on the **producer** side. Training runs were reporting success without actually producing consumer-reachable artifacts:

1. **Silent MLflow skip.** `scripts/train_xg_v2_hf.py` wrapped the entire MLflow registration block in `if tracking_uri:`. When `MLFLOW_TRACKING_URI` was unset on the HF Jobs invocation, the whole block disappeared. The training exited 0, HF Hub got weights, but MLflow `@Champion` was never set.
2. **No UC Volume write.** The script only published to HF Hub + (conditionally) MLflow. The Databricks inference consumer looks at MLflow + UC Volume — never HF Hub. When MLflow was skipped, UC Volume was also empty, and the consumer had nothing to load.
3. **Zombie `@Champion` alias risk.** `mlflow.pyfunc.log_model(..., registered_model_name=...)` registers a version as a side effect. The follow-up `set_registered_model_alias(...)` points the alias at it. If the alias-set silently no-ops (permission glitch, registry race), the run exits 0 with a registered version and no alias. Consumer still broken.
4. **Single-file upload constraint on HF Jobs.** `hf jobs uv run scripts/X.py` uploads ONLY `X.py`. A PR #75 (2026-04-02) architecture-audit split `train_xg_v2_hf.py` into a main script + sibling `train_xg_v2_hf_helpers.py`. The split looked locally-readable but silently broke HF Jobs: sibling imports fail with `ModuleNotFoundError`. Training on HF Jobs was intermittently run from a workstation instead, with the `training_env: "hf_jobs_l40s"` metadata field hardcoded — masking the fact that the HF Jobs path was broken.
5. **Implicit v1/v2 coupling via XGBoost feature list.** The v2 inference UDF reindexed v2's tabular input to XGBoost v1's `feature_names`. v2 weights were trained with 41 features; v1 UC Volume had drifted to a 34-feature stale copy (v1 training also never wrote UC Volume). Inference matmul blew up with `size 57 is different from 50`.

These five defects form the same class: training scripts that look correct in isolation but silently fail to deliver weights to the one location the production consumer will actually read.

## Decision

Codify a shared training-to-production delivery pattern in `src/ingestion/artifact_deploy.py` and apply it uniformly to every training script that targets the Databricks inference path.

### 1. Shared wheel module `src/ingestion/artifact_deploy.py`

Three helpers, each closing one layer of the silent-success chain:

- **`require_mlflow_env()`** — call at the top of `main()` before any work. Raises `RuntimeError` listing every missing env var from `("MLFLOW_TRACKING_URI", "DATABRICKS_HOST", "DATABRICKS_TOKEN")`. The `if tracking_uri:` gate pattern is forbidden going forward — MLflow registration is mandatory.
- **`upload_weights_to_uc_volume(client, *, catalog, schema, model_name, filename, weights_bytes) -> {"path", "sha256"}`** — uploads the artifact + a `.sha256` sidecar to `/Volumes/{catalog}/{schema}/model_weights/{model_name}/{filename}` via `databricks.sdk.WorkspaceClient.files.upload(..., overwrite=True)`. Matches the `bootstrap_artifact_hashes.py` pattern exactly so the SEC2 integrity-verification path works identically on fresh writes and on manually-bootstrapped files. Validates `catalog`/`schema`/`model_name` against `IDENTIFIER_RE` and rejects malformed filenames.
- **`set_and_verify_mlflow_champion(client, *, mlflow_fqn, run_id)`** — wraps the `search_model_versions` + `set_registered_model_alias` pair with a round-trip `get_model_version_by_alias` check that raises `RuntimeError` if the alias doesn't resolve to the freshly-registered version. Zombie state cannot silently ship.

Both `scripts/train_xg_v2_hf.py` (v2 set encoder) and `scripts/train_xg_model_hf.py` (v1 XGBoost) import from this module. Future training scripts that land in the Databricks inference path must do the same.

### 2. `feature_names` envelope convention for self-contained models

Model weight files whose serialization format does not natively embed feature names (e.g., the NumPy-dump envelope used by the Deep Sets set encoder) must inject a top-level `feature_names: list[str]` field into the JSON envelope. Inference reads the embedded list and reindexes tabular input to it. Legacy envelopes without the field fall back to the companion v1 XGBoost feature list for one release window, then the fallback is removed.

**Grace-period closure (2026-05-02, SK3-MIG cycle):** the v2→v1 XGBoost feature-list fallback was removed in `src/ingestion/xg_model_v2.py`. v2 envelopes lacking `feature_names` now raise `RuntimeError` at inference time via the new module-level helper `_parse_v2_envelope_features`. The trainer (`scripts/train_xg_v2_hf.py`) was hardened to inject `tabular_dim` alongside `feature_names` as defense-in-depth. See [ADR-022](ADR-022-direction-of-play-migration.md) for the broader cycle context.

This decouples downstream models from upstream model version drift. XGBoost serialization is exempt because the booster binary already carries feature_names natively via `get_booster().feature_names`.

### 3. HF Jobs single-file constraint — training scripts are self-contained

Any Python module that runs via `hf jobs uv run` MUST be a single-file PEP 723 script. Sibling imports do not work; the CLI uploads only the named script. Helpers that would naturally live next to the script must be either (a) inlined into the script, or (b) published as a wheel dependency. Option (b) is preferred for shared logic; option (a) is the only option for training-run-specific helpers.

When inlining, docstring-document the former location as a pointer:

```python
"""Self-contained PEP 723 script: helpers were inlined from
scripts/<old_name>.py to satisfy `hf jobs uv run`'s single-file
upload constraint. The project wheel provides cross-module
dependencies only.
"""
```

### 4. Three-destination delivery contract

Training scripts must publish to three destinations in this order, with all three mandatory on success:

1. **HF Hub** (open-licensed publish — `huggingface_hub.HfApi.upload_file`)
2. **MLflow UC Registry** (`mlflow.pyfunc.log_model` + `set_and_verify_mlflow_champion`)
3. **UC Volume** (`upload_weights_to_uc_volume`)

Ordering matters: HF Hub first because it's the canonical open artifact — if the Databricks side (2+3) fails mid-run, the HF Hub copy is still available for manual bootstrap via `bootstrap_artifact_hashes.py` + `upload_weights_to_uc_volume`.

### 5. Secrets vs env on `hf jobs uv run`

Training invocations MUST pass secret-valued env vars via `--secrets` (encrypted), not `--env` (plain job metadata, visible via `hf jobs inspect`). Correct form:

```bash
hf jobs uv run scripts/train_xg_v2_hf.py \
    --flavor a10g-large --timeout 60m \
    --secrets HF_TOKEN=$HF_TOKEN \
    --secrets DATABRICKS_TOKEN=$DATABRICKS_TOKEN \
    --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
    --env DATABRICKS_HOST=$DATABRICKS_HOST
```

`MLFLOW_TRACKING_URI` is always the literal string `"databricks"` in this project; `DATABRICKS_HOST` is the workspace URL. Neither is a secret. Tokens ARE secrets.

### 6. Runtime dependency-version assertion (uv silent-downgrade defense)

**Discovered 2026-05-04 (SK3-MIG-B Phase 9 cycle 1):** uv does NOT fail-fast on conflicting top-level vs wheel-transitive dep pins in PEP 723 deps. The top-level pin wins silently. The cancelled cycle ran on poisoned `silly-kicks==1.0.2` for 4323 games (the wheel pinned `>=3.0.1` but `train_vaep_model_hf.py`'s PEP 723 `dependencies` block declared `silly-kicks>=1.0.0,<2.0`, and uv resolved to 1.0.2 without raising `ResolutionImpossible`). Had the cycle not OOM-cancelled, every Champion artifact would have been silently wrong-coordinate.

**Decision:** every PEP 723 trainer whose correctness depends on a project-owned library version MUST:

1. Declare a module-level required-minimum constant: `_REQUIRED_SK_MIN: tuple[int, int, int] = (3, 0, 1)`.
2. Define a module-level helper `_assert_silly_kicks_min()` that imports the dep at runtime and raises `RuntimeError` if `__version__` is below the constant.
3. Call the helper as the FIRST statement of `main()` (before any other work — fail fast).

Explicit upper-bound pins in PEP 723 `dependencies` (e.g., `silly-kicks>=1.0.0,<2.0`) are FORBIDDEN — they are the active footgun. The wheel's transitive pin is the single source of truth. Runtime assertion is the defense.

**CI sentinels (`src/tests/test_sk3_mig_b_orchestrator_invariants.py`):**

- `test_no_trainer_pins_silly_kicks_explicitly` — parses each trainer's PEP 723 metadata block; fails if any trainer declares a `silly-kicks` line.
- `test_all_trainers_assert_silly_kicks_runtime_min` — `importlib` introspection asserts every trainer has module-level `_REQUIRED_SK_MIN = (3, 0, 1)`.

The pattern is intentionally generalisable: when another project-owned dep needs the same discipline (e.g., a hypothetical `lakehouse-utils` whose version determines schema correctness), add a peer constant + helper + sentinel pair. The CLAUDE.md "Project Conventions" entry "uv silent-downgrade footgun in PEP 723 deps" documents the rationale at the front-door level for future contributors.

**Applied to:** `train_vaep_model_hf.py`, `train_xg_v2_hf.py`, `train_football2vec.py`, `train_football2vec_v2.py`, `train_football2vec_360.py`, `train_scoutgpt_hf.py` (all 6 SK3-MIG-B retrain trainers). Future Databricks-inference-targeting trainers must follow the same pattern.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. One-shot bootstrap only (copy HF Hub weights to UC Volume manually once) | Fastest unblock (~5 min of code) | Doesn't close the root cause; next training run silently repeats the same bug | Hyrum's Law: every silent-success defect we've documented in ADR-002 started as "let's just patch it once" |
| B. Per-script duplication of `require_mlflow_env` / `upload_weights_to_uc_volume` | No new wheel module | 60 LOC duplicated in every training script; divergence risk when the pattern evolves | Violates DRY in a load-bearing delivery path; divergence between v1 and v2 implementations was literally the bug |
| C. `feature_names` stored ONLY in metrics.json (not in the weights envelope) | Cleaner separation of concerns | Inference UDF would need to download metrics.json in addition to weights; doubles the cold-start cost | Inference is hot-path; one artifact fetch per executor is the budget |
| D. Migrate to MLflow model flavor that carries `feature_names` natively (e.g., `mlflow.sklearn` wrapping the set encoder) | Zero envelope hacking | The set encoder is pure NumPy, not a sklearn model; wrapping is artifactual; MLflow flavors that support custom models go through CloudPickle which violates the project's no-pickle security policy | No-pickle is non-negotiable (`src/analytics/set_encoder.py` comment) |
| E. **Extract shared helper (`artifact_deploy.py`) + apply uniformly — CHOSEN** | One pattern, two callers, loud failures, DRY | One extra wheel module | — |

## Consequences

### Positive

- **Silent-success defects in training cannot recur** — any missing env var raises at entry; any unregistered MLflow version raises; any unverified alias raises; any UC Volume write failure raises.
- **v1 and v2 share one delivery contract** — future training scripts (xG v3, VAEP retrains, etc.) get the same guarantees by importing three functions.
- **Model-version drift between v1 and v2 is eliminated** — `feature_names` in the envelope decouples v2 from whatever v1 happens to be on disk.
- **HF Jobs invocation pattern is documented** — the `--secrets` vs `--env` hazard is explicit in every training script's docstring.
- **Bootstrap path still works** — `scripts/bootstrap_artifact_hashes.py` uses the same UC Volume path convention, so manual one-shot recovery is unchanged.

### Negative

- **Training scripts are no longer runnable without full Databricks auth** — `require_mlflow_env()` makes the registration step mandatory. A developer wanting to "just test training locally without touching MLflow" cannot. Mitigation: run unit tests instead of full training for local iteration; HF Jobs is the production training runtime.
- **One more wheel module to maintain** — `src/ingestion/artifact_deploy.py` is ~180 LOC. Weight: modest. Coverage: 15 tests in `src/tests/test_artifact_deploy.py`.
- **Helpers are inlined in training scripts** — reverses the PR #75 "architecture audit" split. Rationale is captured in the script docstrings; test coverage is preserved via the wheel module (not the script-local copies).

### Neutral

- **`@Champion` alias is now set for v1 for the first time.** The v1 inference pipeline will start consuming MLflow `@Champion` on future runs (it currently falls back to UC Volume). Predictions are unchanged because the model bytes are identical — only the load path changes.
- **Wheel version 0.3.11 → 0.3.12** to ship the new `ingestion.artifact_deploy` module and the updated `ingestion.xg_model_v2` inference logic.

## Implementation references

- `src/ingestion/artifact_deploy.py` — shared helpers.
- `src/tests/test_artifact_deploy.py` — 15 unit tests.
- `scripts/train_xg_v2_hf.py` — v2 caller, incl. `feature_names` envelope injection.
- `scripts/train_xg_model_hf.py` — v1 caller; first time v1 gets `@Champion` alias + UC Volume write.
- `src/ingestion/xg_model_v2.py:219-242` — inference UDF reads `feature_names` from envelope, falls back to XGBoost v1 features for legacy envelopes.
- `src/tests/test_xg_model_v2.py::TestV2EnvelopeFeatureNames` — envelope primary path + legacy fallback regression.
- `src/tests/test_xg_model_v2.py::TestMlflowLookupsUseGoldSchema` — `DEFAULT_GOLD_SCHEMA` regression (consumer-side).
- `scripts/bootstrap_artifact_hashes.py` — manual bootstrap, pattern reference.

## Verification artifacts (2026-04-22 unblock cycle)

- Daily task run `260074787055820`: `compute_xg_model_v2` TERMINATED/SUCCESS after multi-layer fix.
- `bronze.xg_predictions_v2`: 131,077 rows across 21 competitions written 2026-04-22 16:06 UTC.
- MLflow `soccer_analytics.dev_gold.xg_model_v2@Champion` → v3 (feature_names envelope).
- UC Volume `model_weights.json` 73,581 bytes with `.sha256` sidecar; bytes match HF Hub.
