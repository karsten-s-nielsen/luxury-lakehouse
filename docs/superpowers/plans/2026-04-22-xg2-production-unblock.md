# XG2 Production Unblock — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unblock the daily `compute_xg_model_v2` Databricks task (failing since 2026-04-15 with `RuntimeError: xG v2 weights not available`) by hardening `scripts/train_xg_v2_hf.py` to write to UC Volume + fail-loud on missing MLflow env vars, then re-running training on HF Jobs so fresh weights land in all three destinations (HF Hub, MLflow `@Champion`, UC Volume + sha256 sidecar).

**Architecture:** Mirror the existing SEC2 artifact-deploy pattern in `scripts/bootstrap_artifact_hashes.py` (uses `databricks.sdk.WorkspaceClient.files.upload()` with `io.BytesIO`). The UC Volume upload helper lives in `scripts/train_xg_v2_hf_helpers.py` — localized to this training script until another script needs the same pattern. The training script's silent `if tracking_uri:` skip becomes an explicit fail at the start of `main()`, so "training succeeded with no registry" can't recur.

**Tech Stack:** Python 3.10 (PEP 723 UV script), `huggingface_hub`, `mlflow>=2.17`, `databricks-sdk>=0.102`, `pytest`. Target runtime: HF Jobs L40S.

**Not in scope:**
- dbt mart layer for `fct_xg_predictions` v2 column — that's PR 3 of the Kimball migration (other session).
- Schema changes to `xg_model_v2.py` UDF output — also PR 3 scope.
- Retraining on cross-provider data (SB + IDSSE) — that's HF2 TODO.

**Branch:** `fix/xg2-production-unblock` (already created).

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `scripts/train_xg_v2_hf_helpers.py` | Modify | Add `upload_weights_to_uc_volume()` helper |
| `scripts/train_xg_v2_hf.py` | Modify | Fail-loud on missing env vars; call new helper after HF publish; update PEP 723 deps |
| `src/tests/test_train_xg_v2_hf_helpers.py` | Create | Unit tests for new helper (mock `WorkspaceClient`) |
| `src/tests/test_train_xg_v2_hf.py` | Create | Unit test for `_require_mlflow_env()` pre-flight check |
| `workflow-cards/wf-xg-v2.yaml` | Modify | Add `uc-volume` destination to `outputs.models` |
| `docs/huggingface/model-cards/xg-v2-model-card.md` | Modify | Add UC Volume path to "Model Files" section |
| `AI_GOVERNANCE.md` | Modify if `test_ai_governance_md.py` flags stale "Next review" date | Governance freshness |
| `TODO.md` | Modify | Remove/retire XG2 entry on completion; note in end-of-cycle update |

---

## Task 1: Add `upload_weights_to_uc_volume` helper (TDD)

**Files:**
- Create: `src/tests/test_train_xg_v2_hf_helpers.py`
- Modify: `scripts/train_xg_v2_hf_helpers.py` (append new function + imports)

- [ ] **Step 1.1: Write failing test for upload + sidecar**

Create `src/tests/test_train_xg_v2_hf_helpers.py`:

```python
"""Unit tests for scripts/train_xg_v2_hf_helpers.py."""

from __future__ import annotations

import hashlib
import io
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# scripts/ is not on sys.path by default; prepend it for import
_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


class TestUploadWeightsToUcVolume:
    """Test upload_weights_to_uc_volume — publishes weights + sha256 sidecar."""

    def test_uploads_weights_and_sidecar_with_correct_paths(self) -> None:
        """Helper must upload both model_weights.json and its .sha256 sidecar."""
        from train_xg_v2_hf_helpers import upload_weights_to_uc_volume

        weights = b'{"model_type": "set_encoder_xg_v2", "weights": {}}'
        mock_client = MagicMock()

        result = upload_weights_to_uc_volume(
            mock_client,
            catalog="soccer_analytics",
            schema="dev_gold",
            model_name="xg_model_v2",
            weights_bytes=weights,
        )

        # Two uploads: weights file + sidecar
        assert mock_client.files.upload.call_count == 2
        call_args = [c.args for c in mock_client.files.upload.call_args_list]
        call_kwargs = [c.kwargs for c in mock_client.files.upload.call_args_list]

        uploaded_paths = [args[0] for args in call_args]
        assert "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/model_weights.json" in uploaded_paths
        assert "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/model_weights.json.sha256" in uploaded_paths

        # Every upload must use overwrite=True (this is a republish path)
        for kwargs in call_kwargs:
            assert kwargs.get("overwrite") is True, f"upload() kwargs missing overwrite=True: {kwargs}"

        # Returned dict carries the canonical path + hex digest
        assert result["path"] == "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/model_weights.json"
        assert result["sha256"] == hashlib.sha256(weights).hexdigest()

    def test_sidecar_contents_match_sha256(self) -> None:
        """The .sha256 sidecar content must be the hex digest of the weights bytes."""
        from train_xg_v2_hf_helpers import upload_weights_to_uc_volume

        weights = b"abcdef"
        expected_hex = hashlib.sha256(weights).hexdigest()
        mock_client = MagicMock()

        upload_weights_to_uc_volume(
            mock_client,
            catalog="soccer_analytics",
            schema="dev_gold",
            model_name="xg_model_v2",
            weights_bytes=weights,
        )

        sidecar_call = next(
            c for c in mock_client.files.upload.call_args_list
            if c.args[0].endswith(".sha256")
        )
        body = sidecar_call.args[1]
        # Helper passes a BytesIO-like object; read its contents
        assert hasattr(body, "read")
        sidecar_bytes = body.read()
        assert sidecar_bytes.decode("utf-8").strip() == expected_hex

    def test_rejects_sql_unsafe_identifiers(self) -> None:
        """Catalog / schema / model_name must match IDENTIFIER_RE; reject otherwise."""
        from train_xg_v2_hf_helpers import upload_weights_to_uc_volume

        mock_client = MagicMock()
        weights = b"x"

        with pytest.raises(ValueError, match="Invalid catalog"):
            upload_weights_to_uc_volume(
                mock_client, catalog="bad;name", schema="dev_gold",
                model_name="xg_model_v2", weights_bytes=weights,
            )
        with pytest.raises(ValueError, match="Invalid schema"):
            upload_weights_to_uc_volume(
                mock_client, catalog="soccer_analytics", schema="dev gold",
                model_name="xg_model_v2", weights_bytes=weights,
            )
        with pytest.raises(ValueError, match="Invalid model_name"):
            upload_weights_to_uc_volume(
                mock_client, catalog="soccer_analytics", schema="dev_gold",
                model_name="xg/model/v2", weights_bytes=weights,
            )

    def test_empty_weights_rejected(self) -> None:
        """Empty weights_bytes is a bug — helper must raise before uploading."""
        from train_xg_v2_hf_helpers import upload_weights_to_uc_volume

        mock_client = MagicMock()
        with pytest.raises(ValueError, match="empty"):
            upload_weights_to_uc_volume(
                mock_client, catalog="soccer_analytics", schema="dev_gold",
                model_name="xg_model_v2", weights_bytes=b"",
            )
        mock_client.files.upload.assert_not_called()
```

- [ ] **Step 1.2: Run test — verify it fails with ImportError**

Run: `uv run pytest src/tests/test_train_xg_v2_hf_helpers.py -v`
Expected: FAIL with `ImportError: cannot import name 'upload_weights_to_uc_volume'`

- [ ] **Step 1.3: Implement helper in `scripts/train_xg_v2_hf_helpers.py`**

Append the following to `scripts/train_xg_v2_hf_helpers.py` (at end of file, after `evaluate_v1_baseline`):

```python
# ---------------------------------------------------------------------------
# UC Volume publish (delivery path for Databricks serverless consumer)
# ---------------------------------------------------------------------------


def upload_weights_to_uc_volume(
    workspace_client: Any,
    *,
    catalog: str,
    schema: str,
    model_name: str,
    weights_bytes: bytes,
) -> dict[str, str]:
    """Upload serialized model weights to UC Volume with SHA-256 sidecar.

    Writes two files to ``/Volumes/{catalog}/{schema}/model_weights/{model_name}/``:
      - ``model_weights.json`` — the weights bytes
      - ``model_weights.json.sha256`` — hex SHA-256 of the weights bytes

    Both writes use ``overwrite=True`` because this is a republish path (same
    as ``bootstrap_artifact_hashes.py``). The SHA-256 sidecar is consumed by
    ``ingestion.utils._load_volume_sidecar_hash`` during inference
    (SEC2 artifact integrity verification).

    Args:
        workspace_client: An authenticated ``databricks.sdk.WorkspaceClient``
            (or any object with a compatible ``.files.upload(path, body, overwrite=True)``).
        catalog: Unity Catalog name (validated against ``IDENTIFIER_RE``).
        schema: UC schema name (validated against ``IDENTIFIER_RE``).
        model_name: Model subdirectory name (validated against ``IDENTIFIER_RE``).
        weights_bytes: Serialized weights (output of
            ``analytics.set_encoder.serialize_set_encoder_weights``).

    Returns:
        Dict with keys ``path`` (canonical volume path of the weights file)
        and ``sha256`` (hex digest of the weights bytes).

    Raises:
        ValueError: If any identifier fails SQL-safety validation, or if
            ``weights_bytes`` is empty.
    """
    import hashlib
    import io
    import re

    _id_re = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    if not _id_re.match(catalog):
        raise ValueError(f"Invalid catalog name: {catalog!r}")
    if not _id_re.match(schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not _id_re.match(model_name):
        raise ValueError(f"Invalid model_name: {model_name!r}")
    if not weights_bytes:
        raise ValueError("weights_bytes is empty; refusing to upload")

    weights_path = f"/Volumes/{catalog}/{schema}/model_weights/{model_name}/model_weights.json"
    sidecar_path = weights_path + ".sha256"
    sha256 = hashlib.sha256(weights_bytes).hexdigest()

    workspace_client.files.upload(weights_path, io.BytesIO(weights_bytes), overwrite=True)
    workspace_client.files.upload(sidecar_path, io.BytesIO(sha256.encode("utf-8")), overwrite=True)

    logger.info(
        "Uploaded xG v2 weights to UC Volume: %s (%d bytes, sha256=%s)",
        weights_path,
        len(weights_bytes),
        sha256[:8],
    )

    return {"path": weights_path, "sha256": sha256}
```

- [ ] **Step 1.4: Run test — verify it passes**

Run: `uv run pytest src/tests/test_train_xg_v2_hf_helpers.py -v`
Expected: 4 passed.

- [ ] **Step 1.5: Run ruff + pyright on touched files**

Run: `uv run ruff check scripts/train_xg_v2_hf_helpers.py src/tests/test_train_xg_v2_hf_helpers.py && uv run pyright scripts/train_xg_v2_hf_helpers.py`
Expected: no errors.

---

## Task 2: Fail loud on missing MLflow env vars (TDD)

**Files:**
- Create: `src/tests/test_train_xg_v2_hf.py`
- Modify: `scripts/train_xg_v2_hf.py`

- [ ] **Step 2.1: Write failing test for `_require_mlflow_env()`**

Create `src/tests/test_train_xg_v2_hf.py`:

```python
"""Unit tests for scripts/train_xg_v2_hf.py pre-flight checks."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


class TestRequireMlflowEnv:
    """Test _require_mlflow_env — fails loud when MLflow/Databricks env vars missing."""

    def test_raises_when_mlflow_tracking_uri_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from train_xg_v2_hf import _require_mlflow_env

        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-xxx")

        with pytest.raises(RuntimeError, match="MLFLOW_TRACKING_URI"):
            _require_mlflow_env()

    def test_raises_when_databricks_host_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from train_xg_v2_hf import _require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-xxx")

        with pytest.raises(RuntimeError, match="DATABRICKS_HOST"):
            _require_mlflow_env()

    def test_raises_when_databricks_token_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from train_xg_v2_hf import _require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

        with pytest.raises(RuntimeError, match="DATABRICKS_TOKEN"):
            _require_mlflow_env()

    def test_passes_when_all_env_vars_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from train_xg_v2_hf import _require_mlflow_env

        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-xxx")

        # Should not raise
        _require_mlflow_env()
```

- [ ] **Step 2.2: Run test — verify it fails with ImportError**

Run: `uv run pytest src/tests/test_train_xg_v2_hf.py -v`
Expected: FAIL with `ImportError: cannot import name '_require_mlflow_env'`.

- [ ] **Step 2.3: Add `_require_mlflow_env()` to `scripts/train_xg_v2_hf.py`**

Insert this function above `@workflow("wf-xg-v2", phase="training")` (before `def main()`):

```python
_REQUIRED_ENV_VARS = ("MLFLOW_TRACKING_URI", "DATABRICKS_HOST", "DATABRICKS_TOKEN")


def _require_mlflow_env() -> None:
    """Fail loud if MLflow/Databricks registration env vars are missing.

    Prevents the silent-skip of the ``if tracking_uri:`` block that previously
    let training "succeed" without ever registering the model to MLflow
    ``@Champion`` or producing a consumer-reachable artifact. Aligned with
    ADR-002 silent-swallow elimination.

    Raises:
        RuntimeError: listing every missing env var, with remediation hint.
    """
    missing = [name for name in _REQUIRED_ENV_VARS if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            f"Missing required env vars for MLflow UC registration: {missing}. "
            "Pass all three on the `hf jobs uv run` invocation: "
            "--env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI "
            "--env DATABRICKS_HOST=$DATABRICKS_HOST "
            "--env DATABRICKS_TOKEN=$DATABRICKS_TOKEN. "
            "Silent MLflow skip is not allowed (ADR-002)."
        )
```

- [ ] **Step 2.4: Run test — verify it passes**

Run: `uv run pytest src/tests/test_train_xg_v2_hf.py -v`
Expected: 4 passed.

---

## Task 3: Wire pre-flight + UC Volume upload into `main()`

**Files:**
- Modify: `scripts/train_xg_v2_hf.py` (PEP 723 header + `main()` body)

- [ ] **Step 3.1: Add `databricks-sdk` to PEP 723 dependencies**

In `scripts/train_xg_v2_hf.py` replace the dependency block at lines 3-13 with:

```python
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#     "luxury-lakehouse @ https://huggingface.co/luxury-lakehouse/build-artifacts/resolve/main/luxury_lakehouse-0.3.11-py3-none-any.whl",
#     "numpy>=1.24",
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "torch>=2.0",
#     "scikit-learn>=1.3.0",
#     "xgboost>=2.0",
#     "huggingface-hub>=1.5.0",
#     "mlflow>=2.17.0",
#     "databricks-sdk>=0.102.0",
# ]
# ///
```

(Note: `databricks-sdk` already ships transitively via the luxury-lakehouse wheel, but pinning it explicitly in PEP 723 makes the dependency visible to future readers.)

- [ ] **Step 3.2: Call `_require_mlflow_env()` at top of `main()`**

In `scripts/train_xg_v2_hf.py`, insert immediately after the opening of `def main()` (right after the `from huggingface_hub import ...` line on what is currently line 96):

```python
    # Pre-flight: fail loud if MLflow registration env vars are missing
    # (ADR-002: no silent-skip of the registry step).
    _require_mlflow_env()
```

- [ ] **Step 3.3: Remove the `if tracking_uri:` gate — registration is now mandatory**

In `scripts/train_xg_v2_hf.py`, replace the block currently reading:

```python
    # 9. MLflow
    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if tracking_uri:
        import mlflow
        ...
```

with:

```python
    # 9. MLflow (always runs — _require_mlflow_env() enforced on entry)
    mlflow_fqn = mlflow_model_uri(CATALOG, SCHEMA, MODEL_NAME)
    tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("/soccer_analytics/xg_model_v2")
```

All the body of the original `if tracking_uri:` block (the `with mlflow.start_run(...)` plus everything through the `logger.info("MLflow complete (version=%s, run=%s)", ...)` call) must be dedented one level — it's now unconditional.

- [ ] **Step 3.4: Add UC Volume upload after HF Hub publish**

In `scripts/train_xg_v2_hf.py`, after the existing `api.upload_file(... path_in_repo="metrics.json" ...)` call (currently around line 341-346) and before the final `logger.info("Published: https://huggingface.co/%s ...", ...)` line, insert:

```python
    # 11. Upload to UC Volume so the Databricks consumer (ingestion.xg_model_v2)
    # can read the weights via its Volume fallback. Writes both the weights file
    # and the .sha256 sidecar consumed by _load_volume_sidecar_hash().
    from databricks.sdk import WorkspaceClient
    from train_xg_v2_hf_helpers import upload_weights_to_uc_volume

    workspace_client = WorkspaceClient()
    volume_result = upload_weights_to_uc_volume(
        workspace_client,
        catalog=CATALOG,
        schema=SCHEMA,
        model_name=MODEL_NAME,
        weights_bytes=weight_bytes,
    )
    logger.info("UC Volume publish complete: %s", volume_result["path"])
```

- [ ] **Step 3.5: Update the module docstring**

In `scripts/train_xg_v2_hf.py`, replace the `Usage (HF Jobs CLI):` block in the docstring (currently lines 26-32) with:

```
Usage (HF Jobs CLI):
    hf jobs uv run scripts/train_xg_v2_hf.py \
        --flavor l40sx1 --timeout 60m \
        --secrets HF_TOKEN=$HF_TOKEN \
        --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
        --env DATABRICKS_HOST=$DATABRICKS_HOST \
        --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN

    All four env vars are REQUIRED. The script fails fast (ADR-002) if
    MLFLOW_TRACKING_URI, DATABRICKS_HOST, or DATABRICKS_TOKEN is missing,
    since silent MLflow skip previously left the production consumer
    without a @Champion alias to load weights from.

Artifacts produced (all three mandatory on success):
  - HF Hub model repo ``luxury-lakehouse/xg-v2-model-set-encoder`` (weights + metrics)
  - MLflow UC Registry ``soccer_analytics.dev_gold.xg_model_v2@Champion``
  - UC Volume ``/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/``
    (``model_weights.json`` + ``model_weights.json.sha256`` sidecar)
```

- [ ] **Step 3.6: Run full test suite for touched modules**

Run: `uv run pytest src/tests/test_train_xg_v2_hf.py src/tests/test_train_xg_v2_hf_helpers.py src/tests/test_xg_model_v2.py -v`
Expected: all pass.

- [ ] **Step 3.7: Run ruff + pyright on full touched surface**

Run: `uv run ruff check scripts/train_xg_v2_hf.py scripts/train_xg_v2_hf_helpers.py src/tests/test_train_xg_v2_hf.py src/tests/test_train_xg_v2_hf_helpers.py && uv run pyright scripts/train_xg_v2_hf.py scripts/train_xg_v2_hf_helpers.py`
Expected: no errors.

---

## Task 4: Update workflow card outputs list

**Files:**
- Modify: `workflow-cards/wf-xg-v2.yaml`

- [ ] **Step 4.1: Add UC Volume to `outputs.models`**

In `workflow-cards/wf-xg-v2.yaml`, replace the `outputs.models` block (currently lines 34-39) with:

```yaml
outputs:
  models:
    - id: "luxury-lakehouse/xg-v2-model-set-encoder"
      destination: huggingface
      format: json-base64
    - id: "xg_model_v2@Champion"
      destination: mlflow-registry
    - id: "/Volumes/{catalog}/dev_gold/model_weights/xg_model_v2/model_weights.json"
      destination: uc-volume
      format: json-base64
      notes: "SHA-256 sidecar .sha256 written by train_xg_v2_hf.py for SEC2 integrity verification"
  tables:
    - id: "{catalog}.bronze.xg_predictions_v2"
      destination: delta-table
```

- [ ] **Step 4.2: Run workflow card validation**

Run: `uv run validate_workflow_cards`
Expected: all cards valid (or `ok` on wf-xg-v2 specifically).

---

## Task 5: Update xG v2 model card Model Files section

**Files:**
- Modify: `docs/huggingface/model-cards/xg-v2-model-card.md`

- [ ] **Step 5.1: Extend "Model Files" section**

In `docs/huggingface/model-cards/xg-v2-model-card.md`, replace the current "Model Files" block (currently lines 258-263) with:

```
## Model Files

The model is published to three destinations, all in sync:

1. **HF Hub**: [`luxury-lakehouse/xg-v2-model-set-encoder`](https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder)
   - `model_weights.json` — set encoder weights (JSON + base64, ~100 KB)
   - `metrics.json` — training metrics + dataset commit SHAs
2. **MLflow UC Registry**: `soccer_analytics.dev_gold.xg_model_v2@Champion`
   - Logged via `mlflow.pyfunc.log_model`; the `@Champion` alias points at
     the latest version. The raw weights are also logged as an artifact
     (`model_weights.json`) so the consumer can download them byte-for-byte.
3. **Databricks UC Volume**: `/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/`
   - `model_weights.json` — identical bytes to the HF Hub copy
   - `model_weights.json.sha256` — hex SHA-256 sidecar for SEC2 integrity verification

The Databricks serverless inference pipeline `ingestion.xg_model_v2` tries
MLflow `@Champion` first, then falls back to the UC Volume copy; the sidecar
lets the consumer detect tampering without trusting the MLflow registry
metadata alone.
```

---

## Task 6: Run AI governance + full quality gates

**Files:**
- No edits yet — only if a test flags a gap.

- [ ] **Step 6.1: Run the AI governance test**

Run: `uv run pytest src/tests/test_ai_governance_md.py -v`
Expected: all pass.

- [ ] **Step 6.2: If step 6.1 fails on "Next review" date staleness, bump it**

If and only if the test reports a stale "Next review" date, open `AI_GOVERNANCE.md`, find the `Next review:` line, and update to today's date plus 30 days (`2026-05-22`). Re-run the test. Do NOT make any other change to the governance doc — this fix does not alter the model's scope, inputs, outputs, or risk classification.

- [ ] **Step 6.3: Run the full unit-test suite**

Run: `uv run pytest src/tests/ -x -q`
Expected: full green. Investigate any failure before proceeding.

- [ ] **Step 6.4: Run repo-wide lint + type checks**

Run: `uv run ruff check src/ scripts/ && uv run ruff format --check src/ scripts/ && uv run pyright src/`
Expected: no errors.

---

## Task 7: Re-train on HF Jobs with hardened script (Option C)

- [ ] **Step 7.1: Confirm HF + Databricks env vars are present locally**

Run: `echo "HF_TOKEN=${HF_TOKEN:+SET}${HF_TOKEN:-UNSET} MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI:+SET}${MLFLOW_TRACKING_URI:-UNSET} DATABRICKS_HOST=${DATABRICKS_HOST:+SET}${DATABRICKS_HOST:-UNSET} DATABRICKS_TOKEN=${DATABRICKS_TOKEN:+SET}${DATABRICKS_TOKEN:-UNSET}"`
Expected: all four `SET`. If any is UNSET, stop and export them before continuing. Do NOT echo the actual values into the session log.

- [ ] **Step 7.2: Dispatch training on HF Jobs**

Run (in foreground — we want the output stream):
```
hf jobs uv run scripts/train_xg_v2_hf.py --flavor l40sx1 --timeout 60m --secrets HF_TOKEN=$HF_TOKEN --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI --env DATABRICKS_HOST=$DATABRICKS_HOST --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
```

Expected: job completes SUCCESS within ~30 min. Log output must show:
- "Using device: cuda"
- "v2 calibrated: brier ≈ 0.06, roc_auc ≈ 0.90"
- "MLflow complete (version=N, run=...)"
- "UC Volume publish complete: /Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/model_weights.json"
- "Published: https://huggingface.co/luxury-lakehouse/xg-v2-model-set-encoder"

Use `run_in_background=True` for the bash call, tail the output file every 60 seconds, and abort if it runs past 45 minutes wall-clock.

- [ ] **Step 7.3: Verify all three artifact destinations are populated**

Run this Python block (fill in tracking URI from env):

```python
from huggingface_hub import HfApi
from databricks.sdk import WorkspaceClient
import mlflow, os
mlflow.set_registry_uri("databricks-uc")

# HF Hub
api = HfApi()
info = api.repo_info(repo_id="luxury-lakehouse/xg-v2-model-set-encoder", repo_type="model")
print(f"HF last_modified={info.last_modified}  sha={info.sha[:8]}")

# MLflow @Champion
from mlflow.tracking import MlflowClient
client = MlflowClient()
fqn = "soccer_analytics.dev_gold.xg_model_v2"
champ = client.get_model_version_by_alias(fqn, "Champion")
print(f"MLflow @Champion v{champ.version}  run={champ.run_id}")

# UC Volume + sidecar
w = WorkspaceClient()
vp = "/Volumes/soccer_analytics/dev_gold/model_weights/xg_model_v2/"
files = list(w.files.list_directory_contents(vp))
names = sorted(f.name for f in files)
assert "model_weights.json" in names, names
assert "model_weights.json.sha256" in names, names
print(f"UC Volume files: {names}")
```

Expected:
- HF `last_modified` is within the last hour (training just ran).
- MLflow returns a new version number; `@Champion` points to it.
- UC Volume lists both `model_weights.json` and its `.sha256` sidecar.

If any check fails, investigate before proceeding — a partial publish is a regression.

---

## Task 8: Manually trigger daily task `compute_xg_model_v2`

- [ ] **Step 8.1: Trigger a scoped one-task re-run**

Run:
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import SubmitTask, PythonWheelTask
# Prefer "run_now with only_task_keys" to exercise the exact TF-managed task config
w = WorkspaceClient()
JOB_ID = 302697362345215
run = w.jobs.run_now(job_id=JOB_ID, only=["compute_xg_model_v2"])
print(f"Started run {run.run_id}; track with w.jobs.wait_get_run(run.run_id)")
```

If `only=[...]` is not available in the installed SDK version, fall back to the UI flow: open the Jobs UI for job `302697362345215` → "Run now with different parameters" → select just `compute_xg_model_v2`. Document the run_id returned.

- [ ] **Step 8.2: Wait for the task to finish + confirm SUCCESS**

Run:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
run_id = <from step 8.1>
w.jobs.wait_get_run(run_id=run_id, timeout=datetime.timedelta(minutes=60))
final = w.jobs.get_run(run_id=run_id)
for t in final.tasks or []:
    if t.task_key == "compute_xg_model_v2":
        print(f"state: {t.state.life_cycle_state}/{t.state.result_state}")
        print(f"msg: {t.state.state_message}")
```

Expected: `TERMINATED/SUCCESS` for the `compute_xg_model_v2` task. If FAILED, pull the run-page stderr and debug before moving on — do NOT declare success on the first green signal.

- [ ] **Step 8.3: Verify `bronze.xg_predictions_v2` is populated**

Run via the SQL warehouse (use `scripts/ensure_warehouse.py` first to guarantee the warehouse is running):

```
uv run python scripts/ensure_warehouse.py -- uv run python -c "
from databricks import sql
import os
with sql.connect(server_hostname=os.environ['DATABRICKS_SERVER_HOSTNAME'],
                 http_path=os.environ['DATABRICKS_HTTP_PATH'],
                 access_token=os.environ['DATABRICKS_TOKEN']) as c, c.cursor() as cur:
    cur.execute('SELECT COUNT(*) AS n, COUNT(DISTINCT competition_id) AS comps, MIN(_ingested_at) AS first_ts, MAX(_ingested_at) AS last_ts FROM soccer_analytics.bronze.xg_predictions_v2')
    print(cur.fetchone())
"
```

Expected: `n > 0`, `comps > 0`, `last_ts` within the last hour.

---

## Task 9: Final verification + governance re-run

- [ ] **Step 9.1: Re-run the full unit-test suite**

Run: `uv run pytest src/tests/ -x -q`
Expected: all pass (unchanged from step 6.3, but sanity check after the two new tests).

- [ ] **Step 9.2: Re-run AI governance test**

Run: `uv run pytest src/tests/test_ai_governance_md.py -v`
Expected: all pass.

- [ ] **Step 9.3: Run workflow-card parity test**

Run: `uv run pytest src/tests/test_card_cost_phase_parity.py src/tests/test_card_dbt_model_field.py src/tests/test_card_parity_with_terraform.py -v`
Expected: all pass.

- [ ] **Step 9.4: `git status` sanity check**

Run: `git status`
Expected: changes on `scripts/train_xg_v2_hf.py`, `scripts/train_xg_v2_hf_helpers.py`, `src/tests/test_train_xg_v2_hf.py`, `src/tests/test_train_xg_v2_hf_helpers.py`, `workflow-cards/wf-xg-v2.yaml`, `docs/huggingface/model-cards/xg-v2-model-card.md`, `docs/superpowers/plans/2026-04-22-xg2-production-unblock.md`, optionally `AI_GOVERNANCE.md`, `TODO.md`.

- [ ] **Step 9.5: `git diff --stat` sanity check**

Run: `git diff --stat main`
Expected: line-count deltas roughly match (new helper ~60 LOC, new tests ~150 LOC, training-script edits ~30 LOC, workflow card + model card ~15 LOC).

---

## Task 10: Update TODO.md + present findings to user

- [ ] **Step 10.1: Retire the XG2 TODO row**

In `TODO.md`, remove the XG2 row from the On Deck table, and add a one-line closure note to the top-level dated header:

```
**Last updated**: 2026-04-22 (three cycles shipping same day).
**(A) Kimball Migration through PR 2 shipped** — [existing text unchanged]
**(B) ScoutGPT cross_attention promotion cycle** — [existing text unchanged]
**(C) XG2 production unblock** — `compute_xg_model_v2` daily task back to SUCCESS after `train_xg_v2_hf.py` hardened to (a) fail loud on missing MLflow env vars, (b) publish to UC Volume + SHA-256 sidecar in addition to HF Hub + MLflow `@Champion`. Re-trained on HF Jobs L40S with all three env vars set; `bronze.xg_predictions_v2` now populates on the next run. Root cause: pre-PR #122 the consumer had `logger.warning(...); return 0` hiding a gap that had been silent since HF-Hub-only publishing began. ADR-002's hard-fail conversion surfaced it.
```

- [ ] **Step 10.2: Summarise the cycle in one paragraph for the user**

Present:
- The root cause chain (5 layers).
- What was changed in code (helper + loudness check + main() wiring + doc updates).
- What was verified (3 destinations populated, daily task SUCCESS, bronze table has rows).
- What's NOT in scope (PR 3 Kimball dbt wiring, cross-provider xG v3).
- The exact `git diff --stat` summary.

- [ ] **Step 10.3: Ask user for commit approval**

DO NOT commit. Ask: "All verification green — daily task succeeded, bronze.xg_predictions_v2 populated, all three destinations synced. Ready to commit on `fix/xg2-production-unblock` with message draft: `<message>`. Approve commit?"

---

## Self-Review Checklist

- [x] Each task has exact file paths with line ranges where applicable
- [x] Every step contains runnable code or exact commands — no placeholders
- [x] TDD order enforced: test first, then implementation, then verification
- [x] No method/type drift between tasks (upload helper signature is stable: `(workspace_client, *, catalog, schema, model_name, weights_bytes) -> dict[str, str]`)
- [x] Scope boundary explicit (not touching dbt marts, not changing UDF output schema, not retraining cross-provider)
- [x] Single commit at end — no intermediate commits
- [x] Verification steps run against production state (HF Hub, MLflow, UC Volume, Databricks Jobs)
- [x] Reversibility: UC Volume uploads use `overwrite=True`, re-running is idempotent. If retraining fails midway, the daily job is no worse off than today.
- [x] Chesterton's fence respected: matches the existing `bootstrap_artifact_hashes.py` pattern rather than inventing a new deploy mechanism.

---

## Execution Notes

- Do not skip Task 3.3 (removing the `if tracking_uri:` gate). The whole point of the hardening is that MLflow registration is now mandatory; leaving the conditional would defeat ADR-002 loudness.
- Task 7.2 is the longest step (~30 min HF Jobs training). Monitor via `hf jobs logs` or the HF UI while waiting.
- Task 8.1 requires `databricks-sdk >= 0.102.0` to have the `jobs.run_now(only=[...])` kwarg. If the installed SDK is older, use the UI flow or upgrade first via `uv pip install --upgrade "databricks-sdk>=0.102.0"`.
- If Task 7.3 or Task 8.3 reveals a partial publish (e.g., HF Hub updated but MLflow didn't), STOP and return to systematic-debugging Phase 1. Do not paper over with a manual fix.
