# HF Import Guard Promotion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace 5 placeholder HF Hub import guards with SHA-based freshness checks, and retrain football2vec v2 on combined StatsBomb + Wyscout data.

**Architecture:** Shared helper `check_hf_dataset_freshness()` in `guards.py` fetches HF Hub commit SHA, compares against stored SHA in a new `workflow_import_checksums` Delta table in the observability schema. Each guard becomes a thin wrapper. After successful import, pipelines write back the SHA via `record_import_sha()`.

**Tech Stack:** PySpark (Delta), huggingface_hub (`HfApi`), pytest (mocked Spark + HfApi), HF Jobs (L40S GPU for training rerun).

**Spec:** `docs/superpowers/specs/2026-04-16-hf-guard-promotion-design.md`

---

## File Map

| File | Responsibility | Action |
|------|---------------|--------|
| `src/ingestion/guards.py` | Guard infrastructure + shared helper | Modify: add `check_hf_dataset_freshness()`, `record_import_sha()`, DDL constant |
| `src/ingestion/player_embeddings_v2.py` | v2 + 360 embedding import | Modify: promote both guards, add SHA write-back |
| `src/ingestion/import_obso_results.py` | OBSO/PAUSA import | Modify: promote guard, add SHA write-back |
| `src/ingestion/import_psxg_predictions.py` | PSxG import | Modify: promote guard, add SHA write-back |
| `src/ingestion/import_space_creation.py` | Space creation import | Modify: promote guard, add SHA write-back |
| `src/tests/test_hf_guard_freshness.py` | TDD tests for SHA-based guard | Create |
| `src/tests/test_guard_conformance.py` | Conformance exemption list | Modify: remove promoted guards from `_METADATA_EXEMPT` |

---

### Task 1: Launch Football2Vec v2 Stage-1 Training (Background)

**Files:** None (operational — HF Jobs CLI)

This runs in parallel with Tasks 2–7. Launch first so GPU time overlaps with code work.

- [ ] **Step 1: Launch stage-1 training on HF Jobs**

```bash
hf jobs uv run scripts/train_football2vec_v2.py --stage 1 \
    --flavor l40sx1 --timeout 120m \
    --secrets HF_TOKEN=$HF_TOKEN \
    --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
    --env DATABRICKS_HOST=$DATABRICKS_HOST \
    --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
```

Expected: Job queued, returns a job ID. Stage-1 trains MLM on ~87K player-match sequences (~3,000 StatsBomb + ~1,900 Wyscout matches). Takes ~30–60 min on L40S.

- [ ] **Step 2: Monitor stage-1 progress**

```bash
hf jobs ps
hf jobs logs <job-id>
```

Expected: Job transitions QUEUED → RUNNING → COMPLETED. Final log shows `Saved stage1 checkpoint to luxury-lakehouse/football2vec-v2`.

---

### Task 2: Write Failing Tests for `check_hf_dataset_freshness`

**Files:**
- Create: `src/tests/test_hf_guard_freshness.py`

- [ ] **Step 1: Create test file with all 7 test cases**

```python
"""Tests for HF Hub SHA-based guard freshness checks."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ingestion.guards import FilterResult


class _FakeRepoInfo:
    """Minimal stand-in for huggingface_hub.hf_api.RepoInfo."""

    def __init__(self, sha: str) -> None:
        self.sha = sha


def _make_spark_mock(*, stored_sha: str | None = None) -> MagicMock:
    """Build a Spark mock whose sql() returns a stored SHA row (or empty)."""
    spark = MagicMock()

    if stored_sha is None:
        # Empty DataFrame — no stored SHA
        row_mock = MagicMock()
        row_mock.collect.return_value = []
        spark.sql.return_value = row_mock
    else:
        # DataFrame with one row containing the stored SHA
        row = MagicMock()
        row["last_imported_sha"] = stored_sha
        row_mock = MagicMock()
        row_mock.collect.return_value = [row]
        spark.sql.return_value = row_mock

    return spark


class TestCheckHfDatasetFreshness:
    """SHA-based guard freshness logic."""

    @patch("ingestion.guards.HfApi")
    def test_first_run_no_stored_sha(self, mock_hf_api_cls: MagicMock) -> None:
        """First run with no stored SHA returns count=1 with commit_sha in metadata."""
        mock_hf_api_cls.return_value.repo_info.return_value = _FakeRepoInfo("abc123")
        spark = _make_spark_mock(stored_sha=None)

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(
            spark, "soccer_analytics", "wf-test", "luxury-lakehouse/test-dataset"
        )

        assert result.count == 1
        assert result.metadata["commit_sha"] == "abc123"
        assert result.workflow_id == "wf-test"

    @patch("ingestion.guards.HfApi")
    def test_stored_sha_matches_current(self, mock_hf_api_cls: MagicMock) -> None:
        """When stored SHA matches current, guard returns count=0 (skip)."""
        mock_hf_api_cls.return_value.repo_info.return_value = _FakeRepoInfo("abc123")
        spark = _make_spark_mock(stored_sha="abc123")

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(
            spark, "soccer_analytics", "wf-test", "luxury-lakehouse/test-dataset"
        )

        assert result.count == 0
        assert result.workflow_id == "wf-test"

    @patch("ingestion.guards.HfApi")
    def test_stored_sha_differs_from_current(self, mock_hf_api_cls: MagicMock) -> None:
        """When stored SHA differs, guard returns count=1 with new SHA."""
        mock_hf_api_cls.return_value.repo_info.return_value = _FakeRepoInfo("def456")
        spark = _make_spark_mock(stored_sha="abc123")

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(
            spark, "soccer_analytics", "wf-test", "luxury-lakehouse/test-dataset"
        )

        assert result.count == 1
        assert result.metadata["commit_sha"] == "def456"

    @patch("ingestion.guards.HfApi")
    def test_hf_hub_unreachable_fails_open(self, mock_hf_api_cls: MagicMock) -> None:
        """When HF Hub is unreachable, guard fails open with count=1."""
        mock_hf_api_cls.return_value.repo_info.side_effect = Exception("Connection refused")
        spark = _make_spark_mock(stored_sha="abc123")

        from ingestion.guards import check_hf_dataset_freshness

        result = check_hf_dataset_freshness(
            spark, "soccer_analytics", "wf-test", "luxury-lakehouse/test-dataset"
        )

        assert result.count == 1
        assert result.workflow_id == "wf-test"


class TestRecordImportSha:
    """SHA write-back after successful import."""

    def test_write_back_issues_merge(self) -> None:
        """record_import_sha issues a MERGE INTO statement."""
        spark = MagicMock()

        from ingestion.guards import record_import_sha

        record_import_sha(
            spark,
            "soccer_analytics",
            "wf-test",
            "luxury-lakehouse/test-dataset",
            "abc123",
        )

        spark.sql.assert_called_once()
        sql = spark.sql.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "workflow_import_checksums" in sql
        assert "abc123" in sql

    def test_no_write_back_when_sha_is_none(self) -> None:
        """No MERGE when commit_sha is None (fail-open path)."""
        spark = MagicMock()

        from ingestion.guards import record_import_sha

        record_import_sha(
            spark,
            "soccer_analytics",
            "wf-test",
            "luxury-lakehouse/test-dataset",
            None,
        )

        spark.sql.assert_not_called()


class TestGuardRepoConstants:
    """Each promoted guard passes the correct HF repo to the shared helper."""

    @patch("ingestion.guards.check_hf_dataset_freshness")
    def test_obso_guard_repo(self, mock_check: MagicMock) -> None:
        mock_check.return_value = FilterResult(workflow_id="wf-import-obso", count=1)
        from ingestion.import_obso_results import skip_guard

        spark = MagicMock()
        skip_guard.check(spark, "cat", "schema")
        mock_check.assert_called_once_with(
            spark, "cat", "wf-import-obso", "luxury-lakehouse/obso-pausa-values"
        )

    @patch("ingestion.guards.check_hf_dataset_freshness")
    def test_psxg_guard_repo(self, mock_check: MagicMock) -> None:
        mock_check.return_value = FilterResult(workflow_id="wf-import-psxg", count=1)
        from ingestion.import_psxg_predictions import skip_guard

        spark = MagicMock()
        skip_guard.check(spark, "cat", "schema")
        mock_check.assert_called_once_with(
            spark, "cat", "wf-import-psxg", "luxury-lakehouse/psxg-predictions"
        )

    @patch("ingestion.guards.check_hf_dataset_freshness")
    def test_space_creation_guard_repo(self, mock_check: MagicMock) -> None:
        mock_check.return_value = FilterResult(workflow_id="wf-import-space-creation", count=1)
        from ingestion.import_space_creation import skip_guard

        spark = MagicMock()
        skip_guard.check(spark, "cat", "schema")
        mock_check.assert_called_once_with(
            spark, "cat", "wf-import-space-creation", "luxury-lakehouse/space-creation-values"
        )

    @patch("ingestion.guards.check_hf_dataset_freshness")
    def test_football2vec_v2_guard_repo(self, mock_check: MagicMock) -> None:
        mock_check.return_value = FilterResult(workflow_id="wf-football2vec-v2", count=1)
        from ingestion.player_embeddings_v2 import skip_guard

        spark = MagicMock()
        skip_guard.check(spark, "cat", "schema")
        mock_check.assert_called_once_with(
            spark, "cat", "wf-football2vec-v2", "luxury-lakehouse/football2vec-statsbomb-wyscout"
        )

    @patch("ingestion.guards.check_hf_dataset_freshness")
    def test_football2vec_360_guard_repo(self, mock_check: MagicMock) -> None:
        mock_check.return_value = FilterResult(workflow_id="wf-football2vec-360", count=1)
        from ingestion.player_embeddings_v2 import _football2vec_360_guard

        spark = MagicMock()
        _football2vec_360_guard.check(spark, "cat", "schema")
        mock_check.assert_called_once_with(
            spark, "cat", "wf-football2vec-360", "luxury-lakehouse/football2vec-360-embeddings"  # pragma: allowlist secret
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest src/tests/test_hf_guard_freshness.py -v
```

Expected: All tests FAIL — `check_hf_dataset_freshness` and `record_import_sha` do not exist yet. Guard repo constants still point to placeholder implementations.

---

### Task 3: Implement `check_hf_dataset_freshness` and `record_import_sha` in guards.py

**Files:**
- Modify: `src/ingestion/guards.py:1-15` (imports), append after line 142 (after `find_new_ids`)

- [ ] **Step 1: Add imports at top of guards.py**

Add `import logging` and `import os` to the existing import block (lines 1–18). The `HfApi` import is deferred to function body to avoid import-time dependency on `huggingface_hub` in environments that don't have it.

At `src/ingestion/guards.py`, add to the imports section (after line 15):

```python
import logging
import os
```

- [ ] **Step 2: Add DDL constant and helper functions**

Insert after `find_new_ids` (after line 141), before the `SkipGuard` Protocol (line 144):

```python
# ---------------------------------------------------------------------------
# HF Hub SHA-based freshness check
# ---------------------------------------------------------------------------

_IMPORT_CHECKSUMS_DDL = (
    "workflow_id STRING NOT NULL, "
    "source_repo STRING NOT NULL, "
    "repo_type STRING NOT NULL, "
    "last_imported_sha STRING NOT NULL, "
    "imported_at TIMESTAMP NOT NULL"
)

_IMPORT_CHECKSUMS_TABLE = "workflow_import_checksums"


def check_hf_dataset_freshness(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    hf_repo: str,
    repo_type: str = "dataset",
) -> FilterResult:
    """Check whether an HF Hub dataset has changed since last import.

    Fetches the current commit SHA from HF Hub and compares it against
    the stored SHA in ``{catalog}.observability.workflow_import_checksums``.

    Returns ``count=0`` (skip) if the SHA matches, ``count=1`` (run) if
    it differs or is missing.  Fails open on network errors — safe to
    re-import rather than silently skip.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        workflow_id: The ``wf-xxx`` identifier.
        hf_repo: HF Hub repo ID (e.g., ``luxury-lakehouse/obso-pausa-values``).
        repo_type: HF Hub repo type (default ``dataset``).

    Returns:
        FilterResult with ``count=0`` (skip) or ``count=1`` (run).
        On run, ``metadata["commit_sha"]`` contains the current SHA.
    """
    logger = logging.getLogger(__name__)

    # 1. Fetch current SHA from HF Hub
    try:
        from huggingface_hub import HfApi

        api = HfApi(token=os.environ.get("HF_TOKEN"))
        info = api.repo_info(repo_id=hf_repo, repo_type=repo_type)
        current_sha: str = info.sha or ""
    except Exception:
        logger.warning(
            "HF Hub unreachable for %s — failing open (will re-import)",
            hf_repo,
            exc_info=True,
        )
        return FilterResult(workflow_id=workflow_id, count=1)

    if not current_sha:
        logger.warning("HF Hub returned empty SHA for %s — failing open", hf_repo)
        return FilterResult(workflow_id=workflow_id, count=1)

    # 2. Read stored SHA from observability table
    table_fqn = f"{catalog}.observability.{_IMPORT_CHECKSUMS_TABLE}"
    ensure_table(spark, table_fqn, _IMPORT_CHECKSUMS_DDL)

    rows = spark.sql(
        f"SELECT last_imported_sha FROM {table_fqn} "  # noqa: S608
        f"WHERE workflow_id = '{workflow_id}'"
    ).collect()

    if rows and rows[0]["last_imported_sha"] == current_sha:
        logger.info(
            "Guard %s: SHA unchanged (%s) — skipping import",
            workflow_id,
            current_sha[:12],
        )
        return FilterResult(workflow_id=workflow_id, count=0)

    logger.info(
        "Guard %s: new SHA detected (%s) — triggering import",
        workflow_id,
        current_sha[:12],
    )
    return FilterResult(
        workflow_id=workflow_id,
        count=1,
        metadata={"commit_sha": current_sha},
    )


def record_import_sha(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    source_repo: str,
    commit_sha: str | None,
    repo_type: str = "dataset",
) -> None:
    """Write-back the imported SHA after a successful import.

    Uses MERGE (upsert on ``workflow_id``) to update the
    ``workflow_import_checksums`` table in the observability schema.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        workflow_id: The ``wf-xxx`` identifier.
        source_repo: HF Hub repo ID that was imported.
        commit_sha: The SHA to record. If None (fail-open path), no-op.
        repo_type: HF Hub repo type (default ``dataset``).
    """
    if commit_sha is None:
        return

    table_fqn = f"{catalog}.observability.{_IMPORT_CHECKSUMS_TABLE}"
    spark.sql(
        f"MERGE INTO {table_fqn} AS target "
        f"USING (SELECT "
        f"  '{workflow_id}' AS workflow_id, "
        f"  '{source_repo}' AS source_repo, "
        f"  '{repo_type}' AS repo_type, "
        f"  '{commit_sha}' AS last_imported_sha, "
        f"  current_timestamp() AS imported_at"
        f") AS source "
        f"ON target.workflow_id = source.workflow_id "
        f"WHEN MATCHED THEN UPDATE SET * "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
```

- [ ] **Step 3: Run the freshness and write-back tests**

```bash
uv run pytest src/tests/test_hf_guard_freshness.py::TestCheckHfDatasetFreshness -v
uv run pytest src/tests/test_hf_guard_freshness.py::TestRecordImportSha -v
```

Expected: All 6 tests in these two classes PASS. The `TestGuardRepoConstants` tests still FAIL (guards not yet promoted).

---

### Task 4: Promote the 5 Guard Classes

**Files:**
- Modify: `src/ingestion/import_obso_results.py:40-44`
- Modify: `src/ingestion/import_psxg_predictions.py:29-33`
- Modify: `src/ingestion/import_space_creation.py:33-37`
- Modify: `src/ingestion/player_embeddings_v2.py:41-62`

- [ ] **Step 1: Promote OBSO guard**

In `src/ingestion/import_obso_results.py`, add import and replace the guard class (lines 40–44):

```python
# Before (lines 40-44):
class _ImportObsoGuard:
    workflow_id = "wf-import-obso"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)

# After:
from ingestion.guards import check_hf_dataset_freshness


class _ImportObsoGuard:
    workflow_id = "wf-import-obso"
    _HF_REPO = "luxury-lakehouse/obso-pausa-values"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, self._HF_REPO)
```

Note: `check_hf_dataset_freshness` is imported separately from the existing `from ingestion.guards import FilterResult, timed_check` at line 29 to keep the diff minimal. Alternatively, add it to the existing import — either is fine.

- [ ] **Step 2: Promote PSxG guard**

In `src/ingestion/import_psxg_predictions.py`, replace lines 29–33:

```python
# Before (lines 29-33):
class _ImportPsxgGuard:
    workflow_id = "wf-import-psxg"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)

# After:
from ingestion.guards import check_hf_dataset_freshness


class _ImportPsxgGuard:
    workflow_id = "wf-import-psxg"
    _HF_REPO = "luxury-lakehouse/psxg-predictions"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, self._HF_REPO)
```

- [ ] **Step 3: Promote Space Creation guard**

In `src/ingestion/import_space_creation.py`, replace lines 33–37:

```python
# Before (lines 33-37):
class _ImportSpaceCreationGuard:
    workflow_id = "wf-import-space-creation"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)

# After:
from ingestion.guards import check_hf_dataset_freshness


class _ImportSpaceCreationGuard:
    workflow_id = "wf-import-space-creation"
    _HF_REPO = "luxury-lakehouse/space-creation-values"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, self._HF_REPO)
```

- [ ] **Step 4: Promote Football2Vec v2 + 360 guards**

In `src/ingestion/player_embeddings_v2.py`, replace lines 41–62:

```python
# Before (lines 41-62):
class _Football2VecV2Guard:
    workflow_id = "wf-football2vec-v2"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _Football2VecV2Guard()


class _Football2Vec360Guard:
    workflow_id = "wf-football2vec-360"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        # Placeholder guard — always returns count=1 to trigger a run.
        # A proper guard would check whether the HF dataset commit hash has
        # changed since the last successful import (track in a sidecar file).
        # Deferred to a follow-up cycle — matches the v2 placeholder pattern.
        return FilterResult(workflow_id=self.workflow_id, count=1)


_football2vec_360_guard = _Football2Vec360Guard()

# After:
from ingestion.guards import check_hf_dataset_freshness


class _Football2VecV2Guard:
    workflow_id = "wf-football2vec-v2"
    _HF_REPO = "luxury-lakehouse/football2vec-statsbomb-wyscout"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, self._HF_REPO)


skip_guard = _Football2VecV2Guard()


class _Football2Vec360Guard:
    workflow_id = "wf-football2vec-360"
    _HF_REPO = "luxury-lakehouse/football2vec-360-embeddings"  # pragma: allowlist secret

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(spark, catalog, self.workflow_id, self._HF_REPO)


_football2vec_360_guard = _Football2Vec360Guard()
```

Note: The `_HF_V2_DATASET` and `_HF_360_DATASET` constants at lines 69–70 remain unchanged — they are used by the pipeline body for `hf_hub_download`, not by the guard.

- [ ] **Step 5: Run all guard repo constant tests**

```bash
uv run pytest src/tests/test_hf_guard_freshness.py::TestGuardRepoConstants -v
```

Expected: All 5 tests PASS — each guard now delegates to `check_hf_dataset_freshness` with the correct repo constant.

- [ ] **Step 6: Run full test file**

```bash
uv run pytest src/tests/test_hf_guard_freshness.py -v
```

Expected: All 13 tests PASS.

---

### Task 5: Add SHA Write-Back to Pipeline Bodies

**Files:**
- Modify: `src/ingestion/import_obso_results.py:220-221`
- Modify: `src/ingestion/import_psxg_predictions.py:147-149`
- Modify: `src/ingestion/import_space_creation.py:150-152`
- Modify: `src/ingestion/player_embeddings_v2.py:259-264` and `src/ingestion/player_embeddings_v2.py:454-459`

- [ ] **Step 1: Add write-back to OBSO pipeline**

In `src/ingestion/import_obso_results.py`, add import at top (with the existing guards import) and insert write-back before the final log line at line 220:

Add to imports (line 29, extend existing):
```python
from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
```

Insert before `logger.info("OBSO import complete")` (line 220):
```python
    # Record imported SHA for freshness guard
    record_import_sha(
        spark, catalog, "wf-import-obso", hf_repo,
        filter_result.metadata.get("commit_sha"),
    )
```

- [ ] **Step 2: Add write-back to PSxG pipeline**

In `src/ingestion/import_psxg_predictions.py`, add import and insert write-back before `logger.info("PSxG import complete")` (line 148):

Add to imports (line 18, extend existing):
```python
from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
```

Insert before `logger.info("PSxG import complete")` (line 148):
```python
    # Record imported SHA for freshness guard
    record_import_sha(
        spark, catalog, "wf-import-psxg", HF_REPO,
        filter_result.metadata.get("commit_sha"),
    )
```

- [ ] **Step 3: Add write-back to Space Creation pipeline**

In `src/ingestion/import_space_creation.py`, add import and insert write-back before `logger.info("Space creation import complete")` (line 151):

Add to imports (line 22, extend existing):
```python
from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
```

Insert before `logger.info("Space creation import complete")` (line 151):
```python
    # Record imported SHA for freshness guard
    record_import_sha(
        spark, catalog, "wf-import-space-creation", HF_REPO,
        filter_result.metadata.get("commit_sha"),
    )
```

- [ ] **Step 4: Add write-back to v2 pipeline**

In `src/ingestion/player_embeddings_v2.py`, add `record_import_sha` to the existing guards import (line 19):

```python
from ingestion.guards import FilterResult, check_hf_dataset_freshness, record_import_sha, timed_check
```

In `run_pipeline_v2()` (line 239), insert write-back after the success branch (after line 262, before `return 0`):

```python
    success = _import_v2_embeddings(spark, catalog, schema, logger)
    if success:
        logger.info("v2 transformer embedding import complete")
        record_import_sha(
            spark, catalog, "wf-football2vec-v2", _HF_V2_DATASET,
            filter_result.metadata.get("commit_sha"),
        )
    else:
        logger.info("v2 transformer embeddings not available — no action taken")
    return 0
```

- [ ] **Step 5: Add write-back to 360 pipeline**

In `run_pipeline_360()` (line 435), insert write-back after the success branch (after line 457, before `return row_count`):

```python
    row_count = _import_embeddings_360(spark, catalog, schema, logger)
    if row_count:
        logger.info("360 transformer embedding import complete (%d rows)", row_count)
        record_import_sha(
            spark, catalog, "wf-football2vec-360", _HF_360_DATASET,
            filter_result.metadata.get("commit_sha"),
        )
    else:
        logger.info("360 transformer embeddings not available — no action taken")
    return row_count
```

- [ ] **Step 6: Run all tests**

```bash
uv run pytest src/tests/test_hf_guard_freshness.py -v
```

Expected: All 13 tests PASS (write-back tests already pass from Task 3; guard tests pass from Task 4).

---

### Task 6: Update Conformance Test Exemptions

**Files:**
- Modify: `src/tests/test_guard_conformance.py:26-42`

- [ ] **Step 1: Remove promoted guards from `_METADATA_EXEMPT`**

In `src/tests/test_guard_conformance.py`, remove 4 entries from `_METADATA_EXEMPT` (lines 26–42). The v2 and 360 guards share a module, so only `wf-football2vec-v2` appears in the set. The other 3 are `wf-import-obso`, `wf-import-psxg`, `wf-import-space-creation`.

```python
# Before (lines 26-42):
_METADATA_EXEMPT = {
    "wf-statsbomb",  # Live data, internal skip logic
    "wf-metrica",  # Static dataset, count-based guard
    "wf-idsse",  # Static dataset, count-based guard
    "wf-idsse-events",  # Static dataset, count-based guard
    "wf-skillcorner",  # Static dataset, count-based guard
    "wf-wyscout",  # Static dataset, count-based guard
    "wf-import-obso",  # HF Hub import, always-run
    "wf-import-psxg",  # HF Hub import, always-run
    "wf-import-space-creation",  # HF Hub import, always-run
    "wf-model-validation",  # Monitoring, always-run
    "wf-sync-hf-costs",  # Polling sync, always-run
    "wf-hf-sync",  # Orchestrator, always-run stub
    "wf-football2vec-v2",  # HF Hub import, always-run stub
    "wf-football2vec-v2-export",  # Count-comparison guard
    "wf-prepare-360-data",  # Count-comparison guard
}

# After:
_METADATA_EXEMPT = {
    "wf-statsbomb",  # Live data, internal skip logic
    "wf-metrica",  # Static dataset, count-based guard
    "wf-idsse",  # Static dataset, count-based guard
    "wf-idsse-events",  # Static dataset, count-based guard
    "wf-skillcorner",  # Static dataset, count-based guard
    "wf-wyscout",  # Static dataset, count-based guard
    "wf-model-validation",  # Monitoring, always-run
    "wf-sync-hf-costs",  # Polling sync, always-run
    "wf-hf-sync",  # Orchestrator, always-run stub
    "wf-football2vec-v2-export",  # Count-comparison guard
    "wf-prepare-360-data",  # Count-comparison guard
}
```

- [ ] **Step 2: Run conformance tests**

```bash
uv run pytest src/tests/test_guard_conformance.py -v
```

Expected: All conformance tests PASS. The promoted guards now call `check_hf_dataset_freshness` which returns a `FilterResult` with metadata, satisfying the metadata-quality checks.

Note: The conformance tests mock Spark and call `guard.check()`. Since `check_hf_dataset_freshness` imports `HfApi` inside the function body and accesses `os.environ.get("HF_TOKEN")`, it will attempt a real HF Hub call during conformance tests. The mock Spark won't affect this, but the `HfApi` call will either succeed (public repos) or fail and the guard will fail open (returning count=1, which passes conformance since count>0). If this causes test instability, wrap the HfApi import in the conformance test mock — but try without first since these repos are public.

---

### Task 7: Lint, Type Check, and Full Test Suite

**Files:** None (verification only)

- [ ] **Step 1: Run ruff lint**

```bash
uv run ruff check src/ingestion/guards.py src/ingestion/player_embeddings_v2.py src/ingestion/import_obso_results.py src/ingestion/import_psxg_predictions.py src/ingestion/import_space_creation.py src/tests/test_hf_guard_freshness.py src/tests/test_guard_conformance.py
```

Expected: No violations.

- [ ] **Step 2: Run ruff format check**

```bash
uv run ruff format --check src/ingestion/guards.py src/ingestion/player_embeddings_v2.py src/ingestion/import_obso_results.py src/ingestion/import_psxg_predictions.py src/ingestion/import_space_creation.py src/tests/test_hf_guard_freshness.py src/tests/test_guard_conformance.py
```

Expected: All files formatted correctly.

- [ ] **Step 3: Run pyright type check**

```bash
uv run pyright src/ingestion/guards.py src/ingestion/player_embeddings_v2.py src/ingestion/import_obso_results.py src/ingestion/import_psxg_predictions.py src/ingestion/import_space_creation.py
```

Expected: No errors in basic mode.

- [ ] **Step 4: Run full guard test suite**

```bash
uv run pytest src/tests/test_guards.py src/tests/test_guard_conformance.py src/tests/test_hf_guard_freshness.py -v
```

Expected: All tests PASS.

---

### Task 8: Launch Football2Vec v2 Stage-2 Training

**Files:** None (operational — HF Jobs CLI)

This task depends on Task 1 (stage-1) completing successfully.

- [ ] **Step 1: Verify stage-1 completed**

```bash
hf jobs ps
```

Expected: Stage-1 job shows COMPLETED status.

- [ ] **Step 2: Launch stage-2 adversarial training**

```bash
hf jobs uv run scripts/train_football2vec_v2.py --stage 2 \
    --flavor l40sx1 --timeout 120m \
    --secrets HF_TOKEN=$HF_TOKEN \
    --env MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI \
    --env DATABRICKS_HOST=$DATABRICKS_HOST \
    --env DATABRICKS_TOKEN=$DATABRICKS_TOKEN
```

Expected: Job queued. Stage-2 loads stage-1 checkpoint from `luxury-lakehouse/football2vec-v2/stage1/model.safetensors`, adds gradient reversal adversary, trains for competition debiasing. Takes ~30–60 min on L40S.

- [ ] **Step 3: Monitor stage-2 progress**

```bash
hf jobs ps
hf jobs logs <job-id>
```

Expected: Job completes. Final log shows saved stage-2 model + published embeddings to `luxury-lakehouse/football2vec-statsbomb-wyscout`. Adversary accuracy should approach chance level (~1/num_competitions).

- [ ] **Step 4: Verify published embeddings**

```bash
python -c "from huggingface_hub import HfApi; info = HfApi().repo_info('luxury-lakehouse/football2vec-statsbomb-wyscout', repo_type='dataset'); print(f'SHA: {info.sha}')"
```

Expected: Returns a new SHA (different from the previous Wyscout-only snapshot SHA). This SHA is what the newly-promoted guard will detect on the next daily job run.
