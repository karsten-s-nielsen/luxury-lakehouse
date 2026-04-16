# HF Import Guard Promotion — Design Spec

**Date:** 2026-04-16
**Branch:** `feat/football2vec-guard-promotion`
**Status:** Approved

## Problem

All 6 HF Hub import pipelines use placeholder guards that return `count=1` unconditionally, meaning they re-import on every daily job run regardless of whether the source data has changed. This wastes compute, network, and Delta write costs.

## Scope

Promote 5 of 6 HF import guards from placeholder stubs to SHA-based freshness checks. Leave `wf-sync-hf-costs` as a placeholder — its MERGE-on-`run_id` deduplication is already an effective idempotency mechanism, and the cost of re-running is negligible (small JSON files from 12+ dynamic repos).

### Guards to Promote

| Guard | HF Repo | workflow_id | repo_type |
|-------|---------|-------------|-----------|
| Football2Vec v2 | `luxury-lakehouse/football2vec-statsbomb-wyscout` | `wf-football2vec-v2` | dataset |
| Football2Vec 360 | `luxury-lakehouse/football2vec-360-embeddings` | `wf-football2vec-360` | dataset |
| OBSO/PAUSA import | `luxury-lakehouse/obso-pausa-values` | `wf-import-obso` | dataset |
| PSxG import | `luxury-lakehouse/psxg-predictions` | `wf-import-psxg` | dataset |
| Space Creation import | `luxury-lakehouse/space-creation-values` | `wf-import-space-creation` | dataset |

### Guard Not Promoted

| Guard | workflow_id | Reason |
|-------|-------------|--------|
| HF Cost Sync | `wf-sync-hf-costs` | Dynamically discovers 12+ repos from workflow cards; MERGE on `run_id` provides idempotency; cost of re-running is negligible |

## Design

### 1. New Delta Table — `workflow_import_checksums`

**Schema:** `{catalog}.observability`

```sql
CREATE TABLE IF NOT EXISTS workflow_import_checksums (
    workflow_id       STRING NOT NULL,
    source_repo       STRING NOT NULL,
    repo_type         STRING NOT NULL,
    last_imported_sha STRING NOT NULL,
    imported_at       TIMESTAMP NOT NULL
)
USING DELTA
```

- One row per workflow, keyed on `workflow_id`.
- Bootstrapped via `ensure_table()` on first guard run.
- Written via MERGE (upsert on `workflow_id`) after successful import.

### 2. Shared Helper Function

**Location:** `src/ingestion/guards.py`

```python
def check_hf_dataset_freshness(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    hf_repo: str,
    repo_type: str = "dataset",
) -> FilterResult:
```

**Logic:**

1. Fetch current commit SHA via `HfApi(token=os.environ.get("HF_TOKEN")).repo_info(repo_id=hf_repo, repo_type=repo_type).sha`.
2. Read stored SHA from `{catalog}.observability.workflow_import_checksums WHERE workflow_id = '{workflow_id}'`.
3. If SHA matches stored value: return `FilterResult(workflow_id=workflow_id, count=0)`.
4. If SHA differs or no stored row: return `FilterResult(workflow_id=workflow_id, count=1, metadata={"commit_sha": current_sha})`.
5. If HF Hub is unreachable (network error, timeout): return `FilterResult(workflow_id=workflow_id, count=1)` — fail open, safe to re-import.

**Authentication:** `HF_TOKEN` is read from the environment. These are public repos, so the token is optional but avoids HF Hub rate limiting. If absent, unauthenticated access is used.

### 3. Guard Class Refactoring

Each of the 5 guard classes becomes a thin wrapper calling `check_hf_dataset_freshness` with its specific constants:

```python
class _ImportObsoGuard:
    workflow_id = "wf-import-obso"
    _HF_REPO = "luxury-lakehouse/obso-pausa-values"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        return check_hf_dataset_freshness(
            spark, catalog, self.workflow_id, self._HF_REPO
        )
```

The v2 and 360 guards in `player_embeddings_v2.py` follow the same pattern. The 360 guard remains a private attribute (`_football2vec_360_guard`) per the D62 multi-workflow module relaxation.

### 4. SHA Write-Back

After successful Delta write in each pipeline's `run_pipeline()`, the commit SHA from `filter_result.metadata["commit_sha"]` is written to `workflow_import_checksums` via MERGE:

```python
def _record_import_sha(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    source_repo: str,
    commit_sha: str,
    repo_type: str = "dataset",
) -> None:
```

**Location:** `src/ingestion/guards.py` (co-located with the helper).

**Rules:**
- Write-back only on successful import — never on skip or error.
- The pipeline must check `filter_result.metadata.get("commit_sha")` before calling — if absent (e.g., fail-open path), no write-back.

### 5. Conformance Test Updates

- Remove `wf-football2vec-v2` from `_METADATA_EXEMPT` in `test_guard_conformance.py`.
- Verify that newly-promoted guards pass all existing conformance test classes.
- The 360 guard remains private, so its conformance treatment follows the D62 relaxation.

### 6. Training Rerun (Task #17)

Parallel to guard implementation:

1. Launch `hf jobs uv run scripts/train_football2vec_v2.py --stage 1 --flavor l40sx1 --timeout 120m` with secrets.
2. After stage-1 completes, launch `--stage 2` using the stage-1 checkpoint.
3. Stage-2 publishes retrained model weights and full embeddings (StatsBomb + Wyscout) to `luxury-lakehouse/football2vec-v2` (model) and `luxury-lakehouse/football2vec-statsbomb-wyscout` (dataset).
4. The newly-promoted guard detects the new SHA on the next daily job run and triggers the import.

## Test Plan (TDD)

All guard logic tests mock `HfApi` and use mock Spark for Delta reads/writes.

| # | Test | Input | Expected |
|---|------|-------|----------|
| 1 | First run — no stored SHA | HfApi returns SHA "abc123", empty table | `count=1`, `metadata={"commit_sha": "abc123"}` |
| 2 | Stored SHA matches current | HfApi returns "abc123", table has "abc123" | `count=0` |
| 3 | Stored SHA differs | HfApi returns "def456", table has "abc123" | `count=1`, `metadata={"commit_sha": "def456"}` |
| 4 | HF Hub unreachable | HfApi raises `requests.ConnectionError` | `count=1` (fail open) |
| 5 | Write-back after import | SHA "abc123" passed | Row upserted in `workflow_import_checksums` |
| 6 | No write-back on skip | `count=0` path | Table unchanged |
| 7 | Each guard passes correct repo | Import-level check | Correct `hf_repo` constant per guard |

## Files Changed

| File | Change |
|------|--------|
| `src/ingestion/guards.py` | Add `check_hf_dataset_freshness()`, `_record_import_sha()`, table DDL constant |
| `src/ingestion/player_embeddings_v2.py` | Promote v2 + 360 guards, add SHA write-back to both `run_pipeline` functions |
| `src/ingestion/import_obso_results.py` | Promote guard, add SHA write-back |
| `src/ingestion/import_psxg_predictions.py` | Promote guard, add SHA write-back |
| `src/ingestion/import_space_creation.py` | Promote guard, add SHA write-back |
| `src/tests/test_guard_conformance.py` | Remove `wf-football2vec-v2` from `_METADATA_EXEMPT` |
| `src/tests/test_hf_guard_freshness.py` | New — TDD tests for SHA-based guard logic |

## Scope Boundary

- Only the 5 listed guards are promoted. `wf-sync-hf-costs` stays as placeholder.
- No changes to `wf-statsbomb` (different import mechanism — API, not HF Hub).
- No base class hierarchy — shared helper function only.
- No changes to the daily job Terraform (guards are backward-compatible — same `check()` signature).
