# F2V Dimension Fix + V1 Deprecation — Design Spec

**Date**: 2026-05-07
**Status**: Implemented (rev 3, all review items addressed)
**Scope**: Bug fix (3 bugs) + deprecation (v1 embeddings)

## 1. Context

The evolve engine promoted `Football2VecConfig.hidden_dim` from 128 to 192 in
PR #158 (commit `b4ebf94`, 2026-04-19). All downstream dimension constants were
never updated. Additionally, the SK3-MIG-B Phase 9 retrain orchestrator only
dispatches stage1 for two-stage trainers (f2v_v2, f2v_360), and stage1
overwrites the production HF embeddings dataset with non-debiased vectors.

The combination caused `compute_embeddings_360` to fail on the daily Databricks
job with `RuntimeError: 360 Parquet vector at row 0 has length 208, expected 144`.
The v2 importer silently ingested 192-dim vectors without validation.

Separately, `compute_embeddings_v1` (Doc2Vec, 32d) still runs daily despite
being superseded. Its output is filtered out by every downstream dbt mart
(`size(behavioral_vector) != 32`). It consumes ~149-291 DBUs per run for zero
value.

## 2. Bugs

### Bug 1: Stale dimension constants

| Constant | Location | Current | Correct | Derivation |
|----------|----------|---------|---------|------------|
| `OUTPUT_DIM` | `scripts/train_football2vec_360_helpers.py` | 144 | 208 | `hidden_dim(192) + context_dim(16)` |
| `_V360_BEHAVIORAL_DIM` | `src/ingestion/player_embeddings_v2.py` | 144 | 208 | Same |
| `_V2_BEHAVIORAL_DIM` | `src/ingestion/player_embeddings_v2.py` | 128 | 192 | `hidden_dim(192)` |
| HNSW 360 indexes | `scripts/create_indexes.py` (HNSW_INDEXES list) | `vector(144)` | `vector(208)` | Matches `_V360_BEHAVIORAL_DIM` |

Source of truth: `Football2VecConfig.hidden_dim = 192` in
`src/analytics/football2vec_transformer.py`. The v2 HNSW indexes are already
at `vector(192)` (correct). Parity enforced by `src/tests/test_hnsw_dim_parity.py`.

### Bug 2: Stage1 overwrites production embeddings

Both `train_football2vec_360.py` and `train_football2vec_v2.py` call their
`_publish_embeddings` / `_publish_emb` function from `_run_stage1`. Stage1
produces non-debiased embeddings that overwrite the production HF dataset.
Only stage2 (adversarial competition debiasing) should publish.

**Causal chain (2026-05-06)**:
1. SK3-MIG-B orchestrator dispatches f2v_360 with no `--stage` arg (defaults to 1)
2. Stage1 trains, then calls `_publish_embeddings(embeddings_df, hf_token, "stage1")`
3. Production HF dataset overwritten with 208-dim stage1 vectors (replacing working 144-dim stage2 vectors from 2026-04-02)
4. Daily `compute_embeddings_360` downloads 208-dim vectors, dimension guard fires

### Bug 3: Orchestrator lacks two-stage dispatch

`_dispatch_trained_model` in `scripts/sk3_mig_b_retrain.py` fires a single
`api.run_uv_job(script=...)` with no `--stage` argument. Both f2v_v2 and
f2v_360 trainers default to `--stage 1`. Stage2 is never dispatched.

## 3. Fixes

### 3.1 Dimension constant updates

Update all stale constants to match the current model architecture:

- `scripts/train_football2vec_360_helpers.py`: `OUTPUT_DIM = 208`
- `src/ingestion/player_embeddings_v2.py`: `_V2_BEHAVIORAL_DIM = 192`, `_V360_BEHAVIORAL_DIM = 208`
- `scripts/create_indexes.py`: 360 HNSW entries from `vector(144)` to `vector(208)`
- `src/analytics/football2vec_360.py`: Update class/method docstrings (144d to 208d)
- `scripts/train_football2vec_360.py`: Update module docstring (references "128d transformer + 16d Deep Sets context = 144d")

Note: `OUTPUT_DIM` lives in `train_football2vec_360_helpers.py` (imported by
the trainer at line 45 via `from train_football2vec_360_helpers import ... OUTPUT_DIM`).
The helpers file is the single source for this constant; the trainer consumes it
in `_save_checkpoint` (config.json metadata), `_generate_embeddings` docstring,
and metrics dicts.

### 3.2 Remove stage1 publish

Remove the `_publish_embeddings` / `_publish_emb` call from `_run_stage1` in
both trainers:

- `scripts/train_football2vec_360.py` — delete `_publish_embeddings(embeddings_df, hf_token, "stage1")` call in `_run_stage1` (committed line 827)
- `scripts/train_football2vec_v2.py` — delete `_publish_emb(emb_df, hf_token, "stage1")` call in `_run_stage1` (committed line 744)

Stage1 continues to save its checkpoint via `_save_checkpoint` / `_save_ckpt`
(required for stage2's `_load_stage1`). Only stage2 publishes to the production
HF embeddings dataset.

### 3.3 Two-stage orchestrator dispatch

Add a module-level constant identifying two-stage items:

```python
_TWO_STAGE_ITEMS: set[str] = {"f2v_v2", "f2v_360"}
```

Modify `_dispatch_trained_model` so that when `cycle_item in _TWO_STAGE_ITEMS`:

1. Build stage1 args: `(script_args or []) + ["--stage", "1"]`
2. Dispatch via `api.run_uv_job(script=script, script_args=stage1_args, flavor=flavor, timeout=timeout, secrets={...})`
3. Poll to completion via existing `_poll_hf_job_until_terminal`
4. Build stage2 args: `(script_args or []) + ["--stage", "2"]`
5. Dispatch second HF Job with `script_args=stage2_args`
6. Poll to completion
7. Return the stage2 `job_id`

Concrete API call shape (the `script_args` parameter is `Optional[list[str]]`
on `HfApi.run_uv_job`):

```python
job = api.run_uv_job(
    script=script,
    script_args=stage_args,  # ["--stage", "1"] or ["--stage", "2"]
    flavor=flavor,
    timeout=timeout,
    secrets={...},
)
```

Status emissions include the stage label:
`f"HF Job dispatched: script={script} flavor={flavor} stage={stage_num}"`.

For non-two-stage items, behavior is unchanged — no `--stage` arg, single dispatch.

Update `_ITEM_COST_USD` to reflect doubled cost for two-stage items:

```python
"f2v_v2": 8.00,    # was 4.00 — now stage1 + stage2
"f2v_360": 10.00,  # was 5.00 — now stage1 + stage2
```

This prevents the orchestrator's cumulative cost guard from halting prematurely
when f2v items legitimately consume 2x their previous budget.

### 3.4 V2 dimension validation guard

Add a dimension check to `_import_v2_embeddings` in `player_embeddings_v2.py`,
mirroring the existing 360 guard pattern. Inserted after the empty/column checks
in `_import_v2_embeddings`, before any processing:

```python
for i, vec in enumerate(v2_pdf["behavioral_vector"].iloc[:10]):
    vec_list = list(vec) if not isinstance(vec, list) else vec
    if len(vec_list) != _V2_BEHAVIORAL_DIM:
        msg = (
            f"v2 Parquet vector at row {i} has length {len(vec_list)}, "
            f"expected {_V2_BEHAVIORAL_DIM}"
        )
        raise RuntimeError(msg)
```

## 4. V1 Deprecation

### 4.1 Remove from daily job

| File | Change |
|------|--------|
| `pyproject.toml` | Remove `compute_embeddings_v1` entry point |
| `terraform/modules/workflows/main.tf` | Remove `compute_embeddings_v1` task block |
| `terraform/modules/workflows/main.tf` | Remove `depends_on { task_key = "compute_embeddings_v1" }` from dbt_build task |
| `terraform/modules/workflows/main.tf` | Update task-key comment list |

### 4.2 Remove combined orchestrator

Remove from `src/ingestion/player_embeddings_v2.py`:

- `run_pipeline()` function — the v2-then-v1-fallback orchestrator
- `main()` function — CLI entry point for `run_pipeline()`

This also removes the last runtime import of `player_embeddings_v1` from the
v2 module (`from ingestion.player_embeddings_v1 import run_pipeline_v1` inside
`run_pipeline()`). The v1 module remains importable for tests/reference but is
no longer reachable from any production code path.

Retain:
- `main_v2()` — dedicated v2 entry point (used by `compute_embeddings_v2`)
- `main_360()` — dedicated 360 entry point (used by `compute_embeddings_360`)

### 4.3 Not touched

These are left in place intentionally:

- `src/ingestion/player_embeddings_v1.py` — file stays (importable for tests/reference), just unwired from all entry points and production code paths
- `src/analytics/football2vec.py` — Doc2Vec code, no runtime cost
- `workflow-cards/wf-football2vec.yaml` — already `status: deprecated`
- `scripts/train_football2vec.py` — v1 trainer, harmless
- dbt marts — already filter by `size(behavioral_vector) != 32`

## 5. File Change Summary

### Wheel changes (require version bump via `scripts/bump_wheel.py`)

| File | Change |
|------|--------|
| `src/ingestion/player_embeddings_v2.py` | `_V2_BEHAVIORAL_DIM=192`, `_V360_BEHAVIORAL_DIM=208`, add v2 dim guard, remove `run_pipeline()` + `main()` |
| `src/analytics/football2vec_360.py` | Update docstrings (144d to 208d) |
| `pyproject.toml` | Remove `compute_embeddings_v1` entry point |

### Script changes (no wheel needed)

| File | Change |
|------|--------|
| `scripts/train_football2vec_360_helpers.py` | `OUTPUT_DIM=208` |
| `scripts/train_football2vec_360.py` | Remove stage1 `_publish_embeddings` call, update module docstring |
| `scripts/train_football2vec_v2.py` | Remove stage1 `_publish_emb` call |
| `scripts/sk3_mig_b_retrain.py` | `_TWO_STAGE_ITEMS`, two-stage dispatch logic, update `_ITEM_COST_USD` |
| `scripts/create_indexes.py` | 360 HNSW: `vector(144)` to `vector(208)` |

### Infrastructure

| File | Change |
|------|--------|
| `terraform/modules/workflows/main.tf` | Remove v1 task block + `depends_on` reference |

## 6. Test Impact

- `test_hnsw_dim_parity.py` — passes automatically (reads from `_V360_BEHAVIORAL_DIM` source-of-truth)
- `test_player_embeddings.py` — **breaking change**: imports `main` from `player_embeddings_v2` (line 25: `from ingestion.player_embeddings_v2 import main as _main_combined`). Used in 3 test methods (lines 992, 1034, 1070) plus a direct import at line 1097. These tests must be removed or rewired to `main_v2` since `main()` is deleted by §4.2.
- `test_player_embeddings_360.py` — **7 hardcoded 144 locations**:

  | Line | Current | Correct |
  |------|---------|---------|
  | 5 | docstring "144-dimensional" | "208-dimensional" |
  | 45 | `[0.1] * 144` test vector | `[0.1] * 208` |
  | 46 | `[0.2] * 144` test vector | `[0.2] * 208` |
  | 47 | `[0.3] * 144` test vector | `[0.3] * 208` |
  | 86 | docstring "vectors with length != 144" | "vectors with length != 208" |
  | 94 | `[0.1] * 128` wrong-dim test input | `[0.1] * 192` (any value != 208 works; 192 is internally consistent with v2 dim) |
  | 106 | `pytest.raises(..., match="144")` | `match="208"` — **critical**: stale match string causes false test failure |

- `test_football2vec_360.py` — check for hardcoded 144, update if present
- `sk3_mig_b/test_f2v_360_post_retrain_smoke.py` — check for hardcoded 144, update if present

## 7. Post-Merge Operator Actions

1. **`terraform apply`** to remove the v1 task from the Databricks job
2. **HNSW 360 index recreation**: Run `scripts/create_indexes.py` after next dbt refresh imports 208-dim embeddings
3. **Phase 9 f2v_360 stage2**: Dispatch standalone via `--stage 2`. The stage1
   checkpoint on HF (commit `1e65e2d1ba`, 2026-05-06 22:09:28) was trained on
   post-SK3-MIG-B corrected data (silly-kicks 3.7.0, correct SPADL coordinates).
   Stage2's `_load_stage1` downloads this checkpoint — no stage1 re-run needed.
4. **Phase 9 f2v_v2 stage2**: Same pattern — dispatch `--stage 2` to produce
   debiased embeddings and overwrite the current stage1-only 192-dim vectors on HF.
5. **V2 bronze re-import**: After f2v_v2 stage2 publishes, trigger
   `compute_embeddings_v2` on the daily job (or manual `run_now`) to re-import
   the debiased 192-dim vectors into `bronze.player_embeddings_raw`. The
   currently-live bronze rows with v2 data sources contain non-debiased stage1
   vectors that were silently imported (no dim guard existed). The new dim guard
   (§3.4) prevents future silent drift; the re-import fixes the current state.

## 8. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| HNSW build fails on dimension mismatch during transition | Indexes are rebuilt by `create_indexes.py` after data lands; daily Lakebase Maintenance handles recreation |
| Stage2 dispatch doubles HF Jobs cost for f2v items | `_ITEM_COST_USD` updated to reflect 2x cost; cumulative cost guard won't halt prematurely |
| Removing v1 fallback breaks `compute_embeddings` entry point | No `compute_embeddings` entry point exists; only `compute_embeddings_v2` and `compute_embeddings_360` are wired |
| `test_player_embeddings.py` imports removed `main()` | Tests rewired to `main_v2` or removed during implementation |
| Tests hardcode 144-dim | Scan and update during implementation |
