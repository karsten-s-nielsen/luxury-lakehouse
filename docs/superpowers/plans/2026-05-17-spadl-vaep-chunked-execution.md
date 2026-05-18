# SPADL/VAEP Chunked Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `compute_spadl_vaep` from a monolithic 30-min-timeout task into a preflight + for_each_task pattern so it scales to any backlog size.

**Architecture:** Preflight discovers work (guard) + caches VAEP models to UC Volume + emits chunk strings as task values. Each for_each_task iteration parses its chunk, converts one provider's matches to SPADL (scoped to chunk match IDs only), then scores them with VAEP. Same pattern as `tracking_context.py`.

**Tech Stack:** PySpark, XGBoost, MLflow, Databricks for_each_task, Delta Lake replaceWhere, UC Volumes

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/ingestion/spadl_vaep.py` | Modify | Guard metadata → per-provider keys; add `_parse_vaep_match_ids_arg`; add `_build_chunks`; add `_vaep_output_schema`; add `_run_chunk`; add `main_preflight`; modify `main` to accept `--match-ids` |
| `src/ingestion/spadl_conversion.py` | Modify | Add `match_id_filter: set[int] | None = None` parameter to all 5 converter functions |
| `src/tests/test_spadl_vaep_preflight.py` | Create | Chunk parser tests, chunk builder tests, model cache round-trip test |
| `src/tests/test_spadl_vaep.py` | Modify | Update guard metadata assertions for new per-provider keys |
| `terraform/modules/workflows/main.tf` | Modify | Replace monolithic task with preflight + for_each_task |
| `pyproject.toml` | Modify | Add `preflight_spadl_vaep` entry point (version bump via `scripts/bump_wheel.py`) |

---

### Task 1: Chunk Parser (TDD)

**Files:**
- Create: `src/tests/test_spadl_vaep_preflight.py`
- Modify: `src/ingestion/spadl_vaep.py` (add `_parse_vaep_match_ids_arg`)

- [ ] **Step 1: Write failing tests for chunk parsing**

```python
"""Tests for SPADL/VAEP preflight chunking and --match-ids parsing."""

from __future__ import annotations

import pytest


def test_parse_vaep_match_ids_arg_none() -> None:
    """None input returns None (no filter)."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    assert _parse_vaep_match_ids_arg(None) is None


def test_parse_vaep_match_ids_arg_empty() -> None:
    """Empty string returns None."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    assert _parse_vaep_match_ids_arg("") is None


def test_parse_vaep_match_ids_arg_convert_chunk() -> None:
    """Provider:ids format returns (provider, [ids])."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    result = _parse_vaep_match_ids_arg("statsbomb:3754348,3754349,3754350")
    assert result == ("statsbomb", [3754348, 3754349, 3754350])


def test_parse_vaep_match_ids_arg_score_chunk() -> None:
    """score: prefix returns ("score", [ids])."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    result = _parse_vaep_match_ids_arg("score:100,200,300")
    assert result == ("score", [100, 200, 300])


def test_parse_vaep_match_ids_arg_single_id() -> None:
    """Single match ID works."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    result = _parse_vaep_match_ids_arg("wyscout:12345")
    assert result == ("wyscout", [12345])


def test_parse_vaep_match_ids_arg_all_providers() -> None:
    """All valid providers parse correctly."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    for provider in ("statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "score"):
        result = _parse_vaep_match_ids_arg(f"{provider}:999")
        assert result is not None
        assert result[0] == provider


def test_parse_vaep_match_ids_arg_unknown_provider() -> None:
    """Unknown provider raises SystemExit."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    with pytest.raises(SystemExit, match="Unknown provider"):
        _parse_vaep_match_ids_arg("opta:12345")


def test_parse_vaep_match_ids_arg_no_colon() -> None:
    """Missing colon raises SystemExit."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    with pytest.raises(SystemExit, match="must be"):
        _parse_vaep_match_ids_arg("12345,67890")


def test_parse_vaep_match_ids_arg_non_integer_ids() -> None:
    """Non-integer match IDs raise SystemExit."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    with pytest.raises(SystemExit, match="non-integer"):
        _parse_vaep_match_ids_arg("statsbomb:abc,def")


def test_chunk_encoding_round_trip() -> None:
    """Chunk string round-trips through parse."""
    from ingestion.spadl_vaep import _parse_vaep_match_ids_arg

    chunk_str = "idsse:111111,222222,333333"
    result = _parse_vaep_match_ids_arg(chunk_str)
    assert result is not None
    provider, ids = result
    reconstructed = f"{provider}:{','.join(str(i) for i in ids)}"
    assert reconstructed == chunk_str
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py -v`
Expected: FAIL with `ImportError` or `AttributeError` — `_parse_vaep_match_ids_arg` does not exist yet.

- [ ] **Step 3: Implement the chunk parser**

Add to `src/ingestion/spadl_vaep.py` after line 272 (`skip_guard = _VaepGuard()`):

```python
_VALID_CHUNK_PROVIDERS = frozenset({"statsbomb", "wyscout", "idsse", "metrica", "skillcorner", "score"})


def _parse_vaep_match_ids_arg(raw: str | None) -> tuple[str, list[int]] | None:
    """Parse a for_each_task chunk string into (provider, match_ids).

    Chunk grammar:
        convert_chunk := provider ":" match_id_list
        score_chunk   := "score:" match_id_list
        match_id_list := BIGINT ("," BIGINT)*

    Returns None for None/empty input. Raises SystemExit on invalid format.
    """
    if not raw:
        return None

    if ":" not in raw:
        raise SystemExit(
            f"--match-ids must be 'provider:id1,id2,...' — got '{raw}'"
        )

    provider, ids_str = raw.split(":", 1)

    if provider not in _VALID_CHUNK_PROVIDERS:
        raise SystemExit(
            f"Unknown provider '{provider}' — must be one of {sorted(_VALID_CHUNK_PROVIDERS)}"
        )

    try:
        match_ids = [int(mid) for mid in ids_str.split(",")]
    except ValueError as exc:
        raise SystemExit(
            f"--match-ids contains non-integer IDs: {exc}"
        ) from exc

    return (provider, match_ids)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py -v`
Expected: All PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/spadl_vaep.py src/tests/test_spadl_vaep_preflight.py && uv run pyright src/ingestion/spadl_vaep.py`
Expected: Clean.

---

### Task 2: Guard Metadata — Per-Provider Keys (TDD)

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:191-222` (guard `.check()` return)
- Modify: `src/tests/test_spadl_vaep.py` (update metadata assertions)

- [ ] **Step 1: Update existing guard tests to expect per-provider keys**

In `src/tests/test_spadl_vaep.py`, class `TestVaepGuardMetadata`, update all assertions that reference `"new_spadl_match_ids"` to instead check per-provider keys `"sb_new"`, `"ws_new"`, `"idsse_new"`, `"metrica_new"`, `"sc_new"`.

For `test_regression_today_scenario_sb_new_nonempty_unscored_empty`, change:
```python
assert result.metadata["new_spadl_match_ids"] == ["3754348", "3754349", "3754350"]
```
to:
```python
assert result.metadata["sb_new"] == ["3754348", "3754349", "3754350"]
assert result.metadata["ws_new"] == []
assert result.metadata["idsse_new"] == []
assert result.metadata["metrica_new"] == []
assert result.metadata["sc_new"] == []
```

For `test_disjoint_union_both_sources_contribute`, change:
```python
assert result.metadata["new_spadl_match_ids"] == ["sb1", "ws1"]
```
to:
```python
assert result.metadata["sb_new"] == ["sb1"]
assert result.metadata["ws_new"] == ["ws1"]
```

For `test_unscored_only_no_new_events`:
```python
assert result.metadata["new_spadl_match_ids"] == []
```
becomes:
```python
assert result.metadata["sb_new"] == []
assert result.metadata["ws_new"] == []
assert result.metadata["idsse_new"] == []
assert result.metadata["metrica_new"] == []
assert result.metadata["sc_new"] == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_spadl_vaep.py::TestVaepGuardMetadata -v`
Expected: FAIL — metadata still has `new_spadl_match_ids` key.

- [ ] **Step 3: Modify guard to emit per-provider keys**

In `src/ingestion/spadl_vaep.py`, replace lines 191–223.

Remove line 191 (`new_spadl = sorted(set(sb_new) | ...)`).

Replace `total_new` calculation (line 200):
```python
        total_new = len(sb_new) + len(ws_new) + len(idsse_new) + len(metrica_new) + len(sc_new) + len(unscored)
```

Replace the `FilterResult` return (lines 216–223):
```python
        return FilterResult(
            workflow_id=self.workflow_id,
            count=total_new,
            metadata={
                "sb_new": sb_new,
                "ws_new": ws_new,
                "idsse_new": idsse_new,
                "metrica_new": metrica_new,
                "sc_new": sc_new,
                "unscored_vaep_match_ids": sorted(unscored),
            },
        )
```

- [ ] **Step 4: Update `run_pipeline` to compute union from per-provider keys**

Replace lines 656–667 in `run_pipeline`:

```python
    # Compute unscored set: union of all per-provider new match IDs + pure unscored
    unscored_ids = sorted(
        set(filter_result.metadata["unscored_vaep_match_ids"])
        | set(filter_result.metadata["sb_new"])
        | set(filter_result.metadata["ws_new"])
        | set(filter_result.metadata["idsse_new"])
        | set(filter_result.metadata["metrica_new"])
        | set(filter_result.metadata["sc_new"]),
    )
```

Note: Direct key access (no `.get()`) — the guard always emits all 6 keys. Missing keys indicate a bug that should propagate loudly.

- [ ] **Step 5: Run guard metadata tests + conformance**

Run: `uv run pytest src/tests/test_spadl_vaep.py::TestVaepGuardMetadata src/tests/test_guard_conformance.py -v -k "vaep or count_equals"`
Expected: All PASS. The conformance test sums all `list[str]` values — per-provider keys are all `list[str]`, so it passes without exemption.

---

### Task 3: Add `match_id_filter` to Converter Functions

**Files:**
- Modify: `src/ingestion/spadl_conversion.py` (all 5 converter functions)
- Add tests to: `src/tests/test_spadl_vaep_preflight.py`

This is the critical change that makes chunking work. Without it, every iteration would convert ALL matches for a provider, defeating the purpose of chunking.

- [ ] **Step 1: Write test for match_id_filter behavior**

Append to `src/tests/test_spadl_vaep_preflight.py`:

```python
def test_converter_match_id_filter_concept() -> None:
    """Verify the match_id_filter intersection logic works correctly.

    This tests the pattern used in all 5 converters:
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]
    if match_id_filter is not None:
        new_game_ids = [gid for gid in new_game_ids if gid in match_id_filter]
    """
    all_game_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    existing_matches: set[int] = {1, 2, 3}  # already converted

    # Without filter: converts 4-10 (7 matches)
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]
    assert new_game_ids == [4, 5, 6, 7, 8, 9, 10]

    # With filter: only converts the chunk's matches (4, 5)
    match_id_filter = {4, 5}
    filtered = [gid for gid in new_game_ids if gid in match_id_filter]
    assert filtered == [4, 5]
```

- [ ] **Step 2: Add `match_id_filter` parameter to all 5 converters**

Each converter function (`_convert_statsbomb_from_bronze`, `_convert_wyscout_from_bronze`, `_convert_idsse_from_bronze`, `_convert_metrica_from_bronze`, `_convert_skillcorner_from_bronze`) gets one change:

**Signature change** — add optional parameter:
```python
def _convert_statsbomb_from_bronze(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    existing_matches: set[int],
    match_id_filter: set[int] | None = None,  # NEW
) -> bool:
```

**Logic change** — add 2 lines after the `new_game_ids` list comprehension (e.g., after line 304 for StatsBomb):
```python
    new_game_ids = [gid for gid in all_game_ids if gid not in existing_matches]
    if match_id_filter is not None:
        new_game_ids = [gid for gid in new_game_ids if gid in match_id_filter]
```

Apply the same pattern to all 5 converters. The existing callers in `run_pipeline` pass no `match_id_filter` (defaults to None = no filtering = existing behavior preserved).

For IDSSE/Metrica/SkillCorner, the `all_game_ids` are BIGINT hashes. The chunk's `match_ids` are also BIGINTs (from the guard's `_diff_hashed_source_against_spadl`). So the filter works identically.

- [ ] **Step 3: Verify existing tests still pass**

Run: `uv run pytest src/tests/test_spadl_vaep.py src/tests/test_spadl_vaep_preflight.py -v`
Expected: All PASS — existing callers don't pass `match_id_filter`, so behavior is unchanged.

- [ ] **Step 4: Run ruff + pyright on spadl_conversion.py**

Run: `uv run ruff check src/ingestion/spadl_conversion.py && uv run pyright src/ingestion/spadl_conversion.py`
Expected: Clean.

---

### Task 4: Chunk Builder + Preflight Entry Point

**Files:**
- Modify: `src/ingestion/spadl_vaep.py` (add `_build_chunks`, `_cache_vaep_models_to_volume`, `main_preflight`)
- Add tests to: `src/tests/test_spadl_vaep_preflight.py`

- [ ] **Step 1: Write failing tests for chunk builder**

Append to `src/tests/test_spadl_vaep_preflight.py`:

```python
def test_build_chunks_statsbomb_200_per_chunk() -> None:
    """StatsBomb matches chunked at 200."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [str(i) for i in range(450)],
        "ws_new": [],
        "idsse_new": [],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": [],
    }
    chunks = _build_chunks(metadata)
    # 450 / 200 = 3 chunks (200 + 200 + 50)
    assert len(chunks) == 3
    assert all(c.startswith("statsbomb:") for c in chunks)
    # First chunk has 200 IDs
    assert len(chunks[0].split(":")[1].split(",")) == 200
    # Last chunk has 50
    assert len(chunks[2].split(":")[1].split(",")) == 50


def test_build_chunks_multiple_providers() -> None:
    """Multiple providers produce separate chunk lists."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": ["1", "2", "3"],
        "ws_new": ["10", "11"],
        "idsse_new": ["100"],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": ["500", "501"],
    }
    chunks = _build_chunks(metadata)
    providers = [c.split(":")[0] for c in chunks]
    assert "statsbomb" in providers
    assert "wyscout" in providers
    assert "idsse" in providers
    assert "score" in providers
    assert len(chunks) == 4  # sb(1) + ws(1) + idsse(1) + score(1)


def test_build_chunks_unscored_uses_score_prefix() -> None:
    """Unscored matches use 'score:' prefix."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [],
        "ws_new": [],
        "idsse_new": [],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": ["1", "2", "3"],
    }
    chunks = _build_chunks(metadata)
    assert len(chunks) == 1
    assert chunks[0] == "score:1,2,3"


def test_build_chunks_empty_metadata() -> None:
    """Empty metadata produces no chunks."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [],
        "ws_new": [],
        "idsse_new": [],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": [],
    }
    chunks = _build_chunks(metadata)
    assert chunks == []


def test_build_chunks_idsse_50_per_chunk() -> None:
    """IDSSE/Metrica/SkillCorner chunked at 50."""
    from ingestion.spadl_vaep import _build_chunks

    metadata = {
        "sb_new": [],
        "ws_new": [],
        "idsse_new": [str(i) for i in range(120)],
        "metrica_new": [],
        "sc_new": [],
        "unscored_vaep_match_ids": [],
    }
    chunks = _build_chunks(metadata)
    assert len(chunks) == 3  # 50 + 50 + 20
    assert all(c.startswith("idsse:") for c in chunks)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py -v -k "build_chunks"`
Expected: FAIL — `_build_chunks` does not exist.

- [ ] **Step 3: Implement chunk builder**

Add to `src/ingestion/spadl_vaep.py` after `_parse_vaep_match_ids_arg`:

```python
_CHUNK_SIZES: dict[str, int] = {
    "statsbomb": 200,
    "wyscout": 200,
    "idsse": 50,
    "metrica": 50,
    "skillcorner": 50,
    "score": 200,
}

_PROVIDER_METADATA_KEYS: dict[str, str] = {
    "sb_new": "statsbomb",
    "ws_new": "wyscout",
    "idsse_new": "idsse",
    "metrica_new": "metrica",
    "sc_new": "skillcorner",
}


def _build_chunks(metadata: dict[str, Any]) -> list[str]:
    """Build for_each_task chunk strings from guard metadata.

    Each chunk is ``"provider:id1,id2,...,idN"`` for convert chunks
    or ``"score:id1,id2,...,idN"`` for score-only chunks.
    """
    chunks: list[str] = []

    # Convert chunks (per-provider) — direct access, guard always emits all keys
    for meta_key, provider in _PROVIDER_METADATA_KEYS.items():
        ids = metadata[meta_key]
        if not ids:
            continue
        chunk_size = _CHUNK_SIZES[provider]
        for i in range(0, len(ids), chunk_size):
            batch = ids[i : i + chunk_size]
            chunks.append(f"{provider}:{','.join(str(mid) for mid in batch)}")

    # Score-only chunks (unscored matches already in spadl_actions)
    unscored = metadata["unscored_vaep_match_ids"]
    if unscored:
        chunk_size = _CHUNK_SIZES["score"]
        for i in range(0, len(unscored), chunk_size):
            batch = unscored[i : i + chunk_size]
            chunks.append(f"score:{','.join(str(mid) for mid in batch)}")

    return chunks
```

- [ ] **Step 4: Run chunk builder tests**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py -v -k "build_chunks"`
Expected: All PASS.

- [ ] **Step 5: Implement model caching + preflight entry point**

Add to `src/ingestion/spadl_vaep.py` before `main()`:

```python
_VAEP_MODEL_CACHE_BASE = "/Volumes/{catalog}/{schema}/model_weights/vaep_cache"


def _cache_vaep_models_to_volume(
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> str | None:
    """Download VAEP Champion models from MLflow and cache to UC Volume.

    Writes scores.xgb + concedes.xgb to a run-scoped directory under the
    existing model_weights Volume (Terraform-managed, ingestion SP has grants).
    Returns the directory path, or None if no Champion model exists.

    Prerequisite: UC Volume /Volumes/soccer_analytics/dev_gold/model_weights/
    must exist. Already Terraform-managed (terraform/modules/catalog/main.tf:134-137)
    with ingestion SP grants — no manual creation needed.
    """
    import os
    import time

    models = _load_models(catalog, schema, logger)
    if models is None:
        return None

    model_scores, model_concedes = models

    scores_raw = bytes(model_scores.get_booster().save_raw("json"))
    concedes_raw = bytes(model_concedes.get_booster().save_raw("json"))

    # Run-scoped directory prevents interference between concurrent preflights
    run_id = os.environ.get("DATABRICKS_RUN_ID", str(int(time.time())))
    base_path = _VAEP_MODEL_CACHE_BASE.format(catalog=catalog, schema="dev_gold")
    model_dir = f"{base_path}/{run_id}"

    # Write both model files
    scores_path = f"{model_dir}/scores.xgb"
    concedes_path = f"{model_dir}/concedes.xgb"

    os.makedirs(model_dir, exist_ok=True)
    with open(scores_path, "wb") as f:
        f.write(scores_raw)
    with open(concedes_path, "wb") as f:
        f.write(concedes_raw)

    logger.info(
        "Cached VAEP models to %s (scores=%d bytes, concedes=%d bytes)",
        model_dir,
        len(scores_raw),
        len(concedes_raw),
    )

    # Cleanup: remove directories older than 7 days (non-critical)
    _cleanup_old_model_cache(base_path, max_age_days=7, logger=logger)

    return model_dir


def _cleanup_old_model_cache(base_path: str, max_age_days: int, logger: logging.Logger) -> None:
    """Remove model cache directories older than max_age_days."""
    import os
    import shutil
    import time

    if not os.path.isdir(base_path):
        return

    cutoff = time.time() - (max_age_days * 86400)
    for entry in os.listdir(base_path):
        entry_path = os.path.join(base_path, entry)
        if os.path.isdir(entry_path):
            try:
                mtime = os.path.getmtime(entry_path)
                if mtime < cutoff:
                    shutil.rmtree(entry_path)
                    logger.info("Cleaned old model cache: %s", entry_path)
            except OSError as exc:
                logger.debug("Model cache cleanup skipped for %s: %s", entry_path, exc)


def main_preflight() -> None:
    """CLI entry point for SPADL/VAEP preflight (guard + chunk emission + model cache).

    Runs the skip guard, builds chunk strings from per-provider match ID lists,
    caches VAEP models to UC Volume, and emits both as Databricks task values
    for downstream for_each_task iterations.
    """
    args = parse_ingestion_args("Preflight: discover unprocessed SPADL/VAEP matches and emit chunks")
    logger = configure_logging("spadl_vaep_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    chunks = _build_chunks(filter_result.metadata) if filter_result.count > 0 else []
    logger.info(
        "VAEP preflight: %d matches across %d chunks",
        filter_result.count,
        len(chunks),
    )

    # Cache models to UC Volume (only if there's work to do)
    model_path: str | None = None
    if chunks:
        model_path = _cache_vaep_models_to_volume(args.catalog, args.schema, logger)
        if model_path is None:
            raise SystemExit(
                "No Champion VAEP model found in MLflow — cannot score. "
                "Run scripts/train_vaep_model_hf.py first."
            )

    # Emit task values
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="spadl_vaep_chunks", value=chunks)
        logger.info("Wrote task value 'spadl_vaep_chunks' (%d chunks)", len(chunks))
        if model_path:
            dbutils.jobs.taskValues.set(key="vaep_model_path", value=model_path)
            logger.info("Wrote task value 'vaep_model_path' = %s", model_path)
    except (ImportError, AttributeError, RuntimeError) as exc:
        logger.warning("Task values not available (likely standalone mode) -- %s", exc)
```

- [ ] **Step 6: Run all preflight tests + ruff**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py src/tests/test_spadl_vaep.py -v && uv run ruff check src/ingestion/spadl_vaep.py`
Expected: All PASS.

---

### Task 5: Extract `_vaep_output_schema` + Implement `_run_chunk`

**Files:**
- Modify: `src/ingestion/spadl_vaep.py` (extract schema, add `_run_chunk`, `_load_model_path_from_task_value`, `_read_cached_models`)

- [ ] **Step 1: Extract `_vaep_output_schema()` from `run_pipeline`**

Move the `StructType([...])` definition (lines 734–792 in `run_pipeline`) into a standalone function placed before `run_pipeline`:

```python
def _vaep_output_schema() -> StructType:
    """Return the output StructType for VAEP scored actions.

    Shared by run_pipeline (monolithic) and _run_chunk (for_each_task iteration).
    """
    from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

    # Copy ALL StructField entries from the current inline definition at
    # src/ingestion/spadl_vaep.py lines 734-791 (the StructType inside
    # run_pipeline's applyInPandas call). The list is ~60 fields starting
    # with game_id and ending with tackle_loser_team_key. Do NOT omit any
    # fields — the schema must be byte-identical to what run_pipeline uses
    # today so that _run_chunk writes are compatible with existing data.
    return StructType([
        StructField("game_id", LongType()),
        StructField("match_id", LongType()),
        StructField("original_event_id", StringType()),
        StructField("period_id", LongType()),
        # ... copy remaining ~56 StructField entries from lines 738-791 ...
        StructField("tackle_loser_team_id_native", StringType()),
        StructField("tackle_loser_team_key", LongType()),
    ])
```

Then in `run_pipeline`, replace the inline schema definition with:
```python
    vaep_schema = _vaep_output_schema()
```

- [ ] **Step 2: Implement `_run_chunk`**

Add before `main()`:

```python
def _load_model_path_from_task_value(spark: SparkSession, logger: logging.Logger) -> str:
    """Read vaep_model_path task value from preflight."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(spark)
        path = dbutils.jobs.taskValues.get(
            taskKey="preflight_spadl_vaep",
            key="vaep_model_path",
        )
        logger.info("Read model path from task value: %s", path)
        return str(path)
    except (ImportError, AttributeError, RuntimeError):
        raise SystemExit(
            "Cannot read vaep_model_path task value — "
            "are you running outside for_each_task? Use monolithic mode (no --match-ids)."
        )


def _read_cached_models(model_dir: str, logger: logging.Logger) -> tuple[bytes, bytes]:
    """Read cached XGBoost model bytes from UC Volume."""
    scores_path = f"{model_dir}/scores.xgb"
    concedes_path = f"{model_dir}/concedes.xgb"

    with open(scores_path, "rb") as f:
        scores_raw = f.read()
    with open(concedes_path, "rb") as f:
        concedes_raw = f.read()

    logger.info(
        "Loaded cached models: scores=%d bytes, concedes=%d bytes",
        len(scores_raw),
        len(concedes_raw),
    )
    return scores_raw, concedes_raw


def _run_chunk(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    provider: str,
    match_ids: list[int],
) -> None:
    """Execute one for_each_task iteration: convert (if needed) + score.

    Args:
        provider: One of the _VALID_CHUNK_PROVIDERS. "score" means skip conversion.
        match_ids: BIGINT match IDs to process.
    """
    spadl_table = f"{catalog}.{schema}.{_SPADL_TABLE}"

    from pyspark.sql import functions as spark_fn

    # Phase A: Convert (unless this is a score-only chunk)
    if provider != "score":
        existing_spadl_matches = _read_existing_match_ids(spark, catalog, schema, _SPADL_TABLE, logger)
        converters = {
            "statsbomb": _convert_statsbomb_from_bronze,
            "wyscout": _convert_wyscout_from_bronze,
            "idsse": _convert_idsse_from_bronze,
            "metrica": _convert_metrica_from_bronze,
            "skillcorner": _convert_skillcorner_from_bronze,
        }
        convert_fn = converters[provider]
        convert_fn(spark, catalog, schema, logger, existing_spadl_matches, match_id_filter=set(match_ids))
        logger.info("Phase A complete: converted %s chunk (%d match_ids)", provider, len(match_ids))

        # Post-conversion verification: ensure chunk match_ids landed in spadl_actions
        converted_count = (
            spark.table(spadl_table)
            .filter(spark_fn.col("match_id").isin(match_ids))
            .select("match_id")
            .distinct()
            .count()
        )
        if converted_count == 0:
            raise RuntimeError(
                f"Post-conversion check failed: 0 of {len(match_ids)} match_ids "
                f"found in {spadl_table} after {provider} conversion"
            )
        if converted_count < len(match_ids):
            logger.warning(
                "Partial conversion: %d/%d match_ids found in %s",
                converted_count,
                len(match_ids),
                spadl_table,
            )
        logger.info("Post-conversion: %d/%d match_ids in spadl_actions", converted_count, len(match_ids))

    # Phase B: Score via VAEP
    model_path = _load_model_path_from_task_value(spark, logger)
    scores_raw, concedes_raw = _read_cached_models(model_path, logger)

    unscored_sdf = spark.table(spadl_table).filter(spark_fn.col("match_id").isin(match_ids))

    vaep_schema = _vaep_output_schema()
    scoring_udf = _make_scoring_udf(scores_raw, concedes_raw)

    scored_sdf = unscored_sdf.groupBy("match_id", "data_source").applyInPandas(
        scoring_udf,  # type: ignore[arg-type]
        schema=vaep_schema,
    )

    ids_sql = ", ".join(str(mid) for mid in match_ids)
    write_delta_table(
        scored_sdf,
        catalog,
        schema,
        _VAEP_TABLE,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )

    logger.info("Chunk complete: provider=%s, %d matches scored", provider, len(match_ids))
```

- [ ] **Step 3: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/spadl_vaep.py && uv run pyright src/ingestion/spadl_vaep.py`
Expected: Clean.

---

### Task 6: Modify `main()` for Chunk Mode

**Files:**
- Modify: `src/ingestion/spadl_vaep.py:825-842` (`main()` function)
- Add test: `src/tests/test_spadl_vaep_preflight.py`

- [ ] **Step 1: Write test for monolithic mode dispatch**

Append to `src/tests/test_spadl_vaep_preflight.py`:

```python
from unittest.mock import MagicMock, patch
import sys


def test_main_without_match_ids_calls_run_pipeline(monkeypatch) -> None:
    """main() without --match-ids dispatches to run_pipeline (monolithic mode)."""
    monkeypatch.setattr(
        sys,
        "argv",
        ["compute_spadl_vaep", "--catalog", "soccer_analytics", "--schema", "bronze"],
    )
    with (
        patch("ingestion.spadl_vaep.get_spark_session") as mock_spark,
        patch("ingestion.spadl_vaep.timed_check") as mock_check,
        patch("ingestion.spadl_vaep.run_pipeline") as mock_run,
        patch("ingestion.spadl_vaep._run_chunk") as mock_chunk,
        patch("ingestion.spadl_vaep.configure_logging") as mock_log,
        patch("ingestion.bootstrap.bootstrap_hooks"),
    ):
        mock_spark.return_value = MagicMock()
        mock_log.return_value = MagicMock()
        mock_check.return_value = MagicMock(count=0, metadata={})

        from ingestion.spadl_vaep import main

        main()

        # Monolithic path: run_pipeline called, NOT _run_chunk
        mock_run.assert_called_once()
        mock_chunk.assert_not_called()


def test_main_with_match_ids_calls_run_chunk(monkeypatch) -> None:
    """main() with --match-ids dispatches to _run_chunk (chunk mode)."""
    monkeypatch.setattr(
        sys,
        "argv",
        ["compute_spadl_vaep", "--catalog", "soccer_analytics", "--schema", "bronze",
         "--match-ids", "statsbomb:100,200,300"],
    )
    with (
        patch("ingestion.spadl_vaep.get_spark_session") as mock_spark,
        patch("ingestion.spadl_vaep._run_chunk") as mock_chunk,
        patch("ingestion.spadl_vaep.run_pipeline") as mock_run,
        patch("ingestion.spadl_vaep.configure_logging") as mock_log,
        patch("ingestion.bootstrap.bootstrap_hooks"),
    ):
        mock_spark.return_value = MagicMock()
        mock_log.return_value = MagicMock()

        from ingestion.spadl_vaep import main

        main()

        # Chunk mode: _run_chunk called with parsed provider + ids
        mock_chunk.assert_called_once_with(
            mock_spark.return_value,
            "soccer_analytics",
            "bronze",
            mock_log.return_value,
            "statsbomb",
            [100, 200, 300],
        )
        mock_run.assert_not_called()
```

- [ ] **Step 2: Rewrite `main()` to accept `--match-ids`**

Replace `main()` (lines 825–842):

```python
def main() -> None:
    """CLI entry point for SPADL conversion and VAEP action valuation.

    Two modes:
    - **Chunk mode** (``--match-ids "provider:id1,id2"``): processes one chunk
      from the for_each_task fan-out. Reads model from UC Volume task value.
    - **Monolithic mode** (no ``--match-ids``): runs the full pipeline for all
      providers. Preserved for local development and debugging.
    """
    args = parse_ingestion_args(
        "Compute SPADL actions and VAEP scores",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2 from for_each_task"})],
    )
    logger = configure_logging("spadl_vaep")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    parsed = _parse_vaep_match_ids_arg(getattr(args, "match_ids", None))

    if parsed is None:
        # Monolithic mode (backward compat / local dev)
        logger.warning(
            "Running in monolithic mode (no --match-ids). "
            "Production uses preflight_spadl_vaep + for_each_task."
        )
        filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)
        logger.info("Starting SPADL/VAEP pipeline into %s.%s", args.catalog, args.schema)
        run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
    else:
        # Chunk mode (for_each_task iteration)
        provider, match_ids = parsed
        logger.info("Chunk mode: provider=%s, %d match_ids", provider, len(match_ids))
        _run_chunk(spark, args.catalog, args.schema, logger, provider, match_ids)
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py -v -k "main_with" && uv run pytest src/tests/test_spadl_vaep_preflight.py -v -k "main_without"`
Expected: All PASS.

---

### Task 7: Model Cache Round-Trip Test

**Files:**
- Modify: `src/tests/test_spadl_vaep_preflight.py`

- [ ] **Step 1: Write model cache round-trip test**

Append to `src/tests/test_spadl_vaep_preflight.py`:

```python
import tempfile
import os

import numpy as np
from xgboost import XGBClassifier


def test_model_cache_round_trip() -> None:
    """Serialize XGBoost model -> write to path -> read -> predict -> same output."""
    rng = np.random.default_rng(42)
    X = rng.random((100, 5))
    y = (X[:, 0] > 0.5).astype(int)
    model = XGBClassifier(n_estimators=3, max_depth=2, use_label_encoder=False, eval_metric="logloss")
    model.fit(X, y)

    # Serialize (same as preflight does)
    raw_bytes = bytes(model.get_booster().save_raw("json"))

    # Write to temp path (simulates UC Volume write)
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "test_model.xgb")
        with open(model_path, "wb") as f:
            f.write(raw_bytes)

        # Read back (simulates iteration read)
        with open(model_path, "rb") as f:
            loaded_bytes = f.read()

    # Deserialize (same as scoring UDF does)
    loaded_model = XGBClassifier()
    loaded_model.load_model(bytearray(loaded_bytes))

    # Predict on known input — must match original
    X_test = rng.random((10, 5))
    original_preds = model.predict_proba(X_test)
    loaded_preds = loaded_model.predict_proba(X_test)

    np.testing.assert_array_almost_equal(original_preds, loaded_preds)
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest src/tests/test_spadl_vaep_preflight.py::test_model_cache_round_trip -v`
Expected: PASS.

---

### Task 8: Terraform Job Definition

**Files:**
- Modify: `terraform/modules/workflows/main.tf:470-508`

- [ ] **Step 1: Replace monolithic `compute_spadl_vaep` with preflight + for_each_task**

Replace lines 470–508 with:

```hcl
  # ── Task: Preflight SPADL/VAEP (guard + chunk emission + model cache) ────
  # Discovers new matches per provider, caches VAEP models to UC Volume,
  # emits chunk strings as task values for downstream for_each_task.
  task {
    task_key        = "preflight_spadl_vaep"
    timeout_seconds = 300
    max_retries     = 0

    # Same dependencies as the old monolithic compute_spadl_vaep.
    # Order: alphabetical (test_workflows_tf_ordering enforcement).
    depends_on {
      task_key = "backfill_statsbomb_extra"
    }
    depends_on {
      task_key = "ingest_idsse_events"
    }
    depends_on {
      task_key = "ingest_metrica"
    }
    depends_on {
      task_key = "ingest_skillcorner"
    }
    depends_on {
      task_key = "ingest_wyscout"
    }

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_spadl_vaep"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "analytics"
  }

  # ── Task: Compute SPADL actions and VAEP scores (for_each_task fan-out) ──
  # Each iteration processes one chunk: converts a single provider's matches
  # to SPADL (scoped to chunk match_ids only), then scores with VAEP.
  # Idempotent via replaceWhere.
  task {
    task_key        = "compute_spadl_vaep"
    timeout_seconds = 0

    depends_on {
      task_key = "preflight_spadl_vaep"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_spadl_vaep.values.spadl_vaep_chunks}}"
      concurrency = 4

      task {
        task_key        = "compute_spadl_vaep_iteration"
        timeout_seconds = 1800
        max_retries     = 0

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "compute_spadl_vaep"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-ids", "{{input}}"
          ]
        }

        environment_key = "analytics"
      }
    }
  }
```

- [ ] **Step 2: Verify downstream `depends_on` references**

All tasks that currently depend on `compute_spadl_vaep` (lines 138, 242, 636, 738, 969) reference the task_key `"compute_spadl_vaep"` — this is unchanged (the for_each_task wrapper keeps the same task_key). No downstream changes needed.

- [ ] **Step 3: Run `terraform validate`**

Run: `cd terraform/environments/dev && terraform validate`
Expected: Success.

---

### Task 9: pyproject.toml Entry Point + Wheel Bump

**Files:**
- Modify: `pyproject.toml` (entry point only — version handled by script)
- Multiple files bumped by `scripts/bump_wheel.py`

- [ ] **Step 1: Add `preflight_spadl_vaep` entry point**

In `pyproject.toml` under `[project.scripts]`, add after `compute_spadl_vaep` (line 109):

```toml
preflight_spadl_vaep = "ingestion.spadl_vaep:main_preflight"
```

- [ ] **Step 2: Bump wheel version**

Run: `uv run python scripts/bump_wheel.py`

This handles pyproject.toml version, `src/shared/wheel.py`, all PEP 723 scripts, `scripts/deploy.sh`, and both Terraform environment files.

- [ ] **Step 3: Verify wheel builds**

Run: `uv run python -m build --wheel -o dist/ 2>&1 | tail -5`
Expected: Wheel builds successfully with new entry point.

---

### Task 10: Integration Verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest src/tests/test_spadl_vaep.py src/tests/test_spadl_vaep_preflight.py src/tests/test_guard_conformance.py -v`
Expected: All PASS.

- [ ] **Step 2: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/spadl_vaep.py src/ingestion/spadl_conversion.py src/tests/test_spadl_vaep_preflight.py && uv run ruff format --check src/ingestion/spadl_vaep.py src/ingestion/spadl_conversion.py src/tests/test_spadl_vaep_preflight.py && uv run pyright src/ingestion/spadl_vaep.py src/ingestion/spadl_conversion.py`
Expected: All clean.

- [ ] **Step 3: Verify terraform validates**

Run: `cd terraform/environments/dev && terraform validate`
Expected: Success.

- [ ] **Step 4: Commit**

```bash
git add src/ingestion/spadl_vaep.py src/ingestion/spadl_conversion.py src/tests/test_spadl_vaep_preflight.py src/tests/test_spadl_vaep.py terraform/modules/workflows/main.tf pyproject.toml
uv run python scripts/bump_wheel.py  # if not already done
git add -u  # pick up all bump_wheel changes
git commit -m "$(cat <<'EOF'
feat(spadl-vaep): chunked execution via preflight + for_each_task

Refactor compute_spadl_vaep from a monolithic 30-min-timeout task into
the preflight + for_each_task pattern (same as tracking context, ADR-024):

- Guard emits per-provider metadata keys (sb_new, ws_new, etc.)
- Preflight builds chunk strings, caches VAEP models to UC Volume
- Each iteration converts one provider's matches + scores with VAEP
- Converters gain match_id_filter param to scope to chunk IDs only
- Monolithic mode preserved for local dev (no --match-ids)
- Terraform: preflight (5 min) + for_each_task (4 concurrent, 30 min/iter)

Closes timeout issue on large backlogs (>200 matches).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```
