# TC-1b: applyInPandas Tracking Context — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace driver-bound `.toPandas()` in `compute_tracking_context` with `applyInPandas` so tracking data stays on executors and IDSSE matches no longer OOM on Databricks serverless (16 GB driver).

**Architecture:** Three layers: preflight fits xT once and serializes as task value; `for_each_task` fan-out (unchanged from TC-1a) spawns one iteration per chunk; each iteration uses `groupBy("match_id", "period").applyInPandas(udf, schema)` to dispatch the full enrichment pipeline to executors. Driver handles only actions (small) and match-level metadata (scalars). Legacy `run_pipeline()` and standalone mode removed.

**Tech Stack:** PySpark `applyInPandas`, silly-kicks tracking enrichments, Databricks `for_each_task`, numpy/base64 for xT serialization.

**Spec:** `docs/superpowers/specs/2026-05-12-tc1b-applyinpandas-design.md`

---

## File Structure

| File | Responsibility | Change |
|------|---------------|--------|
| `src/ingestion/tracking_context.py` | Main module: UDF factory, result schema, preflight xT, orchestration | Modify |
| `src/tests/test_tracking_context_udf.py` | Unit tests for xT round-trip, actions round-trip, closure pickling, UDF schema | Create |
| `src/tests/test_tracking_context_preflight.py` | Preflight chunking + xT task value tests | Modify |

No changes to Terraform, pyproject.toml, seeds, workflow cards, or wheel version.

---

### Task 1: Add `_RESULT_SCHEMA` StructType constant

**Files:**
- Modify: `src/ingestion/tracking_context.py:190-233`
- Test: `src/tests/test_tracking_context_udf.py` (new file)

The existing `_TRACKING_CONTEXT_DDL` string (line 190) already defines all 83 columns with Spark SQL types. We build a `_RESULT_SCHEMA` StructType from this DDL string at module level, excluding `_ingested_at` (added by `write_delta_table`).

- [ ] **Step 1: Write the failing test**

Create `src/tests/test_tracking_context_udf.py`:

```python
"""Tests for tracking context applyInPandas UDF components."""

from __future__ import annotations


def test_result_schema_matches_result_columns() -> None:
    """_RESULT_SCHEMA field names match _RESULT_COLUMNS (minus _ingested_at)."""
    from ingestion.tracking_context import _RESULT_COLUMNS, _RESULT_SCHEMA

    expected = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
    actual = [f.name for f in _RESULT_SCHEMA.fields]
    assert actual == expected, f"Schema mismatch:\n  expected={expected}\n  actual={actual}"


def test_result_schema_field_count() -> None:
    """_RESULT_SCHEMA has 82 fields (83 columns minus _ingested_at)."""
    from ingestion.tracking_context import _RESULT_SCHEMA

    assert len(_RESULT_SCHEMA.fields) == 82, f"Expected 82 fields, got {len(_RESULT_SCHEMA.fields)}"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v -x`
Expected: FAIL with `ImportError` or `AttributeError` — `_RESULT_SCHEMA` does not exist yet.

- [ ] **Step 3: Implement `_RESULT_SCHEMA`**

In `src/ingestion/tracking_context.py`, add after the `_TRACKING_CONTEXT_DDL` constant (after line 233). The DDL string is already a valid Spark SQL column list — use `_parse_ddl_to_struct_type` to build the StructType:

```python
def _parse_ddl_to_struct_type(ddl: str) -> StructType:
    """Parse a Spark DDL column-list string into a StructType.

    Handles: STRING, BIGINT, DOUBLE, BOOLEAN, TIMESTAMP.
    Excludes _ingested_at (added by write_delta_table, not by the UDF).
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    _TYPE_MAP: dict[str, object] = {
        "STRING": StringType(),
        "BIGINT": LongType(),
        "DOUBLE": DoubleType(),
        "BOOLEAN": BooleanType(),
        "TIMESTAMP": TimestampType(),
    }
    fields: list[StructField] = []
    for token in ddl.split(","):
        token = token.strip()
        if not token:
            continue
        parts = token.split()
        if len(parts) != 2:
            continue
        col_name, col_type = parts[0], parts[1].upper()
        if col_name == "_ingested_at":
            continue
        spark_type = _TYPE_MAP.get(col_type)
        if spark_type is None:
            msg = f"Unknown Spark type {col_type!r} for column {col_name!r}"
            raise ValueError(msg)
        fields.append(StructField(col_name, spark_type, nullable=True))
    return StructType(fields)


_RESULT_SCHEMA: StructType = _parse_ddl_to_struct_type(_TRACKING_CONTEXT_DDL)
```

Add the `StructType` import to the `TYPE_CHECKING` block at the top of the file (line 23-26):

```python
if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    from silly_kicks.xthreat import ExpectedThreat
```

Wait — `_RESULT_SCHEMA` is used at runtime (passed to `applyInPandas`), not just for type checking. The `StructType` import inside `_parse_ddl_to_struct_type` handles the runtime import. The `TYPE_CHECKING` annotation is only for the type hint on the module-level variable. Actually, since `_parse_ddl_to_struct_type` returns `StructType` and the module-level assignment happens at import time, we need the lazy import INSIDE the function (which it already has). The type annotation on the module variable should use a string literal:

```python
_RESULT_SCHEMA: "StructType" = _parse_ddl_to_struct_type(_TRACKING_CONTEXT_DDL)
```

No — simpler: just don't annotate the module variable (the function has a return type). Drop the annotation entirely:

```python
_RESULT_SCHEMA = _parse_ddl_to_struct_type(_TRACKING_CONTEXT_DDL)
```

This avoids any import issues. The function's lazy import handles everything.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v -x`
Expected: PASS — both tests green.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py && uv run ruff format --check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py`
Expected: Clean.

Run: `uv run pyright src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py`
Expected: Clean (or only pre-existing warnings).

---

### Task 2: Add xT serialization and round-trip test

**Files:**
- Modify: `src/ingestion/tracking_context.py` (add `_serialize_xt_grid` and `_deserialize_xt_grid` helpers)
- Modify: `src/tests/test_tracking_context_udf.py` (add xT round-trip test)

- [ ] **Step 1: Write the failing test**

Add to `src/tests/test_tracking_context_udf.py`:

```python
def test_xt_grid_round_trip() -> None:
    """Serialize xT grid via tolist(), reconstruct, assert array equality."""
    import numpy as np

    from ingestion.tracking_context import _deserialize_xt_grid, _serialize_xt_grid

    # Synthetic 12x16 grid (same shape as ExpectedThreat default)
    original = np.random.default_rng(42).random((12, 16))
    serialized = _serialize_xt_grid(original, l=16, w=12)

    assert isinstance(serialized, dict)
    assert "xt_grid" in serialized
    assert "l" in serialized
    assert "w" in serialized
    assert serialized["l"] == 16
    assert serialized["w"] == 12
    assert isinstance(serialized["xt_grid"], list)
    assert len(serialized["xt_grid"]) == 12
    assert len(serialized["xt_grid"][0]) == 16

    reconstructed = _deserialize_xt_grid(serialized)
    np.testing.assert_array_equal(reconstructed, original)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_udf.py::test_xt_grid_round_trip -v -x`
Expected: FAIL — `_serialize_xt_grid` does not exist.

- [ ] **Step 3: Implement serialization helpers**

Add to `src/ingestion/tracking_context.py`, after the `_RESULT_SCHEMA` definition:

```python
def _serialize_xt_grid(xt_array: np.ndarray, *, l: int, w: int) -> dict[str, object]:
    """Serialize an ExpectedThreat grid as JSON-safe scalar primitives.

    Follows the established off_ball_xt.py:121 pattern — .tolist() for
    ndarray serialization, no pickle, no base64.

    Only grid + dimensions are needed: ExpectedThreat.rate() and
    interpolator() read only .xT, .l, .w (verified in silly_kicks/xthreat.py
    lines 343-468).
    """
    return {"xt_grid": xt_array.tolist(), "l": l, "w": w}


def _deserialize_xt_grid(data: dict[str, object]) -> np.ndarray:
    """Reconstruct xT grid from serialized scalar primitives."""
    return np.array(data["xt_grid"], dtype=np.float64)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v -x`
Expected: All 3 tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py && uv run pyright src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py`
Expected: Clean.

---

### Task 3: Add actions round-trip test

**Files:**
- Modify: `src/tests/test_tracking_context_udf.py`

No implementation change needed — `to_dict("records")` and `pd.DataFrame(records)` are stdlib pandas. This test locks in the contract that the round-trip is lossless for the column types present in SPADL actions.

- [ ] **Step 1: Write the test**

Add to `src/tests/test_tracking_context_udf.py`:

```python
def test_actions_records_round_trip() -> None:
    """Actions DataFrame survives to_dict('records') → pd.DataFrame round-trip."""
    import pandas as pd

    original = pd.DataFrame(
        {
            "game_id": [1, 1, 1],
            "action_id": [0, 1, 2],
            "period_id": [1, 1, 2],
            "time_seconds": [10.5, 25.3, 0.1],
            "team_id": ["T1", "T2", "T1"],
            "player_id": ["P1", "P2", "P3"],
            "type_id": [0, 1, 0],
            "result_id": [1, 0, 1],
            "bodypart_id": [0, 0, 1],
            "start_x": [50.0, 30.0, 52.5],
            "start_y": [34.0, 20.0, 34.0],
            "end_x": [60.0, 40.0, 55.0],
            "end_y": [34.0, 25.0, 30.0],
        }
    )
    records = original.to_dict("records")
    reconstructed = pd.DataFrame(records)
    pd.testing.assert_frame_equal(reconstructed, original)
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_udf.py::test_actions_records_round_trip -v -x`
Expected: PASS (this is a baseline correctness test, not TDD red-green).

---

### Task 4: Add UDF factory and closure pickling test

**Files:**
- Modify: `src/ingestion/tracking_context.py` (add `_make_tracking_context_udf`)
- Modify: `src/tests/test_tracking_context_udf.py` (add closure pickling + output schema tests)

- [ ] **Step 1: Write the failing tests**

Add to `src/tests/test_tracking_context_udf.py`:

```python
def test_udf_closure_pickling() -> None:
    """UDF closure built by _make_tracking_context_udf survives pickle round-trip."""
    import pickle

    from ingestion.tracking_context import _make_tracking_context_udf

    udf_fn = _make_tracking_context_udf(
        provider="metrica",
        home_team_id="Home",
        home_start_left=True,
        xt_grid_data=[[0.0] * 16 for _ in range(12)],
        xt_l=16,
        xt_w=12,
        actions_records=[
            {
                "game_id": 1,
                "action_id": 0,
                "period_id": 1,
                "time_seconds": 10.0,
                "team_id": "Home",
                "player_id": "P1",
                "type_id": 0,
                "result_id": 1,
                "bodypart_id": 0,
                "start_x": 50.0,
                "start_y": 34.0,
                "end_x": 60.0,
                "end_y": 34.0,
            }
        ],
        native_match_id="test_match",
    )
    assert callable(udf_fn)

    # Pickle round-trip (simulates Spark executor serialization)
    data = pickle.dumps(udf_fn)
    restored = pickle.loads(data)  # noqa: S301
    assert callable(restored)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_tracking_context_udf.py::test_udf_closure_pickling -v -x`
Expected: FAIL — `_make_tracking_context_udf` does not exist.

- [ ] **Step 3: Implement `_make_tracking_context_udf`**

Add to `src/ingestion/tracking_context.py`, after the `_deserialize_xt_grid` function. This is the core of TC-1b:

```python
def _make_tracking_context_udf(
    provider: str,
    home_team_id: str,
    home_start_left: bool,
    xt_grid_data: list[list[float]],
    xt_l: int,
    xt_w: int,
    actions_records: list[dict[str, object]],
    native_match_id: str,
) -> object:
    """Build the applyInPandas UDF closure for tracking context enrichment.

    All arguments are Python scalar primitives — no ndarray, no DataFrame,
    no pickle. Follows the established off_ball_xt.py:102-143 pattern.

    The closure captures these as Python locals. Spark pickles the closure,
    but only primitives travel — no arbitrary object deserialization.

    Returns:
        A callable (pd.DataFrame) -> pd.DataFrame for applyInPandas.
    """

    def _udf(pdf: "pd.DataFrame") -> "pd.DataFrame":
        # Lazy imports — executors have the wheel installed but no internet
        import gc as _gc
        import logging as _logging

        import numpy as _np
        import pandas as _pd

        from silly_kicks.xthreat import ExpectedThreat as _ExpectedThreat

        _logger = _logging.getLogger("tracking_context_udf")

        if pdf.empty:
            # Return empty DataFrame with correct schema
            from ingestion.tracking_context import _RESULT_COLUMNS

            output_cols = [c for c in _RESULT_COLUMNS if c != "_ingested_at"]
            return _pd.DataFrame(columns=_pd.Index(output_cols))

        match_id_val = pdf["match_id"].iloc[0]
        period_val = pdf["period"].iloc[0]

        # Row-count guardrail (observability, not a hard gate)
        if len(pdf) > 2_000_000:
            _logger.warning(
                "Large UDF group: match_id=%s, period=%s, rows=%d (>2M)",
                match_id_val,
                period_val,
                len(pdf),
            )

        try:
            # Reconstruct xT from scalar primitives
            xt = _ExpectedThreat(l=xt_l, w=xt_w)
            xt.xT = _np.array(xt_grid_data, dtype=_np.float64)

            # Reconstruct actions from records
            actions = _pd.DataFrame(actions_records)

            # Provider-specific conversion (tracking → silly-kicks frames)
            if provider == "idsse":
                from silly_kicks.tracking import PreprocessConfig as _PreprocessConfig
                from silly_kicks.tracking.sportec import convert_to_frames as _convert_to_frames

                from ingestion.tracking_context import _bronze_idsse_to_sportec_input

                sportec_input = _bronze_idsse_to_sportec_input(pdf)
                del pdf
                _gc.collect()

                frames, _report = _convert_to_frames(
                    sportec_input,
                    home_team_id=home_team_id,
                    home_team_start_left=home_start_left,
                    output_convention="ltr",
                    preprocess=_PreprocessConfig(derive_velocity=True),
                )
                del sportec_input
                _gc.collect()

            elif provider == "metrica":
                from ingestion.tracking_context import _bronze_metrica_to_frames

                game_id = int(actions["game_id"].iloc[0])
                frames = _bronze_metrica_to_frames(pdf, game_id=game_id)
                del pdf
                _gc.collect()

            elif provider == "skillcorner":
                from ingestion.tracking_context import _bronze_skillcorner_to_frames

                game_id = int(actions["game_id"].iloc[0])
                frames = _bronze_skillcorner_to_frames(pdf, game_id=game_id)
                del pdf
                _gc.collect()

            else:
                raise ValueError(f"Unknown provider: {provider}")

            # Align game_id: converter may use native ID, but SPADL uses BIGINT hash
            frames["game_id"] = int(actions["game_id"].iloc[0])

            # Run full enrichment chain
            from ingestion.tracking_context import _enrich_match

            result = _enrich_match(
                actions=actions,
                frames=frames,
                xt=xt,
                home_team_id=home_team_id,
                match_id_native=native_match_id,
                data_source=provider,
            )
            del frames, actions
            _gc.collect()

            return result

        except Exception as exc:
            raise RuntimeError(
                f"tracking_context UDF failed for match_id={match_id_val}, period={period_val}"
            ) from exc

    return _udf
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_udf.py -v -x`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py && uv run ruff format --check src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py && uv run pyright src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py`
Expected: Clean. Fix any ruff format issues.

---

### Task 5: Extend `main_preflight` to serialize xT as a task value

**Files:**
- Modify: `src/ingestion/tracking_context.py:1262-1291` (`main_preflight`)
- Modify: `src/tests/test_tracking_context_preflight.py` (add xT task value structure test)

- [ ] **Step 1: Write the failing test**

Add to `src/tests/test_tracking_context_preflight.py`:

```python
def test_serialize_xt_grid_produces_valid_task_value() -> None:
    """_serialize_xt_grid output is JSON-serializable with expected keys."""
    import json

    import numpy as np

    from ingestion.tracking_context import _serialize_xt_grid

    grid = np.ones((12, 16), dtype=np.float64)
    data = _serialize_xt_grid(grid, l=16, w=12)

    # Must be JSON-serializable (Databricks task values are JSON)
    json_str = json.dumps(data)
    restored = json.loads(json_str)

    assert restored["l"] == 16
    assert restored["w"] == 12
    assert len(restored["xt_grid"]) == 12
    assert len(restored["xt_grid"][0]) == 16
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest src/tests/test_tracking_context_preflight.py::test_serialize_xt_grid_produces_valid_task_value -v -x`
Expected: PASS (the serialization helper already exists from Task 2).

- [ ] **Step 3: Update `main_preflight` to fit and serialize xT**

Modify `main_preflight` in `src/ingestion/tracking_context.py`. After the chunk task value write (line 1291), add xT fitting and serialization:

Replace the current `main_preflight` function (lines 1262-1291) with:

```python
def main_preflight() -> None:
    """CLI entry point for the tracking context preflight task.

    Runs the skip guard, partitions discovered matches into fan-out chunks
    (``provider:id1,id2`` format), fits xT once, and writes both as
    Databricks task values for downstream ``compute_tracking_context``
    ``for_each_task`` iterations.
    """
    args = parse_ingestion_args(
        "Preflight: discover unprocessed tracking matches and emit chunks "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    logger = configure_logging("tracking_context_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    # Serialize each chunk as a single string (inner list is always length 1)
    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    logger.info(
        "Tracking context preflight: %d missing matches across %d chunks",
        fr.count,
        len(chunks_for_inputs),
    )

    _write_tracking_chunks_task_value(chunks_for_inputs, logger)

    # Fit xT model once and serialize for all iterations (deterministic grid)
    if fr.count > 0:
        from pyspark.sql import functions as F  # noqa: N812
        from silly_kicks.xthreat import ExpectedThreat

        spadl_pdf = (
            spark.table(f"{args.catalog}.bronze.spadl_actions")
            .filter(F.col("data_source").isin("idsse", "metrica", "skillcorner"))
            .select(
                "game_id",
                "action_id",
                "period_id",
                "time_seconds",
                "team_id",
                "player_id",
                "type_id",
                "result_id",
                "bodypart_id",
                "start_x",
                "start_y",
                "end_x",
                "end_y",
                "original_event_id",
            )
            .toPandas()
        )
        xt = ExpectedThreat().fit(spadl_pdf)
        del spadl_pdf
        logger.info("xT model fitted (grid shape %s)", xt.xT.shape)

        xt_data = _serialize_xt_grid(xt.xT, l=xt.l, w=xt.w)

        try:
            from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

            dbutils = DBUtils(spark)
            dbutils.jobs.taskValues.set(key="tracking_context_xt", value=xt_data)
            logger.info("Wrote task value 'tracking_context_xt'")
        except (ImportError, AttributeError, RuntimeError) as exc:
            logger.warning("Task values not available (likely standalone mode) -- %s", exc)
```

- [ ] **Step 4: Run all preflight tests**

Run: `uv run pytest src/tests/test_tracking_context_preflight.py -v -x`
Expected: All 9 tests PASS.

- [ ] **Step 5: Run ruff + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py && uv run pyright src/ingestion/tracking_context.py`
Expected: Clean.

---

### Task 6: Refactor `main()` — remove legacy mode, use applyInPandas

**Files:**
- Modify: `src/ingestion/tracking_context.py:1294-1367` (`main`)

This is the core orchestration change. `main()` becomes iteration-only (no standalone mode). It deserializes the preflight xT, reads match-level metadata on the driver, and dispatches via `applyInPandas`.

- [ ] **Step 1: Replace `main()` with applyInPandas orchestration**

Replace the entire `main` function (lines 1294-1367) with:

```python
def main() -> None:
    """CLI entry point for tracking context enrichment (for_each_task iteration).

    Reads ``--match-ids "provider:id1,id2"`` from the for_each_task input.
    Deserializes the preflight xT grid. For each match, resolves match-level
    metadata on the driver, then dispatches the full enrichment pipeline to
    executors via ``groupBy("match_id", "period").applyInPandas(...)``.
    """
    import json

    args = parse_ingestion_args(
        "Compute action-coupled tracking features",
        extra_args=[("--match-ids", {"type": str, "default": None, "help": "provider:id1,id2 from for_each_task"})],
    )
    logger = configure_logging("tracking_context")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids_parsed = _parse_tracking_match_ids_arg(getattr(args, "match_ids", None))
    if match_ids_parsed is None:
        raise SystemExit("--match-ids is required (for_each_task iteration mode only)")

    provider, ids = match_ids_parsed
    logger.info("Iteration mode: provider=%s, match_ids=%s", provider, ids)

    # Deserialize preflight xT from task value
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

        dbutils = DBUtils(spark)
        xt_raw = dbutils.jobs.taskValues.get(
            taskKey="preflight_tracking_context",
            key="tracking_context_xt",
        )
        if isinstance(xt_raw, str):
            xt_data = json.loads(xt_raw)
        else:
            xt_data = xt_raw
        xt_grid_data: list[list[float]] = xt_data["xt_grid"]
        xt_l: int = int(xt_data["l"])
        xt_w: int = int(xt_data["w"])
        logger.info("Deserialized preflight xT grid (%dx%d)", xt_w, xt_l)
    except (ImportError, AttributeError, RuntimeError):
        # Standalone fallback: fit xT locally
        logger.warning("Task values not available — fitting xT locally")
        from pyspark.sql import functions as F  # noqa: N812
        from silly_kicks.xthreat import ExpectedThreat

        spadl_pdf = (
            spark.table(f"{args.catalog}.bronze.spadl_actions")
            .filter(F.col("data_source").isin("idsse", "metrica", "skillcorner"))
            .select(
                "game_id", "action_id", "period_id", "time_seconds",
                "team_id", "player_id", "type_id", "result_id",
                "bodypart_id", "start_x", "start_y", "end_x", "end_y",
                "original_event_id",
            )
            .toPandas()
        )
        xt = ExpectedThreat().fit(spadl_pdf)
        del spadl_pdf
        xt_serialized = _serialize_xt_grid(xt.xT, l=xt.l, w=xt.w)
        xt_grid_data = xt_serialized["xt_grid"]
        xt_l = xt_serialized["l"]
        xt_w = xt_serialized["w"]

    from pyspark.sql import functions as F  # noqa: N812

    from ingestion.utils import write_delta_table

    catalog, schema = args.catalog, args.schema
    total_written = 0

    for match_id in ids:
        logger.info("Processing %s match %s", provider, match_id)

        # ── Read tracking (stays as Spark DataFrame — NO .toPandas()) ──
        if provider == "idsse":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.idsse_tracking")
                .filter(F.col("match_id") == match_id)
                .select(*_IDSSE_TRACKING_SELECT_COLS)
            )
        elif provider == "metrica":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.metrica_tracking")
                .filter(F.col("match_id") == match_id)
                .select(*_METRICA_TRACKING_SELECT_COLS)
            )
        elif provider == "skillcorner":
            trk_sdf = (
                spark.table(f"{catalog}.bronze.skillcorner_tracking")
                .filter(F.col("match_id") == match_id)
                .select(*_SKILLCORNER_TRACKING_SELECT_COLS)
            )
        else:
            raise SystemExit(f"Unknown provider: {provider}")

        # Quick existence check (count on Spark, not .toPandas())
        if trk_sdf.limit(1).count() == 0:
            logger.warning("No tracking data for %s match %s", provider, match_id)
            continue

        # ── Read actions (small — hundreds of rows, safe to .toPandas()) ──
        actions_pdf = (
            spark.table(f"{catalog}.bronze.spadl_actions")
            .filter((F.col("match_id_native") == match_id) & (F.col("data_source") == provider))
            .toPandas()
        )
        if actions_pdf.empty:
            logger.warning("No SPADL actions for %s match %s", provider, match_id)
            continue
        actions_records = actions_pdf.to_dict("records")

        # ── Resolve match-level metadata on driver (scalars) ──
        if provider == "idsse":
            from ingestion.spadl_adapter import (
                adapt_idsse_events_for_silly_kicks,
                derive_idsse_home_team_start_left,
            )

            events_pdf = (
                spark.table(f"{catalog}.bronze.idsse_events")
                .filter(F.col("match_id") == match_id)
                .toPandas()
            )
            home_team_id = str(events_pdf["home_team_id_native"].dropna().iloc[0])
            adapted_events = adapt_idsse_events_for_silly_kicks(events_pdf)
            home_start_left = derive_idsse_home_team_start_left(adapted_events, home_team_id)
            del events_pdf, adapted_events
        elif provider == "metrica":
            home_team_id = "Home"
            home_start_left = True  # Metrica converter doesn't use this
        elif provider == "skillcorner":
            # One-row Spark query — driver never sees full tracking data
            row = (
                spark.table(f"{catalog}.bronze.skillcorner_tracking")
                .filter(F.col("match_id") == match_id)
                .select("home_team_id")
                .limit(1)
                .collect()[0]
            )
            home_team_id = str(row["home_team_id"])
            home_start_left = True  # SkillCorner converter doesn't use this

        # ── Build UDF and dispatch via applyInPandas ──
        udf_fn = _make_tracking_context_udf(
            provider=provider,
            home_team_id=home_team_id,
            home_start_left=home_start_left,
            xt_grid_data=xt_grid_data,
            xt_l=xt_l,
            xt_w=xt_w,
            actions_records=actions_records,
            native_match_id=match_id,
        )

        result_sdf = trk_sdf.groupBy("match_id", "period").applyInPandas(udf_fn, schema=_RESULT_SCHEMA)

        written = write_delta_table(
            result_sdf,
            catalog,
            schema,
            _TABLE_NAME,
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
        )
        total_written += written
        del actions_pdf, actions_records

    logger.info("Iteration complete -- %d rows written for %s", total_written, provider)
```

- [ ] **Step 2: Remove `run_pipeline()` and dead imports**

Delete the `run_pipeline` function (lines 1162-1228 — the `@workflow`-decorated function) entirely.

Remove unused imports at the top of the file that were only used by `run_pipeline()`:
- Line 20: `from workflows import workflow` — remove
- Line 21: `from workflows.exceptions import WorkflowSkippedError` — remove

- [ ] **Step 3: Delete `_process_idsse`, `_process_metrica`, `_process_skillcorner`**

Delete these three functions (lines 368-1058). They are fully replaced by the applyInPandas UDF path. The converter helpers they called (`_bronze_idsse_to_sportec_input`, `_bronze_metrica_to_frames`, `_bronze_skillcorner_to_frames`) are KEPT — the UDF imports and calls them.

Also delete the `_derive_velocities_savgol` function ONLY if it is called exclusively from the `_process_*` functions and the converter helpers. Check: `_derive_velocities_savgol` is called from `_bronze_metrica_to_frames` (line 814) and `_bronze_skillcorner_to_frames` (line 916) — so it MUST BE KEPT.

**Summary of what to delete:**
- `_process_idsse` (lines 368-462)
- `_process_metrica` (lines 920-987)
- `_process_skillcorner` (lines 990-1058)
- `run_pipeline` (lines 1162-1228)

**Summary of what to keep:**
- `_enrich_match` (lines 239-362)
- `_derive_velocities_savgol` (lines 468-552)
- `_bronze_idsse_to_sportec_input` (lines 578-694)
- `_bronze_metrica_to_frames` (lines 713-815)
- `_bronze_skillcorner_to_frames` (lines 840-917)
- All consumed/projection constants
- `_TrackingContextGuard`, `skip_guard`, `_parse_tracking_match_ids_arg`
- `_write_tracking_chunks_task_value`, `main_preflight`, `main`

- [ ] **Step 4: Run ruff format**

Run: `uv run ruff format src/ingestion/tracking_context.py`

- [ ] **Step 5: Run ruff check + pyright**

Run: `uv run ruff check src/ingestion/tracking_context.py && uv run pyright src/ingestion/tracking_context.py`
Expected: Clean. If ruff reports unused imports for `workflow` or `WorkflowSkippedError`, confirm they were removed in Step 2.

- [ ] **Step 6: Run all existing tests**

Run: `uv run pytest src/tests/test_tracking_context_udf.py src/tests/test_tracking_context_preflight.py src/tests/test_tracking_context_column_projection.py -v`
Expected: All tests PASS. The column projection tests verify that the projection and consumed constants still exist and are consistent. The preflight tests verify chunk parsing.

---

### Task 7: Update test_workflows_tf_ordering anchor (if needed) and full CI check

**Files:**
- Verify: `src/tests/test_workflows_tf_ordering.py` (no change expected)
- Verify: `src/tests/test_card_parity_with_terraform.py` (no change expected)

- [ ] **Step 1: Verify no Terraform changes needed**

Run: `uv run pytest src/tests/test_workflows_tf_ordering.py -v`
Expected: PASS — Terraform is unchanged, anchor count stays 33.

- [ ] **Step 2: Verify card parity**

Run: `uv run pytest src/tests/test_card_parity_with_terraform.py -v`
Expected: PASS — no new TF tasks, `preflight_tracking_context: None` already registered.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest src/tests/ -v --timeout=120 -x`
Expected: All tests PASS. Watch for:
- Any test that imported `run_pipeline` from tracking_context (should not exist — it was internal)
- Any test that imported `_process_idsse`/`_process_metrica`/`_process_skillcorner` (should not exist — they were internal)

- [ ] **Step 4: Run ruff + pyright on full src/**

Run: `uv run ruff check src/ && uv run ruff format --check src/ && uv run pyright src/`
Expected: Clean.

---

### Task 8: Single commit

**Files:** All modified files from Tasks 1-7.

- [ ] **Step 1: Review diff**

Run: `git diff --stat` and `git diff` to review all changes.

Verify:
- `src/ingestion/tracking_context.py`: `_RESULT_SCHEMA`, `_serialize_xt_grid`, `_deserialize_xt_grid`, `_make_tracking_context_udf` added; `_process_*` + `run_pipeline` deleted; `main_preflight` extended; `main` rewritten; `workflow`/`WorkflowSkippedError` imports removed.
- `src/tests/test_tracking_context_udf.py`: new file with 5 tests.
- `src/tests/test_tracking_context_preflight.py`: 1 new test.

- [ ] **Step 2: USER APPROVAL REQUIRED — commit**

```bash
git add src/ingestion/tracking_context.py src/tests/test_tracking_context_udf.py src/tests/test_tracking_context_preflight.py
git commit -m "$(cat <<'EOF'
fix(tracking-context): replace driver .toPandas() with applyInPandas (TC-1b)

IDSSE matches (~3.1M rows) OOM on serverless 16 GB driver because
.toPandas() pulls full tracking data to driver. Replace with
groupBy("match_id", "period").applyInPandas() so tracking data stays
on executors throughout the enrichment pipeline.

- Add _RESULT_SCHEMA StructType parsed from existing DDL constant
- Add _serialize_xt_grid / _deserialize_xt_grid for preflight xT
- Add _make_tracking_context_udf: scalar-primitive closure capture
  following off_ball_xt.py pattern (no pickle, lazy imports)
- Extend main_preflight to fit xT once and write as task value
- Rewrite main() for iteration-only mode with applyInPandas dispatch
- Remove run_pipeline(), _process_idsse/metrica/skillcorner (dead code)
- Remove @workflow decorator + WorkflowSkippedError import
- Staged del inside UDF: peak ~700 MB within 800 MB budget
- Row-count WARNING guardrail at 2M rows per UDF group
- 6 new unit tests (xT round-trip, actions round-trip, closure pickle,
  schema parity, field count, xT task value JSON)

Spec: docs/superpowers/specs/2026-05-12-tc1b-applyinpandas-design.md

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: E2E verification on Databricks

- [ ] **Step 1: USER APPROVAL REQUIRED — push and trigger**

Push the branch and trigger the daily job with only the tracking context tasks:

```bash
git push origin HEAD
```

Then trigger:
```bash
databricks jobs run-now --json '{"job_id": 302697362345215, "only": ["preflight_tracking_context", "compute_tracking_context"]}'
```

- [ ] **Step 2: Monitor for OOM**

Check the Databricks Jobs UI for:
1. `preflight_tracking_context` completes successfully (discovers matches, writes chunks + xT task value).
2. `compute_tracking_context` iterations launch (one per chunk).
3. IDSSE iteration does NOT exit 137 (SIGKILL/OOM).
4. Results appear in `bronze.spadl_tracking_context` with correct row counts.

If OOM persists: the 700 MB peak estimate was optimistic. Next step would be reducing the projection set further or splitting IDSSE periods into frame-range windows.
