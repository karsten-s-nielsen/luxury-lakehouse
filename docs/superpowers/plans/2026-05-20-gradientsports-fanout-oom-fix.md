# Gradient Sports Fan-Out + OOM Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the Spark Connect 256 MB serialization crash on extra-time matches and parallelize Gradient Sports ingestion via preflight + for_each_task fan-out.

**Architecture:** Replace `spark.createDataFrame(pandas_df)` with Parquet staging through a UC Volume (bypasses RPC wire limit entirely). Break the monolithic sequential task into a preflight (discovers matches, emits JSON-serialized MatchInfo array as task value) and a for_each_task fan-out (one iteration per match, concurrency=8). The iteration entry point reuses the existing `ingest_gradientsports` wheel entry point with a new `--match-json` CLI arg via the existing `parse_ingestion_args(extra_args=...)` mechanism.

**Tech Stack:** PySpark, Databricks for_each_task, Pydantic (MatchInfo serialization), Terraform HCL, pandas Parquet I/O, `ensure_volume_directory()` for UC Volume FUSE safety.

**Spec:** `docs/superpowers/specs/2026-05-20-gradientsports-fanout-oom-fix-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|---------------|--------|
| `src/ingestion/utils.py` | Extract shared `write_task_value()` helper | Modify |
| `src/ingestion/gradientsports_tracking.py` | Parquet staging in `write_tracking()` | Modify |
| `src/ingestion/gradientsports.py` | Add `main_preflight()`, add `--match-json` to `main()` | Modify |
| `terraform/modules/workflows/main.tf` | Replace monolithic task with preflight + for_each_task | Modify |
| `pyproject.toml` | Add `preflight_gradientsports` entry point | Modify |
| `scripts/patch_job_retries.py` | Add iteration to `_INGESTION_TASK_KEYS`, update comment | Modify |
| `dbt_project/seeds/task_workflow_mapping.csv` | Add `preflight_gradientsports` row | Modify |
| `workflow-cards/wf-gradientsports.yaml` | Update execution section | Modify |
| `src/tests/test_gradientsports_ingestion.py` | All new tests + verify existing survive | Modify |

---

### Task 1: Parquet staging — TDD for `write_tracking()` refactor

**Files:**
- Test: `src/tests/test_gradientsports_ingestion.py`
- Modify: `src/ingestion/gradientsports_tracking.py:180-205`

- [ ] **Step 1: Add `from pathlib import Path` module-level import to the test file**

Add to the imports at the top of `src/tests/test_gradientsports_ingestion.py` (after the existing imports):

```python
from pathlib import Path
```

- [ ] **Step 2: Write AST source-code guard test**

Add this test class at the end of `src/tests/test_gradientsports_ingestion.py`:

```python
class TestParquetStaging:
    """Regression guards for the Parquet staging fix (spec §4.1)."""

    def test_no_create_dataframe_in_tracking_module(self) -> None:
        """AST guard: spark.createDataFrame must never appear in gradientsports_tracking.py.

        The OOM fix replaces createDataFrame with Parquet staging. This test
        prevents silent reintroduction of the RPC-bound path.
        """
        import ast

        source_path = Path(__file__).resolve().parents[1] / "ingestion" / "gradientsports_tracking.py"
        tree = ast.parse(source_path.read_text())

        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "createDataFrame":
                pytest.fail(
                    f"spark.createDataFrame found at line {node.lineno} in gradientsports_tracking.py. "
                    "Use Parquet staging via UC Volume instead (spec §2.1)."
                )
```

- [ ] **Step 3: Run test — expected FAIL (createDataFrame still exists)**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestParquetStaging::test_no_create_dataframe_in_tracking_module -v`

Expected: FAIL — `spark.createDataFrame found at line 189`

- [ ] **Step 4: Write Parquet schema round-trip test**

Add to the `TestParquetStaging` class:

```python
    def test_parquet_schema_round_trip(self, tmp_path: Path) -> None:
        """Pandas DF -> Parquet -> Pandas preserves column names and dtypes.

        Validates the pandas-to-Parquet layer. Spark's Parquet reader is
        Spark's responsibility — this test catches int64/float64 widening
        and string/object dtype issues at the boundary we control.
        """
        import numpy as np
        import pandas as pd

        n = 100
        df = pd.DataFrame(
            {
                "match_id": ["10502"] * n,
                "game_ref_id": [10502.0] * n,
                "frame_num": np.arange(n, dtype="float64"),
                "period": [1.0] * n,
                "period_elapsed_time": np.random.default_rng(42).uniform(0, 5400, n),
                "period_game_clock_time": np.random.default_rng(42).uniform(0, 5400, n),
                "video_time_ms": np.random.default_rng(42).uniform(0, 5_400_000, n),
                "version": ["4.1.0"] * n,
                "generated_time": ["2023-07-12T07:26:52Z"] * n,
                "smoothed_time": ["2024-02-02T14:01:56Z"] * n,
                "game_event_id": [6629601.0] * n,
                "possession_event_id": [6510902.0] * n,
                "_game_event_json": ['{"type": "FIRSTKICKOFF"}'] * n,
                "_possession_event_json": ['{"type": "PA"}'] * n,
                "team_side": ["home"] * n,
                "is_ball": [False] * n,
                "jersey_num": ["8"] * n,
                "confidence": ["HIGH"] * n,
                "visibility": ["VISIBLE"] * n,
                "x": np.random.default_rng(42).uniform(-55, 55, n),
                "y": np.random.default_rng(42).uniform(-34, 34, n),
                "z": [np.nan] * n,
                "x_smoothed": np.random.default_rng(42).uniform(-55, 55, n),
                "y_smoothed": np.random.default_rng(42).uniform(-34, 34, n),
                "z_smoothed": [np.nan] * n,
                "_ingested_at": pd.Timestamp.now(tz="UTC"),
            }
        )

        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path, index=False)
        df_back = pd.read_parquet(parquet_path)

        assert list(df_back.columns) == list(df.columns)
        assert len(df_back) == len(df)
        for col in ["frame_num", "period", "x", "y"]:
            assert df_back[col].dtype.name == "float64", f"{col} dtype changed to {df_back[col].dtype}"

    def test_staging_path_format(self) -> None:
        """_staging_path produces the expected UC Volume path format."""
        from ingestion.gradientsports_tracking import _staging_path

        assert _staging_path("cat", "bronze", "10502") == "/Volumes/cat/bronze/_staging/gradientsports_tracking/10502.parquet"
        assert _staging_path("soccer_analytics", "dev_bronze", "10508") == "/Volumes/soccer_analytics/dev_bronze/_staging/gradientsports_tracking/10508.parquet"
```

- [ ] **Step 5: Run schema round-trip test — passes; staging path test — FAIL (_staging_path doesn't exist yet)**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestParquetStaging::test_parquet_schema_round_trip src/tests/test_gradientsports_ingestion.py::TestParquetStaging::test_staging_path_format -v`

Expected: `test_parquet_schema_round_trip` PASS, `test_staging_path_format` FAIL (ImportError)

- [ ] **Step 6: Implement Parquet staging in `write_tracking()`**

Replace `write_tracking()` and add `_staging_path()` in `src/ingestion/gradientsports_tracking.py` (lines 180-205). Also add the `ensure_volume_directory` import at the top of the file:

Add to imports (after `from ingestion.utils import validate_dataframe, write_delta_table`):

```python
from ingestion.utils import ensure_volume_directory, validate_dataframe, write_delta_table
```

Replace lines 180-205 with:

```python
def _staging_path(catalog: str, schema: str, match_id: str) -> str:
    """UC Volume staging path for Parquet intermediates.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema name (flows from CLI args, not hardcoded).
        match_id: Gradient Sports match ID.
    """
    return f"/Volumes/{catalog}/{schema}/_staging/gradientsports_tracking/{match_id}.parquet"


def write_tracking(
    spark: SparkSession,
    df: pd.DataFrame,
    catalog: str,
    schema: str,
    match_id: str,
    logger: logging.Logger,
) -> int:
    """Write parsed tracking DataFrame to bronze.gradientsports_tracking.

    Uses Parquet staging via UC Volume to bypass the 256 MB Spark Connect
    RPC serialization limit. pandas DF -> Parquet file -> spark.read.parquet().
    """
    import os

    staging = _staging_path(catalog, schema, match_id)
    ensure_volume_directory(os.path.dirname(staging))

    df.to_parquet(staging, index=False)
    logger.info("Staged %d tracking rows to %s", len(df), staging)

    try:
        sdf = spark.read.parquet(staging)
        row_count = validate_dataframe(
            sdf,
            ["match_id", "frame_num", "period"],
            "gradientsports_tracking",
            logger,
        )
        write_delta_table(
            sdf,
            catalog,
            schema,
            "gradientsports_tracking",
            replace_where=f"match_id = '{match_id}'",
            logger=logger,
            row_count=row_count,
        )
    finally:
        try:
            os.remove(staging)
            logger.debug("Cleaned up staging file %s", staging)
        except OSError:
            # Best-effort cleanup. If FUSE delete fails, the file is
            # overwritten on next run (idempotent by match_id path).
            logger.debug("Staging cleanup failed for %s (will be overwritten on next run)", staging)

    return row_count
```

- [ ] **Step 7: Write Parquet staging flow test (mock spark)**

Add to the `TestParquetStaging` class (this test is written after the implementation because it patches `_staging_path` which now exists):

```python
    @patch("ingestion.gradientsports_tracking.write_delta_table")
    @patch("ingestion.gradientsports_tracking.validate_dataframe")
    @patch("ingestion.gradientsports_tracking.ensure_volume_directory")
    def test_write_tracking_uses_parquet_staging(
        self,
        mock_ensure_dir: MagicMock,
        mock_validate: MagicMock,
        mock_write_delta: MagicMock,
        tmp_path: Path,
    ) -> None:
        """write_tracking() must stage via Parquet, not createDataFrame (spec §4.1 item 2)."""
        import pandas as pd

        from ingestion.gradientsports_tracking import write_tracking

        mock_spark = MagicMock()
        mock_validate.return_value = 5
        df = pd.DataFrame({"match_id": ["10502"] * 5, "frame_num": [1.0] * 5, "period": [1.0] * 5})

        staging_path = str(tmp_path / "staging" / "10502.parquet")
        # Create parent directory manually — ensure_volume_directory is mocked out,
        # but df.to_parquet() needs the directory to exist on the local filesystem.
        (tmp_path / "staging").mkdir()
        with patch("ingestion.gradientsports_tracking._staging_path", return_value=staging_path):
            write_tracking(mock_spark, df, "cat", "bronze", "10502", MagicMock())

        # createDataFrame must NOT be called
        mock_spark.createDataFrame.assert_not_called()
        # ensure_volume_directory must be called for the parent dir
        mock_ensure_dir.assert_called_once()
        # spark.read.parquet must be called with the staging path
        mock_spark.read.parquet.assert_called_once_with(staging_path)
        # Delta write must happen
        mock_write_delta.assert_called_once()
```

- [ ] **Step 8: Run all Parquet staging tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestParquetStaging -v`

Expected: All 4 PASS

- [ ] **Step 9: Run all existing tests to verify no regressions**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py -v`

Expected: All existing tests PASS (TestMatchInfo, TestParseEvents, TestParseTracking, TestIngestAtomicity, TestParquetStaging)

- [ ] **Step 10: Commit**

```
git add src/ingestion/gradientsports_tracking.py src/tests/test_gradientsports_ingestion.py
git commit -m "fix(gradientsports): replace createDataFrame with Parquet staging via UC Volume

Bypasses the 256 MB Spark Connect RPC serialization limit that crashed
on extra-time matches (5.3M+ rows). pandas DF -> Parquet on UC Volume ->
spark.read.parquet(). Uses ensure_volume_directory() for FUSE-safe dir
creation. Staging file cleaned up after Delta write (best-effort).

Includes AST regression guard, schema round-trip test, path format test,
and staging flow test."
```

---

### Task 2: MatchInfo JSON round-trip test

**Files:**
- Test: `src/tests/test_gradientsports_ingestion.py`

- [ ] **Step 1: Write MatchInfo JSON round-trip test**

Add this test class to `src/tests/test_gradientsports_ingestion.py` after `TestMatchInfo`:

```python
class TestMatchInfoSerialization:
    """Verify MatchInfo survives JSON round-trip via model_dump_json (spec §4.2 item 5)."""

    def test_round_trip_preserves_all_fields(self) -> None:
        """model_dump_json -> model_validate_json must produce an identical MatchInfo."""
        from ingestion.gradientsports_common import MatchInfo

        original = MatchInfo(
            id="10508",
            artifacts={"10508_events": "events.json", "10508_tracking": "tracking.jsonl.bz2"},
            home="Morocco",
            away="Spain",
            date="2022-12-06",
            updated_at=datetime(2022, 12, 6, 15, 30, 0, tzinfo=timezone.utc),
            visibility="public",
        )

        json_str = original.model_dump_json()
        restored = MatchInfo.model_validate_json(json_str)

        assert restored == original
        assert restored.id == "10508"
        assert restored.updated_at == datetime(2022, 12, 6, 15, 30, 0, tzinfo=timezone.utc)
        assert restored.artifacts == original.artifacts

    def test_model_dump_json_not_json_dumps(self) -> None:
        """json.dumps(model_dump()) raises TypeError on datetime — guard against misuse."""
        from ingestion.gradientsports_common import MatchInfo

        m = MatchInfo(
            id="10502",
            artifacts={},
            home="A",
            away="B",
            date="2022-01-01",
            updated_at=datetime(2022, 1, 1, tzinfo=timezone.utc),
            visibility="public",
        )

        # model_dump_json works (Pydantic handles datetime)
        json_str = m.model_dump_json()
        assert isinstance(json_str, str)

        # json.dumps(model_dump()) crashes on datetime
        with pytest.raises(TypeError, match="not JSON serializable"):
            json.dumps(m.model_dump())
```

- [ ] **Step 2: Run tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestMatchInfoSerialization -v`

Expected: PASS (both tests)

- [ ] **Step 3: Commit**

```
git add src/tests/test_gradientsports_ingestion.py
git commit -m "test(gradientsports): add MatchInfo JSON round-trip tests

Verifies model_dump_json -> model_validate_json preserves all fields
including datetime. Guards against json.dumps(model_dump()) which
raises TypeError on datetime fields (spec §4.2 item 5)."
```

---

### Task 3: Extract shared `write_task_value()` + preflight entry point

**Files:**
- Modify: `src/ingestion/utils.py`
- Modify: `src/ingestion/gradientsports.py`
- Test: `src/tests/test_gradientsports_ingestion.py`

The task-value writer pattern is duplicated in `idsse.py`, `spadl_vaep.py`, `tracking_context.py`. Rather than copy it a 4th time, extract a shared helper into `utils.py`. Existing callers can be migrated in a follow-up; this task only creates the shared helper and uses it in the new preflight.

- [ ] **Step 1: Add `write_task_value()` to `src/ingestion/utils.py`**

Add at the end of `src/ingestion/utils.py` (after the `ensure_volume_directory` function):

```python
def write_task_value(key: str, value: list[str], logger: logging.Logger | None = None) -> None:
    """Write a Databricks task value for downstream for_each_task consumption.

    Wraps ``dbutils.jobs.taskValues.set()`` with graceful fallback:
    outside the Databricks runtime (local dev, unit tests), the
    ``pyspark.dbutils`` import fails and the function logs a warning
    and returns cleanly so entry points remain testable.

    This is the canonical helper for all preflight task-value emission.
    Existing per-module copies (idsse, spadl_vaep, tracking_context) can
    be migrated to this in a follow-up.

    Args:
        key: Task value key (e.g. ``"gradientsports_matches"``).
        value: Task value payload — list of strings for for_each_task inputs.
        logger: Optional logger. Falls back to module logger if not provided.
    """
    _log = logger or logging.getLogger(__name__)
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            _log.warning("No active SparkSession -- task value '%s' not written", key)
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key=key, value=value)
        _log.info("Wrote task value '%s' (%d elements)", key, len(value))
    except (ImportError, AttributeError, RuntimeError) as exc:
        _log.warning("Task values not available (likely standalone mode) -- %s", exc)
```

- [ ] **Step 2: Write unit test for `write_task_value()` graceful fallback**

Add this test class to `src/tests/test_gradientsports_ingestion.py`:

```python
class TestWriteTaskValue:
    """Tests for the shared write_task_value() helper in utils.py."""

    def test_graceful_fallback_outside_databricks(self) -> None:
        """write_task_value logs warning when DBUtils is unavailable (local/CI)."""
        from ingestion.utils import write_task_value

        mock_logger = MagicMock()
        write_task_value("test_key", ["a", "b"], mock_logger)
        # Outside Databricks, pyspark.dbutils ImportError fires → warning logged
        mock_logger.warning.assert_called_once()
        assert "not available" in str(mock_logger.warning.call_args)

    def test_no_active_session_warns(self) -> None:
        """write_task_value warns when SparkSession.getActiveSession() returns None."""
        import importlib
        import sys

        from ingestion.utils import write_task_value

        # Temporarily make pyspark.dbutils importable but SparkSession returns None
        mock_dbutils_mod = MagicMock()
        mock_spark_mod = MagicMock()
        mock_spark_mod.SparkSession.getActiveSession.return_value = None
        mock_logger = MagicMock()

        with patch.dict(sys.modules, {"pyspark.dbutils": mock_dbutils_mod, "pyspark.sql": mock_spark_mod}):
            write_task_value("test_key", ["a", "b"], mock_logger)

        mock_logger.warning.assert_called_once()
        assert "No active SparkSession" in str(mock_logger.warning.call_args)
```

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestWriteTaskValue -v`

Expected: PASS — both paths tested (ImportError fallback + no active session).

- [ ] **Step 3: Write preflight tests**

Add this test class to `src/tests/test_gradientsports_ingestion.py`:

```python
class TestPreflight:
    """Tests for main_preflight() — spec §4.2."""

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list")
    def test_preflight_emits_json_array(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Preflight emits a JSON array where each element is a valid MatchInfo JSON string."""
        from ingestion.gradientsports import main_preflight
        from ingestion.gradientsports_common import MatchInfo

        matches = [_make_match("10502"), _make_match("10503"), _make_match("10504")]
        mock_fetch.return_value = matches

        emitted: list[list[str]] = []

        def capture_task_value(key: str, value: list[str], logger: object = None) -> None:
            assert key == "gradientsports_matches"
            emitted.append(value)

        mock_spark = MagicMock()
        with (
            patch("ingestion.gradientsports.timed_check") as mock_check,
            patch("ingestion.gradientsports.write_task_value", side_effect=capture_task_value),
            patch("ingestion.gradientsports.get_spark_session", return_value=mock_spark),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.gradientsports.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(catalog="cat", schema="bronze")
            mock_check.return_value = FilterResult(
                workflow_id="wf-gradientsports",
                count=3,
                metadata={"matches": [m.model_dump() for m in matches]},
            )
            main_preflight()

        assert len(emitted) == 1
        task_value = emitted[0]
        assert len(task_value) == 3

        # Each element must be deserializable to MatchInfo
        for json_str in task_value:
            restored = MatchInfo.model_validate_json(json_str)
            assert restored.id in {"10502", "10503", "10504"}

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_match_list", return_value=[])
    def test_preflight_empty_guard_emits_empty_list(
        self,
        mock_fetch: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """When guard finds no matches, preflight emits [] (spec §4.2 item 6)."""
        from ingestion.gradientsports import main_preflight

        emitted: list[list[str]] = []

        def capture_task_value(key: str, value: list[str], logger: object = None) -> None:
            emitted.append(value)

        mock_spark = MagicMock()
        with (
            patch("ingestion.gradientsports.timed_check") as mock_check,
            patch("ingestion.gradientsports.write_task_value", side_effect=capture_task_value),
            patch("ingestion.gradientsports.get_spark_session", return_value=mock_spark),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.gradientsports.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(catalog="cat", schema="bronze")
            mock_check.return_value = FilterResult(
                workflow_id="wf-gradientsports",
                count=0,
            )
            main_preflight()

        assert len(emitted) == 1
        assert emitted[0] == []
```

- [ ] **Step 4: Run preflight tests — expected FAIL (main_preflight doesn't exist)**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestPreflight -v`

Expected: FAIL — `ImportError: cannot import name 'main_preflight'`

- [ ] **Step 5: Implement `main_preflight()` in `gradientsports.py`**

Add the `write_task_value` import. In the import block at the top of `src/ingestion/gradientsports.py`, add `write_task_value` to the `from ingestion.utils import` statement:

```python
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    tolerate_missing_table,
    write_task_value,
)
```

Add `main_preflight()` after `main()` (after line 205):

```python
def main_preflight() -> None:
    """CLI entry point for Gradient Sports preflight task.

    Runs the skip guard to discover matches, serializes each MatchInfo
    as a JSON string, and emits the list as a Databricks task value for
    downstream for_each_task consumption.

    Behavior:
        - N matches found -> emits N-element JSON array
        - 0 matches found -> emits [] (for_each_task spawns 0 iterations)
    """
    args = parse_ingestion_args(
        "Preflight: discover Gradient Sports matches and emit "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    _logger = configure_logging("gradientsports_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    if filter_result.count == 0:
        _logger.info("No new Gradient Sports matches -- emitting empty task value")
        write_task_value("gradientsports_matches", [], _logger)
        return

    raw_matches = filter_result.metadata.get("matches", [])  # type: ignore[union-attr]
    matches = [MatchInfo.model_validate(m) for m in raw_matches]

    # Serialize each MatchInfo as a JSON string for {{input}} consumption.
    # Uses model_dump_json() — NOT json.dumps(model_dump()) which crashes on datetime.
    match_jsons = [m.model_dump_json() for m in matches]

    _logger.info(
        "Gradient Sports preflight: %d matches discovered, emitting task value",
        len(match_jsons),
    )
    write_task_value("gradientsports_matches", match_jsons, _logger)
```

- [ ] **Step 6: Run preflight tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestPreflight -v`

Expected: PASS (both tests)

- [ ] **Step 7: Run all tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py -v`

Expected: All PASS

- [ ] **Step 8: Commit**

```
git add src/ingestion/utils.py src/ingestion/gradientsports.py src/tests/test_gradientsports_ingestion.py
git commit -m "feat(gradientsports): add shared write_task_value() + preflight entry point

Extract write_task_value() into ingestion/utils.py — canonical helper
for all preflight task-value emission (DRY: replaces 4th copy).

main_preflight() runs the skip guard, serializes each MatchInfo via
model_dump_json(), and emits the list as a Databricks task value.
Empty guard result emits [] (0 iterations)."
```

---

### Task 4: `--match-json` iteration mode in `main()`

**Files:**
- Modify: `src/ingestion/gradientsports.py`
- Test: `src/tests/test_gradientsports_ingestion.py`

- [ ] **Step 1: Write `--match-json` tests**

These tests mock `parse_ingestion_args` which returns a Namespace with `match_json` — this is the correct interface because Step 3 adds `--match-json` via the existing `extra_args` mechanism.

Add this test class to `src/tests/test_gradientsports_ingestion.py`:

```python
class TestMatchJsonIteration:
    """Tests for --match-json single-match iteration mode (spec §4.3)."""

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.parse_tracking")
    @patch("ingestion.gradientsports.parse_events")
    def test_match_json_deserializes_and_ingests(
        self,
        mock_parse_events: MagicMock,
        mock_parse_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """--match-json deserializes MatchInfo and calls ingest for that single match."""
        import pandas as pd

        from ingestion.gradientsports import main

        match = _make_match("10508")
        match_json = match.model_dump_json()

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]', content=b"data")
        mock_parse_events.return_value = pd.DataFrame({"match_id": ["10508"]})
        mock_parse_tracking.return_value = pd.DataFrame({"match_id": ["10508"]})

        with (
            patch("ingestion.gradientsports.get_spark_session", return_value=MagicMock()),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.gradientsports.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(catalog="cat", schema="bronze", match_json=match_json)
            main()

        # Should have called ingest for exactly one match
        mock_write_tracking.assert_called_once()
        mock_write_events.assert_called_once()
        # Verify it used match_id 10508
        tracking_call_args = mock_write_tracking.call_args
        assert tracking_call_args[0][4] == "10508"  # match_id positional arg

    @patch("ingestion.gradientsports.resolve_pining_token", return_value="fake-token")
    @patch("ingestion.gradientsports.fetch_artifact")
    @patch("ingestion.gradientsports.write_events")
    @patch("ingestion.gradientsports.write_tracking")
    @patch("ingestion.gradientsports.parse_tracking")
    @patch("ingestion.gradientsports.parse_events")
    def test_match_json_preserves_write_ordering(
        self,
        mock_parse_events: MagicMock,
        mock_parse_tracking: MagicMock,
        mock_write_tracking: MagicMock,
        mock_write_events: MagicMock,
        mock_fetch_artifact: MagicMock,
        mock_token: MagicMock,
    ) -> None:
        """Write-ordering invariant: tracking before events, even in --match-json mode."""
        import pandas as pd

        from ingestion.gradientsports import main

        call_order: list[str] = []
        match = _make_match("10508")
        match_json = match.model_dump_json()

        mock_fetch_artifact.return_value = MagicMock(text='[{"gameId": 1}]', content=b"data")
        mock_parse_events.return_value = pd.DataFrame({"match_id": ["10508"]})
        mock_parse_tracking.return_value = pd.DataFrame({"match_id": ["10508"]})
        mock_write_tracking.side_effect = lambda *a, **kw: call_order.append("tracking")
        mock_write_events.side_effect = lambda *a, **kw: call_order.append("events")

        with (
            patch("ingestion.gradientsports.get_spark_session", return_value=MagicMock()),
            patch("ingestion.gradientsports.configure_logging", return_value=MagicMock()),
            patch("ingestion.gradientsports.parse_ingestion_args") as mock_args,
            patch("ingestion.gradientsports.bootstrap_hooks"),
        ):
            mock_args.return_value = MagicMock(catalog="cat", schema="bronze", match_json=match_json)
            main()

        assert call_order == ["tracking", "events"], (
            f"Write order must be tracking-first, events-last; got {call_order}"
        )
```

- [ ] **Step 2: Run tests — expected FAIL (main() doesn't read match_json)**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestMatchJsonIteration -v`

Expected: FAIL — current `main()` does not read `match_json`; test takes the legacy guard path and fails on un-mocked `timed_check`/`fetch_match_list` interactions

- [ ] **Step 3: Modify `main()` to accept `--match-json` via `parse_ingestion_args(extra_args=...)`**

Replace the `main()` function in `src/ingestion/gradientsports.py` (lines 187-205):

```python
def main() -> None:
    """CLI entry point for Gradient Sports data ingestion.

    Two modes:
        - ``--match-json <JSON>``: Single-match mode (for_each_task iteration).
          Deserializes the JSON to MatchInfo and ingests that one match.
        - No ``--match-json``: Legacy standalone mode. Runs the guard and
          ingests all discovered matches sequentially. Kept for manual CLI
          usage and backward compatibility.
    """
    args = parse_ingestion_args(
        "Ingest Gradient Sports data into the bronze layer",
        extra_args=[
            (
                "--match-json",
                {
                    "type": str,
                    "default": None,
                    "help": (
                        "JSON-serialized MatchInfo for single-match iteration mode. "
                        "Used by the Terraform for_each_task fan-out — each iteration "
                        "receives one match via {{input}}. Omit to run guard + full "
                        "sequential ingestion."
                    ),
                },
            ),
        ],
    )
    _logger = configure_logging("gradientsports")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    if args.match_json is not None:
        # Single-match mode: for_each_task iteration
        match = MatchInfo.model_validate_json(args.match_json)
        _logger.info("Single-match mode: ingesting match %s (%s vs %s)", match.id, match.home, match.away)
        ingest_gradientsports(spark, args.catalog, args.schema, _logger, [match])
    else:
        # Legacy standalone mode: guard + sequential ingestion
        filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)
        _logger.info("Starting Gradient Sports ingestion into %s.%s", args.catalog, args.schema)
        run_pipeline(spark, args.catalog, args.schema, _logger, filter_result=filter_result)

    _logger.info("Gradient Sports ingestion complete")
```

- [ ] **Step 4: Run iteration tests**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py::TestMatchJsonIteration -v`

Expected: PASS (both tests)

- [ ] **Step 5: Run ALL tests including TestIngestAtomicity (write-ordering invariant must survive)**

Run: `uv run pytest src/tests/test_gradientsports_ingestion.py -v`

Expected: All PASS — especially the TestIngestAtomicity tests which verify write ordering

- [ ] **Step 6: Commit**

```
git add src/ingestion/gradientsports.py src/tests/test_gradientsports_ingestion.py
git commit -m "feat(gradientsports): add --match-json single-match iteration mode

When --match-json is provided via parse_ingestion_args(extra_args=...),
deserializes MatchInfo and ingests that single match (for_each_task
iteration path). Without it, runs guard + sequential ingestion (legacy
standalone mode). Write-ordering invariant (tracking before events)
preserved in both paths.

run_pipeline() retained for the legacy standalone path and manual
ad-hoc runs."
```

---

### Task 5: Terraform, pyproject.toml, patch_job_retries, seed CSV, workflow card

**Files:**
- Modify: `terraform/modules/workflows/main.tf:774-794`
- Modify: `pyproject.toml:117`
- Modify: `scripts/patch_job_retries.py:56-72`
- Modify: `dbt_project/seeds/task_workflow_mapping.csv:27`
- Modify: `workflow-cards/wf-gradientsports.yaml:33-41`

- [ ] **Step 1: Replace monolithic Terraform task with preflight + for_each_task**

In `terraform/modules/workflows/main.tf`, replace lines 774-794 (the monolithic `ingest_gradientsports` task block) with:

```hcl
  # ── Task: Gradient Sports preflight (guard + match discovery) ───────────
  # Runs the skip guard, discovers matches via the pining-for-the-data API,
  # and emits each match as a JSON-serialized MatchInfo string in the task
  # value array. Downstream for_each_task consumes via {{input}}.
  #
  # Behavior:
  #   - 64 matches → 64-element JSON array → 64 iterations (concurrency=8)
  #   - 0 matches  → [] → 0 iterations spawned
  task {
    task_key        = "preflight_gradientsports"
    timeout_seconds = 300
    max_retries     = 0

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_gradientsports"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze"
      ]
    }

    environment_key = "default"
  }

  # ── Task: Ingest Gradient Sports data (for_each_task fan-out) ───────────
  # One iteration per match. Each iteration receives a JSON-serialized
  # MatchInfo via {{input}} and ingests that single match (events + tracking).
  # Parquet staging bypasses the 256 MB Spark Connect RPC limit.
  #
  # Downstream tasks reference this task as `ingest_gradientsports` (the
  # parent); Databricks resolves dependencies against the for_each_task
  # parent rather than individual iterations.
  task {
    task_key = "ingest_gradientsports"

    depends_on {
      task_key = "preflight_gradientsports"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_gradientsports.values.gradientsports_matches}}"
      concurrency = 8

      task {
        task_key        = "ingest_gradientsports_iteration"
        timeout_seconds = 900
        max_retries     = 1 # API calls — transient failures benefit from retry

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "ingest_gradientsports"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-json", "{{input}}",
          ]
        }

        environment_key = "default"
      }
    }
  }
```

- [ ] **Step 2: Add `preflight_gradientsports` entry point to `pyproject.toml`**

In `pyproject.toml`, after line 117 (`ingest_gradientsports = "ingestion.gradientsports:main"`), add:

```toml
preflight_gradientsports = "ingestion.gradientsports:main_preflight"
```

- [ ] **Step 3: Update `_INGESTION_TASK_KEYS` in `scripts/patch_job_retries.py`**

Replace lines 56-72 in `scripts/patch_job_retries.py`:

```python
_INGESTION_TASK_KEYS: frozenset[str] = frozenset(
    {
        "backfill_statsbomb_360",
        "backfill_statsbomb_extra",
        "hf_sync",
        "import_obso_results",
        # ingest_gradientsports is a for_each_task parent (no max_retries of its own)
        # ingest_idsse is a for_each_task parent (no max_retries of its own)
        "ingest_gradientsports_iteration",
        "ingest_idsse_iteration",
        "ingest_idsse_events",
        "ingest_metrica",
        "ingest_skillcorner",
        "ingest_statsbomb",
        "ingest_wyscout",
    }
)
```

- [ ] **Step 4: Add `preflight_gradientsports` to task_workflow_mapping.csv**

In `dbt_project/seeds/task_workflow_mapping.csv`, after line 27 (`ingest_gradientsports,wf-gradientsports`), add:

```
preflight_gradientsports,wf-gradientsports
```

- [ ] **Step 5: Update workflow card execution section**

In `workflow-cards/wf-gradientsports.yaml`, replace lines 33-41 (the `execution:` block):

```yaml
execution:
  preflight:
    trigger: scheduled
    runtime: databricks-workflow
    entry_point: preflight_gradientsports
    module: ingestion.gradientsports
    distribution: driver-bound
    timeout: "300s"
    environment: analytics
  ingestion:
    trigger: for_each_task
    runtime: databricks-workflow
    entry_point: ingest_gradientsports
    module: ingestion.gradientsports
    distribution: fan-out
    concurrency: 8
    timeout: "900s"
    environment: analytics
```

- [ ] **Step 6: Run TF parity tests**

Run: `uv run pytest src/tests/test_job_retry_policy.py src/tests/test_workflows_tf_ordering.py -v`

Expected: All PASS

- [ ] **Step 7: Run guard conformance test**

Run: `uv run pytest src/tests/test_guard_conformance.py -v`

Expected: All PASS (guard unchanged, patch target `ingestion.gradientsports.fetch_match_list` still valid)

- [ ] **Step 8: Run full test suite**

Run: `uv run pytest src/tests/ -v`

Expected: All PASS

- [ ] **Step 9: Commit**

```
git add terraform/modules/workflows/main.tf pyproject.toml scripts/patch_job_retries.py dbt_project/seeds/task_workflow_mapping.csv workflow-cards/wf-gradientsports.yaml
git commit -m "feat(gradientsports): replace monolithic task with preflight + for_each_task fan-out

Preflight discovers matches and emits JSON MatchInfo array as task value.
for_each_task spawns one iteration per match (concurrency=8).

- TF: preflight_gradientsports (300s, max_retries=0) + ingest_gradientsports
  for_each_task wrapper + ingest_gradientsports_iteration (900s, max_retries=1)
- pyproject.toml: preflight_gradientsports entry point
- patch_job_retries: add ingest_gradientsports_iteration to INGESTION_TASK_KEYS,
  update ingest_gradientsports exclusion comment
- task_workflow_mapping.csv: add preflight_gradientsports row
- wf-gradientsports.yaml: update execution section for fan-out"
```

---

### Task 6: Final verification and lint

**Files:**
- All changed files

- [ ] **Step 1: Run ruff lint**

Run: `uv run ruff check src/ingestion/gradientsports.py src/ingestion/gradientsports_tracking.py src/ingestion/utils.py scripts/patch_job_retries.py src/tests/test_gradientsports_ingestion.py`

Expected: No errors. If any, fix them.

- [ ] **Step 2: Run ruff format check**

Run: `uv run ruff format --check src/ingestion/gradientsports.py src/ingestion/gradientsports_tracking.py src/ingestion/utils.py scripts/patch_job_retries.py src/tests/test_gradientsports_ingestion.py`

Expected: No formatting issues. If any, run `uv run ruff format` on the affected files.

- [ ] **Step 3: Run pyright**

Run: `uv run pyright src/ingestion/gradientsports.py src/ingestion/gradientsports_tracking.py src/ingestion/utils.py`

Expected: No errors in basic mode.

- [ ] **Step 4: Run full test suite one final time**

Run: `uv run pytest src/tests/ -v`

Expected: All PASS

- [ ] **Step 5: Verify git status is clean**

Run: `git status`

Expected: Clean working tree (all changes committed)

---

## Self-Review

**Spec coverage check:**

| Spec section | Plan task |
|-------------|-----------|
| §2.1 Parquet staging via UC Volume | Task 1 (step 6) |
| §2.1 ensure_volume_directory for staging dir | Task 1 (step 6 — uses `ensure_volume_directory()` not `os.makedirs`) |
| §2.1 staging path uses `{schema}` not hardcoded | Task 1 (step 6 — `_staging_path` function) |
| §2.2.1 Preflight entry point | Task 3 |
| §2.2.1 model_dump_json serialization | Task 3 (step 5) |
| §2.2.2 --match-json iteration mode | Task 4 |
| §2.2.2 Convention note (--match-json vs --match-ids) | Documented in spec, no code action |
| §2.2.3 Terraform changes | Task 5 (step 1) |
| §3 pyproject.toml entry point | Task 5 (step 2) |
| §3.1 patch_job_retries classification | Task 5 (step 3) |
| §3 task_workflow_mapping.csv | Task 5 (step 4) |
| §3 workflow card update | Task 5 (step 5) |
| §4.1 item 1 AST source-code guard | Task 1 (step 2) |
| §4.1 item 2 Parquet staging flow test | Task 1 (step 7) |
| §4.1 item 3 Parquet schema round-trip | Task 1 (step 4) |
| §4.2 item 4 Preflight task value format | Task 3 (step 3) |
| §4.2 item 5 MatchInfo JSON round-trip | Task 2 |
| §4.2 item 6 Empty guard result | Task 3 (step 3 — second test) |
| §4.3 item 7 --match-json deserialization | Task 4 (step 1 — first test) |
| §4.3 item 8 Write-ordering invariant | Task 4 (step 1 — second test) + Task 4 (step 5) |
| §4.4 Guard conformance | Task 5 (step 7) |
| §4.5 TF parity tests | Task 5 (step 6) |
| §5 Wheel bump before deploy | Not in plan (standard procedure, noted in spec §3 "Not in scope") |
| M4 staging path format test | Task 1 (step 4 — `test_staging_path_format`) |

**Placeholder scan:** No TBD, TODO, "implement later", or vague steps found.

**Type consistency check:**
- `_staging_path(catalog, schema, match_id)` — defined in Task 1 step 6, tested in Task 1 step 4 (path format) and patched in Task 1 step 7 (flow test)
- `write_task_value(key, value, logger)` — defined in Task 3 step 1 (`utils.py`), tested in Task 3 step 2, imported in Task 3 step 5 (`gradientsports.py`), patched in Task 3 step 3 (preflight tests). Signature: `list[str]` only (no `str` union).
- `main_preflight()` — defined in Task 3 step 5, imported in Task 3 step 3 (tests)
- `parse_ingestion_args(description, extra_args=...)` — used consistently in both `main()` (Task 4 step 3) and `main_preflight()` (Task 3 step 4). Tests mock `parse_ingestion_args` consistently.
- `MatchInfo.model_dump_json()` / `MatchInfo.model_validate_json()` — consistent across Task 2, Task 3, Task 4
- `_make_match(mid)` helper — already exists in test file at line 264, reused in Tasks 3 and 4. Uses `artifacts={"events": ..., "tracking": ...}` which matches the `"event" in key.lower()` / `"track" in key.lower()` search in `ingest_gradientsports()`.

**Review findings addressed:**

| Finding | Resolution |
|---------|-----------|
| C1: os.makedirs fails on serverless | Uses `ensure_volume_directory()` from `utils.py` (Task 1 step 6) |
| C2: Raw argparse breaks convention | Uses `parse_ingestion_args(extra_args=...)` (Task 4 step 3) |
| H1: Tests written against wrong interface | Tests mock `parse_ingestion_args` from the start — no rewrite needed (Task 4 step 1) |
| H2: _staging_path patched before it exists | Flow test (step 7) moved after implementation (step 6) |
| H3: --timeout=120 not installed | Removed from all `pytest` commands |
| H4: DRY violation — 4th task-value copy | Extracted shared `write_task_value()` into `utils.py` (Task 3 step 1) |
| M1: os.remove may fail on FUSE | Best-effort cleanup with comment; overwritten on next run (Task 1 step 6) |
| M2: No integration test for task-value | Unit test covers graceful fallback (Task 3 step 2); happy path validated by production runs |
| M3: Artifact key format inconsistency | Existing `_make_match()` keys work because of `"event" in key.lower()` substring match |
| M4: No _staging_path format test | Added `test_staging_path_format` (Task 1 step 4) |
| L1: run_pipeline() dead code | Intentionally preserved for legacy standalone path and manual ad-hoc runs (Task 4 commit msg) |
| L2: Naming confirmed OK | `ingest_gradientsports_iteration` matches convention |
| L3: Confusing import paragraph | Replaced with clean module-level `from pathlib import Path` in step 1 |

**v2 review findings addressed:**

| Finding | Resolution |
|---------|-----------|
| M1: Task 4 Step 2 expected failure inaccurate | Updated to describe the actual failure mode (legacy guard path, un-mocked timed_check) |
| M2: write_task_value `list[str] \| str` YAGNI | Narrowed to `list[str]`, removed isinstance branch |
| M3: No unit test for write_task_value() | Added `TestWriteTaskValue` with graceful-fallback and no-active-session tests (Task 3 step 2) |
| M4: Flow test FileNotFoundError | Added `(tmp_path / "staging").mkdir()` before the call (Task 1 step 7) |
| L1: capture_task_value weak coupling | Acknowledged — inherent to mock-based testing |
| L2: §4.6 phantom reference | Fixed to "validated by production runs" |
| L3: artifact key format (pre-existing) | No action — not introduced by this plan |
