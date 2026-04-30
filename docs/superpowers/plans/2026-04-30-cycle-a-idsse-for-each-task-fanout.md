# Cycle A: IDSSE `for_each_task` Fan-Out (Runtime-Discovered) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **HARD RULE — CLAUDE.md:** Every `git commit`, `git push`, `gh pr create`, and `gh pr merge` requires **separate, explicit user approval at the moment of action**. Approval of this plan does NOT grant commit authority. Steps marked **🛑 REQUIRES USER APPROVAL** below MUST halt execution and prompt the user before proceeding.

**Goal:** Unblock Phase H by parallelizing `ingest_idsse` from sequential 7-match (~45 min wall-clock per TODO D40d, 2026-04-21) to runtime-discovered parallel chunks of ≤2 matches each (~13 min/chunk wall-clock), fitting the existing 900 s timeout. **Establishes the runtime-discovered fan-out pattern** that future cycles (D40a — pitch_control / off-ball xT / SPADL-VAEP) will reuse.

**Architecture:** Three layers, all leveraging the existing `FilterResult.chunks` mechanism in `src/ingestion/guards.py:42` (designed exactly for this).

1. **Guard layer (`src/ingestion/idsse.py:_IdsseGuard.check`)** — refactored to anti-join `IDSSE_MATCH_IDS` against `bronze.idsse_tracking ∩ bronze.idsse_events`, producing `FilterResult(count=N, chunks=[[...], [...]])` where chunks are missing-match IDs partitioned at size ≤ `chunk_size=2`.

2. **Preflight task (`ingest_idsse:main_preflight`, new entry point)** — runs the guard, serializes `FilterResult.chunks` as a JSON list of comma-separated strings (`["m1,m2", "m3,m4"]`), writes to `dbutils.jobs.taskValues.set(key="idsse_match_chunks", value=...)`. Best-effort outside Databricks (local/test mode degrades cleanly).

3. **Terraform DAG** — new `preflight_idsse` task before `ingest_idsse`. The `ingest_idsse` `for_each_task` reads `inputs = "{{tasks.preflight_idsse.values.idsse_match_chunks}}"`, concurrency=4. Each iteration receives one comma-separated chunk via `--match-ids "{{input}}"`. **No hardcoded chunks** — the chunk count and contents are discovered at every job run.

**Behavior in practice:**
- All 7 missing → preflight emits 4 chunks → 4 parallel iterations → ~13 min total wall-clock.
- Partial (e.g., 3 missing) → preflight emits 2 chunks → 2 parallel iterations.
- All 7 done (no-op run) → preflight emits empty list `[]` → for_each_task spawns 0 iterations → ~30 s preflight cost only.
- 8th IDSSE match added in the future → just append to `IDSSE_MATCH_IDS`; chunks regenerate automatically next run. **No Terraform change required.**

`ingest_idsse_events` (the separate Terraform task driven by `ingestion.idsse_events:main`) **stays as a single sequential task** in this cycle. The events parser is fast (~1 s per file × 7 = ~7 s parsing); fanning it out is overhead-negative. Right-sizing its 900 s timeout is Cycle C scope.

**Tech Stack:** Python 3.10, argparse, pytest, Databricks Terraform provider (`for_each_task` + task-value substitution `{{tasks.X.values.Y}}`), `dbutils.jobs.taskValues`, ruff, pyright.

**Spec:** Audit findings in this conversation (Critical findings C1 + C3) + TODO D40d + the existing `FilterResult.chunks` design in `src/ingestion/guards.py:42`. No separate design doc.

**Branch:** `feat/cycle-a-idsse-fanout` (created from `origin/main` at `8b99116`).

---

## File Structure

### New Files

| File | Responsibility |
|------|----------------|
| (none) | Cycle A adds no new source files. All changes are in-place edits + tests + the wheel-bump stamp propagation. (The new `preflight_idsse` entry point is added inside the existing `src/ingestion/idsse.py`.) |

### Modified Files

| File | Change |
|------|--------|
| `src/ingestion/idsse.py` | (a) Refactor `_IdsseGuard.check()` to populate `FilterResult.chunks` based on missing matches; add class constant `chunk_size = 2`. (b) Add `_parse_match_ids_arg` helper. (c) Extend `main()` to consume `--match-ids` and thread `match_ids` keyword through `run_pipeline` to `ingest_idsse(...)` + `ingest_idsse_events(...)` (both already accept the kwarg). (d) Add `main_preflight()` entry point + private `_write_match_chunks_task_value` helper. |
| `src/tests/test_idsse.py` | Add ~24 tests across 5 classes (TDD-ordered): `TestParseMatchIdsArg` (9), `TestRunPipelineMatchIds` (3), `TestMainCliE2E` (3), `TestIdsseGuardChunks` (6 — anti-join correctness + chunk sizing + edge cases), `TestPreflightIdsse` (3 — guard invocation + task-value shape + degraded local mode). |
| `pyproject.toml` | Add `preflight_idsse = "ingestion.idsse:main_preflight"` to `[project.scripts]`. Wheel version bump: `0.3.23` → `0.3.24`. |
| `terraform/modules/workflows/main.tf` | Add a new `preflight_idsse` task block (before `ingest_idsse`); replace the existing single-task `ingest_idsse` block with a `for_each_task` wrapper consuming `{{tasks.preflight_idsse.values.idsse_match_chunks}}`. |
| `src/shared/wheel.py` + 21 stamp files | Auto-propagated by `scripts/bump_wheel.py`. |

### Out of scope for Cycle A (queued for later cycles)

- Parser micro-optimization in `_parse_positions_xml` (Cycle B).
- Right-sizing the 5 other 900 s ingest task timeouts + adding `dbt_build` retry (Cycle C).
- Generalizing the runtime-discovered fan-out pattern to pitch_control / off-ball xT / SPADL-VAEP (TODO D40a, future cycle — Cycle A establishes the prototype).

---

## Task 0 — Pre-flight: verify state assumptions

Files: read-only.

- [ ] **Step 1: Verify working tree state**

Run:
```bash
git status --short
git diff --stat
```

Expected: 22 stamp files show as `M` (modified) but `git diff --stat` shows zero content drift (CRLF/LF noise — see session 67 file). Plus `src/tests/test_idsse_period_derivation.py` shows `M` with ~65 lines added (3 implementation-agnostic tests added in session 67, intentionally retained).

If the diff includes other unexpected files, STOP and ask the user before proceeding.

- [ ] **Step 2: Verify current branch is `main`**

Run:
```bash
git branch --show-current
```

Expected: `main`. If on a different branch, ask the user how to proceed.

- [ ] **Step 3: Verify Databricks provider supports `for_each_task` + task-value substitution**

Run:
```bash
cd terraform/environments/dev && terraform version
```

Expected: Databricks provider version ≥ 1.110.0 (the version family present in `.terraform/providers/registry.terraform.io/databricks/databricks/`). Both `for_each_task` and the `{{tasks.X.values.Y}}` substitution are supported since provider 1.50+.

- [ ] **Step 4: Verify `ingest_idsse_events` has its own entry point**

Run:
```bash
grep -n "ingest_idsse_events" pyproject.toml
```

Expected: `ingest_idsse_events = "ingestion.idsse_events:main"` (a separate module). Confirms the Cycle A scope decision: we do NOT touch `idsse_events.py`.

- [ ] **Step 5: Confirm `FilterResult.chunks` field exists**

Run:
```bash
grep -n "chunks:" src/ingestion/guards.py
```

Expected: `chunks: list[list[str]] | None = None` at line ~42. This is the field we'll populate; if the type signature has drifted, STOP and reconcile.

- [ ] **Step 6: Create feature branch**

Run:
```bash
git checkout -b feat/cycle-a-idsse-fanout
```

Expected: branch created off `main` at `8b99116`.

**🛑 NOTE:** No commits will be made until Task 8. The 22 stamp-file CRLF diffs and the 3 retained tests in `test_idsse_period_derivation.py` will ride along in the eventual Cycle A commit (acceptable — those tests are net coverage gain on a recently-changed function and the CRLF noise is incidental).

---

## Task 1 — Add `_parse_match_ids_arg` helper + tests (TDD)

**Files:**
- Modify: `src/ingestion/idsse.py` — add private helper after `_DEFAULT_DATA_DIR` constant (~line 343).
- Modify: `src/tests/test_idsse.py` — add `TestParseMatchIdsArg` test class.

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/test_idsse.py`:

```python
class TestParseMatchIdsArg:
    """`_parse_match_ids_arg` — CLI subset parsing + validation.

    Used by the Terraform `for_each_task` fan-out: each child iteration
    receives `--match-ids "J03WMX,J03WN1"` (comma-separated subset,
    runtime-discovered by the preflight task). `main()` calls this helper
    to parse + validate.
    """

    def test_returns_none_for_none_input(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg
        assert _parse_match_ids_arg(None) is None

    def test_returns_none_for_empty_string(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg
        assert _parse_match_ids_arg("") is None

    def test_parses_single_id(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg
        assert _parse_match_ids_arg("J03WMX") == ["J03WMX"]

    def test_parses_comma_separated_list(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg
        assert _parse_match_ids_arg("J03WMX,J03WN1") == ["J03WMX", "J03WN1"]

    def test_strips_whitespace(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg
        assert _parse_match_ids_arg(" J03WMX , J03WN1 ") == ["J03WMX", "J03WN1"]

    def test_skips_empty_segments(self) -> None:
        from ingestion.idsse import _parse_match_ids_arg
        assert _parse_match_ids_arg("J03WMX,,J03WN1") == ["J03WMX", "J03WN1"]

    def test_rejects_unknown_id(self) -> None:
        import pytest

        from ingestion.idsse import _parse_match_ids_arg

        with pytest.raises(SystemExit) as excinfo:
            _parse_match_ids_arg("BOGUS_ID")
        assert "BOGUS_ID" in str(excinfo.value)

    def test_rejects_mixed_known_and_unknown(self) -> None:
        import pytest

        from ingestion.idsse import _parse_match_ids_arg

        with pytest.raises(SystemExit) as excinfo:
            _parse_match_ids_arg("J03WMX,BOGUS")
        assert "BOGUS" in str(excinfo.value)

    def test_accepts_full_idsse_match_id_set(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, _parse_match_ids_arg

        joined = ",".join(IDSSE_MATCH_IDS)
        result = _parse_match_ids_arg(joined)
        assert result == list(IDSSE_MATCH_IDS)
```

- [ ] **Step 2: Run the tests — verify they fail**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestParseMatchIdsArg -v
```

Expected: 9 FAIL — `ImportError: cannot import name '_parse_match_ids_arg' from 'ingestion.idsse'`.

- [ ] **Step 3: Implement `_parse_match_ids_arg`**

Open `src/ingestion/idsse.py` and add this function immediately after the `_DEFAULT_DATA_DIR` line (~line 343, before `def _parse_teams`):

```python
def _parse_match_ids_arg(raw: str | None) -> list[str] | None:
    """Parse the optional ``--match-ids`` comma-separated CLI value.

    Used by the Terraform ``for_each_task`` fan-out: each child iteration
    receives ``--match-ids "J03WMX,J03WN1"`` (a runtime-discovered subset
    of :data:`IDSSE_MATCH_IDS`). This helper parses the string, validates
    every ID is known, and returns a clean list (or ``None`` when no
    filter was provided — full 7-match run).

    Args:
        raw: Raw CLI string (e.g. ``"J03WMX,J03WN1"`` or ``None``).

    Returns:
        Validated list of match IDs, or ``None`` when ``raw`` is empty.

    Raises:
        SystemExit: When any ID in ``raw`` is not in :data:`IDSSE_MATCH_IDS`.
            Hard-fail-fast — silent filtering would mask preflight/Python
            drift (e.g. an iteration receiving an ID that was removed from
            the constant).
    """
    if raw is None or raw == "":
        return None
    requested = [mid.strip() for mid in raw.split(",") if mid.strip()]
    unknown = [mid for mid in requested if mid not in IDSSE_MATCH_IDS]
    if unknown:
        raise SystemExit(
            f"Unknown IDSSE match IDs in --match-ids: {unknown}. "
            f"Valid IDs: {sorted(IDSSE_MATCH_IDS)}"
        )
    return requested
```

- [ ] **Step 4: Run the tests — verify they pass**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestParseMatchIdsArg -v
```

Expected: 9 PASS.

- [ ] **Step 5: Lint + type check the helper**

Run:
```bash
uv run ruff check src/ingestion/idsse.py src/tests/test_idsse.py
uv run pyright src/ingestion/idsse.py
```

Expected: zero violations.

---

## Task 2 — Wire `--match-ids` through `main` + `run_pipeline` (TDD)

**Files:**
- Modify: `src/ingestion/idsse.py` — extend `main()` and `run_pipeline()` signatures.
- Modify: `src/tests/test_idsse.py` — add `TestRunPipelineMatchIds` + `TestMainCliE2E` test classes.

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/test_idsse.py`:

```python
class TestRunPipelineMatchIds:
    """`run_pipeline` forwards `match_ids` to the inner ingest functions.

    Verifies the for_each_task wiring at the run_pipeline boundary.
    """

    def test_run_pipeline_forwards_match_ids_to_both_inner_functions(self) -> None:
        from unittest.mock import MagicMock, patch

        from ingestion.guards import FilterResult
        from ingestion.idsse import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        fr = FilterResult(workflow_id="wf-idsse", count=2)
        chunk = ["J03WMX", "J03WN1"]

        with (
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            run_pipeline(
                spark, "cat", "schema", logger_mock,
                filter_result=fr,
                match_ids=chunk,
            )

        assert mock_track.call_args.kwargs.get("match_ids") == chunk
        assert mock_events.call_args.kwargs.get("match_ids") == chunk

    def test_run_pipeline_default_match_ids_is_none(self) -> None:
        """Backward-compat: existing callers passing no match_ids see None."""
        from unittest.mock import MagicMock, patch

        from ingestion.guards import FilterResult
        from ingestion.idsse import run_pipeline

        spark = MagicMock()
        logger_mock = MagicMock()
        fr = FilterResult(workflow_id="wf-idsse", count=7)

        with (
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            run_pipeline(spark, "cat", "schema", logger_mock, filter_result=fr)

        assert mock_track.call_args.kwargs.get("match_ids") is None
        assert mock_events.call_args.kwargs.get("match_ids") is None

    def test_run_pipeline_skip_propagates_when_count_zero(self) -> None:
        """When filter_result.count == 0, raises WorkflowSkippedError
        regardless of match_ids — preserves current skip semantics."""
        from unittest.mock import MagicMock

        import pytest

        from ingestion.guards import FilterResult
        from ingestion.idsse import run_pipeline
        from workflows.exceptions import WorkflowSkippedError

        spark = MagicMock()
        logger_mock = MagicMock()
        fr = FilterResult(workflow_id="wf-idsse", count=0)

        with pytest.raises(WorkflowSkippedError):
            run_pipeline(
                spark, "cat", "schema", logger_mock,
                filter_result=fr,
                match_ids=["J03WMX", "J03WN1"],
            )


class TestMainCliE2E:
    """End-to-end test of the iteration's CLI flow.

    Exercises the full path that each `ingest_idsse_iteration` task hits:
    `python -m ingestion.idsse --catalog cat --schema bronze --match-ids "J03WMX,J03WN1"`
    Mocks only the Spark session + bootstrap + the inner ingest functions.
    """

    def test_main_with_chunk_subset_threads_through_to_ingest(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            [
                "ingest_idsse",
                "--catalog", "soccer_analytics",
                "--schema", "bronze",
                "--match-ids", "J03WMX,J03WN1",
            ],
        )

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.idsse.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            from ingestion.guards import FilterResult
            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(workflow_id="wf-idsse", count=2)

            from ingestion.idsse import main
            main()

        assert mock_track.call_count == 1
        assert mock_events.call_count == 1
        assert mock_track.call_args.kwargs.get("match_ids") == ["J03WMX", "J03WN1"]
        assert mock_events.call_args.kwargs.get("match_ids") == ["J03WMX", "J03WN1"]

    def test_main_without_match_ids_processes_all(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            ["ingest_idsse", "--catalog", "soccer_analytics", "--schema", "bronze"],
        )

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.idsse.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse.ingest_idsse") as mock_track,
            patch("ingestion.idsse.ingest_idsse_events") as mock_events,
        ):
            from ingestion.guards import FilterResult
            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(workflow_id="wf-idsse", count=7)

            from ingestion.idsse import main
            main()

        assert mock_track.call_args.kwargs.get("match_ids") is None
        assert mock_events.call_args.kwargs.get("match_ids") is None

    def test_main_with_unknown_match_id_exits(self, monkeypatch) -> None:
        """Fail-fast: SystemExit before any Spark session is created."""
        import pytest
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            [
                "ingest_idsse",
                "--catalog", "soccer_analytics",
                "--schema", "bronze",
                "--match-ids", "J03WMX,BOGUS_ID",
            ],
        )

        with (
            patch("ingestion.idsse.get_spark_session"),
            patch("ingestion.idsse.bootstrap_hooks"),
            patch("ingestion.idsse.ingest_idsse") as mock_track,
        ):
            from ingestion.idsse import main

            with pytest.raises(SystemExit) as excinfo:
                main()
            assert "BOGUS_ID" in str(excinfo.value)
            mock_track.assert_not_called()
```

- [ ] **Step 2: Run the tests — verify they fail**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestRunPipelineMatchIds src/tests/test_idsse.py::TestMainCliE2E -v
```

Expected: 6 FAIL — `TypeError: run_pipeline() got an unexpected keyword argument 'match_ids'` (or similar).

- [ ] **Step 3: Update `run_pipeline` signature**

In `src/ingestion/idsse.py`, replace the existing `run_pipeline` function (currently lines ~891–906) with:

```python
@workflow("wf-idsse", phase="ingestion")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
    match_ids: list[str] | None = None,
) -> int:
    """Ingest IDSSE tracking and event data into the bronze layer.

    Args:
        spark: Active Spark session.
        catalog: Unity Catalog name.
        schema: Bronze schema name.
        logger: Structured logger.
        filter_result: Skip-guard result; ``count == 0`` raises
            :class:`WorkflowSkippedError`.
        ctx: Optional workflow context (kept for hook parity).
        match_ids: Optional subset of :data:`IDSSE_MATCH_IDS` to ingest.
            ``None`` means process all 7 matches (single-task path or
            backward-compat caller). When set (typically by the
            ``for_each_task`` fan-out passing ``--match-ids "J03WMX,J03WN1"``
            from the preflight task's discovered chunks), only the listed
            matches are ingested for both tracking AND events. Per-match
            incremental skip still applies inside :func:`ingest_idsse`
            and :func:`ingest_idsse_events`.
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")
    ingest_idsse(spark, catalog, schema, logger, match_ids=match_ids)
    ingest_idsse_events(spark, catalog, schema, logger, match_ids=match_ids)
    return 0
```

- [ ] **Step 4: Update `main()` to consume `--match-ids`**

In `src/ingestion/idsse.py`, replace the existing `main()` function (currently lines ~909–923) with:

```python
def main() -> None:
    """CLI entry point for IDSSE tracking ingestion (single-iteration handler).

    Each iteration of the ``for_each_task`` fan-out invokes this entry point
    with ``--match-ids "J03WMX,J03WN1"`` (a runtime-discovered subset
    written by the ``preflight_idsse`` task). When invoked without
    ``--match-ids`` (e.g., manual standalone run), the function processes
    the full 7-match :data:`IDSSE_MATCH_IDS` set.
    """
    args = parse_ingestion_args(
        "Ingest IDSSE Bundesliga tracking data into the bronze layer",
        extra_args=[
            (
                "--match-ids",
                {
                    "type": str,
                    "default": None,
                    "help": (
                        "Optional comma-separated subset of IDSSE match IDs to "
                        "ingest (e.g. 'J03WMX,J03WN1'). Used by the Terraform "
                        "for_each_task fan-out — runtime-discovered by the "
                        "preflight_idsse task. Omit to process all 7 matches."
                    ),
                },
            ),
        ],
    )
    logger = configure_logging("idsse")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    match_ids = _parse_match_ids_arg(getattr(args, "match_ids", None))

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting IDSSE ingestion into %s.%s", args.catalog, args.schema)
    if match_ids is not None:
        logger.info("Restricted to chunk: %s (%d matches)", match_ids, len(match_ids))

    run_pipeline(
        spark,
        args.catalog,
        args.schema,
        logger,
        filter_result=filter_result,
        match_ids=match_ids,
    )
    logger.info("IDSSE ingestion complete")
```

- [ ] **Step 5: Run the tests — verify they pass**

Run:
```bash
uv run pytest src/tests/test_idsse.py -v -k "TestRunPipelineMatchIds or TestMainCliE2E"
```

Expected: 6 PASS. Existing tests in `test_idsse.py` continue to pass.

- [ ] **Step 6: Lint + type check**

Run:
```bash
uv run ruff check src/ingestion/idsse.py src/tests/test_idsse.py
uv run pyright src/ingestion/idsse.py
```

Expected: zero violations.

---

## Task 3 — Refactor `_IdsseGuard.check()` to populate `FilterResult.chunks` (TDD)

**Files:**
- Modify: `src/ingestion/idsse.py` — replace `_IdsseGuard` class (currently lines ~55–74).
- Modify: `src/tests/test_idsse.py` — add `TestIdsseGuardChunks` test class.

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/test_idsse.py`:

```python
class TestIdsseGuardChunks:
    """`_IdsseGuard.check()` runtime chunk discovery.

    The guard anti-joins IDSSE_MATCH_IDS against (tracking ∩ events) to
    determine missing matches, then partitions them into chunks of size
    `chunk_size` (default 2). The preflight task forwards these chunks
    to the for_each_task fan-out via `dbutils.jobs.taskValues`.
    """

    def _mock_spark_with_match_ids(
        self,
        tracking_ids: set[str],
        events_ids: set[str],
    ) -> object:
        """Build a MagicMock Spark whose `.table(...).select(...).distinct().collect()`
        returns rows with the configured match_ids per table name."""
        from unittest.mock import MagicMock

        spark = MagicMock()

        def table_side_effect(name: str) -> MagicMock:
            mock_df = MagicMock()
            ids = events_ids if "events" in name else tracking_ids
            rows = [MagicMock(**{"__getitem__.return_value": mid}) for mid in ids]
            # Wire collect() to return rows where row["match_id"] yields the id
            mock_rows = []
            for mid in ids:
                row = MagicMock()
                row.__getitem__ = lambda self, key, _mid=mid: _mid
                mock_rows.append(row)
            mock_df.select.return_value.distinct.return_value.collect.return_value = mock_rows
            return mock_df

        spark.table.side_effect = table_side_effect
        return spark

    def test_all_seven_missing_returns_four_chunks(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        spark = self._mock_spark_with_match_ids(set(), set())
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.workflow_id == "wf-idsse"
        assert result.count == 7
        assert result.chunks is not None
        assert len(result.chunks) == 4  # ceil(7 / 2)
        # Sizing: 2,2,2,1
        assert [len(c) for c in result.chunks] == [2, 2, 2, 1]
        # All match IDs accounted for, in deterministic order
        flattened = [mid for chunk in result.chunks for mid in chunk]
        assert flattened == list(IDSSE_MATCH_IDS)

    def test_all_seven_done_returns_count_zero_no_chunks(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        all_ids = set(IDSSE_MATCH_IDS)
        spark = self._mock_spark_with_match_ids(all_ids, all_ids)
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.count == 0
        assert result.chunks is None or result.chunks == []

    def test_partial_three_missing_returns_two_chunks(self) -> None:
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        # First 4 done (in both tables), last 3 missing.
        done = set(IDSSE_MATCH_IDS[:4])
        spark = self._mock_spark_with_match_ids(done, done)
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.count == 3
        assert result.chunks is not None
        assert len(result.chunks) == 2  # ceil(3 / 2)
        assert [len(c) for c in result.chunks] == [2, 1]
        flattened = [mid for chunk in result.chunks for mid in chunk]
        assert sorted(flattened) == sorted(IDSSE_MATCH_IDS[4:])

    def test_match_in_tracking_but_not_events_counts_as_missing(self) -> None:
        """A match is 'complete' only when present in BOTH tracking AND events.

        If tracking has it but events doesn't (e.g. mid-flight from a
        previous job run), the match must still be re-attempted so the
        events ingestion gets a chance to run."""
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        all_ids = set(IDSSE_MATCH_IDS)
        partial_events = all_ids - {IDSSE_MATCH_IDS[0]}  # missing one in events
        spark = self._mock_spark_with_match_ids(all_ids, partial_events)

        result = skip_guard.check(spark, "cat", "bronze")
        assert result.count == 1
        assert result.chunks == [[IDSSE_MATCH_IDS[0]]]

    def test_chunk_size_is_two(self) -> None:
        from ingestion.idsse import _IdsseGuard
        assert _IdsseGuard.chunk_size == 2

    def test_no_chunk_exceeds_chunk_size(self) -> None:
        """Sizing invariant — preserved for any subset of missing matches."""
        from ingestion.idsse import IDSSE_MATCH_IDS, skip_guard

        # 5 missing
        done = set(IDSSE_MATCH_IDS[:2])
        spark = self._mock_spark_with_match_ids(done, done)
        result = skip_guard.check(spark, "cat", "bronze")

        assert result.chunks is not None
        for chunk in result.chunks:
            assert len(chunk) <= skip_guard.chunk_size
```

- [ ] **Step 2: Run the tests — verify they fail**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestIdsseGuardChunks -v
```

Expected: 6 FAIL — current `_IdsseGuard.check()` returns `count=0|1` with no `chunks` field populated; tests assert chunk structure.

- [ ] **Step 3: Refactor `_IdsseGuard`**

In `src/ingestion/idsse.py`, locate the existing `_IdsseGuard` class (currently lines ~55–74) and REPLACE it with:

```python
class _IdsseGuard:
    """Skip guard + runtime chunk discovery for IDSSE ingestion.

    The guard anti-joins :data:`IDSSE_MATCH_IDS` against the canonical
    ``match_id`` columns of ``bronze.idsse_tracking`` and
    ``bronze.idsse_events`` (intersection — a match is only "complete" when
    BOTH tables have ingested it). The resulting list of missing matches
    is partitioned into chunks of size :attr:`chunk_size` for the Terraform
    ``for_each_task`` fan-out (Cycle A pattern; Cycle B+ extends to other
    workflows).

    Wall-clock budget:
        At ~6.4 min/match wall-clock post-PR-1.8 (TODO D40d, 2026-04-21),
        a 2-match chunk fits ~13 min within the 900 s
        ``ingest_idsse_iteration`` timeout. ``chunk_size = 2`` is the
        largest size that fits with safety margin.
    """

    workflow_id = "wf-idsse"
    chunk_size: int = 2

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Compute missing matches and partition into for_each_task chunks."""
        import logging as _logging

        from ingestion.utils import tolerate_missing_table

        _guard_logger = _logging.getLogger(__name__)

        existing_t: set[str] = set()
        existing_e: set[str] = set()
        with tolerate_missing_table(_guard_logger, "IDSSE tables missing — needs ingestion"):
            t_rows = (
                spark.table(f"{catalog}.{schema}.idsse_tracking")
                .select("match_id")
                .distinct()
                .collect()
            )
            existing_t = {str(row["match_id"]) for row in t_rows}
            e_rows = (
                spark.table(f"{catalog}.{schema}.idsse_events")
                .select("match_id")
                .distinct()
                .collect()
            )
            existing_e = {str(row["match_id"]) for row in e_rows}

        # A match is complete only when present in BOTH tracking AND events.
        # Bronze stores match_id as the canonical bare DFL form (e.g.
        # 'J03WMX') per ADR-018 / Bug #1 (PR-LL2 close-out).
        completed = existing_t & existing_e
        missing = [mid for mid in IDSSE_MATCH_IDS if mid not in completed]

        if not missing:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        chunks = [missing[i : i + self.chunk_size] for i in range(0, len(missing), self.chunk_size)]
        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(missing),
            chunks=chunks,
        )


skip_guard = _IdsseGuard()
```

- [ ] **Step 4: Run the tests — verify they pass**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestIdsseGuardChunks -v
```

Expected: 6 PASS.

- [ ] **Step 5: Run the full IDSSE test module — confirm no regressions**

Run:
```bash
uv run pytest src/tests/test_idsse.py -v
```

Expected: all pre-existing tests pass + new `TestIdsseGuardChunks` (6) pass + tests from Task 1 + Task 2 still pass.

- [ ] **Step 6: Lint + type check**

Run:
```bash
uv run ruff check src/ingestion/idsse.py src/tests/test_idsse.py
uv run pyright src/ingestion/idsse.py
```

Expected: zero violations.

---

## Task 4 — Add `main_preflight` entry point + tests (TDD)

**Files:**
- Modify: `src/ingestion/idsse.py` — add `_write_match_chunks_task_value` helper + `main_preflight()` entry point.
- Modify: `src/tests/test_idsse.py` — add `TestPreflightIdsse` test class.
- Modify: `pyproject.toml` — register entry point.

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/test_idsse.py`:

```python
class TestPreflightIdsse:
    """`main_preflight` runs the guard and writes chunks to task values.

    Output contract: `dbutils.jobs.taskValues.set(key="idsse_match_chunks",
    value=<list>)` where `<list>` is a list of comma-separated match-ID
    strings, exactly the shape that the for_each_task `inputs` field
    expects. Empty list when no work — for_each_task spawns 0 iterations.
    """

    def test_preflight_writes_chunks_in_for_each_input_format(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            ["preflight_idsse", "--catalog", "soccer_analytics", "--schema", "bronze"],
        )

        from ingestion.guards import FilterResult

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.idsse.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse._write_match_chunks_task_value") as mock_write,
        ):
            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(
                workflow_id="wf-idsse",
                count=3,
                chunks=[["J03WMX", "J03WN1"], ["J03WPY"]],
            )

            from ingestion.idsse import main_preflight
            main_preflight()

        # Helper called once with the for_each-shaped chunks.
        assert mock_write.call_count == 1
        chunks_for_inputs = mock_write.call_args.args[0]
        assert chunks_for_inputs == ["J03WMX,J03WN1", "J03WPY"]

    def test_preflight_writes_empty_list_when_no_work(self, monkeypatch) -> None:
        from unittest.mock import MagicMock, patch

        monkeypatch.setattr(
            "sys.argv",
            ["preflight_idsse", "--catalog", "soccer_analytics", "--schema", "bronze"],
        )

        from ingestion.guards import FilterResult

        with (
            patch("ingestion.idsse.get_spark_session") as mock_spark,
            patch("ingestion.idsse.bootstrap_hooks"),
            patch("ingestion.idsse.timed_check") as mock_check,
            patch("ingestion.idsse._write_match_chunks_task_value") as mock_write,
        ):
            mock_spark.return_value = MagicMock()
            mock_check.return_value = FilterResult(workflow_id="wf-idsse", count=0)

            from ingestion.idsse import main_preflight
            main_preflight()

        # Empty list → for_each_task spawns 0 iterations.
        chunks_for_inputs = mock_write.call_args.args[0]
        assert chunks_for_inputs == []

    def test_write_helper_degrades_cleanly_outside_databricks(self) -> None:
        """The dbutils import fails in local/test mode; the helper logs and returns."""
        from unittest.mock import MagicMock

        from ingestion.idsse import _write_match_chunks_task_value

        logger_mock = MagicMock()
        # Should NOT raise, even though dbutils.jobs.taskValues is unavailable.
        _write_match_chunks_task_value(["J03WMX,J03WN1"], logger_mock)

        # Either logged a warning OR succeeded silently — both acceptable.
        # The contract is "does not raise".
        # (No assertion on logger calls — implementation may vary.)
```

- [ ] **Step 2: Run the tests — verify they fail**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestPreflightIdsse -v
```

Expected: 3 FAIL — `ImportError: cannot import name 'main_preflight' from 'ingestion.idsse'` (or `'_write_match_chunks_task_value'`).

- [ ] **Step 3: Implement the helper + entry point**

In `src/ingestion/idsse.py`, append at the end of the file (after `if __name__ == "__main__": main()`):

```python
# ---------------------------------------------------------------------------
# Preflight entry point — runtime-discovered chunks for for_each_task fan-out
# ---------------------------------------------------------------------------


def _write_match_chunks_task_value(
    chunks_for_inputs: list[str],
    logger: logging.Logger,
) -> None:
    """Write the discovered chunks as a Databricks task value.

    The downstream ``ingest_idsse`` task's ``for_each_task`` reads this
    via ``"{{tasks.preflight_idsse.values.idsse_match_chunks}}"``.
    Empty list → 0 iterations spawned (no-op runs cost only the preflight
    task itself, ~30 s).

    Outside the Databricks runtime (local dev, unit tests), the
    ``dbutils`` import fails; we log a warning and return cleanly so
    the entry point remains testable.

    Args:
        chunks_for_inputs: List of comma-separated match-ID strings,
            e.g. ``["J03WMX,J03WN1", "J03WPY,J03WOH"]``. Each element
            becomes one iteration's ``{{input}}`` value.
        logger: Structured logger.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            logger.warning("No active SparkSession — task value not written")
            return
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key="idsse_match_chunks", value=chunks_for_inputs)
        logger.info(
            "Wrote task value 'idsse_match_chunks' (%d chunks)",
            len(chunks_for_inputs),
        )
    except (ImportError, AttributeError, RuntimeError) as exc:
        logger.warning(
            "Task values not available (likely standalone mode) — %s", exc
        )


def main_preflight() -> None:
    """CLI entry point for the IDSSE preflight task.

    Runs the IDSSE skip guard, partitions any missing matches into
    fan-out chunks (size :attr:`_IdsseGuard.chunk_size`), and writes the
    chunks as a Databricks task value (``idsse_match_chunks``) for the
    downstream ``ingest_idsse`` ``for_each_task`` to consume.

    Behavior:
        - All 7 missing → emits 4 chunks (2,2,2,1)
        - Partial (e.g. 3 missing) → emits 2 chunks (2,1)
        - All 7 done → emits empty list ``[]`` (for_each_task spawns 0 iterations)
        - 8th match added to ``IDSSE_MATCH_IDS`` → automatically picked up
          (chunks regenerate on the next preflight run)

    The same pattern (guard returns ``FilterResult.chunks`` → preflight
    writes task value → for_each_task consumes) is the prototype for
    Cycle B+ broader fan-out activation (TODO D40a — pitch_control,
    off-ball xT, SPADL-VAEP).
    """
    args = parse_ingestion_args(
        "Preflight: discover unprocessed IDSSE matches and emit chunks "
        "as a Databricks task value for downstream for_each_task fan-out"
    )
    logger = configure_logging("idsse_preflight")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    fr = timed_check(skip_guard, spark, args.catalog, args.schema)

    # Serialize each chunk as a comma-separated string —
    # for_each_task's `{{input}}` interpolates the entire string, and
    # the iteration's CLI splits on comma via _parse_match_ids_arg.
    chunks_for_inputs: list[str] = [",".join(chunk) for chunk in (fr.chunks or [])]

    logger.info(
        "IDSSE preflight: %d missing matches across %d chunks (chunk_size=%d)",
        fr.count,
        len(chunks_for_inputs),
        skip_guard.chunk_size,
    )

    _write_match_chunks_task_value(chunks_for_inputs, logger)
```

- [ ] **Step 4: Register the entry point in `pyproject.toml`**

Add to `[project.scripts]` (after the existing `ingest_idsse` line):

```toml
preflight_idsse = "ingestion.idsse:main_preflight"
```

- [ ] **Step 5: Run the tests — verify they pass**

Run:
```bash
uv run pytest src/tests/test_idsse.py::TestPreflightIdsse -v
```

Expected: 3 PASS.

- [ ] **Step 6: Run the full IDSSE test module**

Run:
```bash
uv run pytest src/tests/test_idsse.py -v
```

Expected: all tests pass — `TestParseMatchIdsArg` (9) + `TestRunPipelineMatchIds` (3) + `TestMainCliE2E` (3) + `TestIdsseGuardChunks` (6) + `TestPreflightIdsse` (3) = 24 new tests + existing.

- [ ] **Step 7: Lint + type check**

Run:
```bash
uv run ruff check src/ingestion/idsse.py src/tests/test_idsse.py
uv run pyright src/ingestion/idsse.py
```

Expected: zero violations.

---

## Task 5 — Terraform: add `preflight_idsse` task + restructure `ingest_idsse` as `for_each_task`

**Files:**
- Modify: `terraform/modules/workflows/main.tf` — add new `preflight_idsse` task; replace `ingest_idsse` task block with for_each_task wrapper.

- [ ] **Step 1: Add the `preflight_idsse` task block**

In `terraform/modules/workflows/main.tf`, locate the existing `ingest_idsse` task (currently lines 603–622, opens with `# ── Task: Ingest IDSSE Bundesliga tracking data ──`).

INSERT a new task block IMMEDIATELY BEFORE that comment (~before line 603):

```hcl
  # ── Task: IDSSE preflight — discover unprocessed matches + emit chunks ────
  # PR-Cycle-A (2026-04-30): Runtime chunk discovery for the for_each_task
  # fan-out. Anti-joins IDSSE_MATCH_IDS against bronze.idsse_tracking ∩
  # bronze.idsse_events, partitions missing matches into chunks of size 2
  # (per `_IdsseGuard.chunk_size` in src/ingestion/idsse.py), and writes
  # the chunks as a Databricks task value `idsse_match_chunks`.
  #
  # The downstream `ingest_idsse` for_each_task consumes the task value
  # via `{{tasks.preflight_idsse.values.idsse_match_chunks}}` — no
  # hardcoded chunks, no Terraform changes when adding/removing matches.
  task {
    task_key        = "preflight_idsse"
    timeout_seconds = 300
    max_retries     = 1

    python_wheel_task {
      package_name = "luxury_lakehouse"
      entry_point  = "preflight_idsse"

      parameters = [
        "--catalog", var.catalog_name,
        "--schema", "bronze",
      ]
    }

    environment_key = "default"
  }
```

- [ ] **Step 2: Replace the existing `ingest_idsse` task block with a `for_each_task` wrapper**

Locate the `ingest_idsse` task block (originally lines 603–622). Replace the ENTIRE block (its leading comment + `task { ... }` body) with:

```hcl
  # ── Task: Ingest IDSSE Bundesliga tracking data (for_each_task fan-out) ──
  # PR-Cycle-A (2026-04-30): Runtime-discovered fan-out. The chunk array
  # comes from `preflight_idsse` via task-value substitution; each chunk
  # is a comma-separated match-ID list (e.g. "J03WMX,J03WN1"), forwarded
  # to `--match-ids` of the iteration's `ingest_idsse` entry point.
  #
  # Behavior:
  #   - All 7 missing → 4 chunks → 4 parallel iterations → ~13 min wall-clock
  #   - Partial (e.g. 3 missing) → 2 chunks → 2 iterations
  #   - No missing → 0 iterations spawned (preflight emitted [])
  #
  # Downstream tasks reference this task as `ingest_idsse` (the parent);
  # Databricks resolves dependencies against the for_each_task parent
  # rather than individual iterations.
  task {
    task_key = "ingest_idsse"

    depends_on {
      task_key = "preflight_idsse"
    }

    for_each_task {
      inputs      = "{{tasks.preflight_idsse.values.idsse_match_chunks}}"
      concurrency = 4

      task {
        task_key        = "ingest_idsse_iteration"
        timeout_seconds = 900
        max_retries     = 1

        python_wheel_task {
          package_name = "luxury_lakehouse"
          entry_point  = "ingest_idsse"

          parameters = [
            "--catalog", var.catalog_name,
            "--schema", "bronze",
            "--match-ids", "{{input}}",
          ]
        }

        environment_key = "default"
      }
    }
  }
```

**IMPORTANT:** Do NOT modify the `ingest_idsse_events` task (currently lines ~624–647) or any downstream task that has `depends_on { task_key = "ingest_idsse" }`. The `for_each_task` parent name matches the original `task_key`, so existing dependency edges remain valid.

- [ ] **Step 3: Validate Terraform syntax**

Run:
```bash
cd terraform/environments/dev && terraform validate
```

Expected: `Success! The configuration is valid.`

If validate fails with `Unsupported block type "for_each_task"` or `Invalid expression "{{tasks...}}"`, the Databricks provider version is too old. STOP and ask the user — provider upgrade is out of Cycle A scope.

- [ ] **Step 4: Run terraform plan**

Run:
```bash
cd terraform/environments/dev && terraform plan -target=module.workflows -no-color > /tmp/cycle-a-tf-plan.txt 2>&1 || true
head -300 /tmp/cycle-a-tf-plan.txt
```

Expected output should show:
- 1 resource to update: `module.workflows.databricks_job.data_ingestion`
- The diff shows: a new `task { task_key = "preflight_idsse" ... }` block; the existing `task { task_key = "ingest_idsse" ... python_wheel_task { ... } }` replaced with `task { task_key = "ingest_idsse" depends_on { ... } for_each_task { ... } }`
- No other tasks should appear in the diff

If the plan errors with provider permission issues (auth not set up locally), record the local-validate-only result and proceed; the user will run apply post-merge.

---

## Task 6 — Bump wheel version + propagate stamps

**Files:**
- Modify: `pyproject.toml` (version field).
- Modify: 22 stamp files via `scripts/bump_wheel.py`.

- [ ] **Step 1: Bump version in `pyproject.toml`**

Open `pyproject.toml` and change line 3:

```toml
version = "0.3.23"
```

to:

```toml
version = "0.3.24"
```

- [ ] **Step 2: Propagate the bump to all stamp files**

Run:
```bash
uv run python scripts/bump_wheel.py
```

Expected: the script edits `src/shared/wheel.py` and the 21 script/terraform stamp files to reference `luxury_lakehouse-0.3.24-py3-none-any.whl`.

- [ ] **Step 3: Verify the stamp propagation**

Run:
```bash
git diff pyproject.toml | head -10
```

Expected: `pyproject.toml` shows `0.3.23` → `0.3.24`. Note: many stamp files were ALREADY showing as modified due to session-67 CRLF noise — `bump_wheel.py` writes the same `0.3.24` (matching) but with normalized line endings.

---

## Task 7 — Final verification (full lint + format + type + tests)

- [ ] **Step 1: Ruff lint (full repo)**

Run:
```bash
uv run ruff check src/ scripts/
```

Expected: zero violations.

- [ ] **Step 2: Ruff format check (full repo)**

Run:
```bash
uv run ruff format --check src/ scripts/
```

Expected: zero violations. If failures appear, run `uv run ruff format src/ scripts/`.

- [ ] **Step 3: Pyright type check**

Run:
```bash
uv run pyright src/
```

Expected: zero errors. (Warnings about pyspark missing imports are pre-existing and acceptable per CLAUDE.md.)

- [ ] **Step 4: Full pytest run**

Run:
```bash
uv run pytest src/tests/ -v
```

Expected: all tests pass. Pay specific attention to:
- `src/tests/test_idsse.py` — full module pass: existing + new `TestParseMatchIdsArg` (9) + `TestRunPipelineMatchIds` (3) + `TestMainCliE2E` (3) + `TestIdsseGuardChunks` (6) + `TestPreflightIdsse` (3) = 24 new tests.
- `src/tests/test_idsse_period_derivation.py` — passes (includes the 3 session-67 tests).
- `src/tests/test_idsse_bronze_coverage.py` — passes.
- `src/tests/test_wheel_conformance.py` — passes (verifies all stamp files match `pyproject.toml`).

- [ ] **Step 5: Terraform validate (final)**

Run:
```bash
cd terraform/environments/dev && terraform validate
```

Expected: success.

- [ ] **Step 6: Sanity-check the diff**

Run:
```bash
git diff --stat
```

Expected files modified:
- `pyproject.toml` (1 line bump + 1 entry-point line added = ~2 lines)
- `src/shared/wheel.py` + ~21 script/terraform stamp files (1 wheel filename per file)
- `src/ingestion/idsse.py` (~150 lines net: `_parse_match_ids_arg`, refactored `_IdsseGuard`, `run_pipeline` updated, `main` updated, `_write_match_chunks_task_value` + `main_preflight` added)
- `src/tests/test_idsse.py` (~480 lines added across 5 new test classes)
- `src/tests/test_idsse_period_derivation.py` (~65 lines — pre-existing session-67 work)
- `terraform/modules/workflows/main.tf` (~50 lines net: new preflight task + replaced ingest_idsse block)

Total: ~30 files modified, ~700 lines added/modified.

---

## Task 8 — 🛑 REQUEST USER APPROVAL TO COMMIT

**🛑 STOP HERE. Do NOT run `git commit` without explicit user approval at this exact moment.**

- [ ] **Step 1: Surface the diff summary to the user**

Output:
```
Cycle A is ready to commit. Diff summary:
  - Python: src/ingestion/idsse.py
      * _IdsseGuard.check() refactored to populate FilterResult.chunks (runtime discovery)
      * _parse_match_ids_arg + --match-ids CLI threading
      * _write_match_chunks_task_value + main_preflight (new entry point)
  - pyproject.toml: preflight_idsse entry point + 0.3.23 → 0.3.24 wheel bump
  - Tests: src/tests/test_idsse.py (+24 tests across 5 classes —
      TestParseMatchIdsArg, TestRunPipelineMatchIds, TestMainCliE2E,
      TestIdsseGuardChunks, TestPreflightIdsse)
  - Terraform: preflight_idsse task + ingest_idsse for_each_task wrapper
      consuming `{{tasks.preflight_idsse.values.idsse_match_chunks}}`
  - Sidecar: src/tests/test_idsse_period_derivation.py (3 session-67 tests retained)

All checks pass: ruff, format, pyright, pytest, terraform validate.

Ready to commit? Approval needed per CLAUDE.md hard rule.
```

Wait for the user to respond with explicit "yes" / "go" / "approved".

- [ ] **Step 2: After explicit approval, create the commit**

Run:
```bash
git add pyproject.toml src/ingestion/idsse.py src/tests/test_idsse.py src/tests/test_idsse_period_derivation.py src/shared/wheel.py terraform/modules/workflows/main.tf scripts/compute_*.py scripts/evaluate_*.py scripts/publish_*.py scripts/train_*.py scripts/validate_ev1_iter15_hf.py
```

Then commit with HEREDOC message:

```bash
git commit -m "$(cat <<'EOF'
feat(ingestion): IDSSE for_each_task fan-out with runtime-discovered chunks (Cycle A — Phase H unblock)

Splits ingest_idsse from sequential 7-match (~45 min wall-clock per TODO
D40d) to parallel chunks of <=2 matches each (~13 min/chunk wall-clock).
Establishes the runtime-discovered fan-out pattern (FilterResult.chunks
→ preflight task → for_each_task task-value) as the prototype for D40a
(broader fan-out activation: pitch_control, off-ball xT, SPADL-VAEP).

Python (src/ingestion/idsse.py):
- Refactor `_IdsseGuard.check()` to anti-join IDSSE_MATCH_IDS against
  (bronze.idsse_tracking ∩ bronze.idsse_events) and partition the missing
  matches into chunks of size 2 (`_IdsseGuard.chunk_size`). Returns
  `FilterResult(count, chunks)` where chunks is None for no-op.
- Add `_parse_match_ids_arg` helper (validates against IDSSE_MATCH_IDS,
  raises SystemExit on unknown IDs — fail-fast on preflight/Python drift).
- Thread `--match-ids` CLI flag through `main` → `run_pipeline` →
  `ingest_idsse` + `ingest_idsse_events` (both already accept the kwarg).
- Add `main_preflight` entry point + `_write_match_chunks_task_value`
  helper. Preflight runs the guard, serializes chunks as comma-separated
  strings, writes via `dbutils.jobs.taskValues.set(idsse_match_chunks)`.
  Empty list when no work — for_each_task spawns 0 iterations.

Terraform (terraform/modules/workflows/main.tf):
- Add `preflight_idsse` task (entry_point=preflight_idsse, 300s timeout).
- Replace `ingest_idsse` single-task with for_each_task wrapper
  consuming `{{tasks.preflight_idsse.values.idsse_match_chunks}}`,
  concurrency=4. Iteration receives `--match-ids "{{input}}"`.
- Downstream tasks (ingest_idsse_events, compute_pitch_control, etc.)
  reference the parent task name unchanged.

Tests (src/tests/test_idsse.py): 24 new tests across 5 classes.
TestParseMatchIdsArg (9), TestRunPipelineMatchIds (3), TestMainCliE2E
(3), TestIdsseGuardChunks (6), TestPreflightIdsse (3).

Out of scope (queued):
- Parser micro-optimization in `_parse_positions_xml` (Cycle B).
- Right-sizing the 5 other 900s ingest task timeouts (Cycle C).
- Generalizing this pattern to other workflows (TODO D40a).

Wheel: 0.3.23 → 0.3.24.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 3: Verify the commit**

Run:
```bash
git log -1 --stat
git status
```

Expected: single new commit on `feat/cycle-a-idsse-fanout`; working tree clean.

---

## Task 9 — 🛑 REQUEST USER APPROVAL TO PUSH + OPEN PR

**🛑 STOP HERE. Do NOT run `git push` or `gh pr create` without explicit user approval at this exact moment.**

- [ ] **Step 1: Surface the push plan to the user**

Output:
```
Commit ready on `feat/cycle-a-idsse-fanout`. Ready to push + open PR?
The PR will:
  - Title: "feat(ingestion): IDSSE for_each_task fan-out with runtime chunks (Cycle A — Phase H unblock)"
  - Base: main
  - CI runs: ruff/format/pyright/pytest + terraform validate
  - Expected outcome: green; user merges manually; wheel 0.3.24 auto-deploys to UC Volume.
```

Wait for explicit approval.

- [ ] **Step 2: After explicit approval, push the branch**

Run:
```bash
git push -u origin feat/cycle-a-idsse-fanout
```

- [ ] **Step 3: After explicit approval, create the PR**

Run:
```bash
gh pr create --title "feat(ingestion): IDSSE for_each_task fan-out with runtime chunks (Cycle A — Phase H unblock)" --body "$(cat <<'EOF'
## Summary

- Splits `ingest_idsse` from sequential 7-match (~45 min wall-clock per TODO D40d) to runtime-discovered parallel chunks of ≤2 matches each (~13 min/chunk), fitting the existing 900s timeout.
- Refactors `_IdsseGuard.check()` to populate `FilterResult.chunks` based on missing matches (anti-join of `IDSSE_MATCH_IDS` vs `bronze.idsse_tracking ∩ bronze.idsse_events`).
- Adds new `preflight_idsse` task that writes chunks via `dbutils.jobs.taskValues`; the `ingest_idsse` `for_each_task` consumes `{{tasks.preflight_idsse.values.idsse_match_chunks}}`.
- **No hardcoded chunks** — adding/removing matches just edits `IDSSE_MATCH_IDS`; Terraform unchanged.

## Why

Phase H bronze re-ingest blocked since 2026-04-30 by `ingest_idsse` 900s timeout breach (sequential 7-match parser breaches budget by ~3×). TODO D40d (2026-04-21) documented the cause; this PR implements the runtime-discovered fan-out pattern (Path 1 from the optimization audit) and establishes the prototype that D40a will extend to other workflows.

## Test plan
- [x] `uv run ruff check src/ scripts/` — clean
- [x] `uv run ruff format --check src/ scripts/` — clean
- [x] `uv run pyright src/` — zero errors
- [x] `uv run pytest src/tests/ -v` — all pass (24 new tests across 5 classes)
- [x] `terraform validate` — success
- [ ] Post-merge: user triggers manual job run; verify `preflight_idsse` writes the expected chunks; verify 4 (or fewer) `ingest_idsse_iteration_*` child tasks run in parallel
- [ ] Post-merge: Phase H bronze re-ingest unblocks; `validate_native_id_integrity.py` passes
- [ ] Post-merge: re-run on a no-op state (all 7 already done); verify 0 iterations spawn

## Out of scope (queued)

- Parser micro-optimization in `_parse_positions_xml` (Cycle B)
- Right-sizing the 5 other 900s ingest task timeouts + adding `dbt_build` retry (Cycle C)
- Generalizing the runtime-discovered fan-out to pitch_control / off-ball xT / SPADL-VAEP (TODO D40a)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 4: Surface the PR URL to the user**

Output the URL returned by `gh pr create`.

---

## Task 10 — Post-merge deploy verification (handoff to user)

This is a USER-DRIVEN task. Document what to do; do not execute.

- [ ] **Step 1: User merges PR via GitHub UI** (or `gh pr merge`).

- [ ] **Step 2: Wheel 0.3.24 auto-deploys to UC Volume** (Python CI on push to main; ~3 min).

- [ ] **Step 3: User triggers manual ingestion job run via Databricks UI**

The job is `soccer-analytics-ingestion-dev` (job_id 302697362345215). Trigger via UI or:

```bash
uv run --with databricks-sdk python -c "
import os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=os.environ['DATABRICKS_HOST'], token=os.environ['DATABRICKS_TOKEN'])
run = w.jobs.run_now(job_id=302697362345215)
print('Triggered run_id:', run.run_id)
"
```

- [ ] **Step 4: Watch `preflight_idsse` produce the chunks**

In the Databricks UI, the job DAG should now show:
- `preflight_idsse` (new) → succeeds in ~30 s
- `ingest_idsse` (for_each_task) — child iterations expand based on what preflight wrote
- `ingest_idsse_events` (unchanged) → depends on `ingest_idsse`

Click into `preflight_idsse` task output. Look for the log line:
```
IDSSE preflight: 7 missing matches across 4 chunks (chunk_size=2)
Wrote task value 'idsse_match_chunks' (4 chunks)
```

If `count=0` (no work), the message will say `0 missing matches across 0 chunks`; for_each_task spawns 0 iterations. That's correct behavior for a no-op run.

- [ ] **Step 5: Verify iterations run in parallel**

The `ingest_idsse` task should expand into N iterations (where N = number of chunks emitted by preflight). Each iteration's task name follows the Databricks `for_each_task` naming convention (e.g., `ingest_idsse_iteration_0`, `_1`, etc.).

Each iteration should:
- Show `--match-ids "J03WMX,J03WN1"` (or similar 2-match chunk) in its parameters
- Log `Restricted to chunk: ['J03WMX', 'J03WN1'] (2 matches)`
- Complete within ~13 min wall-clock (well under the 900s timeout)

Concurrency: up to 4 iterations run in parallel.

- [ ] **Step 6: Phase H follow-on**

Once `ingest_idsse` (and its `ingest_idsse_events` follow-on) succeed, re-run the rest of Phase H per `memory/project_session66_pr_ll2_path_b_close_out.md`:

- `validate_native_id_integrity.py`
- Phase I local dbt build with `--vars '{include_post_deploy_tests: true}'`
- Phase J (G9) backup-drop after 24 h stability (delayed past 2026-05-01 03:00 UTC per user approval; backups extended manually).

---

## Verification Checklist (final)

Before requesting user review:

- [ ] `uv run ruff check src/ scripts/` — zero violations
- [ ] `uv run ruff format --check src/ scripts/` — clean
- [ ] `uv run pyright src/` — zero errors
- [ ] `uv run pytest src/tests/ -v` — all pass (24 new tests + existing)
- [ ] `cd terraform/environments/dev && terraform validate` — success
- [ ] `cd terraform/environments/dev && terraform plan -target=module.workflows` — 1 resource update (only `databricks_job.data_ingestion`)
- [ ] No secrets/credentials in any changed file
- [ ] Wheel 0.3.23 → 0.3.24 propagated to all 22 stamp files (verified by `test_wheel_conformance.py`)
- [ ] Single commit on `feat/cycle-a-idsse-fanout` ready for push approval

---

## Self-Review Notes

**Spec coverage:**
- C1 (`ingest_idsse` 900 s timeout breach) → Tasks 5 + 10 (Terraform fan-out + post-merge verification).
- C3 (parser 2-pass + dict accumulator) → explicitly OUT OF SCOPE; deferred to Cycle B per user direction.
- TODO D40d (selective fan-out activation) → IDSSE-specific implementation as the prototype; broader activation (pitch_control, off-ball xT, SPADL-VAEP) remains in TODO with a clear pattern to follow.
- Drift risk between the chunk discovery and the data → eliminated by design: the guard reads the actual bronze tables every run. There is no static config to drift.
- E2E coverage of the CLI fan-out path → mitigated by `TestMainCliE2E` (3 integration-style tests).
- E2E coverage of the preflight task → mitigated by `TestPreflightIdsse` (3 tests verifying guard invocation + task-value shape + degraded local mode).

**Placeholder scan:** No TBDs. Every code step contains the actual code.

**Type consistency:** `match_ids: list[str] | None` used uniformly across `_parse_match_ids_arg` return type, `run_pipeline` keyword-only param, `ingest_idsse` (existing line 794), and `ingest_idsse_events` (existing line 1289 in idsse.py). `FilterResult.chunks: list[list[str]] | None` matches the existing field type in `src/ingestion/guards.py:42`. No drift.

**Git safety:**
- Every `git commit`, `git push`, `gh pr create` is gated by an explicit **🛑 REQUIRES USER APPROVAL** step (Tasks 8 + 9).
- No `--no-verify`, no force-push, no rebase, no amend.
- Single commit per PR (CLAUDE.md hard rule).

**Risk if executed:**
- `for_each_task` is first usage in this repo. If the Databricks provider version is older than expected, Task 0 Step 3 catches it before code changes.
- `dbutils.jobs.taskValues` is first programmatic write in this repo (it's been used in the wf-cost hook reads). If the runtime API has changed, the post-merge first run will fail at the preflight task; symptom is "task value not written" warning + 0 iterations spawned (downstream `ingest_idsse_events` then sees no work and skips). Easy to diagnose; fix by examining the preflight log.
- The iteration-level guard (`timed_check(skip_guard, ...)` inside `idsse:main`) is now redundant with the preflight (preflight already determined work exists). The iteration's guard returns count > 0 by construction (preflight wouldn't have emitted a chunk if not), so `run_pipeline` always proceeds. Slight observability double-count in `wf-idsse` workflow records but no correctness issue. Cleanup is post-Cycle-A.
- The `ingest_idsse_events` Terraform task remains at 900 s; events parser is fast (~7 s parsing + Spark write), so timeout is not a real risk. Right-sizing is Cycle C scope.

**Pattern reusability (for D40a):**
- Refactoring a guard to populate `FilterResult.chunks`: directly reusable.
- Per-workflow `main_preflight` entry point: each workflow can adopt the same shape (`parse_args → guard → write_task_values`).
- Per-workflow Terraform `preflight_X` task + `for_each_task` consumer: directly reusable.
- The `_write_match_chunks_task_value` helper takes a workflow-specific key name (`idsse_match_chunks`); for reusability, that should be parameterized when the second workflow adopts the pattern. **Cycle B will refactor it into a shared `ingestion.fanout_helpers.write_chunks_task_value(key: str, ...)` once a second consumer exists.** Premature abstraction with one consumer is a violation of YAGNI; keep IDSSE-specific in Cycle A.

---

**Plan complete.**
