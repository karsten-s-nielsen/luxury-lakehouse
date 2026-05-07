# Skip Guard Watermark & Freshness Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add watermark-based skip guards to 10 downstream Databricks tasks that currently always run, and fix the StatsBomb guard to check for new data before starting the heavy pipeline.

**Architecture:** New `check_upstream_freshness` / `record_watermarks` functions in `guards.py` use Delta `DESCRIBE HISTORY` metadata to compare current data-changing versions against stored watermarks in `observability.workflow_watermarks`. Upstream table lists are derived from workflow card `inputs.tables` (no separate dependency list). hf_sync sub-ops get factory wrappers; dbt_runner and refresh_synced_tables get new module-level guards.

**Tech Stack:** PySpark (Delta Lake DESCRIBE HISTORY), guards.py (FilterResult/timed_check), workflow cards (YAML), pytest (mocked Spark)

**Spec:** `docs/superpowers/specs/2026-05-06-skip-guard-watermark-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/ingestion/guards.py` | Modify | Add `check_upstream_freshness`, `record_watermarks`, `resolve_upstream_tables_from_card`, `_WATERMARKS_DDL`, `_DATA_CHANGING_OPS` |
| `src/ingestion/hf_sync.py` | Modify | Add `_make_watermark_op` and `_make_watermark_volume_op` factories; rewire 5 sub-ops |
| `src/ingestion/dbt_runner.py` | Modify | Add watermark guard class, `_SELECTOR_TO_CARD`, Spark session for guard |
| `src/ingestion/refresh_synced_tables.py` | Modify | Add watermark guard class deriving upstream from `SYNCED_TABLES` |
| `src/ingestion/model_validation.py` | Modify | Replace always-run guard body with watermark check |
| `src/ingestion/statsbomb.py` | Modify | Replace always-run guard with anti-join against `sb.competitions()` |
| `workflow-cards/wf-dbt-build-input-marts.yaml` | Modify | Expand `inputs.datasets` to full bronze source list |
| `workflow-cards/wf-dbt-build-intermediate-marts.yaml` | Modify | Expand `inputs.datasets` to full dependency list |
| `workflow-cards/wf-dbt-build-output-marts.yaml` | Modify | Expand `inputs.datasets` to full dependency list |
| `src/tests/test_watermark_freshness.py` | Create | Unit tests for `check_upstream_freshness` and `record_watermarks` |
| `src/tests/test_guard_conformance.py` | Modify | Add `TestWatermarkGuardHasCardInputs`, `TestWatermarkRecordAfterSuccess`, `TestSelectorToCardParity` |
| `src/tests/test_statsbomb_guard.py` | Create | Unit test for StatsBomb anti-join guard |

---

### Task 0: Cost quantification (Phase 0 — requires Databricks)

**Files:** None (measurement only)

This task requires Databricks SQL access and is best done by the operator before or in parallel with the code tasks. It is not a CI-blocking prerequisite.

- [ ] **Step 1: Query steady-state cost of the 10 target tasks**

Run on Databricks SQL warehouse:

```sql
SELECT
    workflow_id,
    COUNT(*) AS runs_30d,
    SUM(cost_usd) AS total_cost_30d,
    AVG(cost_usd) AS avg_cost_per_run
FROM soccer_analytics.observability.workflow_cost_live
WHERE workflow_id IN (
    'wf-publish-spadl-vaep', 'wf-publish-xg-shots', 'wf-publish-freeze-frames',
    'wf-export-shots', 'wf-scoutgpt-export', 'wf-model-validation',
    'wf-dbt-build-input-marts', 'wf-dbt-build-intermediate-marts',
    'wf-dbt-build-output-marts', 'wf-refresh-synced-tables'
)
AND recorded_at >= current_date() - INTERVAL 30 DAYS
GROUP BY workflow_id
ORDER BY total_cost_30d DESC
```

Record the baseline cost. This is the savings target.

---

### Task 1: Add watermark core functions to `guards.py`

**Files:**
- Create: `src/tests/test_watermark_freshness.py`
- Modify: `src/ingestion/guards.py:382` (append after existing code)

- [ ] **Step 1: Write failing tests for `check_upstream_freshness`**

Create `src/tests/test_watermark_freshness.py`:

```python
"""Unit tests for watermark-based skip guard functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ingestion.guards import FilterResult


class TestCheckUpstreamFreshness:
    """Tests for check_upstream_freshness."""

    def _make_mock_spark(
        self,
        *,
        history_rows: list[list] | None = None,
        watermark_rows: list[tuple[str, str, int]] | None = None,
        history_error: Exception | None = None,
    ) -> MagicMock:
        """Build a mock SparkSession that responds to DESCRIBE HISTORY and table reads."""
        spark = MagicMock()

        if history_error is not None:
            spark.sql.side_effect = history_error
            return spark

        # DESCRIBE HISTORY returns a DataFrame with 'operation' and 'version' columns
        if history_rows is not None:
            history_df = MagicMock()
            row_mocks = []
            for op, ver in history_rows:
                row = MagicMock()
                row.operation = op
                row.version = ver
                row_mocks.append(row)
            history_df.collect.return_value = row_mocks
            spark.sql.return_value = history_df

        return spark

    def test_first_run_no_stored_watermarks(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("WRITE", 5)])
        # Patch the watermark table read to return empty
        with patch("ingestion.guards._load_stored_watermarks", return_value={}):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(
                    spark, "catalog", "wf-test", ["catalog.schema.table_a"]
                )
        assert result.count == 1, "First run (no stored watermarks) should trigger"

    def test_all_versions_match_skips(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("WRITE", 5)])
        stored = {"catalog.schema.table_a": 5}
        with patch("ingestion.guards._load_stored_watermarks", return_value=stored):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(
                    spark, "catalog", "wf-test", ["catalog.schema.table_a"]
                )
        assert result.count == 0, "All versions match → skip"

    def test_one_upstream_changed_triggers(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(history_rows=[("WRITE", 7)])
        stored = {"catalog.schema.table_a": 5}
        with patch("ingestion.guards._load_stored_watermarks", return_value=stored):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(
                    spark, "catalog", "wf-test", ["catalog.schema.table_a"]
                )
        assert result.count == 1, "Version changed → trigger"

    def test_table_not_found_fails_open(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(
            history_error=Exception("TABLE_OR_VIEW_NOT_FOUND")
        )
        with patch("ingestion.guards._load_stored_watermarks", return_value={}):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(
                    spark, "catalog", "wf-test", ["catalog.schema.missing"]
                )
        assert result.count == 1, "Table not found → fail open"

    def test_only_optimize_vacuum_ops_with_stored_watermark_skips(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(
            history_rows=[("OPTIMIZE", 10), ("VACUUM END", 11)]
        )
        # Stored watermark at version 5 — only maintenance ops since then
        stored = {"catalog.schema.table_a": 5}
        with patch("ingestion.guards._load_stored_watermarks", return_value=stored):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(
                    spark, "catalog", "wf-test", ["catalog.schema.table_a"]
                )
        # Stored watermark exists + no data-changing ops → data unchanged → skip
        assert result.count == 0, "Stored watermark + only OPTIMIZE/VACUUM → skip"

    def test_only_optimize_vacuum_ops_no_stored_watermark_fails_open(self) -> None:
        from ingestion.guards import check_upstream_freshness

        spark = self._make_mock_spark(
            history_rows=[("OPTIMIZE", 10), ("VACUUM END", 11)]
        )
        # No stored watermark — first run
        with patch("ingestion.guards._load_stored_watermarks", return_value={}):
            with patch("ingestion.guards.ensure_table"):
                result = check_upstream_freshness(
                    spark, "catalog", "wf-test", ["catalog.schema.table_a"]
                )
        # No stored watermark + no data-changing ops → fail open
        assert result.count == 1, "No stored watermark + no data-changing ops → fail open"


class TestRecordWatermarks:
    """Tests for record_watermarks."""

    def test_records_current_versions(self) -> None:
        from ingestion.guards import record_watermarks

        spark = MagicMock()
        # DESCRIBE HISTORY returns version 7 for a WRITE op
        history_df = MagicMock()
        row = MagicMock()
        row.operation = "WRITE"
        row.version = 7
        history_df.collect.return_value = [row]
        spark.sql.return_value = history_df

        with patch("ingestion.guards.ensure_table"):
            record_watermarks(
                spark, "catalog", "wf-test", ["catalog.schema.table_a"]
            )

        # Verify MERGE was called with correct workflow_id, table, and version
        merge_calls = [
            str(call) for call in spark.sql.call_args_list
            if "MERGE" in str(call)
        ]
        assert len(merge_calls) == 1, "Should MERGE watermark record"
        merge_sql = merge_calls[0]
        assert "'wf-test'" in merge_sql, "MERGE should contain workflow_id"
        assert "'catalog.schema.table_a'" in merge_sql, "MERGE should contain table FQN"
        assert "7 AS last_seen_version" in merge_sql, "MERGE should contain version"


class TestResolveUpstreamTablesFromCard:
    """Tests for resolve_upstream_tables_from_card."""

    def _cards_dir(self) -> Path:
        """Resolve workflow-cards/ from repo root for test use."""
        from ingestion.guards import _repo_cards_dir

        return _repo_cards_dir()

    def test_resolves_placeholders(self) -> None:
        from ingestion.guards import resolve_upstream_tables_from_card

        result = resolve_upstream_tables_from_card(
            "wf-publish-spadl-vaep", "soccer_analytics", "dev_gold",
            cards_dir=self._cards_dir(),
        )
        assert "soccer_analytics.dev_gold.fct_action_values" in result

    def test_filters_delta_table_source_only(self) -> None:
        from ingestion.guards import resolve_upstream_tables_from_card

        result = resolve_upstream_tables_from_card(
            "wf-publish-spadl-vaep", "soccer_analytics", "dev_gold",
            cards_dir=self._cards_dir(),
        )
        # All returned entries should be fully-qualified table names
        for table in result:
            assert table.count(".") >= 2, f"Expected FQN, got {table}"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest src/tests/test_watermark_freshness.py -v 2>&1 | tail -20`
Expected: FAIL — `check_upstream_freshness`, `record_watermarks`, `resolve_upstream_tables_from_card` not defined

- [ ] **Step 3: Implement watermark functions in `guards.py`**

Add the following after the existing code at the end of `src/ingestion/guards.py` (after line 382):

```python
# ---------------------------------------------------------------------------
# Watermark-based skip guard — "has any upstream Delta table changed?"
# ---------------------------------------------------------------------------

_DATA_CHANGING_OPS: frozenset[str] = frozenset({
    "WRITE",
    "MERGE",
    "DELETE",
    "UPDATE",
    "CREATE TABLE AS SELECT",
    "CREATE OR REPLACE TABLE AS SELECT",
    "RESTORE",
})

# Logical PK: (workflow_id, upstream_table) — enforced by MERGE ON clause,
# not by Delta constraints (Delta Lake does not enforce PKs at write time).
_WATERMARKS_DDL = (
    "workflow_id STRING NOT NULL, upstream_table STRING NOT NULL, "
    "last_seen_version BIGINT NOT NULL, checked_at TIMESTAMP NOT NULL"
)


def _load_stored_watermarks(
    spark: SparkSession,
    watermarks_table: str,
    workflow_id: str,
) -> dict[str, int]:
    """Load stored watermarks for a workflow. Returns {table_fqn: version}."""
    rows = spark.sql(
        f"SELECT upstream_table, last_seen_version "  # noqa: S608
        f"FROM {watermarks_table} "
        f"WHERE workflow_id = '{workflow_id}'"
    ).collect()
    return {row.upstream_table: row.last_seen_version for row in rows}


def _get_latest_data_version(
    spark: SparkSession,
    table: str,
) -> int | None:
    """Get the latest data-changing version from DESCRIBE HISTORY.

    Returns None if no data-changing operations found.
    """
    rows = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 20").collect()
    data_versions = [
        row.version for row in rows if row.operation in _DATA_CHANGING_OPS
    ]
    return max(data_versions) if data_versions else None


def check_upstream_freshness(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    upstream_tables: list[str],
) -> FilterResult:
    """Check if any upstream Delta table has changed since last recorded watermark.

    Returns ``FilterResult(count=0)`` if all upstream tables are at the same
    version as the last recorded watermark.  Returns ``count=1`` (fail open)
    on first run, version mismatch, or any error.
    """
    watermarks_table = f"{catalog}.observability.workflow_watermarks"
    ensure_table(spark, watermarks_table, _WATERMARKS_DDL)

    stored = _load_stored_watermarks(spark, watermarks_table, workflow_id)

    for table in upstream_tables:
        try:
            current_version = _get_latest_data_version(spark, table)
        except Exception:  # noqa: BLE001 — fail open on DESCRIBE HISTORY errors
            logger.warning("DESCRIBE HISTORY failed for %s — failing open", table)
            return FilterResult(workflow_id=workflow_id, count=1)

        stored_version = stored.get(table)

        if current_version is None:
            # No data-changing ops in the last 20 history entries.
            # If we have a stored watermark, the data hasn't changed — skip.
            # If no stored watermark (first run), fail open.
            if stored_version is None:
                return FilterResult(workflow_id=workflow_id, count=1)
            continue

        if stored_version is None or current_version != stored_version:
            return FilterResult(workflow_id=workflow_id, count=1)

    return FilterResult(workflow_id=workflow_id, count=0)


def record_watermarks(
    spark: SparkSession,
    catalog: str,
    workflow_id: str,
    upstream_tables: list[str],
) -> None:
    """Record current upstream versions after successful pipeline completion."""
    watermarks_table = f"{catalog}.observability.workflow_watermarks"
    ensure_table(spark, watermarks_table, _WATERMARKS_DDL)

    for table in upstream_tables:
        current_version = _get_latest_data_version(spark, table)
        # If no data-changing ops in recent history, record version 0 as sentinel.
        # This prevents a livelock where the guard perpetually fails open because
        # no watermark is ever stored for rarely-updated tables.
        if current_version is None:
            current_version = 0
        spark.sql(
            f"MERGE INTO {watermarks_table} AS target "  # noqa: S608
            f"USING (SELECT '{workflow_id}' AS workflow_id, "
            f"'{table}' AS upstream_table, "
            f"{current_version} AS last_seen_version, "
            f"current_timestamp() AS checked_at) AS source "
            f"ON target.workflow_id = source.workflow_id "
            f"AND target.upstream_table = source.upstream_table "
            f"WHEN MATCHED THEN UPDATE SET "
            f"target.last_seen_version = source.last_seen_version, "
            f"target.checked_at = source.checked_at "
            f"WHEN NOT MATCHED THEN INSERT *"
        )


def resolve_upstream_tables_from_card(
    workflow_id: str,
    catalog: str,
    schema: str,
    cards_dir: Path | None = None,
) -> list[str]:
    """Load upstream Delta table FQNs from a workflow card's inputs section.

    Reads ``inputs.tables`` and ``inputs.datasets`` entries where
    ``source == "delta-table"``, substitutes ``{catalog}`` and ``{schema}``
    placeholders in the ``id`` field, and returns the resolved list.

    ``cards_dir`` defaults to the Databricks Workspace Repos path
    (``/Workspace/Repos/luxury-lakehouse/workflow-cards``).  workflow-cards/
    is NOT bundled in the wheel — it lives at the repo root and is available
    on Databricks via the Repos integration.  Tests and local callers must
    pass an explicit ``cards_dir``.
    """
    if cards_dir is None:
        cards_dir = Path("/Workspace/Repos/luxury-lakehouse/workflow-cards")

    card_path = cards_dir / f"{workflow_id}.yaml"
    with open(card_path, encoding="utf-8") as f:
        import yaml

        # Workflow cards have YAML front matter delimited by ---
        content = f.read()
        # Split on --- and take the first YAML document
        parts = content.split("---")
        if len(parts) >= 3:
            card = yaml.safe_load(parts[1])
        else:
            card = yaml.safe_load(content)

    tables: list[str] = []
    inputs = card.get("inputs", {})
    for section in ("tables", "datasets"):
        for entry in inputs.get(section, []):
            if entry.get("source") == "delta-table":
                fqn = entry["id"].replace("{catalog}", catalog).replace("{schema}", schema)
                tables.append(fqn)
    return tables


def _repo_cards_dir() -> Path:
    """Resolve workflow-cards/ from the repo root (for local/test use)."""
    return Path(__file__).resolve().parent.parent.parent / "workflow-cards"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest src/tests/test_watermark_freshness.py -v 2>&1 | tail -20`
Expected: All 9 tests PASS

- [ ] **Step 5: Run ruff and pyright**

Run: `uv run ruff check src/ingestion/guards.py src/tests/test_watermark_freshness.py 2>&1 | tail -5`
Run: `uv run pyright src/ingestion/guards.py 2>&1 | tail -5`
Expected: Zero violations

---

### Task 2: Wire watermark guards into hf_sync sub-operations

**Files:**
- Modify: `src/ingestion/hf_sync.py:77-134`

- [ ] **Step 1: Add the two watermark factory functions**

In `src/ingestion/hf_sync.py`, add the new imports at the top (after line 16):

```python
from ingestion.guards import (
    FilterResult,
    check_upstream_freshness,
    record_watermarks,
    resolve_upstream_tables_from_card,
    timed_check,
)
```

Replace line 16 (`from ingestion.guards import FilterResult, timed_check`) with the expanded import above.

Then add two new factory functions after `_make_plain_op` (after line 96):

```python
def _make_watermark_op(module_path: str, card_id: str) -> Callable[..., None]:
    """Create a watermark-guarded sub-operation for modules with (spark, catalog, schema, logger)."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        upstream = resolve_upstream_tables_from_card(card_id, catalog, schema)
        fr = check_upstream_freshness(spark, catalog, card_id, upstream)
        if fr.count == 0:
            logger_arg.info("Watermark skip: %s — no upstream changes", card_id)
            return
        mod = importlib.import_module(module_path)
        mod.run_pipeline(spark, catalog, schema, logger_arg)
        record_watermarks(spark, catalog, card_id, upstream)

    _call.__qualname__ = f"_call[{module_path}]"
    return _call


def _make_watermark_volume_op(module_path: str, card_id: str) -> Callable[..., None]:
    """Create a watermark-guarded sub-operation for modules with (spark, catalog, schema, volume_path).

    Uses the existing ``_VOLUME_PATHS`` dict (line 43 of hf_sync.py) to resolve
    the UC Volume path for the module.  This dict already maps
    ``"ingestion.export_shots_on_target"`` → the psxg Volume path.
    """

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        upstream = resolve_upstream_tables_from_card(card_id, catalog, schema)
        fr = check_upstream_freshness(spark, catalog, card_id, upstream)
        if fr.count == 0:
            logger_arg.info("Watermark skip: %s — no upstream changes", card_id)
            return
        mod = importlib.import_module(module_path)
        volume_path = _VOLUME_PATHS[module_path]
        mod.run_pipeline(spark, catalog, schema, volume_path)
        record_watermarks(spark, catalog, card_id, upstream)

    _call.__qualname__ = f"_call[{module_path}]"
    return _call
```

- [ ] **Step 2: Rewire `_SUB_OPERATIONS` list**

Replace the 5 guardless sub-op entries in `_SUB_OPERATIONS` (lines 127-132):

```python
_SUB_OPERATIONS: list[tuple[str, Callable[..., None]]] = [
    ("ingestion.import_space_creation", _make_volume_op("ingestion.import_space_creation")),
    ("ingestion.import_psxg_predictions", _make_volume_op("ingestion.import_psxg_predictions")),
    ("ingestion.export_embeddings_training_data", _make_logger_op("ingestion.export_embeddings_training_data")),
    ("ingestion.export_shots_on_target", _make_watermark_volume_op("ingestion.export_shots_on_target", "wf-export-shots")),
    ("ingestion.prepare_360_training_data", _make_volume_op("ingestion.prepare_360_training_data")),
    ("ingestion.export_scoutgpt_training_data", _make_watermark_op("ingestion.export_scoutgpt_training_data", "wf-scoutgpt-export")),
    ("ingestion.publish_spadl_vaep_hf", _make_watermark_op("ingestion.publish_spadl_vaep_hf", "wf-publish-spadl-vaep")),
    ("ingestion.publish_xg_shots_hf", _make_watermark_op("ingestion.publish_xg_shots_hf", "wf-publish-xg-shots")),
    ("ingestion.publish_freeze_frame_hf", _make_watermark_op("ingestion.publish_freeze_frame_hf", "wf-publish-freeze-frames")),
    ("ingestion.sync_hf_costs", _make_sync_costs_op()),
]
```

- [ ] **Step 3: Verify ruff and pyright**

Run: `uv run ruff check src/ingestion/hf_sync.py 2>&1 | tail -5`
Run: `uv run pyright src/ingestion/hf_sync.py 2>&1 | tail -5`
Expected: Zero violations

---

### Task 3: Add watermark guard to `dbt_runner.py`

**Files:**
- Modify: `src/ingestion/dbt_runner.py:1-297`

- [ ] **Step 1: Add imports and guard class**

Add after the existing imports (after line 40 of `dbt_runner.py`):

```python
from ingestion.guards import (
    FilterResult,
    check_upstream_freshness,
    record_watermarks,
    resolve_upstream_tables_from_card,
    timed_check,
)
from ingestion.utils import get_spark_session

# Keys are frozensets of selector tags so lookup is order-independent.
# The selector parser normalizes by stripping whitespace and sorting.
_SELECTOR_TO_CARD: dict[frozenset[str], str] = {
    frozenset({"+tag:input_mart", "+tag:dimension"}): "wf-dbt-build-input-marts",
    frozenset({"+tag:intermediate_mart"}): "wf-dbt-build-intermediate-marts",
    frozenset({"tag:output_mart"}): "wf-dbt-build-output-marts",
}


class _DbtWatermarkGuard:
    """Watermark guard parameterized by workflow card ID."""

    def __init__(self, card_id: str) -> None:
        self.workflow_id = card_id

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        return check_upstream_freshness(spark, catalog, self.workflow_id, upstream)
```

Note: `SparkSession` needs a TYPE_CHECKING import. Add `from pyspark.sql import SparkSession` inside the existing `if TYPE_CHECKING:` block (or add the block if not present). Check the existing file structure first.

- [ ] **Step 2: Wire guard into `main()`**

Replace the `main()` function (lines 272-292) with:

```python
def main() -> int:
    """CLI entry point for the Databricks ``python_wheel_task``.

    Forwards any ``sys.argv[1:]`` arguments to dbt (e.g. ``--select dim_competitions``
    for a diagnostic single-model build). The Databricks ``python_wheel_task``
    `parameters` array becomes ``sys.argv[1:]`` when the wheel entry point runs.

    Returns 0 on success. On failure, the underlying ``RuntimeError`` from
    ``run_pipeline`` propagates out of this function — Databricks treats an
    uncaught exception in a ``python_wheel_task`` as task failure, but a
    function that returns ``1`` is silently treated as success. Do NOT catch
    here. Returning a non-zero int does NOT fail the task.

    NOTE: ``@workflow`` is intentionally NOT applied. dbt_runner invokes dbt
    via ``dbtRunner().invoke(args)`` — no Spark infrastructure, no Delta writes.
    The watermark guard below is the only Spark touchpoint, used purely for
    metadata reads (DESCRIBE HISTORY + watermarks table).
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    extra_args = sys.argv[1:] if len(sys.argv) > 1 else None

    # Resolve selector from args to find matching workflow card
    selector_str = ""
    if extra_args:
        select_args: list[str] = []
        capture = False
        for arg in extra_args:
            if arg == "--select":
                capture = True
                continue
            if capture and not arg.startswith("--"):
                select_args.append(arg)
            else:
                capture = False
        selector_str = " ".join(select_args)

    card_id = _SELECTOR_TO_CARD.get(frozenset(selector_str.split()) if selector_str else frozenset())
    spark: SparkSession | None = None
    if card_id is not None:
        spark = get_spark_session()
        guard = _DbtWatermarkGuard(card_id)
        fr = timed_check(guard, spark, "soccer_analytics", "dev_gold")
        if fr.count == 0:
            logger.info("Watermark skip: %s — no upstream changes", card_id)
            return 0

    run_pipeline(extra_args=extra_args)

    # Record watermarks after successful dbt build
    if card_id is not None and spark is not None:
        upstream = resolve_upstream_tables_from_card(card_id, "soccer_analytics", "dev_gold")
        record_watermarks(spark, "soccer_analytics", card_id, upstream)

    return 0
```

- [ ] **Step 3: Add module-level `skip_guard` for `_GUARD_MODULES` registration**

After the `_DbtWatermarkGuard` class, add:

```python
# Default guard for _GUARD_MODULES registration — uses the first dbt stage.
# The actual guard in main() is parameterized per --select value.
skip_guard = _DbtWatermarkGuard("wf-dbt-build-input-marts")
```

- [ ] **Step 4: Register in `_GUARD_MODULES`**

In `src/ingestion/guards.py`, add `"ingestion.dbt_runner"` to the `_GUARD_MODULES` list (after `"ingestion.model_validation"`):

```python
    "ingestion.dbt_runner",
```

- [ ] **Step 5: Verify ruff and pyright**

Run: `uv run ruff check src/ingestion/dbt_runner.py 2>&1 | tail -5`
Run: `uv run pyright src/ingestion/dbt_runner.py 2>&1 | tail -5`
Expected: Zero violations

---

### Task 4: Add watermark guard to `refresh_synced_tables.py`

**Files:**
- Modify: `src/ingestion/refresh_synced_tables.py:1-661`

- [ ] **Step 1: Add imports and guard class**

Add after the existing imports (find the import section near the top):

```python
from ingestion.guards import (
    FilterResult,
    check_upstream_freshness,
    record_watermarks,
    timed_check,
)
from ingestion.utils import get_spark_session
```

Add the guard class before `main()` (before line 518):

```python
class _RefreshSyncedTablesGuard:
    """Watermark guard that derives upstream tables from SYNCED_TABLES."""

    workflow_id = "wf-refresh-synced-tables"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        upstream = _derive_upstream_tables(catalog, schema)
        return check_upstream_freshness(spark, catalog, self.workflow_id, upstream)


def _derive_upstream_tables(catalog: str, default_schema: str) -> list[str]:
    """Derive upstream Delta table FQNs from SYNCED_TABLES.

    For each ``(table_name, schema_override)`` tuple, strips the ``_synced``
    suffix and qualifies with the override schema (or default).
    """
    tables: list[str] = []
    for synced_name, schema_override in SYNCED_TABLES:
        base = synced_name.removesuffix("_synced")
        effective_schema = schema_override or default_schema
        tables.append(f"{catalog}.{effective_schema}.{base}")
    return tables


skip_guard = _RefreshSyncedTablesGuard()
```

- [ ] **Step 2: Wire guard into `main()`**

In `main()`, after arg parsing and validation (after the `table_schema_map` construction around line 590), add the guard check:

```python
    # Watermark guard — skip if no upstream table has changed.
    # This module was historically Spark-free (pure REST API client).
    # The guard requires a Spark session for DESCRIBE HISTORY; if Spark is
    # unavailable (e.g., manual CLI invocation outside Databricks), fail open.
    try:
        spark = get_spark_session()
        fr = timed_check(skip_guard, spark, args.catalog, args.schema)
        if fr.count == 0:
            logger.info("Watermark skip: no upstream changes since last refresh")
            return
    except Exception:  # noqa: BLE001 — fail open outside Databricks
        logger.warning("Spark unavailable for watermark check — proceeding with refresh")
        spark = None
```

At the end of `main()`, after the successful refresh loop completes (before the exit code check), add:

```python
    # Record watermarks after successful refresh (only if Spark was available)
    if errors == 0 and spark is not None:
        upstream = _derive_upstream_tables(args.catalog, args.schema)
        record_watermarks(spark, args.catalog, skip_guard.workflow_id, upstream)
```

- [ ] **Step 3: Add unit test for `_derive_upstream_tables`**

Append to `src/tests/test_watermark_freshness.py`:

```python
class TestDeriveUpstreamTables:
    """Tests for _derive_upstream_tables in refresh_synced_tables."""

    def test_strips_synced_suffix_default_schema(self) -> None:
        from ingestion.refresh_synced_tables import _derive_upstream_tables

        # Monkeypatch SYNCED_TABLES to a known list for isolation
        import ingestion.refresh_synced_tables as mod

        original = mod.SYNCED_TABLES
        mod.SYNCED_TABLES = [("fct_shots_synced", None), ("dim_players_synced", None)]
        try:
            result = _derive_upstream_tables("cat", "gold")
        finally:
            mod.SYNCED_TABLES = original

        assert result == ["cat.gold.fct_shots", "cat.gold.dim_players"]

    def test_applies_schema_override(self) -> None:
        from ingestion.refresh_synced_tables import _derive_upstream_tables

        import ingestion.refresh_synced_tables as mod

        original = mod.SYNCED_TABLES
        mod.SYNCED_TABLES = [
            ("workflow_cost_live_synced", "observability"),
            ("fct_shots_synced", None),
        ]
        try:
            result = _derive_upstream_tables("cat", "gold")
        finally:
            mod.SYNCED_TABLES = original

        assert result == [
            "cat.observability.workflow_cost_live",
            "cat.gold.fct_shots",
        ]
```

Run: `uv run pytest src/tests/test_watermark_freshness.py::TestDeriveUpstreamTables -v 2>&1 | tail -10`
Expected: All PASS

- [ ] **Step 4: Register in `_GUARD_MODULES`**

In `src/ingestion/guards.py`, add `"ingestion.refresh_synced_tables"` to the `_GUARD_MODULES` list:

```python
    "ingestion.refresh_synced_tables",
```

- [ ] **Step 5: Verify ruff and pyright**

Run: `uv run ruff check src/ingestion/refresh_synced_tables.py 2>&1 | tail -5`
Run: `uv run pyright src/ingestion/refresh_synced_tables.py 2>&1 | tail -5`
Expected: Zero violations

---

### Task 5: Replace model_validation always-run guard with watermark

**Files:**
- Modify: `src/ingestion/model_validation.py:43-54`

- [ ] **Step 1: Replace guard body**

Replace `_ModelValidationGuard.check()` (lines 46-51):

```python
    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        from ingestion.guards import (
            check_upstream_freshness,
            ensure_table,
            resolve_upstream_tables_from_card,
        )

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)
        upstream = resolve_upstream_tables_from_card(self.workflow_id, catalog, schema)
        return check_upstream_freshness(spark, catalog, self.workflow_id, upstream)
```

- [ ] **Step 2: Add `record_watermarks` call after successful run**

Find the `main()` function. After the `run_pipeline(...)` call, add watermark recording:

```python
    # Record watermarks after successful validation
    from ingestion.guards import record_watermarks, resolve_upstream_tables_from_card
    upstream = resolve_upstream_tables_from_card(skip_guard.workflow_id, args.catalog, args.schema)
    record_watermarks(spark, args.catalog, skip_guard.workflow_id, upstream)
```

- [ ] **Step 3: Verify ruff and pyright**

Run: `uv run ruff check src/ingestion/model_validation.py 2>&1 | tail -5`
Expected: Zero violations

---

### Task 6: Fix StatsBomb guard — anti-join against `sb.competitions()`

**Files:**
- Create: `src/tests/test_statsbomb_guard.py`
- Modify: `src/ingestion/statsbomb.py:83-90`

- [ ] **Step 1: Write failing test**

Create `src/tests/test_statsbomb_guard.py`:

```python
"""Unit tests for StatsBomb anti-join skip guard."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.guards import FilterResult


class TestStatsbombGuard:
    """Verify the anti-join guard skips when no new competitions/matches exist."""

    def test_no_new_data_skips(self) -> None:
        """When all competitions and matches exist in bronze, count=0."""
        from ingestion.statsbomb import skip_guard

        spark = MagicMock()

        # Mock sb.competitions() returning known competitions
        competitions_df = pd.DataFrame({
            "competition_id": [1, 2],
            "season_id": [10, 20],
        })

        # Mock bronze competitions table — same as API
        bronze_comps = MagicMock()
        bronze_comps.select.return_value = bronze_comps
        bronze_comps.toPandas.return_value = pd.DataFrame({
            "competition_id": [1, 2],
            "season_id": [10, 20],
        })

        # Mock bronze matches table — same match IDs as API
        bronze_matches = MagicMock()
        bronze_matches.select.return_value = bronze_matches
        bronze_matches.toPandas.return_value = pd.DataFrame({
            "match_id": [100, 101, 200, 201],
        })

        # sb.matches() returns same match IDs per sampled competition
        matches_comp1 = pd.DataFrame({"match_id": [100, 101]})
        matches_comp2 = pd.DataFrame({"match_id": [200, 201]})

        def _mock_table(name: str) -> MagicMock:
            if "competitions" in name:
                return bronze_comps
            if "matches" in name:
                return bronze_matches
            return MagicMock()

        spark.table.side_effect = _mock_table

        with patch("ingestion.statsbomb.sb") as mock_sb:
            mock_sb.competitions.return_value = competitions_df
            mock_sb.matches.side_effect = [matches_comp1, matches_comp2]
            result = skip_guard.check(spark, "catalog", "bronze")

        assert result.count == 0, "No new data → skip"

    def test_new_competition_triggers(self) -> None:
        """When a new competition exists, count=1."""
        from ingestion.statsbomb import skip_guard

        spark = MagicMock()

        # sb.competitions() returns 3 competitions (one new)
        competitions_df = pd.DataFrame({
            "competition_id": [1, 2, 3],
            "season_id": [10, 20, 30],
        })

        # Bronze has only 2
        bronze_comps = MagicMock()
        bronze_comps.toPandas.return_value = pd.DataFrame({
            "competition_id": [1, 2],
            "season_id": [10, 20],
        })

        spark.table.side_effect = lambda name: bronze_comps if "competitions" in name else MagicMock()

        with patch("ingestion.statsbomb.sb") as mock_sb:
            mock_sb.competitions.return_value = competitions_df
            result = skip_guard.check(spark, "catalog", "bronze")

        assert result.count == 1, "New competition → trigger"

    def test_new_matches_in_existing_competition_triggers(self) -> None:
        """When competitions match but new matches exist, count=1."""
        from ingestion.statsbomb import skip_guard

        spark = MagicMock()

        # Same competitions in API and bronze
        competitions_df = pd.DataFrame({
            "competition_id": [1, 2],
            "season_id": [10, 20],
        })
        bronze_comps = MagicMock()
        bronze_comps.toPandas.return_value = pd.DataFrame({
            "competition_id": [1, 2],
            "season_id": [10, 20],
        })

        # sb.matches returns 3 matches for comp 1; bronze has 2
        matches_df = pd.DataFrame({"match_id": [100, 101, 102]})

        bronze_matches = MagicMock()
        bronze_matches.filter.return_value = bronze_matches
        bronze_matches.select.return_value = bronze_matches
        bronze_matches.toPandas.return_value = pd.DataFrame({"match_id": [100, 101]})

        def _mock_table(name: str) -> MagicMock:
            if "competitions" in name:
                return bronze_comps
            if "matches" in name:
                return bronze_matches
            return MagicMock()

        spark.table.side_effect = _mock_table

        with patch("ingestion.statsbomb.sb") as mock_sb:
            mock_sb.competitions.return_value = competitions_df
            mock_sb.matches.return_value = matches_df
            result = skip_guard.check(spark, "catalog", "bronze")

        assert result.count == 1, "New matches in existing competition → trigger"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest src/tests/test_statsbomb_guard.py -v 2>&1 | tail -15`
Expected: FAIL — current guard always returns `count=1`

- [ ] **Step 3: Implement anti-join guard**

Replace the `_StatsbombGuard` class in `src/ingestion/statsbomb.py` (lines 83-90):

```python
class _StatsbombGuard:
    workflow_id = "wf-statsbomb"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Anti-join against sb.competitions() to find new competition/season pairs.

        Fetches the competitions JSON from the StatsBomb open-data GitHub repo
        (raw.githubusercontent.com). Unauthenticated GitHub rate limit is
        60 req/hour; one guard check per daily scheduled run is well within this.
        """
        try:
            api_comps = sb.competitions()
        except Exception:  # noqa: BLE001 — fail open if GitHub is unreachable
            logger.warning("sb.competitions() failed — failing open")
            return FilterResult(workflow_id=self.workflow_id, count=1)

        # Load existing bronze competitions
        from ingestion.utils import tolerate_missing_table

        bronze_comps_df = None
        with tolerate_missing_table(logger, "statsbomb_competitions not found — first run"):
            bronze_comps_df = spark.table(
                f"{catalog}.{schema}.statsbomb_competitions"
            ).select("competition_id", "season_id").toPandas()

        if bronze_comps_df is None:
            return FilterResult(workflow_id=self.workflow_id, count=1)

        # Anti-join: find competitions in API but not in bronze
        api_keys = set(zip(api_comps["competition_id"], api_comps["season_id"], strict=False))
        bronze_keys = set(zip(bronze_comps_df["competition_id"], bronze_comps_df["season_id"], strict=False))
        new_comps = api_keys - bronze_keys

        if new_comps:
            return FilterResult(
                workflow_id=self.workflow_id,
                count=1,
                metadata={"new_competitions": [f"{c}_{s}" for c, s in new_comps]},
            )

        # Check for new matches within existing competitions.
        # sb.matches() fetches per competition/season — sample up to 3 existing
        # competition/season pairs to check for new match days.
        # Rate-limit budget: 1 (competitions) + 3 (matches) = 4 unauthenticated
        # GitHub raw requests per guard invocation.  Limit is 60 req/hour.
        # Do NOT increase the 3-pair ceiling without verifying daily run cadence.
        bronze_matches_df = None
        with tolerate_missing_table(logger, "statsbomb_matches not found"):
            bronze_matches_df = spark.table(
                f"{catalog}.{schema}.statsbomb_matches"
            ).select("match_id").toPandas()

        if bronze_matches_df is not None:
            bronze_match_ids = set(bronze_matches_df["match_id"])
            for comp_id, season_id in list(bronze_keys)[:3]:
                try:
                    api_matches = sb.matches(competition_id=comp_id, season_id=season_id)
                except Exception:  # noqa: BLE001 — fail open
                    continue
                api_match_ids = set(api_matches["match_id"])
                new_matches = api_match_ids - bronze_match_ids
                if new_matches:
                    return FilterResult(
                        workflow_id=self.workflow_id,
                        count=1,
                        metadata={"new_match_ids": [str(m) for m in list(new_matches)[:5]]},
                    )

        return FilterResult(workflow_id=self.workflow_id, count=0)


skip_guard = _StatsbombGuard()
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest src/tests/test_statsbomb_guard.py -v 2>&1 | tail -15`
Expected: All PASS

- [ ] **Step 5: Run ruff**

Run: `uv run ruff check src/ingestion/statsbomb.py 2>&1 | tail -5`
Expected: Zero violations

---

### Task 7: Expand dbt workflow card inputs

**Files:**
- Modify: `workflow-cards/wf-dbt-build-input-marts.yaml:23-27`
- Modify: `workflow-cards/wf-dbt-build-intermediate-marts.yaml` (inputs section)
- Modify: `workflow-cards/wf-dbt-build-output-marts.yaml` (inputs section)

**Note:** The 5 existing publisher cards (`wf-publish-spadl-vaep`, `wf-publish-xg-shots`, `wf-publish-freeze-frames`, `wf-export-shots`, `wf-scoutgpt-export`) and `wf-model-validation` already have correct `inputs.tables`/`inputs.datasets` sections with `source: delta-table` entries. No expansion needed — verified against the live YAML files.

**Prerequisite:** Before editing, run `dbt ls` to derive the authoritative source list. If `dbt ls` is not available locally (requires dbt extra + Databricks credentials), use the best-effort lists from the spec and add a `# TODO: verify via dbt ls` comment.

- [ ] **Step 1: Expand wf-dbt-build-input-marts.yaml inputs**

Replace the `inputs:` section (lines 23-27):

```yaml
inputs:
  datasets:
    - id: "{catalog}.bronze.statsbomb_events"
      source: delta-table
      description: "StatsBomb events"
    - id: "{catalog}.bronze.statsbomb_360"
      source: delta-table
      description: "StatsBomb 360 freeze-frame data"
    - id: "{catalog}.bronze.statsbomb_lineups"
      source: delta-table
      description: "StatsBomb lineups"
    - id: "{catalog}.bronze.statsbomb_competitions"
      source: delta-table
      description: "StatsBomb competitions"
    - id: "{catalog}.bronze.statsbomb_matches"
      source: delta-table
      description: "StatsBomb matches"
    - id: "{catalog}.bronze.metrica_tracking"
      source: delta-table
      description: "Metrica tracking data"
    - id: "{catalog}.bronze.metrica_events"
      source: delta-table
      description: "Metrica events"
    - id: "{catalog}.bronze.idsse_tracking"
      source: delta-table
      description: "IDSSE (DFL) tracking data"
    - id: "{catalog}.bronze.idsse_events"
      source: delta-table
      description: "IDSSE (DFL) events"
    - id: "{catalog}.bronze.skillcorner_tracking"
      source: delta-table
      description: "SkillCorner tracking data"
    - id: "{catalog}.bronze.wyscout_events"
      source: delta-table
      description: "Wyscout events"
    - id: "{catalog}.bronze.wyscout_matches"
      source: delta-table
      description: "Wyscout matches"
    - id: "{catalog}.bronze.wyscout_players"
      source: delta-table
      description: "Wyscout players"
    - id: "{catalog}.bronze.wyscout_teams"
      source: delta-table
      description: "Wyscout teams"
    - id: "{catalog}.bronze.player_xref_raw"
      source: delta-table
      description: "Player cross-reference (entity resolution input)"
    - id: "{catalog}.bronze.tracking_player_metadata"
      source: delta-table
      description: "Tracking player metadata"
```

- [ ] **Step 2: Expand wf-dbt-build-intermediate-marts.yaml inputs**

Replace the inputs section with the stage 1 gold tables (input-mart outputs that intermediate marts depend on) + compute bronze tables:

```yaml
inputs:
  datasets:
    # Stage 1 gold outputs (from input-marts dbt build)
    - id: "{catalog}.{schema}.dim_competitions"
      source: delta-table
      description: "Competition dimension (input mart output)"
    - id: "{catalog}.{schema}.dim_matches"
      source: delta-table
      description: "Match dimension (input mart output)"
    - id: "{catalog}.{schema}.dim_players"
      source: delta-table
      description: "Player dimension (input mart output)"
    - id: "{catalog}.{schema}.dim_teams"
      source: delta-table
      description: "Team dimension (input mart output)"
    - id: "{catalog}.{schema}.fct_shots"
      source: delta-table
      description: "Shots fact table (input mart output)"
    - id: "{catalog}.{schema}.fct_action_values"
      source: delta-table
      description: "Action values fact table (input mart output)"
    # Compute bronze tables consumed by intermediate staging models
    - id: "{catalog}.bronze.spadl_actions"
      source: delta-table
      description: "SPADL actions from compute_spadl_vaep"
    - id: "{catalog}.bronze.vaep_action_values"
      source: delta-table
      description: "VAEP action values from compute_spadl_vaep"
```

- [ ] **Step 3: Expand wf-dbt-build-output-marts.yaml inputs**

Replace the inputs section with stage 2 gold + compute bronze tables:

```yaml
inputs:
  datasets:
    - id: "{catalog}.bronze.line_breaking_results"
      source: delta-table
      description: "Line-breaking detection results"
    - id: "{catalog}.bronze.pitch_control_values"
      source: delta-table
      description: "Pitch control computation results"
    - id: "{catalog}.bronze.off_ball_xt_results"
      source: delta-table
      description: "Off-ball expected threat results"
    - id: "{catalog}.bronze.defcon_results"
      source: delta-table
      description: "DEFCON defensive contribution results"
    - id: "{catalog}.bronze.expected_threat_grids"
      source: delta-table
      description: "Expected threat grid values"
    - id: "{catalog}.bronze.formations_efpi_results"
      source: delta-table
      description: "Formation EFPI results"
    - id: "{catalog}.bronze.formations_shape_graph_results"
      source: delta-table
      description: "Formation shape graph results"
    - id: "{catalog}.bronze.elastic_sync_results"
      source: delta-table
      description: "Elastic sync / space creation results"
    - id: "{catalog}.bronze.pausa_values"
      source: delta-table
      description: "PAUSA values from OBSO computation"
    - id: "{catalog}.bronze.player_embeddings_raw"
      source: delta-table
      description: "Player embeddings (v1 + v2)"
    - id: "{catalog}.bronze.xg_predictions_v2"
      source: delta-table
      description: "xG v2 deep-sets predictions"
```

- [ ] **Step 4: Verify YAML is valid**

Run: `uv run python -c "import yaml; [yaml.safe_load(open(f'workflow-cards/{c}.yaml').read().split('---')[1]) for c in ['wf-dbt-build-input-marts','wf-dbt-build-intermediate-marts','wf-dbt-build-output-marts']]"`
Expected: No errors

---

### Task 8: Add conformance tests

**Files:**
- Modify: `src/tests/test_guard_conformance.py` (append new test classes)

- [ ] **Step 1: Add `TestWatermarkGuardHasCardInputs`**

Append to `src/tests/test_guard_conformance.py`:

```python
class TestWatermarkGuardHasCardInputs:
    """Every module using check_upstream_freshness must have a workflow card with delta-table inputs."""

    _WATERMARK_CARDS: list[str] = [
        "wf-publish-spadl-vaep",
        "wf-publish-xg-shots",
        "wf-publish-freeze-frames",
        "wf-export-shots",
        "wf-scoutgpt-export",
        "wf-model-validation",
        "wf-dbt-build-input-marts",
        "wf-dbt-build-intermediate-marts",
        "wf-dbt-build-output-marts",
    ]

    @pytest.mark.parametrize("card_id", _WATERMARK_CARDS)
    def test_card_has_delta_table_inputs(self, card_id: str) -> None:
        from ingestion.guards import _repo_cards_dir, resolve_upstream_tables_from_card

        tables = resolve_upstream_tables_from_card(
            card_id, "test_catalog", "test_schema", cards_dir=_repo_cards_dir()
        )
        assert len(tables) > 0, f"Card {card_id} has no delta-table inputs for watermark guard"


class TestSelectorToCardParity:
    """Every key in _SELECTOR_TO_CARD must correspond to a dbt task in Terraform."""

    def test_all_selectors_have_tf_task(self) -> None:
        from ingestion.dbt_runner import _SELECTOR_TO_CARD

        tf_path = Path(__file__).resolve().parent.parent.parent / "terraform" / "modules" / "workflows" / "main.tf"
        tf_content = tf_path.read_text(encoding="utf-8")

        for selector_tags, card_id in _SELECTOR_TO_CARD.items():
            # Each selector's individual tags should appear in a TF parameters block
            for tag in selector_tags:
                bare_tag = tag.lstrip("+-")
                assert bare_tag in tf_content, (
                    f"Selector tag '{bare_tag}' (from {selector_tags!r}) "
                    f"not found in Terraform workflow definition"
                )
```

- [ ] **Step 2: Add `TestWatermarkRecordAfterSuccess`**

Append to `src/tests/test_guard_conformance.py`:

```python
class TestWatermarkRecordAfterSuccess:
    """Modules with watermark guards must call record_watermarks after run_pipeline."""

    _STANDALONE_MODULES: list[str] = [
        "ingestion.model_validation",
        "ingestion.dbt_runner",
        "ingestion.refresh_synced_tables",
    ]

    @pytest.mark.parametrize("module_path", _STANDALONE_MODULES)
    def test_standalone_module_calls_record_watermarks(self, module_path: str) -> None:
        import ast
        import importlib

        mod = importlib.import_module(module_path)
        source_path = Path(mod.__file__)
        tree = ast.parse(source_path.read_text(encoding="utf-8"))

        # Check that record_watermarks appears in the AST
        has_record = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "record_watermarks":
                    has_record = True
                elif isinstance(func, ast.Attribute) and func.attr == "record_watermarks":
                    has_record = True
        assert has_record, f"{module_path} must call record_watermarks after run_pipeline"

    def test_hf_sync_factories_call_record_watermarks(self) -> None:
        import ast

        hf_sync_path = Path(__file__).resolve().parent.parent / "ingestion" / "hf_sync.py"
        tree = ast.parse(hf_sync_path.read_text(encoding="utf-8"))

        factory_names = {"_make_watermark_op", "_make_watermark_volume_op"}
        found_factories: set[str] = set()

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in factory_names:
                # Check inner function calls record_watermarks
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call):
                        func = inner.func
                        if isinstance(func, ast.Name) and func.id == "record_watermarks":
                            found_factories.add(node.name)

        missing = factory_names - found_factories
        assert not missing, f"hf_sync.py factories missing record_watermarks call: {missing}"
```

- [ ] **Step 3: Run conformance tests**

Run: `uv run pytest src/tests/test_guard_conformance.py -v -k "Watermark or Selector" 2>&1 | tail -25`
Expected: All new tests PASS

---

### Task 9: Run full verification suite

**Files:** None (verification only)

- [ ] **Step 1: Ruff lint**

Run: `uv run ruff check src/ 2>&1 | tail -5`
Expected: Zero violations

- [ ] **Step 2: Pyright type check**

Run: `uv run pyright src/ 2>&1 | tail -5`
Expected: Zero errors

- [ ] **Step 3: Run all guard conformance tests**

Run: `uv run pytest src/tests/test_guard_conformance.py -v 2>&1 | tail -30`
Expected: All PASS (including pre-existing tests — verify no regressions)

- [ ] **Step 4: Run watermark unit tests**

Run: `uv run pytest src/tests/test_watermark_freshness.py src/tests/test_statsbomb_guard.py -v 2>&1 | tail -20`
Expected: All PASS

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest src/tests/ -v 2>&1 | tail -30`
Expected: All PASS

---

### Task 10: Commit

**Files:** All modified files from Tasks 1-8

- [ ] **Step 1: Review changes**

Run:
```bash
git status
git diff --stat
```

Expected modified files:
- `src/ingestion/guards.py` — watermark functions + `_GUARD_MODULES` additions
- `src/ingestion/hf_sync.py` — watermark factories + rewired sub-ops
- `src/ingestion/dbt_runner.py` — watermark guard + `_SELECTOR_TO_CARD`
- `src/ingestion/refresh_synced_tables.py` — watermark guard + `SYNCED_TABLES` derivation
- `src/ingestion/model_validation.py` — watermark guard replaces always-run
- `src/ingestion/statsbomb.py` — anti-join guard replaces always-run
- `workflow-cards/wf-dbt-build-input-marts.yaml` — expanded inputs
- `workflow-cards/wf-dbt-build-intermediate-marts.yaml` — expanded inputs
- `workflow-cards/wf-dbt-build-output-marts.yaml` — expanded inputs
- `src/tests/test_watermark_freshness.py` — new
- `src/tests/test_statsbomb_guard.py` — new
- `src/tests/test_guard_conformance.py` — 3 new test classes
- `docs/superpowers/specs/2026-05-06-skip-guard-watermark-design.md` — spec

- [ ] **Step 2: Await user approval, then commit**

Propose commit message:
```
feat(guards): add watermark skip guards for 10 downstream tasks + fix StatsBomb guard

- New watermark pattern: check_upstream_freshness / record_watermarks in guards.py
  using Delta DESCRIBE HISTORY filtered to data-changing operations
- New observability.workflow_watermarks table (lazy creation via ensure_table)
- hf_sync: 5 sub-ops moved to _make_watermark_op / _make_watermark_volume_op
- dbt_runner: new _DbtWatermarkGuard + _SELECTOR_TO_CARD mapping
- refresh_synced_tables: new guard deriving upstream from SYNCED_TABLES
- model_validation: always-run guard → watermark guard
- statsbomb: always-run guard → anti-join against sb.competitions() + match sampling
- 3 dbt workflow cards: expanded inputs.datasets for watermark resolution
- New tests: test_watermark_freshness.py, test_statsbomb_guard.py
- New conformance: TestWatermarkGuardHasCardInputs, TestWatermarkRecordAfterSuccess,
  TestSelectorToCardParity
```
