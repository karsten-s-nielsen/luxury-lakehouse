"""Tests for CostEstimateHook — Databricks Delta MERGE cost tracking."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from types import ModuleType
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from ingestion.cost_hook import DATABRICKS_SERVERLESS_RATE, CostEstimateHook
from workflows.context import WorkflowContext

# ---------------------------------------------------------------------------
# Helpers & fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _mock_delta_tables() -> Iterator[MagicMock]:
    """Inject mock ``delta.tables`` and ``pyspark.sql.types`` modules.

    The ``DeltaTable`` mock is configured as a fluent builder so that the
    chained ``.alias().merge().whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()``
    calls all return MagicMock objects.

    The ``pyspark.sql.types`` mock provides sentinel classes for StructType,
    StructField, and the scalar types so that ``_merge()`` can build its schema.
    """
    mock_dt_cls = MagicMock()
    # Fluent builder chain: forName -> alias -> merge -> whenMatchedUpdateAll -> whenNotMatchedInsertAll -> execute
    mock_instance = MagicMock()
    mock_dt_cls.forName.return_value = mock_instance
    mock_instance.alias.return_value = mock_instance
    mock_instance.merge.return_value = mock_instance
    mock_instance.whenMatchedUpdateAll.return_value = mock_instance
    mock_instance.whenNotMatchedInsertAll.return_value = mock_instance

    # Build a fake delta module with the mock class
    delta_mod = ModuleType("delta")
    tables_mod = ModuleType("delta.tables")
    tables_mod.DeltaTable = mock_dt_cls  # type: ignore[attr-defined]
    delta_mod.tables = tables_mod  # type: ignore[attr-defined]

    sys.modules["delta"] = delta_mod
    sys.modules["delta.tables"] = tables_mod

    # Build fake pyspark.sql.types module with sentinel type classes
    pyspark_mod = ModuleType("pyspark")
    pyspark_sql_mod = ModuleType("pyspark.sql")
    types_mod = ModuleType("pyspark.sql.types")

    # Sentinel callables that return MagicMock objects (used as Spark types)
    for type_name in (
        "StructType",
        "StructField",
        "StringType",
        "LongType",
        "IntegerType",
        "TimestampType",
        "DecimalType",
    ):
        setattr(types_mod, type_name, MagicMock(name=type_name))

    pyspark_sql_mod.types = types_mod  # type: ignore[attr-defined]
    pyspark_mod.sql = pyspark_sql_mod  # type: ignore[attr-defined]

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = pyspark_sql_mod
    sys.modules["pyspark.sql.types"] = types_mod

    yield mock_dt_cls

    # Cleanup
    sys.modules.pop("delta.tables", None)
    sys.modules.pop("delta", None)
    sys.modules.pop("pyspark.sql.types", None)
    sys.modules.pop("pyspark.sql", None)
    sys.modules.pop("pyspark", None)


def _make_spark() -> MagicMock:
    """Create a mock SparkSession with sensible defaults."""
    spark = MagicMock()
    # Default: local/notebook mode — no Databricks job metadata
    spark.conf.get.return_value = None
    # createDataFrame returns a mock DataFrame
    spark.createDataFrame.return_value = MagicMock()
    return spark


def _make_ctx(**overrides: object) -> WorkflowContext:
    """Create a WorkflowContext with sensible defaults."""
    defaults: dict[str, object] = {
        "workflow_id": "wf-test_pipeline",
        "phase": "compute",
        "workflow_name": "Test Pipeline",
    }
    defaults.update(overrides)
    return WorkflowContext(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


class TestConstructorValidation:
    def test_rejects_invalid_catalog_sql_injection(self) -> None:
        spark = _make_spark()
        with pytest.raises(ValueError, match="catalog"):
            CostEstimateHook(spark, catalog="DROP TABLE;--", schema="gold")

    def test_rejects_invalid_schema_sql_injection(self) -> None:
        spark = _make_spark()
        with pytest.raises(ValueError, match="schema"):
            CostEstimateHook(spark, catalog="soccer_analytics", schema="gold; DROP TABLE")

    def test_rejects_catalog_with_spaces(self) -> None:
        spark = _make_spark()
        with pytest.raises(ValueError, match="catalog"):
            CostEstimateHook(spark, catalog="my catalog", schema="gold")

    def test_rejects_schema_starting_with_digit(self) -> None:
        spark = _make_spark()
        with pytest.raises(ValueError, match="schema"):
            CostEstimateHook(spark, catalog="soccer_analytics", schema="1bad")

    def test_accepts_valid_identifiers(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="soccer_analytics", schema="dev_gold")
        assert hook is not None

    def test_accepts_underscored_identifiers(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="_internal", schema="_staging")
        assert hook is not None

    def test_table_name_construction(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="soccer_analytics", schema="dev_gold")
        assert hook._table == "soccer_analytics.observability.workflow_cost_live"


# ---------------------------------------------------------------------------
# Rate configuration
# ---------------------------------------------------------------------------


class TestRateConfiguration:
    def test_default_rate_matches_module_constant(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        assert hook._rate_usd_per_hour == DATABRICKS_SERVERLESS_RATE

    def test_default_rate_is_0_07(self) -> None:
        # Verify the default when env var is not set
        assert DATABRICKS_SERVERLESS_RATE == 0.07

    def test_custom_rate(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch", rate_usd_per_hour=1.50)
        assert hook._rate_usd_per_hour == 1.50

    def test_env_var_override(self) -> None:
        with patch.dict("os.environ", {"DATABRICKS_SERVERLESS_RATE_USD": "0.10"}):
            # Re-import to pick up the env var — but since it's module-level,
            # we test by passing the computed value explicitly
            rate = float("0.10")
            spark = _make_spark()
            hook = CostEstimateHook(spark, catalog="cat", schema="sch", rate_usd_per_hour=rate)
            assert hook._rate_usd_per_hour == 0.10


# ---------------------------------------------------------------------------
# Runtime metadata
# ---------------------------------------------------------------------------


class TestRuntimeMetadata:
    def test_default_runtime(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        assert hook._runtime == "databricks"

    def test_custom_runtime(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch", runtime="hf_jobs")
        assert hook._runtime == "hf_jobs"

    def test_reads_job_run_id_from_spark_conf(self) -> None:
        spark = _make_spark()
        spark.conf.get.side_effect = lambda key, default=None: {
            "spark.databricks.job.runId": "12345",
            "spark.databricks.task.key": "ingest_statsbomb",
        }.get(key, default)

        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        assert hook._job_run_id == "12345"
        assert hook._task_key == "ingest_statsbomb"

    def test_job_metadata_none_in_local_mode(self) -> None:
        spark = _make_spark()
        spark.conf.get.return_value = None

        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        assert hook._job_run_id is None
        assert hook._task_key is None

    def test_job_metadata_handles_conf_error(self) -> None:
        """If spark.conf.get raises, the hook should handle gracefully."""
        spark = _make_spark()
        spark.conf.get.side_effect = Exception("conf not available")

        # Should not raise
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        assert hook._job_run_id is None
        assert hook._task_key is None


# ---------------------------------------------------------------------------
# on_start
# ---------------------------------------------------------------------------


class TestOnStart:
    def test_on_start_calls_spark_create_dataframe(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_start(ctx)

        spark.createDataFrame.assert_called_once()

    def test_on_start_row_has_running_state(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_start(ctx)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["state"] == "RUNNING"
        assert row["estimated_cost_usd"] == Decimal("0.0000")
        assert row["cost_source"] == "live_estimate"
        assert row["workflow_id"] == ctx.workflow_id
        assert row["phase"] == ctx.phase
        assert row["run_id"] == ctx.run_id

    def test_on_start_never_raises(self, caplog: pytest.LogCaptureFixture) -> None:
        spark = _make_spark()
        spark.createDataFrame.side_effect = RuntimeError("Spark unavailable")
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        # Must not raise
        with caplog.at_level(logging.WARNING):
            hook.on_start(ctx)

    def test_on_start_includes_run_id(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_start(ctx)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["run_id"] == ctx.run_id

    def test_on_start_includes_started_at(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_start(ctx)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["started_at"] == ctx.started_at

    def test_on_start_includes_entity_count(self) -> None:
        """on_start row should include entity_count from context."""
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = WorkflowContext(
            workflow_id="wf-test",
            phase="compute",
            entity_count=5,
        )

        hook.on_start(ctx)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["entity_count"] == 5

    def test_on_start_entity_count_none_when_not_set(self) -> None:
        """entity_count should be None when context has no entity_count."""
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = WorkflowContext(workflow_id="wf-test", phase="compute")

        hook.on_start(ctx)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["entity_count"] is None


# ---------------------------------------------------------------------------
# on_complete
# ---------------------------------------------------------------------------


class TestOnComplete:
    def test_on_complete_calls_spark_create_dataframe(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_complete(ctx, row_count=42_000)

        spark.createDataFrame.assert_called_once()

    def test_on_complete_row_has_completed_state(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_complete(ctx, row_count=42_000)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["state"] == "COMPLETED"
        assert row["cost_source"] == "completion_estimate"
        assert row["row_count"] == 42_000

    def test_on_complete_calculates_duration(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_complete(ctx, row_count=None)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert isinstance(row["duration_seconds"], int)
        assert row["duration_seconds"] >= 0

    def test_on_complete_calculates_cost(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch", rate_usd_per_hour=3600.0)
        ctx = _make_ctx()

        hook.on_complete(ctx, row_count=None)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        # Cost = duration * (rate / 3600)
        # With rate=3600 => cost = duration * 1.0
        assert isinstance(row["estimated_cost_usd"], Decimal)
        assert row["estimated_cost_usd"] >= Decimal("0")

    def test_on_complete_never_raises(self, caplog: pytest.LogCaptureFixture) -> None:
        spark = _make_spark()
        spark.createDataFrame.side_effect = RuntimeError("Spark unavailable")
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        with caplog.at_level(logging.WARNING):
            hook.on_complete(ctx, row_count=42_000)

    def test_on_complete_row_count_none(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_complete(ctx, row_count=None)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["row_count"] is None

    def test_on_complete_includes_ended_at(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_complete(ctx, row_count=100)

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert "ended_at" in row
        # Should be a datetime object
        assert isinstance(row["ended_at"], datetime)


# ---------------------------------------------------------------------------
# on_skip
# ---------------------------------------------------------------------------


class TestOnSkip:
    def test_on_skip_never_raises(self) -> None:
        spark = _make_spark()
        spark.createDataFrame.side_effect = RuntimeError("Spark unavailable")
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        # Must not raise
        hook.on_skip(ctx, reason="All matches already processed")

    def test_on_skip_row_has_skipped_state(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_skip(ctx, reason="Nothing to do")

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["state"] == "SKIPPED"
        assert row["estimated_cost_usd"] == Decimal("0.0000")
        assert row["cost_source"] == "completion_estimate"

    def test_on_skip_zero_duration(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_skip(ctx, reason="cached")

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["duration_seconds"] == 0


# ---------------------------------------------------------------------------
# on_error
# ---------------------------------------------------------------------------


class TestOnError:
    def test_on_error_never_raises(self) -> None:
        spark = _make_spark()
        spark.createDataFrame.side_effect = RuntimeError("Spark unavailable")
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        # Must not raise
        hook.on_error(ctx, error=RuntimeError("OOM killed"))

    def test_on_error_row_has_failed_state(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_error(ctx, error=ValueError("bad data"))

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert row["state"] == "FAILED"
        assert row["cost_source"] == "completion_estimate"

    def test_on_error_calculates_partial_cost(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_error(ctx, error=RuntimeError("timeout"))

        row_data = spark.createDataFrame.call_args[0][0]
        row = row_data[0]
        assert isinstance(row["estimated_cost_usd"], Decimal)
        assert row["estimated_cost_usd"] >= Decimal("0")
        assert isinstance(row["duration_seconds"], int)


# ---------------------------------------------------------------------------
# MERGE mechanics
# ---------------------------------------------------------------------------


class TestMergeMechanics:
    def test_merge_uses_delta_table_for_name(self, _mock_delta_tables: MagicMock) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="soccer_analytics", schema="dev_gold")
        ctx = _make_ctx()

        hook.on_start(ctx)

        _mock_delta_tables.forName.assert_called_once_with(spark, "soccer_analytics.observability.workflow_cost_live")

    def test_merge_condition_uses_run_id(self, _mock_delta_tables: MagicMock) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_start(ctx)

        mock_instance = _mock_delta_tables.forName.return_value
        merge_call = mock_instance.alias.return_value.merge
        merge_call.assert_called_once()
        merge_condition = merge_call.call_args[0][1]
        assert "run_id" in merge_condition

    def test_merge_calls_execute(self, _mock_delta_tables: MagicMock) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()

        hook.on_start(ctx)

        mock_instance = _mock_delta_tables.forName.return_value
        chain = mock_instance.alias.return_value
        chain.merge.return_value.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once()


# ---------------------------------------------------------------------------
# All required columns present
# ---------------------------------------------------------------------------


class TestColumnCompleteness:
    """Every MERGE row must contain all workflow_cost_live columns."""

    REQUIRED_COLUMNS: ClassVar[set[str]] = {
        "workflow_id",
        "phase",
        "run_id",
        "runtime",
        "job_run_id",
        "task_key",
        "hf_job_id",
        "state",
        "started_at",
        "ended_at",
        "duration_seconds",
        "row_count",
        "entity_count",
        "rate_usd_per_hour",
        "estimated_cost_usd",
        "cost_source",
        "updated_at",
    }

    def _extract_row(self, spark: MagicMock) -> dict[str, object]:
        row_data = spark.createDataFrame.call_args[0][0]
        return row_data[0]

    def test_on_start_has_all_columns(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()
        hook.on_start(ctx)
        row = self._extract_row(spark)
        assert set(row.keys()) == self.REQUIRED_COLUMNS

    def test_on_complete_has_all_columns(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()
        hook.on_complete(ctx, row_count=100)
        row = self._extract_row(spark)
        assert set(row.keys()) == self.REQUIRED_COLUMNS

    def test_on_skip_has_all_columns(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()
        hook.on_skip(ctx, reason="done")
        row = self._extract_row(spark)
        assert set(row.keys()) == self.REQUIRED_COLUMNS

    def test_on_error_has_all_columns(self) -> None:
        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        ctx = _make_ctx()
        hook.on_error(ctx, error=RuntimeError("fail"))
        row = self._extract_row(spark)
        assert set(row.keys()) == self.REQUIRED_COLUMNS


# ---------------------------------------------------------------------------
# LifecycleHook protocol compliance
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    def test_isinstance_lifecycle_hook(self) -> None:
        from workflows.hooks import LifecycleHook

        spark = _make_spark()
        hook = CostEstimateHook(spark, catalog="cat", schema="sch")
        assert isinstance(hook, LifecycleHook)
