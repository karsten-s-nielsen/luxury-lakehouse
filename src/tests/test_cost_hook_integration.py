"""End-to-end integration test for CostEstimateHook against a real Delta table.

Separate from ``test_cost_hook.py`` because that file has an autouse fixture
(``_mock_delta_tables``) that installs mock ``delta.tables`` and
``pyspark.sql.types`` modules into ``sys.modules`` — incompatible with running
real Spark.

Skipped automatically when local Spark + Delta are not available. Runs in
Databricks CI (where Spark is installed) and catches schema drift at the MERGE
level (not just at the StructType level like ``TestCostHookSchemaDriftGuard``).

Regression target: the 2026-04-12 warm-tier blocker where every
``CostEstimateHook._merge()`` call failed with DELTA_MERGE_UNRESOLVED_EXPRESSION
because the live Delta table had an orphaned ``task_key`` column that the
hook's DataFrame schema didn't include. Schema drift between the hook's
source schema and the target table schema caused 62+ hours of silent failure.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.cost_hook import CostEstimateHook
from workflows.context import WorkflowContext


@pytest.fixture
def spark():
    """Local SparkSession with Delta Lake extensions.

    Skips the test if pyspark or Delta is unavailable, or if local SparkSession
    initialization fails. Suitable for CI environments that have Spark
    installed and for skipping cleanly on developer workstations without it.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark not installed")
        return  # unreachable but satisfies type checker

    from unittest.mock import MagicMock

    if isinstance(SparkSession, MagicMock):
        # Autouse fixture from another file mocked pyspark.sql; bail out.
        pytest.skip("pyspark.sql is mocked in this test session")
        return

    try:
        session = (
            SparkSession.builder.appName("test_cost_hook_integration")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .master("local[1]")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Local Spark/Delta not available: {exc}")
        return
    yield session
    session.stop()


def test_on_start_then_on_complete_round_trip(spark, tmp_path) -> None:
    """Write a single run via the hook; assert row INSERTed then UPDATEd.

    Builds a temp Delta table from the canonical ``scripts/create_cost_table.sql``
    so if the DDL drifts from the hook's ``_COST_LIVE_COLUMNS`` the MERGE
    fails the same way it did in production 2026-04-12.
    """
    sql = Path("scripts/create_cost_table.sql").read_text()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_obs")
    spark.sql("DROP TABLE IF EXISTS test_obs.workflow_cost_live")
    create_ddl = sql.replace("{catalog}.observability", "test_obs").replace(
        "CREATE TABLE IF NOT EXISTS", "CREATE TABLE"
    )
    spark.sql(create_ddl)

    hook = CostEstimateHook(spark, catalog="test_obs", schema="unused")
    hook._table = "test_obs.workflow_cost_live"  # type: ignore[attr-defined]

    ctx = WorkflowContext(
        workflow_id="wf-test",
        phase="ingest",
        run_id="test-run-integration-1",
        started_at=datetime.now(timezone.utc),
        entity_count=10,
        guard_duration_seconds=5,
    )

    hook.on_start(ctx)
    rows = spark.sql("SELECT state, row_count FROM test_obs.workflow_cost_live").collect()
    assert len(rows) == 1
    assert rows[0]["state"] == "RUNNING"
    assert rows[0]["row_count"] is None

    hook.on_complete(ctx, row_count=100)
    rows = spark.sql("SELECT state, row_count FROM test_obs.workflow_cost_live").collect()
    assert len(rows) == 1, "on_complete must UPDATE the existing row, not INSERT"
    assert rows[0]["state"] == "COMPLETED"
    assert rows[0]["row_count"] == 100
