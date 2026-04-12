"""Databricks cost-estimation lifecycle hook.

Writes run state and cost estimates to a ``workflow_cost_live`` Delta table
via MERGE.  Implements the :class:`~workflows.hooks.LifecycleHook` protocol.

All hook methods are fire-and-forget — exceptions are logged as warnings
but never propagate, so cost tracking cannot crash a compute pipeline.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from shared.constants import COST_TABLE_NAME, DEFAULT_OBSERVABILITY_SCHEMA, IDENTIFIER_RE
from workflows.context import WorkflowContext

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABRICKS_SERVERLESS_RATE: float = float(os.environ.get("DATABRICKS_SERVERLESS_RATE_USD", "0.07"))


class CostEstimateHook:
    """Lifecycle hook that writes cost data to ``workflow_cost_live`` via Delta MERGE.

    Implements the :class:`~workflows.hooks.LifecycleHook` protocol.  Every
    method wraps its body in ``try/except`` so that cost-tracking failures
    never crash a compute pipeline.

    Args:
        spark: Active SparkSession (used for ``createDataFrame`` and Delta MERGE).
        catalog: Unity Catalog name (validated against SQL identifier regex).
        schema: Pipeline's target schema (validated but NOT used for cost table).
        rate_usd_per_hour: Hourly rate for cost estimation. Defaults to
            ``DATABRICKS_SERVERLESS_RATE`` (from env var or ``0.07``).
        runtime: Execution runtime identifier (e.g. ``"databricks"``).
        cost_schema: Schema for the ``workflow_cost_live`` table. Defaults to
            ``"observability"`` — a dedicated schema for platform operational
            metadata, separate from pipeline data schemas.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        rate_usd_per_hour: float = DATABRICKS_SERVERLESS_RATE,
        runtime: str = "databricks",
        cost_schema: str = DEFAULT_OBSERVABILITY_SCHEMA,
    ) -> None:
        for name, value in [("catalog", catalog), ("schema", schema), ("cost_schema", cost_schema)]:
            if not IDENTIFIER_RE.match(value):
                msg = f"Invalid {name} name {value!r}: must match {IDENTIFIER_RE.pattern}"
                raise ValueError(msg)

        self._spark = spark
        self._table = f"{catalog}.{cost_schema}.{COST_TABLE_NAME}"
        self._rate_usd_per_hour = rate_usd_per_hour
        self._runtime = runtime

    # ------------------------------------------------------------------
    # LifecycleHook protocol methods
    # ------------------------------------------------------------------

    def on_start(self, ctx: WorkflowContext) -> None:
        """MERGE insert: state=RUNNING, cost=0.0, cost_source=live_estimate."""
        try:
            now_utc = datetime.now(tz=timezone.utc)
            self._merge(
                ctx,
                state="RUNNING",
                started_at=ctx.started_at,
                ended_at=None,
                duration_seconds=0,
                row_count=None,
                estimated_cost_usd=Decimal("0.0000"),
                cost_source="live_estimate",
                updated_at=now_utc,
            )
        except Exception:
            logger.warning("CostEstimateHook.on_start failed for run_id=%s", ctx.run_id, exc_info=True)

    def on_complete(self, ctx: WorkflowContext, row_count: int | None) -> None:
        """MERGE update: state=COMPLETED with duration-based cost estimate."""
        try:
            now_utc = datetime.now(tz=timezone.utc)
            duration = int((now_utc - ctx.started_at).total_seconds())
            cost = Decimal(str(round(duration * (self._rate_usd_per_hour / 3600), 4)))
            self._merge(
                ctx,
                state="COMPLETED",
                started_at=ctx.started_at,
                ended_at=now_utc,
                duration_seconds=duration,
                row_count=row_count,
                estimated_cost_usd=cost,
                cost_source="completion_estimate",
                updated_at=now_utc,
            )
        except Exception:
            logger.warning("CostEstimateHook.on_complete failed for run_id=%s", ctx.run_id, exc_info=True)

    def on_skip(self, ctx: WorkflowContext, reason: str) -> None:
        """MERGE update: state=SKIPPED, cost=0.0.

        The ``reason`` is not persisted to Delta (no column) — use Databricks
        job logs for skip diagnostics.
        """
        try:
            logger.debug("Workflow %s skipped: %s", ctx.workflow_id, reason)
            now_utc = datetime.now(tz=timezone.utc)
            self._merge(
                ctx,
                state="SKIPPED",
                started_at=ctx.started_at,
                ended_at=now_utc,
                duration_seconds=0,
                row_count=None,
                estimated_cost_usd=Decimal("0.0000"),
                cost_source="completion_estimate",
                updated_at=now_utc,
            )
        except Exception:
            logger.warning("CostEstimateHook.on_skip failed for run_id=%s", ctx.run_id, exc_info=True)

    def on_error(self, ctx: WorkflowContext, error: Exception) -> None:
        """MERGE update: state=FAILED with partial cost."""
        try:
            now_utc = datetime.now(tz=timezone.utc)
            duration = int((now_utc - ctx.started_at).total_seconds())
            cost = Decimal(str(round(duration * (self._rate_usd_per_hour / 3600), 4)))
            self._merge(
                ctx,
                state="FAILED",
                started_at=ctx.started_at,
                ended_at=now_utc,
                duration_seconds=duration,
                row_count=None,
                estimated_cost_usd=cost,
                cost_source="completion_estimate",
                updated_at=now_utc,
            )
        except Exception:
            logger.warning("CostEstimateHook.on_error failed for run_id=%s", ctx.run_id, exc_info=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _merge(self, ctx: WorkflowContext, **fields: Any) -> None:
        """Build a single-row DataFrame and MERGE into the cost table.

        Uses ``DeltaTable.forName`` with ``whenMatchedUpdateAll`` /
        ``whenNotMatchedInsertAll`` keyed on ``run_id``.

        An explicit schema is required because nullable columns (ended_at,
        duration_seconds, etc.) are often ``None`` and Spark Connect cannot
        infer types from null values alone (CANNOT_DETERMINE_TYPE).
        """
        from delta.tables import DeltaTable
        from pyspark.sql.types import (
            DecimalType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        row: dict[str, Any] = {
            "workflow_id": ctx.workflow_id,
            "phase": ctx.phase,
            "run_id": ctx.run_id,
            "runtime": self._runtime,
            "hf_job_id": None,  # Populated by sync_hf_costs.py for HF Jobs runs
            "entity_count": ctx.entity_count,
            "guard_duration_seconds": ctx.guard_duration_seconds,
            "rate_usd_per_hour": Decimal(str(self._rate_usd_per_hour)),
        }
        row.update(fields)

        schema = StructType(
            [
                StructField("workflow_id", StringType(), False),
                StructField("phase", StringType(), False),
                StructField("run_id", StringType(), False),
                StructField("runtime", StringType(), False),
                StructField("hf_job_id", StringType(), True),
                StructField("state", StringType(), False),
                StructField("started_at", TimestampType(), False),
                StructField("ended_at", TimestampType(), True),
                StructField("duration_seconds", IntegerType(), True),
                StructField("row_count", IntegerType(), True),
                StructField("entity_count", IntegerType(), True),
                StructField("guard_duration_seconds", IntegerType(), True),
                StructField("rate_usd_per_hour", DecimalType(10, 6), True),
                StructField("estimated_cost_usd", DecimalType(10, 4), True),
                StructField("cost_source", StringType(), False),
                StructField("updated_at", TimestampType(), False),
            ]
        )

        df = self._spark.createDataFrame([row], schema=schema)
        (
            DeltaTable.forName(self._spark, self._table)
            .alias("t")
            .merge(df.alias("s"), "t.run_id = s.run_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
