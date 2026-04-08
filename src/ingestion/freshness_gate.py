"""Freshness gate — centralized skip guard orchestration.

Runs all workflow guards in a single Databricks task, emits SKIPPED
records for workflows with no new work, and writes FilterResults as
task values for downstream ``run_if`` conditions.

Uses the ``default`` environment (wheel only) — guards use only
Spark SQL, no analytics imports.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from ingestion.guards import FilterResult, get_workflow_guards
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ingestion.guards import SkipGuard

logger = logging.getLogger(__name__)


def run_gate(
    spark: SparkSession,
    catalog: str,
    schema: str,
    *,
    guards: dict[str, SkipGuard] | None = None,
) -> dict[str, FilterResult]:
    """Run all guards and collect results.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Pipeline target schema (passed to guards).
        guards: Override guard registry (for testing).

    Returns:
        Dict mapping workflow_id to FilterResult.
    """
    if guards is None:
        guards = get_workflow_guards()

    results: dict[str, FilterResult] = {}
    timings: dict[str, float] = {}

    for wf_id, guard in guards.items():
        t0 = time.monotonic()
        try:
            result = guard.check(spark, catalog, schema)
            elapsed = round(time.monotonic() - t0, 2)
            results[wf_id] = result
            timings[wf_id] = elapsed
            logger.info(
                "guard_check",
                extra={
                    "workflow_id": wf_id,
                    "count": result.count,
                    "elapsed_seconds": elapsed,
                    "chunks": len(result.chunks) if result.chunks else 0,
                },
            )
        except Exception:
            elapsed = round(time.monotonic() - t0, 2)
            logger.warning(
                "guard_check_failed",
                extra={"workflow_id": wf_id, "elapsed_seconds": elapsed},
                exc_info=True,
            )
            results[wf_id] = FilterResult(workflow_id=wf_id, count=0)
            timings[wf_id] = elapsed

    logger.info(
        "gate_summary",
        extra={
            "total_guards": len(guards),
            "with_work": sum(1 for r in results.values() if r.count > 0),
            "skipped": sum(1 for r in results.values() if r.count == 0),
            "total_elapsed": round(sum(timings.values()), 2),
            "slowest_guard": max(timings, key=timings.get) if timings else "none",  # type: ignore[arg-type]
            "slowest_seconds": round(max(timings.values()), 2) if timings else 0,
        },
    )

    return results


def _emit_skipped_records(
    spark: SparkSession,
    catalog: str,
    schema: str,
    results: dict[str, FilterResult],
) -> None:
    """MERGE SKIPPED records into workflow_cost_live for count=0 workflows.

    Uses CostEstimateHook directly rather than reaching into runner internals.
    """
    from ingestion.cost_hook import CostEstimateHook
    from workflows.context import WorkflowContext

    hook = CostEstimateHook(spark, catalog, schema)

    for wf_id, result in results.items():
        if result.count == 0:
            ctx = WorkflowContext(
                workflow_id=wf_id,
                phase="gate_skip",
            )
            hook.on_skip(ctx, "No new work (freshness gate)")


def _write_task_values(results: dict[str, FilterResult]) -> None:
    """Write FilterResults as Databricks task values for downstream run_if."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark:
            dbutils = DBUtils(spark)
            for wf_id, result in results.items():
                dbutils.jobs.taskValues.set(key=wf_id, value=result.to_json())
    except Exception:
        logger.debug("Task values not available (standalone mode)")


@workflow("wf-freshness-gate", phase="orchestration")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger_arg: logging.Logger,
    *,
    ctx: Any = None,
) -> int:
    """Freshness gate entry point — run all guards, emit skips, write task values."""
    results = run_gate(spark, catalog, schema)

    _emit_skipped_records(spark, catalog, schema, results)
    _write_task_values(results)

    work_count = sum(1 for r in results.values() if r.count > 0)
    logger_arg.info("Freshness gate complete: %d workflows with work", work_count)
    return work_count


def main() -> None:
    """CLI entry point for the freshness_gate Databricks task."""
    gate_logger = configure_logging("freshness_gate")
    args = parse_ingestion_args("Run freshness gate for all pipeline skip guards")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, gate_logger)
