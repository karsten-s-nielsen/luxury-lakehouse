"""Combined HF Hub sync task — imports and exports in a single Databricks task.

Replaces 7 separate HF tasks (3 imports, 3 exports, 1 cost sync) with
one task that calls each as a ``@workflow``-decorated sub-operation.
Each sub-operation gets its own record in ``workflow_cost_live``.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Default workflow cards directory (matches the Databricks task parameter)
_DEFAULT_CARDS_DIR = Path("/Workspace/Repos/luxury-lakehouse/workflow-cards")


def _make_standard_op(module_path: str) -> Callable[..., None]:
    """Create a sub-operation caller for standard (spark, catalog, schema, logger) signatures."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        mod = importlib.import_module(module_path)
        mod.run_pipeline(spark, catalog, schema, logger_arg)

    _call.__qualname__ = f"_call[{module_path}]"
    return _call


def _make_sync_costs_op() -> Callable[..., None]:
    """Create the sync_hf_costs sub-operation (non-standard signature)."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        from ingestion.sync_hf_costs import sync_costs

        sync_costs(catalog, _DEFAULT_CARDS_DIR)

    return _call


# Sub-operations in execution order: imports first, then exports, then cost sync.
# Imports pull from previous HF Jobs runs (not the current pipeline run), so
# their ordering relative to exports is not a within-run dependency.
# Each is a callable(spark, catalog, schema, logger) for uniform invocation.
_SUB_OPERATIONS: list[tuple[str, Callable[..., None]]] = [
    ("ingestion.import_space_creation", _make_standard_op("ingestion.import_space_creation")),
    ("ingestion.import_obso_results", _make_standard_op("ingestion.import_obso_results")),
    ("ingestion.import_psxg_predictions", _make_standard_op("ingestion.import_psxg_predictions")),
    ("ingestion.export_embeddings_training_data", _make_standard_op("ingestion.export_embeddings_training_data")),
    ("ingestion.export_shots_on_target", _make_standard_op("ingestion.export_shots_on_target")),
    ("ingestion.prepare_360_training_data", _make_standard_op("ingestion.prepare_360_training_data")),
    ("ingestion.sync_hf_costs", _make_sync_costs_op()),
]


def _run_sub_workflow(
    label: str,
    op: Callable[..., None],
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger_arg: logging.Logger,
) -> None:
    """Run a single sub-workflow, swallowing failures."""
    try:
        op(spark, catalog, schema, logger_arg)
    except Exception:
        logger_arg.warning(
            "Sub-workflow %s failed — continuing with remaining operations",
            label,
            exc_info=True,
        )


@workflow("wf-hf-sync", phase="orchestration")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger_arg: logging.Logger,
    *,
    ctx: Any = None,
) -> int:
    """Run all HF import/export sub-operations sequentially."""
    completed = 0
    for label, op in _SUB_OPERATIONS:
        _run_sub_workflow(label, op, spark, catalog, schema, logger_arg)
        completed += 1
    return completed


def main() -> None:
    """CLI entry point for the hf_sync Databricks task."""
    configure_logging("hf_sync")
    args = parse_ingestion_args("Run all HF Hub import/export operations")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)
