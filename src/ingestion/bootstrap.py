"""Centralized lifecycle hook registration for all ingestion pipelines.

Every ``main()`` entry point calls :func:`bootstrap_hooks` once before
invoking its ``run_pipeline`` function.  Adding a new hook type requires
changing only this module — no per-pipeline edits.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def bootstrap_hooks(spark: SparkSession, catalog: str, schema: str) -> None:
    """Register all lifecycle hooks for a pipeline execution.

    Call this once in every ``main()`` before ``run_pipeline()``.
    Adding a new hook type requires changing only this function.

    Args:
        spark: Active SparkSession (passed to hooks that need Delta MERGE).
        catalog: Unity Catalog name.
        schema: Pipeline's target schema.
    """
    from ingestion.cost_hook import CostEstimateHook
    from workflows import register_hook

    register_hook(CostEstimateHook(spark, catalog, schema))
