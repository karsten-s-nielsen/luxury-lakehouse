"""Centralized lifecycle hook registration for all ingestion pipelines.

Every ``main()`` entry point calls :func:`bootstrap_hooks` once before
invoking its ``run_pipeline`` function.  Adding a new hook type requires
changing only this module — no per-pipeline edits.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# SK3-MIG (2026-05-02): silly-kicks 3.0.0+ raises on input-convention
# mismatches under this flag instead of warning. Databricks serverless
# `compute.Environment` does not support per-job env vars, so we set it
# at process bootstrap before any silly-kicks import. Mirrors the CI env
# block in .github/workflows/python-ci.yml + dbt-live-ci.yml. setdefault
# preserves a deliberate operator override (e.g., `--no-strict` debugging).
os.environ.setdefault("SILLY_KICKS_ASSERT_INVARIANTS", "1")


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
