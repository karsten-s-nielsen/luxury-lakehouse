"""DEFCON-lite batch computation pipeline (orchestrator).

Reads action values from ``fct_action_values`` and freeze frame data from
``statsbomb_360`` in the bronze layer, computes per-defender defensive credits
using the DEFCON-lite analytics module, and writes results to a new
``defcon_results`` bronze table.

Design: "Read from gold + bronze, compute, write to bronze." Actions come from
the gold mart (SPADL 105x68m coordinates). Freeze frames come from bronze
(StatsBomb 360 data).

Architecture: Uses two-pass ``applyInPandas`` to distribute DEFCON computation
across Spark executors instead of sequential per-match driver loops.
Pass 1 assigns defensive credits per match (Stage 1), Pass 2 estimates
DEFCON values via XGBoost per match (Stage 2).

If a @Champion model is registered in MLflow at
``soccer_analytics.dev_gold.defcon_model``, the pipeline loads it instead of
retraining per-match XGBoost estimators. Falls back to per-match training
when MLflow is not available or no Champion is registered.

Data path modules:
- ``defcon_lite_common``: Shared constants, MLflow loading, value UDF builder.
- ``defcon_lite_360``: StatsBomb 360 freeze-frame processing path.
- ``defcon_lite_tracking``: Metrica tracking processing path.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from analytics.defcon_lite import DefconLiteParams
from ingestion.defcon_lite_360 import process_360_matches
from ingestion.defcon_lite_common import _try_load_champion_defcon
from ingestion.defcon_lite_tracking import process_tracking_matches
from ingestion.guards import FilterResult
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
)
from shared.constants import DEFAULT_GOLD_SCHEMA
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@workflow("wf-defcon", phase="inference")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_360: FilterResult,
    filter_tracking: FilterResult,
    ctx=None,
) -> int:
    """Execute the DEFCON-lite computation pipeline.

    Attempts to load a pre-trained DEFCON value estimator from MLflow @Champion.
    If available, passes serialized model bytes to value UDFs for consistent
    cross-match scoring. Falls back to per-match XGBoost training otherwise.
    """
    if filter_360.count == 0 and filter_tracking.count == 0:
        raise WorkflowSkippedError("No new work")

    params = DefconLiteParams()

    # Attempt to load @Champion model (driver-side only)
    champion_bytes = _try_load_champion_defcon(logger, catalog, DEFAULT_GOLD_SCHEMA)
    if champion_bytes is not None:
        logger.info("Using @Champion DEFCON model for value estimation")
    else:
        logger.info("Using per-match DEFCON training (no @Champion model found)")

    total_360 = process_360_matches(spark, catalog, schema, logger, params, champion_bytes, filter_result=filter_360)
    logger.info("360 processing complete: %d rows", total_360)

    total_tracking = process_tracking_matches(
        spark, catalog, schema, logger, params, champion_bytes, filter_result=filter_tracking
    )
    logger.info("Tracking processing complete: %d rows", total_tracking)

    logger.info("DEFCON-lite pipeline complete — %d total rows written", total_360 + total_tracking)
    return total_360 + total_tracking


def main() -> None:
    """CLI entry point for DEFCON-lite computation."""
    args = parse_ingestion_args("Compute DEFCON-lite defensive valuations")
    logger = configure_logging("defcon_lite")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    from ingestion.defcon_lite_360 import skip_guard as guard_360
    from ingestion.defcon_lite_tracking import skip_guard as guard_tracking

    filter_360 = guard_360.check(spark, args.catalog, args.schema)
    filter_tracking = guard_tracking.check(spark, args.catalog, args.schema)

    logger.info("Starting DEFCON-lite pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_360=filter_360, filter_tracking=filter_tracking)


if __name__ == "__main__":
    main()
