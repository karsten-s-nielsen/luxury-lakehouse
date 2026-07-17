"""Combined HF Hub sync task — imports and exports in a single Databricks task.

Replaces separate HF tasks with one task that calls each as a
``@workflow``-decorated sub-operation (currently 9 sub-operations).
Each sub-operation gets its own record in ``workflow_cost_live``.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ingestion.guards import (
    FilterResult,
    check_upstream_freshness,
    record_watermarks,
    resolve_upstream_tables_from_card,
    timed_check,
)
from ingestion.utils import configure_logging, get_spark_session, parse_ingestion_args
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _HfSyncGuard:
    workflow_id = "wf-hf-sync"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Always-run stub — individual sub-operations have their own guards."""
        return FilterResult(workflow_id=self.workflow_id, count=1)


skip_guard = _HfSyncGuard()

# Default UC Volume paths for sub-operations that stage via Volume.
# These match the CLI defaults in each sub-module's ``main()`` function.
_VOLUME_PATHS: dict[str, str] = {
    "ingestion.import_obso_results": "/Volumes/soccer_analytics/dev_gold/model_weights/obso",
    "ingestion.import_psxg_predictions": "/Volumes/soccer_analytics/dev_gold/model_weights/psxg",
    "ingestion.export_shots_on_target": "/Volumes/soccer_analytics/dev_gold/model_weights/psxg",
    "ingestion.prepare_360_training_data": "/Volumes/soccer_analytics/dev_gold/training_data/football2vec_360",
}


def _make_volume_op(module_path: str) -> Callable[..., None]:
    """Create a sub-operation caller for modules with (spark, catalog, schema, volume_path, *, filter_result)."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        mod = importlib.import_module(module_path)
        filter_result = timed_check(mod.skip_guard, spark, catalog, schema)
        volume_path = _VOLUME_PATHS[module_path]
        mod.run_pipeline(spark, catalog, schema, volume_path, filter_result=filter_result)

    _call.__qualname__ = f"_call[{module_path}]"
    return _call


def _make_logger_op(module_path: str) -> Callable[..., None]:
    """Create a sub-operation caller for modules with (spark, catalog, schema, logger, *, filter_result)."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        mod = importlib.import_module(module_path)
        filter_result = timed_check(mod.skip_guard, spark, catalog, schema)
        mod.run_pipeline(spark, catalog, schema, logger_arg, filter_result=filter_result)

    _call.__qualname__ = f"_call[{module_path}]"
    return _call


def _make_export_shots_op() -> Callable[..., None]:
    """Create the export_shots_on_target sub-operation (no filter_result / no skip_guard)."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        mod = importlib.import_module("ingestion.export_shots_on_target")
        volume_path = _VOLUME_PATHS["ingestion.export_shots_on_target"]
        mod.run_pipeline(spark, catalog, schema, volume_path)

    return _call


def _make_plain_op(module_path: str) -> Callable[..., None]:
    """Create a sub-operation caller for modules with (spark, catalog, schema, logger) — no skip_guard."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        mod = importlib.import_module(module_path)
        mod.run_pipeline(spark, catalog, schema, logger_arg)

    _call.__qualname__ = f"_call[{module_path}]"
    return _call


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

    Uses the existing ``_VOLUME_PATHS`` dict to resolve the UC Volume path.
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


def _make_sync_costs_op() -> Callable[..., None]:
    """Create the sync_hf_costs sub-operation (non-standard signature)."""

    def _call(spark: SparkSession, catalog: str, schema: str, logger_arg: logging.Logger) -> None:
        from ingestion.sync_hf_costs import run_pipeline, skip_guard

        filter_result = timed_check(skip_guard, spark, catalog, schema)
        from ingestion.guards import _default_cards_dir

        run_pipeline(catalog, _default_cards_dir(), filter_result=filter_result)

    return _call


# Sub-operations in execution order: imports first, then exports, then cost sync.
# Imports pull from previous HF Jobs runs (not the current pipeline run), so
# their ordering relative to exports is not a within-run dependency.
# Each is a callable(spark, catalog, schema, logger) for uniform invocation.
#
# PR-Cycle-B (2026-05-01) split `ingestion.import_obso_results` out of this
# list into its own Databricks task. Reason: compute_pausa reads
# bronze.pausa_raw_scores written by import_obso_results — pre-split,
# compute_pausa ran in parallel with hf_sync, so PAUSA silently consumed
# yesterday's pausa_raw_scores when hf_sync took longer than the
# elastic_sync→pausa chain. The standalone import_obso_results task makes
# this dependency explicit; see terraform/modules/workflows/main.tf.
_SUB_OPERATIONS: list[tuple[str, Callable[..., None]]] = [
    ("ingestion.import_psxg_predictions", _make_volume_op("ingestion.import_psxg_predictions")),
    ("ingestion.export_embeddings_training_data", _make_logger_op("ingestion.export_embeddings_training_data")),
    (
        "ingestion.export_shots_on_target",
        _make_watermark_volume_op("ingestion.export_shots_on_target", "wf-export-shots"),
    ),
    ("ingestion.prepare_360_training_data", _make_volume_op("ingestion.prepare_360_training_data")),
    (
        "ingestion.export_scoutgpt_training_data",
        _make_watermark_op("ingestion.export_scoutgpt_training_data", "wf-scoutgpt-export"),
    ),
    (
        "ingestion.publish_spadl_vaep_hf",
        _make_watermark_op("ingestion.publish_spadl_vaep_hf", "wf-publish-spadl-vaep"),
    ),
    (
        "ingestion.publish_xg_shots_hf",
        _make_watermark_op("ingestion.publish_xg_shots_hf", "wf-publish-xg-shots"),
    ),
    (
        "ingestion.publish_freeze_frame_hf",
        _make_watermark_op("ingestion.publish_freeze_frame_hf", "wf-publish-freeze-frames"),
    ),
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
    """Run a single sub-workflow, logging failures at ERROR level and continuing.

    This is an orchestration-level best-effort — the hf_sync workflow runs
    several independent sub-workflows and should complete the remaining ones
    even if one fails. Failures are logged at ERROR level so they surface in
    standard error-log queries; previously-used WARNING level hid real bugs
    (e.g. the 2026-04-12 cost-hook schema drift was a sub-workflow failure
    that nobody saw for 62+ hours).
    """
    try:
        op(spark, catalog, schema, logger_arg)
    except Exception:
        logger_arg.error(
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
    filter_result: FilterResult,
    ctx: Any = None,
) -> int:
    """Run all HF import/export sub-operations sequentially."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No HF sync work")
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

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)
