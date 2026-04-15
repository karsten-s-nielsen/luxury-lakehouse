"""Port/adapter infrastructure for pipeline skip guards.

Each workflow exposes a :class:`SkipGuard` adapter whose ``check()``
method returns a :class:`FilterResult` describing whether the workflow
has new work and how to chunk it for fan-out.

Each pipeline's ``main()`` calls its guard's ``check()`` at startup
and raises ``WorkflowSkippedError`` when ``count == 0``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class FilterResult:
    """Result of a single workflow's skip guard check.

    Attributes:
        workflow_id: The ``wf-xxx`` identifier matching the workflow card.
        count: Number of unprocessed items.  ``0`` means skip entirely.
        chunks: Pre-computed fan-out partitions — a list of ID lists.
            ``None`` means single-task execution (no fan-out).
            ``len(chunks) > 1`` triggers ``for_each_task``.
            The adapter owns chunk sizing (knows its data shape).
        metadata: Pass-through context for the pipeline — avoids
            re-computing what the guard already discovered (e.g.,
            ``need_global`` flag, competitions DataFrame).
        guard_duration_seconds: Wall-clock time the guard check took.
            Populated by :func:`timed_check`, ``None`` for legacy callers.
    """

    workflow_id: str
    count: int
    chunks: list[list[str]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    guard_duration_seconds: int | None = None


def timed_check(guard: SkipGuard, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
    """Run a guard's ``check()`` and record its wall-clock duration.

    Returns a new :class:`FilterResult` with ``guard_duration_seconds``
    populated.  All fields from the guard's result (including ``metadata``
    with pre-computed IDs) are preserved unchanged.
    """
    start = time.monotonic()
    result = guard.check(spark, catalog, schema)
    elapsed = round(time.monotonic() - start)
    return FilterResult(
        workflow_id=result.workflow_id,
        count=result.count,
        chunks=result.chunks,
        metadata=result.metadata,
        guard_duration_seconds=elapsed,
    )


def ensure_table(spark: SparkSession, table_name: str, schema_ddl: str) -> None:
    """Create a Delta table if it does not exist.

    Called by guards before ``find_new_ids()`` to guarantee the results
    table exists.  On first-ever pipeline run the table is empty and the
    anti-join correctly returns all source IDs.  After the first write,
    this is a metadata-only no-op (~100 ms).

    Args:
        spark: Active SparkSession.
        table_name: Fully-qualified table name (``catalog.schema.table``).
        schema_ddl: SQL column definitions (e.g., ``"match_id STRING, value DOUBLE"``).
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_ddl}) USING DELTA"
        " TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true',"
        " 'delta.autoOptimize.optimizeWrite' = 'true')"
    )


def find_new_ids(
    spark: SparkSession,
    source_table: str,
    results_table: str,
    id_column: str = "match_id",
    *,
    results_id_column: str | None = None,
    source_filter: str | None = None,
    results_filter: str | None = None,
) -> list[str]:
    """Spark-native anti-join to find IDs present in source but not in results.

    Pushes the set-difference to Spark executors via LEFT ANTI JOIN,
    collecting only the (small) list of new IDs to the driver.
    All IDs are cast to string for consistent cross-system normalization.

    The results table **must exist** before calling this function.  Use
    :func:`ensure_table` in the guard's ``check()`` to create it on first run.

    Args:
        spark: Active SparkSession.
        source_table: Fully-qualified source table (e.g., ``catalog.schema.table``).
        results_table: Fully-qualified results table (must exist).
        id_column: Column name for the join key in the source table (default ``match_id``).
        results_id_column: Column name for the join key in the results table.
            Defaults to ``id_column`` when source and results use the same name.
            Use when comparing bronze (raw schema) against gold (canonical schema),
            e.g., ``id_column="matchId", results_id_column="match_id"``.
        source_filter: Optional SQL filter expression for source table.
        results_filter: Optional SQL filter expression for results table.

    Returns:
        List of string IDs present in source but absent from results.
        Empty list if source is empty or all IDs are already processed.

    Raises:
        AnalysisException: If the results table does not exist (call
            ``ensure_table`` first).
    """
    from pyspark.sql import functions as F  # noqa: N812

    res_col = results_id_column or id_column
    join_alias = "_join_id"

    source_df = spark.table(source_table)
    if source_filter:
        source_df = source_df.filter(source_filter)
    source_df = source_df.select(F.col(id_column).cast("string").alias(join_alias)).distinct()

    results_df = spark.table(results_table)
    if results_filter:
        results_df = results_df.filter(results_filter)
    results_df = results_df.select(F.col(res_col).cast("string").alias(join_alias)).distinct()

    new_df = source_df.join(results_df, on=join_alias, how="left_anti")
    rows = new_df.collect()
    return [str(row[join_alias]) for row in rows]


class SkipGuard(Protocol):
    """Port: each workflow exposes its freshness check.

    Implementations live alongside their pipeline module as a
    module-level ``skip_guard`` object or function.
    """

    workflow_id: str

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Run the skip guard and return a FilterResult.

        Must be safe to call from the ``default`` environment —
        only Spark SQL, no analytics imports.
        """
        ...


_GUARD_MODULES: list[str] = [
    "ingestion.pitch_control_batch",
    "ingestion.off_ball_xt",
    "ingestion.defcon_lite_360",
    "ingestion.defcon_lite_tracking",
    "ingestion.elastic_sync",
    "ingestion.pausa",
    "ingestion.line_breaking",
    "ingestion.formations_efpi",
    "ingestion.formations_shape_graph",
    "ingestion.spadl_vaep",
    "ingestion.player_embeddings_v1",
    "ingestion.xg_model",
    "ingestion.xg_model_v2",
    "ingestion.expected_threat",
    "ingestion.export_embeddings_training_data",
    "ingestion.prepare_360_training_data",
    "ingestion.entity_resolution",
    "ingestion.statsbomb",
    "ingestion.statsbomb_backfill_extra",
    "ingestion.statsbomb_backfill_360",
    "ingestion.metrica",
    "ingestion.wyscout",
    "ingestion.idsse",
    "ingestion.idsse_events",
    "ingestion.skillcorner",
    "ingestion.import_obso_results",
    "ingestion.import_psxg_predictions",
    "ingestion.import_space_creation",
    "ingestion.tracking_metadata",
    "ingestion.model_validation",
    "ingestion.sync_hf_costs",
    "ingestion.player_embeddings_v2",
    "ingestion.hf_sync",
]


def get_workflow_guards() -> dict[str, SkipGuard]:
    """Build the guard registry with per-module imports.

    Returns a dict mapping ``workflow_id`` to its ``SkipGuard`` adapter.
    Each module is imported individually so that a missing third-party
    dependency (e.g., ``statsbombpy`` in the ``default`` environment)
    skips that guard rather than crashing the entire registry.
    """
    import importlib
    import logging

    _logger = logging.getLogger(__name__)
    guards: dict[str, SkipGuard] = {}

    for module_path in _GUARD_MODULES:
        try:
            mod = importlib.import_module(module_path)
            guard = mod.skip_guard  # type: ignore[attr-defined]
            guards[guard.workflow_id] = guard
        except (ImportError, ModuleNotFoundError, AttributeError):
            # ImportError: guard module itself failed to import (missing dep).
            # AttributeError: guard module exists but doesn't expose `skip_guard`.
            # Both are legitimate "skip this guard" cases on first install.
            _logger.error("Skipping guard from %s (import failed)", module_path, exc_info=True)

    return guards
