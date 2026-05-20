"""EFPI formation detection batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer and
detects team formations using elastic template matching via the Hungarian
method (Shaw & Glickman 2019).  Templates are pre-built on the driver and
serialised into the UDF closure (no mplsoccer import on executors).

Results are written to ``formation_labels`` — window-level formation labels
with ``detector='efpi'``.

Architecture: Uses ``applyInPandas`` grouped by ``(match_id, period, team)``
to distribute formation detection across Spark executors.  Each group is one
team in one half (~7K rows), keeping executor memory well under the 1 GB
serverless limit.

This module also hosts the combined ``run_pipeline()`` / ``main()`` that
orchestrates both EFPI and shape graph detection sequentially for backward
compatibility and local development.

Entry points:
  ``main()`` — runs both detectors sequentially (backward compat / local dev).
  ``main_efpi()`` — runs EFPI detector only (discrete Databricks task).

References:
  Shaw, L. & Glickman, M. (2019). "Dynamic analysis of team strategy
  in professional football."
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.formations_common import (
    RESULT_COLUMNS,
    TABLE_NAME,
    prepare_tracking_data,
)
from ingestion.guards import FilterResult, timed_check
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    parse_ingestion_args,
    write_delta_table,
)
from workflows import workflow
from workflows.exceptions import WorkflowSkippedError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class _FormationsEfpiGuard:
    """SkipGuard adapter for EFPI formation detection pipeline."""

    workflow_id = "wf-formations"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which tracking matches need formation detection."""
        from ingestion.formations_common import find_incomplete_formation_ids

        new_ids = find_incomplete_formation_ids(spark, catalog, schema)
        if not new_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_ids),
            metadata={"new_match_ids": new_ids},
        )


skip_guard = _FormationsEfpiGuard()


# ---------------------------------------------------------------------------
# applyInPandas UDF closure
# ---------------------------------------------------------------------------


def _make_formation_udf(
    window_seconds: int,
    min_outfield_players: int,
    serialized_templates: dict[int, dict[str, dict[str, object]]],
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for formation detection.

    Scalar params and pre-serialized templates are captured by the closure so
    they are serialised with the UDF and available on executors without network
    access.  The UDF does NOT import ``mplsoccer`` — templates are
    reconstructed from plain dicts + numpy arrays on the executor.

    Parameters
    ----------
    window_seconds : Time window length in seconds.
    min_outfield_players : Minimum outfield players for detection.
    serialized_templates : Output of ``templates_to_serializable()`` — plain
        dicts with numpy arrays and string lists (pickle-safe).

    Returns
    -------
    A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
    ``applyInPandas`` grouped by ``("match_id", "period", "team")``.
    """
    _window_seconds = window_seconds
    _min_outfield = min_outfield_players
    _ser_templates = serialized_templates
    _result_columns = RESULT_COLUMNS

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Detect formations for one (match_id, period, team) group."""
        import pandas as _pd

        from analytics.formation_detection import (
            FormationParams,
            process_group_formations,
            templates_from_serializable,
        )

        _empty = _pd.DataFrame(columns=_pd.Index(_result_columns))

        if pdf.empty:
            return _empty

        match_id = str(pdf["match_id"].iloc[0])
        period = int(pdf["period"].iloc[0])
        team = str(pdf["team"].iloc[0])
        source_provider = str(pdf["source_provider"].iloc[0]) if "source_provider" in pdf.columns else None

        params = FormationParams(
            window_seconds=_window_seconds,
            min_outfield_players=_min_outfield,
        )

        # Reconstruct templates from serialized data (no mplsoccer import)
        templates = templates_from_serializable(_ser_templates)

        # Filter to outfield players:
        # - player_id must be non-null (excludes ball rows)
        # - team must be non-null (excludes unassigned rows)
        # - is_goalkeeper must be False (templates only cover 8-10 outfield players)
        # fillna(False) handles NULL values; the column-presence guard handles
        # DataFrames that pre-date the is_goalkeeper column (e.g. unit tests).
        if "is_goalkeeper" not in pdf.columns:
            pdf["is_goalkeeper"] = False
        gk_flag: pd.Series = pdf["is_goalkeeper"].fillna(False)  # type: ignore[assignment]
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna() & pdf["team"].notna() & ~gk_flag])
        if pdf.empty:
            return _empty

        result = process_group_formations(pdf, match_id, period, team, templates, params)

        if len(result) > 0:
            result = result.copy()
            result["detector"] = "efpi"
            result["source_provider"] = source_provider
            return _pd.DataFrame(result[_result_columns])
        return _empty

    return _udf


# ---------------------------------------------------------------------------
# EFPI detector
# ---------------------------------------------------------------------------


def _run_efpi(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    new_match_ids: list[str] | None = None,
) -> int:
    """Run the EFPI formation detector on all new matches.

    Calls ``prepare_tracking_data()`` to materialise filtered tracking data.
    Writes EFPI formation labels to ``formation_labels``.  Does NOT drop the
    temp table (shape graph may still need it).

    Parameters
    ----------
    new_match_ids : list[str] | None
        Pre-computed list of new match IDs from the guard. When provided,
        ``prepare_tracking_data()`` is still called for data materialisation
        but the guard's ID list takes precedence.

    Returns the number of rows written.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from analytics.formation_detection import (
        FormationParams,
        build_formation_templates,
        templates_to_serializable,
    )

    prepared = prepare_tracking_data(spark, catalog, schema, logger)
    if prepared is None:
        return 0

    tracking_df, new_ids_str, _temp_table = prepared
    # If guard provided IDs, use them (they may be a subset of what
    # prepare_tracking_data discovered, or match exactly)
    if new_match_ids is not None:
        new_ids_str = new_match_ids

    params = FormationParams()

    # --- Build templates on the DRIVER (imports mplsoccer here, not on executors) ---
    driver_templates = build_formation_templates()
    serialized_templates = templates_to_serializable(driver_templates)
    logger.info("Formation templates serialized for UDF closure (%d player counts)", len(serialized_templates))

    efpi_udf_fn = _make_formation_udf(
        window_seconds=params.window_seconds,
        min_outfield_players=params.min_outfield_players,
        serialized_templates=serialized_templates,
    )

    efpi_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=False),
            StructField("period", IntegerType(), nullable=False),
            StructField("team", StringType(), nullable=False),
            StructField("window_start_s", DoubleType(), nullable=False),
            StructField("window_end_s", DoubleType(), nullable=False),
            StructField("formation_label", StringType(), nullable=False),
            StructField("cost", DoubleType(), nullable=True),
            StructField("detector", StringType(), nullable=False),
            StructField("source_provider", StringType(), nullable=True),
        ]
    )

    efpi_df = tracking_df.groupBy("match_id", "period", "team").applyInPandas(
        efpi_udf_fn,  # type: ignore[arg-type]
        schema=efpi_schema,
    )

    ids_sql = ", ".join(f"'{mid}'" for mid in new_ids_str)
    written = write_delta_table(
        efpi_df,
        catalog,
        schema,
        TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )
    logger.info("EFPI formation labels written: %d rows", written)
    return written


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-formations", phase="heuristic")
def run_pipeline_efpi(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Execute the EFPI formation detection pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    new_match_ids = filter_result.metadata.get("new_match_ids")
    total = _run_efpi(spark, catalog, schema, logger, new_match_ids=new_match_ids)
    logger.info("EFPI formation detection complete -- %d rows written", total)
    return total


def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> int:
    """Execute both formation detection pipelines sequentially.

    Convenience function for local development and backward compatibility.
    Runs EFPI first (which creates the temp table), then shape graph (which
    reads the temp table and drops it).
    """
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    from ingestion.formations_shape_graph import _run_shape_graph

    new_match_ids = filter_result.metadata.get("new_match_ids")
    efpi_total = _run_efpi(spark, catalog, schema, logger, new_match_ids=new_match_ids)
    sg_total = _run_shape_graph(spark, catalog, schema, logger, new_match_ids=new_match_ids)
    logger.info(
        "Formation detection pipeline complete -- %d total rows written (EFPI: %d, shape graph: %d)",
        efpi_total + sg_total,
        efpi_total,
        sg_total,
    )
    return efpi_total + sg_total


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for formation detection (both detectors)."""
    args = parse_ingestion_args("Detect team formations from tracking data")
    logger = configure_logging("formations")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting formation detection pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger, filter_result=filter_result)


def main_efpi() -> None:
    """CLI entry point for EFPI formation detection only."""
    args = parse_ingestion_args("Detect team formations via EFPI template matching")
    logger = configure_logging("formations_efpi")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting EFPI formation detection pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline_efpi(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main()
