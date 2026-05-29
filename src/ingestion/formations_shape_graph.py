"""Shape graph formation detection batch computation pipeline.

Reads tracking frames from ``fct_tracking_frames`` in the gold layer and
detects team formations using Delaunay-based stable subgraph with face-center
position decomposition (Sotudeh 2026).  Produces both formation labels and
per-window player position assignments.

Results are written to two bronze tables:

* ``formation_labels`` — window-level formation labels with
  ``detector='shape_graph'``.
* ``player_positions`` — per-window player position labels from the shape graph
  detector, one row per player per window midpoint frame.

Architecture: Uses ``applyInPandas`` grouped by ``(match_id, period, team)``
to distribute formation detection across Spark executors.  Each group is one
team in one half (~7K rows), keeping executor memory well under the 1 GB
serverless limit.

Entry points:
  ``main_shape_graph()`` — runs shape graph detector only (discrete Databricks task).

References:
  Sotudeh, H. (2026). "Identification of Team Tactical Formations and Player
  Positions in Association Football." PhD thesis, ETH Zurich.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.formations_common import (
    POSITIONS_TABLE_NAME,
    RESULT_COLUMNS,
    TABLE_NAME,
    TEMP_TABLE_SUFFIX,
    attacking_direction,
    derive_formation_label,
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
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

from ingestion.utils import SparkAnalysisException as _SparkAnalysisException


class _FormationsShapeGraphGuard:
    """SkipGuard adapter for shape graph formation detection pipeline."""

    workflow_id = "wf-shape-graphs"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which tracking matches need shape graph detection."""
        from ingestion.formations_common import find_incomplete_formation_ids

        new_ids = find_incomplete_formation_ids(spark, catalog, schema)
        if not new_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)
        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_ids),
            metadata={"new_match_ids": new_ids},
        )


skip_guard = _FormationsShapeGraphGuard()


# ---------------------------------------------------------------------------
# Shape graph UDF closure
# ---------------------------------------------------------------------------


def _make_shape_graph_udf(
    window_seconds: int = 300,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for shape graph detection.

    Returns a tuple-style output: a *formation rows* DataFrame and a *position
    rows* DataFrame concatenated row-wise with a ``_row_type`` discriminator
    column.  The caller splits them back apart after ``applyInPandas``.

    We use a single UDF (rather than two separate passes) so that each
    (match_id, period, team) group is only materialised once on an executor.

    The combined schema is the *union* of formation columns and position columns
    (missing columns filled with NULL).  ``_row_type`` is ``"formation"`` or
    ``"position"``.
    """
    _window_seconds = window_seconds
    _result_columns = RESULT_COLUMNS

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Detect formations + positions for one (match_id, period, team) group."""
        import numpy as np
        import pandas as _pd

        from analytics.shape_graph import compute_shape_graph, infer_positions

        # Combined schema columns (union of formation + position columns + _row_type)
        _combined_columns = [
            "match_id",
            "period",
            "team",
            "window_start_s",
            "window_end_s",
            "formation_label",
            "cost",
            "detector",
            "frame_id",
            "player_id",
            "position_label",
            "vertical_level",
            "horizontal_level",
            "source_provider",  # PR-1.5: propagate to bronze
            "_row_type",
        ]
        _empty = _pd.DataFrame(columns=_pd.Index(_combined_columns))

        if pdf.empty:
            return _empty

        match_id = str(pdf["match_id"].iloc[0])
        period = int(pdf["period"].iloc[0])
        team = str(pdf["team"].iloc[0])
        # PR-1.5: propagate source_provider from fct_tracking_frames to bronze
        source_provider = str(pdf["source_provider"].iloc[0]) if "source_provider" in pdf.columns else None
        direction = attacking_direction(team, period)

        # Filter to outfield players (same guards as EFPI UDF)
        if "is_goalkeeper" not in pdf.columns:
            pdf["is_goalkeeper"] = False
        gk_flag: _pd.Series = pdf["is_goalkeeper"].fillna(False)  # type: ignore[assignment]
        pdf = _pd.DataFrame(pdf[pdf["player_id"].notna() & pdf["team"].notna() & ~gk_flag])
        if pdf.empty:
            return _empty

        ts = pdf["timestamp_seconds"].to_numpy(dtype=np.float64)
        ts_min = float(ts.min())
        ts_max = float(ts.max())

        formation_rows: list[dict[str, object]] = []
        position_rows: list[dict[str, object]] = []

        # Build frame column lookup (may be absent in unit tests)
        has_frame = "frame" in pdf.columns

        window_start = ts_min
        while window_start < ts_max:
            window_end = window_start + _window_seconds

            mask = (ts >= window_start) & (ts < window_end)
            window_df = pdf[mask]
            if len(window_df) == 0:
                window_start = window_end
                continue

            # Compute mean position per player within the window
            player_means = window_df.groupby("player_id")[["x", "y"]].mean()
            if len(player_means) < 3:
                window_start = window_end
                continue

            player_ids = list(player_means.index)
            positions = player_means[["x", "y"]].values.astype(np.float64)

            sg = compute_shape_graph(positions)
            pos_labels = infer_positions(sg, positions, direction)

            if not pos_labels:
                window_start = window_end
                continue

            # Derive formation label from vertical level counts
            verticals = [pl.vertical for pl in pos_labels]
            formation_label = derive_formation_label(verticals)

            # Formation row (cost is NaN -- shape graph has no template matching cost)
            formation_rows.append(
                {
                    "match_id": match_id,
                    "period": period,
                    "team": team,
                    "window_start_s": window_start,
                    "window_end_s": min(window_end, ts_max),
                    "formation_label": formation_label,
                    "cost": float("nan"),
                    "detector": "shape_graph",
                    "frame_id": None,
                    "player_id": None,
                    "position_label": None,
                    "vertical_level": None,
                    "horizontal_level": None,
                    "source_provider": source_provider,
                    "_row_type": "formation",
                }
            )

            # Position rows -- one per player, frame_id = window midpoint frame.
            # If the tracking data has a 'frame' column, pick the frame closest
            # to the window midpoint timestamp.  Otherwise use a synthetic integer
            # derived from the midpoint timestamp.
            mid_ts = window_start + _window_seconds / 2.0
            if has_frame:
                frame_col = window_df["frame"].values
                ts_col = window_df["timestamp_seconds"].to_numpy(dtype=np.float64)
                mid_idx = int(np.argmin(np.abs(ts_col - mid_ts)))
                mid_frame = int(frame_col[mid_idx])
            else:
                mid_frame = int(mid_ts)

            for pid, pl in zip(player_ids, pos_labels, strict=True):
                position_rows.append(
                    {
                        "match_id": match_id,
                        "frame_id": mid_frame,
                        "player_id": str(pid),
                        "team": team,
                        "position_label": pl.label,
                        "vertical_level": pl.vertical,
                        "horizontal_level": pl.horizontal,
                        "detector": "shape_graph",
                        "period": period,
                        "window_start_s": None,
                        "window_end_s": None,
                        "formation_label": None,
                        "cost": None,
                        "source_provider": source_provider,
                        "_row_type": "position",
                    }
                )

            window_start = window_end

        all_rows = formation_rows + position_rows
        if not all_rows:
            return _empty
        return _pd.DataFrame(all_rows, columns=_pd.Index(_combined_columns))

    return _udf


# ---------------------------------------------------------------------------
# Shape graph detector
# ---------------------------------------------------------------------------


def _run_shape_graph(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    new_match_ids: list[str] | None = None,
) -> int:
    """Run the shape graph formation detector on all new matches.

    First attempts to read from the temp Delta table written by
    ``prepare_tracking_data()``.  If the temp table does not exist (standalone
    run without prior EFPI), calls ``prepare_tracking_data()`` as a fallback.

    Writes shape graph formation labels to ``formation_labels`` and player
    positions to ``player_positions``.  Drops the temp table on completion.

    Parameters
    ----------
    new_match_ids : list[str] | None
        Pre-computed list of new match IDs from the guard. When provided,
        takes precedence over IDs discovered from the temp table or
        ``prepare_tracking_data()``.

    Returns the total number of rows written across both tables.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    temp_table = f"{catalog}.{schema}.{TEMP_TABLE_SUFFIX}"

    # Try to read from the temp table (written by a preceding EFPI run).
    tracking_df: SparkDataFrame | None = None
    new_ids_str: list[str] | None = None
    try:
        temp_df = spark.table(temp_table)
        # Extract match IDs from the temp table
        new_ids_str = [str(row["match_id"]) for row in temp_df.select("match_id").distinct().collect()]
        if not new_ids_str:
            pass  # fall through to prepare_tracking_data below
        else:
            tracking_df = temp_df
            logger.info("Read %d match IDs from existing temp table %s", len(new_ids_str), temp_table)
    except _SparkAnalysisException:
        logger.info("Temp table %s not found -- preparing tracking data from scratch", temp_table)

    # Fallback: prepare from gold table if temp table is unavailable.
    if tracking_df is None or new_ids_str is None:
        prepared = prepare_tracking_data(spark, catalog, schema, logger)
        if prepared is None:
            return 0
        tracking_df, new_ids_str, temp_table = prepared

    # At this point tracking_df and new_ids_str are guaranteed non-None
    # (either from temp table or from prepare_tracking_data fallback).
    if tracking_df is None or new_ids_str is None:  # pragma: no cover -- defensive guard
        logger.error("tracking_df or new_ids_str unexpectedly None after preparation")
        return 0

    # If guard provided IDs, use them (they may be a subset of what
    # prepare_tracking_data discovered, or match exactly)
    if new_match_ids is not None:
        new_ids_str = new_match_ids

    from analytics.formation_detection import FormationParams

    params = FormationParams()
    sg_udf_fn = _make_shape_graph_udf(window_seconds=params.window_seconds)

    # Combined output schema (union of formation + position columns + _row_type).
    # The UDF returns both formation and position rows in a single DataFrame.
    sg_combined_schema = StructType(
        [
            StructField("match_id", StringType(), nullable=False),
            StructField("period", IntegerType(), nullable=True),
            StructField("team", StringType(), nullable=False),
            StructField("window_start_s", DoubleType(), nullable=True),
            StructField("window_end_s", DoubleType(), nullable=True),
            StructField("formation_label", StringType(), nullable=True),
            StructField("cost", DoubleType(), nullable=True),
            StructField("detector", StringType(), nullable=False),
            StructField("frame_id", LongType(), nullable=True),
            StructField("player_id", StringType(), nullable=True),
            StructField("position_label", StringType(), nullable=True),
            StructField("vertical_level", StringType(), nullable=True),
            StructField("horizontal_level", StringType(), nullable=True),
            StructField("source_provider", StringType(), nullable=True),  # PR-1.5
            StructField("_row_type", StringType(), nullable=False),
        ]
    )

    sg_combined_df = tracking_df.groupBy("match_id", "period", "team").applyInPandas(
        sg_udf_fn,  # type: ignore[arg-type]
        schema=sg_combined_schema,
    )

    # Split shape graph results into formation rows and position rows
    sg_formation_df = sg_combined_df.filter(F.col("_row_type") == "formation").select(
        "match_id",
        "period",
        "team",
        "window_start_s",
        "window_end_s",
        "formation_label",
        "cost",
        "detector",
        "source_provider",
    )
    sg_position_df = sg_combined_df.filter(F.col("_row_type") == "position").select(
        "match_id",
        "frame_id",
        "player_id",
        "team",
        "position_label",
        "vertical_level",
        "horizontal_level",
        "detector",
        "source_provider",  # PR-1.5: propagate to bronze
    )

    total_written = 0
    ids_sql = ", ".join(f"'{mid}'" for mid in new_ids_str)

    # Write formation_labels (shape graph only)
    written_formations = write_delta_table(
        sg_formation_df,
        catalog,
        schema,
        TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )
    total_written += written_formations
    logger.info("Shape graph formation labels written: %d rows", written_formations)

    # Write player_positions (shape graph only)
    written_positions = write_delta_table(
        sg_position_df,
        catalog,
        schema,
        POSITIONS_TABLE_NAME,
        replace_where=f"match_id IN ({ids_sql})",
        logger=logger,
    )
    total_written += written_positions
    logger.info("Shape graph player positions written: %d rows", written_positions)

    # Clean up temp table
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
        logger.info("Dropped temp table %s", temp_table)
    except _SparkAnalysisException:
        logger.warning("Could not drop temp table %s -- manual cleanup needed", temp_table)

    return total_written


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-shape-graphs", phase="heuristic")
def run_pipeline_shape_graph(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    filter_result: FilterResult,
    ctx: object = None,
) -> None:
    """Execute the shape graph formation detection pipeline."""
    if filter_result.count == 0:
        raise WorkflowSkippedError("No new work")

    new_match_ids = filter_result.metadata.get("new_match_ids")
    total = _run_shape_graph(spark, catalog, schema, logger, new_match_ids=new_match_ids)
    logger.info("Shape graph formation detection complete -- %d rows written", total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main_shape_graph() -> None:
    """CLI entry point for shape graph formation detection only."""
    args = parse_ingestion_args("Detect team formations via shape graph method")
    logger = configure_logging("formations_shape_graph")
    spark = get_spark_session()

    from ingestion.bootstrap import bootstrap_hooks

    bootstrap_hooks(spark, args.catalog, args.schema)

    filter_result = timed_check(skip_guard, spark, args.catalog, args.schema)

    logger.info("Starting shape graph formation detection pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline_shape_graph(spark, args.catalog, args.schema, logger, filter_result=filter_result)


if __name__ == "__main__":
    main_shape_graph()
