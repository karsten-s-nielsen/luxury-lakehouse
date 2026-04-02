"""Line-breaking pass detection — Path A (StatsBomb 360 freeze frames).

Reads pass events and opponent positions from StatsBomb 360 freeze frame
data, runs the line-breaking detection algorithm via ``applyInPandas``,
and writes results to the ``line_breaking_results`` bronze table.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from analytics.line_breaking import LineBreakingParams
from ingestion.line_breaking_common import (
    _RESULT_COLUMNS,
    _TABLE_NAME,
    _parse_location,
    _parse_locations_series,
)
from ingestion.utils import merge_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# applyInPandas UDF closure
# ---------------------------------------------------------------------------


def _make_statsbomb_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for StatsBomb 360 data.

    The UDF receives a pandas DataFrame containing one match's worth of
    joined pass + opponent rows (one row per pass-opponent pair).  It
    reconstructs the per-event opponent grouping, parses locations, builds
    ``detect_line_breaking_batch`` inputs, and returns results.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import pandas as _pd

        from analytics.line_breaking import LineBreakingParams as _LBParams
        from analytics.line_breaking import detect_line_breaking_batch as _detect_batch

        if pdf.empty:
            return _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        params = _LBParams()
        match_id = str(pdf["pass_match_id"].iloc[0])

        # Deduplicate passes — each pass appears once per opponent row in the join
        pass_cols = ["pass_id", "pass_location", "pass_end_location", "pass_match_id"]
        passes_dedup = pdf[pass_cols].drop_duplicates(subset=["pass_id"])  # type: ignore[call-overload]

        # Build per-event opponent dict
        opponent_groups = pdf.groupby("pass_id")

        passes_list: list[dict[str, object]] = []
        opponents_by_event: dict[str, _pd.DataFrame] = {}

        for _, pass_row in passes_dedup.iterrows():
            event_id = str(pass_row["pass_id"])

            # Parse pass start/end from JSON location arrays
            start_xy = _parse_location(pass_row["pass_location"])
            end_xy = _parse_location(pass_row["pass_end_location"])
            if start_xy is None or end_xy is None:
                continue

            # Get opponents for this event
            try:
                event_opps = opponent_groups.get_group(pass_row["pass_id"])
            except KeyError:
                continue

            opp_loc_series: _pd.Series[object] = event_opps["opp_location"]  # type: ignore[assignment]
            opp_positions = _parse_locations_series(opp_loc_series)
            if len(opp_positions) < params.min_opponents:
                continue

            passes_list.append(
                {
                    "event_id": event_id,
                    "start_x": start_xy[0],
                    "start_y": start_xy[1],
                    "end_x": end_xy[0],
                    "end_y": end_xy[1],
                }
            )
            opponents_by_event[event_id] = opp_positions

        if not passes_list:
            return _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        passes_df = _pd.DataFrame(passes_list)
        result_df: _pd.DataFrame = _detect_batch(passes_df, opponents_by_event, params)

        result_df["match_id"] = match_id
        result_df["data_source"] = "statsbomb_360"

        return _pd.DataFrame(result_df[_RESULT_COLUMNS])

    return _udf


# ---------------------------------------------------------------------------
# Path A — StatsBomb 360 freeze frames
# ---------------------------------------------------------------------------


def _process_statsbomb_360(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
) -> int:
    """Detect line-breaking passes using StatsBomb 360 freeze frame data.

    Joins passes and opponent freeze frames in Spark, then uses
    ``groupBy("pass_match_id").applyInPandas`` to distribute detection
    across executors.

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.statsbomb_events"
    ff_table = f"{catalog}.{schema}.statsbomb_360"

    # Get distinct match_ids that have 360 data (small query — just unique IDs)
    try:
        match_id_rows = spark.table(ff_table).select("match_id").distinct().collect()
    except Exception:
        logger.exception("Cannot read StatsBomb 360 table")
        return 0

    if not match_id_rows:
        logger.info("No 360 data available — skipping Path A")
        return 0

    match_ids = [row["match_id"] for row in match_id_rows]
    logger.info("Path A: %d matches with 360 data", len(match_ids))

    # Incremental skip — only process matches not already in results table
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
    existing_ids: set[str] = set()
    try:
        existing_rows = (
            spark.table(results_table).filter("data_source = 'statsbomb_360'").select("match_id").distinct().collect()
        )
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in match_ids if str(mid) not in existing_ids]
    logger.info(
        "Path A: %d matches total, %d already processed, %d to process",
        len(match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # --- pyspark imports deferred past early-exit guards (not installed in test env) ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

    # Build Spark DFs for passes and opponents, filtered to new matches only
    new_ids_int = [int(mid) for mid in new_match_ids]

    passes_df = (
        spark.table(events_table)
        .filter(F.col("type") == "Pass")
        .filter(F.col("match_id").isin(new_ids_int))
        .select(
            F.col("id").alias("pass_id"),
            F.col("match_id").alias("pass_match_id"),
            F.col("location").alias("pass_location"),
            F.col("pass_end_location"),
        )
    )

    opponents_df = (
        spark.table(ff_table)
        .filter(F.col("match_id").isin(new_ids_int))
        .filter(~F.col("teammate"))
        .filter(~F.col("actor"))
        .filter(~F.col("keeper"))
        .select(
            F.col("id").alias("opp_event_id"),
            F.col("location").alias("opp_location"),
        )
    )

    # Join passes x opponents on event_id (360 id = events id)
    joined = passes_df.join(opponents_df, passes_df["pass_id"] == opponents_df["opp_event_id"], "inner").drop(
        "opp_event_id"
    )

    # Define output schema for applyInPandas
    result_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("is_line_breaking", BooleanType(), nullable=True),
            StructField("lines_broken", IntegerType(), nullable=True),
            StructField("line_breaking_type", StringType(), nullable=True),
            StructField("data_source", StringType(), nullable=True),
        ]
    )

    udf_fn = _make_statsbomb_udf()

    result_sdf = joined.groupBy("pass_match_id").applyInPandas(
        udf_fn,  # type: ignore[arg-type]
        schema=result_schema,
    )

    written = merge_delta_table(
        result_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        merge_key="event_id",
        logger=logger,
    )

    logger.info("Path A complete: %d rows written", written)
    return written
