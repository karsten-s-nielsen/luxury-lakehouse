"""Line-breaking pass detection batch computation pipeline.

Reads pass events and opponent positions from existing bronze Delta tables,
runs the line-breaking detection algorithm, and writes results to a new
``line_breaking_results`` bronze table.

Two data paths:
  - **Path A (360 freeze frames):** StatsBomb matches with per-event opponent
    positions from ``statsbomb_360``.
  - **Path B (Metrica tracking):** Metrica matches with frame-level tracking
    joined to event data.

Architecture: Uses ``applyInPandas`` to distribute line-breaking detection
across Spark executors instead of sequential per-match driver loops.  Each
match group is processed independently via ``detect_line_breaking_batch``
with Ward cluster caching.

Design: "Read from bronze, compute, write to bronze." No external API calls.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from analytics.line_breaking import LineBreakingParams
from ingestion.utils import (
    configure_logging,
    get_spark_session,
    merge_delta_table,
    parse_ingestion_args,
)
from workflows import workflow

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_TABLE_NAME = "line_breaking_results"
_XY_COLS = pd.Index(["x", "y"])


# ---------------------------------------------------------------------------
# JSON parsing helpers
# ---------------------------------------------------------------------------


def _parse_location(loc: object) -> tuple[float, float] | None:
    """Parse a location value to ``(x, y)`` tuple.

    Handles JSON strings (``'[x, y]'``) and Python lists/tuples.
    """
    if loc is None:
        return None
    if isinstance(loc, float) and pd.isna(loc):
        return None
    if isinstance(loc, str) and loc.strip() in ("", "null", "None"):
        return None
    if isinstance(loc, (list, tuple)) and len(loc) >= 2:
        return (float(loc[0]), float(loc[1]))
    try:
        coords = json.loads(str(loc))
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            return (float(coords[0]), float(coords[1]))
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None


def _parse_locations_series(loc_series: pd.Series) -> pd.DataFrame:  # type: ignore[type-arg]
    """Parse a Series of location values to a DataFrame with ``x, y`` columns."""
    positions: list[dict[str, float]] = []
    for loc in loc_series:
        parsed = _parse_location(loc)
        if parsed is not None:
            positions.append({"x": parsed[0], "y": parsed[1]})
    return pd.DataFrame(positions) if positions else pd.DataFrame(columns=_XY_COLS)


def _parse_tracking_json(json_str: object) -> pd.DataFrame:
    """Parse a Metrica tracking JSON dict to DataFrame with ``x, y`` columns.

    Input format: ``'{"11": {"x": 0.43, "y": 0.62}, "7": {"x": 0.51, "y": 0.33}}'``

    Returns DataFrame with one row per player.
    """
    if json_str is None:
        return pd.DataFrame(columns=_XY_COLS)
    if isinstance(json_str, float) and pd.isna(json_str):
        return pd.DataFrame(columns=_XY_COLS)
    try:
        players = json.loads(str(json_str))
        if not isinstance(players, dict):
            return pd.DataFrame(columns=_XY_COLS)
        positions: list[dict[str, float]] = []
        for _jersey, coords in players.items():
            if isinstance(coords, dict) and "x" in coords and "y" in coords:
                x_val, y_val = coords["x"], coords["y"]
                if x_val is not None and y_val is not None:
                    positions.append({"x": float(x_val), "y": float(y_val)})
        return pd.DataFrame(positions) if positions else pd.DataFrame(columns=_XY_COLS)
    except (json.JSONDecodeError, ValueError, TypeError):
        return pd.DataFrame(columns=_XY_COLS)


# ---------------------------------------------------------------------------
# applyInPandas UDF closures
# ---------------------------------------------------------------------------

_RESULT_COLUMNS = ["event_id", "match_id", "is_line_breaking", "lines_broken", "line_breaking_type", "data_source"]


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


def _make_metrica_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for Metrica tracking data.

    The UDF receives a pandas DataFrame containing one match's worth of
    joined pass + tracking rows (one row per pass with tracking JSON).
    It parses opponent positions from the JSON columns, converts Metrica
    0-1 coordinates to StatsBomb 120x80, and runs detection.

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
        match_id = str(pdf["evt_match_id"].iloc[0])

        passes_list: list[dict[str, object]] = []
        opponents_by_event: dict[str, _pd.DataFrame] = {}

        for _, row in pdf.iterrows():
            event_id = str(row.get("evt_event_id", ""))

            # Determine opponent team from event's team field
            event_team = str(row.get("evt_team", ""))
            if event_team == "Home":
                opp_json_str = row.get("trk_away_players")
            elif event_team == "Away":
                opp_json_str = row.get("trk_home_players")
            else:
                continue

            # Parse opponent positions from JSON dict
            opp_positions = _parse_tracking_json(opp_json_str)
            if len(opp_positions) < params.min_opponents:
                continue

            # Convert Metrica 0-1 -> StatsBomb 120x80 (y-flip: Metrica y=0 is top)
            opp_positions["x"] = opp_positions["x"] * 120.0
            opp_positions["y"] = (1.0 - opp_positions["y"]) * 80.0

            # Pass coordinates (also 0-1 -> 120x80 with y-flip)
            start_x = float(row.get("evt_start_x", 0) or 0) * 120.0
            start_y = (1.0 - float(row.get("evt_start_y", 0) or 0)) * 80.0
            end_x = float(row.get("evt_end_x", 0) or 0) * 120.0
            end_y = (1.0 - float(row.get("evt_end_y", 0) or 0)) * 80.0

            passes_list.append(
                {
                    "event_id": event_id,
                    "start_x": start_x,
                    "start_y": start_y,
                    "end_x": end_x,
                    "end_y": end_y,
                }
            )
            opponents_by_event[event_id] = opp_positions

        if not passes_list:
            return _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        passes_df = _pd.DataFrame(passes_list)
        result_df: _pd.DataFrame = _detect_batch(passes_df, opponents_by_event, params)

        result_df["match_id"] = match_id
        result_df["data_source"] = "metrica_tracking"

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


# ---------------------------------------------------------------------------
# Path B — Metrica tracking + events
# ---------------------------------------------------------------------------


def _process_metrica_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
) -> int:
    """Detect line-breaking passes using Metrica tracking + event data.

    Joins pass events with tracking frames in Spark on match_id and frame,
    then uses ``groupBy("evt_match_id").applyInPandas`` to distribute
    detection across executors.

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.metrica_events"
    tracking_table = f"{catalog}.{schema}.metrica_tracking"

    # Get distinct match_ids from pass events
    try:
        match_ids_rows = spark.table(events_table).filter("type = 'PASS'").select("match_id").distinct().collect()
    except Exception:
        logger.exception("Cannot read Metrica events table")
        return 0

    if not match_ids_rows:
        logger.info("No PASS events in Metrica events — skipping Path B")
        return 0

    match_ids = [row["match_id"] for row in match_ids_rows]
    logger.info("Path B: %d matches with PASS events", len(match_ids))

    # Incremental skip — only process matches not already in results table
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
    existing_ids: set[str] = set()
    try:
        existing_rows = (
            spark.table(results_table)
            .filter("data_source = 'metrica_tracking'")
            .select("match_id")
            .distinct()
            .collect()
        )
        existing_ids = {str(row["match_id"]) for row in existing_rows}
    except Exception:
        logger.info("No existing %s table — processing all matches", results_table)

    new_match_ids = [mid for mid in match_ids if str(mid) not in existing_ids]
    logger.info(
        "Path B: %d matches total, %d already processed, %d to process",
        len(match_ids),
        len(existing_ids),
        len(new_match_ids),
    )

    if not new_match_ids:
        return 0

    # --- pyspark imports deferred past early-exit guards (not installed in test env) ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

    # Build Spark DFs for passes and tracking, filtered to new matches
    new_ids_str = [str(mid) for mid in new_match_ids]

    passes_df = (
        spark.table(events_table)
        .filter(F.col("type") == "PASS")
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("event_id").alias("evt_event_id"),
            F.col("match_id").alias("evt_match_id"),
            F.col("team").alias("evt_team"),
            F.col("start_frame").alias("evt_start_frame"),
            F.col("start_x").alias("evt_start_x"),
            F.col("start_y").alias("evt_start_y"),
            F.col("end_x").alias("evt_end_x"),
            F.col("end_y").alias("evt_end_y"),
        )
        .filter(F.col("evt_start_frame").isNotNull())
    )

    tracking_df = (
        spark.table(tracking_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("match_id").alias("trk_match_id"),
            F.col("frame").alias("trk_frame"),
            F.col("home_players").alias("trk_home_players"),
            F.col("away_players").alias("trk_away_players"),
        )
    )

    # Join passes x tracking on match_id and frame = start_frame
    joined = passes_df.join(
        tracking_df,
        (passes_df["evt_match_id"] == tracking_df["trk_match_id"])
        & (passes_df["evt_start_frame"].cast("int") == tracking_df["trk_frame"]),
        "inner",
    ).drop("trk_match_id", "trk_frame")

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

    udf_fn = _make_metrica_udf()

    result_sdf = joined.groupBy("evt_match_id").applyInPandas(
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

    logger.info("Path B complete: %d rows written", written)
    return written


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


@workflow("wf-line-breaking", phase="heuristic")
def run_pipeline(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    *,
    ctx=None,
) -> None:
    """Execute the line-breaking detection pipeline."""
    params = LineBreakingParams()

    path_a_rows = _process_statsbomb_360(spark, catalog, schema, logger, params)
    path_b_rows = _process_metrica_tracking(spark, catalog, schema, logger, params)

    total = path_a_rows + path_b_rows
    logger.info("Line-breaking pipeline complete — %d total rows written", total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for line-breaking pass detection."""
    args = parse_ingestion_args("Detect line-breaking passes from 360 and tracking data")
    logger = configure_logging("line_breaking")
    spark = get_spark_session()

    logger.info("Starting line-breaking pipeline into %s.%s", args.catalog, args.schema)
    run_pipeline(spark, args.catalog, args.schema, logger)


if __name__ == "__main__":
    main()
