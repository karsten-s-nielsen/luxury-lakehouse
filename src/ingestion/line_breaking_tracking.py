"""Line-breaking pass detection — Path B (Metrica) and Path C (IDSSE) tracking.

Reads pass events and opponent positions from tracking data (Metrica and
IDSSE), runs the line-breaking detection algorithm via ``applyInPandas``,
and writes results to the ``line_breaking_results`` bronze table.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.line_breaking_common import (
    _RESULT_COLUMNS,
    _TABLE_NAME,
    _parse_tracking_json,
)
from ingestion.utils import merge_delta_table

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.line_breaking import LineBreakingParams


# ---------------------------------------------------------------------------
# applyInPandas UDF closures
# ---------------------------------------------------------------------------


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

        from analytics.coordinates import metrica_to_statsbomb as _metrica
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
            opp_x, opp_y = _metrica(opp_positions["x"], opp_positions["y"])
            opp_positions["x"] = opp_x
            opp_positions["y"] = opp_y

            # Pass coordinates (also 0-1 -> 120x80 with y-flip)
            raw_sx = float(row.get("evt_start_x", 0) or 0)
            raw_sy = float(row.get("evt_start_y", 0) or 0)
            raw_ex = float(row.get("evt_end_x", 0) or 0)
            raw_ey = float(row.get("evt_end_y", 0) or 0)
            start_x, start_y = _metrica(raw_sx, raw_sy)
            end_x, end_y = _metrica(raw_ex, raw_ey)

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


def _make_idsse_udf() -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build the ``applyInPandas`` UDF closure for IDSSE tracking data.

    The UDF receives a pandas DataFrame containing one match's worth of
    joined event + tracking rows (one row per event-tracking pair from
    the temporal join).  It deduplicates events, filters to opponent
    players, converts coordinates, and runs detection.

    Two coordinate transforms:
      - Event positions (pitch-origin meters 0-105, 0-68) via ``pitch_m_to_statsbomb``
      - Tracking positions (center-origin meters) via ``center_m_to_statsbomb``

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        import pandas as _pd

        from analytics.coordinates import center_m_to_statsbomb as _center_m
        from analytics.coordinates import pitch_m_to_statsbomb as _pitch_m
        from analytics.line_breaking import LineBreakingParams as _LBParams
        from analytics.line_breaking import detect_line_breaking_batch as _detect_batch

        if pdf.empty:
            return _pd.DataFrame(columns=_pd.Index(_RESULT_COLUMNS))

        params = _LBParams()
        match_id = str(pdf["evt_match_id"].iloc[0])

        # Pre-build indexed lookup — avoids O(n*m) boolean mask filtering in loop
        event_groups = dict(iter(pdf.groupby("evt_event_id")))

        passes_list: list[dict[str, object]] = []
        opponents_by_event: dict[str, _pd.DataFrame] = {}

        for eid, event_rows in event_groups.items():
            first_row = event_rows.iloc[0]
            event_id = str(eid)

            # Determine opponent team from event's team field
            event_team = str(first_row.get("evt_team", ""))
            if not event_team or event_team == "unknown":
                continue

            # Filter tracking rows to opponent players only
            opp_rows = event_rows[event_rows["trk_team"] != event_team]
            if len(opp_rows) < params.min_opponents:
                continue

            # Convert opponent tracking positions (center-origin meters -> StatsBomb)
            opp_x, opp_y = _center_m(opp_rows["trk_x"].astype(float), opp_rows["trk_y"].astype(float))
            opp_positions = _pd.DataFrame({"x": opp_x, "y": opp_y})

            # Convert event positions (pitch-origin meters -> StatsBomb)
            raw_sx = float(first_row.get("evt_x", 0) or 0)
            raw_sy = float(first_row.get("evt_y", 0) or 0)
            start_x, start_y = _pitch_m(raw_sx, raw_sy)
            # IDSSE events have single location (no end location) — estimate end
            # from ball position in tracking data at the event frame
            raw_bx = first_row.get("trk_ball_x")
            raw_by = first_row.get("trk_ball_y")
            if raw_bx is not None and raw_by is not None and not (_pd.isna(raw_bx) or _pd.isna(raw_by)):
                # Ball position is in center-origin meters (same as tracking)
                end_x, end_y = _center_m(float(raw_bx), float(raw_by))
            else:
                # No ball data — cannot determine pass end, skip
                continue

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
        result_df["data_source"] = "idsse_tracking"

        return _pd.DataFrame(result_df[_RESULT_COLUMNS])

    return _udf


# ---------------------------------------------------------------------------
# Path B — Metrica tracking + events
# ---------------------------------------------------------------------------


def _process_metrica_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
    *,
    new_ids: list[str],
) -> int:
    """Detect line-breaking passes using Metrica tracking + event data.

    Joins pass events with tracking frames in Spark on match_id and frame,
    then uses ``groupBy("evt_match_id").applyInPandas`` to distribute
    detection across executors.

    Parameters
    ----------
    new_ids : list[str]
        Pre-computed list of new match IDs from the guard.

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.metrica_events"
    tracking_table = f"{catalog}.{schema}.metrica_tracking"

    logger.info("Path B: %d matches from guard metadata", len(new_ids))

    if not new_ids:
        return 0

    # --- pyspark imports deferred past early-exit guards (not installed in test env) ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

    # Build Spark DFs for passes and tracking, filtered to new matches
    new_ids_str = [str(mid) for mid in new_ids]

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
# Path C — IDSSE tracking + events
# ---------------------------------------------------------------------------

# Temporal join tolerance: 1.5 frames at 25fps = 0.06 seconds
_IDSSE_TEMPORAL_TOLERANCE = 0.06


def _process_idsse_tracking(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: LineBreakingParams,
    *,
    new_ids: list[str],
) -> int:
    """Detect line-breaking passes using IDSSE tracking + event data.

    Joins DFL pass events (``event_type = 'Play'``) with narrow-format
    tracking data via temporal proximity (closest frame within 0.06s),
    then uses ``groupBy("evt_match_id").applyInPandas`` to distribute
    detection across executors.

    Coordinate systems:
      - Events: pitch-origin meters (0-105, 0-68)
      - Tracking: center-origin meters (-52.5 to 52.5, -34 to 34)
      Both are converted to StatsBomb 120x80 inside the UDF.

    Parameters
    ----------
    new_ids : list[str]
        Pre-computed list of new match IDs from the guard.

    Returns number of rows written.
    """
    events_table = f"{catalog}.{schema}.idsse_events"
    tracking_table = f"{catalog}.{schema}.idsse_tracking"

    logger.info("Path C: %d matches from guard metadata", len(new_ids))

    if not new_ids:
        return 0

    # --- pyspark imports deferred past early-exit guards (not installed in test env) ---
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

    # Build Spark DFs for events and tracking, filtered to new matches
    new_ids_str = [str(mid) for mid in new_ids]

    events_df = (
        spark.table(events_table)
        .filter(F.col("event_type") == "Play")
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("event_id").alias("evt_event_id"),
            F.col("match_id").alias("evt_match_id"),
            F.col("team").alias("evt_team"),
            F.col("timestamp_seconds").alias("evt_timestamp"),
            F.col("x").alias("evt_x"),
            F.col("y").alias("evt_y"),
        )
        .filter(F.col("evt_x").isNotNull())
        .filter(F.col("evt_y").isNotNull())
    )

    tracking_df = (
        spark.table(tracking_table)
        .filter(F.col("match_id").isin(new_ids_str))
        .select(
            F.col("match_id").alias("trk_match_id"),
            F.col("timestamp").alias("trk_timestamp"),
            F.col("player_id").alias("trk_player_id"),
            F.col("team").alias("trk_team"),
            F.col("x").alias("trk_x"),
            F.col("y").alias("trk_y"),
            F.col("ball_x").alias("trk_ball_x"),
            F.col("ball_y").alias("trk_ball_y"),
        )
    )

    # Temporal join: match on match_id and closest frame within tolerance
    joined = events_df.join(
        tracking_df,
        (events_df["evt_match_id"] == tracking_df["trk_match_id"])
        & (F.abs(events_df["evt_timestamp"] - tracking_df["trk_timestamp"]) <= _IDSSE_TEMPORAL_TOLERANCE),
        "inner",
    ).drop("trk_match_id")

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

    udf_fn = _make_idsse_udf()

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

    logger.info("Path C complete: %d rows written", written)
    return written
