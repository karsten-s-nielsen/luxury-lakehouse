"""DEFCON-lite 360 freeze-frame processing path.

Reads action values from ``fct_action_values`` and freeze frame data from
``statsbomb_360`` in the bronze layer, computes per-defender defensive credits
using the DEFCON-lite analytics module via two-pass ``applyInPandas``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.guards import FilterResult
from ingestion.utils import write_delta_table
from shared.constants import DEFAULT_GOLD_SCHEMA

_TABLE_NAME = "defcon_results"
_RESULTS_SCHEMA = (
    "event_id STRING, match_id STRING, competition_id BIGINT, season_id BIGINT, "
    "defender_player_id BIGINT, defender_team_id BIGINT, defender_x DOUBLE, defender_y DOUBLE, "
    "action_player_id BIGINT, action_type STRING, action_x DOUBLE, action_y DOUBLE, "
    "credit_type STRING, confidence STRING, dist_to_ball DOUBLE, pitch_control_at_action DOUBLE, "
    "defcon_value FLOAT, data_source STRING, _ingested_at TIMESTAMP"
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from analytics.defcon_lite import DefconLiteParams

_guard_logger = logging.getLogger(f"{__name__}.guard")


class _Defcon360Guard:
    """SkipGuard adapter for DEFCON-lite 360 freeze-frame path."""

    workflow_id = "wf-defcon"

    def check(self, spark: SparkSession, catalog: str, schema: str) -> FilterResult:
        """Check which 360 matches need DEFCON computation."""
        from ingestion.guards import ensure_table, find_new_ids

        results_table = f"{catalog}.{schema}.{_TABLE_NAME}"
        ensure_table(spark, results_table, _RESULTS_SCHEMA)

        new_match_ids = find_new_ids(
            spark,
            source_table=f"{catalog}.bronze.statsbomb_360",
            results_table=f"{catalog}.{schema}.{_TABLE_NAME}",
            results_filter="data_source = 'statsbomb_360'",
        )

        if not new_match_ids:
            return FilterResult(workflow_id=self.workflow_id, count=0)

        return FilterResult(
            workflow_id=self.workflow_id,
            count=len(new_match_ids),
            metadata={"new_match_ids": new_match_ids},
        )


skip_guard = _Defcon360Guard()


def _make_credits_udf_360(
    disturb_radius_m: float,
    deter_cone_angle_deg: float,
    pitch_length: float,
    pitch_width: float,
) -> object:
    """Build the Pass 1 ``applyInPandas`` UDF closure for 360 data.

    Scalar params are captured by the closure so they are serialised with
    the UDF and available on executors without network access.

    Returns:
        A callable ``(pd.DataFrame) -> pd.DataFrame`` suitable for
        ``applyInPandas``.
    """
    # Capture serialisable scalars (no dataclass — pickle compatibility)
    _disturb_r = disturb_radius_m
    _deter_angle = deter_cone_angle_deg
    _pitch_l = pitch_length
    _pitch_w = pitch_width

    def _udf(pdf: pd.DataFrame) -> pd.DataFrame:
        """Assign defensive credits for one match group (360 data)."""
        import pandas as _pd

        from analytics.defcon_lite import DefconLiteParams as _Params
        from analytics.defcon_lite import assign_credits_for_period as _assign

        _empty_cols = _pd.Index(
            [
                "event_id",
                "match_id",
                "competition_id",
                "season_id",
                "defender_player_id",
                "defender_team_id",
                "defender_x",
                "defender_y",
                "action_player_id",
                "action_type",
                "action_x",
                "action_y",
                "credit_type",
                "confidence",
                "dist_to_ball",
                "pitch_control_at_action",
                "offensive_value",
                "vaep_target",
            ]
        )

        if pdf.empty:
            return _pd.DataFrame(columns=_empty_cols)

        params = _Params(
            disturb_radius_m=_disturb_r,
            deter_cone_angle_deg=_deter_angle,
            pitch_length=_pitch_l,
            pitch_width=_pitch_w,
        )

        # Split joined columns back into actions and freeze frames
        actions = _pd.DataFrame(
            {
                "event_id": pdf["act_event_id"],
                "match_id": pdf["act_match_id"],
                "competition_id": pdf["act_competition_id"],
                "season_id": pdf["act_season_id"],
                "player_id": pdf["act_player_id"],
                "team_id": pdf["act_team_id"],
                "action_type": pdf["act_action_type"],
                "start_x": pdf["act_start_x"],
                "start_y": pdf["act_start_y"],
                "offensive_value": pdf["act_offensive_value"],
            }
        ).drop_duplicates(subset=["event_id"])

        ff = _pd.DataFrame(
            {
                "event_id": pdf["act_event_id"],
                "player_id": pdf["ff_player_id"],
                "team_id": pdf["ff_team_id"],
                "teammate": pdf["ff_teammate"],
                "x": pdf["ff_x"],
                "y": pdf["ff_y"],
                "velocity_x": pdf["ff_velocity_x"],
                "velocity_y": pdf["ff_velocity_y"],
            }
        )

        result = _assign(actions, ff, params)
        if result.empty:
            return _pd.DataFrame(columns=_empty_cols)

        return _pd.DataFrame(result)

    return _udf


def process_360_matches(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
    params: DefconLiteParams,
    champion_model_bytes: bytes | None = None,
    *,
    filter_result: FilterResult | None = None,
) -> int:
    """Process StatsBomb 360 matches via two-pass applyInPandas.

    Pass 1: ``groupBy("match_id").applyInPandas`` assigns defensive credits
    on executors.
    Pass 2: ``groupBy("match_id").applyInPandas`` runs XGBoost value
    estimation on executors (using @Champion model if available).

    Returns number of rows written.
    """
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    action_table = f"{catalog}.{DEFAULT_GOLD_SCHEMA}.fct_action_values"
    ff_table = f"{catalog}.bronze.statsbomb_360"
    results_table = f"{catalog}.{schema}.{_TABLE_NAME}"

    # Use guard metadata if available, otherwise fall back to inline guard
    if filter_result and filter_result.metadata.get("new_match_ids"):
        new_ids_str = filter_result.metadata["new_match_ids"]
    else:
        from ingestion.guards import find_new_ids

        new_ids_str = find_new_ids(
            spark,
            source_table=ff_table,
            results_table=results_table,
            results_filter="data_source = 'statsbomb_360'",
        )
    logger.info("%d 360 matches to process", len(new_ids_str))

    if not new_ids_str:
        return 0

    actions_df = (
        spark.table(action_table)
        .filter(F.col("match_id").cast("string").isin(new_ids_str))
        .filter("original_event_id IS NOT NULL AND original_event_id != 'None'")
        .select(
            F.col("original_event_id").alias("act_event_id"),
            F.col("match_id").cast("string").alias("act_match_id"),
            F.col("competition_id").alias("act_competition_id"),
            F.col("season_id").alias("act_season_id"),
            F.col("player_id").alias("act_player_id"),
            F.col("team_id").alias("act_team_id"),
            F.col("action_type").alias("act_action_type"),
            F.col("start_x").alias("act_start_x"),
            F.col("start_y").alias("act_start_y"),
            F.col("offensive_value").alias("act_offensive_value"),
        )
    )

    ff_df = (
        spark.table(ff_table)
        .filter(F.col("match_id").cast("string").isin(new_ids_str))
        .selectExpr(
            "id as ff_event_id",
            "teammate as ff_teammate",
            "from_json(location, 'ARRAY<DOUBLE>') as loc",
        )
        .selectExpr(
            "ff_event_id",
            "ff_teammate",
            "loc[0] * 105.0 / 120.0 as ff_x",
            "loc[1] * 68.0 / 80.0 as ff_y",
        )
    )

    # 360 freeze frames are anonymous — add synthetic IDs
    ff_df = (
        ff_df.withColumn("ff_player_id", F.monotonically_increasing_id().cast("int"))
        .withColumn("ff_team_id", F.lit(0).cast("int"))
        .withColumn("ff_velocity_x", F.lit(0.0))
        .withColumn("ff_velocity_y", F.lit(0.0))
    )

    # Join actions x freeze frames on event_id
    joined = actions_df.join(ff_df, actions_df["act_event_id"] == ff_df["ff_event_id"], "inner").drop("ff_event_id")

    # Pass 1: assign credits per match on executors
    credits_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("competition_id", LongType(), nullable=True),
            StructField("season_id", LongType(), nullable=True),
            StructField("defender_player_id", LongType(), nullable=True),
            StructField("defender_team_id", LongType(), nullable=True),
            StructField("defender_x", DoubleType(), nullable=True),
            StructField("defender_y", DoubleType(), nullable=True),
            StructField("action_player_id", StringType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("action_x", DoubleType(), nullable=True),
            StructField("action_y", DoubleType(), nullable=True),
            StructField("credit_type", StringType(), nullable=True),
            StructField("confidence", StringType(), nullable=True),
            StructField("dist_to_ball", DoubleType(), nullable=True),
            StructField("pitch_control_at_action", DoubleType(), nullable=True),
            StructField("offensive_value", DoubleType(), nullable=True),
            StructField("vaep_target", DoubleType(), nullable=True),
        ]
    )

    credits_udf = _make_credits_udf_360(
        disturb_radius_m=params.disturb_radius_m,
        deter_cone_angle_deg=params.deter_cone_angle_deg,
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
    )

    credits_sdf = joined.groupBy("act_match_id").applyInPandas(
        credits_udf,  # type: ignore[arg-type]
        schema=credits_schema,
    )

    # Pass 2: estimate DEFCON values per match on executors
    valued_schema = StructType(
        [
            StructField("event_id", StringType(), nullable=True),
            StructField("match_id", StringType(), nullable=True),
            StructField("competition_id", LongType(), nullable=True),
            StructField("season_id", LongType(), nullable=True),
            StructField("defender_player_id", LongType(), nullable=True),
            StructField("defender_team_id", LongType(), nullable=True),
            StructField("defender_x", DoubleType(), nullable=True),
            StructField("defender_y", DoubleType(), nullable=True),
            StructField("action_player_id", StringType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("action_x", DoubleType(), nullable=True),
            StructField("action_y", DoubleType(), nullable=True),
            StructField("credit_type", StringType(), nullable=True),
            StructField("confidence", StringType(), nullable=True),
            StructField("defcon_value", DoubleType(), nullable=True),
            StructField("dist_to_ball", DoubleType(), nullable=True),
            StructField("pitch_control_at_action", DoubleType(), nullable=True),
            StructField("data_source", StringType(), nullable=True),
        ]
    )

    from ingestion.defcon_lite_common import _make_values_udf

    values_udf = _make_values_udf(
        disturb_radius_m=params.disturb_radius_m,
        deter_cone_angle_deg=params.deter_cone_angle_deg,
        pitch_length=params.pitch_length,
        pitch_width=params.pitch_width,
        data_source="statsbomb_360",
        champion_model_bytes=champion_model_bytes,
    )

    valued_sdf = credits_sdf.groupBy("match_id").applyInPandas(
        values_udf,  # type: ignore[arg-type]
        schema=valued_schema,
    )

    # Build replaceWhere predicate for all new matches
    escaped_ids = ", ".join(f"'{mid}'" for mid in new_ids_str)
    replace_predicate = f"data_source = 'statsbomb_360' AND match_id IN ({escaped_ids})"

    written = write_delta_table(
        valued_sdf,
        catalog,
        schema,
        _TABLE_NAME,
        replace_where=replace_predicate,
        logger=logger,
    )

    logger.info("360 matches: %d DEFCON-lite rows written across %d matches", written, len(new_ids_str))
    return written
