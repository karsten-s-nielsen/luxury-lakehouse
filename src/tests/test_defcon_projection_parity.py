"""Input-projection parity: column constants must match StructType field names.

OPT-3 sub-item (a). Each module-level ``_*_INPUT_COLS`` constant declares which
columns the UDF closure actually reads from the joined DataFrame. The test
asserts parity between the constant and the corresponding ``StructType`` so
that column-list drift between the join output and what the UDF reads is
caught at test time, not at Spark execution time.

Same defensive pattern as ``test_defcon_schema_parity.py`` and
``test_spadl_vaep_writer_parity.py``.
"""

from __future__ import annotations

import pytest


def _replay_credits_schema_360():
    """Replay the credits_schema StructType from defcon_lite_360."""
    t = pytest.importorskip("pyspark.sql.types")

    return t.StructType(
        [
            t.StructField("event_id", t.StringType(), nullable=True),
            t.StructField("match_id", t.StringType(), nullable=True),
            t.StructField("competition_id", t.LongType(), nullable=True),
            t.StructField("season_id", t.LongType(), nullable=True),
            t.StructField("defender_player_id", t.LongType(), nullable=True),
            t.StructField("defender_team_id", t.LongType(), nullable=True),
            t.StructField("defender_x", t.DoubleType(), nullable=True),
            t.StructField("defender_y", t.DoubleType(), nullable=True),
            t.StructField("action_player_id", t.LongType(), nullable=True),
            t.StructField("action_type", t.StringType(), nullable=True),
            t.StructField("action_x", t.DoubleType(), nullable=True),
            t.StructField("action_y", t.DoubleType(), nullable=True),
            t.StructField("credit_type", t.StringType(), nullable=True),
            t.StructField("confidence", t.StringType(), nullable=True),
            t.StructField("dist_to_ball", t.DoubleType(), nullable=True),
            t.StructField("pitch_control_at_action", t.DoubleType(), nullable=True),
            t.StructField("offensive_value", t.DoubleType(), nullable=True),
            t.StructField("vaep_target", t.DoubleType(), nullable=True),
        ]
    )


def _replay_credits_schema_tracking():
    """Replay the credits_schema StructType from defcon_lite_tracking.

    Identical to 360 credits_schema -- both Pass 1 outputs share the same shape.
    """
    return _replay_credits_schema_360()


def _replay_valued_schema():
    """Replay the valued_schema StructType (shared by 360 + tracking Pass 2)."""
    t = pytest.importorskip("pyspark.sql.types")

    return t.StructType(
        [
            t.StructField("event_id", t.StringType(), nullable=True),
            t.StructField("match_id", t.StringType(), nullable=True),
            t.StructField("competition_id", t.LongType(), nullable=True),
            t.StructField("season_id", t.LongType(), nullable=True),
            t.StructField("defender_player_id", t.LongType(), nullable=True),
            t.StructField("defender_team_id", t.LongType(), nullable=True),
            t.StructField("defender_x", t.DoubleType(), nullable=True),
            t.StructField("defender_y", t.DoubleType(), nullable=True),
            t.StructField("action_player_id", t.LongType(), nullable=True),
            t.StructField("action_type", t.StringType(), nullable=True),
            t.StructField("action_x", t.DoubleType(), nullable=True),
            t.StructField("action_y", t.DoubleType(), nullable=True),
            t.StructField("credit_type", t.StringType(), nullable=True),
            t.StructField("confidence", t.StringType(), nullable=True),
            t.StructField("defcon_value", t.FloatType(), nullable=True),
            t.StructField("dist_to_ball", t.DoubleType(), nullable=True),
            t.StructField("pitch_control_at_action", t.DoubleType(), nullable=True),
            t.StructField("data_source", t.StringType(), nullable=True),
        ]
    )


class TestCreditsInputCols360:
    """_CREDITS_UDF_INPUT_COLS_360 must match the joined DF column names."""

    def test_constant_exists(self) -> None:
        from ingestion.defcon_lite_360 import _CREDITS_UDF_INPUT_COLS_360

        assert isinstance(_CREDITS_UDF_INPUT_COLS_360, tuple)
        assert len(_CREDITS_UDF_INPUT_COLS_360) > 0

    def test_constant_covers_pre_join_columns(self) -> None:
        """Every column in the constant must be one of the pre-join aliases."""
        from ingestion.defcon_lite_360 import _CREDITS_UDF_INPUT_COLS_360

        expected_act = {
            "act_event_id",
            "act_match_id",
            "act_competition_id",
            "act_season_id",
            "act_player_id",
            "act_team_id",
            "act_action_type",
            "act_start_x",
            "act_start_y",
            "act_offensive_value",
        }
        expected_ff = {
            "ff_teammate",
            "ff_x",
            "ff_y",
            "ff_velocity_x",
            "ff_velocity_y",
            "ff_player_id",
            "ff_team_id",
        }
        expected_all = expected_act | expected_ff
        actual = set(_CREDITS_UDF_INPUT_COLS_360)
        assert actual == expected_all, (
            f"Column mismatch.\n"
            f"  Missing from constant: {expected_all - actual}\n"
            f"  Extra in constant: {actual - expected_all}"
        )


class TestCreditsInputColsTracking:
    """_CREDITS_UDF_INPUT_COLS_TRACKING must match the joined DF column names."""

    def test_constant_exists(self) -> None:
        from ingestion.defcon_lite_tracking import _CREDITS_UDF_INPUT_COLS_TRACKING

        assert isinstance(_CREDITS_UDF_INPUT_COLS_TRACKING, tuple)
        assert len(_CREDITS_UDF_INPUT_COLS_TRACKING) > 0

    def test_constant_covers_pre_join_columns(self) -> None:
        """Every column in the constant must be one of the pre-join aliases."""
        from ingestion.defcon_lite_tracking import _CREDITS_UDF_INPUT_COLS_TRACKING

        expected_act = {
            "act_event_id",
            "act_match_id",
            "act_competition_id",
            "act_season_id",
            "act_player_id",
            "act_team_id",
            "act_action_type",
            "act_start_x",
            "act_start_y",
            "act_offensive_value",
        }
        expected_trk = {
            "trk_player_id",
            "trk_team",
            "trk_x",
            "trk_y",
            "trk_velocity_x",
            "trk_velocity_y",
            "trk_frame",
            "trk_period",
        }
        expected_all = expected_act | expected_trk
        actual = set(_CREDITS_UDF_INPUT_COLS_TRACKING)
        assert actual == expected_all, (
            f"Column mismatch.\n"
            f"  Missing from constant: {expected_all - actual}\n"
            f"  Extra in constant: {actual - expected_all}"
        )


class TestValueUdfInputCols:
    """_VALUE_UDF_INPUT_COLS must match the credits_schema field names."""

    def test_constant_exists(self) -> None:
        from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS

        assert isinstance(_VALUE_UDF_INPUT_COLS, tuple)
        assert len(_VALUE_UDF_INPUT_COLS) > 0

    def test_constant_matches_credits_schema_fields(self) -> None:
        """Pass 2 input is Pass 1 output (credits_schema). Column constant must match."""
        from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS

        credits_fields = {f.name for f in _replay_credits_schema_360().fields}
        actual = set(_VALUE_UDF_INPUT_COLS)
        assert actual == credits_fields, (
            f"Column mismatch with credits_schema.\n"
            f"  Missing from constant: {credits_fields - actual}\n"
            f"  Extra in constant: {actual - credits_fields}"
        )

    def test_constant_matches_udf_empty_cols(self) -> None:
        """_VALUE_UDF_INPUT_COLS must match the credits_schema, not the output columns."""
        from ingestion.defcon_lite_common import _VALUE_UDF_INPUT_COLS

        credits_fields = {f.name for f in _replay_credits_schema_360().fields}
        assert set(_VALUE_UDF_INPUT_COLS) == credits_fields
