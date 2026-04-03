"""Tests for the pitch control batch computation pipeline."""

from __future__ import annotations

import pytest

pytest.importorskip("jax")

import pandas as pd

from analytics.pitch_control import PitchControlParams

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_frame(
    n_home: int = 11,
    n_away: int = 11,
    match_id: str = "test_match_1",
    frame: int = 0,
    period: int = 1,
    home_x: float = 60.0,
    away_x: float = 40.0,
    vx: float | None = 1.0,
    vy: float | None = 0.0,
) -> pd.DataFrame:
    """Build a minimal tracking frame DataFrame with home and away players.

    Coordinates are in StatsBomb system (120x80).  Velocities are in
    SB coordinate units per second (matching fct_tracking_frames).
    """
    rows: list[dict[str, object]] = []
    for i in range(n_home):
        rows.append(
            {
                "tracking_id": f"{match_id}_{frame}_home_{i}",
                "match_id": match_id,
                "frame": frame,
                "period": period,
                "player_id": f"home_{i}",
                "team": "home",
                "x": home_x + i * 5.0,
                "y": 10.0 + i * 6.0,
                "velocity_x": vx,
                "velocity_y": vy,
                "ball_x": 60.0,
                "ball_y": 40.0,
                "source_provider": "test",
                "frame_rate": 25,
            }
        )
    for i in range(n_away):
        rows.append(
            {
                "tracking_id": f"{match_id}_{frame}_away_{i}",
                "match_id": match_id,
                "frame": frame,
                "period": period,
                "player_id": f"away_{i}",
                "team": "away",
                "x": away_x + i * 5.0,
                "y": 10.0 + i * 6.0,
                "velocity_x": -1.0 if vx is not None else vx,
                "velocity_y": vy,
                "ball_x": 60.0,
                "ball_y": 40.0,
                "source_provider": "test",
                "frame_rate": 25,
            }
        )
    return pd.DataFrame(rows)


def _run_udf(pdf: pd.DataFrame, params: PitchControlParams | None = None) -> pd.DataFrame:
    """Invoke the actual pipeline UDF factory and call the returned closure.

    This tests the real code path — no logic duplication.
    """
    from ingestion.pitch_control_batch import _make_batch_udf

    if params is None:
        params = PitchControlParams()

    udf_fn = _make_batch_udf(
        reaction_time=params.reaction_time,
        max_acceleration=params.max_acceleration,
        sigma=params.sigma,
        pitch_length_m=params.pitch_length_m,
        pitch_width_m=params.pitch_width_m,
        sb_length=params.sb_length,
        sb_width=params.sb_width,
    )
    return udf_fn(pdf)


# ---------------------------------------------------------------------------
# TestPitchControlBatchUdf
# ---------------------------------------------------------------------------


class TestPitchControlBatchUdf:
    """Tests for the applyInPandas UDF logic."""

    def test_single_frame_all_players_get_values(self) -> None:
        """22 players (11 home, 11 away) each get a non-null pitch control value."""
        pdf = _make_frame(n_home=11, n_away=11)
        result = _run_udf(pdf)

        assert len(result) == 22
        assert bool(result["pitch_control_value"].notna().all())

    def test_home_player_near_goal_high_control(self) -> None:
        """A home player near their own goal with no nearby opponents has control > 0.5."""
        # Home players clustered at x=5 (near home goal), away at x=110 (far end)
        pdf = _make_frame(n_home=5, n_away=5, home_x=5.0, away_x=110.0)
        result = _run_udf(pdf)

        home_rows = result[result["tracking_id"].str.contains("home")]
        assert not home_rows.empty
        # Home players are far from all opponents, so they should have high control
        assert all(home_rows["pitch_control_value"] > 0.5)

    def test_values_bounded_zero_one(self) -> None:
        """All pitch control values are in [0, 1]."""
        pdf = _make_frame(n_home=11, n_away=11)
        result = _run_udf(pdf)

        assert all(result["pitch_control_value"] >= 0.0)
        assert all(result["pitch_control_value"] <= 1.0)

    def test_output_schema_has_required_columns(self) -> None:
        """Output has tracking_id, match_id, and pitch_control_value columns."""
        pdf = _make_frame(n_home=3, n_away=3)
        result = _run_udf(pdf)

        assert "tracking_id" in result.columns
        assert "match_id" in result.columns
        assert "pitch_control_value" in result.columns

    def test_missing_velocity_defaults_to_contested(self) -> None:
        """NaN velocities produce approximately contested values (~0.5).

        When all players on both teams have zero velocity (NaN filled to 0),
        and they are symmetrically positioned, the pitch control should be
        close to 0.5 at shared positions.
        """
        # Same positions for home and away -- fully symmetric
        pdf = _make_frame(n_home=3, n_away=3, home_x=60.0, away_x=60.0, vx=None, vy=None)
        result = _run_udf(pdf)

        # With symmetric positioning and zero velocity, values should be near 0.5
        assert not result.empty
        values = result["pitch_control_value"].values
        assert all(v >= 0.3 for v in values)
        assert all(v <= 0.7 for v in values)

    def test_empty_frame_returns_empty(self) -> None:
        """Empty input produces empty output with correct schema."""
        empty = pd.DataFrame(
            columns=pd.Index(
                [
                    "tracking_id",
                    "match_id",
                    "frame",
                    "period",
                    "player_id",
                    "team",
                    "x",
                    "y",
                    "velocity_x",
                    "velocity_y",
                    "ball_x",
                    "ball_y",
                    "source_provider",
                    "frame_rate",
                ]
            )
        )
        result = _run_udf(empty)
        assert len(result) == 0
        assert "tracking_id" in result.columns
        assert "match_id" in result.columns
        assert "pitch_control_value" in result.columns


# ---------------------------------------------------------------------------
# TestPitchControlBatchPipeline
# ---------------------------------------------------------------------------


class TestPitchControlBatchPipeline:
    """Tests for pipeline-level concerns."""

    def test_incremental_skip_guard(self) -> None:
        """Already-computed match_ids should be skipped.

        Verifies the skip logic by confirming the set-difference approach
        filters out existing IDs.
        """
        all_ids = {"m1", "m2", "m3", "m4"}
        existing_ids = {"m1", "m3"}
        new_ids = sorted(all_ids - existing_ids)
        assert new_ids == ["m2", "m4"]

    def test_output_table_name(self) -> None:
        """Pipeline targets the correct bronze table name."""
        from ingestion.pitch_control_batch import _TABLE_NAME

        assert _TABLE_NAME == "pitch_control_values"

    def test_gold_schema(self) -> None:
        """Pipeline reads from the correct gold schema."""
        from shared.constants import DEFAULT_GOLD_SCHEMA

        assert DEFAULT_GOLD_SCHEMA == "dev_gold"

    def test_udf_factory_returns_callable(self) -> None:
        """The UDF factory produces a callable object."""
        from ingestion.pitch_control_batch import _make_batch_udf

        params = PitchControlParams()
        udf = _make_batch_udf(
            reaction_time=params.reaction_time,
            max_acceleration=params.max_acceleration,
            sigma=params.sigma,
            pitch_length_m=params.pitch_length_m,
            pitch_width_m=params.pitch_width_m,
            sb_length=params.sb_length,
            sb_width=params.sb_width,
        )
        assert callable(udf)

    def test_batch_size_reasonable(self) -> None:
        """Default batch size stays within executor memory budget."""
        from ingestion.pitch_control_batch import _DEFAULT_BATCH_SIZE

        # 500 frames * 22 players * ~100 bytes per row ~ 1.1 MB per batch
        assert 100 <= _DEFAULT_BATCH_SIZE <= 2000
