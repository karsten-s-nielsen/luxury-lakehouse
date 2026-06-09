"""RC1 (ADR-040): Metrica period-relative time-base re-base.

bronze.metrica_events.start_time_s/end_time_s and bronze.metrica_tracking.timestamp are on
the ABSOLUTE match clock (P2 ~2885s), so the AC-1 work-unit time-base guard aborts every
Metrica unit. The fix re-bases BOTH streams off the CONTINUOUS frame number keyed on each
period's FIRST tracking frame — identically in three drivers so actions and frames align:

  * SPADL actions:  ingestion.spadl_conversion._convert_metrica_from_bronze
  * Spark AC frames: ingestion.action_context._process_tracking_match
  * local AC frames: analytics.action_context.pipeline.run_work_unit

This module locks the invariant (period-relative + cross-stream alignment + Sample_Game_3
timestamp-reset immunity) and the cross-driver lockstep.
"""

from __future__ import annotations

import inspect

import pandas as pd

FPS = 25.0


def _rebase_by_frame(frame: pd.Series, period: pd.Series, frame_rate: pd.Series) -> pd.Series:
    """The shared re-base expression all three drivers apply (kept in sync by the sentinel below)."""
    period_min = frame.groupby(period).transform("min").astype("float64")
    return (frame.astype("float64") - period_min) / frame_rate.astype("float64").fillna(FPS)


def test_frame_rebase_is_period_relative() -> None:
    """Each period's earliest frame becomes t=0, so the ADR-040 work-unit guard (which keys on
    each period's EARLIEST time, not its duration) accepts the re-based stream."""
    from analytics.action_context.time_base_guard import assert_work_unit_time_base

    frames = pd.DataFrame(
        {
            "period": [1, 1, 2, 2],
            "frame": [1, 1000, 71269, 145006],  # P2 frames are high (continuous), absolute clock
            "frame_rate": [FPS] * 4,
        }
    )
    ts = _rebase_by_frame(frames["frame"], frames["period"], frames["frame_rate"])
    assert ts[frames["period"] == 1].min() == 0.0
    assert ts[frames["period"] == 2].min() == 0.0  # P2 no longer ~2850s (was the abort cause)
    # The guard keys on each period's earliest time; both are 0 < 1800 → passes (pre-fix P2 min ~2885).
    per_period_min = {int(p): float(ts[frames["period"] == p].min()) for p in frames["period"].unique()}
    assert_work_unit_time_base(per_period_min)  # must not raise


def test_sample_game_3_timestamp_reset_is_ignored() -> None:
    """Sample_Game_3's P2 bronze timestamp resets to 0 (hand-curated); frame NUMBERS stay
    continuous. Re-basing off the frame number (not the timestamp) yields correct values."""
    # Continuous frame numbers despite the (irrelevant) reset bronze timestamp.
    frames = pd.DataFrame({"period": [2, 2], "frame": [69662, 143761], "frame_rate": [FPS, FPS]})
    ts = _rebase_by_frame(frames["frame"], frames["period"], frames["frame_rate"])
    assert ts.iloc[0] == 0.0  # period's first frame
    assert ts.iloc[1] == (143761 - 69662) / FPS


def test_action_and_frame_share_the_same_base() -> None:
    """An action at frame F maps to the frame whose timestamp == the action time — i.e. both
    streams use the IDENTICAL period min(frame), so the action<->frame linker aligns exactly."""
    period_min_frame = 71269  # P2 kickoff frame (== min over bronze.metrica_tracking)
    action_frame = 72135  # first P2 action (34.6s after kickoff)
    # SPADL action re-base (start_frame based):
    action_time = (action_frame - period_min_frame) / FPS
    # AC frame re-base for the SAME frame:
    frames = pd.DataFrame({"period": [2, 2], "frame": [period_min_frame, action_frame], "frame_rate": [FPS, FPS]})
    frame_ts = _rebase_by_frame(frames["frame"], frames["period"], frames["frame_rate"])
    assert action_time == frame_ts.iloc[1]  # exact alignment, no constant offset
    assert action_time == (72135 - 71269) / FPS


def test_rebase_present_in_all_three_drivers() -> None:
    """Lockstep: every driver must apply the Metrica frame-number re-base, or actions/frames
    silently de-align (the GS period-2 class). Source-level guard against one driver drifting."""
    from analytics.action_context import pipeline
    from ingestion import action_context, spadl_conversion

    local_src = inspect.getsource(pipeline.run_work_unit)
    assert 'wu.provider == "metrica"' in local_src
    assert 'groupby("period")["frame"].transform("min")' in local_src

    spark_src = inspect.getsource(action_context._process_tracking_match)
    assert 'provider == "metrica"' in spark_src
    assert "_period_min_frame" in spark_src

    spadl_src = inspect.getsource(spadl_conversion._convert_metrica_from_bronze)
    assert "_period_start_frame" in spadl_src
    assert "start_time_s" in spadl_src
