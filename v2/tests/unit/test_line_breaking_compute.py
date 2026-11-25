"""line_breaking compute — the pure actions x frames core (spec D9; plan Task 5).

Builds a canonical LTR frame the real way and a small SPADL actions frame, then scores with `line_breaking_values`,
asserting the per-action bronze shape and that the canonical keys are carried. The match-iterating layer-2 ComputeStep
(reads fct_actions + writes bronze) is covered by the seeded e2e (Task 11).
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import warnings
from typing import Any

import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pytest
from silly_kicks.spadl import config as spadlconfig  # pyright: ignore[reportMissingTypeStubs]
from silly_kicks.tracking.gradientsports import convert_to_frames  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.compute.frames import resolve_roster_goalkeepers, shape_long_input
from lakehouse.compute.line_breaking import (
    LINE_BREAKING_SCHEMA,
    FrameKeyError,
    TimeBaseError,
    line_breaking_values,
)

HOME, AWAY = 100, 200
HOME_GK, AWAY_GK = 1, 11
_PLAYERS = [
    (HOME_GK, HOME, -50_000, 0),
    (2, HOME, -20_000, 12_000),
    (3, HOME, -20_000, -12_000),
    (4, HOME, 0, 0),
    (AWAY_GK, AWAY, 50_000, 0),
    (12, AWAY, 20_000, 12_000),
    (13, AWAY, 20_000, -12_000),
    (14, AWAY, 5_000, 0),
]


def _frame() -> pd.DataFrame:
    frame_rows: list[dict[str, object]] = []
    ball_rows: list[dict[str, object]] = []
    for frame_id in range(6):
        for pid, team, x_mm, y_mm in _PLAYERS:
            frame_rows.append(
                {
                    "canonical_match_key": 5,
                    "canonical_player_key": pid,
                    "team_key": team,
                    "canonical_team_key": team,
                    "provider": "gradientsports",
                    "period": 1,
                    "frame": frame_id,
                    "x_mm": x_mm + frame_id * 200,
                    "y_mm": y_mm,
                    "is_visible": True,
                    "_visibility": "visible",
                }
            )
        ball_rows.append(
            {
                "canonical_match_key": 5,
                "period": 1,
                "frame": frame_id,
                "ball_x_mm": frame_id * 200,
                "ball_y_mm": 0,
                "ball_detected": True,
                "possession_team_key": None,
                "time_seconds": frame_id / 25.0,
            }
        )
    frames = pd.DataFrame(frame_rows).astype({"canonical_player_key": "Int64", "canonical_team_key": "Int64"})
    raw = shape_long_input(frames, pd.DataFrame(ball_rows), frame_rate=25.0)
    raw = resolve_roster_goalkeepers(raw, {(HOME, HOME_GK), (AWAY, AWAY_GK)})
    frame, _ = convert_to_frames(raw, home_team_id=HOME, home_team_start_left=True, output_convention="ltr")
    return frame


def _actions() -> pd.DataFrame:
    pass_id = spadlconfig.actiontype_id["pass"]
    rows = [
        {
            "game_id": 5,
            "team_id": HOME,
            "player_id": 4,
            "period_id": 1,
            "time_seconds": 0.08,
            "action_id": 0,
            "start_x": 40.0,
            "start_y": 34.0,
            "end_x": 75.0,
            "end_y": 34.0,
            "type_id": pass_id,
            "result_id": 1,
            "bodypart_id": 0,
        },
        {
            "game_id": 5,
            "team_id": HOME,
            "player_id": 2,
            "period_id": 1,
            "time_seconds": 0.16,
            "action_id": 1,
            "start_x": 30.0,
            "start_y": 46.0,
            "end_x": 55.0,
            "end_y": 40.0,
            "type_id": pass_id,
            "result_id": 1,
            "bodypart_id": 0,
        },
    ]
    return pd.DataFrame(rows).astype({"player_id": "Int64", "team_id": "Int64"})


def test_line_breaking_values_shape() -> None:
    table = line_breaking_values(_actions(), _frame())
    assert table.schema.equals(LINE_BREAKING_SCHEMA)
    assert table.num_rows == 2  # one row per action
    df = table.to_pandas()
    assert (df["canonical_match_key"] == 5).all()
    assert set(df["action_id"]) == {0, 1}


def test_overlapping_actions_and_frames_link_cleanly() -> None:
    """Guard the D9 time-base anti-goal (review B-CALC-IMPL-08). When the action clock and the frame clock share a time
    range — the real, correct case — the scorer links actions to frames with a HEALTHY link_rate and does NOT warn
    about a near-disjoint time base. The actions here (0.08 s, 0.16 s) fall inside the frames' span (0.0-0.20 s). A
    regression that made the two clocks disjoint (the e2e's short-tracking-slice artifact: link_rate 0.24, 16/21
    unlinked) would drop the link_rate below silly-kicks floor and trip this test, which the e2e's tolerant
    `line_breaking_results may be 0` assertion cannot."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        table = line_breaking_values(_actions(), _frame())
    assert table.num_rows == 2  # both actions produce a result row
    disjoint = [w for w in caught if "link_rate" in str(w.message) and "below" in str(w.message)]
    assert not disjoint, (
        f"actions did not link to frames — near-disjoint time base: {[str(w.message) for w in disjoint]}"
    )


# --- the two-sided time-base guard and the coverage-aware link rate (v1 ADR-040; owner 2026-11-07) ---------------


def test_an_absolute_action_clock_fails() -> None:
    # The 2026-11-06 Mac lake: GS period-2 actions sat on the match clock (2700 s+) against period-relative frames.
    actions = _actions()
    actions["time_seconds"] = actions["time_seconds"] + 2700.0
    with pytest.raises(TimeBaseError, match=r"period 1 actions start at 2700\.08 s"):
        line_breaking_values(actions, _frame())


def test_an_absolute_frame_clock_fails() -> None:
    frame = _frame()
    frame["time_seconds"] = frame["time_seconds"] + 2700.0
    with pytest.raises(TimeBaseError, match=r"period 1 frames start at 2700\.00 s"):
        line_breaking_values(_actions(), frame)


def test_covered_actions_that_do_not_link_fail() -> None:
    # Frames every 0.8 s (continuous coverage: no gap over 1 s): actions at 0.4 s and 1.2 s sit 0.4 s from any frame,
    # beyond the 0.2 s link tolerance, so the frames cover them but do not link them.
    frame = _frame()
    frame["time_seconds"] = frame["frame_id"] * 0.8
    actions = _actions()
    actions["time_seconds"] = [0.4, 1.2]
    with pytest.raises(TimeBaseError, match=r"period 1 link rate 0\.00 \(0 of 2 covered actions\)"):
        line_breaking_values(actions, frame)


def test_actions_outside_a_short_tracking_slice_do_not_fail() -> None:
    # A fixture slice covers seconds of a half; the actions after it are not covered, so they do not count.
    actions = _actions()
    actions.loc[1, "time_seconds"] = 1000.0
    table = line_breaking_values(actions, _frame())
    assert table.num_rows == 2


def _gap_clock(frame_id: Any) -> float:
    return 0.0 if frame_id == 0 else 10.0 + float(frame_id)


def test_an_action_in_a_tracking_gap_does_not_count() -> None:
    # A sparse slice (the idsse fixture): frames at 0 s and 11-15 s. The 5 s and 6 s actions sit in a gap with no
    # tracking, so they are not covered; the action at 11.1 s is covered and links, so the match passes.
    frame = _frame()
    frame["time_seconds"] = frame["frame_id"].map(_gap_clock)
    actions = pd.concat([_actions(), _actions().iloc[[1]]], ignore_index=True)
    actions["action_id"] = [0, 1, 2]
    actions["time_seconds"] = [5.0, 6.0, 11.1]
    assert line_breaking_values(actions, frame).num_rows == 3


# --- the action keys must match the frame keys (owner 2026-11-08: fail loud) ---------------------------------------


def test_action_team_keys_that_match_no_frame_row_fail_the_match() -> None:
    # The 2026-11-08 Mac lake: rounded GS team keys linked by time, but no action (period, team) matched a frame row,
    # so silly-kicks resolved no direction and wrote every result null while the run reported success.
    rounded = _actions().assign(team_id=HOME + 1)
    expected = r"period 1: no linked action's team key \(2 actions\) matches a frame team"
    with pytest.raises(FrameKeyError, match=expected):
        line_breaking_values(rounded, _frame())


def test_a_null_actor_alone_does_not_fail_the_match() -> None:
    # A GS null-actor action has no team key: it cannot match, and it is not evidence of a key mismatch.
    actions = _actions()
    actions.loc[0, "team_id"] = pd.NA
    table = line_breaking_values(actions, _frame())
    assert table.num_rows == 2
