"""Unit tests for the tracking pre-shot snapshot builder (Task 0.4, canonical-SPADL pre-shot xG).

``build_tracking_snapshots`` generalizes ``build_sb360_snapshots`` to the tracking providers
(gradientsports / skillcorner): for each shot action it emits per-player rows from the linked
frame, carrying the teammate/keeper flags, the set cardinality, and the shooter orientation.

The pandas core is pure (no pyspark) so the local hexagon and the Spark cogroup UDF share one impl.
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.sb360_snapshots import build_sb360_snapshots
from analytics.action_context.tracking_snapshots import build_tracking_snapshots


def _shot_row(team_id: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "action_id": 7,
                "match_key": 100,
                "team_id": team_id,
                "type_name": "shot",
                "period_id": 1,
                "data_source": "gradientsports",
                "team_attacking_direction": "ltr",
            }
        ]
    )


def _frames() -> pd.DataFrame:
    # one frame linked to action_id 7: shooter team 1 attacks high-x
    return pd.DataFrame(
        [
            {"action_id": 7, "player_id": "a", "team_id": 1, "is_goalkeeper": False, "x": 95.0, "y": 40.0},
            {"action_id": 7, "player_id": "b", "team_id": 2, "is_goalkeeper": True, "x": 105.0, "y": 34.0},
            {"action_id": 7, "player_id": "c", "team_id": 2, "is_goalkeeper": False, "x": 90.0, "y": 20.0},
        ]
    )


def test_builds_per_player_rows_with_flags_and_cardinality() -> None:
    snaps = build_tracking_snapshots(_shot_row(), _frames())
    row = snaps[snaps.action_id == 7]
    assert len(row) == 3
    kpr = row[row.player_id == "b"].iloc[0]
    assert kpr.is_keeper == 1 and kpr.is_teammate == 0  # opponent GK
    tm = row[row.player_id == "a"].iloc[0]
    assert tm.is_teammate == 1
    assert row.x.between(0, 105).all() and row.y.between(0, 68).all()


def test_cardinality_matches_player_count() -> None:
    snaps = build_tracking_snapshots(_shot_row(), _frames())
    assert snaps.set_cardinality.iloc[0] == 3


def test_output_carries_match_key_and_data_source() -> None:
    snaps = build_tracking_snapshots(_shot_row(), _frames())
    assert (snaps.match_key == 100).all()
    assert (snaps.data_source == "gradientsports").all()


def test_orientation_derived_from_attacking_direction() -> None:
    snaps = build_tracking_snapshots(_shot_row(), _frames())
    # team_attacking_direction "ltr" => shooter attacks high-x
    assert bool(snaps.shooter_attacks_high_x.iloc[0]) is True
    snaps_rtl = build_tracking_snapshots(_shot_row().assign(team_attacking_direction="rtl"), _frames())
    assert bool(snaps_rtl.shooter_attacks_high_x.iloc[0]) is False


def test_empty_inputs_return_schema() -> None:
    out = build_tracking_snapshots(pd.DataFrame(), pd.DataFrame())
    assert len(out) == 0
    for col in ("action_id", "match_key", "data_source", "player_id", "x", "y", "is_keeper", "is_teammate"):
        assert col in out.columns


def test_ball_row_is_excluded() -> None:
    players = _frames().assign(is_ball=False)
    ball = pd.DataFrame(
        [
            {
                "action_id": 7,
                "player_id": None,
                "team_id": None,
                "is_goalkeeper": False,
                "x": 52.5,
                "y": 34.0,
                "is_ball": True,
            }
        ]
    )
    frames = pd.concat([players, ball], ignore_index=True)
    snaps = build_tracking_snapshots(_shot_row(), frames)
    assert len(snaps) == 3  # ball dropped, 3 players kept
    assert snaps.set_cardinality.iloc[0] == 3


def test_non_shot_actions_are_filtered() -> None:
    actions = pd.concat(
        [_shot_row(), _shot_row().assign(action_id=8, type_name="pass")],
        ignore_index=True,
    )
    frames = pd.concat([_frames(), _frames().assign(action_id=8)], ignore_index=True)
    snaps = build_tracking_snapshots(actions, frames)
    assert set(snaps.action_id.unique()) == {7}  # the pass (action 8) is dropped


def test_actor_inclusion_matches_sb360_convention() -> None:
    """M3: build_sb360_snapshots includes EVERY freeze-frame row (no actor-specific filter —
    sb360_snapshots.py lines 44-70 only read teammate/keeper/location, never an ``actor`` column).
    StatsBomb 360 freeze-frames carry the acting player, so the actor is INCLUDED. The tracking
    builder must match: the shooter is one of the frame's player rows and must be kept.
    """
    # --- SB360 side: 3 freeze rows (one is the actor, teammate=True at the event location). ---
    sb_actions = pd.DataFrame({"action_id": [7], "original_event_id": ["ev7"], "team_id": [941]})
    sb360 = pd.DataFrame(
        {
            "id": ["ev7", "ev7", "ev7"],
            "teammate": [True, True, False],  # first row = the acting player (teammate)
            "keeper": [False, False, True],
            "location": ["[100.0, 40.0]", "[80.0, 20.0]", "[118.0, 40.0]"],
        }
    )
    sb_out = build_sb360_snapshots(sb_actions, sb360)
    # sb360 keeps all 3 rows (actor NOT dropped).
    assert len(sb_out) == 3

    # --- Tracking side: equivalent shot, shooter 'a' is a player row in the frame. ---
    trk_out = build_tracking_snapshots(_shot_row(team_id=1), _frames())
    assert len(trk_out) == 3
    # The acting player 'a' (shooter's team) is present — matches sb360 including the actor.
    assert "a" in set(trk_out.player_id)
