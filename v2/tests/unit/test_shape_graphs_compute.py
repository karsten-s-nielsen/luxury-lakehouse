"""shape_graphs compute — the pure frame -> shape_graph_labels core (spec D3/D8; plan Task 6).

Builds a canonical frame the real way and labels each outfield player's tactical position with `shape_graph_values`,
asserting the bronze shape, that goalkeepers are excluded, and that every label is a non-empty 5x5 position string.
The match-iterating ComputeStep (catalog read + bronze write) is covered by the seeded e2e (Task 11).
"""

# pandas/silly_kicks are stub-less; relax the strict "unknown"-family rules (see src/lakehouse/compute/frames.py).
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
from __future__ import annotations

import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
import pytest
from silly_kicks.tracking.gradientsports import convert_to_frames  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.compute.control import ComputeAbandoned
from lakehouse.compute.frames import resolve_roster_goalkeepers, shape_long_input
from lakehouse.compute.shape_graphs import SHAPE_GRAPH_SCHEMA, shape_graph_values

HOME, AWAY = 100, 200
HOME_GK, AWAY_GK = 1, 11
# One keeper + four outfield per team (Delaunay needs >= 3 outfield), spread across the pitch.
_PLAYERS = [
    (HOME_GK, HOME, -50_000, 0),
    (2, HOME, -25_000, 15_000),
    (3, HOME, -25_000, -15_000),
    (4, HOME, 0, 8_000),
    (5, HOME, 10_000, -8_000),
    (AWAY_GK, AWAY, 50_000, 0),
    (12, AWAY, 25_000, 15_000),
    (13, AWAY, 25_000, -15_000),
    (14, AWAY, 0, 8_000),
    (15, AWAY, -10_000, -8_000),
]
_OUTFIELD = {pid for pid, _team, _x, _y in _PLAYERS} - {HOME_GK, AWAY_GK}


def _canonical_frame() -> pd.DataFrame:
    frame_rows: list[dict[str, object]] = []
    ball_rows: list[dict[str, object]] = []
    for frame_id in range(6):
        for pid, team, x_mm, y_mm in _PLAYERS:
            frame_rows.append(
                {
                    "canonical_match_key": 9,
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
                "canonical_match_key": 9,
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


def test_shape_graph_values_labels_outfield_players() -> None:
    table = shape_graph_values(_canonical_frame())
    assert table.schema.equals(SHAPE_GRAPH_SCHEMA)
    df = table.to_pandas()
    assert table.num_rows == 4 * 2 * 6  # four outfield players, two teams, six frames
    assert set(df["canonical_player_key"].unique()) == _OUTFIELD  # no goalkeeper row
    assert (df["position_label"].str.len() > 0).all()
    verticals = {"B", "DM", "M", "AM", "F"}
    assert df["position_label"].str[-1].isin(verticals).all()  # the trailing token is the vertical level


def test_match_key_carried() -> None:
    df = shape_graph_values(_canonical_frame()).to_pandas()
    assert (df["canonical_match_key"] == 9).all()


def test_shape_graphs_abandons_when_should_stop_turns_true() -> None:
    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2

    with pytest.raises(ComputeAbandoned):
        shape_graph_values(_canonical_frame(), should_stop=should_stop, check_every=1)


def test_shape_graphs_output_unchanged_with_should_stop_none() -> None:
    frame = _canonical_frame()
    plain = shape_graph_values(frame)
    assert plain.equals(shape_graph_values(frame, should_stop=lambda: False, check_every=1))
    assert plain.equals(shape_graph_values(frame, should_stop=None))
