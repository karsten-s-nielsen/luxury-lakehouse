"""Unit tests for the geometric home-LTR correction net (``correct_frames_to_home_ltr``).

The net is the flag-free orientation authority for the action-context frame path: it
orients metrica/skillcorner frames built in ABSOLUTE convention AND corrects the
per-match GradientSports extra-time provider flip (tracking ET end-flipped vs events),
reading direction purely from goalkeeper geometry. See
``reference-gs-et-flag-placeholder-unreliable`` /
``reference-ac-frame-orientation-per-provider``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analytics.action_context.pipeline import correct_frames_to_home_ltr


def _frame_rows(
    period: int,
    frame_id: int,
    *,
    home_gk_x: float,
    away_gk_x: float,
    home_label: str | None,
    away_label: str | None,
    home_gk_vx: float = 1.5,
) -> list[dict]:
    """One frame: home GK + away GK + ball. y at mid; velocities given for the home GK."""
    return [
        {
            "team_id": "Home",
            "period_id": period,
            "frame_id": frame_id,
            "is_ball": False,
            "is_goalkeeper": True,
            "x": home_gk_x,
            "y": 34.0,
            "vx": home_gk_vx,
            "vy": 0.5,
            "speed": float(np.hypot(home_gk_vx, 0.5)),
            "team_attacking_direction": home_label,
        },
        {
            "team_id": "Away",
            "period_id": period,
            "frame_id": frame_id,
            "is_ball": False,
            "is_goalkeeper": True,
            "x": away_gk_x,
            "y": 34.0,
            "vx": -1.0,
            "vy": 0.0,
            "speed": 1.0,
            "team_attacking_direction": away_label,
        },
        {
            "team_id": None,
            "period_id": period,
            "frame_id": frame_id,
            "is_ball": True,
            "is_goalkeeper": False,
            "x": 52.5,
            "y": 34.0,
            "vx": 0.0,
            "vy": 0.0,
            "speed": 0.0,
            "team_attacking_direction": None,
        },
    ]


def _assert_home_defends_low(frames: pd.DataFrame) -> None:
    players = frames[~frames["is_ball"].astype(bool)]
    gk = players[players["is_goalkeeper"].astype(bool)]
    for period, g in gk.groupby("period_id"):
        home_x = g[g["team_id"] == "Home"]["x"].median()
        away_x = g[g["team_id"] == "Away"]["x"].median()
        assert home_x < away_x, f"period {period}: home GK ({home_x}) must be below away GK ({away_x})"


def test_metrica_absolute_oriented_and_labeled() -> None:
    # P1 home attacks LEFT (home GK high) -> must flip. P2 home attacks RIGHT -> no flip.
    rows = _frame_rows(1, 1, home_gk_x=95.0, away_gk_x=10.0, home_label=None, away_label=None) + _frame_rows(
        2, 100, home_gk_x=10.0, away_gk_x=95.0, home_label=None, away_label=None
    )
    out = correct_frames_to_home_ltr(pd.DataFrame(rows), home_team_id="Home", provider="metrica")
    _assert_home_defends_low(out)

    # P1 was flipped: home GK 95 -> 10; labels populated (null builder input).
    p1_home_gk = out[(out["period_id"] == 1) & (out["team_id"] == "Home") & out["is_goalkeeper"]]
    assert p1_home_gk["x"].iloc[0] == 10.0
    assert p1_home_gk["vx"].iloc[0] == -1.5  # vx negated by the 180-degree flip
    assert set(out[(out["team_id"] == "Home") & ~out["is_ball"]]["team_attacking_direction"]) == {"ltr"}
    assert set(out[(out["team_id"] == "Away") & ~out["is_ball"]]["team_attacking_direction"]) == {"rtl"}


def test_already_correct_is_noop() -> None:
    rows = _frame_rows(1, 1, home_gk_x=8.0, away_gk_x=97.0, home_label=None, away_label=None) + _frame_rows(
        2, 100, home_gk_x=8.0, away_gk_x=97.0, home_label=None, away_label=None
    )
    src = pd.DataFrame(rows)
    out = correct_frames_to_home_ltr(src, home_team_id="Home", provider="metrica")
    _assert_home_defends_low(out)
    # No flip: x/vx unchanged.
    pd.testing.assert_series_equal(out["x"], src["x"])
    p1_home_gk = out[(out["period_id"] == 1) & (out["team_id"] == "Home") & out["is_goalkeeper"]]
    assert p1_home_gk["vx"].iloc[0] == 1.5


def test_gs_extra_time_flip_corrected_labels_preserved() -> None:
    # GS/idsse-style: convert_to_frames asserts home="ltr". Regular P1/P2 correct, but the
    # provider's ET P3/P4 tracking is end-flipped (home GK high) -> net must flip P3/P4.
    rows = (
        _frame_rows(1, 1, home_gk_x=8.0, away_gk_x=97.0, home_label="ltr", away_label="rtl")
        + _frame_rows(2, 100, home_gk_x=8.0, away_gk_x=97.0, home_label="ltr", away_label="rtl")
        + _frame_rows(3, 200, home_gk_x=96.0, away_gk_x=9.0, home_label="ltr", away_label="rtl")
        + _frame_rows(4, 300, home_gk_x=95.0, away_gk_x=11.0, home_label="ltr", away_label="rtl")
    )
    out = correct_frames_to_home_ltr(pd.DataFrame(rows), home_team_id="Home", provider="gradientsports")
    _assert_home_defends_low(out)  # every period now home-low
    # Labels were already present -> untouched (still home=ltr/away=rtl everywhere).
    assert set(out[(out["team_id"] == "Home") & ~out["is_ball"]]["team_attacking_direction"]) == {"ltr"}
    # ball flips too in flipped periods (point reflection): P3 ball x 52.5 -> 52.5 (symmetric center).
    p3_ball = out[(out["period_id"] == 3) & out["is_ball"]]
    assert p3_ball["x"].iloc[0] == 52.5


def test_speed_unchanged_on_flip() -> None:
    rows = _frame_rows(1, 1, home_gk_x=95.0, away_gk_x=10.0, home_label=None, away_label=None)
    src = pd.DataFrame(rows)
    out = correct_frames_to_home_ltr(src, home_team_id="Home", provider="metrica")
    # period flipped, but speed is a magnitude — invariant under point reflection.
    pd.testing.assert_series_equal(out["speed"], src["speed"])


def test_zero_home_match_returns_unoriented() -> None:
    rows = _frame_rows(1, 1, home_gk_x=95.0, away_gk_x=10.0, home_label=None, away_label=None)
    src = pd.DataFrame(rows)
    out = correct_frames_to_home_ltr(src, home_team_id="NoSuchTeam", provider="metrica")
    # Guard: wrong home id matches nobody -> refuse to guess, return unchanged geometry.
    pd.testing.assert_series_equal(out["x"], src["x"])
