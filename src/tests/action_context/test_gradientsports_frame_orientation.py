"""Regression: GradientSports frame LTR orientation must survive the bronze->frames
conversion (``home_team_id`` dtype contract).

Root cause (2026-06-09): the GS branch of ``pipeline._convert_tracking_batch`` passed
``home_team_id=int(meta.home_team_id)`` to silly-kicks ``gradientsports.convert_to_frames``
while ``converter_input.team_id`` is a native STRING (``gs_team_side_to_id`` maps to
string ids). With the dtype mismatch, ``play_left_to_right``'s ``is_home`` matched ZERO
players, the per-period LTR flip was silently skipped, and GS frames stayed mis-oriented
in switched-end periods (P2/P4). ``structural_pass``'s away-team defender mirror then
amplified that into a ~1e8 ``structural_sgm`` blow-up (GS match 10502 P2 action 457:
-88,955,384 in prod; reproduced to 0.002%).

The fix passes ``meta.home_team_id`` (native string) through, matching the frame
``team_id`` dtype — identical to the IDSSE/Sportec branch, which was always correct.

This test exercises the real lakehouse conversion path and asserts the LTR invariant
the bug violated: the home team must attack +x in EVERY period (so when defending it sits
at low x), and home/away must carry opposite ``team_attacking_direction``. It fails on the
pre-fix ``int(...)`` cast (P2 left unflipped → home defends high x; all rows labelled
``"ltr"``).
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.pipeline import _convert_tracking_batch
from analytics.action_context.work_unit import MatchMeta

_HOME_ID = "366"
_AWAY_ID = "51"
# (team_side, jersey) -> native-string player_id (int-castable: convert_to_frames forces Int64).
_J2P: dict[tuple[str, str], str] = {
    ("home", "1"): "36601",
    ("home", "2"): "36602",
    ("home", "3"): "36603",
    ("away", "1"): "5101",
    ("away", "2"): "5102",
    ("away", "3"): "5103",
}
_GK_IDS = ["36601", "5101"]  # jersey 1 on each side


def _player_rows(period: int, frame_num: int, t: float) -> list[dict]:
    """One frame's player + ball rows in GS centred coords ([-52.5,52.5] x [-34,34]).

    ``home_team_start_left=True``: home attacks +x in P1 (defends -x), switches in P2.
    P1 -> home cluster at low/-x, away at high/+x.  P2 -> mirrored (teams swap ends).
    GK (jersey 1) sits deepest in own half.
    """
    if period == 1:
        home = {"1": -48.0, "2": -10.0, "3": 5.0}  # home defends -x goal
        away = {"1": 48.0, "2": 10.0, "3": -5.0}  # away defends +x goal
    else:  # period 2 — ends swapped
        home = {"1": 48.0, "2": 10.0, "3": -5.0}  # home now defends +x goal
        away = {"1": -48.0, "2": -10.0, "3": 5.0}  # away now defends -x goal

    rows: list[dict] = []
    for side, xmap in (("home", home), ("away", away)):
        for jersey, x in xmap.items():
            rows.append(
                {
                    "match_id": "10502",
                    "period": period,
                    "frame_num": frame_num,
                    "period_elapsed_time": t,
                    "is_ball": False,
                    "x": x,
                    "y": 0.0,
                    "z": 0.0,
                    "team_side": side,
                    "jersey_num": jersey,
                }
            )
    rows.append(
        {
            "match_id": "10502",
            "period": period,
            "frame_num": frame_num,
            "period_elapsed_time": t,
            "is_ball": True,
            "x": 0.0,
            "y": 0.0,
            "z": 0.0,
            "team_side": None,
            "jersey_num": None,
        }
    )
    return rows


def _build_pdf() -> pd.DataFrame:
    rows: list[dict] = []
    # 7 static frames per period: enough for the velocity savgol window; positions constant.
    for period, base in ((1, 0), (2, 100)):
        for k in range(7):
            rows.extend(_player_rows(period, base + k, round(0.1 * k, 1)))
    return pd.DataFrame(rows)


def _meta() -> MatchMeta:
    return MatchMeta(
        home_team_id=_HOME_ID,  # native STRING — must NOT be re-cast to int
        home_start_left=True,
        home_team_start_left_extratime=None,
        gs_team_side_to_id={"home": _HOME_ID, "away": _AWAY_ID},
        gs_jersey_to_player_id=_J2P,
        gs_gk_player_ids=_GK_IDS,
    )


def test_gradientsports_ltr_orientation_normalizes_every_period() -> None:
    frames = _convert_tracking_batch("gradientsports", _build_pdf(), pd.DataFrame({"game_id": ["10502"]}), _meta())
    players = frames[~frames["is_ball"].astype(bool)].copy()
    assert len(players) > 0

    # Invariant 1 (positional, semantic): home attacks +x in EVERY period, so its GK
    # (own goal) sits at LOW x and the away GK at HIGH x — in both periods. The pre-fix
    # bug left P2 unflipped → P2 home GK at high x → this fails.
    for period in (1, 2):
        pf = players[players["period_id"] == period]
        gk = pf[pf["is_goalkeeper"].astype(bool)]
        home_gk_x = gk[gk["team_id"] == _HOME_ID]["x"].mean()
        away_gk_x = gk[gk["team_id"] == _AWAY_ID]["x"].mean()
        assert home_gk_x < away_gk_x, (
            f"period {period}: home must attack +x (defend low-x goal); "
            f"home_gk_x={home_gk_x:.1f} away_gk_x={away_gk_x:.1f} — frame is mis-oriented "
            f"(home_team_id dtype must match converter_input.team_id)"
        )

    # Invariant 2 (direction labels): home and away carry OPPOSITE attacking directions.
    # The bug labelled every player 'ltr' (is_home matched nobody).
    home_dirs = set(players[players["team_id"] == _HOME_ID]["team_attacking_direction"].dropna())
    away_dirs = set(players[players["team_id"] == _AWAY_ID]["team_attacking_direction"].dropna())
    assert home_dirs == {"ltr"}, f"home should attack 'ltr' in all periods, got {home_dirs}"
    assert away_dirs == {"rtl"}, f"away should attack 'rtl' in all periods, got {away_dirs}"
