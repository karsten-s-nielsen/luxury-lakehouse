"""Metrica CSV-path GK derivation (2026-06-11 AC value-audit fix).

The CSV games carry no position metadata, and the former "jersey 1" heuristic was
empirically wrong: on the sample data jersey 1 is an OUTFIELD player (period-1 mean
x 0.442) while the home GK is jersey 11 (mean x 0.125) — bronze shipped
``gk_jersey_numbers=["1"]`` for all three games, so every frame's ``is_goalkeeper``
was misassigned. Downstream blast radius: ``team_shape_n_outfield_players_* == 11``
on ~65% of metrica AC rows, off-pitch ghost-GK positions, and GK metrics attributed
around the wrong player.

These tests pin the replacement: per-team PERIOD-1 positional-depth derivation
(the GK is the team's outlier from the pitch centre, 0.5 in normalized coords).
"""

from __future__ import annotations

import json

import pandas as pd

from ingestion.metrica_tracking import _derive_gk_jerseys_empirically, _reshape_tracking_to_narrow


def _wide_df() -> pd.DataFrame:
    """Sample-data-shaped wide frame: home GK=11 deep, away GK=25 deep, jersey 1 midfield.

    Period 2 swaps ends — included to prove the derivation must NOT average across halves
    (the depth signal cancels: 0.125 in P1 mirrors to ~0.875 in P2).
    """
    n = 20
    rows = []
    for i in range(n):
        period = 1 if i < 10 else 2
        flip = period == 2
        rows.append(
            {
                "Period": period,
                "Frame": i,
                "Time [s]": i * 0.04,
                # home GK (11): very deep; jersey 1: midfield; jersey 7: attacking
                "Home_11_x": 0.88 if flip else 0.12,
                "Home_11_y": 0.5,
                "Home_1_x": 0.56 if flip else 0.44,
                "Home_1_y": 0.4,
                "Home_7_x": 0.35 if flip else 0.65,
                "Home_7_y": 0.6,
                # away GK (25): deep at the OTHER end; jersey 15: midfield
                "Away_25_x": 0.10 if flip else 0.90,
                "Away_25_y": 0.5,
                "Away_15_x": 0.45 if flip else 0.55,
                "Away_15_y": 0.5,
                "Ball_x": 0.5,
                "Ball_y": 0.5,
            }
        )
    return pd.DataFrame(rows)


_PLAYER_GROUPS = {
    "Home": [("11", "Home_11_x", "Home_11_y"), ("1", "Home_1_x", "Home_1_y"), ("7", "Home_7_x", "Home_7_y")],
    "Away": [("25", "Away_25_x", "Away_25_y"), ("15", "Away_15_x", "Away_15_y")],
}


def test_derives_one_gk_per_team_by_period1_depth() -> None:
    df = _wide_df()
    gks = _derive_gk_jerseys_empirically(df, _PLAYER_GROUPS, pd.to_numeric(df["Period"]))
    assert gks == ["11", "25"]  # NOT jersey "1" — the old heuristic's wrong pick


def test_period2_end_swap_does_not_cancel_the_signal() -> None:
    # Whole-match averaging would put the GKs near 0.5 (ends swap); period-1-only must not.
    df = _wide_df()
    whole_match_mean_gk = pd.to_numeric(df["Home_11_x"]).mean()
    assert abs(whole_match_mean_gk - 0.5) < 0.05  # the trap the derivation must avoid
    gks = _derive_gk_jerseys_empirically(df, _PLAYER_GROUPS, pd.to_numeric(df["Period"]))
    assert "11" in gks


def test_reshape_emits_derived_gk_jerseys_in_bronze_column() -> None:
    bronze = _reshape_tracking_to_narrow(_wide_df(), match_id="Sample_Game_T")
    gk_sets = {tuple(json.loads(v)) for v in bronze["gk_jersey_numbers"]}
    assert gk_sets == {("11", "25")}
