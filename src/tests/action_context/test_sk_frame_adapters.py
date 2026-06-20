"""Unit tests for the silly-kicks frame-builder adapters (TF-23, ADR-053/ADR-034).

The adapters shape post-join bronze to the silly-kicks ``tracking.{skillcorner,metrica}``
``convert_to_frames`` contract, then (B') overwrite the builder clock with the dispatcher's
period-relative clock and derive lakehouse velocities. These tests pin the integration seam:
ball_z recovery, SPADL coordinates, geometric LTR orientation, the clock overwrite, and the
AC result-frame schema.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from analytics.action_context.sk_frame_adapters import (
    _AC_FRAME_COLUMNS,
    convert_metrica_bronze_to_frames,
    convert_skillcorner_bronze_to_frames,
)


def _sc_bronze() -> pd.DataFrame:
    """Synthetic post-join SkillCorner bronze: home GK deep (low x), away high, ball with z."""
    rows = []
    for fr in range(6):
        ts = fr / 10.0
        for pid, team, xc, y, gk in [
            ("hgk", "H", -40.0 + fr * 0.1, 0.0, True),
            ("afw", "A", 30.0 + fr * 0.1, 2.0, False),
            ("agk", "A", 45.0, 0.0, True),
        ]:
            rows.append(
                {
                    "match_id": "M1",
                    "period": 1,
                    "frame": fr,
                    "timestamp": ts,
                    "frame_rate": 10.0,
                    "player_id": pid,
                    "team_id": team,
                    "x": xc,
                    "y": y,
                    "is_goalkeeper": gk,
                    "is_visible": True,
                    "ball_x": float(fr),
                    "ball_y": 0.0,
                    "ball_z": 1.5 + fr * 0.3,
                }
            )
    return pd.DataFrame(rows)


def _period_relative_time(bronze: pd.DataFrame, *, offset: float) -> pd.DataFrame:
    """Dispatcher's period-relative clock, deliberately offset so the B' overwrite is observable."""
    prt = bronze[["frame", "period"]].drop_duplicates().rename(columns={"frame": "frame_id", "period": "period_id"})
    prt["time_seconds"] = prt["frame_id"] * 0.1 + offset
    return prt.reset_index(drop=True)


def test_skillcorner_adapter_recovers_ball_z_and_orients_ltr() -> None:
    bronze = _sc_bronze()
    prt = _period_relative_time(bronze, offset=100.0)
    frames, _report = convert_skillcorner_bronze_to_frames(
        bronze, game_id=99, home_team_id="H", period_relative_time=prt
    )

    # ball z recovered (NOT NaN) — the SC ball_z unlock
    ball = frames[frames["is_ball"]].sort_values("frame_id")
    assert ball["z"].notna().all(), "ball z must be populated from bronze ball_z"
    assert np.isclose(ball["z"].iloc[0], 1.5), f"frame-0 ball z {ball['z'].iloc[0]} != input 1.5"

    # SPADL coordinate range
    assert frames["x"].between(0, 105).all() and frames["y"].between(0, 68).all()

    # geometric LTR: home GK at low x, away GK at high x
    p1 = frames[(frames["period_id"] == 1) & (~frames["is_ball"])]
    home_gk_x = p1[(p1["team_id"] == "H") & (p1["is_goalkeeper"])]["x"].median()
    away_gk_x = p1[(p1["team_id"] == "A") & (p1["is_goalkeeper"])]["x"].median()
    assert home_gk_x < 52.5 < away_gk_x, f"home GK {home_gk_x} / away GK {away_gk_x} not home-LTR"


def test_skillcorner_adapter_overwrites_clock_via_map_join() -> None:
    """B': the builder's own time_seconds is discarded; output carries the dispatcher clock."""
    bronze = _sc_bronze()
    prt = _period_relative_time(bronze, offset=100.0)  # offset 100 != builder's 0.0-0.5
    frames, _ = convert_skillcorner_bronze_to_frames(bronze, game_id=99, home_team_id="H", period_relative_time=prt)
    assert frames["time_seconds"].notna().all(), "no row may be left unmapped by the clock join"
    # frame 0 -> 100.0, frame 5 -> 100.5 (period-relative + 100 offset), NOT the builder's 0.0-0.5
    by_frame = frames.groupby("frame_id")["time_seconds"].first()
    assert np.isclose(by_frame.loc[0], 100.0) and np.isclose(by_frame.loc[5], 100.5)


def test_skillcorner_adapter_derives_velocities_and_matches_schema() -> None:
    bronze = _sc_bronze()
    prt = _period_relative_time(bronze, offset=0.0)
    frames, _ = convert_skillcorner_bronze_to_frames(bronze, game_id=99, home_team_id="H", period_relative_time=prt)
    assert {"vx", "vy", "speed"} <= set(frames.columns), "velocity columns must be derived post-builder"
    assert set(frames.columns) == _AC_FRAME_COLUMNS, f"schema drift: {set(frames.columns) ^ _AC_FRAME_COLUMNS}"


# ── Metrica adapter ────────────────────────────────────────────────────────


def _metrica_bronze(start_frame: int = 0) -> pd.DataFrame:
    """Synthetic Metrica bronze (frame-level JSON). Jersey 1 = home GK deep, 12 = away GK."""
    rows = []
    for i in range(6):
        fr = start_frame + i
        ts = fr / 25.0  # period-relative (this synthetic period starts at frame 0)
        home = {"1": {"x": 0.1, "y": 0.5}, "10": {"x": 0.4, "y": 0.5}}
        away = {"12": {"x": 0.9, "y": 0.5}, "11": {"x": 0.6, "y": 0.5}}
        rows.append(
            {
                "period": 1,
                "frame": fr,
                "timestamp": ts,
                "frame_rate": 25.0,
                "ball_x": 0.5,
                "ball_y": 0.5,
                "home_players": json.dumps(home),
                "away_players": json.dumps(away),
                "gk_jersey_numbers": json.dumps(["1", "12"]),
            }
        )
    return pd.DataFrame(rows)


def _metrica_roster(drop: str | None = None) -> dict[str, dict[str, str]]:
    home = {"1": "p_hgk", "10": "p_h10"}
    away = {"12": "p_agk", "11": "p_a11"}
    if drop == "h10":
        home.pop("10")  # leave an acting jersey unmapped -> synthetic "Home_10"
    return {"Home": home, "Away": away}


def _metrica_prt(bronze: pd.DataFrame) -> pd.DataFrame:
    prt = bronze[["frame", "period"]].drop_duplicates().rename(columns={"frame": "frame_id", "period": "period_id"})
    prt["time_seconds"] = prt["frame_id"] / 25.0  # period-relative from period start
    return prt.reset_index(drop=True)


def test_metrica_adapter_orients_resolves_roster_and_schema() -> None:
    bronze = _metrica_bronze()
    frames, _ = convert_metrica_bronze_to_frames(
        bronze, game_id=7, jersey_to_player_id=_metrica_roster(), period_relative_time=_metrica_prt(bronze)
    )
    assert frames["z"].isna().all(), "Metrica bronze has no ball z -> z stays NaN"
    b = frames[~frames["is_ball"]]
    assert set(b["player_id"].dropna()) <= {"p_hgk", "p_h10", "p_agk", "p_a11"}, "ids must be roster pids"
    p1 = b[b["period_id"] == 1]
    hg = p1[(p1["team_id"] == "Home") & (p1["is_goalkeeper"])]["x"].median()
    ag = p1[(p1["team_id"] == "Away") & (p1["is_goalkeeper"])]["x"].median()
    assert hg < 52.5 < ag, f"home GK {hg} / away GK {ag} not home-LTR"
    assert set(frames.columns) == _AC_FRAME_COLUMNS


def test_metrica_adapter_overwrites_clock_on_mid_period_batch() -> None:
    """D1 gate: a batch NOT starting at the period's first frame must keep period-relative-from-
    period-start time (the builder's per-batch-min re-zero would give 0.0-0.2 instead of 4.0-4.2)."""
    bronze = _metrica_bronze(start_frame=100)
    frames, _ = convert_metrica_bronze_to_frames(
        bronze, game_id=7, jersey_to_player_id=_metrica_roster(), period_relative_time=_metrica_prt(bronze)
    )
    by_frame = frames.groupby("frame_id")["time_seconds"].first()
    assert np.isclose(by_frame.loc[100], 4.0) and np.isclose(by_frame.loc[105], 4.2), (
        "clock not period-relative-from-period-start (builder batch re-zero leaked through)"
    )


def test_metrica_adapter_warns_on_unmapped_jersey() -> None:
    """D5 gate: an unmapped acting jersey -> synthetic "Home_10" id is surfaced (linkage risk)."""
    bronze = _metrica_bronze()
    with pytest.warns(UserWarning, match="synthetic roster-fallback"):
        convert_metrica_bronze_to_frames(
            bronze,
            game_id=7,
            jersey_to_player_id=_metrica_roster(drop="h10"),
            period_relative_time=_metrica_prt(bronze),
        )
