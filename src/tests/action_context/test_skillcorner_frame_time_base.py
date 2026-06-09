"""Regression: SkillCorner frames must be PERIOD-RELATIVE on time_seconds.

silly-kicks 4.20.1 re-based SkillCorner SPADL ``time_seconds`` to period-relative
(subtracting the nominal period-start offset). SkillCorner bronze tracking ``timestamp``
is the continuous broadcast clock (2nd half = 45:00+), so ``_bronze_skillcorner_to_frames``
must subtract the same offset — otherwise the action↔frame linker (which matches on
``time_seconds``) silently collapses in the 2nd half and beyond (the mirror image of the
GS absolute-clock class, ADR-040).

This test feeds an absolute-clock bronze slice (P1 ≈ 0 s, P2 ≈ 2700 s) through the real
converter and asserts the emitted frames reset to ~0 each period. It fails on the pre-4.20.1
behaviour (P2 frames at ≈ 2700 s).
"""

from __future__ import annotations

import pandas as pd

from analytics.action_context.convert import _bronze_skillcorner_to_frames

_FRAME_RATE = 10


def _rows(period: int, base_frame: int, base_ts: float) -> list[dict]:
    rows: list[dict] = []
    for k in range(6):  # 6 frames/period (< savgol window → np.gradient fallback, no error)
        ts = base_ts + 0.1 * k
        for pid, team, gk in (("p1", "100", True), ("p2", "100", False), ("p3", "200", False)):
            rows.append(
                {
                    "frame": base_frame + k,
                    "period": period,
                    "timestamp": ts,
                    "player_id": pid,
                    "team": team,
                    "x": 0.0,
                    "y": 0.0,
                    "is_goalkeeper": gk,
                    "frame_rate": _FRAME_RATE,
                    "ball_x": 1.0,
                    "ball_y": 1.0,
                }
            )
    return rows


def _bronze() -> pd.DataFrame:
    rows = _rows(period=1, base_frame=0, base_ts=0.0)  # P1: absolute ≈ [0.0, 0.5]
    rows += _rows(period=2, base_frame=100, base_ts=45 * 60.0)  # P2: absolute ≈ [2700.0, 2700.5]
    return pd.DataFrame(rows)


def test_skillcorner_frames_time_seconds_are_period_relative() -> None:
    frames = _bronze_skillcorner_to_frames(_bronze(), game_id=1886347)

    p1 = frames[frames["period_id"] == 1]["time_seconds"]
    p2 = frames[frames["period_id"] == 2]["time_seconds"]
    assert len(p1) > 0 and len(p2) > 0

    # P1 already period-relative (offset 0): unchanged ~ [0, 0.5].
    assert p1.min() >= 0.0 and p1.max() < 1.0, f"P1 time_seconds drifted: [{p1.min()}, {p1.max()}]"
    # P2 MUST be re-based to ~0 (absolute 2700 - 2700 offset), NOT ~ 2700.
    assert p2.max() < 1.0, (
        f"P2 time_seconds not period-relative: [{p2.min()}, {p2.max()}] - expected ~0, "
        f"got absolute clock (SkillCorner frame re-base missing)"
    )
