"""SkillCorner frame time-base: the CONVERTER is pass-through; the DISPATCHER re-bases.

History (two incidents, one contract):

- silly-kicks 4.20.1 re-based SkillCorner SPADL ``time_seconds`` to period-relative, and the
  converter (``_bronze_skillcorner_to_frames``) gained its own offset subtraction so converted
  frames matched. That fixed the LINKER — but the DISPATCH layer (per-batch action window +
  M13 ownership) reads the bronze ``timestamp`` BEFORE conversion, so period >= 2 still silently
  dropped ~90% of actions (2026-06-11 census, run 1020873732479562).
- The ADR-040 amendment moved the re-base to the DISPATCH layer (both drivers, importing
  ``_SKILLCORNER_PERIOD_START_SECONDS``; see test_skillcorner_dispatch_time_base). With the
  converter ALSO subtracting, frames came out at ≈ -2700 s (double subtraction; linker found
  nothing for P2) — so the converter is now PASS-THROUGH on the time base.

Exactly ONE layer owns the re-base: the dispatcher. This test pins the converter side of that
contract: period-relative input (what the dispatcher hands it) emerges unchanged.
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


def _bronze_period_relative() -> pd.DataFrame:
    """Post-dispatch shape: BOTH periods period-relative (≈ [0, 0.5] each)."""
    rows = _rows(period=1, base_frame=0, base_ts=0.0)
    rows += _rows(period=2, base_frame=100, base_ts=0.0)
    return pd.DataFrame(rows)


def test_skillcorner_converter_passes_period_relative_time_through() -> None:
    frames = _bronze_skillcorner_to_frames(_bronze_period_relative(), game_id=1886347)

    p1 = frames[frames["period_id"] == 1]["time_seconds"]
    p2 = frames[frames["period_id"] == 2]["time_seconds"]
    assert len(p1) > 0 and len(p2) > 0

    # Pass-through: both periods stay ≈ [0, 0.5]. A re-grown converter subtraction would
    # push P2 NEGATIVE (≈ -2700) — the double-subtraction class.
    assert p1.min() >= 0.0 and p1.max() < 1.0, f"P1 time_seconds drifted: [{p1.min()}, {p1.max()}]"
    assert p2.min() >= 0.0, f"P2 time_seconds went negative (double subtraction): min={p2.min()}"
    assert p2.max() < 1.0, f"P2 time_seconds not pass-through: [{p2.min()}, {p2.max()}]"
