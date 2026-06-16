"""Metrica builder y-convention guard (RED on the pre-fix `(1 - y01)` flip, GREEN after).

`_bronze_metrica_to_frames` was found Y-INVERTED on a live full-match check (Sample_Game_1 P1,
n=346 acting-player + 331 ball: y-mirror wins 335/346, d_yflip median 0.19 m vs d_identity 43.4 m;
identical pre/post `correct_frames_to_home_ltr`, so it reached production). The builder's old
`y = (1 - y01) * 68` flip is wrong — Metrica's normalized y is ALREADY SPADL bottom-to-top, so the
correct map is `y = y01 * 68` (mirroring SkillCorner, whose builder does NOT flip).

This is a deterministic synthetic guard (the committed Metrica AC fixture's action<->frame jersey
mapping is too sparse/mismatched to localize — see test_frame_orientation_golden's note). The
cross-provider real-data y-identity golden lives in test_frame_y_identity_golden.py (SkillCorner; a
real Metrica slice is a follow-up once a correctly-jersey-aligned fixture is extracted).
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from analytics.action_context.convert import _bronze_metrica_to_frames

_FRAME_RATE = 25
_Y01 = 0.25  # near Metrica's y=0 edge. SPADL-correct = 0.25*68 = 17.0; the OLD flip gave (1-0.25)*68 = 51.0.
_EXPECTED_Y = _Y01 * 68.0  # 17.0


def _synthetic_bronze(y01: float, n: int = 13) -> pd.DataFrame:
    """n>=11 frames so the builder's Savitzky-Golay velocity pass (window 11 @ 25 fps) has support."""
    rows = [
        {
            "period": 1,
            "frame": 100 + i,
            "timestamp": 4.0 + i / _FRAME_RATE,
            "frame_rate": _FRAME_RATE,
            "ball_x": 0.40,
            "ball_y": y01,
            "home_players": json.dumps({"7": {"x": 0.40, "y": y01}}),
            "away_players": json.dumps({}),
            "gk_jersey_numbers": json.dumps(["1"]),
        }
        for i in range(n)
    ]
    return pd.DataFrame(rows)


def test_metrica_builder_y_is_not_flipped() -> None:
    frames = _bronze_metrica_to_frames(
        _synthetic_bronze(_Y01), game_id=1, jersey_to_pid={"7": "Player7"}, fallback_fmt="Player{}"
    )
    player_y = frames.loc[(~frames["is_ball"].astype(bool)) & (frames["player_id"] == "Player7"), "y"]
    ball_y = frames.loc[frames["is_ball"].astype(bool), "y"]
    assert not player_y.empty and not ball_y.empty

    # GREEN: y01 * 68 = 17.0. RED on the old (1 - y01) * 68 = 51.0 flip.
    assert player_y.iloc[0] == pytest.approx(_EXPECTED_Y), (
        f"metrica player y={player_y.iloc[0]:.2f}, expected {_EXPECTED_Y:.2f} (y01*68, no flip) — "
        "the (1-y01) flip is the live-confirmed Y-MIRROR bug"
    )
    assert ball_y.iloc[0] == pytest.approx(_EXPECTED_Y), (
        f"metrica ball y={ball_y.iloc[0]:.2f}, expected {_EXPECTED_Y:.2f} (y01*68, no flip)"
    )
