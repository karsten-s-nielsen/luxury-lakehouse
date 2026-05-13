"""Tests for Metrica tracking player_id format normalization.

Verifies that _bronze_metrica_to_frames normalizes bare jersey JSON keys
to kloppy 'PlayerN' format at the converter layer (bronze stores raw format).
"""

from __future__ import annotations

import json


def test_converter_normalizes_jersey_to_player_prefix() -> None:
    """_bronze_metrica_to_frames produces 'PlayerN' player_id from bare jersey JSON keys."""
    import pandas as pd

    from ingestion.tracking_context import _bronze_metrica_to_frames

    # Bronze format: bare jersey numbers as JSON keys.
    # Need 3+ frames per player for velocity derivation (np.gradient).
    home = json.dumps({"11": {"x": 0.5, "y": 0.3}})
    away = json.dumps({"25": {"x": 0.6, "y": 0.7}})
    gk = json.dumps(["1"])
    trk_pdf = pd.DataFrame(
        {
            "period": [1, 1, 1],
            "frame": [1, 2, 3],
            "timestamp": [0.04, 0.08, 0.12],
            "frame_rate": [25, 25, 25],
            "gk_jersey_numbers": [gk, gk, gk],
            "home_players": [home, home, home],
            "away_players": [away, away, away],
            "ball_x": [0.5, 0.5, 0.5],
            "ball_y": [0.5, 0.5, 0.5],
        }
    )

    frames = _bronze_metrica_to_frames(trk_pdf, game_id=1)
    player_rows = frames[~frames["is_ball"]]

    player_ids = set(player_rows["player_id"].tolist())
    # Should be "Player11" and "Player25", NOT "Home_11" and "Away_25"
    assert "Player11" in player_ids, f"Expected 'Player11', got {player_ids}"
    assert "Player25" in player_ids, f"Expected 'Player25', got {player_ids}"
    assert not any(pid.startswith("Home_") or pid.startswith("Away_") for pid in player_ids), (
        f"player_id should not use Home_/Away_ prefix: {player_ids}"
    )


def test_converter_gk_detection_with_player_prefix() -> None:
    """GK detection works with Player-prefixed jersey matching bare gk_jersey_numbers."""
    import pandas as pd

    from ingestion.tracking_context import _bronze_metrica_to_frames

    # Need 3+ frames per player for velocity derivation (np.gradient).
    home = json.dumps({"1": {"x": 5.0, "y": 34.0}, "11": {"x": 50.0, "y": 34.0}})
    away = json.dumps({})
    gk = json.dumps(["1"])
    trk_pdf = pd.DataFrame(
        {
            "period": [1, 1, 1],
            "frame": [1, 2, 3],
            "timestamp": [0.04, 0.08, 0.12],
            "frame_rate": [25, 25, 25],
            "gk_jersey_numbers": [gk, gk, gk],
            "home_players": [home, home, home],
            "away_players": [away, away, away],
            "ball_x": [0.5, 0.5, 0.5],
            "ball_y": [0.5, 0.5, 0.5],
        }
    )

    frames = _bronze_metrica_to_frames(trk_pdf, game_id=1)
    player_rows = frames[~frames["is_ball"]]

    gk_rows = player_rows[player_rows["player_id"] == "Player1"]
    non_gk_rows = player_rows[player_rows["player_id"] == "Player11"]

    assert len(gk_rows) == 3  # 3 frames
    assert all(gk_rows["is_goalkeeper"])
    assert len(non_gk_rows) == 3
    assert not any(non_gk_rows["is_goalkeeper"])
