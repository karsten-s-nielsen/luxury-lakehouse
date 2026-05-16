"""Tests for tracking context converter functions.

Exercises the Metrica CSV parser (ball column, player ID format)
against local fixtures without Spark or Databricks.

Note: Tests import underscore-prefixed functions (_build_player_columns,
_parse_tracking_header, _bronze_metrica_to_frames). These are internal
helpers — tests will break if they are renamed. Acceptable trade-off:
these parsers are stable, and direct testing is the only way to verify
the CSV header parsing without downloading from GitHub in CI.
"""

from __future__ import annotations


def test_ball_columns_parsed_from_csv_header() -> None:
    """Fix A: 'Ball' appears in column_row (stripped), not jersey_row.

    The 3-row header for Metrica CSV Games 1+2 places 'Ball' in the
    column_row (row 2), not the jersey_row (row 1). The parser must
    detect Ball in EITHER row to produce Ball_x and Ball_y columns.

    Fixture: src/tests/fixtures/metrica_tracking_home.csv
    Header row 0 (team):   ,,,Home,,Home,,,
    Header row 1 (jersey):  ,,,Player11,,Player1,,,
    Header row 2 (column): Period,Frame,Time [s],Player11,,Player1,,Ball,
    """
    from pathlib import Path

    from ingestion.metrica_tracking import _build_player_columns, _parse_tracking_header

    fixture = Path(__file__).parent / "fixtures" / "metrica_tracking_home.csv"
    csv_text = fixture.read_text()
    team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
    columns = _build_player_columns(team_row, jersey_row, column_row)

    assert "Ball_x" in columns, f"Ball_x not found in columns: {columns}"
    assert "Ball_y" in columns, f"Ball_y not found in columns: {columns}"


def test_ball_data_present_after_csv_parse() -> None:
    """Fix A end-to-end: After parsing, Ball_x/Ball_y have non-null values."""
    import io
    from pathlib import Path

    import pandas as pd

    from ingestion.metrica_tracking import _build_player_columns, _parse_tracking_header

    fixture = Path(__file__).parent / "fixtures" / "metrica_tracking_home.csv"
    csv_text = fixture.read_text()
    team_row, jersey_row, column_row = _parse_tracking_header(csv_text)
    columns = _build_player_columns(team_row, jersey_row, column_row)

    df = pd.read_csv(io.StringIO(csv_text), skiprows=3, header=None, names=columns)

    assert "Ball_x" in df.columns, "Ball_x column missing after parse"
    non_null_count = df["Ball_x"].notna().sum()
    assert non_null_count > 0, f"Ball_x has 0 non-null values out of {len(df)}"


def test_metrica_frames_player_id_matches_spadl_format() -> None:
    """Fix B: _bronze_metrica_to_frames must produce player_ids matching SPADL.

    Game 3 SPADL has 'Player 22' (with space). The converter hardcodes
    'Player{jersey}' (no space). Fix: data-driven lookup from SPADL actions.

    Uses synthetic bronze tracking + actions to verify the lookup works
    for BOTH formats (with-space and without-space).
    """
    import pandas as pd

    from ingestion.tracking_context import _bronze_metrica_to_frames

    # Simulate Game 3 bronze tracking (2 frames — np.gradient needs ≥2 points)
    trk_pdf = pd.DataFrame(
        {
            "period": [1, 1],
            "frame": [100, 101],
            "timestamp": [4.0, 4.04],
            "ball_x": [0.5, 0.51],
            "ball_y": [0.5, 0.49],
            "home_players": [
                '{"22": {"x": 0.3, "y": 0.4}}',
                '{"22": {"x": 0.31, "y": 0.41}}',
            ],
            "away_players": [
                '{"11": {"x": 0.7, "y": 0.6}}',
                '{"11": {"x": 0.71, "y": 0.59}}',
            ],
            "gk_jersey_numbers": ['["1"]', '["1"]'],
            "pitch_length_m": [105.0, 105.0],
            "pitch_width_m": [68.0, 68.0],
            "frame_rate": [25, 25],
        }
    )

    # Case 1: Game 3 format — SPADL has "Player 22" (with space)
    jersey_to_pid_spaced = {"22": "Player 22", "11": "Player 11"}
    fallback_fmt_spaced = "Player {}"
    frames_spaced = _bronze_metrica_to_frames(
        trk_pdf,
        game_id=3,
        jersey_to_pid=jersey_to_pid_spaced,
        fallback_fmt=fallback_fmt_spaced,
    )
    player_ids = frames_spaced[~frames_spaced["is_ball"]]["player_id"].tolist()
    assert "Player 22" in player_ids, f"Expected 'Player 22' in {player_ids}"
    assert "Player 11" in player_ids, f"Expected 'Player 11' in {player_ids}"

    # Case 2: Games 1+2 format — SPADL has "Player22" (no space)
    jersey_to_pid_nospace = {"22": "Player22", "11": "Player11"}
    fallback_fmt_nospace = "Player{}"
    frames_nospace = _bronze_metrica_to_frames(
        trk_pdf,
        game_id=1,
        jersey_to_pid=jersey_to_pid_nospace,
        fallback_fmt=fallback_fmt_nospace,
    )
    player_ids_ns = frames_nospace[~frames_nospace["is_ball"]]["player_id"].tolist()
    assert "Player22" in player_ids_ns, f"Expected 'Player22' in {player_ids_ns}"
    assert "Player11" in player_ids_ns, f"Expected 'Player11' in {player_ids_ns}"


def test_metrica_frames_player_id_fallback_for_unknown_jersey() -> None:
    """Fix B fallback: Jerseys not in SPADL actions use the format-aware fallback."""
    import pandas as pd

    from ingestion.tracking_context import _bronze_metrica_to_frames

    trk_pdf = pd.DataFrame(
        {
            "period": [1, 1],
            "frame": [100, 101],
            "timestamp": [4.0, 4.04],
            "ball_x": [0.5, 0.51],
            "ball_y": [0.5, 0.49],
            "home_players": [
                '{"99": {"x": 0.3, "y": 0.4}}',
                '{"99": {"x": 0.31, "y": 0.41}}',
            ],
            "away_players": ["{}", "{}"],
            "gk_jersey_numbers": ['["1"]', '["1"]'],
            "pitch_length_m": [105.0, 105.0],
            "pitch_width_m": [68.0, 68.0],
            "frame_rate": [25, 25],
        }
    )

    # Jersey "99" NOT in jersey_to_pid — must use fallback format
    jersey_to_pid = {"22": "Player 22"}
    fallback_fmt = "Player {}"
    frames = _bronze_metrica_to_frames(
        trk_pdf,
        game_id=3,
        jersey_to_pid=jersey_to_pid,
        fallback_fmt=fallback_fmt,
    )
    player_ids = frames[~frames["is_ball"]]["player_id"].tolist()
    assert "Player 99" in player_ids, f"Expected 'Player 99' (spaced fallback) in {player_ids}"
