"""Unit tests for the ET start-direction derivers introduced for silly-kicks 4.0.0.

silly-kicks 4.0.0's symmetric ET guard (``require_et_direction``) raises if a
per-period-absolute converter is called with frames containing ``period_id in
{3, 4}`` but no ``home_team_start_left_extratime``. These derivers source the
flag from bronze metadata (IDSSE: authoritative from DFL XML KickOff; Metrica:
empirical from period-3 SHOT positions) so the lakehouse can pass it through
``convert_to_actions`` / ``convert_to_frames`` per ADR-006.

Both derivers return ``None`` when the match has no ET periods — the
zero-IDSSE/Metrica-ET-matches steady state today (per the §8 audit,
2026-05-30). ``None`` is safe under silly-kicks 4.0's guard because it only
raises when ET periods AND the flag is None coincide.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.spadl_adapter import (
    derive_idsse_home_team_start_left_extratime,
    derive_metrica_home_team_start_left_extratime,
)

_HOME = "DFL-CLU-000008"
_AWAY = "DFL-CLU-00000G"


def _idsse_events(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build an adapted-IDSSE-events DataFrame from minimal kwarg rows."""
    defaults: dict[str, object] = {
        "event_type": "Play",
        "kickoff_game_section": None,
        "kickoff_team_left": None,
        "period_id": 1,
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


# --- IDSSE ---


def test_idsse_returns_none_when_no_et_periods() -> None:
    """No ET in the match -> None (silly-kicks 4.0's guard accepts None when no ET)."""
    events = _idsse_events(
        [
            {"event_type": "KickOff", "kickoff_game_section": "firstHalf", "kickoff_team_left": _HOME, "period_id": 1},
            {"event_type": "Play", "period_id": 1},
            {"event_type": "Play", "period_id": 2},
        ]
    )
    assert derive_idsse_home_team_start_left_extratime(events, _HOME) is None


def test_idsse_returns_true_when_home_starts_left_in_et() -> None:
    events = _idsse_events(
        [
            {"event_type": "KickOff", "kickoff_game_section": "firstHalf", "kickoff_team_left": _HOME, "period_id": 1},
            {
                "event_type": "KickOff",
                "kickoff_game_section": "extraTimeFirstHalf",
                "kickoff_team_left": _HOME,
                "period_id": 3,
            },
            {"event_type": "Play", "period_id": 3},
        ]
    )
    assert derive_idsse_home_team_start_left_extratime(events, _HOME) is True


def test_idsse_returns_false_when_away_starts_left_in_et() -> None:
    events = _idsse_events(
        [
            {
                "event_type": "KickOff",
                "kickoff_game_section": "extraTimeFirstHalf",
                "kickoff_team_left": _AWAY,
                "period_id": 3,
            },
            {"event_type": "Play", "period_id": 3},
        ]
    )
    assert derive_idsse_home_team_start_left_extratime(events, _HOME) is False


def test_idsse_prefers_period_3_kickoff_over_period_4() -> None:
    """If both ET KickOff rows present, period-3 (extraTimeFirstHalf) is authoritative."""
    events = _idsse_events(
        [
            {
                "event_type": "KickOff",
                "kickoff_game_section": "extraTimeFirstHalf",
                "kickoff_team_left": _HOME,
                "period_id": 3,
            },
            {
                "event_type": "KickOff",
                "kickoff_game_section": "extraTimeSecondHalf",
                "kickoff_team_left": _AWAY,
                "period_id": 4,
            },  # would normally flip mid-ET
            {"event_type": "Play", "period_id": 3},
        ]
    )
    assert derive_idsse_home_team_start_left_extratime(events, _HOME) is True


def test_idsse_falls_back_to_period_4_when_period_3_missing() -> None:
    """Defensive: if only period-4 KickOff row exists, use it (better than failing)."""
    events = _idsse_events(
        [
            {
                "event_type": "KickOff",
                "kickoff_game_section": "extraTimeSecondHalf",
                "kickoff_team_left": _HOME,
                "period_id": 4,
            },
            {"event_type": "Play", "period_id": 4},
        ]
    )
    assert derive_idsse_home_team_start_left_extratime(events, _HOME) is True


def test_idsse_raises_when_et_periods_present_but_no_et_kickoff() -> None:
    """ET periods recorded but no KickOff metadata is an ingestion-integrity error."""
    events = _idsse_events(
        [
            {"event_type": "Play", "period_id": 3},  # ET data present
            # No ET KickOff row at all
        ]
    )
    with pytest.raises(RuntimeError, match=r"ET periods.*no ET KickOff"):
        derive_idsse_home_team_start_left_extratime(events, _HOME)


def test_idsse_raises_when_et_kickoff_has_null_team_left() -> None:
    """ET KickOff with null kickoff_team_left is the same integrity error."""
    events = _idsse_events(
        [
            {
                "event_type": "KickOff",
                "kickoff_game_section": "extraTimeFirstHalf",
                "kickoff_team_left": None,
                "period_id": 3,
            },
            {"event_type": "Play", "period_id": 3},
        ]
    )
    with pytest.raises(RuntimeError, match=r"ET periods.*no ET KickOff"):
        derive_idsse_home_team_start_left_extratime(events, _HOME)


def test_idsse_no_period_id_column_treated_as_no_et() -> None:
    """Defensive: if events lack period_id, treat as no-ET (return None safely)."""
    events = pd.DataFrame(
        [
            {"event_type": "KickOff", "kickoff_game_section": "firstHalf", "kickoff_team_left": _HOME},
            {"event_type": "Play"},
        ]
    )
    assert derive_idsse_home_team_start_left_extratime(events, _HOME) is None


# --- Metrica ---


def _metrica_events(rows: list[dict[str, object]]) -> pd.DataFrame:
    defaults: dict[str, object] = {"team": "Home", "period": 1, "start_x": None, "type": "PASS"}
    return pd.DataFrame([{**defaults, **r} for r in rows])


def test_metrica_returns_none_when_no_et_periods() -> None:
    events = _metrica_events([{"team": "Home", "period": 1, "start_x": 60.0, "type": "SHOT"}])
    assert derive_metrica_home_team_start_left_extratime(events) is None


def test_metrica_infers_true_from_period_3_shots_in_right_half() -> None:
    """home shooting right in p3 -> home defends LEFT -> start_left_extratime=True."""
    events = _metrica_events(
        [
            {"team": "Home", "period": 3, "start_x": 75.0, "type": "SHOT"},
            {"team": "Home", "period": 3, "start_x": 80.0, "type": "SHOT"},
        ]
    )
    assert derive_metrica_home_team_start_left_extratime(events) is True


def test_metrica_infers_false_from_period_3_shots_in_left_half() -> None:
    events = _metrica_events(
        [
            {"team": "Home", "period": 3, "start_x": 20.0, "type": "SHOT"},
            {"team": "Home", "period": 3, "start_x": 30.0, "type": "SHOT"},
        ]
    )
    assert derive_metrica_home_team_start_left_extratime(events) is False


def test_metrica_falls_back_to_all_events_when_shots_sparse() -> None:
    """<2 ET shots -> use all period-3 home events with non-null start_x."""
    events = _metrica_events(
        [
            {"team": "Home", "period": 3, "start_x": 70.0, "type": "PASS"},
            {"team": "Home", "period": 3, "start_x": 75.0, "type": "PASS"},
            {"team": "Home", "period": 3, "start_x": 80.0, "type": "PASS"},
            {"team": "Home", "period": 3, "start_x": 85.0, "type": "PASS"},
            {"team": "Home", "period": 3, "start_x": 90.0, "type": "PASS"},
        ]
    )
    assert derive_metrica_home_team_start_left_extratime(events) is True


def test_metrica_raises_when_et_present_but_insufficient_signal() -> None:
    """Insufficient period-3 home data with ET periods present -> RuntimeError."""
    events = _metrica_events(
        [
            {"team": "Away", "period": 3, "start_x": 50.0, "type": "SHOT"},  # away only
            {"team": "Home", "period": 3, "start_x": None, "type": "PASS"},  # home but no x
        ]
    )
    with pytest.raises(RuntimeError, match="insufficient period-3 home-team data"):
        derive_metrica_home_team_start_left_extratime(events)


def test_metrica_no_period_column_treated_as_no_et() -> None:
    events = pd.DataFrame([{"team": "Home", "start_x": 60.0, "type": "SHOT"}])
    assert derive_metrica_home_team_start_left_extratime(events) is None


def test_metrica_pitch_length_kwarg_overrides_midpoint() -> None:
    """A non-standard pitch length shifts the inferred midpoint."""
    events = _metrica_events(
        [
            {"team": "Home", "period": 3, "start_x": 55.0, "type": "SHOT"},
            {"team": "Home", "period": 3, "start_x": 56.0, "type": "SHOT"},
        ]
    )
    # 105 mid -> 52.5: avg(55,56)=55.5 > 52.5 -> True
    assert derive_metrica_home_team_start_left_extratime(events, pitch_length_m=105.0) is True
    # 120 mid -> 60.0: avg(55,56)=55.5 < 60.0 -> False
    assert derive_metrica_home_team_start_left_extratime(events, pitch_length_m=120.0) is False
