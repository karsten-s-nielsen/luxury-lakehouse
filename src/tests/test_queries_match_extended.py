"""Tests for the three new match-level query functions added in the Match Summary redesign.

Unit-level: mocks ``execute_query`` at module scope and asserts the SQL shape +
parameter handling. Integration tests against real Lakebase run in staging E2E.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "hf_taipy_app" / "src"))


def _make_vaep_fake() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "match_id": [1, 1, 1],
            "minute": [23, 67, 84],
            "second": [12, 5, 40],
            "period": [1, 2, 2],
            "player_id": [100, 200, 300],
            "team_id": [10, 20, 10],
            "player_name": ["Palmer", "Saka", "Sánchez"],
            "team_name": ["Chelsea", "Arsenal", "Chelsea"],
            "action_type": ["shot", "shot", "keeper_save"],
            "action_result": ["success", "fail", "success"],
            "vaep_value": [0.35, 0.18, -0.04],
            "offensive_value": [0.0, 0.18, 0.0],
            "defensive_value": [0.35, 0.0, -0.04],
            "start_x": [80, 85, 5],
            "start_y": [30, 34, 34],
            "end_x": [99, 90, 30],
            "end_y": [34, 30, 34],
        }
    )


@patch("queries.match.t", side_effect=lambda name: f"public.{name}")
@patch("queries.match.execute_query")
def test_fetch_vaep_decisive_actions_issues_correct_sql(mock_exec, mock_t) -> None:
    from queries.match import fetch_vaep_decisive_actions

    mock_exec.return_value = _make_vaep_fake()
    df = fetch_vaep_decisive_actions.__wrapped__(match_id=1, n=3)
    assert len(df) == 3
    args, _ = mock_exec.call_args
    sql, params = args
    # Match predicate uses the action-values synced table and LIMIT forces row cap
    assert "fct_action_values_synced" in sql
    assert "ORDER BY ABS(av.vaep_value) DESC" in sql
    assert "LIMIT %s" in sql
    assert params == (1, 3)
    # Joins for prose-ready names
    assert "dim_players_synced" in sql
    assert "dim_teams_synced" in sql


@patch("queries.match.execute_query")
def test_fetch_vaep_decisive_actions_validates_match_id_type(mock_exec) -> None:
    from queries.match import fetch_vaep_decisive_actions

    with pytest.raises(TypeError):
        fetch_vaep_decisive_actions.__wrapped__(match_id=None)  # type: ignore[arg-type]
    mock_exec.assert_not_called()


@patch("queries.match.execute_query")
def test_fetch_vaep_decisive_actions_validates_n_bounds(mock_exec) -> None:
    from queries.match import fetch_vaep_decisive_actions

    with pytest.raises(ValueError, match="n must be int"):
        fetch_vaep_decisive_actions.__wrapped__(match_id=1, n=0)
    with pytest.raises(ValueError, match="n must be int"):
        fetch_vaep_decisive_actions.__wrapped__(match_id=1, n=21)
    mock_exec.assert_not_called()


@patch("queries.match.t", side_effect=lambda name: f"public.{name}")
@patch("queries.match.execute_query")
def test_fetch_shots_timeline_uses_statsbomb_xg_alias(mock_exec, mock_t) -> None:
    from queries.match import fetch_shots_timeline

    mock_exec.return_value = pd.DataFrame(
        {
            "match_id": [1, 1],
            "minute": [5, 23],
            "second": [10, 12],
            "period": [1, 1],
            "team_id": [10, 10],
            "xg": [0.03, 0.12],
            "is_goal": [False, True],
            "player_name": ["Palmer", "Palmer"],
            "team_name": ["Chelsea", "Chelsea"],
        }
    )
    df = fetch_shots_timeline.__wrapped__(match_id=1)
    assert "xg" in df.columns
    args, _ = mock_exec.call_args
    sql, params = args
    assert "statsbomb_xg AS xg" in sql
    assert "fct_shots_synced" in sql
    assert "ORDER BY s.period, s.minute, s.second" in sql
    assert params == (1,)


@patch("queries.match.execute_query")
def test_fetch_shots_timeline_validates_type(mock_exec) -> None:
    from queries.match import fetch_shots_timeline

    with pytest.raises(TypeError):
        fetch_shots_timeline.__wrapped__(match_id="bad")  # type: ignore[arg-type]
    mock_exec.assert_not_called()


@patch("queries.match.t", side_effect=lambda name: f"public.{name}")
@patch("queries.match.execute_query")
def test_fetch_discipline_events_filters_red_and_second_yellow_only(mock_exec, mock_t) -> None:
    from queries.match import fetch_discipline_events

    mock_exec.return_value = pd.DataFrame(
        {
            "match_id": [1, 1],
            "minute": [58, 77],
            "second": [20, 10],
            "period": [2, 2],
            "player_id": [999, 888],
            "team_id": [20, 10],
            "card_name": ["Red Card", "Second Yellow"],
            "player_name": ["Saliba", "Mudryk"],
            "team_name": ["Arsenal", "Chelsea"],
        }
    )
    df = fetch_discipline_events.__wrapped__(match_id=1)
    assert set(df["card_name"].tolist()) == {"Red Card", "Second Yellow"}
    args, _ = mock_exec.call_args
    sql, params = args
    # Yellow cards excluded by the query — only Red + Second Yellow surface.
    assert "'Red Card'" in sql and "'Second Yellow'" in sql
    assert "'Yellow Card'" not in sql
    assert "fct_discipline_events_synced" in sql
    assert params == (1,)


@patch("queries.match.execute_query")
def test_fetch_discipline_events_validates_type(mock_exec) -> None:
    from queries.match import fetch_discipline_events

    with pytest.raises(TypeError):
        fetch_discipline_events.__wrapped__(match_id=1.5)  # type: ignore[arg-type]
    mock_exec.assert_not_called()
