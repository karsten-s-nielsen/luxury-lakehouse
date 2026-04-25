"""fct_player_stats adds player_key + team_key + data_source grain (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/marts/fct_player_stats.sql")


def test_has_player_key_in_select() -> None:
    src = MODEL.read_text()
    assert "player_key" in src


def test_has_team_key_in_select() -> None:
    src = MODEL.read_text()
    assert "team_key" in src


def test_data_source_in_group_by() -> None:
    src = MODEL.read_text()
    # At least one of the agg CTEs should group by data_source
    assert "group by player_id, competition_id, season_id, data_source" in src


def test_inner_joins_dim_players() -> None:
    src = MODEL.read_text()
    assert "inner join {{ ref('dim_players') }}" in src
    assert "dp.provider = b.data_source" in src
    assert "dp.native_player_id = cast(b.player_id as string)" in src


def test_surrogate_key_includes_data_source() -> None:
    src = MODEL.read_text()
    assert "coalesce(b.data_source" in src
