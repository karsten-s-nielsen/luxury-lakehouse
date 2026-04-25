"""fct_funnel_stages_agg adds match_key + team_key + opponent_team_key (PR 5a)."""

from pathlib import Path

MODEL = Path("dbt_project/models/marts/fct_funnel_stages_agg.sql")


def test_has_match_team_opponent_keys_in_select() -> None:
    src = MODEL.read_text()
    assert "match_key" in src
    assert "team_key" in src
    assert "opponent_team_key" in src


def test_joins_dim_teams_twice() -> None:
    src = MODEL.read_text()
    assert src.count("dim_teams") >= 2


def test_carries_data_source_through_aggregates() -> None:
    src = MODEL.read_text()
    assert "av.data_source" in src
    assert "g.data_source" in src


def test_join_uses_cast_native_team_id() -> None:
    src = MODEL.read_text()
    assert "native_team_id = cast(g.team_id as string)" in src
    assert "native_team_id = cast(g.opponent_team_id as string)" in src
