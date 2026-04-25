"""fct_match_summary must populate Wyscout home/away team_ids via new bridge (PR 5a)."""

from pathlib import Path


def test_joins_wyscout_home_away_bridge() -> None:
    src = Path("dbt_project/models/marts/fct_match_summary.sql").read_text()
    assert "stg_wyscout__home_away_teams" in src, "fct_match_summary must consume the new Wyscout home/away bridge"
    # Both home + away JOINs should appear
    assert src.count("stg_wyscout__home_away_teams") >= 2
