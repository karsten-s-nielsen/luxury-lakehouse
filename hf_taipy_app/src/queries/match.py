"""Match summary queries — extracted from state/match_summary.py.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import pandas as pd

from queries.common import execute_query, t, ttl_cache


@ttl_cache()
def fetch_match_summary(match_id: int) -> pd.DataFrame:
    """Fetch full match summary row for a single match.

    Expected columns: match_id, match_date, home_team_name, away_team_name,
    home_score, away_score, home_xg, away_xg, home_shots, away_shots,
    home_shots_on_target, away_shots_on_target, home_total_passes,
    away_total_passes, home_completed_passes, away_completed_passes,
    home_progressive_passes, away_progressive_passes,
    home_pass_completion_pct, away_pass_completion_pct,
    home_possession_pct, home_ppda, away_ppda.
    """
    return execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_score, away_score, home_xg, away_xg, "
        f"  home_shots, away_shots, home_shots_on_target, away_shots_on_target, "
        f"  home_total_passes, away_total_passes, "
        f"  home_completed_passes, away_completed_passes, "
        f"  home_progressive_passes, away_progressive_passes, "
        f"  home_pass_completion_pct, away_pass_completion_pct, "
        f"  home_possession_pct, home_ppda, away_ppda "
        f"FROM {t('fct_match_summary_synced')} WHERE match_id = %s",
        (int(match_id),),
    )


@ttl_cache(ttl=600)
def fetch_league_averages(comp_id: int) -> pd.DataFrame:
    """Fetch competition-wide averages for reference context.

    Returns averages for xG per team, possession, and pass completion.

    Expected columns: avg_xg_per_team, avg_possession, avg_pass_completion.
    """
    tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT AVG(home_xg + away_xg) / 2 as avg_xg_per_team, "  # noqa: S608
        f"  AVG(home_possession_pct) as avg_possession, "
        f"  AVG((home_pass_completion_pct + away_pass_completion_pct) / 2) as avg_pass_completion "
        f"FROM {tbl} WHERE competition_id = %s",
        (comp_id,),
    )
