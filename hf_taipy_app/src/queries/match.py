"""Match summary queries — extracted from state/match_summary.py.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

import pandas as pd

from queries.common import execute_query, t, ttl_cache


@ttl_cache()
def fetch_match_summary(match_key: int) -> pd.DataFrame:
    """Fetch full match summary row for a single match.

    Post-PR 2 (ADR-011): fct_match_summary_synced is keyed on match_key
    (Kimball surrogate BIGINT FK to dim_matches).

    Expected columns: match_key, match_date, home_team_name, away_team_name,
    home_team_id, away_team_id, home_score, away_score, home_xg, away_xg,
    home_shots, away_shots, home_shots_on_target, away_shots_on_target,
    home_total_passes, away_total_passes, home_completed_passes,
    away_completed_passes, home_progressive_passes, away_progressive_passes,
    home_pass_completion_pct, away_pass_completion_pct,
    home_possession_pct, home_ppda, away_ppda.
    """
    return execute_query(
        f"SELECT match_key, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_team_id, away_team_id, "
        f"  home_score, away_score, home_xg, away_xg, "
        f"  home_shots, away_shots, home_shots_on_target, away_shots_on_target, "
        f"  home_total_passes, away_total_passes, "
        f"  home_completed_passes, away_completed_passes, "
        f"  home_progressive_passes, away_progressive_passes, "
        f"  home_pass_completion_pct, away_pass_completion_pct, "
        f"  home_possession_pct, home_ppda, away_ppda "
        f"FROM {t('fct_match_summary_synced')} WHERE match_key = %s",
        (int(match_key),),
    )


@ttl_cache(ttl=600)
def fetch_league_averages(comp_id: int) -> pd.DataFrame:
    """Fetch competition-wide averages for reference context.

    Returns averages for xG per team, possession, and pass completion.

    Expected columns: avg_xg_per_team, avg_possession, avg_pass_completion.
    """
    tbl = t("fct_match_summary_synced")
    # Post-PR 2 (ADR-011): filters on competition_key (Kimball surrogate).
    return execute_query(
        f"SELECT AVG(home_xg + away_xg) / 2 as avg_xg_per_team, "  # noqa: S608
        f"  AVG(home_possession_pct) as avg_possession, "
        f"  AVG((home_pass_completion_pct + away_pass_completion_pct) / 2) as avg_pass_completion "
        f"FROM {tbl} WHERE competition_key = %s",
        (comp_id,),
    )


@ttl_cache()
def fetch_vaep_decisive_actions(match_id: int, n: int = 3) -> pd.DataFrame:
    """Top-N on-ball actions for a match ranked by absolute VAEP value.

    Used by Match Summary Row 1 (Big Story + secondary cards). Per spec §5.3 and
    §7.4. JOINs dim_players_synced + dim_teams_synced so the caller gets prose-ready
    player/team names (Approach A from the plan).

    Expected columns: match_id, minute, second, period, player_id, team_id,
    player_name, team_name, action_type, action_result, vaep_value,
    offensive_value, defensive_value, start_x, start_y, end_x, end_y.
    """
    if not isinstance(match_id, int):
        raise TypeError(f"match_id must be int, got {type(match_id).__name__}")
    if not isinstance(n, int) or n <= 0 or n > 20:
        raise ValueError(f"n must be int in 1..20, got {n!r}")

    av = t("fct_action_values_synced")
    dp = t("dim_players_synced")
    dt = t("dim_teams_synced")
    return execute_query(
        f"SELECT av.match_id, av.minute, av.second, av.period, "  # noqa: S608
        f"  av.player_id, av.team_id, "
        f"  p.player_display_name AS player_name, "
        f"  tm.team_name AS team_name, "
        f"  av.action_type, av.action_result, "
        f"  av.vaep_value, av.offensive_value, av.defensive_value, "
        f"  av.start_x, av.start_y, av.end_x, av.end_y "
        f"FROM {av} av "
        f"LEFT JOIN {dp} p ON av.player_id = p.player_id "
        f"LEFT JOIN {dt} tm ON av.team_id = tm.team_id "
        f"WHERE av.match_id = %s "
        f"ORDER BY ABS(av.vaep_value) DESC "
        f"LIMIT %s",
        (int(match_id), int(n)),
    )


@ttl_cache()
def fetch_shots_timeline(match_id: int) -> pd.DataFrame:
    """All shots for a match ordered chronologically.

    Used by Match Summary Row 2 (xG race chart). Returns per-shot ``xg``
    (aliased from ``statsbomb_xg`` for the render module's convention),
    ``is_goal`` flag, and the team display name for per-trace coloring.

    Expected columns: match_id, minute, second, period, team_id, team_name,
    xg, is_goal, player_id, player_name.
    """
    if not isinstance(match_id, int):
        raise TypeError(f"match_id must be int, got {type(match_id).__name__}")

    fs = t("fct_shots_synced")
    dp = t("dim_players_synced")
    dt = t("dim_teams_synced")
    return execute_query(
        f"SELECT s.match_id, s.minute, s.second, s.period, "  # noqa: S608
        f"  s.team_id, s.player_id, "
        f"  s.statsbomb_xg AS xg, s.is_goal, "
        f"  p.player_display_name AS player_name, "
        f"  tm.team_name AS team_name "
        f"FROM {fs} s "
        f"LEFT JOIN {dp} p ON s.player_id = p.player_id "
        f"LEFT JOIN {dt} tm ON s.team_id = tm.team_id "
        f"WHERE s.match_id = %s "
        f"ORDER BY s.period, s.minute, s.second "
        f"LIMIT 200",
        (int(match_id),),
    )


@ttl_cache()
def fetch_discipline_events(match_id: int) -> pd.DataFrame:
    """Red cards + second yellows for a match, ordered chronologically.

    Used by Match Summary Row 1 (auto-include) + Row 2 (red-card markers on
    xG race). Yellow-card-only events are filtered out here — the mart stores
    them for other use cases but the Match Summary only surfaces match-changing
    cards.

    Expected columns: match_id, minute, second, period, player_id, team_id,
    player_name, team_name, card_name.
    """
    if not isinstance(match_id, int):
        raise TypeError(f"match_id must be int, got {type(match_id).__name__}")

    de = t("fct_discipline_events_synced")
    dp = t("dim_players_synced")
    dt = t("dim_teams_synced")
    return execute_query(
        f"SELECT d.match_id, d.minute, d.second, d.period, "  # noqa: S608
        f"  d.player_id, d.team_id, d.card_name, "
        f"  p.player_display_name AS player_name, "
        f"  tm.team_name AS team_name "
        f"FROM {de} d "
        f"LEFT JOIN {dp} p ON d.player_id = p.player_id "
        f"LEFT JOIN {dt} tm ON d.team_id = tm.team_id "
        f"WHERE d.match_id = %s "
        f"  AND d.card_name IN ('Red Card', 'Second Yellow') "
        f"ORDER BY d.period, d.minute, d.second "
        f"LIMIT 10",
        (int(match_id),),
    )
