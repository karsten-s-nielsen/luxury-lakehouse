"""Team KPIs queries — the team-match grain mart ``fct_team_metrics`` (sk4118 P1 Phase A).

Read-side surface for the Team KPIs page. Grain: one row per (competition, team, match). The mart
carries BOTH team-match families in one wide row — team_metrics (44 KPIs, event-only) and
match_outcome (5 win-probability cols, per-shot xG integrated). We present the mart BY GRAIN: a wide
team-match table plus a headline summary aggregated across the current scope. Native surrogates are
resolved to human labels via dim_teams_synced / fct_match_summary_synced (no raw ids reach the user).

team_metrics + match_outcome are per-TEAM aggregates, NOT per-player-evaluative — no governance card.
"""

from __future__ import annotations

import pandas as pd

from queries.common import decode_unicode_columns, execute_query, t, ttl_cache

# The 44 team_metrics + 5 match_outcome columns, in mart order, mapped to human-readable labels.
# Presented wide-by-grain (one row per team-match); scale/direction lives in the page caption + glossary.
TEAM_METRIC_LABELS: dict[str, str] = {
    # match_outcome (headline — self-interpretable probabilities)
    "p_win": "Win Prob",
    "p_draw": "Draw Prob",
    "p_loss": "Loss Prob",
    "xpoints": "xPoints",
    "expected_goals": "xG",
    # pressing / recovery
    "ppda": "PPDA",
    "defensive_intensity": "Def. Intensity",
    "time_to_defensive_action_s": "Time to Def. Action (s)",
    "time_to_recovery_s": "Time to Recovery (s)",
    "recoveries": "Recoveries",
    "recoveries_within_ns_pct": "Recoveries <Ns (%)",
    "counterpress_regains": "Counterpress Regains",
    "counterpress_regain_pct": "Counterpress Regain (%)",
    "field_tilt_pct": "Field Tilt (%)",
    "pass_tempo": "Pass Tempo",
    "long_ball_pct": "Long Ball (%)",
    "defensive_action_height_m": "Def. Action Height (m)",
    "recovery_line_height_m": "Recovery Line Height (m)",
    "turnover_line_height_m": "Turnover Line Height (m)",
    # progression
    "poss_to_final_third_pct": "Poss. to Final Third (%)",
    "final_third_entries": "Final Third Entries",
    "final_third_to_box_pct": "Final Third to Box (%)",
    "box_touches": "Box Touches",
    "box_to_shot_pct": "Box to Shot (%)",
    "shots": "Shots",
    "high_opportunity_shots": "High-Opp. Shots",
    # breakout direction
    "breakout_left": "Breakout Left",
    "breakout_center": "Breakout Center",
    "breakout_right": "Breakout Right",
    "breakout_left_pct": "Breakout Left (%)",
    "breakout_center_pct": "Breakout Center (%)",
    "breakout_right_pct": "Breakout Right (%)",
    # buildup
    "possessions_retained_after_ns_pct": "Poss. Retained <Ns (%)",
    "buildup_final_quarter": "Buildup Final Quarter",
    "buildup_next_phase": "Buildup Next Phase",
    "buildup_opp_int_own_half": "Buildup Opp. Int. Own Half",
    "buildup_stayed_phase_one": "Buildup Stayed Phase 1",
    "buildup_opp_won_own_half": "Buildup Opp. Won Own Half",
    "buildup_led_opp_shot": "Buildup Led Opp. Shot",
    "buildup_success_pct": "Buildup Success (%)",
    # post-regain
    "post_regain_second_pass_pct": "Post-Regain 2nd Pass (%)",
    "post_regain_failed_first_passes": "Post-Regain Failed 1st Pass",
    "post_regain_forward_first_pct": "Post-Regain Fwd 1st (%)",
    "switch_press_success_pct": "Switch-Press Success (%)",
    "switch_press_n": "Switch-Press N",
    "final_third_entries_post_recovery": "Final Third Entries (Post-Rec.)",
    "box_touches_post_recovery": "Box Touches (Post-Rec.)",
    "shots_post_recovery": "Shots (Post-Rec.)",
    "high_opportunity_shots_post_recovery": "High-Opp. Shots (Post-Rec.)",
}

_METRIC_COLS: tuple[str, ...] = tuple(TEAM_METRIC_LABELS)
_METRIC_SELECT = ", ".join(f"tm.{c}" for c in _METRIC_COLS)


def build_team_metrics_rows_sql(competition_key: int, team_id: int | None, match_key: int | None) -> tuple[str, tuple]:
    """Wide team-match rows for the current scope, ordered most-recent first.

    Filters on competition_key (always), the dim_teams legacy team_id (optional), and match_key
    (optional). Joins fct_match_summary_synced for a readable "date — Home v Away" match label and
    dim_teams_synced for the team name. Bounded at 500 rows (ranking/leaderboard budget)."""
    params: list[object] = [competition_key]
    where = ["tm.competition_key = %s"]
    if team_id is not None:
        where.append("dt.team_id = %s")
        params.append(team_id)
    if match_key is not None:
        where.append("tm.match_key = %s")
        params.append(match_key)
    sql = (
        f"SELECT dt.team_name, "  # noqa: S608
        f"       ms.match_date, ms.home_team_name, ms.away_team_name, "
        f"       {_METRIC_SELECT} "
        f"FROM {t('fct_team_metrics_synced')} tm "
        f"JOIN {t('dim_teams_synced')} dt ON dt.team_key = tm.team_key "
        f"LEFT JOIN {t('fct_match_summary_synced')} ms ON ms.match_key = tm.match_key "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY ms.match_date DESC NULLS LAST, dt.team_name "
        f"LIMIT 500"
    )
    return sql, tuple(params)


def build_team_metrics_summary_sql(
    competition_key: int, team_id: int | None, match_key: int | None
) -> tuple[str, tuple]:
    """Scope-level headline aggregates (means over the team-match rows) for the StatCards."""
    params: list[object] = [competition_key]
    where = ["tm.competition_key = %s"]
    if team_id is not None:
        where.append("dt.team_id = %s")
        params.append(team_id)
    if match_key is not None:
        where.append("tm.match_key = %s")
        params.append(match_key)
    sql = (
        f"SELECT COUNT(*) AS n_rows, "  # noqa: S608
        f"       AVG(tm.p_win) AS mean_p_win, "
        f"       AVG(tm.xpoints) AS mean_xpoints, "
        f"       AVG(tm.expected_goals) AS mean_xg, "
        f"       AVG(tm.field_tilt_pct) AS mean_field_tilt, "
        f"       AVG(tm.ppda) AS mean_ppda "
        f"FROM {t('fct_team_metrics_synced')} tm "
        f"JOIN {t('dim_teams_synced')} dt ON dt.team_key = tm.team_key "
        f"WHERE {' AND '.join(where)}"
    )
    return sql, tuple(params)


@ttl_cache()
def fetch_team_metrics_rows(competition_key: int, team_id: int | None, match_key: int | None) -> pd.DataFrame:
    sql, params = build_team_metrics_rows_sql(competition_key, team_id, match_key)
    return decode_unicode_columns(execute_query(sql, params))


@ttl_cache()
def fetch_team_metrics_summary(competition_key: int, team_id: int | None, match_key: int | None) -> pd.DataFrame:
    sql, params = build_team_metrics_summary_sql(competition_key, team_id, match_key)
    return execute_query(sql, params)
