"""Shot-related queries — extracted from state/shot_map.py.

All functions return pd.DataFrame. SQL uses %s parameterized placeholders.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from queries.common import execute_query, t, ttl_cache


@ttl_cache()
def fetch_shots(
    competition_id: int,
    team_id: int | None,
    player_id: int | None,
) -> pd.DataFrame:
    """Fetch shots with player names, filtered by competition/team/player.

    Expected columns: shot_id, location_x, location_y, statsbomb_xg, is_goal,
    shot_outcome, shot_body_part, distance_to_goal, shot_angle, minute,
    player_display_name.
    """
    conditions = ["s.competition_id = %s"]
    params: list[Any] = [int(competition_id)]

    if team_id is not None:
        conditions.append("s.team_id = %s")
        params.append(int(team_id))
    if player_id is not None:
        conditions.append("s.player_id = %s")
        params.append(int(player_id))

    where = " AND ".join(conditions)

    return execute_query(
        f"SELECT s.shot_id, s.location_x, s.location_y, s.statsbomb_xg, s.is_goal, "  # noqa: S608
        f"  s.shot_outcome, s.shot_body_part, s.distance_to_goal, s.shot_angle, "
        f"  s.minute, p.player_display_name "
        f"FROM {t('fct_shots_synced')} s "
        f"JOIN {t('dim_players_synced')} p ON s.player_id = p.player_id "
        f"WHERE {where} "
        f"ORDER BY s.minute, s.second "
        f"LIMIT 10000",
        tuple(params),
    )


@ttl_cache()
def fetch_xg_predictions(competition_id: int) -> pd.DataFrame:
    """Fetch custom xG predictions. Returns empty DataFrame if table unavailable.

    Expected columns: shot_id, xg_logistic, xg_gradient_boosted.
    """
    try:
        return execute_query(
            f"SELECT shot_id, xg_logistic, xg_gradient_boosted "  # noqa: S608
            f"FROM {t('fct_xg_predictions_synced')} "
            f"WHERE competition_id = %s "
            f"LIMIT 100000",
            (competition_id,),
        )
    except RuntimeError:
        return pd.DataFrame()
