"""Conversion rate funnel queries and aggregation logic."""

from __future__ import annotations

import pandas as pd

_A3_THRESHOLD = 70.0
_SHOT_TYPES = ("shot", "shot_penalty", "shot_freekick")


def compute_funnel_stages(df: pd.DataFrame, team_id: int) -> dict[str, int]:
    """Compute funnel stage counts from actions during a team's own possessions.

    Scopes to ``possession_team_id == team_id`` when available.  Wyscout data
    lacks possession metadata (``possession_team_id`` is NULL for ~36% of rows),
    so NULL possession rows fall back to ``team_id`` — if the team performed
    the action, it counts as their possession.

    Possession counting uses ``(match_id, possession_id)`` pairs to avoid
    cross-match collisions (StatsBomb restarts numbering per match).  Wyscout
    rows (NULL ``possession_id``) are counted as one synthetic possession per
    match to avoid zero-possession artifacts.
    """
    team_actions = df[df["team_id"] == team_id]

    # Scope to own-possession: use possession_team_id where available,
    # fall back to team_id for NULL (Wyscout data)
    poss_col = team_actions["possession_team_id"]
    own_poss = team_actions[poss_col.isna() | (poss_col == team_id)]

    # Possession count: StatsBomb rows have (match_id, possession_id),
    # Wyscout rows have NULL possession_id — count those matches as 1 each
    has_poss = own_poss[own_poss["possession_id"].notna()]
    null_poss = own_poss[own_poss["possession_id"].isna()]

    sb_possessions = (
        int(has_poss[["match_id", "possession_id"]].drop_duplicates().shape[0]) if not has_poss.empty else 0
    )
    wy_matches = int(null_poss["match_id"].nunique()) if not null_poss.empty else 0
    possessions = sb_possessions + wy_matches

    a3_mask = (own_poss["start_x"] <= _A3_THRESHOLD) & (own_poss["end_x"] > _A3_THRESHOLD)
    a3_entries = int(a3_mask.sum())

    shot_mask = own_poss["action_type"].isin(_SHOT_TYPES)
    shots = int(shot_mask.sum())

    goal_mask = shot_mask & (own_poss["action_result"] == "success")
    goals = int(goal_mask.sum())

    return {
        "possessions": possessions,
        "a3_entries": a3_entries,
        "shots": shots,
        "goals": goals,
    }


def compute_conversion_rates(stages: dict[str, int]) -> dict[str, float]:
    """Compute step-wise and end-to-end conversion rates (percentages 0-100)."""

    def _pct(num: int, den: int) -> float:
        return round(num / den * 100, 1) if den > 0 else 0.0

    return {
        "poss_to_a3": _pct(stages["a3_entries"], stages["possessions"]),
        "a3_to_shot": _pct(stages["shots"], stages["a3_entries"]),
        "shot_to_goal": _pct(stages["goals"], stages["shots"]),
        "end_to_end": _pct(stages["goals"], stages["possessions"]),
    }


def _fetch_match_meta(
    comp_id: int,
    team_id: int,
    match_id: int | None,
) -> pd.DataFrame:
    """Fetch match metadata (IDs + names) from the small dimension table."""
    from queries.common import execute_query, t

    ms_tbl = t("fct_match_summary_synced")
    where = ["competition_id = %s"]
    params: list[int | str] = [int(comp_id)]
    where.append("(home_team_id = %s OR away_team_id = %s)")
    params.extend([int(team_id), int(team_id)])
    if match_id is not None:
        where.append("match_id = %s")
        params.append(int(match_id))
    return execute_query(
        f"SELECT match_id, home_team_id, away_team_id,"  # noqa: S608
        f" home_team_name, away_team_name"
        f" FROM {ms_tbl}"
        f" WHERE {' AND '.join(where)}"
        f" LIMIT 200",
        tuple(params),
    )


def fetch_funnel_actions(
    comp_id: int,
    team_id: int,
    match_id: int | None = None,
    game_state: str | None = None,
) -> pd.DataFrame:
    """Fetch action data for funnel computation from Lakebase.

    Single-match mode: ``WHERE match_id = %s`` — direct index seek (~33ms).

    Season mode: ``WHERE competition_id = %s AND match_id IN (SELECT ...)``
    — nested loop from the small match_summary table into index seeks on
    fct_action_values_synced (~2s for 232K rows, verified via EXPLAIN ANALYZE).

    Returns both teams' actions so ``compute_funnel_stages`` can build
    the mirror chart.
    """
    from queries.common import execute_query, t

    meta = _fetch_match_meta(comp_id, team_id, match_id)
    if meta.empty:
        return pd.DataFrame()

    tbl = t("fct_action_values_synced")
    ms_tbl = t("fct_match_summary_synced")
    cols = "match_id, team_id, possession_id, possession_team_id, start_x, end_x, action_type, action_result"

    if match_id is not None:
        # Single match — direct index seek
        where = ["match_id = %s"]
        params: list[object] = [int(match_id)]
    else:
        # Season — IN-subquery drives nested loop from small table
        where = [
            "competition_id = %s",
            f"match_id IN (SELECT match_id FROM {ms_tbl}"  # noqa: S608
            f" WHERE competition_id = %s AND (home_team_id = %s OR away_team_id = %s))",
        ]
        params = [int(comp_id), int(comp_id), int(team_id), int(team_id)]

    if game_state and game_state != "All":
        where.append("game_state = %s")
        params.append(game_state.lower())

    actions = execute_query(
        f"SELECT {cols}"  # noqa: S608
        f" FROM {tbl}"
        f" WHERE {' AND '.join(where)}"
        f" LIMIT 500000",
        tuple(params),
    )
    if actions.empty:
        return actions

    return actions.merge(
        meta[["match_id", "home_team_id", "away_team_id", "home_team_name", "away_team_name"]],
        on="match_id",
        how="left",
    )
