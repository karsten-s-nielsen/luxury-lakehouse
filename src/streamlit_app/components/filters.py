"""Reusable Streamlit filter widgets backed by Lakebase dimension tables."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _cached_query(query: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Execute a query with Streamlit caching."""

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _run(q: str, p: tuple[Any, ...] | None) -> pd.DataFrame:
        return execute_query(q, p)

    return _run(query, params)


def render_competition_filter() -> int | None:
    """Render competition selectbox. Returns selected competition_id or None."""
    df = _cached_query(
        f"SELECT competition_id, competition_name, country "  # noqa: S608
        f"FROM {t('dim_competitions_synced')} "
        f"ORDER BY country, competition_name"
    )
    if df.empty:
        st.warning("No competitions found.")
        return None

    options = df.to_dict("records")
    labels = [f"{r['country']} — {r['competition_name']}" for r in options]

    idx = st.selectbox("Competition", range(len(labels)), format_func=lambda i: labels[i])
    if idx is None:
        return None
    return options[idx]["competition_id"]  # type: ignore[return-value]


def render_team_filter(competition_id: int | None) -> int | None:
    """Render team selectbox filtered by competition. Returns team_id or None."""
    if competition_id is None:
        return None

    df = _cached_query(
        f"SELECT DISTINCT t.team_id, t.team_name "  # noqa: S608
        f"FROM {t('dim_teams_synced')} t "
        f"JOIN {t('fct_match_summary_synced')} m "
        f"  ON t.team_id = m.home_team_id OR t.team_id = m.away_team_id "
        f"WHERE m.competition_id = %s "
        f"ORDER BY t.team_name",
        (competition_id,),
    )
    if df.empty:
        st.info("No teams found for this competition.")
        return None

    options = df.to_dict("records")
    labels = [r["team_name"] for r in options]

    idx = st.selectbox("Team", range(len(labels)), format_func=lambda i: labels[i])
    if idx is None:
        return None
    return options[idx]["team_id"]  # type: ignore[return-value]


def render_player_filter(
    competition_id: int | None,
    team_id: int | None,
    min_minutes: int = 0,
    multiselect: bool = False,
) -> list[int] | int | None:
    """Render player filter. Returns player_id(s) or None.

    When multiselect=True, returns a list of player_ids (may be empty).
    When multiselect=False, returns a single player_id or None.
    """
    if competition_id is None:
        return [] if multiselect else None

    conditions = ["ps.competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        shots_tbl = t("fct_shots_synced")
        passes_tbl = t("fct_passes_synced")
        conditions.append(
            f"ps.player_id IN ("  # noqa: S608
            f"  SELECT DISTINCT player_id FROM {shots_tbl} WHERE team_id = %s"
            f"  UNION"
            f"  SELECT DISTINCT player_id FROM {passes_tbl} WHERE team_id = %s"
            f")"
        )
        params.extend([team_id, team_id])

    if min_minutes > 0:
        conditions.append("ps.minutes_played >= %s")
        params.append(min_minutes)

    where = " AND ".join(conditions)
    df = _cached_query(
        f"SELECT DISTINCT p.player_id, p.player_display_name "  # noqa: S608
        f"FROM {t('dim_players_synced')} p "
        f"JOIN {t('fct_player_stats_synced')} ps ON p.player_id = ps.player_id "
        f"WHERE {where} "
        f"ORDER BY p.player_display_name",
        tuple(params),
    )
    if df.empty:
        st.info("No players found matching filters.")
        return [] if multiselect else None

    options = df.to_dict("records")
    labels = [r["player_display_name"] for r in options]

    if multiselect:
        selected_indices: list[int] = st.multiselect(
            "Players (1-3)", range(len(labels)), format_func=lambda i: labels[i], max_selections=3
        )
        return [options[i]["player_id"] for i in selected_indices]

    idx = st.selectbox(
        "Player",
        [None, *range(len(labels))],
        format_func=lambda i: "All" if i is None else labels[i],
    )
    if idx is None:
        return None
    return options[idx]["player_id"]  # type: ignore[return-value]


def render_match_filter(competition_id: int | None, team_id: int | None) -> int | None:
    """Render match selectbox. Returns match_id or None."""
    if competition_id is None:
        return None

    conditions = ["competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        conditions.append("(home_team_id = %s OR away_team_id = %s)")
        params.extend([team_id, team_id])

    where = " AND ".join(conditions)
    df = _cached_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_score, away_score "
        f"FROM {t('fct_match_summary_synced')} "
        f"WHERE {where} "
        f"ORDER BY match_date DESC",
        tuple(params),
    )
    if df.empty:
        st.info("No matches found.")
        return None

    options = df.to_dict("records")
    labels = [
        f"{r['match_date']} — {r['home_team_name']} {r['home_score']}-{r['away_score']} {r['away_team_name']}"
        for r in options
    ]

    idx = st.selectbox("Match", range(len(labels)), format_func=lambda i: labels[i])
    if idx is None:
        return None
    return options[idx]["match_id"]  # type: ignore[return-value]


def render_minutes_filter() -> int:
    """Render minimum minutes played slider. Returns the threshold value."""
    return st.slider("Min. Minutes Played", min_value=0, max_value=2000, value=90, step=45)  # type: ignore[return-value]
