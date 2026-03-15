"""Reusable Streamlit filter widgets backed by Lakebase dimension tables."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app.components.feedback import empty_result
from streamlit_app.db import execute_query, t


@st.cache_data(ttl=600, show_spinner="Loading filters...")
def _cached_query(query: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Execute a query with Streamlit caching.

    The TTL matches the default of ``AppSettings.cache_ttl_seconds`` (600 s).
    The decorator is applied once at module import so Streamlit assigns a
    stable cache key; the previous nested ``_run`` pattern re-applied the
    decorator on every call, making every invocation a cache miss (CACHE-01).
    """
    return execute_query(query, params)


def _find_index(options: list[dict[str, Any]], key: str, session_key: str) -> int | None:
    """Find the index of a previously-selected value in options, or None."""
    prev = st.session_state.get(session_key)
    if prev is None:
        return None
    for i, opt in enumerate(options):
        if opt[key] == prev:
            return i
    return None


def render_competition_filter() -> int | None:
    """Render competition selectbox. Returns selected competition_id or None.

    Persists selection in st.session_state for cross-page continuity (F9).
    """
    df = _cached_query(
        f"SELECT competition_id, competition_name, country "  # noqa: S608
        f"FROM {t('dim_competitions_synced')} "
        f"ORDER BY country, competition_name"
    )
    if df.empty:
        empty_result("competitions")
        return None

    options = df.to_dict("records")
    labels = [f"{r['country']} — {r['competition_name']}" for r in options]

    prev_idx = _find_index(options, "competition_id", "_filter_competition_id")
    idx = st.selectbox(
        "Competition",
        range(len(labels)),
        format_func=lambda i: labels[i],
        index=prev_idx,
        placeholder="Select a competition...",
    )
    if idx is None:
        return None
    selected = options[int(idx)]["competition_id"]
    # Reset dependent filters when competition changes
    prev = st.session_state.get("_filter_competition_id")
    if prev is not None and prev != selected:
        st.session_state.pop("_filter_team_id", None)
        st.session_state.pop("_filter_match_id", None)
    st.session_state["_filter_competition_id"] = selected
    return selected  # type: ignore[return-value]


def render_team_filter(competition_id: int | None) -> int | None:
    """Render team selectbox filtered by competition. Returns team_id or None."""
    if competition_id is None:
        return None

    # L-3: Explicit type assertion before query
    competition_id = int(competition_id)
    # UNION instead of OR join to allow index scans on home_team_id and away_team_id
    df = _cached_query(
        f"SELECT DISTINCT t.team_id, t.team_name "  # noqa: S608
        f"FROM {t('dim_teams_synced')} t "
        f"WHERE t.team_id IN ("
        f"  SELECT m.home_team_id FROM {t('fct_match_summary_synced')} m WHERE m.competition_id = %s "
        f"  UNION "
        f"  SELECT m.away_team_id FROM {t('fct_match_summary_synced')} m WHERE m.competition_id = %s"
        f") ORDER BY t.team_name",
        (competition_id, competition_id),
    )
    if df.empty:
        empty_result("teams for this competition")
        return None

    options = df.to_dict("records")
    labels = [r["team_name"] for r in options]

    prev_idx = _find_index(options, "team_id", "_filter_team_id")
    idx = st.selectbox("Team", range(len(labels)), format_func=lambda i: labels[i], index=prev_idx or 0)
    if idx is None:
        return None
    selected = options[idx]["team_id"]
    # Reset match when team changes
    prev = st.session_state.get("_filter_team_id")
    if prev is not None and prev != selected:
        st.session_state.pop("_filter_match_id", None)
    st.session_state["_filter_team_id"] = selected
    return selected  # type: ignore[return-value]


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

    # L-3: Explicit type assertion before query
    competition_id = int(competition_id)
    conditions = ["ps.competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        team_id = int(team_id)
        # Use EXISTS instead of SELECT DISTINCT to avoid full table scans on
        # fct_passes (3M+ rows) and fct_shots. EXISTS stops after first match.
        shots_tbl = t("fct_shots_synced")
        passes_tbl = t("fct_passes_synced")
        conditions.append(
            f"(EXISTS (SELECT 1 FROM {shots_tbl} sh"  # noqa: S608
            f"         WHERE sh.player_id = ps.player_id AND sh.team_id = %s)"
            f" OR EXISTS (SELECT 1 FROM {passes_tbl} pa"
            f"            WHERE pa.player_id = ps.player_id AND pa.team_id = %s))"
        )
        params.extend([team_id, team_id])

    if min_minutes > 0:
        conditions.append("ps.minutes_played >= %s")
        params.append(min_minutes)

    # SECURITY: `where` is built entirely from hardcoded conditions above
    # (never user input). All user-supplied values use %s parameterized placeholders.
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
        empty_result("players matching filters")
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


def render_match_filter(
    competition_id: int | None,
    team_id: int | None,
    allow_all: bool = False,
) -> int | None:
    """Render match selectbox. Returns match_id or None.

    When allow_all=True, prepends an "All matches" option that returns None.
    """
    if competition_id is None:
        return None

    # L-3: Explicit type assertion before query
    competition_id = int(competition_id)
    conditions = ["competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        team_id = int(team_id)
        conditions.append("(home_team_id = %s OR away_team_id = %s)")
        params.extend([team_id, team_id])

    # SECURITY: `where` is built entirely from hardcoded conditions above
    # (never user input). All user-supplied values use %s parameterized placeholders.
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
        empty_result("matches")
        return None

    options = df.to_dict("records")
    labels = [
        f"{r['match_date']} — {r['home_team_name']} {r['home_score']}-{r['away_score']} {r['away_team_name']}"
        for r in options
    ]

    prev_idx = _find_index(options, "match_id", "_filter_match_id")

    if allow_all:
        idx = st.selectbox(
            "Match",
            [None, *range(len(labels))],
            format_func=lambda i: "All matches" if i is None else labels[i],
        )
        if idx is None:
            return None
        st.session_state["_filter_match_id"] = options[idx]["match_id"]
        return options[idx]["match_id"]  # type: ignore[return-value]

    idx = st.selectbox("Match", range(len(labels)), format_func=lambda i: labels[i], index=prev_idx or 0)
    if idx is None:
        return None
    st.session_state["_filter_match_id"] = options[idx]["match_id"]
    return options[idx]["match_id"]  # type: ignore[return-value]


def render_minutes_filter() -> int:
    """Render minimum minutes played slider. Returns the threshold value."""
    return st.slider("Min. Minutes Played", min_value=0, max_value=2000, value=90, step=45)  # type: ignore[return-value]


def render_tracking_match_filter(matches: pd.DataFrame, key: str = "match") -> str | None:
    """Render a match selectbox for tracking-data pages with human-readable labels.

    Joins match IDs with fct_match_summary for "YYYY-MM-DD — Home v Away" labels.
    Falls back to raw match_id if no summary data exists.
    """
    match_ids = matches["match_id"].tolist()
    if not match_ids:
        return None
    placeholders = ", ".join(["%s"] * len(match_ids))
    labels_df = _cached_query(
        f"SELECT match_id::text AS match_id, match_date, home_team_name, away_team_name "  # noqa: S608
        f"FROM {t('fct_match_summary_synced')} "
        f"WHERE match_id::text IN ({placeholders})",
        tuple(str(m) for m in match_ids),
    )
    label_map: dict[str, str] = {}
    if not labels_df.empty:
        label_map = {
            str(r["match_id"]): f"{r['match_date']} — {r['home_team_name']} v {r['away_team_name']}"
            for _, r in labels_df.iterrows()
        }

    idx = st.selectbox(
        "Match",
        range(len(match_ids)),
        format_func=lambda i: label_map.get(str(match_ids[i]), f"Match {match_ids[i]}"),
        key=key,
    )
    return str(match_ids[idx]) if idx is not None else None
