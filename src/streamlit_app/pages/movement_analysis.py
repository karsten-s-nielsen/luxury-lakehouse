"""Movement Analysis page — physical performance, PPDA, and Off-Ball xT."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_physical_bars, plot_ppda_bars
from streamlit_app.components.feedback import (
    data_freshness,
    data_scope_note,
    empty_result,
    empty_select,
    render_scope_label,
)
from streamlit_app.components.filters import render_competition_filter, render_tracking_match_filter
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.db import execute_query, t

_PROVIDER_OPTIONS = ["All", "metrica", "idsse", "skillcorner"]


def _render_match_selectbox(matches: Any, key: str = "match") -> str | None:
    """Render a tracking match selectbox — delegates to shared filter component."""
    return render_tracking_match_filter(matches, key=key)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=600, show_spinner="Loading matches...")
def _fetch_physical_stats_matches(tbl: str, prov: str | None) -> Any:
    if prov:
        return execute_query(
            f"WITH RECURSIVE dm AS ("  # noqa: S608
            f"  SELECT MIN(match_id) AS match_id FROM {tbl} WHERE source_provider = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(match_id) FROM {tbl}"
            f"          WHERE source_provider = %s AND match_id > dm.match_id)"
            f"  FROM dm WHERE dm.match_id IS NOT NULL"
            f") SELECT match_id FROM dm WHERE match_id IS NOT NULL ORDER BY match_id",
            (prov, prov),
        )
    return execute_query(
        f"WITH RECURSIVE dm AS ("  # noqa: S608
        f"  SELECT MIN(match_id) AS match_id FROM {tbl}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(match_id) FROM {tbl} WHERE match_id > dm.match_id)"
        f"  FROM dm WHERE dm.match_id IS NOT NULL"
        f") SELECT match_id FROM dm WHERE match_id IS NOT NULL ORDER BY match_id"
    )


def _load_tracking_matches(provider: str | None = None) -> Any:
    """Load distinct match IDs from fct_physical_stats using recursive CTE."""
    tbl = t("fct_physical_stats_synced")
    return _fetch_physical_stats_matches(tbl, provider)


@st.cache_data(ttl=600, show_spinner="Loading physical stats...")
def _fetch_physical_stats(tbl: str, dim: str, m_id: str) -> Any:
    return execute_query(
        f"SELECT ps.player_id, COALESCE(dp.player_display_name, ps.player_id::text) AS player_name, "  # noqa: S608
        f"  ps.match_id, ps.source_provider, ps.minutes_played, "
        f"  ps.total_distance_m, ps.total_distance_km, ps.hsr_distance_m, ps.sprint_distance_m, "
        f"  ps.sprint_frame_count, ps.high_accel_count, ps.high_decel_count, "
        f"  ps.distance_per_minute_m, ps.avg_speed_ms, ps.max_speed_ms, "
        f"  ps.total_off_ball_xt, ps.avg_off_ball_xt "
        f"FROM {tbl} ps "
        f"LEFT JOIN {dim} dp ON ps.player_id::text = dp.canonical_player_id::text "
        f"WHERE ps.match_id = %s "
        f"ORDER BY ps.total_distance_m DESC",
        (m_id,),
    )


def _load_physical_stats(match_id: str) -> Any:
    """Load physical stats for a specific match, joined with player names."""
    tbl = t("fct_physical_stats_synced")
    dim = t("dim_players_synced")
    return _fetch_physical_stats(tbl, dim, str(match_id))


@st.cache_data(ttl=600, show_spinner="Loading PPDA data...")
def _fetch_ppda_data(tbl: str, comp_id: int) -> Any:
    return execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_ppda, away_ppda, home_possession_pct "
        f"FROM {tbl} "
        f"WHERE competition_id = %s AND home_ppda IS NOT NULL "
        f"ORDER BY match_date LIMIT 500",
        (comp_id,),
    )


def _load_ppda_data(competition_id: int) -> Any:
    """Load PPDA data for a competition."""
    competition_id = int(competition_id)
    tbl = t("fct_match_summary_synced")
    return _fetch_ppda_data(tbl, competition_id)


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


def page() -> None:
    """Render the Movement Analysis page."""
    st.header(":material/directions_run: Movement & Pressing")
    st.caption(
        "Off-Ball xT combines pitch control "
        "([Spearman 2017](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals)) "
        "with Expected Threat zones "
        "([Karun Singh 2018](https://karun.in/blog/expected-threat.html)). "
        "Physical metrics from tracking data."
    )

    with st.sidebar:
        view = st.radio(
            "View",
            ["Physical Performance", "PPDA / Pressing Intensity", "Off-Ball xT"],
        )

    if view == "Physical Performance":
        _render_physical()
    elif view == "PPDA / Pressing Intensity":
        _render_ppda()
    else:
        _render_off_ball_xt()


# ---------------------------------------------------------------------------
# View 1: Physical Performance (tracking matches only)
# ---------------------------------------------------------------------------


def _render_physical() -> None:
    """Render physical performance metrics for tracking matches."""
    with st.sidebar:
        provider = st.selectbox("Provider", _PROVIDER_OPTIONS, index=0)
        selected_provider = None if provider == "All" else provider

    data_scope_note("Physical metrics available for ~20 matches with tracking data.")

    matches = _load_tracking_matches(selected_provider)
    if matches.empty:
        empty_result("physical stats", scope_hint="This page requires tracking data (available for ~20 matches).")
        return

    with st.sidebar:
        match_id = _render_match_selectbox(matches, key="phys_match")

    if match_id is None:
        empty_select("a match")
        return

    stats = _load_physical_stats(str(match_id))
    if stats.empty:
        empty_result("physical data for this match")
        return

    # Metric selector
    metric_options = {
        "Total Distance (km)": ("total_distance_km", "Distance (km)"),
        "HSR Distance (m)": ("hsr_distance_m", "HSR Distance (m)"),
        "Sprint Distance (m)": ("sprint_distance_m", "Sprint Distance (m)"),
    }
    selected_metric = st.selectbox("Metric", list(metric_options.keys()))
    col_name, col_label = metric_options[selected_metric]  # type: ignore[index]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Players", len(stats), help=METRIC_HELP.get("Players"))
    c2.metric(
        "Avg Distance (km)",
        f"{stats['total_distance_km'].mean():.1f}",
        help=METRIC_HELP.get("Avg Distance (km)"),
    )
    c3.metric(
        "Max Speed (km/h)",
        f"{stats['max_speed_ms'].max() * 3.6:.1f}",
        help=METRIC_HELP.get("Max Speed (km/h)"),
    )
    c4.metric(
        "Max Speed (m/s)",
        f"{stats['max_speed_ms'].max():.1f}",
        help=METRIC_HELP.get("Max Speed (m/s)"),
    )

    fig = plot_physical_bars(stats, col_name, col_label, title=str(selected_metric), label_col="player_name")
    st.pyplot(fig)

    with st.expander("Full Stats Table", icon=":material/table_chart:"):
        display_cols = [
            "player_name",
            "minutes_played",
            "total_distance_km",
            "hsr_distance_m",
            "sprint_distance_m",
            "sprint_frame_count",
            "high_accel_count",
            "high_decel_count",
            "avg_speed_ms",
            "max_speed_ms",
        ]
        st.dataframe(
            stats[[c for c in display_cols if c in stats.columns]].rename(
                columns={
                    "player_name": "Player",
                    "minutes_played": "Minutes",
                    "total_distance_km": "Distance (km)",
                    "hsr_distance_m": "HSR (m)",
                    "sprint_distance_m": "Sprint (m)",
                    "sprint_frame_count": "Sprint Frames",
                    "high_accel_count": "High Accel",
                    "high_decel_count": "High Decel",
                    "avg_speed_ms": "Avg Speed (m/s)",
                    "max_speed_ms": "Max Speed (m/s)",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    data_freshness()  # Default table — fct_physical_stats_synced lacks match_date


# ---------------------------------------------------------------------------
# View 2: PPDA / Pressing Intensity (all StatsBomb matches)
# ---------------------------------------------------------------------------


def _render_ppda() -> None:
    """Render PPDA pressing intensity metrics."""
    with st.sidebar:
        competition_id = render_competition_filter()

    if competition_id is None:
        empty_select("a competition")
        return

    render_scope_label(competition_id)

    data = _load_ppda_data(competition_id)
    if data.empty:
        empty_result("PPDA data")
        return

    # Summary stats in a row above the chart
    avg_home = float(data["home_ppda"].mean())
    avg_away = float(data["away_ppda"].mean())
    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Avg Home PPDA",
        f"{avg_home:.1f}",
        help=METRIC_HELP.get("Avg Home PPDA") or None,
    )
    c2.metric(
        "Avg Away PPDA",
        f"{avg_away:.1f}",
        help=METRIC_HELP.get("Avg Away PPDA") or None,
    )
    c3.metric("Matches", len(data), help=METRIC_HELP.get("Matches"))

    fig = plot_ppda_bars(data, title="PPDA by Match")
    st.pyplot(fig, use_container_width=True)

    with st.expander("PPDA Data", icon=":material/table_chart:"):
        st.dataframe(
            data.rename(
                columns={
                    "match_date": "Date",
                    "home_team_name": "Home",
                    "away_team_name": "Away",
                    "home_ppda": "Home PPDA",
                    "away_ppda": "Away PPDA",
                    "home_possession_pct": "Home Poss %",
                }
            ).drop(columns=["match_id"], errors="ignore"),
            use_container_width=True,
            hide_index=True,
        )

    data_freshness()


# ---------------------------------------------------------------------------
# View 3: Off-Ball xT (tracking matches only)
# ---------------------------------------------------------------------------


def _render_off_ball_xt() -> None:
    """Render Off-Ball xT player rankings for tracking matches."""
    with st.sidebar:
        provider = st.selectbox("Provider", _PROVIDER_OPTIONS, index=0, key="oxt_provider")
        selected_provider = None if provider == "All" else provider

    matches = _load_tracking_matches(selected_provider)
    if matches.empty:
        empty_result("tracking data", scope_hint="This page requires tracking data (available for ~20 matches).")
        return

    with st.sidebar:
        match_id = _render_match_selectbox(matches, key="oxt_match")

    if match_id is None:
        empty_select("a match")
        return

    stats = _load_physical_stats(str(match_id))
    xt_stats = stats[stats["total_off_ball_xt"].notna()] if not stats.empty else stats

    if xt_stats.empty:
        empty_result("Off-Ball xT data", scope_hint="This page requires tracking data with xT computation.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Players", len(xt_stats), help=METRIC_HELP.get("Players"))
    c2.metric(
        "Avg Off-Ball xT",
        f"{xt_stats['total_off_ball_xt'].mean():.3f}",
        help=METRIC_HELP.get("Avg Off-Ball xT") or None,
    )
    c3.metric(
        "Max Off-Ball xT",
        f"{xt_stats['total_off_ball_xt'].max():.3f}",
        help=METRIC_HELP.get("Max Off-Ball xT") or None,
    )

    fig = plot_physical_bars(
        xt_stats, "total_off_ball_xt", "Total Off-Ball xT", title="Off-Ball xT by Player", label_col="player_name"
    )
    st.pyplot(fig)

    with st.expander("Off-Ball xT Data", icon=":material/table_chart:"):
        st.dataframe(
            xt_stats[["player_name", "total_off_ball_xt", "avg_off_ball_xt"]].rename(
                columns={
                    "player_name": "Player",
                    "total_off_ball_xt": "Total xT",
                    "avg_off_ball_xt": "Avg xT/Frame",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    data_freshness()  # Default table — fct_physical_stats_synced lacks match_date
