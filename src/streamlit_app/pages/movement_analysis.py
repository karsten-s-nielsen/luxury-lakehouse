"""Movement Analysis page — physical performance, PPDA, and Off-Ball xT."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_physical_bars, plot_ppda_bars
from streamlit_app.components.filters import render_competition_filter
from streamlit_app.db import execute_query, t

_PROVIDER_OPTIONS = ["All", "metrica", "idsse", "skillcorner"]

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=600, show_spinner=False)
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
def _fetch_physical_stats(tbl: str, m_id: str) -> Any:
    return execute_query(
        f"SELECT player_id, match_id, source_provider, minutes_played, "  # noqa: S608
        f"  total_distance_m, total_distance_km, hsr_distance_m, sprint_distance_m, "
        f"  sprint_frame_count, high_accel_count, high_decel_count, "
        f"  distance_per_minute_m, avg_speed_ms, max_speed_ms, "
        f"  total_off_ball_xt, avg_off_ball_xt "
        f"FROM {tbl} WHERE match_id = %s "
        f"ORDER BY total_distance_m DESC",
        (m_id,),
    )


def _load_physical_stats(match_id: str) -> Any:
    """Load physical stats for a specific match."""
    tbl = t("fct_physical_stats_synced")
    return _fetch_physical_stats(tbl, str(match_id))


@st.cache_data(ttl=600, show_spinner="Loading PPDA data...")
def _fetch_ppda_data(tbl: str, comp_id: int) -> Any:
    return execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_ppda, away_ppda, home_possession_pct "
        f"FROM {tbl} "
        f"WHERE competition_id = %s AND home_ppda IS NOT NULL "
        f"ORDER BY match_date",
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
    st.header(":material/directions_run: Movement Analysis")
    st.caption(
        "Off-Ball xT combines pitch control "
        "([Spearman 2017](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals)) "
        "with Expected Threat zones "
        "([Karun Singh 2018](https://karun.in/blog/expected-threat.html)). "
        "Physical metrics from tracking data."
    )

    view = st.radio(
        "View",
        ["Physical Performance", "PPDA / Pressing Intensity", "Off-Ball xT"],
        horizontal=True,
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

    matches = _load_tracking_matches(selected_provider)
    if matches.empty:
        st.info("No physical stats available. Run the dbt build after tracking ingestion.")
        return

    with st.sidebar:
        match_id = st.selectbox("Match", matches["match_id"].tolist())

    if match_id is None:
        st.info("Select a match.")
        return

    stats = _load_physical_stats(str(match_id))
    if stats.empty:
        st.warning("No physical data for this match.")
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
    c1.metric("Players", len(stats))
    c2.metric("Avg Distance (km)", f"{stats['total_distance_km'].mean():.1f}")
    c3.metric("Max Speed (m/s)", f"{stats['max_speed_ms'].max():.1f}")
    c4.metric("Max Speed (km/h)", f"{stats['max_speed_ms'].max() * 3.6:.1f}")

    fig = plot_physical_bars(stats, col_name, col_label, title=str(selected_metric))
    st.pyplot(fig, use_container_width=True)

    with st.expander("Full Stats Table"):
        display_cols = [
            "player_id",
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
                    "player_id": "Player",
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


# ---------------------------------------------------------------------------
# View 2: PPDA / Pressing Intensity (all StatsBomb matches)
# ---------------------------------------------------------------------------


def _render_ppda() -> None:
    """Render PPDA pressing intensity metrics."""
    with st.sidebar:
        competition_id = render_competition_filter()

    if competition_id is None:
        st.info("Select a competition to view PPDA metrics.")
        return

    data = _load_ppda_data(competition_id)
    if data.empty:
        st.warning("No PPDA data available for this competition.")
        return

    # Summary stats in a row above the chart
    avg_home = float(data["home_ppda"].mean())
    avg_away = float(data["away_ppda"].mean())
    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Home PPDA", f"{avg_home:.1f}")
    c2.metric("Avg Away PPDA", f"{avg_away:.1f}")
    c3.metric("Matches", len(data))

    fig = plot_ppda_bars(data, title="PPDA by Match")
    st.pyplot(fig, use_container_width=True)

    with st.expander("PPDA Data"):
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
        st.info("No tracking data available.")
        return

    with st.sidebar:
        match_id = st.selectbox("Match", matches["match_id"].tolist(), key="oxt_match")

    if match_id is None:
        st.info("Select a match.")
        return

    stats = _load_physical_stats(str(match_id))
    xt_stats = stats[stats["total_off_ball_xt"].notna()] if not stats.empty else stats

    if xt_stats.empty:
        st.warning("No Off-Ball xT data for this match. Run the `compute_off_ball_xt` pipeline and rebuild dbt models.")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Players", len(xt_stats))
    c2.metric("Avg Off-Ball xT", f"{xt_stats['total_off_ball_xt'].mean():.3f}")
    c3.metric("Max Off-Ball xT", f"{xt_stats['total_off_ball_xt'].max():.3f}")

    fig = plot_physical_bars(xt_stats, "total_off_ball_xt", "Total Off-Ball xT", title="Off-Ball xT by Player")
    st.pyplot(fig, use_container_width=True)

    with st.expander("Off-Ball xT Data"):
        st.dataframe(
            xt_stats[["player_id", "total_off_ball_xt", "avg_off_ball_xt"]].rename(
                columns={
                    "player_id": "Player",
                    "total_off_ball_xt": "Total xT",
                    "avg_off_ball_xt": "Avg xT/Frame",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
