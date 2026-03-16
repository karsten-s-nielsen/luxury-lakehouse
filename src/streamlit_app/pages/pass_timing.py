"""Pass Timing page — PAUSA temporal judgment, spatial selection, and composite scoring.

Visualises per-pass and per-player PAUSA metrics from IDSSE Bundesliga tracking
data with ELASTIC event-tracking synchronisation.

Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying
Optimal Pass Timing Beyond Speed." MIT Sloan 2026.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.components.feedback import data_scope_note, empty_result, empty_select
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.db import execute_query, t

# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=600, show_spinner="Loading PAUSA matches...")
def _fetch_pausa_matches(tbl: str) -> pd.DataFrame:
    """Load distinct match IDs that have PAUSA data, with match labels."""
    match_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT DISTINCT pv.match_id, ms.match_date, ms.home_team_name, ms.away_team_name "  # noqa: S608
        f"FROM {tbl} pv "
        f"LEFT JOIN {match_tbl} ms ON pv.match_id::text = ms.match_id::text "
        f"ORDER BY ms.match_date "
        f"LIMIT 100",
    )


@st.cache_data(ttl=600, show_spinner="Loading PAUSA teams...")
def _fetch_teams_for_match(tbl: str, match_id: str) -> pd.DataFrame:
    """Load distinct teams for a given match."""
    return execute_query(
        f"SELECT DISTINCT team FROM {tbl} WHERE match_id = %s AND team IS NOT NULL ORDER BY team LIMIT 50",  # noqa: S608
        (match_id,),
    )


@st.cache_data(ttl=600, show_spinner="Loading PAUSA players...")
def _fetch_players_for_match(pausa_tbl: str, dim_tbl: str, match_id: str, team: str | None) -> pd.DataFrame:
    """Load distinct players for a given match, optionally filtered by team."""
    if team:
        return execute_query(
            f"SELECT DISTINCT pv.player_id, COALESCE(dp.player_display_name, pv.player_id) AS player_display_name "  # noqa: S608
            f"FROM {pausa_tbl} pv "
            f"LEFT JOIN {dim_tbl} dp ON pv.player_id::text = dp.player_id::text "
            f"WHERE pv.match_id = %s AND pv.team = %s AND pv.player_id IS NOT NULL "
            f"ORDER BY player_display_name "
            f"LIMIT 50",
            (match_id, team),
        )
    return execute_query(
        f"SELECT DISTINCT pv.player_id, COALESCE(dp.player_display_name, pv.player_id) AS player_display_name "  # noqa: S608
        f"FROM {pausa_tbl} pv "
        f"LEFT JOIN {dim_tbl} dp ON pv.player_id::text = dp.player_id::text "
        f"WHERE pv.match_id = %s AND pv.player_id IS NOT NULL "
        f"ORDER BY player_display_name "
        f"LIMIT 50",
        (match_id,),
    )


@st.cache_data(ttl=600, show_spinner="Loading PAUSA metrics...")
def _fetch_pausa_summary(tbl: str, match_id: str, team: str | None, player_id: str | None) -> pd.DataFrame:
    """Load aggregate PAUSA metrics for filter selection."""
    conditions = ["match_id = %s"]
    params: list[Any] = [match_id]

    if team:
        conditions.append("team = %s")
        params.append(team)
    if player_id:
        conditions.append("player_id = %s")
        params.append(player_id)

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT "  # noqa: S608
        f"  AVG(pausa_score) AS avg_pausa, "
        f"  AVG(temporal_judgment) AS avg_temporal, "
        f"  AVG(spatial_selection) AS avg_spatial, "
        f"  COUNT(*) AS pass_count "
        f"FROM {tbl} WHERE {where}",
        tuple(params),
    )


@st.cache_data(ttl=600, show_spinner="Loading PAUSA pass data...")
def _fetch_pausa_passes(tbl: str, dim_tbl: str, match_id: str, team: str | None, player_id: str | None) -> pd.DataFrame:
    """Load individual pass PAUSA scores for scatter/heatmap (bounded)."""
    conditions = ["pv.match_id = %s"]
    params: list[Any] = [match_id]

    if team:
        conditions.append("pv.team = %s")
        params.append(team)
    if player_id:
        conditions.append("pv.player_id = %s")
        params.append(player_id)

    where = " AND ".join(conditions)
    return execute_query(
        f"SELECT pv.pass_id, pv.player_id, dp.player_display_name, pv.team, "  # noqa: S608
        f"  pv.temporal_judgment, pv.spatial_selection, pv.pausa_score, "
        f"  pv.actual_obso, pv.peak_obso, pv.optimal_obso, "
        f"  pv.receiver_x, pv.receiver_y "
        f"FROM {tbl} pv "
        f"LEFT JOIN {dim_tbl} dp ON pv.player_id::text = dp.player_id::text "
        f"WHERE {where} "
        f"LIMIT 2000",
        tuple(params),
    )


@st.cache_data(ttl=600, show_spinner="Loading rankings...")
def _fetch_rankings(timing_tbl: str) -> pd.DataFrame:
    """Load fct_pass_timing rankings (bounded)."""
    match_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT COALESCE(pt.player_display_name, pt.player_id) AS player_display_name, "  # noqa: S608
        f"  COALESCE(ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    pt.match_id) AS match_label, "
        f"  pt.pass_count, "
        f"  pt.avg_pausa, pt.avg_temporal_judgment, pt.avg_spatial_selection, "
        f"  pt.median_pausa, pt.passes_above_median_pausa "
        f"FROM {timing_tbl} pt "
        f"LEFT JOIN {match_tbl} ms ON pt.match_id::text = ms.match_id::text "
        f"ORDER BY pt.avg_pausa DESC "
        f"LIMIT 500",
    )


# ---------------------------------------------------------------------------
# Visualisations
# ---------------------------------------------------------------------------


def _create_scatter_plot(df: pd.DataFrame) -> go.Figure:
    """Create temporal vs spatial scatter plot with PAUSA as bubble size."""
    if df.empty:
        return go.Figure()

    fig = px.scatter(
        df,
        x="temporal_judgment",
        y="spatial_selection",
        size="pausa_score",
        color="team",
        hover_data=["player_display_name", "pausa_score"],
        labels={
            "temporal_judgment": "Temporal Judgment (when)",
            "spatial_selection": "Spatial Selection (where)",
            "pausa_score": "PAUSA Score",
            "team": "Team",
            "player_display_name": "Player",
        },
        title="Pass Timing: When vs Where",
        size_max=20,
    )

    # Add quadrant lines at 0.5
    fig.add_hline(y=0.5, line_dash="dash", line_color="gray", opacity=0.5)
    fig.add_vline(x=0.5, line_dash="dash", line_color="gray", opacity=0.5)

    # Quadrant labels
    _annotations = [
        {"x": 0.25, "y": 0.75, "text": "Good where,<br>poor when"},
        {"x": 0.75, "y": 0.75, "text": "Good timing<br>& target"},
        {"x": 0.25, "y": 0.25, "text": "Poor timing<br>& target"},
        {"x": 0.75, "y": 0.25, "text": "Good when,<br>poor where"},
    ]
    for ann in _annotations:
        fig.add_annotation(
            x=ann["x"],
            y=ann["y"],
            text=ann["text"],
            showarrow=False,
            font={"size": 10, "color": "gray"},
            opacity=0.6,
        )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        xaxis={"range": [0, 1]},
        yaxis={"range": [0, 1]},
        height=450,
    )
    return fig


def _create_obso_heatmap(df: pd.DataFrame) -> go.Figure:
    """Create OBSO receiver location heatmap on a pitch overlay."""
    if df.empty or "receiver_x" not in df.columns:
        return go.Figure()

    # Filter rows with valid receiver coordinates
    valid = df.dropna(subset=["receiver_x", "receiver_y"])
    if valid.empty:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117",
            title="No receiver location data available",
        )
        return fig

    fig = px.density_heatmap(
        valid,
        x="receiver_x",
        y="receiver_y",
        z="actual_obso",
        histfunc="avg",
        nbinsx=24,
        nbinsy=16,
        color_continuous_scale="YlOrRd",
        labels={
            "receiver_x": "X (yards)",
            "receiver_y": "Y (yards)",
            "actual_obso": "Avg OBSO",
        },
        title="OBSO at Receiver Location",
    )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        xaxis={"range": [0, 120], "title": "X (yards)"},
        yaxis={"range": [0, 80], "title": "Y (yards)"},
        height=450,
    )
    return fig


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


def page() -> None:
    """Render the Pass Timing page."""
    st.header(":material/timer: Pass Timing")
    st.caption(
        "PAUSA: Passing Ability Under Spatiotemporal Awareness. "
        "Composite of temporal judgment (when) \u00d7 spatial selection (where). "
        "[Lee, Jo, Hong, Bauer & Ko (2026)](https://github.com/leemingo/mitssac-pausa) "
        "MIT Sloan 2026. OBSO value surface by "
        "[Spearman (2018)](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals). "
        "Event-tracking sync via "
        "[Kim et al. (2025)](https://arxiv.org/abs/2508.09238) ELASTIC."
    )

    data_scope_note("PAUSA data available for 7 IDSSE Bundesliga matches with ELASTIC event-tracking sync.")

    pausa_tbl = t("fct_pausa_values_synced")
    timing_tbl = t("fct_pass_timing_synced")
    dim_players_tbl = t("dim_players_synced")

    # Load matches with PAUSA data
    matches_df = _fetch_pausa_matches(pausa_tbl)
    if matches_df.empty:
        empty_result(
            "PAUSA data",
            scope_hint="Pass timing requires OBSO computation (D16) and PAUSA pipeline. Currently 7 IDSSE matches.",
        )
        return

    # Build match labels
    match_options = matches_df.to_dict("records")
    match_labels = [
        f"{r.get('match_date', '?')} \u2014 {r.get('home_team_name', '?')} v {r.get('away_team_name', '?')}"
        if r.get("home_team_name")
        else f"Match {r['match_id']}"
        for r in match_options
    ]

    # Sidebar filters
    with st.sidebar:
        match_idx = st.selectbox(
            "Match",
            range(len(match_labels)),
            format_func=lambda i: match_labels[i],
            key="pausa_match",
        )
        if match_idx is None:
            empty_select("a match")
            return
        selected_match = str(match_options[int(match_idx)]["match_id"])

        # Team filter
        teams_df = _fetch_teams_for_match(pausa_tbl, selected_match)
        team_options = ["All", *(teams_df["team"].tolist() if not teams_df.empty else [])]
        selected_team_label = st.selectbox("Team", team_options, key="pausa_team")
        selected_team: str | None = None if selected_team_label == "All" else selected_team_label

        # Player filter
        players_df = _fetch_players_for_match(pausa_tbl, dim_players_tbl, selected_match, selected_team)
        if not players_df.empty:
            player_options = players_df.to_dict("records")
            player_labels = [r.get("player_display_name") or f"Player {r['player_id']}" for r in player_options]
            player_idx = st.selectbox(
                "Player",
                [None, *range(len(player_labels))],
                format_func=lambda i: "All players" if i is None else player_labels[i],
                key="pausa_player",
            )
            selected_player: str | None = (
                str(player_options[player_idx]["player_id"]) if player_idx is not None else None
            )
        else:
            selected_player = None

    # Metrics row
    summary_df = _fetch_pausa_summary(pausa_tbl, selected_match, selected_team, selected_player)

    if summary_df.empty or summary_df.iloc[0]["avg_pausa"] is None:
        empty_result(
            "PAUSA data for the selected filters",
            scope_hint="Try selecting a different match or removing team/player filters.",
        )
        return

    row = summary_df.iloc[0]
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "Avg PAUSA",
            f"{float(row['avg_pausa']):.3f}",
            help=METRIC_HELP.get("Avg PAUSA"),
        )
    with col2:
        st.metric(
            "Avg Temporal Judgment",
            f"{float(row['avg_temporal']):.3f}",
            help=METRIC_HELP.get("Avg Temporal Judgment"),
        )
    with col3:
        st.metric(
            "Avg Spatial Selection",
            f"{float(row['avg_spatial']):.3f}",
            help=METRIC_HELP.get("Avg Spatial Selection"),
        )

    st.metric(
        "Pass Count",
        f"{int(row['pass_count'])}",
        help=METRIC_HELP.get("Pass Count"),
    )

    # Load individual pass data for visualisations
    passes_df = _fetch_pausa_passes(pausa_tbl, dim_players_tbl, selected_match, selected_team, selected_player)

    if passes_df.empty:
        empty_result("individual pass data for the selected filters")
        return

    # Visualisation row
    col_heatmap, col_scatter = st.columns([1, 1])

    with col_heatmap:
        heatmap_fig = _create_obso_heatmap(passes_df)
        st.plotly_chart(heatmap_fig, use_container_width=True)

    with col_scatter:
        scatter_fig = _create_scatter_plot(passes_df)
        st.plotly_chart(scatter_fig, use_container_width=True)

    # Rankings table
    st.subheader("Player Rankings")
    rankings_df = _fetch_rankings(timing_tbl)
    if rankings_df.empty:
        empty_result("player rankings — run dbt build with pausa_enabled=true")
    else:
        st.dataframe(
            rankings_df.rename(
                columns={
                    "player_display_name": "Player",
                    "match_label": "Match",
                    "pass_count": "Passes",
                    "avg_pausa": "Avg PAUSA",
                    "avg_temporal_judgment": "Avg Temporal",
                    "avg_spatial_selection": "Avg Spatial",
                    "median_pausa": "Median PAUSA",
                    "passes_above_median_pausa": "Above Median",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    # Footer
    st.caption(
        '*Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed." '
        "MIT Sloan 2026. OBSO: Spearman (2018), Fernandez & Bornn (2018). "
        "Event-tracking sync: Kim et al. (2025) ELASTIC.*"
    )
    data_scope_note("IDSSE Bundesliga \u00b7 7 matches \u00b7 Tracking-dependent")
