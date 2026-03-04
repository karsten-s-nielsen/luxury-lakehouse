"""Pass Network page — visualize player-to-player passing connections."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app.components.filters import render_competition_filter, render_match_filter, render_team_filter
from streamlit_app.components.pitch import plot_pass_network_interactive
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_passes(competition_id: int, team_id: int, match_id: int) -> Any:
    """Load completed passes with recipient data for network construction."""
    competition_id, team_id, match_id = int(competition_id), int(team_id), int(match_id)

    passes_tbl = t("fct_passes_synced")
    passer_tbl = t("dim_players_synced")
    receiver_tbl = t("dim_players_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading passes...")
    def _query(comp_id: int, t_id: int, m_id: int) -> Any:
        return execute_query(
            f"SELECT p.player_id, p.pass_recipient_id, "  # noqa: S608
            f"  p.start_x, p.start_y, p.end_x, p.end_y, p.is_complete, "
            f"  passer.player_display_name AS passer_name, "
            f"  receiver.player_display_name AS receiver_name "
            f"FROM {passes_tbl} p "
            f"JOIN {passer_tbl} passer ON p.player_id = passer.player_id "
            f"LEFT JOIN {receiver_tbl} receiver ON p.pass_recipient_id = receiver.player_id "
            f"WHERE p.competition_id = %s AND p.team_id = %s AND p.match_id = %s "
            f"  AND p.is_complete = true AND p.pass_recipient_id IS NOT NULL "
            f"ORDER BY p.minute, p.second",
            (comp_id, t_id, m_id),
        )

    return _query(competition_id, team_id, match_id)


def _build_network(
    passes: pd.DataFrame,
    min_pair_count: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build nodes and edges DataFrames from raw pass data.

    Returns (nodes, edges) where:
      - nodes: player_id, player_display_name, avg_x, avg_y, pass_count
      - edges: passer_id, receiver_id, pair_count, avg_start_x, avg_start_y,
               avg_end_x, avg_end_y
    """
    if passes.empty:
        node_cols = pd.Index(["player_id", "player_display_name", "avg_x", "avg_y", "pass_count"])
        edge_cols = pd.Index(
            [
                "passer_id",
                "receiver_id",
                "pair_count",
                "avg_start_x",
                "avg_start_y",
                "avg_end_x",
                "avg_end_y",
            ]
        )
        return pd.DataFrame(columns=node_cols), pd.DataFrame(columns=edge_cols)

    # Build nodes from both passer and receiver positions
    passer_locs: pd.DataFrame = passes[["player_id", "passer_name", "start_x", "start_y"]].rename(
        columns={"passer_name": "name", "start_x": "x", "start_y": "y"}  # type: ignore[call-overload]
    )
    receiver_locs: pd.DataFrame = passes[["pass_recipient_id", "receiver_name", "end_x", "end_y"]].rename(
        columns={"pass_recipient_id": "player_id", "receiver_name": "name", "end_x": "x", "end_y": "y"}  # type: ignore[call-overload]
    )
    all_locs = pd.concat([passer_locs, receiver_locs], ignore_index=True)

    nodes: pd.DataFrame = (
        all_locs.groupby("player_id")
        .agg(
            player_display_name=("name", "first"),
            avg_x=("x", "mean"),
            avg_y=("y", "mean"),
            pass_count=("x", "count"),
        )
        .reset_index()
    )

    # Build edges
    edge_agg: pd.DataFrame = (
        passes.groupby(["player_id", "pass_recipient_id"])
        .agg(
            pair_count=("start_x", "count"),
            avg_start_x=("start_x", "mean"),
            avg_start_y=("start_y", "mean"),
            avg_end_x=("end_x", "mean"),
            avg_end_y=("end_y", "mean"),
        )
        .reset_index()
        .rename(columns={"player_id": "passer_id", "pass_recipient_id": "receiver_id"})  # type: ignore[call-overload]
    )

    edges: pd.DataFrame = edge_agg[edge_agg["pair_count"] >= min_pair_count].reset_index(drop=True)  # type: ignore[assignment]

    return nodes, edges


def page() -> None:
    """Render the Pass Network page."""
    st.header(":material/hub: Pass Network")

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        match_id = render_match_filter(competition_id, team_id)
        min_passes = st.slider("Min. passes per connection", min_value=1, max_value=10, value=3)

    if competition_id is None or team_id is None or match_id is None:
        st.info("Select a competition, team, and match to view the pass network.")
        return

    passes = _load_passes(competition_id, team_id, match_id)

    if passes.empty:
        st.warning("No completed passes with recipient data found. Wyscout matches do not include pass recipient.")
        return

    nodes, edges = _build_network(passes, min_pair_count=min_passes)

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        fig = plot_pass_network_interactive(nodes, edges, title="Pass Network")
        st.plotly_chart(fig, use_container_width=True)

    with col_stats:
        total_passes = len(passes)
        unique_connections = len(edges)

        st.metric("Completed Passes", total_passes)
        st.metric("Unique Connections", unique_connections)

        if not edges.empty:
            top_edge = edges.loc[edges["pair_count"].idxmax()]
            passer_name = nodes.loc[nodes["player_id"] == top_edge["passer_id"], "player_display_name"].values
            receiver_name = nodes.loc[nodes["player_id"] == top_edge["receiver_id"], "player_display_name"].values
            p_name = passer_name[0] if len(passer_name) > 0 else "?"
            r_name = receiver_name[0] if len(receiver_name) > 0 else "?"
            st.metric("Top Pair Count", int(top_edge["pair_count"]))
            st.caption(f"**{p_name}**  \n\u2192 {r_name}")
