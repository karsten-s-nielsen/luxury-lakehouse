"""Pass Network state — fetch completed passes, build network graph, render pitch overlay.

Prefix: pn_
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from filters import fetch_data_freshness, fetch_scope_label
from queries.passes import fetch_network_passes
from render import fmt_int

from state.shared import get_comp_id, get_match_id, get_team_id, register_page_refresher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exported state variables
# ---------------------------------------------------------------------------
pn_total_passes: str = "--"
pn_unique_connections: str = "--"
pn_top_pair_count: str = "--"
pn_top_pair_names: str = ""
pn_chart_figure: go.Figure | None = None

pn_warning_text: str = ""
pn_scope_label: str = ""
pn_data_freshness: str = ""

__all__ = [
    "pn_data_freshness",
    "pn_chart_figure",
    "pn_refresh",
    "pn_scope_label",
    "pn_top_pair_count",
    "pn_top_pair_names",
    "pn_total_passes",
    "pn_unique_connections",
    "pn_warning_text",
]


# ---------------------------------------------------------------------------
# Network construction
# ---------------------------------------------------------------------------


def _build_network(
    passes: pd.DataFrame,
    min_pair_count: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build nodes and edges DataFrames from raw pass data.

    Returns (nodes, edges) where:
      - nodes: player_id, player_display_name, avg_x, avg_y, pass_count
      - edges: passer_id, receiver_id, pair_count
    """
    if passes.empty:
        node_cols = pd.Index(["player_id", "player_display_name", "avg_x", "avg_y", "pass_count"])
        edge_cols = pd.Index(["passer_id", "receiver_id", "pair_count"])
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
        .agg(pair_count=("start_x", "count"))
        .reset_index()
        .rename(columns={"player_id": "passer_id", "pass_recipient_id": "receiver_id"})  # type: ignore[call-overload]
    )

    edges: pd.DataFrame = edge_agg[edge_agg["pair_count"] >= min_pair_count].reset_index(drop=True)  # type: ignore[assignment]

    return nodes, edges


# ---------------------------------------------------------------------------
# Rendering — static mplsoccer pitch with network overlay
# ---------------------------------------------------------------------------


def _add_pitch_shapes(fig: go.Figure) -> None:
    """Add StatsBomb pitch lines (120x80) as Plotly shapes."""
    line = dict(color="rgba(255,255,255,0.25)", width=1.5)
    # Outer boundary
    fig.add_shape(type="rect", x0=0, y0=0, x1=120, y1=80, line=line)
    # Halfway line
    fig.add_shape(type="line", x0=60, y0=0, x1=60, y1=80, line=line)
    # Center circle
    fig.add_shape(type="circle", x0=60 - 9.15, y0=40 - 9.15, x1=60 + 9.15, y1=40 + 9.15, line=line)
    # Center spot
    fig.add_shape(type="circle", x0=59.5, y0=39.5, x1=60.5, y1=40.5, line=line, fillcolor="rgba(255,255,255,0.25)")
    # Left penalty area
    fig.add_shape(type="rect", x0=0, y0=18, x1=18, y1=62, line=line)
    # Right penalty area
    fig.add_shape(type="rect", x0=102, y0=18, x1=120, y1=62, line=line)
    # Left goal area
    fig.add_shape(type="rect", x0=0, y0=30, x1=6, y1=50, line=line)
    # Right goal area
    fig.add_shape(type="rect", x0=114, y0=30, x1=120, y1=50, line=line)
    # Left penalty spot
    fig.add_shape(type="circle", x0=11.5, y0=39.5, x1=12.5, y1=40.5, line=line, fillcolor="rgba(255,255,255,0.25)")
    # Right penalty spot
    fig.add_shape(type="circle", x0=107.5, y0=39.5, x1=108.5, y1=40.5, line=line, fillcolor="rgba(255,255,255,0.25)")


def _build_network_figure(nodes: pd.DataFrame, edges: pd.DataFrame) -> go.Figure | None:
    """Build interactive Plotly pass network figure."""
    if nodes.empty:
        return None

    fig = go.Figure()

    # Draw pitch lines first (behind data)
    _add_pitch_shapes(fig)

    # Add edges as lines
    node_lookup = {row["player_id"]: row for _, row in nodes.iterrows()}
    pair_max = edges["pair_count"].max() if not edges.empty else 1
    for _, edge in edges.iterrows():
        src = node_lookup.get(edge["passer_id"])
        tgt = node_lookup.get(edge["receiver_id"])
        if src is None or tgt is None:
            continue
        width = 1 + (edge["pair_count"] / max(pair_max, 1)) * 6
        fig.add_trace(
            go.Scatter(
                x=[src["avg_x"], tgt["avg_x"]],
                y=[src["avg_y"], tgt["avg_y"]],
                mode="lines",
                line=dict(width=width, color="rgba(255,255,255,0.3)"),
                hoverinfo="text",
                text=f"{src['player_display_name']} \u2192 {tgt['player_display_name']}: {edge['pair_count']} passes",
                showlegend=False,
            )
        )

    fig.add_annotation(
        text="Line thickness = pass frequency between pair",
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.02,
        showarrow=False,
        font=dict(color="rgba(255,255,255,0.5)", size=10),
    )

    # Add nodes as scatter
    pc_min = nodes["pass_count"].min()
    pc_range = max(nodes["pass_count"].max() - pc_min, 1)
    sizes = 8 + np.sqrt((nodes["pass_count"] - pc_min) / pc_range) * 30
    fig.add_trace(
        go.Scatter(
            x=nodes["avg_x"],
            y=nodes["avg_y"],
            mode="markers+text",
            marker=dict(size=sizes, color="#f59e0b"),
            text=nodes["player_display_name"],
            textposition="top center",
            textfont=dict(color="white", size=10),
            hovertemplate="%{text}<br>Passes: %{customdata}<extra></extra>",
            customdata=nodes["pass_count"],
            showlegend=False,
        )
    )

    fig.update_layout(
        title="Pass Network",
        title_font_color="white",
        plot_bgcolor="#1a3a2a",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(range=[0, 120], showgrid=False, zeroline=False, visible=False),
        yaxis=dict(range=[80, 0], showgrid=False, zeroline=False, visible=False, scaleanchor="x"),
        margin=dict(l=10, r=10, t=40, b=10),
        height=650,
        font=dict(color="white"),
    )

    return fig


# ---------------------------------------------------------------------------
# Page refresh callback
# ---------------------------------------------------------------------------


def pn_refresh(state: Any) -> None:
    """Refresh pass network data and visualization for current filter selection."""
    comp_id = get_comp_id(state.selected_competition)
    team_id = get_team_id(state.selected_team)
    match_id = get_match_id(state.selected_match)

    if comp_id is None or team_id is None or match_id is None:
        state.pn_total_passes = "--"
        state.pn_unique_connections = "--"
        state.pn_top_pair_count = "--"
        state.pn_top_pair_names = ""
        state.pn_chart_figure = None
        state.pn_warning_text = ""
        state.pn_scope_label = ""
        state.pn_data_freshness = ""
        return

    # Scope label
    state.pn_scope_label = fetch_scope_label(comp_id, team_id)

    try:
        passes = fetch_network_passes(comp_id, team_id, match_id)
    except Exception:
        logger.exception("Failed to fetch pass network data")
        state.pn_total_passes = "\u2013"
        state.pn_unique_connections = "\u2013"
        state.pn_top_pair_count = "--"
        state.pn_top_pair_names = ""
        state.pn_chart_figure = None
        state.pn_data_freshness = ""
        return

    if passes.empty:
        state.pn_total_passes = "0"
        state.pn_unique_connections = "0"
        state.pn_top_pair_count = "0"
        state.pn_top_pair_names = "No data (Wyscout matches lack recipient data)"
        state.pn_chart_figure = None
        state.pn_warning_text = (
            "No completed passes for the selected filters. Wyscout matches do not include pass recipient data."
        )
        state.pn_data_freshness = ""
        return

    state.pn_warning_text = ""
    min_pair_count = int(state.min_passes) if hasattr(state, "min_passes") else 3
    nodes, edges = _build_network(passes, min_pair_count=min_pair_count)

    # Metrics
    state.pn_total_passes = fmt_int(len(passes))
    state.pn_unique_connections = fmt_int(len(edges))

    # Top pair info — split into count and names
    if not edges.empty:
        top_edge = edges.loc[edges["pair_count"].idxmax()]
        passer_name = nodes.loc[nodes["player_id"] == top_edge["passer_id"], "player_display_name"].values
        receiver_name = nodes.loc[nodes["player_id"] == top_edge["receiver_id"], "player_display_name"].values
        p_name = passer_name[0] if len(passer_name) > 0 else "?"
        r_name = receiver_name[0] if len(receiver_name) > 0 else "?"
        # top_edge is a row Series; pair_count is a scalar at runtime, but
        # pandas-stubs infers Series.__getitem__ as Unknown | Series. Force
        # the scalar extraction explicitly before int().
        pair_count_scalar = top_edge["pair_count"]
        state.pn_top_pair_count = fmt_int(int(pair_count_scalar))  # type: ignore[arg-type]
        state.pn_top_pair_names = f"{p_name} \u2192 {r_name}"
    else:
        state.pn_top_pair_count = "0"
        state.pn_top_pair_names = "No connections meet the minimum threshold. Try lowering the pass count filter."

    # Render pitch
    state.pn_chart_figure = _build_network_figure(nodes, edges)

    # Data freshness
    state.pn_data_freshness = fetch_data_freshness()

    logger.info(
        "Pass network: %s passes, %s connections",
        state.pn_total_passes,
        state.pn_unique_connections,
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Pass-Network", pn_refresh)
