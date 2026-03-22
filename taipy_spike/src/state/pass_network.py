"""Pass Network state — fetch completed passes, build network graph, render pitch overlay.

Prefix: pn_
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import pandas as pd
from cache import ttl_cache
from db import execute_query, t
from filters import fetch_data_freshness, fetch_scope_label
from mplsoccer import Pitch
from render import PITCH_BG_COLOR, PITCH_LINE_COLOR, TEXT_COLOR, fmt_int, pitch_to_file

from state.shared import get_comp_id, get_match_id, get_team_id, register_page_refresher

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Theme constants (match Streamlit originals)
# ---------------------------------------------------------------------------
_NETWORK_NODE_COLOR = "#f4d03f"
_NETWORK_EDGE_COLOR = "#e0e0e0"

# ---------------------------------------------------------------------------
# Exported state variables
# ---------------------------------------------------------------------------
pn_total_passes: str = "--"
pn_unique_connections: str = "--"
pn_top_pair_count: str = "--"
pn_top_pair_names: str = ""
pn_pitch_image: str = ""

pn_warning_text: str = ""
pn_scope_label: str = ""
pn_data_freshness: str = ""

__all__ = [
    "pn_data_freshness",
    "pn_pitch_image",
    "pn_refresh",
    "pn_scope_label",
    "pn_top_pair_count",
    "pn_top_pair_names",
    "pn_total_passes",
    "pn_unique_connections",
    "pn_warning_text",
]


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


@ttl_cache()
def _fetch_passes(comp_id: int, team_id: int, match_id: int) -> pd.DataFrame:
    """Fetch completed passes with passer/receiver names for network construction."""
    passes_tbl = t("fct_passes_synced")
    players_tbl = t("dim_players_synced")
    return execute_query(
        f"SELECT p.player_id, p.pass_recipient_id, "  # noqa: S608
        f"  p.start_x, p.start_y, p.end_x, p.end_y, p.is_complete, "
        f"  passer.player_display_name AS passer_name, "
        f"  receiver.player_display_name AS receiver_name "
        f"FROM {passes_tbl} p "
        f"JOIN {players_tbl} passer ON p.player_id = passer.player_id "
        f"LEFT JOIN {players_tbl} receiver ON p.pass_recipient_id = receiver.player_id "
        f"WHERE p.competition_id = %s AND p.team_id = %s AND p.match_id = %s "
        f"  AND p.is_complete = true AND p.pass_recipient_id IS NOT NULL "
        f"ORDER BY p.minute, p.second LIMIT 2000",
        (comp_id, team_id, match_id),
    )


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


def _render_network(nodes: pd.DataFrame, edges: pd.DataFrame) -> str:
    """Render pass network on an mplsoccer pitch and save to temp file.

    Edges: line width proportional to pair count.
    Nodes: marker size proportional to total pass involvements.
    Returns file path to the saved PNG.
    """
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if nodes.empty:
        ax.set_title("Pass Network", color=TEXT_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "pn_network")

    # Draw edges
    if not edges.empty:
        node_pos = nodes.set_index("player_id")[["avg_x", "avg_y"]]
        max_pair = int(edges["pair_count"].max())
        min_pair = int(edges["pair_count"].min())
        pair_range = max(max_pair - min_pair, 1)

        for _, edge in edges.iterrows():
            pid = edge["passer_id"]
            rid = edge["receiver_id"]
            if pid not in node_pos.index or rid not in node_pos.index:
                continue

            px, py = float(node_pos.loc[pid, "avg_x"]), float(node_pos.loc[pid, "avg_y"])
            rx, ry = float(node_pos.loc[rid, "avg_x"]), float(node_pos.loc[rid, "avg_y"])
            count = int(edge["pair_count"])
            weight = (count - min_pair) / pair_range
            width = 1 + weight * 6
            alpha = 0.3 + weight * 0.5

            pitch.arrows(
                [px],
                [py],
                [rx],
                [ry],
                color=_NETWORK_EDGE_COLOR,
                alpha=alpha,
                width=width,
                ax=ax,
                headwidth=4,
                headlength=4,
                zorder=2,
            )

    # Draw nodes — size proportional to pass involvement count
    max_passes = int(nodes["pass_count"].max())
    min_passes_val = int(nodes["pass_count"].min())
    pass_range = max(max_passes - min_passes_val, 1)
    sizes = 80 + (nodes["pass_count"] - min_passes_val) / pass_range * 400

    pitch.scatter(
        nodes["avg_x"],
        nodes["avg_y"],
        s=sizes,
        color=_NETWORK_NODE_COLOR,
        edgecolors=PITCH_LINE_COLOR,
        linewidth=0.8,
        ax=ax,
        zorder=3,
    )

    # Player name labels
    for _, node in nodes.iterrows():
        ax.text(
            float(node["avg_x"]),
            float(node["avg_y"]) - 3.5,
            str(node["player_display_name"]),
            color="white",
            fontsize=7,
            ha="center",
            va="top",
            zorder=4,
        )

    ax.set_title("Pass Network", color=TEXT_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "pn_network")


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
        state.pn_pitch_image = ""
        state.pn_warning_text = ""
        state.pn_scope_label = ""
        state.pn_data_freshness = ""
        return

    # Scope label
    state.pn_scope_label = fetch_scope_label(comp_id, team_id)

    try:
        passes = _fetch_passes(comp_id, team_id, match_id)
    except Exception:
        logger.exception("Failed to fetch pass network data")
        state.pn_total_passes = "Error"
        state.pn_unique_connections = "Error"
        state.pn_top_pair_count = "--"
        state.pn_top_pair_names = ""
        state.pn_pitch_image = ""
        state.pn_data_freshness = ""
        return

    if passes.empty:
        state.pn_total_passes = "0"
        state.pn_unique_connections = "0"
        state.pn_top_pair_count = "0"
        state.pn_top_pair_names = "No data (Wyscout matches lack recipient data)"
        state.pn_pitch_image = ""
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
        state.pn_top_pair_count = fmt_int(int(top_edge["pair_count"]))
        state.pn_top_pair_names = f"{p_name} \u2192 {r_name}"
    else:
        state.pn_top_pair_count = "0"
        state.pn_top_pair_names = "No connections meet threshold"

    # Render pitch
    state.pn_pitch_image = _render_network(nodes, edges)

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
