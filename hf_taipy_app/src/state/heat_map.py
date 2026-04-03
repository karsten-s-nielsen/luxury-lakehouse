"""Heat Map state — hm_ prefixed variables, server-side aggregation, zone classification.

Spatial analysis approach per Anzer & Bauer (2021). Server-side binning returns
~96 rows instead of 500K+ individual actions.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import numpy as np
import pandas as pd
from filters import fetch_data_freshness, fetch_scope_label
from mplsoccer import Pitch
from queries.tracking import fetch_heatmap_actions
from render import PITCH_BG_COLOR, PITCH_LINE_COLOR, fmt_int, pitch_to_file

from state.shared import get_comp_id, get_match_id, get_player_id, get_team_id, register_page_refresher

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exported state variables (all hm_ prefixed)
# ---------------------------------------------------------------------------
hm_total: str = "--"
hm_passes: str = "--"
hm_shots: str = "--"
hm_most_active_zone: str = "--"
hm_pitch_image: str = ""

hm_warning_text: str = ""
hm_scope_label: str = ""
hm_data_freshness: str = ""

__all__ = [
    "hm_data_freshness",
    "hm_most_active_zone",
    "hm_passes",
    "hm_pitch_image",
    "hm_refresh",
    "hm_scope_label",
    "hm_shots",
    "hm_total",
    "hm_warning_text",
]


# ---------------------------------------------------------------------------
# Zone classification — 3x3 grid on StatsBomb 120x80
# ---------------------------------------------------------------------------


def _classify_zone(x: float, y: float) -> str:
    """Classify a pitch location into a 3x3 zone grid.

    X-axis: Def (<40), Mid (40-80), Att (>80) — direction of attack.
    Y-axis: Right (<26.7), Center (26.7-53.3), Left (>53.3) — from TV view.
    """
    if x < 40:
        x_zone = "Def"
    elif x < 80:
        x_zone = "Mid"
    else:
        x_zone = "Att"

    if y < 80 / 3:
        y_zone = "Right"
    elif y < 2 * 80 / 3:
        y_zone = "Center"
    else:
        y_zone = "Left"

    return f"{x_zone} {y_zone}"


# ---------------------------------------------------------------------------
# Pitch rendering
# ---------------------------------------------------------------------------


def _render_heatmap(actions: pd.DataFrame) -> str:
    """Render action density heatmap to temp PNG via mplsoccer bin_statistic."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if actions.empty:
        ax.set_title("Action Density Heat Map", color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "pitch_heat_map")

    # Expand pre-aggregated rows for bin_statistic
    counts = actions["cnt"].astype(int).values
    expanded_x = np.repeat(actions["x"].values, counts)
    expanded_y = np.repeat(actions["y"].values, counts)

    bin_stats = pitch.bin_statistic(expanded_x, expanded_y, statistic="count", bins=(12, 8))
    pitch.heatmap(bin_stats, ax=ax, cmap="hot", edgecolors=PITCH_BG_COLOR)

    ax.set_title("Action Density Heat Map", color=PITCH_LINE_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "pitch_heat_map")


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------


def _compute_metrics(actions: pd.DataFrame) -> dict[str, str]:
    """Compute heat map metrics from aggregated action data."""
    if actions.empty:
        return {
            "total": "0",
            "passes": "0",
            "shots": "0",
            "most_active_zone": "--",
        }

    counts = actions["cnt"].astype(int)
    total = int(counts.sum())
    passes = int(actions.loc[actions["action_type"] == "pass", "cnt"].sum())
    shots = int(actions.loc[actions["action_type"] == "shot", "cnt"].sum())

    # Most active zone — 3x3 grid classification
    actions = actions.copy()
    actions["zone"] = actions.apply(lambda r: _classify_zone(float(r["x"]), float(r["y"])), axis=1)
    zone_counts = actions.groupby("zone")["cnt"].sum()
    most_active = str(zone_counts.idxmax()) if not zone_counts.empty else "--"

    return {
        "total": fmt_int(total),
        "passes": fmt_int(passes),
        "shots": fmt_int(shots),
        "most_active_zone": most_active,
    }


# ---------------------------------------------------------------------------
# Refresh callback
# ---------------------------------------------------------------------------


def hm_refresh(state: Any) -> None:
    """Fetch aggregated actions, compute metrics, render heatmap.

    Competition is required; team, player, and match are optional filters.
    """
    comp_id = get_comp_id(state.selected_competition)

    if comp_id is None:
        state.hm_total = "--"
        state.hm_passes = "--"
        state.hm_shots = "--"
        state.hm_most_active_zone = "--"
        state.hm_pitch_image = ""
        state.hm_warning_text = ""
        state.hm_scope_label = ""
        state.hm_data_freshness = ""
        return

    team_id = get_team_id(state.selected_team)
    player_id = get_player_id(state.selected_player)
    match_id = get_match_id(state.selected_match)

    # Scope label
    state.hm_scope_label = fetch_scope_label(comp_id, team_id)

    try:
        actions = fetch_heatmap_actions(comp_id, team_id, player_id, match_id)
    except Exception:
        logger.exception("Failed to fetch heatmap actions for comp=%d", comp_id)
        state.hm_pitch_image = ""
        state.hm_data_freshness = ""
        return

    if actions.empty:
        state.hm_total = "0"
        state.hm_passes = "0"
        state.hm_shots = "0"
        state.hm_most_active_zone = "--"
        state.hm_pitch_image = ""
        state.hm_warning_text = "No actions for the selected filters."
        state.hm_data_freshness = ""
        return

    state.hm_warning_text = ""
    metrics = _compute_metrics(actions)
    state.hm_total = metrics["total"]
    state.hm_passes = metrics["passes"]
    state.hm_shots = metrics["shots"]
    state.hm_most_active_zone = metrics["most_active_zone"]

    state.hm_pitch_image = _render_heatmap(actions)

    # Data freshness
    state.hm_data_freshness = fetch_data_freshness()

    logger.info("Heat map rendered: %s total actions", metrics["total"])


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Heat-Map", hm_refresh)
