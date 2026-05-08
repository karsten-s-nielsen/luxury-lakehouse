"""Heat Map state — hm_ prefixed variables, server-side aggregation, bubble maps.

Spatial analysis approach per Anzer & Bauer (2021). Server-side binning returns
~96 rows instead of 500K+ individual actions.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from filters import build_scope_label_plain, build_warning, fetch_data_freshness
from matplotlib.colors import Normalize
from mplsoccer import Pitch
from queries.tracking import fetch_heatmap_actions
from render import AMBER, PITCH_BG_COLOR, PITCH_LINE_COLOR, fmt_int, pitch_to_file

# matplotlib.use("Agg") is handled by render.py at module load (imported above) — no redundant call needed here.
from state.shared import _ALL_LABEL, get_comp_id, get_match_key, get_player_id, get_team_id, register_page_refresher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exported state variables (all hm_ prefixed)
# ---------------------------------------------------------------------------
hm_total: str = "--"
hm_passes: str = "--"
hm_shots: str = "--"
hm_pass_bubbles: str = ""
hm_shot_bubbles: str = ""
hm_pass_focus: str = ""
hm_shot_focus: str = ""

hm_warning_text: str = ""
hm_scope_comp: str = ""
hm_scope_team: str = ""
hm_scope_player: str = ""
hm_scope_coverage: str = ""
hm_data_freshness: str = ""

hm_pass_bubbles_alt: str = ""
hm_shot_bubbles_alt: str = ""
hm_pass_focus_alt: str = ""
hm_shot_focus_alt: str = ""

__all__ = [
    "hm_data_freshness",
    "hm_pass_bubbles",
    "hm_pass_bubbles_alt",
    "hm_pass_focus",
    "hm_pass_focus_alt",
    "hm_passes",
    "hm_refresh",
    "hm_scope_comp",
    "hm_scope_coverage",
    "hm_scope_player",
    "hm_scope_team",
    "hm_shot_bubbles",
    "hm_shot_bubbles_alt",
    "hm_shot_focus",
    "hm_shot_focus_alt",
    "hm_shots",
    "hm_total",
    "hm_warning_text",
]


# ---------------------------------------------------------------------------
# Pitch rendering
# ---------------------------------------------------------------------------

_BIN_GRID = (12, 8)
_MAX_BUBBLE_SIZE = 500  # max scatter area in pt²
_FIGSIZE = (10, 7)
_TOP_N_LABELS = (
    25  # show count labels on the N largest bins. User-confirmed value; do not
    # reduce without explicit approval (the 12 → 25 restore happened 2026-04-24
    # after an unapproved regression).
)


def _compute_bin_grid(
    actions: pd.DataFrame,
    pitch: Pitch,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (values, cx, cy) arrays of non-zero bins only."""
    counts = actions["cnt"].astype(int).to_numpy()
    expanded_x = np.repeat(actions["x"].to_numpy(dtype=float), counts)
    expanded_y = np.repeat(actions["y"].to_numpy(dtype=float), counts)
    bin_stats = pitch.bin_statistic(expanded_x, expanded_y, statistic="count", bins=_BIN_GRID)
    stat_flat: np.ndarray = bin_stats["statistic"].flatten()
    cx_flat: np.ndarray = bin_stats["cx"].flatten()
    cy_flat: np.ndarray = bin_stats["cy"].flatten()
    mask = stat_flat > 0
    return stat_flat[mask], cx_flat[mask], cy_flat[mask]


def _add_colorbar(
    fig: Any,
    ax: Any,
    cmap: Any,
    norm: Normalize,
    label: str,
) -> None:
    """Add a styled colorbar to a bubble map figure."""
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label(label, color=PITCH_LINE_COLOR, fontsize=10)
    cbar.ax.yaxis.set_tick_params(color=PITCH_LINE_COLOR)
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color=PITCH_LINE_COLOR)


def _render_bubble_map(
    actions: pd.DataFrame,
    title: str,
    cmap_name: str,
    count_label: str,
) -> str:
    """Render area-encoded bubble map at bin centers — ColorBrewer palette."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=_FIGSIZE)
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if actions.empty:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_map")

    values, cx, cy = _compute_bin_grid(actions, pitch)

    if len(values) == 0:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_map")

    sizes = (values / values.max()) * _MAX_BUBBLE_SIZE
    cmap = plt.get_cmap(cmap_name)
    norm = Normalize(vmin=0, vmax=float(values.max()))
    colors = cmap(norm(values))

    ax.scatter(cx, cy, s=sizes, c=colors, alpha=0.85, edgecolors="none", zorder=2)

    # Count labels on top-N bins
    top_n = min(_TOP_N_LABELS, len(values))
    label_indices = np.argsort(values)[-top_n:]
    for idx in label_indices:
        ax.annotate(
            str(int(values[idx])),
            (cx[idx], cy[idx]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            color="white",
            alpha=0.9,
            zorder=4,
        )

    _add_colorbar(fig, ax, cmap, norm, count_label)

    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "bubble_map")


def _render_bubble_focus_map(
    actions: pd.DataFrame,
    title: str,
    cmap_name: str,
    count_label: str,
) -> str:
    """Bubble map with top-5 bins highlighted — gold ring + count annotation."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=_FIGSIZE)
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if actions.empty:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_focus")

    values, cx, cy = _compute_bin_grid(actions, pitch)

    if len(values) == 0:
        ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "bubble_focus")

    sizes = (values / values.max()) * _MAX_BUBBLE_SIZE
    cmap = plt.get_cmap(cmap_name)
    norm = Normalize(vmin=0, vmax=float(values.max()))
    colors = cmap(norm(values))

    # Top-5 bin indices (or fewer if <5 non-zero bins)
    top_k = min(5, len(values))
    top_indices = np.argsort(values)[-top_k:]
    muted_mask = np.ones(len(values), dtype=bool)
    muted_mask[top_indices] = False

    # Muted background bubbles
    if muted_mask.any():
        ax.scatter(
            cx[muted_mask],
            cy[muted_mask],
            s=sizes[muted_mask],
            c=colors[muted_mask],
            alpha=0.25,
            edgecolors="none",
            zorder=2,
        )

    # Highlighted top-5 bubbles with gold ring
    ax.scatter(
        cx[top_indices],
        cy[top_indices],
        s=sizes[top_indices],
        c=colors[top_indices],
        alpha=0.85,
        edgecolors=AMBER,
        linewidths=2,
        zorder=3,
    )

    # Count annotations above each highlighted bubble
    for idx in top_indices:
        ax.annotate(
            str(int(values[idx])),
            (cx[idx], cy[idx]),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=9,
            fontweight="bold",
            color="white",
            zorder=4,
        )

    _add_colorbar(fig, ax, cmap, norm, count_label)

    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "bubble_focus")


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------


def _compute_metrics(actions: pd.DataFrame) -> dict[str, str]:
    """Compute heat map metrics from aggregated action data."""
    if actions.empty:
        return {"total": "0", "passes": "0", "shots": "0"}

    counts = actions["cnt"].astype(int)
    total = int(counts.sum())
    passes = int(actions.loc[actions["action_type"] == "pass", "cnt"].sum())
    shots = int(actions.loc[actions["action_type"] == "shot", "cnt"].sum())

    return {"total": fmt_int(total), "passes": fmt_int(passes), "shots": fmt_int(shots)}


# ---------------------------------------------------------------------------
# Refresh callback
# ---------------------------------------------------------------------------


def hm_refresh(state: Any) -> None:
    """Fetch aggregated actions, compute metrics, render heatmap.

    Competition is required; team, player, and match are optional filters.
    """
    comp_id = get_comp_id(state.selected_competition)

    def _clear_all() -> None:
        state.hm_total = "--"
        state.hm_passes = "--"
        state.hm_shots = "--"
        state.hm_pass_bubbles = ""
        state.hm_shot_bubbles = ""
        state.hm_pass_focus = ""
        state.hm_shot_focus = ""
        state.hm_warning_text = ""
        state.hm_scope_comp = ""
        state.hm_scope_team = ""
        state.hm_scope_player = ""
        state.hm_scope_coverage = ""
        state.hm_data_freshness = ""
        state.hm_pass_bubbles_alt = ""
        state.hm_shot_bubbles_alt = ""
        state.hm_pass_focus_alt = ""
        state.hm_shot_focus_alt = ""

    if comp_id is None:
        _clear_all()
        return

    team_id = get_team_id(state.selected_team)
    player_id = get_player_id(state.selected_player)
    match_key = get_match_key(state.selected_match)

    # Resolve display labels for scope line (dimensions the page filters by)
    comp_label = state.selected_competition or ""
    team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    player_label = state.selected_player if state.selected_player not in (None, _ALL_LABEL) else "All players"

    state.hm_scope_comp = comp_label
    state.hm_scope_team = team_label
    state.hm_scope_player = player_label
    scope_plain = build_scope_label_plain([("Competition", comp_label), ("Team", team_label), ("Player", player_label)])

    try:
        actions = fetch_heatmap_actions(comp_id, team_id, player_id, match_key)
    except Exception:
        logger.exception("Failed to fetch heatmap actions for comp=%d", comp_id)
        state.hm_pass_bubbles = ""
        state.hm_shot_bubbles = ""
        state.hm_pass_focus = ""
        state.hm_shot_focus = ""
        state.hm_scope_coverage = ""
        state.hm_data_freshness = ""
        return

    if actions.empty:
        state.hm_total = "0"
        state.hm_passes = "0"
        state.hm_shots = "0"
        state.hm_pass_bubbles = ""
        state.hm_shot_bubbles = ""
        state.hm_pass_focus = ""
        state.hm_shot_focus = ""
        state.hm_warning_text = build_warning(
            domain="actions",
            suggestions=["removing the team filter", "choosing a different player"],
        )
        state.hm_scope_coverage = ""
        state.hm_data_freshness = ""
        return

    state.hm_warning_text = ""
    metrics = _compute_metrics(actions)
    state.hm_total = metrics["total"]
    state.hm_passes = metrics["passes"]
    state.hm_shots = metrics["shots"]

    # Coverage context for EID — n_matches comes from the query's scalar subquery / CTE
    n_matches = int(actions["n_matches"].iloc[0]) if "n_matches" in actions.columns and not actions.empty else 0
    state.hm_scope_coverage = (
        f"{metrics['total']} actions across {n_matches} match{'es' if n_matches != 1 else ''}" if n_matches > 0 else ""
    )

    # Alt strings (scope-aware) for each of the 4 images
    state.hm_pass_bubbles_alt = f"Pass Distribution — {scope_plain}"
    state.hm_shot_bubbles_alt = f"Shot Distribution — {scope_plain}"
    state.hm_pass_focus_alt = f"Pass Hotspots (Top 5) — {scope_plain}"
    state.hm_shot_focus_alt = f"Shot Hotspots (Top 5) — {scope_plain}"

    # Split by action type — query already returns action_type column
    pass_actions = actions.loc[actions["action_type"] == "pass"]
    shot_actions = actions.loc[actions["action_type"] == "shot"]

    # Row 1 — Combo A: split bubble maps (exploration view)
    state.hm_pass_bubbles = _render_bubble_map(pass_actions, "Pass Distribution", "Blues", "Pass Count")
    state.hm_shot_bubbles = _render_bubble_map(shot_actions, "Shot Distribution", "OrRd", "Shot Count")

    # Row 2 — Combo C: split bubble maps with focus (coaching view)
    state.hm_pass_focus = _render_bubble_focus_map(pass_actions, "Pass Hotspots (Top 5)", "Blues", "Pass Count")
    state.hm_shot_focus = _render_bubble_focus_map(shot_actions, "Shot Hotspots (Top 5)", "OrRd", "Shot Count")

    # Data freshness
    state.hm_data_freshness = fetch_data_freshness()

    logger.info("Heat map rendered: %s total actions", metrics["total"])


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Heat-Map", hm_refresh)
