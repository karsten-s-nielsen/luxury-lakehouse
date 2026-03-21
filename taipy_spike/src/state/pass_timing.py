"""Pass Timing (PAUSA) state module — all variables prefixed with pt_.

Manages page-specific filter cascade (match -> team -> player) from
fct_pausa_values_synced, computes summary metrics, and builds matplotlib
scatter + density heatmap figures saved as static PNGs.

Reference: Lee, Jo, Hong, Bauer & Ko (2026). "Valuing La Pausa:
Quantifying Optimal Pass Timing Beyond Speed." MIT Sloan 2026.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from cache import ttl_cache
from db import execute_query, t
from filters import fetch_pausa_matches, fetch_pausa_players, fetch_pausa_teams
from render import AMBER, GRAY, PITCH_BG_COLOR, TEXT_COLOR, chart_to_file

from state.shared import register_page_refresher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Page-specific filter state
# ---------------------------------------------------------------------------
pt_selected_match: str | None = None
pt_selected_team: str | None = None
pt_selected_player: str | None = None

pt_match_lov: list[str] = []
pt_team_lov: list[str] = []
pt_player_lov: list[str] = []

# Internal lookup maps (label -> id)
_pt_match_map: dict[str, str] = {}
_pt_team_map: dict[str, str] = {}
_pt_player_map: dict[str, str] = {}

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------
pt_avg_pausa: str = ""
pt_avg_temporal: str = ""
pt_avg_spatial: str = ""
pt_pass_count: str = ""

# ---------------------------------------------------------------------------
# Matplotlib chart image paths (static PNGs via chart_to_file)
# ---------------------------------------------------------------------------
pt_scatter_image: str = ""
pt_heatmap_image: str = ""

# ---------------------------------------------------------------------------
# Rankings data
# ---------------------------------------------------------------------------
pt_rankings_data: pd.DataFrame = pd.DataFrame()

# DFL identifier warning flag
pt_show_dfl_caption: bool = False

__all__ = [
    "pt_avg_pausa",
    "pt_avg_spatial",
    "pt_avg_temporal",
    "pt_heatmap_image",
    "pt_match_lov",
    "pt_on_match_change",
    "pt_on_player_change",
    "pt_on_team_change",
    "pt_pass_count",
    "pt_player_lov",
    "pt_rankings_data",
    "pt_scatter_image",
    "pt_selected_match",
    "pt_selected_player",
    "pt_selected_team",
    "pt_show_dfl_caption",
    "pt_team_lov",
]

# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------


def _get_pt_match_id(label: str | None) -> str | None:
    """Resolve PAUSA match label to match_id."""
    return _pt_match_map.get(label) if label else None  # type: ignore[arg-type]


def _get_pt_team(label: str | None) -> str | None:
    """Resolve PAUSA team label to team identifier."""
    if not label or label == "All":
        return None
    return _pt_team_map.get(label)  # type: ignore[arg-type]


def _get_pt_player_id(label: str | None) -> str | None:
    """Resolve PAUSA player label to player_id."""
    if not label or label == "All players":
        return None
    return _pt_player_map.get(label)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


@ttl_cache()
def _fetch_pausa_summary(match_id: str, team: str | None, player_id: str | None) -> pd.DataFrame:
    """Load aggregate PAUSA metrics for current filter selection."""
    pausa_tbl = t("fct_pausa_values_synced")
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
        f"FROM {pausa_tbl} WHERE {where}",
        tuple(params),
    )


@ttl_cache()
def _fetch_pausa_passes(match_id: str, team: str | None, player_id: str | None) -> pd.DataFrame:
    """Load individual pass PAUSA scores for scatter/heatmap (bounded)."""
    pausa_tbl = t("fct_pausa_values_synced")
    dim_tbl = t("dim_players_synced")
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
        f"FROM {pausa_tbl} pv "
        f"LEFT JOIN {dim_tbl} dp ON pv.player_id::text = dp.player_id::text "
        f"WHERE {where} "
        f"LIMIT 2000",
        tuple(params),
    )


@ttl_cache()
def _fetch_rankings() -> pd.DataFrame:
    """Load fct_pass_timing rankings (bounded)."""
    timing_tbl = t("fct_pass_timing_synced")
    match_tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT COALESCE(pt.player_display_name, pt.player_id) AS player_display_name, "  # noqa: S608
        f"  COALESCE(ms.match_date || ' \u2014 ' || ms.home_team_name || ' v ' || ms.away_team_name, "
        f"    'Match ' || pt.match_id) AS match_label, "
        f"  pt.pass_count, "
        f"  pt.avg_pausa, pt.avg_temporal_judgment, pt.avg_spatial_selection, "
        f"  pt.median_pausa, pt.passes_above_median_pausa "
        f"FROM {timing_tbl} pt "
        f"LEFT JOIN {match_tbl} ms ON pt.match_id::text = ms.match_id::text "
        f"ORDER BY pt.avg_pausa DESC "
        f"LIMIT 500",
    )


# ---------------------------------------------------------------------------
# Matplotlib chart builders
# ---------------------------------------------------------------------------

# Distinct team colors for scatter plot
_TEAM_COLORS = ["#e63946", "#457b9d", "#2a9d8f", "#e9c46a", "#264653"]


def _build_scatter_plot(df: pd.DataFrame) -> str:
    """Create temporal vs spatial scatter with PAUSA as bubble size + quadrant lines at 0.5.

    Returns the file path to the saved PNG.
    """
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    if not df.empty:
        # Map each unique team to a colour
        teams = df["team"].unique()
        team_color_map = {team: _TEAM_COLORS[i % len(_TEAM_COLORS)] for i, team in enumerate(teams)}

        # Scale bubble size: min 20, max 200 based on pausa_score
        sizes = df["pausa_score"].fillna(0.0).clip(lower=0.01)
        size_scaled = 20 + (sizes / max(sizes.max(), 0.01)) * 180

        for team in teams:
            mask = df["team"] == team
            ax.scatter(
                df.loc[mask, "temporal_judgment"],
                df.loc[mask, "spatial_selection"],
                s=size_scaled[mask],
                c=team_color_map[team],
                alpha=0.7,
                edgecolors="none",
                label=str(team),
            )

        ax.legend(
            loc="upper left",
            fontsize=8,
            facecolor=PITCH_BG_COLOR,
            edgecolor="#333355",
            labelcolor=TEXT_COLOR,
        )

    # Quadrant lines at 0.5
    ax.axhline(y=0.5, color=GRAY, linestyle="--", linewidth=0.8, alpha=0.5)
    ax.axvline(x=0.5, color=GRAY, linestyle="--", linewidth=0.8, alpha=0.5)

    # Quadrant text annotations
    _annotations = [
        (0.25, 0.75, "Good where,\npoor when"),
        (0.75, 0.75, "Good timing\n& target"),
        (0.25, 0.25, "Poor timing\n& target"),
        (0.75, 0.25, "Good when,\npoor where"),
    ]
    for x, y, text in _annotations:
        ax.text(x, y, text, ha="center", va="center", fontsize=8, color=GRAY, alpha=0.6)

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Temporal Judgment (when, 0\u20131, higher = better)", color=TEXT_COLOR, fontsize=10)
    ax.set_ylabel("Spatial Selection (where, 0\u20131, higher = better)", color=TEXT_COLOR, fontsize=10)
    ax.set_title(
        "Pass Timing: When vs Where (bubble size = PAUSA score)",
        color=TEXT_COLOR,
        fontsize=12,
        pad=10,
        fontweight="bold",
    )
    ax.tick_params(axis="both", colors=TEXT_COLOR, labelcolor=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()

    return chart_to_file(fig, "pt_scatter")


def _build_heatmap(df: pd.DataFrame) -> str:
    """Create OBSO receiver location density heatmap.

    Returns the file path to the saved PNG.
    """
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    valid = df.dropna(subset=["receiver_x", "receiver_y"]) if not df.empty else df
    has_data = not valid.empty and "receiver_x" in valid.columns

    if has_data:
        # Compute 2D histogram weighted by actual_obso for average OBSO per bin
        x = valid["receiver_x"].to_numpy(dtype=float)
        y = valid["receiver_y"].to_numpy(dtype=float)
        weights = valid["actual_obso"].fillna(0.0).to_numpy(dtype=float)

        x_edges = np.linspace(0, 120, 25)  # 24 bins
        y_edges = np.linspace(0, 80, 17)  # 16 bins

        # Sum of weights and counts per bin
        w_hist, _, _ = np.histogram2d(x, y, bins=[x_edges, y_edges], weights=weights)
        c_hist, _, _ = np.histogram2d(x, y, bins=[x_edges, y_edges])

        # Average OBSO per bin (avoid division by zero)
        with np.errstate(divide="ignore", invalid="ignore"):
            avg_obso = np.where(c_hist > 0, w_hist / c_hist, np.nan)

        # imshow expects (rows, cols) = (y, x), so transpose
        im = ax.imshow(
            avg_obso.T,
            origin="lower",
            extent=[0, 120, 0, 80],
            aspect="auto",
            cmap="YlOrRd",
            interpolation="bilinear",
            vmin=0,
            vmax=1,
        )

        cbar = fig.colorbar(im, ax=ax, pad=0.02, shrink=0.8)
        cbar.set_label("Avg OBSO (0\u20131, higher = more open space)", color=TEXT_COLOR, fontsize=9)
        cbar.ax.tick_params(labelcolor=TEXT_COLOR, colors=TEXT_COLOR)
    else:
        ax.text(
            60,
            40,
            "No receiver location data available",
            ha="center",
            va="center",
            fontsize=12,
            color=AMBER,
        )

    ax.set_xlim(0, 120)
    ax.set_ylim(0, 80)
    ax.set_xlabel("X (yards)", color=TEXT_COLOR, fontsize=10)
    ax.set_ylabel("Y (yards)", color=TEXT_COLOR, fontsize=10)
    ax.set_title(
        "OBSO at Receiver Location",
        color=TEXT_COLOR,
        fontsize=12,
        pad=10,
        fontweight="bold",
    )
    ax.tick_params(axis="both", colors=TEXT_COLOR, labelcolor=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")
    plt.tight_layout()

    return chart_to_file(fig, "pt_heatmap")


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def pt_on_match_change(state: Any, var_name: str, var_value: Any) -> None:
    """PAUSA match changed — reload teams and players, refresh data."""
    global _pt_team_map, _pt_player_map
    match_id = _get_pt_match_id(var_value)

    # Reset dependents
    state.pt_selected_team = None
    state.pt_selected_player = None

    if match_id is None:
        state.pt_team_lov = []
        state.pt_player_lov = []
        _pt_team_map = {}
        _pt_player_map = {}
        _clear_data(state)
        return

    try:
        teams = fetch_pausa_teams(match_id)
        _pt_team_map = {label: tid for label, tid in teams}
        state.pt_team_lov = ["All", *(label for label, _ in teams)]

        players = fetch_pausa_players(match_id, None)
        _pt_player_map = {label: pid for label, pid in players}
        state.pt_player_lov = ["All players", *(label for label, _ in players)]

        _refresh_data(state)
    except Exception:
        logger.exception("Failed on PAUSA match change")


def pt_on_team_change(state: Any, var_name: str, var_value: Any) -> None:
    """PAUSA team changed — reload players, refresh data."""
    global _pt_player_map
    match_id = _get_pt_match_id(state.pt_selected_match)
    team = _get_pt_team(var_value)

    state.pt_selected_player = None

    if match_id is None:
        return

    try:
        players = fetch_pausa_players(match_id, team)
        _pt_player_map = {label: pid for label, pid in players}
        state.pt_player_lov = ["All players", *(label for label, _ in players)]

        _refresh_data(state)
    except Exception:
        logger.exception("Failed on PAUSA team change")


def pt_on_player_change(state: Any, var_name: str, var_value: Any) -> None:
    """PAUSA player changed — refresh data."""
    _refresh_data(state)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _clear_data(state: Any) -> None:
    """Reset all data state to empty."""
    state.pt_avg_pausa = ""
    state.pt_avg_temporal = ""
    state.pt_avg_spatial = ""
    state.pt_pass_count = ""
    state.pt_scatter_image = ""
    state.pt_heatmap_image = ""
    state.pt_rankings_data = pd.DataFrame()
    state.pt_show_dfl_caption = False


def _refresh_data(state: Any) -> None:
    """Reload PAUSA data for current filter selection."""
    match_id = _get_pt_match_id(state.pt_selected_match)
    if match_id is None:
        _clear_data(state)
        return

    team = _get_pt_team(state.pt_selected_team)
    player_id = _get_pt_player_id(state.pt_selected_player)

    try:
        # Summary metrics
        summary_df = _fetch_pausa_summary(match_id, team, player_id)
        if summary_df.empty or summary_df.iloc[0]["avg_pausa"] is None:
            _clear_data(state)
            return

        row = summary_df.iloc[0]
        state.pt_avg_pausa = f"{float(row['avg_pausa']):.3f}"
        state.pt_avg_temporal = f"{float(row['avg_temporal']):.3f}"
        state.pt_avg_spatial = f"{float(row['avg_spatial']):.3f}"
        state.pt_pass_count = str(int(row["pass_count"]))

        # Individual pass data for charts
        passes_df = _fetch_pausa_passes(match_id, team, player_id)
        if passes_df.empty:
            state.pt_scatter_image = ""
            state.pt_heatmap_image = ""
        else:
            state.pt_scatter_image = _build_scatter_plot(passes_df)
            state.pt_heatmap_image = _build_heatmap(passes_df)

        # Rankings
        rankings_df = _fetch_rankings()
        if rankings_df.empty:
            state.pt_rankings_data = pd.DataFrame()
            state.pt_show_dfl_caption = False
        else:
            # Rename columns for display
            display_df = rankings_df.rename(
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
            )
            state.pt_rankings_data = display_df
            state.pt_show_dfl_caption = rankings_df["player_display_name"].str.startswith("DFL-OBJ-").any()

        logger.info(
            "PAUSA refreshed: match=%s, team=%s, player=%s, passes=%s",
            match_id,
            team,
            player_id,
            state.pt_pass_count,
        )
    except Exception:
        logger.exception("Failed to refresh PAUSA data")
        _clear_data(state)


# ---------------------------------------------------------------------------
# Page refresh entry point
# ---------------------------------------------------------------------------


def pt_refresh(state: Any) -> None:
    """Top-level refresh: reload match LOV and data.

    Called by shared._refresh_current_page when navigating to Pass-Timing.
    """
    global _pt_match_map
    try:
        matches = fetch_pausa_matches()
        _pt_match_map = {label: mid for label, mid in matches}
        state.pt_match_lov = [label for label, _ in matches]

        if matches and state.pt_selected_match is None:
            # Auto-select first match on initial load
            state.pt_selected_match = matches[0][0]
            pt_on_match_change(state, "pt_selected_match", matches[0][0])
        else:
            _refresh_data(state)
    except Exception:
        logger.exception("Failed to refresh PAUSA page")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Pass-Timing", pt_refresh)
