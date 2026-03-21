"""Movement Analysis state module — all variables prefixed with ma_.

THREE sub-views: Physical Performance, PPDA / Pressing Intensity, Off-Ball xT.
ma_refresh dispatches to the correct sub-view based on selected_sub_view.
Registered as the Movement page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from cache import ttl_cache
from db import execute_query, t
from render import GRAY, PITCH_BG_COLOR, TEXT_COLOR, chart_to_file

from state.shared import (
    get_comp_id,
    get_tracking_match_id,
    register_page_refresher,
)

matplotlib.use("Agg")
logger = logging.getLogger(__name__)

# ── Sub-view options ─────────────────────────────────────────────────────────
MA_SUB_VIEWS: list[str] = ["Physical Performance", "PPDA / Pressing Intensity", "Off-Ball xT"]

# ── Chart palette ────────────────────────────────────────────────────────────
_BAR_COLOR = "#2a9d8f"
_HOME_PPDA_COLOR = "#e63946"
_AWAY_PPDA_COLOR = "#457b9d"

# ── Exported state variables (all ma_ prefixed) ─────────────────────────────
# Physical Performance metrics
ma_phys_players: str = "--"
ma_phys_avg_dist: str = "--"
ma_phys_max_speed_kmh: str = "--"
ma_phys_max_speed_ms: str = "--"
ma_physical_image: str = ""

# PPDA metrics
ma_ppda_avg_home: str = "--"
ma_ppda_avg_away: str = "--"
ma_ppda_matches: str = "--"
ma_ppda_image: str = ""

# Off-Ball xT metrics
ma_oxt_players: str = "--"
ma_oxt_avg: str = "--"
ma_oxt_max: str = "--"
ma_oxt_image: str = ""

# Active sub-view tracking (for conditional rendering in page layout)
ma_active_view: str = "Physical Performance"

__all__ = [
    "ma_active_view",
    "ma_oxt_avg",
    "ma_oxt_image",
    "ma_oxt_max",
    "ma_oxt_players",
    "ma_phys_avg_dist",
    "ma_phys_max_speed_kmh",
    "ma_phys_max_speed_ms",
    "ma_phys_players",
    "ma_physical_image",
    "ma_ppda_avg_away",
    "ma_ppda_avg_home",
    "ma_ppda_image",
    "ma_ppda_matches",
    "ma_refresh",
]


# ── Data fetching ────────────────────────────────────────────────────────────


@ttl_cache()
def _fetch_physical_stats(match_id: str) -> pd.DataFrame:
    """Fetch physical stats for a tracking match, joined with player names."""
    tbl = t("fct_physical_stats_synced")
    dim = t("dim_players_synced")
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
        (str(match_id),),
    )


@ttl_cache()
def _fetch_ppda_data(competition_id: int) -> pd.DataFrame:
    """Fetch PPDA data for a competition from match summary."""
    tbl = t("fct_match_summary_synced")
    return execute_query(
        f"SELECT match_id, match_date, home_team_name, away_team_name, "  # noqa: S608
        f"  home_ppda, away_ppda, home_possession_pct "
        f"FROM {tbl} "
        f"WHERE competition_id = %s AND home_ppda IS NOT NULL "
        f"ORDER BY match_date LIMIT 500",
        (int(competition_id),),
    )


# ── Chart rendering ──────────────────────────────────────────────────────────


def _render_physical_bars(
    data: pd.DataFrame,
    metric: str,
    label: str,
    title: str,
) -> str:
    """Render horizontal bar chart of a physical metric per player. Return file path."""
    display_data = data.head(20) if len(data) > 20 else data
    n = len(display_data) if not display_data.empty else 1
    fig, ax = plt.subplots(figsize=(8, max(2, min(n * 0.22, 6))), dpi=72)
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    if not display_data.empty and metric in display_data.columns:
        sorted_df = display_data.sort_values(metric, ascending=True)
        player_labels = (
            sorted_df["player_name"].astype(str)
            if "player_name" in sorted_df.columns
            else sorted_df["player_id"].astype(str)
        )
        values = sorted_df[metric].astype(float)
        ax.barh(player_labels, values, color=_BAR_COLOR, alpha=0.85, height=0.6)

    if len(data) > 20:
        ax.annotate(
            f"Showing top 20 of {len(data)} players",
            xy=(0.5, -0.06),
            xycoords="axes fraction",
            ha="center",
            fontsize=7,
            color=GRAY,
        )

    ax.set_xlabel(label, color=TEXT_COLOR, fontsize=8)
    ax.set_title(title, color=TEXT_COLOR, fontsize=10, pad=6, fontweight="bold")
    ax.tick_params(axis="both", colors=TEXT_COLOR, labelcolor=TEXT_COLOR, labelsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")

    return chart_to_file(fig, f"ma_{metric}")


def _render_ppda_bars(data: pd.DataFrame, title: str = "PPDA by Match") -> str:
    """Render grouped horizontal bar chart of home vs away PPDA. Return file path."""
    plot_data = data.tail(25) if len(data) > 25 else data

    fig, ax = plt.subplots(figsize=(12, max(4, min(len(plot_data) * 0.5, 14))))
    fig.set_facecolor(PITCH_BG_COLOR)
    ax.set_facecolor(PITCH_BG_COLOR)

    if not plot_data.empty and "home_ppda" in plot_data.columns and "away_ppda" in plot_data.columns:
        labels = [
            f"{row.get('home_team_name', 'Home')} v {row.get('away_team_name', 'Away')}"
            for _, row in plot_data.iterrows()
        ]
        y_pos = np.arange(len(labels))
        home_vals = plot_data["home_ppda"].fillna(0).astype(float)
        away_vals = plot_data["away_ppda"].fillna(0).astype(float)

        ax.barh(y_pos + 0.15, home_vals, height=0.3, color=_HOME_PPDA_COLOR, alpha=0.85, label="Home PPDA")
        ax.barh(y_pos - 0.15, away_vals, height=0.3, color=_AWAY_PPDA_COLOR, alpha=0.85, label="Away PPDA")

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, color=TEXT_COLOR, fontsize=9)
        ax.legend(loc="upper right", facecolor=PITCH_BG_COLOR, edgecolor="#333355", labelcolor=TEXT_COLOR)

        if len(data) > 25:
            ax.annotate(
                f"Showing last 25 of {len(data)} matches",
                xy=(0.5, -0.04),
                xycoords="axes fraction",
                ha="center",
                fontsize=8,
                color=GRAY,
            )

    ax.set_xlabel("PPDA (lower = more aggressive press)", color=TEXT_COLOR, fontsize=11)
    ax.set_title(title, color=TEXT_COLOR, fontsize=14, pad=10, fontweight="bold")
    ax.tick_params(axis="x", colors=TEXT_COLOR)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_color("#333355")
    ax.spines["left"].set_color("#333355")

    return chart_to_file(fig, "ma_ppda")


# ── Sub-view refresh helpers ────────────────────────────────────────────────


def _refresh_physical(state: Any) -> None:
    """Refresh Physical Performance sub-view from fct_physical_stats_synced."""
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        state.ma_phys_players = "--"
        state.ma_phys_avg_dist = "--"
        state.ma_phys_max_speed_kmh = "--"
        state.ma_phys_max_speed_ms = "--"
        state.ma_physical_image = ""
        return

    stats = _fetch_physical_stats(match_id)
    if stats.empty:
        state.ma_phys_players = "0"
        state.ma_phys_avg_dist = "0.0"
        state.ma_phys_max_speed_kmh = "0.0"
        state.ma_phys_max_speed_ms = "0.0"
        state.ma_physical_image = ""
        return

    # Metrics
    state.ma_phys_players = str(len(stats))
    state.ma_phys_avg_dist = f"{stats['total_distance_km'].mean():.1f}"
    state.ma_phys_max_speed_kmh = f"{stats['max_speed_ms'].max() * 3.6:.1f}"
    state.ma_phys_max_speed_ms = f"{stats['max_speed_ms'].max():.1f}"

    # Default metric: total_distance_km
    state.ma_physical_image = _render_physical_bars(
        stats, "total_distance_km", "Distance (km)", "Total Distance by Player"
    )

    logger.info("Physical performance refreshed: %d players", len(stats))


def _refresh_ppda(state: Any) -> None:
    """Refresh PPDA sub-view from fct_match_summary_synced."""
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        state.ma_ppda_avg_home = "--"
        state.ma_ppda_avg_away = "--"
        state.ma_ppda_matches = "--"
        state.ma_ppda_image = ""
        return

    data = _fetch_ppda_data(comp_id)
    if data.empty:
        state.ma_ppda_avg_home = "0.0"
        state.ma_ppda_avg_away = "0.0"
        state.ma_ppda_matches = "0"
        state.ma_ppda_image = ""
        return

    # Metrics
    state.ma_ppda_avg_home = f"{float(data['home_ppda'].mean()):.1f}"
    state.ma_ppda_avg_away = f"{float(data['away_ppda'].mean()):.1f}"
    state.ma_ppda_matches = str(len(data))

    state.ma_ppda_image = _render_ppda_bars(data, title="PPDA by Match")

    logger.info("PPDA refreshed: %d matches", len(data))


def _refresh_off_ball_xt(state: Any) -> None:
    """Refresh Off-Ball xT sub-view — same source as Physical, filtered to non-null xT."""
    match_id = get_tracking_match_id(state.selected_tracking_match)
    if match_id is None:
        state.ma_oxt_players = "--"
        state.ma_oxt_avg = "--"
        state.ma_oxt_max = "--"
        state.ma_oxt_image = ""
        return

    stats = _fetch_physical_stats(match_id)
    xt_stats = stats[stats["total_off_ball_xt"].notna()] if not stats.empty else stats

    if xt_stats.empty:
        state.ma_oxt_players = "0"
        state.ma_oxt_avg = "0.000"
        state.ma_oxt_max = "0.000"
        state.ma_oxt_image = ""
        return

    # Metrics
    state.ma_oxt_players = str(len(xt_stats))
    state.ma_oxt_avg = f"{xt_stats['total_off_ball_xt'].mean():.3f}"
    state.ma_oxt_max = f"{xt_stats['total_off_ball_xt'].max():.3f}"

    state.ma_oxt_image = _render_physical_bars(
        xt_stats, "total_off_ball_xt", "Total Off-Ball xT", "Off-Ball xT by Player"
    )

    logger.info("Off-Ball xT refreshed: %d players with xT data", len(xt_stats))


# ── Main refresh dispatcher ────────────────────────────────────────────────


def ma_refresh(state: Any) -> None:
    """Dispatch to the correct sub-view refresh based on selected_sub_view."""
    # Claim this page's sub-view LOV and reset selection if stale (shared state fix)
    if not getattr(state, "sub_view_lov", None) or state.sub_view_lov != MA_SUB_VIEWS:
        state.sub_view_lov = MA_SUB_VIEWS
    if not state.selected_sub_view or state.selected_sub_view not in MA_SUB_VIEWS:
        state.selected_sub_view = MA_SUB_VIEWS[0]

    view = state.selected_sub_view
    state.ma_active_view = view

    if view == "Physical Performance":
        _refresh_physical(state)
    elif view == "PPDA / Pressing Intensity":
        _refresh_ppda(state)
    elif view == "Off-Ball xT":
        _refresh_off_ball_xt(state)
    else:
        logger.warning("Unknown Movement sub-view: %r", view)
        _refresh_physical(state)


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Movement-Pressing", ma_refresh)
