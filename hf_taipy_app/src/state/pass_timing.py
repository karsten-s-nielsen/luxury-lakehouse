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

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from cache import ttl_cache
from db import execute_query, t
from filters import fetch_data_freshness, fetch_pausa_matches, fetch_pausa_players, fetch_pausa_teams

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
# Interactive Plotly chart figures
# ---------------------------------------------------------------------------
pt_scatter_figure: go.Figure | None = None
pt_heatmap_figure: go.Figure | None = None

# ---------------------------------------------------------------------------
# Rankings data
# ---------------------------------------------------------------------------
pt_rankings_data: pd.DataFrame = pd.DataFrame(
    columns=["Player", "Match", "Passes", "Avg PAUSA", "Avg Temporal", "Avg Spatial", "Median PAUSA", "Above Median"]
)

# ---------------------------------------------------------------------------
# Aggregate rankings + activity filter state
# ---------------------------------------------------------------------------
pt_min_passes_with_value: int = 50
pt_min_minutes: int = 0
pt_per_match_min_passes: int = 5
pt_aggregate_rankings_data: pd.DataFrame = pd.DataFrame()

# DFL identifier warning
pt_show_dfl_caption: bool = False
pt_dfl_caption: str = "Player names shown as DFL identifiers \u2014 IDSSE tracking data does not include player names. Human-readable names require a DFL roster lookup (not yet available)."

pt_data_freshness: str = ""

pt_warning_text: str = ""
pt_footer_text: str = ""

__all__ = [
    "pt_aggregate_rankings_data",
    "pt_avg_pausa",
    "pt_avg_spatial",
    "pt_data_freshness",
    "pt_avg_temporal",
    "pt_footer_text",
    "pt_heatmap_figure",
    "pt_match_lov",
    "pt_min_minutes",
    "pt_min_passes_with_value",
    "pt_on_match_change",
    "pt_on_min_minutes_change",
    "pt_on_min_passes_change",
    "pt_on_per_match_min_passes_change",
    "pt_on_player_change",
    "pt_on_team_change",
    "pt_pass_count",
    "pt_per_match_min_passes",
    "pt_player_lov",
    "pt_rankings_data",
    "pt_scatter_figure",
    "pt_selected_match",
    "pt_selected_player",
    "pt_selected_team",
    "pt_dfl_caption",
    "pt_show_dfl_caption",
    "pt_team_lov",
    "pt_warning_text",
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


@ttl_cache(ttl=600)
def _fetch_aggregate_rankings() -> pd.DataFrame:
    """Load fct_pausa_rankings (player-level aggregate, bounded)."""
    rankings_tbl = t("fct_pausa_rankings_synced")
    return execute_query(
        f"SELECT player_display_name, total_matches, total_passes, "  # noqa: S608
        f"  passes_with_value, avg_pausa, avg_temporal_judgment, "
        f"  avg_spatial_selection, median_pausa, total_minutes "
        f"FROM {rankings_tbl} "
        f"ORDER BY avg_pausa DESC "
        f"LIMIT 500",
    )


# ---------------------------------------------------------------------------
# Plotly chart builders
# ---------------------------------------------------------------------------


def _build_scatter_figure(df: pd.DataFrame) -> go.Figure | None:
    """Build interactive Plotly scatter figure: When vs Where."""
    if df.empty:
        return None
    fig = px.scatter(
        df,
        x="temporal_judgment",
        y="spatial_selection",
        size="pausa_score",
        color="team",
        hover_data={"temporal_judgment": ":.3f", "spatial_selection": ":.3f", "pausa_score": ":.3f"},
        title="Pass Timing: When vs Where (bubble size = PAUSA score)",
        labels={
            "temporal_judgment": "Temporal Judgment (when, 0\u20131, higher = better)",
            "spatial_selection": "Spatial Selection (where, 0\u20131, higher = better)",
        },
    )
    fig.update_layout(
        xaxis_range=[0, 1],
        yaxis_range=[0, 1],
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
        height=450,
    )
    # Quadrant annotations
    fig.add_annotation(
        x=0.25, y=0.75, text="Good timing,<br>wrong target", showarrow=False, font=dict(color="gray", size=10)
    )
    fig.add_annotation(
        x=0.75, y=0.75, text="Right time,<br>right place", showarrow=False, font=dict(color="gray", size=10)
    )
    fig.add_annotation(
        x=0.25, y=0.25, text="Poor timing,<br>wrong target", showarrow=False, font=dict(color="gray", size=10)
    )
    fig.add_annotation(
        x=0.75, y=0.25, text="Poor timing,<br>good target", showarrow=False, font=dict(color="gray", size=10)
    )
    return fig


def _build_heatmap_figure(df: pd.DataFrame) -> go.Figure | None:
    """Build interactive Plotly OBSO heatmap figure."""
    if df.empty:
        return None
    valid = df.dropna(subset=["receiver_x", "receiver_y"])
    if valid.empty:
        return None
    fig = px.density_heatmap(
        valid,
        x="receiver_x",
        y="receiver_y",
        z="actual_obso",
        histfunc="avg",
        nbinsx=24,
        nbinsy=16,
        color_continuous_scale="YlOrRd",
        title="OBSO at Receiver Location",
        labels={"actual_obso": "Avg OBSO (0\u20131, higher = more open space)"},
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
        height=450,
        xaxis_title="Pitch X (m)",
        yaxis_title="Pitch Y (m)",
    )
    return fig


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


def pt_on_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter aggregate rankings when slider changes."""
    _refresh_data(state)


def pt_on_min_minutes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter aggregate rankings when minutes slider changes."""
    _refresh_data(state)


def pt_on_per_match_min_passes_change(state: Any, var_name: str, var_value: Any) -> None:
    """Refilter per-match rankings when slider changes."""
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
    state.pt_scatter_figure = None
    state.pt_heatmap_figure = None
    state.pt_rankings_data = pd.DataFrame(
        columns=[
            "Player",
            "Match",
            "Passes",
            "Avg PAUSA",
            "Avg Temporal",
            "Avg Spatial",
            "Median PAUSA",
            "Above Median",
        ]
    )
    state.pt_show_dfl_caption = False
    state.pt_warning_text = ""
    state.pt_footer_text = ""


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
            state.pt_warning_text = (
                "No PAUSA data for the selected filters. Try a different match or remove team/player filters."
            )
            return

        row = summary_df.iloc[0]
        state.pt_avg_pausa = f"{float(row['avg_pausa']):.3f}"
        state.pt_avg_temporal = f"{float(row['avg_temporal']):.3f}"
        state.pt_avg_spatial = f"{float(row['avg_spatial']):.3f}"
        state.pt_pass_count = str(int(row["pass_count"]))

        # Individual pass data for charts
        passes_df = _fetch_pausa_passes(match_id, team, player_id)
        if passes_df.empty:
            state.pt_scatter_figure = None
            state.pt_heatmap_figure = None
        else:
            state.pt_scatter_figure = _build_scatter_figure(passes_df)
            state.pt_heatmap_figure = _build_heatmap_figure(passes_df)

        # Rankings (per-match) with activity filter
        rankings_df = _fetch_rankings()
        if not rankings_df.empty:
            rankings_df = rankings_df[rankings_df["pass_count"] >= state.pt_per_match_min_passes].reset_index(drop=True)
        if rankings_df.empty:
            state.pt_rankings_data = pd.DataFrame(
                columns=[
                    "Player",
                    "Match",
                    "Passes",
                    "Avg PAUSA",
                    "Avg Temporal",
                    "Avg Spatial",
                    "Median PAUSA",
                    "Above Median",
                ]
            )
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
            # Round numeric columns for display (avoid 18-decimal precision)
            for col in ["Avg PAUSA", "Avg Temporal", "Avg Spatial", "Median PAUSA"]:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(3)
            state.pt_rankings_data = display_df
            state.pt_show_dfl_caption = rankings_df["player_display_name"].str.startswith("DFL-OBJ-").any()

        # Aggregate rankings with activity filter
        agg_df = _fetch_aggregate_rankings()
        if not agg_df.empty:
            mask = agg_df["passes_with_value"] >= state.pt_min_passes_with_value
            if state.pt_min_minutes > 0:
                mask = mask & (agg_df["total_minutes"].fillna(0) >= state.pt_min_minutes)
            state.pt_aggregate_rankings_data = agg_df[mask].reset_index(drop=True)
        else:
            state.pt_aggregate_rankings_data = agg_df

        # Successful load — set footer, clear warning
        state.pt_warning_text = ""
        state.pt_footer_text = (
            "Lee, Jo, Hong, Bauer & Ko (2026). Valuing La Pausa: Quantifying Optimal Pass Timing "
            "Beyond Speed. MIT Sloan 2026. OBSO: Spearman (2018), Fernandez & Bornn (2018). "
            "Event-tracking sync: Kim et al. (2025) ELASTIC. IDSSE Bundesliga \u00b7 7 matches \u00b7 "
            "Tracking-dependent."
        )

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

        if not matches:
            state.pt_warning_text = (
                "Pass timing requires OBSO computation and PAUSA pipeline. Currently available for 7 IDSSE matches."
            )
        elif state.pt_selected_match is None:
            # Auto-select first match on initial load
            state.pt_selected_match = matches[0][0]
            pt_on_match_change(state, "pt_selected_match", matches[0][0])
        else:
            _refresh_data(state)
    except Exception:
        logger.exception("Failed to refresh PAUSA page")

    state.pt_data_freshness = fetch_data_freshness()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Pass-Timing", pt_refresh)
