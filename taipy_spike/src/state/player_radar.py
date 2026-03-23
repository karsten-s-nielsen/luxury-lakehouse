"""Player Radar state module — all variables prefixed with pr_.

Loads per-90 stats for 1-3 selected players, renders mplsoccer Radar chart.
Registered as the Player-Radar page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
from typing import Any

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
from cache import ttl_cache
from db import execute_query, t
from filters import fetch_data_freshness, fetch_scope_label
from mplsoccer import Radar
from render import PITCH_BG_COLOR, PITCH_LINE_COLOR, PLAYER_COLORS, chart_to_file

from state.shared import _page_refreshers, get_comp_id, get_team_id, register_page_refresher

logger = logging.getLogger(__name__)

# ── Metric definitions ──────────────────────────────────────────────────────

_DEFAULT_METRICS: list[tuple[str, str, tuple[float, float]]] = [
    ("goals_per_90", "Goals/90", (0, 1.5)),
    ("xg_per_90", "xG/90", (0, 1.5)),
    ("passes_per_90", "Passes/90", (0, 80)),
    ("progressive_passes_per_90", "Prog. Passes/90", (0, 12)),
    ("pass_completion_pct", "Pass %", (40, 100)),
    ("xg_overperformance", "xG Over-perf", (-5, 5)),
    ("line_breaking_per_90", "LB Passes/90", (0, 5)),
    ("vaep_per_90", "VAEP/90", (-0.5, 1.5)),
    ("offensive_vaep_per_90", "Off. VAEP/90", (-0.5, 1.5)),
    ("defensive_vaep_per_90", "Def. VAEP/90", (-0.5, 1.0)),
    ("defcon_per_90", "DEFCON/90", (-0.5, 2.0)),
]

_PHYSICAL_METRICS: list[tuple[str, str, tuple[float, float]]] = [
    ("avg_distance_per_min", "Dist/Min (m)", (0, 150)),
    ("avg_max_speed_ms", "Top Speed (m/s)", (0, 12)),
]

# Spoke label explanations (for caption below the radar)
SPOKE_LEGEND: dict[str, str] = {
    "Goals/90": "goals per 90 min",
    "xG/90": "expected goals per 90",
    "Passes/90": "completed passes per 90",
    "Prog. Passes/90": "progressive passes per 90",
    "Pass %": "pass completion rate",
    "xG Over-perf": "goals minus xG (positive = overperformed)",
    "LB Passes/90": "line-breaking passes per 90",
    "VAEP/90": "action value per 90 (higher = more impactful)",
    "Off. VAEP/90": "offensive contribution per 90",
    "Def. VAEP/90": "defensive contribution per 90",
    "DEFCON/90": "defensive pressure received per 90",
    "Dist/Min (m)": "distance per minute (meters)",
    "Top Speed (m/s)": "peak sprint speed",
}

# ── Exported state variables (all pr_ prefixed) ─────────────────────────────

pr_radar_image: str = ""
pr_player_count: int = 0
pr_spoke_caption: str = ""
pr_select_hint: str = "Select 1\u20133 players to compare."
pr_low_minute_warning: str = ""
pr_comp_selected: bool = False
pr_no_data_warning: str = ""
pr_no_physical_note: str = ""
pr_data_freshness: str = ""
pr_metric_lov: list[str] = []
pr_selected_metrics: list[str] = []
# Column schema must be declared at init — Taipy infers table structure from
# the initial DataFrame. Empty DataFrame() with no columns = zero-column table.
_STATS_COLUMNS = ["Player", "Minutes"] + [
    m[1].replace("/", " per ").replace("%", "Pct").replace(".", "") for m in _DEFAULT_METRICS
]
pr_stats_table: pd.DataFrame = pd.DataFrame(columns=_STATS_COLUMNS)

pr_metrics_hint: str = ""
pr_scope_label: str = ""
pr_warning_text: str = ""

__all__ = [
    "on_pr_metric_change",
    "pr_comp_selected",
    "pr_data_freshness",
    "pr_low_minute_warning",
    "pr_metric_lov",
    "pr_metrics_hint",
    "pr_no_data_warning",
    "pr_no_physical_note",
    "pr_scope_label",
    "pr_select_hint",
    "pr_player_count",
    "pr_radar_image",
    "pr_selected_metrics",
    "pr_spoke_caption",
    "pr_stats_table",
    "pr_warning_text",
]


# ── Data fetching ────────────────────────────────────────────────────────────


@ttl_cache()
def _fetch_player_radar_stats(
    comp_id: int,
    player_ids: list[int],
) -> pd.DataFrame:
    """Fetch per-90 stats for selected players, picking best season per player.

    Uses ROW_NUMBER() to select the season with most minutes, avoiding
    duplicates when a competition spans multiple seasons. LEFT JOINs
    physical stats averaged across tracking matches.
    """
    placeholders = ", ".join(["%s"] * len(player_ids))
    stats_tbl = t("fct_player_stats_synced")
    players_tbl = t("dim_players_synced")
    phys_tbl = t("fct_physical_stats_synced")

    return execute_query(
        f"SELECT sub.player_id, sub.player_display_name, "  # noqa: S608
        f"  sub.minutes_played, sub.goals_per_90, sub.xg_per_90, "
        f"  sub.passes_per_90, sub.progressive_passes_per_90, "
        f"  sub.pass_completion_pct, sub.xg_overperformance, "
        f"  sub.line_breaking_per_90, "
        f"  sub.vaep_per_90, sub.offensive_vaep_per_90, sub.defensive_vaep_per_90, "
        f"  sub.defcon_per_90, "
        f"  phys.avg_distance_per_min, phys.avg_max_speed_ms "
        f"FROM ("
        f"  SELECT ps.player_id, p.player_display_name, "
        f"    ps.minutes_played, ps.goals_per_90, ps.xg_per_90, "
        f"    ps.passes_per_90, ps.progressive_passes_per_90, "
        f"    ps.pass_completion_pct, ps.xg_overperformance, "
        f"    ps.line_breaking_per_90, "
        f"    ps.vaep_per_90, ps.offensive_vaep_per_90, ps.defensive_vaep_per_90, "
        f"    ps.defcon_per_90, "
        f"    ROW_NUMBER() OVER (PARTITION BY ps.player_id ORDER BY ps.minutes_played DESC) AS rn "
        f"  FROM {stats_tbl} ps "
        f"  JOIN {players_tbl} p ON ps.player_id = p.player_id "
        f"  WHERE ps.competition_id = %s AND ps.player_id IN ({placeholders})"
        f") sub "
        f"LEFT JOIN ("
        f"  SELECT player_id, "
        f"    AVG(distance_per_minute_m) AS avg_distance_per_min, "
        f"    AVG(max_speed_ms) AS avg_max_speed_ms "
        f"  FROM {phys_tbl} "
        f"  GROUP BY player_id"
        f") phys ON sub.player_id::text = phys.player_id "
        f"WHERE sub.rn = 1",
        (comp_id, *player_ids),
    )


# ── Rendering ────────────────────────────────────────────────────────────────


def _render_radar(
    players_data: list[dict[str, float]],
    metric_keys: list[str],
    labels: list[str],
    ranges: list[tuple[float, float]],
    title: str,
    player_names: list[str],
) -> str:
    """Render mplsoccer Radar chart and save to temp PNG. Returns file path."""
    low = [r[0] for r in ranges]
    high = [r[1] for r in ranges]

    radar = Radar(labels, low, high, round_int=[False] * len(labels), num_rings=4)
    result = radar.setup_axis(figsize=(6, 6), facecolor=PITCH_BG_COLOR)
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    radar.draw_circles(ax=ax, facecolor=PITCH_BG_COLOR, edgecolor="#333355")
    radar.draw_param_labels(ax=ax, color=PITCH_LINE_COLOR, fontsize=8)

    for i, player in enumerate(players_data[:3]):
        values = [player.get(m, 0.0) for m in metric_keys]
        color = PLAYER_COLORS[i % len(PLAYER_COLORS)]
        radar.draw_radar(
            values,
            ax=ax,
            kwargs_radar={"facecolor": color, "alpha": 0.2},
            kwargs_rings={"facecolor": color, "alpha": 0.1},
        )
        # Outline
        radar.draw_radar(
            values,
            ax=ax,
            kwargs_radar={"facecolor": "none", "edgecolor": color, "linewidth": 2},
            kwargs_rings={"facecolor": "none"},
        )

    # Legend
    if player_names:
        handles = [
            mpatches.Patch(color=PLAYER_COLORS[i % len(PLAYER_COLORS)], alpha=0.6, label=name)
            for i, name in enumerate(player_names[: len(players_data)])
        ]
        ax.legend(
            handles=handles,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.06),
            ncol=len(handles),
            fontsize=8,
            frameon=False,
            labelcolor=PITCH_LINE_COLOR,
        )

    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=11, pad=15, fontweight="bold")
    plt.close(fig)

    return chart_to_file(fig, "pr_radar")


# ── Metric change callback ───────────────────────────────────────────────────


def on_pr_metric_change(state: Any, var_name: str, var_value: Any) -> None:
    """Re-render radar with only the selected metrics.

    Triggered when the user changes the metric multiselect in the sidebar.
    Re-uses cached player data to avoid re-fetching.
    """
    selected: list[str] = var_value or []
    if not selected:
        state.pr_radar_image = ""
        state.pr_spoke_caption = ""
        return

    # Re-trigger a full refresh which will respect pr_selected_metrics
    state.pr_selected_metrics = selected
    fn = _page_refreshers.get("Player-Comparison")
    if fn:
        fn(state)


# ── Refresh callback ─────────────────────────────────────────────────────────


def _clear_state(state: Any) -> None:
    """Reset all pr_ state variables."""
    state.pr_radar_image = ""
    state.pr_player_count = 0
    state.pr_spoke_caption = ""
    state.pr_metrics_hint = ""
    state.pr_low_minute_warning = ""
    state.pr_no_data_warning = ""
    state.pr_no_physical_note = ""
    state.pr_data_freshness = ""
    state.pr_stats_table = pd.DataFrame(columns=_STATS_COLUMNS)
    state.pr_scope_label = ""
    state.pr_warning_text = ""


def pr_refresh(state: Any) -> None:
    """Reload player stats and render radar for selected players (1-3)."""
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        _clear_state(state)
        state.pr_comp_selected = False
        return

    state.pr_comp_selected = True
    team_id = get_team_id(state.selected_team)
    state.pr_scope_label = fetch_scope_label(comp_id, team_id)

    # Resolve player IDs from multiselect labels
    from state.shared import _player_map

    player_labels: list[str] = state.selected_players_multi or []
    if not player_labels:
        _clear_state(state)
        state.pr_comp_selected = True
        state.pr_data_freshness = fetch_data_freshness()
        return

    player_ids = [_player_map[label] for label in player_labels if label in _player_map]
    if not player_ids:
        _clear_state(state)
        state.pr_comp_selected = True
        state.pr_data_freshness = fetch_data_freshness()
        return

    # Limit to 3 players
    player_ids = player_ids[:3]

    stats = _fetch_player_radar_stats(comp_id, player_ids)
    if stats.empty:
        _clear_state(state)
        state.pr_comp_selected = True
        state.pr_scope_label = fetch_scope_label(comp_id, team_id)
        state.pr_no_data_warning = "No player stats for the selected filters."
        state.pr_warning_text = "No player stats for the selected filters."
        state.pr_data_freshness = fetch_data_freshness()
        return

    state.pr_player_count = len(stats)

    # Determine available metrics (include physical when tracking data exists)
    available_metrics = list(_DEFAULT_METRICS)
    has_physical = "avg_distance_per_min" in stats.columns and stats["avg_distance_per_min"].notna().any()
    if has_physical:
        available_metrics.extend(_PHYSICAL_METRICS)
    else:
        state.pr_no_physical_note = (
            "Physical metrics (distance, speed) unavailable \u2014 requires tracking data (~20 matches)."
        )

    all_labels = [m[1] for m in available_metrics]

    # Populate metric LOV and default selection (all metrics)
    state.pr_metric_lov = all_labels
    current_selection: list[str] = getattr(state, "pr_selected_metrics", []) or []
    if not current_selection or not any(lbl in all_labels for lbl in current_selection):
        # Default to all metrics when no prior selection or prior selection is stale
        state.pr_selected_metrics = list(all_labels)
        current_selection = list(all_labels)

    # Metrics hint — warn if too few selected for a meaningful radar
    if len(current_selection) < 3:
        state.pr_metrics_hint = "Select at least 3 metrics for a meaningful radar chart."
    else:
        state.pr_metrics_hint = ""

    # Filter to only selected metrics
    filtered_metrics = [m for m in available_metrics if m[1] in current_selection]
    if not filtered_metrics:
        filtered_metrics = list(available_metrics)

    metric_keys = [m[0] for m in filtered_metrics]
    labels = [m[1] for m in filtered_metrics]
    ranges = [m[2] for m in filtered_metrics]

    # Build player data and names
    players_data: list[dict[str, float]] = []
    player_names: list[str] = []
    low_minute_warnings: list[str] = []
    stats_rows: list[dict[str, Any]] = []

    for _, row in stats.iterrows():
        players_data.append({k: float(row.get(k, 0) or 0) for k in metric_keys})
        name = str(row["player_display_name"])
        minutes = int(row.get("minutes_played", 0) or 0)
        player_names.append(f"{name} ({minutes:,} min)")
        if minutes < 450:
            low_minute_warnings.append(f"{name} has only {minutes} min \u2014 per-90 stats may be unreliable")

        # Build stats table row with all available metrics.
        # Taipy table rendering breaks on column names with / or % characters.
        # Sanitize: "/" → " per ", "%" → "Pct", "." → "" for safe column names.
        stats_row: dict[str, Any] = {"Player": name, "Minutes": minutes}
        for mk, ml, _ in available_metrics:
            safe_col = ml.replace("/", " per ").replace("%", "Pct").replace(".", "")
            stats_row[safe_col] = round(float(row.get(mk, 0) or 0), 3)
        stats_rows.append(stats_row)

    state.pr_stats_table = pd.DataFrame(stats_rows)
    state.pr_low_minute_warning = " \u00b7 ".join(low_minute_warnings) if low_minute_warnings else ""

    # Build spoke caption (bold labels matching Streamlit st.caption format)
    legend_parts = [f"**{lbl}** = {SPOKE_LEGEND[lbl]}" for lbl in labels if lbl in SPOKE_LEGEND]
    state.pr_spoke_caption = " \u00b7 ".join(legend_parts) if legend_parts else ""

    # Render radar chart
    title = " vs ".join(player_names)
    state.pr_radar_image = _render_radar(
        players_data,
        metric_keys,
        labels,
        ranges,
        title,
        player_names,
    )

    # Data freshness
    state.pr_data_freshness = fetch_data_freshness()

    logger.info("Player radar refreshed: %d players, %d metrics", len(players_data), len(metric_keys))


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Player-Comparison", pr_refresh)
