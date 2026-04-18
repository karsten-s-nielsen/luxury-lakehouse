"""Player Radar state module — all variables prefixed with pr_.

Loads per-90 stats for 1-3 selected players, renders mplsoccer Radar chart.
Registered as the Player-Radar page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
from typing import Any, cast

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
from filters import build_scope_label_plain, build_warning, fetch_data_freshness
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from mplsoccer import Radar
from queries.players import fetch_player_percentiles_batch, fetch_player_radar_stats
from render import PITCH_BG_COLOR, PITCH_LINE_COLOR, PLAYER_COLORS, chart_to_file

from state.shared import _ALL_LABEL, _page_refreshers, get_comp_id, register_page_refresher

logger = logging.getLogger(__name__)

# ── Percentile column mapping (metric_key -> pctile column in fct_player_percentiles_synced) ──
_PCTILE_COL_MAP: dict[str, str | None] = {
    "goals_per_90": "goals_per_90_pctile",
    "xg_per_90": "xg_per_90_pctile",
    "passes_per_90": "passes_per_90_pctile",
    "progressive_passes_per_90": "progressive_passes_per_90_pctile",
    "pass_completion_pct": "pass_completion_pct_pctile",
    "xg_overperformance": None,  # No percentile column for derived metric
    "line_breaking_per_90": "line_breaking_per_90_pctile",
    "vaep_per_90": "vaep_per_90_pctile",
    "offensive_vaep_per_90": "offensive_vaep_per_90_pctile",
    "defensive_vaep_per_90": "defensive_vaep_per_90_pctile",
    "defcon_per_90": "defcon_per_90_pctile",
    "avg_distance_per_min": "distance_per_minute_pctile",
    "avg_max_speed_ms": "max_speed_pctile",
}

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

# Spoke label explanations — shared constant imported by player_similarity.
# Not rendered as page text (glossary covers definitions); used only for
# internal cross-reference.
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
pr_scope_comp: str = ""
pr_scope_team: str = ""
pr_scope_players: str = ""
pr_radar_image_alt: str = ""
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
    "pr_radar_image_alt",
    "pr_scope_comp",
    "pr_scope_players",
    "pr_scope_team",
    "pr_select_hint",
    "pr_player_count",
    "pr_radar_image",
    "pr_selected_metrics",
    "pr_stats_table",
    "pr_warning_text",
]


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
    # mplsoccer's Radar.setup_axis is stubbed as Optional[tuple]; at runtime
    # it always returns (Figure, Axes) when the mandatory figsize is given.
    fig, ax = cast(tuple[Figure, Axes], radar.setup_axis(figsize=(6, 6), facecolor=PITCH_BG_COLOR))
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
        state.pr_metrics_hint = ""
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
    state.pr_radar_image_alt = ""
    state.pr_player_count = 0
    state.pr_metrics_hint = ""
    state.pr_low_minute_warning = ""
    state.pr_no_data_warning = ""
    state.pr_no_physical_note = ""
    state.pr_data_freshness = ""
    state.pr_stats_table = pd.DataFrame(columns=_STATS_COLUMNS)
    state.pr_scope_comp = ""
    state.pr_scope_team = ""
    state.pr_scope_players = ""
    state.pr_warning_text = ""


def pr_refresh(state: Any) -> None:
    """Reload player stats and render radar for selected players (1-3)."""
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        _clear_state(state)
        state.pr_comp_selected = False
        return

    state.pr_comp_selected = True
    # Team filter is not used by fetch_player_radar_stats (player_ids drive the query);
    # scope only needs the team LABEL for display, not the resolved team_id.

    # Canonical Tier A scope — Competition, Team, Players
    state.pr_scope_comp = state.selected_competition or ""
    state.pr_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"

    # Resolve player IDs from multiselect labels
    from state.shared import _player_map

    player_labels: list[str] = state.selected_players_multi or []
    if not player_labels:
        state.pr_scope_players = "None selected"
        _clear_state(state)
        state.pr_comp_selected = True
        state.pr_scope_comp = state.selected_competition or ""
        state.pr_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
        state.pr_scope_players = "None selected"
        state.pr_data_freshness = fetch_data_freshness()
        return

    # Players scope — join up to 3 names; tail-truncate if more
    visible_names = player_labels[:3]
    state.pr_scope_players = ", ".join(visible_names)

    player_ids = [_player_map[label] for label in player_labels if label in _player_map]
    if not player_ids:
        _clear_state(state)
        state.pr_comp_selected = True
        state.pr_data_freshness = fetch_data_freshness()
        return

    # Limit to 3 players
    player_ids = player_ids[:3]

    stats = fetch_player_radar_stats(comp_id, tuple(player_ids))
    if stats.empty:
        _clear_state(state)
        state.pr_comp_selected = True
        # Preserve scope after _clear_state
        state.pr_scope_comp = state.selected_competition or ""
        state.pr_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
        state.pr_scope_players = ", ".join(player_labels[:3])
        state.pr_no_data_warning = build_warning(
            domain="player stats",
            suggestions=["a different team", "different players"],
        )
        state.pr_warning_text = state.pr_no_data_warning
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
    default_ranges = [m[2] for m in filtered_metrics]

    # Attempt to fetch percentile data for each player (graceful degradation).
    # fetch_player_percentiles_batch returns None when the feature is
    # unavailable (e.g., fct_player_percentiles_synced missing / inaccessible).
    # Treat None the same as "no percentiles available" downstream — the
    # use_percentiles guard below handles falsy values correctly.
    pctile_data: dict[int, pd.DataFrame] = fetch_player_percentiles_batch(tuple(player_ids), comp_id) or {}

    # Use percentile-based scaling when ALL players have percentile data
    # and ALL selected metrics have a percentile column mapping.
    use_percentiles = bool(pctile_data) and len(pctile_data) == len(player_ids)
    if use_percentiles:
        pctile_metrics = [k for k in metric_keys if _PCTILE_COL_MAP.get(k) is not None]
        use_percentiles = len(pctile_metrics) == len(metric_keys)

    ranges = [(0.0, 1.0)] * len(metric_keys) if use_percentiles else default_ranges

    # Build player data and names
    players_data: list[dict[str, float]] = []
    player_names: list[str] = []
    low_minute_warnings: list[str] = []
    stats_rows: list[dict[str, Any]] = []

    for _, row in stats.iterrows():
        pid = int(row["player_id"])
        if use_percentiles and pid in pctile_data:
            pctile_row = pctile_data[pid].iloc[0]
            player_vals: dict[str, float] = {}
            for k in metric_keys:
                pctile_col = _PCTILE_COL_MAP.get(k)
                if pctile_col and pctile_col in pctile_row.index:
                    val = pctile_row.get(pctile_col)
                    player_vals[k] = float(val) if pd.notna(val) else 0.0
                else:
                    player_vals[k] = float(row.get(k, 0) or 0)
            players_data.append(player_vals)
        else:
            players_data.append({k: float(row.get(k, 0) or 0) for k in metric_keys})
        name = str(row["player_display_name"])
        minutes = int(row.get("minutes_played", 0) or 0)
        player_names.append(f"{name} ({minutes:,} min)")
        if minutes < 450:
            low_minute_warnings.append(f"{name} has only {minutes} min \u2014 per-90 stats may be unreliable")

        # Build stats table row with all available metrics (always raw values).
        # Taipy table rendering breaks on column names with / or % characters.
        # Sanitize: "/" → " per ", "%" → "Pct", "." → "" for safe column names.
        stats_row: dict[str, Any] = {"Player": name, "Minutes": minutes}
        for mk, ml, _ in available_metrics:
            safe_col = ml.replace("/", " per ").replace("%", "Pct").replace(".", "")
            stats_row[safe_col] = round(float(row.get(mk, 0) or 0), 3)
        stats_rows.append(stats_row)

    state.pr_stats_table = pd.DataFrame(stats_rows)
    state.pr_low_minute_warning = " \u00b7 ".join(low_minute_warnings) if low_minute_warnings else ""

    # Scale context in metrics hint (replaces removed spoke legend)
    if use_percentiles:
        state.pr_metrics_hint = (
            "Percentile scaling (0\u2013100th within competition). See Glossary for metric definitions."
        )
    elif len(current_selection) >= 3:
        state.pr_metrics_hint = "Raw value scaling. See Glossary for metric definitions."

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

    # Scope-aware alt text for accessibility
    scope_plain = build_scope_label_plain(
        [
            ("Competition", state.pr_scope_comp),
            ("Team", state.pr_scope_team),
            ("Players", state.pr_scope_players),
        ]
    )
    state.pr_radar_image_alt = f"Player Comparison Radar — {scope_plain}"

    # Data freshness
    state.pr_data_freshness = fetch_data_freshness()

    logger.info("Player radar refreshed: %d players, %d metrics", len(players_data), len(metric_keys))


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Player-Comparison", pr_refresh)
