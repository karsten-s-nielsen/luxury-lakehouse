"""Player Radar page — compare 1-3 players on per-90 metrics."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.charts import plot_player_radar
from streamlit_app.components.filters import (
    render_competition_filter,
    render_minutes_filter,
    render_player_filter,
    render_team_filter,
)
from streamlit_app.db import execute_query, t

# Default metrics with display labels and reasonable ranges
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

# Physical metrics — only shown when tracking data exists for the player
_PHYSICAL_METRICS: list[tuple[str, str, tuple[float, float]]] = [
    ("avg_distance_per_min", "Dist/Min (m)", (0, 150)),
    ("avg_max_speed_ms", "Top Speed (m/s)", (0, 12)),
]


@st.cache_data(ttl=600, show_spinner="Loading player stats...")
def _fetch_player_radar_stats(
    stats_tbl: str, players_tbl: str, phys_tbl: str, placeholders: str, comp_id: int, p_ids: tuple[int, ...]
) -> Any:
    # Use ROW_NUMBER to pick the season with most minutes per player,
    # avoiding duplicates when a competition spans multiple seasons.
    # LEFT JOIN physical stats averaged across tracking matches.
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
        (comp_id, *p_ids),
    )


def _load_player_stats(competition_id: int, player_ids: list[int]) -> Any:
    """Load per-90 stats for selected players in a competition."""
    # L-3: Explicit type assertion before query
    competition_id = int(competition_id)
    player_ids = [int(pid) for pid in player_ids]
    placeholders = ", ".join(["%s"] * len(player_ids))
    return _fetch_player_radar_stats(
        t("fct_player_stats_synced"),
        t("dim_players_synced"),
        t("fct_physical_stats_synced"),
        placeholders,
        competition_id,
        tuple(player_ids),
    )


def page() -> None:
    """Render the Player Radar page."""
    st.header(":material/radar: Player Radar")

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        min_minutes = render_minutes_filter()
        player_ids = render_player_filter(
            competition_id,
            team_id,
            min_minutes=min_minutes,
            multiselect=True,
        )

    if competition_id is None:
        st.info("Select a competition to begin.")
        return

    if not isinstance(player_ids, list) or len(player_ids) == 0:
        st.info("Select 1-3 players to compare.")
        return

    stats = _load_player_stats(competition_id, player_ids)
    if stats.empty:
        st.warning("No stats found for selected players.")
        return

    # Metric selection — include physical metrics when tracking data exists
    available_metrics = list(_DEFAULT_METRICS)
    if stats["avg_distance_per_min"].notna().any():
        available_metrics.extend(_PHYSICAL_METRICS)

    all_labels = [m[1] for m in available_metrics]
    selected_labels = st.multiselect("Metrics", all_labels, default=all_labels)

    selected = [m for m in available_metrics if m[1] in selected_labels]
    if len(selected) < 3:
        st.warning("Select at least 3 metrics for a meaningful radar chart.")
        return

    metric_keys = [m[0] for m in selected]
    labels = [m[1] for m in selected]
    ranges = [m[2] for m in selected]

    players_data: list[dict[str, float]] = []
    player_names: list[str] = []
    for _, row in stats.iterrows():
        players_data.append({k: float(row.get(k, 0) or 0) for k in metric_keys})
        player_names.append(str(row["player_display_name"]))

    title = " vs ".join(player_names)
    fig = plot_player_radar(players_data, metric_keys, labels, ranges, title=title, player_names=player_names)

    _, col_radar, _ = st.columns([1, 2, 1])
    with col_radar:
        st.pyplot(fig)

    with st.expander("Raw Data"):
        st.dataframe(stats, use_container_width=True)
