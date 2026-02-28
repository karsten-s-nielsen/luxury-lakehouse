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
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t

# Default metrics with display labels and reasonable ranges
_DEFAULT_METRICS: list[tuple[str, str, tuple[float, float]]] = [
    ("goals_per_90", "Goals/90", (0, 1.5)),
    ("xg_per_90", "xG/90", (0, 1.5)),
    ("passes_per_90", "Passes/90", (0, 80)),
    ("progressive_passes_per_90", "Prog. Passes/90", (0, 12)),
    ("pass_completion_pct", "Pass %", (40, 100)),
    ("xg_overperformance", "xG Over-perf", (-5, 5)),
]


def _load_player_stats(competition_id: int, player_ids: list[int]) -> Any:
    """Load per-90 stats for selected players in a competition."""
    # L-3: Explicit type assertion before query
    competition_id = int(competition_id)
    player_ids = [int(pid) for pid in player_ids]
    placeholders = ", ".join(["%s"] * len(player_ids))

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading player stats...")
    def _query(comp_id: int, p_ids: tuple[int, ...]) -> Any:
        return execute_query(
            f"SELECT ps.player_id, p.player_display_name, "  # noqa: S608
            f"  ps.minutes_played, ps.goals_per_90, ps.xg_per_90, "
            f"  ps.passes_per_90, ps.progressive_passes_per_90, "
            f"  ps.pass_completion_pct, ps.xg_overperformance "
            f"FROM {t('fct_player_stats_synced')} ps "
            f"JOIN {t('dim_players_synced')} p ON ps.player_id = p.player_id "
            f"WHERE ps.competition_id = %s AND ps.player_id IN ({placeholders}) ",
            (comp_id, *p_ids),
        )

    return _query(competition_id, tuple(player_ids))


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

    # Metric selection
    all_labels = [m[1] for m in _DEFAULT_METRICS]
    selected_labels = st.multiselect("Metrics", all_labels, default=all_labels)

    selected = [m for m in _DEFAULT_METRICS if m[1] in selected_labels]
    if len(selected) < 3:
        st.warning("Select at least 3 metrics for a meaningful radar chart.")
        return

    stats = _load_player_stats(competition_id, player_ids)
    if stats.empty:
        st.warning("No stats found for selected players.")
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
    fig = plot_player_radar(players_data, metric_keys, labels, ranges, title=title)
    st.pyplot(fig)

    with st.expander("Raw Data"):
        st.dataframe(stats, use_container_width=True)
