"""Heat Map page — visualize action density on a full pitch."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from streamlit_app.components.filters import (
    render_competition_filter,
    render_match_filter,
    render_player_filter,
    render_team_filter,
)
from streamlit_app.components.pitch import plot_heatmap
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_actions(
    competition_id: int,
    team_id: int | None,
    player_id: int | None,
    match_id: int | None,
) -> Any:
    """Load pass and shot locations for the heat map."""
    competition_id = int(competition_id)

    pass_conditions = ["p.competition_id = %s"]
    shot_conditions = ["s.competition_id = %s"]
    pass_params: list[Any] = [competition_id]
    shot_params: list[Any] = [competition_id]

    if team_id is not None:
        team_id = int(team_id)
        pass_conditions.append("p.team_id = %s")
        shot_conditions.append("s.team_id = %s")
        pass_params.append(team_id)
        shot_params.append(team_id)

    if player_id is not None:
        player_id = int(player_id)
        pass_conditions.append("p.player_id = %s")
        shot_conditions.append("s.player_id = %s")
        pass_params.append(player_id)
        shot_params.append(player_id)

    if match_id is not None:
        match_id = int(match_id)
        pass_conditions.append("p.match_id = %s")
        shot_conditions.append("s.match_id = %s")
        pass_params.append(match_id)
        shot_params.append(match_id)

    pass_where = " AND ".join(pass_conditions)
    shot_where = " AND ".join(shot_conditions)
    all_params = tuple(pass_params + shot_params)

    passes_tbl = t("fct_passes_synced")
    shots_tbl = t("fct_shots_synced")

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading actions...")
    def _query(
        p_where: str,
        s_where: str,
        params: tuple[Any, ...],
    ) -> Any:
        # Aggregate server-side: round coordinates to bin centers and count.
        # Returns ~96 rows (12x8 grid) instead of 500K+ individual actions.
        # SECURITY: WHERE clauses are built from hardcoded conditions only;
        # all user values use %s parameterized placeholders.
        return execute_query(
            f"SELECT x, y, action_type, sum(cnt) AS cnt FROM ("  # noqa: S608
            f"  SELECT round(p.start_x / 10) * 10 + 5 AS x,"
            f"    round(p.start_y / 10) * 10 + 5 AS y,"
            f"    'pass' AS action_type, count(*) AS cnt "
            f"  FROM {passes_tbl} p WHERE {p_where} "
            f"  GROUP BY round(p.start_x / 10), round(p.start_y / 10) "
            f"  UNION ALL "
            f"  SELECT round(s.location_x / 10) * 10 + 5 AS x,"
            f"    round(s.location_y / 10) * 10 + 5 AS y,"
            f"    'shot' AS action_type, count(*) AS cnt "
            f"  FROM {shots_tbl} s WHERE {s_where} "
            f"  GROUP BY round(s.location_x / 10), round(s.location_y / 10)"
            f") agg GROUP BY x, y, action_type",
            params,
        )

    return _query(pass_where, shot_where, all_params)


def _classify_zone(x: float, y: float) -> str:
    """Classify a pitch location into a 3x3 zone grid."""
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


def page() -> None:
    """Render the Heat Map page."""
    st.header(":material/local_fire_department: Heat Map")

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        player_id_raw = render_player_filter(competition_id, team_id)
        player_id: int | None = player_id_raw if isinstance(player_id_raw, int) else None
        match_id = render_match_filter(competition_id, team_id, allow_all=True)

    if competition_id is None:
        st.info("Select a competition to view the heat map.")
        return

    actions = _load_actions(competition_id, team_id, player_id, match_id)

    if actions.empty:
        st.warning("No actions found for the selected filters.")
        return

    # Expand pre-aggregated rows (x, y, action_type, cnt) for bin_statistic
    counts = actions["cnt"].astype(int).values
    expanded_x = np.repeat(actions["x"].values, counts)
    expanded_y = np.repeat(actions["y"].values, counts)

    heatmap_df = pd.DataFrame({"x": expanded_x, "y": expanded_y})

    _, col_viz, col_stats = st.columns([1, 2, 1])

    with col_viz:
        fig = plot_heatmap(heatmap_df, title="Action Density Heat Map")
        st.pyplot(fig)

    with col_stats:
        total = int(counts.sum())
        passes = int(actions.loc[actions["action_type"] == "pass", "cnt"].sum())
        shots = int(actions.loc[actions["action_type"] == "shot", "cnt"].sum())

        st.metric("Total Actions", total)
        st.metric("Passes", passes)
        st.metric("Shots", shots)

        # Most active zone (3x3 grid)
        actions["zone"] = actions.apply(lambda r: _classify_zone(float(r["x"]), float(r["y"])), axis=1)
        zone_counts = actions.groupby("zone")["cnt"].sum()
        most_active = str(zone_counts.idxmax())
        st.metric("Most Active Zone", most_active)
