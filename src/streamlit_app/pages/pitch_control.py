"""Pitch Control page — Voronoi diagram of player territorial control per frame."""

from __future__ import annotations

from typing import Any

import streamlit as st

from streamlit_app.components.pitch import plot_pitch_control
from streamlit_app.config import get_settings
from streamlit_app.db import execute_query, t


def _load_matches() -> Any:
    """Load distinct Metrica match IDs from the tracking synced table."""

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query() -> Any:
        return execute_query(
            f"SELECT DISTINCT match_id FROM {t('fct_tracking_frames_synced')} ORDER BY match_id"  # noqa: S608
        )

    return _query()


def _load_frame_range(match_id: str, period: int) -> tuple[int, int]:
    """Get min/max frame numbers for a match and period."""
    match_id = str(match_id)
    period = int(period)

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner=False)
    def _query(m: str, p: int) -> tuple[int, int]:
        df = execute_query(
            f"SELECT MIN(frame) as min_frame, MAX(frame) as max_frame "  # noqa: S608
            f"FROM {t('fct_tracking_frames_synced')} "
            f"WHERE match_id = %s AND period = %s",
            (m, p),
        )
        if df.empty:
            return (0, 0)
        return (int(df.iloc[0]["min_frame"]), int(df.iloc[0]["max_frame"]))

    return _query(match_id, period)


def _load_frame_data(match_id: str, frame: int) -> Any:
    """Load all player rows for a specific frame."""
    match_id = str(match_id)
    frame = int(frame)

    @st.cache_data(ttl=get_settings().cache_ttl_seconds, show_spinner="Loading frame...")
    def _query(m: str, f: int) -> Any:
        return execute_query(
            f"SELECT player_id, team, x, y, ball_x, ball_y, "  # noqa: S608
            f"  velocity_x, velocity_y, speed, distance_to_ball "
            f"FROM {t('fct_tracking_frames_synced')} "
            f"WHERE match_id = %s AND frame = %s",
            (m, f),
        )

    return _query(match_id, frame)


def page() -> None:
    """Render the Pitch Control page."""
    st.header(":material/grid_on: Pitch Control")

    matches = _load_matches()
    if matches.empty:
        st.info("No tracking data available. Sync fct_tracking_frames to Lakebase first.")
        return

    with st.sidebar:
        match_id = st.selectbox("Match", matches["match_id"].tolist())
        period = st.radio("Period", [1, 2], horizontal=True)
        show_velocity = st.toggle("Show velocity arrows", value=False)

    if match_id is None or period is None:
        return

    min_frame, max_frame = _load_frame_range(str(match_id), int(period))
    if min_frame == max_frame == 0:
        st.warning("No frames found for this match and period.")
        return

    with st.sidebar:
        frame = st.slider("Frame", min_value=min_frame, max_value=max_frame, value=min_frame, step=25)

    frame_data = _load_frame_data(str(match_id), frame)
    if frame_data.empty:
        st.warning("No data for this frame.")
        return

    # Extract ball position (same for all rows in a frame)
    ball_x = float(frame_data.iloc[0]["ball_x"]) if frame_data.iloc[0]["ball_x"] is not None else None
    ball_y = float(frame_data.iloc[0]["ball_y"]) if frame_data.iloc[0]["ball_y"] is not None else None

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        title = f"Pitch Control — {match_id} P{period} F{frame}"
        fig = plot_pitch_control(frame_data, ball_x, ball_y, show_velocity, title=title)
        st.pyplot(fig)

    with col_stats:
        player_count = len(frame_data)
        st.metric("Players", player_count)

        if "speed" in frame_data.columns:
            valid_speed = frame_data["speed"].dropna()
            if not valid_speed.empty:
                st.metric("Avg Speed", f"{valid_speed.mean():.1f}")
                st.metric("Max Speed", f"{valid_speed.max():.1f}")

        if "distance_to_ball" in frame_data.columns:
            valid_dist = frame_data["distance_to_ball"].dropna()
            if not valid_dist.empty:
                st.metric("Avg Dist to Ball", f"{valid_dist.mean():.1f}")
