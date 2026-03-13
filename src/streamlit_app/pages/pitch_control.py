"""Pitch Control page — physics-based (Spearman 2017) and Voronoi pitch control."""

from __future__ import annotations

import datetime
from typing import Any

import pandas as pd
import streamlit as st

from analytics.pitch_control import compute_pitch_control_at_point, compute_pitch_control_frame
from streamlit_app.components.pitch import plot_physics_pitch_control, plot_pitch_control
from streamlit_app.db import execute_query, t


@st.cache_data(ttl=300)
def _compute_cached_pc_grid(frame_data_json: str) -> tuple[Any, Any, Any]:
    """Compute pitch control grid with caching; input serialised as JSON string."""
    frame_data = pd.read_json(frame_data_json)
    return compute_pitch_control_frame(frame_data)


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_tracking_matches(tbl: str, prov: str | None) -> Any:
    if prov:
        return execute_query(
            f"WITH RECURSIVE dm AS ("  # noqa: S608
            f"  SELECT MIN(match_id) AS match_id FROM {tbl} WHERE source_provider = %s"
            f"  UNION ALL"
            f"  SELECT (SELECT MIN(match_id) FROM {tbl}"
            f"          WHERE source_provider = %s AND match_id > dm.match_id)"
            f"  FROM dm WHERE dm.match_id IS NOT NULL"
            f") SELECT match_id FROM dm WHERE match_id IS NOT NULL ORDER BY match_id",
            (prov, prov),
        )
    return execute_query(
        f"WITH RECURSIVE dm AS ("  # noqa: S608
        f"  SELECT MIN(match_id) AS match_id FROM {tbl}"
        f"  UNION ALL"
        f"  SELECT (SELECT MIN(match_id) FROM {tbl} WHERE match_id > dm.match_id)"
        f"  FROM dm WHERE dm.match_id IS NOT NULL"
        f") SELECT match_id FROM dm WHERE match_id IS NOT NULL ORDER BY match_id"
    )


def _load_matches(provider: str | None = None) -> Any:
    """Load distinct match IDs from the tracking synced table, optionally filtered by provider.

    Uses a recursive CTE ("loose index scan") to avoid a full-table DISTINCT
    on 38M+ rows.  With only ~20 distinct match_ids, this performs ~20 index
    lookups via the btree on match_id instead of a sequential scan.
    """
    tbl = t("fct_tracking_frames_synced")
    return _fetch_tracking_matches(tbl, provider)


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_frame_range(tbl: str, m: str, p: int) -> tuple[int, int]:
    df = execute_query(
        f"SELECT MIN(frame) as min_frame, MAX(frame) as max_frame "  # noqa: S608
        f"FROM {tbl} "
        f"WHERE match_id = %s AND period = %s",
        (m, p),
    )
    if df.empty:
        return (0, 0)
    return (int(df.iloc[0]["min_frame"]), int(df.iloc[0]["max_frame"]))


def _load_frame_range(match_id: str, period: int) -> tuple[int, int]:
    """Get min/max frame numbers for a match and period."""
    match_id = str(match_id)
    period = int(period)
    return _fetch_frame_range(t("fct_tracking_frames_synced"), match_id, period)


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_frame_rate(tbl: str, m: str) -> int:
    df = execute_query(
        f"SELECT frame_rate FROM {tbl} "  # noqa: S608
        f"WHERE match_id = %s LIMIT 1",
        (m,),
    )
    if df.empty:
        return 25
    return int(df.iloc[0]["frame_rate"])


def _load_frame_rate(match_id: str) -> int:
    """Get the frame rate for a specific match."""
    match_id = str(match_id)
    return _fetch_frame_rate(t("fct_tracking_frames_synced"), match_id)


@st.cache_data(ttl=600, show_spinner="Loading frame...")
def _fetch_frame_data(tbl: str, m: str, f: int) -> Any:
    return execute_query(
        f"SELECT player_id, team, x, y, ball_x, ball_y, "  # noqa: S608
        f"  velocity_x, velocity_y, speed, distance_to_ball "
        f"FROM {tbl} "
        f"WHERE match_id = %s AND frame = %s",
        (m, f),
    )


def _load_frame_data(match_id: str, frame: int) -> Any:
    """Load all player rows for a specific frame."""
    match_id = str(match_id)
    frame = int(frame)
    return _fetch_frame_data(t("fct_tracking_frames_synced"), match_id, frame)


def page() -> None:
    """Render the Pitch Control page."""
    st.header(":material/grid_on: Pitch Control")

    with st.sidebar:
        provider_options = ["All", "metrica", "idsse", "skillcorner"]
        provider = st.selectbox("Provider", provider_options, index=0)
        selected_provider = None if provider == "All" else provider

    matches = _load_matches(selected_provider)
    if matches.empty:
        st.info("No tracking data available. Sync fct_tracking_frames to Lakebase first.")
        return

    with st.sidebar:
        viz_mode = st.radio("Model", ["Physics (Spearman)", "Voronoi"], horizontal=True)
        match_id = st.selectbox("Match", matches["match_id"].tolist())
        period = st.radio("Half", [1, 2], horizontal=True)
        show_velocity = st.toggle("Show velocity arrows", value=True)

    if match_id is None or period is None:
        return

    min_frame, max_frame = _load_frame_range(str(match_id), int(period))
    if min_frame == max_frame == 0:
        st.warning("No frames found for this match and period.")
        return

    # Convert frame range to elapsed seconds for time-based slider
    fps = _load_frame_rate(str(match_id))
    duration_secs = (max_frame - min_frame) // fps
    dur_min, dur_sec = divmod(duration_secs, 60)

    with st.sidebar:
        selected_time = st.slider(
            f"Time ({fps} fps)",
            min_value=datetime.time(0, 0, 0),
            max_value=datetime.time(0, dur_min, dur_sec),
            value=datetime.time(0, 0, 0),
            step=datetime.timedelta(seconds=1),
            format="mm:ss",
        )
        elapsed_secs = selected_time.minute * 60 + selected_time.second
        frame = min_frame + elapsed_secs * fps

    frame_data = _load_frame_data(str(match_id), frame)
    if frame_data.empty:
        st.warning("No data for this frame.")
        return

    # Extract ball position (same for all rows in a frame)
    ball_x = float(frame_data.iloc[0]["ball_x"]) if frame_data.iloc[0]["ball_x"] is not None else None
    ball_y = float(frame_data.iloc[0]["ball_y"]) if frame_data.iloc[0]["ball_y"] is not None else None

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        title = f"Pitch Control — {match_id} H{period} {elapsed_secs // 60:02d}:{elapsed_secs % 60:02d}"
        if viz_mode == "Physics (Spearman)":
            # Fill NaN velocities with 0 for physics model
            physics_data = frame_data.copy()
            physics_data["velocity_x"] = physics_data["velocity_x"].fillna(0.0)
            physics_data["velocity_y"] = physics_data["velocity_y"].fillna(0.0)

            grid_x, grid_y, surface = _compute_cached_pc_grid(physics_data.to_json())
            fig = plot_physics_pitch_control(
                physics_data, surface, grid_x, grid_y, ball_x, ball_y, show_velocity, title=title
            )
        else:
            fig = plot_pitch_control(frame_data, ball_x, ball_y, show_velocity, title=title)
        st.pyplot(fig)

    with col_stats:
        player_count = len(frame_data)
        st.metric("Players", player_count)

        if viz_mode == "Physics (Spearman)":
            # Physics-mode stats: control percentages
            home_pct = float(surface.mean()) * 100  # type: ignore[possibly-undefined]
            away_pct = 100.0 - home_pct
            st.metric("Home Control", f"{home_pct:.1f}%")
            st.metric("Away Control", f"{away_pct:.1f}%")
            if ball_x is not None and ball_y is not None:
                ball_control = compute_pitch_control_at_point(physics_data, ball_x, ball_y)  # type: ignore[possibly-undefined]
                st.metric("Control at Ball", f"{ball_control:.2f}")

        if "speed" in frame_data.columns:
            valid_speed = frame_data["speed"].dropna()
            if not valid_speed.empty:
                st.metric("Avg Speed", f"{valid_speed.mean():.1f}")
                st.metric("Max Speed", f"{valid_speed.max():.1f}")

        if "distance_to_ball" in frame_data.columns:
            valid_dist = frame_data["distance_to_ball"].dropna()
            if not valid_dist.empty:
                st.metric("Avg Dist to Ball", f"{valid_dist.mean():.1f}")
