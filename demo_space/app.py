"""Soccer Analytics Demo Space — interactive explorer for published datasets.

Deployed to HuggingFace Spaces at luxury-lakehouse/soccer-analytics-demo.
Loads pre-cached Parquet subsets from data/ (no live database connectivity).
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend — must be set before pyplot import

import gradio as gr
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from mplsoccer import Pitch
from pitch_control import PitchControlParams, compute_pitch_control_frame

DATA_DIR = Path(__file__).parent / "data"

# ---------------------------------------------------------------------------
# Data loading (cached at startup)
# ---------------------------------------------------------------------------


def _load_parquet(name: str) -> pd.DataFrame:
    """Load a Parquet file from the data directory, returning empty DataFrame if missing."""
    path = DATA_DIR / name
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


embeddings_df = _load_parquet("career_embeddings.parquet")
shots_df = _load_parquet("sample_shots.parquet")
passes_df = _load_parquet("sample_passes.parquet")
tracking_df = _load_parquet("sample_tracking.parquet")
pressure_df = _load_parquet("defcon_pressure.parquet")
pausa_df = _load_parquet("sample_pausa.parquet")

# Coerce string booleans from Spark Parquet export to proper types
if not shots_df.empty and "is_goal" in shots_df.columns:
    shots_df["is_goal"] = shots_df["is_goal"].astype(str).isin(["1", "true", "True"])
if not passes_df.empty and "is_line_breaking" in passes_df.columns:
    passes_df["is_line_breaking"] = passes_df["is_line_breaking"].astype(str).isin(["1", "true", "True"])

# Coerce string numerics from Spark Parquet export to proper float types
_NUMERIC_COLS = [
    "location_x",
    "location_y",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "shot_end_location_x",
    "shot_end_location_y",
    "xg_shot",
]
for _col in _NUMERIC_COLS:
    if not shots_df.empty and _col in shots_df.columns:
        shots_df[_col] = pd.to_numeric(shots_df[_col], errors="coerce")
    if not passes_df.empty and _col in passes_df.columns:
        passes_df[_col] = pd.to_numeric(passes_df[_col], errors="coerce")

# Coerce tracking numeric columns
_TRACKING_NUMERIC = ["x", "y", "velocity_x", "velocity_y", "ball_x", "ball_y", "speed_ms"]
for _col in _TRACKING_NUMERIC:
    if not tracking_df.empty and _col in tracking_df.columns:
        tracking_df[_col] = pd.to_numeric(tracking_df[_col], errors="coerce")

# Coerce PAUSA numeric columns
_PAUSA_NUMERIC = [
    "temporal_judgment", "spatial_selection", "pausa_score",
    "actual_obso", "peak_obso", "optimal_obso", "receiver_x", "receiver_y",
]
for _col in _PAUSA_NUMERIC:
    if not pausa_df.empty and _col in pausa_df.columns:
        pausa_df[_col] = pd.to_numeric(pausa_df[_col], errors="coerce")


# ---------------------------------------------------------------------------
# Player Similarity
# ---------------------------------------------------------------------------


def _extract_vectors(df: pd.DataFrame, col: str) -> np.ndarray:
    """Extract embedding vectors from a DataFrame column into a 2-D array."""
    vectors = df[col].tolist()
    parsed = []
    for v in vectors:
        if isinstance(v, str):
            parsed.append([float(x) for x in json.loads(v)])
        elif isinstance(v, (list, np.ndarray)):
            parsed.append([float(x) for x in v])
        else:
            parsed.append([0.0])
    return np.array(parsed, dtype=np.float64)


# Pre-build sorted player choices for similarity dropdown.
# Limit to top 500 by match count to avoid loading ~8,950 entries into the DOM (H28).
_similarity_players: list[tuple[str, str]] = []
_default_similarity_player: str | None = None
if not embeddings_df.empty and "player_name" in embeddings_df.columns:
    _top_df = embeddings_df.copy()
    if "total_matches" in _top_df.columns:
        _top_df["total_matches"] = pd.to_numeric(_top_df["total_matches"], errors="coerce")
        _top_df = _top_df.nlargest(500, "total_matches")
    _top_df = _top_df.sort_values("player_name")
    for _, row in _top_df.iterrows():
        name = str(row["player_name"])
        total = row.get("total_matches", "")
        label = f"{name} ({int(total)} matches)" if total else name
        _similarity_players.append((label, name))
    _default_similarity_player = _similarity_players[0][1] if _similarity_players else None
    del _top_df


def find_similar_players(selected_player: str | None, top_k: int = 10) -> pd.DataFrame:
    """Find most similar players by cosine distance on behavioral vectors."""
    if not selected_player:
        return pd.DataFrame()

    if embeddings_df.empty:
        return pd.DataFrame({"error": ["No embedding data loaded. Run the publishing notebook first."]})

    # Exact match on selected player name
    matches = embeddings_df[embeddings_df["player_name"] == selected_player]
    if matches.empty:
        return pd.DataFrame({"message": [f"No player found matching '{selected_player}'"]})

    query_idx = matches.index[0]
    vectors = _extract_vectors(embeddings_df, "behavioral_vector")

    # Cosine similarity
    query_vec = vectors[query_idx]
    norm_q = np.linalg.norm(query_vec)
    if norm_q == 0:
        return pd.DataFrame({"error": ["Query player has zero embedding vector."]})

    norms = np.linalg.norm(vectors, axis=1)
    norms = np.maximum(norms, 1e-10)
    similarities = vectors @ query_vec / (norms * norm_q)

    top_indices = np.argsort(similarities)[::-1][1 : top_k + 1]

    result_rows = []
    for idx in top_indices:
        sim = round(float(similarities[idx]), 3)
        dist = round(1.0 - sim, 3)  # Convert to cosine distance (matches Streamlit convention)
        row = {
            "rank": len(result_rows) + 1,
            "player_name": embeddings_df.iloc[idx].get("player_name", ""),
            "cosine_distance": dist,
            "interpretation": (
                "Very Similar"
                if dist < 0.20
                else "Similar"
                if dist < 0.35
                else "Moderately Similar"
                if dist < 0.50
                else "Different"
            ),
        }
        if "total_matches" in embeddings_df.columns:
            row["total_matches"] = int(embeddings_df.iloc[idx]["total_matches"])
        result_rows.append(row)

    return pd.DataFrame(result_rows)


# ---------------------------------------------------------------------------
# Competition name lookup
# ---------------------------------------------------------------------------
_COMP_NAMES: dict[str, str] = {
    "7": "Ligue 1",
    "9": "Bundesliga",
    "11": "La Liga",
    "12": "Serie A",
    "72": "Women's World Cup",
    "1267": "Africa Cup of Nations",
}


def _comp_label(cid: str) -> str:
    return _COMP_NAMES.get(str(cid), f"Competition {cid}")


# Human-readable match labels (CHI-AUDIT-190 Findings #4, #9, #10)
_MATCH_NAMES: dict[str, str] = {
    "game1": "Metrica Game 1",
    "game2": "Metrica Game 2",
    "game3": "Metrica Game 3",
}

# Auto-build IDSSE/PAUSA match labels from team data
if not pausa_df.empty and "match_id" in pausa_df.columns and "team" in pausa_df.columns:
    for _mid in pausa_df["match_id"].unique():
        if str(_mid) not in _MATCH_NAMES:
            _teams = sorted(pausa_df.loc[pausa_df["match_id"] == _mid, "team"].dropna().unique().tolist())
            if len(_teams) >= 2:
                _MATCH_NAMES[str(_mid)] = f"{_teams[0]} v {_teams[1]}"


def _match_label(match_id: str) -> str:
    """Return human-readable label for a match ID."""
    return _MATCH_NAMES.get(str(match_id), str(match_id))


# ---------------------------------------------------------------------------
# Shot Map (mplsoccer)
# ---------------------------------------------------------------------------


def create_shot_map(competition: str, goals_only: bool = False) -> plt.Figure:
    """Create a shot map using mplsoccer with StatsBomb coordinates."""
    pitch = Pitch(pitch_type="statsbomb", half=True, pitch_color="#2d6a2e", line_color="white")
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.set_facecolor("#1a1a2e")

    if shots_df.empty:
        ax.text(90, 40, "No shot data loaded.", ha="center", va="center", color="white", fontsize=14)
        return fig

    df = shots_df.copy()
    if competition and competition != "All" and "competition_id" in df.columns:
        df = df[df["competition_id"].astype(str) == str(competition)]
    if goals_only and "is_goal" in df.columns:
        df = df[df["is_goal"]]

    x_col = "location_x" if "location_x" in df.columns else "start_x"
    y_col = "location_y" if "location_y" in df.columns else "start_y"

    if x_col not in df.columns or y_col not in df.columns:
        ax.text(90, 40, "Missing coordinate columns.", ha="center", va="center", color="white", fontsize=14)
        return fig

    if "is_goal" in df.columns:
        no_goal = df[~df["is_goal"]]
        goals = df[df["is_goal"]]
        if not no_goal.empty:
            pitch.scatter(
                no_goal[x_col],
                no_goal[y_col],
                ax=ax,
                c="#3498db",
                s=40,
                alpha=0.5,
                edgecolors="white",
                linewidth=0.3,
                label=f"No goal ({len(no_goal)})",
                zorder=2,
            )
        if not goals.empty:
            pitch.scatter(
                goals[x_col],
                goals[y_col],
                ax=ax,
                c="#e74c3c",
                s=80,
                alpha=0.8,
                edgecolors="white",
                linewidth=0.5,
                label=f"Goal ({len(goals)})",
                zorder=3,
                marker="*",
            )
    else:
        pitch.scatter(
            df[x_col],
            df[y_col],
            ax=ax,
            c="#3498db",
            s=40,
            alpha=0.5,
            edgecolors="white",
            linewidth=0.3,
            zorder=2,
        )

    n_goals = int(df["is_goal"].sum()) if "is_goal" in df.columns else 0
    title = f"Shot Map \u2014 {len(df)} shots, {n_goals} goals"
    if competition and competition != "All":
        title = f"{_comp_label(competition)}: {title}"
    ax.set_title(title, color="white", fontsize=13, pad=8)
    ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", edgecolor="white", labelcolor="white")

    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Pass Quality (mplsoccer)
# ---------------------------------------------------------------------------


def create_pass_map(
    show_line_breaking: bool = True,
    pass_filter: str = "All",  # noqa: S107
    completed_only: bool = False,
) -> plt.Figure:
    """Create a pass map using mplsoccer with line-breaking highlighting."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color="#2d6a2e", line_color="white")
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.set_facecolor("#1a1a2e")

    if passes_df.empty:
        ax.text(60, 40, "No pass data loaded.", ha="center", va="center", color="white", fontsize=14)
        return fig

    df = passes_df.copy()
    if pass_filter and pass_filter != "All" and "pass_type" in df.columns:  # noqa: S105
        df = df[df["pass_type"] == pass_filter]
    if completed_only and "is_complete" in df.columns:
        df = df[df["is_complete"].astype(str).isin(["1", "true", "True"])]

    lb_count = 0
    has_endpoints = "end_x" in df.columns and "end_y" in df.columns

    if show_line_breaking and "is_line_breaking" in df.columns:
        regular = df[~df["is_line_breaking"]]
        line_breaking = df[df["is_line_breaking"]]
        lb_count = len(line_breaking)

        if not regular.empty:
            if has_endpoints:
                pitch.arrows(
                    regular["start_x"],
                    regular["start_y"],
                    regular["end_x"],
                    regular["end_y"],
                    ax=ax,
                    color="#3498db",
                    alpha=0.25,
                    width=1,
                    headwidth=4,
                    headlength=4,
                    zorder=2,
                    label=f"Regular ({len(regular)})",
                )
            else:
                pitch.scatter(
                    regular["start_x"],
                    regular["start_y"],
                    ax=ax,
                    c="#3498db",
                    s=15,
                    alpha=0.3,
                    zorder=2,
                    label=f"Regular ({len(regular)})",
                )

        if not line_breaking.empty:
            if has_endpoints:
                pitch.arrows(
                    line_breaking["start_x"],
                    line_breaking["start_y"],
                    line_breaking["end_x"],
                    line_breaking["end_y"],
                    ax=ax,
                    color="#e74c3c",
                    alpha=0.6,
                    width=1.5,
                    headwidth=5,
                    headlength=5,
                    zorder=3,
                    label=f"Line-breaking ({lb_count})",
                )
            else:
                pitch.scatter(
                    line_breaking["start_x"],
                    line_breaking["start_y"],
                    ax=ax,
                    c="#e74c3c",
                    s=30,
                    alpha=0.7,
                    zorder=3,
                    label=f"Line-breaking ({lb_count})",
                )
    else:
        if has_endpoints:
            pitch.arrows(
                df["start_x"],
                df["start_y"],
                df["end_x"],
                df["end_y"],
                ax=ax,
                color="#3498db",
                alpha=0.3,
                width=1,
                headwidth=4,
                headlength=4,
                zorder=2,
                label=f"All passes ({len(df)})",
            )
        else:
            pitch.scatter(
                df["start_x"],
                df["start_y"],
                ax=ax,
                c="#3498db",
                s=15,
                alpha=0.4,
                zorder=2,
                label=f"All passes ({len(df)})",
            )

    title = f"Pass Map \u2014 {len(df)} passes"
    if show_line_breaking and lb_count:
        title += f", {lb_count} line-breaking"
    ax.set_title(title, color="white", fontsize=13, pad=8)
    ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", edgecolor="white", labelcolor="white")

    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Pitch Control (mplsoccer + physics model)
# ---------------------------------------------------------------------------

_PC_PARAMS = PitchControlParams()


def _get_tracking_matches() -> list[str]:
    """Return sorted list of match IDs from tracking data."""
    if tracking_df.empty or "match_id" not in tracking_df.columns:
        return []
    return sorted(tracking_df["match_id"].unique().tolist())


def _get_frame_rate(match_id: str) -> int:
    """Return frame rate for a given match."""
    if tracking_df.empty or "frame_rate" not in tracking_df.columns:
        return 25
    match_rows = tracking_df[tracking_df["match_id"] == match_id]
    if match_rows.empty:
        return 25
    return int(match_rows["frame_rate"].iloc[0])


def _fmt_time(seconds: int) -> str:
    """Format seconds as MM:SS."""
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _get_duration_seconds(match_id: str, period: int) -> int:
    """Return duration in whole seconds for a given match and period."""
    if tracking_df.empty:
        return 0
    mask = (tracking_df["match_id"] == match_id) & (tracking_df["period"] == period)
    subset = tracking_df.loc[mask, "frame"]
    if subset.empty:
        return 0
    fps = _get_frame_rate(match_id)
    return (int(subset.max()) - int(subset.min())) // fps


def _seconds_to_frame(match_id: str, period: int, elapsed_sec: int) -> int:
    """Convert elapsed seconds back to the nearest frame number."""
    if tracking_df.empty:
        return 0
    mask = (tracking_df["match_id"] == match_id) & (tracking_df["period"] == period)
    subset = tracking_df.loc[mask, "frame"]
    if subset.empty:
        return 0
    fps = _get_frame_rate(match_id)
    return int(subset.min()) + elapsed_sec * fps


def _update_time_slider(match_id: str, period: int) -> gr.Slider:
    """Update time slider range when match or half changes."""
    duration = _get_duration_seconds(match_id, period)
    fps = _get_frame_rate(match_id)
    return gr.Slider(
        minimum=0,
        maximum=duration,
        value=0,
        step=1,
        label=f"Time: 00:00 \u2014 {_fmt_time(duration)} ({fps} fps)",
        elem_id="pc-time-slider",
    )


def create_pitch_control_plot(
    match_id: str,
    period: int,
    elapsed_sec: int,
    show_velocity: bool,
    progress: gr.Progress = gr.Progress(),  # noqa: B008
) -> plt.Figure:
    """Create a pitch control surface plot for a single tracking frame."""
    progress(0, desc="Computing pitch control surface\u2026")
    pitch = Pitch(pitch_type="statsbomb", pitch_color="#1a1a2e", line_color="#cccccc")
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.set_facecolor("#1a1a2e")

    if tracking_df.empty:
        ax.text(60, 40, "No tracking data loaded.", ha="center", va="center", color="white", fontsize=14)
        return fig

    # Convert elapsed seconds to frame number
    frame = _seconds_to_frame(match_id, period, int(elapsed_sec))

    # Filter to exact frame
    mask = (tracking_df["match_id"] == match_id) & (tracking_df["period"] == period) & (tracking_df["frame"] == frame)
    frame_df = tracking_df.loc[mask].copy()

    if frame_df.empty:
        ax.text(60, 40, "No data for this frame.", ha="center", va="center", color="white", fontsize=14)
        return fig

    # Prepare player DataFrame for pitch control computation
    players = frame_df[["player_id", "team", "x", "y", "velocity_x", "velocity_y"]].copy()
    players = players.dropna(subset=["x", "y"])

    # Fill missing velocities with zero
    players["velocity_x"] = players["velocity_x"].fillna(0.0)
    players["velocity_y"] = players["velocity_y"].fillna(0.0)

    # Compute pitch control surface
    grid_x, grid_y, surface = compute_pitch_control_frame(players, _PC_PARAMS)

    # Plot heatmap — surface is (ny, nx), extent maps to StatsBomb coordinates
    im = ax.imshow(
        surface,
        extent=[grid_x[0], grid_x[-1], grid_y[-1], grid_y[0]],
        cmap="RdBu",
        alpha=0.6,
        vmin=0.0,
        vmax=1.0,
        aspect="auto",
        interpolation="bilinear",
        zorder=1,
    )
    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Control (0=Away, 1=Home)", color="white", fontsize=9)
    cbar.ax.tick_params(colors="white", labelsize=8)

    # Draw players
    home = frame_df[frame_df["team"] == "home"]
    away = frame_df[frame_df["team"] == "away"]

    if not home.empty:
        pitch.scatter(
            home["x"],
            home["y"],
            ax=ax,
            c="#457b9d",
            s=120,
            edgecolors="white",
            linewidth=1.5,
            zorder=4,
            label=f"Home ({len(home)})",
        )
    if not away.empty:
        pitch.scatter(
            away["x"],
            away["y"],
            ax=ax,
            c="#e63946",
            s=120,
            edgecolors="white",
            linewidth=1.5,
            zorder=4,
            label=f"Away ({len(away)})",
        )

    # Draw velocity arrows if enabled
    if show_velocity:
        arrow_scale = 3.0  # Scale factor for visibility
        for _, row in frame_df.iterrows():
            vx = float(row.get("velocity_x", 0) or 0)
            vy = float(row.get("velocity_y", 0) or 0)
            if abs(vx) > 0.1 or abs(vy) > 0.1:
                color = "#457b9d" if row["team"] == "home" else "#e63946"
                ax.arrow(
                    float(row["x"]),
                    float(row["y"]),
                    vx * arrow_scale,
                    vy * arrow_scale,
                    head_width=1.0,
                    head_length=0.5,
                    fc=color,
                    ec=color,
                    alpha=0.7,
                    zorder=5,
                )

    # Draw ball position (if available)
    ball_x_val = frame_df["ball_x"].dropna()
    ball_y_val = frame_df["ball_y"].dropna()
    if not ball_x_val.empty and not ball_y_val.empty:
        bx = float(ball_x_val.iloc[0])
        by = float(ball_y_val.iloc[0])
        ax.plot(
            bx,
            by,
            marker="h",
            markersize=14,
            color="#f1fa8c",
            markeredgecolor="black",
            markeredgewidth=1.5,
            zorder=6,
            label="Ball",
        )

    # Compute control percentages
    home_control = float(np.mean(surface)) * 100
    away_control = 100.0 - home_control

    # Get timestamp for title
    ts_col = frame_df["timestamp_seconds"]
    timestamp = float(ts_col.iloc[0]) if not ts_col.empty else 0.0
    minutes = int(timestamp // 60)
    seconds = int(timestamp % 60)

    title = (
        f"Pitch Control \u2014 {_match_label(match_id)} | H{period} {minutes:02d}:{seconds:02d} | "
        f"Home {home_control:.0f}% \u2013 Away {away_control:.0f}%"
    )
    ax.set_title(title, color="white", fontsize=13, pad=8)
    ax.legend(loc="upper left", fontsize=9, facecolor="#1a1a2e", edgecolor="white", labelcolor="white")

    plt.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# Defensive Impact (Plotly)
# ---------------------------------------------------------------------------


_pressure_players: list[str] = sorted(pressure_df["player_name"].unique().tolist()) if not pressure_df.empty else []
_default_pressure_player: str | None = _pressure_players[0] if _pressure_players else None


def create_pressure_chart(player_name: str | None) -> plt.Figure | go.Figure:
    """Create a DEFCON pressure breakdown chart for a selected player.

    Returns a Plotly figure with grouped bars showing pressure by category per match.
    """
    # Return empty matplotlib figure as placeholder when no player selected
    if not player_name:
        fig_empty, ax_empty = plt.subplots(figsize=(10, 5))
        fig_empty.set_facecolor("#1a1a2e")
        ax_empty.set_facecolor("#1a1a2e")
        ax_empty.text(
            0.5,
            0.5,
            "Search for a player and select from the dropdown.",
            ha="center",
            va="center",
            color="white",
            fontsize=14,
            transform=ax_empty.transAxes,
        )
        ax_empty.set_xticks([])
        ax_empty.set_yticks([])
        for spine in ax_empty.spines.values():
            spine.set_visible(False)
        return fig_empty

    if pressure_df.empty:
        fig_empty, ax_empty = plt.subplots(figsize=(10, 5))
        fig_empty.set_facecolor("#1a1a2e")
        ax_empty.set_facecolor("#1a1a2e")
        ax_empty.text(
            0.5,
            0.5,
            "No pressure data loaded.",
            ha="center",
            va="center",
            color="white",
            fontsize=14,
            transform=ax_empty.transAxes,
        )
        ax_empty.set_xticks([])
        ax_empty.set_yticks([])
        for spine in ax_empty.spines.values():
            spine.set_visible(False)
        return fig_empty

    # Filter to selected player
    player_data = pressure_df[pressure_df["player_name"] == player_name].copy()
    if player_data.empty:
        fig_empty, ax_empty = plt.subplots(figsize=(10, 5))
        fig_empty.set_facecolor("#1a1a2e")
        ax_empty.set_facecolor("#1a1a2e")
        ax_empty.text(
            0.5,
            0.5,
            f"No data for '{player_name}'.",
            ha="center",
            va="center",
            color="white",
            fontsize=14,
            transform=ax_empty.transAxes,
        )
        ax_empty.set_xticks([])
        ax_empty.set_yticks([])
        for spine in ax_empty.spines.values():
            spine.set_visible(False)
        return fig_empty

    # Melt pressure columns for grouped bar chart
    pressure_cols = ["intercept_pressure", "concede_pressure", "disturb_pressure", "deter_pressure"]
    id_cols = ["match_label"]

    melted = player_data[id_cols + pressure_cols].melt(
        id_vars=id_cols,
        value_vars=pressure_cols,
        var_name="pressure_type",
        value_name="pressure_value",
    )

    # Clean up category names for display
    category_map = {
        "intercept_pressure": "Intercept",
        "concede_pressure": "Concede",
        "disturb_pressure": "Disturb",
        "deter_pressure": "Deter",
    }
    melted["pressure_type"] = melted["pressure_type"].map(category_map)

    color_map = {
        "Intercept": "#e63946",
        "Concede": "#f4a261",
        "Disturb": "#457b9d",
        "Deter": "#2a9d8f",
    }

    n_matches = player_data["match_id"].nunique()
    total_actions = int(player_data["total_defensive_actions"].sum())

    plotly_fig = px.bar(
        melted,
        x="match_label",
        y="pressure_value",
        color="pressure_type",
        barmode="group",
        color_discrete_map=color_map,
        labels={
            "match_label": "Match",
            "pressure_value": "Pressure Credits (higher = more impact)",
            "pressure_type": "Category",
        },
        title=f"Defensive Impact \u2014 {player_name} ({n_matches} matches, {total_actions} defensive actions)",
    )

    plotly_fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        font={"color": "white"},
        xaxis_tickangle=-45,
        legend={"title": "Pressure Type"},
        height=500,
    )

    return plotly_fig


# ---------------------------------------------------------------------------
# Pass Timing (PAUSA)
# ---------------------------------------------------------------------------


def _get_pausa_matches() -> list[str]:
    """Return sorted list of match IDs from PAUSA data."""
    if pausa_df.empty or "match_id" not in pausa_df.columns:
        return []
    return sorted(pausa_df["match_id"].unique().tolist())


def _get_pausa_teams(match_id: str) -> list[str]:
    """Return sorted list of teams for a match."""
    if pausa_df.empty or "team" not in pausa_df.columns:
        return []
    subset = pausa_df[pausa_df["match_id"] == match_id]
    teams = subset["team"].dropna().unique().tolist()
    return ["All"] + sorted(teams)


def _get_pausa_players(match_id: str, team: str) -> list[str]:
    """Return sorted list of player names for a match/team."""
    if pausa_df.empty:
        return []
    subset = pausa_df[pausa_df["match_id"] == match_id]
    if team and team != "All" and "team" in subset.columns:
        subset = subset[subset["team"] == team]
    name_col = "player_display_name" if "player_display_name" in subset.columns else "player_id"
    players = subset[name_col].dropna().unique().tolist()
    return ["All"] + sorted([str(p) for p in players])


def create_pausa_scatter(match_id: str, team: str, player: str) -> go.Figure:
    """Create PAUSA temporal vs spatial scatter plot."""
    if pausa_df.empty:
        return go.Figure().update_layout(
            template="plotly_dark", paper_bgcolor="#1a1a2e",
            title="No PAUSA data available",
        )

    df = pausa_df[pausa_df["match_id"] == match_id].copy()
    if team and team != "All" and "team" in df.columns:
        df = df[df["team"] == team]

    name_col = "player_display_name" if "player_display_name" in df.columns else "player_id"
    if player and player != "All":
        df = df[df[name_col].astype(str) == player]

    if df.empty:
        return go.Figure().update_layout(
            template="plotly_dark", paper_bgcolor="#1a1a2e",
            title="No data for selected filters",
        )

    fig = px.scatter(
        df,
        x="temporal_judgment",
        y="spatial_selection",
        size="pausa_score",
        color="team" if "team" in df.columns else None,
        hover_data=[name_col, "pausa_score"],
        labels={
            "temporal_judgment": "Temporal Judgment (0\u20131, higher = better timing)",
            "spatial_selection": "Spatial Selection (0\u20131, higher = better target)",
            "pausa_score": "PAUSA Score",
        },
        title=f"Pass Timing \u2014 {_match_label(match_id)}",
        size_max=18,
    )

    fig.add_hline(y=0.5, line_dash="dash", line_color="gray", opacity=0.4)
    fig.add_vline(x=0.5, line_dash="dash", line_color="gray", opacity=0.4)

    # Quadrant labels (CHI-AUDIT-190 Finding #5 — port from Streamlit pass_timing.py)
    _annotations = [
        {"x": 0.25, "y": 0.75, "text": "Good where,<br>poor when"},
        {"x": 0.75, "y": 0.75, "text": "Good timing<br>& target"},
        {"x": 0.25, "y": 0.25, "text": "Poor timing<br>& target"},
        {"x": 0.75, "y": 0.25, "text": "Good when,<br>poor where"},
    ]
    for ann in _annotations:
        fig.add_annotation(
            x=ann["x"],
            y=ann["y"],
            text=ann["text"],
            showarrow=False,
            font={"size": 10, "color": "gray"},
            opacity=0.6,
        )

    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#1a1a2e",
        plot_bgcolor="#1a1a2e",
        xaxis={"range": [0, 1]},
        yaxis={"range": [0, 1]},
        height=500,
    )
    return fig


def create_pausa_heatmap(match_id: str, team: str, player: str) -> plt.Figure:
    """Create OBSO receiver location heatmap on pitch."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color="#1a1a2e", line_color="#cccccc")
    fig, ax = pitch.draw(figsize=(12, 8))
    fig.set_facecolor("#1a1a2e")

    if pausa_df.empty:
        ax.text(60, 40, "No PAUSA data available.", ha="center", va="center", color="white", fontsize=14)
        return fig

    df = pausa_df[pausa_df["match_id"] == match_id].copy()
    if team and team != "All" and "team" in df.columns:
        df = df[df["team"] == team]

    name_col = "player_display_name" if "player_display_name" in df.columns else "player_id"
    if player and player != "All":
        df = df[df[name_col].astype(str) == player]

    if df.empty or "receiver_x" not in df.columns or "receiver_y" not in df.columns:
        ax.text(60, 40, "No receiver data for selection.", ha="center", va="center", color="white", fontsize=14)
        return fig

    valid = df.dropna(subset=["receiver_x", "receiver_y"])
    if valid.empty:
        ax.text(60, 40, "No receiver coordinates available.", ha="center", va="center", color="white", fontsize=14)
        return fig

    # Scatter receivers colored by OBSO value
    scatter = ax.scatter(
        valid["receiver_x"],
        valid["receiver_y"],
        c=valid["actual_obso"],
        cmap="YlOrRd",
        s=40,
        alpha=0.7,
        edgecolors="white",
        linewidth=0.3,
        zorder=3,
        vmin=0,
    )
    cbar = fig.colorbar(scatter, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("OBSO at Receiver", color="white", fontsize=9)
    cbar.ax.tick_params(colors="white", labelsize=8)

    n_passes = len(valid)
    avg_pausa = float(valid["pausa_score"].mean()) if "pausa_score" in valid.columns else 0.0
    ax.set_title(
        f"OBSO at Receiver \u2014 {n_passes} passes, Avg PAUSA {avg_pausa:.3f} (0\u20131, higher = better)",
        color="white", fontsize=13, pad=8,
    )

    plt.tight_layout()
    return fig


def create_pausa_rankings(match_id: str) -> pd.DataFrame:
    """Create per-player PAUSA rankings table."""
    if pausa_df.empty:
        return pd.DataFrame({"message": ["No PAUSA data loaded."]})

    df = pausa_df[pausa_df["match_id"] == match_id].copy()
    if df.empty:
        return pd.DataFrame({"message": ["No data for this match."]})

    name_col = "player_display_name" if "player_display_name" in df.columns else "player_id"
    agg = df.groupby(name_col).agg(
        passes=("pausa_score", "count"),
        avg_pausa=("pausa_score", "mean"),
        avg_temporal=("temporal_judgment", "mean"),
        avg_spatial=("spatial_selection", "mean"),
    ).reset_index()

    agg = agg.sort_values("avg_pausa", ascending=False)
    agg = agg.rename(columns={
        name_col: "Player",
        "passes": "Passes",
        "avg_pausa": "Avg PAUSA (higher = better timing + target)",
        "avg_temporal": "Avg Temporal (higher = better timing)",
        "avg_spatial": "Avg Spatial (higher = better target)",
    })
    return agg.round(3)


def _update_pausa_teams(match_id: str) -> gr.Dropdown:
    """Update team dropdown when match changes."""
    teams = _get_pausa_teams(match_id)
    return gr.Dropdown(choices=teams, value=teams[0] if teams else "All", filterable=True)


def _update_pausa_players(match_id: str, team: str) -> gr.Dropdown:
    """Update player dropdown when match/team changes."""
    players = _get_pausa_players(match_id, team)
    return gr.Dropdown(choices=players, value=players[0] if players else "All", filterable=True)


# ---------------------------------------------------------------------------
# Per-tab glossary (CHI-AUDIT-190 Finding #1)
# ---------------------------------------------------------------------------

_TAB_GLOSSARY: dict[str, dict[str, str]] = {
    "Pass Quality": {
        "Line-Breaking Pass": (
            "A pass that penetrates at least one defensive line, detected via "
            "Ward clustering on 360 freeze-frame defender positions."
        ),
        "Progressive Pass": (
            "A pass that moves the ball significantly closer to the opponent's goal."
        ),
    },
    "Pass Timing": {
        "PAUSA": (
            "Passing Ability Under Spatiotemporal Awareness. Composite of temporal "
            "judgment \u00d7 spatial selection. Higher = better pass timing and target "
            "choice. (Lee et al., MIT Sloan 2026)"
        ),
        "Temporal Judgment": (
            "Was the pass released at the optimal moment? Ratio of actual OBSO at "
            "release to peak OBSO. 1.0 = perfect timing."
        ),
        "Spatial Selection": (
            "Was the target location the best available? Ratio of actual OBSO at "
            "target to maximum OBSO across all receivers. 1.0 = optimal target."
        ),
        "OBSO": (
            "Off-Ball Scoring Opportunity. Continuous value surface: Pitch Control "
            "\u00d7 Ball Transition \u00d7 Expected Possession Value. (Spearman 2018)"
        ),
    },
    "Pitch Control": {
        "Pitch Control": (
            "Physics-based model estimating which team controls each point on the "
            "pitch, based on player positions, velocities, and time-to-intercept."
        ),
    },
    "Player Similarity": {
        "Cosine Distance": (
            "Similarity measure between player embedding vectors. "
            "0.0 = identical playing style, 1.0 = completely different."
        ),
    },
    "Shot Map": {
        "xG (Expected Goals)": (
            "Probability of scoring from each shot's location and context. "
            "Higher = better chance. Sum over a match = team's expected output."
        ),
    },
    "Defensive Impact": {
        "DEFCON": (
            "Defensive Contribution framework (Kim et al. 2025) \u2014 quantifies how "
            "defenders affect an attacker's scoring probability via four categories."
        ),
        "Intercept": "Defender successfully won the ball from the attacker.",
        "Concede": "Attacker received a shot or goal despite defensive pressure.",
        "Disturb": "Defender disrupted the attacker's possession without winning the ball.",
        "Deter": "Defender's presence prevented the attacker from progressing.",
    },
}


def _render_tab_glossary(tab_name: str) -> None:
    """Render a context-filtered glossary accordion at the bottom of a tab."""
    terms = _TAB_GLOSSARY.get(tab_name, {})
    if not terms:
        return
    lines = [f"**{term}:** {defn}" for term, defn in terms.items()]
    with gr.Accordion("Glossary", open=False):
        gr.Markdown("\n\n".join(lines))


# ---------------------------------------------------------------------------
# Gradio App
# ---------------------------------------------------------------------------

_SLIDER_JS = """
function() {
  function fmt(n) {
    n = parseInt(n);
    if (isNaN(n) || n < 0) return String(n);
    var m = Math.floor(n / 60);
    var s = n % 60;
    return (m < 10 ? '0' : '') + m + ':' + (s < 10 ? '0' : '') + s;
  }
  function reformatSlider() {
    var el = document.getElementById('pc-time-slider');
    if (!el) return;
    el.querySelectorAll('span').forEach(function(span) {
      var text = span.textContent.trim();
      if (/^\\d+$/.test(text) && parseInt(text) >= 0) {
        span.textContent = fmt(text);
      }
    });
  }
  var obs = new MutationObserver(reformatSlider);
  function init() {
    var el = document.getElementById('pc-time-slider');
    if (el) {
      obs.observe(el, {childList: true, subtree: true, characterData: true});
      reformatSlider();
    } else {
      setTimeout(init, 500);
    }
  }
  init();
}
"""

# ---------------------------------------------------------------------------
# Luxury Flagship theme — dark surfaces, gold accents, sharp corners
# ---------------------------------------------------------------------------
_FLAGSHIP_THEME = gr.themes.Monochrome(
    primary_hue="amber",
    secondary_hue="stone",
    neutral_hue="zinc",
    radius_size=gr.themes.sizes.radius_none,
    font=(gr.themes.GoogleFont("Inter"), "system-ui", "sans-serif"),
    font_mono=(gr.themes.GoogleFont("JetBrains Mono"), "ui-monospace", "monospace"),
).set(
    body_background_fill="#0f0f14",
    body_background_fill_dark="#0f0f14",
    body_text_color="#e4e4e7",
    body_text_color_dark="#e4e4e7",
    body_text_color_subdued="#71717a",
    body_text_color_subdued_dark="#71717a",
    background_fill_primary="#18181f",
    background_fill_primary_dark="#18181f",
    background_fill_secondary="#1f1f28",
    background_fill_secondary_dark="#1f1f28",
    block_background_fill="#1f1f28",
    block_background_fill_dark="#1f1f28",
    block_border_color="#2e2e3a",
    block_border_color_dark="#2e2e3a",
    block_border_width="1px",
    block_shadow="0 2px 12px rgba(0,0,0,0.5)",
    block_shadow_dark="0 2px 12px rgba(0,0,0,0.5)",
    block_label_text_color="#f59e0b",
    block_label_text_color_dark="#f59e0b",
    block_label_background_fill="#18181f",
    block_label_background_fill_dark="#18181f",
    block_title_text_color="#f59e0b",
    block_title_text_color_dark="#f59e0b",
    input_background_fill="#18181f",
    input_background_fill_dark="#18181f",
    input_border_color="#3f3f4a",
    input_border_color_dark="#3f3f4a",
    input_border_color_focus="#f59e0b",
    input_border_color_focus_dark="#f59e0b",
    border_color_primary="#2e2e3a",
    border_color_primary_dark="#2e2e3a",
    border_color_accent="#f59e0b",
    border_color_accent_dark="#f59e0b",
    button_primary_background_fill="#f59e0b",
    button_primary_background_fill_hover="#fbbf24",
    button_primary_background_fill_dark="#f59e0b",
    button_primary_background_fill_hover_dark="#fbbf24",
    button_primary_text_color="#0f0f14",
    button_primary_text_color_dark="#0f0f14",
    button_secondary_background_fill="transparent",
    button_secondary_background_fill_dark="transparent",
    button_secondary_border_color="#3f3f4a",
    button_secondary_border_color_dark="#3f3f4a",
    button_secondary_text_color="#e4e4e7",
    button_secondary_text_color_dark="#e4e4e7",
    slider_color="#f59e0b",
    slider_color_dark="#f59e0b",
    link_text_color="#f59e0b",
    link_text_color_dark="#f59e0b",
    table_even_background_fill="#1f1f28",
    table_even_background_fill_dark="#1f1f28",
    table_odd_background_fill="#18181f",
    table_odd_background_fill_dark="#18181f",
    table_border_color="#2e2e3a",
    table_border_color_dark="#2e2e3a",
    panel_background_fill="#18181f",
    panel_background_fill_dark="#18181f",
    code_background_fill="#0f0f14",
    code_background_fill_dark="#0f0f14",
)

_FLAGSHIP_CSS = """
/* --- Tab navigation: prominent gold-accented pills --- */
.tab-nav {
    background: #18181f !important;
    border-bottom: 2px solid #2e2e3a !important;
    padding: 8px 12px 0 12px !important;
    gap: 4px !important;
}
.tab-nav button {
    font-size: 0.95rem !important;
    font-weight: 600 !important;
    padding: 10px 22px !important;
    color: #a1a1aa !important;
    background: transparent !important;
    border: none !important;
    border-bottom: 3px solid transparent !important;
    transition: all 0.2s ease !important;
    margin-bottom: -2px !important;
}
.tab-nav button:hover {
    color: #e4e4e7 !important;
    background: rgba(245, 158, 11, 0.08) !important;
}
.tab-nav button[aria-selected="true"] {
    color: #f59e0b !important;
    border-bottom: 3px solid #f59e0b !important;
    background: rgba(245, 158, 11, 0.06) !important;
}
/* Style time-slider non-range inputs to be compact but accessible (pitch control) */
#pc-time-slider input:not([type='range']), #pc-time-slider button { opacity: 0.4; max-width: 50px; }
"""

demo = gr.Blocks(
    title="Soccer Analytics Explorer",
    theme=_FLAGSHIP_THEME,
    js=_SLIDER_JS,
    css=_FLAGSHIP_CSS,
)

with demo:
    gr.Markdown(
        """
    # Soccer Analytics Explorer

    Interactive demo for the [Luxury Lakehouse](https://huggingface.co/luxury-lakehouse)
    soccer analytics platform. Explore player embeddings, shot maps, pass quality,
    pitch control surfaces, and defensive pressure profiles from open-source soccer data.

    > *This Space runs on free CPU. First load may take 30-60 seconds while the container starts.
    > The full platform has 12 analysis pages with 380+ matches across 5 data providers.*

    > **Getting started:** Click any tab to explore. Start with **Shot Map** for an overview,
    > then try **Player Similarity** to find comparable players. Each tab has a
    > **Glossary** accordion at the bottom with term definitions.

    **Data:** [StatsBomb](https://github.com/statsbomb/open-data) ·
    [Wyscout](https://figshare.com/collections/Soccer_match_event_dataset/4415000) ·
    [Metrica](https://github.com/metrica-sports/sample-data) (all CC-BY 4.0)
    &nbsp;|&nbsp;
    **Models:** [football2vec](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) ·
    [xG](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout)
    &nbsp;|&nbsp;
    **Datasets:** [SPADL/VAEP](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) ·
    [Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) ·
    [Pitch Control](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) ·
    [OBSO/PAUSA](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values)
    """
    )

    with gr.Tab("Pass Quality"):
        gr.Markdown(
            "Pass origins on the pitch with line-breaking passes highlighted in red.\n\n"
            "*A line-breaking pass penetrates at least one defensive line (detected via Ward clustering "
            "on StatsBomb 360 freeze-frame positions). Algorithm adapted from "
            "[Parma Calcio 1913 line-breaking-passes](https://github.com/parmacalcio1913/line-breaking-passes) "
            "(Apache-2.0). Sample of 2,000 passes from Women's World Cup matches.*"
        )
        _pass_type_choices = (
            ["All", *sorted(passes_df["pass_type"].dropna().unique().tolist())] if not passes_df.empty else ["All"]
        )
        with gr.Row():
            pass_type_dd = gr.Dropdown(
                choices=_pass_type_choices,
                value="All",
                label="Pass type",
                interactive=True,
                filterable=True,
            )
            lb_toggle = gr.Checkbox(value=True, label="Highlight line-breaking")
            completed_toggle = gr.Checkbox(value=False, label="Completed only")
        _pass_inputs = [lb_toggle, pass_type_dd, completed_toggle]
        pass_plot = gr.Plot(label="Pass Map", value=create_pass_map(True, "All", False))
        pass_type_dd.change(fn=create_pass_map, inputs=_pass_inputs, outputs=pass_plot)
        lb_toggle.change(fn=create_pass_map, inputs=_pass_inputs, outputs=pass_plot)
        completed_toggle.change(fn=create_pass_map, inputs=_pass_inputs, outputs=pass_plot)
        _render_tab_glossary("Pass Quality")

    with gr.Tab("Pass Timing"):
        gr.Markdown(
            "PAUSA pass quality: temporal judgment (when) \u00d7 spatial selection (where).\n\n"
            "*[Lee, Jo, Hong, Bauer & Ko (2026)](https://github.com/leemingo/mitssac-pausa) "
            "PAUSA metric from MIT Sloan 2026. OBSO value surface by "
            "[Spearman (2018)](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals). "
            "Event-tracking sync via [Kim et al. (2025)](https://arxiv.org/abs/2508.09238) ELASTIC. "
            "Data from 7 IDSSE Bundesliga matches (CC-BY 4.0).*"
        )
        _pausa_matches = _get_pausa_matches()
        _pausa_default_match = _pausa_matches[0] if _pausa_matches else ""
        _pausa_default_teams = _get_pausa_teams(_pausa_default_match) if _pausa_default_match else ["All"]
        _pausa_default_players = (
            _get_pausa_players(_pausa_default_match, "All") if _pausa_default_match else ["All"]
        )

        if not _pausa_matches:
            gr.Markdown(
                "> **No PAUSA data available.** Pass timing analysis requires OBSO computation "
                "and PAUSA pipeline data. Check back after the next data update."
            )
        else:
            with gr.Row():
                pausa_match_dd = gr.Dropdown(
                    choices=[(_match_label(m), m) for m in _pausa_matches] if _pausa_matches else [],
                    value=_pausa_default_match,
                    label="Match",
                    interactive=True,
                )
                pausa_team_dd = gr.Dropdown(
                    choices=_pausa_default_teams,
                    value="All",
                    label="Team",
                    interactive=True,
                )
                pausa_player_dd = gr.Dropdown(
                    choices=_pausa_default_players,
                    value="All",
                    label="Player",
                    interactive=True,
                    filterable=True,
                )

            pausa_heatmap = gr.Plot(label="Receiver Locations (Off-Ball Scoring Opportunity)")
            pausa_scatter = gr.Plot(label="Temporal vs Spatial")
            pausa_rankings = gr.Dataframe(label="Player Rankings")

            _pausa_scatter_inputs = [pausa_match_dd, pausa_team_dd, pausa_player_dd]
            _pausa_heatmap_inputs = [pausa_match_dd, pausa_team_dd, pausa_player_dd]

            pausa_match_dd.change(
                fn=_update_pausa_teams, inputs=[pausa_match_dd], outputs=[pausa_team_dd]
            )
            pausa_match_dd.change(
                fn=_update_pausa_players, inputs=[pausa_match_dd, pausa_team_dd], outputs=[pausa_player_dd]
            )
            pausa_team_dd.change(
                fn=_update_pausa_players, inputs=[pausa_match_dd, pausa_team_dd], outputs=[pausa_player_dd]
            )

            pausa_match_dd.change(
                fn=create_pausa_heatmap, inputs=_pausa_heatmap_inputs, outputs=pausa_heatmap
            )
            pausa_match_dd.change(
                fn=create_pausa_scatter, inputs=_pausa_scatter_inputs, outputs=pausa_scatter
            )
            pausa_match_dd.change(
                fn=create_pausa_rankings, inputs=[pausa_match_dd], outputs=pausa_rankings
            )
            pausa_team_dd.change(
                fn=create_pausa_heatmap, inputs=_pausa_heatmap_inputs, outputs=pausa_heatmap
            )
            pausa_team_dd.change(
                fn=create_pausa_scatter, inputs=_pausa_scatter_inputs, outputs=pausa_scatter
            )
            pausa_player_dd.change(
                fn=create_pausa_heatmap, inputs=_pausa_heatmap_inputs, outputs=pausa_heatmap
            )
            pausa_player_dd.change(
                fn=create_pausa_scatter, inputs=_pausa_scatter_inputs, outputs=pausa_scatter
            )

            demo.load(fn=create_pausa_heatmap, inputs=_pausa_heatmap_inputs, outputs=pausa_heatmap)
            demo.load(fn=create_pausa_scatter, inputs=_pausa_scatter_inputs, outputs=pausa_scatter)
            demo.load(fn=create_pausa_rankings, inputs=[pausa_match_dd], outputs=pausa_rankings)

            gr.Markdown(
                "**Column guide:**\n"
                "- **Avg PAUSA**: Composite score (temporal \u00d7 spatial). Higher = better timing AND target.\n"
                "- **Avg Temporal**: Was the pass released at the best moment? 1.0 = perfect timing.\n"
                "- **Avg Spatial**: Was the target the best option? 1.0 = optimal receiver choice.\n"
                "- **Passes**: Number of evaluated passes (more = more reliable average)."
            )
        _render_tab_glossary("Pass Timing")

    with gr.Tab("Pitch Control"):
        gr.Markdown(
            "Physics-based pitch control surfaces computed from tracking data.\n\n"
            "*Model by [Spearman (2017)](https://www.researchgate.net/publication/315166647_Beyond_Expected_Goals) "
            '"Beyond Expected Goals." '
            "Uses kinematic time-to-intercept equations accounting for player positions, "
            "velocities, and reaction time. Data from "
            "[Metrica Sports sample games](https://github.com/metrica-sports/sample-data) (CC-BY 4.0).*"
        )
        _tracking_matches = _get_tracking_matches()
        _default_match = _tracking_matches[0] if _tracking_matches else ""
        _default_fps = _get_frame_rate(_default_match) if _default_match else 25
        _default_duration = _get_duration_seconds(_default_match, 1) if _default_match else 0

        with gr.Row():
            pc_match = gr.Dropdown(
                choices=[(_match_label(m), m) for m in _tracking_matches] if _tracking_matches else [],
                value=_default_match,
                label="Match",
                interactive=True,
            )
            pc_period = gr.Radio(
                choices=[1, 2],
                value=1,
                label="Half",
            )
        with gr.Row():
            pc_time = gr.Slider(
                minimum=0,
                maximum=_default_duration,
                value=0,
                step=1,
                label=f"Time: 00:00 \u2014 {_fmt_time(_default_duration)} ({_default_fps} fps)",
                elem_id="pc-time-slider",
            )
            pc_velocity = gr.Checkbox(value=True, label="Show velocity arrows")

        pc_plot = gr.Plot(label="Pitch Control")

        # Event bindings: match/half change updates slider range
        pc_match.change(fn=_update_time_slider, inputs=[pc_match, pc_period], outputs=[pc_time])
        pc_period.change(fn=_update_time_slider, inputs=[pc_match, pc_period], outputs=[pc_time])

        # Time slider or velocity toggle triggers plot refresh
        _pc_inputs = [pc_match, pc_period, pc_time, pc_velocity]
        pc_time.release(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)
        pc_velocity.change(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)

        # Render initial plot after page loads (avoids blocking startup)
        demo.load(fn=create_pitch_control_plot, inputs=_pc_inputs, outputs=pc_plot)
        _render_tab_glossary("Pitch Control")

    with gr.Tab("Player Similarity"):
        gr.Markdown(
            "Find players with similar playing styles using Doc2Vec behavioral embeddings.\n\n"
            "*Embeddings via [Theiner et al. (2022)](https://doi.org/10.1007/978-3-031-02044-5_2) football2vec "
            "with [Doc2Vec (Le & Mikolov 2014)](https://arxiv.org/abs/1405.4053). "
            "Match counts reflect games in the open dataset; higher counts indicate more robust similarity.*"
        )
        with gr.Row():
            player_dropdown = gr.Dropdown(
                choices=_similarity_players,
                value=_default_similarity_player,
                label="Player",
                info=f"{len(_similarity_players)} players available",
                interactive=True,
                filterable=True,
            )
            top_k_input = gr.Slider(minimum=5, maximum=50, value=10, step=1, label="Top K results")
        _initial_similarity = (
            find_similar_players(_default_similarity_player) if _default_similarity_player else pd.DataFrame()
        )
        results_table = gr.Dataframe(label="Similar Players", value=_initial_similarity)
        gr.Markdown(
            "*Interpretation: cosine distance < 0.20 = Very Similar, "
            "< 0.35 = Similar, < 0.50 = Moderately Similar, \u2265 0.50 = Different. "
            "Lower distance = more similar playing style.*"
        )
        player_dropdown.change(fn=find_similar_players, inputs=[player_dropdown, top_k_input], outputs=results_table)
        top_k_input.change(fn=find_similar_players, inputs=[player_dropdown, top_k_input], outputs=results_table)
        _render_tab_glossary("Player Similarity")

    with gr.Tab("Shot Map"):
        gr.Markdown(
            "Visualize shot locations colored by outcome (goal in red, no goal in blue).\n\n"
            "*Sample of 1,000 shots across 39 matches from StatsBomb Open Data — "
            "La Liga, Serie A, Bundesliga, Ligue 1, Women's World Cup, and Africa Cup of Nations.*"
        )
        _shot_comp_choices = (
            [("All competitions", "All")]
            + [(_comp_label(c), str(c)) for c in sorted(shots_df["competition_id"].unique())]
            if not shots_df.empty
            else []
        )
        with gr.Row():
            shot_comp = gr.Dropdown(choices=_shot_comp_choices, value="All", label="Competition", interactive=True)
            shot_goals_only = gr.Checkbox(value=False, label="Goals only")
        shot_plot = gr.Plot(label="Shot Map", value=create_shot_map("All", False))
        shot_comp.change(fn=create_shot_map, inputs=[shot_comp, shot_goals_only], outputs=shot_plot)
        shot_goals_only.change(fn=create_shot_map, inputs=[shot_comp, shot_goals_only], outputs=shot_plot)
        _render_tab_glossary("Shot Map")

    with gr.Tab("Defensive Impact"):
        gr.Markdown(
            "Defensive impact profiles per player per match.\n\n"
            "*[Kim et al. (2025)](https://github.com/hyunsungkim-ds/defcon) DEFCON (Defensive Contribution) "
            "quantifies how each defender's actions affect the "
            "probability of the attacking team scoring. Four categories: **Intercept** (ball won), "
            "**Concede** (shot/goal allowed), **Disturb** (disrupted possession), and **Deter** "
            "(prevented progression). Data from 323 StatsBomb 360 matches.*"
        )
        defcon_dropdown = gr.Dropdown(
            choices=_pressure_players,
            value=_default_pressure_player,
            label="Player",
            info=f"{len(_pressure_players)} players available",
            interactive=True,
            filterable=True,
        )

        _initial_defcon = create_pressure_chart(_default_pressure_player) if _default_pressure_player else None
        defcon_plot = gr.Plot(label="Defensive Impact Breakdown", value=_initial_defcon)

        defcon_dropdown.change(fn=create_pressure_chart, inputs=[defcon_dropdown], outputs=defcon_plot)
        _render_tab_glossary("Defensive Impact")

    gr.Markdown(
        """
    ---
    *This is a sample demo with pre-cached data subsets. The full production platform
    adds: 380+ matches across 5 data providers, custom xG model comparison,
    match summary scorecards, player comparison radars, PPDA pressing analysis, and
    cross-player entity resolution across 11,918 unified players.*

    **Published datasets:**
    [SPADL/VAEP](https://huggingface.co/datasets/luxury-lakehouse/spadl-vaep-action-values) |
    [Line-Breaking Passes](https://huggingface.co/datasets/luxury-lakehouse/line-breaking-passes) |
    [Player Embeddings](https://huggingface.co/datasets/luxury-lakehouse/football2vec-player-embeddings) |
    [Pitch Control](https://huggingface.co/datasets/luxury-lakehouse/pitch-control-tracking) |
    [OBSO/PAUSA Inputs](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-inputs) |
    [OBSO/PAUSA Values](https://huggingface.co/datasets/luxury-lakehouse/obso-pausa-values)

    **Models:** [football2vec](https://huggingface.co/luxury-lakehouse/football2vec-statsbomb-wyscout) |
    [xG model](https://huggingface.co/luxury-lakehouse/xg-model-statsbomb-wyscout) |
    **Tracking data:** [Metrica Sports](https://github.com/metrica-sports/sample-data) (CC-BY 4.0)
    """
    )

if __name__ == "__main__":
    demo.launch()
