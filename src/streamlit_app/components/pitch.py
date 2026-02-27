"""mplsoccer pitch wrappers for shot and pass visualizations."""

from __future__ import annotations

from typing import Any

import matplotlib.figure
import matplotlib.pyplot as plt
import pandas as pd
from mplsoccer import Pitch, VerticalPitch

# Dark theme colors
_BG_COLOR = "#1a1a2e"
_LINE_COLOR = "#e0e0e0"
_GOAL_COLOR = "#e63946"
_NO_GOAL_COLOR = "#457b9d"
_PROGRESSIVE_COLOR = "#2a9d8f"
_COMPLETE_COLOR = "#457b9d"
_INCOMPLETE_COLOR = "#6c757d"


def plot_shot_map(shots: pd.DataFrame, title: str = "Shot Map") -> matplotlib.figure.Figure:
    """Plot shots on a half-pitch, sized by xG and colored by outcome.

    Expected columns: location_x, location_y, statsbomb_xg, is_goal.
    Returns a matplotlib Figure.
    """
    pitch = VerticalPitch(half=True, pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(8, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if shots.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        return fig

    goals = shots[shots["is_goal"] == 1]
    no_goals = shots[shots["is_goal"] != 1]

    # Scale marker size by xG (min 20, max 500)
    def _sizes(df: Any) -> Any:
        return (df["statsbomb_xg"].fillna(0.05) * 500).clip(lower=20, upper=500)

    if not no_goals.empty:
        pitch.scatter(
            no_goals["location_x"],
            no_goals["location_y"],
            s=_sizes(no_goals),
            color=_NO_GOAL_COLOR,
            alpha=0.6,
            edgecolors=_LINE_COLOR,
            linewidth=0.5,
            ax=ax,
            zorder=2,
        )
    if not goals.empty:
        pitch.scatter(
            goals["location_x"],
            goals["location_y"],
            s=_sizes(goals),
            color=_GOAL_COLOR,
            alpha=0.9,
            edgecolors=_LINE_COLOR,
            linewidth=0.5,
            ax=ax,
            zorder=3,
            marker="*",
        )

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig


def plot_pass_map(
    passes: pd.DataFrame,
    highlight_progressive: bool = True,
    title: str = "Pass Map",
) -> matplotlib.figure.Figure:
    """Plot passes as arrows on a full pitch.

    Expected columns: start_x, start_y, end_x, end_y, is_complete, is_progressive.
    Returns a matplotlib Figure.
    """
    pitch = Pitch(pitch_type="statsbomb", pitch_color=_BG_COLOR, line_color=_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig: matplotlib.figure.Figure = result[0]
    ax: Any = result[1]
    fig.set_facecolor(_BG_COLOR)

    if passes.empty:
        ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
        return fig

    if highlight_progressive and "is_progressive" in passes.columns:
        prog = passes[passes["is_progressive"] == 1]
        non_prog = passes[passes["is_progressive"] != 1]
    else:
        prog = pd.DataFrame()
        non_prog = passes

    # Split non-progressive by completion
    complete = non_prog[non_prog["is_complete"] == 1] if "is_complete" in non_prog.columns else non_prog
    incomplete = non_prog[non_prog["is_complete"] != 1] if "is_complete" in non_prog.columns else pd.DataFrame()

    for subset, color, alpha, width in [
        (incomplete, _INCOMPLETE_COLOR, 0.3, 1.0),
        (complete, _COMPLETE_COLOR, 0.5, 1.5),
        (prog, _PROGRESSIVE_COLOR, 0.8, 2.0),
    ]:
        if not subset.empty:
            pitch.arrows(
                subset["start_x"],
                subset["start_y"],
                subset["end_x"],
                subset["end_y"],
                color=color,
                alpha=alpha,
                width=width,
                ax=ax,
                headwidth=5,
                headlength=5,
            )

    ax.set_title(title, color=_LINE_COLOR, fontsize=14, pad=10)
    plt.close(fig)
    return fig
