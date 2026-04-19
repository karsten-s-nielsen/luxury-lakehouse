"""Pass Map state — pm_ prefixed variables, data fetch, pitch rendering, metrics.

Progressive passes per Suzuki et al. (2019). Line-breaking detection via Ward
clustering adapted from Parma Calcio 1913 (Apache-2.0).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from filters import build_scope_label_plain, build_warning, fetch_data_freshness
from matplotlib.lines import Line2D
from mplsoccer import Pitch
from queries.passes import fetch_passes
from render import (
    PITCH_BG_COLOR,
    PITCH_LINE_COLOR,
    fmt_int,
    pitch_to_file,
)

# matplotlib.use("Agg") is set by render.py at module load (imported above).
from state.shared import _ALL_LABEL, get_comp_id, get_match_id, get_team_id, register_page_refresher

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pass category colors (match Streamlit pitch.py palette)
# ---------------------------------------------------------------------------
_COMPLETE_COLOR = "#457b9d"
_INCOMPLETE_COLOR = "#6c757d"
_PROGRESSIVE_COLOR = "#2a9d8f"
_LINE_BREAKING_COLOR = "#f4a261"

# ---------------------------------------------------------------------------
# Exported state variables (all pm_ prefixed)
# ---------------------------------------------------------------------------
pm_total: str = "--"
pm_completed: str = "--"
pm_progressive: str = "--"
pm_line_breaking: str = "--"
pm_completion_pct: str = "--"
pm_pitch_image: str = ""
pm_show_progressive: bool = True
pm_show_line_breaking: bool = True

pm_warning_text: str = ""
pm_scope_comp: str = ""
pm_scope_team: str = ""
pm_scope_match: str = ""
pm_data_freshness: str = ""
pm_pitch_image_alt: str = ""

__all__ = [
    "on_pm_toggle_change",
    "pm_completed",
    "pm_completion_pct",
    "pm_data_freshness",
    "pm_line_breaking",
    "pm_pitch_image",
    "pm_pitch_image_alt",
    "pm_progressive",
    "pm_refresh",
    "pm_scope_comp",
    "pm_scope_match",
    "pm_scope_team",
    "pm_show_line_breaking",
    "pm_show_progressive",
    "pm_total",
    "pm_warning_text",
]


# ---------------------------------------------------------------------------
# Pass categorization (mirrors streamlit_app.components.pitch.categorize_passes)
# ---------------------------------------------------------------------------


def _categorize_passes(
    passes: pd.DataFrame,
    highlight_progressive: bool,
    highlight_line_breaking: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Categorize passes into (incomplete, complete, progressive, line_breaking).

    Incomplete passes are always grey regardless of other flags.
    Among completed passes: line-breaking > progressive > complete hierarchy.
    """
    has_lb = highlight_line_breaking and "is_line_breaking" in passes.columns

    if "is_complete" in passes.columns:
        incomplete = pd.DataFrame(passes[passes["is_complete"] != 1])
        completed = pd.DataFrame(passes[passes["is_complete"] == 1])
    else:
        incomplete = pd.DataFrame()
        completed = passes

    if has_lb:
        lb = pd.DataFrame(completed[completed["is_line_breaking"] == 1])
        remaining = pd.DataFrame(completed[completed["is_line_breaking"] != 1])
    else:
        lb = pd.DataFrame()
        remaining = completed

    if highlight_progressive and "is_progressive" in remaining.columns:
        prog = pd.DataFrame(remaining[remaining["is_progressive"] == 1])
        complete = pd.DataFrame(remaining[remaining["is_progressive"] != 1])
    else:
        prog = pd.DataFrame()
        complete = remaining

    return incomplete, complete, prog, lb


# ---------------------------------------------------------------------------
# Pitch rendering
# ---------------------------------------------------------------------------


def _render_pass_map(
    passes: pd.DataFrame,
    highlight_progressive: bool,
    highlight_line_breaking: bool,
) -> str:
    """Render pass map to temp PNG via mplsoccer, return file path."""
    pitch = Pitch(pitch_type="statsbomb", pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    result: Any = pitch.draw(figsize=(12, 8))
    fig = result[0]
    ax = result[1]
    fig.set_facecolor(PITCH_BG_COLOR)

    if passes.empty:
        ax.set_title("Pass Map", color=PITCH_LINE_COLOR, fontsize=14, pad=10)
        return pitch_to_file(fig, "pitch_pass_map")

    incomplete, complete, prog, lb = _categorize_passes(passes, highlight_progressive, highlight_line_breaking)

    for subset, color, alpha, width in [
        (incomplete, _INCOMPLETE_COLOR, 0.3, 1.0),
        (complete, _COMPLETE_COLOR, 0.5, 1.5),
        (prog, _PROGRESSIVE_COLOR, 0.8, 2.0),
        (lb, _LINE_BREAKING_COLOR, 0.9, 2.5),
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

    # Legend
    legend_entries: list[tuple[str, str, float, float]] = [
        ("Incomplete", _INCOMPLETE_COLOR, 0.5, 1.0),
        ("Complete", _COMPLETE_COLOR, 0.7, 1.5),
    ]
    if highlight_progressive:
        legend_entries.append(("Progressive", _PROGRESSIVE_COLOR, 0.9, 2.0))
    if highlight_line_breaking and "is_line_breaking" in passes.columns:
        legend_entries.append(("Line-Breaking", _LINE_BREAKING_COLOR, 0.95, 2.5))

    handles = [Line2D([0], [0], color=c, alpha=a, linewidth=w * 2, label=lbl) for lbl, c, a, w in legend_entries]
    ax.legend(
        handles=handles,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.08),
        ncol=len(handles),
        fontsize=9,
        frameon=False,
        labelcolor=PITCH_LINE_COLOR,
    )

    ax.set_title("Pass Map", color=PITCH_LINE_COLOR, fontsize=14, pad=10)
    return pitch_to_file(fig, "pitch_pass_map")


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------


def _compute_metrics(passes: pd.DataFrame) -> dict[str, str]:
    """Compute pass map metrics from fetched pass data."""
    total = len(passes)
    if total == 0:
        return {
            "total": "0",
            "completed": "0",
            "progressive": "0",
            "line_breaking": "0",
            "completion_pct": "0.0%",
        }

    completed = int(passes["is_complete"].sum()) if "is_complete" in passes.columns else 0
    complete_mask = passes["is_complete"] == 1 if "is_complete" in passes.columns else passes.index.notnull()
    progressive = int(passes.loc[complete_mask, "is_progressive"].sum()) if "is_progressive" in passes.columns else 0
    line_breaking = (
        int(passes.loc[complete_mask, "is_line_breaking"].sum()) if "is_line_breaking" in passes.columns else 0
    )
    pct = completed / total * 100 if total > 0 else 0.0

    return {
        "total": fmt_int(total),
        "completed": fmt_int(completed),
        "progressive": fmt_int(progressive),
        "line_breaking": fmt_int(line_breaking),
        "completion_pct": f"{pct:.1f}%",
    }


# ---------------------------------------------------------------------------
# Refresh callback
# ---------------------------------------------------------------------------

# Module-level cache for re-rendering on toggle changes without re-fetching
_cached_passes: pd.DataFrame = pd.DataFrame()


def pm_refresh(state: Any) -> None:
    """Fetch passes, compute metrics, render pitch image.

    Called by shared.on_*_change callbacks when filters update, and also
    when progressive/line-breaking toggles change.
    """
    global _cached_passes

    comp_id = get_comp_id(state.selected_competition)
    team_id = get_team_id(state.selected_team)
    match_id = get_match_id(state.selected_match)

    if comp_id is None or team_id is None or match_id is None:
        state.pm_total = "--"
        state.pm_completed = "--"
        state.pm_progressive = "--"
        state.pm_line_breaking = "--"
        state.pm_completion_pct = "--"
        state.pm_pitch_image = ""
        state.pm_pitch_image_alt = ""
        state.pm_warning_text = ""
        state.pm_scope_comp = ""
        state.pm_scope_team = ""
        state.pm_scope_match = ""
        state.pm_data_freshness = ""
        _cached_passes = pd.DataFrame()
        return

    # Scope dimensions (Competition, Team, Match) — canonical Tier A scope line
    comp_label = state.selected_competition or ""
    team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    match_label = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "—"
    state.pm_scope_comp = comp_label
    state.pm_scope_team = team_label
    state.pm_scope_match = match_label
    scope_plain = build_scope_label_plain([("Competition", comp_label), ("Team", team_label), ("Match", match_label)])
    state.pm_pitch_image_alt = f"Pass Map — {scope_plain}"

    try:
        passes = fetch_passes(comp_id, team_id, match_id)
        _cached_passes = passes
    except Exception:
        logger.exception("Failed to fetch passes for comp=%d team=%d match=%d", comp_id, team_id, match_id)
        state.pm_pitch_image = ""
        state.pm_data_freshness = ""
        return

    if passes.empty:
        state.pm_total = "0"
        state.pm_completed = "0"
        state.pm_progressive = "0"
        state.pm_line_breaking = "0"
        state.pm_completion_pct = "0.0%"
        state.pm_pitch_image = ""
        state.pm_warning_text = build_warning(
            domain="passes",
            suggestions=["choosing a different match", "a different team"],
        )
        state.pm_data_freshness = ""
        return

    state.pm_warning_text = ""
    metrics = _compute_metrics(passes)
    state.pm_total = metrics["total"]
    state.pm_completed = metrics["completed"]
    state.pm_progressive = metrics["progressive"]
    state.pm_line_breaking = metrics["line_breaking"]
    state.pm_completion_pct = metrics["completion_pct"]

    state.pm_pitch_image = _render_pass_map(passes, state.pm_show_progressive, state.pm_show_line_breaking)

    # Data freshness
    state.pm_data_freshness = fetch_data_freshness()

    logger.info("Pass map rendered: %s passes", metrics["total"])


def on_pm_toggle_change(state: Any, var_name: str, var_value: Any) -> None:
    """Re-render pass map when progressive/line-breaking toggles change.

    Uses cached pass data to avoid re-fetching — only re-renders the chart
    with the updated toggle values.
    """
    progressive = bool(state.pm_show_progressive)
    line_breaking = bool(state.pm_show_line_breaking)

    logger.info(
        "Toggle changed: %s=%r, progressive=%r, line_breaking=%r",
        var_name,
        var_value,
        progressive,
        line_breaking,
    )

    # If we have cached passes, re-render the chart without re-fetching
    if not _cached_passes.empty:
        state.pm_pitch_image = _render_pass_map(_cached_passes, progressive, line_breaking)
    else:
        # No cached data — do a full refresh
        pm_refresh(state)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
register_page_refresher("Pass-Map", pm_refresh)
