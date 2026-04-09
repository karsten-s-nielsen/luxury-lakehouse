"""Shot Map state module — all variables prefixed with sm_.

Loads shots, joins xG predictions, computes 6 metrics, renders pitch to file.
Registered as the Shot-Map page refresher via shared.register_page_refresher.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from filters import fetch_data_freshness, fetch_scope_label
from mplsoccer import VerticalPitch
from queries.shots import fetch_shots, fetch_xg_predictions
from render import AMBER, GRAY, PITCH_BG_COLOR, PITCH_LINE_COLOR, fmt_int, pitch_to_file

from state.shared import get_comp_id, get_player_id, get_team_id, register_page_refresher

logger = logging.getLogger(__name__)

# ── xG model label -> column mapping ─────────────────────────────────────────
_XG_MODEL_OPTIONS: dict[str, str] = {
    "StatsBomb": "statsbomb_xg",
    "Custom (Logistic)": "xg_logistic",
    "Custom (XGBoost)": "xg_gradient_boosted",
}

# ── Exported state variables (all sm_ prefixed) ─────────────────────────────
sm_total_shots: str = "--"
sm_goals: str = "--"
sm_total_xg: str = "--"
sm_conversion: str = "--"
sm_xg_per_shot: str = "--"
sm_brier: str = "--"

sm_xg_delta: str = ""
sm_xg_per_delta: str = ""
sm_brier_delta: str = ""

sm_pitch_image: str = ""

sm_scope_label: str = ""
sm_data_scope_note: str = ""
sm_nan_fallback_note: str = ""
sm_warning_text: str = ""
sm_empty_message: str = ""
sm_data_freshness: str = ""

__all__ = [
    "sm_brier",
    "sm_brier_delta",
    "sm_conversion",
    "sm_data_freshness",
    "sm_data_scope_note",
    "sm_empty_message",
    "sm_goals",
    "sm_nan_fallback_note",
    "sm_pitch_image",
    "sm_scope_label",
    "sm_total_shots",
    "sm_total_xg",
    "sm_warning_text",
    "sm_xg_delta",
    "sm_xg_per_delta",
    "sm_xg_per_shot",
]


# ── Computation ──────────────────────────────────────────────────────────────


def _join_xg_predictions(shots: pd.DataFrame, competition_id: int) -> tuple[pd.DataFrame, bool]:
    """LEFT JOIN xG predictions onto shots. Returns (merged_df, has_custom_xg)."""
    if shots.empty or "shot_id" not in shots.columns:
        return shots, False

    xg_preds = fetch_xg_predictions(competition_id)
    if xg_preds.empty:
        return shots, False

    merged = shots.merge(xg_preds, on="shot_id", how="left")
    return merged, True


def _compute_brier_score(is_goal: pd.Series, xg_values: pd.Series) -> float | None:  # type: ignore[type-arg]
    """Compute Brier score for xG predictions vs actual outcomes.

    Returns None if fewer than 10 valid observations.
    """
    valid_mask = is_goal.notna() & xg_values.notna()
    n_valid = int(valid_mask.sum())
    if n_valid < 10:
        return None
    goals = is_goal[valid_mask].astype(float)
    xg = xg_values[valid_mask].astype(float)
    return float(np.mean((xg - goals) ** 2))


# ── Rendering ────────────────────────────────────────────────────────────────


def _render_pitch(shots: pd.DataFrame, xg_col: str, player_name: str | None = None) -> str:
    """Render shot map to temp PNG. Sized by xG, colored by goal/miss."""
    pitch = VerticalPitch(half=True, pitch_color=PITCH_BG_COLOR, line_color=PITCH_LINE_COLOR)
    fig, ax = pitch.draw(figsize=(8, 10))
    title = f"Shot Map \u2014 {player_name}" if player_name else "Shot Map"
    ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)

    if not shots.empty:
        xg_series = shots[xg_col] if xg_col in shots.columns else shots["statsbomb_xg"]
        xg_sizes = xg_series.fillna(0.05) * 500 + 20

        goals_mask = shots["is_goal"] == True  # noqa: E712
        misses_mask = ~goals_mask

        if misses_mask.any():
            pitch.scatter(
                shots.loc[misses_mask, "location_x"],
                shots.loc[misses_mask, "location_y"],
                ax=ax,
                s=xg_sizes[misses_mask],
                color=GRAY,
                alpha=0.5,
                zorder=2,
                marker="o",
                label="Miss / Saved",
            )
        if goals_mask.any():
            pitch.scatter(
                shots.loc[goals_mask, "location_x"],
                shots.loc[goals_mask, "location_y"],
                ax=ax,
                s=xg_sizes[goals_mask],
                color=AMBER,
                alpha=0.9,
                zorder=3,
                edgecolors="#ffffff",
                linewidth=1,
                marker="*",
                label="Goal",
            )
        ax.legend(
            loc="upper left",
            fontsize=9,
            facecolor=PITCH_BG_COLOR,
            edgecolor="white",
            labelcolor="white",
        )

    return pitch_to_file(fig, "sm_pitch_shot_map")


# ── Refresh callback ─────────────────────────────────────────────────────────


def sm_refresh(state: Any) -> None:
    """Reload shot data, compute metrics, and render pitch for current filters."""
    comp_id = get_comp_id(state.selected_competition)
    if comp_id is None:
        # Clear all state when no competition selected
        state.sm_total_shots = "--"
        state.sm_goals = "--"
        state.sm_total_xg = "--"
        state.sm_conversion = "--"
        state.sm_xg_per_shot = "--"
        state.sm_brier = "--"
        state.sm_xg_delta = ""
        state.sm_xg_per_delta = ""
        state.sm_brier_delta = ""
        state.sm_pitch_image = ""
        state.sm_scope_label = ""
        state.sm_data_scope_note = ""
        state.sm_nan_fallback_note = ""
        state.sm_empty_message = "Select a competition to begin."
        state.sm_warning_text = ""
        state.sm_data_freshness = ""
        return

    team_id = get_team_id(state.selected_team)
    player_id = get_player_id(state.selected_player)

    # Scope label (Gergle state visibility)
    state.sm_scope_label = fetch_scope_label(comp_id, team_id)

    # Fetch shots
    shots = fetch_shots(comp_id, team_id, player_id)
    if shots.empty:
        state.sm_total_shots = "0"
        state.sm_goals = "0"
        state.sm_total_xg = "0.00"
        state.sm_conversion = "0.0%"
        state.sm_xg_per_shot = "0.000"
        state.sm_brier = "N/A"
        state.sm_xg_delta = ""
        state.sm_xg_per_delta = ""
        state.sm_brier_delta = ""
        state.sm_pitch_image = ""
        state.sm_data_scope_note = ""
        state.sm_nan_fallback_note = ""
        state.sm_empty_message = ""
        state.sm_warning_text = "No shots found for this filter combination. Try selecting a different match or player."
        state.sm_data_freshness = ""
        return

    # Clear empty/warning messages since we have data
    state.sm_empty_message = ""
    state.sm_warning_text = ""

    # Join custom xG predictions (graceful degradation)
    shots, has_custom_xg = _join_xg_predictions(shots, comp_id)

    # Data scope note when custom xG not available
    if not has_custom_xg:
        state.sm_data_scope_note = "Custom xG predictions not yet available. Showing StatsBomb xG only."
    else:
        state.sm_data_scope_note = ""

    # Resolve selected xG model column
    selected_model = state.selected_xg_model or "StatsBomb"
    if not has_custom_xg:
        selected_model = "StatsBomb"
    xg_col = _XG_MODEL_OPTIONS.get(selected_model, "statsbomb_xg")

    # Prepare xG column — overwrite statsbomb_xg with selected model for rendering
    plot_shots = shots.copy()
    nan_fallback_count = 0
    if xg_col != "statsbomb_xg" and xg_col in plot_shots.columns:
        nan_mask = plot_shots[xg_col].isna()
        nan_fallback_count = int(nan_mask.sum())
        plot_shots["statsbomb_xg"] = plot_shots[xg_col].fillna(plot_shots["statsbomb_xg"])

    # NaN fallback note
    if nan_fallback_count > 0:
        state.sm_nan_fallback_note = (
            f"{nan_fallback_count} of {len(shots)} shots use StatsBomb xG (custom model has no prediction)."
        )
    else:
        state.sm_nan_fallback_note = ""

    # --- Compute 6 metrics ---
    total = len(shots)
    n_goals = int(shots["is_goal"].sum())
    xg_series = shots[xg_col] if xg_col in shots.columns else shots["statsbomb_xg"]
    xg_sum = float(xg_series.sum())
    conversion = (n_goals / total * 100) if total > 0 else 0.0
    xg_per_shot = xg_sum / total if total > 0 else 0.0
    brier = _compute_brier_score(pd.Series(shots["is_goal"]), pd.Series(xg_series))

    state.sm_total_shots = fmt_int(total)
    state.sm_goals = fmt_int(n_goals)
    state.sm_total_xg = f"{xg_sum:.2f}"
    state.sm_conversion = f"{conversion:.1f}%"
    state.sm_xg_per_shot = f"{xg_per_shot:.3f}"
    state.sm_brier = f"{brier:.4f}" if brier is not None else "N/A"

    # --- Comparison deltas (empty when StatsBomb selected) ---
    if xg_col != "statsbomb_xg" and "statsbomb_xg" in shots.columns:
        sb_sum = float(shots["statsbomb_xg"].sum())
        sb_per = sb_sum / total if total > 0 else 0.0
        state.sm_xg_delta = f"{xg_sum - sb_sum:+.2f} vs StatsBomb"
        state.sm_xg_per_delta = f"{xg_per_shot - sb_per:+.4f} vs StatsBomb"

        sb_brier = _compute_brier_score(pd.Series(shots["is_goal"]), pd.Series(shots["statsbomb_xg"]))
        if brier is not None and sb_brier is not None:
            brier_diff = brier - sb_brier
            # Brier Score: lower is better, so negative delta = improvement
            arrow = "\u2193" if brier_diff < 0 else "\u2191" if brier_diff > 0 else ""
            state.sm_brier_delta = f"{arrow} {brier_diff:+.4f} vs StatsBomb"
        else:
            state.sm_brier_delta = ""
    else:
        state.sm_xg_delta = ""
        state.sm_xg_per_delta = ""
        state.sm_brier_delta = ""

    # --- Render pitch ---
    player_name = state.selected_player if state.selected_player not in (None, "All") else None
    state.sm_pitch_image = _render_pitch(plot_shots, "statsbomb_xg", player_name=player_name)

    # --- Data freshness ---
    state.sm_data_freshness = fetch_data_freshness()

    logger.info("Shot map refreshed: %d shots, %d goals, xG=%.2f", total, n_goals, xg_sum)


# ── Registration ─────────────────────────────────────────────────────────────
register_page_refresher("Shot-Map", sm_refresh)
