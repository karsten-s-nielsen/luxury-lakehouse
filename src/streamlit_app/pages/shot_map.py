"""Shot Map page — visualize shots on a half-pitch with xG sizing."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from streamlit_app.components.feedback import data_scope_note, empty_result, empty_select
from streamlit_app.components.filters import render_competition_filter, render_player_filter, render_team_filter
from streamlit_app.components.glossary import METRIC_HELP
from streamlit_app.components.pitch import plot_shot_map
from streamlit_app.db import execute_query, t

logger = logging.getLogger(__name__)

# ── Model label → column mapping ──────────────────────────────────────────────
_XG_MODEL_OPTIONS: dict[str, str] = {
    "StatsBomb": "statsbomb_xg",
    "Custom (Logistic)": "xg_logistic",
    "Custom (XGBoost)": "xg_gradient_boosted",
}


@st.cache_data(ttl=600, show_spinner="Loading shots...")
def _fetch_shots(shots_tbl: str, players_tbl: str, w: str, p: tuple[Any, ...]) -> Any:
    return execute_query(
        f"SELECT s.shot_id, s.location_x, s.location_y, s.statsbomb_xg, s.is_goal, "  # noqa: S608
        f"  s.shot_outcome, s.shot_body_part, s.distance_to_goal, s.shot_angle, "
        f"  s.minute, p.player_display_name "
        f"FROM {shots_tbl} s "
        f"JOIN {players_tbl} p ON s.player_id = p.player_id "
        f"WHERE {w} "
        f"ORDER BY s.minute, s.second "
        f"LIMIT 10000",
        p,
    )


@st.cache_data(ttl=600, show_spinner="Loading xG predictions...")
def _fetch_xg_predictions(xg_tbl: str) -> pd.DataFrame:
    """Fetch custom xG predictions. Returns empty DataFrame if the table does not exist."""
    try:
        return execute_query(
            f"SELECT shot_id, xg_logistic, xg_gradient_boosted "  # noqa: S608
            f"FROM {xg_tbl} "
            f"LIMIT 100000",
        )
    except Exception:
        logger.warning("fct_xg_predictions_synced not available — custom xG disabled")
        return pd.DataFrame()


def _load_shots(
    competition_id: int,
    team_id: int | None = None,
    player_id: int | None = None,
) -> Any:
    """Load shot data from Lakebase with filters applied."""
    # L-3: Explicit type assertion before query — defense-in-depth beyond
    # Streamlit widget type enforcement and %s parameterized placeholders.
    competition_id = int(competition_id)
    conditions = ["s.competition_id = %s"]
    params: list[Any] = [competition_id]

    if team_id is not None:
        team_id = int(team_id)
        conditions.append("s.team_id = %s")
        params.append(team_id)
    if player_id is not None:
        player_id = int(player_id)
        conditions.append("s.player_id = %s")
        params.append(player_id)

    # SECURITY: `where` is built entirely from hardcoded conditions above
    # (never user input). All user-supplied values use %s parameterized placeholders.
    where = " AND ".join(conditions)

    return _fetch_shots(t("fct_shots_synced"), t("dim_players_synced"), where, tuple(params))


def _join_xg_predictions(shots: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """LEFT JOIN xG predictions onto shots. Returns (merged_df, has_custom_xg)."""
    if shots.empty or "shot_id" not in shots.columns:
        return shots, False

    xg_preds = _fetch_xg_predictions(t("fct_xg_predictions_synced"))
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


def page() -> None:
    """Render the Shot Map page."""
    st.header(":material/target: Shot Map")
    st.caption(
        "Shot locations sized by xG. StatsBomb xG or custom "
        "[XGBoost](https://xgboost.readthedocs.io/) model. "
        "Brier score measures prediction calibration."
    )

    with st.sidebar:
        competition_id = render_competition_filter()
        team_id = render_team_filter(competition_id)
        player_id = render_player_filter(competition_id, team_id)
        if isinstance(player_id, list):
            player_id = player_id[0] if player_id else None

    if competition_id is None:
        empty_select("a competition")
        return

    shots = _load_shots(competition_id, team_id, player_id)

    if shots.empty:
        empty_result("shots")
        return

    # Join custom xG predictions (graceful degradation if table missing)
    shots, has_custom_xg = _join_xg_predictions(shots)

    if not has_custom_xg:
        data_scope_note("Custom xG predictions not yet available. Showing StatsBomb xG only.")

    # Model selector in sidebar
    with st.sidebar:
        if has_custom_xg:
            selected_model = st.radio(
                "xG Model",
                list(_XG_MODEL_OPTIONS.keys()),
                index=0,
                help="StatsBomb: provider's closed-source xG. Custom Logistic: distance + angle only. "
                "Custom XGBoost: 13 features with isotonic calibration (production model).",
            )
        else:
            selected_model = "StatsBomb"

    xg_col = _XG_MODEL_OPTIONS[selected_model]

    # Prepare xG column for visualization — plot_shot_map reads "statsbomb_xg"
    # so we overwrite it with the selected model's values for rendering.
    plot_shots = shots.copy()
    nan_fallback_count = 0
    if xg_col != "statsbomb_xg" and xg_col in plot_shots.columns:
        nan_mask = plot_shots[xg_col].isna()
        nan_fallback_count = int(nan_mask.sum())
        plot_shots["statsbomb_xg"] = plot_shots[xg_col].fillna(plot_shots["statsbomb_xg"])

    if nan_fallback_count > 0:
        st.caption(f"{nan_fallback_count} of {len(shots)} shots use StatsBomb xG (custom model has no prediction).")

    col_viz, col_stats = st.columns([3, 1])

    with col_viz:
        title_parts = ["Shot Map"]
        if player_id is not None and not shots.empty:
            title_parts.append(f"— {shots['player_display_name'].iloc[0]}")
        fig = plot_shot_map(plot_shots, title=" ".join(title_parts))
        st.pyplot(fig)

    with col_stats:
        total = len(shots)
        goals = int(shots["is_goal"].sum())

        # Use selected model's xG for summary metrics
        xg_series = shots[xg_col] if xg_col in shots.columns else shots["statsbomb_xg"]
        xg_sum = float(xg_series.sum())
        conversion = (goals / total * 100) if total > 0 else 0.0
        xg_per_shot = xg_sum / total if total > 0 else 0.0

        st.metric("Total Shots", total, help=METRIC_HELP.get("Total Shots") or None)
        st.metric("Goals", goals, help=METRIC_HELP.get("Goals") or None)
        # Show delta vs StatsBomb when using a custom model (M21)
        xg_delta = None
        if xg_col != "statsbomb_xg" and "statsbomb_xg" in shots.columns:
            sb_sum = float(shots["statsbomb_xg"].sum())
            xg_delta = f"{xg_sum - sb_sum:+.2f} vs StatsBomb"
        st.metric(
            "Total xG",
            f"{xg_sum:.2f}",
            delta=xg_delta,
            delta_color="off",
            help=METRIC_HELP.get("Total xG") or None,
        )
        st.metric(
            "Conversion Rate",
            f"{conversion:.1f}%",
            help=METRIC_HELP.get("Conversion Rate") or None,
        )
        st.metric(
            "xG / Shot",
            f"{xg_per_shot:.3f}",
            help=METRIC_HELP.get("xG / Shot") or None,
        )

        # Brier score — measures calibration of xG predictions
        brier = _compute_brier_score(pd.Series(shots["is_goal"]), pd.Series(xg_series))
        if brier is not None:
            st.metric(
                "Brier Score",
                f"{brier:.4f}",
                help=METRIC_HELP.get("Brier Score") or None,
            )
        else:
            st.metric(
                "Brier Score",
                "N/A",
                help=METRIC_HELP.get("Brier Score") or None,
            )
