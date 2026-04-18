"""Conversion Rate Funnel — state module (prefix: cf_)."""

from __future__ import annotations

import logging
from typing import Any

import plotly.graph_objects as go
from queries.funnel import (
    compute_conversion_rates,
    fetch_funnel_agg,
    fetch_match_meta,
    rollup_stages,
)
from render import AWAY_COLOR, HOME_COLOR, PITCH_BG_COLOR, TEXT_COLOR

from state.shared import (
    _ALL_LABEL,
    get_comp_id,
    get_match_id,
    get_team_id,
    register_page_refresher,
)

logger = logging.getLogger(__name__)

cf_possessions: str = ""
cf_possessions_detail: str = ""
cf_a3_entries: str = ""
cf_a3_detail: str = ""
cf_shots: str = ""
cf_shots_detail: str = ""
cf_goals: str = ""
cf_goals_detail: str = ""
cf_funnel_chart: go.Figure | None = None
cf_scope_comp: str = ""
cf_scope_team: str = ""
cf_scope_match: str = ""
cf_scope_game_state: str = ""
cf_data_freshness: str = ""
cf_warning_text: str = ""

# Game state filter — local to Conversion Funnel only
cf_selected_game_state: str | None = "All"
cf_game_state_lov: list[str] = ["All", "Winning", "Losing", "Drawing"]

__all__ = [
    "cf_possessions",
    "cf_possessions_detail",
    "cf_a3_entries",
    "cf_a3_detail",
    "cf_shots",
    "cf_shots_detail",
    "cf_goals",
    "cf_goals_detail",
    "cf_funnel_chart",
    "cf_scope_comp",
    "cf_scope_team",
    "cf_scope_match",
    "cf_scope_game_state",
    "cf_data_freshness",
    "cf_warning_text",
    "cf_selected_game_state",
    "cf_game_state_lov",
    "on_cf_game_state_change",
]

_STAGE_LABELS = ["Possessions", "A3 Entries", "Shots", "Goals"]
_STAGE_KEYS = ["possessions", "a3_entries", "shots", "goals"]


def _build_mirror_chart(
    home: dict[str, int],
    away: dict[str, int],
    home_name: str,
    away_name: str,
) -> go.Figure:
    """Build horizontal mirror bar chart comparing home vs away funnel."""
    home_vals = [home[k] for k in _STAGE_KEYS]
    away_vals = [-away[k] for k in _STAGE_KEYS]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=_STAGE_LABELS,
            x=home_vals,
            orientation="h",
            name=home_name,
            marker_color=HOME_COLOR,
            text=[str(v) for v in home_vals],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=13),
        )
    )

    fig.add_trace(
        go.Bar(
            y=_STAGE_LABELS,
            x=away_vals,
            orientation="h",
            name=away_name,
            marker_color=AWAY_COLOR,
            text=[str(abs(v)) for v in away_vals],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=13),
        )
    )

    home_rates = compute_conversion_rates(home)
    away_rates = compute_conversion_rates(away)
    rate_keys = ["poss_to_a3", "a3_to_shot", "shot_to_goal"]
    for i, rk in enumerate(rate_keys):
        fig.add_annotation(
            x=0,
            y=i + 0.5,
            text=f"{home_rates[rk]}% | {away_rates[rk]}%",
            showarrow=False,
            font=dict(size=11, color=TEXT_COLOR),
            xanchor="center",
        )

    fig.update_layout(
        barmode="overlay",
        plot_bgcolor=PITCH_BG_COLOR,
        paper_bgcolor=PITCH_BG_COLOR,
        font_color=TEXT_COLOR,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=100, r=40, t=40, b=40),
        xaxis=dict(
            title="",
            showticklabels=False,
            zeroline=True,
            zerolinecolor="#444",
            gridcolor="#333",
        ),
        yaxis=dict(title="", autorange="reversed"),
        height=350,
    )

    return fig


def _clear_state(state: Any) -> None:
    state.cf_possessions = ""
    state.cf_possessions_detail = ""
    state.cf_a3_entries = ""
    state.cf_a3_detail = ""
    state.cf_shots = ""
    state.cf_shots_detail = ""
    state.cf_goals = ""
    state.cf_goals_detail = ""
    state.cf_funnel_chart = None
    state.cf_scope_comp = ""
    state.cf_scope_team = ""
    state.cf_scope_match = ""
    state.cf_scope_game_state = ""
    state.cf_warning_text = ""


def cf_refresh(state: Any) -> None:
    """Refresh conversion funnel data for the selected filters.

    Two aggregation modes:
    - **Single match**: Home vs Away mirror chart with team names from JOIN.
    - **Season**: Selected Team vs Opponents — summed per side from the
      pre-aggregated fct_funnel_stages_agg mart. Straddler + Wyscout
      semantics are handled by rollup_stages() (see queries/funnel.py).
    """
    comp_id = get_comp_id(state.selected_competition)
    if not comp_id:
        _clear_state(state)
        return

    team_id = get_team_id(state.selected_team)
    if not team_id:
        _clear_state(state)
        return

    match_id = get_match_id(state.selected_match)
    game_state = getattr(state, "cf_selected_game_state", "All")
    gs_param = game_state if game_state and game_state != "All" else None
    gs_filtered = gs_param is not None

    df = fetch_funnel_agg(comp_id, team_id, match_id, gs_param)
    if df.empty:
        _clear_state(state)
        state.cf_warning_text = (
            "No action data found for this filter combination. Try selecting a different competition or team."
        )
        return

    # Mart invariant: team_id != opponent_team_id in every row (V09), so these
    # two masks exactly partition df with no overlap or gap.
    team_rows = df[df["team_id"] == team_id]
    opp_rows = df[df["team_id"] != team_id]
    primary_stages = rollup_stages(team_rows, gs_filtered=gs_filtered)
    opp_stages = rollup_stages(opp_rows, gs_filtered=gs_filtered)
    show_stages = primary_stages

    if match_id is not None:
        meta = fetch_match_meta(comp_id, match_id)
        if meta.empty:
            _clear_state(state)
            state.cf_warning_text = "Match metadata not found. Try selecting a different match."
            return
        home_tid = int(meta["home_team_id"].iloc[0])
        home_name = str(meta["home_team_name"].iloc[0])
        away_name = str(meta["away_team_name"].iloc[0])
        home_stages, away_stages = (primary_stages, opp_stages) if team_id == home_tid else (opp_stages, primary_stages)
        state.cf_funnel_chart = _build_mirror_chart(home_stages, away_stages, home_name, away_name)
    else:
        state.cf_funnel_chart = _build_mirror_chart(primary_stages, opp_stages, str(state.selected_team), "Opponents")

    show_rates = compute_conversion_rates(show_stages)

    state.cf_possessions = f"{show_stages['possessions']:,}"
    state.cf_possessions_detail = "total team possessions"
    state.cf_a3_entries = f"{show_stages['a3_entries']:,}"
    state.cf_a3_detail = f"{show_rates['poss_to_a3']}% of possessions"
    state.cf_shots = f"{show_stages['shots']:,}"
    state.cf_shots_detail = f"{show_rates['a3_to_shot']}% of A3 entries"
    state.cf_goals = f"{show_stages['goals']:,}"
    state.cf_goals_detail = f"{show_rates['shot_to_goal']}% of shots"

    # Canonical Tier A scope line — Competition, Team, Match, Game State.
    state.cf_scope_comp = str(state.selected_competition)
    state.cf_scope_team = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    state.cf_scope_match = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "Full season"
    state.cf_scope_game_state = game_state if gs_param else "All"
    state.cf_warning_text = ""

    logger.info("Funnel refreshed: stages=%s rates=%s", show_stages, show_rates)


def on_cf_game_state_change(state: Any, var_name: str, var_value: Any) -> None:
    """Game state changed — refresh funnel page."""
    cf_refresh(state)


register_page_refresher("Conversion-Funnel", cf_refresh, is_dashboard=True)
