"""Match Summary state module — dashboard layout per 2026-04-19 redesign spec.

Orchestrates the refresh pipeline: fetch match summary, compute verdict, fetch
VAEP decisive actions + shots timeline + discipline events, then render Row 1
HTML, Row 2 Plotly figure, Row 3 HTML.

All state variables prefixed with ``ms_``. Registered as the Match-Summary
dashboard-page refresher (is_dashboard=True → hides site-wide footer, wraps
content in the ll-dashboard-scroll viewport container).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from filters import build_scope_label_plain, build_warning, fetch_data_freshness
from queries.match import (
    fetch_discipline_events,
    fetch_league_averages,
    fetch_match_summary,
    fetch_shots_timeline,
    fetch_vaep_decisive_actions,
)
from render import AWAY_COLOR, HOME_COLOR

from state.match_summary_render import (
    build_xg_race_figure,
    render_delta_table_html,
    render_moments_html,
)
from state.match_summary_verdict import derive_verdict
from state.shared import _ALL_LABEL, get_comp_id, get_match_id, get_match_key, register_page_refresher
from state.workflows_dag import RawHtml

logger = logging.getLogger(__name__)


# ── Exported state variables (all ms_ prefixed) ─────────────────────────────
# Tile strip
ms_home_name: str = ""
ms_away_name: str = ""
ms_final_score: str = "--"
ms_home_xg: str = "--"
ms_away_xg: str = "--"
ms_home_xg_delta: str = ""
ms_away_xg_delta: str = ""
ms_verdict_phrase: str = ""
ms_verdict_detail: str = ""

# Row 1 — Big Story decisive actions. MUST be a RawHtml instance (not plain str)
# so Taipy's registered content provider renders it as actual HTML in the
# `<|part|content={var}|>` block. Plain str produces "No valid provider for
# type str" — see state/workflows_dag.py::RawHtml + main.py::_raw_html_provider.
ms_moments_html: RawHtml = RawHtml("")
ms_moments_height: str = "280px"

# Row 2 — Plotly xG race figure
ms_xg_race_fig: Any = None
ms_xg_race_alt: str = ""

# Row 3 — ranked delta table (same RawHtml contract as Row 1).
ms_delta_table_html: RawHtml = RawHtml("")
ms_delta_table_height: str = "260px"

# Shared scope + meta
ms_scope_comp: str = ""
ms_scope_team: str = ""
ms_scope_match: str = ""
ms_warning_text: str = ""
ms_data_freshness: str = ""


__all__ = [
    "ms_away_name",
    "ms_away_xg",
    "ms_away_xg_delta",
    "ms_data_freshness",
    "ms_delta_table_height",
    "ms_delta_table_html",
    "ms_final_score",
    "ms_home_name",
    "ms_home_xg",
    "ms_home_xg_delta",
    "ms_moments_height",
    "ms_moments_html",
    "ms_scope_comp",
    "ms_scope_match",
    "ms_scope_team",
    "ms_verdict_detail",
    "ms_verdict_phrase",
    "ms_warning_text",
    "ms_xg_race_alt",
    "ms_xg_race_fig",
]


# ── Helper: state reset ─────────────────────────────────────────────────────

_CLEAR_VALUES: list[tuple[str, Any]] = [
    ("ms_home_name", ""),
    ("ms_away_name", ""),
    ("ms_final_score", "--"),
    ("ms_home_xg", "--"),
    ("ms_away_xg", "--"),
    ("ms_home_xg_delta", ""),
    ("ms_away_xg_delta", ""),
    ("ms_verdict_phrase", ""),
    ("ms_verdict_detail", ""),
    ("ms_moments_html", RawHtml("")),
    ("ms_xg_race_fig", None),
    ("ms_xg_race_alt", ""),
    ("ms_delta_table_html", RawHtml("")),
    ("ms_scope_comp", ""),
    ("ms_scope_team", ""),
    ("ms_scope_match", ""),
    ("ms_warning_text", ""),
    ("ms_data_freshness", ""),
]


def _clear_all(state: Any) -> None:
    for attr, default in _CLEAR_VALUES:
        setattr(state, attr, default)


# ── Refresh callback ────────────────────────────────────────────────────────


def ms_refresh(state: Any) -> None:
    """Reload match data and render the redesigned Match Summary dashboard."""
    comp_id = get_comp_id(state.selected_competition)
    match_id = get_match_id(state.selected_match)
    # Post-PR 2 (ADR-011): fct_match_summary is keyed on match_key; all
    # other per-match fetchers (fct_action_values, fct_shots, fct_discipline)
    # still use native match_id until their own migration PRs.
    match_key = get_match_key(state.selected_match)

    if match_id is None or match_key is None:
        _clear_all(state)
        return

    comp_label = state.selected_competition or ""
    team_label = state.selected_team if state.selected_team not in (None, _ALL_LABEL) else "All teams"
    match_label = state.selected_match if state.selected_match not in (None, _ALL_LABEL) else "\u2014"
    state.ms_scope_comp = comp_label
    state.ms_scope_team = team_label
    state.ms_scope_match = match_label
    scope_plain = build_scope_label_plain(
        [("Competition", comp_label), ("Team", team_label), ("Match", match_label)],
    )

    match_data = fetch_match_summary(match_key)
    if match_data.empty:
        _clear_all(state)
        state.ms_warning_text = build_warning(
            domain="match data",
            suggestions=["choosing a different match"],
        )
        return

    m = match_data.iloc[0]
    state.ms_warning_text = ""

    home_name = str(m["home_team_name"])
    away_name = str(m["away_team_name"])
    home_score = int(m["home_score"]) if pd.notna(m.get("home_score")) else 0
    away_score = int(m["away_score"]) if pd.notna(m.get("away_score")) else 0
    home_xg = float(m["home_xg"]) if pd.notna(m.get("home_xg")) else 0.0
    away_xg = float(m["away_xg"]) if pd.notna(m.get("away_xg")) else 0.0
    home_team_id_raw = m.get("home_team_id")
    away_team_id_raw = m.get("away_team_id")

    state.ms_home_name = home_name
    state.ms_away_name = away_name
    state.ms_final_score = f"{home_score} \u2014 {away_score}"
    state.ms_home_xg = f"{home_xg:.2f}"
    state.ms_away_xg = f"{away_xg:.2f}"
    state.ms_home_xg_delta = f"{home_score - home_xg:+.2f} vs actual"
    state.ms_away_xg_delta = f"{away_score - away_xg:+.2f} vs actual"

    phrase, detail = derive_verdict(home_xg, away_xg, home_score, away_score)
    state.ms_verdict_phrase = phrase
    state.ms_verdict_detail = detail

    try:
        decisive = fetch_vaep_decisive_actions(int(match_id), n=3)
        shots = fetch_shots_timeline(int(match_id))
    except Exception:
        # ADR-002: ERROR-level so it surfaces in observability; user sees the warning.
        # Both queries are required — without them there is no Row 1 / Row 2.
        logger.exception("Match Summary data fetch failed for match_id=%s", match_id)
        _clear_all(state)
        state.ms_warning_text = build_warning(
            domain="match data",
            suggestions=["choosing a different match", "retrying"],
        )
        return

    # Discipline events are OPTIONAL — a missing sync or empty rowset should render
    # the page without red cards, not fail it. Log at WARNING here because a missing
    # fct_discipline_events_synced table on staging means the mart/sync hasn't been
    # wired yet (expected during rollout), not a real outage.
    try:
        discipline = fetch_discipline_events(int(match_id))
    except Exception:
        logger.warning(
            "Discipline events unavailable for match_id=%s; rendering without red cards",
            match_id,
            exc_info=True,
        )
        discipline = pd.DataFrame(
            columns=[
                "match_id",
                "minute",
                "second",
                "period",
                "player_id",
                "team_id",
                "card_name",
                "player_name",
                "team_name",
            ],
        )

    state.ms_moments_html = RawHtml(render_moments_html(decisive, discipline, scope_plain=scope_plain))

    if home_team_id_raw is not None and away_team_id_raw is not None:
        state.ms_xg_race_fig = build_xg_race_figure(
            shots=shots,
            decisive=decisive,
            red_cards=discipline,
            home_team_id=int(home_team_id_raw),
            home_team_name=home_name,
            away_team_id=int(away_team_id_raw),
            away_team_name=away_name,
            home_color=HOME_COLOR,
            away_color=AWAY_COLOR,
        )
        state.ms_xg_race_alt = f"xG race and decisive moments \u2014 {scope_plain}"
    else:
        state.ms_xg_race_fig = None
        state.ms_xg_race_alt = ""

    home_stats = {
        "xG": home_xg,
        "Progressive passes": float(m.get("home_progressive_passes", 0) or 0),
        "Shots": float(m.get("home_shots", 0) or 0),
        "PPDA (lower = more press)": float(m.get("home_ppda", 0) or 0),
        "Possession %": float(m.get("home_possession_pct", 50) or 50),
        "Pass completion %": float(m.get("home_pass_completion_pct", 0) or 0),
    }
    home_poss = float(m.get("home_possession_pct", 50) or 50)
    away_stats = {
        "xG": away_xg,
        "Progressive passes": float(m.get("away_progressive_passes", 0) or 0),
        "Shots": float(m.get("away_shots", 0) or 0),
        "PPDA (lower = more press)": float(m.get("away_ppda", 0) or 0),
        "Possession %": 100.0 - home_poss,
        "Pass completion %": float(m.get("away_pass_completion_pct", 0) or 0),
    }

    # League averages are rendered per-row in Row 3 only (spec §5.5). The
    # standalone "League avg: ..." scope line was removed 2026-04-19 after a
    # post-deploy UX review flagged it as visually detached — the per-row
    # annotations next to each metric serve the same purpose better.
    league_avgs: dict[str, float] = {}
    if comp_id is not None:
        try:
            avg_df = fetch_league_averages(comp_id)
            if not avg_df.empty:
                avg = avg_df.iloc[0]
                league_avgs = {
                    "xG": float(avg.get("avg_xg_per_team", 0) or 0),
                    "Possession %": float(avg.get("avg_possession", 50) or 50),
                    "Pass completion %": float(avg.get("avg_pass_completion", 0) or 0),
                }
        except Exception:
            logger.exception("Failed to fetch league averages for comp_id=%s", comp_id)

    state.ms_delta_table_html = RawHtml(
        render_delta_table_html(
            home_stats=home_stats,
            away_stats=away_stats,
            home_name=home_name,
            away_name=away_name,
            league_avgs=league_avgs,
        ),
    )

    state.ms_data_freshness = fetch_data_freshness()

    logger.info(
        "Match summary refreshed: %s %d-%d %s (xG %.2f-%.2f, verdict: %s)",
        home_name,
        home_score,
        away_score,
        away_name,
        home_xg,
        away_xg,
        phrase,
    )


# ── Registration ────────────────────────────────────────────────────────────
register_page_refresher("Match-Summary", ms_refresh, is_dashboard=True)
