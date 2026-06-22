"""Goalkeeper Analytics — insight-first redesign (two views) state adapter.

Thin Taipy adapter (gka_ prefix): pick a Competition (tracking cohort), then a Keeper; fetch the
competition cohort ONCE, slice the keeper, run the pure services/gk_insight functions, assign display
vars (top KPI tiles + charts + RawHtml callouts). All heavy logic is in gk_insight (pure).
"""

from __future__ import annotations

import logging
import statistics
from typing import Any

import pandas as pd
from queries.gk_analytics import (
    fetch_distribution_profile,
    fetch_gk_competitions,
    fetch_gk_data_freshness,
    fetch_gk_keepers,
    fetch_goals_prevented,
    fetch_line_context,
    fetch_sweeper_stats,
)
from services.gk_insight import (
    Tercile,
    cohort_values,
    defensive_verdict,
    distribution_quadrant,
    sweeping_command,
    tercile_position,
)

from state.gk_analytics_charts import (
    build_distribution_scatter_figure,
    build_line_height_figure,
    build_sweeper_profile_figure,
)
from state.gk_analytics_render import render_big_story_html, render_honest_secondary_html
from state.shared import register_page_refresher

logger = logging.getLogger(__name__)

GKA_SUB_VIEW_LOV: list[str] = ["Distribution Value", "Shot Review"]

# Floors (spec §2 defaults)
_DIST_FLOOR = 20
_SWEEP_FLOOR = 30
_MIN_COHORT = 8
# Quality words (terciles are ORIENTED so high = better for every metric). "better/worse than cohort"
# — NOT "above/below" — because for closing-time (lower is better) the raw value sits above the median
# while the keeper is worse; positional words would contradict the number shown in the same tile.
_TERCILE_WORD = {"low": "▼ worse than cohort", "mid": "≈ cohort median", "high": "▲ better than cohort"}

# ---------------------------------------------------------------------------
# Exported state variables (gka_ prefixed)
# ---------------------------------------------------------------------------
gka_competition_lov: list[str] = []
gka_selected_competition: str | None = None
gka_keeper_lov: list[str] = []
gka_selected_keeper: str | None = None
gka_scope_comp: str = ""
gka_keeper_label: str = ""
gka_line_label: str = "—"
gka_data_freshness: str = ""
gka_warning_text: str = ""
gka_plot_config: dict[str, Any] = {"displayModeBar": False}

# Distribution Value view — top tiles (var + detail) + content (ADR-060 2-axis profile)
gka_threat_val: str = "—"
gka_threat_detail: str = ""
gka_value_val: str = "—"
gka_value_detail: str = ""
gka_style_val: str = "—"
gka_style_detail: str = ""
gka_off_verdict_val: str = "—"
gka_off_verdict_detail: str = ""
gka_dist_figure: Any = None
gka_dist_story: Any = None
gka_dist_story_height: str = "190px"

# Shot Review view — top tiles + content
gka_reach_val: str = "—"
gka_reach_detail: str = ""
gka_pc_val: str = "—"
gka_pc_detail: str = ""
gka_closing_val: str = "—"
gka_closing_detail: str = ""
gka_def_verdict_val: str = "—"
gka_def_verdict_detail: str = ""
gka_sweeper_figure: Any = None
gka_line_height_figure: Any = None
gka_def_story: Any = None
gka_def_story_height: str = "150px"
gka_honest_secondary: Any = None
gka_honest_secondary_height: str = "120px"

_gka_competition_map: dict[str, int] = {}
_gka_keeper_map: dict[str, int] = {}

__all__ = [
    "GKA_SUB_VIEW_LOV",
    "gka_closing_detail",
    "gka_closing_val",
    "gka_competition_lov",
    "gka_data_freshness",
    "gka_def_story",
    "gka_def_story_height",
    "gka_line_height_figure",
    "gka_def_verdict_detail",
    "gka_def_verdict_val",
    "gka_dist_figure",
    "gka_dist_story",
    "gka_dist_story_height",
    "gka_honest_secondary",
    "gka_honest_secondary_height",
    "gka_keeper_label",
    "gka_keeper_lov",
    "gka_line_label",
    "gka_off_verdict_detail",
    "gka_off_verdict_val",
    "gka_on_competition_change",
    "gka_on_keeper_change",
    "gka_pc_detail",
    "gka_pc_val",
    "gka_plot_config",
    "gka_reach_detail",
    "gka_reach_val",
    "gka_refresh",
    "gka_scope_comp",
    "gka_selected_competition",
    "gka_selected_keeper",
    "gka_style_detail",
    "gka_style_val",
    "gka_sweeper_figure",
    "gka_threat_detail",
    "gka_threat_val",
    "gka_value_detail",
    "gka_value_val",
    "gka_warning_text",
]


# ---------------------------------------------------------------------------
# Pure-ish helpers (DataFrame in, primitive out)
# ---------------------------------------------------------------------------
def _build_keeper_map(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        return {}
    return {str(n): int(k) for n, k in zip(df["player_display_name"], df["gk_player_key"], strict=True)}


def _agg_keeper(df: pd.DataFrame, gk_key: int, value_col: str, weight_col: str) -> float | None:
    rows = df[df["gk_player_key"] == gk_key]
    w = pd.to_numeric(rows[weight_col], errors="coerce")
    v = pd.to_numeric(rows[value_col], errors="coerce")
    denom = float(w.sum())
    if denom <= 0:
        return None
    return float((v * w).sum() / denom)


def _cohort_value_list(df: pd.DataFrame, value_col: str, weight_col: str, floor: float) -> list[float]:
    rows = ((int(k), v, w) for k, v, w in zip(df["gk_player_key"], df[value_col], df[weight_col], strict=True))
    return cohort_values(rows, floor=floor)


def _fmt(v: float | None, fmt: str) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "—"
    return fmt.format(v)


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------
def gka_refresh(state: Any) -> None:
    current_lov = getattr(state, "sub_view_lov", None) or []
    if list(current_lov) != GKA_SUB_VIEW_LOV:
        state.sub_view_lov = GKA_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in GKA_SUB_VIEW_LOV:
        state.selected_sub_view = GKA_SUB_VIEW_LOV[0]
    state.gka_warning_text = ""

    global _gka_competition_map, _gka_keeper_map
    try:
        comps = fetch_gk_competitions()
    except Exception:
        logger.exception("Failed to fetch GK competitions")
        state.gka_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    if comps.empty:
        state.gka_warning_text = "No tracking-provider goalkeeper data available yet."
        return
    _gka_competition_map = {
        str(n): int(k) for n, k in zip(comps["competition_name"], comps["competition_key"], strict=True)
    }
    state.gka_competition_lov = list(_gka_competition_map.keys())
    if not state.gka_selected_competition or state.gka_selected_competition not in _gka_competition_map:
        state.gka_selected_competition = state.gka_competition_lov[0]
    state.gka_scope_comp = state.gka_selected_competition
    comp_key = _gka_competition_map[state.gka_selected_competition]

    try:
        keepers = fetch_gk_keepers(comp_key)
    except Exception:
        logger.exception("Failed to fetch GK keepers for competition=%s", comp_key)
        state.gka_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    _gka_keeper_map = _build_keeper_map(keepers)
    state.gka_keeper_lov = list(_gka_keeper_map.keys())
    if not state.gka_keeper_lov:
        state.gka_warning_text = f"No goalkeeper data for {state.gka_selected_competition} yet."
        return
    if not state.gka_selected_keeper or state.gka_selected_keeper not in _gka_keeper_map:
        state.gka_selected_keeper = state.gka_keeper_lov[0]
    state.gka_keeper_label = state.gka_selected_keeper
    gk_key = _gka_keeper_map[state.gka_selected_keeper]

    if state.selected_sub_view == "Shot Review":
        _refresh_shot_review(state, comp_key, gk_key)
    else:
        _refresh_distribution(state, comp_key, gk_key)

    try:
        state.gka_data_freshness = fetch_gk_data_freshness()
    except Exception:
        state.gka_data_freshness = ""


def _reset_distribution_tiles(state: Any) -> None:
    state.gka_dist_figure = None
    state.gka_threat_val = state.gka_value_val = state.gka_style_val = state.gka_off_verdict_val = "—"
    state.gka_threat_detail = state.gka_value_detail = state.gka_style_detail = state.gka_off_verdict_detail = ""
    state.gka_dist_story = None


def _refresh_distribution(state: Any, comp_key: int, gk_key: int) -> None:
    """ADR-060 2-axis profile: threat (% of distributions that add xT-GK) x style (forward
    progression), positioned in the competition cohort. Replaces the degenerate game-model ladder."""
    try:
        prof = fetch_distribution_profile(comp_key)
    except Exception:
        logger.exception("Failed to fetch distribution profile")
        state.gka_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    mine = prof[prof["gk_player_key"] == gk_key] if not prof.empty else prof
    if mine.empty:
        _reset_distribution_tiles(state)
        state.gka_warning_text = (
            f"{state.gka_selected_keeper} has fewer than 20 distributions in "
            f"{state.gka_selected_competition} — too few for a distribution profile."
        )
        return

    r = mine.iloc[0]
    share = float(r["share_adds_threat"])
    prog = float(r["mean_progress_m"])
    val = float(r["mean_xtgk"])
    compl = float(r["mean_completion"])
    n = int(r["n_distributions"])

    has_cohort = len(prof) >= _MIN_COHORT
    share_med = float(prof["share_adds_threat"].median()) if has_cohort else None
    prog_med = float(prof["mean_progress_m"].median()) if has_cohort else None

    # top tiles
    threat_pos = ""
    if share_med is not None:
        threat_pos = "▲ better than cohort" if share >= share_med else "▼ worse than cohort"
    state.gka_threat_val = f"{share:.0%}"
    state.gka_threat_detail = (
        f"{threat_pos} · median {share_med:.0%}" if share_med is not None else f"cohort too small (n={len(prof)})"
    )
    state.gka_value_val = f"{val:+.3f}"
    state.gka_value_detail = "xT-GK per distribution (signed; the keeper norm is negative)"
    state.gka_style_val = f"{prog:.0f} m forward"
    state.gka_style_detail = f"{compl:.0%} completion · short-safe ↔ long-direct"

    verdict = distribution_quadrant(
        share_adds=share, progress_m=prog, n=n, share_median=share_med, progress_median=prog_med
    )
    state.gka_off_verdict_val = verdict.phrase
    state.gka_off_verdict_detail = verdict.detail

    names = prof["player_display_name"].astype(str).tolist()
    progs = pd.to_numeric(prof["mean_progress_m"], errors="coerce").tolist()
    shares = pd.to_numeric(prof["share_adds_threat"], errors="coerce").tolist()
    nns = pd.to_numeric(prof["n_distributions"], errors="coerce").fillna(0).astype(int).tolist()
    # Selected flag via the EXACT pandas comparison on the int64 key — gk_player_key is a 19-digit
    # hashed bigint, so a pd.to_numeric->float round-trip loses precision and never matches gk_key.
    sel_mask = (prof["gk_player_key"] == gk_key).tolist()
    points = [
        (str(nm), float(pr), float(sh), int(nn), bool(is_sel))
        for nm, pr, sh, nn, is_sel in zip(names, progs, shares, nns, sel_mask, strict=True)
    ]
    state.gka_dist_figure = build_distribution_scatter_figure(points, share_median=share_med, progress_median=prog_med)

    cohort_clause = (
        f" ({'above' if share >= share_med else 'below'} the {state.gka_selected_competition} "
        f"cohort median of {share_med:.0%})"
        if share_med is not None
        else ""
    )
    body = (
        f"He adds threat on <b>{share:.0%}</b> of his distributions{cohort_clause}, at {prog:.0f} m average "
        f"forward progression and {compl:.0%} completion. {verdict.detail.capitalize()}. xT-GK is ~97% "
        f"negative across keepers, so the headline is the SHARE of distributions that add threat — not the "
        f"(negative) average value of {val:+.3f}. n={n} distributions."
    )
    state.gka_dist_story = render_big_story_html(verdict, body=body)


def _refresh_shot_review(state: Any, comp_key: int, gk_key: int) -> None:
    try:
        sweep = fetch_sweeper_stats(comp_key)
    except Exception:
        logger.exception("Failed to fetch sweeper stats")
        state.gka_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return
    if sweep.empty:
        state.gka_warning_text = f"No sweeper data for {state.gka_selected_competition}."
        return

    n_def = int(pd.to_numeric(sweep[sweep["gk_player_key"] == gk_key]["n_defended_actions"], errors="coerce").sum())
    metric_defs = [
        ("reachable_area_mean_m2", False, "Reachable area", "{:.1f} m²"),
        ("pc_share_mean", False, "Pitch-control share", "{:.1%}"),
        ("closing_min_six_yard_mean_s", True, "Closing time · 6-yd", "{:.1f} s"),
    ]
    chart_metrics: list[tuple[str, float, str, list[float], bool]] = []
    terciles: dict[str, Tercile] = {}
    vals: dict[str, float | None] = {}
    for col, lower, label, fmt in metric_defs:
        val = _agg_keeper(sweep, gk_key, col, "n_defended_actions")
        vals[col] = val
        cohort = _cohort_value_list(sweep, col, "n_defended_actions", _SWEEP_FLOOR)
        terciles[col] = tercile_position(val, cohort, lower_is_better=lower) if val is not None else "mid"
        chart_metrics.append((label, val if val is not None else 0.0, _fmt(val, fmt), cohort, lower))
    state.gka_sweeper_figure = build_sweeper_profile_figure(chart_metrics)

    # top tiles — detail wording matches the chart's above/below-cohort cue (terciles are oriented
    # so "above cohort" = better for every metric, incl. closing where faster is better).
    state.gka_reach_val = _fmt(vals["reachable_area_mean_m2"], "{:.1f} m²")
    state.gka_reach_detail = f"{_TERCILE_WORD[terciles['reachable_area_mean_m2']]} · n={n_def}"
    state.gka_pc_val = _fmt(vals["pc_share_mean"], "{:.1%}")
    state.gka_pc_detail = _TERCILE_WORD[terciles["pc_share_mean"]]
    state.gka_closing_val = _fmt(vals["closing_min_six_yard_mean_s"], "{:.1f} s")
    state.gka_closing_detail = _TERCILE_WORD[terciles["closing_min_six_yard_mean_s"]]

    command = sweeping_command(
        reach=terciles["reachable_area_mean_m2"],
        pc=terciles["pc_share_mean"],
        closing=terciles["closing_min_six_yard_mean_s"],
    )
    verdict = defensive_verdict(command=command, n_defended=n_def)
    state.gka_def_verdict_val = verdict.phrase
    state.gka_def_verdict_detail = verdict.detail

    # Defensive line: DESCRIPTIVE own-goal distance vs cohort (no deep/mid/high bucket — the per-keeper
    # average barely varies, so a hard tercile/"right system" verdict would be noise).
    line_m, line_cohort = _line_context(comp_key, gk_key)
    state.gka_line_height_figure = build_line_height_figure(line_m, line_cohort)
    if line_m is None:
        state.gka_line_label = "—"
    else:
        pos = ""
        if len(line_cohort) >= _MIN_COHORT:
            pos = " · above cohort" if line_m > statistics.median(line_cohort) else " · below cohort"
        state.gka_line_label = f"≈{line_m:.0f} m from own goal{pos}"

    # honest secondary: ghost deviation + goals prevented (from the pooled mart, with band)
    gd = _agg_keeper(sweep, gk_key, "ghost_deviation_mean_m", "shots_faced")
    shots = int(pd.to_numeric(sweep[sweep["gk_player_key"] == gk_key]["shots_faced"], errors="coerce").sum())
    gp_val, gp_note, gp_low_sample = _goals_prevented_display(comp_key, gk_key)
    state.gka_honest_secondary = render_honest_secondary_html(
        ghost_dev=_fmt(gd, "{:.1f} m"),
        ghost_n=f"n={shots} shots — indicative, never ranked",
        goals_prevented=gp_val,
        gp_note=gp_note,
        low_sample=gp_low_sample,
    )

    cmd_word = {"upper": "in the upper part of", "mid": "around the middle of", "lower": "in the lower part of"}[
        command
    ]
    line_clause = (
        f", with a defensive line {state.gka_line_label.replace('≈', '').strip()}" if line_m is not None else ""
    )
    body = (
        f"Measured over {n_def} defended actions, his sweeping command sits {cmd_word} the "
        f"{state.gka_selected_competition} cohort{line_clause}. {verdict.detail.capitalize()}. "
        f"Line height is descriptive (avg distance from own goal), not a deep/high label. "
        f"Shot-facing metrics are thin (shown ± band, never ranked)."
    )
    state.gka_def_story = render_big_story_html(verdict, body=body)


# ---------------------------------------------------------------------------
# Small derivations
# ---------------------------------------------------------------------------
def _line_context(comp_key: int, gk_key: int) -> tuple[float | None, list[float]]:
    """(his avg line height m, cohort values list) — descriptive own-goal distance, NO tercile.
    Cohort already floored at n_actions>=30 in the query; the chart draws it as a strip."""
    try:
        line = fetch_line_context(comp_key)
    except Exception:
        logger.exception("Failed to fetch line context")
        return None, []
    if line.empty:
        return None, []
    mine = line[line["gk_player_key"] == gk_key]
    if mine.empty:
        return None, []
    line_m = float(pd.to_numeric(mine["avg_line_height_m"], errors="coerce").mean())
    cohort = pd.to_numeric(line["avg_line_height_m"], errors="coerce").dropna().tolist()
    return line_m, cohort


def _goals_prevented_display(comp_key: int, gk_key: int) -> tuple[str, str, bool]:
    """Returns (value_str, note, low_sample) — low_sample drives the badge (read from the mart,
    never hardcoded)."""
    try:
        gp = fetch_goals_prevented(comp_key)
    except Exception:
        logger.exception("Failed to fetch goals prevented")
        return "—", "", False
    mine = gp[gp["player_key"] == gk_key]
    if mine.empty:
        return "—", "no on-target shots faced", False
    r = mine.iloc[0]
    val = float(r["goals_prevented"])
    lo, hi = float(r["goals_prevented_ci_low"]), float(r["goals_prevented_ci_high"])
    n = int(r["shots_faced_total"])
    low_sample = bool(r["low_sample"])
    straddle = " · band straddles 0 (inconclusive)" if lo <= 0 <= hi else ""
    return f"{val:+.2f} ± {(hi - val):.2f}", f"n={n} shots · higher = better, 0 = as expected{straddle}", low_sample


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------
def gka_on_competition_change(state: Any, var_name: str, var_value: Any) -> None:
    state.gka_selected_keeper = None  # new competition -> reselect its first keeper
    gka_refresh(state)


def gka_on_keeper_change(state: Any, var_name: str, var_value: Any) -> None:
    gka_refresh(state)


register_page_refresher("Goalkeeper-Analytics", gka_refresh)
