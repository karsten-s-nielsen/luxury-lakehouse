"""Match Summary rendering helpers — Row 1 Big Story, Row 2 xG race, Row 3 delta table.

All helpers are pure functions over pandas DataFrames (and plain dicts for Row 3).
No Taipy state, no DB access. Designed for unit testability — ``match_summary.py``
orchestrates the data fetch + calls these at refresh time.
"""

from __future__ import annotations

from html import escape

import pandas as pd
import plotly.graph_objects as go

# ── Row 1: Big Story decisive-action cards ──────────────────────────────────

# SPADL action-type → human label for Row 1 prose. See silly-kicks / Decroos 2019.
_ACTION_TYPE_LABELS: dict[str, str] = {
    "shot": "Shot",
    "pass": "Pass",
    "cross": "Cross",
    "dribble": "Dribble",
    "take_on": "Take-on",
    "tackle": "Tackle",
    "interception": "Interception",
    "clearance": "Clearance",
    "keeper_save": "Keeper save",
    "keeper_claim": "Keeper claim",
    "keeper_punch": "Keeper punch",
    "foul": "Foul",
    "freekick_crossed": "Cross from free kick",
    "freekick_short": "Short free kick",
    "corner_crossed": "Corner cross",
    "corner_short": "Short corner",
    "goalkick": "Goal kick",
    "throw_in": "Throw-in",
    "bad_touch": "Miscontrol",
}


def _action_label(row: pd.Series) -> str:
    """Human label for a decisive action — e.g. 'Shot, scored'."""
    atype_raw = str(row.get("action_type", "action"))
    atype = _ACTION_TYPE_LABELS.get(atype_raw, atype_raw.replace("_", " ").title())
    result = str(row.get("action_result", ""))
    if atype == "Shot" and result == "success":
        return "Shot, scored"
    if atype == "Shot" and result == "fail":
        return "Shot, missed / saved"
    if result == "fail":
        return f"{atype}, failed"
    return atype


def _fmt_minute(row: pd.Series) -> str:
    return f"{int(row['minute'])}'"


def _moment_card_html(row: pd.Series, *, hero: bool) -> str:
    cls = "ll-moment-card ll-big-story-hero" if hero else "ll-moment-card ll-moment-card-secondary"
    minute = _fmt_minute(row)
    player = escape(str(row.get("player_name", "Unknown")))
    team = escape(str(row.get("team_name", "")))
    action = escape(_action_label(row))
    vaep = f"{float(row['vaep_value']):+.2f}"
    return (
        f'<div class="{cls}">'
        f'<span class="ll-moment-minute">{minute}</span>'
        f'<div class="ll-moment-body">'
        f'<strong>{player}</strong> <span class="ll-moment-team">{team}</span><br>'
        f"{action}"
        f"</div>"
        f'<span class="ll-moment-vaep">VAEP {vaep}</span>'
        "</div>"
    )


def _red_card_html(row: pd.Series) -> str:
    minute = _fmt_minute(row)
    player = escape(str(row.get("player_name", "Unknown")))
    team = escape(str(row.get("team_name", "")))
    card = escape(str(row.get("card_name", "Card")))
    return (
        '<div class="ll-moment-card ll-moment-card-red-card">'
        f'<span class="ll-moment-minute">{minute}</span>'
        f'<div class="ll-moment-body">'
        f"<strong>{card}</strong>: {player} ({team}) \u2014 down to 10"
        f"</div>"
        "</div>"
    )


# Taipy renders `html` ContentBlocks inside isolated iframes (via the RawHtml
# content provider). iframes don't inherit the parent document's CSS, so we
# inline the minimum style set needed for Row 1 legibility. Kept narrow —
# richer theming still comes from style_v2.css when the content renders on a
# page that doesn't sandbox into an iframe (not the case here, but keeping the
# external classes lets that path survive).
_MOMENTS_IFRAME_STYLE = """<style>
html,body{background:#1a1a1a;color:#e6e6e6;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:0;padding:8px;}
.ll-match-moments{display:grid;gap:10px;}
.ll-big-story-label{color:#d9a300;font-size:11px;text-transform:uppercase;letter-spacing:0.6px;margin-bottom:4px;}
.ll-moment-card{background:rgba(255,255,255,0.04);border-left:2px solid #d9a300;border-radius:4px;padding:10px 14px;display:grid;grid-template-columns:50px 1fr auto;gap:12px;align-items:center;}
.ll-big-story-hero{background:rgba(255,215,0,0.08);border:1px solid rgba(255,215,0,0.25);border-left:3px solid #d9a300;padding:14px 18px;}
.ll-moments-secondary{display:grid;grid-template-columns:1fr 1fr;gap:8px;}
.ll-moment-card-secondary{padding:8px 12px;font-size:12px;}
.ll-moment-card-red-card{border-left-color:#c84444;background:rgba(200,68,68,0.08);}
.ll-moment-minute{color:#d9a300;font-weight:600;font-size:13px;}
.ll-moment-card-red-card .ll-moment-minute{color:#e77;}
.ll-moment-team{color:#9aa;font-size:11px;}
.ll-moment-vaep{font-family:monospace;color:#9bb;font-size:11px;}
.ll-moments-caveat{color:#888;font-size:10px;font-style:italic;margin-top:6px;padding:0 8px;}
</style>"""


def render_moments_html(
    decisive: pd.DataFrame,
    red_cards: pd.DataFrame,
    *,
    scope_plain: str,
) -> str:
    """Build the Row 1 HTML block: Big Story hero + 2 secondary + optional red cards.

    Expects ``decisive`` sorted descending by ``|vaep_value|`` (the query does the sort).
    ``red_cards`` may be empty; if non-empty, each row becomes an extra card appended
    to the secondary grid.
    """
    _ = scope_plain  # Reserved for future alt-text wiring; keep in signature for parity.

    if decisive.empty:
        return (
            _MOMENTS_IFRAME_STYLE + '<div class="ll-match-moments ll-match-moments-empty">'
            "<p>No decisive actions available for this match.</p>"
            "</div>"
        )

    hero = decisive.iloc[0]
    secondary = decisive.iloc[1:3] if len(decisive) > 1 else decisive.iloc[0:0]

    parts: list[str] = [_MOMENTS_IFRAME_STYLE, '<div class="ll-match-moments">']
    parts.append('<div class="ll-big-story-label">\u2605 Big story</div>')
    parts.append(_moment_card_html(hero, hero=True))
    parts.append('<div class="ll-moments-secondary">')
    for _, row in secondary.iterrows():
        parts.append(_moment_card_html(row, hero=False))
    if not red_cards.empty:
        for _, row in red_cards.iterrows():
            parts.append(_red_card_html(row))
    parts.append("</div>")
    parts.append(
        '<p class="ll-moments-caveat">'
        "Decisive on-ball actions ranked by VAEP impact "
        "(Decroos et al. 2019, computed via silly-kicks). "
        "Off-ball runs and positioning are not valued."
        "</p>"
    )
    parts.append("</div>")
    return "\n".join(parts)


# ── Row 2: Plotly xG race + shot ticks + event markers ──────────────────────


def _cumulative_xg_by_team(
    shots: pd.DataFrame,
    team_id: int,
    *,
    end_minute: float | None = None,
) -> pd.DataFrame:
    """Return (minute, cumulative_xg) for one team, with a (0, 0) start anchor.

    When ``end_minute`` is provided (and greater than the team's last shot
    minute), append a trailing (end_minute, last_cumulative_xg) row so the
    Plotly stepped trace extends flat to the right edge. Without this, a team
    that stopped shooting at minute 54 while the other team kept shooting to
    minute 91 would have its line visually end at 54 — the raw data is
    correct but the chart looks truncated. Callers that use this function
    for point-in-time projection (e.g. `_project_to_curves`) should leave
    ``end_minute`` at the default None to keep the df compact.
    """
    team_shots = shots.loc[shots["team_id"] == team_id].copy()
    if team_shots.empty:
        base = pd.DataFrame({"minute": [0.0], "cumulative_xg": [0.0]})
        if end_minute is not None and end_minute > 0:
            base = pd.concat(
                [base, pd.DataFrame({"minute": [float(end_minute)], "cumulative_xg": [0.0]})],
                ignore_index=True,
            )
        return base
    team_shots = team_shots.sort_values(["period", "minute", "second"]).reset_index(drop=True)
    anchor = pd.DataFrame({"minute": [0.0], "cumulative_xg": [0.0]})
    rest = pd.DataFrame(
        {
            "minute": team_shots["minute"].astype(float),
            "cumulative_xg": team_shots["xg"].cumsum().astype(float),
        }
    )
    frames = [anchor, rest]
    if end_minute is not None:
        last_minute = float(rest["minute"].iloc[-1])
        if end_minute > last_minute:
            last_cum = float(rest["cumulative_xg"].iloc[-1])
            frames.append(pd.DataFrame({"minute": [float(end_minute)], "cumulative_xg": [last_cum]}))
    return pd.concat(frames, ignore_index=True)


def _project_to_curves(
    shots: pd.DataFrame,
    events: pd.DataFrame,
    home_team_id: int,
    away_team_id: int,
) -> list[float]:
    """For each event, the cumulative xG of that event's team at that minute.

    Used to position decisive rings and goal stars on the stepped curves so
    they visually sit ON the line rather than floating.
    """
    _ = away_team_id  # Team ID passed for symmetry; lookup by ev.team_id.
    home_cum = _cumulative_xg_by_team(shots, home_team_id)
    # Build per-team cumulative once for efficiency.
    team_cums: dict[int, pd.DataFrame] = {home_team_id: home_cum}
    ys: list[float] = []
    for _, ev in events.iterrows():
        tid = int(ev["team_id"])
        if tid not in team_cums:
            team_cums[tid] = _cumulative_xg_by_team(shots, tid)
        cum = team_cums[tid]
        mask = cum["minute"] <= float(ev["minute"])
        y = float(cum.loc[mask, "cumulative_xg"].iloc[-1]) if mask.any() else 0.0
        ys.append(y)
    return ys


def build_xg_race_figure(
    *,
    shots: pd.DataFrame,
    decisive: pd.DataFrame,
    red_cards: pd.DataFrame,
    home_team_id: int,
    home_team_name: str,
    away_team_id: int,
    away_team_name: str,
    home_color: str,
    away_color: str,
) -> go.Figure:
    """Build the Row 2 Plotly figure per spec §5.4.

    Traces (z-order back→front):
      1. Cumulative xG stepped lines per team (shape='hv')
      2. Shot ticks per team (markers at y=-0.1, sized by per-shot xG)
      3. Gold decisive-action rings (at team cumulative y)
      4. Goal stars (at team cumulative y)
      5. Red-card markers (at y=-0.25)
      + half-time dashed shape at minute 45.
    """
    fig = go.Figure()

    # Common end-of-match anchor so both team lines extend to the same right
    # edge. Without this, a team that stopped shooting at minute 54 while the
    # other team continued to minute 91 would have its stepped trace end at
    # 54 — the raw data is correct but the chart looks truncated (incident
    # 2026-04-20, Man City 0-1 Man United match). 95 matches the x-axis
    # range upper bound set later in this function; shots past 95 extend the
    # anchor accordingly.
    if shots.empty:
        match_end_minute: float = 95.0
    else:
        match_end_minute = max(95.0, float(shots["minute"].max()))

    # (1) Cumulative xG lines per team.
    for team_id, team_name, color in [
        (home_team_id, home_team_name, home_color),
        (away_team_id, away_team_name, away_color),
    ]:
        cum = _cumulative_xg_by_team(shots, team_id, end_minute=match_end_minute)
        fig.add_trace(
            go.Scatter(
                x=cum["minute"],
                y=cum["cumulative_xg"],
                mode="lines",
                line={"shape": "hv", "width": 2.5, "color": color},
                name=f"{team_name} xG",
                hovertemplate=(f"<b>{team_name}</b><br>Minute %{{x:.0f}}'<br>Cumulative xG %{{y:.2f}}<extra></extra>"),
            )
        )

    # (2) Shot ticks per team, sized by per-shot xG.
    for team_id, team_name, color in [
        (home_team_id, home_team_name, home_color),
        (away_team_id, away_team_name, away_color),
    ]:
        team_shots = shots.loc[shots["team_id"] == team_id]
        if team_shots.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=team_shots["minute"],
                y=[-0.1] * len(team_shots),
                mode="markers",
                marker={
                    "symbol": "line-ns-open",
                    "size": team_shots["xg"] * 20 + 4,
                    "color": color,
                    "line": {"width": 1.5},
                },
                name=f"{team_name} shots",
                hovertemplate=(
                    f"<b>{team_name} shot</b><br>Minute %{{x:.0f}}'<br>xG %{{customdata:.2f}}<extra></extra>"
                ),
                customdata=team_shots["xg"],
                showlegend=False,
            )
        )

    # (3) Gold decisive-action rings.
    if not decisive.empty:
        ring_y = _project_to_curves(shots, decisive, home_team_id, away_team_id)
        ring_colors = [home_color if int(tid) == home_team_id else away_color for tid in decisive["team_id"]]
        fig.add_trace(
            go.Scatter(
                x=decisive["minute"].astype(float),
                y=ring_y,
                mode="markers",
                marker={
                    "symbol": "circle-open",
                    "size": 14,
                    "line": {"color": "#d9a300", "width": 2.5},
                    "color": ring_colors,
                },
                name="Decisive moments",
                hovertemplate=("<b>Decisive</b><br>Minute %{x:.0f}'<br>VAEP %{customdata:+.2f}<extra></extra>"),
                customdata=decisive["vaep_value"],
            )
        )

    # (4) Goal stars. MUST use explicit `== 1` equality — Lakebase returns
    # `is_goal` as int 0/1 (NOT bool), and `shots.loc[int_series]` does LABEL
    # indexing rather than boolean indexing, which silently returns up to one
    # row per shot instead of only the goals. Tests formerly used Python
    # booleans and missed this; integer-typed fixtures now guard against it.
    if "is_goal" in shots.columns:
        goals = shots.loc[shots["is_goal"].astype(bool)]
    else:
        goals = shots.iloc[0:0]
    if not goals.empty:
        goal_y = _project_to_curves(shots, goals, home_team_id, away_team_id)
        goal_colors = [home_color if int(tid) == home_team_id else away_color for tid in goals["team_id"]]
        fig.add_trace(
            go.Scatter(
                x=goals["minute"].astype(float),
                y=goal_y,
                mode="markers",
                marker={
                    "symbol": "star",
                    "size": 14,
                    "color": goal_colors,
                    "line": {"color": "#ffffff", "width": 1.5},
                },
                name="Goals",
                hovertemplate=("<b>Goal</b><br>Minute %{x:.0f}'<br>xG of shot %{customdata:.2f}<extra></extra>"),
                customdata=goals["xg"],
            )
        )

    # (5) Red-card markers.
    if not red_cards.empty:
        fig.add_trace(
            go.Scatter(
                x=red_cards["minute"].astype(float),
                y=[-0.25] * len(red_cards),
                mode="markers",
                marker={
                    "symbol": "square-open",
                    "size": 12,
                    "color": "#c84444",
                    "line": {"width": 2},
                },
                name="Red card",
                hovertemplate="<b>Red card</b><br>Minute %{x:.0f}'<extra></extra>",
            )
        )

    # Half-time divider.
    fig.add_shape(
        type="line",
        x0=45,
        x1=45,
        y0=0,
        y1=1,
        yref="paper",
        line={"color": "#888888", "width": 1, "dash": "dash"},
    )

    fig.update_layout(
        xaxis={"title": "Minute", "range": [0, match_end_minute + 1], "showgrid": False},
        yaxis={"title": "Cumulative xG", "showgrid": True, "gridcolor": "#333333"},
        plot_bgcolor="#1a1a1a",
        paper_bgcolor="#1a1a1a",
        font={"color": "#cccccc"},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
        margin={"l": 40, "r": 20, "t": 40, "b": 40},
        hovermode="closest",
        height=320,
    )
    return fig


# ── Row 3: ranked delta table ───────────────────────────────────────────────

# Same iframe-style-isolation rationale as Row 1 — inline the stylesheet.
_DELTA_IFRAME_STYLE = """<style>
html,body{background:#1a1a1a;color:#e6e6e6;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:0;padding:8px;}
.ll-delta-table{width:100%;border-collapse:collapse;font-size:12px;}
.ll-delta-table th,.ll-delta-table td{padding:6px 10px;border-bottom:1px solid #2a2a2a;text-align:left;}
.ll-delta-table th{color:#888;font-size:10px;text-transform:uppercase;letter-spacing:0.5px;font-weight:500;}
.ll-delta-table td.ll-delta-home,.ll-delta-table th.ll-delta-home{color:#5a9;}
.ll-delta-table td.ll-delta-away,.ll-delta-table th.ll-delta-away{color:#a55;}
.ll-delta-table td.ll-delta-delta{font-family:monospace;font-weight:600;color:#eee;}
.ll-delta-table td.ll-delta-direction{color:#ccc;font-size:11px;}
.ll-delta-star{background:rgba(255,215,0,0.04);}
.ll-delta-star td:first-child{color:#d9a300;font-weight:600;}
.ll-delta-avg{color:#666;font-size:10px;margin-left:4px;}
</style>"""


# Metric-specific direction logic. Each callable takes (home_val, away_val,
# home_name, away_name) and returns a prose direction label. PPDA is the
# canonical inverted case (lower = more press).
_DIRECTION_FNS: dict[str, object] = {
    "xG": lambda h, a, hn, an: f"{hn if h > a else an} created more",
    "Progressive passes": lambda h, a, hn, an: f"{hn if h > a else an} progressed the ball more",
    "Shots": lambda h, a, hn, an: f"{hn if h > a else an} shot more",
    "PPDA (lower = more press)": lambda h, a, hn, an: f"{hn if h < a else an} pressed higher",
    "Possession %": lambda h, a, hn, an: f"{hn if h > a else an} held the ball more",
    "Pass completion %": lambda h, a, hn, an: f"{hn if h > a else an} passed more accurately",
}


def _direction_label(metric: str, home_val: float, away_val: float, home_name: str, away_name: str) -> str:
    fn = _DIRECTION_FNS.get(metric)
    if fn is None:
        return ""
    return fn(home_val, away_val, home_name, away_name)  # type: ignore[operator]


def render_delta_table_html(
    *,
    home_stats: dict[str, float],
    away_stats: dict[str, float],
    home_name: str,
    away_name: str,
    league_avgs: dict[str, float],
) -> str:
    """Build the Row 3 HTML: ranked delta table with directional labels, sorted desc by |Δ|."""
    rows: list[tuple[str, float, float, float, str]] = []
    for metric, home_val in home_stats.items():
        if metric not in away_stats:
            continue
        h = float(home_val)
        a = float(away_stats[metric])
        delta = h - a
        direction = _direction_label(metric, h, a, home_name, away_name)
        rows.append((metric, h, a, delta, direction))

    rows.sort(key=lambda r: (-abs(r[3]), r[0]))

    parts: list[str] = [_DELTA_IFRAME_STYLE, '<table class="ll-delta-table">']
    parts.append(
        "<thead><tr>"
        "<th>Dimension</th>"
        f'<th class="ll-delta-home">{escape(home_name)}</th>'
        f'<th class="ll-delta-away">{escape(away_name)}</th>'
        "<th>\u0394</th>"
        "<th>Direction</th>"
        "</tr></thead>"
    )
    parts.append("<tbody>")
    for i, (metric, h, a, delta, direction) in enumerate(rows):
        row_cls = "ll-delta-star" if i == 0 else ""
        star = "\u2605 " if i == 0 else ""
        avg_annot = ""
        if metric in league_avgs:
            avg_annot = f' <span class="ll-delta-avg">(league avg {league_avgs[metric]:.1f})</span>'
        parts.append(
            f'<tr class="{row_cls}">'
            f"<td>{star}{escape(metric)}{avg_annot}</td>"
            f'<td class="ll-delta-home">{h:.2f}</td>'
            f'<td class="ll-delta-away">{a:.2f}</td>'
            f'<td class="ll-delta-delta">{delta:+.2f}</td>'
            f'<td class="ll-delta-direction">{escape(direction)}</td>'
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "\n".join(parts)
