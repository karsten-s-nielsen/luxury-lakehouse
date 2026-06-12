"""Goalkeeper Tracking state — three tabs on the fct_gk_tracking_* marts (ADR-051).

Prefix: gkt_
Sub-views (shared.selected_sub_view):
  - "Distribution Value":     philosophy bump chart + same-passes-two-presets map pair
  - "Defensive Positioning":  ghost tether scene + line-height/game-state splits + box command
  - "Shot Stopping":          pre-shot cone scene + positioning-vs-outcome map

Chart builders are 1:1 ports of the v3 real-data prototypes
(docs/ui-cycles/gk-redesign/generate_mockups.py — normative layout). The bump chart and every
"vs sample" delta come from fetch_gk_pool_stats (review H2) — never a single-GK fetch.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from queries.gk_tracking import (
    PRESET_COLUMN,
    fetch_gk_actions,
    fetch_gk_pool_stats,
    fetch_scene_frame,
)
from services.ghost_grid import resolve_provider

from state.shared import register_page_refresher

logger = logging.getLogger(__name__)

GKT_SUB_VIEW_LOV: list[str] = ["Distribution Value", "Defensive Positioning", "Shot Stopping"]
GKT_PRESET_LOV: list[str] = list(PRESET_COLUMN.keys())

# App palette (render.py + state/goalkeeper.py conventions)
_BG = "#1a1a2e"
_AMBER = "#f59e0b"
_BLUE = "#3b82f6"
_RED = "#ef4444"
_GREY = "rgba(160,160,180,0.45)"
_GRID = "rgba(255,255,255,0.07)"

_LAYOUT: dict[str, Any] = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor=_BG,
    font=dict(color="white", size=13),
    margin=dict(l=60, r=30, t=60, b=50),
)

# Plausibility gate for ghost deviation (sentinel-ish pre-shot rows exist upstream; the
# prototype validated this range on the v4 sample — revisit with the full corpus).
_MAX_PLAUSIBLE_DEV_M = 8.0

# ---------------------------------------------------------------------------
# Exported state variables (all gkt_ prefixed)
# ---------------------------------------------------------------------------
gkt_player_lov: list[str] = []
gkt_preset_lov: list[str] = list(PRESET_COLUMN.keys())
gkt_selected_player: str | None = None
gkt_selected_preset: str = "Default"
gkt_compare_preset: str = "Possession"
gkt_scope_player: str = ""
gkt_scope_preset: str = ""
gkt_warning_text: str = ""
gkt_data_freshness: str = ""

# Tab 1
gkt_bump_figure: go.Figure | None = None
gkt_map_selected_figure: go.Figure | None = None
gkt_map_compare_figure: go.Figure | None = None
gkt_xtgk_mean_val: str = "—"
gkt_completion_val: str = "—"
gkt_n_dist_val: str = "—"

# Tab 2
gkt_scene_figure: go.Figure | None = None
gkt_context_figure: go.Figure | None = None
gkt_closing_figure: go.Figure | None = None
gkt_deviation_val: str = "—"
gkt_closing_val: str = "—"
gkt_reach_val: str = "—"

# Tab 3
gkt_cone_figure: go.Figure | None = None
gkt_shotmap_figure: go.Figure | None = None
gkt_shots_val: str = "—"
gkt_goals_val: str = "—"
gkt_offline_val: str = "—"

_gkt_player_map: dict[str, int] = {}

__all__ = [
    "GKT_PRESET_LOV",
    "GKT_SUB_VIEW_LOV",
    "PRESET_COLUMN",
    "gkt_bump_figure",
    "gkt_closing_figure",
    "gkt_closing_val",
    "gkt_completion_val",
    "gkt_compare_preset",
    "gkt_cone_figure",
    "gkt_context_figure",
    "gkt_data_freshness",
    "gkt_deviation_val",
    "gkt_goals_val",
    "gkt_map_compare_figure",
    "gkt_map_selected_figure",
    "gkt_n_dist_val",
    "gkt_offline_val",
    "gkt_on_player_change",
    "gkt_on_preset_change",
    "gkt_player_lov",
    "gkt_preset_lov",
    "gkt_reach_val",
    "gkt_refresh",
    "gkt_scene_figure",
    "gkt_scope_player",
    "gkt_scope_preset",
    "gkt_selected_player",
    "gkt_selected_preset",
    "gkt_shotmap_figure",
    "gkt_shots_val",
    "gkt_warning_text",
    "gkt_xtgk_mean_val",
]


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested in test_gk_tracking_state.py)
# ---------------------------------------------------------------------------
def _format_metric(value: Any, fmt: str) -> str:
    """NaN/None -> em dash; else fmt.format(value)."""
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
    except (TypeError, ValueError):
        pass
    return fmt.format(value)


def _preset_rank_frame(pool: pd.DataFrame, presets: list[str]) -> pd.DataFrame:
    """Rank (1 = best) each GK under each preset, from the POOL stats frame (review H2).

    Index: player_display_name; columns: preset labels; values: int ranks.
    """
    out = pd.DataFrame(index=pool["player_display_name"])
    for label in presets:
        col = PRESET_COLUMN[label]
        vals = pd.to_numeric(pool[col], errors="coerce")
        out[label] = vals.rank(ascending=False, method="min").to_numpy()
    return out.astype("Int64")


def _line_height_terciles(shots: pd.DataFrame) -> tuple[list[str], list[float]]:
    """Mean ghost deviation by defensive line-height terciles. Labels carry n=."""
    df = shots.dropna(subset=["line_height_m", "ghost_deviation_m"])
    df = df[pd.to_numeric(df["ghost_deviation_m"], errors="coerce") < _MAX_PLAUSIBLE_DEV_M]
    if df.empty:
        return [], []
    terc = df["line_height_m"].quantile([1 / 3, 2 / 3]).to_numpy()
    bands = [
        (f"Deep block (<{terc[0]:.0f} m)", df[df.line_height_m < terc[0]]),
        ("Mid block", df[(df.line_height_m >= terc[0]) & (df.line_height_m < terc[1])]),
        (f"High line (≥{terc[1]:.0f} m)", df[df.line_height_m >= terc[1]]),
    ]
    cats = [f"{label}  n={len(b)}" for label, b in bands]
    means = [float(b.ghost_deviation_m.mean()) if len(b) else float("nan") for _, b in bands]
    return cats, means


# ---------------------------------------------------------------------------
# Chart builders (ports of generate_mockups.py v3 tab functions)
# ---------------------------------------------------------------------------
def _build_bump_figure(pool: pd.DataFrame, selected_name: str | None, preset: str) -> go.Figure | None:
    if pool.empty:
        return None
    ranks = _preset_rank_frame(pool, GKT_PRESET_LOV)
    n_gk = len(ranks)
    fig = go.Figure()
    for name, row in ranks.iterrows():
        hot = name == selected_name
        fig.add_trace(
            go.Scatter(
                x=GKT_PRESET_LOV,
                y=[int(v) if pd.notna(v) else None for v in row],
                mode="lines+markers+text",
                text=[str(name) if i == 0 else "" for i in range(len(GKT_PRESET_LOV))],
                textposition="middle left",
                textfont=dict(size=10),
                line=dict(color=_AMBER if hot else _GREY, width=4 if hot else 1.2),
                marker=dict(size=9 if hot else 5),
                showlegend=False,
                hovertemplate=f"<b>{name}</b><br>%{{x}}: rank %{{y}}<extra></extra>",
            )
        )
    if preset in GKT_PRESET_LOV:
        i = GKT_PRESET_LOV.index(preset)
        fig.add_shape(
            type="rect",
            x0=i - 0.4,
            x1=i + 0.4,
            y0=0.5,
            y1=n_gk + 0.5,
            fillcolor="rgba(245,158,11,0.10)",
            line=dict(color="rgba(245,158,11,0.45)", width=1),
            layer="below",
        )
    fig.update_layout(
        **_LAYOUT,
        height=380,
        title=f"Mean xT-GK per distribution — rank under every preset ({n_gk} GKs)",
        yaxis=dict(title="rank (1 = best)", autorange="reversed", dtick=1, gridcolor=_GRID),
        xaxis=dict(gridcolor=_GRID),
        margin=dict(l=130, r=30, t=60, b=50),
    )
    return fig


def _half_pitch_shapes() -> list[dict]:
    c = "rgba(224,224,224,0.4)"
    return [
        dict(type="rect", x0=0, y0=0, x1=72, y1=68, line=dict(color=c, width=1)),
        dict(type="rect", x0=0, y0=13.84, x1=16.5, y1=54.16, line=dict(color=c, width=1)),
        dict(type="line", x0=52.5, y0=0, x1=52.5, y1=68, line=dict(color=c, dash="dot", width=1)),
        dict(type="line", x0=0, y0=30.34, x1=0, y1=37.66, line=dict(color="#e0e0e0", width=5)),
    ]


_MAX_MAP_ARROWS = 500  # one Plotly trace per arrow — bounded for browser render time (audit)


def _build_dist_map(dist: pd.DataFrame, value_col: str, title: str) -> go.Figure | None:
    if dist.empty or value_col not in dist.columns:
        return None
    capped = len(dist) > _MAX_MAP_ARROWS
    if capped:
        # No silent caps: the title carries what was dropped (UX standard).
        title = f"{title} — showing first {_MAX_MAP_ARROWS} of {len(dist)} passes"
        dist = dist.iloc[:_MAX_MAP_ARROWS]
    v = pd.to_numeric(dist[value_col], errors="coerce").to_numpy()
    finite = v[np.isfinite(v)]
    if finite.size == 0:
        return None
    cmin, cmax = float(np.percentile(finite, 5)), float(np.percentile(finite, 95))
    cmax = max(cmax, cmin + 1e-6)
    fig = go.Figure()
    sx, sy = dist.start_x.to_numpy(float), dist.start_y.to_numpy(float)
    ex, ey = dist.end_x.to_numpy(float), dist.end_y.to_numpy(float)
    for i in range(len(dist)):
        if not np.isfinite(v[i]):
            continue
        frac = float(np.clip((v[i] - cmin) / (cmax - cmin), 0, 1))
        color = f"rgba({int(120 + 135 * frac)},{int(130 + 28 * frac)},{int(200 - 150 * frac)},0.85)"
        fig.add_trace(
            go.Scatter(
                x=[sx[i], ex[i]],
                y=[sy[i], ey[i]],
                mode="lines",
                line=dict(color=color, width=1.0 + 3.2 * frac),
                showlegend=False,
                hoverinfo="skip",
            )
        )
    for s in _half_pitch_shapes():
        fig.add_shape(**s)
    fig.update_layout(
        **_LAYOUT,
        height=420,
        title=title,
        xaxis=dict(visible=False, range=[-3, 80], scaleanchor="y"),
        yaxis=dict(visible=False, range=[-3, 71]),
    )
    fig.add_annotation(
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.04,
        showarrow=False,
        text=f"Arrow color: blue/grey = low, amber = high xT-GK ({cmin:.3f}…{cmax:.3f}/pass, 5th-95th pct)",
        font=dict(size=11, color="rgba(255,255,255,0.6)"),
    )
    return fig


def _build_scene_figure(shot: pd.Series, frame: pd.DataFrame, gk_name: str) -> go.Figure | None:
    ghost_x, ghost_y = float(shot.ghost_gk_x), float(shot.ghost_gk_y)
    actual_x, actual_y = float(shot.gk_actual_x), float(shot.gk_actual_y)
    grid = resolve_provider().grid(
        ghost_x=ghost_x,
        ghost_y=ghost_y,
        density_spread=float(shot.ghost_gk_density_spread or 0.0),
        frame_players=frame if not frame.empty else None,
    )
    fig = go.Figure()
    fig.add_trace(
        go.Contour(
            x=grid.xs,
            y=grid.ys,
            z=grid.z,
            colorscale=[
                [0, "rgba(26,26,46,0)"],
                [0.35, "rgba(59,130,246,0.18)"],
                [0.7, "rgba(42,157,143,0.45)"],
                [1, "rgba(245,158,11,0.85)"],
            ],
            contours=dict(coloring="fill", showlines=False),
            showscale=False,
            hoverinfo="skip",
        )
    )
    if not frame.empty:
        mirrored = bool(shot.gk_frame_mirrored)
        px = frame.x.to_numpy(float)
        py = frame.y.to_numpy(float)
        if mirrored:
            px, py = 105.0 - px, 68.0 - py
        fig.add_trace(
            go.Scatter(x=px, y=py, mode="markers", name="Players (tracked frame)", marker=dict(size=8, color=_GREY))
        )
        bx = float(frame.ball_x.iloc[0]) if pd.notna(frame.ball_x.iloc[0]) else None
        if bx is not None:
            by = float(frame.ball_y.iloc[0])
            if mirrored:
                bx, by = 105.0 - bx, 68.0 - by
            fig.add_trace(
                go.Scatter(
                    x=[bx],
                    y=[by],
                    mode="markers",
                    name="Ball",
                    marker=dict(size=11, color="white", symbol="circle-open", line=dict(width=2)),
                )
            )
    fig.add_trace(
        go.Scatter(
            x=[ghost_x, actual_x],
            y=[ghost_y, actual_y],
            mode="lines",
            name="Deviation tether",
            line=dict(color="white", dash="dash", width=2.5),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[ghost_x],
            y=[ghost_y],
            mode="markers",
            name="Ghost GK (model optimum)",
            marker=dict(size=28, color="rgba(245,158,11,0.30)", line=dict(color=_AMBER, width=2.5)),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[actual_x],
            y=[actual_y],
            mode="markers",
            name=f"{gk_name} (actual)",
            marker=dict(size=15, color=_BLUE, line=dict(color="white", width=2)),
        )
    )
    dev = float(shot.ghost_deviation_m)
    fig.add_annotation(
        x=(ghost_x + actual_x) / 2 + 6.0,
        y=(ghost_y + actual_y) / 2,
        text=f"{dev:.1f} m off optimum",
        font=dict(color="white", size=12),
        bgcolor="rgba(26,26,46,0.9)",
        showarrow=True,
        arrowhead=0,
        ax=40,
        ay=0,
    )
    grid_note = {
        "stored": "density approximated from stored optimum + spread",
        "model": "live model density grid",
        "stored-fallback": "MODEL UNAVAILABLE — stored approximation shown",
    }[grid.source]
    fig.update_layout(
        **_LAYOUT,
        height=520,
        title=f"Actual vs model-optimal position — {grid_note}",
        xaxis=dict(visible=False, range=[-1.5, 33], scaleanchor="y"),
        yaxis=dict(visible=False, range=[2, 66]),
        legend=dict(orientation="h", y=-0.04),
    )
    for s in _half_pitch_shapes()[:2]:
        fig.add_shape(**s)
    fig.add_shape(type="line", x0=0, y0=30.34, x1=0, y1=37.66, line=dict(color="#e0e0e0", width=5))
    return fig


def _build_context_figure(shots: pd.DataFrame) -> go.Figure | None:
    cats, means = _line_height_terciles(shots)
    if not cats:
        return None
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=means,
            y=cats,
            mode="markers+lines",
            name="By line height",
            line=dict(color="rgba(245,158,11,0.45)", width=2),
            marker=dict(size=12, color=_AMBER, line=dict(color="white", width=1.5)),
        )
    )
    # Game-state split renders ONLY when the data supports it (spec section 9 item 4: the v4
    # sample is 99.9% 'drawing' — open upstream question; hidden-with-caption until then).
    gs = shots.dropna(subset=["game_state", "ghost_deviation_m"])
    gs = gs[pd.to_numeric(gs["ghost_deviation_m"], errors="coerce") < _MAX_PLAUSIBLE_DEV_M]
    caption = ""
    if gs["game_state"].nunique() >= 2:
        rows = gs.groupby("game_state")["ghost_deviation_m"].agg(["mean", "size"])
        fig.add_trace(
            go.Scatter(
                x=rows["mean"],
                y=[f"{str(ix).title()}  n={int(n)}" for ix, n in rows["size"].items()],
                mode="markers",
                name="By game state",
                marker=dict(size=12, color=_BLUE, line=dict(color="white", width=1.5)),
            )
        )
    else:
        caption = "Game-state split hidden: this sample is effectively single-state (upstream data question, ADR-051)."
    fig.update_layout(
        **_LAYOUT,
        height=320,
        title="When does he leave the model line? (mean deviation, m)",
        xaxis=dict(title="mean deviation from ghost optimum (m, lower = more orthodox)", gridcolor=_GRID),
        legend=dict(orientation="h", y=-0.25),
    )
    if caption:
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=-0.55,
            showarrow=False,
            text=caption,
            font=dict(size=11, color="rgba(255,255,255,0.55)"),
        )
    return fig


def _build_closing_figure(pool: pd.DataFrame, gk_row: pd.Series | None, gk_name: str) -> go.Figure | None:
    if pool.empty:
        return None
    zones = [
        ("Six-yard box", "closing_min_six_yard_mean_s"),
        ("Near post", "closing_min_near_post_mean_s"),
        ("Far post", "closing_min_far_post_mean_s"),
    ]
    sample = [float(pd.to_numeric(pool[c], errors="coerce").mean()) for _, c in zones]
    fig = go.Figure()
    labels = [z for z, _ in zones]
    if gk_row is not None:
        mine = [float(gk_row[c]) if pd.notna(gk_row[c]) else float("nan") for _, c in zones]
        for lbl, a, b in zip(labels, sample, mine, strict=True):
            if np.isfinite(b):
                fig.add_trace(
                    go.Scatter(
                        x=[a, b],
                        y=[lbl, lbl],
                        mode="lines",
                        showlegend=False,
                        line=dict(color="rgba(59,130,246,0.55)", width=4),
                    )
                )
        fig.add_trace(
            go.Scatter(
                x=mine,
                y=labels,
                mode="markers",
                name=gk_name,
                marker=dict(size=13, color=_BLUE, line=dict(color="white", width=1.5)),
            )
        )
    fig.add_trace(
        go.Scatter(
            x=sample,
            y=labels,
            mode="markers",
            name="Sample average",
            marker=dict(size=11, color=_GREY, line=dict(color="white", width=1)),
        )
    )
    fig.update_layout(
        **_LAYOUT,
        height=320,
        title="Command of the box — min closing time (s, lower = better)",
        xaxis=dict(title="minimum closing time (s)", gridcolor=_GRID),
        yaxis=dict(categoryorder="array", categoryarray=labels[::-1]),
        legend=dict(orientation="h", y=-0.25),
    )
    return fig


def _build_cone_figure(shot: pd.Series, gk_name: str) -> go.Figure | None:
    sbx, sby = float(shot.start_x), float(shot.start_y)
    kx, ky = 105.0 - float(shot.gk_actual_x), 68.0 - float(shot.gk_actual_y)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[sbx, 105.0, 105.0, sbx],
            y=[sby, 30.34, 37.66, sby],
            fill="toself",
            fillcolor="rgba(239,68,68,0.16)",
            line=dict(color="rgba(239,68,68,0.6)", width=1.5),
            name="Shot cone (ball to posts)",
            mode="lines",
        )
    )
    fig.add_trace(
        go.Scatter(x=[sbx], y=[sby], mode="markers", name="Ball (shot origin)", marker=dict(size=11, color="white"))
    )
    fig.add_trace(
        go.Scatter(
            x=[sbx, 105],
            y=[sby, 34.0],
            mode="lines",
            name="Shot line (to centre)",
            line=dict(color="rgba(255,255,255,0.5)", dash="dot", width=1.5),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[kx],
            y=[ky],
            mode="markers",
            name=f"{gk_name} (pre-shot)",
            marker=dict(size=13, color=_BLUE, line=dict(color="white", width=1.5)),
        )
    )
    ang = abs(float(shot.pre_shot_gk_angle_to_shot_trajectory or 0.0)) * 180.0 / np.pi
    if ang > 90.0:
        ang = 180.0 - ang  # stored angle is signed direction; show the acute offset
    off_line = float(shot.gk_actual_x)
    fig.add_annotation(
        x=kx,
        y=ky + 3.2,
        text=f"{off_line:.1f} m off line · {ang:.0f}° off shot line",
        font=dict(size=11),
        bgcolor="rgba(26,26,46,0.9)",
        showarrow=False,
    )
    fig.add_shape(type="line", x0=105, y0=30.34, x1=105, y1=37.66, line=dict(color="#e0e0e0", width=5))
    fig.add_shape(type="rect", x0=88.5, y0=13.84, x1=105, y1=54.16, line=dict(color="rgba(224,224,224,0.4)", width=1))
    result = str(shot.get("action_result") or "")
    fig.update_layout(
        **_LAYOUT,
        height=460,
        title=f"Pre-shot geometry — {'GOAL conceded' if result == 'success' else 'shot faced'}",
        xaxis=dict(visible=False, range=[70, 107], scaleanchor="y"),
        yaxis=dict(visible=False, range=[6, 62]),
        legend=dict(orientation="h", y=-0.04),
    )
    return fig


def _build_shotmap_figure(shots: pd.DataFrame) -> go.Figure | None:
    df = shots.dropna(subset=["gk_actual_x", "gk_actual_y", "start_x", "start_y"]).copy()
    if df.empty:
        return None
    sox = 105.0 - df.start_x.to_numpy(float)  # shot origin into canonical (goal at x=0)
    soy = 68.0 - df.start_y.to_numpy(float)
    ax_ = df.gk_actual_x.to_numpy(float)
    ay_ = df.gk_actual_y.to_numpy(float)
    dd = np.hypot(sox, soy - 34.0)
    lat = np.abs((0.0 - sox) * (soy - ay_) - (sox - ax_) * (34.0 - soy)) / np.where(dd == 0, 1, dd)
    is_goal = (df["action_result"] == "success").to_numpy()
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=ax_[~is_goal],
            y=lat[~is_goal],
            mode="markers",
            name=f"No goal (n={int((~is_goal).sum())})",
            marker=dict(size=8, color=_BLUE, opacity=0.75),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=ax_[is_goal],
            y=lat[is_goal],
            mode="markers",
            name=f"Goal conceded (n={int(is_goal.sum())})",
            marker=dict(size=12, color=_RED, symbol="star"),
        )
    )
    fig.update_layout(
        **_LAYOUT,
        height=460,
        title="Every shot faced — where was he standing?",
        xaxis=dict(title="distance off goal line at shot (m)", gridcolor=_GRID),
        yaxis=dict(title="lateral error vs shot line (m)", gridcolor=_GRID),
        legend=dict(orientation="h", y=-0.18),
    )
    return fig


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------
def _pick_scene_shot(shots: pd.DataFrame) -> pd.Series | None:
    s = shots.dropna(subset=["ghost_deviation_m", "gk_actual_x", "ghost_gk_x"])
    s = s[pd.to_numeric(s["ghost_deviation_m"], errors="coerce") < _MAX_PLAUSIBLE_DEV_M]
    if s.empty:
        return None
    return s.sort_values("ghost_deviation_m", ascending=False).iloc[0]


def _selected_key(state: Any) -> int | None:
    label = state.gkt_selected_player
    return _gkt_player_map.get(label) if label else None


def _vs_sample(own: float | None, sample: float | None, fmt: str, lower_better: bool = False) -> str:
    own_s = _format_metric(own, fmt)
    if own is None or own_s == "—" or sample is None or pd.isna(sample):
        return own_s
    delta = float(own) - float(sample)
    good = (delta < 0) if lower_better else (delta > 0)
    arrow = "▲" if good else "▼"
    return f"{own_s} {arrow} (sample {fmt.format(sample)})"


def gkt_refresh(state: Any) -> None:
    """Full refresh: sub-view LOV, GK LOV (pool-derived), then all three tabs."""
    global _gkt_player_map

    current_lov = getattr(state, "sub_view_lov", None) or []
    if not current_lov or list(current_lov) != GKT_SUB_VIEW_LOV:
        state.sub_view_lov = GKT_SUB_VIEW_LOV
    if not state.selected_sub_view or state.selected_sub_view not in GKT_SUB_VIEW_LOV:
        state.selected_sub_view = GKT_SUB_VIEW_LOV[0]

    try:
        pool = fetch_gk_pool_stats()
    except Exception:
        logger.exception("Failed to fetch GK pool stats")
        state.gkt_warning_text = "Something went wrong loading goalkeeper data. Try refreshing."
        return

    if pool.empty:
        state.gkt_warning_text = (
            "No tracking-provider goalkeeper data available yet (marts not built or synced tables empty)."
        )
        return

    _gkt_player_map = {
        str(name): int(key) for name, key in zip(pool["player_display_name"], pool["gk_player_key"], strict=True)
    }
    state.gkt_player_lov = list(_gkt_player_map.keys())
    if not state.gkt_selected_player or state.gkt_selected_player not in _gkt_player_map:
        state.gkt_selected_player = state.gkt_player_lov[0] if state.gkt_player_lov else None
    if state.gkt_selected_preset not in GKT_PRESET_LOV:
        state.gkt_selected_preset = "Default"

    name = state.gkt_selected_player
    preset = state.gkt_selected_preset
    compare = (
        state.gkt_compare_preset
        if state.gkt_compare_preset != preset
        else ("Possession" if preset != "Possession" else "Counter")
    )
    state.gkt_compare_preset = compare
    state.gkt_scope_player = name or ""
    state.gkt_scope_preset = preset
    state.gkt_warning_text = ""

    key = _selected_key(state)
    mine = pool[pool.gk_player_key == key]
    mine_row = mine.iloc[0] if len(mine) else None

    # ---- Tab 1 ----
    state.gkt_bump_figure = _build_bump_figure(pool, name, preset)
    dist = pd.DataFrame()
    if key is not None:
        try:
            dist = fetch_gk_actions(str(key), "distribution")
        except Exception:
            logger.exception("Failed to fetch GK distributions for key=%s", key)
    sel_col = {
        "Default": "xt_gk",
        "Possession": "xt_gk_possession",
        "Counter": "xt_gk_counter",
        "Direct": "xt_gk_direct",
        "High Press": "xt_gk_high_press",
        "Low Block": "xt_gk_low_block",
    }
    state.gkt_map_selected_figure = _build_dist_map(dist, sel_col[preset], f"{name} under {preset.upper()}")
    state.gkt_map_compare_figure = _build_dist_map(dist, sel_col[compare], f"The SAME passes under {compare.upper()}")
    if mine_row is not None:
        sample_mean = float(pd.to_numeric(pool[PRESET_COLUMN[preset]], errors="coerce").mean())
        state.gkt_xtgk_mean_val = _vs_sample(mine_row[PRESET_COLUMN[preset]], sample_mean, "{:+.4f}")
        state.gkt_completion_val = _vs_sample(
            mine_row["dist_completion_mean"],
            float(pd.to_numeric(pool["dist_completion_mean"], errors="coerce").mean()),
            "{:.0%}",
        )
        state.gkt_n_dist_val = _format_metric(mine_row["n_distributions"], "{:.0f}")

    # ---- Tabs 2 + 3 ----
    shots = pd.DataFrame()
    if key is not None:
        try:
            shots = fetch_gk_actions(str(key), "shots")
        except Exception:
            logger.exception("Failed to fetch GK shots for key=%s", key)
    scene = _pick_scene_shot(shots) if not shots.empty else None
    if scene is not None:
        frame = pd.DataFrame()
        if pd.notna(scene.frame_id):
            try:
                frame = fetch_scene_frame(int(scene.match_key), int(scene.period_id), int(scene.frame_id))
            except Exception:
                logger.exception("Failed to fetch scene frame")
        state.gkt_scene_figure = _build_scene_figure(scene, frame, name or "GK")
        state.gkt_cone_figure = _build_cone_figure(scene, name or "GK")
    else:
        state.gkt_scene_figure = None
        state.gkt_cone_figure = None
    state.gkt_context_figure = _build_context_figure(shots) if not shots.empty else None
    state.gkt_closing_figure = _build_closing_figure(pool, mine_row, name or "GK")
    state.gkt_shotmap_figure = _build_shotmap_figure(shots) if not shots.empty else None

    if mine_row is not None:
        state.gkt_deviation_val = _vs_sample(
            mine_row["ghost_deviation_mean_m"],
            float(pd.to_numeric(pool["ghost_deviation_mean_m"], errors="coerce").mean()),
            "{:.1f} m",
            lower_better=True,
        )
        state.gkt_closing_val = _vs_sample(
            mine_row["closing_min_six_yard_mean_s"],
            float(pd.to_numeric(pool["closing_min_six_yard_mean_s"], errors="coerce").mean()),
            "{:.2f} s",
            lower_better=True,
        )
        state.gkt_reach_val = _format_metric(mine_row["reachable_area_mean_m2"], "{:.0f} m²")
        state.gkt_shots_val = _format_metric(mine_row["shots_faced"], "{:.0f}")
        state.gkt_goals_val = _format_metric(mine_row["goals_conceded"], "{:.0f}")
    if not shots.empty:
        ok = shots.dropna(subset=["gk_actual_x"])
        state.gkt_offline_val = _format_metric(
            float(pd.to_numeric(ok["gk_actual_x"], errors="coerce").mean()) if len(ok) else None, "{:.1f} m"
        )

    from queries.gk_tracking import fetch_gk_data_freshness  # O2: this page's OWN tables

    state.gkt_data_freshness = fetch_gk_data_freshness()
    logger.info(
        "GK tracking refresh: pool=%d gk=%s preset=%s shots=%d dist=%d", len(pool), name, preset, len(shots), len(dist)
    )


def gkt_on_player_change(state: Any, var_name: str, var_value: Any) -> None:
    gkt_refresh(state)


def gkt_on_preset_change(state: Any, var_name: str, var_value: Any) -> None:
    gkt_refresh(state)


register_page_refresher("Goalkeeper-Tracking", gkt_refresh)
