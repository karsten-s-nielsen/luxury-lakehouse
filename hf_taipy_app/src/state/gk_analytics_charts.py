"""Plotly chart builders for the GK Analytics insight views.

- Distribution profile (offensive): cohort 2-D scatter — % adds-threat vs directness, quadrant story
  (ADR-061 redesign; the old game-model preset ladder was statistically degenerate, ~0.99 collinear).
- Sweeper profile (defensive): per-metric cohort STRIP (each keeper a dot) + median, keeper highlighted.
- Line height (defensive): cohort strip of own-goal distances + median, keeper highlighted (descriptive).

All cohort comparisons are drawn as individual-keeper strips, not IQR boxes (Kirk: a strip shows the
real spread/skew/bimodality a box summary hides).
"""

from __future__ import annotations

import math
import statistics
from typing import Any

import plotly.graph_objects as go

_MIN_COHORT = 8  # min cohort keepers to draw the strip + median (else show value alone)


def _jit(y: float, j: int, spread: float) -> float:
    """Deterministic golden-ratio vertical jitter so overlapping strip dots spread without RNG."""
    return y + (((j * 0.61803) % 1.0) - 0.5) * spread


def build_distribution_scatter_figure(
    points: list[tuple[str, float, float, int, bool]],
    *,
    share_median: float | None,
    progress_median: float | None,
) -> go.Figure | None:
    """Cohort 2-D scatter: x = avg forward progression (m; short-safe <-> long-direct), y = % of
    distributions that add threat (xt_gk_v2>0). Each WC keeper is a point (size = volume = confidence),
    the selected keeper highlighted, with a cohort-median crosshair splitting the four quadrants
    (proactive / recycler / risk-without-reward). Cross-keeper POSITIONING, not a rank.
    `points` = [(name, progress_m, share, n, is_selected)]."""
    if not points:
        return None
    xs = [p[1] for p in points]
    ys = [p[2] * 100.0 for p in points]  # share -> %
    max_n = max(p[3] for p in points) or 1
    _hover = "%{text}<br>%{y:.0f}% add threat · %{x:.0f} m forward<extra></extra>"

    def _size(n: int) -> float:
        return 9 + 22 * math.sqrt(n / max_n)

    cohort = [p for p in points if not p[4]]
    sel = next((p for p in points if p[4]), None)
    fig = go.Figure()
    if cohort:  # grey cohort dots, drawn first (behind)
        fig.add_trace(
            go.Scatter(
                x=[p[1] for p in cohort],
                y=[p[2] * 100.0 for p in cohort],
                mode="markers",
                marker=dict(size=[_size(p[3]) for p in cohort], color="rgba(150,165,185,0.40)"),
                text=[p[0] for p in cohort],
                hovertemplate=_hover,
                showlegend=False,
            )
        )
    if sel is not None:  # selected keeper: amber halo + solid dot + labelled arrow, ON TOP
        sx, sy = sel[1], sel[2] * 100.0
        ssize = max(_size(sel[3]), 16.0)
        fig.add_trace(
            go.Scatter(
                x=[sx],
                y=[sy],
                mode="markers",
                marker=dict(size=ssize + 18, color="rgba(245,158,11,0.18)", line=dict(color=_AMBER, width=1.5)),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[sx],
                y=[sy],
                mode="markers",
                marker=dict(size=ssize, color=_AMBER, line=dict(color="white", width=2.5)),
                text=[sel[0]],
                hovertemplate=_hover,
                showlegend=False,
            )
        )
        fig.add_annotation(
            x=sx,
            y=sy,
            text=f"<b>{sel[0]}</b>",
            showarrow=True,
            arrowhead=2,
            arrowcolor=_AMBER,
            arrowwidth=1.5,
            ax=30,
            ay=-28,
            font=dict(size=13, color=_AMBER),
            bgcolor="rgba(20,22,34,0.85)",
            bordercolor=_AMBER,
            borderwidth=1,
            borderpad=3,
        )
    x_lo, x_hi = min(xs), max(xs)
    y_lo, y_hi = min(ys), max(ys)
    xpad = (x_hi - x_lo) * 0.15 or 2.0
    ypad = (y_hi - y_lo) * 0.15 or 2.0
    x0, x1 = x_lo - xpad, x_hi + xpad
    yy0, yy1 = max(0.0, y_lo - ypad), y_hi + ypad
    # cohort-median crosshair -> quadrants
    if share_median is not None and progress_median is not None:
        sm = share_median * 100.0
        fig.add_shape(
            type="line",
            x0=progress_median,
            x1=progress_median,
            y0=yy0,
            y1=yy1,
            line=dict(color="#6e7681", width=1, dash="dash"),
        )
        fig.add_shape(type="line", x0=x0, x1=x1, y0=sm, y1=sm, line=dict(color="#6e7681", width=1, dash="dash"))
        corners = [
            (x1, yy1, "right", "top", "proactive · direct"),
            (x0, yy1, "left", "top", "proactive · short"),
            (x1, yy0, "right", "bottom", "direct, low reward"),
            (x0, yy0, "left", "bottom", "secure recycler"),
        ]
        for cx, cy, xa, ya, txt in corners:
            fig.add_annotation(
                x=cx, y=cy, xanchor=xa, yanchor=ya, text=txt, showarrow=False, font=dict(size=9, color="#5d7290")
            )
    fig.update_layout(
        **{**_LAYOUT, "margin": dict(l=70, r=30, t=54, b=52)},
        height=420,
        title=dict(
            text="point size = volume · <span style='color:#f59e0b'>● this keeper</span> · "
            "grey = cohort · dashed = cohort median (position, not a rank)",
            font=dict(size=11, color="#8b9bb0"),
        ),
        xaxis=dict(
            title="avg forward progression (m) · short-safe ← → long-direct",
            range=[x0, x1],
            gridcolor=_GRID,
            zeroline=False,
        ),
        yaxis=dict(title="% of distributions that add threat", range=[yy0, yy1], gridcolor=_GRID, zeroline=False),
    )
    return fig


_BG = "#1a1a2e"
_GRID = "rgba(255,255,255,0.07)"
_AMBER = "#f59e0b"
# ColorBrewer-safe diverging pair (RdYlBu endpoints) — NOT red/green.
_POS_COLOR = "#2c7bb6"  # blue  — adds threat (>= 0)
_NEG_COLOR = "#fdae61"  # orange — removes threat (< 0)
_DOT = "#5b9bd5"
_MED = "#aab8cc"

_LAYOUT: dict[str, Any] = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor=_BG,
    font=dict(color="white", size=13),
    margin=dict(l=120, r=40, t=50, b=44),
)


def build_sweeper_profile_figure(metrics: list[tuple[str, float, str, list[float], bool]]) -> go.Figure | None:
    """One horizontal track per sweeper metric. The cohort is drawn as a STRIP — every keeper a faint
    dot (Kirk: reveals spread/skew/clustering that an IQR box hides) — with the cohort median ticked
    and THIS keeper's dot highlighted blue (better) / orange (worse). `metrics` =
    [(label, value, value_str, cohort_values, lower_is_better)]. Position in the spread, not a rank."""
    if not metrics:
        return None
    fig = go.Figure()
    n = len(metrics)
    for i, (label, value, value_str, cohort, lower_is_better) in enumerate(metrics):
        y = n - 1 - i
        pts = [value, *cohort]
        lo, hi = min(pts), max(pts)
        span = (hi - lo) or 1.0
        lo, hi = lo - 0.12 * span, hi + 0.12 * span

        def _x(v: float, lo: float = lo, hi: float = hi, flip: bool = lower_is_better) -> float:
            frac = (v - lo) / (hi - lo)
            return (1.0 - frac) if flip else frac  # flip so lower-is-better reads right=good

        fig.add_trace(
            go.Scatter(
                x=[0.0, 1.0],
                y=[y, y],
                mode="lines",
                line=dict(color="#2a3a4f", width=2),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        med = statistics.median(cohort) if len(cohort) >= _MIN_COHORT else None
        if med is not None:
            fig.add_trace(  # cohort strip — one faint dot per keeper, jittered to show density
                go.Scatter(
                    x=[_x(c) for c in cohort],
                    y=[_jit(y, j, 0.26) for j in range(len(cohort))],
                    mode="markers",
                    marker=dict(size=7, color="rgba(150,165,185,0.45)"),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
            fig.add_shape(type="line", x0=_x(med), x1=_x(med), y0=y - 0.28, y1=y + 0.28, line=dict(color=_MED, width=2))
        else:
            fig.add_annotation(
                x=0.5, y=y + 0.32, text="cohort too small", showarrow=False, font=dict(size=9, color="#8b9bb0")
            )
        # _x() orients right = better, so compare screen positions: blue = better than median, orange = worse.
        if med is None:
            dot_color, pos_word = _DOT, ""
        else:
            good = _x(value) > _x(med)
            dot_color = _POS_COLOR if good else _NEG_COLOR
            pos_word = "▲ better than cohort" if good else "▼ worse than cohort"
        fig.add_trace(
            go.Scatter(
                x=[_x(value)],
                y=[y],
                mode="markers",
                marker=dict(size=16, color=dot_color, line=dict(color="white", width=2)),
                hovertemplate=f"{label}: {value_str}<extra></extra>",
                showlegend=False,
            )
        )
        fig.add_annotation(
            x=_x(value),
            y=y + 0.32,
            text=f"{value_str} &nbsp; {pos_word}" if pos_word else value_str,
            showarrow=False,
            font=dict(size=11, color=dot_color),
        )
        fig.add_annotation(
            x=0.0, y=y, xanchor="right", xshift=-8, text=label, showarrow=False, font=dict(size=12, color="#aab8cc")
        )
        fig.add_annotation(
            x=1.0,
            y=y,
            xanchor="left",
            xshift=8,
            text=("faster →" if lower_is_better else "better →"),
            showarrow=False,
            font=dict(size=9, color="#8a93a0"),
        )
    fig.update_layout(
        **{**_LAYOUT, "margin": dict(l=170, r=90, t=52, b=24)},
        height=96 + 74 * n,
        title=dict(
            text="each grey ● = a cohort keeper · <span style='color:#2c7bb6'>● better</span> / "
            "<span style='color:#fdae61'>● worse</span> than median · tick = cohort median",
            font=dict(size=11, color="#8b9bb0"),
        ),
        xaxis=dict(visible=False, range=[-0.05, 1.05]),
        yaxis=dict(visible=False, range=[-0.7, n - 0.3]),
    )
    return fig


def build_line_height_figure(value_m: float | None, cohort: list[float]) -> go.Figure | None:
    """Descriptive defensive-line-height: his side's average back-line distance from own goal (m) on a
    real-metre axis, with the cohort drawn as a STRIP (each keeper a faint dot — shows the actual
    spread, incl. any bimodality, that an IQR box would hide) + median tick. Purely descriptive —
    line height is a style, not a quality — so NO good/bad colour and NO deep/high bucket."""
    if value_m is None:
        return None
    y = 0.5
    pts = [value_m, *cohort]
    lo, hi = min(pts), max(pts)
    span = (hi - lo) or 1.0
    lo, hi = lo - 0.18 * span, hi + 0.18 * span
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=[lo, hi], y=[y, y], mode="lines", line=dict(color="#2a3a4f", width=2), hoverinfo="skip", showlegend=False
        )
    )
    pos_txt = ""
    if len(cohort) >= _MIN_COHORT:
        med = statistics.median(cohort)
        fig.add_trace(  # cohort strip — one faint dot per keeper
            go.Scatter(
                x=list(cohort),
                y=[_jit(y, j, 0.34) for j in range(len(cohort))],
                mode="markers",
                marker=dict(size=7, color="rgba(150,165,185,0.45)"),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        fig.add_shape(type="line", x0=med, x1=med, y0=y - 0.3, y1=y + 0.3, line=dict(color=_MED, width=2))
        fig.add_annotation(
            x=med, y=y - 0.36, text=f"cohort median {med:.0f} m", showarrow=False, font=dict(size=9, color="#8a93a0")
        )
        pos_txt = " · above cohort" if value_m > med else " · below cohort"
    fig.add_trace(
        go.Scatter(
            x=[value_m],
            y=[y],
            mode="markers",
            marker=dict(size=17, color=_DOT, line=dict(color="white", width=2)),
            hovertemplate=f"{value_m:.0f} m from own goal<extra></extra>",
            showlegend=False,
        )
    )
    fig.add_annotation(
        x=value_m, y=y + 0.36, text=f"{value_m:.0f} m{pos_txt}", showarrow=False, font=dict(size=12, color=_DOT)
    )
    fig.update_layout(
        **{**_LAYOUT, "margin": dict(l=40, r=40, t=48, b=40)},
        height=200,
        title="Defensive line height vs cohort — each ● = a keeper (descriptive, not a deep/high label)",
        xaxis=dict(
            title="avg distance from own goal (m) · ← deeper · higher line →",
            range=[lo, hi],
            gridcolor=_GRID,
            zeroline=False,
        ),
        yaxis=dict(visible=False, range=[0, 1]),
    )
    return fig
