# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = ["plotly>=6.0", "kaleido>=1.0", "numpy>=1.26", "pandas>=2.0", "pyarrow>=14"]
# # NOTE: kaleido 0.2.1 write_image HANGS on this machine (mathjax workaround ineffective);
# # kaleido>=1 drives a real Chrome via choreographer and renders in ~2 s/figure.
# ///
"""GK Analytics redesign — spec mockup generator (synthetic data).

Renders 8 PNG mockups for the Goalkeeper Analytics page spec into ./mockups/.
All data is SYNTHETIC (WC2022-flavored names) — these are design artifacts,
not analytical outputs. Theme matches the Taipy app (render.py constants).

Run:  uv run docs/ui-cycles/gk-redesign/generate_mockups.py
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import plotly.graph_objects as go

OUT = Path(__file__).parent / "mockups"
OUT.mkdir(parents=True, exist_ok=True)

# Taipy app theme (hf_taipy_app/src/render.py + state/goalkeeper.py palette)
BG = "#1a1a2e"
PAPER = "#11111e"
LINE = "#e0e0e0"
BLUE = "#3b82f6"
AMBER = "#f59e0b"
RED = "#ef4444"
TEAL = "#2a9d8f"
PURPLE = "#a78bfa"
GREY = "rgba(160,160,180,0.45)"

GKS = ["Bounou", "E. Martinez", "Livakovic", "Lloris", "Szczesny", "Pickford", "U. Simon", "D. Costa"]

LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor=PAPER,
    plot_bgcolor=BG,
    font=dict(color="white", size=14),
    title_font=dict(size=17, color="white"),
    margin=dict(l=70, r=40, t=90, b=60),
)


def L(**overrides) -> dict:
    """LAYOUT merged with per-figure overrides (avoids duplicate-kwarg on margin etc.)."""
    d = dict(LAYOUT)
    d.update(overrides)
    return d


def save(fig: go.Figure, name: str, w: int = 1100, h: int = 620) -> None:
    fig.write_image(str(OUT / name), width=w, height=h, scale=2)
    print("wrote", name)


# ---------------------------------------------------------------- 1. radar
def radar() -> None:
    axes = ["xT-GK /90", "Completion vs Expected", "Positioning (Ghost dev.)",
            "Box Command (closing)", "Sweeping", "Pressure Resilience"]
    bounou = [85, 78, 92, 70, 58, 88]
    league = [50] * 6
    fig = go.Figure()
    for ring, dashc in [(95, "rgba(42,157,143,0.55)"), (5, "rgba(230,57,70,0.45)")]:
        fig.add_trace(go.Scatterpolar(
            r=[ring] * 7, theta=axes + axes[:1], mode="lines",
            line=dict(color=dashc, dash="dot", width=1.5),
            name=f"{'Top' if ring == 95 else 'Bottom'} 5% (league)", hoverinfo="skip"))
    fig.add_trace(go.Scatterpolar(r=league + league[:1], theta=axes + axes[:1],
                                  mode="lines", name="League median",
                                  line=dict(color="rgba(200,200,220,0.6)", width=1.5)))
    fig.add_trace(go.Scatterpolar(r=bounou + bounou[:1], theta=axes + axes[:1],
                                  mode="lines+markers", name="Bounou (Morocco)",
                                  fill="toself", fillcolor="rgba(245,158,11,0.18)",
                                  line=dict(color=AMBER, width=3)))
    fig.update_layout(**L(
        title="Goalkeeper profile — Bounou vs WC2022 tracking corpus (percentiles)<br>"
              "<sup>All axes are league percentiles (0-100, higher = better). "
              "Rings mark the top/bottom 5% of the 32 tournament GKs.</sup>",
        polar=dict(bgcolor=BG, radialaxis=dict(range=[0, 100], showticklabels=True,
                   tickfont=dict(size=10), gridcolor="rgba(255,255,255,0.12)"),
                   angularaxis=dict(gridcolor="rgba(255,255,255,0.12)")),
        legend=dict(orientation="h", y=-0.08),
        margin=dict(l=110, r=110, t=90, b=60),
    ))
    save(fig, "01_overview_radar.png", h=680)


# ------------------------------------------- 2. philosophy switcher (bump)
PRESETS = ["Default", "Possession", "Counter", "Direct", "High Press", "Low Block"]
# Column-wise rank PERMUTATIONS (each preset column has unique ranks 1-8 — a
# bump chart's invariant; a prior draft drew duplicate ranks per column).
_ORDER_BY_PRESET = [
    ["E. Martinez", "Livakovic", "Bounou", "Lloris", "Szczesny", "Pickford", "U. Simon", "D. Costa"],
    ["Lloris", "Pickford", "Livakovic", "E. Martinez", "U. Simon", "Bounou", "D. Costa", "Szczesny"],
    ["Bounou", "E. Martinez", "Szczesny", "Livakovic", "Lloris", "D. Costa", "Pickford", "U. Simon"],
    ["E. Martinez", "Bounou", "Szczesny", "D. Costa", "Livakovic", "Lloris", "U. Simon", "Pickford"],
    ["Livakovic", "Bounou", "E. Martinez", "Szczesny", "Lloris", "Pickford", "U. Simon", "D. Costa"],
    ["E. Martinez", "Livakovic", "D. Costa", "Bounou", "Szczesny", "Lloris", "Pickford", "U. Simon"],
]
RANKS = {gk: [order.index(gk) + 1 for order in _ORDER_BY_PRESET] for gk in GKS}


def philosophy_bump() -> None:
    presets = PRESETS
    ranks = RANKS
    fig = go.Figure()
    for gk in GKS:
        hot = gk == "Bounou"
        fig.add_trace(go.Scatter(
            x=presets, y=ranks[gk], mode="lines+markers+text",
            text=[gk if i == 0 else "" for i in range(6)],
            textposition="middle left", textfont=dict(size=11),
            line=dict(color=AMBER if hot else GREY, width=4 if hot else 1.5),
            marker=dict(size=10 if hot else 6), name=gk, showlegend=False))
    fig.add_annotation(x="Possession", y=6, text="Bounou drops to #6 under a<br>possession game model —"
                       "<br>his value is transition-built", showarrow=True, arrowhead=2,
                       ax=40, ay=60, font=dict(color=AMBER, size=12),
                       bgcolor="rgba(26,26,46,0.85)")
    fig.update_layout(**L(
        title="Which goalkeepers' distribution value survives a change of game model?<br>"
              "<sup>xT-GK rank under each stored philosophy preset (rank 1 = most value /90).<br>"
              "Switching is instant — all presets are precomputed columns.</sup>",
        yaxis=dict(title="xT-GK /90 rank", autorange="reversed", dtick=1,
                   gridcolor="rgba(255,255,255,0.08)"),
        xaxis=dict(title="Philosophy preset", gridcolor="rgba(255,255,255,0.08)"),
        margin=dict(l=140, r=40, t=110, b=60),
    ))
    save(fig, "02_xtgk_philosophy_switcher.png")


# --------------------------------------------- 3. component decomposition
def components() -> None:
    rng = np.random.default_rng(3)
    base = rng.uniform(0.05, 0.10, 8)
    pev = rng.uniform(0.02, 0.16, 8)
    rav = rng.uniform(-0.06, 0.08, 8)
    dzv = rng.uniform(0.02, 0.09, 8)
    pres = rng.uniform(-0.03, 0.03, 8)
    order = np.argsort(base + pev + rav + dzv + pres)
    fig = go.Figure()
    parts = [("Base xT", base, "rgba(120,140,200,0.85)"),
             ("PEV — pressure escape", pev, TEAL),
             ("RAV — risk-adjusted", rav, AMBER),
             ("DZV — defensive-zone", dzv, BLUE),
             ("Pressure adj.", pres, PURPLE)]
    names = [GKS[i] for i in order]
    for label, vals, color in parts:
        fig.add_trace(go.Bar(y=names, x=vals[order], name=label, orientation="h",
                             marker_color=color))
    fig.update_layout(**L(
        barmode="relative",
        title="What KIND of distribution value does each goalkeeper create?<br>"
              "<sup>xT-GK /90 decomposed into its stored Eyestone components; diverging at zero — "
              "value-destroying components read left.<br>Selecting a component re-sorts by it "
              "(per-component drill-down = plain sorted bars).</sup>",
        xaxis=dict(title="xT-GK /90 contribution (sum = composite, higher = better)",
                   gridcolor="rgba(255,255,255,0.08)", zerolinecolor="rgba(255,255,255,0.35)"),
        legend=dict(orientation="h", y=-0.15),
        margin=dict(l=110, r=40, t=110, b=60),
    ))
    save(fig, "03_xtgk_components.png")


# -------------------------------------------------- 4. pressure split
def pressure_split() -> None:
    """Connected dot plot (Kirk: when the question is the GAP vs a reference, the
    delta — not the absolute bar lengths — should be the primary visual signal;
    chart-choice-audit upgrade from the reflexive grouped bar)."""
    cats = ["Low pressure", "Medium pressure", "High pressure"]
    bounou = [0.024, 0.041, 0.066]
    league = [0.021, 0.028, 0.035]
    fig = go.Figure()
    for c, lg, bn in zip(cats, league, bounou):
        fig.add_trace(go.Scatter(x=[lg, bn], y=[c, c], mode="lines", showlegend=False,
                                 line=dict(color="rgba(245,158,11,0.55)", width=4)))
        fig.add_annotation(x=(lg + bn) / 2, y=c, yshift=18, showarrow=False,
                           text=f"+{(bn - lg) / lg:.0%}", font=dict(color=AMBER, size=12))
    fig.add_trace(go.Scatter(x=league, y=cats, mode="markers", name="League average",
                             marker=dict(size=13, color=GREY, line=dict(color="white", width=1))))
    fig.add_trace(go.Scatter(x=bounou, y=cats, mode="markers", name="Bounou",
                             marker=dict(size=15, color=AMBER, line=dict(color="white", width=1.5))))
    fig.add_annotation(x=0.066, y="High pressure", text="The gap WIDENS as pressure rises —<br>"
                       "press him at your peril", showarrow=True, arrowhead=2, ax=-10, ay=-55,
                       font=dict(color=AMBER, size=12), bgcolor="rgba(26,26,46,0.85)")
    fig.update_layout(
        **LAYOUT,
        title="Same pass, different value — how far above the league is he under pressure?<br>"
              "<sup>Mean xT-GK per distribution by tracked pressure on the GK at release (Andrienko terciles).<br>"
              "Amber connector = Bounou's gap over the league average; right = better.</sup>",
        xaxis=dict(title="xT-GK per distribution (higher = better)", range=[0.015, 0.075],
                   gridcolor="rgba(255,255,255,0.08)"),
        yaxis=dict(categoryorder="array", categoryarray=cats[::-1]),
        legend=dict(orientation="h", y=-0.12),
    )
    save(fig, "04_pressure_split.png", h=560)


# ---------------------------------------------------------- pitch helpers
def _pitch_shapes_defensive_third() -> list[dict]:
    """SPADL meters, defending goal at x=0, showing x in [0, 36]."""
    c = "rgba(224,224,224,0.45)"
    return [
        dict(type="rect", x0=0, y0=0, x1=36, y1=68, line=dict(color=c, width=1)),
        dict(type="rect", x0=0, y0=13.84, x1=16.5, y1=54.16, line=dict(color=c, width=1)),
        dict(type="rect", x0=0, y0=24.84, x1=5.5, y1=43.16, line=dict(color=c, width=1)),
        dict(type="line", x0=0, y0=30.34, x1=0, y1=37.66, line=dict(color=LINE, width=5)),
        dict(type="circle", x0=11 - 0.3, y0=34 - 0.3, x1=11 + 0.3, y1=34 + 0.3,
             line=dict(color=c), fillcolor=c),
    ]


# ------------------------------------------------------ 5. ghost-GK tether
def ghost_tether() -> None:
    gx, gy = np.meshgrid(np.linspace(0, 36, 90), np.linspace(0, 68, 90))
    ox, oy = 10.4, 31.2  # ghost (model optimum)
    ax_, ay_ = 7.2, 36.5  # actual Bounou
    dens = np.exp(-(((gx - ox) / 4.2) ** 2 + ((gy - oy) / 5.0) ** 2))
    fig = go.Figure()
    fig.add_trace(go.Contour(
        x=np.linspace(0, 36, 90), y=np.linspace(0, 68, 90), z=dens,
        colorscale=[[0, "rgba(26,26,46,0)"], [0.35, "rgba(59,130,246,0.18)"],
                    [0.7, "rgba(42,157,143,0.45)"], [1, "rgba(245,158,11,0.85)"]],
        contours=dict(coloring="fill", showlines=False), showscale=False, hoverinfo="skip"))
    rng = np.random.default_rng(5)
    px = rng.uniform(4, 30, 9)
    py = rng.uniform(8, 62, 9)
    fig.add_trace(go.Scatter(x=px, y=py, mode="markers", name="Players (context)",
                             marker=dict(size=9, color=GREY)))
    fig.add_trace(go.Scatter(x=[26.0], y=[52.0], mode="markers", name="Ball (cross origin)",
                             marker=dict(size=11, color="white", symbol="circle-open",
                                         line=dict(width=2))))
    fig.add_trace(go.Scatter(x=[ox, ax_], y=[oy, ay_], mode="lines", name="Deviation tether",
                             line=dict(color="white", dash="dash", width=2.5)))
    fig.add_trace(go.Scatter(x=[ox], y=[oy], mode="markers", name="Ghost GK (model optimum)",
                             marker=dict(size=30, color="rgba(245,158,11,0.30)",
                                         line=dict(color=AMBER, width=2.5), symbol="circle")))
    fig.add_trace(go.Scatter(x=[ax_], y=[ay_], mode="markers", name="Bounou (actual)",
                             marker=dict(size=17, color=BLUE, line=dict(color="white", width=2))))
    dev = float(np.hypot(ox - ax_, oy - ay_))
    fig.add_annotation(x=(ox + ax_) / 2 + 6.5, y=(oy + ay_) / 2,
                       text=f"{dev:.1f} m off optimum<br>(spread ±2.1 m)",
                       font=dict(color="white", size=13), align="left",
                       bgcolor="rgba(26,26,46,0.9)", showarrow=True, arrowhead=0,
                       ax=46, ay=0)
    fig.update_layout(
        **LAYOUT,
        title="How far did Bounou deviate from the league-average optimum on this cross?<br>"
              "<sup>Heatmap = ghost-GK positional density (RFCDE model, brighter = more probable);<br>"
              "ring = model optimum; blue dot = actual position at the linked frame.</sup>",
        shapes=_pitch_shapes_defensive_third(),
        xaxis=dict(range=[-1.5, 27], visible=False, scaleanchor="y"),
        yaxis=dict(range=[14, 60], visible=False),
        legend=dict(orientation="h", y=-0.04),
    )
    save(fig, "05_ghost_tether.png", h=720)


# ---------------------------------------------------- 6. pre-shot cone
def preshot_cone() -> None:
    bx, by = 88.0, 27.0
    p1, p2 = (105.0, 30.34), (105.0, 37.66)
    kx, ky = 102.3, 33.2
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[bx, p1[0], p2[0], bx], y=[by, p1[1], p2[1], by],
                             fill="toself", fillcolor="rgba(239,68,68,0.16)",
                             line=dict(color="rgba(239,68,68,0.6)", width=1.5),
                             name="Shot cone (ball to posts)", mode="lines"))
    rng = np.random.default_rng(11)
    fig.add_trace(go.Scatter(x=rng.uniform(80, 103, 8), y=rng.uniform(14, 54, 8),
                             mode="markers", name="Players (context)",
                             marker=dict(size=9, color=GREY)))
    fig.add_trace(go.Scatter(x=[bx], y=[by], mode="markers", name="Ball (shot origin)",
                             marker=dict(size=12, color="white")))
    cx, cy = (p1[1] + p2[1]) / 2, None
    fig.add_trace(go.Scatter(x=[bx, 105], y=[by, 34.0], mode="lines", name="Shot line (to centre)",
                             line=dict(color="rgba(255,255,255,0.5)", dash="dot", width=1.5)))
    fig.add_trace(go.Scatter(x=[kx], y=[ky], mode="markers", name="Bounou (pre-shot frame)",
                             marker=dict(size=14, color=BLUE, line=dict(color="white", width=1.5))))
    fig.add_trace(go.Scatter(x=[kx, kx + 2.0], y=[ky, ky - 1.4], mode="lines",
                             name="Angle to trajectory", line=dict(color=AMBER, width=3)))
    for x0, y0, y1 in [(105, 30.34, 37.66)]:
        fig.add_shape(type="line", x0=x0, y0=y0, x1=x0, y1=y1, line=dict(color=LINE, width=5))
    fig.add_shape(type="rect", x0=88.5, y0=13.84, x1=105, y1=54.16,
                  line=dict(color="rgba(224,224,224,0.4)", width=1))
    fig.add_annotation(x=kx, y=ky + 3.2, text="2.9 m off line - 4.8 deg off shot line<br>"
                       "bisector error 0.6 m (96th pct.)", font=dict(size=12),
                       bgcolor="rgba(26,26,46,0.85)", showarrow=False)
    fig.update_layout(
        **LAYOUT,
        title="Pre-shot geometry — was the save makeable from where he stood?<br>"
              "<sup>One linked tracking frame before the shot. Cone = shooter's visible goal; "
              "amber vector = GK angle to the shot trajectory (stored AC columns).</sup>",
        xaxis=dict(range=[78, 107], visible=False, scaleanchor="y"),
        yaxis=dict(range=[10, 58], visible=False),
        legend=dict(orientation="h", y=-0.04),
    )
    save(fig, "06_preshot_cone.png", h=700)


# ------------------------------------------------ 7. distribution map
def distribution_map() -> None:
    rng = np.random.default_rng(23)
    n = 26
    sx = rng.uniform(2, 12, n)
    sy = rng.uniform(22, 46, n)
    ex = np.clip(sx + rng.gamma(3.5, 7, n), 5, 70)
    ey = np.clip(sy + rng.normal(0, 16, n), 2, 66)
    val = np.clip((ex - sx) / 70 * 0.08 + rng.normal(0, 0.02, n), -0.04, 0.12)
    fig = go.Figure()
    cmin, cmax = -0.04, 0.12
    for i in range(n):
        frac = (val[i] - cmin) / (cmax - cmin)
        color = (f"rgba({int(120 + 135 * frac)},{int(130 + 28 * frac)},"
                 f"{int(200 - 150 * frac)},0.8)")
        fig.add_trace(go.Scatter(x=[sx[i], ex[i]], y=[sy[i], ey[i]], mode="lines",
                                 line=dict(color=color, width=1.2 + 3.5 * frac),
                                 showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=ex, y=ey, mode="markers", showlegend=False,
                             marker=dict(size=7, color=val, colorscale=[
                                 [0, "rgba(120,130,200,0.9)"], [1, "rgba(255,158,50,0.95)"]],
                                 cmin=cmin, cmax=cmax,
                                 colorbar=dict(title=dict(text="xT-GK", side="right"),
                                               thickness=14, len=0.7))))
    fig.add_shape(type="rect", x0=0, y0=0, x1=72, y1=68, line=dict(color="rgba(224,224,224,0.4)"))
    fig.add_shape(type="rect", x0=0, y0=13.84, x1=16.5, y1=54.16,
                  line=dict(color="rgba(224,224,224,0.4)"))
    fig.add_shape(type="line", x0=52.5, y0=0, x1=52.5, y1=68,
                  line=dict(color="rgba(224,224,224,0.4)", dash="dot"))
    fig.add_shape(type="line", x0=0, y0=30.34, x1=0, y1=37.66, line=dict(color=LINE, width=5))
    fig.update_layout(
        **LAYOUT,
        title="Where does Bounou's distribution actually create value?<br>"
              "<sup>Every distribution, colored by its xT-GK (selected philosophy preset).<br>"
              "Long diagonals right are his value engine; short left passes are safe but sterile.</sup>",
        xaxis=dict(range=[-3, 75], visible=False, scaleanchor="y"),
        yaxis=dict(range=[-3, 71], visible=False),
    )
    save(fig, "07_distribution_map.png", h=700)


# ------------------------------------------------ 8. risk-reward quadrant
def risk_reward() -> None:
    rng = np.random.default_rng(17)
    risk = rng.uniform(0.62, 0.92, 8)   # mean gk_completion of attempted passes
    reward = rng.uniform(0.05, 0.45, 8)  # xT-GK /90
    risk[0], reward[0] = 0.71, 0.41      # Bounou
    fig = go.Figure()
    mx, my = float(np.median(risk)), float(np.median(reward))
    fig.add_shape(type="line", x0=mx, y0=0, x1=mx, y1=0.5,
                  line=dict(color="rgba(255,255,255,0.25)", dash="dot"))
    fig.add_shape(type="line", x0=0.55, y0=my, x1=0.98, y1=my,
                  line=dict(color="rgba(255,255,255,0.25)", dash="dot"))
    for label, x, y in [("Risky + rewarded", 0.585, 0.47), ("Safe + valuable", 0.955, 0.47),
                        ("Risky + wasteful", 0.585, 0.02), ("Safe but sterile", 0.955, 0.02)]:
        fig.add_annotation(x=x, y=y, text=label, showarrow=False,
                           font=dict(color="rgba(255,255,255,0.45)", size=12))
    colors = [AMBER] + [GREY] * 7
    sizes = [16] + [10] * 7
    fig.add_trace(go.Scatter(x=risk, y=reward, mode="markers+text", text=GKS,
                             textposition="top center", textfont=dict(size=11),
                             marker=dict(size=sizes, color=colors,
                                         line=dict(color="white", width=1)),
                             showlegend=False))
    fig.update_layout(
        **LAYOUT,
        title="Risk appetite vs reward — who earns their gambles?<br>"
              "<sup>x: mean completion probability of ATTEMPTED distributions (left = riskier selection, from gk_completion).<br>"
              "y: xT-GK /90 (higher = better). Dotted lines = league medians.</sup>",
        xaxis=dict(title="Mean P(completion) of attempted distributions", range=[0.55, 0.98],
                   gridcolor="rgba(255,255,255,0.08)"),
        yaxis=dict(title="xT-GK /90", range=[0, 0.5], gridcolor="rgba(255,255,255,0.08)"),
    )
    save(fig, "08_risk_reward_quadrant.png")


# ===================================================================== V2
# Second revision (2026-06-11, owner feedback): the page is 2-3 TABS, radar dropped
# (too common). Each PNG below mocks a FULL TAB. Defensive tab folds ghost-GK
# together with the other advanced defensive metrics (closing times, reachable
# area, pitch-control share) and contextual splits (game_state, line height).


def _half_pitch_shapes(xref: str, yref: str) -> list[dict]:
    c = "rgba(224,224,224,0.4)"
    return [
        dict(type="rect", x0=0, y0=0, x1=72, y1=68, line=dict(color=c, width=1), xref=xref, yref=yref),
        dict(type="rect", x0=0, y0=13.84, x1=16.5, y1=54.16, line=dict(color=c, width=1), xref=xref, yref=yref),
        dict(type="line", x0=52.5, y0=0, x1=52.5, y1=68, line=dict(color=c, dash="dot", width=1), xref=xref, yref=yref),
        dict(type="line", x0=0, y0=30.34, x1=0, y1=37.66, line=dict(color=LINE, width=5), xref=xref, yref=yref),
    ]


def _selector_chips(fig: go.Figure, selected: str, y: float = 1.075) -> None:
    """Fake philosophy-selector chip strip (paper refs, drawn in the top margin)."""
    x = 0.0
    for p in PRESETS:
        w = 0.022 + 0.0125 * len(p)
        hot = p == selected
        fig.add_shape(type="rect", xref="paper", yref="paper",
                      x0=x, x1=x + w, y0=y - 0.026, y1=y + 0.026,
                      line=dict(color=AMBER if hot else "rgba(255,255,255,0.35)",
                                width=2 if hot else 1),
                      fillcolor="rgba(245,158,11,0.18)" if hot else "rgba(255,255,255,0.04)")
        fig.add_annotation(xref="paper", yref="paper", x=x + w / 2, y=y,
                           text=f"<b>{p}</b>" if hot else p, showarrow=False,
                           font=dict(size=12, color=AMBER if hot else "rgba(255,255,255,0.75)"))
        x += w + 0.011
    fig.add_annotation(xref="paper", yref="paper", x=min(x + 0.012, 0.99), y=y, xanchor="left",
                       text="← switch is instant (all presets precomputed)", showarrow=False,
                       font=dict(size=11, color="rgba(255,255,255,0.5)"))


def _style_subplot_titles(fig: go.Figure) -> None:
    for a in fig.layout.annotations:
        a.font = dict(size=13, color="rgba(255,255,255,0.85)")


def tab1_xtgk() -> None:
    """TAB 1 — Distribution Value (xT-GK): the philosophy switcher is the hero."""
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2, cols=2, specs=[[{"colspan": 2}, None], [{}, {}]],
        row_heights=[0.40, 0.60], vertical_spacing=0.13, horizontal_spacing=0.05,
        subplot_titles=(
            "xT-GK /90 rank under every preset (amber = Bounou; band = selected preset)",
            "Bounou's distributions valued under COUNTER — xT-GK/90 0.41, rank 1",
            "The SAME passes valued under POSSESSION — xT-GK/90 0.19, rank 6",
        ),
    )
    _style_subplot_titles(fig)

    # Row 1: bump chart across presets
    for gk in GKS:
        hot = gk == "Bounou"
        fig.add_trace(go.Scatter(
            x=PRESETS, y=RANKS[gk], mode="lines+markers+text",
            text=[gk if i == 0 else "" for i in range(6)],
            textposition="middle left", textfont=dict(size=10),
            line=dict(color=AMBER if hot else GREY, width=4 if hot else 1.2),
            marker=dict(size=9 if hot else 5), showlegend=False), row=1, col=1)
    fig.add_shape(type="rect", x0=1.6, x1=2.4, y0=0.5, y1=8.5, xref="x", yref="y",
                  fillcolor="rgba(245,158,11,0.10)",
                  line=dict(color="rgba(245,158,11,0.45)", width=1), layer="below")
    fig.update_yaxes(autorange="reversed", dtick=1, title_text="rank", row=1, col=1,
                     gridcolor="rgba(255,255,255,0.07)")
    fig.update_xaxes(row=1, col=1, gridcolor="rgba(255,255,255,0.07)")

    # Row 2: SAME pass geometry, re-valued under two presets
    rng = np.random.default_rng(23)
    n = 26
    sx = rng.uniform(2, 12, n)
    sy = rng.uniform(22, 46, n)
    ex = np.clip(sx + rng.gamma(3.5, 7, n), 5, 70)
    ey = np.clip(sy + rng.normal(0, 16, n), 2, 66)
    dist = np.hypot(ex - sx, ey - sy)
    panels = [
        (1, "x2", "y2", np.clip(dist / 70 * 0.12 - 0.01 + rng.normal(0, 0.012, n), -0.04, 0.12)),
        (2, "x3", "y3", np.clip(0.055 - dist / 70 * 0.07 + rng.normal(0, 0.010, n), -0.04, 0.12)),
    ]
    for cc, xref, yref, v in panels:
        for s in _half_pitch_shapes(xref, yref):
            fig.add_shape(**s)
        for i in range(n):
            frac = float(np.clip((v[i] + 0.04) / 0.16, 0, 1))
            color = (f"rgba({int(120 + 135 * frac)},{int(130 + 28 * frac)},"
                     f"{int(200 - 150 * frac)},0.85)")
            fig.add_trace(go.Scatter(x=[sx[i], ex[i]], y=[sy[i], ey[i]], mode="lines",
                                     line=dict(color=color, width=1.0 + 3.2 * frac),
                                     showlegend=False, hoverinfo="skip"), row=2, col=cc)
        fig.update_xaxes(visible=False, range=[-3, 75], row=2, col=cc, scaleanchor=yref)
        fig.update_yaxes(visible=False, range=[-3, 71], row=2, col=cc)

    _selector_chips(fig, selected="Counter")
    fig.update_layout(**L(
        title=dict(text="TAB 1 · Distribution Value — what is his passing worth under YOUR game model?",
                   y=0.985),
        margin=dict(l=90, r=40, t=150, b=70),
    ))
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.05, showarrow=False,
                       text="Arrow color: blue/grey = low or negative xT-GK, amber = high "
                            "(scale −0.04…0.12 per pass). Same 26 passes in both panels — only the "
                            "philosophy parameters (δ, γ, φ, η) change.",
                       font=dict(size=12, color="rgba(255,255,255,0.6)"))
    save(fig, "v2_tab1_xtgk_value.png", w=1400, h=1000)


def tab2_ghost_defense() -> None:
    """TAB 2 — Defensive Positioning & Box Command: ghost-GK + closing times +
    contextual splits (game_state stored per action; defensive_line_x terciles)."""
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2, cols=2, specs=[[{"rowspan": 2}, {}], [None, {}]],
        column_widths=[0.52, 0.48], vertical_spacing=0.17, horizontal_spacing=0.10,
        subplot_titles=(
            "Scene: cross vs Croatia, 78' — actual vs ghost optimum",
            "WHEN does he leave the model line? (mean deviation, m)",
            "Command of the box — minimum closing time (s, lower = better)",
        ),
    )
    _style_subplot_titles(fig)

    # ---- left: ghost tether scene ----
    gx, gy = np.meshgrid(np.linspace(0, 36, 90), np.linspace(0, 68, 90))
    ox, oy = 10.4, 31.2
    ax_, ay_ = 7.2, 36.5
    dens = np.exp(-(((gx - ox) / 4.2) ** 2 + ((gy - oy) / 5.0) ** 2))
    fig.add_trace(go.Contour(
        x=np.linspace(0, 36, 90), y=np.linspace(0, 68, 90), z=dens,
        colorscale=[[0, "rgba(26,26,46,0)"], [0.35, "rgba(59,130,246,0.18)"],
                    [0.7, "rgba(42,157,143,0.45)"], [1, "rgba(245,158,11,0.85)"]],
        contours=dict(coloring="fill", showlines=False), showscale=False,
        hoverinfo="skip"), row=1, col=1)
    rng = np.random.default_rng(5)
    fig.add_trace(go.Scatter(x=rng.uniform(4, 30, 9), y=rng.uniform(8, 62, 9), mode="markers",
                             name="Players (context)", marker=dict(size=9, color=GREY)),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=[26.0], y=[52.0], mode="markers", name="Ball (cross origin)",
                             marker=dict(size=11, color="white", symbol="circle-open",
                                         line=dict(width=2))), row=1, col=1)
    fig.add_trace(go.Scatter(x=[ox, ax_], y=[oy, ay_], mode="lines", name="Deviation tether",
                             line=dict(color="white", dash="dash", width=2.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=[ox], y=[oy], mode="markers", name="Ghost GK (model optimum)",
                             marker=dict(size=30, color="rgba(245,158,11,0.30)",
                                         line=dict(color=AMBER, width=2.5))), row=1, col=1)
    fig.add_trace(go.Scatter(x=[ax_], y=[ay_], mode="markers", name="Bounou (actual)",
                             marker=dict(size=17, color=BLUE, line=dict(color="white", width=2))),
                  row=1, col=1)
    dev = float(np.hypot(ox - ax_, oy - ay_))
    fig.add_annotation(x=(ox + ax_) / 2 + 6.5, y=(oy + ay_) / 2, xref="x", yref="y",
                       text=f"{dev:.1f} m off optimum<br>(spread ±2.1 m)",
                       font=dict(color="white", size=13), align="left",
                       bgcolor="rgba(26,26,46,0.9)", showarrow=True, arrowhead=0, ax=46, ay=0)
    for s in _pitch_shapes_defensive_third():
        s = dict(s)
        s["xref"], s["yref"] = "x", "y"
        fig.add_shape(**s)
    fig.update_xaxes(visible=False, range=[-1.5, 27], scaleanchor="y", row=1, col=1)
    fig.update_yaxes(visible=False, range=[14, 60], row=1, col=1)

    # ---- right top: contextual splits (game state + line height) ----
    cats = ["Trailing", "Level", "Leading", " ", "Deep block", "Mid block", "High line"]
    league = [3.2, 3.0, 3.3, None, 2.5, 3.1, 4.2]
    bounou = [2.7, 2.5, 4.9, None, 2.3, 2.8, 6.1]
    for c, lg, bn in zip(cats, league, bounou):
        if lg is None:
            continue
        fig.add_trace(go.Scatter(x=[lg, bn], y=[c, c], mode="lines", showlegend=False,
                                 line=dict(color="rgba(245,158,11,0.5)", width=4)), row=1, col=2)
    fig.add_trace(go.Scatter(x=[v for v in league if v is not None],
                             y=[c for c, v in zip(cats, league) if v is not None],
                             mode="markers", name="League average",
                             marker=dict(size=11, color=GREY, line=dict(color="white", width=1))),
                  row=1, col=2)
    fig.add_trace(go.Scatter(x=[v for v in bounou if v is not None],
                             y=[c for c, v in zip(cats, bounou) if v is not None],
                             mode="markers", name="Bounou",
                             marker=dict(size=13, color=AMBER, line=dict(color="white", width=1.5))),
                  row=1, col=2)
    fig.add_annotation(x=6.1, y="High line", xref="x2", yref="y2", ax=-30, ay=-32,
                       text="deviation concentrates behind a HIGH line<br>and when LEADING — "
                            "sweeping duty, not indiscipline?", showarrow=True, arrowhead=2,
                       font=dict(color=AMBER, size=11), bgcolor="rgba(26,26,46,0.9)")
    fig.update_xaxes(range=[1.5, 8.5], title_text="mean deviation from ghost optimum (m)",
                     gridcolor="rgba(255,255,255,0.07)", row=1, col=2)
    fig.update_yaxes(categoryorder="array", categoryarray=cats[::-1], row=1, col=2)

    # ---- right bottom: box command (closing times + reach) ----
    zones = ["Six-yard box", "Near post", "Far post"]
    lg_t = [0.92, 1.05, 1.42]
    bn_t = [0.84, 0.96, 1.18]
    for z, a, b in zip(zones, lg_t, bn_t):
        fig.add_trace(go.Scatter(x=[a, b], y=[z, z], mode="lines", showlegend=False,
                                 line=dict(color="rgba(59,130,246,0.55)", width=4)), row=2, col=2)
    fig.add_trace(go.Scatter(x=lg_t, y=zones, mode="markers", showlegend=False,
                             marker=dict(size=11, color=GREY, line=dict(color="white", width=1))),
                  row=2, col=2)
    fig.add_trace(go.Scatter(x=bn_t, y=zones, mode="markers", showlegend=False,
                             marker=dict(size=13, color=BLUE, line=dict(color="white", width=1.5))),
                  row=2, col=2)
    fig.add_annotation(xref="x3", yref="paper", x=1.30, y=0.075, showarrow=False, align="left",
                       text="reachable area 142 m² (88th pct.)<br>box pitch-control share 64% (81st pct.)",
                       font=dict(size=11, color="rgba(255,255,255,0.7)"),
                       bgcolor="rgba(26,26,46,0.9)")
    fig.update_xaxes(range=[0.7, 1.6], title_text="min closing time, s (blue = Bounou, faster than league in all zones)",
                     gridcolor="rgba(255,255,255,0.07)", row=2, col=2)
    fig.update_yaxes(categoryorder="array", categoryarray=zones[::-1], row=2, col=2)

    fig.update_layout(**L(
        title=dict(text="TAB 2 · Defensive Positioning & Box Command — is he standing where the "
                        "league's keepers would?", y=0.985),
        margin=dict(l=60, r=40, t=110, b=150),
        legend=dict(orientation="h", y=-0.055),
    ))
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.155, showarrow=False, align="center",
                       text="The ghost optimum is conditioned on the FULL tracking frame (both teams' positions), "
                            "so formation/strategy is largely priced into the baseline — the splits show WHEN "
                            "deviation appears.<br>game_state and defensive_line_x are stored per action; "
                            "formation windows (EFPI / shape-graph) join from fct_formation_labels.",
                       font=dict(size=11, color="rgba(255,255,255,0.55)"))
    save(fig, "v2_tab2_ghost_defense.png", w=1400, h=900)


def tab3_shot_geometry() -> None:
    """TAB 3 — Shot-Stopping Geometry: pre-shot scene + the positioning-vs-outcome map."""
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=1, cols=2, column_widths=[0.54, 0.46], horizontal_spacing=0.08,
        subplot_titles=(
            "Scene: pre-shot frame — the cone he must cover",
            "Every on-target shot faced — where was he standing?",
        ),
    )
    _style_subplot_titles(fig)

    # ---- left: cone scene ----
    bx, by = 88.0, 27.0
    kx, ky = 102.3, 33.2
    fig.add_trace(go.Scatter(x=[bx, 105.0, 105.0, bx], y=[by, 30.34, 37.66, by],
                             fill="toself", fillcolor="rgba(239,68,68,0.16)",
                             line=dict(color="rgba(239,68,68,0.6)", width=1.5),
                             name="Shot cone (ball to posts)", mode="lines"), row=1, col=1)
    rng = np.random.default_rng(11)
    fig.add_trace(go.Scatter(x=rng.uniform(80, 103, 8), y=rng.uniform(14, 54, 8), mode="markers",
                             name="Players (context)", marker=dict(size=9, color=GREY)), row=1, col=1)
    fig.add_trace(go.Scatter(x=[bx], y=[by], mode="markers", name="Ball (shot origin)",
                             marker=dict(size=12, color="white")), row=1, col=1)
    fig.add_trace(go.Scatter(x=[bx, 105], y=[by, 34.0], mode="lines", name="Shot line (to centre)",
                             line=dict(color="rgba(255,255,255,0.5)", dash="dot", width=1.5)),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=[kx], y=[ky], mode="markers", name="Bounou (pre-shot frame)",
                             marker=dict(size=14, color=BLUE, line=dict(color="white", width=1.5))),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=[kx, kx + 2.0], y=[ky, ky - 1.4], mode="lines",
                             name="Angle to trajectory", line=dict(color=AMBER, width=3)), row=1, col=1)
    fig.add_shape(type="line", x0=105, y0=30.34, x1=105, y1=37.66, xref="x", yref="y",
                  line=dict(color=LINE, width=5))
    fig.add_shape(type="rect", x0=88.5, y0=13.84, x1=105, y1=54.16, xref="x", yref="y",
                  line=dict(color="rgba(224,224,224,0.4)", width=1))
    fig.add_annotation(x=kx, y=ky + 3.4, xref="x", yref="y",
                       text="2.9 m off line · 4.8° off shot line<br>bisector error 0.6 m (96th pct.)",
                       font=dict(size=12), bgcolor="rgba(26,26,46,0.9)", showarrow=False)
    fig.update_xaxes(visible=False, range=[78, 107], scaleanchor="y", row=1, col=1)
    fig.update_yaxes(visible=False, range=[10, 58], row=1, col=1)

    # ---- right: positioning vs outcome ----
    rng = np.random.default_rng(31)
    n_s = 34
    sx_ = np.clip(np.abs(rng.normal(2.4, 1.0, n_s)), 0.3, 6.5)
    sy_ = np.clip(np.abs(rng.normal(0.55, 0.35, n_s)), 0.02, 2.8)
    n_g = 9
    gx_ = np.clip(np.abs(rng.normal(3.9, 1.2, n_g)) + 0.6, 0.5, 6.8)
    gy_ = np.clip(np.abs(rng.normal(1.1, 0.5, n_g)) + 0.15, 0.1, 2.9)
    fig.add_trace(go.Scatter(x=sx_, y=sy_, mode="markers", name="Saved",
                             marker=dict(size=9, color=BLUE, opacity=0.75)), row=1, col=2)
    fig.add_trace(go.Scatter(x=gx_, y=gy_, mode="markers", name="Goal conceded",
                             marker=dict(size=12, color=RED, symbol="star")), row=1, col=2)
    fig.add_shape(type="rect", x0=3.5, x1=7.0, y0=1.0, y1=3.0, xref="x2", yref="y2",
                  fillcolor="rgba(239,68,68,0.07)", line=dict(color="rgba(239,68,68,0.35)",
                  width=1, dash="dot"))
    fig.add_annotation(xref="x2", yref="y2", x=5.2, y=2.75, showarrow=False,
                       text="deep AND off-line —<br>where the goals live",
                       font=dict(size=11, color=RED))
    fig.update_xaxes(title_text="distance off goal line at shot (m)", range=[0, 7],
                     gridcolor="rgba(255,255,255,0.07)", row=1, col=2)
    fig.update_yaxes(title_text="lateral error vs shot line (m)", range=[0, 3],
                     gridcolor="rgba(255,255,255,0.07)", row=1, col=2)

    fig.update_layout(**L(
        title=dict(text="TAB 3 · Shot-Stopping Geometry — was the save makeable from where he stood?",
                   y=0.98),
        margin=dict(l=60, r=40, t=100, b=130),
        legend=dict(orientation="h", y=-0.095),
    ))
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.225, showarrow=False,
                       text="All geometry from stored AC columns (pre_shot_gk_*): no PSxG required for v1 — "
                            "Goals Prevented arrives with the TF-48 goalmouth fast-follow.",
                       font=dict(size=11, color="rgba(255,255,255,0.55)"))
    save(fig, "v2_tab3_shot_geometry.png", w=1400, h=780)


# ===================================================================== V3
# REAL DATA (2026-06-11): same three tabs, driven by the scoped v4 sample in
# bronze.spadl_action_context, snapshotted to ./data/ by extract_proto_data.py.
# Sample = 2 matches x 2 halves per tracking provider; Metrica excluded
# (owner decision: anonymized players). Re-run the extractor when the full
# recompute lands (~100x rows).

DATA = Path(__file__).parent / "data"
PRESET_COLS = {
    "Default": "xt_gk", "Possession": "xt_gk_possession", "Counter": "xt_gk_counter",
    "Direct": "xt_gk_direct", "High Press": "xt_gk_high_press", "Low Block": "xt_gk_low_block",
}
REAL_BADGE = ("REAL DATA — scoped v4 sample (2 matches × 2 halves per tracking provider; "
              "Metrica excluded). Distributions/ranks are illustrative at this volume.")


def _pd():
    import pandas as pd
    return pd


def _names_map() -> dict:
    pd = _pd()
    nm = pd.read_parquet(DATA / "names.parquet")
    return {(r.provider, r.native_player_id): r.player_display_name for r in nm.itertuples()}


def _gk_label(names: dict, ds: str, pid: str) -> str:
    return str(names.get((ds, pid)) or f"{ds[:2].upper()} {str(pid)[-5:]}")


def _shots_canonical():
    """Shots with actual GK position reconciled into the ghost's canonical frame
    (defended goal at x=0). pre_shot_gk_* is frame-oriented, ghost_gk_* canonical —
    mirror when on opposite halves (prototype heuristic; flagged for the spec)."""
    pd = _pd()
    s = pd.read_parquet(DATA / "shots.parquet")
    s = s[s.data_source != "metrica"].copy()
    for c in ("pre_shot_gk_x", "pre_shot_gk_y", "ghost_gk_x", "ghost_gk_y",
              "start_x", "start_y", "defensive_line_x"):
        s[c] = pd.to_numeric(s[c], errors="coerce")
    flip = (s.pre_shot_gk_x - s.ghost_gk_x).abs() > 52.5
    s["actual_x"] = np.where(flip, 105.0 - s.pre_shot_gk_x, s.pre_shot_gk_x)
    s["actual_y"] = np.where(flip, 68.0 - s.pre_shot_gk_y, s.pre_shot_gk_y)
    s["flip"] = flip
    s["dev"] = np.hypot(s.actual_x - s.ghost_gk_x, s.actual_y - s.ghost_gk_y)
    s["line_height_m"] = np.where(flip, 105.0 - s.defensive_line_x, s.defensive_line_x)
    return s


def tab1_xtgk_real() -> None:
    """TAB 1 on real xT-GK columns: ranks + the same-passes-two-presets pair."""
    from plotly.subplots import make_subplots
    pd = _pd()

    dist = pd.read_parquet(DATA / "distributions.parquet")
    dist = dist[dist.data_source != "metrica"].copy()
    names = _names_map()
    dist["gk"] = [_gk_label(names, ds, p) for ds, p in zip(dist.data_source, dist.player_id)]
    for c in PRESET_COLS.values():
        dist[c] = pd.to_numeric(dist[c], errors="coerce")
    counts = dist.groupby("gk").size()
    qualified = counts[counts >= 10].index
    means = dist[dist.gk.isin(qualified)].groupby("gk")[list(PRESET_COLS.values())].mean()
    ranks = means.rank(ascending=False).astype(int)
    n_gk = len(means)
    # featured GK = clearest transition-built profile: among well-sampled GKs, the
    # largest SIGNED counter-minus-possession value gap (rank-swing alone can pick a
    # bottom-ranked GK, which buries the story)
    well = counts[counts >= 25].index.intersection(means.index)
    pool_idx = well if len(well) else means.index
    feat = (means.loc[pool_idx, "xt_gk_counter"] - means.loc[pool_idx, "xt_gk_possession"]).idxmax()
    fdist = dist[dist.gk == feat]
    vc = fdist["xt_gk_counter"].to_numpy(float)
    vp = fdist["xt_gk_possession"].to_numpy(float)
    pool = np.concatenate([vc[np.isfinite(vc)], vp[np.isfinite(vp)]])
    cmin, cmax = float(np.nanpercentile(pool, 5)), float(np.nanpercentile(pool, 95))
    cmax = max(cmax, cmin + 1e-6)

    fig = make_subplots(
        rows=2, cols=2, specs=[[{"colspan": 2}, None], [{}, {}]],
        row_heights=[0.42, 0.58], vertical_spacing=0.13, horizontal_spacing=0.05,
        subplot_titles=(
            f"Mean xT-GK per distribution — rank under every preset ({n_gk} GKs with ≥10 distributions)",
            f"{feat} under COUNTER — mean {means.loc[feat, 'xt_gk_counter']:.4f}/pass, "
            f"rank {ranks.loc[feat, 'xt_gk_counter']}/{n_gk}",
            f"SAME passes under POSSESSION — mean {means.loc[feat, 'xt_gk_possession']:.4f}/pass, "
            f"rank {ranks.loc[feat, 'xt_gk_possession']}/{n_gk}",
        ),
    )
    _style_subplot_titles(fig)

    for gk in means.index:
        hot = gk == feat
        fig.add_trace(go.Scatter(
            x=PRESETS, y=[int(ranks.loc[gk, c]) for c in PRESET_COLS.values()],
            mode="lines+markers+text",
            text=[gk if i == 0 else "" for i in range(6)],
            textposition="middle left", textfont=dict(size=10),
            line=dict(color=AMBER if hot else GREY, width=4 if hot else 1.2),
            marker=dict(size=9 if hot else 5), showlegend=False), row=1, col=1)
    fig.add_shape(type="rect", x0=1.6, x1=2.4, y0=0.5, y1=n_gk + 0.5, xref="x", yref="y",
                  fillcolor="rgba(245,158,11,0.10)",
                  line=dict(color="rgba(245,158,11,0.45)", width=1), layer="below")
    fig.update_yaxes(autorange="reversed", dtick=1, title_text="rank", row=1, col=1,
                     gridcolor="rgba(255,255,255,0.07)")
    fig.update_xaxes(row=1, col=1, gridcolor="rgba(255,255,255,0.07)")

    sx = fdist.start_x.to_numpy(float)
    sy = fdist.start_y.to_numpy(float)
    ex = fdist.end_x.to_numpy(float)
    ey = fdist.end_y.to_numpy(float)
    for cc, xref, yref, v in ((1, "x2", "y2", vc), (2, "x3", "y3", vp)):
        for sshape in _half_pitch_shapes(xref, yref):
            fig.add_shape(**sshape)
        for i in range(len(fdist)):
            if not np.isfinite(v[i]):
                continue
            frac = float(np.clip((v[i] - cmin) / (cmax - cmin), 0, 1))
            color = (f"rgba({int(120 + 135 * frac)},{int(130 + 28 * frac)},"
                     f"{int(200 - 150 * frac)},0.85)")
            fig.add_trace(go.Scatter(x=[sx[i], ex[i]], y=[sy[i], ey[i]], mode="lines",
                                     line=dict(color=color, width=1.0 + 3.2 * frac),
                                     showlegend=False, hoverinfo="skip"), row=2, col=cc)
        fig.update_xaxes(visible=False, range=[-3, 80], row=2, col=cc, scaleanchor=yref)
        fig.update_yaxes(visible=False, range=[-3, 71], row=2, col=cc)

    _selector_chips(fig, selected="Counter")
    fig.update_layout(**L(
        title=dict(text="TAB 1 · Distribution Value — what is his passing worth under YOUR game model?",
                   y=0.985),
        margin=dict(l=90, r=40, t=150, b=70),
    ))
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.055, showarrow=False,
                       text=f"Arrow color: blue/grey = low, amber = high xT-GK "
                            f"(scale {cmin:.3f}…{cmax:.3f}/pass, 5th–95th pct of this GK's values). "
                            f"{REAL_BADGE}",
                       font=dict(size=11, color="rgba(255,255,255,0.6)"))
    save(fig, "v3_tab1_xtgk_value.png", w=1400, h=1000)


def tab2_ghost_defense_real() -> None:
    """TAB 2 on real data: scene from the sample's largest plausible deviation;
    line-height split + closing times. game_state split deferred (sample 99.9%
    'drawing' — flagged upstream)."""
    import json

    from plotly.subplots import make_subplots
    pd = _pd()

    scene = json.loads((DATA / "scene.json").read_text(encoding="utf-8"))
    frame = pd.read_parquet(DATA / "scene_frame.parquet")
    ctx = pd.read_parquet(DATA / "context.parquet")
    ctx = ctx[ctx.data_source != "metrica"].copy()
    shots = _shots_canonical()
    names = _names_map()

    fig = make_subplots(
        rows=2, cols=2, specs=[[{"rowspan": 2}, {}], [None, {}]],
        column_widths=[0.52, 0.48], vertical_spacing=0.17, horizontal_spacing=0.10,
        subplot_titles=(
            f"Scene: shot faced, {scene['match_label']}, P{scene['period_id']} — actual vs ghost",
            f"Deviation vs DEFENSIVE LINE HEIGHT ({len(shots)} shots, pooled)",
            "Command of the box — min closing time (s, lower = better)",
        ),
    )
    _style_subplot_titles(fig)

    # ---- left: scene in canonical defended-goal-at-x=0 frame ----
    flip = bool(scene["pre_shot_gk_x"] > 52.5)
    px = frame.x.to_numpy(float)
    py = frame.y.to_numpy(float)
    if flip:
        px, py = 105.0 - px, 68.0 - py
    gk_native = scene["defending_gk_player_id_native"]
    keep = frame.player_id.astype(str) != str(gk_native)
    bx = float(frame.ball_x.iloc[0])
    by = float(frame.ball_y.iloc[0])
    if flip:
        bx, by = 105.0 - bx, 68.0 - by
    ox, oy = float(scene["ghost_gk_x"]), float(scene["ghost_gk_y"])
    ax_, ay_ = float(scene["actual_x"]), float(scene["actual_y"])
    gx, gy = np.meshgrid(np.linspace(0, 36, 90), np.linspace(0, 68, 90))
    dens = np.exp(-(((gx - ox) / 2.6) ** 2 + ((gy - oy) / 3.0) ** 2))
    fig.add_trace(go.Contour(
        x=np.linspace(0, 36, 90), y=np.linspace(0, 68, 90), z=dens,
        colorscale=[[0, "rgba(26,26,46,0)"], [0.35, "rgba(59,130,246,0.18)"],
                    [0.7, "rgba(42,157,143,0.45)"], [1, "rgba(245,158,11,0.85)"]],
        contours=dict(coloring="fill", showlines=False), showscale=False,
        hoverinfo="skip"), row=1, col=1)
    fig.add_trace(go.Scatter(x=px[keep.to_numpy()], y=py[keep.to_numpy()], mode="markers",
                             name="Players (tracked frame)", marker=dict(size=9, color=GREY)),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=[bx], y=[by], mode="markers", name="Ball",
                             marker=dict(size=11, color="white", symbol="circle-open",
                                         line=dict(width=2))), row=1, col=1)
    fig.add_trace(go.Scatter(x=[ox, ax_], y=[oy, ay_], mode="lines", name="Deviation tether",
                             line=dict(color="white", dash="dash", width=2.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=[ox], y=[oy], mode="markers", name="Ghost GK (model optimum)",
                             marker=dict(size=30, color="rgba(245,158,11,0.30)",
                                         line=dict(color=AMBER, width=2.5))), row=1, col=1)
    gk_name = _gk_label(names, scene["data_source"], gk_native)
    fig.add_trace(go.Scatter(x=[ax_], y=[ay_], mode="markers", name=f"{gk_name} (actual)",
                             marker=dict(size=17, color=BLUE, line=dict(color="white", width=2))),
                  row=1, col=1)
    fig.add_annotation(x=(ox + ax_) / 2 + 6.5, y=(oy + ay_) / 2, xref="x", yref="y",
                       text=f"{scene['dev']:.1f} m off optimum",
                       font=dict(color="white", size=13), align="left",
                       bgcolor="rgba(26,26,46,0.9)", showarrow=True, arrowhead=0, ax=46, ay=0)
    for sshape in _pitch_shapes_defensive_third():
        sshape = dict(sshape)
        sshape["xref"], sshape["yref"] = "x", "y"
        fig.add_shape(**sshape)
    fig.update_xaxes(visible=False, range=[-1.5, 33], scaleanchor="y", row=1, col=1)
    fig.update_yaxes(visible=False, range=[2, 66], row=1, col=1)

    # ---- right top: deviation by line-height terciles (REAL) ----
    sh = shots[np.isfinite(shots.dev) & np.isfinite(shots.line_height_m) & (shots.dev < 8.0)]
    terc = sh.line_height_m.quantile([1 / 3, 2 / 3]).to_numpy()
    bands = [
        (f"Deep block (<{terc[0]:.0f} m)", sh[sh.line_height_m < terc[0]]),
        ("Mid block", sh[(sh.line_height_m >= terc[0]) & (sh.line_height_m < terc[1])]),
        (f"High line (≥{terc[1]:.0f} m)", sh[sh.line_height_m >= terc[1]]),
    ]
    cats = [f"{lbl}  n={len(b)}" for lbl, b in bands]
    devs = [float(b.dev.mean()) for _, b in bands]
    fig.add_trace(go.Scatter(x=devs, y=cats, mode="markers+lines", name="Pooled sample GKs",
                             line=dict(color="rgba(245,158,11,0.45)", width=2),
                             marker=dict(size=13, color=AMBER, line=dict(color="white", width=1.5))),
                  row=1, col=2)
    fig.update_xaxes(title_text="mean deviation from ghost optimum (m)",
                     gridcolor="rgba(255,255,255,0.07)", row=1, col=2)
    fig.update_yaxes(categoryorder="array", categoryarray=cats[::-1], row=1, col=2)

    # ---- right bottom: closing times by zone — featured GK vs pooled (REAL) ----
    for c in ("gk_closing_time_min_s__six_yard_box", "gk_closing_time_min_s__near_post",
              "gk_closing_time_min_s__far_post", "gk_reachable_area_m2",
              "gk_pitch_control_share_weighted"):
        ctx[c] = pd.to_numeric(ctx[c], errors="coerce")
    feat_gk = ctx.groupby("defending_gk_player_id_native").size().idxmax()
    feat_ds = ctx[ctx.defending_gk_player_id_native == feat_gk].data_source.iloc[0]
    feat_name = _gk_label(names, feat_ds, feat_gk)
    fctx = ctx[ctx.defending_gk_player_id_native == feat_gk]
    zones = ["Six-yard box", "Near post", "Far post"]
    zcols = ["gk_closing_time_min_s__six_yard_box", "gk_closing_time_min_s__near_post",
             "gk_closing_time_min_s__far_post"]
    lg_t = [float(ctx[c].mean()) for c in zcols]
    bn_t = [float(fctx[c].mean()) for c in zcols]
    for z, a, b in zip(zones, lg_t, bn_t):
        fig.add_trace(go.Scatter(x=[a, b], y=[z, z], mode="lines", showlegend=False,
                                 line=dict(color="rgba(59,130,246,0.55)", width=4)), row=2, col=2)
    fig.add_trace(go.Scatter(x=lg_t, y=zones, mode="markers", name="Sample average",
                             marker=dict(size=11, color=GREY, line=dict(color="white", width=1))),
                  row=2, col=2)
    fig.add_trace(go.Scatter(x=bn_t, y=zones, mode="markers", name=feat_name,
                             marker=dict(size=13, color=BLUE, line=dict(color="white", width=1.5))),
                  row=2, col=2)
    reach = float(fctx.gk_reachable_area_m2.mean())
    share = float(fctx.gk_pitch_control_share_weighted.mean())
    fig.add_annotation(xref="paper", yref="paper", x=0.99, y=0.06, xanchor="right",
                       showarrow=False, align="left",
                       text=f"{feat_name}: reachable area {reach:.0f} m² · "
                            f"box pitch-control share {share:.0%}",
                       font=dict(size=11, color="rgba(255,255,255,0.7)"),
                       bgcolor="rgba(26,26,46,0.9)")
    fig.update_xaxes(title_text=f"min closing time, s (blue = {feat_name})",
                     gridcolor="rgba(255,255,255,0.07)", row=2, col=2)
    fig.update_yaxes(categoryorder="array", categoryarray=zones[::-1], row=2, col=2)

    fig.update_layout(**L(
        title=dict(text="TAB 2 · Defensive Positioning & Box Command — is he standing where the "
                        "league's keepers would?", y=0.985),
        margin=dict(l=60, r=40, t=110, b=150),
        legend=dict(orientation="h", y=-0.055),
    ))
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.155, showarrow=False, align="center",
                       text="Ghost density blob approximated from the stored optimum + spread "
                            "(the app renders the live model grid). Game-state split is designed in "
                            "but deferred: this sample is 99.9% 'drawing' (flagged upstream). "
                            f"<br>{REAL_BADGE}",
                       font=dict(size=11, color="rgba(255,255,255,0.55)"))
    save(fig, "v3_tab2_ghost_defense.png", w=1400, h=900)


def tab3_shot_geometry_real() -> None:
    """TAB 3 on real data: a real conceded/saved shot scene + the full positioning map."""
    from plotly.subplots import make_subplots
    pd = _pd()

    shots = _shots_canonical()
    names = _names_map()
    ok = shots[np.isfinite(shots.actual_x) & np.isfinite(shots.start_x) & (shots.dev < 8.0)].copy()
    # lateral error vs the shot line (shot origin -> goal centre), canonical frame
    sox = 105.0 - ok.start_x.to_numpy(float)   # per-action LTR -> canonical goal-at-0
    soy = 68.0 - ok.start_y.to_numpy(float)
    axv = ok.actual_x.to_numpy(float)
    ayv = ok.actual_y.to_numpy(float)
    dd = np.hypot(sox - 0.0, soy - 34.0)
    lat = np.abs((0.0 - sox) * (soy - ayv) - (sox - axv) * (34.0 - soy)) / np.where(dd == 0, 1, dd)
    ok["off_line_m"] = axv
    ok["lat_err_m"] = lat
    goals = ok[ok.result_id == 1]
    saved = ok[ok.result_id != 1]

    # scene: a real GOAL with a VISIBLE cone (close-range scramble goals degenerate
    # to a sliver) and sane geometry; else max-lat-err among all shots
    cand = goals[np.isfinite(goals.lat_err_m) & (goals.start_x <= 98.0) & (goals.off_line_m > 0.3)]
    scene_row = (cand.sort_values("lat_err_m", ascending=False).iloc[0]
                 if len(cand) else ok.sort_values("lat_err_m", ascending=False).iloc[0])
    gk_name = _gk_label(names, scene_row.data_source, scene_row.defending_gk_player_id_native)

    fig = make_subplots(
        rows=1, cols=2, column_widths=[0.54, 0.46], horizontal_spacing=0.08,
        subplot_titles=(
            f"Scene: {'GOAL conceded' if scene_row.result_id == 1 else 'shot faced'} — "
            f"{scene_row.data_source}, {gk_name}",
            f"All {len(ok)} on-target-ish shots in sample — where was the GK standing?",
        ),
    )
    _style_subplot_titles(fig)

    # left scene in attack-at-105 view: GK mirrored from canonical
    sbx, sby = float(scene_row.start_x), float(scene_row.start_y)
    kx, ky = 105.0 - float(scene_row.actual_x), 68.0 - float(scene_row.actual_y)
    fig.add_trace(go.Scatter(x=[sbx, 105.0, 105.0, sbx], y=[sby, 30.34, 37.66, sby],
                             fill="toself", fillcolor="rgba(239,68,68,0.16)",
                             line=dict(color="rgba(239,68,68,0.6)", width=1.5),
                             name="Shot cone (ball to posts)", mode="lines"), row=1, col=1)
    fig.add_trace(go.Scatter(x=[sbx], y=[sby], mode="markers", name="Ball (shot origin)",
                             marker=dict(size=12, color="white")), row=1, col=1)
    fig.add_trace(go.Scatter(x=[sbx, 105], y=[sby, 34.0], mode="lines",
                             name="Shot line (to centre)",
                             line=dict(color="rgba(255,255,255,0.5)", dash="dot", width=1.5)),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=[kx], y=[ky], mode="markers", name=f"{gk_name} (pre-shot)",
                             marker=dict(size=14, color=BLUE, line=dict(color="white", width=1.5))),
                  row=1, col=1)
    # stored angle is a signed direction angle — fold to the acute offset for display
    ang_deg = abs(float(scene_row.pre_shot_gk_angle_to_shot_trajectory)) * 180.0 / np.pi
    if ang_deg > 90.0:
        ang_deg = 180.0 - ang_deg
    fig.add_annotation(x=kx, y=ky + 3.4, xref="x", yref="y",
                       text=(f"{float(scene_row.actual_x):.1f} m off line · "
                             f"{ang_deg:.0f}° off shot trajectory<br>"
                             f"lateral error {float(scene_row.lat_err_m):.1f} m"),
                       font=dict(size=12), bgcolor="rgba(26,26,46,0.9)", showarrow=False)
    fig.add_shape(type="line", x0=105, y0=30.34, x1=105, y1=37.66, xref="x", yref="y",
                  line=dict(color=LINE, width=5))
    fig.add_shape(type="rect", x0=88.5, y0=13.84, x1=105, y1=54.16, xref="x", yref="y",
                  line=dict(color="rgba(224,224,224,0.4)", width=1))
    fig.update_xaxes(visible=False, range=[72, 107], scaleanchor="y", row=1, col=1)
    fig.update_yaxes(visible=False, range=[6, 62], row=1, col=1)

    # right: real positioning-vs-outcome map
    fig.add_trace(go.Scatter(x=saved.off_line_m, y=saved.lat_err_m, mode="markers",
                             name=f"No goal (n={len(saved)})",
                             marker=dict(size=8, color=BLUE, opacity=0.7)), row=1, col=2)
    fig.add_trace(go.Scatter(x=goals.off_line_m, y=goals.lat_err_m, mode="markers",
                             name=f"Goal conceded (n={len(goals)})",
                             marker=dict(size=12, color=RED, symbol="star")), row=1, col=2)
    fig.update_xaxes(title_text="distance off goal line at shot (m)",
                     gridcolor="rgba(255,255,255,0.07)", row=1, col=2)
    fig.update_yaxes(title_text="lateral error vs shot line (m)",
                     gridcolor="rgba(255,255,255,0.07)", row=1, col=2)

    fig.update_layout(**L(
        title=dict(text="TAB 3 · Shot-Stopping Geometry — was the save makeable from where he stood?",
                   y=0.98),
        margin=dict(l=60, r=40, t=100, b=130),
        legend=dict(orientation="h", y=-0.095),
    ))
    fig.add_annotation(xref="paper", yref="paper", x=0.5, y=-0.225, showarrow=False,
                       text="All geometry from stored AC columns (pre_shot_gk_*) + SPADL result join; "
                            f"Goals Prevented arrives with TF-48. {REAL_BADGE}",
                       font=dict(size=11, color="rgba(255,255,255,0.55)"))
    save(fig, "v3_tab3_shot_geometry.png", w=1400, h=780)


if __name__ == "__main__":
    import sys

    if "--v1" in sys.argv:
        radar()
        philosophy_bump()
        components()
        pressure_split()
        ghost_tether()
        preshot_cone()
        distribution_map()
        risk_reward()
    elif "--v2" in sys.argv:
        tab1_xtgk()
        tab2_ghost_defense()
        tab3_shot_geometry()
    else:
        tab1_xtgk_real()
        tab2_ghost_defense_real()
        tab3_shot_geometry_real()
    print("done ->", OUT)
