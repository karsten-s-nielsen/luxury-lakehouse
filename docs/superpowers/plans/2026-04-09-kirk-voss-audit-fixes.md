# Kirk/Voss Audit Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve all 11 findings from the Kirk (data visualization) and Voss (empathy/tone) audits — 3 Medium, 6 Low, 2 Advisory.

**Architecture:** Three logical commits grouping changes by concern: (1) Kirk visualization fixes — shared colour constants, chart annotations, editorial hierarchy, redundant encoding, advisories; (2) Voss error message empathy — systematic rewrite across all state modules and Gradio; (3) Voss documentation tone — getting-started, CONTRIBUTING, ARCHITECTURE, glossary.

**Tech Stack:** Python (matplotlib, Plotly, mplsoccer), Taipy state modules, Gradio demo, Markdown docs.

**Audit source documents:** `D:\Development\LAKEHOUSE-KIRK-VOSS.md`, `D:\Development\LAKEHOUSE-VISUAL-AUDIT.md`

---

## Commit Strategy

| # | Commit | Tasks | Findings |
|---|--------|-------|----------|
| 1 | `fix(ui): unify colours and improve chart annotations` | 1–4 | K-1, K-2, K-3, K-4, K-5, K-6, K-7 |
| 2 | `fix(ui): empathetic error messages across all pages` | 5 | V-1 |
| 3 | `docs: tone, front-loading, reader ownership` | 6 | V-2, V-3, V-4 |

---

## Task 1: Extract Shared Colour Constants to render.py (K-1 + K-2)

**Findings addressed:** K-1 (DEFCON colours swapped between surfaces), K-2 (home/away inconsistent cross-page)

**Files:**
- Modify: `hf_taipy_app/src/render.py` (add constants after line 22)
- Modify: `hf_taipy_app/src/state/match_summary.py:64-65` (replace local constants)
- Modify: `hf_taipy_app/src/state/movement_analysis.py:34-36` (replace local constants)
- Modify: `hf_taipy_app/src/state/pitch_control.py:31-34` (replace local constants, fix swap)
- Modify: `hf_taipy_app/src/state/team_shape.py:38-43` (replace local constants, align palette)
- Modify: `hf_taipy_app/src/state/defensive_valuation.py:147-158` (align to canonical DEFCON mapping)
- Modify: `demo_space/app.py:552,564,579,729-734` (use canonical colours)

### Design Decisions

**Home/Away canonical convention:** Red = Home, Steel-blue = Away. This matches the majority convention (match_summary, movement_analysis — 2 of 4 Taipy pages). Pitch control and team_shape currently have them swapped or use a different palette — both will be updated.

**DEFCON canonical mapping:** Adopt the Gradio mapping (stronger semantic justification per audit):
- Intercept → `#e63946` (red — most decisive defensive action)
- Concede → `#f4a261` (orange — negative outcome)
- Disturb → `#457b9d` (steel-blue)
- Deter → `#2a9d8f` (teal)

This also resolves the Visual Audit's teal-blue perceptual proximity finding — teal is now maximally separated from its neighbours.

**Team Shape hull/line variants:** Derived from the canonical home/away hex values as rgba strings for Plotly.

### Steps

- [ ] **Step 1: Add shared colour constants to render.py**

Add after the existing `PLAYER_COLORS` line (line 22) in `hf_taipy_app/src/render.py`:

```python
# Home / Away — canonical convention across all pages and surfaces.
# Red = Home, Steel-blue = Away (Kirk audit K-2).
HOME_COLOR = "#e63946"
AWAY_COLOR = "#457b9d"
# Plotly rgba variants for team shape hulls and lines
HULL_HOME_COLOR = "rgba(230,57,70,0.15)"
HULL_AWAY_COLOR = "rgba(69,123,157,0.15)"
LINE_HOME_COLOR = "rgba(230,57,70,0.5)"
LINE_AWAY_COLOR = "rgba(69,123,157,0.5)"

# DEFCON credit type colours — canonical mapping across surfaces.
# Semantic: red = decisive intercept, orange = negative concede,
# steel-blue = disrupt, teal = deter (Kirk audit K-1).
DEFCON_COLORS = {
    "Intercept": "#e63946",
    "Concede": "#f4a261",
    "Disturb": "#457b9d",
    "Deter": "#2a9d8f",
}
```

- [ ] **Step 2: Update match_summary.py**

Replace lines 64-65:

```python
# Before:
_HOME_COLOR = "#e63946"
_AWAY_COLOR = "#457b9d"

# After:
from hf_taipy_app.src.render import HOME_COLOR as _HOME_COLOR, AWAY_COLOR as _AWAY_COLOR
```

Move the import to the top of the file with the other `render` imports (the file already imports `PITCH_BG_COLOR, TEXT_COLOR` from render). Remove the two local constant lines.

- [ ] **Step 3: Update movement_analysis.py**

Replace lines 34-36. Keep `_BAR_COLOR` (teal, used for physical bars — not a home/away colour):

```python
# Before:
_BAR_COLOR       = "#2a9d8f"
_HOME_PPDA_COLOR = "#e63946"
_AWAY_PPDA_COLOR = "#457b9d"

# After (import HOME_COLOR, AWAY_COLOR from render at top):
_BAR_COLOR       = "#2a9d8f"
_HOME_PPDA_COLOR = HOME_COLOR
_AWAY_PPDA_COLOR = AWAY_COLOR
```

- [ ] **Step 4: Update pitch_control.py — fix the swap**

Replace lines 31-34 (currently swapped: home=steel-blue, away=red):

```python
# Before:
_HOME_COLOR  = "#457b9d"
_AWAY_COLOR  = "#e63946"
_BALL_COLOR  = "#f4d03f"
_TEXT_COLOR  = "#e0e0e0"

# After (import HOME_COLOR, AWAY_COLOR from render at top):
_HOME_COLOR = HOME_COLOR
_AWAY_COLOR = AWAY_COLOR
_BALL_COLOR  = "#f4d03f"
_TEXT_COLOR  = "#e0e0e0"
```

- [ ] **Step 5: Update team_shape.py — align palette**

Replace lines 38-43 (currently uses Tailwind blue/red instead of the shared palette):

```python
# Before:
_HOME_COLOR      = "#3b82f6"
_AWAY_COLOR      = "#ef4444"
_HULL_HOME_COLOR = "rgba(59,130,246,0.15)"
_HULL_AWAY_COLOR = "rgba(239,68,68,0.15)"
_LINE_HOME_COLOR = "rgba(59,130,246,0.5)"
_LINE_AWAY_COLOR = "rgba(239,68,68,0.5)"

# After (import from render at top):
_HOME_COLOR      = HOME_COLOR
_AWAY_COLOR      = AWAY_COLOR
_HULL_HOME_COLOR = HULL_HOME_COLOR
_HULL_AWAY_COLOR = HULL_AWAY_COLOR
_LINE_HOME_COLOR = LINE_HOME_COLOR
_LINE_AWAY_COLOR = LINE_AWAY_COLOR
```

Note: to avoid name collision with local `_` prefixed aliases, import with explicit names:
```python
from hf_taipy_app.src.render import (
    HOME_COLOR, AWAY_COLOR,
    HULL_HOME_COLOR, HULL_AWAY_COLOR,
    LINE_HOME_COLOR, LINE_AWAY_COLOR,
)
```
Then replace lines 38-43 with assignments from the imports, or use the imports directly throughout the file. The simpler approach is to just import them and delete the local constants, then find-replace the `_HOME_COLOR` references to `HOME_COLOR` etc. Check which approach causes fewer changes.

- [ ] **Step 6: Update defensive_valuation.py — align DEFCON colours**

Replace lines 147-152:

```python
# Before:
_CREDIT_COLORS = {
    "intercept_pressure": "#2a9d8f",
    "concede_pressure":   "#e63946",
    "disturb_pressure":   "#457b9d",
    "deter_pressure":     "#f4a261",
}

# After (import DEFCON_COLORS from render at top):
_CREDIT_COLORS = {
    "intercept_pressure": DEFCON_COLORS["Intercept"],
    "concede_pressure":   DEFCON_COLORS["Concede"],
    "disturb_pressure":   DEFCON_COLORS["Disturb"],
    "deter_pressure":     DEFCON_COLORS["Deter"],
}
```

The `_CREDIT_LABELS` dict on lines 153-158 stays unchanged.

- [ ] **Step 7: Update demo_space/app.py — pitch control colours**

Replace the inline colour literals at lines 552, 564, 579:

Add constants near the top of the file (demo_space is a standalone Space — cannot import from hf_taipy_app):

```python
# Canonical colours — must match hf_taipy_app/src/render.py (Kirk audit K-1, K-2)
_HOME_COLOR = "#e63946"
_AWAY_COLOR = "#457b9d"
_DEFCON_COLORS = {
    "Intercept": "#e63946",
    "Concede": "#f4a261",
    "Disturb": "#457b9d",
    "Deter": "#2a9d8f",
}
```

Then replace:
- Line 552: `c="#457b9d"` → `c=_HOME_COLOR`
- Line 564: `c="#e63946"` → `c=_AWAY_COLOR`
- Line 579: `color = "#457b9d" if row["team"] == "home" else "#e63946"` → `color = _HOME_COLOR if row["team"] == "home" else _AWAY_COLOR`
- Lines 729-734: Replace local `color_map` dict with `color_map = _DEFCON_COLORS`

- [ ] **Step 8: Verify colour consistency**

Run: `uv run ruff check hf_taipy_app/src/render.py hf_taipy_app/src/state/match_summary.py hf_taipy_app/src/state/movement_analysis.py hf_taipy_app/src/state/pitch_control.py hf_taipy_app/src/state/team_shape.py hf_taipy_app/src/state/defensive_valuation.py demo_space/app.py`

Run: `uv run pyright hf_taipy_app/src/render.py hf_taipy_app/src/state/match_summary.py hf_taipy_app/src/state/movement_analysis.py hf_taipy_app/src/state/pitch_control.py hf_taipy_app/src/state/team_shape.py hf_taipy_app/src/state/defensive_valuation.py`

---

## Task 2: Chart Annotations (K-3) + Editorial Focus Hierarchy (K-4)

**Findings addressed:** K-3 (3 missing annotations), K-4 (3 pages lack focus hierarchy)

**Files:**
- Modify: `hf_taipy_app/src/state/heat_map.py:101-105` (add colorbar)
- Modify: `hf_taipy_app/src/state/pass_network.py:148-178` (add edge-width legend annotation)
- Modify: `hf_taipy_app/src/state/shot_map.py:118-141` (add legend with labels)
- Modify: `hf_taipy_app/src/state/match_summary.py:78` (increase primary chart size)
- Modify: `hf_taipy_app/src/state/goalkeeper.py:287` (increase goalmouth scatter height)
- Modify: `hf_taipy_app/src/state/pass_timing.py:168,209` (differentiate chart heights)

### Steps

- [ ] **Step 1: Add colorbar to heat map**

In `hf_taipy_app/src/state/heat_map.py`, after the `pitch.heatmap(...)` call at line 102, add a colorbar:

```python
    bin_stats = pitch.bin_statistic(expanded_x, expanded_y, statistic="count", bins=(12, 8))
    hm = pitch.heatmap(bin_stats, ax=ax, cmap="hot", edgecolors=PITCH_BG_COLOR)
    cbar = fig.colorbar(hm, ax=ax, shrink=0.6, pad=0.02)
    cbar.set_label("Action Count", color=PITCH_LINE_COLOR, fontsize=10)
    cbar.ax.yaxis.set_tick_params(color=PITCH_LINE_COLOR)
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color=PITCH_LINE_COLOR)
```

Key change: capture the return value of `pitch.heatmap()` (it returns the `matplotlib.collections.QuadMesh` mappable) and pass it to `fig.colorbar()`.

- [ ] **Step 2: Add edge-width annotation to pass network**

In `hf_taipy_app/src/state/pass_network.py`, after the edge loop (after line 159) and before the node scatter, add a legend annotation explaining edge width:

```python
    # Edge-width legend annotation (Kirk audit K-3)
    fig.add_annotation(
        text="Line thickness = pass frequency between pair",
        xref="paper", yref="paper",
        x=0.5, y=-0.02,
        showarrow=False,
        font=dict(color="rgba(255,255,255,0.5)", size=10),
    )
```

- [ ] **Step 3: Add legend to Taipy shot map**

In `hf_taipy_app/src/state/shot_map.py`, add `label=` to both scatter calls and an `ax.legend()` before the return:

```python
        if misses_mask.any():
            pitch.scatter(
                shots.loc[misses_mask, "location_x"],
                shots.loc[misses_mask, "location_y"],
                ax=ax,
                s=xg_sizes[misses_mask],
                color=GRAY,
                alpha=0.5,
                zorder=2,
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
                label="Goal",
            )
        ax.legend(
            loc="upper left",
            fontsize=9,
            facecolor=PITCH_BG_COLOR,
            edgecolor="white",
            labelcolor="white",
        )
```

- [ ] **Step 4: Editorial focus — Match Summary primary chart**

In `hf_taipy_app/src/state/match_summary.py`, the `_render_stat_bars` function at line 78 uses a fixed `figsize=(6, ...)`. The function is called 4 times for Shooting, Passing, Possession, PPDA.

Add a `primary` parameter to make the shooting chart (editorial lead) visually larger:

```python
def _render_stat_bars(
    home_vals: list[float],
    away_vals: list[float],
    labels: list[str],
    home_name: str,
    away_name: str,
    title: str,
    file_name: str,
    *,
    primary: bool = False,
) -> str:
    width = 7 if primary else 6
    fig, ax = plt.subplots(figsize=(width, max(2.5, len(labels) * 0.8)), facecolor=PITCH_BG_COLOR)
```

Then pass `primary=True` in the shooting chart call site (find the call that renders the shooting chart — it should be the first `_render_stat_bars` call in `ms_refresh`).

- [ ] **Step 5: Editorial focus — GK goalmouth scatter height**

In `hf_taipy_app/src/state/goalkeeper.py`, increase the goalmouth scatter height from 450 to 520 at line 287:

```python
        height=520,  # Primary view — taller than supporting charts (Kirk K-4)
```

- [ ] **Step 6: Editorial focus — Pass Timing scatter vs heatmap**

In `hf_taipy_app/src/state/pass_timing.py`:
- Line 168: Change scatter `height=450` → `height=520` (primary analytical view)
- Line 209: Keep heatmap at `height=450` (supporting context)

```python
# _build_scatter_figure, line 168:
        height=520,  # Primary view (Kirk K-4)

# _build_heatmap_figure stays at height=450
```

- [ ] **Step 7: Verify**

Run: `uv run ruff check hf_taipy_app/src/state/heat_map.py hf_taipy_app/src/state/pass_network.py hf_taipy_app/src/state/shot_map.py hf_taipy_app/src/state/match_summary.py hf_taipy_app/src/state/goalkeeper.py hf_taipy_app/src/state/pass_timing.py`

Run: `uv run pyright hf_taipy_app/src/state/heat_map.py hf_taipy_app/src/state/pass_network.py hf_taipy_app/src/state/shot_map.py hf_taipy_app/src/state/match_summary.py hf_taipy_app/src/state/goalkeeper.py hf_taipy_app/src/state/pass_timing.py`

---

## Task 3: Redundant Data Encoding (K-5 + K-7)

**Findings addressed:** K-5 (8 charts lack redundant encoding), K-7 (player similarity green/red needs marker shapes)

**Files:**
- Modify: `hf_taipy_app/src/state/pass_map.py:156-165` (legend patches → Line2D with line-width)
- Modify: `hf_taipy_app/src/state/defensive_valuation.py:179-186` (add `pattern_shape` to DEFCON bars)
- Modify: `demo_space/app.py:739-752` (add `pattern_shape` to Gradio DEFCON bars)
- Modify: `hf_taipy_app/src/state/goalkeeper.py:241-260` (add marker symbols to goalmouth scatter)
- Modify: `hf_taipy_app/src/state/goalkeeper.py:392-410` (add varying arrow widths to distribution)
- Modify: `hf_taipy_app/src/state/shot_map.py:118-139` (add marker differentiation — star vs circle)
- Modify: `hf_taipy_app/src/state/player_similarity.py:363-400` (add marker symbols per bucket)
- Modify: `hf_taipy_app/src/state/pitch_control.py:152-175` (add marker shapes to home/away)

### Steps

- [ ] **Step 1: Pass map — use Line2D legend to show arrow width differences**

In `hf_taipy_app/src/state/pass_map.py`, the arrows already have different widths (1.0, 1.5, 2.0, 2.5) but the legend uses flat `mpatches.Patch` which can't show width. Replace lines 165 with `Line2D` handles:

```python
    from matplotlib.lines import Line2D
    handles = [
        Line2D([0], [0], color=c, alpha=a, linewidth=w * 2, label=lbl)
        for lbl, c, a, w in [
            ("Incomplete", _INCOMPLETE_COLOR, 0.5, 1.0),
            ("Complete", _COMPLETE_COLOR, 0.7, 1.5),
            *([("Progressive", _PROGRESSIVE_COLOR, 0.9, 2.0)] if highlight_progressive else []),
            *([("Line-Breaking", _LINE_BREAKING_COLOR, 0.95, 2.5)]
              if highlight_line_breaking and "is_line_breaking" in passes.columns else []),
        ]
    ]
```

Wait — this replaces the existing dynamic legend construction. A simpler approach: keep the current structure but swap `mpatches.Patch` for `Line2D`:

```python
    handles = [
        Line2D([0], [0], color=c, alpha=a, linewidth=w * 2, label=lbl)
        for lbl, c, a in legend_entries
        for w in [dict(zip(
            [_INCOMPLETE_COLOR, _COMPLETE_COLOR, _PROGRESSIVE_COLOR, _LINE_BREAKING_COLOR],
            [1.0, 1.5, 2.0, 2.5], strict=False,
        )).get(c, 1.5)]
    ]
```

Actually, the cleanest approach — build the legend entries with width included:

```python
    legend_entries: list[tuple[str, str, float, float]] = [
        ("Incomplete", _INCOMPLETE_COLOR, 0.5, 1.0),
        ("Complete", _COMPLETE_COLOR, 0.7, 1.5),
    ]
    if highlight_progressive:
        legend_entries.append(("Progressive", _PROGRESSIVE_COLOR, 0.9, 2.0))
    if highlight_line_breaking and "is_line_breaking" in passes.columns:
        legend_entries.append(("Line-Breaking", _LINE_BREAKING_COLOR, 0.95, 2.5))

    handles = [
        Line2D([0], [0], color=c, alpha=a, linewidth=w * 2, label=lbl)
        for lbl, c, a, w in legend_entries
    ]
```

Add `from matplotlib.lines import Line2D` to the imports. Remove `import matplotlib.patches as mpatches` if it becomes unused.

- [ ] **Step 2: DEFCON bars (Taipy) — add pattern_shape**

In `hf_taipy_app/src/state/defensive_valuation.py`, after the `px.bar(...)` call at line 179, the simplest approach is to add `pattern_shape="Credit Type"` to the `px.bar()` call:

```python
    fig = px.bar(
        plot_data,
        x=label_col,
        y="Pressure",
        color="Credit Type",
        pattern_shape="Credit Type",
        barmode="group",
        title=title,
    )
```

Plotly will auto-assign distinct hatch patterns (/, \, x, +) to each credit type.

- [ ] **Step 3: DEFCON bars (Gradio) — add pattern_shape**

In `demo_space/app.py`, at the `px.bar(...)` call around line 739, add `pattern_shape="pressure_type"`:

```python
    plotly_fig = px.bar(
        melted,
        x="match_label",
        y="pressure_value",
        color="pressure_type",
        pattern_shape="pressure_type",
        barmode="group",
        color_discrete_map=color_map,
        ...
    )
```

- [ ] **Step 4: GK goalmouth scatter — add marker symbols**

In `hf_taipy_app/src/state/goalkeeper.py`, lines 241-260, add `marker.symbol` to differentiate saved vs goal:

```python
        for mask, color, name, symbol in [
            (saved_mask, _SAVED_COLOR, "Saved", "circle"),
            (goal_mask, _GOAL_COLOR, "Goal", "star"),
        ]:
            subset = df[mask]
            if subset.empty:
                continue
            fig.add_trace(
                go.Scatter(
                    x=subset["end_x"],
                    y=subset["end_y"],
                    mode="markers",
                    marker=dict(size=8, color=color, opacity=0.7, symbol=symbol),
                    name=name,
                    ...
                )
            )
```

- [ ] **Step 5: GK distribution — vary arrow widths by distance category**

In `hf_taipy_app/src/state/goalkeeper.py`, lines 392-410, change the constant `width=1.5` to vary per category:

```python
        width_map = {"short": 1.0, "medium": 1.5, "long": 2.5}
        for cat, color in color_map.items():
            subset = df[df["category"] == cat]
            if subset.empty:
                continue
            pitch.arrows(
                ...,
                color=color,
                width=width_map[cat],
                ...,
            )
```

- [ ] **Step 6: Shot map (Taipy) — add marker differentiation**

In `hf_taipy_app/src/state/shot_map.py`, add `marker="o"` for misses and `marker="*"` for goals (matching Gradio's star-for-goals pattern):

```python
        if misses_mask.any():
            pitch.scatter(
                ...,
                marker="o",
                label="Miss / Saved",
            )
        if goals_mask.any():
            pitch.scatter(
                ...,
                marker="*",
                label="Goal",
            )
```

Note: mplsoccer's `pitch.scatter()` passes `marker` through to matplotlib. Verify `*` works (it's a standard matplotlib marker).

- [ ] **Step 7: Player similarity — add marker symbols per bucket (K-7)**

In `hf_taipy_app/src/state/player_similarity.py`, add a symbol map alongside the colour map at lines 363-368:

```python
_SIMILARITY_COLORS = {
    "Very Similar":       "#2ecc71",
    "Similar":            "#3498db",
    "Moderately Similar": "#f39c12",
    "Different":          "#e74c3c",
}
_SIMILARITY_SYMBOLS = {
    "Very Similar":       "circle",
    "Similar":            "diamond",
    "Moderately Similar": "square",
    "Different":          "x",
}
```

Then in the scatter loop at lines 386-400, add the symbol:

```python
    for bucket, color in _SIMILARITY_COLORS.items():
        mask = labels == bucket
        if not mask.any():
            continue
        fig.add_trace(
            go.Scatter(
                x=similarity[mask],
                y=names[mask],
                mode="markers",
                marker={"size": 12, "color": color, "symbol": _SIMILARITY_SYMBOLS[bucket]},
                name=bucket,
                hovertemplate="%{y}<br>Similarity: %{x:.1%}<extra></extra>",
            )
        )
```

- [ ] **Step 8: Pitch control — add marker shapes for home vs away**

In `hf_taipy_app/src/state/pitch_control.py`, lines 152-175, add `marker="o"` for home and `marker="s"` (square) for away:

```python
    if not home.empty:
        pitch_obj.scatter(
            home["x"], home["y"],
            color=_HOME_COLOR, s=120,
            edgecolors=PITCH_LINE_COLOR, linewidth=0.8,
            ax=ax, zorder=3, label="Home",
            marker="o",
        )
    if not away.empty:
        pitch_obj.scatter(
            away["x"], away["y"],
            color=_AWAY_COLOR, s=120,
            edgecolors=PITCH_LINE_COLOR, linewidth=0.8,
            ax=ax, zorder=3, label="Away",
            marker="s",
        )
```

- [ ] **Step 9: Verify**

Run: `uv run ruff check hf_taipy_app/src/state/pass_map.py hf_taipy_app/src/state/defensive_valuation.py hf_taipy_app/src/state/goalkeeper.py hf_taipy_app/src/state/shot_map.py hf_taipy_app/src/state/player_similarity.py hf_taipy_app/src/state/pitch_control.py demo_space/app.py`

Run: `uv run pyright hf_taipy_app/src/state/pass_map.py hf_taipy_app/src/state/defensive_valuation.py hf_taipy_app/src/state/goalkeeper.py hf_taipy_app/src/state/shot_map.py hf_taipy_app/src/state/player_similarity.py hf_taipy_app/src/state/pitch_control.py`

---

## Task 4: Pass Network Node Sizing (K-6)

**Finding addressed:** K-6 (Advisory — linear diameter scaling overstates differences)

**Files:**
- Modify: `hf_taipy_app/src/state/pass_network.py:164` (apply sqrt for area-proportional sizing)

### Steps

- [ ] **Step 1: Apply sqrt to node sizing**

In `hf_taipy_app/src/state/pass_network.py`, line 164:

```python
# Before:
    sizes = 8 + (nodes["pass_count"] - pc_min) / pc_range * 30

# After:
    import numpy as np
    sizes = 8 + np.sqrt((nodes["pass_count"] - pc_min) / pc_range) * 30
```

Add `import numpy as np` to the file's imports if not already present. Alternatively, use `** 0.5` instead of `np.sqrt` if numpy is not already imported — check file imports.

- [ ] **Step 2: Verify**

Run: `uv run ruff check hf_taipy_app/src/state/pass_network.py`
Run: `uv run pyright hf_taipy_app/src/state/pass_network.py`

---

## Task 5: Error Message Empathy (V-1)

**Finding addressed:** V-1 (Medium — 42+ flat error messages across all pages)

**Files:**
- Modify: `hf_taipy_app/src/state/action_values.py`
- Modify: `hf_taipy_app/src/state/defensive_valuation.py`
- Modify: `hf_taipy_app/src/state/goalkeeper.py`
- Modify: `hf_taipy_app/src/state/heat_map.py`
- Modify: `hf_taipy_app/src/state/match_summary.py`
- Modify: `hf_taipy_app/src/state/movement_analysis.py`
- Modify: `hf_taipy_app/src/state/pass_map.py`
- Modify: `hf_taipy_app/src/state/pass_network.py`
- Modify: `hf_taipy_app/src/state/pitch_control.py`
- Modify: `hf_taipy_app/src/state/player_radar.py`
- Modify: `hf_taipy_app/src/state/player_similarity.py`
- Modify: `hf_taipy_app/src/state/shot_map.py`
- Modify: `hf_taipy_app/src/state/tactical_positions.py`
- Modify: `hf_taipy_app/src/state/team_shape.py`
- Modify: `hf_taipy_app/src/state/workflows_stats.py`
- Modify: `demo_space/app.py`

### Message Template

Every error/empty-state message follows this structure:

1. **Label the situation** — acknowledge what happened
2. **Explain why** (when applicable) — data source limitation, tracking requirement, etc.
3. **Suggest next step** — filter change, page-appropriate action

### Message Categories and Rewrites

Messages fall into categories. The replacement pattern per category:

**Category A: Bare "Error" on stat cards**
```
"Error" → "–"
```
Stat cards should show a dash (the same as the initial/cleared state), not a bare "Error" string. The warning_text variable already carries the error context — the stat card doesn't need to duplicate it.

**Category B: "Error loading X" warning messages**
```
"Error loading X." → "Something went wrong loading X. Try refreshing the page."
```

**Category C: "No X for the selected filters" — event data pages**
These pages work with any match (StatsBomb + Wyscout). The issue is filter combination.
```
"No X for the selected filters." → "No X found for this filter combination. Try broadening your selection."
```

**Category D: "No X" — tracking-dependent pages**
These pages require tracking data (~20 matches: 3 Metrica, 7 IDSSE, 10 SkillCorner).
```
"No tracking data for ..." → "Tracking data isn't available for this match. Try a Metrica, IDSSE, or SkillCorner match."
```

**Category E: "No X" — PAUSA/OBSO-dependent**
Only 7 IDSSE matches.
```
Already good — pass_timing.py messages are the audit's passing examples.
```

**Category F: "No X" — Wyscout limitation**
```
Already good — pass_network.py messages explain the Wyscout gap.
```

**Category G: "Select a X" prompts**
```
Leave as-is — these are directional UI prompts, not error states.
```

### Exact Rewrites Per File

- [ ] **Step 1: action_values.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 281 | `"Error loading rankings."` | `"Something went wrong loading rankings. Try refreshing the page."` |
| 282 | `"Error loading VAEP rankings."` | `"Something went wrong loading VAEP rankings. Try refreshing the page."` |
| 287 | `"No VAEP data available for the selected filters."` | `"No VAEP data for this filter combination. Try selecting a different competition or removing player filters."` |
| 288 | `"No VAEP data for the selected filters."` | `"No VAEP data for this filter combination. Try selecting a different competition or removing player filters."` |
| 311-313 | `"Error"` (×3 stat cards) | `"–"` |
| 315 | `"Error loading action breakdown."` | `"Something went wrong loading the breakdown. Try refreshing the page."` |
| 323 | `"No VAEP data for the selected filters."` | `"No VAEP data for this filter combination. Try selecting a different competition or match."` |
| 367-370 | `"Error"` (×4 stat cards) | `"–"` |
| 375 | `"Error loading match timeline."` | `"Something went wrong loading the timeline. Try refreshing the page."` |
| 387 | `"No VAEP data for the selected match."` | `"No VAEP data for this match. Try selecting a different match."` |

- [ ] **Step 2: defensive_valuation.py**

| Lines | Current | Replacement |
|-------|---------|-------------|
| 343, 347, 382, 391, 399, 490, 499, 507 | `"No defensive pressure data for the selected filters."` | `"No DEFCON data for this filter combination. DEFCON requires tracking data — try selecting an IDSSE Bundesliga match."` |
| 431-434 | `"Error"` (×4 stat cards) | `"–"` |

- [ ] **Step 3: goalkeeper.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 453 | `"Error loading GK rankings."` | `"Something went wrong loading GK rankings. Try refreshing the page."` |
| 460 | `"No GK data available for the selected filters."` | `"No GK data for this filter combination. Try selecting a different competition or team."` |
| 491 | `"Error loading shot stopping data."` | `"Something went wrong loading shot stopping data. Try refreshing the page."` |
| 528 | `"No on-target shots found for the selected filters."` | `"No on-target shots for this selection. Try a different GK or match range."` |
| 559 | `"Error loading distribution data."` | `"Something went wrong loading distribution data. Try refreshing the page."` |
| 570 | `"No GK distribution passes for the selected filters."` | `"No GK distribution passes found. Try selecting a different match or GK."` |

- [ ] **Step 4: heat_map.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 186 | `"No actions for the selected filters."` | `"No actions found for this filter combination. Try broadening your selection."` |

- [ ] **Step 5: match_summary.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 146 | `"No match data for the selected filters."` | `"No match data for this selection. Try choosing a different competition or match."` |

- [ ] **Step 6: movement_analysis.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 231 | `"No physical stats for the selected match."` | `"No physical stats for this match. Physical data requires tracking (~20 matches from Metrica, IDSSE, SkillCorner)."` |
| 289 | `"No PPDA data for the selected competition."` | `"No PPDA data for this competition. PPDA uses StatsBomb defensive actions — try a StatsBomb competition."` |
| 336 | `"No off-ball xT data for the selected match."` | `"No off-ball xT data for this match. Off-ball xT requires tracking data — try a Metrica, IDSSE, or SkillCorner match."` |

- [ ] **Step 7: pass_map.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 266 | `"No passes for the selected filters."` | `"No passes found for this filter combination. Try selecting a different match or team."` |

- [ ] **Step 8: pass_network.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 224-225 | `"Error"` (×2 stat cards) | `"–"` |
| 263 | `"No connections meet threshold"` | `"No connections meet the minimum threshold. Try lowering the pass count filter."` |

Lines 236, 239 already good — leave as-is.

- [ ] **Step 9: pitch_control.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 347-348 | `"No data for this frame for the selected filters."` | `"No pitch control data for this frame. Try selecting a different frame or half."` |
| 461 | `"No frames for this match and period for the selected filters."` | `"No frames found for this match and period. Try a different half or match."` |

Line 449 is already borderline good — leave or mildly enhance:
| 449 | `"Pitch control requires player tracking data (~20 matches from Metrica, IDSSE, SkillCorner)."` | Leave as-is (already explains the requirement). |

- [ ] **Step 10: player_radar.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 262-263 | `"No player stats for the selected filters."` | `"No player stats for this filter combination. Try selecting a different competition, team, or position."` |

Line 276 already partial — leave as-is.

- [ ] **Step 11: player_similarity.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 266 | `"No players with embeddings found."` | `"No players with embeddings found. Embeddings are available for players with enough match history."` |
| 447 | `"No embedding vector for this player for the selected filters."` | `"No embedding vector for this player. Try switching the search mode or selecting a player with more match history."` |
| 453 | `f"No {search_mode.lower()} vector for this player for the selected filters."` | `f"No {search_mode.lower()} vector for this player. Try switching to {other} search instead."` (where `other` is already computed nearby — check context) |

Lines 466-468, 489 already good — leave as-is.

- [ ] **Step 12: shot_map.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 192 | `"No shots for the selected filters."` | `"No shots found for this filter combination. Try selecting a different match or player."` |

- [ ] **Step 13: tactical_positions.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 385 | `"Error loading position timeline data."` | `"Something went wrong loading position data. Try refreshing the page."` |
| 390 | `"No position label data available for this match and team."` | `"No position data for this match and team. Try selecting a different match."` |
| 597 | `"Error loading formation labels."` | `"Something went wrong loading formations. Try refreshing the page."` |
| 602 | `"No formation label data available for this match and team."` | `"No formation data for this match and team. Try selecting a different match."` |
| 756 | `"Error loading player list."` | `"Something went wrong loading the player list. Try refreshing the page."` |
| 761 | `"No position map data available for this match and team."` | `"No position map data for this match and team. Try selecting a different team."` |
| 774 | `"No players available."` | `"No players available for this selection."` |
| 784 | `"Error loading position map data."` | `"Something went wrong loading position maps. Try refreshing the page."` |
| 789 | `"No position map data for the selected player."` | `"No position map data for this player. Try selecting a different player."` |

Leave "Select a..." prompts (371, 377, 583, 589, 741, 747) and the data-requirement message (898) as-is.

- [ ] **Step 14: team_shape.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 356 | `"No tracking data for the selected match and half."` | `"No tracking data for this match and half. Tracking requires Metrica, IDSSE, or SkillCorner matches."` |
| 392 | `"Failed to load tracking data."` | `"Something went wrong loading tracking data. Try refreshing the page."` |
| 519 | `"Failed to load tracking data."` | `"Something went wrong loading tracking data. Try refreshing the page."` |
| 524 | `"No tracking data for the selected match."` | `"No tracking data for this match. Try a Metrica, IDSSE, or SkillCorner match."` |
| 532 | `f"No tracking data for {team_side} team."` | `f"No tracking data for the {team_side} team in this match."` |
| 569 | `"Insufficient data to compute timeline."` | `"Not enough data points to compute the timeline. Try a different match or half."` |

Leave "Select a team" prompts (349, 507), the minimum-player message (397), and the data-requirement message (991) as-is.

- [ ] **Step 15: workflows_stats.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 668 | `"No SLAs configured"` | `"No SLAs configured yet."` |

- [ ] **Step 16: demo_space/app.py**

| Line | Current | Replacement |
|------|---------|-------------|
| 133 | `f"No player found matching '{selected_player}'"` | `f"No player found matching '{selected_player}'. Check the spelling or try a different name."` |
| 142 | `"Query player has zero embedding vector."` | `"This player has no embedding vector. They may not have enough match data."` |
| 225 | `"No shot data loaded."` | `"No shot data loaded. Try selecting a competition first."` |
| 238 | `"Missing coordinate columns."` | `"Shot coordinate data is missing for this selection."` |
| 311 | `"No pass data loaded."` | `"No pass data loaded. Try selecting a competition first."` |
| 502 | `"No tracking data loaded."` | `"No tracking data loaded. Tracking data is available for ~20 matches."` |
| 513 | `"No data for this frame."` | `"No data for this frame. Try a different frame number."` |
| 674 | `"No pressure data loaded."` | `"No pressure data loaded. Try selecting a competition and player."` |
| 696 | `f"No data for '{player_name}'."` | `f"No data found for '{player_name}'. Try selecting a different player."` |
| 805 | `"No PAUSA data available"` (title) | `"No PAUSA data available — try an IDSSE match"` |
| 819 | `"No data for selected filters"` (title) | `"No data for selected filters — try broadening selection"` |
| 876 | `"No PAUSA data available."` | `"No PAUSA data available. PAUSA is computed for 7 IDSSE matches."` |
| 888 | `"No receiver data for selection."` | `"No receiver data for this selection. Try a different match or pass filter."` |
| 893 | `"No receiver coordinates available."` | `"Receiver coordinates aren't available for this data source."` |
| 927 | `"No PAUSA data loaded."` | `"No PAUSA data loaded. PAUSA is available for 7 IDSSE matches."` |
| 931 | `"No data for this match."` | `"No data for this match. Try selecting a different match."` |

Line 128 already good — leave as-is.

- [ ] **Step 17: Verify all files**

Run: `uv run ruff check hf_taipy_app/src/state/ demo_space/app.py`
Run: `uv run pyright hf_taipy_app/src/state/`

---

## Task 6: Documentation Updates (V-2 + V-3 + V-4)

**Findings addressed:** V-2 (limitation front-loading), V-3 (reader ownership), V-4 (tone consistency)

**Files:**
- Modify: `docs/getting-started.md`
- Modify: `CONTRIBUTING.md`
- Modify: `ARCHITECTURE.md:820-834`
- Modify: `docs/glossary.md`

### Steps

- [ ] **Step 1: getting-started.md — Python 3.10 empathy lead + verification context**

Replace the prerequisites section (lines 5-9) with a front-loaded explanation:

```markdown
## Prerequisites

> **Python 3.10 specifically — not newer.** Databricks serverless only supports 3.10, so the project pins to it to catch version-specific issues locally before they hit production. If you're on 3.12+, you'll need to install 3.10 alongside it — `uv` handles the rest.

- [Git](https://git-scm.com/)
- [Python 3.10](https://www.python.org/downloads/) (strict: >=3.10, <3.11)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
```

Update the verification section (lines 28-46) to explain why each check exists:

```markdown
## 2. Verify the Environment

Run the same quality checks that CI enforces — if these pass locally, your PR will pass CI:

    # Lint — catches unused imports, security anti-patterns, naming issues
    uv run ruff check src/ scripts/

    # Format — ensures consistent code style across all contributors
    uv run ruff format --check src/ scripts/

    # Type check — catches type mismatches before runtime
    uv run pyright src/

    # Unit tests — verifies correctness of analytics and pipeline logic
    uv run pytest src/tests/ -x -q

**Verify:** All four commands exit with code 0. If `pyright` reports errors, run `uv sync` first — it installs the required type stubs.
```

- [ ] **Step 2: CONTRIBUTING.md — warm up tone, add context**

Replace the full file:

```markdown
# Contributing to (Right! Luxury!) Lakehouse

Thank you for your interest in contributing! The engineering standards are strict — four checks must pass before merge — but the tradeoff is zero-regression confidence. Once you're set up, the feedback loop is fast.

## Engineering Standards

All contributions follow the standards in [CLAUDE.md](CLAUDE.md). The key constraints:

- **Python 3.10** (strict: >=3.10, <3.11 — Databricks serverless constraint)
- **Line length**: 120 characters maximum
- **Type annotations**: All public function signatures

## Development Setup

See the [Getting Started guide](docs/getting-started.md) for local environment setup.

## Required Checks

These are the same gates CI runs — if they pass locally, your PR will pass CI:

```bash
uv run ruff check src/ scripts/           # Lint
uv run ruff format --check src/ scripts/  # Format
uv run pyright src/                       # Type check
uv run pytest src/tests/ -v               # Unit tests
```

If a check fails, the command output will tell you exactly what to fix. `ruff check --fix` can auto-fix most lint issues.

## Pull Request Process

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all checks pass
3. Write descriptive commit messages (see git history for style)
4. Open a PR with a clear title and description of what and why

## Questions?

Open a [GitHub Discussion](https://github.com/karsten-s-nielsen/luxury-lakehouse/discussions) or reach out via the project's [Hugging Face community](https://huggingface.co/luxury-lakehouse).
```

- [ ] **Step 3: ARCHITECTURE.md — Risk Register front-loading**

In `ARCHITECTURE.md`, add an introductory sentence before the Risk Register table at line 822:

```markdown
## 7. Risk Register

Some Terraform provider gaps and data constraints require manual workarounds that add operational friction. The table below tracks these risks and their mitigations.
```

- [ ] **Step 4: glossary.md — add "why you'd care" context to thin entries**

Update the following glossary entries to include practitioner context. The entries with Scale/Direction already serve this purpose well — focus on the `—` entries:

| Term | Addition to Definition |
|------|----------------------|
| **applyInPandas** | Append: `. Used in compute pipelines (pitch control, DEFCON, team shape) to distribute per-match analytics across Spark executors instead of bottlenecking the driver` |
| **Bronze / Silver / Gold** | Append: `. This lets you query raw data for debugging (Bronze), use standardized schemas for transforms (Silver), or build dashboards on pre-aggregated tables (Gold)` |
| **Delta Lake** | Append: `. This project relies on ACID transactions for idempotent writes (partition overwrite) and time travel for debugging pipeline issues` |
| **dbt** | Append: `. Chosen over Spark transforms because SQL is more accessible for analytics logic and dbt provides built-in testing, documentation, and lineage` |
| **EFPI** | Append: `. Used on the Tactical Positions page alongside Shape Graph — EFPI matches against known templates while Shape Graph discovers formations geometrically` |
| **EPTS** | Append: `. In this project, IDSSE (7 matches) and SkillCorner (10 matches) data comes from EPTS systems; StatsBomb and Wyscout are event-only` |
| **Football2vec** | Append: `. V2 (128-dim Transformer) is the current default on the Player Similarity page; V1 (32-dim Doc2Vec) is available as a fallback` |
| **GRL** | Append: `. Used in Football2vec V2 to prevent player embeddings from encoding which team a player belongs to, ensuring similarity reflects playing style, not squad membership` |
| **Lakebase** | Append: `. The Taipy dashboard reads from Lakebase (PostgreSQL) for sub-second page loads instead of querying Delta tables through Spark` |
| **SPADL** | Append: `. This lets you compare players from different leagues on equal terms — a Wyscout "smart pass" and a StatsBomb "through ball" become the same action type` |
| **Synced table** | Append: `. Important operational note: SNAPSHOT synced tables require manual refresh after dbt rebuilds (see scripts/refresh_synced_tables.py)` |
| **UC Volume** | Append: `. Stores model weights (xG, VAEP, Football2vec), Parquet exports for HF Hub upload, and build artifacts` |

- [ ] **Step 5: Verify docs**

Scan for broken markdown: `uv run ruff check` won't catch these, but a quick visual review of the changed sections is sufficient. No CI linter for markdown.

---

## Verification Checklist (Post-Implementation)

After all tasks complete:

- [ ] `uv run ruff check src/ scripts/` — 0 violations
- [ ] `uv run ruff format --check src/ scripts/` — 0 reformatted
- [ ] `uv run pyright src/` — 0 errors
- [ ] `uv run pytest src/tests/ -x -q` — all pass
- [ ] Grep for orphaned local colour constants that should have been removed
- [ ] Grep for any remaining bare `"Error"` stat card assignments
