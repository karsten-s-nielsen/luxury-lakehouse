# Taipy Parity Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 12 missing content items, 15 truncated text items, restore Plotly interactivity on 3 pages, and resolve minor bugs — achieving full content parity between Taipy and Streamlit.

**Architecture:** Changes are grouped into 6 semantic groups (G0 resolved — wrong venv). Each group targets specific layers: `template.py` (glossary/footer), `page_template.py` (content block types), `pages/*.py` (page configs), `state/*.py` (data + rendering). Groups execute in dependency order: G3 → G1 → G2 → G4 → G6 → G5.

**Tech Stack:** Taipy 4.1.1, Python 3.10, mplsoccer, Plotly (for G5), psycopg2, Lakebase PostgreSQL.

**Spec:** `docs/superpowers/specs/2026-03-22-taipy-parity-sweep-design.md`

**Runtime:** Taipy must be started with `taipy_spike/.venv/Scripts/python.exe src/main.py` (NOT `uv run`) from the `taipy_spike/` directory. Set env vars `LAKEBASE_HOST` and `LAKEBASE_ENDPOINT_NAME`.

**Verification:** Every task must be verified via MCP/Puppeteer screenshot against Chrome at `http://localhost:7860/<Page-Route>`.

---

## File Map

| File | Responsibility | Tasks |
|------|---------------|-------|
| `taipy_spike/src/template.py` | GLOSSARY dict, sidebar widget lists, footer, root page | T1, T2, T5 |
| `taipy_spike/src/page_template.py` | ContentBlock type, _build_content_block() renderer | T7 |
| `taipy_spike/src/pages/player_radar.py` | Player Comparison page config | T2 |
| `taipy_spike/src/pages/player_similarity.py` | Player Similarity page config | T2 |
| `taipy_spike/src/state/player_radar.py` | Player Comparison state (spoke caption already exists) | T2 |
| `taipy_spike/src/state/player_similarity.py` | Player Similarity state (spoke caption net-new) | T2 |
| `taipy_spike/src/pages/movement_analysis.py` | Movement & Pressing page config | T3 |
| `taipy_spike/src/state/movement_analysis.py` | Movement state (metric selector, table columns) | T3 |
| `taipy_spike/src/state/shot_map.py` | Shot Map state (chart title with player name) | T5 |
| `taipy_spike/src/state/defensive_valuation.py` | Defensive Impact state (top-10 caption, Plotly) | T5, T8 |
| `taipy_spike/src/pages/action_values.py` | Player Impact page config (scale notes) | T5 |
| `taipy_spike/src/pages/defensive_valuation.py` | Defensive Impact page config | T5, T8 |
| `taipy_spike/src/pages/pass_network.py` | Pass Network page config (empty condition fix) | T6, T8 |
| `taipy_spike/src/pages/pass_timing.py` | Pass Timing page config | T8 |
| `taipy_spike/src/state/pass_network.py` | Pass Network state (Plotly chart) | T8 |
| `taipy_spike/src/state/pass_timing.py` | Pass Timing state (Plotly charts) | T8 |
| `taipy_spike/src/filters.py` | Shared filter queries (competition sort) | T6 |
| All 12 `taipy_spike/src/pages/*.py` | Page descriptions with citations | T4 |

---

### Task 1: G3 — Footer Attribution and HuggingFace Links

**Files:**
- Modify: `taipy_spike/src/template.py:521-523`

- [ ] **Step 1: Add footer content to `build_root_page()`**

In `template.py`, the `ll-footer` part at lines 521-523 is empty:
```python
<|part|class_name=ll-footer|
|>
```

Replace with:
```python
<|part|class_name=ll-footer|
Soccer analytics powered by StatsBomb, Metrica Sports & Wyscout open data.

[Interactive Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) · [Published Datasets](https://huggingface.co/luxury-lakehouse)
|>
```

- [ ] **Step 2: Restart Taipy and verify via Puppeteer**

Navigate to `http://localhost:7860/Shot-Map`, scroll to bottom, screenshot. Verify footer text and links are visible.

---

### Task 2: G1 — Missing Widgets and Content Blocks (M1-M4)

**Files:**
- Modify: `taipy_spike/src/template.py:428-457` (add M2 slider to `_SEARCH_WIDGETS`)
- Modify: `taipy_spike/src/pages/player_radar.py` (add M1 spoke caption + M4 metrics guard ContentBlocks)
- Modify: `taipy_spike/src/state/player_radar.py` (add M4 `pr_metrics_hint` state var)
- Modify: `taipy_spike/src/pages/player_similarity.py` (add M3 spoke caption ContentBlock)
- Modify: `taipy_spike/src/state/player_similarity.py` (add `ps_spoke_caption` state var + computation)

- [ ] **Step 1: M2 — Add min matches slider to `_SEARCH_WIDGETS` in `template.py`**

At line 457 (before the closing `]` of `_SEARCH_WIDGETS`), add:
```python
    SidebarWidget(
        "slider", "ps_min_matches", "Min. matches", "on_ps_min_matches_change",
        slider_min="1", slider_max="50",
        slider_range_labels=("1", "50"),
    ),
```

Note: `ps_min_matches` is already in `__all__` at line 104 — do NOT add a duplicate.

- [ ] **Step 2: M1 — Add spoke caption ContentBlock to Player Comparison page config**

In `pages/player_radar.py`, in the `content` list, after the `pr_radar_image` image block (currently the 5th ContentRow), add a new ContentRow:
```python
ContentRow([ContentBlock("text", "pr_spoke_caption", condition="len(pr_spoke_caption) > 0")]),
```

- [ ] **Step 3: M4 — Add metrics hint state var and ContentBlock**

In `state/player_radar.py`:
- Add at module level (near line 67): `pr_metrics_hint: str = ""`
- Add to `__all__` (line 86-102): `"pr_metrics_hint"`
- In `pr_refresh()` (NOT `on_pr_metric_change` — that function just calls `pr_refresh` and the hint would be overwritten), add logic after the metric selection is resolved (~line 318-321): if `len(state.pr_selected_metrics) < 3`, set `state.pr_metrics_hint = "Select at least 3 metrics for a meaningful radar chart."`, else `state.pr_metrics_hint = ""`.

In `pages/player_radar.py`, add before the radar image block:
```python
ContentRow([ContentBlock("text", "pr_metrics_hint", condition="len(pr_metrics_hint) > 0")]),
```

The template renderer should render this as an `ll-info-box`. Check `_build_content_block()` — if `"text"` blocks don't get info-box styling, add a `css_class` field or use the existing condition mechanism.

- [ ] **Step 4: M3 — Add spoke caption to Player Similarity**

In `state/player_similarity.py`:
- Add at module level (near line 80): `ps_spoke_caption: str = ""`
- Add to `__all__` (line 91-118): `"ps_spoke_caption"`
- Import `_SPOKE_LEGEND` from `state.player_radar`: `from state.player_radar import _SPOKE_LEGEND`
- In `on_ps_selected_compare_change` (line 419-453), after the radar is rendered, add spoke caption computation. Note: there is no `radar_df` variable in this function — metric labels come from `_DEFAULT_METRICS`, not DataFrame columns. Mirror the pattern from `state/player_radar.py` line 360:
```python
# Build spoke legend caption (import _DEFAULT_METRICS from player_radar if not already available)
from state.player_radar import _DEFAULT_METRICS
if state.ps_radar_image:
    labels = [m[1] for m in _DEFAULT_METRICS]
    legend_parts = [f"**{lbl}** = {_SPOKE_LEGEND[lbl]}" for lbl in labels if lbl in _SPOKE_LEGEND]
    state.ps_spoke_caption = " · ".join(legend_parts) if legend_parts else ""
else:
    state.ps_spoke_caption = ""
```

In `pages/player_similarity.py`, add a ContentRow after the `ps_radar_image` block:
```python
ContentRow([ContentBlock("text", "ps_spoke_caption", condition="len(ps_spoke_caption) > 0")]),
```

- [ ] **Step 5: Restart Taipy and verify via Puppeteer**

1. Navigate to `http://localhost:7860/Player-Similarity` — verify min matches slider appears in Search sidebar section.
2. Select a player, get results, select comparison — verify spoke legend appears below radar.
3. Navigate to `http://localhost:7860/Player-Comparison` — select a competition, team, players. Verify spoke legend below radar. Deselect metrics to < 3 — verify info message appears.

---

### Task 3: G2 — Movement & Pressing Completeness (M5, M6)

**Files:**
- Modify: `taipy_spike/src/template.py` `_FILTER_WIDGETS` list (add metric selector SidebarWidget — NOT in `pages/movement_analysis.py`)
- Modify: `taipy_spike/src/state/movement_analysis.py:226-276` (metric selector logic + table columns)

- [ ] **Step 1: M5 — Add metric selector state and callback**

In `state/movement_analysis.py`, add at module level:
```python
ma_physical_metric: str = "Total Distance (km)"
ma_physical_metric_lov: list[str] = ["Total Distance (km)", "HSR Distance (m)", "Sprint Distance (m)"]
```

Add to `__all__`:
```python
"ma_physical_metric", "ma_physical_metric_lov", "on_ma_physical_metric_change",
```

Add metric-to-column mapping:
```python
_PHYSICAL_METRIC_MAP: dict[str, tuple[str, str, str]] = {
    "Total Distance (km)": ("total_distance_km", "Distance (km)", "Total Distance by Player"),
    "HSR Distance (m)": ("hsr_distance_m", "HSR (m)", "HSR Distance by Player"),
    "Sprint Distance (m)": ("sprint_distance_m", "Sprint (m)", "Sprint Distance by Player"),
}
```

Add callback:
```python
def on_ma_physical_metric_change(state: Any, var_name: str, var_value: Any) -> None:
    """Re-render physical bars when metric selector changes."""
    _refresh_physical(state)
```

Update `_refresh_physical()` at line 257 — replace the hardcoded call:
```python
# Old:
state.ma_physical_image = _render_physical_bars(stats, "total_distance_km", "Distance (km)", "Total Distance by Player")

# New:
metric_key = getattr(state, "ma_physical_metric", "Total Distance (km)")
col, label, title = _PHYSICAL_METRIC_MAP.get(metric_key, ("total_distance_km", "Distance (km)", "Total Distance by Player"))
state.ma_physical_image = _render_physical_bars(stats, col, label, title)
```

- [ ] **Step 2: M5 — Add metric selector SidebarWidget**

In `template.py` `_FILTER_WIDGETS` list (line ~275-299 has Movement-Pressing widgets), add a new widget after the `selected_tracking_match` widget (around line 299). The widget is visible only for Physical Performance sub-view:
```python
SidebarWidget(
    "dropdown", "ma_physical_metric", "Metric", "on_ma_physical_metric_change",
    lov="ma_physical_metric_lov",
    condition="current_page == 'Movement-Pressing' and selected_sub_view == 'Physical Performance'",
),
```

- [ ] **Step 3: M6 — Add High Accel/Decel to table column mapping**

In `state/movement_analysis.py` at lines 262-273, add the two missing columns to the rename dict:
```python
"sprint_frame_count": "Sprint Frames",
"high_accel_count": "High Accel",   # ADD
"high_decel_count": "High Decel",   # ADD
"avg_speed_ms": "Avg Speed (m/s)",
```

- [ ] **Step 4: Restart Taipy and verify via Puppeteer**

1. Navigate to `http://localhost:7860/Movement-Pressing` with Physical Performance sub-view.
2. Select a provider and tracking match.
3. Verify "Metric" dropdown appears with 3 options.
4. Switch between Total Distance, HSR, Sprint — verify chart title and bars change.
5. Expand "Full Stats" table — verify "High Accel" and "High Decel" columns are present.

---

### Task 4: G4a+G4b — Glossary Definitions and Page Descriptions

**Files:**
- Modify: `taipy_spike/src/template.py:33-62` (GLOSSARY dict)
- Modify: All 12 `taipy_spike/src/pages/*.py` (description fields)

- [ ] **Step 1: Restore full glossary definitions in `template.py`**

Update these entries in the GLOSSARY dict (lines 33-62):

```python
"xG (Expected Goals)": "Probability of scoring from each shot's location and context. Higher = better chance. Sum over a match = team's expected output.",
"Brier Score": "Prediction calibration metric. 0.0 = perfect, 0.25 = coin flip. Lower is better. Good models score < 0.10.",
"PPDA": "Passes Per Defensive Action — measures pressing intensity. Lower = more aggressive pressing. Range: 5\u201315.",
"Temporal Judgment": "Was the pass released at the optimal moment? Ratio of actual OBSO at release to peak OBSO in the \u00b13s/+1s window. 1.0 = perfect timing.",
"DEFCON": "Defensive Contribution framework (Kim et al. 2025). Quantifies how defenders affect an attacker's scoring probability via four credit categories.",
"Line-Breaking Pass": "A pass that penetrates at least one defensive line, detected via Ward clustering on StatsBomb 360 freeze-frame defender positions.",
"Progressive Pass": "A pass moving the ball significantly closer to the opponent's goal \u2014 defined by a minimum distance threshold toward the goal line.",
```

Also add a new entry for "Most Active Zone" (missing from Taipy GLOSSARY entirely — used in Heat Map metrics):
```python
"Most Active Zone": "The 3x3 pitch zone (e.g., 'Att Center') with the highest action count.",
```

Leave all other definitions unchanged. Keep the Taipy additions (Off. VAEP/90, Def. VAEP/90, Goals/90, etc.).

- [ ] **Step 2: Restore page descriptions with paper title quotes**

Update the `description` field in each page's `PageConfig`:

**`pages/shot_map.py`**:
```python
description='Shot locations sized by xG. xG methodology per Rathke (2017) "An examination of expected goals and shot efficiency." Custom model via XGBoost with isotonic calibration.',
```

**`pages/heat_map.py`**:
```python
description='Action density visualization using bin statistics. Spatial analysis approach per Anzer & Bauer (2021) "A goal scoring probability model based on tracking data." Rendered via mplsoccer.',
```

**`pages/pass_network.py`**:
```python
description='Network analysis of passing connections per Pena & Touchette (2012) "A network theory analysis of football strategies." Wyscout matches do not include pass recipient data.',
```

**`pages/pitch_control.py`**:
```python
description='Physics-based pitch control model by Spearman (2017) "Beyond Expected Goals." Voronoi baseline also available. Tracking data available for ~20 matches from Metrica, IDSSE, and SkillCorner.',
```

**`pages/pass_timing.py`**:
```python
description='PAUSA: Passing Ability Under Spatiotemporal Awareness. Composite of temporal judgment (when) \u00d7 spatial selection (where). Lee, Jo, Hong, Bauer & Ko (2026), MIT Sloan 2026.',
```

**`pages/defensive_valuation.py`**:
```python
description='How much defensive attention does each attacker attract? Tier 3 (tabular heuristic, no GNN) approximation of the DEFCON framework. Credits: Intercept, Concede, Disturb, Deter. Tiers: 1 = full GNN, 2 = simplified GNN, 3 = tabular heuristic (this implementation).',
```

**`pages/match_summary.py`**:
```python
description='Match scorecard with Expected Goals (xG) per Rathke (2017). Pressing intensity via PPDA (Trainor & Chassy 2021).',
```

**`pages/action_values.py`**:
```python
description='Valuing Actions by Estimating Probabilities (VAEP) \u2014 Decroos et al. (2019). Implemented via socceraction.',
```

**`pages/player_radar.py`**:
```python
description='Multi-metric player comparison using mplsoccer radar chart. Metrics from VAEP (Decroos et al. 2019) and tracking data.',
```

**`pages/player_similarity.py`**:
```python
description='Find similar players using pgvector cosine distance on behavioral (32-d) or statistical (13-d) embedding vectors. Behavioral embeddings via Theiner et al. (2022) football2vec with Doc2Vec (Le & Mikolov 2014). Model: luxury-lakehouse/football2vec-statsbomb-wyscout.',
```

**`pages/movement_analysis.py`**:
```python
description='Off-Ball xT combines pitch control (Spearman 2017) with Expected Threat zones (Karun Singh 2018). Physical metrics from tracking data.',
```

- [ ] **Step 3: Restart Taipy and verify via Puppeteer**

Navigate to each page and verify description text includes paper title quotes and citations. Spot-check: Shot Map, Heat Map, Defensive Impact, Pass Timing.

---

### Task 5: G4c+G4d — Missing Captions, Labels, and Help Text

**Files:**
- Modify: `taipy_spike/src/state/shot_map.py:152,315` (M9 chart title)
- Modify: `taipy_spike/src/pages/action_values.py` (M11 scale notes)
- Modify: `taipy_spike/src/state/defensive_valuation.py` (M12 top-10 caption)
- Modify: Various `pages/*.py` for help text alignment

- [ ] **Step 1: M9 — Player name in Shot Map chart title**

In `state/shot_map.py`, update `_render_pitch` signature at line 152:
```python
def _render_pitch(shots: pd.DataFrame, xg_col: str, player_name: str | None = None) -> str:
```

Update the title at line 156:
```python
title = f"Shot Map \u2014 {player_name}" if player_name else "Shot Map"
ax.set_title(title, color=PITCH_LINE_COLOR, fontsize=14, pad=10)
```

Update the call site in `sm_refresh` (line 315) to pass the player name. `state.selected_player` holds the human-readable label string (e.g., "Lionel Messi") or `"All"` when no player is selected. Guard against the "All" case:
```python
player_name = state.selected_player if state.selected_player not in (None, "All") else None
state.sm_pitch_image = _render_pitch(plot_shots, "statsbomb_xg", player_name=player_name)
```

- [ ] **Step 2: M11 — Expand Player Impact scale notes**

In `pages/action_values.py`, find the Rankings sub-view's `scale_notes` and expand it:
```python
scale_notes=[
    "VAEP/90: higher = more impactful (typical range 0.01\u20131.0).",
    "Off. VAEP/90: offensive contribution per 90 min. Def. VAEP/90: defensive contribution per 90 min.",
],
```

- [ ] **Step 3: M12 — Defensive Impact top-10 caption**

In `state/defensive_valuation.py`, where the breakdown data is trimmed to top 10 matches, add a caption state var. Find the breakdown callback where `breakdown = breakdown.head(10)` or similar, and add:
```python
if len(full_breakdown) > 10:
    state.dv_breakdown_caption = f"Showing top 10 of {len(full_breakdown)} matches."
else:
    state.dv_breakdown_caption = ""
```

Add `dv_breakdown_caption: str = ""` at module level and to `__all__`.

In `pages/defensive_valuation.py`, add a caption to the Breakdown sub-view's content:
```python
ContentRow([ContentBlock("text", "dv_breakdown_caption", condition="len(dv_breakdown_caption) > 0")]),
```

- [ ] **Step 4: Help text alignment across pages**

Restore truncated help text strings in page Metric definitions:

**`pages/match_summary.py`** — Home xG / Away xG `help_text`:
```python
help_text="Probability of scoring from each shot's location and context. Higher = better chance. Sum over a match = team's expected output.",
```

**`pages/heat_map.py`** — fix "heat map scope" → "selected scope" in metric help text for Passes and Shots.

**`pages/pass_timing.py`** — Temporal Judgment metric `help_text`:
```python
help_text="Was the pass released at the optimal moment? Ratio of actual OBSO at release to peak OBSO in the \u00b13s/+1s window. 1.0 = perfect timing.",
```

**`pages/pass_timing.py`** — DFL caption: find `pt_dfl_caption` text and restore full version:
```python
"Player names shown as DFL identifiers \u2014 IDSSE tracking data does not include player names. Human-readable names require a DFL roster lookup (not yet available)."
```

- [ ] **Step 5: Restart Taipy and verify via Puppeteer**

1. Shot Map: select a competition, team, and player — verify chart title shows "Shot Map — {player_name}".
2. Player Impact Rankings: verify scale notes below table include Off/Def VAEP/90 explanation.
3. Defensive Impact Breakdown: select a player — verify "Showing top 10 of N matches" caption appears.
4. Pass Timing: hover over Temporal Judgment metric info icon — verify window spec in tooltip.

---

### Task 6: G6 — Minor Fixes

**Files:**
- Modify: `taipy_spike/src/pages/pass_network.py:17` (empty condition)
- Modify: `taipy_spike/src/filters.py:47` (competition sort order)
- Modify: `taipy_spike/src/state/player_similarity.py` (M10 empty lov warning)

- [ ] **Step 1: Fix Pass Network empty condition**

In `pages/pass_network.py` at line 17, change:
```python
empty_condition="selected_competition is None",
```
to:
```python
empty_condition="selected_competition is None or selected_team is None or selected_match is None",
```

- [ ] **Step 2: Fix competition sort order**

In `filters.py` at line 47, change:
```python
f"FROM {tbl} ORDER BY competition_name LIMIT 50"
```
to:
```python
f"FROM {tbl} ORDER BY country, competition_name LIMIT 50"
```

- [ ] **Step 3: M10 — Add empty lov warning for Player Similarity**

In `state/player_similarity.py`, in the function that loads the player list (`_load_player_list` or equivalent), after the query, add:
```python
if not players:
    state.ps_warning_text = "No players with embeddings found."
    return
```

- [ ] **Step 4: Restart Taipy and verify via Puppeteer**

1. Pass Network: select only competition (not team/match) — verify blue info box still shows "Select a competition, team, and match to begin."
2. Shot Map: open competition dropdown — verify competitions sorted by country first.
3. Player Similarity: (hard to test empty lov without data setup — verify code change is correct).

---

### Task 7: G5 Infrastructure — Add HTML Content Block Type

**Files:**
- Modify: `taipy_spike/src/page_template.py:241,372-416`

- [ ] **Step 1: Update ContentBlock Literal type**

At line 241 in `page_template.py`, change:
```python
kind: Literal["image", "table", "text", "expandable_table"]
```
to:
```python
kind: Literal["image", "table", "text", "expandable_table", "html"]
```

- [ ] **Step 2: Add HTML rendering branch in `_build_content_block()`**

In `_build_content_block()` at lines 372-416, add a new branch after the `expandable_table` handler:
```python
elif block.kind == "html":
    parts.append(f"<|{{{block.var}}}|html|>")
```

- [ ] **Step 3: Verify Pyright passes**

Run: `cd taipy_spike && .venv/Scripts/python.exe -m pyright src/page_template.py` (or equivalent type check). No new errors expected.

---

### Task 8: G5 — Plotly Interactivity Restoration (3 Pages)

**Files:**
- Modify: `taipy_spike/src/state/pass_network.py:139-221` (Plotly network chart)
- Modify: `taipy_spike/src/pages/pass_network.py:15` (image → html ContentBlock)
- Modify: `taipy_spike/src/state/pass_timing.py:209-355` (Plotly scatter + heatmap)
- Modify: `taipy_spike/src/pages/pass_timing.py:22-23` (image → html ContentBlocks)
- Modify: `taipy_spike/src/state/defensive_valuation.py:408-475` (Plotly grouped bar)
- Modify: `taipy_spike/src/pages/defensive_valuation.py:35` (image → html ContentBlock)

**Dependency:** Task 7 must be completed first (HTML content block type).

- [ ] **Step 0: Smoke-test Plotly HTML embed in Taipy**

Before building all 3 charts, test that `<|{var}|html|>` works with Plotly output. Temporarily add a hardcoded test in any state module:
```python
import plotly.graph_objects as go
fig = go.Figure(data=go.Bar(x=["A", "B"], y=[1, 2]))
fig.update_layout(height=200, paper_bgcolor="rgba(0,0,0,0)")
test_html = fig.to_html(include_plotlyjs="cdn", full_html=False)
```
Verify it renders interactively (hover works) via Puppeteer. If CSP or sandboxing blocks it, stop and report. Remove the test code after verification.

- [ ] **Step 1: Add Plotly to Taipy spike requirements**

In `taipy_spike/requirements.txt`, add:
```
plotly==6.1.2
```

Then install: `cd taipy_spike && .venv/Scripts/pip.exe install plotly==6.1.2`

- [ ] **Step 2: Pass Network — Replace matplotlib with Plotly**

In `state/pass_network.py`:
- Add imports: `import plotly.graph_objects as go`
- Replace `_render_network()` (lines 139-221) with a new function that builds a Plotly figure:

```python
def _render_network_html(nodes: pd.DataFrame, edges: pd.DataFrame) -> str:
    """Build interactive Plotly pass network and return HTML string."""
    if nodes.empty:
        return ""

    fig = go.Figure()

    # Add edges as lines
    for _, edge in edges.iterrows():
        src = nodes[nodes["player_id"] == edge["passer_id"]].iloc[0]
        tgt = nodes[nodes["player_id"] == edge["receiver_id"]].iloc[0]
        width = 1 + (edge["pair_count"] / edges["pair_count"].max()) * 6
        fig.add_trace(go.Scatter(
            x=[src["avg_x"], tgt["avg_x"]], y=[src["avg_y"], tgt["avg_y"]],
            mode="lines", line=dict(width=width, color="rgba(255,255,255,0.3)"),
            hoverinfo="text",
            text=f"{src['player_name']} → {tgt['player_name']}: {edge['pair_count']} passes",
            showlegend=False,
        ))

    # Add nodes as scatter
    sizes = 8 + (nodes["pass_count"] - nodes["pass_count"].min()) / max(nodes["pass_count"].max() - nodes["pass_count"].min(), 1) * 30
    fig.add_trace(go.Scatter(
        x=nodes["avg_x"], y=nodes["avg_y"],
        mode="markers+text", marker=dict(size=sizes, color="#f59e0b"),
        text=nodes["player_name"], textposition="top center",
        textfont=dict(color="white", size=10),
        hovertemplate="%{text}<br>Passes: %{customdata}<extra></extra>",
        customdata=nodes["pass_count"],
        showlegend=False,
    ))

    fig.update_layout(
        title="Pass Network", title_font_color="white",
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(range=[0, 120], showgrid=False, zeroline=False, visible=False),
        yaxis=dict(range=[0, 80], showgrid=False, zeroline=False, visible=False, scaleanchor="x"),
        margin=dict(l=10, r=10, t=40, b=10), height=500,
        font=dict(color="white"),
    )

    return fig.to_html(include_plotlyjs="cdn", full_html=False)
```

Update the call site in `pn_refresh` (line 295):
```python
# Old: state.pn_pitch_image = _render_network(nodes, edges)
state.pn_pitch_html = _render_network_html(nodes, edges)
```

Add `pn_pitch_html: str = ""` at module level and to `__all__`. Remove `pn_pitch_image` from `__all__` and the module-level declaration (it's replaced, not supplemented). Do the same cleanup for `pt_scatter_image`, `pt_heatmap_image`, and `dv_breakdown_image` in their respective state modules after replacing with HTML equivalents.

In `pages/pass_network.py`, replace the content block:
```python
# Old: ContentRow([ContentBlock("image", "pn_pitch_image")])
ContentRow([ContentBlock("html", "pn_pitch_html", condition="pn_total_passes != '--'")])
```

- [ ] **Step 3: Pass Timing — Replace matplotlib with Plotly**

In `state/pass_timing.py`:
- Add imports: `import plotly.express as px`, `import plotly.graph_objects as go`
- Replace `_build_scatter_plot()` (lines 209-279) with Plotly version:

```python
def _build_scatter_html(df: pd.DataFrame) -> str:
    """Build interactive Plotly scatter: When vs Where."""
    if df.empty:
        return ""
    fig = px.scatter(
        df, x="temporal_judgment", y="spatial_selection",
        size="pausa_score", color="team",
        hover_data={"temporal_judgment": ":.3f", "spatial_selection": ":.3f", "pausa_score": ":.3f"},
        title="Pass Timing: When vs Where (bubble size = PAUSA score)",
        labels={
            "temporal_judgment": "Temporal Judgment (when, 0\u20131, higher = better)",
            "spatial_selection": "Spatial Selection (where, 0\u20131, higher = better)",
        },
    )
    fig.update_layout(
        xaxis_range=[0, 1], yaxis_range=[0, 1],
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"), height=450,
    )
    # Add quadrant annotations
    fig.add_annotation(x=0.25, y=0.75, text="Good timing,<br>wrong target", showarrow=False, font=dict(color="gray", size=10))
    fig.add_annotation(x=0.75, y=0.75, text="Right time,<br>right place", showarrow=False, font=dict(color="gray", size=10))
    fig.add_annotation(x=0.25, y=0.25, text="Poor timing,<br>wrong target", showarrow=False, font=dict(color="gray", size=10))
    fig.add_annotation(x=0.75, y=0.25, text="Poor timing,<br>good target", showarrow=False, font=dict(color="gray", size=10))
    return fig.to_html(include_plotlyjs="cdn", full_html=False)
```

- Replace `_build_heatmap()` (lines 282-355) with Plotly version:

```python
def _build_heatmap_html(df: pd.DataFrame) -> str:
    """Build interactive Plotly OBSO heatmap."""
    if df.empty:
        return ""
    fig = px.density_heatmap(
        df, x="receiver_x", y="receiver_y", z="obso_at_receiver",
        histfunc="avg", nbinsx=24, nbinsy=16,
        color_continuous_scale="YlOrRd",
        title="OBSO at Receiver Location",
        labels={"obso_at_receiver": "Avg OBSO (0\u20131, higher = more open space)"},
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"), height=450,
        xaxis_title="Pitch X (m)", yaxis_title="Pitch Y (m)",
    )
    return fig.to_html(include_plotlyjs="cdn", full_html=False)
```

Update call sites (lines 482-483):
```python
state.pt_scatter_html = _build_scatter_html(passes_df)
state.pt_heatmap_html = _build_heatmap_html(passes_df)
```

Add `pt_scatter_html: str = ""` and `pt_heatmap_html: str = ""` at module level and to `__all__`.

In `pages/pass_timing.py`, replace content blocks:
```python
# Old:
ContentBlock("image", "pt_scatter_image")
ContentBlock("image", "pt_heatmap_image")
# New:
ContentBlock("html", "pt_scatter_html", condition="len(pt_scatter_html) > 0")
ContentBlock("html", "pt_heatmap_html", condition="len(pt_heatmap_html) > 0")
```

- [ ] **Step 4: Defensive Impact Breakdown — Replace matplotlib with Plotly**

In `state/defensive_valuation.py`:
- Add imports: `import plotly.express as px`
- Replace `_render_breakdown_chart()` (lines 408-475) with Plotly version:

```python
def _render_breakdown_html(breakdown: pd.DataFrame, player_name: str) -> str:
    """Build interactive Plotly grouped bar for pressure breakdown."""
    if breakdown.empty:
        return ""
    credit_cols = ["intercept_pressure", "concede_pressure", "disturb_pressure", "deter_pressure"]
    labels = ["Intercept", "Concede", "Disturb", "Deter"]
    # Guard against null match_label (same as matplotlib version)
    label_col = "match_label" if breakdown["match_label"].notna().all() else "match_id"
    plot_data = breakdown.head(10).melt(
        id_vars=[label_col], value_vars=credit_cols,
        var_name="Credit Type", value_name="Pressure",
    )
    plot_data["Credit Type"] = plot_data["Credit Type"].map(dict(zip(credit_cols, labels)))
    fig = px.bar(
        plot_data, x=label_col, y="Pressure", color="Credit Type",
        barmode="group", title=f"Pressure Breakdown: {player_name}",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"), height=450,
        xaxis_title="", yaxis_title="Pressure Credits",
    )
    return fig.to_html(include_plotlyjs="cdn", full_html=False)
```

Update call site:
```python
state.dv_breakdown_html = _render_breakdown_html(breakdown, player_name)
```

Add `dv_breakdown_html: str = ""` at module level and to `__all__`.

In `pages/defensive_valuation.py`, in the Breakdown sub-view content, replace:
```python
# Old: ContentBlock("image", "dv_breakdown_image")
ContentBlock("html", "dv_breakdown_html", condition="len(dv_breakdown_html) > 0")
```

- [ ] **Step 5: Restart Taipy and verify all 3 Plotly pages via Puppeteer**

1. Pass Network: select competition, team, match — verify interactive chart with hover tooltips.
2. Pass Timing: select a match — verify interactive scatter (hover shows pass details) and heatmap (hover shows OBSO values).
3. Defensive Impact Breakdown: select a player — verify interactive grouped bar with hover on credit categories.

Screenshot all three pages.

---

## Execution Notes

- **Task ordering**: T1 → T2 → T3 → T4 → T5 → T6 → T7 → T8 (T7 must precede T8)
- **Parallelizable**: T4 (text only) can run in parallel with T2/T3 since they don't share files (except `template.py` GLOSSARY vs sidebar widgets — coordinate carefully)
- **Restart frequency**: Taipy requires restart for code changes. Batch related changes before restarting.
- **Startup command**: `cd taipy_spike && LAKEBASE_HOST="ep-spring-rain-d2i6lozx.database.us-east-1.cloud.databricks.com" LAKEBASE_ENDPOINT_NAME="projects/soccer-analytics-dev/branches/production/endpoints/primary" PYTHONPATH=src .venv/Scripts/python.exe src/main.py`
