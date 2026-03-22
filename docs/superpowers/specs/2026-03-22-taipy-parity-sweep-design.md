# Taipy Parity Sweep — Final Content Fixes

**Date**: 2026-03-22
**Branch**: `spike/taipy-proof-of-concept`
**Context**: Systematic 12-page comparison (Streamlit vs Taipy) identified 12 missing content items, 15 truncated text items, and 3 pages needing Plotly interactivity restoration. This is the final sweep before potentially switching to Taipy.

## Verification Protocol

Every group must be verified via MCP/Puppeteer against Chrome before claiming done. The Taipy app must be running and rendering correctly.

## Execution Order

```
G0 (runtime fix) → G3 (footer) → G1 (widgets) → G2 (movement) → G4 (text) → G6 (minor) → G5 (Plotly)
```

---

## G0: Runtime Fix (Prerequisite)

**Problem**: Flask-SocketIO `AttributeError: can't set attribute 'session'` prevents all dynamic rendering.
**Root cause**: Dependency version conflict between `flask-socketio`, `flask`, and/or `werkzeug`.
**Fix**: Pin compatible versions or update to resolve the session attribute conflict.
**Files**: `taipy_spike/requirements.txt` or `pyproject.toml` Taipy extras.
**Verification**: Navigate to `http://localhost:7860/Shot-Map` in Puppeteer — page must render sidebar, title, and empty state message.

---

## G1: Missing Widgets & Content Blocks

**Items**: M1, M2, M3, M4

### M1 + M3: Spoke legend below radar charts

**Player Comparison** (`pages/player_radar.py`, `state/player_radar.py`):
- State already computes `pr_spoke_caption`. Add `ContentBlock("text", "pr_spoke_caption", condition="len(pr_spoke_caption) > 0")` after the radar image block in the page config's content list.

**Player Similarity** (`pages/player_similarity.py`, `state/player_similarity.py`):
- `ps_spoke_caption` does NOT exist in state — this is net-new work:
  - `state/player_similarity.py`: Add `ps_spoke_caption: str = ""` module-level declaration and add to `__all__`
  - Import or duplicate `_SPOKE_LEGEND` dict from `state/player_radar.py`
  - Compute `ps_spoke_caption` inside `on_ps_selected_compare_change` (where the comparison radar is rendered), using the same ` · `.join pattern as `pr_spoke_caption`
- `pages/player_similarity.py`: Add `ContentBlock("text", "ps_spoke_caption", condition="len(ps_spoke_caption) > 0")` after the radar image block.

### M2: Min. matches slider (Player Similarity)

- `template.py` (NOT `pages/player_similarity.py`): Add to `_SEARCH_WIDGETS` list (line ~428):
  ```python
  SidebarWidget("slider", "ps_min_matches", "Min. matches", "on_ps_min_matches_change",
                slider_min="1", slider_max="50", slider_range_labels=("1", "50"))
  ```
- State var `ps_min_matches` and callback `on_ps_min_matches_change` already exist in `state/player_similarity.py`.

### M4: "Select at least 3 metrics" guard (Player Comparison)

- `state/player_radar.py`: When `len(selected_metrics) < 3`, set `state.pr_metrics_hint = "Select at least 3 metrics for a meaningful radar chart."`, else `""`.
- `pages/player_radar.py`: Add `ContentBlock("text", "pr_metrics_hint", condition="len(pr_metrics_hint) > 0")` rendered as `ll-info-box`.

**Files touched**: `template.py` (M2 slider), `pages/player_radar.py`, `pages/player_similarity.py`, `state/player_radar.py`, `state/player_similarity.py`

---

## G2: Movement & Pressing Completeness

**Items**: M5, M6

### M5: Physical metric selector

- `pages/movement_analysis.py`: Add `SidebarWidget("dropdown", "ma_physical_metric", "Metric", "ma_on_metric_change", lov="ma_physical_metric_lov")` visible when sub-view is "Physical Performance".
- `state/movement_analysis.py`:
  - Add `ma_physical_metric_lov = ["Total Distance (km)", "HSR Distance (m)", "Sprint Distance (m)"]`
  - Add `ma_physical_metric = "Total Distance (km)"` default state
  - Update `_refresh_physical()` to map selected metric to column name and pass to `_render_physical_bars()`
  - Map: `{"Total Distance (km)": "total_distance_km", "HSR Distance (m)": "hsr_distance_m", "Sprint Distance (m)": "sprint_distance_m"}`

### M6: Missing table columns

- `state/movement_analysis.py`: Add `high_accel_count` → "High Accel" and `high_decel_count` → "High Decel" to the physical stats table column rename mapping (between "Sprint Frames" and "Avg Speed (m/s)").

**Files touched**: `pages/movement_analysis.py`, `state/movement_analysis.py`

---

## G3: Global Elements

**Items**: M7, M8

### M7 + M8: Footer attribution and HuggingFace links

- `template.py` (inside `build_root_page()`, NOT `page_template.py`): The `ll-footer` part exists but is empty. Add content:
  ```
  Soccer analytics powered by StatsBomb, Metrica Sports & Wyscout open data.
  [Interactive Demo](https://huggingface.co/spaces/luxury-lakehouse/soccer-analytics-demo) · [Published Datasets](https://huggingface.co/luxury-lakehouse)
  ```
- Render as Taipy markdown inside the existing `<|part|class_name=ll-footer|>` block.

**Files touched**: `template.py`

---

## G4: Glossary & Description Text

**Items**: All 15 truncation items + M9, M11, M12

### G4a: Glossary definitions (`template.py`)

Restore full definitions to match Streamlit while keeping Taipy improvements:

| Term | Change |
|------|--------|
| xG (Expected Goals) | Append: "Sum over a match = team's expected output." |
| Brier Score | Append: "Good models score < 0.10." |
| Line-Breaking Pass | Restore: "...detected via Ward clustering on StatsBomb 360 freeze-frame defender positions." |
| Progressive Pass | Restore: "...defined by a minimum distance threshold toward the goal line." |
| Temporal Judgment | Restore: "...in the ±3s/+1s window." |
| PPDA | Merge: keep "Range: 5-15" (Taipy improvement) + restore "measures pressing intensity" |
| DEFCON | Restore: "quantifies how defenders affect an attacker's scoring probability via four credit categories." |
| Most Active Zone | Restore: "3x3 pitch zone" and example "(e.g., 'Att Center')" |

### G4b: Page descriptions (all 12 `pages/*.py`)

Restore paper title quotes and inline details:

| Page | File | Change |
|------|------|--------|
| Shot Map | `pages/shot_map.py` | Add: *"An examination of expected goals and shot efficiency."* |
| Heat Map | `pages/heat_map.py` | Add: *"A goal scoring probability model based on tracking data."* |
| Pass Network | `pages/pass_network.py` | Add: *"A network theory analysis of football strategies."* |
| Pitch Control | `pages/pitch_control.py` | Add: *"Beyond Expected Goals."* |
| Pass Timing | `pages/pass_timing.py` | Add "MIT Sloan 2026" venue, restore "x" → "×", restore full DFL caption |
| Defensive Impact | `pages/defensive_valuation.py` | Restore: "How much defensive attention does each attacker attract?" |
| Match Summary | `pages/match_summary.py` | Restore "per Rathke (2017)" inline |
| Player Impact | `pages/action_values.py` | Restore "Decroos et al. (2019)" and "socceraction" inline |
| Player Comparison | `pages/player_radar.py` | Restore "mplsoccer" inline |
| Player Similarity | `pages/player_similarity.py` | Restore Doc2Vec/football2vec model ref |
| Movement & Pressing | `pages/movement_analysis.py` | Restore inline citation links |
| Heat Map | `pages/heat_map.py` | Add mplsoccer mention |

### G4c: Missing captions & labels

| Item | File | Change |
|------|------|--------|
| M9 | `state/shot_map.py` | Add player name to chart title: `_render_pitch()` currently hardcodes `ax.set_title("Shot Map", ...)` and accepts no player name parameter. Fix: add `player_name: str | None` parameter to `_render_pitch()`, pass it from `sm_refresh()` (which already has the player filter state), and conditionally set title to `f"Shot Map — {player_name}"` when not None. |
| M11 | `pages/action_values.py` | Expand `scale_notes` to include Off. VAEP/90 and Def. VAEP/90 explanations |
| M12 | `state/defensive_valuation.py` | Add "Showing top 10 of N matches." caption state var |

### G4d: Help text alignment

Restore truncated metric help text across pages:
- Match Summary: Home/Away xG help → full GLOSSARY definition
- Heat Map: "selected scope" not "heat map scope"
- Movement: PPDA help → full definition
- Pass Timing: Temporal Judgment help → include window spec
- Pass Timing DFL caption: restore "Human-readable names require a DFL roster lookup (not yet available)."

**Files touched**: `template.py`, all 12 `pages/*.py`, `state/shot_map.py`, `state/defensive_valuation.py`

---

## G5: Plotly Interactivity Restoration

**Items**: 3 pages back to interactive Plotly

### Template infrastructure

- `page_template.py`: Two changes required:
  1. Update `ContentBlock.kind` Literal type to include `"html"`: `Literal["image", "table", "text", "expandable_table", "html"]`
  2. Add `elif block.kind == "html":` branch in `_build_content_block()` that renders `<|{var_name}|html|>`
- **Important**: HTML blocks from `fig.to_html()` are never empty strings (always contain boilerplate). Each HTML ContentBlock MUST have an explicit `condition=` parameter that checks whether the underlying data is populated (e.g., `condition="len(pn_nodes_data) > 0"`), not relying on the default `len(var) > 0` guard which would always be true.

### Pass Network (`state/pass_network.py`, `pages/pass_network.py`)

- Build Plotly figure: scatter for nodes (sized by pass count) + lines for edges (weighted by pair count) + hover tooltips with player names and pass counts.
- `fig.to_html(include_plotlyjs="cdn", full_html=False)` → `state.pn_pitch_html`
- Page config: replace `ContentBlock("image", "pn_pitch_image")` with `ContentBlock("html", "pn_pitch_html", condition="len(pn_nodes_data) > 0")`

### Pass Timing (`state/pass_timing.py`, `pages/pass_timing.py`)

Two charts:
- **Scatter** (When vs Where): `px.scatter()` with bubble size = PAUSA, team color, quadrant annotations. Keep Taipy's improved axis labels ("0-1, higher = better").
- **Density heatmap** (OBSO): `px.density_heatmap()` with `histfunc="avg"`, 24x16 bins, YlOrRd. Keep Taipy's improved colorbar label.
- Both → `fig.to_html(...)` → `state.pt_scatter_html`, `state.pt_heatmap_html`
- Page config: replace image blocks with html blocks

### Defensive Impact Breakdown (`state/defensive_valuation.py`, `pages/defensive_valuation.py`)

- Build Plotly grouped bar: `px.bar(barmode="group")` with credit categories (Intercept, Concede, Disturb, Deter) per match.
- `fig.to_html(...)` → `state.dv_breakdown_html`
- Page config: replace image block with html block

### Migration path to Taipy native charts

The state layer returns **data + config** (DataFrames, column names, axis labels). The Plotly figure construction is isolated in rendering functions. To migrate to Taipy native `<|chart|>` later:
1. Keep data preparation unchanged in `state/*.py`
2. Replace Plotly figure construction with Taipy chart properties
3. Replace `ContentBlock("html", ...)` with `ContentBlock("chart", ...)` and a new template handler

No throwaway work — the data pipeline is the same regardless of rendering backend.

**Files touched**: `page_template.py`, `state/pass_network.py`, `state/pass_timing.py`, `state/defensive_valuation.py`, `pages/pass_network.py`, `pages/pass_timing.py`, `pages/defensive_valuation.py`

---

## G6: Minor Fixes

### Pass Network empty condition bug
- `pages/pass_network.py`: Change `empty_condition` to check all 3 filters (competition AND team AND match), not just competition.

### Competition sort order
- `filters.py`: Change `ORDER BY competition_name` to `ORDER BY country, competition_name` to match Streamlit.

### M10: Player Similarity empty lov warning
- `state/player_similarity.py`: When player lov query returns empty, set `ps_warning_text = "No players with embeddings found."` instead of silent empty dropdown.

### Wording standardization (optional polish)
- Align "No X for..." to "No X found for..." across all warning messages in state files (6-8 state files).

**Files touched**: `pages/pass_network.py`, `filters.py`, `state/player_similarity.py`, scattered state files

---

## Risk Notes

- **G0 is blocking**: If the Flask-SocketIO issue can't be resolved by pinning versions, Taipy may need a version upgrade or downgrade. Check `taipy==4.x` compatibility matrix.
- **G5 Plotly HTML embed**: Taipy's `<|html|>` block may have CSP or rendering limitations. Test with a simple Plotly chart first before building all 3.
- **G4 is high-volume but low-risk**: Pure text changes, no logic. Good candidate for parallel subagent execution.
