# Taipy Cognitive Interface Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port all 42 cognitive interface findings from the CHI audit comparison (Streamlit vs Taipy) so the Taipy spike has full UX parity.

**Architecture:** Three infrastructure additions to the shared template layer (warning boxes, sidebar help, contextual loading), then per-page wiring across 12 pages grouped into 3 batches (Match Analysis, Player Analysis, Advanced). All changes are additive — no restructuring.

**Tech Stack:** Taipy 4.1.1, Python dataclasses, CSS, matplotlib

---

## File Map

### Infrastructure (Task 1)

| File | Change | Responsibility |
|------|--------|----------------|
| `taipy_spike/src/page_template.py` | Modify | Add `warning_var` to PageConfig/SubView, `footer_var` to PageConfig, `help` to SidebarWidget |
| `taipy_spike/src/template.py` | Modify | Add `help=` to 3 sidebar widgets, bind `loading_text` in overlay |
| `taipy_spike/src/state/shared.py` | Modify | Add `loading_text` state var, per-page loading text dict |

### Match Analysis Pages (Task 2)

| File | Change |
|------|--------|
| `taipy_spike/src/pages/shot_map.py` | Add `warning_var`, chart title note |
| `taipy_spike/src/state/shot_map.py` | Add `sm_warning_text`, split guidance/warning, add chart title |
| `taipy_spike/src/pages/pass_map.py` | Add `warning_var` |
| `taipy_spike/src/state/pass_map.py` | Add `pm_warning_text` |
| `taipy_spike/src/pages/heat_map.py` | Add `warning_var` |
| `taipy_spike/src/state/heat_map.py` | Add `hm_warning_text` |
| `taipy_spike/src/pages/pass_network.py` | Add `warning_var` |
| `taipy_spike/src/state/pass_network.py` | Add `pn_warning_text` |
| `taipy_spike/src/pages/match_summary.py` | Add `warning_var`, help_text on score metrics |
| `taipy_spike/src/state/match_summary.py` | Add `ms_warning_text` |

### Player Analysis Pages (Task 3)

| File | Change |
|------|--------|
| `taipy_spike/src/pages/action_values.py` | Add `warning_var` + `scope_vars` to sub-views |
| `taipy_spike/src/state/action_values.py` | Add `av_warning_text`, `av_scope_label` |
| `taipy_spike/src/pages/player_radar.py` | Add `warning_var`, `scope_vars` |
| `taipy_spike/src/state/player_radar.py` | Add `pr_warning_text`, `pr_scope_label` |
| `taipy_spike/src/pages/player_similarity.py` | Add `warning_var`, bind threshold caption |
| `taipy_spike/src/state/player_similarity.py` | Add `ps_warning_text` |

### Advanced Pages (Task 4)

| File | Change |
|------|--------|
| `taipy_spike/src/pages/movement_analysis.py` | Add `warning_var`, expandable tables, PPDA scope |
| `taipy_spike/src/state/movement_analysis.py` | Add `ma_warning_text`, `ma_ppda_scope_label`, table state vars |
| `taipy_spike/src/pages/pitch_control.py` | Add `warning_var` for fallback empty |
| `taipy_spike/src/state/pitch_control.py` | Add `pc_warning_text` |
| `taipy_spike/src/pages/pass_timing.py` | Add `warning_var`, `footer_var` |
| `taipy_spike/src/state/pass_timing.py` | Add `pt_warning_text`, `pt_footer_text` |
| `taipy_spike/src/pages/defensive_valuation.py` | Add `warning_var`, tier disclaimer |
| `taipy_spike/src/state/defensive_valuation.py` | Add `dv_warning_text` |

---

## Task 1: Template Infrastructure

All shared-layer changes that enable the per-page fixes.

**Files:**
- Modify: `taipy_spike/src/page_template.py`
- Modify: `taipy_spike/src/template.py`
- Modify: `taipy_spike/src/state/shared.py`

### 1a. Add `warning_var` to PageConfig and SubView

- [ ] **Step 1: Add `warning_var` and `footer_var` fields to PageConfig**

In `page_template.py`, add to `PageConfig`:

```python
# After freshness_var line (~303):
warning_var: str = ""   # state variable for "no data" warnings (ll-warning-box)
footer_var: str = ""    # state variable for footer text (citations, scope notes)
```

- [ ] **Step 2: Add `warning_var` field to SubView**

In `page_template.py`, add to `SubView`:

```python
# After fallback_empty_condition/message lines (~336):
warning_var: str = ""
```

- [ ] **Step 3: Render warning_var in `build_page` (single-view pages)**

In `build_page()`, after the empty state block (~line 574) and before the freshness block:

```python
# Warning state (no-data — amber box, distinct from guidance)
if cfg.warning_var:
    parts.append(f"<|part|render={{len({cfg.warning_var}) > 0}}|class_name=ll-warning-box|")
    parts.append(f"<|{{{cfg.warning_var}}}|text|>")
    parts.append("|>")
```

- [ ] **Step 4: Render warning_var in `_build_sub_view`**

In `_build_sub_view()`, after the fallback_empty block (~line 481) and before closing left column:

```python
if sv.warning_var:
    parts.append(f"<|part|render={{len({sv.warning_var}) > 0}}|class_name=ll-warning-box|")
    parts.append(f"<|{{{sv.warning_var}}}|text|>")
    parts.append("|>")
```

- [ ] **Step 5: Render scope_vars in `_build_sub_view`**

`_build_sub_view()` declares `scope_vars` on SubView but never renders them. Add scope_vars rendering after the scale_notes block (~line 461), mirroring the `build_page()` pattern at lines 559-563:

```python
# Scope variables (above content, below scale notes)
for scope_v in sv.scope_vars:
    parts.append(f"<|part|render={{len({scope_v}) > 0}}|")
    parts.append(f"<|{{{scope_v}}}|text|>")
    parts.append("|>")
    parts.append("")
```

- [ ] **Step 6: Render footer_var in `build_page`**

In `build_page()`, after the freshness block (~line 581) and before closing left column:

```python
if cfg.footer_var:
    parts.append("")
    parts.append(f"<|part|render={{len({cfg.footer_var}) > 0}}|")
    parts.append(f"<|{{{cfg.footer_var}}}|text|class_name=ll-reference|>")
    parts.append("|>")
```

### 1b. Add `help` field to SidebarWidget

- [ ] **Step 7: Add `help` field to SidebarWidget dataclass**

In `page_template.py`, add to `SidebarWidget`:

```python
# After change_delay line (~52):
help: str = ""  # tooltip help text (rendered as info icon next to widget)
```

- [ ] **Step 8: Render help icon in `_build_sidebar_widget`**

In `_build_sidebar_widget()`, insert before the final `parts.append("|>")` (line 158):

```python
    if w.help:
        parts.append(
            f'<span class="ll-help material-symbols-outlined" title="{w.help}">info</span>'
        )
    parts.append("|>")  # close outer wrapper
```

Remove the existing final `parts.append("|>")` since it's now included above.

- [ ] **Step 9: Add help text to 3 sidebar widgets in template.py**

In `template.py`, add `help=` to these `SidebarWidget` definitions:

**xG Model** (~line 146-153):
```python
help="StatsBomb: provider xG. Custom Logistic: distance + angle. Custom XGBoost: 13 features with isotonic calibration (production model).",
```

**Metrics** (~line 208-215, the `pr_selected_metrics` widget):
```python
help="Per-90 stats: Goals, xG, Passes, Pass%, VAEP (action value), DEFCON (defensive pressure). See Glossary for definitions.",
```

**Search by** (~line 427, the `ps_search_mode` widget):
```python
help="Playing style: 32-d behavioral embedding from match action sequences. Statistical output: 13-d z-score vector from per-90 stats.",
```

### 1c. Contextual loading text

- [ ] **Step 10: Add `loading_text` state variable**

In `state/shared.py`, add after `is_loading` (~line 64):

```python
loading_text: str = "Loading..."
```

Add `"loading_text"` to the `__all__` list.

- [ ] **Step 11: Add per-page loading text dict and wire into `_refresh_current_page`**

In `state/shared.py`, add before `_refresh_current_page` (~line 127):

```python
_LOADING_TEXTS: dict[str, str] = {
    "Shot-Map": "Loading shots...",
    "Pass-Map": "Loading passes...",
    "Heat-Map": "Loading actions...",
    "Pass-Network": "Loading passes...",
    "Match-Summary": "Loading match data...",
    "Player-Impact": "Loading VAEP data...",
    "Player-Comparison": "Loading player stats...",
    "Player-Similarity": "Finding similar players...",
    "Movement-Pressing": "Loading movement data...",
    "Pitch-Control": "Computing pitch control...",
    "Pass-Timing": "Loading PAUSA data...",
    "Defensive-Impact": "Loading defensive data...",
}
```

Update `_refresh_current_page` to set loading text:

```python
def _refresh_current_page(state: Any) -> None:
    fn = _page_refreshers.get(state.current_page)
    if fn:
        state.loading_text = _LOADING_TEXTS.get(state.current_page, "Loading...")
        state.is_loading = True
        try:
            fn(state)
        except Exception:
            logger.exception("Failed to refresh page %s", state.current_page)
        finally:
            state.is_loading = False
```

- [ ] **Step 12: Bind `loading_text` in template overlay**

In `template.py`, change the loading overlay (~line 504) from:

```
<span class="material-symbols-outlined ll-spin">progress_activity</span> Loading...
```

to:

```
<span class="material-symbols-outlined ll-spin">progress_activity</span> <|{loading_text}|text|raw|>
```

- [ ] **Step 13: Verify infrastructure — run test_render.py**

```bash
cd taipy_spike && python src/test_render.py
```

Expected: Gui builds without errors on port 7861. The test uses mock state, so it validates template generation.

- [ ] **Step 14: Commit infrastructure changes**

```bash
git add taipy_spike/src/page_template.py taipy_spike/src/template.py taipy_spike/src/state/shared.py
git commit -m "feat(taipy): template infrastructure — warning boxes, sidebar help, contextual loading"
```

---

## Task 2: Match Analysis Pages (5 pages)

Wire up warning states, add missing help text and chart titles.

**Files:**
- Modify: `taipy_spike/src/pages/shot_map.py` + `taipy_spike/src/state/shot_map.py`
- Modify: `taipy_spike/src/pages/pass_map.py` + `taipy_spike/src/state/pass_map.py`
- Modify: `taipy_spike/src/pages/heat_map.py` + `taipy_spike/src/state/heat_map.py`
- Modify: `taipy_spike/src/pages/pass_network.py` + `taipy_spike/src/state/pass_network.py`
- Modify: `taipy_spike/src/pages/match_summary.py` + `taipy_spike/src/state/match_summary.py`

### Pattern for all 5 pages

Each page gets a `xx_warning_text` state variable. The state file sets it to the warning message when data is empty, and clears it when data loads. The page config adds `warning_var="xx_warning_text"`.

The existing `empty_condition`/`empty_message` stays for guidance ("Select a competition..."). The new `warning_var` handles no-data ("No shots for...").

The state file's existing `xx_empty_message` variable remains for backwards-compat in conditions but is no longer the sole empty-state mechanism.

### Shot Map

- [ ] **Step 1: Add warning state variable**

In `state/shot_map.py`, add after `sm_empty_message` (~line 48):

```python
sm_warning_text: str = ""
```

Add `"sm_warning_text"` to `__all__`.

- [ ] **Step 2: Split guidance vs warning in state refresh**

In `state/shot_map.py` `sm_refresh()`:

When `comp_id is None` (~line 209): keep `sm_empty_message = "Select a competition to begin."`, add `state.sm_warning_text = ""`.

When `shots.empty` (~line 234): change `sm_empty_message = ""` (clear guidance), add `state.sm_warning_text = "No shots for the selected filters."`.

When data loads (~line 239): keep `sm_empty_message = ""`, add `state.sm_warning_text = ""`.

- [ ] **Step 3: Add chart title to `_render_pitch`**

In `state/shot_map.py` `_render_pitch()`, after `fig, ax = pitch.draw(...)` (~line 153), add:

```python
ax.set_title("Shot Map", color=PITCH_LINE_COLOR, fontsize=14, pad=10)
```

- [ ] **Step 4: Wire page config**

In `pages/shot_map.py`, add to `PageConfig`:

```python
warning_var="sm_warning_text",
```

### Pass Map

- [ ] **Step 5: Add warning state + wire page**

State: add `pm_warning_text: str = ""` + `__all__` entry. Set to `"No passes for the selected filters."` when `passes.empty`, clear otherwise.

Page: add `warning_var="pm_warning_text"` to PageConfig.

### Heat Map

- [ ] **Step 6: Add warning state + wire page**

State: add `hm_warning_text: str = ""` + `__all__`. Set to `"No actions for the selected filters."` when empty.

Page: add `warning_var="hm_warning_text"`.

### Pass Network

- [ ] **Step 7: Add warning state + wire page**

State: add `pn_warning_text: str = ""` + `__all__`. Set to `"No completed passes for the selected filters. Wyscout matches do not include pass recipient data."` when empty.

Page: add `warning_var="pn_warning_text"`.

### Match Summary

- [ ] **Step 8: Add warning state + wire page**

State: add `ms_warning_text: str = ""` + `__all__`. Set to `"No match data for the selected filters."` when empty.

Page: add `warning_var="ms_warning_text"`.

- [ ] **Step 9: Add help_text to score metrics**

In `pages/match_summary.py`, the `Metric("Home Score", ...)` and `Metric("Away Score", ...)` — add:

```python
help_text="Match score."
```

- [ ] **Step 10: Verify all 5 pages render**

```bash
cd taipy_spike && python src/test_render.py
```

- [ ] **Step 11: Commit Match Analysis fixes**

```bash
git add taipy_spike/src/pages/shot_map.py taipy_spike/src/state/shot_map.py \
       taipy_spike/src/pages/pass_map.py taipy_spike/src/state/pass_map.py \
       taipy_spike/src/pages/heat_map.py taipy_spike/src/state/heat_map.py \
       taipy_spike/src/pages/pass_network.py taipy_spike/src/state/pass_network.py \
       taipy_spike/src/pages/match_summary.py taipy_spike/src/state/match_summary.py
git commit -m "fix(taipy): Match Analysis cognitive fixes — warning states, chart titles, help text"
```

---

## Task 3: Player Analysis Pages (3 pages)

Wire up warning states, add missing scope labels, bind threshold caption.

**Files:**
- Modify: `taipy_spike/src/pages/action_values.py` + `taipy_spike/src/state/action_values.py`
- Modify: `taipy_spike/src/pages/player_radar.py` + `taipy_spike/src/state/player_radar.py`
- Modify: `taipy_spike/src/pages/player_similarity.py` + `taipy_spike/src/state/player_similarity.py`

### Action Values (Player Impact)

- [ ] **Step 1: Add scope + warning state variables**

In `state/action_values.py`, add:

```python
av_scope_label: str = ""
av_warning_text: str = ""
```

Add both to `__all__`. In the refresh function, populate `av_scope_label` via `fetch_scope_label(comp_id, team_id)`. Set `av_warning_text` when queries return empty.

- [ ] **Step 2: Wire page config — scope + warning on each sub-view**

In `pages/action_values.py`, add to each `SubView`:

```python
scope_vars=["av_scope_label"],
warning_var="av_warning_text",
```

### Player Radar (Player Comparison)

- [ ] **Step 3: Add scope + warning state variables**

In `state/player_radar.py`, add:

```python
pr_scope_label: str = ""
pr_warning_text: str = ""
```

Add both to `__all__`. Populate `pr_scope_label` via `fetch_scope_label(comp_id, team_id)` in the refresh function. Move the "No player stats" message from `pr_no_data_warning` to `pr_warning_text`.

- [ ] **Step 4: Wire page config**

In `pages/player_radar.py`, add to `PageConfig`:

```python
scope_vars=["pr_scope_label"],
warning_var="pr_warning_text",
```

### Player Similarity

- [ ] **Step 5: Add warning state variable**

In `state/player_similarity.py`, add:

```python
ps_warning_text: str = ""
```

Add to `__all__`. Route error/empty messages to `ps_warning_text` instead of (or in addition to) `ps_status_message`. Keep `ps_status_message` for success feedback ("Found N similar players.").

- [ ] **Step 6: Bind threshold caption to a ContentBlock**

In `pages/player_similarity.py`, add a new `ContentBlock` that renders the threshold caption. The state variable `ps_threshold_caption` already exists and is populated. Add:

```python
ContentBlock("text", "ps_threshold_caption"),
```

Place it in the content list BEFORE the results table ContentBlock, so the scale legend appears above the results.

- [ ] **Step 7: Wire warning_var on page config**

```python
warning_var="ps_warning_text",
```

- [ ] **Step 8: Verify all 3 pages render**

```bash
cd taipy_spike && python src/test_render.py
```

- [ ] **Step 9: Commit Player Analysis fixes**

```bash
git add taipy_spike/src/pages/action_values.py taipy_spike/src/state/action_values.py \
       taipy_spike/src/pages/player_radar.py taipy_spike/src/state/player_radar.py \
       taipy_spike/src/pages/player_similarity.py taipy_spike/src/state/player_similarity.py
git commit -m "fix(taipy): Player Analysis cognitive fixes — scope labels, threshold caption, warnings"
```

---

## Task 4: Advanced Pages (4 pages)

Wire up warning states, add expandable tables, fallback empties, footer citations, tier disclaimer.

**Files:**
- Modify: `taipy_spike/src/pages/movement_analysis.py` + `taipy_spike/src/state/movement_analysis.py`
- Modify: `taipy_spike/src/pages/pitch_control.py` + `taipy_spike/src/state/pitch_control.py`
- Modify: `taipy_spike/src/pages/pass_timing.py` + `taipy_spike/src/state/pass_timing.py`
- Modify: `taipy_spike/src/pages/defensive_valuation.py` + `taipy_spike/src/state/defensive_valuation.py`

### Movement & Pressing

- [ ] **Step 1: Add state variables for expandable tables + scope + warning**

In `state/movement_analysis.py`, add:

```python
ma_physical_table: pd.DataFrame = pd.DataFrame()
ma_ppda_table: pd.DataFrame = pd.DataFrame()
ma_oxt_table: pd.DataFrame = pd.DataFrame()
ma_ppda_scope_label: str = ""
ma_warning_text: str = ""
```

Add all to `__all__`. Populate:
- `ma_physical_table` with the renamed stats DataFrame (Player, Minutes, Distance, etc.)
- `ma_ppda_table` with the PPDA data (Date, Home, Away, Home PPDA, Away PPDA)
- `ma_oxt_table` with xT data (Player, Total xT, Avg xT/Frame)
- `ma_ppda_scope_label` via `fetch_scope_label(comp_id)` in the PPDA sub-view path
- `ma_warning_text` when queries return empty

- [ ] **Step 2: Wire page config — add expandable tables + scope + warning per sub-view**

In `pages/movement_analysis.py`:

Physical sub-view content — add:
```python
ContentBlock("expandable_table", "ma_physical_table", header="Full Stats"),
```

PPDA sub-view — add:
```python
scope_vars=["ma_ppda_scope_label"],
```
and in content:
```python
ContentBlock("expandable_table", "ma_ppda_table", header="PPDA Data"),
```

Off-Ball xT sub-view content — add:
```python
ContentBlock("expandable_table", "ma_oxt_table", header="Off-Ball xT Data"),
```

Add `warning_var="ma_warning_text"` to all three sub-views.

### Pitch Control

- [ ] **Step 3: Add fallback warning for no tracking matches**

In `state/pitch_control.py`, add:

```python
pc_warning_text: str = ""
```

Add to `__all__`. In the refresh function, when `tracking_match_lov` is empty (no tracking data at all), set:

```python
state.pc_warning_text = "Pitch control requires player tracking data (~20 matches from Metrica, IDSSE, SkillCorner)."
```

When data loads or tracking matches exist, clear it.

- [ ] **Step 4: Wire page config**

In `pages/pitch_control.py`, add:

```python
warning_var="pc_warning_text",
```

### Pass Timing

- [ ] **Step 5: Add warning + footer state variables**

In `state/pass_timing.py`, add:

```python
pt_warning_text: str = ""
pt_footer_text: str = ""
```

Add both to `__all__`. Set `pt_footer_text` on every successful refresh:

```python
state.pt_footer_text = (
    "Lee, Jo, Hong, Bauer & Ko (2026). "
    '"Valuing La Pausa: Quantifying Optimal Pass Timing Beyond Speed." '
    "MIT Sloan 2026. OBSO: Spearman (2018), Fernandez & Bornn (2018). "
    "Event-tracking sync: Kim et al. (2025) ELASTIC. "
    "IDSSE Bundesliga · 7 matches · Tracking-dependent."
)
```

Set `pt_warning_text` when summary is empty:

```python
state.pt_warning_text = "No PAUSA data for the selected filters. Try a different match or remove team/player filters."
```

- [ ] **Step 6: Add fallback empty for no PAUSA matches**

In `pages/pass_timing.py`, the current `empty_message` is guidance. When `pt_match_lov` is empty, there's no explanation. Add or update the page-level empty state:

If implementing as a warning (recommended): the state file should set `pt_warning_text` when no PAUSA matches exist at all:

```python
# In pt_refresh, if no matches available:
state.pt_warning_text = "Pass timing requires OBSO computation and PAUSA pipeline. Currently available for 7 IDSSE matches."
```

- [ ] **Step 7: Wire page config — footer + warning**

In `pages/pass_timing.py`, add:

```python
warning_var="pt_warning_text",
footer_var="pt_footer_text",
```

### Defensive Impact

- [ ] **Step 8: Add tier disclaimer to description**

In `pages/defensive_valuation.py`, append to the `description` string:

```python
description="... Tiers: 1 = full GNN, 2 = simplified GNN, 3 = tabular heuristic (this implementation).",
```

- [ ] **Step 9: Add `lower_is_better=True` to Concede metric**

In `pages/defensive_valuation.py`, the Concede `Metric` says "Lower is better" in help_text but lacks the flag. Add `lower_is_better=True`:

```python
Metric("Concede", "dv_concede", "DEFCON credit charged when a shot or goal occurs despite pressure. Lower is better. Typical match total per player: 0.0-0.5 credits.", lower_is_better=True),
```

- [ ] **Step 10: Add warning state variable + wire**

In `state/defensive_valuation.py`, add `dv_warning_text: str = ""` + `__all__`. Set it when rankings/breakdown/timeline queries return empty.

In `pages/defensive_valuation.py`, add `warning_var="dv_warning_text"` to each sub-view.

- [ ] **Step 11: Verify all 4 pages render**

```bash
cd taipy_spike && python src/test_render.py
```

- [ ] **Step 12: Commit Advanced fixes**

```bash
git add taipy_spike/src/pages/movement_analysis.py taipy_spike/src/state/movement_analysis.py \
       taipy_spike/src/pages/pitch_control.py taipy_spike/src/state/pitch_control.py \
       taipy_spike/src/pages/pass_timing.py taipy_spike/src/state/pass_timing.py \
       taipy_spike/src/pages/defensive_valuation.py taipy_spike/src/state/defensive_valuation.py
git commit -m "fix(taipy): Advanced page cognitive fixes — expandable tables, fallbacks, citations, tier disclaimer"
```

---

## Task 5: Verification

Full end-to-end verification across all 12 pages.

- [ ] **Step 1: Run test_render.py (no-database validation)**

```bash
cd taipy_spike && python src/test_render.py
```

Expected: Gui builds on port 7861 without errors. All 12 pages generate valid template markdown.

- [ ] **Step 2: Start the app and verify with Puppeteer**

```bash
cd taipy_spike && python src/main.py
```

Navigate to each page and verify:
1. Guidance empty states show in blue (`ll-info-box`)
2. No-data warnings show in amber (`ll-warning-box`)
3. Loading overlay shows contextual text (not "Loading...")
4. Sidebar widgets with help show info icon with tooltip on hover
5. Player Similarity shows threshold caption above results table
6. Movement pages show expandable data tables
7. Pass Timing shows footer citation
8. Defensive Impact description includes tier disclaimer
9. Player Impact, Player Radar, Movement/PPDA show scope labels

- [ ] **Step 3: Final commit**

```bash
git add -A taipy_spike/
git commit -m "fix(taipy): cognitive interface parity — 42 findings from CHI audit comparison"
```

---

## Cross-Reference: Audit Findings → Tasks

| Finding | Severity | Task |
|---------|----------|------|
| X1: Single empty-state visual (all pages) | High | T1 (infra) + T2-T4 (per-page) |
| X2: Generic loading overlay | Medium | T1c |
| X3: No sidebar widget help | Medium | T1b |
| Shot Map: no chart title | Low | T2 |
| Match Summary: no help on score metrics | Medium | T2 |
| Action Values: no scope label | High | T3 |
| Player Radar: no scope label | High | T3 |
| Player Similarity: threshold caption unbound | Critical | T3 |
| Movement: no expandable tables | Medium | T4 |
| Movement PPDA: no scope label | High | T4 |
| Pitch Control: no fallback for no tracking data | High | T4 |
| Pass Timing: no footer citation | Medium | T4 |
| Pass Timing: no fallback for no PAUSA matches | High | T4 |
| Defensive Impact: no tier disclaimer | Medium | T4 |
| Concede metric: missing `lower_is_better` | Low | T4 |
