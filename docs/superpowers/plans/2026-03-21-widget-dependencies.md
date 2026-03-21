# Widget Dependencies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add declarative widget dependencies to sidebar widgets so child widgets are hidden until their parent has a value, matching the Streamlit app's progressive disclosure behavior.

**Architecture:** Add 3 fields to `SidebarWidget` dataclass (`depends_on`, `depends_value`, `depends_lov_populated`). A new `_build_render_condition()` function combines page visibility + dependency into a single `render=` expression. Fix `build_sidebar_section()` to use `dataclasses.replace()`. Update all 33 widget definitions in `template.py`.

**Tech Stack:** Taipy 4.x, Python 3.10, dataclasses, Puppeteer/Chrome for verification.

**Spec:** `docs/superpowers/specs/2026-03-21-widget-dependencies-design.md`

---

### Task 1: Add dependency fields to `SidebarWidget` and implement `_build_render_condition()`

**Files:**
- Modify: `taipy_spike/src/page_template.py`

- [ ] **Step 1: Add 3 new fields to `SidebarWidget` dataclass**

After the existing `filter_box_label` field (line 42), add:

```python
    # Dependency fields — template generates render conditions from these.
    # depends_on: parent state variable name. Widget hidden until parent is not None.
    depends_on: str = ""
    # depends_value: show only when parent equals this specific value (requires depends_on).
    depends_value: str = ""
    # depends_lov_populated: show only when this widget's own LOV has entries.
    depends_lov_populated: bool = False
```

- [ ] **Step 2: Add `_build_render_condition()` function**

Add before `_build_sidebar_widget()` (before line 45):

```python
def _build_render_condition(w: SidebarWidget) -> str:
    """Build compound render condition from page visibility + dependency fields.

    Combines condition (page visibility), depends_on (parent check),
    depends_value (parent value match), and depends_lov_populated (LOV gate)
    into a single Taipy expression string joined with 'and'.
    """
    parts: list[str] = []
    if w.condition:
        parts.append(w.condition)
    if w.depends_on:
        if w.depends_value:
            parts.append(f'{w.depends_on} == "{w.depends_value}"')
        else:
            parts.append(f"{w.depends_on} is not None")
    if w.depends_lov_populated and w.lov:
        parts.append(f"len({w.lov}) > 0")
    return " and ".join(parts) if parts else ""
```

- [ ] **Step 3: Update `_build_sidebar_widget()` to use `_build_render_condition()`**

Replace lines 59-63 (the current condition handling):

```python
    # Outer wrapper — every widget gets the same structural treatment
    if w.condition:
        parts.append(f"<|part|render={lb}{w.condition}{rb}|")
    else:
        parts.append("<|part|")
```

With:

```python
    # Outer wrapper — render condition combines page visibility + dependencies
    render_cond = _build_render_condition(w)
    if render_cond:
        parts.append(f"<|part|render={lb}{render_cond}{rb}|")
    else:
        parts.append("<|part|")
```

- [ ] **Step 4: Fix `build_sidebar_section()` condition inheritance**

Replace lines 162-180 (manual `SidebarWidget` construction loop body):

```python
    for w in widgets:
        effective_w = w
        if not w.condition and condition:
            # Inherit section condition so widget hides with the section
            effective_w = SidebarWidget(
                kind=w.kind,
                var=w.var,
                label=w.label,
                on_change=w.on_change,
                condition=condition,
                lov=w.lov,
                slider_min=w.slider_min,
                slider_max=w.slider_max,
                slider_step=w.slider_step,
                slider_range_labels=w.slider_range_labels,
                slider_range_vars=w.slider_range_vars,
                filter_box_label=w.filter_box_label,
            )
        parts.append(_build_sidebar_widget(effective_w, f_string))
```

With:

```python
    for w in widgets:
        effective_w = w
        if not w.condition and condition:
            # Inherit section condition so widget hides with the section.
            # replace() preserves depends_on/depends_value/depends_lov_populated.
            effective_w = replace(w, condition=condition)
        parts.append(_build_sidebar_widget(effective_w, f_string))
```

Also add `from dataclasses import replace` at the top of the file (after the existing `from dataclasses import dataclass, field` import — update it to `from dataclasses import dataclass, field, replace`).

- [ ] **Step 5: Verify syntax**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from page_template import SidebarWidget, _build_render_condition; w = SidebarWidget('dropdown','x','X','cb', condition='current_page in (\"A\",)', depends_on='y'); print(_build_render_condition(w)); assert 'y is not None' in _build_render_condition(w); print('OK')"`

Expected: `current_page in ("A",) and y is not None` then `OK`

---

### Task 2: Add dependencies to shared filter widgets (`_FILTER_WIDGETS` first half)

**Files:**
- Modify: `taipy_spike/src/template.py`

Update the shared cascade and tracking filter widgets in `_FILTER_WIDGETS`. Only 7 widgets need changes — the remaining widgets in this group (Competition, xG Model, Min passes, Progressive toggle, Line-breaking toggle, Provider, View) have no dependencies and stay unchanged.

- [ ] **Step 1: Add `depends_on` to Team widget**

```python
    SidebarWidget(
        "dropdown",
        "selected_team",
        "Team",
        "on_team_change",
        condition=f"current_page in {_TEAM_PAGES}",
        depends_on="selected_competition",
        lov="team_lov",
    ),
```

- [ ] **Step 2: Add `depends_on` to Match widget**

```python
        depends_on="selected_team",
```

- [ ] **Step 3: Add `depends_on` to Player widget and fix label**

Change label from `"Player"` to `"Player (optional)"` and add:

```python
        depends_on="selected_competition",
```

- [ ] **Step 4: Add `depends_on` to Players (multi) widget**

```python
        depends_on="selected_competition",
```

- [ ] **Step 5: Add `depends_on` to Metrics widget**

```python
        depends_on="selected_competition",
```

- [ ] **Step 6: Add `depends_on` to Min minutes slider**

```python
        depends_on="selected_competition",
```

- [ ] **Step 7: Add `depends_on` to Tracking Match widget**

```python
        depends_on="selected_provider",
```

- [ ] **Step 8: Verify imports work**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from template import build_root_page; print('OK')"`

Expected: `OK`

---

### Task 3: Add dependencies to page-specific widgets (`_FILTER_WIDGETS` second half)

**Files:**
- Modify: `taipy_spike/src/template.py`

Update Pass Timing, Defensive Impact, Pitch Control, and Player Similarity widgets.

- [ ] **Step 1: Add `depends_on` to Pass Timing Team and Player**

Both get `depends_on="pt_selected_match"`.

- [ ] **Step 2: Add `depends_on` to Defensive Impact Team and View**

Both get `depends_on="dv_selected_comp"`.

- [ ] **Step 3: Migrate Defensive Impact Breakdown Player**

Replace:
```python
    SidebarWidget(
        "dropdown",
        "dv_selected_breakdown_player",
        "Player",
        "dv_on_breakdown_player_change",
        condition='current_page == "Defensive-Impact" and dv_current_view == "Breakdown" and len(dv_breakdown_player_lov) > 0',
        lov="dv_breakdown_player_lov",
    ),
```

With:
```python
    SidebarWidget(
        "dropdown",
        "dv_selected_breakdown_player",
        "Player",
        "dv_on_breakdown_player_change",
        condition=f"current_page in {_DEFCON_PAGES}",
        depends_on="dv_current_view",
        depends_value="Breakdown",
        depends_lov_populated=True,
        lov="dv_breakdown_player_lov",
    ),
```

- [ ] **Step 4: Migrate Defensive Impact Timeline Player**

Same pattern — `condition=f"current_page in {_DEFCON_PAGES}"`, `depends_on="dv_current_view"`, `depends_value="Timeline"`, `depends_lov_populated=True`.

- [ ] **Step 5: Migrate Defensive Impact Timeline Match**

Same pattern as Timeline Player.

- [ ] **Step 6: Add `depends_on` to Pitch Control Half, Model, and Velocity**

All three get `depends_on="selected_tracking_match"`.

- [ ] **Step 7: Add `depends_lov_populated` to Player Similarity Compare with**

The widget keeps its existing `condition='current_page == "Player-Similarity" and len(ps_results_data) > 0'`. Replace with:

```python
    SidebarWidget(
        "dropdown",
        "ps_selected_compare",
        "Compare with",
        "on_ps_selected_compare_change",
        condition='current_page == "Player-Similarity"',
        depends_lov_populated=True,
        lov="ps_compare_lov",
    ),
```

- [ ] **Step 8: Verify the generated render conditions**

Run:
```
cd taipy_spike && .venv/Scripts/python.exe -c "
import sys; sys.path.insert(0,'src')
from template import _FILTER_WIDGETS
from page_template import _build_render_condition
# Check Team depends on Competition
team = [w for w in _FILTER_WIDGETS if w.var == 'selected_team'][0]
cond = _build_render_condition(team)
assert 'selected_competition is not None' in cond, f'Team: {cond}'
# Check DV Breakdown Player
dv_bp = [w for w in _FILTER_WIDGETS if w.var == 'dv_selected_breakdown_player'][0]
cond = _build_render_condition(dv_bp)
assert 'dv_current_view == \"Breakdown\"' in cond, f'DV BP: {cond}'
assert 'len(dv_breakdown_player_lov) > 0' in cond, f'DV BP lov: {cond}'
# Check PC Half depends on tracking match
pc_half = [w for w in _FILTER_WIDGETS if w.var == 'pc_half'][0]
cond = _build_render_condition(pc_half)
assert 'selected_tracking_match is not None' in cond, f'PC Half: {cond}'
# Check PS Compare with
ps_cw = [w for w in _FILTER_WIDGETS if w.var == 'ps_selected_compare'][0]
cond = _build_render_condition(ps_cw)
assert 'len(ps_compare_lov) > 0' in cond, f'PS CW: {cond}'
print('All dependency conditions correct')
"
```

Expected: `All dependency conditions correct`

---

### Task 4: Verify no changes needed for `_SEARCH_WIDGETS`

**Files:** None (verification only)

The 5 `_SEARCH_WIDGETS` were analyzed in the spec:
- `ps_search_mode`, `ps_selected_player`, `ps_result_count`, `ps_filter_by_competition`: no dependencies (always visible within the Search section)
- `ps_selected_competition`: keeps existing `condition="ps_filter_by_competition"` (bool check — not migrated to `depends_on` because `is not None` doesn't work for booleans)

- [ ] **Step 1: Verify search widgets are unchanged and work**

Run:
```
cd taipy_spike && .venv/Scripts/python.exe -c "
import sys; sys.path.insert(0,'src')
from template import _SEARCH_WIDGETS
# Verify Competition widget still has condition for bool check
comp_w = [w for w in _SEARCH_WIDGETS if w.var == 'ps_selected_competition'][0]
assert comp_w.condition == 'ps_filter_by_competition', f'Got: {comp_w.condition}'
# Verify no search widgets accidentally got depends_on
for w in _SEARCH_WIDGETS:
    assert not w.depends_on, f'{w.var} has unexpected depends_on={w.depends_on}'
print('Search widgets OK — no changes needed')
"
```

Expected: `Search widgets OK — no changes needed`

---

### Task 5: Visual verification via Puppeteer against Chrome

**Files:** None (verification only)

Kill any running server and start fresh:

`cd taipy_spike/src && LAKEBASE_HOST="ep-spring-rain-d2i6lozx.database.us-east-1.cloud.databricks.com" LAKEBASE_ENDPOINT_NAME="projects/soccer-analytics-dev/branches/production/endpoints/primary" ../.venv/Scripts/python.exe main.py`

Wait for "Loaded 21 competitions" in server output before testing.

- [ ] **Step 1: Shot Map — verify cascade**

Navigate to `http://localhost:7860/Shot-Map`. Screenshot the sidebar.

Verify:
- Competition dropdown is visible
- Team, Player (optional), xG Model are ALL hidden (Competition not yet selected)
- Only MATCH ANALYSIS nav, FILTERS header, and Competition dropdown visible

- [ ] **Step 2: Shot Map — select Competition, verify Team appears**

Select a competition from the dropdown. Wait 2 seconds. Screenshot.

Verify:
- Team dropdown now visible
- Player (optional) dropdown now visible (depends on Competition, not Team)
- xG Model visible (no dependency)

- [ ] **Step 3: Pass Map — verify Match hidden until Team selected**

Navigate to Pass Map. Select Competition. Verify Team appears. Verify Match is still hidden. Select Team. Verify Match appears.

- [ ] **Step 4: Pitch Control — verify Half/Model/Velocity hidden**

Navigate to Pitch Control. Verify:
- Provider dropdown visible
- Tracking Match, Half, Model, Velocity toggle, Time slider are ALL hidden

Select Provider. Verify Tracking Match appears. Select a tracking match. Verify Half, Model, Velocity appear.

- [ ] **Step 5: Defensive Impact — verify view-conditional widgets**

Navigate to Defensive Impact. Select Competition. Verify:
- Team (optional), View dropdowns visible
- No Breakdown/Timeline player widgets visible

Select View = "Rankings". Verify no extra widgets.
Select View = "Breakdown". Verify Player dropdown appears (if LOV populated).

- [ ] **Step 6: Pass Timing — verify Team/Player hidden until Match selected**

Navigate to Pass Timing. Verify:
- PAUSA Match dropdown visible
- Team (optional), Player (optional) hidden

Select a match. Verify Team and Player appear.

- [ ] **Step 7: Player Similarity — verify Compare with hidden**

Navigate to Player Similarity. Verify:
- Search section widgets visible (Search by, Player, Results, Filter toggle)
- "Compare with" in Filters section is hidden
- Search for a player. After results load, verify "Compare with" appears.

- [ ] **Step 8: Verify Player label shows "(optional)"**

On Shot Map with Competition selected, verify the Player dropdown label reads "Player (optional)" not "Player".

- [ ] **Step 9: Widget spacing — verify hidden widgets collapse**

On Shot Map before selecting Competition: verify no empty gaps where Team/Player/xG Model would be. The sidebar should show only Competition with no phantom spacing.
