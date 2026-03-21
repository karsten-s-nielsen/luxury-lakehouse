# Template Consistency Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate one-off patterns in the Taipy spike template system — generate glossary panels from data, remove CSS class names from page files, unify sub-view switching, and merge redundant constants.

**Architecture:** Extend the template-driven pattern (established for navigation) to glossary panels and content building blocks. Add `ScaleNote` and `ContentBlock` abstractions to replace raw Taipy markdown in page files. Unify sub-view state variables. All changes are in `taipy_spike/src/`.

**Tech Stack:** Taipy 4.x, Python 3.10, dataclasses

**Audit source:** Template audit findings from session 2026-03-20 (7 high/medium/low items)

---

### Task 1: Generate glossary panels from `PAGE_TERMS` data (Finding #2)

**Files:**
- Modify: `taipy_spike/src/template.py`

Currently `template.py` has a `GLOSSARY` dict, `PAGE_TERMS` dict, and `get_glossary_md()` function — but `build_root_page()` doesn't use any of them. Instead, it has 12 hardcoded `<|part|render={{show_glossary and current_page == "..."}}>` blocks with inline glossary content. This is the same drift problem we fixed for navigation.

- [ ] **Step 1: Write `build_glossary_panels()` function**

Add after `get_glossary_md()` in `template.py`:

```python
def _build_glossary_panels() -> str:
    """Generate all per-page glossary panels from PAGE_TERMS + GLOSSARY data."""
    parts: list[str] = []
    for page_key, terms in PAGE_TERMS.items():
        filtered = {k: v for k, v in GLOSSARY.items() if k in terms}
        parts.append(f'<|part|render={{{{show_glossary and current_page == "{page_key}"}}}}|class_name=ll-dropdown|')
        if not filtered:
            parts.append("*No domain-specific terms on this page.*")
        else:
            for k, v in filtered.items():
                parts.append(f"**{k}** \u2014 {v}")
                parts.append("")
        parts.append("|>")
        parts.append("")
    return "\n".join(parts)
```

Note on brace escaping: `{{{{` in source → `{{` in `_build_glossary_panels()` return value → `{` after f-string interpolation in `build_root_page()` → Taipy evaluates it as a binding expression. This matches the existing pattern: the hardcoded blocks in the current f-string use `{{...}}` which the f-string reduces to `{...}` for Taipy.

Note on content fidelity: the generated panels use `GLOSSARY` dict values which are slightly more complete than some hardcoded panels (e.g., PPDA includes "Range: 5–15", Line-Breaking includes "Ward clustering" detail). This is an intentional improvement — single source of truth with the fullest definition.

- [ ] **Step 2: Replace hardcoded glossary blocks in `build_root_page()`**

Generate the panels at module level:

```python
_glossary_panels = _build_glossary_panels()
```

In `build_root_page()`, replace the 12 hardcoded `<|part|render={{show_glossary and current_page == "..."}}|...` blocks (lines ~420–512 of current file) with:

```python
{_glossary_panels}
```

Keep the Getting Started panel (`<|part|render={{show_getting_started}}|...`) — it's a single block, not per-page.

- [ ] **Step 3: Verify the generated panels match**

Run:
```
cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from template import _build_glossary_panels; p = _build_glossary_panels(); assert 'Shot-Map' in p; assert 'xG (Expected Goals)' in p; assert 'Defensive-Impact' in p; assert 'DEFCON' in p; print(f'OK — {p.count(chr(60)+chr(124)+\"part\")} panels'); print(p[:500])"
```

Expected: `OK — 12 panels` and the first glossary panel content matching the current hardcoded output.

- [ ] **Step 4: Restart app and verify glossary via Puppeteer**

Kill old server, restart app, navigate to Shot Map, click Glossary button, screenshot to verify glossary panel appears with correct terms.

---

### Task 2: Remove CSS class names from page files (Finding #5)

**Files:**
- Modify: `taipy_spike/src/page_template.py` (add `scale_notes` field to `SubView`, rendering in `_build_sub_view()`)
- Modify: `taipy_spike/src/pages/action_values.py`
- Modify: `taipy_spike/src/pages/defensive_valuation.py`

Note: `pass_timing.py`, `player_radar.py`, and `player_similarity.py` also use `ll-subtitle`/`ll-reference` but in free-form `pre_image_content` strings (the escape hatch). These are documented as acceptable in Step 6 — no code changes needed in those files.

The `ll-reference` usages in `SubView.pre_content` (action_values, defensive_valuation) are scale notes that can be migrated to a template-controlled field.

**Approach:** The `ll-reference` usages in `SubView.pre_content` are scale notes (e.g., "VAEP/90: higher = more impactful"). Add a `scale_notes: list[str]` field to `SubView` and render them in `_build_sub_view()`. For `ll-subtitle` usages in `pre_image_content` / `post_content`, they are section headings within free-form content blocks — these are harder to template because they are interspersed with conditional renders, tables, and images unique to each page. The `pre_image_content` and `post_content` fields are designed as escape hatches for complex layouts. Pragmatic fix: accept that these free-form content blocks need CSS classes, but document the contract ("pages may use `ll-subtitle` and `ll-reference` in free-form content blocks").

- [ ] **Step 1: Add `scale_notes` to `SubView`**

In `page_template.py`, add to the `SubView` dataclass:

```python
    # Scale reference notes rendered above the content grid as ll-reference blocks.
    # Rendered BEFORE pre_content in _build_sub_view().
    scale_notes: list[str] = field(default_factory=list)
```

- [ ] **Step 2: Render `scale_notes` in `_build_sub_view()`**

In `_build_sub_view()`, replace the `if sv.pre_content:` block with:

```python
    # Scale reference notes (template-controlled styling)
    for note in sv.scale_notes:
        parts.append(f"<|part|class_name=ll-reference|")
        parts.append(note)
        parts.append("|>")

    # Additional pre-content (free-form — may use ll-subtitle, ll-reference)
    if sv.pre_content:
        parts.append(sv.pre_content)
        parts.append("")
```

- [ ] **Step 3: Migrate `action_values.py` scale note**

Change `SubView` for Rankings from:
```python
pre_content=(
    "<|part|class_name=ll-reference|\nVAEP/90: higher = more impactful (typical range 0.01-1.0)\n|>"
),
```
To:
```python
scale_notes=["VAEP/90: higher = more impactful (typical range 0.01-1.0)"],
```

- [ ] **Step 4: Migrate `defensive_valuation.py` scale notes**

Each sub-view's `pre_content` has 1-2 `ll-reference` blocks. Move them to `scale_notes` lists. For example, Rankings sub-view:

From:
```python
pre_content=(
    "<|part|class_name=ll-reference|"
    "\nRequires StatsBomb 360 freeze-frame data (323 of 380+ matches)."
    "\n|>\n"
    "<|part|class_name=ll-reference|"
    "\nTotal Pressure: higher = more defensive attention attracted (typical range 1-50 per competition)"
    "\n|>"
),
```
To:
```python
scale_notes=[
    "Requires StatsBomb 360 freeze-frame data (323 of 380+ matches).",
    "Total Pressure: higher = more defensive attention attracted (typical range 1-50 per competition)",
],
```

Apply the same pattern to the Breakdown and Timeline sub-views. Remove `pre_content` from all 3 sub-views (it becomes empty after migration).

- [ ] **Step 5: Verify migration succeeded**

Verify that page source files no longer contain raw `ll-reference` in `SubView` fields, and that the template generates it instead:

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from pages.action_values import page_config; from pages.defensive_valuation import page_config as dc; assert all(sv.pre_content == '' or 'll-reference' not in sv.pre_content for sv in page_config.sub_views); assert all(sv.pre_content == '' or 'll-reference' not in sv.pre_content for sv in dc.sub_views); assert any(len(sv.scale_notes) > 0 for sv in page_config.sub_views); assert any(len(sv.scale_notes) > 0 for sv in dc.sub_views); print('Migration OK — scale notes in config, ll-reference removed from pre_content')"`

- [ ] **Step 6: Document the free-form content contract**

Add a comment to the `pre_image_content` field in `PageConfig` and `pre_content`/`post_content` fields in `SubView`:

```python
    # Free-form Taipy markdown — escape hatch for complex layouts.
    # May use ll-subtitle and ll-reference CSS classes.
    pre_image_content: str = ""
```

---

### Task 3: Unify sub-view state variable (Finding #6 + #2 partial)

**Files:**
- Modify: `taipy_spike/src/pages/movement_analysis.py`
- Modify: `taipy_spike/src/state/movement_analysis.py`
- Modify: `taipy_spike/src/test_render.py` (remove dead `ma_active_view` variable)

Movement & Pressing uses `ma_active_view` in its `SubView.condition` strings, while the sidebar dropdown binds to `selected_sub_view`. The callback (`ma_refresh` at line 310) copies `selected_sub_view` → `ma_active_view`. This indirection is unnecessary — use `selected_sub_view` directly in the conditions like Player Impact does.

Defensive Impact uses its own `dv_current_view` with a dedicated sidebar widget — this is intentionally isolated (different LOV, different callback) so it should stay as-is.

- [ ] **Step 1: Update `movement_analysis.py` SubView conditions**

Change all three `condition=` values:
- `'ma_active_view == "Physical Performance"'` → `'selected_sub_view == "Physical Performance"'`
- `'ma_active_view == "PPDA / Pressing Intensity"'` → `'selected_sub_view == "PPDA / Pressing Intensity"'`
- `'ma_active_view == "Off-Ball xT"'` → `'selected_sub_view == "Off-Ball xT"'`

- [ ] **Step 2: Remove `ma_active_view` from state**

In `taipy_spike/src/state/movement_analysis.py`:
- Remove `ma_active_view: str = "Physical Performance"` (line 59)
- Remove `"ma_active_view"` from `__all__` (line 62)
- Remove `state.ma_active_view = view` (line 318)

- [ ] **Step 3: Remove dead `ma_active_view` from `test_render.py`**

`test_render.py` also declares `ma_active_view = "Physical Performance"` (line 76). Remove this dead variable — the page template now uses `selected_sub_view` directly (which already exists in `test_render.py` at line 25).

- [ ] **Step 4: Verify page renders**

Restart app, navigate to Movement & Pressing, verify sub-view switching still works by clicking through Physical Performance / PPDA / Off-Ball xT.

---

### Task 4: Merge redundant constants (Finding #8)

**Files:**
- Modify: `taipy_spike/src/template.py`

- [ ] **Step 1: Merge `_TRACKING_PAGES` and `_PROVIDER_PAGES`**

Both are `("Movement-Pressing", "Pitch-Control")`. Replace with a single constant:

```python
_TRACKING_PROVIDER_PAGES = ("Movement-Pressing", "Pitch-Control")
```

Update all references:
- `condition=f"current_page in {_PROVIDER_PAGES}"` → `condition=f"current_page in {_TRACKING_PROVIDER_PAGES}"`
- `condition=f"current_page in {_TRACKING_PAGES}"` → `condition=f"current_page in {_TRACKING_PROVIDER_PAGES}"`
- Delete the two old constants

- [ ] **Step 2: Verify no broken conditions**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from template import build_root_page; print('OK')"`

---

### Task 5: Fix inconsistent PageConfig indentation (Finding #9)

**Files:**
- Modify: all 12 files in `taipy_spike/src/pages/`

All page files have misaligned indentation — `citations=`, `image_var=`, etc. are indented 8 spaces while `title=`, `icon=`, `nav_section=` use 4. Fix to consistent 4-space indent on all `PageConfig` fields.

- [ ] **Step 1: Fix indentation in all 12 page files**

For each file, ensure all keyword arguments inside `PageConfig(...)` use the same indentation level (4 spaces from the `PageConfig(` line). Example:

```python
page_config = PageConfig(
    title="Shot Map",
    icon="target",
    nav_section="Match Analysis",
    description="Shot locations sized by xG with isotonic calibration.",
    citations=[
        Citation("Rathke (2017)", "https://doi.org/10.1515/jqas-2019-0044"),
        Citation("XGBoost", "https://xgboost.readthedocs.io/"),
    ],
    image_var="sm_pitch_image",
    empty_message="Select a competition to begin.",
    ...
)
```

Files to fix: `shot_map.py`, `pass_map.py`, `heat_map.py`, `pass_network.py`, `match_summary.py`, `action_values.py`, `player_radar.py`, `player_similarity.py`, `movement_analysis.py`, `pitch_control.py`, `pass_timing.py`, `defensive_valuation.py`.

- [ ] **Step 2: Verify all pages import cleanly**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from pages.shot_map import page_config; from pages.pass_map import page_config; from pages.heat_map import page_config; from pages.pass_network import page_config; from pages.match_summary import page_config; from pages.action_values import page_config; from pages.player_radar import page_config; from pages.movement_analysis import page_config; from pages.pitch_control import page_config; from pages.pass_timing import page_config; from pages.defensive_valuation import page_config; from pages.player_similarity import page_config; print('All 12 OK')"`

---

### Task 6: Visual verification via Puppeteer against Chrome

**Files:** None (verification only)

- [ ] **Step 1: Verify glossary panels**

Navigate to Shot Map, click Glossary. Verify "xG (Expected Goals)" and "Brier Score" appear. Navigate to Defensive Impact, click Glossary. Verify DEFCON terms appear.

- [ ] **Step 2: Verify sub-view switching on Movement & Pressing**

Navigate to Movement & Pressing. Verify Physical Performance / PPDA / Off-Ball xT sub-views render when selected.

- [ ] **Step 3: Verify scale notes on Defensive Impact**

Navigate to Defensive Impact. Verify "Requires StatsBomb 360..." reference text appears above the Rankings table.

- [ ] **Step 4: Verify Player Impact scale note**

Navigate to Player Impact. Verify "VAEP/90: higher = more impactful..." reference text appears above Rankings table.

- [ ] **Step 5: Full nav + page scan**

Click through all 12 pages verifying no rendering regressions.

---

## Findings NOT addressed (intentional)

| # | Finding | Rationale for deferring |
|---|---------|----------------------|
| 3 | `pre_image_content` used as full-page escape hatch in `player_radar.py` and `player_similarity.py` | These pages have genuinely unique layouts (conditional blocks + tables + radar images). A template abstraction would be over-engineered for 2 pages. Documented in Task 2 Step 6. |
| 4 | Repeated 2-up chart grid in `match_summary.py` and `pass_timing.py` | Only 2 pages use this pattern. A `paired_images` field is YAGNI until a third page needs it. The raw `<|layout|>` in `pre_image_content` is acceptable per the documented escape hatch contract. |
| 7 | `empty_condition` referencing global variables in `heat_map.py` / `movement_analysis.py` | These references are correct (the conditions check global LOV availability). The inconsistency is cosmetic — the right variable is used for the right purpose. |
