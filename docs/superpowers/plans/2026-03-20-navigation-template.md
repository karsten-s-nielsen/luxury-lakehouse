# Navigation Template Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hardcoded sidebar navigation with template-driven nav generated from `PageConfig`, making icon/label/section a single source of truth shared between nav and page headers.

**Architecture:** Add `nav_section` field to `PageConfig`, introduce `PageEntry` dataclass and `build_nav()` in `page_template.py`. Convert `template.py`'s `root_page` from a static f-string into a `build_root_page(nav_md)` function. `main.py` orchestrates: builds registry, generates nav, constructs root page, derives Taipy `pages` dict.

**Tech Stack:** Taipy 4.x, Python 3.10, dataclasses, Puppeteer/Chrome for visual verification.

**Spec:** `docs/superpowers/specs/2026-03-20-navigation-template-design.md`

---

### Task 1: Add `nav_section` to `PageConfig` and `PageEntry` + `build_nav()` to `page_template.py`

**Files:**
- Modify: `taipy_spike/src/page_template.py`

- [ ] **Step 1: Add `nav_section` field to `PageConfig`**

Insert `nav_section: str` as the third field (after `icon`, before `description`):

```python
@dataclass(frozen=True)
class PageConfig:
    title: str
    icon: str
    nav_section: str  # "Match Analysis", "Player Analysis", "Advanced"
    description: str
    # ... rest unchanged
```

- [ ] **Step 2: Add `PageEntry` dataclass**

Add below the `PageConfig` class (before `SubView`):

```python
@dataclass(frozen=True)
class PageEntry:
    """One page in the navigation registry. Order in the registry list = display order."""

    route: str
    config: PageConfig
    markdown: str
```

- [ ] **Step 3: Add `build_nav()` function**

Add after `build_sidebar_section()`:

```python
def build_nav(registry: list[PageEntry]) -> str:
    """Generate sidebar nav markdown from page registry.

    Groups pages by nav_section, preserving registration order.
    Section header order = first page encountered for each section.
    """
    sections: dict[str, list[PageEntry]] = {}
    for entry in registry:
        sections.setdefault(entry.config.nav_section, []).append(entry)

    parts: list[str] = []
    for section_name, entries in sections.items():
        parts.append(f"<|part|class_name=ll-nav-header|\n**{section_name}**\n|>\n")
        for entry in entries:
            parts.append(
                f'[<span class="material-symbols-outlined">{entry.config.icon}</span>'
                f" {entry.config.title}](/{entry.route})\n"
            )
    return "\n".join(parts)
```

- [ ] **Step 4: Verify syntax**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from page_template import PageConfig, PageEntry, build_nav; print('OK')"`

Expected: `OK`

---

### Task 2: Update all 12 page files — extract `page_config`, add `nav_section`

**Files:**
- Modify: all 12 files in `taipy_spike/src/pages/`

Each page follows the same mechanical transformation:
1. Extract the `PageConfig(...)` to a named `page_config` variable.
2. Add `nav_section="..."` as the third argument.
3. `page_md = build_page(page_config)`.

Section assignments:
| Page file | `nav_section` |
|-----------|---------------|
| `shot_map.py` | `"Match Analysis"` |
| `pass_map.py` | `"Match Analysis"` |
| `heat_map.py` | `"Match Analysis"` |
| `pass_network.py` | `"Match Analysis"` |
| `match_summary.py` | `"Match Analysis"` |
| `action_values.py` | `"Player Analysis"` |
| `player_radar.py` | `"Player Analysis"` |
| `player_similarity.py` | `"Player Analysis"` |
| `movement_analysis.py` | `"Advanced"` |
| `pitch_control.py` | `"Advanced"` |
| `pass_timing.py` | `"Advanced"` |
| `defensive_valuation.py` | `"Advanced"` |

- [ ] **Step 1: Update `pages/shot_map.py`**

Change from:
```python
page_md = build_page(
    PageConfig(
        title="Shot Map",
        icon="target",
        description=...
```

To:
```python
page_config = PageConfig(
    title="Shot Map",
    icon="target",
    nav_section="Match Analysis",
    description=...
)
page_md = build_page(page_config)
```

- [ ] **Step 2: Update `pages/pass_map.py`** — same pattern, `nav_section="Match Analysis"`

- [ ] **Step 3: Update `pages/heat_map.py`** — same pattern, `nav_section="Match Analysis"`

- [ ] **Step 4: Update `pages/pass_network.py`** — same pattern, `nav_section="Match Analysis"`

- [ ] **Step 5: Update `pages/match_summary.py`** — same pattern, `nav_section="Match Analysis"`

- [ ] **Step 6: Update `pages/action_values.py`** — same pattern, `nav_section="Player Analysis"`

- [ ] **Step 7: Update `pages/player_radar.py`** — same pattern, `nav_section="Player Analysis"`

- [ ] **Step 8: Update `pages/player_similarity.py`** — same pattern, `nav_section="Player Analysis"`

- [ ] **Step 9: Update `pages/movement_analysis.py`** — same pattern, `nav_section="Advanced"`

- [ ] **Step 10: Update `pages/pitch_control.py`** — same pattern, `nav_section="Advanced"`

- [ ] **Step 11: Update `pages/pass_timing.py`** — same pattern, `nav_section="Advanced"`

- [ ] **Step 12: Update `pages/defensive_valuation.py`** — same pattern, `nav_section="Advanced"`

- [ ] **Step 13: Verify all pages import cleanly**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from pages.shot_map import page_config, page_md; from pages.pass_map import page_config; from pages.heat_map import page_config; from pages.pass_network import page_config; from pages.match_summary import page_config; from pages.action_values import page_config; from pages.player_radar import page_config; from pages.movement_analysis import page_config; from pages.pitch_control import page_config; from pages.pass_timing import page_config; from pages.defensive_valuation import page_config; from pages.player_similarity import page_config; print('All 12 OK')"`

Expected: `All 12 OK`

---

### Task 3: Convert `template.py` — `root_page` string to `build_root_page()` function

**Files:**
- Modify: `taipy_spike/src/template.py`

The goal: replace the hardcoded nav block (lines 389–423) with a `{nav_md}` placeholder, and wrap `root_page` construction in a function that accepts the generated nav markdown.

- [ ] **Step 1: Change `root_page` f-string to `build_root_page()` function**

Replace the `root_page = f"""...` assignment (line 384 onward) with:

```python
def build_root_page(nav_md: str) -> str:
    """Build the root page shell with generated navigation."""
    return f"""
<|layout|columns=300px 1fr|gap=0.75rem|

<|part|class_name=sidebar|

{nav_md}

{_filter_section}
{_search_section}

|>

<|part|

<|part|class_name=ll-title-row|
<|layout|columns=1fr auto auto|gap=0.5rem|class_name=align-columns-center|

<|part|class_name=ll-brand|
# <span class="material-symbols-outlined">sports_soccer</span> (Right! Luxury!) Lakehouse
|>

<|Glossary|button|class_name=text-no-transform ll-header-btn ll-btn-glossary|on_action=toggle_glossary|>

<|Getting Started|button|class_name=text-no-transform ll-header-btn ll-btn-started|on_action=toggle_getting_started|>

|>
|>

... (all glossary panels, getting started, loading overlay, content, footer — unchanged)

|>

|>
"""
```

The ONLY change to the f-string body is: remove the hardcoded nav block (the `<|part|class_name=ll-nav-header|` sections with `[<span ...>...]` links, lines 389–423) and replace with `{nav_md}`.

Everything else in the f-string stays exactly as-is — glossary panels (lines 446–538), brand bar (lines 430–443), buttons, loading overlay (lines 544–548), `<|content|>` (line 550), footer (lines 552–553). Preserve all of lines 425–558 verbatim.

Note: `_filter_section` and `_search_section` are module-level variables computed at import time (lines 377–382). They are captured by the f-string inside `build_root_page()` via closure — this is correct and unchanged from the current behavior.

- [ ] **Step 2: Remove the `from template import root_page` expectation**

`template.py` no longer exports `root_page` as a module-level variable. It exports `build_root_page` instead. Update the module docstring if needed.

- [ ] **Step 3: Verify `build_root_page` is callable**

Run: `cd taipy_spike && .venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from template import build_root_page; print(type(build_root_page)); r = build_root_page('NAV_PLACEHOLDER'); assert 'NAV_PLACEHOLDER' in r; assert 'content' in r; print('OK')"`

Expected: `<class 'function'>` then `OK`

---

### Task 4: Rewire `main.py` — build registry, generate nav, construct root page

**Files:**
- Modify: `taipy_spike/src/main.py`

- [ ] **Step 1: Update imports**

Change page imports to import both `page_config` and `page_md` from each page module. Change template import from `root_page` to `build_root_page`. Add `PageEntry` and `build_nav` imports.

```python
from page_template import PageEntry, build_nav

from pages.shot_map import page_config as shot_map_config, page_md as shot_map_page
from pages.pass_map import page_config as pass_map_config, page_md as pass_map_page
from pages.heat_map import page_config as heat_map_config, page_md as heat_map_page
from pages.pass_network import page_config as pass_network_config, page_md as pass_network_page
from pages.match_summary import page_config as match_summary_config, page_md as match_summary_page
from pages.action_values import page_config as action_values_config, page_md as action_values_page
from pages.player_radar import page_config as player_radar_config, page_md as player_radar_page
from pages.player_similarity import page_config as player_similarity_config, page_md as player_similarity_page
from pages.movement_analysis import page_config as movement_config, page_md as movement_page
from pages.pitch_control import page_config as pitch_control_config, page_md as pitch_control_page
from pages.pass_timing import page_config as pass_timing_config, page_md as pass_timing_page
from pages.defensive_valuation import page_config as defensive_impact_config, page_md as defensive_impact_page

from pages.widget_spacing_test import *  # noqa: F403
from pages.widget_spacing_test import page_md as spacing_test_page

from template import build_root_page
```

- [ ] **Step 2: Build `PAGE_REGISTRY`**

Replace the `pages = {...}` dict with:

```python
# --- Page registry (ordered) ---
# List order = nav display order. Section headers appear in order of first occurrence.
PAGE_REGISTRY: list[PageEntry] = [
    # Match Analysis
    PageEntry("Shot-Map", shot_map_config, shot_map_page),
    PageEntry("Pass-Map", pass_map_config, pass_map_page),
    PageEntry("Heat-Map", heat_map_config, heat_map_page),
    PageEntry("Pass-Network", pass_network_config, pass_network_page),
    PageEntry("Match-Summary", match_summary_config, match_summary_page),
    # Player Analysis
    PageEntry("Player-Impact", action_values_config, action_values_page),
    PageEntry("Player-Comparison", player_radar_config, player_radar_page),
    PageEntry("Player-Similarity", player_similarity_config, player_similarity_page),
    # Advanced
    PageEntry("Movement-Pressing", movement_config, movement_page),
    PageEntry("Pitch-Control", pitch_control_config, pitch_control_page),
    PageEntry("Pass-Timing", pass_timing_config, pass_timing_page),
    PageEntry("Defensive-Impact", defensive_impact_config, defensive_impact_page),
]

# Generate nav and root page
_nav_md = build_nav(PAGE_REGISTRY)
root_page = build_root_page(_nav_md)

# Build Taipy pages dict
pages: dict[str, str] = {"/": root_page}
pages.update({entry.route: entry.markdown for entry in PAGE_REGISTRY})
pages["Widget-Spacing-Test"] = spacing_test_page  # dev-only, not in nav
```

- [ ] **Step 3: Update `test_render.py`**

`test_render.py` (lines 244–274) has its own page imports and `pages` dict that mirrors `main.py`. It also imports `root_page` from `template`. Update it to use the new pattern:

```python
# Replace lines 244-274 with:

# Import ALL pages
from pages.shot_map import page_config as shot_map_config, page_md as shot_map_page
from pages.pass_map import page_config as pass_map_config, page_md as pass_map_page
from pages.heat_map import page_config as heat_map_config, page_md as heat_map_page
from pages.pass_network import page_config as pass_network_config, page_md as pass_network_page
from pages.match_summary import page_config as match_summary_config, page_md as match_summary_page
from pages.action_values import page_config as action_values_config, page_md as action_values_page
from pages.player_radar import page_config as player_radar_config, page_md as player_radar_page
from pages.player_similarity import page_config as player_similarity_config, page_md as player_similarity_page
from pages.movement_analysis import page_config as movement_config, page_md as movement_page
from pages.pitch_control import page_config as pitch_control_config, page_md as pitch_control_page
from pages.pass_timing import page_config as pass_timing_config, page_md as pass_timing_page
from pages.defensive_valuation import page_config as defensive_impact_config, page_md as defensive_impact_page
from page_template import PageEntry, build_nav
from template import build_root_page

from taipy.gui import Gui

PAGE_REGISTRY: list[PageEntry] = [
    PageEntry("Shot-Map", shot_map_config, shot_map_page),
    PageEntry("Pass-Map", pass_map_config, pass_map_page),
    PageEntry("Heat-Map", heat_map_config, heat_map_page),
    PageEntry("Pass-Network", pass_network_config, pass_network_page),
    PageEntry("Match-Summary", match_summary_config, match_summary_page),
    PageEntry("Player-Impact", action_values_config, action_values_page),
    PageEntry("Player-Comparison", player_radar_config, player_radar_page),
    PageEntry("Player-Similarity", player_similarity_config, player_similarity_page),
    PageEntry("Movement-Pressing", movement_config, movement_page),
    PageEntry("Pitch-Control", pitch_control_config, pitch_control_page),
    PageEntry("Pass-Timing", pass_timing_config, pass_timing_page),
    PageEntry("Defensive-Impact", defensive_impact_config, defensive_impact_page),
]

root_page = build_root_page(build_nav(PAGE_REGISTRY))
pages: dict[str, str] = {"/": root_page}
pages.update({entry.route: entry.markdown for entry in PAGE_REGISTRY})
```

Note: `test_render.py` is a dev-only file (not committed) used for mock-data testing without a DB connection. It runs on port 7861.

- [ ] **Step 4: Verify the app starts**

Run: `cd taipy_spike/src && LAKEBASE_HOST="ep-spring-rain-d2i6lozx.database.us-east-1.cloud.databricks.com" LAKEBASE_ENDPOINT_NAME="projects/soccer-analytics-dev/branches/production/endpoints/primary" ..\\.venv\\Scripts\\python.exe main.py`

Expected: Taipy starts on port 7860 without errors.

---

### Task 5: Visual verification via Puppeteer against Chrome

**Files:** None (verification only)

- [ ] **Step 1: Screenshot Shot Map nav (Match Analysis section, active highlight)**

Navigate Puppeteer to `http://localhost:7860/Shot-Map`. Take a 400x600 screenshot. Verify:
- MATCH ANALYSIS header visible
- Shot Map has active highlight (lighter blue box)
- All 5 Match Analysis pages listed in order
- PLAYER ANALYSIS and ADVANCED sections follow

- [ ] **Step 2: Screenshot Pitch Control nav (Advanced section, active highlight)**

Navigate to `http://localhost:7860/Pitch-Control`. Verify:
- Pitch Control has active highlight under ADVANCED
- All sections and pages present

- [ ] **Step 3: Screenshot Player Similarity nav (Player Analysis section)**

Navigate to `http://localhost:7860/Player-Similarity`. Verify:
- Player Similarity has active highlight under PLAYER ANALYSIS

- [ ] **Step 4: Verify page header matches nav**

For Shot Map: compare nav label ("Shot Map" with target icon) against page header. Both should show identical icon and title — both sourced from `page_config`.

- [ ] **Step 5: Verify Widget Spacing Test is NOT in nav but IS routable**

Navigate to `http://localhost:7860/Widget-Spacing-Test`. Verify:
- Page loads successfully
- "Widget Spacing Test" does NOT appear in the sidebar nav

- [ ] **Step 6: Verify no hardcoded nav links remain**

Run: `grep -n "Shot Map\|Pass Map\|Heat Map\|Pass Network\|Match Summary\|Player Impact\|Player Comparison\|Player Similarity\|Movement.*Pressing\|Pitch Control\|Pass Timing\|Defensive Impact" taipy_spike/src/template.py | grep -v "glossary\|GLOSSARY\|PAGE_TERMS\|show_glossary\|GETTING_STARTED\|Suggested" | head -20`

Expected: Only glossary panel references remain — zero nav link references.
