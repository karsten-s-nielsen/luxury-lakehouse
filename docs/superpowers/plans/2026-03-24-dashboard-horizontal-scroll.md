# Dashboard Horizontal Scroll Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dashboard pages (AI/ML Workflows, future Observability) get a single horizontal scrollbar at the container level when the viewport is narrower than the content. No individual scrollbars on DAG or table. DAG always renders at 1:1 zoom. Content is left-aligned.

**Architecture:** Three-layer approach — CSS defines the scroll surface and kills child scrollbars, the template wraps dashboard content in the scroll container, and the DAG JS renders at 1:1 and propagates its natural width through the iframe parent chain so the scroll wrapper knows when to show a scrollbar.

**Tech Stack:** Taipy markdown templates, CSS, Cytoscape.js (DAG iframe)

**DOM context (verified via Puppeteer):** The DAG iframe is 6 levels deep from the scroll wrapper:
- Depth 0: `<iframe>` (frameElement)
- Depth 1: `taipy-part MuiBox-root` div
- Depth 2: `md-para` div
- Depth 3: `taipy-part ll-dag-container` div
- Depth 4: `taipy-part MuiBox-root` div (render-condition wrapper from template)
- Depth 5: `taipy-part ll-dashboard-scroll` div (scroll wrapper — **target**)

---

### Task 1: CSS — scroll wrapper and child scrollbar suppression

**Files:**
- Modify: `hf_taipy_app/src/style_v2.css:776-783`

- [ ] **Step 1: Add scroll wrapper CSS and modify DAG container**

Replace the existing `.ll-dag-container` block (lines 776–783) with:

```css
/* Dashboard scroll wrapper — single horizontal scrollbar for the entire
   dashboard body (stats, DAG, table).  Content blocks propagate their
   natural width as min-width; overflow-x: auto triggers the scrollbar. */
.ll-dashboard-scroll {
    overflow-x: auto !important;
}

/* Kill table-level scrollbars inside dashboard pages — the scroll wrapper
   is the ONLY scroll surface for horizontal overflow. */
.ll-dashboard-scroll .MuiTableContainer-root {
    overflow: visible !important;
    max-height: none !important;
}

/* DAG container — overflow visible so the iframe can extend to its natural
   width, which the scroll wrapper then makes scrollable.
   margin: auto centers the DAG when its natural width is narrower than
   the table (the widest sibling determines scroll container width). */
.ll-dag-container {
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 8px !important;
    margin-bottom: 1.5rem !important;
    margin-left: auto !important;
    margin-right: auto !important;
    overflow: visible !important;
    background: rgba(255, 255, 255, 0.02) !important;
}
```

Key changes from committed version:
- New `.ll-dashboard-scroll` rule with `overflow-x: auto`
- New `.ll-dashboard-scroll .MuiTableContainer-root` rule killing table scrollbars
- `.ll-dag-container` changed from `overflow: hidden` to `overflow: visible`
- `.ll-dag-container` gets `margin-left/right: auto` for centering when DAG is narrower than table

- [ ] **Step 2: Verify CSS parses**

Run: `cd hf_taipy_app && python -c "print('CSS is not Python-parseable, visual verification needed')"`

CSS changes are verified via Puppeteer in Task 4.

---

### Task 2: Template — wrap dashboard body in scroll container

**Files:**
- Modify: `hf_taipy_app/src/page_template.py:662-681`

- [ ] **Step 1: Wrap dashboard body in ll-dashboard-scroll part**

In `build_page()`, replace the `elif cfg.stats:` branch (lines 662–681) with:

```python
    elif cfg.stats:
        # Dashboard page: stats bar + full-width content blocks.
        # Wrap in a horizontal-scroll container so narrow viewports
        # get a scrollbar instead of clipping the DAG / table.
        parts.append("<|part|class_name=ll-dashboard-scroll|")
        parts.append("")
        parts.append(_build_stats_bar(cfg.stats))
        parts.append("")

        for row in cfg.content:
            parts.append(_build_content_row(row, cfg.title))
            parts.append("")

        if cfg.empty_condition:
            parts.append(f"<|part|render={{{cfg.empty_condition}}}|class_name=ll-info-box|")
            parts.append(f"{cfg.empty_message}")
            parts.append("|>")

        if cfg.warning_var:
            parts.append(f"<|part|render={{len({cfg.warning_var}) > 0}}|class_name=ll-warning-box|")
            parts.append(f"<|{{{cfg.warning_var}}}|text|>")
            parts.append("|>")

        parts.append("|>")  # close ll-dashboard-scroll
```

Only change from committed version: opening `<|part|class_name=ll-dashboard-scroll|` before the stats bar, and closing `|>` after the warning block.

- [ ] **Step 2: Verify generated markdown structure**

Run:
```bash
cd hf_taipy_app && .venv/Scripts/python -c "
import sys; sys.path.insert(0, 'src')
from pages.workflows import _dashboard_md
# Confirm scroll wrapper opens and closes
assert 'll-dashboard-scroll' in _dashboard_md
# Count opening vs closing to verify nesting
opens = _dashboard_md.count('<|part|class_name=ll-dashboard-scroll|')
print(f'Scroll wrapper opens: {opens}')
print('OK')
"
```

Expected: `Scroll wrapper opens: 1` and `OK`.

---

### Task 3: DAG JS — 1:1 zoom and min-width propagation

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py:502-538`

- [ ] **Step 1: Replace `_fitAndResize` JS function**

Replace lines 502–538 (the `_fitAndResize` function and iframe parent loop) with:

```python
        f"    // After layout: render at 1:1 (no zoom scaling).  Set container\n"
        f"    // to the graph's natural width.  Propagate min-width through the\n"
        f"    // iframe parent chain up to the ll-dashboard-scroll wrapper so\n"
        f"    // the scroll wrapper triggers a horizontal scrollbar on narrow\n"
        f"    // viewports instead of crushing the graph.\n"
        f"    function _fitAndResize() {{\n"
        f"        var pad = 30;\n"
        f"\n"
        f"        if (cy.elements().length === 0) {{\n"
        f"            // Empty graph — collapse container\n"
        f"            container.style.height = '0px';\n"
        f"        }} else {{\n"
        f"            var bb = cy.elements().boundingBox();\n"
        f"            var naturalW = Math.ceil(bb.w) + pad * 2;\n"
        f"            var naturalH = Math.ceil(bb.h) + pad * 2;\n"
        f"            container.style.height = Math.max(120, naturalH) + 'px';\n"
        f"            cy.resize();\n"
        f"            cy.zoom(1);\n"
        f"            cy.center();\n"
        f"        }}\n"
        f"\n"
        f"        // Propagate height and min-width through iframe parent chain.\n"
        f"        // Walk up to the ll-dashboard-scroll wrapper (max 8 as safety cap).\n"
        f"        // Height: exact content size.  Min-width: natural graph width so\n"
        f"        // the scroll wrapper knows when content exceeds the viewport.\n"
        f"        var legendEl = container.nextElementSibling;\n"
        f"        var legendH = legendEl ? legendEl.offsetHeight : 0;\n"
        f"        var bs = getComputedStyle(document.body);\n"
        f"        var bodyM = parseInt(bs.marginTop) + parseInt(bs.marginBottom);\n"
        f"        var totalH = container.offsetHeight + legendH + bodyM;\n"
        f"        var bb2 = cy.elements().length > 0 ? cy.elements().boundingBox() : null;\n"
        f"        var minW = bb2 ? Math.ceil(bb2.w) + pad * 2 : 0;\n"
        f"        if (window.frameElement) {{\n"
        f"            var el = window.frameElement;\n"
        f"            for (var i = 0; i < 8 && el; i++) {{\n"
        f"                el.style.setProperty('height', totalH + 'px', 'important');\n"
        f"                el.style.setProperty('min-height', 'auto', 'important');\n"
        f"                if (minW > 0) {{\n"
        f"                    el.style.setProperty('min-width', minW + 'px', 'important');\n"
        f"                }}\n"
        f"                if (el.classList && el.classList.contains('ll-dashboard-scroll')) break;\n"
        f"                el = el.parentElement;\n"
        f"            }}\n"
        f"        }}\n"
        f"    }}\n"
```

Key changes from committed version:
- Removed zoom-to-fit: no `Math.min(1, containerW / ...)`, always `cy.zoom(1)`
- Container height uses natural height (not zoom-scaled)
- Removed `containerW` variable (not needed without zoom-to-fit)
- Added `minW` computation from bounding box
- Loop walks 8 levels (was 4) and stops at `ll-dashboard-scroll` class
- Each parent gets both `height` and `min-width`

- [ ] **Step 2: Verify Python syntax**

Run:
```bash
cd hf_taipy_app && .venv/Scripts/python -c "
import sys; sys.path.insert(0, 'src')
from state.workflows import _build_dag_html
print('Import OK')
"
```

Expected: `Import OK` (no syntax errors in the f-string).

---

### Task 4: Puppeteer verification — full width

**Files:** None (verification only)

- [ ] **Step 1: Start Taipy app**

```bash
cd hf_taipy_app && .venv/Scripts/python src/main.py --port 5098 --no-reloader
```

Wait for `Server starting on http://localhost:5098`.

- [ ] **Step 2: Navigate to workflows page at full width (1400px)**

Using Puppeteer MCP: navigate to `http://localhost:5098/AI-ML-Workflows`, wait 10s for data load.

- [ ] **Step 3: Screenshot at 1400px width**

Take screenshot at 1400×1200. Verify:
- DAG renders at 1:1 (node labels fully readable, graph left-aligned)
- Stats bar shows live data (16 workflows, freshness, cost)
- Table renders below DAG with "Last Duration" column
- No horizontal scrollbar at full width (content fits)

- [ ] **Step 4: Check scroll wrapper state at full width**

Execute JS:
```javascript
const s = document.querySelector('.ll-dashboard-scroll');
JSON.stringify({scrollWidth: s.scrollWidth, clientWidth: s.clientWidth, isScrollable: s.scrollWidth > s.clientWidth});
```

Expected: `isScrollable: false` (content fits at full width).

---

### Task 5: Puppeteer verification — narrow width + scroll

- [ ] **Step 1: Navigate at narrow viewport (700px)**

Re-navigate to the page with Puppeteer viewport set to 700×900 (or use `defaultViewport` launch option). Wait 10s.

- [ ] **Step 2: Check scroll wrapper is scrollable**

Execute JS:
```javascript
const s = document.querySelector('.ll-dashboard-scroll');
JSON.stringify({scrollWidth: s.scrollWidth, clientWidth: s.clientWidth, isScrollable: s.scrollWidth > s.clientWidth});
```

Expected: `isScrollable: true` (DAG's natural width exceeds viewport).

- [ ] **Step 3: Check NO individual scrollbars on table**

Execute JS:
```javascript
const t = document.querySelector('.ll-dashboard-scroll .MuiTableContainer-root');
const style = t ? getComputedStyle(t) : null;
JSON.stringify({overflow: style?.overflow, overflowX: style?.overflowX});
```

Expected: `overflow: visible`.

- [ ] **Step 4: Scroll right and screenshot**

Execute JS: `document.querySelector('.ll-dashboard-scroll').scrollLeft = 400;`
Take screenshot at 700×900. Verify:
- Right side of DAG is now visible (Model & Pipeline Validation node, etc.)
- Stats bar also scrolled (TOTAL COST, RUN VOLUME cards visible)
- Table also scrolled (right columns visible)
- Everything scrolled together as one unit

- [ ] **Step 5: Verify min-width on parent chain**

Execute JS:
```javascript
const iframes = document.querySelectorAll('.ll-dag-container iframe');
const chain = [];
let el = iframes[0];
for (let i = 0; i < 8 && el; i++) {
  chain.push({depth: i, minWidth: el.style.minWidth, class: el.className?.toString().substring(0,50)});
  if (el.classList?.contains('ll-dashboard-scroll')) break;
  el = el.parentElement;
}
JSON.stringify(chain);
```

Expected: min-width set on each element in the chain, stopping at `ll-dashboard-scroll`.

---

### Task 6: Fallback — if min-width propagation fails

**Only execute this task if Task 5 Step 2 shows `isScrollable: false` at narrow width.**

If Taipy re-renders clear inline min-width styles, the fallback is a Python-computed width estimate:

- Add `_estimate_dag_width(cards)` function to `state/workflows.py`
- Add `wf_dag_min_width` state variable
- Inject a `<style>` tag inside the DAG HTML targeting `.ll-dashboard-scroll > .MuiBox-root { min-width: Xpx }`
- This is a last resort — Task 3's approach is preferred because it uses the exact measured width.

---

### Task 7: Stop Taipy test instance

- [ ] **Step 1: Stop test server**

```bash
pkill -f "python src/main.py --port 5098"
```
