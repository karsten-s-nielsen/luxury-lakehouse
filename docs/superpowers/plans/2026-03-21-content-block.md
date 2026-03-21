# ContentBlock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all raw markdown escape hatches in the Taipy spike's content area with a typed `ContentBlock` + `ContentRow` model, add sticky metrics, and add expandable tables.

**Architecture:** Add `ContentBlock` and `ContentRow` frozen dataclasses to `page_template.py`. Update `build_page()` and `_build_sub_view()` to render content from these typed lists. Migrate all 12 pages from old fields (`image_var`, `pre_image_content`, `post_content`) to the new `content: list[ContentRow]` field. Add CSS for content row grids, sticky metrics, and expandable tables.

**Tech Stack:** Python 3.10, Taipy 4.1, frozen dataclasses, CSS grid, Puppeteer for validation

**Spec:** `docs/superpowers/specs/2026-03-21-content-block-design.md`

---

### Task 1: Add ContentBlock and ContentRow dataclasses

**Files:**
- Modify: `taipy_spike/src/page_template.py`

- [ ] **Step 1: Add ContentBlock dataclass after the SidebarWidget section**

Add after the `_build_render_condition` / `_build_sidebar_widget` / `build_sidebar_section` block, before the `Metric` class:

```python
@dataclass(frozen=True)
class ContentBlock:
    """One piece of content in the diagram area.

    Pages provide only data — kind, variable, optional header/caption.
    The template controls all structural wrapping and styling.
    """

    kind: Literal["image", "table", "text", "expandable_table"]
    var: str  # state variable name
    header: str = ""  # optional subtitle above the block (ll-subtitle)
    condition: str = ""  # Taipy render condition (empty = auto len(var) > 0)
    table_page_size: int = 50  # table and expandable_table only
    caption: str = ""  # static text below the block
    caption_var: str = ""  # dynamic caption (state variable)
    caption_condition: str = ""  # render condition for caption


@dataclass(frozen=True)
class ContentRow:
    """One horizontal row of ContentBlocks in the diagram area.

    Single block = full width. Multiple blocks = CSS grid columns.
    Use explicit `columns` to force alignment across consecutive rows.
    """

    blocks: list[ContentBlock] = field(default_factory=list)
    columns: int = 0  # 0 = auto (len(blocks)), explicit forces grid column count
    condition: str = ""  # row-level render condition (gates entire row)
```

- [ ] **Step 2: Add `content` field to PageConfig, keep old fields temporarily**

```python
class PageConfig:
    # ... existing fields unchanged ...
    content: list[ContentRow] = field(default_factory=list)
    # image_var, pre_image_content kept temporarily for backward compat
```

- [ ] **Step 3: Add `content` field to SubView, keep old fields temporarily**

```python
class SubView:
    # ... existing fields unchanged ...
    content: list[ContentRow] = field(default_factory=list)
    # image_var, table_var, pre_content, post_content kept temporarily
```

- [ ] **Step 4: Add `_build_content_block()` helper function**

Add before `_build_sub_view()`:

```python
def _build_content_block(block: ContentBlock, page_title: str) -> str:
    """Generate markdown for a single content block.

    Auto-generates render condition len(var) > 0 when condition is empty.
    """
    parts: list[str] = []
    cond = block.condition or f"len({block.var}) > 0"

    # Outer wrapper with render condition
    parts.append(f"<|part|render={{{cond}}}|")

    # Optional header (subtitle above content — skip for expandable_table
    # where header is the toggle label, not a separate subtitle)
    if block.header and block.kind != "expandable_table":
        parts.append("<|part|class_name=ll-subtitle|")
        parts.append(block.header)
        parts.append("|>")

    # Content by kind
    if block.kind == "image":
        parts.append(f"<|{{{block.var}}}|image|label={page_title}|width=100%|>")
    elif block.kind == "table":
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}|>")
    elif block.kind == "text":
        parts.append(f"<|{{{block.var}}}|text|>")
    elif block.kind == "expandable_table":
        # header is required for expandable — used as toggle label
        parts.append(f"<|{block.header}|expandable|expanded=False|")
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}|>")
        parts.append("|>")

    parts.append("|>")  # close render condition

    # Optional caption (static or dynamic)
    if block.caption:
        parts.append("<|part|class_name=ll-reference|")
        parts.append(block.caption)
        parts.append("|>")
    if block.caption_var:
        cap_cond = block.caption_condition or f"len({block.caption_var}) > 0"
        parts.append(f"<|part|render={{{cap_cond}}}|class_name=ll-reference|")
        parts.append(f"<|{{{block.caption_var}}}|text|>")
        parts.append("|>")

    return "\n".join(parts)


def _build_content_row(row: ContentRow, page_title: str) -> str:
    """Generate markdown for a content row (one or more blocks).

    Single block = full width. Multiple blocks = CSS grid.
    """
    parts: list[str] = []

    # Row-level render condition
    if row.condition:
        parts.append(f"<|part|render={{{row.condition}}}|")

    n_cols = row.columns or len(row.blocks)
    if len(row.blocks) > 1:
        parts.append(f"<|part|class_name=ll-content-row ll-content-cols-{n_cols}|")

    for block in row.blocks:
        parts.append(_build_content_block(block, page_title))
        parts.append("")

    if len(row.blocks) > 1:
        parts.append("|>")  # close grid

    if row.condition:
        parts.append("|>")  # close row condition

    return "\n".join(parts)
```

- [ ] **Step 5: Update `build_page()` to use `content` field when present**

In the single-view branch of `build_page()`, after scope_vars rendering and before the empty state, add:

```python
        # Content rows (new model — replaces image_var / pre_image_content)
        if cfg.content:
            for row in cfg.content:
                parts.append(_build_content_row(row, cfg.title))
                parts.append("")
        elif cfg.pre_image_content:
            # Legacy: raw markdown escape hatch (will be removed after migration)
            parts.append(cfg.pre_image_content)
            parts.append("")
        elif cfg.image_var:
            # Legacy: single image (will be removed after migration)
            parts.append(f"<|part|render={{len({cfg.image_var}) > 0}}|")
            parts.append(f"<|{{{cfg.image_var}}}|image|label={cfg.title}|width=100%|>")
            parts.append("|>")
            parts.append("")
```

- [ ] **Step 6: Update `_build_sub_view()` to use `content` field when present**

In `_build_sub_view()`, in the left column section, replace the image_var/table_var/post_content block with:

```python
    if sv.content:
        # New model: typed content blocks
        for row in sv.content:
            parts.append(_build_content_row(row, page_title))
            parts.append("")
    else:
        # Legacy: image_var / table_var / post_content (will be removed)
        if sv.image_var:
            parts.append(f"<|part|render={{len({sv.image_var}) > 0}}|")
            parts.append(f"<|{{{sv.image_var}}}|image|label={page_title}|width=100%|>")
            parts.append("|>")
            parts.append("")
        if sv.table_var:
            parts.append(f"<|part|render={{len({sv.table_var}) > 0}}|")
            parts.append(f"<|{{{sv.table_var}}}|table|page_size={sv.table_page_size}|>")
            parts.append("|>")
            parts.append("")
```

- [ ] **Step 7: Update imports in `page_template.py` module docstring and exports**

Ensure `ContentBlock` and `ContentRow` are importable by page files.

- [ ] **Step 8: Verify server starts without errors**

Run: Start Taipy server, navigate to Shot Map, verify it renders with legacy path.

---

### Task 2: Add CSS for content rows and sticky metrics

**Files:**
- Modify: `taipy_spike/src/style_v2.css`

- [ ] **Step 1: Add content row CSS grid classes**

Add after the existing `.ll-grid-3-1` section:

```css
/* Content row — CSS grid for multi-block horizontal layouts */
.ll-content-row {
    display: grid !important;
    gap: 1rem !important;
    width: 100% !important;
}
.ll-content-cols-2 { grid-template-columns: 1fr 1fr !important; }

/* Fix Taipy table zero-height bug inside CSS grid cells */
.ll-content-row .MuiTableContainer-root {
    height: auto !important;
    max-height: 600px !important;
}
```

- [ ] **Step 2: Add sticky metrics CSS**

Add after the content row classes:

```css
/* Sticky metrics column — pins to viewport top while content scrolls.
   Scrolls naturally when metrics exceed viewport height (small screens). */
.ll-metrics-column {
    position: sticky;
    top: 4rem;
    align-self: start;
    max-height: calc(100vh - 5rem);
    overflow-y: auto;
}
/* NOTE: !important intentionally omitted — the CSS spike (Step 3) validates
   whether MUI parent overrides require it. Add !important only if needed. */
```

- [ ] **Step 3: Validate sticky metrics on Pitch Control via Puppeteer**

Navigate to Pitch Control, select a match, verify metrics column has sticky positioning. This is the CSS spike — if sticky doesn't work inside Taipy's MUI layout, we need to adjust before proceeding.

Note: The sticky class must be applied in the template. In `build_page()`, change the right column wrapper from `<|part|` to `<|part|class_name=ll-metrics-column|`. Same for `_build_sub_view()`.

- [ ] **Step 4: Apply `ll-metrics-column` class in template**

In `build_page()`, change the metrics column:
```python
# Before:
parts.append("<|part|")
# After:
parts.append("<|part|class_name=ll-metrics-column|")
```

Same in `_build_sub_view()`:
```python
# Before:
parts.append("<|part|")  # Right column: metrics
# After:
parts.append("<|part|class_name=ll-metrics-column|")
```

- [ ] **Step 5: Restart server and verify sticky behavior via Puppeteer**

Navigate to Pitch Control with a match loaded (7 metrics). Scroll the page. Verify metrics stay pinned at top while content scrolls. If sticky fails, check for MUI `overflow: hidden` on parent elements and add CSS overrides.

---

### Task 3: Migrate simple single-image pages (5 pages)

**Files:**
- Modify: `taipy_spike/src/pages/shot_map.py`
- Modify: `taipy_spike/src/pages/pass_map.py`
- Modify: `taipy_spike/src/pages/heat_map.py`
- Modify: `taipy_spike/src/pages/pass_network.py`
- Modify: `taipy_spike/src/pages/pitch_control.py`

- [ ] **Step 1: Migrate Shot Map**

Replace `image_var="sm_pitch_image"` with:
```python
from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

PageConfig(
    ...,
    # image_var removed
    content=[
        ContentRow([ContentBlock("image", "sm_pitch_image")]),
    ],
)
```

Remove `image_var` from the `PageConfig(...)` call. Keep all other fields unchanged.

- [ ] **Step 2: Migrate Pass Map**

Same pattern — replace `image_var="pm_pitch_image"` with `content=[ContentRow([ContentBlock("image", "pm_pitch_image")])]`.

- [ ] **Step 3: Migrate Heat Map**

Replace `image_var="hm_pitch_image"` with `content=[ContentRow([ContentBlock("image", "hm_pitch_image")])]`.

- [ ] **Step 4: Migrate Pass Network**

Replace `image_var="pn_pitch_image"` with `content=[ContentRow([ContentBlock("image", "pn_pitch_image")])]`.

- [ ] **Step 5: Migrate Pitch Control**

Replace `image_var="pc_pitch_image"` with `content=[ContentRow([ContentBlock("image", "pc_pitch_image")])]`.

- [ ] **Step 6: Restart server and verify all 5 pages via Puppeteer**

Navigate to each page, verify images render correctly with the new content path.

---

### Task 4: Migrate multi-view pages — Movement & Pressing (3 sub-views)

**Files:**
- Modify: `taipy_spike/src/pages/movement_analysis.py`

Note: The spec's migration map mentions expandable stats tables for Movement sub-views (porting from Streamlit's expander tables). These are deferred — the state variables (`ma_physical_stats_data`, etc.) don't exist yet. This task migrates only the existing image content. Expandable tables can be added when the state modules are updated.

- [ ] **Step 1: Migrate Physical Performance sub-view**

Replace `image_var="ma_physical_image"` with:
```python
content=[
    ContentRow([ContentBlock("image", "ma_physical_image")]),
],
```

- [ ] **Step 2: Migrate PPDA sub-view**

Replace `image_var="ma_ppda_image"` with:
```python
content=[
    ContentRow([ContentBlock("image", "ma_ppda_image")]),
],
```

- [ ] **Step 3: Migrate Off-Ball xT sub-view**

Replace `image_var="ma_oxt_image"` with:
```python
content=[
    ContentRow([ContentBlock("image", "ma_oxt_image")]),
],
```

- [ ] **Step 4: Verify via Puppeteer**

Navigate to Movement & Pressing, switch between all 3 sub-views, verify images render correctly.

---

### Task 5: Migrate multi-view pages — Player Impact / VAEP (3 sub-views)

**Files:**
- Modify: `taipy_spike/src/pages/action_values.py`

- [ ] **Step 1: Migrate Rankings sub-view**

Replace `table_var="av_rankings_data"` with:
```python
content=[
    ContentRow([ContentBlock("table", "av_rankings_data")]),
],
```

- [ ] **Step 2: Migrate Breakdown sub-view**

Replace `image_var="av_breakdown_image"` with:
```python
content=[
    ContentRow([ContentBlock("image", "av_breakdown_image")]),
],
```

- [ ] **Step 3: Migrate Timeline sub-view — image + expandable table**

Replace `image_var="av_timeline_image"` and `post_content=_TIMELINE_POST` with:
```python
content=[
    ContentRow([ContentBlock("image", "av_timeline_image")]),
    ContentRow([ContentBlock("expandable_table", "av_timeline_data",
        header="Action Details")]),
],
```

Remove the `_TIMELINE_POST` raw markdown constant.

- [ ] **Step 4: Verify via Puppeteer**

Navigate to Player Impact, switch between Rankings/Breakdown/Timeline. Verify expandable table on Timeline view opens and closes.

---

### Task 6: Migrate multi-view pages — Defensive Impact (3 sub-views)

**Files:**
- Modify: `taipy_spike/src/pages/defensive_valuation.py`

- [ ] **Step 1: Migrate Rankings sub-view**

Replace `table_var="dv_rankings_data"` with:
```python
content=[
    ContentRow([ContentBlock("table", "dv_rankings_data")]),
],
```

- [ ] **Step 2: Migrate Breakdown sub-view**

Replace `image_var="dv_breakdown_image"` with:
```python
content=[
    ContentRow([ContentBlock("image", "dv_breakdown_image")]),
],
```

- [ ] **Step 3: Migrate Timeline sub-view**

Replace `table_var="dv_timeline_data"` with:
```python
content=[
    ContentRow([ContentBlock("table", "dv_timeline_data")]),
],
```

- [ ] **Step 4: Verify via Puppeteer**

Navigate to Defensive Impact, verify all 3 sub-views render correctly.

---

### Task 7: Migrate Match Summary (2x2 chart grid)

**Files:**
- Modify: `taipy_spike/src/pages/match_summary.py`

- [ ] **Step 1: Replace raw markdown with ContentRow declarations**

Remove `_CHARTS_CONTENT` constant and `pre_image_content=_CHARTS_CONTENT`. Replace with:

```python
from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

PageConfig(
    ...,
    # image_var and pre_image_content removed
    content=[
        ContentRow([
            ContentBlock("image", "ms_shooting_chart"),
            ContentBlock("image", "ms_passing_chart"),
        ], columns=2, condition="len(ms_home_name) > 0"),
        ContentRow([
            ContentBlock("image", "ms_possession_chart"),
            ContentBlock("image", "ms_ppda_chart",
                caption="PPDA: Passes Per Defensive Action. Under 10 = aggressive pressing, over 15 = passive."),
        ], columns=2, condition="len(ms_home_name) > 0"),
    ],
)
```

- [ ] **Step 2: Verify via Puppeteer — 2x2 alignment**

Navigate to Match Summary, select a competition and match. Verify 4 charts render in a 2x2 grid with exact column alignment between rows. Check that PPDA caption appears below the pressing chart.

---

### Task 8: Migrate Pass Timing (stacked charts + table)

**Files:**
- Modify: `taipy_spike/src/pages/pass_timing.py`

**Intentional layout change:** The current implementation renders the scatter and heatmap charts side-by-side (`<|layout|columns=1 1|>`). Per user direction, this migration stacks them vertically for better readability. To revert to side-by-side later, move both image blocks into a single `ContentRow`.

- [ ] **Step 1: Replace raw markdown with ContentRow declarations**

Remove `_CHARTS_CONTENT` constant and `pre_image_content=_CHARTS_CONTENT`. Replace with:

```python
from page_template import Citation, ContentBlock, ContentRow, Metric, PageConfig, build_page

PageConfig(
    ...,
    # image_var and pre_image_content removed
    content=[
        ContentRow([ContentBlock("image", "pt_scatter_image")]),
        ContentRow([ContentBlock("image", "pt_heatmap_image")]),
        ContentRow([ContentBlock("table", "pt_rankings_data",
            header="Player Rankings",
            caption_var="pt_dfl_caption",
            caption_condition="pt_show_dfl_caption")]),
    ],
)
```

- [ ] **Step 2: Verify via Puppeteer**

Navigate to Pass Timing, select a match. Verify charts are stacked vertically (NOT side-by-side), "Player Rankings" subtitle appears above the table, and DFL caption appears conditionally.

---

### Task 9: Migrate Player Comparison (text blocks + radar + expandable stats)

**Files:**
- Modify: `taipy_spike/src/pages/player_radar.py`

- [ ] **Step 1: Replace raw markdown with ContentRow declarations**

Remove `_CONTENT` constant and `pre_image_content=_CONTENT`. Replace with:

```python
from page_template import Citation, ContentBlock, ContentRow, PageConfig, build_page

PageConfig(
    ...,
    # image_var and pre_image_content removed
    # empty_condition handles "Select a competition to begin"
    content=[
        ContentRow([ContentBlock("text", "pr_select_hint",
            condition='pr_comp_selected and pr_player_count == 0 and len(pr_no_data_warning) == 0')]),
        ContentRow([ContentBlock("text", "pr_no_data_warning")]),
        ContentRow([ContentBlock("text", "pr_no_physical_note")]),
        ContentRow([ContentBlock("text", "pr_low_minute_warning")]),
        ContentRow([ContentBlock("image", "pr_radar_image",
            caption_var="pr_spoke_caption")]),
        ContentRow([ContentBlock("expandable_table", "pr_stats_table",
            header="Full Stats", table_page_size=25)]),
    ],
    freshness_var="pr_data_freshness_text",
)
```

Note: The "Select 1-3 players" guidance needs a new state variable `pr_select_hint` (static string "Select 1\u20133 players to compare.") initialized in `state/player_radar.py` and added to `__all__`. Its compound condition preserves the original visibility logic — shown only when a competition is selected, no players chosen, and no warning active.

- [ ] **Step 2: Verify via Puppeteer**

Navigate to Player Comparison, verify conditional text blocks appear/disappear correctly, radar renders, expandable stats table works.

---

### Task 10: Migrate Player Similarity (status + table + radar)

**Files:**
- Modify: `taipy_spike/src/pages/player_similarity.py`

- [ ] **Step 1: Replace raw markdown with ContentRow declarations**

Remove `_CONTENT` constant and `pre_image_content=_CONTENT`. Replace with:

```python
from page_template import Citation, ContentBlock, ContentRow, PageConfig, build_page

PageConfig(
    ...,
    # image_var and pre_image_content removed
    content=[
        ContentRow([ContentBlock("text", "ps_status_message")]),
        ContentRow([ContentBlock("table", "ps_results_data",
            header="Similar Players")]),
        ContentRow([ContentBlock("image", "ps_radar_image",
            header="Radar Comparison",
            condition="len(ps_results_data) > 0 and len(ps_radar_image) > 0")]),
    ],
)
```

- [ ] **Step 2: Verify via Puppeteer**

Navigate to Player Similarity, select a player. Verify status message, results table with header, and radar image with header all render correctly.

---

### Task 11: Remove legacy fields from dataclasses

**Files:**
- Modify: `taipy_spike/src/page_template.py`

- [ ] **Step 1: Remove old fields from PageConfig**

Remove: `image_var`, `pre_image_content`.
Remove the legacy branches in `build_page()` (the `elif cfg.pre_image_content` and `elif cfg.image_var` fallbacks).

- [ ] **Step 2: Remove old fields from SubView**

Remove: `image_var`, `table_var`, `table_page_size`, `pre_content`, `post_content`.
Remove the legacy branch in `_build_sub_view()`.

- [ ] **Step 3: Verify server starts and all pages render**

Restart server. Navigate to at least 4 pages (Shot Map, Match Summary, Player Impact, Player Similarity) to confirm nothing broke.

---

### Task 12: Puppeteer visual regression on all 12 pages

**Files:**
- No code changes — validation only

- [ ] **Step 1: Screenshot all 12 pages**

Navigate to each page via Puppeteer at 1440x900:
1. Shot Map
2. Pass Map
3. Heat Map
4. Pass Network
5. Match Summary (with data loaded)
6. Pitch Control (with match loaded — verify sticky metrics)
7. Pass Timing (with match loaded — verify stacked charts)
8. Movement & Pressing (all 3 sub-views)
9. Player Impact (all 3 sub-views — verify expandable table on Timeline)
10. Defensive Impact (all 3 sub-views)
11. Player Comparison
12. Player Similarity

- [ ] **Step 2: Verify Match Summary 2x2 grid alignment**

Inspect DOM to confirm both grid rows use identical `grid-template-columns: 1fr 1fr`.

- [ ] **Step 3: Verify sticky metrics on Pitch Control**

Scroll page content and confirm metrics column stays pinned at viewport top.

- [ ] **Step 4: Verify conditional text blocks on Player Comparison**

Navigate to Player Comparison. Verify "Select 1-3 players" hint appears when competition is selected but no players chosen. Select a player — verify warning/reference text appears/disappears correctly based on data availability.

- [ ] **Step 5: Report findings**

Document any visual regressions or issues found.
