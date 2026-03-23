# ContentBlock Diagram Box Design

**Date:** 2026-03-21
**Status:** Draft
**Scope:** Taipy spike — template-driven content area for all 12 pages

## Problem

The content area (left column in the 3:1 grid) currently uses escape hatches (`pre_image_content`, `post_content`, raw markdown strings) for anything beyond a single image or table. Four pages (Match Summary, Pass Timing, Player Comparison, Player Similarity) bypass the template entirely with hand-written Taipy markdown. This violates the template-driven architecture established for headers, sidebar widgets, and metrics.

## Goals

1. Replace all escape hatches with a typed content model (`ContentBlock` + `ContentRow`)
2. Support all 12 pages' content patterns through data declarations only — zero raw markdown in page files
3. Enable independent scrolling of the content area while metrics stay pinned
4. Make layout changes (e.g., stacked → side-by-side) a one-line config change

## Non-Goals

- New content types beyond what the 12 pages currently display
- Drag-and-drop or runtime layout customization
- Animation or transitions between content states

## Design

### ContentBlock Dataclass

One piece of content — an image, a table, a text block, or an expandable table.

```python
@dataclass(frozen=True)
class ContentBlock:
    kind: Literal["image", "table", "text", "expandable_table"]
    var: str                      # state variable name
    header: str = ""              # optional subtitle rendered above the block
    condition: str = ""           # Taipy render condition (empty = auto-generate len(var) > 0)
    table_page_size: int = 50     # table and expandable_table only
    caption: str = ""             # static text rendered below the block
    caption_var: str = ""         # dynamic caption (state variable, rendered below)
    caption_condition: str = ""   # render condition for the caption
```

**Default render condition:** When `condition` is empty, the template auto-generates `len({var}) > 0` as the render condition. This matches the existing behavior for `image_var` and `table_var`. Pages that need custom conditions (e.g., referencing a different state variable) set `condition` explicitly.

**Content kinds:**

| Kind | Renders | `var` contains | Notes |
|------|---------|----------------|-------|
| `image` | `<\|{var}\|image\|>` | Base64/file path string | Full width within its ContentRow column |
| `table` | `<\|{var}\|table\|>` | DataFrame | `table_page_size` controls pagination |
| `text` | `<\|{var}\|text\|>` | String | For conditional status messages, warnings, reference notes |
| `expandable_table` | Taipy `<\|expandable\|>` wrapping a table | DataFrame | `header` is the toggle label (required for this kind) |

**`text` kind use cases:**
- Player Comparison: `pr_no_data_warning`, `pr_no_physical_note`, `pr_low_minute_warning` (conditional warnings above radar)
- Player Similarity: `ps_status_message` (search status)
- Pass Timing: DFL identifier caption (via `caption_var` + `caption_condition` on the table block)

**Header rendering:** When `header` is set, the template renders `<|part|class_name=ll-subtitle|>` above the content. Reuses the existing `.ll-subtitle` CSS class (already defined in `style_v2.css`). Used for section titles like "Player Rankings", "Action Details", "Similar Players".

**Caption rendering:** When `caption` or `caption_var` is set, the template renders an `ll-reference`-styled text block below the content. `caption_condition` gates visibility (e.g., DFL identifier note only shown when DFL data is present).

### ContentRow Dataclass

Groups one or more ContentBlocks into a horizontal row.

```python
@dataclass(frozen=True)
class ContentRow:
    blocks: list[ContentBlock]
    columns: int = 0      # 0 = auto (len(blocks)), explicit value forces grid column count
    condition: str = ""   # row-level render condition (gates entire row)
```

**Layout rules:**
- Single block → full width (no grid wrapper needed)
- Multiple blocks → CSS grid with `grid-template-columns: repeat(N, 1fr)` where N = `columns` if set, else `len(blocks)`
- Explicit `columns` ensures alignment across consecutive rows (Match Summary 2×2 grid)

**Row-level `condition`:** When set, wraps the entire row in a single `render=` part. Used when multiple blocks share the same visibility gate (e.g., Match Summary 2×2 grid gated on `len(ms_home_name) > 0`). Individual block conditions still apply within the row.

**CSS class:** `ll-content-row` for multi-block rows. Template generates:
```html
<|part|class_name=ll-content-row ll-content-cols-{N}|>
  ... blocks ...
|>
```

**Content positioning:** All content rows render INSIDE the left column of the 3fr/1fr grid (for single-view pages) or inside the left column of `ll-grid-3-1` (for sub-view pages). This preserves the current behavior where `post_content` renders inside the left column, not below the entire grid.

### PageConfig Changes

```python
@dataclass(frozen=True)
class PageConfig:
    # UNCHANGED
    title: str
    icon: str
    nav_section: str
    description: str
    citations: list[Citation] = field(default_factory=list)
    empty_message: str = ""
    empty_condition: str = ""
    metrics: list[Metric] = field(default_factory=list)
    scope_vars: list[str] = field(default_factory=list)
    freshness_var: str = ""
    sub_views: list[SubView] = field(default_factory=list)

    # NEW — replaces image_var, pre_image_content
    content: list[ContentRow] = field(default_factory=list)

    # REMOVED
    # image_var: str          → use ContentRow([ContentBlock("image", var)])
    # pre_image_content: str  → use content: list[ContentRow]
```

### SubView Changes

```python
@dataclass(frozen=True)
class SubView:
    # UNCHANGED
    condition: str
    scale_notes: list[str] = field(default_factory=list)
    metrics: list[Metric] = field(default_factory=list)
    empty_message: str = ""
    empty_condition: str = ""
    fallback_empty_message: str = ""
    fallback_empty_condition: str = ""
    scope_vars: list[str] = field(default_factory=list)

    # NEW — replaces image_var, table_var, pre_content, post_content
    content: list[ContentRow] = field(default_factory=list)

    # REMOVED
    # image_var: str          → use ContentRow([ContentBlock("image", var)])
    # table_var: str          → use ContentRow([ContentBlock("table", var)])
    # table_page_size: int    → moved to ContentBlock.table_page_size
    # pre_content: str        → use content list ordering
    # post_content: str       → use content list ordering
```

### Template Rendering Order

Within the content column (left side of 3:1 grid), the template renders in this fixed order:

1. **Scope variables** — `scope_vars` as conditional text blocks
2. **Scale/reference notes** — `scale_notes` as `ll-reference` blocks
3. **Empty state** — `empty_condition` / `empty_message` as `ll-info-box`
4. **Fallback empty state** — `fallback_empty_condition` / `fallback_empty_message` (SubView only)
5. **Content rows** — each `ContentRow` in list order, blocks within each row in left-to-right order
6. **Data freshness** — `freshness_var` as `ll-reference` text

All content rows (step 5) render inside the left column, after scope/notes/empty-state and before freshness. This preserves the current layout where images, tables, and expandable sections all appear in the content area without breaking out of the 3:1 grid.

### Sticky Metrics (Right Column)

```css
/* Metrics column pins to top of viewport while content scrolls.
   Scrolls naturally when metrics exceed viewport height. */
.ll-metrics-column {
    position: sticky;
    top: 4rem;       /* below header */
    align-self: start;
    max-height: calc(100vh - 5rem);
    overflow-y: auto;
}
```

This gives the behavior: metrics stay visible while scrolling content, but scroll naturally if the metrics column itself is taller than the viewport (many metrics, small screen).

**Validation note:** `position: sticky` inside Taipy's MUI-based layout requires empirical validation. Taipy's `<|layout|>` uses MUI Grid (flexbox). Sticky works in flexbox IF the parent does not set `overflow: hidden`. A CSS spike step (see Migration Order step 2) validates this on Pitch Control (7 metrics) before committing to the full migration. If sticky fails, fallback is `position: fixed` with explicit width calculation.

### Expandable Table Rendering

Taipy's `<|expandable|>` component wraps the table:

```
<|part|render={condition}|>
<|{header}|expandable|expanded=False|>
<|{var}|table|page_size={page_size}|>
|>
|>
```

The `header` field on `ContentBlock` doubles as the expandable toggle label. The `expanded=False` default keeps detail tables collapsed until the user clicks.

## Page Migration Examples

### Shot Map (simplest — single image)

```python
PageConfig(..., content=[
    ContentRow([ContentBlock("image", "sm_pitch_image")]),
])
```

### Match Summary (2×2 chart grid)

```python
PageConfig(..., content=[
    ContentRow([
        ContentBlock("image", "ms_shooting_chart"),
        ContentBlock("image", "ms_passing_chart"),
    ], columns=2, condition='len(ms_home_name) > 0'),
    ContentRow([
        ContentBlock("image", "ms_possession_chart"),
        ContentBlock("image", "ms_ppda_chart",
            caption="PPDA: Passes Per Defensive Action. Under 10 = aggressive pressing, over 15 = passive."),
    ], columns=2, condition='len(ms_home_name) > 0'),
])
```

### Pass Timing (stacked charts + table)

Charts stacked vertically per user requirement. To switch to side-by-side later, move both image blocks into a single `ContentRow`.

```python
PageConfig(..., content=[
    ContentRow([ContentBlock("image", "pt_scatter_image")]),
    ContentRow([ContentBlock("image", "pt_heatmap_image")]),
    ContentRow([ContentBlock("table", "pt_rankings_data", header="Player Rankings",
        caption_var="pt_dfl_caption", caption_condition="pt_show_dfl_caption")]),
])
```

### Movement Physical (image + expandable stats)

```python
SubView(..., content=[
    ContentRow([ContentBlock("image", "ma_physical_image")]),
    ContentRow([ContentBlock("expandable_table", "ma_physical_stats_data",
        header="Full Stats Table")]),
])
```

### Player Comparison (warnings + radar + expandable stats)

```python
PageConfig(..., content=[
    ContentRow([ContentBlock("text", "pr_no_data_warning")]),
    ContentRow([ContentBlock("text", "pr_no_physical_note")]),
    ContentRow([ContentBlock("text", "pr_low_minute_warning")]),
    ContentRow([ContentBlock("image", "pr_radar_image",
        caption_var="pr_spoke_caption")]),
    ContentRow([ContentBlock("expandable_table", "pr_stats_table",
        header="Full Stats")]),
])
```

### Player Similarity (status + results table + radar)

```python
PageConfig(..., content=[
    ContentRow([ContentBlock("text", "ps_status_message")]),
    ContentRow([ContentBlock("table", "ps_results_data",
        header="Similar Players")]),
    ContentRow([ContentBlock("image", "ps_radar_image",
        header="Radar Comparison")]),
])
```

## All 12 Pages — Content Migration Map

| Page | Current | Content Rows |
|------|---------|--------------|
| Shot Map | `image_var` | 1 row: image |
| Pass Map | `image_var` | 1 row: image |
| Heat Map | `image_var` | 1 row: image |
| Pass Network | `image_var` | 1 row: image |
| Match Summary | `pre_image_content` (raw) | 2 rows: 2×2 image grid (columns=2) + caption |
| Pitch Control | `image_var` | 1 row: image |
| Pass Timing | `pre_image_content` (raw) | 3 rows: image, image, table w/ header + caption |
| Movement (3 views) | `image_var` per SubView | 2 rows/view: image + expandable_table |
| Player Impact (3 views) | `image_var`/`table_var`/`post_content` | 1-2 rows/view: table OR image OR image + expandable_table |
| Defensive Impact (3 views) | `image_var`/`table_var` | 1 row/view: image OR table |
| Player Comparison | `pre_image_content` (raw) | 5 rows: 3× text + image + expandable_table |
| Player Similarity | `pre_image_content` (raw) | 3 rows: text + table w/ header + image w/ header |

## CSS Changes

```css
/* Content row — CSS grid for multi-block rows */
.ll-content-row {
    display: grid;
    gap: 1rem;
    width: 100%;
}
.ll-content-cols-2 { grid-template-columns: 1fr 1fr; }

/* Sticky metrics column */
.ll-metrics-column {
    position: sticky;
    top: 4rem;
    align-self: start;
    max-height: calc(100vh - 5rem);
    overflow-y: auto;
}
```

Note: `.ll-subtitle` already exists in `style_v2.css` — reuse it for block headers. No new subtitle class needed.

## Testing Strategy

1. **Visual regression per page:** Puppeteer screenshot before/after migration — content must render identically (minus expandable tables which are new)
2. **Layout test:** Match Summary 2×2 alignment verified via Puppeteer DOM inspection (column positions match)
3. **Expandable test:** Verify accordion opens/closes on Movement, VAEP, Player Comparison
4. **Sticky metrics test:** Scroll content on a page with many metrics (Pitch Control: 7 metrics) and verify metrics stay pinned; also test small viewport to verify metrics scroll when needed
5. **Responsive:** Verify at 1440px and 1024px widths
6. **Text blocks:** Verify Player Comparison conditional warnings and Player Similarity status message render/hide correctly

## Migration Order

1. `page_template.py` — add `ContentBlock`, `ContentRow`, update `build_page()` and `_build_sub_view()`
2. **CSS spike** — add sticky metrics CSS and content row grid classes. Validate `position: sticky` works inside Taipy's MUI layout on Pitch Control (7 metrics). If it doesn't, fall back to alternative before proceeding.
3. Simple single-image pages (Shot Map, Pass Map, Heat Map, Pass Network, Pitch Control) — 5 pages
4. Multi-view pages (Movement, Player Impact, DEFCON) — 3 pages × 3 views
5. Complex pages (Match Summary, Pass Timing, Player Comparison, Player Similarity) — 4 pages
6. Remove old fields (`image_var`, `pre_image_content`, `post_content`, `table_var`, `table_page_size`) from dataclasses
7. Puppeteer visual regression on all 12 pages
