# Dashboard Template Extension — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `page_template.py` with dashboard layout support (stats bar + full-width content), then refactor the workflows page from hand-crafted markdown to template configuration.

**Architecture:** Add `StatCard` dataclass and dashboard layout branch to the existing `build_page()` pipeline. The presence of `cfg.stats` determines layout mode: dashboard (full-width + stats bar) vs analytics (3fr/1fr + metrics). Extend `ContentBlock` with `on_action`, `click_bridge_*`, `height_var`, and `container_class` fields. Refactor `pages/workflows.py` to use `PageConfig` + `build_page()` for the dashboard mode while keeping the detail drilldown hand-crafted.

**Tech Stack:** Python 3.10, Taipy 4.1.1, pytest, Puppeteer (E2E verification)

**Spec:** `docs/superpowers/specs/2026-03-24-dashboard-template-design.md`

**Commit strategy:** Single commit after full E2E verification (per project convention).

---

### Task 1: Add `StatCard` dataclass and builder functions

**Files:**
- Modify: `hf_taipy_app/src/page_template.py` (add dataclass + two functions)

- [ ] **Step 1: Add `StatCard` dataclass after `Metric`**

Insert after the `Metric` class (around line 297):

```python
@dataclass(frozen=True)
class StatCard:
    """One card in a dashboard stats bar.

    Pages provide only data — label, variable, optional detail + help.
    The template controls all wrapping (ll-stats-bar, ll-stat-card, etc.).
    """

    label: str           # "Workflows", "Total Cost (30 Days)"
    var: str             # state variable for the big number
    detail_var: str = "" # state variable for detail line below
    help_text: str = ""  # tooltip (ll-help info icon, same as Metric)
```

- [ ] **Step 2: Add `_build_stat_card()` function**

Insert after `_build_metric()` (around line 391):

```python
def _build_stat_card(card: StatCard) -> str:
    """Generate markdown for a single stat card.

    All styling decisions are made here — pages only provide data.
    """
    help_span = ""
    if card.help_text:
        help_span = (
            f' <span class="ll-help material-symbols-outlined"'
            f' title="{card.help_text}">info</span>'
        )

    lines = [
        "<|part|class_name=ll-stat-card|",
        f"<|part|class_name=ll-stat-label|",
        f"{card.label}{help_span}",
        "|>",
        "",
        f"### <|{{{card.var}}}|text|>",
    ]

    if card.detail_var:
        lines.append("")
        lines.append(f"<|part|render={{len({card.detail_var}) > 0}}|class_name=ll-stat-detail|")
        lines.append(f"<|{{{card.detail_var}}}|text|>")
        lines.append("|>")

    lines.append("|>")
    return "\n".join(lines)


def _build_stats_bar(stats: list[StatCard]) -> str:
    """Generate the horizontal stats bar from a list of StatCards."""
    parts = ["<|part|class_name=ll-stats-bar|", ""]
    for card in stats:
        parts.append(_build_stat_card(card))
        parts.append("")
    parts.append("|>")
    return "\n".join(parts)
```

- [ ] **Step 3: Verify syntax**

Run: `cd hf_taipy_app && ../.venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from page_template import StatCard, _build_stat_card, _build_stats_bar; print('OK')"`

---

### Task 2: Extend `ContentBlock` with new fields

**Files:**
- Modify: `hf_taipy_app/src/page_template.py` (ContentBlock dataclass + _build_content_block)

- [ ] **Step 1: Add fields to `ContentBlock`**

Add after `chart_height` (line 269):

```python
    on_action: str = ""             # table/expandable_table: callback function name (string, same as SidebarWidget.on_change)
    click_bridge_var: str = ""      # html kind: hidden input var for iframe→Taipy callback
    click_bridge_callback: str = "" # html kind: callback triggered by click bridge (string name)
    height_var: str = ""            # html kind: dynamic height state variable (e.g., "wf_dag_height")
    container_class: str = ""       # html kind: override wrapper CSS class (default: "ll-html-content")
```

- [ ] **Step 2: Update `_build_content_block()` for `kind="table"` with `on_action`**

Replace the table rendering line:
```python
    elif block.kind == "table":
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}|>")
```
With:
```python
    elif block.kind == "table":
        action_attr = f"|on_action={block.on_action}" if block.on_action else ""
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}{action_attr}|>")
```

- [ ] **Step 3: Update `_build_content_block()` for `kind="html"` with height, container class, and click bridge**

Replace the html rendering line:
```python
    elif block.kind == "html":
        parts.append(f"<|part|class_name=ll-html-content|content={{{block.var}}}|>")
```
With:
```python
    elif block.kind == "html":
        css_class = block.container_class or "ll-html-content"
        height_attr = f"|height={{{block.height_var}}}" if block.height_var else ""
        parts.append(f"<|part|class_name={css_class}|")
        parts.append(f"<|part|content={{{block.var}}}{height_attr}|>")
        parts.append("|>")
```

Append the click bridge at the **end of `_build_content_block()`**, after the caption section (lines 430-440), as the final append before `return`. The bridge must be OUTSIDE the render-condition wrapper so it remains in the DOM even when the content is hidden:
```python
    # Click bridge: hidden input for iframe JS → Taipy callback.
    # Placed outside render condition — must exist in DOM even when content is hidden.
    if block.click_bridge_var and block.click_bridge_callback:
        parts.append(f"<|part|class_name=ll-hidden|")
        parts.append(f"<|{{{block.click_bridge_var}}}|input|on_change={block.click_bridge_callback}|>")
        parts.append("|>")

    return "\n".join(parts)
```

- [ ] **Step 4: Also update `kind="expandable_table"` with `on_action`**

Replace:
```python
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}|>")
```
(inside the expandable_table branch) with:
```python
        action_attr = f"|on_action={block.on_action}" if block.on_action else ""
        parts.append(f"<|{{{block.var}}}|table|page_size={block.table_page_size}{action_attr}|>")
```

---

### Task 3: Add `stats` to `PageConfig` and dashboard layout in `build_page()`

**Files:**
- Modify: `hf_taipy_app/src/page_template.py` (PageConfig + build_page)

- [ ] **Step 1: Add `stats` field to `PageConfig`**

Add after `citations` (line 321):
```python
    stats: list[StatCard] = field(default_factory=list)  # dashboard stat cards (triggers dashboard layout)
```

- [ ] **Step 2: Add dashboard layout branch to `build_page()`**

The current `build_page()` has: `if cfg.sub_views: ... else: ...`. Add a new branch. Replace the `else` block's opening with a three-way check:

```python
def build_page(cfg: PageConfig) -> str:
    """Generate the standard page template markdown from config."""
    parts = [build_header_from_config(cfg), ""]

    if cfg.sub_views:
        # Multi-view page: conditional sub-view blocks
        for sv in cfg.sub_views:
            parts.append(_build_sub_view(sv, cfg.title))
            parts.append("")
    elif cfg.stats:
        # Dashboard page: stats bar + full-width content blocks
        parts.append(_build_stats_bar(cfg.stats))
        parts.append("")

        # Content rows (full width, no 3fr/1fr grid)
        for row in cfg.content:
            parts.append(_build_content_row(row, cfg.title))
            parts.append("")

        # Empty state
        if cfg.empty_condition:
            parts.append(f"<|part|render={{{cfg.empty_condition}}}|class_name=ll-info-box|")
            parts.append(f"{cfg.empty_message}")
            parts.append("|>")

        # Warning state
        if cfg.warning_var:
            parts.append(f"<|part|render={{len({cfg.warning_var}) > 0}}|class_name=ll-warning-box|")
            parts.append(f"<|{{{cfg.warning_var}}}|text|>")
            parts.append("|>")
    else:
        # Single-view page: standard 3fr/1fr layout
        # ... existing code unchanged (lines 595-650) ...

    # IMPORTANT: return stays at function level, outside all branches
    return "\n".join(parts)
```

- [ ] **Step 3: Verify import/syntax**

Run: `cd hf_taipy_app && ../.venv/Scripts/python.exe -c "import sys; sys.path.insert(0,'src'); from page_template import PageConfig, StatCard, build_page; print('OK')"`

---

### Task 4: Add `wf_no_cards_warning` state variable

**Files:**
- Modify: `hf_taipy_app/src/state/workflows.py`

- [ ] **Step 1: Add state variable**

Add near line 126 (with other dashboard state):
```python
wf_no_cards_warning: str = ""  # Non-empty when YAML cards fail to load (warning_var target)
```

- [ ] **Step 2: Add to `__all__`**

Add `"wf_no_cards_warning"` to the `__all__` list.

- [ ] **Step 3: Set warning in `wf_refresh()`**

In the `wf_refresh()` function, after the `if not _cards:` check, set the warning:
```python
    if not _cards:
        logger.warning("No workflow cards loaded")
        state.wf_no_cards_warning = "No workflow cards loaded. Check that the workflow-cards/ directory is available."
        return
    state.wf_no_cards_warning = ""  # Clear on successful load
```

---

### Task 5: Refactor `pages/workflows.py` to use templates

**Files:**
- Modify: `hf_taipy_app/src/pages/workflows.py`

This is the main refactoring task. Replace the hand-crafted dashboard markdown with `PageConfig` + `build_page()`. Keep the detail drilldown hand-crafted.

- [ ] **Step 1: Update imports**

```python
from page_template import (
    NAV_OPERATIONS,
    ContentBlock,
    ContentRow,
    PageConfig,
    StatCard,
    build_header_from_config,
    build_page,
)
```

- [ ] **Step 2: Update `PageConfig` with stats and content**

```python
page_config = PageConfig(
    title="AI/ML Workflows",
    icon="account_tree",
    nav_section=NAV_OPERATIONS,
    description=(
        "Interactive dependency graph and operational dashboard for all AI/ML workflows. "
        "16 workflow cards covering training (HF Jobs) and inference (Databricks) pipelines. "
        "Cost transparency across three tiers: actual (billing), estimated (live), and projected (YAML)."
    ),
    citations=[],
    stats=[
        StatCard("Workflows", "wf_total_workflows", "wf_workflows_detail"),
        StatCard("Freshness", "wf_freshness_summary", "wf_freshness_detail"),
        StatCard("Total Cost (30 Days)", "wf_total_cost_30d", "wf_cost_detail"),
        StatCard("Run Volume (30 Days)", "wf_run_volume", "wf_run_volume_detail"),
    ],
    content=[
        ContentRow([ContentBlock(
            "html", "wf_dag_html",
            height_var="wf_dag_height",
            container_class="ll-dag-container",
            click_bridge_var="wf_dag_clicked",
            click_bridge_callback="wf_on_dag_node_click",
        )]),
        ContentRow([ContentBlock(
            "table", "wf_table_data",
            on_action="wf_on_table_action",
            table_page_size=20,
        )]),
    ],
    empty_message="No workflows match the selected filters.",
    empty_condition="len(wf_table_data) == 0 and wf_cards_loaded",
    warning_var="wf_no_cards_warning",
)
```

- [ ] **Step 3: Build dashboard markdown from template, keep detail hand-crafted**

IMPORTANT: Do NOT use an f-string for `page_md`. The `build_page()` output contains bare single-brace Taipy bindings (`{wf_dag_html}`, etc.) that would be interpreted as f-string variable lookups and raise `NameError`. Use string concatenation instead.

```python
_dashboard_md = build_page(page_config)

# Detail drilldown — hand-crafted (future: separate detail template)
_detail_md = """
<|Back to Workflows|button|on_action=wf_on_back_click|class_name=ll-header-btn text-no-transform|>

<|part|class_name=ll-detail-header|
## <|{wf_detail_title}|text|>
...
"""
# Copy lines 114-183 from the current page_md verbatim for the detail sections.

page_md = (
    "<|part|render={wf_selected_workflow is None}|\n"
    + _dashboard_md
    + "\n|>\n\n"
    + "<|part|render={wf_selected_workflow is not None}|\n"
    + _detail_md
    + "\n|>\n"
)
```

Note: The render conditions use single braces — this is correct because `page_md` is NOT an f-string.

- [ ] **Step 4: Verify the app starts**

Run the app and navigate to the Workflows page. The dashboard should render identically to the current hand-crafted version.

---

### Task 6: E2E verification with Puppeteer

**Files:** None (verification only)

- [ ] **Step 1: Full DAG (All, 16 nodes)**

Navigate to Workflows page. Verify: stats bar with 4 cards, DAG with all 16 nodes, table below, no scrollbar.

- [ ] **Step 2: Filtered (Inference, 2 nodes)**

Select Workflow Type = Inference. Verify: stats update, DAG shrinks, table filters, no scrollbar.

- [ ] **Step 3: Empty state (0 matches)**

Add Freshness = Stale. Verify: DAG hidden, "No workflows match" info box, stats show 0.

- [ ] **Step 4: Recovery (back to All)**

Reset both filters to All. Verify: full DAG returns, all stats restored.

- [ ] **Step 5: Detail drilldown**

Click a workflow in the table. Verify: detail view renders with back button, sections load.

- [ ] **Step 6: Back to dashboard**

Click "Back to Workflows". Verify: dashboard restores correctly.
