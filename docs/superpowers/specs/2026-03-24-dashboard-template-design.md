# Dashboard Template Extension — Design Spec

**Date:** 2026-03-24
**Scope:** Extend `page_template.py` to support dashboard-style pages (stats bar + full-width content) alongside existing analytics pages (3fr/1fr with metrics). Refactor workflows page to use templates.

## Problem

The workflows page bypasses `build_page()` with ~180 lines of hand-crafted Taipy markdown. This creates:
- Inconsistency: one page doesn't follow the template-first rule
- Duplication: stats bar CSS classes, empty states, header rendering duplicated
- Reuse barrier: future observability pages need the same dashboard pattern

## Design

### New Dataclass: `StatCard`

```python
@dataclass(frozen=True)
class StatCard:
    label: str           # "Workflows", "Total Cost (30 Days)"
    var: str             # state variable for the big number
    detail_var: str = "" # state variable for detail line below
    help_text: str = ""  # tooltip (ll-help info icon, same as Metric)
```

One card in the stats bar. Template handles all wrapping (`ll-stats-bar`, `ll-stat-card`, `ll-stat-label`, `ll-stat-detail`). Detail line auto-hidden when the referenced state variable is empty string (render condition: `len({detail_var}) > 0`).

### `PageConfig` Changes

Add one field:
```python
stats: list[StatCard] = field(default_factory=list)
```

### `ContentBlock` Changes

Add five optional fields:
```python
on_action: str = ""             # table/expandable_table callback (string name, same convention as SidebarWidget.on_change)
click_bridge_var: str = ""      # hidden input var for iframe→Taipy callback
click_bridge_callback: str = "" # callback triggered by click bridge (string name)
height_var: str = ""            # html kind: dynamic height state variable (e.g., "wf_dag_height")
container_class: str = ""       # html kind: override wrapper CSS class (e.g., "ll-dag-container")
```

All callbacks are string names of functions registered in state — consistent with `SidebarWidget.on_change` and `PageConfig.on_action` patterns throughout the template.

### Layout Logic in `build_page()`

The presence of `cfg.stats` determines the layout mode:

**Dashboard layout** (`cfg.stats` populated):
1. Page header via `build_header_from_config(cfg)`
2. Stats bar: `_build_stats_bar(cfg.stats)`
3. Content blocks: full-width, no 3fr/1fr grid, no metrics column
4. Empty/warning states (same mechanism as analytics pages)

**Analytics layout** (`cfg.stats` empty):
- Unchanged — current 3fr/1fr with metrics column

### New Template Functions

```python
def _build_stat_card(card: StatCard) -> str:
    """One stat card: label + ### value + optional detail + optional help icon."""

def _build_stats_bar(stats: list[StatCard]) -> str:
    """Horizontal stats bar (ll-stats-bar) from list of StatCards."""
```

### Content Block Rendering Updates

**`kind="table"`** with `on_action`:
```
<|{var}|table|page_size=N|on_action=callback|>
```

**`kind="html"`** with container class, height, and click bridge:
```
<|part|class_name={container_class or "ll-html-content"}|
<|part|content={var}|height={height_var}|>    ← height_var omitted when empty
|>
```
When `click_bridge_var` is set, appends a hidden input bridge:
```
<|part|class_name=ll-hidden|
<|{click_bridge_var}|input|on_change=click_bridge_callback|>
|>
```

Note: `ll-dag-trigger` has no CSS definition — it exists only as a semantic marker in the current hand-crafted markup and is not needed in the template. The hiding is done entirely by `ll-hidden`.

### Empty State Handling

The dashboard layout uses the same `PageConfig` fields as analytics pages:
- `empty_condition` + `empty_message` → blue `ll-info-box` ("No workflows match the selected filters.")
- `warning_var` → amber `ll-warning-box` (dynamic text, shown when non-empty)

For the "no cards loaded" case, a state variable `wf_no_cards_warning` is set to the warning message when `_cards` fails to load, and cleared otherwise. This maps to the existing `warning_var` mechanism:
```python
warning_var="wf_no_cards_warning",
```

### Resulting Workflows Page

`pages/workflows.py` becomes configuration-only for the dashboard mode:

```python
page_config = PageConfig(
    title="AI/ML Workflows",
    icon="account_tree",
    nav_section=NAV_OPERATIONS,
    description="...",
    stats=[
        StatCard("Workflows", "wf_total_workflows", "wf_workflows_detail"),
        StatCard("Freshness", "wf_freshness_summary", "wf_freshness_detail"),
        StatCard("Total Cost (30 Days)", "wf_total_cost_30d", "wf_cost_detail"),
        StatCard("Run Volume (30 Days)", "wf_run_volume", "wf_run_volume_detail"),
    ],
    content=[
        ContentRow([ContentBlock("html", "wf_dag_html",
                                 height_var="wf_dag_height",
                                 container_class="ll-dag-container",
                                 click_bridge_var="wf_dag_clicked",
                                 click_bridge_callback="wf_on_dag_node_click")]),
        ContentRow([ContentBlock("table", "wf_table_data",
                                 on_action="wf_on_table_action",
                                 table_page_size=20)]),
    ],
    empty_message="No workflows match the selected filters.",
    empty_condition="len(wf_table_data) == 0 and wf_cards_loaded",
    warning_var="wf_no_cards_warning",
)
```

Detail drilldown stays hand-crafted, wrapped alongside the dashboard:
```python
dashboard_md = build_page(page_config)
page_md = f"""
<|part|render={{wf_selected_workflow is None}}|
{dashboard_md}
|>
<|part|render={{wf_selected_workflow is not None}}|
... detail markdown ...
|>
"""
```

### CSS Classes (Existing, No Changes)

- `ll-stats-bar` — flex container for stat cards
- `ll-stat-card` — individual card styling
- `ll-stat-label` — uppercase muted label
- `ll-stat-detail` — secondary detail text
- `ll-html-content` — generic HTML iframe content (default for `kind="html"`)
- `ll-dag-container` — DAG-specific iframe styling (via `container_class` override)
- `ll-hidden` — hides click bridge input widget

### What This Enables

Future operational pages (observability, cost analysis) use the same pattern:
```python
PageConfig(
    stats=[StatCard("SLI Coverage", ...), StatCard("P99 Latency", ..., help_text="...")],
    content=[ContentRow([ContentBlock("chart", ...)]), ContentRow([ContentBlock("table", ...)])],
)
```

No hand-crafted markdown needed.

## Files Changed

| File | Change |
|------|--------|
| `page_template.py` | Add `StatCard`, `stats` field on `PageConfig`, `on_action`/`click_bridge_*`/`height_var`/`container_class` on `ContentBlock`, `_build_stat_card()`, `_build_stats_bar()`, dashboard layout branch in `build_page()` |
| `pages/workflows.py` | Replace hand-crafted dashboard markdown with `PageConfig` + `build_page()`. Keep hand-crafted detail drilldown. |
| `state/workflows.py` | Add `wf_no_cards_warning` state variable. Set it when `_load_cards_from_yaml()` returns empty. |
| `style_v2.css` | No changes. |

## Out of Scope

- Detail drilldown template (future session)
- Sidebar widget generation for workflows (already works via `build_sidebar_section`)
- Multi-view / sub-view support for dashboard pages
