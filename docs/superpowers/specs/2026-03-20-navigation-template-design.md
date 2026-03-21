# Navigation Template Design

**Date:** 2026-03-20
**Branch:** `spike/taipy-proof-of-concept`
**Status:** Approved design, pending implementation

## Problem

The sidebar navigation in `template.py` is hardcoded markdown — 12 links with icons, labels, and route paths written inline in `root_page`. Meanwhile, each page's `PageConfig` already declares `title` and `icon` for the page header. This creates two independent sources of truth for what a page is called and which icon it uses:

1. The nav link in `root_page` (e.g., `[<span ...>target</span> Shot Map](/Shot-Map)`)
2. The `PageConfig(title="Shot Map", icon="target")` in the page file

If these drift, the nav and header show different names or icons. The fix is to make `PageConfig` the single source of truth and have the template generate navigation from it.

## Design

### PageConfig: one new field

Add `nav_section: str` to `PageConfig`. This is the only new field — `title` and `icon` already exist and will drive both the nav link text/icon and the page header.

```python
@dataclass(frozen=True)
class PageConfig:
    title: str
    icon: str
    nav_section: str  # e.g., "Match Analysis", "Player Analysis", "Advanced"
    description: str
    # ... existing fields unchanged
```

Pages declare their section identity. Each page module exports both `page_config` and `page_md`:

```python
# pages/shot_map.py
page_config = PageConfig(
    title="Shot Map",
    icon="target",
    nav_section="Match Analysis",
    description="Shot locations sized by xG with isotonic calibration.",
    ...
)
page_md = build_page(page_config)
```

Convention: every page module exports `page_config: PageConfig` and `page_md: str`.

### Page registry: ordered list replaces dict-of-strings

`main.py` currently registers pages as a `dict[str, str]` mapping route keys to markdown strings. To generate navigation, the template needs access to each page's `PageConfig` and its route key. The registry becomes an ordered list of `(route_key, PageConfig, page_md)` tuples (or a lightweight dataclass):

```python
@dataclass(frozen=True)
class PageEntry:
    route: str        # e.g., "Shot-Map"
    config: PageConfig
    markdown: str     # output of build_page()

PAGE_REGISTRY: list[PageEntry] = [
    PageEntry("Shot-Map", shot_map_config, shot_map_page),
    PageEntry("Pass-Map", pass_map_config, pass_map_page),
    ...
]
```

The list order defines nav display order — pages have no knowledge of their position. Section header order is determined by the first page registered in that section (Python 3.7+ dict insertion order).

### Avoiding circular imports

`main.py` imports `root_page` from `template.py`. If `template.py` also imported `PAGE_REGISTRY` from `main.py`, that would create a circular import. Solution: `main.py` owns both the registry and `root_page` construction:

1. `page_template.py` exports `build_nav()` (pure function, no imports from `main.py` or `template.py`).
2. `template.py` exports a `build_root_page(nav_section: str) -> str` function (or the static parts as a template string with a `{nav_section}` placeholder).
3. `main.py` builds `PAGE_REGISTRY`, calls `build_nav(PAGE_REGISTRY)`, and passes the result to `build_root_page()` to construct the final `root_page`.

No module imports from `main.py`. The dependency graph is: `main.py` → `template.py` → `page_template.py`. Pages import only from `page_template.py`.

### Non-nav pages (Widget Spacing Test)

The Widget Spacing Test page has no `PageConfig` and does not appear in `PAGE_REGISTRY`. It is added directly to the `pages` dict after the registry-derived entries:

```python
pages = {"/": root_page}
pages.update({entry.route: entry.markdown for entry in PAGE_REGISTRY})
pages["Widget-Spacing-Test"] = spacing_test_page  # dev-only, not in nav
```

Pattern: any page that should be routable but not in navigation is added to `pages` directly, outside the registry.

### Nav generation: `build_nav()`

A new function in `page_template.py` generates the nav markdown from the registry:

```python
def build_nav(registry: list[PageEntry]) -> str:
    """Generate sidebar nav markdown from page registry.

    Groups pages by nav_section (preserving registration order).
    Emits section headers and icon+label links.
    """
    sections: dict[str, list[PageEntry]] = {}
    for entry in registry:
        sections.setdefault(entry.config.nav_section, []).append(entry)

    parts = []
    for section_name, entries in sections.items():
        parts.append(f'<|part|class_name=ll-nav-header|>\n**{section_name}**\n|>\n')
        for entry in entries:
            icon = entry.config.icon
            label = entry.config.title
            route = entry.route
            parts.append(
                f'[<span class="material-symbols-outlined">{icon}</span> {label}](/{route})\n'
            )
    return "\n".join(parts)
```

### root_page: generated, not hardcoded

The hardcoded nav block (lines 389–423 of current `template.py`) is replaced by a call to `build_nav()`:

```python
_nav_section = build_nav(PAGE_REGISTRY)

root_page = f"""
<|layout|columns=300px 1fr|gap=0.75rem|

<|part|class_name=sidebar|

{_nav_section}

{_filter_section}
{_search_section}

|>
...
"""
```

### Active page highlighting

Taipy automatically applies a visual highlight (lighter background) to the nav link matching the current route. No custom CSS class is needed. This was verified via Puppeteer — the highlight follows page navigation correctly.

### What does NOT change

- **CSS**: No navigation CSS changes. The existing `.sidebar a`, `.ll-nav-header`, hover styles all work unchanged.
- **`on_navigate` / `current_page`**: The state callback and routing logic are unaffected.
- **Sidebar widgets**: `_FILTER_WIDGETS`, `_SEARCH_WIDGETS`, `build_sidebar_section()` are unchanged.
- **Glossary / Getting Started panels**: Unchanged.
- **Page body rendering**: `build_page()` and all page-body generation stay the same. The `icon` and `title` in `PageConfig` now also drive the nav, but their use in `build_header_from_config()` is unchanged.

### Future: role-based navigation

When AI/ML Workflow pages are added:

1. New pages declare `nav_section="AI/ML Workflows"`.
2. `nav_section` evolves from `str` to a `NavSection` dataclass: `NavSection(name="AI/ML Workflows", role="ml_ops")`.
3. `build_nav()` accepts a `role` filter parameter.
4. **No page files change** — they reference the `NavSection` object instead of a string. The migration is additive.

## Files Changed

| File | Change |
|------|--------|
| `page_template.py` | Add `nav_section` field to `PageConfig`. Add `PageEntry` dataclass. Add `build_nav()` function. |
| `template.py` | Replace hardcoded nav block with `build_root_page(nav_section)` function or template. Export static parts; no import from `main.py`. |
| `main.py` | Build `PAGE_REGISTRY` list. Call `build_nav()` + `build_root_page()` to construct `root_page`. Derive `pages` dict from registry. Non-nav pages (Widget Spacing Test) added directly. |
| `pages/*.py` (all 12) | Extract `PageConfig` to named `page_config` variable. Add `nav_section="..."`. Export both `page_config` and `page_md`. |

## Known remaining duplication (future work)

These use hardcoded route-key strings and are out of scope for this change:

- **Glossary panels**: Per-page `render={{show_glossary and current_page == "Shot-Map"}}` blocks and the `PAGE_TERMS` dict. Natural next step: add `glossary_terms` to `PageConfig` and generate these panels.
- **Sidebar widget visibility tuples**: `_COMP_PAGES`, `_TEAM_PAGES`, etc. These control which filters appear on which pages. Could be driven by `PageConfig` in a future pass.

## Acceptance Criteria

1. All 12 pages render with correct nav section grouping and order (visual match to current via Puppeteer against Chrome on localhost).
2. Nav icon and label match page header icon and title for every page (single source of truth — both derived from `page_config`).
3. Active page highlight works on all pages (Taipy built-in behavior preserved).
4. No hardcoded nav links remain in `template.py`.
5. Widget Spacing Test page is routable via URL but does not appear in navigation.
6. Adding a new page requires only: create `pages/new_page.py` with `PageConfig(nav_section=...)`, add one `PageEntry` to `main.py`, and add the state star-import.
