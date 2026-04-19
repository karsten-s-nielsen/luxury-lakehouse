# Taipy App — UI Architecture

The Taipy dashboard uses a template-driven architecture where pages are declarative data, not imperative layout code. All page rendering flows through `page_template.py` → `build_page(cfg: PageConfig)`. This document defines the rules for maintaining and extending it.

## Adding a New Page

A new page requires exactly 3 files and 2 edits (4 files for dashboard pages):

1. **`hf_taipy_app/src/state/<page_name>.py`** — State variables, callbacks, SQL queries, chart rendering. Must follow prefix naming (`<prefix>_variable`) to avoid Taipy namespace collisions.
2. **`hf_taipy_app/src/pages/<page_name>.py`** — A `page_config: PageConfig` and `page_md: str` (from `build_page(page_config)`). No hand-crafted Taipy Markdown — the page file is pure configuration.
3. **`hf_taipy_app/src/main.py`** — Import the page's `page_config` and `page_md`, add a `PageEntry` to `PAGE_REGISTRY`.
4. **`hf_taipy_app/src/template.py`** — Add page-specific glossary terms to `PAGE_TERMS`.

### Dashboard Page Variant

For operations/dashboard pages (stats cards + full-width content instead of 3fr/1fr layout):

- Use `stats: list[StatCard]` in `PageConfig` instead of `metrics` — this triggers the dashboard layout (`_build_dashboard_page`), which wraps content in a viewport-contained scroll wrapper (`ll-dashboard-scroll`).
- Call `register_page_refresher("Page-Name", refresh_fn, is_dashboard=True)` — the `is_dashboard` flag ensures the site-wide footer is hidden (dashboard pages render the footer inside the scroll wrapper).
- `ContentRow` wraps content blocks. `StatCard` defines the stat cards in the top bar.

## Template Rules

- **All pages must use `build_page()`**: Zero hand-crafted layouts. A page is a `PageConfig` (title, icon, description, metrics, sidebar widgets, content blocks, citations), not a string of Taipy Markdown.
- **`Metric` requires `help_text`**: If the metric name is not universally understood, `help_text` is mandatory — the `PageConfig` dataclass enforces this. "What does this mean?" and "Is this good or bad?" must be answerable from the tooltip alone.
- **`SidebarWidget` requires `help`**: Every filter widget must have a `help` tooltip explaining what it controls. Help icons are positioned absolute-right of the widget via CSS (`.md-para:has(> .ll-help)`), keeping all widget widths identical regardless of help presence.
- **`Citation` for every methodology**: Any page implementing a published algorithm must include a `Citation(text, url)` in its `PageConfig`. No uncited methodologies. Practitioner methodologies (course materials, coaching frameworks) use `Citation(text)` without a URL but must include "(course materials)" in the label.
- **`NOTICE` file maintenance**: When adding a new analytics module, page, or algorithm, add a corresponding entry to the `NOTICE` file in the project root. The NOTICE file is the authoritative record of all third-party data attributions, library credits, and mathematical/academic references. Every `Citation` in a `PageConfig` and every `references:` entry in a workflow card must have a corresponding NOTICE entry. Update NOTICE in the same change — not as a follow-up.
- **`StatCard` for dashboard stat cards**: Dashboard pages use `stats: list[StatCard]` in `PageConfig`. Each card has `label`, `var`, optional `detail_var`, `help_text`, and `detail_html`. The presence of `stats` activates the dashboard layout branch. Set `detail_html=True` to render `detail_var` as raw HTML via a content provider iframe (supports inline `<span style="">` coloring); default `False` renders as plain text. Convention: every `StatCard` should have `help_text` (same rationale as `Metric`).
- **`ContentBlock` for all content**: Images use `ContentBlock("image", var)`, tables use `ContentBlock("table", var)`, Plotly charts use `ContentBlock("chart", var)`. Tables accept `table_cell_class_name={column: callback_name}` for per-cell CSS styling via Taipy's `cell_class_name` attribute (the callback returns a CSS class string). Never construct raw `<|{var}|chart|>` markup in page files.
- **WCAG color-independence on table columns**: Table columns that use color for categorization (e.g., Type, Freshness) must include a `::before` shape marker as a WCAG 1.4.1 secondary visual cue. Each category gets a distinct CSS-drawn shape (circle, diamond, triangle, square, ring) via `currentColor` so shapes inherit the text color. If the page includes a legend (e.g., DAG legend), shapes and colored text in the legend must match the table column markers.
- **Layout changes go through the template**: If a visual change requires editing more than one page file, it belongs in `page_template.py`. Individual page files contain only page-specific data.
- **`_FOOTER_CONTENT` for footer text**: The footer text ("Interactive Demo · Published Datasets") is a shared constant in `page_template.py`. Dashboard pages render it inside the scroll wrapper; other pages render it as the site-wide footer. Do not hardcode footer text in page files.
- **`is_dashboard=True` on `register_page_refresher`**: Required for dashboard pages. Controls `show_site_footer` state variable — omitting it causes footer duplication.
- **`ll-dashboard-scroll` for dashboard viewport**: Dashboard content is wrapped in a viewport-contained scroll area (`overflow: auto`, `max-height: calc(100vh - 245px)`). Both horizontal and vertical scrollbars live inside this container. The horizontal scrollbar stays at the viewport bottom.
- **State module isolation**: Each page's state module manages its own variables and callbacks. Shared state (competition/team/match filters) lives in `state/shared.py`. No cross-page state imports except from `shared`.
- **Never use `tp_` as a state variable prefix**: Taipy reserves `tp_` internally for its expression evaluator (`TpExPr_`). Variables starting with `tp_` will have state updates silently dropped — no error, no warning. Also avoid `tpec_` (Taipy edge-case prefix). Safe prefixes: `tac_`, `gk_`, `ts_`, `pt_`, `dv_`, etc.
- **Glossary coverage**: Every domain-specific term used in metric names, chart labels, or descriptions must have an entry in `GLOSSARY` (in `template.py`) and be listed in the page's `PAGE_TERMS` entry.
- **Server-driven autocomplete (`SidebarWidget.kind="combobox"`)**: Dropdowns whose LOV would exceed ~200 items must use the `combobox` kind with `search_var` + `on_search_change` instead of the client-side `filterable=True` flag. The template emits a `<|{var}|ll_ext.combobox|...|>` fragment backed by the in-repo Taipy GUI extension at `src/extensions/ll_ext/` (React + MUI Autocomplete, bundle built into `front-end/dist/library.js`). Behaviour is WAI-ARIA APG combobox-with-list-autocomplete: the listbox is hidden until the user types, opens via ArrowDown / typing, closes on Escape / selection / blur, and full keyboard navigation + `aria-activedescendant` highlighting is handled natively. The search-change callback hits a `filters.search_*` function (SQL-backed) or filters an in-memory map (for DV / TAC where the candidate set is already loaded for the page's primary use). Every callback must set the LOV to `[..., filters.NO_MATCHES_SENTINEL]` when the match list is empty — Taipy's state diff treats `state.lov = []` as a no-op, so the bare empty list leaves a stale LOV visible. Previous provisional pattern (`searchable=True` flag on `kind="dropdown"`) was removed 2026-04-18 once all 8 migrations landed; the flag no longer exists on `SidebarWidget`.

## Why Template-First

Building pages as imperative layout code leads to inconsistency debt that compounds per-page. With 12+ pages, hand-crafting each one guarantees: missing tooltips on some pages, different metric formats, inconsistent empty-state handling, and layout drift. The template makes these structurally impossible — required fields are constructor parameters, not afterthoughts. A CHI audit against a template architecture produces template-level fixes (one change, all pages); without it, the same audit produces N per-page fixes.
