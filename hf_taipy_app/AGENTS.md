# AGENTS.md — Taipy App UI Architecture

Class-1 rules for the template-driven Taipy dashboard. Detail, rationale, and the full "why template-first" narrative: → docs/context/ui-ux.md.

## Rules

- **All page rendering flows through build_page** — `page_template.py` → `build_page(cfg: PageConfig)`; pages are declarative data, not imperative layout. → docs/context/ui-ux.md
- **A new page requires exactly 3 files and 2 edits** (4 for dashboard): `state/<page>.py`, `pages/<page>.py`, `main.py` (`PAGE_REGISTRY` + `PageEntry`), `template.py` (`PAGE_TERMS`). → docs/context/ui-ux.md
- **All pages must use build_page, zero hand-crafted layouts** — a page is a `PageConfig`, never a string of Taipy Markdown. → docs/context/ui-ux.md
- **dashboard layout triggered by stats list StatCard** — `stats: list[StatCard]` in `PageConfig` selects `_build_dashboard_page` + the `ll-dashboard-scroll` wrapper. → docs/context/ui-ux.md
- **Metric requires help_text** when the metric name is not universally understood (`PageConfig` enforces it). → docs/context/ui-ux.md
- **SidebarWidget requires help** — every filter widget carries a `help` tooltip explaining what it controls. → docs/context/ui-ux.md
- **Citation for every methodology** — any published algorithm needs a `Citation(text, url)`; no uncited methodologies. → docs/context/ui-ux.md
- **NOTICE file is the authoritative record** of third-party attributions; every `Citation`/`references:` entry has a NOTICE entry, updated in the same change. → docs/context/ui-ux.md
- **StatCard for dashboard stat cards** — `label`/`var`/`detail_var`/`help_text`/`detail_html`; every card should have `help_text`. → docs/context/ui-ux.md
- **SubView may carry its own stats list StatCard** — `_build_sub_view` renders a top `ll-stats-bar`. → docs/context/ui-ux.md
- **RequiredFilter for filter requirements** — template auto-generates `empty_condition` + `empty_message`. → docs/context/ui-ux.md
- **ContentBlock for all content** (image/table/chart); never construct raw `<|{var}|chart|>` markup in page files. → docs/context/ui-ux.md
- **WCAG color-independence on table columns** — a `::before` shape marker (WCAG 1.4.1) is the secondary visual cue. → docs/context/ui-ux.md
- **Layout changes go through the template** — any change touching more than one page file belongs in `page_template.py`. → docs/context/ui-ux.md
- **_FOOTER_CONTENT for footer text** — shared constant in `page_template.py`; never hardcode footer text in page files. → docs/context/ui-ux.md
- **is_dashboard True on register_page_refresher** controls `show_site_footer`; omitting it causes footer duplication. → docs/context/ui-ux.md
- **State module isolation** — shared filters live in `state/shared.py`; no cross-page state imports except from `shared`. → docs/context/ui-ux.md
- **Never use tp_ as a state variable prefix** — Taipy reserves it (updates silently dropped); avoid `tpec_` too. → docs/context/ui-ux.md
- **Glossary coverage** — every domain-specific term has a `GLOSSARY` entry and is listed in the page's `PAGE_TERMS`. → docs/context/ui-ux.md
- **Dropdowns whose LOV would exceed** ~200 items use `kind="combobox"` with `search_var` + `on_search_change` (`ll_ext` extension at `src/extensions/ll_ext/`); set `NO_MATCHES_SENTINEL` when empty. → docs/context/ui-ux.md
