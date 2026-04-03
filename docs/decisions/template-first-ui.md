# Decision: Template-First Page Architecture for Dashboard

**Status:** Accepted
**Date:** 2026-04-02

## Context

The dashboard has 14+ analytics pages. Building each page as imperative layout code — directly authoring Taipy Markdown strings with chart, table, and metric widgets inline — leads to structural inconsistency that compounds per-page. By the time Streamlit had 12 pages, the codebase had accumulated: missing `help=` tooltips on some pages but not others, different metric format conventions, inconsistent empty-state handling (some pages used `st.info`, others `st.warning`, others nothing), and layout drift where pages looked visually different for no intentional reason.

A CHI audit (CHI-AUDIT-180, CHI-AUDIT-190) against hand-crafted pages produces N per-page findings; the same audit against a shared template produces template-level findings (one fix, all pages).

## Decision

All pages are rendered through `build_page(PageConfig)` in `page_template.py`. A page file contains exactly one `PageConfig` dataclass instance and one `page_md` string derived from `build_page()`. No hand-crafted Taipy Markdown layout code is permitted in page files. `PageConfig` constructor parameters enforce required metadata: `help_text` on every `Metric`, `help` on every `SidebarWidget`, and `Citation` for every page implementing a published algorithm.

Three layout branches exist inside the template: `_build_standard_page` (3fr/1fr sidebar layout), `_build_sub_view_page` (tabbed sub-views), and `_build_dashboard_page` (stats cards + full-width viewport scroll). The presence of `stats: list[StatCard]` in `PageConfig` selects the dashboard branch automatically.

## Alternatives Considered

| Option | Assessment |
|--------|------------|
| Hand-crafted per-page layouts (the Streamlit approach) | Proven to produce drift at scale; missing tooltips and format inconsistencies are bugs waiting to accumulate |
| Component library with conventions (documented but not enforced) | Still drift-prone — conventions require discipline, not structure; new pages can omit tooltips without any error |
| Code generation from YAML | Adds a build step; YAML is harder to read than Python dataclasses for structured config |

## Consequences

**Positive:**
- Missing tooltips, missing citations, and inconsistent metric formats are structurally impossible — `PageConfig` constructor raises `TypeError` if required fields are absent.
- A cross-cutting UI change (e.g., adding glossary filtering to all pages) requires a single edit in `page_template.py`, not 14 edits in 14 page files.
- New pages added by following a 3-file/2-edit checklist (state module, page module, main.py registration, template glossary).

**Negative:**
- The template must be flexible enough to accommodate all page types. New layout variants require extending `page_template.py` — a potential bottleneck if many novel layouts are needed.
- Page authors cannot deviate from the template for one-off layout experiments without modifying the shared template or bypassing the `build_page()` call (which is forbidden by convention).
