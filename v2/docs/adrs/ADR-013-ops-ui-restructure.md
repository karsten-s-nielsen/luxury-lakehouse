# ADR-013: Operator UI restructure — five pages, drill-downs, on-demand editor

## Status

Accepted (2026-11-02). Decided from the cognitive-interface audit of the v2 operator app
(`D:\Development\_reviews\2026-11-02-v2-ui-cognitive-audit.md`, 9 findings, 0 Critical); the owner approved F1–F8.
Amends ADR-008 (the page list and the page interface). Verified live on the Mac Mini; uncommitted, pending the A/B
implementation review and the owner's commit approval.

## Context

- The operator app had eight flat top-level pages in one "Operations" nav section. The audit found two cases where
  pages were really one object shown at two levels, and a consistency gap against the v1 convention (per-metric help,
  scale and direction).
- "Runs", "Monitoring" and "Logs" all pivoted on one run but were three pages pinned to the latest run; a user could
  not move run → its steps → its logs.
- "Data sources" and "What to download" were the same source at two levels, and "What to download" made the operator
  re-pick the source.
- The source preview on the "What to download" page is an external listing (a provider API), not a lake read, so it
  never warms the Space object cache; its synchronous first render overran the §19.5 page budget (audit-ledger T8).

## Decision

1. **Eight pages become five** — Monitoring, Data sources, Runs, Review queue, Settings.
   - Logs folds into the Runs **run-detail drill-down** (`logs.py` deleted; `_logs_data.py` holds the Taipy-free log
     helpers). A sidebar Run selector drives a detail panel: the selected run's steps, spend, layer dependencies and
     logs (F6).
   - "What to download" folds into **Data sources** (`modules.py`). One shared `filter_module` Source selector drives
     both the active toggle and the tournaments/Preview/Save editor, so the operator picks the source once.
     `filters.py` becomes a non-page editor helper (F7).
   - Schedule folds into **Settings** as a section. `schedule.py` becomes a non-page status/edit helper (F8).
2. **Drill-down pattern.** A drill-down is a bound selector + an `on_change` handler + a render-gated detail part on the
   same route — the v1 selector → detail pattern reused with the existing template primitives. No new routing.
3. **On-demand editor on Data sources (resolves T8 / doc-audit D1).** `modules.refresh` fills the module table and the
   Source selector (lake reads) but loads the external source preview only when the operator opens a source
   (`on_pick_source` sets `editor_loaded`). The Data sources navigate is therefore a lake read and keeps the §19.5
   ≤500 ms budget (`_LAKE_PAGES` in the latency tests). Until a source is opened the editor shows a prompt and the draft
   is stale, so a Save is refused.
4. **Interpretation (F1–F3, F5).** Every control carries `help=` (a `.ll-help` info icon with a title tooltip); every
   quantitative value states its scale and direction; a bronze/silver/gold glossary line sits on the layer-dependency
   views. Control hints moved from the labels into the help.
5. **No raw ids (F4).** The Review undo takes a recent-batches selector, backed by `ReviewService.recent_batches()`
   (a decisions-store read, no catalog attach), instead of a typed batch id.

## Consequences

- Fewer top-level choices and run → steps → logs traceability on one object. The drill-downs add no routing surface.
- `PAGE_MODULES` lists five modules; `filters.py` and `schedule.py` are importable helpers but are not registered pages.
- Settings reads the schedule backend on load (a status read), so it keeps the §19.4 ≤3 s page-load budget; Data
  sources returns to the ≤500 ms lake budget.
- This supersedes the eight-page lists in the dated design docs (`docs/specs/2026-10-29-v2-ops-ui-template-design.md`,
  `docs/specs/2026-10-28-v2-page-state-binding-design.md`), which carry a supersession banner. ADR-008's page list
  (Context) and page interface (Decision 1, `page_markdown(services)`) are amended: a page now exposes `TITLE`, `ICON`,
  `ROUTE`, a `PageConfig` `CONFIG`, a `refresh(state, services)` and a module-level `page = Markdown(build_page(CONFIG))`
  (the page-binding design).
- The whole change is tested as builder functions + the page-binding integration tests; verified live on the Mac Mini.
