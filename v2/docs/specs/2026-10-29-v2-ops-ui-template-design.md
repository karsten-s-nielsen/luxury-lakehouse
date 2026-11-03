# v2 Ops UI template and stylekit — design (2026-10-29, rev 7)

> **Superseded in part by ADR-013 (2026-11-02 UI restructure).** The eight-page lists and mappings below are a
> point-in-time snapshot. The operator app is now five pages (Logs folded into Runs, "What to download" into Data
> sources, Schedule into Settings); the template primitives are unchanged. See `docs/adrs/ADR-013-ops-ui-restructure.md`.

Status: ready for owner final approval. The design was A/B spec-approved at rev 5–6; rev 7 records the impl-review
amendments (the owner's decisions are in ingestion §29.19). Author: implementer session. Branch: feat/v2-ingestion.

Rev-7 note: amendments found during the A/B implementation review (owner decisions recorded in ingestion §29.19):

- Source selectors (modules + filters) and the Tournaments multiselect use an `(id, label)` lov with
  `value_by_id=True`, so the operator sees the display name while the bound value stays the id/key (review
  V2UI-IMPL-23/20). The mapping rows below show `value_by_id`.
- `filter_notes` (V2UI-11) is a filters `body` element (the mapping row).
- The settings service rejects an unknown module id (V2UI-IMPL-24), so an action never reports success on a no-op.

Rev-6 note: round-4 both reviewers APPROVE rev 5. Reviewer B added two non-blocking CONSIDER (wording), applied here:

- C09 → the S13 dispatcher-failure test and the `build_gui` duplicate-variable-name check are now in the "Named tests"
  list, not only the prose.
- C10 → the S13 Save guard is named at its enforcement point (the `on_save_filter` collect in `web/live_actions.py`)
  and is stated as a deliverable of THIS cycle, not deferred; the named test runs end-to-end through that collect. No
  scope is reduced.

Rev-5 note: round-3 Reviewer B returned REQUEST CHANGES with one SHOULD FIX (S13) and three CONSIDER, then "with S13
added, I approve"; B verified B04, S03, S08, S09, S11, S12, C04, C05 resolved on Taipy 4.1.1. This revision closes
them:

- S13 → the Gui-level `on_change` dispatcher has a failure rule: on a handler exception it writes the failure into
  `ll_action_notice` (fact 9), does not leave stale state silently, and refuses Save until a reload succeeds; a named
  test drives a raising handler.
- C06 → one handler shape `(state, services, value)` (`on_pick_tournaments` adapted); `build_gui` asserts control
  variable names are unique at build time; the logs mapping row uses `log_level_filter` / `log_text_filter`.
- C07 → fact 8 corrected: `build_gui` already reads `module.page` (`shell.py:206-216,291-297`); the removed string
  path is `_load`/`module.page_markdown` (`shell.py:137-142`), used by `render_for`.
- C08 → a test pins that a signed-in `GET` of a `web/` `.py` module is never 200.

Rev-4 note: round-2 A/B spec review of rev 3 split — Reviewer A APPROVE (7/7 round-1 findings resolved; two
non-blocking CONSIDER), Reviewer B REQUEST CHANGES. The split is not a judgement conflict: B found one reproduced
blocking bug (B04) that A explicitly could not verify (the Taipy `__is_ignored` resolver order). The author confirmed
B04 against the installed Taipy 4.1.1 (`taipy/gui/server.py` and `gitignore_parser.py`) and fixes it. This revision
keeps the rev-3 model and closes every round-2 finding:

- B04 → fact 6 rewritten: `.taipyignore` lives in `ui/` ONLY (never `web/`), so the gitignore matcher anchors at
  `ui/` and `GET /styles.css` resolves; the Flask sign-in guard is the primary source protection; the build path and a
  test pin it.
- S03 (partial) → the control `on_change` uses one Gui-level dispatcher closure over the services (no services in a
  page handler signature).
- S08 (partial) → §29.19 marks the build_page / A-B-first / superset / single-cold-sample decisions as owner
  selections (the offered options the owner chose), with the two verbatim quotes kept; no quote is invented.
- S09 → the filters mapping row uses the as-built `filter_module` / `filter_tournaments` / `draft_360` (not the
  modules page's `active_module` / `draft_active`).
- S11 → the shared notice is cleared on navigation, so a stale notice does not follow the operator; a test pins it.
- S12 / A-SPEC-09 → `Metric`, `StatCard`, `metrics`, `stats` and the `slider` control kind are removed (no cycle-1
  page uses them; the "remove dead types" rule and the owner's "no reusable code for unknown scenarios" both apply).
- A-SPEC-08 → fact 8 marks the "`build_gui` reads `module.page`" clause as target-state wiring, so the as-built
  section stays strictly as-built.
- C04 → `render_for`'s recording state is seeded from the page module's default variable values.
- C05 → the `.taipyignore` covers `__pycache__/` and `*.pyc`; the visual acceptance names a concrete v1 reference
  page.

Rev-3 note (kept): round-1 A/B spec review of rev 2 returned REQUEST CHANGES from both reviewers. The owner chose the
superset. Rev 3 closed every round-1 finding:

- B01 → as-built fact 8 (each page builds its `Markdown` where its variables live).
- B02 → the new "The one render path" section (the live `refresh` path is the single content path; `page_markdown` is
  removed).
- B03 → the ordered-element model with a static-prose element, plus the per-page mapping table.
- S01 → the metrics rail renders only when `metrics` is non-empty.
- S02 → the shared notice renders on every page.
- S03 → the control `on_change` → services-refresh mechanism (the filters Source widget).
- S04 → fact 6 corrected: `.taipyignore` covers both served directories and ships in the package; a 404 test.
- S05 → as-built fact 9 (the `on_init` landing refresh; its failure is shown, not discarded).
- S06 / A-SPEC-03 → one Taipy-free palette source; the loud failure fires at app start.
- S07 / A-SPEC-01 / A-SPEC-02 → the Verification section names every guard.
- S08 / A-SPEC-04 → the owner decisions are recorded in the ingestion spec §29.19; the latency budgets are stated.
- C01 → control labels escape or refuse `|`, `{`, `}`.
- C02 → the table paging rule (`show_all` with a bounded row count).
- C03 / A-SPEC-05 → the pining-registry parity test is a V2UI-IMPL-15 impl fix, not a UI goal; removed from here.
- A-SPEC-06 → one control vocabulary for the sidebar and the body.
- A-SPEC-07 → the `notice_var` render gate (hide on empty).

## Intent and why

Every v2 Ops page, and the anonymous public page, on the HF Space and on a local Mac install, must share the full
lakehouse styling of v1: the brand header, the navigation, the page titles and headers, and the filter placement. The
owner reported the deployed `karstenskyt/ops` Space rendered unstyled and unlike the v1 pages. v1 already has the
worked-out system; v2 adopts it. The template styles all current and future v2 pages, never the page file, and builds
them from declarative data, so one build path keeps them consistent.

## Goals

- A v2 template engine in `lakehouse.ui.template`: a page is declarative data (`PageConfig` + element types), one
  `build_page` path renders it, and all CSS classes and structural wrapping live in the template. A page file writes
  zero styling and zero hand-written Taipy control markup.
- `build_page(PageConfig)` builds every Ops page. A page file exposes a `PageConfig` and a `refresh` that fills the
  bound display variables from the services.
- The full chrome (v1's `build_root_page`): a sidebar (brand + navigation + the per-page filter section), a page header
  (icon + title + description + divider), and a footer.
- The v1 stylekit: the Taipy stylekit base theme through `gui.run(stylekit={...})` (amber-primary / dark-paper
  `--color-*` tokens) plus the ported `ui/styles.css` (`ll-*` vocabulary + Material Symbols `@font-face`).
- The anonymous public page shares the theme and the chrome classes.
- The stylesheet resolves over http on both the HF Space and the local Mac.

## Non-goals and anti-goals

- Not a copy of v1's analytics content: v2 Ops has its own pages. v1's glossary and getting-started panels and the
  analytics-specific sidebar filters are not ported.
- The template emits the action buttons and form controls the pages need. The action HANDLERS already exist in the
  separate live-action layer (`ui/actions.py`, `web/live_actions.py`): this work does not rebuild them from scratch,
  and emits only the controls and buttons they read and dispatch through. This "out of scope" does not exempt the S13
  Save guard, which IS a deliverable of this cycle (see the Dispatcher failure rule).
- Anti-goal: any v2 page or the public page that renders unstyled, or that does not match v1's palette and layout — a
  failure even when functionally correct.

## Constraints

- `lakehouse.ui` reaches the lake only through the services layer (import-linter); the template imports no lake
  package. Only `ui` and `web` import Taipy or Flask.
- Taipy 4.1.1.
- `web/main.serve` builds the Gui the same way for HF mode and local mode.

## The as-built Taipy architecture (the hard-won facts the builder must preserve)

The rev-1 implementation found these facts the hard way. Each is load-bearing: a later change that "simplifies" one
silently breaks styling or reactivity (Chesterton's fence). A code comment pins each one, and a test must pin each one.

1. **`css_file` is the BASENAME, served over http.** Pass `css_file="styles.css"`, not an absolute path. Taipy serves
   `css_file` over http relative to the Gui's `_root_dir`. An absolute path becomes a `file://` link, which the browser
   blocks on an http page, so the whole stylesheet and its `@font-face` silently fail. (This is the fix of rev-1
   decision #4.)
2. **The Gui's `_root_dir` and default variable context come from its frame.** `build_gui` (a function) builds the Gui,
   so it calls `gui._set_frame(_SHELL_FRAME)` — the shell module frame, captured at module scope — before `gui.run`.
   `gui.run` rebuilds the default variable context and `_root_dir` from that frame, so the shared sidebar variables
   bind and `css_file` resolves from the shell directory. `serve()` builds a fresh Gui for Flask-blueprint isolation.
3. **Shared sidebar state lives in the default scope.** Taipy binds the root page from the default (shell) module. The
   filter variables and the tournament `on_change` live in `ui/filter_state`, imported into `shell`. A per-page filter
   section renders only on its page (`current_page == "<route>"`). `ll_action_notice` is a shared variable in `shell`.
   *Amendment (2026-11-02):* this holds for EVERY sidebar control, on every page: its value variable and its lov
   variable are declared in `ui/filter_state`, never in the page module. The Runs Run selector declared them in
   `pages/runs.py`, and the live selector showed no options and no value. `test_sidebar_vars_are_shared` guards it.
4. **Native tables, not Markdown tables.** Taipy's `text|mode=markdown` does not render a Markdown table. Tables use
   the native `table` control fed Taipy's column format — a dict of equal-length column lists (`table_columns` converts
   row dicts). A list of row-dicts does not render. A no-data table is hidden by a `<|part|render=...|>` wrapper,
   because the `table` control ignores `render`.
5. **Four markup gotchas**, each documented at its site: a converted page fills only data into a `<|{var}|text|...|>`
   value, because Taipy escapes markup there, so the header sits in the page definition as Markdown; no Markdown `---`
   before a `<|{var}|...|>` control, because it kills the control's reactive binding; no single f-string page
   definition that holds `{var}` bindings, because Python substitutes them at import and the page renders `<Empty>`;
   the page header is Markdown (`##` + inline `<span>`), not a `<|part|>` wrapper or block HTML.
6. **Source protection: the sign-in guard is primary; `.taipyignore` lives in `ui/` only.** Taipy serves two
   directories as static from its catch-all route (`server.py:205-238`): the `__main__` script directory
   (`web/` under `python -m lakehouse.web`, `server.py:219-229`) and the Gui `_root_dir` (`ui/`, `server.py:230-237`).
   The PRIMARY protection is the Flask sign-in guard: the Taipy catch-all endpoint (`Taipy.my_index`) is not an open
   endpoint (`web/app.py:37-48,127-142`), so an anonymous `GET` of any `.py` source or the stylesheet is redirected to
   sign-in (302). No anonymous source leak is possible, regardless of `.taipyignore`.
   `.taipyignore` is defense-in-depth against a SIGNED-IN operator reading the `ui/` source. It must sit in `ui/`
   (the `_root_dir`) ONLY, NEVER in `web/`. `__is_ignored` reads one file: the one beside `__main__` (`web/`) first,
   else the `_root_dir` one (`server.py:132-142`), and builds ONE `gitignore_parser` matcher anchored at that file's
   directory. `gitignore_parser` matches with `Path.relative_to(anchor)` (`gitignore_parser.py:126`), which RAISES
   `ValueError` for a path outside the anchor. A `web/.taipyignore` would anchor the matcher at `web/`, so serving
   `ui/styles.css` (`server.py:235`) would raise and return 500 — an unstyled Space, the anti-goal (this is review
   B04). With `.taipyignore` in `ui/` only, the matcher anchors at `ui/`: `GET /styles.css` resolves (a `ui/` path, not
   a `*.py`), and a signed-in `GET /shell.py` is excluded (404). A signed-in `GET` of a `web/` source file raises
   inside the matcher and returns 500 — not served, not leaked; the guard already stops the anonymous case. Ship
   `ui/.taipyignore` in the wheel and the Space image (a dotfile is easy to drop from a package). The build path lists
   this; the Verification names the tests.
7. **Public-page privacy guard.** `web/public.py` inlines `ui/styles.css` into the anonymous page, and a privacy test
   scans the body with `assert "match" not in body.lower()` for a leaked fixture name or id. `ui/styles.css` must stay
   free of that substring.
8. **Each page builds its `Markdown` where its variables live.** A Taipy `Markdown` binds its `{var}` names from the
   frame that builds it. The root page is built at shell module scope with `frame=inspect.currentframe()`, and each
   converted page builds `page = Markdown(build_page(CONFIG))` at its own module scope, so its page variables
   (`review_rows`, `logs_rows`, `draft_cron`, …) bind. `build_gui` must NOT build a page's `Markdown` inside itself:
   the frame would be `build_gui`'s locals, the page variables would not bind, and the page would render `<Empty>`.
   The frame-binding rule is as-built: each page already builds its `Markdown` at module scope, and `build_gui`
   already reads that module-level `page` object and does not construct it (`shell.py:206-216,291-297`). TARGET-STATE
   change after this rework: the SEPARATE string path — `_load` reading `module.page_markdown` (`shell.py:137-142`),
   used by `render_for` — is removed (see "The one render path").
9. **The landing page needs an `on_init` refresh.** Taipy shows the first registered page at `/`, but the state writes
   made in the initial `on_navigate` (during the connection handshake) do not reach the client, so the landing page
   would render empty. `on_init` repeats the landing page's `refresh` after the client connects. A refresh failure on
   `on_init` (and on `on_navigate`) is shown in the shared notice, never discarded silently.

## The one render path

A page's content is computed in ONE place: `refresh(state, services)` fills the page's display variables. The live
page is `page = Markdown(build_page(CONFIG))`, which binds those variables (fact 8), so the operator's browser renders
exactly what `refresh` fills. There is no second, hand-written string renderer per page.

- `page_markdown(services)` (the parallel resolved-string renderer each page exposed in rev 1) is REMOVED. It produced
  a hand-written Markdown string that could drift from the live page, and it wrote markup in the page file.
- `render_for(route, services, scope)` keeps its role (spec §6.2 point 3: the content resolved per caller). A public
  caller refused an operator-only page gets the sign-in notice, with no service read. For an allowed page, `render_for`
  runs the page's `refresh` against a recording stand-in state and returns the filled display variables as text. So the
  §29.18 item-16 Space scope proof and the latency test exercise the SAME `refresh` code the browser runs, which is
  what Reviewer B asked for ("prefer the live refresh path"), and the scope proof still inspects resolved content for a
  leaked match id.
- The anonymous public page keeps its Taipy-free render path (`web/public.py` over the `_monitoring_data` module),
  because `web/public.py` must not import Taipy (an import-linter constraint). It is not a per-page string renderer; it
  serves the one anon-viewable page (monitoring) only.
- Consumers that used `page_markdown` (`tests/unit/test_pages.py`, `test_monitoring_pages.py`, `test_page_scope.py`,
  `test_web_entry.py`, `tests/e2e/test_page_latency.py`, and `tests/hf/test_space_hf_live.py` through `render_for`) move
  to `render_for` or the recording-state helper.

## The decided shape (how): the ordered-element build_page model

`build_page(PageConfig) -> str` renders the whole page from data. A page file provides `PageConfig` (static) and
`refresh(state, services)` (fills the bound variables). No page file writes Taipy markup.

`PageConfig` fields:

- **Header** — `title`, `icon`, `nav_section`, `description`. The template renders the Markdown header (gotcha 5).
- **`body`** — ONE ordered tuple of heterogeneous elements, rendered top to bottom in list order (this is the fix for
  B03: order is explicit, not split across separate `content` and `sections` lists). Element kinds:
  - `Prose(markdown)` — a fixed sentence or paragraph, a constant in the `PageConfig`, rendered as static Markdown. It
    binds no variable. This carries the fixed text of `schedule`, `logs` and `modules` (B03's static-prose gap).
  - `Text(var)` — reactive text, `<|{var}|text|mode=markdown|>`, filled by `refresh`.
  - `Table(var, render_var=None, class_name="ll-table", height="24rem")` — the native `table` control (gotcha 4)
    bound to a column-dict variable; `render_var` hides a no-data table through the `part render` wrapper; `height`
    caps its scroll box (C02).
  - `Section(subtitle, items, subtitle_var=None, render_var=None)` — a form group with its own ordered `items`
    sub-list (`Prose`, `Text`, `Table`, `Control`, `Button`), wrapped in the form class. Order inside a `Section` is
    the `items` order. *Amendment (2026-11-02):* `subtitle_var` titles the section from a variable (the picked
    source's name, never a generic "This source"); `render_var` hides it while the variable is false. A run of
    consecutive `Button`s renders as one `ll-actions` row (side by side), in the body and in a `Section`.
  - `Control(kind, var, label, lov=None, on_change=None, condition=None, help=None)` — one input. `kind` is one of
    `selector`, `selector_multi`, `input`, `toggle`. `on_change` marks a services-backed handler (below).
  - `Button(label, action, primary=False)` — an action button; `primary` adds `class_name=ll-primary`.
- **`sidebar_widgets`** — the per-page sidebar filter widgets (filters page), rendered in the root sidebar section and
  gated to the page (`current_page == "<route>"`). A sidebar widget is a `Control`, placed in the sidebar and wrapped in
  `ll-filter-box` (A-SPEC-06: one control vocabulary; placement, not kind, decides the wrapper). *Amendment
  (2026-11-02):* only a `toggle` gets `ll-filter-box` (with an `ll-filter-label`). A `selector` or `input` is an MUI
  outlined control with its own outline and floating label, so it renders bare (the v1 pattern); boxing it drew two
  outlines and squeezed the label ("Tournaments" read "To…"). Every sidebar widget, and every body `selector` or
  `input` with `help`, sits in an `ll-control` wrapper that places the help icon at the control's right edge.
- The shared action notice is NOT a `PageConfig` field. The template renders `<|{ll_action_notice}|text|>` at the foot
  of EVERY page (S02), gated so an empty notice renders nothing (A-SPEC-07). `ll_action_notice` is shared default-scope
  state (fact 3), and `on_navigate`/`on_init` write a load failure into it (fact 9), so every page — including
  monitoring, settings and logs — shows a load failure instead of a blank page. Because the notice is shared across all
  pages, `on_navigate` CLEARS `ll_action_notice` before it refreshes the opened page (S11). So an action notice
  ("Saved…") or a load failure stays only on its own page, and a stale notice does not follow the operator to the next
  page. A refresh that fails writes a fresh failure into the just-cleared notice.

Control `on_change` → services (S03). Taipy calls an `on_change` handler as `(state, var_name, value)`, with no
services, so a page handler cannot take `services` in its signature. The template therefore emits NO control-level
`on_change=<name>` in the markup. Instead `build_gui` installs ONE Gui-level `gui.on_change` closure over `services`
(the same pattern it already uses for `gui.on_navigate` and `gui.on_init`). That closure dispatches by the changed
`var_name` to the owning page's handler, collected into a by-variable map from each `Control.on_change`. Every handler
has the one shape `<name>(state, services, value)`; a handler that needs no services ignores the argument (the
existing `on_pick_tournaments`, which rebuilds clauses from `state` only, is adapted from its `(state, var, value)`
shape to this one — C06). Because the dispatch key is the variable name, the control variable names must be unique
across all pages; `build_gui` asserts no duplicate at build time (C06). The filters Source `Control` (`filter_module`)
is the services case: its handler reloads the tournament list, the saved draft and the §29.13 360 gate, which fixes
the stale-clause save bug (V2UI-IMPL-04).

*Amendment (2026-11-02) — how Taipy reaches the dispatcher.* The premise "the same pattern as `gui.on_navigate`" is
false for `on_change` in Taipy 4.1.1. `Gui._call_on_change` finds a change callback BY NAME (`_get_user_function`:
the state scope, the page locals, then the default module) and never reads the `gui.on_change` attribute. With only
that attribute set, no control handler ever ran live: a Source, Tournaments or Run pick changed the selector and
reloaded nothing. The tests called `gui.on_change(...)` directly, so they could not see it. A global `on_change`
function is also wrong: Taipy calls it for EVERY state write, server-side refresh writes included, so a refresh that
fills a control's variable would re-run that control's handler. So the template now emits `on_change=ll_on_control_change`
on each control that has a handler, and no other control. Taipy calls that one shared callback only when the operator
changes the control. `shell.ll_on_control_change` lives in the default module, and it forwards
`(state, var_name, value)` to the dispatcher that `build_gui` stores on the state's own Gui. The rest of S03 is
unchanged: one dispatcher over the services, the by-variable map, the `(state, services, value)` handler shape, unique
variable names (C06) and the S13 failure rule. No module may define a global `on_change`.
`test_taipy_resolves_the_control_change_callback_by_name` drives Taipy's own lookup and guards both rules.

Dispatcher failure rule (S13). Taipy catches a handler exception and only logs it (`gui.py:855-857`), so a failed
handler would leave the stale state in place with no notice: a failed filters Source reload would keep the old
tournaments, draft and 360 gate, and a following Save would write them into the NEW module — V2UI-04 on the error
path. So the dispatcher wraps each handler. On an exception it writes the failure into `ll_action_notice` (the same
rule as fact 9), it does not leave the stale state silently, and it marks the draft stale (a `draft_stale` state flag).
The Save guard reads that flag and refuses the save until a reload succeeds. The guard is enforced in the
`on_save_filter` collect (`web/live_actions.py`) and is a deliverable of THIS cycle (the V2UI-04 error-path fix), not
deferred to cycle 2; the non-goal that the action handlers "already exist / out of scope" covers not rebuilding them
from scratch, and does not exempt this guard. A named end-to-end test drives a reload handler that raises, then asserts
`ll_action_notice` is set AND the Save is refused through the `on_save_filter` collect (not a unit stub).

Label safety (C01). `build_page` escapes or refuses `|`, `{` and `}` in a `Control.label` or `Button.label`, because an
unescaped one of these breaks the control or creates a stray binding.

Table paging (C02). A `Table` renders with `show_all`, and `refresh` fills a bounded row count (the AGENTS LIMIT rule:
500 for a ranking, 2000 for a timeline). No Ops table is unbounded, so `show_all` is safe and avoids a pager on a short
table. *Amendment (2026-11-02):* a bounded row count does not bound the table's HEIGHT. Taipy caps the scroll box at
80vh by default, so one long table (a run's 40 steps, 200 log lines) filled the screen. Every `Table` therefore emits
`height=24rem` (about ten rows; the rows past it scroll inside the table, and a shorter table shrinks to fit). A page
may set its own `height` (the Data sources table of one row per source uses 40rem, so every source stays in view).

Palette single source (S06 / A-SPEC-03). The `--color-*` tokens live in ONE Taipy-free module, for example
`ui/palette.py`, as a plain dict. `shell.STYLEKIT` and `web/public.py`'s inline `:root` block both derive from it, so
the operator pages and the public page cannot drift. `web/public.py` still imports no Taipy (the palette module is
Taipy-free). A missing `ui/styles.css` is a loud failure at APP START (when `web/main.serve` builds the app), not a
silent unstyled render and not a per-request check.

CSS completeness (S02 / A-SPEC-02). `ui/styles.css` defines a rule for every class the template can emit. Today it is
missing `.ll-primary` and `.ll-signin-notice`, and `.ll-table` is only a marker (tables are styled through
`.taipy-table`). The build adds a `.ll-primary` rule (the amber primary button) and a `.ll-signin-notice` rule, and
either gives `.ll-table` a rule or the Verification test treats it as a known marker styled through `.taipy-table`.

## Per-page element mapping (proof the model expresses all 8 pages)

Each row is the page's `body` in order. "notice" is the template's shared notice, on every page.

| Page | `body` in order | Sidebar |
|---|---|---|
| monitoring | `Text(top)`, `Table(steps, render_var=has_run)`, `Text(deps)`, `Table(dbt, render_var=has_run)`, `Text(note)` | — |
| settings | `Text(cfg_prose)`, `Table(cfg_history)` | — |
| modules | `Prose("Each row is one provider through one channel…")`, `Table(module_rows)`, `Section("Change a source", [Control(selector, active_module, label=Source, lov=module_ids, value_by_id, on_change=on_pick_module), Control(toggle, draft_active, label=Active), Button(Apply, primary, on_module_active), Button("Run now", on_run_now)])` | — |
| filters | `Table(filter_preview_rows)`, `Text(filter_notes)` (V2UI-11: the source preview notes, never dropped), `Button(Preview, on_preview_filter)`, `Button("Save filter", primary, on_save_filter)` | `Control(selector, filter_module, label=Source, lov=filter_module_ids, value_by_id, on_change)`, `Control(selector_multi, filter_tournaments, label=Tournaments, lov=filter_tournament_lov, value_by_id, on_change=on_pick_tournaments)`, `Control(toggle, draft_360, condition=draft_is_statsbomb)` |
| runs | `Button("Run now", primary, on_run_now)`, `Text(runs_progress)`, `Table(runs_rows)` | — |
| review | `Text(review_notes)`, `Table(review_rows)`, `Section("Accept a batch", [Control(selector, decide_entity, label=Entity), Control(input, accept_batch_filter), Button("Accept recommended", primary, on_accept_recommended)])`, `Section("Decide", [Control(selector, decide_item), Control(selector, decide_action), Control(input, decide_reason), Button("Decide selected", on_decide)])`, `Section("Undo", [Control(input, undo_batch_id), Button("Undo last batch", on_undo_batch)])` | — |
| schedule | `Prose("One schedule runs every active data source. A missed run is not replayed…")`, `Table(schedule_rows)`, `Section("Edit", [Control(input, draft_cron, label=Cron), Control(toggle, draft_enabled), Button("Save schedule", primary, on_save_schedule), Button(Pause, on_pause_schedule), Button(Resume, on_resume_schedule), Button("Run now", on_run_now)])` | — |
| logs | `Prose(f"The most recent {DEFAULT_N} log lines…")`, `Section("Filter", [Control(input, log_level_filter, on_change), Control(input, log_text_filter, on_change)])`, `Table(logs_rows)` | — |

Every control the 8 pages use is in `{selector, selector_multi, input, toggle}`; every display block is
`Prose`, `Text` or `Table`; every button is `Button`. No page builds a dynamic structure (a variable section count);
dynamic data goes into table rows, which `table_columns` already handles.

## Build path

1. `ui/template.py` holds `PageConfig`, `NavSection`, `PageEntry`, `Prose`, `Text`, `Table`, `Section`, `Control`,
   `Button`, `table_columns`, `build_page`, `build_header_from_config`, `build_nav`, `build_sidebar_section`, and the
   root-page chrome builder. Any element type with no production page is removed, not carried dead (rev 1 carried
   `build_page`, `ContentBlock`, `ContentRow`, `Metric`, `StatCard` dead). `ContentBlock` and `ContentRow` are replaced
   by the `body` element list above. `Metric`, `StatCard`, the `metrics`/`stats` fields and the `slider` control kind
   are removed: no cycle-1 page uses them, and the owner's rule is no reusable code for unknown scenarios (S12 /
   A-SPEC-09). A later page that needs them adds them back with an owner decision.
2. Each `ui/pages/<name>.py` exposes a `PageConfig`, a `refresh`, and `page = Markdown(build_page(CONFIG))` built at
   module scope (fact 8). The page writes no Taipy markup, and exposes no `page_markdown`. `build_gui` reads the
   module-level `page` and binds `refresh` and each `Control.on_change` over the services.
3. `ui/styles.css` is the ported `style_v2.css` subset (`@font-face`, the `ll-*` vocabulary, the Taipy
   table/navbar/watermark overrides, responsive), with a rule for every emittable class (`.ll-primary`,
   `.ll-signin-notice` added). The palette tokens come from the one Taipy-free palette module.
4. `web/main.serve`'s `gui.run(...)` sets the stylekit from the palette module; `css_file` is the basename (fact 1).
   `.taipyignore` sits in `ui/` ONLY, never `web/` (fact 6 / B04), and lists `*.py`, `*.pyc`, `__pycache__/` and
   `.taipyignore` (C05); it ships in the wheel and the Space image. The Flask sign-in guard is the primary source
   protection (fact 6). A missing `ui/styles.css` fails loudly at app start (S06).
5. `web/public.py` serves the shared theme: the `:root` block derived from the one palette module plus the inlined
   `ui/styles.css`, wrapped in the lakehouse chrome classes. A missing stylesheet is a loud failure, not a silent
   unstyled render.

## Verification

Visual acceptance (the anti-goal's closer). Run the v2 app locally and screenshot, with Puppeteer, each of the 8 Ops
pages, the filters sidebar section, and the public page. The reproducible v1 reference is the v1 app rendered from
`hf_taipy_app/src/template.py` (the `build_root_page` chrome), `hf_taipy_app/src/page_template.py` (`build_page`), and
`hf_taipy_app/src/style_v2.css` (the stylekit). The acceptance criteria, each confirmed against that v1 rendered look:

- the dark paper background and the amber brand gradient on the title row;
- the Material Symbols icons in the nav and the page header (not empty boxes);
- the sidebar chrome (brand border, grouped nav under the single "Operations" group, the per-page filter box);
- the native tables styled (not raw Markdown pipes);
- the filter placement in the sidebar, gated to the filters page;
- a styled `.ll-primary` button and a styled public sign-in notice.

Store the accepted screenshots at `v2/docs/evidence/2026-10-29-ops-ui/` and name them per page, so a reviewer checks
the same images. Re-check the deployed `karstenskyt/ops` Space after the next deploy.

Named tests:

- **No page writes Taipy markup** (S07a / A-SPEC-01): a test scans every `ui/pages/*.py` source and asserts it contains
  no `<|` and no `class_name=`, and that each page module exposes a `PageConfig` and a `refresh` and no `page_markdown`.
- **Every emittable class has a rule** (S07b / A-SPEC-02): a test enumerates every `class_name` `build_page` can emit
  and asserts `ui/styles.css` defines a matching selector (or lists `.ll-table` as a known `.taipy-table` marker).
- **`build_page` emits the chrome** for every `PageConfig`, and the public page carries the shared theme classes.
- **The seam** is pinned: `css_file` is a basename (fact 1); `gui._set_frame`/`_root_dir` (fact 2); each page builds its
  `Markdown` at module scope (fact 8); the `on_init` landing refresh and its shown-not-discarded failure (fact 9).
- **Stylesheet served and source protected** (S04 / B04): with `ui/.taipyignore` in place and no `web/.taipyignore`,
  a signed-in `GET /styles.css` returns 200 and a signed-in `GET /shell.py` returns 404; a signed-in `GET` of a `web/`
  `.py` module is never 200 (it raises in the matcher and returns 500, never serving source — C08); an anonymous
  `GET /styles.css` and `GET /shell.py` both return 302 to sign-in (the guard). A unit test asserts that under
  `python -m lakehouse.web` the ignore file Taipy reads is `ui/.taipyignore` (the `web/` fallback is absent), so the
  matcher anchors at `ui/`.
- **ACTIONS derive from the model** (S07c): each page's `ACTIONS` tuple is derived from its `Button.action` values, so
  the two lists cannot drift.
- **on_change dispatcher failure** (S13 / C09): an end-to-end test drives a reload handler that raises and asserts
  `ll_action_notice` is set AND the Save is refused through the `on_save_filter` collect (`web/live_actions.py`).
- **Unique control variable names** (C06 / C09): a test asserts `build_gui` rejects two controls that bind the same
  variable name (the by-variable `on_change` dispatch needs unique names).
- **The 360 gate value** (S07d / V2UI-IMPL-05): `refresh` for a non-StatsBomb module sets `draft_is_statsbomb is
  False`, and for a StatsBomb module sets it `True`.
- **Palette single source** (S06 / A-SPEC-03): a test asserts `shell.STYLEKIT` and the public `:root` tokens are both
  derived from the one palette module (equal palettes).
- **Latency** (S07e / A-SPEC-04 / §29.18): `tests/e2e/test_page_latency.py` asserts, on the live `refresh` path, the
  cached warm (second) render PER PAGE — the five pure lake pages ≤ 500 ms, `review` ≤ 1.0 s (local carve-out), the two
  external pages ≤ 3 s. The COLD first-load bound (§19.4, ≤ 3 s) and the item-16 client render are measured on HF by the
  Task-33 page test (`tests/hf/test_space_hf_live.py`), where the object cache makes the warm 500 ms budget
  authoritative. The per-page warm method and the `review` carve-out are recorded in §29.19.
- **Shared notice on every page** (S02): a test asserts `build_page` emits the `ll_action_notice` element for a page
  with no notice field (monitoring, settings, logs included).
- **Notice cleared on navigation** (S11): a test sets `ll_action_notice`, runs `on_navigate` to another route, and
  asserts the notice is cleared before the target page's refresh (so it does not follow the operator).

## Owner decisions

The owner decisions behind this rework — the single "Operations" nav group, the `build_page` adoption, the A/B spec
review before the impl commit, the per-page warm latency method (cold ≤ 3 s measured on HF), and the superset scope — are recorded in the
ingestion spec §29.19 with the owner's words (S08 / A-SPEC-04). This design document is the engineering detail; §29 is
the decision record. After this re-review, the status line carries the owner's approval.

## Left to the builder

- The exact `Control`/`Section`/`Button` field names, as long as the model expresses all 8 pages (the mapping table
  above) with no page writing markup.
- Which v1 CSS rules are Ops-relevant versus analytics-only.
- The recording-stand-in-state helper that `render_for` uses to run `refresh` off-connection; seed it from each page
  module's default variable values, so an unfilled variable reads its module default, not an attribute error (C04).
- The `ll_ext` combobox extension — cycle-1 LOVs are small; omit it unless a page needs it.
