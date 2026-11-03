# ADR-008: Taipy for the Ops UI, with four guards and the flask-socketio override

## Status

Accepted (2026-10-27). Decided in spec review (spec §19.6, decision log "keep Taipy with four guards"). The
override re-check is done (below). The install path on macOS is verified in Task 30, on Linux in Task 32, and in the
v2 Space image in Task 33 (spec §26 item 14).

## Context

- The v2 Ops UI shows the modules, the filters, the runs, the review queue, the schedule and the settings
  (spec §19.3). v1 already runs a Taipy app, so v2 reuses the framework and the shell patterns rather than adopting a
  second UI stack.
- Taipy binds the UI to Python state and serves a single-page app over a websocket. That power invites business logic
  and authentication to drift into the UI, which the spec forbids.
- `taipy-gui==4.1.1` caps `flask-socketio<5.5.1`, but Flask 3.1.3's `_session` fix needs `flask-socketio>=5.5.1`. v1
  lifted the cap with a uv override (v1 ADR-083). The v2 spec requires the builder to re-check whether the override
  is still needed and to test the install path on all three operating systems.

## Decision

Keep Taipy for the Ops UI, pinned `taipy-gui==4.1.1`, behind four guards (spec §19.6):

1. **Thin pages over the services layer.** Each `lakehouse.ui.pages.<name>` exposes `TITLE`, `ICON`, `ROUTE` and
   `page_markdown(services)`. A page reads only the Task 25 services (`Services` dataclass) and renders their data
   into Taipy Markdown; it holds no lake access. An import-linter contract forbids `lakehouse.ui` and `lakehouse.web`
   from reaching the lake packages except through the services layer (the contract allows the chain
   ui → services → lake and forbids a direct ui → lake import; ADR notes it as `allow_indirect_imports`).
2. **Sign-in in the web layer, not in Taipy.** `lakehouse.web` (Task 27) owns the Flask app, the OAuth or local
   sign-in, the session and the CSRF token. A global `before_request` guard puts every non-open endpoint — every
   Taipy page included — behind a signed-in session, so no page carries its own auth.
3. **Exact pin and the override.** `taipy-gui==4.1.1`, with `pip-audit` in CI. The uv override
   `override-dependencies = ["flask-socketio>=5.5.1", "python-dotenv>=1.2"]` stays. The Space image installs the
   uv-locked set with `--no-deps`, as v1 does.
4. **Quarterly external-research review.** Taipy is in the quarterly review. Exit trigger: a known security flaw in a
   Taipy dependency that Taipy does not fix, or a forced move off Python 3.12 with no Taipy support.

### Override re-check (2026-10-27, this task)

Removing `flask-socketio>=5.5.1` from the override and re-locking resolves `flask-socketio` to **5.4.1**, below the
Flask 3.1.3 floor of 5.5.1: `taipy-gui==4.1.1` still caps it. **The override is still needed and is kept.** With the
override the lock holds `flask-socketio==5.7.0`. `uv sync --locked` on Windows installs it; macOS (Task 30) and Linux
(Task 32) follow. The `python-dotenv>=1.2` half of the override is a separate, unrelated conflict (taipy-gui pins
`python-dotenv<=1.0.1`, dbt-core 1.12.6 needs `>=1.2`; taipy uses only the stable `load_dotenv`/`dotenv_values` API),
and it is kept too.

### Rendering and latency (author's call)

A page renders its data into Markdown when the Gui is built (`build_gui(services)`), so the server-side render is a
small, fast string build. The page-latency test (`tests/e2e/test_page_latency.py`) measures it well under the budget
(first load ≤ 3 s, cached interaction ≤ 500 ms, spec §19.4). The client-side render on HF is measured in Task 33.

## Consequences

- The UI cannot reach the lake except through the services layer, enforced by the import-linter contract, so the CLI
  and the pages share one tested entry point.
- A page needs no auth code: adding a page cannot forget the sign-in guard, because the guard is global.
- The override is a standing maintenance item: every Taipy, Flask or flask-socketio bump re-runs the re-check, and
  the quarterly review watches the exit trigger.
- Taipy's websocket SPA makes server-side HTML assertions impractical, so the pages are tested as builder functions
  over fake services, and the login guard is tested at the Flask layer; this is weaker than a full browser test,
  which the HF verification in Task 33 adds.

## Amendment (2026-11-02, ADR-013)

Two parts of this ADR drifted and are superseded by later work; the decision to keep Taipy behind four guards still
holds.

- **Page interface (Decision 1).** A page no longer exposes `page_markdown(services)`. The page-binding redesign
  (`docs/specs/2026-10-28-v2-page-state-binding-design.md`) made each page expose `TITLE`, `ICON`, `ROUTE`, a
  `PageConfig` named `CONFIG`, a `refresh(state, services)` that fills bound display variables, and a module-level
  `page = Markdown(build_page(CONFIG))`. The thin-page and import-linter guarantees are unchanged.
- **Page list (Context).** The operator app is now five pages — Monitoring, Data sources, Runs, Review queue,
  Settings — not the modules/filters/runs/review/schedule/settings set. Logs folded into the Runs drill-down,
  "What to download" into Data sources, Schedule into Settings (ADR-013). The thin-page-over-services guard holds for
  every page.
