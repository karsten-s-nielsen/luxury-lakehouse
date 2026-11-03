# v2 Ops pages — reactive state binding implementation plan

> **For agentic workers:** implement task-by-task with TDD (write the failing test, watch it fail, minimal code,
> watch it pass). Steps use checkbox (`- [ ]`) syntax. No commit per task.

**Goal:** Make every operator page fully interactive — controls hold values, buttons perform writes through the
guarded dispatch, and the page reflects the result — with a pattern that scales to a dozen+ pages and no hacks.

**Architecture:** Each `ui/pages/<name>.py` owns its module-level bound state (control + display variables), builds
its `taipy.gui.Markdown` in-module (so the page's renderer frame — not `build_gui`'s dead frame — is captured), and
exposes `refresh(state, services)` + control `on_change`s. `build_gui` registers pages with `gui.add_page(route,
page.page)`, installs one Gui-level `on_navigate` backed by a route→`refresh` registry, and keeps the root layout +
nav. Writes keep routing through the one Gui-level `gui.on_action` dispatcher → `authz.dispatch` (unchanged gating),
which gains only a post-action `refresh` of the active page.

**Tech Stack:** Taipy GUI 4.1.1, Flask, flask-socketio; Python 3.12; pytest.

**Spec:** `docs/specs/2026-10-28-v2-page-state-binding-design.md` (A/B-approved round 2).

## Global Constraints

- **Security invariant:** no bound callable name may equal an action name (`ACTION_NAMES`). Taipy `__on_action`
  resolves `_get_user_function(name)` before the Gui-level `on_action`; a colliding bound name would skip
  `authz.dispatch`. Page `on_change` callbacks are named for their control (`on_pick_*`), never for an action.
- **No change** to `services/*`, `services/authz.py` (role model + `dispatch` gating/attribution), the socket wrap
  (`guard_live_socket`), `ValidLogins`/`ConnectionRegistry`, or the public page's render. The only `web/live_actions.py`
  delta is a post-action `refresh` call (and `_collect` extends only for a new action).
- **The public page stays Taipy-free:** shared monitoring computation lives in a Taipy-free module; `web/public.py`
  imports no `taipy`.
- Python 3.12; line length 120; ruff [E,W,F,I,N,UP,B,S,BLE,RUF]; strict pyright; import-linter 5 contracts.
- No commit per task. All of this cycle (observability + live-web + page-binding) lands in **one** cycle commit
  behind the owner's explicit approval + two A/B implementation reviews.

## Review Focus

- A page whose `refresh` reads a service that raises (missing table on a fresh lake) → visible fallback, never a
  blank page or a stack trace. Test: `test_page_service_error_shows_fallback` (PB8).
- An invalid control value (empty cron, unknown module id) → the service's validation error shown via the notice,
  never a silent no-op. Test: `test_invalid_control_value_surfaces_error` (PB8).
- Two operators' unsaved drafts never collide (per-client state). Test: `test_two_clients_isolated` (PB8).
- A bound name colliding with an action silently skips dispatch. Test: `test_no_bound_name_is_an_action` (PB2).
- Display stale after a write (no post-action refresh). Test: `test_action_then_refresh` (PB2).

---

## Task PB1: Taipy-free monitoring data module

**Files:** Create `v2/src/lakehouse/ui/pages/_monitoring_data.py`. Modify `v2/src/lakehouse/ui/pages/monitoring.py`
and `v2/src/lakehouse/web/public.py` to import from it. Test: `v2/tests/unit/test_public_page.py` (kept),
`v2/tests/unit/test_monitoring_pages.py`.

**Interfaces — Produces:** `spend_line(rows) -> str`, `dependency_lines(view) -> tuple[list[str], list[str]]` (or the
existing pure helpers), importing no `taipy`. **Consumes:** `observability` types only.

- [ ] **Step 1:** Write `test_monitoring_data_pure` — `ui.pages._monitoring_data.spend_line(rows)` returns the dated
  estimate string, the dependency/step formatting matches the current output, and the module imports no `taipy`
  (subprocess `sys.modules` check). Keep `test_public_page_no_taipy` as the standing regression guard (it is green
  now and must STAY green when `monitoring.py` gains a module-level `Markdown` in PB5 — that is the risk PB1 removes;
  it is a guard, not this task's red-green — Reviewer A D5-PLAN-02).
- [ ] **Step 2:** Run. Expected: `test_monitoring_data_pure` FAILS (`ModuleNotFoundError: _monitoring_data`);
  `test_public_page_no_taipy` still passes.
- [ ] **Step 3:** Create `_monitoring_data.py` with `spend_line` + the dependency/step formatting (pure, Taipy-free);
  repoint `web/public.py` and `monitoring.py` to import from it.
- [ ] **Step 4:** Run both; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB2: Framework foundation + the `modules` reference page

**Files:** Modify `v2/src/lakehouse/ui/shell.py` (`build_gui` → `add_page`, one Gui-level `on_navigate`, the
route→`refresh` registry, root/nav kept), `v2/src/lakehouse/web/live_actions.py` (post-action `refresh`; and
`_collect`'s `on_preview_filter`/`on_save_filter` read `filter_module` not `active_module` — the recorded §5 delta),
`v2/src/lakehouse/ui/pages/modules.py` (module-level state + `Markdown` + `refresh` + `on_change`). Test:
`v2/tests/integration/test_page_binding.py` (new), `v2/tests/unit/test_pages_modules.py` (new).

**Interfaces — Produces:** each page module exposes `ROUTE`, `TITLE`, `ICON`, `page` (`taipy.gui.Markdown`),
`refresh(state, services) -> None`, and its module-level state variables; `shell.build_gui(services)` registers them,
installs `on_navigate(state, page_name, ...)` that calls the opened page's `refresh` from a `dict[route, refresh]`
closed over `services` and returns `page_name`; `live_actions` dispatcher calls the active page's `refresh` after a
successful `dispatch`. **Consumes:** L1–L6 `gui.on_action`, `authz.dispatch`, `ConnectionRegistry`.

- [ ] **Step 1: Write the failing tests** — `test_every_page_var_binds` (after `build_gui`, `get_all_keys()` contains
  `active_module`, `draft_active`, `module_ids`); `test_no_bound_name_is_an_action` (for every `ACTION_NAMES` entry,
  Taipy `_get_user_function(name)` is not callable after `build_gui`); `test_nav_refresh_fills_display` (the Gui-level
  `on_navigate` for `modules` fills the module rows from a fake services and returns `"modules"`);
  `test_action_then_refresh` (a successful `on_module_active` dispatch re-runs `modules.refresh`, so the display shows
  the new active flag); `test_collect_inputs_declared_per_page` (for every `ACTION_NAMES` entry, each key `_collect`
  reads is a module-level variable of the page module whose `Markdown` references that action — Reviewer B PSB-B12;
  seed one mismatch and watch it red before wiring the pages); `test_pages_modules` unit (module-level
  `active_module`/`draft_active`/`module_ids` defaults + types; `on_pick_module` loads the picked module's active flag
  into `draft_active`; `refresh` fills `module_ids` + rows from a fake services; `modules.page` builds).
- [ ] **Step 2:** Run. Expected FAIL (dead-frame build binds nothing; no `on_navigate`; no post-action refresh; the
  structural guard red against the current `active_module`-for-filters contract).
- [ ] **Step 3:** Implement: `modules.py` declares module-level state + `on_pick_module` + `refresh` + builds `page`;
  `shell.build_gui` switches to `add_page` + the `on_navigate` + registry (root layout + nav kept);
  `live_actions` adds the post-action `refresh` of the active page (gating unchanged) and renames the filters
  `module_id` read to `filter_module`.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB3: `filters` page

**Files:** Modify `v2/src/lakehouse/ui/pages/filters.py`. Test: `v2/tests/unit/test_pages_filters.py` (new).

**Interfaces — Produces:** module-level `filter_module: str` (selector), `draft_clauses: list[Clause]` (a bound
clause editor — a control the operator edits, not a fixed empty list), `draft_360: bool`; `on_pick_filter_module`
`on_change` reloads the preview + seeds `draft_clauses`/`draft_360` from that module's current filter; `refresh` fills
the preview diff + `filter_module` options from `services.filters`; `page` built in-module. Actions
`on_preview_filter`/`on_save_filter` route through the dispatcher, which reads EXACTLY `filter_module`, `draft_clauses`,
`draft_360` (`_collect` after the PB2 rename; Reviewer B PSB-B10).

- [ ] **Step 1: Write the failing tests** — defaults + types; `on_pick_filter_module` reloads the preview and seeds
  the clause editor; `refresh` fills the diff; `page` builds; the Markdown references
  `on_action=on_preview_filter`/`on_save_filter` and has a bound clause control for `draft_clauses`;
  `test_filter_save_writes_picked_module_clauses` (pick a non-default module, edit `draft_clauses`, dispatch
  `on_save_filter`; `services.settings.set_module_filter` is called with the PICKED `filter_module` and the EDITED
  clauses — never `statsbomb_public` / `[]`).
- [ ] **Step 2:** Run. Expected FAIL.
- [ ] **Step 3:** Implement the pattern (PB2 Interfaces), binding `filter_module`/`draft_clauses`/`draft_360`.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB4: `schedule` page

**Files:** Modify `v2/src/lakehouse/ui/pages/schedule.py`. Test: `v2/tests/unit/test_pages_schedule.py` (new).

**Interfaces — Produces:** module-level `draft_cron: str`, `draft_enabled: bool`, `schedule_version: int`; `refresh`
fills the status + seeds `draft_cron`/`draft_enabled`/`schedule_version` from `services.schedule.get()`; `page`
built in-module. Actions `on_save_schedule`/`on_pause_schedule`/`on_resume_schedule`/`on_run_now` through the
dispatcher (reads `draft_cron`, `draft_enabled`, `schedule_version`).

- [ ] **Step 1: Write the failing test** — defaults + types; `refresh` seeds the three vars from a fake schedule
  service; `page` builds; the action names are referenced.
- [ ] **Step 2:** Run. Expected FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB5: display-only pages — `runs`, `settings`, `monitoring`

**Files:** Modify `v2/src/lakehouse/ui/pages/runs.py`, `settings.py`, `monitoring.py`. Test:
`v2/tests/unit/test_pages_display.py` (new); `v2/tests/unit/test_monitoring_pages.py` (updated).

**Interfaces — Produces:** each declares its module-level display variables + `refresh(state, services)` + builds
`page`; `runs` keeps `on_run_now` (no input); `monitoring` renders from the PB1 `_monitoring_data` module (anon
parity with the public page); `settings` is display-only (no controls). No control `on_change`.

- [ ] **Step 1: Write the failing test** — for each page: `refresh` fills its display vars from a fake services; the
  `page` builds; `monitoring`'s computation matches the public page's (same `_monitoring_data`).
- [ ] **Step 2:** Run. Expected FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB6: `logs` page (reactive filter)

**Files:** Modify `v2/src/lakehouse/ui/pages/logs.py`. Test: `v2/tests/unit/test_pages_logs.py` (new).

**Interfaces — Produces:** module-level `log_level_filter: str`, `log_text_filter: str`; `on_change` re-runs the
filter (re-reads `services.runs.logs(level, text)`); `refresh` fills the rows; `page` built in-module. No write
action (filtering is a read).

- [ ] **Step 1: Write the failing test** — defaults + types; changing `log_level_filter`/`log_text_filter` re-filters
  the rows from a fake services; `page` builds; authenticated-only (unchanged).
- [ ] **Step 2:** Run. Expected FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB7: `review` page (adds decide/undo inputs)

**Files:** Modify `v2/src/lakehouse/ui/pages/review.py`. Test: `v2/tests/unit/test_pages_review.py` (new).

**Interfaces — Produces:** module-level `decide_entity: str`, `accept_batch_filter: str`, `undo_batch_id: str`,
`decide_item: str`, `decide_action: str`, `decide_reason: str` — all SIX, matching `_collect` exactly
(`web/live_actions.py:63-72`: `on_accept_recommended` → `decide_entity`, `accept_batch_filter`; `on_undo_batch` →
`undo_batch_id`; `on_decide` → `decide_item`, `decide_action`, `decide_reason`). Input widgets for each in the
Markdown; `refresh` fills the queue; `page` built in-module. This is the one page that gains new input controls, so
`on_accept_recommended`/`on_undo_batch`/`on_decide` become functional (Reviewer B PSB-B11, PSB-B14).

- [ ] **Step 1: Write the failing tests** — the six vars with defaults + types; the Markdown has an input per
  decide/undo/accept field and references the three action names; `refresh` fills the queue from a fake review
  service; `page` builds; `test_accept_recommended_carries_entity` (bind `decide_entity`, dispatch
  `on_accept_recommended`, assert the review service is called with that entity, not `None`).
- [ ] **Step 2:** Run. Expected FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task PB8: cross-cutting tests + full verification

**Files:** Test: `v2/tests/integration/test_page_binding.py` (extend). No production change unless a test surfaces a
gap (then fix under TDD).

- [ ] **Step 1: Write the tests** — `test_two_clients_isolated` (two Taipy client scopes hold independent drafts; one
  operator's unsaved `draft_active` never reaches the other). It must be SEEN TO FAIL first: assert the red against a
  deliberately-shared single scope (or before the per-client binding), watch it fail, then make it green with the
  real per-client state — a guard must be seen to fail (Reviewer B B13; owner rule). Also
  `test_page_service_error_shows_fallback` (a page whose `refresh` hits a raising service shows a visible fallback, no
  stack trace); `test_invalid_control_value_surfaces_error` (an empty cron / unknown module id surfaces the service
  validation error via the notice, no silent no-op).
- [ ] **Step 2:** Run. Expected FAIL (each shown red before its fix).
- [ ] **Step 3:** Implement the minimal production change to satisfy each (fallback rendering, error surfacing,
  per-client binding confirmed).
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5: Full verification** — the v2 check set on the full tree: `ruff check .`, `ruff format --check .`,
  `lint-imports`, `uv run pyright`, `scripts/pip_audit.py`, `pytest -m "not live and not hf and not bench"`. Expected:
  green (the live-web L7 baseline plus the new tests). Confirm `test_live_action_through_dispatch` and every design §6
  named test pass against bound state. No commit.

---

## Self-review notes

- **Spec coverage:** §3 pattern → PB2 (reference) + PB3–PB7 (per page); §3 security invariant → PB2
  `test_no_bound_name_is_an_action`; §3 Taipy-free public → PB1; §4 one `on_navigate` + registry + post-action
  refresh → PB2; §6 every named test mapped (binding guard/security/nav-refresh/action-refresh → PB2; public-no-taipy
  → PB1; isolation/fallback/validation → PB8; per-page → PB3–PB7); §9 commit disposition → Global Constraints.
- **Conversion order (spec §8 open question):** `modules` first (PB2, the reference + the representative action),
  then the rest. Owner to confirm at plan review.
- **Proportion:** the pattern is fully specified once in PB2; PB3–PB7 apply it, each naming only its own vars,
  actions and service calls (derived from the page's current markdown + `_collect`).
