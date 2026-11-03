# v2 Ops pages — reactive state binding (design)

> **Superseded in part by ADR-013 (2026-11-02 UI restructure).** This is a point-in-time snapshot. `active_module` is
> gone — one shared `filter_module` Source selector on the Data sources page now drives both the active toggle and the
> what-to-download editor; `filters.py`/`schedule.py`/`logs.py` are no longer standalone pages. The page-binding
> mechanism (refresh fills bound variables, the Gui-level on_change dispatcher) is unchanged. See
> `docs/adrs/ADR-013-ops-ui-restructure.md`.

**Status:** draft, 2026-10-28. For owner A/B review. Builds on the live-web enforcement addendum
(`2026-10-28-v2-live-web-enforcement-design.md`), which is A/B-approved and implemented (L1–L6).

**Supersedes (same cycle, same uncommitted tree).** This design REPLACES the live-web addendum's operator-render
mechanism — the `Gui(pages={route: page_markdown(...)})` string-dict build (live-web plan L-note "the Gui renders
pages for operators via `page_markdown`"). After this change the operator pages render from module-level `Markdown`
objects registered with `gui.add_page`, and `refresh` fills the display variables. Both artifacts are uncommitted in
the same tree and land in the same single cycle commit, so this supersede is explicit, not a conflict.

**Genuinely unchanged by this design:** the role-ready `Principal`/`Permission` model and `authz.dispatch`; the
Taipy socket wrap (`guard_live_socket`) and its origin lock / WRITE-only connect / per-message revocation; the
Gui-level `gui.on_action` dispatcher (`web/live_actions.py`); the server-side `ValidLogins`/`ConnectionRegistry`;
and the anonymous public page (`web/public.py`, plain server-rendered HTML, no Taipy). This is the operator
presentation layer only.

## 1. Intent

Make the operator pages fully interactive at the v2 gold-standard bar, and make the pattern scale to a dozen or more
pages with no per-page special cases and no framework hacks. After sign-in an operator must see every page with
working controls: a selector or toggle holds a value, a button performs the write through the guarded dispatch, and
the page reflects the result.

## 2. The problem (evidence)

The live-web addendum built the authorization and action seam (role-ready `Principal`/`Permission`, the WRITE-only
Taipy socket, the Gui-level `gui.on_action` dispatcher through `authz.dispatch`). It did **not** make the page
controls functional, because the page variables never bind.

Taipy binds a page's variables from that page's renderer frame (the frame a `Page`/`Markdown` captures at
construction), read at `gui.run`. `ui/shell.py` `build_gui(services)` builds `Gui(pages={route: markdown_string})`
from plain strings in its own body, so each page's renderer frame is `build_gui`'s frame; `build_gui` then returns,
so by the time the server runs that frame is dead. Measured on the built Gui:

- current `build_gui`: `gui._Gui__locals_context.get_all_keys()` == `{page, services, pages, Gui}` — every page
  variable (`active_module`, `draft_active`, `draft_cron`, `draft_enabled`, `draft_360`, …) is **unbound**; a control
  reads and writes nothing.

So the controls render against undefined variables and collect no input. An action with no input (Run now) already
works end to end through the dispatcher; an input-backed action cannot, until the variables bind.

## 3. Decision — per-page modules own their bound state; register via `add_page`

Each `ui/pages/<name>.py` becomes self-contained and owns its own state:

- It declares its interactive (control) variables at module level, typed, with defaults (for example
  `active_module: str = ""`, `draft_active: bool = False`).
- It declares its display variables at module level — the data the page shows (for example the module rows) — also
  with defaults.
- It builds its `taipy.gui.Markdown` (a `Page`) **in the module**. A Taipy `Page` captures its own renderer frame
  (`taipy/gui/page.py` `_get_frame`/`__init__`), so a `Markdown` built at the page module's top level captures that
  module's frame, which never dies — unlike `build_gui`'s dead frame (§2) (Reviewer B B07).
- It declares thin callbacks in the module: a `refresh(state, services)` that repopulates the display variables from
  the services (called by the one Gui-level nav hook — §4), and an optional `on_change` per control for dependent
  updates. Neither holds business logic; the services own that.

**No bound name may equal an action name (security invariant).** Taipy's `__on_action` resolves the action string to
a user function first (`gui.py` `_get_user_function`, ~1470-1529) and only falls back to the Gui-level `on_action`
when none is found. So a page module must never define a callable whose name equals an action name
(`ACTION_NAMES`): such a callable would be invoked directly, skipping `authz.dispatch` — no permission check, no
operator attribution. Page `on_change` callbacks are named for their control (for example `on_pick_module`), never
for an action. §6 names the guard test (Reviewer B B01).

**The public page stays Taipy-free.** `web/public.py` must import no Taipy. The monitoring computation it shares
(`spend_line` and the dependency formatting) moves into a Taipy-free module (for example `ui/pages/_monitoring_data.py`)
that both `web/public.py` and the Taipy `monitoring` page module import; only the Taipy page module builds a
`Markdown`. `test_public_page.py` keeps its `taipy not in sys.modules` assertion (Reviewer B B02).

**Each page declares EXACTLY the variables `_collect` reads for its actions** (`web/live_actions.py` `_collect`,
lines 36–73). Because a page variable is client-wide once bound, two page modules must not declare the same name; so
the one name two pages would share — the filter module id, which `_collect` read as `active_module` for both the
modules page and the filters page — is split: `_collect`'s `on_preview_filter`/`on_save_filter` now read
`filter_module`, owned by the filters page, while `active_module` stays owned by the modules page. This is the one
recorded `_collect` change this cycle (§5), driven by Reviewer A D5-PLAN-01 / Reviewer B PSB-B10. The resulting
per-page contract:

| Page | Actions | Bound control variables (exact `_collect` names) |
|------|---------|---------------------------------------------------|
| modules | `on_module_active`, `on_run_now` | `active_module`, `draft_active` |
| filters | `on_preview_filter`, `on_save_filter` | `filter_module`, `draft_clauses` (a bound clause editor), `draft_360` |
| schedule | `on_save_schedule`, `on_pause_schedule`, `on_resume_schedule`, `on_run_now` | `draft_cron`, `draft_enabled`, `schedule_version` |
| review | `on_accept_recommended`, `on_undo_batch`, `on_decide` | `decide_entity`, `accept_batch_filter`, `undo_batch_id`, `decide_item`, `decide_action`, `decide_reason` |
| runs | `on_run_now` | (none) |
| logs | (read-only filter) | `log_level_filter`, `log_text_filter` |
| settings, monitoring | (display only) | (none) |

A structural test (§6 `test_collect_inputs_declared_per_page`) asserts this contract holds: every `_collect` input is
a declared variable in the page module whose `Markdown` uses that action. `_collect`'s `getattr` defaults otherwise
hide a drifted name (Reviewer B PSB-B12).

`ui/shell.py` `build_gui` registers each page with `gui.add_page(route, page.page)` instead of a dict of strings, and
keeps the root layout and the nav. Writes keep routing through the one Gui-level `gui.on_action` dispatcher
(`web/live_actions.py`) to `authz.dispatch`, which gates on the action's permission and attributes the write to the
operator.

Measured evidence that the pattern binds (throwaway spike, a page module whose `Markdown` is built in-module,
registered with `gui.add_page`): `get_all_keys()` then contains the page module's `spike_module`, `spike_active`,
`on_spike_change`, versus `{page, services, pages, Gui}` for the current string-dict build. The dead-frame problem is
gone.

**Scaling.** A new page that reuses existing actions is one new module — no change to the auth model, the socket
wrap, the dispatcher's gating, or the nav registry. A new page that introduces a NEW action still extends the input
contract (`web/live_actions.py` `_collect`), the `ActionRegistry` (`ui/actions.py`) and `ACTION_NAMES` — three named
places (Reviewer B B05).

## 4. The reactive model

- **Render contract.** Taipy 4.1.1 exposes ONE Gui-level navigation hook, `on_navigate(state, page_name, ...) ->
  page_name` (`gui.py` ~300-313, 2409-2423); a per-page `on_navigate` never fires (Reviewer B B03). `build_gui`
  installs one `on_navigate` that looks the opened page up in a route→`refresh` registry (built in `build_gui` over
  the same `services`, injected by closure), calls that page's `refresh(state, services)` to fill the display
  variables, and returns `page_name` unchanged. This replaces both `ui/shell.py` `page_markdown(services, route) ->
  str` and each page's `page_markdown(services) -> str`: the page is a module-level `Markdown`, and `refresh` fills
  its display variables.
- **Controls.** A selector or toggle binds directly to its state variable. An `on_change` (named for the control, not
  an action — §3 invariant) updates a dependent variable (for example, picking a module loads that module's current
  active flag into the toggle).
- **Actions.** A button's `on_action` reaches the Gui-level `gui.on_action` dispatcher, which reads the connect-time
  principal by socket id, collects the inputs from the state, calls `authz.dispatch`, and sets the visible notice. On
  a successful write the dispatcher then re-runs the active page's `refresh(state, services)` so the page shows the
  new state. A refusal (`Unauthorized`) or a service error changes nothing and shows the notice. The dispatcher's
  authorization and gating logic is unchanged (§5); the one addition is the post-action `refresh` call, which reads
  the active page from the same route→`refresh` registry.

## 5. Non-goals and the exact `live_actions` delta

- No change to `services/*`, `services/authz.py` (the role model and `dispatch`'s permission gating / attribution),
  the socket wrap (`guard_live_socket`, the origin lock, the WRITE-only connect, the per-message revocation), the
  `ValidLogins`/`ConnectionRegistry`, or the auth model. This is the operator presentation layer only.
- `web/live_actions.py`: its authorization and gating are unchanged. Two deltas, both recorded: (1) a post-action
  `refresh(state, services)` call of the active page (§4, Reviewer B B04); (2) `_collect`'s `on_preview_filter`/
  `on_save_filter` read `filter_module` instead of `active_module`, so the filters page owns its own module-id
  variable and does not collide with the modules page's `active_module` (§3; Reviewer A D5-PLAN-01 / Reviewer B
  PSB-B10). No other `_collect` read changes; `_collect` otherwise extends only when a NEW action is introduced (§3).
- The public page (`web/public.py`) stays plain server-rendered HTML with no socket and no Taipy import; only its
  shared monitoring computation moves to a Taipy-free module it imports (§3, Reviewer B B02).
- No new dependency. Taipy 4.1.1 as pinned.

## 6. Testing (named, all in this cycle)

- **Per page, a unit test:** the module-level state variables exist with the documented defaults and types; each
  `on_change` updates the dependent variable; `refresh(state, services)` fills the display variables from a fake
  services; the `page` `Markdown` builds.
- `test_every_page_var_binds` (integration) — after `build_gui` + `add_page`, the built Gui's `get_all_keys()`
  contains every page's interactive variables. This fails on the current dead-frame string-dict build, so it is a
  guard that cannot pass without the fix.
- `test_no_bound_name_is_an_action` (integration, security invariant B01) — after `build_gui`, Taipy's
  `_get_user_function(name)` is not callable for every name in `ACTION_NAMES`, so no bound callable shadows an action
  and skips `authz.dispatch`.
- `test_collect_inputs_declared_per_page` (integration, Reviewer B PSB-B12) — for every `ACTION_NAMES` entry, each
  key `_collect` reads for that action is a declared variable in the page module whose `Markdown` references that
  action. `_collect`'s `getattr` defaults otherwise hide a drifted name, so this guard pins the §3 contract. It is
  written to fail first (seed one page with a mismatched name and watch it red).
- `test_filter_save_writes_picked_module_clauses` (integration, Reviewer B PSB-B10) — on the filters page, pick a
  module and edit the clauses, then `on_save_filter`; `set_module_filter` is called with the PICKED module id and the
  EDITED clauses (not `statsbomb_public` / `[]`). This is the regression the binding defect would have shipped.
- `test_accept_recommended_carries_entity` (integration, Reviewer B PSB-B11) — the review page binds `decide_entity`;
  `on_accept_recommended` dispatches with that entity, not `None`.
- `test_public_page_no_taipy` (unit, B02) — `import lakehouse.web.public` pulls in no `taipy` module
  (the existing `test_public_page.py` assertion, kept after the shared computation moves to the Taipy-free module).
- `test_nav_refresh_fills_display` (integration, B03) — the one Gui-level `on_navigate` calls the opened page's
  `refresh` and fills its display variables from the services, and returns the page name.
- `test_action_then_refresh` (integration, B04) — a successful dispatched action re-runs the active page's `refresh`,
  so the display reflects the new state.
- `test_two_clients_isolated` (integration, B06 / Reviewer A D5-SPEC-02) — two client scopes hold independent draft
  state; one operator's unsaved draft never reaches the other. In this cycle, not a follow-up. Written to fail first:
  it must demonstrate a red (for example against a single shared scope) before the per-client binding makes it green
  (Reviewer B B13; owner rule: a guard must be seen to fail).
- `test_page_service_error_shows_fallback` (integration, §7) — a page whose `refresh` hits a raising service shows a
  visible fallback, never a blank page or a stack trace.
- `test_invalid_control_value_surfaces_error` (integration, §7) — an invalid control value (an empty cron, an unknown
  module id) surfaces the service's validation error through the notice, never a silent no-op.
- `test_live_action_through_dispatch` (existing) stands; it now runs against bound state.
- **Taipy at page-import (B08):** a page module now builds a `Markdown` at import, so importing a page imports Taipy.
  `test_pages.py` and the per-page tests therefore import Taipy (an existing dependency); the pure render/computation
  that must stay Taipy-free lives in the §3 data module, which the public page imports and which these tests can
  exercise without Taipy.

## 7. Review focus (each has a named §6 test)

- A page whose `refresh` reads a service that raises (a missing table on a fresh lake) must show a visible fallback,
  never a blank page or a stack trace (the v2 UI rule: never silently substitute data) →
  `test_page_service_error_shows_fallback`.
- A control value invalid for the service (an empty cron, an unknown module id) must surface the service's
  validation error through the notice, never a silent no-op → `test_invalid_control_value_surfaces_error`.
- Per-client isolation: Taipy copies module-level variables per client, so two operators do not share a draft →
  `test_two_clients_isolated` (this cycle). The security invariant (no bound name equals an action) →
  `test_no_bound_name_is_an_action`.

## 8. Open questions (owner)

1. **Conversion order.** Recommend converting `modules` first as the reference page (the dispatcher's representative
   action), then the rest; each page is an independent task. A periodic auto-poll of display data is out of scope
   this cycle (refresh is on navigate + after an action — §4).

(The earlier open question on the refresh trigger is now decided in §4: one Gui-level `on_navigate` plus a
post-action refresh, as Reviewer B B03/B04 required.)

## 9. Commit disposition

This work is cycle-2 presentation scope. It is uncommitted and lands in the **same single cycle commit** as the
observability-and-ops cycle and the live-web enforcement addendum, behind the owner's explicit approval gate (no
per-task commits; mirrors live-web §9). The two independent A/B implementation reviews run before that commit.
