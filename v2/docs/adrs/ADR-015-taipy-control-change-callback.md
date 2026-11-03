# ADR-015: One named control-level `on_change` callback, not the `gui.on_change` attribute

## Status

Accepted (2026-11-03). Decided after the operator filters never fired live — a Source, Tournaments or Run pick changed
the selector but reloaded nothing. Root-caused in Taipy 4.1.1. Uncommitted, pending the owner's commit approval.
Amends ADR-008 (Taipy for the Ops UI) and ADR-013 (the page interface and the drill-down pattern), which both assumed
a single Gui-level `gui.on_change` dispatcher.

## Context

- The page template marks a control with a services-backed handler (`Control.on_change`), and `build_gui` installs one
  dispatcher over the services. ADR-013's drill-down pattern and the page-state-binding design both set
  `gui.on_change = <dispatcher>` and emitted no control-level `on_change=` in the markup.
- In Taipy 4.1.1, `Gui._call_on_change` resolves the change callback **by name** (`_get_user_function`: the state
  scope, the page locals, then the default module) and never reads the `gui.on_change` attribute. With only that
  attribute set, no control handler ran live. The integration tests called `gui.on_change(...)` directly, so they
  passed and hid the defect.
- A module-level global `on_change` function is also wrong: Taipy calls it for **every** state write, including the
  server-side writes a `refresh` makes, so it would re-run a control's handler each time a refresh fills that
  control's variable.
- Separately, a sidebar control binds only the default (shell) module, so every variable a sidebar control binds (its
  value and its lov) must be declared in the shared scope (`ui/filter_state`), not in the page module — the Runs Run
  selector declared them in `pages/runs.py` and showed no options and no value.

## Decision

1. **The template emits `on_change=ll_on_control_change` on each marked control** — the one shared callback name, never
   the handler's own name (Taipy would call the handler as `(state, var, value)` with no services).
2. **`shell.ll_on_control_change` lives in the default module** (where Taipy's by-name lookup finds it) and forwards
   `(state, var_name, value)` to the per-Gui dispatcher stored on `state.get_gui()`. Each Gui keeps its own services.
3. **No module may define a global `on_change`.** Taipy would call it for server-side writes too.
4. **Every sidebar control's value and lov variables are declared in `ui/filter_state`** (the shared default scope),
   never in a page module.
5. **Both rules are test-enforced.** `test_taipy_resolves_the_control_change_callback_by_name` drives Taipy's own
   lookup (asserts the named callback resolves and that no global `on_change` resolves);
   `test_every_marked_control_names_the_shared_callback` and `test_sidebar_vars_are_shared` cover the markup and the
   shared-scope rule.

## Consequences

- Operator control changes (Source, Tournaments, Run, the schedule and filter inputs) now run their services-backed
  handlers live; server-side refresh writes do not re-trigger them. Proven live: a pick fires exactly one dispatch.
- The rest of ADR-013's S03 mechanism is unchanged — one dispatcher over the services, the by-variable map, the
  `(state, services, value)` handler shape, unique variable names (C06) and the S13 failure rule.
- The page-state-binding and ops-UI-template design docs' "set `gui.on_change`, emit no control-level `on_change`"
  statements are superseded by this ADR; the UI template design doc carries the amendment inline.
- New sidebar controls must declare their variables in `ui/filter_state`; a page-local declaration binds to nothing.
