"""The bound-page framework end to end (page-binding plan PB2): after `build_gui` + `add_page`, the page variables
bind (no dead frame), no bound name shadows an action, the one Gui-level `on_navigate` refreshes the opened page, and
a dispatched action refreshes the active page. One built Gui is shared (serve is ~18s)."""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from lakekit import LakeKit, lake_roots
from runkit import active, save_settings

from lakehouse.services.account import AccountService
from lakehouse.services.authz import Principal, Role
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake
from lakehouse.ui import actions as actions_mod
from lakehouse.ui import shell
from lakehouse.ui.pages import modules as modules_page
from lakehouse.web import live_actions

pytestmark = pytest.mark.integration

SECRET = "a-long-enough-session-secret-value"  # noqa: S105 - a test value


@pytest.fixture(scope="module")
def built() -> Iterator[Any]:
    base = Path(tempfile.mkdtemp(prefix="pbind-"))
    roots = lake_roots(base)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=base / "boot.json")
    storage = LocalStorage(roots)
    kit = LakeKit(base=base, roots=roots, storage=storage)
    save_settings(storage, [active("idsse_public")])
    AccountService(storage).create("ops", "correct horse battery staple")
    from lakehouse.web import main

    env = {
        "LAKEHOUSE_LAKE_ROOT": str(roots.lake.local_path),
        "LAKEHOUSE_RESTRICTED_ROOT": str(roots.restricted.local_path),
        "LAKEHOUSE_SESSION_SECRET": SECRET,
    }
    app, gui = main.serve(env, run=False)
    yield SimpleNamespace(app=app, gui=gui, kit=kit)


def test_every_page_var_binds(built: Any) -> None:
    keys = built.gui._Gui__locals_context.get_all_keys()
    for var in ("filter_module", "draft_active", "filter_module_ids", "module_rows"):
        assert var in keys, f"{var} not bound (dead-frame regression): {sorted(keys)}"


def test_no_bound_name_is_an_action() -> None:
    """No page module defines a callable named like an action — the root guarantee that Taipy's `__on_action` finds
    no user function and falls through to the Gui-level `gui.on_action` (and thus `authz.dispatch`). Checked at the
    source (context-free), which is stronger than probing `_get_user_function` inside one client scope."""
    from importlib import import_module

    from lakehouse.ui.shell import PAGE_MODULES

    for page_name in PAGE_MODULES:
        module = import_module(f"lakehouse.ui.pages.{page_name}")
        for action in actions_mod.ACTION_NAMES:
            assert not callable(getattr(module, action, None)), (
                f"{page_name} defines a callable named like action {action} — it would skip authz.dispatch"
            )


def test_collect_inputs_declared_per_page() -> None:
    """For every page, each variable `_collect` reads for that page's actions (derived via `action_names(CONFIG)`) is
    a declared module-level attribute of that page module. `_collect`'s getattr defaults otherwise hide a drifted
    name."""
    from importlib import import_module

    from lakehouse.ui import filter_state  # the shared sidebar scope (declares filter_module/draft_clauses/draft_360)

    class _Tracker:
        def __init__(self) -> None:
            self.seen: set[str] = set()

        def __getattr__(self, name: str) -> int:
            self.seen.add(name)
            return 0  # survives bool()/int()/str() in _collect

    from lakehouse.ui.template import action_names

    checked = 0
    for page_name in ("modules", "review", "runs", "settings", "monitoring"):
        module = import_module(f"lakehouse.ui.pages.{page_name}")
        for action in action_names(module.CONFIG):
            tracker = _Tracker()
            live_actions._collect(action, tracker)  # pyright: ignore[reportPrivateUsage] - the guard probes the private contract
            # An input is declared on the page module, or in the shared sidebar scope (filter_state) the shell binds.
            missing = {name for name in tracker.seen if not (hasattr(module, name) or hasattr(filter_state, name))}
            assert not missing, f"{page_name}.{action} reads undeclared vars {missing}"
            checked += 1
    assert checked >= 2  # at least the modules page's two actions are guarded


def test_nav_refresh_fills_display(built: Any) -> None:
    state = SimpleNamespace()
    result = built.gui.on_navigate(state, "modules")
    assert result == "modules"  # the hook returns the page name unchanged
    assert state.module_rows.get("Source")  # refresh filled the native table (was unset)
    assert state.filter_module_ids  # and the Source selector options


def test_action_then_refresh(built: Any) -> None:
    from flask import request

    connections = built.app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-op", "sid-pb", Principal("ops", frozenset({Role.OPERATOR})))
    # current_page drives the post-action refresh: the dispatch refreshes the ACTIVE page, not a per-action map
    state = SimpleNamespace(current_page="modules", filter_module="idsse_public", draft_active=False, module_rows={})
    with built.app.test_request_context():
        request.sid = "sid-pb"  # type: ignore[attr-defined]
        built.gui.on_action(state, "apply", {"action": "on_module_active"})
    assert state.module_rows.get("Source")  # the post-action refresh re-ran (was {})
    assert str(state.ll_action_notice).startswith("Done")


def test_run_now_refreshes_the_active_page_not_a_colliding_action(built: Any) -> None:
    """on_run_now is declared on runs, modules and schedule. The post-action refresh must re-fill the page the
    operator is ON (current_page), not a page chosen by an action-keyed map (review B-16). Seen to fail: key the
    refresh by action name and `on_run_now` from Runs refreshes Schedule instead, leaving runs_rows unset."""
    from flask import request

    connections = built.app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-rn", "sid-rn", Principal("ops", frozenset({Role.OPERATOR})))
    state = SimpleNamespace(current_page="runs", runs_rows={}, schedule_rows={})
    with built.app.test_request_context():
        request.sid = "sid-rn"  # type: ignore[attr-defined]
        built.gui.on_action(state, "run", {"action": "on_run_now"})
    assert "Started" in state.runs_rows  # the Runs page (current_page) was refreshed — its native table filled
    assert state.schedule_rows == {}  # the Schedule page was NOT refreshed (no action-key collision)


def test_css_file_is_a_basename_on_the_gui(built: Any) -> None:
    """build_gui must pass the BASENAME to Gui (fact 1 / review B-M2): an absolute path becomes a blocked file://
    link and the whole stylesheet fails. Seen to fail: pass the absolute STYLES_CSS and this reads the full path."""
    assert built.gui._Gui__css_file == "styles.css"  # pyright: ignore[reportAttributeAccessIssue] - Taipy private


def test_build_gui_rejects_duplicate_control_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """build_gui asserts control variable names are unique across pages (spec C06 / review B-M3). Seen to fail: drop
    assert_unique_control_vars from _change_handlers and build_gui accepts a colliding variable. build_gui reaches the
    check (in _change_handlers) before it ever calls `services`, so a dummy services object is enough."""
    from dataclasses import replace

    from lakehouse.ui.pages import modules as modules_mod
    from lakehouse.ui.template import Control, Section

    # give the modules page a control binding a variable the review page already uses (decide_item)
    clash = Section("Clash", (Control("input", "decide_item"),))
    monkeypatch.setattr(modules_mod, "CONFIG", replace(modules_mod.CONFIG, body=(*modules_mod.CONFIG.body, clash)))
    with pytest.raises(ValueError, match="duplicate control variable 'decide_item'"):
        shell.build_gui(cast("Any", object()))


def test_filter_save_writes_picked_module_clauses(built: Any) -> None:
    """The filter Save writes the PICKED module's EDITED clauses — not statsbomb_public / [] (Reviewer B PSB-B10)."""
    from flask import request

    from lakehouse.config.model import CompetitionClause
    from lakehouse.wiring import build_services

    connections = built.app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-f", "sid-f", Principal("ops", frozenset({Role.OPERATOR})))
    clause = CompetitionClause(competition_id="43", seasons=["281"])
    state = SimpleNamespace(filter_module="statsbomb_public", draft_clauses=[clause], draft_360=False)
    with built.app.test_request_context():
        request.sid = "sid-f"  # type: ignore[attr-defined]
        built.gui.on_action(state, "save", {"action": "on_save_filter"})
    assert str(state.ll_action_notice).startswith("Done")
    services = build_services(built.kit.storage, ResolvedRoots(roots=built.kit.roots, source="bootstrap"), env={})
    settings, _ = services.settings.load()
    module = next(m for m in settings.modules if m.module_id == "statsbomb_public")
    assert module.filter == [clause]  # the picked module got the edited clauses


def test_accept_recommended_carries_entity() -> None:
    """`_collect` carries the review page's bound `decide_entity` into the action's `entity` — not None (Reviewer B
    PSB-B11, which flagged the omitted variable)."""
    state = SimpleNamespace(decide_entity="player", accept_batch_filter="")
    kwargs = live_actions._collect("on_accept_recommended", state)  # pyright: ignore[reportPrivateUsage]
    assert kwargs["entity"] == "player"


def test_two_clients_isolated() -> None:
    """Per-client state lives on the `state` object, never on the page module's globals — so two operators' drafts do
    not collide (Reviewer B B06/B13). Seen to fail: have any refresh/on_change write to a module global and the
    module-globals-unchanged assertion goes red."""
    from datetime import UTC, datetime

    from lakehouse.sources.base import PreviewResult

    def _mod(module_id: str, *, active: bool) -> Any:
        return SimpleNamespace(
            module_id=module_id,
            display_name=module_id,
            active=active,
            filter_summary="all",
            match_count=1,
            last_run=None,
            token_status=None,
            warning=None,
        )

    class _M:
        def list_modules(self) -> list[Any]:
            return [_mod("idsse_public", active=True), _mod("statsbomb_public", active=False)]

    class _F:
        def preview(self, module_id: str) -> PreviewResult:
            return PreviewResult(tournaments=[], fetched_at=datetime(2026, 10, 28, tzinfo=UTC))

    class _S:
        def load(self) -> tuple[Any, int]:
            return (SimpleNamespace(modules=[]), 1)

    services = cast("Any", SimpleNamespace(modules=_M(), filters=_F(), settings=_S()))
    s1 = SimpleNamespace(filter_module="idsse_public")
    s2 = SimpleNamespace(filter_module="statsbomb_public")
    modules_page.on_pick_source(s1, services, "idsse_public")  # the shared (state, services, value) handler shape
    modules_page.on_pick_source(s2, services, "statsbomb_public")
    assert s1.draft_active is True and s2.draft_active is False  # independent per state
    # the module globals are never mutated (refresh/on_change write to `state`, which Taipy copies per client)
    assert modules_page.draft_active is False and modules_page.module_rows == {}


def test_page_service_error_shows_fallback() -> None:
    """A page whose `refresh` raises shows a visible notice, never a blank page or a stack trace (§7). Seen to fail:
    drop the try/except in `shell._on_navigate` and this raises instead of setting the notice."""

    def _boom(_state: Any) -> None:
        raise RuntimeError("the lake is unreachable")

    nav = shell._on_navigate({"modules": _boom})  # pyright: ignore[reportPrivateUsage]
    state = SimpleNamespace()
    result = nav(state, "modules")  # must not raise
    assert result == "modules"
    assert "could not load" in str(state.ll_action_notice).lower()


def test_on_navigate_clears_stale_notice() -> None:
    """The shared notice is cleared on navigation (spec S11), so a "Saved…" or failure notice does not follow the
    operator to the next page."""
    nav = shell._on_navigate({"modules": lambda _s: None})  # pyright: ignore[reportPrivateUsage]
    state = SimpleNamespace(ll_action_notice="Saved earlier")
    nav(state, "modules")
    assert state.ll_action_notice == ""


def test_on_init_refreshes_landing_and_shows_failure() -> None:
    """on_init fills the landing page after connect (fact 9); a refresh failure is shown, not discarded."""
    done: list[int] = []
    shell._on_init({"monitoring": lambda _s: done.append(1)})(SimpleNamespace())  # pyright: ignore[reportPrivateUsage]
    assert done == [1]

    def _boom(_s: Any) -> None:
        raise RuntimeError("the lake is unreachable")

    state = SimpleNamespace()
    shell._on_init({"monitoring": _boom})(state)  # pyright: ignore[reportPrivateUsage] - must not raise
    assert "could not load" in str(state.ll_action_notice).lower()


def test_source_reload_failure_refuses_save_end_to_end(built: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """S13 end-to-end (review 17b): a Source pick whose reload FAILS goes through the real built gui.on_change →
    on_pick_source → the failing refresh → the dispatcher sets draft_stale → the built gui.on_action refuses Save.
    Not a unit stub + hand-seeded flag. The reload is forced to fail by monkeypatching filters.refresh."""
    from flask import request

    from lakehouse.ui.pages import filters as filters_mod

    def _boom(_state: Any, _services: Any) -> None:
        raise RuntimeError("the source would not reload")

    monkeypatch.setattr(filters_mod, "refresh", _boom)  # on_pick_source calls the module-level refresh at call time
    connections = built.app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-s13e", "sid-s13e", Principal("ops", frozenset({Role.OPERATOR})))
    state = SimpleNamespace(
        current_page="modules", filter_module="idsse_public", draft_clauses=[], draft_360=False, draft_stale=False
    )
    with built.app.test_request_context():
        request.sid = "sid-s13e"  # type: ignore[attr-defined]
        built.gui.on_change(state, "filter_module", "statsbomb_public")  # the real dispatcher → on_pick_source → boom
        assert state.draft_stale is True  # the dispatcher marked the draft stale on the failed reload
        built.gui.on_action(state, "save", {"action": "on_save_filter"})
    assert "stale" in str(state.ll_action_notice).lower()  # Save refused through the live collect, not a stub


def test_on_change_failure_sets_notice_and_marks_stale() -> None:
    """The Gui-level on_change dispatcher does not swallow a handler failure (spec S13): on an exception it writes the
    notice and sets `draft_stale`, so the Save guard can refuse a stale save. Seen to fail: drop the except branch in
    `shell._on_change` and the notice/stale flag go unset."""

    def _boom(_state: Any, _services: Any, _value: Any) -> None:
        raise RuntimeError("the source would not reload")

    on_change = shell._on_change({"filter_module": _boom}, cast("Any", object()))  # pyright: ignore[reportPrivateUsage]
    state = SimpleNamespace()
    on_change(state, "filter_module", "statsbomb_public")  # must not raise
    assert "could not apply the change" in str(state.ll_action_notice).lower()
    assert state.draft_stale is True  # the Save guard reads this


def test_save_refused_when_draft_stale(built: Any) -> None:
    """The in-cycle Save guard (spec S13): with `draft_stale` set (a failed Source reload), on_save_filter is refused
    through the live_actions dispatch — it never writes the stale clauses (V2UI-04 on the error path)."""
    from flask import request

    from lakehouse.config.model import CompetitionClause

    connections = built.app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-s13", "sid-s13", Principal("ops", frozenset({Role.OPERATOR})))
    clause = CompetitionClause(competition_id="43", seasons=["281"])
    state = SimpleNamespace(filter_module="statsbomb_public", draft_clauses=[clause], draft_360=False, draft_stale=True)
    with built.app.test_request_context():
        request.sid = "sid-s13"  # type: ignore[attr-defined]
        built.gui.on_action(state, "save", {"action": "on_save_filter"})
    notice = str(state.ll_action_notice).lower()
    assert "stale" in notice and not notice.startswith("done")  # refused, not written


def test_source_change_reloads_through_gui_on_change(built: Any) -> None:
    """Firing the real Gui-level gui.on_change for the Source variable (filter_module) runs on_pick_source and
    reloads the gate for the new module (review B-M1). Seen to fail: drop on_change="on_pick_source" from the filters
    Source control and filter_module is not in the dispatch map, so the gate never updates."""
    state = SimpleNamespace(filter_module="statsbomb_public", draft_is_statsbomb=True, draft_stale=False)
    built.gui.on_change(state, "filter_module", "wyscout_public")  # the real installed dispatcher
    assert state.draft_is_statsbomb is False  # on_pick_source reloaded the §29.13 gate for the non-StatsBomb module


def test_taipy_resolves_the_control_change_callback_by_name(built: Any) -> None:
    """Taipy 4.1.1 `_call_on_change` finds a change callback BY NAME (`_get_user_function`), never via the
    `gui.on_change` attribute — so a dispatcher set only as that attribute never ran live (a Source, Tournaments or Run
    pick changed the selector and reloaded nothing). This drives Taipy's own lookup, which the direct
    `gui.on_change(...)` calls above bypass. The marked controls name `ll_on_control_change`, which must resolve; a
    global `on_change` must NOT resolve, because Taipy calls it for every state write (server-side refresh writes too)
    and would re-run a handler each time a refresh fills its variable. Seen to fail: remove `ll_on_control_change`
    and the lookup returns the bare name, not a callable; define a module-level `on_change` in shell and the second
    assertion fails."""
    with built.app.test_request_context():
        resolved = built.gui._get_user_function("ll_on_control_change")  # pyright: ignore[reportAttributeAccessIssue]
        catch_all = built.gui._get_user_function("on_change")  # pyright: ignore[reportAttributeAccessIssue]
    assert resolved is shell.ll_on_control_change
    assert not callable(catch_all)  # no global on_change: a server-side write never runs a control handler
    forwarded: list[tuple[str, Any]] = []

    def _dispatcher(_state: Any, var: str, value: Any) -> None:
        forwarded.append((var, value))

    gui = SimpleNamespace(_lakehouse_on_change=_dispatcher)
    shell.ll_on_control_change(SimpleNamespace(get_gui=lambda: gui), "filter_module", "idsse_public")
    assert forwarded == [("filter_module", "idsse_public")]  # it forwards to the state's own Gui's dispatcher


def test_every_marked_control_names_the_shared_callback() -> None:
    """Every control with a handler emits `on_change=ll_on_control_change` in its page markup, and a control without
    one emits none."""
    from importlib import import_module

    from lakehouse.ui.shell import PAGE_MODULES
    from lakehouse.ui.template import CHANGE_CALLBACK, build_page, iter_controls

    markup = shell.build_root_page(shell.nav_markdown(), shell._sidebar_sections())  # pyright: ignore[reportPrivateUsage]
    modules = [import_module(f"lakehouse.ui.pages.{page_name}") for page_name in PAGE_MODULES]
    markup += "".join(build_page(module.CONFIG) for module in modules)
    for module in modules:
        for control in iter_controls(module.CONFIG):
            start = markup.index(f"<|{{{control.var}}}|")
            tag = markup[start : markup.index("|>", start) + 2]
            assert (f"|on_change={CHANGE_CALLBACK}|" in tag) == bool(control.on_change), tag


def test_sidebar_vars_are_shared() -> None:
    """Every variable a sidebar control binds (its value and its lov) is declared in the shared default scope
    (`filter_state`, imported into shell): the sidebar is part of the root page, which binds only that scope. Seen to
    fail: the Runs Run selector's vars lived in pages/runs.py, and the live selector showed no options and no value."""
    from importlib import import_module

    from lakehouse.ui import filter_state
    from lakehouse.ui.shell import PAGE_MODULES

    for page_name in PAGE_MODULES:
        module = import_module(f"lakehouse.ui.pages.{page_name}")
        for control in module.CONFIG.sidebar_widgets:
            for var in filter(None, (control.var, control.lov, control.condition)):
                assert hasattr(filter_state, var) and hasattr(shell, var), f"{page_name} sidebar var {var} not shared"
                assert not hasattr(module, var), f"{page_name} shadows the shared sidebar var {var}"


def test_invalid_control_value_surfaces_error(built: Any) -> None:
    """An invalid control value (an unknown decide item) surfaces the service error through the notice, never a silent
    no-op (§7)."""
    from flask import request

    connections = built.app.config["LAKEHOUSE_CONNECTIONS"]
    connections.bind("login-d", "sid-d", Principal("ops", frozenset({Role.OPERATOR})))
    state = SimpleNamespace(decide_item="ghost-item", decide_action="confirm", decide_reason="x")
    with built.app.test_request_context():
        request.sid = "sid-d"  # type: ignore[attr-defined]
        built.gui.on_action(state, "decide", {"action": "on_decide"})
    assert "did not apply" in str(state.ll_action_notice).lower()
