"""The Taipy Ops shell (spec §19; plan Task 28): the navigation, the root layout and `build_gui`.

The shell copies the v1 patterns (a sidebar of goal-oriented links with Material Symbols icons, a root page that frames
each page's content), but the v2 pages are declarative: each `pages/<name>.py` exposes a `PageConfig` (`CONFIG`) and a
`refresh(state, services)` that fills the bound display variables, and builds `page = Markdown(build_page(CONFIG))` at
its own module scope (the template owns all markup and styling). The UI imports the services layer only (an
import-linter contract), never the lake packages. `build_gui` assembles the pages into a `taipy.gui.Gui`; the Task 29
entrypoint runs it on the Task 27 Flask app, so every page sits behind the sign-in guard.

There is ONE render path: `refresh` fills the display variables, the live page binds them, and `render_for` runs the
same `refresh` against a recording stand-in state for the per-caller scope resolution (spec §6.2 point 3) — no
hand-written per-page string renderer.
"""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

from taipy.gui import Gui, Markdown  # pyright: ignore[reportMissingTypeStubs]

from lakehouse.logging import get_logger
from lakehouse.services.authz import Scope
from lakehouse.services.modules import ModuleService
from lakehouse.services.review import ReviewService
from lakehouse.services.runs import RunService
from lakehouse.services.schedule import ScheduleService
from lakehouse.services.settings import SettingsService

# The shared sidebar filter state is imported into THIS module (the Taipy default module) so the shared sidebar in
# the root page binds it; see lakehouse.ui.filter_state. shell itself does not use these names — they are re-exported
# (listed in __all__) only to place the shared variables (and the tournament on_change) in the default scope.
from lakehouse.ui.filter_state import (
    draft_360,
    draft_clauses,
    draft_is_statsbomb,
    draft_module,
    draft_stale,
    filter_diff,
    filter_module,
    filter_module_ids,
    filter_tournament_lov,
    filter_tournament_map,
    filter_tournaments,
    on_pick_360,
    on_pick_tournaments,
    runs_run_lov,
    runs_selected,
)
from lakehouse.ui.palette import palette_stylekit
from lakehouse.ui.template import (
    BRAND_ICON,
    BRAND_WORD,
    FOOTER_LABEL,
    FOOTER_LEAD,
    FOOTER_URL,
    Button,
    Control,
    PageConfig,
    PageEntry,
    Prose,
    Section,
    Table,
    Text,
    assert_unique_control_vars,
    build_header_from_config,
    build_nav,
    build_sidebar_section,
    iter_controls,
)

if TYPE_CHECKING:
    from lakehouse.services.filters import FilterService

__all__ = [
    "ANON_VIEWABLE_ROUTES",
    "PAGE_MODULES",
    "STYLEKIT",
    "STYLES_CSS",
    "Services",
    "anon_viewable",
    "build_gui",
    "build_root_page",
    "draft_360",
    "draft_clauses",
    "draft_is_statsbomb",
    "draft_module",
    "draft_stale",
    "filter_diff",
    "filter_module",
    "filter_module_ids",
    "filter_tournament_lov",
    "filter_tournament_map",
    "filter_tournaments",
    "ll_on_control_change",
    "nav_markdown",
    "on_pick_360",
    "on_pick_tournaments",
    "page_routes",
    "render_for",
    "runs_run_lov",
    "runs_selected",
]

# styles.css sits next to this module. Taipy serves `css_file` over http, relative to the Gui's root dir, which is
# THIS file's directory (Taipy sets its _root_dir from the frame that builds the Gui, i.e. build_gui below). Pass the
# BASENAME to Gui, never an absolute path: an absolute path becomes a file:// <link> that the browser blocks when the
# app is served over http, so the whole stylesheet (and its @font-face icons) silently fails to load.
STYLES_CSS = Path(__file__).resolve().parent / "styles.css"
# The Taipy Stylekit base theme (amber primary on dark paper), derived from the single Taipy-free palette module so the
# operator pages and the public page cannot drift (spec S06). style_v2.css reads the --color-* tokens from the stylekit.
STYLEKIT: dict[str, str] = palette_stylekit()

_log = get_logger(__name__)

# The route of the page the operator is on, set by the navigation hook. The sidebar filter sections render only for
# their own page (`current_page == "<route>"`), the v1 pattern of placing a page's filters in the sidebar.
current_page: str = ""

# The action-outcome notice the dispatcher writes and the template shows at the foot of every page. It is a shared
# variable, so it lives in the default scope (this module); every page binds the one copy. `on_navigate` clears it
# before each page's refresh, so a notice never follows the operator to the next page (spec S11).
ll_action_notice: str = ""

# Routes an anonymous caller may view (spec §6.3): the spend-per-run and layer-dependency monitoring view carries no
# match identity (§7.2), so it is safe by column scope. Every other page can expose a private match and is
# operator-only this cycle; the log viewer is authenticated-only (§9).
ANON_VIEWABLE_ROUTES: frozenset[str] = frozenset({"monitoring"})


def anon_viewable(route: str) -> bool:
    """Whether an anonymous caller may view a page's content (spec §6.3)."""
    return route in ANON_VIEWABLE_ROUTES


# Shown in place of an operator-only page for an anonymous caller: no service is read, so no private data is rendered.
_SIGNIN_NOTICE: str = "<|part|class_name=ll-signin-notice|\n**Sign in to view this page.**\n|>\n"

# The eight Ops pages, in nav order. The labels are goal-oriented (spec §19.6): what the operator wants to do, not the
# implementation. Each name is the module under `lakehouse.ui.pages`.
# "logs" folded into the Runs run-detail drill-down (audit F6); "filters" ("What to download") folded into the Data
# sources page (audit F7); "schedule" folded into the Settings page as a section (audit F8). None is a top-level page
# any more.
PAGE_MODULES: tuple[str, ...] = ("monitoring", "modules", "runs", "review", "settings")


@dataclass(frozen=True)
class Services:
    """One instance of each Task 25 service; the pages read only these."""

    settings: SettingsService
    modules: ModuleService
    filters: FilterService
    runs: RunService
    review: ReviewService
    schedule: ScheduleService


# `render_for` runs per request, so its per-call scans are memoized (audit O1). Both inputs are build-time constant:
# a page module's `ROUTE` is fixed, and its module-level data variables are templates Taipy copies per connection —
# a page writes its live state, never the module (no `filter_state.<var> = ...` anywhere in the UI), so the seed
# defaults do not change at runtime. The caches are keyed by route and fill once per process.
_PAGE_BY_ROUTE: dict[str, ModuleType] = {}
_DEFAULTS_BY_ROUTE: dict[str, dict[str, Any]] = {}


def _page_map() -> dict[str, ModuleType]:
    """The route -> page module map, built once (each page's `CONFIG`, `refresh` and `page` are read by the shell)."""
    if not _PAGE_BY_ROUTE:
        for name in PAGE_MODULES:
            module = import_module(f"lakehouse.ui.pages.{name}")
            _PAGE_BY_ROUTE[module.ROUTE] = module
    return _PAGE_BY_ROUTE


def _page_module(route: str) -> ModuleType:
    """The page module whose `ROUTE` matches (memoized; the former per-call linear scan over the 8 imports)."""
    module = _page_map().get(route)
    if module is None:
        raise KeyError(f"no Ops page for route {route!r}")
    return module


def page_routes() -> list[str]:
    """The URL path of every Ops page, for the sign-in guard check (every one is behind login)."""
    return [import_module(f"lakehouse.ui.pages.{name}").ROUTE for name in PAGE_MODULES]


def _registry() -> list[PageEntry]:
    """Each page's `PageConfig`, in nav order, for `build_nav` to group into sections."""
    return [
        PageEntry(
            route=import_module(f"lakehouse.ui.pages.{name}").ROUTE,
            config=import_module(f"lakehouse.ui.pages.{name}").CONFIG,
        )
        for name in PAGE_MODULES
    ]


def nav_markdown() -> str:
    """The operator sidebar navigation, grouped into goal-oriented sections from each page's `PageConfig.nav_section`
    (the v1 chrome), plus a Sign-out link. An operator reaches every page; the anonymous public page carries its own
    Sign-in link (`web/public.py`), so the two surfaces never share a nav."""
    # Sign out is a trailing action, not a page: wrap it in an `ll-nav-trailing` part so it sits apart from the last
    # group by the same gap `.ll-nav-header` puts between groups (the public page spaces its Sign in the same way).
    sign_out = (
        "<|part|class_name=ll-nav-trailing|\n"
        '[<span class="material-symbols-outlined">logout</span> Sign out](/signout)\n'
        "|>\n"
    )
    return f"{build_nav(_registry())}\n{sign_out}"


# The brand wordmark + footer come from the one Taipy-free source (template.py), so the operator shell (this Taipy
# markup) and the public page (plain HTML, via build_brand_html / build_footer_html) cannot drift.
_BRAND = f'# <span class="material-symbols-outlined">{BRAND_ICON}</span> {BRAND_WORD}'
_FOOTER = f"{FOOTER_LEAD} [{FOOTER_LABEL}]({FOOTER_URL})."


def _sidebar_sections() -> str:
    """The per-page filter sections placed in the sidebar (the v1 pattern): each page's `PageConfig.sidebar_widgets`,
    wrapped in a `Filters` section that renders only on that page (`current_page == "<route>"`)."""
    sections = [
        build_sidebar_section("Filters", entry.config.sidebar_widgets, condition=f'current_page == "{entry.route}"')
        for entry in _registry()
        if entry.config.sidebar_widgets
    ]
    return "\n".join(sections)


def build_root_page(nav: str, filters: str = "") -> str:
    """The chrome every page sits in: a brand-bordered sidebar with the navigation and the per-page filter sections, a
    brand title row, the page content and a footer. Mirrors the v1 Taipy shell so every Ops page shares the look."""
    return (
        "<|layout|columns=300px 1fr|gap=0.75rem|\n\n"
        f"<|part|class_name=sidebar|\n\n{nav}\n\n{filters}\n\n|>\n\n"
        "<|part|\n\n"
        f"<|part|class_name=ll-title-row|\n<|part|class_name=ll-brand|\n{_BRAND}\n|>\n|>\n\n"
        "<|content|>\n\n"
        f"<|part|class_name=ll-footer|\n{_FOOTER}\n|>\n\n"
        "|>\n\n"
        "|>\n"
    )


# The root page ("/") is built HERE, at module level, so Taipy binds its variables — `current_page` and the shared
# sidebar filter state imported above — from THIS module's globals. Built inside `build_gui` (a function) instead, the
# Markdown's frame would be the function's locals, not shell's globals, and the shared sidebar would silently not bind
# (every converted sub-page likewise defines its `page` at module level for the same reason). `frame=` is passed
# explicitly so Taipy binds from THIS module's frame (its f_locals are shell's globals) rather than guessing a stack
# depth — the guess lands on the wrong frame when the Markdown is built from inside a function.
_ROOT_PAGE = Markdown(build_root_page(nav_markdown(), _sidebar_sections()), frame=inspect.currentframe())


def _page_entries() -> dict[str, object]:
    """The Gui's page map: the root frame plus each page's module-level `Markdown` (bound from its own module)."""
    entries: dict[str, object] = {"/": _ROOT_PAGE}
    for name in PAGE_MODULES:
        module = import_module(f"lakehouse.ui.pages.{name}")
        entries[module.ROUTE] = module.page
    return entries


# This module's frame — its f_locals ARE shell's globals (current_page, ll_action_notice and the imported filter
# state). build_gui hands it to each per-serve Gui via `gui._set_frame`, so Taipy's default variable context (which
# `gui.run` rebuilds from the frame's f_locals) is shell's globals and the shared sidebar binds. Capturing it here, at
# module scope, is the only way to get a frame whose locals are the module globals — a Gui built inside build_gui (a
# function) would otherwise take that function's locals, which lack the shared variables.
_SHELL_FRAME = inspect.currentframe()


class _RecordingState:
    """A stand-in Taipy state for `render_for`: it runs a page's `refresh` off-connection. Attribute reads return the
    seeded module default (or "" when unseeded), and writes are recorded, so `render_for` can collect the resolved
    display variables without a live Gui. Seeded from the page and shared module defaults (spec C04)."""

    def __init__(self, defaults: dict[str, Any]) -> None:
        object.__setattr__(self, "_values", dict(defaults))

    def __getattr__(self, name: str) -> Any:
        return object.__getattribute__(self, "_values").get(name, "")

    def __setattr__(self, name: str, value: Any) -> None:
        object.__getattribute__(self, "_values")[name] = value


def _module_defaults(module: ModuleType) -> dict[str, Any]:
    """The module-level data variables (names, not functions/types/dunders) a page or the shared state declares."""
    return {
        name: value
        for name, value in vars(module).items()
        if not name.startswith("_") and not callable(value) and not isinstance(value, type)
    }


def _recording_defaults(module: ModuleType) -> dict[str, Any]:
    """The recording state's seed: the shared filter state and `ll_action_notice`, then the page's own defaults. The
    two `vars()` scans are memoized per route (audit O1); a fresh copy is returned so the caller cannot mutate the
    cache."""
    cached = _DEFAULTS_BY_ROUTE.get(module.ROUTE)
    if cached is None:
        cached = _module_defaults(import_module("lakehouse.ui.filter_state"))
        cached["ll_action_notice"] = ""
        cached.update(_module_defaults(module))
        _DEFAULTS_BY_ROUTE[module.ROUTE] = cached
    return dict(cached)


def _resolved_text(cfg: PageConfig, state: _RecordingState) -> str:
    """The resolved page content as text, for the per-caller scope proof: the header, the fixed prose, the filled
    text and table cells, the control labels and values, and the button labels — everything a reader would see."""
    out: list[str] = [build_header_from_config(cfg)]

    def walk(elements: tuple[Any, ...]) -> None:
        for element in elements:
            if isinstance(element, Prose):
                out.append(element.markdown)
            elif isinstance(element, Text):
                out.append(str(getattr(state, element.var, "")))
            elif isinstance(element, Table):
                columns = getattr(state, element.var, {})
                if isinstance(columns, dict):
                    for cells in cast("dict[str, list[str]]", columns).values():
                        out.extend(str(cell) for cell in cells)
            elif isinstance(element, Button):
                out.append(element.label)
            elif isinstance(element, Control):
                out.append(element.label)
                out.append(str(getattr(state, element.var, "")))
            elif isinstance(element, Section):
                out.append(str(getattr(state, element.subtitle_var, "")) if element.subtitle_var else element.subtitle)
                walk(element.items)

    walk(cfg.body)
    return "\n".join(out)


def render_for(route: str, services: Services, scope: Scope) -> str:
    """The page content for one caller, resolved at request time (spec §6.2 point 3) — not once at build. A public
    caller is refused an operator-only page (the sign-in notice, no service read), and sees an anon-viewable page in
    full; an operator sees every page. A page cannot widen its own scope: the scope comes from the authenticated
    connection (Task 7), never from the page. The one render path: run the page's `refresh` against a recording state,
    then resolve its display variables to text (so this exercises the same services reads the browser does)."""
    if scope == "public" and not anon_viewable(route):
        return _SIGNIN_NOTICE
    module = _page_module(route)
    state = _RecordingState(_recording_defaults(module))
    module.refresh(state, services)
    return _resolved_text(module.CONFIG, state)


def _refresher(module: ModuleType, services: Services) -> Callable[[Any], None]:
    """A `refresh(state)` bound over `services` for one page module (page-binding addendum §4)."""

    def refresh(state: Any) -> None:
        module.refresh(state, services)

    return refresh


def _on_change(by_var: dict[str, Callable[[Any, Services, Any], None]], services: Services) -> Callable[..., None]:
    """The one change dispatcher (spec S03). `ll_on_control_change` forwards each operator change of a marked control
    to it as `(state, var_name, value)`; it dispatches by `var_name` to the control's services-backed handler
    `(state, services, value)`, so a handler can take `services`.

    Failure rule (spec S13): Taipy catches a handler exception and only logs it, so a failed handler would leave the
    stale state in place with no notice (V2UI-04 on the error path). So this wrapper, on an exception, writes the
    failure into `ll_action_notice` and sets `draft_stale`, so the Save guard refuses the save until a reload
    succeeds. A successful handler (e.g. the filters Source reload) clears `draft_stale` itself."""

    def on_change(state: Any, var_name: str, value: Any) -> None:
        handler = by_var.get(var_name)
        if handler is None:
            return
        try:
            handler(state, services, value)
        except Exception as exc:  # noqa: BLE001 - a failed change shows a notice + marks the draft stale, never silent
            with contextlib.suppress(Exception):
                state.ll_action_notice = f"Could not apply the change: {exc}"
            with contextlib.suppress(Exception):
                state.draft_stale = True  # set independently (B-19): a notice-write failure must not skip the guard

    return on_change


# The Gui attribute that holds the dispatcher `_on_change` built for that Gui's services.
_DISPATCHER_ATTR = "_lakehouse_on_change"


def ll_on_control_change(state: Any, var_name: str, value: Any) -> None:
    """The change callback Taipy calls when the OPERATOR changes a marked control (the template emits
    `on_change=ll_on_control_change` on it). Taipy 4.1.1 `Gui._call_on_change` resolves a callback BY NAME
    (`_get_user_function`: the state scope, the page locals, then the default module — THIS module) and never reads
    the `gui.on_change` attribute, so a dispatcher set only as that attribute never ran live: a Source, Tournaments or
    Run pick changed the selector but reloaded nothing. A control-level callback is used, not a global `on_change`:
    Taipy calls a global `on_change` for EVERY state write, server-side refresh writes included, which would re-run a
    handler each time a refresh fills its variable. No module may define a global `on_change` (a guard test checks).
    This forwards to the dispatcher `build_gui` stored on the state's own Gui, so each Gui keeps its own services."""
    dispatcher = getattr(state.get_gui(), _DISPATCHER_ATTR, None)
    if dispatcher is None:
        _log.warning("no change dispatcher on this Gui; the change is not handled", extra={"var": var_name})
        return
    dispatcher(state, var_name, value)


def _change_handlers(services: Services) -> dict[str, Callable[[Any, Services, Any], None]]:
    """The `on_change` dispatch map: each control's bound variable to its services-backed handler, across all pages.
    A variable that two controls bind is rejected (the by-variable dispatch needs unique names — spec C06)."""
    modules = [import_module(f"lakehouse.ui.pages.{name}") for name in PAGE_MODULES]
    assert_unique_control_vars([module.CONFIG for module in modules])  # spec C06, at build time
    by_var: dict[str, Callable[[Any, Services, Any], None]] = {}
    for module in modules:
        for control in iter_controls(module.CONFIG):
            if control.on_change:
                by_var[control.var] = getattr(module, control.on_change)
    return by_var


def _on_navigate(by_route: dict[str, Callable[[Any], None]]) -> Callable[..., str]:
    """The one Gui-level navigation hook: clear the shared notice (spec S11), refresh the opened page's display,
    return the page name unchanged (§4)."""

    def on_navigate(state: Any, page_name: str, *args: Any) -> str:
        # Fail-open by design (review V2UI-IMPL-09): current_page gates the sidebar filter display AND keys the
        # post-action refresh (web/live_actions.py selects the refresher by current_page). A state write can
        # transiently fail during the connection handshake; raising would blank the whole page, so the write is
        # swallowed but LOGGED (never silent — ADR-002). A stale current_page at worst re-gates the sidebar or
        # refreshes the wrong page's display after an action; it never corrupts a saved value.
        try:
            state.current_page = page_name  # the page the operator is on: sidebar gating + post-action refresh key
        except Exception as exc:  # noqa: BLE001 - fail-open navigation (see above); logged, not silent
            _log.warning("could not set current_page on navigate", extra={"page": page_name, "error": str(exc)})
        with contextlib.suppress(Exception):
            state.ll_action_notice = ""  # cosmetic: clear a stale notice so it does not follow the operator (S11)
        refresh = by_route.get(page_name)
        if refresh is not None:
            try:
                refresh(state)
            except Exception as exc:  # noqa: BLE001 - a page that cannot load shows a notice, never a blank page
                with contextlib.suppress(Exception):
                    state.ll_action_notice = f"This page could not load: {exc}"
        return page_name

    return on_navigate


def _on_init(by_route: dict[str, Callable[[Any], None]]) -> Callable[[Any], None]:
    """Refresh the landing page once the client is connected. Taipy shows the first registered page at `/`, but the
    state writes made inside the initial `on_navigate` (which runs during the connection handshake) do not reach the
    client, so the landing page would render empty until the operator navigates away and back. `on_init` runs after
    the client is ready, so the same refresh now reaches it; a failure is shown in the notice, not discarded."""
    default = next(iter(by_route), None)  # PAGE_MODULES[0] — the page shown at "/"

    def on_init(state: Any) -> None:
        if default is not None:
            try:
                by_route[default](state)
            except Exception as exc:  # noqa: BLE001 - a landing page that cannot load shows a notice, never blank
                with contextlib.suppress(Exception):
                    state.ll_action_notice = f"This page could not load: {exc}"

    return on_init


def build_gui(services: Services) -> tuple[object, dict[str, Callable[[Any], None]]]:
    """Assemble a FRESH Taipy Gui for one set of services and return it with the route→refresh map. A fresh Gui per
    call means each server gets its own Flask blueprints (test isolation). The pages are module-level `Markdown`
    objects (each binds from its own module); `gui._set_frame(_SHELL_FRAME)` makes the Gui's default variable context
    shell's globals so the shared sidebar binds (see `_SHELL_FRAME`). Each page's `refresh` is bound over `services`
    and keyed by ROUTE, so a successful action re-fills the page the operator is ON (`current_page`) — not a per-action
    map, because an action like `on_run_now` is declared on several pages and an action-keyed map would collide. One
    Gui-level `on_change` dispatcher carries `services` to the control handlers (spec S03)."""
    gui = Gui(pages=_page_entries(), css_file=STYLES_CSS.name)
    gui._set_frame(_SHELL_FRAME)  # pyright: ignore - Taipy private method (no stubs); rebinds default context + _root_dir
    by_route: dict[str, Callable[[Any], None]] = {}
    for name in PAGE_MODULES:
        module = import_module(f"lakehouse.ui.pages.{name}")
        by_route[module.ROUTE] = _refresher(module, services)
    gui.on_navigate = _on_navigate(by_route)
    gui.on_init = _on_init(by_route)  # fill the landing page once the client is connected (see _on_init)
    dispatcher = _on_change(_change_handlers(services), services)  # services-backed control handlers (S03)
    # Taipy calls `ll_on_control_change` (the marked controls' callback, by name), which forwards to this dispatcher.
    # `gui.on_change` is NOT read by Taipy's change path; it is kept as the direct entry the integration tests drive.
    setattr(gui, _DISPATCHER_ATTR, dispatcher)
    gui.on_change = dispatcher
    return gui, by_route
