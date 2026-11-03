"""The Ops web entrypoint (spec §19, §22; plan Task 29): `python -m lakehouse.web` on the Space.

It resolves the lake roots from the environment, opens the storage, builds the Flask app (sign-in, sessions, CSRF)
and the Taipy Ops pages, and runs the Gui on that app. The heavy composition lives in `lakehouse.wiring`, the
composition root, so this module imports no lake package directly and the import-linter contract holds.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any, Literal, cast

from flask import Flask

from lakehouse.config.model import Settings
from lakehouse.web.app import create_app
from lakehouse.web.live_actions import install_live_actions
from lakehouse.web.sessions import ConnectionRegistry, ValidLogins
from lakehouse.web.socket import guard_live_socket, install_origin

__all__ = ["DEFAULT_PORT", "serve"]

DEFAULT_PORT = 7860


def _mode(env: Mapping[str, str]) -> Literal["hf", "local"]:
    lake = env.get("LAKEHOUSE_LAKE_ROOT", "")
    return "hf" if lake.startswith("hf://") else "local"


def serve(
    env: Mapping[str, str] | None = None,
    *,
    run: bool = True,
    runner: Callable[[Flask, Any, int], None] | None = None,
) -> tuple[Flask, Any]:
    """Build the Flask app and the live Taipy Gui (origin-locked, connect/message-authed — Option 3, Taipy at `/`),
    and (when `run`) serve them. Returns the app and the Gui. `runner` is injected in tests so no server starts; the
    socket is wrapped either way, so a `run=False` test sees the live, guarded socket."""
    chosen = dict(os.environ if env is None else env)
    # imported here so the module imports no lake package at load time; the composition root (wiring) and the
    # services layer do the reaching (import-linter: web reaches the lake only through the services layer)
    from lakehouse.services.settings import SettingsService
    from lakehouse.ui.actions import ACTION_OUTCOMES, READ_ONLY_ACTIONS, build_action_registry
    from lakehouse.ui.shell import STYLEKIT, STYLES_CSS, build_gui
    from lakehouse.wiring import build_services, open_lake

    # Loud at app start (spec S06): the Ops styling depends on ui/styles.css being served over http. A missing file
    # would render an unstyled Space (the anti-goal), so fail here rather than deploy-and-observe.
    if not STYLES_CSS.is_file():
        raise RuntimeError(f"the Ops stylesheet is missing: {STYLES_CSS} — the app would render unstyled")

    mode = _mode(chosen)
    storage, resolved = open_lake(chosen)
    settings: Settings = SettingsService(storage, resolved).load()[0]
    services = build_services(storage, resolved, env=chosen)
    app = create_app(settings, mode=mode, env=chosen, public_runs=services.runs)
    built, page_refreshers = build_gui(services)
    gui: Any = built  # `object` from the shell; the Taipy Gui is untyped, so the seam calls are Any
    gui._set_flask(app)  # bind OUR Flask app as Taipy's server (the run() `flask=` kwarg does NOT bind it)

    # Build the Taipy server on the Flask app WITHOUT serving (Option 3: Taipy stays at `/`), lock the socket origin,
    # then wrap its connect/message/disconnect handlers for auth before anything can connect.
    origin = install_origin(chosen, mode)
    gui.run(
        run_server=False,
        server_config={"socketio": {"cors_allowed_origins": origin}},
        dark_mode=True,
        use_reloader=False,
        stylekit=STYLEKIT,
        title="Lakehouse Ops",
    )
    connections = _require(app, "LAKEHOUSE_CONNECTIONS", ConnectionRegistry)
    guard_live_socket(
        gui._server._ws,
        gui,
        valid_logins=_require(app, "LAKEHOUSE_VALID_LOGINS", ValidLogins),
        connections=connections,
    )
    # Every page button routes through the guarded dispatch with the connect-time principal (Option 3, §3); a
    # successful write refreshes the active page's display (page-binding addendum §4).
    install_live_actions(
        gui,
        registry=build_action_registry(services),
        connections=connections,
        refreshers=page_refreshers,
        outcomes=ACTION_OUTCOMES,
        read_only=READ_ONLY_ACTIONS,
    )

    port = int(chosen.get("APP_PORT", DEFAULT_PORT))
    if run:
        _serve(gui, port) if runner is None else runner(app, gui, port)
    return app, gui


def _require[T](app: Flask, key: str, kind: type[T]) -> T:
    value = cast("object", app.config[key])  # Flask config values are `Any`; narrow via isinstance below
    if not isinstance(value, kind):
        raise TypeError(f"{key} must be a {kind.__name__}")  # pragma: no cover - create_app sets it
    return value


def _serve(gui: Any, port: int) -> None:
    """Serve the already-built, already-wrapped Taipy server through `_Server.run` (pinned by test_taipy_shape)."""
    gui._server.run(
        host="0.0.0.0",  # noqa: S104 - the Space binds all interfaces
        port=port,
        client_url=None,
        debug=False,
        use_reloader=False,
        flask_log=False,
        run_in_thread=False,
        allow_unsafe_werkzeug=True,
        notebook_proxy=False,
        port_auto_ranges=None,
    )
