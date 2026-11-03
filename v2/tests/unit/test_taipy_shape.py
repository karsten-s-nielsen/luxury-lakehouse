"""Pin the Taipy 4.1.1 internals the live-web socket wrap depends on (addendum §2; plan L4).

The community Taipy socket has no per-message auth seam, so the live app wraps Taipy's own connect/message/disconnect
handlers (`web/socket.py`) on `gui._server._ws`, served through `_Server.run` after `gui.run(run_server=False)`. These
are private Taipy internals; a Taipy upgrade that moves any of them must fail HERE, loudly, before the socket auth
silently breaks (Reviewer B B04/B12; Hyrum's Law). Each assertion names the wrap point it guards.
"""
# Taipy ships no type stubs, so poking its internals is all `Unknown` under strict mode, and the wrap points are
# private (`_Server`, `gui._server`, `gui._manage_message`). This probe exists precisely to pin those private
# internals, so suppress the untyped-dependency and private-usage rules here, keeping the rest of strict on.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportPrivateUsage=false

from __future__ import annotations

import inspect
from typing import Any

from taipy.gui import Gui  # pyright: ignore[reportMissingTypeStubs]
from taipy.gui.server import _Server  # pyright: ignore[reportMissingTypeStubs]


def _built_gui() -> Any:
    """A Gui built but not served (`run_server=False`), with the origin lock, as the live app builds it. The shape
    probe inspects Taipy internals only, so it lets Taipy make its own internal Flask (the live app binds ours)."""
    gui = Gui(pages={"/": "# probe"})
    gui.run(
        run_server=False,
        server_config={"socketio": {"cors_allowed_origins": ["http://localhost:7860"]}},
        use_reloader=False,
    )
    return gui


def test_gui_run_accepts_run_server_and_server_config() -> None:
    params = inspect.signature(Gui.run).parameters
    assert "run_server" in params  # the serve seam: build without serving
    # `server_config` rides through **kwargs into app_config (proved to reach engineio by the origin-lock test).
    assert any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def test_run_server_false_builds_server_and_ws() -> None:
    gui = _built_gui()
    assert hasattr(gui, "_server")  # gui.run(run_server=False) builds the server without serving
    ws = gui._server._ws
    assert ws is not None and hasattr(ws, "server")  # a flask_socketio.SocketIO with a python-socketio server


def test_socket_handlers_present() -> None:
    handlers = _built_gui()._server._ws.server.handlers["/"]
    assert {"connect", "message", "disconnect"} <= set(handlers)  # the three handlers the wrap re-registers


def test_gui_message_seam_present() -> None:
    gui = _built_gui()
    assert callable(gui._manage_message)  # the wrapped message handler forwards to this
    assert callable(gui._handle_connect) and callable(gui._handle_disconnect)  # connect/disconnect pass-through


def test_server_run_signature_pinned() -> None:
    params = set(inspect.signature(_Server.run).parameters)
    needed = {
        "host",
        "port",
        "client_url",
        "debug",
        "use_reloader",
        "flask_log",
        "run_in_thread",
        "allow_unsafe_werkzeug",
        "notebook_proxy",
        "port_auto_ranges",
    }
    assert needed <= params  # the serve call passes exactly these


def test_origin_lock_lands_on_engineio() -> None:
    eio = _built_gui()._server._ws.server.eio
    assert eio.cors_allowed_origins == ["http://localhost:7860"]  # server_config.socketio.cors_allowed_origins wins


def test_taipy_internal_shape_pinned() -> None:
    """The spec §7 named guard: every Taipy 4.1.1 internal the wiring depends on exists, in one place. The granular
    tests above give finer failures; this one pins the whole set the spec names (Reviewer B B04/B12)."""
    gui = _built_gui()
    server = gui._server
    handlers = server._ws.server.handlers["/"]
    assert {"connect", "message", "disconnect"} <= set(handlers)  # @self._ws.on("message") at server.py:117-122
    run_params = inspect.signature(Gui.run).parameters
    assert "run_server" in run_params  # gui.run(run_server=False) builds without serving
    assert "server_config" in run_params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in run_params.values())
    assert "host" in inspect.signature(_Server.run).parameters  # _Server.run serves the built server
    assert callable(gui._manage_message)  # the wrapped message handler forwards here
