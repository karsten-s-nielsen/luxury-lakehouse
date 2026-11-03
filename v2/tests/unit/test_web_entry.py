"""The web entrypoint (plan Task 29): `lakehouse.wiring.build_services` composes one of each Ops service over a real
lake with no network, and `lakehouse.web.main.serve` builds the Flask app and the Gui and serves them through an
injected runner (so no server starts in the test)."""

from __future__ import annotations

from typing import Any, cast

from lakekit import LakeKit
from runkit import active, save_settings

from lakehouse.storage.roots import ResolvedRoots
from lakehouse.ui import shell
from lakehouse.wiring import build_services


def test_build_services_composes_every_service_offline(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    resolved = ResolvedRoots(roots=kit.roots, source="bootstrap")
    services = build_services(kit.storage, resolved, env={})
    assert isinstance(services, shell.Services)
    # every page renders from the composed services (a real catalog read, no network) through the one render path
    for route in shell.page_routes():
        assert shell.render_for(route, services, "operator")


def test_serve_builds_app_and_gui_without_starting(kit: LakeKit, monkeypatch: Any) -> None:
    from lakehouse.web import main

    save_settings(kit.storage, [active("idsse_public")])
    env = {
        "LAKEHOUSE_LAKE_ROOT": str(kit.roots.lake.local_path),
        "LAKEHOUSE_RESTRICTED_ROOT": str(kit.roots.restricted.local_path),
        "LAKEHOUSE_SESSION_SECRET": "a-long-enough-session-secret-value",
    }
    built = main.serve(env, run=False)
    app, gui = built
    assert app.config["LAKEHOUSE_MODE"] == "local"
    app.testing = True
    client = app.test_client()
    base = "https://localhost"
    assert client.get("/ops/ping", base_url=base).status_code == 302  # the sign-in guard is wired
    assert gui is not None

    # B04 / spec S04: the sign-in guard is the primary source protection — an anonymous GET of the stylesheet or any
    # .py source is redirected (the Taipy catch-all is not an open endpoint), so no anonymous leak is possible.
    assert client.get("/styles.css", base_url=base).status_code == 302
    assert client.get("/shell.py", base_url=base).status_code == 302
    # signed in: the stylesheet is served (ui/.taipyignore anchors the matcher at ui/, so styles.css resolves), and the
    # ui source is excluded (404). A web/ source request is never served as source (it raises in the matcher → 500).
    from lakehouse.web.auth_local import SESSION_USER
    from lakehouse.web.sessions import ValidLogins

    valid = cast("ValidLogins", app.config["LAKEHOUSE_VALID_LOGINS"])
    with client.session_transaction() as sess:
        sess[SESSION_USER] = "ops"
        valid.mint(sess)
    assert client.get("/styles.css", base_url=base).status_code == 200
    assert client.get("/shell.py", base_url=base).status_code == 404
    assert client.get("/main.py", base_url=base).status_code != 200  # web/ source never served (C08)

    calls: list[int] = []
    main.serve(env, run=True, runner=lambda a, g, port: calls.append(port))
    assert calls == [main.DEFAULT_PORT]  # the runner is called instead of starting a server


def test_security_headers_on_every_response(kit: LakeKit) -> None:
    """Audit S2: every response carries the defence-in-depth headers (CSP, HSTS, nosniff, X-Frame-Options), set by
    the `after_request` so even a sign-in redirect (302) gets them."""
    from lakehouse.web import main

    save_settings(kit.storage, [active("idsse_public")])
    env = {
        "LAKEHOUSE_LAKE_ROOT": str(kit.roots.lake.local_path),
        "LAKEHOUSE_RESTRICTED_ROOT": str(kit.roots.restricted.local_path),
        "LAKEHOUSE_SESSION_SECRET": "a-long-enough-session-secret-value",
    }
    app, _ = main.serve(env, run=False)
    app.testing = True
    response = app.test_client().get("/ops/ping", base_url="https://localhost")
    assert response.status_code == 302  # the after_request runs on the sign-in redirect too

    csp = response.headers.get("Content-Security-Policy", "")
    assert "default-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    assert "object-src 'none'" in csp
    assert "https://fonts.gstatic.com" in csp  # font-src allows the Material Symbols @font-face in ui/styles.css
    assert response.headers.get("Strict-Transport-Security", "").startswith("max-age=")
    assert response.headers.get("X-Content-Type-Options") == "nosniff"
    assert response.headers.get("X-Frame-Options") == "DENY"


def test_taipyignore_excludes_python_source_from_static_serving() -> None:
    """Taipy serves the Gui's _root_dir (the ui/ package) as static; .taipyignore must exclude the .py source from
    signed-in callers while keeping styles.css served (review V2UI-14)."""
    from collections.abc import Callable
    from pathlib import Path

    from gitignore_parser import parse_gitignore  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]

    ui_dir = Path(shell.__file__).resolve().parent
    ignore = ui_dir / ".taipyignore"
    assert ignore.is_file(), ".taipyignore must exist at the Gui _root_dir (the ui/ package)"
    # B04 / spec S04: it must live in ui/ ONLY. A web/.taipyignore would anchor the one matcher at web/, so serving
    # ui/styles.css would raise ValueError (a 500 → unstyled Space). Under `python -m lakehouse.web` Taipy reads the
    # web/ ignore first, else falls back to this ui/ one, so the matcher anchors at ui/.
    web_dir = ui_dir.parent / "web"
    assert not (web_dir / ".taipyignore").exists(), "no web/.taipyignore: it would 500 the stylesheet (B04)"
    matches = cast("Callable[[str], bool]", parse_gitignore(ignore))
    assert matches(str(ui_dir / "shell.py"))  # the shell source is NOT served
    assert matches(str(ui_dir / "pages" / "monitoring.py"))  # a page source is NOT served
    assert not matches(str(ui_dir / "styles.css"))  # the stylesheet IS served
