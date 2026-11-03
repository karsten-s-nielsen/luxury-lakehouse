"""CSRF protection for the Ops web app (spec §20; plan Task 27).

Every state-changing request (POST, PUT, PATCH, DELETE) must carry the session's CSRF token, in the form field
`csrf_token` or the header `X-CSRF-Token`. A GET issues the token: it is created once per session and sent back in
the `X-CSRF-Token` response header, so a page can embed it. The token is compared in constant time.
"""

from __future__ import annotations

import secrets
from typing import Final

from flask import Flask, Response, request, session

__all__ = ["CSRF_HEADER", "FORM_FIELD", "SESSION_KEY", "current_token", "install_csrf"]

SESSION_KEY: Final = "_csrf_token"
CSRF_HEADER: Final = "X-CSRF-Token"
FORM_FIELD: Final = "csrf_token"
_UNSAFE: Final = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def current_token() -> str:
    """The session's CSRF token, created on first use, for a page to embed."""
    token = session.get(SESSION_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[SESSION_KEY] = token
    return token


def _submitted() -> str | None:
    return request.form.get(FORM_FIELD) or request.headers.get(CSRF_HEADER)


def install_csrf(app: Flask, *, exempt: frozenset[str] = frozenset()) -> None:
    """Add the CSRF check to every unsafe request, and send the token on every response. `exempt` holds endpoint
    names that run their own check (the HF OAuth callback, which the identity provider calls)."""

    @app.before_request
    def _check() -> tuple[str, int] | None:
        if request.method not in _UNSAFE or request.endpoint in exempt:
            return None
        expected = session.get(SESSION_KEY)
        submitted = _submitted()
        if not expected or not submitted or not secrets.compare_digest(str(expected), submitted):
            return "CSRF token missing or wrong", 400
        return None

    @app.after_request
    def _emit(response: Response) -> Response:
        token = session.get(SESSION_KEY)
        if token:
            response.headers[CSRF_HEADER] = str(token)
        return response
