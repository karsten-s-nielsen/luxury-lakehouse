"""Local sign-in (spec §19, §20; plan Task 27): one operator with an Argon2id password hash in `config/account.json`.

The blueprint serves the sign-in page and checks the password through `AccountService` (never the store). A correct
password sets the session user; a wrong one, or no account, stays on the page with a message and no session. Sign-out
is a two-step action: GET /signout renders a confirmation page and POST /signout (CSRF-protected) does the sign-out,
so a stray link, prefetch or one errant click never signs an operator out.
"""

from __future__ import annotations

from flask import Blueprint, Response, make_response, redirect, request, session
from flask.typing import ResponseReturnValue

from lakehouse.services.account import AccountService
from lakehouse.ui.template import build_brand_html, inline_style_block
from lakehouse.web.csrf import current_token
from lakehouse.web.sessions import ConnectionRegistry, ValidLogins

__all__ = ["SESSION_USER", "local_blueprint"]

SESSION_USER = "user"

# The sign-in page is plain Flask HTML with no Taipy (socket-free, like the public page). It wears the same lakehouse
# theme as the operator shell by inlining the one shared `<style>` and the brand wordmark from the Taipy-free template
# (spec §29.18 styling port). A centred `.ll-login-card` on the dark `.ll-root` page; the button reuses `.ll-primary`.
_STYLE = inline_style_block()
_PAGE = (
    '<!doctype html><html lang="en"><head><meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1">'
    "<title>Lakehouse — Sign in</title>{style}</head>"
    '<body class="ll-root"><div class="ll-login-wrap"><div class="ll-login-card">'
    '<div class="ll-login-brand">{brand}</div>{message}'
    '<form method="post" action="/signin" class="ll-login-form">'
    '<input type="hidden" name="csrf_token" value="{token}">'
    '<label>Operator <input name="username" autocomplete="username"></label>'
    '<label>Password <input type="password" name="password" autocomplete="current-password"></label>'
    '<button type="submit" class="ll-primary">Sign in</button></form></div></div></body></html>'
)


# Sign out is a state-changing action, so it is NOT a one-click GET link: GET /signout renders this confirmation page
# and the actual sign-out is a POST (CSRF-protected) from its button. Same `.ll-login-card` chrome as the sign-in page.
_CONFIRM_PAGE = (
    '<!doctype html><html lang="en"><head><meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1">'
    "<title>Lakehouse — Sign out</title>{style}</head>"
    '<body class="ll-root"><div class="ll-login-wrap"><div class="ll-login-card">'
    '<div class="ll-login-brand">{brand}</div>'
    '<p class="ll-confirm-text">Sign out of the operator app?</p>'
    '<form method="post" action="/signout" class="ll-login-form">'
    '<input type="hidden" name="csrf_token" value="{token}">'
    '<button type="submit" class="ll-primary">Sign out</button></form>'
    '<a href="/" class="ll-cancel-link">Cancel</a></div></div></body></html>'
)


def _render(message: str = "") -> Response:
    note = f'<p role="alert" class="ll-login-alert">{message}</p>' if message else ""
    return make_response(_PAGE.format(style=_STYLE, brand=build_brand_html(), message=note, token=current_token()))


def _render_confirm() -> Response:
    return make_response(_CONFIRM_PAGE.format(style=_STYLE, brand=build_brand_html(), token=current_token()))


def local_blueprint(accounts: AccountService, *, logins: ValidLogins, connections: ConnectionRegistry) -> Blueprint:
    blueprint = Blueprint("auth_local", __name__)

    @blueprint.get("/signin")
    def signin_page() -> ResponseReturnValue:
        return _render()

    @blueprint.post("/signin")
    def signin() -> ResponseReturnValue:
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if not accounts.verify(username, password):
            return _render("The operator name and password did not match.")  # 200: stay on the page, no session
        session.clear()  # session fixation defence
        session[SESSION_USER] = username
        logins.mint(session)  # the valid login id, the single validity authority (addendum §2.4)
        return redirect("/")  # the Taipy operator app lives at the root (Option 3)

    @blueprint.get("/signout")
    def signout_confirm() -> ResponseReturnValue:
        return _render_confirm()  # a confirmation step: GET never signs out, so a stray link or prefetch cannot

    @blueprint.post("/signout")
    def signout() -> ResponseReturnValue:
        login_id = logins.revoke(session)  # read before clearing the session
        session.clear()
        if login_id is not None:
            connections.disconnect(login_id)  # sever the live socket at once (addendum §2.4)
        return redirect("/signin")

    return blueprint
