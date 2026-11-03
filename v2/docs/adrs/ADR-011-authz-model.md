# ADR-011: The Ops authorization model

## Status

Accepted (2026-10-28). Amended (2026-10-28, live-web enforcement addendum) to the role-ready model and the live
Taipy integration (Option 3). Owner decisions: the access map is approved (spec §6.3); the public surface is
Monitoring only; HF mode requires `LAKEHOUSE_SESSION_SECRET`; Taipy keeps `/` and the guard serves the public page
in place for an anonymous `GET /` (Option 3). Specs: `docs/specs/2026-10-28-v2-observability-ops-design.md` §6 and
`docs/specs/2026-10-28-v2-live-web-enforcement-design.md`.

## Context

- Cycle 1 enforced sign-in on every Ops route with a Flask `before_request` guard, and the CSRF check is also a
  `before_request`. Both run only for Flask HTTP requests.
- Cycle 2 adds interactive actions. Taipy sends its messages over socket.io, which `flask-socketio` mounts as a WSGI
  middleware that wraps `app.wsgi_app`. A socket request never reaches Flask's `before_request`, so a route-level
  guard alone does not cover the socket (Reviewer B OBS-B01).
- The community Taipy socket has no per-message authorization seam: its `message` handler runs `U` (update), `A`
  (action), `RU`, `DU`, `GDT` and a client-named `on_change` with no gate. Gating only the `A` message does not
  protect the app, because `U` plus an `on_change` name already changes state. So the socket is safe only for a
  full-trust, WRITE-capable principal; a read-only role is never admitted and is served a separate read surface.
- Authorization must decide on a principal's permissions, not on a specific user name, so a later identity source
  (OIDC claims, an account role) plugs in without a refactor.

## Decision

### The principal model (role-ready)

`services/authz.py` defines a `Principal` (`user: str | None`, `roles: frozenset[Role]`) resolved by one
`resolve_principal(session)`. A `Permission` is `VIEW_PUBLIC`, `VIEW_PRIVATE` or `WRITE`. `ROLE_PERMISSIONS` maps each
role to its permissions: `ANONYMOUS` holds `VIEW_PUBLIC`; `OPERATOR` holds all three. `Principal.can(permission)`
checks the union of its roles, deny-by-default. Today a signed-in user is the sole `OPERATOR`; an unsigned caller is
`ANONYMOUS`. Adding a role is one `ROLE_PERMISSIONS` entry plus a read surface for a non-WRITE role; `resolve_principal`
is the only seam a new identity source changes.

### Server-side session validity (the single authority)

`web/sessions.py` `ValidLogins` holds the set of valid login ids. A login id is minted at sign-in, checked on every
HTTP request and every socket message, and revoked at sign-out. It is the one validity authority for the HTTP guard,
the socket connect and every socket message, so a restart (empty set) or a sign-out invalidates both surfaces
together — no dead UI, no cookie that outlives sign-out. `ConnectionRegistry` maps a login id to its live socket
id(s) and holds each socket's connect-time principal, so an HTTP sign-out disconnects the live socket at once and a
socket message resolves its principal without a session.

### Three enforcement points (live)

1. **The socket.io connection.** `web/socket.py` `install_origin` sets `cors_allowed_origins` to the install's own
   origin (the Space origin on HF, the localhost origin locally), never a wildcard; engineio refuses a foreign
   `Origin` handshake with HTTP 400. `guard_live_socket` wraps Taipy's own `connect` handler on `gui._server._ws` to
   return `False` unless `resolve_principal(session).can(WRITE)` and the session's login is valid, so an anonymous,
   non-WRITE or stale caller never joins. On a permitted connect it binds the login id, sid and principal in the
   `ConnectionRegistry`. The wrapped `message` handler re-checks the connection's login per message and severs a
   revoked or stale socket. There is no CSRF token on the socket: its defence is the origin lock, the WRITE-only
   connect and the valid login.
2. **The action handlers.** `services/authz.py` `ActionRegistry.dispatch(action_name, principal, **kwargs)` is the one
   invocation path. It gates on the action's `Permission` — a write needs `WRITE`, the operator-only filter preview
   needs `VIEW_PRIVATE` — and raises `Unauthorized` otherwise (fail closed). It attributes the write to the
   principal's user, never to a client-sent name. `web/live_actions.py` installs the Gui-level `on_action` handler
   that reads the caller's connect-time principal by sid, collects the action's inputs from the page state, and
   routes through `dispatch`; a refusal or a service error sets a visible notice and changes nothing.
3. **The page content.** `ui/shell.py` `render_for` resolves content per caller, and `scoped_reads` is the
   private-data boundary: a caller without `VIEW_PRIVATE` sees only rows whose visibility is explicitly `public`; a
   `null` or `private` visibility is never shown (fail-safe). The spend and dependency view carries no match identity
   and is safe for an anonymous caller by column scope; every page that can expose a private match is operator-only.

### The public surface (Option 3)

Taipy owns `/`. The Flask guard serves the anonymous public page in place for an anonymous `GET /` (`web/public.py`
`render_public_html`: read-only Monitoring, no socket, no Taipy import), lets an operator fall through to the Taipy
app, and redirects an anonymous caller on any other Taipy route to `/signin`. Because `/` serves two bodies by
cookie, every `/` response carries `Cache-Control: no-store` and `Vary: Cookie` so no cache crosses them. Sign-in
redirects to `/`; the operator nav lists every page and Sign out; the public page carries its own Sign-in link.

### The Taipy-pinned seam

The wrap re-registers private Taipy internals (`gui._server._ws`, the connect/message/disconnect handlers,
`gui._manage_message`, `_Server.run`) reached through `gui.run(run_server=False)` after `gui._set_flask(app)` binds
our Flask app. `test_taipy_shape` pins each of these, so a Taipy upgrade that moves one fails loudly before the socket
auth breaks (Hyrum's Law).

## Consequences

- The services layer never imports the web layer: `services/authz.py` defines its own `SESSION_USER` and a parity
  test asserts it equals `web.auth_local.SESSION_USER`.
- The log viewer is authenticated-only this cycle. Every collected record carries its visibility, so a later public
  view passes a principal without `VIEW_PRIVATE` and `scoped_reads` redacts private-match records with no page
  rewrite (ADR-012, spec §12).
- `ValidLogins` and `ConnectionRegistry` are process-local, which suits the single-process Space. A multi-worker
  deployment backs them with a shared store; that is the one documented seam.
- **Open finding (owner decision pending): live widget state binding.** The live socket, the dispatch seam and the
  per-caller surfaces are built and tested, and a button click routes through the guarded dispatch. But Taipy binds a
  page's interactive variables from the frame that constructs the `Gui`, and `build_gui` constructs then returns, so
  its frame is dead when the server runs and no page variable (`active_module`, `draft_active`, `draft_cron`, …) is
  bound. An action with no input (Run now) works end to end; an input-backed action becomes fully functional once the
  variables bind. The binding approach is a presentation decision for the owner (construct the Gui and define the
  interactive variables and callbacks at module scope — the canonical Taipy pattern — or bind each variable
  explicitly). The authorization and dispatch seam does not change under any option.

## Verification (to be re-measured at the cycle gate)

The L1–L6 layers pass the scoped CI checks on this machine: `ruff check`, `ruff format --check`, `lint-imports`
(5 contracts kept), `uv run pyright` (0 errors). The live-web tests pass: `test_authz`, `test_web_auth`,
`test_public_page`, `test_socket`, `test_taipy_shape`, `test_nav` (unit) and `test_live_web` (integration: connect
rejects non-WRITE, sign-out severs the socket, a stale session redirects, the origin lock, the Taipy endpoint guard
with the operator-`/` and public-`/` split, the action through dispatch). The full-tree run and the OS-gated tests
are confirmed at plan L7.
