# v2 Live Web Enforcement and Role-Ready Authorization — design addendum (round 5)

**Status:** draft for review, round 5 (owner, then two independent A/B reviews). Extends the approved cycle-2 design
`docs/specs/2026-10-28-v2-observability-ops-design.md` (§6 authorization, §7 observability, §9 log viewer). Owner
decided (2026-10-28) this work is 100% part of cycle 2, to the v2 gold-standard bar. Review history: round-2 A
APPROVE; round-2 B resolved B01/B02/B04/B05; round 3 resolved B03/B08/B09/B10 (round-3 A APPROVE); round 4 resolved
B11/B12 (round-4 A and B APPROVE); owner decided §8 Q1-Q4. **Round 5 (this one): the `/ops` mount is dropped.** The
L4 Step-1 empirical verify found Taipy 4.1.1 `base_url` rewrites only client URLs, not the Flask server routes, so
Taipy cannot be relocated under `/ops` (`gui.run(run_server=False)` returning the app and `gui._server._ws` existing
were both confirmed). The owner chose Option 3 (2026-10-28): Taipy keeps `/`, and the Flask guard serves the public
page in place for an anonymous `GET /`. This is the approved path-design change (Reviewer B B16).

**Goal:** make the cycle-2 authorization and observability real in the running app, with a role-ready authorization
model, without relying on Taipy socket security the community edition does not provide.

## 0. The constraint that shapes the design (Reviewer B LWE-B01, confirmed in Taipy 4.1.1 sources)

Taipy's socket has **no per-message authorization in the community edition**. `_manage_message` (gui.py:655-695)
handles the `_WsType` messages `U` (update), `A` (action), `DU`, `RU`, `GMC`, `GDT`, `GR`; each is wrapped only by
`_get_authorization()` (gui.py:667), which delegates to `_Hooks()._get_authorization` (gui.py:2946-2951) — a
`contextlib.nullcontext()` no-op unless the enterprise package is installed. A `U` message sets any variable **and**
runs a client-named `on_change` (gui.py:669-676; the name is looked up and called); `RU`/`DU`/`GDT` read state. So
any client on the Taipy socket can set state, run a callback by name, or read page state with **no gate**. Gating
only the `A` (action) message does not protect the app.

**Consequences:** (1) the anonymous surface and the operator surface are **structurally separate**; (2) the Taipy
socket is trusted only for a **full-trust (WRITE-capable) principal** — a read-only role cannot be safe on it and is
served its own read surface instead (see §3, §6).

## 1. User experience (owner)

- An **anonymous** visitor who opens the app at `/` lands on the **public page**: Monitoring — spend per run and the
  inter-layer dependency view, which carry no match identity (cycle-2 §6.4, §7.2). A **Sign in** link is visible.
- After **sign-in**, the visitor sees the operator app at `/` (the Taipy app) with **every** operator page
  (Monitoring, Data sources, What to download, Runs, Review queue, Schedule, Settings, Logs) with **complete
  functionality** — every action works through the guarded dispatch — and a **Sign out** link.

## 2. Architecture: operators-only Taipy app + a guard-served public page

**Paths (owner decision Option 3, 2026-10-28).** Taipy owns `/` and its own routes (`/taipy-jsx/<page>`,
`/taipy-init`, the catch-all `/<path>`, content, upload, `/socket.io`); `base_url` rewrites only client URLs, not the
server routes (L4 Step-1 verify), so Taipy is not relocated. Instead the Flask guard (`_require_login`) serves the
public page **in place** for an anonymous `GET /`, falls through to Taipy for an operator, and redirects an anonymous
request to any other Taipy route to `/signin`. So an anonymous visitor sees only the public page at `/` and the
sign-in flow; an operator sees the Taipy app at `/`. Because one URL (`/`) returns two bodies by auth, every `/`
response sets `Cache-Control: no-store` and `Vary: Cookie`, so no browser or proxy cache serves the public page to
an operator or the Taipy shell to an anonymous visitor (Reviewer B LWE-B20).

1. **Public view — guard-served at `/`.** For an anonymous `GET /`, the guard returns a read-only HTML page built by
   `render_public_html(runs)` from the Monitoring data (spend + dependency, no match identity) from `RunService`
   (`task_rows`/`dependency`), plus a **Sign in** link. It is **not a Flask route** (Taipy owns `/`); it has **no
   socket, no callbacks, no per-client state**, so it is anon-safe by construction and immune to §0.
2. **Operator app — the Taipy Gui at `/`, operators only.** An anonymous caller never reaches it:
   - The Flask guard (`web/app.py` `_require_login`) keeps **every** Taipy endpoint behind sign-in — `/`, the
     catch-all `/<path>` (server.py:167), `/taipy-jsx/<page>` (gui.py:2732), `/taipy-init`, content and upload. An
     anonymous `GET /` is answered **in place** with the public page; any other Taipy route redirects an anonymous
     caller to `/signin` (Reviewer B LWE-B05). The guard applies the **same validity rule as the socket**
     (§2.4): a session whose `login_id` is not in the valid-login set is treated as signed-out and redirected,
     clearing the stale cookie. So a server restart (empty set) or a sign-out redirects the HTTP pages too — never
     pages that load while every socket is refused, and never a copied cookie that outlives sign-out (Reviewer B
     LWE-B11).
   - The socket handshake can bypass `before_request`, so the Taipy socket **rejects a connection whose principal
     cannot `WRITE`** (today: a non-operator): after `gui.run(run_server=False, server_config=...)` builds
     `gui._server._ws` (gui.py `__init_server`) without serving, the entrypoint wraps `_ws`'s `connect` handler
     (server.py:124) to return `False` unless `resolve_principal(session).can(WRITE)`, then serves. A permitted
     connection proceeds to Taipy's `gui._handle_connect` (gui.py:635).
   - **Origin lock (finding S1):** the entrypoint passes the **supported** config
     `server_config={"socketio": {"cors_allowed_origins": install_origin(env, mode)}}` to `gui.run`, overriding
     Taipy's `"*"` default (server.py:94, merged at :101-102). Never `"*"`.
3. **Actions.** Every operator action routes through `authz.dispatch` for the `WRITE` permission check (§3), audit
   attribution, and defence in depth. Because the socket is WRITE-only and origin-locked, the connect auth + origin
   lock are the **primary CSRF defence**; the per-event token adds nothing an attacker could not already satisfy, so
   round 1's self-comparing token is dropped (Reviewer B LWE-B02), and the origin-lock test is the guard.
4. **Session revocation (Reviewer B LWE-B03).** An HTTP sign-out cannot reach the socket's session copy
   (flask-socketio `manage_session=True` copies the session at connect; the Flask session is a client cookie). So
   revocation uses **server-side state**:
   - Sign-in mints a random `login_id` into the session and adds it to a server-side **valid-login set**; the
     connect captures that `login_id` and its `sid`.
   - Sign-out removes the `login_id` from the valid set and **disconnects** the matching socket(s) (`socketio`
     disconnect by `sid`).
   - The valid-login set is the **single validity authority** for all three enforcement points — the HTTP guard
     (§2.2), the socket connect (§2.2), and **every socket message**. The per-message check wraps Taipy's message
     handler (`@self._ws.on("message")`, server.py:117-122), not only connect (Reviewer B LWE-B12); a `login_id` no
     longer in the set refuses the message and disconnects.
   - The valid set is process-local for the single-process Space; a multi-worker deployment backs it with a shared
     store (builder confirms the deployment). Because it is the single authority, a restart or a sign-out invalidates
     HTTP and socket together — consistent, no dead UI. Test: sign out over HTTP **on the same browser session that
     holds the socket**, then the next socket action is refused.

**Disposition of cycle-2 `build_socketio`** (Reviewer A D3-SPEC-04, Reviewer B LWE-B04): it added a *second*
`SocketIO` the live app never starts (`web/main.py:56`), so `test_cross_origin_socket_refused` tested an unused
socket — false assurance. `build_socketio` and that test are **removed**; the origin-lock role moves to the
`server_config` pass, the connect-auth role to the `_ws` connect wrap, and a new test exercises the **live Taipy
socket** only.

**Taipy-private dependency.** `gui._server._ws`, the connect wrap, and `run_server=False` + self-serve (via
`_Server.run`, server.py:303) are pinned to Taipy 4.1.1 and covered by `test_taipy_internal_shape_pinned`, which
pins `run_server`/`server_config` on `gui.run` and the `_server`/`_ws`/`_Server.run` shape — so an upgrade breaks the
test rather than silently disabling enforcement. `server_config.socketio.cors_allowed_origins` is a supported config
key, not a private.

## 3. The role-ready authorization model (gold standard)

Authorization decides on a principal's permissions, never on a specific username.

- **`Principal`** — `user: str | None`, `roles: frozenset[Role]`, resolved by one `resolve_principal(session)`.
  Today: no `SESSION_USER` ⇒ `Principal(None, {Role.ANONYMOUS})`; a signed-in user ⇒ `Principal(user, {Role.OPERATOR})`.
- **`Permission`** — `VIEW_PUBLIC`, `VIEW_PRIVATE`, `WRITE` (extensible, e.g. `MANAGE_ACCOUNTS`).
- **`ROLE_PERMISSIONS`** — the only role→permission map. Today `ANONYMOUS → {VIEW_PUBLIC}`,
  `OPERATOR → {VIEW_PUBLIC, VIEW_PRIVATE, WRITE}`.
- **`Principal.can(permission)`** — union of its roles' permissions; **deny-by-default**.
- **Keyed on permission:** the Taipy socket connect requires `can(WRITE)`; `dispatch(action)` needs the action's
  permission (writes: `WRITE`; preview: `VIEW_PRIVATE`); the public page serves `VIEW_PUBLIC`; `scoped_reads` filters
  on `VIEW_PRIVATE`.
- **Role-ready scope (Reviewer B LWE-B08).** Adding a role is one `ROLE_PERMISSIONS` entry **and** a surface
  decision, not a handler change:
  - A role **with `WRITE`** (e.g. a future `ADMIN`) uses the operator Taipy app as is.
  - A role **without `WRITE`** (e.g. a read-only `VIEWER`) is **not** admitted to the Taipy socket (§0 would let it
    run callbacks un-gated); it is served a **read surface** built with the §2.1 static pattern. The permission map
    entry is still one line; the read surface reuses an existing pattern.

## 4. Access map applied to the live app

| Surface / endpoint | Anonymous | Operator | Enforced by |
|---|---|---|---|
| `GET /` | public page (guard-served HTML) | Taipy app | `_require_login` branches on auth; `VIEW_PUBLIC` for anon |
| Other Taipy endpoints (`/<path>`, `/taipy-jsx/*`, `/taipy-init`, `/socket.io`, content, upload) | no (→ `/signin`) | view | Flask `_require_login` + socket connect-auth requiring `can(WRITE)` |
| Any change (toggle, save filter, preview, Run now, schedule, review decision) | no | perform | `authz.dispatch` — `WRITE` (preview: `VIEW_PRIVATE`) |

## 5. Security requirements (from the security audit)

- **S1 (High):** origin lock of §2.2 is mandatory; `test_live_socket_origin_lock` asserts the live Taipy socket
  rejects a foreign origin and accepts the install origin, and fails if the lock is removed.
- **S2 (Medium):** in HF/production mode, **require `LAKEHOUSE_SESSION_SECRET`** — `create_app(mode="hf")` fails fast
  if absent, instead of the random per-process fallback (`web/app.py:66`). Local mode keeps the fallback.
- **S3 (Low, clean-extension):** no sign-in lockout yet; the role model leaves room for one. Documented, not built
  (owner confirms).
- **B02:** the socket's CSRF defence is the origin lock + WRITE-only connect, stated plainly; no self-comparing
  token. **B03:** server-side revocation per §2.4.

## 6. Clean-extension invariants

- Adding a role is one `ROLE_PERMISSIONS` entry; a `WRITE` role uses the Taipy app, a read-only role is served a read
  surface (§3, §2.1 pattern) — no per-handler change.
- A future public read-only summary of other data is another guard-served public page (§2.1), never the Taipy socket.
- The public log view (cycle-2 §12) passes a principal without `VIEW_PRIVATE`; `scoped_reads` already redacts.
- Swapping the identity source (local account → OIDC roles) changes only `resolve_principal`.

## 7. Verification (named tests, tiers)

- `test_principal_permissions` (unit) — the role→permission map; deny-by-default; the checks reproduce cycle-2
  results.
- `test_public_page_anon_no_identity` (integration) — an anonymous GET of `/` is 200, shows the spend and dependency
  view, contains no match name/id, serves no socket script.
- `test_taipy_endpoints_require_login` (integration) — an anonymous GET of `/taipy-jsx/<page>`, `/taipy-init` and the
  catch-all each redirect to `/signin`; an anonymous GET of `/` returns the public page; an **operator** GET of `/`
  returns the Taipy app, not the public page; both `/` responses carry `Cache-Control: no-store` and `Vary: Cookie`
  (Reviewer A D3-SPEC-05, Reviewer B LWE-B05, LWE-B20).
- `test_taipy_socket_rejects_non_write` (integration) — a connection whose principal cannot `WRITE` is refused; a
  `U` (with an `on_change` name), `RU`, `DU` and `GDT` message from such a client reaches no handler (Reviewer B
  LWE-B01).
- `test_signout_revokes_socket` (integration) — an operator action over the live socket performs the write; after
  sign-out over HTTP on the same session, the socket is disconnected and the next action is refused (Reviewer B
  LWE-B03).
- `test_stale_session_redirects_http` (integration) — with a valid session cookie but an empty valid-login set (a
  restart, or after sign-out), a GET of a Taipy route redirects to `/signin` — the HTTP guard and the socket share
  one validity rule (Reviewer B LWE-B11).
- `test_live_socket_origin_lock` (integration) — the live Taipy socket rejects a foreign origin, accepts the install
  origin (S1).
- `test_session_secret_required_on_hf` (unit) — `create_app(mode="hf")` without `LAKEHOUSE_SESSION_SECRET` fails fast
  (S2).
- `test_taipy_internal_shape_pinned` (unit) — the Taipy internals the wiring depends on exist in 4.1.1
  (`gui._server._ws`, the connect handler and the **message** handler `@self._ws.on("message")` at server.py:117-122,
  `run_server`/`server_config`, `_Server.run`).

## 8. Author's calls and open questions

**Author's calls (builder confirms):** the public page's renderer (reuse `ui/pages/monitoring.py` spend/dependency
computation as HTML, no Taipy); the valid-login store backing (process-local vs shared); the module placement of the
role/permission model (`services/authz.py`).

**Owner decisions (2026-10-28, approved as recommended):**
1. **Public surface = Monitoring only** this addendum. The cycle-2 §6.3 public summary of Data sources / What to
   download is **deferred to a later cycle with the owner's approval** — a recorded, approved reduction of the §6.3
   anon-viewable set, not an unapproved cut. It returns as further guard-served public pages (§2.1 pattern).
2. **Require `LAKEHOUSE_SESSION_SECRET` in HF/production** (S2): `create_app(mode="hf")` fails fast if it is absent.
3. **Sign-in lockout: document-only** this addendum (S3); the role model leaves room to add it later.
4. **Accept the pinned Taipy-internal seam** (`run_server=False` + `_ws` connect/message wrap, §2.2) guarded by
   `test_taipy_internal_shape_pinned`.
5. **Path scheme = Option 3** (2026-10-28): Taipy keeps `/`, the guard serves the public page in place for an
   anonymous `GET /` (the `/ops` mount is dropped — Taipy `base_url` does not prefix the server routes).

## 9. Plan note

Implemented as an amendment to the cycle-2 plan, landing in the **same single cycle commit** (owner: combine). No
commit without explicit owner approval.
