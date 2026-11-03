# v2 Live Web Enforcement and Role-Ready Authorization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task,
> native execution, no subagents (owner rule). Steps use checkbox (`- [ ]`) syntax.

**Goal:** make the cycle-2 authorization and observability real in the running app — an anonymous visitor sees a
static public Monitoring page with a Sign-in link; a signed-in operator sees the full Taipy Ops app with every action
guarded — on a role-ready authorization model, without relying on Taipy socket security the community edition lacks.

**Architecture (Option 3, owner 2026-10-28):** Taipy keeps `/` (its `base_url` does not prefix the Flask server
routes — L4 Step-1 verify). The Flask guard serves a socket-free public page in place for an anonymous `GET /`, and
keeps every Taipy route operators-only (the socket rejects a connection that cannot `WRITE`; origin-locked). One
server-side valid-login set is the single validity authority for the HTTP guard, the socket connect and every socket
message. Authorization decides on a `Principal`'s permissions, not a username.

**Tech Stack:** Python 3.12, Flask + flask-socketio 5.7.0, Taipy GUI 4.1.1 (pinned), the cycle-2 `authz`/`observability`
seams, uv, pytest.

**Spec:** `docs/specs/2026-10-28-v2-live-web-enforcement-design.md` (A/B-approved round 4; owner decisions §8). Read it
and the cycle-2 design `docs/specs/2026-10-28-v2-observability-ops-design.md` §6/§7/§9.

## Global Constraints

- Python 3.12; line length 120; the v2 checks all pass (ruff check [E,W,F,I,N,UP,B,S,BLE,RUF], ruff format --check,
  lint-imports, pyright, pytest, pip-audit) — no `lf` (not a v2 check, Reviewer B B17).
- Taipy 4.1.1 internals are pinned and guarded by a shape test; `server_config.socketio.cors_allowed_origins` is a
  supported config, not a private.
- **No commit.** Every task ends without a commit; this work lands in the **single cycle commit** (cycle-2 plan Task
  13 Gate, owner: combine), only on the owner's explicit approval.
- Tests run on the Mac Mini / DGX, not the AI-PC C: drive (owner rule).
- Owner decisions (spec §8): public surface = Monitoring only; require `LAKEHOUSE_SESSION_SECRET` on HF; sign-in
  lockout document-only; accept the pinned Taipy seam.
- The `integration` pytest marker is already registered (cycle-2 obs-ops Task 7, `pyproject.toml`); this plan runs on
  the same combined tree, so it adds no marker. L7 runs `-m integration` against that tree (Reviewer A D4-PLAN-02).

## Reconciliation with the cycle-2 implementation (one commit, one model)

This work and the already-implemented cycle-2 (obs-ops) Tasks 6/7/9 land in the **same single commit** and must leave
**one** socket model, one anon surface, one authz model, and a green suite (Reviewer A D4-PLAN-01). Cycle 2 built an
anon-**admitting** Taipy socket plus per-caller `render_for` anon gating; this plan replaces that with an
operators-only Taipy app plus a separate static public page. Each task below supersedes specific cycle-2 artifacts:

- **Socket model — L1 removes, L4 adds (obs-ops Task 7).** The L1 `dispatch(action, principal)` migration obsoletes
  the standalone socket, so **L1** removes `web/socket.py`'s `build_socketio`, `register_action_events` and
  `_CONNECTION_SESSIONS` (the second `SocketIO` the live app never starts) and `tests/integration/test_socket_authz.py`
  (`test_anon_socket_action_refused`, `test_authed_socket_connects_and_writes`, `test_cross_origin_socket_refused`).
  **Keep** `install_origin`; its `test_install_origin_per_mode` moves to `tests/unit/test_socket.py` (L1). **L4** adds
  the live Taipy-socket wraps. Cycle-2 §11 items 2-3 (`test_anon_socket_action_refused`, `test_cross_origin_socket_refused`)
  map to L4's `test_taipy_socket_rejects_non_write` and `test_live_socket_origin_lock` (Reviewer A D4-PLAN-03, Reviewer
  B LWE-B15/B21).
- **Per-caller content — L3/L4/L6 supersede obs-ops Task 7's `render_for`.** The Taipy app is operators-only, so
  `ui/shell.py`'s `render_for` anon branch, `ANON_VIEWABLE_ROUTES` and `anon_viewable` are removed; the Gui renders
  pages for operators via `page_markdown`, and the anon surface is the separate public page (L3). Remove
  `tests/unit/test_page_scope.py` (it asserts `render_for`'s anon gate); its intent is re-homed to L3
  (`test_public_page_anon_no_identity`) and L4 (`test_taipy_endpoints_require_login`).
- **Monitoring / Logs anon assertions — L3/L4 supersede obs-ops Task 9.** Keep the `monitoring.py` / `logs.py` pages
  (operators see them in the Taipy app; `monitoring.py`'s computation also feeds the public page). In
  `tests/unit/test_monitoring_pages.py`: `test_monitoring_public_no_match_identity` → re-homed to L3 (the public page
  carries no identity); `test_operator_only_pages` → re-homed to L4 (every Taipy route redirects anon); the
  `anon`-notice part of `test_log_viewer_authenticated_only_top_n` → superseded by the guard, its newest-first-top-N
  part stays as a logs-page render test.
- **authz — L1 extends obs-ops Task 6.** L1 adds the principal/permission model and re-expresses the write gate as
  `can(WRITE)`. It **drops the CSRF token from `dispatch`** — on the Taipy socket a token could only be compared with
  itself, which spec §2.3/§5 removed (Reviewer B LWE-B02/B13). CSRF stays where a client really sends one: the HTTP
  form layer (`csrf.py` `before_request`, covered by `test_csrf_required_on_post`). The socket's defence is origin
  lock + connect-auth + valid `login_id`. The cycle-2 `test_action_requires_operator_and_write` is updated to assert
  the `can(WRITE)` gate, not a token; `scoped_reads`'s tests stay green.

A pointer to this amendment is added to the cycle-2 obs-ops plan, so its Task 7/9 socket/`render_for` steps are not
read in isolation (Reviewer B LWE-B15).

Net after the commit: one socket (Taipy's, operators-only, origin-locked), one anon surface (the static public page),
one authz model (permissions), one green suite. Each task's steps below carry the removal/re-home it owns.

## Review Focus

The spec implies these; no task's happy-path test exercises them, and each would bite a real deployment. Each has a
test pinned to the owning task.

1. **RESOLVED (L4 Step 1 verify, owner Option 3):** Taipy `base_url` rewrites only client URLs, not the Flask server
   routes, so the `/ops` mount is not viable. Taipy keeps `/`; the guard serves the public page in place for an
   anonymous `GET /`. `gui.run(run_server=False)` returning the app and `gui._server._ws` existing were confirmed, so
   the serve seam and socket wrap stand.
2. **Returning `False` from the wrapped Taipy connect may not reject under the chosen `async_mode`**, and
   disconnect-by-sid may not reach the socket. Task L4 tests an anonymous connect is refused and a signed-out socket
   is severed, under the production `async_mode`.
3. **The valid-login set is process-local**: a multi-worker HF deployment would give each worker its own set, so a
   socket on worker B is refused by worker A's empty set. Task L2 pins the deployment as single-process (or a shared
   store) and tests the stale-session redirect with the set emptied.
4. **A forged/copied session cookie** (valid signature, unknown `login_id`) must be refused by both the HTTP guard and
   the socket. Task L2 tests a cookie whose `login_id` is not in the set is redirected.
5. **The public page must not pull Taipy** (its Markdown is not plain HTML; a stray Taipy import would add a socket).
   Task L3 tests the public response serves no socket script and imports no Taipy.

---

## Task L1: The role-ready authorization model (and the standalone-socket removal it obsoletes)

**Files:** Modify `v2/src/lakehouse/services/authz.py`, `v2/src/lakehouse/ui/actions.py` (principal-based handlers),
`v2/src/lakehouse/web/socket.py` (reduce to `install_origin`). Remove `v2/tests/integration/test_socket_authz.py`.
Test `v2/tests/unit/test_authz.py`, `v2/tests/unit/test_config_actions.py`, `v2/tests/unit/test_socket.py` (new,
holds `test_install_origin_per_mode`).

**Removal (Reviewer A D4-PLAN-03 / Reviewer B B21):** the `dispatch(action, principal)` signature obsoletes the
cycle-2 standalone socket, so L1 removes `web/socket.py`'s `build_socketio`/`register_action_events`/
`_CONNECTION_SESSIONS` and the whole `test_socket_authz.py` (test-only in production); `install_origin` stays and its
test moves to `test_socket.py`. L4 adds the live Taipy-socket wraps.

**Interfaces — Produces:** `Role` (`ANONYMOUS`, `OPERATOR`); `Permission` (`VIEW_PUBLIC`, `VIEW_PRIVATE`, `WRITE`);
`ROLE_PERMISSIONS: dict[Role, frozenset[Permission]]`; `Principal(user: str | None, roles: frozenset[Role])` with
`can(permission) -> bool` (deny-by-default); `resolve_principal(session: Mapping) -> Principal`. `dispatch(action,
principal, **kwargs)` refuses unless the principal `can` the action's permission (writes: `WRITE`; preview:
`VIEW_PRIVATE`); **no CSRF token in `dispatch`** — CSRF is the HTTP form layer's job (`csrf.py`). `ScopedReads`/
`scoped_reads` filter on `VIEW_PRIVATE`.

- [ ] **Step 1: Write the failing tests** — `test_principal_permissions`: `ROLE_PERMISSIONS` gives anon
  `{VIEW_PUBLIC}` and operator all three; `Principal(None, {ANONYMOUS}).can(WRITE)` is False; `resolve_principal({})`
  is anonymous, `resolve_principal({SESSION_USER: "op"})` is operator with `WRITE`; an unknown role grants nothing.
  Update `test_action_requires_operator_and_write` to assert `dispatch` refuses a principal without `WRITE` and
  performs for one with `WRITE`, **with no token argument** (Reviewer B LWE-B13).
- [ ] **Step 2:** Run `pytest tests/unit/test_authz.py -v`. Expected FAIL.
- [ ] **Step 3:** Add the model; re-express `dispatch`'s write check as `can(WRITE)` and the read action as
  `can(VIEW_PRIVATE)`, and `scoped_reads` operator-scope as `can(VIEW_PRIVATE)`; remove the `csrf_token`/`_write_ok`
  self-comparison from `dispatch`. CSRF for state-changing HTTP form posts stays in `csrf.py` `before_request`
  (`test_csrf_required_on_post` unchanged). `test_read_action_requires_sign_in_only`/`test_public_read_scope` stay
  green.
- [ ] **Step 4:** Reduce `web/socket.py` to `install_origin(env, mode)` only. Remove `build_socketio`,
  `register_action_events`, and `_CONNECTION_SESSIONS` (the standalone-socket dead end). Delete
  `tests/integration/test_socket_authz.py`; move `test_install_origin_per_mode` to `tests/unit/test_socket.py`.
  L4 adds the live-integration socket wraps on the Taipy server; no standalone socket survives. (Reviewer A/B
  B21, D4-PLAN-04.)
- [ ] **Step 5:** Run `pytest tests/unit/test_authz.py -v`. Expected PASS (new + the updated cycle-2 authz tests).
- [ ] **Step 6:** Task suite green. No commit.

## Task L2: Server-side session validity and the web guard (B11, S2)

**Files:** Create `v2/src/lakehouse/web/sessions.py` (the valid-login set **and** the connection registry). Modify
`v2/src/lakehouse/web/app.py` (`_require_login`, `_secret_key`), `v2/src/lakehouse/web/auth_local.py` and
`v2/src/lakehouse/web/auth_hf.py` (mint / invalidate `login_id`, sign-in redirect, sign-out disconnect). Test
`v2/tests/unit/test_web_auth.py`, `v2/tests/integration/test_session_validity.py`.

**Interfaces — Produces:** `ValidLogins` with `mint(session) -> str`, `is_valid(session) -> bool`, `revoke(session)`;
`ConnectionRegistry` with `bind(login_id, sid)`, `unbind(sid)`, `set_disconnector(fn)`, `disconnect(login_id)` — a
`login_id → set[sid]` map **owned here** so the HTTP sign-out can reach it (L4 binds at connect and registers the
socketio disconnector). `_require_login` redirects to `/signin` when the session is anonymous **or** its `login_id`
is not valid, clearing the stale cookie. Sign-in mints a `login_id` and redirects to `/` (the Taipy app — Option 3);
sign-out revokes the `login_id` **and** `ConnectionRegistry.disconnect`s its live socket(s), not waiting for the next
message (Reviewer B LWE-B14). `create_app(mode="hf")` raises if `LAKEHOUSE_SESSION_SECRET` is absent.
**Consumes:** L1 `resolve_principal`.

- [ ] **Step 1: Write the failing tests** — `test_session_secret_required_on_hf` (`create_app(mode="hf")` without
  `LAKEHOUSE_SESSION_SECRET` raises); `test_forged_login_id_refused` (a valid-signature cookie with an unknown
  `login_id` redirects); `test_signin_redirects_to_root` (a correct sign-in redirects to `/`); `test_signout_revokes`
  (sign-out removes the `login_id` and calls `ConnectionRegistry.disconnect`). (The HTTP stale-session redirect and
  the socket disconnect-without-a-message are integration — `test_stale_session_redirects_http` and
  `test_signout_revokes_socket` live in L4's integration module, spec §7, Reviewer B B18.)
- [ ] **Step 2:** Run `pytest tests/unit/test_web_auth.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement `ValidLogins` and `ConnectionRegistry` (process-local for the single-process Space;
  documented shared-store seam for multi-worker — Review Focus 3); sign-in mints a `login_id` and redirects to
  `/`; sign-out revokes + disconnects; `_require_login` applies sign-in + `is_valid` to every endpoint; HF mode
  requires the session secret (fail fast). Keep `session.clear()` on login (fixation defence).
- [ ] **Step 4:** Run; Expected PASS (new + the cycle-2 web-auth tests).
- [ ] **Step 5:** Task suite green. No commit.

## Task L3: The guard-served public page at `/` (Option 3)

**Files:** Create `v2/src/lakehouse/web/public.py` (`render_public_html`). Modify `v2/src/lakehouse/web/app.py` (the
guard serves it for an anonymous `GET /`; `public_runs` param). Test `v2/tests/unit/test_public_page.py`.

**Interfaces — Produces:** `render_public_html(runs) -> str` — the Monitoring data (spend + dependency, no identity)
as plain HTML with a `Sign in` link, from `RunService.task_rows`/`dependency`; no socket, no Taipy import. It is **not
a Flask route** (Taipy owns `/`): `_require_login` returns it for an anonymous `GET /`.
**Consumes:** cycle-2 `RunService.task_rows`/`dependency`, `observability.RATE_TABLE_DATE`.

- [ ] **Step 1: Write the failing test** — `test_public_page_anon_no_identity`: an anonymous `GET /` is 200, shows the
  spend estimate "est. $X at 2026-10-26 rates (process time)" and the dependency edges, contains no match name/id and
  no `socket.io` script, carries `Cache-Control: no-store` and `Vary: Cookie` (Reviewer B LWE-B20), and `web.public`
  imports no `taipy` module (Review Focus 5).
- [ ] **Step 2:** Run `pytest tests/unit/test_public_page.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement `render_public_html` (reuse `ui/pages/monitoring.py`'s computation; plain HTML, not Taipy
  Markdown). In `_require_login`, an anonymous `GET /` returns `render_public_html(public_runs)`; an operator falls
  through to Taipy; any other Taipy route redirects an anonymous caller to `/signin`. An `after_request` sets
  `Cache-Control: no-store` and `Vary: Cookie` on every `/` response so a cache never serves the wrong body (B20).
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task L4: Taipy live integration — origin lock, connect/message auth (Option 3, Taipy at `/`)

**Files:** Modify `v2/src/lakehouse/web/main.py` (serve seam + wraps), `v2/src/lakehouse/web/socket.py` (keep
`install_origin`; add the Taipy-socket wrap helpers). Test `v2/tests/integration/test_live_web.py`,
`v2/tests/unit/test_taipy_shape.py`. (`build_socketio` and `test_socket_authz.py` were already removed in L1.)

**Interfaces — Produces:** `serve` builds the Gui with `gui.run(run_server=False, server_config={"socketio":
{"cors_allowed_origins": install_origin(env, mode)}})` — Taipy stays at `/` (no mount, Option 3) — wraps
`gui._server._ws` `connect` to reject a principal that cannot `WRITE` and **bind `login_id → sid` in L2's
`ConnectionRegistry`** (and register the socketio disconnector so sign-out can sever immediately), wraps the
`message` handler (server.py:117-122) to refuse + disconnect when the connection's `login_id` is no longer valid,
then serves via `_Server.run`.
**Consumes:** L1 `resolve_principal`/`Permission`, L2 `ValidLogins` + `ConnectionRegistry`, cycle-2 `install_origin`.

- [ ] **Step 1 (done at the owner gate):** L4 Step-1 verified `base_url` does not prefix the server routes; the owner
  chose Option 3 (Taipy at `/`, guard-served public). `gui.run(run_server=False)` returns the app and
  `gui._server._ws` exists — the serve seam and wrap point stand. No `/ops` mount.
- [ ] **Step 2: Write the failing tests** — `test_taipy_socket_rejects_non_write` (an anon connect is refused; a `U`
  with an `on_change` name, `RU`, `DU`, `GDT` from a non-WRITE client reaches no handler); `test_signout_revokes_socket`
  (an operator action works; after HTTP sign-out on the same session the socket is **disconnected without the client
  sending a message**, and a later action is refused — Reviewer B LWE-B14); `test_stale_session_redirects_http` (a
  valid cookie with an empty valid-set — a restart — makes `GET /taipy-init` redirect to `/signin`, Reviewer B B18);
  `test_live_socket_origin_lock` (a foreign `Origin` handshake is refused, the install origin accepted, and it fails
  if the lock is removed); `test_taipy_endpoints_require_login` (anon `GET /taipy-jsx/<page>`, `/taipy-init`,
  catch-all → `/signin`; anon `GET /` returns the public page; an OPERATOR `GET /` returns the Taipy app, not the
  public page — Reviewer A D3-SPEC-05; both `/` responses carry `Cache-Control: no-store` + `Vary: Cookie` — B20);
  `test_taipy_internal_shape_pinned` (`gui._server._ws`, the connect and `@self._ws.on("message")` handlers,
  `run_server`/`server_config`, `_Server.run` exist in 4.1.1).
- [ ] **Step 3:** Run the tests. Expected FAIL.
- [ ] **Step 4:** Implement the serve seam (no mount — Taipy at `/`), the origin lock, the connect wrap (reject
  non-WRITE; `bind` the `login_id → sid` in `ConnectionRegistry`; register the socketio disconnector) and the message
  wrap (per-event `is_valid` recheck + disconnect). (`build_socketio` and `test_socket_authz.py` were already removed
  in L1; their cases are re-homed by Step 2's tests — Reviewer B LWE-B15.)
- [ ] **Step 5:** Run; Expected PASS.
- [ ] **Step 6:** Task suite green. No commit.

## Task L5: Page actions through the live dispatch

**Files:** Modify `v2/src/lakehouse/ui/actions.py` (principal-based), the `ui/pages/*` `on_action` callbacks, and the
Taipy action wiring in `web/main.py`. Test `v2/tests/integration/test_live_web.py`.

**Interfaces — Consumes:** L1 `dispatch`/`resolve_principal`, L2 `ConnectionRegistry` (bound by L4 at connect).
**Produces:** each page `on_action` is a Taipy callback that resolves the connection principal and calls
`authz.dispatch`; a refusal shows a visible message and changes nothing.

- [ ] **Step 1: Write the failing test** — `test_live_action_through_dispatch`: a signed-in operator triggers a page
  action over the live socket and the service write happens (e.g. a module toggle persists); the same action from a
  connection without `WRITE` is refused and nothing changes.
- [ ] **Step 2:** Run `pytest tests/integration/test_live_web.py::test_live_action_through_dispatch -v`. Expected FAIL.
- [ ] **Step 3:** Wire the Taipy `on_action` callbacks through `dispatch` with the connection principal (from
  `ConnectionRegistry`) — **no CSRF token**; the socket's defence is origin lock + connect-auth + valid `login_id`,
  and `dispatch` gates on `can(WRITE)` (Reviewer B LWE-B13).
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task L6: Per-caller navigation, Sign-in/out, and ADR update

**Files:** Modify `v2/src/lakehouse/ui/shell.py` (operator nav + Sign out), `v2/src/lakehouse/web/public.py` (Sign in
link — done in L3, verify), `v2/docs/adrs/ADR-011-authz-model.md` (live enforcement + the role model). Test
`v2/tests/unit/test_nav.py`.

**Interfaces — Consumes:** L1 `Principal`. **Produces:** the operator nav lists every page + Sign out; the public page
lists Monitoring + Sign in.

- [ ] **Step 1: Write the failing test** — `test_nav_operator_and_public`: the operator nav lists all operator pages
  and a Sign-out link; the public page lists Monitoring and a Sign-in link.
- [ ] **Step 2:** Run `pytest tests/unit/test_nav.py -v`. Expected FAIL.
- [ ] **Step 3:** Implement the operator nav + Sign out; confirm the public Sign in; update ADR-011 (Nygard, STE) with
  the live-enforcement model, the role-ready `Principal`/`Permission` model, the valid-login single authority, and the
  Taipy-pinned seam.
- [ ] **Step 4:** Run; Expected PASS.
- [ ] **Step 5:** Task suite green. No commit.

## Task L7: Full verification

- [ ] **Step 1:** Run the v2 check set on the full tree (ruff, format, lint-imports, pyright, pip-audit,
  `pytest -m "not live and not hf and not bench and not integration"`, then `pytest -m integration`). Expected: green.
- [ ] **Step 2:** Confirm every spec §7 named test exists and passes; list any skip with its reason.
- [ ] **Step 3:** No commit — this work joins the cycle commit at the cycle-2 plan Task 13 Gate, on the owner's
  explicit approval, with the two independent A/B implementation reviews.
