# ADR-083: Override flask-socketio past Taipy's cap and install the Space lockfile with --no-deps

| Field | Value |
|---|---|
| **Date** | 2026-08-31 |
| **Status** | Accepted |
| **Deciders** | Karsten Nielsen |

## Context

The production Taipy Space (`luxury-lakehouse/soccer-analytics-app`) rendered a **blank page** while
HF reported the container `RUNNING`. Taipy delivers all page content over a Socket.IO websocket; the
`connect` handler crashed on **every** connection with `AttributeError: can't set attribute 'session'`
at `flask_socketio/__init__.py:819` (`ctx.session = session_obj`), so the shell loaded but the
`<div id="root">` never populated.

Root cause, established by live logs + a byte-identical local reproduction + a version matrix:

- **Flask 3.1.3** made `RequestContext.session` a read-only property (new `_session` backing field).
- **flask-socketio 5.4.1** still assigns `ctx.session = …` directly → crash on every socket event
  (`manage_session=True`, Taipy's default). The matrix: flask 3.1.0/3.1.1/3.1.2 OK, **3.1.3 breaks**.
  flask-socketio fixed it in **5.5.1** (its `_session` compatibility branch).
- The 2026-08-12 "D5" CVE deploy added a `flask>=3.1.3` floor (PYSEC-2026-2151) to `[tool.uv]
  constraint-dependencies`. Before it, the Space resolved the *working* flask 3.1.1 (capped by
  taipy-rest). The floor pushed flask to 3.1.3 — the exact version that breaks flask-socketio 5.4.1.
  The app has rendered blank since that deploy (the D5 audit measured package movement + security
  resolution, not that the app renders).

The forcing function is a direct conflict: **security requires `flask>=3.1.3`; flask-socketio 5.4.1
requires `flask<3.1.3`.** And **taipy-gui 4.1.2 caps `flask-socketio<5.5`** — 4.1.2 is the latest
release, so no Taipy bump reaches the fixed 5.5.1. There is no version combination inside taipy-gui
4.1.2's declared constraints that both keeps the CVE fixed and works.

## Decision

Add `[tool.uv] override-dependencies = ["flask-socketio>=5.5.1"]` to lift taipy-gui 4.1.2's
`flask-socketio<5.5` cap (resolving to 5.6.1), keeping `flask>=3.1.3` so PYSEC-2026-2151 stays fixed;
and install the compiled Space lockfile with `pip install --no-deps` in `hf_taipy_app/Dockerfile`,
because the override makes `requirements.txt` pin a version that plain `pip install -r` rejects
(`ResolutionImpossible` against taipy-gui's metadata cap). `--no-deps` installs the complete
uv-resolved pin set as-is, matching how `scripts/audit_resolutions.py` already treats the file.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| A. Pin `flask==3.1.2` | Self-consistent `requirements.txt`; no Dockerfile change; flask-socketio 5.4.1 works | Reopens PYSEC-2026-2151 (flask < 3.1.3) | Security regression the team deliberately fixed; "No shortcuts on security" |
| B. Bump Taipy to lift the cap | Clean dep graph; no override, no `--no-deps` | **taipy-gui 4.1.2 is the latest release** — nothing newer exists | Impossible: no Taipy release requires flask≥3.1.3 while allowing flask-socketio≥5.5.1 |
| C. Override `flask-socketio>=5.5.1` alone (no Dockerfile change) | One-line change | `pip install -r requirements.txt` fails `ResolutionImpossible` (taipy-gui 4.1.2 metadata caps `<5.5`) — the Space build breaks | uv's override doesn't reach pip; proven failing in `python:3.10-slim` |
| D. Override + Dockerfile `--no-deps` (chosen) | Keeps flask≥3.1.3 (CVE fixed); minimal delta (flask-socketio 5.4.1→5.6.1 only); build succeeds; socket connects | `requirements.txt` carries a taipy-gui↔flask-socketio metadata conflict (cosmetic under `--no-deps`); pip no longer re-validates the graph at build (uv already did) | — |

## Consequences

### Positive

- Production websocket layer works again (validated: `import taipy.gui` OK + socket `is_connected: True`
  on the real `python:3.10-slim` base image running the exact Dockerfile step).
- PYSEC-2026-2151 stays fixed (`flask>=3.1.3` retained); the two socketio CVE floors are unaffected.
- Minimal blast radius: the override moves exactly one package (flask-socketio 5.4.1→5.6.1);
  flask-socketio is GUI-only, so Terraform env pins, CI dbt pins, and the Databricks runtime are
  untouched (`sync_tf_env_pins --check` clean).

### Negative

- `requirements.txt` now contains a version (flask-socketio 5.6.1) that violates taipy-gui 4.1.2's
  declared `<5.5` cap. This is inert under `--no-deps` but will read as a conflict to anyone who runs
  a plain `pip install -r requirements.txt` outside the Docker build.
- `--no-deps` means pip performs no dependency-graph validation at build time. This is safe because
  the file is a complete uv-resolved lockfile, but it removes a (previously redundant) backstop.

### Neutral

- python-engineio drifts 4.13.4→4.14.0 on the next compile — independent of this change (a redeploy
  would do it regardless), still above the `>=4.13.2` CVE floor.

## CLAUDE.md Amendment

None required. The `--no-deps` install is a deliberate, documented deviation from the default
`pip install -r requirements.txt` for the Taipy Space only; it does not alter any project-wide rule.
The change follows the existing convention that `requirements.txt` is "a complete locked resolution"
(`scripts/audit_resolutions.py`, which already audits it with `--no-deps`).

## Related

- **Commits:** (this change)
- **Issues / PRs:** (this change)
- **ADRs:** builds on ADR-076 (Taipy Space deploy artifact validation); the `flask>=3.1.3` floor lives
  in `[tool.uv] constraint-dependencies` (PYSEC-2026-2151, added 2026-08-12).
- **External references:** Flask 3.1.3 `RequestContext.session` property change; flask-socketio 5.5.1
  `_session` compatibility branch (`__init__.py` — "update session for Flask >= 3.1.3"); PYSEC-2026-2151.

## Notes

Version matrix (flask-socketio 5.4.1, forcing the `manage_session` path via the real test client):

| flask | result |
|---|---|
| 3.1.0 / 3.1.1 / 3.1.2 | socket connects |
| 3.1.3 | `AttributeError: can't set attribute 'session'` (blank page) |

pip resolver behaviour (`python:3.10-slim`, pip 23.0.1): plain `pip install -r requirements.txt` →
`ResolutionImpossible`; `pip install --no-deps -r requirements.txt` → installs 137 packages, exit 0.
