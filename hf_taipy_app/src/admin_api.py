"""Authenticated admin endpoints for the Taipy app.

Provides POST /api/cache/clear (with optional ?refresh_synced=1) protected
by HuggingFace user-token validation against whoami-v2.

Auth model: caller presents an HF user access token as
`Authorization: Bearer hf_xxx`. The token is validated by calling
https://huggingface.co/api/whoami-v2 with the token in the header.
The response must show membership in the `luxury-lakehouse` org with
role `admin` or `write`.

Tokens are not stored anywhere — each call validates independently
against HF, so revocation by the user is immediate.
"""

from __future__ import annotations

import logging
import re
import subprocess
import sys
import threading
from typing import Final

import requests
from cache import cache_size, clear_cache
from flask import Blueprint, jsonify, request

_logger = logging.getLogger(__name__)

_HF_WHOAMI_URL: Final = "https://huggingface.co/api/whoami-v2"
_REQUIRED_ORG: Final = "luxury-lakehouse"
_ALLOWED_ROLES: Final = frozenset({"admin", "write"})
_HF_TOKEN_RE: Final = re.compile(r"^hf_[A-Za-z0-9]{20,}$")
_REQUEST_TIMEOUT: Final = (5, 15)  # (connect, read) per CLAUDE.md security standard


def _validate_hf_admin(auth_header: str | None) -> tuple[bool, int, str]:
    """Validate HF user token and check org membership.

    Returns
    -------
    (allowed, http_status, message)
        On success: (True, 200, <username>) — message is the HF username for logging.
        On failure: (False, <4xx-or-503>, <reason>) — reason is safe to return to caller.
    """
    if not auth_header or not auth_header.startswith("Bearer "):
        return False, 401, "Missing or malformed Authorization header"

    token = auth_header.removeprefix("Bearer ").strip()
    if not _HF_TOKEN_RE.match(token):
        return False, 401, "Token format invalid"

    try:
        resp = requests.get(
            _HF_WHOAMI_URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=_REQUEST_TIMEOUT,
            verify=True,
        )
    except requests.Timeout:
        return False, 503, "HuggingFace identity service timeout"
    except requests.RequestException as exc:
        return False, 503, f"HuggingFace identity service unreachable: {type(exc).__name__}"

    if resp.status_code == 401:
        return False, 401, "Token invalid or revoked"
    if not resp.ok:
        return False, 503, f"HuggingFace API error: {resp.status_code}"

    try:
        data = resp.json()
    except ValueError:
        return False, 503, "HuggingFace API returned non-JSON"

    orgs_raw = data.get("orgs", [])
    if not isinstance(orgs_raw, list):
        return False, 503, "HuggingFace API returned malformed orgs field"

    orgs = {o.get("name", ""): o.get("roleInOrg", "") for o in orgs_raw if isinstance(o, dict)}
    if _REQUIRED_ORG not in orgs:
        return False, 403, f"Token user is not a member of {_REQUIRED_ORG}"
    if orgs[_REQUIRED_ORG] not in _ALLOWED_ROLES:
        return False, 403, "Insufficient role (need admin or write)"

    username = data.get("name", "unknown")
    return True, 200, str(username)


def _trigger_synced_refresh_async() -> None:
    """Spawn a background thread to refresh all 37 synced tables.

    Uses subprocess (not in-process import) for two reasons: (1) refresh
    takes minutes and the HTTP caller should not wait, (2) running in an
    isolated process avoids sys.argv mutation races between concurrent
    admin requests. Errors are logged but not propagated. The supervising
    thread is daemonized so app shutdown does not block on it.
    """

    def _run() -> None:
        try:
            result = subprocess.run(  # noqa: S603
                [sys.executable, "-m", "ingestion.refresh_synced_tables", "--wait"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2400,  # 30 min refresh window + overhead, matches Databricks job task timeout
            )
            if result.returncode == 0:
                _logger.info("admin: background synced refresh completed (exit 0)")
            else:
                _logger.warning(
                    "admin: background synced refresh failed exit=%d stdout=%s stderr=%s",
                    result.returncode,
                    result.stdout[-1500:] if result.stdout else "<empty>",
                    result.stderr[-500:] if result.stderr else "<empty>",
                )
        except subprocess.TimeoutExpired:
            _logger.error("admin: background synced refresh timed out after 2400s")
        except Exception:
            _logger.exception("admin: background synced refresh raised unexpectedly")

    threading.Thread(target=_run, daemon=True, name="admin-synced-refresh").start()


def build_admin_blueprint() -> Blueprint:
    """Build the admin Flask blueprint.

    The blueprint registers POST /api/cache/clear with HF token auth.
    Inject the returned blueprint into a Flask app, then pass that
    Flask app to `taipy.gui.Gui(flask=...)`.
    """
    bp = Blueprint("admin", __name__)

    @bp.route("/api/cache/clear", methods=["POST"])
    def _clear_cache_endpoint():  # type: ignore[no-untyped-def]
        ok, status, msg = _validate_hf_admin(request.headers.get("Authorization"))
        remote = request.remote_addr or "unknown"

        if not ok:
            _logger.info(
                "admin: cache clear DENIED status=%d remote=%s reason=%s",
                status,
                remote,
                msg,
            )
            return jsonify({"error": msg}), status

        # `msg` holds the HF username on success
        _logger.info(
            "admin: cache clear ALLOWED user=%s remote=%s",
            msg,
            remote,
        )

        entries_before = cache_size()
        clear_cache()

        also_refresh = request.args.get("refresh_synced") == "1"
        if also_refresh:
            _trigger_synced_refresh_async()
            _logger.info("admin: synced table refresh triggered (background) by user=%s", msg)

        return (
            jsonify(
                {
                    "cleared": True,
                    "entries_cleared": entries_before,
                    "refresh_synced_triggered": also_refresh,
                }
            ),
            200,
        )

    return bp
