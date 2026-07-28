"""Databricks credential helpers for CI and local runs (ADR-071 amendment, 2026-07-27).

**The rule this module exists to enforce: never materialise the bearer.**

The Databricks SDK re-mints on *every* request when configured for GitHub OIDC --
``_base_client.py:84`` wires ``session.auth = self._authenticate``, which calls
``_header_factory()`` per prepared request (``:105-110``); for ``github-oidc`` that
factory is ``refreshed_headers()`` (``credentials_provider.py:494-497``), which builds a
fresh ``ClientCredentials`` from a live GitHub id-token fetch every call
(``:473-491`` -> ``oidc_token_supplier.py:16-32``). A reused ``WorkspaceClient``
therefore *cannot* serve a stale bearer.

Snapshotting ``config.authenticate()`` into ``DATABRICKS_TOKEN`` throws that away and
yields a dead string. That is what broke every scheduled workflow from 2026-07-22:
measured valid at mint+3:59 and rejected with ``403 Invalid Token`` at mint+5:13, in the
same job, on the same API.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:  # pragma: no cover - typing only
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    from databricks.sdk.credentials_provider import CredentialsStrategy
    from databricks.sdk.oauth import Token

__all__ = [
    "CachedGitHubOidcStrategy",
    "auth_headers",
    "bearer_token",
    "has_databricks_auth",
    "workspace_client",
]

#: Both are required by ``GitHubOIDCTokenSupplier.get_oidc_token`` (``oidc_token_supplier.py:17``);
#: either one alone yields ``None`` and no token.
_GITHUB_OIDC_VARS = ("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_URL")


def _is_set(var: str) -> bool:
    """True only for a non-empty value. An empty ``DATABRICKS_TOKEN`` is the classic
    ``Authorization: Bearer `` footgun, not a credential."""
    return bool(os.environ.get(var, "").strip())


def has_databricks_auth() -> bool:
    """Whether a live Databricks call can be attempted from this process.

    Requires a host **and** a credential: a static ``DATABRICKS_TOKEN``, or GitHub OIDC
    (from which the SDK mints one per request).

    Used by the live-test skip guards in place of a bare ``DATABRICKS_TOKEN`` check, which
    would silently skip the whole live suite once CI stops materialising a token -- turning
    a real signal into a vacuous green.

    Host is part of the predicate rather than a separate ``and`` at each of the 11 call
    sites: a credential with nowhere to send it is not auth, and an absent host otherwise
    produces a confusing live-call failure instead of a skip. Deliberately an env-var check
    and **not** SDK-config resolution -- a host resolved from ``~/.databrickscfg`` would
    turn a safe local skip into a live call.

    Returns ``False`` on fork pull requests, where GitHub issues no id-token and no secrets.
    """
    if not _is_set("DATABRICKS_HOST"):
        return False
    if _is_set("DATABRICKS_TOKEN"):
        return True
    return all(_is_set(var) for var in _GITHUB_OIDC_VARS)


def bearer_token() -> str:
    """A **fresh** bearer token string, resolved through the SDK's provider chain.

    For call sites that need the raw credential rather than a header -- chiefly
    ``databricks.sql.connect(access_token=...)`` in the live data-quality suite, whose
    driver takes a token, not an ``Authorization`` value.

    This is the read-only replacement for ``os.environ["DATABRICKS_TOKEN"]``. Those direct
    reads are why the 2026-07-28 Data Quality CI run on main produced ``1 passed, 119
    errors``: once the workflows stopped materialising the token, ``has_databricks_auth()``
    correctly reported auth-available via OIDC, so the tests *ran* -- and then every module
    fixture died on ``KeyError: 'DATABRICKS_TOKEN'``. Migrating the skip guards without
    migrating the connection bodies converted a silent skip into a loud collection error.

    Works unchanged under a static ``DATABRICKS_TOKEN`` (local dev) or GitHub OIDC (CI).
    Call it at the point of use, never once at start-up.

    Raises
    ------
    RuntimeError
        Via :func:`auth_headers`, if the SDK cannot produce a ``Bearer`` header.
    """
    return auth_headers()["Authorization"].removeprefix("Bearer ")


def auth_headers() -> dict[str, str]:
    """A **fresh** ``Authorization`` header, resolved through the SDK's provider chain.

    For raw-``requests`` call sites that cannot use a ``WorkspaceClient`` -- the Lakebase
    credential/DNS endpoints and the Jobs REST calls in ``patch_job_retries``. Call it at
    the point of use, never once at start-up: under ``github-oidc`` each call mints a new
    bearer, which is the entire point.

    Works unchanged whether the process is configured with a static ``DATABRICKS_TOKEN``
    (``auth_type=pat``, local dev) or GitHub OIDC (CI) -- ``Config.authenticate()``
    dispatches on whichever is configured, so call sites need no branch of their own.

    Raises
    ------
    RuntimeError
        If the SDK cannot produce a ``Bearer`` header. Loud by design: the alternative is
        an ``Authorization: Bearer `` that 403s far from its cause.
    """
    from databricks.sdk import WorkspaceClient

    headers = WorkspaceClient().config.authenticate() or {}
    value = headers.get("Authorization", "")
    if not value.startswith("Bearer "):
        msg = (
            "Databricks SDK returned no Bearer authorization header "
            f"(got {value[:24]!r}). Check DATABRICKS_HOST and either DATABRICKS_TOKEN or "
            "DATABRICKS_AUTH_TYPE=github-oidc with ACTIONS_ID_TOKEN_REQUEST_* present."
        )
        raise RuntimeError(msg)
    return {"Authorization": value}


# ---------------------------------------------------------------------------
# Tier 2: cache the EXCHANGED bearer (spec D5)
# ---------------------------------------------------------------------------
#
# The stock github-oidc strategy is correct but chatty: `token_source_for` builds a NEW
# `ClientCredentials` on every call (credentials_provider.py:494-497 -> :473-491), so
# `Refreshable`'s cache never engages and each Databricks API call pays a GitHub id-token
# fetch plus a token exchange before the request itself.
#
# The naive fix -- caching the SDK's `ClientCredentials` -- is WRONG: its `refresh()`
# re-posts the original `endpoint_params`, which embed the GitHub subject JWT. Once that
# JWT expires the exchange fails, so a cache at that layer works until it suddenly does
# not. That is why the SDK rebuilds per call.
#
# So we cache one layer up, at the exchanged Databricks token, and delegate the refresh to
# the stock provider's own `oauth_token()` -- which re-runs the whole chain including a
# fresh subject-JWT fetch. Correct by delegation rather than by reimplementation.


class _CachedExchangedToken:
    """Wraps a full token-exchange callable in ``Refreshable``'s caching semantics."""

    def __init__(self, fetch: Callable[[], Token]) -> None:
        from databricks.sdk import oauth

        self._fetch = fetch

        class _Source(oauth.Refreshable):
            def refresh(self) -> Token:
                # `fetch` comes from the enclosing closure, not from self — the outer
                # instance is never touched here, so plain `self` shadows nothing.
                return fetch()

        self._source = _Source()

    def token(self) -> Token:
        return self._source.token()


class CachedGitHubOidcStrategy:
    """``CredentialsStrategy`` that caches the exchanged Databricks bearer.

    Keeps every property of the stock ``github-oidc`` strategy -- ``Refreshable``'s
    40-second pre-expiry skew (``oauth.py:114-119``), FRESH/STALE states and asynchronous
    pre-emptive refresh -- while actually letting the cache engage.
    """

    def auth_type(self) -> str:
        return "github-oidc"

    def __call__(self, cfg: Config) -> Callable[[], dict[str, str]]:
        from databricks.sdk import credentials_provider as cp

        provider = cp.github_oidc(cfg)
        if provider is None:
            msg = (
                "github-oidc credentials unavailable: need DATABRICKS_HOST, "
                "DATABRICKS_CLIENT_ID and ACTIONS_ID_TOKEN_REQUEST_TOKEN/_URL."
            )
            raise RuntimeError(msg)
        cached = _CachedExchangedToken(provider.oauth_token)

        def headers() -> dict[str, str]:
            tok = cached.token()
            return {"Authorization": f"{tok.token_type} {tok.access_token}"}

        return headers


def workspace_client(**kwargs: Any) -> WorkspaceClient:
    """A ``WorkspaceClient`` that re-mints on demand, and caches the exchange when it can.

    Under GitHub OIDC this installs :class:`CachedGitHubOidcStrategy`; otherwise it returns
    a stock client so local dev (PAT or an OAuth CLI profile) is untouched.

    Adopted by the three CI-reachable construction sites -- ``heal_synced_tables``,
    ``run_lakebase_grants`` and ``grant_synced_table_permissions``. NOT
    ``refresh_synced_tables``: that one runs as a Databricks task under ambient runtime auth,
    where this strategy is inert, and its module-level ``WorkspaceClient`` is a test seam.
    Everything else stays on a bare ``WorkspaceClient()``,
    which is already correct: it re-mints per request, just without the cache.
    """
    from databricks.sdk import WorkspaceClient

    using_oidc = os.environ.get("DATABRICKS_AUTH_TYPE") == "github-oidc" and all(
        _is_set(var) for var in _GITHUB_OIDC_VARS
    )
    if using_oidc:
        # Structural, not nominal: subclassing CredentialsStrategy would require importing
        # databricks-sdk at module scope, and this module must stay importable without the
        # [sdk] extra — 11 test files call has_databricks_auth() at collection time.
        strategy = cast("CredentialsStrategy", CachedGitHubOidcStrategy())
        return WorkspaceClient(credentials_strategy=strategy, **kwargs)
    return WorkspaceClient(**kwargs)
