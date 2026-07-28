"""Unit tests for ``ingestion.databricks_auth`` (ADR-071 amendment, 2026-07-27).

Offline and deterministic: no live workspace, no SDK auth performed.
"""

from __future__ import annotations

from typing import cast

import pytest
from databricks.sdk.core import Config

_ALL_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_URL",
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Start from no Databricks/OIDC env at all, whatever the developer's shell has."""
    for var in _ALL_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_true_with_host_and_static_token(clean_env: pytest.MonkeyPatch) -> None:
    from ingestion.databricks_auth import has_databricks_auth

    clean_env.setenv("DATABRICKS_HOST", "https://example.databricks.com")
    clean_env.setenv("DATABRICKS_TOKEN", "x")
    assert has_databricks_auth() is True


def test_true_with_host_and_github_oidc(clean_env: pytest.MonkeyPatch) -> None:
    """The CI case after this PR: no token materialised, OIDC available instead."""
    from ingestion.databricks_auth import has_databricks_auth

    clean_env.setenv("DATABRICKS_HOST", "https://example.databricks.com")
    clean_env.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "x")
    clean_env.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "https://token.actions.example/x")
    assert has_databricks_auth() is True


def test_false_on_a_fork_pull_request(clean_env: pytest.MonkeyPatch) -> None:
    """Fork PRs get no OIDC and no secrets, so live tests must SKIP, not fail.

    GitHub does not issue an id-token to workflows triggered from a fork, so both
    ``ACTIONS_ID_TOKEN_REQUEST_*`` vars are absent. This is the scenario most likely
    to decay into a vacuous green later -- if this predicate ever returned True here,
    every live test would attempt a real call and fail confusingly on a fork PR.
    """
    from ingestion.databricks_auth import has_databricks_auth

    clean_env.setenv("DATABRICKS_HOST", "https://example.databricks.com")
    assert has_databricks_auth() is False


def test_false_without_a_host(clean_env: pytest.MonkeyPatch) -> None:
    """Host is part of the predicate: a credential with nowhere to send it is not auth.

    Folded in here rather than repeated as ``has_databricks_auth() and HOST`` at each of
    the 11 call sites. Deliberately an env-var check, not SDK-config resolution: a host
    resolved from ``~/.databrickscfg`` would turn a safe local skip into a live call.
    """
    from ingestion.databricks_auth import has_databricks_auth

    clean_env.setenv("DATABRICKS_TOKEN", "x")
    assert has_databricks_auth() is False


def test_false_with_only_half_the_oidc_pair(clean_env: pytest.MonkeyPatch) -> None:
    """``GitHubOIDCTokenSupplier`` requires BOTH vars (oidc_token_supplier.py:17)."""
    from ingestion.databricks_auth import has_databricks_auth

    clean_env.setenv("DATABRICKS_HOST", "https://example.databricks.com")
    clean_env.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "https://token.actions.example/x")
    assert has_databricks_auth() is False


def test_empty_strings_do_not_count_as_credentials(clean_env: pytest.MonkeyPatch) -> None:
    """An empty ``DATABRICKS_TOKEN`` is the classic ``Bearer `` footgun, not auth."""
    from ingestion.databricks_auth import has_databricks_auth

    clean_env.setenv("DATABRICKS_HOST", "https://example.databricks.com")
    clean_env.setenv("DATABRICKS_TOKEN", "")
    assert has_databricks_auth() is False


# --- auth_headers ----------------------------------------------------------


class _FakeConfig:
    def __init__(self, header: str | None) -> None:
        self._header = header
        self.calls = 0

    def authenticate(self) -> dict[str, str]:
        self.calls += 1
        # A live github-oidc config mints a NEW bearer per call; model that.
        return {} if self._header is None else {"Authorization": f"{self._header}{self.calls}"}


def _patch_client(monkeypatch: pytest.MonkeyPatch, cfg: _FakeConfig) -> None:
    import databricks.sdk as sdk

    monkeypatch.setattr(sdk, "WorkspaceClient", lambda *a, **k: type("_WS", (), {"config": cfg})())


def test_auth_headers_returns_the_sdk_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.databricks_auth import auth_headers

    _patch_client(monkeypatch, _FakeConfig("Bearer tok-"))
    assert auth_headers() == {"Authorization": "Bearer tok-1"}


def test_auth_headers_is_fresh_on_every_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole point: call sites must re-resolve, not cache a start-up snapshot.

    Under github-oidc the SDK mints a new bearer per call. A helper that memoised would
    silently reintroduce the 2026-07-22 outage inside library code instead of YAML.
    """
    from ingestion.databricks_auth import auth_headers

    cfg = _FakeConfig("Bearer tok-")
    _patch_client(monkeypatch, cfg)
    first, second = auth_headers(), auth_headers()
    assert first != second, "auth_headers() memoised a bearer instead of re-resolving"
    assert cfg.calls == 2


def test_auth_headers_raises_when_the_sdk_yields_no_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loud beats ``Authorization: Bearer `` 403-ing far from its cause."""
    from ingestion.databricks_auth import auth_headers

    _patch_client(monkeypatch, _FakeConfig(None))
    with pytest.raises(RuntimeError, match="no Bearer authorization header"):
        auth_headers()


# --- Tier 2: CachedGitHubOidcStrategy --------------------------------------


def _token(seconds_valid: int, n: int):  # -> oauth.Token
    from datetime import datetime, timedelta, timezone

    from databricks.sdk import oauth

    return oauth.Token(
        access_token=f"exchanged-{n}",
        token_type="Bearer",  # noqa: S106 -- OAuth token_type, not a password
        expiry=datetime.now(tz=timezone.utc) + timedelta(seconds=seconds_valid),
    )


class _FakeOauthProvider:
    """Stands in for the SDK's ``OAuthCredentialsProvider`` from ``github_oidc(cfg)``.

    Its ``oauth_token`` models the real one: a FULL re-run of the chain, i.e. a fresh
    GitHub subject-JWT fetch plus a token exchange (``credentials_provider.py:498-501``).
    """

    def __init__(self, seconds_valid: int = 3600) -> None:
        self.subject_jwt_fetches = 0
        self._seconds_valid = seconds_valid

    def oauth_token(self):  # -> oauth.Token
        self.subject_jwt_fetches += 1
        return _token(self._seconds_valid, self.subject_jwt_fetches)


def _install(monkeypatch: pytest.MonkeyPatch, provider: _FakeOauthProvider) -> None:
    from databricks.sdk import credentials_provider as cp

    monkeypatch.setattr(cp, "github_oidc", lambda cfg: provider)


def test_cache_engages_within_the_freshness_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """THE point of Tier 2 (spec D5).

    The stock strategy rebuilds ClientCredentials per call, so Refreshable's cache never
    engages and every API call pays a GitHub fetch + an exchange. Two header resolutions
    inside the window must cost exactly ONE exchange.
    """
    from ingestion.databricks_auth import CachedGitHubOidcStrategy

    provider = _FakeOauthProvider(seconds_valid=3600)
    _install(monkeypatch, provider)

    headers = CachedGitHubOidcStrategy()(cast("Config", object()))
    first, second = headers(), headers()

    assert first == second == {"Authorization": "Bearer exchanged-1"}
    assert provider.subject_jwt_fetches == 1, "cache did not engage — Tier 2 is a no-op"


def test_refresh_refetches_the_github_subject_jwt(monkeypatch: pytest.MonkeyPatch) -> None:
    """The trap this design exists to avoid (spec Task 4 Step 1, assertion 2).

    Caching the SDK's ``ClientCredentials`` would be wrong: its ``refresh()``
    (``oauth.py:834+``) re-posts the original ``endpoint_params``, which embed the
    now-expired GitHub subject JWT — so it works until that JWT expires, then fails the
    same way we are fixing. Assert the SUPPLIER was re-invoked, not merely that a token
    came back.
    """
    from ingestion.databricks_auth import CachedGitHubOidcStrategy

    # Shorter than Refreshable's 40s pre-expiry skew (oauth.py:114-119) => already stale.
    provider = _FakeOauthProvider(seconds_valid=5)
    _install(monkeypatch, provider)

    headers = CachedGitHubOidcStrategy()(cast("Config", object()))
    first, second = headers(), headers()

    assert provider.subject_jwt_fetches == 2, "expired token was served from cache, not refreshed"
    assert first != second


def test_strategy_reports_the_github_oidc_auth_type() -> None:
    from ingestion.databricks_auth import CachedGitHubOidcStrategy

    assert CachedGitHubOidcStrategy().auth_type() == "github-oidc"


def test_strategy_raises_when_oidc_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fork PRs / local runs: fail loudly rather than return a header-less provider."""
    from databricks.sdk import credentials_provider as cp

    from ingestion.databricks_auth import CachedGitHubOidcStrategy

    monkeypatch.setattr(cp, "github_oidc", lambda cfg: None)
    with pytest.raises(RuntimeError, match="github-oidc credentials unavailable"):
        CachedGitHubOidcStrategy()(cast("Config", object()))


def test_workspace_client_only_installs_the_strategy_under_oidc(
    clean_env: pytest.MonkeyPatch,
) -> None:
    """Local dev (PAT or an OAuth CLI profile) must get a stock client, untouched."""
    import databricks.sdk as sdk

    from ingestion.databricks_auth import workspace_client

    seen: list[dict[str, object]] = []
    clean_env.setattr(sdk, "WorkspaceClient", lambda **kw: seen.append(kw) or object())

    clean_env.setenv("DATABRICKS_HOST", "https://example.databricks.com")
    clean_env.setenv("DATABRICKS_TOKEN", "x")
    workspace_client()
    assert seen[-1] == {}, "a static-token run must not get the OIDC strategy"

    clean_env.setenv("DATABRICKS_AUTH_TYPE", "github-oidc")
    clean_env.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "x")
    clean_env.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "https://token.actions.example/x")
    workspace_client()
    assert "credentials_strategy" in seen[-1], "OIDC run did not get the caching strategy"
