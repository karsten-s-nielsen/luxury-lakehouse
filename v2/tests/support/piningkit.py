"""Pining test helpers: a fake pining API, synthetic owner-tier entries and an adapter factory (plan Task 20).

Owner-tier entries are synthetic (invented ids and names on the observed field set), because private entries never
enter the repository (spec §23.4).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from requests.structures import CaseInsensitiveDict

from lakehouse.config.model import AllClause, ModuleConfig
from lakehouse.errors import IntegrityError
from lakehouse.net.http import Expect, HttpResult
from lakehouse.sources.pining import PiningAdapter, PiningToken

BASE = "https://pining.example.test/v1"
NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
OWNER = PiningToken.of("owner-token-value-for-tests", "owner")  # pragma: allowlist secret - a test value


def result(body: bytes, status: int = 200) -> HttpResult:
    return HttpResult(status=status, body=body, headers=CaseInsensitiveDict({}), final_url="")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def owner_entry(match_id: str, artifacts: Mapping[str, bytes], **fields: Any) -> dict[str, Any]:
    """A synthetic owner-tier entry with artifact_meta computed from the given bytes."""
    entry: dict[str, Any] = {
        "id": match_id,
        "artifacts": {key: f"{key}.json" for key in artifacts},
        "artifact_meta": {key: {"sha256": sha(data), "size": len(data)} for key, data in artifacts.items()},
        "visibility": "private",
        "updated_at": "2026-10-20T10:00:00Z",
        "date": "2026-01-01",
        "home": "Home FC",
        "away": "Away FC",
        "provenance": "original",
        "source": {"name": "Example source", "url": "https://example.test/", "licence": "Restricted"},
    }
    entry.update(fields)
    return entry


class FakePining:
    """Serves listings and artifact bytes by URL; a 401 queue simulates a token rotation. Like the real client, a
    body longer than `max_bytes` fails with `IntegrityError`."""

    def __init__(self, provider: str, entries: list[dict[str, Any]], files: Mapping[tuple[str, str], bytes]) -> None:
        self.provider = provider
        self.entries = entries
        self.files = dict(files)
        self.unauthorised = 0
        self.not_found: set[tuple[str, str]] = set()
        self.calls: list[tuple[str, Mapping[str, str]]] = []
        self.listed_provider = provider  # the provider that the match list names

    def get(self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect) -> HttpResult:
        self.calls.append((url, dict(headers or {})))
        if self.unauthorised:
            self.unauthorised -= 1
            return result(b'{"error": "Invalid token"}', 401)
        prefix = f"{BASE}/{self.provider}/matches"
        if url == prefix or url.startswith(prefix + "?"):
            return result(json.dumps({"provider": self.listed_provider, "matches": self.entries}).encode())
        if url == f"{BASE}/{self.provider}/players":
            return result(json.dumps({"provider": self.provider, "players": []}).encode())
        match_id, _, key = url.removeprefix(prefix + "/").partition("/")
        if (match_id, key) in self.not_found or (match_id, key) not in self.files:
            return result(b'{"error": "Match not found"}', 404)
        data = self.files[(match_id, key)]
        if len(data) > max_bytes:
            raise IntegrityError(f"{url} sent more than max_bytes={max_bytes}")
        return result(data)


def adapter(
    http: FakePining,
    *clauses: Any,
    token: PiningToken = OWNER,
    sleep: Callable[[float], None] | None = None,
) -> PiningAdapter:
    module = ModuleConfig(module_id=f"{http.provider}_pining", active=True, filter=list(clauses) or [AllClause()])  # type: ignore[arg-type]
    return PiningAdapter(
        http.provider,  # type: ignore[arg-type]
        http=http,
        base_url=BASE,
        token=token,
        module=module,
        clock=lambda: NOW,
        sleep=sleep or (lambda _: None),
    )
