"""The HTTP client (spec §20; plan Task 14; Review focus 1 and 2)."""

from __future__ import annotations

import gzip
import io
from collections.abc import Mapping
from typing import Any

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from lakehouse.errors import HttpStatusError, IntegrityError, UnexpectedPayloadError
from lakehouse.net.http import HttpClient


class FakeRaw:
    def __init__(self, body: bytes) -> None:
        self._buffer = io.BytesIO(body)

    def read(self, amt: int | None = None, decode_content: bool | None = None) -> bytes:
        assert decode_content is False, "the client must never decode Content-Encoding"
        return self._buffer.read(amt)


class FakeResponse:
    def __init__(self, status: int, body: bytes = b"", headers: Mapping[str, str] | None = None) -> None:
        self.status_code = status
        self.headers: CaseInsensitiveDict[str] = CaseInsensitiveDict(dict(headers or {}))
        self.raw = FakeRaw(body)
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, script: list[FakeResponse | Exception]) -> None:
        self.script = script
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def _next(self, method: str, url: str, kwargs: dict[str, Any]) -> FakeResponse:
        self.calls.append((method, url, kwargs))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._next("GET", url, kwargs)

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._next("POST", url, kwargs)


def _client(script: list[FakeResponse | Exception]) -> tuple[HttpClient, FakeSession, list[float]]:
    session = FakeSession(script)
    waits: list[float] = []
    return HttpClient(session_factory=lambda: session, sleep=waits.append), session, waits  # type: ignore[arg-type, return-value]


def test_rejects_http() -> None:
    client, session, _ = _client([])
    with pytest.raises(ValueError, match="https"):
        client.get("http://example.org/x", max_bytes=10, expect="binary")
    assert session.calls == []


def test_timeouts_set() -> None:
    client, session, _ = _client([FakeResponse(200, b"ok")])
    result = client.get("https://example.org/x", max_bytes=10, expect="text")
    assert (result.status, result.body) == (200, b"ok")
    _, _, kwargs = session.calls[0]
    assert kwargs["timeout"] == (10, 30)
    assert kwargs["verify"] is True
    assert kwargs["stream"] is True
    assert kwargs["allow_redirects"] is False


def test_retry_429_then_ok() -> None:
    client, session, waits = _client([FakeResponse(429), FakeResponse(503), FakeResponse(200, b"ok")])
    assert client.get("https://example.org/x", max_bytes=10, expect="text").body == b"ok"
    assert len(session.calls) == 3
    assert waits == [1.0, 2.0]  # exponential backoff


def test_retry_honours_retry_after() -> None:
    client, _, waits = _client([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, b"ok")])
    client.get("https://example.org/x", max_bytes=10, expect="text")
    assert waits == [7.0]


def test_retry_gives_up_after_3() -> None:
    client, session, waits = _client([FakeResponse(500) for _ in range(4)])
    with pytest.raises(HttpStatusError) as caught:
        client.get("https://example.org/x", max_bytes=10, expect="text")
    assert caught.value.status == 500
    assert len(session.calls) == 4  # the first try and 3 retries
    assert len(waits) == 3


def test_connection_errors_retry() -> None:
    client, session, _ = _client([requests.ConnectionError("reset"), FakeResponse(200, b"ok")])
    assert client.get("https://example.org/x", max_bytes=10, expect="text").body == b"ok"
    assert len(session.calls) == 2


def test_auth_dropped_on_cross_host_redirect() -> None:
    """The pining API answers 302 with a presigned S3 URL; S3 refuses a request with two kinds of auth (§7.2)."""
    client, session, _ = _client(
        [
            FakeResponse(302, headers={"Location": "https://bucket.s3.amazonaws.com/a?X-Amz-Signature=s"}),
            FakeResponse(200, b"\x00data"),
        ]
    )
    result = client.get(
        "https://api.example.org/v1/x",
        headers={"Authorization": "Bearer t", "Accept": "*/*"},
        max_bytes=10,
        expect="binary",
    )
    assert result.final_url == "https://bucket.s3.amazonaws.com/a?X-Amz-Signature=s"
    assert session.calls[0][2]["headers"]["Authorization"] == "Bearer t"
    assert "Authorization" not in session.calls[1][2]["headers"]
    assert session.calls[1][2]["headers"]["Accept"] == "*/*"


def test_auth_kept_on_same_host_redirect() -> None:
    client, session, _ = _client([FakeResponse(301, headers={"Location": "/v2/x"}), FakeResponse(200, b"ok")])
    client.get("https://api.example.org/v1/x", headers={"Authorization": "Bearer t"}, max_bytes=10, expect="text")
    assert session.calls[1][1] == "https://api.example.org/v2/x"
    assert session.calls[1][2]["headers"]["Authorization"] == "Bearer t"


def test_redirect_to_http_refused() -> None:
    client, _, _ = _client([FakeResponse(302, headers={"Location": "http://evil.example.org/"})])
    with pytest.raises(ValueError, match="https"):
        client.get("https://api.example.org/v1/x", max_bytes=10, expect="text")


def test_no_content_encoding_decode() -> None:
    compressed = gzip.compress(b'{"a": 1}')
    client, _, _ = _client([FakeResponse(200, compressed, headers={"Content-Encoding": "gzip"})])
    assert client.get("https://example.org/x.json", max_bytes=1000, expect="json").body == compressed


def test_html_payload_rejected() -> None:
    page = b"<!DOCTYPE html><html><body>Google Drive can't scan this file for viruses.</body></html>"
    for expect in ("binary", "json"):
        client, _, _ = _client([FakeResponse(200, page, headers={"Content-Type": "text/html"})])
        with pytest.raises(UnexpectedPayloadError, match="HTML"):
            client.get("https://example.org/file", max_bytes=1000, expect=expect)  # type: ignore[arg-type]
    client, _, _ = _client([FakeResponse(200, page)])
    assert client.get("https://example.org/page", max_bytes=1000, expect="text").body == page


def test_truncated_body_never_returned() -> None:
    short = [FakeResponse(200, b"x" * 50, headers={"Content-Length": "100"}) for _ in range(4)]
    client, session, _ = _client(list(short))
    with pytest.raises(IntegrityError, match="truncated"):
        client.get("https://example.org/x", max_bytes=1000, expect="binary")
    assert len(session.calls) == 4
    client, _, _ = _client(
        [
            FakeResponse(200, b"x" * 50, headers={"Content-Length": "100"}),
            FakeResponse(200, b"x" * 100, headers={"Content-Length": "100"}),
        ]
    )
    assert len(client.get("https://example.org/x", max_bytes=1000, expect="binary").body) == 100


def test_max_bytes_enforced() -> None:
    client, _, _ = _client([FakeResponse(200, b"x" * 2000)])
    with pytest.raises(IntegrityError, match="max_bytes"):
        client.get("https://example.org/x", max_bytes=1000, expect="binary")


def test_client_errors_returned_not_retried() -> None:
    client, session, _ = _client([FakeResponse(404, b"not found")])
    assert client.get("https://example.org/x", max_bytes=100, expect="json").status == 404
    assert len(session.calls) == 1


def test_post_json() -> None:
    client, session, _ = _client([FakeResponse(200, b'{"ok": true}')])
    result = client.post_json("https://example.org/batch", {"a": 1}, max_bytes=100)
    assert result.body == b'{"ok": true}'
    method, _, kwargs = session.calls[0]
    assert method == "POST"
    assert kwargs["json"] == {"a": 1}
    assert kwargs["timeout"] == (10, 30)


def test_asks_for_identity_encoding() -> None:
    """requests would offer gzip by default; the client asks for the bytes as stored, and never decodes (§7.2)."""
    client, session, _ = _client([FakeResponse(200, b"ok"), FakeResponse(200, b"ok")])
    client.get("https://example.org/x", max_bytes=10, expect="text")
    assert session.calls[0][2]["headers"]["Accept-Encoding"] == "identity"
    client.get("https://example.org/x", headers={"accept-encoding": "br"}, max_bytes=10, expect="text")
    assert session.calls[1][2]["headers"]["accept-encoding"] == "br"
    assert "Accept-Encoding" not in session.calls[1][2]["headers"]
