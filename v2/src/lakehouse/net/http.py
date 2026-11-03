"""The one HTTP client of v2 (spec §7.2, §20).

Rules: HTTPS only, `verify=True`, timeouts `(10, 30)`, at most 3 retries with exponential backoff on 429, 5xx,
connection errors and truncated bodies; `Retry-After` is honoured. The body is read raw from the stream, so a
`Content-Encoding` is never decoded, and the client asks for `Accept-Encoding: identity` unless the caller sets an
encoding: wrappers are detected later by magic bytes (§13.1). Redirects are followed by
hand, and `Authorization` is dropped on every cross-host redirect (the pining API's 302 to a presigned S3 URL).
`expect` checks the payload kind: an HTML page where binary or JSON data is expected raises
`UnexpectedPayloadError` and is never parsed as data. Error messages show URLs without their query string.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal, Protocol, cast
from urllib.parse import urljoin, urlsplit

import requests
from requests.structures import CaseInsensitiveDict

from lakehouse.errors import HttpStatusError, IntegrityError, UnexpectedPayloadError
from lakehouse.logging import get_logger

__all__ = ["Expect", "HttpClient", "HttpResult", "safe_url"]

Expect = Literal["binary", "json", "text"]

TIMEOUTS: Final = (10, 30)
MAX_RETRIES: Final = 3
MAX_REDIRECTS: Final = 5
MAX_RETRY_AFTER_S: Final = 60.0
_BACKOFF_S: Final = 1.0
_CHUNK: Final = 1024 * 1024
_REDIRECTS: Final = frozenset({301, 302, 303, 307, 308})
_HTML_STARTS: Final = (b"<!doctype html", b"<html", b"<head", b"<body")

_log = get_logger(__name__)


class _Raw(Protocol):
    def read(self, amt: int | None = None, decode_content: bool | None = None) -> bytes: ...


class _Response(Protocol):
    status_code: int
    headers: Mapping[str, str]

    @property
    def raw(self) -> _Raw: ...

    def close(self) -> None: ...


class _Session(Protocol):
    def get(self, url: str, **kwargs: Any) -> _Response: ...
    def post(self, url: str, **kwargs: Any) -> _Response: ...


@dataclass(frozen=True)
class HttpResult:
    status: int
    body: bytes
    headers: Mapping[str, str]
    final_url: str


class _Transient(Exception):  # noqa: N818 - an internal retry signal, never raised to callers
    def __init__(self, reason: str, *, wait_s: float | None = None, error: Exception | None = None) -> None:
        super().__init__(reason)
        self.wait_s = wait_s
        self.error = error


def safe_url(url: str) -> str:
    """The URL without its query string and fragment (a presigned URL holds a signature there)."""
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}{parts.path}"


def _require_https(url: str) -> None:
    if urlsplit(url).scheme != "https":
        raise ValueError(f"only https URLs are allowed, not {safe_url(url)}")


def _looks_like_html(body: bytes) -> bool:
    head = body[:512].lstrip().lower()
    return head.startswith(_HTML_STARTS)


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # an HTTP date: fall back to the backoff


class HttpClient:
    """GET (and the one JSON POST that Git LFS needs) under the v2 rules."""

    def __init__(
        self,
        session_factory: Callable[[], requests.Session] = requests.Session,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        # the requests stubs spell out every keyword of Session.get; the client uses the protocol's subset
        self._session = cast("_Session", session_factory())
        self._sleep = sleep

    def wait(self, seconds: float) -> None:
        """Wait, with the client's clock (tests inject it): for a caller that honours a `Retry-After` itself."""
        self._sleep(seconds)

    def get(self, url: str, *, headers: Mapping[str, str] | None = None, max_bytes: int, expect: Expect) -> HttpResult:
        return self._request("GET", url, headers=headers or {}, max_bytes=max_bytes, expect=expect, json_body=None)

    def post_json(
        self, url: str, body: Mapping[str, Any], *, headers: Mapping[str, str] | None = None, max_bytes: int
    ) -> HttpResult:
        return self._request("POST", url, headers=headers or {}, max_bytes=max_bytes, expect="json", json_body=body)

    def _request(
        self,
        method: Literal["GET", "POST"],
        url: str,
        *,
        headers: Mapping[str, str],
        max_bytes: int,
        expect: Expect,
        json_body: Mapping[str, Any] | None,
    ) -> HttpResult:
        _require_https(url)
        failure = _Transient("no attempt yet")
        for attempt in range(MAX_RETRIES + 1):
            if attempt:
                wait = failure.wait_s if failure.wait_s is not None else _BACKOFF_S * 2 ** (attempt - 1)
                if wait > MAX_RETRY_AFTER_S:
                    break  # a long Retry-After: report it instead of blocking the run
                self._sleep(wait)
            try:
                return self._once(method, url, headers=headers, max_bytes=max_bytes, expect=expect, json_body=json_body)
            except _Transient as transient:
                failure = transient
                _log.warning(
                    "http retry", extra={"url": safe_url(url), "attempt": attempt + 1, "reason": str(transient)}
                )
        if isinstance(failure.error, HttpStatusError):
            raise failure.error
        raise IntegrityError(f"{method} {safe_url(url)} failed after {MAX_RETRIES} retries: {failure}")

    def _once(
        self,
        method: Literal["GET", "POST"],
        url: str,
        *,
        headers: Mapping[str, str],
        max_bytes: int,
        expect: Expect,
        json_body: Mapping[str, Any] | None,
    ) -> HttpResult:
        current, sent = url, dict(headers)
        if not any(name.lower() == "accept-encoding" for name in sent):
            sent["Accept-Encoding"] = "identity"  # the bytes as stored; requests would offer gzip and deflate
        for _ in range(MAX_REDIRECTS + 1):
            kwargs: dict[str, Any] = {
                "headers": sent,
                "timeout": TIMEOUTS,
                "verify": True,
                "stream": True,
                "allow_redirects": False,
            }
            try:
                if method == "POST":
                    response = self._session.post(current, json=json_body, **kwargs)
                else:
                    response = self._session.get(current, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                raise _Transient(f"{type(exc).__name__}: {exc}") from exc
            try:
                status = response.status_code
                if status in _REDIRECTS:
                    location = response.headers.get("Location")
                    if not location:
                        raise HttpStatusError(status, safe_url(current), "a redirect without Location")
                    target = urljoin(current, location)
                    _require_https(target)
                    if urlsplit(target).netloc.lower() != urlsplit(current).netloc.lower():
                        sent = {k: v for k, v in sent.items() if k.lower() != "authorization"}
                    current = target
                    continue
                if status == 429 or status >= 500:
                    error = HttpStatusError(status, safe_url(current))
                    raise _Transient(f"HTTP {status}", wait_s=_retry_after(response.headers), error=error)
                body = self._read(response, max_bytes=max_bytes, url=current)
                result = HttpResult(
                    status=status, body=body, headers=CaseInsensitiveDict(dict(response.headers)), final_url=current
                )
            finally:
                response.close()
            if 200 <= status < 300 and expect != "text" and _looks_like_html(body):
                raise UnexpectedPayloadError(f"{safe_url(current)} answered with an HTML page, not {expect} data")
            return result
        raise HttpStatusError(310, safe_url(url), f"more than {MAX_REDIRECTS} redirects")

    @staticmethod
    def _read(response: _Response, *, max_bytes: int, url: str) -> bytes:
        chunks: list[bytes] = []
        total = 0
        try:
            while chunk := response.raw.read(_CHUNK, decode_content=False):
                total += len(chunk)
                if total > max_bytes:
                    raise IntegrityError(f"{safe_url(url)} sent more than max_bytes={max_bytes}")
                chunks.append(chunk)
        except (requests.exceptions.ChunkedEncodingError, requests.ConnectionError) as exc:
            raise _Transient(f"the body was cut off: {exc}") from exc
        body = b"".join(chunks)
        declared = response.headers.get("Content-Length")
        if declared is not None and declared.isdigit() and int(declared) != len(body):
            raise _Transient(f"truncated body: {len(body)} of {declared} bytes")
        return body
