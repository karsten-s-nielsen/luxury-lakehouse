"""The GitHub client for the public repositories (spec §7.1): the git tree, raw files and Git LFS objects.

One tree call lists every file with its blob SHA, the change signal of StatsBomb and SkillCorner. A file kept in
Git LFS is a small pointer in the tree; its `oid sha256` is the change signal and names the object. Rate limits are
honoured: a short `Retry-After` is waited out, and an exhausted limit (`X-RateLimit-Remaining: 0`) sets
`rate_limited_until` and raises `RateLimitedError`, which the run reports visibly.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, cast
from urllib.parse import quote

from lakehouse.errors import IntegrityError, RateLimitedError
from lakehouse.net.http import MAX_RETRY_AFTER_S, Expect, HttpClient, HttpResult

__all__ = ["GitHubClient", "TreeEntry"]

API: Final = "https://api.github.com"
RAW: Final = "https://raw.githubusercontent.com"
MAX_TREE_BYTES: Final = 64 * 1024 * 1024
MAX_FILE_BYTES: Final = 2 * 1024**3
_POINTER_MAX: Final = 1024
_POINTER: Final = re.compile(
    rb"^version https://git-lfs\.github\.com/spec/v1\noid sha256:([0-9a-f]{64})\nsize (\d+)\n?$"
)
_REPO: Final = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


@dataclass(frozen=True)
class TreeEntry:
    path: str
    blob_sha: str
    size: int
    lfs_oid: str | None
    lfs_size: int | None


def _check_repo(repo: str) -> str:
    if not _REPO.match(repo):
        raise ValueError(f"invalid repository {repo!r}: expected <owner>/<name>")
    return repo


class GitHubClient:
    def __init__(self, http: HttpClient, token: str | None) -> None:
        self._http = http
        self._token = token
        self.rate_limited_until: datetime | None = None

    def __repr__(self) -> str:
        return f"GitHubClient(token={'set' if self._token else 'none'}, rate_limited_until={self.rate_limited_until})"

    def _headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        headers.update(extra or {})
        return headers

    def _checked(self, call: Callable[[], HttpResult], url: str) -> HttpResult:
        """Run `call`; wait out a short `Retry-After` once, and turn an exhausted limit into `RateLimitedError`."""
        result = call()
        if result.status in (403, 429):
            wait = result.headers.get("Retry-After")
            if wait is not None and wait.isdigit() and float(wait) <= MAX_RETRY_AFTER_S:
                self._http.wait(float(wait))
                result = call()
            if result.status in (403, 429) and (
                result.headers.get("X-RateLimit-Remaining") == "0" or result.headers.get("Retry-After") is not None
            ):
                reset = result.headers.get("X-RateLimit-Reset")
                self.rate_limited_until = (
                    datetime.fromtimestamp(int(reset), UTC) if reset is not None and reset.isdigit() else None
                )
                raise RateLimitedError(result.status, url, self.rate_limited_until)
        if result.status != 200:
            raise IntegrityError(f"GitHub answered HTTP {result.status} for {url}")
        return result

    def _get(self, url: str, *, max_bytes: int, expect: Expect, headers: Mapping[str, str] | None = None) -> bytes:
        return self._checked(
            lambda: self._http.get(url, headers=self._headers(headers), max_bytes=max_bytes, expect=expect), url
        ).body

    def tree(self, repo: str, ref: str, *, lfs: Callable[[str], bool] | None = None) -> list[TreeEntry]:
        """Every blob of `ref`. For paths that `lfs` selects, the LFS pointer is read to fill `lfs_oid` and
        `lfs_size`."""
        url = f"{API}/repos/{_check_repo(repo)}/git/trees/{quote(ref, safe='')}?recursive=1"
        payload = cast("dict[str, Any]", json.loads(self._get(url, max_bytes=MAX_TREE_BYTES, expect="json")))
        if payload.get("truncated"):
            raise IntegrityError(f"the GitHub tree of {repo}@{ref} is truncated: the listing would be incomplete")
        entries: list[TreeEntry] = []
        for item in cast("list[dict[str, Any]]", payload.get("tree", [])):
            if item.get("type") != "blob":
                continue
            path, size = str(item["path"]), int(item.get("size", 0))
            oid: str | None = None
            lfs_size: int | None = None
            if lfs is not None and lfs(path) and size <= _POINTER_MAX:
                pointer = _POINTER.match(self.raw(repo, ref, path).replace(b"\r\n", b"\n"))
                if pointer is not None:
                    oid, lfs_size = pointer.group(1).decode(), int(pointer.group(2))
            entries.append(TreeEntry(path=path, blob_sha=str(item["sha"]), size=size, lfs_oid=oid, lfs_size=lfs_size))
        return entries

    def raw(self, repo: str, ref: str, path: str) -> bytes:
        url = f"{RAW}/{_check_repo(repo)}/{quote(ref, safe='')}/{quote(path)}"
        return self._get(url, max_bytes=MAX_FILE_BYTES, expect="binary")

    def lfs_object(self, repo: str, oid: str, size: int) -> bytes:
        """Download a Git LFS object through the batch API and check its SHA-256 and size."""
        url = f"https://github.com/{_check_repo(repo)}.git/info/lfs/objects/batch"
        request = {"operation": "download", "transfers": ["basic"], "objects": [{"oid": oid, "size": size}]}
        headers = {"Accept": "application/vnd.git-lfs+json", "Content-Type": "application/vnd.git-lfs+json"}
        answer = self._checked(
            lambda: self._http.post_json(url, request, headers=self._headers(headers), max_bytes=1024 * 1024), url
        )
        objects = cast("list[dict[str, Any]]", json.loads(answer.body).get("objects", []))
        found = next((o for o in objects if o.get("oid") == oid), None)
        if found is None or "error" in found:
            raise IntegrityError(f"Git LFS has no object {oid} in {repo}: {found and found.get('error')}")
        download = cast("dict[str, Any]", found["actions"]["download"])
        data = self._http.get(
            str(download["href"]),
            headers={str(k): str(v) for k, v in cast("dict[str, Any]", download.get("header", {})).items()},
            max_bytes=max(size, 1),
            expect="binary",
        ).body
        if len(data) != size or hashlib.sha256(data).hexdigest() != oid:
            raise IntegrityError(f"the Git LFS object {oid} does not match its sha256 and size")
        return data
