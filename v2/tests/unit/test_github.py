"""The GitHub client (spec §7.1; plan Task 14; Review focus 3)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest
from test_http import FakeResponse, FakeSession

from lakehouse.errors import IntegrityError, RateLimitedError
from lakehouse.net.http import HttpClient
from lakehouse.sources.github import GitHubClient, TreeEntry

TREE = {
    "sha": "abc",
    "truncated": False,
    "tree": [
        {"path": "data", "type": "tree", "sha": "t1"},
        {"path": "data/matches/9/281.json", "type": "blob", "sha": "b1", "size": 120},
        {"path": "data/matches/1899585/tracking.jsonl", "type": "blob", "sha": "b2", "size": 133},
    ],
}
LFS_OID = hashlib.sha256(b"tracking bytes").hexdigest()
POINTER = f"version https://git-lfs.github.com/spec/v1\noid sha256:{LFS_OID}\nsize 14\n".encode()


def _github(
    script: list[FakeResponse | Exception], token: str | None = None
) -> tuple[GitHubClient, FakeSession, list[float]]:
    session = FakeSession(script)
    waits: list[float] = []
    http = HttpClient(session_factory=lambda: session, sleep=waits.append)  # type: ignore[arg-type, return-value]
    return GitHubClient(http, token), session, waits


def test_tree_lists_blobs() -> None:
    client, session, _ = _github([FakeResponse(200, json.dumps(TREE).encode())])
    entries = client.tree("statsbomb/open-data", "master")
    assert entries == [
        TreeEntry(path="data/matches/9/281.json", blob_sha="b1", size=120, lfs_oid=None, lfs_size=None),
        TreeEntry(path="data/matches/1899585/tracking.jsonl", blob_sha="b2", size=133, lfs_oid=None, lfs_size=None),
    ]
    assert session.calls[0][1] == "https://api.github.com/repos/statsbomb/open-data/git/trees/master?recursive=1"


def test_tree_reads_lfs_pointers() -> None:
    client, _, _ = _github([FakeResponse(200, json.dumps(TREE).encode()), FakeResponse(200, POINTER)])
    entries = client.tree("SkillCorner/opendata", "master", lfs=lambda path: path.endswith(".jsonl"))
    assert entries[1].lfs_oid == LFS_OID
    assert entries[1].lfs_size == 14


def test_truncated_tree_fails() -> None:
    client, _, _ = _github([FakeResponse(200, json.dumps({**TREE, "truncated": True}).encode())])
    with pytest.raises(IntegrityError, match="truncated"):
        client.tree("statsbomb/open-data", "master")


FAKE_TOKEN = "ghp_secretvalue123"  # noqa: S105 - a fake test value


def test_github_token_used_when_set() -> None:
    client, session, _ = _github([FakeResponse(200, json.dumps(TREE).encode())], token=FAKE_TOKEN)
    client.tree("statsbomb/open-data", "master")
    assert session.calls[0][2]["headers"]["Authorization"] == "Bearer ghp_secretvalue123"
    assert "ghp_secretvalue123" not in repr(client)
    anonymous, session, _ = _github([FakeResponse(200, json.dumps(TREE).encode())])
    anonymous.tree("statsbomb/open-data", "master")
    assert "Authorization" not in session.calls[0][2]["headers"]


def test_github_rate_limit_backoff_and_status() -> None:
    reset = int(datetime(2026, 10, 26, 13, 0, tzinfo=UTC).timestamp())
    limited = FakeResponse(
        403,
        b'{"message": "API rate limit exceeded"}',
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset)},
    )
    client, _, _ = _github([limited])
    with pytest.raises(RateLimitedError) as caught:
        client.tree("statsbomb/open-data", "master")
    assert client.rate_limited_until == datetime(2026, 10, 26, 13, 0, tzinfo=UTC)
    assert caught.value.until == client.rate_limited_until
    assert "rate limit" in str(caught.value)


def test_github_short_retry_after_waits() -> None:
    client, _, waits = _github(
        [
            FakeResponse(403, b"{}", headers={"Retry-After": "3"}),
            FakeResponse(200, json.dumps(TREE).encode()),
        ]
    )
    assert len(client.tree("statsbomb/open-data", "master")) == 2
    assert waits == [3.0]
    assert client.rate_limited_until is None


def test_raw_file() -> None:
    client, session, _ = _github([FakeResponse(200, b"[]")])
    assert client.raw("statsbomb/open-data", "master", "data/competitions.json") == b"[]"
    assert session.calls[0][1] == "https://raw.githubusercontent.com/statsbomb/open-data/master/data/competitions.json"


def test_lfs_object_download_and_check() -> None:
    batch = {
        "objects": [
            {
                "oid": LFS_OID,
                "size": 14,
                "actions": {
                    "download": {"href": "https://github-cloud.s3.amazonaws.com/obj?sig=1", "header": {"X-Extra": "1"}}
                },
            }
        ]
    }
    client, session, _ = _github([FakeResponse(200, json.dumps(batch).encode()), FakeResponse(200, b"tracking bytes")])
    assert client.lfs_object("SkillCorner/opendata", LFS_OID, 14) == b"tracking bytes"
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", "https://github.com/SkillCorner/opendata.git/info/lfs/objects/batch")
    assert kwargs["json"]["objects"] == [{"oid": LFS_OID, "size": 14}]
    assert session.calls[1][2]["headers"]["X-Extra"] == "1"
    bad, _, _ = _github([FakeResponse(200, json.dumps(batch).encode()), FakeResponse(200, b"other bytes!!!")])
    with pytest.raises(IntegrityError, match="sha256"):
        bad.lfs_object("SkillCorner/opendata", LFS_OID, 14)
