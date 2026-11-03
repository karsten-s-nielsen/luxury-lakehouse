"""The HF lease ledger seam (T6 spec D1, plan Task 3): `HubLedger` over a fake `HfApi`.

`head()` reads the head commit (`repo_info`: sha and `last_modified`) and `lease.json` at that sha, with at most two
requests and one when the head is unchanged; it never pages the commit history. `commit()` is a `create_commit` with
`parent_commit`: a parent-check failure is `LedgerConflictError`, HTTP 429 is `LedgerUnavailableError`, every other
error propagates.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx2
import pytest
from huggingface_hub.errors import HfHubHTTPError, RemoteEntryNotFoundError, RepositoryNotFoundError

from lakehouse.errors import ConfigError, IntegrityError, LedgerConflictError, LedgerUnavailableError
from lakehouse.storage import hf
from lakehouse.storage import ledger as ledger_module
from lakehouse.storage.ledger import HubLedger, LedgerEntry
from lakehouse.storage.roots import open_storage, resolve_roots

REPO = "ns/lake-test-control"
T = datetime(2026, 11, 1, 2, 0, tzinfo=UTC)


def _http_error(cls: type[HfHubHTTPError], status: int) -> HfHubHTTPError:
    request = httpx2.Request("POST", "https://huggingface.co/api/datasets/ns/lake-test-control/commit/main")
    return cls(f"HTTP {status}", response=httpx2.Response(status, request=request))


@dataclass
class _Info:
    sha: str | None
    last_modified: datetime | None


@dataclass
class _Commit:
    oid: str


@dataclass
class FakeApi:
    tmp: Path
    files: dict[str, bytes] = field(default_factory=dict[str, bytes])  # sha -> lease.json bytes
    head_sha: str = "c0"
    calls: list[str] = field(default_factory=list[str])
    commit_error: BaseException | None = None
    missing_repo: bool = False

    def repo_info(self, repo_id: str, *, repo_type: str) -> _Info:
        self.calls.append("repo_info")
        assert (repo_id, repo_type) == (REPO, "dataset")
        if self.missing_repo:
            raise _http_error(RepositoryNotFoundError, 404)
        return _Info(sha=self.head_sha, last_modified=T)

    def hf_hub_download(self, repo_id: str, filename: str, *, repo_type: str, revision: str) -> str:
        self.calls.append("hf_hub_download")
        assert (repo_id, filename, repo_type) == (REPO, "lease.json", "dataset")
        if revision not in self.files:
            raise _http_error(RemoteEntryNotFoundError, 404)
        path = self.tmp / f"{revision}.json"
        path.write_bytes(self.files[revision])
        return str(path)

    def create_commit(self, repo_id: str, **kw: Any) -> _Commit:
        self.calls.append("create_commit")
        if self.commit_error is not None:
            raise self.commit_error
        if kw["parent_commit"] != self.head_sha:
            raise _http_error(HfHubHTTPError, min(ledger_module.PARENT_CHECK_STATUS))
        new = f"c{len(self.files) + 1}"
        [operation] = kw["operations"]
        self.files[new] = operation.path_or_fileobj
        self.head_sha = new
        return _Commit(oid=new)

    def list_repo_commits(self, *args: Any, **kw: Any) -> list[Any]:
        raise AssertionError("head() must never page the commit history")


def _entry(generation: int = 1) -> LedgerEntry:
    return LedgerEntry(generation=generation, holder_id="h", run_id=f"r{generation}", acquired_at=T)


def test_head_of_an_empty_repo_has_no_entry(tmp_path: Path) -> None:
    head = HubLedger(FakeApi(tmp_path), REPO).head()  # type: ignore[arg-type]
    assert (head.sha, head.entry, head.committed_at) == ("c0", None, T)


def test_head_reads_the_entry_at_its_sha(tmp_path: Path) -> None:
    api = FakeApi(tmp_path)
    ledger = HubLedger(api, REPO)  # type: ignore[arg-type]
    sha = ledger.claim(_entry(3), parent="c0")
    head = ledger.head()
    assert head.sha == sha and head.entry == _entry(3)


def test_head_is_bounded(tmp_path: Path) -> None:
    api = FakeApi(tmp_path, files={"c0": _entry(1).model_dump_json().encode()})
    ledger = HubLedger(api, REPO)  # type: ignore[arg-type]
    ledger.head()
    assert api.calls == ["repo_info", "hf_hub_download"]
    ledger.head()
    assert api.calls == ["repo_info", "hf_hub_download", "repo_info"]  # an unchanged head reads no file


def test_unparsable_entry_is_an_integrity_error(tmp_path: Path) -> None:
    api = FakeApi(tmp_path, files={"c0": b'{"generation": "not a number"}'})
    with pytest.raises(IntegrityError, match=r"lease\.json"):
        HubLedger(api, REPO).head()  # type: ignore[arg-type]


def test_parent_check_failure_is_a_conflict(tmp_path: Path) -> None:
    api = FakeApi(tmp_path)
    ledger = HubLedger(api, REPO)  # type: ignore[arg-type]
    ledger.claim(_entry(1), parent="c0")
    with pytest.raises(LedgerConflictError):
        ledger.claim(_entry(2), parent="c0")  # the head moved on


def test_rate_limit_is_unavailable(tmp_path: Path) -> None:
    api = FakeApi(tmp_path, commit_error=_http_error(HfHubHTTPError, 429))
    with pytest.raises(LedgerUnavailableError, match="429"):
        HubLedger(api, REPO).claim(_entry(1), parent="c0")  # type: ignore[arg-type]


@pytest.mark.parametrize("status", [412, 503])
def test_other_errors_propagate(tmp_path: Path, status: int) -> None:
    """Only the parent-check status the Task 1 probe measured (409, 40 of 40 losers) is a conflict; any other status,
    412 included, propagates and the lease decides the outcome by reading the head."""
    api = FakeApi(tmp_path, commit_error=_http_error(HfHubHTTPError, status))
    with pytest.raises(HfHubHTTPError):
        HubLedger(api, REPO).claim(_entry(1), parent="c0")  # type: ignore[arg-type]


def test_missing_control_repo_fails_at_check(tmp_path: Path) -> None:
    api = FakeApi(tmp_path, missing_repo=True)
    with pytest.raises(ConfigError, match=REPO):
        HubLedger(api, REPO).check()  # type: ignore[arg-type]


def test_missing_control_repo_fails_at_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`open_storage` checks the control repository before any work (Review Focus 4)."""

    def fake_api(token: object) -> FakeApi:
        return FakeApi(tmp_path, missing_repo=True)

    def fake_s3(key: object) -> object:
        return object()

    monkeypatch.setattr(hf, "_default_api", fake_api)
    monkeypatch.setattr(hf, "_default_s3_client", fake_s3)
    env = {
        "HF_TOKEN": "hf_faketoken",
        "HF_S3_ACCESS_KEY_ID": "AKIAEXAMPLEKEY",
        "HF_S3_SECRET_ACCESS_KEY": "example-secret-value",
        "LAKEHOUSE_LAKE_ROOT": "hf://buckets/ns/lake-test",
        "LAKEHOUSE_RESTRICTED_ROOT": "hf://buckets/ns/restricted-test",
        "LAKEHOUSE_LAKE_S3_PATH": "s3://ns/lake-test",
        "LAKEHOUSE_RESTRICTED_S3_PATH": "s3://ns/restricted-test",
        "LAKEHOUSE_CONTROL_REPO": REPO,
    }
    resolved = resolve_roots(cli_lake=None, cli_restricted=None, env=env, bootstrap_file=tmp_path / "x")
    with pytest.raises(ConfigError, match=REPO):
        open_storage(resolved, env)


def test_entry_round_trips_as_json() -> None:
    assert LedgerEntry.model_validate_json(json.dumps(json.loads(_entry(2).model_dump_json()))) == _entry(2)
