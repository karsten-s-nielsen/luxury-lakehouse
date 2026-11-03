"""The HF lease ledger: the fence of record for the HF single-writer lease (T6 spec D1).

The HF S3 gateway does not make create-if-absent atomic under concurrency, so on HF a lease generation is won by an HF
Hub commit with `parent_commit`: the server refuses it when the branch head moved, so exactly one of several racers
succeeds. One private dataset repository per lake (`<lake bucket>-control`) holds one file, `lease.json`. Only the
Job's token writes it; readers never read it. The heartbeat stays a bucket write, so a run makes one commit.

`head()` makes at most two requests (`repo_info` for the head sha and its time, and `lease.json` at that sha) and one
when the head is unchanged; it never pages the commit history.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Protocol

from huggingface_hub import CommitOperationAdd
from huggingface_hub.errors import EntryNotFoundError, HfHubHTTPError, RemoteEntryNotFoundError, RepositoryNotFoundError
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from lakehouse.errors import ConfigError, IntegrityError, LedgerConflictError, LedgerUnavailableError

__all__ = ["LEDGER_FILE", "PARENT_CHECK_STATUS", "HubLedger", "LeaseLedger", "LedgerApi", "LedgerEntry", "LedgerHead"]

LEDGER_FILE: Final = "lease.json"
REPO_TYPE: Final = "dataset"
# The HTTP status of a commit whose `parent_commit` is no longer the head, measured by the T6 spike's commit probe (plan
# Task 1, HF Job 6ae6b2950e0338d1bb7d5908, 2026-11-01): 20 overlapped 3-way rounds, exactly one success each, all 40
# losers HTTP 409. Any other status propagates, and the lease decides the outcome by reading the head.
PARENT_CHECK_STATUS: Final = frozenset({409})
RATE_LIMITED: Final = 429


class LedgerEntry(BaseModel):
    """The ledger's `lease.json`: who won which generation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    generation: int = Field(ge=1)
    holder_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    acquired_at: AwareDatetime


@dataclass(frozen=True)
class LedgerHead:
    """The ledger's head: its commit sha, the entry at that commit (`None` on a fresh ledger), and the commit's
    server time (the 12-minute rule of D1 step 3 counts from it)."""

    sha: str
    entry: LedgerEntry | None
    committed_at: datetime


class LeaseLedger(Protocol):
    def head(self) -> LedgerHead: ...

    def claim(self, entry: LedgerEntry, *, parent: str) -> str:
        """Commit `entry` on top of `parent`; return the new sha. `LedgerConflictError` when the head moved,
        `LedgerUnavailableError` on HTTP 429; every other error propagates. Named `claim`, not `commit`: a
        `.commit(...)` call with arguments is the catalog-publish shape that `test_no_direct_publish` gates."""
        ...


class LedgerApi(Protocol):
    """The part of `huggingface_hub.HfApi` (2.3.0) that `HubLedger` uses, with the same signatures."""

    def repo_info(self, repo_id: str, *, repo_type: str | None = ...) -> Any: ...

    def hf_hub_download(
        self, repo_id: str, filename: str, *, repo_type: str | None = ..., revision: str | None = ...
    ) -> Any: ...

    def create_commit(
        self,
        repo_id: str,
        operations: Sequence[CommitOperationAdd],
        *,
        commit_message: str,
        parent_commit: str | None = ...,
        repo_type: str | None = ...,
    ) -> Any: ...


def _status(exc: HfHubHTTPError) -> int:
    return exc.response.status_code


class HubLedger:
    """`LeaseLedger` over a private HF dataset repository."""

    def __init__(self, api: LedgerApi, repo_id: str) -> None:
        self._api = api
        self._repo = repo_id
        self._cached: tuple[str, LedgerEntry | None] | None = None

    @property
    def repo_id(self) -> str:
        return self._repo

    def check(self) -> None:
        """Fail before any work when the control repository is missing or this token cannot see it."""
        try:
            self._api.repo_info(self._repo, repo_type=REPO_TYPE)
        except RepositoryNotFoundError as exc:
            raise ConfigError(
                f"the lease ledger repository {self._repo} does not exist or this token cannot read it"
            ) from exc

    def head(self) -> LedgerHead:
        info = self._api.repo_info(self._repo, repo_type=REPO_TYPE)
        if info.sha is None or info.last_modified is None:
            raise IntegrityError(f"the lease ledger {self._repo} reported no head commit")
        if self._cached is not None and self._cached[0] == info.sha:
            entry = self._cached[1]
        else:
            entry = self._read(info.sha)
            self._cached = (info.sha, entry)
        return LedgerHead(sha=info.sha, entry=entry, committed_at=info.last_modified)

    def _read(self, sha: str) -> LedgerEntry | None:
        try:
            path = self._api.hf_hub_download(self._repo, LEDGER_FILE, repo_type=REPO_TYPE, revision=sha)
        except (EntryNotFoundError, RemoteEntryNotFoundError):
            return None  # a fresh ledger: no generation committed yet
        try:
            return LedgerEntry.model_validate_json(Path(str(path)).read_bytes())
        except ValidationError as exc:
            raise IntegrityError(f"the lease ledger's {LEDGER_FILE} at {sha} is not readable: {exc}") from exc

    def claim(self, entry: LedgerEntry, *, parent: str) -> str:
        operation = CommitOperationAdd(path_in_repo=LEDGER_FILE, path_or_fileobj=entry.model_dump_json().encode())
        try:
            info = self._api.create_commit(
                self._repo,
                operations=[operation],
                commit_message=f"lease generation {entry.generation} ({entry.run_id})",
                parent_commit=parent,
                repo_type=REPO_TYPE,
            )
        except HfHubHTTPError as exc:
            status = _status(exc)
            if status in PARENT_CHECK_STATUS:
                raise LedgerConflictError(
                    f"lease ledger commit of generation {entry.generation}: the head moved past {parent}"
                ) from exc
            if status == RATE_LIMITED:
                raise LedgerUnavailableError(f"lease ledger commit refused: HTTP {RATE_LIMITED} (rate limit)") from exc
            raise
        return str(info.oid)
