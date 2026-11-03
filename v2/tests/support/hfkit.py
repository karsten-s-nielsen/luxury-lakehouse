"""Shared helpers for the live HF tests (plan Task 33): the per-session run id, the object-prefix cleanup and the
test-lake safety guard. The fixtures live in `tests/hf/conftest.py`; this module holds the pure helpers the hf test
files import (it is on the pytest path, and on pyright's `extraPaths`).

Every test object lives under a prefix namespaced by `RUN`, so a cleanup never removes another run's objects. The
committing and sweep tests run only against a throwaway `*-test` bucket pair, never the production lake:
`require_test_lake` is the guard that refuses otherwise (owner decision, 2026-10-29).
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime
from typing import Final

import pytest

from lakehouse.storage.base import Storage
from lakehouse.storage.keys import ROOTS, Root
from lakehouse.storage.lease import LeaseState, lease_key

RUN: Final = uuid.uuid4().hex[:12]
MIB: Final = 1024 * 1024
TEST_LAKE_ENV: Final = "LAKEHOUSE_HF_TEST_LAKE"

# Prefixes the hf_job tests write under (both roots); every one is namespaced by RUN.
JOB_PREFIXES: Final[tuple[str, ...]] = (
    f"data/_live/{RUN}/",
    f"raw/_live/{RUN}/",
    f"runs/lease/_live-{RUN}/",
    f"catalog/v/_live-{RUN}/",
    f"runs/_live-{RUN}/",
)
# Prefixes the hf_space tests write under (lake root, HfApi channel only).
SPACE_PREFIXES: Final[tuple[str, ...]] = (f"config/_live-{RUN}/",)


def purge(storage: Storage, prefixes: tuple[str, ...], *, roots: tuple[Root, ...] = ROOTS) -> None:
    """Delete every object under the given prefixes on the given roots (best effort, missing is fine)."""
    for root in roots:
        for prefix in prefixes:
            for info in list(storage.list(root, prefix)):
                storage.delete(root, info.key, missing_ok=True)


# -- the reader/writer barrier (plan Task 33 step 3): fixed lake keys the two concurrent jobs rendezvous on. The
# markers live under `runs/test/` (the HfApi channel, so the gateway-less Space can write and read them too). The
# test lake is REUSED across campaigns, so stale markers and a stale `ops_barrier` can linger. Two mechanisms make a
# campaign ignore the leftovers (impl review B HFB-IMPL-01): (1) the writer drops+recreates `ops_barrier` empty and
# publishes that CURRENT before it arms the barrier, so the reader never samples residual rows; (2) a per-campaign
# nonce — the reader mints it and writes it as `reader-ready`, and the writer echoes it in `barrier-armed` and in
# `writer-done`. The reader acts only on markers that carry its nonce, so an earlier campaign's markers are ignored,
# and the campaign launcher clears all three `runs/test/` markers before it starts the pair so a writer-first start
# cannot read a stale `reader-ready` (A/B impl review round 2). `reset_lease` does not touch these markers.
BARRIER_READER_READY: Final = "runs/test/reader-ready"  # the reader writes its per-campaign nonce here
BARRIER_WRITER_DONE: Final = "runs/test/writer-done"  # the writer writes f"{nonce}:<seq>:<count>" or f"{nonce}:error"
BARRIER_ARMED: Final = "runs/test/barrier-armed"  # the writer writes the reader's nonce here
WRITER_WAIT_S: Final = 15 * 60  # the writer waits for the reader at most 15 minutes (plan Task 33 step 3)
READER_WAIT_S: Final = 30 * 60  # the reader waits for the writer at most 30 minutes


def clear_marker(storage: Storage, key: str) -> None:
    storage.delete("lake", key, missing_ok=True)


def write_marker(storage: Storage, key: str) -> None:
    storage.put("lake", key, b"1")


def armed_for(storage: Storage, nonce: str) -> bool:
    """True only when `barrier-armed` carries THIS campaign's nonce; a prior campaign's armed (another nonce) is
    ignored. The writer sets it after it publishes an empty `ops_barrier` (hfrunkit.paced_commits)."""
    try:
        return storage.exists("lake", BARRIER_ARMED) and storage.get("lake", BARRIER_ARMED) == nonce.encode()
    except FileNotFoundError:
        return False  # the marker vanished between exists() and get() (a concurrent clear): treat as not armed


def done_for(storage: Storage, nonce: str) -> str | None:
    """The `writer-done` payload for THIS campaign (after the `<nonce>:` prefix): `<final_seq>:<count>`, or `error`
    when the writer failed; None when absent or another campaign's nonce."""
    try:
        if not storage.exists("lake", BARRIER_WRITER_DONE):
            return None
        raw = storage.get("lake", BARRIER_WRITER_DONE).decode()
    except FileNotFoundError:
        return None  # the marker vanished between exists() and get() (a concurrent clear)
    return raw[len(nonce) + 1 :] if raw.startswith(f"{nonce}:") else None


def wait_for_marker(storage: Storage, key: str, *, timeout_s: float, poll_s: float = 5.0) -> None:
    """Block until `key` exists on the lake, or fail loudly at `timeout_s` (never skip): a missed rendezvous is a
    real failure of the barrier, not a reason to pass (plan Task 33 step 3)."""
    deadline = time.monotonic() + timeout_s
    while not storage.exists("lake", key):
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out after {timeout_s:.0f}s waiting for the barrier marker {key} on the test lake")
        time.sleep(poll_s)


def reset_lease(storage: Storage) -> None:
    """Delete every lease generation and skip record on the lake, so a lease-taking test on the shared test lake
    starts from a known-free lease (the hf_job tests share one throwaway lake; a held generation left by one test
    would otherwise block the next). Safe only on a throwaway lake — call it behind `require_test_lake`.

    On a lake fenced by a lease ledger (T6 spec D1) the ledger's generation would then have no bucket object, which is
    held for 12 minutes, so the reset writes that generation back as released: the next taker commits the one above."""
    for prefix in ("runs/lease/", "runs/skipped/"):
        for info in list(storage.list("lake", prefix)):
            storage.delete("lake", info.key, missing_ok=True)
    ledger = storage.lease_ledger()
    entry = ledger.head().entry if ledger is not None else None
    if entry is not None:
        now = datetime.now(UTC)
        released = LeaseState(
            generation=entry.generation,
            holder_id=entry.holder_id,
            run_id=entry.run_id,
            started_at=entry.acquired_at,
            expires_at=now,
            heartbeat_at=now,
            released=True,
        )
        storage.put("lake", lease_key(entry.generation), released.model_dump_json().encode("utf-8"))


def require_test_lake(storage: Storage) -> None:
    """Refuse to commit or sweep unless the env sentinel is set and both roots are throwaway `*-test` buckets. A
    mis-set environment can then never write the production lake (owner decision, 2026-10-29). Fails loudly, never
    skips: a committing test that cannot prove it is on the test lake must stop the run."""
    if os.environ.get(TEST_LAKE_ENV) != "1":
        pytest.fail(f"refusing to commit/sweep on HF: set {TEST_LAKE_ENV}=1 only on a throwaway test bucket (Task 33)")
    for root in ROOTS:
        bucket = storage.roots.info(root).hf_bucket or ""
        if not bucket.endswith("-test"):
            pytest.fail(f"the committing tests need a throwaway '*-test' {root} bucket, not {bucket!r}")
