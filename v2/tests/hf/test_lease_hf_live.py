"""The single-writer lease race on the real gateway (spec §26 item 2, §23.6 "Two writers"; plan Task 33 step 3).

Two processes race `Lease.acquire` on the throwaway test lake: the gateway's `If-None-Match` on the lease generation
object lets exactly one win. A second test expires generation 1 and races two takers for generation 2. These prove
the single-writer lease end to end on the real gateway, across processes — not only the in-process `put_if_absent`
thread race in test_storage. Each test resets the lease chain before and after, because the hf_job tests share one
lake and a held generation would otherwise block the run test.
"""

from __future__ import annotations

import multiprocessing
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from hfkit import reset_lease

from lakehouse.storage.hf import HfStorage
from lakehouse.storage.lease import Lease
from lakehouse.storage.roots import default_bootstrap_file, open_storage, resolve_roots

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]


def _open_from_env() -> HfStorage:
    resolved = resolve_roots(
        cli_lake=None, cli_restricted=None, env=os.environ, bootstrap_file=default_bootstrap_file(os.environ)
    )
    storage = open_storage(resolved, os.environ)
    assert isinstance(storage, HfStorage), "the lease race needs the HF gateway roots"
    return storage


def _acquire_in_child(run_id: str, barrier: Any) -> bool:
    """Run in a spawned child: build the storage from the inherited environment and race for the lease."""
    storage = _open_from_env()
    barrier.wait()
    return Lease.acquire(storage, holder_id=f"host-{run_id}", run_id=run_id) is not None


def _race(run_ids: list[str]) -> list[bool]:
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(len(run_ids)) as pool:
        barrier = manager.Barrier(len(run_ids))
        return list(pool.starmap(_acquire_in_child, [(run_id, barrier) for run_id in run_ids]))


def test_two_processes_one_wins(test_lake: HfStorage) -> None:
    reset_lease(test_lake)
    try:
        results = _race(["hfa", "hfb"])
        assert results.count(True) == 1, f"exactly one process must win the lease, got {results}"
    finally:
        reset_lease(test_lake)


def test_two_takers_of_one_expired_lease(test_lake: HfStorage) -> None:
    reset_lease(test_lake)
    try:
        expired = Lease.acquire(
            test_lake, holder_id="old", run_id="hf-old", clock=lambda: datetime.now(UTC) - timedelta(hours=1)
        )
        # on a ledger lake generations are never reused (T6 spec D1 bootstrap), so the expired one is the next number
        assert expired is not None, "the expired lease was not taken"
        results = _race(["hfc", "hfd"])
        assert results.count(True) == 1, f"exactly one taker must win the expired lease, got {results}"
    finally:
        reset_lease(test_lake)
