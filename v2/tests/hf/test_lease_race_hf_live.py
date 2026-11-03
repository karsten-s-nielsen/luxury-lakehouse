"""Six takers race one HF lease generation (T6 spec Verification, plan Task 6).

Six processes start together and call `Lease.acquire`; exactly one may return a lease, and the others return `None`
with a skip record. The winner releases at once, so the next repeat races for the next generation. The repeat count
comes from `LAKEHOUSE_RACE_REPEATS` (default 1); `LAKEHOUSE_RACE_GAP_S` (default 0) waits between repeats, so a long
session keeps to the commit budget (6 attempts per repeat; a 360 s gap gives 60 per hour). Each repeat records
every taker's outcome, so a failure shows its mode: more than one lease returned, or a taker that raised (for example
the T3 `LeaseMarkerError` "already exists" after a double win). Before T6, the gateway's non-atomic `If-None-Match`
let two takers win (Jobs `6ae675bd…`, `6ae681a5…`); the plan runs this test red first on that code.
"""

from __future__ import annotations

import multiprocessing
import os
import time
from typing import Any

import pytest
from hfkit import reset_lease

from lakehouse.storage.hf import HfStorage
from lakehouse.storage.lease import Lease
from lakehouse.storage.roots import default_bootstrap_file, open_storage, resolve_roots

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]

TAKERS = 6
REPEATS_ENV = "LAKEHOUSE_RACE_REPEATS"
GAP_ENV = "LAKEHOUSE_RACE_GAP_S"


def _open_from_env() -> HfStorage:
    resolved = resolve_roots(
        cli_lake=None, cli_restricted=None, env=os.environ, bootstrap_file=default_bootstrap_file(os.environ)
    )
    storage = open_storage(resolved, os.environ)
    assert isinstance(storage, HfStorage), "the lease race needs the HF gateway roots"
    return storage


def _take(run_id: str, barrier: Any) -> str:
    """Run in a spawned child: race for the lease; a winner releases at once. Returns the outcome."""
    storage = _open_from_env()
    barrier.wait()
    try:
        lease = Lease.acquire(storage, holder_id=f"host-{run_id}", run_id=run_id)
    except Exception as exc:  # noqa: BLE001 - every failure class is an outcome to report, never swallowed
        return f"raised {type(exc).__name__}: {exc}"
    if lease is None:
        return "skipped"
    lease.release()
    return "won"


def _race(repeat: int) -> list[str]:
    ctx = multiprocessing.get_context("spawn")
    run_ids = [f"race{repeat}-{i}" for i in range(TAKERS)]
    with ctx.Manager() as manager, ctx.Pool(TAKERS) as pool:
        barrier = manager.Barrier(TAKERS)
        return list(pool.starmap(_take, [(run_id, barrier) for run_id in run_ids]))


def test_six_takers_one_wins(test_lake: HfStorage) -> None:
    repeats = int(os.environ.get(REPEATS_ENV, "1"))
    gap = float(os.environ.get(GAP_ENV, "0"))
    reset_lease(test_lake)
    failures: list[str] = []
    try:
        for repeat in range(repeats):
            if repeat and gap:
                time.sleep(gap)
            outcomes = _race(repeat)
            print(f"race repeat {repeat}: {outcomes}", flush=True)
            if outcomes.count("won") != 1 or any(o not in ("won", "skipped") for o in outcomes):
                failures.append(f"repeat {repeat}: {outcomes}")
    finally:
        reset_lease(test_lake)
    assert not failures, f"{len(failures)} of {repeats} repeats did not have exactly one winner: {failures}"
