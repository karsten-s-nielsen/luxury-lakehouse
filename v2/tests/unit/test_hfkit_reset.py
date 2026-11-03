"""`hfkit.reset_lease` on a lake fenced by a lease ledger (T6 plan Task 6 carry).

The hf_job tests share one test lake and reset the lease between tests. With the ledger, deleting the bucket
generation objects alone would leave the ledger's generation without a bucket object, which is held for 12 minutes
(T6 spec D1 step 3) and would block the next test. `reset_lease` therefore leaves the ledger's generation released.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from hfkit import reset_lease
from lakekit import Clock, FakeLedger, LedgerStorage, lake_roots

from lakehouse.storage.lease import Lease


def test_reset_leaves_the_ledger_generation_free(tmp_path: Path) -> None:
    for name in ("lake", "lake-restricted"):
        (tmp_path / name).mkdir()
    storage = LedgerStorage(lake_roots(tmp_path), FakeLedger())
    now = Clock(datetime.now(UTC))  # the fake ledger stamps its commits with the real clock
    held = Lease.acquire(storage, holder_id="h", run_id="r1", clock=now)
    assert held is not None  # generation 1, held for 10 minutes
    reset_lease(storage)
    after = Lease.acquire(storage, holder_id="h", run_id="r2", clock=now)
    assert after is not None and after.generation == 2, "the reset left the ledger generation held"


def test_reset_without_a_ledger_clears_the_bucket(tmp_path: Path) -> None:
    for name in ("lake", "lake-restricted"):
        (tmp_path / name).mkdir()
    storage = LedgerStorage(lake_roots(tmp_path), FakeLedger())
    storage.ledger = None  # type: ignore[assignment]
    first = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(datetime.now(UTC)))
    assert first is not None
    reset_lease(storage)
    assert [info.key for info in storage.list("lake", "runs/lease/")] == []
