"""`Lease.acquire` and `Lease.assert_held` on the HF lease ledger (T6 spec D1 steps 1-7, plan Task 4).

A storage that provides a lease ledger (an HF writer) wins a generation only by a ledger commit whose parent is the
head it read; the bucket generation object, the run anchor and the marker follow, and the winner reads the ledger
again before it returns the lease. `FakeLedger` scripts each outcome. A storage without a ledger keeps today's lease
(`tests/unit/test_lease.py`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from lakekit import Clock, FakeLedger, LedgerStorage, lake_roots

from lakehouse.errors import IntegrityError, LeaseLedgerError, LeaseLostError, LeaseMarkerError
from lakehouse.storage.lease import (
    LEASE_TTL,
    TAKEOVER_GRACE,
    Lease,
    LeaseState,
    SkipRecord,
    anchor_key,
    lease_key,
    marker_key,
)
from lakehouse.storage.ledger import LedgerEntry

T = datetime(2026, 11, 1, 4, 0, tzinfo=UTC)


def _storage(tmp_path: Path, ledger: FakeLedger) -> LedgerStorage:
    for name in ("lake", "lake-restricted"):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    return LedgerStorage(lake_roots(tmp_path), ledger)


def _entry(generation: int, run_id: str = "old-run", at: datetime = T) -> LedgerEntry:
    return LedgerEntry(generation=generation, holder_id="old-host", run_id=run_id, acquired_at=at)


def _bucket(
    storage: LedgerStorage, generation: int, *, released: bool, expires: datetime, run_id: str = "old-run"
) -> None:
    state = LeaseState(
        generation=generation,
        holder_id="old-host",
        run_id=run_id,
        started_at=expires - LEASE_TTL,
        expires_at=expires,
        heartbeat_at=expires - LEASE_TTL,
        released=released,
    )
    storage.put("lake", lease_key(generation), state.model_dump_json().encode())


def _skips(storage: LedgerStorage) -> list[SkipRecord]:
    return [SkipRecord.model_validate_json(storage.get("lake", i.key)) for i in storage.list("lake", "runs/skipped/")]


def _lease_objects(storage: LedgerStorage) -> list[str]:
    return [info.key for info in storage.list("lake", "runs/lease/")]


def test_single_winner_commits_the_next_generation(tmp_path: Path) -> None:
    ledger = FakeLedger()
    storage = _storage(tmp_path, ledger)
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert lease is not None and lease.generation == 1
    assert [(e.generation, e.holder_id, e.run_id) for e in ledger.commits] == [(1, "h", "r1")]
    assert set(_lease_objects(storage)) == {lease_key(1), anchor_key(1), marker_key(1)}


def test_generation_is_max_of_ledger_and_bucket(tmp_path: Path) -> None:
    """Step 2 (A-07): a bucket generation above the ledger's (only an old image can write one) is decided on; held,
    it gives a skip record and no commit."""
    ledger = FakeLedger(entry=_entry(1))
    storage = _storage(tmp_path, ledger)
    _bucket(storage, 3, released=False, expires=T + timedelta(minutes=5), run_id="old-image-run")
    assert Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T)) is None
    assert ledger.commits == []
    assert [s.blocking_run_id for s in _skips(storage)] == ["old-image-run"]


def test_bootstrap_from_bucket_listing(tmp_path: Path) -> None:
    ledger = FakeLedger()  # a fresh ledger
    storage = _storage(tmp_path, ledger)
    for generation in (1, 2, 3):
        _bucket(storage, generation, released=True, expires=T - timedelta(hours=1))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert lease is not None and lease.generation == 4
    assert [e.generation for e in ledger.commits] == [4]


def test_ledger_generation_without_bucket_object_is_held(tmp_path: Path) -> None:
    """Step 3: the winner of generation 2 crashed between its commit and its bucket write: generation 2 is held until
    the commit time + LEASE_TTL + TAKEOVER_GRACE (12 minutes), never free at once."""
    ledger = FakeLedger(entry=_entry(2), committed_at=T)
    storage = _storage(tmp_path, ledger)
    held_until = T + LEASE_TTL + TAKEOVER_GRACE
    assert Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(held_until - timedelta(seconds=1))) is None
    assert ledger.commits == []
    lease = Lease.acquire(storage, holder_id="h", run_id="r2", clock=Clock(held_until + timedelta(seconds=1)))
    assert lease is not None and lease.generation == 3


def test_held_until_uses_commit_time(tmp_path: Path) -> None:
    """Review Focus 3: the server's commit time decides, not the entry's own `acquired_at` (a client clock)."""
    ledger = FakeLedger(entry=_entry(2, at=T), committed_at=T - LEASE_TTL - TAKEOVER_GRACE - timedelta(minutes=1))
    storage = _storage(tmp_path, ledger)
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert lease is not None and lease.generation == 3


def test_lost_race_writes_skip_record_naming_the_winner(tmp_path: Path) -> None:
    ledger = FakeLedger(script=["conflict"])
    storage = _storage(tmp_path, ledger)
    assert Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T)) is None
    assert [s.blocking_run_id for s in _skips(storage)] == ["other-run"]
    assert _lease_objects(storage) == [], "a lost race writes no lease object"


def test_lost_race_with_a_lagging_head_read_is_still_a_skip(tmp_path: Path) -> None:
    """A parent-check failure (HTTP 409) means the commit did not land: a lost race by itself. The Hub's head read can
    still show the old head right after the winner's commit (two-process HF Job, 2026-11-01), which must not turn a
    lost race into a loud stop; the skip record names the winner when a later read shows it, else no one."""
    ledger = FakeLedger(script=["conflict"], stale_reads=5)
    storage = _storage(tmp_path, ledger)
    assert Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T)) is None
    [skip] = _skips(storage)
    assert "another run committed lease generation 1 first" in skip.reason
    assert skip.blocking_run_id is None  # every head read still showed the old head
    assert _lease_objects(storage) == [], "a lost race writes no lease object"


def test_landed_commit_with_lost_reply_is_a_win(tmp_path: Path) -> None:
    ledger = FakeLedger(script=["lost_reply"])
    storage = _storage(tmp_path, ledger)
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert lease is not None and lease.generation == 1


def test_rate_limit_stops_loudly(tmp_path: Path) -> None:
    ledger = FakeLedger(script=["rate_limit"])
    storage = _storage(tmp_path, ledger)
    with pytest.raises(LeaseLedgerError, match="429"):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert _lease_objects(storage) == []


def test_ledger_outage_writes_nothing(tmp_path: Path) -> None:
    """Review Focus 1: the Hub is down; the acquisition stops loudly and writes nothing to the bucket."""
    ledger = FakeLedger(head_error=ConnectionError("hub down"))
    storage = _storage(tmp_path, ledger)
    with pytest.raises(LeaseLedgerError):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert _lease_objects(storage) == []
    assert _skips(storage) == []


def test_unknown_commit_outcome_stops_loudly(tmp_path: Path) -> None:
    """An outage during the commit with the head unchanged: neither a win nor a lost race is proven."""
    ledger = FakeLedger(script=["outage"])
    storage = _storage(tmp_path, ledger)
    with pytest.raises(LeaseLedgerError):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert _lease_objects(storage) == []


def test_corrupt_ledger_entry_stops(tmp_path: Path) -> None:
    """Review Focus 2: an unreadable `lease.json` is never treated as a free lease."""
    ledger = FakeLedger(head_error=IntegrityError("lease.json is not readable"))
    storage = _storage(tmp_path, ledger)
    with pytest.raises(LeaseLedgerError, match="not readable"):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert _lease_objects(storage) == []


def test_bucket_fence_false_raises_and_writes_nothing(tmp_path: Path) -> None:
    """Step 6: an old image created the bucket generation while this taker won it on the ledger."""
    ledger = FakeLedger()
    storage = _storage(tmp_path, ledger)
    ledger.on_claim = lambda: _bucket(storage, 1, released=False, expires=T + LEASE_TTL, run_id="old-image-run")
    with pytest.raises(LeaseMarkerError, match="outside the ledger"):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert set(_lease_objects(storage)) == {lease_key(1)}, "no anchor or marker after the bucket fence fails"


def test_stalled_winner_is_caught_by_the_re_read(tmp_path: Path) -> None:
    """Step 7 (B-11): the winner of generation 1 stalled past the 12-minute rule after its commit; a new taker
    committed generation 2. The re-read after the bucket object stops it before the anchor and the marker."""
    ledger = FakeLedger(later_heads=[_entry(2, run_id="new-run")])
    storage = _storage(tmp_path, ledger)
    with pytest.raises(LeaseLostError, match="generation 2"):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert set(_lease_objects(storage)) == {lease_key(1)}, "no anchor or marker after the re-read fails"


def test_stall_after_the_marker_is_caught_before_return(tmp_path: Path) -> None:
    """The second re-read, just before the lease is returned, catches a newer generation that landed while the anchor
    and the marker were stored."""
    ledger = FakeLedger(later_heads=[_entry(1, run_id="r1"), _entry(2, run_id="new-run")])
    ledger.later_heads[0] = ledger.later_heads[0].model_copy(update={"holder_id": "h"})
    storage = _storage(tmp_path, ledger)
    with pytest.raises(LeaseLostError, match="generation 2"):
        Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))


def test_assert_held_reads_the_ledger(tmp_path: Path) -> None:
    ledger = FakeLedger()
    storage = _storage(tmp_path, ledger)
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert lease is not None
    lease.assert_held()  # the ledger still names this holder
    ledger.entry = _entry(2, run_id="new-run")
    with pytest.raises(LeaseLostError, match="generation 2"):
        lease.assert_held()


def test_assert_held_fails_closed_on_a_ledger_outage(tmp_path: Path) -> None:
    ledger = FakeLedger()
    storage = _storage(tmp_path, ledger)
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(T))
    assert lease is not None
    ledger.head_error = ConnectionError("hub down")
    with pytest.raises(LeaseLostError, match="cannot confirm"):
        lease.assert_held()
