"""The single-writer lease and the skip records (spec §10.6, §23.6 "Two writers", "Skip record")."""

from __future__ import annotations

import multiprocessing
import os
import re
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lakehouse.errors import LeaseLostError, LeaseMarkerError
from lakehouse.storage.base import ObjectInfo
from lakehouse.storage.keys import Root, RootMap, StorageRootInfo
from lakehouse.storage.lease import (
    COMMIT_MARGIN,
    HEARTBEAT,
    LEASE_TTL,
    TAKEOVER_GRACE,
    Lease,
    LeaseState,
    RunAnchor,
    SkipRecord,
    anchor_intervals,
    anchor_key,
    fold_skip_records,
    lease_key,
    marker_key,
    remove_anchors,
    write_skip_record,
)
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

if TYPE_CHECKING:
    from threading import Barrier

HEARTBEAT_THREAD = "lakehouse-lease-heartbeat"
GENERATION_KEY = re.compile(r"^runs/lease/(\d{12})\.json$")  # the generation-object pattern (spec §10.6)


def _roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def _lake(tmp_path: Path) -> LocalStorage:
    init_lake(ResolvedRoots(roots=_roots(tmp_path), source="cli"), bootstrap_file=tmp_path / "boot.json")
    return LocalStorage(_roots(tmp_path))


class Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


def _generation_keys(storage: LocalStorage) -> list[str]:
    """The generation objects only: run anchors and acquisition markers share the `runs/lease/` prefix (T3 D5)."""
    return [info.key for info in storage.list("lake", "runs/lease/") if GENERATION_KEY.match(info.key)]


def _skips(storage: LocalStorage) -> list[str]:
    return [info.key for info in storage.list("lake", "runs/skipped/")]


def test_constants() -> None:
    assert (LEASE_TTL, HEARTBEAT, TAKEOVER_GRACE, COMMIT_MARGIN) == (
        timedelta(minutes=10),
        timedelta(seconds=60),
        timedelta(minutes=2),
        timedelta(seconds=60),
    )
    assert lease_key(7) == "runs/lease/000000000007.json"


def test_acquire_on_empty_lake(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    lease = Lease.acquire(storage, holder_id="host-1", run_id="r1")
    assert lease is not None
    assert lease.generation == 1
    assert lease.started_storage_time == storage.stat("lake", lease_key(1)).modified
    lease.assert_held()
    assert _skips(storage) == []


def _acquire_in_child(base: str, run_id: str, barrier: Barrier) -> bool:
    storage = LocalStorage(_roots(Path(base)))
    barrier.wait()
    return Lease.acquire(storage, holder_id=f"host-{run_id}", run_id=run_id) is not None


def _race(tmp_path: Path, runs: list[str]) -> list[bool]:
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(len(runs)) as pool:
        barrier = manager.Barrier(len(runs))
        return pool.starmap(_acquire_in_child, [(str(tmp_path), run, barrier) for run in runs])


def test_two_writers_one_wins(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    results = _race(tmp_path, ["a", "b"])
    assert results.count(True) == 1
    assert _generation_keys(storage) == [lease_key(1)]
    assert len(_skips(storage)) == 1


def test_two_takers_one_wins(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    past = Clock(datetime.now(UTC) - timedelta(hours=1))
    assert Lease.acquire(storage, holder_id="old", run_id="r0", clock=past) is not None
    results = _race(tmp_path, ["a", "b"])
    assert results.count(True) == 1
    assert _generation_keys(storage) == [lease_key(1), lease_key(2)]
    winner = LeaseState.model_validate_json(storage.get("lake", lease_key(2)))
    loser = "b" if winner.run_id == "a" else "a"
    assert _skips(storage) == [f"runs/skipped/{loser}.json"]


def test_released_lease_taken_at_once(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    first = Lease.acquire(storage, holder_id="h", run_id="r1")
    assert first is not None
    first.release()
    second = Lease.acquire(storage, holder_id="h", run_id="r2")
    assert second is not None
    assert second.generation == 2
    with pytest.raises(LeaseLostError):
        first.assert_held()


def test_held_lease_writes_skip_record(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    assert Lease.acquire(storage, holder_id="h", run_id="r1") is not None
    assert Lease.acquire(storage, holder_id="h", run_id="r2") is None
    record = SkipRecord.model_validate_json(storage.get("lake", "runs/skipped/r2.json"))
    assert record.blocking_run_id == "r1"
    assert "run in progress" in record.reason


def test_takeover_after_winner_crash(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    first = Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock)
    assert first is not None
    first.release()
    crashed = Lease.acquire(storage, holder_id="h", run_id="r2", clock=clock)
    assert crashed is not None
    assert crashed.generation == 2
    clock.advance(LEASE_TTL + TAKEOVER_GRACE - timedelta(seconds=1))
    assert Lease.acquire(storage, holder_id="h", run_id="r3", clock=clock) is None
    clock.advance(timedelta(seconds=2))
    third = Lease.acquire(storage, holder_id="h", run_id="r4", clock=clock)
    assert third is not None
    assert third.generation == 3


def test_lost_lease_blocks_commit(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    holder_a = Lease.acquire(storage, holder_id="a", run_id="ra", clock=clock)
    assert holder_a is not None
    clock.advance(LEASE_TTL + TAKEOVER_GRACE + timedelta(seconds=1))
    holder_b = Lease.acquire(storage, holder_id="b", run_id="rb", clock=clock)
    assert holder_b is not None
    with pytest.raises(LeaseLostError, match="generation 2"):
        holder_a.assert_held()
    holder_b.assert_held()


def test_commit_margin(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock)
    assert lease is not None
    clock.advance(LEASE_TTL - COMMIT_MARGIN - timedelta(seconds=1))
    lease.assert_held()
    clock.advance(timedelta(seconds=2))
    with pytest.raises(LeaseLostError, match="margin"):
        lease.assert_held()
    lease.renew()
    lease.assert_held()


def test_renew_writes_expiry_with_plain_put(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock)
    assert lease is not None
    clock.advance(timedelta(minutes=5))
    lease.renew()
    state = LeaseState.model_validate_json(storage.get("lake", lease_key(1)))
    assert state.heartbeat_at == clock.now
    assert state.expires_at == clock.now + LEASE_TTL
    assert state.released is False


class _HeldHeartbeat(LocalStorage):
    """Holds the first heartbeat write until the test lets it go, to race a heartbeat against `release`."""

    def __init__(self, roots: RootMap) -> None:
        super().__init__(roots)
        self.in_put = threading.Event()
        self.go = threading.Event()

    def put(self, root: Root, key: str, data: bytes | Path) -> None:
        if threading.current_thread().name == HEARTBEAT_THREAD and not self.in_put.is_set():
            self.in_put.set()
            assert self.go.wait(timeout=10)
        super().put(root, key, data)


def test_release_waits_for_the_heartbeat_thread(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`release` returns only after the heartbeat thread has ended, so no daemon thread outlives the lease. Timing
    alone cannot show it (a stopped thread ends within milliseconds), so a spy on the lease's own thread does."""
    storage = _lake(tmp_path)
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=Clock(datetime.now(UTC)))
    assert lease is not None
    others = set(threading.enumerate())
    lease.start_heartbeat(interval_s=60)
    (beat,) = {t for t in threading.enumerate() if t.name == HEARTBEAT_THREAD} - others
    joined: list[float | None] = []
    real_join = beat.join

    def join(timeout: float | None = None) -> None:
        joined.append(timeout)
        real_join(timeout)

    monkeypatch.setattr(beat, "join", join)
    lease.release()
    assert joined, "release returned without waiting for the heartbeat thread"
    assert not beat.is_alive()


def test_release_stops_heartbeat_first(tmp_path: Path) -> None:
    _lake(tmp_path)
    storage = _HeldHeartbeat(_roots(tmp_path))
    clock = Clock(datetime.now(UTC))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock)
    assert lease is not None
    others = set(threading.enumerate())  # another test's heartbeat thread must not decide this one
    lease.start_heartbeat(interval_s=0.01)
    (beat,) = {t for t in threading.enumerate() if t.name == HEARTBEAT_THREAD} - others
    assert storage.in_put.wait(timeout=10)
    releaser = threading.Thread(target=lease.release)
    releaser.start()
    releaser.join(timeout=0.5)  # release must wait for the heartbeat write that is under way
    storage.go.set()
    releaser.join(timeout=10)
    assert not releaser.is_alive()
    assert not beat.is_alive()
    released = storage.get("lake", lease_key(1))
    assert LeaseState.model_validate_json(released).released is True
    clock.advance(timedelta(minutes=1))
    lease.renew()  # a late heartbeat after release writes nothing
    assert storage.get("lake", lease_key(1)) == released


class _HidingListing(LocalStorage):
    """The first listing of the lease folder hides one generation, like a stale listing."""

    def __init__(self, roots: RootMap, hidden: str) -> None:
        super().__init__(roots)
        self.hidden = hidden
        self.listings = 0

    def list(self, root: Root, prefix: str) -> Iterator[ObjectInfo]:
        items = super().list(root, prefix)
        if prefix == "runs/lease/":
            self.listings += 1
            if self.listings == 1:
                return (info for info in items if info.key != self.hidden)
        return items


def test_stale_listing_abandons(tmp_path: Path) -> None:
    plain = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    for run in ("r1", "r2"):
        lease = Lease.acquire(plain, holder_id="h", run_id=run, clock=clock)
        assert lease is not None
        lease.release()
    newer = LeaseState(
        generation=5,
        holder_id="other",
        run_id="r5",
        started_at=clock.now,
        expires_at=clock.now + LEASE_TTL,
        heartbeat_at=clock.now,
        released=False,
    )
    plain.put("lake", lease_key(5), newer.model_dump_json().encode())
    storage = _HidingListing(_roots(tmp_path), hidden=lease_key(5))
    assert Lease.acquire(storage, holder_id="h", run_id="r3", clock=clock) is None
    abandoned = LeaseState.model_validate_json(plain.get("lake", lease_key(3)))
    assert (abandoned.run_id, abandoned.released) == ("r3", True)
    assert _skips(plain) == ["runs/skipped/r3.json"]


def test_unparsable_generation_held_until_mtime_ttl_grace(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    storage.put("lake", lease_key(1), b"{partial")
    written = storage.stat("lake", lease_key(1)).modified
    clock = Clock(written + LEASE_TTL + TAKEOVER_GRACE - timedelta(seconds=1))
    assert Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock) is None
    clock.advance(timedelta(seconds=2))
    lease = Lease.acquire(storage, holder_id="h", run_id="r2", clock=clock)
    assert lease is not None
    assert lease.generation == 2


def _chain(storage: LocalStorage, clock: Clock, count: int) -> None:
    for index in range(count):
        lease = Lease.acquire(storage, holder_id="h", run_id=f"r{index}", clock=clock)
        assert lease is not None
        lease.release()


def test_pruning_keeps_recent(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    old_lake = _lake(tmp_path / "old")
    clock = Clock(now - timedelta(days=40))
    _chain(old_lake, clock, 15)
    clock.now = now - timedelta(days=10)
    _chain(old_lake, clock, 10)
    clock.now = now
    assert Lease.acquire(old_lake, holder_id="h", run_id="new", clock=clock) is not None
    # 26 generations; the newest 20 are 7..26. Of 1..6, all expired 40 days ago, so all go.
    assert _generation_keys(old_lake) == [lease_key(g) for g in range(7, 27)]

    young_lake = _lake(tmp_path / "young")
    clock = Clock(now - timedelta(days=10))
    _chain(young_lake, clock, 25)
    clock.now = now
    assert Lease.acquire(young_lake, holder_id="h", run_id="new", clock=clock) is not None
    # outside the newest 20, but younger than 30 days: kept
    assert _generation_keys(young_lake) == [lease_key(g) for g in range(1, 27)]


def test_skip_record_written_and_folded(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    holder = Lease.acquire(storage, holder_id="h", run_id="r1")
    assert holder is not None
    assert Lease.acquire(storage, holder_id="h", run_id="r2") is None
    assert _skips(storage) == ["runs/skipped/r2.json"]
    assert [info.key for info in storage.list("lake", "catalog/")] == []

    def failing(record: SkipRecord) -> None:
        raise RuntimeError("run history is not writable")

    with pytest.raises(RuntimeError):
        fold_skip_records(storage, record=failing)
    assert _skips(storage) == ["runs/skipped/r2.json"]
    recorded: list[SkipRecord] = []
    folded = fold_skip_records(storage, record=recorded.append)
    assert [r.run_id for r in folded] == ["r2"]
    assert recorded == folded
    assert folded[0].blocking_run_id == "r1"
    assert _skips(storage) == []


def test_write_skip_record_direct(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    write_skip_record(storage, run_id="r9", reason="skipped: run in progress", blocking_run_id=None)
    record = SkipRecord.model_validate_json(storage.get("lake", "runs/skipped/r9.json"))
    assert (record.run_id, record.blocking_run_id) == ("r9", None)
    assert record.at.tzinfo is not None


# --- the run anchor and the acquisition marker (T3 spec D5, owner decisions 12 and 13) -------------------------------


class _Recording(LocalStorage):
    """Records the order of writes, and fails the write-once store of one chosen key."""

    def __init__(self, roots: RootMap, *, fail_key: str | None = None) -> None:
        super().__init__(roots)
        self.writes: list[str] = []
        self.fail_key = fail_key

    def put(self, root: Root, key: str, data: bytes | Path) -> None:
        self.writes.append(key)
        super().put(root, key, data)

    def put_if_absent(self, root: Root, key: str, data: bytes | Path) -> bool:
        if key == self.fail_key:
            raise OSError("no space left on device")
        self.writes.append(key)
        return super().put_if_absent(root, key, data)


def _all_keys(storage: LocalStorage) -> set[str]:
    return {info.key for prefix in ("runs/", "catalog/", "data/", "raw/") for info in storage.list("lake", prefix)}


def _put_at(storage: LocalStorage, key: str, data: bytes, when: datetime) -> None:
    storage.put("lake", key, data)
    path = storage.roots.lake.local_path
    assert path is not None
    stamp = when.timestamp()
    os.utime(path / key, (stamp, stamp))


def _anchor(storage: LocalStorage, generation: int, when: datetime) -> None:
    body = RunAnchor(generation=generation, run_id=f"r{generation}").model_dump_json().encode()
    _put_at(storage, anchor_key(generation), body, when)


def _stamps(storage: LocalStorage, keys: tuple[str, ...]) -> dict[str, tuple[datetime, bytes]]:
    return {key: (storage.stat("lake", key).modified, storage.get("lake", key)) for key in keys}


def test_acquire_stores_anchor_then_marker_before_returning(tmp_path: Path) -> None:
    _lake(tmp_path)
    storage = _Recording(_roots(tmp_path))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1")
    assert lease is not None
    assert storage.writes == [lease_key(1), anchor_key(1), marker_key(1)]
    anchor = RunAnchor.model_validate_json(storage.get("lake", anchor_key(1)))
    assert (anchor.generation, anchor.run_id) == (1, "r1")
    assert storage.stat("lake", marker_key(1)).modified >= lease.started_storage_time


def test_renew_and_release_do_not_touch_anchor_or_marker(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock)
    assert lease is not None
    before = _stamps(storage, (anchor_key(1), marker_key(1)))
    clock.advance(timedelta(minutes=1))
    lease.renew()
    lease.release()
    assert _stamps(storage, (anchor_key(1), marker_key(1))) == before


def test_anchor_and_marker_keys_are_not_generations(tmp_path: Path) -> None:
    assert GENERATION_KEY.match(anchor_key(7)) is None
    assert GENERATION_KEY.match(marker_key(7)) is None
    assert (anchor_key(7), marker_key(7)) == ("runs/lease/000000000007.anchor", "runs/lease/000000000007.acquired")
    storage = _lake(tmp_path)
    first = Lease.acquire(storage, holder_id="h", run_id="r1")
    assert first is not None
    first.release()
    second = Lease.acquire(storage, holder_id="h", run_id="r2")  # anchors and markers present: acquire ignores them
    assert second is not None
    assert second.generation == 2
    assert _generation_keys(storage) == [lease_key(1), lease_key(2)]


def test_failed_anchor_store_releases_and_raises(tmp_path: Path) -> None:
    _lake(tmp_path)
    storage = _Recording(_roots(tmp_path), fail_key=anchor_key(1))
    with pytest.raises(LeaseMarkerError, match="generation 1"):
        Lease.acquire(storage, holder_id="h", run_id="r1")
    assert LeaseState.model_validate_json(storage.get("lake", lease_key(1))).released is True
    assert _all_keys(storage) == {lease_key(1)}


def test_failed_marker_store_releases_and_raises(tmp_path: Path) -> None:
    _lake(tmp_path)
    storage = _Recording(_roots(tmp_path), fail_key=marker_key(1))
    with pytest.raises(LeaseMarkerError, match="generation 1"):
        Lease.acquire(storage, holder_id="h", run_id="r1")
    assert LeaseState.model_validate_json(storage.get("lake", lease_key(1))).released is True
    assert _all_keys(storage) == {lease_key(1), anchor_key(1)}
    after = Lease.acquire(storage, holder_id="h", run_id="r2")  # the released generation is taken at once
    assert after is not None
    assert after.generation == 2


@pytest.mark.parametrize("present", ["anchor", "marker"])
def test_existing_anchor_or_marker_raises_without_releasing(tmp_path: Path, present: str) -> None:
    """An anchor or marker that already exists means another taker holds the same generation (a store whose create
    is not atomic let two takers win it: the HF gateway, T3 Task 12 / audit T6). The loser raises and writes nothing
    more: marking the generation released would release the winner's lease and let a third taker in at once."""
    storage = _lake(tmp_path)
    other = RunAnchor(generation=1, run_id="other").model_dump_json().encode()
    storage.put("lake", anchor_key(1) if present == "anchor" else marker_key(1), other)
    with pytest.raises(LeaseMarkerError, match="already exists"):
        Lease.acquire(storage, holder_id="h", run_id="r1")
    assert LeaseState.model_validate_json(storage.get("lake", lease_key(1))).released is False
    assert storage.get("lake", anchor_key(1) if present == "anchor" else marker_key(1)) == other


def test_prune_deletes_marker_and_keeps_anchor(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    now = datetime.now(UTC)
    clock = Clock(now - timedelta(days=40))
    _chain(storage, clock, 21)
    clock.now = now
    assert Lease.acquire(storage, holder_id="h", run_id="new", clock=clock) is not None
    # 22 generations; the newest 20 are 3..22; generations 1 and 2 expired 40 days ago and go, with their markers
    assert not storage.exists("lake", lease_key(1))
    assert not storage.exists("lake", marker_key(1))
    assert storage.exists("lake", anchor_key(1))
    assert storage.exists("lake", marker_key(3))


def test_interval_ends_at_earliest_later_marker(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    t0 = datetime(2026, 10, 31, 12, tzinfo=UTC)
    t1, t5 = t0 + timedelta(minutes=1), t0 + timedelta(minutes=5)
    _anchor(storage, 3, t0)
    _put_at(storage, marker_key(3), b"", t1)
    _anchor(storage, 4, t0 + timedelta(minutes=2))  # abandoned during acquisition: no marker
    _anchor(storage, 5, t0 + timedelta(minutes=4))
    _put_at(storage, marker_key(5), b"", t5)
    intervals = {i.generation: i for i in anchor_intervals(storage)}
    assert (intervals[3].start, intervals[3].end) == (t1, t5)
    assert intervals[4].start is None
    assert intervals[5].end is None
    assert intervals[3].covers(t0 + timedelta(minutes=3))
    assert not intervals[3].covers(t5)
    assert not intervals[4].covers(t0 + timedelta(minutes=3))
    assert intervals[5].covers(t5 + timedelta(hours=1))


def test_out_of_order_markers_leave_no_gap(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    t0 = datetime(2026, 10, 31, 12, tzinfo=UTC)
    t7, t8, t9 = (t0 + timedelta(minutes=m) for m in (7, 8, 9))
    _anchor(storage, 6, t0)
    _put_at(storage, marker_key(6), b"", t8)  # a stalled acquirer stored its marker late
    _anchor(storage, 7, t0 + timedelta(minutes=6))
    _put_at(storage, marker_key(7), b"", t7)
    intervals = anchor_intervals(storage)
    for moment in (t7, t8, t9):
        assert any(i.covers(moment) for i in intervals), moment


def test_interval_ignores_a_later_heartbeat(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    t0 = datetime(2026, 10, 31, 12, tzinfo=UTC)
    t3 = t0 + timedelta(minutes=3)
    _anchor(storage, 1, t0)
    _put_at(storage, marker_key(1), b"", t0)
    _anchor(storage, 2, t3)
    _put_at(storage, marker_key(2), b"", t3)
    _put_at(storage, lease_key(2), b"{}", t0 + timedelta(minutes=30))  # renewed long after the interval
    first = next(i for i in anchor_intervals(storage) if i.generation == 1)
    assert (first.start, first.end) == (t0, t3)


def test_examined_before_and_remove_anchors(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    t0 = datetime(2026, 10, 31, 12, tzinfo=UTC)
    _anchor(storage, 1, t0)
    _put_at(storage, marker_key(1), b"", t0)
    _anchor(storage, 2, t0 + timedelta(minutes=1))  # no marker
    _anchor(storage, 3, t0 + timedelta(minutes=5))
    _put_at(storage, marker_key(3), b"", t0 + timedelta(minutes=5))
    intervals = {i.generation: i for i in anchor_intervals(storage)}
    assert intervals[1].examined_before(t0 + timedelta(minutes=6))
    assert not intervals[1].examined_before(t0 + timedelta(minutes=5))  # the interval must END before the start
    assert intervals[2].examined_before(t0)  # no marker: covers nothing
    assert not intervals[3].examined_before(t0 + timedelta(days=1))  # open interval
    remove_anchors(storage, [1, 2])
    assert [i.generation for i in anchor_intervals(storage)] == [3]
    assert storage.exists("lake", marker_key(1))


def test_confirm_calls_assert_held_only_after_a_heartbeat_gap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    lease = Lease.acquire(storage, holder_id="h", run_id="r1", clock=clock)
    assert lease is not None
    calls: list[datetime] = []
    original = Lease.assert_held

    def counting(self: Lease) -> None:
        calls.append(clock.now)
        original(self)

    monkeypatch.setattr(Lease, "assert_held", counting)
    clock.advance(timedelta(seconds=30))
    lease.confirm()
    assert calls == []
    clock.advance(timedelta(seconds=31))
    lease.confirm()
    assert len(calls) == 1
    clock.advance(timedelta(seconds=29))
    lease.assert_held()  # a commit's own check counts as a confirmation
    clock.advance(timedelta(seconds=30))
    lease.confirm()
    assert len(calls) == 2  # the direct call above, and nothing more


def test_confirm_ignores_a_renew_after_takeover(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    clock = Clock(datetime.now(UTC))
    old = Lease.acquire(storage, holder_id="a", run_id="ra", clock=clock)
    assert old is not None
    clock.advance(LEASE_TTL + TAKEOVER_GRACE + timedelta(seconds=1))
    assert Lease.acquire(storage, holder_id="b", run_id="rb", clock=clock) is not None
    old.renew()  # a plain put: it succeeds although generation 2 exists
    with pytest.raises(LeaseLostError, match="generation 2"):
        old.confirm()
