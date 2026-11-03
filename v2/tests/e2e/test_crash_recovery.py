"""Crash recovery (spec §23.6 "Crash loses at most one match"; plan Task 21; T3 spec D1).

A child process runs a five-match run and is killed (SIGKILL on POSIX, TerminateProcess on Windows) while it fetches
match 3. The saved settings set `checkpoint.matches = 1`, so each match publishes at once and matches 1 and 2 are
published: T3 bounds a crash's loss by the checkpoint interval, and M = 1 reproduces the per-match bound here. The
next run takes over the dead run's lease after its expiry plus grace (an injected clock) and fetches only matches 3
to 5.
"""

from __future__ import annotations

import multiprocessing
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from lakekit import lake_roots
from runkit import FakeAdapter, FakeParser, active, entry, save_settings

import lakehouse.bronze.writers as writers_module
import lakehouse.runs.run as run_module
from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CheckpointConfig
from lakehouse.errors import LeaseLostError
from lakehouse.runs.history import read_table
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.lease import HEARTBEAT, HEARTBEAT_THREAD, LEASE_TTL, TAKEOVER_GRACE, Lease
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

NATIVE = ("J03WPY", "J03WMX", "J03WN1", "J03WN2", "J03WN3")
MODULE = "idsse_public"


def _adapter() -> FakeAdapter:
    files = {(native, "events"): f"{native}-a\n{native}-b".encode() for native in NATIVE}
    return FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, native) for native in NATIVE], files)


def _run(storage: LocalStorage, adapter: FakeAdapter, work_dir: Path, **kwargs: Any) -> Any:
    return run_ingestion(
        storage=storage,
        trigger="cli",
        adapters={MODULE: adapter},
        parsers={"idsse": FakeParser("idsse")},
        work_dir=work_dir,
        dbt=None,  # the crash test is about the per-match commits
        **kwargs,
    )


def _child(base: str, at_third: Event) -> None:
    """The crashing run: it blocks in the fetch of match 3 until the parent kills it."""
    adapter = _adapter()

    def block() -> None:
        if len(adapter.fetched) == 3:
            at_third.set()
            time.sleep(300)

    adapter.on_fetch = block
    _run(LocalStorage(lake_roots(Path(base))), adapter, Path(base) / "work-crashed")


def _matches(storage: LocalStorage) -> set[str]:
    with CatalogReader(storage).session() as conn:
        return {row["native_match_id"] for row in read_table(conn, "ops_matches")}


def test_crash_loses_at_most_one_match(tmp_path: Path) -> None:
    roots = lake_roots(tmp_path)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=tmp_path / "boot.json")
    storage = LocalStorage(roots)
    save_settings(storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=1))
    at_third = multiprocessing.get_context("spawn").Event()
    child = multiprocessing.get_context("spawn").Process(target=_child, args=(str(tmp_path), at_third))
    child.start()
    try:
        assert at_third.wait(timeout=120), "the child run never reached match 3"
        child.kill()
        child.join(timeout=60)
    finally:
        if child.is_alive():
            child.kill()
            child.join(timeout=60)
    assert child.exitcode is not None and child.exitcode != 0
    committed = _matches(storage)
    assert len(committed) == 2  # matches 1 and 2, each in its own in-run commit
    assert Lease.acquire(storage, holder_id="early", run_id="run-early") is None  # the dead run still holds it

    def later() -> datetime:
        return datetime.now(UTC) + LEASE_TTL + TAKEOVER_GRACE + timedelta(minutes=1)

    adapter = _adapter()
    result = _run(storage, adapter, tmp_path / "work-next", now=later)
    assert result.status == "ok"
    assert {native for native, _ in adapter.fetched} == set(NATIVE) - committed  # only match 3 onward
    assert (result.counts.added, result.counts.unchanged) == (3, 2)
    assert _matches(storage) == set(NATIVE)


# --- T3: crashes, lost leases and the run anchors (spec D5; plan round 3 Task 8 Step 2) -----------------------------
#
# A crash is a `_Crash(BaseException)` raised from a hook: no `except Exception` in the run catches it, and with
# `Lease.release` patched to do nothing neither the run end nor the release runs, as after a kill. The next run takes
# over after the dead lease's expiry plus grace (an injected clock). On these small lakes the crashed run's orphans are
# most of `data/`, far over the 10 % large-sweep guard, so a sweep that did not exempt them would stop the run.

KEY_ENV = {"LAKEHOUSE_ENCRYPTION_KEY": "b" * 64}
PATHS: dict[str, dict[str, str] | None] = {"register": None, "native": KEY_ENV}


class _Crash(BaseException):
    """A simulated kill."""


def _lake(base: Path, *, matches: int = 50, minutes: int = 5) -> LocalStorage:
    roots = lake_roots(base)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=base / "boot.json")
    storage = LocalStorage(roots)
    save_settings(storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=matches, minutes=minutes))
    return storage


def _published(storage: LocalStorage, env: dict[str, str] | None) -> set[str]:
    try:
        with CatalogReader(storage, env=env).session() as conn:
            return {row["native_match_id"] for row in read_table(conn, "ops_matches")}
    except FileNotFoundError:
        return set()


def _fake(natives: Sequence[str]) -> FakeAdapter:
    files = {(native, "events"): f"{native}-a\n{native}-b".encode() for native in natives}
    return FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, native) for native in natives], files)


def _go(storage: LocalStorage, adapter: FakeAdapter, work: Path, env: dict[str, str] | None, **kwargs: Any) -> Any:
    kwargs.setdefault("dbt", None)
    return run_ingestion(
        storage=storage,
        trigger="cli",
        adapters={MODULE: adapter},
        parsers={"idsse": FakeParser("idsse")},
        work_dir=work,
        env=env,
        **kwargs,
    )


def _heartbeats() -> set[threading.Thread]:
    return {thread for thread in threading.enumerate() if thread.name == HEARTBEAT_THREAD}


def _crashing(monkeypatch: pytest.MonkeyPatch, fn: Callable[[], object]) -> None:
    """Run `fn` as a killed run: it must end in `_Crash`, its lease is never released, and, as with a killed process,
    no heartbeat thread outlives it (a leaked one would renew the lease for the rest of the session)."""

    def never_released(self: Lease) -> None:
        return None

    def no_heartbeat(self: Lease, *, interval_s: float | None = None) -> None:
        # A killed process takes its heartbeat with it. The first renewal comes one HEARTBEAT after the start, so a
        # run that crashes sooner leaves the same lease objects with or without the thread (checked below).
        return None

    before = _heartbeats()
    started = time.monotonic()
    with monkeypatch.context() as patch:
        patch.setattr(Lease, "release", never_released)
        patch.setattr(Lease, "start_heartbeat", no_heartbeat)
        try:
            fn()
        except _Crash:
            assert time.monotonic() - started < HEARTBEAT.total_seconds(), "the run crashed after its first renewal"
            assert _heartbeats() <= before, "the killed run's heartbeat thread outlived it"
            return
    raise AssertionError("the run did not crash")


def _after(minutes: float) -> Callable[[], datetime]:
    def clock() -> datetime:
        return datetime.now(UTC) + LEASE_TTL + TAKEOVER_GRACE + timedelta(minutes=minutes)

    return clock


def _recovers(
    storage: LocalStorage, natives: Sequence[str], base: Path, env: dict[str, str] | None, minutes: float
) -> None:
    """The next run redoes the lost work and sweeps the orphans without stopping; the run after next is clean too."""
    second = _go(storage, _fake(natives), base / "work-next", env, now=_after(minutes))
    assert second.status == "ok", second.errors
    assert second.counts.failed == 0
    third = _go(storage, _fake(natives), base / "work-after", env, now=_after(minutes + 30))
    assert third.status == "ok", third.errors
    assert third.counts.unchanged == len(natives)
    assert _published(storage, env) == set(natives)


@pytest.mark.parametrize("path", sorted(PATHS))
@pytest.mark.parametrize("next_matches", [50, 1])
def test_crash_after_unpublished_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str, next_matches: int
) -> None:
    env = PATHS[path]
    storage = _lake(tmp_path)
    natives = ["C0001", "C0002", "C0003", "C0004"]
    adapter = _fake(natives)

    def crash_at_fourth() -> None:
        if len(adapter.fetched) == 4:
            raise _Crash

    adapter.on_fetch = crash_at_fourth
    _crashing(monkeypatch, lambda: _go(storage, adapter, tmp_path / "work-crash", env))
    assert not _published(storage, env)  # three units were written but never published
    # the next run may publish checkpoints (M = 1) before its sweep in finish()
    save_settings(storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=next_matches))
    _recovers(storage, natives, tmp_path, env, minutes=1)


def test_crash_on_a_large_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    big = [f"L{index:04d}" for index in range(40)]
    assert _go(storage, _fake(big), tmp_path / "work-big", KEY_ENV).status == "ok"
    natives = [*big, "C0001", "C0002", "C0003"]
    adapter = _fake(natives)

    def crash_at_last() -> None:
        if adapter.fetched and adapter.fetched[-1][0] == "C0003":
            raise _Crash

    adapter.on_fetch = crash_at_last
    _crashing(monkeypatch, lambda: _go(storage, adapter, tmp_path / "work-crash", KEY_ENV, now=_after(0)))
    _recovers(storage, natives, tmp_path, KEY_ENV, minutes=20)


class _MovingClock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.now


def _taken_over_mid_unit(storage: LocalStorage, adapter: FakeAdapter, clock: _MovingClock) -> None:
    """While the old writer fetches its second match, another holder takes the lease over; the old writer still
    writes that match (an in-flight unit), so its files land after the new holder's marker."""

    def takeover() -> None:
        if len(adapter.fetched) == 2:
            clock.now += LEASE_TTL + TAKEOVER_GRACE + timedelta(minutes=3)
            assert Lease.acquire(storage, holder_id="other", run_id="run-other", clock=clock) is not None

    adapter.on_fetch = takeover


@pytest.mark.parametrize("path", sorted(PATHS))
def test_lost_lease_writer_files_trip_no_guard(tmp_path: Path, path: str) -> None:
    env = PATHS[path]
    storage = _lake(tmp_path, minutes=1000)  # no time-threshold publish: the write-time check must stop it
    natives = ["C0001", "C0002", "C0003"]
    adapter = _fake(natives)
    clock = _MovingClock()
    _taken_over_mid_unit(storage, adapter, clock)
    with pytest.raises(LeaseLostError):  # it stops at its next write step, before match 3
        _go(storage, adapter, tmp_path / "work-old", env, now=clock)
    assert [native for native, _ in adapter.fetched] == ["C0001", "C0002"]
    _recovers(storage, natives, tmp_path, env, minutes=20)  # after the other holder's lease expired too


@pytest.mark.parametrize("path", sorted(PATHS))
def test_lost_lease_in_flight_write_then_crash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    """The round-7 residual: a lost lease, an in-flight write, then a crash. Its files are in the new holder's
    interval, so they trip no guard."""
    env = PATHS[path]
    storage = _lake(tmp_path, minutes=1000)  # no time-threshold publish: the write-time check must stop it
    natives = ["C0001", "C0002", "C0003"]
    adapter = _fake(natives)
    clock = _MovingClock()
    _taken_over_mid_unit(storage, adapter, clock)
    checks = 0

    def crash_before_match_3() -> bool:
        nonlocal checks
        checks += 1
        if checks == 4:  # the module check, then before matches 1, 2 and 3
            raise _Crash
        return False

    _crashing(
        monkeypatch, lambda: _go(storage, adapter, tmp_path / "work-old", env, now=clock, stop=crash_before_match_3)
    )
    _recovers(storage, natives, tmp_path, env, minutes=20)


# --- the encrypted-local writes outside any unit (Reviewer B plan finding T3-PLAN-01) --------------------------------


def _resolution_crashes(monkeypatch: pytest.MonkeyPatch, *, after_real: bool = False) -> None:
    real = run_module.run_resolution

    def resolution(*args: Any, **kwargs: Any) -> Any:
        if after_real:
            real(*args, **kwargs)  # resolution wrote its tables
        raise _Crash

    monkeypatch.setattr(run_module, "run_resolution", resolution)


def test_crash_after_many_unchanged_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    natives = [f"U{index:04d}" for index in range(200)]
    assert _go(storage, _fake(natives), tmp_path / "work-first", KEY_ENV).status == "ok"

    def crash() -> None:
        _resolution_crashes(monkeypatch)  # after 200 unchanged matches refreshed their signals, unpublished
        _go(storage, _fake(natives), tmp_path / "work-crash", KEY_ENV, now=_after(0))

    _crashing(monkeypatch, crash)
    monkeypatch.undo()
    _recovers(storage, natives, tmp_path, KEY_ENV, minutes=20)


def test_crash_after_error_sink_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    natives = [f"E{index:04d}" for index in range(20)]
    files = {(native, "events"): b"bad" for native in natives}  # every match fails: 20 error rows, no unit
    adapter = FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, native) for native in natives], files)

    def crash() -> None:
        _resolution_crashes(monkeypatch)
        _go(storage, adapter, tmp_path / "work-crash", KEY_ENV)

    _crashing(monkeypatch, crash)
    monkeypatch.undo()
    _recovers(storage, natives, tmp_path, KEY_ENV, minutes=1)  # the sweep exempted the error rows' files


def test_crash_during_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    natives = [f"R{index:04d}" for index in range(5)]

    def crash() -> None:
        _resolution_crashes(monkeypatch, after_real=True)
        _go(storage, _fake(natives), tmp_path / "work-crash", KEY_ENV)

    _crashing(monkeypatch, crash)
    monkeypatch.undo()
    _recovers(storage, natives, tmp_path, KEY_ENV, minutes=1)


def test_crash_during_dbt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    natives = [f"D{index:04d}" for index in range(5)]
    for name, value in KEY_ENV.items():
        monkeypatch.setenv(name, value)  # a run with a dbt step needs the key in the process environment (audit T10)

    def dbt(cat: Any, *, run_id: str, caps: Any) -> Any:
        cat.conn.execute("CREATE TABLE lake.main.gold_probe AS SELECT range AS x FROM range(1000)")  # a native write
        raise _Crash

    _crashing(monkeypatch, lambda: _go(storage, _fake(natives), tmp_path / "work-crash", KEY_ENV, dbt=dbt))
    _recovers(storage, natives, tmp_path, KEY_ENV, minutes=1)


def test_crash_after_a_rolled_back_unit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _lake(tmp_path)
    natives = [f"B{index:04d}" for index in range(5)]
    real_swap = writers_module.native_swap

    def failing_swap(conn: Any, pending: Any, delete_tables: Any, *, key_column: str, key: Any, also: Any) -> None:
        def broken() -> None:
            also()
            raise RuntimeError("the unit failed after its INSERT: the transaction rolls back")

        real_swap(conn, pending, delete_tables, key_column=key_column, key=key, also=broken)

    def crash() -> None:
        monkeypatch.setattr(writers_module, "native_swap", failing_swap)
        _resolution_crashes(monkeypatch)
        _go(storage, _fake(natives), tmp_path / "work-crash", KEY_ENV)

    _crashing(monkeypatch, crash)
    monkeypatch.undo()
    _recovers(storage, natives, tmp_path, KEY_ENV, minutes=1)
