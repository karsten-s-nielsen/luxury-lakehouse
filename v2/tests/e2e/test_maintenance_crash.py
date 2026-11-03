"""A crash during the run-end maintenance (T12 spec D3; plan Task 4 Step 3).

The maintenance changes only the working copy, and only the run-end publish publishes it, so a kill inside the
rewrite, between the rewrite and the merge, or inside the merge leaves new data files that no published catalog names.
The next run sweeps them (the T3 run anchors exempt them from the large-sweep guard) and completes its own
maintenance; at no point does a published version name a file that is not stored.

A crash is a `_Crash(BaseException)`: no `except Exception` in the run catches it, and with `Lease.release` and the
heartbeat patched out the lease stays held, as after a kill (the pattern of `test_crash_recovery.py`). The maintenance
needs a table with delete files and more than `MERGE_MIN_FILES` files; the wrapper adds `ops_probe` (20 inserts and a
partial delete) to the run's working copy just before the real maintenance. The lake is encrypted, so the inserts are
files, not inlined rows.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest
from lakekit import lake_roots
from runkit import FakeAdapter, FakeParser, active, entry, save_settings

import lakehouse.runs.run as run_module
from lakehouse.catalog import maintenance as maintenance_module
from lakehouse.catalog.maintenance import MERGE_MIN_FILES, MaintenanceReport
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.verify import verify_lake
from lakehouse.config.model import CheckpointConfig
from lakehouse.runs.history import read_table
from lakehouse.runs.run import run_ingestion
from lakehouse.storage.lease import HEARTBEAT, LEASE_TTL, TAKEOVER_GRACE, Lease
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

MODULE = "idsse_public"
KEY_ENV = {"LAKEHOUSE_ENCRYPTION_KEY": "c" * 64}
PROBE_INSERTS = MERGE_MIN_FILES + 4


class _Crash(BaseException):
    """A simulated kill."""


class _CrashingConn:
    """The working connection, except that the call whose SQL holds `marker` runs and then crashes before it returns:
    its files are written and the working copy holds its result, as when the process dies right after it."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, marker: str) -> None:
        self._conn = conn
        self._marker = marker

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> duckdb.DuckDBPyConnection:
        result = self._conn.execute(sql, params)
        if self._marker in sql:
            result.fetchall()
            raise _Crash
        return result


class _Shim:
    def __init__(self, conn: Any) -> None:
        self.conn = conn


def _fake(natives: Sequence[str]) -> FakeAdapter:
    files = {(native, "events"): f"{native}-a\n{native}-b".encode() for native in natives}
    return FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, native) for native in natives], files)


def _go(storage: LocalStorage, natives: Sequence[str], work: Path, **kwargs: Any) -> Any:
    return run_ingestion(
        storage=storage,
        trigger="cli",
        adapters={MODULE: _fake(natives)},
        parsers={"idsse": FakeParser("idsse")},
        work_dir=work,
        env=KEY_ENV,
        dbt=None,
        **kwargs,
    )


def _after(minutes: float) -> Callable[[], datetime]:
    def clock() -> datetime:
        return datetime.now(UTC) + LEASE_TTL + TAKEOVER_GRACE + timedelta(minutes=minutes)

    return clock


def _stored(base: Path) -> set[Path]:
    roots = (base / "lake" / "data", base / "lake-restricted" / "data")
    return {path for root in roots for path in root.rglob("*") if path.is_file()}


def _probe(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE lake.main.ops_probe (k BIGINT, note VARCHAR)")
    for i in range(PROBE_INSERTS):
        conn.execute("INSERT INTO lake.main.ops_probe SELECT ? * 100 + range, 'probe' FROM range(20)", [i])
    conn.execute("DELETE FROM lake.main.ops_probe WHERE k % 7 = 0")


def _with_probe(
    monkeypatch: pytest.MonkeyPatch, base: Path, *, crash: str | None
) -> tuple[list[set[Path]], list[MaintenanceReport]]:
    """Wrap the run's maintenance: add `ops_probe`, note the stored files, then run the real maintenance, crashing at
    `crash` (`in_rewrite`, `after_rewrite`, `in_merge`) when given."""
    real = maintenance_module.maintain_tables
    before: list[set[Path]] = []
    reports: list[MaintenanceReport] = []

    def maintain(cat: Any, **kwargs: Any) -> MaintenanceReport:
        _probe(cat.conn)
        before.append(_stored(base))
        target: Any = cat
        if crash == "in_rewrite":
            target = _Shim(_CrashingConn(cat.conn, "ducklake_rewrite_data_files"))
        elif crash == "in_merge":
            target = _Shim(_CrashingConn(cat.conn, "ducklake_merge_adjacent_files"))
        elif crash == "after_rewrite":
            real_windows = maintenance_module._windows  # pyright: ignore[reportPrivateUsage]

            def crashing_windows(cat: Any, table: str) -> list[Any]:
                if table == "ops_probe":  # the probe's rewrite has run; the merge has not started
                    raise _Crash
                return real_windows(cat, table)

            monkeypatch.setattr(maintenance_module, "_windows", crashing_windows)
        reports.append(real(target, **kwargs))
        return reports[-1]

    monkeypatch.setattr(run_module, "maintain_tables", maintain)
    return before, reports


def _published(storage: LocalStorage) -> set[str]:
    with CatalogReader(storage, env=KEY_ENV).session() as conn:
        return {row["native_match_id"] for row in read_table(conn, "ops_matches")}


@pytest.mark.parametrize("crash", ["in_rewrite", "after_rewrite", "in_merge"])
def test_crash_in_maintenance_is_swept_and_redone(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash: str) -> None:
    roots = lake_roots(tmp_path)
    init_lake(ResolvedRoots(roots=roots, source="cli"), bootstrap_file=tmp_path / "boot.json")
    storage = LocalStorage(roots)
    save_settings(storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=1))
    first = ["M0001", "M0002"]
    assert _go(storage, first, tmp_path / "work-1").status == "ok"

    natives = [*first, "M0003", "M0004"]
    with monkeypatch.context() as patch:
        before, _ = _with_probe(patch, tmp_path, crash=crash)

        def never_released(self: Lease) -> None:
            return None

        def no_heartbeat(self: Lease, *, interval_s: float | None = None) -> None:
            return None

        patch.setattr(Lease, "release", never_released)
        patch.setattr(Lease, "start_heartbeat", no_heartbeat)
        started = time.monotonic()
        with pytest.raises(_Crash):
            _go(storage, natives, tmp_path / "work-crash")
        assert time.monotonic() - started < HEARTBEAT.total_seconds(), "the run crashed after its first renewal"
    orphans = _stored(tmp_path) - before[0]
    assert orphans, f"the crash {crash} left no file of the maintenance behind"
    assert verify_lake(storage).problems == []  # no published version names a file that is not stored
    assert _published(storage) == set(natives)  # the matches were published by their checkpoints

    with monkeypatch.context() as patch:
        _, reports = _with_probe(patch, tmp_path, crash=None)
        second = _go(storage, natives, tmp_path / "work-next", now=_after(1))
    assert second.status == "ok", second.errors
    [probe] = [t for t in reports[0].tables if t.table == "ops_probe"]
    assert (probe.files_before, probe.rewritten, probe.files_after) == (PROBE_INSERTS, True, 1)
    third = _go(storage, natives, tmp_path / "work-after", now=_after(31))
    assert third.status == "ok", third.errors
    assert not orphans & _stored(tmp_path), "the crashed maintenance's files were not swept"
    assert verify_lake(storage).problems == []
    assert _published(storage) == set(natives)
