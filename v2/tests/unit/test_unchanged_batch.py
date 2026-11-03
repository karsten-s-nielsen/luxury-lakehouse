"""The unchanged-match ops writes ride one DuckLake transaction per publish (T3 spec O-5, decision 20; plan round 3
Task 9b). On the encrypted lake each unchanged match's signal refresh and source row stored files and added snapshots
(the spike: 3.0 files and 4.0 snapshots per unchanged match per run). They are buffered and written in one transaction
just before each publish and before the run end's resolution and dbt."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import CheckpointConfig
from lakehouse.runs import checkpoint as checkpoint_module
from lakehouse.runs import run as run_module
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, run_ingestion

KEY = {"LAKEHOUSE_ENCRYPTION_KEY": "a1" * 32}
MODULE = "idsse_public"


def _adapter(natives: list[str]) -> FakeAdapter:
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    return FakeAdapter(MODULE, "idsse", "public", [entry(MODULE, n) for n in natives], files)


def _run(kit: LakeKit, natives: list[str], *, at: Any = NOW, **kwargs: Any) -> RunResult:
    kwargs.setdefault("dbt", None)
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={MODULE: _adapter(natives)},
        parsers={"idsse": FakeParser("idsse")},
        now=lambda: at,
        work_dir=kit.base / "work",
        env=KEY,
        **kwargs,
    )


def _snapshots(kit: LakeKit) -> int:
    with CatalogReader(kit.storage, env=KEY).session() as conn:
        row = conn.execute("SELECT count(*) FROM __ducklake_metadata_lake.ducklake_snapshot").fetchone()
    assert row is not None
    return int(row[0])


def _last_listed(kit: LakeKit) -> set[Any]:
    with CatalogReader(kit.storage, env=KEY).session() as conn:
        return {row["last_listed"] for row in read_table(conn, "ops_match_sources")}


NATIVES = [f"U{index:03d}" for index in range(50)]


def test_unchanged_writes_ride_one_transaction_per_publish(kit: LakeKit) -> None:
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=1000))
    assert _run(kit, NATIVES).status == "ok"
    before = _snapshots(kit)
    result = _run(kit, NATIVES, at=NOW + timedelta(days=1))
    assert (result.status, result.counts.unchanged) == ("ok", 50)
    added = _snapshots(kit) - before
    assert added <= 10, added  # was about 4 per unchanged match (200); now one flush plus the run's own writes


def test_flush_runs_before_the_guard_and_the_commit(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=1000))
    assert _run(kit, NATIVES[:3]).status == "ok"
    order: list[str] = []
    real_guard = checkpoint_module.assert_bronze_integrity
    real_commit = WorkingCatalog.commit
    real_flush = run_module.MatchWriter.flush_unchanged

    def flush(self: Any) -> None:
        order.append("flush")
        real_flush(self)

    def guard(cat: WorkingCatalog) -> None:
        order.append("guard")
        real_guard(cat)

    def commit(self: WorkingCatalog, kind: Any) -> Any:
        order.append("commit")
        return real_commit(self, kind)

    monkeypatch.setattr(run_module.MatchWriter, "flush_unchanged", flush)
    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", guard)
    monkeypatch.setattr(WorkingCatalog, "commit", commit)
    assert _run(kit, NATIVES[:3], at=NOW + timedelta(days=1)).status == "ok"
    assert order[-3:] == ["flush", "guard", "commit"]  # the run-end publish carries the flushed rows


def test_unchanged_run_flushes_at_run_end(kit: LakeKit) -> None:
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=1000))
    assert _run(kit, NATIVES[:5]).status == "ok"
    later = NOW + timedelta(days=1)
    assert _run(kit, NATIVES[:5], at=later).status == "ok"
    assert _last_listed(kit) == {later}  # every unchanged match's source row reached the run-end publish


def test_crash_loses_the_buffer_and_the_next_run_redoes_it(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=1000))
    assert _run(kit, NATIVES[:5]).status == "ok"

    class _Crash(BaseException):
        pass

    def crash(*args: Any, **kwargs: Any) -> Any:
        raise _Crash

    with monkeypatch.context() as patch:
        patch.setattr(run_module, "run_resolution", crash)
        with pytest.raises(_Crash):
            _run(kit, NATIVES[:5], at=NOW + timedelta(days=1))
    assert _last_listed(kit) == {NOW}  # the buffered refresh was never published
    later = NOW + timedelta(days=2)
    assert _run(kit, NATIVES[:5], at=later).status == "ok"
    assert _last_listed(kit) == {later}
