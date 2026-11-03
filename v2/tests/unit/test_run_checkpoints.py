"""The run engine publishes through the checkpoint policy (T3 spec D1, D2, D4 at the run end, D8, D9; plan round 3
Task 5). A recording subclass of the real policy records which site asked for which publish."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest
from lakekit import Clock, LakeKit
from runkit import NOW, FakeAdapter, FakeParser, ListingAdapter, ListingParser, active, entry, save_settings

from lakehouse.catalog.publish import listed_sequences
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import CheckpointConfig
from lakehouse.runs import checkpoint as checkpoint_module
from lakehouse.runs import run as run_module
from lakehouse.runs._common import RUN_LOGGER_NAME
from lakehouse.runs.checkpoint import CheckpointPolicy, PublishRecord
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, reparse_matches, run_ingestion
from lakehouse.storage.lease import write_skip_record


class Recording(CheckpointPolicy):
    calls: ClassVar[list[tuple[str, str]]] = []
    instances: ClassVar[list[Recording]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        Recording.instances.append(self)

    def maybe(self, reason: str) -> PublishRecord | None:
        Recording.calls.append(("maybe", reason))
        return super().maybe(reason)

    def now(self, reason: str) -> PublishRecord:
        Recording.calls.append(("now", reason))
        return super().now(reason)

    def run_end(self) -> PublishRecord:
        Recording.calls.append(("run_end", "run_end"))
        return super().run_end()


@pytest.fixture(autouse=True)
def recording(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[Recording]]:
    Recording.calls = []
    Recording.instances = []
    monkeypatch.setattr(run_module, "CheckpointPolicy", Recording)
    yield Recording


def _adapter(
    *natives: str, module: str = "idsse_public", provider: str = "idsse", channel: str = "public"
) -> FakeAdapter:
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    return FakeAdapter(module, provider, channel, [entry(module, n) for n in natives], files)  # type: ignore[arg-type]


def _run(kit: LakeKit, *adapters: FakeAdapter, now: Any = None, **kwargs: Any) -> RunResult:
    kwargs.setdefault("dbt", None)
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={a.module_id: a for a in adapters},  # type: ignore[misc]
        parsers={a.provider: FakeParser(a.provider) for a in adapters},  # type: ignore[misc]
        now=now if now is not None else (lambda: NOW),
        work_dir=kit.base / "work",
        **kwargs,
    )


def _publishes() -> list[PublishRecord]:
    return [record for policy in Recording.instances for record in policy.published]


def test_match_write_calls_maybe(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    _run(kit, _adapter("J03WPY", "J03WMX"))
    assert Recording.calls.count(("maybe", "match")) == 2


def test_record_not_kept_calls_maybe(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("statsbomb_public"), active("statsbomb_pining")])
    public = _adapter("3788741", module="statsbomb_public", provider="statsbomb", channel="public")
    pining = _adapter("3788741", module="statsbomb_pining", provider="statsbomb", channel="pining")
    _run(kit, public, pining)
    assert ("maybe", "not_kept") in Recording.calls  # one channel's copy of the match is not the kept one


def test_listing_calls_maybe(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    adapter = ListingAdapter(
        "idsse_public", "idsse", "public", [], {}, listings={"players": b'[{"id": "1"}, {"id": "2"}]'}
    )
    run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={adapter.module_id: adapter},  # type: ignore[dict-item]
        parsers={adapter.provider: ListingParser(adapter.provider)},  # type: ignore[dict-item]
        now=lambda: NOW,
        work_dir=kit.base / "work",
        dbt=None,
    )
    assert ("maybe", "listing") in Recording.calls


def test_move_native_calls_now(kit: LakeKit) -> None:
    env = {"LAKEHOUSE_ENCRYPTION_KEY": "a" * 64}
    save_settings(kit.storage, [active("statsbomb_pining")])
    pining = _adapter("3788741", module="statsbomb_pining", provider="statsbomb", channel="pining")
    _run(kit, pining, env=env)
    pining.entries = [entry("statsbomb_pining", "3788741", visibility="private")]
    Recording.calls = []
    _run(kit, pining, env=env)
    assert ("now", "move") in Recording.calls


def test_fold_skips_calls_now(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    write_skip_record(kit.storage, run_id="run-skipped", reason="skipped: run in progress", blocking_run_id=None)
    _run(kit, _adapter("J03WPY"))
    assert Recording.calls[0] == ("now", "fold_skips")


def test_run_end_is_guarded_and_publishes_pending(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    save_settings(kit.storage, [active("idsse_public")], checkpoint=CheckpointConfig(matches=1000))
    guards: list[str] = []
    real_guard = checkpoint_module.assert_bronze_integrity

    def guard(cat: WorkingCatalog) -> None:
        guards.append("guard")
        real_guard(cat)

    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", guard)
    _run(kit, _adapter("J03WPY", "J03WMX", "J03WN1"))
    assert [(r.kind, r.reason) for r in _publishes()] == [("run_end", "run_end")]
    assert guards == ["guard"]
    with CatalogReader(kit.storage).session() as conn:
        assert len(read_table(conn, "ops_matches")) == 3


def test_stopped_run_end_is_guarded(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    save_settings(kit.storage, [active("idsse_public")], checkpoint=CheckpointConfig(matches=1000))
    guards: list[str] = []

    def guard(cat: WorkingCatalog) -> None:
        guards.append("guard")

    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", guard)
    adapter = _adapter("J03WPY", "J03WMX", "J03WN1")
    result = _run(kit, adapter, stop=lambda: len(adapter.fetched) >= 2)
    assert result.status == "failed"
    assert [(r.kind, r.reason) for r in _publishes()] == [("run_end", "run_end")]
    assert guards == ["guard"]
    with CatalogReader(kit.storage).session() as conn:
        assert len(read_table(conn, "ops_matches")) == 2  # the stopped run's pending matches ride the run end


def test_finish_sweeps_before_and_prunes_after_the_run_end_commit(
    kit: LakeKit, monkeypatch: pytest.MonkeyPatch
) -> None:
    save_settings(kit.storage, [active("idsse_public")])
    order: list[str] = []
    real_cleanup = run_module.run_cleanup
    real_plan = checkpoint_module.plan_version_pruning
    real_commit = WorkingCatalog.commit

    def cleanup(*args: Any, **kwargs: Any) -> Any:
        order.append("cleanup")
        return real_cleanup(*args, **kwargs)

    def plan(*args: Any, **kwargs: Any) -> list[int]:
        order.append("prune")
        return real_plan(*args, **kwargs)

    def commit(self: WorkingCatalog, kind: Any) -> Any:
        order.append(f"commit:{kind}")
        return real_commit(self, kind)

    monkeypatch.setattr(run_module, "run_cleanup", cleanup)
    monkeypatch.setattr(checkpoint_module, "plan_version_pruning", plan)
    monkeypatch.setattr(WorkingCatalog, "commit", commit)
    _run(kit, _adapter("J03WPY"))
    assert order[-3:] == ["cleanup", "commit:run_end", "prune"]


def _crashed_then_run(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """A crashed predecessor left in-run versions 1 and 2 (30 minutes ago); this run publishes 3 and 4 (M = 1, G =
    1 minute) and its run end 5 two minutes later. Returns the versions left after the run."""
    base = datetime.now(UTC).replace(microsecond=0)
    clock = Clock(base - timedelta(minutes=30))
    kit.clock.now = clock.now
    crashed = kit.catalog("run-crashed")
    kit.add_match(crashed, 500, visibility="public", frames=1, offset=0)
    crashed.commit("in_run")
    kit.add_match(crashed, 501, visibility="public", frames=1, offset=0)
    crashed.commit("in_run")
    crashed.close()
    save_settings(kit.storage, [active("idsse_public")], checkpoint=CheckpointConfig(matches=1, grace_minutes=1))
    clock.now = base
    real_cleanup = run_module.run_cleanup

    def later_cleanup(*args: Any, **kwargs: Any) -> Any:
        clock.now = base + timedelta(minutes=2)  # the run end comes two minutes after the last match publish
        return real_cleanup(*args, **{**kwargs, "now": clock.now})

    monkeypatch.setattr(run_module, "run_cleanup", later_cleanup)
    _run(kit, _adapter("J03WPY", "J03WMX"), now=clock)
    return listed_sequences(kit.storage)


def test_run_end_prunes_own_and_crashed_versions(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    left = _crashed_then_run(kit, monkeypatch)
    for gone in (1, 2, 3):  # the crashed run's two in-run versions, and this run's first, all past the grace
        assert gone not in left


def test_run_end_keeps_versions_inside_grace(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    left = _crashed_then_run(kit, monkeypatch)
    assert left == [4, 5]  # 4's successor (the run end) was published just now: inside the grace


class _Collect(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_run_end_log_carries_publish_totals(kit: LakeKit) -> None:
    save_settings(kit.storage, [active("idsse_public")], checkpoint=CheckpointConfig(matches=1))
    handler = _Collect()
    logger = logging.getLogger(RUN_LOGGER_NAME)
    logger.addHandler(handler)
    try:
        _run(kit, _adapter("J03WPY", "J03WMX"))
    finally:
        logger.removeHandler(handler)
    [record] = [r for r in handler.records if r.getMessage() == "catalog totals"]
    fields = record.__dict__
    assert fields["publishes"] == 3  # two matches (M = 1) and the run end
    for name in ("bytes_published", "max_retained", "guard_ms", "compaction_ms"):
        assert isinstance(fields[name], int), name


def test_reader_sees_matches_per_publish(kit: LakeKit) -> None:
    """Decision 2 / 15: a reader sees a match's write only after the publish that carries it."""
    save_settings(kit.storage, [active("idsse_public")], checkpoint=CheckpointConfig(matches=2))
    adapter = _adapter("J03WPY", "J03WMX", "J03WN1")
    seen: dict[int, int] = {}

    def visible() -> int:
        try:
            with CatalogReader(kit.storage).session() as conn:
                return len(read_table(conn, "ops_matches"))
        except FileNotFoundError:
            return 0  # nothing published yet

    def on_fetch() -> None:
        seen[len(adapter.fetched)] = visible()

    adapter.on_fetch = on_fetch
    _run(kit, adapter)
    assert (seen[2], seen[3]) == (0, 2)  # after match 1: unpublished; after match 2: one publish carries both


def test_reparse_all_publishes_per_checkpoint(kit: LakeKit) -> None:
    natives = [f"J0{i:04d}" for i in range(10)]
    save_settings(kit.storage, [active("idsse_public")], raw_archive=True, checkpoint=CheckpointConfig(matches=1000))
    _run(kit, _adapter(*natives))
    save_settings(kit.storage, [active("idsse_public")], raw_archive=True, checkpoint=CheckpointConfig(matches=4))
    Recording.instances = []
    result = reparse_matches(
        storage=kit.storage,
        parsers={"idsse": FakeParser("idsse", version="2")},
        target="all",
        now=lambda: NOW,
        work_dir=kit.base / "work-reparse",
        dbt=None,
    )
    assert result.status == "ok"
    kinds = [r.kind for r in _publishes()]
    assert kinds == ["in_run", "in_run", "run_end"]  # 10 re-parsed units with M = 4: after units 4 and 8
