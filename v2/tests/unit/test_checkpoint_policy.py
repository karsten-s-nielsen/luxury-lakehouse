"""The checkpoint policy: bounded in-run publishes, the guard before each publish, and grace-aware pruning (T3 spec
D1, D4, D8; plan round 3 Task 4)."""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from lakekit import T0, LakeKit

from lakehouse.catalog.publish import listed_sequences, read_current, version_key
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import CheckpointConfig, load_defaults
from lakehouse.errors import GuardStopError, LeaseLostError
from lakehouse.runs import checkpoint as checkpoint_module
from lakehouse.runs._common import RUN_LOGGER_NAME
from lakehouse.runs.checkpoint import CheckpointPolicy
from lakehouse.storage import hf
from lakehouse.storage.keys import Root
from lakehouse.storage.local import LocalStorage


class _Sink:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


class _Collect(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def logs() -> Iterator[_Collect]:
    handler = _Collect()
    logger = logging.getLogger(RUN_LOGGER_NAME)
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)


def _at(kit: LakeKit, minutes: float) -> None:
    kit.clock.now = T0 + timedelta(minutes=minutes)


def _setup(kit: LakeKit, sink: _Sink | None = None, **config: int) -> tuple[WorkingCatalog, CheckpointPolicy]:
    """A pre-run run-end version (seq 1) at T0, then a run's working catalog and its policy at T0."""
    _at(kit, 0)
    base = kit.catalog("run-0")
    base.commit("run_end")
    base.close()
    cat = kit.catalog("run-1")
    policy = CheckpointPolicy(
        cat,
        CheckpointConfig(**config),
        retention=kit.settings.retention,
        run_id="run-1",
        now=kit.clock,
        errors=sink if sink is not None else _Sink(),
        storage=kit.storage,
    )
    return cat, policy


def test_maybe_publishes_after_minutes(kit: LakeKit) -> None:
    cat, policy = _setup(kit, minutes=5, matches=1000)
    _at(kit, 4 + 59 / 60)
    assert policy.maybe("match") is None
    _at(kit, 5)
    record = policy.maybe("match")
    assert record is not None
    assert (record.kind, record.reason) == ("in_run", "threshold_time")
    assert read_current(kit.storage).seq == record.seq == 2
    cat.close()


def test_maybe_publishes_after_matches(kit: LakeKit) -> None:
    cat, policy = _setup(kit, matches=3)
    assert policy.maybe("match") is None
    assert policy.maybe("match") is None
    record = policy.maybe("match")
    assert record is not None
    assert record.reason == "threshold_matches"
    cat.close()


def test_now_always_publishes_and_resets_counters(kit: LakeKit) -> None:
    cat, policy = _setup(kit, matches=3)
    assert policy.maybe("match") is None
    assert policy.maybe("match") is None
    barrier = policy.now("move")
    assert (barrier.kind, barrier.reason) == ("in_run", "move")
    assert policy.maybe("match") is None  # the counter restarted at the barrier
    assert policy.maybe("match") is None
    assert policy.maybe("match") is not None
    assert [r.seq for r in policy.published] == [2, 3]
    cat.close()


def test_guard_runs_before_every_publish(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    cat, policy = _setup(kit, matches=1)
    order: list[str] = []

    def guard(c: WorkingCatalog) -> None:
        order.append("guard")

    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", guard)
    real_commit = cat.commit

    def commit(kind: Any) -> Any:
        order.append("commit")
        return real_commit(kind)

    monkeypatch.setattr(cat, "commit", commit)
    policy.maybe("match")
    policy.now("move")
    policy.run_end()
    assert order == ["guard", "commit"] * 3

    def broken(c: WorkingCatalog) -> None:
        raise GuardStopError("bronze table t has 1 rows whose _visibility disagrees with ops_matches")

    monkeypatch.setattr(checkpoint_module, "assert_bronze_integrity", broken)
    before = listed_sequences(kit.storage)
    with pytest.raises(GuardStopError):
        policy.now("move")
    assert listed_sequences(kit.storage) == before
    cat.close()


def test_lost_lease_publishes_nothing(kit: LakeKit) -> None:
    cat, policy = _setup(kit)
    kit.lease_of(cat).held = False
    with pytest.raises(LeaseLostError):
        policy.now("move")
    assert read_current(kit.storage).seq == 1
    cat.close()


def test_in_run_pruning_respects_grace(kit: LakeKit) -> None:
    cat, policy = _setup(kit, grace_minutes=10)
    for minute in (0, 1, 2):
        _at(kit, minute)
        policy.now("move")
    # the run published 2, 3, 4; seq 1 is the pre-run run-end version
    assert listed_sequences(kit.storage) == [1, 2, 3, 4]
    _at(kit, 13)
    policy.now("move")  # 5: 2's successor published 12 min ago, 3's 11 min ago, 4's just now
    assert listed_sequences(kit.storage) == [1, 4, 5]
    cat.close()


def test_barrier_burst_keeps_versions_inside_grace(kit: LakeKit) -> None:
    cat, policy = _setup(kit, grace_minutes=10)
    for step in range(20):
        _at(kit, step * 3 / 60)
        policy.now("move")
    assert listed_sequences(kit.storage) == list(range(1, 22))
    cat.close()


class _LockedOnce(LocalStorage):
    """A Windows reader holds one version open: its first delete raises `PermissionError`."""

    def __init__(self, storage: LocalStorage, locked: str) -> None:
        super().__init__(storage.roots)
        self.locked: str | None = locked

    def delete(self, root: Root, key: str, *, missing_ok: bool = False) -> None:
        if key == self.locked:
            self.locked = None
            raise PermissionError(f"{key} is open in another process")
        super().delete(root, key, missing_ok=missing_ok)


def test_postponed_delete_still_counts_as_retained(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    _at(kit, 0)
    base = kit.catalog("run-0")
    base.commit("run_end")
    base.close()
    cat = kit.catalog("run-1")
    storage = _LockedOnce(kit.storage, version_key(2))
    policy = CheckpointPolicy(
        cat,
        CheckpointConfig(grace_minutes=1),
        retention=kit.settings.retention,
        run_id="run-1",
        now=kit.clock,
        errors=_Sink(),
        storage=storage,
    )
    for minute in (0, 2):
        _at(kit, minute)
        policy.now("move")  # 2, 3
    retained = policy.retained_versions
    _at(kit, 4)
    policy.now("move")  # 4: 2 is past the grace, but its delete is postponed
    assert kit.storage.exists("lake", version_key(2))
    assert policy.retained_versions == retained + 1
    _at(kit, 6)
    policy.now("move")  # 5: the postponed 2 goes now, and 3 with it
    assert not kit.storage.exists("lake", version_key(2))
    assert not kit.storage.exists("lake", version_key(3))
    cat.close()


def test_publish_logs_d8_fields(kit: LakeKit, logs: _Collect) -> None:
    cat, policy = _setup(kit)
    policy.now("move")
    [record] = [r for r in logs.records if r.getMessage() == "catalog publish"]
    fields = record.__dict__
    assert (fields["seq"], fields["kind"], fields["reason"]) == (2, "in_run", "move")
    for name in ("bytes", "bytes_published_run", "retained_versions", "guard_ms"):
        assert isinstance(fields[name], int), name
    assert fields["bytes"] == fields["bytes_published_run"] > 0
    cat.close()


def test_catalog_growth_warning_on_versions(kit: LakeKit) -> None:
    sink = _Sink()
    cat, policy = _setup(kit, sink, warn_versions=2, grace_minutes=60)
    policy.now("move")  # versions 1, 2
    assert sink.calls == []
    policy.now("move")  # versions 1, 2, 3: over the threshold
    [call] = sink.calls
    assert (call["kind"], call["severity"]) == ("catalog_growth", "warning")
    assert "3 retained catalog versions" in call["message"]
    policy.now("move")
    assert len(sink.calls) == 1  # once per run per threshold
    cat.close()


class _Limited(LocalStorage):
    """A storage whose conditional writes stop at `limit` bytes, as HF's single-part limit does (T6 spec D2)."""

    def __init__(self, roots: Any, limit: int) -> None:
        super().__init__(roots)
        self._limit = limit

    @property
    def single_part_limit(self) -> int | None:
        return self._limit


def test_catalog_warning_band_below_the_limit(kit: LakeKit) -> None:
    """The growth warning fires before a catalog can reach the single-part limit (T6 plan B-04): the default is at most
    half the HF limit, and the policy clamps a larger saved threshold to half the storage's limit."""
    assert CheckpointConfig().warn_catalog_mb * 1024 * 1024 <= hf.SINGLE_PART_LIMIT // 2
    assert load_defaults("laptop").checkpoint.warn_catalog_mb == CheckpointConfig().warn_catalog_mb
    _at(kit, 0)
    base = kit.catalog("run-0")
    base.commit("run_end")
    base.close()
    size = read_current(kit.storage).size
    kit.storage = _Limited(kit.roots, limit=int(size * 1.6))  # the next copy lies between half the limit and it
    sink = _Sink()
    cat = kit.catalog("run-1")
    policy = CheckpointPolicy(
        cat,
        CheckpointConfig(warn_catalog_mb=2048),  # a saved threshold far above this storage's limit
        retention=kit.settings.retention,
        run_id="run-1",
        now=kit.clock,
        errors=sink,
        storage=kit.storage,
    )
    policy.now("move")
    [call] = [c for c in sink.calls if c["kind"] == "catalog_growth"]
    assert "refuses" in call["message"]
    cat.close()


def test_catalog_growth_warning_on_size(kit: LakeKit, monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _Sink()
    cat, policy = _setup(kit, sink, warn_catalog_mb=1)
    real_commit = cat.commit

    def big(kind: Any) -> Any:
        info = real_commit(kind)
        return type(info)(
            seq=info.seq,
            kind=info.kind,
            key=info.key,
            sha256=info.sha256,
            size=3 * 1024 * 1024,
            published_at=info.published_at,
        )

    monkeypatch.setattr(cat, "commit", big)
    policy.now("move")
    [call] = sink.calls
    assert call["kind"] == "catalog_growth"
    assert "3 MB" in call["message"]
    cat.close()


def test_totals(kit: LakeKit, tmp_path: Path) -> None:
    cat, policy = _setup(kit, matches=1)
    policy.maybe("match")
    policy.run_end()
    totals = policy.totals()
    assert totals.publishes == 2
    assert totals.bytes_published == sum(r.size for r in policy.published)
    assert totals.max_retained >= 2
    cat.close()
