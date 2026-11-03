"""Bounded catalog checkpoints over the real S3 gateway (T3 spec D1, D4, D5, D8; plan round 3 Task 12).

A run with M = 2 over 5 synthetic matches publishes per checkpoint through the gateway. The grace G is 1 minute (the
smallest the settings allow: a grace of 0 would defeat the reader protection of D4), and the run pauses longer than G
after its second checkpoint, so the run-end pruning deletes the first in-run version (superseded more than G ago) and
keeps the second (superseded by the run-end publish moments ago). The run anchor and the acquisition marker are
write-once objects under the gateway prefix `runs/lease/`. A reader whose attached version is deleted mid-session
completes or raises `StaleCatalogError`, never another error (the T3 spike measured that the session completes).
Both tests run only on the throwaway `*-test` bucket pair (`test_lake`).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any, ClassVar

import duckdb
import pytest
from hfkit import RUN, reset_lease
from runkit import FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.publish import listed_sequences, read_current, version_key
from lakehouse.config.model import CheckpointConfig
from lakehouse.errors import StaleCatalogError
from lakehouse.runs import run as run_module
from lakehouse.runs.checkpoint import CheckpointPolicy, PublishRecord
from lakehouse.runs.run import run_ingestion
from lakehouse.services.catalog import read_catalog
from lakehouse.storage.hf import HfStorage
from lakehouse.storage.lease import anchor_key, marker_key

pytestmark = [pytest.mark.hf, pytest.mark.hf_job]

MODULE = "skillcorner_public"
PROVIDER = "skillcorner"
MATCHES = [f"cp-{RUN}-{i}" for i in range(5)]  # new per session, so the test reruns on a reused test bucket
GRACE_MINUTES = 1
PAUSE_S = GRACE_MINUTES * 60 + 5  # after the second checkpoint, longer than G


class Recording(CheckpointPolicy):
    published_all: ClassVar[list[PublishRecord]] = []

    def maybe(self, reason: str) -> PublishRecord | None:
        record = super().maybe(reason)
        if record is not None:
            Recording.published_all.append(record)
        return record

    def now(self, reason: str) -> PublishRecord:
        record = super().now(reason)
        Recording.published_all.append(record)
        return record

    def run_end(self) -> PublishRecord:
        record = super().run_end()
        Recording.published_all.append(record)
        return record


@pytest.fixture
def recording(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    Recording.published_all = []
    monkeypatch.setattr(run_module, "CheckpointPolicy", Recording)
    yield


def _generations(storage: HfStorage, suffix: str) -> list[int]:
    return sorted(
        int(info.key.rsplit("/", 1)[-1].removesuffix(suffix))
        for info in storage.list("lake", "runs/lease/")
        if info.key.endswith(suffix)
    )


def test_checkpoint_publish_and_prune_over_gateway(test_lake: HfStorage, tmp_path: Path, recording: None) -> None:
    storage = test_lake
    reset_lease(storage)  # also removes earlier anchors and markers: the run's own are then the only ones
    save_settings(storage, [active(MODULE)], checkpoint=CheckpointConfig(matches=2, grace_minutes=GRACE_MINUTES))
    adapter = FakeAdapter(
        MODULE,
        PROVIDER,
        "public",
        [entry(MODULE, native) for native in MATCHES],
        {(native, "events"): f"{native}-a\n{native}-b".encode() for native in MATCHES},
    )

    def pause_before_the_last_match() -> None:  # the second checkpoint published after match 4
        if len(adapter.fetched) == len(MATCHES):
            time.sleep(PAUSE_S)

    adapter.on_fetch = pause_before_the_last_match
    result = run_ingestion(
        storage=storage,
        trigger="schedule",
        adapters={MODULE: adapter},
        parsers={PROVIDER: FakeParser(PROVIDER)},
        work_dir=tmp_path / "work",
        dbt=None,
    )
    assert result.status == "ok", result.errors
    assert result.counts.added == len(MATCHES)

    published = Recording.published_all
    assert [(record.kind, record.reason) for record in published] == [
        ("in_run", "threshold_matches"),
        ("in_run", "threshold_matches"),
        ("run_end", "run_end"),
    ], published
    first, second, end = published
    assert end.at - second.at > timedelta(minutes=GRACE_MINUTES)
    assert read_current(storage).seq == end.seq
    left = set(listed_sequences(storage))
    assert first.seq not in left, "the in-run version superseded more than G ago survived the run-end pruning"
    assert second.seq in left, "the in-run version superseded less than G ago was deleted (the grace was ignored)"

    [generation] = _generations(storage, ".anchor")
    assert _generations(storage, ".acquired") == [generation]
    for key in (anchor_key(generation), marker_key(generation)):
        before = storage.get("lake", key)
        assert storage.put_if_absent("lake", key, b"overwrite") is False, f"{key} is not write-once on the gateway"
        assert storage.get("lake", key) == before


def test_reader_on_deleted_version_is_stale_or_completes(test_lake: HfStorage) -> None:
    storage = test_lake
    current = read_current(storage)
    key = version_key(current.seq)
    saved = storage.get("lake", key)

    deleted: list[str] = []

    def read_after_delete(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
        if not deleted:  # the version this session attached disappears mid-session (once; a retry reads on)
            storage.delete("lake", key)
            deleted.append(key)
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = 'main'"
        ).fetchall()
        counts: dict[str, Any] = {}
        for (table,) in tables:
            row = conn.execute(f'SELECT count(*) FROM lake.main."{table}"').fetchone()  # noqa: S608 - catalog names
            counts[table] = row[0] if row else None
        return counts

    try:
        try:
            counts = read_catalog(storage, read_after_delete, empty={})
        except StaleCatalogError:
            return  # the allowed failure: the page shows it and its next read opens the new CURRENT
        assert counts, "the session read no table"
    finally:
        storage.put("lake", key, saved)  # leave the test lake consistent for the next test
