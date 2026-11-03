"""M1 guard (plan Task 0): the run logs and spans the move/write/commit steps under the fixed logger name
`lakehouse.runs.run`. This pins the observable the Logs page shows, so the M1 extraction (which moves `_move`
to `MatchFiles` and `_write` to `MatchWriter`) cannot silently change the logger name or drop the spans. It is
green on today's `run.py`; after extraction a collaborator using `get_logger(__name__)` turns it red (plan
Task 4 Step 6).
"""

from __future__ import annotations

from collections.abc import Sequence

from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings, with_entry

from lakehouse.cli import observe_run
from lakehouse.config.model import CheckpointConfig
from lakehouse.observability import LogRecord, read_logs, read_task_rows
from lakehouse.runs.run import new_run_id, run_ingestion

RUN_LOGGER = "lakehouse.runs.run"
_ENV = {"LAKEHOUSE_EXECUTION_CONTEXT": "local"}


def _statsbomb_adapter() -> FakeAdapter:
    listed = entry("statsbomb_public", "9100001", signals={"events": "s1"}, visibility="public")
    return FakeAdapter("statsbomb_public", "statsbomb", "public", [listed], {("9100001", "events"): b"one\ntwo\n"})


def _loggers_for(records: Sequence[LogRecord], span: str) -> set[str]:
    return {r.logger for r in records if r.msg == span}


def test_logger_and_spans_on_move_and_write(kit: LakeKit) -> None:
    adapter = _statsbomb_adapter()
    parsers = {"statsbomb": FakeParser("statsbomb")}
    # M = 1: the match write publishes at once, so its commit span carries a catalog_seq (T3 spec D1)
    save_settings(kit.storage, [active("statsbomb_public")], checkpoint=CheckpointConfig(matches=1))

    # Run 1: a new public match — the write and commit spans come from the write path (MatchWriter after M1).
    run1 = new_run_id(NOW)
    with observe_run(kit.storage, run1, _ENV):
        r1 = run_ingestion(
            storage=kit.storage,
            trigger="cli",
            adapters={"statsbomb_public": adapter},  # type: ignore[dict-item]
            parsers=parsers,  # type: ignore[arg-type]
            now=lambda: NOW,
            run_id=run1,
            work_dir=kit.base / "work1",
            env=_ENV,
        )
    assert r1.status == "ok", r1.errors
    logs1 = read_logs(kit.storage, run1, limit=5000)
    tasks1 = {row.task for row in read_task_rows(kit.storage, run1)}
    assert {"lakehouse.run.write", "lakehouse.run.commit"} <= tasks1
    assert _loggers_for(logs1, "lakehouse.run.write") == {RUN_LOGGER}
    assert _loggers_for(logs1, "lakehouse.run.commit") == {RUN_LOGGER}
    # span attributes preserved (spec §7.2): the write span's layer, and the per-match commit span's catalog_seq
    write_rec = next(r for r in logs1 if r.msg == "lakehouse.run.write")
    assert write_rec.extra.get("layer") == "bronze"
    commit_rec = next(r for r in logs1 if r.msg == "lakehouse.run.commit" and "module_id" in r.extra)
    assert "catalog_seq" in commit_rec.extra

    # Run 2: the same match now listed private, same signal — a visibility-only move (the move span comes from
    # the move path, MatchFiles after M1). No re-fetch, no re-parse.
    with_entry(adapter, "9100001", visibility="private")
    run2 = new_run_id(NOW)
    with observe_run(kit.storage, run2, _ENV):
        r2 = run_ingestion(
            storage=kit.storage,
            trigger="cli",
            adapters={"statsbomb_public": adapter},  # type: ignore[dict-item]
            parsers=parsers,  # type: ignore[arg-type]
            now=lambda: NOW,
            run_id=run2,
            work_dir=kit.base / "work2",
            env=_ENV,
        )
    assert r2.status == "ok", r2.errors
    logs2 = read_logs(kit.storage, run2, limit=5000)
    tasks2 = {row.task for row in read_task_rows(kit.storage, run2)}
    assert "lakehouse.run.move" in tasks2, "run 2 did not move the match (visibility flip not exercised)"
    assert _loggers_for(logs2, "lakehouse.run.move") == {RUN_LOGGER}
    move_rec = next(r for r in logs2 if r.msg == "lakehouse.run.move")
    assert "match_key" in move_rec.extra  # the move span keeps its match_key attribute
