"""Batched private-to-public moves and the one-publish register move (T3 spec D2, D3, decisions 8 and 10; plan round 3
Task 9). The privacy direction is never batched: each public-to-private move publishes on its own, at once."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, ClassVar

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.reader import CatalogReader
from lakehouse.config.model import CheckpointConfig
from lakehouse.ids.keys import match_key
from lakehouse.runs import run as run_module
from lakehouse.runs.checkpoint import CheckpointPolicy, PublishRecord
from lakehouse.runs.history import read_table
from lakehouse.runs.run import RunResult, run_ingestion

MODULE = "statsbomb_pining"
KEY_ENV = {"LAKEHOUSE_ENCRYPTION_KEY": "c" * 64}
PATHS: dict[str, dict[str, str] | None] = {"register": None, "native": KEY_ENV}


class Recording(CheckpointPolicy):
    published_all: ClassVar[list[PublishRecord]] = []
    fail_on: ClassVar[str | None] = None

    def now(self, reason: str) -> PublishRecord:
        if reason == Recording.fail_on:
            raise _Crash
        record = super().now(reason)
        Recording.published_all.append(record)
        return record

    def maybe(self, reason: str) -> PublishRecord | None:
        record = super().maybe(reason)
        if record is not None:
            Recording.published_all.append(record)
        return record


class _Crash(BaseException):
    """A simulated kill inside the visibility pass."""


@pytest.fixture(autouse=True)
def recording(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    Recording.published_all = []
    Recording.fail_on = None
    monkeypatch.setattr(run_module, "CheckpointPolicy", Recording)
    yield


def _adapter(visibility: dict[str, str]) -> FakeAdapter:
    natives = sorted(visibility)
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    entries = [entry(MODULE, n, visibility=visibility[n]) for n in natives]
    return FakeAdapter(MODULE, "statsbomb", "pining", entries, files)


def _run(kit: LakeKit, adapter: FakeAdapter, env: dict[str, str] | None) -> RunResult:
    return run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={MODULE: adapter},
        parsers={"statsbomb": FakeParser("statsbomb")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
        env=env,
        dbt=None,
    )


def _visibility(kit: LakeKit, env: dict[str, str] | None) -> dict[str, str]:
    with CatalogReader(kit.storage, env=env).session() as conn:
        return {row["native_match_id"]: row["visibility"] for row in read_table(conn, "ops_matches")}


def _flip(kit: LakeKit, env: dict[str, str] | None, before: dict[str, str], after: dict[str, str], batch: int) -> Any:
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(move_batch=batch))
    assert _run(kit, _adapter(before), env).status == "ok"
    Recording.published_all = []
    return _run(kit, _adapter(after), env)


def _reasons() -> list[str]:
    return [record.reason for record in Recording.published_all]


FIVE = [f"M{index}" for index in range(5)]


@pytest.mark.parametrize("path", sorted(PATHS))
@pytest.mark.parametrize(("batch", "publishes"), [(50, 1), (2, 3)])
def test_private_to_public_batch_publishes_once(kit: LakeKit, path: str, batch: int, publishes: int) -> None:
    env = PATHS[path]
    result = _flip(kit, env, dict.fromkeys(FIVE, "private"), dict.fromkeys(FIVE, "public"), batch)
    assert result.status == "ok"
    assert _reasons().count("move_batch") == publishes  # 5 moves: one batch, or batches of 2, 2 and 1
    assert "move" not in _reasons()
    assert _visibility(kit, env) == dict.fromkeys(FIVE, "public")  # the last batch closed at the end of the pass


@pytest.mark.parametrize("path", sorted(PATHS))
def test_public_to_private_publishes_per_match_in_a_mixed_pass(kit: LakeKit, path: str) -> None:
    env = PATHS[path]
    before = {**dict.fromkeys(["A1", "A2", "A3"], "public"), **dict.fromkeys(["B1", "B2", "B3", "B4"], "private")}
    after = {**dict.fromkeys(["A1", "A2", "A3"], "private"), **dict.fromkeys(["B1", "B2", "B3", "B4"], "public")}
    assert _flip(kit, env, before, after, 50).status == "ok"
    moves = [reason for reason in _reasons() if reason in ("move", "move_batch")]
    # each privacy move publishes on its own, at once, in the visibility pass; the batch closes at the end of the pass
    assert moves == ["move", "move", "move", "move_batch"]
    assert _visibility(kit, env) == after


def test_native_batch_keeps_visibility_invariant(kit: LakeKit) -> None:
    _flip(kit, KEY_ENV, dict.fromkeys(FIVE, "private"), dict.fromkeys(FIVE, "public"), 50)
    with CatalogReader(kit.storage, env=KEY_ENV).session() as conn:
        rows = conn.execute(
            'SELECT DISTINCT b._visibility, m.visibility FROM lake.main."statsbomb_fake_events" AS b '
            "JOIN lake.main.ops_matches AS m USING (match_key)"
        ).fetchall()
    assert rows == [("public", "public")]  # _visibility equals ops_matches.visibility in the batch's version


def test_register_batch_is_one_publish_not_two(kit: LakeKit) -> None:
    _flip(kit, None, dict.fromkeys(FIVE, "private"), dict.fromkeys(FIVE, "public"), 50)
    assert [reason for reason in _reasons() if reason != "run_end"] == ["move_batch"]


def test_register_private_move_is_one_publish_then_deletes(kit: LakeKit) -> None:
    _flip(kit, None, {"P1": "public"}, {"P1": "private"}, 50)
    assert [reason for reason in _reasons() if reason != "run_end"] == ["move"]
    key = match_key("statsbomb", "P1")
    lake = [k for k in kit.objects("lake", "data/") if f"/{key}-" in k]
    restricted = [k for k in kit.objects("restricted", "data/") if f"/{key}-" in k]
    assert lake == []  # the old public copies were deleted after the publish (§29.7)
    assert restricted != []


def test_crash_inside_native_batch_leaves_matches_private(kit: LakeKit) -> None:
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(move_batch=50))
    assert _run(kit, _adapter(dict.fromkeys(FIVE, "private")), KEY_ENV).status == "ok"
    Recording.fail_on = "move_batch"
    with pytest.raises(_Crash):
        _run(kit, _adapter(dict.fromkeys(FIVE, "public")), KEY_ENV)
    assert _visibility(kit, KEY_ENV) == dict.fromkeys(FIVE, "private")  # the unpublished flips expose nothing


@pytest.mark.parametrize("path", sorted(PATHS))
def test_crash_inside_batch_then_the_next_run_completes_it(kit: LakeKit, path: str) -> None:
    """A crash inside a private-to-public batch (T3 spec D3/D5): on the register path the staged copies in the lake root
    are orphans; the next run sweeps them without tripping the large-sweep guard (the run anchor covers them) and
    redoes the moves."""
    env = PATHS[path]
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(move_batch=50))
    assert _run(kit, _adapter(dict.fromkeys(FIVE, "private")), env).status == "ok"
    Recording.fail_on = "move_batch"
    with pytest.raises(_Crash):
        _run(kit, _adapter(dict.fromkeys(FIVE, "public")), env)
    Recording.fail_on = None
    result = _run(kit, _adapter(dict.fromkeys(FIVE, "public")), env)
    assert result.status == "ok", result.errors
    assert _visibility(kit, env) == dict.fromkeys(FIVE, "public")
    assert _run(kit, _adapter(dict.fromkeys(FIVE, "public")), env).status == "ok"  # the run after next is clean
