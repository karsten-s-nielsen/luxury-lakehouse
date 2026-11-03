"""Move invariants under batching, through a restore (T3 spec D3, §29.11, §29.12; plan round 3 Task 9 Step 3).

A public-to-private move (its own publish), then a batched private-to-public move, then a restore to the version from
before the batch: every match ends with its latest visibility, every file the restored version names exists, and no
now-private match is readable as public."""

from __future__ import annotations

import pytest
from lakekit import LakeKit
from runkit import NOW, FakeAdapter, FakeParser, active, entry, save_settings

from lakehouse.catalog.manifest import read_manifest
from lakehouse.catalog.publish import read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.restore import restore
from lakehouse.config.model import CheckpointConfig
from lakehouse.ids.keys import match_key
from lakehouse.runs import run as run_module
from lakehouse.runs.checkpoint import CheckpointPolicy, PublishRecord
from lakehouse.runs.history import read_table
from lakehouse.runs.run import run_ingestion

MODULE = "statsbomb_pining"
KEY_ENV = {"LAKEHOUSE_ENCRYPTION_KEY": "d" * 64}
PATHS: dict[str, dict[str, str] | None] = {"register": None, "native": KEY_ENV}


def _adapter(visibility: dict[str, str]) -> FakeAdapter:
    natives = sorted(visibility)
    files = {(n, "events"): f"{n}-a\n{n}-b".encode() for n in natives}
    return FakeAdapter(
        MODULE, "statsbomb", "pining", [entry(MODULE, n, visibility=visibility[n]) for n in natives], files
    )


def _run(kit: LakeKit, visibility: dict[str, str], env: dict[str, str] | None) -> None:
    result = run_ingestion(
        storage=kit.storage,
        trigger="cli",
        adapters={MODULE: _adapter(visibility)},
        parsers={"statsbomb": FakeParser("statsbomb")},
        now=lambda: NOW,
        work_dir=kit.base / "work",
        env=env,
        dbt=None,
    )
    assert result.status == "ok", result.errors


@pytest.mark.parametrize("path", sorted(PATHS))
def test_batched_moves_then_restore(kit: LakeKit, monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    env = PATHS[path]
    if env is not None:
        for name, value in env.items():
            monkeypatch.setenv(name, value)  # restore resolves the key from the environment
    save_settings(kit.storage, [active(MODULE)], checkpoint=CheckpointConfig(move_batch=50))
    _run(kit, {"A": "public", "B": "private", "C": "private"}, env)
    before_batch: list[int] = []
    real_now = CheckpointPolicy.now

    def spy(self: CheckpointPolicy, reason: str) -> PublishRecord:
        record = real_now(self, reason)
        if reason == "move":
            before_batch.append(record.seq)  # the privacy move's own version, published before the batch
        return record

    monkeypatch.setattr(run_module.CheckpointPolicy, "now", spy)
    _run(kit, {"A": "private", "B": "public", "C": "public"}, env)
    monkeypatch.undo()
    if env is not None:
        for name, value in env.items():
            monkeypatch.setenv(name, value)
    [target] = before_batch
    restore(kit.storage, target_seq=target, operator="ops", env=env)
    with CatalogReader(kit.storage, env=env).session() as conn:
        visibility = {row["native_match_id"]: row["visibility"] for row in read_table(conn, "ops_matches")}
    assert visibility == {"A": "private", "B": "public", "C": "public"}  # each match keeps its latest visibility
    manifest = read_manifest(kit.storage, read_current(kit.storage).seq)
    assert all(kit.storage.exists(key.root, key.path) for key in manifest.canonical_keys)
    if env is None:  # register path: a now-private match has no file in the public root
        a_key = match_key("statsbomb", "A")
        assert not [key for key in manifest.canonical_keys if key.root == "lake" and f"/{a_key}-" in key.path]
