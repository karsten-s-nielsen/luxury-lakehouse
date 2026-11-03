"""The versioned config store (spec §8.3, §23.6 "Stale save")."""

from __future__ import annotations

import json
import multiprocessing
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lakehouse.config.model import Settings, load_defaults
from lakehouse.config.store import CONFIG_KEY, ConfigLock, ConfigStore
from lakehouse.errors import StaleSaveError
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

if TYPE_CHECKING:
    from threading import Barrier

SAVES_PER_PROCESS = 25


def _roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def _store(base: Path) -> ConfigStore:
    return ConfigStore(LocalStorage(_roots(base)))


def _seeded(tmp_path: Path) -> ConfigStore:
    init_lake(ResolvedRoots(roots=_roots(tmp_path), source="cli"), bootstrap_file=tmp_path / "config" / "boot.json")
    store = _store(tmp_path)
    assert store.save(load_defaults("laptop"), expected_version=0, operator="installer") == 1
    return store


def _toggled(settings: Settings, active: bool) -> Settings:
    modules = [
        m.model_copy(update={"active": active}) if m.module_id == "wyscout_public" else m for m in settings.modules
    ]
    return settings.model_copy(update={"modules": modules})


def test_load_before_any_save_fails_clearly(tmp_path: Path) -> None:
    init_lake(ResolvedRoots(roots=_roots(tmp_path), source="cli"), bootstrap_file=tmp_path / "boot.json")
    with pytest.raises(FileNotFoundError, match="no config saved"):
        _store(tmp_path).load()


def test_first_save_needs_version_zero(tmp_path: Path) -> None:
    init_lake(ResolvedRoots(roots=_roots(tmp_path), source="cli"), bootstrap_file=tmp_path / "boot.json")
    with pytest.raises(StaleSaveError, match="no config"):
        _store(tmp_path).save(load_defaults("laptop"), expected_version=1, operator="alice")


def test_stale_save_rejected(tmp_path: Path) -> None:
    store = _seeded(tmp_path)
    settings, version = store.load()
    assert version == 1
    barrier = threading.Barrier(2)
    outcomes: dict[str, int | StaleSaveError] = {}

    def save(operator: str, active: bool) -> None:
        barrier.wait()
        try:
            outcomes[operator] = _store(tmp_path).save(
                _toggled(settings, active), expected_version=1, operator=operator
            )
        except StaleSaveError as exc:
            outcomes[operator] = exc

    threads = [threading.Thread(target=save, args=args) for args in (("alice", False), ("bob", True))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    winners = [name for name, result in outcomes.items() if result == 2]
    assert len(winners) == 1
    loser = "bob" if winners == ["alice"] else "alice"
    error = outcomes[loser]
    assert isinstance(error, StaleSaveError)
    assert "version 2" in str(error)
    assert winners[0] in str(error)
    assert store.load()[1] == 2


def test_history_keeps_previous(tmp_path: Path) -> None:
    store = _seeded(tmp_path)
    settings, version = store.load()
    assert store.save(_toggled(settings, False), expected_version=version, operator="alice") == 2
    history = store.history(limit=10)
    assert [(h.version, h.saved_by) for h in history] == [(2, "alice"), (1, "installer")]
    assert history[0].changes == ["modules.wyscout_public.active: true -> false"]
    assert history[1].changes == ["first save"]
    assert history[0].saved_at >= history[1].saved_at
    assert store.history(limit=1) == history[:1]
    storage = LocalStorage(_roots(tmp_path))
    first = json.loads(storage.get("lake", "config/history/00000001.json"))
    assert Settings.model_validate(first["settings"]).module("wyscout_public").active is True
    assert store.load()[0].module("wyscout_public").active is False


def test_unknown_schema_version_rejected(tmp_path: Path) -> None:
    store = _seeded(tmp_path)
    storage = LocalStorage(_roots(tmp_path))
    document = json.loads(storage.get("lake", CONFIG_KEY))
    document["settings"]["schema_version"] = 99
    storage.put("lake", CONFIG_KEY, json.dumps(document).encode())
    with pytest.raises(ValueError, match="schema_version"):
        store.load()


def _save_many(base: str, operator: str, barrier: Barrier) -> int:
    store = _store(Path(base))
    barrier.wait()
    done = 0
    while done < SAVES_PER_PROCESS:
        settings, version = store.load()
        try:
            store.save(_toggled(settings, done % 2 == 0), expected_version=version, operator=operator)
        except StaleSaveError:
            continue
        done += 1
    return done


def test_cli_and_app_lock_locally(tmp_path: Path) -> None:
    store = _seeded(tmp_path)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(2) as pool:
        barrier = manager.Barrier(2)
        done = pool.starmap(_save_many, [(str(tmp_path), "cli", barrier), (str(tmp_path), "app", barrier)])
    assert done == [SAVES_PER_PROCESS, SAVES_PER_PROCESS]
    _, version = store.load()
    assert version == 1 + 2 * SAVES_PER_PROCESS
    history = store.history(limit=1000)
    assert [h.version for h in history] == list(range(version, 0, -1))
    assert sum(h.saved_by == "cli" for h in history) == SAVES_PER_PROCESS
    assert sum(h.saved_by == "app" for h in history) == SAVES_PER_PROCESS


def test_config_lock_is_shared_per_lake(tmp_path: Path) -> None:
    _seeded(tmp_path)
    first = ConfigLock(LocalStorage(_roots(tmp_path)))
    second = ConfigLock(LocalStorage(_roots(tmp_path)))
    with first:
        assert second.locked()
    assert not second.locked()
