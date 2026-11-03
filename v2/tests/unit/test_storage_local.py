"""Local storage (spec §10.4, §10.5, §23.6 "Missing root fails")."""

from __future__ import annotations

import errno
import hashlib
import multiprocessing
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lakehouse.errors import ConflictError, MissingRootError
from lakehouse.storage import local
from lakehouse.storage.base import validate_key
from lakehouse.storage.keys import Root, RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake, open_storage

if TYPE_CHECKING:
    from threading import Barrier


def _roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def _storage(tmp_path: Path) -> LocalStorage:
    resolved = ResolvedRoots(roots=_roots(tmp_path), source="cli")
    init_lake(resolved, bootstrap_file=tmp_path / "config" / "bootstrap.json")
    return LocalStorage(_roots(tmp_path))


def test_missing_root_fails(tmp_path: Path) -> None:
    resolved = ResolvedRoots(roots=_roots(tmp_path), source="cli")
    with pytest.raises(MissingRootError):
        open_storage(resolved, {})
    assert not (tmp_path / "lake").exists()
    assert not (tmp_path / "lake-restricted").exists()


def test_init_creates_both_roots(tmp_path: Path) -> None:
    _storage(tmp_path)
    assert (tmp_path / "lake").is_dir()
    assert (tmp_path / "lake-restricted").is_dir()
    assert (tmp_path / "config" / "bootstrap.json").is_file()
    open_storage(ResolvedRoots(roots=_roots(tmp_path), source="cli"), {})


@pytest.mark.parametrize("root", ["lake", "restricted"])
def test_put_get_list_delete_roundtrip(tmp_path: Path, root: Root) -> None:
    storage = _storage(tmp_path)
    storage.put(root, "data/bronze/t/1-a-b.parquet", b"one")
    storage.put(root, "data/bronze/t/2-a-b.parquet", b"two")
    storage.put(root, "data/other/x.parquet", b"x")
    assert storage.get(root, "data/bronze/t/1-a-b.parquet") == b"one"
    keys = [info.key for info in storage.list(root, "data/bronze/")]
    assert keys == ["data/bronze/t/1-a-b.parquet", "data/bronze/t/2-a-b.parquet"]
    # a narrow file-name prefix (spec §29.12 strays)
    assert [i.key for i in storage.list(root, "data/bronze/t/2-")] == ["data/bronze/t/2-a-b.parquet"]
    assert storage.exists(root, "data/other/x.parquet")
    storage.delete(root, "data/other/x.parquet")
    assert not storage.exists(root, "data/other/x.parquet")
    with pytest.raises(FileNotFoundError):
        storage.get(root, "data/other/x.parquet")


def test_list_hides_internal_folders(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    assert storage.put_if_absent("lake", "catalog/CURRENT", b"1")
    storage.replace_if_match("lake", "catalog/CURRENT", b"2", expected_tag=hashlib.sha256(b"1").hexdigest())
    storage.put("lake", "catalog/v/1.ducklake", b"v")
    assert [i.key for i in storage.list("lake", "")] == ["catalog/CURRENT", "catalog/v/1.ducklake"]


def test_put_from_path_and_fresh_mtime(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    old = time.time() - 3600
    os.utime(src, (old, old))
    storage.put("lake", "data/f.bin", src)
    info = storage.stat("lake", "data/f.bin")
    assert info.size == 7
    assert info.modified > datetime.now(UTC) - timedelta(minutes=5)


def test_put_if_absent_never_overwrites(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    assert storage.put_if_absent("lake", "runs/lease/000000000001.json", b"first") is True
    assert storage.put_if_absent("lake", "runs/lease/000000000001.json", b"second") is False
    assert storage.get("lake", "runs/lease/000000000001.json") == b"first"
    src = tmp_path / "v.bin"
    src.write_bytes(b"version")
    assert storage.put_if_absent("lake", "catalog/v/1.ducklake", src) is True
    assert storage.put_if_absent("lake", "catalog/v/1.ducklake", src) is False


def _race_put_if_absent(base: str, value: bytes, barrier: Barrier) -> bool:
    storage = LocalStorage(_roots(Path(base)))
    barrier.wait()
    return storage.put_if_absent("lake", "runs/lease/000000000002.json", value)


def test_put_if_absent_race_one_winner(tmp_path: Path) -> None:
    _storage(tmp_path)
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(4) as pool:
        barrier = manager.Barrier(4)
        results = pool.starmap(_race_put_if_absent, [(str(tmp_path), bytes([i]), barrier) for i in range(4)])
    assert results.count(True) == 1


def _no_link(src: str, dst: str) -> None:
    raise OSError(errno.EPERM, "hard links not supported")


def _race_without_links(base: str, value: bytes, barrier: Barrier) -> bool:
    os.link = _no_link  # this spawned child only: a file system without hard links
    storage = LocalStorage(_roots(Path(base)))
    barrier.wait()
    return storage.put_if_absent("lake", "runs/lease/000000000004.json", value)


def test_put_if_absent_without_hard_links(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _storage(tmp_path)
    monkeypatch.setattr(os, "link", _no_link)
    assert storage.put_if_absent("lake", "runs/lease/000000000003.json", b"a") is True
    assert storage.put_if_absent("lake", "runs/lease/000000000003.json", b"b") is False
    assert storage.get("lake", "runs/lease/000000000003.json") == b"a"
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(4) as pool:
        barrier = manager.Barrier(4)
        results = pool.starmap(_race_without_links, [(str(tmp_path), bytes([i]), barrier) for i in range(4)])
    assert results.count(True) == 1


def test_replace_if_match_cas(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    assert storage.put_if_absent("lake", "catalog/CURRENT", b"one")
    data, tag1 = storage.get_versioned("lake", "catalog/CURRENT")
    assert (data, tag1) == (b"one", hashlib.sha256(b"one").hexdigest())
    tag2 = storage.replace_if_match("lake", "catalog/CURRENT", b"two", expected_tag=tag1)
    assert tag2 == hashlib.sha256(b"two").hexdigest()
    with pytest.raises(ConflictError):
        storage.replace_if_match("lake", "catalog/CURRENT", b"three", expected_tag=tag1)
    with pytest.raises(ConflictError):
        storage.replace_if_match("lake", "catalog/absent", b"three", expected_tag=tag1)
    assert storage.get_versioned("lake", "catalog/CURRENT") == (b"two", tag2)


def _race_cas(base: str, value: bytes, tag: str, barrier: Barrier) -> bool:
    storage = LocalStorage(_roots(Path(base)))
    barrier.wait()
    try:
        storage.replace_if_match("lake", "catalog/CURRENT", value, expected_tag=tag)
    except ConflictError:
        return False
    return True


def test_replace_if_match_race_one_winner(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    assert storage.put_if_absent("lake", "catalog/CURRENT", b"base")
    _, tag = storage.get_versioned("lake", "catalog/CURRENT")
    ctx = multiprocessing.get_context("spawn")
    with ctx.Manager() as manager, ctx.Pool(4) as pool:
        barrier = manager.Barrier(4)
        results = pool.starmap(_race_cas, [(str(tmp_path), bytes([65 + i]), tag, barrier) for i in range(4)])
    assert results.count(True) == 1


def test_delete_missing_ok_only_when_asked(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    with pytest.raises(FileNotFoundError):
        storage.delete("lake", "data/nothing.parquet")
    storage.delete("lake", "data/nothing.parquet", missing_ok=True)


def test_read_uri_forms(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    uri = storage.read_uri("restricted", "data/bronze/t/f.parquet")
    assert Path(uri).is_absolute()
    assert Path(uri) == tmp_path / "lake-restricted" / "data" / "bronze" / "t" / "f.parquet"
    assert storage.duckdb_secrets() == []


@pytest.mark.parametrize(
    "bad",
    ["../x", "/abs", "C:\\x", "C:/x", "data\\x", "", "data/../x", "data//x", "./x", "data/x:y", "a\x00b"],
)
def test_key_traversal_refused(tmp_path: Path, bad: str) -> None:
    storage = _storage(tmp_path)
    with pytest.raises(ValueError):
        validate_key(bad)
    with pytest.raises(ValueError):
        storage.put("lake", bad, b"x")


def test_internal_folders_are_not_keys(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    for key in (".tmp/x", ".locks/catalog/CURRENT.lock"):
        with pytest.raises(ValueError, match="internal"):
            storage.put("lake", key, b"x")


def _no_sleep(_seconds: float) -> None:
    return None


def test_locked_replace_retries_then_fails_loudly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = _storage(tmp_path)
    real_replace = os.replace
    calls: list[str] = []

    def locked_twice(src: str, dst: str) -> None:
        calls.append(dst)
        if len(calls) <= 2:
            raise PermissionError(13, "the file is held open by another process")
        real_replace(src, dst)

    monkeypatch.setattr(local, "_RETRY_LOCKED_REPLACE", True)
    monkeypatch.setattr(local.time, "sleep", _no_sleep)
    monkeypatch.setattr(os, "replace", locked_twice)
    storage.put("lake", "data/f.bin", b"ok")
    assert len(calls) == 3
    assert storage.get("lake", "data/f.bin") == b"ok"

    def always_locked(src: str, dst: str) -> None:
        calls.append(dst)
        raise PermissionError(13, "the file is held open by another process")

    calls.clear()
    monkeypatch.setattr(os, "replace", always_locked)
    with pytest.raises(PermissionError):
        storage.put("lake", "data/f.bin", b"new")
    assert len(calls) == 10
    assert storage.get("lake", "data/f.bin") == b"ok"
    assert list((tmp_path / "lake" / ".tmp").iterdir()) == []


def test_writes_never_create_a_missing_root(tmp_path: Path) -> None:
    # A drive that is not mounted: no write may create the root (spec §10.5, §23.6; only init_lake creates roots).
    storage = LocalStorage(_roots(tmp_path))
    with pytest.raises(MissingRootError):
        storage.put("lake", "data/f.bin", b"x")
    with pytest.raises(MissingRootError):
        storage.put_if_absent("lake", "runs/lease/000000000001.json", b"x")
    with pytest.raises(MissingRootError):
        storage.replace_if_match("lake", "catalog/CURRENT", b"x", expected_tag="t")
    assert not (tmp_path / "lake").exists()
