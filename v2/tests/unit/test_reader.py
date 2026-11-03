"""Catalog readers (spec §10.4): one attachment per version, re-rooted reads, DuckDB extension folder."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from lakehouse.catalog.duck import EXTENSION_DIR_ENV, connect_duckdb
from lakehouse.catalog.publish import CurrentPointer, publish_current, read_current, roots_record
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import ResourceCaps, load_defaults
from lakehouse.errors import IntegrityError
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake


class _Held:
    generation = 1
    run_id = "run-1"

    def assert_held(self) -> None:
        return None


def _roots(base: Path) -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )


def _lake(base: Path) -> LocalStorage:
    init_lake(ResolvedRoots(roots=_roots(base), source="cli"), bootstrap_file=base / "boot.json")
    return LocalStorage(_roots(base))


def _commit(storage: LocalStorage, sql: str, work: Path) -> None:
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), _Held(), work_dir=work)
    cat.conn.execute(sql)
    cat.commit("run_end")
    cat.close()


def test_reader_reuses_attachment(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    _commit(storage, "CREATE TABLE lake.main.t AS SELECT range AS i FROM range(100)", tmp_path / "w")
    reader = CatalogReader(storage)
    with reader.session() as first:
        assert first.execute("SELECT count(*) FROM lake.t").fetchone() == (100,)
    with reader.session() as second:
        assert second.execute("SELECT sum(i) FROM lake.t").fetchone() == (sum(range(100)),)
    assert reader.attach_count == 1
    reader.close()


def test_reader_reattaches_on_new_current(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    _commit(storage, "CREATE TABLE lake.main.t AS SELECT range AS i FROM range(100)", tmp_path / "w1")
    reader = CatalogReader(storage)
    with reader.session() as conn:
        assert conn.execute("SELECT count(*) FROM lake.t").fetchone() == (100,)
        _commit(storage, "INSERT INTO lake.t SELECT range FROM range(100, 150)", tmp_path / "w2")
        # a session keeps its version to the end, so one page never mixes two versions
        assert conn.execute("SELECT count(*) FROM lake.t").fetchone() == (100,)
    with reader.session() as conn:
        assert conn.execute("SELECT count(*) FROM lake.t").fetchone() == (150,)
    assert reader.attach_count == 2
    reader.close()


def test_reader_checks_version_size(tmp_path: Path) -> None:
    storage = _lake(tmp_path)
    _commit(storage, "CREATE TABLE lake.main.t AS SELECT 1 AS i", tmp_path / "w")
    current = read_current(storage)
    wrong = CurrentPointer.build(
        seq=current.seq,
        version_key=current.version_key,
        size=current.size + 1,
        sha256=current.sha256,
        parent_seq=current.parent_seq,
        lease_generation=current.lease_generation,
        storage_roots=current.storage_roots,
    )
    publish_current(storage, wrong, expected=current)
    with pytest.raises(IntegrityError, match="size"), CatalogReader(storage).session():
        pass


def test_reader_rerooted_native_files(tmp_path: Path) -> None:
    old = tmp_path / "old"
    storage = _lake(old)
    _commit(storage, "CREATE TABLE lake.main.t AS SELECT range AS i FROM range(5000)", tmp_path / "w")
    new = tmp_path / "new"
    new.mkdir()
    for name in ("lake", "lake-restricted"):
        shutil.move(old / name, new / name)
    moved = LocalStorage(_roots(new))
    current = read_current(moved)
    # the pointer change that `relocate` makes (Task 11); the native files themselves are not touched
    repointed = CurrentPointer.build(
        seq=current.seq,
        version_key=current.version_key,
        size=current.size,
        sha256=current.sha256,
        parent_seq=current.parent_seq,
        lease_generation=current.lease_generation,
        storage_roots=roots_record(moved.roots),
    )
    publish_current(moved, repointed, expected=current)
    assert not (old / "lake").exists()
    with CatalogReader(moved).session() as conn:
        # sum, not count(*): a count can come from catalog statistics without opening any file (spec §25.5)
        assert conn.execute("SELECT sum(i) FROM lake.t").fetchone() == (sum(range(5000)),)


def test_extension_directory_from_env(tmp_path: Path) -> None:
    folder = tmp_path / "duckdb-extensions"
    conn = connect_duckdb(env={EXTENSION_DIR_ENV: str(folder)}, caps=ResourceCaps(), extensions=())
    setting = conn.execute("SELECT current_setting('extension_directory')").fetchone()
    assert setting is not None
    assert Path(setting[0]) == folder
    assert conn.execute("SELECT current_setting('threads')").fetchone() == (4,)
    conn.close()
    default = connect_duckdb(env={}, caps=None, extensions=())
    assert default.execute("SELECT current_setting('extension_directory')").fetchone() != (str(folder),)
    default.close()
