"""Every DuckDB instance that encrypts its spill uses its own temp directory (audit T13).

DuckDB refuses `SET temp_file_encryption = true` while any live instance has temp files in the same directory. The
catalog compaction and the encrypted reader used the default `./.tmp`, so an unrelated spill in the same working
directory (another process, the web app's reader) failed the compaction, and so the run's publish (T11 real-data run,
2026-11-01). A failed publish also left the working catalog detached, so the run could not record the error.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import duckdb
import pytest
from lakekit import StubLease, lake_roots

from lakehouse.catalog import compact as compact_module
from lakehouse.catalog import working as working_module
from lakehouse.catalog.compact import compact_catalog_file
from lakehouse.catalog.publish import read_current
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import IntegrityError
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.keys import Root
from lakehouse.storage.local import LocalStorage

KEY = "7" * 64
ENV = {ENCRYPTION_KEY_ENV: KEY}


def _catalog(base: Path) -> tuple[LocalStorage, WorkingCatalog]:
    for name in ("lake", "lake-restricted"):
        (base / name).mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(base))
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env=ENV)
    cat.conn.execute("CREATE TABLE lake.main.ops_probe AS SELECT range AS seq FROM range(500)")
    return storage, cat


@pytest.fixture
def foreign_spill(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Another DuckDB instance in the same working directory holds spilled temp files in the default `./.tmp`."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    other = duckdb.connect(config={"memory_limit": "64MB", "threads": 2})
    other.execute("CREATE TABLE big AS SELECT range AS i, md5(range::VARCHAR) AS s FROM range(3000000)")
    assert any((cwd / ".tmp").iterdir()), "the foreign instance did not spill"
    yield cwd
    other.close()


def test_compaction_ignores_a_foreign_spill(
    tmp_path: Path, foreign_spill: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compact_module, "COMPACT_MIN_BYTES", 0)
    _, cat = _catalog(tmp_path / "base")
    try:
        path = cat.detach()
        folders = {p.name for p in path.parent.iterdir() if p.is_dir()}
        result = compact_catalog_file(path, key=KEY)
        assert (result.compacted, result.failed_check) == (True, None)
        assert {p.name for p in path.parent.iterdir() if p.is_dir()} == folders, "a temp directory was left behind"
    finally:
        cat.close()


def test_encrypted_reader_ignores_a_foreign_spill(tmp_path: Path, foreign_spill: Path) -> None:
    storage, cat = _catalog(tmp_path / "base")
    cat.commit("run_end")
    cat.close()
    reader = CatalogReader(storage, env=ENV)
    try:
        with reader.session() as conn:
            assert conn.execute("SELECT count(*) FROM lake.main.ops_probe").fetchone() == (500,)
    finally:
        reader.close()


def test_a_failed_publish_reattaches_the_working_catalog(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The run records the error in the working copy after a failed publish, so the copy must be attached again."""
    _, cat = _catalog(tmp_path / "base")

    def broken(path: Path, *, key: str | None) -> object:
        raise RuntimeError("compaction failed")

    monkeypatch.setattr(working_module, "compact_catalog_file", broken)
    try:
        with pytest.raises(RuntimeError, match="compaction failed"):
            cat.commit("in_run")
        assert cat.conn.execute("SELECT count(*) FROM lake.main.ops_probe").fetchone() == (500,)
    finally:
        cat.close()


def test_a_compaction_error_publishes_the_original(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A rewrite-side error (here the fresh file's attach) keeps the original, which was just read in full, and the
    publish goes on (Reviewer B T3-IMPL-02); the result names the error and no `.compact` file is left."""
    monkeypatch.setattr(compact_module, "COMPACT_MIN_BYTES", 0)
    _, cat = _catalog(tmp_path / "base")

    real_attach = compact_module._attach  # pyright: ignore[reportPrivateUsage] - the rewrite-side seam

    def broken_attach(conn: duckdb.DuckDBPyConnection, path: Path, alias: str, key: str | None) -> None:
        if alias == "dst":
            raise duckdb.IOException("No space left on device")
        real_attach(conn, path, alias, key)

    monkeypatch.setattr(compact_module, "_attach", broken_attach)
    try:
        info = cat.commit("run_end")
        assert info.compaction is not None
        assert (info.compaction.compacted, info.compaction.failed_check) == (False, "error: IOException")
        assert not list((tmp_path / "base" / "w").rglob("*.compact*")), "a rewrite file or temp folder was left"
        assert cat.conn.execute("SELECT count(*) FROM lake.main.ops_probe").fetchone() == (500,)
    finally:
        cat.close()


def _flip_then(real: Callable[..., object]) -> Callable[..., object]:
    """`compact_catalog_file` after one flipped byte in the detached working copy (a damaged file)."""

    def damaged(path: Path, *, key: str | None) -> object:
        data = bytearray(path.read_bytes())
        data[3 * 4096 + 2] ^= 0xFF  # inside the first block after DuckDB's three header blocks
        path.write_bytes(bytes(data))
        return real(path, key=key)

    return damaged


@pytest.mark.parametrize("encrypted", [False, True], ids=["plain", "keyed"])
def test_a_damaged_working_copy_is_never_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, encrypted: bool
) -> None:
    """Reviewer B T3-IMPL-04: the fallback to the original applies to rewrite-side errors only. A working copy that
    cannot be opened or read stops the publish, and `CURRENT` does not move."""
    monkeypatch.setattr(compact_module, "COMPACT_MIN_BYTES", 0)
    base = tmp_path / "base"
    for name in ("lake", "lake-restricted"):
        (base / name).mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(base))
    env = ENV if encrypted else {}
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env=env)
    try:
        cat.conn.execute("CREATE TABLE lake.main.ops_probe AS SELECT range AS seq FROM range(500)")
        first = cat.commit("in_run").seq
        cat.conn.execute("INSERT INTO lake.main.ops_probe SELECT range FROM range(500, 600)")
        monkeypatch.setattr(working_module, "compact_catalog_file", _flip_then(compact_catalog_file))
        with pytest.raises(IntegrityError, match="cannot be read"):
            cat.commit("in_run")
        assert read_current(storage).seq == first
    finally:
        cat.close()


class _ManifestFailsOnce(LocalStorage):
    """The first manifest put fails after the version object was stored."""

    failed = False

    def put_if_absent(self, root: Root, key: str, data: bytes | Path) -> bool:
        if key.endswith(".files.json") and not self.failed:
            type(self).failed = True
            raise OSError("the manifest upload failed")
        return super().put_if_absent(root, key, data)


def test_a_failed_publish_does_not_reuse_a_stored_sequence(tmp_path: Path) -> None:
    """Reviewer B T3-IMPL-05: once the version object is stored, its sequence is used; the next publish takes the next
    one instead of hitting the version fence for the rest of the run."""
    base = tmp_path / "base"
    for name in ("lake", "lake-restricted"):
        (base / name).mkdir(parents=True, exist_ok=True)
    storage = _ManifestFailsOnce(lake_roots(base))
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env={})
    try:
        cat.conn.execute("CREATE TABLE lake.main.ops_probe AS SELECT range AS seq FROM range(10)")
        with pytest.raises(OSError, match="manifest upload failed"):
            cat.commit("in_run")
        orphan = int(next(iter(storage.list("lake", "catalog/v/"))).key.split("/")[-1].split(".")[0])
        info = cat.commit("in_run")
        assert info.seq == orphan + 1
        assert read_current(storage).seq == orphan + 1
    finally:
        cat.close()
