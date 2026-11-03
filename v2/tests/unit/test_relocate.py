"""Lake relocation and restore across a relocation (spec §10.5.1, §10.8; §23.6 "Lake relocation")."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from lakekit import PLAYERS, LakeKit, StubLease, content_sum, lake_roots

from lakehouse.catalog import relocate as relocate_module
from lakehouse.catalog.publish import read_current, roots_record
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.relocate import relocate
from lakehouse.catalog.restore import restore
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import GuardStopError, RootMismatchError
from lakehouse.storage.lease import RunAnchor, anchor_intervals, anchor_key, marker_key
from lakehouse.storage.local import LocalStorage

M = 9001


def _lake_with_native_and_bronze(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, M, visibility="public", frames=6, offset=2)
    kit.add_match(cat, M + 1, visibility="private", frames=4, offset=3)
    cat.conn.execute("CREATE TABLE lake.main.dims AS SELECT range AS x_mm FROM range(5000)")  # DuckLake-written
    cat.commit("run_end")
    cat.close()


def _move_lake(kit: LakeKit, new_base: Path) -> LocalStorage:
    new_base.mkdir()
    for name in ("lake", "lake-restricted"):
        shutil.move(kit.base / name, new_base / name)
    return LocalStorage(lake_roots(new_base))


def _sums(storage: LocalStorage) -> tuple[int, int]:
    reader = CatalogReader(storage)
    with reader.session() as conn:
        bronze = conn.execute(f"SELECT sum(x_mm) FROM lake.{PLAYERS}").fetchone()  # noqa: S608 - constant name
        native = conn.execute("SELECT sum(x_mm) FROM lake.dims").fetchone()
    reader.close()
    assert bronze is not None
    assert native is not None
    return int(bronze[0]), int(native[0])


def test_relocate_reads_by_value(kit: LakeKit, tmp_path: Path) -> None:
    _lake_with_native_and_bronze(kit)
    expected = (content_sum(6, 2) + content_sum(4, 3), sum(range(5000)))
    moved = _move_lake(kit, tmp_path / "new-drive")
    with pytest.raises(RootMismatchError):
        _sums(moved)
    info = relocate(moved, old_roots=kit.roots, new_roots=moved.roots)
    assert info.seq == 2
    assert read_current(moved).storage_roots == roots_record(moved.roots)
    assert _sums(moved) == expected  # by value, not count(*)


def test_relocate_refuses_other_format_version(kit: LakeKit, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _lake_with_native_and_bronze(kit)
    moved = _move_lake(kit, tmp_path / "new-drive")
    monkeypatch.setattr(relocate_module, "SUPPORTED_DUCKLAKE_FORMAT", "9.9")
    before = read_current(moved)
    with pytest.raises(GuardStopError, match=r"1\.1"):
        relocate(moved, old_roots=kit.roots, new_roots=moved.roots)
    assert read_current(moved) == before


def test_relocate_refuses_wrong_old_roots(kit: LakeKit, tmp_path: Path) -> None:
    _lake_with_native_and_bronze(kit)
    moved = _move_lake(kit, tmp_path / "new-drive")
    with pytest.raises(RootMismatchError):
        relocate(moved, old_roots=lake_roots(tmp_path / "elsewhere"), new_roots=moved.roots)


def test_restore_across_relocate(kit: LakeKit, tmp_path: Path) -> None:
    _lake_with_native_and_bronze(kit)  # version 1, at the old roots
    expected = _sums(kit.storage)
    moved = _move_lake(kit, tmp_path / "new-drive")
    relocate(moved, old_roots=kit.roots, new_roots=moved.roots)  # version 2, at the new roots
    record = restore(moved, target_seq=1, operator="ops")
    assert record.new_seq == 3
    assert read_current(moved).storage_roots == roots_record(moved.roots)
    assert _sums(moved) == expected


def test_relocate_moves_the_native_data_path(kit: LakeKit, tmp_path: Path) -> None:
    _lake_with_native_and_bronze(kit)
    moved = _move_lake(kit, tmp_path / "new-drive")
    relocate(moved, old_roots=kit.roots, new_roots=moved.roots)
    cat = WorkingCatalog.open(moved, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w")
    cat.conn.execute("INSERT INTO lake.dims SELECT range FROM range(5000)")
    cat.commit("run_end")
    files = cat.conn.execute("SELECT data_file FROM ducklake_list_files('lake', 'dims')").fetchall()
    cat.close()
    assert files
    for (path,) in files:
        assert Path(str(path)).is_relative_to(tmp_path / "new-drive" / "lake")
    assert not (kit.base / "lake").exists()  # nothing was written under the old root


def test_relocate_removes_earlier_anchors(kit: LakeKit, tmp_path: Path) -> None:
    """T3 spec D5: the copied files' storage times no longer show which generation stored them (fail closed)."""
    _lake_with_native_and_bronze(kit)
    for generation in (5, 6):
        body = RunAnchor(generation=generation, run_id=f"run-{generation}").model_dump_json().encode()
        kit.storage.put("lake", anchor_key(generation), body)
        kit.storage.put("lake", marker_key(generation), body)
    moved = _move_lake(kit, tmp_path / "new-drive")
    relocate(moved, old_roots=kit.roots, new_roots=moved.roots)
    generations = [interval.generation for interval in anchor_intervals(moved)]
    assert 5 not in generations and 6 not in generations
    assert len(generations) == 1  # only the relocate's own anchor survives
