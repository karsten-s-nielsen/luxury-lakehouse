"""Bronze registration and root placement (spec §9, §10.3 steps 1 to 4, §10.5, §23.6 "Restricted root placement")."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from lakehouse.bronze.schemas import TRACKING_PLAYERS_CORE, bronze_table
from lakehouse.bronze.writer import (
    Visibility,
    WrittenFile,
    assert_bronze_registered_only,
    place_root,
    register_match_files,
    replace_match_files,
    store_written_file,
    write_tracking_file,
)
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import GuardStopError, IntegrityError
from lakehouse.storage.keys import RootMap, StorageRootInfo, canonical_key
from lakehouse.storage.local import LocalStorage
from lakehouse.storage.roots import ResolvedRoots, init_lake

PLAYERS = bronze_table("skillcorner_tracking_players")


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


def _catalog(tmp_path: Path) -> tuple[LocalStorage, WorkingCatalog]:
    init_lake(ResolvedRoots(roots=_roots(tmp_path), source="cli"), bootstrap_file=tmp_path / "boot.json")
    storage = LocalStorage(_roots(tmp_path))
    return storage, WorkingCatalog.open(storage, load_defaults("laptop"), _Held(), work_dir=tmp_path / "work")


def _players(match_key: int, frames: int) -> pa.Table:
    return pa.table(
        {
            "match_key": pa.array([match_key] * frames, pa.int64()),
            "period": pa.array([1] * frames, pa.int8()),
            "frame": pa.array(range(frames), pa.int32()),
            "player_key": pa.array([11] * frames, pa.int64()),
            "x_mm": pa.array(range(frames), pa.int32()),
            "y_mm": pa.array(range(frames), pa.int32()),
            "is_visible": pa.array([True] * frames, pa.bool_()),
        },
        schema=TRACKING_PLAYERS_CORE,
    )


def _stored(
    storage: LocalStorage, tmp_path: Path, match_key: int, frames: int, visibility: Visibility = "public"
) -> WrittenFile:
    path = write_tracking_file(
        PLAYERS, _players(match_key, frames), match_key=match_key, content_version="v1", out_dir=tmp_path / "out"
    )
    return store_written_file(
        storage,
        path,
        table=PLAYERS,
        visibility=visibility,
        rows=frames,
        content_version="v1",
    )


def _listed(cat: WorkingCatalog, storage: LocalStorage) -> list[str]:
    rows = cat.conn.execute("SELECT data_file FROM ducklake_list_files('lake', ?)", [PLAYERS]).fetchall()
    return sorted(str(canonical_key(str(r[0]), roots=storage.roots, relative_to_data_path=False)) for r in rows)


def test_place_root() -> None:
    assert place_root("public") == "lake"
    assert place_root("private") == "restricted"


def test_public_match_goes_to_lake(tmp_path: Path) -> None:
    storage, cat = _catalog(tmp_path)
    written = _stored(storage, tmp_path, 1, 10)
    assert written.root == "lake"
    assert str(written.key).startswith(f"lake:data/bronze/{PLAYERS}/1-v1-")
    assert storage.exists("lake", written.key.path)
    assert not storage.exists("restricted", written.key.path)
    register_match_files(cat, [written])
    assert _listed(cat, storage) == [str(written.key)]
    assert cat.conn.execute(f"SELECT count(*), sum(frame) FROM lake.{PLAYERS}").fetchone() == (10, 45)  # noqa: S608 - constant test table name
    cat.close()


def test_private_match_goes_to_restricted(tmp_path: Path) -> None:
    storage, cat = _catalog(tmp_path)
    written = _stored(storage, tmp_path, 2, 5, visibility="private")
    assert written.root == "restricted"
    assert storage.exists("restricted", written.key.path)
    assert [i.key for i in storage.list("lake", "data/")] == []
    register_match_files(cat, [written])
    assert _listed(cat, storage) == [str(written.key)]
    cat.close()


def test_replace_drops_old_file(tmp_path: Path) -> None:
    storage, cat = _catalog(tmp_path)
    old = _stored(storage, tmp_path, 3, 10)
    other = _stored(storage, tmp_path, 4, 7)
    register_match_files(cat, [old, other])
    new = _stored(storage, tmp_path, 3, 12)
    replace_match_files(cat, PLAYERS, 3, [new])
    assert _listed(cat, storage) == sorted([str(new.key), str(other.key)])
    counts = cat.conn.execute(f"SELECT match_key, count(*) FROM lake.{PLAYERS} GROUP BY 1 ORDER BY 1").fetchall()  # noqa: S608 - constant test table name
    assert counts == [(3, 12), (4, 7)]
    with pytest.raises(ValueError, match="match"):
        replace_match_files(cat, PLAYERS, 5, [new])
    cat.close()


def test_replace_is_one_transaction(tmp_path: Path) -> None:
    storage, cat = _catalog(tmp_path)
    old = _stored(storage, tmp_path, 3, 10)
    register_match_files(cat, [old])
    missing = _stored(storage, tmp_path, 3, 4)
    storage.delete(missing.root, missing.key.path)
    with pytest.raises(IntegrityError, match="not in storage"):
        replace_match_files(cat, PLAYERS, 3, [missing])
    assert _listed(cat, storage) == [str(old.key)]
    assert cat.conn.execute(f"SELECT count(*) FROM lake.{PLAYERS}").fetchone() == (10,)  # noqa: S608 - constant test table name
    cat.close()


def test_auto_compact_off(tmp_path: Path) -> None:
    storage, cat = _catalog(tmp_path)
    register_match_files(cat, [_stored(storage, tmp_path, 1, 3)])
    rows = cat.conn.execute(
        "SELECT value FROM lake.options() WHERE option_name = 'auto_compact' AND scope_entry = ?", [f"main.{PLAYERS}"]
    ).fetchall()
    assert rows == [("false",)]
    cat.close()


def test_no_native_writer_for_bronze(tmp_path: Path) -> None:
    storage, cat = _catalog(tmp_path)
    register_match_files(cat, [_stored(storage, tmp_path, 1, 3)])
    assert_bronze_registered_only(cat)
    cat.conn.execute(f"INSERT INTO lake.{PLAYERS} SELECT 9, 1, range, 11, range, range, true FROM range(5000)")  # noqa: S608 - constant test table name
    with pytest.raises(GuardStopError, match=PLAYERS):
        assert_bronze_registered_only(cat)
    cat.close()
