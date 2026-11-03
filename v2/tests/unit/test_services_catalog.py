"""`read_catalog` retries once when its version object is gone, then raises `StaleCatalogError` (T3 spec D4; plan
round 3 Tasks 6 and 12). Pruning deletes superseded versions after the grace, so a page that read `CURRENT` just
before a publish and its pruning can find its version object deleted; it reads the new `CURRENT` instead, never an
unmapped error. A fresh attach of a deleted version raises `FileNotFoundError` locally and on HF. On HF, a session
whose version object is deleted mid-session raises `duckdb.HTTPException` (HTTP 404 on the version URL) at its next
metadata read (Task 12, 2026-10-31; the round-9 spike had seen such a session complete)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from lakekit import LakeKit

from lakehouse.catalog.publish import version_key
from lakehouse.errors import StaleCatalogError
from lakehouse.services.catalog import read_catalog
from lakehouse.storage.base import ObjectInfo
from lakehouse.storage.keys import Root
from lakehouse.storage.local import LocalStorage


class _Gone(LocalStorage):
    """The version object is gone for the first `misses` attaches (a pruning raced the read)."""

    def __init__(self, storage: LocalStorage, seq: int, misses: int) -> None:
        super().__init__(storage.roots)
        self.key = version_key(seq)
        self.misses = misses

    def stat(self, root: Root, key: str) -> ObjectInfo:
        if key == self.key and self.misses > 0:
            self.misses -= 1
            raise FileNotFoundError(f"{root}:{key}")
        return super().stat(root, key)


def _two_versions(kit: LakeKit) -> None:
    for count in (1, 2):
        cat = kit.catalog(f"run-{count}")
        cat.conn.execute("CREATE TABLE IF NOT EXISTS lake.main.t (x INTEGER)")
        cat.conn.execute("INSERT INTO lake.main.t VALUES (?)", [count])
        cat.commit("run_end")
        cat.close()


def _rows(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute("SELECT count(*) FROM lake.main.t").fetchone()
    assert row is not None
    return int(row[0])


def test_missing_version_retries_on_new_current(kit: LakeKit) -> None:
    _two_versions(kit)
    storage = _Gone(kit.storage, seq=2, misses=1)
    assert read_catalog(storage, _rows, empty=0) == 2


def test_missing_version_twice_raises_stale(kit: LakeKit) -> None:
    _two_versions(kit)
    storage = _Gone(kit.storage, seq=2, misses=2)
    with pytest.raises(StaleCatalogError, match="reload"):
        read_catalog(storage, _rows, empty=0)


def test_cannot_open_file_still_maps_to_stale(kit: LakeKit) -> None:
    _two_versions(kit)

    def moved(conn: duckdb.DuckDBPyConnection) -> int:
        raise duckdb.IOException('IO Error: Cannot open file "data/bronze/x.parquet": No such file or directory')

    with pytest.raises(StaleCatalogError):
        read_catalog(kit.storage, moved, empty=0)


def test_no_current_reads_empty(tmp_path: Path, kit: LakeKit) -> None:
    assert read_catalog(kit.storage, _rows, empty=-1) == -1


_GONE_MID_SESSION = (
    "HTTP Error: Failed to query most recent snapshot for DuckLake: HTTP GET error on "
    "'https://huggingface.co/buckets/ns/lake-test/resolve/catalog/v/{seq}.ducklake' (HTTP 404 Not Found)"
)  # the exact error of the Task 12 HF run, 2026-10-31: the session's version object was deleted mid-session


def test_version_deleted_mid_session_retries_on_new_current(kit: LakeKit) -> None:
    """On HF the next metadata read of a session whose version object was deleted raises HTTP 404 (Task 12): the
    read retries once on the new `CURRENT`."""
    _two_versions(kit)
    calls: list[int] = []

    def gone_once(conn: duckdb.DuckDBPyConnection) -> int:
        calls.append(1)
        if len(calls) == 1:
            raise duckdb.HTTPException(_GONE_MID_SESSION.format(seq=1))
        return _rows(conn)

    assert read_catalog(kit.storage, gone_once, empty=0) == 2
    assert calls == [1, 1]


def test_version_deleted_mid_session_twice_raises_stale(kit: LakeKit) -> None:
    _two_versions(kit)

    def always_gone(conn: duckdb.DuckDBPyConnection) -> int:
        raise duckdb.HTTPException(_GONE_MID_SESSION.format(seq=2))

    with pytest.raises(StaleCatalogError, match="reload"):
        read_catalog(kit.storage, always_gone, empty=0)


@pytest.mark.parametrize(
    "message",
    [
        _GONE_MID_SESSION.replace("HTTP 404 Not Found", "HTTP 401 Unauthorized").format(seq=2),  # not a missing object
        _GONE_MID_SESSION.replace("HTTP 404 Not Found", "HTTP 503 Service Unavailable").format(seq=2),
        _GONE_MID_SESSION.replace("catalog/v/{seq}.ducklake", "data/bronze/t/{seq}.parquet").format(seq=2),
    ],
    ids=["unauthorized", "unavailable", "not-a-version"],
)
def test_other_http_errors_propagate_unchanged(kit: LakeKit, message: str) -> None:
    _two_versions(kit)
    calls: list[int] = []

    def failing(conn: duckdb.DuckDBPyConnection) -> int:
        calls.append(1)
        raise duckdb.HTTPException(message)

    with pytest.raises(duckdb.HTTPException) as raised:
        read_catalog(kit.storage, failing, empty=0)
    assert not isinstance(raised.value, StaleCatalogError) and str(raised.value) == message
    assert calls == [1]


def test_missing_file_inside_the_read_is_not_retried(kit: LakeKit) -> None:
    """Only a miss while opening the session is a pruned version; a page's own missing file keeps its error."""
    _two_versions(kit)
    calls: list[int] = []

    def missing(conn: duckdb.DuckDBPyConnection) -> int:
        calls.append(1)
        raise FileNotFoundError("a page asset")

    with pytest.raises(FileNotFoundError, match="a page asset"):
        read_catalog(kit.storage, missing, empty=0)
    assert calls == [1]
