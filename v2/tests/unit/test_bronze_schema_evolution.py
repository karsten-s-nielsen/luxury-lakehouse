"""Additive bronze schema evolution (audit T9).

A source field that a parser starts to map (spec §12.3: unmapped fails loudly until it is mapped) must land on a lake
that already holds the table. A write that brings a new column adds it to the table as a nullable column, and the rows
written before read null. A changed type, or a table column that a fresh write no longer carries, is not additive and
fails loudly. A move re-registers bytes that the table already holds, so an older, narrower file is accepted there.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from lakekit import PLAYERS, LakeKit, StubLease, lake_roots, move_now

from lakehouse.bronze.evolve import table_columns
from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.bronze.writer import store_written_file, swap_match_files
from lakehouse.bronze.writers import select_listing_writer
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import IntegrityError
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_TABLE: BronzeTable = bronze_table("skillcorner_pining_matches")
_MODES = {"register": {}, "native": {ENCRYPTION_KEY_ENV: _KEY}}


def _catalog(base: Path, mode: str) -> WorkingCatalog:
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    storage = LocalStorage(lake_roots(base))
    return WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=base / "w", env=_MODES[mode])


def _write(
    cat: WorkingCatalog, base: Path, key: int, records: pa.Table, also: Callable[[], None] = lambda: None
) -> None:
    writer = select_listing_writer(cat, cat.storage)
    writer.write(
        _TABLE,
        records,
        key=key,
        visibility_column="visibility",
        source_visibility="public",
        content_version=f"v{key}",
        out_dir=base / f"out-{key}",
    )
    writer.swap(_TABLE, key, also=also)


def _rows(cat: WorkingCatalog, columns: str) -> list[tuple[Any, ...]]:
    return cat.conn.execute(f'SELECT {columns} FROM lake.main."{_TABLE}" ORDER BY listing_key, id').fetchall()  # noqa: S608 - test constants


_OLD = pa.table({"id": pa.array([1, 2], pa.int64()), "name": ["a", "b"], "visibility": ["public", "public"]})


@pytest.mark.parametrize("mode", sorted(_MODES))
def test_a_new_column_is_added_and_older_rows_read_null(tmp_path: Path, mode: str) -> None:
    cat = _catalog(tmp_path, mode)
    try:
        _write(cat, tmp_path, 1, _OLD)
        # the new column sits in the middle: a positional insert would put its values into the wrong column
        wider = pa.table(
            {"id": pa.array([3], pa.int64()), "revision": ["4340d27"], "name": ["c"], "visibility": ["public"]}
        )
        _write(cat, tmp_path, 2, wider)
        assert _rows(cat, "listing_key, id, name, revision") == [
            (1, 1, "a", None),
            (1, 2, "b", None),
            (2, 3, "c", "4340d27"),
        ]
    finally:
        cat.close()


@pytest.mark.parametrize("mode", sorted(_MODES))
def test_a_nested_new_column_keeps_its_type(tmp_path: Path, mode: str) -> None:
    cat = _catalog(tmp_path, mode)
    try:
        _write(cat, tmp_path, 1, _OLD)
        nested = pa.array(
            [[{"key": "tracking", "filename": "t.jsonl"}]],
            pa.list_(pa.struct([("key", pa.string()), ("filename", pa.string())])),
        )
        _write(cat, tmp_path, 2, _OLD.slice(0, 1).append_column("artifacts", nested))
        assert _rows(cat, "listing_key, artifacts") == [
            (1, None),
            (1, None),
            (2, [{"key": "tracking", "filename": "t.jsonl"}]),
        ]
    finally:
        cat.close()


@pytest.mark.parametrize("mode", sorted(_MODES))
def test_a_failed_write_adds_no_column(tmp_path: Path, mode: str) -> None:
    """The column is added inside the write's transaction, so a write that fails afterwards leaves the table as it
    was (no column without data in the published catalog)."""
    cat = _catalog(tmp_path, mode)

    def fail() -> None:
        raise RuntimeError("a later step of the same write fails")

    try:
        _write(cat, tmp_path, 1, _OLD)
        before = table_columns(cat.conn, _TABLE)
        with pytest.raises(RuntimeError, match="later step"):
            _write(cat, tmp_path, 2, _OLD.append_column("revision", pa.array(["r", "s"])), also=fail)
        assert table_columns(cat.conn, _TABLE) == before
        assert _rows(cat, "listing_key, id") == [(1, 1), (1, 2)]
    finally:
        cat.close()


@pytest.mark.parametrize("mode", sorted(_MODES))
def test_a_changed_type_fails_loudly(tmp_path: Path, mode: str) -> None:
    cat = _catalog(tmp_path, mode)
    try:
        _write(cat, tmp_path, 1, _OLD)
        retyped = _OLD.set_column(1, "name", pa.array([10, 20], pa.int64()))
        with pytest.raises(IntegrityError, match=r"column name of skillcorner_pining_matches is VARCHAR.*BIGINT"):
            _write(cat, tmp_path, 2, retyped)
    finally:
        cat.close()


@pytest.mark.parametrize("mode", sorted(_MODES))
def test_a_dropped_column_fails_loudly(tmp_path: Path, mode: str) -> None:
    cat = _catalog(tmp_path, mode)
    try:
        _write(cat, tmp_path, 1, _OLD)
        with pytest.raises(IntegrityError, match=r"no longer carries column name of skillcorner_pining_matches"):
            _write(cat, tmp_path, 2, _OLD.drop_columns(["name"]))
    finally:
        cat.close()


def _wider_file(kit: LakeKit, match_key: int) -> Any:
    """A PLAYERS file of `match_key` with one column the table does not have yet."""
    older = kit.written(match_key, visibility="public", frames=3, offset=0)
    table: pa.Table = pq.read_table(older.local_path)  # pyright: ignore[reportUnknownMemberType] - pyarrow.parquet ships no stubs
    wider = table.append_column("quality", pa.array(["good"] * table.num_rows, pa.string()))
    path = kit.base / "out-wider" / f"{match_key}-v2-{uuid.uuid4().hex}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(wider, path)  # pyright: ignore[reportUnknownMemberType] - pyarrow.parquet ships no stubs
    return store_written_file(
        kit.storage, path, table=PLAYERS, visibility="public", rows=wider.num_rows, content_version="v2"
    )


def test_a_move_re_registers_an_older_narrower_file(kit: LakeKit) -> None:
    cat = kit.catalog()
    kit.add_match(cat, 1, visibility="public", frames=3, offset=0)  # written before the column existed
    swap_match_files(cat, 2, {PLAYERS: [_wider_file(kit, 2).key]})
    cat.commit("run_end")
    move_now(cat, kit.storage, 1, "restricted")  # copies the narrower file of match 1 and registers the copy
    rows = cat.conn.execute(
        f"SELECT DISTINCT match_key, quality FROM lake.main.{PLAYERS} ORDER BY match_key"  # noqa: S608 - test constant
    ).fetchall()
    assert rows == [(1, None), (2, "good")]
    cat.close()
