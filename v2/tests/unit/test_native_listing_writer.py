"""S1 Task 9: encrypted-local listings write natively with a per-record `_visibility` column (plan T9 gap).

A source-level listing splits by visibility. On an unencrypted-local or HF lake it becomes one file per root
(`place_root`), so a private record lands in the restricted root and the bronze table has no `_visibility`. On an
encrypted-local lake there is one root, so the restricted copy is the `_visibility` column: `NativeListingWriter`
tags each record and writes it into one encrypted DuckLake table.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from lakekit import StubLease, lake_roots

from lakehouse.bronze.schemas import BronzeTable, bronze_table
from lakehouse.bronze.writers import (
    NativeListingWriter,
    RegisterListingWriter,
    select_listing_writer,
)
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_TABLE: BronzeTable = bronze_table("skillcorner_pining_matches")
_RECORDS = pa.table({"entity_id": pa.array([1, 2, 3], pa.int64()), "visibility": ["public", "private", "public"]})


def _storage(base: Path) -> LocalStorage:
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LocalStorage(lake_roots(base))


def _columns(cat: WorkingCatalog, table: BronzeTable) -> list[str]:
    return [
        str(r[0])
        for r in cat.conn.execute(
            "SELECT column_name FROM duckdb_columns() WHERE database_name = 'lake' "
            "AND schema_name = 'main' AND table_name = ?",
            [table],
        ).fetchall()
    ]


def test_select_listing_writer_picks_native_only_for_encrypted_local(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    keyed = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "wk", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    plain = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "wp", env={})
    try:
        assert isinstance(select_listing_writer(keyed, storage), NativeListingWriter)
        assert isinstance(select_listing_writer(plain, storage), RegisterListingWriter)
    finally:
        keyed.close()
        plain.close()


def test_native_listing_tags_each_record_and_is_encrypted(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    writer = NativeListingWriter(cat, storage)
    rows = writer.write(
        _TABLE,
        _RECORDS,
        key=77,
        visibility_column="visibility",
        source_visibility="public",
        content_version="v1",
        out_dir=tmp_path / "out",
    )
    writer.swap(_TABLE, 77, also=lambda: None)
    cat.commit("run_end")
    assert rows == 3

    found = cat.conn.execute(
        f'SELECT entity_id, _visibility, listing_key FROM lake.main."{_TABLE}" ORDER BY entity_id'  # noqa: S608 - constant
    ).fetchall()
    assert [(int(e), str(v), int(k)) for e, v, k in found] == [
        (1, "public", 77),
        (2, "private", 77),
        (3, "public", 77),
    ]

    parquets = list((tmp_path / "lake" / "data").rglob("*.parquet"))
    assert parquets, "no native parquet written"
    with pytest.raises(Exception):  # noqa: B017 - an encrypted DuckLake parquet is not a plain parquet
        pq.read_table(parquets[0])  # pyright: ignore[reportUnknownMemberType] - pyarrow.parquet ships no stubs

    cat.close()
    reader = CatalogReader(storage, env={ENCRYPTION_KEY_ENV: _KEY})
    try:
        with reader.session() as conn:
            total = conn.execute(f'SELECT count(*) FROM lake.main."{_TABLE}"').fetchone()  # noqa: S608 - constant
    finally:
        reader.close()
    assert total is not None and total[0] == 3


def test_unencrypted_listing_carries_no_visibility_column(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={})
    writer = RegisterListingWriter(cat, storage)
    try:
        writer.write(
            _TABLE,
            _RECORDS,
            key=77,
            visibility_column="visibility",
            source_visibility="public",
            content_version="v1",
            out_dir=tmp_path / "out",
        )
        writer.swap(_TABLE, 77, also=lambda: None)
        cat.commit("run_end")
        columns = _columns(cat, _TABLE)
        assert "_visibility" not in columns, "unencrypted-local keeps today's two-root listing schema"
        assert "listing_key" in columns
    finally:
        cat.close()
