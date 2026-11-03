"""S1 Task 8: LocalNativeWriter writes bronze data natively so ENCRYPTED covers it (plan T8, step B)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from lakekit import PLAYERS, StubLease, players

from lakehouse.bronze.writers import LocalNativeWriter, Put, RegisterWriter, select_writer
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


def _storage(base: Path) -> LocalStorage:
    roots = RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LocalStorage(roots)


def test_select_writer_picks_native_only_for_encrypted_local(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    keyed = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "wk", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    plain = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "wp", env={})
    try:
        assert isinstance(select_writer(keyed, storage), LocalNativeWriter)
        assert isinstance(select_writer(plain, storage), RegisterWriter)
    finally:
        keyed.close()
        plain.close()


def test_native_bronze_is_encrypted_and_reads_back(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    writer = LocalNativeWriter(cat, storage)
    put = Put(PLAYERS, players(100, 50, 0), is_tracking=True, schema=None, source_hash=None)
    row = writer.write_table(
        put,
        match_key=100,
        visibility="public",
        content_version="v1",
        run_id="r",
        out_dir=tmp_path / "out",
        ingested_at=datetime.now(UTC),
    )
    writer.swap(100, [row], set(), also=lambda: None)
    cat.commit("run_end")
    cat.close()

    parquets = list((tmp_path / "lake" / "data").rglob("*.parquet"))
    assert parquets, "no native parquet written"
    with pytest.raises(Exception):  # noqa: B017 - an encrypted DuckLake parquet is not a plain parquet
        pq.read_table(parquets[0])  # pyright: ignore[reportUnknownMemberType] - pyarrow.parquet ships no stubs

    reader = CatalogReader(storage, env={ENCRYPTION_KEY_ENV: _KEY})
    try:
        with reader.session() as conn:
            found = conn.execute(f"SELECT count(*) FROM lake.main.{PLAYERS}").fetchone()  # noqa: S608 - constant
    finally:
        reader.close()
    assert found is not None and found[0] == 50


def test_native_swap_survives_a_pending_result_on_the_connection(tmp_path: Path) -> None:
    """S1-IMPL-16 regression (unit-level, not only the e2e oracle): an upstream `count(*)` `fetchone` leaves an
    unfinished result on the connection, and `conn.register` refuses one. `native_swap` drains it first, so the
    write still succeeds."""
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    try:
        cat.conn.execute("SELECT count(*) FROM duckdb_tables()").fetchone()  # leave a pending result, as a read would
        writer = LocalNativeWriter(cat, storage)
        put = Put(PLAYERS, players(100, 5, 0), is_tracking=True, schema=None, source_hash=None)
        row = writer.write_table(
            put,
            match_key=100,
            visibility="public",
            content_version="v1",
            run_id="r",
            out_dir=tmp_path / "out",
            ingested_at=datetime.now(UTC),
        )
        writer.swap(100, [row], set(), also=lambda: None)  # must not raise ResourceInUseException
        cat.commit("run_end")
        count = cat.conn.execute(f"SELECT count(*) FROM lake.main.{PLAYERS}").fetchone()  # noqa: S608 - constant
        assert count is not None and count[0] == 5
    finally:
        cat.close()
