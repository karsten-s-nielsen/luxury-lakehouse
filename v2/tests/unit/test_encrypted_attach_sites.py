"""S1 Task 4: encrypted attach at the working catalog + reader, both-direction fail-closed on open (plan T4)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from lakekit import StubLease

from lakehouse.catalog.duck import attach_or_config_error, connect_duckdb, encryption_options
from lakehouse.catalog.reader import CatalogReader
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.errors import ConfigError
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
_WRONG = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"  # a different valid-length key
_MARKER = b"secretmarker"


def _storage(base: Path) -> LocalStorage:
    roots = RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LocalStorage(roots)


def _settings():
    return load_defaults("laptop")


def _no_cleartext(root: Path) -> bool:
    return all(_MARKER not in p.read_bytes() for p in root.rglob("*") if p.is_file())


def _write_and_commit(storage: LocalStorage, env: dict[str, str], work: Path) -> None:
    cat = WorkingCatalog.open(storage, _settings(), StubLease(), work_dir=work, env=env)
    try:
        # 5 rows is below the default DATA_INLINING_ROW_LIMIT (10): a parquet appears only because the encrypted
        # attach sets the limit to 0, so the parquet-exists assertion is a real check of that option.
        cat.conn.execute("CREATE TABLE lake.main.ev AS SELECT 'secretmarker' AS x FROM range(5)")
        cat.commit("run_end")
    finally:
        cat.close()


def test_encrypted_catalog_is_not_cleartext_and_writes_a_parquet(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    env = {ENCRYPTION_KEY_ENV: _KEY}
    _write_and_commit(storage, env, tmp_path / "work1")
    lake = tmp_path / "lake"
    assert _no_cleartext(lake), "the marker is cleartext in the encrypted lake"
    # DATA_INLINING_ROW_LIMIT 0 -> the 50-row native write is a parquet file, not an inlined catalog row.
    assert list((lake / "data").rglob("*.parquet")), "no parquet written (data inlined?)"


def test_reopen_without_key_fails_closed(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _write_and_commit(storage, {ENCRYPTION_KEY_ENV: _KEY}, tmp_path / "work1")
    with pytest.raises(ConfigError):
        WorkingCatalog.open(storage, _settings(), StubLease(), work_dir=tmp_path / "work2", env={})


def test_reopen_with_key_reads_and_reader_too(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    env = {ENCRYPTION_KEY_ENV: _KEY}
    _write_and_commit(storage, env, tmp_path / "work1")
    reader = CatalogReader(storage, env=env)
    try:
        with reader.session() as conn:
            row = conn.execute("SELECT x FROM lake.main.ev LIMIT 1").fetchone()
    finally:
        reader.close()
    assert row is not None and row[0] == "secretmarker"


def test_unencrypted_lake_with_key_set_fails_closed(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _write_and_commit(storage, {}, tmp_path / "work1")  # created unencrypted
    with pytest.raises(ConfigError):
        WorkingCatalog.open(
            storage, _settings(), StubLease(), work_dir=tmp_path / "work2", env={ENCRYPTION_KEY_ENV: _KEY}
        )


def test_reattach_after_a_spill_does_not_fail(tmp_path: Path) -> None:
    """Regression (Mac encrypted-lake ingestion failure): `attach()` must set `temp_directory` only once per
    connection. DuckDB rejects a `temp_directory` change after a spill has used it, and `commit()` re-attaches on the
    same connection. A large spill (gradientsports scale) followed by a commit must not raise a
    `NotImplementedException` ("Cannot switch temporary directory after the current one has been used")."""
    storage = _storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, _settings(), StubLease(), work_dir=tmp_path / "work", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    try:
        cat.conn.execute("CREATE TABLE lake.main.ev AS SELECT 'secretmarker' AS x FROM range(5)")
        # Force a spill so DuckDB marks the temp directory as used, then restore headroom for the commit.
        cat.conn.execute("SET preserve_insertion_order=false")
        cat.conn.execute("SET memory_limit='200MB'")
        spill = "SELECT count(*) FROM (SELECT i, i * 2 AS j FROM range(40000000) t(i) ORDER BY j DESC)"
        cat.conn.execute(spill).fetchall()
        cat.conn.execute("SET memory_limit='2GB'")
        info = cat.commit("run_end")  # re-attaches; pre-fix this raised NotImplementedException at SET temp_directory
        assert info.seq >= 1
    finally:
        cat.close()


def test_wrong_key_fails_closed_as_config_error_at_both_sites(tmp_path: Path) -> None:
    """S1-IMPL-08: a wrong key is a DuckDB attach reject; it must surface as `ConfigError` (cli exit 2) at both the
    working-catalog and the reader open sites, with no key value in the message — not an uncaught DuckDB error."""
    storage = _storage(tmp_path)
    _write_and_commit(storage, {ENCRYPTION_KEY_ENV: _KEY}, tmp_path / "work1")

    with pytest.raises(ConfigError) as writer_exc:
        WorkingCatalog.open(
            storage, _settings(), StubLease(), work_dir=tmp_path / "work2", env={ENCRYPTION_KEY_ENV: _WRONG}
        )
    assert _WRONG not in str(writer_exc.value) and "ATTACH" not in str(writer_exc.value)

    reader = CatalogReader(storage, env={ENCRYPTION_KEY_ENV: _WRONG})
    try:
        with pytest.raises(ConfigError) as reader_exc, reader.session():
            pass
        assert _WRONG not in str(reader_exc.value)
    finally:
        reader.close()


def test_corrupt_encrypted_catalog_attach_keeps_its_own_error(tmp_path: Path) -> None:
    """T3 spec D7, Reviewer B T3-SPEC-22: one flipped region in an encrypted catalog, opened through the DuckLake attach
    with the RIGHT key, raises DuckDB's own error ("Computed AES tag differs ..."), not the wrong-key `ConfigError`. A
    wrong key still raises `ConfigError`, with no key in the message."""
    storage = _storage(tmp_path)
    _write_and_commit(storage, {ENCRYPTION_KEY_ENV: _KEY}, tmp_path / "w1")
    version = next((tmp_path / "lake" / "catalog" / "v").glob("*.ducklake"))
    corrupt = tmp_path / "corrupt.ducklake"
    data = bytearray(version.read_bytes())
    data[4096:8192] = b"\x00" * 4096
    corrupt.write_bytes(bytes(data))
    opts = ", ".join(encryption_options(_KEY))
    conn = connect_duckdb(env={}, caps=None)
    with pytest.raises(duckdb.Error) as raised:
        attach_or_config_error(conn, f"ATTACH 'ducklake:{corrupt.as_posix()}' AS lake ({opts})", key_present=True)
    assert not isinstance(raised.value, ConfigError)
    wrong = ", ".join(encryption_options(_WRONG))
    with pytest.raises(ConfigError) as refused:
        attach_or_config_error(conn, f"ATTACH 'ducklake:{version.as_posix()}' AS lake2 ({wrong})", key_present=True)
    assert _WRONG not in str(refused.value) and _KEY not in str(refused.value)
