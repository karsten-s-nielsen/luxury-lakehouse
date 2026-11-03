"""S1 Task 7: keyed connections set temp_file_encryption so DuckDB encrypts spill/temp files (plan T7).

A byte-level spill grep is INFEASIBLE, not merely unreliable (audit S1-IMPL-04): an implementation spike forced an
external sort to spill under a 20 MB memory_limit and the positive control failed — even the UNKEYED spill files
held no cleartext marker, because DuckDB writes spill in a compressed/binary temp format. A grep therefore cannot
distinguish keyed from unkeyed spill, so it proves nothing either way. The oracle is that the setting is applied on
a keyed connection (DuckDB then encrypts temp files per its documented behavior); an unkeyed connection keeps the
default (off). The reliance on DuckDB's `temp_file_encryption` without an independent byte-proof is a residual
surfaced to the owner (spec "Temp and spill files"). The keyed dbt profile sets it too (S1-IMPL-01).
"""

from __future__ import annotations

from pathlib import Path

from lakekit import StubLease

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


def _setting(cat: WorkingCatalog) -> bool:
    row = cat.conn.execute("SELECT current_setting('temp_file_encryption')").fetchone()
    assert row is not None
    return bool(row[0])


def test_keyed_connection_enables_temp_file_encryption(tmp_path: Path) -> None:
    cat = WorkingCatalog.open(
        _storage(tmp_path),
        load_defaults("laptop"),
        StubLease(),
        work_dir=tmp_path / "w",
        env={ENCRYPTION_KEY_ENV: _KEY},
    )
    try:
        assert _setting(cat) is True
    finally:
        cat.close()


def test_unkeyed_connection_leaves_temp_file_encryption_off(tmp_path: Path) -> None:
    cat = WorkingCatalog.open(_storage(tmp_path), load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={})
    try:
        assert _setting(cat) is False
    finally:
        cat.close()
