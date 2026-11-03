"""S1 Task 0/Task 8 pin: the three-mode writer selection, including HF (plan Review Focus; audit S1-IMPL-13).

Encrypted-local (the catalog holds a key) selects the native writers; unencrypted-local and HF select the
`add_data_files` register writers, so their bronze path (`place_root` + `ducklake_add_data_files` +
`move_match_files`, no `_visibility`) is unchanged. HF is proven by composition: an `hf://` lake resolves no key,
so it reaches `select_writer` with `encryption_key is None` and gets the register writer, exactly like
unencrypted-local.
"""

from __future__ import annotations

from pathlib import Path

from lakekit import StubLease

from lakehouse.bronze.writers import (
    LocalNativeWriter,
    NativeListingWriter,
    RegisterListingWriter,
    RegisterWriter,
    select_listing_writer,
    select_writer,
)
from lakehouse.catalog.working import WorkingCatalog
from lakehouse.config.model import load_defaults
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV, resolve_encryption_key
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


def _local_storage(base: Path) -> LocalStorage:
    roots = RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )
    (base / "lake").mkdir(parents=True, exist_ok=True)
    (base / "lake-restricted").mkdir(parents=True, exist_ok=True)
    return LocalStorage(roots)


def _hf_roots() -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=None, hf_bucket="luxury-lakehouse/lake", s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=None, hf_bucket="luxury-lakehouse/restricted", s3_bucket_path=None),
    )


def test_encrypted_local_selects_the_native_writers(tmp_path: Path) -> None:
    storage = _local_storage(tmp_path)
    cat = WorkingCatalog.open(
        storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={ENCRYPTION_KEY_ENV: _KEY}
    )
    try:
        assert isinstance(select_writer(cat, storage), LocalNativeWriter)
        assert isinstance(select_listing_writer(cat, storage), NativeListingWriter)
    finally:
        cat.close()


def test_unencrypted_local_selects_the_register_writers(tmp_path: Path) -> None:
    storage = _local_storage(tmp_path)
    cat = WorkingCatalog.open(storage, load_defaults("laptop"), StubLease(), work_dir=tmp_path / "w", env={})
    try:
        assert isinstance(select_writer(cat, storage), RegisterWriter)
        assert isinstance(select_listing_writer(cat, storage), RegisterListingWriter)
    finally:
        cat.close()


def test_hf_lake_reaches_selection_with_no_key_so_it_uses_the_register_writers() -> None:
    # An hf:// lake resolves no key even when the var is set, so select_writer sees `encryption_key is None` and
    # selects the register writers — the same unchanged bronze path as unencrypted-local.
    assert resolve_encryption_key(_hf_roots(), {ENCRYPTION_KEY_ENV: _KEY}) is None
