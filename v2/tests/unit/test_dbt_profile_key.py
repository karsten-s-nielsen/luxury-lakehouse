"""S1 Task 5: the dbt profile opens an encrypted catalog via a ducklake secret, with no key value on disk (T5)."""

from __future__ import annotations

from pathlib import Path

from lakehouse.config.model import ResourceCaps
from lakehouse.runs.dbt import render_profile
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV
from lakehouse.storage.keys import RootMap, StorageRootInfo
from lakehouse.storage.local import LocalStorage

_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


def _storage(base: Path) -> LocalStorage:
    roots = RootMap(
        lake=StorageRootInfo(local_path=base / "lake", hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=base / "lake-restricted", hf_bucket=None, s3_bucket_path=None),
    )
    return LocalStorage(roots)


def test_encrypted_profile_uses_a_secret_and_hides_the_key(tmp_path: Path) -> None:
    profile = render_profile(
        catalog_path=tmp_path / "lake" / "work.ducklake",
        caps=ResourceCaps(),
        storage=_storage(tmp_path),
        env={ENCRYPTION_KEY_ENV: _KEY},
    )
    assert _KEY not in profile, "the key value must never reach the rendered profile"
    assert "type: ducklake" in profile and "name: lakesec" in profile
    assert "metadata_parameters" in profile
    assert f"env_var('{ENCRYPTION_KEY_ENV}')" in profile
    assert 'path: "ducklake:lakesec"' in profile


def test_encrypted_profile_encrypts_dbt_spill(tmp_path: Path) -> None:
    """S1-IMPL-01: the keyed dbt session sets `temp_file_encryption` and a cleaned temp dir, so an over-memory
    dbt build spills private silver/gold intermediates as ciphertext, not plaintext."""
    profile = render_profile(
        catalog_path=tmp_path / "lake" / "work.ducklake",
        caps=ResourceCaps(),
        storage=_storage(tmp_path),
        env={ENCRYPTION_KEY_ENV: _KEY},
    )
    assert "temp_file_encryption: true" in profile
    assert 'temp_directory: "' in profile and "dbt_tmp" in profile


def test_unencrypted_profile_attaches_the_catalog_directly(tmp_path: Path) -> None:
    profile = render_profile(
        catalog_path=tmp_path / "lake" / "work.ducklake",
        caps=ResourceCaps(),
        storage=_storage(tmp_path),
        env={},
    )
    assert "lakesec" not in profile
    assert ENCRYPTION_KEY_ENV not in profile
    assert "temp_file_encryption" not in profile  # the unkeyed profile sets no spill encryption
    assert 'path: "ducklake:' in profile and "work.ducklake" in profile
