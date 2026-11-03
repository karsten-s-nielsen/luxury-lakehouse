"""S1 Task 1: the encryption-key resolver, validation, and masking (spec § "Constraints", plan T1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse.errors import ConfigError
from lakehouse.mask import MIN_SECRET_LENGTH, SECRET_ENV_VARS
from lakehouse.schedule.base import check_env
from lakehouse.storage.encryption import ENCRYPTION_KEY_ENV, MIN_KEY_LENGTH, resolve_encryption_key, validate_key
from lakehouse.storage.keys import RootMap, StorageRootInfo

_HEX = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"  # openssl rand -hex 32 (64 hex = 256 bits)
_B64URL = "AbCd_-90" * 8  # 64 base64url characters


def _local() -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=Path("lake-root"), hf_bucket=None, s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=Path("lake-restricted-root"), hf_bucket=None, s3_bucket_path=None),
    )


def _hf() -> RootMap:
    return RootMap(
        lake=StorageRootInfo(local_path=None, hf_bucket="luxury-lakehouse/lake", s3_bucket_path=None),
        restricted=StorageRootInfo(local_path=None, hf_bucket="luxury-lakehouse/restricted", s3_bucket_path=None),
    )


def test_valid_hex_key_resolves_on_local_lake() -> None:
    assert resolve_encryption_key(_local(), {ENCRYPTION_KEY_ENV: _HEX}) == _HEX


def test_valid_base64url_key_resolves() -> None:
    assert resolve_encryption_key(_local(), {ENCRYPTION_KEY_ENV: _B64URL}) == _B64URL


def test_unset_var_on_local_lake_returns_none() -> None:
    assert resolve_encryption_key(_local(), {}) is None


def test_hf_lake_never_resolves_a_key_even_when_set() -> None:
    assert resolve_encryption_key(_hf(), {ENCRYPTION_KEY_ENV: _HEX}) is None


def test_short_key_raises_and_message_leaks_nothing() -> None:
    short = "0123456789abcdef"  # 16 chars (64 bits of hex): under the 256-bit floor
    with pytest.raises(ConfigError) as exc:
        resolve_encryption_key(_local(), {ENCRYPTION_KEY_ENV: short})
    message = str(exc.value)
    assert short not in message
    assert "ATTACH" not in message and "METADATA_PARAMETERS" not in message


def test_out_of_charset_key_raises_and_message_leaks_nothing() -> None:
    bad = "has a space and 'quote'"
    with pytest.raises(ConfigError) as exc:
        validate_key(bad)
    assert bad not in str(exc.value)


def test_key_floor_is_256_bits_and_above_the_masker_floor() -> None:
    assert MIN_KEY_LENGTH == 64  # 256 bits even for a hex key (4 bits/char)
    assert MIN_KEY_LENGTH >= MIN_SECRET_LENGTH  # a valid key is always long enough for the Masker to redact
    assert len(validate_key("a" * MIN_KEY_LENGTH)) == MIN_KEY_LENGTH
    with pytest.raises(ConfigError):
        validate_key("a" * (MIN_KEY_LENGTH - 1))


def test_env_var_is_a_masked_secret_and_a_scheduler_secret() -> None:
    assert ENCRYPTION_KEY_ENV in SECRET_ENV_VARS
    # The scheduler never writes a secret-named var into a task file (schedule/base.py guard).
    with pytest.raises(ValueError, match="looks like a secret"):
        check_env({ENCRYPTION_KEY_ENV: _HEX})
