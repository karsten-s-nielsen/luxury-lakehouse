"""S1 Task 3: the plaintext `encrypted` flag + both-direction fail-closed (spec S1, plan T3)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from lakehouse.catalog.manifest import Manifest
from lakehouse.catalog.publish import CurrentPointer, check_encryption
from lakehouse.errors import ConfigError

_ROOTS = {"lake": "/l", "restricted": "/r"}


def _pointer(*, encrypted: bool) -> CurrentPointer:
    return CurrentPointer.build(
        seq=1,
        version_key="catalog/v/1.ducklake",
        size=10,
        sha256="abc",
        parent_seq=None,
        lease_generation=1,
        storage_roots=dict(_ROOTS),
        encrypted=encrypted,
    )


def test_flag_round_trips_true() -> None:
    assert CurrentPointer.parse(_pointer(encrypted=True).to_bytes(), tag=None).encrypted is True


def test_flag_round_trips_false() -> None:
    assert CurrentPointer.parse(_pointer(encrypted=False).to_bytes(), tag=None).encrypted is False


def test_old_current_without_the_field_still_parses() -> None:
    # A CURRENT written before S1 has no "encrypted" key; it must parse (default False) with a valid checksum,
    # so the flag must be excluded from the self-checksum.
    payload = json.loads(_pointer(encrypted=False).to_bytes())
    del payload["encrypted"]
    parsed = CurrentPointer.parse(json.dumps(payload).encode(), tag=None)
    assert parsed.encrypted is False


def test_manifest_carries_the_flag() -> None:
    manifest = Manifest(
        seq=1,
        kind="run_end",
        run_id="r",
        published_at=datetime.now(UTC),
        version_size=1,
        version_sha256="x",
        storage_roots=dict(_ROOTS),
        keys=(),
        encrypted=True,
    )
    assert Manifest.model_validate_json(manifest.to_bytes()).encrypted is True


def test_encrypted_lake_without_key_fails_closed() -> None:
    with pytest.raises(ConfigError):
        check_encryption(recorded=True, key_present=False, migrating=False)


def test_unencrypted_lake_with_key_fails_closed() -> None:
    with pytest.raises(ConfigError):
        check_encryption(recorded=False, key_present=True, migrating=False)


def test_migration_is_exempt_from_the_unencrypted_with_key_stop() -> None:
    check_encryption(recorded=False, key_present=True, migrating=True)  # no raise


def test_matching_states_do_not_raise() -> None:
    check_encryption(recorded=True, key_present=True, migrating=False)
    check_encryption(recorded=False, key_present=False, migrating=False)
