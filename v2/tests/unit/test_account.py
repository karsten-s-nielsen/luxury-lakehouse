"""The local operator account (spec §19, §20; plan Tasks 26 and 27): one operator, an Argon2id hash in
`config/account.json`, written under the config lock, never the password itself."""

from __future__ import annotations

import pytest
from lakekit import LakeKit

from lakehouse.config.account import ACCOUNT_KEY, AccountStore
from lakehouse.errors import ConflictError

PASSWORD = "correct horse battery staple"  # noqa: S105 - a test value


def test_create_and_verify(kit: LakeKit) -> None:
    store = AccountStore(kit.storage)
    assert not store.exists()
    store.create("ops", PASSWORD)
    assert store.exists() and store.username() == "ops"
    assert store.verify("ops", PASSWORD)
    assert not store.verify("ops", PASSWORD + "!")
    assert not store.verify("someone", PASSWORD)
    stored = kit.storage.get("lake", ACCOUNT_KEY).decode()
    assert '"$argon2id$' in stored and PASSWORD not in stored


def test_create_twice_needs_replace(kit: LakeKit) -> None:
    store = AccountStore(kit.storage)
    store.create("ops", PASSWORD)
    with pytest.raises(ConflictError, match="already"):
        store.create("other", PASSWORD)
    store.create("other", PASSWORD + " 2", replace=True)
    assert store.verify("other", PASSWORD + " 2") and not store.verify("ops", PASSWORD)


def test_weak_or_odd_input_refused(kit: LakeKit) -> None:
    store = AccountStore(kit.storage)
    with pytest.raises(ValueError, match="12"):
        store.create("ops", "short")
    with pytest.raises(ValueError, match="name"):
        store.create("ops user", PASSWORD)
    assert not store.exists()
