"""The local operator account's service (spec §19, §20; plan Tasks 26 and 27).

`lakehouse account create` and the local sign-in of the web app reach the account through here, never through the
store. The password is passed through to the Argon2id hash and is never returned, stored or logged.
"""

from __future__ import annotations

from lakehouse.config.account import AccountStore
from lakehouse.storage.base import Storage

__all__ = ["AccountService"]


class AccountService:
    def __init__(self, storage: Storage) -> None:
        self._store = AccountStore(storage)

    def exists(self) -> bool:
        return self._store.exists()

    def username(self) -> str | None:
        return self._store.username()

    def create(self, username: str, password: str, *, replace: bool = False) -> None:
        self._store.create(username, password, replace=replace)

    def verify(self, username: str, password: str) -> bool:
        return self._store.verify(username, password)
