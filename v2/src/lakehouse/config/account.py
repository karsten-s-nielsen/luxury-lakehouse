"""The local operator account (spec §19, §20; plan Tasks 26 and 27).

A local install has one operator. `lakehouse account create` stores the name and an Argon2id hash of the password in
`config/account.json` in the lake root, under the config lock; the password itself is never stored or logged. The
local sign-in of the web app (Task 27) verifies against it.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Final

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from pydantic import AwareDatetime, BaseModel, ConfigDict

from lakehouse.config.store import ConfigLock
from lakehouse.errors import ConflictError
from lakehouse.storage.base import Storage

__all__ = ["ACCOUNT_KEY", "MIN_PASSWORD_LENGTH", "AccountStore"]

ACCOUNT_KEY: Final = "config/account.json"
MIN_PASSWORD_LENGTH: Final = 12
_NAME: Final = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")
_HASHER: Final = PasswordHasher()  # Argon2id with the library's recommended parameters


class _Account(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    username: str
    password_hash: str
    created_at: AwareDatetime


class AccountStore:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage
        self._lock = ConfigLock(storage)

    def _read(self) -> _Account | None:
        try:
            return _Account.model_validate_json(self._storage.get("lake", ACCOUNT_KEY))
        except FileNotFoundError:
            return None

    def exists(self) -> bool:
        return self._read() is not None

    def username(self) -> str | None:
        account = self._read()
        return None if account is None else account.username

    def create(self, username: str, password: str, *, replace: bool = False) -> None:
        """Store the operator; an existing account is replaced only with `replace=True`."""
        if not _NAME.match(username):
            raise ValueError("the operator name is letters, digits, '.', '_', '@' and '-' only")
        if len(password) < MIN_PASSWORD_LENGTH:
            raise ValueError(f"the password needs at least {MIN_PASSWORD_LENGTH} characters")
        account = _Account(username=username, password_hash=_HASHER.hash(password), created_at=datetime.now(UTC))
        with self._lock:
            if self._read() is not None and not replace:
                raise ConflictError("an operator account already exists: replace it with --replace")
            self._storage.put("lake", ACCOUNT_KEY, account.model_dump_json(indent=2).encode("utf-8"))

    def verify(self, username: str, password: str) -> bool:
        """Whether the name and password are the operator's (the hash check always runs, so timing says nothing)."""
        account = self._read()
        stored = account.password_hash if account is not None else _HASHER.hash("no account: compare anyway")
        try:
            matched = _HASHER.verify(stored, password)
        except (VerificationError, InvalidHashError):
            return False
        return matched and account is not None and account.username == username
