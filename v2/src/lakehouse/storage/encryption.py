"""The encryption-key resolver for a local lake (spec S1 "Constraints", plan Task 1).

One seam: the application reads the key only from the environment, through `resolve_encryption_key`, and never
knows how the environment was populated (owner decision 2). The key gates encryption on an **encrypted-local**
lake (a local lake with the key present); an `hf://` lake never takes a key. `validate_key` pins the charset and
length before the value is ever placed in SQL, so a bad key never reaches an `ATTACH` literal and the `Masker`
(values of at least `MIN_SECRET_LENGTH`) can redact it. The resolver raises `ConfigError` (exit 2) without ever
echoing the key value.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Final

from lakehouse.errors import ConfigError
from lakehouse.mask import MIN_SECRET_LENGTH
from lakehouse.storage.keys import RootMap

__all__ = ["ENCRYPTION_KEY_ENV", "resolve_encryption_key", "validate_key"]

ENCRYPTION_KEY_ENV: Final = "LAKEHOUSE_ENCRYPTION_KEY"
# base64url / hex alphabet: no quote, space, or SQL-special character can enter the METADATA_PARAMETERS literal.
_KEY_CHARSET: Final = re.compile(r"^[A-Za-z0-9_-]+$")
# At least 256 bits of entropy (spec "minimum entropy"): 64 characters is 256 bits even for a hex key (16-symbol
# alphabet, 4 bits/char) — the owner's `openssl rand -hex 32` is exactly 64 — and 384 bits for base64url. Raw is
# HKDF-derived with no salt, so an underlength key would be open to an offline brute force. This floor is >=
# `MIN_SECRET_LENGTH` (the mask threshold), so a valid key is always redactable.
MIN_KEY_LENGTH: Final = 64
assert MIN_KEY_LENGTH >= MIN_SECRET_LENGTH  # noqa: S101 - a module-load invariant, not test logic


def validate_key(value: str) -> str:
    """Return the key when it is at least `MIN_KEY_LENGTH` characters of the base64url/hex charset (>= 256 bits);
    otherwise raise `ConfigError`. The message names the constraint only, never the value."""
    if len(value) < MIN_KEY_LENGTH:
        raise ConfigError(
            f"{ENCRYPTION_KEY_ENV} must be at least {MIN_KEY_LENGTH} characters (>= 256 bits: 64 hex, e.g. "
            "`openssl rand -hex 32`, or 64 base64url)"
        )
    if _KEY_CHARSET.match(value) is None:
        raise ConfigError(f"{ENCRYPTION_KEY_ENV} must use only base64url characters: A-Z a-z 0-9 _ -")
    return value


def resolve_encryption_key(roots: RootMap, env: Mapping[str, str]) -> str | None:
    """The validated encryption key for an encrypted-local lake, or `None`.

    Returns the validated key when the lake root is local (`roots.lake.local_path is not None`) and the env var is
    set; `None` when the lake is local and the var is unset, or for any `hf://` lake (even with the var set).
    Raises `ConfigError` when the var is set on a local lake but invalid.
    """
    if roots.lake.local_path is None:
        return None
    value = env.get(ENCRYPTION_KEY_ENV)
    if not value:
        return None
    return validate_key(value)
