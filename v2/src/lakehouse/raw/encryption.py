"""AES-256-GCM encryption of raw-archive objects on an encrypted-local lake (spec S1, plan T6).

Raw deliveries are v2-written storage objects, not DuckLake-managed, so `ENCRYPTED` does not reach them. On an
encrypted-local lake they are encrypted here with the lakehouse key: AES-256-GCM (a vetted primitive from the
`cryptography` dependency), a fresh 12-byte nonce per object prepended to the ciphertext, the 32-byte key derived
from the lakehouse key with HKDF-SHA256. The content addressing is unchanged (the storage key stays
`raw/<content_sha256>.zst`); only the stored bytes are the ciphertext of the compressed bytes, so `copy_to` and
the visibility move copy the opaque ciphertext unchanged.
"""

from __future__ import annotations

import os
from typing import Final

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

__all__ = ["decrypt_raw", "encrypt_raw"]

_INFO: Final = b"lakehouse-raw-aes256gcm-v1"
_NONCE_BYTES: Final = 12
_KEY_BYTES: Final = 32


def _derive(key: str) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=_KEY_BYTES, salt=None, info=_INFO).derive(key.encode("utf-8"))


def encrypt_raw(data: bytes, key: str) -> bytes:
    """`nonce || AES-256-GCM(data)`; the nonce is fresh per call, the key HKDF-derived from the lakehouse key."""
    nonce = os.urandom(_NONCE_BYTES)
    return nonce + AESGCM(_derive(key)).encrypt(nonce, data, None)


def decrypt_raw(blob: bytes, key: str) -> bytes:
    """The plaintext of `encrypt_raw`; raises (GCM auth failure) on a wrong key or a tampered object."""
    return AESGCM(_derive(key)).decrypt(blob[:_NONCE_BYTES], blob[_NONCE_BYTES:], None)
