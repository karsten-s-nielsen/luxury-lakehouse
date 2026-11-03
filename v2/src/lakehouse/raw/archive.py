"""The raw archive (spec §13).

Each delivered file is stored once under its `content_sha256` (the SHA-256 of the decoded bytes), as the decoded bytes
compressed with zstd, at `raw/<content_sha256>.zst`. A raw file takes the visibility of its own delivery: a private
delivery lives only in the restricted root, a public one in the lake root (§9, §29.2). An object is never overwritten;
when a source changes a file, both versions stay. The archive has an on/off switch per install (§13.2).
"""

from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal

import zstandard

from lakehouse.bronze.writer import Visibility, place_root
from lakehouse.errors import IntegrityError
from lakehouse.hashing import MAX_DECODED_BYTES, Wrapper, decode_transport, delivery_sha256
from lakehouse.ids.native import Channel, Provider
from lakehouse.raw.encryption import decrypt_raw, encrypt_raw
from lakehouse.storage.base import Storage
from lakehouse.storage.keys import CanonicalKey, Root

__all__ = ["RAW_PREFIX", "Delivery", "RawArchive", "raw_key"]

RAW_PREFIX: Final = "raw/"
_ZSTD_LEVEL: Final = 9  # spec §25.3 measured the raw zone at zstd 9
_READ_CHUNK: Final = 1024 * 1024
_LOCATE_ORDER: Final[tuple[Root, ...]] = ("restricted", "lake")


def raw_key(content_sha256: str) -> str:
    if len(content_sha256) != 64 or any(c not in "0123456789abcdef" for c in content_sha256):
        raise ValueError(f"invalid content_sha256 {content_sha256!r}")
    return f"{RAW_PREFIX}{content_sha256}.zst"


@dataclass(frozen=True)
class Delivery:
    """One file as one channel delivered it."""

    provider: Provider
    channel: Channel
    native_match_id: str
    artifact: str
    raw: bytes
    decoded: bytes
    wrapper: Wrapper
    delivery_sha256: str
    content_sha256: str
    visibility: Literal["public", "private"]
    change_signal: str | None
    fetched_at: datetime

    @classmethod
    def received(
        cls,
        *,
        provider: Provider,
        channel: Channel,
        native_match_id: str,
        artifact: str,
        raw: bytes,
        visibility: Visibility,
        change_signal: str | None,
        fetched_at: datetime,
    ) -> Delivery:
        """A delivery with its wrapper removed and both hashes computed (§13.1)."""
        decoded, wrapper = decode_transport(raw)
        return cls(
            provider=provider,
            channel=channel,
            native_match_id=native_match_id,
            artifact=artifact,
            raw=raw,
            decoded=decoded,
            wrapper=wrapper,
            delivery_sha256=delivery_sha256(raw),
            content_sha256=hashlib.sha256(decoded).hexdigest(),
            visibility=visibility,
            change_signal=change_signal,
            fetched_at=fetched_at,
        )

    @classmethod
    def from_archive(
        cls,
        *,
        provider: Provider,
        channel: Channel,
        native_match_id: str,
        artifact: str,
        decoded: bytes,
        wrapper: Wrapper,
        delivery_sha256: str,
        content_sha256: str,
        visibility: Visibility,
        change_signal: str | None,
        fetched_at: datetime,
    ) -> Delivery:
        """A delivery rebuilt from the raw archive for an offline re-parse (§13.2): the archived decoded bytes with the
        wrapper and the delivery hash that lineage recorded, so lineage stays exact. The content hash is checked."""
        if hashlib.sha256(decoded).hexdigest() != content_sha256:
            raise IntegrityError(f"the archived {artifact} of {native_match_id} does not match its content hash")
        return cls(
            provider=provider,
            channel=channel,
            native_match_id=native_match_id,
            artifact=artifact,
            raw=decoded,
            decoded=decoded,
            wrapper=wrapper,
            delivery_sha256=delivery_sha256,
            content_sha256=content_sha256,
            visibility=visibility,
            change_signal=change_signal,
            fetched_at=fetched_at,
        )


def _other(root: Root) -> Root:
    return "lake" if root == "restricted" else "restricted"


class RawArchive:
    """The raw zone of both roots. With `enabled=False`, `store` writes nothing (§13.2). On an encrypted-local lake
    (`key` set) each archived object is AES-256-GCM encrypted at rest (spec S1, plan T6)."""

    def __init__(self, storage: Storage, *, enabled: bool, key: str | None = None) -> None:
        self._storage = storage
        self._enabled = enabled
        self._key = key

    @property
    def enabled(self) -> bool:
        return self._enabled

    def store(self, delivery: Delivery) -> CanonicalKey | None:
        """Archive the decoded bytes in the root of the delivery's own visibility; never overwrite."""
        if not self._enabled:
            return None
        if hashlib.sha256(delivery.decoded).hexdigest() != delivery.content_sha256:
            raise IntegrityError(f"the decoded bytes of {delivery.artifact} do not match their content_sha256")
        # Encrypted-local is a single root (spec S1 A-prime): all raw lives in the lake root, protected by the
        # AES-256-GCM encryption rather than a restricted root. Unencrypted-local and HF keep the two-root split.
        root = "lake" if self._key is not None else place_root(delivery.visibility)
        key = raw_key(delivery.content_sha256)
        if not self._storage.exists(root, key):
            compressed = zstandard.ZstdCompressor(level=_ZSTD_LEVEL, write_content_size=True).compress(delivery.decoded)
            payload = encrypt_raw(compressed, self._key) if self._key is not None else compressed
            self._storage.put_if_absent(root, key, payload)  # False: a concurrent or earlier store, same bytes
        return CanonicalKey(root, key)

    def locate(self, content_sha256: str) -> CanonicalKey | None:
        """The archived object, restricted root first; the bytes are the same wherever it is found."""
        key = raw_key(content_sha256)
        for root in _LOCATE_ORDER:
            if self._storage.exists(root, key):
                return CanonicalKey(root, key)
        return None

    def copy_to(self, content_sha256: str, root: Root) -> CanonicalKey | None:
        """Copy an archived object into `root` with `put_if_absent` (§29.12). `None`, and no write, when no object
        exists (archive off, or a delivery that was never archived)."""
        key = raw_key(content_sha256)
        if self._storage.exists(root, key):
            return CanonicalKey(root, key)
        source = _other(root)
        if not self._storage.exists(source, key):
            return None
        self._storage.put_if_absent(root, key, self._storage.get(source, key))
        return CanonicalKey(root, key)

    def read(self, content_sha256: str) -> bytes:
        """The decoded bytes of an archived object, checked against their hash (offline reparse reads only this)."""
        found = self.locate(content_sha256)
        if found is None:
            raise IntegrityError(f"raw object {content_sha256} is in neither root")
        stored = self._storage.get(found.root, found.path)
        compressed = decrypt_raw(stored, self._key) if self._key is not None else stored
        out = io.BytesIO()
        reader = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(compressed))
        while chunk := reader.read(_READ_CHUNK):
            out.write(chunk)
            if out.tell() > MAX_DECODED_BYTES:
                raise IntegrityError(f"raw object {found} decodes to more than {MAX_DECODED_BYTES} bytes")
        data = out.getvalue()
        if hashlib.sha256(data).hexdigest() != content_sha256:
            raise IntegrityError(f"raw object {found} does not match its SHA-256")
        return data
